/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.discover.profile.ProfileEphemeralCodec.Envelope;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore.Node;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore.Snapshot;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Experimental cold-path publisher under the persistent readiness namespace.
 * Only this publisher's session/nonce can be reconciled or demoted; foreign, unavailable and corrupt reads throw.
 * Caller owns the connected client and local admission gate. No watcher, reconnect, retry or production registration.
 */
public final class ZKProfileEphemeralRegistrationStore implements EphemeralProfileRegistrationStore {
    private final ZooKeeper zk;
    private final ZKProfileRegistrationStore backend;
    private final String bookieId;
    private final long sessionId;
    private final UUID nonce = UUID.randomUUID();
    private final long timeoutNanos;
    private volatile boolean retired;

    public ZKProfileEphemeralRegistrationStore(ZooKeeper zk, String root, String bookieId,
                                              List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        backend = new ZKProfileRegistrationStore(zk, root, acls, timeout);
        backend.path(bookieId);
        this.bookieId = bookieId;
        sessionId = zk.getSessionId();
        timeoutNanos = timeout.toNanos();
        requireSession();
    }

    @Override
    public Optional<ProfileServiceInfoProjection> readServiceInfo(String bookieId) {
        return read(bookieId, false).map(Envelope::projection);
    }

    @Override
    public Optional<WritableRegistration> readWritable(String bookieId) {
        return read(bookieId, true).map(e -> writable(e.projection()));
    }

    private Optional<Envelope> read(String bookieId, boolean writable) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            requireScope(bookieId);
            requireSession();
            if (writable && retired) {
                throw new IllegalStateException("Publisher was demoted; use a new startup attempt");
            }
            Snapshot authority = backend.snapshot(bookieId, deadline);
            if (authority == null) {
                return Optional.empty();
            }
            Node node = node(authority.path() + (writable ? "/writable" : "/service-info"), deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return Optional.empty();
            }
            Envelope envelope = owned(node, bookieId);
            marker(authority.path(), deadline);
            if (writable) {
                Envelope service = owned(node(authority.path() + "/service-info", deadline), bookieId);
                if (!service.equals(envelope) || !matches(authority, envelope.projection())) {
                    throw new IllegalStateException("Writable projection is stale or mismatched");
                }
            }
            requireSession();
            if (writable && retired) {
                throw new IllegalStateException("Publisher was demoted during read");
            }
            return Optional.of(envelope);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted registration read", interrupted);
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("Registration read unavailable", timeout);
        }
    }

    @Override
    public PublicationResult publishServiceInfo(ProfileServiceInfoProjection projection) {
        Objects.requireNonNull(projection, "projection");
        return publish(projection.bookieId(), projection, null);
    }

    @Override
    public PublicationResult registerWritable(WritableRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        return publish(registration.bookieId(), null, registration);
    }

    private PublicationResult publish(String bookieId, ProfileServiceInfoProjection requested,
                                      WritableRegistration registration) {
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            requireScope(bookieId);
            requirePublishingSession();
            Snapshot authority = backend.snapshot(bookieId, deadline);
            if (authority == null) {
                return result(PublicationStatus.CONFLICT);
            }
            String path = authority.path();
            Node serviceNode = node(path + "/service-info", deadline);
            Node writableNode = node(path + "/writable", deadline);
            Envelope current = serviceNode.code() == Code.NONODE.intValue() ? null : owned(serviceNode, bookieId);
            if (current != null) {
                marker(path, deadline);
            }
            ProfileServiceInfoProjection target = requested != null ? requested
                    : current == null ? null : current.projection();
            if (target == null || !matches(authority, target)
                    || registration != null && !writable(target).equals(registration)) {
                return result(PublicationStatus.CONFLICT);
            }
            List<Op> operations = conditions(authority);
            Envelope desired = new Envelope(nonce, sessionId, target);
            byte[] bytes = ProfileEphemeralCodec.encode(desired);
            if (registration == null) {
                if (desired.equals(current)) {
                    requirePublishingSession();
                    return result(PublicationStatus.ALREADY_APPLIED);
                }
                if (writableNode.code() != Code.NONODE.intValue()) {
                    return result(PublicationStatus.CONFLICT);
                }
                if (current == null) {
                    operations.add(Op.create(markerPath(path), new byte[0], backend.publicationMarkerAcls(),
                            CreateMode.EPHEMERAL));
                    operations.add(Op.create(path + "/service-info", bytes, backend.publicationAcls(),
                            CreateMode.EPHEMERAL));
                } else {
                    operations.add(Op.check(markerPath(path), 0));
                    operations.add(Op.setData(path + "/service-info", bytes, version(serviceNode)));
                }
            } else {
                if (writableNode.code() != Code.NONODE.intValue()) {
                    Envelope existing = owned(writableNode, bookieId);
                    requirePublishingSession();
                    return result(existing.equals(desired)
                            ? PublicationStatus.ALREADY_APPLIED : PublicationStatus.CONFLICT);
                }
                operations.add(Op.check(markerPath(path), 0));
                // Updating the unchanged projection orders publication against a competing service-info refresh.
                operations.add(Op.setData(path + "/service-info", bytes, version(serviceNode)));
                operations.add(Op.create(path + "/writable", bytes, backend.publicationAcls(), CreateMode.EPHEMERAL));
            }
            requirePublishingSession();
            ZKProfileRegistrationStore.requireTime(deadline);
            submitted = true;
            int code = backend.multi(operations, deadline);
            if (code == Code.OK.intValue()) {
                // A caller-supplied ZooKeeperClient may have recreated its session while retrying the RPC.
                requirePublishingSession();
            }
            return mutation(code);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return result(submitted ? PublicationStatus.RESPONSE_LOST : PublicationStatus.UNAVAILABLE);
        } catch (TimeoutException timeout) {
            return result(submitted ? PublicationStatus.RESPONSE_LOST : PublicationStatus.UNAVAILABLE);
        } catch (IllegalArgumentException | IllegalStateException rejected) {
            return result(submitted ? PublicationStatus.RESPONSE_LOST : PublicationStatus.UNAVAILABLE);
        }
    }

    @Override
    public void demote(String bookieId) {
        requireScope(bookieId);
        // A demoted nonce is never reused, including after a lost deletion reply.
        retired = true;
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            requireSession();
            String path = backend.path(bookieId);
            Snapshot authority = backend.snapshot(bookieId, deadline);
            if (authority == null) {
                return;
            }
            Node service = node(path + "/service-info", deadline);
            Node writable = node(path + "/writable", deadline);
            Node anchor = node(markerPath(path), deadline);
            if (service.code() == Code.NONODE.intValue() && writable.code() == Code.NONODE.intValue()
                    && anchor.code() == Code.NONODE.intValue()) {
                return;
            }
            marker(path, deadline);
            List<Op> operations = new ArrayList<>();
            operations.add(Op.check(markerPath(path), 0));
            for (String child : List.of("writable", "service-info")) {
                Node observed = child.equals("writable") ? writable : service;
                if (observed.code() != Code.NONODE.intValue()) {
                    owned(observed, bookieId);
                    operations.add(Op.delete(path + "/" + child, observed.stat().getVersion()));
                }
            }
            operations.add(Op.delete(markerPath(path), 0));
            requireSession();
            if (backend.multi(operations, deadline) != Code.OK.intValue()) {
                throw new IllegalStateException("Demotion unconfirmed; retain local non-writable state");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Demotion unconfirmed", interrupted);
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("Demotion unconfirmed", timeout);
        }
    }

    private List<Op> conditions(Snapshot authority) {
        List<Op> operations = new ArrayList<>();
        operations.add(Op.check(authority.path() + "/birth-" + authority.envelope().birthId(), 0));
        operations.add(Op.check(authority.path(), authority.stat().getVersion()));
        return operations;
    }

    private Node node(String path, long deadline) throws InterruptedException, TimeoutException {
        Node node = backend.get(path, deadline);
        if (node.code() != Code.OK.intValue() && node.code() != Code.NONODE.intValue()) {
            throw new IllegalStateException("Registration read unavailable");
        }
        return node;
    }

    private Envelope owned(Node node, String bookieId) {
        if (node.code() != Code.OK.intValue() || node.stat() == null
                || node.stat().getEphemeralOwner() != sessionId || node.stat().getVersion() < 0) {
            throw new IllegalStateException("Registration missing or belongs to another session");
        }
        Envelope envelope = ProfileEphemeralCodec.decode(node.bytes());
        if (!envelope.projection().bookieId().equals(bookieId) || envelope.sessionId() != sessionId
                || !envelope.nonce().equals(nonce)) {
            throw new IllegalStateException("Registration scope or publisher mismatch");
        }
        return envelope;
    }

    private void marker(String path, long deadline) throws InterruptedException, TimeoutException {
        Node node = node(markerPath(path), deadline);
        if (node.code() != Code.OK.intValue() || node.stat().getEphemeralOwner() != sessionId
                || node.stat().getVersion() != 0 || node.bytes() == null || node.bytes().length != 0) {
            throw new IllegalStateException("Publisher marker missing or changed");
        }
    }

    private String markerPath(String path) {
        return path + "/session-" + nonce;
    }

    private void requireSession() {
        if (sessionId == 0 || zk.getSessionId() != sessionId || zk.getState() != ZooKeeper.States.CONNECTED) {
            throw new IllegalStateException("Publisher session is unavailable; local admission must remain closed");
        }
    }

    private void requireScope(String requested) {
        if (!bookieId.equals(requested)) {
            throw new IllegalArgumentException("Wrong publisher Bookie scope");
        }
    }

    private void requirePublishingSession() {
        requireSession();
        if (retired) {
            throw new IllegalStateException("Publisher was demoted; use a new startup attempt");
        }
    }

    private static boolean matches(Snapshot snapshot, ProfileServiceInfoProjection projection) {
        ProfileReadinessRecord record = snapshot.envelope().record();
        return record.state() == ProfileReadinessRecord.State.READY
                && record.bookieId().equals(projection.bookieId())
                && record.storageIncarnation().equals(projection.storageIncarnation())
                && record.localReadinessGeneration().equals(projection.readinessGeneration())
                && snapshot.stat().getMzxid() == projection.persistentStoreVersion()
                && record.capabilityHints().equals(projection.capabilityHints());
    }

    private static int version(Node node) {
        if (node.stat().getVersion() == Integer.MAX_VALUE) {
            throw new IllegalStateException("Service-info version exhausted");
        }
        return node.stat().getVersion();
    }

    private static WritableRegistration writable(ProfileServiceInfoProjection projection) {
        return new WritableRegistration(projection.bookieId(), projection.storageIncarnation(),
                projection.readinessGeneration(), projection.persistentStoreVersion());
    }

    private static PublicationResult result(PublicationStatus status) {
        return new PublicationResult(status);
    }

    private static PublicationResult mutation(int code) {
        if (code == Code.OK.intValue()) {
            return result(PublicationStatus.APPLIED);
        }
        if (code == Code.BADVERSION.intValue() || code == Code.NODEEXISTS.intValue()
                || code == Code.NONODE.intValue()) {
            return result(PublicationStatus.CONFLICT);
        }
        return result(ZKProfileRegistrationStore.knownRejection(code)
                ? PublicationStatus.UNAVAILABLE : PublicationStatus.RESPONSE_LOST);
    }
}

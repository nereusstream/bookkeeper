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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * Experimental ZooKeeper persistent readiness CAS. The caller owns the client and provisioned namespace/ACLs.
 * Invoke on a bounded cold-path caller, never on the ZooKeeper event thread or a DATA append owner.
 * No service-info, ephemeral writable registration, automatic readiness, or adapter retry is supplied here.
 */
public final class ZKProfileRegistrationStore implements ProfileRegistrationStore {
    private final ZooKeeper zk;
    private final String root;
    private final List<ACL> acls;
    private final List<ACL> markerAcls;
    private final long timeoutNanos;

    private record Node(int code, byte[] bytes, Stat stat) { }

    private record Current(String path, ProfileReadinessCodec.Envelope envelope) { }

    public ZKProfileRegistrationStore(ZooKeeper zk, String root, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        PathUtils.validatePath(root);
        if (root.equals("/") || acls.isEmpty() || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid readiness namespace, ACLs or timeout");
        }
        this.root = root;
        this.acls = acls.stream().map(acl -> new ACL(acl.getPerms(),
                new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        markerAcls = this.acls.stream().filter(acl -> (acl.getPerms() & ZooDefs.Perms.READ) != 0)
                .map(acl -> new ACL(ZooDefs.Perms.READ, new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        if (markerAcls.isEmpty()) {
            throw new IllegalArgumentException("Readiness birth marker requires a reader ACL");
        }
        timeoutNanos = timeout.toNanos();
    }

    @Override
    public ReadResult read(String bookieId) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            if (sync(deadline) != Code.OK.intValue()) {
                return ReadResult.unavailable();
            }
            Node node = get(path(bookieId), deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return ReadResult.notFound();
            }
            if (node.code() != Code.OK.intValue()) {
                return ReadResult.unavailable();
            }
            Current current = validate(bookieId, node);
            Node anchor = get(anchor(current), deadline);
            if (anchor.code() != Code.OK.intValue()) {
                return anchor.code() == Code.NONODE.intValue() ? ReadResult.corrupt() : ReadResult.unavailable();
            }
            if (!validAnchor(anchor)) {
                return ReadResult.corrupt();
            }
            return ReadResult.found(new VersionedRecord(node.stat().getMzxid(), current.envelope().record()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ReadResult.unavailable();
        } catch (TimeoutException timeout) {
            return ReadResult.unavailable();
        } catch (IllegalArgumentException invalid) {
            return ReadResult.corrupt();
        }
    }

    @Override
    public CasResult compareAndSet(String bookieId, long expectedStoreVersion,
                                   long expectedReadinessGeneration, ProfileReadinessRecord newRecord) {
        Objects.requireNonNull(newRecord, "newRecord");
        if (!newRecord.bookieId().equals(bookieId) || expectedStoreVersion < NO_STORE_VERSION
                || expectedReadinessGeneration < 0) {
            return new CasResult(CasStatus.CONFLICT);
        }
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            if (sync(deadline) != Code.OK.intValue()) {
                return new CasResult(CasStatus.UNAVAILABLE);
            }
            String path = path(bookieId);
            Node node = get(path, deadline);
            List<Op> operations;
            if (node.code() == Code.NONODE.intValue()) {
                if (expectedStoreVersion != NO_STORE_VERSION
                        || expectedReadinessGeneration != NO_READINESS_GENERATION) {
                    return new CasResult(CasStatus.CONFLICT);
                }
                ProfileReadinessCodec.Envelope envelope =
                        new ProfileReadinessCodec.Envelope(UUID.randomUUID(), newRecord);
                operations = List.of(Op.create(path, ProfileReadinessCodec.encode(envelope), acls,
                                CreateMode.PERSISTENT),
                        Op.create(path + "/birth-" + envelope.birthId(), new byte[0], markerAcls,
                                CreateMode.PERSISTENT));
            } else if (node.code() == Code.OK.intValue()) {
                Current current = validate(bookieId, node);
                if (expectedStoreVersion != node.stat().getMzxid()
                        || expectedReadinessGeneration
                        != current.envelope().record().localReadinessGeneration().value()) {
                    return new CasResult(CasStatus.CONFLICT);
                }
                if (node.stat().getVersion() == Integer.MAX_VALUE) {
                    return new CasResult(CasStatus.UNAVAILABLE);
                }
                Node marker = get(anchor(current), deadline);
                if (marker.code() != Code.OK.intValue() || !validAnchor(marker)) {
                    return new CasResult(CasStatus.UNAVAILABLE);
                }
                operations = List.of(Op.check(anchor(current), 0),
                        Op.setData(path, ProfileReadinessCodec.encode(new ProfileReadinessCodec.Envelope(
                                current.envelope().birthId(), newRecord)), node.stat().getVersion()));
            } else {
                return new CasResult(CasStatus.UNAVAILABLE);
            }
            requireTime(deadline);
            CompletableFuture<Integer> reply = new CompletableFuture<>();
            submitted = true;
            zk.multi(operations, (code, ignored, context, results) -> reply.complete(code), null);
            int result = await(reply, deadline);
            if (result == Code.OK.intValue()) {
                return new CasResult(CasStatus.APPLIED);
            }
            if (result == Code.BADVERSION.intValue() || result == Code.NODEEXISTS.intValue()
                    || result == Code.NONODE.intValue()) {
                return new CasResult(CasStatus.CONFLICT);
            }
            return new CasResult(knownRejection(result) ? CasStatus.UNAVAILABLE : CasStatus.RESPONSE_LOST);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CasResult(submitted ? CasStatus.RESPONSE_LOST : CasStatus.UNAVAILABLE);
        } catch (TimeoutException timeout) {
            return new CasResult(submitted ? CasStatus.RESPONSE_LOST : CasStatus.UNAVAILABLE);
        } catch (IllegalArgumentException invalid) {
            return new CasResult(CasStatus.UNAVAILABLE);
        }
    }

    private Current validate(String bookieId, Node node) {
        ProfileReadinessCodec.Envelope envelope = ProfileReadinessCodec.decode(node.bytes());
        if (node.stat() == null || node.stat().getEphemeralOwner() != 0 || node.stat().getVersion() < 0
                || node.stat().getMzxid() < 0 || !envelope.record().bookieId().equals(bookieId)) {
            throw new IllegalArgumentException("Invalid persistent readiness scope or version");
        }
        return new Current(path(bookieId), envelope);
    }

    private static boolean validAnchor(Node anchor) {
        return anchor.bytes() != null && anchor.bytes().length == 0 && anchor.stat() != null
                && anchor.stat().getVersion() == 0 && anchor.stat().getEphemeralOwner() == 0;
    }

    private static String anchor(Current current) {
        return current.path() + "/birth-" + current.envelope().birthId();
    }

    private String path(String bookieId) {
        byte[] name = Objects.requireNonNull(bookieId, "bookieId").getBytes(StandardCharsets.UTF_8);
        if (name.length < 1 || name.length > BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES) {
            throw new IllegalArgumentException("Invalid readiness scope length");
        }
        return root + "/" + Base64.getUrlEncoder().withoutPadding().encodeToString(name);
    }

    private Node get(String path, long deadline) throws InterruptedException, TimeoutException {
        requireTime(deadline);
        CompletableFuture<Node> reply = new CompletableFuture<>();
        zk.getData(path, false, (code, ignored, context, bytes, stat) ->
                reply.complete(new Node(code, bytes, stat)), null);
        return await(reply, deadline);
    }

    private int sync(long deadline) throws InterruptedException, TimeoutException {
        requireTime(deadline);
        CompletableFuture<Integer> reply = new CompletableFuture<>();
        zk.sync(root, (code, ignored, context) -> reply.complete(code), null);
        return await(reply, deadline);
    }

    private static void requireTime(long deadline) throws TimeoutException {
        if (deadline - System.nanoTime() <= 0) {
            throw new TimeoutException("Readiness operation deadline elapsed");
        }
    }

    private static <T> T await(CompletableFuture<T> reply, long deadline)
            throws InterruptedException, TimeoutException {
        try {
            return reply.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException impossible) {
            throw new IllegalStateException("Readiness callback completed exceptionally", impossible);
        }
    }

    private static boolean knownRejection(int result) {
        return result == Code.NOAUTH.intValue() || result == Code.INVALIDACL.intValue()
                || result == Code.BADARGUMENTS.intValue() || result == Code.UNIMPLEMENTED.intValue();
    }
}

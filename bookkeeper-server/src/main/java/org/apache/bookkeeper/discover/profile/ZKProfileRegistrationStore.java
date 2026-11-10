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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
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
 * No service-info publication, ephemeral writable mutation, automatic readiness, or adapter retry is supplied here.
 */
public final class ZKProfileRegistrationStore implements ProfileRegistrationStore {
    private final ZooKeeper zk;
    private final ProfileZKReadClient reads;
    private final String root;
    private final List<ACL> acls;
    private final List<ACL> markerAcls;
    private final long timeoutNanos;

    record Node(int code, byte[] bytes, Stat stat) { }

    record Snapshot(String path, ProfileReadinessCodec.Envelope envelope, Stat stat) { }

    private record Current(String path, ProfileReadinessCodec.Envelope envelope) { }

    Snapshot snapshot(String bookieId, long deadline) throws InterruptedException, TimeoutException {
        if (sync(deadline) != Code.OK.intValue()) {
            throw new IllegalStateException("Readiness sync unavailable");
        }
        Node node = get(path(bookieId), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Readiness read unavailable");
        }
        Current current = validate(bookieId, node);
        Node marker = get(anchor(current), deadline);
        if (marker.code() == Code.NONODE.intValue() || marker.code() == Code.OK.intValue() && !validAnchor(marker)) {
            throw new IllegalArgumentException("Invalid readiness birth marker");
        }
        if (marker.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Readiness birth read unavailable");
        }
        return new Snapshot(current.path(), current.envelope(), node.stat());
    }

    /** Cold discovery observation, never transport authentication or ledger mutation authority. */
    public static final class RegisteredPeer {
        private final ProfileServiceInfoProjection projection;
        private final ProfileReadinessRecord readiness;
        private final PeerIdentity identity;

        private RegisteredPeer(ProfileServiceInfoProjection projection, ProfileReadinessRecord readiness,
                PeerIdentity identity) {
            this.projection = projection;
            this.readiness = readiness;
            this.identity = identity;
        }

        public ProfileServiceInfoProjection projection() {
            return projection;
        }

        public ProfileReadinessRecord readiness() {
            return readiness;
        }

        public long publisherSession() {
            return identity.service().owner();
        }

        byte[] reconstructionIdentity() {
            var out = java.nio.ByteBuffer.allocate(32 + 4 * 28);
            out.putLong(identity.readinessBirth().getMostSignificantBits())
                    .putLong(identity.readinessBirth().getLeastSignificantBits())
                    .putLong(identity.nonce().getMostSignificantBits())
                    .putLong(identity.nonce().getLeastSignificantBits());
            for (var node : List.of(identity.readiness(), identity.service(), identity.writable(), identity.marker())) {
                out.putLong(node.birth()).putLong(node.modified()).putInt(node.version()).putLong(node.owner());
            }
            return out.array();
        }
    }

    public record PeerReadResult(ReadStatus status, RegisteredPeer peer) {
        public PeerReadResult {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (peer != null)) {
                throw new IllegalArgumentException("Only a verified registration observation carries a peer");
            }
        }
    }

    private record NodeIdentity(long birth, long modified, int version, long owner) {
        private static NodeIdentity of(Stat stat) {
            return new NodeIdentity(stat.getCzxid(), stat.getMzxid(), stat.getVersion(), stat.getEphemeralOwner());
        }
    }

    private record PeerIdentity(UUID readinessBirth, NodeIdentity readiness, UUID nonce,
            NodeIdentity service, NodeIdentity writable, NodeIdentity marker) { }

    /** Observe another publisher through this reader's client, with a final cold identity check.
     * No enumeration, mutation, watch, automatic retry or same-ID rejoin is performed. */
    public PeerReadResult readPeer(String bookieId) {
        return readPeer(bookieId, System.nanoTime() + timeoutNanos);
    }

    PeerReadResult readPeer(String bookieId, long callerDeadline) {
        long deadline = Math.min(callerDeadline, System.nanoTime() + timeoutNanos);
        long readerSession = zk.getSessionId();
        try {
            if (readerSession == 0 || zk.getState() != ZooKeeper.States.CONNECTED) {
                return new PeerReadResult(ReadStatus.UNAVAILABLE, null);
            }
            var authority = snapshot(bookieId, deadline);
            if (authority == null || authority.envelope().record().state() != ProfileReadinessRecord.State.READY) {
                return new PeerReadResult(ReadStatus.NOT_FOUND, null);
            }
            var service = get(authority.path() + "/service-info", deadline);
            var writable = get(authority.path() + "/writable", deadline);
            if (service.code() == Code.NONODE.intValue() || writable.code() == Code.NONODE.intValue()) {
                return new PeerReadResult(ReadStatus.NOT_FOUND, null);
            }
            var envelope = peerEnvelope(service, bookieId);
            var registered = peerEnvelope(writable, bookieId);
            if (!envelope.equals(registered) || !matchesPeer(authority, envelope.projection())) {
                return new PeerReadResult(ReadStatus.UNAVAILABLE, null);
            }
            String markerPath = authority.path() + "/session-" + envelope.nonce();
            var marker = get(markerPath, deadline);
            if (marker.code() == Code.NONODE.intValue()) {
                return new PeerReadResult(ReadStatus.NOT_FOUND, null);
            }
            requirePeerMarker(marker, envelope.sessionId());
            var after = snapshot(bookieId, deadline);
            if (after == null || !authority.envelope().equals(after.envelope())
                    || !NodeIdentity.of(authority.stat()).equals(NodeIdentity.of(after.stat()))
                    || !samePeerNode(service, get(authority.path() + "/service-info", deadline))
                    || !samePeerNode(writable, get(authority.path() + "/writable", deadline))
                    || !samePeerNode(marker, get(markerPath, deadline))
                    || zk.getState() != ZooKeeper.States.CONNECTED || zk.getSessionId() != readerSession) {
                return new PeerReadResult(ReadStatus.UNAVAILABLE, null);
            }
            var identity = new PeerIdentity(authority.envelope().birthId(), NodeIdentity.of(authority.stat()),
                    envelope.nonce(), NodeIdentity.of(service.stat()), NodeIdentity.of(writable.stat()),
                    NodeIdentity.of(marker.stat()));
            return new PeerReadResult(ReadStatus.FOUND,
                    new RegisteredPeer(envelope.projection(), authority.envelope().record(), identity));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new PeerReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new PeerReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException corrupt) {
            return new PeerReadResult(ReadStatus.CORRUPT, null);
        }
    }

    /** Reconcile after an external handshake. A replacement observation never silently adopts a new peer. */
    public PeerReadResult revalidatePeer(RegisteredPeer expected) {
        return revalidatePeer(expected, System.nanoTime() + timeoutNanos);
    }

    PeerReadResult revalidatePeer(RegisteredPeer expected, long deadline) {
        Objects.requireNonNull(expected, "expected");
        var current = readPeer(expected.projection().bookieId(), deadline);
        if (current.peer() != null && (!expected.identity.equals(current.peer().identity)
                || !expected.projection().equals(current.peer().projection())
                || !expected.readiness().equals(current.peer().readiness()))) {
            return new PeerReadResult(ReadStatus.NOT_FOUND, null);
        }
        return current;
    }

    void requireManifestClosedClient(ZooKeeper mutationClient) throws IOException {
        if (mutationClient != zk) {
            throw new IOException("Manifest CLOSED sources must borrow the admission ZooKeeper client");
        }
    }

    List<Op> manifestClosedSourceChecks(RegisteredPeer peer) {
        String path = path(peer.projection().bookieId());
        var identity = peer.identity;
        return List.of(Op.check(path, identity.readiness().version()),
                Op.check(path + "/birth-" + identity.readinessBirth(), 0),
                Op.check(path + "/service-info", identity.service().version()),
                Op.check(path + "/writable", identity.writable().version()),
                Op.check(path + "/session-" + identity.nonce(), identity.marker().version()));
    }

    /** Bounded persistent cohort watch. Events only request a cold audit; they do not classify peers. */
    public AutoCloseable watchPeers(List<String> bookies, Runnable changed)
            throws org.apache.zookeeper.KeeperException, InterruptedException {
        List<String> peers = List.copyOf(bookies);
        if (peers.isEmpty() || peers.size() > 64 || peers.stream().distinct().count() != peers.size()) {
            throw new IllegalArgumentException("Profile watch requires a bounded unique cohort");
        }
        List<String> paths = peers.stream().map(this::path).toList();
        var stopped = new java.util.concurrent.atomic.AtomicBoolean();
        var watched = new java.util.ArrayList<String>();
        org.apache.zookeeper.Watcher watcher = event -> {
            if (!stopped.get() && event.getType()
                    != org.apache.zookeeper.Watcher.Event.EventType.PersistentWatchRemoved) {
                changed.run();
            }
        };
        AutoCloseable retired = () -> {
            stopped.set(true);
            // Retire only this observer; actual session ownership remains with the service SDK.
            watched.forEach(path -> zk.removeWatches(path, watcher, org.apache.zookeeper.Watcher.WatcherType.Any,
                    true, (rc, removed, context) -> { }, null));
        };
        try {
            for (String path : paths) {
                zk.addWatch(path, watcher, org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE);
                watched.add(path);
            }
        } catch (org.apache.zookeeper.KeeperException | InterruptedException failed) {
            stopped.set(true);
            watched.forEach(path -> zk.removeWatches(path, watcher, org.apache.zookeeper.Watcher.WatcherType.Any,
                    true, (rc, removed, context) -> { }, null));
            throw failed;
        }
        return retired;
    }

    private static boolean matchesPeer(Snapshot authority, ProfileServiceInfoProjection projection) {
        var record = authority.envelope().record();
        return record.bookieId().equals(projection.bookieId())
                && record.storageIncarnation().equals(projection.storageIncarnation())
                && record.localReadinessGeneration().equals(projection.readinessGeneration())
                && authority.stat().getMzxid() == projection.persistentStoreVersion()
                && record.capabilityHints().equals(projection.capabilityHints());
    }

    private static ProfileEphemeralCodec.Envelope peerEnvelope(Node node, String bookieId) {
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Peer registration unavailable");
        }
        if (node.stat() == null || node.stat().getVersion() < 0 || node.stat().getEphemeralOwner() == 0) {
            throw new IllegalArgumentException("Peer registration must be ephemeral");
        }
        var envelope = ProfileEphemeralCodec.decode(node.bytes());
        if (!envelope.projection().bookieId().equals(bookieId)
                || envelope.sessionId() != node.stat().getEphemeralOwner()) {
            throw new IllegalArgumentException("Peer registration publisher or scope mismatch");
        }
        return envelope;
    }

    private static void requirePeerMarker(Node marker, long owner) {
        if (marker.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Peer session marker unavailable");
        }
        if (marker.stat() == null || marker.stat().getVersion() != 0 || marker.stat().getEphemeralOwner() != owner
                || marker.bytes() == null || marker.bytes().length != 0) {
            throw new IllegalArgumentException("Peer session marker changed or belongs to another publisher");
        }
    }

    private static boolean samePeerNode(Node before, Node after) {
        return after.code() == Code.OK.intValue() && after.stat() != null
                && NodeIdentity.of(before.stat()).equals(NodeIdentity.of(after.stat()))
                && Arrays.equals(before.bytes(), after.bytes());
    }

    List<ACL> publicationAcls() {
        return acls;
    }

    List<ACL> publicationMarkerAcls() {
        return markerAcls;
    }

    int multi(List<Op> operations, long deadline) throws InterruptedException, TimeoutException {
        return multi(operations, deadline, null);
    }

    int multi(List<Op> operations, long deadline, ProfileZKReadClient.Pending pending)
            throws InterruptedException, TimeoutException {
        requireTime(deadline);
        return submitMulti(operations, deadline, pending);
    }

    private int submitMulti(List<Op> operations, long deadline, ProfileZKReadClient.Pending pending)
            throws InterruptedException, TimeoutException {
        CompletableFuture<Integer> reply = new CompletableFuture<>();
        if (pending != null) {
            pending.submitted();
        }
        zk.multi(operations, (code, ignored, context, results) -> {
            try {
                reply.complete(code);
            } finally {
                if (pending != null) {
                    pending.returned();
                }
            }
        }, null);
        return await(reply, deadline);
    }

    public ZKProfileRegistrationStore(ZooKeeper zk, String root, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        reads = new ProfileZKReadClient(zk);
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

    boolean samePublisherContext(ZKProfileRegistrationStore other) {
        return other != null && other.zk == zk && other.root.equals(root);
    }

    @Override
    public ReadResult read(String bookieId) {
        return read(bookieId, System.nanoTime() + timeoutNanos);
    }

    ReadResult read(String bookieId, long deadline) {
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
        return compareAndSet(bookieId, expectedStoreVersion, expectedReadinessGeneration, newRecord,
                System.nanoTime() + timeoutNanos, null);
    }

    CasResult compareAndSet(String bookieId, long expectedStoreVersion, long expectedReadinessGeneration,
            ProfileReadinessRecord newRecord, long deadline, ProfileZKReadClient.Pending pending) {
        Objects.requireNonNull(newRecord, "newRecord");
        if (!newRecord.bookieId().equals(bookieId) || expectedStoreVersion < NO_STORE_VERSION
                || expectedReadinessGeneration < 0) {
            return new CasResult(CasStatus.CONFLICT);
        }
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
            submitted = true;
            int result = submitMulti(operations, deadline, pending);
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

    String path(String bookieId) {
        byte[] name = Objects.requireNonNull(bookieId, "bookieId").getBytes(StandardCharsets.UTF_8);
        if (name.length < 1 || name.length > BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES) {
            throw new IllegalArgumentException("Invalid readiness scope length");
        }
        return root + "/" + Base64.getUrlEncoder().withoutPadding().encodeToString(name);
    }

    Node get(String path, long deadline) throws InterruptedException, TimeoutException {
        ProfileZKReadClient.Node node = reads.get(path, deadline);
        return new Node(node.code(), node.bytes(), node.stat());
    }

    private int sync(long deadline) throws InterruptedException, TimeoutException {
        return reads.sync(root, deadline);
    }

    static void requireTime(long deadline) throws TimeoutException {
        ProfileZKReadClient.requireTime(deadline);
    }

    private static <T> T await(CompletableFuture<T> reply, long deadline)
            throws InterruptedException, TimeoutException {
        return ProfileZKReadClient.await(reply, deadline);
    }

    static boolean knownRejection(int result) {
        return result == Code.NOAUTH.intValue() || result == Code.INVALIDACL.intValue()
                || result == Code.BADARGUMENTS.intValue() || result == Code.UNIMPLEMENTED.intValue();
    }
}

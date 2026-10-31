/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.BookieRouteBinding.Observation;
import org.apache.bookkeeper.discover.BookieRouteBinding.Phase;
import org.apache.bookkeeper.discover.BookieRouteBinding.ReadResult;
import org.apache.bookkeeper.discover.BookieRouteBinding.ReadStatus;
import org.apache.bookkeeper.discover.BookieRouteBinding.WriteResult;
import org.apache.bookkeeper.discover.BookieRouteBinding.WriteStatus;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/** Fixed native namespace on the existing metadata connection; cold operations only, never an Add-path store. */
public final class ZKBookieRouteBindingStore {
    /** Fresh original values only, not a future common binding, launch permit or isolation proof. */
    public record ClassicLaunchSnapshot(String ledgersRoot, BookieId bookie, String clusterId,
            long clusterBirthZxid, BookieRouteBinding.SourceCookie cookieIdentity, byte[] cookieBytes) {
        public ClassicLaunchSnapshot {
            cookieBytes = cookieBytes.clone();
        }

        @Override
        public byte[] cookieBytes() {
            return cookieBytes.clone();
        }
    }

    public record ClassicLaunchDrain(boolean drained, int pendingCallbacks, Throwable firstReadFailure) { }

    /** Reads on this exact manager client/namespace; the enclosing episode still owns the client. */
    public final class ClassicLaunchSources {
        private final BookieId bookie;
        private final long readTimeoutNanos;
        private final ProfileZKReadClient.Pending pending = new ProfileZKReadClient.Pending();
        private final ProfileZKReadClient reader = new ProfileZKReadClient(zk, pending);
        private boolean closing;
        private Thread readingThread;
        private Throwable firstReadFailure;

        private ClassicLaunchSources(BookieId bookie, Duration timeout) {
            this.bookie = Objects.requireNonNull(bookie, "bookie");
            path(bookie);
            if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("Bounded Classic launch read timeout required");
            }
            readTimeoutNanos = timeout.toNanos();
        }

        /** Every invocation rechecks native absence and original source bytes; never provisions or registers. */
        public synchronized ClassicLaunchSnapshot read() throws IOException {
            return read(0, 0, false, 0, true);
        }

        /** Final cold observation after this raw session registered; no registration or permission is issued. */
        public synchronized ClassicLaunchSnapshot readForServing(long session, long birth, boolean readOnly)
                throws IOException {
            if (session == 0 || birth <= 0 || zk.getClass() != ZooKeeper.class
                    || zk.getSessionId() != session || !zk.getState().isAlive()) {
                throw new IOException("Original raw Classic registration session is unavailable");
            }
            return read(session, birth, readOnly, session, true);
        }

        /** Independent raw observer of the child's advertised registration. Data only, never a launch permit. */
        public synchronized ClassicLaunchSnapshot readForObservedRegistration(long session, long birth,
                boolean readOnly) throws IOException {
            long observer = zk.getSessionId();
            if (session == 0 || birth <= 0 || zk.getClass() != ZooKeeper.class || observer == 0
                    || observer == session || !zk.getState().isAlive()) {
                throw new IOException("Independent original raw registration observer is unavailable");
            }
            return read(session, birth, readOnly, observer, true);
        }

        /** Post-exit source/registration data only. Existing commissioning is allowed, never adopted or modified. */
        public synchronized ClassicLaunchSnapshot readAfterServiceStopped() throws IOException {
            if (zk.getClass() != ZooKeeper.class || zk.getSessionId() == 0 || !zk.getState().isAlive()) {
                throw new IOException("Original raw stopped-service observer is unavailable");
            }
            return read(0, 0, false, 0, false);
        }

        private ClassicLaunchSnapshot read(long session, long birth, boolean readOnly, long observerSession,
                boolean nativeAbsent)
                throws IOException {
            if (closing || readingThread != null || firstReadFailure != null) {
                throw new IOException("Classic launch sources are closing or failed", firstReadFailure);
            }
            readingThread = Thread.currentThread();
            try {
                long deadline = System.nanoTime() + readTimeoutNanos;
                if (nativeAbsent) {
                    requireAbsent(deadline);
                }
                var cluster = reader.get(instancePath, deadline);
                if (cluster.code() != Code.OK.intValue() || cluster.stat() == null
                        || cluster.stat().getEphemeralOwner() != 0 || cluster.stat().getVersion() != 0
                        || cluster.stat().getCzxid() <= 0 || cluster.bytes() == null
                        || cluster.bytes().length < 1 || cluster.bytes().length > 1024) {
                    throw new IOException("Original Classic cluster source is missing, invalid or unavailable");
                }
                String clusterId = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(cluster.bytes())).toString();
                if (clusterId.isBlank() || clusterId.equals("null") || clusterId.indexOf(0) >= 0) {
                    throw new IOException("Original Classic cluster identity is invalid");
                }
                var cookie = reader.get(cookieRoot + "/" + bookie, deadline);
                if (cookie.code() != Code.OK.intValue() || cookie.stat() == null
                        || cookie.stat().getEphemeralOwner() != 0 || cookie.stat().getCzxid() <= 0
                        || cookie.bytes() == null || cookie.bytes().length < 1
                        || cookie.bytes().length > MAX_COOKIE_BYTES) {
                    throw new IOException("Original Classic Cookie is missing, invalid or unavailable");
                }
                var source = new BookieRouteBinding.SourceCookie(cookie.stat().getCzxid(), cookie.stat().getVersion(),
                        sha256(cookie.bytes()));
                String prefix = ledgersRoot.equals("/") ? "" : ledgersRoot;
                List<String> registrations = List.of(prefix + "/available/" + bookie,
                        prefix + "/available/readonly/" + bookie);
                for (int i = 0; i < registrations.size(); i++) {
                    var result = reader.exists(registrations.get(i), deadline);
                    if (session != 0 && i == (readOnly ? 1 : 0)) {
                        if (result.code() != Code.OK.intValue() || result.stat() == null
                                || result.stat().getEphemeralOwner() != session || result.stat().getCzxid() != birth
                                || result.stat().getVersion() != 0 || zk.getSessionId() != observerSession
                                || !zk.getState().isAlive()) {
                            throw new IOException("Classic service registration is not its original session/birth");
                        }
                    } else if (result.code() != Code.NONODE.intValue()) {
                        throw new IOException("Classic launch other registration absence is not established");
                    }
                }
                if (nativeAbsent) {
                    requireAbsent(deadline);
                }
                return new ClassicLaunchSnapshot(ledgersRoot, bookie, clusterId, cluster.stat().getCzxid(),
                        source, cookie.bytes());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                var failure = new IOException("Classic launch source read interrupted", interrupted);
                firstReadFailure = failure;
                throw failure;
            } catch (TimeoutException unavailable) {
                var failure = new IOException("Classic launch source deadline elapsed", unavailable);
                firstReadFailure = failure;
                throw failure;
            } catch (IOException | RuntimeException | Error failure) {
                firstReadFailure = failure;
                throw failure;
            } finally {
                readingThread = null;
            }
        }

        private void requireAbsent(long deadline) throws IOException {
            var nativeRead = ZKBookieRouteBindingStore.this.read(bookie, deadline, reader);
            if (nativeRead.status() != ReadStatus.ABSENT) {
                throw new IOException("Classic launch requires native ABSENT: " + nativeRead.status());
            }
        }

        /** Actual submitted callbacks remain owned after a caller timeout/interruption or unknown submission. */
        public synchronized ClassicLaunchDrain closeAndDrain() {
            if (readingThread == Thread.currentThread()) {
                throw new IllegalStateException("Classic launch source reader cannot drain itself");
            }
            closing = true;
            int remaining = pending.stop();
            return new ClassicLaunchDrain(remaining == 0, remaining, firstReadFailure);
        }
    }

    private static final long MAGIC = 0x555742524e303031L; // UWBRN001
    private static final int MAX_COOKIE_BYTES = 1024 * 1024;
    private final ZooKeeper zk;
    private final ProfileZKReadClient reads;
    private final String ledgersRoot;
    private final String root;
    private final String instancePath;
    private final String cookieRoot;
    private final List<ACL> acls;
    private final List<ACL> markerAcls;
    private final long timeoutNanos;

    public ZKBookieRouteBindingStore(ZooKeeper zk, String ledgersRoot, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        this.reads = new ProfileZKReadClient(zk);
        PathUtils.validatePath(ledgersRoot);
        this.ledgersRoot = ledgersRoot;
        String prefix = ledgersRoot.equals("/") ? "" : ledgersRoot;
        root = prefix + "/bookie-route-bindings";
        instancePath = prefix + "/INSTANCEID";
        cookieRoot = prefix + "/cookies";
        if (acls.isEmpty() || timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid native common binding ACLs or deadline");
        }
        this.acls = acls.stream().map(a -> new ACL(a.getPerms(), new Id(a.getId().getScheme(), a.getId().getId())))
                .toList();
        markerAcls = this.acls.stream().filter(a -> (a.getPerms() & org.apache.zookeeper.ZooDefs.Perms.READ) != 0)
                .map(a -> new ACL(org.apache.zookeeper.ZooDefs.Perms.READ,
                        new Id(a.getId().getScheme(), a.getId().getId()))).toList();
        if (markerAcls.isEmpty()) {
            throw new IllegalArgumentException("Native common binding birth marker requires reader ACL");
        }
        timeoutNanos = timeout.toNanos();
    }

    public ReadResult read(BookieId bookie) {
        return read(bookie, System.nanoTime() + timeoutNanos, reads);
    }

    /** Heap-only preparation, bound to this store's actual source client and fixed native namespace. */
    public ClassicLaunchSources borrowClassicLaunchSources(BookieId bookie, Duration timeout) {
        return new ClassicLaunchSources(bookie, timeout);
    }

    ReadResult read(BookieId bookie, long deadline, ProfileZKReadClient reader) {
        try {
            return snapshot(bookie, deadline, reader);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ReadResult.status(ReadStatus.UNAVAILABLE, "Interrupted native binding query");
        } catch (TimeoutException unavailable) {
            return ReadResult.status(ReadStatus.UNAVAILABLE, "Native binding query deadline elapsed");
        } catch (BookieRouteBinding.UnsupportedSchemaException incompatible) {
            return ReadResult.status(ReadStatus.INCOMPATIBLE, incompatible.getMessage());
        } catch (IllegalArgumentException corrupt) {
            return ReadResult.status(ReadStatus.CORRUPT, corrupt.getMessage());
        }
    }

    void requireSourceClient(ZooKeeper client, String expectedLedgersRoot) {
        if (zk != client || !ledgersRoot.equals(expectedLedgersRoot)) {
            throw new IllegalArgumentException("Classic bootstrap native client/namespace mismatch");
        }
    }

    /** Only explicit commissioning may create the fixed namespace. Existing records are never replaced. */
    public WriteResult prepare(BookieRouteBinding binding) {
        Objects.requireNonNull(binding, "binding");
        if (binding.phase() != Phase.PREPARED) {
            throw new IllegalArgumentException("Commissioning must first persist PREPARED");
        }
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            ReadResult existing = snapshot(binding.bookieId(), deadline);
            if (existing.status() == ReadStatus.FOUND) {
                var prior = existing.observation().orElseThrow();
                return prior.binding().prepared().equals(binding)
                        ? WriteResult.observed(WriteStatus.ALREADY_APPLIED, prior)
                        : WriteResult.status(WriteStatus.CONFLICT, "Bookie scope already has another binding");
            }
            if (existing.status() != ReadStatus.ABSENT) {
                return unreadable(existing);
            }
            SourceCheck source = sourceChecks(binding, deadline);
            if (source.failure() != null) {
                return source.failure();
            }
            CompletableFuture<Integer> namespace = new CompletableFuture<>();
            ProfileZKReadClient.requireTime(deadline);
            zk.create(root, new byte[0], acls, CreateMode.PERSISTENT,
                    (code, ignored, context, name) -> namespace.complete(code), null);
            int created = ProfileZKReadClient.await(namespace, deadline);
            if (created != Code.OK.intValue() && created != Code.NODEEXISTS.intValue()) {
                return WriteResult.status(WriteStatus.UNAVAILABLE, "Native binding namespace unavailable");
            }
            var parent = reads.get(root, deadline);
            if (parent.code() != Code.OK.intValue() || parent.stat() == null
                    || parent.stat().getEphemeralOwner() != 0 || parent.stat().getVersion() != 0
                    || parent.bytes() == null || parent.bytes().length != 0) {
                return WriteResult.status(WriteStatus.INCOMPATIBLE, "Invalid native binding namespace");
            }
            UUID birth = UUID.randomUUID();
            var operations = new ArrayList<>(source.operations());
            operations.add(Op.create(path(binding.bookieId()), encode(birth, binding), acls, CreateMode.PERSISTENT));
            operations.add(Op.create(marker(binding.bookieId(), birth), new byte[0], markerAcls,
                    CreateMode.PERSISTENT));
            var reply = new CompletableFuture<Integer>();
            ProfileZKReadClient.requireTime(deadline);
            submitted = true;
            zk.multi(operations, (code, ignored, context, results) -> reply.complete(code), null);
            int code = ProfileZKReadClient.await(reply, deadline);
            if (code != Code.OK.intValue()) {
                return rejected(code);
            }
            return confirmed(binding, birth, 0, snapshot(binding.bookieId(), deadline), WriteStatus.APPLIED);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return WriteResult.status(submitted ? WriteStatus.RESPONSE_LOST : WriteStatus.UNAVAILABLE,
                    "Interrupted native binding commissioning");
        } catch (TimeoutException unavailable) {
            return WriteResult.status(submitted ? WriteStatus.RESPONSE_LOST : WriteStatus.UNAVAILABLE,
                    "Native binding commissioning deadline elapsed");
        } catch (IllegalArgumentException corrupt) {
            return WriteResult.status(WriteStatus.INCOMPATIBLE, corrupt.getMessage());
        }
    }

    public WriteResult complete(Observation expected, BookieRouteBinding.BootstrapSeal seal) {
        Objects.requireNonNull(expected, "expected native binding");
        Objects.requireNonNull(seal, "actual control seal");
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            ReadResult read = snapshot(expected.binding().bookieId(), deadline);
            if (read.status() != ReadStatus.FOUND) {
                return unreadable(read);
            }
            Observation current = read.observation().orElseThrow();
            if (!current.birthId().equals(expected.birthId()) || current.birthZxid() != expected.birthZxid()
                    || !current.binding().prepared().equals(expected.binding().prepared())) {
                return WriteResult.status(WriteStatus.CONFLICT, "Native common binding physical identity changed");
            }
            BookieRouteBinding enabled = expected.binding().enabled(seal);
            if (current.binding().phase() == Phase.ENABLED) {
                return current.binding().equals(enabled)
                        ? WriteResult.observed(WriteStatus.ALREADY_APPLIED, current)
                        : WriteResult.status(WriteStatus.CONFLICT, "Native common binding has another seal");
            }
            if (current.version() != expected.version() || current.modifiedZxid() != expected.modifiedZxid()) {
                return WriteResult.status(WriteStatus.CONFLICT, "Native common binding version changed");
            }
            SourceCheck source = sourceChecks(enabled, deadline);
            if (source.failure() != null) {
                return source.failure();
            }
            var operations = new ArrayList<>(source.operations());
            operations.add(Op.check(marker(enabled.bookieId(), current.birthId()), 0));
            operations.add(Op.setData(path(enabled.bookieId()), encode(current.birthId(), enabled), current.version()));
            var reply = new CompletableFuture<Integer>();
            ProfileZKReadClient.requireTime(deadline);
            submitted = true;
            zk.multi(operations, (code, ignored, context, results) -> reply.complete(code), null);
            int code = ProfileZKReadClient.await(reply, deadline);
            if (code != Code.OK.intValue()) {
                return rejected(code);
            }
            return confirmed(enabled, current.birthId(), current.birthZxid(),
                    snapshot(enabled.bookieId(), deadline), WriteStatus.APPLIED);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return WriteResult.status(submitted ? WriteStatus.RESPONSE_LOST : WriteStatus.UNAVAILABLE,
                    "Interrupted native binding completion");
        } catch (TimeoutException unavailable) {
            return WriteResult.status(submitted ? WriteStatus.RESPONSE_LOST : WriteStatus.UNAVAILABLE,
                    "Native binding completion deadline elapsed");
        } catch (IllegalArgumentException corrupt) {
            return WriteResult.status(WriteStatus.INCOMPATIBLE, corrupt.getMessage());
        }
    }

    private ReadResult snapshot(BookieId bookie, long deadline) throws InterruptedException, TimeoutException {
        return snapshot(bookie, deadline, reads);
    }

    private ReadResult snapshot(BookieId bookie, long deadline, ProfileZKReadClient reader)
            throws InterruptedException, TimeoutException {
        if (reader.sync(ledgersRoot, deadline) != Code.OK.intValue()) {
            return ReadResult.status(ReadStatus.UNAVAILABLE, "Native binding sync unavailable");
        }
        var node = reader.get(path(bookie), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return ReadResult.status(ReadStatus.ABSENT, "");
        }
        if (node.code() != Code.OK.intValue()) {
            return ReadResult.status(ReadStatus.UNAVAILABLE, "Native binding read unavailable");
        }
        Envelope envelope = decode(node.bytes());
        var binding = envelope.binding();
        if (!binding.bookieId().equals(bookie)) {
            return ReadResult.status(ReadStatus.INCOMPATIBLE, "Native binding belongs to another Bookie");
        }
        var stat = node.stat();
        if (stat == null || stat.getEphemeralOwner() != 0) {
            return ReadResult.status(ReadStatus.CORRUPT, "Invalid native common binding node");
        }
        var anchor = reader.get(marker(bookie, envelope.birth()), deadline);
        if (anchor.code() == Code.NONODE.intValue()) {
            return ReadResult.status(ReadStatus.CORRUPT, "Required native binding birth marker is missing");
        }
        if (anchor.code() != Code.OK.intValue()) {
            return ReadResult.status(ReadStatus.UNAVAILABLE, "Native binding birth marker unavailable");
        }
        if (anchor.stat() == null || anchor.stat().getEphemeralOwner() != 0 || anchor.stat().getVersion() != 0
                || anchor.stat().getCzxid() != stat.getCzxid()
                || anchor.bytes() == null || anchor.bytes().length != 0) {
            return ReadResult.status(ReadStatus.CORRUPT, "Invalid native binding birth marker");
        }
        SourceCheck source = sourceChecks(binding, deadline, reader);
        if (source.failure() != null) {
            return ReadResult.status(source.failure().status() == WriteStatus.UNAVAILABLE
                    ? ReadStatus.UNAVAILABLE : ReadStatus.INCOMPATIBLE, source.failure().detail());
        }
        return ReadResult.found(new Observation(binding, envelope.birth(), stat.getCzxid(),
                stat.getMzxid(), stat.getVersion()));
    }

    private record SourceCheck(List<Op> operations, WriteResult failure) { }

    private SourceCheck sourceChecks(BookieRouteBinding binding, long deadline)
            throws InterruptedException, TimeoutException {
        return sourceChecks(binding, deadline, reads);
    }

    private SourceCheck sourceChecks(BookieRouteBinding binding, long deadline, ProfileZKReadClient reader)
            throws InterruptedException, TimeoutException {
        var cluster = reader.get(instancePath, deadline);
        if (cluster.code() != Code.OK.intValue()) {
            return new SourceCheck(List.of(), WriteResult.status(cluster.code() == Code.NONODE.intValue()
                    ? WriteStatus.INCOMPATIBLE : WriteStatus.UNAVAILABLE, "Native cluster source unavailable"));
        }
        if (!matchesCluster(cluster, binding)) {
            return new SourceCheck(List.of(), WriteResult.status(WriteStatus.CONFLICT, "Cluster source changed"));
        }
        String cookiePath = cookieRoot + "/" + binding.bookieId();
        var cookie = reader.get(cookiePath, deadline);
        if (cookie.code() != Code.OK.intValue()) {
            return new SourceCheck(List.of(), WriteResult.status(cookie.code() == Code.NONODE.intValue()
                    ? WriteStatus.CONFLICT : WriteStatus.UNAVAILABLE, "Original Cookie source unavailable"));
        }
        var source = binding.sourceCookie();
        if (cookie.stat() == null || cookie.stat().getEphemeralOwner() != 0
                || cookie.stat().getCzxid() != source.birthZxid() || cookie.stat().getVersion() != source.version()
                || cookie.bytes() == null || cookie.bytes().length > MAX_COOKIE_BYTES
                || !MessageDigest.isEqual(sha256(cookie.bytes()), source.sha256())) {
            return new SourceCheck(List.of(), WriteResult.status(WriteStatus.CONFLICT,
                    "Original Cookie source changed"));
        }
        return new SourceCheck(List.of(Op.check(instancePath, cluster.stat().getVersion()),
                Op.check(cookiePath, cookie.stat().getVersion())), null);
    }

    private static boolean matchesCluster(ProfileZKReadClient.Node node, BookieRouteBinding binding) {
        if (node.stat() == null || node.stat().getEphemeralOwner() != 0 || node.stat().getVersion() != 0
                || node.stat().getCzxid() != binding.clusterBirthZxid()
                || node.bytes() == null || node.bytes().length < 1 || node.bytes().length > 1024) {
            return false;
        }
        try {
            String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(node.bytes())).toString();
            return value.equals(binding.clusterId());
        } catch (java.nio.charset.CharacterCodingException malformed) {
            return false;
        }
    }

    private static WriteResult confirmed(BookieRouteBinding binding, UUID birth, long expectedBirthZxid,
            ReadResult read, WriteStatus status) {
        if (read.status() != ReadStatus.FOUND) {
            return WriteResult.status(read.status() == ReadStatus.CORRUPT || read.status() == ReadStatus.INCOMPATIBLE
                    ? WriteStatus.INCOMPATIBLE : WriteStatus.RESPONSE_LOST,
                    "Committed native binding cannot be accepted: " + read.status());
        }
        var observed = read.observation().orElseThrow();
        return observed.birthId().equals(birth) && observed.binding().equals(binding)
                && (expectedBirthZxid == 0 || observed.birthZxid() == expectedBirthZxid)
                ? WriteResult.observed(status, observed)
                : WriteResult.status(WriteStatus.CONFLICT, "Native binding changed after submission");
    }

    private static WriteResult rejected(int code) {
        WriteStatus status = code == Code.NODEEXISTS.intValue() || code == Code.BADVERSION.intValue()
                || code == Code.NONODE.intValue() ? WriteStatus.CONFLICT
                : ZKProfileRegistrationStore.knownRejection(code) ? WriteStatus.UNAVAILABLE : WriteStatus.RESPONSE_LOST;
        return WriteResult.status(status, "Native binding multi result " + code);
    }

    private static WriteResult unreadable(ReadResult read) {
        return WriteResult.status(read.status() == ReadStatus.UNAVAILABLE
                ? WriteStatus.UNAVAILABLE : WriteStatus.INCOMPATIBLE, read.status() + ": " + read.detail());
    }

    String path(BookieId bookie) {
        byte[] name = Objects.requireNonNull(bookie, "bookie").toString().getBytes(StandardCharsets.UTF_8);
        if (name.length < 1 || name.length > 255 || bookie.toString().indexOf(0) >= 0
                || !bookie.toString().equals(new String(name, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid common binding Bookie identity");
        }
        return root + "/" + Base64.getUrlEncoder().withoutPadding().encodeToString(name);
    }

    private String marker(BookieId bookie, UUID birth) {
        return path(bookie) + "/birth-" + birth;
    }

    private record Envelope(UUID birth, BookieRouteBinding binding) { }

    private static byte[] encode(UUID birth, BookieRouteBinding binding) {
        byte[] body = binding.encode();
        return ByteBuffer.allocate(24 + body.length).putLong(MAGIC).putLong(birth.getMostSignificantBits())
                .putLong(birth.getLeastSignificantBits()).put(body).array();
    }

    private static Envelope decode(byte[] bytes) {
        if (bytes == null || bytes.length < 40 || bytes.length > BookieRouteBinding.MAX_BYTES + 24) {
            throw new IllegalArgumentException("Invalid native common binding length");
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getLong() != MAGIC) {
            throw new IllegalArgumentException("Unknown native common binding envelope");
        }
        UUID birth = new UUID(in.getLong(), in.getLong());
        if (birth.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("Missing native common binding birth identity");
        }
        return new Envelope(birth, BookieRouteBinding.decode(Arrays.copyOfRange(bytes, 24, bytes.length)));
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}

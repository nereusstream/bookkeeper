/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.versioning.Versioned;
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
 * Experimental initial lifecycle: immutable reservation, bounded target page, READY and DELETE_INTENT CAS.
 * Borrows the client and provisioned namespace/ACLs. Use bounded cold callers, not event or DATA owners.
 * No data deletion, page GC, local activation, writer, retry, watcher or client ownership is supplied.
 */
public final class ZKProfileReservationStore {
    public enum CreateStatus { APPLIED, ALREADY_APPLIED, CONFLICT, QUARANTINED, DURABILITY_UNKNOWN, UNAVAILABLE }

    public record Snapshot(long storeVersion, ProfileReservation reservation, long birthZxid, int rootDataVersion,
            long lifecycleVersion, int lifecycleDataVersion, ProfileLifecycleHead lifecycle) {
        public Snapshot {
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(lifecycle, "lifecycle");
            if (storeVersion < 0 || birthZxid < 0 || rootDataVersion < 0
                    || lifecycleVersion < 0 || lifecycleDataVersion < 0) {
                throw new IllegalArgumentException("Invalid reservation store version");
            }
        }
    }

    public record ReadySnapshot(ProfileReadyPage page, UUID birthId, long storeVersion, byte[] digest) {
        public ReadySnapshot {
            Objects.requireNonNull(page, "page");
            Objects.requireNonNull(birthId, "birthId");
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            if (birthId.equals(new UUID(0, 0)) || storeVersion < 0) {
                throw new IllegalArgumentException("Invalid initial page snapshot");
            }
        }

        @Override
        public byte[] digest() {
            return digest.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ReadySnapshot that && page.equals(that.page) && birthId.equals(that.birthId)
                    && storeVersion == that.storeVersion && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(page, birthId, storeVersion, Arrays.hashCode(digest));
        }
    }

    public record ReplacementSnapshot(ProfileReplacementPage page, UUID birthId, long storeVersion, byte[] digest) {
        public ReplacementSnapshot {
            Objects.requireNonNull(page, "page");
            Objects.requireNonNull(birthId, "birthId");
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            if (birthId.equals(new UUID(0, 0)) || storeVersion < 0) {
                throw new IllegalArgumentException("Invalid replacement page snapshot");
            }
        }
        @Override
        public byte[] digest() {
            return digest.clone();
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof ReplacementSnapshot value && page.equals(value.page)
                    && birthId.equals(value.birthId) && storeVersion == value.storeVersion
                    && Arrays.equals(digest, value.digest);
        }
        @Override
        public int hashCode() {
            return Objects.hash(page, birthId, storeVersion, Arrays.hashCode(digest));
        }
    }

    public record ReplacementResult(CreateStatus status, ReplacementSnapshot page) {
        public ReplacementResult {
            Objects.requireNonNull(status, "status");
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (page != null)) {
                throw new IllegalArgumentException("Only persisted replacement carries a page");
            }
        }
    }

    public record RetainedTargets(ReadStatus status, List<ProfileInstallReceipt> targets) {
        public RetainedTargets {
            Objects.requireNonNull(status, "status");
            targets = List.copyOf(targets);
            if (status != ReadStatus.FOUND && !targets.isEmpty()) {
                throw new IllegalArgumentException("Only verified history carries retained targets");
            }
        }
    }

    public record InstallPreparationSnapshot(ProfileReplacementInstallPage page, UUID birthId,
            long storeVersion, byte[] digest) {
        public InstallPreparationSnapshot {
            Objects.requireNonNull(page, "page");
            Objects.requireNonNull(birthId, "birthId");
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            if (birthId.equals(new UUID(0, 0)) || storeVersion < 0) {
                throw new IllegalArgumentException("Invalid replacement INSTALL snapshot");
            }
        }
        @Override
    public byte[] digest() {
        return digest.clone();
    }
    }

    public record InstallPreparationResult(CreateStatus status, InstallPreparationSnapshot page) {
        public InstallPreparationResult {
            Objects.requireNonNull(status, "status");
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (page != null)) {
                throw new IllegalArgumentException("Only persisted preparation carries a snapshot");
            }
        }
    }

    public record ReadResult(ReadStatus status, Snapshot snapshot) {
        public ReadResult {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (snapshot != null)) {
                throw new IllegalArgumentException("Only FOUND carries a reservation snapshot");
            }
        }
    }

    public record CreateResult(CreateStatus status, Snapshot snapshot) {
        public CreateResult {
            Objects.requireNonNull(status, "status");
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (snapshot != null)) {
                throw new IllegalArgumentException("Only proved lifecycle mutation carries a snapshot");
            }
        }
    }

    public record PageResult(CreateStatus status, ReadySnapshot page) {
        public PageResult {
            Objects.requireNonNull(status, "status");
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (page != null)) {
                throw new IllegalArgumentException("Only proved page persistence carries a snapshot");
            }
        }
    }

    private final ZooKeeper zk;
    private final ProfileZKReadClient reads;
    private final String root;
    private final List<ACL> acls;
    private final List<ACL> immutableAcls;
    private final List<ACL> pageAcls;
    private final long timeoutNanos;

    public ZKProfileReservationStore(ZooKeeper zk, String root, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        reads = new ProfileZKReadClient(zk);
        PathUtils.validatePath(root);
        if (root.equals("/") || acls.isEmpty() || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid reservation namespace, ACLs or timeout");
        }
        this.root = root;
        this.acls = acls.stream().map(acl -> new ACL(acl.getPerms(),
                new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        immutableAcls = this.acls.stream().filter(acl -> (acl.getPerms() & ZooDefs.Perms.READ) != 0)
                .map(acl -> new ACL(ZooDefs.Perms.READ, new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        // Data remains immutable, while authorized namespace owners can create/delete its birth child.
        pageAcls = this.acls.stream().map(acl -> new ACL(acl.getPerms()
                & (ZooDefs.Perms.READ | ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE),
                new Id(acl.getId().getScheme(), acl.getId().getId())))
                .filter(acl -> acl.getPerms() != 0).toList();
        if (immutableAcls.isEmpty()) {
            throw new IllegalArgumentException("Lifecycle markers/pages require a reader ACL");
        }
        timeoutNanos = timeout.toNanos();
    }

    String path(long ledgerId) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("Invalid reservation ledger");
        }
        return root + "/" + Long.toHexString(ledgerId);
    }

    private String headPath(long ledger) {
        return path(ledger) + "/lifecycle";
    }
    private String pagePath(long ledger) {
        return path(ledger) + "/initial-activation";
    }
    private String replacementPath(long ledger, long sequence) {
        return path(ledger) + "/replacement-" + sequence;
    }
    private String installPreparationPath(long ledger, long sequence) {
        return path(ledger) + "/replacement-install-" + sequence;
    }
    private static long replacementSequence(ProfileLifecycleHead head) {
        return head.state() == ProfileLifecycleHead.State.REPLACEMENT ? head.sequence()
                : head.state() == ProfileLifecycleHead.State.DELETE_INTENT && head.sequence() >= 4
                    ? head.predecessor() : -1;
    }

    private static String marker(String parent, UUID birth) {
        return parent + "/birth-" + birth;
    }

    public ReadResult read(long ledgerId) {
        return read(ledgerId, System.nanoTime() + timeoutNanos);
    }

    private ReadResult read(long ledgerId, long deadline) {
        String parent = path(ledgerId);
        try {
            if (reads.sync(root, deadline) != Code.OK.intValue()) {
                return new ReadResult(ReadStatus.UNAVAILABLE, null);
            }
            ProfileZKReadClient.Node node = reads.get(parent, deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return new ReadResult(ReadStatus.NOT_FOUND, null);
            }
            requireFound(node);
            requirePersistent(node, false);
            ProfileReservation reservation = ProfileReservationCodec.decode(node.bytes());
            if (reservation.backlink().ledgerId() != ledgerId) {
                throw new IllegalArgumentException("Wrong reservation ledger");
            }
            ProfileZKReadClient.Node headNode = reads.get(headPath(ledgerId), deadline);
            requireFound(headNode);
            requirePersistent(headNode, false);
            ProfileLifecycleHead head = ProfileLifecycleCodec.decodeHead(headNode.bytes());
            long initialGeneration = reservation.installs().get(0).authorityGeneration();
            if (node.stat().getCzxid() != headNode.stat().getCzxid()
                    || !head.backlink().equals(reservation.backlink())
                    || !Arrays.equals(head.reservationDigest(), ProfileLifecycleCodec.digest(node.bytes()))
                    || headNode.stat().getVersion() != head.sequence() - 1
                    || (head.state() == ProfileLifecycleHead.State.DELETE_INTENT
                            ? head.controlGeneration() - 1 != initialGeneration
                            : head.controlGeneration() != initialGeneration)) {
                throw new IllegalArgumentException("Lifecycle birth, immutable reservation or generation mismatch");
            }
            requireMarker(marker(headPath(ledgerId), head.birthId()), headNode.stat().getCzxid(), deadline);
            return new ReadResult(ReadStatus.FOUND, new Snapshot(node.stat().getMzxid(), reservation,
                    node.stat().getCzxid(), node.stat().getVersion(), headNode.stat().getMzxid(),
                    headNode.stat().getVersion(), head));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new ReadResult(ReadStatus.QUARANTINED, null);
        }
    }

    public CreateResult createIfAbsent(ProfileReservation desired) {
        Objects.requireNonNull(desired, "desired");
        byte[] bytes = ProfileReservationCodec.encode(desired);
        var first = desired.installs().get(0);
        ProfileLifecycleHead head = new ProfileLifecycleHead(desired.backlink(), ProfileLifecycleCodec.digest(bytes),
                UUID.randomUUID(), ProfileLifecycleHead.State.PREPARING, 1, 0, first.authorityGeneration(),
                first.operationId(), first.publicSemanticPayloadIdentity(), -1, new byte[32]);
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            ProfileZKReadClient.requireTime(deadline);
            submitted = true;
            int code = multi(List.of(Op.create(path(desired.backlink().ledgerId()), bytes, acls, CreateMode.PERSISTENT),
                    Op.create(headPath(desired.backlink().ledgerId()), ProfileLifecycleCodec.encode(head), acls,
                            CreateMode.PERSISTENT),
                    Op.create(marker(headPath(desired.backlink().ledgerId()), head.birthId()), new byte[0],
                            immutableAcls, CreateMode.PERSISTENT)), deadline);
            ReadResult current = read(desired.backlink().ledgerId(), deadline);
            if (current.status() != ReadStatus.FOUND) {
                return new CreateResult(current.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            if (!desired.equals(current.snapshot().reservation())) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            return new CreateResult(code == Code.OK.intValue() ? CreateStatus.APPLIED : CreateStatus.ALREADY_APPLIED,
                    current.snapshot());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CreateResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        } catch (TimeoutException timeout) {
            return new CreateResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        }
    }

    /** Persist the sole bounded candidate page. APPLIED here is page persistence, never global READY. */
    public PageResult prepareReady(Snapshot expected, ProfileReadyPage page, LedgerManager manager) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(page, "page");
        long ledger = page.backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new PageResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            Snapshot current = read.snapshot();
            validatePage(current, page);
            ReadySnapshot existing = page(current, deadline);
            if (existing != null) {
                return new PageResult(existing.page().equals(page)
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        existing.page().equals(page) ? existing : null);
            }
            if (!expected.equals(current) || current.lifecycle().state() != ProfileLifecycleHead.State.PREPARING
                    || !page.matchesMetadata(metadata(manager, ledger, deadline))) {
                return new PageResult(CreateStatus.CONFLICT, null);
            }
            ProfileLifecycleCodec.Page envelope = new ProfileLifecycleCodec.Page(UUID.randomUUID(), page);
            ProfileZKReadClient.requireTime(deadline);
            submitted = true;
            int code = multi(List.of(Op.check(path(ledger), current.rootDataVersion()),
                    Op.check(marker(headPath(ledger), current.lifecycle().birthId()), 0),
                    Op.check(headPath(ledger), current.lifecycleDataVersion()),
                    Op.create(pagePath(ledger), ProfileLifecycleCodec.encode(envelope), pageAcls,
                            CreateMode.PERSISTENT),
                    Op.create(marker(pagePath(ledger), envelope.birthId()), new byte[0], immutableAcls,
                            CreateMode.PERSISTENT)), deadline);
            ReadResult after = read(ledger, deadline);
            if (after.status() != ReadStatus.FOUND) {
                return new PageResult(CreateStatus.DURABILITY_UNKNOWN, null);
            }
            ReadySnapshot actual = page(after.snapshot(), deadline);
            if (actual != null && actual.page().equals(page)) {
                return new PageResult(code == Code.OK.intValue() ? CreateStatus.APPLIED : CreateStatus.ALREADY_APPLIED,
                        actual);
            }
            return new PageResult(conditionalFailure(code)
                    ? CreateStatus.CONFLICT : CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new PageResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new PageResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new PageResult(CreateStatus.QUARANTINED, null);
        }
    }

    public CreateResult publishReady(Snapshot expected, ReadySnapshot prepared, LedgerManager manager) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(prepared, "prepared");
        long ledger = prepared.page().backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new CreateResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            Snapshot current = read.snapshot();
            ReadySnapshot actual = page(current, deadline);
            if (actual == null || !samePage(actual, prepared)) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            if (published(current.lifecycle(), actual)) {
                return new CreateResult(CreateStatus.ALREADY_APPLIED, current);
            }
            if (!expected.equals(current) || current.lifecycle().state() != ProfileLifecycleHead.State.PREPARING
                    || !prepared.page().matchesMetadata(metadata(manager, ledger, deadline))) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            ProfileLifecycleHead next = new ProfileLifecycleHead(current.reservation().backlink(),
                    current.lifecycle().reservationDigest(), current.lifecycle().birthId(),
                            ProfileLifecycleHead.State.READY,
                    2, 1, current.lifecycle().controlGeneration(), actual.page().operationId(),
                    actual.page().payloadIdentity(), actual.storeVersion(), actual.digest());
            return cas(current, next, actual, deadline);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new CreateResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Admission cut only. It retains published targets; it neither fences Bookies nor deletes/GCs any data. */
    public CreateResult cutDelete(Snapshot expected, byte[] operationId, byte[] payloadIdentity) {
        operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
        payloadIdentity = ProfileLifecycleHead.fixed(payloadIdentity, 32, false);
        long ledger = expected.reservation().backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new CreateResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            Snapshot current = read.snapshot();
            ProfileLifecycleHead head = current.lifecycle();
            if (!head.birthId().equals(expected.lifecycle().birthId())
                    || !current.reservation().equals(expected.reservation())) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            if (head.state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                return new CreateResult(Arrays.equals(operationId, head.operationId())
                        && Arrays.equals(payloadIdentity, head.payloadIdentity())
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        Arrays.equals(operationId, head.operationId()) && Arrays.equals(payloadIdentity,
                                head.payloadIdentity())
                                ? current : null);
            }
            if (!current.equals(expected) || Arrays.equals(operationId, head.operationId())
                    || Arrays.equals(operationId, current.reservation().installs().get(0).operationId())
                    || head.controlGeneration() == Long.MAX_VALUE) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            ProfileLifecycleHead next = new ProfileLifecycleHead(head.backlink(), head.reservationDigest(),
                    head.birthId(),
                    ProfileLifecycleHead.State.DELETE_INTENT, head.sequence() + 1, head.sequence(),
                    head.controlGeneration() + 1, operationId, payloadIdentity, head.readyPageVersion(),
                            head.readyPageDigest());
            return cas(current, next, null, deadline);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new CreateResult(CreateStatus.QUARANTINED, null);
        }
    }

    private CreateResult cas(Snapshot current, ProfileLifecycleHead desired, ReadySnapshot prepared, long deadline)
            throws InterruptedException, TimeoutException {
        long ledger = desired.backlink().ledgerId();
        var operations = new java.util.ArrayList<Op>();
        operations.add(Op.check(path(ledger), current.rootDataVersion()));
        operations.add(Op.check(marker(headPath(ledger), current.lifecycle().birthId()), 0));
        if (prepared != null) {
            operations.add(Op.check(marker(pagePath(ledger), prepared.birthId()), 0));
            operations.add(Op.check(pagePath(ledger), 0));
        }
        operations.add(Op.setData(headPath(ledger), ProfileLifecycleCodec.encode(desired),
                current.lifecycleDataVersion()));
        int code = multi(operations, deadline);
        ReadResult read = read(ledger, deadline);
        if (read.status() != ReadStatus.FOUND) {
            return new CreateResult(read.status() == ReadStatus.QUARANTINED
                    ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
        }
        Snapshot actual = read.snapshot();
        boolean applied = actual.lifecycle().equals(desired)
                || desired.state() == ProfileLifecycleHead.State.READY && prepared != null
                        && published(actual.lifecycle(), prepared);
        if (applied) {
            return new CreateResult(code == Code.OK.intValue()
                    ? CreateStatus.APPLIED : CreateStatus.ALREADY_APPLIED, actual);
        }
        return new CreateResult(conditionalFailure(code)
                ? CreateStatus.CONFLICT : CreateStatus.DURABILITY_UNKNOWN, null);
    }

    /** Resolve the persisted initial candidate for recovery. Presence is not a READY/availability fact. */
    public PageResult readCandidate(long ledger) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new PageResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            ReadySnapshot candidate = page(read.snapshot(), deadline);
            return new PageResult(candidate == null ? CreateStatus.CONFLICT : CreateStatus.ALREADY_APPLIED, candidate);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new PageResult(CreateStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new PageResult(CreateStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new PageResult(CreateStatus.QUARANTINED, null);
        }
    }

    public PageResult readPublishedPage(long ledger) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new PageResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            Snapshot snapshot = read.snapshot();
            if (snapshot.lifecycle().readyPageVersion() == -1) {
                return new PageResult(CreateStatus.CONFLICT, null);
            }
            ReadySnapshot page = page(snapshot, deadline);
            if (page == null || !initialPublished(snapshot, page, deadline)) {
                return new PageResult(CreateStatus.QUARANTINED, null);
            }
            return new PageResult(CreateStatus.ALREADY_APPLIED, page);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new PageResult(CreateStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new PageResult(CreateStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new PageResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Reserve the next cold slot for inactive INSTALL, before any membership mutation. */
    public InstallPreparationResult prepareReplacementInstall(Snapshot expected, ProfileReplacementInstallPage page,
            ProfileLedgerMetadataStore metadata) {
        long ledger = page.backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new InstallPreparationResult(read.status() == ReadStatus.UNAVAILABLE
                        ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.QUARANTINED, null);
            }
            var current = read.snapshot();
            if (!current.lifecycle().birthId().equals(page.lifecycleBirth())
                    || !current.reservation().equals(expected.reservation())) {
                return new InstallPreparationResult(CreateStatus.CONFLICT, null);
            }
            var existing = installPreparation(current, page.sequence(), deadline);
            if (existing != null) {
                return new InstallPreparationResult(existing.page().equals(page)
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        existing.page().equals(page) ? existing : null);
            }
            var head = current.lifecycle();
            if (!current.equals(expected) || head.state() != ProfileLifecycleHead.State.READY
                    && head.state() != ProfileLifecycleHead.State.REPLACEMENT
                    || page.sequence() != head.sequence() + 1) {
                return new InstallPreparationResult(CreateStatus.CONFLICT, null);
            }
            var observed = metadata.readMembership(ledger, deadline);
            if (observed.status() != ReadStatus.FOUND || !page.matchesBefore(observed.snapshot())
                    || !page(current, deadline).page().matchesQuorum(observed.snapshot().metadata().getValue())) {
                return new InstallPreparationResult(observed.status() == ReadStatus.UNAVAILABLE
                        ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.CONFLICT, null);
            }
            var guarded = metadata.ensureMembershipGuard(observed.snapshot(), deadline);
            if (guarded == null || !page.matchesBefore(guarded)) {
                return new InstallPreparationResult(CreateStatus.CONFLICT, null);
            }
            var envelope = new ProfileLifecycleCodec.InstallPreparation(UUID.randomUUID(), page);
            var ops = new ArrayList<Op>();
            ops.add(Op.check(path(ledger), current.rootDataVersion()));
            ops.add(Op.check(marker(headPath(ledger), head.birthId()), 0));
            ops.add(Op.check(headPath(ledger), current.lifecycleDataVersion()));
            ops.addAll(metadata.checksForMembership(guarded));
            ops.add(Op.create(installPreparationPath(ledger, page.sequence()), ProfileLifecycleCodec.encode(envelope),
                    pageAcls, CreateMode.PERSISTENT));
            ops.add(Op.create(marker(installPreparationPath(ledger, page.sequence()), envelope.birthId()),
                    new byte[0], immutableAcls, CreateMode.PERSISTENT));
            int code = multi(ops, deadline);
            var after = read(ledger, deadline);
            if (after.status() != ReadStatus.FOUND) {
                return new InstallPreparationResult(CreateStatus.DURABILITY_UNKNOWN, null);
            }
            var actual = installPreparation(after.snapshot(), page.sequence(), deadline);
            boolean equal = actual != null && actual.page().equals(page);
            return new InstallPreparationResult(equal ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, equal ? actual : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new InstallPreparationResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new InstallPreparationResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IOException | IllegalArgumentException invalid) {
            return new InstallPreparationResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Persist one bounded post-CAS candidate. Its existence is never activation authority. */
    public ReplacementResult prepareReplacement(Snapshot expected, ProfileReplacementPage desired,
            ProfileLedgerMetadataStore metadata) {
        long ledger = desired.backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return replacementFailure(read.status());
            }
            Snapshot current = read.snapshot();
            if (!expected.lifecycle().birthId().equals(current.lifecycle().birthId())
                    || !expected.reservation().equals(current.reservation())) {
                return new ReplacementResult(CreateStatus.CONFLICT, null);
            }
            ReplacementSnapshot existing = replacement(current, desired.sequence(), deadline);
            if (existing != null) {
                return new ReplacementResult(existing.page().equals(desired)
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        existing.page().equals(desired) ? existing : null);
            }
            if (!expected.equals(current) || !liveReplacementPredecessor(current, desired, deadline)) {
                return new ReplacementResult(CreateStatus.CONFLICT, null);
            }
            var actual = metadata.readMembership(ledger, deadline);
            if (actual.status() != ReadStatus.FOUND) {
                return replacementFailure(actual.status());
            }
            if (!desired.matchesMetadata(actual.snapshot())
                    || !page(current, deadline).page().matchesQuorum(actual.snapshot().metadata().getValue())) {
                return new ReplacementResult(CreateStatus.CONFLICT, null);
            }
            var envelope = new ProfileLifecycleCodec.Replacement(UUID.randomUUID(), desired);
            var ops = new ArrayList<Op>();
            ops.add(Op.check(path(ledger), current.rootDataVersion()));
            ops.add(Op.check(marker(headPath(ledger), current.lifecycle().birthId()), 0));
            ops.add(Op.check(headPath(ledger), current.lifecycleDataVersion()));
            ops.addAll(metadata.checksForMembership(actual.snapshot()));
            ops.add(Op.create(replacementPath(ledger, desired.sequence()), ProfileLifecycleCodec.encode(envelope),
                    pageAcls, CreateMode.PERSISTENT));
            ops.add(Op.create(marker(replacementPath(ledger, desired.sequence()), envelope.birthId()),
                    new byte[0], immutableAcls, CreateMode.PERSISTENT));
            int code = multi(ops, deadline);
            var after = read(ledger, deadline);
            if (after.status() != ReadStatus.FOUND) {
                return replacementFailure(after.status());
            }
            var page = replacement(after.snapshot(), desired.sequence(), deadline);
            boolean equal = page != null && page.page().equals(desired);
            return new ReplacementResult(equal ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, equal ? page : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ReplacementResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ReplacementResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new ReplacementResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Publish authority and its cleanup targets at the same cut as DELETE_INTENT. */
    public CreateResult publishReplacement(Snapshot expected, ReplacementSnapshot prepared,
            ProfileLedgerMetadataStore metadata) {
        long ledger = prepared.page().backlink().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            ReadResult read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new CreateResult(read.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            Snapshot current = read.snapshot();
            var actual = replacement(current, prepared.page().sequence(), deadline);
            if (actual == null || !sameReplacement(actual, prepared)) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            if (replacementPublished(current.lifecycle(), actual)) {
                return new CreateResult(CreateStatus.ALREADY_APPLIED, current);
            }
            if (!expected.equals(current) || !liveReplacementPredecessor(current, prepared.page(), deadline)) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            var observed = metadata.readMembership(ledger, deadline);
            if (observed.status() != ReadStatus.FOUND) {
                return new CreateResult(observed.status() == ReadStatus.QUARANTINED
                        || observed.status() == ReadStatus.NOT_FOUND ? CreateStatus.QUARANTINED
                        : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            if (!prepared.page().matchesMetadata(observed.snapshot())
                    || !page(current, deadline).page().matchesQuorum(observed.snapshot().metadata().getValue())) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            var head = current.lifecycle();
            var next = new ProfileLifecycleHead(head.backlink(), head.reservationDigest(), head.birthId(),
                    ProfileLifecycleHead.State.REPLACEMENT, head.sequence() + 1, head.sequence(),
                    head.controlGeneration(), prepared.page().operationId(), prepared.page().payloadIdentity(),
                    prepared.storeVersion(), prepared.digest());
            var ops = new ArrayList<Op>();
            ops.add(Op.check(path(ledger), current.rootDataVersion()));
            ops.add(Op.check(marker(headPath(ledger), head.birthId()), 0));
            ops.add(Op.check(marker(replacementPath(ledger, actual.page().sequence()), actual.birthId()), 0));
            ops.add(Op.check(replacementPath(ledger, actual.page().sequence()), 0));
            ops.addAll(metadata.checksForMembership(observed.snapshot()));
            ops.add(Op.setData(headPath(ledger), ProfileLifecycleCodec.encode(next), current.lifecycleDataVersion()));
            int code = multi(ops, deadline);
            var after = read(ledger, deadline);
            if (after.status() != ReadStatus.FOUND) {
                return new CreateResult(after.status() == ReadStatus.QUARANTINED
                        ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
            }
            boolean published = replacementPublished(after.snapshot().lifecycle(), actual);
            return new CreateResult(published ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, published ? after.snapshot() : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new CreateResult(CreateStatus.QUARANTINED, null);
        }
    }

    public ReplacementResult readReplacement(long ledger, long sequence) {
        if (sequence < 3 || sequence > 2 + ProfileReplacementPage.MAX_PUBLICATIONS) {
            throw new IllegalArgumentException("Invalid replacement page sequence");
        }
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var source = read(ledger, deadline);
            if (source.status() != ReadStatus.FOUND) {
                return replacementFailure(source.status());
            }
            var value = replacement(source.snapshot(), sequence, deadline);
            return new ReplacementResult(value == null ? CreateStatus.CONFLICT : CreateStatus.ALREADY_APPLIED, value);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ReplacementResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ReplacementResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new ReplacementResult(CreateStatus.QUARANTINED, null);
        }
    }

    public ProfileControlStore replacementAuthorityReader(String targetBookie, ProfileLedgerMetadataStore metadata) {
        ProfileAuthorityCodec.text(targetBookie, ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        return key -> {
            long deadline = System.nanoTime() + timeoutNanos;
            try {
                var read = read(key.ledgerId(), deadline);
                if (read.status() != ReadStatus.FOUND) {
                    return new ProfileControlStore.ReadResult(read.status(), null);
                }
                var current = read.snapshot();
                if (current.lifecycle().state() != ProfileLifecycleHead.State.REPLACEMENT) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                var page = replacement(current, current.lifecycle().sequence(), deadline);
                if (page == null || !replacementPublished(current.lifecycle(), page)) {
                    return ProfileControlStore.ReadResult.quarantined();
                }
                var observed = metadata.readMembership(key.ledgerId(), deadline);
                if (observed.status() != ReadStatus.FOUND) {
                    return observed.status() == ReadStatus.NOT_FOUND ? ProfileControlStore.ReadResult.quarantined()
                            : new ProfileControlStore.ReadResult(observed.status(), null);
                }
                if (!page.page().matchesMetadata(observed.snapshot()) || page.page().installs().stream()
                        .noneMatch(value -> value.scope().targetBookie().equals(targetBookie))) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                var scope = page.page().activation(targetBookie);
                return ProfileAuthorityKey.fromScope(scope).equals(key)
                        ? ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(scope,
                                CommittedProfileAuthority.State.POST_MEMBERSHIP_REPLACEMENT,
                                EngineProfile.SEGMENT_WAL_ENGINE, current.lifecycleVersion(), true))
                        : ProfileControlStore.ReadResult.notFound();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return ProfileControlStore.ReadResult.unavailable();
            } catch (TimeoutException | IllegalStateException unavailable) {
                return ProfileControlStore.ReadResult.unavailable();
            } catch (IllegalArgumentException invalid) {
                return ProfileControlStore.ReadResult.quarantined();
            }
        };
    }

    /** Bounded discovery only: it grants neither activation nor delete/physical completion. */
    public RetainedTargets retainedActivationTargets(long ledger) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var read = read(ledger, deadline);
            if (read.status() != ReadStatus.FOUND) {
                return new RetainedTargets(read.status(), List.of());
            }
            var current = read.snapshot();
            var initial = page(current, deadline);
            if (initial == null || !initialPublished(current, initial, deadline)) {
                return new RetainedTargets(ReadStatus.QUARANTINED, List.of());
            }
            var targets = new LinkedHashMap<String, ProfileInstallReceipt>();
            initial.page().installs().forEach(value -> retain(targets, value));
            long last = replacementSequence(current.lifecycle());
            long previousVersion = initial.storeVersion();
            byte[] previousDigest = initial.digest();
            for (long sequence = 3; sequence <= last; sequence++) {
                var next = replacement(current, sequence, deadline);
                if (next == null || next.page().previousPageVersion() != previousVersion
                        || !Arrays.equals(next.page().previousPageDigest(), previousDigest)
                        || next.page().initialPageVersion() != initial.storeVersion()
                        || !Arrays.equals(next.page().initialPageDigest(), initial.digest())) {
                    return new RetainedTargets(ReadStatus.QUARANTINED, List.of());
                }
                next.page().installs().forEach(value -> retain(targets, value));
                previousVersion = next.storeVersion();
                previousDigest = next.digest();
            }
            if (last >= 3 && (previousVersion != current.lifecycle().readyPageVersion()
                    || !Arrays.equals(previousDigest, current.lifecycle().readyPageDigest()))) {
                return new RetainedTargets(ReadStatus.QUARANTINED, List.of());
            }
            return new RetainedTargets(ReadStatus.FOUND, List.copyOf(targets.values()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RetainedTargets(ReadStatus.UNAVAILABLE, List.of());
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RetainedTargets(ReadStatus.UNAVAILABLE, List.of());
        } catch (IllegalArgumentException invalid) {
            return new RetainedTargets(ReadStatus.QUARANTINED, List.of());
        }
    }

    private static void retain(LinkedHashMap<String, ProfileInstallReceipt> targets, ProfileInstallReceipt value) {
        targets.putIfAbsent(value.scope().targetBookie() + "/"
                + HexFormat.of().formatHex(value.scope().targetStorageIncarnation()), value);
    }

    private static ReplacementResult replacementFailure(ReadStatus status) {
        return new ReplacementResult(status == ReadStatus.QUARANTINED || status == ReadStatus.NOT_FOUND
                ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null);
    }

    public ProfileControlStore installAuthorityReader(String targetBookie) {
        ProfileAuthorityCodec.text(Objects.requireNonNull(targetBookie, "targetBookie"),
                ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        return key -> {
            ReadResult current = read(key.ledgerId());
            if (current.status() != ReadStatus.FOUND) {
                return new ProfileControlStore.ReadResult(current.status(), null);
            }
            if (current.snapshot().lifecycle().state() != ProfileLifecycleHead.State.PREPARING) {
                return ProfileControlStore.ReadResult.notFound();
            }
            return current.snapshot().reservation().installs().stream()
                    .filter(scope -> scope.targetBookie().equals(targetBookie)
                            && ProfileAuthorityKey.fromScope(scope).equals(key))
                    .findFirst().map(scope -> ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(
                            scope, CommittedProfileAuthority.State.PREPARING, EngineProfile.SEGMENT_WAL_ENGINE,
                            current.snapshot().lifecycleVersion(), true)))
                    .orElseGet(ProfileControlStore.ReadResult::notFound);
        };
    }

    public ProfileControlStore installAuthorityReader(String targetBookie, ProfileLedgerMetadataStore metadata) {
        var initial = installAuthorityReader(targetBookie);
        return key -> {
            long deadline = System.nanoTime() + timeoutNanos;
            try {
                var read = read(key.ledgerId(), deadline);
                if (read.status() != ReadStatus.FOUND) {
                    return new ProfileControlStore.ReadResult(read.status(), null);
                }
                var current = read.snapshot();
                var head = current.lifecycle();
                if (head.state() == ProfileLifecycleHead.State.PREPARING) {
                    return initial.read(key);
                }
                if (head.state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                InstallPreparationSnapshot plan = head.sequence() < 2 + ProfileReplacementPage.MAX_PUBLICATIONS
                        ? installPreparation(current, head.sequence() + 1, deadline) : null;
                if (plan == null && head.state() == ProfileLifecycleHead.State.REPLACEMENT) {
                    plan = installPreparation(current, head.sequence(), deadline);
                }
                if (plan == null) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                var observed = metadata.readMembership(key.ledgerId(), deadline);
                if (observed.status() != ReadStatus.FOUND) {
                    return observed.status() == ReadStatus.NOT_FOUND ? ProfileControlStore.ReadResult.quarantined()
                            : new ProfileControlStore.ReadResult(observed.status(), null);
                }
                if (!plan.page().matchesBefore(observed.snapshot())
                        && !plan.page().matchesCommitted(observed.snapshot())) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                return plan.page().installs().stream().filter(scope -> scope.targetBookie().equals(targetBookie)
                        && ProfileAuthorityKey.fromScope(scope).equals(key)).findFirst()
                        .map(scope -> ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(scope,
                                CommittedProfileAuthority.State.PREPARING, EngineProfile.SEGMENT_WAL_ENGINE,
                                current.lifecycleVersion(), true))).orElseGet(ProfileControlStore.ReadResult::notFound);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return ProfileControlStore.ReadResult.unavailable();
            } catch (TimeoutException | IllegalStateException unavailable) {
                return ProfileControlStore.ReadResult.unavailable();
            } catch (IllegalArgumentException invalid) {
                return ProfileControlStore.ReadResult.quarantined();
            }
        };
    }

    public ProfileControlStore activationAuthorityReader(String targetBookie, LedgerManager manager) {
        ProfileAuthorityCodec.text(Objects.requireNonNull(targetBookie, "targetBookie"),
                ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        Objects.requireNonNull(manager, "manager");
        return key -> {
            long deadline = System.nanoTime() + timeoutNanos;
            try {
                ReadResult current = read(key.ledgerId(), deadline);
                if (current.status() != ReadStatus.FOUND) {
                    return new ProfileControlStore.ReadResult(current.status(), null);
                }
                Snapshot snapshot = current.snapshot();
                if (snapshot.lifecycle().state() != ProfileLifecycleHead.State.READY) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                ReadySnapshot page = page(snapshot, deadline);
                if (page == null || !published(snapshot.lifecycle(), page)) {
                    return ProfileControlStore.ReadResult.quarantined();
                }
                Versioned<LedgerMetadata> actual = metadata(manager, key.ledgerId(), deadline);
                ProfileMetadataBacklink.from(actual.getValue());
                if (!page.page().matchesMetadata(actual)) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                if (page.page().installs().stream()
                        .noneMatch(receipt -> receipt.scope().targetBookie().equals(targetBookie))) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                ProfileControlScope scope = page.page().activation(targetBookie);
                return ProfileAuthorityKey.fromScope(scope).equals(key)
                        ? ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(scope,
                                CommittedProfileAuthority.State.READY_INITIAL, EngineProfile.SEGMENT_WAL_ENGINE,
                                snapshot.lifecycleVersion(), true)) : ProfileControlStore.ReadResult.notFound();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return ProfileControlStore.ReadResult.unavailable();
            } catch (TimeoutException | IllegalStateException unavailable) {
                return ProfileControlStore.ReadResult.unavailable();
            } catch (IllegalArgumentException invalid) {
                return ProfileControlStore.ReadResult.quarantined();
            }
        };
    }

    private ReadySnapshot page(Snapshot current, long deadline) throws InterruptedException, TimeoutException {
        long ledger = current.reservation().backlink().ledgerId();
        ProfileZKReadClient.Node node = reads.get(pagePath(ledger), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        requirePersistent(node, true);
        ProfileLifecycleCodec.Page envelope = ProfileLifecycleCodec.decodePage(node.bytes());
        validatePage(current, envelope.value());
        requireMarker(marker(pagePath(ledger), envelope.birthId()), node.stat().getCzxid(), deadline);
        return new ReadySnapshot(envelope.value(), envelope.birthId(), node.stat().getMzxid(),
                ProfileLifecycleCodec.digest(node.bytes()));
    }

    private InstallPreparationSnapshot installPreparation(Snapshot current, long sequence, long deadline)
            throws InterruptedException, TimeoutException {
        var node = reads.get(installPreparationPath(current.reservation().backlink().ledgerId(), sequence), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        requirePersistent(node, true);
        var envelope = ProfileLifecycleCodec.decodeInstallPreparation(node.bytes());
        var value = envelope.value();
        var base = current.reservation().installs().get(0);
        if (!value.backlink().equals(current.reservation().backlink())
                || !value.lifecycleBirth().equals(current.lifecycle().birthId()) || value.sequence() != sequence
                || value.controlGeneration() != base.authorityGeneration()
                || value.installs().stream().anyMatch(scope ->
                        !scope.descriptorIdentity().equals(base.descriptorIdentity())
                        || !scope.authorityDomain().equals(base.authorityDomain()))) {
            throw new IllegalArgumentException("Invalid replacement preparation identity/descriptor/domain");
        }
        String path = installPreparationPath(current.reservation().backlink().ledgerId(), sequence);
        requireMarker(marker(path, envelope.birthId()), node.stat().getCzxid(), deadline);
        return new InstallPreparationSnapshot(value, envelope.birthId(), node.stat().getMzxid(),
                ProfileLifecycleCodec.digest(node.bytes()));
    }

    private ReplacementSnapshot replacement(Snapshot current, long sequence, long deadline)
            throws InterruptedException, TimeoutException {
        long ledger = current.reservation().backlink().ledgerId();
        var node = reads.get(replacementPath(ledger, sequence), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        requirePersistent(node, true);
        var envelope = ProfileLifecycleCodec.decodeReplacement(node.bytes());
        var value = envelope.value();
        var base = current.reservation().installs().get(0);
        if (!value.backlink().equals(current.reservation().backlink())
                || !value.lifecycleBirth().equals(current.lifecycle().birthId()) || value.sequence() != sequence
                || value.controlGeneration() != base.authorityGeneration()
                || value.installs().stream().anyMatch(receipt ->
                        !receipt.scope().descriptorIdentity().equals(base.descriptorIdentity())
                        || !receipt.scope().authorityDomain().equals(base.authorityDomain()))) {
            throw new IllegalArgumentException("Replacement page does not match this lifecycle identity");
        }
        requireMarker(marker(replacementPath(ledger, sequence), envelope.birthId()), node.stat().getCzxid(), deadline);
        return new ReplacementSnapshot(value, envelope.birthId(), node.stat().getMzxid(),
                ProfileLifecycleCodec.digest(node.bytes()));
    }

    private boolean liveReplacementPredecessor(Snapshot current, ProfileReplacementPage desired, long deadline)
            throws InterruptedException, TimeoutException {
        var head = current.lifecycle();
        if (head.state() != ProfileLifecycleHead.State.READY && head.state() != ProfileLifecycleHead.State.REPLACEMENT
                || desired.sequence() != head.sequence() + 1 || !desired.lifecycleBirth().equals(head.birthId())
                || !desired.backlink().equals(head.backlink())
                || desired.controlGeneration() != head.controlGeneration()
                || desired.previousPageVersion() != head.readyPageVersion()
                || !Arrays.equals(desired.previousPageDigest(), head.readyPageDigest())
                || Arrays.equals(desired.operationId(), head.operationId())
                || Arrays.equals(desired.operationId(), current.reservation().installs().get(0).operationId())) {
            return false;
        }
        var initial = page(current, deadline);
        if (initial == null || !initialPublished(current, initial, deadline)
                || desired.initialPageVersion() != initial.storeVersion()
                || !Arrays.equals(desired.initialPageDigest(), initial.digest())
                || desired.creatorToken() != initial.page().creatorToken()) {
            return false;
        }
        long previousMetadata = initial.page().metadataVersion();
        if (head.state() == ProfileLifecycleHead.State.REPLACEMENT) {
            var previous = replacement(current, head.sequence(), deadline);
            if (previous == null || !replacementPublished(head, previous)) {
                return false;
            }
            previousMetadata = previous.page().metadataVersion();
        }
        var base = current.reservation().installs().get(0);
        var preparedInstall = installPreparation(current, desired.sequence(), deadline);
        boolean installBound = preparedInstall == null
                ? current.reservation().installs().containsAll(desired.installs().stream()
                        .map(ProfileInstallReceipt::scope).toList())
                : preparedInstall.page().metadataVersion() + 1 == desired.metadataVersion()
                    && preparedInstall.page().metadataBirthZxid() == desired.metadataBirthZxid()
                    && preparedInstall.page().creatorToken() == desired.creatorToken()
                    && preparedInstall.page().fragmentStart() == desired.fragmentStart()
                    && Arrays.equals(preparedInstall.page().ensembleDigest(), desired.ensembleDigest())
                    && preparedInstall.page().installs().equals(desired.installs().stream()
                            .map(ProfileInstallReceipt::scope).toList());
        return installBound && desired.metadataVersion() == previousMetadata + 1
                && desired.installs().stream().allMatch(value ->
                value.scope().descriptorIdentity().equals(base.descriptorIdentity())
                        && value.scope().authorityDomain().equals(base.authorityDomain()));
    }

    private boolean initialPublished(Snapshot source, ReadySnapshot initial, long deadline)
            throws InterruptedException, TimeoutException {
        if (published(source.lifecycle(), initial)) {
            return true;
        }
        long sequence = replacementSequence(source.lifecycle());
        if (sequence < 3) {
            return false;
        }
        var latest = replacement(source, sequence, deadline);
        return latest != null && replacementPublished(source.lifecycle(), latest)
                && latest.page().initialPageVersion() == initial.storeVersion()
                && Arrays.equals(latest.page().initialPageDigest(), initial.digest());
    }

    private static boolean replacementPublished(ProfileLifecycleHead head, ReplacementSnapshot page) {
        return replacementSequence(head) == page.page().sequence()
                && head.birthId().equals(page.page().lifecycleBirth())
                && head.backlink().equals(page.page().backlink())
                && head.readyPageVersion() == page.storeVersion()
                && Arrays.equals(head.readyPageDigest(), page.digest())
                && (head.state() == ProfileLifecycleHead.State.DELETE_INTENT
                        ? head.controlGeneration() == page.page().controlGeneration() + 1
                        : head.controlGeneration() == page.page().controlGeneration()
                            && Arrays.equals(head.operationId(), page.page().operationId())
                            && Arrays.equals(head.payloadIdentity(), page.page().payloadIdentity()));
    }

    private static boolean sameReplacement(ReplacementSnapshot a, ReplacementSnapshot b) {
        return a.page().equals(b.page()) && a.birthId().equals(b.birthId()) && a.storeVersion() == b.storeVersion()
                && Arrays.equals(a.digest(), b.digest());
    }

    private static void validatePage(Snapshot current, ProfileReadyPage page) {
        if (!current.reservation().backlink().equals(page.backlink())
                || !current.lifecycle().birthId().equals(page.lifecycleBirth())
                || !current.reservation().installs().equals(
                        page.installs().stream().map(ProfileInstallReceipt::scope).toList())
                || current.reservation().installs().get(0).authorityGeneration() != page.controlGeneration()) {
            throw new IllegalArgumentException("Initial page does not match reservation birth and targets");
        }
    }

    private static boolean published(ProfileLifecycleHead head, ReadySnapshot page) {
        return head.state() != ProfileLifecycleHead.State.PREPARING
                && head.birthId().equals(page.page().lifecycleBirth())
                && head.backlink().equals(page.page().backlink())
                && head.readyPageVersion() == page.storeVersion()
                && Arrays.equals(head.readyPageDigest(), page.digest())
                && (head.state() == ProfileLifecycleHead.State.DELETE_INTENT
                        || Arrays.equals(head.operationId(), page.page().operationId())
                                && Arrays.equals(head.payloadIdentity(), page.page().payloadIdentity()));
    }

    private static boolean samePage(ReadySnapshot left, ReadySnapshot right) {
        return left.page().equals(right.page()) && left.birthId().equals(right.birthId())
                && left.storeVersion() == right.storeVersion() && Arrays.equals(left.digest(), right.digest());
    }

    private Versioned<LedgerMetadata> metadata(LedgerManager manager, long ledger, long deadline)
            throws InterruptedException, TimeoutException {
        try {
            return ProfileZKReadClient.await(Objects.requireNonNull(manager,
                    "manager").readLedgerMetadata(ledger), deadline);
        } catch (IllegalStateException failed) {
            if (BKException.getExceptionCode(failed) == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                throw new IllegalArgumentException("Required standard metadata missing");
            }
            throw failed;
        }
    }

    private void requireMarker(String path, long birthZxid, long deadline) throws InterruptedException,
            TimeoutException {
        ProfileZKReadClient.Node node = reads.get(path, deadline);
        requireFound(node);
        requirePersistent(node, true);
        if (node.bytes().length != 0 || node.stat().getCzxid() != birthZxid) {
            throw new IllegalArgumentException("Invalid immutable lifecycle/page birth marker");
        }
    }

    private static void requireFound(ProfileZKReadClient.Node node) {
        if (node.code() == Code.NONODE.intValue()) {
            throw new IllegalArgumentException("Required lifecycle child missing");
        }
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Lifecycle read unavailable");
        }
    }

    private static void requirePersistent(ProfileZKReadClient.Node node, boolean immutable) {
        Stat stat = node.stat();
        if (node.bytes() == null || stat == null || stat.getEphemeralOwner() != 0 || stat.getVersion() < 0
                || stat.getCzxid() < 0 || stat.getMzxid() < stat.getCzxid() || immutable && stat.getVersion() != 0) {
            throw new IllegalArgumentException("Invalid persistent lifecycle/page record");
        }
    }

    private int multi(List<Op> operations, long deadline) throws InterruptedException, TimeoutException {
        ProfileZKReadClient.requireTime(deadline);
        CompletableFuture<Integer> reply = new CompletableFuture<>();
        zk.multi(operations, (code, ignored, context, results) -> reply.complete(code), null);
        return ProfileZKReadClient.await(reply, deadline);
    }

    private static boolean conditionalFailure(int code) {
        return code == Code.BADVERSION.intValue() || code == Code.NODEEXISTS.intValue()
                || code == Code.NONODE.intValue();
    }
}

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
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityPurpose;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
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

    public record RecoveryPlan(ProfileLedgerMetadataStore.MembershipSnapshot predecessor, Snapshot lifecycle,
                               ProfileRecoveryContext context) { }

    public record RecoveryCapture(ReadStatus status, RecoveryPlan plan) {
        public RecoveryCapture {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (plan != null)) {
                throw new IllegalArgumentException("Only FOUND carries a frozen recovery plan");
            }
        }
    }

    public record RecoveryResult(ProfileLedgerMetadataStore.Outcome outcome, ProfileRecoveryContext context,
                                 ProfileLedgerMetadataStore.MembershipSnapshot metadata) {
        public RecoveryResult {
            Objects.requireNonNull(outcome, "outcome");
            boolean success = outcome == ProfileLedgerMetadataStore.Outcome.APPLIED
                    || outcome == ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED;
            if (success ? context == null || metadata == null : context != null || metadata != null) {
                throw new IllegalArgumentException("Only proved IN_RECOVERY carries its context/metadata");
            }
        }
    }

    /** Freeze once. An UNKNOWN caller must reuse this plan, not silently recapture another birth or version. */
    public RecoveryCapture captureRecovery(ProfileLedgerMetadataStore metadata, long ledger, byte[] operation) {
        return captureRecovery(metadata, ledger, operation, null);
    }

    RecoveryCapture captureRecovery(ProfileLedgerMetadataStore metadata, long ledger, byte[] operation,
            ProfileRecoveryBoundary.WriterClose writerClose) {
        requireRecoveryClient(metadata);
        operation = ProfileLifecycleHead.fixed(operation, 16, true);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var root = read(ledger, deadline);
            var member = metadata.readMembership(ledger, deadline);
            if (root.status() != ReadStatus.FOUND || member.status() != ReadStatus.FOUND) {
                return new RecoveryCapture(root.status() == ReadStatus.QUARANTINED
                        || member.status() == ReadStatus.QUARANTINED ? ReadStatus.QUARANTINED
                        : ReadStatus.UNAVAILABLE, null);
            }
            if (member.snapshot().metadata().getValue().getState() != LedgerMetadata.State.OPEN
                    || member.snapshot().metadata().getValue().getCustomMetadata()
                        .containsKey(ProfileRecoveryBoundary.WRITER_CLOSE_KEY)
                    || !recoveryPublicationMatches(root.snapshot(), member.snapshot(), deadline)) {
                return new RecoveryCapture(ReadStatus.QUARANTINED, null);
            }
            if (writerClose != null && writerClose.prefixEntryId()
                    < member.snapshot().metadata().getValue().getAllEnsembles().lastKey() - 1) {
                return new RecoveryCapture(ReadStatus.QUARANTINED, null);
            }
            var context = ProfileRecoveryContext.capture(member.snapshot(), root.snapshot(), operation, writerClose);
            return new RecoveryCapture(ReadStatus.FOUND, new RecoveryPlan(member.snapshot(), root.snapshot(), context));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RecoveryCapture(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RecoveryCapture(ReadStatus.UNAVAILABLE, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new RecoveryCapture(ReadStatus.QUARANTINED, null);
        }
    }

    public RecoveryResult beginRecovery(ProfileLedgerMetadataStore metadata, RecoveryPlan frozen) {
        return beginRecovery(metadata, frozen, null);
    }

    RecoveryResult beginRecovery(ProfileLedgerMetadataStore metadata, RecoveryPlan frozen,
            ProfileRecoveryBoundary.WriterClose writerClose) {
        requireRecoveryClient(metadata);
        var context = frozen.context();
        long ledger = context.ledger().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var root = read(ledger, deadline);
            if (root.status() != ReadStatus.FOUND) {
                return recoveryFailure(root.status() == ReadStatus.QUARANTINED || root.status() == ReadStatus.NOT_FOUND
                        ? ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        : ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
            }
            if (!context.matchesRoot(root.snapshot()) || !root.snapshot().equals(frozen.lifecycle())
                    || !recoveryPublicationMatches(root.snapshot(), frozen.predecessor(), deadline)) {
                return recoveryFailure(ProfileLedgerMetadataStore.Outcome.CONFLICT);
            }
            var checks = recoveryRootChecks(root.snapshot(), deadline);
            var committed = metadata.enterRecovery(frozen.predecessor(), context, checks, deadline, writerClose);
            if (committed.metadata() == null) {
                return recoveryFailure(committed.outcome());
            }
            var resolved = readRecovery(metadata, ledger);
            return resolved.context() != null && resolved.context().equals(context)
                    ? new RecoveryResult(committed.outcome(), context, resolved.metadata()) : resolved;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.QUARANTINED);
        }
    }

    /** Adopt only the existing durable operation, preserving its generation and identity after restart. */
    public RecoveryResult readRecovery(ProfileLedgerMetadataStore metadata, long ledger) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var root = read(ledger, deadline);
            var member = metadata.readMembership(ledger, deadline);
            if (root.status() != ReadStatus.FOUND || member.status() != ReadStatus.FOUND) {
                return recoveryFailure(root.status() == ReadStatus.QUARANTINED || root.status() == ReadStatus.NOT_FOUND
                        || member.status() == ReadStatus.QUARANTINED || member.status() == ReadStatus.NOT_FOUND
                        ? ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        : ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
            }
            if (member.snapshot().metadata().getValue().getState() != LedgerMetadata.State.IN_RECOVERY) {
                return recoveryFailure(ProfileLedgerMetadataStore.Outcome.CONFLICT);
            }
            var context = ProfileRecoveryContext.decode(
                    member.snapshot().metadata().getValue().getCustomMetadata().get(ProfileRecoveryContext.KEY));
            if (!context.matchesRoot(root.snapshot())) {
                return recoveryFailure(ProfileLedgerMetadataStore.Outcome.CONFLICT);
            }
            var projection = recoveryRepairProjection(context, member.snapshot(), deadline);
            var predecessor = new ProfileLedgerMetadataStore.MembershipSnapshot(context.metadataBirth(),
                    new org.apache.bookkeeper.versioning.Versioned<>(ProfileRecoveryContext.predecessorProjection(
                            projection.metadata().getValue()),
                            new org.apache.bookkeeper.versioning.LongVersion(context.predecessorVersion())));
            if (!context.matchesRoot(root.snapshot()) || !context.matchesRecovery(projection)
                    || !recoveryPublicationMatches(root.snapshot(), predecessor, deadline)) {
                return recoveryFailure(ProfileLedgerMetadataStore.Outcome.CONFLICT);
            }
            metadata.verifyRecoveryGuard(member.snapshot(), deadline);
            verifyRecoveryRootGuard(root.snapshot(), deadline);
            return new RecoveryResult(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, context, member.snapshot());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return recoveryFailure(ProfileLedgerMetadataStore.Outcome.QUARANTINED);
        }
    }

    private ProfileLedgerMetadataStore.MembershipSnapshot recoveryRepairProjection(ProfileRecoveryContext context,
            ProfileLedgerMetadataStore.MembershipSnapshot current, long deadline)
            throws InterruptedException, TimeoutException, java.io.IOException {
        return recoveryRepairProjection(context, current, deadline, null);
    }

    private ProfileLedgerMetadataStore.MembershipSnapshot recoveryRepairProjection(ProfileRecoveryContext context,
            ProfileLedgerMetadataStore.MembershipSnapshot current, long deadline, ProfileRecoveryBoundary candidate)
            throws InterruptedException, TimeoutException, java.io.IOException {
        var value = current.metadata().getValue();
        var head = ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY));
        boolean closed = value.getState() == LedgerMetadata.State.CLOSED;
        var selected = selectedBoundary(context, value, deadline);
        var boundary = selected == null ? candidate : selected.boundary();
        var writerClose = ProfileRecoveryBoundary.writerClose(value);
        if (writerClose != null && (writerClose.prefixEntryId() < value.getAllEnsembles().lastKey() - 1
                || boundary != null && (boundary.prefixEntryId() != writerClose.prefixEntryId()
                    || boundary.prefixLength() != writerClose.prefixLength()))) {
            throw new IllegalArgumentException("Recovery cannot change the frozen writer close prefix");
        }
        int publicationIndex = selected == null ? Integer.MAX_VALUE
                : Math.toIntExact(selected.reference().publicationBeforeVersion() - context.generation());
        int closeIndex = head.size();
        if (value.getCustomMetadata().containsKey(ProfileRecoveryBoundary.CLOSE_KEY)) {
            var close = ProfileRecoveryBoundary.CloseReference.decode(value.getCustomMetadata()
                    .get(ProfileRecoveryBoundary.CLOSE_KEY));
            if (!closed || boundary == null || !close.matches(boundary) || close.headCount() > head.size()
                    || publicationIndex > close.headCount()) {
                throw new IllegalArgumentException("Required close history cut no longer matches its boundary");
            }
            closeIndex = close.headCount();
        }
        boolean origin = boundary == null;
        if (current.birthZxid() != context.metadataBirth() || value.getCToken() != context.creatorToken()
                || value.getState() != LedgerMetadata.State.IN_RECOVERY && !closed
                || !Arrays.equals(value.getCustomMetadata().get(ProfileRecoveryContext.KEY), context.encode())
                || !(current.metadata().getVersion() instanceof org.apache.bookkeeper.versioning.LongVersion version)
                || version.getLongVersion() != context.generation() + head.size()
                    + (selected == null ? 0 : 1) + (closed ? 1 : 0)
                || closed && (selected == null || value.getLastEntryId() != selected.boundary().prefixEntryId()
                    || value.getLength() != selected.boundary().prefixLength())) {
            throw new IllegalArgumentException("Repair commit head no longer matches its recovery context/version");
        }
        if (boundary != null && boundary.beforeVersion() == context.generation() + head.size()) {
            origin = Arrays.equals(boundary.metadataDigest(), ProfileRecoveryContext.metadataDigest(value));
        }
        for (int i = head.size() - 1; i >= 0; i--) {
            var reference = head.get(i);
            var repair = readRepair(value.getLedgerId(), reference.slot(), deadline);
            if (repair == null || !repair.birth().equals(reference.intentBirth())
                    || repair.storeVersion() != reference.intentStoreVersion()
                    || !repair.intent().recovery().equals(context)) {
                throw new IllegalArgumentException("Required committed repair intent is missing or recreated");
            }
            var admission = reads.get(repairPath(value.getLedgerId(), reference.slot()) + "/admitted", deadline);
            if (admission.code() != Code.OK.intValue()) {
                throw new IllegalArgumentException("Committed repair lost its admission");
            }
            validateRepairAdmission(repair, admission, deadline);
            var node = reads.get(repairPath(value.getLedgerId(), reference.slot()) + "/committed", deadline);
            var commit = repairCommit(node);
            validateEmptyBoundary(repair, commit, selected, deadline);
            if (!Arrays.equals(reference.digest(), ProfileRepairIntent.digest(node.bytes()))
                    || commit.beforeVersion() != context.generation() + i + (i >= publicationIndex ? 1 : 0)
                        + (closed && i >= closeIndex ? 1 : 0)
                    || !Arrays.equals(commit.afterDigest(), ProfileRecoveryContext.metadataDigest(value))
                    || !value.getAllEnsembles().get(repair.intent().fragmentStart())
                        .get(repair.intent().replacedMemberSlot()).toString().equals(repair.intent().install()
                            .targetBookie())) {
                throw new IllegalArgumentException("Committed repair no longer matches the standard membership");
            }
            value = ProfileRepairCommit.replace(value, repair.intent(), commit.oldBookie());
            if (!boundedRepairFragment(repair.intent(), value, boundary)
                    || !repairMatchesMetadata(repair.intent(), value)
                    || !Arrays.equals(commit.beforeDigest(), ProfileRecoveryContext.metadataDigest(value))) {
                throw new IllegalArgumentException("Invalid repair predecessor or unbounded fragment");
            }
            if (boundary != null && boundary.beforeVersion() == context.generation() + i) {
                origin = Arrays.equals(boundary.metadataDigest(), ProfileRecoveryContext.metadataDigest(value));
            }
        }
        if (!origin || boundary != null && !context.equals(boundary.context())) {
            throw new IllegalArgumentException("Required prefix selection lost its exact metadata origin");
        }
        var custom = new java.util.TreeMap<>(value.getCustomMetadata());
        custom.remove(ProfileRepairCommit.KEY);
        custom.remove(ProfileRecoveryBoundary.KEY);
        custom.remove(ProfileRecoveryBoundary.CLOSE_KEY);
        value = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(
                ProfileRecoveryContext.predecessorProjection(value))
                .withCToken(value.getCToken())
                .withCustomMetadata(custom).withInRecoveryState().build();
        return new ProfileLedgerMetadataStore.MembershipSnapshot(current.birthZxid(),
                new org.apache.bookkeeper.versioning.Versioned<>(value,
                        new org.apache.bookkeeper.versioning.LongVersion(context.generation())));
    }

    private static ProfileRepairCommit repairCommit(ProfileZKReadClient.Node node) {
        if (node.code() != Code.OK.intValue() || node.stat().getVersion() != 0
                || node.stat().getEphemeralOwner() != 0) {
            throw new IllegalArgumentException("Required immutable repair commit is missing or changed");
        }
        return ProfileRepairCommit.decode(node.bytes());
    }

    private static boolean boundedRepairFragment(ProfileRepairIntent intent, LedgerMetadata metadata,
            ProfileRecoveryBoundary boundary) {
        Long end = metadata.getAllEnsembles().higherKey(intent.fragmentStart());
        if (intent.empty()) {
            return end == null && boundary != null && boundary.context().equals(intent.recovery())
                    && intent.fragmentStart() == metadata.getAllEnsembles().lastKey()
                    && boundary.prefixEntryId() == intent.fragmentStart() - 1;
        }
        return intent.range().firstEntryId() == intent.fragmentStart()
                && (end != null ? intent.range().lastEntryId() == end - 1
                    : boundary != null && boundary.context().equals(intent.recovery())
                        && intent.fragmentStart() == metadata.getAllEnsembles().lastKey()
                        && intent.range().lastEntryId() == boundary.prefixEntryId());
    }

    private static CloseTarget repairCloseTarget(ProfileRepairIntent intent, byte[] operation, long generation,
            BoundarySnapshot selected) {
        if (!intent.empty()) {
            return new CloseTarget(intent.replacedMemberSlot(), intent, intent.closeScope(operation, generation));
        }
        if (selected == null || !intent.recovery().equals(selected.boundary().context())) {
            throw new IllegalArgumentException("EMPTY close lost its selected boundary");
        }
        var binding = new org.apache.bookkeeper.common.profile.control.ProfileControlRequest.TerminalBinding(
                selected.boundary().prefixEntryId(), selected.boundary().prefixLength(), selected.reference().digest());
        return new CloseTarget(intent.replacedMemberSlot(), intent,
                intent.terminalScope(operation, generation, binding),
                binding);
    }

    private static boolean emptyBoundaryMatches(ProfileRepairCommit.EmptyBoundary proof, ProfileRepairIntent intent,
            CloseTarget close) {
        return intent.empty() && close.terminal() != null && Arrays.equals(proof.intentDigest(),
                ProfileRepairIntent.digest(intent.encode())) && Arrays.equals(proof.operationId(),
                    close.scope().operationId()) && proof.generation() == close.scope().authorityGeneration()
                && proof.binding().equals(close.terminal());
    }

    private void validateEmptyBoundary(RepairSnapshot repair, ProfileRepairCommit commit, BoundarySnapshot selected,
            long deadline) throws InterruptedException, TimeoutException {
        if (!repair.intent().empty()) {
            if (commit.emptyBoundaryDigest() != null) {
                throw new IllegalArgumentException("DATA commit carries an EMPTY boundary proof");
            }
            return;
        }
        var node = reads.get(repairPath(repair.intent().recovery().ledger().ledgerId(), repair.intent().slot())
                + "/empty-boundary", deadline);
        requireFound(node);
        requirePersistent(node, true);
        var proof = ProfileRepairCommit.EmptyBoundary.decode(node.bytes());
        if (commit.emptyBoundaryDigest() == null || !Arrays.equals(commit.emptyBoundaryDigest(),
                ProfileRepairIntent.digest(node.bytes())) || !emptyBoundaryMatches(proof, repair.intent(),
                    repairCloseTarget(repair.intent(), commit.closeOperationId(),
                            commit.closeGeneration(), selected))) {
            throw new IllegalArgumentException("EMPTY membership lost its original boundary proof");
        }
    }

    public record CloseTarget(int member, ProfileRepairIntent intent, ProfileControlScope scope,
            org.apache.bookkeeper.common.profile.control.ProfileControlRequest.TerminalBinding terminal) {
        public CloseTarget(int member, ProfileRepairIntent intent, ProfileControlScope scope) {
            this(member, intent, scope, null);
        }
        public org.apache.bookkeeper.common.profile.control.ProfileControlRequest request() {
            return terminal != null ? org.apache.bookkeeper.common.profile.control.ProfileControlRequest
                    .terminalClose(scope, terminal)
                    : org.apache.bookkeeper.common.profile.control.ProfileControlRequest.recoveryClose(scope,
                        new org.apache.bookkeeper.common.profile.control.ProfileControlRequest.RecoveryBinding(
                            intent.intentId(), intent.intentGeneration(), intent.grantGeneration()),
                        intent.grantOperationId());
        }
    }

    public record ClosingPlan(ProfileRecoveryContext context,
            ProfileLedgerMetadataStore.MembershipSnapshot metadata, BoundarySnapshot selection,
            List<CloseTarget> targets) {
        public ClosingPlan {
            targets = List.copyOf(targets);
        }
    }

    public record ClosingPlanResult(CreateStatus status, ClosingPlan plan) { }

    /** Only actual current-member full-copy results can enter the final quorum; this read produces no ACK. */
    public ClosingPlanResult closingPlan(ProfileLedgerMetadataStore metadata, ProfileRecoveryContext context) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRecovery(metadata, context.ledger().ledgerId());
            if (current.context() == null || !context.equals(current.context())) {
                return new ClosingPlanResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        ? CreateStatus.DURABILITY_UNKNOWN : current.outcome()
                            == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                ? CreateStatus.QUARANTINED : CreateStatus.CONFLICT, null);
            }
            var value = current.metadata().metadata().getValue();
            var selected = selectedBoundary(context, value, deadline);
            if (selected == null || value.getEnsembleSize() != 3 || value.getWriteQuorumSize() != 3
                    || value.getAckQuorumSize() < 2 || value.getAckQuorumSize() > 3
                    || selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey() - 1) {
                return new ClosingPlanResult(CreateStatus.CONFLICT, null);
            }
            if (selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey()) {
                return new ClosingPlanResult(CreateStatus.ALREADY_APPLIED,
                        new ClosingPlan(context, current.metadata(), selected,
                                emptyClosingTargets(context, value, selected, deadline)));
            }
            var targets = new java.util.TreeMap<Integer, CloseTarget>();
            for (var ref : ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY))) {
                var repair = readRepair(value.getLedgerId(), ref.slot(), deadline);
                var intent = repair.intent();
                if (intent.fragmentStart() != value.getAllEnsembles().lastKey()
                        || !boundedRepairFragment(intent, value, selected.boundary())
                        || !value.getAllEnsembles().lastEntry().getValue().get(intent.replacedMemberSlot()).toString()
                            .equals(intent.install().targetBookie())) {
                    continue;
                }
                var committed = repairCommit(reads.get(repairPath(value.getLedgerId(), ref.slot()) + "/committed",
                        deadline));
                targets.put(intent.replacedMemberSlot(), new CloseTarget(intent.replacedMemberSlot(), intent,
                        intent.closeScope(committed.closeOperationId(), committed.closeGeneration())));
            }
            return new ClosingPlanResult(CreateStatus.ALREADY_APPLIED,
                    new ClosingPlan(context, current.metadata(), selected, List.copyOf(targets.values())));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ClosingPlanResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ClosingPlanResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new ClosingPlanResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Activated or published EMPTY targets only. Selection itself grants no DATA range or new member. */
    private List<CloseTarget> emptyClosingTargets(ProfileRecoveryContext context,
            org.apache.bookkeeper.client.api.LedgerMetadata metadata, BoundarySnapshot selected, long deadline)
            throws InterruptedException, TimeoutException {
        var installs = new java.util.HashMap<String, ProfileControlScope>();
        recoveryCurrentTargets(context, deadline).forEach(receipt ->
                installs.put(receipt.scope().targetBookie(), receipt.scope()));
        var targets = new java.util.ArrayList<CloseTarget>();
        var members = metadata.getAllEnsembles().lastEntry().getValue();
        var boundary = selected.boundary();
        var binding = new org.apache.bookkeeper.common.profile.control.ProfileControlRequest.TerminalBinding(
                boundary.prefixEntryId(), boundary.prefixLength(), selected.reference().digest());
        for (int slot = 0; slot < members.size(); slot++) {
            boolean repaired = false;
            for (var ref : ProfileRepairCommit.head(metadata.getCustomMetadata().get(ProfileRepairCommit.KEY))) {
                var repair = readRepair(metadata.getLedgerId(), ref.slot(), deadline);
                var intent = repair.intent();
                if (!intent.empty() || intent.fragmentStart() != metadata.getAllEnsembles().lastKey()
                        || intent.replacedMemberSlot() != slot
                        || !intent.install().targetBookie().equals(members.get(slot).toString())) {
                    continue;
                }
                var commit = repairCommit(reads.get(repairPath(metadata.getLedgerId(), ref.slot()) + "/committed",
                        deadline));
                validateEmptyBoundary(repair, commit, selected, deadline);
                targets.add(repairCloseTarget(intent, commit.closeOperationId(), commit.closeGeneration(), selected));
                repaired = true;
                break;
            }
            if (repaired) {
                continue;
            }
            var install = installs.get(members.get(slot).toString());
            if (install == null) {
                throw new IllegalArgumentException("Empty last fragment has no retained activated target");
            }
            byte[] bookie = install.targetBookie().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] operation = Arrays.copyOf(ProfileRepairIntent.digest(java.nio.ByteBuffer.allocate(
                    ProfileRecoveryContext.ENCODED_BYTES + 52 + bookie.length).put(context.encode())
                    .put(selected.reference().digest()).putInt(slot).put(install.targetStorageIncarnation())
                    .put(bookie).array()), 16);
            var base = new ProfileControlScope(ProfileControlOperation.RECOVERY_TERMINAL_CLOSE,
                    ProfileAuthorityPurpose.RECOVERY_TERMINAL_CLOSE, context.ledger().ledgerId(),
                    context.ledger().ledgerInstanceId(), install.descriptorIdentity(), install.targetBookie(),
                    install.targetStorageIncarnation(), "recovery-terminal", context.generation(),
                    operation, new byte[32], null);
            var scope = new ProfileControlScope(base.operation(), base.purpose(), base.ledgerId(),
                    base.ledgerInstanceId(), base.descriptorIdentity(), base.targetBookie(),
                    base.targetStorageIncarnation(), base.authorityDomain(), base.authorityGeneration(),
                    base.operationId(), org.apache.bookkeeper.common.profile.control.ProfileControlRequest
                        .terminalPayloadIdentity(base, binding), null);
            targets.add(new CloseTarget(slot, null, scope, binding));
        }
        return List.copyOf(targets);
    }

    private static boolean closingTargetMatches(CloseTarget target, ClosingPlan plan) {
        var value = plan.metadata().metadata().getValue();
        if (target.intent() != null) {
            return (target.intent().empty() ? target.terminal() != null : target.terminal() == null)
                    && target.intent().recovery().equals(plan.context())
                    && boundedRepairFragment(target.intent(), value, plan.selection().boundary())
                    && (!target.intent().empty() || target.terminal().prefixEntryId()
                        == plan.selection().boundary().prefixEntryId()
                        && target.terminal().prefixLength() == plan.selection().boundary().prefixLength()
                        && Arrays.equals(target.terminal().selectionDigest(), plan.selection().reference().digest()));
        }
        var selected = plan.selection();
        return target.terminal() != null && selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey()
                && target.scope().operation() == ProfileControlOperation.RECOVERY_TERMINAL_CLOSE
                && target.scope().authorityGeneration() == plan.context().generation()
                && target.terminal().prefixEntryId() == selected.boundary().prefixEntryId()
                && target.terminal().prefixLength() == selected.boundary().prefixLength()
                && Arrays.equals(target.terminal().selectionDigest(), selected.reference().digest());
    }

    public record TerminalAck(CloseTarget target, org.apache.bookkeeper.common.profile.wire.ProfileHello.Server hello,
            org.apache.bookkeeper.common.profile.control.ProfileControlResult result) { }

    public record ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome outcome,
            ProfileLedgerMetadataStore.MembershipSnapshot metadata, BoundarySnapshot selection) { }

    /** Resolve only matching standard CLOSED, including its complete retained recovery origin. Never reopen. */
    public ClosedRecoveryResult readClosedRecovery(ProfileLedgerMetadataStore metadata,
            ProfileRecoveryContext context) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var root = read(context.ledger().ledgerId(), deadline);
            var member = metadata.readMembership(context.ledger().ledgerId(), deadline);
            if (root.status() != ReadStatus.FOUND || member.status() != ReadStatus.FOUND) {
                return new ClosedRecoveryResult(root.status() == ReadStatus.UNAVAILABLE
                        || member.status() == ReadStatus.UNAVAILABLE
                            ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                            : ProfileLedgerMetadataStore.Outcome.QUARANTINED, null, null);
            }
            var value = member.snapshot().metadata().getValue();
            if (value.getState() != LedgerMetadata.State.CLOSED) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            boolean deleted = closedAfterDelete(context, root.snapshot());
            if (!context.matchesRoot(root.snapshot()) && !deleted) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            var projection = recoveryRepairProjection(context,
                    deleteProjection(root.snapshot(), member.snapshot(), deadline), deadline);
            var predecessor = new ProfileLedgerMetadataStore.MembershipSnapshot(context.metadataBirth(),
                    new org.apache.bookkeeper.versioning.Versioned<>(ProfileRecoveryContext.predecessorProjection(
                            projection.metadata().getValue()),
                            new org.apache.bookkeeper.versioning.LongVersion(context.predecessorVersion())));
            boolean publicationMatches;
            if (deleted && context.lifecycleSequence() == 2) {
                var page = page(root.snapshot(), deadline);
                publicationMatches = page != null
                        && page.storeVersion() == root.snapshot().lifecycle().readyPageVersion()
                        && Arrays.equals(page.digest(), root.snapshot().lifecycle().readyPageDigest())
                        && page.page().matchesMetadata(predecessor.metadata());
            } else if (deleted) {
                var page = replacement(root.snapshot(), context.lifecycleSequence(), deadline);
                publicationMatches = page != null
                        && page.storeVersion() == root.snapshot().lifecycle().readyPageVersion()
                        && Arrays.equals(page.digest(), root.snapshot().lifecycle().readyPageDigest())
                        && page.page().matchesMetadata(predecessor);
            } else {
                publicationMatches = recoveryPublicationMatches(root.snapshot(), predecessor, deadline);
            }
            if (!context.matchesRecovery(projection) || !publicationMatches) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            metadata.verifyRecoveryGuard(member.snapshot(), deadline);
            verifyRecoveryRootGuard(root.snapshot(), deadline);
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, member.snapshot(),
                    selectedBoundary(context, value, deadline));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.QUARANTINED, null, null);
        }
    }

    public enum ReadOnlyStatus { READABLE_CLOSED, NOT_CLOSED, DELETED, CONFLICT, QUARANTINED, UNAVAILABLE }

    public record ReadOnlySnapshot(ProfileRecoveryContext context, ClosedRecoveryResult closed,
            List<ProfileInstallReceipt> sources) {
        public ReadOnlySnapshot {
            sources = List.copyOf(sources);
        }
    }

    public record ReadOnlyRead(ReadOnlyStatus status, ReadOnlySnapshot snapshot) {
        public ReadOnlyRead {
            Objects.requireNonNull(status, "status");
            if ((status == ReadOnlyStatus.READABLE_CLOSED) != (snapshot != null)) {
                throw new IllegalArgumentException("Only proved readonly admission carries its snapshot");
            }
        }
    }

    /** Independent open: derive the token from actual CLOSED, never adopt a historical close after DELETE. */
    public ReadOnlyRead readClosedForOpen(ProfileLedgerMetadataStore metadata, long ledger) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var first = metadata.readMembership(ledger, deadline);
            if (first.status() != ReadStatus.FOUND) {
                return new ReadOnlyRead(first.status() == ReadStatus.UNAVAILABLE
                        ? ReadOnlyStatus.UNAVAILABLE : ReadOnlyStatus.QUARANTINED, null);
            }
            if (first.snapshot().metadata().getValue().getState() != LedgerMetadata.State.CLOSED) {
                return new ReadOnlyRead(ReadOnlyStatus.NOT_CLOSED, null);
            }
            var context = ProfileRecoveryContext.decode(first.snapshot().metadata().getValue()
                    .getCustomMetadata().get(ProfileRecoveryContext.KEY));
            var root = read(ledger, deadline);
            if (root.status() != ReadStatus.FOUND) {
                return new ReadOnlyRead(root.status() == ReadStatus.UNAVAILABLE
                        ? ReadOnlyStatus.UNAVAILABLE : ReadOnlyStatus.QUARANTINED, null);
            }
            if (root.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                return new ReadOnlyRead(ReadOnlyStatus.DELETED, null);
            }
            if (!context.matchesRoot(root.snapshot())) {
                return new ReadOnlyRead(ReadOnlyStatus.CONFLICT, null);
            }
            var closed = readClosedRecovery(metadata, context);
            if (closed.metadata() == null) {
                return new ReadOnlyRead(closed.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        ? ReadOnlyStatus.QUARANTINED : closed.outcome()
                            == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                                ? ReadOnlyStatus.UNAVAILABLE : ReadOnlyStatus.CONFLICT, null);
            }
            var retained = retainedActivationTargets(ledger);
            if (retained.status() != ReadStatus.FOUND) {
                return new ReadOnlyRead(retained.status() == ReadStatus.UNAVAILABLE
                        ? ReadOnlyStatus.UNAVAILABLE : ReadOnlyStatus.QUARANTINED, null);
            }
            var members = new java.util.HashSet<String>();
            closed.metadata().metadata().getValue().getAllEnsembles().values().forEach(ensemble ->
                    ensemble.forEach(bookie -> members.add(bookie.toString())));
            var sources = new java.util.LinkedHashMap<String, ProfileInstallReceipt>();
            for (var receipt : retained.targets()) {
                String name = receipt.scope().targetBookie();
                if (!members.contains(name)) {
                    continue;
                }
                var prior = sources.putIfAbsent(name, receipt);
                if (prior != null && !Arrays.equals(prior.scope().targetStorageIncarnation(),
                        receipt.scope().targetStorageIncarnation())) {
                    return new ReadOnlyRead(ReadOnlyStatus.QUARANTINED, null);
                }
            }
            if (!sources.keySet().containsAll(members)) {
                return new ReadOnlyRead(ReadOnlyStatus.QUARANTINED, null);
            }
            var afterRoot = read(ledger, deadline);
            var after = metadata.readMembership(ledger, deadline);
            if (afterRoot.status() != ReadStatus.FOUND || after.status() != ReadStatus.FOUND) {
                return new ReadOnlyRead(afterRoot.status() == ReadStatus.UNAVAILABLE
                        || after.status() == ReadStatus.UNAVAILABLE
                            ? ReadOnlyStatus.UNAVAILABLE : ReadOnlyStatus.QUARANTINED, null);
            }
            if (afterRoot.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                return new ReadOnlyRead(ReadOnlyStatus.DELETED, null);
            }
            if (!context.matchesRoot(afterRoot.snapshot()) || after.snapshot().birthZxid()
                    != closed.metadata().birthZxid() || !after.snapshot().metadata().getVersion()
                        .equals(closed.metadata().metadata().getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(after.snapshot().metadata().getValue(),
                        closed.metadata().metadata().getValue())) {
                return new ReadOnlyRead(ReadOnlyStatus.CONFLICT, null);
            }
            return new ReadOnlyRead(ReadOnlyStatus.READABLE_CLOSED,
                    new ReadOnlySnapshot(context, closed, List.copyOf(sources.values())));
        } catch (IllegalStateException unavailable) {
            return new ReadOnlyRead(ReadOnlyStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new ReadOnlyRead(ReadOnlyStatus.QUARANTINED, null);
        }
    }

    private static boolean closedAfterDelete(ProfileRecoveryContext context, Snapshot root) {
        return root.lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT
                && root.birthZxid() == context.lifecycleBirthZxid()
                && root.lifecycle().birthId().equals(context.lifecycleBirth())
                && root.lifecycle().sequence() == context.lifecycleSequence() + 1
                && root.lifecycle().predecessor() == context.lifecycleSequence()
                && root.lifecycleDataVersion() == context.lifecycleVersion() + 1
                && root.lifecycle().controlGeneration() == context.controlGeneration() + 1
                && root.reservation().backlink().ledgerId() == context.ledger().ledgerId()
                && Arrays.equals(root.reservation().backlink().instanceId(), context.ledger().ledgerInstanceId())
                && Arrays.equals(root.reservation().installs().get(0).descriptorIdentity().toBytes(),
                        context.ledger().descriptorIdentity());
    }

    /** ACKs are caller inputs from actual UIC4 RPCs; metadata guards do not manufacture remote durability. */
    public ClosedRecoveryResult closeRecovery(ProfileLedgerMetadataStore metadata, ClosingPlan plan,
            List<TerminalAck> acknowledgments) {
        requireRecoveryClient(metadata);
        if (acknowledgments.size() > 3) {
            throw new IllegalArgumentException("Closing acknowledgments exceed the supported ensemble");
        }
        var value = plan.metadata().metadata().getValue();
        var ack = new org.apache.bookkeeper.client.RoundRobinDistributionSchedule(value.getWriteQuorumSize(),
                value.getAckQuorumSize(), value.getEnsembleSize()).getAckSet();
        boolean quorum = false;
        try {
            for (var evidence : acknowledgments) {
                var target = evidence.target();
                var reply = evidence.result();
                if (!plan.targets().contains(target) || target.member() < 0
                        || target.member() >= value.getEnsembleSize()
                        || !value.getAllEnsembles().lastEntry().getValue().get(target.member()).toString()
                            .equals(target.scope().targetBookie())
                        || !closingTargetMatches(target, plan)
                        || !evidence.hello().bookieId().equals(target.scope().targetBookie())
                        || !Arrays.equals(evidence.hello().storageIncarnation(),
                            target.scope().targetStorageIncarnation())
                        || evidence.hello().activeEngine() != 3 || !evidence.hello().capabilities().contains(
                            org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec
                                .terminalCapability(target.terminal() != null))
                        || !reply.durable() || reply.localGeneration() <= 0
                        || reply.operation() != target.scope().operation()
                        || reply.ledgerId() != value.getLedgerId()
                        || reply.authorityGeneration() != target.scope().authorityGeneration()
                        || !reply.operationIdHex().equals(java.util.HexFormat.of().formatHex(
                            target.scope().operationId()))
                        || reply.outcome() != org.apache.bookkeeper.common.profile.control.ProfileControlResult
                            .Outcome.APPLIED && reply.outcome() != org.apache.bookkeeper.common.profile.control
                                .ProfileControlResult.Outcome.ALREADY_APPLIED) {
                    continue;
                }
                quorum |= ack.completeBookieAndCheck(target.member());
            }
        } finally {
            ack.recycle();
        }
        if (!quorum) {
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        }
        var context = plan.context();
        long ledger = context.ledger().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var existing = readClosedRecovery(metadata, context);
            if (existing.metadata() != null) {
                return existing;
            }
            var current = readRecovery(metadata, ledger);
            if (current.context() == null || !context.equals(current.context())) {
                return new ClosedRecoveryResult(current.outcome(), null, null);
            }
            if (current.metadata().birthZxid() != plan.metadata().birthZxid()
                    || !current.metadata().metadata().getVersion().equals(plan.metadata().metadata().getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(value, current.metadata().metadata().getValue())) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            var root = read(ledger, deadline);
            if (root.status() != ReadStatus.FOUND || !context.matchesRoot(root.snapshot())) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            var selected = selectedBoundary(context, value, deadline);
            if (selected == null || !selected.equals(plan.selection())) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            if (selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey()
                    && !plan.targets().equals(emptyClosingTargets(context, value, selected, deadline))) {
                return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null);
            }
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(current.metadata()));
            checks.add(Op.check(boundaryPath(ledger), 0));
            checks.add(Op.check(boundaryBirthGuard(ledger, selected.birthZxid()), 0));
            checks.add(metadata.closedMetadataWrite(current.metadata(), selected.boundary()));
            int code = multi(checks, deadline);
            var resolved = readClosedRecovery(metadata, context);
            if (resolved.metadata() != null) {
                return new ClosedRecoveryResult(code == Code.OK.intValue() ? ProfileLedgerMetadataStore.Outcome.APPLIED
                        : ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED,
                        resolved.metadata(), resolved.selection());
            }
            return new ClosedRecoveryResult(conditionalFailure(code) ? ProfileLedgerMetadataStore.Outcome.CONFLICT
                    : resolved.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        ? resolved.outcome() : ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN, null, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new ClosedRecoveryResult(ProfileLedgerMetadataStore.Outcome.QUARANTINED, null, null);
        }
    }

    public record BoundarySnapshot(ProfileRecoveryBoundary boundary, long birthZxid,
            ProfileRecoveryBoundary.Reference reference) { }

    public record BoundaryResult(CreateStatus status, BoundarySnapshot snapshot) { }

    public BoundaryResult readSelectedPrefix(ProfileLedgerMetadataStore metadata, ProfileRecoveryContext context) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, context.ledger().ledgerId());
            if (current.context() == null || !context.equals(current.context())) {
                return new BoundaryResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        ? CreateStatus.DURABILITY_UNKNOWN : current.outcome()
                            == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                ? CreateStatus.QUARANTINED : CreateStatus.CONFLICT, null);
            }
            var selected = selectedBoundary(context, current.metadata().metadata().getValue(), deadline);
            return new BoundaryResult(selected == null ? CreateStatus.CONFLICT
                    : CreateStatus.ALREADY_APPLIED, selected);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new BoundaryResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new BoundaryResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new BoundaryResult(CreateStatus.QUARANTINED, null);
        }
    }

    private String boundaryPath(long ledger) {
        return path(ledger) + "/recovery-prefix";
    }

    private BoundarySnapshot selectedBoundary(ProfileRecoveryContext context, LedgerMetadata value, long deadline)
            throws InterruptedException, TimeoutException {
        byte[] bytes = value.getCustomMetadata().get(ProfileRecoveryBoundary.KEY);
        if (bytes == null) {
            return null;
        }
        var reference = ProfileRecoveryBoundary.reference(bytes);
        var node = reads.get(boundaryPath(context.ledger().ledgerId()), deadline);
        requireFound(node);
        requirePersistent(node, true);
        var boundary = ProfileRecoveryBoundary.decode(node.bytes());
        var guard = reads.get(boundaryBirthGuard(context.ledger().ledgerId(), node.stat().getCzxid()), deadline);
        requireFound(guard);
        requirePersistent(guard, true);
        if (node.stat().getCzxid() != reference.birthZxid() || !boundary.context().equals(context)
                || !Arrays.equals(guard.bytes(), boundaryGuardBytes(node))
                || !Arrays.equals(reference.digest(), ProfileRepairIntent.digest(node.bytes()))
                || reference.publicationBeforeVersion() < boundary.beforeVersion()
                || reference.publicationBeforeVersion() > context.generation()
                    + ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY)).size()) {
            throw new IllegalArgumentException("Required recovery prefix is missing, recreated or mismatched");
        }
        return new BoundarySnapshot(boundary, node.stat().getCzxid(), reference);
    }

    private String boundaryBirthGuard(long ledger, long birth) {
        return boundaryPath(ledger) + "/birth-" + Long.toHexString(birth);
    }

    private static byte[] boundaryGuardBytes(ProfileZKReadClient.Node node) {
        return java.nio.ByteBuffer.allocate(40).putLong(node.stat().getCzxid())
                .put(ProfileRepairIntent.digest(node.bytes())).array();
    }

    /** One verified caller read proof, fixed once. It neither produces AQ nor closes standard metadata. */
    public BoundaryResult selectPrefix(ProfileLedgerMetadataStore metadata,
            ProfileRecoveryCoordinator.PrefixResult proof) {
        requireRecoveryClient(metadata);
        if (proof.status() != ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL
                && proof.status() != ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_WRITER_PREFIX
                || proof.metadata() == null) {
            throw new IllegalArgumentException("Prefix selection requires the verified read predicate");
        }
        var context = proof.context();
        long ledger = context.ledger().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRecovery(metadata, ledger);
            if (current.context() == null || !context.equals(current.context())) {
                return new BoundaryResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.CONFLICT, null);
            }
            var selected = selectedBoundary(context, current.metadata().metadata().getValue(), deadline);
            if (selected != null) {
                return new BoundaryResult(selected.boundary().prefixEntryId() == proof.prefixEntryId()
                        && selected.boundary().prefixLength() == proof.prefixLength()
                            ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        selected.boundary().prefixEntryId() == proof.prefixEntryId()
                            && selected.boundary().prefixLength() == proof.prefixLength() ? selected : null);
            }
            if (proof.metadata().birthZxid() != current.metadata().birthZxid()
                    || !proof.metadata().metadata().getVersion().equals(current.metadata().metadata().getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(proof.metadata().metadata().getValue(),
                            current.metadata().metadata().getValue())) {
                return new BoundaryResult(CreateStatus.CONFLICT, null);
            }
            if (proof.requiredFrontier() < current.metadata().metadata().getValue().getAllEnsembles().lastKey() - 1) {
                return new BoundaryResult(CreateStatus.CONFLICT, null);
            }
            var boundary = new ProfileRecoveryBoundary(context, Math.toIntExact(((org.apache.bookkeeper.versioning
                    .LongVersion) proof.metadata().metadata().getVersion()).getLongVersion()),
                    ProfileRecoveryContext.metadataDigest(proof.metadata().metadata().getValue()),
                    proof.requiredFrontier(), proof.prefixEntryId(), proof.prefixLength());
            var root = read(ledger, deadline);
            if (root.status() != ReadStatus.FOUND || !context.matchesRoot(root.snapshot())) {
                return new BoundaryResult(CreateStatus.CONFLICT, null);
            }
            String target = boundaryPath(ledger);
            var before = reads.get(target, deadline);
            if (before.code() == Code.NONODE.intValue()) {
                var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
                checks.addAll(metadata.checksForMembership(current.metadata()));
                checks.add(Op.create(target, boundary.encode(), pageAcls, CreateMode.PERSISTENT));
                multi(checks, deadline);
            } else if (before.code() != Code.OK.intValue()) {
                throw new IllegalStateException("Prefix candidate unavailable");
            }
            var candidate = reads.get(target, deadline);
            requireFound(candidate);
            requirePersistent(candidate, true);
            var candidateBoundary = ProfileRecoveryBoundary.decode(candidate.bytes());
            if (!candidateBoundary.context().equals(context)
                    || candidateBoundary.prefixEntryId() != boundary.prefixEntryId()
                    || candidateBoundary.prefixLength() != boundary.prefixLength()) {
                return new BoundaryResult(CreateStatus.CONFLICT, null);
            }
            boundary = candidateBoundary;
            recoveryRepairProjection(context, current.metadata(), deadline, boundary);
            var reference = new ProfileRecoveryBoundary.Reference(Math.toIntExact(((org.apache.bookkeeper.versioning
                    .LongVersion) current.metadata().metadata().getVersion()).getLongVersion()),
                    candidate.stat().getCzxid(), ProfileRepairIntent.digest(candidate.bytes()));
            String guardPath = boundaryBirthGuard(ledger, candidate.stat().getCzxid());
            var guard = reads.get(guardPath, deadline);
            if (guard.code() == Code.NONODE.intValue()) {
                var birthChecks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
                birthChecks.addAll(metadata.checksForMembership(current.metadata()));
                birthChecks.add(Op.check(target, 0));
                birthChecks.add(Op.create(guardPath, boundaryGuardBytes(candidate), immutableAcls,
                        CreateMode.PERSISTENT));
                multi(birthChecks, deadline);
                guard = reads.get(guardPath, deadline);
            }
            requireFound(guard);
            requirePersistent(guard, true);
            if (!Arrays.equals(guard.bytes(), boundaryGuardBytes(candidate))) {
                return new BoundaryResult(CreateStatus.QUARANTINED, null);
            }
            var desired = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(
                    current.metadata().metadata().getValue()).withCToken(context.creatorToken());
            var custom = new java.util.TreeMap<>(current.metadata().metadata().getValue().getCustomMetadata());
            custom.put(ProfileRecoveryBoundary.KEY, reference.encode());
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(current.metadata()));
            checks.add(Op.check(target, 0));
            checks.add(Op.check(guardPath, 0));
            checks.add(metadata.repairMetadataWrite(current.metadata(), desired.withCustomMetadata(custom).build()));
            int code = multi(checks, deadline);
            var after = readRecovery(metadata, ledger);
            if (after.context() != null && context.equals(after.context())) {
                var actual = selectedBoundary(context, after.metadata().metadata().getValue(), deadline);
                if (actual != null && actual.boundary().equals(boundary)) {
                    return new BoundaryResult(code == Code.OK.intValue() ? CreateStatus.APPLIED
                            : CreateStatus.ALREADY_APPLIED, actual);
                }
            }
            return new BoundaryResult(conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new BoundaryResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new BoundaryResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new BoundaryResult(CreateStatus.QUARANTINED, null);
        }
    }

    List<ProfileInstallReceipt> recoveryCurrentTargets(ProfileRecoveryContext context, long deadline)
            throws InterruptedException, TimeoutException {
        long now = System.nanoTime();
        deadline = now + Math.min(deadline - now, timeoutNanos);
        var root = read(context.ledger().ledgerId(), deadline);
        if (root.status() != ReadStatus.FOUND || !context.matchesRoot(root.snapshot())) {
            throw new IllegalArgumentException("Recovery lifecycle changed before selecting fence targets");
        }
        var retained = retainedActivationTargets(context.ledger().ledgerId());
        if (retained.status() != ReadStatus.FOUND) {
            throw new IllegalArgumentException("Required activation history is unavailable");
        }
        var activated = new java.util.LinkedHashMap<String, ProfileInstallReceipt>();
        page(root.snapshot(), deadline).page().installs().forEach(receipt ->
                activated.put(receipt.scope().targetBookie(), receipt));
        for (long sequence = 3; sequence <= context.lifecycleSequence(); sequence++) {
            replacement(root.snapshot(), sequence, deadline).page().installs().forEach(receipt ->
                    activated.put(receipt.scope().targetBookie(), receipt));
        }
        return List.copyOf(activated.values());
    }

    boolean supportsWorker(ProfileLedgerMetadataStore metadata,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker) {
        return metadata.usesMutationClient(zk) && worker.usesMutationClient(zk);
    }

    ProfileRecoveryCoordinator.MarkerCleanupStatus completeReplicaMarker(ProfileLedgerMetadataStore metadata,
            ReadOnlySnapshot closed, org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task) {
        requireRecoveryClient(metadata);
        if (!worker.usesMutationClient(zk)) {
            return ProfileRecoveryCoordinator.MarkerCleanupStatus.CONFLICT;
        }
        long deadline = System.nanoTime() + timeoutNanos;
        long ledger = closed.context().ledger().ledgerId();
        try {
            var root = read(ledger, deadline);
            if (root.status() != ReadStatus.FOUND) {
                return root.status() == ReadStatus.UNAVAILABLE ? ProfileRecoveryCoordinator.MarkerCleanupStatus.DEFERRED
                        : ProfileRecoveryCoordinator.MarkerCleanupStatus.QUARANTINED;
            }
            if (root.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                return ProfileRecoveryCoordinator.MarkerCleanupStatus.DELETED;
            }
            if (!closed.context().matchesRoot(root.snapshot())) {
                return ProfileRecoveryCoordinator.MarkerCleanupStatus.CONFLICT;
            }
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(closed.closed().metadata()));
            return worker.markProfileLedgerReplicated(task, checks)
                    ? ProfileRecoveryCoordinator.MarkerCleanupStatus.CLEARED
                    : ProfileRecoveryCoordinator.MarkerCleanupStatus.CONFLICT;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ProfileRecoveryCoordinator.MarkerCleanupStatus.DEFERRED;
        } catch (TimeoutException | IllegalStateException
                | org.apache.bookkeeper.replication.ReplicationException.UnavailableException unavailable) {
            return ProfileRecoveryCoordinator.MarkerCleanupStatus.DEFERRED;
        } catch (IllegalArgumentException invalid) {
            return ProfileRecoveryCoordinator.MarkerCleanupStatus.QUARANTINED;
        }
    }

    private void requireRecoveryClient(ProfileLedgerMetadataStore metadata) {
        if (!metadata.usesMutationClient(zk)) {
            throw new IllegalArgumentException("Recovery requires the same borrowed ZK mutation context");
        }
    }

    private boolean recoveryPublicationMatches(Snapshot current,
            ProfileLedgerMetadataStore.MembershipSnapshot predecessor, long deadline)
            throws InterruptedException, TimeoutException {
        if (ProfileRecoveryBoundary.writerClose(predecessor.metadata().getValue()) != null) {
            var value = predecessor.metadata().getValue();
            var custom = new java.util.TreeMap<>(value.getCustomMetadata());
            custom.remove(ProfileRecoveryBoundary.WRITER_CLOSE_KEY);
            predecessor = new ProfileLedgerMetadataStore.MembershipSnapshot(predecessor.birthZxid(),
                    new org.apache.bookkeeper.versioning.Versioned<>(org.apache.bookkeeper.client
                        .LedgerMetadataBuilder.from(value).withCToken(value.getCToken())
                        .withCustomMetadata(custom).build(), predecessor.metadata().getVersion()));
        }
        if (current.lifecycle().state() == ProfileLifecycleHead.State.READY) {
            var page = page(current, deadline);
            return page != null && page.storeVersion() == current.lifecycle().readyPageVersion()
                    && Arrays.equals(page.digest(), current.lifecycle().readyPageDigest())
                    && page.page().matchesMetadata(predecessor.metadata());
        }
        if (current.lifecycle().state() == ProfileLifecycleHead.State.REPLACEMENT) {
            var page = replacement(current, current.lifecycle().sequence(), deadline);
            return page != null && page.storeVersion() == current.lifecycle().readyPageVersion()
                    && Arrays.equals(page.digest(), current.lifecycle().readyPageDigest())
                    && page.page().matchesMetadata(predecessor);
        }
        return false;
    }

    private void verifyRecoveryRootGuard(Snapshot current, long deadline)
            throws InterruptedException, TimeoutException {
        String guard = headPath(current.reservation().backlink().ledgerId())
                + "/recovery-birth-" + Long.toHexString(current.birthZxid());
        var observed = reads.get(guard, deadline);
        if (observed.code() == Code.NONODE.intValue()) {
            throw new IllegalArgumentException("Required recovery lifecycle birth guard is missing");
        }
        requireFound(observed);
        if (observed.stat().getEphemeralOwner() != 0 || observed.stat().getVersion() != 0
                || !Arrays.equals(recoveryRootGuardBytes(current), observed.bytes())) {
            throw new IllegalArgumentException("Required recovery lifecycle birth guard is missing or corrupt");
        }
    }

    private static byte[] recoveryRootGuardBytes(Snapshot current) {
        return java.nio.ByteBuffer.allocate(84).putInt(0x55575247).putInt(1)
                .putLong(current.birthZxid()).putLong(current.lifecycle().birthId().getMostSignificantBits())
                .putLong(current.lifecycle().birthId().getLeastSignificantBits())
                .put(current.reservation().backlink().encode()).array();
    }

    private List<Op> recoveryRootChecks(Snapshot current, long deadline)
            throws InterruptedException, TimeoutException {
        long ledger = current.reservation().backlink().ledgerId();
        String guard = headPath(ledger) + "/recovery-birth-" + Long.toHexString(current.birthZxid());
        byte[] bytes = recoveryRootGuardBytes(current);
        var observed = reads.get(guard, deadline);
        if (observed.code() == Code.NONODE.intValue()) {
            multi(List.of(Op.check(headPath(ledger), current.lifecycleDataVersion()),
                    Op.check(marker(headPath(ledger), current.lifecycle().birthId()), 0),
                    Op.create(guard, bytes, immutableAcls, CreateMode.PERSISTENT)), deadline);
            observed = reads.get(guard, deadline);
        }
        requireFound(observed);
        if (observed.stat().getEphemeralOwner() != 0 || observed.stat().getVersion() != 0
                || !Arrays.equals(bytes, observed.bytes())) {
            throw new IllegalArgumentException("Invalid immutable recovery lifecycle birth guard");
        }
        return List.of(Op.check(path(ledger), current.rootDataVersion()), Op.check(guard, 0),
                Op.check(marker(headPath(ledger), current.lifecycle().birthId()), 0),
                Op.check(headPath(ledger), current.lifecycleDataVersion()));
    }

    private static RecoveryResult recoveryFailure(ProfileLedgerMetadataStore.Outcome outcome) {
        return new RecoveryResult(outcome, null, null);
    }

    /** Repair-only view. CLOSED stays CLOSED and its original close reference remains required. */
    RecoveryResult readRepairable(ProfileLedgerMetadataStore metadata, long ledger) {
        var current = metadata.readMembership(ledger);
        if (current.status() != ReadStatus.FOUND) {
            return recoveryFailure(current.status() == ReadStatus.UNAVAILABLE
                    ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                    : ProfileLedgerMetadataStore.Outcome.QUARANTINED);
        }
        if (current.snapshot().metadata().getValue().getState() != LedgerMetadata.State.CLOSED) {
            return readRecovery(metadata, ledger);
        }
        var opened = readClosedForOpen(metadata, ledger);
        if (opened.snapshot() == null || !opened.snapshot().closed().metadata().metadata().getValue()
                .getCustomMetadata().containsKey(ProfileRecoveryBoundary.CLOSE_KEY)) {
            return recoveryFailure(opened.status() == ReadOnlyStatus.UNAVAILABLE
                    ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                    : opened.status() == ReadOnlyStatus.QUARANTINED ? ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        : ProfileLedgerMetadataStore.Outcome.CONFLICT);
        }
        return new RecoveryResult(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, opened.snapshot().context(),
                opened.snapshot().closed().metadata());
    }

    public record RepairSnapshot(ProfileRepairIntent intent, UUID birth, long storeVersion) { }
    public record RepairResult(CreateStatus status, RepairSnapshot snapshot) {
        public RepairResult {
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (snapshot != null)) {
                throw new IllegalArgumentException("Only proved repair persistence carries a snapshot");
            }
        }
    }

    private String repairPath(long ledger, int slot) {
        if (slot < 0 || slot >= ProfileRepairIntent.MAX_INTENTS) {
            throw new IllegalArgumentException("Invalid repair slot");
        }
        return path(ledger) + "/repair-" + slot;
    }

    record SelfRepairSelection(CreateStatus status, RecoveryResult recovery, RepairSnapshot prepared,
            byte[] closeOperationId, RepairCommitResult committed) {
        SelfRepairSelection(CreateStatus status, RecoveryResult recovery, RepairSnapshot prepared, byte[] closeId) {
            this(status, recovery, prepared, closeId, null);
        }
    }

    /** Find the durable target operation first; creating a new SELF intent never replaces unresolved history. */
    SelfRepairSelection prepareLastSelfRepair(ProfileLedgerMetadataStore metadata, ProfileRecoveryContext context,
            int member) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRecovery(metadata, context.ledger().ledgerId());
            if (current.context() == null || !context.equals(current.context())) {
                return new SelfRepairSelection(current.outcome()
                        == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.CONFLICT, null, null, null);
            }
            var value = current.metadata().metadata().getValue();
            var selected = selectedBoundary(context, value, deadline);
            var ensemble = value.getAllEnsembles().lastEntry().getValue();
            long first = value.getAllEnsembles().lastKey();
            if (selected == null || member < 0 || member >= ensemble.size()
                    || selected.boundary().prefixEntryId() < first) {
                return new SelfRepairSelection(CreateStatus.CONFLICT, null, null, null);
            }
            String name = ensemble.get(member).toString();
            RepairSnapshot published = null;
            byte[] publishedClose = null;
            long publishedGeneration = 0;
            for (var reference : ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY))) {
                var repair = readRepair(value.getLedgerId(), reference.slot(), deadline);
                if (selfTarget(repair, context, first, member, name, selected.boundary().prefixEntryId())) {
                    var node = reads.get(repairPath(value.getLedgerId(), reference.slot()) + "/committed", deadline);
                    if (!repair.birth().equals(reference.intentBirth())
                            || repair.storeVersion() != reference.intentStoreVersion()
                            || !Arrays.equals(reference.digest(), ProfileRepairIntent.digest(node.bytes()))) {
                        return new SelfRepairSelection(CreateStatus.QUARANTINED, null, null, null);
                    }
                    var commit = repairCommit(node);
                    published = repair;
                    publishedClose = commit.closeOperationId();
                    publishedGeneration = commit.closeGeneration();
                }
            }
            if (published != null) {
                return new SelfRepairSelection(CreateStatus.ALREADY_APPLIED, current, published, publishedClose,
                        new RepairCommitResult(CreateStatus.ALREADY_APPLIED, current.metadata(),
                                published.intent().closeScope(publishedClose, publishedGeneration)));
            }
            int free = -1;
            for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                var repair = readRepair(value.getLedgerId(), slot, deadline);
                if (repair == null) {
                    if (free < 0) {
                        free = slot;
                    }
                } else if (selfTarget(repair, context, first, member, name, selected.boundary().prefixEntryId())) {
                    return new SelfRepairSelection(CreateStatus.ALREADY_APPLIED, current, repair,
                            selfOperation("close", repair.intent().encode()));
                }
            }
            if (free < 0) {
                return new SelfRepairSelection(CreateStatus.DURABILITY_UNKNOWN, null, null, null);
            }
            var installed = recoveryCurrentTargets(context, deadline).stream()
                    .filter(receipt -> receipt.scope().targetBookie().equals(name)).findFirst().orElse(null);
            if (installed == null) {
                return new SelfRepairSelection(CreateStatus.CONFLICT, null, null, null);
            }
            var old = installed.scope();
            byte[] binding = java.nio.ByteBuffer.allocate(ProfileRecoveryContext.ENCODED_BYTES + 32 + 4 + 16)
                    .put(context.encode()).put(selected.reference().digest()).putInt(member)
                    .put(old.targetStorageIncarnation()).array();
            var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, old.ledgerId(), old.ledgerInstanceId(),
                    old.descriptorIdentity(), old.targetBookie(),
                    old.targetStorageIncarnation(), "recovery-self", context.controlGeneration(),
                    selfOperation("install", binding), old.publicSemanticPayloadIdentity(), null);
            var desired = new ProfileRepairIntent(context, free, selfOperation("intent", binding), first, member,
                    repairEnsembleDigest(value, first), install, selfOperation("grant", binding),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(first,
                            selected.boundary().prefixEntryId()));
            var result = prepareRepair(metadata, desired);
            return new SelfRepairSelection(result.status(), result.snapshot() == null ? null : current,
                    result.snapshot(), result.snapshot() == null ? null : selfOperation("close", desired.encode()));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new SelfRepairSelection(CreateStatus.DURABILITY_UNKNOWN, null, null, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new SelfRepairSelection(CreateStatus.DURABILITY_UNKNOWN, null, null, null);
        } catch (IllegalArgumentException invalid) {
            return new SelfRepairSelection(CreateStatus.QUARANTINED, null, null, null);
        }
    }

    record WorkerRepairPlan(CreateStatus status, RecoveryResult recovery, RepairSnapshot prepared,
            byte[] closeId, boolean published) { }

    /** Reuse this worker's retained DATA or EMPTY operation before allocating another fixed intent slot. */
    WorkerRepairPlan resumeWorkerRepair(ProfileLedgerMetadataStore metadata, ProfileRecoveryContext context,
            java.util.Set<Integer> resolved, java.util.Set<String> suppliedTargets) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, context.ledger().ledgerId());
            if (current.context() == null || !context.equals(current.context())) {
                return new WorkerRepairPlan(CreateStatus.CONFLICT, null, null, null, false);
            }
            var value = current.metadata().metadata().getValue();
            var head = ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY));
            for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                var repair = readRepair(value.getLedgerId(), slot, deadline);
                if (repair == null || resolved.contains(slot) || !repair.intent().recovery().equals(context)
                        || !repair.intent().install().authorityDomain().equals("worker-repair")) {
                    continue;
                }
                var committed = reads.get(repairPath(value.getLedgerId(), slot) + "/committed", deadline);
                if (committed.code() == Code.NONODE.intValue()) {
                    return new WorkerRepairPlan(CreateStatus.ALREADY_APPLIED, current, repair,
                            repair.intent().empty() ? repair.intent().terminalOperationId()
                                : selfOperation("worker-close", repair.intent().encode()), false);
                }
                var commit = repairCommit(committed);
                var ensemble = value.getAllEnsembles().get(repair.intent().fragmentStart());
                if (ensemble == null || !ensemble.get(repair.intent().replacedMemberSlot()).toString()
                        .equals(repair.intent().install().targetBookie())) {
                    continue; // Obsolete targets remain in history for cleanup, never counted as current replicas.
                }
                if (head.stream().noneMatch(ref -> ref.slot() == repair.intent().slot()
                        && ref.intentBirth().equals(repair.birth()) && ref.intentStoreVersion() == repair.storeVersion()
                        && Arrays.equals(ref.digest(), ProfileRepairIntent.digest(committed.bytes())))) {
                    return new WorkerRepairPlan(CreateStatus.QUARANTINED, null, null, null, false);
                }
                if (!suppliedTargets.contains(repair.intent().install().targetBookie())) {
                    continue; // A later loss of an already published target is a new replica repair, not intent reset.
                }
                return new WorkerRepairPlan(CreateStatus.ALREADY_APPLIED, current, repair,
                        commit.closeOperationId(), true);
            }
            return new WorkerRepairPlan(CreateStatus.ALREADY_APPLIED, current, null, null, false);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new WorkerRepairPlan(CreateStatus.DURABILITY_UNKNOWN, null, null, null, false);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new WorkerRepairPlan(CreateStatus.DURABILITY_UNKNOWN, null, null, null, false);
        } catch (IllegalArgumentException invalid) {
            return new WorkerRepairPlan(CreateStatus.QUARANTINED, null, null, null, false);
        }
    }

    WorkerRepairPlan prepareWorkerRepair(ProfileLedgerMetadataStore metadata, ProfileRecoveryContext context,
            long entry, int member, String target, byte[] incarnation) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, context.ledger().ledgerId());
            if (current.context() == null || !context.equals(current.context())) {
                return new WorkerRepairPlan(CreateStatus.CONFLICT, null, null, null, false);
            }
            var value = current.metadata().metadata().getValue();
            var selected = selectedBoundary(context, value, deadline);
            var fragment = value.getAllEnsembles().floorEntry(entry);
            if (selected == null || fragment == null || member < 0 || member >= value.getEnsembleSize()
                    || value.getAllEnsembles().values().stream().anyMatch(ensemble ->
                        ensemble.stream().anyMatch(bookie -> bookie.toString().equals(target)))) {
                return new WorkerRepairPlan(CreateStatus.CONFLICT, null, null, null, false);
            }
            Long next = value.getAllEnsembles().higherKey(fragment.getKey());
            long last = next == null ? selected.boundary().prefixEntryId() : next - 1;
            boolean empty = next == null && last == fragment.getKey() - 1;
            if (last < fragment.getKey() && !empty) {
                return new WorkerRepairPlan(CreateStatus.UNAVAILABLE, current, null, null, false);
            }
            int free = -1;
            for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                if (readRepair(value.getLedgerId(), slot, deadline) == null) {
                    free = slot;
                    break;
                }
            }
            if (free < 0) {
                return new WorkerRepairPlan(CreateStatus.UNAVAILABLE, current, null, null, false);
            }
            var root = read(value.getLedgerId(), deadline);
            if (root.status() != ReadStatus.FOUND || !context.matchesRoot(root.snapshot())) {
                return new WorkerRepairPlan(CreateStatus.CONFLICT, null, null, null, false);
            }
            byte[] name = target.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] binding = java.nio.ByteBuffer.allocate(ProfileRecoveryContext.ENCODED_BYTES + 32 + 8 + 4 + 8
                    + name.length + 16).put(context.encode()).put(ProfileRecoveryContext.metadataDigest(value))
                    .putLong(fragment.getKey()).putInt(member).putLong(last).put(name)
                    .put(ProfileLifecycleHead.fixed(incarnation, 16, true)).array();
            var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, value.getLedgerId(), context.ledger().ledgerInstanceId(),
                    root.snapshot().reservation().installs().get(0).descriptorIdentity(), target, incarnation,
                    "worker-repair", context.controlGeneration(), selfOperation("worker-install", binding),
                    ProfileRepairIntent.digest(binding), null);
            var intent = new ProfileRepairIntent(context, free, selfOperation("worker-intent", binding),
                    fragment.getKey(), member, repairEnsembleDigest(value, fragment.getKey()), install,
                    empty ? new byte[16] : selfOperation("worker-grant", binding),
                    empty ? null : new org.apache.bookkeeper.common.profile.control.ProfileExactRange(
                            fragment.getKey(), last),
                    empty ? selfOperation("worker-empty-terminal", binding) : null);
            var prepared = prepareRepair(metadata, intent);
            return new WorkerRepairPlan(prepared.status(), current, prepared.snapshot(),
                    prepared.snapshot() == null ? null : intent.empty() ? intent.terminalOperationId()
                            : selfOperation("worker-close", intent.encode()), false);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new WorkerRepairPlan(CreateStatus.DURABILITY_UNKNOWN, null, null, null, false);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new WorkerRepairPlan(CreateStatus.DURABILITY_UNKNOWN, null, null, null, false);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new WorkerRepairPlan(CreateStatus.QUARANTINED, null, null, null, false);
        }
    }

    private static boolean selfTarget(RepairSnapshot repair, ProfileRecoveryContext context, long first,
            int member, String bookie, long prefix) {
        return repair != null && repair.intent().recovery().equals(context)
                && !repair.intent().empty()
                && repair.intent().fragmentStart() == first && repair.intent().replacedMemberSlot() == member
                && repair.intent().install().targetBookie().equals(bookie)
                && repair.intent().range().firstEntryId() == first && repair.intent().range().lastEntryId() == prefix;
    }

    private static byte[] selfOperation(String kind, byte[] binding) {
        byte[] domain = ("UW-SELF-" + kind + "-V1").getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return Arrays.copyOf(ProfileRepairIntent.digest(java.nio.ByteBuffer.allocate(domain.length + binding.length)
                .put(domain).put(binding).array()), 16);
    }

    public RepairResult prepareRepair(ProfileLedgerMetadataStore metadata, ProfileRepairIntent desired) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, desired.recovery().ledger().ledgerId());
            if (current.context() == null || !desired.recovery().equals(current.context())) {
                return new RepairResult(CreateStatus.CONFLICT, null);
            }
            if (!repairMatchesMetadata(desired, current.metadata().metadata().getValue())) {
                return new RepairResult(CreateStatus.CONFLICT, null);
            }
            var previous = readRepair(desired.recovery().ledger().ledgerId(), desired.slot(), deadline);
            if (previous != null) {
                return new RepairResult(previous.intent().equals(desired)
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        previous.intent().equals(desired) ? previous : null);
            }
            var lifecycle = read(desired.recovery().ledger().ledgerId(), deadline);
            if (lifecycle.status() != ReadStatus.FOUND || !desired.recovery().matchesRoot(lifecycle.snapshot())) {
                return new RepairResult(CreateStatus.CONFLICT, null);
            }
            UUID birth = UUID.randomUUID();
            String target = repairPath(desired.recovery().ledger().ledgerId(), desired.slot());
            byte[] bytes = java.nio.ByteBuffer.allocate(16 + desired.encode().length)
                    .putLong(birth.getMostSignificantBits()).putLong(birth.getLeastSignificantBits())
                    .put(desired.encode()).array();
            var checks = new java.util.ArrayList<>(recoveryRootChecks(lifecycle.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(current.metadata()));
            checks.add(Op.create(target, bytes, pageAcls, CreateMode.PERSISTENT));
            checks.add(Op.create(marker(target, birth), new byte[0], immutableAcls, CreateMode.PERSISTENT));
            int code = multi(checks, deadline);
            var actual = readRepair(desired.recovery().ledger().ledgerId(), desired.slot(), deadline);
            boolean equal = actual != null && actual.intent().equals(desired);
            return new RepairResult(equal ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, equal ? actual : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RepairResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RepairResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new RepairResult(CreateStatus.QUARANTINED, null);
        }
    }

    /** Receipt is a cold caller input;
    the Bookie still independently requires its actual installed route. */
    public RepairResult admitRepair(ProfileLedgerMetadataStore metadata, RepairSnapshot expected,
            ProfileInstallReceipt installed) {
        requireRecoveryClient(metadata);
        var intent = expected.intent();
        if (!installed.scope().equals(intent.install())) {
            throw new IllegalArgumentException("Repair admission requires the matching target install receipt");
        }
        long ledger = intent.recovery().ledger().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, ledger);
            var root = read(ledger, deadline);
            var actual = readRepair(ledger, intent.slot(), deadline);
            if (current.context() == null || !intent.recovery().equals(current.context())
                    || root.status() != ReadStatus.FOUND || !intent.recovery().matchesRoot(root.snapshot())
                    || actual == null || !actual.equals(expected)
                    || !repairMatchesMetadata(intent, current.metadata().metadata().getValue())) {
                return new RepairResult(CreateStatus.CONFLICT, null);
            }
            String target = repairPath(ledger, intent.slot());
            byte[] admission = repairAdmissionBytes(intent, installed);
            var before = reads.get(target + "/admitted", deadline);
            if (before.code() == Code.OK.intValue()) {
                validateRepairAdmission(actual, before, deadline);
                return new RepairResult(admissionMatches(before, admission)
                        ? CreateStatus.ALREADY_APPLIED : CreateStatus.QUARANTINED,
                        admissionMatches(before, admission) ? actual : null);
            }
            if (before.code() != Code.NONODE.intValue()) {
                throw new IllegalStateException("Repair admission unavailable");
            }
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(current.metadata()));
            checks.add(Op.check(marker(target, actual.birth()), 0));
            checks.add(Op.check(target, 0));
            checks.add(Op.create(target + "/admitted", admission, immutableAcls, CreateMode.PERSISTENT));
            checks.add(Op.create(repairAdmissionGuardPath(actual), repairAdmissionGuardBytes(actual),
                    immutableAcls, CreateMode.PERSISTENT));
            int code = multi(checks, deadline);
            var after = reads.get(target + "/admitted", deadline);
            var resolved = readRepair(ledger, intent.slot(), deadline);
            boolean equal = after.code() == Code.OK.intValue() && actual.equals(resolved)
                    && admissionMatches(after, admission);
            if (equal) {
                validateRepairAdmission(actual, after, deadline);
            }
            return new RepairResult(equal ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, equal ? actual : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RepairResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RepairResult(CreateStatus.DURABILITY_UNKNOWN, null);
        } catch (IllegalArgumentException invalid) {
            return new RepairResult(CreateStatus.QUARANTINED, null);
        }
    }

    boolean repairAdmitted(RepairSnapshot expected) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var actual = readRepair(expected.intent().recovery().ledger().ledgerId(),
                    expected.intent().slot(), deadline);
            if (actual == null || !actual.equals(expected)) {
                throw new IllegalArgumentException("Repair admission belongs to another intent birth");
            }
            var node = reads.get(repairPath(expected.intent().recovery().ledger().ledgerId(), expected.intent().slot())
                    + "/admitted", deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return false;
            }
            requireFound(node);
            validateRepairAdmission(actual, node, deadline);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Repair admission read interrupted", interrupted);
        } catch (TimeoutException unavailable) {
            throw new IllegalStateException("Repair admission unavailable", unavailable);
        }
    }

    public record RepairCommitResult(CreateStatus status,
            ProfileLedgerMetadataStore.MembershipSnapshot metadata, ProfileControlScope closeScope,
            org.apache.bookkeeper.common.profile.control.ProfileControlRequest.TerminalBinding terminal) {
        public RepairCommitResult(CreateStatus status, ProfileLedgerMetadataStore.MembershipSnapshot metadata,
                ProfileControlScope closeScope) {
            this(status, metadata, closeScope, null);
        }
    }

    /** Cold adoption of an already published result; no copied-at claim, recopy or metadata write is made. */
    public RepairCommitResult resolveRepairClose(ProfileLedgerMetadataStore metadata, RepairSnapshot expected,
            byte[] closeOperationId) {
        requireRecoveryClient(metadata);
        closeOperationId = ProfileLifecycleHead.fixed(closeOperationId, 16, true);
        var intent = expected.intent();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, intent.recovery().ledger().ledgerId());
            if (current.context() == null || !intent.recovery().equals(current.context())) {
                return new RepairCommitResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        ? CreateStatus.QUARANTINED : current.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                            ? CreateStatus.CONFLICT : CreateStatus.DURABILITY_UNKNOWN, null, null);
            }
            var actual = readRepair(intent.recovery().ledger().ledgerId(), intent.slot(), deadline);
            var node = reads.get(repairPath(intent.recovery().ledger().ledgerId(), intent.slot()) + "/committed",
                    deadline);
            var commit = repairCommit(node);
            var value = current.metadata().metadata().getValue();
            if (actual == null || !actual.equals(expected)
                    || !Arrays.equals(commit.closeOperationId(), closeOperationId)
                    || !value.getAllEnsembles().get(intent.fragmentStart()).get(intent.replacedMemberSlot())
                        .toString().equals(intent.install().targetBookie())
                    || ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY)).stream()
                        .noneMatch(ref -> ref.slot() == intent.slot() && ref.intentBirth().equals(expected.birth())
                            && ref.intentStoreVersion() == expected.storeVersion()
                            && Arrays.equals(ref.digest(), ProfileRepairIntent.digest(node.bytes())))) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            var selected = selectedBoundary(intent.recovery(), value, deadline);
            validateEmptyBoundary(actual, commit, selected, deadline);
            var close = repairCloseTarget(intent, closeOperationId, commit.closeGeneration(), selected);
            return new RepairCommitResult(CreateStatus.ALREADY_APPLIED, current.metadata(),
                    close.scope(), close.terminal());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RepairCommitResult(CreateStatus.DURABILITY_UNKNOWN, null, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RepairCommitResult(CreateStatus.DURABILITY_UNKNOWN, null, null);
        } catch (IllegalArgumentException invalid) {
            return new RepairCommitResult(CreateStatus.QUARANTINED, null, null);
        }
    }

    /** Cold publication primitive after the caller completed the full bounded historical-fragment copy.
     * This is not a copy producer, required-frontier proof or strong loss-budget reset.
     * Reuse the same intent, copied-at metadata snapshot and close ID after UNKNOWN. */
    public RepairCommitResult commitRepair(ProfileLedgerMetadataStore metadata, RepairSnapshot expected,
            ProfileLedgerMetadataStore.MembershipSnapshot copiedAt, byte[] closeOperationId) {
        if (expected.intent().empty()) {
            throw new IllegalArgumentException("EMPTY membership cannot use a caller DATA copied-at claim");
        }
        return commitRepairOnce(metadata, expected, copiedAt, closeOperationId, null);
    }

    RepairCommitResult commitEmptyRepair(ProfileLedgerMetadataStore metadata,
            ProfileRecoveryCoordinator.EmptyRepairProof proof) {
        if (!proof.repair().intent().empty()) {
            throw new IllegalArgumentException("EMPTY publication requires its actual boundary proof");
        }
        return commitRepairOnce(metadata, proof.repair(), proof.before(), proof.boundary().operationId(),
                proof.boundary());
    }

    private RepairCommitResult commitRepairOnce(ProfileLedgerMetadataStore metadata, RepairSnapshot expected,
            ProfileLedgerMetadataStore.MembershipSnapshot copiedAt, byte[] closeOperationId,
            ProfileRepairCommit.EmptyBoundary emptyProof) {
        requireRecoveryClient(metadata);
        var intent = expected.intent();
        closeOperationId = ProfileLifecycleHead.fixed(closeOperationId, 16, true);
        long ledger = intent.recovery().ledger().ledgerId();
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var current = readRepairable(metadata, ledger);
            var root = read(ledger, deadline);
            var actual = readRepair(ledger, intent.slot(), deadline);
            if (current.context() == null) {
                return new RepairCommitResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                        ? CreateStatus.CONFLICT : current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                            ? CreateStatus.QUARANTINED : CreateStatus.DURABILITY_UNKNOWN, null, null);
            }
            if (root.status() == ReadStatus.UNAVAILABLE) {
                return new RepairCommitResult(CreateStatus.DURABILITY_UNKNOWN, null, null);
            }
            if (!current.context().equals(intent.recovery())
                    || root.status() != ReadStatus.FOUND || !intent.recovery().matchesRoot(root.snapshot())
                    || actual == null || !actual.equals(expected)) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            String path = repairPath(ledger, intent.slot());
            if (!(copiedAt.metadata().getVersion() instanceof org.apache.bookkeeper.versioning.LongVersion)
                    || copiedAt.metadata().getValue().getState() != current.metadata().metadata().getValue().getState()
                    || !Arrays.equals(copiedAt.metadata().getValue().getCustomMetadata()
                            .get(ProfileRecoveryContext.KEY), intent.recovery().encode())) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            var prior = reads.get(path + "/committed", deadline);
            var before = current.metadata();
            var head = ProfileRepairCommit.head(before.metadata().getValue().getCustomMetadata()
                    .get(ProfileRepairCommit.KEY));
            var selection = selectedBoundary(current.context(), before.metadata().getValue(), deadline);
            if (prior.code() == Code.OK.intValue()) {
                var commit = repairCommit(prior);
                validateEmptyBoundary(actual, commit, selection, deadline);
                int index = -1;
                for (int i = 0; i < head.size(); i++) {
                    var ref = head.get(i);
                    if (ref.slot() == intent.slot() && ref.intentBirth().equals(expected.birth())
                            && ref.intentStoreVersion() == expected.storeVersion()
                            && Arrays.equals(ref.digest(), ProfileRepairIntent.digest(prior.bytes()))) {
                        index = i;
                    }
                }
                boolean matching = Arrays.equals(commit.closeOperationId(), closeOperationId)
                        && commit.beforeVersion() == ((org.apache.bookkeeper.versioning.LongVersion)
                            copiedAt.metadata().getVersion()).getLongVersion()
                        && Arrays.equals(commit.beforeDigest(), ProfileRecoveryContext.metadataDigest(
                            copiedAt.metadata().getValue()))
                        && copiedAt.birthZxid() == current.context().metadataBirth()
                        && index >= 0 && Arrays.equals(copiedAt.metadata().getValue().getCustomMetadata()
                            .get(ProfileRepairCommit.KEY),
                            index == 0 ? null : ProfileRepairCommit.head(head.subList(0, index)))
                        && head.stream().anyMatch(ref -> ref.slot() == intent.slot()
                            && ref.intentBirth().equals(expected.birth())
                            && ref.intentStoreVersion() == expected.storeVersion()
                            && Arrays.equals(ref.digest(), ProfileRepairIntent.digest(prior.bytes())));
                var close = matching ? repairCloseTarget(intent, closeOperationId, commit.closeGeneration(), selection)
                        : null;
                return new RepairCommitResult(matching ? CreateStatus.ALREADY_APPLIED : CreateStatus.CONFLICT,
                        matching ? before : null, close == null ? null : close.scope(),
                        close == null ? null : close.terminal());
            }
            if (prior.code() != Code.NONODE.intValue()) {
                throw new IllegalStateException("Repair membership result unavailable");
            }
            var value = before.metadata().getValue();
            if (before.birthZxid() != copiedAt.birthZxid()
                    || !before.metadata().getVersion().equals(copiedAt.metadata().getVersion())
                    || !Arrays.equals(value.getCustomMetadata().get(ProfileRepairCommit.KEY),
                            copiedAt.metadata().getValue().getCustomMetadata().get(ProfileRepairCommit.KEY))
                    || !Arrays.equals(value.getCustomMetadata().get(ProfileRecoveryBoundary.KEY),
                            copiedAt.metadata().getValue().getCustomMetadata().get(ProfileRecoveryBoundary.KEY))
                    || !Arrays.equals(value.getCustomMetadata().get(ProfileRecoveryBoundary.CLOSE_KEY),
                            copiedAt.metadata().getValue().getCustomMetadata().get(ProfileRecoveryBoundary.CLOSE_KEY))
                    || !Arrays.equals(ProfileRecoveryContext.metadataDigest(value),
                        ProfileRecoveryContext.metadataDigest(copiedAt.metadata().getValue()))
                    || !boundedRepairFragment(intent, value, selection == null ? null : selection.boundary())
                    || !repairMatchesMetadata(intent, value)
                    || value.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY)
                    || value.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.PENDING_PUBLICATION_KEY)
                    || head.size() == ProfileRepairIntent.MAX_INTENTS) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            var admitted = reads.get(path + "/admitted", deadline);
            if (admitted.code() != Code.OK.intValue()) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            validateRepairAdmission(actual, admitted, deadline);
            String old = value.getAllEnsembles().get(intent.fragmentStart())
                    .get(intent.replacedMemberSlot()).toString();
            if (old.equals(intent.install().targetBookie()) && (selection == null
                    || value.getAllEnsembles().higherKey(intent.fragmentStart()) != null)) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            var desired = ProfileRepairCommit.replace(value, intent, intent.install().targetBookie());
            long generation = intent.empty() ? intent.grantGeneration()
                    : Math.addExact(intent.grantGeneration(), head.size() + 1L);
            var close = repairCloseTarget(intent, closeOperationId, generation, selection);
            if (intent.empty() && (emptyProof == null || !emptyBoundaryMatches(emptyProof, intent, close))) {
                return new RepairCommitResult(CreateStatus.CONFLICT, null, null);
            }
            var commit = new ProfileRepairCommit(Math.toIntExact(((org.apache.bookkeeper.versioning.LongVersion)
                    before.metadata().getVersion()).getLongVersion()), ProfileRecoveryContext.metadataDigest(value),
                    ProfileRecoveryContext.metadataDigest(desired), old, closeOperationId, generation,
                    emptyProof == null ? null : ProfileRepairIntent.digest(emptyProof.encode()));
            var next = new java.util.ArrayList<>(head);
            next.add(new ProfileRepairCommit.Reference(intent.slot(), expected.birth(), expected.storeVersion(),
                    ProfileRepairIntent.digest(commit.encode())));
            var custom = new java.util.TreeMap<>(desired.getCustomMetadata());
            custom.put(ProfileRepairCommit.KEY, ProfileRepairCommit.head(next));
            desired = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(desired).withCToken(desired.getCToken())
                    .withCustomMetadata(custom).build();
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root.snapshot(), deadline));
            checks.addAll(metadata.checksForMembership(before));
            checks.add(Op.check(marker(path, expected.birth()), 0));
            checks.add(Op.check(path, 0));
            checks.add(Op.check(path + "/admitted", 0));
            checks.add(Op.check(repairAdmissionGuardPath(actual), 0));
            checks.add(metadata.repairMetadataWrite(before, desired));
            if (emptyProof != null) {
                checks.add(Op.create(path + "/empty-boundary", emptyProof.encode(), immutableAcls,
                        CreateMode.PERSISTENT));
            }
            checks.add(Op.create(path + "/committed", commit.encode(), immutableAcls, CreateMode.PERSISTENT));
            int code = multi(checks, deadline);
            var after = readRepairable(metadata, ledger);
            var result = reads.get(path + "/committed", deadline);
            boolean matching = after.context() != null && after.context().equals(intent.recovery())
                    && result.code() == Code.OK.intValue() && Arrays.equals(result.bytes(), commit.encode());
            return new RepairCommitResult(matching ? code == Code.OK.intValue() ? CreateStatus.APPLIED
                    : CreateStatus.ALREADY_APPLIED : conditionalFailure(code) ? CreateStatus.CONFLICT
                    : CreateStatus.DURABILITY_UNKNOWN, matching ? after.metadata() : null,
                    matching ? close.scope() : null, matching ? close.terminal() : null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new RepairCommitResult(CreateStatus.DURABILITY_UNKNOWN, null, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new RepairCommitResult(CreateStatus.DURABILITY_UNKNOWN, null, null);
        } catch (java.io.IOException | IllegalArgumentException | ArithmeticException invalid) {
            return new RepairCommitResult(CreateStatus.QUARANTINED, null, null);
        }
    }

    public ProfileControlStore repairAuthorityReader(String localBookie, ProfileLedgerMetadataStore metadata) {
        ProfileAuthorityCodec.text(localBookie, ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        requireRecoveryClient(metadata);
        return key -> {
            long deadline = System.nanoTime() + timeoutNanos;
            try {
                var current = readRepairable(metadata, key.ledgerId());
                if (current.context() == null) {
                    return new ProfileControlStore.ReadResult(current.outcome()
                            == ProfileLedgerMetadataStore.Outcome.CONFLICT
                            || current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                ? ReadStatus.QUARANTINED : ReadStatus.UNAVAILABLE, null);
                }
                if (key.purpose() == ProfileAuthorityPurpose.RECOVERY_TERMINAL_CLOSE) {
                    var value = current.metadata().metadata().getValue();
                    var selected = selectedBoundary(current.context(), value, deadline);
                    for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                        var repair = readRepair(key.ledgerId(), slot, deadline);
                        if (repair == null || !repair.intent().empty()
                                || !repair.intent().recovery().equals(current.context()) || selected == null
                                || !boundedRepairFragment(repair.intent(), value, selected.boundary())) {
                            continue;
                        }
                        var intent = repair.intent();
                        var scope = repairCloseTarget(intent, intent.terminalOperationId(), intent.grantGeneration(),
                                selected).scope();
                        if (!scope.targetBookie().equals(localBookie)
                                || !ProfileAuthorityKey.fromScope(scope).equals(key)) {
                            continue;
                        }
                        var admitted = reads.get(repairPath(key.ledgerId(), slot) + "/admitted", deadline);
                        if (admitted.code() != Code.OK.intValue()) {
                            return new ProfileControlStore.ReadResult(admitted.code() == Code.NONODE.intValue()
                                    ? ReadStatus.NOT_FOUND : ReadStatus.UNAVAILABLE, null);
                        }
                        validateRepairAdmission(repair, admitted, deadline);
                        var committed = reads.get(repairPath(key.ledgerId(), slot) + "/committed", deadline);
                        if (committed.code() == Code.OK.intValue()) {
                            var commit = repairCommit(committed);
                            validateEmptyBoundary(repair, commit, selected, deadline);
                            if (!value.getAllEnsembles().get(intent.fragmentStart()).get(intent.replacedMemberSlot())
                                    .toString().equals(intent.install().targetBookie())) {
                                continue;
                            }
                        } else if (committed.code() != Code.NONODE.intValue()) {
                            throw new IllegalStateException("EMPTY repair result unavailable");
                        } else if (!repairMatchesMetadata(intent, value)) {
                            continue;
                        }
                        var after = readRepairable(metadata, key.ledgerId());
                        if (after.context() == null || !after.context().equals(current.context())) {
                            return new ProfileControlStore.ReadResult(ReadStatus.QUARANTINED, null);
                        }
                        return ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(scope,
                                CommittedProfileAuthority.State.RECOVERY_TERMINAL_CLOSE_AUTHORIZED,
                                EngineProfile.SEGMENT_WAL_ENGINE, admitted.stat().getMzxid(), true));
                    }
                    var prepared = closingPlan(metadata, current.context());
                    if (prepared.plan() == null) {
                        return new ProfileControlStore.ReadResult(prepared.status() == CreateStatus.DURABILITY_UNKNOWN
                                ? ReadStatus.UNAVAILABLE : ReadStatus.QUARANTINED, null);
                    }
                    return prepared.plan().targets().stream().filter(target -> target.terminal() != null
                            && target.scope().targetBookie().equals(localBookie)
                            && ProfileAuthorityKey.fromScope(target.scope()).equals(key))
                            .findFirst().map(target -> ProfileControlStore.ReadResult.found(
                                new CommittedProfileAuthority(target.scope(),
                                        CommittedProfileAuthority.State.RECOVERY_TERMINAL_CLOSE_AUTHORIZED,
                                        EngineProfile.SEGMENT_WAL_ENGINE,
                                        prepared.plan().selection().birthZxid(), true)))
                            .orElseGet(() -> new ProfileControlStore.ReadResult(ReadStatus.NOT_FOUND, null));
                }
                for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                    var repair = readRepair(key.ledgerId(), slot, deadline);
                    if (repair == null || !repair.intent().recovery().equals(current.context())) {
                        continue;
                    }
                    var intent = repair.intent();
                    if (intent.empty() && key.purpose() != ProfileAuthorityPurpose.PREPARING_INSTALL) {
                        continue; // EMPTY never authorizes either a DATA grant or grant-close.
                    }
                    var committed = reads.get(repairPath(key.ledgerId(), slot) + "/committed", deadline);
                    ProfileControlScope scope;
                    if (key.purpose() == ProfileAuthorityPurpose.RECOVERY_GRANT_CLOSE) {
                        if (committed.code() == Code.NONODE.intValue()) {
                            continue;
                        }
                        var result = repairCommit(committed);
                        var head = ProfileRepairCommit.head(current.metadata().metadata().getValue()
                                .getCustomMetadata().get(ProfileRepairCommit.KEY));
                        if (head.stream().noneMatch(ref -> ref.slot() == intent.slot()
                                && ref.intentBirth().equals(repair.birth())
                                && ref.intentStoreVersion() == repair.storeVersion()
                                && Arrays.equals(ref.digest(), ProfileRepairIntent.digest(committed.bytes())))
                                || !current.metadata().metadata().getValue().getAllEnsembles()
                                    .get(intent.fragmentStart()).get(intent.replacedMemberSlot())
                                    .toString().equals(intent.install().targetBookie())) {
                            continue;
                        }
                        scope = intent.closeScope(result.closeOperationId(), result.closeGeneration());
                    } else {
                        if (key.purpose() == ProfileAuthorityPurpose.RECOVERY_GRANT
                                && committed.code() == Code.OK.intValue()) {
                            continue;
                        }
                        if (committed.code() != Code.OK.intValue() && committed.code() != Code.NONODE.intValue()) {
                            throw new IllegalStateException("Repair commit authority unavailable");
                        }
                        scope = key.purpose() == ProfileAuthorityPurpose.PREPARING_INSTALL
                                ? intent.install() : intent.grantScope();
                    }
                    if (!scope.targetBookie().equals(localBookie)
                            || !ProfileAuthorityKey.fromScope(scope).equals(key)) {
                        continue;
                    }
                    if (scope.operation() == ProfileControlOperation.INSTALL) {
                        return new ProfileControlStore.ReadResult(ReadStatus.FOUND,
                                new CommittedProfileAuthority(scope,
                                        CommittedProfileAuthority.State.PREPARING,
                                        EngineProfile.SEGMENT_WAL_ENGINE,
                                        repair.storeVersion(), true));
                    }
                    var admission = reads.get(repairPath(key.ledgerId(), slot) + "/admitted", deadline);
                    if (admission.code() != Code.OK.intValue()) {
                        return new ProfileControlStore.ReadResult(admission.code() == Code.NONODE.intValue()
                                ? ReadStatus.NOT_FOUND : ReadStatus.UNAVAILABLE, null);
                    }
                    validateRepairAdmission(repair, admission, deadline);
                    var after = readRepairable(metadata, key.ledgerId());
                    if (after.context() == null || !after.context().equals(current.context())) {
                        return new ProfileControlStore.ReadResult(ReadStatus.QUARANTINED, null);
                    }
                    return new ProfileControlStore.ReadResult(ReadStatus.FOUND,
                            new CommittedProfileAuthority(scope,
                                    scope.operation() == ProfileControlOperation.RECOVERY_GRANT_CLOSE
                                            ? current.metadata().metadata().getValue().getAllEnsembles()
                                                .higherKey(intent.fragmentStart()) == null
                                                ? CommittedProfileAuthority.State.RECOVERY_TERMINAL_CLOSE_AUTHORIZED
                                                : CommittedProfileAuthority.State.RECOVERY_CLOSE_AUTHORIZED
                                            : CommittedProfileAuthority.State.RECOVERY_OPEN,
                                    EngineProfile.SEGMENT_WAL_ENGINE,
                                    scope.operation() == ProfileControlOperation.RECOVERY_GRANT_CLOSE
                                            ? committed.stat().getMzxid() : admission.stat().getMzxid(), true));
                }
                return new ProfileControlStore.ReadResult(ReadStatus.NOT_FOUND, null);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return new ProfileControlStore.ReadResult(ReadStatus.UNAVAILABLE, null);
            } catch (TimeoutException | IllegalStateException unavailable) {
                return new ProfileControlStore.ReadResult(ReadStatus.UNAVAILABLE, null);
            } catch (IllegalArgumentException invalid) {
                return new ProfileControlStore.ReadResult(ReadStatus.QUARANTINED, null);
            }
        };
    }

    private RepairSnapshot readRepair(long ledger, int slot, long deadline)
            throws InterruptedException, TimeoutException {
        String target = repairPath(ledger, slot);
        var node = reads.get(target, deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        requirePersistent(node, true);
        if (node.bytes().length < 16 || node.bytes().length > 16 + ProfileRepairIntent.MAX_BYTES) {
            throw new IllegalArgumentException("Repair intent envelope exceeds its bound");
        }
        var in = java.nio.ByteBuffer.wrap(node.bytes());
        UUID birth = new UUID(in.getLong(), in.getLong());
        byte[] bytes = new byte[in.remaining()];
        in.get(bytes);
        var intent = ProfileRepairIntent.decode(bytes);
        if (intent.slot() != slot || intent.recovery().ledger().ledgerId() != ledger) {
            throw new IllegalArgumentException("Repair intent lookup scope mismatch");
        }
        requireMarker(marker(target, birth), node.stat().getCzxid(), deadline);
        return new RepairSnapshot(intent, birth, node.stat().getMzxid());
    }

    private static byte[] repairAdmissionBytes(ProfileRepairIntent intent, ProfileInstallReceipt receipt) {
        return java.nio.ByteBuffer.allocate(112).putInt(0x55575241).putInt(1)
                .putLong(receipt.localInstallGeneration()).put(receipt.capabilityDigest())
                .put(ProfileRepairIntent.digest(intent.encode()))
                .put(ProfileRepairIntent.digest(intent.recovery().encode()))
                .array();
    }

    private static boolean admissionMatches(ProfileZKReadClient.Node node, byte[] expected) {
        return node.stat().getVersion() == 0 && node.stat().getEphemeralOwner() == 0
                && Arrays.equals(node.bytes(), expected);
    }

    private String repairAdmissionGuardPath(RepairSnapshot repair) {
        return repairPath(repair.intent().recovery().ledger().ledgerId(), repair.intent().slot())
                + "/admitted-birth-" + Long.toHexString(repair.storeVersion());
    }

    private static byte[] repairAdmissionGuardBytes(RepairSnapshot repair) {
        return java.nio.ByteBuffer.allocate(24).putLong(repair.storeVersion())
                .putLong(repair.birth().getMostSignificantBits()).putLong(repair.birth().getLeastSignificantBits())
                .array();
    }

    private void validateRepairAdmission(RepairSnapshot repair, ProfileZKReadClient.Node node, long deadline)
            throws InterruptedException, TimeoutException {
        validateRepairAdmission(repair.intent(), node);
        var guard = reads.get(repairAdmissionGuardPath(repair), deadline);
        if (guard.code() != Code.OK.intValue() || guard.stat().getVersion() != 0
                || guard.stat().getEphemeralOwner() != 0
                || !Arrays.equals(guard.bytes(), repairAdmissionGuardBytes(repair))) {
            throw new IllegalArgumentException("Required physical repair admission guard is missing or changed");
        }
    }

    private static void validateRepairAdmission(ProfileRepairIntent intent, ProfileZKReadClient.Node node) {
        if (node.stat().getVersion() != 0 || node.stat().getEphemeralOwner() != 0
                || node.bytes() == null || node.bytes().length != 112) {
            throw new IllegalArgumentException("Invalid immutable repair admission");
        }
        var in = java.nio.ByteBuffer.wrap(node.bytes());
        if (in.getInt() != 0x55575241 || in.getInt() != 1 || in.getLong() <= 0) {
            throw new IllegalArgumentException("Invalid repair admission shape");
        }
        in.position(48);
        byte[] digest = new byte[32];
        in.get(digest);
        byte[] context = new byte[32];
        in.get(context);
        if (!Arrays.equals(digest, ProfileRepairIntent.digest(intent.encode()))
                || !Arrays.equals(context, ProfileRepairIntent.digest(intent.recovery().encode()))) {
            throw new IllegalArgumentException("Repair admission no longer binds its intent/context");
        }
    }

    private static boolean repairMatchesMetadata(ProfileRepairIntent intent,
            LedgerMetadata value) {
        long first = intent.empty() ? intent.fragmentStart() : intent.range().firstEntryId();
        var entry = value.getAllEnsembles().floorEntry(first);
        var next = value.getAllEnsembles().higherKey(first);
        return entry != null && entry.getKey() == intent.fragmentStart()
                && intent.replacedMemberSlot() < entry.getValue().size()
                && (intent.empty() ? next == null : next == null || intent.range().lastEntryId() < next)
                && Arrays.equals(intent.oldEnsembleDigest(), repairEnsembleDigest(value, intent.fragmentStart()));
    }

    public static byte[] repairEnsembleDigest(LedgerMetadata value, long start) {
        var ensemble = value.getAllEnsembles().get(start);
        if (ensemble == null || ensemble.size() > 32) {
            throw new IllegalArgumentException("Missing bounded repair fragment");
        }
        var bytes = new java.io.ByteArrayOutputStream();
        try (var out = new java.io.DataOutputStream(bytes)) {
            out.write("BookKeeper/ProfileRepairEnsemble/v1\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.writeLong(start);
            out.writeInt(value.getEnsembleSize());
            out.writeInt(value.getWriteQuorumSize());
            out.writeInt(value.getAckQuorumSize());
            for (var bookie : ensemble) {
                byte[] name = ProfileAuthorityCodec.text(bookie.toString(), 255);
                out.writeShort(name.length);
                out.write(name);
            }
        } catch (java.io.IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return ProfileRepairIntent.digest(bytes.toByteArray());
    }

    public enum DeleteStatus { LOGICALLY_DELETED, PRECONDITION_FAILED, CONFLICT, QUARANTINED, UNAVAILABLE, CANCELLED }

    public record DeleteResult(DeleteStatus status, ProfileDeleteRecord.Manifest manifest) {
        public DeleteResult {
            if ((status == DeleteStatus.LOGICALLY_DELETED) != (manifest != null)) {
                throw new IllegalArgumentException("Only complete cluster tombstone carries its target obligation");
            }
        }
    }

    private String deleteManifestPath(long ledger) {
        return path(ledger) + "/delete-targets";
    }

    private ProfileDeleteRecord.Manifest deleteManifest(long ledger, ProfileDeleteRecord.Reference reference,
            ProfileDeleteRecord.Seal seal, long deadline) throws InterruptedException, TimeoutException {
        String name = deleteManifestPath(ledger);
        var node = reads.get(name, deadline);
        requireFound(node);
        requirePersistent(node, true);
        var guard = reads.get(name + "/birth-" + Long.toHexString(reference.birth()), deadline);
        requireFound(guard);
        requirePersistent(guard, true);
        var manifest = ProfileDeleteRecord.Manifest.decode(node.bytes());
        byte[] expected = java.nio.ByteBuffer.allocate(40).putLong(reference.birth()).put(reference.digest()).array();
        if (node.stat().getCzxid() != reference.birth()
                || !Arrays.equals(ProfileRepairIntent.digest(node.bytes()), reference.digest())
                || !Arrays.equals(guard.bytes(), expected) || !manifest.seal().equals(seal)
                || !Arrays.equals(reference.sealDigest(), ProfileRepairIntent.digest(seal.encode()))
                || manifest.ledger().ledgerId() != ledger) {
            throw new IllegalArgumentException("Required delete target manifest is missing, recreated or corrupt");
        }
        return manifest;
    }

    private ProfileLedgerMetadataStore.MembershipSnapshot deleteProjection(Snapshot root,
            ProfileLedgerMetadataStore.MembershipSnapshot current, long deadline)
            throws InterruptedException, TimeoutException, java.io.IOException {
        var value = current.metadata().getValue();
        byte[] bytes = value.getCustomMetadata().get(ProfileDeleteRecord.SEAL_KEY);
        byte[] logical = value.getCustomMetadata().get(ProfileDeleteRecord.TOMBSTONE_KEY);
        if (bytes == null) {
            if (logical != null) {
                throw new IllegalArgumentException("Cluster tombstone has no membership seal");
            }
            return current;
        }
        var seal = ProfileDeleteRecord.Seal.decode(bytes);
        if (!seal.matches(root) || current.birthZxid() != seal.metadataBirth()
                || value.getState() != LedgerMetadata.State.CLOSED
                || ((org.apache.bookkeeper.versioning.LongVersion) current.metadata().getVersion()).getLongVersion()
                    != seal.beforeVersion() + 1L + (logical == null ? 0 : 1)
                || !Arrays.equals(ProfileDeleteRecord.closedDigest(value), seal.closedDigest())) {
            throw new IllegalArgumentException("Deletion seal does not preserve the exact CLOSED record");
        }
        if (logical != null) {
            var manifest = deleteManifest(value.getLedgerId(), ProfileDeleteRecord.Reference.decode(logical),
                    seal, deadline);
            var context = ProfileRecoveryContext.decode(value.getCustomMetadata().get(ProfileRecoveryContext.KEY));
            if (!manifest.ledger().equals(context.ledger())) {
                throw new IllegalArgumentException("Deletion manifest belongs to a different ledger instance");
            }
        }
        return new ProfileLedgerMetadataStore.MembershipSnapshot(current.birthZxid(),
                new org.apache.bookkeeper.versioning.Versioned<>(ProfileDeleteRecord.strip(value),
                    new org.apache.bookkeeper.versioning.LongVersion(seal.beforeVersion())));
    }

    private record PreparedDeliverySnapshot(ProfileDeleteRecord.PreparedDeliveries value,
            ProfileZKReadClient.Node node, String guard) { }

    private PreparedDeliverySnapshot preparedDeliveries(long ledger, ProfileDeleteRecord.Manifest manifest,
            ProfileDeleteRecord.Reference reference, long deadline) throws InterruptedException, TimeoutException {
        String name = deleteManifestPath(ledger) + "/delivery-prepared";
        var node = reads.get(name, deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        requirePersistent(node, false);
        var value = ProfileDeleteRecord.PreparedDeliveries.decode(node.bytes());
        String guard = name + "/birth-" + HexFormat.of().formatHex(value.birthId());
        var anchor = reads.get(guard, deadline);
        requireFound(anchor);
        requirePersistent(anchor, true);
        if (value.reference().birth() != reference.birth()
                || !Arrays.equals(value.reference().digest(), reference.digest())
                || !Arrays.equals(value.reference().sealDigest(), reference.sealDigest())
                || reference.publicationId() != null
                    && !Arrays.equals(value.reference().publicationId(), reference.publicationId())
                || value.reference().publicationId() != null
                    && !Arrays.equals(value.reference().publicationId(), value.birthId())
                || value.targetCount() != manifest.targets().size()
                || node.stat().getVersion() != value.preparedThrough()
                || anchor.stat().getCzxid() != node.stat().getCzxid()
                || !Arrays.equals(anchor.bytes(), value.birthId())) {
            throw new IllegalArgumentException("Delete publication prefix is missing, recreated or mismatched");
        }
        return new PreparedDeliverySnapshot(value, node, guard);
    }

    private PreparedDeliverySnapshot prepareDeleteDeliveries(ProfileLedgerMetadataStore metadata, Snapshot frozen,
            ProfileLedgerMetadataStore.MembershipSnapshot member, ProfileDeleteRecord.Manifest manifest,
            ProfileDeleteRecord.Reference reference, boolean versioned,
            java.util.function.BooleanSupplier live, long deadline)
            throws InterruptedException, TimeoutException {
        String name = deleteManifestPath(manifest.ledger().ledgerId()) + "/delivery-prepared";
        var current = preparedDeliveries(manifest.ledger().ledgerId(), manifest, reference, deadline);
        if (current == null) {
            if (!versioned && reference.publicationId() != null) {
                return null;
            }
            UUID id = UUID.randomUUID();
            byte[] birth = java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                    .putLong(id.getLeastSignificantBits()).array();
            var bound = versioned ? new ProfileDeleteRecord.Reference(reference.birth(), reference.digest(),
                    reference.sealDigest(), birth) : reference;
            var initial = new ProfileDeleteRecord.PreparedDeliveries(bound, birth, manifest.targets().size(), 0);
            var checks = new java.util.ArrayList<>(recoveryRootChecks(frozen, deadline));
            checks.addAll(metadata.checksForMembership(member));
            checks.add(Op.check(deleteManifestPath(manifest.ledger().ledgerId()), 0));
            checks.add(Op.check(deleteManifestPath(manifest.ledger().ledgerId()) + "/birth-"
                    + Long.toHexString(reference.birth()), 0));
            checks.add(Op.create(name, initial.encode(), acls, CreateMode.PERSISTENT));
            checks.add(Op.create(name + "/birth-" + HexFormat.of().formatHex(birth), birth,
                    immutableAcls, CreateMode.PERSISTENT));
            if (!live.getAsBoolean()) {
                return null;
            }
            multi(checks, deadline);
            current = preparedDeliveries(manifest.ledger().ledgerId(), manifest, reference, deadline);
        }
        while (current != null && current.value().preparedThrough() < manifest.targets().size()) {
            if (!live.getAsBoolean()) {
                return null;
            }
            int slot = current.value().preparedThrough();
            var target = manifest.targets().get(slot);
            long ledger = manifest.ledger().ledgerId();
            String headPath = deliveryPath(target);
            String receiptPath = path(ledger) + "/delete-delivery-" + deliveryTargetKey(target);
            var receipt = reads.get(receiptPath, deadline);
            var head = deleteHead(target, deadline);
            if (head == null) {
                if (receipt.code() != Code.NONODE.intValue()) {
                    throw new IllegalArgumentException("Retained delivery has lost its required stream head");
                }
                if (createInitialDeleteAssignment(target).status() != ReadStatus.FOUND) {
                    return null;
                }
                head = deleteHead(target, deadline);
            }
            if (head == null || head.head().committedThrough() == ProfileDeleteDelivery.MAX_EVENTS
                    && receipt.code() == Code.NONODE.intValue()) {
                return null;
            }
            var bound = current.value().reference();
            var checks = new java.util.ArrayList<>(recoveryRootChecks(frozen, deadline));
            checks.addAll(metadata.checksForMembership(member));
            checks.add(Op.check(current.guard(), 0));
            checks.add(Op.check(headPath + "/birth-" + HexFormat.of().formatHex(head.head().streamId()), 0));
            if (receipt.code() == Code.OK.intValue()) {
                var resolved = resolveDelivery(ledger, target, receipt, head.node(), head.head(), deadline);
                var event = resolved.event();
                if (!event.ledger().equals(manifest.ledger()) || event.deleteEpoch() != manifest.seal().epoch()
                        || !Arrays.equals(event.requestId(), manifest.seal().requestId())
                        || !Arrays.equals(event.tombstone().encode(), bound.encode())) {
                    throw new IllegalArgumentException("Retained delivery belongs to another delete request");
                }
                checks.add(Op.check(receiptPath, 0));
                checks.add(Op.check(headPath + "/event-" + Long.toHexString(event.sequence()), 0));
                checks.add(Op.check(headPath, head.node().stat().getVersion()));
            } else if (receipt.code() == Code.NONODE.intValue()) {
                var next = head.head().next();
                var event = new ProfileDeleteDelivery.Event(next.streamId(), next.streamGeneration(),
                        next.assignmentGeneration(), next.committedThrough(), manifest.ledger(), target, bound,
                        manifest.seal().requestId(), manifest.seal().epoch());
                checks.add(Op.create(headPath + "/event-" + Long.toHexString(event.sequence()), event.encode(),
                        immutableAcls, CreateMode.PERSISTENT));
                checks.add(Op.create(receiptPath, event.encode(), immutableAcls, CreateMode.PERSISTENT));
                checks.add(Op.setData(headPath, next.encode(), head.node().stat().getVersion()));
            } else {
                return null;
            }
            checks.add(Op.setData(name, current.value().next().encode(), current.node().stat().getVersion()));
            if (!live.getAsBoolean()) {
                return null;
            }
            multi(checks, deadline);
            var after = preparedDeliveries(ledger, manifest, reference, deadline);
            if (after == null || after.value().preparedThrough() <= slot) {
                return null;
            }
            current = after;
        }
        return current;
    }

    /** Logical deletion only: same-record seal, complete retained targets and authoritative tombstone.
     * Does not wait for target ACKs, local barriers or physical reclaim; no online-delete fallback. */
    DeleteResult deleteClosed(ProfileLedgerMetadataStore metadata, long ledger, byte[] request,
            java.util.function.BooleanSupplier live) {
        requireRecoveryClient(metadata);
        request = ProfileLifecycleHead.fixed(request, 16, true);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var observed = read(ledger, deadline);
            var member = metadata.readMembership(ledger, deadline);
            if (observed.status() != ReadStatus.FOUND || member.snapshot() == null) {
                return new DeleteResult(observed.status() == ReadStatus.UNAVAILABLE
                        || member.status() == ReadStatus.UNAVAILABLE ? DeleteStatus.UNAVAILABLE
                            : DeleteStatus.QUARANTINED, null);
            }
            var root = observed.snapshot();
            var value = member.snapshot().metadata().getValue();
            if (value.getState() != LedgerMetadata.State.CLOSED) {
                return new DeleteResult(DeleteStatus.PRECONDITION_FAILED, null);
            }
            var context = ProfileRecoveryContext.decode(value.getCustomMetadata().get(ProfileRecoveryContext.KEY));
            var closed = readClosedRecovery(metadata, context);
            if (closed.metadata() == null) {
                return new DeleteResult(closed.outcome() == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        ? DeleteStatus.UNAVAILABLE : DeleteStatus.QUARANTINED, null);
            }
            member = new ProfileLedgerMetadataStore.MembershipRead(ReadStatus.FOUND, closed.metadata());
            value = closed.metadata().metadata().getValue();
            byte[] payload = ProfileRepairIntent.digest(java.nio.ByteBuffer.allocate(
                    org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext.ENCODED_LENGTH + 16)
                    .put(context.ledger().encode()).put(request).array());
            if (root.lifecycle().state() != ProfileLifecycleHead.State.DELETE_INTENT) {
                if (!context.matchesRoot(root) || !live.getAsBoolean()) {
                    return new DeleteResult(live.getAsBoolean() ? DeleteStatus.CONFLICT : DeleteStatus.CANCELLED, null);
                }
                var cut = cutDelete(root, request, payload, metadata.checksForMembership(closed.metadata()));
                if (cut.snapshot() == null) {
                    return new DeleteResult(cut.status() == CreateStatus.CONFLICT ? DeleteStatus.CONFLICT
                            : cut.status() == CreateStatus.QUARANTINED ? DeleteStatus.QUARANTINED
                                : DeleteStatus.UNAVAILABLE, null);
                }
                root = cut.snapshot();
            }
            if (!Arrays.equals(root.lifecycle().operationId(), request)
                    || !Arrays.equals(root.lifecycle().payloadIdentity(), payload)) {
                return new DeleteResult(DeleteStatus.CONFLICT, null);
            }
            if (!live.getAsBoolean()) {
                return new DeleteResult(DeleteStatus.CANCELLED, null);
            }
            byte[] oldSeal = value.getCustomMetadata().get(ProfileDeleteRecord.SEAL_KEY);
            var seal = oldSeal == null ? new ProfileDeleteRecord.Seal(request, root.lifecycle().controlGeneration(),
                    Math.toIntExact(((org.apache.bookkeeper.versioning.LongVersion) member.snapshot().metadata()
                        .getVersion()).getLongVersion()), member.snapshot().birthZxid(), root.birthZxid(),
                    ProfileDeleteRecord.closedDigest(value)) : ProfileDeleteRecord.Seal.decode(oldSeal);
            if (oldSeal == null) {
                var checks = new java.util.ArrayList<>(recoveryRootChecks(root, deadline));
                checks.addAll(metadata.checksForMembership(member.snapshot()));
                checks.add(metadata.deleteSealWrite(member.snapshot(), seal));
                multi(checks, deadline);
            }
            var actual = metadata.readMembership(ledger, deadline);
            if (actual.snapshot() == null) {
                return new DeleteResult(DeleteStatus.UNAVAILABLE, null);
            }
            deleteProjection(root, actual.snapshot(), deadline);
            value = actual.snapshot().metadata().getValue();
            if (!Arrays.equals(value.getCustomMetadata().get(ProfileDeleteRecord.SEAL_KEY), seal.encode())) {
                return new DeleteResult(DeleteStatus.UNAVAILABLE, null);
            }
            byte[] tombstone = value.getCustomMetadata().get(ProfileDeleteRecord.TOMBSTONE_KEY);
            if (tombstone != null) {
                var latest = read(ledger, deadline);
                if (latest.snapshot() == null || !latest.snapshot().equals(root)) {
                    return new DeleteResult(latest.status() == ReadStatus.UNAVAILABLE ? DeleteStatus.UNAVAILABLE
                            : DeleteStatus.CONFLICT, null);
                }
                var reference = ProfileDeleteRecord.Reference.decode(tombstone);
                var manifest = deleteManifest(ledger, reference, seal, deadline);
                var published = reference.publicationId() == null
                        ? prepareDeleteDeliveries(metadata, root, actual.snapshot(), manifest, reference, false,
                            live, deadline)
                        : preparedDeliveries(ledger, manifest, reference, deadline);
                if (published != null && published.value().preparedThrough() == manifest.targets().size()) {
                    return new DeleteResult(DeleteStatus.LOGICALLY_DELETED, manifest);
                }
                return new DeleteResult(reference.publicationId() != null ? DeleteStatus.QUARANTINED
                        : live.getAsBoolean() ? DeleteStatus.UNAVAILABLE : DeleteStatus.CANCELLED, null);
            }
            if (!live.getAsBoolean()) {
                return new DeleteResult(DeleteStatus.CANCELLED, null);
            }
            var retained = retainedActivationTargets(ledger);
            if (retained.status() != ReadStatus.FOUND) {
                return new DeleteResult(retained.status() == ReadStatus.UNAVAILABLE ? DeleteStatus.UNAVAILABLE
                        : DeleteStatus.QUARANTINED, null);
            }
            var targets = new java.util.TreeSet<ProfileDeleteRecord.Target>();
            retained.targets().forEach(receipt -> targets.add(new ProfileDeleteRecord.Target(
                    receipt.scope().targetBookie(), receipt.scope().targetStorageIncarnation())));
            if (value.getAllEnsembles().values().stream().flatMap(List::stream)
                    .anyMatch(bookie -> targets.stream()
                        .noneMatch(target -> target.bookie().equals(bookie.toString())))) {
                return new DeleteResult(DeleteStatus.QUARANTINED, null);
            }
            var manifest = new ProfileDeleteRecord.Manifest(context.ledger(), seal, List.copyOf(targets));
            String name = deleteManifestPath(ledger);
            var node = reads.get(name, deadline);
            if (node.code() == Code.NONODE.intValue()) {
                var checks = new java.util.ArrayList<>(recoveryRootChecks(root, deadline));
                checks.addAll(metadata.checksForMembership(actual.snapshot()));
                checks.add(Op.create(name, manifest.encode(), pageAcls, CreateMode.PERSISTENT));
                multi(checks, deadline);
                node = reads.get(name, deadline);
            }
            requireFound(node);
            requirePersistent(node, true);
            if (!Arrays.equals(node.bytes(), manifest.encode())) {
                return new DeleteResult(DeleteStatus.CONFLICT, null);
            }
            var reference = new ProfileDeleteRecord.Reference(node.stat().getCzxid(),
                    ProfileRepairIntent.digest(node.bytes()), ProfileRepairIntent.digest(seal.encode()));
            String guardPath = name + "/birth-" + Long.toHexString(reference.birth());
            byte[] guardBytes = java.nio.ByteBuffer.allocate(40).putLong(reference.birth())
                    .put(reference.digest()).array();
            var guard = reads.get(guardPath, deadline);
            if (guard.code() == Code.NONODE.intValue()) {
                var checks = new java.util.ArrayList<>(recoveryRootChecks(root, deadline));
                checks.addAll(metadata.checksForMembership(actual.snapshot()));
                checks.add(Op.check(name, 0));
                checks.add(Op.create(guardPath, guardBytes, immutableAcls, CreateMode.PERSISTENT));
                multi(checks, deadline);
            }
            deleteManifest(ledger, reference, seal, deadline);
            if (!live.getAsBoolean()) {
                return new DeleteResult(DeleteStatus.CANCELLED, null);
            }
            var published = prepareDeleteDeliveries(metadata, root, actual.snapshot(), manifest, reference,
                    true, live, deadline);
            if (published == null || published.value().preparedThrough() != manifest.targets().size()) {
                return new DeleteResult(live.getAsBoolean() ? DeleteStatus.UNAVAILABLE : DeleteStatus.CANCELLED, null);
            }
            if (!live.getAsBoolean()) {
                return new DeleteResult(DeleteStatus.CANCELLED, null);
            }
            var checks = new java.util.ArrayList<>(recoveryRootChecks(root, deadline));
            checks.addAll(metadata.checksForMembership(actual.snapshot()));
            checks.add(Op.check(name, 0));
            checks.add(Op.check(guardPath, 0));
            checks.add(Op.check(published.guard(), 0));
            checks.add(Op.check(name + "/delivery-prepared", manifest.targets().size()));
            reference = published.value().reference();
            checks.add(metadata.deleteTombstoneWrite(actual.snapshot(), reference));
            multi(checks, deadline);
            var done = metadata.readMembership(ledger, deadline);
            if (done.snapshot() == null) {
                return new DeleteResult(DeleteStatus.UNAVAILABLE, null);
            }
            deleteProjection(root, done.snapshot(), deadline);
            var latest = read(ledger, deadline);
            if (latest.snapshot() == null || !latest.snapshot().equals(root)) {
                return new DeleteResult(latest.status() == ReadStatus.UNAVAILABLE ? DeleteStatus.UNAVAILABLE
                        : DeleteStatus.CONFLICT, null);
            }
            return Arrays.equals(done.snapshot().metadata().getValue().getCustomMetadata()
                    .get(ProfileDeleteRecord.TOMBSTONE_KEY), reference.encode())
                        ? new DeleteResult(DeleteStatus.LOGICALLY_DELETED, manifest)
                        : new DeleteResult(DeleteStatus.UNAVAILABLE, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeleteResult(DeleteStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new DeleteResult(DeleteStatus.UNAVAILABLE, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new DeleteResult(DeleteStatus.QUARANTINED, null);
        }
    }

    public enum DeliveryStatus {
        COMMITTED, PREPARED, NOT_LOGICALLY_DELETED, NOT_APPLICABLE, LIMIT, QUARANTINED, UNAVAILABLE
    }

    public record DeliveryResult(DeliveryStatus status, ProfileDeleteDelivery.Event event) {
        public DeliveryResult {
            if ((status == DeliveryStatus.COMMITTED || status == DeliveryStatus.PREPARED) != (event != null)) {
                throw new IllegalArgumentException("Only recorded delete delivery carries an event");
            }
        }
    }

    private String deliveryRoot() {
        return root + "/delete-delivery";
    }

    private static String deliveryTargetKey(ProfileDeleteRecord.Target target) {
        byte[] name = target.bookie().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return HexFormat.of().formatHex(ProfileRepairIntent.digest(java.nio.ByteBuffer.allocate(18 + name.length)
                .putShort((short) name.length).put(name).put(target.incarnation()).array()));
    }

    private String deliveryPath(ProfileDeleteRecord.Target target) {
        return deliveryRoot() + "/" + deliveryTargetKey(target);
    }

    private void verifyDeliveryHead(ProfileZKReadClient.Node node, ProfileDeleteDelivery.Head head,
            String parent, long deadline) throws InterruptedException, TimeoutException {
        requireFound(node);
        requirePersistent(node, false);
        if (node.stat().getVersion() != head.committedThrough()) {
            throw new IllegalArgumentException("Delete stream head version no longer matches its committed sequence");
        }
        var guard = reads.get(parent + "/birth-" + HexFormat.of().formatHex(head.streamId()), deadline);
        requireFound(guard);
        requirePersistent(guard, true);
        if (guard.stat().getCzxid() != node.stat().getCzxid() || !Arrays.equals(head.streamId(), guard.bytes())) {
            throw new IllegalArgumentException("Delete stream assignment birth is missing or recreated");
        }
        var assignment = reads.get(parent + "-assignment", deadline);
        requireFound(assignment);
        requirePersistent(assignment, true);
        var initial = ProfileDeleteDelivery.Head.decode(assignment.bytes());
        if (assignment.stat().getCzxid() != node.stat().getCzxid() || initial.committedThrough() != 0
                || !Arrays.equals(initial.streamId(), head.streamId()) || !initial.target().equals(head.target())) {
            throw new IllegalArgumentException("Initial delete assignment cannot be replaced with a new stream");
        }
    }

    public record DeleteHeadResult(ReadStatus status, ProfileDeleteDelivery.Head head) {
        public DeleteHeadResult {
            if ((status == ReadStatus.FOUND) != (head != null)) {
                throw new IllegalArgumentException("Only verified delete assignment carries its head");
            }
        }
    }

    private record DeleteHeadSnapshot(ProfileDeleteDelivery.Head head, ProfileZKReadClient.Node node) { }

    private DeleteHeadSnapshot deleteHead(ProfileDeleteRecord.Target target, long deadline)
            throws InterruptedException, TimeoutException {
        String name = deliveryPath(target);
        var node = reads.get(name, deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        requireFound(node);
        var head = ProfileDeleteDelivery.Head.decode(node.bytes());
        verifyDeliveryHead(node, head, name, deadline);
        if (!head.target().equals(target)) {
            throw new IllegalArgumentException("Delete assignment belongs to another storage incarnation");
        }
        return new DeleteHeadSnapshot(head, node);
    }

    /** Explicit initial commissioning only. Startup readers never recreate missing assignment authority. */
    public DeleteHeadResult createInitialDeleteAssignment(ProfileDeleteRecord.Target target) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var parent = reads.get(deliveryRoot(), deadline);
            byte[] parentBytes = java.nio.ByteBuffer.allocate(8).putInt(0x55574450).putInt(1).array();
            if (parent.code() == Code.NONODE.intValue()) {
                multi(List.of(Op.create(deliveryRoot(), parentBytes, pageAcls, CreateMode.PERSISTENT)), deadline);
                parent = reads.get(deliveryRoot(), deadline);
            }
            requireFound(parent);
            requirePersistent(parent, true);
            if (!Arrays.equals(parent.bytes(), parentBytes)) {
                return new DeleteHeadResult(ReadStatus.QUARANTINED, null);
            }
            String name = deliveryPath(target);
            if (reads.get(name, deadline).code() == Code.NONODE.intValue()) {
                var previous = reads.get(name + "-assignment", deadline);
                if (previous.code() != Code.NONODE.intValue()) {
                    return new DeleteHeadResult(previous.code() == Code.OK.intValue()
                            ? ReadStatus.QUARANTINED : ReadStatus.UNAVAILABLE, null);
                }
                UUID id = UUID.randomUUID();
                byte[] stream = java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                        .putLong(id.getLeastSignificantBits()).array();
                var initial = new ProfileDeleteDelivery.Head(stream, 1, 1, 0, target);
                multi(List.of(Op.create(name, initial.encode(), acls, CreateMode.PERSISTENT),
                        Op.create(name + "/birth-" + HexFormat.of().formatHex(stream), stream,
                                immutableAcls, CreateMode.PERSISTENT),
                        Op.create(name + "-assignment", initial.encode(), immutableAcls, CreateMode.PERSISTENT)),
                        deadline);
            }
            return readDeleteHead(target);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeleteHeadResult(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new DeleteHeadResult(ReadStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new DeleteHeadResult(ReadStatus.QUARANTINED, null);
        }
    }

    /** Missing is unknown authority, never an empty applicable set or a bootstrap instruction. */
    public DeleteHeadResult readDeleteHead(ProfileDeleteRecord.Target target) {
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            if (reads.sync(root, deadline) != Code.OK.intValue()) {
                return new DeleteHeadResult(ReadStatus.UNAVAILABLE, null);
            }
            var current = deleteHead(target, deadline);
            return current == null ? new DeleteHeadResult(ReadStatus.NOT_FOUND, null)
                    : new DeleteHeadResult(ReadStatus.FOUND, current.head());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeleteHeadResult(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new DeleteHeadResult(ReadStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new DeleteHeadResult(ReadStatus.QUARANTINED, null);
        }
    }

    void requireDeletePublisherClient(ZooKeeper client) {
        if (client != zk) {
            throw new IllegalArgumentException("Delete registration cut must use the publisher's exact ZK client");
        }
    }

    /** Checks join the caller's existing writable-publication multi, ordering it against every head increment. */
    List<Op> deleteRegistrationChecks(org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord record,
            org.apache.bookkeeper.common.profile.control.ProfileControlRequest.DeleteBinding local, long deadline)
            throws InterruptedException, TimeoutException {
        if (reads.sync(root, deadline) != Code.OK.intValue()) {
            throw new IllegalStateException("Delete assignment unavailable for writable registration");
        }
        var target = new ProfileDeleteRecord.Target(record.bookieId(), record.storageIncarnation().value());
        var current = deleteHead(target, deadline);
        if (current == null
                || record.effectiveAssignmentGeneration().value() != current.head().assignmentGeneration()) {
            throw new IllegalStateException("Missing or different effective delete assignment");
        }
        var head = current.head();
        if (local == null ? head.committedThrough() != 0
                : !Arrays.equals(local.streamId(), head.streamId())
                    || local.streamGeneration() != head.streamGeneration()
                    || local.assignmentGeneration() != head.assignmentGeneration()
                    || local.sequence() != head.committedThrough()) {
            throw new IllegalStateException("Local delete-applied cursor has not reached required-through");
        }
        String name = deliveryPath(target);
        return List.of(Op.check(name + "-assignment", 0),
                Op.check(name + "/birth-" + HexFormat.of().formatHex(head.streamId()), 0),
                Op.check(name, current.node().stat().getVersion()));
    }

    private DeliveryResult resolveDelivery(long ledger, ProfileDeleteRecord.Target target,
            ProfileZKReadClient.Node receipt, ProfileZKReadClient.Node headNode, ProfileDeleteDelivery.Head head,
            long deadline) throws InterruptedException, TimeoutException {
        requireFound(receipt);
        requirePersistent(receipt, true);
        var event = ProfileDeleteDelivery.Event.decode(receipt.bytes());
        if (event.ledger().ledgerId() != ledger || !event.target().equals(target) || !event.belongsTo(head)) {
            throw new IllegalArgumentException("Retained delete delivery does not match its stream assignment");
        }
        var actual = reads.get(deliveryPath(target) + "/event-" + Long.toHexString(event.sequence()), deadline);
        requireFound(actual);
        requirePersistent(actual, true);
        if (actual.stat().getCzxid() != receipt.stat().getCzxid()
                || actual.stat().getCzxid() > headNode.stat().getMzxid()
                || !Arrays.equals(actual.bytes(), receipt.bytes())) {
            throw new IllegalArgumentException("Delete event/index commit is missing, recreated or mismatched");
        }
        return new DeliveryResult(DeliveryStatus.COMMITTED, event);
    }

    /** Commit one event and contiguous head in one ZK multi; each incarnation shares this stream across ledgers. */
    public DeliveryResult appendDeleteDelivery(ProfileLedgerMetadataStore metadata, long ledger,
            ProfileDeleteRecord.Target target) {
        requireRecoveryClient(metadata);
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            var rootRead = read(ledger, deadline);
            var member = metadata.readMembership(ledger, deadline);
            if (rootRead.snapshot() == null || member.snapshot() == null) {
                return new DeliveryResult(rootRead.status() == ReadStatus.UNAVAILABLE
                        || member.status() == ReadStatus.UNAVAILABLE ? DeliveryStatus.UNAVAILABLE
                            : DeliveryStatus.QUARANTINED, null);
            }
            var state = rootRead.snapshot();
            var value = member.snapshot().metadata().getValue();
            if (state.lifecycle().state() != ProfileLifecycleHead.State.DELETE_INTENT
                    || !value.getCustomMetadata().containsKey(ProfileDeleteRecord.TOMBSTONE_KEY)) {
                return new DeliveryResult(DeliveryStatus.NOT_LOGICALLY_DELETED, null);
            }
            deleteProjection(state, member.snapshot(), deadline);
            var reference = ProfileDeleteRecord.Reference.decode(value.getCustomMetadata()
                    .get(ProfileDeleteRecord.TOMBSTONE_KEY));
            var seal = ProfileDeleteRecord.Seal.decode(value.getCustomMetadata().get(ProfileDeleteRecord.SEAL_KEY));
            var manifest = deleteManifest(ledger, reference, seal, deadline);
            if (!manifest.targets().contains(target)) {
                return new DeliveryResult(DeliveryStatus.NOT_APPLICABLE, null);
            }
            if (reference.publicationId() != null) {
                var recorded = reads.get(path(ledger) + "/delete-delivery-" + deliveryTargetKey(target), deadline);
                if (recorded.code() != Code.OK.intValue()) {
                    return new DeliveryResult(recorded.code() == Code.NONODE.intValue()
                            ? DeliveryStatus.QUARANTINED : DeliveryStatus.UNAVAILABLE, null);
                }
                requirePersistent(recorded, true);
                var event = ProfileDeleteDelivery.Event.decode(recorded.bytes());
                if (!event.ledger().equals(manifest.ledger()) || !event.target().equals(target)
                        || event.deleteEpoch() != seal.epoch() || !Arrays.equals(event.requestId(), seal.requestId())
                        || !Arrays.equals(event.tombstone().encode(), reference.encode())) {
                    return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
                }
                return readDeleteDelivery(metadata, target, event.sequence());
            }
            var parent = reads.get(deliveryRoot(), deadline);
            byte[] parentBytes = java.nio.ByteBuffer.allocate(8).putInt(0x55574450).putInt(1).array();
            if (parent.code() == Code.NONODE.intValue()) {
                multi(List.of(Op.create(deliveryRoot(), parentBytes, pageAcls, CreateMode.PERSISTENT)), deadline);
                parent = reads.get(deliveryRoot(), deadline);
            }
            requireFound(parent);
            requirePersistent(parent, true);
            if (!Arrays.equals(parent.bytes(), parentBytes)) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            String name = deliveryPath(target);
            String receiptName = path(ledger) + "/delete-delivery-" + deliveryTargetKey(target);
            var headNode = reads.get(name, deadline);
            if (headNode.code() == Code.NONODE.intValue()) {
                var previousAssignment = reads.get(name + "-assignment", deadline);
                var previousReceipt = reads.get(receiptName, deadline);
                if (previousAssignment.code() != Code.NONODE.intValue()
                        || previousReceipt.code() != Code.NONODE.intValue()) {
                    return new DeliveryResult(previousAssignment.code() == Code.OK.intValue()
                            || previousReceipt.code() == Code.OK.intValue() ? DeliveryStatus.QUARANTINED
                                : DeliveryStatus.UNAVAILABLE, null);
                }
                UUID id = UUID.randomUUID();
                byte[] stream = java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                        .putLong(id.getLeastSignificantBits()).array();
                var initial = new ProfileDeleteDelivery.Head(stream, 1, 1, 0, target);
                // Czxid is the multi's transaction. The immutable child name fixes the initial stream identity.
                String guardName = name + "/birth-" + HexFormat.of().formatHex(stream);
                multi(List.of(Op.create(name, initial.encode(), acls, CreateMode.PERSISTENT),
                        Op.create(guardName, stream, immutableAcls, CreateMode.PERSISTENT),
                        Op.create(name + "-assignment", initial.encode(), immutableAcls, CreateMode.PERSISTENT)),
                        deadline);
            }
            headNode = reads.get(name, deadline);
            requireFound(headNode);
            var head = ProfileDeleteDelivery.Head.decode(headNode.bytes());
            verifyDeliveryHead(headNode, head, name, deadline);
            if (!head.target().equals(target)) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            var receipt = reads.get(receiptName, deadline);
            if (receipt.code() == Code.OK.intValue()) {
                var resolved = resolveDelivery(ledger, target, receipt, headNode, head, deadline);
                if (!Arrays.equals(resolved.event().tombstone().encode(), reference.encode())
                        || !resolved.event().ledger().equals(manifest.ledger())
                        || resolved.event().deleteEpoch() != seal.epoch()
                        || !Arrays.equals(resolved.event().requestId(), seal.requestId())) {
                    return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
                }
                return resolved;
            }
            if (receipt.code() != Code.NONODE.intValue()) {
                return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
            }
            if (head.committedThrough() == ProfileDeleteDelivery.MAX_EVENTS) {
                return new DeliveryResult(DeliveryStatus.LIMIT, null);
            }
            var next = head.next();
            var event = new ProfileDeleteDelivery.Event(head.streamId(), head.streamGeneration(),
                    head.assignmentGeneration(), next.committedThrough(), manifest.ledger(), target, reference,
                    seal.requestId(), seal.epoch());
            var operations = new java.util.ArrayList<>(recoveryRootChecks(state, deadline));
            operations.addAll(metadata.checksForMembership(member.snapshot()));
            operations.add(Op.check(name + "/birth-" + HexFormat.of().formatHex(head.streamId()), 0));
            operations.add(Op.create(name + "/event-" + Long.toHexString(event.sequence()), event.encode(),
                    immutableAcls, CreateMode.PERSISTENT));
            operations.add(Op.create(receiptName, event.encode(), immutableAcls, CreateMode.PERSISTENT));
            operations.add(Op.setData(name, next.encode(), headNode.stat().getVersion()));
            multi(operations, deadline);
            var after = reads.get(name, deadline);
            requireFound(after);
            var actual = ProfileDeleteDelivery.Head.decode(after.bytes());
            verifyDeliveryHead(after, actual, name, deadline);
            var committed = reads.get(receiptName, deadline);
            if (committed.code() == Code.OK.intValue()) {
                return resolveDelivery(ledger, target, committed, after, actual, deadline);
            }
            return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
        }
    }

    /** Read only a committed effect. Missing sequence/receipt never becomes a cursor or non-applicability proof. */
    public DeliveryResult readDeleteDelivery(ProfileLedgerMetadataStore metadata, ProfileDeleteRecord.Target target,
            long sequence) {
        requireRecoveryClient(metadata);
        if (sequence < 1 || sequence > ProfileDeleteDelivery.MAX_EVENTS) {
            throw new IllegalArgumentException("Invalid bounded delivery sequence");
        }
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            if (reads.sync(root, deadline) != Code.OK.intValue()) {
                return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
            }
            String name = deliveryPath(target);
            var node = reads.get(name, deadline);
            requireFound(node);
            var head = ProfileDeleteDelivery.Head.decode(node.bytes());
            verifyDeliveryHead(node, head, name, deadline);
            if (!head.target().equals(target)) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            if (sequence > head.committedThrough()) {
                return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
            }
            var eventNode = reads.get(name + "/event-" + Long.toHexString(sequence), deadline);
            requireFound(eventNode);
            requirePersistent(eventNode, true);
            var event = ProfileDeleteDelivery.Event.decode(eventNode.bytes());
            if (event.sequence() != sequence || !event.belongsTo(head)) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            var receipt = reads.get(path(event.ledger().ledgerId()) + "/delete-delivery-" + deliveryTargetKey(target),
                    deadline);
            var resolved = resolveDelivery(event.ledger().ledgerId(), target, receipt, node, head, deadline);
            if (!event.equals(resolved.event())) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            var state = read(event.ledger().ledgerId(), deadline);
            var member = metadata.readMembership(event.ledger().ledgerId(), deadline);
            if (state.snapshot() == null || member.snapshot() == null) {
                return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
            }
            deleteProjection(state.snapshot(), member.snapshot(), deadline);
            var value = member.snapshot().metadata().getValue();
            byte[] reference = value.getCustomMetadata().get(ProfileDeleteRecord.TOMBSTONE_KEY);
            var seal = ProfileDeleteRecord.Seal.decode(value.getCustomMetadata().get(ProfileDeleteRecord.SEAL_KEY));
            if (!seal.matches(state.snapshot()) || reference != null
                    && !Arrays.equals(reference, event.tombstone().encode())
                    || event.deleteEpoch() != seal.epoch() || !Arrays.equals(event.requestId(), seal.requestId())) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            var manifest = deleteManifest(event.ledger().ledgerId(), event.tombstone(), seal, deadline);
            if (!manifest.ledger().equals(event.ledger()) || !manifest.targets().contains(target)) {
                return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
            }
            if (event.tombstone().publicationId() != null) {
                var published = preparedDeliveries(event.ledger().ledgerId(), manifest, event.tombstone(), deadline);
                int slot = manifest.targets().indexOf(target);
                if (published == null || published.value().preparedThrough() <= slot
                        || reference != null && published.value().preparedThrough() != manifest.targets().size()) {
                    return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
                }
            }
            return reference == null ? new DeliveryResult(DeliveryStatus.PREPARED, event) : resolved;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new DeliveryResult(DeliveryStatus.UNAVAILABLE, null);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            return new DeliveryResult(DeliveryStatus.QUARANTINED, null);
        }
    }

    /** Exact committed delivery grants local TOMBSTONE; DELETE_INTENT alone never grants this authority. */
    public ProfileControlStore deleteAuthorityReader(String localBookie, byte[] storageIncarnation,
            ProfileLedgerMetadataStore metadata) {
        var target = new ProfileDeleteRecord.Target(localBookie, storageIncarnation);
        requireRecoveryClient(metadata);
        return key -> {
            if (key.purpose() != ProfileAuthorityPurpose.TOMBSTONE_OR_DELETE) {
                return ProfileControlStore.ReadResult.notFound();
            }
            long deadline = System.nanoTime() + timeoutNanos;
            try {
                var state = read(key.ledgerId(), deadline);
                if (state.snapshot() == null) {
                    return new ProfileControlStore.ReadResult(state.status(), null);
                }
                String domain = state.snapshot().reservation().installs().get(0).authorityDomain();
                if (!domain.equals(key.authorityDomain())) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                var receipt = reads.get(path(key.ledgerId()) + "/delete-delivery-" + deliveryTargetKey(target),
                        deadline);
                if (receipt.code() == Code.NONODE.intValue()) {
                    return ProfileControlStore.ReadResult.notFound();
                }
                requireFound(receipt);
                var candidate = ProfileDeleteDelivery.Event.decode(receipt.bytes());
                var committed = readDeleteDelivery(metadata, target, candidate.sequence());
                if (committed.status() != DeliveryStatus.COMMITTED) {
                    return committed.status() == DeliveryStatus.QUARANTINED
                            ? ProfileControlStore.ReadResult.quarantined()
                            : ProfileControlStore.ReadResult.unavailable();
                }
                var scope = committed.event().deleteApply(domain).scope();
                if (!candidate.equals(committed.event()) || !ProfileAuthorityKey.fromScope(scope).equals(key)) {
                    return ProfileControlStore.ReadResult.quarantined();
                }
                return ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(scope,
                        CommittedProfileAuthority.State.DELETE_AUTHORIZED, EngineProfile.SEGMENT_WAL_ENGINE,
                        receipt.stat().getCzxid(), true));
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

    /** Admission cut only. It retains published targets; it neither fences Bookies nor deletes/GCs any data. */
    public CreateResult cutDelete(Snapshot expected, byte[] operationId, byte[] payloadIdentity) {
        return cutDelete(expected, operationId, payloadIdentity, List.of());
    }

    private CreateResult cutDelete(Snapshot expected, byte[] operationId, byte[] payloadIdentity, List<Op> checks) {
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
            return cas(current, next, null, deadline, checks);
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
        return cas(current, desired, prepared, deadline, List.of());
    }

    private CreateResult cas(Snapshot current, ProfileLifecycleHead desired, ReadySnapshot prepared, long deadline,
            List<Op> additionalChecks) throws InterruptedException, TimeoutException {
        long ledger = desired.backlink().ledgerId();
        var operations = new java.util.ArrayList<Op>();
        operations.addAll(additionalChecks);
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
            // Admitted repair targets are retained independently of membership publication and delete cut.
            for (int slot = 0; slot < ProfileRepairIntent.MAX_INTENTS; slot++) {
                var repair = readRepair(ledger, slot, deadline);
                if (repair == null) {
                    continue;
                }
                var intent = repair.intent();
                if (intent.recovery().lifecycleBirthZxid() != current.birthZxid()
                        || !intent.recovery().lifecycleBirth().equals(current.lifecycle().birthId())) {
                    throw new IllegalArgumentException("Retained repair belongs to another lifecycle birth");
                }
                var admission = reads.get(repairPath(ledger, slot) + "/admitted", deadline);
                if (admission.code() == Code.NONODE.intValue()) {
                    continue;
                }
                requireFound(admission);
                validateRepairAdmission(repair, admission, deadline);
                var in = java.nio.ByteBuffer.wrap(admission.bytes());
                long installedGeneration = in.getLong(8);
                byte[] capability = new byte[32];
                in.position(16);
                in.get(capability);
                retain(targets, new ProfileInstallReceipt(intent.install(),
                        EngineProfile.SEGMENT_WAL_ENGINE,
                        capability, installedGeneration));
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

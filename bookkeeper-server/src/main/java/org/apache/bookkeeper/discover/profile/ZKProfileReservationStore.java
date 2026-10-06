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

import java.time.Duration;
import java.util.Arrays;
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
            if (page == null || !published(snapshot.lifecycle(), page)) {
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

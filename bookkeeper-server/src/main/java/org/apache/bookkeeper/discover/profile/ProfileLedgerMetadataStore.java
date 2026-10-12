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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.LedgerMetadataUtils;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/**
 * Experimental standard-metadata creation after the coordinator has verified reservation and all-E install.
 * Borrows the manager; its explicit ZK context enables guarded cold membership and recovery transitions.
 * It neither retries writes nor publishes READY, activation or a writer.
 */
public final class ProfileLedgerMetadataStore {
    public static final String MEMBERSHIP_FROZEN_KEY = "bookkeeper.experimental.profile.membership-frozen";
    public static final String PENDING_PUBLICATION_KEY = "bookkeeper.experimental.profile.pending-publication";
    public static final int MAX_EXPERIMENTAL_FRAGMENTS = 32;

    public enum Outcome { APPLIED, ALREADY_APPLIED, CONFLICT, QUARANTINED, DURABILITY_UNKNOWN }

    public record Result(Outcome outcome, Versioned<LedgerMetadata> metadata) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            if ((outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED) != (metadata != null)) {
                throw new IllegalArgumentException("Only a proved metadata transition carries metadata");
            }
        }

        @Override
        public String toString() {
            return "ProfileMetadataResult{outcome=" + outcome + ", metadataPresent=" + (metadata != null) + '}';
        }
    }

    /** One observed physical metadata birth. Keep it, not a newly captured birth, across UNKNOWN retries. */
    public record MembershipSnapshot(long birthZxid, Versioned<LedgerMetadata> metadata) {
        public MembershipSnapshot {
            Objects.requireNonNull(metadata, "metadata");
            if (birthZxid <= 0) {
                throw new IllegalArgumentException("Invalid physical metadata birth");
            }
        }

        @Override
        public String toString() {
            return "ProfileMembershipSnapshot{birthZxid=" + birthZxid + ", metadataPresent=true}";
        }
    }

    public record MembershipRead(ReadStatus status, MembershipSnapshot snapshot) {
        public MembershipRead {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (snapshot != null)) {
                throw new IllegalArgumentException("Only FOUND carries a physical metadata snapshot");
            }
        }
    }

    private final LedgerManager manager;
    private final ZooKeeper mutationClient;
    private final ProfileZKReadClient mutationReads;
    private final List<ACL> markerAcls;
    private final long mutationTimeoutNanos;

    public ProfileLedgerMetadataStore(LedgerManager manager) {
        this.manager = Objects.requireNonNull(manager, "manager");
        mutationClient = null;
        mutationReads = null;
        markerAcls = List.of();
        mutationTimeoutNanos = 0;
    }

    /** Enable bounded cold ZK membership mutation using the same client/namespace as this manager. */
    public ProfileLedgerMetadataStore(ZooKeeper client, AbstractZkLedgerManager manager, List<ACL> acls,
            Duration timeout) {
        this.manager = Objects.requireNonNull(manager, "manager");
        mutationClient = Objects.requireNonNull(client, "client");
        mutationReads = new ProfileZKReadClient(client);
        if (timeout.compareTo(Duration.ofMillis(1)) < 0 || timeout.compareTo(Duration.ofMinutes(1)) > 0
                || acls.isEmpty() || acls.stream().anyMatch(acl -> (acl.getPerms() & ZooDefs.Perms.READ) == 0)) {
            throw new IllegalArgumentException("Membership mutation requires bounded deadline and reader ACLs");
        }
        markerAcls = acls.stream().map(acl -> new ACL(ZooDefs.Perms.READ,
                new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        mutationTimeoutNanos = timeout.toNanos();
    }

    public CompletableFuture<Result> createIfAbsent(LedgerMetadata initial, ProfileMetadataBacklink backlink) {
        LedgerMetadata desired = snapshotInitial(initial, backlink);
        // The create callback can return the submitted object, including its NODEEXISTS token-resolution path.
        // One actual snapshot must prove the full reference/content/version even after an acknowledged create.
        return manager.createLedgerMetadata(desired.getLedgerId(), desired).handle((created, failure) ->
                manager.readLedgerMetadata(desired.getLedgerId()).handle((current, readFailure) ->
                        readFailure == null ? classify(desired, backlink, current,
                                failure == null ? Outcome.APPLIED : Outcome.ALREADY_APPLIED)
                                : new Result(Outcome.DURABILITY_UNKNOWN, null))).thenCompose(result -> result);
    }

    /** Bounded cold read; no record is created and FOUND is not activation/writer authority. */
    public MembershipRead readMembership(long ledger) {
        if (mutationClient == null || ledger < 0) {
            throw new IllegalArgumentException("Membership read requires an explicit ZK context and ledger");
        }
        return readMembership(ledger, System.nanoTime() + mutationTimeoutNanos);
    }

    MembershipRead readMembership(long ledger, long callerDeadline) {
        long now = System.nanoTime();
        long deadline = now + Math.min(callerDeadline - now, mutationTimeoutNanos);
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(ledger);
        try {
            if (mutationReads.sync(path, deadline) != Code.OK.intValue()) {
                return new MembershipRead(ReadStatus.UNAVAILABLE, null);
            }
            MembershipSnapshot actual = readMutationMetadata(path, ledger, deadline);
            if (actual == null) {
                return new MembershipRead(ReadStatus.NOT_FOUND, null);
            }
            ProfileMetadataBacklink.from(actual.metadata().getValue());
            return new MembershipRead(ReadStatus.FOUND, actual);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new MembershipRead(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new MembershipRead(ReadStatus.UNAVAILABLE, null);
        } catch (IOException | IllegalArgumentException invalid) {
            return new MembershipRead(ReadStatus.QUARANTINED, null);
        }
    }

    /**
     * One conditional membership mutation. The caller supplies its frozen LAC+1 and verified inactive targets.
     * This operation neither installs nor activates them, publishes authority, or resends DATA.
     * Freeze readMembership once per operation, and reuse that same birth/snapshot/mapping after UNKNOWN.
     * Call on a bounded cold caller, never a ZooKeeper callback, event loop, ledger or DATA owner thread.
     */
    public Result replaceEnsemble(MembershipSnapshot expectedBirth, long firstEntry,
            List<BookieId> ensemble) {
        if (mutationClient == null) {
            throw new IllegalStateException("Profile membership requires the explicit borrowed ZK context");
        }
        Objects.requireNonNull(expectedBirth, "expectedBirth");
        Versioned<LedgerMetadata> expected = expectedBirth.metadata();
        ensemble = List.copyOf(ensemble);
        LedgerMetadata input = expected.getValue();
        ProfileMetadataBacklink backlink = ProfileMetadataBacklink.from(input);
        if (!(expected.getVersion() instanceof LongVersion version) || version.getLongVersion() < 0
                || version.getLongVersion() >= Integer.MAX_VALUE || input.getCToken() < 0
                || input.getMetadataFormatVersion() != 3 || input.getDigestType() != DigestType.CRC32C
                || input.getEnsembleSize() > ProfileReservation.MAX_INITIAL_TARGETS
                || input.getAllEnsembles().isEmpty() || input.getAllEnsembles().firstKey() != 0
                || firstEntry < input.getAllEnsembles().lastKey() || ensemble.size() != input.getEnsembleSize()
                || new HashSet<>(ensemble).size() != ensemble.size()
                || ensemble.equals(input.getAllEnsembles().lastEntry().getValue())
                || input.getMetadataFormatVersion() != 3 || input.getDigestType() != DigestType.CRC32C
                || input.getAllEnsembles().isEmpty() || input.getAllEnsembles().firstKey() != 0
                || input.getAllEnsembles().size() > MAX_EXPERIMENTAL_FRAGMENTS
                || firstEntry > input.getAllEnsembles().lastKey()
                    && input.getAllEnsembles().size() == MAX_EXPERIMENTAL_FRAGMENTS) {
            throw new IllegalArgumentException("Invalid bounded Profile membership replacement");
        }
        if (!mutableMembership(input)) {
            return new Result(Outcome.CONFLICT, null);
        }
        // The original builder.from deliberately omits the creation token. Profile mutation must preserve it.
        LedgerMetadata base = LedgerMetadataBuilder.from(backlink.attachTo(input))
                .withCToken(input.getCToken()).build();
        var builder = LedgerMetadataBuilder.from(base).withCToken(base.getCToken());
        if (base.hasPassword()) {
            builder.withPassword(base.getPassword().clone());
        }
        if (firstEntry == base.getAllEnsembles().lastKey()) {
            builder.replaceEnsembleEntry(firstEntry, ensemble);
        } else {
            builder.newEnsembleEntry(firstEntry, ensemble);
        }
        LedgerMetadata desired = builder.build();
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(input.getLedgerId());
        long deadline = System.nanoTime() + mutationTimeoutNanos;
        try {
            byte[] encoded = new LedgerMetadataSerDe().serialize(desired);
            if (encoded.length > 1024 * 1024) {
                throw new IllegalArgumentException("Profile metadata exceeds the fixed mutation byte limit");
            }
            if (mutationReads.sync(path, deadline) != Code.OK.intValue()) {
                return new Result(Outcome.DURABILITY_UNKNOWN, null);
            }
            MembershipSnapshot before = readMutationMetadata(path, input.getLedgerId(), deadline);
            if (before == null) {
                return new Result(Outcome.QUARANTINED, null);
            }
            if (before.birthZxid() != expectedBirth.birthZxid()) {
                return new Result(Outcome.CONFLICT, null);
            }
            Result resolved = resolveReplacement(base, desired, expected.getVersion(), before.metadata(),
                    Outcome.ALREADY_APPLIED);
            if (resolved != null) {
                return resolved;
            }
            // A dataVersion alone cannot prevent delete/recreate ABA between the read and conditional write.
            String marker = path + "/profile-birth-" + Long.toHexString(before.birthZxid())
                    + "-" + Long.toHexString(base.getCToken());
            MembershipSnapshot checked = ensureMembershipGuard(before, deadline);
            if (checked == null) {
                return new Result(Outcome.QUARANTINED, null);
            }
            if (checked.birthZxid() != before.birthZxid()) {
                return new Result(Outcome.CONFLICT, null);
            }
            resolved = resolveReplacement(base, desired, expected.getVersion(), checked.metadata(),
                    Outcome.ALREADY_APPLIED);
            if (resolved != null) {
                return resolved;
            }
            int code = multi(List.of(Op.check(marker, 0),
                    Op.setData(path, encoded, (int) version.getLongVersion())), deadline);
            MembershipSnapshot after = readMutationMetadata(path, input.getLedgerId(), deadline);
            if (after == null) {
                return new Result(Outcome.QUARANTINED, null);
            }
            if (after.birthZxid() != before.birthZxid()) {
                return new Result(Outcome.CONFLICT, null);
            }
            resolved = resolveReplacement(base, desired, expected.getVersion(), after.metadata(),
                    code == Code.OK.intValue() ? Outcome.APPLIED : Outcome.ALREADY_APPLIED);
            // An unresolved submitted CAS is never creation authority or proof that no effect occurred.
            return resolved == null ? new Result(Outcome.DURABILITY_UNKNOWN, null) : resolved;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new Result(Outcome.DURABILITY_UNKNOWN, null);
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new Result(Outcome.DURABILITY_UNKNOWN, null);
        } catch (IOException | IllegalArgumentException invalid) {
            return new Result(Outcome.QUARANTINED, null);
        }
    }

    boolean usesMutationClient(ZooKeeper client) {
        return mutationClient == client;
    }

    /** Same-record OPEN -> IN_RECOVERY, guarded by both observed physical births. No fence/grant is implied. */
    Result enterRecovery(MembershipSnapshot expected, ProfileRecoveryContext context,
            List<Op> lifecycleChecks, long deadline) throws InterruptedException, TimeoutException, IOException {
        return enterRecovery(expected, context, lifecycleChecks, deadline, null);
    }

    Result enterRecovery(MembershipSnapshot expected, ProfileRecoveryContext context,
            List<Op> lifecycleChecks, long deadline, ProfileRecoveryBoundary.WriterClose writerClose)
            throws InterruptedException, TimeoutException, IOException {
        var input = expected.metadata().getValue();
        if (mutationClient == null || !mutableMembership(input)
                || !(expected.metadata().getVersion() instanceof LongVersion version)
                || version.getLongVersion() != context.predecessorVersion()
                || expected.birthZxid() != context.metadataBirth() || input.getCToken() != context.creatorToken()
                || input.getAllEnsembles().size() > MAX_EXPERIMENTAL_FRAGMENTS
                || input.getEnsembleSize() > ProfileReservation.MAX_INITIAL_TARGETS
                || input.getCustomMetadata().containsKey(ProfileRecoveryBoundary.WRITER_CLOSE_KEY)
                || !Arrays.equals(ProfileRecoveryContext.metadataDigest(
                    writerClose == null ? input : writerClose.attachTo(input)), context.predecessorDigest())) {
            return new Result(Outcome.CONFLICT, null);
        }
        var original = ProfileRecoveryContext.predecessorProjection(input);
        if (!sameMetadata(original, input)) {
            return new Result(Outcome.CONFLICT, null);
        }
        var base = writerClose == null ? original : writerClose.attachTo(original);
        var custom = new java.util.TreeMap<>(base.getCustomMetadata());
        custom.put(ProfileRecoveryContext.KEY, context.encode());
        var desired = LedgerMetadataBuilder.from(base).withCToken(base.getCToken())
                .withCustomMetadata(custom).withInRecoveryState().build();
        byte[] encoded = new LedgerMetadataSerDe().serialize(desired);
        if (encoded.length > 1024 * 1024) {
            throw new IllegalArgumentException("Profile recovery metadata exceeds its fixed bound");
        }
        var before = readMembership(input.getLedgerId(), deadline);
        if (before.status() != ReadStatus.FOUND) {
            return new Result(before.status() == ReadStatus.QUARANTINED || before.status() == ReadStatus.NOT_FOUND
                    ? Outcome.QUARANTINED : Outcome.DURABILITY_UNKNOWN, null);
        }
        if (context.matchesRecovery(before.snapshot())) {
            return new Result(Outcome.ALREADY_APPLIED, before.snapshot().metadata());
        }
        if (before.snapshot().birthZxid() != expected.birthZxid()
                || !before.snapshot().metadata().getVersion().equals(expected.metadata().getVersion())
                || !sameMetadata(input, before.snapshot().metadata().getValue())) {
            return new Result(Outcome.CONFLICT, null);
        }
        var guarded = ensureMembershipGuard(before.snapshot(), deadline);
        if (guarded == null || guarded.birthZxid() != expected.birthZxid()
                || !guarded.metadata().getVersion().equals(expected.metadata().getVersion())
                || !sameMetadata(input, guarded.metadata().getValue())) {
            return new Result(Outcome.CONFLICT, null);
        }
        var checks = new java.util.ArrayList<>(lifecycleChecks);
        checks.addAll(checksForMembership(expected));
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(input.getLedgerId());
        checks.add(Op.setData(path, encoded, (int) version.getLongVersion()));
        int code = multi(checks, deadline);
        var after = readMembership(input.getLedgerId(), deadline);
        if (after.status() != ReadStatus.FOUND) {
            return new Result(after.status() == ReadStatus.QUARANTINED || after.status() == ReadStatus.NOT_FOUND
                    ? Outcome.QUARANTINED : Outcome.DURABILITY_UNKNOWN, null);
        }
        if (context.matchesRecovery(after.snapshot())) {
            return new Result(code == Code.OK.intValue() ? Outcome.APPLIED : Outcome.ALREADY_APPLIED,
                    after.snapshot().metadata());
        }
        return new Result(code == Code.BADVERSION.intValue() || code == Code.NONODE.intValue()
                || after.snapshot().birthZxid() != expected.birthZxid()
                || !sameMetadata(input, after.snapshot().metadata().getValue())
                ? Outcome.CONFLICT : Outcome.DURABILITY_UNKNOWN, null);
    }

    private MembershipSnapshot readMutationMetadata(String path, long ledger, long deadline)
            throws InterruptedException, TimeoutException, IOException {
        ProfileZKReadClient.Node node = mutationReads.get(path, deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Profile membership metadata read unavailable");
        }
        if (node.stat().getEphemeralOwner() != 0 || node.stat().getVersion() < 0 || node.bytes() == null
                || node.bytes().length > 1024 * 1024) {
            throw new IllegalArgumentException("Invalid persistent Profile membership metadata");
        }
        LedgerMetadata metadata = new LedgerMetadataSerDe().parseConfig(node.bytes(), ledger,
                Optional.of(node.stat().getCtime()));
        return new MembershipSnapshot(node.stat().getCzxid(), new Versioned<>(metadata,
                new LongVersion(node.stat().getVersion())));
    }

    private int multi(List<Op> operations, long deadline) throws InterruptedException, TimeoutException {
        ProfileZKReadClient.requireTime(deadline);
        CompletableFuture<Integer> reply = new CompletableFuture<>();
        mutationClient.multi(operations, (code, ignored, context, results) -> reply.complete(code), null);
        return ProfileZKReadClient.await(reply, deadline);
    }

    /** Establish the same immutable physical-birth guard before a cold preparation/publication transaction. */
    MembershipSnapshot ensureMembershipGuard(MembershipSnapshot expected, long deadline)
            throws InterruptedException, TimeoutException, IOException {
        var metadata = expected.metadata().getValue();
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(metadata.getLedgerId());
        var before = readMutationMetadata(path, metadata.getLedgerId(), deadline);
        if (before == null || before.birthZxid() != expected.birthZxid()
                || !before.metadata().getVersion().equals(expected.metadata().getVersion())
                || !sameMetadata(metadata, before.metadata().getValue())) {
            return before;
        }
        String marker = path + "/profile-birth-" + Long.toHexString(expected.birthZxid())
                + "-" + Long.toHexString(metadata.getCToken());
        byte[] bytes = ByteBuffer.allocate(76).putInt(0x55574d42).putInt(1).putLong(expected.birthZxid())
                .putLong(metadata.getCToken()).put(ProfileMetadataBacklink.from(metadata).encode()).array();
        var guarded = mutationReads.get(marker, deadline);
        if (guarded.code() == Code.NONODE.intValue()) {
            multi(List.of(Op.check(path, (int) ((LongVersion) expected.metadata().getVersion()).getLongVersion()),
                    Op.create(marker, bytes, markerAcls, CreateMode.PERSISTENT)), deadline);
            guarded = mutationReads.get(marker, deadline);
        }
        if (guarded.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Membership birth guard unavailable");
        }
        if (guarded.stat().getEphemeralOwner() != 0 || guarded.stat().getVersion() != 0
                || !Arrays.equals(bytes, guarded.bytes())) {
            throw new IllegalArgumentException("Invalid immutable metadata birth guard");
        }
        return readMutationMetadata(path, metadata.getLedgerId(), deadline);
    }

    /** Required guard lookup during recovery adoption; never rebuild missing authority after restart. */
    void verifyRecoveryGuard(MembershipSnapshot current, long deadline)
            throws InterruptedException, TimeoutException {
        var value = current.metadata().getValue();
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(value.getLedgerId());
        String marker = path + "/profile-birth-" + Long.toHexString(current.birthZxid())
                + "-" + Long.toHexString(value.getCToken());
        byte[] expected = ByteBuffer.allocate(76).putInt(0x55574d42).putInt(1).putLong(current.birthZxid())
                .putLong(value.getCToken()).put(ProfileMetadataBacklink.from(value).encode()).array();
        var guard = mutationReads.get(marker, deadline);
        if (guard.code() != Code.OK.intValue() || guard.stat().getEphemeralOwner() != 0
                || guard.stat().getVersion() != 0 || !Arrays.equals(expected, guard.bytes())) {
            throw new IllegalArgumentException("Required recovery metadata birth guard is missing or corrupt");
        }
    }

    /** Same namespace checks for lifecycle publication: metadata birth/version cannot change across that CAS. */
    List<Op> checksForMembership(MembershipSnapshot snapshot) {
        if (mutationClient == null || !(snapshot.metadata().getVersion() instanceof LongVersion version)
                || version.getLongVersion() < 0 || version.getLongVersion() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Lifecycle publication requires the guarded metadata context");
        }
        var metadata = snapshot.metadata().getValue();
        String path = ((AbstractZkLedgerManager) manager).getLedgerPath(metadata.getLedgerId());
        String marker = path + "/profile-birth-" + Long.toHexString(snapshot.birthZxid())
                + "-" + Long.toHexString(metadata.getCToken());
        return List.of(Op.check(marker, 0), Op.check(path, (int) version.getLongVersion()));
    }

    Op repairMetadataWrite(MembershipSnapshot expected, LedgerMetadata desired) throws IOException {
        if (expected.metadata().getValue().getCustomMetadata().containsKey(MEMBERSHIP_FROZEN_KEY)) {
            throw new IllegalArgumentException("Delete-sealed membership is irreversible");
        }
        boolean closed = expected.metadata().getValue().getState() == LedgerMetadata.State.CLOSED;
        if (closed) {
            var before = expected.metadata().getValue();
            if (desired.getState() != LedgerMetadata.State.CLOSED || desired.getLastEntryId() != before.getLastEntryId()
                    || desired.getLength() != before.getLength() || !before.getCustomMetadata()
                        .containsKey(ProfileRecoveryBoundary.CLOSE_KEY) || !Arrays.equals(before.getCustomMetadata()
                            .get(ProfileRecoveryBoundary.CLOSE_KEY), desired.getCustomMetadata()
                                .get(ProfileRecoveryBoundary.CLOSE_KEY))) {
                throw new IllegalArgumentException("Closed repair must preserve its exact durable close cut");
            }
        }
        return recoveryMetadataWrite(expected, desired, closed);
    }

    Op deleteSealWrite(MembershipSnapshot expected, ProfileDeleteRecord.Seal seal) throws IOException {
        var value = expected.metadata().getValue();
        if (value.getState() != LedgerMetadata.State.CLOSED || expected.birthZxid() != seal.metadataBirth()
                || ((LongVersion) expected.metadata().getVersion()).getLongVersion() != seal.beforeVersion()
                || value.getCustomMetadata().containsKey(MEMBERSHIP_FROZEN_KEY)
                || value.getCustomMetadata().containsKey(ProfileDeleteRecord.TOMBSTONE_KEY)
                || value.getCustomMetadata().containsKey(ProfileDeleteRetention.KEY)
                || !Arrays.equals(ProfileDeleteRecord.closedDigest(value), seal.closedDigest())) {
            throw new IllegalArgumentException("Deletion seal requires its exact matching CLOSED predecessor");
        }
        var custom = new java.util.TreeMap<>(value.getCustomMetadata());
        custom.put(MEMBERSHIP_FROZEN_KEY, seal.encode());
        return recoveryMetadataWrite(expected, LedgerMetadataBuilder.from(value).withCToken(value.getCToken())
                .withCustomMetadata(custom).build(), true);
    }

    Op deleteTombstoneWrite(MembershipSnapshot expected, ProfileDeleteRecord.Reference reference) throws IOException {
        return deleteTombstoneWrite(expected, reference, null);
    }

    Op deleteTombstoneWrite(MembershipSnapshot expected, ProfileDeleteRecord.Reference reference,
            ProfileDeleteRetention.Receipt retention) throws IOException {
        var value = expected.metadata().getValue();
        var seal = ProfileDeleteRecord.Seal.decode(value.getCustomMetadata().get(MEMBERSHIP_FROZEN_KEY));
        if (value.getState() != LedgerMetadata.State.CLOSED || expected.birthZxid() != seal.metadataBirth()
                || ((LongVersion) expected.metadata().getVersion()).getLongVersion() != seal.beforeVersion() + 1L
                || value.getCustomMetadata().containsKey(ProfileDeleteRecord.TOMBSTONE_KEY)
                || value.getCustomMetadata().containsKey(ProfileDeleteRetention.KEY)
                || !Arrays.equals(ProfileRepairIntent.digest(seal.encode()), reference.sealDigest())) {
            throw new IllegalArgumentException("Cluster tombstone requires its sealed metadata and manifest");
        }
        var custom = new java.util.TreeMap<>(value.getCustomMetadata());
        custom.put(ProfileDeleteRecord.TOMBSTONE_KEY, reference.encode());
        if (retention != null) {
            if (!retention.matches(reference)) {
                throw new IllegalArgumentException("Retention receipt belongs to another deletion");
            }
            custom.put(ProfileDeleteRetention.KEY, retention.encode());
        }
        return recoveryMetadataWrite(expected, LedgerMetadataBuilder.from(value).withCToken(value.getCToken())
                .withCustomMetadata(custom).build(), true);
    }

    long deleteTombstoneCommit(MembershipSnapshot expected, long deadline)
            throws InterruptedException, TimeoutException, IOException {
        String name = ((AbstractZkLedgerManager) manager).getLedgerPath(expected.metadata().getValue().getLedgerId());
        var node = mutationReads.get(name, deadline);
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Delete retention metadata unavailable");
        }
        if (node.stat().getEphemeralOwner() != 0 || node.stat().getCzxid() != expected.birthZxid()
                || node.stat().getVersion() != ((LongVersion) expected.metadata().getVersion()).getLongVersion()
                || !sameMetadata(expected.metadata().getValue(), new LedgerMetadataSerDe().parseConfig(node.bytes(),
                        expected.metadata().getValue().getLedgerId(), Optional.of(node.stat().getCtime())))) {
            throw new IllegalArgumentException("Delete retention metadata changed or was recreated");
        }
        return node.stat().getMzxid();
    }

    Op closedMetadataWrite(MembershipSnapshot expected, ProfileRecoveryBoundary boundary) throws IOException {
        var value = expected.metadata().getValue();
        if (value.getState() != LedgerMetadata.State.IN_RECOVERY || expected.birthZxid()
                != boundary.context().metadataBirth() || !Arrays.equals(value.getCustomMetadata()
                    .get(ProfileRecoveryContext.KEY), boundary.context().encode())) {
            throw new IllegalArgumentException("Closed metadata requires its exact recovery instance");
        }
        if (!(expected.metadata().getVersion() instanceof LongVersion version)) {
            throw new IllegalArgumentException("Closed publication requires a concrete metadata version");
        }
        var custom = new java.util.TreeMap<>(value.getCustomMetadata());
        custom.put(ProfileRecoveryBoundary.CLOSE_KEY, new ProfileRecoveryBoundary.CloseReference(
                Math.toIntExact(version.getLongVersion()), ProfileRepairCommit.head(custom.get(ProfileRepairCommit.KEY))
                    .size(), expected.birthZxid(), boundary.prefixEntryId(), boundary.prefixLength(),
                ProfileRepairIntent.digest(boundary.context().encode())).encode());
        return recoveryMetadataWrite(expected, LedgerMetadataBuilder.from(value).withCToken(value.getCToken())
                .withCustomMetadata(custom).withClosedState().withLastEntryId(boundary.prefixEntryId())
                .withLength(boundary.prefixLength()).build(), true);
    }

    private Op recoveryMetadataWrite(MembershipSnapshot expected, LedgerMetadata desired, boolean closed)
            throws IOException {
        if (!(expected.metadata().getVersion() instanceof LongVersion version)
                || version.getLongVersion() < 0 || version.getLongVersion() >= Integer.MAX_VALUE
                || desired.getLedgerId() != expected.metadata().getValue().getLedgerId()
                || desired.getState() != (closed ? LedgerMetadata.State.CLOSED : LedgerMetadata.State.IN_RECOVERY)
                || desired.getCToken() != expected.metadata().getValue().getCToken()) {
            throw new IllegalArgumentException("Invalid guarded repair metadata write");
        }
        byte[] encoded = new LedgerMetadataSerDe().serialize(desired);
        if (encoded.length > 1024 * 1024) {
            throw new IllegalArgumentException("Repair metadata exceeds its fixed byte bound");
        }
        return Op.setData(((AbstractZkLedgerManager) manager).getLedgerPath(desired.getLedgerId()), encoded,
                Math.toIntExact(version.getLongVersion()));
    }

    private static boolean mutableMembership(LedgerMetadata metadata) {
        return metadata.getState() == LedgerMetadata.State.OPEN
                && !metadata.getCustomMetadata().containsKey(MEMBERSHIP_FROZEN_KEY)
                && !metadata.getCustomMetadata().containsKey(PENDING_PUBLICATION_KEY)
                && metadata.getCustomMetadata().keySet().stream().noneMatch(key ->
                        key.startsWith("bookkeeper.experimental.profile.") && !key.equals(ProfileMetadataBacklink.KEY));
    }

    /** null means the exact predecessor is still current; callers may attempt one write or keep UNKNOWN. */
    private static Result resolveReplacement(LedgerMetadata base, LedgerMetadata desired,
            Version expected, Versioned<LedgerMetadata> current, Outcome success) {
        try {
            LedgerMetadata actual = current.getValue();
            if (!(current.getVersion() instanceof LongVersion version) || !mutableMembership(actual)
                    || actual.getCToken() != base.getCToken()
                    || !ProfileMetadataBacklink.from(base).equals(ProfileMetadataBacklink.from(actual))) {
                return new Result(Outcome.CONFLICT, null);
            }
            long previous = ((LongVersion) expected).getLongVersion();
            if (version.getLongVersion() == previous + 1 && sameMetadata(desired, actual)) {
                return new Result(success, current);
            }
            return version.getLongVersion() == previous && sameMetadata(base, actual)
                    ? null : new Result(Outcome.CONFLICT, null);
        } catch (IllegalArgumentException invalid) {
            return new Result(Outcome.QUARANTINED, null);
        }
    }

    static boolean sameMetadata(LedgerMetadata desired, LedgerMetadata actual) {
        return actual.getLedgerId() == desired.getLedgerId()
                && actual.getCToken() == desired.getCToken()
                && actual.getMetadataFormatVersion() == desired.getMetadataFormatVersion()
                && actual.getState() == desired.getState()
                && actual.getLastEntryId() == desired.getLastEntryId() && actual.getLength() == desired.getLength()
                && actual.getEnsembleSize() == desired.getEnsembleSize()
                && actual.getWriteQuorumSize() == desired.getWriteQuorumSize()
                && actual.getAckQuorumSize() == desired.getAckQuorumSize()
                && actual.hasPassword() == desired.hasPassword() && actual.getDigestType() == desired.getDigestType()
                && Arrays.equals(actual.getPassword(), desired.getPassword())
                && LedgerMetadataUtils.shouldStoreCtime(actual) == LedgerMetadataUtils.shouldStoreCtime(desired)
                && (!LedgerMetadataUtils.shouldStoreCtime(desired) || actual.getCtime() == desired.getCtime())
                && actual.getAllEnsembles().equals(desired.getAllEnsembles())
                && actual.getCustomMetadata().keySet().equals(desired.getCustomMetadata().keySet())
                && actual.getCustomMetadata().entrySet().stream().allMatch(entry ->
                        Arrays.equals(entry.getValue(), desired.getCustomMetadata().get(entry.getKey())));
    }

    LedgerManager manager() {
        return manager;
    }

    /** Read an already-required metadata record. It never rebuilds a missing record during lifecycle recovery. */
    CompletableFuture<Result> readMatching(LedgerMetadata initial, ProfileMetadataBacklink backlink) {
        LedgerMetadata desired = snapshotInitial(initial, backlink);
        return manager.readLedgerMetadata(desired.getLedgerId()).handle((current, failed) -> {
            if (failed == null) {
                return classify(desired, backlink, current, Outcome.ALREADY_APPLIED);
            }
            return new Result(BKException.getExceptionCode(failed)
                    == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                    ? Outcome.QUARANTINED : Outcome.DURABILITY_UNKNOWN, null);
        });
    }

    static LedgerMetadata snapshotInitial(LedgerMetadata initial, ProfileMetadataBacklink backlink) {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(backlink, "backlink");
        if (initial.getMetadataFormatVersion() != 3 || initial.getState() != LedgerMetadata.State.OPEN
                || initial.getCustomMetadata().containsKey(ProfileRecoveryBoundary.WRITER_CLOSE_KEY)
                || initial.getDigestType() != DigestType.CRC32C || initial.getAllEnsembles().size() != 1
                || initial.getAllEnsembles().firstKey() != 0
                || initial.getEnsembleSize() < 1 || initial.getAckQuorumSize() < 1
                || new HashSet<>(initial.getAllEnsembles().firstEntry().getValue()).size()
                        != initial.getEnsembleSize()) {
            throw new IllegalArgumentException("Invalid initial Profile metadata");
        }
        // Own the reserved reference and copies of custom values before the asynchronous create.
        return backlink.attachTo(initial);
    }

    private static Result classify(LedgerMetadata desired, ProfileMetadataBacklink backlink,
            Versioned<LedgerMetadata> current, Outcome success) {
        try {
            LedgerMetadata actual = current.getValue();
            if (!actual.getCustomMetadata().containsKey(ProfileMetadataBacklink.KEY)) {
                return new Result(Outcome.CONFLICT, null);
            }
            if (!backlink.equals(ProfileMetadataBacklink.from(actual))) {
                return new Result(Outcome.CONFLICT, null);
            }
            boolean equal = actual.getMetadataFormatVersion() == desired.getMetadataFormatVersion()
                    && actual.getState() == LedgerMetadata.State.OPEN
                    && actual.getEnsembleSize() == desired.getEnsembleSize()
                    && actual.getWriteQuorumSize() == desired.getWriteQuorumSize()
                    && actual.getAckQuorumSize() == desired.getAckQuorumSize()
                    && actual.hasPassword() == desired.hasPassword()
                    && actual.getDigestType() == desired.getDigestType()
                    && Arrays.equals(actual.getPassword(), desired.getPassword())
                    && LedgerMetadataUtils.shouldStoreCtime(actual) == LedgerMetadataUtils.shouldStoreCtime(desired)
                    && (!LedgerMetadataUtils.shouldStoreCtime(desired) || actual.getCtime() == desired.getCtime())
                    && actual.getAllEnsembles().equals(desired.getAllEnsembles())
                    && actual.getCustomMetadata().keySet().equals(desired.getCustomMetadata().keySet())
                    && actual.getCustomMetadata().entrySet().stream().allMatch(entry ->
                            Arrays.equals(entry.getValue(), desired.getCustomMetadata().get(entry.getKey())));
            return equal ? new Result(success, current) : new Result(Outcome.CONFLICT, null);
        } catch (IllegalArgumentException invalid) {
            return new Result(Outcome.QUARANTINED, null);
        }
    }
}

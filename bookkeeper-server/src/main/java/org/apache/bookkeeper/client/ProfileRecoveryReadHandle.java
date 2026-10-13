/*
 *
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
 *
 */
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileInstallReceipt;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryContext;
import org.apache.bookkeeper.discover.profile.ProfileRepairCommit;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.versioning.LongVersion;

/** Borrowed readonly recovery session. The coordinator must revalidate its durable context around each attempt. */
public final class ProfileRecoveryReadHandle extends LedgerHandle {
    public enum ProbeOutcome { CANDIDATE, ABSENT }

    public static final class Probe implements AutoCloseable {
        private final ProbeOutcome outcome;
        private final long entryId;
        private final long length;
        private final byte[] bytes;
        private final Object owner;
        private ByteBuf backing;

        private Probe(ProbeOutcome outcome, long entryId, long length, byte[] bytes, Object owner) {
            this.outcome = outcome;
            this.entryId = entryId;
            this.length = length;
            this.bytes = bytes;
            this.owner = owner;
        }

        private Probe(long entryId, long length, ByteBuf payload, Object owner) {
            outcome = ProbeOutcome.CANDIDATE;
            this.entryId = entryId;
            this.length = length;
            bytes = null;
            this.owner = owner;
            backing = payload.asReadOnly().retain();
        }

        public ProbeOutcome outcome() {
            return outcome;
        }
        public long entryId() {
            return entryId;
        }
        public long length() {
            return length;
        }
        public synchronized ByteBuffer payload() {
            if (bytes != null) {
                return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
            }
            if (backing == null) {
                throw new IllegalStateException("Recovery candidate has been released");
            }
            return backing.nioBuffer(backing.readerIndex(), backing.readableBytes()).asReadOnlyBuffer();
        }

        private synchronized ByteBuf payloadBuffer() {
            if (bytes != null) {
                return Unpooled.wrappedBuffer(bytes).asReadOnly();
            }
            if (backing == null) {
                throw new IllegalStateException("Recovery candidate has been released");
            }
            return backing.retainedDuplicate();
        }

        @Override
        public synchronized void close() {
            if (backing != null) {
                backing.release();
                backing = null;
            }
        }
    }

    private final ProfileBookieClient transport;
    private final boolean fencingReads;
    private final AtomicBoolean active = new AtomicBoolean();
    private final Object probeOwner = new Object();

    public enum SourceOutcome { FOUND, MISSING, CORRUPT, UNAVAILABLE, UNAUTHORIZED }

    public record SourceEvidence(BookieId bookie, String storageIncarnation, SourceOutcome outcome) { }

    /** All exact write-set sources terminated. Absence coverage and finite exhaustion are separate facts. */
    public static final class PointEvidence implements AutoCloseable {
        private final Probe candidate;
        private final List<SourceEvidence> sources;
        private final boolean absenceCovered;
        private final boolean conflict;

        private PointEvidence(Probe candidate, List<SourceEvidence> sources, boolean absenceCovered,
                boolean conflict) {
            this.candidate = candidate;
            this.sources = List.copyOf(sources);
            this.absenceCovered = absenceCovered;
            this.conflict = conflict;
        }

        public Probe candidate() {
            return candidate;
        }
        public List<SourceEvidence> sources() {
            return sources;
        }
        public boolean absenceCovered() {
            return absenceCovered;
        }
        public boolean conflict() {
            return conflict;
        }
        public boolean exhausted() {
            return sources.stream().allMatch(source -> source.outcome() == SourceOutcome.MISSING
                    || source.outcome() == SourceOutcome.CORRUPT);
        }

        @Override
        public void close() {
            if (candidate != null) {
                candidate.close();
            }
        }
    }

    private ProfileRecoveryReadHandle(ProfileLedgerHandle.ProfileContext context,
            ZKProfileReservationStore.RecoveryResult accepted) throws GeneralSecurityException {
        this(context, accepted.context().ledger().ledgerId(), accepted.metadata().metadata(), true);
    }

    private ProfileRecoveryReadHandle(ProfileLedgerHandle.ProfileContext context, long ledger,
            org.apache.bookkeeper.versioning.Versioned<LedgerMetadata> metadata, boolean fencingReads)
            throws GeneralSecurityException {
        super(context, ledger, metadata,
                BookKeeper.DigestType.CRC32C, metadata.getValue().getPassword(),
                EnumSet.noneOf(WriteFlag.class));
        transport = context.transport();
        this.fencingReads = fencingReads;
    }

    /** Read-only OPEN prefix observer. The coordinator supplies and revalidates the actual frozen membership/root. */
    public static ProfileRecoveryReadHandle fromOpenAudit(ClientContext base,
            org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.MembershipSnapshot member,
            org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext ledger,
            List<ProfileInstallReceipt> permitted, Map<BookieId, ProfileNormalDataClient> data)
            throws GeneralSecurityException {
        requireCompatibleSdk(base);
        var value = member.metadata().getValue();
        if (value.getState() != LedgerMetadata.State.OPEN || value.getLedgerId() != ledger.ledgerId()
                || value.getDigestType() != org.apache.bookkeeper.client.api.DigestType.CRC32C
                || value.getEnsembleSize() != 3 || value.getWriteQuorumSize() != 3
                || value.getAckQuorumSize() < 2 || value.getAckQuorumSize() > 3
                || value.getAllEnsembles().size() > 32 || data.isEmpty() || data.size() > 64
                || !Arrays.equals(org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink.from(value)
                        .instanceId(),
                        ledger.ledgerInstanceId())) {
            throw new IllegalArgumentException("OPEN audit requires matching original membership/instance and peers");
        }
        for (var entry : data.entrySet()) {
            String name = entry.getKey().toString();
            var hello = entry.getValue().hello();
            if (hello.activeEngine() != 3 || !hello.bookieId().equals(name)
                    || permitted.stream().noneMatch(receipt -> receipt.scope().targetBookie().equals(name)
                        && java.util.Arrays.equals(receipt.scope().targetStorageIncarnation(),
                                hello.storageIncarnation())
                        && receipt.scope().ledgerId() == ledger.ledgerId()
                        && java.util.Arrays.equals(receipt.scope().ledgerInstanceId(), ledger.ledgerInstanceId())
                        && java.util.Arrays.equals(receipt.scope().descriptorIdentity().toBytes(),
                                ledger.descriptorIdentity()))) {
                throw new IllegalArgumentException("OPEN audit DATA peer is not a retained source incarnation");
            }
        }
        var credential = ProtectedProfileCredential.of(1, DigestManager.generateMasterKey(value.getPassword()));
        var transport = new ProfileBookieClient(base, ledger, credential, data, 1,
                new ProfileBookieClient.Retry(1, java.time.Duration.ofSeconds(5), java.time.Duration.ofMillis(1)));
        return new ProfileRecoveryReadHandle(new ProfileLedgerHandle.ProfileContext(base, transport),
                ledger.ledgerId(), member.metadata(), false);
    }

    public static void requireCompatibleSdk(ClientContext base) {
        if (base.getConf().useV2WireProtocol || base.getConf().explicitLacInterval != 0) {
            throw new IllegalArgumentException("Profile recovery requires the enabled SDK wire scope");
        }
    }

    /** Supplied receipts are the retained published sources, with current targets overriding old incarnations. */
    public static ProfileRecoveryReadHandle fromContext(ClientContext base,
            ZKProfileReservationStore.RecoveryResult accepted, List<ProfileInstallReceipt> permitted,
            Map<BookieId, ProfileNormalDataClient> data, Map<BookieId, ProfileInitialControlClient> controls)
            throws GeneralSecurityException {
        return build(base, accepted, permitted, data, controls, false);
    }

    public static ProfileRecoveryReadHandle fromClosedRepairContext(ClientContext base,
            ZKProfileReservationStore.RecoveryResult accepted, List<ProfileInstallReceipt> permitted,
            Map<BookieId, ProfileNormalDataClient> data, Map<BookieId, ProfileInitialControlClient> controls)
            throws GeneralSecurityException {
        return build(base, accepted, permitted, data, controls, true);
    }

    private static ProfileRecoveryReadHandle build(ClientContext base,
            ZKProfileReservationStore.RecoveryResult accepted,
            List<ProfileInstallReceipt> permitted, Map<BookieId, ProfileNormalDataClient> data,
            Map<BookieId, ProfileInitialControlClient> controls, boolean closedRepair) throws GeneralSecurityException {
        if (accepted.context() == null || data.isEmpty() || data.size() > 64 || controls.size() > 64
                || base.getConf().useV2WireProtocol || base.getConf().explicitLacInterval != 0) {
            throw new IllegalArgumentException("Readonly recovery requires the frozen context and bounded V3 peers");
        }
        var context = accepted.context();
        var metadata = accepted.metadata().metadata();
        var value = metadata.getValue();
        if (value.getState() != (closedRepair ? LedgerMetadata.State.CLOSED : LedgerMetadata.State.IN_RECOVERY)
                || !(metadata.getVersion() instanceof LongVersion version)
                || version.getLongVersion() != context.generation()
                    + ProfileRepairCommit.head(
                            value.getCustomMetadata().get(ProfileRepairCommit.KEY))
                        .size()
                    + org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.versionOffset(value)
                    + (closedRepair ? 1 : 0)
                || accepted.metadata().birthZxid() != context.metadataBirth()
                || !context.equals(ProfileRecoveryContext.decode(
                        value.getCustomMetadata().get(ProfileRecoveryContext.KEY)))
                || value.getEnsembleSize() > 32 || value.getAllEnsembles().size() > 32) {
            throw new IllegalArgumentException("Recovery metadata does not carry the exact accepted context");
        }
        if (closedRepair) {
            var cut = org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CloseReference.decode(
                    value.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CLOSE_KEY));
            if (cut.metadataBirth() != context.metadataBirth() || cut.prefixEntryId() != value.getLastEntryId()
                    || cut.prefixLength() != value.getLength() || !cut.matchesContext(context)) {
                throw new IllegalArgumentException("Closed copy requires its unchanged boundary/context cut");
            }
        }
        for (var entry : data.entrySet()) {
            var peer = entry.getValue();
            String name = entry.getKey().toString();
            if (peer.hello().activeEngine() != 3 || !peer.hello().bookieId().equals(name)
                    || permitted.stream().noneMatch(receipt -> receipt.scope().targetBookie().equals(name)
                        && Arrays.equals(receipt.scope().targetStorageIncarnation(), peer.hello().storageIncarnation())
                        && receipt.scope().ledgerId() == context.ledger().ledgerId()
                        && Arrays.equals(receipt.scope().ledgerInstanceId(), context.ledger().ledgerInstanceId())
                        && Arrays.equals(receipt.scope().descriptorIdentity().toBytes(),
                                context.ledger().descriptorIdentity()))) {
                throw new IllegalArgumentException("Recovery DATA peer is not a permitted source incarnation");
            }
        }
        var credential = ProtectedProfileCredential.of(1, DigestManager.generateMasterKey(value.getPassword()));
        var transport = new ProfileBookieClient(base, context.ledger(), credential, data, 1,
                new ProfileBookieClient.Retry(1, Duration.ofSeconds(5), Duration.ofMillis(1)), context, controls,
                closedRepair ? value.getLastEntryId() : Long.MAX_VALUE);
        return new ProfileRecoveryReadHandle(new ProfileLedgerHandle.ProfileContext(base, transport), accepted);
    }

    public CompletableFuture<ProfileLedgerHandle.LacEvidence> readLacEvidence() {
        if (!active.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException));
        }
        return new ProfileLacEvidenceRead(this, transport).start()
                .whenComplete((result, failure) -> active.set(false)).copy();
    }

    /** Original recovery read aggregation; every missing source is fenced with this instance before its point read. */
    public CompletableFuture<Probe> probe(long entryId) {
        return probe(entryId, false);
    }

    /** Original quorum probe with an owned candidate. Caller closes it; payload views borrow its backing. */
    public CompletableFuture<Probe> probeBuffer(long entryId) {
        return probe(entryId, true);
    }

    private CompletableFuture<Probe> probe(long entryId, boolean owned) {
        if (!fencingReads) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.IllegalOpException));
        }
        if (entryId < 0 || !active.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(entryId < 0 ? BKException.Code.IllegalOpException
                    : BKException.Code.TooManyRequestsException));
        }
        CompletableFuture<Probe> actual = new CompletableFuture<>();
        try {
            executeOrdered(() -> {
                try {
                    new ListenerBasedPendingReadOp(this, clientCtx, entryId, entryId,
                    (code, handle, entry, ignored) -> {
                        if (code == BKException.Code.OK) {
                            if (owned) {
                                ByteBuf payload = entry.getEntryBuffer();
                                try {
                                    Probe candidate = new Probe(entryId, entry.getLength(), payload, probeOwner);
                                    if (!actual.complete(candidate)) {
                                        candidate.close();
                                    }
                                } catch (RuntimeException failure) {
                                    actual.completeExceptionally(failure);
                                } finally {
                                    payload.release();
                                }
                            } else {
                                actual.complete(new Probe(ProbeOutcome.CANDIDATE,
                                        entryId, entry.getLength(), entry.getEntry(), probeOwner));
                            }
                        } else if (code == BKException.Code.NoSuchEntryException) {
                            actual.complete(new Probe(ProbeOutcome.ABSENT, entryId, 0, new byte[0], probeOwner));
                        } else {
                            actual.completeExceptionally(BKException.create(code));
                        }
                    }, null, true).initiate();
                } catch (RuntimeException failure) {
                    actual.completeExceptionally(failure);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            actual.completeExceptionally(BKException.create(BKException.Code.InterruptedException));
        }
        if (!owned) {
            return actual.whenComplete((result, failure) -> active.set(false)).copy();
        }
        var caller = new CompletableFuture<Probe>();
        actual.whenComplete((value, failure) -> {
            active.set(false);
            if (failure != null) {
                caller.completeExceptionally(failure);
            } else if (!caller.complete(value)) {
                value.close();
            }
        });
        return caller;
    }

    /** Required-hole verification cannot stop at the normal-tail missing threshold or the first valid payload. */
    public CompletableFuture<PointEvidence> probeEvidence(long entryId) {
        return probeEvidence(entryId, false);
    }

    /** Caller closes the evidence; candidate payload views borrow its original accounted source backing. */
    public CompletableFuture<PointEvidence> probeBufferEvidence(long entryId) {
        return probeEvidence(entryId, true);
    }

    private CompletableFuture<PointEvidence> probeEvidence(long entryId, boolean owned) {
        if (entryId < 0 || !active.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(entryId < 0 ? BKException.Code.IllegalOpException
                    : BKException.Code.TooManyRequestsException));
        }
        CompletableFuture<PointEvidence> actual = new CompletableFuture<>();
        try {
            executeOrdered(() -> {
                try {
                    new EvidenceRead(entryId, actual, owned).start();
                } catch (RuntimeException failure) {
                    actual.completeExceptionally(failure);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            actual.completeExceptionally(BKException.create(BKException.Code.InterruptedException));
        }
        if (!owned) {
            return actual.whenComplete((value, failure) -> active.set(false)).copy();
        }
        var caller = new CompletableFuture<PointEvidence>();
        actual.whenComplete((value, failure) -> {
            active.set(false);
            if (failure != null) {
                caller.completeExceptionally(failure);
            } else if (!caller.complete(value)) {
                value.close();
            }
        });
        return caller;
    }

    private final class EvidenceRead {
        private final long entryId;
        private final CompletableFuture<PointEvidence> done;
        private final org.apache.bookkeeper.versioning.Versioned<LedgerMetadata> expected;
        private final Map<BookieId, ProfileNormalDataClient> peers;
        private final List<SourceEvidence> sources = new ArrayList<>();
        private final DistributionSchedule.AckSet absent;
        private final boolean owned;
        private Probe candidate;
        private boolean covered;
        private boolean conflict;
        private Throwable failure;
        private int remaining;

        EvidenceRead(long entryId, CompletableFuture<PointEvidence> done, boolean owned) {
            this.entryId = entryId;
            this.done = done;
            this.owned = owned;
            expected = getVersionedLedgerMetadata();
            peers = transport.readPeers();
            var value = expected.getValue();
            absent = new RoundRobinDistributionSchedule(value.getWriteQuorumSize(),
                    value.getWriteQuorumSize() - value.getAckQuorumSize() + 1, value.getEnsembleSize()).getAckSet();
        }

        void start() {
            var ensemble = expected.getValue().getAllEnsembles().floorEntry(entryId).getValue();
            var writeSet = distributionSchedule.getWriteSet(entryId);
            remaining = writeSet.size();
            for (int i = 0; i < remaining; i++) {
                sources.add(null);
            }
            try {
                for (int i = 0; i < writeSet.size(); i++) {
                    int slot = i;
                    int member = writeSet.get(i);
                    BookieId bookie = ensemble.get(member);
                    int flags = fencingReads ? org.apache.bookkeeper.proto.BookieProtocol.FLAG_HIGH_PRIORITY
                            | org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING : 0;
                    try {
                        transport.readEntry(bookie, getId(), entryId,
                                (code, ledger, entry, bytes, ignored) -> receive(slot, member, bookie, code, bytes),
                                null, flags, getLedgerKey(), false);
                    } catch (RuntimeException rejected) {
                        synchronized (this) {
                            failure = rejected;
                            receive(slot, member, bookie, BKException.Code.ReadException, null);
                        }
                    }
                }
            } finally {
                writeSet.recycle();
            }
        }

        private synchronized void receive(int slot, int member, BookieId bookie, int code,
                io.netty.buffer.ByteBuf raw) {
            if (sources.get(slot) != null) {
                return;
            }
            SourceOutcome outcome = SourceOutcome.UNAVAILABLE;
            var peer = peers.get(bookie);
            if (code == BKException.Code.OK && peer != null) {
                try {
                    var payload = getDigestManager().verifyDigestAndReturnData(entryId, raw);
                    long length = raw.getLong(24);
                    if (candidate == null) {
                        if (owned) {
                            candidate = new Probe(entryId, length, payload, probeOwner);
                        } else {
                            byte[] bytes = new byte[payload.readableBytes()];
                            payload.getBytes(payload.readerIndex(), bytes);
                            candidate = new Probe(ProbeOutcome.CANDIDATE, entryId, length, bytes, probeOwner);
                        }
                    } else if (candidate.length() != length || !candidate.payload().equals(
                            payload.nioBuffer(payload.readerIndex(), payload.readableBytes()))) {
                        conflict = true;
                    }
                    outcome = SourceOutcome.FOUND;
                } catch (BKException.BKDigestMatchException invalid) {
                    outcome = SourceOutcome.CORRUPT;
                } catch (RuntimeException invalid) {
                    failure = invalid;
                }
            } else if (code == BKException.Code.NoSuchEntryException && peer != null) {
                outcome = SourceOutcome.MISSING;
                covered |= absent.completeBookieAndCheck(member);
            } else if (code == BKException.Code.UnauthorizedAccessException) {
                outcome = SourceOutcome.UNAUTHORIZED;
            }
            sources.set(slot, new SourceEvidence(bookie, peer == null ? ""
                    : java.util.HexFormat.of().formatHex(peer.hello().storageIncarnation()), outcome));
            if (--remaining == 0) {
                absent.recycle();
                if (failure != null) {
                    closeCandidate();
                    done.completeExceptionally(failure);
                } else if (transport.isClosed() || getVersionedLedgerMetadata() != expected
                        || transport.readPeers() != peers) {
                    closeCandidate();
                    done.completeExceptionally(BKException.create(BKException.Code.ReadException));
                } else {
                    done.complete(new PointEvidence(candidate, sources, covered, conflict));
                }
            }
        }

        private void closeCandidate() {
            if (candidate != null) {
                candidate.close();
            }
        }
    }

    /** Original SDK recovery packing of this handle's validated probe. Serialization performs no Add or activation. */
    public io.netty.util.ReferenceCounted packageRecoveryEntry(Probe probe) {
        return packageRecoveryEntry(probe, null);
    }

    /** Uses the target DATA allocator for the standard header; the retained source payload keeps its own credit. */
    public io.netty.util.ReferenceCounted packageRecoveryEntry(Probe probe, ByteBufAllocator headerAllocator) {
        if (!fencingReads || probe.owner != probeOwner || probe.outcome() != ProbeOutcome.CANDIDATE) {
            throw new IllegalArgumentException("Recovery packing requires this handle's candidate");
        }
        ByteBuf payload = probe.payloadBuffer();
        try {
            if (headerAllocator != null) {
                return getDigestManager().computeDigestAndPackageForSendingV3(probe.entryId(), getLastAddConfirmed(),
                        probe.length(), payload, headerAllocator);
            }
            return getDigestManager().computeDigestAndPackageForSending(probe.entryId(), getLastAddConfirmed(),
                    probe.length(), payload, getLedgerKey(),
                    org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD);
        } catch (RuntimeException | Error failure) {
            payload.release();
            throw failure;
        }
    }

    @Override
    boolean isHandleWritable() {
        return false;
    }

    @Override
    void asyncCloseInternal(org.apache.bookkeeper.client.AsyncCallback.CloseCallback callback, Object ctx, int code) {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        transport.close();
        callback.closeComplete(BKException.Code.OK, this, ctx);
    }
}

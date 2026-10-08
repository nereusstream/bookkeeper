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

    public static final class Probe {
        private final ProbeOutcome outcome;
        private final long entryId;
        private final long length;
        private final byte[] bytes;
        private final Object owner;

        private Probe(ProbeOutcome outcome, long entryId, long length, byte[] bytes, Object owner) {
            this.outcome = outcome;
            this.entryId = entryId;
            this.length = length;
            this.bytes = bytes;
            this.owner = owner;
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
        public ByteBuffer payload() {
            return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
        }
    }

    private final ProfileBookieClient transport;
    private final ProfileRecoveryContext recovery;
    private final AtomicBoolean active = new AtomicBoolean();
    private final Object probeOwner = new Object();

    public enum SourceOutcome { FOUND, MISSING, CORRUPT, UNAVAILABLE, UNAUTHORIZED }

    public record SourceEvidence(BookieId bookie, String storageIncarnation, SourceOutcome outcome) { }

    /** All exact write-set sources terminated. Absence coverage and finite exhaustion are separate facts. */
    public static final class PointEvidence {
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
    }

    private ProfileRecoveryReadHandle(ProfileLedgerHandle.ProfileContext context,
            ZKProfileReservationStore.RecoveryResult accepted) throws GeneralSecurityException {
        super(context, accepted.context().ledger().ledgerId(), accepted.metadata().metadata(),
                BookKeeper.DigestType.CRC32C, accepted.metadata().metadata().getValue().getPassword(),
                EnumSet.noneOf(WriteFlag.class));
        transport = context.transport();
        recovery = accepted.context();
    }

    /** Supplied receipts are the retained published sources, with current targets overriding old incarnations. */
    public static ProfileRecoveryReadHandle fromContext(ClientContext base,
            ZKProfileReservationStore.RecoveryResult accepted, List<ProfileInstallReceipt> permitted,
            Map<BookieId, ProfileNormalDataClient> data, Map<BookieId, ProfileInitialControlClient> controls)
            throws GeneralSecurityException {
        if (accepted.context() == null || data.isEmpty() || data.size() > 64 || controls.size() > 64
                || base.getConf().useV2WireProtocol || base.getConf().explicitLacInterval != 0) {
            throw new IllegalArgumentException("Readonly recovery requires the frozen context and bounded V3 peers");
        }
        var context = accepted.context();
        var metadata = accepted.metadata().metadata();
        var value = metadata.getValue();
        if (value.getState() != LedgerMetadata.State.IN_RECOVERY
                || !(metadata.getVersion() instanceof LongVersion version)
                || version.getLongVersion() != context.generation()
                    + ProfileRepairCommit.head(
                            value.getCustomMetadata().get(ProfileRepairCommit.KEY))
                        .size()
                || accepted.metadata().birthZxid() != context.metadataBirth()
                || !context.equals(ProfileRecoveryContext.decode(
                        value.getCustomMetadata().get(ProfileRecoveryContext.KEY)))
                || value.getEnsembleSize() > 32 || value.getAllEnsembles().size() > 32) {
            throw new IllegalArgumentException("Recovery metadata does not carry the exact accepted context");
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
                new ProfileBookieClient.Retry(1, Duration.ofSeconds(5), Duration.ofMillis(1)), context, controls);
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
                            actual.complete(new Probe(ProbeOutcome.CANDIDATE,
                                    entryId, entry.getLength(), entry.getEntry(), probeOwner));
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
        return actual.whenComplete((result, failure) -> active.set(false)).copy();
    }

    /** Required-hole verification cannot stop at the normal-tail missing threshold or the first valid payload. */
    public CompletableFuture<PointEvidence> probeEvidence(long entryId) {
        if (entryId < 0 || !active.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(entryId < 0 ? BKException.Code.IllegalOpException
                    : BKException.Code.TooManyRequestsException));
        }
        CompletableFuture<PointEvidence> actual = new CompletableFuture<>();
        try {
            executeOrdered(() -> {
                try {
                    new EvidenceRead(entryId, actual).start();
                } catch (RuntimeException failure) {
                    actual.completeExceptionally(failure);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            actual.completeExceptionally(BKException.create(BKException.Code.InterruptedException));
        }
        return actual.whenComplete((value, failure) -> active.set(false)).copy();
    }

    private final class EvidenceRead {
        private final long entryId;
        private final CompletableFuture<PointEvidence> done;
        private final org.apache.bookkeeper.versioning.Versioned<LedgerMetadata> expected;
        private final Map<BookieId, ProfileNormalDataClient> peers;
        private final List<SourceEvidence> sources = new ArrayList<>();
        private final DistributionSchedule.AckSet absent;
        private Probe candidate;
        private boolean covered;
        private boolean conflict;
        private Throwable failure;
        private int remaining;

        EvidenceRead(long entryId, CompletableFuture<PointEvidence> done) {
            this.entryId = entryId;
            this.done = done;
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
                    int flags = org.apache.bookkeeper.proto.BookieProtocol.FLAG_HIGH_PRIORITY
                            | org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING;
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
                        byte[] bytes = new byte[payload.readableBytes()];
                        payload.getBytes(payload.readerIndex(), bytes);
                        candidate = new Probe(ProbeOutcome.CANDIDATE, entryId, length, bytes, probeOwner);
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
                    done.completeExceptionally(failure);
                } else if (transport.isClosed() || getVersionedLedgerMetadata() != expected
                        || transport.readPeers() != peers) {
                    done.completeExceptionally(BKException.create(BKException.Code.ReadException));
                } else {
                    done.complete(new PointEvidence(candidate, sources, covered, conflict));
                }
            }
        }
    }

    /** Original SDK recovery packing of this handle's validated probe. Serialization performs no Add or activation. */
    public io.netty.util.ReferenceCounted packageRecoveryEntry(Probe probe) {
        if (probe.owner != probeOwner || probe.outcome() != ProbeOutcome.CANDIDATE) {
            throw new IllegalArgumentException("Recovery packing requires this handle's candidate");
        }
        return getDigestManager().computeDigestAndPackageForSending(probe.entryId(), getLastAddConfirmed(),
                probe.length(), io.netty.buffer.Unpooled.wrappedBuffer(probe.bytes).asReadOnly(), getLedgerKey(),
                org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD);
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

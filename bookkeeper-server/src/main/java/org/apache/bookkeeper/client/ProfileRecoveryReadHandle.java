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

        private Probe(ProbeOutcome outcome, long entryId, long length, byte[] bytes) {
            this.outcome = outcome;
            this.entryId = entryId;
            this.length = length;
            this.bytes = bytes;
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
                                    entryId, entry.getLength(), entry.getEntry()));
                        } else if (code == BKException.Code.NoSuchEntryException) {
                            actual.complete(new Probe(ProbeOutcome.ABSENT, entryId, 0, new byte[0]));
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

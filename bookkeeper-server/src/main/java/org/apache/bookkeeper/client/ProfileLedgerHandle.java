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

import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.AsyncCallback.AddCallbackWithLatency;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileReplacementCoordinator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Experimental Profile writer, using the original LedgerHandle/PendingAddOp write engine.
 * Borrows the SDK context and verified clients; replacement requires an explicitly configured bounded pool.
 * Ordinary point reads and explicit source/anchor evidence are opt-in; recovery/close publication stay disabled.
 * Local stop is never a metadata CLOSED receipt or cancellation of remote I/O.
 */
public final class ProfileLedgerHandle extends LedgerHandle {
    private final ProfileBookieClient transport;
    private final Semaphore entries;
    private final int maxPayloadBytes;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean lacInFlight = new AtomicBoolean();
    private ProfileReplacementCoordinator replacement;
    private List<ProfileReplacementCoordinator.Target> candidates = List.of();
    private byte[] replacementDescriptor;
    private int replacementLimit;
    private volatile ProfileReplacementCoordinator.Operation currentReplacement;

    private ProfileLedgerHandle(ProfileContext context, ProfileInitialLifecycleCoordinator.Result initial,
            int maxPendingEntries, int maxPayloadBytes) throws GeneralSecurityException {
        super(context, initial.metadata().getValue().getLedgerId(), initial.metadata(), BookKeeper.DigestType.CRC32C,
                initial.metadata().getValue().getPassword(), EnumSet.noneOf(WriteFlag.class));
        transport = context.transport;
        entries = new Semaphore(maxPendingEntries);
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public static ProfileLedgerHandle fromInitialAvailability(ClientContext base,
            ProfileInitialLifecycleCoordinator.Result initial, Map<BookieId, ProfileNormalDataClient> clients,
            int maxPendingEntries, int maxPayloadBytes, int maxAttempts, Duration deadline, Duration backoff)
            throws GeneralSecurityException {
        if (initial.status() != ProfileInitialLifecycleCoordinator.Status.AVAILABLE
                || maxPendingEntries < 1 || maxPendingEntries > 256 || maxPayloadBytes < 1
                || maxPayloadBytes > 1024 * 1024 || base.getConf().useV2WireProtocol
                || !base.getConf().delayEnsembleChange || base.getConf().explicitLacInterval != 0
                || base.getConf().enforceMinNumFaultDomainsForWrite) {
            throw new IllegalArgumentException("Profile writer requires verified initial availability "
                    + "and enabled scope");
        }
        LedgerMetadata metadata = initial.metadata().getValue();
        var page = initial.ready().page();
        List<BookieId> ensemble = metadata.getAllEnsembles().firstEntry().getValue();
        if (metadata.getState() != LedgerMetadata.State.OPEN || metadata.getDigestType() != DigestType.CRC32C
                || !clients.keySet().equals(new HashSet<>(ensemble))
                || !page.matchesMetadata(initial.metadata())
                || page.installs().size() != initial.activations().size()) {
            throw new IllegalArgumentException("Initial Profile writer metadata/targets do not match");
        }
        for (var installed : page.installs()) {
            var activation = initial.activations().stream()
                    .filter(value -> value.scope().targetBookie().equals(installed.scope().targetBookie()))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("Missing initial activation"));
            if (!activation.scope().equals(page.activation(installed.scope().targetBookie()))
                    || activation.localGeneration() <= installed.localInstallGeneration()) {
                throw new IllegalArgumentException("Initial activation is not a matching current completion");
            }
            ProfileNormalDataClient client = clients.get(BookieId.parse(installed.scope().targetBookie()));
            if (client.hello().activeEngine() != 3
                    || !client.hello().bookieId().equals(installed.scope().targetBookie())
                    || !Arrays.equals(client.hello().storageIncarnation(),
                        installed.scope().targetStorageIncarnation())) {
                throw new IllegalArgumentException("DATA peer does not match the initial target incarnation");
            }
        }
        var scope = page.installs().get(0).scope();
        var ledger = new ProfileLedgerContext(scope.ledgerId(), scope.ledgerInstanceId(),
                scope.descriptorIdentity().toBytes());
        var credential = ProtectedProfileCredential.of(1,
                DigestManager.generateMasterKey(metadata.getPassword()));
        var transport = new ProfileBookieClient(base, ledger, credential, clients, maxPendingEntries,
                new ProfileBookieClient.Retry(maxAttempts, deadline, backoff));
        return new ProfileLedgerHandle(new ProfileContext(base, transport), initial,
                maxPendingEntries, maxPayloadBytes);
    }

    /** Configure a bounded verified pool before issuing entries; no registration/connection is synthesized. */
    public CompletableFuture<Void> enableReplacement(ProfileReplacementCoordinator coordinator,
            List<ProfileReplacementCoordinator.Target> targets, byte[] descriptor, int maxChanges) {
        List<ProfileReplacementCoordinator.Target> pool = List.copyOf(targets);
        if (descriptor.length > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Replacement descriptor exceeds the canonical bound");
        }
        byte[] ownedDescriptor = descriptor.clone();
        if (pool.isEmpty() || pool.size() > 32 || maxChanges < 1 || maxChanges > 32
                || pool.stream().map(ProfileReplacementCoordinator.Target::bookie).distinct().count() != pool.size()) {
            throw new IllegalArgumentException("Invalid bounded Profile replacement pool");
        }
        CompletableFuture<Void> configured = new CompletableFuture<>();
        executeOrdered(() -> {
            synchronized (this) {
                if (lastAddPushed != INVALID_ENTRY_ID || stopped.get() || replacement != null) {
                    configured.completeExceptionally(new IllegalStateException("Configure before first append"));
                    return;
                }
                replacement = Objects.requireNonNull(coordinator, "coordinator");
                candidates = pool;
                replacementDescriptor = ownedDescriptor;
                replacementLimit = maxChanges;
            }
            configured.complete(null);
        });
        return configured;
    }

    /** An unresolved control operation can be explicitly reconciled; this is not permission to reopen a writer. */
    public ProfileReplacementCoordinator.Operation unresolvedReplacement() {
        return currentReplacement;
    }

    /** Logical failure can still leave remote DATA; a later NONE never clears this observation. */
    public boolean hasUncertainWrites() {
        return transport.hasUncertainWrites();
    }


    /** A verified source sample; sourceLength belongs to sourceEntryId, not to its piggyback LAC. */
    public record LacSource(BookieId bookie, String storageIncarnation, long sourceEntryId,
                            long piggybackLac, long sourceLength, int payloadBytes) { }

    /** Snapshot evidence only: no writer LAC/length mutation, fencing coverage or CLOSED authority. */
    public record ExplicitLacSource(BookieId bookie, String storageIncarnation, long lac) { }

    public record LacEvidence(Versioned<LedgerMetadata> metadata, long maxObservedLac, long prefixEntryId,
                              long prefixLength, List<LacSource> sources, List<ExplicitLacSource> explicitSources) {
        public LacEvidence(Versioned<LedgerMetadata> metadata, long maxObservedLac, long prefixEntryId,
                long prefixLength, List<LacSource> sources) {
            this(metadata, maxObservedLac, prefixEntryId, prefixLength, sources, List.of());
        }
        public LacEvidence {
            sources = List.copyOf(sources);
            explicitSources = List.copyOf(explicitSources);
        }
    }

    /** One bounded query per handle; cancellation stops caller waiting while actual socket work drains. */
    public CompletableFuture<LacEvidence> readLacEvidence() {
        if (!lacInFlight.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException));
        }
        var actual = new ProfileLacEvidenceRead(this, transport).start();
        return actual.whenComplete((result, failure) -> lacInFlight.set(false)).copy();
    }

    /** Local resource stop only. Metadata remains OPEN and needs the proper Profile close/recovery protocol. */
    public CompletableFuture<Void> stopLocalWrites() {
        handleUnrecoverableErrorDuringAdd(BKException.Code.ClientClosedException);
        return transport.closeCompletion();
    }

    @Override
    boolean isHandleWritable() {
        return !stopped.get() && super.isHandleWritable();
    }

    @Override
    protected void doAsyncAddEntry(PendingAddOp op) {
        if (op.payload.readableBytes() > maxPayloadBytes || !entries.tryAcquire()) {
            int code = op.payload.readableBytes() > maxPayloadBytes ? BKException.Code.IllegalOpException
                    : BKException.Code.TooManyRequestsException;
            Runnable rejected = () -> {
                try {
                    op.cb.addCompleteWithLatency(code, this, INVALID_ENTRY_ID, 0, op.ctx);
                } finally {
                    op.recyclePendAddOpObject();
                }
            };
            try {
                executeOrdered(rejected);
            } catch (RejectedExecutionException shutdown) {
                rejected.run();
            }
            return;
        }
        AddCallbackWithLatency original = op.cb;
        op.cb = (code, handle, entryId, latency, ctx) -> {
            try {
                original.addCompleteWithLatency(code, handle, entryId, latency, ctx);
            } finally {
                entries.release();
            }
        };
        try {
            super.doAsyncAddEntry(op);
        } catch (RuntimeException failure) {
            handleUnrecoverableErrorDuringAdd(BKException.Code.InterruptedException);
        }
    }

    @Override
    void handleBookieFailure(Map<Integer, BookieId> failed) {
        if (replacement == null || stopped.get()) {
            handleUnrecoverableErrorDuringAdd(BKException.Code.WriteException);
            return;
        }
        synchronized (metadataLock) {
            delayedWriteFailedBookies.putAll(failed);
            if (changingEnsemble) {
                return;
            }
            changingEnsemble = true;
        }
        try {
            executeOrdered(this::replaceNext);
        } catch (RejectedExecutionException unavailable) {
            handleUnrecoverableErrorDuringAdd(BKException.Code.InterruptedException);
        }
    }

    private void replaceNext() {
        if (stopped.get()) {
            return;
        }
        Map<Integer, BookieId> failed;
        synchronized (metadataLock) {
            failed = new HashMap<>(delayedWriteFailedBookies);
            delayedWriteFailedBookies.clear();
        }
        Versioned<LedgerMetadata> expected = getVersionedLedgerMetadata();
        List<BookieId> ensemble = getCurrentEnsemble();
        failed.entrySet().removeIf(entry -> !ensemble.get(entry.getKey()).equals(entry.getValue()));
        if (failed.isEmpty()) {
            synchronized (metadataLock) {
                changingEnsemble = false;
            }
            sendAddSuccessCallbacks();
            return;
        }
        if (numEnsembleChanges.incrementAndGet() > replacementLimit) {
            handleUnrecoverableErrorDuringAdd(BKException.Code.WriteException);
            return;
        }
        ensembleChangeCounter.inc();
        Map<Integer, ProfileReplacementCoordinator.Target> selected = new HashMap<>();
        var available = candidates.stream().filter(target -> !ensemble.contains(target.bookie())).iterator();
        for (int slot : failed.keySet().stream().sorted().toList()) {
            if (!available.hasNext()) {
                handleUnrecoverableErrorDuringAdd(BKException.Code.NotEnoughBookiesException);
                return;
            }
            selected.put(slot, available.next());
        }
        long start = getLastAddConfirmed() + 1;
        var credential = ProtectedProfileCredential.of(1, ledgerKey);
        CompletableFuture<ProfileReplacementCoordinator.Operation> begun;
        try {
            begun = replacement.begin(expected, start, selected, replacementDescriptor, credential,
                    identity(), identity());
        } catch (RuntimeException refused) {
            handleUnrecoverableErrorDuringAdd(BKException.Code.WriteException);
            return;
        }
        begun.thenCompose(operation -> {
                    currentReplacement = operation;
                    return replacement.execute(operation, () -> !stopped.get());
                }).whenComplete((result, failure) -> {
                    Runnable completed = () -> {
                        if (stopped.get()) {
                            return;
                        }
                        if (failure != null || result == null
                                || result.status() != ProfileReplacementCoordinator.Status.AVAILABLE) {
                            handleUnrecoverableErrorDuringAdd(BKException.Code.WriteException);
                            return;
                        }
                        Map<BookieId, ProfileNormalDataClient> current = new HashMap<>(transport.clients());
                        failed.values().forEach(current::remove);
                        current.putAll(result.clients());
                        transport.switchClients(current);
                        if (!setLedgerMetadata(expected, result.metadata())) {
                            handleUnrecoverableErrorDuringAdd(BKException.Code.WriteException);
                            return;
                        }
                        unsetSuccessAndSendWriteRequest(getCurrentEnsemble(), selected.keySet());
                        currentReplacement = null;
                        synchronized (metadataLock) {
                            delayedWriteFailedBookies.entrySet().removeIf(entry ->
                                    !getCurrentEnsemble().get(entry.getKey()).equals(entry.getValue()));
                            if (delayedWriteFailedBookies.isEmpty()) {
                                changingEnsemble = false;
                            }
                        }
                        if (changingEnsemble) {
                            replaceNext();
                        } else {
                            sendAddSuccessCallbacks();
                        }
                    };
                    try {
                        executeOrdered(completed);
                    } catch (RejectedExecutionException unavailable) {
                        handleUnrecoverableErrorDuringAdd(BKException.Code.InterruptedException);
                    }
                });
    }

    private static byte[] identity() {
        UUID value = UUID.randomUUID();
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits())
                .array();
    }

    @Override
    void handleUnrecoverableErrorDuringAdd(int code) {
        if (stopped.compareAndSet(false, true)) {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            Runnable stop = () -> {
                try {
                    errorOutPendingAdds(code);
                } finally {
                    transport.close();
                }
            };
            try {
                executeOrdered(stop);
            } catch (RejectedExecutionException shutdown) {
                stop.run();
            }
        }
    }

    @Override
    void asyncCloseInternal(CloseCallback callback, Object ctx, int code) {
        handleUnrecoverableErrorDuringAdd(code);
        executeOrdered(() -> callback.closeComplete(BKException.Code.IllegalOpException, this, ctx));
    }

    record ProfileContext(ClientContext base, ProfileBookieClient transport) implements ClientContext {
        @Override
        public ClientInternalConf getConf() {
            return base.getConf();
        }
        @Override
        public LedgerManager getLedgerManager() {
            return base.getLedgerManager();
        }
        @Override
        public BookieWatcher getBookieWatcher() {
            return base.getBookieWatcher();
        }
        @Override
        public EnsemblePlacementPolicy getPlacementPolicy() {
            return base.getPlacementPolicy();
        }
        @Override
        public BookieClient getBookieClient() {
            return transport;
        }
        @Override
        public ByteBufAllocator getByteBufAllocator() {
            return base.getByteBufAllocator();
        }
        @Override
        public OrderedExecutor getMainWorkerPool() {
            return base.getMainWorkerPool();
        }
        @Override
        public OrderedScheduler getScheduler() {
            return base.getScheduler();
        }
        @Override
        public BookKeeperClientStats getClientStats() {
            return base.getClientStats();
        }
        @Override
        public boolean isClientClosed() {
            return base.isClientClosed() || transport.isClosed();
        }
    }
}

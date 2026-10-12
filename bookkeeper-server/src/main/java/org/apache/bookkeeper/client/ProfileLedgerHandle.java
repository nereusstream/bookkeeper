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
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileReplacementCoordinator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Experimental Profile writer, using the original LedgerHandle/PendingAddOp write engine.
 * Borrows the SDK context and verified clients; replacement requires an explicitly configured bounded pool.
 * Ordinary point reads and source evidence are opt-in; writer close borrows explicitly configured cold peers.
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
    private final ClientContext base;
    private volatile CloseSettings closeSettings;
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final CompletableFuture<Void> durableClose = new CompletableFuture<>();
    private volatile ProfileRecoveryCoordinator.WriterCloseRequest writerCloseRequest;

    private record CloseSettings(ProfileRecoveryCoordinator coordinator, byte[] descriptor,
            org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            Map<String, ProfileInitialControlClient> controls, int coordinates, Duration duration) { }

    private ProfileLedgerHandle(ProfileContext context, ProfileInitialLifecycleCoordinator.Result initial,
            int maxPendingEntries, int maxPayloadBytes) throws GeneralSecurityException {
        super(context, initial.metadata().getValue().getLedgerId(), initial.metadata(), BookKeeper.DigestType.CRC32C,
                initial.metadata().getValue().getPassword(), EnumSet.noneOf(WriteFlag.class));
        transport = context.transport;
        base = context.base;
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

    /** Cold snapshot input only; the coordinator validates live authority before publishing the handle. */
    public static LedgerHandle fromClosedRead(ClientContext base,
            org.apache.bookkeeper.discover.profile.ZKProfileReservationStore.ReadOnlySnapshot snapshot,
            byte[] password, Map<BookieId, ProfileNormalDataClient> supplied, int maxReadEntries)
            throws GeneralSecurityException, BKException {
        var context = snapshot.context();
        var member = snapshot.closed().metadata();
        var metadata = member.metadata().getValue();
        var boundary = snapshot.closed().selection().boundary();
        if (maxReadEntries < 1 || maxReadEntries > 256 || supplied.size() > 64 || base.getConf().useV2WireProtocol
                || metadata.getState() != LedgerMetadata.State.CLOSED || metadata.getDigestType() != DigestType.CRC32C
                || member.birthZxid() != context.metadataBirth() || !boundary.context().equals(context)
                || metadata.getLastEntryId() != boundary.prefixEntryId()
                || metadata.getLength() != boundary.prefixLength()
                || metadata.getCToken() != context.creatorToken() || !Arrays.equals(metadata.getCustomMetadata()
                    .get(org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY), context.encode())) {
            throw new IllegalArgumentException("Readonly Profile handle requires matching bounded CLOSED facts");
        }
        if (!java.security.MessageDigest.isEqual(password, metadata.getPassword())) {
            throw BKException.create(BKException.Code.UnauthorizedAccessException);
        }
        var clients = Map.copyOf(supplied);
        for (var entry : clients.entrySet()) {
            var hello = entry.getValue().hello();
            if (!entry.getKey().toString().equals(hello.bookieId()) || hello.activeEngine() != 3
                    || snapshot.sources().stream().noneMatch(receipt ->
                        receipt.scope().targetBookie().equals(hello.bookieId())
                        && Arrays.equals(receipt.scope().targetStorageIncarnation(), hello.storageIncarnation())
                        && receipt.scope().ledgerId() == context.ledger().ledgerId()
                        && Arrays.equals(receipt.scope().ledgerInstanceId(), context.ledger().ledgerInstanceId())
                        && Arrays.equals(receipt.scope().descriptorIdentity().toBytes(),
                            context.ledger().descriptorIdentity()))) {
                throw new IllegalArgumentException("Readonly DATA peer is not the retained member incarnation");
            }
        }
        var credential = ProtectedProfileCredential.of(1, DigestManager.generateMasterKey(metadata.getPassword()));
        var transport = new ProfileBookieClient(base, context.ledger(), credential, clients, maxReadEntries,
                new ProfileBookieClient.Retry(1, Duration.ofSeconds(5), Duration.ofMillis(1)),
                null, Map.of(), metadata.getLastEntryId());
        return new ClosedReader(new ProfileContext(base, transport), member.metadata(), maxReadEntries);
    }

    /** Original SDK readonly engine, frozen metadata, no writer tasks, fencing, grant or metadata listener. */
    private static final class ClosedReader extends ReadOnlyLedgerHandle {
        private final ProfileBookieClient transport;
        private final Semaphore reads;
        private final int maxReadEntries;
        private final long prefix;

        ClosedReader(ProfileContext context, Versioned<LedgerMetadata> metadata, int maxReadEntries)
                throws GeneralSecurityException {
            super(context, metadata.getValue().getLedgerId(), metadata, BookKeeper.DigestType.CRC32C,
                    metadata.getValue().getPassword(), false);
            transport = context.transport();
            reads = new Semaphore(maxReadEntries);
            this.maxReadEntries = maxReadEntries;
            prefix = metadata.getValue().getLastEntryId();
        }

        private int admission(long first, long last) {
            return transport.isClosed() ? BKException.Code.ClientClosedException
                    : first < 0 || last < first ? BKException.Code.IncorrectParameterException
                    : last > prefix ? BKException.Code.ReadException
                    : last - first >= maxReadEntries || !reads.tryAcquire((int) (last - first + 1))
                        ? BKException.Code.TooManyRequestsException : BKException.Code.OK;
        }

        private CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> read(long first, long last,
                boolean unconfirmed) {
            int code = admission(first, last);
            if (code != BKException.Code.OK) {
                return CompletableFuture.failedFuture(BKException.create(code));
            }
            int count = (int) (last - first + 1);
            var caller = new CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries>();
            try {
                var actual = unconfirmed ? super.readUnconfirmedAsync(first, last) : super.readAsync(first, last);
                actual.whenComplete((entries, failure) -> {
                    reads.release(count);
                    if (failure != null) {
                        caller.completeExceptionally(failure);
                    } else if (!caller.complete(entries)) {
                        entries.close();
                    }
                });
            } catch (RuntimeException failure) {
                reads.release(count);
                caller.completeExceptionally(failure);
            }
            return caller;
        }

        @Override
        public CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> readAsync(long first, long last) {
            return read(first, last, false);
        }

        @Override
        public CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> readUnconfirmedAsync(long first,
                long last) {
            return read(first, last, true);
        }

        @Override
        void asyncReadEntriesInternal(long first, long last, AsyncCallback.ReadCallback callback, Object ctx,
                boolean recovery) {
            int code = recovery ? BKException.Code.IllegalOpException : admission(first, last);
            if (code != BKException.Code.OK) {
                callback.readComplete(code, this, null, ctx);
                return;
            }
            int count = (int) (last - first + 1);
            var released = new AtomicBoolean();
            Runnable release = () -> {
                if (released.compareAndSet(false, true)) {
                    reads.release(count);
                }
            };
            try {
                super.asyncReadEntriesInternal(first, last, (result, handle, entries, ignored) -> {
                    release.run();
                    callback.readComplete(result, handle, entries, ctx);
                }, null, false);
            } catch (RuntimeException failure) {
                release.run();
                throw failure;
            }
        }

        @Override
        public void onChanged(long ledger, Versioned<LedgerMetadata> metadata) {
            // A new cold open must validate any replacement snapshot; this handle keeps its exact CLOSED fact.
        }

        @Override
        synchronized void updateLastConfirmed(long lac, long length) {
            // CLOSED P/L are immutable; source hints cannot change this handle's confirmed window.
        }

        @Override
        public CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> batchReadAsync(long start,
                int count, long bytes) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.IllegalOpException));
        }

        @Override
        public CompletableFuture<org.apache.bookkeeper.client.api.LedgerEntries> batchReadUnconfirmedAsync(long start,
                int count, long bytes) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.IllegalOpException));
        }

        @Override
        public void asyncBatchReadEntries(long start, int count, long bytes, AsyncCallback.ReadCallback callback,
                Object ctx) {
            callback.readComplete(BKException.Code.IllegalOpException, this, null, ctx);
        }

        @Override
        public void asyncBatchReadUnconfirmedEntries(long start, int count, long bytes,
                AsyncCallback.ReadCallback callback, Object ctx) {
            callback.readComplete(BKException.Code.IllegalOpException, this, null, ctx);
        }

        @Override
        public void close() throws InterruptedException, BKException {
            transport.close();
            try {
                transport.closeCompletion().get();
            } catch (java.util.concurrent.ExecutionException failed) {
                throw BKException.create(BKException.getExceptionCode(failed));
            }
        }

        @Override
        public void asyncClose(CloseCallback callback, Object ctx) {
            transport.close();
            transport.closeCompletion().whenComplete((unused, failure) -> callback.closeComplete(
                    failure == null ? BKException.Code.OK : BKException.getExceptionCode(failure), this, ctx));
        }
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

    /** Explicit experimental close configuration; the normal DATA hot path acquires no new authority. */
    public CompletableFuture<Void> enableWriterClose(ProfileRecoveryCoordinator coordinator, byte[] descriptor,
            org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            Map<String, ProfileInitialControlClient> controls, int maxCoordinates, Duration duration) {
        if (descriptor.length > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH || controls.size() > 64
                || maxCoordinates < 1 || duration.isNegative() || duration.isZero()
                || duration.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Writer close requires bounded descriptor, peers and duration");
        }
        byte[] owned = descriptor.clone();
        var decoded = ProfileDescriptorCodec.decode(owned, validator);
        if (!Arrays.equals(ProfileDescriptorCodec.identityOf(decoded, validator).toBytes(),
                transport.ledgerContext().descriptorIdentity())) {
            throw new IllegalArgumentException("Writer close descriptor does not match this handle");
        }
        var settings = new CloseSettings(Objects.requireNonNull(coordinator, "coordinator"), owned, validator,
                Map.copyOf(controls), maxCoordinates, duration);
        var configured = new CompletableFuture<Void>();
        executeOrdered(() -> {
            synchronized (this) {
                if (lastAddPushed != INVALID_ENTRY_ID || stopped.get() || closeSettings != null) {
                    configured.completeExceptionally(new IllegalStateException("Configure before first append"));
                    return;
                }
                closeSettings = settings;
            }
            configured.complete(null);
        });
        return configured;
    }

    /** The original frozen request after local stop; retry/adoption must preserve this P/L and identity. */
    public ProfileRecoveryCoordinator.WriterCloseRequest writerCloseRequest() {
        return writerCloseRequest;
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

    /** Verified boundary lengths disagree; retains the existing legacy digest-error projection. */
    public static final class LacLengthConflictException extends BKException.BKDigestMatchException {
        private static final long serialVersionUID = 1L;
    }

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

    /** Flush this writer's confirmed prefix through the original SDK digest and ACK-quorum engine.
     * Caller cancellation stops waiting; stop/close still wait for every borrowed LAC output/slot. */
    public CompletableFuture<Long> flushConfirmedLac() {
        if (!lacInFlight.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException));
        }
        var result = new CompletableFuture<Long>();
        try {
            executeOrdered(() -> {
                if (!isHandleWritable() || getLastAddConfirmed() < 0) {
                    result.completeExceptionally(BKException.create(BKException.Code.IllegalOpException));
                    return;
                }
                try {
                    long confirmed = getLastAddConfirmed();
                    var operation = new PendingWriteLacOp(this, new ProfileContext(base, transport),
                            List.copyOf(getCurrentEnsemble()), (code, handle, ignored) -> {
                        if (code == BKException.Code.OK) {
                            result.complete(confirmed);
                        } else {
                            result.completeExceptionally(BKException.create(code));
                        }
                    }, null);
                    operation.setLac(confirmed);
                    operation.initiate(getDigestManager().computeDigestAndPackageForSendingLac(confirmed));
                } catch (RuntimeException failure) {
                    result.completeExceptionally(failure);
                }
            });
        } catch (RejectedExecutionException unavailable) {
            result.completeExceptionally(unavailable);
        }
        return result.whenComplete((value, failure) -> lacInFlight.set(false)).copy();
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
        var settings = closeSettings;
        if (settings != null) {
            durableClose.whenComplete((unused, failure) -> callback.closeComplete(failure == null ? BKException.Code.OK
                    : BKException.getExceptionCode(failure, BKException.Code.UnexpectedConditionException), this, ctx));
            if (closeStarted.compareAndSet(false, true)) {
                try {
                    executeOrdered(() -> closeWriter(settings, code));
                } catch (RejectedExecutionException unavailable) {
                    handleUnrecoverableErrorDuringAdd(BKException.Code.InterruptedException);
                    transport.closeCompletion().whenComplete((unused, failure) ->
                            durableClose.completeExceptionally(
                                BKException.create(BKException.Code.InterruptedException)));
                }
            }
            return;
        }
        handleUnrecoverableErrorDuringAdd(code);
        executeOrdered(() -> callback.closeComplete(BKException.Code.IllegalOpException, this, ctx));
    }

    private void closeWriter(CloseSettings settings, int code) {
        final List<PendingAddOp> pending;
        final Versioned<LedgerMetadata> expected;
        final long prefix;
        final long finalLength;
        synchronized (this) {
            stopped.set(true);
            pending = drainPendingAddsAndAdjustLength();
            lastAddPushed = lastAddConfirmed;
            prefix = lastAddConfirmed;
            finalLength = length.get();
            expected = getVersionedLedgerMetadata();
        }
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        Throwable localFailure = null;
        try {
            writerCloseRequest = new ProfileRecoveryCoordinator.WriterCloseRequest(transport.ledgerContext(),
                    expected, prefix, finalLength, identity());
            errorOutPendingAdds(code, pending);
        } catch (RuntimeException failure) {
            localFailure = failure;
        } finally {
            transport.close();
        }
        Throwable stopFailure = localFailure;
        transport.closeCompletion().whenComplete((unused, failure) -> {
            if (failure != null || stopFailure != null) {
                durableClose.completeExceptionally(failure != null ? failure : stopFailure);
                return;
            }
            try {
                settings.coordinator().closeWriter(writerCloseRequest, base, settings.descriptor(),
                        settings.validator(), ProtectedProfileCredential.of(1, ledgerKey), transport.readPeers(),
                        settings.controls(), settings.coordinates(), settings.duration())
                            .whenComplete((result, error) -> {
                    if (error != null) {
                        durableClose.completeExceptionally(error);
                        return;
                    }
                    int closeCode = switch (result.status()) {
                        case RECOVERED_AND_CLOSED -> BKException.Code.OK;
                        case DEFERRED -> BKException.Code.TimeoutException;
                        case CONFLICT -> BKException.Code.MetadataVersionException;
                        case QUARANTINED -> BKException.Code.DigestMatchException;
                        case DATA_LOSS -> BKException.Code.ReadException;
                        case DELETED -> BKException.Code.NoSuchLedgerExistsException;
                    };
                    if (closeCode == BKException.Code.OK) {
                        if (!setLedgerMetadata(expected, result.publication().metadata().metadata())) {
                            durableClose.completeExceptionally(
                                    BKException.create(BKException.Code.MetadataVersionException));
                        } else {
                            durableClose.complete(null);
                        }
                    } else {
                        durableClose.completeExceptionally(BKException.create(closeCode));
                    }
                });
            } catch (RuntimeException unavailable) {
                durableClose.completeExceptionally(unavailable);
            }
        });
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

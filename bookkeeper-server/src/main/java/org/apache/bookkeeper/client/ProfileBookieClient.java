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

import io.netty.util.ReferenceCounted;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.BatchedReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.ByteBufList;

/** Classify full status and retry same bytes before the original scalar callback, bound to verified peers. */
final class ProfileBookieClient implements BookieClient {
    record Retry(int attempts, Duration deadline, Duration backoff) {
        Retry {
            if (attempts < 1 || attempts > 16 || deadline.compareTo(Duration.ofMillis(1)) < 0
                    || deadline.compareTo(Duration.ofMinutes(1)) > 0 || backoff.compareTo(Duration.ofMillis(1)) < 0
                    || backoff.compareTo(deadline) > 0) {
                throw new IllegalArgumentException("Invalid bounded Profile retry settings");
            }
        }
    }
    private final ClientContext base;
    private final ProfileLedgerContext ledger;
    private final ProtectedProfileCredential credential;
    private volatile Map<BookieId, ProfileNormalDataClient> clients;
    private volatile Map<BookieId, ProfileNormalDataClient> readPeers;
    private final Retry retry;
    private final org.apache.bookkeeper.discover.profile.ProfileRecoveryContext recoveryContext;
    private final Map<BookieId, org.apache.bookkeeper.discover.profile.ProfileInitialControlClient> controlPeers;
    private final Map<BookieId, CompletableFuture<ProfileStatus>> readFences = new ConcurrentHashMap<>();
    private final int perTargetLimit;
    private final long readUpperBound;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean uncertain = new AtomicBoolean();
    private final Map<BookieId, Integer> counts = new ConcurrentHashMap<>();
    // Original ledger executor owns delivery state; shutdown completion uses a terminal CAS.
    private final Set<Delivery> pending = ConcurrentHashMap.newKeySet();
    // A logical result may finish while its own socket request still owns source/output custody.
    private final Set<CompletableFuture<Void>> requestDrains = ConcurrentHashMap.newKeySet();
    private final Object requestLifecycle = new Object();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();

    ProfileBookieClient(ClientContext base, ProfileLedgerContext ledger, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> clients, int perTargetLimit, Retry retry) {
        this(base, ledger, credential, clients, perTargetLimit, retry, null, Map.of());
    }

    ProfileBookieClient(ClientContext base, ProfileLedgerContext ledger, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> clients, int perTargetLimit, Retry retry,
            org.apache.bookkeeper.discover.profile.ProfileRecoveryContext recovery,
            Map<BookieId, org.apache.bookkeeper.discover.profile.ProfileInitialControlClient> controls) {
        this(base, ledger, credential, clients, perTargetLimit, retry, recovery, controls, Long.MAX_VALUE);
    }

    ProfileBookieClient(ClientContext base, ProfileLedgerContext ledger, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> clients, int perTargetLimit, Retry retry,
            org.apache.bookkeeper.discover.profile.ProfileRecoveryContext recovery,
            Map<BookieId, org.apache.bookkeeper.discover.profile.ProfileInitialControlClient> controls,
            long readUpperBound) {
        this.readUpperBound = readUpperBound;
        recoveryContext = recovery;
        controlPeers = Map.copyOf(controls);
        this.base = base;
        this.ledger = ledger;
        this.credential = credential;
        this.clients = Map.copyOf(clients);
        this.readPeers = this.clients;
        this.retry = retry;
        if (perTargetLimit < 1 || perTargetLimit > 256) {
            throw new IllegalArgumentException("Invalid per-target logical delivery bound");
        }
        this.perTargetLimit = perTargetLimit;
    }

    private void ordered(Runnable task) {
        base.getMainWorkerPool().executeOrdered(ledger.ledgerId(), task);
    }

    @Override
    public void addEntry(BookieId address, long ledgerId, byte[] key, long entryId, ReferenceCounted bytes,
            WriteCallback callback, Object context, int options, boolean failFast, EnumSet<WriteFlag> flags) {
        writeEntry(address, ledgerId, key, entryId, bytes, callback, context, options, flags, false);
    }

    private void writeEntry(BookieId address, long ledgerId, byte[] key, long entryId, ReferenceCounted bytes,
            WriteCallback callback, Object context, int options, EnumSet<WriteFlag> flags, boolean lacWrite) {
        ProfileNormalDataClient peer = clients.get(address);
        int rejected = closed.get() ? BKException.Code.ClientClosedException
                : recoveryContext != null ? BKException.Code.IllegalOpException
                : ledgerId != ledger.ledgerId() || peer == null || options != 0 || !flags.isEmpty()
                    ? BKException.Code.IllegalOpException
                    : !credential.matchesWireVerifier(key) ? BKException.Code.UnauthorizedAccessException : 0;
        if (rejected != 0) {
            ordered(() -> callback.writeComplete(rejected, ledgerId, entryId, address, context));
            return;
        }
        bytes.retain(); // Original PendingAddOp may complete at A before this target resolves.
        try {
            ordered(() -> {
                if (closed.get() || counts.getOrDefault(address, 0) >= perTargetLimit) {
                    bytes.release();
                    callback.writeComplete(closed.get() ? BKException.Code.ClientClosedException
                            : BKException.Code.TooManyRequestsException, ledgerId, entryId, address, context);
                    return;
                }
                Delivery delivery = new Delivery(address, peer, entryId, bytes, callback, context, lacWrite);
                counts.merge(address, 1, Integer::sum);
                pending.add(delivery);
                var current = clients.get(address);
                if (current == null || !Arrays.equals(delivery.incarnation, current.hello().storageIncarnation())) {
                    delivery.finish(BKException.Code.WriteException);
                    return;
                }
                try {
                    delivery.expiryTask = base.getScheduler().scheduleOrdered(ledger.ledgerId(), () -> {
                        try {
                            ordered(() -> {
                                if (delivery.terminal.get()) {
                                    return;
                                }
                                if (delivery.inFlight) {
                                    delivery.unknown = true;
                                    uncertain.set(true);
                                }
                                delivery.finish(delivery.unknown ? BKException.Code.TimeoutException
                                        : BKException.Code.TooManyRequestsException);
                            });
                        } catch (RejectedExecutionException shutdown) {
                            delivery.finishAfterExecutorShutdown();
                        }
                    }, retry.deadline().toNanos(), TimeUnit.NANOSECONDS);
                    delivery.send();
                } catch (RejectedExecutionException shutdown) {
                    delivery.finishAfterExecutorShutdown();
                }
            });
        } catch (RejectedExecutionException unavailable) {
            bytes.release();
            throw unavailable;
        }
    }

    private final class Delivery {
        final BookieId target;
        final ProfileNormalDataClient client;
        final byte[] incarnation;
        final long entryId;
        final ReferenceCounted bytes;
        final WriteCallback callback;
        final Object context;
        final boolean lacWrite;
        final long deadline = System.nanoTime() + retry.deadline().toNanos();
        int attempts;
        boolean unknown;
        final AtomicBoolean terminal = new AtomicBoolean();
        ScheduledFuture<?> retryTask;
        ScheduledFuture<?> expiryTask;
        volatile boolean inFlight;

        Delivery(BookieId target, ProfileNormalDataClient client, long entryId, ReferenceCounted bytes,
                WriteCallback callback, Object context, boolean lacWrite) {
            this.lacWrite = lacWrite;
            this.target = target;
            this.client = client;
            this.incarnation = client.hello().storageIncarnation();
            this.entryId = entryId;
            this.bytes = bytes;
            this.callback = callback;
            this.context = context;
        }

        void send() {
            if (terminal.get()) {
                return;
            }
            if (closed.get() || base.isClientClosed()) {
                finish(BKException.Code.ClientClosedException);
                return;
            }
            if (++attempts > retry.attempts() || System.nanoTime() >= deadline) {
                finish(unknown ? BKException.Code.TimeoutException : BKException.Code.TooManyRequestsException);
                return;
            }
            try {
                inFlight = true;
                CompletableFuture<ProfileStatus> result;
                CompletableFuture<Void> completion;
                synchronized (requestLifecycle) {
                    // Also covers the synchronous close fallback after the original executor rejects work.
                    if (closed.get()) {
                        finish(BKException.Code.ClientClosedException);
                        return;
                    }
                    if (lacWrite) {
                        var actual = client.beginWriteLac(ledger, credential, entryId, (ByteBufList) bytes);
                        result = actual.result();
                        completion = actual.completion();
                    } else {
                        var actual = client.beginNormalAdd(ledger, credential, entryId, bytes);
                        result = actual.result();
                        completion = actual.completion();
                    }
                    retainRequest(completion);
                }
                result.whenComplete((status, failure) -> {
                    try {
                        ordered(() -> classify(status, failure));
                    } catch (RejectedExecutionException shutdown) {
                        // Caller must stop this adapter and drain its original executor before closing the context.
                        finishAfterExecutorShutdown();
                    }
                });
            } catch (RuntimeException refused) {
                classify(null, refused);
            }
        }

        void classify(ProfileStatus status, Throwable failure) {
            if (terminal.get()) {
                return;
            }
            inFlight = false;
            if (failure != null || status == null) {
                unknown = true;
                uncertain.set(true);
                finish(BKException.Code.TimeoutException); // No implicit reconnect or identity change.
                return;
            }
            unknown |= status.durableResult() == ProfileStatus.DurableResult.UNKNOWN;
            if (unknown) {
                uncertain.set(true);
            }
            if (status.statusClass() == ProfileStatus.StatusClass.OK
                    && (status.durableResult() == ProfileStatus.DurableResult.APPLIED
                        || status.durableResult() == ProfileStatus.DurableResult.ALREADY_APPLIED)) {
                finish(BKException.Code.OK);
                return;
            }
            if ((status.statusClass() == ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE
                    || status.statusClass() == ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN)
                    && status.retryDisposition() == ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION) {
                if (attempts >= retry.attempts() || System.nanoTime() + retry.backoff().toNanos() >= deadline) {
                    finish(unknown ? BKException.Code.TimeoutException : BKException.Code.TooManyRequestsException);
                    return;
                }
                try {
                    retryTask = base.getScheduler().scheduleOrdered(ledger.ledgerId(), () -> {
                        try {
                            ordered(this::send);
                        } catch (RejectedExecutionException shutdown) {
                            finishAfterExecutorShutdown();
                        }
                    }, retry.backoff().toNanos(), TimeUnit.NANOSECONDS);
                } catch (RejectedExecutionException shutdown) {
                    finishAfterExecutorShutdown();
                }
                return;
            }
            int result = switch (status.statusClass()) {
                case FENCED -> BKException.Code.LedgerFencedException;
                case UNAUTHORIZED -> BKException.Code.UnauthorizedAccessException;
                case BAD_REQUEST, UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE -> BKException.Code.IllegalOpException;
                default -> BKException.Code.WriteException;
            };
            finish(unknown ? BKException.Code.TimeoutException : result);
        }

        void finish(int code) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            if (code != BKException.Code.OK && inFlight) {
                unknown = true;
                uncertain.set(true);
            }
            if (retryTask != null) {
                retryTask.cancel(false);
            }
            if (expiryTask != null) {
                expiryTask.cancel(false);
            }
            pending.remove(this);
            counts.compute(target, (key, count) -> count == null || count == 1 ? null : count - 1);
            try {
                var current = clients.get(target);
                boolean currentReplica = current != null
                        && Arrays.equals(incarnation, current.hello().storageIncarnation());
                if (callback instanceof PendingAddOp op) {
                    op.profileWriteComplete(code, ledger.ledgerId(), entryId, target, context, currentReplica);
                } else {
                    callback.writeComplete(code, ledger.ledgerId(), entryId, target, context);
                }
            } finally {
                bytes.release();
            }
        }

        void finishAfterExecutorShutdown() {
            finish(BKException.Code.InterruptedException);
        }
    }

    /** Called only under the original ledger completion gate on its executor. Wire I/O keeps its own custody. */
    void switchClients(Map<BookieId, ProfileNormalDataClient> current) {
        var retained = new java.util.HashMap<>(readPeers);
        retained.putAll(current);
        if (retained.size() > 64) {
            throw new IllegalArgumentException("Historical read peers exceed initial plus replacement pool bound");
        }
        readPeers = Map.copyOf(retained);
        clients = Map.copyOf(current);
        for (Delivery delivery : List.copyOf(pending)) {
            var peer = clients.get(delivery.target);
            if (peer == null || !Arrays.equals(delivery.incarnation, peer.hello().storageIncarnation())) {
                delivery.finish(BKException.Code.WriteException);
            }
        }
    }

    Map<BookieId, ProfileNormalDataClient> clients() {
        return clients;
    }

    boolean isRecoveryReader() {
        return recoveryContext != null;
    }

    ProfileLedgerContext ledgerContext() {
        return ledger;
    }

    Map<BookieId, ProfileNormalDataClient> readPeers() {
        return readPeers;
    }

    boolean hasUncertainWrites() {
        return uncertain.get();
    }

    @Override
    public List<BookieId> getFaultyBookies() {
        return List.of();
    }
    @Override
    public boolean isWritable(BookieId address, long ledgerId) {
        return !closed.get() && ledgerId == ledger.ledgerId() && clients.containsKey(address)
                && counts.getOrDefault(address, 0) < perTargetLimit;
    }
    @Override
    public long getNumPendingRequests(BookieId address, long ledgerId) {
        long count = counts.getOrDefault(address, 0);
        return isWritable(address, ledgerId) ? count : count | PENDINGREQ_NOTWRITABLE_MASK;
    }
    @Override
    public boolean isClosed() {
        return closed.get();
    }
    CompletableFuture<Void> closeCompletion() {
        return closeCompletion.copy();
    }

    @Override
    public void close() {
        synchronized (requestLifecycle) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
        }
        Runnable drain = () -> {
            Throwable callbackFailure = null;
            for (Delivery delivery : List.copyOf(pending)) {
                try {
                    delivery.finish(BKException.Code.ClientClosedException);
                } catch (RuntimeException failure) {
                    if (callbackFailure == null) {
                        callbackFailure = failure;
                    } else {
                        callbackFailure.addSuppressed(failure);
                    }
                }
            }
            Throwable failure = callbackFailure;
            CompletableFuture.allOf(requestDrains.toArray(CompletableFuture[]::new))
                    .whenComplete((unused, terminalFailure) -> {
                        if (failure != null || terminalFailure != null) {
                            closeCompletion.completeExceptionally(failure != null ? failure : terminalFailure);
                        } else {
                            closeCompletion.complete(null);
                        }
                    });
        };
        try {
            ordered(drain);
        } catch (RejectedExecutionException shutdown) {
            drain.run();
        }
    }
    @Override
    public void forceLedger(BookieId address, long ledgerId, ForceLedgerCallback cb, Object ctx) {
        ordered(() -> cb.forceLedgerComplete(BKException.Code.IllegalOpException, ledgerId, address, ctx));
    }
    @Override
    public void readLac(BookieId address, long ledgerId, ReadLacCallback cb, Object ctx) {
        ordered(() -> cb.readLacComplete(BKException.Code.IllegalOpException, ledgerId, null, null, ctx));
    }
    @Override
    public void writeLac(BookieId address, long ledgerId, byte[] key, long lac, ByteBufList bytes,
            WriteLacCallback cb, Object ctx) {
        if (lac < 0 || bytes.size() != 1 || bytes.readableBytes() != 20) {
            ordered(() -> cb.writeLacComplete(BKException.Code.IllegalOpException, ledgerId, address, ctx));
            return;
        }
        writeEntry(address, ledgerId, key, lac, bytes,
                (code, id, entry, peer, ignored) -> cb.writeLacComplete(code, id, peer, ctx),
                ctx, 0, EnumSet.noneOf(WriteFlag.class), true);
    }
    /** Admission and the close snapshot share requestLifecycle; completion never closes a borrowed peer. */
    private void retainRequest(CompletableFuture<Void> completion) {
        requestDrains.add(completion);
        completion.whenComplete((unused, failure) -> requestDrains.remove(completion));
    }

    /** LAC collectors release their owned replies before completing this internal, uncancelled result. */
    boolean retainLacRead(CompletableFuture<ProfileLedgerHandle.LacEvidence> result) {
        synchronized (requestLifecycle) {
            if (closed.get()) {
                return false;
            }
            retainRequest(result.handle((unused, failure) -> null));
            return true;
        }
    }

    CompletableFuture<ProfileNormalDataClient.ReadEntryBuffer> readEntryBuffer(
            ProfileNormalDataClient peer, long entryId) {
        synchronized (requestLifecycle) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
            }
            var attempt = peer.beginReadEntryBuffer(ledger, entryId);
            retainRequest(attempt.completion());
            return attempt.result();
        }
    }

    CompletableFuture<ProfileNormalDataClient.LacBuffer> readLacBuffer(ProfileNormalDataClient peer) {
        synchronized (requestLifecycle) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
            }
            var attempt = peer.beginReadLacBuffer(ledger);
            retainRequest(attempt.completion());
            return attempt.result();
        }
    }

    @Override
    public void readEntry(BookieId address, long ledgerId, long entryId, ReadEntryCallback cb, Object ctx,
            int flags, byte[] key, boolean failFast) {
        ProfileNormalDataClient peer = readPeers.get(address);
        int recoveryFlags = org.apache.bookkeeper.proto.BookieProtocol.FLAG_HIGH_PRIORITY
                | org.apache.bookkeeper.proto.BookieProtocol.FLAG_DO_FENCING;
        boolean fencing = recoveryContext != null && flags == recoveryFlags;
        if ((!fencing && flags != 0) || fencing && !credential.matchesWireVerifier(key)
                || ledgerId != ledger.ledgerId() || entryId < 0 || peer == null || closed.get()) {
            ordered(() -> cb.readEntryComplete(closed.get() ? BKException.Code.ClientClosedException
                    : BKException.Code.IllegalOpException, ledgerId, entryId, null, ctx));
            return;
        }
        if (entryId > readUpperBound) {
            ordered(() -> cb.readEntryComplete(BKException.Code.ReadException, ledgerId, entryId, null, ctx));
            return;
        }
        var callbackDone = new CompletableFuture<Void>();
        synchronized (requestLifecycle) {
            if (closed.get()) {
                ordered(() -> cb.readEntryComplete(BKException.Code.ClientClosedException,
                        ledgerId, entryId, null, ctx));
                return;
            }
            retainRequest(callbackDone);
        }
        CompletableFuture<ProfileNormalDataClient.ReadEntryBuffer> actual;
        try {
            CompletableFuture<ProfileStatus> fence = fencing ? fenceSource(address)
                    : CompletableFuture.completedFuture(null);
            actual = fence.thenCompose(status -> {
                if (fencing && (status == null || status.statusClass() != ProfileStatus.StatusClass.OK
                        || status.durableResult() != ProfileStatus.DurableResult.APPLIED
                            && status.durableResult() != ProfileStatus.DurableResult.ALREADY_APPLIED)) {
                    return CompletableFuture.failedFuture(BKException.create(
                            status != null && status.statusClass() == ProfileStatus.StatusClass.UNAUTHORIZED
                                ? BKException.Code.UnauthorizedAccessException : BKException.Code.ReadException));
                }
                return readEntryBuffer(peer, entryId);
            });
        } catch (RuntimeException failure) {
            actual = CompletableFuture.failedFuture(failure);
        }
        actual.whenComplete((reply, failure) -> {
            Runnable completed = () -> {
                try (reply) {
                    var current = readPeers.get(address);
                    if (failure != null || reply == null || current == null
                            || !Arrays.equals(peer.hello().storageIncarnation(),
                                    current.hello().storageIncarnation())) {
                        cb.readEntryComplete(BKException.getExceptionCode(failure)
                                == BKException.Code.UnauthorizedAccessException
                                ? BKException.Code.UnauthorizedAccessException : BKException.Code.ReadException,
                                ledgerId, entryId, null, ctx);
                        return;
                    }
                    switch (reply.outcome()) {
                        case FOUND -> {
                            var bytes = reply.entry();
                            try {
                                cb.readEntryComplete(BKException.Code.OK, ledgerId, entryId, bytes, ctx);
                            } finally {
                                bytes.release();
                            }
                        }
                        case MISSING -> cb.readEntryComplete(BKException.Code.NoSuchEntryException,
                                ledgerId, entryId, null, ctx);
                        case REFUSED -> cb.readEntryComplete(reply.status().statusClass()
                                == ProfileStatus.StatusClass.UNAUTHORIZED ? BKException.Code.UnauthorizedAccessException
                                    : BKException.Code.ReadException, ledgerId, entryId, null, ctx);
                        default -> throw new IllegalStateException("Unknown Profile point-read result");
                    }
                } finally {
                    callbackDone.complete(null);
                }
            };
            try {
                ordered(completed);
            } catch (RejectedExecutionException shutdown) {
                try {
                    if (reply != null) {
                        reply.close();
                    }
                    cb.readEntryComplete(BKException.Code.InterruptedException, ledgerId, entryId, null, ctx);
                } finally {
                    callbackDone.complete(null);
                }
            }
        });
    }
    private CompletableFuture<ProfileStatus> fenceSource(BookieId source) {
        return readFences.computeIfAbsent(source, bookie -> {
            var control = controlPeers.get(bookie);
            var data = readPeers.get(bookie);
            if (control == null || data == null || !Arrays.equals(control.hello().storageIncarnation(),
                    data.hello().storageIncarnation()) || !control.hello().bookieId().equals(bookie.toString())) {
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.ReadException));
            }
            synchronized (requestLifecycle) {
                if (closed.get()) {
                    return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
                }
                var attempt = control.beginFence(ledger, recoveryContext.operationId(), credential);
                retainRequest(attempt.completion());
                return attempt.result();
            }
        });
    }

    @Override
    public void batchReadEntries(BookieId address, long ledgerId, long entryId, int maxCount, long maxSize,
            BatchedReadEntryCallback cb, Object ctx, int flags, byte[] key, boolean failFast) {
        ordered(() -> cb.readEntriesComplete(BKException.Code.IllegalOpException, ledgerId, entryId, null, ctx));
    }
    @Override
    public void readEntryWaitForLACUpdate(BookieId address, long ledgerId, long entryId, long previousLac,
            long timeout, boolean entry, ReadEntryCallback cb, Object ctx) {
        ordered(() -> cb.readEntryComplete(BKException.Code.IllegalOpException, ledgerId, entryId, null, ctx));
    }
    @Override
    public void getBookieInfo(BookieId address, long requested, GetBookieInfoCallback cb, Object ctx) {
        ordered(() -> cb.getBookieInfoComplete(BKException.Code.IllegalOpException, null, ctx));
    }
    @Override
    public CompletableFuture<AvailabilityOfEntriesOfLedger> getListOfEntriesOfLedger(BookieId address,
            long ledgerId) {
        return CompletableFuture.failedFuture(BKException.create(BKException.Code.IllegalOpException));
    }
}

/*
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
 */
package org.apache.bookkeeper.discover.profile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.storage.segment.SegmentProfileDeleteAdoption;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.common.profile.wire.ProfilePhysicalDeleteCodec;

/** Borrowed cold logical deletion; delivery, access barriers and physical reclaim are separate procedures. */
public final class ProfileDeleteCoordinator {
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ThreadPoolExecutor cold;
    private final Semaphore active = new Semaphore(1);
    private final Object retentionEpisode = new Object();

    public ProfileDeleteCoordinator(ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ThreadPoolExecutor cold) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cold = Objects.requireNonNull(cold, "cold");
    }

    /** Fenced-CLOSED only. Reuse requestId after UNKNOWN; cancellation stops waiting and future stages. */
    public CompletableFuture<ZKProfileReservationStore.DeleteResult> deleteLedgerAsync(long ledger, byte[] requestId,
            ClientContext sdk, Duration maxDuration) {
        return beginDelete(ledger, requestId, sdk, maxDuration).result();
    }

    /** Completion denotes actual cold procedure return, not cancellation of an uncertain remote ZK request. */
    public record Attempt(CompletableFuture<ZKProfileReservationStore.DeleteResult> result,
            CompletableFuture<Void> completion) { }

    public Attempt beginDelete(long ledger, byte[] requestId, ClientContext sdk, Duration maxDuration) {
        byte[] request = ProfileLifecycleHead.fixed(requestId, 16, true);
        if (ledger < 0 || maxDuration.isNegative() || maxDuration.isZero()
                || maxDuration.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Logical deletion requires a bounded cold attempt");
        }
        if (sdk.isClientClosed()) {
            return new Attempt(CompletableFuture.failedFuture(
                    BKException.create(BKException.Code.ClientClosedException)),
                    CompletableFuture.completedFuture(null));
        }
        if (!active.tryAcquire()) {
            return new Attempt(CompletableFuture.completedFuture(unavailable()),
                    CompletableFuture.completedFuture(null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<ZKProfileReservationStore.DeleteResult>();
        var completion = new CompletableFuture<Void>();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.deleteClosed(metadata, ledger, request,
                    () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(unavailable());
                }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(unavailable());
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new Attempt(caller, completion.copy());
        } catch (RejectedExecutionException unavailable) {
            active.release();
            return new Attempt(CompletableFuture.completedFuture(unavailable()),
                    CompletableFuture.completedFuture(null));
        }
    }

    /** Opaque process-local observation; restart requires a fresh full window, never a wall-clock subtraction. */
    public static final class RetentionWindow {
        private final Object episode;
        private final ProfileDeleteRecord.Manifest manifest;
        private final ZKProfileReservationStore.RetentionBarrierResult original;
        private final long observedAt;

        private RetentionWindow(Object episode, ProfileDeleteRecord.Manifest manifest,
                ZKProfileReservationStore.RetentionBarrierResult original) {
            this.episode = episode;
            this.manifest = manifest;
            this.original = original;
            observedAt = System.nanoTime();
        }

        public ProfileDeleteRetention.Barrier barrier() {
            return original.barrier();
        }

        public long nativeCommit() {
            return original.nativeCommit();
        }

        public long remainingMillis() {
            long elapsed = System.nanoTime() - observedAt;
            long window = barrier().retention().policy().totalWindowMillis();
            if (elapsed < 0) {
                return window;
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(elapsed);
            return millis >= window ? 0 : window - millis;
        }
    }

    public enum RetentionWindowStatus {
        WAITING, WINDOW_ELAPSED, PENDING_TARGETS, PROTECTED_WITHOUT_POLICY,
        QUARANTINED, UNAVAILABLE, DURABILITY_UNKNOWN
    }

    public record RetentionWindowResult(RetentionWindowStatus status, RetentionWindow window) {
        public RetentionWindowResult {
            if ((status == RetentionWindowStatus.WAITING || status == RetentionWindowStatus.WINDOW_ELAPSED)
                    != (window != null)) {
                throw new IllegalArgumentException("Only a verified retention observation carries a window");
            }
        }
    }

    public record RetentionWindowAttempt(CompletableFuture<RetentionWindowResult> result,
            CompletableFuture<Void> completion) { }

    /** Persist the original all-target cut and start a conservative local wait. No GC permission is returned. */
    public RetentionWindowAttempt beginRetentionWindow(ProfileDeleteRecord.Manifest manifest,
            ClientContext sdk, Duration maxDuration) {
        return beginRetentionWindow(manifest, null, sdk, maxDuration);
    }

    /** Fresh native revalidation plus elapsed local window; backup acceptance and other GC gates remain separate. */
    public RetentionWindowAttempt beginCheckRetentionWindow(RetentionWindow window,
            ClientContext sdk, Duration maxDuration) {
        Objects.requireNonNull(window, "window");
        if (window.episode != retentionEpisode) {
            throw new IllegalArgumentException("A restarted coordinator requires a fresh retention observation");
        }
        return beginRetentionWindow(window.manifest, window, sdk, maxDuration);
    }

    private RetentionWindowAttempt beginRetentionWindow(ProfileDeleteRecord.Manifest manifest,
            RetentionWindow window, ClientContext sdk, Duration maxDuration) {
        Objects.requireNonNull(manifest, "manifest");
        if (maxDuration.isNegative() || maxDuration.isZero() || maxDuration.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Retention observation requires bounded original cold work");
        }
        if (sdk.isClientClosed() || !active.tryAcquire()) {
            return new RetentionWindowAttempt(CompletableFuture.completedFuture(
                    new RetentionWindowResult(RetentionWindowStatus.UNAVAILABLE, null)),
                    CompletableFuture.completedFuture(null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<RetentionWindowResult>();
        var completion = new CompletableFuture<Void>();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> window == null
                    ? reservations.publishDeleteRetentionBarrier(metadata, manifest,
                        () -> !stopped.get() && !sdk.isClientClosed())
                    : reservations.readDeleteRetentionBarrier(metadata, manifest), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else if (result.status() == ZKProfileReservationStore.RetentionBarrierStatus.BOUND) {
                    if (window != null && (result.nativeCommit() != window.nativeCommit()
                            || !Arrays.equals(result.barrier().encode(), window.barrier().encode()))) {
                        caller.complete(new RetentionWindowResult(RetentionWindowStatus.QUARANTINED, null));
                    } else {
                        var observed = window == null
                                ? new RetentionWindow(retentionEpisode, manifest, result) : window;
                        caller.complete(new RetentionWindowResult(observed.remainingMillis() == 0
                                ? RetentionWindowStatus.WINDOW_ELAPSED : RetentionWindowStatus.WAITING, observed));
                    }
                } else {
                    var status = switch (result.status()) {
                        case PENDING_TARGETS -> RetentionWindowStatus.PENDING_TARGETS;
                        case PROTECTED_WITHOUT_POLICY -> RetentionWindowStatus.PROTECTED_WITHOUT_POLICY;
                        case UNAVAILABLE -> RetentionWindowStatus.UNAVAILABLE;
                        case DURABILITY_UNKNOWN -> RetentionWindowStatus.DURABILITY_UNKNOWN;
                        default -> RetentionWindowStatus.QUARANTINED;
                    };
                    caller.complete(new RetentionWindowResult(status, null));
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new RetentionWindowResult(RetentionWindowStatus.UNAVAILABLE, null));
                }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(new RetentionWindowResult(RetentionWindowStatus.UNAVAILABLE, null));
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new RetentionWindowAttempt(caller, completion.copy());
        } catch (RejectedExecutionException unavailable) {
            active.release();
            return new RetentionWindowAttempt(CompletableFuture.completedFuture(
                    new RetentionWindowResult(RetentionWindowStatus.UNAVAILABLE, null)),
                    CompletableFuture.completedFuture(null));
        }
    }

    public record HandoffAttempt(CompletableFuture<ZKProfileReservationStore.DeleteHandoffResult> result,
            CompletableFuture<Void> completion) { }

    /** Real PREPARED publication only; actual local adoption/EFFECTIVE switching remain separate stages. */
    public HandoffAttempt beginPrepareHandoff(ProfileDeleteRecord.Target target, byte[] operationId,
            ClientContext sdk, int maximumEvents, Duration duration) {
        Objects.requireNonNull(target, "target");
        byte[] operation = ProfileLifecycleHead.fixed(operationId, 16, true);
        if (maximumEvents < 1 || maximumEvents > ProfileDeleteDelivery.MAX_EVENTS
                || duration.isNegative() || duration.isZero() || duration.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Handoff preparation needs the original bounded cold executor");
        }
        var unavailable = new ZKProfileReservationStore.DeleteHandoffResult(
                ZKProfileReservationStore.DeleteHandoffStatus.UNAVAILABLE, null);
        if (sdk.isClientClosed() || !active.tryAcquire()) {
            return new HandoffAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
        var caller = new CompletableFuture<ZKProfileReservationStore.DeleteHandoffResult>();
        var completion = new CompletableFuture<Void>();
        var stopped = new AtomicBoolean();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.prepareDeleteHandoff(
                    metadata, target, operation, maximumEvents, duration,
                    () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new ZKProfileReservationStore.DeleteHandoffResult(
                            ZKProfileReservationStore.DeleteHandoffStatus.DURABILITY_UNKNOWN, null));
                }, duration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException rejected) {
                stopped.set(true);
                caller.complete(unavailable);
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new HandoffAttempt(caller, completion.copy());
        } catch (RejectedExecutionException rejected) {
            active.release();
            return new HandoffAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
    }

    public record HandoffAckAttempt(CompletableFuture<ZKProfileReservationStore.DeleteHandoffAckResult> result,
            CompletableFuture<Void> completion) { }

    /** Persist the original local owner result; cancellation cannot manufacture or revoke native ACK. */
    public HandoffAckAttempt beginAcknowledgeHandoff(SegmentProfileDeleteAdoption local,
            ClientContext sdk, Duration duration) {
        Objects.requireNonNull(local, "local");
        if (duration.isNegative() || duration.isZero() || duration.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Handoff acknowledgement needs the original bounded cold executor");
        }
        var unavailable = new ZKProfileReservationStore.DeleteHandoffAckResult(
                ZKProfileReservationStore.DeleteHandoffAckStatus.UNAVAILABLE, null);
        if (sdk.isClientClosed() || !active.tryAcquire()) {
            return new HandoffAckAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
        var caller = new CompletableFuture<ZKProfileReservationStore.DeleteHandoffAckResult>();
        var completion = new CompletableFuture<Void>();
        var stopped = new AtomicBoolean();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.acknowledgeDeleteHandoff(
                    local, duration,
                    () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new ZKProfileReservationStore.DeleteHandoffAckResult(
                            ZKProfileReservationStore.DeleteHandoffAckStatus.DURABILITY_UNKNOWN, null));
                }, duration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException rejected) {
                stopped.set(true);
                caller.complete(unavailable);
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new HandoffAckAttempt(caller, completion.copy());
        } catch (RejectedExecutionException rejected) {
            active.release();
            return new HandoffAckAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
    }

    public record SnapshotAttempt(CompletableFuture<ZKProfileReservationStore.DeleteSnapshotResult> result,
            CompletableFuture<Void> completion) { }

    /** Original cold/timer custody; waiting cancellation stops future publication stages, not accepted ZK I/O. */
    public SnapshotAttempt beginSnapshot(ProfileDeleteRecord.Target target, ClientContext sdk,
            int maximumEvents, Duration maxDuration) {
        Objects.requireNonNull(target, "target");
        if (maximumEvents < 1 || maximumEvents > ProfileDeleteDelivery.MAX_EVENTS
                || maxDuration.isNegative() || maxDuration.isZero()
                || maxDuration.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Delete snapshot requires a bounded cold attempt");
        }
        var unavailable = new ZKProfileReservationStore.DeleteSnapshotResult(
                ZKProfileReservationStore.DeleteSnapshotStatus.UNAVAILABLE, null);
        if (sdk.isClientClosed() || !active.tryAcquire()) {
            return new SnapshotAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<ZKProfileReservationStore.DeleteSnapshotResult>();
        var completion = new CompletableFuture<Void>();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.publishDeleteSnapshot(
                    metadata, target, maximumEvents, maxDuration,
                    () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new ZKProfileReservationStore.DeleteSnapshotResult(
                            ZKProfileReservationStore.DeleteSnapshotStatus.DURABILITY_UNKNOWN, null));
                }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException rejected) {
                stopped.set(true);
                caller.complete(unavailable);
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new SnapshotAttempt(caller, completion.copy());
        } catch (RejectedExecutionException rejected) {
            active.release();
            return new SnapshotAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
    }

    public enum PhysicalStatus { REUSABLE_EVERY_TARGET, DURABLY_REUSABLE_EVERY_TARGET, PENDING, QUARANTINED }

    public record JournalCompactionAttempt(
            CompletableFuture<ZKProfileReservationStore.JournalCompactionResult> result,
            CompletableFuture<Void> completion) { }

    /** Bounded duplicate-prefix cleanup; actual cold completion remains independent of waiting cancellation. */
    public JournalCompactionAttempt beginCompactJournalPrefix(ProfileDeleteRecord.Target target,
            long first, long last, ClientContext sdk, Duration maxDuration) {
        Objects.requireNonNull(target, "target");
        if (first < 1 || last < first || last > ProfileDeleteDelivery.MAX_EVENTS
                || last - first >= ProfileDeleteSnapshot.EVENTS_PER_PAGE
                || maxDuration.isNegative() || maxDuration.isZero()
                || maxDuration.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Delete journal compaction requires a bounded prefix batch");
        }
        var unavailable = ZKProfileReservationStore.journalCompactionFailure(
                ZKProfileReservationStore.JournalCompactionStatus.UNAVAILABLE, first, last);
        if (sdk.isClientClosed()) {
            return new JournalCompactionAttempt(CompletableFuture.failedFuture(
                    BKException.create(BKException.Code.ClientClosedException)),
                    CompletableFuture.completedFuture(null));
        }
        if (!active.tryAcquire()) {
            return new JournalCompactionAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<ZKProfileReservationStore.JournalCompactionResult>();
        var completion = new CompletableFuture<Void>();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.compactDeleteJournalPrefix(
                    metadata, target, first, last, () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(ZKProfileReservationStore.journalCompactionFailure(
                            ZKProfileReservationStore.JournalCompactionStatus.DURABILITY_UNKNOWN, first, last));
                }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException rejected) {
                stopped.set(true);
                caller.complete(unavailable);
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new JournalCompactionAttempt(caller, completion.copy());
        } catch (RejectedExecutionException rejected) {
            active.release();
            return new JournalCompactionAttempt(CompletableFuture.completedFuture(unavailable),
                    CompletableFuture.completedFuture(null));
        }
    }

    public record TargetObservation(ProfilePhysicalDeleteCodec.Outcome outcome,
            ProfilePhysicalDeleteCodec.Reply reply) { }

    /** Ephemeral exact-event observations, not persisted terminal receipts or history-GC authority. */
    public record PhysicalResult(PhysicalStatus status, ProfileDeleteRecord.Manifest manifest,
            Map<ProfileDeleteRecord.Target, TargetObservation> targets) {
        public PhysicalResult {
            targets = Map.copyOf(targets);
        }
    }

    public record PhysicalAttempt(CompletableFuture<PhysicalResult> result, CompletableFuture<Void> completion) { }

    /** Observe every historical identity/incarnation through existing verified CONTROL peers.
     * Missing peers remain PENDING. This method neither applies UIC7 nor discards offline obligations. */
    public PhysicalAttempt beginObservePhysicalDeletion(ProfileDeleteRecord.Manifest manifest,
            Map<ProfileDeleteRecord.Target, ProfileInitialControlClient> peers, ClientContext sdk,
            int maximumAllocations, int maximumCompactions, Duration maxDuration) {
        return beginPhysicalDeletion(manifest, peers, sdk, maximumAllocations, maximumCompactions, maxDuration, false);
    }

    /** Persist actual peer terminal receipts and resume from them after coordinator restart or target disconnect.
     * All historical targets must have an exact stored receipt. No retention or history-GC permission is granted. */
    public PhysicalAttempt beginAwaitPhysicalDeletion(ProfileDeleteRecord.Manifest manifest,
            Map<ProfileDeleteRecord.Target, ProfileInitialControlClient> peers, ClientContext sdk,
            int maximumAllocations, int maximumCompactions, Duration maxDuration) {
        return beginPhysicalDeletion(manifest, peers, sdk, maximumAllocations, maximumCompactions, maxDuration, true);
    }

    private PhysicalAttempt beginPhysicalDeletion(ProfileDeleteRecord.Manifest manifest,
            Map<ProfileDeleteRecord.Target, ProfileInitialControlClient> peers, ClientContext sdk,
            int maximumAllocations, int maximumCompactions, Duration maxDuration, boolean persist) {
        Objects.requireNonNull(manifest, "manifest");
        var clients = Map.copyOf(peers);
        if (maximumAllocations < 1 || maximumAllocations > 4096 || maximumCompactions < 1
                || maximumCompactions > 256 || maxDuration.isZero() || maxDuration.isNegative()
                || maxDuration.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Physical observation requires bounded original cold work");
        }
        for (var entry : clients.entrySet()) {
            var hello = entry.getValue().hello();
            if (!manifest.targets().contains(entry.getKey()) || !entry.getKey().bookie().equals(hello.bookieId())
                    || !Arrays.equals(entry.getKey().incarnation(), hello.storageIncarnation())) {
                throw new IllegalArgumentException("Physical observation peer differs from its obligated target");
            }
        }
        var observations = new java.util.concurrent.ConcurrentHashMap<ProfileDeleteRecord.Target, TargetObservation>();
        for (var target : manifest.targets()) {
            observations.put(target, new TargetObservation(ProfilePhysicalDeleteCodec.Outcome.PENDING, null));
        }
        var pending = new PhysicalResult(PhysicalStatus.PENDING, manifest, observations);
        if (sdk.isClientClosed() || !active.tryAcquire()) {
            return new PhysicalAttempt(CompletableFuture.completedFuture(pending),
                    CompletableFuture.completedFuture(null));
        }
        var caller = new CompletableFuture<PhysicalResult>();
        var completion = new CompletableFuture<Void>();
        var stopped = new AtomicBoolean();
        var terminal = new ArrayList<CompletableFuture<Void>>();
        var quarantined = new AtomicBoolean();
        long deadline = System.nanoTime() + maxDuration.toNanos();
        java.util.concurrent.ScheduledFuture<?> timer;
        try {
            timer = sdk.getScheduler().schedule(() -> {
                stopped.set(true);
                caller.complete(new PhysicalResult(PhysicalStatus.PENDING, manifest, observations));
            }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException rejected) {
            active.release();
            return new PhysicalAttempt(CompletableFuture.completedFuture(pending),
                    CompletableFuture.completedFuture(null));
        }
        caller.whenComplete((result, failure) -> {
            if (caller.isCancelled()) {
                stopped.set(true);
            }
        });
        java.util.function.BooleanSupplier live =
                () -> !stopped.get() && !sdk.isClientClosed() && System.nanoTime() < deadline;
        CompletableFuture<Boolean> chain;
        try {
            chain = CompletableFuture.supplyAsync(() -> {
                var fresh = reservations.readLogicalDeletion(metadata, manifest.ledger().ledgerId(),
                        manifest.seal().requestId());
                boolean valid = fresh.manifest() != null && Arrays.equals(fresh.manifest().encode(), manifest.encode());
                quarantined.set(!valid && fresh.status() != ZKProfileReservationStore.DeleteStatus.UNAVAILABLE);
                return valid;
            }, cold);
        } catch (RejectedExecutionException exhausted) {
            chain = CompletableFuture.failedFuture(exhausted);
        }
        for (var target : manifest.targets()) {
            chain = chain.thenComposeAsync(valid -> {
                if (!valid || !live.getAsBoolean()) {
                    return CompletableFuture.completedFuture(valid);
                }
                if (persist) {
                    var stored = reservations.readPhysicalDeleteReceipt(metadata, manifest, target);
                    if (stored.status() == ZKProfileReservationStore.PhysicalReceiptStatus.FOUND) {
                        observations.put(target, new TargetObservation(ProfilePhysicalDeleteCodec.Outcome.REUSABLE,
                                stored.receipt().reply()));
                        return CompletableFuture.completedFuture(true);
                    }
                    if (stored.status() != ZKProfileReservationStore.PhysicalReceiptStatus.ABSENT) {
                        observations.put(target,
                                new TargetObservation(ProfilePhysicalDeleteCodec.Outcome.UNKNOWN, null));
                        if (stored.status() == ZKProfileReservationStore.PhysicalReceiptStatus.QUARANTINED) {
                            quarantined.set(true);
                        }
                        return CompletableFuture.completedFuture(true);
                    }
                }
                if (!clients.containsKey(target) || !live.getAsBoolean()) {
                    return CompletableFuture.completedFuture(true);
                }
                var event = reservations.readLedgerDeleteDelivery(metadata, manifest.ledger().ledgerId(), target);
                if (event.status() != ZKProfileReservationStore.DeliveryStatus.COMMITTED
                        || !event.event().ledger().equals(manifest.ledger())
                        || event.event().deleteEpoch() != manifest.seal().epoch()
                        || !Arrays.equals(event.event().requestId(), manifest.seal().requestId())) {
                    observations.put(target, new TargetObservation(ProfilePhysicalDeleteCodec.Outcome.UNKNOWN, null));
                    if (event.status() != ZKProfileReservationStore.DeliveryStatus.UNAVAILABLE) {
                        quarantined.set(true);
                    }
                    return CompletableFuture.completedFuture(true);
                }
                var root = reservations.read(manifest.ledger().ledgerId());
                if (root.snapshot() == null || !live.getAsBoolean()) {
                    return CompletableFuture.completedFuture(false);
                }
                String domain = root.snapshot().reservation().installs().get(0).authorityDomain();
                int millis = (int) Math.max(1, Math.min(60_000,
                        TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                var request = new ProfilePhysicalDeleteCodec.Request(event.event().deleteApply(domain),
                        maximumAllocations, maximumCompactions, millis);
                var attempt = clients.get(target).physicalDelete(request);
                terminal.add(attempt.completion());
                return attempt.result().handleAsync((reply, failure) -> {
                    var outcome = failure == null && reply != null
                            ? reply.outcome() : ProfilePhysicalDeleteCodec.Outcome.UNKNOWN;
                    if (persist && outcome == ProfilePhysicalDeleteCodec.Outcome.REUSABLE) {
                        var receipt = ProfileDeletePhysicalReceipt.fromVerifiedReply(
                                event.event(), clients.get(target).hello(), request, reply);
                        var written = reservations.publishPhysicalDeleteReceipt(metadata, manifest, receipt, live);
                        if (written.status() != ZKProfileReservationStore.PhysicalReceiptStatus.FOUND) {
                            outcome = ProfilePhysicalDeleteCodec.Outcome.UNKNOWN;
                            if (written.status() == ZKProfileReservationStore.PhysicalReceiptStatus.QUARANTINED) {
                                quarantined.set(true);
                            }
                        } else {
                            reply = written.receipt().reply();
                        }
                    }
                    observations.put(target, new TargetObservation(outcome, reply));
                    return true;
                }, cold);
            }, cold);
        }
        chain.thenApplyAsync(valid -> {
            if (!valid || !live.getAsBoolean()) {
                return new PhysicalResult(quarantined.get() ? PhysicalStatus.QUARANTINED : PhysicalStatus.PENDING,
                        manifest, observations);
            }
            var fresh = reservations.readLogicalDeletion(metadata, manifest.ledger().ledgerId(),
                    manifest.seal().requestId());
            if (fresh.manifest() == null || !Arrays.equals(fresh.manifest().encode(), manifest.encode())) {
                return new PhysicalResult(fresh.status() == ZKProfileReservationStore.DeleteStatus.UNAVAILABLE
                        ? PhysicalStatus.PENDING : PhysicalStatus.QUARANTINED, manifest, observations);
            }
            if (quarantined.get()) {
                return new PhysicalResult(PhysicalStatus.QUARANTINED, manifest, observations);
            }
            boolean all = manifest.targets().stream().allMatch(target ->
                    observations.get(target).outcome() == ProfilePhysicalDeleteCodec.Outcome.REUSABLE);
            if (persist && all) {
                // Re-read every immutable target row; an earlier read or write callback is not the final gate.
                for (var target : manifest.targets()) {
                    if (!live.getAsBoolean()) {
                        return new PhysicalResult(PhysicalStatus.PENDING, manifest, observations);
                    }
                    var stored = reservations.readPhysicalDeleteReceipt(metadata, manifest, target);
                    if (stored.status() != ZKProfileReservationStore.PhysicalReceiptStatus.FOUND) {
                        observations.put(target,
                                new TargetObservation(ProfilePhysicalDeleteCodec.Outcome.UNKNOWN, null));
                        return new PhysicalResult(stored.status()
                                == ZKProfileReservationStore.PhysicalReceiptStatus.QUARANTINED
                                    ? PhysicalStatus.QUARANTINED : PhysicalStatus.PENDING, manifest, observations);
                    }
                }
                var finalLogical = reservations.readLogicalDeletion(metadata, manifest.ledger().ledgerId(),
                        manifest.seal().requestId());
                if (!live.getAsBoolean() || finalLogical.manifest() == null
                        || !Arrays.equals(finalLogical.manifest().encode(), manifest.encode())) {
                    return new PhysicalResult(finalLogical.status()
                            == ZKProfileReservationStore.DeleteStatus.UNAVAILABLE || !live.getAsBoolean()
                                ? PhysicalStatus.PENDING : PhysicalStatus.QUARANTINED, manifest, observations);
                }
            }
            return new PhysicalResult(all
                    ? persist ? PhysicalStatus.DURABLY_REUSABLE_EVERY_TARGET
                        : PhysicalStatus.REUSABLE_EVERY_TARGET
                    : PhysicalStatus.PENDING, manifest, observations);
        }, cold).whenComplete((result, failure) ->
                CompletableFuture.allOf(terminal.toArray(CompletableFuture[]::new)).whenComplete((ignored, failed) -> {
                    timer.cancel(false);
                    active.release();
                    completion.complete(null);
                    caller.complete(failure == null ? result
                            : new PhysicalResult(PhysicalStatus.PENDING, manifest, observations));
                }));
        return new PhysicalAttempt(caller, completion.copy());
    }

    private static ZKProfileReservationStore.DeleteResult unavailable() {
        return new ZKProfileReservationStore.DeleteResult(ZKProfileReservationStore.DeleteStatus.UNAVAILABLE, null);
    }
}

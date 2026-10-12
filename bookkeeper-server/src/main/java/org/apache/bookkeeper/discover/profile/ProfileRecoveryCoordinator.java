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

import io.netty.util.ReferenceCounted;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.ProfileRecoveryReadHandle;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.net.BookieId;

/** Borrowed bounded recovery coordination. Write-back, terminal cut and matching metadata close are distinct facts. */
public final class ProfileRecoveryCoordinator {
    public enum Status { FENCED_CONTEXT, DEFERRED, CONFLICT, QUARANTINED }

    public record Fence(String bookie, String storageIncarnation) { }

    /** Fences remain historical evidence on non-OK results; no result is RECOVERED_AND_CLOSED. */
    public record Result(Status status, ProfileRecoveryContext context, List<Fence> fences) {
        public Result {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(context, "context");
            fences = List.copyOf(fences);
        }
    }

    private record Prepared(ZKProfileReservationStore.RecoveryResult recovery, List<ProfileInstallReceipt> targets) { }
    private record Reply(String bookie, ProfileInitialControlClient peer, ProfileStatus status, Throwable failure) { }
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ThreadPoolExecutor cold;
    private final Semaphore active = new Semaphore(1);

    public ProfileRecoveryCoordinator(ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ThreadPoolExecutor cold) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cold = Objects.requireNonNull(cold, "cold");
        if (!(cold.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue<?>)
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Recovery requires a bounded rejecting borrowed cold executor");
        }
    }

    public record WorkerOptions(byte[] descriptor,
            org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            Map<BookieId, ProfileNormalDataClient> data, Map<String, ProfileInitialControlClient> controls,
            int maxCoordinates, int maxRepairs, Duration maxDuration) {
        public WorkerOptions {
            if (descriptor.length > org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.MAX_CANONICAL_LENGTH
                    || data.size() > 64 || controls.size() > 64 || maxCoordinates < 1 || maxRepairs < 1
                    || maxRepairs > ProfileRepairIntent.MAX_INTENTS || maxDuration.isZero() || maxDuration.isNegative()
                    || maxDuration.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("Profile worker requires bounded descriptor, pools and work");
            }
            descriptor = descriptor.clone();
            org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.decode(descriptor, validator);
            data = Map.copyOf(data);
            controls = Map.copyOf(controls);
        }

        @Override
        public byte[] descriptor() {
            return descriptor.clone();
        }
    }

    public enum WorkerStatus { REPLICATED, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS, DELETED }
    public record WorkerResult(WorkerStatus status, int repairs, long copiedEntries) { }

    /** Terminal means actual child drain, including a stopped/deadline attempt; never an early caller timeout. */
    public static final class WorkerAttempt {
        private final CompletableFuture<WorkerResult> terminal;
        private final AtomicBoolean stopped;
        private final org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager;
        private final org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task;

        private WorkerAttempt(CompletableFuture<WorkerResult> terminal, AtomicBoolean stopped,
                org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager,
                org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task) {
            this.terminal = terminal;
            this.stopped = stopped;
            this.manager = manager;
            this.task = task;
        }

        public CompletableFuture<WorkerResult> completion() {
            return terminal.copy();
        }

        public void stop() {
            stopped.set(true);
        }

        public void releaseTask() throws org.apache.bookkeeper.replication.ReplicationException.UnavailableException {
            if (!terminal.isDone()) {
                throw new IllegalStateException("Original Profile task remains held until actual child drain");
            }
            manager.releaseProfileTask(task);
        }
    }

    public WorkerAttempt beginWorker(long ledger, ClientContext sdk, WorkerOptions options,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager) {
        return beginWorker(ledger, sdk, options, manager, null);
    }

    /** A dynamic lease is drained before this attempt can consume its original task and marker. */
    public WorkerAttempt beginWorker(long ledger, ClientContext sdk, WorkerOptions options,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager, ProfileRecoveryPeerPool.Lease lease) {
        ProfileRecoveryReadHandle.requireCompatibleSdk(sdk);
        if (lease != null) {
            options = new WorkerOptions(options.descriptor(), options.validator(), lease.data(), lease.controls(),
                    options.maxCoordinates(), options.maxRepairs(), options.maxDuration());
        }
        var task = manager.captureProfileTask(ledger);
        var stopped = new AtomicBoolean();
        if (ledger < 0 || task == null || sdk.isClientClosed() || !supportsWorker(manager) || !active.tryAcquire()) {
            return new WorkerAttempt(CompletableFuture.completedFuture(
                    new WorkerResult(WorkerStatus.DEFERRED, 0, 0)), stopped, manager, task);
        }
        var terminal = new CompletableFuture<WorkerResult>();
        var attempt = new WorkerAttempt(terminal, stopped, manager, task);
        var flow = new WorkerFlow(ledger, sdk, options, manager, task, stopped, lease);
        var duration = options.maxDuration();
        var timer = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        flow.done.whenComplete((result, failure) -> {
            var scheduled = timer.get();
            if (scheduled != null) {
                scheduled.cancel(false);
            }
            active.release();
            if (failure != null) {
                terminal.completeExceptionally(failure);
            } else {
                terminal.complete(result);
            }
        });
        try {
            var scheduled = sdk.getScheduler().schedule(() -> stopped.set(true), duration.toNanos(),
                    java.util.concurrent.TimeUnit.NANOSECONDS);
            timer.set(scheduled);
            flow.step();
            if (flow.done.isDone()) {
                scheduled.cancel(false);
            }
        } catch (RuntimeException unavailable) {
            stopped.set(true);
            flow.done.completeExceptionally(unavailable);
        }
        return attempt;
    }

    public boolean supportsWorker(org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager) {
        return reservations.supportsWorker(metadata, manager);
    }

    private final class WorkerFlow {
        private final long ledger;
        private final ClientContext sdk;
        private final WorkerOptions options;
        private final org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager;
        private final org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task;
        private final AtomicBoolean stopped;
        private final ProfileRecoveryPeerPool.Lease lease;
        private final long deadline;
        private final CompletableFuture<WorkerResult> done = new CompletableFuture<>();
        private final java.util.Set<Integer> resolved = new java.util.HashSet<>();
        private int repairs;
        private long copied;

        WorkerFlow(long ledger, ClientContext sdk, WorkerOptions options,
                org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager manager,
                org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task, AtomicBoolean stopped,
                ProfileRecoveryPeerPool.Lease lease) {
            this.ledger = ledger;
            this.sdk = sdk;
            this.options = options;
            this.manager = manager;
            this.task = task;
            this.stopped = stopped;
            this.lease = lease;
            deadline = System.nanoTime() + options.maxDuration().toNanos();
        }

        void finish(WorkerStatus status) {
            done.complete(new WorkerResult(stopped.get() ? WorkerStatus.DEFERRED : status, repairs, copied));
        }

        void step() {
            if (stopped.get() || sdk.isClientClosed()) {
                finish(WorkerStatus.DEFERRED);
                return;
            }
            CompletableFuture.supplyAsync(() -> metadata.readMembership(ledger, deadline), cold).thenCompose(read -> {
                if (read.snapshot() == null) {
                    finish(read.status() == org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus
                            .UNAVAILABLE ? WorkerStatus.DEFERRED : WorkerStatus.QUARANTINED);
                    return CompletableFuture.<RecoveryResult>completedFuture(null);
                }
                ProtectedProfileCredential key;
                try {
                    key = ProtectedProfileCredential.of(1, org.apache.bookkeeper.proto.checksum.DigestManager
                            .generateMasterKey(read.snapshot().metadata().getValue().getPassword()));
                } catch (java.security.NoSuchAlgorithmException impossible) {
                    return CompletableFuture.failedFuture(impossible);
                }
                byte[] descriptor = options.descriptor();
                var decoded = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.decode(descriptor,
                        options.validator());
                byte[] identity = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.identityOf(decoded,
                        options.validator()).toBytes();
                var progress = new java.util.concurrent.atomic.AtomicReference<>(new RecoveryResult(ledger,
                        RecoveryStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE, null, -1, 0, null, null));
                var id = java.util.UUID.randomUUID();
                byte[] operation = java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                        .putLong(id.getLeastSignificantBits()).array();
                return recoverOnce(ledger, operation, sdk, descriptor, decoded, identity, key,
                        workerData(read.snapshot().metadata().getValue(), null), options.controls(),
                        options.maxCoordinates(), stopped, progress).thenCompose(recovered -> {
                    if (recovered.context() == null || stopped.get()) {
                        finish(workerStatus(recovered.status()));
                        return CompletableFuture.completedFuture(recovered);
                    }
                    if (recovered.status() != RecoveryStatus.RECOVERED_AND_CLOSED
                            && recovered.status() != RecoveryStatus.DEFERRED) {
                        finish(workerStatus(recovered.status()));
                        return CompletableFuture.completedFuture(recovered);
                    }
                    return workCurrent(recovered.context(), key).thenApply(ignored -> recovered);
                });
            }).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    done.completeExceptionally(failure);
                }
            });
        }

        private CompletableFuture<Void> workCurrent(ProfileRecoveryContext context, ProtectedProfileCredential key) {
            var supplied = new java.util.HashSet<String>();
            options.data().keySet().forEach(name -> {
                if (options.controls().containsKey(name.toString())) {
                    supplied.add(name.toString());
                }
            });
            return CompletableFuture.supplyAsync(
                    () -> reservations.resumeWorkerRepair(metadata, context, resolved, supplied), cold)
                    .thenCompose(plan -> {
                if (stopped.get() || plan.recovery() == null) {
                    finish(plan.status() == ZKProfileReservationStore.CreateStatus.QUARANTINED
                            ? WorkerStatus.QUARANTINED : WorkerStatus.DEFERRED);
                    return CompletableFuture.completedFuture(null);
                }
                if (plan.prepared() != null) {
                    return runRepair(plan, key);
                }
                var value = plan.recovery().metadata().metadata().getValue();
                if (value.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED) {
                    return CompletableFuture.supplyAsync(() -> reservations.readClosedForOpen(metadata, ledger), cold)
                            .thenCompose(closed -> {
                    if (closed.snapshot() == null) {
                        finish(closed.status() == ZKProfileReservationStore.ReadOnlyStatus.DELETED
                                ? WorkerStatus.DELETED : WorkerStatus.DEFERRED);
                        return CompletableFuture.completedFuture(null);
                    }
                    return verifyClosedReplicasOnce(closed.snapshot(), sdk, key, options.data(), options.controls(),
                            options.maxCoordinates(), stopped, deadline, task).thenCompose(health -> {
                        if (health.proof() != null && !stopped.get()) {
                            var drained = lease == null ? CompletableFuture.<Void>completedFuture(null)
                                    : lease.closeAsync();
                            return drained.thenCompose(ignored -> CompletableFuture.supplyAsync(
                                    () -> completeReplicationOnce(health.proof(), manager, stopped), cold))
                                        .thenAccept(status -> finish(status == MarkerCleanupStatus.CLEARED
                                                ? WorkerStatus.REPLICATED : status == MarkerCleanupStatus.DELETED
                                                    ? WorkerStatus.DELETED : WorkerStatus.DEFERRED));
                        }
                        if (health.status() == ReplicaHealthStatus.UNDERREPLICATED
                                || health.status() == ReplicaHealthStatus.DEFERRED) {
                            return selectRepair(plan.recovery(), health.unresolvedEntry(), health.sources(), key);
                        }
                        finish(health.status() == ReplicaHealthStatus.DELETED ? WorkerStatus.DELETED
                                : health.status() == ReplicaHealthStatus.CONFLICT ? WorkerStatus.CONFLICT
                                    : WorkerStatus.QUARANTINED);
                        return CompletableFuture.completedFuture(null);
                    });
                    });
                }
                return probeLast(plan.recovery()).thenCompose(evidence -> {
                    if (evidence == null) {
                        return emptyUnavailable(plan.recovery(), key).thenCompose(sources -> selectRepair(
                                plan.recovery(), value.getAllEnsembles().lastKey(), sources, key));
                    }
                    if (evidence.conflict()) {
                        finish(WorkerStatus.QUARANTINED);
                        return CompletableFuture.completedFuture(null);
                    }
                    return selectRepair(plan.recovery(), value.getAllEnsembles().lastKey(), evidence.sources(), key);
                });
            });
        }

        private CompletableFuture<List<ProfileRecoveryReadHandle.SourceEvidence>> emptyUnavailable(
                ZKProfileReservationStore.RecoveryResult accepted, ProtectedProfileCredential key) {
            return CompletableFuture.supplyAsync(() -> {
                var selected = reservations.readSelectedPrefix(metadata, accepted.context());
                long first = accepted.metadata().metadata().getValue().getAllEnsembles().lastKey();
                return selected.snapshot() != null && selected.snapshot().boundary().prefixEntryId() == first - 1;
            }, cold).thenCompose(empty -> {
                if (!empty || stopped.get()) {
                    return CompletableFuture.completedFuture(List.of());
                }
                return fenceOnce(accepted, options.controls(), key).thenApply(fenced -> {
                    if (fenced.status() == Status.CONFLICT || fenced.status() == Status.QUARANTINED) {
                        return List.of();
                    }
                    return accepted.metadata().metadata().getValue().getAllEnsembles().lastEntry().getValue().stream()
                            .filter(bookie -> !options.data().containsKey(bookie) || fenced.fences().stream()
                                .noneMatch(proof -> proof.bookie().equals(bookie.toString())))
                            .map(bookie -> new ProfileRecoveryReadHandle.SourceEvidence(bookie, "",
                                ProfileRecoveryReadHandle.SourceOutcome.UNAVAILABLE)).toList();
                });
            });
        }

        private CompletableFuture<ProfileRecoveryReadHandle.PointEvidence> probeLast(
                ZKProfileReservationStore.RecoveryResult accepted) {
            return CompletableFuture.supplyAsync(() -> {
                var selected = reservations.readSelectedPrefix(metadata, accepted.context());
                long first = accepted.metadata().metadata().getValue().getAllEnsembles().lastKey();
                return selected.snapshot() == null || selected.snapshot().boundary().prefixEntryId() < first
                        ? null : readers(accepted.context());
            }, cold).thenCompose(ready -> {
                if (ready == null || ready.recovery().context() == null || stopped.get()) {
                    return CompletableFuture.completedFuture(null);
                }
                var controls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
                options.controls().forEach((name, peer) -> controls.put(BookieId.parse(name), peer));
                try {
                    var value = ready.recovery().metadata().metadata().getValue();
                    var reader = ProfileRecoveryReadHandle.fromContext(sdk, ready.recovery(), ready.permitted(),
                            workerData(value, null), controls);
                    return reader.probeEvidence(value.getAllEnsembles().lastKey())
                            .whenComplete((result, failure) -> reader.closeAsync());
                } catch (java.security.GeneralSecurityException invalid) {
                    return CompletableFuture.failedFuture(invalid);
                }
            });
        }

        private CompletableFuture<Void> selectRepair(ZKProfileReservationStore.RecoveryResult accepted, long entry,
                List<ProfileRecoveryReadHandle.SourceEvidence> sources, ProtectedProfileCredential key) {
            if (stopped.get() || repairs >= options.maxRepairs() || entry < 0) {
                finish(WorkerStatus.DEFERRED);
                return CompletableFuture.completedFuture(null);
            }
            if (sources.stream().anyMatch(source -> source.outcome()
                    == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                finish(WorkerStatus.CONFLICT);
                return CompletableFuture.completedFuture(null);
            }
            var bad = sources.stream().filter(source -> source.outcome()
                    != ProfileRecoveryReadHandle.SourceOutcome.FOUND).findFirst().orElse(null);
            if (bad == null) {
                finish(WorkerStatus.DEFERRED);
                return CompletableFuture.completedFuture(null);
            }
            var value = accepted.metadata().metadata().getValue();
            int member = value.getAllEnsembles().floorEntry(entry).getValue().indexOf(bad.bookie());
            return CompletableFuture.supplyAsync(() -> {
                var retained = reservations.retainedActivationTargets(ledger);
                if (retained.status() != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus
                        .FOUND) {
                    return null;
                }
                var used = new java.util.HashSet<String>();
                retained.targets().forEach(receipt -> used.add(receipt.scope().targetBookie()));
                value.getAllEnsembles().values().forEach(ensemble ->
                        ensemble.forEach(bookie -> used.add(bookie.toString())));
                for (var candidate : options.data().keySet().stream()
                        .sorted(java.util.Comparator.comparing(BookieId::toString)).toList()) {
                    var peer = options.data().get(candidate);
                    var control = options.controls().get(candidate.toString());
                    if (used.contains(candidate.toString()) || control == null || peer.hello().activeEngine() != 3
                            || control.hello().activeEngine() != 3
                            || !peer.hello().bookieId().equals(candidate.toString())
                            || !control.hello().bookieId().equals(candidate.toString())
                            || !Arrays.equals(peer.hello().storageIncarnation(),
                                    control.hello().storageIncarnation())) {
                        continue;
                    }
                    return reservations.prepareWorkerRepair(metadata, accepted.context(), entry, member,
                            candidate.toString(), peer.hello().storageIncarnation());
                }
                return null;
            }, cold).thenCompose(plan -> {
                if (plan == null || plan.prepared() == null) {
                    finish(WorkerStatus.DEFERRED);
                    return CompletableFuture.completedFuture(null);
                }
                return runRepair(plan, key);
            });
        }

        private CompletableFuture<Void> runRepair(ZKProfileReservationStore.WorkerRepairPlan plan,
                ProtectedProfileCredential key) {
            var target = BookieId.parse(plan.prepared().intent().install().targetBookie());
            if (stopped.get() || (!plan.published() && repairs >= options.maxRepairs())
                    || !options.data().containsKey(target)
                    || !options.controls().containsKey(target.toString())) {
                finish(WorkerStatus.DEFERRED);
                return CompletableFuture.completedFuture(null);
            }
            var value = plan.recovery().metadata().metadata().getValue();
            boolean last = value.getAllEnsembles().higherKey(plan.prepared().intent().fragmentStart()) == null;
            return repairFragmentOnce(plan.recovery(), plan.prepared(), sdk, options.descriptor(), key,
                    workerData(value, target), options.controls(), plan.closeId(), last, stopped).thenAccept(result -> {
                copied += result.copiedEntries();
                if (result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                        || result.status() == RepairStatus.REPAIRED_LAST_FRAGMENT) {
                    if (!plan.published()) {
                        repairs++;
                    }
                    resolved.add(plan.prepared().intent().slot());
                    step();
                } else {
                    finish(result.status() == RepairStatus.DATA_LOSS ? WorkerStatus.DATA_LOSS
                            : result.status() == RepairStatus.QUARANTINED ? WorkerStatus.QUARANTINED
                                : result.status() == RepairStatus.CONFLICT ? WorkerStatus.CONFLICT
                                    : WorkerStatus.DEFERRED);
                }
            });
        }

        private Map<BookieId, ProfileNormalDataClient> workerData(
                org.apache.bookkeeper.client.api.LedgerMetadata value, BookieId extra) {
            var names = new java.util.HashSet<BookieId>();
            value.getAllEnsembles().values().forEach(names::addAll);
            if (extra != null) {
                names.add(extra);
            }
            var selected = new java.util.HashMap<BookieId, ProfileNormalDataClient>();
            names.forEach(name -> {
                var peer = options.data().get(name);
                if (peer != null) {
                    selected.put(name, peer);
                }
            });
            return selected;
        }
    }

    private static WorkerStatus workerStatus(RecoveryStatus status) {
        return switch (status) {
            case DELETED -> WorkerStatus.DELETED;
            case DATA_LOSS -> WorkerStatus.DATA_LOSS;
            case CONFLICT -> WorkerStatus.CONFLICT;
            case QUARANTINED -> WorkerStatus.QUARANTINED;
            default -> WorkerStatus.DEFERRED;
        };
    }

    public CompletableFuture<Result> fence(ZKProfileReservationStore.RecoveryResult accepted,
            Map<String, ProfileInitialControlClient> supplied, ProtectedProfileCredential credential) {
        if (accepted.context() == null) {
            throw new IllegalArgumentException("Fencing requires its accepted recovery context");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, accepted.context(), List.of()));
        }
        try {
            return fenceOnce(accepted, supplied, credential).whenComplete((value, failure) -> active.release()).copy();
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private CompletableFuture<Result> fenceOnce(ZKProfileReservationStore.RecoveryResult accepted,
            Map<String, ProfileInitialControlClient> supplied, ProtectedProfileCredential credential) {
        if (accepted.context() == null || supplied.size() > 64) {
            throw new IllegalArgumentException("Fencing requires a proved recovery context and bounded peer pool");
        }
        var context = accepted.context();
        boolean closedRepair = accepted.metadata().metadata().getValue().getState()
                == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED;
        var peers = Map.copyOf(supplied);
        Objects.requireNonNull(credential, "credential");
        CompletableFuture<Result> actual;
        try {
            actual = CompletableFuture.supplyAsync(() -> prepare(context, closedRepair), cold).thenCompose(prepared -> {
                if (prepared.recovery().context() == null
                        || !context.equals(prepared.recovery().context())) {
                    return CompletableFuture.completedFuture(new Result(
                            failureStatus(prepared.recovery().outcome()), context, List.of()));
                }
                var value = prepared.recovery().metadata().metadata().getValue();
                var ensemble = value.getAllEnsembles().lastEntry().getValue();
                List<CompletableFuture<Reply>> pending = new ArrayList<>();
                for (var bookie : ensemble) {
                    String name = bookie.toString();
                    var peer = peers.get(name);
                    boolean matches = peer != null && peer.hello().activeEngine() == 3
                            && peer.hello().bookieId().equals(name)
                            && prepared.targets().stream().anyMatch(receipt ->
                                receipt.scope().targetBookie().equals(name)
                                    && Arrays.equals(receipt.scope().targetStorageIncarnation(),
                                            peer.hello().storageIncarnation()));
                    if (!matches) {
                        pending.add(CompletableFuture.completedFuture(new Reply(name, peer, null, null)));
                        continue;
                    }
                    try {
                        pending.add(peer.fence(context.ledger(), context.operationId(), credential)
                                .handle((status, failure) -> new Reply(name, peer, status, failure)));
                    } catch (RuntimeException failure) {
                        pending.add(CompletableFuture.completedFuture(new Reply(name, peer, null, failure)));
                    }
                }
                return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).thenCompose(unused -> {
                    var replies = pending.stream().map(CompletableFuture::join).toList();
                    return CompletableFuture.supplyAsync(() -> {
                        var coverage = new RoundRobinDistributionSchedule(value.getWriteQuorumSize(),
                                value.getAckQuorumSize(), value.getEnsembleSize()).getCoverageSet();
                        List<Fence> proofs = new ArrayList<>();
                        for (int i = 0; i < replies.size(); i++) {
                            var reply = replies.get(i);
                            boolean fenced = reply.failure() == null && reply.status() != null
                                    && reply.status().statusClass() == ProfileStatus.StatusClass.OK
                                    && (reply.status().durableResult() == ProfileStatus.DurableResult.APPLIED
                                        || reply.status().durableResult()
                                            == ProfileStatus.DurableResult.ALREADY_APPLIED);
                            coverage.addBookie(i, fenced ? BKException.Code.OK : BKException.Code.ReadException);
                            if (fenced) {
                                proofs.add(new Fence(reply.bookie(),
                                        HexFormat.of().formatHex(reply.peer().hello().storageIncarnation())));
                            }
                        }
                        var current = closedRepair ? reservations.readRepairable(metadata, context.ledger().ledgerId())
                                : reservations.readRecovery(metadata, context.ledger().ledgerId());
                        if (current.context() == null || !context.equals(current.context())) {
                            return new Result(failureStatus(current.outcome()), context, proofs);
                        }
                        return new Result(coverage.checkCovered() ? Status.FENCED_CONTEXT : Status.DEFERRED,
                                context, proofs);
                    }, cold);
                });
            });
        } catch (RejectedExecutionException rejected) {
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, context, List.of()));
        }
        return actual;
    }

    public enum ObservationStatus { OBSERVED_BOUNDARY, DEFERRED, CONFLICT, QUARANTINED }

    /** Read-only observations. Missing above the hint is not a final normal-tail or CLOSED authority. */
    public record Observation(ObservationStatus status, ProfileRecoveryContext context,
            org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence lac,
            List<ProfileRecoveryReadHandle.Probe> candidates, long missingEntry) {
        public Observation {
            candidates = List.copyOf(candidates);
        }
    }

    private record Readers(ZKProfileReservationStore.RecoveryResult recovery, List<ProfileInstallReceipt> permitted) { }

    public CompletableFuture<Observation> observe(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCandidates) {
        if (accepted.context() == null) {
            throw new IllegalArgumentException("Observation requires its accepted recovery context");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new Observation(ObservationStatus.DEFERRED,
                    accepted.context(), null, List.of(), -1));
        }
        try {
            return observeOnce(accepted, sdk, suppliedData, suppliedControl, credential, maxCandidates)
                    .whenComplete((value, failure) -> active.release()).copy();
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private CompletableFuture<Observation> observeOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCandidates) {
        if (maxCandidates < 1 || maxCandidates > 32 || suppliedData.size() > 64) {
            throw new IllegalArgumentException("Recovery observation requires a bounded candidate window and peers");
        }
        var data = Map.copyOf(suppliedData);
        var control = Map.copyOf(suppliedControl);
        var context = accepted.context();
        return fenceOnce(accepted, control, credential).thenCompose(fenced -> {
            if (fenced.status() != Status.FENCED_CONTEXT) {
                return CompletableFuture.completedFuture(new Observation(
                        observationStatus(fenced.status()), context, null, List.of(), -1));
            }
            return CompletableFuture.supplyAsync(() -> readers(context), cold).thenCompose(readers -> {
                if (readers.recovery().context() == null || !context.equals(readers.recovery().context())) {
                    return CompletableFuture.completedFuture(new Observation(observationStatus(
                            failureStatus(readers.recovery().outcome())), context, null, List.of(), -1));
                }
                var controls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
                control.forEach((name, peer) -> controls.put(BookieId.parse(name), peer));
                ProfileRecoveryReadHandle reader;
                try {
                    reader = ProfileRecoveryReadHandle.fromContext(sdk,
                            readers.recovery(), readers.permitted(), data, controls);
                } catch (java.security.GeneralSecurityException invalid) {
                    return CompletableFuture.failedFuture(invalid);
                }
                return reader.readLacEvidence().thenCompose(lac -> scan(reader, context, lac,
                        new ArrayList<>(), lac.prefixEntryId(), lac.prefixLength(), maxCandidates))
                        .thenCompose(observation -> CompletableFuture.supplyAsync(() -> {
                            var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                            return current.context() != null && context.equals(current.context()) ? observation
                                    : new Observation(observationStatus(failureStatus(current.outcome())), context,
                                            observation.lac(), observation.candidates(), -1);
                        }, cold)).whenComplete((value, failure) -> reader.closeAsync());
            });
        });
    }

    private Readers readers(ProfileRecoveryContext context) {
        return readers(context, false);
    }

    private Readers readers(ProfileRecoveryContext context, boolean closedRepair) {
        var current = closedRepair ? reservations.readRepairable(metadata, context.ledger().ledgerId())
                : reservations.readRecovery(metadata, context.ledger().ledgerId());
        if (current.context() == null || !context.equals(current.context())) {
            return new Readers(current, List.of());
        }
        var retained = reservations.retainedActivationTargets(context.ledger().ledgerId());
        if (retained.status()
                != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND) {
            return new Readers(new ZKProfileReservationStore.RecoveryResult(retained.status()
                    == org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.UNAVAILABLE
                        ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        : ProfileLedgerMetadataStore.Outcome.QUARANTINED, null, null), List.of());
        }
        var currentTargets = prepare(context, closedRepair).targets();
        var permitted = new java.util.LinkedHashMap<String, ProfileInstallReceipt>();
        for (var receipt : retained.targets()) {
            var old = permitted.put(receipt.scope().targetBookie(), receipt);
            if (old != null && !Arrays.equals(old.scope().targetStorageIncarnation(),
                    receipt.scope().targetStorageIncarnation())) {
                return new Readers(new ZKProfileReservationStore.RecoveryResult(
                        ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null), List.of());
            }
        }
        currentTargets.forEach(receipt -> permitted.put(receipt.scope().targetBookie(), receipt));
        return new Readers(current, List.copyOf(permitted.values()));
    }

    private CompletableFuture<Observation> scan(ProfileRecoveryReadHandle reader,
            ProfileRecoveryContext context, org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence lac,
            List<ProfileRecoveryReadHandle.Probe> candidates,
            long prefix, long length, int remaining) {
        if (remaining == 0 || prefix == Long.MAX_VALUE) {
            return CompletableFuture.completedFuture(new Observation(ObservationStatus.DEFERRED,
                    context, lac, candidates, -1));
        }
        return reader.probe(prefix + 1).thenCompose(probe -> {
            if (probe.outcome() == ProfileRecoveryReadHandle.ProbeOutcome.ABSENT) {
                return CompletableFuture.completedFuture(new Observation(ObservationStatus.OBSERVED_BOUNDARY,
                        context, lac, candidates, prefix + 1));
            }
            int payload = probe.payload().remaining();
            if (length > Long.MAX_VALUE - payload || probe.length() != length + payload) {
                return CompletableFuture.completedFuture(new Observation(ObservationStatus.QUARANTINED,
                        context, lac, candidates, -1));
            }
            candidates.add(probe);
            return scan(reader, context, lac, candidates, prefix + 1, probe.length(), remaining - 1);
        });
    }

    private static ObservationStatus observationStatus(Status status) {
        return switch (status) {
            case CONFLICT -> ObservationStatus.CONFLICT;
            case QUARANTINED -> ObservationStatus.QUARANTINED;
            default -> ObservationStatus.DEFERRED;
        };
    }

    public enum PrefixStatus {
        VERIFIED_NORMAL_TAIL, VERIFIED_WRITER_PREFIX, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS
    }

    public enum PrefixReason {
        NORMAL_TAIL, WRITER_CLOSE, LIMIT, CANCELLED, DEADLINE, LAC_UNAVAILABLE, SOURCE_UNAVAILABLE, REQUIRED_HOLE,
        PAYLOAD_CONFLICT, LENGTH_CONFLICT, PERMISSION, AUTHORITY_CHANGED,
        AUTHORITY_UNAVAILABLE, AUTHORITY_UNRECOVERABLE
    }

    /** Read evidence only. Required recovery Adds and matching durable CLOSED still precede recovery success. */
    public record PrefixResult(PrefixStatus status, PrefixReason reason, ProfileRecoveryContext context,
            ProfileLedgerMetadataStore.MembershipSnapshot metadata, long requiredFrontier,
            long prefixEntryId, long prefixLength, long unresolvedEntry,
            List<ProfileRecoveryReadHandle.SourceEvidence> sources) {
        public PrefixResult {
            sources = List.copyOf(sources);
        }
    }

    /** Check the complete required prefix and then the open tail, holding one payload and one exact source set. */
    public CompletableFuture<PrefixResult> verifyPrefix(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCoordinates, Duration maxDuration) {
        long durationNanos = maxDuration.toNanos();
        if (accepted.context() == null || maxCoordinates < 1
                || suppliedData.size() > 64 || suppliedControl.size() > 64 || durationNanos <= 0) {
            throw new IllegalArgumentException("Prefix verification requires its frozen context and finite bounds");
        }
        var context = accepted.context();
        if (ProfileRecoveryBoundary.writerClose(accepted.metadata().metadata().getValue()) == null
                && suppliedData.values().stream().anyMatch(peer -> !peer.hello().capabilities().contains(
                org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.EXPLICIT_CAPABILITY))) {
            return CompletableFuture.completedFuture(new PrefixResult(PrefixStatus.DEFERRED,
                    PrefixReason.LAC_UNAVAILABLE, context, null, -1, -1, 0, 0, List.of()));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.LIMIT,
                    context, null, -1, -1, 0, 0, List.of()));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = verifyPrefixOnce(accepted, sdk, suppliedData, suppliedControl, credential,
                    maxCoordinates, stopped);
            var caller = actual.whenComplete((result, failure) -> active.release()).copy();
            java.util.concurrent.ScheduledFuture<?> deadline;
            try {
                deadline = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.DEADLINE,
                            context, null, -1, -1, 0, 0, List.of()));
                }, durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.LIMIT,
                        context, null, -1, -1, 0, 0, List.of()));
                return caller;
            }
            actual.whenComplete((result, failure) -> deadline.cancel(false));
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<PrefixResult> verifyPrefixOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCoordinates, AtomicBoolean stopped) {
        var context = accepted.context();
        var data = Map.copyOf(suppliedData);
        var control = Map.copyOf(suppliedControl);
        return fenceOnce(accepted, control, credential).thenCompose(fenced -> {
            if (stopped.get() || fenced.status() != Status.FENCED_CONTEXT) {
                return CompletableFuture.completedFuture(new PrefixResult(stopped.get()
                        ? PrefixStatus.DEFERRED : prefixStatus(fenced.status()), stopped.get()
                            ? PrefixReason.CANCELLED : PrefixReason.SOURCE_UNAVAILABLE,
                        context, null, -1, -1, 0, 0, List.of()));
            }
            return CompletableFuture.supplyAsync(() -> readers(context), cold).thenCompose(ready -> {
                if (ready.recovery().context() == null || !context.equals(ready.recovery().context())) {
                    return CompletableFuture.completedFuture(new PrefixResult(
                            prefixStatus(failureStatus(ready.recovery().outcome())),
                            authorityReason(ready.recovery().outcome()), context, null, -1, -1, 0, 0, List.of()));
                }
                var controls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
                control.forEach((name, peer) -> controls.put(BookieId.parse(name), peer));
                ProfileRecoveryReadHandle reader;
                try {
                    reader = ProfileRecoveryReadHandle.fromContext(sdk, ready.recovery(), ready.permitted(),
                            data, controls);
                } catch (java.security.GeneralSecurityException invalid) {
                    return CompletableFuture.failedFuture(invalid);
                }
                var verify = new PrefixVerification(reader, context, ready.recovery().metadata(),
                        stopped, maxCoordinates);
                CompletableFuture<PrefixResult> checked;
                if (verify.writerClose != null) {
                    verify.required = verify.writerClose.prefixEntryId();
                    verify.start();
                    checked = verify.done;
                } else {
                    checked = reader.readLacEvidence().handle((lac, failure) -> {
                        if (failure != null) {
                            if (!(cause(failure) instanceof BKException)) {
                                verify.done.completeExceptionally(failure);
                                return verify.done;
                            }
                            boolean conflict = cause(failure)
                                    instanceof org.apache.bookkeeper.client.ProfileLedgerHandle
                                        .LacLengthConflictException;
                            boolean unauthorized = BKException.getExceptionCode(cause(failure))
                                    == BKException.Code.UnauthorizedAccessException;
                            verify.finish(conflict ? PrefixStatus.QUARANTINED : unauthorized
                                    ? PrefixStatus.CONFLICT : PrefixStatus.DEFERRED,
                                    conflict ? PrefixReason.LENGTH_CONFLICT : unauthorized
                                        ? PrefixReason.PERMISSION : PrefixReason.LAC_UNAVAILABLE, List.of());
                        } else {
                            verify.required = lac.prefixEntryId();
                            verify.start();
                        }
                        return verify.done;
                    }).thenCompose(value -> value);
                }
                return checked.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                    var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                    if (current.context() == null || !context.equals(current.context())
                            || current.metadata().birthZxid() != result.metadata().birthZxid()
                            || !current.metadata().metadata().getVersion()
                                .equals(result.metadata().metadata().getVersion())
                            || !ProfileLedgerMetadataStore.sameMetadata(
                                    result.metadata().metadata().getValue(),
                                    current.metadata().metadata().getValue())) {
                        return new PrefixResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                ? PrefixStatus.QUARANTINED : current.outcome()
                                    == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                                        ? PrefixStatus.DEFERRED : PrefixStatus.CONFLICT,
                                authorityReason(current.outcome()), context, result.metadata(),
                                result.requiredFrontier(),
                                result.prefixEntryId(), result.prefixLength(), result.unresolvedEntry(),
                                result.sources());
                    }
                    return result;
                }, cold)).whenComplete((result, failure) -> reader.closeAsync());
            });
        });
    }

    private static PrefixStatus prefixStatus(Status status) {
        return switch (status) {
            case CONFLICT -> PrefixStatus.CONFLICT;
            case QUARANTINED -> PrefixStatus.QUARANTINED;
            default -> PrefixStatus.DEFERRED;
        };
    }

    private static PrefixReason authorityReason(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case DURABILITY_UNKNOWN -> PrefixReason.AUTHORITY_UNAVAILABLE;
            case QUARANTINED -> PrefixReason.AUTHORITY_UNRECOVERABLE;
            default -> PrefixReason.AUTHORITY_CHANGED;
        };
    }

    private static Throwable cause(Throwable failure) {
        while ((failure instanceof java.util.concurrent.CompletionException
                || failure instanceof java.util.concurrent.ExecutionException) && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static final class PrefixVerification {
        private final ProfileRecoveryReadHandle reader;
        private final ProfileRecoveryContext context;
        private final ProfileLedgerMetadataStore.MembershipSnapshot metadata;
        private final AtomicBoolean stopped;
        private final ProfileRecoveryBoundary.WriterClose writerClose;
        private int remaining;
        private long required = -1;
        private long next;
        private long length;
        private final CompletableFuture<PrefixResult> done = new CompletableFuture<>();

        PrefixVerification(ProfileRecoveryReadHandle reader, ProfileRecoveryContext context,
                ProfileLedgerMetadataStore.MembershipSnapshot metadata, AtomicBoolean stopped, int bound) {
            this.reader = reader;
            this.context = context;
            this.metadata = metadata;
            this.stopped = stopped;
            remaining = bound;
            writerClose = ProfileRecoveryBoundary.writerClose(metadata.metadata().getValue());
        }

        void start() {
            if (!stopped.get() && writerClose != null && next > writerClose.prefixEntryId()) {
                finish(length == writerClose.prefixLength() ? PrefixStatus.VERIFIED_WRITER_PREFIX
                        : PrefixStatus.QUARANTINED, length == writerClose.prefixLength() ? PrefixReason.WRITER_CLOSE
                            : PrefixReason.LENGTH_CONFLICT, List.of());
                return;
            }
            if (stopped.get() || remaining-- == 0 || next == Long.MAX_VALUE) {
                finish(PrefixStatus.DEFERRED, stopped.get() ? PrefixReason.CANCELLED : PrefixReason.LIMIT, List.of());
                return;
            }
            reader.probeEvidence(next).whenComplete((evidence, failure) -> {
                if (failure != null && !(cause(failure) instanceof BKException)) {
                    done.completeExceptionally(failure);
                } else if (failure != null || stopped.get()) {
                    finish(PrefixStatus.DEFERRED, stopped.get() ? PrefixReason.CANCELLED
                            : PrefixReason.SOURCE_UNAVAILABLE, List.of());
                } else if (evidence.sources().stream().anyMatch(source -> source.outcome()
                        == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                    finish(PrefixStatus.CONFLICT, PrefixReason.PERMISSION, evidence.sources());
                } else if (evidence.conflict()) {
                    finish(PrefixStatus.QUARANTINED, PrefixReason.PAYLOAD_CONFLICT, evidence.sources());
                } else if (evidence.candidate() != null) {
                    var candidate = evidence.candidate();
                    int bytes = candidate.payload().remaining();
                    if (length > Long.MAX_VALUE - bytes || candidate.length() != length + bytes) {
                        finish(PrefixStatus.QUARANTINED, PrefixReason.LENGTH_CONFLICT, evidence.sources());
                    } else {
                        length = candidate.length();
                        next++;
                        start(); // Each probe uses the original ordered executor; no range-sized future chain.
                    }
                } else if (next <= required) {
                    finish(evidence.exhausted() ? PrefixStatus.DATA_LOSS : PrefixStatus.DEFERRED,
                            PrefixReason.REQUIRED_HOLE, evidence.sources());
                } else {
                    finish(evidence.absenceCovered() ? PrefixStatus.VERIFIED_NORMAL_TAIL : PrefixStatus.DEFERRED,
                            evidence.absenceCovered() ? PrefixReason.NORMAL_TAIL : PrefixReason.SOURCE_UNAVAILABLE,
                            evidence.sources());
                }
            });
        }

        void finish(PrefixStatus status, PrefixReason reason, List<ProfileRecoveryReadHandle.SourceEvidence> sources) {
            done.complete(new PrefixResult(status, reason, context, metadata, required,
                    next - 1, length, next, sources));
        }
    }

    private Prepared prepare(ProfileRecoveryContext context) {
        return prepare(context, false);
    }

    private Prepared prepare(ProfileRecoveryContext context, boolean closedRepair) {
        if (closedRepair) {
            var closed = reservations.readClosedForOpen(metadata, context.ledger().ledgerId());
            if (closed.snapshot() == null || !context.equals(closed.snapshot().context())) {
                return new Prepared(new ZKProfileReservationStore.RecoveryResult(closed.status()
                        == ZKProfileReservationStore.ReadOnlyStatus.UNAVAILABLE
                            ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                            : ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null), List.of());
            }
            // Include committed repair targets, which were installed inactive and have no normal activation page.
            return new Prepared(new ZKProfileReservationStore.RecoveryResult(
                    ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, context, closed.snapshot().closed().metadata()),
                    closed.snapshot().sources());
        }
        var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
        if (current.context() == null || !context.equals(current.context())) {
            return new Prepared(current, List.of());
        }
        try {
            return new Prepared(current, reservations.recoveryCurrentTargets(context,
                    System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(1)));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CompletionException(interrupted);
        } catch (java.util.concurrent.TimeoutException unavailable) {
            throw new java.util.concurrent.CompletionException(unavailable);
        }
    }

    public enum RecoveryStatus { RECOVERED_AND_CLOSED, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS, DELETED }

    public record RecoveryResult(long ledgerId, RecoveryStatus status, PrefixReason reason,
            ProfileRecoveryContext context, long prefixEntryId, long prefixLength, PrefixResult evidence,
            ZKProfileReservationStore.ClosedRecoveryResult publication) { }

    private record RecoveryStart(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.ClosedRecoveryResult closed, RecoveryStatus failure, PrefixReason reason) {
        RecoveryStart(ZKProfileReservationStore.RecoveryResult accepted,
                ZKProfileReservationStore.ClosedRecoveryResult closed, RecoveryStatus failure) {
            this(accepted, closed, failure, failure == RecoveryStatus.DEFERRED ? PrefixReason.AUTHORITY_UNAVAILABLE
                    : failure == RecoveryStatus.QUARANTINED ? PrefixReason.AUTHORITY_UNRECOVERABLE
                        : PrefixReason.AUTHORITY_CHANGED);
        }
    }

    /** One actual attempt owns the existing gate across metadata, point oracle, SELF write-back and final close. */
    public CompletableFuture<RecoveryResult> recover(long ledger, byte[] operationId, ClientContext sdk,
            byte[] canonicalDescriptor, org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration) {
        return recover(ledger, operationId, sdk, canonicalDescriptor, validator, credential, suppliedData,
                suppliedControl, maxCoordinates, maxDuration, null);
    }

    /** Local SDK stop supplies this one immutable confirmed cut; durable publication remains separate. */
    public record WriterCloseRequest(org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext ledger,
            org.apache.bookkeeper.versioning.Versioned<org.apache.bookkeeper.client.api.LedgerMetadata> metadata,
            long prefixEntryId, long prefixLength, byte[] operationId) {
        public WriterCloseRequest {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(metadata, "metadata");
            operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
            new ProfileRecoveryBoundary.WriterClose(prefixEntryId, prefixLength);
            if (metadata.getValue().getState() != org.apache.bookkeeper.client.api.LedgerMetadata.State.OPEN
                    || !(metadata.getVersion() instanceof org.apache.bookkeeper.versioning.LongVersion)
                    || metadata.getValue().getLedgerId() != ledger.ledgerId()
                    || prefixEntryId < metadata.getValue().getAllEnsembles().lastKey() - 1) {
                throw new IllegalArgumentException("Writer close requires its exact OPEN snapshot and confirmed cut");
            }
        }
        @Override
        public byte[] operationId() {
            return operationId.clone();
        }
        @Override
        public String toString() {
            return "WriterCloseRequest{ledger=" + ledger.ledgerId() + ", prefix=" + prefixEntryId + '}';
        }
    }

    /** Ordinary close freezes P/L in the same IN_RECOVERY CAS, then reuses fixed-prefix terminal close. */
    public CompletableFuture<RecoveryResult> closeWriter(WriterCloseRequest request, ClientContext sdk,
            byte[] canonicalDescriptor, org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration) {
        return recover(request.ledger().ledgerId(), request.operationId(), sdk, canonicalDescriptor, validator,
                credential, suppliedData, suppliedControl, maxCoordinates, maxDuration, request);
    }

    private CompletableFuture<RecoveryResult> recover(long ledger, byte[] operationId, ClientContext sdk,
            byte[] canonicalDescriptor, org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration,
            WriterCloseRequest writerRequest) {
        long duration = maxDuration.toNanos();
        if (ledger < 0 || maxCoordinates < 1 || suppliedData.size() > 64 || suppliedControl.size() > 64
                || duration <= 0 || maxDuration.compareTo(Duration.ofMinutes(1)) > 0
                || canonicalDescriptor.length > org.apache.bookkeeper.common.profile.ProfileDescriptorCodec
                    .MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Recovery requires bounded peers, descriptor, coordinates and duration");
        }
        ProfileRecoveryReadHandle.requireCompatibleSdk(sdk);
        if (sdk.isClientClosed()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new RecoveryResult(ledger, RecoveryStatus.DEFERRED,
                    PrefixReason.LIMIT, null, -1, 0, null, null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<RecoveryResult>();
        var progress = new java.util.concurrent.atomic.AtomicReference<>(new RecoveryResult(ledger,
                RecoveryStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE, null, -1, 0, null, null));
        try {
            byte[] operation = ProfileLifecycleHead.fixed(operationId, 16, true);
            byte[] descriptor = canonicalDescriptor.clone();
            var decoded = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.decode(descriptor, validator);
            byte[] identity = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.identityOf(decoded, validator)
                    .toBytes();
            var key = credential.copy();
            var data = Map.copyOf(suppliedData);
            var controls = Map.copyOf(suppliedControl);
            var actual = recoverOnce(ledger, operation, sdk, descriptor, decoded, identity, key, data,
                    controls, maxCoordinates, stopped, progress, writerRequest);
            var timer = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else {
                    caller.complete(result);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    var known = progress.get();
                    caller.complete(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.DEADLINE,
                            known.context(), known.prefixEntryId(), known.prefixLength(), known.evidence(), null));
                }, duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.LIMIT,
                        null, -1, 0, null, null));
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private CompletableFuture<RecoveryResult> recoverOnce(long ledger, byte[] operation, ClientContext sdk,
            byte[] descriptor, org.apache.bookkeeper.common.profile.ProfileDescriptor decoded, byte[] identity,
            ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, int maxCoordinates, AtomicBoolean stopped,
            java.util.concurrent.atomic.AtomicReference<RecoveryResult> progress) {
        return recoverOnce(ledger, operation, sdk, descriptor, decoded, identity, key, data, controls,
                maxCoordinates, stopped, progress, null);
    }

    private CompletableFuture<RecoveryResult> recoverOnce(long ledger, byte[] operation, ClientContext sdk,
            byte[] descriptor, org.apache.bookkeeper.common.profile.ProfileDescriptor decoded, byte[] identity,
            ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, int maxCoordinates, AtomicBoolean stopped,
            java.util.concurrent.atomic.AtomicReference<RecoveryResult> progress, WriterCloseRequest writerRequest) {
        return CompletableFuture.supplyAsync(
                () -> startRecovery(ledger, operation, decoded, identity, key, stopped, writerRequest), cold)
                .thenCompose(start -> {
            if (start.closed() != null) {
                return CompletableFuture.completedFuture(new RecoveryResult(ledger,
                        RecoveryStatus.RECOVERED_AND_CLOSED, PrefixReason.NORMAL_TAIL,
                        start.accepted().context(),
                        start.closed().metadata().metadata().getValue().getLastEntryId(),
                        start.closed().metadata().metadata().getValue().getLength(), null, start.closed()));
            }
            if (start.accepted() == null || start.accepted().context() == null) {
                return CompletableFuture.completedFuture(new RecoveryResult(ledger, start.failure(),
                        start.reason(), null, -1, 0, null, null));
            }
            var accepted = start.accepted();
            var context = accepted.context();
            progress.set(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE,
                    context, -1, 0, null, null));
            return CompletableFuture.supplyAsync(() -> reservations.readSelectedPrefix(metadata, context), cold)
                    .thenCompose(selected -> {
                if (stopped.get()) {
                    return CompletableFuture.completedFuture(progress.get());
                }
                if (selected.snapshot() != null) {
                    return recoverSelected(accepted, selected.snapshot(), null, sdk, descriptor, key, data,
                            controls, stopped, progress);
                }
                if (selected.status() != ZKProfileReservationStore.CreateStatus.CONFLICT) {
                    return CompletableFuture.completedFuture(recoveryFailure(ledger, context,
                            completionStatus(selected.status()), null));
                }
                if (ProfileRecoveryBoundary.writerClose(accepted.metadata().metadata().getValue()) == null
                        && data.values().stream().anyMatch(peer -> !peer.hello().capabilities().contains(
                        org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.EXPLICIT_CAPABILITY))) {
                    return CompletableFuture.completedFuture(progress.get());
                }
                return verifyPrefixOnce(accepted, sdk, data, controls, key, maxCoordinates, stopped)
                        .thenCompose(proof -> {
                    if (proof.status() != PrefixStatus.VERIFIED_NORMAL_TAIL
                            && proof.status() != PrefixStatus.VERIFIED_WRITER_PREFIX || stopped.get()) {
                        return CompletableFuture.completedFuture(new RecoveryResult(ledger,
                                stopped.get() || proof.status() == PrefixStatus.DEFERRED ? RecoveryStatus.DEFERRED
                                    : proof.status() == PrefixStatus.DATA_LOSS ? RecoveryStatus.DATA_LOSS
                                        : proof.status() == PrefixStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                                            : RecoveryStatus.CONFLICT,
                                proof.reason(), context, proof.prefixEntryId(), proof.prefixLength(), proof, null));
                    }
                    return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                            : reservations.selectPrefix(metadata, proof), cold).thenCompose(published -> {
                        if (published == null || published.snapshot() == null) {
                            return CompletableFuture.completedFuture(recoveryFailure(ledger, context,
                                    published == null ? CompletionStatus.DEFERRED
                                        : completionStatus(published.status()), proof));
                        }
                        return recoverSelected(accepted, published.snapshot(), proof, sdk, descriptor, key, data,
                                controls, stopped, progress);
                    });
                });
            });
        });
    }

    private RecoveryStart startRecovery(long ledger, byte[] operation,
            org.apache.bookkeeper.common.profile.ProfileDescriptor descriptor, byte[] identity,
            ProtectedProfileCredential key, AtomicBoolean stopped, WriterCloseRequest writerRequest) {
        if (stopped.get()) {
            return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
        }
        var member = metadata.readMembership(ledger);
        if (member.status() != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND) {
            return new RecoveryStart(null, null, member.status()
                    == org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.UNAVAILABLE
                        ? RecoveryStatus.DEFERRED : RecoveryStatus.QUARANTINED);
        }
        var value = member.snapshot().metadata().getValue();
        byte[] masterKey;
        try {
            masterKey = org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(value.getPassword());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new java.util.concurrent.CompletionException(impossible);
        }
        if (value.getDigestType() != org.apache.bookkeeper.client.api.DigestType.CRC32C
                || value.getEnsembleSize() != 3 || value.getWriteQuorumSize() != 3
                || value.getAckQuorumSize() < 2 || value.getAckQuorumSize() > 3
                || descriptor.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || descriptor.ensembleSize() != value.getEnsembleSize()
                || descriptor.writeQuorumSize() != value.getWriteQuorumSize()
                || descriptor.ackQuorumSize() != value.getAckQuorumSize()) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        if (!key.matchesWireVerifier(masterKey)) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT, PrefixReason.PERMISSION);
        }
        if (value.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED) {
            var read = reservations.readClosedForOpen(metadata, ledger);
            if (read.snapshot() == null) {
                return new RecoveryStart(null, null, read.status() == ZKProfileReservationStore.ReadOnlyStatus.DELETED
                        ? RecoveryStatus.DELETED : read.status() == ZKProfileReservationStore.ReadOnlyStatus.UNAVAILABLE
                            ? RecoveryStatus.DEFERRED : RecoveryStatus.QUARANTINED);
            }
            var snapshot = read.snapshot();
            if (writerRequest != null && !writerRequestMatches(writerRequest, snapshot.context(),
                    snapshot.closed().metadata().metadata().getValue())) {
                return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
            }
            return Arrays.equals(identity, snapshot.context().ledger().descriptorIdentity())
                    ? new RecoveryStart(new ZKProfileReservationStore.RecoveryResult(
                        ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, snapshot.context(),
                        snapshot.closed().metadata()), snapshot.closed(), null)
                    : new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        if (stopped.get()) {
            return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
        }
        ZKProfileReservationStore.RecoveryResult accepted;
        if (value.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.IN_RECOVERY) {
            accepted = reservations.readRecovery(metadata, ledger);
        } else {
            if (writerRequest != null && (!member.snapshot().metadata().getVersion()
                    .equals(writerRequest.metadata().getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(value, writerRequest.metadata().getValue()))) {
                return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
            }
            var cut = writerRequest == null ? null : new ProfileRecoveryBoundary.WriterClose(
                    writerRequest.prefixEntryId(), writerRequest.prefixLength());
            var captured = reservations.captureRecovery(metadata, ledger, operation, cut);
            if (captured.plan() == null || !Arrays.equals(identity,
                    captured.plan().context().ledger().descriptorIdentity()) || writerRequest != null
                        && (!writerRequest.ledger().equals(captured.plan().context().ledger())
                            || !captured.plan().predecessor().metadata().getVersion()
                            .equals(writerRequest.metadata().getVersion())
                            || !ProfileLedgerMetadataStore.sameMetadata(
                                captured.plan().predecessor().metadata().getValue(),
                                writerRequest.metadata().getValue()))) {
                return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
            }
            if (stopped.get()) {
                return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
            }
            accepted = reservations.beginRecovery(metadata, captured.plan(), cut);
        }
        if (writerRequest != null && accepted.context() != null && !writerRequestMatches(writerRequest,
                accepted.context(), accepted.metadata().metadata().getValue())) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        if (accepted.context() != null && !Arrays.equals(identity, accepted.context().ledger().descriptorIdentity())) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        return new RecoveryStart(accepted, null, accepted.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                ? RecoveryStatus.QUARANTINED : accepted.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                    ? RecoveryStatus.CONFLICT : RecoveryStatus.DEFERRED);
    }

    private static boolean writerRequestMatches(WriterCloseRequest request, ProfileRecoveryContext context,
            org.apache.bookkeeper.client.api.LedgerMetadata current) {
        var cut = ProfileRecoveryBoundary.writerClose(current);
        try {
            return cut != null && cut.prefixEntryId() == request.prefixEntryId()
                    && cut.prefixLength() == request.prefixLength() && context.ledger().equals(request.ledger())
                    && Arrays.equals(context.operationId(), request.operationId())
                    && context.predecessorVersion() == ((org.apache.bookkeeper.versioning.LongVersion)
                        request.metadata().getVersion()).getLongVersion()
                    && context.creatorToken() == request.metadata().getValue().getCToken()
                    && Arrays.equals(context.predecessorDigest(), ProfileRecoveryContext.metadataDigest(
                        cut.attachTo(request.metadata().getValue())));
        } catch (java.io.IOException invalid) {
            return false;
        }
    }

    private CompletableFuture<RecoveryResult> recoverSelected(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.BoundarySnapshot selected, PrefixResult proof, ClientContext sdk,
            byte[] descriptor, ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, AtomicBoolean stopped,
            java.util.concurrent.atomic.AtomicReference<RecoveryResult> progress) {
        var context = accepted.context();
        progress.set(new RecoveryResult(context.ledger().ledgerId(), RecoveryStatus.DEFERRED,
                PrefixReason.SOURCE_UNAVAILABLE, context, selected.boundary().prefixEntryId(),
                selected.boundary().prefixLength(), proof, null));
        return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, 0)
                .thenCompose(failed -> {
            if (failed != null || stopped.get()) {
                return CompletableFuture.completedFuture(failed == null ? progress.get() : failed);
            }
            return finishRecoveryOnce(context, controls, stopped).thenApply(closed ->
                    closed.status() == CompletionStatus.CLOSED_METADATA_DURABLE
                        ? new RecoveryResult(context.ledger().ledgerId(), RecoveryStatus.RECOVERED_AND_CLOSED,
                            PrefixReason.NORMAL_TAIL, context, selected.boundary().prefixEntryId(),
                            selected.boundary().prefixLength(), proof, closed.publication())
                        : recoveryFailure(context.ledger().ledgerId(), context, closed.status(), proof));
        });
    }

    private CompletableFuture<RecoveryResult> recoverMembers(ProfileRecoveryContext context,
            ZKProfileReservationStore.BoundarySnapshot selected, ClientContext sdk, byte[] descriptor,
            ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, AtomicBoolean stopped, int member) {
        if (member == 3 || stopped.get()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.supplyAsync(
                () -> reservations.readRecovery(metadata, context.ledger().ledgerId()), cold)
                .thenCompose(current -> {
            if (current.context() == null || !context.equals(current.context())) {
                return CompletableFuture.completedFuture(recoveryFailure(context.ledger().ledgerId(), context,
                        current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                            ? CompletionStatus.QUARANTINED : CompletionStatus.CONFLICT, null));
            }
            var value = current.metadata().metadata().getValue();
            if (selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey()) {
                return CompletableFuture.completedFuture(null);
            }
            String name = value.getAllEnsembles().lastEntry().getValue().get(member).toString();
            if (!data.containsKey(BookieId.parse(name)) || !controls.containsKey(name)) {
                return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, member + 1);
            }
            return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                    : reservations.prepareLastSelfRepair(metadata, context, member), cold).thenCompose(plan -> {
                if (plan == null || plan.prepared() == null) {
                    return CompletableFuture.completedFuture(recoveryFailure(context.ledger().ledgerId(), context,
                            plan == null ? CompletionStatus.DEFERRED : completionStatus(plan.status()), null));
                }
                var repairedTarget = plan.committed() != null
                        ? closeRepair(plan.committed(), plan.prepared().intent(), controls.get(name), 0)
                            .thenApply(result -> result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                                ? new RepairResult(RepairStatus.REPAIRED_LAST_FRAGMENT, 0, result.publication())
                                : result)
                        : repairFragmentOnce(plan.recovery(), plan.prepared(), sdk, descriptor, key, data, controls,
                            plan.closeOperationId(), true, stopped);
                return repairedTarget.thenCompose(repaired -> {
                    if (repaired.status() != RepairStatus.REPAIRED_LAST_FRAGMENT) {
                        return CompletableFuture.completedFuture(new RecoveryResult(context.ledger().ledgerId(),
                                repaired.status() == RepairStatus.DATA_LOSS ? RecoveryStatus.DATA_LOSS
                                    : repaired.status() == RepairStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                                        : repaired.status() == RepairStatus.CONFLICT ? RecoveryStatus.CONFLICT
                                            : RecoveryStatus.DEFERRED,
                                PrefixReason.SOURCE_UNAVAILABLE, context, selected.boundary().prefixEntryId(),
                                selected.boundary().prefixLength(), null, null));
                    }
                    return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, member + 1);
                });
            });
        });
    }

    private static RecoveryResult recoveryFailure(long ledger, ProfileRecoveryContext context, CompletionStatus status,
            PrefixResult proof) {
        return new RecoveryResult(ledger, status == CompletionStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                : status == CompletionStatus.CONFLICT ? RecoveryStatus.CONFLICT : RecoveryStatus.DEFERRED,
                status == CompletionStatus.DEFERRED ? PrefixReason.SOURCE_UNAVAILABLE
                    : status == CompletionStatus.QUARANTINED ? PrefixReason.AUTHORITY_UNRECOVERABLE
                        : PrefixReason.AUTHORITY_CHANGED,
                context, proof == null ? -1 : proof.prefixEntryId(),
                proof == null ? 0 : proof.prefixLength(), proof, null);
    }

    public enum CompletionStatus { CLOSED_METADATA_DURABLE, DEFERRED, CONFLICT, QUARANTINED }

    /** Standard CLOSED fact only; not a production readiness or a globally revoked read lease. */
    public record CompletionResult(CompletionStatus status, ProfileRecoveryContext context,
            ZKProfileReservationStore.ClosedRecoveryResult publication, int terminalMembers) { }

    /** Resolve each current full-copy target through actual UIC4, count distinct original quorum slots, then CAS. */
    public CompletableFuture<CompletionResult> finishRecovery(ProfileRecoveryContext context,
            Map<String, ProfileInitialControlClient> supplied) {
        if (supplied.size() > 64) {
            throw new IllegalArgumentException("Recovery close peer pool exceeds its bound");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new CompletionResult(CompletionStatus.DEFERRED,
                    context, null, 0));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = finishRecoveryOnce(context, supplied, stopped);
            var caller = actual.whenComplete((result, failure) -> active.release()).copy();
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<CompletionResult> finishRecoveryOnce(ProfileRecoveryContext context,
            Map<String, ProfileInitialControlClient> supplied, AtomicBoolean stopped) {
        var peers = Map.copyOf(supplied);
        return CompletableFuture.supplyAsync(() -> reservations.readClosedRecovery(metadata, context), cold)
                .thenCompose(existing -> {
            if (existing.metadata() != null) {
                return CompletableFuture.completedFuture(new CompletionResult(
                        CompletionStatus.CLOSED_METADATA_DURABLE, context, existing, 0));
            }
            return CompletableFuture.supplyAsync(() -> reservations.closingPlan(metadata, context), cold)
                    .thenCompose(prepared -> {
                if (stopped.get() || prepared.plan() == null) {
                    return CompletableFuture.completedFuture(new CompletionResult(
                            stopped.get() ? CompletionStatus.DEFERRED : completionStatus(prepared.status()),
                            context, null, 0));
                }
                var plan = prepared.plan();
                var replies = new ArrayList<CompletableFuture<ZKProfileReservationStore.TerminalAck>>();
                for (var target : plan.targets()) {
                    var peer = peers.get(target.scope().targetBookie());
                    if (peer == null || !peer.hello().capabilities().contains(
                            org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec
                                .terminalCapability(target.terminal() != null))
                            || !Arrays.equals(peer.hello().storageIncarnation(),
                            target.scope().targetStorageIncarnation())) {
                        continue;
                    }
                    try {
                        var requested = target.terminal() != null ? peer.terminalClose(target.request())
                                : peer.recoveryClose(target.request());
                        replies.add(requested.handle((reply, failure) ->
                                        failure == null && reply != null && reply.durable()
                                            && (reply.outcome() == org.apache.bookkeeper.common.profile.control
                                                .ProfileControlResult.Outcome.APPLIED
                                                || reply.outcome() == org.apache.bookkeeper.common.profile.control
                                                    .ProfileControlResult.Outcome.ALREADY_APPLIED)
                                            ? new ZKProfileReservationStore.TerminalAck(
                                                target, peer.hello(), reply) : null));
                    } catch (RuntimeException rejected) {
                        replies.add(CompletableFuture.failedFuture(rejected));
                    }
                }
                return CompletableFuture.allOf(replies.toArray(CompletableFuture[]::new)).thenCompose(unused -> {
                    var acknowledgments = replies.stream().map(CompletableFuture::join)
                            .filter(Objects::nonNull).toList();
                    if (stopped.get()) {
                        return CompletableFuture.completedFuture(new CompletionResult(
                                CompletionStatus.DEFERRED, context, null, acknowledgments.size()));
                    }
                    return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                            : reservations.closeRecovery(metadata, plan, acknowledgments), cold)
                                .thenApply(published -> new CompletionResult(published == null
                                        ? CompletionStatus.DEFERRED : published.metadata() != null
                                            ? CompletionStatus.CLOSED_METADATA_DURABLE
                                            : published.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                                                ? CompletionStatus.CONFLICT : published.outcome()
                                                    == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                                        ? CompletionStatus.QUARANTINED : CompletionStatus.DEFERRED,
                                        context, published, acknowledgments.size()));
                });
            });
        });
    }

    /** Independent closed open; supplied clients remain borrowed. Cancellation closes an undelivered handle. */
    public CompletableFuture<org.apache.bookkeeper.client.LedgerHandle> openReadOnly(long ledger, ClientContext sdk,
            byte[] password, Map<BookieId, ProfileNormalDataClient> supplied, int maxReadEntries) {
        if (supplied.size() > 64 || maxReadEntries < 1 || maxReadEntries > 256) {
            throw new IllegalArgumentException("Readonly pool or request bound is invalid");
        }
        if (sdk.isClientClosed()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException));
        }
        var caller = new CompletableFuture<org.apache.bookkeeper.client.LedgerHandle>();
        try {
            byte[] owned = password.clone();
            var peers = Map.copyOf(supplied);
            CompletableFuture.supplyAsync(() -> {
                var read = reservations.readClosedForOpen(metadata, ledger);
                if (read.snapshot() == null) {
                    throw new java.util.concurrent.CompletionException(readOnlyFailure(read.status()));
                }
                org.apache.bookkeeper.client.LedgerHandle handle = null;
                try {
                    handle = org.apache.bookkeeper.client.ProfileLedgerHandle.fromClosedRead(sdk, read.snapshot(),
                            owned, peers, maxReadEntries);
                    var after = reservations.readClosedForOpen(metadata, ledger);
                    if (after.snapshot() == null) {
                        throw readOnlyFailure(after.status());
                    }
                    var before = read.snapshot();
                    var next = after.snapshot();
                    if (!before.context().equals(next.context()) || before.closed().metadata().birthZxid()
                            != next.closed().metadata().birthZxid()
                            || !before.closed().metadata().metadata().getVersion()
                                .equals(next.closed().metadata().metadata().getVersion())
                            || !ProfileLedgerMetadataStore.sameMetadata(
                                before.closed().metadata().metadata().getValue(),
                                next.closed().metadata().metadata().getValue())
                            || !before.sources().equals(next.sources())) {
                        throw BKException.create(BKException.Code.MetadataVersionException);
                    }
                    if (sdk.isClientClosed()) {
                        throw BKException.create(BKException.Code.ClientClosedException);
                    }
                    return handle;
                } catch (Exception rejected) {
                    if (handle != null) {
                        handle.asyncClose((code, ignored, ctx) -> { }, null);
                    }
                    throw new java.util.concurrent.CompletionException(rejected);
                }
            }, cold).whenComplete((handle, failure) -> {
                active.release();
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else if (!caller.complete(handle)) {
                    handle.asyncClose((code, ignored, ctx) -> { }, null);
                }
            });
            return caller;
        } catch (RuntimeException failedAdmission) {
            active.release();
            throw failedAdmission;
        }
    }

    private static BKException readOnlyFailure(ZKProfileReservationStore.ReadOnlyStatus status) {
        return BKException.create(switch (status) {
            case DELETED -> BKException.Code.NoSuchLedgerExistsException;
            case NOT_CLOSED -> BKException.Code.IllegalOpException;
            case UNAVAILABLE -> BKException.Code.ReadException;
            case CONFLICT, QUARANTINED -> BKException.Code.MetadataVersionException;
            case READABLE_CLOSED -> throw new IllegalArgumentException("Readonly admission was successful");
        });
    }

    private static CompletionStatus completionStatus(ZKProfileReservationStore.CreateStatus status) {
        return switch (status) {
            case CONFLICT -> CompletionStatus.CONFLICT;
            case QUARANTINED -> CompletionStatus.QUARANTINED;
            default -> CompletionStatus.DEFERRED;
        };
    }

    public enum ReplicaHealthStatus {
        FULLY_REPLICATED, VERIFIED_OPEN_PREFIX, UNDERREPLICATED, DEFERRED, CONFLICT, QUARANTINED, DELETED
    }

    /** Produced only after all required write-set copies and the final cold metadata recheck succeed.
     * This is an observation tied to one snapshot, not authority to clear an underreplication marker.
     */
    public static final class ReplicaHealthProof {
        private final ZKProfileReservationStore.ReadOnlySnapshot snapshot;
        private final ProfileRecoveryCoordinator issuer;
        private final long deadline;
        private final org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private ReplicaHealthProof(ZKProfileReservationStore.ReadOnlySnapshot snapshot,
                ProfileRecoveryCoordinator issuer, long deadline,
                org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task) {
            this.snapshot = snapshot;
            this.issuer = issuer;
            this.deadline = deadline;
            this.task = task;
        }

        public ZKProfileReservationStore.ReadOnlySnapshot snapshot() {
            return snapshot;
        }
    }

    public record ReplicaHealthResult(ReplicaHealthStatus status, PrefixReason reason, long unresolvedEntry,
            List<ProfileRecoveryReadHandle.SourceEvidence> sources, ReplicaHealthProof proof,
            org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence openCut) {
        public ReplicaHealthResult(ReplicaHealthStatus status, PrefixReason reason, long unresolvedEntry,
                List<ProfileRecoveryReadHandle.SourceEvidence> sources, ReplicaHealthProof proof) {
            this(status, reason, unresolvedEntry, sources, proof, null);
        }

        public ReplicaHealthResult {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(reason, "reason");
            sources = List.copyOf(sources);
            if ((status == ReplicaHealthStatus.FULLY_REPLICATED) != (proof != null)) {
                throw new IllegalArgumentException("Only a complete replica health observation carries its proof");
            }
            if (proof != null && openCut != null
                    || status == ReplicaHealthStatus.VERIFIED_OPEN_PREFIX && openCut == null) {
                throw new IllegalArgumentException("OPEN prefix observations cannot carry CLOSED health proof");
            }
        }
    }

    /** Actual health child terminal, independent of an early caller deadline/cancellation. */
    public static final class ReplicaHealthAttempt {
        private final CompletableFuture<ReplicaHealthResult> reply;
        private final CompletableFuture<ReplicaHealthResult> terminal;
        private final AtomicBoolean stopped;

        private ReplicaHealthAttempt(CompletableFuture<ReplicaHealthResult> reply,
                CompletableFuture<ReplicaHealthResult> terminal, AtomicBoolean stopped) {
            this.reply = reply;
            this.terminal = terminal;
            this.stopped = stopped;
        }

        public CompletableFuture<ReplicaHealthResult> completion() {
            return terminal.copy();
        }

        public void stop() {
            stopped.set(true);
        }
    }

    private static ReplicaHealthAttempt finishedHealth(CompletableFuture<ReplicaHealthResult> result) {
        return new ReplicaHealthAttempt(result, result, new AtomicBoolean());
    }

    /** Bounded, one-coordinate-at-a-time health scan of matching CLOSED; quorum success is insufficient. */
    public CompletableFuture<ReplicaHealthResult> verifyClosedReplicas(long ledger, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration) {
        return verifyClosedReplicas(ledger, sdk, credential, suppliedData, suppliedControl,
                maxCoordinates, maxDuration, null);
    }

    /** Capture the original acquired task before any health read; an unbound observation cannot clear a marker. */
    public CompletableFuture<ReplicaHealthResult> verifyClosedReplicas(long ledger, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker) {
        return beginVerifyClosedReplicas(ledger, sdk, credential, suppliedData, suppliedControl,
                maxCoordinates, maxDuration, worker).reply;
    }

    public ReplicaHealthAttempt beginVerifyClosedReplicas(long ledger, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker) {
        return beginVerifyReplicas(ledger, sdk, credential, suppliedData, suppliedControl, maxCoordinates,
                maxDuration, worker, false);
    }

    /** Observe only the original SDK LAC-derived lower prefix of OPEN, without FENCE or marker-clear proof. */
    public ReplicaHealthAttempt beginVerifyOpenReplicas(long ledger, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            int maxCoordinates, Duration maxDuration) {
        return beginVerifyReplicas(ledger, sdk, credential, suppliedData, Map.of(), maxCoordinates,
                maxDuration, null, true);
    }

    private ReplicaHealthAttempt beginVerifyReplicas(long ledger, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker, boolean open) {
        long duration = maxDuration.toNanos();
        if (ledger < 0 || maxCoordinates < 1 || suppliedData.size() > 64 || suppliedControl.size() > 64
                || duration <= 0 || maxDuration.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Replica health requires bounded coordinates, duration and peer pools");
        }
        ProfileRecoveryReadHandle.requireCompatibleSdk(sdk);
        if (sdk.isClientClosed()) {
            return finishedHealth(CompletableFuture.failedFuture(
                    BKException.create(BKException.Code.ClientClosedException)));
        }
        if (!active.tryAcquire()) {
            return finishedHealth(CompletableFuture.completedFuture(
                    replicaFailure(ReplicaHealthStatus.DEFERRED, PrefixReason.LIMIT)));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<ReplicaHealthResult>();
        var terminal = new CompletableFuture<ReplicaHealthResult>();
        long deadline = System.nanoTime() + duration;
        try {
            var data = Map.copyOf(suppliedData);
            var controls = Map.copyOf(suppliedControl);
            var key = credential.copy();
            var task = worker == null ? null : worker.captureProfileTask(ledger);
            if (worker != null && task == null) {
                active.release();
                return finishedHealth(CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                        PrefixReason.AUTHORITY_CHANGED)));
            }
            CompletableFuture<ReplicaHealthResult> actual;
            if (open) {
                actual = CompletableFuture.supplyAsync(() -> captureOpenSnapshot(ledger), cold)
                        .thenCompose(snapshot -> {
                    if (snapshot == null) {
                        return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.DEFERRED,
                                PrefixReason.AUTHORITY_UNAVAILABLE));
                    }
                    return verifyOpenReplicasOnce(snapshot, sdk, key, data, maxCoordinates, stopped);
                });
            } else {
                actual = CompletableFuture.supplyAsync(() -> reservations.readClosedForOpen(metadata, ledger), cold)
                    .thenCompose(read -> {
                if (read.snapshot() == null) {
                    return CompletableFuture.completedFuture(replicaReadFailure(read.status()));
                }
                return verifyClosedReplicasOnce(read.snapshot(), sdk, key, data, controls, maxCoordinates,
                        stopped, deadline, task);
            });
            }
            var timer = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                if (failure != null) {
                    terminal.completeExceptionally(failure);
                    caller.completeExceptionally(failure);
                } else {
                    terminal.complete(result);
                    caller.complete(result);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(replicaFailure(ReplicaHealthStatus.DEFERRED, PrefixReason.DEADLINE));
                }, duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(replicaFailure(ReplicaHealthStatus.DEFERRED, PrefixReason.LIMIT));
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new ReplicaHealthAttempt(caller, terminal, stopped);
        } catch (RuntimeException failedAdmission) {
            active.release();
            throw failedAdmission;
        }
    }

    private record OpenSnapshot(ProfileLedgerMetadataStore.MembershipSnapshot member,
            ZKProfileReservationStore.Snapshot root, List<ProfileInstallReceipt> sources,
            org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext ledger) { }

    private OpenSnapshot captureOpenSnapshot(long ledger) {
        var member = metadata.readMembership(ledger).snapshot();
        var root = reservations.read(ledger).snapshot();
        var retained = reservations.retainedActivationTargets(ledger);
        var page = reservations.readPublishedPage(ledger).page();
        if (member == null || root == null || page == null
                || member.metadata().getValue().getState() != org.apache.bookkeeper.client.api.LedgerMetadata.State.OPEN
                || root.lifecycle().state() != ProfileLifecycleHead.State.READY
                || retained.status()
                    != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND
                || !root.reservation().backlink().equals(ProfileMetadataBacklink.from(member.metadata().getValue()))
                || page.page().creatorToken() != member.metadata().getValue().getCToken()
                || !page.page().matchesQuorum(member.metadata().getValue())) {
            return null;
        }
        var scope = root.reservation().installs().get(0);
        var current = new java.util.HashMap<String, byte[]>();
        for (var receipt : retained.targets()) {
            String name = receipt.scope().targetBookie();
            byte[] incarnation = receipt.scope().targetStorageIncarnation();
            byte[] old = current.putIfAbsent(name, incarnation);
            if (old != null && !Arrays.equals(old, incarnation)) {
                return null;
            }
        }
        return new OpenSnapshot(member, root, retained.targets(),
                new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(ledger,
                        scope.ledgerInstanceId(), scope.descriptorIdentity().toBytes()));
    }

    private static boolean sameOpenSnapshot(OpenSnapshot before, OpenSnapshot after) {
        return after != null && before.member().birthZxid() == after.member().birthZxid()
                && before.member().metadata().getVersion().equals(after.member().metadata().getVersion())
                && ProfileLedgerMetadataStore.sameMetadata(before.member().metadata().getValue(),
                        after.member().metadata().getValue())
                && before.root().equals(after.root()) && before.sources().equals(after.sources());
    }

    private CompletableFuture<ReplicaHealthResult> verifyOpenReplicasOnce(OpenSnapshot snapshot, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> data, int maxCoordinates,
            AtomicBoolean stopped) {
        ProfileRecoveryReadHandle reader;
        try {
            if (!credential.matchesWireVerifier(org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(
                    snapshot.member().metadata().getValue().getPassword()))) {
                return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                        PrefixReason.PERMISSION));
            }
            var permitted = new java.util.HashMap<BookieId, ProfileNormalDataClient>();
            for (var receipt : snapshot.sources()) {
                var name = BookieId.parse(receipt.scope().targetBookie());
                if (data.containsKey(name)) {
                    permitted.put(name, data.get(name));
                }
            }
            reader = ProfileRecoveryReadHandle.fromOpenAudit(sdk, snapshot.member(), snapshot.ledger(),
                    snapshot.sources(), permitted);
        } catch (java.security.GeneralSecurityException failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (IllegalArgumentException unsupported) {
            return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                    PrefixReason.AUTHORITY_CHANGED));
        }
        return reader.readLacEvidence().thenCompose(cut -> {
            if (stopped.get() || cut.prefixEntryId() >= maxCoordinates) {
                return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.DEFERRED,
                        stopped.get() ? PrefixReason.CANCELLED : PrefixReason.LIMIT));
            }
            var scan = new ReplicaScan(reader, cut.prefixEntryId(), cut.prefixLength(), stopped);
            scan.step();
            return scan.done.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                if (stopped.get()) {
                    return replicaFailure(ReplicaHealthStatus.DEFERRED, PrefixReason.CANCELLED);
                }
                if (!sameOpenSnapshot(snapshot, captureOpenSnapshot(snapshot.ledger().ledgerId()))) {
                    return replicaFailure(ReplicaHealthStatus.CONFLICT, PrefixReason.AUTHORITY_CHANGED);
                }
                return new ReplicaHealthResult(result.status() == ReplicaHealthStatus.FULLY_REPLICATED
                        ? ReplicaHealthStatus.VERIFIED_OPEN_PREFIX : result.status(), result.reason(),
                        result.unresolvedEntry(), result.sources(), null, cut);
            }, cold));
        }).handle((result, failure) -> {
            if (failure == null) {
                return result;
            }
            var actual = cause(failure);
            if (actual instanceof BKException exception) {
                return replicaFailure(exception.getCode() == BKException.Code.UnauthorizedAccessException
                        ? ReplicaHealthStatus.CONFLICT : ReplicaHealthStatus.DEFERRED,
                        exception.getCode() == BKException.Code.UnauthorizedAccessException
                            ? PrefixReason.PERMISSION : PrefixReason.LAC_UNAVAILABLE);
            }
            throw new java.util.concurrent.CompletionException(actual);
        }).whenComplete((result, failure) -> reader.closeAsync());
    }

    private CompletableFuture<ReplicaHealthResult> verifyClosedReplicasOnce(
            ZKProfileReservationStore.ReadOnlySnapshot snapshot, ClientContext sdk,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> controls, int maxCoordinates, AtomicBoolean stopped,
            long deadline, org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager.ProfileTask task) {
        var member = snapshot.closed().metadata();
        var value = member.metadata().getValue();
        if (value.getEnsembleSize() != 3 || value.getWriteQuorumSize() != 3
                || value.getAckQuorumSize() < 2 || value.getAckQuorumSize() > 3
                || value.getDigestType() != org.apache.bookkeeper.client.api.DigestType.CRC32C
                || !value.getCustomMetadata().containsKey(ProfileRecoveryBoundary.CLOSE_KEY)) {
            return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                    PrefixReason.AUTHORITY_CHANGED));
        }
        try {
            if (!credential.matchesWireVerifier(org.apache.bookkeeper.proto.checksum.DigestManager
                    .generateMasterKey(value.getPassword()))) {
                return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                        PrefixReason.PERMISSION));
            }
        } catch (java.security.NoSuchAlgorithmException impossible) {
            return CompletableFuture.failedFuture(impossible);
        }
        if (stopped.get() || value.getLastEntryId() >= maxCoordinates) {
            return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.DEFERRED,
                    stopped.get() ? PrefixReason.CANCELLED : PrefixReason.LIMIT));
        }
        var data = new java.util.HashMap<BookieId, ProfileNormalDataClient>();
        for (var receipt : snapshot.sources()) {
            String name = receipt.scope().targetBookie();
            var peer = suppliedData.get(BookieId.parse(name));
            if (peer == null) {
                continue;
            }
            if (peer.hello().activeEngine() != 3 || !peer.hello().bookieId().equals(name)
                    || !Arrays.equals(peer.hello().storageIncarnation(), receipt.scope().targetStorageIncarnation())) {
                return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.CONFLICT,
                        PrefixReason.AUTHORITY_CHANGED));
            }
            data.put(BookieId.parse(name), peer);
        }
        var accepted = new ZKProfileReservationStore.RecoveryResult(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED,
                snapshot.context(), member);
        return fenceOnce(accepted, controls, credential).thenCompose(fenced -> {
            // Includes a possibly empty final fragment: all its current targets must answer, not merely A of E.
            var last = value.getAllEnsembles().lastEntry().getValue();
            if (stopped.get()) {
                return CompletableFuture.completedFuture(replicaFailure(ReplicaHealthStatus.DEFERRED,
                        PrefixReason.CANCELLED));
            }
            if (fenced.status() == Status.CONFLICT || fenced.status() == Status.QUARANTINED) {
                return CompletableFuture.completedFuture(replicaFailure(fenced.status() == Status.CONFLICT
                        ? ReplicaHealthStatus.CONFLICT : ReplicaHealthStatus.QUARANTINED,
                        PrefixReason.AUTHORITY_CHANGED));
            }
            var unavailable = last.stream().filter(bookie -> !data.containsKey(bookie) || fenced.fences().stream()
                    .noneMatch(fence -> fence.bookie().equals(bookie.toString()))).map(bookie ->
                        new ProfileRecoveryReadHandle.SourceEvidence(bookie, data.containsKey(bookie)
                            ? HexFormat.of().formatHex(data.get(bookie).hello().storageIncarnation()) : "",
                            ProfileRecoveryReadHandle.SourceOutcome.UNAVAILABLE)).toList();
            if (fenced.status() != Status.FENCED_CONTEXT || !unavailable.isEmpty()) {
                return CompletableFuture.completedFuture(new ReplicaHealthResult(ReplicaHealthStatus.DEFERRED,
                        PrefixReason.SOURCE_UNAVAILABLE, value.getAllEnsembles().lastKey(), unavailable, null));
            }
            var controlPeers = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
            controls.forEach((name, peer) -> controlPeers.put(BookieId.parse(name), peer));
            ProfileRecoveryReadHandle reader;
            try {
                reader = ProfileRecoveryReadHandle.fromClosedRepairContext(sdk, accepted, snapshot.sources(),
                        data, controlPeers);
            } catch (java.security.GeneralSecurityException invalid) {
                return CompletableFuture.failedFuture(invalid);
            }
            var scan = new ReplicaScan(reader, snapshot, stopped);
            scan.step();
            return scan.done.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                if (stopped.get()) {
                    return replicaFailure(ReplicaHealthStatus.DEFERRED, PrefixReason.CANCELLED);
                }
                var fresh = reservations.readClosedForOpen(metadata, value.getLedgerId());
                if (fresh.snapshot() == null) {
                    return replicaReadFailure(fresh.status());
                }
                if (!sameClosedSnapshot(snapshot, fresh.snapshot())) {
                    return replicaFailure(ReplicaHealthStatus.CONFLICT, PrefixReason.AUTHORITY_CHANGED);
                }
                return result.status() == ReplicaHealthStatus.FULLY_REPLICATED
                        ? new ReplicaHealthResult(result.status(), result.reason(), -1, List.of(),
                                new ReplicaHealthProof(fresh.snapshot(), this, deadline, task))
                        : new ReplicaHealthResult(result.status(), result.reason(), result.unresolvedEntry(),
                                result.sources(), null);
            }, cold)).whenComplete((result, failure) -> reader.closeAsync());
        });
    }

    private static boolean sameClosedSnapshot(ZKProfileReservationStore.ReadOnlySnapshot before,
            ZKProfileReservationStore.ReadOnlySnapshot after) {
        return before.context().equals(after.context())
                && before.closed().metadata().birthZxid() == after.closed().metadata().birthZxid()
                && before.closed().metadata().metadata().getVersion()
                    .equals(after.closed().metadata().metadata().getVersion())
                && ProfileLedgerMetadataStore.sameMetadata(before.closed().metadata().metadata().getValue(),
                    after.closed().metadata().metadata().getValue()) && before.sources().equals(after.sources());
    }

    public enum MarkerCleanupStatus { CLEARED, DEFERRED, CONFLICT, QUARANTINED, DELETED }

    /** Consume this attempt's health proof only with fresh cold authority and the original scoped worker lock. */
    public CompletableFuture<MarkerCleanupStatus> completeReplication(ReplicaHealthProof proof,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker) {
        Objects.requireNonNull(proof, "proof");
        Objects.requireNonNull(worker, "worker");
        if (proof.task == null || proof.issuer != this || proof.deadline - System.nanoTime() <= 0) {
            return CompletableFuture.completedFuture(MarkerCleanupStatus.CONFLICT);
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(MarkerCleanupStatus.DEFERRED);
        }
        var caller = new CompletableFuture<MarkerCleanupStatus>();
        var stopped = new AtomicBoolean();
        try {
            CompletableFuture.supplyAsync(() -> completeReplicationOnce(proof, worker, stopped), cold)
                    .whenComplete((result, failure) -> {
                active.release();
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else {
                    caller.complete(result);
                }
            });
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException rejected) {
            active.release();
            throw rejected;
        }
    }

    private MarkerCleanupStatus completeReplicationOnce(ReplicaHealthProof proof,
            org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager worker, AtomicBoolean stopped) {
        if (stopped.get() || proof.deadline - System.nanoTime() <= 0) {
            return MarkerCleanupStatus.DEFERRED;
        }
        if (!proof.consumed.compareAndSet(false, true)) {
            return MarkerCleanupStatus.CONFLICT;
        }
        var current = reservations.readClosedForOpen(metadata, proof.snapshot.context().ledger().ledgerId());
        if (current.snapshot() == null) {
            return switch (current.status()) {
                case DELETED -> MarkerCleanupStatus.DELETED;
                case UNAVAILABLE -> MarkerCleanupStatus.DEFERRED;
                case QUARANTINED -> MarkerCleanupStatus.QUARANTINED;
                default -> MarkerCleanupStatus.CONFLICT;
            };
        }
        if (!sameClosedSnapshot(proof.snapshot, current.snapshot())) {
            return MarkerCleanupStatus.CONFLICT;
        }
        return stopped.get() || proof.deadline - System.nanoTime() <= 0 ? MarkerCleanupStatus.DEFERRED
                : reservations.completeReplicaMarker(metadata, current.snapshot(), worker, proof.task);
    }

    private static ReplicaHealthResult replicaFailure(ReplicaHealthStatus status, PrefixReason reason) {
        return new ReplicaHealthResult(status, reason, -1, List.of(), null);
    }

    private static ReplicaHealthResult replicaReadFailure(ZKProfileReservationStore.ReadOnlyStatus status) {
        return replicaFailure(switch (status) {
            case DELETED -> ReplicaHealthStatus.DELETED;
            case UNAVAILABLE, NOT_CLOSED -> ReplicaHealthStatus.DEFERRED;
            case CONFLICT -> ReplicaHealthStatus.CONFLICT;
            default -> ReplicaHealthStatus.QUARANTINED;
        }, status == ZKProfileReservationStore.ReadOnlyStatus.UNAVAILABLE ? PrefixReason.AUTHORITY_UNAVAILABLE
                : PrefixReason.AUTHORITY_CHANGED);
    }

    private record ReplicaScanResult(ReplicaHealthStatus status, PrefixReason reason, long unresolvedEntry,
            List<ProfileRecoveryReadHandle.SourceEvidence> sources) { }

    private static final class ReplicaScan {
        private final ProfileRecoveryReadHandle reader;
        private final long prefix;
        private final long finalLength;
        private final AtomicBoolean stopped;
        private final CompletableFuture<ReplicaScanResult> done = new CompletableFuture<>();
        private long next;
        private long length;

        ReplicaScan(ProfileRecoveryReadHandle reader, ZKProfileReservationStore.ReadOnlySnapshot snapshot,
                AtomicBoolean stopped) {
            this(reader, snapshot.closed().metadata().metadata().getValue().getLastEntryId(),
                    snapshot.closed().metadata().metadata().getValue().getLength(), stopped);
        }

        ReplicaScan(ProfileRecoveryReadHandle reader, long prefix, long finalLength, AtomicBoolean stopped) {
            this.reader = reader;
            this.prefix = prefix;
            this.finalLength = finalLength;
            this.stopped = stopped;
        }

        void step() {
            if (stopped.get()) {
                done.complete(new ReplicaScanResult(ReplicaHealthStatus.DEFERRED,
                        PrefixReason.CANCELLED, next, List.of()));
            } else if (next > prefix) {
                done.complete(length == finalLength
                        ? new ReplicaScanResult(ReplicaHealthStatus.FULLY_REPLICATED,
                                PrefixReason.NORMAL_TAIL, -1, List.of())
                        : new ReplicaScanResult(ReplicaHealthStatus.QUARANTINED, PrefixReason.LENGTH_CONFLICT,
                                next, List.of()));
            } else {
                reader.probeEvidence(next).whenComplete((evidence, failure) -> {
                    if (failure != null) {
                        if (cause(failure) instanceof BKException) {
                            done.complete(new ReplicaScanResult(ReplicaHealthStatus.DEFERRED,
                                    PrefixReason.SOURCE_UNAVAILABLE, next, List.of()));
                        } else {
                            done.completeExceptionally(failure);
                        }
                    } else if (stopped.get()) {
                        done.complete(new ReplicaScanResult(ReplicaHealthStatus.DEFERRED,
                        PrefixReason.CANCELLED, next, List.of()));
                    } else if (evidence.conflict()) {
                        finish(ReplicaHealthStatus.QUARANTINED, PrefixReason.PAYLOAD_CONFLICT, evidence);
                    } else if (evidence.sources().stream().anyMatch(source -> source.outcome()
                            == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                        finish(ReplicaHealthStatus.CONFLICT, PrefixReason.PERMISSION, evidence);
                    } else if (evidence.sources().stream().anyMatch(source -> source.outcome()
                            == ProfileRecoveryReadHandle.SourceOutcome.MISSING || source.outcome()
                                == ProfileRecoveryReadHandle.SourceOutcome.CORRUPT)) {
                        finish(ReplicaHealthStatus.UNDERREPLICATED, PrefixReason.REQUIRED_HOLE, evidence);
                    } else if (evidence.candidate() == null || evidence.sources().stream().anyMatch(source ->
                            source.outcome() != ProfileRecoveryReadHandle.SourceOutcome.FOUND)) {
                        finish(ReplicaHealthStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE, evidence);
                    } else {
                        var candidate = evidence.candidate();
                        int bytes = candidate.payload().remaining();
                        if (length > Long.MAX_VALUE - bytes || candidate.length() != length + bytes) {
                            finish(ReplicaHealthStatus.QUARANTINED, PrefixReason.LENGTH_CONFLICT, evidence);
                        } else {
                            length = candidate.length();
                            next++;
                            step();
                        }
                    }
                });
            }
        }

        void finish(ReplicaHealthStatus status, PrefixReason reason, ProfileRecoveryReadHandle.PointEvidence evidence) {
            done.complete(new ReplicaScanResult(status, reason, next, evidence.sources()));
        }
    }

    public enum RepairStatus {
        REPAIRED_HISTORICAL_FRAGMENT, REPAIRED_LAST_FRAGMENT, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS
    }

    public record RepairResult(RepairStatus status, long copiedEntries,
            ZKProfileReservationStore.RepairCommitResult publication) { }

    /** Only the actual terminalClose client completion can construct an EMPTY publication proof. */
    static final class EmptyRepairProof {
        private final ZKProfileReservationStore.RepairSnapshot repair;
        private final ProfileLedgerMetadataStore.MembershipSnapshot before;
        private final ProfileRepairCommit.EmptyBoundary boundary;

        private EmptyRepairProof(ZKProfileReservationStore.RepairSnapshot repair,
                ProfileLedgerMetadataStore.MembershipSnapshot before, ProfileControlScope scope,
                ProfileControlRequest.TerminalBinding binding, ProfileInitialControlClient peer,
                org.apache.bookkeeper.common.profile.control.ProfileControlResult result) {
            if (!repair.intent().empty() || !result.durable()
                    || !peer.hello().capabilities().contains(org.apache.bookkeeper.common.profile.wire
                        .ProfileInitialControlCodec.terminalCapability(true))
                    || !Arrays.equals(peer.hello().storageIncarnation(), scope.targetStorageIncarnation())
                    || !peer.hello().bookieId().equals(scope.targetBookie())) {
                throw new IllegalArgumentException("EMPTY proof requires its actual terminal receiver completion");
            }
            this.repair = repair;
            this.before = before;
            boundary = new ProfileRepairCommit.EmptyBoundary(ProfileRepairIntent.digest(repair.intent().encode()),
                    scope.operationId(), scope.authorityGeneration(), binding, result.localGeneration());
        }

        ZKProfileReservationStore.RepairSnapshot repair() {
            return repair;
        }
        ProfileLedgerMetadataStore.MembershipSnapshot before() {
            return before;
        }
        ProfileRepairCommit.EmptyBoundary boundary() {
            return boundary;
        }
    }

    /** One frozen intent. Copy is sequential and only retains one entry/RPC; caller cancellation stops future work,
     * never the child RPC already using buffers. Retry/resume reuses accepted, prepared and closeOperationId. */
    public CompletableFuture<RepairResult> repairHistorical(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId) {
        return repairFragment(accepted, prepared, sdk, canonicalDescriptor, credential,
                suppliedData, suppliedControl, closeOperationId, false);
    }

    /** The published prefix selection fixes the last fragment; this does not publish ledger CLOSED. */
    public CompletableFuture<RepairResult> repairLastFragment(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId) {
        return repairFragment(accepted, prepared, sdk, canonicalDescriptor, credential,
                suppliedData, suppliedControl, closeOperationId, true);
    }

    /** Replicate a CLOSED fragment while retaining the original close P/L/context; no reopen or marker clear. */
    public CompletableFuture<RepairResult> repairClosedFragment(ZKProfileReservationStore.ReadOnlySnapshot accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk, byte[] descriptor,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, byte[] closeId) {
        var value = accepted.closed().metadata().metadata().getValue();
        if (value.getState() != org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED) {
            throw new IllegalArgumentException("Closed repair requires its durable closed snapshot");
        }
        return repairFragment(new ZKProfileReservationStore.RecoveryResult(
                ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, accepted.context(), accepted.closed().metadata()),
                prepared, sdk, descriptor, credential, data, controls, closeId,
                value.getAllEnsembles().higherKey(prepared.intent().fragmentStart()) == null);
    }

    private CompletableFuture<RepairResult> repairFragment(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId, boolean last) {
        if (accepted.context() == null || !accepted.context().equals(prepared.intent().recovery())
                || suppliedData.size() > 64 || suppliedControl.size() > 64) {
            throw new IllegalArgumentException("Historical repair requires its frozen context and bounded peers");
        }
        if (canonicalDescriptor.length > org.apache.bookkeeper.common.profile.ProfileDescriptorCodec
                .MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Repair descriptor exceeds its canonical bound");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = repairFragmentOnce(accepted, prepared, sdk, canonicalDescriptor, credential,
                    suppliedData, suppliedControl, closeOperationId, last, stopped);
            var caller = actual.whenComplete((value, failure) -> active.release()).copy();
            caller.whenComplete((value, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<RepairResult> repairFragmentOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk, byte[] canonicalDescriptor,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId, boolean last,
            AtomicBoolean stopped) {
        var input = accepted.metadata().metadata().getValue();
        boolean closedRepair = input.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED;
        var custom = new java.util.TreeMap<String, byte[]>();
        input.getCustomMetadata().forEach((name, bytes) -> custom.put(name, bytes.clone()));
        var builder = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(input).withCToken(input.getCToken())
                .withCustomMetadata(custom);
        if (input.hasPassword()) {
            builder.withPassword(input.getPassword().clone());
        }
        var frozen = new ZKProfileReservationStore.RecoveryResult(accepted.outcome(), accepted.context(),
                new ProfileLedgerMetadataStore.MembershipSnapshot(accepted.metadata().birthZxid(),
                        new org.apache.bookkeeper.versioning.Versioned<>(builder.build(),
                                accepted.metadata().metadata().getVersion())));
        var intent = prepared.intent();
        var data = Map.copyOf(suppliedData);
        var controls = Map.copyOf(suppliedControl);
        var name = intent.install().targetBookie();
        var target = data.get(BookieId.parse(name));
        var control = controls.get(name);
        if (target == null || control == null || target.hello().activeEngine() != 3
                || control.hello().activeEngine() != 3 || !target.hello().bookieId().equals(name)
                || !control.hello().bookieId().equals(name)
                || !Arrays.equals(target.hello().storageIncarnation(), intent.install().targetStorageIncarnation())
                || !Arrays.equals(control.hello().storageIncarnation(),
                    intent.install().targetStorageIncarnation())) {
            throw new IllegalArgumentException("Repair target must retain its verified control/DATA incarnation");
        }
        byte[] descriptor = canonicalDescriptor.clone();
        byte[] closeId = ProfileLifecycleHead.fixed(closeOperationId, 16, true);
        var key = credential.copy();
        if (intent.empty()) {
            return repairEmptyOnce(frozen, prepared, descriptor, key, controls, control, closeId, stopped);
        }
        CompletableFuture<RepairResult> actual;
            actual = CompletableFuture.supplyAsync(() -> readers(frozen.context(), closedRepair), cold)
                    .thenCompose(source -> {
                if (source.recovery().context() == null) {
                    return CompletableFuture.completedFuture(repairFailure(source.recovery().outcome()));
                }
                var current = source.recovery();
                var value = current.metadata().metadata().getValue();
                Long end = value.getAllEnsembles().higherKey(intent.fragmentStart());
                if (last != (end == null && intent.fragmentStart() == value.getAllEnsembles().lastKey())) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                }
                if (last) {
                    var boundary = reservations.readSelectedPrefix(metadata, current.context());
                    if (boundary.snapshot() == null
                            || intent.range().lastEntryId() != boundary.snapshot().boundary().prefixEntryId()) {
                        return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                    }
                    end = boundary.snapshot().boundary().prefixEntryId() + 1;
                }
                boolean published = ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY))
                        .stream().anyMatch(ref -> ref.slot() == intent.slot()
                            && ref.intentBirth().equals(prepared.birth())
                            && ref.intentStoreVersion() == prepared.storeVersion());
                if (published) {
                    return CompletableFuture.supplyAsync(() -> reservations.resolveRepairClose(metadata, prepared,
                            closeId), cold).thenCompose(result -> closeRepair(result, intent,
                                    control, 0));
                }
                boolean sameContent;
                try {
                    sameContent = Arrays.equals(ProfileRecoveryContext.metadataDigest(value),
                            ProfileRecoveryContext.metadataDigest(frozen.metadata().metadata().getValue()))
                            && Arrays.equals(value.getCustomMetadata().get(ProfileRepairCommit.KEY),
                                frozen.metadata().metadata().getValue().getCustomMetadata()
                                    .get(ProfileRepairCommit.KEY))
                            && Arrays.equals(value.getCustomMetadata().get(ProfileRecoveryBoundary.KEY),
                                frozen.metadata().metadata().getValue().getCustomMetadata()
                                    .get(ProfileRecoveryBoundary.KEY));
                } catch (java.io.IOException invalid) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.QUARANTINED, 0, null));
                }
                if (!frozen.context().equals(current.context())
                        || !current.metadata().metadata().getVersion()
                            .equals(frozen.metadata().metadata().getVersion())
                        || current.metadata().birthZxid() != frozen.metadata().birthZxid()
                        || !sameContent || end == null || intent.range().firstEntryId() != intent.fragmentStart()
                        || intent.range().lastEntryId() != end - 1 || stopped.get()) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                }
                return fenceOnce(current, controls, key).thenCompose(fenced -> {
                    if (fenced.status() != Status.FENCED_CONTEXT || stopped.get()) {
                        return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
                    }
                    return control.install(ProfileControlRequest.install(
                            intent.install(), descriptor, EngineProfile
                                .SEGMENT_WAL_ENGINE, key)).thenCompose(installed -> {
                        if (!installed.durable() || stopped.get()) {
                            return CompletableFuture.completedFuture(
                                    new RepairResult(RepairStatus.DEFERRED, 0, null));
                        }
                        var receipt = ProfileInstallReceipt.from(intent.install(), control.hello(), installed);
                        return CompletableFuture.supplyAsync(
                                () -> reservations.admitRepair(metadata, prepared, receipt), cold)
                                    .thenCompose(admitted -> {
                            if (admitted.status() != ZKProfileReservationStore.CreateStatus.APPLIED
                                    && admitted.status() != ZKProfileReservationStore.CreateStatus.ALREADY_APPLIED
                                    || stopped.get()) {
                                return CompletableFuture.completedFuture(
                                        new RepairResult(RepairStatus.DEFERRED, 0, null));
                            }
                            return control.recoveryGrant(org.apache.bookkeeper.common.profile.control
                                    .ProfileControlRequest.recoveryGrant(intent.grantScope(),
                                        new ProfileControlRequest
                                            .RecoveryBinding(intent.intentId(), intent.intentGeneration(),
                                                intent.grantGeneration()))).thenCompose(granted -> {
                                if (!granted.durable() || stopped.get()) {
                                    return CompletableFuture.completedFuture(new RepairResult(
                                            RepairStatus.DEFERRED, 0, null));
                                }
                                return CompletableFuture.supplyAsync(
                                        () -> readers(frozen.context(), closedRepair), cold)
                                        .thenCompose(ready -> copyRepair(frozen, prepared, sdk, data, controls,
                                                target, control, key, closeId, stopped, ready));
                            });
                        });
                    });
                });
            });
        if (last) {
            actual = actual.thenApply(result -> result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                    ? new RepairResult(RepairStatus.REPAIRED_LAST_FRAGMENT, result.copiedEntries(),
                            result.publication()) : result);
        }
        return actual;
    }

    private CompletableFuture<RepairResult> repairEmptyOnce(ZKProfileReservationStore.RecoveryResult frozen,
            ZKProfileReservationStore.RepairSnapshot prepared, byte[] descriptor, ProtectedProfileCredential key,
            Map<String, ProfileInitialControlClient> controls, ProfileInitialControlClient control, byte[] closeId,
            AtomicBoolean stopped) {
        var intent = prepared.intent();
        if (!Arrays.equals(closeId, intent.terminalOperationId()) || !control.hello().capabilities().contains(
                org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec.terminalCapability(true))) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
        }
        return CompletableFuture.supplyAsync(() -> {
            var current = reservations.readRepairable(metadata, intent.recovery().ledger().ledgerId());
            if (current.context() == null || !current.context().equals(frozen.context())) {
                return null;
            }
            var selected = reservations.readSelectedPrefix(metadata, current.context());
            if (selected.snapshot() == null || current.metadata().metadata().getValue().getAllEnsembles().lastKey()
                    != intent.fragmentStart() || selected.snapshot().boundary().prefixEntryId()
                        != intent.fragmentStart() - 1) {
                return null;
            }
            boolean published = ProfileRepairCommit.head(current.metadata().metadata().getValue()
                    .getCustomMetadata().get(ProfileRepairCommit.KEY)).stream().anyMatch(ref ->
                        ref.slot() == intent.slot() && ref.intentBirth().equals(prepared.birth())
                            && ref.intentStoreVersion() == prepared.storeVersion());
            return new EmptyReady(current, selected.snapshot(), published,
                    !published && reservations.repairAdmitted(prepared));
        }, cold).thenCompose(ready -> {
            if (ready == null || stopped.get()) {
                return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
            }
            if (ready.published()) {
                return CompletableFuture.supplyAsync(
                        () -> reservations.resolveRepairClose(metadata, prepared, closeId), cold)
                            .thenCompose(published -> closeRepair(published, intent, control, 0));
            }
            if (ready.current().metadata().birthZxid() != frozen.metadata().birthZxid()
                    || !ready.current().metadata().metadata().getVersion().equals(
                        frozen.metadata().metadata().getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(ready.current().metadata().metadata().getValue(),
                        frozen.metadata().metadata().getValue())) {
                return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
            }
            return fenceOnce(ready.current(), controls, key).thenCompose(fenced -> {
                if (fenced.status() != Status.FENCED_CONTEXT || stopped.get()) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
                }
                if (ready.admitted()) {
                    return emptyBoundaryThenPublish(prepared, ready, control, closeId, stopped);
                }
                return control.install(ProfileControlRequest.install(intent.install(), descriptor,
                        EngineProfile.SEGMENT_WAL_ENGINE, key)).thenCompose(installed -> {
                    if (!installed.durable() || stopped.get()) {
                        return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
                    }
                    var receipt = ProfileInstallReceipt.from(intent.install(), control.hello(), installed);
                    return CompletableFuture.supplyAsync(
                            () -> reservations.admitRepair(metadata, prepared, receipt), cold)
                            .thenCompose(admitted -> admitted.snapshot() == null || stopped.get()
                                ? CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null))
                                : emptyBoundaryThenPublish(prepared, ready, control, closeId, stopped));
                });
            });
        }).thenApply(result -> result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                ? new RepairResult(RepairStatus.REPAIRED_LAST_FRAGMENT, 0, result.publication()) : result);
    }

    private record EmptyReady(ZKProfileReservationStore.RecoveryResult current,
            ZKProfileReservationStore.BoundarySnapshot selected, boolean published, boolean admitted) { }

    private CompletableFuture<RepairResult> emptyBoundaryThenPublish(ZKProfileReservationStore.RepairSnapshot prepared,
            EmptyReady ready, ProfileInitialControlClient control, byte[] closeId, AtomicBoolean stopped) {
        var boundary = ready.selected().boundary();
        var binding = new ProfileControlRequest.TerminalBinding(boundary.prefixEntryId(), boundary.prefixLength(),
                ready.selected().reference().digest());
        var scope = prepared.intent().terminalScope(closeId, prepared.intent().grantGeneration(), binding);
        if (stopped.get()) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
        }
        return control.terminalClose(ProfileControlRequest.terminalClose(scope, binding)).thenCompose(result -> {
            if (!result.durable() || stopped.get()) {
                return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
            }
            var proof = new EmptyRepairProof(prepared, ready.current().metadata(), scope, binding, control, result);
            return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                    : reservations.commitEmptyRepair(metadata, proof), cold).thenApply(published ->
                        new RepairResult(published != null && published.metadata() != null
                            ? RepairStatus.REPAIRED_LAST_FRAGMENT : RepairStatus.DEFERRED, 0, published));
        });
    }

    private CompletableFuture<RepairResult> copyRepair(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, ProfileNormalDataClient target,
            ProfileInitialControlClient control, ProtectedProfileCredential credential, byte[] closeId,
            AtomicBoolean stopped, Readers ready) {
        if (ready.recovery().context() == null || !accepted.context().equals(ready.recovery().context())
                || !ready.recovery().metadata().metadata().getVersion()
                    .equals(accepted.metadata().metadata().getVersion())) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
        }
        var boundControls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
        controls.forEach((name, peer) -> boundControls.put(BookieId.parse(name), peer));
        ProfileRecoveryReadHandle reader;
        try {
            reader = accepted.metadata().metadata().getValue().getState()
                    == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED
                        ? ProfileRecoveryReadHandle.fromClosedRepairContext(sdk, ready.recovery(), ready.permitted(),
                            data, boundControls)
                        : ProfileRecoveryReadHandle.fromContext(sdk, ready.recovery(), ready.permitted(),
                            data, boundControls);
        } catch (java.security.GeneralSecurityException invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        var copy = new Copy(reader, target, prepared.intent(), credential, stopped);
        copy.step();
        return copy.done.thenCompose(result -> {
            if (result.status() != RepairStatus.REPAIRED_HISTORICAL_FRAGMENT || stopped.get()) {
                return CompletableFuture.completedFuture(stopped.get()
                        ? new RepairResult(RepairStatus.DEFERRED, result.copiedEntries(), null) : result);
            }
            return CompletableFuture.supplyAsync(() -> stopped.get() ? null : reservations.commitRepair(metadata,
                    prepared, accepted.metadata(), closeId), cold).thenCompose(published -> published == null
                        ? CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED,
                                result.copiedEntries(), null))
                        : closeRepair(published, prepared.intent(), control, result.copiedEntries()));
        }).whenComplete((result, failure) -> reader.closeAsync());
    }

    private CompletableFuture<RepairResult> closeRepair(ZKProfileReservationStore.RepairCommitResult published,
            ProfileRepairIntent intent, ProfileInitialControlClient control, long copied) {
        if (published.status() != ZKProfileReservationStore.CreateStatus.APPLIED
                && published.status() != ZKProfileReservationStore.CreateStatus.ALREADY_APPLIED) {
            return CompletableFuture.completedFuture(new RepairResult(published.status()
                    == ZKProfileReservationStore.CreateStatus.CONFLICT ? RepairStatus.CONFLICT
                    : published.status() == ZKProfileReservationStore.CreateStatus.QUARANTINED
                        ? RepairStatus.QUARANTINED : RepairStatus.DEFERRED, copied, published));
        }
        var request = published.terminal() != null ? control.terminalClose(ProfileControlRequest.terminalClose(
                published.closeScope(), published.terminal())) : control.recoveryClose(
                        ProfileControlRequest.recoveryClose(
                published.closeScope(), new ProfileControlRequest
                    .RecoveryBinding(intent.intentId(), intent.intentGeneration(), intent.grantGeneration()),
                intent.grantOperationId()));
        return request.handle((closed, failure) -> new RepairResult(failure == null
                        && closed.durable() ? RepairStatus.REPAIRED_HISTORICAL_FRAGMENT : RepairStatus.DEFERRED,
                        copied, published));
    }

    private static RepairResult repairFailure(ProfileLedgerMetadataStore.Outcome outcome) {
        return new RepairResult(outcome == ProfileLedgerMetadataStore.Outcome.CONFLICT ? RepairStatus.CONFLICT
                : outcome == ProfileLedgerMetadataStore.Outcome.QUARANTINED ? RepairStatus.QUARANTINED
                    : RepairStatus.DEFERRED, 0, null);
    }

    private static final class Copy {
        private final ProfileRecoveryReadHandle reader;
        private final ProfileNormalDataClient target;
        private final ProfileRepairIntent intent;
        private final ProtectedProfileCredential credential;
        private final AtomicBoolean stopped;
        private final CompletableFuture<RepairResult> done = new CompletableFuture<>();
        private long next;
        private long copied;

        Copy(ProfileRecoveryReadHandle reader, ProfileNormalDataClient target,
                ProfileRepairIntent intent, ProtectedProfileCredential credential,
                AtomicBoolean stopped) {
            this.reader = reader;
            this.target = target;
            this.intent = intent;
            this.credential = credential;
            this.stopped = stopped;
            next = intent.range().firstEntryId();
        }

        void step() {
            if (stopped.get()) {
                done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                return;
            }
            reader.probeEvidence(next).whenComplete((evidence, readFailure) -> {
                if (readFailure != null && !(cause(readFailure) instanceof BKException)) {
                    done.completeExceptionally(readFailure);
                    return;
                }
                if (readFailure != null || stopped.get()) {
                    done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                    return;
                }
                if (evidence.conflict()) {
                    done.complete(new RepairResult(RepairStatus.QUARANTINED, copied, null));
                    return;
                }
                if (evidence.sources().stream().anyMatch(source -> source.outcome()
                        == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                    done.complete(new RepairResult(RepairStatus.CONFLICT, copied, null));
                    return;
                }
                var probe = evidence.candidate();
                if (probe == null) {
                    done.complete(new RepairResult(evidence.exhausted() ? RepairStatus.DATA_LOSS
                            : RepairStatus.DEFERRED, copied, null));
                    return;
                }
                ReferenceCounted packed;
                try {
                    packed = reader.packageRecoveryEntry(probe);
                } catch (RuntimeException invalid) {
                    done.completeExceptionally(invalid);
                    return;
                }
                try {
                    target.addRecovery(intent, credential, next, packed).whenComplete((reply, failure) -> {
                        packed.release();
                        if (failure != null || reply.statusClass() != ProfileStatus.StatusClass.OK
                                || reply.durableResult() != ProfileStatus.DurableResult.APPLIED
                                    && reply.durableResult() != ProfileStatus.DurableResult.ALREADY_APPLIED) {
                            done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                            return;
                        }
                        copied++;
                        if (next == intent.range().lastEntryId()) {
                            done.complete(new RepairResult(RepairStatus.REPAIRED_HISTORICAL_FRAGMENT, copied, null));
                        } else {
                            next++;
                            step(); // probe schedules on the original SDK ordered executor; no growing future chain.
                        }
                    });
                } catch (RuntimeException failure) {
                    packed.release();
                    done.completeExceptionally(failure);
                }
            });
        }
    }

    private static Status failureStatus(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.DEFERRED;
        };
    }
}

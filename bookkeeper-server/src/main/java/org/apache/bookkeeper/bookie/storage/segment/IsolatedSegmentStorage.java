/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.LocalReadinessStore;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts;
import org.apache.bookkeeper.common.profile.startup.StartupCrashPoint;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessHarness;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.common.profile.wire.ProfilePhysicalDeleteCodec;
import org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery;
import org.apache.bookkeeper.discover.profile.ProfileDeleteHandoff;
import org.apache.bookkeeper.discover.profile.ProfileDeleteRecord;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.proto.profile.ProfileInboundPipeline;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Owns the isolated local storage graph. This is not Bookie registration or production ACK readiness. */
final class IsolatedSegmentStorage implements AutoCloseable {
    enum ControlRotationStatus {
        ROTATED, TRANSIENT_UNAVAILABLE, UNKNOWN
    }

    record ControlRotationResult(ControlRotationStatus status, SegmentControlRotation.Root bookieRoot,
                                 SegmentControlRotation.Root arenaRoot) { }

    static final class ControlRotationRequest {
        private final AtomicReference<BiConsumer<ControlRotationResult, Throwable>> response;

        ControlRotationRequest(BiConsumer<ControlRotationResult, Throwable> completion) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(ControlRotationResult result, Throwable failure) {
            BiConsumer<ControlRotationResult, Throwable> completion = response.getAndSet(null);
            if (completion != null) {
                completion.accept(result, failure);
            }
        }
    }
    enum IndexCheckpointStatus {
        PERSISTED, TRANSIENT_UNAVAILABLE, UNKNOWN
    }

    record IndexCheckpointResult(IndexCheckpointStatus status, SegmentDerivedIndex.Coverage coverage) { }

    static final class IndexCheckpointRequest {
        private final AtomicReference<BiConsumer<IndexCheckpointResult, Throwable>> response;
        private final Runnable terminal;

        IndexCheckpointRequest(BiConsumer<IndexCheckpointResult, Throwable> completion) {
            this(completion, null);
        }

        IndexCheckpointRequest(BiConsumer<IndexCheckpointResult, Throwable> completion, Runnable terminal) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
            this.terminal = terminal;
        }

        private void terminal() {
            if (terminal != null) {
                terminal.run();
            }
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(IndexCheckpointResult result, Throwable failure) {
            BiConsumer<IndexCheckpointResult, Throwable> completion = response.getAndSet(null);
            if (completion != null) {
                completion.accept(result, failure);
            }
        }
    }

    enum FreeStatus {
        APPLIED_DURABLE, CONDITION_FAILED, TRANSIENT_UNAVAILABLE, UNKNOWN
    }

    record FreeResult(FreeStatus status, SegmentArenaControlLog.Result control) { }

    enum CompactionStatus {
        RECLAIMED, NO_NET_GAIN, TRANSIENT_UNAVAILABLE, PARTIAL, UNKNOWN
    }

    record CompactionResult(CompactionStatus status, int movedEntries, int freedAllocations,
                            long freedBytes, long destinationBytes, long destinationAllocation) {
        long netFreedBytes() {
            return freedBytes - destinationBytes;
        }
    }

    static final class CompactionRequest {
        private final AtomicReference<BiConsumer<CompactionResult, Throwable>> response;
        private final Runnable terminal;

        CompactionRequest(BiConsumer<CompactionResult, Throwable> completion) {
            this(completion, null);
        }

        CompactionRequest(BiConsumer<CompactionResult, Throwable> completion, Runnable terminal) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
            this.terminal = terminal;
        }

        private void terminal() {
            if (terminal != null) {
                terminal.run();
            }
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(CompactionResult result, Throwable failure) {
            BiConsumer<CompactionResult, Throwable> completion = response.getAndSet(null);
            if (completion != null) {
                completion.accept(result, failure);
            }
        }
    }

    static final class FreeRequest {
        private final AtomicReference<BiConsumer<FreeResult, Throwable>> response;

        FreeRequest(BiConsumer<FreeResult, Throwable> completion) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(FreeResult result, Throwable failure) {
            BiConsumer<FreeResult, Throwable> completion = response.getAndSet(null);
            if (completion != null) {
                completion.accept(result, failure);
            }
        }
    }

    record Limits(long controlLogBytes, int hotEntries, int indexedEntries, int poolBlocks, int maintenanceBlocks,
                  SegmentDataPhysicalDriver.BatchLimits batches) {
        Limits {
            Objects.requireNonNull(batches, "batches");
            if (controlLogBytes < 4096 + 320 || hotEntries < 1 || indexedEntries < hotEntries
                    || poolBlocks < 1 || maintenanceBlocks < 1) {
                throw new IllegalArgumentException("Invalid isolated storage bounds");
            }
        }
    }

    private final Object gate = new Object();
    private final Object closeGate = new Object();
    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final SegmentExecutionGroup.Settings executionSettings;
    private final Limits limits;
    private final ProfileReceiveBudget recoveryBudget;
    private final SegmentDataPhysicalDriver.NormalBatcher[] writes;
    private final SegmentPointRead.Reader[] reads;
    private final SegmentHotDirectory[] hotDirectories;
    private final SegmentDataPhysicalDriver[] drivers;
    private record CompactionCursor(long afterOrder, long throughOrder) { }
    private final CompactionCursor[] compactionCursors;
    private IsolatedSegmentFiles files;
    private SegmentDataFileLock startupLock;
    private SegmentArenaControlLog arena;
    private SegmentBookieControlLog bookie;
    private SegmentDerivedIndex index;
    private volatile SegmentExecutionGroup execution;
    private SegmentBookieControlDriver controls;
    private SegmentArenaControlDriver allocator;
    private boolean startAttempted;
    private boolean accepting;
    private ZKProfileEphemeralRegistrationStore.Admission profileAdmission;
    private SegmentProfileDeleteFollower deleteFollower;
    private boolean closed;
    private IndexCheckpoint indexCheckpoint;
    private FreeOperation freeOperation;
    private CompactionOperation compaction;
    private boolean deleteInspection;
    private final AtomicBoolean physicalDeleteRunning = new AtomicBoolean();
    private ControlRotation controlRotation;
    private volatile Throwable controlMaintenanceFailure;
    private final SpaceSweep[] spaceSweeps;
    private volatile Throwable spaceMaintenanceFailure;

    private static final class SpaceSweep {
        private final long arenaCut;
        private final long bookieCut;
        private int shard;
        private long afterOrder = -1;

        private SpaceSweep(int file, long arenaCut, long bookieCut) {
            shard = file;
            this.arenaCut = arenaCut;
            this.bookieCut = bookieCut;
        }
    }

    /** Construct the resource owner before startup so a failed start still has an explicit cleanup handle. */
    IsolatedSegmentStorage(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
                           ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings executionSettings,
                           Limits limits) {
        this(config, identity, key, validator, executionSettings, limits, null);
    }

    IsolatedSegmentStorage(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
                           ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings executionSettings,
                           Limits limits, ProfileReceiveBudget recoveryBudget) {
        this.recoveryBudget = recoveryBudget;
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.executionSettings = Objects.requireNonNull(executionSettings, "executionSettings");
        this.limits = Objects.requireNonNull(limits, "limits");
        if (limits.hotEntries() < config.logicalShards()) {
            throw new IllegalArgumentException("Global hot capacity must provide at least one entry per shard");
        }
        long blocks = (config.bytesPerFile() - 4096) / SegmentArenaControlLog.BLOCK_BYTES;
        if ((long) limits.poolBlocks() + limits.maintenanceBlocks() > blocks) {
            throw new IllegalArgumentException("Each DATA file needs one foreground pool and its maintenance reserve");
        }
        writes = new SegmentDataPhysicalDriver.NormalBatcher[config.logicalShards()];
        reads = new SegmentPointRead.Reader[config.logicalShards()];
        hotDirectories = new SegmentHotDirectory[config.logicalShards()];
        drivers = new SegmentDataPhysicalDriver[config.logicalShards()];
        compactionCursors = new CompactionCursor[config.logicalShards()];
        spaceSweeps = new SpaceSweep[config.dataFiles()];
    }

    void startNew() throws IOException, InterruptedException {
        start(false);
    }

    /** Bind the real Profile registration attempt before opening this otherwise local-only graph. */
    void bindProfileRegistration(ZKProfileEphemeralRegistrationStore.Admission admission) {
        synchronized (gate) {
            if (startAttempted || closed || profileAdmission != null) {
                throw new IllegalStateException("Profile registration must be bound once before startup");
            }
            if (!Objects.requireNonNull(admission, "admission").bookieId().equals(identity.bookieId().toString())) {
                throw new IllegalArgumentException("Profile registration belongs to another Bookie");
            }
            profileAdmission = admission;
        }
    }

    /** A fresh nonce may replace a retired gate only after the original control log made G+1 effective. */
    void bindNextProfileRegistration(ZKProfileEphemeralRegistrationStore.Admission admission, Duration timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        Objects.requireNonNull(admission, "admission");
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0
                || controls == null || controls.isOwnerThread()) {
            throw new IllegalArgumentException("Next registration requires a bounded cold caller");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        SegmentProfileDeleteFollower previous;
        ZKProfileEphemeralRegistrationStore.Admission retired;
        synchronized (gate) {
            retired = profileAdmission;
            if (!accepting || closed || retired == null || !retired.isRetired()
                    || bookie.effectiveDeleteHandoff() == null
                    || bookie.effectiveDeleteProgress().assignmentGeneration() <= retired.openedAssignmentGeneration()
                    || !admission.bookieId().equals(identity.bookieId().toString())) {
                throw new IllegalStateException("Next registration has no new applied effective assignment");
            }
            previous = deleteFollower;
        }
        if (previous != null) {
            previous.closeAsync().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        }
        synchronized (gate) {
            if (!accepting || closed || profileAdmission != retired || retired.getAsBoolean()
                    || !bookie.matchesScope(config, identity)) {
                throw new IllegalStateException("Original registration handoff owner changed during actual drain");
            }
            deleteFollower = null;
            profileAdmission = admission;
        }
    }

    boolean retireProfileAdmissionForHandoff(SegmentBookieControlLog source, ProfileDeleteHandoff.Plan plan,
            ZKProfileEphemeralRegistrationStore.Admission expected) {
        synchronized (gate) {
            if (!accepting || closed || bookie != source || profileAdmission != expected || expected == null
                    || source.adoptedDeleteHandoffSequence(plan) < 0
                    || !expected.bookieId().equals(identity.bookieId().toString())) {
                return false;
            }
            expected.close();
            return true;
        }
    }

    ProfileDeleteDelivery.Progress profileDeleteAssignmentProgress() {
        synchronized (gate) {
            if (!accepting || closed || bookie == null || !bookie.matchesScope(config, identity)) {
                throw new IllegalStateException("Original local assignment recovery is not available");
            }
            return bookie.effectiveDeleteProgress();
        }
    }

    enum DeleteCatchUpStatus { CAUGHT_UP, LIMIT, UNAVAILABLE, QUARANTINED, UNKNOWN }

    record DeleteCatchUpAttempt(DeleteCatchUpStatus status, ProfileDeleteDelivery.Head required,
            CompletableFuture<Void> completion) {
        DeleteCatchUpAttempt {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(completion, "completion");
        }
        @Override
        public CompletableFuture<Void> completion() {
            return completion.copy();
        }
    }

    /** Cold registration supplier, derived from this opened graph's original recovered control log. */
    ProfileControlRequest.DeleteBinding profileDeleteProgress() {
        synchronized (gate) {
            if (!accepting || closed || bookie == null || !bookie.matchesScope(config, identity)) {
                throw new IllegalStateException("Original local delete recovery is not available");
            }
            return bookie.deleteAppliedCursor();
        }
    }

    enum DeleteAdoptionStatus { ADOPTED_DURABLE, UNAVAILABLE, QUARANTINED, UNKNOWN }

    record DeleteAdoptionAttempt(DeleteAdoptionStatus status, SegmentProfileDeleteAdoption adoption,
            CompletableFuture<Void> completion) {
        DeleteAdoptionAttempt {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(completion, "completion");
            if ((status == DeleteAdoptionStatus.ADOPTED_DURABLE) != (adoption != null)) {
                throw new IllegalArgumentException("Only a durable local adoption carries its plan");
            }
        }
        ProfileDeleteHandoff.Plan plan() {
            return adoption == null ? null : adoption.plan();
        }
        @Override
        public CompletableFuture<Void> completion() {
            return completion.copy();
        }
    }

    /** Native cold authority read, complete old cut, original force/apply, then fresh native recheck. */
    DeleteAdoptionAttempt adoptPreparedProfileDeletes(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, Duration timeout, BooleanSupplier live) {
        Objects.requireNonNull(reservations, "reservations");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(live, "live");
        if (maxEvents < 1 || maxEvents > 4096 || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0 || controls == null || controls.isOwnerThread()) {
            throw new IllegalArgumentException("Delete adoption requires a bounded cold caller");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        var actual = CompletableFuture.<Void>completedFuture(null);
        var target = new ProfileDeleteRecord.Target(identity.bookieId().toString(), identity.incarnation().value());
        try {
            var source = reservations.readPreparedDeleteHandoff(target);
            if (source.status() != ZKProfileReservationStore.DeleteHandoffStatus.PREPARED) {
                return new DeleteAdoptionAttempt(source.status()
                        == ZKProfileReservationStore.DeleteHandoffStatus.QUARANTINED
                            ? DeleteAdoptionStatus.QUARANTINED : DeleteAdoptionStatus.UNAVAILABLE, null, actual);
            }
            if (!live.getAsBoolean() || System.nanoTime() >= deadline) {
                return new DeleteAdoptionAttempt(DeleteAdoptionStatus.UNAVAILABLE, null, actual);
            }
            var catchUp = catchUpProfileDeletes(reservations, metadata, domain, maxEvents,
                    Duration.ofNanos(deadline - System.nanoTime()), live);
            actual = catchUp.completion();
            if (catchUp.status() != DeleteCatchUpStatus.CAUGHT_UP) {
                return new DeleteAdoptionAttempt(switch (catchUp.status()) {
                    case QUARANTINED -> DeleteAdoptionStatus.QUARANTINED;
                    case UNKNOWN -> DeleteAdoptionStatus.UNKNOWN;
                    default -> DeleteAdoptionStatus.UNAVAILABLE;
                }, null, actual);
            }
            actual.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            var fresh = reservations.readPreparedDeleteHandoff(target);
            if (fresh.status() != ZKProfileReservationStore.DeleteHandoffStatus.PREPARED
                    || !Arrays.equals(fresh.plan().encode(), source.plan().encode())) {
                return new DeleteAdoptionAttempt(DeleteAdoptionStatus.QUARANTINED, null, actual);
            }
            var result = new CompletableFuture<SegmentBookieControlLog.Status>();
            var terminal = new CompletableFuture<Void>();
            SegmentBookieControlLog appliedOwner;
            synchronized (gate) {
                if (!accepting || closed || !live.getAsBoolean() || System.nanoTime() >= deadline
                        || !controls.submitHandoff(source.plan(), (value, failure) -> {
                            if (failure == null) {
                                result.complete(value);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        }, () -> terminal.complete(null))) {
                    return new DeleteAdoptionAttempt(DeleteAdoptionStatus.UNAVAILABLE, null, actual);
                }
                appliedOwner = bookie;
            }
            actual = terminal;
            var applied = result.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            actual.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (applied != SegmentBookieControlLog.Status.APPLIED_DURABLE
                    && applied != SegmentBookieControlLog.Status.ALREADY_DURABLE) {
                return new DeleteAdoptionAttempt(DeleteAdoptionStatus.QUARANTINED, null, actual);
            }
            fresh = reservations.readPreparedDeleteHandoff(target);
            if (fresh.status() != ZKProfileReservationStore.DeleteHandoffStatus.PREPARED
                    || !Arrays.equals(fresh.plan().encode(), source.plan().encode())) {
                return new DeleteAdoptionAttempt(DeleteAdoptionStatus.QUARANTINED, null, actual);
            }
            var adoption = SegmentProfileDeleteAdoption.fromApplied(this, appliedOwner, source.plan());
            return adoption == null
                    ? new DeleteAdoptionAttempt(DeleteAdoptionStatus.UNAVAILABLE, null, actual)
                    : new DeleteAdoptionAttempt(DeleteAdoptionStatus.ADOPTED_DURABLE, adoption, actual);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeleteAdoptionAttempt(DeleteAdoptionStatus.UNKNOWN, null, actual);
        } catch (ExecutionException | TimeoutException unknown) {
            return new DeleteAdoptionAttempt(DeleteAdoptionStatus.UNKNOWN, null, actual);
        } catch (IllegalArgumentException | IllegalStateException invalid) {
            return new DeleteAdoptionAttempt(DeleteAdoptionStatus.QUARANTINED, null, actual);
        }
    }

    private record EffectiveLocalAttempt(DeleteCatchUpStatus status, CompletableFuture<Void> completion) { }

    /** Original native witness only; repeat both local stages after UNKNOWN without inventing an event. */
    private EffectiveLocalAttempt applyEffectiveProfileDeletes(ZKProfileReservationStore reservations,
            ProfileDeleteHandoff.Effective effective, long deadline, BooleanSupplier live) {
        CompletableFuture<Void> actual = CompletableFuture.completedFuture(null);
        var target = effective.plan().predecessor().target();
        try {
            for (boolean adopt : new boolean[] {true, false}) {
                var fresh = reservations.readEffectiveDeleteHandoff(target, effective.plan().nextGeneration());
                if (fresh.value() == null || !Arrays.equals(fresh.value().encode(), effective.encode())) {
                    return new EffectiveLocalAttempt(fresh.status()
                            == ZKProfileReservationStore.DeleteHandoffEffectiveStatus.QUARANTINED
                                ? DeleteCatchUpStatus.QUARANTINED : DeleteCatchUpStatus.UNAVAILABLE, actual);
                }
                var result = new CompletableFuture<SegmentBookieControlLog.Status>();
                var terminal = new CompletableFuture<Void>();
                synchronized (gate) {
                    if (!accepting || closed || !live.getAsBoolean() || System.nanoTime() >= deadline) {
                        return new EffectiveLocalAttempt(DeleteCatchUpStatus.UNAVAILABLE, actual);
                    }
                    SegmentBookieControlDriver.LacCompletion completed = (value, failure) -> {
                        if (failure == null) {
                            result.complete(value);
                        } else {
                            result.completeExceptionally(failure);
                        }
                    };
                    boolean accepted = adopt
                            ? controls.submitHandoff(effective.plan(), completed, () -> terminal.complete(null))
                            : controls.submitEffectiveHandoff(effective, completed, () -> terminal.complete(null));
                    if (!accepted) {
                        return new EffectiveLocalAttempt(DeleteCatchUpStatus.UNAVAILABLE, actual);
                    }
                }
                actual = terminal;
                var value = result.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                actual.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (value != SegmentBookieControlLog.Status.APPLIED_DURABLE
                        && value != SegmentBookieControlLog.Status.ALREADY_DURABLE) {
                    return new EffectiveLocalAttempt(DeleteCatchUpStatus.QUARANTINED, actual);
                }
            }
            var fresh = reservations.readEffectiveDeleteHandoff(target, effective.plan().nextGeneration());
            return new EffectiveLocalAttempt(fresh.value() != null
                    && Arrays.equals(fresh.value().encode(), effective.encode())
                        ? DeleteCatchUpStatus.CAUGHT_UP : DeleteCatchUpStatus.UNAVAILABLE, actual);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new EffectiveLocalAttempt(DeleteCatchUpStatus.UNKNOWN, actual);
        } catch (ExecutionException | TimeoutException unknown) {
            return new EffectiveLocalAttempt(DeleteCatchUpStatus.UNKNOWN, actual);
        }
    }

    /** Bounded cold startup/rejoin replay; no executor, remote abort or physical reclaim wait is added. */
    DeleteCatchUpAttempt catchUpProfileDeletes(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, Duration timeout) {
        return catchUpProfileDeletes(reservations, metadata, domain, maxEvents, timeout, () -> true);
    }

    DeleteCatchUpAttempt catchUpProfileDeletes(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, Duration timeout, BooleanSupplier live) {
        Objects.requireNonNull(live, "live");
        Objects.requireNonNull(reservations, "reservations");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(domain, "domain");
        if (maxEvents < 1 || maxEvents > 4096 || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Delete catch-up requires a bounded cold caller");
        }
        SegmentBookieControlDriver originalControls;
        synchronized (gate) {
            if (closed) {
                return new DeleteCatchUpAttempt(DeleteCatchUpStatus.QUARANTINED, null,
                        CompletableFuture.completedFuture(null));
            }
            originalControls = controls;
        }
        if (originalControls == null || originalControls.isOwnerThread()) {
            throw new IllegalArgumentException("Delete catch-up requires a bounded cold caller");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        var target = new ProfileDeleteRecord.Target(identity.bookieId().toString(), identity.incarnation().value());
        ZKProfileReservationStore.DeleteSnapshotView snapshot = null;
        boolean snapshotLoaded = false;
        CompletableFuture<Void> actual = CompletableFuture.completedFuture(null);
        ProfileDeleteDelivery.Head required = null;
        int applied = 0;
        try {
            while (live.getAsBoolean() && System.nanoTime() < deadline) {
                var read = reservations.readDeleteHead(target);
                if (read.status() != ProfileControlStore.ReadStatus.FOUND) {
                    return new DeleteCatchUpAttempt(read.status() == ProfileControlStore.ReadStatus.QUARANTINED
                            ? DeleteCatchUpStatus.QUARANTINED : DeleteCatchUpStatus.UNAVAILABLE,
                                required, actual);
                }
                required = read.head();
                var cursor = profileDeleteAssignmentProgress();
                if (cursor != null && (!Arrays.equals(cursor.streamId(), required.streamId())
                        || cursor.streamGeneration() != required.streamGeneration()
                        || cursor.assignmentGeneration() > required.assignmentGeneration())) {
                    return new DeleteCatchUpAttempt(DeleteCatchUpStatus.QUARANTINED, required, actual);
                }
                long through = cursor == null ? 0 : cursor.through();
                long assignment = cursor == null ? 1 : cursor.assignmentGeneration();
                if (assignment < required.assignmentGeneration()) {
                    var effective = reservations.readEffectiveDeleteHandoff(target, assignment + 1);
                    if (effective.value() == null) {
                        return new DeleteCatchUpAttempt(effective.status()
                                == ZKProfileReservationStore.DeleteHandoffEffectiveStatus.QUARANTINED
                                    ? DeleteCatchUpStatus.QUARANTINED : DeleteCatchUpStatus.UNAVAILABLE,
                                required, actual);
                    }
                    long cut = effective.value().plan().predecessor().committedThrough();
                    if (through > cut) {
                        return new DeleteCatchUpAttempt(DeleteCatchUpStatus.QUARANTINED, required, actual);
                    }
                    if (through == cut) {
                        var appliedAssignment = applyEffectiveProfileDeletes(
                                reservations, effective.value(), deadline, live);
                        actual = appliedAssignment.completion();
                        if (appliedAssignment.status() != DeleteCatchUpStatus.CAUGHT_UP) {
                            return new DeleteCatchUpAttempt(appliedAssignment.status(), required, actual);
                        }
                        snapshot = null;
                        snapshotLoaded = false;
                        continue;
                    }
                }
                if (through > required.committedThrough()) {
                    return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNAVAILABLE, required, actual);
                }
                if (through == required.committedThrough() && assignment == required.assignmentGeneration()) {
                    return new DeleteCatchUpAttempt(DeleteCatchUpStatus.CAUGHT_UP, required, actual);
                }
                if (applied == maxEvents) {
                    return new DeleteCatchUpAttempt(DeleteCatchUpStatus.LIMIT, required, actual);
                }
                if (!snapshotLoaded) {
                    var loaded = reservations.readDeleteSnapshot(target);
                    if (loaded.status() != ZKProfileReservationStore.DeleteSnapshotStatus.FOUND
                            && loaded.status() != ZKProfileReservationStore.DeleteSnapshotStatus.ABSENT) {
                        return new DeleteCatchUpAttempt(loaded.status()
                                == ZKProfileReservationStore.DeleteSnapshotStatus.QUARANTINED
                                    ? DeleteCatchUpStatus.QUARANTINED : DeleteCatchUpStatus.UNAVAILABLE,
                                required, actual);
                    }
                    snapshot = loaded.snapshot();
                    snapshotLoaded = true;
                }
                var delivery = reservations.readDeleteDelivery(metadata, target, through + 1, snapshot);
                if (delivery.status() != ZKProfileReservationStore.DeliveryStatus.COMMITTED
                        || !Arrays.equals(delivery.event().streamId(), required.streamId())) {
                    return new DeleteCatchUpAttempt(delivery.status()
                            == ZKProfileReservationStore.DeliveryStatus.UNAVAILABLE
                            || delivery.status() == ZKProfileReservationStore.DeliveryStatus.PREPARED
                            ? DeleteCatchUpStatus.UNAVAILABLE : DeleteCatchUpStatus.QUARANTINED, required, actual);
                }
                var request = delivery.event().deleteApply(domain);
                var authority = reservations.deleteAuthorityReader(target.bookie(), target.incarnation(),
                        metadata, snapshot).read(ProfileAuthorityKey.fromScope(request.scope()));
                if (authority.authority() == null || !authority.authority().scope().equals(request.scope())) {
                    return new DeleteCatchUpAttempt(authority.status() == ProfileControlStore.ReadStatus.UNAVAILABLE
                            ? DeleteCatchUpStatus.UNAVAILABLE : DeleteCatchUpStatus.QUARANTINED, required, actual);
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !live.getAsBoolean()) {
                    break;
                }
                actual = new CompletableFuture<>();
                var terminal = actual;
                var store = new SegmentProfileInstallStore(originalControls, identity, Duration.ofNanos(remaining));
                var result = store.conditionalApply(new ProtectedProfileStateStore.Transition(
                        request, authority.authority(), null), () -> terminal.complete(null));
                switch (result.outcome()) {
                    case APPLIED, ALREADY_APPLIED -> {
                        actual.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                        applied++;
                    }
                    case DURABILITY_UNKNOWN -> {
                        return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNKNOWN, required, actual);
                    }
                    case TRANSIENT_RECONCILING -> {
                        return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNAVAILABLE, required, actual);
                    }
                    default -> {
                        return new DeleteCatchUpAttempt(DeleteCatchUpStatus.QUARANTINED, required, actual);
                    }
                }
            }
            return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNAVAILABLE, required, actual);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNKNOWN, required, actual);
        } catch (ExecutionException | TimeoutException unknown) {
            return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNKNOWN, required, actual);
        } catch (IllegalArgumentException | IllegalStateException invalid) {
            return new DeleteCatchUpAttempt(DeleteCatchUpStatus.QUARANTINED, required, actual);
        }
    }

    /** One original registration attempt, borrowing the caller's already bounded cold executor and scheduler. */
    SegmentProfileDeleteFollower startDeleteFollower(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, ScheduledExecutorService scheduler,
            ThreadPoolExecutor cold, int maxEvents, Duration probeTimeout, Duration pollInterval) {
        synchronized (gate) {
            if (!accepting || closed || deleteFollower != null || profileAdmission == null
                    || !profileAdmission.allowsDeleteFollower(reservations, identity.incarnation())) {
                throw new IllegalStateException("Delete follower requires this opened, registered original attempt");
            }
            deleteFollower = new SegmentProfileDeleteFollower(this, reservations, metadata, domain, profileAdmission,
                    identity.incarnation(), scheduler, cold, maxEvents, probeTimeout, pollInterval);
            deleteFollower.start();
            return deleteFollower;
        }
    }

    // Reference feature 1 denotes the mandatory experimental version-1 header/control/framing base.
    static final int STARTUP_BASE_FORMAT_FEATURE = 1;
    static final int STARTUP_READER_VERSION = 1;
    static final int STARTUP_WRITER_VERSION = 1;

    /** Expected full single-Arena manifest identity, independent of a readable filesystem subset. */
    static ManifestIdentity startupDeviceManifestIdentity(
            IsolatedSegmentConfig config, SegmentFileHeader identity) {
        try {
            var hash = java.security.MessageDigest.getInstance("SHA-256");
            hash.update("BookKeeper/IsolatedStartupDevice/v1\0".getBytes(StandardCharsets.US_ASCII));
            hash.update(identity.encode(config, -1));
            return new ManifestIdentity(hash.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /** Explicit pre-construction check; no storage directory is opened and no observed recovery is invented. */
    static StartupReadinessInput startupPolicy(ZKProfileStartupRequirementsStore.Observation source,
            StartupReadinessInput requested, IsolatedSegmentConfig config, SegmentFileHeader identity)
            throws IOException {
        var policy = Objects.requireNonNull(source, "source").applyTo(requested);
        requireStartupPolicyScope(policy, config, identity);
        var compatibility = policy.compatibility();
        if (compatibility.migrationState()
                    != org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact.MigrationState.FORMAT_READY
                || STARTUP_READER_VERSION < compatibility.minimumReaderVersion()
                || STARTUP_WRITER_VERSION < compatibility.minimumWriterVersion()
                || !policy.newScopeFallback().permitsNewScopeWritable()
                || !policy.newScopeFallback().newBookieId().equals(identity.bookieId().toString())
                || !policy.newScopeFallback().newStorageIncarnation().equals(identity.incarnation())) {
            throw new IOException("Pre-open startup requirements or new-scope policy are not ready");
        }
        return policy;
    }

    record StartupFacts(StartupReadinessInput input,
            DeleteCatchUpStatus deleteStatus, CompletableFuture<Void> completion) {
        @Override
        public CompletableFuture<Void> completion() {
            return completion.copy();
        }
    }

    /** Replaces supplied device/recovery conclusions with this actual graph and committed delete catch-up. */
    StartupFacts recoveredStartupFacts(StartupReadinessInput policy,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata, String domain,
            int maxEvents, Duration timeout) throws IOException {
        return recoveredStartupFacts(policy, reservations, metadata, domain, maxEvents, timeout, () -> true);
    }

    private StartupFacts recoveredStartupFacts(StartupReadinessInput policy,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata, String domain,
            int maxEvents, Duration timeout, BooleanSupplier live) throws IOException {
        Objects.requireNonNull(policy, "policy");
        requireStartupPolicyScope(policy, config, identity);
        var one = new Generation(1);
        var digest = startupDeviceManifestIdentity(config, identity);
        var caughtUp = catchUpProfileDeletes(reservations, metadata, domain, maxEvents, timeout, live);
        if (caughtUp.status() != DeleteCatchUpStatus.CAUGHT_UP) {
            return new StartupFacts(null, caughtUp.status(), caughtUp.completion());
        }
        synchronized (gate) {
            if (!accepting || closed || !files.matchesScope(config, identity)
                    || !arena.matchesScope(config, identity) || !bookie.matchesScope(config, identity)) {
                throw new IOException("Original recovered graph is no longer available");
            }
            // Actual opened graph has validated every required header, recovered/synced control and DATA,
            // restored derived locations and bootstrapped its fixed owners. A caller READY cannot replace these steps.
            bookie.recoveryReadCut(config, identity);
            arena.requireStartupRecovered(config, identity);
            var observed = new RequiredDeviceManifest(digest, one,
                    List.of(new RequiredDeviceManifest.RequiredDeviceFact(
                            identity.arenaId().toString(), identity.incarnation(), one,
                            RequiredDeviceManifest.DeviceStatus.READY,
                            1, 1, 1, one, List.of(STARTUP_BASE_FORMAT_FEATURE))));
            var status = LocalRecoveryFacts.RecoveryStatus.RECOVERED;
            var recovery = new LocalRecoveryFacts(identity.incarnation(),
                    one, status, status, status, new Generation(caughtUp.required().assignmentGeneration()));
            var controls = bookie.rollbackSafetyFacts();
            var rollback = new RollbackSafetyFacts(
                    controls.localSuccess(), controls.routeOrInstall(), controls.activation(), controls.fenceOrGrant(),
                    controls.tombstone(), arena.appliedThrough() >= 0, controls.durabilityUnknown());
            var actual = new StartupReadinessInput(policy.compatibility(),
                    observed, recovery, policy.newScopeFallback(), rollback, policy.targetReadinessGeneration(),
                    policy.capabilityGeneration(), STARTUP_READER_VERSION, STARTUP_WRITER_VERSION,
                    List.of(STARTUP_BASE_FORMAT_FEATURE), policy.profileEndpoint(), policy.capabilityHints());
            return new StartupFacts(actual, caughtUp.status(), caughtUp.completion());
        }
    }

    private static void requireStartupPolicyScope(StartupReadinessInput policy,
            IsolatedSegmentConfig config, SegmentFileHeader identity) throws IOException {
        var compatibility = policy.compatibility();
        var expected = policy.requiredDevices();
        var one = new Generation(1);
        var digest = startupDeviceManifestIdentity(config, identity);
        if (!compatibility.bookieId().equals(identity.bookieId().toString())
                || !compatibility.storageIncarnation().equals(identity.incarnation())
                || compatibility.activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || compatibility.profileWireMajor() != 1 || compatibility.profileWireMinor() != 0
                || compatibility.descriptorHashSuite() != 1
                || compatibility.minimumReaderVersion() > STARTUP_READER_VERSION
                || compatibility.minimumWriterVersion() > STARTUP_WRITER_VERSION
                || !compatibility.mandatoryLocalFeatures().equals(List.of(STARTUP_BASE_FORMAT_FEATURE))
                || !compatibility.localFormatGeneration().equals(one)
                || !compatibility.deviceManifestGeneration().equals(one)
                || !compatibility.deviceManifestIdentity().equals(digest)
                || !expected.identity().equals(digest) || !expected.generation().equals(one)
                || expected.devices().size() != 1) {
            throw new IOException("Required compatibility/device scope does not match this actual isolated graph");
        }
        var device = expected.devices().get(0);
        if (!device.deviceId().equals(identity.arenaId().toString())
                || !device.storageIncarnation().equals(identity.incarnation())
                || !device.manifestGeneration().equals(one) || device.arenaFormatVersion() != 1
                || device.controlFormatVersion() != 1 || device.checkpointFormatVersion() != 1
                || !device.superblockGeneration().equals(one)
                || !device.mandatoryFeatures().equals(List.of(STARTUP_BASE_FORMAT_FEATURE))) {
            throw new IOException("Required Arena identity/generation/format is unsupported or mismatched");
        }
    }

    record ReadinessAttempt(StartupReadinessResult readiness, DeleteCatchUpStatus deleteStatus,
            CompletableFuture<Void> completion) {
        @Override
        public CompletableFuture<Void> completion() {
            return completion.copy();
        }
    }

    /** Cold bridge to the original startup state machine. Compatibility/fallback policy still needs its issuer. */
    ReadinessAttempt reconcileReadiness(StartupReadinessInput policy, LocalReadinessStore local,
            ZKProfileRegistrationStore persistent, ZKProfileEphemeralRegistrationStore publisher,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata, String domain,
            ScheduledExecutorService scheduler, ThreadPoolExecutor cold, int maxEvents, Duration timeout,
            Duration pollInterval, BooleanSupplier live) throws IOException {
        Objects.requireNonNull(live, "live");
        if (profileAdmission == null) {
            throw new IllegalStateException("Readiness requires the original pre-bound pending admission");
        }
        publisher.requireStartupStores(profileAdmission, persistent, reservations, identity.incarnation());
        publisher.requireStartupPolicy(policy);
        var facts = recoveredStartupFacts(policy, reservations, metadata, domain, maxEvents, timeout, live);
        if (facts.input() == null) {
            return new ReadinessAttempt(null, facts.deleteStatus(), facts.completion());
        }
        if (!live.getAsBoolean()) {
            publisher.demote(identity.bookieId().toString());
            return new ReadinessAttempt(null, DeleteCatchUpStatus.UNAVAILABLE, facts.completion());
        }
        var readiness = new StartupReadinessHarness(local, persistent, publisher)
                .run(facts.input(), StartupCrashPoint.NONE);
        if (readiness.outcome() != StartupReadinessResult.Outcome.WRITABLE) {
            return new ReadinessAttempt(readiness, facts.deleteStatus(), facts.completion());
        }
        try {
            var registration = publisher.readWritable(identity.bookieId().toString()).orElseThrow();
            if (!live.getAsBoolean() || !profileAdmission.openAfterReconciliation(registration)) {
                throw new IllegalStateException("Startup attempt retired before final local admission");
            }
            var follower = startDeleteFollower(reservations, metadata, domain, scheduler, cold, maxEvents,
                    timeout, pollInterval);
            if (!live.getAsBoolean() || !profileAdmission.getAsBoolean() || follower.failure() != null) {
                follower.closeAsync();
                throw new IllegalStateException("Original startup consumer did not retain admission");
            }
            return new ReadinessAttempt(readiness, facts.deleteStatus(), facts.completion());
        } catch (RuntimeException retired) {
            publisher.demote(identity.bookieId().toString());
            throw retired;
        }
    }

    private boolean writableRegistrationAllowsAdmission() {
        return profileAdmission == null
                || profileAdmission.allows(identity.bookieId().toString(), identity.incarnation());
    }

    void restart() throws IOException, InterruptedException {
        start(true);
    }

    private void start(boolean existing) throws IOException, InterruptedException {
        synchronized (closeGate) {
            synchronized (gate) {
                if (startAttempted || closed) {
                    throw new IllegalStateException("Storage startup already attempted or storage closed");
                }
                startAttempted = true;
            }
            try {
                if (Files.exists(config.directory().resolve(SegmentBackupImage.MARKER), LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Offline backup input cannot start a storage service");
                }
                if (!existing) {
                    files = IsolatedSegmentFiles.create(config, identity);
                } else {
                    // Exclude another DATA writer before any control replay, recovery force or index open.
                    startupLock = SegmentDataFileLock.open(config.directory());
                }
                arena = SegmentArenaControlLog.open(config, identity, limits.controlLogBytes());
                bookie = SegmentBookieControlLog.open(config, identity, limits.controlLogBytes(), key, validator);
                index = SegmentDerivedIndex.open(config, identity, limits.indexedEntries());
                if (existing) {
                    files = IsolatedSegmentFiles.openForLocalResults(config, identity, arena, bookie,
                            new SegmentFileIo(), limits.indexedEntries(), config.logicalShards(), index, startupLock,
                            recoveryBudget);
                    startupLock = null; // The same native lock now belongs to files.
                } else {
                    files.startSession(arena);
                }
                execution = SegmentExecutionGroup.start(config, executionSettings);
                controls = new SegmentBookieControlDriver(execution.owner(0), execution.io(), bookie);
                bootstrap();
                synchronized (gate) {
                    accepting = true;
                }
            } catch (IOException | InterruptedException | RuntimeException | Error failure) {
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failure.addSuppressed(new IOException("Storage startup cleanup still requires drain"));
                    }
                } catch (IOException | InterruptedException | RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw failure;
            }
        }
    }

    private void bootstrap() throws IOException, InterruptedException {
        allocator = new SegmentArenaControlDriver(execution.owner(0), execution.io(), arena,
                limits.maintenanceBlocks());
        long end = System.nanoTime() + executionSettings.startupTimeout().toNanos();
        for (int shard = 0; shard < config.logicalShards(); shard++) {
            final int selected = shard;
            SegmentOwnerLoop owner = execution.owner(shard);
            FutureTask<Void> task = new FutureTask<>(() -> {
                SegmentArenaControlLog.SingleStream stream = arena.restartStreamForShard(selected).orElse(
                        new SegmentArenaControlLog.SingleStream(new Generation(1), 1));
                int capacity = limits.hotEntries() / config.logicalShards()
                        + (selected < limits.hotEntries() % config.logicalShards() ? 1 : 0);
                int coordinates = limits.indexedEntries() / config.logicalShards()
                        + (selected < limits.indexedEntries() % config.logicalShards() ? 1 : 0);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity, capacity, coordinates);
                hotDirectories[selected] = hot;
                hot.attachIndex(selected, index, execution.io());
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(selected, owner, execution.io(),
                        files, arena, new SegmentPhysicalPrefix(owner, selected, stream.generation(),
                                stream.firstSequence(), config.maxPendingEntries()), hot);
                drivers[selected] = driver;
                writes[selected] = driver.enableNormalBatching(
                        bookie, limits.batches(), allocator, limits.poolBlocks());
                reads[selected] = SegmentPointRead.Reader.live(owner, execution.io(), files, arena, bookie, hot);
                if (selected < executionSettings.workers()) {
                    owner.attachControlMaintenance(this::maintainPressuredCapacity);
                }
                return null;
            });
            if (!owner.control(task)) {
                throw new IOException("Storage bootstrap control capacity exhausted");
            }
            // Do not fill a worker's bounded control queue with the other shards it owns.
            await(task, end);
        }
        for (int worker = 0; worker < executionSettings.workers(); worker++) {
            SegmentOwnerLoop owner = execution.owner(worker);
            while (owner.hasPendingCommands()) {
                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Storage bootstrap commands did not terminate");
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException("Storage bootstrap interrupted");
                }
                LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
            }
            if (owner.failure() != null) {
                throw new IOException("Storage bootstrap owner failed", owner.failure());
            }
        }
    }

    private static void await(FutureTask<Void> task, long end) throws IOException, InterruptedException {
        try {
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException("Storage bootstrap deadline expired");
            }
            task.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IOException("Storage owner bootstrap failed", failure);
        }
    }

    /** Borrow the original control driver for a bounded, synchronous cold INSTALL endpoint. */
    SegmentProfileInstallStore profileInstallStore(Duration timeout) {
        return new SegmentProfileInstallStore(controls(), identity, timeout);
    }

    SegmentBookieControlDriver controls() {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                throw new IllegalStateException("Storage is not accepting control requests");
            }
            return controls;
        }
    }

    /** Borrow and copy the entry into the existing shared source budget; refusal never owns the caller's buffer. */
    boolean submitNormal(ProfileLedgerContext context, byte[] credential, ByteBuf entry,
                         SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || !writableRegistrationAllowsAdmission()
                    || freeOperation != null || compaction != null
                    || controlRotation != null || controlCapacityPaused() || dataCapacityPaused()) {
                return false;
            }
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(credential, "credential");
            Objects.requireNonNull(entry, "entry");
            Objects.requireNonNull(request, "request");
            SegmentBookieControlLog.Admission admission = bookie.admitNormal(context, credential).orElse(null);
            if (admission == null) {
                return false;
            }
            SegmentEntryInputs.Input input = null;
            try {
                input = execution.inputs().acquire(context, entry.readableBytes(),
                        ProfileInboundPipeline.inputCopyAllocator(entry));
                if (input == null) {
                    admission.physicalTerminal();
                    return false;
                }
                input.bytes().writeBytes(entry, entry.readerIndex(), entry.readableBytes());
            } catch (RuntimeException | Error failure) {
                if (input != null) {
                    input.close();
                }
                admission.physicalTerminal();
                throw failure;
            }
            int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
            try {
                boolean accepted = writes[shard].submit(input, admission, request);
                if (!accepted) {
                    controlCapacityPaused();
                }
                return accepted;
            } catch (IOException | RuntimeException | Error failure) {
                input.close();
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    /** Disposable socket integration only; the supplied readiness generation is not a recovery proof. */
    boolean installProfileConnection(io.netty.channel.ChannelPipeline pipeline, TLSContextFactory tls,
                                     ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                                     String trustDomain, long readinessGeneration)
            throws org.apache.bookkeeper.tls.SecurityException {
        return installProfileConnection(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                trustDomain, readinessGeneration, null);
    }

    /** Explicit opt-in for the disposable initial-control schema; generic control wire remains blocked. */
    boolean installProfileConnection(io.netty.channel.ChannelPipeline pipeline, TLSContextFactory tls,
                                     ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                                     String trustDomain, long readinessGeneration, SegmentProfileControlConfig control)
            throws org.apache.bookkeeper.tls.SecurityException {
        return installProfileConnection(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                trustDomain, readinessGeneration, control, null);
    }

    boolean installProfileConnection(io.netty.channel.ChannelPipeline pipeline, TLSContextFactory tls,
                                     ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                                     String trustDomain, long readinessGeneration, SegmentProfileControlConfig control,
                                     SegmentProfileReadConfig read)
            throws org.apache.bookkeeper.tls.SecurityException {
        synchronized (gate) {
            if (!accepting) {
                pipeline.channel().close();
                return false;
            }
            List<ProfileHello.Capability> capabilities = new java.util.ArrayList<>();
            if (control != null) {
                capabilities.add(new ProfileHello.Capability(ProfileInitialControlCodec.CAPABILITY_ID,
                        ProfileInitialControlCodec.SCHEMA));
                capabilities.add(new ProfileHello.Capability(ProfileInitialControlCodec.REPLACEMENT_CAPABILITY_ID,
                        ProfileInitialControlCodec.REPLACEMENT_SCHEMA));
                capabilities.add(new ProfileHello.Capability(ProfileInitialControlCodec.RECOVERY_CAPABILITY_ID,
                        ProfileInitialControlCodec.RECOVERY_SCHEMA));
                capabilities.add(new ProfileHello.Capability(ProfileInitialControlCodec.RECOVERY_CLOSE_CAPABILITY_ID,
                        ProfileInitialControlCodec.RECOVERY_CLOSE_SCHEMA));
                capabilities.add(ProfileInitialControlCodec.TERMINAL_CLOSE_CAPABILITY);
                capabilities.add(ProfileInitialControlCodec.TERMINAL_BOUNDARY_CAPABILITY);
                capabilities.add(ProfileInitialControlCodec.DELETE_APPLY_CAPABILITY);
                capabilities.add(ProfilePhysicalDeleteCodec.CAPABILITY);
            }
            if (read != null) {
                capabilities.add(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.CAPABILITY);
                if (read.lacAuthorization() != null) {
                    capabilities.add(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.CAPABILITY);
                    capabilities.add(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec
                            .EXPLICIT_CAPABILITY);
                }
            }
            if (control != null) {
                capabilities.add(ProfileOperationCodec.WRITE_LAC_CAPABILITY);
            }
            ProfileHello.Server hello = new ProfileHello.Server(
                    EngineProfile.SEGMENT_WAL_ENGINE.wireValue(),
                    identity.incarnation().value(), readinessGeneration, identity.bookieId().toString(),
                    capabilities);
            return ProfileInboundPipeline.install(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                    100 + SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(config),
                    144 + SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(config),
                    new SegmentProfileAddHandler(this, tls, trustDomain, hello,
                            SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(config),
                            config.maxPendingEntries(), control, read), control != null);
        }
    }

    /** Exact validation precedes the existing charged synchronous copy and real admission path. */
    SegmentBookieControlLog.NormalRejection submitProfileNormal(ProfileLedgerContext context, byte[] credential,
            long entryId, ByteBuf entry, SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || !writableRegistrationAllowsAdmission()) {
                return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            SegmentBookieControlLog.NormalRejection rejection = bookie.normalRejection(context, credential);
            if (rejection != null) {
                return rejection;
            }
            SegmentEntryIdentity parsed;
            try {
                parsed = SegmentEntryIdentity.parse(context, entry.nioBuffer());
            } catch (IOException invalid) {
                throw new IllegalArgumentException("Invalid Profile BK entry", invalid);
            }
            if (parsed.entryId() != entryId) {
                throw new IllegalArgumentException("Profile coordinate differs from its BK entry");
            }
            if (submitNormal(context, credential, entry, request)) {
                return null;
            }
            rejection = bookie.normalRejection(context, credential);
            return rejection != null ? rejection : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    SegmentBookieControlLog.NormalRejection profileFenceRejection(ProfileOperationCodec.FenceLedger request) {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            return bookie.fenceRejection(request.context(), request.credential());
        }
    }

    /** Controls keep their reserved owner/I/O path when DATA input or space is backpressured. */
    SegmentBookieControlLog.NormalRejection submitProfileFence(ProfileOperationCodec.FenceLedger request,
            SegmentBookieControlDriver.FenceCompletion completion) {
        synchronized (gate) {
            SegmentBookieControlLog.NormalRejection rejection = profileFenceRejection(request);
            if (rejection != null) {
                return rejection;
            }
            return controls.submitFence(request, completion)
                    ? null : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    SegmentBookieControlLog.NormalRejection profileLacRejection(ProfileOperationCodec.WriteLac request) {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || !writableRegistrationAllowsAdmission()) {
                return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            return bookie.explicitLacRejection(request);
        }
    }

    SegmentBookieControlLog.NormalRejection submitProfileLac(ProfileOperationCodec.WriteLac request,
            SegmentBookieControlDriver.LacCompletion completion) {
        synchronized (gate) {
            var rejection = profileLacRejection(request);
            if (rejection != null) {
                return rejection;
            }
            return controls.submitExplicitLac(request, completion)
                    ? null : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    boolean submitRecovery(ProfileOperationCodec.AddRecovery request,
                           SegmentDataPhysicalDriver.RecoveryRequest response)
            throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || !writableRegistrationAllowsAdmission()
                    || freeOperation != null || compaction != null
                    || controlRotation != null || controlCapacityPaused() || dataCapacityPaused()) {
                return false;
            }
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(response, "response");
            SegmentBookieControlLog.RecoveryAdmission admission = bookie.admitRecovery(request).orElse(null);
            if (admission == null) {
                return false;
            }
            try {
                boolean accepted = writes[SegmentOwnerLoop.shardFor(admission.context().ledgerId(),
                        config.logicalShards())].submitRecovery(admission, response);
                if (!accepted) {
                    controlCapacityPaused();
                }
                return accepted;
            } catch (IOException | RuntimeException | Error failure) {
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    /** The complete entry is borrowed only until its synchronous, precharged source copy finishes. */
    SegmentBookieControlLog.NormalRejection submitProfileRecovery(ProfileOperationCodec.AddRecovery tuple,
            ByteBuf entry, SegmentDataPhysicalDriver.RecoveryRequest response) throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || !writableRegistrationAllowsAdmission()
                    || freeOperation != null || compaction != null || controlRotation != null
                    || controlCapacityPaused() || dataCapacityPaused()) {
                return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            SegmentBookieControlLog.NormalRejection rejection = bookie.recoveryRejection(tuple);
            if (rejection != null) {
                return rejection;
            }
            SegmentEntryIdentity parsed;
            try {
                parsed = SegmentEntryIdentity.parse(tuple.context(), entry.nioBuffer());
            } catch (IOException invalid) {
                throw new IllegalArgumentException("Invalid Profile recovery BK entry", invalid);
            }
            if (parsed.entryId() != tuple.entryId()) {
                throw new IllegalArgumentException("Profile recovery coordinate differs from its BK entry");
            }
            SegmentBookieControlLog.RecoveryAdmission admission = bookie.admitRecoveryTuple(tuple).orElse(null);
            if (admission == null) {
                rejection = bookie.recoveryRejection(tuple);
                return rejection != null ? rejection : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            SegmentEntryInputs.Input input = null;
            try {
                input = execution.inputs().acquire(tuple.context(), entry.readableBytes(),
                        ProfileInboundPipeline.inputCopyAllocator(entry));
                if (input == null) {
                    admission.releaseUnclaimed();
                    return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
                }
                input.bytes().writeBytes(entry, entry.readerIndex(), entry.readableBytes());
                boolean accepted = writes[SegmentOwnerLoop.shardFor(tuple.context().ledgerId(),
                        config.logicalShards())].submitRecovery(input, admission, response);
                if (!accepted) {
                    controlCapacityPaused();
                }
                return accepted ? null : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            } catch (IOException | RuntimeException | Error failure) {
                if (input != null) {
                    input.close();
                }
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    boolean profileReadVisible(ProfileLedgerContext context, long entryId) {
        synchronized (gate) {
            return accepting && bookie.networkReadable(context, entryId);
        }
    }

    boolean submitLacRead(ProfileLedgerContext context, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            return accepting && execution.accepting() && freeOperation == null
                    && compaction == null && !deleteInspection && controlRotation == null && !controlCapacityPaused()
                    && reads[SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards())]
                            .submitLac(context, request);
        }
    }

    boolean submitLatestRead(ProfileLedgerContext context, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            return accepting && execution.accepting() && freeOperation == null
                    && compaction == null && !deleteInspection && controlRotation == null && !controlCapacityPaused()
                    && reads[SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards())]
                            .submitLatest(context, request);
        }
    }

    boolean submitRead(ProfileLedgerContext context, long entryId, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            return accepting && execution.accepting() && freeOperation == null
                    && compaction == null && !deleteInspection && controlRotation == null && !controlCapacityPaused()
                    && reads[SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards())]
                            .submit(context, entryId, request);
        }
    }

    boolean checkpointIndex(IndexCheckpointRequest request) {
        return checkpointIndex(request, new SegmentFileIo());
    }

    /** One bounded graph request; DATA refusal may report TRANSIENT_UNAVAILABLE on the calling thread. */
    boolean checkpointIndex(IndexCheckpointRequest request, SegmentFileIo publicationIo) {
        return maintainIndex(request, publicationIo, false);
    }

    boolean reclaimTerminalEntries(IndexCheckpointRequest request) {
        return reclaimTerminalEntries(request, new SegmentFileIo());
    }

    /** Native row removal, original-owner coordinate release and durable coverage share the existing checkpoint. */
    boolean reclaimTerminalEntries(IndexCheckpointRequest request, SegmentFileIo publicationIo) {
        return maintainIndex(request, publicationIo, true);
    }

    private boolean maintainIndex(IndexCheckpointRequest request, SegmentFileIo publicationIo, boolean cleanup) {
        return maintainIndex(request, publicationIo, cleanup, null);
    }

    private boolean maintainIndex(IndexCheckpointRequest request, SegmentFileIo publicationIo, boolean cleanup,
                                  CompactionOperation admitted) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(publicationIo, "publicationIo");
        synchronized (gate) {
            if (!accepting || !execution.accepting() || indexCheckpoint != null || freeOperation != null
                    || compaction != admitted || deleteInspection || controlRotation != null
                    || controlCapacityPaused()) {
                return false;
            }
            IndexCheckpoint checkpoint = new IndexCheckpoint(request, publicationIo, cleanup);
            indexCheckpoint = checkpoint;
            for (IndexCheckpoint.Worker worker : checkpoint.workers) {
                if (!execution.owner(worker.id).submit(worker.id, worker)) {
                    checkpoint.refused(worker);
                }
            }
            return true;
        }
    }

    /** One bounded maintenance operation; DATA proof and CONTROL force use their existing separate services. */
    boolean reclaimAllocation(UUID operationId, long allocationId, FreeRequest request) {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(request, "request");
        synchronized (gate) {
            if (!accepting || !execution.accepting() || freeOperation != null || indexCheckpoint != null
                    || compaction != null || deleteInspection || controlRotation != null || controlCapacityPaused()) {
                return false;
            }
            FreeOperation operation = new FreeOperation(operationId, allocationId, request);
            freeOperation = operation;
            try {
                if (execution.owner(0).submit(0, operation)) {
                    return true;
                }
            } catch (RuntimeException | Error failure) {
                freeOperation = null;
                throw failure;
            }
            freeOperation = null;
            return false;
        }
    }

    /** One explicit, bounded same-stream victim group; the actual frozen live set must have positive net gain. */
    boolean compactAllocations(UUID operationId, List<Long> allocationIds, CompactionRequest request)
            throws IOException {
        return compactAllocations(operationId, allocationIds, request, new SegmentFileIo());
    }

    /** Select from actual current DATA in a bounded shard window; the final FREE still requires its own proof. */
    boolean compactShard(UUID operationId, int shard, CompactionRequest request) throws IOException {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(request, "request");
        if (operationId.equals(new UUID(0, 0)) || shard < 0 || shard >= config.logicalShards()) {
            throw new IllegalArgumentException("Compaction needs a new operation and a valid fixed shard");
        }
        synchronized (gate) {
            if (!canCompact()) {
                return false;
            }
            CompactionCursor cursor = compactionCursors[shard];
            if (cursor == null) {
                cursor = new CompactionCursor(-1, arena.appliedThrough());
            }
            SegmentArenaControlLog.CompactionWindow window = arena.compactionCandidates(shard,
                    cursor.afterOrder(), cursor.throughOrder());
            if (window.allocations().isEmpty()) {
                // Refresh once: new grants cannot indefinitely delay revisiting earlier current allocations.
                cursor = new CompactionCursor(-1, arena.appliedThrough());
                window = arena.compactionCandidates(shard, cursor.afterOrder(), cursor.throughOrder());
                if (window.allocations().isEmpty()) {
                    compactionCursors[shard] = null;
                    return false;
                }
            }
            boolean admitted = admitCompaction(operationId, window.allocations(), request, new SegmentFileIo(), true);
            if (admitted) {
                compactionCursors[shard] = new CompactionCursor(window.lastOrder(), cursor.throughOrder());
            }
            return admitted;
        }
    }

    /** Explicit cold physical deletion; global completion/retention and strong access revocation are separate. */
    public SegmentProfilePhysicalDelete.Attempt reclaimDeletedLedger(ProfileLedgerContext context,
            ThreadPoolExecutor cold, int maxAllocations, int maxCompactions, Duration timeout) {
        return SegmentProfilePhysicalDelete.begin(this, context, cold, physicalDeleteRunning,
                maxAllocations, maxCompactions, timeout);
    }

    record DeletedScan(boolean complete, List<SegmentArenaControlLog.Allocation> victims,
            int scanned, long bookieCut, long arenaCut) { }

    SegmentProfilePhysicalDelete.Status physicalDeletePrecondition(ProfileLedgerContext context) {
        synchronized (gate) {
            if (!accepting || execution == null || !execution.accepting()
                    || !bookie.matchesScope(config, identity)) {
                return SegmentProfilePhysicalDelete.Status.PENDING;
            }
            return bookie.hasTerminalTombstone(context) ? null : SegmentProfilePhysicalDelete.Status.NOT_DELETED;
        }
    }

    boolean hasAppliedDeleteEvent(org.apache.bookkeeper.common.profile.control.ProfileControlRequest request) {
        synchronized (gate) {
            return accepting && execution != null && execution.accepting()
                    && bookie.matchesScope(config, identity) && bookie.hasAppliedDeleteEvent(request);
        }
    }

    boolean hasDeletedInstance(ProfileLedgerContext context) {
        synchronized (gate) {
            return accepting && execution.accepting() && bookie.hasTerminalTombstone(context);
        }
    }

    CompletableFuture<DeletedScan> inspectDeletedInstance(ProfileLedgerContext context, int maximum) {
        var result = new CompletableFuture<DeletedScan>();
        synchronized (gate) {
            if (!canCompact() || !bookie.hasTerminalTombstone(context)) {
                result.completeExceptionally(new IOException("Deleted-instance inspection is unavailable"));
                return result;
            }
            deleteInspection = true;
            var command = new SegmentOwnerLoop.Command() {
                private SegmentBufferPool.Lease buffer;
                private DeletedScan completed;
                private Throwable failure;

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    buffer = execution.owner(0).pool().acquire(execution.owner(0).pool().bufferBytes());
                    if (buffer == null) {
                        ticket.complete(() -> finish(null, new IOException("Physical inspection buffer is busy")));
                        return;
                    }
                    var value = new java.util.concurrent.atomic.AtomicReference<DeletedScan>();
                    execution.io().submitContinuing(ticket, () -> {
                        long through = -1;
                        long bookieCut = -1;
                        int scanned = 0;
                        for (int shard = 0; shard < config.logicalShards(); shard++) {
                            int file = shard % config.dataFiles();
                            if (!arena.dataFileAvailable(file)) {
                                throw new IOException(
                                        "Physical deletion cannot interpret an unavailable DATA file");
                            }
                            try (var lease = bookie.protectReclamation(config, identity);
                                    var guard = files.protectReclamation(arena, file, lease)) {
                                if (through < 0) {
                                    // Freeze only after previously accepted DATA/read I/O is quiescent.
                                    through = arena.appliedThrough();
                                    bookieCut = lease.cut();
                                }
                                long after = -1;
                                for (;;) {
                                    var window = arena.compactionCandidates(shard, after, through);
                                    if (window.allocations().isEmpty()) {
                                        break;
                                    }
                                    if (scanned > maximum - window.allocations().size()) {
                                        value.set(new DeletedScan(false, List.of(), scanned, bookieCut, through));
                                        return;
                                    }
                                    boolean found = false;
                                    for (var allocation : window.allocations()) {
                                        found |= guard.containsAllocatedInstance(allocation, bookie, context,
                                                buffer.bytes());
                                    }
                                    scanned += window.allocations().size();
                                    if (found) {
                                        value.set(new DeletedScan(false, window.allocations(),
                                                scanned, bookieCut, through));
                                        return;
                                    }
                                    after = window.lastOrder();
                                }
                            }
                        }
                        value.set(new DeletedScan(true, List.of(), scanned, bookieCut, through));
                    }, failure -> ticket.complete(() -> finish(value.get(), failure)));
                }

                private void finish(DeletedScan value, Throwable failure) {
                    if (buffer != null) {
                        buffer.close();
                        buffer = null;
                    }
                    synchronized (gate) {
                        deleteInspection = false;
                    }
                    completed = value;
                    this.failure = failure;
                }

                @Override
                public void terminal() {
                    if (failure == null && completed != null) {
                        result.complete(completed);
                    } else {
                        result.completeExceptionally(failure != null ? failure
                                : new IOException("Physical inspection has no result"));
                    }
                }

                @Override
                public void failed(Throwable failure) {
                    finish(null, failure);
                }
            };
            try {
                if (!execution.owner(0).submit(0, command)) {
                    deleteInspection = false;
                    result.completeExceptionally(new IOException("Physical inspection owner is busy"));
                }
            } catch (RuntimeException | Error failure) {
                deleteInspection = false;
                result.completeExceptionally(failure);
            }
        }
        return result;
    }

    boolean compactDeletedWindow(ProfileLedgerContext context, List<SegmentArenaControlLog.Allocation> victims,
            CompactionRequest request) throws IOException {
        synchronized (gate) {
            if (!canCompact() || !bookie.hasTerminalTombstone(context)) {
                return false;
            }
            // Physical retirement may relocate a mixed block at zero net capacity gain.
            // The original capacity-maintenance callers continue to require positive net gain.
            return admitCompaction(UUID.randomUUID(), victims, request, new SegmentFileIo(), false, context);
        }
    }

    private boolean canCompact() {
        return accepting && execution.accepting() && indexCheckpoint == null && freeOperation == null
                && compaction == null && !deleteInspection && controlRotation == null && !controlCapacityPaused()
                && config.maxPendingEntries() >= 2;
    }

    private boolean controlCapacityPaused() {
        if (bookie.rotationRequired() || arena.rotationRequired()) {
            execution.owner(0).wakeup();
            return true;
        }
        return false;
    }

    private boolean dataCapacityPaused() {
        if (arena.hasSpacePressure()) {
            execution.owner(0).wakeup();
            return true;
        }
        return false;
    }

    /** One frozen, bounded-window sweep per pressured file; control rotation retains priority. */
    private void maintainPressuredCapacity() {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                return;
            }
            rotatePressuredControls();
            if (!arena.hasSpacePressure() || !canCompact() || !bookie.quiescentForRotation()
                    || !files.quiescentForControlRotation(arena)) {
                return;
            }
            for (int file = 0; file < spaceSweeps.length; file++) {
                if (!arena.spacePressure(file)) {
                    continue;
                }
                try {
                    if (arena.restoreForegroundCapacity(file, limits.poolBlocks(), limits.maintenanceBlocks())) {
                        spaceSweeps[file] = null;
                        continue;
                    }
                    long arenaCut = arena.appliedThrough();
                    long bookieCut = bookie.appliedThrough();
                    SpaceSweep sweep = spaceSweeps[file];
                    if (sweep == null || sweep.shard >= config.logicalShards()
                            && (sweep.arenaCut != arenaCut || sweep.bookieCut != bookieCut)) {
                        sweep = new SpaceSweep(file, arenaCut, bookieCut);
                        spaceSweeps[file] = sweep;
                    }
                    while (sweep.shard < config.logicalShards()) {
                        SegmentArenaControlLog.CompactionWindow window = arena.compactionCandidates(sweep.shard,
                                sweep.afterOrder, sweep.arenaCut);
                        if (window.allocations().isEmpty()) {
                            sweep.shard += config.dataFiles();
                            sweep.afterOrder = -1;
                            continue;
                        }
                        for (int worker = 0; worker < executionSettings.workers(); worker++) {
                            int required = worker == sweep.shard % executionSettings.workers() ? 2 : 1;
                            if (!execution.owner(worker).hasMaintenanceBuffers(required)) {
                                return;
                            }
                        }
                        if (admitCompaction(UUID.randomUUID(), window.allocations(),
                                new CompactionRequest(this::spaceMaintenanceCompleted), new SegmentFileIo(), true)) {
                            sweep.afterOrder = window.lastOrder();
                        }
                        return;
                    }
                } catch (IOException failure) {
                    // A concurrent cached control can invalidate the diagnostic precheck before admission.
                    if (!bookie.quiescentForRotation() || !files.quiescentForControlRotation(arena)
                            || arena.rotationRequired()) {
                        return;
                    }
                    stopFailedSpaceMaintenance(failure);
                }
            }
        }
    }

    private void spaceMaintenanceCompleted(CompactionResult result, Throwable failure) {
        synchronized (gate) {
            if (failure instanceof SegmentControlCapacityException
                    && (arena.rotationRequired() || bookie.rotationRequired())) {
                // This write never started. A fresh sweep must inspect the group after control rotation.
                for (int file = 0; file < spaceSweeps.length; file++) {
                    if (arena.spacePressure(file)) {
                        spaceSweeps[file] = null;
                    }
                }
                execution.owner(0).wakeup();
                return;
            }
            if (failure != null || result.status() == CompactionStatus.UNKNOWN) {
                stopFailedSpaceMaintenance(failure != null ? failure
                        : new IOException("Automatic space maintenance has an unknown result"));
            } else {
                execution.owner(0).wakeup();
            }
        }
    }

    private void stopFailedSpaceMaintenance(Throwable failure) {
        spaceMaintenanceFailure = failure;
        accepting = false;
        execution.stopAdmission();
    }

    Throwable spaceMaintenanceFailure() {
        return spaceMaintenanceFailure;
    }

    boolean spaceMaintenanceIdle() {
        synchronized (gate) {
            if (compaction != null || indexCheckpoint != null) {
                return false;
            }
            for (int file = 0; file < spaceSweeps.length; file++) {
                if (arena.spacePressure(file) && (spaceSweeps[file] == null
                        || spaceSweeps[file].shard < config.logicalShards())) {
                    return false;
                }
            }
            return true;
        }
    }

    private void rotatePressuredControls() {
        synchronized (gate) {
            if (!accepting || !execution.accepting() || controlRotation != null
                    || (!bookie.rotationRequired() && !arena.rotationRequired())
                    || !bookie.quiescentForRotation() || !files.quiescentForControlRotation(arena)) {
                return;
            }
            rotateControls(new ControlRotationRequest((result, failure) -> {
                if (result.status() == ControlRotationStatus.UNKNOWN) {
                    controlMaintenanceFailure = failure;
                }
            }));
        }
    }

    Throwable controlMaintenanceFailure() {
        return controlMaintenanceFailure;
    }

    boolean rotateControls(ControlRotationRequest request) {
        return rotateControls(request, new SegmentFileIo(), new SegmentFileIo());
    }

    /** One graph-wide pause and one reserved CONTROL I/O ticket; no DATA session or prefix reset. */
    boolean rotateControls(ControlRotationRequest request, SegmentFileIo bookieIo, SegmentFileIo arenaIo) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(bookieIo, "bookieIo");
        Objects.requireNonNull(arenaIo, "arenaIo");
        synchronized (gate) {
            if (!accepting || !execution.accepting() || indexCheckpoint != null || freeOperation != null
                    || compaction != null || controlRotation != null || !controls.reserveMaintenance()) {
                return false;
            }
            if (!allocator.reserveMaintenance()) {
                controls.releaseMaintenance();
                return false;
            }
            ControlRotation operation = new ControlRotation(request, bookieIo, arenaIo);
            controlRotation = operation;
            try {
                if (execution.owner(0).submitControl(operation)) {
                    return true;
                }
            } catch (RuntimeException | Error failure) {
                operation.releaseReservation();
                throw failure;
            }
            operation.releaseReservation();
            return false;
        }
    }

    private final class ControlRotation implements SegmentOwnerLoop.Command {
        private final ControlRotationRequest request;
        private final SegmentFileIo bookieIo;
        private final SegmentFileIo arenaIo;
        private SegmentBookieControlLog.ReclamationLease bookieLease;
        private IsolatedSegmentFiles.ReclamationGuard protection;
        private SegmentControlRotation.Root bookieRoot;
        private SegmentControlRotation.Root arenaRoot;
        private boolean attempted;

        private ControlRotation(ControlRotationRequest request, SegmentFileIo bookieIo, SegmentFileIo arenaIo) {
            this.request = request;
            this.bookieIo = bookieIo;
            this.arenaIo = arenaIo;
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket ticket) {
            execution.io().submitControl(ticket, () -> {
                bookieLease = bookie.protectReclamation(config, identity);
                protection = files.protectReclamation(arena, 0, bookieLease);
                attempted = true;
                bookieRoot = bookie.rotate(bookieIo);
                arenaRoot = arena.rotate(arenaIo, protection);
            }, this::finished);
        }

        @Override
        public void failed(Throwable failure) {
            finished(failure);
        }

        private void releaseReservation() {
            synchronized (gate) {
                allocator.releaseMaintenance();
                controls.releaseMaintenance();
                if (controlRotation == this) {
                    controlRotation = null;
                }
            }
        }

        private void finished(Throwable failure) {
            execution.owner(0).requireOwner();
            if (protection != null) {
                protection.close();
            } else if (bookieLease != null) {
                bookieLease.close();
            }
            if (attempted && failure != null) {
                synchronized (gate) {
                    accepting = false;
                    execution.stopAdmission();
                }
            }
            releaseReservation();
            ControlRotationStatus status = failure == null ? ControlRotationStatus.ROTATED
                    : attempted ? ControlRotationStatus.UNKNOWN : ControlRotationStatus.TRANSIENT_UNAVAILABLE;
            request.complete(new ControlRotationResult(status, bookieRoot, arenaRoot), failure);
        }
    }

    private boolean admitCompaction(UUID operationId, List<SegmentArenaControlLog.Allocation> victims,
                                    CompactionRequest request, SegmentFileIo publicationIo, boolean selectVictims)
            throws IOException {
        return admitCompaction(operationId, victims, request, publicationIo, selectVictims, null);
    }

    private boolean admitCompaction(UUID operationId, List<SegmentArenaControlLog.Allocation> victims,
            CompactionRequest request, SegmentFileIo publicationIo, boolean selectVictims,
            ProfileLedgerContext deletedContext) throws IOException {
        arena.requireUnusedOperation(operationId);
        arena.requireUnusedOperation(phaseId(operationId, "ALLOC"));
        CompactionOperation operation = new CompactionOperation(operationId, victims, request, publicationIo,
                selectVictims, deletedContext);
        compaction = operation;
        if (!maintainIndex(new IndexCheckpointRequest(operation::prepared), new SegmentFileIo(), true, operation)) {
            compaction = null;
            return false;
        }
        return true;
    }

    boolean compactAllocations(UUID operationId, List<Long> allocationIds, CompactionRequest request,
                               SegmentFileIo publicationIo) throws IOException {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(publicationIo, "publicationIo");
        allocationIds = List.copyOf(allocationIds);
        if (operationId.equals(new UUID(0, 0)) || allocationIds.isEmpty() || allocationIds.size() > 16
                || allocationIds.stream().distinct().count() != allocationIds.size()) {
            throw new IllegalArgumentException("Compaction needs 1..16 unique same-stream allocations");
        }
        synchronized (gate) {
            if (!canCompact()) {
                return false;
            }
            List<SegmentArenaControlLog.Allocation> victims = new ArrayList<>();
            for (long allocationId : allocationIds) {
                SegmentArenaControlLog.Allocation victim = arena.currentAllocation(allocationId);
                if (!victims.isEmpty() && (victim.ownerShard() != victims.get(0).ownerShard()
                        || !victim.ownerGeneration().equals(victims.get(0).ownerGeneration()))) {
                    throw new IllegalArgumentException("Compaction victims must belong to one fixed DATA stream");
                }
                victims.add(victim);
            }
            return admitCompaction(operationId, victims, request, publicationIo, false);
        }
    }

    private static UUID phaseId(UUID operationId, String phase) {
        return UUID.nameUUIDFromBytes((operationId + "/" + phase).getBytes(StandardCharsets.UTF_8));
    }

    private final class CompactionOperation implements SegmentOwnerLoop.Command {
        private final UUID operationId;
        private List<SegmentArenaControlLog.Allocation> victims;
        private final CompactionRequest request;
        private final SegmentFileIo publicationIo;
        private final int shard;
        private final boolean selectVictims;
        private final ProfileLedgerContext deletedContext;
        private SegmentOwnerLoop.Ticket ticket;
        private SegmentBufferPool.Lease buffer;
        private SegmentDataRecovery.CompactionPlan plan;
        private SegmentBatchFormat.RelocationBuilder copyBuilder;
        private SegmentBatchFormat.FrozenBatch copyBatch;
        private SegmentArenaControlLog.Allocation destination;
        private SegmentArenaMove move;
        private List<SegmentArenaControlLog.Free> frees;
        private boolean moveSubmitted;
        private boolean freesSubmitted;
        private boolean finishing;
        private int movedEntries;
        private int freedAllocations;
        private long freedBytes;
        private long destinationBytes;
        private long destinationAllocation = -1;
        private int batchIndex;
        private long destinationOffset;

        private CompactionOperation(UUID operationId, List<SegmentArenaControlLog.Allocation> victims,
                                    CompactionRequest request, SegmentFileIo publicationIo, boolean selectVictims,
                                    ProfileLedgerContext deletedContext) {
            this.deletedContext = deletedContext;
            this.operationId = operationId;
            this.victims = List.copyOf(victims);
            this.request = request;
            this.publicationIo = publicationIo;
            shard = victims.get(0).ownerShard();
            this.selectVictims = selectVictims;
        }

        private void prepared(IndexCheckpointResult result, Throwable failure) {
            if (failure != null || result.status() != IndexCheckpointStatus.PERSISTED) {
                finish(failure == null ? CompactionStatus.TRANSIENT_UNAVAILABLE : CompactionStatus.UNKNOWN, failure);
            } else if (!execution.owner(shard).submit(shard, this)) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
            }
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket admitted) {
            ticket = admitted;
            if (!acquireBuffer()) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            execution.io().submitContinuing(ticket, () -> {
                try (SegmentBookieControlLog.ReclamationLease lease = bookie.protectReclamation(config, identity);
                        IsolatedSegmentFiles.ReclamationGuard guard = files.protectReclamation(arena,
                                victims.get(0).file(), lease)) {
                    plan = guard.inspectCompaction(victims, bookie, limits.indexedEntries(),
                            buffer.bytes(), selectVictims);
                    victims = plan.victims();
                }
            }, this::captured);
        }

        private boolean acquireBuffer() {
            buffer = execution.owner(shard).pool().acquire(execution.owner(shard).pool().bufferBytes());
            return buffer != null;
        }

        private void releaseBuffer() {
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
        }

        private void captured(Throwable failure) {
            releaseBuffer();
            if (failure != null) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, failure);
            } else if (!plan.hasNetGain() && deletedContext == null) {
                finish(CompactionStatus.NO_NET_GAIN, null);
            } else if (plan.destinationBlocks() > limits.maintenanceBlocks()) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
            } else if (plan.batches().isEmpty()) {
                prepareFrees();
            } else {
                try {
                    SegmentArenaControlLog.Allocation selected = arena.planPools(List.of(
                            new SegmentArenaControlLog.PoolRequest(shard, victims.get(0).ownerGeneration(),
                                    victims.get(0).streamFirstSequence(), plan.destinationBlocks())))
                            .get(0).orElse(null);
                    if (selected == null) {
                        resumeRetainedTail();
                        return;
                    }
                    destination = new SegmentArenaControlLog.Allocation(selected.type(),
                            phaseId(operationId, "ALLOC"),
                            selected.operationGeneration(), selected.expectedGeneration(), selected.file(),
                            selected.ownerShard(), selected.ownerGeneration(), selected.allocationId(),
                            selected.offset(), selected.blocks(), selected.streamFirstSequence());
                    if (!allocator.submit(List.of(destination), (results, cause) ->
                            ticket.resume(() -> allocated(results, cause)))) {
                        finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
                    }
                } catch (IOException failureToAllocate) {
                    finish(CompactionStatus.TRANSIENT_UNAVAILABLE, failureToAllocate);
                }
            }
        }

        private void resumeRetainedTail() throws IOException {
            SegmentArenaControlLog.Allocation retained = arena.latestAllocationForShard(shard).orElse(null);
            if (retained == null || !selectVictims && victims.contains(retained)) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            List<SegmentArenaControlLog.Allocation> sources = victims.stream()
                    .filter(victim -> !victim.equals(retained)).toList();
            if (sources.isEmpty() || !acquireBuffer()) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            long nextSequence = drivers[shard].relocationAddress(retained, retained.offset()).sequence();
            execution.io().submitContinuing(ticket, () -> {
                try (SegmentBookieControlLog.ReclamationLease lease = bookie.protectReclamation(config, identity);
                        IsolatedSegmentFiles.ReclamationGuard guard = files.protectReclamation(arena,
                                retained.file(), lease)) {
                    plan = guard.inspectCompactionTail(sources, retained, nextSequence, bookie,
                            limits.indexedEntries(), buffer.bytes());
                }
            }, failure -> {
                releaseBuffer();
                if (failure != null || plan == null) {
                    finish(CompactionStatus.TRANSIENT_UNAVAILABLE, failure);
                } else {
                    victims = plan.victims();
                    if (plan.batches().isEmpty()) {
                        prepareFrees();
                    } else {
                        destination = plan.tail().allocation();
                        destinationAllocation = destination.allocationId();
                        destinationOffset = plan.tail().offset();
                        copyNextBatch();
                    }
                }
            });
        }

        private void allocated(List<SegmentArenaControlLog.Result> results, Throwable failure) {
            if (failure != null) {
                finish(CompactionStatus.UNKNOWN, failure);
                return;
            }
            if (!applied(results.get(0))) {
                finish(CompactionStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            destinationBytes = destination.blocks() * SegmentArenaControlLog.BLOCK_BYTES;
            destinationAllocation = destination.allocationId();
            destinationOffset = destination.offset();
            copyNextBatch();
        }

        private SegmentDataRecovery.CompactionBatch currentBatch() {
            return plan.batches().get(batchIndex);
        }

        private void copyNextBatch() {
            try {
                if (!acquireBuffer()) {
                    finish(CompactionStatus.PARTIAL, null);
                    return;
                }
                UUID copyId = batchIndex == 0 ? operationId : phaseId(operationId, "COPY/" + batchIndex);
                arena.requireUnusedOperation(copyId);
                copyBuilder = SegmentBatchFormat.relocationBuilder(config,
                        drivers[shard].relocationAddress(destination, destinationOffset), copyId, new Generation(1),
                        execution.owner(shard).pool());
                if (copyBuilder == null) {
                    finish(CompactionStatus.PARTIAL, null);
                    return;
                }
                execution.io().submitContinuing(ticket, () -> {
                    try (SegmentBookieControlLog.ReclamationLease lease = bookie.protectReclamation(config, identity);
                            IsolatedSegmentFiles.ReclamationGuard guard = files.protectReclamation(arena,
                                    victims.get(0).file(), lease)) {
                        copyBatch = guard.captureCompaction(plan, batchIndex, bookie, copyBuilder, buffer.bytes());
                    }
                }, this::copyCaptured);
            } catch (IOException failureToCopy) {
                finish(CompactionStatus.PARTIAL, failureToCopy);
            }
        }

        private void copyCaptured(Throwable failure) {
            releaseBuffer();
            copyBuilder.close();
            copyBuilder = null;
            if (failure != null) {
                finish(CompactionStatus.PARTIAL, failure);
                return;
            }
            SegmentBatchFormat.FrozenBatch offered = copyBatch;
            copyBatch = null;
            try {
                if (!drivers[shard].submitRelocation(offered, (copy, cause) ->
                        ticket.resume(() -> copied(copy, cause)))) {
                    finish(CompactionStatus.PARTIAL, null);
                }
            } catch (IOException failureToCopy) {
                finish(CompactionStatus.PARTIAL, failureToCopy);
            }
        }

        private void copied(SegmentDataPhysicalDriver.DurableRelocation copy, Throwable failure) {
            if (failure != null) {
                finish(CompactionStatus.UNKNOWN, failure);
                return;
            }
            if (!acquireBuffer()) {
                finish(CompactionStatus.PARTIAL, null);
                return;
            }
            move = null;
            moveSubmitted = false;
            execution.io().submitContinuing(ticket, () -> move = SegmentArenaMove.prepare(arena, files, bookie,
                    copy, currentBatch().entries().stream().map(SegmentDataRecovery.CompactionEntry::source).toList(),
                    buffer.bytes()), cause -> {
                        releaseBuffer();
                        if (cause != null) {
                            finish(CompactionStatus.PARTIAL, cause);
                        } else {
                            moveSubmitted = allocator.submitMove(move, (results, error) ->
                                    ticket.resume(() -> moved(results, error)));
                            if (!moveSubmitted) {
                                move.close();
                                finish(CompactionStatus.PARTIAL, null);
                            }
                        }
                    });
        }

        private void moved(List<SegmentArenaControlLog.Result> results, Throwable failure) {
            if (failure != null || !applied(results.get(0))) {
                finish(failure == null ? CompactionStatus.PARTIAL : CompactionStatus.UNKNOWN, failure);
                return;
            }
            movedEntries += currentBatch().entries().size();
            destinationOffset = Math.addExact(destinationOffset, currentBatch().bytes());
            if (++batchIndex < plan.batches().size()) {
                copyNextBatch();
                return;
            }
            if (!maintainIndex(new IndexCheckpointRequest((result, cause) -> ticket.resume(() -> {
                if (cause != null || result.status() != IndexCheckpointStatus.PERSISTED) {
                    finish(cause == null ? CompactionStatus.PARTIAL : CompactionStatus.UNKNOWN, cause);
                } else {
                    prepareFrees();
                }
            })), publicationIo, false, this)) {
                finish(CompactionStatus.PARTIAL, null);
            }
        }

        private void prepareFrees() {
            if (!acquireBuffer()) {
                finish(CompactionStatus.PARTIAL, null);
                return;
            }
            execution.io().submitContinuing(ticket, () -> frees = arena.prepareFrees(victims.stream().map(victim ->
                    new SegmentArenaControlLog.FreeTarget(phaseId(operationId, "FREE/" + victim.allocationId()),
                            victim.allocationId())).toList(), files, bookie, buffer.bytes()), failure -> {
                        releaseBuffer();
                        if (failure != null) {
                            finish(CompactionStatus.PARTIAL, failure);
                        } else {
                            freesSubmitted = allocator.submitFrees(frees, (results, cause) -> ticket.resume(() -> {
                                if (cause == null) {
                                    for (int i = 0; i < results.size(); i++) {
                                        if (applied(results.get(i))) {
                                            freedAllocations++;
                                            freedBytes += victims.get(i).blocks() * SegmentArenaControlLog.BLOCK_BYTES;
                                        }
                                    }
                                }
                                finish(cause != null ? CompactionStatus.UNKNOWN
                                        : freedAllocations == victims.size() ? CompactionStatus.RECLAIMED
                                                : CompactionStatus.PARTIAL, cause);
                            }));
                            if (!freesSubmitted) {
                                for (SegmentArenaControlLog.Free free : frees) {
                                    free.close();
                                }
                                finish(CompactionStatus.PARTIAL, null);
                            }
                        }
                    });
        }

        private boolean applied(SegmentArenaControlLog.Result result) {
            return result.status() == SegmentArenaControlLog.Status.APPLIED_DURABLE
                    || result.status() == SegmentArenaControlLog.Status.ALREADY_DURABLE;
        }

        private synchronized void finish(CompactionStatus status, Throwable failure) {
            if (finishing) {
                return;
            }
            finishing = true;
            if (ticket == null) {
                deliver(status, failure);
            } else {
                ticket.complete(() -> deliver(status, failure));
            }
        }

        private void deliver(CompactionStatus status, Throwable failure) {
            releaseBuffer();
            if (copyBuilder != null) {
                copyBuilder.close();
                copyBuilder = null;
            }
            if (copyBatch != null) {
                copyBatch.close();
                copyBatch = null;
            }
            synchronized (gate) {
                compaction = null;
            }
            request.complete(new CompactionResult(status, movedEntries, freedAllocations, freedBytes,
                    destinationBytes, destinationAllocation), failure);
        }

        @Override
        public void terminal() {
            request.terminal();
        }

        @Override
        public void failed(Throwable cause) {
            if (move != null && !moveSubmitted) {
                move.close();
            }
            if (frees != null && !freesSubmitted) {
                for (SegmentArenaControlLog.Free free : frees) {
                    free.close();
                }
            }
            deliver(CompactionStatus.UNKNOWN, cause);
        }
    }

    private final class FreeOperation implements SegmentOwnerLoop.Command {
        private final UUID operationId;
        private final long allocationId;
        private final FreeRequest request;
        private SegmentOwnerLoop.Ticket ticket;
        private SegmentBufferPool.Lease buffer;
        private SegmentArenaControlLog.Free command;

        private FreeOperation(UUID operationId, long allocationId, FreeRequest request) {
            this.operationId = operationId;
            this.allocationId = allocationId;
            this.request = request;
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket admitted) {
            ticket = admitted;
            SegmentArenaControlLog.Result prior;
            try {
                prior = arena.resolvedFreeResult(operationId, allocationId);
            } catch (IOException failure) {
                ticket.complete(() -> finish(FreeStatus.UNKNOWN, null, failure));
                return;
            }
            if (prior != null) {
                ticket.complete(() -> finish(status(prior), prior, null));
                return;
            }
            buffer = execution.owner(0).pool().acquire(4096);
            if (buffer == null) {
                ticket.complete(() -> finish(FreeStatus.TRANSIENT_UNAVAILABLE, null, null));
                return;
            }
            execution.io().submitContinuing(ticket,
                    () -> command = arena.prepareFree(operationId, allocationId, files, bookie, buffer.bytes()),
                    cause -> {
                        releaseBuffer();
                        if (cause != null) {
                            ticket.complete(() -> finish(FreeStatus.TRANSIENT_UNAVAILABLE, null, cause));
                        } else if (!allocator.submitFree(command, (results, failure) -> {
                            SegmentArenaControlLog.Result result = results == null ? null : results.get(0);
                            FreeStatus outcome = failure != null ? FreeStatus.UNKNOWN : status(result);
                            ticket.complete(() -> finish(outcome, result, failure));
                        })) {
                            command.close();
                            ticket.complete(() -> finish(FreeStatus.TRANSIENT_UNAVAILABLE, null, null));
                        }
                    });
        }

        private FreeStatus status(SegmentArenaControlLog.Result result) {
            return result.status() == SegmentArenaControlLog.Status.APPLIED_DURABLE
                    || result.status() == SegmentArenaControlLog.Status.ALREADY_DURABLE
                            ? FreeStatus.APPLIED_DURABLE : FreeStatus.CONDITION_FAILED;
        }

        private void releaseBuffer() {
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
        }

        private void finish(FreeStatus status, SegmentArenaControlLog.Result result, Throwable failure) {
            synchronized (gate) {
                freeOperation = null;
            }
            request.complete(new FreeResult(status, result), failure);
        }

        @Override
        public void failed(Throwable cause) {
            releaseBuffer();
            if (command != null) {
                command.close();
            }
            finish(FreeStatus.UNKNOWN, null, cause);
        }
    }

    /** Fixed original worker set; no extra threads, queue, control slot or per-shard DATA ticket. */
    private final class IndexCheckpoint {
        private final IndexCheckpointRequest request;
        private final SegmentFileIo publicationIo;
        private final boolean cleanup;
        private final Worker[] workers;
        private int ownerTerminated;
        private int ready;
        private int finished;
        private boolean finishing;
        private boolean ioStarted;
        private boolean responded;
        private IndexCheckpointStatus status = IndexCheckpointStatus.PERSISTED;
        private Throwable failure;
        private SegmentDerivedIndex.Coverage coverage;
        private SegmentBookieControlLog.ReclamationLease cleanupLease;
        private IsolatedSegmentFiles.ReclamationGuard moveProtection;
        private SegmentBufferPool.Lease moveBuffer;
        private boolean nativeStarted;
        private boolean coordinatesStarted;
        private int coordinatesDone;

        private IndexCheckpoint(IndexCheckpointRequest request, SegmentFileIo publicationIo, boolean cleanup) {
            this.request = request;
            this.publicationIo = publicationIo;
            this.cleanup = cleanup;
            workers = new Worker[executionSettings.workers()];
            for (int worker = 0; worker < workers.length; worker++) {
                workers[worker] = new Worker(worker);
            }
        }

        private synchronized void refused(Worker worker) {
            worker.terminal();
            worker.terminal = true;
            finished++;
            unavailable(null);
            prepared(worker);
        }

        private synchronized void unavailable(Throwable cause) {
            if (cause != null) {
                status = IndexCheckpointStatus.UNKNOWN;
                if (failure == null) {
                    failure = cause;
                }
            } else if (status == IndexCheckpointStatus.PERSISTED) {
                status = IndexCheckpointStatus.TRANSIENT_UNAVAILABLE;
            }
        }

        private synchronized void prepared(Worker worker) {
            if (worker.ready) {
                throw new IllegalStateException("Checkpoint worker already prepared");
            }
            worker.ready = true;
            if (++ready != workers.length) {
                return;
            }
            if (status != IndexCheckpointStatus.PERSISTED) {
                finishWorkers();
                return;
            }
            Worker writer = workers[0];
            ioStarted = true;
            if (cleanup || arena.hasMoves()) {
                writer.ticket.resume(() -> {
                    if (arena.hasMoves()) {
                        moveBuffer = execution.owner(0).pool().acquire(execution.owner(0).pool().bufferBytes());
                        if (moveBuffer == null) {
                            unavailable(null);
                            finishWorkers();
                            return;
                        }
                    }
                    execution.io().submitContinuing(writer.ticket, () -> {
                        cleanupLease = cleanup ? bookie.protectTerminalCleanup(config, identity)
                                : bookie.protectReclamation(config, identity);
                        if (moveBuffer != null) {
                            arena.requireMovePreparation(files);
                            moveProtection = files.protectReclamation(arena, 0, cleanupLease);
                        }
                        if (cleanup) {
                            nativeStarted = true;
                            index.retireTerminalEntries(cleanupLease);
                        }
                        if (moveProtection != null) {
                            nativeStarted = true;
                            index.refreshMovedLocations(arena, files, moveProtection, moveBuffer.bytes());
                        }
                    }, this::terminalRowsRetired);
                });
            } else {
                publish();
            }
        }

        private synchronized void terminalRowsRetired(Throwable cause) {
            if (cause != null) {
                if (!nativeStarted && cause instanceof IOException) {
                    unavailable(null);
                    failure = cause;
                } else {
                    unavailable(cause);
                }
            }
            if (status != IndexCheckpointStatus.PERSISTED) {
                finishWorkers();
            } else if (cleanup) {
                retireCoordinates();
            } else {
                publish();
            }
        }

        private synchronized void retireCoordinates() {
            coordinatesStarted = true;
            for (Worker worker : workers) {
                if (worker.terminal) {
                    coordinatesRetired(worker);
                } else {
                    worker.ticket.resume(() -> {
                        try {
                            for (int i = 0; i < worker.cuts.size(); i++) {
                                int shard = worker.cuts.get(i).stream().shard();
                                worker.cuts.set(i, hotDirectories[shard].retireTerminalCoordinates(cleanupLease));
                            }
                        } catch (IOException failure) {
                            unavailable(failure);
                        }
                        coordinatesRetired(worker);
                    });
                }
            }
        }

        private synchronized void coordinatesRetired(Worker worker) {
            if (worker.coordinatesReady) {
                throw new IllegalStateException("Terminal cleanup owner already retired its coordinates");
            }
            worker.coordinatesReady = true;
            if (++coordinatesDone == workers.length) {
                if (status == IndexCheckpointStatus.PERSISTED) {
                    publish();
                } else {
                    finishWorkers();
                }
            }
        }

        private synchronized void publish() {
            List<SegmentHotDirectory.IndexCut> cuts = new ArrayList<>();
            for (Worker original : workers) {
                cuts.addAll(original.cuts);
            }
            Worker writer = workers[0];
            writer.ticket.resume(() -> execution.io().submitContinuing(writer.ticket,
                    () -> coverage = index.checkpointRuntime(cuts, arena, bookie, publicationIo), cause -> {
                        if (cause != null) {
                            unavailable(cause);
                        }
                        finishWorkers();
                    }));
        }

        private synchronized void finishWorkers() {
            if (finishing) {
                return;
            }
            finishing = true;
            for (Worker worker : workers) {
                if (!worker.terminal) {
                    worker.ticket.resume(() -> {
                        worker.resumeIndexes();
                        worker.ticket.complete(() -> completed(worker));
                    });
                }
            }
            deliverIfFinished();
        }

        private synchronized void completed(Worker worker) {
            if (worker.terminal) {
                throw new IllegalStateException("Checkpoint worker already terminal");
            }
            worker.terminal = true;
            finished++;
            deliverIfFinished();
        }

        private void deliverIfFinished() {
            if (finished != workers.length || responded) {
                return;
            }
            responded = true;
            if (moveProtection != null) {
                moveProtection.close();
                moveProtection = null;
            }
            if (moveBuffer != null) {
                moveBuffer.close();
                moveBuffer = null;
            }
            if (cleanupLease != null) {
                cleanupLease.close();
                cleanupLease = null;
            }
            synchronized (gate) {
                if (indexCheckpoint == this) {
                    indexCheckpoint = null;
                }
            }
            request.complete(new IndexCheckpointResult(status,
                    status == IndexCheckpointStatus.PERSISTED ? coverage : null), failure);
        }

        private final class Worker implements SegmentOwnerLoop.Command {
            private final int id;
            private final List<SegmentHotDirectory.IndexCut> cuts = new ArrayList<>();
            private SegmentOwnerLoop.Ticket ticket;
            private boolean ownerDone;
            private boolean ready;
            private boolean terminal;
            private boolean coordinatesReady;

            private Worker(int id) {
                this.id = id;
            }

            @Override
            public void run(SegmentOwnerLoop.Ticket accepted) {
                ticket = accepted;
                for (int shard = id; shard < hotDirectories.length; shard += workers.length) {
                    SegmentHotDirectory.IndexCut cut = hotDirectories[shard].pauseIndexForCheckpoint();
                    if (cut == null) {
                        unavailable(null);
                        break;
                    }
                    cuts.add(cut);
                }
                prepared(this);
            }

            private void resumeIndexes() {
                for (SegmentHotDirectory.IndexCut cut : cuts) {
                    hotDirectories[cut.stream().shard()].resumeIndexAfterCheckpoint();
                }
            }

            @Override
            public void terminal() {
                synchronized (IndexCheckpoint.this) {
                    if (!ownerDone) {
                        ownerDone = true;
                        if (++ownerTerminated == workers.length) {
                            request.terminal();
                        }
                    }
                }
            }

            @Override
            public void failed(Throwable cause) {
                resumeIndexes();
                synchronized (IndexCheckpoint.this) {
                    unavailable(cause);
                    if (!terminal) {
                        terminal = true;
                        finished++;
                    }
                    if (!ready) {
                        prepared(this);
                    } else if (coordinatesStarted && coordinatesDone != workers.length) {
                        if (!coordinatesReady) {
                            coordinatesRetired(this);
                        }
                    } else if (IndexCheckpoint.this.ready == workers.length && (!ioStarted || id == 0)) {
                        finishWorkers();
                    }
                    deliverIfFinished();
                }
            }
        }
    }

    /** False retains the same graph and locks for another drain attempt; it never closes around live I/O. */
    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive storage drain timeout required");
        }
        timeout.toNanos();
        SegmentExecutionGroup group = execution;
        if (group != null) {
            for (int worker = 0; worker < executionSettings.workers(); worker++) {
                if (group.owner(worker).isOwnerThread()) {
                    throw new IllegalStateException("Storage drain requires an external thread");
                }
            }
        }
        synchronized (closeGate) {
            synchronized (gate) {
                accepting = false;
                if (profileAdmission != null) {
                    profileAdmission.close();
                }
                if (closed) {
                    return true;
                }
                if (execution != null) {
                    execution.stopAdmission();
                }
            }
            long deadline = System.nanoTime() + timeout.toNanos();
            if (deleteFollower != null) {
                try {
                    deleteFollower.closeAsync().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (TimeoutException unfinished) {
                    return false;
                } catch (ExecutionException failure) {
                    throw new IOException("Delete follower drain failed", failure.getCause());
                }
            }
            IOException first = null;
            if (execution != null) {
                try {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0 || !execution.shutdown(Duration.ofNanos(remaining))) {
                        return false;
                    }
                } catch (IOException failure) {
                    if (!execution.terminated() || execution.inputs().usedBytes() != 0) {
                        throw failure;
                    }
                    first = failure;
                }
            }
            for (AutoCloseable resource : new AutoCloseable[] {files, index, bookie, arena, startupLock}) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (Exception failure) {
                        IOException cause = failure instanceof IOException io ? io
                                : new IOException("Storage resource cleanup failed", failure);
                        if (first == null) {
                            first = cause;
                        } else {
                            first.addSuppressed(cause);
                        }
                    }
                }
            }
            if (first != null) {
                throw first;
            }
            synchronized (gate) {
                if (files != null) {
                    files.retireClosedInventory();
                }
                for (SegmentHotDirectory hot : hotDirectories) {
                    if (hot != null) {
                        hot.retireClosedState(execution);
                    }
                }
                Arrays.fill(writes, null);
                Arrays.fill(reads, null);
                Arrays.fill(hotDirectories, null);
                Arrays.fill(drivers, null);
                controls = null;
                allocator = null;
                execution = null;
                closed = true;
            }
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Storage drain incomplete; retain this owner and retry after work ends");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Storage drain interrupted", interrupted);
        }
    }
}

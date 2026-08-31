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

package org.apache.bookkeeper.common.profile.startup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact.MigrationState;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.PublicationStatus;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.WritableRegistration;
import org.apache.bookkeeper.common.profile.startup.LocalReadinessStore.WriteStatus;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts.RecoveryStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadResult;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.VersionedRecord;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.DeviceStatus;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessMetrics.ColdOperation;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Outcome;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Reason;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.StartupPhase;

/**
 * Fail-closed reference startup state machine.
 *
 * <p>It has no production integration and consumes only already-parsed facts plus semantic in-memory-capable stores.
 */
public final class StartupReadinessHarness {

    private final LocalReadinessStore localReadinessStore;
    private final ProfileRegistrationStore persistentRegistrationStore;
    private final EphemeralProfileRegistrationStore ephemeralRegistrationStore;

    public StartupReadinessHarness(
            LocalReadinessStore localReadinessStore,
            ProfileRegistrationStore persistentRegistrationStore,
            EphemeralProfileRegistrationStore ephemeralRegistrationStore) {
        this.localReadinessStore = StartupFactSupport.requireNonNull(localReadinessStore);
        this.persistentRegistrationStore = StartupFactSupport.requireNonNull(persistentRegistrationStore);
        this.ephemeralRegistrationStore = StartupFactSupport.requireNonNull(ephemeralRegistrationStore);
    }

    /** Runs one complete cold-path attempt or throws at the requested deterministic crash boundary. */
    public StartupReadinessResult run(StartupReadinessInput input, StartupCrashPoint crashPoint) {
        StartupFactSupport.requireNonNull(input);
        StartupFactSupport.requireNonNull(crashPoint);
        StartupReadinessMetrics metrics = new StartupReadinessMetrics();
        List<StartupPhase> completed = new ArrayList<>();
        BookieCompatibilityFact compatibility = input.compatibility();

        metrics.increment(ColdOperation.COMPATIBILITY_FACT_READ);
        Reason compatibilityFailure = validateCompatibility(input);
        if (compatibilityFailure != null) {
            return fail(compatibility.bookieId(), compatibilityFailure, Outcome.NON_WRITABLE, completed, metrics);
        }
        complete(StartupPhase.COMPATIBILITY_FACT, completed, crashPoint, metrics);

        metrics.increment(ColdOperation.DEVICE_MANIFEST_READ);
        metrics.add(ColdOperation.DEVICE_SUPERBLOCK_READ, input.requiredDevices().devices().size());
        Reason deviceFailure = validateDevices(input);
        if (deviceFailure != null) {
            return fail(compatibility.bookieId(), deviceFailure, Outcome.NON_WRITABLE, completed, metrics);
        }
        complete(StartupPhase.REQUIRED_DEVICE_FACTS, completed, crashPoint, metrics);

        if (!validateRecoveryIdentity(input)) {
            return fail(
                    compatibility.bookieId(), Reason.RECOVERY_NOT_READY, Outcome.NON_WRITABLE, completed, metrics);
        }
        metrics.increment(ColdOperation.ALLOCATOR_RECOVERY);
        if (input.recoveryFacts().allocatorRecovery() != RecoveryStatus.RECOVERED) {
            return fail(
                    compatibility.bookieId(), Reason.RECOVERY_NOT_READY, Outcome.NON_WRITABLE, completed, metrics);
        }
        complete(StartupPhase.ALLOCATOR_RECOVERY, completed, crashPoint, metrics);

        metrics.increment(ColdOperation.ROUTE_RECOVERY);
        if (input.recoveryFacts().routeRecovery() != RecoveryStatus.RECOVERED) {
            return fail(
                    compatibility.bookieId(), Reason.RECOVERY_NOT_READY, Outcome.NON_WRITABLE, completed, metrics);
        }
        complete(StartupPhase.ROUTE_RECOVERY, completed, crashPoint, metrics);

        metrics.increment(ColdOperation.DELETE_RECOVERY);
        if (input.recoveryFacts().deleteRecovery() != RecoveryStatus.RECOVERED
                || !input.recoveryFacts()
                        .effectiveDeleteAssignmentGeneration()
                        .equals(compatibility.effectiveDeleteAssignmentGeneration())) {
            return fail(
                    compatibility.bookieId(), Reason.RECOVERY_NOT_READY, Outcome.NON_WRITABLE, completed, metrics);
        }
        complete(StartupPhase.DELETE_RECOVERY, completed, crashPoint, metrics);

        DurableLocalReadiness localReadiness = localReadiness(input);
        metrics.increment(ColdOperation.LOCAL_READINESS_DURABILITY);
        LocalReadinessStore.WriteResult localWrite = localReadinessStore.write(localReadiness);
        if (localWrite.status() == WriteStatus.CONFLICT) {
            return fail(
                    compatibility.bookieId(),
                    Reason.LOCAL_READINESS_CONFLICT,
                    Outcome.NON_WRITABLE,
                    completed,
                    metrics);
        }
        if (localWrite.status() == WriteStatus.DURABILITY_UNKNOWN
                && !localReadinessStore.read(compatibility.bookieId()).filter(localReadiness::equals).isPresent()) {
            return fail(
                    compatibility.bookieId(),
                    Reason.LOCAL_READINESS_NOT_DURABLE,
                    Outcome.RETRY_REQUIRED,
                    completed,
                    metrics);
        }
        complete(StartupPhase.DURABLE_LOCAL_READINESS, completed, crashPoint, metrics);

        ProfileReadinessRecord target = persistentReadiness(input);
        PersistentStep persistent = reconcilePersistent(target, metrics);
        if (persistent.versionedRecord() == null) {
            return fail(
                    compatibility.bookieId(), persistent.reason(), Outcome.RETRY_REQUIRED, completed, metrics);
        }
        complete(StartupPhase.PERSISTENT_READINESS, completed, crashPoint, metrics);

        VersionedRecord versioned = persistent.versionedRecord();
        ProfileServiceInfoProjection projection = serviceInfo(input, versioned);
        Reason staleReason = rejectStaleEphemeral(projection, metrics);
        if (staleReason != null) {
            return fail(compatibility.bookieId(), staleReason, Outcome.RETRY_REQUIRED, completed, metrics);
        }
        metrics.increment(ColdOperation.SERVICE_INFO_PUBLISH);
        EphemeralProfileRegistrationStore.PublicationResult servicePublication =
                ephemeralRegistrationStore.publishServiceInfo(projection);
        if (!publicationReconciles(
                servicePublication.status(),
                () -> ephemeralRegistrationStore
                        .readServiceInfo(compatibility.bookieId())
                        .filter(projection::equals))) {
            return fail(
                    compatibility.bookieId(),
                    Reason.SERVICE_INFO_NOT_MATCHING,
                    Outcome.RETRY_REQUIRED,
                    completed,
                    metrics);
        }
        complete(StartupPhase.MATCHING_SERVICE_INFO, completed, crashPoint, metrics);

        VersionedRecord currentPersistent = readExactPersistent(target, metrics);
        if (currentPersistent == null
                || currentPersistent.storeVersion() != versioned.storeVersion()
                || !readExactServiceInfo(projection, metrics)) {
            return fail(
                    compatibility.bookieId(),
                    Reason.FINAL_RECONCILIATION_MISMATCH,
                    Outcome.NON_WRITABLE,
                    completed,
                    metrics);
        }
        WritableRegistration registration = new WritableRegistration(
                compatibility.bookieId(),
                compatibility.storageIncarnation(),
                input.targetReadinessGeneration(),
                versioned.storeVersion());
        metrics.increment(ColdOperation.EPHEMERAL_REGISTRATION_READ);
        Optional<WritableRegistration> currentWritable =
                ephemeralRegistrationStore.readWritable(compatibility.bookieId());
        if (currentWritable.isPresent() && !currentWritable.get().equals(registration)) {
            return fail(
                    compatibility.bookieId(),
                    Reason.STALE_EPHEMERAL_REGISTRATION,
                    Outcome.RETRY_REQUIRED,
                    completed,
                    metrics);
        }
        if (currentWritable.isEmpty()) {
            metrics.increment(ColdOperation.EPHEMERAL_REGISTRATION_WRITE);
            EphemeralProfileRegistrationStore.PublicationResult writablePublication =
                    ephemeralRegistrationStore.registerWritable(registration);
            if (!publicationReconciles(
                    writablePublication.status(),
                    () -> ephemeralRegistrationStore
                            .readWritable(compatibility.bookieId())
                            .filter(registration::equals))) {
                return fail(
                        compatibility.bookieId(),
                        Reason.EPHEMERAL_REGISTRATION_FAILED,
                        Outcome.RETRY_REQUIRED,
                        completed,
                        metrics);
            }
        }
        complete(StartupPhase.EPHEMERAL_WRITABLE_REGISTRATION, completed, crashPoint, metrics);

        VersionedRecord finalPersistent = readExactPersistent(target, metrics);
        if (finalPersistent == null
                || finalPersistent.storeVersion() != versioned.storeVersion()
                || !readExactServiceInfo(projection, metrics)
                || !readExactWritable(registration, metrics)) {
            return fail(
                    compatibility.bookieId(),
                    Reason.FINAL_RECONCILIATION_MISMATCH,
                    Outcome.NON_WRITABLE,
                    completed,
                    metrics);
        }
        return new StartupReadinessResult(Outcome.WRITABLE, Reason.COMPLETE, completed, metrics.snapshot());
    }

    private Reason validateCompatibility(StartupReadinessInput input) {
        BookieCompatibilityFact compatibility = input.compatibility();
        if (compatibility.migrationState() != MigrationState.FORMAT_READY) {
            return Reason.COMPATIBILITY_REJECTED;
        }
        NewScopeFallbackManifest fallback = input.newScopeFallback();
        if (!fallback.permitsNewScopeWritable()
                || !fallback.newBookieId().equals(compatibility.bookieId())
                || !fallback.newStorageIncarnation().equals(compatibility.storageIncarnation())
                || fallback.rejectsOldIdentity(
                        compatibility.bookieId(),
                        compatibility.storageIncarnation(),
                        fallback.newCredentialScope())) {
            return Reason.UNSAFE_NEW_SCOPE;
        }
        if (input.runtimeReaderVersion() < compatibility.minimumReaderVersion()
                || input.runtimeWriterVersion() < compatibility.minimumWriterVersion()) {
            return Reason.RUNTIME_VERSION_REJECTED;
        }
        if (!input.supportedMandatoryFeatures().containsAll(compatibility.mandatoryLocalFeatures())) {
            return Reason.COMPATIBILITY_REJECTED;
        }
        return null;
    }

    private Reason validateDevices(StartupReadinessInput input) {
        BookieCompatibilityFact compatibility = input.compatibility();
        RequiredDeviceManifest manifest = input.requiredDevices();
        if (!manifest.identity().equals(compatibility.deviceManifestIdentity())
                || !manifest.generation().equals(compatibility.deviceManifestGeneration())) {
            return Reason.DEVICE_MANIFEST_MISMATCH;
        }
        for (RequiredDeviceManifest.RequiredDeviceFact device : manifest.devices()) {
            if (device.status() != DeviceStatus.READY
                    || !device.storageIncarnation().equals(compatibility.storageIncarnation())
                    || !device.manifestGeneration().equals(manifest.generation())
                    || !input.supportedMandatoryFeatures().containsAll(device.mandatoryFeatures())) {
                return Reason.DEVICE_NOT_READY;
            }
        }
        return null;
    }

    private boolean validateRecoveryIdentity(StartupReadinessInput input) {
        LocalRecoveryFacts recovery = input.recoveryFacts();
        BookieCompatibilityFact compatibility = input.compatibility();
        return recovery.storageIncarnation().equals(compatibility.storageIncarnation())
                && recovery.localFormatGeneration().equals(compatibility.localFormatGeneration());
    }

    private DurableLocalReadiness localReadiness(StartupReadinessInput input) {
        BookieCompatibilityFact compatibility = input.compatibility();
        return new DurableLocalReadiness(
                compatibility.bookieId(),
                compatibility.storageIncarnation(),
                compatibility.deviceManifestIdentity(),
                compatibility.deviceManifestGeneration(),
                compatibility.localFormatGeneration(),
                compatibility.effectiveDeleteAssignmentGeneration(),
                input.targetReadinessGeneration(),
                DurableLocalReadiness.Status.READY,
                true);
    }

    private ProfileReadinessRecord persistentReadiness(StartupReadinessInput input) {
        BookieCompatibilityFact compatibility = input.compatibility();
        return new ProfileReadinessRecord(
                compatibility.bookieId(),
                compatibility.storageIncarnation(),
                compatibility.activeEngine(),
                compatibility.profileWireMajor(),
                compatibility.profileWireMinor(),
                input.capabilityGeneration(),
                compatibility.localFormatGeneration(),
                compatibility.deviceManifestIdentity(),
                compatibility.deviceManifestGeneration(),
                compatibility.effectiveDeleteAssignmentGeneration(),
                input.targetReadinessGeneration(),
                compatibility.minimumReaderVersion(),
                compatibility.minimumWriterVersion(),
                input.capabilityHints(),
                ProfileReadinessRecord.State.READY);
    }

    private PersistentStep reconcilePersistent(
            ProfileReadinessRecord target, StartupReadinessMetrics metrics) {
        ReadResult initial = readPersistent(target.bookieId(), metrics);
        long expectedVersion;
        long expectedGeneration;
        if (initial.status() == ReadStatus.FOUND) {
            VersionedRecord current = initial.versionedRecord().orElseThrow();
            if (current.record().equals(target)) {
                return new PersistentStep(current, null);
            }
            if (!current.record().storageIncarnation().equals(target.storageIncarnation())
                    || current.record().localReadinessGeneration().compareTo(target.localReadinessGeneration()) >= 0) {
                return new PersistentStep(null, Reason.PERSISTENT_READINESS_CONFLICT);
            }
            expectedVersion = current.storeVersion();
            expectedGeneration = current.record().localReadinessGeneration().value();
        } else if (initial.status() == ReadStatus.NOT_FOUND) {
            expectedVersion = ProfileRegistrationStore.NO_STORE_VERSION;
            expectedGeneration = ProfileRegistrationStore.NO_READINESS_GENERATION;
        } else {
            return new PersistentStep(null, Reason.PERSISTENT_READINESS_UNAVAILABLE);
        }

        metrics.increment(ColdOperation.PERSISTENT_READINESS_CAS);
        ProfileRegistrationStore.CasResult cas = persistentRegistrationStore.compareAndSet(
                target.bookieId(), expectedVersion, expectedGeneration, target);
        if (cas.status() == CasStatus.UNAVAILABLE) {
            return new PersistentStep(null, Reason.PERSISTENT_READINESS_UNAVAILABLE);
        }
        VersionedRecord reconciled = readExactPersistent(target, metrics);
        if (reconciled != null) {
            return new PersistentStep(reconciled, null);
        }
        return new PersistentStep(null, Reason.PERSISTENT_READINESS_CONFLICT);
    }

    private Reason rejectStaleEphemeral(
            ProfileServiceInfoProjection expected, StartupReadinessMetrics metrics) {
        metrics.increment(ColdOperation.SERVICE_INFO_READ);
        Optional<ProfileServiceInfoProjection> service =
                ephemeralRegistrationStore.readServiceInfo(expected.bookieId());
        metrics.increment(ColdOperation.EPHEMERAL_REGISTRATION_READ);
        Optional<WritableRegistration> writable =
                ephemeralRegistrationStore.readWritable(expected.bookieId());
        if ((service.isPresent() && !service.get().equals(expected))
                || (writable.isPresent()
                        && (!writable.get().storageIncarnation().equals(expected.storageIncarnation())
                                || !writable.get().readinessGeneration().equals(expected.readinessGeneration())
                                || writable.get().persistentStoreVersion() != expected.persistentStoreVersion()))) {
            demote(expected.bookieId(), metrics);
            return Reason.STALE_EPHEMERAL_REGISTRATION;
        }
        return null;
    }

    private ProfileServiceInfoProjection serviceInfo(
            StartupReadinessInput input, VersionedRecord persistent) {
        BookieCompatibilityFact compatibility = input.compatibility();
        return new ProfileServiceInfoProjection(
                compatibility.bookieId(),
                compatibility.storageIncarnation(),
                input.targetReadinessGeneration(),
                persistent.storeVersion(),
                input.profileEndpoint(),
                input.capabilityHints());
    }

    private ReadResult readPersistent(String bookieId, StartupReadinessMetrics metrics) {
        metrics.increment(ColdOperation.PERSISTENT_READINESS_READ);
        try {
            return persistentRegistrationStore.read(bookieId);
        } catch (RuntimeException failure) {
            return ReadResult.unavailable();
        }
    }

    private VersionedRecord readExactPersistent(
            ProfileReadinessRecord target, StartupReadinessMetrics metrics) {
        ReadResult read = readPersistent(target.bookieId(), metrics);
        return read.status() == ReadStatus.FOUND
                        && read.versionedRecord().orElseThrow().record().equals(target)
                ? read.versionedRecord().orElseThrow()
                : null;
    }

    private boolean readExactServiceInfo(
            ProfileServiceInfoProjection expected, StartupReadinessMetrics metrics) {
        metrics.increment(ColdOperation.SERVICE_INFO_READ);
        return ephemeralRegistrationStore
                .readServiceInfo(expected.bookieId())
                .filter(expected::equals)
                .isPresent();
    }

    private boolean readExactWritable(
            WritableRegistration expected, StartupReadinessMetrics metrics) {
        metrics.increment(ColdOperation.EPHEMERAL_REGISTRATION_READ);
        return ephemeralRegistrationStore
                .readWritable(expected.bookieId())
                .filter(expected::equals)
                .isPresent();
    }

    private boolean publicationReconciles(
            PublicationStatus status, java.util.function.Supplier<Optional<?>> reread) {
        if (status == PublicationStatus.APPLIED || status == PublicationStatus.ALREADY_APPLIED) {
            return true;
        }
        return (status == PublicationStatus.RESPONSE_LOST || status == PublicationStatus.CONFLICT)
                && reread.get().isPresent();
    }

    private StartupReadinessResult fail(
            String bookieId,
            Reason reason,
            Outcome outcome,
            List<StartupPhase> completed,
            StartupReadinessMetrics metrics) {
        demote(bookieId, metrics);
        return new StartupReadinessResult(outcome, reason, completed, metrics.snapshot());
    }

    private void demote(String bookieId, StartupReadinessMetrics metrics) {
        metrics.increment(ColdOperation.EPHEMERAL_DEMOTION);
        try {
            ephemeralRegistrationStore.demote(bookieId);
        } catch (RuntimeException ignored) {
            // Fail closed: a failed demotion never produces a writable result.
        }
    }

    private void complete(
            StartupPhase phase,
            List<StartupPhase> completed,
            StartupCrashPoint crashPoint,
            StartupReadinessMetrics metrics) {
        completed.add(phase);
        if (crashPoint.matches(phase)) {
            throw new SimulatedStartupCrash(completed, metrics.snapshot());
        }
    }

    private record PersistentStep(VersionedRecord versionedRecord, Reason reason) {}
}

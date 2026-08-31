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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact.MigrationState;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.WritableRegistration;
import org.apache.bookkeeper.common.profile.startup.InMemoryEphemeralProfileRegistrationStore.Fault;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts.RecoveryStatus;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.AccessLevel;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.CredentialScope;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.OldScopeDisposition;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.RootKind;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.StorageRoot;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadStatus;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.DeviceStatus;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.RequiredDeviceFact;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessMetrics.ColdOperation;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Outcome;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Reason;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.StartupPhase;
import org.junit.Test;

public class StartupReadinessHarnessTest {

    @Test
    public void happyPathUsesFrozenOrderAndKeepsNormalAddCountersZero() {
        Environment environment = environment();

        StartupReadinessResult result = environment.harness.run(environment.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.WRITABLE, result.outcome());
        assertEquals(Reason.COMPLETE, result.reason());
        assertEquals(List.of(StartupPhase.values()), result.completedPhases());
        assertEquals(1, result.metrics().coldCount(ColdOperation.COMPATIBILITY_FACT_READ));
        assertEquals(2, result.metrics().coldCount(ColdOperation.DEVICE_SUPERBLOCK_READ));
        assertEquals(1, result.metrics().coldCount(ColdOperation.PERSISTENT_READINESS_CAS));
        assertTrue(result.metrics().normalAdd().isZero());
        assertEquals(
                List.of(
                        "read-service-info",
                        "read-writable",
                        "publish-service-info",
                        "read-service-info",
                        "read-writable",
                        "register-writable",
                        "read-service-info",
                        "read-writable"),
                environment.ephemeral.events());
    }

    @Test
    public void everyNamedCrashBoundaryRestartsToMatchingWritableState() {
        for (StartupCrashPoint crashPoint : StartupCrashPoint.values()) {
            if (crashPoint == StartupCrashPoint.NONE) {
                continue;
            }
            Environment environment = environment();
            SimulatedStartupCrash crash = assertThrows(
                    SimulatedStartupCrash.class,
                    () -> environment.harness.run(environment.input, crashPoint));
            assertFalse(crash.completedPhases().isEmpty());
            assertTrue(crash.metrics().normalAdd().isZero());

            StartupReadinessResult restart = environment.harness.run(environment.input, StartupCrashPoint.NONE);
            assertEquals(crashPoint.name(), Outcome.WRITABLE, restart.outcome());
            assertEquals(crashPoint.name(), Reason.COMPLETE, restart.reason());
        }
    }

    @Test
    public void missingPartialCorruptAndUnknownMandatoryDevicesNeverPublishReadiness() {
        List<DeviceStatus> rejected = List.of(
                DeviceStatus.MISSING,
                DeviceStatus.PARTIAL,
                DeviceStatus.CORRUPT,
                DeviceStatus.UNKNOWN_MANDATORY);
        for (DeviceStatus status : rejected) {
            Environment environment = environment();
            StartupReadinessInput input = withDevices(environment.input, requiredDevices(status));

            StartupReadinessResult result = environment.harness.run(input, StartupCrashPoint.NONE);

            assertEquals(status.name(), Outcome.NON_WRITABLE, result.outcome());
            assertEquals(status.name(), Reason.DEVICE_NOT_READY, result.reason());
            assertEquals(ReadStatus.NOT_FOUND, environment.persistent.read("bookie-new").status());
            assertTrue(environment.ephemeral.readWritable("bookie-new").isEmpty());
        }
    }

    @Test
    public void everyAllocatorRouteOrDeleteRecoveryFailureIsNonWritable() {
        List<RecoveryStatus> rejected = List.of(
                RecoveryStatus.MISSING,
                RecoveryStatus.CORRUPT,
                RecoveryStatus.UNKNOWN_MANDATORY,
                RecoveryStatus.DURABILITY_UNKNOWN);
        for (int component = 0; component < 3; component++) {
            for (RecoveryStatus status : rejected) {
                Environment environment = environment();
                RecoveryStatus allocator = component == 0 ? status : RecoveryStatus.RECOVERED;
                RecoveryStatus route = component == 1 ? status : RecoveryStatus.RECOVERED;
                RecoveryStatus delete = component == 2 ? status : RecoveryStatus.RECOVERED;
                StartupReadinessInput input = withRecovery(
                        environment.input,
                        new LocalRecoveryFacts(
                                incarnation(2),
                                new Generation(4),
                                allocator,
                                route,
                                delete,
                                new Generation(5)));

                StartupReadinessResult result = environment.harness.run(input, StartupCrashPoint.NONE);

                assertEquals(status.name(), Outcome.NON_WRITABLE, result.outcome());
                assertEquals(status.name(), Reason.RECOVERY_NOT_READY, result.reason());
                assertEquals(ReadStatus.NOT_FOUND, environment.persistent.read("bookie-new").status());
            }
        }
    }

    @Test
    public void localReadinessResponseLossRereadsDurableFact() {
        Environment afterApply = environment();
        afterApply.local.injectNextWriteFault(InMemoryLocalReadinessStore.Fault.DURABILITY_UNKNOWN_AFTER_APPLY);
        Environment beforeApply = environment();
        beforeApply.local.injectNextWriteFault(InMemoryLocalReadinessStore.Fault.DURABILITY_UNKNOWN_BEFORE_APPLY);

        assertEquals(
                Outcome.WRITABLE,
                afterApply.harness.run(afterApply.input, StartupCrashPoint.NONE).outcome());
        StartupReadinessResult retry = beforeApply.harness.run(beforeApply.input, StartupCrashPoint.NONE);
        assertEquals(Outcome.RETRY_REQUIRED, retry.outcome());
        assertEquals(Reason.LOCAL_READINESS_NOT_DURABLE, retry.reason());
    }

    @Test
    public void persistentCasResponseLossRereadsExactAppliedRecord() {
        Environment environment = environment();
        environment.persistent.injectNextCasFault(
                InMemoryProfileRegistrationStore.CasFault.RESPONSE_LOST_AFTER_APPLY);

        StartupReadinessResult result = environment.harness.run(environment.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.WRITABLE, result.outcome());
        assertEquals(ReadStatus.FOUND, environment.persistent.read("bookie-new").status());
    }

    @Test
    public void persistentCasConflictBeforeApplyRequiresRetryAndNeverRegisters() {
        Environment environment = environment();
        environment.persistent.injectNextCasFault(
                InMemoryProfileRegistrationStore.CasFault.CONFLICT_BEFORE_APPLY);

        StartupReadinessResult result = environment.harness.run(environment.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.RETRY_REQUIRED, result.outcome());
        assertEquals(Reason.PERSISTENT_READINESS_CONFLICT, result.reason());
        assertTrue(environment.ephemeral.readWritable("bookie-new").isEmpty());
    }

    @Test
    public void lowerMatchingGenerationAdvancesButIncarnationMismatchDemotes() {
        Environment advancing = environment();
        advancing.persistent.seed(readiness(advancing.input, incarnation(2), new Generation(8), 1));
        StartupReadinessResult advanced = advancing.harness.run(advancing.input, StartupCrashPoint.NONE);
        assertEquals(Outcome.WRITABLE, advanced.outcome());

        Environment mismatch = environment();
        ProfileReadinessRecord stale = readiness(mismatch.input, incarnation(9), new Generation(9), 1);
        ProfileRegistrationStore.VersionedRecord seeded = mismatch.persistent.seed(stale);
        ProfileServiceInfoProjection staleProjection = new ProfileServiceInfoProjection(
                "bookie-new",
                incarnation(9),
                new Generation(9),
                seeded.storeVersion(),
                "bookie-profile",
                List.of(101));
        mismatch.ephemeral.publishServiceInfo(staleProjection);
        mismatch.ephemeral.registerWritable(new WritableRegistration(
                "bookie-new", incarnation(9), new Generation(9), seeded.storeVersion()));

        StartupReadinessResult rejected = mismatch.harness.run(mismatch.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.RETRY_REQUIRED, rejected.outcome());
        assertEquals(Reason.PERSISTENT_READINESS_CONFLICT, rejected.reason());
        assertTrue(mismatch.ephemeral.readWritable("bookie-new").isEmpty());
        assertTrue(mismatch.ephemeral.readServiceInfo("bookie-new").isEmpty());
    }

    @Test
    public void staleEphemeralRegistrationIsDemotedBeforeRetryCanBecomeWritable() {
        Environment environment = environment();
        ProfileServiceInfoProjection stale = new ProfileServiceInfoProjection(
                "bookie-new", incarnation(2), new Generation(8), 77, "bookie-profile", List.of(101));
        environment.ephemeral.publishServiceInfo(stale);
        environment.ephemeral.registerWritable(
                new WritableRegistration("bookie-new", incarnation(2), new Generation(8), 77));

        StartupReadinessResult first = environment.harness.run(environment.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.RETRY_REQUIRED, first.outcome());
        assertEquals(Reason.STALE_EPHEMERAL_REGISTRATION, first.reason());
        assertTrue(environment.ephemeral.readWritable("bookie-new").isEmpty());
        StartupReadinessResult restart = environment.harness.run(environment.input, StartupCrashPoint.NONE);
        assertEquals(Outcome.WRITABLE, restart.outcome());
    }

    @Test
    public void serviceInfoAndRegistrationResponseLossReconcileWithoutDuplicateAuthority() {
        Environment environment = environment();
        environment.ephemeral.injectNextServiceInfoFault(Fault.RESPONSE_LOST_AFTER_APPLY);
        environment.ephemeral.injectNextWritableFault(Fault.RESPONSE_LOST_AFTER_APPLY);

        StartupReadinessResult result = environment.harness.run(environment.input, StartupCrashPoint.NONE);

        assertEquals(Outcome.WRITABLE, result.outcome());
        assertEquals(1, environment.ephemeral.events().stream()
                .filter("publish-service-info"::equals)
                .count());
        assertEquals(1, environment.ephemeral.events().stream()
                .filter("register-writable"::equals)
                .count());
    }

    @Test
    public void preparedCompatibilityAndUnsafeFallbackFailBeforeDeviceRecovery() {
        Environment prepared = environment();
        BookieCompatibilityFact compatibility = compatibility(MigrationState.PREPARED);
        StartupReadinessInput preparedInput = withCompatibility(prepared.input, compatibility);
        Environment unsafe = environment();
        NewScopeFallbackManifest unsafeFallback = fallback(
                AccessLevel.READ_ONLY, OldScopeDisposition.DRAINED, "bookie-new");
        StartupReadinessInput unsafeInput = withFallback(unsafe.input, unsafeFallback);

        assertEquals(
                Reason.COMPATIBILITY_REJECTED,
                prepared.harness.run(preparedInput, StartupCrashPoint.NONE).reason());
        assertEquals(
                Reason.UNSAFE_NEW_SCOPE,
                unsafe.harness.run(unsafeInput, StartupCrashPoint.NONE).reason());
    }

    private static Environment environment() {
        InMemoryLocalReadinessStore local = new InMemoryLocalReadinessStore();
        InMemoryProfileRegistrationStore persistent = new InMemoryProfileRegistrationStore();
        InMemoryEphemeralProfileRegistrationStore ephemeral =
                new InMemoryEphemeralProfileRegistrationStore();
        StartupReadinessHarness harness = new StartupReadinessHarness(local, persistent, ephemeral);
        return new Environment(local, persistent, ephemeral, harness, input());
    }

    private static StartupReadinessInput input() {
        return new StartupReadinessInput(
                compatibility(MigrationState.FORMAT_READY),
                requiredDevices(DeviceStatus.READY),
                new LocalRecoveryFacts(
                        incarnation(2),
                        new Generation(4),
                        RecoveryStatus.RECOVERED,
                        RecoveryStatus.RECOVERED,
                        RecoveryStatus.RECOVERED,
                        new Generation(5)),
                fallback(AccessLevel.NONE, OldScopeDisposition.DRAINED, "bookie-new"),
                new RollbackSafetyFacts(false, false, false, false, false, false, false),
                new Generation(9),
                new Generation(10),
                1,
                1,
                List.of(11, 12),
                "bookie-profile",
                List.of(101));
    }

    private static BookieCompatibilityFact compatibility(MigrationState migrationState) {
        return new BookieCompatibilityFact(
                "bookie-new",
                incarnation(2),
                EngineProfile.SEGMENT_WAL_ENGINE,
                1,
                0,
                1,
                List.of(11),
                manifestIdentity(3),
                new Generation(3),
                new Generation(4),
                new Generation(5),
                1,
                1,
                migrationState,
                new Generation(6));
    }

    private static RequiredDeviceManifest requiredDevices(DeviceStatus firstStatus) {
        return new RequiredDeviceManifest(
                manifestIdentity(3),
                new Generation(3),
                List.of(device("device-a", firstStatus), device("device-b", DeviceStatus.READY)));
    }

    private static RequiredDeviceFact device(String id, DeviceStatus status) {
        return new RequiredDeviceFact(
                id,
                incarnation(2),
                new Generation(3),
                status,
                1,
                1,
                1,
                new Generation(7),
                List.of(11, 12));
    }

    private static NewScopeFallbackManifest fallback(
            AccessLevel oldAccess, OldScopeDisposition disposition, String newBookieId) {
        return new NewScopeFallbackManifest(
                "bookie-old",
                newBookieId,
                incarnation(1),
                incarnation(2),
                roots("/old"),
                roots("/new"),
                new CredentialScope("old-service"),
                new CredentialScope("new-service"),
                disposition,
                oldAccess,
                AccessLevel.READ_WRITE);
    }

    private static List<StorageRoot> roots(String prefix) {
        return List.of(
                new StorageRoot(RootKind.ARENA, prefix + "/a-arena"),
                new StorageRoot(RootKind.INDEX, prefix + "/b-index"),
                new StorageRoot(RootKind.JOURNAL, prefix + "/c-journal"),
                new StorageRoot(RootKind.LEDGER, prefix + "/d-ledger"));
    }

    private static StartupReadinessInput withDevices(
            StartupReadinessInput input, RequiredDeviceManifest devices) {
        return copy(input, input.compatibility(), devices, input.recoveryFacts(), input.newScopeFallback());
    }

    private static StartupReadinessInput withRecovery(
            StartupReadinessInput input, LocalRecoveryFacts recovery) {
        return copy(input, input.compatibility(), input.requiredDevices(), recovery, input.newScopeFallback());
    }

    private static StartupReadinessInput withCompatibility(
            StartupReadinessInput input, BookieCompatibilityFact compatibility) {
        return copy(input, compatibility, input.requiredDevices(), input.recoveryFacts(), input.newScopeFallback());
    }

    private static StartupReadinessInput withFallback(
            StartupReadinessInput input, NewScopeFallbackManifest fallback) {
        return copy(input, input.compatibility(), input.requiredDevices(), input.recoveryFacts(), fallback);
    }

    private static StartupReadinessInput copy(
            StartupReadinessInput input,
            BookieCompatibilityFact compatibility,
            RequiredDeviceManifest devices,
            LocalRecoveryFacts recovery,
            NewScopeFallbackManifest fallback) {
        return new StartupReadinessInput(
                compatibility,
                devices,
                recovery,
                fallback,
                input.rollbackSafetyFacts(),
                input.targetReadinessGeneration(),
                input.capabilityGeneration(),
                input.runtimeReaderVersion(),
                input.runtimeWriterVersion(),
                input.supportedMandatoryFeatures(),
                input.profileEndpoint(),
                input.capabilityHints());
    }

    private static ProfileReadinessRecord readiness(
            StartupReadinessInput input,
            StorageIncarnation incarnation,
            Generation readinessGeneration,
            int capabilityMarker) {
        BookieCompatibilityFact compatibility = input.compatibility();
        return new ProfileReadinessRecord(
                compatibility.bookieId(),
                incarnation,
                compatibility.activeEngine(),
                compatibility.profileWireMajor(),
                compatibility.profileWireMinor(),
                new Generation(capabilityMarker),
                compatibility.localFormatGeneration(),
                compatibility.deviceManifestIdentity(),
                compatibility.deviceManifestGeneration(),
                compatibility.effectiveDeleteAssignmentGeneration(),
                readinessGeneration,
                compatibility.minimumReaderVersion(),
                compatibility.minimumWriterVersion(),
                input.capabilityHints(),
                ProfileReadinessRecord.State.READY);
    }

    private static StorageIncarnation incarnation(int marker) {
        return new StorageIncarnation(bytes(StorageIncarnation.LENGTH, marker));
    }

    private static ManifestIdentity manifestIdentity(int marker) {
        return new ManifestIdentity(bytes(ManifestIdentity.LENGTH, marker));
    }

    private static byte[] bytes(int length, int marker) {
        byte[] value = new byte[length];
        Arrays.fill(value, (byte) 0);
        value[0] = (byte) marker;
        return value;
    }

    private record Environment(
            InMemoryLocalReadinessStore local,
            InMemoryProfileRegistrationStore persistent,
            InMemoryEphemeralProfileRegistrationStore ephemeral,
            StartupReadinessHarness harness,
            StartupReadinessInput input) {
        private Environment {
            assertNotNull(local);
            assertNotNull(persistent);
            assertNotNull(ephemeral);
            assertNotNull(harness);
            assertNotNull(input);
        }
    }
}

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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.AccessLevel;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.CredentialScope;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.OldScopeDisposition;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.RootKind;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest.StorageRoot;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.DeviceStatus;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.RequiredDeviceFact;
import org.junit.Test;

public class StartupReadinessFactsTest {

    @Test
    public void identitiesAndCollectionsAreImmutable() {
        byte[] incarnationBytes = bytes(StorageIncarnation.LENGTH, 1);
        byte[] manifestBytes = bytes(ManifestIdentity.LENGTH, 2);
        List<Integer> features = Arrays.asList(11, 12);

        StorageIncarnation incarnation = new StorageIncarnation(incarnationBytes);
        ManifestIdentity identity = new ManifestIdentity(manifestBytes);
        BookieCompatibilityFact fact = compatibility(incarnation, identity, features);
        incarnationBytes[0] = 99;
        manifestBytes[0] = 99;
        features.set(0, 99);

        assertNotEquals(99, incarnation.value()[0]);
        assertNotEquals(99, identity.value()[0]);
        assertEquals(List.of(11, 12), fact.mandatoryLocalFeatures());
        assertThrows(UnsupportedOperationException.class, () -> fact.mandatoryLocalFeatures().add(13));
    }

    @Test
    public void strictFactsRejectAliasesAndInvalidVersions() {
        assertThrows(StartupReadinessValidationException.class, () -> new Generation(0));
        assertThrows(StartupReadinessValidationException.class, () -> new StorageIncarnation(new byte[16]));
        assertThrows(
                StartupReadinessValidationException.class,
                () -> compatibility(incarnation(1), manifestIdentity(2), List.of(12, 11)));
        assertThrows(
                StartupReadinessValidationException.class,
                () -> new BookieCompatibilityFact(
                        "bookie-new",
                        incarnation(1),
                        EngineProfile.CLASSIC_ENGINE,
                        1,
                        0,
                        1,
                        List.of(11),
                        manifestIdentity(2),
                        new Generation(3),
                        new Generation(4),
                        new Generation(5),
                        1,
                        1,
                        BookieCompatibilityFact.MigrationState.FORMAT_READY,
                        new Generation(6)));
    }

    @Test
    public void requiredDeviceManifestRequiresCanonicalUniqueOrder() {
        RequiredDeviceFact deviceA = device("device-a", DeviceStatus.READY);
        RequiredDeviceFact deviceB = device("device-b", DeviceStatus.READY);
        RequiredDeviceManifest manifest = new RequiredDeviceManifest(
                manifestIdentity(2), new Generation(3), List.of(deviceA, deviceB));

        assertEquals(2, manifest.devices().size());
        assertThrows(
                StartupReadinessValidationException.class,
                () -> new RequiredDeviceManifest(
                        manifestIdentity(2), new Generation(3), List.of(deviceB, deviceA)));
        assertThrows(
                StartupReadinessValidationException.class,
                () -> new RequiredDeviceManifest(
                        manifestIdentity(2), new Generation(3), List.of(deviceA, deviceA)));
    }

    @Test
    public void newScopeRequiresIdentityRootAndCredentialIsolationForWritable() {
        NewScopeFallbackManifest safe = scopeManifest(
                "bookie-old", "bookie-new", incarnation(1), incarnation(2), AccessLevel.NONE);
        NewScopeFallbackManifest unsafeAccess = scopeManifest(
                "bookie-old", "bookie-new", incarnation(1), incarnation(2), AccessLevel.READ_ONLY);
        NewScopeFallbackManifest sameIdentity = scopeManifest(
                "bookie-old", "bookie-old", incarnation(1), incarnation(2), AccessLevel.NONE);

        assertTrue(safe.permitsNewScopeWritable());
        assertTrue(safe.rejectsOldIdentity(
                "bookie-old", incarnation(1), new CredentialScope("old-service")));
        assertFalse(unsafeAccess.permitsNewScopeWritable());
        assertFalse(sameIdentity.permitsNewScopeWritable());
    }

    @Test
    public void rootManifestRequiresJournalLedgerIndexAndArena() {
        List<StorageRoot> incomplete = List.of(
                new StorageRoot(RootKind.JOURNAL, "/new/a-journal"),
                new StorageRoot(RootKind.LEDGER, "/new/b-ledger"),
                new StorageRoot(RootKind.INDEX, "/new/c-index"));

        assertThrows(
                StartupReadinessValidationException.class,
                () -> new NewScopeFallbackManifest(
                        "bookie-old",
                        "bookie-new",
                        incarnation(1),
                        incarnation(2),
                        roots("/old"),
                        incomplete,
                        new CredentialScope("old-service"),
                        new CredentialScope("new-service"),
                        OldScopeDisposition.DRAINED,
                        AccessLevel.NONE,
                        AccessLevel.READ_WRITE));
    }

    @Test
    public void anyExistingOrUnknownAuthorityRefusesOldBinaryRollback() {
        assertTrue(new RollbackSafetyFacts(false, true, false, false, false, false, false)
                .refusesOldBinarySameScopeRollback());
        assertTrue(new RollbackSafetyFacts(false, false, false, false, false, false, true)
                .refusesOldBinarySameScopeRollback());
        assertFalse(new RollbackSafetyFacts(false, false, false, false, false, false, false)
                .refusesOldBinarySameScopeRollback());
        assertEquals(4, new RollbackSafetyFacts(true, true, true, true, true, true, true)
                .allowedRecoveryActions()
                .size());
    }

    private static BookieCompatibilityFact compatibility(
            StorageIncarnation incarnation, ManifestIdentity identity, List<Integer> features) {
        return new BookieCompatibilityFact(
                "bookie-new",
                incarnation,
                EngineProfile.SEGMENT_WAL_ENGINE,
                1,
                0,
                1,
                features,
                identity,
                new Generation(3),
                new Generation(4),
                new Generation(5),
                1,
                1,
                BookieCompatibilityFact.MigrationState.FORMAT_READY,
                new Generation(6));
    }

    private static RequiredDeviceFact device(String id, DeviceStatus status) {
        return new RequiredDeviceFact(
                id,
                incarnation(1),
                new Generation(3),
                status,
                1,
                1,
                1,
                new Generation(7),
                List.of(11, 12));
    }

    private static NewScopeFallbackManifest scopeManifest(
            String oldBookie,
            String newBookie,
            StorageIncarnation oldIncarnation,
            StorageIncarnation newIncarnation,
            AccessLevel oldAccess) {
        return new NewScopeFallbackManifest(
                oldBookie,
                newBookie,
                oldIncarnation,
                newIncarnation,
                roots("/old"),
                roots("/new"),
                new CredentialScope("old-service"),
                new CredentialScope("new-service"),
                OldScopeDisposition.DRAINED,
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

    private static StorageIncarnation incarnation(int marker) {
        return new StorageIncarnation(bytes(StorageIncarnation.LENGTH, marker));
    }

    private static ManifestIdentity manifestIdentity(int marker) {
        return new ManifestIdentity(bytes(ManifestIdentity.LENGTH, marker));
    }

    private static byte[] bytes(int length, int marker) {
        byte[] value = new byte[length];
        value[0] = (byte) marker;
        return value;
    }
}

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.PublicationStatus;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.WritableRegistration;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZKProfileStartupRequirementsStoreTest extends ProfileZKTestBase {
    private static ZKProfileStartupRequirementsStore store(ZooKeeper client) {
        return new ZKProfileStartupRequirementsStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static BookieCompatibilityFact compatibility(int minimumReader) {
        var one = new Generation(1);
        return new BookieCompatibilityFact(BOOKIE, new StorageIncarnation(bytes(16, 1)),
                EngineProfile.SEGMENT_WAL_ENGINE,
                1, 0, 1, List.of(1), new ManifestIdentity(bytes(32, 2)), one, one, one,
                minimumReader, 1, BookieCompatibilityFact.MigrationState.FORMAT_READY, one);
    }

    private static RequiredDeviceManifest devices() {
        var c = compatibility(1);
        var one = new Generation(1);
        return new RequiredDeviceManifest(c.deviceManifestIdentity(), one,
                List.of(new RequiredDeviceManifest.RequiredDeviceFact("arena", c.storageIncarnation(), one,
                        RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1, one, List.of(1))));
    }

    private static RequiredDeviceManifest manifestDevices(String... names) {
        var old = devices();
        var member = old.devices().get(0);
        return new RequiredDeviceManifest(old.identity(), old.generation(), Arrays.stream(names)
                .map(name -> new RequiredDeviceManifest.RequiredDeviceFact(name, member.storageIncarnation(),
                        member.manifestGeneration(), member.status(), member.arenaFormatVersion(),
                        member.controlFormatVersion(), member.checkpointFormatVersion(), member.superblockGeneration(),
                        member.mandatoryFeatures())).toList());
    }

    private static ProfileReadinessRecord readiness() {
        var c = compatibility(1);
        var one = new Generation(1);
        return new ProfileReadinessRecord(BOOKIE, c.storageIncarnation(), c.activeEngine(), 1, 0, one, one,
                c.deviceManifestIdentity(), one, one, one, 1, 1, List.of(1), ProfileReadinessRecord.State.READY);
    }

    private ZKProfileEphemeralRegistrationStore publisher(ZKProfileStartupRequirementsStore source,
            ZKProfileStartupRequirementsStore.Observation observation) {
        var persistent = new ZKProfileRegistrationStore(first, ROOT + "-registration", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        assertEquals(CasStatus.APPLIED,
                persistent.compareAndSet(BOOKIE, -1, 0, readiness()).status());
        var read = persistent.read(BOOKIE).versionedRecord().orElseThrow();
        var publisher = new ZKProfileEphemeralRegistrationStore(first, ROOT + "-registration", BOOKIE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        publisher.bindStartupRequirements(source, observation);
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(new ProfileServiceInfoProjection(BOOKIE,
                    read.record().storageIncarnation(), read.record().localReadinessGeneration(), read.storeVersion(),
                    "tls://localhost:3181", List.of(1))).status());
        return publisher;
    }

    private WritableRegistration writable() {
        var read = new ZKProfileRegistrationStore(first, ROOT + "-registration", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5)).read(BOOKIE).versionedRecord().orElseThrow();
        return new WritableRegistration(
                BOOKIE, read.record().storageIncarnation(),
                read.record().localReadinessGeneration(), read.storeVersion());
    }

    @Test
    public void sourceIdentityMatchesVerifiedNativeBirthAndDefensivelyRetainsTheOriginalBytes() throws Exception {
        var source = store(first);
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                source.provision(compatibility(1), devices()));
        var observation = source.read(BOOKIE).observation().orElseThrow();
        var identity = observation.sourceIdentity();
        String path = source.path(BOOKIE);
        var stat = first.exists(path, false);
        byte[] original = first.getData(path, false, null);
        assertEquals(ProfileStartupRequirementsCodec.decode(original).compatibility(),
                observation.commissionedCompatibility());
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(original);
        assertEquals(path, identity.nodePath());
        assertEquals(ProfileStartupRequirementsCodec.decode(original).birth(), identity.birthId());
        assertEquals(stat.getCzxid(), identity.birthZxid());
        assertEquals(stat.getMzxid(), identity.modifiedZxid());
        assertEquals(stat.getVersion(), identity.version());
        assertArrayEquals(digest, identity.sha256());
        var secondRead = store(second).read(BOOKIE).observation().orElseThrow().sourceIdentity();
        assertEquals(identity, secondRead);
        assertEquals(identity.hashCode(), secondRead.hashCode());
        identity.sha256()[0] ^= 1;
        assertArrayEquals(digest, identity.sha256());
        var value = new ZKProfileStartupRequirementsStore.SourceIdentity(identity.nodePath(), identity.birthId(),
                identity.birthZxid(), identity.modifiedZxid(), identity.version(), digest);
        digest[0] ^= 1;
        assertEquals(identity, value);
        assertArrayEquals(original, first.getData(path, false, null));
        assertEquals(stat, first.exists(path, false));
    }

    @Test
    public void identicalBytesAndEnvelopeBirthCannotHideNativeSourceRecreation() throws Exception {
        var source = store(first);
        source.provision(compatibility(1), devices());
        var original = source.read(BOOKIE).observation().orElseThrow().sourceIdentity();
        String path = source.path(BOOKIE);
        byte[] bytes = first.getData(path, false, null);
        String marker = path + "/" + first.getChildren(path, false).get(0);
        first.multi(List.of(Op.delete(marker, 0), Op.delete(path, 0),
                Op.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(marker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
        var current = store(second).read(BOOKIE).observation().orElseThrow().sourceIdentity();
        assertEquals(original.birthId(), current.birthId());
        assertEquals(original.version(), current.version());
        assertArrayEquals(original.sha256(), current.sha256());
        assertTrue(current.birthZxid() > original.birthZxid());
        assertNotEquals(original, current);
        assertArrayEquals(bytes, first.getData(path, false, null));
    }

    @Test
    public void identicalBytesAndNativeBirthInDifferentNamespacesRemainDistinct() throws Exception {
        var source = store(first);
        source.provision(compatibility(1), devices());
        byte[] bytes = first.getData(source.path(BOOKIE), false, null);
        var birth = ProfileStartupRequirementsCodec.decode(bytes).birth();
        String firstRoot = ROOT + "-a";
        String secondRoot = ROOT + "-b";
        first.create(firstRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.create(secondRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var firstStore = new ZKProfileStartupRequirementsStore(first, firstRoot, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        var secondStore = new ZKProfileStartupRequirementsStore(second, secondRoot, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        String firstPath = firstStore.path(BOOKIE);
        String secondPath = secondStore.path(BOOKIE);
        first.multi(List.of(Op.create(firstPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(firstPath + "/birth-" + birth, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT),
                Op.create(secondPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(secondPath + "/birth-" + birth, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT)));
        var firstIdentity = firstStore.read(BOOKIE).observation().orElseThrow().sourceIdentity();
        var secondIdentity = secondStore.read(BOOKIE).observation().orElseThrow().sourceIdentity();
        assertEquals(firstIdentity.birthId(), secondIdentity.birthId());
        assertEquals(firstIdentity.birthZxid(), secondIdentity.birthZxid());
        assertArrayEquals(firstIdentity.sha256(), secondIdentity.sha256());
        assertNotEquals(firstIdentity.nodePath(), secondIdentity.nodePath());
        assertNotEquals(firstIdentity, secondIdentity);
    }

    @Test
    public void provisionAndReadAreImmutableAndLostReplyIsResolvedByActualSource() throws Exception {
        var source = store(first);
        assertEquals(ZKProfileStartupRequirementsStore.Status.NOT_FOUND, store(second).read(BOOKIE).status());
        first.loseReply.set(true);
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.RESPONSE_LOST,
                source.provision(compatibility(1), devices()));
        var observed = store(second).read(BOOKIE);
        assertEquals(ZKProfileStartupRequirementsStore.Status.FOUND, observed.status());
        assertEquals(compatibility(1), observed.observation().orElseThrow().compatibility());
        assertEquals(devices(), observed.observation().orElseThrow().requiredDevices());
        var stat = first.exists(source.path(BOOKIE), false);
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.ALREADY_APPLIED,
                source.provision(compatibility(1), devices()));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.CONFLICT,
                source.provision(compatibility(2), devices()));
        assertEquals(stat, first.exists(source.path(BOOKIE), false));
    }

    @Test
    public void concurrentProvisioningHasOneWinnerWithoutOverwritingDifferentRequirements() throws Exception {
        first.beforeMulti.set(() -> assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                store(second).provision(compatibility(2), devices())));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.CONFLICT,
                store(first).provision(compatibility(1), devices()));
        assertEquals(compatibility(2), store(first).read(BOOKIE).observation().orElseThrow().compatibility());
    }

    @Test
    public void corruptOrMissingBirthNeverBecomesAProvisioningSuccess() throws Exception {
        var source = store(first);
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                source.provision(compatibility(1), devices()));
        var path = source.path(BOOKIE);
        var anchor = first.getChildren(path, false).get(0);
        first.delete(path + "/" + anchor, -1);
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, source.read(BOOKIE).status());
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.UNAVAILABLE,
                source.provision(compatibility(1), devices()));
        assertTrue(first.getChildren(path, false).isEmpty());
        var original = first.getData(path, false, null);
        first.setData(path, original, -1);
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, source.read(BOOKIE).status());
        first.delete(path, -1);
        first.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, source.read(BOOKIE).status());
    }

    @Test
    public void recreatedSourceDoesNotSubstituteForFrozenPublisherObservation() throws Exception {
        first.create(ROOT + "-registration", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var source = store(first);
        source.provision(compatibility(1), devices());
        var observed = source.read(BOOKIE).observation().orElseThrow();
        var publisher = publisher(source, observed);
        var path = source.path(BOOKIE);
        for (var child : first.getChildren(path, false)) {
            first.delete(path + "/" + child, -1);
        }
        first.delete(path, -1);
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                store(second).provision(compatibility(1), devices()));
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.registerWritable(writable()).status());
        assertFalse(publisher.readWritable(BOOKIE).isPresent());
        assertThrows(IllegalStateException.class,
                () -> publisher.bindStartupRequirements(source, source.read(BOOKIE).observation().orElseThrow()));
    }

    @Test
    public void sourceChangeBetweenFinalReadAndPublicationIsRejectedBySameTransaction() throws Exception {
        first.create(ROOT + "-registration", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var source = store(first);
        source.provision(compatibility(1), devices());
        var publisher = publisher(source, source.read(BOOKIE).observation().orElseThrow());
        first.beforeMulti.set(() -> {
            try {
                var path = source.path(BOOKIE);
                for (var child : second.getChildren(path, false)) {
                    second.delete(path + "/" + child, -1);
                }
                second.delete(path, -1);
                assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                        store(second).provision(compatibility(1), devices()));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
        assertEquals(PublicationStatus.CONFLICT, publisher.registerWritable(writable()).status());
        String record = ROOT + "-registration/" + first.getChildren(ROOT + "-registration", false).get(0);
        assertEquals(null, first.exists(record + "/writable", false));
        assertTrue(first.exists(record + "/service-info", false) != null);
    }

    @Test
    public void boundSourceSurvivesWritableLostReplyAndRejectsForeignClientObservation() throws Exception {
        first.create(ROOT + "-registration", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var source = store(first);
        source.provision(compatibility(1), devices());
        var publisher = publisher(source, source.read(BOOKIE).observation().orElseThrow());
        first.loseReply.set(true);
        assertEquals(PublicationStatus.RESPONSE_LOST, publisher.registerWritable(writable()).status());
        assertTrue(publisher.readWritable(BOOKIE).isPresent());
        var admissionPublisher = new ZKProfileEphemeralRegistrationStore(first, ROOT + "-registration", BOOKIE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        assertThrows(IllegalArgumentException.class, () -> admissionPublisher.bindStartupRequirements(
                source, store(second).read(BOOKIE).observation().orElseThrow()));
        publisher.demote(BOOKIE);
    }

    @Test
    public void codecRejectsCorruptionUnknownFieldsTruncationAndNoncanonicalText() {
        var expected = new ProfileStartupRequirementsCodec.Envelope(new UUID(1, 2), compatibility(1), devices());
        byte[] encoded = ProfileStartupRequirementsCodec.encode(expected);
        assertEquals(expected, ProfileStartupRequirementsCodec.decode(encoded));
        byte[] vector = java.util.HexFormat.of().parseHex(
                "55575351000000010000000000000001000000000000000200000011626f6f6b69653a5b3a3a315d3a333138310101010101"
                + "0101010101010101010101000000030000000100000000000000010000000100000001020202020202020202020202020202"
                + "0202020202020202020202020202020202000000000000000100000000000000010000000000000001000000010000000100"
                + "0000020000000000000001000000056172656e61010101010101010101010101010101010000000000000001000000010000"
                + "00010000000100000000000000010000000100000001ee9a81b1");
        assertArrayEquals(vector, encoded);
        assertArrayEquals(encoded,
                ProfileStartupRequirementsCodec.encode(ProfileStartupRequirementsCodec.decode(encoded)));
        for (int length : List.of(0, 1, 183, encoded.length - 1, encoded.length + 1, 4097)) {
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileStartupRequirementsCodec.decode(Arrays.copyOf(encoded, length)));
        }
        for (int offset : List.of(0, 4, 8, encoded.length - 1)) {
            byte[] damaged = encoded.clone();
            damaged[offset] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> ProfileStartupRequirementsCodec.decode(damaged));
        }
        for (boolean unknownVersion : List.of(true, false)) {
            byte[] invalid = encoded.clone();
            if (unknownVersion) {
                ByteBuffer.wrap(invalid).putInt(4, 2);
            } else {
                invalid[28] = (byte) 0xff; // Invalid UTF-8 in the first Bookie name byte.
            }
            CRC32C crc = new CRC32C();
            crc.update(invalid, 0, invalid.length - 4);
            ByteBuffer.wrap(invalid).putInt(invalid.length - 4, (int) crc.getValue());
            assertThrows(IllegalArgumentException.class, () -> ProfileStartupRequirementsCodec.decode(invalid));
        }
    }

    @Test
    public void fixedManifestSchemaIsExplicitAndKeepsTheNativeBirthAndBytes() throws Exception {
        var source = store(first);
        var required = manifestDevices("arena-a", "arena-b");
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                source.provisionManifest(compatibility(1), required));
        var observed = source.readManifest(BOOKIE).observation().orElseThrow();
        var identity = observed.sourceIdentity();
        String path = identity.nodePath();
        byte[] bytes = first.getData(path, false, null);
        String marker = path + "/birth-" + identity.birthId();
        var stat = first.exists(path, false);
        assertEquals(stat.getCzxid(), first.exists(marker, false).getCzxid());
        assertEquals(0, stat.getVersion());
        assertEquals(2, ByteBuffer.wrap(bytes).getInt(4));
        assertEquals(required, observed.requiredDevices());
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.ALREADY_APPLIED,
                source.provisionManifest(compatibility(1), required));
        assertEquals(identity, source.readManifest(BOOKIE).observation().orElseThrow().sourceIdentity());
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.UNAVAILABLE,
                source.provision(compatibility(1), devices()));
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, source.read(BOOKIE).status());
        assertArrayEquals(bytes, first.getData(path, false, null));
        assertEquals(stat, first.exists(path, false));

        String otherRoot = ROOT + "-v1";
        first.create(otherRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var legacy = new ZKProfileStartupRequirementsStore(first, otherRoot, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                legacy.provision(compatibility(1), devices()));
        var legacyIdentity = legacy.read(BOOKIE).observation().orElseThrow().sourceIdentity();
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.UNAVAILABLE,
                legacy.provisionManifest(compatibility(1), devices()));
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, legacy.readManifest(BOOKIE).status());
        assertEquals(legacyIdentity, legacy.read(BOOKIE).observation().orElseThrow().sourceIdentity());

        first.multi(List.of(Op.delete(marker, 0), Op.delete(path, 0)));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                source.provisionManifest(compatibility(1), devices()));
        var one = source.readManifest(BOOKIE).observation().orElseThrow();
        assertTrue(one.isManifestSource());
        assertThrows(IllegalArgumentException.class, () -> one.applyTo(null));
        var publisher = new ZKProfileEphemeralRegistrationStore(first, ROOT + "-registration", BOOKIE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        assertThrows(IllegalArgumentException.class, () -> publisher.bindStartupRequirements(source, one));
        first.delete(path + "/birth-" + one.sourceIdentity().birthId(), 0);
        assertEquals(ZKProfileStartupRequirementsStore.Status.CORRUPT, source.readManifest(BOOKIE).status());
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.UNAVAILABLE,
                source.provisionManifest(compatibility(1), devices()));
    }

    @Test
    public void fixedManifestCodecRejectsNoncanonicalOrUnboundedDeviceVectors() {
        var expected = new ProfileStartupRequirementsCodec.Envelope(2, new UUID(1, 2), compatibility(1),
                manifestDevices("arena-a", "arena-b"));
        byte[] encoded = ProfileStartupRequirementsCodec.encode(expected);
        assertEquals(expected, ProfileStartupRequirementsCodec.decode(encoded));
        byte[] legacy = ProfileStartupRequirementsCodec.encode(new ProfileStartupRequirementsCodec.Envelope(
                expected.birth(), expected.compatibility(), manifestDevices("arena-a")));
        int memberBytes = 4 + 7 + 16 + 8 + 3 * 4 + 8 + 2 * 4;
        int countOffset = legacy.length - Integer.BYTES - memberBytes;
        assertEquals(2, ByteBuffer.wrap(encoded).getInt(countOffset));
        assertArrayEquals(Arrays.copyOfRange(legacy, countOffset, legacy.length - 4),
                Arrays.copyOfRange(encoded, countOffset + 4, countOffset + 4 + memberBytes));
        byte[] unknownSchema = encoded.clone();
        ByteBuffer.wrap(unknownSchema).putInt(4, 3);
        assertInvalidManifestCodec(unknownSchema);
        for (int invalidCount : List.of(0, 65)) {
            byte[] invalid = encoded.clone();
            ByteBuffer.wrap(invalid).putInt(countOffset, invalidCount);
            assertInvalidManifestCodec(invalid);
        }
        int firstLastCharacter = countOffset + 4 + 4 + 6;
        byte[] duplicate = encoded.clone();
        duplicate[firstLastCharacter + memberBytes] = 'a';
        assertInvalidManifestCodec(duplicate);
        byte[] unordered = encoded.clone();
        unordered[firstLastCharacter] = 'b';
        unordered[firstLastCharacter + memberBytes] = 'a';
        assertInvalidManifestCodec(unordered);
        assertInvalidManifestCodec(Arrays.copyOf(encoded, encoded.length + 1));
        assertInvalidManifestCodec(Arrays.copyOf(encoded, encoded.length - 1));
        assertInvalidManifestCodec(Arrays.copyOf(encoded, 32 * 1024 + 1));
        var names = new ArrayList<String>();
        for (int i = 0; i < 65; i++) {
            names.add("arena-" + (100 + i));
        }
        var tooMany = new ProfileStartupRequirementsCodec.Envelope(2, expected.birth(), expected.compatibility(),
                manifestDevices(names.toArray(String[]::new)));
        assertThrows(IllegalArgumentException.class, () -> ProfileStartupRequirementsCodec.encode(tooMany));

        var c = expected.compatibility();
        var features = java.util.stream.IntStream.rangeClosed(1, 64).boxed().toList();
        var maximumCompatibility = new BookieCompatibilityFact("b".repeat(BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES),
                c.storageIncarnation(), c.activeEngine(), c.profileWireMajor(), c.profileWireMinor(),
                c.descriptorHashSuite(), features, c.deviceManifestIdentity(), c.deviceManifestGeneration(),
                c.localFormatGeneration(), c.effectiveDeleteAssignmentGeneration(), c.minimumReaderVersion(),
                c.minimumWriterVersion(), c.migrationState(), c.migrationGeneration());
        var maximumDevices = new RequiredDeviceManifest(c.deviceManifestIdentity(), c.deviceManifestGeneration(),
                names.subList(0, 64).stream().map(name -> new RequiredDeviceManifest.RequiredDeviceFact(
                        name + "x".repeat(119), c.storageIncarnation(), c.deviceManifestGeneration(),
                        RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1, new Generation(1), features)).toList());
        var maximum = new ProfileStartupRequirementsCodec.Envelope(2, expected.birth(),
                maximumCompatibility, maximumDevices);
        byte[] maximumBytes = ProfileStartupRequirementsCodec.encode(maximum);
        assertEquals(28_563, maximumBytes.length);
        assertEquals(maximum, ProfileStartupRequirementsCodec.decode(maximumBytes));
    }

    private static void assertInvalidManifestCodec(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        ByteBuffer.wrap(bytes).putInt(bytes.length - Integer.BYTES, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileStartupRequirementsCodec.decode(bytes));
    }

    @Test
    public void aclRejectionAndHeldCommitDoNotFabricateSuccess() throws Exception {
        first.create(ROOT + "-denied", new byte[0], ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
        var denied = new ZKProfileStartupRequirementsStore(first, ROOT + "-denied", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.UNAVAILABLE,
                denied.provision(compatibility(1), devices()));
        assertEquals(ZKProfileStartupRequirementsStore.Status.NOT_FOUND, denied.read(BOOKIE).status());
        var source = new ZKProfileStartupRequirementsStore(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofMillis(200));
        first.holdReply.set(true);
        var pending = CompletableFuture.supplyAsync(() -> source.provision(compatibility(1), devices()));
        assertTrue(first.replyHeld.await(5, TimeUnit.SECONDS));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.RESPONSE_LOST, pending.get(5, TimeUnit.SECONDS));
        assertEquals(ZKProfileStartupRequirementsStore.Status.FOUND, store(second).read(BOOKIE).status());
        first.lateReply.getAndSet(null).run();
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.ALREADY_APPLIED,
                store(first).provision(compatibility(1), devices()));
    }
}

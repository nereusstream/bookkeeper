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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
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

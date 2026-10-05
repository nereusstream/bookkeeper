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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
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
import org.apache.bookkeeper.common.profile.startup.InMemoryLocalReadinessStore;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.VersionedRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts;
import org.apache.bookkeeper.common.profile.startup.StartupCrashPoint;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessHarness;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ZKProfileEphemeralRegistrationStoreTest extends ProfileZKTestBase {
    private ZKProfileRegistrationStore persistent(TestClient client) {
        return new ZKProfileRegistrationStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private ZKProfileEphemeralRegistrationStore publisher(TestClient client) {
        return new ZKProfileEphemeralRegistrationStore(client, ROOT, BOOKIE,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private String path() {
        return ROOT + "/" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(BOOKIE.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static ProfileReadinessRecord record(long generation) {
        return new ProfileReadinessRecord(BOOKIE, storage(1), EngineProfile.SEGMENT_WAL_ENGINE,
                1, 2, new Generation(3), new Generation(4), new ManifestIdentity(bytes(32, 2)), new Generation(5),
                new Generation(6), new Generation(generation), 7, 8, List.of(11, 13),
                ProfileReadinessRecord.State.READY);
    }

    private VersionedRecord create() {
        assertEquals(CasStatus.APPLIED, persistent(first).compareAndSet(BOOKIE, -1, 0, record(1)).status());
        return persistent(second).read(BOOKIE).versionedRecord().orElseThrow();
    }

    private static ProfileServiceInfoProjection projection(VersionedRecord record) {
        return new ProfileServiceInfoProjection(BOOKIE, record.record().storageIncarnation(),
                record.record().localReadinessGeneration(), record.storeVersion(), "bookie-profile", List.of(11, 13));
    }

    private static WritableRegistration writable(ProfileServiceInfoProjection projection) {
        return new WritableRegistration(BOOKIE, projection.storageIncarnation(),
                projection.readinessGeneration(), projection.persistentStoreVersion());
    }

    @Test
    public void orderedPublicationHasSessionOwnershipAndDoesNotChangePersistentReadiness() throws Exception {
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        ProfileServiceInfoProjection absent = projection(new VersionedRecord(0, record(1)));
        assertEquals(PublicationStatus.CONFLICT, publisher.publishServiceInfo(absent).status());
        assertTrue(first.getChildren(ROOT, false).isEmpty());
        VersionedRecord initial = create();
        ProfileServiceInfoProjection projection = projection(initial);
        assertEquals(PublicationStatus.CONFLICT, publisher.registerWritable(writable(projection)).status());
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        assertEquals(projection, publisher.readServiceInfo(BOOKIE).orElseThrow());
        assertTrue(publisher.readWritable(BOOKIE).isEmpty());
        assertEquals(first.getSessionId(), second.exists(path() + "/service-info", false).getEphemeralOwner());
        assertEquals(PublicationStatus.APPLIED, publisher.registerWritable(writable(projection)).status());
        assertEquals(writable(projection), publisher.readWritable(BOOKIE).orElseThrow());
        assertEquals(first.getSessionId(), second.exists(path() + "/writable", false).getEphemeralOwner());
        assertEquals(1, second.exists(path() + "/service-info", false).getVersion());
        assertEquals(PublicationStatus.ALREADY_APPLIED, publisher.publishServiceInfo(projection).status());
        assertEquals(PublicationStatus.ALREADY_APPLIED, publisher.registerWritable(writable(projection)).status());
        assertEquals(1, second.exists(path() + "/service-info", false).getVersion());
        assertEquals(initial, persistent(second).read(BOOKIE).versionedRecord().orElseThrow());
        String marker = first.getChildren(path(), false).stream().filter(n -> n.startsWith("session-"))
                .findFirst().orElseThrow();
        assertThrows(KeeperException.NoAuthException.class,
                () -> first.setData(path() + "/" + marker, new byte[0], 0));
        publisher.demote(BOOKIE);
        assertEquals(1, second.getChildren(path(), false).size());
        assertEquals(initial, persistent(second).read(BOOKIE).versionedRecord().orElseThrow());
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.publishServiceInfo(projection).status());
        assertThrows(IllegalStateException.class, () -> publisher.readWritable(BOOKIE));
    }

    @Test
    public void lostPublishRepliesReconcileAndOldDemotionCannotDeleteNewNonce() throws Exception {
        ProfileServiceInfoProjection projection = projection(create());
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        first.loseReply.set(true);
        assertEquals(PublicationStatus.RESPONSE_LOST, publisher.publishServiceInfo(projection).status());
        assertEquals(projection, publisher.readServiceInfo(BOOKIE).orElseThrow());
        first.loseReply.set(true);
        assertEquals(PublicationStatus.RESPONSE_LOST, publisher.registerWritable(writable(projection)).status());
        assertEquals(writable(projection), publisher.readWritable(BOOKIE).orElseThrow());
        assertEquals(PublicationStatus.ALREADY_APPLIED, publisher.registerWritable(writable(projection)).status());
        first.loseReply.set(true);
        assertThrows(IllegalStateException.class, () -> publisher.demote(BOOKIE));
        List<org.apache.zookeeper.Op> oldDemotion = first.lastOperations.get();
        assertEquals(1, second.getChildren(path(), false).size());
        ZKProfileEphemeralRegistrationStore replacement = publisher(first);
        assertEquals(PublicationStatus.APPLIED, replacement.publishServiceInfo(projection).status());
        assertEquals(PublicationStatus.APPLIED, replacement.registerWritable(writable(projection)).status());
        assertThrows(KeeperException.NoNodeException.class, () -> second.multi(oldDemotion));
        assertEquals(writable(projection), replacement.readWritable(BOOKIE).orElseThrow());
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.registerWritable(writable(projection)).status());
    }

    @Test
    public void persistentChangeBeforeServiceTransactionRejectsAllEphemeralCreates() throws Exception {
        VersionedRecord initial = create();
        ProfileServiceInfoProjection projection = projection(initial);
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        first.beforeMulti.set(() -> assertEquals(CasStatus.APPLIED,
                persistent(second).compareAndSet(BOOKIE, initial.storeVersion(), 1, record(2)).status()));
        assertEquals(PublicationStatus.CONFLICT, publisher.publishServiceInfo(projection).status());
        assertEquals(1, second.getChildren(path(), false).size());
        ProfileServiceInfoProjection next = projection(persistent(second).read(BOOKIE).versionedRecord().orElseThrow());
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(next).status());
        assertEquals(PublicationStatus.APPLIED, publisher.registerWritable(writable(next)).status());
    }

    @Test
    public void persistentChangeBeforeWritableTransactionCannotPublishOldReadiness() throws Exception {
        VersionedRecord initial = create();
        ProfileServiceInfoProjection projection = projection(initial);
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        first.beforeMulti.set(() -> assertEquals(CasStatus.APPLIED,
                persistent(second).compareAndSet(BOOKIE, initial.storeVersion(), 1, record(2)).status()));
        assertEquals(PublicationStatus.CONFLICT, publisher.registerWritable(writable(projection)).status());
        assertNull(second.exists(path() + "/writable", false));
        publisher.demote(BOOKIE);
        assertEquals(1, second.getChildren(path(), false).size());
    }

    @Test
    public void serviceRefreshAndWritableCreationCannotCrossInEitherOrder() throws Exception {
        ProfileServiceInfoProjection projection = projection(create());
        ProfileServiceInfoProjection refresh = new ProfileServiceInfoProjection(BOOKIE, projection.storageIncarnation(),
                projection.readinessGeneration(), projection.persistentStoreVersion(), "other-profile-endpoint",
                projection.capabilityHints());
        for (boolean refreshFirst : List.of(true, false)) {
            ZKProfileEphemeralRegistrationStore publisher = publisher(first);
            assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
            if (refreshFirst) {
                first.beforeMulti.set(() -> assertEquals(PublicationStatus.APPLIED,
                        publisher.publishServiceInfo(refresh).status()));
                assertEquals(PublicationStatus.CONFLICT, publisher.registerWritable(writable(projection)).status());
                assertTrue(publisher.readWritable(BOOKIE).isEmpty());
                assertEquals(refresh, publisher.readServiceInfo(BOOKIE).orElseThrow());
            } else {
                first.beforeMulti.set(() -> assertEquals(PublicationStatus.APPLIED,
                        publisher.registerWritable(writable(projection)).status()));
                assertEquals(PublicationStatus.CONFLICT, publisher.publishServiceInfo(refresh).status());
                assertEquals(writable(projection), publisher.readWritable(BOOKIE).orElseThrow());
                assertEquals(projection, publisher.readServiceInfo(BOOKIE).orElseThrow());
            }
            publisher.demote(BOOKIE);
        }
    }

    @Test
    public void foreignSessionCannotReconcileOrDemoteEvenIdenticalRecords() {
        ProfileServiceInfoProjection projection = projection(create());
        ZKProfileEphemeralRegistrationStore owner = publisher(first);
        ZKProfileEphemeralRegistrationStore foreign = publisher(second);
        assertEquals(PublicationStatus.APPLIED, owner.publishServiceInfo(projection).status());
        assertEquals(PublicationStatus.APPLIED, owner.registerWritable(writable(projection)).status());
        assertEquals(PublicationStatus.UNAVAILABLE, foreign.publishServiceInfo(projection).status());
        assertThrows(IllegalStateException.class, () -> foreign.readServiceInfo(BOOKIE));
        assertThrows(IllegalStateException.class, () -> foreign.readWritable(BOOKIE));
        assertThrows(IllegalStateException.class, () -> foreign.demote(BOOKIE));
        assertEquals(writable(projection), owner.readWritable(BOOKIE).orElseThrow());
    }

    @Test
    public void actualServerSessionExpiryRemovesAllEphemeralsAndKeepsPersistentRecord() throws Exception {
        VersionedRecord initial = create();
        ProfileServiceInfoProjection projection = projection(initial);
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        assertEquals(PublicationStatus.APPLIED, publisher.registerWritable(writable(projection)).status());
        cluster.expire(first.getSessionId());
        assertTrue(first.expired.await(15, TimeUnit.SECONDS));
        assertEquals(1, second.getChildren(path(), false).size());
        assertEquals(initial, persistent(second).read(BOOKIE).versionedRecord().orElseThrow());
        assertThrows(IllegalStateException.class, () -> publisher.readWritable(BOOKIE));
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.registerWritable(writable(projection)).status());
        ZKProfileEphemeralRegistrationStore next = publisher(second);
        assertEquals(PublicationStatus.APPLIED, next.publishServiceInfo(projection).status());
        assertEquals(PublicationStatus.APPLIED, next.registerWritable(writable(projection)).status());
        assertEquals(writable(projection), next.readWritable(BOOKIE).orElseThrow());
    }

    @Test
    public void actualSessionExpiryBeforeSuccessCallbackCannotReportApplied() throws Exception {
        ProfileServiceInfoProjection projection = projection(create());
        ZKProfileEphemeralRegistrationStore publisher = new ZKProfileEphemeralRegistrationStore(first, ROOT,
                BOOKIE, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofMinutes(1));
        first.holdReply.set(true);
        CompletableFuture<PublicationStatus> reply = CompletableFuture.supplyAsync(
                () -> publisher.publishServiceInfo(projection).status());
        assertTrue(first.replyHeld.await(10, TimeUnit.SECONDS));
        assertNotNull(second.exists(path() + "/service-info", false));
        cluster.expire(first.getSessionId());
        assertTrue(first.expired.await(15, TimeUnit.SECONDS));
        assertNull(second.exists(path() + "/service-info", false));
        first.lateReply.getAndSet(null).run();
        assertEquals(PublicationStatus.RESPONSE_LOST, reply.get(10, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> publisher.readWritable(BOOKIE));
        assertEquals(1, second.getChildren(path(), false).size());
    }

    @Test
    public void committedWritableWithHeldReplyRemainsUnknownUntilFreshRead() throws Exception {
        ProfileServiceInfoProjection projection = projection(create());
        ZKProfileEphemeralRegistrationStore publisher = new ZKProfileEphemeralRegistrationStore(first, ROOT,
                BOOKIE, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(2));
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        first.holdReply.set(true);
        assertEquals(PublicationStatus.RESPONSE_LOST, publisher.registerWritable(writable(projection)).status());
        assertNotNull(second.exists(path() + "/writable", false));
        assertEquals(writable(projection), publisher.readWritable(BOOKIE).orElseThrow());
        first.lateReply.getAndSet(null).run();
        assertEquals(PublicationStatus.ALREADY_APPLIED, publisher.registerWritable(writable(projection)).status());
        assertEquals(1, second.exists(path() + "/service-info", false).getVersion());
    }

    @Test
    public void codecIsStrictAndCorruptionCannotBeReadAsAbsent() throws Exception {
        ProfileServiceInfoProjection projection = projection(create());
        ProfileEphemeralCodec.Envelope envelope = new ProfileEphemeralCodec.Envelope(new UUID(1, 2),
                first.getSessionId(), projection);
        byte[] data = ProfileEphemeralCodec.encode(envelope);
        assertEquals(envelope, ProfileEphemeralCodec.decode(data));
        assertEquals(80 + BOOKIE.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + projection.endpoint().length() + 8, data.length);
        byte[] schema = data.clone();
        ByteBuffer.wrap(schema).putInt(4, 2);
        checksum(schema);
        assertThrows(IllegalArgumentException.class, () -> ProfileEphemeralCodec.decode(schema));
        byte[] count = data.clone();
        ByteBuffer.wrap(count).putInt(count.length - 16, Integer.MAX_VALUE);
        checksum(count);
        assertThrows(IllegalArgumentException.class, () -> ProfileEphemeralCodec.decode(count));
        byte[] trailing = Arrays.copyOf(data, data.length + 4);
        checksum(trailing);
        assertThrows(IllegalArgumentException.class, () -> ProfileEphemeralCodec.decode(trailing));
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        byte[] corrupt = second.getData(path() + "/service-info", false, new Stat());
        corrupt[0] ^= 1;
        second.setData(path() + "/service-info", corrupt, 0);
        assertThrows(IllegalArgumentException.class, () -> publisher.readServiceInfo(BOOKIE));
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.registerWritable(writable(projection)).status());
        assertThrows(IllegalArgumentException.class, () -> publisher.readServiceInfo("wrong-scope"));
    }

    @Test
    public void realParentAclDenialLeavesNoPartialPublication() throws Exception {
        VersionedRecord initial = create();
        ProfileServiceInfoProjection projection = projection(initial);
        first.setACL(path(), Arrays.asList(new org.apache.zookeeper.data.ACL(
                ZooDefs.Perms.READ | ZooDefs.Perms.WRITE, ZooDefs.Ids.ANYONE_ID_UNSAFE)), -1);
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        assertEquals(PublicationStatus.UNAVAILABLE, publisher.publishServiceInfo(projection).status());
        assertEquals(1, second.getChildren(path(), false).size());
        assertTrue(publisher.readServiceInfo(BOOKIE).isEmpty());
        assertTrue(publisher.readWritable(BOOKIE).isEmpty());
        assertEquals(initial, persistent(second).read(BOOKIE).versionedRecord().orElseThrow());
    }

    @Test
    public void referenceStartupUsesActualPersistentAndEphemeralStoresWithoutClaimingLocalRecovery() {
        StartupReadinessInput input = input();
        ZKProfileEphemeralRegistrationStore publisher = publisher(first);
        StartupReadinessHarness harness = new StartupReadinessHarness(new InMemoryLocalReadinessStore(),
                persistent(first), publisher);
        first.loseReply.set(true);
        StartupReadinessResult result = harness.run(input, StartupCrashPoint.NONE);
        assertEquals(StartupReadinessResult.Outcome.WRITABLE, result.outcome());
        assertEquals(List.of(StartupReadinessResult.StartupPhase.values()), result.completedPhases());
        assertTrue(result.metrics().normalAdd().isZero());
        assertEquals(record(1), persistent(second).read(BOOKIE).versionedRecord().orElseThrow().record());
        assertTrue(publisher.readWritable(BOOKIE).isPresent());
        assertEquals(StartupReadinessResult.Outcome.WRITABLE,
                harness.run(input, StartupCrashPoint.NONE).outcome());
    }

    private static StartupReadinessInput input() {
        ManifestIdentity manifest = new ManifestIdentity(bytes(32, 2));
        BookieCompatibilityFact compatibility = new BookieCompatibilityFact(BOOKIE, storage(1),
                EngineProfile.SEGMENT_WAL_ENGINE, 1, 2, 1, List.of(11), manifest, new Generation(5),
                new Generation(4), new Generation(6), 7, 8, BookieCompatibilityFact.MigrationState.FORMAT_READY,
                new Generation(7));
        RequiredDeviceManifest devices = new RequiredDeviceManifest(manifest, new Generation(5), List.of(
                new RequiredDeviceManifest.RequiredDeviceFact("device-a", storage(1), new Generation(5),
                        RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1, new Generation(7), List.of(11))));
        LocalRecoveryFacts recovery = new LocalRecoveryFacts(storage(1), new Generation(4),
                LocalRecoveryFacts.RecoveryStatus.RECOVERED, LocalRecoveryFacts.RecoveryStatus.RECOVERED,
                LocalRecoveryFacts.RecoveryStatus.RECOVERED, new Generation(6));
        NewScopeFallbackManifest fallback = new NewScopeFallbackManifest("old-bookie", BOOKIE, storage(2), storage(1),
                roots("/old"), roots("/new"), new NewScopeFallbackManifest.CredentialScope("old-service"),
                new NewScopeFallbackManifest.CredentialScope("new-service"),
                NewScopeFallbackManifest.OldScopeDisposition.DRAINED, NewScopeFallbackManifest.AccessLevel.NONE,
                NewScopeFallbackManifest.AccessLevel.READ_WRITE);
        return new StartupReadinessInput(compatibility, devices, recovery, fallback,
                new RollbackSafetyFacts(false, false, false, false, false, false, false), new Generation(1),
                new Generation(3), 7, 8, List.of(11, 13), "bookie-profile", List.of(11, 13));
    }

    private static List<NewScopeFallbackManifest.StorageRoot> roots(String prefix) {
        return Arrays.stream(NewScopeFallbackManifest.RootKind.values())
                .map(kind -> new NewScopeFallbackManifest.StorageRoot(kind, prefix + "/" + kind.name()))
                .sorted(java.util.Comparator.comparing(root -> root.kind().name())).toList();
    }

    private static StorageIncarnation storage(int marker) {
        return new StorageIncarnation(bytes(16, marker));
    }

    private static byte[] bytes(int length, int marker) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) marker);
        return bytes;
    }

    private static void checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }
}

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileLifecycleHead.State;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore.CreateStatus;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

/** Real lifecycle backend tests; receipt facts are inputs, the physical all-E chain is tested in Segment storage. */
public class ZKProfileLifecycleStoreTest extends ProfileZKTestBase {
    private static byte[] bytes(int length, int value) {
        byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileControlScope scope(String bookie) {
        byte[] descriptor = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1)
                .put(bytes(32, 9)).array();
        return new ProfileControlScope(ProfileControlOperation.INSTALL, ProfileControlOperation.INSTALL.purpose(),
                44, bytes(16, 1), ProfileDescriptorIdentity.fromBytes(descriptor), bookie, bytes(16, 2), "d", 8,
                bytes(16, 3), bytes(32, 4), null);
    }

    private static ProfileReservation reservation() {
        return new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 2)),
                List.of(scope("b1:3181"), scope("b2:3181"), scope("b3:3181")));
    }

    private static ZKProfileReservationStore store(TestClient client) {
        return new ZKProfileReservationStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private LedgerManager manager(TestClient client) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk+longhierarchical://127.0.0.1:2181" + ROOT + "/ledgers");
        var factory = new LongHierarchicalLedgerManagerFactory();
        factory.initialize(conf, new ZkLayoutManager(client, ROOT + "/ledgers", ZooDefs.Ids.OPEN_ACL_UNSAFE),
                factory.getCurrentVersion());
        return factory.newLedgerManager();
    }

    private static LedgerMetadata metadata() {
        return LedgerMetadataBuilder.create().withId(44).withEnsembleSize(3).withWriteQuorumSize(3)
                .withAckQuorumSize(2).withDigestType(DigestType.CRC32C).withPassword(new byte[0])
                .newEnsembleEntry(0, List.of(BookieId.parse("b1:3181"), BookieId.parse("b2:3181"),
                        BookieId.parse("b3:3181"))).withCustomMetadata(Map.of()).build();
    }

    private record Prepared(ZKProfileReservationStore.Snapshot root, ProfileReadyPage desired,
                            ZKProfileReservationStore.ReadySnapshot page) { }

    private Prepared prepare(LedgerManager lm) throws Exception {
        return prepare(lm, false);
    }

    private Prepared prepare(LedgerManager lm, boolean losePageReply) throws Exception {
        var root = store(first).createIfAbsent(reservation()).snapshot();
        var created = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), reservation().backlink())
                .get(5, TimeUnit.SECONDS);
        List<ProfileInstallReceipt> receipts = reservation().installs().stream()
                .map(scope -> new ProfileInstallReceipt(scope, EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7),
                        1)).toList();
        var facts = new ProfileInitialMetadataCoordinator.Result(
                ProfileInitialMetadataCoordinator.Status.METADATA_CREATED,
                created.metadata(), receipts);
        var desired = ProfileReadyPage.from(root, facts, bytes(16, 4), bytes(32, 5));
        first.loseReply.set(losePageReply);
        var prepared = store(first).prepareReady(root, desired, lm);
        assertEquals(losePageReply ? CreateStatus.ALREADY_APPLIED : CreateStatus.APPLIED, prepared.status());
        return new Prepared(root, desired, prepared.page());
    }

    @Test
    public void deleteDeliveryCodecsHaveFixedBoundsAndIndependentCrc() {
        var target = new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2));
        var head = new ProfileDeleteDelivery.Head(bytes(16, 81), 1, 1, 2, target);
        byte[] encoded = head.encode();
        assertEquals(77, encoded.length);
        var crc = new CRC32C();
        crc.update(encoded, 0, 73);
        assertEquals((int) crc.getValue(), ByteBuffer.wrap(encoded).getInt(73));
        assertEquals(head, ProfileDeleteDelivery.Head.decode(encoded));
        var scope = scope("b1:3181");
        var context = new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(44,
                scope.ledgerInstanceId(), scope.descriptorIdentity().toBytes());
        var event = new ProfileDeleteDelivery.Event(head.streamId(), 1, 1, 2, context, target,
                new ProfileDeleteRecord.Reference(100, bytes(32, 7), bytes(32, 8)), bytes(16, 9), 10);
        byte[] raw = event.encode();
        assertEquals(249, raw.length);
        crc.reset();
        crc.update(raw, 0, raw.length - 4);
        assertEquals((int) crc.getValue(), ByteBuffer.wrap(raw).getInt(raw.length - 4));
        assertEquals(event, ProfileDeleteDelivery.Event.decode(raw));
        byte[] stream = head.streamId();
        stream[0] ^= 1;
        assertArrayEquals(encoded, head.encode());
        for (int count = 0; count < raw.length; count++) {
            byte[] truncated = Arrays.copyOf(raw, count);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteDelivery.Event.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteDelivery.Head(bytes(16, 1), 2, 1, 0, target));
        for (int generation : new int[] {2, 3, ProfileDeleteSnapshot.MAX_GENERATIONS}) {
            var reassigned = new ProfileDeleteDelivery.Head(bytes(16, 1), 1, generation, 0, target);
            assertEquals(reassigned, ProfileDeleteDelivery.Head.decode(reassigned.encode()));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteDelivery.Head(bytes(16, 1), 1, 0, 0, target));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteDelivery.Head(bytes(16, 1), 1,
                        ProfileDeleteSnapshot.MAX_GENERATIONS + 1, 0, target));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteDelivery.Head(bytes(16, 1), 1, 1, 4097, target));
    }

    @Test
    public void versionedDeleteReferenceBindsPublicationBirthWithoutChangingLegacyBytes() {
        var legacy = new ProfileDeleteRecord.Reference(100, bytes(32, 7), bytes(32, 8));
        var versioned = new ProfileDeleteRecord.Reference(100, bytes(32, 7), bytes(32, 8), bytes(16, 9));
        assertEquals(88, legacy.encode().length);
        assertEquals(104, versioned.encode().length);
        assertArrayEquals(legacy.encode(), ProfileDeleteRecord.Reference.decode(legacy.encode()).encode());
        assertArrayEquals(versioned.encode(), ProfileDeleteRecord.Reference.decode(versioned.encode()).encode());
        var prefix = new ProfileDeleteRecord.PreparedDeliveries(versioned, bytes(16, 9), 1088, 1088);
        assertEquals(140, prefix.encode().length);
        assertArrayEquals(prefix.encode(), ProfileDeleteRecord.PreparedDeliveries.decode(prefix.encode()).encode());
        var scope = scope("b1:3181");
        var context = new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(44,
                scope.ledgerInstanceId(), scope.descriptorIdentity().toBytes());
        var target = new ProfileDeleteRecord.Target("x".repeat(255), bytes(16, 2));
        var event = new ProfileDeleteDelivery.Event(bytes(16, 81), 1, 1, 1, context, target, versioned,
                bytes(16, 9), 10);
        assertEquals(513, event.encode().length);
        assertEquals(event, ProfileDeleteDelivery.Event.decode(event.encode()));
        byte[] owned = versioned.publicationId();
        owned[0] ^= 1;
        assertArrayEquals(bytes(16, 9), versioned.publicationId());
        for (byte[] raw : List.of(versioned.encode(), prefix.encode(), event.encode())) {
            for (int size = 0; size < raw.length; size++) {
                byte[] truncated = Arrays.copyOf(raw, size);
                if (raw.length == 104) {
                    assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRecord.Reference.decode(truncated));
                } else if (raw.length == 140) {
                    assertThrows(IllegalArgumentException.class,
                            () -> ProfileDeleteRecord.PreparedDeliveries.decode(truncated));
                } else {
                    assertThrows(IllegalArgumentException.class, () -> ProfileDeleteDelivery.Event.decode(truncated));
                }
            }
        }
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRecord.PreparedDeliveries(
                versioned, bytes(16, 9), 1089, 0));
        byte[] changed = versioned.encode();
        changed[90] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRecord.Reference.decode(changed));
    }

    private record Replaced(ZKProfileReservationStore.Snapshot source, ProfileReplacementPage desired,
                            ZKProfileReservationStore.ReplacementSnapshot candidate) { }

    private ProfileLedgerMetadataStore membership(TestClient client, LedgerManager manager) {
        return new ProfileLedgerMetadataStore(client, (AbstractZkLedgerManager) manager,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private ZKProfileReservationStore.Snapshot ready(LedgerManager manager) throws Exception {
        var prepared = prepare(manager);
        return store(first).publishReady(prepared.root(), prepared.page(), manager).snapshot();
    }

    private Replaced replacement(LedgerManager manager, String target, int nonce) {
        var lifecycle = store(first);
        var source = lifecycle.read(44).snapshot();
        var initial = lifecycle.readPublishedPage(44).page();
        var metadata = membership(first, manager);
        var before = metadata.readMembership(44).snapshot();
        var old = before.metadata().getValue().getAllEnsembles().lastEntry().getValue();
        var next = List.of(old.get(0), old.get(1), BookieId.parse(target));
        var installed = new ProfileControlScope(ProfileControlOperation.INSTALL,
                ProfileControlOperation.INSTALL.purpose(), 44, bytes(16, 1), scope(target).descriptorIdentity(),
                target, bytes(16, nonce), "d", 8, bytes(16, nonce), bytes(32, nonce), null);
        var installation = ProfileReplacementInstallPage.from(source, before, 100, next, List.of(installed));
        assertEquals(CreateStatus.APPLIED,
                lifecycle.prepareReplacementInstall(source, installation, metadata).status());
        var changed = metadata.replaceEnsemble(before, 100, next);
        assertEquals(ProfileLedgerMetadataStore.Outcome.APPLIED, changed.outcome());
        var page = ProfileReplacementPage.from(source, initial,
                new ProfileLedgerMetadataStore.MembershipSnapshot(before.birthZxid(), changed.metadata()), 100,
                List.of(new ProfileInstallReceipt(installed, EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1)),
                bytes(16, nonce + 80), bytes(32, nonce + 80));
        var prepared = lifecycle.prepareReplacement(source, page, metadata);
        assertEquals(CreateStatus.APPLIED, prepared.status());
        return new Replaced(source, page, prepared.page());
    }

    private ProfileRepairIntent repairIntent(ZKProfileReservationStore.RecoveryResult entered, int slot, int id) {
        var template = scope("b4:3181");
        var install = new ProfileControlScope(template.operation(), template.purpose(), template.ledgerId(),
                template.ledgerInstanceId(), template.descriptorIdentity(), template.targetBookie(),
                template.targetStorageIncarnation(), template.authorityDomain(), entered.context().controlGeneration(),
                bytes(16, 20 + id), template.publicSemanticPayloadIdentity(), null);
        return new ProfileRepairIntent(entered.context(), slot, bytes(16, id), 0, 1,
                ZKProfileReservationStore.repairEnsembleDigest(entered.metadata().metadata().getValue(), 0),
                install, bytes(16, 40 + id), new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 4));
    }

    private ProfileRepairIntent historicalRepair(ZKProfileReservationStore.RecoveryResult entered, int slot,
            int member, String target, int id) {
        var template = scope(target);
        var install = new ProfileControlScope(template.operation(), template.purpose(), 44,
                template.ledgerInstanceId(), template.descriptorIdentity(), target,
                template.targetStorageIncarnation(), template.authorityDomain(), entered.context().controlGeneration(),
                bytes(16, id), bytes(32, id), null);
        return new ProfileRepairIntent(entered.context(), slot, bytes(16, id + 1), 0, member,
                ZKProfileReservationStore.repairEnsembleDigest(entered.metadata().metadata().getValue(), 0),
                install, bytes(16, id + 2), new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 99));
    }

    @Test
    public void historicalRepairCommitAtomicallyPublishesMembershipAndCloseAuthorityAndAdoptsSameContext()
            throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var replacement = replacement(manager, "b4:3181", 10);
            var metadata = membership(first, manager);
            assertEquals(CreateStatus.APPLIED, store(first).publishReplacement(replacement.source(),
                    replacement.candidate(), metadata).status());
            var lifecycle = store(first);
            var plan = lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan();
            var entered = lifecycle.beginRecovery(metadata, plan);
            var current = entered;
            for (int slot = 0; slot < 2; slot++) {
                var intent = historicalRepair(current, slot, 1 - slot, "b" + (slot + 5) + ":3181", 60 + slot * 5);
                var prepared = lifecycle.prepareRepair(metadata, intent);
                var receipt = new ProfileInstallReceipt(intent.install(), EngineProfile.SEGMENT_WAL_ENGINE,
                        bytes(32, 7), 1);
                assertEquals(CreateStatus.APPLIED,
                        lifecycle.admitRepair(metadata, prepared.snapshot(), receipt).status());
                var closeId = bytes(16, 80 + slot);
                var closeScope = intent.closeScope(closeId, intent.grantGeneration() + slot + 1);
                var reader = lifecycle.repairAuthorityReader(intent.install().targetBookie(), metadata);
                assertEquals(ReadStatus.NOT_FOUND, reader.read(ProfileAuthorityKey.fromScope(closeScope)).status());
                first.loseReply.set(true);
                var committed = lifecycle.commitRepair(metadata, prepared.snapshot(), current.metadata(), closeId);
                assertEquals(CreateStatus.ALREADY_APPLIED, committed.status());
                assertEquals(closeScope, committed.closeScope());
                assertEquals(intent.install().targetBookie(), committed.metadata().metadata().getValue()
                        .getAllEnsembles().get(0L).get(intent.replacedMemberSlot()).toString());
                assertEquals(CreateStatus.ALREADY_APPLIED, lifecycle.commitRepair(
                        metadata, prepared.snapshot(), current.metadata(), closeId).status());
                assertEquals(CreateStatus.CONFLICT, lifecycle.commitRepair(
                        metadata, prepared.snapshot(), current.metadata(), bytes(16, 90 + slot)).status());
                assertEquals(ReadStatus.FOUND, reader.read(ProfileAuthorityKey.fromScope(closeScope)).status());
                assertEquals(ReadStatus.NOT_FOUND,
                        reader.read(ProfileAuthorityKey.fromScope(intent.grantScope())).status());
                current = store(second).readRecovery(membership(second, manager), 44);
                assertEquals(entered.context(), current.context());
                assertEquals(entered.metadata().birthZxid(), current.metadata().birthZxid());
            }
            assertEquals(6, lifecycle.retainedActivationTargets(44).targets().size());
            assertEquals(2, ProfileRepairCommit.head(current.metadata().metadata().getValue().getCustomMetadata()
                    .get(ProfileRepairCommit.KEY)).size());
            first.delete(ROOT + "/2c/repair-0/committed", 0);
            assertEquals(ProfileLedgerMetadataStore.Outcome.QUARANTINED,
                    lifecycle.readRecovery(metadata, 44).outcome());
            assertEquals(6, lifecycle.retainedActivationTargets(44).targets().size());
            assertEquals(CreateStatus.APPLIED, lifecycle.cutDelete(lifecycle.read(44).snapshot(),
                    bytes(16, 100), bytes(32, 100)).status());
            assertEquals(6, lifecycle.retainedActivationTargets(44).targets().size());
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT, lifecycle.readRecovery(metadata, 44).outcome());
        }
    }

    @Test
    public void historicalRepairDeleteBeforePublicationLeavesMembershipUnchanged() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var replacement = replacement(manager, "b4:3181", 10);
            var metadata = membership(first, manager);
            var lifecycle = store(first);
            lifecycle.publishReplacement(replacement.source(), replacement.candidate(), metadata);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var intent = historicalRepair(entered, 0, 1, "b5:3181", 60);
            var prepared = lifecycle.prepareRepair(metadata, intent).snapshot();
            lifecycle.admitRepair(metadata, prepared, new ProfileInstallReceipt(intent.install(),
                    EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1));
            first.beforeMulti.set(() -> assertEquals(CreateStatus.APPLIED,
                    store(second).cutDelete(store(second).read(44).snapshot(),
                            bytes(16, 100), bytes(32, 100)).status()));
            assertEquals(CreateStatus.CONFLICT,
                    lifecycle.commitRepair(metadata, prepared, entered.metadata(), bytes(16, 80)).status());
            assertEquals(entered.metadata().metadata().getVersion(), metadata.readMembership(44)
                    .snapshot().metadata().getVersion());
            assertEquals(5, lifecycle.retainedActivationTargets(44).targets().size());
        }
    }

    @Test
    public void historicalRepairRecreatedSameIntentUuidCannotPassTheOldPublicationCut() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var replacement = replacement(manager, "b4:3181", 10);
            var metadata = membership(first, manager);
            var lifecycle = store(first);
            lifecycle.publishReplacement(replacement.source(), replacement.candidate(), metadata);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var intent = historicalRepair(entered, 0, 1, "b5:3181", 60);
            var prepared = lifecycle.prepareRepair(metadata, intent).snapshot();
            var receipt = new ProfileInstallReceipt(intent.install(), EngineProfile.SEGMENT_WAL_ENGINE,
                    bytes(32, 7), 1);
            lifecycle.admitRepair(metadata, prepared, receipt);
            String path = ROOT + "/2c/repair-0";
            byte[] original = second.getData(path, false, null);
            first.beforeMulti.set(() -> {
                try {
                    removeTree(path);
                    second.multi(List.of(Op.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                            Op.create(path + "/birth-" + prepared.birth(), new byte[0],
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)),
                            (code, ignored, context, results) -> assertEquals(0, code), null);
                    var recreated = store(second).prepareRepair(membership(second, manager), intent).snapshot();
                    assertEquals(prepared.birth(), recreated.birth());
                    assertTrue(prepared.storeVersion() != recreated.storeVersion());
                    assertEquals(CreateStatus.APPLIED,
                            store(second).admitRepair(membership(second, manager), recreated, receipt).status());
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            });
            assertEquals(CreateStatus.CONFLICT,
                    lifecycle.commitRepair(metadata, prepared, entered.metadata(), bytes(16, 80)).status());
            assertEquals(entered.metadata().metadata().getVersion(),
                    metadata.readMembership(44).snapshot().metadata().getVersion());
            assertEquals(null, second.exists(path + "/committed", false));
        }
    }

    @Test
    public void repairCommitVectorsRejectMalformedAndOversizeProofs() throws Exception {
        byte[] vector = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-commit.bin").toURI()));
        byte[] head = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-commit-head.bin").toURI()));
        assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryContext.decode(vector));
        byte[] recovery = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/recovery-context.bin").toURI()));
        assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.decode(recovery));
        var commit = ProfileRepairCommit.decode(vector);
        assertEquals(2, commit.beforeVersion());
        assertEquals("b2:3181", commit.oldBookie());
        assertArrayEquals(vector, commit.encode());
        var references = ProfileRepairCommit.head(head);
        assertEquals(new UUID(0, 7), references.get(0).intentBirth());
        assertEquals(11, references.get(0).intentStoreVersion());
        assertArrayEquals(head, ProfileRepairCommit.head(references));
        commit.beforeDigest()[0] = 0;
        references.get(0).digest()[0] = 0;
        assertArrayEquals(vector, commit.encode());
        assertArrayEquals(head, ProfileRepairCommit.head(references));
        for (int offset : List.of(0, 4, 8, 100)) {
            byte[] bad = vector.clone();
            ByteBuffer.wrap(bad).putInt(offset, -1);
            checksum(bad);
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.decode(bad));
        }
        for (int count : List.of(0, 33)) {
            byte[] bad = head.clone();
            ByteBuffer.wrap(bad).putInt(8, count);
            checksum(bad);
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.head(bad));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.head(
                List.of(references.get(0), references.get(0))));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileRepairCommit.decode(Arrays.copyOf(vector, ProfileRepairCommit.MAX_BYTES + 1)));
    }

    private static ProfileRecoveryCoordinator.PrefixResult prefixProof(
            ZKProfileReservationStore.RecoveryResult entered, long p, long length) {
        return new ProfileRecoveryCoordinator.PrefixResult(
                ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL,
                ProfileRecoveryCoordinator.PrefixReason.NORMAL_TAIL, entered.context(), entered.metadata(),
                Math.min(1, p), p, length, p + 1, List.of());
    }

    @Test
    public void closeHistoryReferenceMatchesIndependentVectorAndOwnsBytes() throws Exception {
        byte[] prefix = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(getClass().getResource(
                "/unified-wal-segment/recovery-prefix.bin").toURI()));
        byte[] fixed = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(getClass().getResource(
                "/unified-wal-segment/recovery-close-cut.bin").toURI()));
        var boundary = ProfileRecoveryBoundary.decode(prefix);
        var decoded = ProfileRecoveryBoundary.CloseReference.decode(fixed);
        assertEquals(80, fixed.length);
        assertEquals(0, decoded.headCount());
        assertEquals(2, decoded.prefixEntryId());
        assertEquals(48, decoded.prefixLength());
        assertTrue(decoded.matchesContext(boundary.context()));
        assertArrayEquals(fixed, decoded.encode());
        byte[] borrowed = decoded.contextDigest();
        borrowed[0] ^= 1;
        assertArrayEquals(fixed, decoded.encode());
        for (int count = 0; count < fixed.length; count++) {
            byte[] cut = java.util.Arrays.copyOf(fixed, count);
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.CloseReference.decode(cut));
        }
        byte[] corrupt = fixed.clone();
        corrupt[72] = 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.CloseReference.decode(corrupt));
        assertThrows(IllegalArgumentException.class, () -> new ProfileRecoveryBoundary.CloseReference(
                Integer.MAX_VALUE, 0, 1, 2, 48, new byte[32]));
        assertThrows(IllegalArgumentException.class, () -> new ProfileRecoveryBoundary.CloseReference(
                1, 33, 1, 2, 48, new byte[32]));
        assertThrows(IllegalArgumentException.class, () -> new ProfileRecoveryBoundary.CloseReference(
                1, 0, 1, -1, 48, new byte[32]));
    }

    @Test
    public void prefixSelectionVectorsOwnBytesAndRejectAliasTruncationAndBadBounds() throws Exception {
        byte[] vector = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(getClass().getResource(
                "/unified-wal-segment/recovery-prefix.bin").toURI()));
        byte[] reference = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(getClass().getResource(
                "/unified-wal-segment/recovery-prefix-reference.bin").toURI()));
        var decoded = ProfileRecoveryBoundary.decode(vector);
        assertEquals(2, decoded.prefixEntryId());
        assertEquals(48, decoded.prefixLength());
        assertArrayEquals(vector, decoded.encode());
        var ref = ProfileRecoveryBoundary.reference(reference);
        assertEquals(999, ref.birthZxid());
        assertArrayEquals(reference, ref.encode());
        decoded.metadataDigest()[0] = 0;
        ref.digest()[0] = 0;
        assertArrayEquals(vector, decoded.encode());
        assertArrayEquals(reference, ref.encode());
        for (int size = 0; size < vector.length; size++) {
            byte[] shortVector = Arrays.copyOf(vector, size);
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.decode(shortVector));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryContext.decode(vector));
        assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.decode(reference));
        for (int offset : List.of(0, 4, 192, 228, 236, 244)) {
            byte[] bad = vector.clone();
            ByteBuffer.wrap(bad).putInt(offset, -1);
            checksum(bad);
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.decode(bad));
        }
        for (int offset : List.of(0, 4, 8, 12)) {
            byte[] bad = reference.clone();
            ByteBuffer.wrap(bad).putInt(offset, -1);
            checksum(bad);
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.reference(bad));
        }
    }

    @Test
    public void ordinaryWriterCutHasIndependentFixedBytesAndStrictBounds() {
        for (String hex : java.util.List.of(
                "55574e4300010000ffffffffffffffff000000000000000000000000f8ee81ee",
                "55574e4300010000000000000000000000000000000000100000000089792ce9")) {
            byte[] vector = java.util.HexFormat.of().parseHex(hex);
            var raw = java.nio.ByteBuffer.wrap(vector);
            var crc = new java.util.zip.CRC32C();
            crc.update(vector, 0, 28);
            assertEquals((int) crc.getValue(), raw.getInt(28));
            var decoded = ProfileRecoveryBoundary.WriterClose.decode(vector);
            assertEquals(raw.getLong(8), decoded.prefixEntryId());
            assertEquals(raw.getLong(16), decoded.prefixLength());
            assertArrayEquals(vector, decoded.encode());
            for (int bytes = 0; bytes < vector.length; bytes++) {
                byte[] truncated = java.util.Arrays.copyOf(vector, bytes);
                assertThrows(IllegalArgumentException.class,
                        () -> ProfileRecoveryBoundary.WriterClose.decode(truncated));
            }
            byte[] extra = java.util.Arrays.copyOf(vector, 33);
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.WriterClose.decode(extra));
            byte[] corrupt = vector.clone();
            corrupt[12] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.WriterClose.decode(corrupt));
            byte[] reserved = vector.clone();
            reserved[27] = 1;
            crc.reset();
            crc.update(reserved, 0, 28);
            java.nio.ByteBuffer.wrap(reserved).putInt(28, (int) crc.getValue());
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryBoundary.WriterClose.decode(reserved));
        }
        assertThrows(IllegalArgumentException.class, () -> new ProfileRecoveryBoundary.WriterClose(-1, 16));
        assertThrows(IllegalArgumentException.class, () -> new ProfileRecoveryBoundary.WriterClose(Long.MAX_VALUE, 0));
    }

    @Test
    public void writerCloseContextHasItsOwnSchemaAndPreservesTheLegacyVector() throws Exception {
        byte[] legacy = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(getClass().getResource(
                "/unified-wal-segment/recovery-context.bin").toURI()));
        var old = ProfileRecoveryContext.decode(legacy);
        assertFalse(old.writerClose());
        assertArrayEquals(legacy, old.encode());
        var writer = new ProfileRecoveryContext(old.ledger(), old.operationId(), old.metadataBirth(),
                old.predecessorVersion(), old.creatorToken(), old.predecessorDigest(), old.lifecycleBirthZxid(),
                old.lifecycleBirth(), old.lifecycleSequence(), old.lifecycleVersion(), old.controlGeneration(), true);
        byte[] vector = legacy.clone();
        java.nio.ByteBuffer.wrap(vector).putShort(4, (short) 2);
        var crc = new java.util.zip.CRC32C();
        crc.update(vector, 0, vector.length - 4);
        java.nio.ByteBuffer.wrap(vector).putInt(vector.length - 4, (int) crc.getValue());
        assertArrayEquals(vector, writer.encode());
        assertEquals(184, vector.length);
        assertTrue(ProfileRecoveryContext.decode(vector).writerClose());
        // The locked pre-writer-close source requires schema1; schema2 cannot pass that guard.
        assertTrue(java.nio.ByteBuffer.wrap(vector).getShort(4) != 1);
        vector[5] = 3;
        crc.reset();
        crc.update(vector, 0, vector.length - 4);
        java.nio.ByteBuffer.wrap(vector).putInt(vector.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryContext.decode(vector));
    }

    @Test
    public void deleteSealAndManifestHaveBoundedIndependentBytesAndCanonicalTargets() {
        byte[] vector = HexFormat.of().parseHex("555744530001000051515151515151515151515151515151"
                + "000000000000000900000006000000000000006400000000000000c8"
                + "09090909090909090909090909090909090909090909090909090909090909098b0d9a6d");
        var seal = ProfileDeleteRecord.Seal.decode(vector);
        assertEquals(9, seal.epoch());
        assertEquals(6, seal.beforeVersion());
        assertEquals(100, seal.metadataBirth());
        assertEquals(200, seal.lifecycleBirth());
        assertArrayEquals(vector, seal.encode());
        var crc = new java.util.zip.CRC32C();
        crc.update(vector, 0, 84);
        assertEquals((int) crc.getValue(), ByteBuffer.wrap(vector).getInt(84));
        var scope = scope("b1:3181");
        var context = new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(44,
                scope.ledgerInstanceId(), scope.descriptorIdentity().toBytes());
        var first = new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2));
        var second = new ProfileDeleteRecord.Target("b2:3181", bytes(16, 3));
        var manifest = new ProfileDeleteRecord.Manifest(context, seal, List.of(first, second));
        byte[] encoded = manifest.encode();
        assertEquals(164 + 2 * (18 + 7), encoded.length);
        crc.reset();
        crc.update(encoded, 0, encoded.length - 4);
        assertEquals((int) crc.getValue(), ByteBuffer.wrap(encoded).getInt(encoded.length - 4));
        assertEquals(manifest, ProfileDeleteRecord.Manifest.decode(encoded));
        byte[] incarnation = first.incarnation();
        incarnation[0] ^= 1;
        assertArrayEquals(encoded, manifest.encode());
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRecord.Manifest(
                context, seal, List.of(second, first)));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRecord.Manifest(
                context, seal, List.of(first, first)));
        for (int count = 0; count < encoded.length; count++) {
            byte[] truncated = Arrays.copyOf(encoded, count);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRecord.Manifest.decode(truncated));
        }
        byte[] excessive = encoded.clone();
        ByteBuffer.wrap(excessive).putInt(156, Integer.MAX_VALUE);
        crc.reset();
        crc.update(excessive, 0, excessive.length - 4);
        ByteBuffer.wrap(excessive).putInt(excessive.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRecord.Manifest.decode(excessive));
        var reference = new ProfileDeleteRecord.Reference(1000, bytes(32, 7), bytes(32, 8));
        assertArrayEquals(reference.encode(), ProfileDeleteRecord.Reference.decode(reference.encode()).encode());
    }

    @Test
    public void selectedPrefixFixesOriginAndAllowsLastFragmentCommitAndContextRestart() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var lifecycle = store(first);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var proof = prefixProof(entered, 2, 48);
            first.loseSetDataMultiReply.set(true);
            var chosen = lifecycle.selectPrefix(metadata, proof);
            assertEquals(CreateStatus.ALREADY_APPLIED, chosen.status());
            var current = lifecycle.readRecovery(metadata, 44);
            assertEquals(entered.context(), current.context());
            assertEquals(((org.apache.bookkeeper.versioning.LongVersion) entered.metadata().metadata().getVersion())
                    .getLongVersion() + 1, ((org.apache.bookkeeper.versioning.LongVersion)
                        current.metadata().metadata().getVersion()).getLongVersion());
            assertEquals(chosen.snapshot(), store(second).readSelectedPrefix(membership(second, lm),
                    entered.context()).snapshot());
            assertEquals(CreateStatus.ALREADY_APPLIED, lifecycle.selectPrefix(metadata, proof).status());
            assertEquals(CreateStatus.CONFLICT, lifecycle.selectPrefix(metadata, prefixProof(entered, 1, 32)).status());
            var base = repairIntent(current, 0, 60);
            var intent = new ProfileRepairIntent(base.recovery(), base.slot(), base.intentId(), base.fragmentStart(),
                    base.replacedMemberSlot(), base.oldEnsembleDigest(), base.install(), base.grantOperationId(),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
            var prepared = lifecycle.prepareRepair(metadata, intent).snapshot();
            lifecycle.admitRepair(metadata, prepared, new ProfileInstallReceipt(intent.install(),
                    EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1));
            var committed = lifecycle.commitRepair(metadata, prepared, current.metadata(), bytes(16, 80));
            assertEquals(CreateStatus.APPLIED, committed.status());
            assertEquals(entered.context(), store(second).readRecovery(membership(second, lm), 44).context());
            assertEquals(chosen.snapshot(), lifecycle.readSelectedPrefix(metadata, entered.context()).snapshot());
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    lifecycle.commitRepair(metadata, prepared, current.metadata(), bytes(16, 80)).status());
            assertEquals(LedgerMetadata.State.IN_RECOVERY, lm.readLedgerMetadata(44).get().getValue().getState());
        }
    }

    @Test
    public void selectedPrefixNodeRecreationCannotReuseItsIdenticalBytesOrVersionZero() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var lifecycle = store(first);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var chosen = lifecycle.selectPrefix(metadata, prefixProof(entered, 2, 48));
            assertEquals(CreateStatus.APPLIED, chosen.status());
            String path = ROOT + "/2c/recovery-prefix";
            byte[] exact = second.getData(path, false, null);
            removeRecoveryTree(path);
            second.create(path, exact, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals(0, second.exists(path, false).getVersion());
            assertEquals(ProfileLedgerMetadataStore.Outcome.QUARANTINED,
                    lifecycle.readRecovery(metadata, 44).outcome());
            assertEquals(CreateStatus.QUARANTINED,
                    lifecycle.readSelectedPrefix(metadata, entered.context()).status());
        }
    }

    @Test
    public void prefixSelectionPhysicalGuardRejectsRecreateBeforeMetadataCasAndResolvesSameChoice() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var lifecycle = store(first);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var proof = prefixProof(entered, 2, 48);
            first.beforeSetDataMulti.set(() -> {
                try {
                    String path = ROOT + "/2c/recovery-prefix";
                    byte[] same = second.getData(path, false, null);
                    removeRecoveryTree(path);
                    second.create(path, same, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception failure) {
                    throw new RuntimeException(failure);
                }
            });
            assertEquals(CreateStatus.CONFLICT, lifecycle.selectPrefix(metadata, proof).status());
            assertFalse(lm.readLedgerMetadata(44).get().getValue().getCustomMetadata()
                    .containsKey(ProfileRecoveryBoundary.KEY));
            assertEquals(CreateStatus.APPLIED, lifecycle.selectPrefix(metadata, proof).status());
            assertEquals(entered.context(), lifecycle.readRecovery(metadata, 44).context());
        }
    }

    @Test
    public void selectedPrefixBetweenHistoricalAndLastCopiesKeepsTheirOriginalVersions() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var lifecycle = store(first);
            var replacement = replacement(lm, "b4:3181", 10);
            lifecycle.publishReplacement(replacement.source(), replacement.candidate(), metadata);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var history = historicalRepair(entered, 0, 1, "b5:3181", 60);
            var preparedHistory = lifecycle.prepareRepair(metadata, history).snapshot();
            lifecycle.admitRepair(metadata, preparedHistory, new ProfileInstallReceipt(history.install(),
                    EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1));
            var earlyProof = new ProfileRecoveryCoordinator.PrefixResult(
                    ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL,
                    ProfileRecoveryCoordinator.PrefixReason.NORMAL_TAIL, entered.context(), entered.metadata(),
                    99, 105, 1696, 106, List.of());
            first.beforeSetDataMulti.set(() -> assertEquals(CreateStatus.APPLIED,
                    store(second).commitRepair(membership(second, lm), preparedHistory,
                            entered.metadata(), bytes(16, 80)).status()));
            assertEquals(CreateStatus.CONFLICT, lifecycle.selectPrefix(metadata, earlyProof).status());
            var afterHistory = lifecycle.readRecovery(metadata, 44);
            var proof = new ProfileRecoveryCoordinator.PrefixResult(
                    ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL,
                    ProfileRecoveryCoordinator.PrefixReason.NORMAL_TAIL,
                    afterHistory.context(), afterHistory.metadata(),
                    99, 105, 1696, 106, List.of());
            assertEquals(CreateStatus.APPLIED, lifecycle.selectPrefix(metadata, proof).status());
            var afterSelection = lifecycle.readRecovery(metadata, 44);
            var template = historicalRepair(afterSelection, 1, 2, "b6:3181", 70);
            var tail = new ProfileRepairIntent(template.recovery(), template.slot(), template.intentId(),
                    100, 2, ZKProfileReservationStore.repairEnsembleDigest(
                            afterSelection.metadata().metadata().getValue(), 100), template.install(),
                    template.grantOperationId(), new org.apache.bookkeeper.common.profile.control.ProfileExactRange(
                            100, 105));
            var preparedTail = lifecycle.prepareRepair(metadata, tail).snapshot();
            lifecycle.admitRepair(metadata, preparedTail, new ProfileInstallReceipt(tail.install(),
                    EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1));
            assertEquals(CreateStatus.APPLIED, lifecycle.commitRepair(metadata, preparedTail,
                    afterSelection.metadata(), bytes(16, 90)).status());
            assertEquals(entered.context(), store(second).readRecovery(membership(second, lm), 44).context());
            assertEquals(CreateStatus.ALREADY_APPLIED, lifecycle.commitRepair(metadata, preparedHistory,
                    entered.metadata(), bytes(16, 80)).status());
            assertEquals(CreateStatus.ALREADY_APPLIED, lifecycle.commitRepair(metadata, preparedTail,
                    afterSelection.metadata(), bytes(16, 90)).status());
        }
    }

    @Test
    public void prefixSelectionDeleteAndMetadataChangeWinWithoutFalsePublication() throws Exception {
        for (boolean deleted : List.of(false, true)) {
            try (LedgerManager lm = manager(first)) {
                ready(lm);
                var metadata = membership(first, lm);
                var lifecycle = store(first);
                var plan = lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan();
                var entered = lifecycle.beginRecovery(metadata, plan);
                first.beforeMulti.set(() -> {
                    if (deleted) {
                        assertEquals(CreateStatus.APPLIED, store(second).cutDelete(
                                store(second).read(44).snapshot(), bytes(16, 90), bytes(32, 90)).status());
                    } else {
                        try {
                            var actual = lm.readLedgerMetadata(44).get();
                            var custom = new java.util.HashMap<>(actual.getValue().getCustomMetadata());
                            custom.put("changed", bytes(4, 91));
                            lm.writeLedgerMetadata(44, LedgerMetadataBuilder.from(actual.getValue())
                                    .withCustomMetadata(custom).build(), actual.getVersion()).get();
                        } catch (Exception failure) {
                            throw new RuntimeException(failure);
                        }
                    }
                });
                assertTrue(lifecycle.selectPrefix(metadata, prefixProof(entered, 2, 48)).snapshot() == null);
                assertFalse(lm.readLedgerMetadata(44).get().getValue().getCustomMetadata()
                        .containsKey(ProfileRecoveryBoundary.KEY));
            }
            if (!deleted) {
                removeRecoveryTree(ROOT + "/2c");
                removeRecoveryTree(ROOT + "/ledgers");
            }
        }
    }

    @Test
    public void repairOfUnboundedLastFragmentCannotPublishMembershipOrClose() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var metadata = membership(first, manager);
            var lifecycle = store(first);
            var entered = lifecycle.beginRecovery(metadata,
                    lifecycle.captureRecovery(metadata, 44, bytes(16, 50)).plan());
            var intent = repairIntent(entered, 0, 60);
            var prepared = lifecycle.prepareRepair(metadata, intent).snapshot();
            lifecycle.admitRepair(metadata, prepared, new ProfileInstallReceipt(intent.install(),
                    EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1));
            assertEquals(CreateStatus.CONFLICT, lifecycle.commitRepair(metadata, prepared, entered.metadata(),
                    bytes(16, 80)).status());
            assertEquals(entered.context(), lifecycle.readRecovery(metadata, 44).context());
            assertEquals(entered.metadata().metadata().getVersion(),
                    metadata.readMembership(44).snapshot().metadata().getVersion());
        }
    }

    @Test
    public void repairIntentVectorRejectsMalformedLengthsAndOwnsItsFields() throws Exception {
        byte[] vector = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-intent.bin").toURI()));
        var intent = ProfileRepairIntent.decode(vector);
        assertEquals(384, vector.length);
        assertEquals(0, intent.slot());
        assertEquals("b4:3181", intent.install().targetBookie());
        assertEquals(4, intent.range().lastEntryId());
        assertArrayEquals(vector, intent.encode());
        intent.intentId()[0] = 0;
        assertArrayEquals(vector, intent.encode());
        for (int offset : new int[]{0, 4, 6, 352}) {
            byte[] bad = vector.clone();
            bad[offset] ^= 1;
            CRC32C crc = new CRC32C();
            crc.update(bad, 0, bad.length - 4);
            ByteBuffer.wrap(bad).putInt(bad.length - 4, (int) crc.getValue());
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairIntent.decode(bad));
        }
    }

    @Test
    public void emptyRepairVectorsExplicitlyForbidDataAndBindTheirBoundaryReceipt() throws Exception {
        byte[] intentBytes = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-empty-intent.bin").toURI()));
        byte[] boundaryBytes = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-empty-boundary.bin").toURI()));
        byte[] commitBytes = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/repair-empty-commit.bin").toURI()));
        var intent = ProfileRepairIntent.decode(intentBytes);
        assertEquals(400, intentBytes.length);
        assertTrue(intent.empty());
        assertNull(intent.range());
        assertArrayEquals(new byte[16], intent.grantOperationId());
        assertArrayEquals(intentBytes, intent.encode());
        assertThrows(IllegalStateException.class, intent::grantScope);
        assertThrows(IllegalStateException.class, () -> intent.closeScope(bytes(16, 91), 7));
        var boundary = ProfileRepairCommit.EmptyBoundary.decode(boundaryBytes);
        assertEquals(124, boundaryBytes.length);
        assertArrayEquals(boundaryBytes, boundary.encode());
        assertEquals(-1, boundary.binding().prefixEntryId());
        assertEquals(0, boundary.binding().prefixLength());
        assertArrayEquals(ProfileRepairIntent.digest(intentBytes), boundary.intentDigest());
        var scope = intent.terminalScope(intent.terminalOperationId(), intent.grantGeneration(), boundary.binding());
        assertNull(scope.exactRange());
        var commit = ProfileRepairCommit.decode(commitBytes);
        assertArrayEquals(commitBytes, commit.encode());
        assertArrayEquals(ProfileRepairIntent.digest(boundaryBytes), commit.emptyBoundaryDigest());
        intent.terminalOperationId()[0] = 0;
        boundary.operationId()[0] = 0;
        commit.emptyBoundaryDigest()[0] = 0;
        assertArrayEquals(intentBytes, intent.encode());
        assertArrayEquals(boundaryBytes, boundary.encode());
        assertArrayEquals(commitBytes, commit.encode());
        for (int size = 0; size < intentBytes.length; size++) {
            byte[] truncated = Arrays.copyOf(intentBytes, size);
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairIntent.decode(truncated));
        }
        for (int size = 0; size < boundaryBytes.length; size++) {
            byte[] truncated = Arrays.copyOf(boundaryBytes, size);
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.EmptyBoundary.decode(truncated));
        }
        for (int size = 0; size < commitBytes.length; size++) {
            byte[] truncated = Arrays.copyOf(commitBytes, size);
            assertThrows(IllegalArgumentException.class, () -> ProfileRepairCommit.decode(truncated));
        }
        for (int offset : new int[] {4, 6, 248, 304, 320}) {
            byte[] bad = intentBytes.clone();
            bad[offset] ^= 1;
            CRC32C crc = new CRC32C();
            crc.update(bad, 0, bad.length - 4);
            ByteBuffer.wrap(bad).putInt(bad.length - 4, (int) crc.getValue());
            if (offset != 320) {
                assertThrows(IllegalArgumentException.class, () -> ProfileRepairIntent.decode(bad));
            } else {
                var changed = ProfileRepairIntent.decode(bad);
                assertThrows(IllegalArgumentException.class,
                        () -> changed.terminalScope(intent.terminalOperationId(),
                                changed.grantGeneration(), boundary.binding()));
            }
        }
    }

    @Test
    public void repairIntentIsInertThenAdmittedAndRetainedAfterDelete() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var plan = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
            var entered = store(first).beginRecovery(metadata, plan);
            var desired = repairIntent(entered, 0, 12);
            var prepared = store(first).prepareRepair(metadata, desired);
            assertEquals(CreateStatus.APPLIED, prepared.status());
            assertEquals(desired, ProfileRepairIntent.decode(desired.encode()));
            var authority = store(first).repairAuthorityReader("b4:3181", metadata);
            assertEquals(ReadStatus.FOUND, authority.read(ProfileAuthorityKey.fromScope(desired.install())).status());
            assertEquals(ReadStatus.NOT_FOUND,
                    authority.read(ProfileAuthorityKey.fromScope(desired.grantScope())).status());
            assertEquals(3, store(first).retainedActivationTargets(44).targets().size());
            var receipt = new ProfileInstallReceipt(desired.install(), EngineProfile.SEGMENT_WAL_ENGINE,
                    bytes(32, 7), 1);
            first.loseReply.set(true);
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    store(first).admitRepair(metadata, prepared.snapshot(), receipt).status());
            assertEquals(ReadStatus.FOUND,
                    authority.read(ProfileAuthorityKey.fromScope(desired.grantScope())).status());
            assertEquals(4, store(first).retainedActivationTargets(44).targets().size());
            assertEquals(CreateStatus.CONFLICT,
                    store(first).prepareRepair(metadata, repairIntent(entered, 0, 13)).status());
            assertEquals(CreateStatus.APPLIED,
                    store(first).cutDelete(plan.lifecycle(), bytes(16, 14), bytes(32, 14)).status());
            assertEquals(ReadStatus.QUARANTINED,
                    authority.read(ProfileAuthorityKey.fromScope(desired.grantScope())).status());
            assertEquals(4, store(first).retainedActivationTargets(44).targets().size());
            assertThrows(IllegalArgumentException.class, () -> repairIntent(entered, 32, 15));
        }
    }

    @Test
    public void repairAdmissionDeleteOrIntentRecreationLeavesPreparedTargetInert() throws Exception {
        for (boolean recreate : List.of(false, true)) {
            try (LedgerManager lm = manager(first)) {
                ready(lm);
                var metadata = membership(first, lm);
                var plan = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
                var entered = store(first).beginRecovery(metadata, plan);
                var desired = repairIntent(entered, 0, 12);
                var prepared = store(first).prepareRepair(metadata, desired).snapshot();
                var receipt = new ProfileInstallReceipt(desired.install(), EngineProfile.SEGMENT_WAL_ENGINE,
                        bytes(32, 7), 1);
                if (recreate) {
                    first.beforeMulti.set(() -> {
                        try {
                            removeTree(ROOT + "/2c/repair-0");
                            assertEquals(CreateStatus.APPLIED, store(second).prepareRepair(
                                    membership(second, lm), desired).status());
                        } catch (Exception failed) {
                            throw new AssertionError(failed);
                        }
                    });
                } else {
                    assertEquals(CreateStatus.APPLIED,
                            store(first).cutDelete(plan.lifecycle(), bytes(16, 13), bytes(32, 13)).status());
                }
                assertEquals(CreateStatus.CONFLICT, store(first).admitRepair(metadata, prepared, receipt).status());
                assertEquals(null, second.exists(ROOT + "/2c/repair-0/admitted", false));
            }
            // Each branch uses an independent real namespace/metadata setup.
            if (!recreate) {
                removeRecoveryTree(ROOT + "/2c");
                removeRecoveryTree(ROOT + "/ledgers");
            }
        }
    }

    @Test
    public void recoveryContextVectorOwnsItsFieldsAndRejectsMalformedIdentity() throws Exception {
        byte[] vector = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(
                getClass().getResource("/unified-wal-segment/recovery-context.bin").toURI()));
        var context = ProfileRecoveryContext.decode(vector);
        assertEquals(184, vector.length);
        ByteBuffer raw = ByteBuffer.wrap(vector);
        assertEquals(0x55575243, raw.getInt(0));
        assertEquals(17, raw.getLong(84));
        assertEquals(29, raw.getLong(96));
        assertEquals(31, raw.getLong(136));
        assertEquals(2, raw.getLong(160));
        assertEquals(1, context.generation());
        assertEquals(new java.util.UUID(0, 37), context.lifecycleBirth());
        assertArrayEquals(vector, context.encode());
        context.operationId()[0] = 0;
        context.predecessorDigest()[0] = 0;
        assertArrayEquals(vector, context.encode());
        for (int offset : new int[]{0, 4, 6, 92, 140}) {
            byte[] bad = vector.clone();
            if (offset == 92) {
                ByteBuffer.wrap(bad).putInt(offset, -1);
            } else if (offset == 140) {
                ByteBuffer.wrap(bad).putLong(136, 0);
            } else {
                bad[offset] ^= 1;
            }
            CRC32C crc = new CRC32C();
            crc.update(bad, 0, bad.length - 4);
            ByteBuffer.wrap(bad).putInt(bad.length - 4, (int) crc.getValue());
            assertThrows(IllegalArgumentException.class, () -> ProfileRecoveryContext.decode(bad));
        }
    }

    @Test
    public void recoveryLostCasReplyResumesOneFrozenOperationWithoutChangingFields() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var lifecycle = store(first);
            var captured = lifecycle.captureRecovery(metadata, 44, bytes(16, 11)).plan();
            first.loseSetDataMultiReply.set(true);
            var entered = lifecycle.beginRecovery(metadata, captured);
            assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, entered.outcome());
            var current = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(LedgerMetadata.State.IN_RECOVERY, current.getValue().getState());
            assertEquals(captured.predecessor().metadata().getValue().getCToken(), current.getValue().getCToken());
            assertEquals(captured.predecessor().metadata().getValue().getAllEnsembles(),
                    current.getValue().getAllEnsembles());
            assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED,
                    store(first).readRecovery(metadata, 44).outcome());
            var different = new ZKProfileReservationStore.RecoveryPlan(captured.predecessor(), captured.lifecycle(),
                    ProfileRecoveryContext.capture(captured.predecessor(), captured.lifecycle(), bytes(16, 12)));
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                    lifecycle.beginRecovery(metadata, different).outcome());
            assertEquals(current.getVersion(), lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getVersion());
        }
    }

    @Test
    public void recoveryDeleteCutWinsAgainstTheSameRecordTransition() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var captured = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
            first.beforeSetDataMulti.set(() -> assertEquals(CreateStatus.APPLIED,
                    store(second).cutDelete(captured.lifecycle(), bytes(16, 12), bytes(32, 12)).status()));
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                    store(first).beginRecovery(metadata, captured).outcome());
            var current = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(LedgerMetadata.State.OPEN, current.getValue().getState());
            assertEquals(captured.predecessor().metadata().getVersion(), current.getVersion());
        }
    }

    @Test
    public void recoveryMetadataRecreationCannotReuseTheCapturedBirth() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var captured = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(44);
            byte[] original = second.getData(path, false, null);
            first.beforeSetDataMulti.set(() -> {
                try {
                    removeTree(path);
                    second.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                    store(first).beginRecovery(metadata, captured).outcome());
            assertEquals(LedgerMetadata.State.OPEN,
                    lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                    store(first).beginRecovery(metadata, captured).outcome());
        }
    }

    @Test
    public void recoveryLifecycleRecreationCannotReuseItsUuidAndVersionAfterThePrecheck() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var captured = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
            String root = ROOT + "/2c";
            String head = root + "/lifecycle";
            byte[] parentBytes = second.getData(root, false, null);
            byte[] headBytes = second.getData(head, false, null);
            first.beforeSetDataMulti.set(() -> {
                try {
                    removeRecoveryTree(root);
                    second.multi(List.of(Op.create(root, parentBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT), Op.create(head, headBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT), Op.create(head + "/birth-" + captured.context().lifecycleBirth(),
                            new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                            Op.setData(head, headBytes, 0)));
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                    store(first).beginRecovery(metadata, captured).outcome());
            var current = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(LedgerMetadata.State.OPEN, current.getValue().getState());
            assertEquals(captured.predecessor().metadata().getVersion(), current.getVersion());
        }
    }

    private void removeRecoveryTree(String path) throws Exception {
        for (String child : second.getChildren(path, false)) {
            removeRecoveryTree(path + "/" + child);
        }
        second.delete(path, second.exists(path, false).getVersion());
    }

    @Test
    public void recoveryMissingRequiredGuardIsNotRebuiltDuringAdoption() throws Exception {
        try (LedgerManager lm = manager(first)) {
            ready(lm);
            var metadata = membership(first, lm);
            var captured = store(first).captureRecovery(metadata, 44, bytes(16, 11)).plan();
            assertEquals(ProfileLedgerMetadataStore.Outcome.APPLIED,
                    store(first).beginRecovery(metadata, captured).outcome());
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(44);
            String child = second.getChildren(path, false).get(0);
            second.delete(path + "/" + child, 0);
            assertEquals(ProfileLedgerMetadataStore.Outcome.QUARANTINED,
                    store(first).readRecovery(metadata, 44).outcome());
            assertTrue(second.getChildren(path, false).isEmpty());
        }
    }

    @Test
    public void replacementSameStartHistorySurvivesOverwriteAndDeleteCutWithoutAuthorizingOldTargets()
            throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var firstReplacement = replacement(manager, "b4:3181", 10);
            var metadata = membership(first, manager);
            var lifecycle = store(first);
            assertEquals(CreateStatus.APPLIED, lifecycle.publishReplacement(firstReplacement.source(),
                    firstReplacement.candidate(), metadata).status());
            var oldKey = ProfileAuthorityKey.fromScope(firstReplacement.desired().activation("b4:3181"));
            assertEquals(ReadStatus.FOUND, store(second).replacementAuthorityReader("b4:3181",
                    membership(second, manager)).read(oldKey).status());
            var secondReplacement = replacement(manager, "b5:3181", 11);
            assertEquals(CreateStatus.APPLIED, lifecycle.publishReplacement(secondReplacement.source(),
                    secondReplacement.candidate(), metadata).status());
            var current = manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(List.of("b1:3181", "b2:3181", "b5:3181"),
                    current.getValue().getAllEnsembles().get(100L).stream().map(Object::toString).toList());
            assertEquals(ReadStatus.NOT_FOUND, lifecycle.replacementAuthorityReader("b4:3181", metadata)
                    .read(oldKey).status());
            var beforeDelete = lifecycle.read(44).snapshot();
            assertEquals(CreateStatus.APPLIED, lifecycle.cutDelete(beforeDelete, bytes(16, 120),
                    bytes(32, 120)).status());
            var retained = store(second).retainedActivationTargets(44);
            assertEquals(ReadStatus.FOUND, retained.status());
            assertEquals(List.of("b1:3181", "b2:3181", "b3:3181", "b4:3181", "b5:3181"),
                    retained.targets().stream().map(value -> value.scope().targetBookie()).toList());
            assertEquals(ReadStatus.NOT_FOUND, lifecycle.replacementAuthorityReader("b5:3181", metadata).read(
                    ProfileAuthorityKey.fromScope(secondReplacement.desired().activation("b5:3181"))).status());
            assertEquals(CreateStatus.ALREADY_APPLIED, lifecycle.readPublishedPage(44).status());
        }
    }

    @Test
    public void replacementCandidateIsInertAndDeleteFirstPreventsPublicationAndTargetRetention() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var candidate = replacement(manager, "b4:3181", 10);
            var lifecycle = store(first);
            var metadata = membership(first, manager);
            var key = ProfileAuthorityKey.fromScope(candidate.desired().activation("b4:3181"));
            assertEquals(ReadStatus.NOT_FOUND, lifecycle.replacementAuthorityReader("b4:3181", metadata)
                    .read(key).status());
            assertEquals(CreateStatus.APPLIED, lifecycle.cutDelete(candidate.source(), bytes(16, 120),
                    bytes(32, 120)).status());
            assertEquals(CreateStatus.CONFLICT, lifecycle.publishReplacement(candidate.source(),
                    candidate.candidate(), metadata).status());
            assertEquals(3, lifecycle.retainedActivationTargets(44).targets().size());
            assertEquals(ReadStatus.NOT_FOUND, lifecycle.replacementAuthorityReader("b4:3181", metadata)
                    .read(key).status());
        }
    }

    @Test
    public void replacementLostPublicationReplyAfterDeleteReportsHistoryWithoutCurrentActivation() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var candidate = replacement(manager, "b4:3181", 10);
            var lifecycle = store(first);
            first.loseReply.set(true);
            first.beforeLostMultiReply.set(() -> {
                var other = store(second);
                assertEquals(CreateStatus.APPLIED, other.cutDelete(other.read(44).snapshot(), bytes(16, 120),
                        bytes(32, 120)).status());
            });
            var result = lifecycle.publishReplacement(candidate.source(), candidate.candidate(),
                    membership(first, manager));
            assertEquals(CreateStatus.ALREADY_APPLIED, result.status());
            assertEquals(State.DELETE_INTENT, result.snapshot().lifecycle().state());
            assertEquals(4, lifecycle.retainedActivationTargets(44).targets().size());
            assertEquals(ReadStatus.NOT_FOUND, lifecycle.replacementAuthorityReader("b4:3181",
                    membership(first, manager)).read(ProfileAuthorityKey.fromScope(
                            candidate.desired().activation("b4:3181"))).status());
        }
    }

    @Test
    public void replacementMetadataFreezeAfterReadFailsTheSamePublicationMulti() throws Exception {
        try (LedgerManager manager = manager(first); LedgerManager other = manager(second)) {
            ready(manager);
            var candidate = replacement(manager, "b4:3181", 10);
            first.beforeMulti.set(() -> {
                try {
                    var actual = other.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    var custom = new HashMap<>(actual.getValue().getCustomMetadata());
                    custom.put(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY, new byte[]{1});
                    other.writeLedgerMetadata(44, LedgerMetadataBuilder.from(actual.getValue())
                            .withCToken(actual.getValue().getCToken()).withCustomMetadata(custom).build(),
                            actual.getVersion()).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(CreateStatus.CONFLICT, store(first).publishReplacement(candidate.source(),
                    candidate.candidate(), membership(first, manager)).status());
            assertEquals(State.READY, store(second).read(44).snapshot().lifecycle().state());
            assertEquals(3, store(second).retainedActivationTargets(44).targets().size());
        }
    }

    @Test
    public void replacementPageRecreatedAtVersionZeroCannotPassThePublicationBirthCheck() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var candidate = replacement(manager, "b4:3181", 10);
            String pagePath = store(first).path(44) + "/replacement-3";
            first.beforeMulti.set(() -> {
                try {
                    second.delete(pagePath + "/birth-" + candidate.candidate().birthId(), 0);
                    second.delete(pagePath, 0);
                    var envelope = new ProfileLifecycleCodec.Replacement(UUID.randomUUID(), candidate.desired());
                    second.multi(List.of(Op.create(pagePath, ProfileLifecycleCodec.encode(envelope),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT), Op.create(pagePath + "/birth-"
                            + envelope.birthId(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(CreateStatus.CONFLICT, store(first).publishReplacement(candidate.source(),
                    candidate.candidate(), membership(first, manager)).status());
            assertEquals(State.READY, store(second).read(44).snapshot().lifecycle().state());
        }
    }

    @Test
    public void replacementMissingHistoryNeverReturnsPartialCleanupTargets() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            var firstPage = replacement(manager, "b4:3181", 10);
            store(first).publishReplacement(firstPage.source(), firstPage.candidate(), membership(first, manager));
            var secondPage = replacement(manager, "b5:3181", 11);
            store(first).publishReplacement(secondPage.source(), secondPage.candidate(), membership(first, manager));
            String path = store(first).path(44) + "/replacement-3";
            second.delete(path + "/birth-" + firstPage.candidate().birthId(), 0);
            second.delete(path, 0);
            var retained = store(second).retainedActivationTargets(44);
            assertEquals(ReadStatus.QUARANTINED, retained.status());
            assertTrue(retained.targets().isEmpty());
        }
    }

    @Test
    public void replacementPublicationBudgetKeepsAllTargetsAndStopsBeforeTheNextPage() throws Exception {
        try (LedgerManager manager = manager(first)) {
            ready(manager);
            for (int index = 0; index < ProfileReplacementPage.MAX_PUBLICATIONS; index++) {
                var candidate = replacement(manager, "target" + index + ":3181", 10 + index);
                assertEquals(CreateStatus.APPLIED, store(first).publishReplacement(candidate.source(),
                        candidate.candidate(), membership(first, manager)).status());
            }
            var source = store(first).read(44).snapshot();
            assertEquals(34, source.lifecycle().sequence());
            assertEquals(35, store(second).retainedActivationTargets(44).targets().size());
            assertEquals(66, first.getChildren(store(first).path(44), false).size());
            var metadata = membership(first, manager).readMembership(44).snapshot();
            var initial = store(first).readPublishedPage(44).page();
            assertThrows(IllegalArgumentException.class, () -> ProfileReplacementPage.from(source, initial,
                    metadata, 100, List.of(new ProfileInstallReceipt(scope("one-more:3181"),
                            EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1)), bytes(16, 120), bytes(32, 120)));
            assertEquals(source, store(first).read(44).snapshot());
        }
    }

    @Test
    public void replacementFixedVectorAndIndependentFieldsRejectKindScopeAndLengthCorruption() {
        byte[] vector = HexFormat.of().parseHex(
                "5557415000000001000002260000000200000000000000005557424c00000001000000000000002c01010101010101010101"
                + "01010101010102020202020202020202020202020202b62a5921000000000000000000000000000000090000000000000000"
                + "000000000000000a040404040404040404040404040404040505050505050505050505050505050505050505050505050505"
                + "0505050505050000000000000008000000000000000300000000000000110606060606060606060606060606060606060606"
                + "0606060606060606060606060000000000000015000000000000000100000000000000330000000000000064070707070707"
                + "0707070707070707070707070707070707070707070707070707000000000000001208080808080808080808080808080808"
                + "0808080808080808080808080808080800000001000000b65557415500000001000000b60000000100000001000000010000"
                + "000100000003000000000000002c010101010101010101010101010101010001000109090909090909090909090909090909"
                + "0909090909090909090909090909090902020202020202020202020202020202000000000000000803030303030303030303"
                + "0303030303030404040404040404040404040404040404040404040404040404040404040404000000016200000001640000"
                + "0000731b211407070707070707070707070707070707070707070707070707070707070707070000000000000001"
                + "e6ef3b19");
        var value = new ProfileReplacementPage(reservation().backlink(), new UUID(0, 9), bytes(16, 4),
                bytes(32, 5), 8, 3, 17, bytes(32, 6), 21, 1, 51, 100, bytes(32, 7), 18, bytes(32, 8),
                List.of(new ProfileInstallReceipt(scope("b"), EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1)));
        var envelope = new ProfileLifecycleCodec.Replacement(new UUID(0, 10), value);
        assertArrayEquals(vector, ProfileLifecycleCodec.encode(envelope));
        assertEquals(envelope, ProfileLifecycleCodec.decodeReplacement(vector));
        ByteBuffer fields = ByteBuffer.wrap(vector);
        assertEquals(2, fields.getInt(12));
        assertEquals(3, fields.getLong(164));
        assertEquals(21, fields.getLong(212));
        assertEquals(100, fields.getLong(236));
        assertEquals(1, fields.getInt(316));
        assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodePage(vector));
        for (int offset : List.of(0, 4, 8, 12, 16, 316)) {
            byte[] wrong = vector.clone();
            ByteBuffer.wrap(wrong).putInt(offset, -1);
            checksum(wrong);
            assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeReplacement(wrong));
        }
        for (int offset : List.of(164, 212, 220, 236)) {
            byte[] wrong = vector.clone();
            ByteBuffer.wrap(wrong).putLong(offset, -1);
            checksum(wrong);
            assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeReplacement(wrong));
        }
        byte[] trailing = Arrays.copyOf(vector, vector.length + 1);
        ByteBuffer.wrap(trailing).putInt(8, trailing.length);
        checksum(trailing);
        assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeReplacement(trailing));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLifecycleCodec.decodeReplacement(new byte[323]));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLifecycleCodec.decodeReplacement(
                        new byte[ProfileLifecycleCodec.REPLACEMENT_MAX_BYTES + 1]));
    }

    private ProfileReplacementInstallPage installationPlan(ZKProfileReservationStore.Snapshot source,
            ProfileLedgerMetadataStore.MembershipSnapshot before) {
        var target = "b4:3181";
        var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                ProfileControlOperation.INSTALL.purpose(),
                44, bytes(16, 1), scope(target).descriptorIdentity(), target, bytes(16, 10), "d", 8,
                bytes(16, 10), bytes(32, 10), null);
        return ProfileReplacementInstallPage.from(source, before, 100,
                List.of(BookieId.parse("b1:3181"), BookieId.parse("b2:3181"), BookieId.parse(target)),
                List.of(install));
    }

    @Test
    public void installPreparationIsBoundedInactiveAndResolvesLostReplyWithoutAnotherVersion() throws Exception {
        try (LedgerManager manager = manager(first)) {
            var source = ready(manager);
            var metadata = membership(first, manager);
            var before = metadata.readMembership(44).snapshot();
            var plan = installationPlan(source, before);
            first.loseReply.set(true); // Metadata birth guard transaction can also lose a reply and reconcile.
            var prepared = store(first).prepareReplacementInstall(source, plan, metadata);
            assertTrue(prepared.status() == CreateStatus.APPLIED || prepared.status() == CreateStatus.ALREADY_APPLIED);
            var retried = store(first).prepareReplacementInstall(source, plan, metadata);
            assertEquals(CreateStatus.ALREADY_APPLIED, retried.status());
            assertEquals(prepared.page().storeVersion(), retried.page().storeVersion());
            var installed = plan.installs().get(0);
            var authority = store(second).installAuthorityReader("b4:3181", membership(second, manager))
                    .read(ProfileAuthorityKey.fromScope(installed));
            assertEquals(ReadStatus.FOUND, authority.status());
            assertEquals(installed, authority.authority().scope());
            assertEquals(source, store(first).read(44).snapshot());
            assertEquals(3, store(first).retainedActivationTargets(44).targets().size());
            assertEquals(CreateStatus.APPLIED, store(first).cutDelete(source, bytes(16, 120), bytes(32, 120)).status());
            assertEquals(ReadStatus.NOT_FOUND, store(first).installAuthorityReader("b4:3181", metadata)
                    .read(ProfileAuthorityKey.fromScope(installed)).status());
        }
    }

    @Test
    public void installPreparationMetadataFreezeWinsAtomicallyBeforeAnyCandidateIsCreated() throws Exception {
        try (LedgerManager manager = manager(first); LedgerManager other = manager(second)) {
            var source = ready(manager);
            var metadata = membership(first, manager);
            var before = metadata.readMembership(44).snapshot();
            metadata.ensureMembershipGuard(before, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
            var plan = installationPlan(source, before);
            first.beforeMulti.set(() -> {
                try {
                    var custom = new HashMap<>(before.metadata().getValue().getCustomMetadata());
                    custom.put(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY, new byte[]{1});
                    other.writeLedgerMetadata(44, LedgerMetadataBuilder.from(before.metadata().getValue())
                            .withCToken(before.metadata().getValue().getCToken()).withCustomMetadata(custom).build(),
                            before.metadata().getVersion()).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(CreateStatus.CONFLICT,
                    store(first).prepareReplacementInstall(source, plan, metadata).status());
            assertEquals(null, first.exists(store(first).path(44) + "/replacement-install-3", false));
            assertEquals(source, store(first).read(44).snapshot());
        }
    }

    @Test
    public void installPreparationCodecOwnsArraysAndRejectsKindLengthAndScopeCorruption() throws Exception {
        try (LedgerManager manager = manager(first)) {
            var source = ready(manager);
            var plan = installationPlan(source, membership(first, manager).readMembership(44).snapshot());
            var envelope = new ProfileLifecycleCodec.InstallPreparation(new UUID(0, 11), plan);
            byte[] encoded = ProfileLifecycleCodec.encode(envelope);
            assertEquals(3, ByteBuffer.wrap(encoded).getInt(12));
            assertEquals(1, ByteBuffer.wrap(encoded).getInt(228));
            assertEquals(plan, ProfileLifecycleCodec.decodeInstallPreparation(encoded).value());
            plan.ensembleDigest()[0]++;
            assertEquals(plan, ProfileLifecycleCodec.decodeInstallPreparation(encoded).value());
            for (int offset : List.of(0, 4, 8, 12, 16, 228)) {
                byte[] corrupt = encoded.clone();
                ByteBuffer.wrap(corrupt).putInt(offset, -1);
                checksum(corrupt);
                assertThrows(IllegalArgumentException.class,
                        () -> ProfileLifecycleCodec.decodeInstallPreparation(corrupt));
            }
            assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeInstallPreparation(
                    new byte[ProfileLifecycleCodec.INSTALL_PREP_MAX_BYTES + 1]));
            assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeReplacement(encoded));
        }
    }

    @Test
    public void installPreparationFixedIndependentVectorMatchesItsPublicSchema() {
        byte[] expected = HexFormat.of().parseHex(
                "5557415000000001000001a60000000300000000000000005557424c00000001000000000000002c0101010101"
                + "010101010101010101010102020202020202020202020202020202b62a59210000000000000000000000000000"
                + "00090000000000000000000000000000000b000000000000000800000000000000030000000000000015000000"
                + "000000000000000000000000330000000000000000060606060606060606060606060606060606060606060606"
                + "060606060606060600000000000000640707070707070707070707070707070707070707070707070707070707"
                + "07070700000001000000b65557415500000001000000b600000001000000010000000100000001000000030000"
                + "00000000002c010101010101010101010101010101010001000109090909090909090909090909090909090909"
                + "090909090909090909090909090202020202020202020202020202020200000000000000080303030303030303"
                + "030303030303030304040404040404040404040404040404040404040404040404040404040404040000000162"
                + "000000016400000000731b2114ca293b1c");
        var value = new ProfileReplacementInstallPage(reservation().backlink(), new UUID(0, 9), 3, 8,
                21, 0, 51, 0, bytes(32, 6), 100, bytes(32, 7), List.of(scope("b")));
        var envelope = new ProfileLifecycleCodec.InstallPreparation(new UUID(0, 11), value);
        assertArrayEquals(expected, ProfileLifecycleCodec.encode(envelope));
        assertEquals(value, ProfileLifecycleCodec.decodeInstallPreparation(expected).value());
        assertEquals(21, ByteBuffer.wrap(expected).getLong(124));
        assertEquals(100, ByteBuffer.wrap(expected).getLong(188));
        assertEquals(1, ByteBuffer.wrap(expected).getInt(228));
    }

    private static void checksum(byte[] value) {
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length - 4);
        ByteBuffer.wrap(value).putInt(value.length - 4, (int) crc.getValue());
    }

    @Test
    public void independentHeadAndPageVectorsHaveStrictBoundedDiscriminators() {
        byte[] headVector = HexFormat.of().parseHex(
                "55574c4800000001000000f00000000100000000000000005557424c00000001000000000000002c01010101010101010101"
                + "01010101010102020202020202020202020202020202b62a5921030303030303030303030303030303030303030303030303"
                + "0303030303030303000000000000000000000000000000090000000000000001000000000000000000000000000000080404"
                + "04040404040404040404040404040505050505050505050505050505050505050505050505050505050505050505ffffffff"
                + "ffffffff0000000000000000000000000000000000000000000000000000000000000000a6950eef");
        ProfileLifecycleHead head = new ProfileLifecycleHead(reservation().backlink(), bytes(32, 3), new UUID(0, 9),
                State.PREPARING, 1, 0, 8, bytes(16, 4), bytes(32, 5), -1, new byte[32]);
        assertArrayEquals(headVector, ProfileLifecycleCodec.encode(head));
        assertEquals(head, ProfileLifecycleCodec.decodeHead(headVector));
        byte[] pageVector = HexFormat.of().parseHex(
                "5557415000000001000001be0000000100000000000000005557424c00000001000000000000002c01010101010101010101"
                + "01010101010102020202020202020202020202020202b62a5921000000000000000000000000000000090000000000000000"
                + "000000000000000a040404040404040404040404040404040505050505050505050505050505050505050505050505050505"
                + "0505050505050000000000000008000000000000000000000000000000330606060606060606060606060606060606060606"
                + "06060606060606060606060600000001000000b65557415500000001000000b6000000010000000100000001000000010000"
                + "0003000000000000002c01010101010101010101010101010101000100010909090909090909090909090909090909090909"
                + "0909090909090909090909090202020202020202020202020202020200000000000000080303030303030303030303030303"
                + "030304040404040404040404040404040404040404040404040404040404040404040000000162000000016400000000731b"
                + "211407070707070707070707070707070707070707070707070707070707070707070000000000000001f443c6db");
        ProfileReadyPage page = new ProfileReadyPage(reservation().backlink(), new UUID(0, 9), bytes(16, 4),
                bytes(32, 5), 8, 0, 51, bytes(32, 6), List.of(new ProfileInstallReceipt(scope("b"),
                        EngineProfile.SEGMENT_WAL_ENGINE, bytes(32, 7), 1)));
        var envelope = new ProfileLifecycleCodec.Page(new UUID(0, 10), page);
        assertArrayEquals(pageVector, ProfileLifecycleCodec.encode(envelope));
        assertEquals(envelope, ProfileLifecycleCodec.decodePage(pageVector));
        for (int offset : List.of(0, 4, 8, 12, 16)) {
            byte[] changed = headVector.clone();
            ByteBuffer.wrap(changed).putInt(offset, -1);
            checksum(changed);
            assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodeHead(changed));
        }
        pageVector[80]++;
        assertThrows(IllegalArgumentException.class, () -> ProfileLifecycleCodec.decodePage(pageVector));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLifecycleCodec.decodePage(new byte[ProfileLifecycleCodec.PAGE_MAX_BYTES + 1]));
    }

    @Test
    public void actualPageIsInertUntilReadyAndThenBindsMetadataAndEveryTarget() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            var reader = store(second).activationAuthorityReader("b1:3181", lm);
            var key = ProfileAuthorityKey.fromScope(prepared.desired().activation("b1:3181"));
            assertEquals(ReadStatus.NOT_FOUND, reader.read(key).status());
            assertEquals(CreateStatus.CONFLICT, store(second).readPublishedPage(44).status());
            assertEquals(State.PREPARING, store(second).read(44).snapshot().lifecycle().state());
            var published = store(first).publishReady(prepared.root(), prepared.page(), lm);
            assertEquals(CreateStatus.APPLIED, published.status());
            assertEquals(2, published.snapshot().lifecycle().sequence());
            assertEquals(8, published.snapshot().lifecycle().controlGeneration());
            assertEquals(ReadStatus.FOUND, reader.read(key).status());
            assertEquals(prepared.desired().activation("b1:3181"), reader.read(key).authority().scope());
            assertEquals(prepared.page(), store(second).readPublishedPage(44).page());
            assertEquals(3, store(second).readPublishedPage(44).page().page().installs().size());
            assertThrows(KeeperException.NoAuthException.class, () -> second.setData(
                    ROOT + "/2c/initial-activation", ProfileLifecycleCodec.encode(
                            new ProfileLifecycleCodec.Page(prepared.page().birthId(), prepared.desired())), 0));
        }
    }

    @Test
    public void realLostPageAndReadyRepliesResolveExactOperationWithoutAnotherMutation() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm, true);
            first.loseReply.set(true);
            var result = store(first).publishReady(prepared.root(), prepared.page(), lm);
            assertEquals(CreateStatus.ALREADY_APPLIED, result.status());
            assertEquals(1, first.exists(ROOT + "/2c/lifecycle", false).getVersion());
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    store(second).publishReady(prepared.root(), prepared.page(), lm).status());
            assertEquals(1, first.exists(ROOT + "/2c/lifecycle", false).getVersion());
            var changed = new ProfileReadyPage(prepared.desired().backlink(), prepared.desired().lifecycleBirth(),
                    prepared.desired().operationId(), bytes(32, 9), 8, prepared.desired().metadataVersion(),
                    prepared.desired().creatorToken(), prepared.desired().ensembleDigest(),
                            prepared.desired().installs());
            assertEquals(CreateStatus.CONFLICT, store(second).prepareReady(prepared.root(), changed, lm).status());
        }
    }

    @Test
    public void deleteBeforeReadyWinsAndTheCandidateNeverAuthorizesActivation() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            var cut = store(second).cutDelete(prepared.root(), bytes(16, 5), bytes(32, 6));
            assertEquals(CreateStatus.APPLIED, cut.status());
            assertEquals(9, cut.snapshot().lifecycle().controlGeneration());
            assertEquals(CreateStatus.CONFLICT, store(first).publishReady(prepared.root(), prepared.page(),
                    lm).status());
            assertEquals(CreateStatus.CONFLICT, store(first).readPublishedPage(44).status());
            assertEquals(ReadStatus.NOT_FOUND, store(first).activationAuthorityReader("b1:3181", lm).read(
                    ProfileAuthorityKey.fromScope(prepared.desired().activation("b1:3181"))).status());
            assertEquals(ReadStatus.NOT_FOUND, store(first).installAuthorityReader("b1:3181").read(
                    ProfileAuthorityKey.fromScope(reservation().installs().get(0))).status());
        }
    }

    @Test
    public void readyThenDeleteRetainsAllTargetsAndResolvesBothCommittedOperations() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            var ready = store(first).publishReady(prepared.root(), prepared.page(), lm);
            first.loseReply.set(true);
            var cut = store(first).cutDelete(ready.snapshot(), bytes(16, 5), bytes(32, 6));
            assertEquals(CreateStatus.ALREADY_APPLIED, cut.status());
            assertEquals(State.DELETE_INTENT, cut.snapshot().lifecycle().state());
            assertEquals(3, cut.snapshot().lifecycle().sequence());
            assertEquals(prepared.page(), store(second).readPublishedPage(44).page());
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    store(second).publishReady(prepared.root(), prepared.page(), lm).status());
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    store(second).cutDelete(ready.snapshot(), bytes(16, 5), bytes(32, 6)).status());
            assertEquals(CreateStatus.CONFLICT,
                    store(second).cutDelete(ready.snapshot(), bytes(16, 5), bytes(32, 7)).status());
        }
    }

    @Test
    public void concurrentReadyAndDeleteHaveOneConditionalWinner() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            CountDownLatch both = new CountDownLatch(2);
            Runnable meet = () -> {
                both.countDown();
                try {
                    assertTrue(both.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            };
            first.beforeMulti.set(meet);
            second.beforeMulti.set(meet);
            var ready = CompletableFuture.supplyAsync(() -> store(first).publishReady(prepared.root(),
                    prepared.page(), lm));
            var cut = CompletableFuture.supplyAsync(() -> store(second).cutDelete(prepared.root(), bytes(16,
                    5), bytes(32, 6)));
            List<CreateStatus> outcomes = List.of(ready.get(5, TimeUnit.SECONDS).status(), cut.get(5,
                    TimeUnit.SECONDS).status());
            assertEquals(1, outcomes.stream().filter(status -> status == CreateStatus.APPLIED).count());
            assertTrue(outcomes.contains(CreateStatus.CONFLICT));
        }
    }

    private void removeTree(String path) throws Exception {
        for (String child : second.getChildren(path, false)) {
            removeTree(path + "/" + child);
        }
        second.delete(path, 0);
    }

    @Test
    public void recreatedRootVersionZeroCannotAcceptAnOldReadyCas() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            first.beforeMulti.set(() -> {
                try {
                    removeTree(ROOT + "/2c");
                    assertEquals(CreateStatus.APPLIED, store(second).createIfAbsent(reservation()).status());
                    assertEquals(0, second.exists(ROOT + "/2c/lifecycle", false).getVersion());
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(CreateStatus.CONFLICT, store(first).publishReady(prepared.root(), prepared.page(),
                    lm).status());
            var newer = store(second).read(44).snapshot();
            assertNotEquals(prepared.root().lifecycle().birthId(), newer.lifecycle().birthId());
            assertEquals(State.PREPARING, newer.lifecycle().state());
        }
    }

    @Test
    public void recreatedPageVersionZeroCannotBePublishedByAnOldCandidate() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            first.beforeMulti.set(() -> {
                try {
                    removeTree(ROOT + "/2c/initial-activation");
                    var newer = store(second).prepareReady(prepared.root(), prepared.desired(), lm);
                    assertEquals(CreateStatus.APPLIED, newer.status());
                    assertNotEquals(prepared.page().birthId(), newer.page().birthId());
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(CreateStatus.CONFLICT, store(first).publishReady(prepared.root(), prepared.page(),
                    lm).status());
            assertEquals(State.PREPARING, store(second).read(44).snapshot().lifecycle().state());
        }
    }

    @Test
    public void actualMetadataRecreationWithSameDataVersionDoesNotMatchTheReadyBinding() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            var old = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            lm.removeLedgerMetadata(44, old.getVersion()).get(5, TimeUnit.SECONDS);
            new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), reservation().backlink()).get(5,
                    TimeUnit.SECONDS);
            var newer = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(old.getVersion(), newer.getVersion());
            assertNotEquals(old.getValue().getCToken(), newer.getValue().getCToken());
            assertEquals(CreateStatus.CONFLICT, store(first).publishReady(prepared.root(), prepared.page(),
                    lm).status());
        }
    }

    @Test
    public void missingPublishedPageMarkerAndReferencedMetadataFailClosed() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            store(first).publishReady(prepared.root(), prepared.page(), lm);
            var key = ProfileAuthorityKey.fromScope(prepared.desired().activation("b1:3181"));
            var reader = store(second).activationAuthorityReader("b1:3181", lm);
            var actual = lm.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            lm.removeLedgerMetadata(44, actual.getVersion()).get(5, TimeUnit.SECONDS);
            assertEquals(ReadStatus.QUARANTINED, reader.read(key).status());
            second.delete(ROOT + "/2c/initial-activation/birth-" + prepared.page().birthId(), 0);
            assertEquals(CreateStatus.QUARANTINED, store(second).readPublishedPage(44).status());
            assertEquals(ReadStatus.QUARANTINED, reader.read(key).status());
        }
    }
    @Test
    public void submittedReadyTimeoutDoesNotCancelTheActualPublication() throws Exception {
        try (LedgerManager lm = manager(first)) {
            Prepared prepared = prepare(lm);
            var bounded = new ZKProfileReservationStore(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofMillis(200));
            first.holdReply.set(true);
            var pending = CompletableFuture.supplyAsync(() -> bounded.publishReady(prepared.root(),
                    prepared.page(), lm));
            assertTrue(first.replyHeld.await(5, TimeUnit.SECONDS));
            assertEquals(CreateStatus.DURABILITY_UNKNOWN, pending.get(5, TimeUnit.SECONDS).status());
            assertEquals(State.READY, store(second).read(44).snapshot().lifecycle().state());
            first.lateReply.get().run();
            assertEquals(CreateStatus.ALREADY_APPLIED,
                    store(second).publishReady(prepared.root(), prepared.page(), lm).status());
            assertEquals(1, second.exists(ROOT + "/2c/lifecycle", false).getVersion());
        }
    }

    @Test
    public void deleteCasChecksBirthAfterReadEvenWhenRecreatedHeadVersionIsZero() throws Exception {
        var root = store(first).createIfAbsent(reservation()).snapshot();
        first.beforeMulti.set(() -> {
            try {
                removeTree(ROOT + "/2c");
                assertEquals(CreateStatus.APPLIED, store(second).createIfAbsent(reservation()).status());
            } catch (Exception failed) {
                throw new AssertionError(failed);
            }
        });
        assertEquals(CreateStatus.CONFLICT, store(first).cutDelete(root, bytes(16, 5), bytes(32, 6)).status());
        assertEquals(State.PREPARING, store(second).read(44).snapshot().lifecycle().state());
        assertEquals(0, second.exists(ROOT + "/2c/lifecycle", false).getVersion());
    }

}

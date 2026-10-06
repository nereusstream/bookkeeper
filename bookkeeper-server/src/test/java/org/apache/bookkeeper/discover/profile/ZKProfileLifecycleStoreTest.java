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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
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
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.KeeperException;
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

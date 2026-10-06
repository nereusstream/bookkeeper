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
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.MembershipSnapshot;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.Outcome;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

public class ProfileLedgerMetadataStoreTest extends ProfileZKTestBase {
    private static final long LEDGER = 44;

    private static byte[] bytes(int value) {
        byte[] result = new byte[16];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileMetadataBacklink backlink(int instance, int sidecar) {
        return new ProfileMetadataBacklink(LEDGER, bytes(instance), bytes(sidecar));
    }

    private static LedgerMetadata metadata() {
        return LedgerMetadataBuilder.create().withId(LEDGER).withEnsembleSize(3)
                .withWriteQuorumSize(3).withAckQuorumSize(2).withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0]).newEnsembleEntry(0, List.of(
                        BookieId.parse("b1:3181"), BookieId.parse("b2:3181"), BookieId.parse("b3:3181")))
                .withCustomMetadata(Map.of("user", new byte[]{7})).build();
    }

    private LedgerManager manager(TestClient client) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk+longhierarchical://127.0.0.1:2181" + ROOT + "/ledgers");
        var factory = new LongHierarchicalLedgerManagerFactory();
        factory.initialize(conf, new ZkLayoutManager(client, ROOT + "/ledgers", ZooDefs.Ids.OPEN_ACL_UNSAFE),
                factory.getCurrentVersion());
        return factory.newLedgerManager();
    }

    private static void checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }

    @Test
    public void fixedBacklinkVectorAndImmutableCopyRejectMalformedOrDifferentScope() {
        byte[] vector = HexFormat.of().parseHex("5557424c00000001000000000000002c"
                + "0101010101010101010101010101010102020202020202020202020202020202b62a5921");
        ProfileMetadataBacklink value = backlink(1, 2);
        assertArrayEquals(vector, value.encode());
        assertEquals(value, ProfileMetadataBacklink.decode(vector));
        byte[] instance = bytes(1);
        ProfileMetadataBacklink owned = new ProfileMetadataBacklink(LEDGER, instance, bytes(2));
        instance[0] = 0;
        owned.instanceId()[0] = 0;
        assertEquals(value, owned);
        for (int offset : List.of(0, 4, 8, 16, 32)) {
            byte[] corrupt = vector.clone();
            if (offset == 8) {
                ByteBuffer.wrap(corrupt).putLong(offset, -1);
            } else if (offset >= 16) {
                Arrays.fill(corrupt, offset, offset + 16, (byte) 0);
            } else {
                corrupt[offset + 3]++;
            }
            checksum(corrupt);
            assertThrows(IllegalArgumentException.class, () -> ProfileMetadataBacklink.decode(corrupt));
        }
        vector[20]++;
        assertThrows(IllegalArgumentException.class, () -> ProfileMetadataBacklink.decode(vector));
        assertThrows(IllegalArgumentException.class, () -> ProfileMetadataBacklink.decode(new byte[51]));
        LedgerMetadata attached = value.attachTo(metadata());
        assertArrayEquals(new byte[]{7}, attached.getCustomMetadata().get("user"));
        assertEquals(value, ProfileMetadataBacklink.from(value.attachTo(attached)));
        assertThrows(IllegalArgumentException.class, () -> backlink(1, 3).attachTo(attached));
        assertThrows(IllegalArgumentException.class, () -> value.attachTo(
                LedgerMetadataBuilder.from(metadata()).withId(45).build()));
    }

    @Test
    public void realMetadataCreatePreservesUserEntriesAndSameRequestResolvesWithoutAnotherVersion() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager reader = manager(second)) {
            var store = new ProfileLedgerMetadataStore(lm);
            var created = store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.APPLIED, created.outcome());
            var current = reader.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(backlink(1, 2), ProfileMetadataBacklink.from(current.getValue()));
            assertArrayEquals(new byte[]{7}, current.getValue().getCustomMetadata().get("user"));
            var retried = store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.ALREADY_APPLIED, retried.outcome());
            assertEquals(current.getVersion(), retried.metadata().getVersion());
            assertEquals(current.getValue().getCToken(), retried.metadata().getValue().getCToken());
        }
    }

    @Test
    public void actualCreateWithLostReplyResolvesSameInstanceAndDoesNotOverwriteConflict() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager reader = manager(second)) {
            first.loseCreateReplyPath.set(((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER));
            var store = new ProfileLedgerMetadataStore(lm);
            assertEquals(Outcome.ALREADY_APPLIED,
                    store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
            var before = reader.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.CONFLICT,
                    store.createIfAbsent(metadata(), backlink(3, 2)).get(5, TimeUnit.SECONDS).outcome());
            assertEquals(Outcome.CONFLICT,
                    store.createIfAbsent(metadata(), backlink(1, 3)).get(5, TimeUnit.SECONDS).outcome());
            LedgerMetadata altered = LedgerMetadataBuilder.from(metadata()).withAckQuorumSize(3).build();
            assertEquals(Outcome.CONFLICT,
                    store.createIfAbsent(altered, backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
            for (LedgerMetadata changed : List.of(
                    LedgerMetadataBuilder.from(metadata()).withPassword(new byte[]{8}).build(),
                    LedgerMetadataBuilder.from(metadata()).withCustomMetadata(Map.of("user", new byte[]{8})).build(),
                    LedgerMetadataBuilder.from(metadata()).replaceEnsembleEntry(0, List.of(
                            BookieId.parse("b2:3181"), BookieId.parse("b1:3181"), BookieId.parse("b3:3181"))).build(),
                    LedgerMetadataBuilder.from(metadata()).storingCreationTime(true).withCreationTime(123).build())) {
                assertEquals(Outcome.CONFLICT,
                        store.createIfAbsent(changed, backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
            }
            var after = reader.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(before.getVersion(), after.getVersion());
            assertEquals(backlink(1, 2), ProfileMetadataBacklink.from(after.getValue()));
        }
    }

    @Test
    public void simultaneousSameCreateHasOnePhysicalWinnerAndOneMatchingResolution() throws Exception {
        try (LedgerManager a = manager(first); LedgerManager b = manager(second)) {
            var x = new ProfileLedgerMetadataStore(a).createIfAbsent(metadata(), backlink(1, 2));
            var y = new ProfileLedgerMetadataStore(b).createIfAbsent(metadata(), backlink(1, 2));
            List<Outcome> outcomes = List.of(x.get(5, TimeUnit.SECONDS).outcome(),
                    y.get(5, TimeUnit.SECONDS).outcome());
            assertTrue(outcomes.contains(Outcome.APPLIED));
            assertTrue(outcomes.contains(Outcome.ALREADY_APPLIED));
            assertEquals(x.get().metadata().getVersion(), y.get().metadata().getVersion());
        }
    }

    @Test
    public void callbackDelayKeepsOwnedInitialPayloadAndDoesNotTreatRecreatedVersionAsSameInstance() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            first.holdCreateReplyPath.set(((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER));
            LedgerMetadata initial = metadata();
            var pending = new ProfileLedgerMetadataStore(lm).createIfAbsent(initial, backlink(1, 2));
            assertTrue(first.createReplyHeld.await(5, TimeUnit.SECONDS));
            initial.getCustomMetadata().get("user")[0] = 99;
            assertFalse(pending.isDone());
            first.lateCreateReply.get().run();
            assertEquals(Outcome.APPLIED, pending.get(5, TimeUnit.SECONDS).outcome());
            var old = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{7}, old.getValue().getCustomMetadata().get("user"));
            other.removeLedgerMetadata(LEDGER, old.getVersion()).get(5, TimeUnit.SECONDS);
            other.createLedgerMetadata(LEDGER, backlink(3, 4).attachTo(metadata())).get(5, TimeUnit.SECONDS);
            var newer = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(old.getVersion(), newer.getVersion()); // ZooKeeper dataVersion resets to zero.
            assertEquals(Outcome.CONFLICT, new ProfileLedgerMetadataStore(lm)
                    .createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
            assertEquals(backlink(3, 4), ProfileMetadataBacklink.from(
                    other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS).getValue()));
        }
    }

    @Test
    public void acknowledgedCreateRechecksActualMetadataRatherThanItsSubmittedObject() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            first.holdCreateReplyPath.set(((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER));
            var pending = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2));
            assertTrue(first.createReplyHeld.await(5, TimeUnit.SECONDS));
            var created = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            LedgerMetadata changed = LedgerMetadataBuilder.from(created.getValue()).withAckQuorumSize(3).build();
            var current = other.writeLedgerMetadata(LEDGER, changed, created.getVersion()).get(5, TimeUnit.SECONDS);
            first.lateCreateReply.get().run();
            assertEquals(Outcome.CONFLICT, pending.get(5, TimeUnit.SECONDS).outcome());
            assertEquals(current.getVersion(), other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS).getVersion());
        }
    }

    @Test
    public void existingLegacyOrCorruptReferenceCannotBeAdoptedAsMatchingCreation() throws Exception {
        try (LedgerManager lm = manager(first)) {
            var legacy = lm.createLedgerMetadata(LEDGER, metadata()).get(5, TimeUnit.SECONDS);
            var store = new ProfileLedgerMetadataStore(lm);
            assertEquals(Outcome.CONFLICT,
                    store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
            LedgerMetadata corrupt = LedgerMetadataBuilder.from(legacy.getValue()).withCustomMetadata(
                    Map.of(ProfileMetadataBacklink.KEY, new byte[52])).build();
            lm.writeLedgerMetadata(LEDGER, corrupt, legacy.getVersion()).get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.QUARANTINED,
                    store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
        }
    }

    @Test
    public void deletionAfterRealCreateAndLostReplyCannotTurnAbsenceIntoProofOfNoEffect() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            first.loseCreateReplyPath.set(((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER));
            first.beforeLostCreateReply.set(() -> {
                try {
                    var current = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
                    assertEquals(backlink(1, 2), ProfileMetadataBacklink.from(current.getValue()));
                    other.removeLedgerMetadata(LEDGER, current.getVersion()).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(Outcome.DURABILITY_UNKNOWN, new ProfileLedgerMetadataStore(lm)
                    .createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
        }
    }

    private ProfileLedgerMetadataStore membershipStore(TestClient client, LedgerManager lm) {
        return new ProfileLedgerMetadataStore(client, (AbstractZkLedgerManager) lm,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private static List<BookieId> replaced(String name) {
        return List.of(BookieId.parse("b1:3181"), BookieId.parse("b2:3181"), BookieId.parse(name));
    }

    private void removeWithBirthMarker(LedgerManager lm, TestClient client) throws Exception {
        String path = ((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER);
        // A Profile cold mutation owns a child guard. Removing only the parent must fail NONEMPTY.
        for (String child : client.getChildren(path, false)) {
            client.delete(path + "/" + child, -1);
        }
        var actual = lm.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
        lm.removeLedgerMetadata(LEDGER, actual.getVersion()).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void membershipCasPreservesFieldsAndSameStartOverwriteResolvesWithoutExtraVersion() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var store = membershipStore(first, lm);
            var snapshot = store.readMembership(LEDGER).snapshot();
            var changed = store.replaceEnsemble(snapshot, 100, replaced("b4:3181"));
            assertEquals(Outcome.APPLIED, changed.outcome());
            var actual = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(List.of(0L, 100L), List.copyOf(actual.getValue().getAllEnsembles().keySet()));
            assertEquals(replaced("b4:3181"), actual.getValue().getAllEnsembles().get(100L));
            assertEquals(initial.getValue().getCToken(), actual.getValue().getCToken());
            assertEquals(backlink(1, 2), ProfileMetadataBacklink.from(actual.getValue()));
            assertArrayEquals(new byte[]{7}, actual.getValue().getCustomMetadata().get("user"));
            var repeated = store.replaceEnsemble(snapshot, 100, replaced("b4:3181"));
            assertEquals(Outcome.ALREADY_APPLIED, repeated.outcome());
            assertEquals(actual.getVersion(), repeated.metadata().getVersion());
            var overwritten = store.replaceEnsemble(store.readMembership(LEDGER).snapshot(), 100, replaced("b5:3181"));
            assertEquals(Outcome.APPLIED, overwritten.outcome());
            assertEquals(List.of(0L, 100L),
                    List.copyOf(overwritten.metadata().getValue().getAllEnsembles().keySet()));
            assertEquals(replaced("b5:3181"), overwritten.metadata().getValue().getAllEnsembles().get(100L));
            assertEquals(Outcome.CONFLICT, store.replaceEnsemble(snapshot, 100, replaced("b4:3181")).outcome());
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER);
            var children = second.getChildren(path, false);
            assertEquals(1, children.size());
            assertThrows(KeeperException.NoAuthException.class,
                    () -> second.setData(path + "/" + children.get(0), new byte[0], 0));
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> lm.removeLedgerMetadata(LEDGER, overwritten.metadata().getVersion())
                            .get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void membershipLostActualCasReplyResolvesOnlyExactMappingAndVersion() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var snapshot = membershipStore(first, lm).readMembership(LEDGER).snapshot();
            first.loseSetDataMultiReply.set(true);
            var changed = membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b4:3181"));
            assertEquals(Outcome.ALREADY_APPLIED, changed.outcome());
            assertEquals(replaced("b4:3181"), other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS)
                    .getValue().getAllEnsembles().get(0L));
            assertEquals(Outcome.CONFLICT,
                    membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b5:3181")).outcome());
        }
    }

    @Test
    public void membershipCasCannotOverwriteRecreatedVersionZeroRecordAfterItsPrecheck() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var snapshot = membershipStore(first, lm).readMembership(LEDGER).snapshot();
            first.beforeSetDataMulti.set(() -> {
                try {
                    removeWithBirthMarker(other, second);
                    other.createLedgerMetadata(LEDGER, backlink(3, 4).attachTo(metadata())).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(Outcome.CONFLICT,
                    membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
            var actual = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(initial.getVersion(), actual.getVersion());
            assertEquals(backlink(3, 4), ProfileMetadataBacklink.from(actual.getValue()));
            assertEquals(metadata().getAllEnsembles(), actual.getValue().getAllEnsembles());
        }
    }

    @Test
    public void membershipFreezeBetweenPrecheckAndCasWinsWithoutRemovingItsMarker() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var snapshot = membershipStore(first, lm).readMembership(LEDGER).snapshot();
            first.beforeSetDataMulti.set(() -> {
                try {
                    var entries = new HashMap<>(initial.getValue().getCustomMetadata());
                    entries.put(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY, new byte[]{1});
                    other.writeLedgerMetadata(LEDGER, LedgerMetadataBuilder.from(initial.getValue())
                            .withCToken(initial.getValue().getCToken())
                            .withCustomMetadata(entries).build(), initial.getVersion()).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(Outcome.CONFLICT,
                    membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
            var actual = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertTrue(actual.getValue().getCustomMetadata().containsKey(
                    ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY));
            assertEquals(metadata().getAllEnsembles(), actual.getValue().getAllEnsembles());
        }
    }

    @Test
    public void membershipRecoveryClosedAndPublicationTokenRejectBeforeAnyGuardOrWrite() throws Exception {
        try (LedgerManager lm = manager(first)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var snapshot = membershipStore(first, lm).readMembership(LEDGER).snapshot();
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER);
            for (var candidate : List.of(LedgerMetadataBuilder.from(initial.getValue())
                    .withCToken(initial.getValue().getCToken()).withInRecoveryState().build(),
                    LedgerMetadataBuilder.from(initial.getValue()).withCToken(initial.getValue().getCToken())
                            .withClosedState().withLastEntryId(10)
                            .withLength(20).build(),
                    LedgerMetadataBuilder.from(initial.getValue()).withCToken(initial.getValue().getCToken())
                            .withCustomMetadata(Map.of(
                            ProfileMetadataBacklink.KEY, backlink(1, 2).encode(),
                            ProfileLedgerMetadataStore.PENDING_PUBLICATION_KEY, new byte[]{1})).build())) {
                assertEquals(Outcome.CONFLICT, membershipStore(first, lm).replaceEnsemble(
                        new MembershipSnapshot(snapshot.birthZxid(), new Versioned<>(candidate, initial.getVersion())),
                        0, replaced("b4:3181")).outcome());
                assertTrue(first.getChildren(path, false).isEmpty());
            }
            assertThrows(IllegalStateException.class,
                    () -> new ProfileLedgerMetadataStore(lm).replaceEnsemble(snapshot, 0, replaced("b4:3181")));
            assertThrows(IllegalArgumentException.class,
                    () -> membershipStore(first, lm).replaceEnsemble(snapshot, 0, List.of(
                            BookieId.parse("b1:3181"), BookieId.parse("b1:3181"), BookieId.parse("b4:3181"))));
        }
    }

    @Test
    public void membershipMissingRequiredRecordAndUnavailableClientNeverRecreateOrAcknowledge() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            var initial = new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS).metadata();
            var snapshot = membershipStore(first, lm).readMembership(LEDGER).snapshot();
            other.removeLedgerMetadata(LEDGER, initial.getVersion()).get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.QUARANTINED,
                    membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER);
            assertEquals(null, second.exists(path, false));
            first.close();
            assertEquals(Outcome.DURABILITY_UNKNOWN,
                    membershipStore(first, lm).replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
        }
    }

    @Test
    public void membershipRetryKeepsCapturedPhysicalBirthEvenWhenRestoredBytesAreIdentical() throws Exception {
        try (LedgerManager lm = manager(first)) {
            new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2))
                    .get(5, TimeUnit.SECONDS);
            var store = membershipStore(first, lm);
            var snapshot = store.readMembership(LEDGER).snapshot();
            String path = ((AbstractZkLedgerManager) lm).getLedgerPath(LEDGER);
            byte[] restored = new LedgerMetadataSerDe().serialize(snapshot.metadata().getValue());
            first.beforeMulti.set(() -> {
                try {
                    second.delete(path, 0);
                    second.create(path, restored, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(Outcome.CONFLICT, store.replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
            assertEquals(Outcome.CONFLICT, store.replaceEnsemble(snapshot, 0, replaced("b4:3181")).outcome());
            var actual = store.readMembership(LEDGER).snapshot();
            assertTrue(snapshot.birthZxid() != actual.birthZxid());
            assertEquals(snapshot.metadata().getVersion(), actual.metadata().getVersion());
            assertEquals(snapshot.metadata().getValue().getCToken(), actual.metadata().getValue().getCToken());
            assertEquals(snapshot.metadata().getValue().getAllEnsembles(),
                    actual.metadata().getValue().getAllEnsembles());
        }
    }

    @Test
    public void membershipTwoActualCasClientsChooseOneMappingAndOneVersion() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS);
            var a = membershipStore(first, lm);
            var b = membershipStore(second, other);
            var expected = a.readMembership(LEDGER).snapshot();
            var firstCas = CompletableFuture.supplyAsync(() -> a.replaceEnsemble(expected, 0, replaced("b4:3181")));
            var secondCas = CompletableFuture.supplyAsync(() -> b.replaceEnsemble(expected, 0, replaced("b5:3181")));
            var outcomes = List.of(firstCas.get(5, TimeUnit.SECONDS).outcome(),
                    secondCas.get(5, TimeUnit.SECONDS).outcome());
            assertTrue(outcomes.contains(Outcome.APPLIED));
            assertTrue(outcomes.contains(Outcome.CONFLICT));
            var actual = b.readMembership(LEDGER).snapshot();
            assertEquals(expected.birthZxid(), actual.birthZxid());
            assertEquals(expected.metadata().getValue().getCToken(), actual.metadata().getValue().getCToken());
            var mapping = actual.metadata().getValue().getAllEnsembles().get(0L);
            assertTrue(mapping.equals(replaced("b4:3181")) || mapping.equals(replaced("b5:3181")));
            assertEquals(1, ((LongVersion) actual.metadata().getVersion())
                    .getLongVersion());
        }
    }

    @Test
    public void membershipLaterVersionWithSameMappingDoesNotResolveTheOldCasAsCurrentAuthority() throws Exception {
        try (LedgerManager lm = manager(first); LedgerManager other = manager(second)) {
            new ProfileLedgerMetadataStore(lm).createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS);
            var store = membershipStore(first, lm);
            var expected = store.readMembership(LEDGER).snapshot();
            first.loseSetDataMultiReply.set(true);
            first.beforeLostMultiReply.set(() -> {
                try {
                    var actual = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
                    other.writeLedgerMetadata(LEDGER, actual.getValue(), actual.getVersion()).get(5, TimeUnit.SECONDS);
                } catch (Exception failed) {
                    throw new AssertionError(failed);
                }
            });
            assertEquals(Outcome.CONFLICT, store.replaceEnsemble(expected, 0, replaced("b4:3181")).outcome());
            var actual = other.readLedgerMetadata(LEDGER).get(5, TimeUnit.SECONDS);
            assertEquals(replaced("b4:3181"), actual.getValue().getAllEnsembles().get(0L));
            assertEquals(2, ((LongVersion) actual.getVersion()).getLongVersion());
        }
    }

    @Test
    public void closedClientAndInvalidInitialShapeNeverReturnCreationSuccess() throws Exception {
        try (LedgerManager lm = manager(first)) {
            var store = new ProfileLedgerMetadataStore(lm);
            LedgerMetadata wrong = LedgerMetadataBuilder.from(metadata()).withDigestType(DigestType.MAC).build();
            assertThrows(IllegalArgumentException.class, () -> store.createIfAbsent(wrong, backlink(1, 2)));
            first.close();
            assertEquals(Outcome.DURABILITY_UNKNOWN,
                    store.createIfAbsent(metadata(), backlink(1, 2)).get(5, TimeUnit.SECONDS).outcome());
        }
    }
    @Test
    public void creationResultDoesNotExposeStandardMetadataPasswordOrCustomBytes() throws Exception {
        try (LedgerManager lm = manager(first)) {
            LedgerMetadata input = LedgerMetadataBuilder.from(metadata()).withPassword(new byte[]{81, 82, 83, 84})
                    .withCustomMetadata(Map.of("private-value", new byte[]{91, 92, 93})).build();
            var result = new ProfileLedgerMetadataStore(lm).createIfAbsent(input, backlink(1, 2))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.APPLIED, result.outcome());
            assertFalse(result.toString().contains("password"));
            assertFalse(result.toString().contains("UVJTVA=="));
            assertFalse(result.toString().contains("private-value"));
        }
    }

}

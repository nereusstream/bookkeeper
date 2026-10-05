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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.Outcome;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
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
}

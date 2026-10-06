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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore.CreateStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

public class ZKProfileReservationStoreTest extends ProfileZKTestBase {
    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileControlScope scope(String bookie, int incarnation, long generation) {
        return scope(ProfileControlOperation.INSTALL, bookie, incarnation, generation, bytes(16, 1), "d");
    }

    private static ProfileControlScope scope(ProfileControlOperation op, String bookie, int incarnation,
            long generation, byte[] instance, String domain) {
        byte[] descriptor = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1)
                .put(bytes(32, 9)).array();
        return new ProfileControlScope(op, op.purpose(), 44, instance, ProfileDescriptorIdentity.fromBytes(descriptor),
                bookie, bytes(16, incarnation), domain, generation, bytes(16, 3), bytes(32, 4), null);
    }

    private static ProfileReservation reservation() {
        return new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 2)), List.of(
                scope("b1:3181", 2, 8), scope("b2:3181", 5, 8), scope("b3:3181", 6, 8)));
    }

    private static ZKProfileReservationStore store(TestClient client) {
        return new ZKProfileReservationStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private static void checksum(byte[] value) {
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length - 4);
        ByteBuffer.wrap(value).putInt(value.length - 4, (int) crc.getValue());
    }

    @Test
    public void fixedRootVectorRejectsMandatoryFieldsCountsAndTruncation() {
        ProfileReservation value = new ProfileReservation(reservation().backlink(), List.of(scope("b", 2, 8)));
        byte[] vector = HexFormat.of().parseHex(
                "55575052000000010000010e0000000100000000000000005557424c00000001000000000000002c01010101010101010101"
                + "01010101010102020202020202020202020202020202b62a592100000001000000b65557415500000001000000b600000001"
                + "00000001000000010000000100000003000000000000002c0101010101010101010101010101010100010001090909090909"
                + "0909090909090909090909090909090909090909090909090909020202020202020202020202020202020000000000000008"
                + "0303030303030303030303030303030304040404040404040404040404040404040404040404040404040404040404040000"
                + "000162000000016400000000731b2114a6f89345");
        assertArrayEquals(vector, ProfileReservationCodec.encode(value));
        assertEquals(value, ProfileReservationCodec.decode(vector));
        for (int offset : List.of(0, 4, 8, 12, 16, 76, 80)) {
            byte[] changed = vector.clone();
            ByteBuffer.wrap(changed).putInt(offset, offset == 76 ? 33 : -1);
            checksum(changed);
            assertThrows(IllegalArgumentException.class, () -> ProfileReservationCodec.decode(changed));
        }
        vector[100]++;
        assertThrows(IllegalArgumentException.class, () -> ProfileReservationCodec.decode(vector));
        assertThrows(IllegalArgumentException.class, () -> ProfileReservationCodec.decode(new byte[83]));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileReservationCodec.decode(new byte[ProfileReservationCodec.MAX_BYTES + 1]));
    }

    @Test
    public void rootOwnsTargetListAndEnforcesBoundedConsistentInstallIdentity() {
        ArrayList<ProfileControlScope> targets = new ArrayList<>(reservation().installs());
        ProfileReservation owned = new ProfileReservation(reservation().backlink(), targets);
        targets.clear();
        assertEquals(reservation(), owned);
        assertThrows(UnsupportedOperationException.class, () -> owned.installs().clear());
        assertThrows(IllegalArgumentException.class, () -> new ProfileReservation(owned.backlink(), List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ProfileReservation(owned.backlink(),
                List.of(scope("b", 2, 8), scope("b", 3, 8))));
        assertThrows(IllegalArgumentException.class, () -> new ProfileReservation(owned.backlink(),
                List.of(scope("b", 2, 8), scope("c", 3, 9))));
        assertThrows(IllegalArgumentException.class, () -> new ProfileReservation(owned.backlink(), List.of(
                scope(ProfileControlOperation.ACTIVATE_INITIAL, "b", 2, 8, bytes(16, 1), "d"))));
        List<ProfileControlScope> maximum = new ArrayList<>();
        for (int index = 0; index < ProfileReservation.MAX_INITIAL_TARGETS; index++) {
            maximum.add(scope("界".repeat(83) + String.format("%06d", index), 2, 8));
        }
        var bounded = new ProfileReservation(owned.backlink(), maximum);
        byte[] encoded = ProfileReservationCodec.encode(bounded);
        assertTrue(encoded.length <= ProfileReservationCodec.MAX_BYTES);
        assertEquals(bounded, ProfileReservationCodec.decode(encoded));
        maximum.add(scope("one-more", 2, 8));
        assertThrows(IllegalArgumentException.class, () -> new ProfileReservation(owned.backlink(), maximum));
    }

    @Test
    public void actualImmutableCreateFeedsEveryTargetThroughTheExistingColdInterface() throws Exception {
        var writer = store(first);
        assertEquals(ReadStatus.NOT_FOUND, writer.read(44).status());
        var created = writer.createIfAbsent(reservation());
        assertEquals(CreateStatus.APPLIED, created.status());
        assertEquals(reservation(), store(second).read(44).snapshot().reservation());
        long version = second.exists(writer.path(44), false).getMzxid();
        assertEquals(version, created.snapshot().storeVersion());
        assertEquals(CreateStatus.ALREADY_APPLIED, writer.createIfAbsent(reservation()).status());
        assertEquals(0, second.exists(writer.path(44), false).getVersion());
        for (ProfileControlScope scope : reservation().installs()) {
            var read = store(second).installAuthorityReader(scope.targetBookie())
                    .read(ProfileAuthorityKey.fromScope(scope));
            assertEquals(ReadStatus.FOUND, read.status());
            assertEquals(scope, read.authority().scope());
            assertEquals(version, read.authority().opaqueStoreVersion());
            assertTrue(read.authority().permits(ProfileControlOperation.INSTALL));
            assertFalse(read.authority().permits(ProfileControlOperation.ACTIVATE_INITIAL));
        }
        var scope = reservation().installs().get(0);
        assertEquals(ReadStatus.NOT_FOUND, writer.installAuthorityReader("other").read(
                ProfileAuthorityKey.fromScope(scope)).status());
        for (ProfileControlScope wrongKey : List.of(
                scope(ProfileControlOperation.ACTIVATE_INITIAL, scope.targetBookie(), 2, 8, bytes(16, 1), "d"),
                scope(ProfileControlOperation.INSTALL, scope.targetBookie(), 2, 8, bytes(16, 7), "d"),
                scope(ProfileControlOperation.INSTALL, scope.targetBookie(), 2, 8, bytes(16, 1), "other-domain"))) {
            assertEquals(ReadStatus.NOT_FOUND, writer.installAuthorityReader(scope.targetBookie())
                    .read(ProfileAuthorityKey.fromScope(wrongKey)).status());
        }
        assertEquals(List.of("2c"), second.getChildren(ROOT, false));
    }

    @Test
    public void sameOperationWithConflictingSemanticContentNeverOverwritesTheRoot() throws Exception {
        var writer = store(first);
        writer.createIfAbsent(reservation());
        var original = store(second).read(44).snapshot();
        List<ProfileControlScope> reversed = new ArrayList<>(reservation().installs());
        java.util.Collections.reverse(reversed);
        for (ProfileReservation conflict : List.of(
                new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 7)),
                        reservation().installs()),
                new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 7), bytes(16, 2)), List.of(
                        scope(ProfileControlOperation.INSTALL, "b1:3181", 2, 8, bytes(16, 7), "d"))),
                new ProfileReservation(reservation().backlink(), reversed),
                new ProfileReservation(reservation().backlink(), List.of(scope("other", 2, 8))),
                new ProfileReservation(reservation().backlink(), List.of(scope("b1:3181", 7, 8))),
                new ProfileReservation(reservation().backlink(), List.of(scope("b1:3181", 2, 9))))) {
            assertEquals(CreateStatus.CONFLICT, writer.createIfAbsent(conflict).status());
            assertEquals(original, store(second).read(44).snapshot());
        }
    }

    @Test
    public void twoActualClientsHaveOnePhysicalCreatorAndResolveTheSameCommittedRequest() throws Exception {
        var a = CompletableFuture.supplyAsync(() -> store(first).createIfAbsent(reservation()));
        var b = CompletableFuture.supplyAsync(() -> store(second).createIfAbsent(reservation()));
        List<CreateStatus> outcomes = List.of(a.get(10, TimeUnit.SECONDS).status(),
                b.get(10, TimeUnit.SECONDS).status());
        assertTrue(outcomes.contains(CreateStatus.APPLIED));
        assertTrue(outcomes.contains(CreateStatus.ALREADY_APPLIED));
        assertEquals(a.get().snapshot(), b.get().snapshot());
    }

    @Test
    public void actualLostCreateReplyAndRetryResolveWithoutAnotherPhysicalWrite() throws Exception {
        var writer = store(first);
        first.loseReply.set(true);
        assertEquals(CreateStatus.ALREADY_APPLIED, writer.createIfAbsent(reservation()).status());
        assertEquals(CreateStatus.ALREADY_APPLIED, store(second).createIfAbsent(reservation()).status());
        assertEquals(0, second.exists(writer.path(44), false).getVersion());
    }

    @Test
    public void acknowledgedReplyStillRequiresMatchingActualRootRatherThanSubmittedBytes() throws Exception {
        var writer = store(first);
        first.holdReply.set(true);
        var pending = CompletableFuture.supplyAsync(() -> writer.createIfAbsent(reservation()));
        assertTrue(first.replyHeld.await(5, TimeUnit.SECONDS));
        ProfileReservation other = new ProfileReservation(reservation().backlink(), List.of(scope("other", 2, 8)));
        second.setData(writer.path(44), ProfileReservationCodec.encode(other), 0);
        first.lateReply.get().run();
        assertEquals(CreateStatus.QUARANTINED, pending.get(5, TimeUnit.SECONDS).status());
        assertEquals(ReadStatus.QUARANTINED, store(second).read(44).status());
    }

    @Test
    public void deletedCommittedReservationCannotBeResolvedAsNoEffect() throws Exception {
        var writer = store(first);
        first.loseReply.set(true);
        first.beforeLostMultiReply.set(() -> {
            try {
                String head = writer.path(44) + "/lifecycle";
                for (String child : second.getChildren(head, false)) {
                    second.delete(head + "/" + child, 0);
                }
                second.delete(head, 0);
                second.delete(writer.path(44), 0);
            } catch (Exception failed) {
                throw new AssertionError(failed);
            }
        });
        assertEquals(CreateStatus.DURABILITY_UNKNOWN, writer.createIfAbsent(reservation()).status());
        assertEquals(ReadStatus.NOT_FOUND, store(second).read(44).status());
    }

    @Test
    public void timeoutAfterRealCreateLeavesItsAuthorityQueryableWithoutCancellingTheWrite() throws Exception {
        var writer = new ZKProfileReservationStore(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofMillis(200));
        first.holdReply.set(true);
        var pending = CompletableFuture.supplyAsync(() -> writer.createIfAbsent(reservation()));
        assertTrue(first.replyHeld.await(5, TimeUnit.SECONDS));
        assertEquals(CreateStatus.DURABILITY_UNKNOWN, pending.get(5, TimeUnit.SECONDS).status());
        assertEquals(reservation(), store(second).read(44).snapshot().reservation());
        first.lateReply.get().run();
        assertEquals(CreateStatus.ALREADY_APPLIED, store(first).createIfAbsent(reservation()).status());
        assertEquals(0, second.exists(writer.path(44), false).getVersion());
    }

    @Test
    public void malformedEphemeralWrongLedgerAndUnauthorizedRootsFailClosed() throws Exception {
        var writer = store(first);
        String path = writer.path(44);
        first.create(path, new byte[84], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(ReadStatus.QUARANTINED, store(second).read(44).status());
        assertEquals(CreateStatus.QUARANTINED, writer.createIfAbsent(reservation()).status());
        first.delete(path, 0);
        first.create(path, ProfileReservationCodec.encode(reservation()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        assertEquals(ReadStatus.QUARANTINED, store(second).read(44).status());
        first.delete(path, 0);
        first.create(writer.path(45), ProfileReservationCodec.encode(reservation()), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertEquals(ReadStatus.QUARANTINED, store(second).read(45).status());
        first.addAuthInfo("digest", "reservation-owner:opaque-test".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        first.create(path, ProfileReservationCodec.encode(reservation()), ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.PERSISTENT);
        assertEquals(ReadStatus.UNAVAILABLE, store(second).read(44).status());
        assertEquals(CreateStatus.DURABILITY_UNKNOWN, store(second).createIfAbsent(reservation()).status());
        first.close();
        assertEquals(ReadStatus.UNAVAILABLE, writer.read(44).status());
        assertEquals(CreateStatus.DURABILITY_UNKNOWN, writer.createIfAbsent(reservation()).status());
    }
}

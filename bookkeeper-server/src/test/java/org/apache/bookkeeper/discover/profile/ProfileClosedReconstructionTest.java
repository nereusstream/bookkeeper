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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.junit.Test;

public class ProfileClosedReconstructionTest {
    @Test
    public void legacyAdmissionKeepsItsExactNativeBytes() throws Exception {
        byte[] bytes = legacyBytes();
        var legacy = ProfileClosedReconstruction.Admission.decode(bytes);
        assertEquals(44, legacy.members().get(0).ledger());
        assertNull(legacy.members().get(0).context());
        assertEquals(admission(List.of(new ProfileClosedReconstruction.Member(44, null,
                "01".repeat(32), "02".repeat(32)))), legacy);
        assertArrayEquals(bytes, legacy.encode());
    }

    private static byte[] legacyBytes() {
        return HexFormat.of().parseHex(
                "5557434100000001000b626f6f6b69653a3331383109090909090909090909090909090909000000"
                + "00000000000000000000000001000000000000000200000000000000000000000000000002000000"
                + "000000002c000000000000000a000000000000000b000000000000000c000000000000000d000000"
                + "000000000e00062f6172656e61000000000000000100000000000000010000000000000001000000"
                + "0000000002000000000000000100000000000000030000000000001000"
                + "04".repeat(32) + "03".repeat(32) + "01".repeat(32) + "02".repeat(32));
    }

    @Test
    public void cohortAdmissionRequiresBoundedCanonicalCompleteMemberBytes() throws Exception {
        var admission = admission(List.of(member(44), member(45)));
        byte[] encoded = admission.encode();
        assertEquals(2, ByteBuffer.wrap(encoded).getInt(4));
        assertEquals(admission, ProfileClosedReconstruction.Admission.decode(encoded));
        int rowBytes = ProfileLedgerContext.ENCODED_LENGTH + 64;
        byte[] legacy = legacyBytes();
        int ledgerOffset = 77;
        int commonBytes = legacy.length - Long.BYTES - 64;
        var golden = ByteBuffer.allocate(commonBytes + Integer.BYTES + 2 * rowBytes)
                .put(legacy, 0, ledgerOffset)
                .put(legacy, ledgerOffset + Long.BYTES, commonBytes - ledgerOffset)
                .putInt(2);
        golden.putInt(4, 2);
        for (long ledger : new long[] {44, 45}) {
            golden.putLong(ledger).put(bytes(16, 1)).put(bytes(36, 2)).put(bytes(32, 1)).put(bytes(32, 2));
        }
        assertArrayEquals(golden.array(), encoded);
        int firstRow = encoded.length - 2 * rowBytes;
        byte[] reversed = encoded.clone();
        System.arraycopy(encoded, firstRow + rowBytes, reversed, firstRow, rowBytes);
        System.arraycopy(encoded, firstRow, reversed, firstRow + rowBytes, rowBytes);
        assertThrows(IllegalArgumentException.class, () -> ProfileClosedReconstruction.Admission.decode(reversed));
        byte[] duplicate = encoded.clone();
        System.arraycopy(encoded, firstRow, duplicate, firstRow + rowBytes, rowBytes);
        assertThrows(IllegalArgumentException.class, () -> ProfileClosedReconstruction.Admission.decode(duplicate));
        byte[] tooMany = encoded.clone();
        ByteBuffer.wrap(tooMany).putInt(firstRow - Integer.BYTES, ProfileClosedReconstruction.MAX_MEMBERS + 1);
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(tooMany));
        byte[] unknown = encoded.clone();
        ByteBuffer.wrap(unknown).putInt(4, 5);
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(unknown));
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(
                Arrays.copyOf(encoded, 8193)));
    }

    @Test
    public void mixedVersionRequiresBothKnownMemberKinds() throws Exception {
        var original = member(44);
        var empty = new ProfileClosedReconstruction.Member(44, original.context(), original.closed(),
                original.sources(), ProfileClosedReconstruction.Kind.EMPTY_TERMINAL);
        var mixed = admission(List.of(empty, member(45)));
        byte[] encoded = mixed.encode();
        assertEquals(3, ByteBuffer.wrap(encoded).getInt(4));
        assertEquals(mixed, ProfileClosedReconstruction.Admission.decode(encoded));
        int rowBytes = 1 + ProfileLedgerContext.ENCODED_LENGTH + 64;
        int first = encoded.length - 2 * rowBytes;
        assertEquals(2, encoded[first]);
        assertEquals(1, encoded[first + rowBytes]);
        byte[] previous = admission(List.of(original, member(45))).encode();
        var golden = ByteBuffer.allocate(previous.length + 2).put(previous, 0, first);
        golden.putInt(4, 3);
        golden.put((byte) 2).put(previous, first, rowBytes - 1)
                .put((byte) 1).put(previous, first + rowBytes - 1, rowBytes - 1);
        assertArrayEquals(golden.array(), encoded);
        byte[] unknown = encoded.clone();
        unknown[first] = 3;
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(unknown));
        byte[] onlySelf = encoded.clone();
        onlySelf[first] = 1;
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(onlySelf));
        byte[] onlyEmpty = encoded.clone();
        onlyEmpty[first + rowBytes] = 2;
        assertThrows(IllegalArgumentException.class, () -> ProfileClosedReconstruction.Admission.decode(onlyEmpty));
        assertThrows(IllegalArgumentException.class, () -> ProfileClosedReconstruction.Admission.decode(
                Arrays.copyOf(encoded, encoded.length - 1)));
    }

    @Test
    public void readyOneRetainsItsExactNativeBytes() throws Exception {
        var ready = ready(List.of(file(0, 4, 11)));
        byte[] golden = ByteBuffer.allocate(148).putInt(0x55574359).putInt(1)
                .putLong(0).putLong(2).putLong(16).put(bytes(32, 9)).putLong(0).putLong(3)
                .putLong(16).putInt(1).putLong(1).putLong(4).putLong(4096).put(bytes(32, 11)).array();
        assertArrayEquals(golden, ready.encode());
        assertEquals(ready, ProfileClosedReconstruction.Ready.decode(golden));
    }

    @Test
    public void fileVectorsRequireEveryOrderedDistinctMatchingObject() throws Exception {
        var original0 = file(0, 3, 11);
        var original1 = file(1, 4, 12);
        var originals = List.of(original0, original1);
        var base = admission(List.of(new ProfileClosedReconstruction.Member(44, null,
                "01".repeat(32), "02".repeat(32))));
        var admission = new ProfileClosedReconstruction.Admission(base.scope(), base.operation(), base.arenaCut(),
                base.bookieCut(), base.publisherPid(), base.publisherStartMillis(), base.publisherSession(),
                base.directory(), base.directoryObject(), base.stagingObject(), originals, base.dependencies(),
                List.of(member(44)));
        byte[] encoded = admission.encode();
        assertEquals(4, ByteBuffer.wrap(encoded).getInt(4));
        assertEquals(admission, ProfileClosedReconstruction.Admission.decode(encoded));
        byte[] tooFew = encoded.clone();
        int vectorCountOffset = legacyBytes().length - 64 - 32 - 56 - Long.BYTES;
        ByteBuffer.wrap(tooFew).putInt(vectorCountOffset, 1);
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(tooFew));

        var fresh0 = file(0, 5, 11);
        var fresh1 = file(1, 6, 12);
        var ready = ready(List.of(fresh0, fresh1));
        assertEquals(2, ByteBuffer.wrap(ready.encode()).getInt(4));
        assertEquals(ready, ProfileClosedReconstruction.Ready.decode(ready.encode()));
        assertTrue(admission.matchesCandidates(ready.candidates()));
        assertFalse(admission.matchesCandidates(List.of(fresh0)));
        assertFalse(admission.matchesCandidates(List.of(fresh0, file(1, 3, 12))));
        assertFalse(admission.matchesCandidates(List.of(fresh0, file(1, 6, 11))));
        assertThrows(IllegalArgumentException.class, () -> ready(List.of(fresh1, fresh0)));
        assertThrows(IllegalArgumentException.class, () -> ready(List.of(fresh0, file(1, 5, 12))));
    }

    @Test
    public void committedMoveAdmissionUsesTheTypedFileVectorEvenForOneFileAndMember() throws Exception {
        var original = file(0, 3, 11);
        var member = member(44);
        var base = admission(List.of(new ProfileClosedReconstruction.Member(44, null,
                member.closed(), member.sources())));
        var moved = new ProfileClosedReconstruction.Admission(base.scope(), base.operation(), base.arenaCut(),
                base.bookieCut(), base.publisherPid(), base.publisherStartMillis(), base.publisherSession(),
                base.directory(), base.directoryObject(), base.stagingObject(), List.of(original),
                base.dependencies(), List.of(member), true);
        byte[] encoded = moved.encode();
        assertEquals(5, ByteBuffer.wrap(encoded).getInt(4));
        assertEquals(moved, ProfileClosedReconstruction.Admission.decode(encoded));
        int vectorCount = legacyBytes().length - 64 - 32 - 56 - Long.BYTES;
        assertEquals(1, ByteBuffer.wrap(encoded).getInt(vectorCount));
        assertEquals(0, ByteBuffer.wrap(encoded).getInt(vectorCount + Integer.BYTES));
        assertEquals(1, ByteBuffer.wrap(encoded).getInt(vectorCount + 2 * Integer.BYTES + 56 + 32));
        byte[] downgrade = encoded.clone();
        ByteBuffer.wrap(downgrade).putInt(4, 4);
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(downgrade));
        assertTrue(moved.matchesCandidates(ready(List.of(file(0, 4, 11))).candidates()));
        var retired = new ProfileClosedReconstruction.Admission(base.scope(), base.operation(), base.arenaCut(),
                base.bookieCut(), base.publisherPid(), base.publisherStartMillis(), base.publisherSession(),
                base.directory(), base.directoryObject(), base.stagingObject(), List.of(original),
                base.dependencies(), List.of(member), ProfileClosedReconstruction.HistoryMode.RETIRED);
        byte[] retiredGolden = encoded.clone();
        ByteBuffer.wrap(retiredGolden).putInt(4, 6);
        assertArrayEquals(retiredGolden, retired.encode());
        assertEquals(retired, ProfileClosedReconstruction.Admission.decode(retiredGolden));
        var sealed = new ProfileClosedReconstruction.Admission(base.scope(), base.operation(), base.arenaCut(),
                base.bookieCut(), base.publisherPid(), base.publisherStartMillis(), base.publisherSession(),
                base.directory(), base.directoryObject(), base.stagingObject(), List.of(original),
                base.dependencies(), List.of(member), ProfileClosedReconstruction.HistoryMode.FULLY_SEALED_REUSE);
        byte[] sealedGolden = retiredGolden.clone();
        ByteBuffer.wrap(sealedGolden).putInt(4, 7);
        assertArrayEquals(sealedGolden, sealed.encode());
        assertEquals(sealed, ProfileClosedReconstruction.Admission.decode(sealedGolden));
    }

    private static ProfileClosedReconstruction.Ready ready(List<ProfileClosedReconstruction.FileImage> files) {
        return new ProfileClosedReconstruction.Ready(new UUID(0, 2), 16, "09".repeat(32),
                new UUID(0, 3), 16, 1, files);
    }

    private static ProfileClosedReconstruction.FileImage file(int index, long inode, int digestByte) {
        var image = new ProfileClosedReconstruction.Image(new FileIdentity(1, inode), 4096,
                HexFormat.of().formatHex(bytes(32, digestByte)));
        return new ProfileClosedReconstruction.FileImage(index, image);
    }

    private static ProfileClosedReconstruction.Member member(long ledger) {
        var context = new ProfileLedgerContext(ledger, bytes(16, 1), bytes(36, 2));
        return new ProfileClosedReconstruction.Member(ledger, context, "01".repeat(32), "02".repeat(32));
    }

    private static ProfileClosedReconstruction.Admission admission(List<ProfileClosedReconstruction.Member> members) {
        var scope = new ProfileClosedReconstruction.Scope("bookie:3181", new StorageIncarnation(bytes(16, 9)),
                new UUID(0, 1), 2);
        var image = new ProfileClosedReconstruction.Image(new FileIdentity(1, 3), 4096, "04".repeat(32));
        return new ProfileClosedReconstruction.Admission(scope, new UUID(0, 2), 10, 11, 12, 13, 14, "/arena",
                new FileIdentity(1, 1), new FileIdentity(1, 2), image, "03".repeat(32), members);
    }

    private static byte[] bytes(int size, int value) {
        byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }
}

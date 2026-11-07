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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

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
        ByteBuffer.wrap(unknown).putInt(4, 4);
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

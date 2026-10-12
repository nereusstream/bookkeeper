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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.junit.Test;

public class ProfileDeleteSnapshotTest {
    private static byte[] bytes(int size, int value) {
        byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileDeleteSnapshot.Effect effect(long sequence) {
        byte[] descriptor = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1)
                .put(bytes(32, 9)).array();
        var event = new ProfileDeleteDelivery.Event(bytes(16, 1), 1, 1, sequence,
                new ProfileLedgerContext(sequence, bytes(16, 2), descriptor),
                new ProfileDeleteRecord.Target("b1:3181", bytes(16, 3)),
                new ProfileDeleteRecord.Reference(100, bytes(32, 4), bytes(32, 5), bytes(16, 6)),
                bytes(16, 7), 1);
        return new ProfileDeleteSnapshot.Effect(sequence + 100, event);
    }

    @Test
    public void completePagesHaveCanonicalBoundsAndRejectGapsCrossScopeAndCorruptLengths() {
        var effects = new ArrayList<ProfileDeleteSnapshot.Effect>();
        for (long sequence = 1; sequence <= 65; sequence++) {
            effects.add(effect(sequence));
        }
        var first = new ProfileDeleteSnapshot.Page(bytes(16, 8), 0, effects.subList(0, 64));
        var last = new ProfileDeleteSnapshot.Page(bytes(16, 8), 1, effects.subList(64, 65));
        var cut = new ProfileDeleteDelivery.Head(bytes(16, 1), 1, 1, 65, effects.get(0).event().target());
        var root = new ProfileDeleteSnapshot.Root(bytes(16, 8), 1, cut,
                List.of(ProfileRepairIntent.digest(first.encode()), ProfileRepairIntent.digest(last.encode())));
        assertTrue(first.matches(root));
        assertTrue(last.matches(root));
        assertArrayEquals(root.encode(), ProfileDeleteSnapshot.Root.decode(root.encode()).encode());
        assertArrayEquals(first.encode(), ProfileDeleteSnapshot.Page.decode(first.encode()).encode());
        byte[] owned = root.pageDigests().get(0);
        owned[0] ^= 1;
        assertTrue(first.matches(root));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteSnapshot.Page(
                bytes(16, 8), 0, List.of(effect(2))));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteSnapshot.Root(
                bytes(16, 8), 1, cut, List.of(ProfileRepairIntent.digest(first.encode()))));
        var wrongTarget = new ProfileDeleteDelivery.Head(bytes(16, 1), 1, 1, 65,
                new ProfileDeleteRecord.Target("b1:3181", bytes(16, 11)));
        assertFalse(first.matches(new ProfileDeleteSnapshot.Root(bytes(16, 8), 1, wrongTarget, root.pageDigests())));
        for (int size = 0; size < root.encode().length; size++) {
            byte[] truncated = Arrays.copyOf(root.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteSnapshot.Root.decode(truncated));
        }
        for (int size = 0; size < last.encode().length; size++) {
            byte[] truncated = Arrays.copyOf(last.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteSnapshot.Page.decode(truncated));
        }
        byte[] excessive = last.encode();
        ByteBuffer.wrap(excessive).putInt(28, Integer.MAX_VALUE);
        CRC32C crc = new CRC32C();
        crc.update(excessive, 0, excessive.length - 4);
        ByteBuffer.wrap(excessive).putInt(excessive.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteSnapshot.Page.decode(excessive));
    }
}

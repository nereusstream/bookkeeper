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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfilePhysicalDeleteCodec;
import org.junit.Test;

/** Receipt syntax only. Native mTLS/ZK/file collection is covered in SegmentProfileInitialSocketClientTest. */
public class ProfileDeletePhysicalReceiptTest {
    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileDeleteRecord.Manifest manifest() {
        byte[] descriptor = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1)
                .put(bytes(32, 9)).array();
        var context = new ProfileLedgerContext(44, bytes(16, 1), descriptor);
        return new ProfileDeleteRecord.Manifest(context,
                new ProfileDeleteRecord.Seal(bytes(16, 3), 8, 1, 100, 200, bytes(32, 4)),
                List.of(new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2))));
    }

    private static ProfileDeleteDelivery.Event event() {
        var manifest = manifest();
        var reference = new ProfileDeleteRecord.Reference(300, ProfileRepairIntent.digest(manifest.encode()),
                ProfileRepairIntent.digest(manifest.seal().encode()), bytes(16, 6));
        return new ProfileDeleteDelivery.Event(bytes(16, 5), 1, 1, 1, manifest.ledger(),
                manifest.targets().get(0), reference, manifest.seal().requestId(), manifest.seal().epoch());
    }

    private static ProfileHello.Server hello(byte[] incarnation, List<ProfileHello.Capability> capabilities) {
        return new ProfileHello.Server(3, incarnation, 0, "b1:3181", capabilities);
    }

    @Test
    public void boundedReceiptRetainsExactEventRequestAndReplyAndRejectsCorruption() {
        var event = event();
        var request = new ProfilePhysicalDeleteCodec.Request(event.deleteApply("d"), 64, 8, 5000);
        var reply = new ProfilePhysicalDeleteCodec.Reply(
                ProfilePhysicalDeleteCodec.status(ProfilePhysicalDeleteCodec.Outcome.REUSABLE),
                ProfilePhysicalDeleteCodec.Outcome.REUSABLE, 4, 1, 1, 4096, 0, 10, 20);
        var receipt = ProfileDeletePhysicalReceipt.fromVerifiedReply(event,
                hello(bytes(16, 2), List.of(ProfilePhysicalDeleteCodec.CAPABILITY)), request, reply);
        assertTrue(receipt.matches(manifest(), "d"));
        var decoded = ProfileDeletePhysicalReceipt.decode(receipt.encode());
        assertEquals(event, decoded.event());
        assertEquals(reply, decoded.reply());
        assertArrayEquals(receipt.encode(), decoded.encode());
        byte[] owned = receipt.birthId();
        owned[0] ^= 1;
        assertArrayEquals(receipt.encode(), decoded.encode());
        for (int size = 0; size < receipt.encode().length; size++) {
            byte[] cut = Arrays.copyOf(receipt.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeletePhysicalReceipt.decode(cut));
        }
        for (int offset : List.of(0, 4, 6, 24)) {
            byte[] wrong = receipt.encode();
            ByteBuffer.wrap(wrong).putInt(offset, Integer.MAX_VALUE);
            CRC32C crc = new CRC32C();
            crc.update(wrong, 0, wrong.length - 4);
            ByteBuffer.wrap(wrong).putInt(wrong.length - 4, (int) crc.getValue());
            assertThrows(IllegalArgumentException.class, () -> ProfileDeletePhysicalReceipt.decode(wrong));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileDeletePhysicalReceipt.fromVerifiedReply(event,
                hello(bytes(16, 7), List.of(ProfilePhysicalDeleteCodec.CAPABILITY)), request, reply));
        assertThrows(IllegalArgumentException.class, () -> ProfileDeletePhysicalReceipt.fromVerifiedReply(event,
                hello(bytes(16, 2), List.of()), request, reply));
        var pending = new ProfilePhysicalDeleteCodec.Reply(
                ProfilePhysicalDeleteCodec.status(ProfilePhysicalDeleteCodec.Outcome.PENDING),
                ProfilePhysicalDeleteCodec.Outcome.PENDING, 0, 0, 0, 0, 0, -1, -1);
        assertThrows(IllegalArgumentException.class, () -> ProfileDeletePhysicalReceipt.fromVerifiedReply(event,
                hello(bytes(16, 2), List.of(ProfilePhysicalDeleteCodec.CAPABILITY)), request, pending));
    }
}

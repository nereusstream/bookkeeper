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

package org.apache.bookkeeper.common.profile.wire;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProfileReadEntryReplyCodecTest {
    private static final ProfileLedgerContext CONTEXT = new ProfileLedgerContext(44, filled(16, 1), filled(36, 2));

    @Test
    public void fixedOffsetsBindTheEntryAndMissingIsDistinctFromUnknown() throws Exception {
        ByteBuffer entry = ByteBuffer.allocate(40).putLong(44).putLong(7).putLong(6).putLong(4)
                .putInt(0x12345678).putInt(0x09080706);
        ByteBuffer body = ByteBuffer.allocate(128);
        ProfileReadEntryReplyCodec.writePrefix(body.slice(0, 88), CONTEXT, 7,
                ProfileReadEntryReplyCodec.Outcome.FOUND, 40, 40);
        body.position(88).put(entry.array()).flip();
        assertArrayEquals(vector("found"), body.array());
        assertEquals(0x55575244, body.getInt(8));
        assertEquals(1, body.getShort(12));
        assertEquals(1, body.getShort(14));
        byte[] context = new byte[60];
        body.get(16, context);
        assertArrayEquals(CONTEXT.encode(), context);
        assertEquals(7, body.getLong(76));
        assertEquals(40, body.getInt(84));
        var found = ProfileReadEntryReplyCodec.decode(body, CONTEXT, 7, 40);
        assertEquals(ProfileReadEntryReplyCodec.Outcome.FOUND, found.outcome());
        assertEquals(entry.flip(), found.entry());
        assertFalse(found.entry().hasArray());
        var borrowed = ProfileReadEntryReplyCodec.decodeView(body, CONTEXT, 7, 40);
        assertEquals(found.entry(), borrowed.entry());
        assertFalse(borrowed.entry().hasArray());
        borrowed.entry().position(10);
        assertEquals(0, borrowed.entry().position());
        body.put(127, (byte) 0);
        assertEquals(0x09080706, found.entry().getInt(36));
        assertEquals(0x09080700, borrowed.entry().getInt(36));

        ByteBuffer missing = ByteBuffer.allocate(88);
        ProfileReadEntryReplyCodec.writePrefix(missing, CONTEXT, 7,
                ProfileReadEntryReplyCodec.Outcome.MISSING, 0, 40);
        missing.flip();
        assertArrayEquals(vector("missing"), missing.array());
        var absent = ProfileReadEntryReplyCodec.decode(missing, CONTEXT, 7, 40);
        assertEquals(ProfileReadEntryReplyCodec.Outcome.MISSING, absent.outcome());
        assertEquals(0, absent.entry().remaining());
        assertEquals(ProfileStatus.DurableResult.NONE, absent.status().durableResult());
        var unknown = new ProfileStatus(ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                ProfileStatus.RetryDisposition.AFTER_CONTROL_RECONCILIATION, ProfileStatus.DurableResult.NONE, 0);
        assertArrayEquals(vector("not-ready"), unknown.encode());
        var refused = ProfileReadEntryReplyCodec.decode(ByteBuffer.wrap(vector("not-ready")), CONTEXT, 7, 40);
        assertEquals(ProfileReadEntryReplyCodec.Outcome.REFUSED, refused.outcome());
    }

    @Test
    public void malformedScopeShapeAndPayloadIdentityFailBeforeAnEntryCopy() {
        ByteBuffer body = ByteBuffer.allocate(128);
        ProfileReadEntryReplyCodec.writePrefix(body.slice(0, 88), CONTEXT, 7,
                ProfileReadEntryReplyCodec.Outcome.FOUND, 40, 40);
        body.position(88).putLong(44).putLong(7).put(new byte[24]).flip();
        assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.decode(body, CONTEXT, 8, 40));
        assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.decode(body,
                new ProfileLedgerContext(44, filled(16, 3), filled(36, 2)), 7, 40));
        int[] offsets = {8, 12, 14, 84, 88, 96};
        for (int offset : offsets) {
            ByteBuffer bad = ByteBuffer.wrap(body.array().clone());
            bad.put(offset, (byte) (bad.get(offset) ^ 1));
            assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.decode(bad, CONTEXT, 7, 40));
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileReadEntryReplyCodec.decodeView(bad, CONTEXT, 7, 40));
        }
        assertThrows(IllegalArgumentException.class,
                () -> ProfileReadEntryReplyCodec.decode(body.slice(0, 127), CONTEXT, 7, 40));
        assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.decode(body, CONTEXT, 7, 39));
        assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.writePrefix(
                ByteBuffer.allocate(88), CONTEXT, 7, ProfileReadEntryReplyCodec.Outcome.MISSING, 40, 40));
        assertThrows(IllegalArgumentException.class, () -> ProfileReadEntryReplyCodec.refused(
                new ProfileStatus(ProfileStatus.StatusClass.OK, ProfileStatus.RetryDisposition.NEVER,
                        ProfileStatus.DurableResult.NONE, 0)));
    }

    private static byte[] vector(String name) throws Exception {
        return Files.readAllBytes(Path.of(ProfileReadEntryReplyCodecTest.class.getResource(
                "/profile-wire/read-entry-reply/" + name + ".bin").toURI()));
    }

    private static byte[] filled(int length, int value) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }
}

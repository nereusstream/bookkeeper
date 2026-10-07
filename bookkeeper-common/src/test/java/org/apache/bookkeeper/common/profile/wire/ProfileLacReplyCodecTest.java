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
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProfileLacReplyCodecTest {
    private static final ProfileLedgerContext CONTEXT = new ProfileLedgerContext(44, filled(16, 1), filled(36, 2));

    @Test
    public void independentVectorsPreserveSourceLengthAsSourceLength() throws Exception {
        byte[] source = vector("source");
        var decoded = ProfileLacReplyCodec.decode(ByteBuffer.wrap(source), CONTEXT, 40);
        assertEquals(ProfileLacReplyCodec.Outcome.SOURCE, decoded.outcome());
        assertEquals(100, decoded.sourceEntryId());
        assertEquals(99, decoded.piggybackLac());
        assertEquals(1100, decoded.sourceLength());
        ByteBuffer written = ByteBuffer.allocate(128);
        ProfileLacReplyCodec.writePrefix(written.slice(0, 88), CONTEXT, 100,
                ProfileLacReplyCodec.Outcome.SOURCE, 40, 40);
        written.position(88).put(decoded.entry());
        assertArrayEquals(source, written.array());
        var empty = ProfileLacReplyCodec.decode(ByteBuffer.wrap(vector("empty")), CONTEXT, 40);
        assertEquals(ProfileLacReplyCodec.Outcome.EMPTY, empty.outcome());
        assertEquals(-1, empty.sourceEntryId());
        assertEquals(-1, empty.piggybackLac());
        assertEquals(0, empty.sourceLength());
        ByteBuffer emptyOutput = ByteBuffer.allocate(88);
        ProfileLacReplyCodec.writePrefix(emptyOutput, CONTEXT, -1, ProfileLacReplyCodec.Outcome.EMPTY, 0, 40);
        assertArrayEquals(vector("empty"), emptyOutput.array());
        source[127] = 0;
        assertEquals(0x09080706, decoded.entry().getInt(36));
    }

    @Test
    public void malformedSourceCannotBecomeEvidenceOrEmpty() throws Exception {
        byte[] source = vector("source");
        for (int offset : new int[] {8, 12, 14, 24, 76, 84, 88, 96}) {
            byte[] bad = source.clone();
            bad[offset] ^= 1;
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileLacReplyCodec.decode(ByteBuffer.wrap(bad), CONTEXT, 40));
        }
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decode(ByteBuffer.wrap(source, 0, 127), CONTEXT, 40));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decode(ByteBuffer.wrap(source), CONTEXT, 39));
        assertThrows(IllegalArgumentException.class, () -> ProfileLacReplyCodec.writePrefix(
                ByteBuffer.allocate(88), CONTEXT, 100, ProfileLacReplyCodec.Outcome.EMPTY, 0, 40));
        var refused = new ProfileStatus(ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                ProfileStatus.RetryDisposition.AFTER_CONTROL_RECONCILIATION, ProfileStatus.DurableResult.NONE, 0);
        assertEquals(ProfileLacReplyCodec.Outcome.REFUSED,
                ProfileLacReplyCodec.decode(ByteBuffer.wrap(refused.encode()), CONTEXT, 40).outcome());
    }

    private static byte[] vector(String name) throws Exception {
        return Files.readAllBytes(Path.of(ProfileLacReplyCodecTest.class.getResource(
                "/profile-wire/lac-source/" + name + ".bin").toURI()));
    }

    private static byte[] filled(int length, int value) {
        byte[] data = new byte[length];
        java.util.Arrays.fill(data, (byte) value);
        return data;
    }
}

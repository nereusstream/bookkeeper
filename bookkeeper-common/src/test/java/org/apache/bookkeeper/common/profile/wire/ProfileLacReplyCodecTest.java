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
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

public class ProfileLacReplyCodecTest {
    private static final ProfileLedgerContext CONTEXT = new ProfileLedgerContext(44, filled(16, 1), filled(36, 2));

    @Test
    public void independentExplicitVectorsKeepTwoWitnessesAndDoNotInventLength() throws Exception {
        byte[] combined = vector("explicit-source");
        var reply = ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(combined), CONTEXT, 40);
        var borrowed = ProfileLacReplyCodec.decodeExplicitView(ByteBuffer.wrap(combined), CONTEXT, 40);
        assertEquals(reply.entry(), borrowed.entry());
        assertEquals(reply.explicitBody(), borrowed.explicitBody());
        assertTrue(borrowed.entry().isReadOnly());
        assertTrue(borrowed.explicitBody().isReadOnly());
        borrowed.entry().position(10);
        borrowed.explicitBody().position(10);
        assertEquals(0, borrowed.entry().position());
        assertEquals(0, borrowed.explicitBody().position());
        assertEquals(100, reply.sourceEntryId());
        assertEquals(99, reply.piggybackLac());
        assertEquals(1100, reply.sourceLength());
        assertEquals(101, reply.explicitLac());
        assertTrue(reply.hasExplicitLac());
        ByteBuffer written = ByteBuffer.allocate(combined.length);
        ProfileLacReplyCodec.writeExplicitPrefix(written.slice(0, 100), CONTEXT, 100, 40, 101, 20, 40);
        written.position(100).put(reply.entry()).put(reply.explicitBody());
        assertArrayEquals(combined, written.array());
        var only = ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(vector("explicit-only")), CONTEXT, 40);
        assertEquals(ProfileLacReplyCodec.Outcome.EXPLICIT, only.outcome());
        assertEquals(-1, only.sourceEntryId());
        assertEquals(0, only.sourceLength());
        assertEquals(101, only.explicitLac());
        assertEquals(ProfileLacReplyCodec.Outcome.EMPTY,
                ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(vector("explicit-empty")), CONTEXT, 40).outcome());
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decode(ByteBuffer.wrap(combined), CONTEXT, 40));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(vector("source")), CONTEXT, 40));
        for (int offset : new int[] {12, 14, 76, 84, 88, 96, 140, 148}) {
            byte[] bad = combined.clone();
            bad[offset] ^= 1;
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(bad), CONTEXT, 40));
        }
        for (int length = 0; length < combined.length; length++) {
            ByteBuffer truncated = ByteBuffer.wrap(combined, 0, length);
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileLacReplyCodec.decodeExplicit(truncated, CONTEXT, 40));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileLacReplyCodec.decodeExplicit(
                ByteBuffer.wrap(java.util.Arrays.copyOf(combined, combined.length + 1)), CONTEXT, 40));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(combined), CONTEXT, 39));
        byte[] oversizedExplicit = combined.clone();
        ByteBuffer.wrap(oversizedExplicit).putInt(96, -1);
        assertThrows(IllegalArgumentException.class,
                () -> ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(oversizedExplicit), CONTEXT, 40));
        combined[combined.length - 1] ^= 1;
        assertEquals(vector("explicit-source")[159], reply.explicitBody().get(19));
        assertEquals(combined[159], borrowed.explicitBody().get(19));
    }

    @Test
    public void explicitSchemaAlsoSupportsSourceOnlyAndExactRefusal() throws Exception {
        var legacy = ProfileLacReplyCodec.decode(ByteBuffer.wrap(vector("source")), CONTEXT, 40);
        ByteBuffer source = ByteBuffer.allocate(140);
        ProfileLacReplyCodec.writeExplicitPrefix(source.slice(0, 100), CONTEXT, 100, 40, -1, 0, 40);
        source.position(100).put(legacy.entry());
        var reply = ProfileLacReplyCodec.decodeExplicit(source.flip(), CONTEXT, 40);
        assertEquals(ProfileLacReplyCodec.Outcome.SOURCE, reply.outcome());
        assertEquals(false, reply.hasExplicitLac());
        assertEquals(1100, reply.sourceLength());
        var refused = ProfileStatus.unsupported();
        assertEquals(ProfileLacReplyCodec.Outcome.REFUSED,
                ProfileLacReplyCodec.decodeExplicit(ByteBuffer.wrap(refused.encode()), CONTEXT, 40).outcome());
        assertThrows(IllegalArgumentException.class, () -> ProfileLacReplyCodec.decodeExplicit(
                ByteBuffer.wrap(java.util.Arrays.copyOf(refused.encode(), 9)), CONTEXT, 40));
    }

    @Test
    public void independentVectorsPreserveSourceLengthAsSourceLength() throws Exception {
        byte[] source = vector("source");
        var decoded = ProfileLacReplyCodec.decode(ByteBuffer.wrap(source), CONTEXT, 40);
        var borrowed = ProfileLacReplyCodec.decodeView(ByteBuffer.wrap(source), CONTEXT, 40);
        assertEquals(decoded.entry(), borrowed.entry());
        assertEquals(decoded.sourceEntryId(), borrowed.sourceEntryId());
        assertEquals(decoded.piggybackLac(), borrowed.piggybackLac());
        assertEquals(decoded.sourceLength(), borrowed.sourceLength());
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
        assertEquals(0x09080700, borrowed.entry().getInt(36));
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

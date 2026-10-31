/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Disposition;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Frame;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Kind;
import org.junit.Test;

/** Independent Python fixed vectors and negative framing only; no listener, peer or accepted operation. */
public class SegmentClassicLauncherOperationCodecTest {
    private static final UUID EPISODE = UUID.fromString("01020304-0506-0708-090a-0b0c0d0e0f10");
    private static final UUID BINDING = UUID.fromString("11121314-1516-1718-191a-1b1c1d1e1f20");
    private static final List<String> VECTORS = List.of(
            "5557434c4f30303100010100000000100102030405060708090a0b0c0d0e0f10",
            "5557434c4f30303100010200000000100102030405060708090a0b0c0d0e0f10",
            "5557434c4f30303100010300000000200102030405060708090a0b0c0d0e0f10"
                    + "1112131415161718191a1b1c1d1e1f20",
            "5557434c4f30303100010400000000140102030405060708090a0b0c0d0e0f1000000001",
            "5557434c4f30303100010400000000140102030405060708090a0b0c0d0e0f1000000002");

    @Test
    public void independentVectorsPreserveTheEpisodeBindingAndAcceptanceOnlyReply() throws Exception {
        List<Frame> expected = List.of(new Frame(Kind.GREETING, EPISODE, null, null),
                new Frame(Kind.STOP, EPISODE, null, null), new Frame(Kind.HANDOFF, EPISODE, BINDING, null),
                new Frame(Kind.REPLY, EPISODE, null, Disposition.ACCEPTED),
                new Frame(Kind.REPLY, EPISODE, null, Disposition.REJECTED));
        for (int i = 0; i < VECTORS.size(); i++) {
            byte[] bytes = HexFormat.of().parseHex(VECTORS.get(i));
            Frame decoded = SegmentClassicLauncherOperationCodec.decode(bytes);
            assertEquals(expected.get(i), decoded);
            assertArrayEquals(bytes, SegmentClassicLauncherOperationCodec.encode(expected.get(i)));
            assertEquals(bytes.length - 16,
                    SegmentClassicLauncherOperationCodec.bodyBytes(Arrays.copyOf(bytes, 16)));
            decoded.requireEpisode(EPISODE);
            assertThrows(IOException.class, () -> decoded.requireEpisode(BINDING));
        }
    }

    @Test
    public void invalidHeadersAreRejectedBeforeAnyBodyReservation() throws Exception {
        byte[] stop = HexFormat.of().parseHex(VECTORS.get(1));
        for (int offset = 0; offset < 12; offset++) {
            byte[] corrupted = Arrays.copyOf(stop, 16);
            corrupted[offset] ^= (byte) 0x80;
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.bodyBytes(corrupted));
        }
        for (int invalid : List.of(-1, 0, 15, 17, 32, Integer.MAX_VALUE)) {
            byte[] header = Arrays.copyOf(stop, 16);
            ByteBuffer.wrap(header).putInt(12, invalid);
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.bodyBytes(header));
        }
        for (byte[] invalid : List.of(new byte[0], new byte[15], new byte[17])) {
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.bodyBytes(invalid));
        }
        assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.bodyBytes(null));
        assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(null));
        assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(new byte[49]));
    }

    @Test
    public void truncatedTrailingOrInvalidOperationBodiesCannotProduceAFrame() throws Exception {
        for (String vector : VECTORS) {
            byte[] bytes = HexFormat.of().parseHex(vector);
            for (int length = 0; length < bytes.length; length++) {
                byte[] truncated = Arrays.copyOf(bytes, length);
                assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(truncated));
            }
            assertThrows(IOException.class,
                    () -> SegmentClassicLauncherOperationCodec.decode(Arrays.copyOf(bytes, bytes.length + 1)));
            Arrays.fill(bytes, 16, 32, (byte) 0);
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(bytes));
        }
        byte[] handoff = HexFormat.of().parseHex(VECTORS.get(2));
        Arrays.fill(handoff, 32, 48, (byte) 0);
        assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(handoff));
        for (int invalid : List.of(0, 3, 256, -1)) {
            byte[] reply = HexFormat.of().parseHex(VECTORS.get(3));
            ByteBuffer.wrap(reply).putInt(32, invalid);
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(reply));
        }
        assertThrows(IllegalArgumentException.class, () -> new Frame(Kind.HANDOFF, EPISODE, null, null));
        assertThrows(IllegalArgumentException.class, () -> new Frame(Kind.STOP, EPISODE, BINDING, null));
        assertThrows(IllegalArgumentException.class,
                () -> new Frame(Kind.STOP, EPISODE, null, Disposition.ACCEPTED));
    }

    @Test
    public void childLaunchMessagesCannotBeDecodedAsOperations() throws Exception {
        for (String name : List.of("classic-launch-hello-v1", "classic-launch-start-v1")) {
            try (var input = getClass().getResourceAsStream("/unified-wal-segment/" + name + ".hex")) {
                byte[] bytes = HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                        .trim());
                assertThrows(IOException.class, () -> SegmentClassicLauncherOperationCodec.decode(bytes));
                assertThrows(IOException.class,
                        () -> SegmentClassicLauncherOperationCodec.bodyBytes(Arrays.copyOf(bytes, 16)));
            }
        }
    }
}

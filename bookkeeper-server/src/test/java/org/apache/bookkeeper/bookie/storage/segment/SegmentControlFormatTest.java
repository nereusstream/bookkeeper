/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentControlFormatTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void controlVectorSurvivesRealFileRoundTrip() throws Exception {
        ByteBuffer body = new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                new UUID(1, 2), new Generation(1), 0, 0, 0, new Generation(1),
                5, 4096, 1, 11).body();
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.ALLOC, new Generation(1), 0,
                new UUID(1, 2), new Generation(1), 0, body);
        byte[] golden;
        try (var input = getClass().getResourceAsStream("/unified-wal-segment/arena-alloc-v1.hex")) {
            golden = HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                    .lines().filter(line -> !line.startsWith("#")).collect(Collectors.joining()));
        }
        byte[] encoded = new byte[record.remaining()];
        record.duplicate().get(encoded);
        assertArrayEquals(golden, encoded);
        try (FileChannel file = FileChannel.open(temporary.newFile().toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            SegmentFileIo io = new SegmentFileIo(3, -1, -1, false);
            io.writeFully(file, record, 0);
            io.force(file, true);
            ByteBuffer read = ByteBuffer.allocate(encoded.length);
            SegmentFileIo.readFully(file, read, 0);
            SegmentControlParser.Command parsed = SegmentControlParser.parse(read,
                    SegmentControlFormat.Owner.ARENA, new Generation(1), 0);
            assertEquals(SegmentControlFormat.Type.ALLOC, parsed.type());
            assertEquals(new UUID(1, 2), parsed.operationId());
            assertEquals(body, parsed.body());
        }
    }

    @Test
    public void ownerSequenceGenerationAndEveryTruncationAreRejected() {
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.FENCE, new Generation(3), 9,
                new UUID(1, 2), new Generation(1), 0, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        assertThrows(IOException.class, () -> SegmentControlParser.parse(record, SegmentControlFormat.Owner.ARENA,
                new Generation(3), 9));
        assertThrows(IOException.class, () -> SegmentControlParser.parse(record, SegmentControlFormat.Owner.BOOKIE,
                new Generation(4), 9));
        assertThrows(IOException.class, () -> SegmentControlParser.parse(record, SegmentControlFormat.Owner.BOOKIE,
                new Generation(3), 10));
        for (int i = 0; i < record.remaining(); i++) {
            ByteBuffer truncated = record.slice(0, i);
            assertThrows(IOException.class, () -> SegmentControlParser.parse(truncated,
                    SegmentControlFormat.Owner.BOOKIE, new Generation(3), 9));
        }
    }

    @Test
    public void everySingleByteCorruptionIsDetected() {
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.ALLOC, new Generation(1), 0,
                new UUID(1, 2), new Generation(1), 0, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        for (int i = 0; i < record.remaining(); i++) {
            ByteBuffer corrupt = ByteBuffer.allocate(record.remaining()).put(record.duplicate()).flip();
            corrupt.put(i, (byte) (corrupt.get(i) ^ 1));
            assertThrows(IOException.class, () -> SegmentControlParser.parse(corrupt,
                    SegmentControlFormat.Owner.ARENA, new Generation(1), 0));
        }
        assertThrows(IllegalArgumentException.class, () -> SegmentControlFormat.encode(SegmentControlFormat.Type.ALLOC,
                new Generation(1), 0, new UUID(1, 2), new Generation(1), 0, ByteBuffer.allocate(65536)));
    }

    @Test
    public void preflightPreservesTheBodyAtAlignmentAndMaximumLengthBoundaries() throws Exception {
        for (int length : new int[] {0, 1, 7, 8, 9, 65385, 65391, 65392}) {
            ByteBuffer body = ByteBuffer.allocate(length + 3).position(3).asReadOnlyBuffer();
            body.mark();
            int size = SegmentControlFormat.encodedLength(SegmentControlFormat.Type.FENCE,
                    new Generation(3), 9, new UUID(1, 2), new Generation(1), 0, body);
            assertEquals(144 + (length + 7) / 8 * 8, size);
            assertEquals(3, body.position());
            assertEquals(length, body.remaining());
            body.reset();
            ByteBuffer encoded = SegmentControlFormat.encode(SegmentControlFormat.Type.FENCE,
                    new Generation(3), 9, new UUID(1, 2), new Generation(1), 0, body);
            assertEquals(size, encoded.remaining());
            assertEquals(body, SegmentControlParser.parse(encoded, SegmentControlFormat.Owner.BOOKIE,
                    new Generation(3), 9).body());
            assertEquals(3, body.position());
            body.reset();
        }
    }

    @Test
    public void preflightRejectsInvalidFramingInputs() {
        var type = SegmentControlFormat.Type.FENCE;
        var generation = new Generation(1);
        var operation = new UUID(1, 2);
        ByteBuffer body = ByteBuffer.allocate(0);
        assertThrows(IllegalArgumentException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, -1, operation, generation, 0, body));
        assertThrows(IllegalArgumentException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, operation, generation, -1, body));
        assertThrows(IllegalArgumentException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, new UUID(0, 0), generation, 0, body));
        assertThrows(IllegalArgumentException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, operation, generation, 0, ByteBuffer.allocate(65393)));
        assertThrows(NullPointerException.class, () -> SegmentControlFormat.encodedLength(
                null, generation, 0, operation, generation, 0, body));
        assertThrows(NullPointerException.class, () -> SegmentControlFormat.encodedLength(
                type, null, 0, operation, generation, 0, body));
        assertThrows(NullPointerException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, null, generation, 0, body));
        assertThrows(NullPointerException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, operation, null, 0, body));
        assertThrows(NullPointerException.class, () -> SegmentControlFormat.encodedLength(
                type, generation, 0, operation, generation, 0, null));
    }
}

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Independent bounded reader: validates bytes without calling the writer or its validation helpers. */
final class SegmentBatchParser {
    record Record(ProfileLedgerContext context, long entryId, long lac, long ledgerLength,
                  int origin, UUID moveId, long moveGeneration, long offset, ByteBuffer entry) {
        Record {
            entry = entry.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer entry() {
            return entry.asReadOnlyBuffer();
        }
    }

    record Batch(SegmentBatchFormat.Address address, int length, List<Record> records) {
        Batch {
            records = List.copyOf(records);
        }
    }

    static int batchLength(ByteBuffer input, SegmentBatchFormat.Address expected, long fileCapacity)
            throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        require(b.remaining() >= 128, "Truncated batch header");
        require(b.getLong(0) == 0x5557444154413031L && b.getInt(8) == 1 && b.getInt(12) == 128,
                "Unknown batch format");
        require(crc(b, 0, 124) == b.getInt(124), "Batch header CRC mismatch");
        zero(b, 84, 124);
        require(b.getInt(16) == expected.fileIndex() && b.getInt(20) == expected.shard()
                && b.getLong(24) == expected.allocation() && b.getLong(32) == expected.generation().value()
                && b.getLong(40) == expected.shard() && b.getLong(48) == expected.streamGeneration().value()
                && b.getLong(56) == expected.sequence() && b.getLong(64) == expected.offset(),
                "Batch identity or generation mismatch");
        int length = b.getInt(72);
        require(length >= 4096 && length <= 2 * 1024 * 1024 && length % 4096 == 0
                && expected.offset() <= fileCapacity && length <= fileCapacity - expected.offset(),
                "Batch length outside authorized file");
        int count = b.getInt(76);
        int recordBytes = b.getInt(80);
        require(count > 0 && count <= 256 && recordBytes >= (long) count * 196
                && recordBytes <= length - 160, "Invalid record count or region length");
        return length;
    }

    static Batch parse(ByteBuffer input, SegmentBatchFormat.Address expected, long fileCapacity) throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        int length = batchLength(b, expected, fileCapacity);
        require(b.remaining() == length, "Truncated or extra batch bytes");
        int footer = length - 32;
        require(b.getLong(footer) == 0x5557454e44303031L && b.getLong(footer + 8) == expected.sequence()
                && b.getInt(footer + 16) == length && b.getInt(footer + 24) == b.getInt(124)
                && b.getInt(footer + 28) == crc(b, footer, 28), "Invalid or torn batch footer");
        require(b.getInt(footer + 20) == crc(b, 128, footer - 128), "Batch payload CRC mismatch");
        int count = b.getInt(76);
        int end = 128 + b.getInt(80);
        List<Record> records = new ArrayList<>(count);
        int position = 128;
        for (int i = 0; i < count; i++) {
            require(end - position >= 160, "Truncated record header");
            int recordLength = recordLength(b.slice(position, end - position));
            require(recordLength <= end - position, "Truncated record");
            records.add(parseRecord(b.slice(position, recordLength), expected, expected.offset() + position));
            position += recordLength;
        }
        require(position == end, "Record count and region disagree");
        zero(b, end, footer);
        return new Batch(expected, length, records);
    }

    // Safe to call after reading only the fixed 160-byte header, before allocating a record body.
    static int recordLength(ByteBuffer input) throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        require(b.remaining() >= 160, "Truncated record header");
        require(b.getInt(0) == 0x55575231 && b.getShort(4) == 1 && b.get(7) == 0,
                "Unknown record format");
        require(b.getInt(156) == crc(b, 0, 156), "Record header CRC mismatch");
        zero(b, 148, 156);
        int length = b.getInt(8);
        int entryLength = b.getInt(12);
        require(entryLength >= 36 && entryLength <= 1024 * 1024 + 36
                && length == 160L + entryLength, "Invalid record length");
        return length;
    }

    static Record parseRecord(ByteBuffer input, SegmentBatchFormat.Address expected, long recordOffset)
            throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        int length = recordLength(b);
        require(b.remaining() == length && recordOffset >= expected.offset()
                && recordOffset - expected.offset() >= 128
                && recordOffset - expected.offset() <= 2 * 1024 * 1024 - 32 - length,
                "Invalid record extent");
        require(b.getLong(16) == expected.allocation() && b.getLong(24) == expected.generation().value()
                && b.getLong(32) == recordOffset && b.getInt(124) == expected.fileIndex()
                && b.getInt(128) == expected.shard() && b.getLong(132) == expected.streamGeneration().value()
                && b.getLong(140) == expected.sequence(), "Record identity or generation mismatch");
        int origin = Byte.toUnsignedInt(b.get(6));
        UUID move = new UUID(b.getLong(100), b.getLong(108));
        long moveGeneration = b.getLong(116);
        boolean zeroMove = move.getMostSignificantBits() == 0 && move.getLeastSignificantBits() == 0;
        require(origin == 1 ? zeroMove && moveGeneration == 0 : origin == 2 && !zeroMove && moveGeneration > 0,
                "Unknown origin or invalid relocation identity");
        byte[] contextBytes = new byte[60];
        b.get(40, contextBytes);
        ProfileLedgerContext context;
        try {
            context = ProfileLedgerContext.decode(contextBytes);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid ledger context", e);
        }
        ByteBuffer entry = b.slice(160, length - 160).order(ByteOrder.BIG_ENDIAN);
        SegmentEntryIdentity identity = SegmentEntryIdentity.parse(context, entry);
        return new Record(context, identity.entryId(), identity.lac(), identity.ledgerLength(),
                origin, move, moveGeneration, recordOffset, entry);
    }

    private static int crc(ByteBuffer b, int offset, int length) {
        CRC32C checksum = new CRC32C();
        checksum.update(b.slice(offset, length));
        return (int) checksum.getValue();
    }

    private static void zero(ByteBuffer b, int from, int to) throws IOException {
        for (int i = from; i < to; i++) {
            require(b.get(i) == 0, "Nonzero reserved field or padding");
        }
    }

    private static void require(boolean condition, String reason) throws IOException {
        if (!condition) {
            throw new IOException(reason);
        }
    }

    private SegmentBatchParser() {
    }
}

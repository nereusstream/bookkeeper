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
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Independent control reader. Semantic validation and ordered conditional apply belong to each owner. */
final class SegmentControlParser {
    record Command(SegmentControlFormat.Type type, UUID operationId, Generation operationGeneration,
                   long expectedGeneration, ByteBuffer body) {
        Command {
            body = body.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer body() {
            return body.asReadOnlyBuffer();
        }
    }

    static int recordLength(ByteBuffer input, SegmentControlFormat.Owner owner, Generation logGeneration,
                            long sequence) throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        require(b.remaining() >= 128, "Truncated control header");
        require(b.getLong(0) == 0x555743544c303031L && b.getInt(8) == 1 && b.getInt(12) == owner.code
                && b.getInt(68) == 128, "Unknown control format or owner");
        require(b.getInt(124) == crc(b, 0, 124), "Control header CRC mismatch");
        int length = b.getInt(20);
        int bodyLength = b.getInt(64);
        require(length >= 144 && length <= 65536 && length % 8 == 0
                && bodyLength >= 0 && bodyLength <= length - 144
                && length - 144 - bodyLength < 8, "Invalid control length");
        require(sequence >= 0 && b.getLong(24) == sequence && b.getLong(72) == logGeneration.value(),
                "Control sequence gap or generation mismatch");
        require((b.getLong(32) != 0 || b.getLong(40) != 0) && b.getLong(48) > 0 && b.getLong(56) >= 0,
                "Invalid control operation identity");
        zero(b, 80, 124);
        type(owner, b.getInt(16));
        return length;
    }

    static Command parse(ByteBuffer input, SegmentControlFormat.Owner owner, Generation logGeneration, long sequence)
            throws IOException {
        ByteBuffer b = input.slice().order(ByteOrder.BIG_ENDIAN);
        int length = recordLength(b, owner, logGeneration, sequence);
        require(b.remaining() == length, "Truncated or extra control bytes");
        int bodyLength = b.getInt(64);
        int footer = length - 16;
        require(b.getInt(footer) == 0x43544c45 && b.getLong(footer + 8) == sequence
                && b.getInt(footer + 4) == crc(b, 128, footer - 128), "Control body or footer mismatch");
        zero(b, 128 + bodyLength, footer);
        return new Command(type(owner, b.getInt(16)), new UUID(b.getLong(32), b.getLong(40)),
                new Generation(b.getLong(48)), b.getLong(56), b.slice(128, bodyLength));
    }

    private static SegmentControlFormat.Type type(SegmentControlFormat.Owner owner, int code) throws IOException {
        for (SegmentControlFormat.Type type : SegmentControlFormat.Type.values()) {
            if (type.owner == owner && type.code == code) {
                return type;
            }
        }
        throw new IOException("Unsupported control command");
    }

    private static int crc(ByteBuffer b, int start, int length) {
        CRC32C checksum = new CRC32C();
        checksum.update(b.slice(start, length));
        return (int) checksum.getValue();
    }

    private static void zero(ByteBuffer b, int start, int end) throws IOException {
        for (int i = start; i < end; i++) {
            require(b.get(i) == 0, "Unsupported control extension or nonzero padding");
        }
    }

    private static void require(boolean condition, String reason) throws IOException {
        if (!condition) {
            throw new IOException(reason);
        }
    }

    private SegmentControlParser() {
    }
}

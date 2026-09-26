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
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;

/** Initial immutable superblock/file identity; checkpoint and rotation generations are not enabled. */
record SegmentFileHeader(StorageIncarnation incarnation, UUID arenaId, BookieId bookieId) {
    SegmentFileHeader {
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(arenaId, "arenaId");
        Objects.requireNonNull(bookieId, "bookieId");
        if (arenaId.equals(new UUID(0, 0)) || bookieId.toString().isEmpty()
                || bookieId.toString().indexOf(0) >= 0
                || bookieId.toString().getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("Invalid isolated storage identity");
        }
    }

    ByteBuffer encode(IsolatedSegmentConfig config, int fileId) {
        ByteBuffer b = ByteBuffer.allocate(4096);
        byte[] bookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        b.putLong(0x555753544f523031L).putInt(1).putInt(fileId).put(incarnation.value())
                .putLong(arenaId.getMostSignificantBits()).putLong(arenaId.getLeastSignificantBits())
                .putLong(1).putLong(1).putLong(1).putLong(1).putLong(0)
                .putInt(config.dataFiles()).putInt(config.logicalShards()).putLong(config.bytesPerFile())
                .putInt(config.alignment()).putInt(1024 * 1024).putLong(1)
                .putInt(bookie.length).put(bookie);
        CRC32C crc = new CRC32C();
        crc.update(b.slice(0, 4092));
        b.putInt(4092, (int) crc.getValue());
        b.clear();
        return b;
    }

    void validate(ByteBuffer input, IsolatedSegmentConfig config, int fileId) throws IOException {
        ByteBuffer b = input.slice();
        if (b.remaining() != 4096) {
            throw new IOException("Truncated identity block");
        }
        CRC32C crc = new CRC32C();
        crc.update(b.slice(0, 4092));
        if (b.getInt(4092) != (int) crc.getValue()) {
            throw new IOException("Identity block CRC mismatch");
        }
        byte[] expectedBookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        if (b.getLong(0) != 0x555753544f523031L || b.getInt(8) != 1 || b.getInt(12) != fileId
                || b.getLong(32) != arenaId.getMostSignificantBits()
                || b.getLong(40) != arenaId.getLeastSignificantBits()
                || b.getLong(48) != 1 || b.getLong(56) != 1 || b.getLong(64) != 1 || b.getLong(72) != 1
                || b.getLong(80) != 0 || b.getInt(88) != config.dataFiles()
                || b.getInt(92) != config.logicalShards() || b.getLong(96) != config.bytesPerFile()
                || b.getInt(104) != 4096 || b.getInt(108) != 1024 * 1024 || b.getLong(112) != 1
                || b.getInt(120) != expectedBookie.length) {
            throw new IOException("Storage identity, generation, configuration or feature mismatch");
        }
        byte[] storage = incarnation.value();
        for (int i = 0; i < storage.length; i++) {
            if (b.get(16 + i) != storage[i]) {
                throw new IOException("Storage incarnation mismatch");
            }
        }
        for (int i = 0; i < expectedBookie.length; i++) {
            if (b.get(124 + i) != expectedBookie[i]) {
                throw new IOException("Bookie identity mismatch");
            }
        }
        for (int i = 124 + expectedBookie.length; i < 4092; i++) {
            if (b.get(i) != 0) {
                throw new IOException("Unsupported identity block extension");
            }
        }
    }
}

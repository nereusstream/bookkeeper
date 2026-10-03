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
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.CRC32C;

/** Arena-only physical generation and its two independently recoverable applied roots. */
final class SegmentArenaRotation {
    static final long FIRST_RECORD = 8192;
    private static final long HEADER_MAGIC = 0x5557415247303031L; // UWARG001
    private static final long EXTENSION_MAGIC = 0x5557415250303031L; // UWARP001

    record Root(long generation, SegmentControlCheckpoint.Reference first, UUID firstId,
                SegmentControlCheckpoint.Reference second, UUID secondId) {
        Root {
            if (generation < 2 || first.generation() == Long.MAX_VALUE
                    || second.generation() != first.generation() + 1 || first.slot() == second.slot()
                    || first.through() > Long.MAX_VALUE - 2 || second.through() != first.through() + 1
                    || first.logOffset() > Long.MAX_VALUE - 208 || second.logOffset() != first.logOffset() + 208
                    || firstId.equals(new UUID(0, 0)) || secondId.equals(new UUID(0, 0))
                    || firstId.equals(secondId)) {
                throw new IllegalArgumentException("Invalid Arena rotation roots");
            }
        }

        long through() {
            return second.through() + 1;
        }
    }

    static Path path(IsolatedSegmentConfig config, long generation) {
        return config.directory().resolve("arena-control-generation-" + (generation % 2 == 0 ? "a" : "b"));
    }

    static ByteBuffer header(Root root) {
        ByteBuffer page = ByteBuffer.allocate(4096).putLong(HEADER_MAGIC).putInt(1).putInt(64)
                .put(root.second().encode());
        extension(page, root);
        checksum(page);
        return page.clear();
    }

    static Root readHeader(FileChannel file) throws IOException {
        if (file.size() < FIRST_RECORD) {
            throw new IOException("Incomplete Arena generation header");
        }
        ByteBuffer page = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(file, page, 4096);
        verifyChecksum(page);
        Root root = parseExtension(page);
        if (page.getLong(0) != HEADER_MAGIC || page.getInt(8) != 1 || page.getInt(12) != 64
                || !root.second().equals(SegmentControlCheckpoint.decodeReference(page.slice(16, 64)))) {
            throw new IOException("Invalid Arena generation header binding");
        }
        return root;
    }

    static Root publication(IsolatedSegmentConfig config, int slot) throws IOException {
        try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, slot),
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            ByteBuffer page = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(file, page, 4096);
            // The shared codec validates the envelope, selected reference and owner before this call.
            verifyChecksum(page);
            return page.getLong(80) == 0 ? null : parseExtension(page);
        }
    }

    static void publish(IsolatedSegmentConfig config, SegmentControlCheckpoint.Reference reference,
                        Root root, SegmentFileIo io) throws IOException {
        ByteBuffer page = ByteBuffer.allocate(4096).putLong(0x5557415342303031L).putInt(1).putInt(64)
                .put(reference.encode());
        extension(page, root);
        checksum(page);
        try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, reference.slot()),
                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.writeFully(file, page.clear(), 4096);
            io.force(file, true);
        }
        if (!reference.equals(SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.ARENA,
                reference.slot())) || !root.equals(publication(config, reference.slot()))) {
            throw new IOException("Arena rotation publication verification failed");
        }
    }

    static Root parseExtension(ByteBuffer page) throws IOException {
        if (page.getLong(80) != EXTENSION_MAGIC) {
            throw new IOException("Unsupported Arena rotation extension");
        }
        for (int index = 256; index < 4092; index++) {
            if (page.get(index) != 0) {
                throw new IOException("Unsupported Arena rotation extension bytes");
            }
        }
        try {
            return new Root(page.getLong(88), SegmentControlCheckpoint.decodeReference(page.slice(96, 64)),
                    new UUID(page.getLong(160), page.getLong(168)),
                    SegmentControlCheckpoint.decodeReference(page.slice(176, 64)),
                    new UUID(page.getLong(240), page.getLong(248)));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid Arena rotation extension", invalid);
        }
    }

    private static void extension(ByteBuffer page, Root root) {
        page.position(80).putLong(EXTENSION_MAGIC).putLong(root.generation()).put(root.first().encode())
                .putLong(root.firstId().getMostSignificantBits()).putLong(root.firstId().getLeastSignificantBits())
                .put(root.second().encode()).putLong(root.secondId().getMostSignificantBits())
                .putLong(root.secondId().getLeastSignificantBits());
    }

    private static void checksum(ByteBuffer page) {
        CRC32C crc = new CRC32C();
        crc.update(page.slice(0, 4092));
        page.putInt(4092, (int) crc.getValue());
    }

    private static void verifyChecksum(ByteBuffer page) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(page.slice(0, 4092));
        if (page.getInt(4092) != (int) crc.getValue()) {
            throw new IOException("Arena rotation CRC mismatch");
        }
    }

    private SegmentArenaRotation() {
    }
}

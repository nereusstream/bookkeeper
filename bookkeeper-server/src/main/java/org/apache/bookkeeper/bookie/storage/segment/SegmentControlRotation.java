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

/** Owner-specific physical generation and its two independently recoverable applied roots. */
final class SegmentControlRotation {
    static final long FIRST_RECORD = 8192;

    record Root(long generation, SegmentControlCheckpoint.Reference first, UUID firstId,
                SegmentControlCheckpoint.Reference second, UUID secondId) {
        Root {
            if (generation < 2 || first.generation() == Long.MAX_VALUE
                    || second.generation() != first.generation() + 1 || first.slot() == second.slot()
                    || first.through() > Long.MAX_VALUE - 2 || second.through() != first.through() + 1
                    || first.logOffset() > Long.MAX_VALUE - 208 || second.logOffset() != first.logOffset() + 208
                    || firstId.equals(new UUID(0, 0)) || secondId.equals(new UUID(0, 0))
                    || firstId.equals(secondId)) {
                throw new IllegalArgumentException("Invalid control rotation roots");
            }
        }

        long through() {
            return second.through() + 1;
        }
    }

    static Path path(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner, long generation) {
        String prefix = owner == SegmentControlFormat.Owner.ARENA ? "arena" : "bookie";
        return config.directory().resolve(prefix + "-control-generation-" + (generation % 2 == 0 ? "a" : "b"));
    }

    static ByteBuffer header(SegmentControlFormat.Owner owner, Root root) {
        ByteBuffer page = ByteBuffer.allocate(4096).putLong(headerMagic(owner)).putInt(1).putInt(64)
                .put(root.second().encode());
        extension(page, owner, root);
        checksum(page);
        return page.clear();
    }

    static Root readHeader(FileChannel file, SegmentControlFormat.Owner owner) throws IOException {
        if (file.size() < FIRST_RECORD) {
            throw new IOException("Incomplete control generation header");
        }
        ByteBuffer page = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(file, page, 4096);
        verifyChecksum(page);
        Root root = parseExtension(page, owner);
        if (page.getLong(0) != headerMagic(owner) || page.getInt(8) != 1 || page.getInt(12) != 64
                || !root.second().equals(SegmentControlCheckpoint.decodeReference(page.slice(16, 64)))) {
            throw new IOException("Invalid control generation header binding");
        }
        return root;
    }

    static Root publication(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner, int slot)
            throws IOException {
        return publication(config, owner, slot, null);
    }

    static Root publication(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner, int slot,
            SegmentOwnedFiles files) throws IOException {
        try (FileChannel file = SegmentOwnedFiles.openRegularReadOnly(
                files, SegmentControlCheckpoint.superblock(config, slot))) {
            ByteBuffer page = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(file, page, publicationOffset(owner));
            // The shared codec validates the envelope, selected reference and owner before this call.
            verifyChecksum(page);
            return page.getLong(80) == 0 ? null : parseExtension(page, owner);
        }
    }

    static void publish(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner,
                        SegmentControlCheckpoint.Reference reference, Root root, SegmentFileIo io) throws IOException {
        publish(config, owner, reference, root, io, null);
    }

    static void publish(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner,
                        SegmentControlCheckpoint.Reference reference, Root root, SegmentFileIo io,
                        SegmentOwnedFiles files) throws IOException {
        ByteBuffer page = ByteBuffer.allocate(4096).putLong(publicationMagic(owner)).putInt(1).putInt(64)
                .put(reference.encode());
        extension(page, owner, root);
        checksum(page);
        try (FileChannel file = SegmentOwnedFiles.open(files,
                SegmentControlCheckpoint.superblock(config, reference.slot()),
                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.writeFully(file, page.clear(), publicationOffset(owner));
            io.force(file, true);
        }
        if (!reference.equals(SegmentControlCheckpoint.publication(config, owner,
                reference.slot(), files)) || !root.equals(publication(config, owner, reference.slot(), files))) {
            throw new IOException("Control rotation publication verification failed");
        }
    }

    static Root parseExtension(ByteBuffer page, SegmentControlFormat.Owner owner) throws IOException {
        if (page.getLong(80) != extensionMagic(owner)) {
            throw new IOException("Unsupported control rotation extension");
        }
        for (int index = 256; index < 4092; index++) {
            if (page.get(index) != 0) {
                throw new IOException("Unsupported control rotation extension bytes");
            }
        }
        try {
            return new Root(page.getLong(88), SegmentControlCheckpoint.decodeReference(page.slice(96, 64)),
                    new UUID(page.getLong(160), page.getLong(168)),
                    SegmentControlCheckpoint.decodeReference(page.slice(176, 64)),
                    new UUID(page.getLong(240), page.getLong(248)));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid control rotation extension", invalid);
        }
    }

    private static void extension(ByteBuffer page, SegmentControlFormat.Owner owner, Root root) {
        page.position(80).putLong(extensionMagic(owner)).putLong(root.generation()).put(root.first().encode())
                .putLong(root.firstId().getMostSignificantBits()).putLong(root.firstId().getLeastSignificantBits())
                .put(root.second().encode()).putLong(root.secondId().getMostSignificantBits())
                .putLong(root.secondId().getLeastSignificantBits());
    }

    private static long headerMagic(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 0x5557415247303031L : 0x5557425247303031L;
    }

    private static long extensionMagic(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 0x5557415250303031L : 0x5557425250303031L;
    }

    private static long publicationMagic(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 0x5557415342303031L : 0x5557425342303031L;
    }

    private static long publicationOffset(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 4096 : 8192;
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
            throw new IOException("Control rotation CRC mismatch");
        }
    }

    private SegmentControlRotation() {
    }
}

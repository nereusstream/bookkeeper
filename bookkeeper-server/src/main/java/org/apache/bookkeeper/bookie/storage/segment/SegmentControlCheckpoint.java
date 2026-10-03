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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Shared bounded control image codec; each owner retains its own commit order and publication page. */
final class SegmentControlCheckpoint {
    record Reference(long generation, int slot, long through, long logOffset, int payloadBytes, String digest) {
        Reference {
            if (generation < 1 || slot < 0 || slot > 1 || through < -1 || logOffset < 4096
                    || payloadBytes < 0 || digest == null || !digest.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid Control checkpoint reference");
            }
        }

        ByteBuffer encode() {
            return ByteBuffer.allocate(64).putLong(generation).putInt(slot).putLong(through).putLong(logOffset)
                    .putInt(payloadBytes).put(HexFormat.of().parseHex(digest)).flip();
        }
    }

    record Operation(long sequence, int status, ByteBuffer record) {
        Operation {
            record = record.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer record() {
            return record.asReadOnlyBuffer();
        }
    }

    record Snapshot(long through, long logOffset, List<Operation> operations) {
        Snapshot {
            operations = List.copyOf(operations);
        }
    }

    static Reference reference(long generation, int slot, Snapshot snapshot, long maxLogBytes) throws IOException {
        MessageDigest digest = sha256();
        long length = 0;
        for (Operation operation : snapshot.operations()) {
            ByteBuffer bytes = encode(operation);
            length += bytes.remaining();
            if (length > Integer.MAX_VALUE || length - maxLogBytes > maxLogBytes) {
                throw new IOException("Control checkpoint exceeds its control-state budget");
            }
            digest.update(bytes);
        }
        return new Reference(generation, slot, snapshot.through(), snapshot.logOffset(), (int) length,
                HexFormat.of().formatHex(digest.digest()));
    }

    static void write(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentControlFormat.Owner owner,
                      Reference reference,
                      Snapshot snapshot, SegmentFileIo io) throws IOException {
        try (FileChannel file = FileChannel.open(path(config, owner, reference.slot()), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING,
                LinkOption.NOFOLLOW_LINKS)) {
            io.writeFully(file, identity.encode(config, fileId(owner, reference.slot())), 0);
            ByteBuffer header = envelope(128, imageMagic(owner), reference);
            header.putInt(80, snapshot.operations().size());
            checksum(header);
            io.writeFully(file, header, 4096);
            long offset = 4224;
            for (Operation operation : snapshot.operations()) {
                ByteBuffer bytes = encode(operation);
                io.writeFully(file, bytes, offset);
                offset += bytes.remaining();
            }
            io.force(file, true);
        }
        IsolatedSegmentFiles.syncDirectory(config.directory());
    }

    static Snapshot read(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentControlFormat.Owner owner,
                         Reference reference, long maxLogBytes) throws IOException {
        if (reference.payloadBytes() - maxLogBytes > maxLogBytes) {
            throw new IOException("Control checkpoint exceeds its control-state budget");
        }
        try (FileChannel file = FileChannel.open(path(config, owner, reference.slot()), StandardOpenOption.READ,
                LinkOption.NOFOLLOW_LINKS)) {
            if (file.size() != 4224L + reference.payloadBytes()) {
                throw new IOException("Incomplete or extra Control checkpoint bytes");
            }
            ByteBuffer identityBytes = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(file, identityBytes, 0);
            identity.validate(identityBytes, config, fileId(owner, reference.slot()));
            ByteBuffer header = ByteBuffer.allocate(128);
            SegmentFileIo.readFully(file, header, 4096);
            if (!reference.equals(parseEnvelope(header, imageMagic(owner)))) {
                throw new IOException("Control checkpoint content identity differs from its commit");
            }
            int count = header.getInt(80);
            if (count < 0 || count > reference.payloadBytes() / 160
                    || (reference.through() == -1) != (count == 0)) {
                throw new IOException("Invalid Control checkpoint operation count");
            }
            zero(header, 84, 124);
            MessageDigest digest = sha256();
            List<Operation> operations = new ArrayList<>(count);
            long offset = 4224;
            long sequence = -1;
            for (int index = 0; index < count; index++) {
                if (file.size() - offset < 16) {
                    throw new IOException("Truncated Control checkpoint operation");
                }
                ByteBuffer prefix = ByteBuffer.allocate(16);
                SegmentFileIo.readFully(file, prefix, offset);
                digest.update(prefix.duplicate());
                long nextSequence = prefix.getLong();
                int status = prefix.getInt();
                int length = prefix.getInt();
                if (nextSequence <= sequence || nextSequence > reference.through()
                        || (index == 0 && nextSequence != 0) || status < 1 || status > 4
                        || length < 144 || length > 65536 || length > file.size() - offset - 16) {
                    throw new IOException("Invalid Control checkpoint operation");
                }
                ByteBuffer record = ByteBuffer.allocate(length);
                SegmentFileIo.readFully(file, record, offset + 16);
                digest.update(record.duplicate());
                SegmentControlParser.parse(record, owner, new Generation(1), nextSequence);
                operations.add(new Operation(nextSequence, status, record));
                offset += 16L + length;
                sequence = nextSequence;
            }
            if (offset != file.size() || !reference.digest().equals(HexFormat.of().formatHex(digest.digest()))) {
                throw new IOException("Control checkpoint payload digest mismatch");
            }
            return new Snapshot(reference.through(), reference.logOffset(), operations);
        }
    }

    static void publish(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner,
                        Reference reference, SegmentFileIo io) throws IOException {
        try (FileChannel file = FileChannel.open(superblock(config, reference.slot()), StandardOpenOption.READ,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.writeFully(file, envelope(4096, publicationMagic(owner), reference), publicationOffset(owner));
            // The other owner's page may already exist; never truncate it during this publication.
            io.force(file, true);
        }
    }

    static Reference publication(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner, int slot)
            throws IOException {
        try (FileChannel file = FileChannel.open(superblock(config, slot), StandardOpenOption.READ,
                LinkOption.NOFOLLOW_LINKS)) {
            if (file.size() < publicationOffset(owner) + 4096 || file.size() > 12288) {
                throw new IOException("No complete Control checkpoint publication");
            }
            ByteBuffer page = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(file, page, publicationOffset(owner));
            Reference reference = parseEnvelope(page, publicationMagic(owner));
            zero(page, 80, 4092);
            if (reference.slot() != slot) {
                throw new IOException("Control checkpoint publication slot mismatch");
            }
            return reference;
        }
    }

    static void syncPublication(IsolatedSegmentConfig config, int slot) throws IOException {
        try (FileChannel file = FileChannel.open(superblock(config, slot), StandardOpenOption.READ,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            file.force(true);
        }
    }

    static Reference decodeReference(ByteBuffer bytes) throws IOException {
        if (bytes.remaining() != 64) {
            throw new IOException("Invalid Control checkpoint reference length");
        }
        ByteBuffer b = bytes.duplicate();
        try {
            long generation = b.getLong();
            int slot = b.getInt();
            long through = b.getLong();
            long offset = b.getLong();
            int length = b.getInt();
            byte[] digest = new byte[32];
            b.get(digest);
            return new Reference(generation, slot, through, offset, length, HexFormat.of().formatHex(digest));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid Control checkpoint reference", invalid);
        }
    }

    private static ByteBuffer encode(Operation operation) {
        ByteBuffer record = operation.record();
        return ByteBuffer.allocate(16 + record.remaining()).putLong(operation.sequence())
                .putInt(operation.status()).putInt(record.remaining()).put(record).flip();
    }

    private static ByteBuffer envelope(int size, long magic, Reference reference) {
        ByteBuffer bytes = ByteBuffer.allocate(size).putLong(magic).putInt(1).putInt(64).put(reference.encode());
        checksum(bytes);
        bytes.clear();
        return bytes;
    }

    private static Reference parseEnvelope(ByteBuffer bytes, long magic) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(bytes.slice(0, bytes.limit() - 4));
        if (bytes.getLong(0) != magic || bytes.getInt(8) != 1 || bytes.getInt(12) != 64
                || bytes.getInt(bytes.limit() - 4) != (int) crc.getValue()) {
            throw new IOException("Control checkpoint envelope or CRC mismatch");
        }
        return decodeReference(bytes.slice(16, 64));
    }

    private static void checksum(ByteBuffer bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes.slice(0, bytes.limit() - 4));
        bytes.putInt(bytes.limit() - 4, (int) crc.getValue());
    }

    private static void zero(ByteBuffer bytes, int start, int end) throws IOException {
        for (int index = start; index < end; index++) {
            if (bytes.get(index) != 0) {
                throw new IOException("Unsupported Control checkpoint extension");
            }
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("Required SHA-256 unavailable", unavailable);
        }
    }

    static Path path(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner, int slot) {
        String prefix = owner == SegmentControlFormat.Owner.ARENA ? "arena-checkpoint-" : "bookie-checkpoint-";
        return config.directory().resolve(prefix + (slot == 0 ? "a" : "b"));
    }

    private static int fileId(SegmentControlFormat.Owner owner, int slot) {
        return (owner == SegmentControlFormat.Owner.ARENA ? -5 : -7) - slot;
    }

    private static long imageMagic(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 0x5557414350303031L : 0x5557424350303031L;
    }

    private static long publicationMagic(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 0x5557415342303031L : 0x5557425342303031L;
    }

    private static long publicationOffset(SegmentControlFormat.Owner owner) {
        return owner == SegmentControlFormat.Owner.ARENA ? 4096 : 8192;
    }

    static Path superblock(IsolatedSegmentConfig config, int slot) {
        return config.directory().resolve(slot == 0 ? "superblock-a" : "superblock-b");
    }

    private SegmentControlCheckpoint() {
    }
}

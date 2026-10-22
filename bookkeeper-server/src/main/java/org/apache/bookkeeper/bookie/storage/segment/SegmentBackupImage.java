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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Offline immutable recovery input, never a current grant, absence proof or mountable storage scope. */
final class SegmentBackupImage {
    static final String MARKER = "backup-input";
    private static final String MANIFEST = "backup-manifest";
    private static final long MAGIC = 0x55574241434b3031L;
    private static final int MAX_MANIFEST = 1024 * 1024;
    private static final int COPY_BYTES = 64 * 1024;

    record Reference(UUID imageId, String digest) {
        Reference {
            if (imageId == null || imageId.equals(new UUID(0, 0))
                    || digest == null || !digest.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid backup image reference");
            }
        }
    }

    private record FileSeal(String name, long bytes, byte[] digest) { }

    /** Native locks and a bounded original recovery inventory; returned records borrow the caller's scratch. */
    static final class Image implements AutoCloseable,
            org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.BackupSource {
        private final IsolatedSegmentConfig config;
        private final SegmentFileHeader identity;
        private final SegmentDataFileLock lock;
        private final SegmentArenaControlLog arena;
        private final SegmentBookieControlLog bookie;
        private final SegmentDataRecovery.PhysicalInventory inventory;
        private boolean closed;

        private Image(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentDataFileLock lock,
                SegmentArenaControlLog arena, SegmentBookieControlLog bookie,
                SegmentDataRecovery.PhysicalInventory inventory) {
            this.config = config;
            this.identity = identity;
            this.lock = lock;
            this.arena = arena;
            this.bookie = bookie;
            this.inventory = inventory;
        }

        @Override
        public org.apache.bookkeeper.net.BookieId bookieId() {
            return identity.bookieId();
        }

        @Override
        public byte[] storageIncarnation() {
            return identity.incarnation().value();
        }

        /** One target-accounted aligned input; the retained entry keeps that capacity until its last release. */
        @Override
        public synchronized io.netty.buffer.ByteBuf readEntry(ProfileLedgerContext context, long entryId,
                io.netty.buffer.ByteBufAllocator allocator) throws IOException {
            if (closed) {
                throw new IOException("Backup input is closed");
            }
            if (inventory.entries().stream().noneMatch(value -> value.context().equals(context)
                    && value.entryId() == entryId)) {
                return null;
            }
            var location = inventory.uniqueLocation(arena, context, entryId);
            int bytes = SegmentPointRead.scratchBytes(location);
            var scratch = allocator.buffer(bytes, bytes);
            try {
                var record = SegmentPointRead.readWithScratch(config, identity, arena,
                        location, scratch.nioBuffer(0, bytes)).record();
                int offset = Math.toIntExact(location.recordOffset() % 4096) + SegmentBatchFormat.RECORD_HEADER;
                return scratch.retainedSlice(offset, record.entry().remaining()).asReadOnly();
            } finally {
                scratch.release();
            }
        }

        List<ProfileLedgerContext> contexts() {
            return inventory.entries().stream().map(SegmentPointRead.Location::context).distinct()
                    .sorted(Comparator.comparing(value -> HexFormat.of().formatHex(value.encode()))).toList();
        }

        /** Empty means this mirror lacks the coordinate, never quorum absence or a current confirmed frontier. */
        synchronized Optional<SegmentBatchParser.Record> readEntry(ProfileLedgerContext context, long entryId,
                ByteBuffer scratch) throws IOException {
            if (closed) {
                throw new IOException("Backup input is closed");
            }
            if (inventory.entries().stream().noneMatch(value -> value.context().equals(context)
                    && value.entryId() == entryId)) {
                return Optional.empty();
            }
            var location = inventory.uniqueLocation(arena, context, entryId);
            return Optional.of(SegmentPointRead.readWithScratch(config, identity, arena, location, scratch).record());
        }

        @Override
        public synchronized void close() throws IOException {
            if (!closed) {
                closed = true;
                release(lock, arena, bookie);
            }
        }
    }

    static Reference capture(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, long controlBytes, int maximumEntries,
            String clusterScope, String deviceScope, Path destination) throws IOException {
        return capture(config, identity, key, validator, controlBytes, maximumEntries,
                clusterScope, deviceScope, destination, new SegmentFileIo());
    }

    static Reference capture(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, long controlBytes, int maximumEntries,
            String clusterScope, String deviceScope, Path destination, SegmentFileIo io) throws IOException {
        checkScope(clusterScope);
        checkScope(deviceScope);
        Path output = destination.toAbsolutePath().normalize();
        output = output.getParent().toRealPath().resolve(output.getFileName());
        if (output.startsWith(config.directory().toRealPath())
                || Files.exists(config.directory().resolve(MARKER), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Backup output must be a new independent input directory");
        }
        try (Image source = inspect(config, identity, key, validator, controlBytes, maximumEntries, null)) {
            Files.createDirectory(output);
            UUID imageId = UUID.randomUUID();
            // Mark even an incomplete copy before any native files can appear in it.
            write(output.resolve(MARKER), ByteBuffer.allocate(24).putLong(MAGIC)
                    .putLong(imageId.getMostSignificantBits()).putLong(imageId.getLeastSignificantBits()).flip(), io);
            write(output.resolve("isolated.lock"), ByteBuffer.allocate(0), io);
            IsolatedSegmentFiles.syncDirectory(output);
            IsolatedSegmentFiles.syncDirectory(output.getParent());
            var seals = new ArrayList<FileSeal>();
            ByteBuffer buffer = ByteBuffer.allocate(COPY_BYTES);
            for (String name : fileNames(config, controlBytes)) {
                Path from = config.directory().resolve(name);
                MessageDigest digest = sha256();
                try (FileChannel input = FileChannel.open(from, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                        FileChannel copied = FileChannel.open(output.resolve(name), StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                    long size = input.size();
                    for (long offset = 0; offset < size;) {
                        buffer.clear().limit((int) Math.min(buffer.capacity(), size - offset));
                        SegmentFileIo.readFully(input, buffer, offset);
                        digest.update(buffer.duplicate());
                        io.writeFully(copied, buffer, offset);
                        offset += buffer.remaining();
                    }
                    io.force(copied, true);
                    seals.add(new FileSeal(name, size, digest.digest()));
                }
            }
            byte[] manifest = encode(source, clusterScope, deviceScope, imageId, seals);
            write(output.resolve(MANIFEST), ByteBuffer.wrap(manifest), io);
            IsolatedSegmentFiles.syncDirectory(output);
            IsolatedSegmentFiles.syncDirectory(output.getParent());
            var reference = new Reference(imageId, HexFormat.of().formatHex(sha256().digest(manifest)));
            try (Image ignored = open(config, identity, key, validator, controlBytes, maximumEntries,
                    clusterScope, deviceScope, output, reference)) {
                return reference;
            }
        }
    }

    static Image open(IsolatedSegmentConfig sourceConfig, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, long controlBytes, int maximumEntries,
            String clusterScope, String deviceScope, Path directory, Reference reference) throws IOException {
        checkScope(clusterScope);
        checkScope(deviceScope);
        var config = new IsolatedSegmentConfig(directory, 1, sourceConfig.logicalShards(), sourceConfig.dataFiles(),
                sourceConfig.bytesPerFile(), 4096, sourceConfig.maxPayloadBytes(), sourceConfig.maxPendingEntries(),
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        return inspect(config, identity, key, validator, controlBytes, maximumEntries,
                image -> verify(image, clusterScope, deviceScope, reference, controlBytes));
    }

    @FunctionalInterface
    private interface Verification {
        void check(Image image) throws IOException;
    }

    private static Image inspect(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, long controlBytes, int maximumEntries,
            Verification verification) throws IOException {
        SegmentDataFileLock lock = SegmentDataFileLock.open(config.directory());
        SegmentArenaControlLog arena = null;
        SegmentBookieControlLog bookie = null;
        try {
            arena = SegmentArenaControlLog.open(config, identity, controlBytes);
            bookie = SegmentBookieControlLog.open(config, identity, controlBytes, key, validator);
            var inventory = SegmentDataRecovery.recoverLocallyReadableInventoryWhileLocked(config, identity,
                    arena, bookie, new SegmentFileIo(), maximumEntries, config.logicalShards(), lock.lock());
            var image = new Image(config, identity, lock, arena, bookie, inventory);
            if (verification != null) {
                verification.check(image);
            }
            return image;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                release(lock, arena, bookie);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static void release(SegmentDataFileLock lock, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie) throws IOException {
        try (lock; arena; bookie) {
            // Close the control owners before releasing DATA exclusion, including all failure paths.
        }
    }

    private static List<String> fileNames(IsolatedSegmentConfig config, long controlBytes) throws IOException {
        var names = new ArrayList<>(List.of("superblock-a", "superblock-b", "arena-control", "bookie-control"));
        for (int i = 0; i < config.dataFiles(); i++) {
            names.add("data-" + i);
        }
        for (String owner : List.of("arena", "bookie")) {
            for (String kind : List.of("checkpoint", "control-generation")) {
                for (String slot : List.of("a", "b")) {
                    String name = owner + "-" + kind + "-" + slot;
                    if (Files.exists(config.directory().resolve(name), LinkOption.NOFOLLOW_LINKS)) {
                        names.add(name);
                    }
                }
            }
        }
        for (String name : names) {
            if (!Files.isRegularFile(config.directory().resolve(name), LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Backup requires every native file, without symlinks: " + name);
            }
            if (!name.startsWith("data-") && Files.size(config.directory().resolve(name))
                    > Math.addExact(Math.multiplyExact(controlBytes, 2), 8192)) {
                throw new IOException("Backup control file exceeds its configured state budget");
            }
        }
        try (var contents = Files.newDirectoryStream(config.directory())) {
            for (Path file : contents) {
                String name = file.getFileName().toString();
                if ((name.startsWith("data-") || name.startsWith("arena-") || name.startsWith("bookie-")
                        || name.startsWith("superblock-")) && !names.contains(name)
                        && !List.of("arena-control.lock", "bookie-control.lock").contains(name)) {
                    throw new IOException("Backup contains an unsupported native file: " + name);
                }
            }
        }
        return names.stream().sorted().toList();
    }

    private static byte[] encode(Image source, String cluster, String device, UUID id, List<FileSeal> files)
            throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeLong(MAGIC);
            out.writeInt(1);
            out.writeLong(id.getMostSignificantBits());
            out.writeLong(id.getLeastSignificantBits());
            out.writeUTF(cluster);
            out.writeUTF(device);
            out.write(source.identity.encode(source.config, -1).array());
            out.writeLong(source.arena.appliedThrough());
            out.writeLong(source.bookie.appliedThrough());
            List<ProfileLedgerContext> contexts = source.contexts();
            if (contexts.size() > (MAX_MANIFEST - 8192) / ProfileLedgerContext.ENCODED_LENGTH) {
                throw new IOException("Backup context inventory exceeds its manifest bound");
            }
            out.writeInt(contexts.size());
            for (var context : contexts) {
                out.write(context.encode());
            }
            out.writeInt(files.size());
            for (var file : files) {
                out.writeUTF(file.name());
                out.writeLong(file.bytes());
                out.write(file.digest());
            }
        }
        if (bytes.size() > MAX_MANIFEST) {
            throw new IOException("Backup manifest exceeds its bound");
        }
        return bytes.toByteArray();
    }

    private static void verify(Image image, String cluster, String device, Reference reference, long controlBytes)
            throws IOException {
        Path manifest = image.config.directory().resolve(MANIFEST);
        long size = Files.size(manifest);
        if (size < 4096 || size > MAX_MANIFEST) {
            throw new IOException("Backup manifest length is invalid");
        }
        ByteBuffer content = ByteBuffer.allocate((int) size);
        try (var file = FileChannel.open(manifest, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            SegmentFileIo.readFully(file, content, 0);
        }
        if (!reference.digest().equals(HexFormat.of().formatHex(sha256().digest(content.array())))) {
            throw new IOException("Backup manifest differs from its external image reference");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(content.array()))) {
            if (in.readLong() != MAGIC || in.readInt() != 1
                    || !new UUID(in.readLong(), in.readLong()).equals(reference.imageId())
                    || !in.readUTF().equals(cluster) || !in.readUTF().equals(device)
                    || !Arrays.equals(in.readNBytes(4096), image.identity.encode(image.config, -1).array())
                    || in.readLong() != image.arena.appliedThrough()
                    || in.readLong() != image.bookie.appliedThrough()) {
                throw new IOException("Backup scope, native control cut or image identity differs");
            }
            List<ProfileLedgerContext> contexts = image.contexts();
            if (in.readInt() != contexts.size()) {
                throw new IOException("Backup ledger-instance inventory is incomplete");
            }
            for (var context : contexts) {
                if (!Arrays.equals(in.readNBytes(ProfileLedgerContext.ENCODED_LENGTH), context.encode())) {
                    throw new IOException("Backup ledger-instance identity differs");
                }
            }
            List<String> names = fileNames(image.config, controlBytes);
            if (in.readInt() != names.size()) {
                throw new IOException("Backup native file range is incomplete");
            }
            ByteBuffer scratch = ByteBuffer.allocate(COPY_BYTES);
            for (String name : names) {
                if (!in.readUTF().equals(name)) {
                    throw new IOException("Backup file name or range differs");
                }
                long length = in.readLong();
                byte[] digest = in.readNBytes(32);
                try (var file = FileChannel.open(image.config.directory().resolve(name),
                        StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                    if (length != file.size()) {
                        throw new IOException("Backup file length differs");
                    }
                    MessageDigest hash = sha256();
                    for (long offset = 0; offset < length;) {
                        scratch.clear().limit((int) Math.min(scratch.capacity(), length - offset));
                        SegmentFileIo.readFully(file, scratch, offset);
                        hash.update(scratch);
                        offset += scratch.limit();
                    }
                    if (!Arrays.equals(digest, hash.digest())) {
                        throw new IOException("Backup file digest differs");
                    }
                }
            }
            if (in.read() != -1) {
                throw new IOException("Backup manifest has an unsupported suffix");
            }
        }
        ByteBuffer marker = ByteBuffer.allocate(24);
        try (var file = FileChannel.open(image.config.directory().resolve(MARKER),
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (file.size() != 24) {
                throw new IOException("Backup input marker length differs");
            }
            SegmentFileIo.readFully(file, marker, 0);
        }
        if (marker.getLong() != MAGIC
                || !new UUID(marker.getLong(), marker.getLong()).equals(reference.imageId())) {
            throw new IOException("Backup marker does not bind this image");
        }
    }

    private static void write(Path path, ByteBuffer bytes, SegmentFileIo io) throws IOException {
        try (var file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS)) {
            io.writeFully(file, bytes, 0);
            io.force(file, true);
        }
    }

    private static void checkScope(String value) {
        if (value == null || value.isEmpty() || value.length() > 255 || value.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Explicit bounded cluster/Profile and device scopes required");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException(unavailable);
        }
    }

    private SegmentBackupImage() { }
}

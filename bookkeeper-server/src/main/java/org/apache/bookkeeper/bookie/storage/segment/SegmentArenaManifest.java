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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;

/** Fixed complete topology. Its digest must come from commissioned requirements, never a discovered device subset. */
record SegmentArenaManifest(StorageIncarnation incarnation, BookieId bookieId,
        List<Arena> arenas, Path controlDirectory, List<Integer> shardArenas, List<ClassicRoot> classicRoots) {
    static final String FILE_NAME = "arena-manifest";
    static final int MAX_BYTES = 1024 * 1024;
    private static final long MAGIC = 0x5557414d414e3031L; // UWAMAN01
    private static final int MAX_ROOT_BYTES = 4096;
    private static final UUID ZERO = new UUID(0, 0);
    private static final byte[] HASH_DOMAIN = "BookKeeper/SegmentArenaManifest/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COMMON_HASH_DOMAIN = "BookKeeper/SegmentArenaManifest/v2\0"
            .getBytes(StandardCharsets.US_ASCII);

    enum ClassicRole {
        JOURNAL(1), LEDGER(2), INDEX(4);

        final int bit;

        ClassicRole(int bit) {
            this.bit = bit;
        }
    }

    /** Physically shared ledger/index roots appear once, with their complete role set. */
    record ClassicRoot(Path directory, Set<ClassicRole> roles) {
        ClassicRoot {
            requireRoot(directory);
            roles = Set.copyOf(roles);
            if (roles.isEmpty()) {
                throw new IllegalArgumentException("Classic root requires its storage roles");
            }
        }

        private int roleBits() {
            return roles.stream().mapToInt(role -> role.bit).reduce(0, (a, b) -> a | b);
        }
    }

    /** Original schema remains byte-for-byte unchanged, including its hash domain. */
    SegmentArenaManifest(StorageIncarnation incarnation, BookieId bookieId,
            List<Arena> arenas, Path controlDirectory, List<Integer> shardArenas) {
        this(incarnation, bookieId, arenas, controlDirectory, shardArenas, List.of());
    }

    /** DATA geometry belongs to the incarnation; execution workers and admission limits do not. */
    record Arena(UUID id, Path directory, int dataFiles, long bytesPerFile) {
        Arena {
            Objects.requireNonNull(id, "id");
            requireRoot(directory);
            if (id.equals(ZERO) || dataFiles < 1 || dataFiles > 64 || bytesPerFile < 3L * 4096
                    || bytesPerFile % 4096 != 0) {
                throw new IllegalArgumentException("Invalid fixed Arena identity, root or DATA geometry");
            }
            Math.multiplyExact(bytesPerFile, dataFiles);
        }
    }

    SegmentArenaManifest {
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(bookieId, "bookieId");
        requireRoot(controlDirectory);
        arenas = List.copyOf(arenas);
        shardArenas = List.copyOf(shardArenas);
        classicRoots = List.copyOf(classicRoots);
        byte[] bookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        if (bookie.length < 1 || bookie.length > 255 || bookieId.toString().indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(bookieId.toString())
                || arenas.isEmpty() || arenas.size() > 64
                || shardArenas.isEmpty() || shardArenas.size() > 64) {
            throw new IllegalArgumentException("Invalid complete Arena manifest bounds");
        }
        String previous = null;
        long capacity = 0;
        for (int i = 0; i < arenas.size(); i++) {
            Arena arena = arenas.get(i);
            String id = arena.id().toString();
            if (previous != null && previous.compareTo(id) >= 0) {
                throw new IllegalArgumentException("Arena identities must be unique and canonically ordered");
            }
            previous = id;
            if (arena.directory().startsWith(controlDirectory) || controlDirectory.startsWith(arena.directory())) {
                throw new IllegalArgumentException(
                        "Bookie control and Arena roots must be distinct and non-overlapping");
            }
            capacity = Math.addExact(capacity, Math.multiplyExact(arena.bytesPerFile(), arena.dataFiles()));
            for (int j = 0; j < i; j++) {
                Path other = arenas.get(j).directory();
                if (arena.directory().startsWith(other) || other.startsWith(arena.directory())) {
                    throw new IllegalArgumentException("Arena roots must be distinct and non-overlapping");
                }
            }
        }
        for (int arena : shardArenas) {
            if (arena < 0 || arena >= arenas.size()) {
                throw new IllegalArgumentException("Each logical shard needs one declared Arena");
            }
        }
        if (classicRoots.size() > 192) {
            throw new IllegalArgumentException("Classic root set exceeds its bounded manifest capacity");
        }
        int roles = 0;
        previous = null;
        for (int i = 0; i < classicRoots.size(); i++) {
            ClassicRoot root = classicRoots.get(i);
            String path = root.directory().toString();
            if (previous != null && previous.compareTo(path) >= 0) {
                throw new IllegalArgumentException("Classic roots must be unique and canonically ordered");
            }
            previous = path;
            roles |= root.roleBits();
            if (overlap(root.directory(), controlDirectory)
                    || arenas.stream().anyMatch(arena -> overlap(root.directory(), arena.directory()))) {
                throw new IllegalArgumentException("Classic roots cannot overlap Segment roots");
            }
            for (int j = 0; j < i; j++) {
                if (overlap(root.directory(), classicRoots.get(j).directory())) {
                    throw new IllegalArgumentException("Classic roots must be physically distinct members");
                }
            }
        }
        if (!classicRoots.isEmpty() && roles != 7) {
            throw new IllegalArgumentException("Common route manifest requires all Classic storage roles");
        }
    }

    ManifestIdentity identity() {
        return new ManifestIdentity(digest(encode().array()));
    }

    int arenaForShard(int shard) {
        return shardArenas.get(shard);
    }

    Path path() {
        return controlDirectory.resolve(FILE_NAME);
    }

    IsolatedSegmentConfig arenaConfig(IsolatedSegmentConfig runtime, int arena) {
        Arena member = arenas.get(arena);
        return localConfig(runtime, member.directory(), member.dataFiles(), member.bytesPerFile());
    }

    IsolatedSegmentConfig bookieConfig(IsolatedSegmentConfig runtime) {
        // Bookie headers carry zero DATA geometry; these values are never used to open DATA in this root.
        return localConfig(runtime, controlDirectory, runtime.dataFiles(), runtime.bytesPerFile());
    }

    private IsolatedSegmentConfig localConfig(IsolatedSegmentConfig runtime, Path root, int files, long bytes) {
        return new IsolatedSegmentConfig(root, 1, shardArenas.size(), files, bytes, runtime.alignment(),
                runtime.maxPayloadBytes(), runtime.maxPendingEntries(), runtime.backend(), runtime.affinity(),
                runtime.numaPolicy(), runtime.waitStrategy());
    }

    /** Create once inside the externally protected control root. Failure never authorizes adoption or replacement. */
    void writeNew(SegmentFileIo io) throws IOException {
        Path file = path();
        if (!file.getParent().equals(file.getParent().toRealPath())) {
            throw new IOException("Arena manifest control root must be canonical and contain no symbolic links");
        }
        try (FileChannel output = FileChannel.open(file,
                java.util.Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS), PosixFilePermissions.asFileAttribute(
                                PosixFilePermissions.fromString("rw-------")))) {
            io.writeFully(output, encode(), 0);
            io.force(output, true);
        }
        IsolatedSegmentFiles.syncDirectory(file.getParent());
    }

    /** Verify the original complete manifest before returning any parsed routing data. Does not create paths. */
    static SegmentArenaManifest read(Path file, ManifestIdentity commissioned) throws IOException {
        Objects.requireNonNull(commissioned, "commissioned manifest identity");
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Arena manifest must be an existing regular file");
        }
        if (!file.toAbsolutePath().normalize().equals(file.toRealPath())) {
            throw new IOException("Arena manifest path must contain no symbolic links");
        }
        var permissions = Files.getPosixFilePermissions(file, LinkOption.NOFOLLOW_LINKS);
        if (permissions.stream().anyMatch(p -> p != PosixFilePermission.OWNER_READ
                && p != PosixFilePermission.OWNER_WRITE)) {
            throw new IOException("Arena manifest must be private to its protected service credential");
        }
        ByteBuffer bytes;
        try (FileChannel input = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            long length = input.size();
            if (length < 52 || length > MAX_BYTES) {
                throw new IOException("Arena manifest length is outside its fixed bounds");
            }
            bytes = ByteBuffer.allocate((int) length);
            SegmentFileIo.readFully(input, bytes, 0);
            if (input.size() != length) {
                throw new IOException("Arena manifest changed while reading");
            }
        }
        if (!MessageDigest.isEqual(commissioned.value(), digest(bytes.array()))) {
            throw new IOException("Arena manifest differs from commissioned complete topology");
        }
        try {
            long magic = bytes.getLong();
            int schema = bytes.getInt();
            if (magic != MAGIC || schema != 1 && schema != 2) {
                throw new IOException("Unsupported Arena manifest format");
            }
            byte[] storage = new byte[16];
            bytes.get(storage);
            BookieId bookie = BookieId.parse(text(bytes, 255));
            int count = bytes.getInt();
            Path control = Path.of(text(bytes, MAX_ROOT_BYTES));
            int shards = bytes.getInt();
            if (count < 1 || count > 64 || shards < 1 || shards > 64) {
                throw new IOException("Arena manifest count is outside its fixed bounds");
            }
            List<Arena> arenas = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                UUID id = new UUID(bytes.getLong(), bytes.getLong());
                Path root = Path.of(text(bytes, MAX_ROOT_BYTES));
                arenas.add(new Arena(id, root, bytes.getInt(), bytes.getLong()));
            }
            List<Integer> mapping = new ArrayList<>(shards);
            for (int i = 0; i < shards; i++) {
                mapping.add(bytes.getInt());
            }
            List<ClassicRoot> classic = new ArrayList<>();
            if (schema == 2) {
                int roots = bytes.getInt();
                if (roots < 1 || roots > 192) {
                    throw new IOException("Invalid common route Classic root count");
                }
                for (int i = 0; i < roots; i++) {
                    Path root = Path.of(text(bytes, MAX_ROOT_BYTES));
                    int bits = bytes.getInt();
                    if (bits < 1 || bits > 7) {
                        throw new IOException("Invalid Classic root storage roles");
                    }
                    var roles = java.util.EnumSet.noneOf(ClassicRole.class);
                    for (ClassicRole role : ClassicRole.values()) {
                        if ((bits & role.bit) != 0) {
                            roles.add(role);
                        }
                    }
                    classic.add(new ClassicRoot(root, roles));
                }
            }
            var result = new SegmentArenaManifest(new StorageIncarnation(storage), bookie, arenas, control, mapping,
                    classic);
            if (bytes.hasRemaining() || !Arrays.equals(bytes.array(), result.encode().array())
                    || !file.toAbsolutePath().normalize().equals(result.path())) {
                throw new IOException("Noncanonical or misplaced Arena manifest");
            }
            return result;
        } catch (IllegalArgumentException | ArithmeticException | java.nio.BufferUnderflowException malformed) {
            throw new IOException("Malformed Arena manifest", malformed);
        }
    }

    private ByteBuffer encode() {
        byte[] bookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        byte[] control = controlDirectory.toString().getBytes(StandardCharsets.UTF_8);
        int length = 8 + 4 + 16 + 4 + bookie.length + 3 * 4 + control.length + 4 * shardArenas.size();
        for (Arena arena : arenas) {
            length = Math.addExact(length, 16 + 4
                    + arena.directory().toString().getBytes(StandardCharsets.UTF_8).length + 4 + 8);
        }
        if (!classicRoots.isEmpty()) {
            length = Math.addExact(length, 4);
            for (ClassicRoot root : classicRoots) {
                length = Math.addExact(length, 8
                        + root.directory().toString().getBytes(StandardCharsets.UTF_8).length);
            }
        }
        if (length > MAX_BYTES) {
            throw new IllegalArgumentException("Complete manifest exceeds its bounded file capacity");
        }
        ByteBuffer bytes = ByteBuffer.allocate(length);
        bytes.putLong(MAGIC).putInt(classicRoots.isEmpty() ? 1 : 2)
                .put(incarnation.value()).putInt(bookie.length).put(bookie)
                .putInt(arenas.size()).putInt(control.length).put(control).putInt(shardArenas.size());
        for (Arena arena : arenas) {
            byte[] root = arena.directory().toString().getBytes(StandardCharsets.UTF_8);
            bytes.putLong(arena.id().getMostSignificantBits()).putLong(arena.id().getLeastSignificantBits())
                    .putInt(root.length).put(root).putInt(arena.dataFiles()).putLong(arena.bytesPerFile());
        }
        shardArenas.forEach(bytes::putInt);
        if (!classicRoots.isEmpty()) {
            bytes.putInt(classicRoots.size());
            for (ClassicRoot root : classicRoots) {
                byte[] path = root.directory().toString().getBytes(StandardCharsets.UTF_8);
                bytes.putInt(path.length).put(path).putInt(root.roleBits());
            }
        }
        return bytes.flip();
    }

    private static boolean overlap(Path first, Path second) {
        return first.startsWith(second) || second.startsWith(first);
    }

    private static void requireRoot(Path root) {
        Objects.requireNonNull(root, "root");
        if (!root.isAbsolute() || !root.normalize().equals(root) || root.getParent() == null
                || root.toString().indexOf(0) >= 0
                || root.toString().getBytes(StandardCharsets.UTF_8).length > MAX_ROOT_BYTES) {
            throw new IllegalArgumentException("Manifest roots must be bounded, absolute and normalized");
        }
    }

    private static byte[] digest(byte[] encoded) {
        try {
            var hash = MessageDigest.getInstance("SHA-256");
            hash.update(ByteBuffer.wrap(encoded).getInt(8) == 2 ? COMMON_HASH_DOMAIN : HASH_DOMAIN);
            return hash.digest(encoded);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String text(ByteBuffer bytes, int maximum) throws IOException {
        int length = bytes.getInt();
        if (length < 1 || length > maximum || length > bytes.remaining()) {
            throw new IOException("Invalid Arena manifest text length");
        }
        ByteBuffer text = bytes.slice(bytes.position(), length);
        bytes.position(bytes.position() + length);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(text).toString();
        } catch (CharacterCodingException malformed) {
            throw new IOException("Invalid Arena manifest UTF-8", malformed);
        }
    }
}

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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;

/** Immutable file identity: original single-Arena v1 or explicitly manifest-bound v2. */
record SegmentFileHeader(StorageIncarnation incarnation, UUID arenaId, BookieId bookieId,
        SegmentArenaManifest manifest) {
    private static final UUID BOOKIE_SCOPE = new UUID(0, 0);
    private static final long TOPOLOGY_MAGIC = 0x5557544f50303031L; // UWTOP001

    SegmentFileHeader(StorageIncarnation incarnation, UUID arenaId, BookieId bookieId) {
        this(incarnation, arenaId, bookieId, null);
    }

    static SegmentFileHeader forArena(SegmentArenaManifest manifest, int arena) {
        return new SegmentFileHeader(manifest.incarnation(), manifest.arenas().get(arena).id(),
                manifest.bookieId(), manifest);
    }

    static SegmentFileHeader forBookie(SegmentArenaManifest manifest) {
        return new SegmentFileHeader(manifest.incarnation(), BOOKIE_SCOPE, manifest.bookieId(), manifest);
    }

    SegmentFileHeader {
        Objects.requireNonNull(incarnation, "incarnation");
        Objects.requireNonNull(arenaId, "arenaId");
        Objects.requireNonNull(bookieId, "bookieId");
        if ((manifest == null && arenaId.equals(BOOKIE_SCOPE)) || bookieId.toString().isEmpty()
                || bookieId.toString().indexOf(0) >= 0
                || bookieId.toString().getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("Invalid isolated storage identity");
        }
        if (manifest != null && (!incarnation.equals(manifest.incarnation()) || !bookieId.equals(manifest.bookieId())
                || !arenaId.equals(BOOKIE_SCOPE)
                        && manifest.arenas().stream().noneMatch(a -> a.id().equals(arenaId)))) {
            throw new IllegalArgumentException("File identity is not a member of the complete Arena manifest");
        }
    }

    private int arenaIndex() {
        for (int i = 0; i < manifest.arenas().size(); i++) {
            if (manifest.arenas().get(i).id().equals(arenaId)) {
                return i;
            }
        }
        return -1; // Explicit Bookie control scope, independent of every member Arena.
    }

    boolean bookieScope() {
        return manifest != null && arenaId.equals(BOOKIE_SCOPE);
    }

    boolean ownsShard(int shard) {
        return manifest == null || !bookieScope() && shard >= 0 && shard < manifest.shardArenas().size()
                && manifest.arenaForShard(shard) == arenaIndex();
    }

    boolean matchesManifestScope(IsolatedSegmentConfig config) {
        return manifest != null && matchesManifestLayout(config, bookieScope() ? -3 : -4);
    }

    void requireStoredManifest(IsolatedSegmentConfig config, int fileId) throws IOException {
        if (manifest == null) {
            return;
        }
        if (!matchesManifestLayout(config, fileId)) {
            throw new IOException("Complete Arena manifest member scope does not match");
        }
        Path root = config.directory();
        Path existing = Files.exists(root, LinkOption.NOFOLLOW_LINKS) ? root : root.getParent();
        if (!existing.equals(existing.toRealPath())
                || !manifest.equals(SegmentArenaManifest.read(manifest.path(), manifest.identity()))) {
            throw new IOException("Stored complete Arena manifest or canonical member root does not match");
        }
    }

    private boolean matchesManifestLayout(IsolatedSegmentConfig config, int fileId) {
        if (manifest == null) {
            return true;
        }
        boolean bookie = arenaId.equals(BOOKIE_SCOPE);
        boolean bookieOnly = fileId == -3 || fileId == -7 || fileId == -8;
        boolean arenaOnly = fileId >= 0 || fileId == -4 || fileId == -5 || fileId == -6;
        if ((bookie && arenaOnly || !bookie && bookieOnly)
                || fileId < -8 || fileId >= config.dataFiles()
                || config.logicalShards() != manifest.shardArenas().size()) {
            return false;
        }
        if (bookie) {
            return config.directory().equals(manifest.controlDirectory());
        }
        var arena = manifest.arenas().get(arenaIndex());
        return config.directory().equals(arena.directory()) && config.dataFiles() == arena.dataFiles()
                && config.bytesPerFile() == arena.bytesPerFile();
    }

    ByteBuffer encode(IsolatedSegmentConfig config, int fileId) {
        if (!matchesManifestLayout(config, fileId)) {
            throw new IllegalArgumentException("File role, root or geometry differs from the fixed Arena manifest");
        }
        boolean global = manifest != null && arenaId.equals(BOOKIE_SCOPE);
        ByteBuffer b = ByteBuffer.allocate(4096);
        byte[] bookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        b.putLong(0x555753544f523031L).putInt(manifest == null ? 1 : 2).putInt(fileId).put(incarnation.value())
                .putLong(arenaId.getMostSignificantBits()).putLong(arenaId.getLeastSignificantBits())
                .putLong(1).putLong(1).putLong(1).putLong(1).putLong(0)
                .putInt(global ? 0 : config.dataFiles()).putInt(config.logicalShards())
                .putLong(global ? 0 : config.bytesPerFile())
                .putInt(config.alignment()).putInt(1024 * 1024).putLong(manifest == null ? 1 : 3)
                .putInt(bookie.length).put(bookie);
        if (manifest != null) {
            b.position(384);
            b.putLong(TOPOLOGY_MAGIC).putLong(1).put(manifest.identity().value())
                    .putInt(manifest.arenas().size()).putInt(global ? 1 : 2).putInt(arenaIndex());
        }
        CRC32C crc = new CRC32C();
        crc.update(b.slice(0, 4092));
        b.putInt(4092, (int) crc.getValue());
        b.clear();
        return b;
    }

    void validate(ByteBuffer input, IsolatedSegmentConfig config, int fileId) throws IOException {
        if (!matchesManifestLayout(config, fileId)) {
            throw new IOException("File role, root or geometry differs from the fixed Arena manifest");
        }
        boolean global = manifest != null && arenaId.equals(BOOKIE_SCOPE);
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
        if (b.getLong(0) != 0x555753544f523031L || b.getInt(8) != (manifest == null ? 1 : 2) || b.getInt(12) != fileId
                || b.getLong(32) != arenaId.getMostSignificantBits()
                || b.getLong(40) != arenaId.getLeastSignificantBits()
                || b.getLong(48) != 1 || b.getLong(56) != 1 || b.getLong(64) != 1 || b.getLong(72) != 1
                || b.getLong(80) != 0 || b.getInt(88) != (global ? 0 : config.dataFiles())
                || b.getInt(92) != config.logicalShards() || b.getLong(96) != (global ? 0 : config.bytesPerFile())
                || b.getInt(104) != 4096 || b.getInt(108) != 1024 * 1024 || b.getLong(112) != (manifest == null ? 1 : 3)
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
        int reserved = 124 + expectedBookie.length;
        if (manifest != null) {
            for (int i = reserved; i < 384; i++) {
                if (b.get(i) != 0) {
                    throw new IOException("Nonzero identity block padding");
                }
            }
            byte[] digest = new byte[32];
            b.get(400, digest);
            if (b.getLong(384) != TOPOLOGY_MAGIC || b.getLong(392) != 1
                    || !java.security.MessageDigest.isEqual(digest, manifest.identity().value())
                    || b.getInt(432) != manifest.arenas().size() || b.getInt(436) != (global ? 1 : 2)
                    || b.getInt(440) != arenaIndex()) {
                throw new IOException("File differs from its complete Arena manifest or owner role");
            }
            reserved = 444;
        }
        for (int i = reserved; i < 4092; i++) {
            if (b.get(i) != 0) {
                throw new IOException("Unsupported identity block extension");
            }
        }
    }
}

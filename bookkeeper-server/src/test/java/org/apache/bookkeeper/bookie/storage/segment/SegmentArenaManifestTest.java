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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentArenaManifestTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private SegmentArenaManifest manifest() throws IOException {
        byte[] incarnation = new byte[16];
        incarnation[0] = 1;
        Path root = temporary.getRoot().toPath().toRealPath();
        return new SegmentArenaManifest(new StorageIncarnation(incarnation), BookieId.parse("manifest:3181"),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 9), root.resolve("a"), 1, 16384),
                        new SegmentArenaManifest.Arena(new UUID(0, 10), root.resolve("b"), 2, 32768)),
                root.resolve("bookie-control"), List.of(1, 0, 1, 0));
    }

    private static ManifestIdentity identity(byte[] bytes) throws Exception {
        var hash = java.security.MessageDigest.getInstance("SHA-256");
        hash.update((ByteBuffer.wrap(bytes).getInt(8) == 2
                ? "BookKeeper/SegmentArenaManifest/v2\0" : "BookKeeper/SegmentArenaManifest/v1\0")
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        return new ManifestIdentity(hash.digest(bytes));
    }

    private static IsolatedSegmentConfig config(Path root, int files, long bytes) {
        return new IsolatedSegmentConfig(root, 1, 4, files, bytes, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private static IsolatedSegmentConfig config(SegmentArenaManifest.Arena arena) {
        return config(arena.directory(), arena.dataFiles(), arena.bytesPerFile());
    }

    private static void checksum(ByteBuffer bytes) {
        var crc = new java.util.zip.CRC32C();
        crc.update(bytes.slice(0, 4092));
        bytes.putInt(4092, (int) crc.getValue());
    }

    @Test
    public void commonManifestBindsCompleteClassicRootsAndPreservesOriginalSchemaBytes() throws Exception {
        var original = manifest();
        Path parent = original.controlDirectory().getParent();
        var roots = List.of(new SegmentArenaManifest.ClassicRoot(parent.resolve("classic-journal"),
                        Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                new SegmentArenaManifest.ClassicRoot(parent.resolve("classic-ledger-index"),
                        Set.of(SegmentArenaManifest.ClassicRole.LEDGER, SegmentArenaManifest.ClassicRole.INDEX)));
        var common = new SegmentArenaManifest(original.incarnation(), original.bookieId(), original.arenas(),
                original.controlDirectory(), original.shardArenas(), roots);
        Files.createDirectory(common.controlDirectory());
        original.writeNew(new SegmentFileIo());
        byte[] v1 = Files.readAllBytes(original.path());
        assertEquals(1, ByteBuffer.wrap(v1).getInt(8));
        assertEquals(identity(v1), original.identity());
        assertEquals(original, SegmentArenaManifest.read(original.path(), original.identity()));
        Files.delete(original.path());
        common.writeNew(new SegmentFileIo());
        byte[] v2 = Files.readAllBytes(common.path());
        assertEquals(2, ByteBuffer.wrap(v2).getInt(8));
        assertEquals(identity(v2), common.identity());
        assertEquals(common, SegmentArenaManifest.read(common.path(), common.identity()));
        byte[] prefix = java.util.Arrays.copyOf(v2, v1.length);
        ByteBuffer.wrap(prefix).putInt(8, 1);
        org.junit.Assert.assertArrayEquals(v1, prefix);
        ByteBuffer suffix = ByteBuffer.wrap(v2, v1.length, v2.length - v1.length).slice();
        assertEquals(2, suffix.getInt());
        for (int i = 0; i < roots.size(); i++) {
            byte[] path = roots.get(i).directory().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(path.length, suffix.getInt());
            byte[] actual = new byte[path.length];
            suffix.get(actual);
            org.junit.Assert.assertArrayEquals(path, actual);
            assertEquals(i == 0 ? 1 : 6, suffix.getInt());
            assertFalse(Files.exists(roots.get(i).directory()));
        }
        assertFalse(suffix.hasRemaining());
        assertNotEquals(original.identity(), common.identity());
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(common.path(), original.identity()));
        assertThrows(IOException.class, () -> common.writeNew(new SegmentFileIo()));
        for (int offset : new int[] {v1.length, v2.length - 4}) {
            byte[] malformed = v2.clone();
            ByteBuffer.wrap(malformed).putInt(offset, Integer.MAX_VALUE);
            Files.write(common.path(), malformed);
            var expected = identity(malformed);
            assertThrows(IOException.class, () -> SegmentArenaManifest.read(common.path(), expected));
        }
    }

    @Test
    public void classicRootRolesMustBeCompleteCanonicalAndDisjointFromSegmentMembers() throws Exception {
        var original = manifest();
        Path parent = original.controlDirectory().getParent();
        var all = Set.of(SegmentArenaManifest.ClassicRole.values());
        for (var roots : List.of(
                List.of(new SegmentArenaManifest.ClassicRoot(parent.resolve("classic"),
                        Set.of(SegmentArenaManifest.ClassicRole.JOURNAL))),
                List.of(new SegmentArenaManifest.ClassicRoot(original.controlDirectory(), all)),
                List.of(new SegmentArenaManifest.ClassicRoot(original.arenas().get(0).directory(), all)),
                List.of(new SegmentArenaManifest.ClassicRoot(parent.resolve("classic"), all),
                        new SegmentArenaManifest.ClassicRoot(parent.resolve("classic/sub"), all)),
                List.of(new SegmentArenaManifest.ClassicRoot(parent.resolve("z"), all),
                        new SegmentArenaManifest.ClassicRoot(parent.resolve("y"), all)))) {
            assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(original.incarnation(),
                    original.bookieId(), original.arenas(), original.controlDirectory(),
                    original.shardArenas(), roots));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentArenaManifest.ClassicRoot(parent.resolve("empty"), Set.of()));
    }

    @Test
    public void v2HeaderBindsWholeTopologyAndDistinctBookieRoleWhileV1BytesRemainExact() throws Exception {
        var manifest = manifest();
        var first = SegmentFileHeader.forArena(manifest, 0);
        var second = SegmentFileHeader.forArena(manifest, 1);
        var bookie = SegmentFileHeader.forBookie(manifest);
        var a = config(manifest.arenas().get(0));
        var b = config(manifest.arenas().get(1));
        var global = config(manifest.controlDirectory(), 1, 16384);
        for (int role : new int[] {-1, -2, -4, -5, -6, 0}) {
            ByteBuffer bytes = first.encode(a, role);
            first.validate(bytes.asReadOnlyBuffer(), a, role);
            assertEquals(2, bytes.getInt(8));
            assertEquals(2, bytes.getInt(436));
            assertEquals(0, bytes.getInt(440));
            assertThrows(IOException.class, () -> second.validate(bytes.asReadOnlyBuffer(), b, role));
        }
        for (int role : new int[] {-1, -2, -3, -7, -8}) {
            ByteBuffer bytes = bookie.encode(global, role);
            bookie.validate(bytes.asReadOnlyBuffer(), global, role);
            assertEquals(0, bytes.getLong(32));
            assertEquals(0, bytes.getLong(40));
            assertEquals(0, bytes.getInt(88));
            assertEquals(0, bytes.getLong(96));
            assertEquals(1, bytes.getInt(436));
            assertEquals(-1, bytes.getInt(440));
        }
        assertThrows(IllegalArgumentException.class, () -> first.encode(a, -3));
        assertThrows(IllegalArgumentException.class, () -> bookie.encode(global, -4));
        assertThrows(IOException.class, () -> first.validate(first.encode(a, 0), b, 0));
        var legacy = new SegmentFileHeader(manifest.incarnation(), first.arenaId(), manifest.bookieId());
        ByteBuffer oldBytes = legacy.encode(a, 0);
        assertEquals("538994a190bf5e4956eb3130bbea2166bc474317dcd10d974297d43a3195f533",
                java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                        .digest(oldBytes.array())));
        assertThrows(IOException.class, () -> first.validate(oldBytes, a, 0));
        assertThrows(IOException.class, () -> legacy.validate(first.encode(a, 0), a, 0));
        assertEquals(manifest.identity(), IsolatedSegmentStorage.startupDeviceManifestIdentity(a, first));
        assertEquals(manifest.identity(), IsolatedSegmentStorage.startupDeviceManifestIdentity(b, second));
        assertEquals(manifest.identity(), IsolatedSegmentStorage.startupDeviceManifestIdentity(global, bookie));
    }

    @Test
    public void changedTopologyAndRechecksummedIdentityCannotBeAcceptedAsOriginalMember() throws Exception {
        var manifest = manifest();
        var a = config(manifest.arenas().get(0));
        var header = SegmentFileHeader.forArena(manifest, 0);
        var changed = new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(), manifest.arenas(),
                manifest.controlDirectory(), List.of(0, 1, 0, 1));
        var other = SegmentFileHeader.forArena(changed, 0);
        assertThrows(IOException.class, () -> other.validate(header.encode(a, 0), a, 0));
        for (int offset : new int[] {8, 12, 32, 56, 88, 92, 384, 392, 400, 432, 436, 440, 444}) {
            ByteBuffer bytes = header.encode(a, 0);
            bytes.putInt(offset, bytes.getInt(offset) ^ 1);
            checksum(bytes);
            assertThrows(IOException.class, () -> header.validate(bytes, a, 0));
        }
        Files.createDirectory(manifest.controlDirectory());
        manifest.writeNew(new SegmentFileIo());
        var reloaded = SegmentArenaManifest.read(manifest.path(), manifest.identity());
        Path data = temporary.getRoot().toPath().resolve("identity-page");
        Files.write(data, header.encode(a, 0).array());
        try (var channel = java.nio.channels.FileChannel.open(data, java.nio.file.StandardOpenOption.READ)) {
            ByteBuffer bytes = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(channel, bytes, 0);
            SegmentFileHeader.forArena(reloaded, 0).validate(bytes, a, 0);
        }
        assertFalse(Files.exists(manifest.arenas().get(0).directory()));
        assertFalse(Files.exists(manifest.arenas().get(1).directory()));
    }

    @Test
    public void completeTopologyRoundTripsThroughPrivateForcedCreateOnceFile() throws Exception {
        var manifest = manifest();
        assertFalse(Files.exists(manifest.path().getParent()));
        assertEquals(1, manifest.arenaForShard(0));
        assertEquals(0, manifest.arenaForShard(3));
        Files.createDirectory(manifest.path().getParent());
        var io = new SegmentFileIo(7, -1, -1, false);
        manifest.writeNew(io);
        assertEquals(1, io.forces);
        assertTrue(io.writes > 1);
        assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(manifest.path()));
        assertEquals(manifest, SegmentArenaManifest.read(manifest.path(), manifest.identity()));
        byte[] original = Files.readAllBytes(manifest.path());
        assertThrows(IOException.class, () -> manifest.writeNew(new SegmentFileIo()));
        org.junit.Assert.assertArrayEquals(original, Files.readAllBytes(manifest.path()));
        assertFalse(Files.exists(manifest.arenas().get(0).directory()));
    }

    @Test
    public void sameIncarnationTopologyChangesCannotMatchCommissionedIdentity() throws Exception {
        var manifest = manifest();
        var changed = List.of(
                new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(), manifest.arenas(),
                        manifest.controlDirectory(), List.of(0, 1, 0, 1)),
                new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(), manifest.arenas(),
                        manifest.controlDirectory().resolveSibling("other-control"), manifest.shardArenas()),
                new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(),
                        List.of(manifest.arenas().get(1)), manifest.controlDirectory(), List.of(0, 0, 0, 0)),
                new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(),
                        List.of(new SegmentArenaManifest.Arena(new UUID(0, 9),
                                manifest.arenas().get(0).directory(), 2, 16384), manifest.arenas().get(1)),
                        manifest.controlDirectory(), manifest.shardArenas()));
        Files.createDirectory(manifest.path().getParent());
        manifest.writeNew(new SegmentFileIo());
        for (var other : changed) {
            assertNotEquals(manifest.identity(), other.identity());
            assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), other.identity()));
        }
        byte[] bytes = Files.readAllBytes(manifest.path());
        bytes[bytes.length - 1] ^= 1;
        Files.write(manifest.path(), bytes);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
    }

    @Test(timeout = 10000)
    public void missingPublicMovedSymlinkAndOversizedInputsNeverReturnRoutes() throws Exception {
        var manifest = manifest();
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
        assertFalse(Files.exists(manifest.path().getParent()));
        Files.createDirectory(manifest.path().getParent());
        var fifo = new ProcessBuilder("/usr/bin/mkfifo", "-m", "600", manifest.path().toString()).start();
        assertTrue(fifo.waitFor(5, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, fifo.exitValue());
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
        Files.delete(manifest.path());
        manifest.writeNew(new SegmentFileIo());
        Files.setPosixFilePermissions(manifest.path(), PosixFilePermissions.fromString("rw-r--r--"));
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
        Files.setPosixFilePermissions(manifest.path(), PosixFilePermissions.fromString("rw-------"));
        Path moved = manifest.path().resolveSibling("moved");
        Files.move(manifest.path(), moved);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(moved, manifest.identity()));
        Files.createSymbolicLink(manifest.path(), moved);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
        Files.delete(manifest.path());
        Files.move(moved, manifest.path());
        Files.write(manifest.path(), new byte[SegmentArenaManifest.MAX_BYTES + 1]);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), manifest.identity()));
    }

    @Test
    public void failedForceDoesNotPermitRetryToReplaceOrTruncateCandidate() throws Exception {
        var manifest = manifest();
        Files.createDirectory(manifest.path().getParent());
        assertThrows(IOException.class, () -> manifest.writeNew(new SegmentFileIo(11, -1, 1, false)));
        byte[] candidate = Files.readAllBytes(manifest.path());
        assertThrows(IOException.class, () -> manifest.writeNew(new SegmentFileIo()));
        org.junit.Assert.assertArrayEquals(candidate, Files.readAllBytes(manifest.path()));
        // A failed caller has no durability receipt. The original complete bytes remain verifiable after restart.
        assertEquals(manifest, SegmentArenaManifest.read(manifest.path(), manifest.identity()));
    }

    @Test
    public void malformedButMatchingCommissionedBytesStillFailBoundedParsing() throws Exception {
        var manifest = manifest();
        Files.createDirectory(manifest.path().getParent());
        manifest.writeNew(new SegmentFileIo());
        byte[] original = Files.readAllBytes(manifest.path());
        int counts = 32 + manifest.bookieId().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        for (int offset : new int[] {8, 28, counts, counts + 4, original.length - 4}) {
            byte[] malformed = original.clone();
            ByteBuffer.wrap(malformed).putInt(offset, Integer.MAX_VALUE);
            Files.write(manifest.path(), malformed);
            var expected = identity(malformed);
            assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), expected));
        }
        byte[] overflow = original.clone();
        int filesOffset = counts + 4 + 4
                + manifest.controlDirectory().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + 4 + 16 + 4
                + manifest.arenas().get(0).directory().toString()
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        ByteBuffer.wrap(overflow).putInt(filesOffset, 64).putLong(filesOffset + 4, Long.MAX_VALUE - 4095);
        Files.write(manifest.path(), overflow);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), identity(overflow)));
        byte[] malformed = original.clone();
        malformed[32] = (byte) 0xff;
        Files.write(manifest.path(), malformed);
        var expected = identity(malformed);
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(manifest.path(), expected));
        Path alias = manifest.path().getParent().resolveSibling("alias");
        Files.createSymbolicLink(alias, manifest.path().getParent());
        assertThrows(IOException.class, () -> SegmentArenaManifest.read(
                alias.resolve(SegmentArenaManifest.FILE_NAME), manifest.identity()));
    }

    @Test
    public void invalidListsRootsMappingsAndGeometryRejectBeforeCreatingAnyPath() throws Exception {
        var manifest = manifest();
        var arenas = manifest.arenas();
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(manifest.incarnation(),
                manifest.bookieId(), List.of(arenas.get(1), arenas.get(0)),
                manifest.controlDirectory(), List.of(0, 1)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(manifest.incarnation(),
                manifest.bookieId(), List.of(arenas.get(0), arenas.get(0)),
                manifest.controlDirectory(), List.of(0, 1)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(manifest.incarnation(),
                manifest.bookieId(), arenas, arenas.get(0).directory(), List.of(0, 1)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(manifest.incarnation(),
                manifest.bookieId(), arenas, manifest.controlDirectory(), List.of(0, 2)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest.Arena(new UUID(0, 11),
                Path.of("relative"), 1, 16384));
        assertThrows(IllegalArgumentException.class, () -> new SegmentArenaManifest(manifest.incarnation(),
                manifest.bookieId(), List.of(arenas.get(0), new SegmentArenaManifest.Arena(new UUID(0, 11),
                        arenas.get(0).directory().resolve("nested"), 1, 16384)),
                manifest.controlDirectory(), List.of(0, 1)));
        var mutable = new ArrayList<>(manifest.shardArenas());
        var frozen = new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(), arenas,
                manifest.controlDirectory(), mutable);
        mutable.set(0, 0);
        assertEquals(1, frozen.arenaForShard(0));
        assertThrows(UnsupportedOperationException.class, () -> frozen.shardArenas().set(0, 0));
        assertFalse(Files.exists(arenas.get(0).directory()));
        assertFalse(Files.exists(arenas.get(1).directory()));
    }
}

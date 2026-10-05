/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.DurableLocalReadiness;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.LocalReadinessStore.WriteStatus;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentLocalReadinessStoreTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static StorageIncarnation incarnation() {
        byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) 1);
        return new StorageIncarnation(bytes);
    }

    private static DurableLocalReadiness record(long generation) {
        byte[] manifest = new byte[32];
        Arrays.fill(manifest, (byte) 2);
        return new DurableLocalReadiness("b", incarnation(), new ManifestIdentity(manifest),
                new Generation(5), new Generation(4), new Generation(6), new Generation(generation),
                DurableLocalReadiness.Status.READY, true);
    }

    private static SegmentLocalReadinessStore open(Path directory) throws IOException {
        return new SegmentLocalReadinessStore(directory, "b", incarnation());
    }

    private Path initial() throws IOException {
        Path directory = temporary.newFolder().toPath();
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertEquals(WriteStatus.APPLIED, store.write(record(1)).status());
        }
        return directory;
    }

    @Test
    public void realReceiptReopensAndKeepsOnlyTheCurrentGeneration() throws Exception {
        Path directory = initial();
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertEquals(record(1), store.read("b").orElseThrow());
            assertEquals(WriteStatus.ALREADY_APPLIED, store.write(record(1)).status());
            DurableLocalReadiness conflict = new DurableLocalReadiness("b", incarnation(),
                    record(1).deviceManifestIdentity(), new Generation(7), new Generation(4), new Generation(6),
                    new Generation(1), DurableLocalReadiness.Status.READY, true);
            assertEquals(WriteStatus.CONFLICT, store.write(conflict).status());
            assertThrows(IllegalArgumentException.class, () -> store.read("foreign"));
            for (int generation = 2; generation <= 100; generation++) {
                assertEquals(WriteStatus.APPLIED, store.write(record(generation)).status());
            }
            assertEquals(WriteStatus.CONFLICT, store.write(record(1)).status());
            store.close();
            assertThrows(IllegalStateException.class, () -> store.read("b"));
            assertThrows(IllegalStateException.class, () -> store.write(record(101)));
            store.close();
        }
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertEquals(record(100), store.read("b").orElseThrow());
        }
        try (var files = Files.list(directory)) {
            assertEquals(2, files.count());
        }
        assertEquals(113, Files.size(directory.resolve("local-readiness")));
    }

    @Test
    public void shortTransfersCompleteButPartialFailureCannotBeReadAsDurable() throws Exception {
        Path directory = temporary.newFolder().toPath();
        SegmentFileIo io = new SegmentFileIo(7, 2, -1, false);
        try (SegmentLocalReadinessStore store = new SegmentLocalReadinessStore(directory, "b", incarnation(), io)) {
            assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(1)).status());
            assertThrows(UncheckedIOException.class, () -> store.read("b"));
            int writes = io.writes;
            assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(2)).status());
            assertEquals(writes, io.writes);
        }
        io = new SegmentFileIo(7, -1, -1, false);
        try (SegmentLocalReadinessStore store = new SegmentLocalReadinessStore(directory, "b", incarnation(), io)) {
            assertTrue(store.read("b").isEmpty());
            assertEquals(WriteStatus.APPLIED, store.write(record(1)).status());
            assertTrue(io.writes > 1);
            assertEquals(record(1), store.read("b").orElseThrow());
        }
    }

    @Test
    public void failedFileOrDirectoryBarrierPoisonsTheAttemptEvenWhenBytesAreReadable() throws Exception {
        for (int barrier = 1; barrier <= 2; barrier++) {
            Path directory = initial();
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, barrier, false);
            // Opening existing authority uses two recovery barriers before the injected publication.
            io.forces = -2;
            try (SegmentLocalReadinessStore store = new SegmentLocalReadinessStore(directory, "b", incarnation(), io)) {
                assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(2)).status());
                assertEquals(record(barrier), SegmentLocalReadinessCodec.decode(
                        ByteBuffer.wrap(Files.readAllBytes(directory.resolve("local-readiness")))));
                assertThrows(UncheckedIOException.class, () -> store.read("b"));
                assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(2)).status());
            }
            try (SegmentLocalReadinessStore recovered = open(directory)) {
                assertEquals(record(barrier), recovered.read("b").orElseThrow());
            }
        }
    }

    @Test
    public void unpublishedTemporaryRecordNeverBecomesAuthority() throws Exception {
        Path directory = temporary.newFolder().toPath();
        Files.write(directory.resolve("local-readiness.pending"), SegmentLocalReadinessCodec.encode(record(9)).array());
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertTrue(store.read("b").isEmpty());
            assertEquals(WriteStatus.APPLIED, store.write(record(1)).status());
            assertEquals(record(1), store.read("b").orElseThrow());
        }
        assertTrue(Files.notExists(directory.resolve("local-readiness.pending")));
    }

    @Test
    public void canonicalFixedVectorHasIndependentFieldAndChecksumChecks() throws Exception {
        byte[] vector = HexFormat.of().parseHex("55574c524430303100000001000000710000000162"
                + "01010101010101010101010101010101"
                + "0202020202020202020202020202020202020202020202020202020202020202"
                + "0000000000000005000000000000000400000000000000060000000000000001"
                + "000000010000000181893d61");
        assertArrayEquals(vector, SegmentLocalReadinessCodec.encode(record(1)).array());
        assertEquals(record(1), SegmentLocalReadinessCodec.decode(ByteBuffer.wrap(vector)));
        ByteBuffer parser = ByteBuffer.wrap(vector);
        assertEquals(0x55574c5244303031L, parser.getLong());
        assertEquals(1, parser.getInt());
        assertEquals(vector.length, parser.getInt());
        assertEquals(1, parser.getInt());
        assertEquals('b', parser.get());
        parser.position(101);
        assertEquals(1, parser.getInt());
        assertEquals(1, parser.getInt());
        CRC32C crc = new CRC32C();
        crc.update(vector, 0, vector.length - 4);
        assertEquals((int) crc.getValue(), parser.getInt());
    }

    @Test
    public void invalidAuthoritativeBytesAndWrongScopeFailClosedAndReleaseTheLock() throws Exception {
        byte[] good = SegmentLocalReadinessCodec.encode(record(1)).array();
        for (int field : new int[] {0, 8, 12, 16, 20, 21, 69, 101, 105, 109}) {
            Path directory = initial();
            byte[] invalid = good.clone();
            if (field == 20 || field == 21) {
                invalid[field] = 0;
                if (field == 21) {
                    Arrays.fill(invalid, 21, 37, (byte) 0);
                }
            } else if (field == 69) {
                Arrays.fill(invalid, 69, 77, (byte) 0);
            } else {
                ByteBuffer.wrap(invalid).putInt(field, 99);
            }
            if (field != 109) {
                checksum(invalid);
            }
            Files.write(directory.resolve("local-readiness"), invalid);
            assertThrows(IOException.class, () -> open(directory));
            Files.write(directory.resolve("local-readiness"), good);
            try (SegmentLocalReadinessStore store = open(directory)) {
                assertEquals(record(1), store.read("b").orElseThrow());
            }
        }
        Path directory = initial();
        assertThrows(IOException.class, () -> new SegmentLocalReadinessStore(directory, "other", incarnation()));
        byte[] different = new byte[16];
        Arrays.fill(different, (byte) 3);
        assertThrows(IOException.class, () -> new SegmentLocalReadinessStore(directory, "b",
                new StorageIncarnation(different)));
        Files.write(directory.resolve("local-readiness"), Arrays.copyOf(good, good.length + 1));
        assertThrows(IOException.class, () -> open(directory));
        Files.write(directory.resolve("local-readiness"), new byte[SegmentLocalReadinessCodec.MAX_BYTES + 1]);
        assertThrows(IOException.class, () -> open(directory));
    }

    @Test
    public void corruptionDuringAnOpenAttemptCannotBeHiddenOrOverwritten() throws Exception {
        Path directory = initial();
        try (SegmentLocalReadinessStore store = open(directory)) {
            byte[] bytes = Files.readAllBytes(directory.resolve("local-readiness"));
            bytes[0] ^= 1;
            Files.write(directory.resolve("local-readiness"), bytes);
            assertThrows(UncheckedIOException.class, () -> store.read("b"));
            assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(2)).status());
            assertArrayEquals(bytes, Files.readAllBytes(directory.resolve("local-readiness")));
        }
    }

    @Test
    public void anotherProcessCannotTakeTheWriterAfterAnAliasOpenFails() throws Exception {
        Path directory = initial();
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertThrows(IOException.class, () -> open(directory.resolve(".")));
            assertEquals(0, child(directory, "lock", 2));
            assertEquals(record(1), store.read("b").orElseThrow());
        }
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertEquals(WriteStatus.APPLIED, store.write(record(2)).status());
        }
    }

    @Test
    public void realProcessExitsAtPublicationBoundariesRecoverOnlyPublishedReceipts() throws Exception {
        for (String stage : new String[] {"before-file-sync", "before-directory-sync", "after-publication"}) {
            Path directory = initial();
            assertEquals(stage.equals("before-file-sync") ? 91 : stage.equals("before-directory-sync") ? 92 : 93,
                    child(directory, stage, 2));
            try (SegmentLocalReadinessStore store = open(directory)) {
                assertEquals(record(stage.equals("before-file-sync") ? 1 : 2), store.read("b").orElseThrow());
            }
            assertEquals(93, child(directory, "after-publication", 3));
            try (SegmentLocalReadinessStore store = open(directory)) {
                assertEquals(record(3), store.read("b").orElseThrow());
                assertEquals(WriteStatus.CONFLICT, store.write(record(1)).status());
                assertEquals(WriteStatus.ALREADY_APPLIED, store.write(record(3)).status());
            }
        }
    }

    @Test
    public void requiredRecoveryBarrierFailureAndSymbolicLinksAreRejected() throws Exception {
        Path directory = initial();
        for (int barrier = 1; barrier <= 2; barrier++) {
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, barrier, false);
            assertThrows(IOException.class, () -> new SegmentLocalReadinessStore(directory, "b", incarnation(), io));
        }
        Path target = temporary.newFile().toPath();
        Files.write(target, SegmentLocalReadinessCodec.encode(record(1)).array());
        Files.delete(directory.resolve("local-readiness"));
        Files.createSymbolicLink(directory.resolve("local-readiness"), target);
        assertThrows(IOException.class, () -> open(directory));
        Files.delete(directory.resolve("local-readiness"));
        Files.createSymbolicLink(directory.resolve("local-readiness.pending"), target);
        try (SegmentLocalReadinessStore store = open(directory)) {
            assertEquals(WriteStatus.DURABILITY_UNKNOWN, store.write(record(2)).status());
        }
        assertArrayEquals(SegmentLocalReadinessCodec.encode(record(1)).array(), Files.readAllBytes(target));
    }

    private int child(Path directory, String stage, int generation) throws Exception {
        Path log = temporary.newFile().toPath();
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                ProcessProbe.class.getName(), directory.toString(), stage, Integer.toString(generation))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue("Child output: " + log, process.waitFor(30, TimeUnit.SECONDS));
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    public static final class ProcessProbe {
        public static void main(String[] arguments) throws Exception {
            Path directory = Path.of(arguments[0]);
            String stage = arguments[1];
            if (stage.equals("lock")) {
                try (SegmentLocalReadinessStore unexpected = open(directory)) {
                    throw new AssertionError("Another process acquired a live writer lock");
                } catch (IOException expected) {
                    assertTrue(expected.getMessage().contains("writer"));
                    return;
                }
            }
            AtomicBoolean armed = new AtomicBoolean();
            AtomicInteger barriers = new AtomicInteger();
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (armed.get()) {
                    int barrier = barriers.incrementAndGet();
                    if (stage.equals("before-file-sync") && barrier == 1) {
                        Runtime.getRuntime().halt(91);
                    } else if (stage.equals("before-directory-sync") && barrier == 2) {
                        Runtime.getRuntime().halt(92);
                    }
                }
            });
            try (SegmentLocalReadinessStore store = new SegmentLocalReadinessStore(directory, "b", incarnation(), io)) {
                armed.set(true);
                assertEquals(WriteStatus.APPLIED, store.write(record(Integer.parseInt(arguments[2]))).status());
                Runtime.getRuntime().halt(93);
            }
        }
    }

    private static void checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }
}

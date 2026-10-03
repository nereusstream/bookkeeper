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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentArenaControlLogTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private IsolatedSegmentConfig config() {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("arena"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private static SegmentFileHeader identity() {
        byte[] incarnation = new byte[16];
        java.util.Arrays.fill(incarnation, (byte) 7);
        return new SegmentFileHeader(new StorageIncarnation(incarnation), new UUID(0, 9),
                BookieId.parse("isolated:3181"));
    }

    private SegmentArenaControlLog.Allocation alloc(int operation, long offset) {
        return new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                new UUID(0, operation), new Generation(1), 0, 0, 1, new Generation(1), operation, offset, 1, 1);
    }

    private boolean granted(SegmentArenaControlLog log, long offset, int shard, long ownerGeneration,
                            long allocationId) {
        return log.hasAppliedGrant(0, offset, SegmentArenaControlLog.BLOCK_BYTES, shard, ownerGeneration,
                allocationId, 1);
    }

    @Test
    public void rotationHeaderAndPublicationMatchIndependentByteVectors() throws Exception {
        SegmentControlCheckpoint.Reference first = new SegmentControlCheckpoint.Reference(1, 0, -1, 4096, 0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        SegmentControlCheckpoint.Reference second = new SegmentControlCheckpoint.Reference(2, 1, 0, 4304, 224,
                "e89150ad58ef5c1b765c176c382ca122f91fca6f509144de7f379f800105125b");
        SegmentControlRotation.Root root = new SegmentControlRotation.Root(2, first, new UUID(1, 2),
                second, new UUID(3, 4));
        assertArrayEquals(checkpointVector("arena-rotation-header-v1.hex"),
                SegmentControlRotation.header(SegmentControlFormat.Owner.ARENA, root).array());
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.ARENA, first, root, new SegmentFileIo());
            try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, 0),
                    StandardOpenOption.READ)) {
                ByteBuffer page = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(file, page, 4096);
                assertArrayEquals(checkpointVector("arena-rotation-publication-v1.hex"), page.array());
            }
            assertEquals(root, SegmentControlRotation.publication(config, SegmentControlFormat.Owner.ARENA, 0));
        }
    }

    @Test
    public void rotationReclaimsFullJournalsAndPreservesConditionalResultsAcrossRepeatedRestarts() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            for (int cycle = 0; cycle < 3; cycle++) {
                try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
                    if (cycle == 0) {
                        log.submitBatch(List.of(alloc(1, 4096), alloc(2, 4096)));
                    }
                    boolean full = false;
                    for (int retry = 0; retry < 200; retry++) {
                        try {
                            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                                    log.submitBatch(List.of(alloc(1, 4096))).get(0).status());
                        } catch (IOException capacity) {
                            full = true;
                            break;
                        }
                    }
                    assertTrue("The ordinary journal budget must be exhausted before rotation", full);
                    SegmentControlRotation.Root root = log.rotate();
                    assertEquals(cycle + 2, root.generation());
                    assertEquals(8192, Files.size(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                            root.generation())));
                    assertEquals(8192, Files.size(config.directory().resolve("arena-control")));
                    assertTrue(granted(log, 4096, 1, 1, 1));
                    assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                            log.submitBatch(List.of(alloc(2, 4096))).get(0).status());
                    if (cycle > 0) {
                        assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                                root.generation() - 1)));
                    }
                }
            }
        }
    }

    @Test
    public void rotatedFallbackRequiresOneImageAndEveryCurrentSuffixByte() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlRotation.Root root;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            root = log.rotate();
            log.submitBatch(List.of(alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES)));
        }
        Path newest = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, root.second().slot());
        byte[] image = Files.readAllBytes(newest);
        Path publication = SegmentControlCheckpoint.superblock(config, root.second().slot());
        byte[] publicationBytes = Files.readAllBytes(publication);
        corruptCheckpointByte(publication, 4096);
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
        }
        Files.write(publication, publicationBytes);
        corruptCheckpointByte(newest, 4096);
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            assertTrue(granted(log, 4096, 1, 1, 1));
            assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
            // Replace both base images; a later publication must retain the physical generation binding.
            log.checkpoint();
            log.checkpoint();
        }
        Path a = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, 0);
        Path b = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, 1);
        byte[] aBytes = Files.readAllBytes(a);
        byte[] bBytes = Files.readAllBytes(b);
        corruptCheckpointByte(b, 4096);
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
        }
        corruptCheckpointByte(a, 4096);
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
        Files.write(a, aBytes);
        Files.write(b, bBytes);
        Path current = SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                            root.generation());
        byte[] journal = Files.readAllBytes(current);
        try (FileChannel file = FileChannel.open(current, StandardOpenOption.WRITE)) {
            file.truncate(journal.length - 1);
        }
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
        Files.write(current, journal);
        corruptCheckpointByte(current, 8192);
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
        Files.write(current, journal);
        Files.delete(current);
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
        assertTrue(image.length > 4224);
    }

    @Test
    public void slowRotationKeepsQueriesResponsiveAndWriterExclusionAcrossProcesses() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger forces = new AtomicInteger();
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (forces.incrementAndGet() == 5) {
                    started.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("Rotation was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        throw new AssertionError(interrupted);
                    }
                }
            });
            FutureTask<SegmentControlRotation.Root> rotation = new FutureTask<>(() -> log.rotate(io));
            Thread thread = new Thread(rotation);
            thread.start();
            try {
                assertTrue(started.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(() -> granted(log, 4096, 1, 1, 1));
                new Thread(query).start();
                assertTrue(query.get(1, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(2, 4096))));
                assertThrows(IOException.class, log::close);
                assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
                try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(), 32768,
                        new javax.crypto.spec.SecretKeySpec(new byte[16], "AES"),
                        new org.apache.bookkeeper.common.profile.ProfileDescriptorValidator(
                                (id, version) -> false, (id, generation) -> false))) {
                    assertEquals(23, rotationProbe(config.directory(), "lock"));
                }
            } finally {
                release.countDown();
                thread.join(5000);
            }
            assertEquals(2, rotation.get(1, TimeUnit.SECONDS).generation());
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
            assertEquals(23, rotationProbe(config.directory(), "lock"));
        }
        assertEquals(0, rotationProbe(config.directory(), "lock"));
    }

    @Test
    public void rotationProcessExitAtFileAndRootBarriersRetainsAuthority() throws Exception {
        for (String stage : List.of("1", "3", "5", "6", "7", "8", "done")) {
            IsolatedSegmentConfig config = checkpointConfig(temporary.newFolder("rotation-" + stage)
                    .toPath().resolve("arena"));
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
                log.submitBatch(List.of(alloc(1, 4096)));
            }
            assertEquals(stage, 23, rotationProbe(config.directory(), stage));
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
                assertTrue(stage, granted(log, 4096, 1, 1, 1));
                if (stage.equals("done")) {
                    assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
                }
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        log.submitBatch(List.of(alloc(1, 4096))).get(0).status());
                // This also proves an interrupted candidate can be safely retired before the next rotation.
                log.rotate();
            }
        }
    }

    @Test
    public void failedUnpublishedCandidateIsRemovedWithoutDiscardingTheAppliedCut() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            // Four successful image/publication forces precede the candidate's own force.
            assertThrows(IOException.class, () -> log.rotate(new SegmentFileIo(13, -1, 5, false)));
            assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            assertTrue(granted(log, 4096, 1, 1, 1));
            SegmentArenaControlLog.DurableBatch pending = log.appendDurably(List.of(alloc(2, 4096)));
            assertThrows(IOException.class, log::rotate);
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED, log.applyDurable(pending).get(0).status());
            log.rotate();
        }
    }

    @Test
    public void rotationForceFailureDoesNotReturnSuccessAndCanRecover() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            assertThrows(IOException.class, () -> log.rotate(new SegmentFileIo(31, -1, 7, false)));
            assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(2, 4096))));
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
            assertTrue(granted(log, 4096, 1, 1, 1));
            log.rotate();
        }
    }

    private int rotationProbe(Path directory, String stage) throws Exception {
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-XX:MaxDirectMemorySize=32m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                RotationCrashProbe.class.getName(), directory.toString(), stage)
                .redirectErrorStream(true).redirectOutput(directory.getParent().resolve("rotation-probe.log").toFile())
                .start();
        try {
            assertTrue(stage, process.waitFor(15, TimeUnit.SECONDS));
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    public static class RotationCrashProbe {
        public static void main(String[] args) throws Exception {
            IsolatedSegmentConfig config = checkpointConfig(Path.of(args[0]));
            AtomicInteger forces = new AtomicInteger();
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (Integer.toString(forces.incrementAndGet()).equals(args[1])) {
                    Runtime.getRuntime().halt(23);
                }
            });
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 32768)) {
                if (args[1].equals("lock")) {
                    return;
                }
                log.rotate(io);
                log.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, 2), new Generation(1), 0, 0, 1, new Generation(1), 2,
                        4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
                Runtime.getRuntime().halt(23);
            } catch (IOException failure) {
                if (args[1].equals("lock")) {
                    Runtime.getRuntime().halt(23);
                }
                throw failure;
            }
        }
    }

    @Test
    public void emptyCheckpointAndCommitMatchIndependentFixedByteVectors() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentControlCheckpoint.Reference reference = log.checkpoint();
            assertEquals(-1, reference.through());
            assertArrayEquals(checkpointVector("arena-empty-checkpoint-v1.hex"),
                    Files.readAllBytes(SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, 0)));
            try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, 0),
                    StandardOpenOption.READ)) {
                ByteBuffer page = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(file, page, 4096);
                assertArrayEquals(checkpointVector("arena-checkpoint-publication-v1.hex"), page.array());
            }
            ByteBuffer encoded = SegmentControlFormat.encode(SegmentControlFormat.Type.CHECKPOINT_COMMIT,
                    new Generation(1), 0, new UUID(1, 2), new Generation(1), 0, reference.encode());
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            byte[] golden = checkpointVector("arena-checkpoint-commit-v1.hex");
            assertArrayEquals(golden, bytes);
            SegmentControlParser.Command parsed = SegmentControlParser.parse(ByteBuffer.wrap(golden),
                    SegmentControlFormat.Owner.ARENA, new Generation(1), 0);
            assertEquals(reference, SegmentControlCheckpoint.decodeReference(parsed.body()));
        }
    }

    private byte[] checkpointVector(String name) throws IOException {
        try (var input = getClass().getResourceAsStream("/unified-wal-segment/" + name)) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                    .lines().filter(line -> !line.startsWith("#")).collect(Collectors.joining()));
        }
    }

    @Test
    public void checkpointRestoresAppliedResultsAndSuffixAndFallsBackAfterCorruption() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlCheckpoint.Reference first;
        SegmentControlCheckpoint.Reference second;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            log.submitBatch(List.of(alloc(1, 4096), alloc(2, 4096), alloc(1, 4096)));
            first = log.checkpoint();
            assertEquals(2, first.through());
            assertEquals(4096 + 3 * 192, first.logOffset());
            log.submitBatch(List.of(alloc(3, 4096 + SegmentArenaControlLog.BLOCK_BYTES)));
            second = log.checkpoint();
            assertEquals(2, second.generation());
            assertEquals(1 - first.slot(), second.slot());
            log.submitBatch(List.of(alloc(4, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES)));
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(second, log.publishedCheckpoint().orElseThrow());
            assertTrue(granted(log, 4096, 1, 1, 1));
            assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 3));
            assertTrue(granted(log, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 4));
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(alloc(2, 4096))).get(0).status());
            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                    log.submitBatch(List.of(alloc(1, 4096))).get(0).status());
            assertEquals(SegmentArenaControlLog.Status.INCOMPATIBLE,
                    log.submitBatch(List.of(alloc(1, 4096 + SegmentArenaControlLog.BLOCK_BYTES))).get(0).status());
        }
        corruptCheckpointByte(
                SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, second.slot()), 4224);
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertTrue(granted(log, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 4));
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(alloc(2, 4096))).get(0).status());
        }
        corruptCheckpointByte(
                SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, first.slot()), 4096);
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertTrue(log.publishedCheckpoint().isEmpty());
            assertTrue(granted(log, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 4));
            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                    log.submitBatch(List.of(alloc(1, 4096))).get(0).status());
        }
    }

    @Test
    public void checkpointCapturesAppliedCutWhileLaterBatchIsDurableAndDoesNotHoldTheGrantLock() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger barriers = new AtomicInteger();
        SegmentFileIo delayed = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (barriers.incrementAndGet() == 1) {
                forcing.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Checkpoint force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        });
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            SegmentArenaControlLog.DurableBatch pending = log.appendDurably(
                    List.of(alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES)));
            assertEquals(1, log.durableThrough());
            assertEquals(0, log.appliedThrough());
            FutureTask<SegmentControlCheckpoint.Reference> task = new FutureTask<>(() -> log.checkpoint(delayed));
            Thread worker = new Thread(task, "isolated-checkpoint");
            worker.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(() -> granted(log, 4096, 1, 1, 1));
                new Thread(query, "isolated-checkpoint-query").start();
                assertTrue(query.get(1, TimeUnit.SECONDS));
                assertThrows(IOException.class, log::close);
                assertThrows(IOException.class, () -> log.checkpoint());
                log.applyDurable(pending);
            } finally {
                release.countDown();
                worker.join(5000);
            }
            assertEquals(0, task.get(1, TimeUnit.SECONDS).through());
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(0, log.publishedCheckpoint().orElseThrow().through());
            assertTrue(granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
        }
    }

    @Test
    public void checkpointFailuresNeverAcknowledgePublicationOrDiscardTheOldAuthority() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlCheckpoint.Reference first;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            first = log.checkpoint();
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, 1, -1, false)));
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertTrue(granted(log, 4096, 1, 1, 1));
            // Payload is forced and the commit applied; publication write then fails before touching its page.
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, 5, -1, false)));
            assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(2, 4096))));
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertTrue(granted(log, 4096, 1, 1, 1));
            assertEquals(3, log.checkpoint().generation());
        }
    }

    @Test
    public void checkpointPreservesUnclosedSessionAndDurableQuarantine() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            log.submitBatch(List.of(new SegmentArenaControlLog.DataSession(new UUID(0, 1),
                    new Generation(1), 0, true), alloc(2, 4096),
                    new SegmentArenaControlLog.FileQuarantine(new UUID(0, 3), new Generation(1), 0)));
            log.checkpoint();
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertTrue(log.publishedCheckpoint().isPresent());
            assertEquals(1, log.dataSessionEpoch());
            assertTrue(log.hasUnclosedDataSession());
            assertTrue(log.hasDurableQuarantine(0));
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity(), log, new SegmentFileIo()));
        }
    }

    @Test
    public void checkpointCannotHideMissingSuffixOrDamageToTheRequiredFullLog() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            log.checkpoint();
            log.submitBatch(List.of(alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES)));
        }
        Path path = config.directory().resolve("arena-control");
        byte[] intact = Files.readAllBytes(path);
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
            file.truncate(intact.length - 1);
            file.force(true);
        }
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 1024 * 1024));
        assertEquals(intact.length - 1, Files.size(path));
        Files.write(path, intact);
        corruptCheckpointByte(path, 4096);
        assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 1024 * 1024));
    }

    @Test
    public void checkpointRestartAfterAbruptExitAtEachPublicationBarrier() throws Exception {
        for (String stage : List.of("payload", "commit", "publication", "done")) {
            Path directory = temporary.newFolder(stage).toPath().resolve("arena");
            IsolatedSegmentConfig config = checkpointConfig(directory);
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                log.submitBatch(List.of(alloc(1, 4096)));
                log.checkpoint();
            }
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx64m", "-XX:MaxDirectMemorySize=32m", "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    CheckpointCrashProbe.class.getName(), directory.toString(), stage)
                    .redirectErrorStream(true)
                    .redirectOutput(directory.getParent().resolve("probe.log").toFile()).start();
            try {
                assertTrue(stage, process.waitFor(15, TimeUnit.SECONDS));
                assertEquals(stage, 23, process.exitValue());
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                }
            }
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertTrue(stage, granted(log, 4096, 1, 1, 1));
                assertTrue(stage, granted(log, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1, 2));
                assertEquals(stage, stage.equals("publication") || stage.equals("done") ? 2 : 1,
                        log.publishedCheckpoint().orElseThrow().generation());
                assertEquals(stage, SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        log.submitBatch(List.of(alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES))).get(0).status());
            }
        }
    }

    public static class CheckpointCrashProbe {
        public static void main(String[] args) throws Exception {
            IsolatedSegmentConfig config = checkpointConfig(Path.of(args[0]));
            String stage = args[1];
            AtomicInteger controlForces = new AtomicInteger();
            SegmentFileIo control = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (controlForces.incrementAndGet() == 2 && stage.equals("commit")) {
                    Runtime.getRuntime().halt(23);
                }
            });
            AtomicInteger checkpointForces = new AtomicInteger();
            SegmentFileIo checkpoint = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                int force = checkpointForces.incrementAndGet();
                if ((force == 1 && stage.equals("payload")) || (force == 2 && stage.equals("publication"))) {
                    Runtime.getRuntime().halt(23);
                }
            });
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024, control)) {
                log.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, 2), new Generation(1), 0, 0, 1, new Generation(1), 2,
                        4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
                log.checkpoint(checkpoint);
                Runtime.getRuntime().halt(23);
            }
        }
    }

    private static IsolatedSegmentConfig checkpointConfig(Path directory) {
        return new IsolatedSegmentConfig(directory, 1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private static void corruptCheckpointByte(Path path, long offset) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer byteValue = ByteBuffer.allocate(1);
            SegmentFileIo.readFully(file, byteValue, offset);
            file.write(ByteBuffer.wrap(new byte[] {(byte) (byteValue.get(0) ^ 1)}), offset);
            file.force(true);
        }
    }

    @Test
    public void dataSessionPredicatesReplayAndCannotClosePastAQuarantine() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentArenaControlLog.DataSession begin = new SegmentArenaControlLog.DataSession(
                new UUID(0, 1), new Generation(1), 0, true);
        SegmentArenaControlLog.DataSession second = new SegmentArenaControlLog.DataSession(
                new UUID(0, 4), new Generation(2), 1, true);
        SegmentArenaControlLog.DataSession failedClose = new SegmentArenaControlLog.DataSession(
                new UUID(0, 6), new Generation(2), 2, false);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertEquals(List.of(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    SegmentArenaControlLog.Status.CONDITION_FAILED, SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    SegmentArenaControlLog.Status.APPLIED_DURABLE, SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    SegmentArenaControlLog.Status.CONDITION_FAILED), arena.submitBatch(List.of(begin,
                    new SegmentArenaControlLog.DataSession(new UUID(0, 2), new Generation(2), 1, true),
                    new SegmentArenaControlLog.DataSession(new UUID(0, 3), new Generation(1), 1, false), second,
                    new SegmentArenaControlLog.FileQuarantine(new UUID(0, 5), new Generation(1), 0), failedClose))
                    .stream().map(SegmentArenaControlLog.Result::status).toList());
            assertEquals(2, arena.dataSessionEpoch());
            assertTrue(arena.hasUnclosedDataSession());
            assertTrue(arena.hasDurableQuarantine(0));
            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                    arena.submitBatch(List.of(second)).get(0).status());
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertEquals(2, replay.dataSessionEpoch());
            assertTrue(replay.hasUnclosedDataSession());
            assertTrue(replay.hasDurableQuarantine(0));
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    replay.submitBatch(List.of(failedClose)).get(0).status());
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity, replay, new SegmentFileIo()));
        }
    }

    @Test
    public void grantQueriesRemainAvailableWhileControlForceIsBlocked() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo delayed = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Control force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024, delayed)) {
            AtomicReference<SegmentArenaControlLog.DurableBatch> durable = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    durable.set(log.appendDurably(List.of(alloc(1, 4096))));
                } catch (Throwable cause) {
                    failure.set(cause);
                }
            });
            writer.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(() -> granted(log, 4096, 1, 1, 1));
                new Thread(query).start();
                assertFalse(query.get(1, TimeUnit.SECONDS));
                FutureTask<Throwable> second = new FutureTask<>(() -> {
                    try {
                        log.appendDurably(List.of(alloc(2, 4096)));
                        return null;
                    } catch (Throwable cause) {
                        return cause;
                    }
                });
                new Thread(second).start();
                assertTrue(second.get(1, TimeUnit.SECONDS) instanceof IllegalArgumentException);
                FutureTask<Throwable> close = new FutureTask<>(() -> {
                    try {
                        log.close();
                        return null;
                    } catch (Throwable cause) {
                        return cause;
                    }
                });
                new Thread(close).start();
                assertTrue(close.get(1, TimeUnit.SECONDS) instanceof IOException);
                assertEquals(-1, log.durableThrough());
            } finally {
                release.countDown();
                writer.join(5000);
            }
            assertFalse(writer.isAlive());
            assertNull(failure.get());
            assertEquals(0, log.durableThrough());
            assertFalse(granted(log, 4096, 1, 1, 1));
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    log.applyDurable(durable.get()).get(0).status());
            assertTrue(granted(log, 4096, 1, 1, 1));
        }
    }

    @Test
    public void driverAppliesOneDurableBatchAndRejectsConcurrentBatch() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 2);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<List<SegmentArenaControlLog.Result>> results = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            SegmentArenaControlDriver driver = new SegmentArenaControlDriver(owner, io, log);
            assertTrue(driver.submit(List.of(alloc(1, 4096)), (applied, cause) -> {
                owner.requireOwner();
                results.set(applied);
                failure.set(cause);
                done.countDown();
            }));
            assertFalse(driver.submit(List.of(alloc(2, 4096)), (applied, cause) -> {
                throw new AssertionError("Concurrent control batch was admitted");
            }));
            owner.start();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, results.get().get(0).status());
            assertTrue(granted(log, 4096, 1, 1, 1));
        } finally {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertNull(owner.failure());
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertTrue(granted(replay, 4096, 1, 1, 1));
        }
    }

    @Test
    public void driverReportsUncertainForceWithoutApplyingGrant() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024,
                        new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false))) {
            SegmentArenaControlDriver driver = new SegmentArenaControlDriver(owner, io, log);
            assertTrue(driver.submit(List.of(alloc(1, 4096)), (applied, cause) -> {
                owner.requireOwner();
                assertNull(applied);
                failure.set(cause);
                done.countDown();
            }));
            owner.start();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertTrue(failure.get() instanceof IOException);
            assertFalse(granted(log, 4096, 1, 1, 1));
        } finally {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertNull(owner.failure());
    }

    @Test
    public void arenaControlForceAndApplyProgressWhileDataAdmissionIsFull() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
        CountDownLatch dataStarted = new CountDownLatch(1);
        CountDownLatch dataQueued = new CountDownLatch(1);
        CountDownLatch releaseData = new CountDownLatch(1);
        CountDownLatch controlStarted = new CountDownLatch(1);
        CountDownLatch releaseControl = new CountDownLatch(1);
        CountDownLatch controlDone = new CountDownLatch(1);
        CountDownLatch dataDone = new CountDownLatch(2);
        AtomicReference<SegmentArenaControlLog.DurableBatch> durable = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            for (int i = 0; i < 2; i++) {
                int request = i;
                assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                    @Override
                    public void run(SegmentOwnerLoop.Ticket ticket) {
                        io.submit(ticket, () -> {
                            if (request == 0) {
                                dataStarted.countDown();
                                if (!releaseData.await(5, TimeUnit.SECONDS)) {
                                    throw new IOException("DATA I/O was not released");
                                }
                            }
                        }, cause -> {
                            owner.requireOwner();
                            if (cause != null) {
                                failure.set(cause);
                            }
                            dataDone.countDown();
                        });
                        if (request == 1) {
                            dataQueued.countDown();
                        }
                    }

                    @Override
                    public void failed(Throwable cause) {
                        failure.set(cause);
                        dataDone.countDown();
                    }
                }));
                if (i == 0) {
                    owner.start();
                    assertTrue(dataStarted.await(5, TimeUnit.SECONDS));
                }
            }
            assertTrue(dataQueued.await(5, TimeUnit.SECONDS));
            assertFalse(owner.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    throw new AssertionError("DATA capacity exceeded");
                }

                @Override
                public void failed(Throwable cause) {
                    failure.set(cause);
                }
            }));
            assertTrue(owner.submitControl(new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    io.submitControl(ticket, () -> {
                        controlStarted.countDown();
                        if (!releaseControl.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("Control I/O was not released");
                        }
                        durable.set(log.appendDurably(List.of(alloc(1, 4096))));
                    }, cause -> {
                        owner.requireOwner();
                        try {
                            if (cause != null) {
                                throw new AssertionError(cause);
                            }
                            assertFalse(granted(log, 4096, 1, 1, 1));
                            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                                    log.applyDurable(durable.get()).get(0).status());
                        } catch (Throwable problem) {
                            failure.set(problem);
                        } finally {
                            controlDone.countDown();
                        }
                    });
                }

                @Override
                public void failed(Throwable cause) {
                    failure.set(cause);
                    controlDone.countDown();
                }
            }));
            assertTrue(controlStarted.await(5, TimeUnit.SECONDS));
            assertFalse(owner.control(() -> {}));
            assertFalse(granted(log, 4096, 1, 1, 1));
            owner.stopAdmission();
            assertFalse(owner.awaitTermination(Duration.ofMillis(20)));
            releaseControl.countDown();
            assertTrue(controlDone.await(5, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertTrue(granted(log, 4096, 1, 1, 1));
            releaseData.countDown();
            assertTrue(dataDone.await(5, TimeUnit.SECONDS));
        } finally {
            releaseControl.countDown();
            releaseData.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertEquals(0, owner.pending());
        assertEquals(0, owner.controlPending());
        assertNull(owner.failure());
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertTrue(granted(replay, 4096, 1, 1, 1));
        }
    }

    @Test
    public void sameBatchCompetitionAppliesOnlyAfterForceAndReplaysTheSameWinner() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            SegmentFileIo io = new SegmentFileIo();
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024, io)) {
                SegmentArenaControlLog.Allocation first = alloc(1, 4096);
                SegmentArenaControlLog.Allocation rival = alloc(2, 4096);
                SegmentArenaControlLog.DurableBatch durable = log.appendDurably(List.of(first, rival));
                assertEquals(1, io.forces);
                assertEquals(1, log.durableThrough());
                assertEquals(-1, log.appliedThrough());
                assertFalse(granted(log, 4096, 1, 1, 1));
                List<SegmentArenaControlLog.Result> results = log.applyDurable(durable);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, results.get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED, results.get(1).status());
                assertEquals(0, results.get(0).appliedThrough());
                assertEquals(1, results.get(1).appliedThrough());
                assertTrue(granted(log, 4096, 1, 1, 1));
                assertFalse(granted(log, 4096, 2, 1, 1));
                assertFalse(granted(log, 4096, 1, 2, 1));
                assertFalse(granted(log, 4097, 1, 1, 1));
                assertFalse(granted(log, 4096, 1, 1, 2));
                assertFalse(log.hasAppliedGrant(0, 4096, SegmentArenaControlLog.BLOCK_BYTES + 4096,
                        1, 1, 1, 1));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(granted(reopened, 4096, 1, 1, 1));
                List<SegmentArenaControlLog.Result> retried = reopened.submitBatch(List.of(alloc(1, 4096),
                        alloc(2, 4096)));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE, retried.get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED, retried.get(1).status());
                assertEquals(SegmentArenaControlLog.Status.INCOMPATIBLE, reopened.submitBatch(List.of(
                        alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES))).get(0).status());
                SegmentArenaControlLog.Allocation reusedId = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 10), new Generation(1), 0,
                        0, 1, new Generation(1), 1, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1);
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        reopened.submitBatch(List.of(reusedId)).get(0).status());
            }
        }
    }

    @Test
    public void durableButNotYetAppliedPoolIsResolvedByRestartReplay() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            SegmentArenaControlLog.Allocation pool = new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 3), new Generation(1), 0,
                    0, 2, new Generation(1), 3, 4096, 2, 7);
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                log.appendDurably(List.of(pool));
                assertFalse(granted(log, 4096, 2, 1, 3));
                assertTrue(log.streamFirstSequence(2, 1).isEmpty());
                assertEquals(4096, log.grantedHighWatermark(0));
                assertFalse(granted(log, 4096, 1, 2, 3));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertEquals(0, reopened.durableThrough());
                assertEquals(0, reopened.appliedThrough());
                assertTrue(granted(reopened, 4096, 2, 1, 3));
                assertTrue(granted(reopened, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 2, 1, 3));
                assertEquals(7, reopened.streamFirstSequence(2, 1).orElseThrow());
                assertEquals(4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES,
                        reopened.grantedHighWatermark(0));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        reopened.submitBatch(List.of(pool)).get(0).status());
                long next = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES;
                SegmentArenaControlLog.Allocation wrongStart = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 4), new Generation(1), 0,
                        0, 2, new Generation(1), 4, next, 1, 8);
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        reopened.submitBatch(List.of(wrongStart)).get(0).status());
                assertFalse(granted(reopened, next, 2, 1, 4));
                assertEquals(next, reopened.grantedHighWatermark(0));
                SegmentArenaControlLog.Allocation sameStart = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 5), new Generation(1), 0,
                        0, 2, new Generation(1), 5, next, 1, 7);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        reopened.submitBatch(List.of(sameStart)).get(0).status());
                assertEquals(next + SegmentArenaControlLog.BLOCK_BYTES, reopened.grantedHighWatermark(0));
            }
        }
    }

    @Test
    public void earlierAllocationBodyCannotBeReopenedAsTheNewStreamContract() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            ByteBuffer earlierBody = ByteBuffer.allocate(40).putInt(0).putInt(1).putLong(1)
                    .putLong(5).putLong(4096).putInt(1).putInt(0).flip();
            ByteBuffer earlier = SegmentControlFormat.encode(SegmentControlFormat.Type.ALLOC,
                    new Generation(1), 0, new UUID(0, 5), new Generation(1), 0, earlierBody);
            try (FileChannel file = FileChannel.open(config.directory().resolve("arena-control"),
                    StandardOpenOption.WRITE)) {
                SegmentFileIo io = new SegmentFileIo();
                io.writeFully(file, earlier, 4096);
                io.force(file, true);
            }
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity, 1024 * 1024));
        }
    }

    @Test
    public void failedForceIsUnknownUntilReplayAndPartialTailRemainsNonWritable() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024,
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false))) {
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(4, 4096))));
                assertFalse(granted(log, 4096, 1, 1, 4));
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(5, 4096))));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(granted(reopened, 4096, 1, 1, 4));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        reopened.submitBatch(List.of(alloc(4, 4096))).get(0).status());
            }
        }

        IsolatedSegmentConfig torn = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("torn"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(torn, identity)) {
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(torn, identity, 1024 * 1024,
                    new SegmentFileIo(3, 2, -1, false))) {
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(6, 4096))));
                assertFalse(granted(log, 4096, 1, 1, 6));
            }
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(torn, identity, 1024 * 1024));
        }
    }

    @Test
    public void sharedFailureReservationProtectsLogSpaceAndPersistsAfterAdmissionStops() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 2);
        SegmentFileIo fileIo = new SegmentFileIo();
        CountDownLatch refused = new CountDownLatch(1);
        AtomicReference<Throwable> capacityFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(
                        config, identity, 4096 + 192 + 160, fileIo)) {
            log.submitBatch(List.of(alloc(1, 4096)));
            SegmentArenaControlDriver driver = new SegmentArenaControlDriver(owner, io, log);
            SegmentArenaControlDriver.FailureLease first = driver.reserveFailure(0);
            SegmentArenaControlDriver.FailureLease second = driver.reserveFailure(0);
            assertTrue(first != null && second != null);
            assertThrows(IOException.class, log::close);
            assertTrue(driver.submit(List.of(alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES)),
                    (results, cause) -> {
                        capacityFailure.set(cause);
                        refused.countDown();
                    }));
            owner.start();
            assertTrue(refused.await(5, TimeUnit.SECONDS));
            assertTrue(capacityFailure.get() instanceof IOException);
            owner.stopAdmission();
            assertFalse(owner.awaitTermination(Duration.ofMillis(50)));
            IOException firstFailure = new IOException("Known DATA failure");
            first.failed(firstFailure);
            second.failed(new IOException("Same shared file failure"));
            first.close();
            second.close();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(log.hasDurableQuarantine(0));
            assertFalse(log.dataFileAvailable(0));
            assertTrue(granted(log, 4096, 1, 1, 1));
            assertEquals(1, log.appliedThrough());
            assertEquals(2, fileIo.forces);
            assertEquals(0, firstFailure.getSuppressed().length);
        } finally {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertNull(owner.failure());
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 4096 + 192 + 160)) {
            assertTrue(replay.hasDurableQuarantine(0));
            assertFalse(replay.dataFileAvailable(0));
        }
    }

    @Test
    public void quarantineAppliesInLogOrderAndPreservesOwnershipAcrossReplay() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("quarantine"),
                1, 4, 2, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        SegmentArenaControlLog.Allocation first = new SegmentArenaControlLog.Allocation(
                SegmentControlFormat.Type.ALLOC, new UUID(0, 1), new Generation(1), 0,
                0, 0, new Generation(1), 1, 4096, 1, 1);
        SegmentArenaControlLog.FileQuarantine quarantine = new SegmentArenaControlLog.FileQuarantine(
                new UUID(0, 2), new Generation(1), 0);
        SegmentArenaControlLog.Allocation after = new SegmentArenaControlLog.Allocation(
                SegmentControlFormat.Type.ALLOC, new UUID(0, 3), new Generation(1), 0,
                0, 0, new Generation(1), 3, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1);
        SegmentArenaControlLog.Allocation other = new SegmentArenaControlLog.Allocation(
                SegmentControlFormat.Type.ALLOC, new UUID(0, 4), new Generation(1), 0,
                1, 1, new Generation(1), 4, 4096, 1, 1);
        SegmentFileIo io = new SegmentFileIo();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024, io)) {
            io.forces = 0;
            SegmentArenaControlLog.DurableBatch batch = log.appendDurably(List.of(first, quarantine, after, other));
            assertEquals(1, io.forces);
            assertTrue(log.dataFileAvailable(0));
            assertFalse(log.hasDurableQuarantine(0));
            assertFalse(log.hasAppliedGrant(0, 4096, 4096, 0, 1, 1, 1));
            assertEquals(List.of(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    SegmentArenaControlLog.Status.APPLIED_DURABLE, SegmentArenaControlLog.Status.CONDITION_FAILED,
                    SegmentArenaControlLog.Status.APPLIED_DURABLE),
                    log.applyDurable(batch).stream().map(SegmentArenaControlLog.Result::status).toList());
            assertTrue(log.hasAppliedGrant(0, 4096, 4096, 0, 1, 1, 1));
            assertFalse(log.hasAppliedGrant(0, after.offset(), 4096, 0, 1, 3, 1));
            assertFalse(log.dataFileAvailable(0));
            assertTrue(log.dataFileAvailable(1));
            assertTrue(log.hasAppliedGrant(1, 4096, 4096, 1, 1, 4, 1));
            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                    log.submitBatch(List.of(quarantine)).get(0).status());
            assertEquals(SegmentArenaControlLog.Status.INCOMPATIBLE, log.submitBatch(List.of(
                    new SegmentArenaControlLog.FileQuarantine(new UUID(0, 2), new Generation(1), 1)))
                    .get(0).status());
            assertTrue(log.dataFileAvailable(1));
            assertTrue(log.planPools(List.of(new SegmentArenaControlLog.PoolRequest(
                    0, new Generation(1), 1, 1))).get(0).isEmpty());
            assertTrue(log.appendAddress(0, 4096, 4096, 0, new Generation(1), 1).isEmpty());
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertTrue(replay.hasDurableQuarantine(0));
            assertFalse(replay.dataFileAvailable(0));
            assertTrue(replay.dataFileAvailable(1));
            assertTrue(replay.hasAppliedGrant(0, 4096, 4096, 0, 1, 1, 1));
            assertTrue(replay.hasAppliedGrant(1, 4096, 4096, 1, 1, 4, 1));
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity, replay, new SegmentFileIo()));
        }
    }

    @Test
    public void runtimeQuarantineDoesNotChangeEarlierDurableAllocationResult() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                SegmentArenaControlLog.DurableBatch first = log.appendDurably(List.of(alloc(1, 4096)));
                assertTrue(log.blockDataFile(0));
                assertFalse(log.blockDataFile(0));
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, log.applyDurable(first).get(0).status());
                assertTrue(granted(log, 4096, 1, 1, 1));
                assertFalse(log.dataFileAvailable(0));
                log.appendDurably(List.of(new SegmentArenaControlLog.FileQuarantine(
                        new UUID(0, 2), new Generation(1), 0)));
                assertFalse(log.hasDurableQuarantine(0));
            }
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(granted(replay, 4096, 1, 1, 1));
                assertTrue(replay.hasDurableQuarantine(0));
                assertFalse(replay.dataFileAvailable(0));
            }
        }
    }

    @Test
    public void unsupportedQuarantineActionsAndExtensionsRejectReplay() throws Exception {
        int[][] bodies = {{2, 1, 0, 0}, {1, 2, 0, 0}, {1, 1, -1, 0}, {1, 1, 1, 0}, {1, 1, 0, 1},
                {1, 1, 0}, {1, 1, 0, 0, 0}};
        for (int i = 0; i < bodies.length; i++) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("invalid-quarantine-" + i),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
                ByteBuffer body = ByteBuffer.allocate(4 * bodies[i].length);
                for (int value : bodies[i]) {
                    body.putInt(value);
                }
                ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.DEVICE_STATE,
                        new Generation(1), 0, new UUID(0, 7), new Generation(1), 0, body.flip());
                try (FileChannel channel = FileChannel.open(config.directory().resolve("arena-control"),
                        StandardOpenOption.WRITE)) {
                    SegmentFileIo io = new SegmentFileIo();
                    io.writeFully(channel, record, 4096);
                    io.force(channel, true);
                }
                assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity, 1024 * 1024));
            }
        }
    }

    @Test
    public void wrongIdentityAndFullLogRejectWithoutChangingPreviousGrant() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            long oneRecordCapacity = 4096 + 192;
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, oneRecordCapacity)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        log.submitBatch(List.of(alloc(7, 4096))).get(0).status());
                assertThrows(IOException.class, () -> log.submitBatch(List.of(
                        alloc(8, 4096 + SegmentArenaControlLog.BLOCK_BYTES))));
                assertTrue(granted(log, 4096, 1, 1, 7));
            }
            byte[] before = Files.readAllBytes(config.directory().resolve("arena-control"));
            SegmentFileHeader wrong = new SegmentFileHeader(identity.incarnation(), new UUID(0, 10),
                    identity.bookieId());
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, wrong, oneRecordCapacity));
            assertEquals(java.util.Arrays.hashCode(before),
                    java.util.Arrays.hashCode(Files.readAllBytes(config.directory().resolve("arena-control"))));
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity,
                    oneRecordCapacity)) {
                assertTrue(granted(reopened, 4096, 1, 1, 7));
            }
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                data.write(ByteBuffer.wrap(new byte[] {0}), 0);
                data.force(true);
            }
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity,
                    oneRecordCapacity));
        }
    }
}

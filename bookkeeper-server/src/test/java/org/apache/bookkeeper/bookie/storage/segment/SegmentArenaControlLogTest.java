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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

    private SegmentFileHeader identity() {
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
                assertFalse(granted(log, 4096, 1, 2, 3));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertEquals(0, reopened.durableThrough());
                assertEquals(0, reopened.appliedThrough());
                assertTrue(granted(reopened, 4096, 2, 1, 3));
                assertTrue(granted(reopened, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 2, 1, 3));
                assertEquals(7, reopened.streamFirstSequence(2, 1).orElseThrow());
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        reopened.submitBatch(List.of(pool)).get(0).status());
                long next = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES;
                SegmentArenaControlLog.Allocation wrongStart = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 4), new Generation(1), 0,
                        0, 2, new Generation(1), 4, next, 1, 8);
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        reopened.submitBatch(List.of(wrongStart)).get(0).status());
                assertFalse(granted(reopened, next, 2, 1, 4));
                SegmentArenaControlLog.Allocation sameStart = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 5), new Generation(1), 0,
                        0, 2, new Generation(1), 5, next, 1, 7);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        reopened.submitBatch(List.of(sameStart)).get(0).status());
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

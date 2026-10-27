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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentBatchFormatTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private IsolatedSegmentConfig config() {
        return IsolatedSegmentConfig.defaults(temporary.getRoot().toPath().resolve("arena"));
    }

    private SegmentBatchFormat.Address address(long offset) {
        return new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 1, offset);
    }

    private ProfileLedgerContext context() {
        return context(8);
    }

    private ProfileLedgerContext context(long ledgerId) {
        byte[] instance = new byte[16];
        for (int i = 0; i < instance.length; i++) {
            instance[i] = (byte) (i + 1);
        }
        byte[] descriptor = new byte[36];
        java.util.Arrays.fill(descriptor, (byte) 3);
        return new ProfileLedgerContext(ledgerId, instance, descriptor);
    }

    private SegmentFileHeader identity() {
        return new SegmentFileHeader(new StorageIncarnation(context().ledgerInstanceId()),
                new UUID(0, 9), BookieId.parse("isolated:3181"));
    }

    private ByteBuf entry(byte[] payload) throws Exception {
        return entry(payload, 3);
    }

    private ByteBuf entry(byte[] payload, long entryId) throws Exception {
        return entry(payload, entryId, 2, payload.length);
    }

    private ByteBuf entry(byte[] payload, long entryId, long lac, long ledgerLength) throws Exception {
        return entry(payload, entryId, lac, ledgerLength, 8);
    }

    private ByteBuf entry(byte[] payload, long entryId, long lac, long ledgerLength, long ledgerId) throws Exception {
        DigestManager digest = DigestManager.instantiate(ledgerId, new byte[0], DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        ByteBufList encoded = (ByteBufList) digest.computeDigestAndPackageForSending(entryId, lac, ledgerLength,
                Unpooled.wrappedBuffer(payload), new byte[0], 0);
        try {
            ByteBuf copy = Unpooled.buffer(encoded.readableBytes());
            for (int i = 0; i < encoded.size(); i++) {
                ByteBuf part = encoded.getBuffer(i);
                copy.writeBytes(part, part.readerIndex(), part.readableBytes());
            }
            return copy;
        } finally {
            encoded.release();
        }
    }

    @Test
    public void dataSessionReservesItsCleanCloseAndReopensWithANewEpoch() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("session-clean"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        long capacity = 4096 + 160 + 192 + 160;
        SegmentFileIo controlIo = new SegmentFileIo();
        ByteBuf entry = entry(new byte[] {1, 2, 3});
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), capacity, controlIo)) {
            files.startSession(arena);
            assertEquals(1, arena.dataSessionEpoch());
            assertTrue(arena.hasUnclosedDataSession());
            assertThrows(IOException.class, arena::close);
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
            assertThrows(IOException.class, () -> arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC, new UUID(0, 6), new Generation(1), 0,
                    0, 0, new Generation(1), 6, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1))));
            SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            assertThrows(IOException.class, () -> files.write(batch));
            files.writeAuthorized(batch, arena);
            assertEquals(1, files.dataIo.forces);
            assertEquals(2, controlIo.forces);
            files.close();
            assertFalse(arena.hasUnclosedDataSession());
            assertEquals(3, controlIo.forces);
        } finally {
            entry.release();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), replay, new SegmentFileIo(), 1, 1)) {
            assertEquals(1, replay.dataSessionEpoch());
            assertFalse(replay.hasUnclosedDataSession());
            assertEquals(1, reopened.startupInventory().streamCandidate(replay, 0, new Generation(1)).through());
            assertFalse(reopened.supportsLocalResults());
            reopened.startSession(replay);
            assertEquals(2, replay.dataSessionEpoch());
            assertEquals(4096 + SegmentArenaControlLog.BLOCK_BYTES, reopened.nextOffset(0));
            reopened.close();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(2, replay.dataSessionEpoch());
            assertFalse(replay.hasUnclosedDataSession());
        }
    }

    @Test
    public void dataSessionUnknownBeginAndCloseHaveDifferentRecoveryOutcomes() throws Exception {
        for (boolean failBegin : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("session-unknown-" + failBegin), 1, 4, 1,
                    4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, failBegin ? 1 : 2, false);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024,
                            controlIo)) {
                if (failBegin) {
                    assertThrows(IOException.class, () -> files.startSession(arena));
                    assertThrows(IOException.class, () -> files.startSession(arena));
                    files.close();
                } else {
                    files.startSession(arena);
                    assertThrows(IOException.class, files::close);
                    assertTrue(arena.hasUnclosedDataSession());
                }
            }
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertEquals(1, replay.dataSessionEpoch());
                assertEquals(failBegin, replay.hasUnclosedDataSession());
                if (failBegin) {
                    SegmentFileIo syncIo = new SegmentFileIo();
                    assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                            config, identity(), replay, syncIo));
                    assertEquals(0, syncIo.forces);
                } else {
                    try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnly(
                            config, identity(), replay, new SegmentFileIo())) {
                        assertFalse(reopened.supportsLocalResults());
                    }
                }
            }
        }
    }

    @Test
    public void dataSessionCloseWaitsForActualIoAndRetainsWriterExclusion() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("session-pending"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ByteBuf entry = entry(new byte[] {1});
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            files.startSession(arena);
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            });
            FutureTask<Void> write = new FutureTask<>(() -> {
                files.writeAuthorized(SegmentBatchFormat.encode(config, address(4096),
                        List.of(SegmentBatchFormat.entry(context(), entry))), arena);
                return null;
            });
            new Thread(write, "session-data-io").start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, files::close);
                assertThrows(IOException.class, arena::close);
                assertTrue(arena.hasUnclosedDataSession());
                assertEquals("LOCKED", probeWriterLock(config.directory()));
            } finally {
                release.countDown();
                write.get(5, TimeUnit.SECONDS);
            }
            files.close();
            assertFalse(arena.hasUnclosedDataSession());
            assertEquals("ACQUIRED", probeWriterLock(config.directory()));
        } finally {
            entry.release();
        }
    }

    @Test
    public void failedOrUnsubmittedDataSessionNeverProducesACleanClose() throws Exception {
        for (boolean submit : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("session-data-failed-" + submit), 1, 4, 1,
                    4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            ByteBuf entry = entry(new byte[] {1});
            SegmentFileIo controlIo = new SegmentFileIo();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024,
                            controlIo)) {
                files.startSession(arena);
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
                IsolatedSegmentFiles.Reservation reservation = files.reserveAuthorized(
                        SegmentBatchFormat.encode(config, address(4096),
                                List.of(SegmentBatchFormat.entry(context(), entry))), arena);
                if (submit) {
                    files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                    assertThrows(IOException.class, () -> files.writeReserved(reservation));
                } else {
                    files.cancelBeforeSubmission(reservation);
                }
                assertFalse(arena.dataFileAvailable(0));
                assertFalse(arena.hasDurableQuarantine(0));
                assertThrows(IOException.class, files::close);
                assertTrue(arena.hasUnclosedDataSession());
                assertEquals(2, controlIo.forces);
            } finally {
                entry.release();
            }
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertTrue(replay.hasUnclosedDataSession());
                assertFalse(replay.hasDurableQuarantine(0));
                assertTrue(replay.hasAppliedGrant(0, 4096, 4096, 0, 1, 5, 1));
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                        config, identity(), replay, new SegmentFileIo()));
            }
        }
    }

    @Test
    public void pooledEncodingClearsReusedPaddingAndRefusesUnavailableWorkingSet() throws Exception {
        IsolatedSegmentConfig config = config();
        byte[] payload = new byte[3000];
        java.util.Arrays.fill(payload, (byte) 0x5a);
        ByteBuf large = entry(payload);
        ByteBuf small = entry(new byte[] {7});
        ByteBuf tooLarge = entry(new byte[4000]);
        try (SegmentBufferPool pool = new SegmentBufferPool(0, 1, 1, 4096, 4096, false, () -> { })) {
            SegmentBatchFormat.FrozenBatch first = SegmentBatchFormat.encodePooled(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), large)), pool);
            try {
                assertTrue(first.bytes().isDirect());
                assertEquals(0, first.bytes().alignmentOffset(0, 4096));
                assertEquals(SegmentBatchFormat.encode(config, address(4096),
                        List.of(SegmentBatchFormat.entry(context(), large))).bytes(), first.bytes());
                assertNull(SegmentBatchFormat.encodePooled(config, address(4096),
                        List.of(SegmentBatchFormat.entry(context(), small)), pool));
            } finally {
                first.close();
            }
            assertThrows(IllegalStateException.class, first::bytes);
            assertNull(SegmentBatchFormat.encodePooled(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), tooLarge)), pool));
            try (SegmentBatchFormat.FrozenBatch reused = SegmentBatchFormat.encodePooled(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), small)), pool)) {
                assertEquals(SegmentBatchFormat.encode(config, address(4096),
                        List.of(SegmentBatchFormat.entry(context(), small))).bytes(), reused.bytes());
                assertEquals(1, SegmentBatchParser.parse(reused.bytes(), address(4096), config.bytesPerFile())
                        .records().size());
                try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
                    assertThrows(IllegalStateException.class, () -> files.reserve(reused));
                    assertEquals(4096, files.nextOffset(0));
                }
            }
            small.setByte(36, 8);
            assertThrows(IllegalArgumentException.class, () -> SegmentBatchFormat.encodePooled(config,
                    address(4096), List.of(SegmentBatchFormat.entry(context(), small)), pool));
            pool.drainReturns();
            assertEquals(0, pool.outstanding());
        } finally {
            large.release();
            small.release();
            tooLarge.release();
        }
    }

    @Test
    public void pooledBatchParsingRejectsAnInsufficientRecordAllowanceWithoutTruncating() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentBatchFormat.Address address = address(4096);
        byte[] firstPayload = {1, 2, 3};
        byte[] secondPayload = {4, 5};
        ByteBuf first = entry(firstPayload, 3, 2, firstPayload.length, 8);
        ByteBuf second = entry(secondPayload, 7, 6, secondPayload.length, 9);
        try (SegmentBufferPool pool = new SegmentBufferPool(0, 1, 1, 4096, 4096, false, () -> { })) {
            try (SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encodePooled(config, address,
                    List.of(SegmentBatchFormat.entry(context(8), first),
                            SegmentBatchFormat.entry(context(9), second)), pool)) {
                assertTrue(batch.bytes().isDirect());
                IOException refusal = assertThrows(IOException.class, () -> SegmentBatchParser.parse(
                        batch.bytes(), address, config.bytesPerFile(), 1));
                assertEquals("Batch record count exceeds metadata allowance", refusal.getMessage());
                for (SegmentBatchParser.Batch parsed : List.of(
                        SegmentBatchParser.parse(batch.bytes(), address, config.bytesPerFile(), 2),
                        SegmentBatchParser.parse(batch.bytes(), address, config.bytesPerFile()))) {
                    assertEquals(2, parsed.records().size());
                    SegmentBatchParser.Record firstRecord = parsed.records().get(0);
                    SegmentBatchParser.Record secondRecord = parsed.records().get(1);
                    assertEquals(context(8), firstRecord.context());
                    assertEquals(context(9), secondRecord.context());
                    assertEquals(3, firstRecord.entryId());
                    assertEquals(7, secondRecord.entryId());
                    assertArrayEquals(firstPayload, bytes(firstRecord.entry().slice(
                            SegmentBatchFormat.ENTRY_OVERHEAD, firstPayload.length)));
                    assertArrayEquals(secondPayload, bytes(secondRecord.entry().slice(
                            SegmentBatchFormat.ENTRY_OVERHEAD, secondPayload.length)));
                }
            }
            pool.drainReturns();
            assertEquals(0, pool.outstanding());
        } finally {
            first.release();
            second.release();
        }
    }

    @Test
    public void dataReservationAndCloseDecisionDoNotWaitForFileForce() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("data-lock"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[16], 1);
        ByteBuf secondEntry = entry(new byte[16], 2);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch second;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 2, 8192),
                    List.of(SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger forces = new AtomicInteger();
        SegmentFileIo slow = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (forces.getAndIncrement() == 0) {
                forcing.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("DATA force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        });
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            files.dataIo = slow;
            IsolatedSegmentFiles.Reservation firstReservation = files.reserve(first);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    files.writeReserved(firstReservation);
                } catch (Throwable cause) {
                    failure.set(cause);
                }
            });
            writer.start();
            IsolatedSegmentFiles.Reservation secondReservation;
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<IsolatedSegmentFiles.Reservation> reserve = new FutureTask<>(() -> files.reserve(second));
                new Thread(reserve).start();
                secondReservation = reserve.get(1, TimeUnit.SECONDS);
                assertEquals(8192, secondReservation.address().offset());
                FutureTask<Throwable> close = new FutureTask<>(() -> {
                    try {
                        files.close();
                        return null;
                    } catch (Throwable cause) {
                        return cause;
                    }
                });
                new Thread(close).start();
                assertTrue(close.get(1, TimeUnit.SECONDS) instanceof IOException);
            } finally {
                release.countDown();
                writer.join(5000);
            }
            assertFalse(writer.isAlive());
            assertNull(failure.get());
            assertTrue(firstReservation.durable());
            assertThrows(IOException.class, files::close);
            files.writeReserved(secondReservation);
            assertTrue(secondReservation.durable());
            assertEquals(12288, files.nextOffset(0));
        }
    }

    @Test
    public void rejectedPhysicalIoRetiresReservationWithoutReusingItsCoordinates() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("rejected-data-io"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024 * 1024, 1, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[8], 1);
        ByteBuf secondEntry = entry(new byte[8], 2);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch second;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 2, 8192),
                    List.of(SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1);
        io.shutdown();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 2, 1)));
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 1),
                    new SegmentHotDirectory(owner, config, identity(), 1));
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submit(first, (locations, cause) -> {
                    failure.set(cause);
                    completed.countDown();
                }));
                owner.start();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof IOException);
                assertEquals(8192, files.nextOffset(0));
                assertThrows(IOException.class, () -> files.reserveAuthorized(first, arena));
                assertFalse(arena.dataFileAvailable(0));
                assertFalse(arena.hasDurableQuarantine(0)); // This manual prototype has no control driver.
                assertTrue(arena.hasAppliedGrant(0, 4096, 8192, 0, 1, 5, 1));
                assertThrows(IOException.class, () -> files.reserveAuthorized(second, arena));
                assertEquals(8192, files.nextOffset(0));
            } finally {
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
            files.close();
        }
    }

    @Test
    public void physicalDriverPublishesOnlyContiguousForcedCandidates() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("driver"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[16], 1);
        ByteBuf secondEntry = entry(new byte[16], 2);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch second;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 2, 8192),
                    List.of(SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2);
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> firstReady = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> secondReady = new AtomicReference<>();
        List<Long> published = Collections.synchronizedList(new ArrayList<>());
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 2, 1)));
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, authority,
                    prefix, hot);
            SegmentFileHeader anotherIdentity = new SegmentFileHeader(
                    new StorageIncarnation(context().ledgerInstanceId()), new UUID(0, 10),
                    BookieId.parse("isolated:3181"));
            assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(0, owner, io, files,
                    authority, prefix, new SegmentHotDirectory(owner, config, anotherIdentity, 2)));
            assertTrue(driver.submit(first, (candidates, cause) -> {
                owner.requireOwner();
                if (cause != null) {
                    failure.compareAndSet(null, cause);
                }
                firstReady.set(candidates);
                published.add(1L);
                done.countDown();
            }));
            assertTrue(driver.submit(second, (candidates, cause) -> {
                owner.requireOwner();
                if (cause != null) {
                    failure.compareAndSet(null, cause);
                }
                secondReady.set(candidates);
                published.add(2L);
                done.countDown();
            }));
            owner.start();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertEquals(List.of(1L, 2L), published);
            assertEquals(1, firstReady.get().size());
            assertEquals(1, secondReady.get().size());
            assertEquals(1, SegmentPointRead.read(config, identity(), authority, firstReady.get().get(0))
                    .record().entryId());
            assertEquals(2, SegmentPointRead.read(config, identity(), authority, secondReady.get().get(0))
                    .record().entryId());
        } finally {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertNull(owner.failure());
    }

    @Test
    public void physicalDriverPublishesTwoReadableEntriesAfterOneBatchForce() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("driver-batch"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[16], 1);
        ByteBuf secondEntry = entry(new byte[16], 2);
        SegmentBatchFormat.FrozenBatch batch;
        try {
            batch = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry),
                            SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        SegmentExecutionGroup.Settings settings = new SegmentExecutionGroup.Settings(
                1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5));
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> published = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 1, 1)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config, settings);
            try {
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, group.owner(0), group.io(),
                        files, authority, new SegmentPhysicalPrefix(group.owner(0), 0, new Generation(1), 1, 1),
                        new SegmentHotDirectory(group.owner(0), config, identity(), 2));
                assertTrue(driver.submit(batch, (locations, cause) -> {
                    group.owner(0).requireOwner();
                    published.set(locations);
                    failure.set(cause);
                    done.countDown();
                }));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(1, files.dataIo.forces);
                assertEquals(2, published.get().size());
                assertEquals(1, SegmentPointRead.read(config, identity(), authority, published.get().get(0))
                        .record().entryId());
                assertEquals(2, SegmentPointRead.read(config, identity(), authority, published.get().get(1))
                        .record().entryId());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void physicalDriverForceFailureKeepsCoordinatesAndReportsNoCandidates() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("driver-fail"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[16], 1);
        ByteBuf secondEntry = entry(new byte[16], 2);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch second;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 2, 8192),
                    List.of(SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 2);
        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2);
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> firstCandidates = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> secondCandidates = new AtomicReference<>();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 2, 1)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false, () -> {
                forcing.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Failed force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            });
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, authority,
                    prefix, hot);
            assertTrue(driver.submit(first, (ready, cause) -> {
                owner.requireOwner();
                firstCandidates.set(ready);
                firstFailure.set(cause);
                done.countDown();
            }));
            assertTrue(driver.submit(second, (ready, cause) -> {
                owner.requireOwner();
                secondCandidates.set(ready);
                secondFailure.set(cause);
                done.countDown();
            }));
            owner.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (files.nextOffset(0) != 12288 && System.nanoTime() < end) {
                    Thread.onSpinWait();
                }
                assertEquals(12288, files.nextOffset(0));
            } finally {
                releaseForce.countDown();
            }
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertTrue(firstFailure.get() instanceof IOException);
            assertTrue(secondFailure.get() instanceof IOException);
            assertNull(firstCandidates.get());
            assertNull(secondCandidates.get());
            assertEquals(12288, files.nextOffset(0));
            CountDownLatch checked = new CountDownLatch(1);
            assertTrue(owner.control(() -> {
                assertTrue(prefix.failed());
                assertEquals(0, hot.candidateCount());
                checked.countDown();
            }));
            assertTrue(checked.await(5, TimeUnit.SECONDS));
        } finally {
            releaseForce.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        assertNull(owner.failure());
    }

    @Test
    public void executionGroupDrainWaitsForRealPhysicalDriverForceAndCompletion() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("driver-drain"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[16], 1);
        SegmentBatchFormat.FrozenBatch batch;
        try {
            batch = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
        } finally {
            entry.release();
        }
        SegmentExecutionGroup.Settings settings = new SegmentExecutionGroup.Settings(
                1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5));
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> published = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 1, 1)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("DATA force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config, settings);
            try {
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, group.owner(0), group.io(),
                        files, authority, new SegmentPhysicalPrefix(group.owner(0), 0, new Generation(1), 1, 1),
                        new SegmentHotDirectory(group.owner(0), config, identity(), 1));
                assertTrue(driver.submit(batch, (locations, cause) -> {
                    group.owner(0).requireOwner();
                    published.set(locations);
                    failure.set(cause);
                    done.countDown();
                }));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertFalse(group.shutdown(Duration.ofMillis(20)));
                assertThrows(IOException.class, files::close);
                releaseForce.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(1, published.get().size());
                assertEquals(1, SegmentPointRead.read(config, identity(), authority, published.get().get(0))
                        .record().entryId());
            } finally {
                releaseForce.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void writerAndIndependentParserMatchFixedVectors() throws Exception {
        for (boolean relocation : new boolean[] {false, true}) {
            ByteBuf entry = entry("abc".getBytes(StandardCharsets.UTF_8), 3, 2, 3, 7);
            try {
                SegmentBatchFormat.Data record = relocation
                        ? SegmentBatchFormat.relocation(context(7), entry, new UUID(1, 2), new Generation(4))
                        : SegmentBatchFormat.entry(context(7), entry);
                SegmentBatchFormat.FrozenBatch frozen = SegmentBatchFormat.encode(config(), address(4096),
                        List.of(record));
                String name = relocation ? "copy-v1.hex" : "entry-v1.hex";
                byte[] golden;
                try (var input = getClass().getResourceAsStream("/unified-wal-segment/" + name)) {
                    golden = HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                            .lines().filter(line -> !line.startsWith("#"))
                            .collect(java.util.stream.Collectors.joining()));
                }
                assertArrayEquals(golden, bytes(frozen.bytes()));
                SegmentBatchParser.Record parsed = SegmentBatchParser.parse(ByteBuffer.wrap(golden), address(4096),
                        config().bytesPerFile()).records().get(0);
                assertEquals(context(7), parsed.context());
                assertEquals(relocation ? 2 : 1, parsed.origin());
                assertEquals(relocation ? new UUID(1, 2) : new UUID(0, 0), parsed.moveId());
                assertEquals(3, parsed.entryId());
                assertThrows(ReadOnlyBufferException.class, () -> frozen.bytes().put(0, (byte) 1));
                entry.setByte(entry.writerIndex() - 1, 'x');
                assertArrayEquals(golden, bytes(frozen.bytes()));
            } finally {
                entry.release();
            }
        }
    }

    @Test
    public void truncatedCorruptUnknownAndMismatchedRecordsFailClosed() throws Exception {
        ByteBuf entry = entry(new byte[17]);
        try {
            ByteBuffer good = SegmentBatchFormat.encode(config(), address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry))).bytes();
            for (int cut : new int[] {0, 8, 127, 128, 200, 4095}) {
                assertThrows(IOException.class, () -> SegmentBatchParser.parse(good.slice(0, cut), address(4096),
                        config().bytesPerFile()));
            }
            for (int offset : new int[] {0, 8, 72, 124, 128, 134, 140, 168, 284, 330, 4092}) {
                ByteBuffer bad = ByteBuffer.wrap(bytes(good));
                bad.put(offset, (byte) (bad.get(offset) ^ 1));
                assertThrows(IOException.class, () -> SegmentBatchParser.parse(bad, address(4096),
                        config().bytesPerFile()));
            }
            ByteBuffer large = ByteBuffer.wrap(bytes(good));
            large.putInt(72, Integer.MAX_VALUE);
            large.putInt(124, crc(large, 0, 124));
            assertThrows(IOException.class, () -> SegmentBatchParser.batchLength(large, address(4096),
                    config().bytesPerFile()));
            SegmentBatchFormat.Address generation2 = new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(2), new Generation(1), 1, 4096);
            assertThrows(IOException.class, () -> SegmentBatchParser.parse(good, generation2, config().bytesPerFile()));
            ByteBuffer record = ByteBuffer.wrap(bytes(good.slice(128, 160 + 36 + 17)));
            record.put(6, (byte) 9);
            record.putInt(156, crc(record, 0, 156));
            assertThrows(IOException.class, () -> SegmentBatchParser.parseRecord(record, address(4096), 4224));
        } finally {
            entry.release();
        }
    }

    @Test
    public void entryLimitsAndOverflowAreCheckedBeforeBatchAllocation() throws Exception {
        int n = SegmentBatchFormat.maxPayload(config());
        for (int length : new int[] {n - 1, n, n + 1}) {
            ByteBuf entry = entry(new byte[length]);
            try {
                if (length <= n) {
                    SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config(), address(4096),
                            List.of(SegmentBatchFormat.entry(context(), entry)));
                    assertEquals(length + 36, SegmentBatchParser.parse(batch.bytes(), address(4096),
                            config().bytesPerFile()).records().get(0).entry().remaining());
                } else {
                    assertThrows(IllegalArgumentException.class,
                            () -> SegmentBatchFormat.encode(config(), address(4096),
                            List.of(SegmentBatchFormat.entry(context(), entry))));
                }
            } finally {
                entry.release();
            }
        }
        ByteBuf small = entry(new byte[0]);
        try {
            assertThrows(ArithmeticException.class, () -> SegmentBatchFormat.encode(config(),
                    address(Long.MAX_VALUE - 4095), List.of(SegmentBatchFormat.entry(context(), small))));
        } finally {
            small.release();
        }
        // Every length in the historical normal/recovery gap is rejected by size before CRC or copying.
        ByteBuf oversized = entry(new byte[5242712]);
        try {
            for (int length = 5242669; length <= 5242712; length++) {
                oversized.writerIndex(length + 36);
                IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> SegmentBatchFormat.encode(config(), address(4096),
                                List.of(SegmentBatchFormat.entry(context(), oversized))));
                assertTrue(failure.getMessage().contains("size"));
            }
        } finally {
            oversized.release();
        }
    }

    @Test
    public void shortWritesReachForceAndPointReadNeedsOnlyOneRecord() throws Exception {
        IsolatedSegmentConfig config = config();
        ByteBuf entry = entry(new byte[1024]);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            files.dataIo = new SegmentFileIo(7, -1, -1, false);
            SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry), SegmentBatchFormat.entry(context(), entry)));
            files.write(batch);
            assertTrue(files.dataIo.writes > 1);
            assertEquals(1, files.dataIo.forces);
            assertEquals(8192, files.nextOffset(0));
            assertThrows(IOException.class, () -> files.write(batch));
            for (int file = 0; file < config.dataFiles(); file++) {
                assertEquals(config.bytesPerFile(), Files.size(IsolatedSegmentFiles.dataPath(config, file)));
            }
            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                ByteBuffer header = ByteBuffer.allocate(160);
                SegmentFileIo.readFully(channel, header, 4224);
                int length = SegmentBatchParser.recordLength(header);
                ByteBuffer record = ByteBuffer.allocate(length);
                SegmentFileIo.readFully(channel, record, 4224);
                assertEquals(1024 + 36,
                        SegmentBatchParser.parseRecord(record, address(4096), 4224).entry().remaining());
                assertTrue(length < batch.bytes().remaining());
                ByteBuffer identityBytes = ByteBuffer.allocateDirect(4096);
                SegmentFileIo.readFully(channel, identityBytes, 0);
                identity().validate(identityBytes.asReadOnlyBuffer(), config, 0);
            }
        } finally {
            entry.release();
        }
    }

    @Test
    public void grantedPointReadChecksTheRealRecordWithoutReadingTheWholeBatch() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("point-read"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf filler = entry(new byte[3676], 2);
        ByteBuf small = entry(new byte[1024], 3);
        ByteBuf large = entry(new byte[256 * 1024], 4);
        SegmentPointRead.Location smallLocation;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentBatchFormat.FrozenBatch frozen = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), filler), SegmentBatchFormat.entry(context(), small),
                            SegmentBatchFormat.entry(context(), large)));
            assertEquals(270336, frozen.bytes().remaining());
            smallLocation = SegmentPointRead.locations(
                    SegmentBatchParser.parse(frozen.bytes(), address(4096), config.bytesPerFile())).get(1);
            assertEquals(4000, smallLocation.recordOffset() % 4096);
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), authority, smallLocation));
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), authority, smallLocation));
            files.writeAuthorized(frozen, authority);
            SegmentPointRead.Result read = SegmentPointRead.read(config, identity(), authority, smallLocation);
            byte[] expected = new byte[small.readableBytes()];
            small.getBytes(small.readerIndex(), expected);
            assertArrayEquals(expected, bytes(read.record().entry()));
            assertTrue(read.bytesRead() < frozen.bytes().remaining());
            assertEquals(3, read.record().entryId());
            SegmentPointRead.Location wrongEntry = new SegmentPointRead.Location(smallLocation.address(),
                    smallLocation.batchLength(), smallLocation.recordOffset(), smallLocation.recordLength(),
                    smallLocation.context(), 4);
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), authority, wrongEntry));
            SegmentPointRead.Location unbounded = new SegmentPointRead.Location(smallLocation.address(),
                    smallLocation.batchLength(), smallLocation.recordOffset(), Integer.MAX_VALUE,
                    smallLocation.context(), 3);
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), authority, unbounded));
        } finally {
            filler.release();
            small.release();
            large.release();
        }
        IsolatedSegmentConfig lowerAdmission = new IsolatedSegmentConfig(config.directory(), 1, 4, 1,
                config.bytesPerFile(), 4096, 8, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(lowerAdmission, identity(), 1024 * 1024)) {
            SegmentPointRead.Result afterReopen = SegmentPointRead.read(lowerAdmission, identity(), replay,
                    smallLocation);
            assertEquals(3, afterReopen.record().entryId());
            assertEquals(1024 + 36, afterReopen.record().entry().remaining());
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                data.write(ByteBuffer.wrap(new byte[] {1}), smallLocation.recordOffset() + 160 + 36);
                data.force(false);
            }
            assertThrows(IOException.class, () -> SegmentPointRead.read(lowerAdmission, identity(), replay,
                    smallLocation));
        }
    }

    @Test
    public void recoveredPhysicalStreamCandidateRejectsGapDuplicateAndWrongStart() throws Exception {
        for (long secondSequence : new long[] {12, 13, 11}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("stream-candidate-" + secondSequence),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2,
                    "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            ByteBuf firstEntry = entry(new byte[16], 11);
            ByteBuf secondEntry = entry(new byte[16], secondSequence);
            SegmentBatchFormat.FrozenBatch first;
            SegmentBatchFormat.FrozenBatch second;
            try {
                first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                        0, 0, 5, new Generation(1), new Generation(1), 11, 4096),
                        List.of(SegmentBatchFormat.entry(context(), firstEntry)));
                second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                        0, 0, 5, new Generation(1), new Generation(1), secondSequence, 8192),
                        List.of(SegmentBatchFormat.entry(context(), secondEntry)));
            } finally {
                firstEntry.release();
                secondEntry.release();
            }
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                        0, 0, new Generation(1), 5, 4096, 1, 11)));
                files.writeAuthorized(first, authority);
                files.writeAuthorized(second, authority);
            }
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                SegmentFileIo sync = new SegmentFileIo();
                if (secondSequence == 12) {
                    SegmentDataRecovery.PhysicalStreamCandidate candidate =
                            SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), replay, sync,
                                    0, new Generation(1), 11, 2, 2);
                    assertEquals(12, candidate.through());
                    assertEquals(2, candidate.scan().batches());
                    assertEquals(2, candidate.entries().size());
                    assertEquals(List.of(4096 + SegmentArenaControlLog.BLOCK_BYTES),
                            candidate.grantedHighWatermarks());
                    assertEquals(11, SegmentPointRead.read(config, identity(), replay, candidate.entries().get(0))
                            .record().entryId());
                    assertEquals(12, SegmentPointRead.read(config, identity(), replay, candidate.entries().get(1))
                            .record().entryId());
                    assertEquals(1, sync.forces);
                    SegmentOwnerLoop recoveredOwner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
                    CountDownLatch restored = new CountDownLatch(1);
                    AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
                    AtomicReference<List<SegmentPointRead.Location>> hotLocations = new AtomicReference<>();
                    recoveredOwner.start();
                    try {
                        assertTrue(recoveredOwner.control(() -> {
                            try {
                                SegmentHotDirectory tooSmall = new SegmentHotDirectory(recoveredOwner, config,
                                        identity(), 1);
                                assertThrows(IOException.class, () -> tooSmall.restorePhysical(candidate));
                                assertEquals(0, tooSmall.candidateCount());
                                IsolatedSegmentConfig anotherDirectory = new IsolatedSegmentConfig(
                                        config.directory().resolveSibling("another-directory"),
                                        config.arenaCount(), config.logicalShards(), config.dataFiles(),
                                        config.bytesPerFile(), config.alignment(), config.maxPayloadBytes(),
                                        config.maxPendingEntries(), config.backend(), config.affinity(),
                                        config.numaPolicy(), config.waitStrategy());
                                SegmentHotDirectory wrongDirectory = new SegmentHotDirectory(recoveredOwner,
                                        anotherDirectory, identity(), 2);
                                assertThrows(IOException.class, () -> wrongDirectory.restorePhysical(candidate));
                                assertEquals(0, wrongDirectory.candidateCount());
                                SegmentFileHeader anotherIdentity = new SegmentFileHeader(
                                        new StorageIncarnation(context().ledgerInstanceId()), new UUID(0, 10),
                                        BookieId.parse("isolated:3181"));
                                SegmentHotDirectory wrongIdentity = new SegmentHotDirectory(recoveredOwner, config,
                                        anotherIdentity, 2);
                                assertThrows(IOException.class, () -> wrongIdentity.restorePhysical(candidate));
                                assertEquals(0, wrongIdentity.candidateCount());
                                IsolatedSegmentConfig lowerAdmission = new IsolatedSegmentConfig(config.directory(),
                                        config.arenaCount(), config.logicalShards(), config.dataFiles(),
                                        config.bytesPerFile(), config.alignment(), 8, config.maxPendingEntries(),
                                        config.backend(), config.affinity(), config.numaPolicy(),
                                        config.waitStrategy());
                                SegmentHotDirectory reconfigured = new SegmentHotDirectory(recoveredOwner,
                                        lowerAdmission, identity(), 2);
                                reconfigured.restorePhysical(candidate);
                                assertEquals(2, reconfigured.candidateCount());
                                SegmentHotDirectory inFlight = new SegmentHotDirectory(recoveredOwner, config,
                                        identity(), 2);
                                inFlight.reserve(first);
                                assertThrows(IOException.class, () -> inFlight.restorePhysical(candidate));
                                assertEquals(0, inFlight.candidateCount());
                                SegmentHotDirectory hot = new SegmentHotDirectory(recoveredOwner, config,
                                        identity(), 2);
                                hot.restorePhysical(candidate);
                                assertThrows(IOException.class, () -> hot.restorePhysical(candidate));
                                assertEquals(2, hot.candidateCount());
                                assertTrue(hot.liveDurableCandidate(context(), 11).isEmpty());
                                assertTrue(hot.liveDurableCandidate(context(), 12).isEmpty());
                                hotLocations.set(List.of(hot.candidate(context(), 11).orElseThrow(),
                                        hot.candidate(context(), 12).orElseThrow()));
                            } catch (Throwable failure) {
                                restoreFailure.set(failure);
                            } finally {
                                restored.countDown();
                            }
                        }));
                        assertTrue(restored.await(5, TimeUnit.SECONDS));
                        assertNull(restoreFailure.get());
                        assertEquals(11, SegmentPointRead.read(config, identity(), replay, hotLocations.get().get(0))
                                .record().entryId());
                        assertEquals(12, SegmentPointRead.read(config, identity(), replay, hotLocations.get().get(1))
                                .record().entryId());
                    } finally {
                        recoveredOwner.stopAdmission();
                        assertTrue(recoveredOwner.awaitTermination(Duration.ofSeconds(5)));
                    }
                    assertNull(recoveredOwner.failure());
                    assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                            config, identity(), replay, new SegmentFileIo(), 0, new Generation(1), 10, 2, 2));
                    assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                            config, identity(), replay, new SegmentFileIo(), 0, new Generation(1), 11, 1, 2));
                } else {
                    assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                            config, identity(), replay, sync, 0, new Generation(1), 11, 2, 2));
                }
            }
        }
    }

    @Test
    public void pooledPointReadReusesScratchAcrossPageBoundariesAndRejectsCorruption() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("read-scratch"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf filler = entry(new byte[3676], 2);
        ByteBuf small = entry(new byte[1024], 3);
        ByteBuf maximum = entry(new byte[1024 * 1024], 4);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBufferPool pool = new SegmentBufferPool(0, 1, 1, SegmentBatchFormat.MAX_BATCH,
                        4096, false, () -> { })) {
            SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), filler), SegmentBatchFormat.entry(context(), small),
                            SegmentBatchFormat.entry(context(), maximum)));
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 2, 1)));
            files.writeAuthorized(batch, arena);
            List<SegmentPointRead.Location> locations = SegmentPointRead.locations(
                    SegmentBatchParser.parse(batch.bytes(), address(4096), config.bytesPerFile()));
            assertEquals(4000, locations.get(1).recordOffset() % 4096);
            try (SegmentBufferPool.Lease scratch = pool.acquire()) {
                assertTrue(scratch.bytes().isDirect());
                assertEquals(0, scratch.bytes().alignmentOffset(0, 4096));
                for (int i = 1; i <= 2; i++) {
                    SegmentPointRead.Location location = locations.get(i);
                    SegmentPointRead.Result heap = SegmentPointRead.read(config, identity(), arena, location);
                    SegmentPointRead.Result pooled = SegmentPointRead.readWithScratch(
                            config, identity(), arena, location, scratch.bytes());
                    assertEquals(heap.bytesRead(), pooled.bytesRead());
                    assertArrayEquals(bytes(heap.record().entry()), bytes(pooled.record().entry()));
                    assertEquals(location.entryId(), pooled.record().entryId());
                    assertTrue(SegmentDuplicateCheck.sameLogicalPayloadOwned(config, identity(), arena, location,
                            context(), (i == 1 ? small : maximum).nioBuffer(), scratch.bytes()));
                    int required = SegmentPointRead.scratchBytes(location);
                    assertThrows(IOException.class, () -> SegmentPointRead.readWithScratch(config, identity(), arena,
                            location, scratch.bytes().limit(required - 1).slice()));
                }
                SegmentPointRead.Location location = locations.get(2);
                try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                        StandardOpenOption.WRITE)) {
                    channel.write(ByteBuffer.wrap(new byte[] {1}),
                            location.recordOffset() + SegmentBatchFormat.RECORD_HEADER + 36);
                    channel.force(false);
                }
                assertThrows(IOException.class, () -> SegmentPointRead.readWithScratch(
                        config, identity(), arena, location, scratch.bytes()));
                assertThrows(IOException.class, () -> SegmentDuplicateCheck.sameLogicalPayloadOwned(
                        config, identity(), arena, location, context(), maximum.nioBuffer(), scratch.bytes()));
            }
            pool.drainReturns();
            assertEquals(0, pool.outstanding());
        } finally {
            filler.release();
            small.release();
            maximum.release();
        }
    }

    @Test
    public void recoveredStreamCandidateKeepsOtherGenerationInSharedFile() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("mixed-stream-generations"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 2,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        long newerOffset = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf olderEntry = entry(new byte[8], 11);
        ByteBuf newerEntry = entry(new byte[8], 50);
        SegmentBatchFormat.FrozenBatch older;
        SegmentBatchFormat.FrozenBatch newer;
        try {
            older = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 11, 4096),
                    List.of(SegmentBatchFormat.entry(context(), olderEntry)));
            newer = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 6, new Generation(1), new Generation(2), 50, newerOffset),
                    List.of(SegmentBatchFormat.entry(context(), newerEntry)));
        } finally {
            olderEntry.release();
            newerEntry.release();
        }
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            // The two applied grants are distinct; an unused tail of the older block is left intact.
        }
        try (SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                            new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1),
                            5, 4096, 1, 11),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                            new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(2),
                            6, newerOffset, 1, 50)));
        }
        try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                StandardOpenOption.WRITE)) {
            SegmentFileIo io = new SegmentFileIo();
            io.writeFully(data, older.bytes(), 4096);
            io.writeFully(data, newer.bytes(), newerOffset);
            io.force(data, false);
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.PhysicalStreamCandidate olderCandidate =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), replay,
                            new SegmentFileIo(), 0, new Generation(1), 11, 1, 1);
            assertEquals(11, olderCandidate.through());
            assertEquals(11, olderCandidate.entries().get(0).entryId());
            assertEquals(1, olderCandidate.entries().size());

            SegmentDataRecovery.PhysicalStreamCandidate newerCandidate =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), replay,
                            new SegmentFileIo(), 0, new Generation(2), 50, 1, 1);
            assertEquals(50, newerCandidate.through());
            assertEquals(50, newerCandidate.entries().get(0).entryId());
            assertEquals(1, newerCandidate.entries().size());
            assertEquals(1, newerCandidate.scan().batches());

            SegmentOwnerLoop recoveredOwner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
            CountDownLatch restored = new CountDownLatch(1);
            AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
            recoveredOwner.start();
            try {
                assertTrue(recoveredOwner.control(() -> {
                    try {
                        SegmentHotDirectory tooSmall = new SegmentHotDirectory(recoveredOwner, config,
                                identity(), 1);
                        assertThrows(IOException.class, () -> tooSmall.restorePhysical(
                                List.of(olderCandidate, newerCandidate)));
                        assertEquals(0, tooSmall.candidateCount());
                        SegmentHotDirectory duplicate = new SegmentHotDirectory(recoveredOwner, config,
                                identity(), 2);
                        assertThrows(IOException.class, () -> duplicate.restorePhysical(
                                List.of(olderCandidate, olderCandidate)));
                        assertEquals(0, duplicate.candidateCount());
                        SegmentHotDirectory hot = new SegmentHotDirectory(recoveredOwner, config, identity(), 2);
                        hot.restorePhysical(List.of(olderCandidate, newerCandidate));
                        assertEquals(2, hot.candidateCount());
                        assertEquals(11, hot.candidate(context(), 11).orElseThrow().entryId());
                        assertEquals(50, hot.candidate(context(), 50).orElseThrow().entryId());
                    } catch (Throwable failure) {
                        restoreFailure.set(failure);
                    } finally {
                        restored.countDown();
                    }
                }));
                assertTrue(restored.await(5, TimeUnit.SECONDS));
                assertNull(restoreFailure.get());
            } finally {
                recoveredOwner.stopAdmission();
                assertTrue(recoveredOwner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(recoveredOwner.failure());

            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ByteBuffer original = ByteBuffer.allocate(1);
                SegmentFileIo.readFully(data, original, 4096);
                new SegmentFileIo().writeFully(data,
                        ByteBuffer.wrap(new byte[] {(byte) (original.get(0) ^ 1)}), 4096);
                data.force(false);
            }
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), replay, new SegmentFileIo(), 1));
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                    config, identity(), replay, new SegmentFileIo(), 0, new Generation(1), 11, 1, 1));
            SegmentDataRecovery.PhysicalStreamCandidate stillRequired =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), replay,
                            new SegmentFileIo(), 0, new Generation(2), 50, 1, 1);
            assertEquals(50, stillRequired.through());
            assertEquals(1, stillRequired.scan().batches());
            assertEquals(50, stillRequired.entries().get(0).entryId());
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                    config, identity(), replay, new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false),
                    0, new Generation(2), 50, 1, 1));
        }
    }

    @Test
    public void wrongLogicalShardIsRejectedBeforePhysicalWriteAndDuringRecovery() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("wrong-shard"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 2,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[8], 1, 0, 8, 7);
        SegmentBatchFormat.FrozenBatch wrong;
        try {
            wrong = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(7), entry)));
        } finally {
            entry.release();
        }
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 1, 1)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity(), 2));
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submit(wrong, (locations, cause) -> {
                    failure.set(cause);
                    completed.countDown();
                }));
                owner.start();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof IOException);
                assertEquals(4096, files.nextOffset(0));
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());

            // Raw framing I/O is deliberately not an admission path; its bytes must still fail read/recovery.
            files.writeAuthorized(wrong, arena);
            SegmentPointRead.Location location = SegmentPointRead.locations(SegmentBatchParser.parse(
                    wrong.bytes(), wrong.address(), config.bytesPerFile())).get(0);
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), arena, location));
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentFileIo sync = new SegmentFileIo();
            assertThrows(IOException.class, () -> SegmentDataRecovery.validateAndSync(
                    config, identity(), replay, sync));
            assertEquals(0, sync.forces);
        }
    }

    @Test
    public void dataWriteRequiresAnAppliedGrantForTheWholeBatchAndMatchingIdentity() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("granted"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[1024 * 1024]);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            assertTrue(batch.bytes().remaining() > SegmentArenaControlLog.BLOCK_BYTES);
            assertThrows(IOException.class, () -> files.writeAuthorized(batch, log));
            SegmentArenaControlLog.Allocation pool = new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 2, 1);
            SegmentArenaControlLog.DurableBatch durable = log.appendDurably(List.of(pool));
            assertThrows(IOException.class, () -> files.writeAuthorized(batch, log));
            log.applyDurable(durable);
            SegmentBatchFormat.Address wrongAllocation = new SegmentBatchFormat.Address(
                    0, 0, 6, new Generation(1), new Generation(1), 1, 4096);
            SegmentBatchFormat.Address wrongOwnerGeneration = new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(2), 1, 4096);
            assertThrows(IOException.class, () -> files.writeAuthorized(SegmentBatchFormat.encode(config,
                    wrongAllocation, List.of(SegmentBatchFormat.entry(context(), entry))), log));
            assertThrows(IOException.class, () -> files.writeAuthorized(SegmentBatchFormat.encode(config,
                    wrongOwnerGeneration, List.of(SegmentBatchFormat.entry(context(), entry))), log));
            IsolatedSegmentConfig other = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("other-granted"), 1, 4, 1,
                    4L * 1024 * 1024, 4096, 1024 * 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            try (IsolatedSegmentFiles otherFiles = IsolatedSegmentFiles.create(other, identity())) {
                assertThrows(IOException.class, () -> otherFiles.writeAuthorized(batch, log));
                assertEquals(4096, otherFiles.nextOffset(0));
            }
            assertEquals(4096, files.nextOffset(0));
            files.writeAuthorized(batch, log);
            assertEquals(4096 + batch.bytes().remaining(), files.nextOffset(0));
            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                ByteBuffer persisted = ByteBuffer.allocate(batch.bytes().remaining());
                SegmentFileIo.readFully(channel, persisted, 4096);
                assertEquals(3, SegmentBatchParser.parse(persisted, address(4096), config.bytesPerFile())
                        .records().get(0).entryId());
            }
            assertThrows(IOException.class, () -> SegmentDataRecovery.inspect(config, identity(), log));
        } finally {
            entry.release();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.Scan scan = SegmentDataRecovery.inspect(config, identity(), replay);
            assertEquals(1, scan.batches());
            assertEquals(1, scan.records());
            assertTrue(scan.validatedBytes() > SegmentArenaControlLog.BLOCK_BYTES);
            assertTrue(scan.zeroPages() > 0);
            SegmentFileIo recoveryIo = new SegmentFileIo();
            assertEquals(scan, SegmentDataRecovery.validateAndSync(config, identity(), replay, recoveryIo));
            assertEquals(1, recoveryIo.forces);
            SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class,
                    () -> SegmentDataRecovery.validateAndSync(config, identity(), replay, failedSync));
            assertEquals(1, failedSync.forces);
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                data.write(ByteBuffer.wrap(new byte[] {1}), config.bytesPerFile() - 4096);
                data.force(false);
            }
            assertThrows(IOException.class, () -> SegmentDataRecovery.inspect(config, identity(), replay));
            SegmentFileIo invalidIo = new SegmentFileIo();
            assertThrows(IOException.class,
                    () -> SegmentDataRecovery.validateAndSync(config, identity(), replay, invalidIo));
            assertEquals(0, invalidIo.forces);
        }
    }

    @Test
    public void completeRawDataBatchWithoutControlGrantIsNotRecoveryAuthority() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("raw-only"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[16]);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            files.write(SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry))));
        } finally {
            entry.release();
        }
        try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertThrows(IOException.class, () -> SegmentDataRecovery.inspect(config, identity(), log));
            SegmentFileIo syncIo = new SegmentFileIo();
            assertThrows(IOException.class,
                    () -> SegmentDataRecovery.validateAndSync(config, identity(), log, syncIo));
            assertEquals(0, syncIo.forces);
        }
    }

    @Test
    public void appendOnlyReopenSkipsPreviouslyGrantedZeroTailAndKeepsBothStreams() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("append-reopen"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        long nextBlock = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf firstEntry = entry(new byte[8], 11);
        ByteBuf secondEntry = entry(new byte[8], 12);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch second;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 11, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            second = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 6, new Generation(1), new Generation(2), 1, nextBlock),
                    List.of(SegmentBatchFormat.entry(context(), secondEntry)));
        } finally {
            firstEntry.release();
            secondEntry.release();
        }
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                    0, 0, new Generation(1), 5, 4096, 1, 11)));
            files.writeAuthorized(first, arena);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(config, identity(),
                    arena, new SegmentFileIo()));
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(config, identity(),
                    arena, failedSync));
            assertEquals(1, failedSync.forces);
            SegmentFileIo sync = new SegmentFileIo();
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity(), arena, sync, 1, 1)) {
                assertEquals(1, sync.forces);
                assertEquals(nextBlock, reopened.nextOffset(0));
                assertEquals(1, reopened.startupInventory().entries().size());
                SegmentDataRecovery.PhysicalStreamCandidate stream = reopened.startupInventory()
                        .streamCandidate(arena, 0, new Generation(1));
                assertEquals(11, stream.through());
                assertEquals(1, stream.entries().size());
                assertEquals(11, SegmentPointRead.read(config, identity(), arena,
                        reopened.startupInventory().entries().get(0)).record().entryId());
                assertThrows(IOException.class, () -> reopened.writeAuthorized(first, arena));
                SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
                SegmentIoExecutor io = new SegmentIoExecutor(1, 1);
                owner.start();
                try {
                    assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(
                            0, owner, io, reopened, arena,
                            new SegmentPhysicalPrefix(owner, 0, new Generation(2), 1, 1),
                            new SegmentHotDirectory(owner, config, identity(), 2)));
                    CountDownLatch restored = new CountDownLatch(1);
                    AtomicReference<Throwable> restoreFailure = new AtomicReference<>();
                    assertTrue(owner.control(() -> {
                        try {
                            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 2);
                            hot.restorePhysicalInventory(reopened.startupInventory());
                            assertEquals(1, hot.candidateCount());
                            assertTrue(hot.liveDurableCandidate(context(), 11).isEmpty());
                            assertThrows(IOException.class, () -> hot.reserve(first));
                        } catch (Throwable failure) {
                            restoreFailure.set(failure);
                        } finally {
                            restored.countDown();
                        }
                    }));
                    assertTrue(restored.await(5, TimeUnit.SECONDS));
                    assertNull(restoreFailure.get());
                } finally {
                    io.shutdown();
                    assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                    owner.stopAdmission();
                    assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
                }
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 6), new Generation(1), 0,
                        0, 0, new Generation(2), 6, nextBlock, 1, 1)));
                assertThrows(IOException.class, () -> reopened.write(second));
                assertEquals(nextBlock, reopened.nextOffset(0));
                reopened.writeAuthorized(second, arena);
                assertEquals(nextBlock + second.bytes().remaining(), reopened.nextOffset(0));
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.PhysicalStreamCandidate oldStream =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), arena,
                            new SegmentFileIo(), 0, new Generation(1), 11, 1, 1);
            SegmentDataRecovery.PhysicalStreamCandidate newStream =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity(), arena,
                            new SegmentFileIo(), 0, new Generation(2), 1, 1, 1);
            assertEquals(11, SegmentPointRead.read(config, identity(), arena,
                    oldStream.entries().get(0)).record().entryId());
            assertEquals(12, SegmentPointRead.read(config, identity(), arena,
                    newStream.entries().get(0)).record().entryId());
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity(), arena, new SegmentFileIo(), 2, 1));
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity(), arena, new SegmentFileIo(), 2, 2)) {
                assertEquals(11, reopened.startupInventory().streamCandidate(arena, 0, new Generation(1)).through());
                assertEquals(1, reopened.startupInventory().streamCandidate(arena, 0, new Generation(2)).through());
            }
        }
    }

    @Test
    public void appendOnlyReopenWaitsForArenaControlApply() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("append-pending-control"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            files.close();
            SegmentArenaControlLog.DurableBatch pending = arena.appendDurably(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                            new UUID(0, 5), new Generation(1), 0, 0, 0,
                            new Generation(1), 5, 4096, 1, 1)));
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity(), arena, new SegmentFileIo()));
            arena.applyDurable(pending);
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity(), arena, new SegmentFileIo(), 1, 1)) {
                assertEquals(4096 + SegmentArenaControlLog.BLOCK_BYTES, reopened.nextOffset(0));
                SegmentDataRecovery.PhysicalStreamCandidate empty = reopened.startupInventory()
                        .streamCandidate(arena, 0, new Generation(1));
                assertEquals(0, empty.through());
                assertTrue(empty.entries().isEmpty());
            }
        }
    }

    @Test
    public void startupInventoryDoesNotTreatALaterBatchAcrossAGapAsAStreamPrefix() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("startup-gap"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[8], 11);
        ByteBuf laterEntry = entry(new byte[8], 12);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch later;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 11, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            later = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 12, 8192),
                    List.of(SegmentBatchFormat.entry(context(), laterEntry)));
        } finally {
            firstEntry.release();
            laterEntry.release();
        }
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0,
                    new Generation(1), 5, 4096, 1, 11)));
            files.cancelBeforeSubmission(files.reserveAuthorized(first, arena));
            files.writeAuthorized(later, arena);
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), arena, new SegmentFileIo(), 1, 1)) {
            assertEquals(1, reopened.startupInventory().entries().size());
            assertEquals(12, SegmentPointRead.read(config, identity(), arena,
                    reopened.startupInventory().entries().get(0)).record().entryId());
            assertThrows(IOException.class, () -> reopened.startupInventory()
                    .streamCandidate(arena, 0, new Generation(1)));
        }
    }

    @Test
    public void failedSameProcessRecoveryKeepsTheWriterLockAcrossProcesses() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals("LOCKED", probeWriterLock(config.directory()));
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity(), arena, new SegmentFileIo()));
            assertEquals("LOCKED", probeWriterLock(config.directory()));
            assertThrows(IOException.class, () -> SegmentDataRecovery.inspect(config, identity(), arena));
            assertEquals("LOCKED", probeWriterLock(config.directory()));
        }
        assertEquals("ACQUIRED", probeWriterLock(config.directory()));
    }

    private String probeWriterLock(Path directory) throws Exception {
        Path output = temporary.getRoot().toPath().resolve("lock-probe.log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), SegmentBatchFormatTest.class.getName(),
                directory.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        boolean finished = child.waitFor(15, TimeUnit.SECONDS);
        if (!finished) {
            child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
        String result = Files.readString(output);
        assertTrue(result, finished);
        assertEquals(result, 0, child.exitValue());
        return result;
    }

    /** Independent JVM probe for the process-wide writer exclusion. */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected isolated DATA directory");
        }
        try (FileChannel channel = FileChannel.open(Path.of(args[0]).resolve("isolated.lock"),
                StandardOpenOption.WRITE)) {
            var lock = channel.tryLock();
            if (lock == null) {
                System.out.print("LOCKED");
            } else {
                try (lock) {
                    System.out.print("ACQUIRED");
                }
            }
        }
    }

    @Test
    public void laterDurableBatchWaitsForTheEarlierPhysicalStreamPrefix() throws Exception {
        runOutOfOrderPhysicalBatches(false);
    }

    @Test
    public void laterDurableBatchCannotBypassAnEarlierUncertainForce() throws Exception {
        runOutOfOrderPhysicalBatches(true);
    }

    private void runOutOfOrderPhysicalBatches(boolean failFirst) throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("prefix-" + failFirst),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 2, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf firstEntry = entry(new byte[16], 11);
        ByteBuf laterEntry = entry(new byte[16], 12);
        ByteBuf thirdEntry = entry(new byte[16], 13);
        SegmentBatchFormat.FrozenBatch first;
        SegmentBatchFormat.FrozenBatch later;
        SegmentBatchFormat.FrozenBatch third;
        try {
            first = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 11, 4096),
                    List.of(SegmentBatchFormat.entry(context(), firstEntry)));
            later = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 12, 8192),
                    List.of(SegmentBatchFormat.entry(context(), laterEntry)));
            third = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 13, 12288),
                    List.of(SegmentBatchFormat.entry(context(), thirdEntry)));
        } finally {
            firstEntry.release();
            laterEntry.release();
            thirdEntry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 11, 2);
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 2);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch laterCompleted = new CountDownLatch(1);
        CountDownLatch firstCompleted = new CountDownLatch(1);
        CountDownLatch bothReleased = new CountDownLatch(1);
        List<Long> released = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<SegmentPointRead.Location> laterLocation = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            if (failFirst) {
                files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false);
            }
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 11)));
            for (SegmentBatchFormat.FrozenBatch batch : List.of(first, later)) {
                assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                    @Override
                    public void run(SegmentOwnerLoop.Ticket ticket) {
                        try {
                            SegmentHotDirectory.Admission admission = hot.reserve(batch);
                            IsolatedSegmentFiles.Reservation reservation;
                            try {
                                reservation = prefix.reserve(batch, files, authority);
                            } catch (IOException failure) {
                                hot.cancelBeforeSubmission(admission);
                                throw failure;
                            }
                            hot.bind(admission, reservation);
                            io.submit(ticket, () -> {
                                if (batch.address().sequence() == 11) {
                                    firstStarted.countDown();
                                    if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                                        throw new IOException("Timed out waiting for earlier batch");
                                    }
                                }
                                files.writeReserved(reservation);
                            }, failure -> {
                                if (failure != null) {
                                    error.set(failure);
                                }
                                for (SegmentPhysicalPrefix.DurableBatch ready : prefix.completed(reservation,
                                        failure)) {
                                    hot.publish(ready);
                                    released.add(ready.address().sequence());
                                }
                                if (batch.address().sequence() == 12) {
                                    laterCompleted.countDown();
                                } else {
                                    firstCompleted.countDown();
                                }
                                if (released.size() == 2) {
                                    bothReleased.countDown();
                                }
                            });
                        } catch (IOException failure) {
                            throw new AssertionError(failure);
                        }
                    }

                    @Override
                    public void failed(Throwable failure) {
                        error.set(failure);
                    }
                }));
            }
            owner.start();
            assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            assertTrue(laterCompleted.await(5, TimeUnit.SECONDS));
            assertTrue(released.isEmpty());
            assertEquals(12288, files.nextOffset(0));
            CountDownLatch checkedGap = new CountDownLatch(1);
            assertTrue(owner.control(() -> {
                assertEquals(10, prefix.durableThrough());
                assertEquals(2, prefix.pending());
                assertEquals(2, hot.pendingBatches());
                assertEquals(0, hot.candidateCount());
                assertTrue(hot.candidate(context(), 12).isEmpty());
                checkedGap.countDown();
            }));
            assertTrue(checkedGap.await(5, TimeUnit.SECONDS));
            releaseFirst.countDown();
            assertTrue(firstCompleted.await(5, TimeUnit.SECONDS));
            if (failFirst) {
                assertTrue(released.isEmpty());
            } else {
                assertTrue(bothReleased.await(5, TimeUnit.SECONDS));
                assertEquals(List.of(11L, 12L), released);
            }
            CountDownLatch checkedPrefix = new CountDownLatch(1);
            assertTrue(owner.control(() -> {
                assertEquals(failFirst ? 10 : 12, prefix.durableThrough());
                assertEquals(failFirst ? 2 : 0, prefix.pending());
                assertEquals(failFirst, prefix.failed());
                assertEquals(failFirst ? 2 : 0, hot.pendingBatches());
                assertEquals(failFirst ? 0 : 2, hot.candidateCount());
                if (!failFirst) {
                    laterLocation.set(hot.candidate(context(), 12).orElseThrow());
                } else {
                    assertTrue(hot.candidate(context(), 12).isEmpty());
                }
                assertThrows(IOException.class, () -> hot.reserve(third));
                checkedPrefix.countDown();
            }));
            assertTrue(checkedPrefix.await(5, TimeUnit.SECONDS));
            assertEquals(failFirst, error.get() instanceof IOException);
            assertEquals(12288, files.nextOffset(0));
            if (!failFirst) {
                SegmentPointRead.Location location = laterLocation.get();
                assertEquals(12, SegmentPointRead.read(config, identity(), authority, location)
                        .record().entryId());
                ByteBuf same = entry(new byte[16], 12, 99, 16);
                byte[] changedPayload = new byte[16];
                changedPayload[0] = 1;
                ByteBuf differentPayload = entry(changedPayload, 12, 99, 16);
                ByteBuf differentLength = entry(new byte[16], 12, 99, 17);
                ByteBuf corrupt = entry(new byte[16], 12, 99, 16);
                ByteBuf wrongEntry = entry(new byte[16], 13, 99, 16);
                try {
                    corrupt.setInt(32, corrupt.getInt(32) ^ 1);
                    byte[] otherInstance = context().ledgerInstanceId();
                    otherInstance[0] ^= 1;
                    ProfileLedgerContext otherContext = new ProfileLedgerContext(context().ledgerId(), otherInstance,
                            context().descriptorIdentity());
                    assertTrue(SegmentDuplicateCheck.sameLogicalPayload(config, identity(), authority,
                            location, context(), same.nioBuffer()));
                    assertTrue(!SegmentDuplicateCheck.sameLogicalPayload(config, identity(), authority,
                            location, context(), differentPayload.nioBuffer()));
                    assertTrue(!SegmentDuplicateCheck.sameLogicalPayload(config, identity(), authority,
                            location, context(), differentLength.nioBuffer()));
                    assertThrows(IOException.class, () -> SegmentDuplicateCheck.sameLogicalPayload(config,
                            identity(), authority, location, context(), corrupt.nioBuffer()));
                    assertThrows(IOException.class, () -> SegmentDuplicateCheck.sameLogicalPayload(config,
                            identity(), authority, location, context(), wrongEntry.nioBuffer()));
                    assertThrows(IOException.class, () -> SegmentDuplicateCheck.sameLogicalPayload(config,
                            identity(), authority, location, otherContext, same.nioBuffer()));
                } finally {
                    same.release();
                    differentPayload.release();
                    differentLength.release();
                    corrupt.release();
                    wrongEntry.release();
                }
            }
            for (SegmentBatchFormat.FrozenBatch batch : failFirst ? List.of(later) : List.of(first, later)) {
                try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                    ByteBuffer bytes = ByteBuffer.allocate(batch.bytes().remaining());
                    SegmentFileIo.readFully(data, bytes, batch.address().offset());
                    assertEquals(batch.address().sequence(), SegmentBatchParser.parse(bytes, batch.address(),
                            config.bytesPerFile())
                            .records().get(0).entryId());
                }
            }
        } finally {
            releaseFirst.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        if (!failFirst) {
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                SegmentFileIo insufficient = new SegmentFileIo();
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), replay, insufficient, 1, 1));
                assertEquals(0, insufficient.forces);
                SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), replay, failedSync, 2, 1));
                assertEquals(1, failedSync.forces);
                try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), replay, new SegmentFileIo(), 2, 1)) {
                    SegmentDataRecovery.PhysicalStreamCandidate stream = reopened.startupInventory()
                            .streamCandidate(replay, 0, new Generation(1));
                    assertEquals(12, stream.through());
                    assertEquals(2, stream.entries().size());
                }
                SegmentFileIo sync = new SegmentFileIo();
                SegmentDataRecovery.PhysicalInventory inventory = SegmentDataRecovery.recoverPhysicalInventory(
                        config, identity(), replay, sync, 2);
                assertEquals(2, inventory.scan().batches());
                assertEquals(2, inventory.entries().size());
                assertEquals(List.of(4096 + SegmentArenaControlLog.BLOCK_BYTES),
                        inventory.grantedHighWatermarks());
                assertEquals(1, sync.forces);
                assertEquals(11, SegmentPointRead.read(config, identity(), replay, inventory.entries().get(0))
                        .record().entryId());
                assertEquals(12, SegmentPointRead.read(config, identity(), replay, inventory.entries().get(1))
                        .record().entryId());
            }
        }
    }

    @Test
    public void durableRelocationCopyDoesNotPublishAnOrdinaryHotCandidate() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("copy-hot"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 1, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[16]);
        SegmentBatchFormat.FrozenBatch copy;
        try {
            copy = SegmentBatchFormat.encode(config, address(4096), List.of(SegmentBatchFormat.relocation(
                    context(), entry, new UUID(1, 2), new Generation(4))));
        } finally {
            entry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1);
        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 1);
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> published = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
            assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    try {
                        SegmentHotDirectory.Admission admission = hot.reserve(copy);
                        IsolatedSegmentFiles.Reservation reservation = prefix.reserve(copy, files, authority);
                        hot.bind(admission, reservation);
                        io.submit(ticket, () -> files.writeReserved(reservation), failure -> {
                            if (failure != null) {
                                error.set(failure);
                            } else {
                                published.set(hot.publish(prefix.completed(reservation, null).get(0)));
                            }
                            done.countDown();
                        });
                    } catch (IOException failure) {
                        error.set(failure);
                        ticket.complete(done::countDown);
                    }
                }

                @Override
                public void failed(Throwable failure) {
                    error.set(failure);
                }
            }));
            owner.start();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(error.get());
            assertTrue(published.get().isEmpty());
            CountDownLatch checked = new CountDownLatch(1);
            assertTrue(owner.control(() -> {
                assertEquals(0, hot.candidateCount());
                assertTrue(hot.candidate(context(), 3).isEmpty());
                checked.countDown();
            }));
            assertTrue(checked.await(5, TimeUnit.SECONDS));
        } finally {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.PhysicalInventory inventory = SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), replay, new SegmentFileIo(), 1);
            assertEquals(1, inventory.scan().batches());
            assertTrue(inventory.entries().isEmpty());
        }
    }

    @Test
    public void completionWithoutTheReservationForceCannotAdvanceThePhysicalPrefix() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("no-force"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024 * 1024, 1, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[16]);
        SegmentBatchFormat.FrozenBatch batch;
        SegmentBatchFormat.FrozenBatch wrongStartBatch;
        try {
            batch = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            wrongStartBatch = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 0, 4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
        } finally {
            entry.release();
        }
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> problem = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1)));
            assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    try {
                        SegmentPhysicalPrefix wrongStart = new SegmentPhysicalPrefix(owner, 0,
                                new Generation(1), 0, 1);
                        assertThrows(IOException.class, () -> wrongStart.reserve(wrongStartBatch, files, authority));
                        assertEquals(4096, files.nextOffset(0));
                        IsolatedSegmentFiles.Reservation reservation = prefix.reserve(batch, files, authority);
                        assertTrue(prefix.completed(reservation, null).isEmpty());
                        assertEquals(0, prefix.durableThrough());
                        assertTrue(prefix.failed());
                        files.cancelBeforeSubmission(reservation);
                    } catch (Throwable failure) {
                        problem.set(failure);
                    } finally {
                        ticket.complete(done::countDown);
                    }
                }

                @Override
                public void failed(Throwable failure) {
                    problem.set(failure);
                }
            }));
            owner.start();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(problem.get());
            assertEquals(8192, files.nextOffset(0));
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                ByteBuffer firstByte = ByteBuffer.allocate(1);
                SegmentFileIo.readFully(data, firstByte, 4096);
                assertEquals(0, firstByte.get(0));
            }
        } finally {
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
        }
    }

    @Test
    public void writeSyncAndNoProgressFailuresRetainCoordinatesAndPoisonFile() throws Exception {
        for (SegmentFileIo faults : List.of(new SegmentFileIo(100, 2, -1, false),
                new SegmentFileIo(100, -1, 1, false), new SegmentFileIo(100, -1, -1, true))) {
            IsolatedSegmentConfig config = IsolatedSegmentConfig.defaults(
                    temporary.newFolder().toPath().resolve("arena"));
            ByteBuf entry = entry(new byte[16]);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
                files.dataIo = faults;
                SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config, address(4096),
                        List.of(SegmentBatchFormat.entry(context(), entry)));
                assertThrows(IOException.class, () -> files.write(batch));
                assertEquals(8192, files.nextOffset(0));
                files.dataIo = new SegmentFileIo();
                SegmentBatchFormat.FrozenBatch later = SegmentBatchFormat.encode(config, address(8192),
                        List.of(SegmentBatchFormat.entry(context(), entry)));
                assertThrows(IOException.class, () -> files.write(later));
                assertEquals(config.bytesPerFile(), Files.size(IsolatedSegmentFiles.dataPath(config, 0)));
            } finally {
                entry.release();
            }
        }
    }

    @Test
    public void existingDirectoryAndWrongIdentityAreRejected() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.create(config, identity()));
        }
        byte[] before = Files.readAllBytes(config.directory().resolve("superblock-a"));
        assertThrows(IOException.class, () -> IsolatedSegmentFiles.create(config, identity()));
        assertArrayEquals(before, Files.readAllBytes(config.directory().resolve("superblock-a")));
        SegmentFileHeader wrong = new SegmentFileHeader(
                identity().incarnation(), new UUID(0, 10), identity().bookieId());
        assertThrows(IOException.class, () -> wrong.validate(ByteBuffer.wrap(before), config, -1));
        try (FileChannel channel = FileChannel.open(temporary.newFile().toPath(), StandardOpenOption.READ)) {
            assertThrows(IOException.class, () -> SegmentFileIo.readFully(channel, ByteBuffer.allocate(1), 0));
        }
    }

    @Test
    public void authorizedPoolsAllowReverseOrderAcrossSharedFileStreams() throws Exception {
        IsolatedSegmentConfig config = config();
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf lowEntry = entry(new byte[8], 1, 0, 8, 8);
        ByteBuf highEntry = entry(new byte[8], 1, 0, 8, 10);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 6), new Generation(1), 0, 0, 2, new Generation(1), 6, upper, 1, 1)));
            SegmentBatchFormat.FrozenBatch low = SegmentBatchFormat.encode(config,
                    new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(8), lowEntry)));
            SegmentBatchFormat.FrozenBatch high = SegmentBatchFormat.encode(config,
                    new SegmentBatchFormat.Address(0, 2, 6, new Generation(1), new Generation(1), 1, upper),
                    List.of(SegmentBatchFormat.entry(context(10), highEntry)));
            files.writeAuthorized(high, arena);
            files.writeAuthorized(low, arena);
            assertEquals(upper + high.length(), files.nextOffset(0));
            assertEquals(2, files.dataIo.forces);
            assertThrows(IOException.class, () -> files.reserveAuthorized(low, arena));
            assertThrows(IOException.class, () -> files.reserveAuthorized(high, arena));
            SegmentPointRead.Location lowLocation = SegmentPointRead.locations(
                    SegmentBatchParser.parse(low.bytes(), low.address(), Long.MAX_VALUE)).get(0);
            SegmentPointRead.Location highLocation = SegmentPointRead.locations(
                    SegmentBatchParser.parse(high.bytes(), high.address(), Long.MAX_VALUE)).get(0);
            assertEquals(8,
                    SegmentPointRead.read(config, identity(), arena, lowLocation).record().context().ledgerId());
            assertEquals(10,
                    SegmentPointRead.read(config, identity(), arena, highLocation).record().context().ledgerId());
        } finally {
            lowEntry.release();
            highEntry.release();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.PhysicalInventory inventory = SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), replay, new SegmentFileIo(), 2);
            assertEquals(2, inventory.scan().batches());
            assertEquals(2, inventory.entries().size());
        }
    }

    @Test
    public void laterAppliedAllocationCanAppendBelowTheSameStreamsPreviousFileOffset() throws Exception {
        IsolatedSegmentConfig config = config();
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf first = entry(new byte[8], 1, 0, 8, 8);
        ByteBuf second = entry(new byte[8], 2, 0, 8, 8);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, upper, 1, 1)));
            SegmentBatchFormat.Address high = new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, upper);
            files.writeAuthorized(SegmentBatchFormat.encode(config, high,
                    List.of(SegmentBatchFormat.entry(context(8), first))), arena);
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(1), 6, 4096, 1, 1)));
            SegmentBatchFormat.Address low = new SegmentBatchFormat.Address(
                    0, 0, 6, new Generation(1), new Generation(1), 2, 4096);
            files.writeAuthorized(SegmentBatchFormat.encode(config, low,
                    List.of(SegmentBatchFormat.entry(context(8), second))), arena);
            assertEquals(new SegmentBatchFormat.Address(0, 0, 6, new Generation(1), new Generation(1), 3, 8192),
                    files.appendAddress(arena, 0, new Generation(1), 3, 4096));
            SegmentBatchFormat.Address oldTail = new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 3, upper + 4096);
            assertThrows(IOException.class, () -> files.reserveAuthorized(SegmentBatchFormat.encode(config,
                    oldTail, List.of(SegmentBatchFormat.entry(context(8), second))), arena));
            assertEquals(upper + 4096, files.nextOffset(0));
            assertEquals(2, files.dataIo.forces);
        } finally {
            first.release();
            second.release();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity(), replay, new SegmentFileIo(), 2, 1)) {
            SegmentDataRecovery.PhysicalInventory inventory = reopened.startupInventory();
            assertEquals(2, inventory.streamCandidate(replay, 0, new Generation(1)).through());
            assertEquals(2, inventory.entries().size());
            for (SegmentPointRead.Location location : inventory.entries()) {
                assertEquals(location.entryId(),
                        SegmentPointRead.read(config, identity(), replay, location).record().entryId());
            }
            assertNull(reopened.appendAddress(replay, 0, new Generation(1), 3, 4096));
        }
    }

    @Test
    public void restartSkipsOldAllocationTailsButAcceptsANewLowerGrant() throws Exception {
        IsolatedSegmentConfig config = config();
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf first = entry(new byte[8], 1, 0, 8, 8);
        ByteBuf second = entry(new byte[8], 2, 0, 8, 8);
        try {
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                        new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, upper, 1, 1)));
                files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                        0, 0, 5, new Generation(1), new Generation(1), 1, upper),
                        List.of(SegmentBatchFormat.entry(context(8), first))), arena);
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                            config, identity(), arena, new SegmentFileIo(), 2, 1)) {
                assertNull(files.appendAddress(arena, 0, new Generation(1), 2, 4096));
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                        new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(1), 6, 4096, 1, 1)));
                SegmentBatchFormat.Address low = new SegmentBatchFormat.Address(
                        0, 0, 6, new Generation(1), new Generation(1), 2, 4096);
                assertEquals(low, files.appendAddress(arena, 0, new Generation(1), 2, 4096));
                SegmentBatchFormat.FrozenBatch lowerBatch = SegmentBatchFormat.encode(config, low,
                        List.of(SegmentBatchFormat.entry(context(8), second)));
                files.writeAuthorized(lowerBatch, arena);
                assertEquals(2, SegmentPointRead.read(config, identity(), arena, SegmentPointRead.locations(
                        SegmentBatchParser.parse(lowerBatch.bytes(), low,
                                config.bytesPerFile())).get(0)).record().entryId());
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                            config, identity(), arena, new SegmentFileIo(), 2, 1)) {
                assertEquals(2, files.startupInventory().streamCandidate(arena, 0, new Generation(1)).through());
                assertNull(files.appendAddress(arena, 0, new Generation(1), 3, 4096));
            }
        } finally {
            first.release();
            second.release();
        }
    }

    @Test
    public void allocationOrderedRecoveryStillRejectsCrossAllocationSequenceGapsAndDuplicates() throws Exception {
        for (long badSequence : new long[] {1, 3}) {
            IsolatedSegmentConfig config = IsolatedSegmentConfig.defaults(temporary.newFolder("bad-range-"
                    + badSequence).toPath().resolve("arena"));
            long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            ByteBuf first = entry(new byte[8], 1, 0, 8, 8);
            ByteBuf second = entry(new byte[8], 2, 0, 8, 8);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                        new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, upper, 1, 1),
                        new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                                new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(1), 6, 4096, 1, 1)));
                files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                        0, 0, 5, new Generation(1), new Generation(1), 1, upper),
                        List.of(SegmentBatchFormat.entry(context(8), first))), arena);
                files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                        0, 0, 6, new Generation(1), new Generation(1), badSequence, 4096),
                        List.of(SegmentBatchFormat.entry(context(8), second))), arena);
            } finally {
                first.release();
                second.release();
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                IOException failure = assertThrows(IOException.class, () -> {
                    try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                            config, identity(), arena, new SegmentFileIo(), 2, 1)) {
                        files.startupInventory().streamCandidate(arena, 0, new Generation(1));
                    }
                });
                assertTrue(failure.getMessage(), failure.getMessage().contains("sequence gap"));
            }
        }
    }

    @Test
    public void authorizedStreamSkipsAndCancelledReservationsCannotBeReused() throws Exception {
        IsolatedSegmentConfig config = config();
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        ByteBuf entry = entry(new byte[8]);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentBatchFormat.FrozenBatch raw = SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            files.write(raw);
            arena.submitBatch(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(1), 6, upper, 1, 1)));
            assertThrows(IOException.class, () -> files.reserveAuthorized(raw, arena));
            // Adjacent one-block grants cannot be combined across different allocation identities.
            assertNull(files.appendAddress(arena, 0, new Generation(1), 3,
                    (int) SegmentArenaControlLog.BLOCK_BYTES + 4096));
            SegmentBatchFormat.FrozenBatch later = SegmentBatchFormat.encode(config,
                    new SegmentBatchFormat.Address(0, 0, 6, new Generation(1), new Generation(1), 2, upper),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            IsolatedSegmentFiles.Reservation cancelled = files.reserveAuthorized(later, arena);
            assertThrows(IOException.class, () -> files.close());
            files.cancelBeforeSubmission(cancelled);
            assertThrows(IllegalArgumentException.class, () -> files.cancelBeforeSubmission(cancelled));
            assertThrows(IOException.class, () -> files.reserveAuthorized(later, arena));
            SegmentBatchFormat.FrozenBatch skipped = SegmentBatchFormat.encode(config, address(8192),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            assertThrows(IOException.class, () -> files.reserveAuthorized(skipped, arena));
            SegmentBatchFormat.FrozenBatch rawAfter = SegmentBatchFormat.encode(config, address(upper + 4096),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
            assertThrows(IOException.class, () -> files.reserve(rawAfter));
            assertEquals(upper + 4096, files.nextOffset(0));
            assertEquals(1, files.dataIo.forces);
        } finally {
            entry.release();
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnly(
                        config, identity(), replay, new SegmentFileIo())) {
            assertEquals(upper + SegmentArenaControlLog.BLOCK_BYTES, reopened.nextOffset(0));
            assertNull(reopened.appendAddress(replay, 0, new Generation(1), 3, 4096));
        }
    }

    @Test
    public void capacityExhaustionAndLowerAdmissionLimitDoNotDamageExistingData() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("small"),
                1, 4, 1, 16384, 4096, 1024, 256, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        ByteBuf entry = entry(new byte[1024]);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            for (long offset : new long[] {4096, 8192, 12288}) {
                files.write(SegmentBatchFormat.encode(config, address(offset),
                        List.of(SegmentBatchFormat.entry(context(), entry))));
            }
            assertEquals(16384, files.nextOffset(0));
            assertThrows(IllegalArgumentException.class, () -> SegmentBatchFormat.encode(config, address(16384),
                    List.of(SegmentBatchFormat.entry(context(), entry))));
            IsolatedSegmentConfig lower = new IsolatedSegmentConfig(config.directory(), 1, 4, 1, 16384, 4096,
                    8, 256, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            assertThrows(IllegalArgumentException.class, () -> SegmentBatchFormat.encode(lower, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry))));
            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                ByteBuffer batch = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(channel, batch, 4096);
                assertEquals(1060, SegmentBatchParser.parse(batch, address(4096), lower.bytesPerFile())
                        .records().get(0).entry().remaining());
            }
            assertEquals(16384, Files.size(IsolatedSegmentFiles.dataPath(config, 0)));
        } finally {
            entry.release();
        }
    }

    @Test
    public void partialInitializationClosesFilesAndLeavesScopeUnadoptable() throws Exception {
        IsolatedSegmentConfig config = config();
        assertThrows(IOException.class, () -> IsolatedSegmentFiles.create(config, identity(),
                new SegmentFileIo(64, 2, -1, false)));
        try (FileChannel channel = FileChannel.open(config.directory().resolve("isolated.lock"),
                StandardOpenOption.WRITE); var lock = channel.tryLock()) {
            assertTrue(lock != null && lock.isValid());
        }
        assertThrows(IOException.class, () -> IsolatedSegmentFiles.create(config, identity()));
    }

    @Test
    public void corruptLowerStreamDoesNotRemoveHigherStreamBytes() throws Exception {
        IsolatedSegmentConfig config = config();
        ByteBuf entry = entry(new byte[16]);
        SegmentBatchFormat.Address higher = new SegmentBatchFormat.Address(0, 2, 6,
                new Generation(1), new Generation(1), 50, 8192);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            files.write(SegmentBatchFormat.encode(config, address(4096),
                    List.of(SegmentBatchFormat.entry(context(), entry))));
            files.write(SegmentBatchFormat.encode(config, higher,
                    List.of(SegmentBatchFormat.entry(context(), entry))));
            // Controlled corruption, not a recovery operation: shared-file length must stay fixed.
            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(new byte[] {0}), 4096);
                channel.force(false);
                ByteBuffer lowerBytes = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(channel, lowerBytes, 4096);
                assertThrows(IOException.class, () -> SegmentBatchParser.parse(lowerBytes, address(4096),
                        config.bytesPerFile()));
                ByteBuffer higherBytes = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(channel, higherBytes, 8192);
                assertEquals(3, SegmentBatchParser.parse(higherBytes, higher, config.bytesPerFile())
                        .records().get(0).entryId());
                assertEquals(config.bytesPerFile(), channel.size());
            }
        } finally {
            entry.release();
        }
    }

    private static byte[] bytes(ByteBuffer source) {
        byte[] bytes = new byte[source.remaining()];
        source.duplicate().get(bytes);
        return bytes;
    }

    private static int crc(ByteBuffer source, int start, int length) {
        CRC32C crc = new CRC32C();
        crc.update(source.slice(start, length));
        return (int) crc.getValue();
    }
}

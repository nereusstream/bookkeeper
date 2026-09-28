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
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
        byte[] instance = new byte[16];
        for (int i = 0; i < instance.length; i++) {
            instance[i] = (byte) (i + 1);
        }
        byte[] descriptor = new byte[36];
        java.util.Arrays.fill(descriptor, (byte) 3);
        return new ProfileLedgerContext(7, instance, descriptor);
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
        DigestManager digest = DigestManager.instantiate(7, new byte[0], DigestType.CRC32C,
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
    public void writerAndIndependentParserMatchFixedVectors() throws Exception {
        for (boolean relocation : new boolean[] {false, true}) {
            ByteBuf entry = entry("abc".getBytes(StandardCharsets.UTF_8));
            try {
                SegmentBatchFormat.Data record = relocation
                        ? SegmentBatchFormat.relocation(context(), entry, new UUID(1, 2), new Generation(4))
                        : SegmentBatchFormat.entry(context(), entry);
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
                assertEquals(context(), parsed.context());
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
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1)));
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
                    0, 0, new Generation(1), 5, 4096, 2);
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
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, 2);
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
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1)));
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
                    ProfileLedgerContext otherContext = new ProfileLedgerContext(7, otherInstance,
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
                assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalInventory(
                        config, identity(), replay, insufficient, 1));
                assertEquals(0, insufficient.forces);
                SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalInventory(
                        config, identity(), replay, failedSync, 2));
                assertEquals(1, failedSync.forces);
                SegmentFileIo sync = new SegmentFileIo();
                SegmentDataRecovery.PhysicalInventory inventory = SegmentDataRecovery.recoverPhysicalInventory(
                        config, identity(), replay, sync, 2);
                assertEquals(2, inventory.scan().batches());
                assertEquals(2, inventory.entries().size());
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
        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, 1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> published = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog authority = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            authority.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1)));
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
        try {
            batch = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
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
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1)));
            assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    try {
                        IsolatedSegmentFiles.Reservation reservation = prefix.reserve(batch, files, authority);
                        assertTrue(prefix.completed(reservation, null).isEmpty());
                        assertEquals(0, prefix.durableThrough());
                        assertTrue(prefix.failed());
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

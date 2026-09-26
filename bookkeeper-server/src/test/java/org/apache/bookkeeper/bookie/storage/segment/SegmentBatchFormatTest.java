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
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
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
        DigestManager digest = DigestManager.instantiate(7, new byte[0], DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        ByteBufList encoded = (ByteBufList) digest.computeDigestAndPackageForSending(3, 2, payload.length,
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

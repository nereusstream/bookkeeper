/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual entry-log resource graph only; no fresh Bookie permit, facade, root-unlock or ACK proof. */
public class FreshDefaultEntryLoggerTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void ownerExistsBeforeInitializationAndPartialInitializationKeepsItsFailure() throws Exception {
        try (var fixture = new Fixture()) {
            assertNull(fixture.logger.getEntryLoggerAllocator());
            assertThrows(IOException.class, () -> fixture.logger.addEntry(7, Unpooled.EMPTY_BUFFER));
            fixture.logger.initializeForFresh();
            assertNotNull(fixture.logger.getEntryLoggerAllocator());
            assertEquals(0, fixture.current.list().length);
            assertThrows(IllegalStateException.class, fixture.logger::initializeForFresh);
            var result = fixture.logger.closeForFresh();
            assertTrue(result.drained());
            assertNull(result.operationFailure());
            assertNull(result.flushFailure());
            assertNull(result.closeFailure());
            assertEquals(result, fixture.logger.closeForFresh());
            assertThrows(IllegalStateException.class, fixture.logger::initializeForFresh);
            assertThrows(IllegalStateException.class, fixture.logger::close);
        }
        try (var fixture = new Fixture()) {
            Files.writeString(fixture.current.toPath().resolve("lastId"), "history");
            IOException failure = assertThrows(IOException.class, fixture.logger::initializeForFresh);
            var result = fixture.logger.closeForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.operationFailure());
            assertNull(result.closeFailure());
            assertNull(fixture.logger.getEntryLoggerAllocator());
        }
    }

    @Test
    public void realRotationReadAliasesAndCallerResultsHaveDistinctCloseOwnership() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            List<Long> locations = new ArrayList<>();
            for (int entry = 0; entry < 8; entry++) {
                locations.add(fixture.append(entry));
            }
            fixture.logger.flush();
            assertTrue(fixture.logger.getFlushedLogIds().size() >= 2);
            List<BufferedReadChannel> aliases = new ArrayList<>();
            List<ByteBuf> results = new ArrayList<>();
            ByteBuf framing = ((EntryLogManagerForSingleEntryLog) fixture.logger.getEntryLogManager())
                    .sizeBufferForAdd();
            try {
                for (int entry = 0; entry < locations.size(); entry++) {
                    long location = locations.get(entry);
                    results.add(fixture.logger.readEntry(7, entry, location));
                    aliases.add(fixture.logger.getChannelForLogId(DefaultEntryLogger.logIdForOffset(location)));
                }
                var result = fixture.logger.closeForFresh();
                assertTrue(result.drained());
                assertNull(result.operationFailure());
                assertNull(result.closeFailure());
                assertNull(result.flushFailure());
                assertEquals(0, framing.refCnt());
                for (BufferedReadChannel alias : aliases) {
                    assertFalse(alias.fileChannel.isOpen());
                    assertEquals(0, alias.readBuffer.refCnt());
                    assertThrows(IOException.class, alias::size);
                }
                for (int entry = 0; entry < results.size(); entry++) {
                    assertEquals(1, results.get(entry).refCnt());
                    assertEquals(entry, results.get(entry).getLong(8));
                }
                assertThrows(IOException.class, () -> fixture.logger.readEntry(7, 0, locations.get(0)));
                long logId = DefaultEntryLogger.logIdForOffset(locations.get(0));
                assertThrows(UncheckedIOException.class, () -> fixture.logger.getFromChannels(logId));
            } finally {
                results.forEach(ByteBuf::release);
            }
        }
    }

    @Test
    public void successfullyClosedReadCacheMayReopenButItsOldAliasNeverReturnsCachedData() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            long location = fixture.append(0);
            fixture.logger.flush();
            long logId = DefaultEntryLogger.logIdForOffset(location);
            BufferedReadChannel previous = fixture.logger.getChannelForLogId(logId);
            ByteBuf first = fixture.logger.readEntry(7, 0, location);
            first.release();
            fixture.logger.removeFromChannelsAndClose(logId);
            assertEquals(0, previous.readBuffer.refCnt());
            assertThrows(IOException.class, previous::size);
            ByteBuf result = fixture.logger.readEntry(7, 0, location);
            try {
                assertEquals(7, result.getLong(0));
                assertEquals(0, result.getLong(8));
            } finally {
                result.release();
            }
            assertTrue(fixture.logger.closeForFresh().drained());
        }
    }

    @Test
    public void weakHeapAliasDoesNotOwnOrDischargeTheNativeCloseObligation() throws Exception {
        File file = temporary.newFile();
        Files.write(file.toPath(), new byte[]{1, 2, 3, 4});
        var resource = new FreshEntryLogReadResource(9);
        resource.open(file);
        resource.completeOpen();
        var reference = new AtomicReference<WeakReference<FreshEntryLogReadResource.Reader>>();
        var failure = new AtomicReference<Throwable>();
        var thread = new Thread(() -> {
            try {
                var reader = resource.newReader(16, true);
                assertFalse(reader.readBuffer.isDirect());
                reference.set(new WeakReference<>(reader));
            } catch (Throwable problem) {
                failure.set(problem);
            }
        });
        thread.start();
        thread.join(5000);
        assertFalse(thread.isAlive());
        assertNull(failure.get());
        FreshEntryLogReadResource.Reader keepAlive = reference.get().get();
        assertNotNull(keepAlive);
        var fields = FreshEntryLogReadResource.class.getDeclaredField("readers");
        fields.setAccessible(true);
        Map<?, ?> readers = (Map<?, ?>) fields.get(resource);
        Map.Entry<?, ?> entry = readers.entrySet().iterator().next();
        assertSame(Boolean.TRUE, entry.getValue());
        assertTrue(entry instanceof Reference<?>);
        FileChannel fd = keepAlive.fileChannel;
        Reference.reachabilityFence(keepAlive);
        keepAlive = null;
        // Model the weak-reference notification deterministically, not a physical-GC or refCnt-zero claim.
        ((Reference<?>) entry).clear();
        ((Reference<?>) entry).enqueue();
        reference.get().clear();
        assertTrue(fd.isOpen());
        resource.close();
        assertFalse(fd.isOpen());
        resource.close();
    }

    @Test
    public void unobservedAutomaticChannelCloseCannotBeRequalifiedByAnOwnerNoopClose() throws Exception {
        File file = temporary.newFile();
        Files.write(file.toPath(), new byte[128]);
        var resource = new FreshEntryLogReadResource(10);
        resource.open(file);
        resource.completeOpen();
        var reader = resource.newReader(16, false);
        ByteBuf destination = Unpooled.buffer(16);
        try {
            Thread.currentThread().interrupt();
            try {
                assertThrows(IOException.class, () -> reader.read(destination, 0, 16));
            } finally {
                Thread.interrupted();
            }
            assertFalse(reader.fileChannel.isOpen());
            IOException failure = assertThrows(IOException.class, resource::close);
            assertSame(failure, assertThrows(IOException.class, resource::close));
            assertEquals(0, reader.readBuffer.refCnt());
        } finally {
            destination.release();
            reader.fileChannel.close();
        }
    }

    @Test
    public void partiallyWrittenSealIsNotAppendedAgainAndItsFailureSurvivesResourceDrain() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            File file = new File(fixture.current, "0.log");
            Files.createFile(file.toPath());
            var controlled = new ControlledFileChannel(file);
            var channel = BufferedLogChannel.forFresh(UnpooledByteBufAllocator.DEFAULT, controlled,
                    1024, 1024, 0, file, 0);
            ByteBuf header = Unpooled.buffer(DefaultEntryLogger.LOGFILE_HEADER_SIZE).writeZero(
                    DefaultEntryLogger.LOGFILE_HEADER_SIZE);
            try {
                channel.write(header);
                ((EntryLogManagerForSingleEntryLog) fixture.logger.getEntryLogManager())
                        .setCurrentLogForLedgerAndAddToRotate(-1, channel);
                fixture.logger.recentlyCreatedEntryLogsStatus.createdEntryLog(0L);
                ByteBuf data = Fixture.entry(0);
                try {
                    fixture.logger.addEntry(7, data);
                } finally {
                    data.release();
                }
                controlled.failAtPosition = (long) DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION;
                controlled.failPartialWrite.set(true);
                assertSame(controlled.writeFailure, assertThrows(IOException.class, channel::appendLedgersMap));
                long length = controlled.delegate.size();
                assertSame(controlled.writeFailure, assertThrows(IOException.class, channel::appendLedgersMap));
                assertEquals(length, controlled.delegate.size());
                var result = fixture.logger.closeForFresh();
                assertTrue(result.drained());
                assertSame(controlled.writeFailure, result.operationFailure());
                assertNull(result.flushFailure());
                assertNull(result.closeFailure());
                assertEquals(1, controlled.closeAttempts.get());
            } finally {
                header.release();
                controlled.delegate.close();
            }
        }
    }

    @Test
    public void concurrentOpenLoserKeepsItsNativeCloseFailureOutsideThePublishedMap() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            long location = fixture.append(0);
            fixture.logger.flush();
            long logId = DefaultEntryLogger.logIdForOffset(location);
            var opened = new CountDownLatch(1);
            var winnerPublished = new CountDownLatch(1);
            var controlled = new AtomicReference<ControlledFileChannel>();
            var closeFailure = new IOException("open loser native close failed");
            var loser = new FutureTask<BufferedReadChannel>(() -> {
                try (var mockOpen = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                    mockOpen.when(() -> FileChannel.open(any(Path.class), eq(StandardOpenOption.READ)))
                            .thenAnswer(invocation -> {
                                FileChannel real = (FileChannel) invocation.callRealMethod();
                                var wrapped = new ControlledFileChannel(real);
                                wrapped.closeFailure = closeFailure;
                                controlled.set(wrapped);
                                opened.countDown();
                                assertTrue(winnerPublished.await(5, TimeUnit.SECONDS));
                                return wrapped;
                            });
                    return fixture.logger.getChannelForLogId(logId);
                }
            });
            var thread = new Thread(loser);
            try {
                thread.start();
                assertTrue(opened.await(5, TimeUnit.SECONDS));
                BufferedReadChannel winner = fixture.logger.getChannelForLogId(logId);
                winnerPublished.countDown();
                var failed = assertThrows(ExecutionException.class, () -> loser.get(5, TimeUnit.SECONDS));
                assertSame(closeFailure, failed.getCause());
                assertTrue(controlled.get().delegate.isOpen());
                var result = fixture.logger.closeForFresh();
                assertFalse(result.drained());
                assertSame(closeFailure, result.closeFailure());
                assertFalse(winner.fileChannel.isOpen());
                assertEquals(0, winner.readBuffer.refCnt());
                assertFalse(fixture.logger.closeForFresh().drained());
                assertEquals(1, controlled.get().closeAttempts.get());
            } finally {
                winnerPublished.countDown();
                thread.join(5000);
                if (controlled.get() != null) {
                    controlled.get().delegate.close();
                }
            }
        }
    }

    @Test
    public void closeDoesNotHoldTheResourceMonitorWhileWaitingForAnActualReader() throws Exception {
        File file = temporary.newFile();
        Files.write(file.toPath(), new byte[128]);
        var resource = new FreshEntryLogReadResource(11);
        resource.open(file);
        resource.completeOpen();
        var reader = resource.newReader(16, true);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var read = new FutureTask<Void>(() -> {
            synchronized (reader) {
                entered.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, resource::checkReadable);
            }
            return null;
        });
        var readThread = new Thread(read);
        var close = new FutureTask<Void>(() -> {
            resource.close();
            return null;
        });
        var closeThread = new Thread(close);
        try {
            readThread.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closeThread.start();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                    () -> assertThrows(IOException.class, resource::checkReadable));
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertTrue(reader.fileChannel.isOpen());
            assertEquals(1, reader.readBuffer.refCnt());
            release.countDown();
            read.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertFalse(reader.fileChannel.isOpen());
            assertEquals(0, reader.readBuffer.refCnt());
        } finally {
            release.countDown();
            readThread.join(5000);
            closeThread.join(5000);
            resource.close();
        }
    }

    @Test
    public void aliasConstructedBeforeClosingButNotYetRegisteredReleasesItsUnpublishedBuffer() throws Exception {
        File file = temporary.newFile();
        var resource = new FreshEntryLogReadResource(12);
        resource.open(file);
        resource.completeOpen();
        var allocated = new AtomicReference<ByteBuf>();
        var constructed = new CountDownLatch(1);
        var create = new FutureTask<FreshEntryLogReadResource.Reader>(() -> {
            try (var mockBuffers = mockStatic(Unpooled.class, CALLS_REAL_METHODS)) {
                mockBuffers.when(() -> Unpooled.buffer(16)).thenAnswer(invocation -> {
                    ByteBuf buffer = (ByteBuf) invocation.callRealMethod();
                    allocated.set(buffer);
                    constructed.countDown();
                    return buffer;
                });
                return resource.newReader(16, true);
            }
        });
        var thread = new Thread(create);
        try {
            synchronized (resource) {
                thread.start();
                assertTrue(constructed.await(5, TimeUnit.SECONDS));
                resource.beginClosing();
            }
            var failure = assertThrows(ExecutionException.class, () -> create.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IOException);
            assertEquals(0, allocated.get().refCnt());
            resource.close();
            assertTrue(resource.closedSuccessfully());
        } finally {
            thread.join(5000);
            resource.close();
        }
    }

    @Test
    public void compactionFlushMarkAvailableAndAbortUseActualNativeCloseResults() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            var copy = fixture.logger.newCompactionLog(100);
            var framingField = DefaultEntryLogger.class.getDeclaredField("freshCompactionSizeBuffer");
            framingField.setAccessible(true);
            ByteBuf framing = (ByteBuf) framingField.get(fixture.logger);
            ByteBuf entry = Fixture.entry(0);
            long location;
            try {
                location = copy.addEntry(7, entry);
            } finally {
                entry.release();
            }
            copy.flush();
            copy.markCompacted();
            copy.makeAvailable();
            copy.finalizeAndCleanup();
            ByteBuf actual = fixture.logger.readEntry(7, 0, location);
            try {
                assertEquals(7, actual.getLong(0));
                assertEquals(0, actual.getLong(8));
            } finally {
                actual.release();
            }
            var abandoned = fixture.logger.newCompactionLog(101);
            ByteBuf unused = Fixture.entry(1);
            try {
                abandoned.addEntry(7, unused);
            } finally {
                unused.release();
            }
            File temporaryCopy = new File(fixture.current,
                    Long.toHexString(abandoned.getDstLogId()) + ".log.compacting");
            assertTrue(temporaryCopy.exists());
            abandoned.abort();
            assertFalse(temporaryCopy.exists());
            assertTrue(fixture.logger.closeForFresh().drained());
            assertEquals(0, framing.refCnt());
        }
    }

    @Test
    public void failedCompactionDiscardRetainsTheFileAndItsStickyNativeCloseFailure() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            var controlled = new AtomicReference<ControlledFileChannel>();
            IOException closeFailure = new IOException("compaction discard native close failed");
            org.apache.bookkeeper.bookie.storage.CompactionEntryLog copy;
            try (var mockOpen = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                mockOpen.when(() -> FileChannel.open(any(Path.class), any(OpenOption[].class)))
                        .thenAnswer(invocation -> {
                            FileChannel real = (FileChannel) invocation.callRealMethod();
                            Path path = invocation.getArgument(0);
                            if (path.toString().endsWith(".log.compacting")) {
                                var wrapped = new ControlledFileChannel(real);
                                wrapped.closeFailure = closeFailure;
                                controlled.set(wrapped);
                                return wrapped;
                            }
                            return real;
                        });
                copy = fixture.logger.newCompactionLog(102);
            }
            try {
                File file = new File(fixture.current, Long.toHexString(copy.getDstLogId()) + ".log.compacting");
                var failed = assertThrows(UncheckedIOException.class, copy::abort);
                assertSame(closeFailure, failed.getCause());
                assertTrue(file.exists());
                var result = fixture.logger.closeForFresh();
                assertFalse(result.drained());
                assertSame(closeFailure, result.closeFailure());
                assertEquals(1, controlled.get().closeAttempts.get());
                assertTrue(controlled.get().delegate.isOpen());
            } finally {
                controlled.get().delegate.close();
            }
        }
    }

    @Test
    public void retryableFinalForceFailurePreservesTheRangeAndCanFinishTheSameClose() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            var controlled = fixture.installControlledCurrent(1024);
            fixture.append(0);
            IOException forceFailure = new IOException("final force failed");
            controlled.forceFailure = forceFailure;
            var first = fixture.logger.closeForFresh();
            assertFalse(first.drained());
            assertSame(forceFailure, first.flushFailure());
            assertNull(first.closeFailure());
            assertTrue(controlled.delegate.isOpen());
            long size = controlled.delegate.size();
            var second = fixture.logger.closeForFresh();
            assertTrue(second.drained());
            assertNull(second.flushFailure());
            assertNull(second.closeFailure());
            assertEquals(size, new File(fixture.current, "0.log").length());
            assertSame(forceFailure, first.flushFailure());
        }
    }

    @Test
    public void periodicFlushThatCapturedARetiredChannelObservesItsOriginalCompletedBarrier() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            fixture.append(0);
            BufferedLogChannel captured = ((EntryLogManagerForSingleEntryLog) fixture.logger.getEntryLogManager())
                    .getCurrentLogForLedger(-1);
            fixture.append(1);
            fixture.logger.flush();
            assertFalse(captured.fileChannel.isOpen());
            assertEquals(0, captured.writeBuffer.refCnt());
            captured.flushAndForceWrite(true);
            assertTrue(fixture.logger.closeForFresh().drained());
        }
    }

    @Test
    public void interruptedPartialEntryWriteCannotBecomeAHealthySubsequentAppend() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.logger.initializeForFresh();
            var controlled = fixture.installControlledCurrent(16);
            controlled.failPartialWrite.set(true);
            assertSame(controlled.writeFailure, assertThrows(IOException.class, () -> fixture.append(0)));
            assertThrows(IOException.class, () -> fixture.append(1));
            var result = fixture.logger.closeForFresh();
            assertTrue(result.drained());
            assertSame(controlled.writeFailure, result.operationFailure());
            assertNull(result.closeFailure());
            assertEquals(1, controlled.closeAttempts.get());
        }
    }

    private final class Fixture implements AutoCloseable {
        private final File current;
        private final DefaultEntryLogger logger;

        private Fixture() throws Exception {
            File root = temporary.newFolder();
            current = Files.createDirectory(root.toPath().resolve("current")).toFile();
            var conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()})
                    .setEntryLogSizeLimit(1500).setWriteBufferBytes(1024).setReadBufferBytes(1024);
            var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.999f, 0.99f));
            logger = DefaultEntryLogger.forFresh(conf, directories, null, NullStatsLogger.INSTANCE,
                    UnpooledByteBufAllocator.DEFAULT);
        }

        private long append(long id) throws IOException {
            ByteBuf entry = entry(id);
            try {
                return logger.addEntry(7, entry);
            } finally {
                entry.release();
            }
        }

        private ControlledFileChannel installControlledCurrent(int capacity) throws Exception {
            File file = new File(current, "0.log");
            Files.createFile(file.toPath());
            var controlled = new ControlledFileChannel(file);
            var channel = BufferedLogChannel.forFresh(UnpooledByteBufAllocator.DEFAULT, controlled,
                    capacity, 1024, 0, file, 0);
            ByteBuf header = Unpooled.buffer(DefaultEntryLogger.LOGFILE_HEADER_SIZE).writeZero(
                    DefaultEntryLogger.LOGFILE_HEADER_SIZE);
            try {
                channel.write(header);
            } finally {
                header.release();
            }
            ((EntryLogManagerForSingleEntryLog) logger.getEntryLogManager())
                    .setCurrentLogForLedgerAndAddToRotate(-1, channel);
            logger.recentlyCreatedEntryLogsStatus.createdEntryLog(0L);
            return controlled;
        }

        private static ByteBuf entry(long id) {
            ByteBuf entry = Unpooled.buffer(256).writeZero(256);
            entry.setLong(0, 7).setLong(8, id).setLong(16, id - 1);
            return entry;
        }

        @Override
        public void close() throws Exception {
            logger.closeForFresh();
        }
    }
}

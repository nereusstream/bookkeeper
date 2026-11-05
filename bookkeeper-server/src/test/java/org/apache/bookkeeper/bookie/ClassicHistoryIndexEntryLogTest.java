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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real local history and resource facts; no metadata/launcher episode, adoption permit or Unix qualification. */
public class ClassicHistoryIndexEntryLogTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void existingCookieUsesOwnedReadWithoutCreatingRepairingOrChangingTheOriginalFile() throws Exception {
        try (var f = new Fixture()) {
            f.conf.setBookieId("historical-cookie:3181");
            var cookie = Cookie.generateCookie(f.conf).setInstanceId("historical-cluster").build();
            cookie.writeToDirectory(f.ledger.toFile());
            Path version = f.ledger.resolve("VERSION");
            byte[] original = Files.readAllBytes(version);
            Object inode = Files.getAttribute(version, "unix:ino");
            var files = new ClassicHistoryFiles();
            cookie.verify(files.readCookie(f.ledger));
            assertEquals(inode, Files.getAttribute(version, "unix:ino"));
            assertArrayEquals(original, Files.readAllBytes(version));
            assertTrue(files.closeAndDrain().drained());

            Files.write(version, new byte[]{5, (byte) 0xc3});
            var malformed = new ClassicHistoryFiles();
            assertThrows(java.nio.charset.CharacterCodingException.class, () -> malformed.readCookie(f.ledger));
            assertTrue(malformed.closeAndDrain().drained());
            Files.write(version, new byte[1024 * 1024 + 1]);
            var oversized = new ClassicHistoryFiles();
            assertThrows(IOException.class, () -> oversized.readCookie(f.ledger));
            assertTrue(oversized.closeAndDrain().drained());
            assertEquals(1024 * 1024 + 1, Files.size(version));
            Files.delete(version);
            var missing = new ClassicHistoryFiles();
            assertThrows(NoSuchFileException.class, () -> missing.readCookie(f.ledger));
            assertTrue(missing.closeAndDrain().drained());
            assertFalse(Files.exists(version));
        }
    }

    @Test
    public void cookieReadCloseFailureNeverReturnsAParsedCookieOrRetriesTheOriginalNativeClose() throws Exception {
        try (var f = new Fixture()) {
            f.conf.setBookieId("historical-cookie:3181");
            Cookie.generateCookie(f.conf).setInstanceId("historical-cluster").build()
                    .writeToDirectory(f.ledger.toFile());
            Path version = f.ledger.resolve("VERSION");
            var channel = new ControlledFileChannel(FileChannel.open(version, StandardOpenOption.READ));
            IOException failure = new IOException("original Cookie reader close");
            channel.closeFailure = failure;
            var files = new ClassicHistoryFiles();
            try (var opens = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                opens.when(() -> FileChannel.open(version, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(failure, assertThrows(IOException.class, () -> files.readCookie(f.ledger)));
                assertFalse(files.closeAndDrain().drained());
                assertSame(failure, files.closeAndDrain().closeFailure());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
            } finally {
                channel.delegate.close();
            }
            assertFalse(files.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void persistedHistoryOpensWithSeparateIndexesThenWritesAndReopensAgain() throws Exception {
        try (var f = new Fixture()) {
            DefaultEntryLogger first = f.logger(false);
            LedgerCacheImpl index = f.cache(false);
            first.initializeForFresh();
            index.initializeForFresh();
            index.setMasterKey(7, new byte[]{1, 2, 3});
            long original = f.append(first, 0);
            index.putEntryOffset(7, 0, original);
            index.setFenced(7);
            first.flush();
            index.flushLedger(true);
            assertTrue(index.closeForFresh().drained());
            assertTrue(first.closeForFresh().drained());
            Path oldLog = f.ledger.resolve(Long.toHexString(DefaultEntryLogger.logIdForOffset(original)) + ".log");
            byte[] oldBytes = Files.readAllBytes(oldLog);
            Files.writeString(f.ledger.resolve("lastId"), "incomplete hint");
            DefaultEntryLogger second = f.logger(true);
            LedgerCacheImpl reopened = f.cache(true);
            assertThrows(IllegalStateException.class, second::initializeForFresh);
            assertThrows(IllegalStateException.class, reopened::initializeForFresh);
            second.initializeForClassicHistory();
            reopened.initializeForClassicHistory();
            assertArrayEquals(new byte[]{1, 2, 3}, reopened.readMasterKey(7));
            assertTrue(reopened.isFenced(7));
            assertEquals(original, reopened.getEntryOffset(7, 0));
            f.assertEntry(second, original, 0);
            long added = f.append(second, 1);
            assertNotEquals(DefaultEntryLogger.logIdForOffset(original), DefaultEntryLogger.logIdForOffset(added));
            reopened.putEntryOffset(7, 1, added);
            second.flush();
            reopened.flushLedger(true);
            assertTrue(reopened.closeForFresh().drained());
            assertTrue(second.closeForFresh().drained());
            assertArrayEquals(oldBytes, Files.readAllBytes(oldLog));
            DefaultEntryLogger third = f.logger(true);
            LedgerCacheImpl thirdIndex = f.cache(true);
            third.initializeForClassicHistory();
            thirdIndex.initializeForClassicHistory();
            f.assertEntry(third, thirdIndex.getEntryOffset(7, 0), 0);
            f.assertEntry(third, thirdIndex.getEntryOffset(7, 1), 1);
            LedgerCacheImpl fresh = f.cache(false);
            assertThrows(IOException.class, fresh::initializeForFresh);
            DefaultEntryLogger freshLogger = f.logger(false);
            assertThrows(IOException.class, freshLogger::initializeForFresh);
        }
    }

    @Test(timeout = 30000)
    public void historicalIndexInventoryStreamsOriginalsAndRelocationsWithoutChangingFiles() throws Exception {
        try (var f = new Fixture()) {
            Path original = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path redundant = f.writeIndex(1, 7, FileInfo.V1, 202);
            Path orphan = f.writeIndex(1, 8, FileInfo.V1, 303);
            Path redundantRelocation = Files.move(redundant, Path.of(redundant + IndexPersistenceMgr.RLOC));
            Path orphanRelocation = Files.move(orphan, Path.of(orphan + IndexPersistenceMgr.RLOC));
            var observed = new ArrayList<Long>();
            var paths = List.of(original, redundantRelocation, orphanRelocation);
            byte[][] bytes = new byte[paths.size()][];
            var identities = new ArrayList<Object>();
            for (int i = 0; i < paths.size(); i++) {
                bytes[i] = Files.readAllBytes(paths.get(i));
                identities.add(Files.getAttribute(paths.get(i), "unix:ino"));
            }
            var reader = new IndexPersistenceMgr.HistoricalIndexReader(f.indexDirectories.getAllLedgerDirs());
            try {
                reader.readLedgerIds(observed::add);
                assertEquals(List.of(7L, 7L, 8L), observed.stream().sorted().toList());
                assertFalse(Files.exists(orphan));
                assertFalse(Files.exists(redundant));
                for (int i = 0; i < paths.size(); i++) {
                    assertArrayEquals(bytes[i], Files.readAllBytes(paths.get(i)));
                    assertEquals(identities.get(i), Files.getAttribute(paths.get(i), "unix:ino"));
                }
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void historicalIndexInventoryFailsOnTheLaterCorruptHeaderOfAnObservedLedger() throws Exception {
        try (var f = new Fixture()) {
            Path destination = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path originalRelocation = Files.move(destination, Path.of(destination + IndexPersistenceMgr.RLOC));
            // Root 0's valid relocation is redundant against the original in root 1.
            f.writeIndex(1, 7, FileInfo.V0, 202);
            byte[] bytes = Files.readAllBytes(originalRelocation);
            var observed = new ArrayList<Long>();
            var reader = new IndexPersistenceMgr.HistoricalIndexReader(f.indexDirectories.getAllLedgerDirs());
            try {
                var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(observed::add));
                assertEquals(List.of(7L), observed);
                assertSame(failure, assertThrows(IOException.class, () -> reader.readLedgerIds(observed::add))
                        .getCause());
                assertSame(failure, reader.closeAndDrain().readFailure());
                assertFalse(Files.exists(destination));
                assertArrayEquals(bytes, Files.readAllBytes(originalRelocation));
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void historicalIndexInventoryRejectsSameByteReplacementOnFirstHeaderClose() throws Exception {
        try (var f = new Fixture()) {
            Path index = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path retired = index.resolveSibling("retired");
            var reader = new IndexPersistenceMgr.HistoricalIndexReader(f.indexDirectories.getAllLedgerDirs());
            var channel = new ControlledFileChannel(FileChannel.open(index, StandardOpenOption.READ));
            channel.beforeClose = () -> {
                try {
                    Files.move(index, retired);
                    Files.copy(retired, index);
                } catch (IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            };
            var observed = new ArrayList<Long>();
            try (var files = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                files.when(() -> FileChannel.open(index, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(observed::add));
                assertTrue(failure.getMessage(), failure.getMessage().contains("file changed"));
                assertTrue(observed.isEmpty());
                assertSame(failure, reader.closeAndDrain().readFailure());
                assertEquals(1, channel.closeAttempts.get());
                assertArrayEquals(Files.readAllBytes(retired), Files.readAllBytes(index));
            } finally {
                assertTrue(reader.closeAndDrain().drained());
                channel.delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void historicalIndexInventoryDoesNotRetryAFailedHeaderClose() throws Exception {
        try (var f = new Fixture()) {
            Path index = f.writeIndex(0, 7, FileInfo.V1, 101);
            var reader = new IndexPersistenceMgr.HistoricalIndexReader(f.indexDirectories.getAllLedgerDirs());
            var channel = new ControlledFileChannel(FileChannel.open(index, StandardOpenOption.READ));
            channel.closeFailure = new IOException("inventory first header close failed");
            channel.beforeClose = () -> assertThrows(IllegalStateException.class, reader::closeAndDrain);
            var observed = new ArrayList<Long>();
            try (var files = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                files.when(() -> FileChannel.open(index, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(channel.closeFailure, assertThrows(IOException.class,
                        () -> reader.readLedgerIds(observed::add)));
                assertTrue(observed.isEmpty());
            } finally {
                var drain = reader.closeAndDrain();
                assertFalse(drain.drained());
                assertEquals(1, drain.retainedResources());
                assertSame(channel.closeFailure, drain.readFailure());
                assertSame(channel.closeFailure, drain.closeFailure());
                assertEquals(drain, reader.closeAndDrain());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
                channel.delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void historicalIndexInventoryPreservesVisitorAndListingCloseFailures() throws Exception {
        try (var f = new Fixture()) {
            Path index = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path leaf = index.getParent();
            var reader = new IndexPersistenceMgr.HistoricalIndexReader(f.indexDirectories.getAllLedgerDirs());
            var visitorFailure = new IOException("inventory visitor failed");
            var closeFailure = new IOException("inventory first listing close failed");
            var closeAttempts = new AtomicInteger();
            try (var delegate = Files.newDirectoryStream(leaf);
                    var files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
                var failing = new DirectoryStream<Path>() {
                    @Override
                    public Iterator<Path> iterator() {
                        return delegate.iterator();
                    }

                    @Override
                    public void close() throws IOException {
                        closeAttempts.incrementAndGet();
                        throw closeFailure;
                    }
                };
                files.when(() -> Files.newDirectoryStream(leaf)).thenReturn(failing);
                assertSame(visitorFailure, assertThrows(IOException.class, () -> reader.readLedgerIds(ledger -> {
                    assertEquals(7, ledger);
                    throw visitorFailure;
                })));
                assertArrayEquals(new Throwable[]{closeFailure}, visitorFailure.getSuppressed());
            } finally {
                var drain = reader.closeAndDrain();
                assertFalse(drain.drained());
                assertEquals(1, drain.retainedResources());
                assertSame(visitorFailure, drain.readFailure());
                assertSame(closeFailure, drain.closeFailure());
                assertEquals(drain, reader.closeAndDrain());
                assertEquals(1, closeAttempts.get());
            }
        }
    }

    @Test(timeout = 30000)
    public void relocationPreservesTheOriginalIndexOrPromotesTheOnlyOrphan() throws Exception {
        try (var f = new Fixture()) {
            Path original = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path discarded = f.writeIndex(1, 7, FileInfo.V1, 202);
            discarded = Files.move(discarded, Path.of(discarded + IndexPersistenceMgr.RLOC));
            byte[] originalBytes = Files.readAllBytes(original);
            Path orphan = f.writeIndex(1, 8, FileInfo.V1, 303);
            Path destination = orphan;
            orphan = Files.move(orphan, Path.of(orphan + IndexPersistenceMgr.RLOC));
            byte[] orphanBytes = Files.readAllBytes(orphan);
            Object inode = Files.getAttribute(orphan, "unix:ino");
            LedgerCacheImpl cache = f.cache(true);
            cache.initializeForClassicHistory();
            assertEquals(101, cache.getEntryOffset(7, 0));
            assertEquals(303, cache.getEntryOffset(8, 0));
            assertFalse(Files.exists(discarded));
            assertFalse(Files.exists(orphan));
            assertEquals(inode, Files.getAttribute(destination, "unix:ino"));
            assertArrayEquals(originalBytes, Files.readAllBytes(original));
            assertArrayEquals(orphanBytes, Files.readAllBytes(destination));
        }
    }

    @Test(timeout = 30000)
    public void corruptOrUnsupportedHeadersRejectBeforeAnyRelocationMutation() throws Exception {
        for (int kind = 0; kind < 3; kind++) {
            try (var f = new Fixture()) {
                Path destination = f.writeIndex(0, 7, FileInfo.V1, 101);
                Path orphan = Files.move(destination, Path.of(destination + IndexPersistenceMgr.RLOC));
                byte[] bytes = Files.readAllBytes(orphan);
                Path bad = f.writeIndex(1, 8, kind == 0 ? FileInfo.V0 : FileInfo.V1, 202);
                if (kind > 0) {
                    byte[] header = Files.readAllBytes(bad);
                    ByteBuffer.wrap(header).putInt(19, kind == 1 ? Integer.MAX_VALUE : 1);
                    Files.write(bad, header);
                }
                LedgerCacheImpl cache = f.cache(true);
                assertThrows(IOException.class, cache::initializeForClassicHistory);
                assertFalse(Files.exists(destination));
                assertArrayEquals(bytes, Files.readAllBytes(orphan));
                assertThrows(IOException.class, () -> cache.readMasterKey(7));
            }
        }
    }

    @Test(timeout = 30000)
    public void ambiguousOriginalsOrOrphansArePreservedWithoutGuessing() throws Exception {
        for (boolean orphan : new boolean[]{false, true}) {
            try (var f = new Fixture()) {
                Path first = f.writeIndex(0, 7, FileInfo.V1, 101);
                Path second = f.writeIndex(1, 7, FileInfo.V1, 202);
                if (orphan) {
                    first = Files.move(first, Path.of(first + IndexPersistenceMgr.RLOC));
                    second = Files.move(second, Path.of(second + IndexPersistenceMgr.RLOC));
                }
                byte[] a = Files.readAllBytes(first);
                byte[] b = Files.readAllBytes(second);
                assertThrows(IOException.class, f.cache(true)::initializeForClassicHistory);
                assertArrayEquals(a, Files.readAllBytes(first));
                assertArrayEquals(b, Files.readAllBytes(second));
            }
        }
    }

    @Test(timeout = 30000)
    public void lastIdNativeCloseFailureCannotBecomeAMissingOrMalformedHint() throws Exception {
        try (var f = new Fixture()) {
            Path hint = Files.writeString(f.ledger.resolve("lastId"), "3\n");
            var channel = new ControlledFileChannel(FileChannel.open(hint, StandardOpenOption.READ));
            channel.closeFailure = new NoSuchFileException("native close has no definite result");
            DefaultEntryLogger logger = f.logger(true);
            try (var files = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                files.when(() -> FileChannel.open(hint, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(channel.closeFailure, assertThrows(IOException.class, logger::initializeForClassicHistory));
            } finally {
                assertFalse(logger.closeForFresh().drained());
                assertSame(channel.closeFailure, logger.closeForFresh().closeFailure());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
                channel.delegate.close();
                f.loggers.remove(logger);
            }
            assertFalse(logger.closeForFresh().drained());
        }
    }

    @Test(timeout = 30000)
    public void headerNativeCloseFailureBlocksPromotionAndInitializerCannotDrainItself() throws Exception {
        try (var f = new Fixture()) {
            Path destination = f.writeIndex(0, 7, FileInfo.V1, 101);
            Path orphan = Files.move(destination, Path.of(destination + IndexPersistenceMgr.RLOC));
            var channel = new ControlledFileChannel(FileChannel.open(orphan, StandardOpenOption.READ));
            channel.closeFailure = new IOException("historical header first native close");
            LedgerCacheImpl cache = f.cache(true);
            channel.beforeClose = () -> assertThrows(IllegalStateException.class, cache::closeForFresh);
            try (var files = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                files.when(() -> FileChannel.open(orphan, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(channel.closeFailure, assertThrows(IOException.class, cache::initializeForClassicHistory));
            } finally {
                assertFalse(cache.closeForFresh().drained());
                assertSame(channel.closeFailure, cache.closeForFresh().closeFailure());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
                channel.delegate.close();
                f.caches.remove(cache);
            }
            assertFalse(Files.exists(destination));
            assertTrue(Files.exists(orphan));
            assertFalse(cache.closeForFresh().drained());
        }
    }

    @Test(timeout = 30000)
    public void lazyExistingIndexOpenNeverRecreatesAFileRemovedBeforeNativeOpen() throws Exception {
        try (var f = new Fixture()) {
            Path index = f.writeIndex(0, 7, FileInfo.V1, 101);
            FileInfo info = FileInfo.forClassicHistory(index.toFile(), null, FileInfo.V1);
            try (var files = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                files.when(() -> FileChannel.open(index, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS)).thenAnswer(call -> {
                            Files.delete(index);
                            throw new NoSuchFileException(index.toString());
                        });
                assertThrows(NoSuchFileException.class, info::getMasterKey);
            } finally {
                assertTrue(info.closeForFresh(false).drained());
            }
            assertFalse(Files.exists(index));
        }
    }

    private final class Fixture implements AutoCloseable {
        private final Path ledger;
        private final List<Path> indexes = new ArrayList<>();
        private final List<DefaultEntryLogger> loggers = new ArrayList<>();
        private final List<LedgerCacheImpl> caches = new ArrayList<>();
        private final ServerConfiguration conf;
        private final LedgerDirsManager ledgerDirectories;
        private final LedgerDirsManager indexDirectories;

        private Fixture() throws Exception {
            Path ledgerRoot = temporary.newFolder().toPath();
            ledger = Files.createDirectory(ledgerRoot.resolve("current"));
            List<String> roots = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Path root = temporary.newFolder().toPath();
                roots.add(root.toString());
                indexes.add(Files.createDirectory(root.resolve("current")));
            }
            conf = new ServerConfiguration().setLedgerDirNames(new String[]{ledgerRoot.toString()})
                    .setIndexDirName(roots.toArray(String[]::new))
                    .setJournalDirName(temporary.newFolder().getCanonicalPath())
                    .setFileInfoFormatVersionToWrite(1).setJournalFormatVersionToWrite(6)
                    .setEntryLogFilePreAllocationEnabled(false).setOpenFileLimit(4)
                    .setWriteBufferBytes(4096).setReadBufferBytes(4096);
            ledgerDirectories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
            indexDirectories = new LedgerDirsManager(conf, conf.getIndexDirs(), new DiskChecker(.999f, .99f));
        }

        private DefaultEntryLogger logger(boolean historical) throws IOException {
            DefaultEntryLogger logger = historical
                    ? DefaultEntryLogger.forClassicHistory(conf, ledgerDirectories, null,
                            NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT)
                    : DefaultEntryLogger.forFresh(conf, ledgerDirectories, null,
                            NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
            loggers.add(logger);
            return logger;
        }

        private LedgerCacheImpl cache(boolean historical) throws IOException {
            LedgerCacheImpl cache = historical
                    ? LedgerCacheImpl.forClassicHistory(conf, new SnapshotMap<>(), indexDirectories,
                            NullStatsLogger.INSTANCE)
                    : LedgerCacheImpl.forFresh(conf, new SnapshotMap<>(), indexDirectories, NullStatsLogger.INSTANCE);
            caches.add(cache);
            return cache;
        }

        private Path writeIndex(int root, long ledger, int version, long offset) throws IOException {
            Path path = indexes.get(root).resolve(IndexPersistenceMgr.getLedgerName(ledger));
            Files.createDirectories(path.getParent());
            FileInfo info = new FileInfo(path.toFile(), new byte[]{1, 2, 3}, version);
            try {
                info.write(new ByteBuffer[]{ByteBuffer.allocate(8192).putLong(0, offset)}, 0);
            } finally {
                info.close(true);
            }
            return path;
        }

        private long append(DefaultEntryLogger logger, long entryId) throws IOException {
            ByteBuf entry = Unpooled.buffer(24).writeLong(7).writeLong(entryId).writeLong(123);
            try {
                return logger.addEntry(7, entry);
            } finally {
                entry.release();
            }
        }

        private void assertEntry(DefaultEntryLogger logger, long location, long entry) throws Exception {
            ByteBuf read = logger.readEntry(7, entry, location);
            try {
                assertEquals(7, read.getLong(0));
                assertEquals(entry, read.getLong(8));
                assertEquals(123, read.getLong(16));
            } finally {
                read.release();
            }
        }

        @Override
        public void close() throws Exception {
            for (LedgerCacheImpl cache : caches) {
                assertTrue(cache.closeForFresh().drained());
            }
            for (DefaultEntryLogger logger : loggers) {
                assertTrue(logger.closeForFresh().drained());
            }
        }
    }
}

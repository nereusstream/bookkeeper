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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual index/storage child graph; no whole Bookie, Journal, root-unlock, native-memory or permit proof. */
public class FreshClassicIndexDrainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 30000)
    public void realIndexFenceExplicitLacAndCallerResultHaveSeparateOwnership() throws Exception {
        File path = new File(temporary.newFolder(), "7.idx");
        FileInfo file = FileInfo.forFresh(path, new byte[]{1, 2}, FileInfo.CURRENT_HEADER_VERSION);
        assertTrue(file.setFenced());
        ByteBuf lac = Unpooled.buffer(16).writeLong(7).writeLong(99);
        try {
            file.setExplicitLac(lac);
        } finally {
            lac.release();
        }
        file.write(new ByteBuffer[]{indexBytes(123, 456)}, 0);
        assertTrue(file.closeForFresh(true).drained());
        assertThrows(IOException.class, file::getMasterKey);
        FileInfo reopened = FileInfo.forFresh(path, null, FileInfo.CURRENT_HEADER_VERSION);
        ByteBuf caller = null;
        try {
            assertTrue(reopened.isFenced());
            caller = reopened.getExplicitLac();
            assertEquals(99, caller.getLong(8));
            ByteBuffer bytes = ByteBuffer.allocate(16);
            assertEquals(16, reopened.read(bytes, 0, false));
            bytes.flip();
            assertEquals(123, bytes.getLong());
            assertEquals(456, bytes.getLong());
            assertTrue(reopened.closeForFresh(false).drained());
            assertEquals(1, caller.refCnt());
            assertThrows(IllegalStateException.class, reopened::getExplicitLac);
        } finally {
            if (caller != null) {
                caller.release();
            }
            reopened.closeForFresh(false);
        }
    }

    @Test(timeout = 30000)
    public void finalCloseDoesNotCreateAnUnopenedIndexButEvictionPersistsFence() throws Exception {
        File path = new File(temporary.newFolder(), "lazy.idx");
        FileInfo file = FileInfo.forFresh(path, new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        assertTrue(file.closeForFresh(false).drained());
        assertFalse(path.exists());
        FileInfo evicted = FileInfo.forFresh(path, new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        assertTrue(evicted.setFenced());
        assertFalse(path.exists());
        assertTrue(evicted.closeForFresh(true).drained());
        FileInfo reader = FileInfo.forFresh(path, null, FileInfo.CURRENT_HEADER_VERSION);
        try {
            assertTrue(reader.isFenced());
        } finally {
            reader.closeForFresh(false);
        }
    }

    @Test(timeout = 30000)
    public void incompleteHeaderRetainsItsActualChannelAndOriginalFailure() throws Exception {
        File path = temporary.newFile("partial.idx");
        Files.write(path.toPath(), new byte[]{1, 2, 3});
        FileInfo file = FileInfo.forFresh(path, null, FileInfo.CURRENT_HEADER_VERSION);
        var failure = assertThrows(BufferUnderflowException.class, file::readHeader);
        FileChannel channel = channel(file);
        assertTrue(channel.isOpen());
        assertThrows(IOException.class, file::getMasterKey);
        var closed = file.closeForFresh(false);
        assertTrue(closed.drained());
        assertSame(failure, closed.operationFailure());
        assertFalse(channel.isOpen());
    }

    @Test(timeout = 30000)
    public void partialHeaderCloseRetriesOnlyTheFrozenFixedPositionImage() throws Exception {
        File path = new File(temporary.newFolder(), "header.idx");
        FileInfo file = FileInfo.forFresh(path, new byte[]{3}, FileInfo.CURRENT_HEADER_VERSION);
        file.setFenced();
        var controlled = new AtomicReference<ControlledFileChannel>();
        try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
            open.when(() -> FileChannel.open(any(Path.class), eq(CREATE), eq(READ), eq(WRITE)))
                    .thenAnswer(invocation -> {
                        var wrapped = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                        wrapped.failPartialWrite.set(true);
                        wrapped.failAtPosition = 0L;
                        controlled.set(wrapped);
                        return wrapped;
                    });
            var first = file.closeForFresh(true);
            assertFalse(first.drained());
            assertSame(controlled.get().writeFailure, first.flushFailure());
            assertTrue(controlled.get().isOpen());
            var second = file.closeForFresh(false);
            assertTrue(second.drained());
            assertNull(second.flushFailure());
            assertSame(controlled.get().writeFailure, first.flushFailure());
            assertEquals(1, controlled.get().closeAttempts.get());
            assertEquals(FileInfo.START_OF_DATA, path.length());
        } finally {
            file.closeForFresh(false);
            if (controlled.get() != null) {
                controlled.get().delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void partialPageWriteRejectsDifferentBytesAndResumesTheOriginalCoordinates() throws Exception {
        File path = new File(temporary.newFolder(), "page.idx");
        FileInfo file = FileInfo.forFresh(path, new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.checkOpen(true);
        ControlledFileChannel controlled = control(file);
        controlled.failPartialWrite.set(true);
        controlled.failAtPosition = FileInfo.START_OF_DATA;
        ByteBuffer original = indexBytes(123, 456);
        assertSame(controlled.writeFailure,
                assertThrows(IOException.class, () -> file.write(new ByteBuffer[]{original}, 0)));
        assertEquals(0, original.position());
        assertThrows(IOException.class, () -> file.write(new ByteBuffer[]{indexBytes(999, 456)}, 0));
        assertEquals(16, file.write(new ByteBuffer[]{original}, 0));
        assertEquals(16, original.position());
        var result = file.closeForFresh(false);
        assertTrue(result.drained());
        assertSame(controlled.writeFailure, result.operationFailure());
        assertEquals(FileInfo.START_OF_DATA + 16, path.length());
        FileInfo reader = FileInfo.forFresh(path, null, FileInfo.CURRENT_HEADER_VERSION);
        try {
            ByteBuffer data = ByteBuffer.allocate(16);
            reader.read(data, 0, false);
            data.flip();
            assertEquals(123, data.getLong());
            assertEquals(456, data.getLong());
        } finally {
            reader.closeForFresh(false);
        }
    }

    @Test(timeout = 30000)
    public void finalCloseCompletesAnAlreadyOwnedPageImageAfterAForceFailure() throws Exception {
        FileInfo file = FileInfo.forFresh(new File(temporary.newFolder(), "force.idx"),
                new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.checkOpen(true);
        ControlledFileChannel controlled = control(file);
        IOException failure = new IOException("page force failed");
        controlled.forceFailure = failure;
        assertSame(failure, assertThrows(IOException.class,
                () -> file.write(new ByteBuffer[]{indexBytes(12, 34)}, 0)));
        var result = file.closeForFresh(false);
        assertTrue(result.drained());
        assertSame(failure, result.operationFailure());
        assertNull(result.flushFailure());
        assertEquals(1, controlled.closeAttempts.get());
    }

    @Test(timeout = 30000)
    public void nativeCloseFailureRemainsStickyAfterJdkMarksTheWrapperClosed() throws Exception {
        FileInfo file = FileInfo.forFresh(new File(temporary.newFolder(), "close.idx"),
                new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.checkOpen(true);
        ControlledFileChannel controlled = control(file);
        IOException failure = new IOException("native index close failed");
        controlled.closeFailure = failure;
        try {
            var first = file.closeForFresh(false);
            assertFalse(first.drained());
            assertSame(failure, first.closeFailure());
            assertFalse(controlled.isOpen());
            assertTrue(controlled.delegate.isOpen());
            assertSame(failure, file.closeForFresh(false).closeFailure());
            assertEquals(1, controlled.closeAttempts.get());
            assertThrows(IOException.class, file::getMasterKey);
        } finally {
            controlled.delegate.close();
        }
    }

    @Test(timeout = 30000)
    public void realInterruptedReadCannotTurnAutomaticCloseIntoAnObservedOwnerClose() throws Exception {
        FileInfo file = FileInfo.forFresh(new File(temporary.newFolder(), "interrupt.idx"),
                new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.write(new ByteBuffer[]{indexBytes(12, 34)}, 0);
        FileChannel raw = channel(file);
        try {
            Thread.currentThread().interrupt();
            assertThrows(ClosedByInterruptException.class, () -> file.read(ByteBuffer.allocate(8), 0, false));
        } finally {
            Thread.interrupted();
        }
        assertFalse(raw.isOpen());
        var result = file.closeForFresh(false);
        assertFalse(result.drained());
        assertNotNull(result.closeFailure());
        assertSame(result.closeFailure(), file.closeForFresh(false).closeFailure());
    }

    @Test(timeout = 30000)
    public void failedRelocationRetainsItsCandidateAndDoesNotUnlinkTheOriginal() throws Exception {
        File source = new File(temporary.newFolder(), "source.idx");
        File target = new File(temporary.newFolder(), "target.idx");
        FileInfo file = FileInfo.forFresh(source, new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.write(new ByteBuffer[]{indexBytes(12, 34)}, 0);
        FileChannel original = channel(file);
        IOException forceFailure = new IOException("relocation force failed");
        IOException closeFailure = new IOException("relocation close failed");
        var candidate = new AtomicReference<ControlledFileChannel>();
        try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
            open.when(() -> FileChannel.open(any(Path.class), eq(CREATE), eq(READ), eq(WRITE)))
                    .thenAnswer(invocation -> {
                        var wrapped = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                        wrapped.forceFailure = forceFailure;
                        wrapped.closeFailure = closeFailure;
                        candidate.set(wrapped);
                        return wrapped;
                    });
            assertThrows(IOException.class, () -> file.moveToNewLocation(target, Long.MAX_VALUE));
            assertTrue(source.exists());
            assertTrue(new File(target.getParentFile(), target.getName() + IndexPersistenceMgr.RLOC).exists());
            var result = file.closeForFresh(false);
            assertFalse(result.drained());
            assertSame(forceFailure, result.operationFailure());
            assertSame(closeFailure, result.closeFailure());
            assertFalse(original.isOpen());
            assertTrue(candidate.get().delegate.isOpen());
            assertEquals(1, candidate.get().closeAttempts.get());
        } finally {
            if (candidate.get() != null) {
                candidate.get().delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void backingCloseWaitsForBorrowersAndRejectsNewLazyOpens() throws Exception {
        File path = new File(temporary.newFolder(), "borrowed.idx");
        var cache = new FileInfoBackingCache((id, create) -> path, FileInfo.CURRENT_HEADER_VERSION, true);
        var borrowed = cache.loadFileInfo(7, new byte[0]);
        borrowed.checkOpen(true);
        FileChannel raw = channel(borrowed);
        var close = new FutureTask<>(cache::closeForFresh);
        Thread closer = new Thread(close);
        try {
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(borrowed::isClosed);
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertTrue(raw.isOpen());
            assertThrows(IOException.class, () -> cache.loadFileInfo(7, null));
            assertFalse(borrowed.tryRetain());
        } finally {
            borrowed.release();
            closer.join(5000);
        }
        assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        assertFalse(raw.isOpen());
        assertTrue(cache.fileInfos.isEmpty());
    }

    @Test(timeout = 30000)
    public void successfulRelocationRetiresEachOldHandleBeforeReplacingTheFile() throws Exception {
        File source = new File(temporary.newFolder(), "source.idx");
        File first = new File(temporary.newFolder(), "first.idx");
        File second = new File(temporary.newFolder(), "second.idx");
        FileInfo file = FileInfo.forFresh(source, new byte[0], FileInfo.CURRENT_HEADER_VERSION);
        file.write(new ByteBuffer[]{indexBytes(12, 34)}, 0);
        FileChannel original = channel(file);
        file.moveToNewLocation(first, Long.MAX_VALUE);
        assertFalse(original.isOpen());
        assertFalse(source.exists());
        FileChannel intermediate = channel(file);
        file.moveToNewLocation(second, Long.MAX_VALUE);
        assertFalse(intermediate.isOpen());
        assertFalse(first.exists());
        assertTrue(second.exists());
        ByteBuffer data = ByteBuffer.allocate(16);
        file.read(data, 0, false);
        data.flip();
        assertEquals(12, data.getLong());
        assertEquals(34, data.getLong());
        assertTrue(file.closeForFresh(false).drained());
    }

    @Test(timeout = 30000)
    public void concurrentHeapOnlyLoadersShareOneFileInfoAndOneRetirement() throws Exception {
        File path = new File(temporary.newFolder(), "winner.idx");
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var cache = new FileInfoBackingCache((id, create) -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
            return path;
        }, FileInfo.CURRENT_HEADER_VERSION, true);
        var one = new FutureTask<>(() -> cache.loadFileInfo(7, new byte[0]));
        var two = new FutureTask<>(() -> cache.loadFileInfo(7, new byte[0]));
        Thread first = new Thread(one);
        Thread second = new Thread(two);
        try {
            first.start();
            second.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            release.countDown();
            var winner = one.get(5, TimeUnit.SECONDS);
            var alias = two.get(5, TimeUnit.SECONDS);
            assertSame(winner, alias);
            assertEquals(2, winner.getRefCount());
            assertFalse(path.exists());
            winner.release();
            assertFalse(path.exists());
            alias.release();
            assertEquals(FileInfo.START_OF_DATA, path.length());
            assertTrue(cache.fileInfos.isEmpty());
            assertTrue(cache.closeForFresh().drained());
        } finally {
            release.countDown();
            first.join(5000);
            second.join(5000);
        }
    }

    @Test(timeout = 30000)
    public void closingDuringAHeapOnlyLoaderWaitsForItsActualReturn() throws Exception {
        File path = new File(temporary.newFolder(), "loader.idx");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var cache = new FileInfoBackingCache((id, create) -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
            return path;
        }, FileInfo.CURRENT_HEADER_VERSION, true);
        var load = new FutureTask<>(() -> cache.loadFileInfo(7, new byte[0]));
        var close = new FutureTask<>(cache::closeForFresh);
        Thread loader = new Thread(load);
        Thread closer = new Thread(close);
        try {
            loader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try {
                    cache.requireFreshAdmission();
                    return false;
                } catch (IOException expected) {
                    return true;
                }
            });
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            assertTrue(assertThrows(ExecutionException.class, () -> load.get(5, TimeUnit.SECONDS))
                    .getCause() instanceof IOException);
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            assertFalse(path.exists());
        } finally {
            release.countDown();
            loader.join(5000);
            closer.join(5000);
        }
    }

    @Test(timeout = 30000)
    public void sharedFrontCachesReleaseTheirOwnReferencesBeforeTheActualBorrower() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerCacheImpl cache = fixture.newCache();
            cache.setMasterKey(7, new byte[0]);
            var manager = cache.getIndexPersistenceManager();
            var borrowed = manager.getFileInfo(7L, null);
            borrowed.checkOpen(true);
            FileChannel raw = channel(borrowed);
            assertEquals(3, borrowed.getRefCount());
            var close = new FutureTask<>(cache::closeForFresh);
            Thread closer = new Thread(close);
            try {
                closer.start();
                await().atMost(5, TimeUnit.SECONDS).until(borrowed::isClosed);
                assertEquals(1, borrowed.getRefCount());
                assertTrue(raw.isOpen());
                assertThrows(IOException.class, () -> manager.getFileInfo(7L, new byte[0]));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            } finally {
                borrowed.release();
                closer.join(5000);
            }
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            assertFalse(raw.isOpen());
            assertEquals(0, manager.writeFileInfoCache.size());
            assertEquals(0, manager.readFileInfoCache.size());
        }
    }

    @Test(timeout = 30000)
    public void pageBorrowersReturnBeforeOwnedDirectPageReferencesRetire() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerCacheImpl cache = fixture.newCache();
            cache.setMasterKey(7, new byte[0]);
            cache.putEntryOffset(7, 0, 123);
            LedgerEntryPage page = cache.getIndexPageManager().getLedgerEntryPage(7, 0);
            var close = new FutureTask<>(cache::closeForFresh);
            Thread closer = new Thread(close);
            try {
                closer.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> {
                    try {
                        cache.getEntryOffset(7, 0);
                        return false;
                    } catch (IOException expected) {
                        return true;
                    }
                });
                assertFalse(page.isRetiredForFresh());
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            } finally {
                page.releasePage();
                closer.join(5000);
            }
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            assertTrue(page.isRetiredForFresh());
            assertThrows(IllegalStateException.class, page::usePage);
        }
    }

    @Test(timeout = 30000)
    public void failedRuntimeEvictionCannotReloadAroundTheOriginalCloseRecord() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerCacheImpl cache = fixture.newCache();
            cache.setMasterKey(7, new byte[0]);
            var manager = cache.getIndexPersistenceManager();
            var file = manager.getFileInfo(7L, new byte[0]);
            file.checkOpen(true);
            ControlledFileChannel raw = control(file);
            IOException failure = new IOException("eviction close failed");
            raw.closeFailure = failure;
            file.release();
            try {
                manager.writeFileInfoCache.invalidate(7L);
                manager.writeFileInfoCache.cleanUp();
                assertFalse(raw.isOpen());
                assertTrue(raw.delegate.isOpen());
                assertSame(file, manager.fileInfoBackingCache.fileInfos.get(7));
                assertSame(failure,
                        assertThrows(IOException.class, () -> manager.getFileInfo(7L, new byte[0])).getCause());
                var result = cache.closeForFresh();
                assertFalse(result.drained());
                assertSame(failure, result.closeFailure());
                assertSame(failure, cache.closeForFresh().closeFailure());
                assertEquals(1, raw.closeAttempts.get());
            } finally {
                raw.delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void actualSizeEvictionKeepsTheSharedBorrowerUntilItsLastRelease() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.conf.setOpenFileLimit(1);
            LedgerCacheImpl cache = fixture.newCache();
            cache.setMasterKey(7, new byte[0]);
            var manager = cache.getIndexPersistenceManager();
            var borrowed = manager.getFileInfo(7L, null);
            borrowed.setFenced();
            borrowed.checkOpen(true);
            ControlledFileChannel raw = control(borrowed);
            try {
                for (long id = 8; id <= 9; id++) {
                    cache.setMasterKey(id, new byte[0]);
                    manager.getFileInfo(id, null).release();
                }
                manager.writeFileInfoCache.cleanUp();
                manager.readFileInfoCache.cleanUp();
                assertEquals(1, borrowed.getRefCount());
                assertTrue(raw.isOpen());
            } finally {
                borrowed.release();
            }
            assertFalse(raw.isOpen());
            assertEquals(1, raw.closeAttempts.get());
            assertTrue(borrowed.closeForFresh(true).drained());
            assertEquals(1, raw.closeAttempts.get());
            FileInfo reader = FileInfo.forFresh(borrowed.getLf(), null, FileInfo.CURRENT_HEADER_VERSION);
            try {
                assertTrue(reader.isFenced());
            } finally {
                reader.closeForFresh(false);
            }
        }
    }

    @Test(timeout = 30000)
    public void evictionCallbacksRunOutsideTheGlobalCacheLockAndCannotAwaitTheirOwnRetirement() throws Exception {
        try (var fixture = new Fixture()) {
            LedgerCacheImpl cache = fixture.newCache();
            cache.setMasterKey(7, new byte[0]);
            var manager = cache.getIndexPersistenceManager();
            var file = manager.getFileInfo(7L, new byte[0]);
            var checked = new AtomicBoolean();
            assertTrue(file.waitForLastAddConfirmedUpdate(-1, notification -> {
                try {
                    assertThrows(IllegalStateException.class, cache::closeForFresh);
                    var other = new FutureTask<>(() -> manager.getFileInfo(8L, new byte[0]));
                    Thread thread = new Thread(other);
                    thread.start();
                    try {
                        other.get(5, TimeUnit.SECONDS).release();
                        thread.join(5000);
                    } catch (Exception failure) {
                        throw new AssertionError(failure);
                    }
                    checked.set(true);
                } finally {
                    notification.recycle();
                }
            }));
            file.release();
            manager.writeFileInfoCache.invalidate(7L);
            manager.writeFileInfoCache.cleanUp();
            assertTrue(checked.get());
            cache.setMasterKey(9, new byte[0]);
            assertNull(cache.closeForFresh().operationFailure());
        }
    }

    @Test(timeout = 30000)
    public void interleavedPartialCacheAndGcInitializationKeepAlreadyRegisteredChildren() throws Exception {
        var uninitialized = InterleavedLedgerStorage.forFresh();
        assertTrue(uninitialized.closeForFresh().drained());
        assertThrows(IllegalStateException.class, uninitialized::start);
        try (var fixture = new Fixture()) {
            File indexRoot = temporary.newFolder();
            Path current = Files.createDirectory(indexRoot.toPath().resolve("current"));
            Files.createDirectories(current.resolve("a/b"));
            Files.writeString(current.resolve("a/b/old.idx"), "historical");
            ServerConfiguration indexConf = new ServerConfiguration()
                    .setLedgerDirNames(new String[]{indexRoot.getCanonicalPath()});
            var indexes = new LedgerDirsManager(indexConf, indexConf.getLedgerDirs(), new DiskChecker(.999f, .99f));
            var storage = InterleavedLedgerStorage.forFresh();
            IOException failure = assertThrows(IOException.class, () -> storage.initializeForFresh(fixture.conf,
                    fixture.directories, indexes, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT));
            assertNotNull(storage.entryLogger.getEntryLoggerAllocator());
            assertNotNull(storage.ledgerCache);
            var result = storage.closeForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.initializationFailure());
            assertSame(failure, result.index().operationFailure());
            assertEquals("historical", Files.readString(current.resolve("a/b/old.idx")));
            assertThrows(IllegalStateException.class, storage::start);
        }
        try (var fixture = new Fixture()) {
            fixture.conf.setGcEntryLogMetadataCachePath("forbidden");
            var storage = InterleavedLedgerStorage.forFresh();
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> storage.initializeForFresh(fixture.conf, fixture.directories, null,
                            NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT));
            assertNull(storage.gcThread);
            var result = storage.closeForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.initializationFailure());
            assertTrue(result.entryLogger().drained());
            assertTrue(result.index().drained());
        }
    }

    @Test(timeout = 30000)
    public void interleavedWaitsForActualGcWorkBeforeIndexAndEntryLogClosure() throws Exception {
        try (var fixture = new Fixture()) {
            var storage = InterleavedLedgerStorage.forFresh();
            storage.initializeForFresh(fixture.conf, fixture.directories, null,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
            storage.setMasterKey(7, new byte[0]);
            ByteBuf entry = Unpooled.buffer(32).writeLong(7).writeLong(0).writeLong(0).writeLong(123);
            try {
                assertEquals(0, storage.addEntry(entry));
            } finally {
                entry.release();
            }
            storage.flush();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var finishedIo = new AtomicBoolean();
            var executorField = GarbageCollectorThread.class.getDeclaredField("gcExecutor");
            executorField.setAccessible(true);
            var executor = (ScheduledExecutorService) executorField.get(storage.gcThread);
            executor.submit(() -> {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    long offset = storage.ledgerCache.getEntryOffset(7, 0);
                    ByteBuf actual = storage.entryLogger.readEntry(7, 0, offset);
                    try {
                        assertEquals(123, actual.getLong(24));
                    } finally {
                        actual.release();
                    }
                    finishedIo.set(true);
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            });
            var close = new FutureTask<>(storage::closeForFresh);
            Thread closer = new Thread(close);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closer.start();
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                assertFalse(finishedIo.get());
                assertThrows(IOException.class, () -> storage.addEntry(Unpooled.EMPTY_BUFFER));
                release.countDown();
                var result = close.get(5, TimeUnit.SECONDS);
                assertTrue(result.drained());
                assertTrue(finishedIo.get());
                assertTrue(storage.gcThread.freshUsersTerminated());
                assertTrue(storage.closeForFresh().drained());
                assertThrows(IllegalStateException.class, storage::shutdown);
            } finally {
                release.countDown();
                closer.join(5000);
                storage.closeForFresh();
            }
        }
    }

    @Test(timeout = 30000)
    public void interleavedFinalFlushResumesTheOwnedPageRangeAndPreservesItsOldFailure() throws Exception {
        try (var fixture = new Fixture()) {
            var storage = InterleavedLedgerStorage.forFresh();
            storage.initializeForFresh(fixture.conf, fixture.directories, null,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
            storage.setMasterKey(7, new byte[0]);
            ByteBuf entry = Unpooled.buffer(32).writeLong(7).writeLong(0).writeLong(0).writeLong(123);
            try {
                storage.addEntry(entry);
            } finally {
                entry.release();
            }
            var cache = (LedgerCacheImpl) storage.ledgerCache;
            var file = cache.getIndexPersistenceManager().getFileInfo(7L, new byte[0]);
            file.checkOpen(true);
            ControlledFileChannel raw = control(file);
            raw.failPartialWrite.set(true);
            raw.failAtPosition = FileInfo.START_OF_DATA;
            file.release();
            var first = storage.closeForFresh();
            try {
                assertFalse(first.drained());
                assertSame(raw.writeFailure, first.finalFlushFailure());
                assertTrue(raw.isOpen());
                var second = storage.closeForFresh();
                assertTrue(second.drained());
                assertNull(second.finalFlushFailure());
                assertSame(raw.writeFailure, second.operationFailure());
                assertSame(raw.writeFailure, second.index().operationFailure());
                assertSame(raw.writeFailure, first.finalFlushFailure());
                assertFalse(raw.isOpen());
                assertEquals(1, raw.closeAttempts.get());
            } finally {
                storage.closeForFresh();
            }
        }
    }

    private static ByteBuffer indexBytes(long first, long second) {
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.putLong(first).putLong(second).flip();
        return bytes;
    }

    private static FileChannel channel(FileInfo file) throws Exception {
        var field = FileInfo.class.getDeclaredField("fc");
        field.setAccessible(true);
        return (FileChannel) field.get(file);
    }

    // Replace only this test's already registered handle; the wrapper delegates to its actual native file.
    private static ControlledFileChannel control(FileInfo file) throws Exception {
        FileChannel real = channel(file);
        var wrapped = new ControlledFileChannel(real);
        var field = FileInfo.class.getDeclaredField("fc");
        field.setAccessible(true);
        field.set(file, wrapped);
        var freshField = FileInfo.class.getDeclaredField("fresh");
        freshField.setAccessible(true);
        Object fresh = freshField.get(file);
        var resourcesField = fresh.getClass().getDeclaredField("channels");
        resourcesField.setAccessible(true);
        for (Object resource : (Iterable<?>) resourcesField.get(fresh)) {
            var channelField = resource.getClass().getDeclaredField("channel");
            channelField.setAccessible(true);
            if (channelField.get(resource) == real) {
                channelField.set(resource, wrapped);
            }
        }
        return wrapped;
    }

    private final class Fixture implements AutoCloseable {
        final ServerConfiguration conf;
        final LedgerDirsManager directories;
        LedgerCacheImpl cache;

        Fixture() throws Exception {
            File root = temporary.newFolder();
            Files.createDirectory(root.toPath().resolve("current"));
            conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()})
                    .setEntryLogPerLedgerEnabled(false).setPageLimit(8).setOpenFileLimit(4).setAllowLoopback(true);
            directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
        }

        LedgerCacheImpl newCache() throws Exception {
            cache = LedgerCacheImpl.forFresh(conf, new SnapshotMap<>(), directories, NullStatsLogger.INSTANCE);
            cache.initializeForFresh();
            return cache;
        }

        @Override
        public void close() throws Exception {
            if (cache != null) {
                cache.closeForFresh();
            }
        }
    }
}

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Explicit local lifecycle evidence; no cluster registration, network ACK or full-bytes proof. */
public class FreshClassicOwnerTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private ServerConfiguration configuration() throws Exception {
        var conf = new ServerConfiguration().setBookieId("fresh-owner:3181")
                .setJournalDirName(temporary.newFolder().getCanonicalPath())
                .setLedgerDirNames(new String[]{temporary.newFolder().getCanonicalPath()})
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName())
                .setJournalMaxMemorySizeMb(1).setJournalQueueSize(4).setJournalWriteBufferSizeKB(4)
                .setJournalBufferedWritesThreshold(1).setJournalMaxGroupWaitMSec(1)
                .setJournalFlushWhenQueueEmpty(true).setAllowLoopback(true).setPageLimit(8).setOpenFileLimit(4)
                .setDiskUsageThreshold(.999f).setDiskUsageWarnThreshold(.99f);
        conf.setProperty("journalPreAllocSizeMB", 1);
        return conf;
    }

    private static ByteBuf entry(long id) {
        return Unpooled.buffer(32).writeLong(7).writeLong(id).writeLong(id).writeLong(123 + id);
    }

    @Test(timeout = 30000)
    public void pureCreationAndUnstartedCloseHaveNoFilesystemSideEffectsOrAuthorityBypass() throws Exception {
        var conf = configuration();
        var owner = new TestBookieImpl.ResourceBuilder(conf).buildFresh();
        assertEquals(FreshClassicOwner.State.NEW, owner.status().state());
        assertEquals(0, new File(conf.getJournalDirNames()[0]).list().length);
        assertThrows(IllegalStateException.class, owner::bookie);
        assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
        assertEquals(0, new File(conf.getJournalDirNames()[0]).list().length);
        assertThrows(java.io.IOException.class, owner::start);
        assertThrows(java.io.IOException.class, () -> new TestBookieImpl.ResourceBuilder(conf).build());
        conf.setSanityCheckMetricsEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> FreshClassicOwner.create(conf));
        conf.setSanityCheckMetricsEnabled(false).setPersistBookieStatusEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> FreshClassicOwner.create(conf));
        assertEquals(0, new File(conf.getJournalDirNames()[0]).list().length);
    }

    @Test(timeout = 30000)
    public void realLocalOperationsDrainBeforeRootReleaseAndKeepCallerReadResults() throws Exception {
        var conf = configuration();
        var original = new ServerConfiguration(conf);
        var owner = FreshClassicOwner.create(conf);
        conf.setJournalSyncData(false);
        ByteBuf first = entry(0);
        ByteBuf recovered = entry(1);
        ByteBuf read = null;
        try {
            owner.start();
            var handle = owner.bookie();
            assertSame(handle, owner.bookie());
            assertFalse(Bookie.class.isAssignableFrom(handle.getClass()));
            var added = new CompletableFuture<Integer>();
            Object context = new Object();
            handle.addEntry(first, (rc, lid, eid, address, ctx) -> {
                assertSame(context, ctx);
                added.complete(rc);
            }, context, new byte[0]);
            assertEquals(BookieProtocol.EOK, (int) added.get(5, TimeUnit.SECONDS));
            read = handle.readEntry(7, 0);
            assertEquals(123, read.getLong(24));
            assertEquals(0, handle.readLastAddConfirmed(7));
            ByteBuf lac = Unpooled.buffer(64).writeInt(123).writeLong(7).writeLong(88).readerIndex(4);
            var lacWrite = new CompletableFuture<Integer>();
            handle.setExplicitLac(lac, (rc, lid, eid, address, ctx) -> lacWrite.complete(rc), null, new byte[0]);
            assertEquals(BookieProtocol.EOK, (int) lacWrite.get(5, TimeUnit.SECONDS));
            ByteBuf lacRead = handle.getExplicitLac(7);
            try {
                assertEquals(88, lacRead.getLong(8));
            } finally {
                lacRead.release();
            }
            assertTrue(handle.fenceLedger(7, new byte[0]).get(5, TimeUnit.SECONDS));
            ByteBuf rejected = entry(2);
            var rejectedCallbacks = new AtomicInteger();
            assertThrows(BookieException.class, () -> handle.addEntry(rejected,
                    (rc, lid, eid, address, ctx) -> rejectedCallbacks.incrementAndGet(), null, new byte[0]));
            assertEquals(0, rejectedCallbacks.get());
            assertEquals(0, rejected.refCnt());
            var recovery = new CompletableFuture<Integer>();
            handle.recoveryAddEntry(recovered, (rc, lid, eid, address, ctx) -> recovery.complete(rc),
                    null, new byte[0]);
            assertEquals(BookieProtocol.EOK, (int) recovery.get(5, TimeUnit.SECONDS));
            var forced = new CompletableFuture<Integer>();
            handle.forceLedger(7, (rc, lid, eid, address, ctx) -> forced.complete(rc), null);
            assertEquals(BookieProtocol.EOK, (int) forced.get(5, TimeUnit.SECONDS));
            var drained = owner.closeAndAwait(Duration.ofSeconds(5));
            assertTrue(String.valueOf(drained), drained.drained());
            assertNull(drained.closeFailure());
            assertNull(drained.operationFailure());
            assertEquals(0, first.refCnt());
            assertEquals(0, recovered.refCnt());
            assertEquals(0, lac.refCnt());
            assertEquals(123, read.getLong(24));
            assertEquals(0, drained.activeCalls());
            assertEquals(0, drained.pendingOperations());
            ByteBuf closed = entry(3);
            assertThrows(RejectedExecutionException.class,
                    () -> handle.addEntry(closed, (rc, lid, eid, address, ctx) -> { }, null, new byte[0]));
            assertEquals(0, closed.refCnt());
            assertTrue(Files.exists(new File(original.getJournalDirNames()[0], "fresh-classic-owner.lock").toPath()));
            var next = FreshClassicOwner.create(original);
            assertThrows(java.io.IOException.class, next::start);
            assertTrue(next.closeAndAwait(Duration.ofSeconds(5)).drained());
        } finally {
            owner.closeAndAwait(Duration.ofSeconds(5));
            if (read != null) {
                read.release();
            }
            if (first.refCnt() > 0) {
                first.release();
            }
            if (recovered.refCnt() > 0) {
                recovered.release();
            }
        }
    }

    @Test(timeout = 30000)
    public void heldRealForceKeepsOneWorkerAndTheRootsAcrossTimeoutAndCallerInterruption() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        var handle = owner.bookie();
        var controlled = controlJournal(owner);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var ioInterrupted = new AtomicBoolean();
        controlled.beforeForce = () -> hold(entered, release, ioInterrupted);
        var callback = new CompletableFuture<Integer>();
        var callbacks = new AtomicInteger();
        ByteBuf data = entry(0);
        var waiting = new FutureTask<>(() -> owner.closeAndAwait(Duration.ofSeconds(10)));
        Thread waiter = new Thread(waiting);
        try {
            handle.addEntry(data, (rc, lid, eid, address, ctx) -> {
                callbacks.incrementAndGet();
                callback.complete(rc);
            }, null, new byte[0]);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            Thread worker = (Thread) field(owner, "closeWorker");
            assertTrue(worker.isAlive());
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            assertSame(worker, field(owner, "closeWorker"));
            waiter.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> waiter.getState() == Thread.State.TIMED_WAITING);
            waiter.interrupt();
            assertTrue(assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS))
                    .getCause() instanceof InterruptedException);
            assertTrue(worker.isAlive());
            assertTrue(controlled.delegate.isOpen());
            assertFalse(ioInterrupted.get());
            assertFalse(callback.isDone());
            probeAll(conf, "HELD");
            release.countDown();
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertFalse(worker.isAlive());
            assertEquals(BookieProtocol.EOK, (int) callback.get(5, TimeUnit.SECONDS));
            assertEquals(1, callbacks.get());
            assertEquals(0, data.refCnt());
            assertFalse(controlled.delegate.isOpen());
            probeAll(conf, "AVAILABLE");
        } finally {
            release.countDown();
            waiter.join(5000);
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void callbackCanReturnBeforeTheCallingFinallyWithoutRetiringItsAdmissionOrChangingItsResult()
            throws Exception {
        var conf = configuration();
        StatsLogger stats = controlledStats();
        var addStats = mock(OpStatsLogger.class,
                delegatesTo(NullStatsLogger.INSTANCE.getOpStatsLogger(BookKeeperServerStats.BOOKIE_ADD_ENTRY)));
        doReturn(addStats).when(stats).getOpStatsLogger(BookKeeperServerStats.BOOKIE_ADD_ENTRY);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        RuntimeException failure = new RuntimeException("post-publication stats failed");
        doAnswer(invocation -> {
            hold(entered, release, new AtomicBoolean());
            throw failure;
        }).when(addStats).registerSuccessfulEvent(anyLong(), eq(TimeUnit.NANOSECONDS));
        var owner = FreshClassicOwner.create(conf, stats);
        owner.start();
        var handle = owner.bookie();
        var callback = new CompletableFuture<Integer>();
        ByteBuf data = entry(0);
        var write = new FutureTask<Void>(() -> {
            handle.addEntry(data, (rc, lid, eid, address, ctx) -> callback.complete(rc), null, new byte[0]);
            return null;
        });
        Thread caller = new Thread(write);
        try {
            caller.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(BookieProtocol.EOK, (int) callback.get(5, TimeUnit.SECONDS));
            assertFalse(write.isDone());
            assertEquals(1, owner.status().activeCalls());
            assertEquals(1, owner.status().pendingOperations());
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            probeAll(conf, "HELD");
            release.countDown();
            write.get(5, TimeUnit.SECONDS);
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertSame(failure, owner.status().observationFailure());
            assertEquals(0, data.refCnt());
            probeAll(conf, "AVAILABLE");
        } finally {
            release.countDown();
            caller.join(5000);
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void callbackFailureAndRenamingDoNotEnableSelfWaitOrLoseOtherCompletions() throws Exception {
        var owner = FreshClassicOwner.create(configuration());
        owner.start();
        var handle = owner.bookie();
        var first = new CountDownLatch(1);
        var second = new CompletableFuture<Integer>();
        RuntimeException failure = new RuntimeException("user callback failed");
        ByteBuf one = entry(0);
        ByteBuf two = entry(1);
        try {
            handle.addEntry(one, (rc, lid, eid, address, ctx) -> {
                Thread.currentThread().setName("renamed-fresh-callback");
                assertEquals(BookieProtocol.EOK, rc);
                assertThrows(IllegalStateException.class, () -> owner.closeAndAwait(Duration.ZERO));
                first.countDown();
                throw failure;
            }, null, new byte[0]);
            handle.addEntry(two, (rc, lid, eid, address, ctx) -> second.complete(rc), null, new byte[0]);
            assertTrue(first.await(5, TimeUnit.SECONDS));
            assertEquals(BookieProtocol.EOK, (int) second.get(5, TimeUnit.SECONDS));
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertSame(failure, owner.status().observationFailure());
            assertEquals(0, one.refCnt());
            assertEquals(0, two.refCnt());
        } finally {
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void closingDuringInitializationKeepsTheOriginalOwnerAndNeverPublishesTheHandle() throws Exception {
        var conf = configuration();
        StatsLogger stats = controlledStats();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var ownerRef = new AtomicReference<FreshClassicOwner>();
        doAnswer(invocation -> {
            assertThrows(IllegalStateException.class,
                    () -> ownerRef.get().closeAndAwait(Duration.ZERO));
            hold(entered, release, new AtomicBoolean());
            return NullStatsLogger.INSTANCE.getThreadScopedCounter("sync-thread-time");
        }).when(stats).getThreadScopedCounter("sync-thread-time");
        var owner = FreshClassicOwner.create(conf, stats);
        ownerRef.set(owner);
        var start = new FutureTask<Void>(() -> {
            owner.start();
            return null;
        });
        Thread initializer = new Thread(start);
        try {
            initializer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            assertThrows(IllegalStateException.class, owner::bookie);
            probeAll(conf, "HELD");
            release.countDown();
            assertTrue(assertThrows(ExecutionException.class, () -> start.get(5, TimeUnit.SECONDS))
                    .getCause() instanceof IOException);
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertTrue(owner.status().initializationFailure() instanceof IOException);
            assertThrows(IllegalStateException.class, owner::bookie);
            probeAll(conf, "AVAILABLE");
        } finally {
            release.countDown();
            initializer.join(5000);
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void partialJournalBufferConstructionClosesTheAlreadyAcquiredActualFdThroughTheSameOwner()
            throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        var actual = new AtomicReference<FileChannel>();
        OutOfMemoryError failure = new OutOfMemoryError("partial Journal buffer allocation");
        var allocator = mock(ByteBufAllocatorWithOomHandler.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doNothing().when(allocator).setOomHandler(any());
        doAnswer(invocation -> {
            BookieImpl bookie = (BookieImpl) field(owner, "implementation");
            Object state = field(bookie.journals.get(0), "fresh");
            synchronized (field(state, "gate")) {
                var channels = (List<?>) field(state, "channels");
                actual.set(((JournalChannel) channels.get(0)).fc);
            }
            assertTrue(actual.get().isOpen());
            throw failure;
        }).when(allocator).directBuffer(4096);
        try (var resources = mockStatic(BookieResources.class, CALLS_REAL_METHODS)) {
            resources.when(() -> BookieResources.createAllocator(any(ServerConfiguration.class))).thenReturn(allocator);
            IOException observed = assertThrows(IOException.class, owner::start);
            assertSame(failure, observed.getCause());
        }
        assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
        assertFalse(actual.get().isOpen());
        assertSame(failure, owner.status().operationFailure());
        probeAll(conf, "AVAILABLE");
    }

    @Test(timeout = 30000)
    public void nativeCloseFailureKeepsBothRootsAndNeverUpgradesTheAlreadyDurableCallback() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        BookieImpl bookie = (BookieImpl) field(owner, "implementation");
        var state = field(bookie.journals.get(0), "fresh");
        JournalChannel channel = (JournalChannel) ((List<?>) field(state, "channels")).get(0);
        var rafField = DefaultFileChannel.class.getDeclaredField("randomAccessFile");
        rafField.setAccessible(true);
        RandomAccessFile actual = (RandomAccessFile) rafField.get(channel.channel);
        RandomAccessFile failing = spy(actual);
        IOException failure = new IOException("owned Journal RAF close failed");
        doThrow(failure).when(failing).close();
        rafField.set(channel.channel, failing);
        ByteBuf data = entry(0);
        var callback = new CompletableFuture<Integer>();
        try {
            owner.bookie().addEntry(data, (rc, lid, eid, address, ctx) -> callback.complete(rc), null, new byte[0]);
            assertEquals(BookieProtocol.EOK, (int) callback.get(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertSame(failure, owner.status().closeFailure());
            probeAll(conf, "HELD");
            actual.close();
            assertFalse(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertSame(failure, owner.status().closeFailure());
            verify(failing, times(1)).close();
            assertEquals(0, data.refCnt());
            probeAll(conf, "HELD");
        } finally {
            actual.close();
            owner.closeAndAwait(Duration.ofSeconds(5));
            // Fixture-only external cleanup after all users/fd ended; never requalifies the owner's sticky failure.
            ((FreshClassicRoots) field(owner, "roots")).close();
        }
    }

    @Test(timeout = 30000)
    public void duplicatePhysicalJournalMembersAreRejectedBeforeAnyCurrentDirectoryIsCreated() throws Exception {
        var conf = configuration();
        Path journal = Path.of(conf.getJournalDirNames()[0]);
        Path alias = temporary.getRoot().toPath().resolve("journal-alias");
        Files.createSymbolicLink(alias, journal);
        conf.setJournalDirsName(new String[]{journal.toString(), alias.toString()}).setJournalMaxMemorySizeMb(2)
                .setAllowMultipleDirsUnderSameDiskPartition(true);
        var owner = FreshClassicOwner.create(conf);
        assertThrows(IOException.class, owner::start);
        assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
        assertFalse(Files.exists(journal.resolve("current")));
    }

    private static StatsLogger controlledStats() {
        StatsLogger stats = mock(StatsLogger.class, delegatesTo(NullStatsLogger.INSTANCE));
        doReturn(stats).when(stats).scope(anyString());
        doReturn(stats).when(stats).scopeLabel(anyString(), anyString());
        return stats;
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private static ControlledFileChannel controlJournal(FreshClassicOwner owner) throws Exception {
        var bookie = (BookieImpl) field(owner, "implementation");
        Object state = field(bookie.journals.get(0), "fresh");
        JournalChannel channel;
        synchronized (field(state, "gate")) {
            channel = (JournalChannel) ((List<?>) field(state, "channels")).get(0);
        }
        synchronized (channel) {
            var wrapped = new ControlledFileChannel(channel.fc);
            channel.fc = wrapped;
            var field = BufferedChannelBase.class.getDeclaredField("fileChannel");
            field.setAccessible(true);
            field.set(channel.bc, wrapped);
            return wrapped;
        }
    }

    private static void hold(CountDownLatch entered, CountDownLatch release, AtomicBoolean interrupted) {
        entered.countDown();
        try {
            assertTrue("Controlled operation was not released", release.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
            interrupted.set(true);
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static void probeAll(ServerConfiguration conf, String expected) throws Exception {
        FreshClassicRootsTest.probe(
                Path.of(conf.getJournalDirNames()[0]).resolve("fresh-classic-owner.lock"), expected);
        FreshClassicRootsTest.probe(Path.of(conf.getLedgerDirNames()[0]).resolve("fresh-classic-owner.lock"), expected);
    }

    @Test(timeout = 30000)
    public void cancelledFenceObservationDoesNotCancelTheOwnedJournalOperation() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        var handle = owner.bookie();
        var added = new CompletableFuture<Integer>();
        ByteBuf data = entry(0);
        handle.addEntry(data, (rc, lid, eid, address, ctx) -> added.complete(rc), null, new byte[0]);
        assertEquals(BookieProtocol.EOK, (int) added.get(5, TimeUnit.SECONDS));
        var controlled = controlJournal(owner);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        controlled.beforeForce = () -> hold(entered, release, new AtomicBoolean());
        try {
            var observation = handle.fenceLedger(7, new byte[0]);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var bookie = (BookieImpl) field(owner, "implementation");
            var descriptor = bookie.handles.getHandle(7, new byte[0], false);
            @SuppressWarnings("unchecked")
            var actual = (CompletableFuture<Boolean>) field(descriptor, "logFenceResult");
            assertTrue(observation.cancel(false));
            assertFalse(actual.isCancelled());
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            assertFalse(actual.isDone());
            probeAll(conf, "HELD");
            release.countDown();
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertTrue(actual.get(5, TimeUnit.SECONDS));
            assertTrue(observation.isCancelled());
            assertEquals(0, data.refCnt());
            probeAll(conf, "AVAILABLE");
        } finally {
            release.countDown();
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void failedFinalSyncRetainsItsFilesAndRootsUntilTheFixedRangeRetryCompletes() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        var added = new CompletableFuture<Integer>();
        ByteBuf data = entry(0);
        owner.bookie().addEntry(data, (rc, lid, eid, address, ctx) -> added.complete(rc), null, new byte[0]);
        assertEquals(BookieProtocol.EOK, (int) added.get(5, TimeUnit.SECONDS));
        var bookie = (BookieImpl) field(owner, "implementation");
        var storage = (InterleavedLedgerStorage) bookie.ledgerStorage;
        var cache = (LedgerCacheImpl) storage.ledgerCache;
        var file = cache.getIndexPersistenceManager().getFileInfo(7L, new byte[0]);
        file.checkOpen(true);
        var raw = FreshClassicIndexDrainTest.control(file);
        raw.failPartialWrite.set(true);
        raw.failAtPosition = FileInfo.START_OF_DATA;
        file.release();
        try {
            var first = owner.closeAndAwait(Duration.ofSeconds(5));
            assertFalse(first.drained());
            assertSame(raw.writeFailure, first.closeFailure());
            assertTrue(raw.delegate.isOpen());
            assertTrue(bookie.journals.get(0).newCheckpoint() != null);
            probeAll(conf, "HELD");
            var second = owner.closeAndAwait(Duration.ofSeconds(5));
            assertTrue(String.valueOf(second), second.drained());
            assertNull(second.closeFailure());
            assertSame(raw.writeFailure, second.operationFailure());
            assertFalse(raw.delegate.isOpen());
            assertEquals(1, raw.closeAttempts.get());
            assertEquals(0, data.refCnt());
            probeAll(conf, "AVAILABLE");
        } finally {
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void actualGcRotationCanPublishItsCheckpointBeforeTheFinalSyncExecutorCloses() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        var added = new CompletableFuture<Integer>();
        ByteBuf data = entry(0);
        owner.bookie().addEntry(data, (rc, lid, eid, address, ctx) -> added.complete(rc), null, new byte[0]);
        assertEquals(BookieProtocol.EOK, (int) added.get(5, TimeUnit.SECONDS));
        var bookie = (BookieImpl) field(owner, "implementation");
        var storage = (InterleavedLedgerStorage) bookie.ledgerStorage;
        var durable = bookie.journals.get(0).getLastLogMark().markLog().getCurMark();
        var executor = (ScheduledExecutorService) field(storage.gcThread, "gcExecutor");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        var rotation = executor.submit(() -> {
            assertThrows(IllegalStateException.class, () -> owner.closeAndAwait(Duration.ZERO));
            hold(entered, release, interrupted);
            ((EntryLogManagerForSingleEntryLog) storage.entryLogger.getEntryLogManager()).createNewLog(-1);
            return null;
        });
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndAwait(Duration.ofMillis(100)).drained());
            assertFalse(interrupted.get());
            probeAll(conf, "HELD");
            release.countDown();
            rotation.get(5, TimeUnit.SECONDS);
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            ByteBuffer mark = ByteBuffer.wrap(Files.readAllBytes(
                    Path.of(conf.getLedgerDirNames()[0], "current", "lastMark")));
            assertEquals(durable.getLogFileId(), mark.getLong());
            assertEquals(durable.getLogFileOffset(), mark.getLong());
            assertEquals(0, data.refCnt());
            probeAll(conf, "AVAILABLE");
        } finally {
            release.countDown();
            owner.closeAndAwait(Duration.ofSeconds(5));
        }
    }

    @Test(timeout = 30000)
    public void failedInputReleaseRetainsTheActualReferenceAfterTheRejectedOperationHasReturned() throws Exception {
        var conf = configuration();
        var owner = FreshClassicOwner.create(conf);
        owner.start();
        var handle = owner.bookie();
        ByteBuf good = entry(0);
        var added = new CompletableFuture<Integer>();
        handle.addEntry(good, (rc, lid, eid, address, ctx) -> added.complete(rc), null, new byte[0]);
        assertEquals(BookieProtocol.EOK, (int) added.get(5, TimeUnit.SECONDS));
        ByteBuf actual = entry(1);
        ByteBuf failing = mock(ByteBuf.class, delegatesTo(actual));
        RuntimeException failure = new RuntimeException("input reference release failed");
        doThrow(failure).when(failing).release();
        var callbacks = new AtomicInteger();
        try {
            assertThrows(BookieException.class, () -> handle.addEntry(failing,
                    (rc, lid, eid, address, ctx) -> callbacks.incrementAndGet(), null, new byte[]{1}));
            assertFalse(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertSame(failure, owner.status().closeFailure());
            assertEquals(0, callbacks.get());
            assertEquals(0, owner.status().pendingOperations());
            assertEquals(1, actual.refCnt());
            var bookie = (BookieImpl) field(owner, "implementation");
            var state = field(bookie, "fresh");
            var operations = (java.util.Set<?>) field(state, "operations");
            assertEquals(1, operations.size());
            assertSame(failing, field(operations.iterator().next(), "input"));
            actual.release();
            assertFalse(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            verify(failing, times(1)).release();
            probeAll(conf, "HELD");
        } finally {
            if (actual.refCnt() > 0) {
                actual.release();
            }
            owner.closeAndAwait(Duration.ofSeconds(5));
            ((FreshClassicRoots) field(owner, "roots")).close();
        }
    }

    @Test(timeout = 60000)
    public void everyAssemblyBoundaryRetainsTheOriginalOwnerAndItsOriginalFailure() throws Exception {
        for (String phase : List.of("directories", "state", "journal", "storage", "sync", "bookie-stats")) {
            var conf = configuration();
            StatsLogger stats = controlledStats();
            var ownerRef = new AtomicReference<FreshClassicOwner>();
            var reached = new AtomicBoolean();
            RuntimeException failure = new RuntimeException("controlled initialization failure: " + phase);
            org.mockito.stubbing.Answer<Object> fail = invocation -> {
                reached.set(true);
                throw failure;
            };
            if (phase.equals("directories")) {
                doAnswer(fail).when(stats).registerGauge(anyString(), any());
            } else if (phase.equals("state")) {
                doAnswer(fail).when(stats).registerGauge(eq(BookKeeperServerStats.SERVER_STATUS), any());
            } else if (phase.equals("journal")) {
                doAnswer(fail).when(stats).scopeLabel(eq("journalIndex"), anyString());
            } else if (phase.equals("storage")) {
                doAnswer(invocation -> {
                    var bookie = (BookieImpl) field(ownerRef.get(), "implementation");
                    var storage = (InterleavedLedgerStorage) bookie.ledgerStorage;
                    assertTrue(storage.entryLogger != null && storage.ledgerCache != null);
                    reached.set(true);
                    throw failure;
                }).when(stats).scope("gc");
            } else if (phase.equals("sync")) {
                doAnswer(fail).when(stats).getThreadScopedCounter("sync-thread-time");
            } else {
                doAnswer(invocation -> {
                    var bookie = (BookieImpl) field(ownerRef.get(), "implementation");
                    if (bookie.syncThread != null) {
                        reached.set(true);
                        throw failure;
                    }
                    return NullStatsLogger.INSTANCE.getCounter(BookKeeperServerStats.WRITE_BYTES);
                }).when(stats).getCounter(BookKeeperServerStats.WRITE_BYTES);
            }
            var owner = FreshClassicOwner.create(conf, stats);
            ownerRef.set(owner);
            try {
                assertSame(failure, assertThrows(RuntimeException.class, owner::start));
                assertTrue("Initialization boundary was not reached: " + phase, reached.get());
                var result = owner.closeAndAwait(Duration.ofSeconds(5));
                assertTrue(phase + ": " + result, result.drained());
                assertSame(failure, result.initializationFailure());
                assertNull(result.closeFailure());
                assertThrows(IllegalStateException.class, owner::bookie);
                probeAll(conf, "AVAILABLE");
            } finally {
                owner.closeAndAwait(Duration.ofSeconds(5));
            }
        }
    }

    @Test(timeout = 30000)
    public void multipleJournalsAndSeparateIndexesKeepTheirOwnCheckpointAndPhysicalRootObligations()
            throws Exception {
        var conf = configuration();
        String second = temporary.newFolder().getCanonicalPath();
        String index = temporary.newFolder().getCanonicalPath();
        conf.setJournalDirsName(new String[]{conf.getJournalDirNames()[0], second})
                .setJournalMaxMemorySizeMb(2).setAllowMultipleDirsUnderSameDiskPartition(true);
        conf.setProperty("indexDirectories", new String[]{index});
        var owner = FreshClassicOwner.create(conf);
        ByteBuf data = entry(0);
        try {
            owner.start();
            FreshClassicRootsTest.probe(Path.of(second, "fresh-classic-owner.lock"), "HELD");
            FreshClassicRootsTest.probe(Path.of(index, "fresh-classic-owner.lock"), "HELD");
            var written = new CompletableFuture<Integer>();
            owner.bookie().addEntry(data, (rc, lid, eid, address, ctx) -> written.complete(rc), null, new byte[0]);
            assertEquals(BookieProtocol.EOK, (int) written.get(5, TimeUnit.SECONDS));
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertFalse(Files.exists(Path.of(conf.getLedgerDirNames()[0], "current", "lastMark.0")));
            assertEquals(16, Files.size(Path.of(conf.getLedgerDirNames()[0], "current", "lastMark.1")));
            try (var files = Files.walk(Path.of(index, "current"))) {
                assertTrue(files.anyMatch(path -> path.toString().endsWith(".idx")));
            }
            FreshClassicRootsTest.probe(Path.of(second, "fresh-classic-owner.lock"), "AVAILABLE");
            FreshClassicRootsTest.probe(Path.of(index, "fresh-classic-owner.lock"), "AVAILABLE");
            assertEquals(0, data.refCnt());
        } finally {
            owner.closeAndAwait(Duration.ofSeconds(5));
            if (data.refCnt() > 0) {
                data.release();
            }
        }
    }

    @Test(timeout = 30000)
    public void interpolationIsResolvedBeforeValidationAndCannotChangeTheAcceptedRootsOrLocalStateLater()
            throws Exception {
        var conf = configuration();
        String rootProperty = "bookkeeper.fresh.owner.test.root";
        String sanityProperty = "bookkeeper.fresh.owner.test.sanity";
        String oldRoot = System.getProperty(rootProperty);
        String oldSanity = System.getProperty(sanityProperty);
        Path journal = temporary.newFolder("resolved,journal").toPath();
        Path changed = temporary.getRoot().toPath().resolve("changed-root");
        FreshClassicOwner owner = null;
        try {
            System.setProperty(rootProperty, journal.toString());
            System.setProperty(sanityProperty, "false");
            conf.setJournalDirsName(new String[]{"${sys:" + rootProperty + "}"});
            conf.setProperty("sanityCheckMetricsEnabled", "${sys:" + sanityProperty + "}");
            owner = FreshClassicOwner.create(conf);
            System.setProperty(rootProperty, changed.toString());
            System.setProperty(sanityProperty, "true");
            owner.start();
            var bookie = (BookieImpl) field(owner, "implementation");
            assertFalse(bookie.conf.isSanityCheckMetricsEnabled());
            assertEquals(1, bookie.conf.getJournalDirNames().length);
            assertEquals(journal.toString(), bookie.conf.getJournalDirNames()[0]);
            assertTrue(owner.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertTrue(Files.exists(journal.resolve("fresh-classic-owner.lock")));
            assertFalse(Files.exists(changed));
        } finally {
            if (owner != null) {
                owner.closeAndAwait(Duration.ofSeconds(5));
            }
            if (oldRoot == null) {
                System.clearProperty(rootProperty);
            } else {
                System.setProperty(rootProperty, oldRoot);
            }
            if (oldSanity == null) {
                System.clearProperty(sanityProperty);
            } else {
                System.setProperty(sanityProperty, oldSanity);
            }
        }
    }
}

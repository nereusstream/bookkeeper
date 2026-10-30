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
package org.apache.bookkeeper.bookie;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LedgerDescriptorImplTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    @Test
    public void rejectedMasterKeyDoesNotExposeEitherCredentialInTheLog() throws Exception {
        byte[] cached = {11, 22, 33, 44};
        byte[] requested = {55, 66, 77, 88};
        AtomicReference<String> rejectionLog = new AtomicReference<>();
        AbstractAppender capture = new AbstractAppender("master-key-rejection", null, null, true,
                Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getLoggerName().endsWith("LedgerDescriptorImpl")) {
                    LogEvent snapshot = event.toImmutable();
                    rejectionLog.set(snapshot.getMessage().getFormattedMessage() + " "
                            + snapshot.getContextData().toMap() + " " + snapshot);
                }
            }
        };
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        capture.start();
        configuration.addAppender(capture);
        configuration.getRootLogger().addAppender(capture, Level.ERROR, null);
        context.updateLoggers();
        try {
            LedgerDescriptorImpl descriptor = new LedgerDescriptorImpl(cached, 7, null);
            assertEquals(BookieException.Code.UnauthorizedAccessException,
                    assertThrows(BookieException.class, () -> descriptor.checkAccess(requested)).getCode());
            assertNotNull(rejectionLog.get());
            assertFalse(rejectionLog.get().contains(Arrays.toString(cached)));
            assertFalse(rejectionLog.get().contains(Arrays.toString(requested)));
        } finally {
            configuration.getRootLogger().removeAppender(capture.getName());
            capture.stop();
            context.updateLoggers();
        }
    }

    @Test
    public void fenceSubmissionTransfersOnlyTheJournalsRetainedReferenceUntilDeferredCompletion() throws Exception {
        var descriptor = descriptor();
        Journal journal = mock(Journal.class);
        var record = new AtomicReference<ByteBuf>();
        var callback = new AtomicReference<WriteCallback>();
        doAnswer(invocation -> {
            record.set(((ByteBuf) invocation.getArgument(0)).retain());
            callback.set(invocation.getArgument(2));
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), any());
        try {
            var result = descriptor.fenceAndLogInJournal(journal);
            assertFalse(result.isDone());
            assertSame(result, descriptor.fenceAndLogInJournal(journal));
            assertEquals(1, record.get().refCnt());
            callback.get().writeComplete(0, 7, BookieImpl.METAENTRY_ID_FENCE_KEY, null, null);
            record.get().release();
            assertTrue(result.get(1, TimeUnit.SECONDS));
            assertEquals(0, record.get().refCnt());
        } finally {
            releaseRemaining(record.get());
        }
    }

    @Test
    public void inlineFenceCompletionDoesNotReleaseTheOriginalReferenceBeforeSubmissionReturns() throws Exception {
        var descriptor = descriptor();
        Journal journal = mock(Journal.class);
        var record = new AtomicReference<ByteBuf>();
        doAnswer(invocation -> {
            ByteBuf data = invocation.getArgument(0);
            record.set(data);
            data.retain();
            try {
                assertEquals(2, data.refCnt());
                ((WriteCallback) invocation.getArgument(2)).writeComplete(0, 7,
                        BookieImpl.METAENTRY_ID_FENCE_KEY, null, null);
                assertEquals(2, data.refCnt());
            } finally {
                data.release();
            }
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), any());
        try {
            assertTrue(descriptor.fenceAndLogInJournal(journal).get(1, TimeUnit.SECONDS));
            assertEquals(0, record.get().refCnt());
        } finally {
            releaseRemaining(record.get());
        }
    }

    @Test
    public void rejectedFenceIsTerminalAndTheNextFenceCanRetryWithoutLeakingItsNewRecord() throws Exception {
        var descriptor = descriptor();
        Journal journal = mock(Journal.class);
        var record = new AtomicReference<ByteBuf>();
        var attempts = new AtomicInteger();
        RejectedExecutionException failure = new RejectedExecutionException("unpublished fence");
        doAnswer(invocation -> {
            ByteBuf data = invocation.getArgument(0);
            record.set(data);
            if (attempts.incrementAndGet() == 1) {
                throw failure;
            }
            data.retain();
            try {
                ((WriteCallback) invocation.getArgument(2)).writeComplete(0, 7,
                        BookieImpl.METAENTRY_ID_FENCE_KEY, null, null);
            } finally {
                data.release();
            }
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), any());
        try {
            assertSame(failure, assertThrows(RejectedExecutionException.class,
                    () -> descriptor.fenceAndLogInJournal(journal)));
            var first = fenceResult(descriptor);
            assertTrue(first.isCompletedExceptionally());
            assertEquals(0, record.get().refCnt());
            var second = descriptor.fenceAndLogInJournal(journal);
            assertNotSame(first, second);
            assertTrue(second.get(1, TimeUnit.SECONDS));
            assertEquals(0, record.get().refCnt());
        } finally {
            releaseRemaining(record.get());
        }
    }

    @Test
    public void interruptedUnpublishedFencePreservesInterruptionAndReleasesItsOriginalReference() throws Exception {
        var descriptor = descriptor();
        Journal journal = mock(Journal.class);
        var record = new AtomicReference<ByteBuf>();
        doAnswer(invocation -> {
            record.set(invocation.getArgument(0));
            throw new InterruptedException("unpublished fence interrupted");
        }).when(journal).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), any());
        try {
            assertTrue(descriptor.fenceAndLogInJournal(journal).isCompletedExceptionally());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, record.get().refCnt());
        } finally {
            Thread.interrupted();
            releaseRemaining(record.get());
        }
    }

    @Test
    public void failedFenceRecordConstructionDoesNotLeaveTheSharedFuturePending() throws Exception {
        var descriptor = descriptor();
        var failure = new OutOfMemoryError("fence record allocation");
        try (var records = mockStatic(LedgerDescriptor.class, CALLS_REAL_METHODS)) {
            records.when(() -> LedgerDescriptor.createLedgerFenceEntry(7L)).thenThrow(failure);
            assertSame(failure, assertThrows(OutOfMemoryError.class,
                    () -> descriptor.fenceAndLogInJournal(mock(Journal.class))));
            assertTrue(fenceResult(descriptor).isCompletedExceptionally());
        }
    }

    private static LedgerDescriptorImpl descriptor() throws Exception {
        LedgerStorage storage = mock(LedgerStorage.class);
        when(storage.setFenced(7L)).thenReturn(true, false);
        return new LedgerDescriptorImpl(new byte[0], 7, storage);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Boolean> fenceResult(LedgerDescriptorImpl descriptor) throws Exception {
        var field = LedgerDescriptorImpl.class.getDeclaredField("logFenceResult");
        field.setAccessible(true);
        return (CompletableFuture<Boolean>) field.get(descriptor);
    }

    private static void releaseRemaining(ByteBuf buffer) {
        if (buffer != null && buffer.refCnt() > 0) {
            buffer.release(buffer.refCnt());
        }
    }

    @Test
    public void failedFutureAllocationCannotTurnAnUnprovedInMemoryFenceIntoSuccess() throws Exception {
        var descriptor = descriptor();
        Journal journal = mock(Journal.class);
        var attempts = new AtomicInteger();
        doAnswer(invocation -> {
            attempts.incrementAndGet();
            ByteBuf data = invocation.getArgument(0);
            data.retain();
            try {
                ((WriteCallback) invocation.getArgument(2)).writeComplete(0, 7,
                        BookieImpl.METAENTRY_ID_FENCE_KEY, null, null);
            } finally {
                data.release();
            }
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), eq(false), any(WriteCallback.class), any());
        var failure = new OutOfMemoryError("fence future allocation");
        try (var futures = mockStatic(FutureUtils.class, CALLS_REAL_METHODS)) {
            futures.when(FutureUtils::createFuture).thenThrow(failure);
            assertSame(failure, assertThrows(OutOfMemoryError.class,
                    () -> descriptor.fenceAndLogInJournal(journal)));
        }
        assertTrue(descriptor.fenceAndLogInJournal(journal).get(1, TimeUnit.SECONDS));
        assertEquals(1, attempts.get());
    }

    @Test(timeout = 30000)
    public void actualClassicQueueInterruptionReturnsTheUnpublishedFencesReferenceAndReservation() throws Exception {
        var journalRoot = temporary.newFolder();
        var journalCurrent = Files.createDirectory(journalRoot.toPath().resolve("current")).toFile();
        var ledgerRoot = temporary.newFolder();
        Files.createDirectory(ledgerRoot.toPath().resolve("current"));
        var conf = new ServerConfiguration().setJournalDirName(journalRoot.getCanonicalPath())
                .setLedgerDirNames(new String[]{ledgerRoot.getCanonicalPath()}).setJournalQueueSize(2)
                .setJournalMaxMemorySizeMb(1).setJournalWriteBufferSizeKB(4).setJournalMaxGroupWaitMSec(1)
                .setJournalFlushWhenQueueEmpty(true);
        conf.setProperty("journalPreAllocSizeMB", 1);
        var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
        Journal journal = new Journal(0, journalCurrent, conf, directories);
        var completed = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            ByteBuf accepted = LedgerDescriptor.createLedgerFenceEntry(7L);
            try {
                journal.logAddEntry(accepted, false,
                        (rc, lid, eid, address, context) -> completed.countDown(), null);
            } finally {
                accepted.release();
            }
        }
        var record = new AtomicReference<ByteBuf>();
        try {
            try (var records = mockStatic(LedgerDescriptor.class, CALLS_REAL_METHODS)) {
                records.when(() -> LedgerDescriptor.createLedgerFenceEntry(7L)).thenAnswer(invocation -> {
                    ByteBuf data = (ByteBuf) invocation.callRealMethod();
                    record.set(data);
                    return data;
                });
                var target = descriptor();
                Thread.currentThread().interrupt();
                assertTrue(target.fenceAndLogInJournal(journal).isCompletedExceptionally());
                assertTrue(Thread.currentThread().isInterrupted());
                assertEquals(0, record.get().refCnt());
                assertEquals(2, journal.getJournalQueueLength());
                assertEquals(32, journal.getMemoryUsage());
            }
        } finally {
            Thread.interrupted();
            releaseRemaining(record.get());
            journal.start();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            journal.shutdown();
        }
        assertEquals(0, journal.getMemoryUsage());
    }

    @Test
    public void recoveredFencedStateKeepsTheOriginalCompletedShortcut() throws Exception {
        LedgerStorage storage = mock(LedgerStorage.class);
        when(storage.setFenced(7L)).thenReturn(false);
        var descriptor = new LedgerDescriptorImpl(new byte[0], 7, storage);
        var result = descriptor.fenceAndLogInJournal(mock(Journal.class));
        assertTrue(result.isDone());
        assertTrue(result.get(1, TimeUnit.SECONDS));
    }

    @Test(timeout = 30000)
    public void actualMemoryWaitInterruptionDoesNotDebitUnacquiredBytesOrLoseTheOriginalException()
            throws Exception {
        var journalRoot = temporary.newFolder();
        var current = Files.createDirectory(journalRoot.toPath().resolve("current")).toFile();
        var ledgerRoot = temporary.newFolder();
        Files.createDirectory(ledgerRoot.toPath().resolve("current"));
        var conf = new ServerConfiguration().setJournalDirName(journalRoot.getCanonicalPath())
                .setLedgerDirNames(new String[]{ledgerRoot.getCanonicalPath()}).setJournalQueueSize(4)
                .setJournalMaxMemorySizeMb(1).setJournalWriteBufferSizeKB(4).setJournalMaxGroupWaitMSec(1)
                .setJournalFlushWhenQueueEmpty(true);
        conf.setProperty("journalPreAllocSizeMB", 1);
        var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
        StatsLogger stats = mock(StatsLogger.class, delegatesTo(NullStatsLogger.INSTANCE));
        doReturn(stats).when(stats).scopeLabel(any(String.class), any(String.class));
        var counter = mock(Counter.class, delegatesTo(NullStatsLogger.INSTANCE.getCounter("queue")));
        doReturn(counter).when(stats).getCounter(BookKeeperServerStats.JOURNAL_QUEUE_SIZE);
        var producer = new AtomicReference<Thread>();
        var failure = new RuntimeException("queue metric rollback failed");
        doAnswer(invocation -> {
            if (Thread.currentThread() == producer.get()) {
                throw failure;
            }
            return null;
        }).when(counter).dec();
        Journal journal = new Journal(0, current, conf, directories, stats,
                io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
        // Two 16-byte records already exceed this limit; the next reservation must wait.
        journal.setMemoryLimitController(new MemoryLimitController(16));
        var completed = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            ByteBuf accepted = LedgerDescriptor.createLedgerFenceEntry(7L);
            try {
                journal.logAddEntry(accepted, false,
                        (rc, lid, eid, address, context) -> completed.countDown(), null);
            } finally {
                accepted.release();
            }
        }
        var record = new AtomicReference<ByteBuf>();
        var attempt = new FutureTask<CompletableFuture<Boolean>>(() -> {
            var target = descriptor();
            try (var records = mockStatic(LedgerDescriptor.class, CALLS_REAL_METHODS)) {
                records.when(() -> LedgerDescriptor.createLedgerFenceEntry(7L)).thenAnswer(invocation -> {
                    ByteBuf data = (ByteBuf) invocation.callRealMethod();
                    record.set(data);
                    return data;
                });
                return target.fenceAndLogInJournal(journal);
            }
        });
        Thread caller = new Thread(attempt);
        producer.set(caller);
        try {
            caller.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> record.get() != null
                    && caller.getState() == Thread.State.WAITING);
            assertEquals(32, journal.getMemoryUsage());
            assertEquals(2, journal.getJournalQueueLength());
            caller.interrupt();
            var result = attempt.get(5, TimeUnit.SECONDS);
            var observed = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> result.get(1, TimeUnit.SECONDS)).getCause();
            assertTrue(observed instanceof InterruptedException);
            assertEquals(1, observed.getSuppressed().length);
            assertSame(failure, observed.getSuppressed()[0]);
            assertEquals(0, record.get().refCnt());
            assertEquals(32, journal.getMemoryUsage());
            assertEquals(2, journal.getJournalQueueLength());
        } finally {
            caller.interrupt();
            caller.join(5000);
            releaseRemaining(record.get());
            journal.start();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            journal.shutdown();
        }
        assertEquals(0, journal.getMemoryUsage());
    }
}

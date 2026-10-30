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

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Fresh Journal resource/force facts only; no full Bookie/facade, root release or full-bytes proof. */
public class FreshClassicJournalDrainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 30000)
    public void pureOwnerAndUnstartedCloseDoNotCreateAFileOrIssueAReplayContext() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.create();
            assertEquals(0, fixture.current.list().length);
            assertThrows(IllegalStateException.class, journal::start);
            assertTrue(journal.closeForFresh().drained());
            assertEquals(0, fixture.current.list().length);
            assertThrows(IllegalStateException.class, journal::initializeForFresh);
            assertThrows(IllegalStateException.class, journal::shutdown);
        }
        try (var fixture = new Fixture()) {
            fixture.conf.setJournalFormatVersionToWrite(5);
            assertThrows(IllegalArgumentException.class, fixture::create);
            fixture.conf.setJournalFormatVersionToWrite(6).setFileInfoFormatVersionToWrite(0);
            assertThrows(IllegalArgumentException.class, fixture::create);
            fixture.conf.setFileInfoFormatVersionToWrite(1).setBusyWaitEnabled(true);
            assertThrows(IllegalArgumentException.class, fixture::create);
            assertEquals(0, fixture.current.list().length);
        }
    }

    @Test(timeout = 30000)
    public void realV6RecordsForceAndCheckpointRemainSeparateFromAppendQuiescence() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            var results = new Results();
            fixture.submit(0, 32, results);
            fixture.submit(BookieImpl.METAENTRY_ID_FENCE_KEY, 16, results);
            fixture.submit(BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC, 32, results);
            await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 3);
            assertTrue(results.codes.values().stream().allMatch(rc -> rc == BookieProtocol.EOK));
            var cut = journal.newCheckpoint();
            assertFalse(new File(fixture.ledgerCurrent, "lastMark").exists());
            assertTrue(journal.quiesceForFresh().quiesced());
            assertThrows(java.util.concurrent.RejectedExecutionException.class,
                    () -> fixture.submit(9, 32, results));
            journal.checkpointComplete(cut, false);
            byte[] mark = Files.readAllBytes(fixture.ledgerCurrent.toPath().resolve("lastMark"));
            assertEquals(16, mark.length);
            ByteBuffer bytes = ByteBuffer.wrap(mark);
            var durable = journal.getLastLogMark().markLog().getCurMark();
            assertEquals(durable.getLogFileId(), bytes.getLong());
            assertEquals(durable.getLogFileOffset(), bytes.getLong());
            assertTrue(journal.closeForFresh().drained());
            assertThrows(IOException.class, () -> journal.checkpointComplete(cut, false));
            assertEquals(List.of(0L, BookieImpl.METAENTRY_ID_FENCE_KEY,
                    BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC), fixture.readRecordIds());
            fixture.assertSourcesReleased();
        }
    }

    @Test(timeout = 30000)
    public void heldActualForceKeepsClosePendingWithoutInterruptingTheFileUser() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            JournalChannel channel = fixture.active();
            ControlledFileChannel controlled = control(channel);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var interrupted = new AtomicBoolean();
            controlled.beforeForce = () -> hold(entered, release, interrupted);
            var results = new Results();
            var close = new FutureTask<>(journal::closeForFresh);
            Thread closer = new Thread(close);
            try {
                fixture.submit(0, 32, results);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closer.start();
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, results.count());
                assertTrue(controlled.delegate.isOpen());
                assertTrue(forceThread(journal).isAlive());
                assertFalse(interrupted.get());
                closer.interrupt();
                assertTrue(assertThrows(ExecutionException.class, () -> close.get(5, TimeUnit.SECONDS))
                        .getCause() instanceof InterruptedException);
                assertTrue(controlled.delegate.isOpen());
                assertTrue(forceThread(journal).isAlive());
                assertFalse(interrupted.get());
                release.countDown();
                assertTrue(journal.closeForFresh().drained());
                assertEquals(BookieProtocol.EOK, results.code(0));
                assertEquals(1, results.calls(0));
                assertFalse(controlled.delegate.isOpen());
                assertFalse(forceThread(journal).isAlive());
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
                closer.join(5000);
            }
        }
    }

    @Test(timeout = 30000)
    public void callbackThrowAndRenamedThreadCannotDuplicateCompletionOrAwaitThemselves() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            RuntimeException failure = new RuntimeException("user callback failed");
            var second = new CountDownLatch(1);
            var firstCalls = new AtomicInteger();
            fixture.submit(0, 32, (rc, lid, eid, address, context) -> {
                firstCalls.incrementAndGet();
                assertEquals(BookieProtocol.EOK, rc);
                Thread.currentThread().setName("renamed-journal-callback");
                assertTrue(journal.isCurrentResourceActivityForFresh());
                assertThrows(IllegalStateException.class, journal::closeForFresh);
                throw failure;
            });
            fixture.submit(1, 32, (rc, lid, eid, address, context) -> second.countDown());
            assertTrue(second.await(5, TimeUnit.SECONDS));
            var result = journal.closeForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.callbackFailure());
            assertEquals(1, firstCalls.get());
            fixture.assertSourcesReleased();
        }
    }

    @Test(timeout = 30000)
    public void partialLazyRafAndHeaderAcquisitionStayOwnedAfterInitializationFailure() throws Exception {
        File directory = temporary.newFolder();
        var conf = new ServerConfiguration();
        var partial = new AtomicReference<FileChannel>();
        var failure = new FileNotFoundException("after native acquisition");
        var provider = new DefaultFileChannelProvider() {
            @Override
            public BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException {
                return new DefaultFileChannel(file, configuration) {
                    @Override
                    public FileChannel getFileChannel() throws FileNotFoundException {
                        partial.set(super.getFileChannel());
                        throw failure;
                    }
                };
            }
        };
        JournalChannel owner = JournalChannel.forFresh(directory, 1, 1024 * 1024, 4096, 512,
                conf, UnpooledByteBufAllocator.DEFAULT);
        assertSame(failure, assertThrows(FileNotFoundException.class, () -> owner.initializeForFresh(provider)));
        assertTrue(partial.get().isOpen());
        var closed = owner.closeForFresh();
        assertTrue(closed.drained());
        assertSame(failure, closed.operationFailure());
        assertFalse(partial.get().isOpen());

        var controlled = new AtomicReference<ControlledFileChannel>();
        var headerProvider = new DefaultFileChannelProvider() {
            @Override
            public BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException {
                return new DefaultFileChannel(file, configuration) {
                    @Override
                    public FileChannel getFileChannel() throws FileNotFoundException {
                        var wrapped = new ControlledFileChannel(super.getFileChannel());
                        wrapped.failPartialWrite.set(true);
                        wrapped.failAtPosition = 0L;
                        controlled.set(wrapped);
                        return wrapped;
                    }
                };
            }
        };
        JournalChannel header = JournalChannel.forFresh(directory, 2, 1024 * 1024, 4096, 512,
                conf, UnpooledByteBufAllocator.DEFAULT);
        IOException headerFailure = assertThrows(IOException.class, () -> header.initializeForFresh(headerProvider));
        assertSame(controlled.get().writeFailure, headerFailure);
        assertTrue(header.closeForFresh().drained());
        assertFalse(controlled.get().delegate.isOpen());
        assertEquals(3, new File(directory, "2.txn").length());
    }

    @Test(timeout = 30000)
    public void realAutomaticCloseDoesNotBecomeAQualifiedNoopOwnerClose() throws Exception {
        File directory = temporary.newFolder();
        JournalChannel channel = JournalChannel.forFresh(directory, 1, 1024 * 1024, 4096, 512,
                new ServerConfiguration(), UnpooledByteBufAllocator.DEFAULT);
        channel.initializeForFresh(new DefaultFileChannelProvider());
        try {
            Thread.currentThread().interrupt();
            assertThrows(ClosedByInterruptException.class, () -> channel.fc.read(ByteBuffer.allocate(1), 0));
        } finally {
            Thread.interrupted();
        }
        var result = channel.closeForFresh();
        assertFalse(result.drained());
        assertNotNull(result.closeFailure());
        assertSame(result.closeFailure(), channel.closeForFresh().closeFailure());
        assertEquals(0, channel.bc.writeBuffer.refCnt());
    }

    @Test(timeout = 30000)
    public void durableRequestStaysEokWhenItsLaterNativeCloseFails() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            JournalChannel channel = fixture.active();
            RandomAccessFile actual = raf(channel);
            RandomAccessFile failing = spy(actual);
            IOException failure = new IOException("RAF native close failed");
            doThrow(failure).when(failing).close();
            setRaf(channel, failing);
            try {
                var results = new Results();
                fixture.submit(0, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 1);
                var first = journal.closeForFresh();
                assertFalse(first.drained());
                assertEquals(BookieProtocol.EOK, results.code(0));
                assertSame(failure, first.closeFailure());
                assertSame(failure, journal.closeForFresh().closeFailure());
                verify(failing, times(1)).close();
                assertTrue(actual.getFD().valid());
                fixture.assertSourcesReleased();
            } finally {
                actual.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void crossLogGroupCannotAdvancePastTheEarlierFailedForce() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            JournalChannel old = fixture.active();
            ControlledFileChannel controlled = control(old);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var calls = new AtomicInteger();
            var firstCut = new AtomicLong();
            IOException failure = new IOException("old file force failed");
            controlled.beforeForce = () -> {
                if (calls.incrementAndGet() == 1) {
                    firstCut.set(old.bc.getFileChannelPosition());
                    hold(entered, release, new AtomicBoolean());
                } else if (calls.get() == 2) {
                    controlled.forceFailure = failure;
                }
            };
            var results = new Results();
            try {
                fixture.submit(0, 600 * 1024, results);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                fixture.submit(1, 600 * 1024, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() == 1
                        && Journal.listJournalIds(fixture.current, null).size() == 2);
                fixture.submit(2, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() >= 2);
                release.countDown();
                var quiet = journal.quiesceForFresh();
                assertTrue(quiet.quiesced());
                assertSame(failure, quiet.operationFailure());
                assertEquals(BookieProtocol.EOK, results.code(0));
                assertEquals(BookieProtocol.EIO, results.code(1));
                assertEquals(BookieProtocol.EIO, results.code(2));
                assertEquals(3, results.count());
                assertTrue(results.counts.values().stream().allMatch(n -> n.get() == 1));
                long firstId = Journal.listJournalIds(fixture.current, null).get(0);
                assertEquals(firstId, journal.getLastLogMark().getCurMark().getLogFileId());
                assertEquals(firstCut.get(), journal.getLastLogMark().getCurMark().getLogFileOffset());
                assertTrue(journal.closeForFresh().drained());
                assertEquals(BookieProtocol.EIO, results.code(1));
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
            }
        }
    }

    @Test(timeout = 30000)
    public void oldLogCloseFailurePreservesAllProvenForceResultsAndTheFile() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.conf.setMaxBackupJournals(0);
            Journal journal = fixture.start();
            JournalChannel old = fixture.active();
            ControlledFileChannel controlled = control(old);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            controlled.beforeForce = () -> hold(entered, release, new AtomicBoolean());
            RandomAccessFile actual = raf(old);
            RandomAccessFile failing = spy(actual);
            IOException failure = new IOException("rolled RAF close failed");
            doThrow(failure).when(failing).close();
            setRaf(old, failing);
            var results = new Results();
            try {
                fixture.submit(0, 600 * 1024, results);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                fixture.submit(1, 600 * 1024, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() == 1
                        && Journal.listJournalIds(fixture.current, null).size() == 2);
                fixture.submit(2, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() >= 2);
                long firstId = Journal.listJournalIds(fixture.current, null).get(0);
                release.countDown();
                assertTrue(journal.quiesceForFresh().quiesced());
                assertEquals(3, results.count());
                assertTrue(results.codes.values().stream().allMatch(rc -> rc == BookieProtocol.EOK));
                journal.checkpointComplete(journal.newCheckpoint(), true);
                assertTrue(new File(fixture.current, Long.toHexString(firstId) + ".txn").exists());
                var result = journal.closeForFresh();
                assertFalse(result.drained());
                assertSame(failure, result.closeFailure());
                verify(failing, times(1)).close();
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
                actual.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void fullQueuesDoNotNeedASpareDataSlotForCloseAndRejectUnpublishedSubmission() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.conf.setJournalQueueSize(1);
            Journal journal = fixture.start();
            ControlledFileChannel controlled = control(fixture.active());
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            controlled.beforeForce = () -> hold(entered, release, new AtomicBoolean());
            var results = new Results();
            var producer = new FutureTask<>(() -> {
                fixture.submit(4, 32, results);
                return null;
            });
            var close = new FutureTask<>(journal::closeForFresh);
            Thread caller = new Thread(producer);
            Thread closer = new Thread(close);
            try {
                fixture.submit(0, 32, results);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                fixture.submit(1, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() == 1);
                fixture.submit(2, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> requests(journal) == 3);
                fixture.submit(3, 32, results);
                caller.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> producers(journal) == 1);
                closer.start();
                assertTrue(assertThrows(ExecutionException.class, () -> producer.get(5, TimeUnit.SECONDS))
                        .getCause() instanceof java.util.concurrent.RejectedExecutionException);
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
                assertEquals(4, results.count());
                assertEquals(0, results.calls(4));
                assertEquals(0, journal.getMemoryUsage());
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
                caller.join(5000);
                closer.join(5000);
            }
        }
    }

    @Test(timeout = 30000)
    public void interruptedUnpublishedProducerReturnsItsSourceWithoutACallback() throws Exception {
        try (var fixture = new Fixture()) {
            fixture.conf.setJournalQueueSize(1);
            Journal journal = fixture.start();
            ControlledFileChannel controlled = control(fixture.active());
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            controlled.beforeForce = () -> hold(entered, release, new AtomicBoolean());
            var results = new Results();
            var producer = new FutureTask<>(() -> {
                fixture.submit(4, 32, results);
                return null;
            });
            Thread caller = new Thread(producer);
            try {
                fixture.submit(0, 32, results);
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                fixture.submit(1, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> journal.forceWriteRequests.size() == 1);
                fixture.submit(2, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> requests(journal) == 3);
                fixture.submit(3, 32, results);
                caller.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> producers(journal) == 1);
                caller.interrupt();
                assertTrue(assertThrows(ExecutionException.class, () -> producer.get(5, TimeUnit.SECONDS))
                        .getCause() instanceof InterruptedException);
                assertEquals(0, results.calls(4));
                assertEquals(0, fixture.sources.get(4).refCnt());
                assertTrue(controlled.delegate.isOpen());
                release.countDown();
                assertTrue(journal.closeForFresh().drained());
                assertEquals(4, results.count());
                assertTrue(results.codes.values().stream().allMatch(rc -> rc == BookieProtocol.EOK));
                assertTrue(results.counts.values().stream().allMatch(n -> n.get() == 1));
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
                caller.join(5000);
            }
        }
    }

    @Test(timeout = 30000)
    public void shortWritesCompleteTheHeaderPreallocationAndFrozenBufferedBytes() throws Exception {
        File directory = temporary.newFolder();
        var controlled = new AtomicReference<ControlledFileChannel>();
        var provider = new DefaultFileChannelProvider() {
            @Override
            public BookieFileChannel open(File file, ServerConfiguration configuration) throws IOException {
                return new DefaultFileChannel(file, configuration) {
                    @Override
                    public FileChannel getFileChannel() throws FileNotFoundException {
                        var wrapped = new ControlledFileChannel(super.getFileChannel());
                        wrapped.maxPositionalWrite = 3;
                        controlled.set(wrapped);
                        return wrapped;
                    }
                };
            }
        };
        JournalChannel channel = JournalChannel.forFresh(directory, 1, 1024, 4096, 512,
                new ServerConfiguration(), UnpooledByteBufAllocator.DEFAULT);
        ByteBuf source = Unpooled.buffer(2600).writeLong(7).writeLong(19).writeZero(2584);
        try {
            channel.initializeForFresh(provider);
            assertEquals(1024, channel.nextPrealloc);
            channel.preAllocIfNeeded(source.readableBytes());
            assertEquals(4096, channel.nextPrealloc);
            channel.bc.write(source);
            channel.bc.flush();
            channel.forceThroughForFresh(channel.bc.position());
            assertTrue(channel.closeForFresh().drained());
            assertFalse(controlled.get().delegate.isOpen());
            ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(directory.toPath().resolve("1.txn")));
            assertEquals(0x424b4c47, bytes.getInt());
            assertEquals(6, bytes.getInt());
            bytes.position(512);
            assertEquals(7, bytes.getLong());
            assertEquals(19, bytes.getLong());
            while (bytes.hasRemaining()) {
                assertEquals(0, bytes.get());
            }
            assertEquals(0, channel.bc.writeBuffer.refCnt());
        } finally {
            source.release();
            channel.closeForFresh();
        }
    }

    @Test(timeout = 30000)
    public void partialLastMarkWriteRetriesOnlyTheFrozenSixteenBytes() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            var results = new Results();
            fixture.submit(0, 32, results);
            await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 1);
            var cut = journal.newCheckpoint();
            var controlled = new AtomicReference<ControlledFileChannel>();
            IOException failure;
            try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                open.when(() -> FileChannel.open(any(Path.class), eq(CREATE_NEW), eq(READ), eq(WRITE),
                        eq(LinkOption.NOFOLLOW_LINKS))).thenAnswer(invocation -> {
                            var wrapped = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                            wrapped.failPartialWrite.set(true);
                            wrapped.maxPositionalWrite = 3;
                            controlled.set(wrapped);
                            return wrapped;
                        });
                failure = assertThrows(IOException.class, () -> journal.checkpointComplete(cut, true));
                assertSame(controlled.get().writeFailure, failure);
                assertEquals(0, persisted(journal).getLogFileId());
                Path mark = fixture.ledgerCurrent.toPath().resolve("lastMark");
                assertEquals(3, Files.size(mark));
                journal.checkpointComplete(cut, false);
                ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(mark));
                assertEquals(16, bytes.remaining());
                assertEquals(persisted(journal).getLogFileId(), bytes.getLong());
                assertEquals(persisted(journal).getLogFileOffset(), bytes.getLong());
                assertFalse(controlled.get().delegate.isOpen());
            }
            var closed = journal.closeForFresh();
            assertTrue(closed.drained());
            assertSame(failure, closed.checkpointFailure());
            assertEquals(1, results.calls(0));
            fixture.assertSourcesReleased();
        }
    }

    @Test(timeout = 30000)
    public void lastMarkForceFailureDoesNotAdvancePersistenceAndRetriesTheSameFrozenCut() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            var results = new Results();
            fixture.submit(0, 32, results);
            await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 1);
            var old = journal.newCheckpoint();
            IOException failure = new IOException("lastMark force failed");
            var controlled = new AtomicReference<ControlledFileChannel>();
            try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                open.when(() -> FileChannel.open(any(Path.class), eq(CREATE_NEW), eq(READ), eq(WRITE),
                        eq(LinkOption.NOFOLLOW_LINKS))).thenAnswer(invocation -> {
                            var wrapped = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                            wrapped.forceFailure = failure;
                            controlled.set(wrapped);
                            return wrapped;
                        });
                assertSame(failure, assertThrows(IOException.class, () -> journal.checkpointComplete(old, true)));
                assertEquals(0, persisted(journal).getLogFileId());
                assertTrue(controlled.get().delegate.isOpen());
                fixture.submit(1, 32, results);
                await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 2);
                journal.checkpointComplete(journal.newCheckpoint(), false);
                assertFalse(controlled.get().delegate.isOpen());
                byte[] newer = Files.readAllBytes(fixture.ledgerCurrent.toPath().resolve("lastMark"));
                journal.checkpointComplete(old, false);
                assertTrue(java.util.Arrays.equals(newer,
                        Files.readAllBytes(fixture.ledgerCurrent.toPath().resolve("lastMark"))));
                assertTrue(persisted(journal).getLogFileOffset() > 0);
            }
            var closed = journal.closeForFresh();
            assertTrue(closed.drained());
            assertSame(failure, closed.checkpointFailure());
        }
    }

    @Test(timeout = 30000)
    public void finalCloseWaitsForTheActualExternalCheckpointReturn() throws Exception {
        try (var fixture = new Fixture()) {
            Journal journal = fixture.start();
            var results = new Results();
            fixture.submit(0, 32, results);
            await().atMost(5, TimeUnit.SECONDS).until(() -> results.count() == 1);
            assertTrue(journal.quiesceForFresh().quiesced());
            var cut = journal.newCheckpoint();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var markChannel = new AtomicReference<ControlledFileChannel>();
            var checkpoint = new FutureTask<Void>(() -> {
                try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                    open.when(() -> FileChannel.open(any(Path.class), eq(CREATE_NEW), eq(READ), eq(WRITE),
                            eq(LinkOption.NOFOLLOW_LINKS))).thenAnswer(invocation -> {
                                var wrapped = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                                wrapped.beforeForce = () -> hold(entered, release, new AtomicBoolean());
                                markChannel.set(wrapped);
                                return wrapped;
                            });
                    journal.checkpointComplete(cut, false);
                }
                return null;
            });
            JournalChannel data = fixture.active();
            var close = new FutureTask<>(journal::closeForFresh);
            Thread cp = new Thread(checkpoint);
            Thread closer = new Thread(close);
            try {
                cp.start();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closer.start();
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                assertTrue(data.fc.isOpen());
                assertTrue(markChannel.get().delegate.isOpen());
                release.countDown();
                checkpoint.get(5, TimeUnit.SECONDS);
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
                assertFalse(data.fc.isOpen());
                assertFalse(markChannel.get().delegate.isOpen());
            } finally {
                release.countDown();
                cp.join(5000);
                closer.join(5000);
            }
        }
    }

    @Test(timeout = 30000)
    public void halfStartedThreadsAndFailedBufferConstructionKeepTheAlreadyOpenedRaf() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        OutOfMemoryError failure = new OutOfMemoryError("journal write buffer allocation");
        ByteBufAllocator allocator = mock(ByteBufAllocator.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doAnswer(invocation -> {
            hold(entered, release, new AtomicBoolean());
            throw failure;
        }).when(allocator).directBuffer(anyInt());
        try (var fixture = new Fixture()) {
            Journal journal = fixture.create(allocator);
            journal.initializeForFresh();
            journal.start();
            var results = new Results();
            var close = new FutureTask<>(journal::closeForFresh);
            Thread closer = new Thread(close);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertTrue(forceThread(journal).isAlive());
                fixture.submit(0, 32, results);
                closer.start();
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                var result = close.get(5, TimeUnit.SECONDS);
                assertTrue(result.drained());
                assertSame(failure, result.operationFailure());
                assertEquals(BookieProtocol.EIO, results.code(0));
                assertEquals(1, results.calls(0));
                fixture.assertSourcesReleased();
            } finally {
                release.countDown();
                closer.join(5000);
            }
        }
    }

    private static void hold(CountDownLatch entered, CountDownLatch release, AtomicBoolean interrupted) {
        entered.countDown();
        try {
            if (!release.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("controlled force/allocator was not released");
            }
        } catch (InterruptedException failure) {
            interrupted.set(true);
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static ControlledFileChannel control(JournalChannel channel) throws Exception {
        synchronized (channel) {
            var wrapped = new ControlledFileChannel(channel.fc);
            channel.fc = wrapped;
            var field = BufferedChannelBase.class.getDeclaredField("fileChannel");
            field.setAccessible(true);
            field.set(channel.bc, wrapped);
            return wrapped;
        }
    }

    private static RandomAccessFile raf(JournalChannel channel) throws Exception {
        var field = DefaultFileChannel.class.getDeclaredField("randomAccessFile");
        field.setAccessible(true);
        return (RandomAccessFile) field.get(channel.channel);
    }

    private static void setRaf(JournalChannel channel, RandomAccessFile raf) throws Exception {
        var field = DefaultFileChannel.class.getDeclaredField("randomAccessFile");
        field.setAccessible(true);
        field.set(channel.channel, raf);
    }

    private static Object state(Journal journal) throws Exception {
        var field = Journal.class.getDeclaredField("fresh");
        field.setAccessible(true);
        return field.get(journal);
    }

    private static int producers(Journal journal) throws Exception {
        Object state = state(journal);
        var gate = state.getClass().getDeclaredField("gate");
        gate.setAccessible(true);
        synchronized (gate.get(state)) {
            var field = state.getClass().getDeclaredField("producers");
            field.setAccessible(true);
            return field.getInt(state);
        }
    }

    private static int requests(Journal journal) throws Exception {
        Object state = state(journal);
        var gate = state.getClass().getDeclaredField("gate");
        gate.setAccessible(true);
        synchronized (gate.get(state)) {
            var field = state.getClass().getDeclaredField("requests");
            field.setAccessible(true);
            return ((java.util.Set<?>) field.get(state)).size();
        }
    }

    private static Thread forceThread(Journal journal) throws Exception {
        var field = Journal.class.getDeclaredField("forceWriteThread");
        field.setAccessible(true);
        return (Thread) field.get(journal);
    }

    private static LogMark persisted(Journal journal) throws Exception {
        var field = Journal.class.getDeclaredField("lastPersistedMark");
        field.setAccessible(true);
        return (LogMark) field.get(journal);
    }

    private static final class Results implements WriteCallback {
        final Map<Long, Integer> codes = new ConcurrentHashMap<>();
        final Map<Long, AtomicInteger> counts = new ConcurrentHashMap<>();

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId,
                org.apache.bookkeeper.net.BookieId address, Object context) {
            counts.computeIfAbsent(entryId, ignored -> new AtomicInteger()).incrementAndGet();
            codes.put(entryId, rc);
        }

        int count() {
            return codes.size();
        }

        int code(long id) {
            return codes.get(id);
        }

        int calls(long id) {
            return counts.getOrDefault(id, new AtomicInteger()).get();
        }
    }

    private final class Fixture implements AutoCloseable {
        final ServerConfiguration conf;
        final File current;
        final File ledgerCurrent;
        final LedgerDirsManager directories;
        final List<ByteBuf> sources = java.util.Collections.synchronizedList(new ArrayList<>());
        Journal journal;

        Fixture() throws Exception {
            File journalRoot = temporary.newFolder();
            current = Files.createDirectory(journalRoot.toPath().resolve("current")).toFile();
            File ledgerRoot = temporary.newFolder();
            ledgerCurrent = Files.createDirectory(ledgerRoot.toPath().resolve("current")).toFile();
            conf = new ServerConfiguration().setJournalDirName(journalRoot.getCanonicalPath())
                    .setLedgerDirNames(new String[]{ledgerRoot.getCanonicalPath()})
                    .setJournalMaxMemorySizeMb(1).setJournalQueueSize(4)
                    .setJournalWriteBufferSizeKB(4).setMaxJournalSizeMB(1)
                    .setJournalBufferedWritesThreshold(1).setJournalMaxGroupWaitMSec(1)
                    .setJournalFlushWhenQueueEmpty(true);
            conf.setProperty("journalPreAllocSizeMB", 1);
            directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
        }

        Journal create() {
            return create(UnpooledByteBufAllocator.DEFAULT);
        }

        Journal create(ByteBufAllocator allocator) {
            journal = Journal.forFresh(0, current, conf, directories, NullStatsLogger.INSTANCE, allocator, null);
            return journal;
        }

        Journal start() throws Exception {
            Journal candidate = create();
            candidate.initializeForFresh();
            candidate.start();
            active();
            return candidate;
        }

        @SuppressWarnings("unchecked")
        JournalChannel active() throws Exception {
            var selected = new AtomicReference<JournalChannel>();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                Object state = state(journal);
                var gate = state.getClass().getDeclaredField("gate");
                gate.setAccessible(true);
                JournalChannel channel;
                synchronized (gate.get(state)) {
                    var field = state.getClass().getDeclaredField("channels");
                    field.setAccessible(true);
                    List<JournalChannel> list = (List<JournalChannel>) field.get(state);
                    if (list.isEmpty()) {
                        return false;
                    }
                    channel = list.get(list.size() - 1);
                }
                synchronized (channel) {
                    if (channel.bc != null && channel.nextPrealloc > 0) {
                        selected.set(channel);
                        return true;
                    }
                }
                return false;
            });
            return selected.get();
        }

        void submit(long id, int size, WriteCallback callback) throws Exception {
            ByteBuf source;
            if (id == BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC) {
                source = Unpooled.buffer(36).writeLong(7).writeLong(id).writeInt(16).writeLong(7).writeLong(99);
            } else {
                source = Unpooled.buffer(size).writeLong(7).writeLong(id);
                if (size >= 24) {
                    source.writeLong(0);
                }
                source.writeZero(size - source.writerIndex());
            }
            sources.add(source);
            try {
                journal.logAddEntry(7, id, source, false, callback, null);
            } finally {
                source.release();
            }
        }

        void assertSourcesReleased() {
            for (ByteBuf source : sources) {
                assertEquals(0, source.refCnt());
            }
            assertEquals(0, journal.getMemoryUsage());
        }

        List<Long> readRecordIds() throws Exception {
            List<Long> ids = new ArrayList<>();
            for (long id : Journal.listJournalIds(current, null)) {
                Path path = current.toPath().resolve(Long.toHexString(id) + ".txn");
                byte[] file = Files.readAllBytes(path);
                ByteBuffer bytes = ByteBuffer.wrap(file);
                assertEquals(0x424b4c47, bytes.getInt());
                assertEquals(6, bytes.getInt());
                bytes.position(512);
                while (bytes.remaining() >= 4) {
                    int length = bytes.getInt();
                    if (length == 0) {
                        break;
                    }
                    if (length == -256) {
                        int padding = bytes.getInt();
                        bytes.position(Math.addExact(bytes.position(), padding));
                    } else {
                        assertTrue(length >= 16 && length <= bytes.remaining());
                        int start = bytes.position();
                        assertEquals(7, bytes.getLong());
                        long entryId = bytes.getLong();
                        ids.add(entryId);
                        if (entryId == BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC) {
                            assertEquals(16, bytes.getInt());
                            assertEquals(7, bytes.getLong());
                            assertEquals(99, bytes.getLong());
                        }
                        bytes.position(Math.addExact(start, length));
                    }
                }
            }
            return ids;
        }

        @Override
        public void close() throws Exception {
            if (journal != null) {
                journal.closeForFresh();
            }
        }
    }
}

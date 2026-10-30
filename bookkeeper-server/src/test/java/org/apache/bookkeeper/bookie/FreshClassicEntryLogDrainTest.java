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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real entry-log files and child ownership, not a complete fresh owner, ACK, or root-unlock proof. */
public class FreshClassicEntryLogDrainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void partialFlushRetriesTheFrozenRangeAtTheSameCoordinates() throws Exception {
        File file = temporary.newFile();
        var controlled = new ControlledFileChannel(file);
        var channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, controlled, 64, 64, 0, true);
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        try {
            channel.write(payload);
            controlled.failPartialWrite.set(true);
            assertSame(controlled.writeFailure, assertThrows(IOException.class, channel::flush));
            assertEquals(3, controlled.delegate.size());
            assertEquals(0, channel.getFileChannelPosition());
            assertThrows(IOException.class, () -> channel.write(payload));
            assertThrows(IllegalStateException.class, channel::clear);
            channel.flushAndForceWrite(true);
            assertEquals(8, channel.getFileChannelPosition());
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Files.readAllBytes(file.toPath()));
            channel.closeForFresh();
            channel.close();
            assertEquals(0, channel.writeBuffer.refCnt());
            assertEquals(0, channel.readBuffer.refCnt());
            assertEquals(1, controlled.closeAttempts.get());
        } finally {
            payload.release();
            channel.closeForFresh();
            controlled.delegate.close();
        }
    }

    @Test
    public void failedNativeCloseStaysFailedEvenWhenJdkReportsClosedAndSecondCloseIsNoop() throws Exception {
        var controlled = new ControlledFileChannel(temporary.newFile());
        controlled.closeFailure = new IOException("native close failed before the real fd closed");
        var channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, controlled, 64, 64, 0, true);
        try {
            assertSame(controlled.closeFailure, assertThrows(IOException.class, channel::closeForFresh));
            assertFalse(controlled.isOpen());
            assertTrue(controlled.delegate.isOpen());
            controlled.close(); // The JDK's second close is a no-op, not new native-close evidence.
            assertSame(controlled.closeFailure, assertThrows(IOException.class, channel::close));
            assertEquals(1, controlled.closeAttempts.get());
            assertEquals(0, channel.writeBuffer.refCnt());
            assertEquals(0, channel.readBuffer.refCnt());
        } finally {
            controlled.delegate.close();
        }
    }

    @Test
    public void cancelledCloseObservationCannotReleaseBuffersWhileActualCloseIsStillRunning() throws Exception {
        var controlled = new ControlledFileChannel(temporary.newFile());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        controlled.beforeClose = () -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failure);
            }
        };
        var channel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, controlled, 64, 64, 0, true);
        var result = new FutureTask<Void>(() -> {
            channel.closeForFresh();
            return null;
        });
        var thread = new Thread(result);
        try {
            thread.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(result.cancel(false));
            assertTrue(result.isDone());
            assertTrue(thread.isAlive());
            assertEquals(1, channel.writeBuffer.refCnt());
            assertEquals(1, channel.readBuffer.refCnt());
            release.countDown();
            thread.join(5000);
            assertFalse(thread.isAlive());
            channel.closeForFresh();
            assertEquals(0, channel.writeBuffer.refCnt());
            assertEquals(0, channel.readBuffer.refCnt());
            assertFalse(controlled.delegate.isOpen());
        } finally {
            release.countDown();
            thread.join(5000);
            controlled.delegate.close();
        }
    }

    @Test
    public void allocatorClosesOnlyUnclaimedCandidatesAndPreservesTheSameTerminalResult() throws Exception {
        try (var fixture = new Fixture(UnpooledByteBufAllocator.DEFAULT)) {
            fixture.start();
            assertTrue(fixture.conf.isEntryLogFilePreAllocationEnabled());
            var candidate = fixture.allocator.getPreallocationFuture().get(5, TimeUnit.SECONDS);
            var result = fixture.allocator.stopForFresh();
            assertTrue(result.drained());
            assertNull(result.allocationFailure());
            assertNull(result.closeFailure());
            assertSame(result, fixture.allocator.stopForFresh());
            assertFalse(candidate.fileChannel.isOpen());
            assertEquals(0, candidate.writeBuffer.refCnt());
            assertEquals(0, candidate.readBuffer.refCnt());
            assertEquals(0, fixture.allocator.logfileHeader.refCnt());
            assertTrue(fixture.active.fileChannel.isOpen());
            assertEquals(1, fixture.active.writeBuffer.refCnt());
            assertThrows(IOException.class, () -> fixture.allocator.createNewLog(fixture.current));
            assertThrows(IOException.class, () -> fixture.allocator.createNewLogForCompaction(fixture.current));
            assertThrows(IllegalStateException.class, fixture.allocator::stop);
        }
    }

    @Test
    public void stickyPartialChannelCloseFailureDoesNotHideIndependentCandidateCleanup() throws Exception {
        var controlled = new ControlledFileChannel(temporary.newFile());
        controlled.closeFailure = new IOException("unresolved native fd close");
        try (var fixture = new Fixture(UnpooledByteBufAllocator.DEFAULT)) {
            fixture.start();
            var candidate = fixture.allocator.getPreallocationFuture().get(5, TimeUnit.SECONDS);
            // Inject a native-close error into the same registered raw-slot state used after partial construction.
            var resources = EntryLoggerAllocator.class.getDeclaredField("freshAllocations");
            resources.setAccessible(true);
            List<?> allocations = (List<?>) resources.get(fixture.allocator);
            var constructor = allocations.get(0).getClass().getDeclaredConstructor();
            constructor.setAccessible(true);
            Object partial = constructor.newInstance();
            var raw = partial.getClass().getDeclaredField("channel");
            raw.setAccessible(true);
            raw.set(partial, controlled);
            addPartialSlot(allocations, partial);
            var result = fixture.allocator.stopForFresh();
            assertFalse(result.drained());
            assertSame(controlled.closeFailure, result.closeFailure());
            assertNull(result.allocationFailure());
            assertFalse(candidate.fileChannel.isOpen());
            assertEquals(0, candidate.writeBuffer.refCnt());
            assertEquals(0, fixture.allocator.logfileHeader.refCnt());
            assertTrue(controlled.delegate.isOpen());
            assertFalse(controlled.isOpen());
            controlled.close();
            assertSame(result, fixture.allocator.stopForFresh());
            assertEquals(1, controlled.closeAttempts.get());
        } finally {
            controlled.delegate.close();
        }
    }

    @Test
    public void cancelledPreallocationStillOwnsItsLateFileAndInterruptedStopCanContinueWaiting() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var actualThread = new AtomicReference<Thread>();
        var allocatedBuffer = new AtomicReference<ByteBuf>();
        ByteBufAllocator buffers = mock(ByteBufAllocator.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doAnswer(invocation -> {
            ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.directBuffer(invocation.getArgument(0));
            if (calls.incrementAndGet() == 2) {
                allocatedBuffer.set(buffer);
                actualThread.set(Thread.currentThread());
                entered.countDown();
                release.await();
            }
            return buffer;
        }).when(buffers).directBuffer(anyInt());
        try (var fixture = new Fixture(buffers)) {
            var interrupted = new AtomicBoolean();
            var unexpected = new AtomicReference<Throwable>();
            var firstCloser = new Thread(() -> {
                try {
                    fixture.allocator.stopForFresh();
                } catch (InterruptedException expected) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                } catch (Throwable failure) {
                    unexpected.set(failure);
                }
            });
            var close = new FutureTask<>(fixture.allocator::stopForFresh);
            var secondCloser = new Thread(close);
            try {
                fixture.start();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                FileChannel candidateFd = ownedRawChannel(fixture.allocator);
                assertTrue(candidateFd.isOpen());
                assertTrue(fixture.allocator.getPreallocationFuture().cancel(false));
                firstCloser.start();
                await().atMost(5, TimeUnit.SECONDS).until(fixture.allocator.allocatorExecutor::isShutdown);
                firstCloser.interrupt();
                firstCloser.join(5000);
                assertTrue(interrupted.get());
                assertNull(unexpected.get());
                assertTrue(candidateFd.isOpen());
                assertEquals(1, fixture.allocator.logfileHeader.refCnt());
                secondCloser.start();
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
                assertFalse(candidateFd.isOpen());
                assertFalse(actualThread.get().isAlive());
                assertEquals(0, allocatedBuffer.get().refCnt());
                assertEquals(0, fixture.allocator.logfileHeader.refCnt());
            } finally {
                release.countDown();
                firstCloser.join(5000);
                secondCloser.join(5000);
            }
        }
    }

    @Test
    public void constructorFailureRetainsTheRawFdAndOriginalFailureAfterSuccessfulCleanup() throws Exception {
        var reference = new AtomicReference<EntryLoggerAllocator>();
        var calls = new AtomicInteger();
        var raw = new AtomicReference<FileChannel>();
        var failure = new IllegalStateException("direct buffer construction failed after file open");
        ByteBufAllocator buffers = mock(ByteBufAllocator.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doAnswer(invocation -> {
            if (calls.incrementAndGet() == 2) {
                raw.set(ownedRawChannel(reference.get()));
                throw failure;
            }
            return UnpooledByteBufAllocator.DEFAULT.directBuffer(invocation.getArgument(0));
        }).when(buffers).directBuffer(anyInt());
        try (var fixture = new Fixture(buffers)) {
            reference.set(fixture.allocator);
            fixture.start();
            var observed = assertThrows(ExecutionException.class,
                    () -> fixture.allocator.getPreallocationFuture().get(5, TimeUnit.SECONDS));
            assertSame(failure, observed.getCause());
            assertTrue(raw.get().isOpen());
            var result = fixture.allocator.stopForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.allocationFailure());
            assertNull(result.closeFailure());
            assertFalse(raw.get().isOpen());
            assertSame(result, fixture.allocator.stopForFresh());
            assertEquals(0, fixture.allocator.logfileHeader.refCnt());
        }
    }

    @Test
    public void closeStopsNewSubmissionBeforeAnAlreadyAdmittedAllocationReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        ByteBufAllocator buffers = mock(ByteBufAllocator.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doAnswer(invocation -> {
            calls.incrementAndGet();
            entered.countDown();
            release.await();
            return UnpooledByteBufAllocator.DEFAULT.directBuffer(invocation.getArgument(0));
        }).when(buffers).directBuffer(anyInt());
        try (var fixture = new Fixture(buffers)) {
            var allocate = new FutureTask<>(() -> fixture.allocator.createNewLog(fixture.current));
            var producer = new Thread(allocate);
            var close = new FutureTask<>(fixture.allocator::stopForFresh);
            var closer = new Thread(close);
            try {
                producer.start();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closer.start();
                var stopped = EntryLoggerAllocator.class.getDeclaredField("freshStopRequested");
                stopped.setAccessible(true);
                await().atMost(5, TimeUnit.SECONDS).until(() -> stopped.getBoolean(fixture.allocator));
                assertThrows(IOException.class, () -> fixture.allocator.createNewLogForCompaction(fixture.current));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                release.countDown();
                fixture.active = allocate.get(5, TimeUnit.SECONDS);
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
                assertNull(fixture.allocator.getPreallocationFuture());
                assertEquals(1, calls.get());
                assertTrue(fixture.active.fileChannel.isOpen());
            } finally {
                release.countDown();
                producer.join(5000);
                closer.join(5000);
            }
        }
    }

    @Test
    public void realLastIdOpenFailureKeepsTheConstructedCandidateUntilCleanup() throws Exception {
        try (var fixture = new Fixture(UnpooledByteBufAllocator.DEFAULT)) {
            Files.createDirectory(fixture.current.toPath().resolve("lastId"));
            IOException failure = assertThrows(IOException.class, fixture::start);
            FileChannel raw = ownedRawChannel(fixture.allocator);
            assertTrue(raw.isOpen());
            var result = fixture.allocator.stopForFresh();
            assertTrue(result.drained());
            assertSame(failure, result.allocationFailure());
            assertNull(result.closeFailure());
            assertFalse(raw.isOpen());
            assertEquals(0, fixture.allocator.logfileHeader.refCnt());
        }
    }

    @Test
    public void renamedAllocatorThreadCannotAwaitItselfAndUnstartedAllocatorCanClose() throws Exception {
        var reference = new AtomicReference<EntryLoggerAllocator>();
        var calls = new AtomicInteger();
        var checked = new AtomicBoolean();
        ByteBufAllocator buffers = mock(ByteBufAllocator.class, delegatesTo(UnpooledByteBufAllocator.DEFAULT));
        doAnswer(invocation -> {
            if (calls.incrementAndGet() == 2) {
                Thread.currentThread().setName("renamed-entry-log-allocator");
                assertTrue(reference.get().isCurrentExecutorThread());
                assertThrows(IllegalStateException.class, reference.get()::stopForFresh);
                checked.set(true);
            }
            return UnpooledByteBufAllocator.DEFAULT.directBuffer(invocation.getArgument(0));
        }).when(buffers).directBuffer(anyInt());
        try (var fixture = new Fixture(buffers)) {
            reference.set(fixture.allocator);
            fixture.start();
            fixture.allocator.getPreallocationFuture().get(5, TimeUnit.SECONDS);
            assertTrue(checked.get());
        }
        try (var fixture = new Fixture(UnpooledByteBufAllocator.DEFAULT)) {
            var result = fixture.allocator.stopForFresh();
            assertTrue(result.drained());
            assertSame(result, fixture.allocator.stopForFresh());
            assertEquals(0, fixture.allocator.logfileHeader.refCnt());
        }
    }

    // Inspect only the test's already-registered partial resource; no production raw getter is added.
    private static FileChannel ownedRawChannel(EntryLoggerAllocator allocator) throws Exception {
        var resources = EntryLoggerAllocator.class.getDeclaredField("freshAllocations");
        resources.setAccessible(true);
        Object allocation = ((List<?>) resources.get(allocator)).get(0);
        var channel = allocation.getClass().getDeclaredField("channel");
        channel.setAccessible(true);
        return (FileChannel) channel.get(allocation);
    }

    @SuppressWarnings("unchecked")
    private static <T> void addPartialSlot(List<T> allocations, Object partial) {
        allocations.add((T) partial);
    }

    private final class Fixture implements AutoCloseable {
        private final ServerConfiguration conf;
        private final File current;
        private final EntryLoggerAllocator allocator;
        private BufferedLogChannel active;

        private Fixture(ByteBufAllocator buffers) throws Exception {
            File root = temporary.newFolder();
            current = Files.createDirectory(root.toPath().resolve("current")).toFile();
            conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()});
            var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.999f, 0.99f));
            allocator = EntryLoggerAllocator.forFresh(conf, directories,
                    new DefaultEntryLogger.RecentEntryLogsStatus(0), -1, buffers);
        }

        private void start() throws IOException {
            active = allocator.createNewLog(current);
        }

        @Override
        public void close() throws Exception {
            allocator.stopForFresh();
            if (active != null) {
                active.closeForFresh();
            }
        }
    }

    /** A real FileChannel with controlled failures inside JDK final close and positional write. */
    private static final class ControlledFileChannel extends FileChannel {
        private final FileChannel delegate;
        private final AtomicInteger closeAttempts = new AtomicInteger();
        private final AtomicBoolean failPartialWrite = new AtomicBoolean();
        private final IOException writeFailure = new IOException("failed after three bytes were written");
        private IOException closeFailure;
        private Runnable beforeClose;

        private ControlledFileChannel(File file) throws IOException {
            delegate = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            closeAttempts.incrementAndGet();
            if (beforeClose != null) {
                beforeClose.run();
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
            delegate.close();
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            if (failPartialWrite.compareAndSet(true, false)) {
                int limit = src.limit();
                src.limit(src.position() + Math.min(3, src.remaining()));
                try {
                    delegate.write(src, position);
                } finally {
                    src.limit(limit);
                }
                throw writeFailure;
            }
            return delegate.write(src, position);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return delegate.write(src);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return delegate.write(srcs, offset, length);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return delegate.read(dst);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return delegate.read(dst, position);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return delegate.read(dsts, offset, length);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public FileChannel position(long position) throws IOException {
            delegate.position(position);
            return this;
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public void force(boolean metadata) throws IOException {
            delegate.force(metadata);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return delegate.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return delegate.transferFrom(src, position, count);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return delegate.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return delegate.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return delegate.tryLock(position, size, shared);
        }
    }
}

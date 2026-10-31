/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;

/** Actual native handles, with faults confined to test-side wrappers around their first release calls. */
public class SegmentDataFileLockTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void firstPrivateCreationHasItsOwnerBeforeTheOnlyFileOpen() throws Exception {
        Path path = temporary.newFolder().toPath().toRealPath().resolve("route.lock");
        SegmentDataFileLock owner = SegmentDataFileLock.creatingControlOwner(path);
        assertFalse(Files.exists(path));
        var options = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
            try (var factory = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                factory.when(() -> FileChannel.open(eq(path), eq(options), any(FileAttribute[].class)))
                        .thenAnswer(invocation -> {
                            assertSame(owner, registered(path));
                            assertFalse(Files.exists(path));
                            return invocation.callRealMethod();
                        });
                owner.initialize();
            }
            assertTrue(owner.lock().isValid());
            assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(path));
        } finally {
            owner.close();
        }
        assertTrue(owner.drainResult().drained());
    }

    @Test
    public void symbolicParentDotDotUsesTheSamePhysicalDirectoryAsTheOriginalFactory() throws Exception {
        Path parent = temporary.newFolder().toPath().toRealPath();
        Files.createDirectories(parent.resolve("target/nested"));
        Path actualDirectory = Files.createDirectory(parent.resolve("target/chosen"));
        Path lexicalDirectory = Files.createDirectory(parent.resolve("chosen"));
        Files.createSymbolicLink(parent.resolve("alias"), parent.resolve("target/nested"));
        try (var lease = SegmentDataFileLock.create(parent.resolve("alias/../chosen"))) {
            assertTrue(Files.exists(actualDirectory.resolve("isolated.lock")));
            assertFalse(Files.exists(lexicalDirectory.resolve("isolated.lock")));
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(actualDirectory));
        }
        try (var next = SegmentDataFileLock.open(actualDirectory)) {
            assertTrue(next.lock().isValid());
        }
    }

    @Test
    public void releasedLockDoesNotEraseAFailedChannelCloseOrPermitInProcessReentry() throws Exception {
        Path directory = temporary.newFolder().toPath();
        SegmentDataFileLock lease = SegmentDataFileLock.create(directory);
        FileChannel actual = lease.channel();
        FaultChannel fault = new FaultChannel(actual);
        fault.closeFailure = new IOException("native channel close failed");
        replace(lease, "channel", fault);
        try {
            assertSame(fault.closeFailure, assertThrows(IOException.class, lease::close));
            assertFalse(lease.lock().isValid());
            assertTrue(actual.isOpen());
            assertFalse(fault.isOpen());
            assertSame(fault.closeFailure, assertThrows(IOException.class, lease::close));
            assertEquals(1, fault.closes.get());
            var result = lease.drainResult();
            assertFalse(result.drained());
            assertNull(result.lock().failure());
            assertSame(fault.closeFailure, result.channel().failure());
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(directory));
        } finally {
            actual.close();
        }
        assertFalse(lease.drainResult().drained());
    }

    @Test
    public void failedReleaseStillClosesTheChannelOnceAndRetainsItsOriginalResult() throws Exception {
        Path directory = temporary.newFolder().toPath();
        SegmentDataFileLock lease = SegmentDataFileLock.create(directory);
        FileChannel actual = lease.channel();
        FileLock original = lease.lock();
        AtomicInteger releases = new AtomicInteger();
        IOException failure = new IOException("native release failed");
        FileLock broken = new FileLock(actual, original.position(), original.size(), original.isShared()) {
            @Override
            public boolean isValid() {
                return original.isValid();
            }

            @Override
            public void release() throws IOException {
                releases.incrementAndGet();
                throw failure;
            }
        };
        FaultChannel channel = new FaultChannel(actual);
        replace(lease, "lock", broken);
        replace(lease, "channel", channel);
        try {
            assertSame(failure, assertThrows(IOException.class, lease::close));
            assertFalse(actual.isOpen());
            assertFalse(original.isValid());
            assertSame(failure, assertThrows(IOException.class, lease::close));
            assertEquals(1, releases.get());
            assertEquals(1, channel.closes.get());
            var result = lease.drainResult();
            assertFalse(result.drained());
            assertSame(failure, result.lock().failure());
            assertNull(result.channel().failure());
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(directory));
        } finally {
            actual.close();
        }
    }

    @Test
    public void partialAcquisitionAndFailedCleanupRemainWithTheRegisteredTwoPhaseOwner() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("route.lock")).toRealPath();
        FileChannel actual = openActual(path);
        FaultChannel channel = new FaultChannel(actual);
        channel.lockFailure = new IOException("lock acquisition failed");
        channel.closeFailure = new IOException("partial channel close failed");
        SegmentDataFileLock owner = SegmentDataFileLock.existingControlOwner(path);
        try {
            try (var factory = returning(path, channel)) {
                assertSame(channel.lockFailure, assertThrows(IOException.class, owner::initialize));
            }
            assertSame(channel, owner.channel());
            assertSame(channel.lockFailure, owner.drainResult().acquisitionFailure());
            assertSame(channel.closeFailure, assertThrows(IOException.class, owner::close));
            assertSame(channel.closeFailure, assertThrows(IOException.class, owner::close));
            assertSame(owner, registered(path));
            assertEquals(1, channel.closes.get());
            assertTrue(actual.isOpen());
            assertFalse(owner.drainResult().drained());
            assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(path));
        } finally {
            actual.close();
        }
    }

    @Test
    public void staticFactoryFailureRetainsTheFailedOwnerAndDoesNotJustRetainAPath() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("route.lock")).toRealPath();
        FileChannel actual = openActual(path);
        FaultChannel channel = new FaultChannel(actual);
        channel.lockFailure = new IOException("static acquisition failed");
        channel.closeFailure = new IOException("static cleanup failed");
        try {
            try (var factory = returning(path, channel)) {
                assertSame(channel.lockFailure,
                        assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(path)));
            }
            assertArrayEquals(new Throwable[]{channel.closeFailure}, channel.lockFailure.getSuppressed());
            SegmentDataFileLock retained = registered(path);
            assertNotNull(retained);
            assertSame(channel, retained.channel());
            assertSame(channel.closeFailure, assertThrows(IOException.class, retained::close));
            assertEquals(1, channel.closes.get());
            assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(path));
        } finally {
            actual.close();
        }
    }

    @Test
    public void acquisitionFailureWithSuccessfulCleanupAllowsANewActualOwner() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("route.lock")).toRealPath();
        FileChannel actual = openActual(path);
        FaultChannel channel = new FaultChannel(actual);
        channel.lockFailure = new IOException("acquisition failed before obtaining a lock");
        try (var factory = returning(path, channel)) {
            assertSame(channel.lockFailure,
                    assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(path)));
        } finally {
            actual.close();
        }
        assertEquals(1, channel.closes.get());
        assertEquals(0, channel.lockFailure.getSuppressed().length);
        assertNull(registered(path));
        try (var next = SegmentDataFileLock.openExistingControl(path)) {
            assertTrue(next.lock().isValid());
        }
    }

    @Test
    public void pureOwnerAndSuccessfulCloseNeverCreateOrKeepAnUnownedScope() throws Exception {
        Path path = temporary.newFolder().toPath().resolve("not-created.lock");
        SegmentDataFileLock unused = SegmentDataFileLock.existingControlOwner(path);
        assertFalse(Files.exists(path));
        unused.close();
        assertTrue(unused.drainResult().drained());
        assertThrows(IOException.class, unused::initialize);
        assertFalse(Files.exists(path));
        Files.createFile(path);
        SegmentDataFileLock first = SegmentDataFileLock.openExistingControl(path);
        first.close();
        first.close();
        assertTrue(first.drainResult().drained());
        try (var next = SegmentDataFileLock.openExistingControl(path)) {
            assertTrue(next.lock().isValid());
        }
    }

    @Test
    public void unobservedNativeCloseCannotBeRequalifiedByNoOpClose() throws Exception {
        Path directory = temporary.newFolder().toPath();
        SegmentDataFileLock lease = SegmentDataFileLock.create(directory);
        lease.channel().close();
        IOException first = assertThrows(IOException.class, lease::close);
        assertSame(first, assertThrows(IOException.class, lease::close));
        assertFalse(lease.drainResult().drained());
        assertNotNull(lease.drainResult().lock().failure());
        assertNotNull(lease.drainResult().channel().failure());
        assertEquals(1, first.getSuppressed().length);
        assertThrows(IOException.class, () -> SegmentDataFileLock.open(directory));
    }

    @Test(timeout = 30000)
    public void initializationActuallyReturnsBeforeAnotherThreadCanCloseItsChannel() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("route.lock")).toRealPath();
        FileChannel actual = openActual(path);
        FaultChannel channel = new FaultChannel(actual);
        SegmentDataFileLock owner = SegmentDataFileLock.existingControlOwner(path);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failed = new AtomicReference<>();
        channel.beforeLock = () -> {
            assertThrows(IllegalStateException.class, owner::close);
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        Thread initializer = new Thread(() -> {
            try (var factory = returning(path, channel)) {
                owner.initialize();
            } catch (Throwable failure) {
                failed.set(failure);
            }
        });
        FutureTask<Void> closing = new FutureTask<>(() -> {
            owner.close();
            return null;
        });
        Thread closer = new Thread(closing);
        try {
            initializer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(closing.isDone());
            assertEquals(0, channel.closes.get());
            assertTrue(actual.isOpen());
            assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(path));
            release.countDown();
            initializer.join(5000);
            closing.get(5, TimeUnit.SECONDS);
            closer.join(5000);
            assertNull(failed.get());
            assertTrue(owner.drainResult().drained());
            assertEquals(1, channel.closes.get());
        } finally {
            release.countDown();
            initializer.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertFalse(initializer.isAlive());
            assertFalse(closer.isAlive());
            actual.close();
        }
    }

    static FileChannel openActual(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
    }

    static MockedStatic<FileChannel> returning(Path path, FileChannel channel) {
        var factory = mockStatic(FileChannel.class, CALLS_REAL_METHODS);
        factory.when(() -> FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS)).thenReturn(channel);
        return factory;
    }

    static void replace(SegmentDataFileLock lease, String field, Object value) throws Exception {
        var member = SegmentDataFileLock.class.getDeclaredField(field);
        member.setAccessible(true);
        member.set(lease, value);
    }

    @SuppressWarnings("unchecked")
    private static SegmentDataFileLock registered(Path path) throws Exception {
        var member = SegmentDataFileLock.class.getDeclaredField("ACTIVE");
        member.setAccessible(true);
        var active = (Map<Path, SegmentDataFileLock>) member.get(null);
        synchronized (active) {
            return active.get(path);
        }
    }

    static final class FaultChannel extends FileChannel {
        final FileChannel actual;
        final AtomicInteger closes = new AtomicInteger();
        IOException closeFailure;
        IOException lockFailure;
        Runnable beforeLock;

        FaultChannel(FileChannel actual) {
            this.actual = actual;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            closes.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
            actual.close();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            if (beforeLock != null) {
                beforeLock.run();
            }
            if (lockFailure != null) {
                throw lockFailure;
            }
            return actual.tryLock(position, size, shared);
        }

        @Override
        public int read(ByteBuffer buffer) throws IOException {
            return actual.read(buffer);
        }

        @Override
        public long read(ByteBuffer[] buffers, int offset, int length) throws IOException {
            return actual.read(buffers, offset, length);
        }

        @Override
        public int write(ByteBuffer buffer) throws IOException {
            return actual.write(buffer);
        }

        @Override
        public long write(ByteBuffer[] buffers, int offset, int length) throws IOException {
            return actual.write(buffers, offset, length);
        }

        @Override
        public long position() throws IOException {
            return actual.position();
        }

        @Override
        public FileChannel position(long position) throws IOException {
            actual.position(position);
            return this;
        }

        @Override
        public long size() throws IOException {
            return actual.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            actual.truncate(size);
            return this;
        }

        @Override
        public void force(boolean metadata) throws IOException {
            actual.force(metadata);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return actual.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel source, long position, long count) throws IOException {
            return actual.transferFrom(source, position, count);
        }

        @Override
        public int read(ByteBuffer buffer, long position) throws IOException {
            return actual.read(buffer, position);
        }

        @Override
        public int write(ByteBuffer buffer, long position) throws IOException {
            return actual.write(buffer, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return actual.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return actual.lock(position, size, shared);
        }
    }
}

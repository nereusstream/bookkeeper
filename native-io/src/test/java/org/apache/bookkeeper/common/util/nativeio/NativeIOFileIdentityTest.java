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
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual host descriptor observations only; no approved vendor, owned trust source or drain authority. */
public class NativeIOFileIdentityTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static NativeIO io() {
        assumeTrue(SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC);
        return new NativeIOImpl();
    }

    private static NativeIO.FileIdentity pathIdentity(Path path) throws Exception {
        return new NativeIO.FileIdentity(((Number) Files.getAttribute(path, "unix:dev")).longValue(),
                ((Number) Files.getAttribute(path, "unix:ino")).longValue());
    }

    @Test
    public void actualOriginalFdSurvivesPathRenameReplacementWithoutChangingPositionOrReadAbility() throws Exception {
        NativeIO io = io();
        Path path = temporary.newFile().toPath();
        Files.write(path, new byte[]{10, 20, 30});
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(1);
            var original = io.fileIdentity(channel);
            assertEquals(pathIdentity(path), original);
            Path renamed = path.resolveSibling(path.getFileName() + ".renamed");
            Files.move(path, renamed);
            Files.write(path, new byte[]{40, 50});
            assertNotEquals(original, pathIdentity(path));
            assertEquals(original, pathIdentity(renamed));
            assertEquals(original, io.fileIdentity(channel));
            assertEquals(original, io.fileIdentity(channel));
            assertEquals(1, channel.position());
            ByteBuffer value = ByteBuffer.allocate(1);
            assertEquals(1, channel.read(value));
            assertEquals(20, value.get(0));
            assertTrue(channel.isOpen());
        }
    }

    @Test
    public void nullClosedAndNonOriginalWrapperRejectWithoutTouchingTheOriginalChannel() throws Exception {
        NativeIO io = io();
        assertThrows(NativeIOException.class, () -> io.fileIdentity(null));
        try (FileChannel original = FileChannel.open(temporary.newFile().toPath(), StandardOpenOption.READ);
             FileChannel wrapper = new Wrapper(original)) {
            var expected = io.fileIdentity(original);
            assertThrows(NativeIOException.class, () -> io.fileIdentity(wrapper));
            assertThrows(NativeIOException.class, () -> io.directoryFileSystem(wrapper));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(wrapper));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(wrapper));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(wrapper));
            var notDirectory = assertThrows(NativeIOException.class, () -> io.directoryFileSystem(original));
            assertTrue(notDirectory.getMessage().contains("not a directory"));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(original));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(original));
            assertTrue(wrapper.isOpen());
            assertEquals(expected, io.fileIdentity(original));
            original.close();
            assertThrows(NativeIOException.class, () -> io.fileIdentity(original));
        }
    }

    @Test(timeout = 30000)
    public void descriptorMonitorWaitFollowedByLogicalCloseCannotReturnAnIdentity() throws Exception {
        NativeIO io = io();
        FileInputStream input = new FileInputStream(temporary.newFile());
        FileChannel channel = input.getChannel();
        var query = new FutureTask<>(() -> io.fileIdentity(channel));
        var close = new FutureTask<Void>(() -> {
            channel.close();
            return null;
        });
        Thread reader = new Thread(query, "file-id-query-fixture");
        Thread closer = new Thread(close, "file-id-close-fixture");
        reader.setDaemon(true);
        closer.setDaemon(true);
        try {
            io.fileIdentity(channel); // Resolve the JNI/classes before this deterministic monitor schedule.
            synchronized (input.getFD()) {
                reader.start();
                await(() -> reader.getState() == Thread.State.BLOCKED);
                closer.start();
                await(() -> !channel.isOpen());
                assertFalse(query.isDone());
                assertFalse(close.isDone());
            }
            var failure = assertThrows(ExecutionException.class, () -> query.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof NativeIOException);
            close.get(5, TimeUnit.SECONDS);
            reader.join(5000);
            closer.join(5000);
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
            assertFalse(channel.isOpen());
        } finally {
            // A failed/stranded JNI fixture is not drained; do not hide it by blocking this thread on another close.
            if (!reader.isAlive() && !closer.isAlive()) {
                input.close();
            }
        }
    }

    private static void await(BooleanSupplier condition) {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() - end < 0) {
            Thread.yield();
        }
        assertTrue("Expected actual fixture monitor/close state was not reached", condition.getAsBoolean());
    }

    @Test
    public void legacyDefaultRejectsAndDtoKeepsAllRawBitPatterns() {
        var legacy = (NativeIO) Proxy.newProxyInstance(NativeIO.class.getClassLoader(), new Class<?>[]{NativeIO.class},
                (proxy, method, arguments) -> InvocationHandler.invokeDefault(proxy, method, arguments));
        assertThrows(NativeIOException.class, () -> legacy.fileIdentity(null));
        var raw = new NativeIO.FileIdentity(Long.MIN_VALUE, -1);
        assertEquals(Long.MIN_VALUE, raw.device());
        assertEquals(-1, raw.inode());
        assertEquals(0, new NativeIO.FileIdentity(0, 0).inode());
    }

    /** New Java with the original sealed JNI: only the added capability must reject. */
    public static final class OldLibraryProbe {
        public static void main(String[] arguments) throws Exception {
            NativeIO io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().effectiveUid() >= 0);
            try (FileChannel channel = FileChannel.open(Path.of(arguments[0]), StandardOpenOption.READ)) {
                var failure = assertThrows(NativeIOException.class, () -> io.fileIdentity(channel));
                assertTrue(failure.getCause() instanceof LinkageError);
                assertTrue(channel.isOpen());
            }
            System.out.println("FILE_ID_OLD_JNI_REJECTED");
        }
    }

    /** A real wrapper object must be rejected before any attempt to delegate or unwrap it. */
    private static final class Wrapper extends FileChannel {
        private final FileChannel original;

        private Wrapper(FileChannel original) {
            this.original = original;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            original.close();
        }
        @Override
        public int read(ByteBuffer buffer) {
            throw new AssertionError();
        }
        @Override
        public long read(ByteBuffer[] buffers, int offset, int length) {
            throw new AssertionError();
        }
        @Override
        public int write(ByteBuffer buffer) {
            throw new AssertionError();
        }
        @Override
        public long write(ByteBuffer[] buffers, int offset, int length) {
            throw new AssertionError();
        }
        @Override
        public long position() {
            throw new AssertionError();
        }
        @Override
        public FileChannel position(long position) {
            throw new AssertionError();
        }
        @Override
        public long size() {
            throw new AssertionError();
        }
        @Override
        public FileChannel truncate(long size) {
            throw new AssertionError();
        }
        @Override
        public void force(boolean metadata) {
            throw new AssertionError();
        }
        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            throw new AssertionError();
        }
        @Override
        public long transferFrom(ReadableByteChannel source, long position, long count) {
            throw new AssertionError();
        }
        @Override
        public int read(ByteBuffer buffer, long position) {
            throw new AssertionError();
        }
        @Override
        public int write(ByteBuffer buffer, long position) {
            throw new AssertionError();
        }
        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            throw new AssertionError();
        }
        @Override
        public FileLock lock(long position, long size, boolean shared) {
            throw new AssertionError();
        }
        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            throw new AssertionError();
        }
    }
}

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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentOwnedFilesTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void originalOwnedWrapperQueriesActualFdAndRejectsForeignRawAndClosedBeforeTheOperator() throws Exception {
        Path path = temporary.newFile().toPath();
        Files.write(path, new byte[]{1, 2, 3});
        var files = new SegmentOwnedFiles();
        var other = new SegmentOwnedFiles();
        NativeIO io = new NativeIOImpl();
        NativeIO forbidden = mock(NativeIO.class, call -> {
            throw new AssertionError("Invalid wrapper reached the native operator");
        });
        try (FileChannel owned = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ);
             FileChannel foreign = SegmentOwnedFiles.open(other, path, StandardOpenOption.READ);
             FileChannel raw = FileChannel.open(path, StandardOpenOption.READ)) {
            owned.position(1);
            var id = files.fileIdentity(owned, io);
            assertEquals(((Number) Files.getAttribute(path, "unix:dev")).longValue(), id.device());
            assertEquals(((Number) Files.getAttribute(path, "unix:ino")).longValue(), id.inode());
            assertEquals(1, owned.position());
            ByteBuffer value = ByteBuffer.allocate(1);
            assertEquals(1, owned.read(value));
            assertEquals(2, value.get(0));
            assertTrue(owned.isOpen());
            assertThrows(IOException.class, () -> files.fileIdentity(foreign, forbidden));
            assertThrows(IOException.class, () -> files.fileIdentity(raw, forbidden));
            assertThrows(IOException.class, () -> files.fileIdentity(null, forbidden));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            assertThrows(IOException.class, () -> files.linuxExt4Mount(foreign, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxExt4Mount(raw, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxCgroupEvents(foreign, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxCgroupEvents(raw, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxStorageMember(foreign, path, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxStorageMember(raw, path, forbidden, deadline));
            var expired = assertThrows(IOException.class,
                    () -> files.linuxExt4Mount(owned, forbidden, System.nanoTime()));
            assertTrue(expired.getMessage().contains("deadline"));
            var expiredMember = assertThrows(IOException.class,
                    () -> files.linuxStorageMember(owned, path, forbidden, System.nanoTime()));
            assertTrue(expiredMember.getMessage().contains("deadline"));
            var expiredEvents = assertThrows(IOException.class,
                    () -> files.linuxCgroupEvents(owned, forbidden, System.nanoTime()));
            assertTrue(expiredEvents.getMessage().contains("deadline"));
            assertThrows(NativeIOException.class, () -> files.linuxCgroupEvents(owned, io, deadline));
            assertEquals(id, files.fileIdentity(owned, io));
            owned.close();
            assertThrows(IOException.class, () -> files.fileIdentity(owned, forbidden));
            assertThrows(IOException.class, () -> files.linuxExt4Mount(owned, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxStorageMember(owned, path, forbidden, deadline));
            assertThrows(IOException.class, () -> files.linuxCgroupEvents(owned, forbidden, deadline));
        } finally {
            assertTrue(files.drain().drained());
            assertTrue(other.drain().drained());
        }
    }

    @Test
    public void heldDirectoryListingRejectsTheFileKeyOfAReplacement() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path root = Files.createDirectory(parent.resolve("retained"));
        Path file = Files.createFile(root.resolve("original"));
        var before = SegmentClassicHandoffPaths.fileIdentity(root);
        Object originalKey = SegmentClassicHandoffPaths.retainedDirectoryKey(root, before);
        boolean secure;
        try (var providerListing = Files.newDirectoryStream(root)) {
            secure = providerListing instanceof SecureDirectoryStream<Path>;
        }
        var files = new SegmentOwnedFiles();
        try (var listing = files.openDirectory(root)) {
            if (!secure) {
                assertFalse(SystemUtils.IS_OS_LINUX);
                var failure = assertThrows(IOException.class, () -> listing.requireFileKey(originalKey));
                assertTrue(failure.getMessage().contains("held secure directory"));
                assertEquals(file.getFileName(), listing.iterator().next().getFileName());
                return;
            }
            listing.requireFileKey(originalKey);
            Files.move(root, parent.resolve("parked"));
            Files.createDirectory(root);
            var replacement = Files.readAttributes(root, BasicFileAttributes.class).fileKey();
            assertThrows(IOException.class, () -> SegmentClassicHandoffPaths.retainedDirectoryKey(root, before));
            assertThrows(IOException.class, () -> listing.requireFileKey(replacement));
            listing.requireFileKey(originalKey);
            assertEquals(file.getFileName(), listing.iterator().next().getFileName());
        } finally {
            assertTrue(files.drain().drained());
        }
    }

    @Test
    public void ownedLinuxMemberDiscardsTheActualNativeResultAfterTheDeadline() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        Path root = temporary.getRoot().toPath().toRealPath();
        Path member = Files.createDirectory(root.resolve("current"));
        var files = new SegmentOwnedFiles();
        NativeIO io = spy(new NativeIOImpl());
        var deadline = new AtomicLong();
        var actual = new AtomicReference<NativeIO.LinuxStorageMember>();
        doAnswer(call -> {
            var result = (NativeIO.LinuxStorageMember) call.callRealMethod();
            actual.set(result);
            long remaining = deadline.get() - System.nanoTime();
            if (remaining > 0) {
                assertFalse(new CountDownLatch(1).await(remaining, TimeUnit.NANOSECONDS));
            }
            return result;
        }).when(io).linuxStorageMember(any(FileChannel.class), eq("current"));
        try (var held = SegmentOwnedFiles.open(files, root, StandardOpenOption.READ)) {
            deadline.set(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
            var failure = assertThrows(IOException.class,
                    () -> files.linuxStorageMember(held, member, io, deadline.get()));
            assertTrue(failure.getMessage().contains("deadline"));
            assertNotNull(actual.get());
            assertTrue(held.isOpen());
        } finally {
            assertTrue(files.drain().drained());
        }
    }

    @Test
    public void ownedCgroupEventsDoesNotPublishAnActualLateKernelSample() throws Exception {
        String fixture = System.getProperty("bookkeeper.test.cgroup.events.directory");
        assumeTrue(SystemUtils.IS_OS_LINUX && fixture != null);
        var files = new SegmentOwnedFiles();
        NativeIO io = spy(new NativeIOImpl());
        var deadline = new AtomicLong();
        var actual = new AtomicReference<NativeIO.LinuxCgroupEvents>();
        doAnswer(call -> {
            var result = (NativeIO.LinuxCgroupEvents) call.callRealMethod();
            actual.set(result);
            long remaining = deadline.get() - System.nanoTime();
            if (remaining > 0) {
                assertFalse(new CountDownLatch(1).await(remaining, TimeUnit.NANOSECONDS));
            }
            return result;
        }).when(io).linuxCgroupEvents(any(FileChannel.class));
        try (var held = SegmentOwnedFiles.open(files, Path.of(fixture), StandardOpenOption.READ)) {
            deadline.set(System.nanoTime() + TimeUnit.SECONDS.toNanos(3));
            var failure = assertThrows(IOException.class, () -> files.linuxCgroupEvents(held, io, deadline.get()));
            assertTrue(failure.getMessage().contains("deadline"));
            assertNotNull(actual.get());
            assertEquals(actual.get().directory(), files.fileIdentity(held, io));
            assertTrue(held.isOpen());
        } finally {
            assertTrue(files.drain().drained());
        }
    }

    @Test
    public void actualIdentityBodyBlocksNativeCloseAndRejectsSelfDrainAndLatePublication() throws Exception {
        Path path = temporary.newFile().toPath();
        var files = new SegmentOwnedFiles();
        FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ);
        NativeIO io = spy(new NativeIOImpl());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var raw = new AtomicReference<FileChannel>();
        var kernel = new AtomicReference<NativeIO.FileIdentity>();
        doAnswer(call -> {
            raw.set(call.getArgument(0));
            assertThrows(IllegalStateException.class, files::drain);
            assertThrows(IllegalStateException.class, () -> files.fileIdentity(channel, io));
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            var actual = (NativeIO.FileIdentity) call.callRealMethod();
            kernel.set(actual);
            return actual;
        }).when(io).fileIdentity(any(FileChannel.class));
        var query = new FutureTask<>(() -> files.fileIdentity(channel, io));
        var drain = new FutureTask<>(files::drain);
        Thread reader = new Thread(query);
        Thread closer = new Thread(drain);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(0, files.result().opening());
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(drain.isDone());
            assertFalse(files.result().drained());
            assertFalse(files.result().retained().get(0).closeStarted());
            assertTrue(raw.get().isOpen());
            release.countDown();
            var failed = assertThrows(ExecutionException.class, () -> query.get(5, TimeUnit.SECONDS));
            assertTrue(failed.getCause() instanceof IOException);
            assertTrue(drain.get(5, TimeUnit.SECONDS).drained());
            assertNotNull(kernel.get()); // The delayed operator still made the real JNI call, never a fake id.
            assertEquals(((Number) Files.getAttribute(path, "unix:ino")).longValue(), kernel.get().inode());
            assertFalse(raw.get().isOpen());
            assertEquals(List.of(), files.result().retained());
        } finally {
            release.countDown();
            reader.join(5000);
            closer.join(5000);
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
            assertTrue(files.drain().drained());
        }
    }

    @Test(timeout = 30000)
    public void actualDirectoryQueryHoldsOriginalFdUntilDrainAndRejectsForeignWrappers() throws Exception {
        Path path = temporary.newFolder().toPath();
        var files = new SegmentOwnedFiles();
        var other = new SegmentOwnedFiles();
        FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        NativeIO io = spy(new NativeIOImpl());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var raw = new AtomicReference<FileChannel>();
        var observed = new AtomicReference<NativeIO.DirectoryFileSystem>();
        doAnswer(call -> {
            raw.set(call.getArgument(0));
            assertThrows(IllegalStateException.class, files::drain);
            assertThrows(IllegalStateException.class, () -> files.directoryFileSystem(channel, io));
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            var actual = (NativeIO.DirectoryFileSystem) call.callRealMethod();
            observed.set(actual);
            return actual;
        }).when(io).directoryFileSystem(any(FileChannel.class));
        var query = new FutureTask<>(() -> files.directoryFileSystem(channel, io));
        var drain = new FutureTask<>(files::drain);
        Thread reader = new Thread(query);
        Thread closer = new Thread(drain);
        try (FileChannel foreign = SegmentOwnedFiles.open(other, path, StandardOpenOption.READ);
             FileChannel direct = FileChannel.open(path, StandardOpenOption.READ)) {
            NativeIO forbidden = mock(NativeIO.class, call -> {
                throw new AssertionError("Invalid directory wrapper reached the native operator");
            });
            assertThrows(IOException.class, () -> files.directoryFileSystem(foreign, forbidden));
            assertThrows(IOException.class, () -> files.directoryFileSystem(direct, forbidden));
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(drain.isDone());
            assertTrue(raw.get().isOpen());
            release.countDown();
            var failed = assertThrows(ExecutionException.class, () -> query.get(5, TimeUnit.SECONDS));
            assertTrue(failed.getCause() instanceof IOException);
            assertTrue(drain.get(5, TimeUnit.SECONDS).drained());
            assertNotNull(observed.get());
            long inode = ((Number) Files.getAttribute(path, "unix:ino")).longValue();
            assertEquals(inode, observed.get().identity().inode());
            assertFalse(raw.get().isOpen());
            assertThrows(IOException.class, () -> files.directoryFileSystem(channel, forbidden));
        } finally {
            release.countDown();
            reader.join(5000);
            closer.join(5000);
            assertTrue(files.drain().drained());
            assertTrue(other.drain().drained());
        }
    }

    @Test
    public void failedMembershipObservationLeavesTheOriginalDirectoryWithItsOwnerUntilDrain() throws Exception {
        Path path = temporary.newFolder().toPath();
        var files = new SegmentOwnedFiles();
        var channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        var io = new NativeIOImpl();
        try (var raw = FileChannel.open(path, StandardOpenOption.READ)) {
            assertThrows(IOException.class, () -> files.linuxCgroupMembership(raw, io));
            var original = files.directoryFileSystem(channel, io);
            assertThrows(IOException.class, () -> files.linuxCgroupMembership(channel, io));
            assertTrue(channel.isOpen());
            assertEquals(original, files.directoryFileSystem(channel, io));
            assertEquals(1, files.result().retained().size());
        } finally {
            assertTrue(files.drain().drained());
        }
        assertFalse(channel.isOpen());
        assertThrows(IOException.class, () -> files.linuxCgroupMembership(channel, io));
    }

    @Test
    public void atomicNewWritesWorkButReadOnlyCreateNewStillRejectsProtectedInodes() throws Exception {
        Path root = temporary.newFolder().toPath().toRealPath();
        Path path = Files.createFile(root.resolve("guard"));
        var guard = SegmentDataFileLock.existingControlOwner(path);
        guard.initialize();
        var files = new SegmentOwnedFiles(java.util.List.of(path));
        try {
            for (boolean setOptions : new boolean[]{false, true}) {
                Path fresh = root.resolve("new-" + setOptions);
                try (FileChannel channel = setOptions
                        ? SegmentOwnedFiles.open(files, fresh,
                                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
                        : SegmentOwnedFiles.open(files, fresh,
                                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    channel.write(ByteBuffer.wrap(new byte[]{7}));
                }
                assertArrayEquals(new byte[]{7}, Files.readAllBytes(fresh));
            }
            assertThrows(IOException.class, () -> SegmentOwnedFiles.open(files, path,
                    StandardOpenOption.READ, StandardOpenOption.CREATE_NEW));
            assertThrows(IOException.class, () -> SegmentOwnedFiles.open(files, path,
                    Set.of(StandardOpenOption.READ, StandardOpenOption.CREATE_NEW)));
            assertThrows(IOException.class, () -> SegmentOwnedFiles.open(files, path,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            assertTrue(guard.lock().isValid());
            assertTrue(files.drain().drained());
            var probe = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"),
                    SegmentOwnedBookieControlLogTest.GuardProbe.class.getName(), path.toString())
                    .redirectErrorStream(true).start();
            try {
                assertTrue(probe.waitFor(10, TimeUnit.SECONDS));
                String output = new String(probe.getInputStream().readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8).trim();
                assertEquals(output, 0, probe.exitValue());
                assertEquals("HELD", output);
            } finally {
                if (probe.isAlive()) {
                    probe.destroyForcibly();
                    assertTrue(probe.waitFor(5, TimeUnit.SECONDS));
                }
            }
        } finally {
            files.drain();
            guard.close();
        }
    }

    @Test
    public void successfulTemporaryFilesRetireImmediatelyAndOldAliasesStayClosed() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("file"));
        var files = new SegmentOwnedFiles();
        for (int value = 0; value < 32; value++) {
            FileChannel channel = SegmentOwnedFiles.open(files, path,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            assertSame(channel, channel.position(0));
            channel.write(ByteBuffer.allocate(8).putLong(value).flip(), 0);
            channel.force(true);
            channel.close();
            channel.close();
            assertThrows(IOException.class, channel::size);
            assertTrue(files.result().retained().isEmpty());
        }
        assertEquals(31, ByteBuffer.wrap(Files.readAllBytes(path)).getLong());
        assertTrue(files.drain().drained());
        assertThrows(IOException.class, () -> SegmentOwnedFiles.open(files, path, StandardOpenOption.READ));
    }

    @Test(timeout = 30000)
    public void interruptedDrainWaiterDoesNotCancelTheActuallyOpeningThreadOrLoseItsLateFile() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("file"));
        var files = new SegmentOwnedFiles();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var opened = new FutureTask<FileChannel>(() -> {
            try (var factory = mockStatic(FileChannel.class, invocation -> {
                if (invocation.getMethod().getName().equals("open")
                        && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                        && path.equals(invocation.getArgument(0))) {
                    assertEquals(1, files.result().opening());
                    assertThrows(IllegalStateException.class, files::drain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                    assertFalse(Thread.currentThread().isInterrupted());
                }
                return invocation.callRealMethod();
            })) {
                return SegmentOwnedFiles.open(files, path, StandardOpenOption.READ);
            }
        });
        Thread opener = new Thread(opened);
        var drained = new FutureTask<>(files::drain);
        Thread waiter = new Thread(drained);
        try {
            opener.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            waiter.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> !files.healthy()
                    && waiter.getState() == Thread.State.WAITING);
            waiter.interrupt();
            assertTrue(assertThrows(ExecutionException.class,
                    () -> drained.get(5, TimeUnit.SECONDS)).getCause() instanceof InterruptedException);
            assertEquals(1, files.result().opening());
            assertFalse(files.result().drained());
            release.countDown();
            FileChannel late = opened.get(5, TimeUnit.SECONDS);
            opener.join(5000);
            waiter.join(5000);
            assertTrue(late.isOpen());
            assertTrue(files.drain().drained());
            assertFalse(late.isOpen());
        } finally {
            release.countDown();
            opener.join(5000);
            if (waiter.getState() != Thread.State.NEW) {
                waiter.join(5000);
            }
            assertFalse(opener.isAlive());
            assertFalse(waiter.isAlive());
            assertTrue(files.drain().drained());
        }
    }

    @Test(timeout = 30000)
    public void logicalCloseDoesNotSettleABlockedNativeCloseOrPermitSelfDrain() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("file"));
        var files = new SegmentOwnedFiles();
        FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ);
        var member = channel.getClass().getDeclaredField("entry");
        member.setAccessible(true);
        Object entry = member.get(channel);
        var raw = entry.getClass().getDeclaredField("resource");
        raw.setAccessible(true);
        var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) raw.get(entry));
        raw.set(entry, fault);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        fault.beforeClose = () -> {
            assertThrows(IllegalStateException.class, files::drain);
            entered.countDown();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        var closed = new FutureTask<Void>(() -> {
            channel.close();
            return null;
        });
        Thread closer = new Thread(closed);
        var drained = new FutureTask<>(files::drain);
        Thread drainer = new Thread(drained);
        try {
            closer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(channel.isOpen());
            assertTrue(fault.actual.isOpen());
            drainer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> drainer.getState() == Thread.State.BLOCKED);
            assertFalse(drained.isDone());
            assertEquals(1, fault.closes.get());
            release.countDown();
            closed.get(5, TimeUnit.SECONDS);
            closer.join(5000);
            assertTrue(drained.get(5, TimeUnit.SECONDS).drained());
            drainer.join(5000);
            assertTrue(files.drain().drained());
            assertFalse(fault.actual.isOpen());
            assertEquals(1, fault.closes.get());
        } finally {
            release.countDown();
            closer.join(5000);
            if (drainer.getState() != Thread.State.NEW) {
                drainer.join(5000);
            }
            assertFalse(closer.isAlive());
            assertFalse(drainer.isAlive());
            fault.actual.close();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void wrapperConstructionFailureKeepsTheAlreadyOpenedNativeFileInItsOriginalOwner() throws Exception {
        Path path = Files.createFile(temporary.newFolder().toPath().resolve("file"));
        var files = new SegmentOwnedFiles();
        var actual = new AtomicReference<FileChannel>();
        var failure = new OutOfMemoryError("tracked channel construction failed");
        Class wrapper = Arrays.stream(SegmentOwnedFiles.class.getDeclaredClasses())
                .filter(type -> type.getSimpleName().equals("Channel")).findFirst().orElseThrow();
        try {
            try (var factory = mockStatic(FileChannel.class, invocation -> {
                Object result = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open")
                        && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                        && path.equals(invocation.getArgument(0))) {
                    actual.set((FileChannel) result);
                }
                return result;
            }); var construction = mockConstruction(wrapper, (mock, context) -> {
                throw failure;
            })) {
                Throwable observed = assertThrows(Throwable.class,
                        () -> SegmentOwnedFiles.open(files, path, StandardOpenOption.READ));
                while (observed != failure && observed.getCause() != null) {
                    observed = observed.getCause();
                }
                assertSame(failure, observed);
            }
            assertNotNull(actual.get());
            assertTrue(actual.get().isOpen());
            assertEquals(1, files.result().retained().size());
            assertTrue(files.drain().drained());
            assertFalse(actual.get().isOpen());
        } finally {
            if (actual.get() != null) {
                actual.get().close();
            }
        }
    }
}

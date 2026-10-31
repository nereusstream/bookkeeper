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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentOwnedFilesTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

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

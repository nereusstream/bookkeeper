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
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentOwnerLoopTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private static final Duration WAIT = Duration.ofSeconds(5);

    private static SegmentOwnerLoop.Command command(Consumer<SegmentOwnerLoop.Ticket> action,
                                                    Consumer<Throwable> failure) {
        return new SegmentOwnerLoop.Command() {
            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                action.accept(ticket);
            }

            @Override
            public void failed(Throwable cause) {
                failure.accept(cause);
            }
        };
    }

    @Test
    public void resumeRetainsDataCapacityAcrossShutdownAndContinuationFailure() throws Exception {
        for (boolean fail : new boolean[] {false, true}) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 1, 1);
            AtomicReference<SegmentOwnerLoop.Ticket> waiting = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch ready = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            assertTrue(owner.submit(0, command(ticket -> {
                waiting.set(ticket);
                ready.countDown();
            }, cause -> {
                failure.set(cause);
                done.countDown();
            })));
            owner.start();
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            assertFalse(owner.submit(0, command(ticket -> ticket.complete(() -> { }), cause -> { })));
            owner.stopAdmission();
            assertFalse(owner.awaitTermination(Duration.ofMillis(20)));
            waiting.get().resume(() -> {
                owner.requireOwner();
                if (fail) {
                    throw new IllegalStateException("Continuation failed");
                }
                waiting.get().complete(done::countDown);
            });
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertTrue(owner.awaitTermination(WAIT));
            if (fail) {
                assertTrue(failure.get() instanceof IllegalStateException);
                assertSame(failure.get(), owner.failure());
            } else {
                assertNull(failure.get());
                assertNull(owner.failure());
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue("Timed out waiting for deterministic boundary", latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static Consumer<Throwable> unexpected() {
        return cause -> {
            throw new AssertionError(cause);
        };
    }

    private static void stop(SegmentOwnerLoop owner) throws Exception {
        owner.stopAdmission();
        assertTrue(owner.awaitTermination(WAIT));
    }

    @Test
    public void roundRobinAndReservedCompletionsSurviveFullDataAdmission() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 2, 0, 4, 1);
        List<Integer> visited = Collections.synchronizedList(new ArrayList<>());
        List<SegmentOwnerLoop.Ticket> held = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch dispatched = new CountDownLatch(4);
        CountDownLatch completed = new CountDownLatch(4);
        AtomicInteger controls = new AtomicInteger();
        try {
            for (int shard : new int[] {0, 0, 0, 2}) {
                assertTrue(owner.submit(shard, command(ticket -> {
                    owner.requireOwner();
                    visited.add(shard);
                    held.add(ticket);
                    dispatched.countDown();
                }, unexpected())));
            }
            assertFalse(owner.submit(0, command(ticket -> {}, unexpected())));
            assertTrue(owner.control(controls::incrementAndGet));
            assertFalse(owner.control(controls::incrementAndGet));
            owner.start();
            await(dispatched);
            assertEquals(List.of(0, 2, 0, 0), visited);
            assertEquals(1, controls.get());
            // Four admitted batches hold all credits, but all four may post completion without waiting for DATA.
            for (SegmentOwnerLoop.Ticket ticket : held) {
                ticket.complete(() -> {
                    owner.requireOwner();
                    completed.countDown();
                });
            }
            await(completed);
            assertThrows(IllegalStateException.class, () -> held.get(0).complete(() -> {}));
        } finally {
            stop(owner);
        }
        assertNull(owner.failure());
        assertEquals(0, owner.pending());
    }

    @Test
    public void oldestDeadlineFiresWithoutNewInputAndCannotBePostponed() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(2, 1, 0, 2, 1);
        CountDownLatch fired = new CountDownLatch(1);
        AtomicInteger wrong = new AtomicInteger();
        try {
            owner.start();
            assertTrue(owner.submit(0, command(ticket -> {
                owner.deadline(0, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(20),
                        () -> ticket.complete(fired::countDown));
                owner.deadline(0, System.nanoTime() + TimeUnit.SECONDS.toNanos(30), wrong::incrementAndGet);
            }, unexpected())));
            assertTrue(owner.submit(1, command(ticket -> ticket.complete(() -> {}), unexpected())));
            await(fired);
            assertEquals(0, wrong.get());
        } finally {
            stop(owner);
        }
        assertNull(owner.failure());
    }

    @Test
    public void ioIsBoundedAndReturnsToOwnerWhileStopRetainsUnknownWork() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 3, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1);
        CountDownLatch ioStarted = new CountDownLatch(1);
        CountDownLatch releaseIo = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);
        CountDownLatch control = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Thread> ownerThread = new AtomicReference<>();
        AtomicReference<Throwable> problem = new AtomicReference<>();
        try (FileChannel file = FileChannel.open(temporary.newFile().toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            for (int i = 0; i < 3; i++) {
                int request = i;
                assertTrue(owner.submit(0, command(ticket -> {
                    ownerThread.set(Thread.currentThread());
                    io.submit(ticket, () -> {
                        assertFalse(Thread.currentThread() == ownerThread.get());
                        if (request == 0) {
                            ioStarted.countDown();
                            await(releaseIo);
                            SegmentFileIo backend = new SegmentFileIo();
                            backend.writeFully(file, ByteBuffer.wrap(new byte[] {1, 2, 3}), 0);
                            backend.force(file, false);
                        } else {
                            throw new IOException("Injected I/O failure");
                        }
                    }, failure -> {
                        owner.requireOwner();
                        if (request == 2) {
                            assertTrue(failure instanceof RejectedExecutionException);
                            rejected.countDown();
                        } else {
                            if (request == 0 && failure != null) {
                                problem.set(failure);
                            } else if (request == 1) {
                                assertTrue(failure instanceof IOException);
                            }
                            done.countDown();
                        }
                    });
                    // Ensure the first task is executing before trying the single queue slot.
                    if (request == 0) {
                        await(ioStarted);
                    }
                }, unexpected())));
            }
            owner.start();
            await(rejected);
            assertTrue(owner.control(control::countDown));
            await(control);
            owner.stopAdmission();
            io.shutdown();
            assertFalse(owner.awaitTermination(Duration.ofMillis(20)));
            assertEquals(2, owner.pending());
            releaseIo.countDown();
            await(done);
            assertTrue(io.awaitTermination(WAIT));
            assertTrue(owner.awaitTermination(WAIT));
            assertEquals(3, file.size());
            assertNull(problem.get());
            assertNull(owner.failure());
        } finally {
            releaseIo.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(WAIT));
            stop(owner);
        }
    }

    @Test
    public void handlerFailureAfterSubmissionDoesNotReleaseOutstandingIo() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 1, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch failed = new CountDownLatch(1);
        RuntimeException cause = new IllegalStateException("after submission");
        AtomicInteger success = new AtomicInteger();
        try {
            assertTrue(owner.submit(0, command(ticket -> {
                io.submit(ticket, () -> {
                    entered.countDown();
                    await(release);
                }, ignored -> success.incrementAndGet());
                throw cause;
            }, actual -> {
                assertSame(cause, actual);
                failed.countDown();
            })));
            owner.start();
            await(entered);
            owner.stopAdmission();
            assertFalse(owner.awaitTermination(Duration.ofMillis(20)));
            assertEquals(1, owner.pending());
            release.countDown();
            await(failed);
        } finally {
            release.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(WAIT));
            stop(owner);
        }
        assertSame(cause, owner.failure());
        assertEquals(0, success.get());
        assertEquals(0, owner.pending());
    }

    @Test
    public void failedOwnerRejectsQueuedCommandsAndReportsEachObligation() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 2, 1);
        RuntimeException cause = new IllegalStateException("failed owner");
        CountDownLatch failed = new CountDownLatch(2);
        AtomicInteger executed = new AtomicInteger();
        try {
            assertTrue(owner.submit(0, command(ticket -> {
                throw cause;
            }, actual -> {
                assertSame(cause, actual);
                failed.countDown();
            })));
            assertTrue(owner.submit(0, command(ticket -> executed.incrementAndGet(), actual -> {
                assertSame(cause, actual);
                failed.countDown();
            })));
            owner.start();
            await(failed);
            assertFalse(owner.submit(0, command(ticket -> {}, unexpected())));
        } finally {
            stop(owner);
        }
        assertSame(cause, owner.failure());
        assertEquals(0, executed.get());
        assertEquals(0, owner.pending());
    }

    @Test
    public void placementCheckFailureStillDrainsSubmittedIoAndQueuedData() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 2, 1);
        SegmentIoExecutor io = new SegmentIoExecutor(1, 2);
        RuntimeException cause = new IllegalStateException("placement check failed");
        CountDownLatch checked = new CountDownLatch(1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        CountDownLatch rejected = new CountDownLatch(1);
        try {
            assertTrue(owner.control(() -> owner.setPlacementCheck(() -> {
                checked.countDown();
                throw cause;
            })));
            assertTrue(owner.submit(0, command(ticket -> io.submit(ticket, () -> {
                entered.countDown();
                await(release);
            }, failure -> {
                assertNull(failure);
                completed.countDown();
            }), unexpected())));
            assertTrue(owner.submit(0, command(ticket -> {
                throw new AssertionError("Queued DATA ran after placement failure");
            }, failure -> {
                assertSame(cause, failure);
                rejected.countDown();
            })));
            owner.start();
            await(entered);
            await(checked);
            assertSame(cause, owner.failure());
            assertFalse(owner.submit(0, command(ticket -> {}, unexpected())));
            await(rejected);
            release.countDown();
            await(completed);
        } finally {
            release.countDown();
            io.shutdown();
            assertTrue(io.awaitTermination(WAIT));
            stop(owner);
        }
        assertEquals(0, owner.pending());
    }

    @Test
    public void parkedOwnerWakesAndRoutingDoesNotDependOnWorkerCount() throws Exception {
        SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 3, 2, 1, 1);
        AtomicReference<Thread> ownerThread = new AtomicReference<>();
        try {
            assertEquals(2, SegmentOwnerLoop.shardFor(14, 4));
            assertThrows(IllegalArgumentException.class,
                    () -> owner.submit(0, command(ticket -> {}, unexpected())));
            assertThrows(IllegalStateException.class, owner::requireOwner);
            owner.start();
            // Repeated empty-to-nonempty transitions target the enqueue/park race, not throughput.
            for (int i = 0; i < 100; i++) {
                CountDownLatch done = new CountDownLatch(1);
                assertTrue(owner.control(() -> {
                    Thread previous = ownerThread.getAndSet(Thread.currentThread());
                    if (previous != null) {
                        assertSame(previous, Thread.currentThread());
                    }
                    done.countDown();
                }));
                await(done);
                long end = System.nanoTime() + WAIT.toNanos();
                while (owner.controlPending() != 0 && System.nanoTime() < end) {
                    Thread.onSpinWait();
                }
                assertEquals(0, owner.controlPending());
            }
        } finally {
            stop(owner);
        }
        assertNull(owner.failure());
    }
}

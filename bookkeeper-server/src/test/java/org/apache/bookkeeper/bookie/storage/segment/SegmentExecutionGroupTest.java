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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentExecutionGroupTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private static final Duration WAIT = Duration.ofSeconds(5);

    private IsolatedSegmentConfig config(String affinity, String policy) {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("arena"), 1, 4, 2,
                16 * 1024 * 1024, 4096, 1024 * 1024, 5, "FILE_CHANNEL", affinity, policy, "PARK");
    }

    private SegmentExecutionGroup.Settings settings() {
        return new SegmentExecutionGroup.Settings(2, 1, 2, 3, 4096, 3 * 8191, WAIT);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static SegmentOwnerLoop.Command command(Consumer<SegmentOwnerLoop.Ticket> work) {
        return new SegmentOwnerLoop.Command() {
            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                work.accept(ticket);
            }

            @Override
            public void failed(Throwable failure) {
                throw new AssertionError(failure);
            }
        };
    }

    @Test
    public void allOwnerPoolsAreReadyAndGlobalBudgetIsPartitionedOnce() throws Exception {
        List<Thread> initialized = new CopyOnWriteArrayList<>();
        AtomicInteger count = new AtomicInteger();
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("OFF", "OWNER_TOUCH"), settings(),
                (owner, buffers, bytes, touch, wake) -> {
                    initialized.add(Thread.currentThread());
                    count.addAndGet(buffers);
                    assertTrue(touch);
                    return new SegmentBufferPool(owner, 1, buffers, bytes, 4096, touch, wake);
                });
        try {
            assertEquals(2, initialized.size());
            assertEquals(3, count.get());
            assertEquals(3 * 8191, group.reservedBufferBytes());
            assertEquals(3, group.actualMasks().size());
            CountDownLatch checked = new CountDownLatch(2);
            for (int shard : new int[] {0, 1}) {
                assertTrue(group.submit(shard, command(ticket -> {
                    assertTrue(initialized.contains(Thread.currentThread()));
                    SegmentBufferPool pool = group.owner(shard).pool();
                    SegmentBufferPool.Lease lease = pool.acquire();
                    assertNotNull(lease);
                    assertEquals(shard, lease.ownerId());
                    assertEquals(1, lease.generation());
                    ByteBuffer bytes = lease.bytes();
                    assertEquals(4096, bytes.remaining());
                    assertEquals(0, bytes.alignmentOffset(0, 4096));
                    group.io().submit(ticket, () -> {
                        bytes.putLong(0, 17);
                        lease.close();
                    }, failure -> {
                        assertNull(failure);
                        checked.countDown();
                    });
                })));
            }
            await(checked);
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
        for (Thread thread : initialized) {
            assertFalse(thread.isAlive());
        }
    }

    @Test
    public void crossThreadReturnWakesDrainingOwnerWithoutReleasingLiveBuffer() throws Exception {
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("OFF", "SYSTEM"), settings());
        AtomicReference<SegmentBufferPool.Lease> lease = new AtomicReference<>();
        CountDownLatch borrowed = new CountDownLatch(1);
        try {
            assertTrue(group.submit(0, command(ticket -> {
                lease.set(group.owner(0).pool().acquire());
                ticket.complete(borrowed::countDown);
            })));
            await(borrowed);
            assertFalse(group.shutdown(Duration.ofMillis(20)));
            assertEquals(4096, lease.get().bytes().remaining());
            lease.get().close();
            assertThrows(IllegalStateException.class, () -> lease.get().close());
            assertThrows(IllegalStateException.class, () -> lease.get().bytes());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void partialPoolInitializationFailureTerminatesAllStartedScopes() throws Exception {
        List<Thread> initialized = new CopyOnWriteArrayList<>();
        CountDownLatch first = new CountDownLatch(1);
        IOException failure = assertThrows(IOException.class,
                () -> SegmentExecutionGroup.start(config(Boolean.getBoolean("uw.requireBinding") ? "REQUIRE" : "OFF",
                                "SYSTEM"), settings(),
                        (owner, count, bytes, touch, wake) -> {
                            initialized.add(Thread.currentThread());
                            if (owner == 1) {
                                await(first);
                                throw new IllegalStateException("Injected pool initialization failure");
                            }
                            SegmentBufferPool pool = new SegmentBufferPool(owner, 1, count, bytes, 4096, touch, wake);
                            first.countDown();
                            return pool;
                        }));
        assertTrue(failure.getMessage().contains("initialization failed"));
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(2, initialized.size());
        for (Thread thread : initialized) {
            assertFalse(thread.isAlive());
        }
        // A later independent startup is possible; no failed group is published to the caller.
        SegmentExecutionGroup retry = SegmentExecutionGroup.start(config("OFF", "SYSTEM"), settings());
        assertTrue(retry.shutdown(WAIT));
    }

    @Test
    public void fullDataAdmissionDoesNotConsumeBufferReturnCapacity() throws Exception {
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("OFF", "SYSTEM"), settings());
        List<SegmentOwnerLoop.Ticket> tickets = new CopyOnWriteArrayList<>();
        List<SegmentBufferPool.Lease> leases = new CopyOnWriteArrayList<>();
        CountDownLatch active = new CountDownLatch(3);
        CountDownLatch reclaimed = new CountDownLatch(1);
        try {
            for (int i = 0; i < 3; i++) {
                int entry = i;
                assertTrue(group.submit(0, command(ticket -> {
                    tickets.add(ticket);
                    if (entry == 0) {
                        leases.add(group.owner(0).pool().acquire());
                        leases.add(group.owner(0).pool().acquire());
                    }
                    active.countDown();
                })));
            }
            await(active);
            assertFalse(group.submit(0, command(ticket -> {})));
            for (SegmentBufferPool.Lease lease : leases) {
                lease.close();
            }
            assertTrue(group.owner(0).control(() -> {
                SegmentBufferPool pool = group.owner(0).pool();
                pool.drainReturns();
                assertEquals(0, pool.outstanding());
                reclaimed.countDown();
            }));
            await(reclaimed);
        } finally {
            for (SegmentOwnerLoop.Ticket ticket : tickets) {
                ticket.complete(() -> {});
            }
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void linuxRequireBindsBeforePoolInitializationAndKeepsStartupThreadUnchanged() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        Assume.assumeTrue("Requires the declared 3-CPU container fixture", Boolean.getBoolean("uw.requireBinding"));
        java.util.BitSet original = org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask();
        List<java.util.BitSet> poolMasks = new CopyOnWriteArrayList<>();
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("REQUIRE", "OWNER_TOUCH"), settings(),
                (owner, count, bytes, touch, wake) -> {
                    try {
                        poolMasks.add(org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask());
                    } catch (IOException failure) {
                        throw new IllegalStateException(failure);
                    }
                    return new SegmentBufferPool(owner, 1, count, bytes, 4096, touch, wake);
                });
        try {
            assertNull(group.degradation());
            assertEquals(2, poolMasks.size());
            assertTrue(poolMasks.stream().allMatch(mask -> mask.cardinality() == 1));
            List<java.util.BitSet> masks = group.actualMasks();
            assertEquals(3, masks.size());
            for (int i = 0; i < masks.size(); i++) {
                assertFalse(masks.get(i).isEmpty());
                for (int j = i + 1; j < masks.size(); j++) {
                    assertFalse(masks.get(i).intersects(masks.get(j)));
                }
            }
            assertEquals(original, org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
        assertEquals(original, org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask());
    }

    @Test
    public void unavailableAffinityPrefersFallbackButRequireFailsBeforeOpening() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").equals("Linux"));
        SegmentExecutionGroup preferred = SegmentExecutionGroup.start(config("PREFER", "SYSTEM"), settings());
        try {
            assertNotNull(preferred.degradation());
        } finally {
            assertTrue(preferred.shutdown(WAIT));
        }
        assertThrows(IOException.class, () -> SegmentExecutionGroup.start(config("REQUIRE", "SYSTEM"), settings()));
    }

    @Test
    public void bufferBudgetAlignmentAndPoolExhaustionAreObservable() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                new SegmentExecutionGroup.Settings(2, 1, 2, 3, 4096, 3 * 4096, WAIT));
        SegmentBufferPool pool = new SegmentBufferPool(0, 3, 1, 4096, 4096, false, () -> {});
        SegmentBufferPool.Lease first = pool.acquire();
        assertNull(pool.acquire());
        assertThrows(IllegalStateException.class, pool::close);
        first.close();
        SegmentBufferPool.Lease second = pool.acquire();
        assertNotNull(second);
        assertThrows(IllegalStateException.class, first::close);
        assertEquals(1, pool.outstanding());
        second.close();
        pool.close();
        assertEquals(0, pool.outstanding());
        assertThrows(IllegalStateException.class, pool::acquire);
    }
}

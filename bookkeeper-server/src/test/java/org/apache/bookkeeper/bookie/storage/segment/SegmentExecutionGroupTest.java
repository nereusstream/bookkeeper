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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.affinity.CpuBinding;
import org.apache.bookkeeper.common.util.affinity.CpuTopologySnapshot;
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
        return new SegmentExecutionGroup.Settings(2, 2, 2, 3, 4096, 4096, 3 * 8191 + 4096, WAIT);
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

    private static CpuTopologySnapshot lowerQuota(CpuTopologySnapshot source) {
        return new CpuTopologySnapshot(source.deploymentMask(), source.onlineCpus(), source.effectiveCpus(),
                source.memoryNodes(), OptionalDouble.of(1.0), source.cpus(), source.deviceNode(),
                source.limitations());
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
            assertEquals(4, group.actualMasks().size());
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
    public void terminalObserverRunsAfterActualIoAndMakesTheOriginalOwnerSlotAvailable() throws Exception {
        var config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("terminal-owner"), 1, 4, 2,
                16 * 1024 * 1024, 4096, 1024 * 1024, 1, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        var group = SegmentExecutionGroup.start(config,
                new SegmentExecutionGroup.Settings(1, 2, 2, 3, 4096, 4096, 3 * 8191 + 4096, WAIT));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var terminal = new CountDownLatch(1);
        var followup = new CountDownLatch(1);
        var acceptedAfterTerminal = new AtomicReference<Boolean>();
        var failure = new AtomicReference<Throwable>();
        try {
            assertTrue(group.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    group.io().submit(ticket, () -> {
                        entered.countDown();
                        await(release);
                    }, failure::set);
                }

                @Override
                public void failed(Throwable cause) {
                    failure.set(cause);
                }

                @Override
                public void terminal() {
                    acceptedAfterTerminal.set(group.submit(0,
                            command(ticket -> ticket.complete(followup::countDown))));
                    terminal.countDown();
                }
            }));
            await(entered);
            assertFalse(group.submit(0, command(ticket -> ticket.complete(() -> { }))));
            assertFalse(terminal.await(150, TimeUnit.MILLISECONDS));
            release.countDown();
            await(terminal);
            assertTrue(acceptedAfterTerminal.get());
            await(followup);
            assertNull(failure.get());
        } finally {
            release.countDown();
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void controlIoRunsWhenDataWorkerIsBlocked() throws Exception {
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("OFF", "SYSTEM"), settings());
        CountDownLatch dataStarted = new CountDownLatch(1);
        CountDownLatch releaseData = new CountDownLatch(1);
        CountDownLatch dataDone = new CountDownLatch(1);
        CountDownLatch controlDone = new CountDownLatch(1);
        AtomicReference<Thread> dataWorker = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            assertTrue(group.submit(0, command(ticket -> group.io().submit(ticket, () -> {
                dataWorker.set(Thread.currentThread());
                dataStarted.countDown();
                if (!releaseData.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("DATA I/O was not released");
                }
            }, cause -> {
                if (cause != null) {
                    failure.compareAndSet(null, cause);
                }
                dataDone.countDown();
            }))));
            await(dataStarted);
            assertTrue(group.submitControl(0, command(ticket -> group.io().submitControl(ticket, () -> {
                assertFalse(Thread.currentThread() == dataWorker.get());
            }, cause -> {
                group.owner(0).requireOwner();
                if (cause != null) {
                    failure.compareAndSet(null, cause);
                }
                controlDone.countDown();
            }))));
            await(controlDone);
            assertNull(failure.get());
            releaseData.countDown();
            await(dataDone);
            assertNull(failure.get());
        } finally {
            releaseData.countDown();
            assertTrue(group.shutdown(WAIT));
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
            assertEquals(4, masks.size());
            for (java.util.BitSet mask : masks) {
                assertFalse(mask.isEmpty());
            }
            assertFalse(masks.get(0).intersects(masks.get(1)));
            assertFalse(masks.get(0).intersects(masks.get(2)));
            assertFalse(masks.get(1).intersects(masks.get(2)));
            assertEquals(masks.get(2), masks.get(3));
            assertEquals(original, org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
        assertEquals(original, org.apache.bookkeeper.common.util.affinity.CpuBinding.currentThreadMask());
    }

    @Test
    public void linuxPreferRelaxesTheOriginalWorkersWhenQuotaDrops() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        Assume.assumeTrue("Requires the declared 3-CPU container fixture", Boolean.getBoolean("uw.requireBinding"));
        CpuTopologySnapshot baseline = CpuTopologySnapshot.capture(null);
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("PREFER", "SYSTEM"), settings());
        try {
            assertNull(group.degradation());
            group.observePlacement(lowerQuota(baseline));
            assertNotNull(group.degradation());
            List<java.util.BitSet> ownerMasks = new CopyOnWriteArrayList<>();
            List<java.util.BitSet> ioMasks = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> ioFailure = new AtomicReference<>();
            CountDownLatch finished = new CountDownLatch(2);
            for (int shard : new int[] {0, 1}) {
                assertTrue(group.submit(shard, command(ticket -> {
                    try {
                        ownerMasks.add(CpuBinding.currentThreadMask());
                        group.io().submit(ticket, () -> ioMasks.add(CpuBinding.currentThreadMask()), failure -> {
                            if (failure != null) {
                                ioFailure.compareAndSet(null, failure);
                            }
                            finished.countDown();
                        });
                    } catch (IOException failure) {
                        ticket.fail(failure);
                    }
                })));
            }
            await(finished);
            assertEquals(2, ownerMasks.size());
            assertEquals(2, ioMasks.size());
            assertTrue(ownerMasks.stream().allMatch(baseline.eligibleCpus()::equals));
            assertTrue(ioMasks.stream().allMatch(baseline.eligibleCpus()::equals));
            assertNull(ioFailure.get());
            assertNull(group.runtimeFailure());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void linuxRequireStopsAdmissionButDrainsAnAcceptedTicketAfterQuotaLoss() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        Assume.assumeTrue("Requires the declared 3-CPU container fixture", Boolean.getBoolean("uw.requireBinding"));
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("REQUIRE", "SYSTEM"), settings());
        AtomicReference<SegmentOwnerLoop.Ticket> accepted = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        try {
            assertTrue(group.submit(0, command(ticket -> {
                accepted.set(ticket);
                started.countDown();
            })));
            await(started);
            group.observePlacement(lowerQuota(CpuTopologySnapshot.capture(null)));
            assertNotNull(group.runtimeFailure());
            assertFalse(group.submit(0, command(ticket -> {})));
            assertFalse(group.shutdown(Duration.ofMillis(20)));
            accepted.get().complete(finished::countDown);
            await(finished);
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void linuxRequireDetectsAnOwnerMaskChangedAfterStartup() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        Assume.assumeTrue("Requires the declared 3-CPU container fixture", Boolean.getBoolean("uw.requireBinding"));
        java.util.BitSet eligible = CpuBinding.currentThreadMask();
        int otherCpu = eligible.nextSetBit(eligible.nextSetBit(0) + 1);
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("REQUIRE", "SYSTEM"), settings());
        CountDownLatch changed = new CountDownLatch(1);
        try {
            assertTrue(group.submit(0, command(ticket -> {
                try {
                    org.apache.bookkeeper.common.util.affinity.impl.CpuAffinityImpl
                            .setCurrentThreadMask(new int[] {otherCpu});
                    ticket.complete(changed::countDown);
                } catch (IOException failure) {
                    throw new IllegalStateException(failure);
                }
            })));
            await(changed);
            long end = System.nanoTime() + WAIT.toNanos();
            while (group.runtimeFailure() == null && System.nanoTime() < end) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertNotNull(group.runtimeFailure());
            assertFalse(group.submit(0, command(ticket -> {})));
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
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
    public void linuxPreferStartupFallsBackOffWhenQuotaCannotSupportTheBoundPlan() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        Assume.assumeTrue("Requires the declared 2-CPU-quota fixture", Boolean.getBoolean("uw.expectStartupFallback"));
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("PREFER", "SYSTEM"), settings());
        try {
            assertNotNull(group.degradation());
            assertEquals(4, group.actualMasks().size());
            CountDownLatch finished = new CountDownLatch(1);
            assertTrue(group.submit(0, command(ticket -> ticket.complete(finished::countDown))));
            await(finished);
            assertNull(group.runtimeFailure());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void entryInputsShareOneBudgetAndRetainWholeAllocationCharge() throws Exception {
        SegmentExecutionGroup group = SegmentExecutionGroup.start(config("OFF", "SYSTEM"), settings());
        try {
            assertSame(group.inputs(), group.owner(0).inputs());
            assertSame(group.inputs(), group.owner(1).inputs());
            byte[] instance = new byte[16];
            byte[] descriptor = new byte[36];
            instance[0] = 1;
            descriptor[0] = 1;
            ProfileLedgerContext context = new ProfileLedgerContext(0, instance, descriptor);
            SegmentEntryInputs.Input input = group.inputs().acquire(context, 2048);
            ByteBuf original = input.bytes();
            assertEquals(2048, group.inputs().usedBytes());
            assertNull(group.inputs().acquire(context, 2049));
            assertThrows(IllegalArgumentException.class, () -> group.inputs().claim(List.of(input)));
            original.writerIndex(original.capacity());
            ByteBuf tiny = original.retainedSlice(0, 1);
            assertThrows(IllegalArgumentException.class, () -> group.inputs().claim(List.of(input)));
            assertThrows(IllegalStateException.class, input::close);
            assertEquals(2048, group.inputs().usedBytes());
            tiny.release();
            group.inputs().claim(List.of(input));
            assertThrows(IllegalStateException.class, input::close);
            assertThrows(IllegalStateException.class, input::bytes);
            input.releaseFromDriver();
            assertEquals(0, original.refCnt());
            assertEquals(0, group.inputs().usedBytes());
            assertThrows(IllegalArgumentException.class, () -> group.inputs().acquire(context, 0));
            assertThrows(IllegalArgumentException.class, () -> group.inputs().acquire(context,
                    SegmentBatchFormat.maxPayload(config("OFF", "SYSTEM")) + SegmentBatchFormat.ENTRY_OVERHEAD + 1));
            try (SegmentEntryInputs.Input full = group.inputs().acquire(context, 4096)) {
                assertNotNull(full);
                assertNull(group.inputs().acquire(context, 36));
            }
            assertEquals(0, group.inputs().usedBytes());
        } finally {
            assertTrue(group.shutdown(WAIT));
        }
    }

    @Test
    public void bufferBudgetAlignmentAndPoolExhaustionAreObservable() throws Exception {
        assertThrows(IllegalArgumentException.class, () ->
                new SegmentExecutionGroup.Settings(2, 2, 2, 3, 4096, 4096, 3 * 4096, WAIT));
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

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

import java.io.IOException;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.util.affinity.CpuBinding;
import org.apache.bookkeeper.common.util.affinity.CpuTopologySnapshot;
import org.apache.bookkeeper.common.util.affinity.ExecutionPlacementPlan;

/** Cold startup/drain orchestration for the isolated owner and I/O threads. No storage readiness authority. */
final class SegmentExecutionGroup {
    record Settings(int workers, int ioThreads, int controlSlots, int buffers, int bufferBytes,
                    long bufferBudgetBytes, Duration startupTimeout) {
        Settings {
            if (workers < 1 || ioThreads < 1 || ioThreads > 64 || controlSlots < workers || buffers < workers
                    || bufferBytes < 4096 || bufferBytes % 4096 != 0 || bufferBudgetBytes < 1
                    || startupTimeout.isNegative() || startupTimeout.isZero()) {
                throw new IllegalArgumentException("Invalid execution or buffer limits");
            }
            startupTimeout.toNanos();
            long bytes = Math.multiplyExact((long) buffers, Math.addExact(bufferBytes, 4095));
            if (bytes > bufferBudgetBytes) {
                throw new IllegalArgumentException("Aligned buffer allocation exceeds the global buffer budget");
            }
        }
    }

    @FunctionalInterface
    interface PoolFactory {
        SegmentBufferPool create(int owner, int count, int bytes, boolean touch, Runnable wakeup);
    }

    private static final PoolFactory POOLS = (owner, count, bytes, touch, wakeup) ->
            new SegmentBufferPool(owner, 1, count, bytes, 4096, touch, wakeup);

    private static final class BindingFailure extends IOException {
        BindingFailure(Throwable cause) {
            super("Thread binding failed", cause);
        }
    }

    private static final class ThreadState {
        final CountDownLatch ready = new CountDownLatch(1);
        final String name;
        volatile Throwable startupFailure;
        volatile Throwable cleanupFailure;
        volatile BitSet actualMask = new BitSet();
        Thread thread;

        ThreadState(String name) {
            this.name = name;
        }
    }

    private final IsolatedSegmentConfig config;
    private final Settings settings;
    private final ExecutionPlacementPlan plan;
    private final PoolFactory pools;
    private final SegmentOwnerLoop[] owners;
    private final List<ThreadState> threads = new CopyOnWriteArrayList<>();
    private final CountDownLatch activate = new CountDownLatch(1);
    private SegmentIoExecutor io;
    private volatile boolean accepting;
    private String degradation;

    static SegmentExecutionGroup start(IsolatedSegmentConfig config, Settings settings) throws IOException {
        return start(config, settings, POOLS);
    }

    static SegmentExecutionGroup start(IsolatedSegmentConfig config, Settings settings, PoolFactory pools)
            throws IOException {
        if (settings.workers() > config.logicalShards() || settings.workers() > config.maxPendingEntries()) {
            throw new IllegalArgumentException("Workers exceed logical shards or global pending capacity");
        }
        ExecutionPlacementPlan plan;
        String degradation = null;
        if (config.affinity().equals("OFF")) {
            plan = ExecutionPlacementPlan.off(config.logicalShards(), settings.workers());
        } else {
            try {
                plan = ExecutionPlacementPlan.bound(CpuTopologySnapshot.capture(null), config.logicalShards(),
                        settings.workers(), null, null, 1);
            } catch (IOException unavailable) {
                if (config.affinity().equals("REQUIRE")) {
                    throw unavailable;
                }
                degradation = unavailable.toString();
                plan = ExecutionPlacementPlan.off(config.logicalShards(), settings.workers());
            }
        }
        SegmentExecutionGroup group = new SegmentExecutionGroup(config, settings, plan, pools);
        group.degradation = degradation;
        try {
            group.startThreads();
            return group;
        } catch (IOException failed) {
            // startThreads has already joined all initialized scopes. Failed cleanup prohibits fallback.
            boolean bindingOnly = failed.getCause() instanceof BindingFailure
                    && failed.getCause().getCause().getSuppressed().length == 0;
            if (!config.affinity().equals("PREFER") || !bindingOnly || failed.getSuppressed().length != 0) {
                throw failed;
            }
            SegmentExecutionGroup fallback = new SegmentExecutionGroup(config, settings,
                    ExecutionPlacementPlan.off(config.logicalShards(), settings.workers()), pools);
            fallback.degradation = failed.toString();
            fallback.startThreads();
            return fallback;
        }
    }

    private SegmentExecutionGroup(IsolatedSegmentConfig config, Settings settings, ExecutionPlacementPlan plan,
                                  PoolFactory pools) {
        this.config = config;
        this.settings = settings;
        this.plan = plan;
        this.pools = pools;
        owners = new SegmentOwnerLoop[settings.workers()];
    }

    private void startThreads() throws IOException {
        try {
            for (int worker = 0; worker < owners.length; worker++) {
                owners[worker] = new SegmentOwnerLoop(config.logicalShards(), owners.length, worker,
                        share(config.maxPendingEntries(), worker), share(settings.controlSlots(), worker),
                        factory(true, worker));
            }
            // All application threads originate here, before any worker narrows its own mask.
            io = new SegmentIoExecutor(settings.ioThreads(), config.maxPendingEntries(), factory(false, 0));
            for (SegmentOwnerLoop owner : owners) {
                owner.start();
            }
            long end = System.nanoTime() + settings.startupTimeout().toNanos();
            for (ThreadState state : threads) {
                long remaining = end - System.nanoTime();
                if (remaining <= 0 || !state.ready.await(remaining, TimeUnit.NANOSECONDS)) {
                    throw new IOException("Thread initialization timed out: " + state.name);
                }
                if (state.startupFailure != null) {
                    throw new IOException("Thread initialization failed: " + state.name, state.startupFailure);
                }
            }
            accepting = true;
            activate.countDown();
        } catch (Throwable failure) {
            IOException result = failure instanceof IOException ? (IOException) failure
                    : new IOException("Execution startup failed", failure);
            boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
            stopAdmission();
            if (io != null) {
                io.shutdown();
            }
            activate.countDown();
            try {
                if (!joinThreads(settings.startupTimeout())) {
                    result.addSuppressed(new IOException("Startup cleanup did not terminate all threads"));
                }
                addCleanupFailures(result);
                for (ThreadState state : threads) {
                    if (state.startupFailure instanceof BindingFailure) {
                        if (state.startupFailure.getCause().getSuppressed().length != 0) {
                            result.addSuppressed(state.startupFailure);
                        }
                    } else if (state.startupFailure != null && state.startupFailure != result.getCause()) {
                        result.addSuppressed(state.startupFailure);
                    }
                }
            } catch (InterruptedException interruptedCleanup) {
                interrupted = true;
                result.addSuppressed(interruptedCleanup);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            throw result;
        }
    }

    private ThreadFactory factory(boolean append, int worker) {
        AtomicInteger sequence = new AtomicInteger();
        return body -> {
            int id = append ? worker : sequence.getAndIncrement();
            ThreadState state = new ThreadState("isolated-wal-" + (append ? "owner-" : "io-") + id);
            Thread thread = new Thread(() -> {
                CpuBinding binding = null;
                SegmentBufferPool pool = null;
                try {
                    try {
                        BitSet mask = append ? plan.workerMask(id) : plan.serviceMask();
                        if (!mask.isEmpty()) {
                            try {
                                binding = CpuBinding.bindCurrentThread(mask);
                                state.actualMask = binding.actualMask();
                            } catch (Throwable failure) {
                                throw new BindingFailure(failure);
                            }
                        } else {
                            try {
                                state.actualMask = CpuBinding.currentThreadMask();
                            } catch (IOException unavailable) {
                                // OFF never sets a mask; an empty observation explicitly means unavailable.
                                state.actualMask = new BitSet();
                            }
                        }
                        if (append) {
                            pool = pools.create(id, share(settings.buffers(), id), settings.bufferBytes(),
                                    config.numaPolicy().equals("OWNER_TOUCH"), owners[id]::wakeup);
                            owners[id].attachPool(pool);
                        }
                    } catch (Throwable failure) {
                        state.startupFailure = failure;
                    } finally {
                        state.ready.countDown();
                    }
                    boolean interrupted = false;
                    for (;;) {
                        try {
                            activate.await();
                            break;
                        } catch (InterruptedException ignored) {
                            interrupted = true;
                        }
                    }
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    if (state.startupFailure != null) {
                        // Also covers a service thread replacement after the startup gate has opened.
                        stopAdmission();
                        if (io != null) {
                            io.shutdown();
                        }
                    }
                    // Even after failed initialization, ThreadPoolExecutor's Worker must run its shutdown path.
                    body.run();
                } finally {
                    try {
                        if (pool != null) {
                            pool.close();
                        }
                    } catch (Throwable failure) {
                        state.cleanupFailure = failure;
                    } finally {
                        try {
                            if (binding != null) {
                                binding.close();
                            }
                        } catch (Throwable failure) {
                            if (state.cleanupFailure == null) {
                                state.cleanupFailure = failure;
                            } else {
                                state.cleanupFailure.addSuppressed(failure);
                            }
                        }
                    }
                }
            }, state.name);
            state.thread = thread;
            threads.add(state);
            return thread;
        };
    }

    boolean submit(int shard, SegmentOwnerLoop.Command command) {
        return accepting && owners[plan.workerForShard(shard)].submit(shard, command);
    }

    SegmentOwnerLoop owner(int shard) {
        return owners[plan.workerForShard(shard)];
    }

    SegmentIoExecutor io() {
        return io;
    }

    String degradation() {
        return degradation;
    }

    List<BitSet> actualMasks() {
        return threads.stream().map(state -> (BitSet) state.actualMask.clone()).toList();
    }

    long reservedBufferBytes() {
        return Math.multiplyExact((long) settings.buffers(), settings.bufferBytes() + 4095L);
    }

    void stopAdmission() {
        accepting = false;
        for (SegmentOwnerLoop owner : owners) {
            if (owner != null) {
                owner.stopAdmission();
            }
        }
    }

    boolean shutdown(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive shutdown timeout required");
        }
        timeout.toNanos();
        stopAdmission();
        long end = System.nanoTime() + timeout.toNanos();
        for (SegmentOwnerLoop owner : owners) {
            long remaining = end - System.nanoTime();
            if (remaining <= 0 || !owner.awaitTermination(Duration.ofNanos(remaining))) {
                return false;
            }
        }
        // Accepted owner commands may still submit I/O while draining, so shut services down last.
        io.shutdown();
        long remaining = end - System.nanoTime();
        if (remaining <= 0 || !joinThreads(Duration.ofNanos(remaining))) {
            return false;
        }
        IOException cleanup = new IOException("Execution cleanup failed");
        addCleanupFailures(cleanup);
        if (cleanup.getSuppressed().length != 0) {
            throw cleanup;
        }
        return true;
    }

    private boolean joinThreads(Duration timeout) throws InterruptedException {
        long end = System.nanoTime() + timeout.toNanos();
        for (ThreadState state : threads) {
            if (Thread.currentThread() == state.thread) {
                throw new IllegalStateException("Drain must run outside application workers");
            }
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedJoin(state.thread, remaining);
            if (state.thread.isAlive()) {
                return false;
            }
        }
        return true;
    }

    private void addCleanupFailures(IOException failure) {
        for (ThreadState state : threads) {
            if (state.cleanupFailure != null) {
                failure.addSuppressed(state.cleanupFailure);
            }
        }
    }

    private int share(int total, int worker) {
        return total / owners.length + (worker < total % owners.length ? 1 : 0);
    }
}

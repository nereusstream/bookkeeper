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
import java.util.concurrent.locks.LockSupport;
import org.apache.bookkeeper.common.util.affinity.CpuBinding;
import org.apache.bookkeeper.common.util.affinity.CpuTopologySnapshot;
import org.apache.bookkeeper.common.util.affinity.ExecutionPlacementPlan;

/** Cold startup/drain orchestration for the isolated owner and I/O threads. No storage readiness authority. */
final class SegmentExecutionGroup {
    record Settings(int workers, int ioThreads, int controlSlots, int buffers, int bufferBytes,
                    long inputBudgetBytes, long bufferBudgetBytes, Duration startupTimeout) {
        Settings {
            if (workers < 1 || ioThreads < 2 || ioThreads > 64 || controlSlots < workers || buffers < workers
                    || bufferBytes < 4096 || bufferBytes % 4096 != 0 || inputBudgetBytes < 1 || bufferBudgetBytes < 1
                    || startupTimeout.isNegative() || startupTimeout.isZero()) {
                throw new IllegalArgumentException("Invalid execution or buffer limits");
            }
            startupTimeout.toNanos();
            long bytes = Math.multiplyExact((long) buffers, Math.addExact(bufferBytes, 4095));
            if (Math.addExact(bytes, inputBudgetBytes) > bufferBudgetBytes) {
                throw new IllegalArgumentException(
                        "Source and aligned buffer allocations exceed the DATA buffer budget");
            }
        }

        long reservedBufferBytes() {
            return Math.multiplyExact((long) buffers, Math.addExact(bufferBytes, 4095));
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
    private final SegmentEntryInputs inputs;
    private final Settings settings;
    private final ExecutionPlacementPlan plan;
    private final PoolFactory pools;
    private final SegmentOwnerLoop[] owners;
    private final CpuTopologySnapshot startupTopology;
    private final List<ThreadState> threads = new CopyOnWriteArrayList<>();
    private final CountDownLatch activate = new CountDownLatch(1);
    private final ThreadLocal<ThreadState> currentState = new ThreadLocal<>();
    private final ThreadLocal<CpuBinding> currentBinding = new ThreadLocal<>();
    private final ThreadLocal<long[]> lastBindingCheck = ThreadLocal.withInitial(() -> new long[1]);
    private SegmentIoExecutor io;
    private Thread monitor;
    private volatile boolean accepting;
    private volatile boolean monitorRunning;
    private volatile BitSet currentEligible;
    private volatile BitSet fallbackMask;
    private volatile String degradation;
    private volatile String runtimeFailure;

    /**
     * Known fixed execution/pool Java metadata, separate from aligned backing bytes and operation fees.
     * Dynamic Input graphs, thread tracking/ThreadLocal/CPU topology, executor Workers, AQS waiters,
     * cleaners, leak trackers and native resources are outside this deliberately bounded component.
     */
    static long metadataCapacity(IsolatedSegmentConfig config, Settings settings) throws IOException {
        return metadataCapacity(config, settings, SegmentControlCapacity.Layout.hotSpot());
    }

    static long metadataCapacity(IsolatedSegmentConfig config, Settings settings,
                                 SegmentControlCapacity.Layout layout) {
        if (settings.workers() > config.logicalShards() || settings.workers() > config.maxPendingEntries()) {
            throw new IllegalArgumentException("Workers exceed logical shards or global pending capacity");
        }
        long containers = SegmentControlCapacity.sum(layout.object(SegmentExecutionGroup.class),
                layout.object(SegmentEntryInputs.class),
                layout.refs(settings.workers()), layout.object(CountDownLatch.class),
                layout.object("java.util.concurrent.CountDownLatch$Sync"),
                SegmentIoExecutor.metadataCapacity(layout, settings.ioThreads() - 1, config.maxPendingEntries(),
                        1, settings.controlSlots()));
        long pools = 0;
        for (int worker = 0; worker < settings.workers(); worker++) {
            int pending = share(config.maxPendingEntries(), settings.workers(), worker);
            int controls = share(settings.controlSlots(), settings.workers(), worker);
            int buffers = share(settings.buffers(), settings.workers(), worker);
            long poolContainers = SegmentBufferPool.containerMetadataCapacity(layout, buffers);
            containers = SegmentControlCapacity.sum(containers, poolContainers,
                    SegmentOwnerLoop.metadataCapacity(layout, config.logicalShards(), settings.workers(), worker,
                            pending, controls));
            pools = SegmentControlCapacity.sum(pools,
                    SegmentBufferPool.metadataCapacity(layout, buffers) - poolContainers);
        }
        // A PREFER binding failure retains its joined original group while constructing and returning fallback.
        // Pool.close has released its Slots; the original group's empty owner/I/O/pool containers still coexist.
        return SegmentControlCapacity.sum(layout.object(Settings.class), layout.object(Duration.class), pools,
                SegmentControlCapacity.times(config.affinity().equals("PREFER") ? 2 : 1, containers));
    }

    static SegmentExecutionGroup start(IsolatedSegmentConfig config, Settings settings) throws IOException {
        return start(config, settings, POOLS);
    }

    /** The local graph retains this owner even if thread initialization or its first drain fails. */
    static SegmentExecutionGroup prepareOff(IsolatedSegmentConfig config, Settings settings) {
        if (!config.affinity().equals("OFF") || settings.workers() > config.logicalShards()
                || settings.workers() > config.maxPendingEntries()) {
            throw new IllegalArgumentException("Invalid local execution geometry");
        }
        return new SegmentExecutionGroup(config, settings,
                ExecutionPlacementPlan.off(config.logicalShards(), settings.workers()), POOLS, null);
    }

    static SegmentExecutionGroup start(IsolatedSegmentConfig config, Settings settings, PoolFactory pools)
            throws IOException {
        if (settings.workers() > config.logicalShards() || settings.workers() > config.maxPendingEntries()) {
            throw new IllegalArgumentException("Workers exceed logical shards or global pending capacity");
        }
        ExecutionPlacementPlan plan;
        CpuTopologySnapshot topology = null;
        String degradation = null;
        if (config.affinity().equals("OFF")) {
            plan = ExecutionPlacementPlan.off(config.logicalShards(), settings.workers());
        } else {
            try {
                topology = CpuTopologySnapshot.capture(null);
                plan = ExecutionPlacementPlan.bound(topology, config.logicalShards(),
                        settings.workers(), null, null, 1);
            } catch (IOException unavailable) {
                if (config.affinity().equals("REQUIRE")) {
                    throw unavailable;
                }
                degradation = unavailable.toString();
                plan = ExecutionPlacementPlan.off(config.logicalShards(), settings.workers());
                topology = null;
            }
        }
        SegmentExecutionGroup group = new SegmentExecutionGroup(config, settings, plan, pools, topology);
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
                    ExecutionPlacementPlan.off(config.logicalShards(), settings.workers()), pools, null);
            fallback.degradation = failed.toString();
            fallback.startThreads();
            return fallback;
        }
    }

    private SegmentExecutionGroup(IsolatedSegmentConfig config, Settings settings, ExecutionPlacementPlan plan,
                                  PoolFactory pools, CpuTopologySnapshot topology) {
        this.config = config;
        this.settings = settings;
        inputs = new SegmentEntryInputs(settings.inputBudgetBytes(),
                SegmentBatchFormat.maxPayload(config) + SegmentBatchFormat.ENTRY_OVERHEAD);
        this.plan = plan;
        this.pools = pools;
        startupTopology = topology;
        currentEligible = topology == null ? null : topology.eligibleCpus();
        owners = new SegmentOwnerLoop[settings.workers()];
    }

    void startThreads() throws IOException {
        try {
            for (int worker = 0; worker < owners.length; worker++) {
                owners[worker] = new SegmentOwnerLoop(config.logicalShards(), owners.length, worker,
                        share(config.maxPendingEntries(), worker), share(settings.controlSlots(), worker),
                        factory(true, worker));
            }
            // All application threads originate here, before any worker narrows its own mask.
            io = new SegmentIoExecutor(settings.ioThreads() - 1, config.maxPendingEntries(), 1,
                    settings.controlSlots(), factory(false, 0),
                    startupTopology == null ? null : this::checkCurrentBinding);
            for (SegmentOwnerLoop owner : owners) {
                owner.start();
            }
            if (startupTopology != null) {
                monitorRunning = true;
                monitor = factory(false, 0, "placement-").newThread(this::monitorLoop);
                monitor.start();
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
            stopMonitor();
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
        return factory(append, worker, append ? "owner-" : "io-");
    }

    private ThreadFactory factory(boolean append, int worker, String role) {
        AtomicInteger sequence = new AtomicInteger();
        return body -> {
            int id = append ? worker : sequence.getAndIncrement();
            ThreadState state = new ThreadState("isolated-wal-" + role + id);
            Thread thread = new Thread(() -> {
                CpuBinding binding = null;
                SegmentBufferPool pool = null;
                try {
                    try {
                        BitSet mask = append ? plan.workerMask(id) : plan.serviceMask();
                        BitSet released = fallbackMask;
                        if (released != null) {
                            state.actualMask = CpuBinding.currentThreadMask();
                            if (!state.actualMask.equals(released)) {
                                throw new IOException("Replacement thread inherited a narrow mask after fallback");
                            }
                        } else if (!mask.isEmpty()) {
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
                            owners[id].attachInputs(inputs);
                            if (binding != null) {
                                owners[id].setPlacementCheck(this::checkCurrentBinding);
                            }
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
                    currentState.set(state);
                    if (binding != null) {
                        currentBinding.set(binding);
                    }
                    // Even after failed initialization, ThreadPoolExecutor's Worker must run its shutdown path.
                    body.run();
                } finally {
                    currentBinding.remove();
                    currentState.remove();
                    lastBindingCheck.remove();
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

    boolean submitControl(int shard, SegmentOwnerLoop.Command command) {
        return accepting && owners[plan.workerForShard(shard)].submitControl(command);
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

    String runtimeFailure() {
        return runtimeFailure;
    }

    List<BitSet> actualMasks() {
        return threads.stream().map(state -> (BitSet) state.actualMask.clone()).toList();
    }

    SegmentEntryInputs inputs() {
        return inputs;
    }

    long reservedBufferBytes() {
        return settings.reservedBufferBytes();
    }

    void stopAdmission() {
        accepting = false;
        inputs.stopAdmission();
        for (SegmentOwnerLoop owner : owners) {
            if (owner != null) {
                owner.stopAdmission();
            }
        }
    }

    boolean accepting() {
        return accepting;
    }

    boolean hasFailedOwner() {
        for (SegmentOwnerLoop owner : owners) {
            if (owner != null && owner.failure() != null) {
                return true;
            }
        }
        return false;
    }

    boolean onOwnerThread() {
        for (SegmentOwnerLoop owner : owners) {
            if (owner != null && owner.isOwnerThread()) {
                return true;
            }
        }
        return false;
    }

    /** A cleanup error can be reported after all threads stopped; resource owners must distinguish that case. */
    boolean terminated() {
        for (ThreadState state : threads) {
            if (state.thread != null && state.thread.isAlive()) {
                return false;
            }
        }
        return monitor == null || !monitor.isAlive();
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
            if (owner != null && (remaining <= 0 || !owner.awaitTermination(Duration.ofNanos(remaining)))) {
                return false;
            }
        }
        // Accepted owner commands may still submit I/O while draining, so shut services down last.
        if (io != null) {
            io.shutdown();
        }
        stopMonitor();
        long remaining = end - System.nanoTime();
        if (remaining <= 0 || !joinThreads(Duration.ofNanos(remaining))) {
            return false;
        }
        IOException cleanup = new IOException("Execution cleanup failed");
        addCleanupFailures(cleanup);
        if (cleanup.getSuppressed().length != 0) {
            throw cleanup;
        }
        if (inputs.usedBytes() != 0) {
            throw new IOException("Unreleased caller-owned entry inputs remain after execution drain");
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

    private void monitorLoop() {
        while (monitorRunning) {
            try {
                observePlacement(CpuTopologySnapshot.refresh(startupTopology.deploymentMask(), null));
                checkCurrentBinding();
            } catch (IOException | RuntimeException failure) {
                placementFailure("Runtime topology check failed: " + failure);
            }
            if (monitorRunning) {
                LockSupport.parkNanos(this, TimeUnit.SECONDS.toNanos(1));
            }
        }
    }

    void observePlacement(CpuTopologySnapshot now) {
        BitSet eligible = now.eligibleCpus();
        currentEligible = eligible;
        if (eligible.isEmpty()) {
            placementFailure("No currently eligible CPU remains");
            return;
        }
        if (config.numaPolicy().equals("OWNER_TOUCH")
                && !now.memoryNodes().equals(startupTopology.memoryNodes()) && degradation == null) {
            degradation = "Effective memory nodes changed; existing buffers retain their original placement";
        }
        if (fallbackMask != null) {
            fallbackMask = eligible;
            return;
        }
        boolean masksValid = containsAll(eligible, plan.serviceMask());
        for (int owner = 0; owner < owners.length; owner++) {
            masksValid &= containsAll(eligible, plan.workerMask(owner));
        }
        boolean quotaValid = now.cpuQuota().isPresent()
                && now.cpuQuota().getAsDouble() >= owners.length + 1;
        if (!masksValid || !quotaValid) {
            placementChange("CPU mask or quota no longer supports the startup placement");
        }
    }

    private static boolean containsAll(BitSet available, BitSet required) {
        BitSet missing = (BitSet) required.clone();
        missing.andNot(available);
        return missing.isEmpty();
    }

    private void checkCurrentBinding() {
        CpuBinding binding = currentBinding.get();
        if (binding == null) {
            return;
        }
        ThreadState state = currentState.get();
        BitSet release = fallbackMask;
        if (release != null) {
            try {
                binding.closeTo(release);
                state.actualMask = binding.restoredMask();
                currentBinding.remove();
            } catch (IOException | RuntimeException failure) {
                placementFailure("Cannot release preferred binding on " + state.name + ": " + failure);
            }
            return;
        }
        long now = System.nanoTime();
        long[] last = lastBindingCheck.get();
        if (now - last[0] < TimeUnit.SECONDS.toNanos(1)) {
            return;
        }
        last[0] = now;
        try {
            state.actualMask = binding.verifyCurrentMask();
        } catch (IOException failure) {
            placementChange("Actual mask changed on " + state.name + ": " + failure);
            if (fallbackMask != null) {
                checkCurrentBinding();
            }
        }
    }

    private void placementChange(String reason) {
        if (config.affinity().equals("PREFER") && currentEligible != null && !currentEligible.isEmpty()) {
            degradation = reason;
            fallbackMask = (BitSet) currentEligible.clone();
        } else {
            placementFailure(reason);
        }
    }

    private void placementFailure(String reason) {
        if (runtimeFailure == null) {
            runtimeFailure = reason;
        }
        stopAdmission();
    }

    private void stopMonitor() {
        monitorRunning = false;
        if (monitor != null) {
            LockSupport.unpark(monitor);
        }
    }

    private int share(int total, int worker) {
        return share(total, owners.length, worker);
    }

    private static int share(int total, int workers, int worker) {
        return total / workers + (worker < total % workers ? 1 : 0);
    }
}

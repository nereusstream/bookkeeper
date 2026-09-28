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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Fixed platform-thread owner for a static subset of shards. DATA and asynchronous control admissions
 * each reserve their eventual completion slots. Handlers must be bounded and never block on I/O here.
 */
final class SegmentOwnerLoop {
    interface Command {
        void run(Ticket ticket);

        void failed(Throwable cause);
    }

    private record ControlTask(Runnable action, boolean awaitsCompletion) { }

    /** One admitted operation's completion obligation, retained across timeouts and I/O submission. */
    final class Ticket {
        private final Command command;
        private final boolean control;
        private boolean posted;
        private boolean ioSubmitted;
        private volatile Throwable commandFailure;
        private Runnable completion;

        private Ticket(Command command, boolean control) {
            this.command = command;
            this.control = control;
        }

        // May be called by an I/O thread. Even after admission closes, this slot remains reserved.
        void complete(Runnable action) {
            Objects.requireNonNull(action, "action");
            synchronized (gate) {
                if (posted) {
                    throw new IllegalStateException("Completion already posted");
                }
                completion = action;
                if (!(control ? controlCompletions : completions).offer(this)) {
                    throw new IllegalStateException("Reserved completion capacity violated");
                }
                posted = true;
            }
            LockSupport.unpark(thread);
        }

        void beginIo() {
            requireOwner();
            synchronized (gate) {
                if (posted || ioSubmitted) {
                    throw new IllegalStateException("Batch already submitted or completed");
                }
                ioSubmitted = true;
            }
        }

        void fail(Throwable cause) {
            complete(() -> command.failed(cause));
        }
    }

    private final Object gate = new Object();
    private final int logicalShards;
    private final int workerId;
    private final int workerCount;
    private final int capacity;
    private final int controlCapacity;
    private final ArrayBlockingQueue<Ticket>[] data;
    private final ArrayBlockingQueue<Ticket> completions;
    private final ArrayBlockingQueue<Ticket> controlCompletions;
    private final ArrayBlockingQueue<ControlTask> controls;
    private final long[] deadlines;
    private final Runnable[] due;
    private final Thread thread;
    private int pending;
    private int controlPending;
    private boolean accepting = true;
    private volatile Throwable failure;
    private int nextShard;
    private SegmentBufferPool pool;
    private Runnable placementCheck;

    SegmentOwnerLoop(int logicalShards, int workerCount, int workerId, int capacity, int controlCapacity) {
        this(logicalShards, workerCount, workerId, capacity, controlCapacity,
                task -> new Thread(task, "isolated-wal-owner-" + workerId));
    }

    @SuppressWarnings("unchecked")
    SegmentOwnerLoop(int logicalShards, int workerCount, int workerId, int capacity, int controlCapacity,
                     ThreadFactory factory) {
        if (logicalShards < 1 || logicalShards > 64 || workerCount < 1 || workerCount > logicalShards
                || workerId < 0 || workerId >= workerCount || capacity < 1 || controlCapacity < 1) {
            throw new IllegalArgumentException("Invalid fixed owner bounds");
        }
        this.logicalShards = logicalShards;
        this.workerCount = workerCount;
        this.workerId = workerId;
        this.capacity = capacity;
        this.controlCapacity = controlCapacity;
        data = new ArrayBlockingQueue[logicalShards];
        for (int shard = workerId; shard < logicalShards; shard += workerCount) {
            data[shard] = new ArrayBlockingQueue<>(capacity);
        }
        completions = new ArrayBlockingQueue<>(capacity);
        controlCompletions = new ArrayBlockingQueue<>(controlCapacity);
        controls = new ArrayBlockingQueue<>(controlCapacity);
        deadlines = new long[logicalShards];
        due = new Runnable[logicalShards];
        nextShard = workerId;
        thread = factory.newThread(this::run);
    }

    void attachPool(SegmentBufferPool pool) {
        requireOwner();
        if (this.pool != null) {
            throw new IllegalStateException("Pool already initialized");
        }
        this.pool = pool;
    }

    void setPlacementCheck(Runnable check) {
        requireOwner();
        placementCheck = Objects.requireNonNull(check, "check");
    }

    SegmentBufferPool pool() {
        requireOwner();
        return pool;
    }

    void wakeup() {
        LockSupport.unpark(thread);
    }

    // Called from the deployment/startup thread; placement occurs in the supplied thread entrypoint.
    void start() {
        thread.start();
    }

    static int shardFor(long ledgerId, int logicalShards) {
        if (ledgerId < 0 || logicalShards < 1) {
            throw new IllegalArgumentException("Invalid persistent routing input");
        }
        return (int) (ledgerId % logicalShards);
    }

    boolean submit(int shard, Command command) {
        requireShard(shard);
        Objects.requireNonNull(command, "command");
        synchronized (gate) {
            if (!accepting || pending == capacity) {
                return false;
            }
            Ticket ticket = new Ticket(command, false);
            if (!data[shard].offer(ticket)) {
                throw new IllegalStateException("DATA capacity accounting violated");
            }
            pending++;
        }
        LockSupport.unpark(thread);
        return true;
    }

    boolean control(Runnable action) {
        Objects.requireNonNull(action, "action");
        synchronized (gate) {
            if (!accepting || controlPending == controlCapacity
                    || !controls.offer(new ControlTask(action, false))) {
                return false;
            }
            controlPending++;
        }
        LockSupport.unpark(thread);
        return true;
    }

    boolean submitControl(Command command) {
        Objects.requireNonNull(command, "command");
        synchronized (gate) {
            if (!accepting || controlPending == controlCapacity) {
                return false;
            }
            Ticket ticket = new Ticket(command, true);
            if (!controls.offer(new ControlTask(() -> dispatch(ticket), true))) {
                throw new IllegalStateException("Control capacity accounting violated");
            }
            controlPending++;
        }
        LockSupport.unpark(thread);
        return true;
    }

    /** Keep the oldest pending batch deadline; visiting another shard cannot postpone it. */
    void deadline(int shard, long deadlineNanos, Runnable action) {
        requireOwner();
        requireShard(shard);
        Objects.requireNonNull(action, "action");
        if (due[shard] == null || deadlineNanos - deadlines[shard] < 0) {
            deadlines[shard] = deadlineNanos;
            due[shard] = action;
        }
    }

    void cancelDeadline(int shard) {
        requireOwner();
        requireShard(shard);
        due[shard] = null;
    }

    void stopAdmission() {
        synchronized (gate) {
            accepting = false;
        }
        LockSupport.unpark(thread);
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        if (Thread.currentThread() == thread || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive external wait required");
        }
        TimeUnit.NANOSECONDS.timedJoin(thread, timeout.toNanos());
        return !thread.isAlive();
    }

    Throwable failure() {
        return failure;
    }

    int pending() {
        synchronized (gate) {
            return pending;
        }
    }

    int controlPending() {
        synchronized (gate) {
            return controlPending;
        }
    }

    private void requireShard(int shard) {
        if (shard < 0 || shard >= logicalShards || shard % workerCount != workerId) {
            throw new IllegalArgumentException("Shard belongs to another owner");
        }
    }

    void requireOwner() {
        if (Thread.currentThread() != thread) {
            throw new IllegalStateException("Shard state accessed outside its owner");
        }
    }

    private void recordFailure(Throwable cause) {
        synchronized (gate) {
            if (failure == null) {
                failure = cause;
            }
            accepting = false;
        }
    }

    private void run() {
        for (;;) {
            // Clear external interrupts instead of turning PARK into a busy loop. Shutdown is explicit.
            Thread.interrupted();
            if (placementCheck != null) {
                placementCheck.run();
            }
            boolean work = pool != null && pool.drainReturns();
            Ticket completed = controlCompletions.poll();
            if (completed != null) {
                work = true;
                complete(completed);
            }
            completed = completions.poll();
            if (completed != null) {
                work = true;
                complete(completed);
            }
            ControlTask control = controls.poll();
            if (control != null) {
                work = true;
                try {
                    control.action().run();
                } catch (Throwable cause) {
                    recordFailure(cause);
                } finally {
                    if (!control.awaitsCompletion()) {
                        synchronized (gate) {
                            controlPending--;
                        }
                    }
                }
            }
            // One DATA command from each owned shard per round, even under a permanently hot shard.
            for (int visited = workerId; visited < logicalShards; visited += workerCount) {
                int shard = nextShard;
                nextShard += workerCount;
                if (nextShard >= logicalShards) {
                    nextShard = workerId;
                }
                Ticket ticket = data[shard].poll();
                if (ticket != null) {
                    work = true;
                    dispatch(ticket);
                }
            }
            long delay = Long.MAX_VALUE;
            for (int shard = workerId; shard < logicalShards; shard += workerCount) {
                if (due[shard] != null) {
                    long remaining = deadlines[shard] - System.nanoTime();
                    if (remaining <= 0) {
                        Runnable action = due[shard];
                        due[shard] = null;
                        work = true;
                        try {
                            action.run();
                        } catch (Throwable cause) {
                            recordFailure(cause);
                        }
                    } else {
                        delay = Math.min(delay, remaining);
                    }
                }
            }
            synchronized (gate) {
                if (!accepting && pending == 0 && controlPending == 0 && controls.isEmpty() && !hasDeadline()
                        && (pool == null || pool.outstanding() == 0)) {
                    return;
                }
            }
            if (!work) {
                // enqueue-before-park is safe: unpark retains a permit; inputs queued before start are polled first.
                if (placementCheck != null) {
                    delay = Math.min(delay, TimeUnit.SECONDS.toNanos(1));
                }
                if (delay == Long.MAX_VALUE) {
                    LockSupport.park(this);
                } else {
                    LockSupport.parkNanos(this, delay);
                }
            }
        }
    }

    private void dispatch(Ticket ticket) {
        try {
            if (failure != null) {
                ticket.fail(failure);
            } else {
                ticket.command.run(ticket);
            }
        } catch (Throwable cause) {
            recordFailure(cause);
            synchronized (gate) {
                ticket.commandFailure = cause;
                if (!ticket.posted && !ticket.ioSubmitted) {
                    ticket.fail(cause);
                }
            }
        }
    }

    private void complete(Ticket ticket) {
        try {
            if (ticket.commandFailure != null) {
                ticket.command.failed(ticket.commandFailure);
            } else {
                ticket.completion.run();
            }
        } catch (Throwable cause) {
            recordFailure(cause);
        } finally {
            synchronized (gate) {
                if (ticket.control) {
                    controlPending--;
                } else {
                    pending--;
                }
            }
        }
    }

    private boolean hasDeadline() {
        for (Runnable action : due) {
            if (action != null) {
                return true;
            }
        }
        return false;
    }
}

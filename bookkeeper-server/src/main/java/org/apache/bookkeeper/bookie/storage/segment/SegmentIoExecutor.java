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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Bounded blocking I/O service. Results always return through the original owner's reserved slot. */
final class SegmentIoExecutor {
    @FunctionalInterface
    interface Operation {
        void run() throws Exception;
    }

    private final ThreadPoolExecutor executor;
    private final ThreadPoolExecutor controlExecutor;
    private final Runnable beforeOperation;

    /** Known executor/container shells; Worker, Thread, thread registries and AQS waiters remain outside this fee. */
    static long metadataCapacity(SegmentControlCapacity.Layout layout, int dataThreads, int dataQueueCapacity,
                                 int controlThreads, int controlQueueCapacity) {
        if (dataThreads < 1 || dataQueueCapacity < 1 || controlThreads < 0
                || (controlThreads == 0 ? controlQueueCapacity != 0 : controlQueueCapacity < 1)) {
            throw new IllegalArgumentException("Invalid I/O bounds");
        }
        // Both executors share the group's one thread factory and its sequence counter.
        long fixed = SegmentControlCapacity.sum(layout.object(SegmentIoExecutor.class), layout.captured(8),
                layout.object(AtomicInteger.class), layout.captured(1),
                executorMetadataCapacity(layout, dataThreads, dataQueueCapacity));
        return controlThreads == 0 ? fixed : SegmentControlCapacity.sum(fixed,
                executorMetadataCapacity(layout, controlThreads, controlQueueCapacity));
    }

    private static long executorMetadataCapacity(SegmentControlCapacity.Layout layout, int threads, int capacity) {
        return SegmentControlCapacity.sum(layout.object(ThreadPoolExecutor.class), layout.object(AtomicInteger.class),
                layout.blockingQueue(capacity), layout.reentrantLock(1),
                layout.object(ThreadPoolExecutor.AbortPolicy.class), layout.hashSet(),
                SegmentControlCapacity.times(threads, layout.hashEntry()));
    }

    SegmentIoExecutor(int threads, int queueCapacity) {
        this(threads, queueCapacity, defaultFactory(), null);
    }

    private static ThreadFactory defaultFactory() {
        AtomicInteger number = new AtomicInteger();
        return task -> new Thread(task, "isolated-wal-io-" + number.getAndIncrement());
    }

    SegmentIoExecutor(int threads, int queueCapacity, ThreadFactory factory) {
        this(threads, queueCapacity, factory, null);
    }

    SegmentIoExecutor(int threads, int queueCapacity, ThreadFactory factory, Runnable beforeOperation) {
        this(threads, queueCapacity, 0, 0, factory, beforeOperation);
    }

    SegmentIoExecutor(int dataThreads, int dataQueueCapacity, int controlThreads, int controlQueueCapacity) {
        this(dataThreads, dataQueueCapacity, controlThreads, controlQueueCapacity, defaultFactory(), null);
    }

    SegmentIoExecutor(int dataThreads, int dataQueueCapacity, int controlThreads, int controlQueueCapacity,
                      ThreadFactory factory, Runnable beforeOperation) {
        if (dataThreads < 1 || dataQueueCapacity < 1 || controlThreads < 0
                || (controlThreads == 0 ? controlQueueCapacity != 0 : controlQueueCapacity < 1)) {
            throw new IllegalArgumentException("Invalid I/O bounds");
        }
        this.beforeOperation = beforeOperation;
        executor = create(dataThreads, dataQueueCapacity, factory);
        try {
            controlExecutor = controlThreads == 0 ? null : create(controlThreads, controlQueueCapacity, factory);
        } catch (RuntimeException | Error failure) {
            executor.shutdownNow();
            throw failure;
        }
    }

    private static ThreadPoolExecutor create(int threads, int queueCapacity, ThreadFactory factory) {
        if (threads < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Positive I/O bounds required");
        }
        ThreadPoolExecutor result = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.NANOSECONDS,
                new ArrayBlockingQueue<>(queueCapacity), factory,
                new ThreadPoolExecutor.AbortPolicy());
        // Start from the unbound startup thread, never lazily from a single-CPU append owner.
        try {
            result.prestartAllCoreThreads();
        } catch (RuntimeException | Error failure) {
            result.shutdownNow();
            throw failure;
        }
        return result;
    }

    void submit(SegmentOwnerLoop.Ticket ticket, Operation operation, Consumer<Throwable> completed) {
        submit(executor, ticket, operation, completed, false);
    }

    /** A fresh index Get can return to the owner and reuse its ticket for the subsequent DATA read. */
    void submitContinuing(SegmentOwnerLoop.Ticket ticket, Operation operation, Consumer<Throwable> completed) {
        submit(executor, ticket, operation, completed, true);
    }

    void submitControl(SegmentOwnerLoop.Ticket ticket, Operation operation, Consumer<Throwable> completed) {
        if (controlExecutor == null) {
            throw new IllegalStateException("No reserved control I/O service");
        }
        submit(controlExecutor, ticket, operation, completed, false);
    }

    private void submit(ThreadPoolExecutor target, SegmentOwnerLoop.Ticket ticket, Operation operation,
                        Consumer<Throwable> completed, boolean continuing) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(completed, "completed");
        ticket.beginIo();
        try {
            target.execute(() -> {
                Throwable failure = null;
                try {
                    if (beforeOperation != null) {
                        beforeOperation.run();
                    }
                    operation.run();
                } catch (Throwable cause) {
                    failure = cause;
                }
                Throwable result = failure;
                returnToOwner(ticket, () -> completed.accept(result), continuing);
            });
        } catch (RejectedExecutionException rejected) {
            returnToOwner(ticket, () -> completed.accept(rejected), continuing);
        }
    }

    private static void returnToOwner(SegmentOwnerLoop.Ticket ticket, Runnable action, boolean continuing) {
        if (continuing) {
            ticket.resumeAfterIo(action);
        } else {
            ticket.complete(action);
        }
    }

    void shutdown() {
        // Do not interrupt submitted file I/O or pretend an unresolved write released its coordinates.
        executor.shutdown();
        if (controlExecutor != null) {
            controlExecutor.shutdown();
        }
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        long end = System.nanoTime() + timeout.toNanos();
        if (!executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            return false;
        }
        return controlExecutor == null || controlExecutor.awaitTermination(
                Math.max(0, end - System.nanoTime()), TimeUnit.NANOSECONDS);
    }
}

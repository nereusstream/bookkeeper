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

    SegmentIoExecutor(int threads, int queueCapacity) {
        if (threads < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Positive I/O bounds required");
        }
        AtomicInteger number = new AtomicInteger();
        executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.NANOSECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task ->
                        new Thread(task, "isolated-wal-io-" + number.getAndIncrement()),
                new ThreadPoolExecutor.AbortPolicy());
        // Start from the unbound startup thread, never lazily from a single-CPU append owner.
        try {
            executor.prestartAllCoreThreads();
        } catch (RuntimeException | Error failure) {
            executor.shutdownNow();
            throw failure;
        }
    }

    void submit(SegmentOwnerLoop.Ticket ticket, Operation operation, Consumer<Throwable> completed) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(completed, "completed");
        ticket.beginIo();
        try {
            executor.execute(() -> {
                Throwable failure = null;
                try {
                    operation.run();
                } catch (Throwable cause) {
                    failure = cause;
                }
                Throwable result = failure;
                ticket.complete(() -> completed.accept(result));
            });
        } catch (RejectedExecutionException rejected) {
            ticket.complete(() -> completed.accept(rejected));
        }
    }

    void shutdown() {
        // Do not interrupt submitted file I/O or pretend an unresolved write released its coordinates.
        executor.shutdown();
    }

    boolean awaitTermination(Duration timeout) throws InterruptedException {
        return executor.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }
}

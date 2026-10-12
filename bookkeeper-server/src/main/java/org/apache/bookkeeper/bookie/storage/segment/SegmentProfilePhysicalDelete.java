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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Explicit local physical retirement through the original index/COPY/MOVE/FREE services.
 * REUSABLE reports Arena reuse, never filesystem shrink, secure erase, cluster completion or history GC. */
public final class SegmentProfilePhysicalDelete {
    private SegmentProfilePhysicalDelete() { }

    public enum Status { REUSABLE, NOT_DELETED, PENDING, LIMIT, UNKNOWN }

    public record Result(Status status, int scannedAllocations, int compactions, int freedAllocations,
            long sourceBytesRetired,
            long destinationBytes, long bookieCut, long arenaCut, Throwable failure) { }

    public record Attempt(CompletableFuture<Result> result, CompletableFuture<Void> completion) { }

    static Attempt begin(IsolatedSegmentStorage storage, ProfileLedgerContext context, ThreadPoolExecutor cold,
            AtomicBoolean active, int maximumAllocations, int maximumCompactions, Duration timeout) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(cold, "cold");
        Objects.requireNonNull(timeout, "timeout");
        if (maximumAllocations < 1 || maximumAllocations > 4096 || maximumCompactions < 1
                || maximumCompactions > 256 || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0
                || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Physical deletion needs bounded cold work and explicit finite limits");
        }
        if (!active.compareAndSet(false, true)) {
            return new Attempt(CompletableFuture.completedFuture(unavailable(Status.PENDING)),
                    CompletableFuture.completedFuture(null));
        }
        var caller = new CompletableFuture<Result>();
        var completion = new CompletableFuture<Void>();
        var stopped = new AtomicBoolean();
        caller.whenComplete((result, failure) -> {
            if (caller.isCancelled() || result != null && result.status() == Status.PENDING) {
                stopped.set(true);
            }
        });
        // The caller deadline includes time waiting in the borrowed cold queue.
        // JDK's shared timeout service changes this waiting future only, never the actual terminal.
        caller.completeOnTimeout(unavailable(Status.PENDING), timeout.toNanos(), TimeUnit.NANOSECONDS);
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            cold.execute(() -> {
                var operation = new Operation(storage, context, maximumAllocations, maximumCompactions,
                        deadline, stopped);
                Result value;
                try {
                    value = operation.run();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    value = operation.result(Status.PENDING, -1, -1);
                } catch (TimeoutException timedOut) {
                    value = operation.result(Status.PENDING, -1, -1);
                } catch (ExecutionException | RuntimeException failed) {
                    operation.failure = failed instanceof ExecutionException ? failed.getCause() : failed;
                    value = operation.result(Status.UNKNOWN, -1, -1);
                } finally {
                    // Stop/cancel/deadline do not abort a queued index scan, COPY, force, owner apply or FREE.
                    operation.child.whenComplete((result, failure) -> {
                        active.set(false);
                        completion.complete(null);
                    });
                }
                caller.complete(value);
            });
        } catch (RuntimeException rejected) {
            active.set(false);
            caller.complete(unavailable(Status.PENDING));
            completion.complete(null);
        }
        return new Attempt(caller, completion.copy());
    }

    private static Result unavailable(Status status) {
        return new Result(status, 0, 0, 0, 0, 0, -1, -1, null);
    }

    private static final class Operation {
        private final IsolatedSegmentStorage storage;
        private final ProfileLedgerContext context;
        private final int maximumAllocations;
        private final int maximumCompactions;
        private final long deadline;
        private final AtomicBoolean stopped;
        private CompletableFuture<?> child = CompletableFuture.completedFuture(null);
        private int scannedAllocations;
        private int compactions;
        private int freed;
        private long sourceBytes;
        private long destinationBytes;
        private Throwable failure;

        private Operation(IsolatedSegmentStorage storage, ProfileLedgerContext context, int maximumAllocations,
                int maximumCompactions, long deadline, AtomicBoolean stopped) {
            this.storage = storage;
            this.context = context;
            this.maximumAllocations = maximumAllocations;
            this.maximumCompactions = maximumCompactions;
            this.deadline = deadline;
            this.stopped = stopped;
        }

        private Result run() throws InterruptedException, ExecutionException, TimeoutException {
            var precondition = storage.physicalDeletePrecondition(context);
            if (precondition != null) {
                return result(precondition, -1, -1);
            }
            if (stop()) {
                return result(Status.PENDING, -1, -1);
            }
            var cleanup = new CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult>();
            var cleanupTerminal = new CompletableFuture<Void>();
            child = cleanupTerminal;
            if (!storage.reclaimTerminalEntries(new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, failure) -> complete(cleanup, result, failure), () -> cleanupTerminal.complete(null)))) {
                cleanup.complete(null);
                cleanupTerminal.complete(null);
                return result(Status.PENDING, -1, -1);
            }
            var cleaned = await(cleanup);
            await(cleanupTerminal);
            if (cleaned == null || cleaned.status() != IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED) {
                return result(cleaned != null
                        && cleaned.status() == IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN
                            ? Status.UNKNOWN : Status.PENDING, -1, -1);
            }
            for (;;) {
                if (stop()) {
                    return result(Status.PENDING, -1, -1);
                }
                var scanned = storage.inspectDeletedInstance(context, maximumAllocations - scannedAllocations);
                child = scanned;
                var observed = await(scanned);
                scannedAllocations = Math.addExact(scannedAllocations, observed.scanned());
                if (observed.complete()) {
                    return result(Status.REUSABLE, observed.bookieCut(), observed.arenaCut());
                }
                if (observed.victims().isEmpty() || compactions == maximumCompactions) {
                    return result(Status.LIMIT, -1, -1);
                }
                if (stop()) {
                    return result(Status.PENDING, -1, -1);
                }
                var compacted = new CompletableFuture<IsolatedSegmentStorage.CompactionResult>();
                var compactTerminal = new CompletableFuture<Void>();
                child = compactTerminal;
                try {
                    if (!storage.compactDeletedWindow(context, observed.victims(),
                            new IsolatedSegmentStorage.CompactionRequest(
                                    (result, failure) -> complete(compacted, result, failure),
                                    () -> compactTerminal.complete(null)))) {
                        compacted.complete(null);
                        compactTerminal.complete(null);
                        return result(Status.PENDING, -1, -1);
                    }
                } catch (java.io.IOException changed) {
                    compacted.complete(null);
                    compactTerminal.complete(null);
                    return result(Status.PENDING, -1, -1);
                }
                var actual = await(compacted);
                await(compactTerminal);
                if (actual == null || actual.status() != IsolatedSegmentStorage.CompactionStatus.RECLAIMED) {
                    return result(actual != null && actual.status() == IsolatedSegmentStorage.CompactionStatus.UNKNOWN
                            ? Status.UNKNOWN : Status.PENDING, -1, -1);
                }
                compactions++;
                freed += actual.freedAllocations();
                sourceBytes = Math.addExact(sourceBytes, actual.freedBytes());
                destinationBytes = Math.addExact(destinationBytes, actual.destinationBytes());
            }
        }

        private boolean stop() {
            return stopped.get() || System.nanoTime() >= deadline;
        }

        private <T> T await(CompletableFuture<T> future)
                throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        }

        private Result result(Status status, long bookieCut, long arenaCut) {
            return new Result(status, scannedAllocations, compactions, freed, sourceBytes, destinationBytes,
                    bookieCut, arenaCut, failure);
        }

        private static <T> void complete(CompletableFuture<T> future, T result, Throwable failure) {
            if (failure == null) {
                future.complete(result);
            } else {
                future.completeExceptionally(failure);
            }
        }
    }
}

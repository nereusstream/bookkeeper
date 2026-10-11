/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.ClientContext;

/** Borrowed cold logical deletion; delivery, access barriers and physical reclaim are separate procedures. */
public final class ProfileDeleteCoordinator {
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ThreadPoolExecutor cold;
    private final Semaphore active = new Semaphore(1);

    public ProfileDeleteCoordinator(ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ThreadPoolExecutor cold) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cold = Objects.requireNonNull(cold, "cold");
    }

    /** Fenced-CLOSED only. Reuse requestId after UNKNOWN; cancellation stops waiting and future stages. */
    public CompletableFuture<ZKProfileReservationStore.DeleteResult> deleteLedgerAsync(long ledger, byte[] requestId,
            ClientContext sdk, Duration maxDuration) {
        return beginDelete(ledger, requestId, sdk, maxDuration).result();
    }

    /** Completion denotes actual cold procedure return, not cancellation of an uncertain remote ZK request. */
    public record Attempt(CompletableFuture<ZKProfileReservationStore.DeleteResult> result,
            CompletableFuture<Void> completion) { }

    public Attempt beginDelete(long ledger, byte[] requestId, ClientContext sdk, Duration maxDuration) {
        byte[] request = ProfileLifecycleHead.fixed(requestId, 16, true);
        if (ledger < 0 || maxDuration.isNegative() || maxDuration.isZero()
                || maxDuration.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Logical deletion requires a bounded cold attempt");
        }
        if (sdk.isClientClosed()) {
            return new Attempt(CompletableFuture.failedFuture(
                    BKException.create(BKException.Code.ClientClosedException)),
                    CompletableFuture.completedFuture(null));
        }
        if (!active.tryAcquire()) {
            return new Attempt(CompletableFuture.completedFuture(unavailable()),
                    CompletableFuture.completedFuture(null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<ZKProfileReservationStore.DeleteResult>();
        var completion = new CompletableFuture<Void>();
        var timer = new AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
        try {
            var actual = CompletableFuture.supplyAsync(() -> reservations.deleteClosed(metadata, ledger, request,
                    () -> !stopped.get() && !sdk.isClientClosed()), cold);
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                completion.complete(null);
                if (failure == null) {
                    caller.complete(result);
                } else {
                    caller.completeExceptionally(failure);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(unavailable());
                }, maxDuration.toNanos(), TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(unavailable());
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return new Attempt(caller, completion.copy());
        } catch (RejectedExecutionException unavailable) {
            active.release();
            return new Attempt(CompletableFuture.completedFuture(unavailable()),
                    CompletableFuture.completedFuture(null));
        }
    }

    private static ZKProfileReservationStore.DeleteResult unavailable() {
        return new ZKProfileReservationStore.DeleteResult(ZKProfileReservationStore.DeleteStatus.UNAVAILABLE, null);
    }
}

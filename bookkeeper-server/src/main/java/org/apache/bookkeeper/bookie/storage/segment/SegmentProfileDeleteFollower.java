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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.discover.profile.ProfileDeleteRecord;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;

/** One bounded online consumer per original registered graph; executor/scheduler/client remain borrowed. */
final class SegmentProfileDeleteFollower {
    private final Object gate = new Object();
    private final IsolatedSegmentStorage storage;
    private final SegmentManifestOwnedService manifestOwner;
    private final SegmentManifestColdPublication manifestPublication;
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final String domain;
    private final ZKProfileEphemeralRegistrationStore.Admission admission;
    private final StorageIncarnation incarnation;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor cold;
    private final int maxEvents;
    private final Duration probeTimeout;
    private final long pollNanos;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private ScheduledFuture<?> scheduled;
    private boolean active;
    private boolean closed;
    private volatile IsolatedSegmentStorage.DeleteCatchUpStatus last;
    private volatile Throwable failure;

    SegmentProfileDeleteFollower(IsolatedSegmentStorage storage, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, ZKProfileEphemeralRegistrationStore.Admission admission,
            StorageIncarnation incarnation, ScheduledExecutorService scheduler, ThreadPoolExecutor cold,
            int maxEvents, Duration probeTimeout, Duration pollInterval) {
        this(Objects.requireNonNull(storage, "storage"), null, null, reservations, metadata, domain, admission,
                incarnation, scheduler, cold, maxEvents, probeTimeout, pollInterval);
    }

    SegmentProfileDeleteFollower(SegmentManifestOwnedService owner, SegmentManifestColdPublication publication,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata, String domain,
            ZKProfileEphemeralRegistrationStore.Admission admission, StorageIncarnation incarnation,
            ScheduledExecutorService scheduler, ThreadPoolExecutor cold, int maxEvents, Duration probeTimeout,
            Duration pollInterval) {
        this(null, Objects.requireNonNull(owner), Objects.requireNonNull(publication), reservations, metadata, domain,
                admission, incarnation, scheduler, cold, maxEvents, probeTimeout, pollInterval);
    }

    private SegmentProfileDeleteFollower(IsolatedSegmentStorage storage, SegmentManifestOwnedService manifestOwner,
            SegmentManifestColdPublication manifestPublication, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, ZKProfileEphemeralRegistrationStore.Admission admission,
            StorageIncarnation incarnation, ScheduledExecutorService scheduler, ThreadPoolExecutor cold,
            int maxEvents, Duration probeTimeout, Duration pollInterval) {
        this.storage = storage;
        this.manifestOwner = manifestOwner;
        this.manifestPublication = manifestPublication;
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.domain = Objects.requireNonNull(domain, "domain");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.incarnation = Objects.requireNonNull(incarnation, "incarnation");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.cold = Objects.requireNonNull(cold, "cold");
        requireBounds(cold, maxEvents, probeTimeout, pollInterval);
        this.maxEvents = maxEvents;
        this.probeTimeout = probeTimeout;
        pollNanos = pollInterval.toNanos();
    }

    static void requireBounds(ThreadPoolExecutor cold, int maxEvents, Duration probeTimeout, Duration pollInterval) {
        if (maxEvents < 1 || maxEvents > 4096 || probeTimeout.isNegative() || probeTimeout.isZero()
                || probeTimeout.compareTo(Duration.ofMinutes(1)) > 0
                || pollInterval.compareTo(Duration.ofMillis(10)) < 0
                || pollInterval.compareTo(Duration.ofMinutes(1)) > 0 || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Online deletion needs finite cold and polling bounds");
        }
    }

    void start() {
        synchronized (gate) {
            schedule(0);
        }
    }

    boolean prepared() {
        synchronized (gate) {
            return !closed && (scheduled != null || active);
        }
    }

    private boolean live() {
        synchronized (gate) {
            return !closed && admission.allowsDeleteFollower(reservations, incarnation);
        }
    }

    private void schedule(long delay) {
        if (closed || active || scheduled != null) {
            return;
        }
        try {
            scheduled = scheduler.schedule(this::dispatch, delay, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException stopped) {
            failure = stopped;
            stop();
        }
    }

    private void dispatch() {
        try {
            synchronized (gate) {
                scheduled = null;
                if (manifestOwner != null && !closed && admission.isPending()) {
                    schedule(pollNanos);
                    return;
                }
                if (!live()) {
                    stop();
                    return;
                }
                active = true;
            }
            cold.execute(this::probe);
        } catch (RejectedExecutionException busy) {
            synchronized (gate) {
                active = false;
                last = IsolatedSegmentStorage.DeleteCatchUpStatus.UNAVAILABLE;
                if (closed) {
                    drained.complete(null);
                } else {
                    schedule(pollNanos);
                }
            }
        } finally {
            notifyManifestRetired();
        }
    }

    private void probe() {
        CompletableFuture<Void> completion = CompletableFuture.completedFuture(null);
        boolean demote = false;
        try {
            if (live()) {
                var attempt = manifestPublication == null ? probeIsolated()
                        : manifestPublication.followG1(domain, maxEvents, probeTimeout, this::live);
                if (attempt != null) {
                    completion = attempt.completion();
                    last = attempt.status();
                }
                if (last == IsolatedSegmentStorage.DeleteCatchUpStatus.QUARANTINED) {
                    closeAsync();
                    demote = true;
                }
            }
        } catch (Throwable stopped) {
            failure = stopped;
            closeAsync();
            demote = true;
        }
        notifyManifestRetired();
        if (demote && manifestOwner != null) {
            manifestOwner.demoteFromFollower(manifestPublication);
        }
        completion.whenComplete((ignored, terminalFailure) -> {
            synchronized (gate) {
                if (terminalFailure != null) {
                    failure = terminalFailure;
                    stop();
                }
                active = false;
                if (closed) {
                    drained.complete(null);
                } else {
                    schedule(last == IsolatedSegmentStorage.DeleteCatchUpStatus.LIMIT ? 0 : pollNanos);
                }
            }
            notifyManifestRetired();
        });
    }

    private IsolatedSegmentStorage.DeleteCatchUpAttempt probeIsolated() {
        var head = reservations.readDeleteHead(new ProfileDeleteRecord.Target(
                admission.bookieId(), incarnation.value()));
        if (head.status() == ProfileControlStore.ReadStatus.NOT_FOUND
                || head.status() == ProfileControlStore.ReadStatus.QUARANTINED) {
            last = IsolatedSegmentStorage.DeleteCatchUpStatus.QUARANTINED;
        } else if (head.status() == ProfileControlStore.ReadStatus.FOUND) {
            return storage.catchUpProfileDeletes(reservations, metadata, domain, maxEvents, probeTimeout, this::live);
        } else {
            last = IsolatedSegmentStorage.DeleteCatchUpStatus.UNAVAILABLE;
        }
        return null;
    }

    private void notifyManifestRetired() {
        if (manifestOwner == null) {
            return;
        }
        synchronized (gate) {
            if (!closed) {
                return;
            }
        }
        manifestOwner.stopReceiving();
    }

    /** Retire admission immediately, but retain accepted cold work and its actual local control terminal. */
    CompletableFuture<Void> closeAsync() {
        synchronized (gate) {
            stop();
            return drained.copy();
        }
    }

    private void stop() {
        closed = true;
        admission.close();
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        if (!active) {
            drained.complete(null);
        }
    }

    IsolatedSegmentStorage.DeleteCatchUpStatus lastStatus() {
        return last;
    }

    Throwable failure() {
        return failure;
    }
}

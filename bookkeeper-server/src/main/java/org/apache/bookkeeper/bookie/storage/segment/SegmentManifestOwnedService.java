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
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

/** One controlled fixed-manifest restart. The caller retains the network and metadata dependencies. */
final class SegmentManifestOwnedService implements AutoCloseable {
    record Startup(SegmentManifestStartupPolicy.PreOpen preflight, SegmentManifestAccessObservation access,
            ProfileLedgerMetadataStore metadata, ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, ZooKeeperWatcherBase watcher, String domain,
            ScheduledExecutorService scheduler, ThreadPoolExecutor cold, int maxEvents, Duration timeout,
            Duration pollInterval, Generation capabilities, List<Integer> hints) {
        Startup {
            Objects.requireNonNull(preflight, "preflight");
            Objects.requireNonNull(access, "access");
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(persistent, "persistent");
            Objects.requireNonNull(publisher, "publisher");
            Objects.requireNonNull(watcher, "watcher");
            Objects.requireNonNull(scheduler, "scheduler");
            Objects.requireNonNull(cold, "cold");
            Objects.requireNonNull(capabilities, "capabilities");
            if (domain == null || domain.isBlank()) {
                throw new IllegalArgumentException("Manifest service needs its original delete domain");
            }
            SegmentProfileDeleteFollower.requireBounds(cold, maxEvents, timeout, pollInterval);
            hints = List.copyOf(hints);
        }
    }

    private final Object gate = new Object();
    private final ReentrantLock closeGate = new ReentrantLock();
    private final ManifestSegmentStorage storage;
    private final Startup startup;
    private final SegmentProfileService.Transport transport;
    private final DiskChecker diskChecker;
    private final Generation readiness;
    private final AtomicReference<IOException> firstCleanupFailure = new AtomicReference<>();
    private boolean attempted;
    private boolean startupFinished;
    private boolean retired;
    private boolean closed;
    private ZKProfileEphemeralRegistrationStore.Admission admission;
    private ManifestSegmentStorage.OwnedListener ownedListener;
    private SegmentManifestColdPublication publication;
    private SegmentProfileDeleteFollower follower;
    private CompletableFuture<Void> recoveryTerminal = CompletableFuture.completedFuture(null);

    SegmentManifestOwnedService(ManifestSegmentStorage storage, Startup startup,
            SegmentProfileService.Transport transport, DiskChecker diskChecker) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.startup = Objects.requireNonNull(startup, "startup");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.diskChecker = Objects.requireNonNull(diskChecker, "diskChecker");
        storage.requireLoopbackTransport(transport);
        readiness = new Generation(transport.readinessGeneration());
    }

    boolean owns(ManifestSegmentStorage candidate) {
        return storage == candidate;
    }

    InetSocketAddress start() throws IOException, InterruptedException {
        requireExternalThread();
        synchronized (gate) {
            if (attempted || retired) {
                throw new IllegalStateException("Manifest service episode is one-use");
            }
            attempted = true;
        }
        long deadline = System.nanoTime() + startup.timeout().toNanos();
        Throwable failed = null;
        try {
            storage.requireOwnedTransport(transport, diskChecker);
            storage.restartForStartupPreflight(startup.preflight(), startup.access(), startup.metadata(), deadline);
            requireLive();
            var recovered = storage.checkRecoveredStartup(startup.domain(), startup.maxEvents());
            recoveryTerminal = recovered.recovery().completion();
            var checked = recovered.checked();
            if (checked == null) {
                throw new IOException("Fixed manifest startup facts unavailable: " + recovered.recovery().status());
            }
            synchronized (gate) {
                requireLive();
                admission = startup.publisher().watchAdmission(startup.watcher());
            }
            var listener = storage.listenOwned(this, checked, admission, transport, deadline);
            synchronized (gate) {
                ownedListener = listener;
                requireLive();
            }
            var bound = listener.listener.bound().get(remaining(deadline), TimeUnit.NANOSECONDS);
            requireLive();
            publication = storage.bindOwnedPublication(listener, startup.persistent(), startup.publisher(),
                    readiness, startup.capabilities(), startup.hints());
            var confirmed = publication.run();
            if (confirmed.reconciled() == null) {
                throw new IOException("Fixed manifest cold publication did not reconcile: " + confirmed.phases());
            }
            synchronized (gate) {
                requireLive();
                follower = new SegmentProfileDeleteFollower(this, publication, startup.preflight().reservations,
                        startup.metadata(), startup.domain(), admission, checked.facts().incarnation(),
                        startup.scheduler(), startup.cold(), startup.maxEvents(), startup.timeout(),
                        startup.pollInterval());
            }
            follower.start();
            if (!admission.openOwnedManifest(publication)) {
                throw new IOException("Original fixed manifest admission retired before first open");
            }
            return bound;
        } catch (TimeoutException | ExecutionException unavailable) {
            failed = new IOException("Fixed manifest service did not complete startup", unavailable);
            throw (IOException) failed;
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            failed = failure;
            throw failure;
        } finally {
            synchronized (gate) {
                startupFinished = true;
            }
            if (failed != null) {
                stopReceiving();
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failed.addSuppressed(new IOException("Manifest service cleanup pending; retain the owner"));
                    }
                } catch (IOException | InterruptedException | RuntimeException cleanup) {
                    if (cleanup != failed) {
                        failed.addSuppressed(cleanup);
                    }
                } finally {
                    if (failed instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    void requirePrepared(SegmentManifestColdPublication candidate) {
        synchronized (gate) {
            if (retired || publication != candidate || follower == null || !follower.prepared()) {
                throw new IllegalStateException("Original manifest follower did not prepare its first poll");
            }
        }
    }

    private void requireLive() throws IOException {
        synchronized (gate) {
            if (retired) {
                throw new IOException("Original manifest service is retired");
            }
        }
    }

    void stopReceiving() {
        ManifestSegmentStorage.OwnedListener listener;
        synchronized (gate) {
            if (admission != null) {
                admission.close();
            }
            retired = true;
            listener = ownedListener;
        }
        transport.budget().closeAdmissionAndDrain();
        if (listener != null) {
            listener.listener.closeAsync();
        }
    }

    void demoteFromFollower(SegmentManifestColdPublication original) {
        try {
            original.demote();
        } catch (RuntimeException failure) {
            if (!(failure.getCause() instanceof TimeoutException)) {
                firstCleanupFailure.compareAndSet(null,
                        new IOException("Manifest follower demotion unconfirmed", failure));
            }
        }
    }

    /** False retains this episode and all dependencies needed by the actual terminal work. */
    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive manifest service drain timeout required");
        }
        requireExternalThread();
        long deadline = System.nanoTime() + timeout.toNanos();
        stopReceiving();
        synchronized (gate) {
            if (attempted && !startupFinished) {
                return false;
            }
        }
        if (!closeGate.tryLock(remaining(deadline), TimeUnit.NANOSECONDS)) {
            return false;
        }
        try {
            if (!closed && !drain(deadline)) {
                return false;
            }
            closed = true;
            var failure = firstCleanupFailure.get();
            if (failure != null) {
                throw failure;
            }
            return true;
        } finally {
            closeGate.unlock();
        }
    }

    private boolean drain(long deadline) throws IOException, InterruptedException {
        if (!await(recoveryTerminal, deadline)
                || follower != null && !await(follower.closeAsync(), deadline)) {
            return false;
        }
        if (publication != null && !publication.drained()) {
            if (remaining(deadline) == 0) {
                return false;
            }
            try {
                publication.demoteUntil(deadline);
            } catch (RuntimeException failure) {
                if (failure.getCause() instanceof TimeoutException) {
                    return false;
                }
                var wrapped = new IOException("Manifest service demotion unconfirmed", failure);
                firstCleanupFailure.compareAndSet(null, wrapped);
                throw wrapped;
            }
            if (!publication.awaitDrained(deadline)) {
                return false;
            }
        }
        if (ownedListener != null && !await(ownedListener.listener.closeAsync(), deadline)) {
            return false;
        }
        long left = remaining(deadline);
        if (left == 0 || !storage.close(Duration.ofNanos(left))) {
            return false;
        }
        if (publication != null) {
            publication.closeLocal();
        }
        return true;
    }

    private boolean await(CompletableFuture<Void> actual, long deadline) throws IOException, InterruptedException {
        try {
            actual.get(remaining(deadline), TimeUnit.NANOSECONDS);
            return true;
        } catch (TimeoutException pending) {
            return false;
        } catch (ExecutionException failure) {
            var wrapped = new IOException("Manifest service actual drain failed", failure.getCause());
            firstCleanupFailure.compareAndSet(null, wrapped);
            throw wrapped;
        }
    }

    private static long remaining(long deadline) {
        return Math.max(0, deadline - System.nanoTime());
    }

    private void requireExternalThread() {
        for (var group : List.of(transport.acceptor(), transport.worker())) {
            for (var loop : group) {
                if (loop.inEventLoop()) {
                    throw new IllegalStateException("Manifest service lifecycle requires an external caller");
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Manifest service cleanup pending; retain the owner");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Manifest service cleanup interrupted", interrupted);
        }
    }
}

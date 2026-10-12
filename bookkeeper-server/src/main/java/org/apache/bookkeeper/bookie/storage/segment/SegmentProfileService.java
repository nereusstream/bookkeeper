/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleListener;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

/**
 * Explicit disposable Profile service episode; owns graph, receipt, admission and listener.
 * Metadata client, TLS, event loops, bounded cold executor and scheduler remain borrowed until actual drain.
 * The supplied new-scope policy still requires its external physical/old-service proof issuer.
 */
public final class SegmentProfileService implements AutoCloseable {
    record Startup(ZKProfileStartupRequirementsStore requirements, StartupReadinessInput requested,
            ZKProfileRegistrationStore persistent, ZKProfileEphemeralRegistrationStore publisher,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ZooKeeperWatcherBase watcher, String domain, ScheduledExecutorService scheduler,
            ThreadPoolExecutor cold, int maxEvents, Duration timeout, Duration pollInterval) {
        Startup {
            Objects.requireNonNull(requirements, "requirements");
            Objects.requireNonNull(requested, "requested");
            Objects.requireNonNull(persistent, "persistent");
            Objects.requireNonNull(publisher, "publisher");
            Objects.requireNonNull(reservations, "reservations");
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(watcher, "watcher");
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(scheduler, "scheduler");
            Objects.requireNonNull(cold, "cold");
            if (domain.isEmpty() || maxEvents < 1 || maxEvents > 4096
                    || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0
                    || pollInterval.compareTo(Duration.ofMillis(10)) < 0
                    || pollInterval.compareTo(Duration.ofMinutes(1)) > 0
                    || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                    || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                    || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
                throw new IllegalArgumentException("Bounded startup/catch-up settings required");
            }
        }
    }

    record Transport(TLSContextFactory tls, ProfileReceiveBudget budget, SegmentProfileListener.Settings settings,
            InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
            SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
        Transport {
            Objects.requireNonNull(tls, "tls");
            Objects.requireNonNull(budget, "budget");
            Objects.requireNonNull(settings, "settings");
            Objects.requireNonNull(address, "address");
            Objects.requireNonNull(acceptor, "acceptor");
            Objects.requireNonNull(worker, "worker");
            if (address.isUnresolved() || address.getAddress().isAnyLocalAddress()) {
                throw new IllegalArgumentException("Explicit resolved Profile endpoint required");
            }
        }
    }

    private final Object gate = new Object();
    private final Object closeGate = new Object();
    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final SegmentExecutionGroup.Settings execution;
    private final IsolatedSegmentStorage.Limits limits;
    private final Startup startup;
    private final Transport transport;
    private IsolatedSegmentStorage storage;
    private SegmentLocalReadinessStore local;
    private ZKProfileEphemeralRegistrationStore.Admission admission;
    private SegmentProfileListener listener;
    private boolean attempted;
    private boolean startupFinished;
    private boolean retired;
    private boolean closed;
    private boolean demoted;
    private boolean lifecycleBound;

    /** No directory, storage constructor, listener or registration is opened here. */
    SegmentProfileService(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings execution,
            IsolatedSegmentStorage.Limits limits, Startup startup, Transport transport) {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.execution = Objects.requireNonNull(execution, "execution");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.startup = Objects.requireNonNull(startup, "startup");
        this.transport = Objects.requireNonNull(transport, "transport");
        if (transport.settings().readinessGeneration() != startup.requested().targetReadinessGeneration().value()
                || transport.control() != null && transport.control().executor() != startup.cold()) {
            throw new IllegalArgumentException("Listener must use this readiness generation and cold executor");
        }
    }

    /** Blocking cold caller only. Failed starts retain this owner if real cleanup cannot yet finish. */
    InetSocketAddress start(boolean existing) throws IOException, InterruptedException {
        requireExternalThread();
        synchronized (gate) {
            if (attempted || retired) {
                throw new IllegalStateException("Profile service episode already attempted or retired");
            }
            attempted = true;
        }
        Throwable failed = null;
        try {
            var source = startup.requirements().read(identity.bookieId().toString());
            if (source.status() != ZKProfileStartupRequirementsStore.Status.FOUND) {
                throw new IOException("Startup requirements unavailable: " + source.status());
            }
            var observed = source.observation().orElseThrow();
            var policy = IsolatedSegmentStorage.startupPolicy(observed, startup.requested(), config, identity);
            requireStorageRoots(policy.newScopeFallback());
            startup.publisher().bindStartupRequirements(startup.requirements(), observed);
            requireLive();
            storage = new IsolatedSegmentStorage(config, identity, key, validator, execution, limits);
            startup.publisher().bindDeleteDelivery(startup.reservations(), identity.incarnation(),
                    storage::profileDeleteProgress);
            synchronized (gate) {
                requireLive();
                admission = startup.publisher().watchAdmission(startup.watcher());
                storage.bindProfileRegistration(admission);
            }
            startup.publisher().requireStartupStores(admission, startup.persistent(), startup.reservations(),
                    identity.incarnation());
            requireLive();
            if (existing) {
                storage.restart();
            } else {
                storage.startNew();
            }
            requireLive();
            local = new SegmentLocalReadinessStore(config.directory(), identity.bookieId().toString(),
                    identity.incarnation());
            listener = new SegmentProfileListener(storage, transport.tls(), transport.budget(), transport.settings(),
                    transport.address(), transport.acceptor(), transport.worker(),
                    transport.control(), transport.read());
            InetSocketAddress bound = listener.bound().get(startup.timeout().toNanos(), TimeUnit.NANOSECONDS);
            // The published address comes from the actual native bind, including an ephemeral port.
            String host = bound.getAddress().getHostAddress();
            String endpoint = "tls://" + (host.contains(":") ? "[" + host + "]" : host) + ":" + bound.getPort();
            var boundPolicy = new StartupReadinessInput(policy.compatibility(), policy.requiredDevices(),
                    policy.recoveryFacts(), policy.newScopeFallback(), policy.rollbackSafetyFacts(),
                    policy.targetReadinessGeneration(), policy.capabilityGeneration(), policy.runtimeReaderVersion(),
                    policy.runtimeWriterVersion(), policy.supportedMandatoryFeatures(), endpoint,
                    policy.capabilityHints());
            requireLive();
            var result = storage.reconcileReadiness(boundPolicy, local, startup.persistent(), startup.publisher(),
                    startup.reservations(), startup.metadata(), startup.domain(), startup.scheduler(), startup.cold(),
                    startup.maxEvents(), startup.timeout(), startup.pollInterval(), this::isLive);
            result.completion().get(startup.timeout().toNanos(), TimeUnit.NANOSECONDS);
            if (result.readiness() == null || result.readiness().outcome() != StartupReadinessResult.Outcome.WRITABLE) {
                throw new IOException("Profile service not writable: " + result.deleteStatus()
                        + "/" + result.readiness());
            }
            synchronized (gate) {
                requireLive();
                if (!admission.getAsBoolean()) {
                    throw new IOException("Profile service admission retired during startup");
                }
                return bound;
            }
        } catch (ExecutionException | TimeoutException unavailable) {
            failed = new IOException("Profile service startup did not complete", unavailable);
            throw (IOException) failed;
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            failed = failure;
            throw failure;
        } finally {
            synchronized (gate) {
                startupFinished = true;
            }
            if (failed != null) {
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failed.addSuppressed(new IOException("Profile service cleanup incomplete; retain this owner"));
                    }
                } catch (IOException | InterruptedException | RuntimeException cleanup) {
                    failed.addSuppressed(cleanup);
                }
                if (failed instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean isLive() {
        synchronized (gate) {
            return !retired;
        }
    }

    /** Bind the external root proof to the only two filesystem roots opened by this isolated graph. */
    private void requireStorageRoots(NewScopeFallbackManifest fallback) throws IOException {
        var arena = canonicalPlannedPath(config.directory());
        var index = canonicalPlannedPath(SegmentDerivedIndex.directory(config));
        for (var root : fallback.newRoots()) {
            var expected = switch (root.kind()) {
                case ARENA -> arena;
                case INDEX -> index;
                default -> null; // The isolated graph has no Classic journal or ledger storage.
            };
            if (expected != null && !expected.equals(canonicalPlannedPath(Path.of(root.opaquePath())))) {
                throw new IOException("New-scope root proof belongs to another " + root.kind());
            }
        }
        for (var root : fallback.oldRoots()) {
            var old = canonicalPlannedPath(Path.of(root.opaquePath()));
            if (arena.startsWith(old) || old.startsWith(arena) || index.startsWith(old) || old.startsWith(index)) {
                throw new IOException("New storage overlaps an old-scope root");
            }
        }
    }

    private static Path canonicalPlannedPath(Path path) throws IOException {
        if (!path.isAbsolute()) {
            throw new IOException("Native new-scope proof requires absolute roots");
        }
        path = path.normalize();
        Path existing = path;
        while (!Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        return existing.toRealPath().resolve(existing.relativize(path));
    }

    /** One explicit experimental EmbeddedServer component; no storage is opened while assembling the stack. */
    public LifecycleComponent lifecycle(boolean existing) {
        synchronized (gate) {
            if (lifecycleBound || attempted || retired) {
                throw new IllegalStateException("Profile service lifecycle must be bound once before startup");
            }
            lifecycleBound = true;
            return new ServiceLifecycle(existing);
        }
    }

    private final class ServiceLifecycle implements LifecycleComponent {
        private final boolean existing;
        private final Set<LifecycleListener> listeners = new CopyOnWriteArraySet<>();
        private volatile Lifecycle.State state = Lifecycle.State.INITIALIZED;
        private volatile Thread.UncaughtExceptionHandler handler;

        private ServiceLifecycle(boolean existing) {
            this.existing = existing;
        }

        @Override
        public String getName() {
            return "experimental-segment-profile";
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return state;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void setExceptionHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void start() {
            synchronized (gate) {
                if (state == Lifecycle.State.STARTED) {
                    return;
                }
                if (state != Lifecycle.State.INITIALIZED || attempted || retired) {
                    throw new IllegalStateException("Use a fresh Profile episode to restart storage");
                }
            }
            listeners.forEach(LifecycleListener::beforeStart);
            try {
                SegmentProfileService.this.start(existing);
                synchronized (gate) {
                    requireLive();
                    state = Lifecycle.State.STARTED;
                }
            } catch (IOException | InterruptedException | RuntimeException | Error failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                var notify = handler;
                if (notify != null) {
                    try {
                        notify.uncaughtException(Thread.currentThread(), failure);
                    } catch (RuntimeException callbackFailure) {
                        failure.addSuppressed(callbackFailure);
                    }
                }
                throw new IllegalStateException("Profile service failed to start", failure);
            }
            listeners.forEach(LifecycleListener::afterStart);
        }

        @Override
        public void stop() {
            if (state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED) {
                return;
            }
            listeners.forEach(LifecycleListener::beforeStop);
            // Do not mark terminal or let the stack release borrowed resources before actual drain.
            drain();
            synchronized (gate) {
                if (state != Lifecycle.State.CLOSED) {
                    state = Lifecycle.State.STOPPED;
                }
            }
            listeners.forEach(LifecycleListener::afterStop);
        }

        private void drain() {
            try {
                SegmentProfileService.this.close();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }

        @Override
        public void close() {
            if (state == Lifecycle.State.CLOSED) {
                return;
            }
            stop();
            listeners.forEach(LifecycleListener::beforeClose);
            drain();
            synchronized (gate) {
                state = Lifecycle.State.CLOSED;
            }
            listeners.forEach(LifecycleListener::afterClose);
        }
    }

    private void requireLive() throws IOException {
        if (!isLive()) {
            throw new IOException("Profile service episode retired");
        }
    }

    private void requireExternalThread() {
        for (var loop : transport.worker()) {
            if (loop.inEventLoop()) {
                throw new IllegalStateException("Profile service lifecycle requires an external cold caller");
            }
        }
        for (var loop : transport.acceptor()) {
            if (loop.inEventLoop()) {
                throw new IllegalStateException("Profile service lifecycle requires an external cold caller");
            }
        }
    }

    /** Local drain deadline; metadata demotion also has its own store timeout. False retains all live owners. */
    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive service drain timeout required");
        }
        requireExternalThread();
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (gate) {
            retired = true;
            if (admission != null) {
                admission.close();
            }
            transport.budget().closeAdmissionAndDrain();
            if (attempted && !startupFinished) {
                return false;
            }
        }
        synchronized (closeGate) {
            if (closed) {
                return true;
            }
            IOException demotionFailure = null;
            if (!demoted && admission != null) {
                try {
                    startup.publisher().demote(identity.bookieId().toString());
                    demoted = true;
                } catch (RuntimeException unconfirmed) {
                    demotionFailure = new IOException("Profile registration demotion unconfirmed", unconfirmed);
                }
            }
            if (listener != null) {
                try {
                    listener.closeAsync().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (TimeoutException pending) {
                    if (demotionFailure != null) {
                        throw demotionFailure;
                    }
                    return false;
                } catch (ExecutionException failure) {
                    throw new IOException("Profile listener drain failed", failure.getCause());
                }
            }
            long remaining = deadline - System.nanoTime();
            if (storage != null && (remaining <= 0 || !storage.close(Duration.ofNanos(remaining)))) {
                if (demotionFailure != null) {
                    throw demotionFailure;
                }
                return false;
            }
            if (local != null) {
                local.close();
            }
            if (demotionFailure != null) {
                throw demotionFailure;
            }
            closed = true;
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Profile service drain incomplete; retain this owner and retry");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Profile service drain interrupted", interrupted);
        }
    }
}

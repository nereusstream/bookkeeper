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
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleListener;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer;
import org.apache.bookkeeper.common.profile.control.ProfileStaticOperationAuthorizer;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.util.DiskChecker;

/** One deferred loopback restart, taking exclusive ownership of a fresh graph and resource owner. */
public final class SegmentManifestResourceService implements AutoCloseable {
    private final Object gate = new Object();
    private final ReentrantLock closeGate = new ReentrantLock();
    private final ManifestSegmentStorage storage;
    private final SegmentManifestStartupPolicy policy;
    private final IsolatedSegmentConfig runtime;
    private final SegmentManifestAccessObservation access;
    private final Generation capabilities;
    private final List<Integer> hints;
    private final SegmentProfileOwnedResources resources;
    private final DiskChecker diskChecker;
    private boolean attempted;
    private boolean startupFinished;
    private boolean retired;
    private boolean lifecycleBound;
    private SegmentManifestOwnedService service;
    private boolean closed;
    private IOException firstCleanupFailure;

    private SegmentManifestResourceService(ManifestSegmentStorage storage, SegmentManifestStartupPolicy policy,
            IsolatedSegmentConfig runtime, SegmentManifestAccessObservation access, Generation capabilities,
            List<Integer> hints, SegmentProfileOwnedResources resources, DiskChecker diskChecker) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.access = Objects.requireNonNull(access, "access");
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.hints = List.copyOf(hints);
        this.resources = Objects.requireNonNull(resources, "resources");
        this.diskChecker = Objects.requireNonNull(diskChecker, "diskChecker");
    }

    /** Freeze local bounds for the supplied manifest scope. Actual startup owns all file and metadata I/O. */
    static SegmentManifestResourceService fromConfiguration(ServerConfiguration configuration, SecretKey key,
            ProfileDescriptorValidator validator, SegmentManifestStartupPolicy policy,
            SegmentManifestAccessObservation access, SegmentProfileOwnedResources.ResourceSettings options,
            ProfileReceiveBudget budget, SegmentProfileListener.Settings listener, InetSocketAddress address,
            ProfileStaticOperationAuthorizer staticAuthorizer, ProfileControlAuthorizer exactAuthorizer,
            SegmentProfileReadConfig read, Generation capabilities, List<Integer> hints) throws IOException {
        var frozen = new ServerConfiguration(configuration);
        if (!SegmentProfileService.isolatedConfigurationEnabled(frozen)) {
            throw new IllegalArgumentException("Experimental Segment configuration is OFF");
        }
        for (String field : List.of("Directory", "LogicalShards", "DataFiles", "BytesPerFile")) {
            String keyName = SegmentProfileService.CONFIG_PREFIX + field;
            if (frozen.containsKey(keyName)) {
                throw new IllegalArgumentException("Fixed manifest supplies topology instead of " + keyName);
            }
        }
        var manifest = policy.manifest();
        var first = manifest.arenas().get(0);
        var defaults = IsolatedSegmentConfig.defaults(manifest.controlDirectory());
        // The Bookie runtime carries local geometry. The manifest supplies each actual DATA member.
        var runtime = SegmentProfileService.configuredRuntime(frozen, defaults, manifest.shardArenas().size(),
                first.dataFiles(), first.bytesPerFile());
        var execution = SegmentProfileService.configuredExecution(frozen);
        var limits = SegmentProfileService.configuredLimits(frozen);
        access.requirePolicyScope(policy);
        var storage = new ManifestSegmentStorage(manifest, manifest.identity(), runtime, key, validator,
                execution, limits, budget);
        var resources = new SegmentProfileOwnedResources(frozen, options, budget, listener, address,
                staticAuthorizer, exactAuthorizer, read, validator);
        var disk = new DiskChecker(frozen.getDiskUsageThreshold(), frozen.getDiskUsageWarnThreshold());
        return new SegmentManifestResourceService(storage, policy, runtime, access, capabilities, hints,
                resources, disk);
    }

    InetSocketAddress start() throws IOException, InterruptedException {
        resources.requireExternalThread();
        synchronized (gate) {
            if (attempted || retired) {
                throw new IllegalStateException("Owned manifest resource episode is one-use");
            }
            attempted = true;
        }
        boolean started = false;
        try {
            var environment = resources.openManifest(policy, runtime, access, capabilities, hints);
            synchronized (gate) {
                requireLive();
                service = new SegmentManifestOwnedService(storage, environment.startup(), environment.transport(),
                        diskChecker);
            }
            var address = service.start();
            synchronized (gate) {
                requireLive();
                started = true;
            }
            return address;
        } finally {
            synchronized (gate) {
                startupFinished = true;
                if (!started) {
                    retire();
                }
            }
        }
    }

    private void requireLive() throws IOException {
        if (retired) {
            throw new IOException("Owned manifest resource episode is retired");
        }
    }

    private void retire() {
        retired = true;
        if (service == null) {
            resources.retire();
        } else {
            service.stopReceiving();
        }
    }

    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive owned manifest resource drain timeout required");
        }
        resources.requireExternalThread();
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (gate) {
            retire();
            if (attempted && !startupFinished) {
                return false;
            }
        }
        if (!closeGate.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            return false;
        }
        try {
            if (!closed) {
                try {
                    if (!drain(deadline)) {
                        return false;
                    }
                } catch (IOException failure) {
                    retainCleanupFailure(failure);
                    throw firstCleanupFailure;
                }
                closed = true;
            }
            if (firstCleanupFailure != null) {
                throw firstCleanupFailure;
            }
            return true;
        } finally {
            closeGate.unlock();
        }
    }

    private boolean drain(long deadline) throws IOException, InterruptedException {
        if (service != null) {
            try {
                if (!service.closeUntil(deadline)) {
                    return false;
                }
            } catch (IOException failure) {
                if (!service.isDrained()) {
                    throw failure;
                }
                retainCleanupFailure(failure);
            }
        }
        return resources.close(deadline);
    }

    private void retainCleanupFailure(IOException failure) {
        if (firstCleanupFailure == null) {
            firstCleanupFailure = failure;
        }
    }

    /** Bind this prepared owner once for explicit experimental server assembly. */
    public LifecycleComponent lifecycle() {
        resources.requireExternalThread();
        synchronized (gate) {
            if (lifecycleBound || attempted || retired) {
                throw new IllegalStateException("Manifest lifecycle must be bound once before startup");
            }
            lifecycleBound = true;
            return new ManifestLifecycle();
        }
    }

    private final class ManifestLifecycle implements LifecycleComponent {
        private final Set<LifecycleListener> listeners = new CopyOnWriteArraySet<>();
        private volatile Lifecycle.State state = Lifecycle.State.INITIALIZED;
        private volatile Thread.UncaughtExceptionHandler handler;

        @Override
        public String getName() {
            return "experimental-segment-profile-manifest";
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
            resources.requireExternalThread();
            synchronized (gate) {
                if (state == Lifecycle.State.STARTED && !retired) {
                    return;
                }
                if (state != Lifecycle.State.INITIALIZED || attempted || retired) {
                    throw new IllegalStateException("Use a fresh manifest episode to restart storage");
                }
            }
            listeners.forEach(LifecycleListener::beforeStart);
            try {
                SegmentManifestResourceService.this.start();
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
                        if (callbackFailure != failure) {
                            failure.addSuppressed(callbackFailure);
                        }
                    }
                }
                throw new IllegalStateException("Manifest Profile service failed to start", failure);
            }
            listeners.forEach(LifecycleListener::afterStart);
        }

        @Override
        public void stop() {
            resources.requireExternalThread();
            if (state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED) {
                return;
            }
            listeners.forEach(LifecycleListener::beforeStop);
            try {
                SegmentManifestResourceService.this.close();
            } catch (IOException failure) {
                // Keep the component and the stack's earlier dependencies until the original owner drains.
                throw new UncheckedIOException(failure);
            }
            synchronized (gate) {
                if (state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED) {
                    return;
                }
                state = Lifecycle.State.STOPPED;
            }
            listeners.forEach(LifecycleListener::afterStop);
        }

        @Override
        public void close() {
            resources.requireExternalThread();
            if (state == Lifecycle.State.CLOSED) {
                return;
            }
            stop();
            listeners.forEach(LifecycleListener::beforeClose);
            synchronized (gate) {
                if (state == Lifecycle.State.CLOSED) {
                    return;
                }
                state = Lifecycle.State.CLOSED;
            }
            listeners.forEach(LifecycleListener::afterClose);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Owned manifest resource cleanup pending; retain the owner");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Owned manifest resource cleanup interrupted", interrupted);
        }
    }
}

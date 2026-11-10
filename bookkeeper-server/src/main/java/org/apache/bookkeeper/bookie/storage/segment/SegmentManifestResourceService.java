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
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.util.DiskChecker;

/** One deferred loopback restart, taking exclusive ownership of a fresh graph and resource owner. */
final class SegmentManifestResourceService implements AutoCloseable {
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
    private SegmentManifestOwnedService service;
    private boolean closed;
    private IOException firstCleanupFailure;

    SegmentManifestResourceService(ManifestSegmentStorage storage, SegmentManifestStartupPolicy policy,
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

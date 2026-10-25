/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.discover.profile;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.bookkeeper.net.BookieId;

/** One bounded registered peer snapshot at a time. Group and cold executor remain borrowed.
 * Reservations include discovery, connecting, borrowed and actual draining work; no background retries. */
public final class ProfileRecoveryPeerPool {
    /** Exclusive custody transferred once to an SDK owner; borrowed executors remain external. */
    public final class Owner {
        private Owner() { }

        public Acquisition acquire() {
            return acquireOwned();
        }

        public CompletableFuture<Void> closeAsync() {
            return closeOwned();
        }
    }

    public record Limits(int connections, int rpcSlots, int perConnectionSlots,
            int maxEntryBytes, Duration timeout) {
        public Limits {
            if (connections < 2 || connections > 128 || rpcSlots < 2 || rpcSlots > 4096
                    || perConnectionSlots < 1 || perConnectionSlots > ProfileReservation.MAX_INITIAL_TARGETS
                    || maxEntryBytes < 36 || maxEntryBytes > 1024 * 1024 + 36
                    || timeout.compareTo(Duration.ofMillis(1)) < 0 || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("Invalid bounded recovery peer capacity");
            }
        }
    }

    public record Usage(int reservedConnections, int reservedRpcSlots, boolean borrowed, boolean closing) { }

    public final class Lease {
        private final Acquisition acquisition;
        private final Map<BookieId, ProfileNormalDataClient> data;
        private final Map<String, ProfileInitialControlClient> controls;
        private boolean released;

        private Lease(Acquisition acquisition) {
            this.acquisition = acquisition;
            data = Map.copyOf(acquisition.data);
            controls = Map.copyOf(acquisition.controls);
        }

        public synchronized Map<BookieId, ProfileNormalDataClient> data() {
            requireLive();
            return data;
        }

        public synchronized Map<String, ProfileInitialControlClient> controls() {
            requireLive();
            return controls;
        }

        private void requireLive() {
            synchronized (ProfileRecoveryPeerPool.this) {
                if (released || acquisition.stopped) {
                    throw new IllegalStateException("Peer snapshot lease has been released");
                }
            }
        }

        /** Caller releases only after its actual recovery child is terminal. */
        public CompletableFuture<Void> closeAsync() {
            synchronized (this) {
                released = true;
            }
            return acquisition.closeAsync();
        }
    }

    public final class Acquisition {
        private final CompletableFuture<Lease> completion = new CompletableFuture<>();
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        private final List<ProfileSocketClient.RegisteredConnection<?>> connections = new ArrayList<>();
        private final Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
        private final Map<String, ProfileInitialControlClient> controls = new HashMap<>();
        private boolean stopped;
        private boolean actualDone;
        private boolean delivered;
        private boolean drainStarted;

        private Acquisition() { }

        public CompletableFuture<Lease> completion() {
            var waiter = completion.copy();
            waiter.whenComplete((value, failure) -> {
                if (waiter.isCancelled()) {
                    closeAsync();
                }
            });
            return waiter;
        }

        public CompletableFuture<Void> closeAsync() {
            List<ProfileSocketClient.RegisteredConnection<?>> owned;
            synchronized (ProfileRecoveryPeerPool.this) {
                stopped = true;
                owned = List.copyOf(connections);
            }
            owned.forEach(c -> c.closeAsync());
            startDrainIfReady();
            completion.completeExceptionally(unavailable());
            return drained.copy();
        }

        private void startDrainIfReady() {
            List<ProfileSocketClient.RegisteredConnection<?>> owned;
            synchronized (ProfileRecoveryPeerPool.this) {
                if (!stopped || !actualDone || drainStarted) {
                    return;
                }
                drainStarted = true;
                owned = List.copyOf(connections);
            }
            CompletableFuture.allOf(owned.stream().map(c -> c.closeAsync())
                    .toArray(CompletableFuture[]::new)).whenComplete((value, failure) -> {
                        if (failure != null) {
                            drained.completeExceptionally(failure);
                            closed.completeExceptionally(failure);
                            return;
                        }
                        synchronized (ProfileRecoveryPeerPool.this) {
                            if (active == this) {
                                active = null;
                            }
                        }
                        drained.complete(null);
                        finishPoolClose();
                    });
        }
    }

    private final ZKProfileRegistrationStore registry;
    private final EventLoopGroup group;
    private final SslContext tls;
    private final ThreadPoolExecutor cold;
    private final List<BookieId> cohort;
    private final Limits limits;
    private final Map<BookieId, byte[]> incarnations = new HashMap<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private Acquisition active;
    private boolean closing;
    private boolean used;
    private Owner owner;

    public ProfileRecoveryPeerPool(ZKProfileRegistrationStore registry, EventLoopGroup group, SslContext tls,
            ThreadPoolExecutor cold, List<BookieId> cohort, Limits limits) {
        this.registry = Objects.requireNonNull(registry);
        this.group = Objects.requireNonNull(group);
        this.tls = Objects.requireNonNull(tls);
        this.cold = Objects.requireNonNull(cold);
        this.limits = Objects.requireNonNull(limits);
        this.cohort = cohort.stream().distinct().sorted(java.util.Comparator.comparing(BookieId::toString)).toList();
        if (!tls.isClient() || this.cohort.isEmpty() || this.cohort.size() != cohort.size() || cohort.size() > 64
                || 2 * cohort.size() > limits.connections()
                || 2 * cohort.size() * limits.perConnectionSlots() > limits.rpcSlots()
                || !(cold.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue<?>)
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Recovery pool requires bounded unique cohort, capacity and cold owner");
        }
    }

    /** Transfer only a pristine pool. Rejection leaves the caller's ownership intact. */
    public synchronized Owner claimOwnership() {
        if (owner != null || used || closing || active != null) {
            throw new IllegalStateException("Recovery pool must be unused and exclusively owned");
        }
        owner = new Owner();
        return owner;
    }

    /** A synchronous drain on one of these event loops would wait for itself. */
    public boolean inEventLoop() {
        for (var executor : group) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    public Acquisition acquire() {
        synchronized (this) {
            if (owner != null) {
                throw new IllegalStateException("Recovery pool ownership was transferred");
            }
            used = true;
        }
        return acquireOwned();
    }

    private Acquisition acquireOwned() {
        var acquisition = new Acquisition();
        synchronized (this) {
            if (closing || active != null) {
                acquisition.actualDone = true;
                acquisition.stopped = true;
            } else {
                used = true;
                active = acquisition; // Reserve the entire cohort before issuing any discovery/connect task.
            }
        }
        if (acquisition.stopped) {
            acquisition.closeAsync();
            return acquisition;
        }
        var flow = CompletableFuture.<Void>completedFuture(null);
        try {
            for (var bookie : cohort) {
                flow = flow.thenCompose(ignored -> connectPeer(acquisition, bookie));
            }
        } catch (RuntimeException failedAdmission) {
            flow = CompletableFuture.failedFuture(failedAdmission);
        }
        flow.whenComplete((value, failure) -> {
            Lease lease = null;
            synchronized (this) {
                acquisition.actualDone = true;
                if (failure == null && !acquisition.stopped && !closing && !acquisition.data.isEmpty()) {
                    acquisition.delivered = true;
                    lease = new Lease(acquisition);
                } else {
                    acquisition.stopped = true;
                }
            }
            if (lease == null) {
                acquisition.closeAsync();
            } else if (!acquisition.completion.complete(lease)) {
                lease.closeAsync();
            }
        });
        return acquisition;
    }

    private CompletableFuture<Void> connectPeer(Acquisition acquisition, BookieId bookie) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this) {
                if (acquisition.stopped || closing) {
                    throw new IllegalStateException("Recovery pool acquisition stopped");
                }
            }
            var found = registry.readPeer(bookie.toString());
            if (found.status() == org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore
                    .ReadStatus.CORRUPT) {
                throw new IllegalStateException("Corrupt recovery peer observation");
            }
            return found.peer();
        }, cold).thenCompose(peer -> {
            if (peer == null) {
                return CompletableFuture.completedFuture(null);
            }
            ProfileSocketClient.RegisteredConnection<ProfileInitialControlClient> control;
            ProfileSocketClient.RegisteredConnection<ProfileNormalDataClient> data;
            synchronized (this) {
                if (acquisition.stopped || closing) {
                    return CompletableFuture.failedFuture(unavailable());
                }
                byte[] prior = incarnations.get(bookie);
                if (prior != null && !Arrays.equals(prior, peer.projection().storageIncarnation().value())) {
                    return CompletableFuture.failedFuture(new IOException("Same-ID incarnation rejoin is disabled"));
                }
                control = ProfileInitialControlClient.connectRegistered(group, tls, registry, peer, cold,
                        limits.perConnectionSlots(), limits.timeout());
                acquisition.connections.add(control);
                data = ProfileNormalDataClient.connectRegistered(group, tls, registry, peer, cold,
                        limits.perConnectionSlots(), limits.maxEntryBytes(), limits.timeout());
                acquisition.connections.add(data);
            }
            var controlReady = control.completion();
            var dataReady = data.completion();
            return CompletableFuture.allOf(controlReady, dataReady).handle((value, failure) -> failure)
                    .thenCompose(failure -> {
                        if (failure != null) {
                            return CompletableFuture.allOf(control.closeAsync(), data.closeAsync());
                        }
                        synchronized (this) {
                            if (!acquisition.stopped && !closing) {
                                incarnations.putIfAbsent(bookie, peer.projection().storageIncarnation().value());
                                acquisition.controls.put(bookie.toString(), controlReady.join());
                                acquisition.data.put(bookie, dataReady.join());
                            }
                        }
                        return CompletableFuture.completedFuture(null);
                    });
        });
    }

    public synchronized Usage usage() {
        int reserved = active == null ? 0 : 2 * cohort.size();
        return new Usage(reserved, reserved * limits.perConnectionSlots(),
                active != null && active.delivered, closing);
    }

    /** Sampled actual reserved ByteBuf capacities across the owned cohort, including closing connections. */
    public synchronized long backingBytes() {
        return active == null ? 0 : active.connections.stream()
                .mapToLong(ProfileSocketClient.RegisteredConnection::backingBytes).sum();
    }

    public synchronized long codecWorkspaceBytes() {
        return active == null ? 0 : active.connections.stream()
                .mapToLong(ProfileSocketClient.RegisteredConnection::codecWorkspaceBytes).sum();
    }

    /** Fixed transport-backing ceiling for the currently reserved CONTROL/DATA cohort; excludes opaque memory. */
    public synchronized long backingLimitBytes() {
        return active == null ? 0 : Math.multiplyExact((long) cohort.size(), Math.addExact(
                (long) ProfileSocketClient.backingLimit(false, limits.perConnectionSlots(), 0),
                ProfileSocketClient.backingLimit(true, limits.perConnectionSlots(), limits.maxEntryBytes())));
    }

    /** Pool close stops new acquisitions. A delivered lease remains pinned until its owner releases it. */
    public CompletableFuture<Void> closeAsync() {
        synchronized (this) {
            if (owner != null) {
                throw new IllegalStateException("Recovery pool ownership was transferred");
            }
            closing = true;
        }
        return closeOwned();
    }

    private CompletableFuture<Void> closeOwned() {
        Acquisition stop;
        synchronized (this) {
            closing = true;
            stop = active != null && !active.delivered ? active : null;
        }
        if (stop != null) {
            stop.closeAsync();
        }
        finishPoolClose();
        return closed.copy();
    }

    private void finishPoolClose() {
        boolean done;
        synchronized (this) {
            done = closing && active == null;
        }
        if (done) {
            closed.complete(null);
        }
    }

    private static IOException unavailable() {
        return new IOException("Registered recovery peer snapshot unavailable");
    }
}

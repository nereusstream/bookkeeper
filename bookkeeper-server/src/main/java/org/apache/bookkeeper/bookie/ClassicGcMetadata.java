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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.KeeperException;

/** GC activity on a real episode's borrowed metadata resources. Never startup or deletion authority. */
final class ClassicGcMetadata {
    record Drain(boolean drained, int pendingReads, int heldLocks, int actualBodies, Throwable firstFailure) { }

    private static final class Read {
        final CompletableFuture<Versioned<LedgerMetadata>> result = new CompletableFuture<>();
        boolean unknown;
    }

    static final class Lock {
        private final ClassicGcMetadata owner;
        private final long ledger;
        private boolean acquired;
        private boolean releaseAttempted;
        private boolean released;
        private Throwable failure;

        private Lock(ClassicGcMetadata owner, long ledger) {
            this.owner = owner;
            this.ledger = ledger;
        }
    }

    private final AbstractZkLedgerManager manager;
    private final ZkLedgerUnderreplicationManager replication;
    private final int limit;
    private final Set<Read> reads = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<Long, Lock> locks = new HashMap<>();
    private final Map<Thread, Integer> bodies = new IdentityHashMap<>();
    private boolean closing;
    private Throwable firstFailure;

    /**
     * Heap-only. The enclosing episode owns these same-namespace sources and the dedicated replication manager,
     * and retains them through this child and the actual GC executor. This child never closes either source.
     */
    ClassicGcMetadata(AbstractZkLedgerManager manager, ZkLedgerUnderreplicationManager replication, int limit) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.replication = Objects.requireNonNull(replication, "replication");
        if (limit <= 0) {
            throw new IllegalArgumentException("Finite positive GC metadata concurrency required");
        }
        this.limit = limit;
        // Only the episode's dedicated, unused manager is passed here. Reuse its exact-session cleanup guards.
        replication.enableProfileCleanupGuards();
    }

    AbstractZkLedgerManager manager() {
        return manager;
    }

    CompletableFuture<Versioned<LedgerMetadata>> read(long ledger) {
        Read read = new Read();
        synchronized (this) {
            if (closing || firstFailure != null || reads.size() >= limit) {
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.MetaStoreException));
            }
            reads.add(read);
            try {
                enterBody();
            } catch (RuntimeException | Error failure) {
                reads.remove(read); // No metadata submission happened yet.
                throw failure;
            }
        }
        try {
            // Registered before submission. A timeout on result never cancels this source or its actual callback.
            var original = Objects.requireNonNull(manager.readLedgerMetadata(ledger), "metadata future");
            original.whenComplete((value, failure) -> {
                enterBody();
                try {
                    if (failure == null) {
                        read.result.complete(value);
                    } else {
                        read.result.completeExceptionally(failure);
                    }
                } finally {
                    synchronized (ClassicGcMetadata.this) {
                        reads.remove(read);
                        leaveBody();
                    }
                }
            });
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                // If submission or callback attachment is unknown, keep the original slot until actually observed.
                if (reads.contains(read)) {
                    read.unknown = true;
                }
                failed(failure);
            }
            read.result.completeExceptionally(failure);
        } finally {
            leaveBody();
        }
        return read.result;
    }

    boolean isBeingReplicated(long ledger) throws Exception {
        synchronized (this) {
            requireAdmission();
            enterBody();
        }
        try {
            return replication.isLedgerBeingReplicated(ledger);
        } finally {
            leaveBody();
        }
    }

    Lock acquire(long ledger) throws Exception {
        Lock captured = new Lock(this, ledger);
        synchronized (this) {
            requireAdmission();
            if (locks.size() >= limit || locks.putIfAbsent(ledger, captured) != null) {
                throw new IOException("GC lock capacity or the original ledger obligation is still occupied");
            }
            enterBody();
        }
        try {
            replication.acquireUnderreplicatedLedger(ledger);
            captured.acquired = true;
            return captured;
        } catch (Exception | Error failure) {
            synchronized (this) {
                if (occupied(failure)) {
                    locks.remove(ledger, captured); // The guarded atomic create reported definite non-effect.
                } else {
                    captured.failure = failure;
                    failed(failure);
                }
            }
            throw failure;
        } finally {
            leaveBody();
        }
    }

    void release(Lock captured) throws Exception {
        if (captured == null || captured.owner != this) {
            throw new IOException("GC release does not belong to this original metadata owner");
        }
        synchronized (captured) {
            if (captured.released) {
                return;
            }
            if (!captured.acquired || captured.releaseAttempted) {
                throw new IOException("GC lock has an unresolved original acquire or release", captured.failure);
            }
            captured.releaseAttempted = true;
            enterBody();
            try {
                replication.releaseUnderreplicatedLedger(captured.ledger);
                synchronized (this) {
                    captured.released = true;
                    locks.remove(captured.ledger, captured);
                    notifyAll();
                }
            } catch (Exception | Error failure) {
                synchronized (this) {
                    captured.failure = failure;
                    failed(failure);
                }
                throw failure;
            } finally {
                leaveBody();
            }
        }
    }

    synchronized void failed(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        }
        notifyAll();
    }

    synchronized void retainUnknown(Lock captured, Throwable failure) {
        if (captured != null && locks.get(captured.ledger) == captured && !captured.released) {
            captured.failure = failure;
        }
        failed(failure);
    }

    private void requireAdmission() throws IOException {
        if (closing || firstFailure != null) {
            throw new IOException("GC metadata admission is closed or failed", firstFailure);
        }
    }

    private synchronized void enterBody() {
        Thread current = Thread.currentThread();
        bodies.put(current, bodies.getOrDefault(current, 0) + 1);
    }

    private synchronized void leaveBody() {
        Thread current = Thread.currentThread();
        int remaining = bodies.get(current) - 1;
        if (remaining == 0) {
            bodies.remove(current);
        } else {
            bodies.put(current, remaining);
        }
        notifyAll();
    }

    synchronized boolean isCurrentActivity() {
        return bodies.containsKey(Thread.currentThread());
    }

    synchronized Drain state() {
        return new Drain(closing && reads.isEmpty() && locks.isEmpty() && bodies.isEmpty(),
                reads.size(), locks.size(), bodies.values().stream().mapToInt(Integer::intValue).sum(), firstFailure);
    }

    /** Called after the actual GC executor returns. Unknown callbacks/locks stay owned, never retried or cancelled. */
    Drain closeAndDrain() throws InterruptedException {
        List<Lock> unused;
        synchronized (this) {
            if (isCurrentActivity()) {
                throw new IllegalStateException("GC metadata activity cannot drain its own original body");
            }
            closing = true;
            try {
                while (!bodies.isEmpty() || reads.stream().anyMatch(read -> !read.unknown)) {
                    wait();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
            if (!reads.isEmpty()) {
                return state();
            }
            unused = locks.values().stream()
                    .filter(lock -> lock.acquired && !lock.releaseAttempted && lock.failure == null).toList();
        }
        // Only known, unused leases get their first release. No native metadata call runs under the owner monitor.
        for (Lock lock : unused) {
            try {
                release(lock);
            } catch (Exception | Error failure) {
                failed(failure);
            }
        }
        return state();
    }

    static boolean occupied(Throwable failure) {
        for (int depth = 0; failure != null && depth < 8; depth++, failure = failure.getCause()) {
            if (failure instanceof KeeperException.NodeExistsException) {
                return true;
            }
        }
        return false;
    }
}

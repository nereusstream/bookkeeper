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
package org.apache.bookkeeper.client;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool;

/** One SDK-owned cold open or CLOSED reader, retaining its peers through actual local drain. */
final class ProfileReadOnlyClient {
    private final ClientContext sdk;
    private final ProfileRecoveryCoordinator coordinator;
    private final ProfileRecoveryPeerPool pool;
    private final ProfileRecoveryPeerPool.Owner peers;
    private final int maxReadEntries;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private Open active;
    private boolean closing;
    private boolean poolCloseStarted;

    ProfileReadOnlyClient(ClientContext sdk, ProfileRecoveryCoordinator coordinator,
            ProfileRecoveryPeerPool pool, int maxReadEntries) {
        this.sdk = sdk;
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.pool = Objects.requireNonNull(pool, "ownedPool");
        if (maxReadEntries < 1 || maxReadEntries > 256 || sdk.getConf().useV2WireProtocol) {
            throw new IllegalArgumentException("Profile readonly requires bounded reads and the v3 SDK protocol");
        }
        this.maxReadEntries = maxReadEntries;
        peers = pool.claimOwnership(); // Last fallible admission step; success transfers exclusive custody.
    }

    CompletableFuture<LedgerHandle> open(long ledger, byte[] password) {
        Open operation;
        synchronized (this) {
            if (closing) {
                return failed(BKException.Code.ClientClosedException);
            }
            if (active != null) {
                return failed(BKException.Code.TooManyRequestsException);
            }
            if (ledger < 0 || password == null) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            operation = new Open(ledger, password.clone());
            active = operation;
        }
        operation.start();
        return operation.result;
    }

    /** Called before the SDK marks itself closed; failure must leave the SDK usable. */
    synchronized void checkBlockingClose() throws BKException {
        if (!closed.isDone() && active != null && (pool.inEventLoop()
                || sdk.getMainWorkerPool().getThreadID(active.ledger) == Thread.currentThread().getId())) {
            throw BKException.create(BKException.Code.IllegalOpException);
        }
    }

    CompletableFuture<Void> closeAsync() {
        Open stop;
        synchronized (this) {
            closing = true;
            stop = active;
        }
        if (stop != null) {
            stop.stop();
        }
        finishClose();
        return closed.copy();
    }

    private void finishClose() {
        synchronized (this) {
            if (!closing || active != null || poolCloseStarted) {
                return;
            }
            poolCloseStarted = true;
        }
        peers.closeAsync().whenComplete((unused, failure) -> {
            if (failure == null) {
                closed.complete(null);
            } else {
                closed.completeExceptionally(failure);
            }
        });
    }

    private static CompletableFuture<LedgerHandle> failed(int code) {
        return CompletableFuture.failedFuture(BKException.create(code));
    }

    private final class Open {
        private final long ledger;
        private final byte[] password;
        private final CompletableFuture<Void> terminal = new CompletableFuture<>();
        private final CompletableFuture<LedgerHandle> result = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                synchronized (ProfileReadOnlyClient.this) {
                    if (claimed) {
                        return false;
                    }
                    stopped = true;
                    cancelling = true;
                }
                stop();
                try {
                    return super.cancel(mayInterruptIfRunning);
                } finally {
                    synchronized (ProfileReadOnlyClient.this) {
                        cancelling = false;
                    }
                }
            }
        };
        private ProfileRecoveryPeerPool.Acquisition acquisition;
        private ProfileRecoveryPeerPool.Lease lease;
        private ProfileRecoveryCoordinator.ReadOnlyOperation cold;
        private CompletableFuture<Void> coldDrained;
        private LedgerHandle handle;
        private boolean stopped;
        private boolean claimed;
        private boolean cancelling;
        private boolean cleanupStarted;

        Open(long ledger, byte[] password) {
            this.ledger = ledger;
            this.password = password;
        }

        void start() {
            ProfileRecoveryPeerPool.Acquisition started;
            synchronized (ProfileReadOnlyClient.this) {
                if (stopped || closing) {
                    started = null;
                } else {
                    acquisition = peers.acquire();
                    started = acquisition;
                }
            }
            if (started == null) {
                failAfterCleanup(BKException.create(BKException.Code.ClientClosedException));
                return;
            }
            started.completion().whenComplete(this::acquired);
        }

        private void acquired(ProfileRecoveryPeerPool.Lease acquired, Throwable failure) {
            if (failure != null) {
                failAfterCleanup(failure);
                return;
            }
            ProfileRecoveryCoordinator.ReadOnlyOperation started = null;
            Throwable rejected = null;
            synchronized (ProfileReadOnlyClient.this) {
                lease = acquired;
                if (!stopped && !closing && !result.isDone()) {
                    try {
                        cold = coordinator.beginOpenReadOnly(ledger, sdk, password, acquired.data(), maxReadEntries);
                        coldDrained = cold.drained(); // These mutable core futures never escape this owner.
                        started = cold;
                    } catch (RuntimeException admissionFailure) {
                        rejected = admissionFailure;
                    }
                }
            }
            if (started == null) {
                failAfterCleanup(rejected == null
                        ? BKException.create(BKException.Code.ClientClosedException) : rejected);
            } else {
                started.completion().whenComplete(this::opened);
            }
        }

        private void opened(LedgerHandle opened, Throwable failure) {
            if (failure != null) {
                failAfterCleanup(failure);
                return;
            }
            boolean deliver;
            CompletableFuture<Void> localDrained;
            synchronized (ProfileReadOnlyClient.this) {
                handle = opened;
                localDrained = ProfileLedgerHandle.adoptClosedRead(opened, terminal,
                        () -> pool.inEventLoop()
                            || sdk.getMainWorkerPool().getThreadID(ledger) == Thread.currentThread().getId());
                deliver = !stopped && !closing && !result.isDone();
                claimed = deliver;
            }
            localDrained.whenComplete((unused, closeFailure) -> {
                if (closeFailure == null) {
                    cleanup();
                } else {
                    finish(closeFailure);
                }
            });
            if (!deliver || !result.complete(opened)) {
                opened.asyncClose((code, ignored, ctx) -> { }, null);
            }
        }

        private void failAfterCleanup(Throwable failure) {
            cleanup();
            terminal.whenComplete((unused, closeFailure) -> {
                synchronized (ProfileReadOnlyClient.this) {
                    if (cancelling) {
                        return;
                    }
                }
                result.completeExceptionally(closeFailure == null ? failure : closeFailure);
            });
        }

        void stop() {
            ProfileRecoveryCoordinator.ReadOnlyOperation stopCold;
            ProfileRecoveryPeerPool.Acquisition stopAcquisition;
            LedgerHandle stopHandle;
            synchronized (ProfileReadOnlyClient.this) {
                stopped = true;
                stopCold = cold;
                stopAcquisition = acquisition;
                stopHandle = handle;
            }
            if (stopHandle != null) {
                stopHandle.asyncClose((code, ignored, ctx) -> { }, null);
            } else if (stopCold != null) {
                stopCold.cancel();
            } else if (stopAcquisition != null) {
                stopAcquisition.closeAsync();
            }
        }

        private void cleanup() {
            CompletableFuture<Void> actual;
            ProfileRecoveryPeerPool.Lease release;
            ProfileRecoveryPeerPool.Acquisition discard;
            synchronized (ProfileReadOnlyClient.this) {
                if (cleanupStarted) {
                    return;
                }
                cleanupStarted = true;
                actual = coldDrained == null ? CompletableFuture.completedFuture(null) : coldDrained;
                release = lease;
                discard = acquisition;
            }
            actual.thenCompose(unused -> release != null ? release.closeAsync()
                    : discard != null ? discard.closeAsync() : CompletableFuture.completedFuture(null))
                    .whenComplete((unused, failure) -> finish(failure));
        }

        private void finish(Throwable failure) {
            synchronized (ProfileReadOnlyClient.this) {
                if (failure == null) {
                    if (active == this) {
                        active = null;
                    }
                }
            }
            // Reentrant public close callbacks may immediately open the next reader.
            if (failure == null) {
                terminal.complete(null);
                finishClose();
            } else {
                terminal.completeExceptionally(failure);
                closed.completeExceptionally(failure); // Preserve active custody; do not tear down SDK dependencies.
            }
        }
    }
}

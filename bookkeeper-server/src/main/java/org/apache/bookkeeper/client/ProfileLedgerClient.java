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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInitialMetadataCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;

/** One SDK-owned create, cold open or handle, retaining its peers through actual local drain. */
final class ProfileLedgerClient {
    private final ClientContext sdk;
    private final ProfileInitialLifecycleCoordinator initializer;
    private final ProfileRecoveryCoordinator coordinator;
    private final ProfileDescriptorValidator validator;
    private final ProfileRecoveryPeerPool pool;
    private final ProfileRecoveryPeerPool.Owner peers;
    private final int maxReadEntries;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private Operation active;
    private PendingCreate pendingCreate;
    private boolean closing;
    private boolean poolCloseStarted;

    private record PendingCreate(ProfileCreateRequest request, ZKProfileReservationStore.FreshInitialClaim claim) { }

    ProfileLedgerClient(ClientContext sdk, ProfileRecoveryCoordinator coordinator,
            ProfileRecoveryPeerPool pool, int maxReadEntries) {
        this(sdk, null, coordinator, null, pool, maxReadEntries);
    }

    ProfileLedgerClient(ClientContext sdk, ProfileInitialLifecycleCoordinator initializer,
            ProfileRecoveryCoordinator coordinator, ProfileDescriptorValidator validator,
            ProfileRecoveryPeerPool pool, int maxReadEntries) {
        this.sdk = sdk;
        this.initializer = initializer;
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.validator = validator;
        this.pool = Objects.requireNonNull(pool, "ownedPool");
        if (maxReadEntries < 1 || maxReadEntries > 256 || sdk.getConf().useV2WireProtocol
                || (initializer == null) != (validator == null)
                || initializer != null && (!sdk.getConf().delayEnsembleChange || sdk.getConf().explicitLacInterval != 0
                    || sdk.getConf().enforceMinNumFaultDomainsForWrite)) {
            throw new IllegalArgumentException("Profile requires bounded reads and the supported SDK configuration");
        }
        this.maxReadEntries = maxReadEntries;
        peers = pool.claimOwnership(); // Last fallible admission step; success transfers exclusive custody.
    }

    CompletableFuture<LedgerHandle> open(long ledger, byte[] password) {
        Operation operation;
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
            operation = new Operation(ledger, password.clone(), null, null, null);
            active = operation;
        }
        operation.start();
        return operation.result;
    }

    CompletableFuture<LedgerHandle> create(ProfileCreateRequest request) {
        Operation operation;
        synchronized (this) {
            if (closing) {
                return failed(BKException.Code.ClientClosedException);
            }
            if (initializer == null) {
                return failed(BKException.Code.IllegalOpException);
            }
            if (active != null) {
                return failed(BKException.Code.TooManyRequestsException);
            }
            if (request == null) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            ProfileDescriptor descriptor;
            try {
                descriptor = request.validateDescriptor(validator);
            } catch (IllegalArgumentException invalid) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            if (pendingCreate != null && !pendingCreate.request().equals(request)) {
                if (pendingCreate.request().desired().getLedgerId() == request.desired().getLedgerId()) {
                    return failed(BKException.Code.LedgerExistException);
                }
                pendingCreate = null; // Drop local custody only; never delete the old persistent origin.
            }
            operation = new Operation(request.desired().getLedgerId(), null, request, descriptor,
                    pendingCreate == null ? null : pendingCreate.claim());
            active = operation;
        }
        operation.start();
        return operation.result;
    }

    private boolean cannotWait(long ledger) {
        long thread = Thread.currentThread().getId();
        return pool.inEventLoop() || sdk.getMainWorkerPool().getThreadID(ledger) == thread
                || sdk.getScheduler() != null && sdk.getScheduler().getThreadID(ledger) == thread;
    }

    /** Called before the SDK marks itself closed; failure must leave the SDK usable. */
    synchronized void checkBlockingClose() throws BKException {
        if (!closed.isDone() && active != null && cannotWait(active.ledger)) {
            throw BKException.create(BKException.Code.IllegalOpException);
        }
    }

    CompletableFuture<Void> closeAsync() {
        Operation stop;
        synchronized (this) {
            closing = true;
            pendingCreate = null;
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

    private final class Operation {
        private final long ledger;
        private final byte[] password;
        private final ProfileCreateRequest request;
        private final ProfileDescriptor descriptor;
        private final ZKProfileReservationStore.FreshInitialClaim retainedClaim;
        private final CompletableFuture<Void> terminal = new CompletableFuture<>();
        private final CompletableFuture<LedgerHandle> result = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                synchronized (ProfileLedgerClient.this) {
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
                    synchronized (ProfileLedgerClient.this) {
                        cancelling = false;
                    }
                }
            }
        };
        private ProfileRecoveryPeerPool.Acquisition acquisition;
        private ProfileRecoveryPeerPool.Lease lease;
        private ProfileRecoveryCoordinator.ReadOnlyOperation cold;
        private CompletableFuture<Void> operationDrained;
        private CompletableFuture<Void> handleDrained;
        private LedgerHandle handle;
        private boolean createStarted;
        private boolean writerAdopted;
        private boolean stopped;
        private boolean claimed;
        private boolean cancelling;
        private boolean cleanupStarted;

        Operation(long ledger, byte[] password, ProfileCreateRequest request, ProfileDescriptor descriptor,
                ZKProfileReservationStore.FreshInitialClaim retainedClaim) {
            this.ledger = ledger;
            this.password = password;
            this.request = request;
            this.descriptor = descriptor;
            this.retainedClaim = retainedClaim;
        }

        void start() {
            ProfileRecoveryPeerPool.Acquisition started;
            synchronized (ProfileLedgerClient.this) {
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
            Throwable rejected = null;
            synchronized (ProfileLedgerClient.this) {
                lease = acquired;
                if (stopped || closing || result.isDone()) {
                    rejected = BKException.create(BKException.Code.ClientClosedException);
                } else if (request != null) {
                    createStarted = true;
                    operationDrained = new CompletableFuture<>();
                } else {
                    try {
                        cold = coordinator.beginOpenReadOnly(ledger, sdk, password, acquired, maxReadEntries);
                        operationDrained = cold.drained(); // Mutable core futures never escape this owner.
                    } catch (RuntimeException admissionFailure) {
                        rejected = admissionFailure;
                    }
                }
            }
            if (rejected != null) {
                failAfterCleanup(rejected);
            } else if (request != null) {
                createWithLease();
            } else {
                cold.completion().whenComplete(this::opened);
            }
        }

        private void createWithLease() {
            CompletableFuture<LedgerHandle> work;
            try {
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                var controls = new HashMap<String, ProfileInitialControlClient>();
                var installers = new HashMap<String, ProfileInitialMetadataCoordinator.Target>();
                var activators = new HashMap<String, ProfileInitialLifecycleCoordinator.Target>();
                for (var scope : request.reservation().installs()) {
                    var bookie = BookieId.parse(scope.targetBookie());
                    var dataPeer = lease.data().get(bookie);
                    var controlPeer = lease.controls().get(scope.targetBookie());
                    if (dataPeer == null || controlPeer == null) {
                        throw BKException.create(BKException.Code.NotEnoughBookiesException);
                    }
                    requirePeer(dataPeer.hello(), scope.targetBookie(), scope.targetStorageIncarnation());
                    requirePeer(controlPeer.hello(), scope.targetBookie(), scope.targetStorageIncarnation());
                    data.put(bookie, dataPeer);
                    controls.put(scope.targetBookie(), controlPeer);
                    installers.put(scope.targetBookie(), new ProfileInitialMetadataCoordinator.Target(
                            controlPeer.hello(), controlPeer));
                    activators.put(scope.targetBookie(), new ProfileInitialLifecycleCoordinator.Target(
                            controlPeer.hello(), controlPeer));
                }
                var claim = retainedClaim == null ? claimFresh()
                        : CompletableFuture.completedFuture(retainedClaim);
                work = claim.thenCompose(fresh -> {
                    synchronized (ProfileLedgerClient.this) {
                        if (stopped || closing || result.isDone()) {
                            return CompletableFuture.failedFuture(
                                    BKException.create(BKException.Code.ClientClosedException));
                        }
                    }
                    return initializer.initializeFresh(fresh, request.desired(), request.reservation(),
                            request.descriptor(), request.credential(), Map.copyOf(installers), Map.copyOf(activators),
                            request.readyOperationId(), request.readyPayloadIdentity());
                }).thenCompose(initial -> configureWriter(initial, Map.copyOf(data), Map.copyOf(controls)));
            } catch (Exception rejected) {
                work = CompletableFuture.failedFuture(rejected);
            }
            // This private chain is never cancelled, including during SDK shutdown or caller cancellation.
            work.whenComplete((created, failure) -> {
                if (failure != null) {
                    stopHandle();
                }
                operationDrained.complete(null);
                if (failure != null) {
                    failAfterCleanup(failure);
                } else {
                    publish(created);
                }
            });
        }

        private CompletableFuture<ZKProfileReservationStore.FreshInitialClaim> claimFresh() {
            synchronized (ProfileLedgerClient.this) {
                if (stopped || closing || result.isDone()) {
                    return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
                }
            }
            return initializer.claimFresh(request.reservation()).thenApply(fresh -> {
                if (fresh.claim() == null) {
                    throw new CompletionException(BKException.create(switch (fresh.status()) {
                        case ALREADY_APPLIED, CONFLICT -> BKException.Code.LedgerExistException;
                        case QUARANTINED -> BKException.Code.MetadataVersionException;
                        case DURABILITY_UNKNOWN, UNAVAILABLE -> BKException.Code.TimeoutException;
                        case APPLIED -> throw new IllegalStateException("Fresh create omitted its claim");
                    }));
                }
                synchronized (ProfileLedgerClient.this) {
                    if (!closing) {
                        pendingCreate = new PendingCreate(request, fresh.claim());
                    }
                }
                return fresh.claim();
            });
        }

        private void requirePeer(ProfileHello.Server hello, String bookie, byte[] incarnation) throws BKException {
            if (hello.activeEngine() != 3 || !hello.bookieId().equals(bookie)
                    || !Arrays.equals(hello.storageIncarnation(), incarnation)
                    || descriptor.mandatoryCapabilities().stream().anyMatch(capability ->
                        !hello.capabilities().contains(new ProfileHello.Capability(
                            capability.capabilityId(), capability.semanticVersion())))) {
                throw BKException.create(BKException.Code.MetadataVersionException);
            }
        }

        private CompletableFuture<LedgerHandle> configureWriter(ProfileInitialLifecycleCoordinator.Result initial,
                Map<BookieId, ProfileNormalDataClient> data, Map<String, ProfileInitialControlClient> controls) {
            if (initial.status() != ProfileInitialLifecycleCoordinator.Status.AVAILABLE) {
                return failed(switch (initial.status()) {
                    case NOT_READY -> BKException.Code.NotEnoughBookiesException;
                    case UNCERTAIN -> BKException.Code.TimeoutException;
                    case CONFLICT -> BKException.Code.MetadataVersionException;
                    case QUARANTINED -> BKException.Code.DigestMatchException;
                    case DELETED -> BKException.Code.NoSuchLedgerExistsException;
                    case AVAILABLE -> throw new IllegalStateException("Availability branch was not handled");
                });
            }
            ProfileLedgerHandle writer;
            var limits = request.limits();
            synchronized (ProfileLedgerClient.this) {
                if (stopped || closing || result.isDone()) {
                    return failed(BKException.Code.ClientClosedException);
                }
                try {
                    writer = ProfileLedgerHandle.fromInitialAvailability(sdk, initial, data,
                            limits.maxPendingEntries(), limits.maxPayloadBytes(), limits.maxAttempts(),
                            limits.deadline(), limits.backoff());
                } catch (java.security.GeneralSecurityException failedDigest) {
                    return CompletableFuture.failedFuture(failedDigest);
                }
                handle = writer;
            }
            return writer.enableWriterClose(coordinator, request.descriptor(), validator, controls,
                    limits.maxCloseCoordinates(), limits.closeDuration()).thenApply(unused -> {
                CompletableFuture<Void> actual;
                synchronized (ProfileLedgerClient.this) {
                    if (stopped || closing || result.isDone()) {
                        throw new CompletionException(BKException.create(BKException.Code.ClientClosedException));
                    }
                    actual = writer.adoptWriterOwnership(terminal, () -> cannotWait(ledger));
                    handleDrained = actual;
                    writerAdopted = true;
                }
                actual.whenComplete(this::handleTerminal);
                return writer;
            });
        }

        private void opened(LedgerHandle opened, Throwable failure) {
            if (failure != null) {
                failAfterCleanup(failure);
                return;
            }
            CompletableFuture<Void> localDrained;
            synchronized (ProfileLedgerClient.this) {
                handle = opened;
                localDrained = ProfileLedgerHandle.adoptClosedRead(opened, terminal, () -> cannotWait(ledger));
                handleDrained = localDrained;
            }
            localDrained.whenComplete(this::handleTerminal);
            publish(opened);
        }

        private void publish(LedgerHandle opened) {
            boolean deliver;
            synchronized (ProfileLedgerClient.this) {
                deliver = !stopped && !closing && !result.isDone();
                claimed = deliver;
                if (deliver && request != null) {
                    pendingCreate = null; // Handoff may now be visible; this origin can never recreate a writer.
                }
            }
            if (!deliver) {
                stopHandle();
                failAfterCleanup(BKException.create(BKException.Code.ClientClosedException));
            } else if (!result.complete(opened)) {
                stopHandle();
            }
        }

        private void handleTerminal(Void unused, Throwable failure) {
            if (failure == null) {
                cleanup();
            } else {
                finish(failure);
            }
        }

        private void failAfterCleanup(Throwable failure) {
            cleanup();
            terminal.whenComplete((unused, closeFailure) -> {
                synchronized (ProfileLedgerClient.this) {
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
            boolean initializing;
            synchronized (ProfileLedgerClient.this) {
                stopped = true;
                stopCold = cold;
                stopAcquisition = acquisition;
                initializing = createStarted;
            }
            if (stopHandle()) {
                return;
            }
            if (stopCold != null) {
                stopCold.cancel();
            } else if (!initializing && stopAcquisition != null) {
                stopAcquisition.closeAsync();
            }
        }

        private boolean stopHandle() {
            LedgerHandle stopHandle;
            boolean adopted;
            synchronized (ProfileLedgerClient.this) {
                stopHandle = handle;
                adopted = writerAdopted;
            }
            if (stopHandle == null) {
                return false;
            }
            if (request == null) {
                stopHandle.asyncClose((code, ignored, ctx) -> { }, null);
            } else {
                var writer = (ProfileLedgerHandle) stopHandle;
                var actual = adopted ? writer.stopOwnedWrites() : writer.stopLocalWrites();
                synchronized (ProfileLedgerClient.this) {
                    if (handleDrained == null) {
                        handleDrained = actual;
                    }
                }
            }
            return true;
        }

        private void cleanup() {
            CompletableFuture<Void> actual;
            ProfileRecoveryPeerPool.Lease release;
            ProfileRecoveryPeerPool.Acquisition discard;
            synchronized (ProfileLedgerClient.this) {
                if (cleanupStarted) {
                    return;
                }
                cleanupStarted = true;
                var work = operationDrained == null ? CompletableFuture.<Void>completedFuture(null) : operationDrained;
                actual = handleDrained == null ? work : CompletableFuture.allOf(work, handleDrained);
                release = lease;
                discard = acquisition;
            }
            actual.thenCompose(unused -> release != null ? release.closeAsync()
                    : discard != null ? discard.closeAsync() : CompletableFuture.completedFuture(null))
                    .whenComplete((unused, failure) -> finish(failure));
        }

        private void finish(Throwable failure) {
            synchronized (ProfileLedgerClient.this) {
                if (failure == null && active == this) {
                    active = null;
                }
            }
            // Reentrant public close callbacks may immediately create/open the next handle.
            if (failure == null) {
                terminal.complete(null);
                finishClose();
            } else {
                terminal.completeExceptionally(failure);
                closed.completeExceptionally(failure); // Preserve active custody and SDK dependencies.
            }
        }
    }
}

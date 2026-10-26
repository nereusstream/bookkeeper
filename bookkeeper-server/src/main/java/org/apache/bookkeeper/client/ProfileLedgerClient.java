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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.bookkeeper.discover.profile.ProfileReplacementCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileReplacementPage;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;

/** Bounded SDK-owned ledgers sharing one peer cohort through each child's actual local drain. */
final class ProfileLedgerClient {
    private final ClientContext sdk;
    private final ProfileInitialLifecycleCoordinator initializer;
    private final ProfileRecoveryCoordinator coordinator;
    private final ProfileReplacementCoordinator replacement;
    private final ProfileRecoveryCoordinator.ReadOnlyAdmission readOnlyAdmission;
    private final ProfileDescriptorValidator validator;
    private final ProfileRecoveryPeerPool pool;
    private final ProfileRecoveryPeerPool.Owner peers;
    private final int maxReadEntries;
    private final int maxOpenLedgers;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final Map<Long, Operation> active = new HashMap<>();
    private final Map<Long, PendingCreate> pendingCreates = new LinkedHashMap<>();
    private Epoch epoch;
    private boolean closing;
    private boolean poolCloseStarted;

    private record PendingCreate(ProfileCreateRequest request, ZKProfileReservationStore.FreshInitialClaim claim) { }

    ProfileLedgerClient(ClientContext sdk, ProfileRecoveryCoordinator coordinator,
            ProfileRecoveryPeerPool pool, int maxReadEntries) {
        this(sdk, coordinator, pool, maxReadEntries, 1);
    }

    ProfileLedgerClient(ClientContext sdk, ProfileRecoveryCoordinator coordinator,
            ProfileRecoveryPeerPool pool, int maxReadEntries, int maxOpenLedgers) {
        this(sdk, null, coordinator, null, pool, maxReadEntries, maxOpenLedgers);
    }

    ProfileLedgerClient(ClientContext sdk, ProfileInitialLifecycleCoordinator initializer,
            ProfileRecoveryCoordinator coordinator, ProfileDescriptorValidator validator,
            ProfileRecoveryPeerPool pool, int maxReadEntries) {
        this(sdk, initializer, coordinator, validator, pool, maxReadEntries, 1);
    }

    ProfileLedgerClient(ClientContext sdk, ProfileInitialLifecycleCoordinator initializer,
            ProfileRecoveryCoordinator coordinator, ProfileDescriptorValidator validator,
            ProfileRecoveryPeerPool pool, int maxReadEntries, int maxOpenLedgers) {
        this(sdk, initializer, coordinator, null, validator, pool, maxReadEntries, maxOpenLedgers);
    }

    ProfileLedgerClient(ClientContext sdk, ProfileInitialLifecycleCoordinator initializer,
            ProfileRecoveryCoordinator coordinator, ProfileReplacementCoordinator replacement,
            ProfileDescriptorValidator validator, ProfileRecoveryPeerPool pool, int maxReadEntries,
            int maxOpenLedgers) {
        this.sdk = sdk;
        this.initializer = initializer;
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.replacement = replacement;
        this.validator = validator;
        this.pool = Objects.requireNonNull(pool, "ownedPool");
        if (maxReadEntries < 1 || maxReadEntries > 256 || maxOpenLedgers < 1
                || sdk.getConf().useV2WireProtocol
                || (initializer == null) != (validator == null) || initializer == null && replacement != null
                || initializer != null && (!sdk.getConf().delayEnsembleChange || sdk.getConf().explicitLacInterval != 0
                    || sdk.getConf().enforceMinNumFaultDomainsForWrite)) {
            throw new IllegalArgumentException("Profile requires bounded reads and the supported SDK configuration");
        }
        this.maxReadEntries = maxReadEntries;
        this.maxOpenLedgers = maxOpenLedgers;
        readOnlyAdmission = coordinator.readOnlyAdmission(maxOpenLedgers);
        peers = pool.claimOwnership(); // Last fallible admission step; success transfers exclusive custody.
    }

    CompletableFuture<LedgerHandle> open(long ledger, byte[] password) {
        Operation operation;
        synchronized (this) {
            if (closing) {
                return failed(BKException.Code.ClientClosedException);
            }
            if (active.containsKey(ledger) || active.size() >= maxOpenLedgers
                    || epoch != null && epoch.retiring) {
                return failed(BKException.Code.TooManyRequestsException);
            }
            if (ledger < 0 || password == null) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            operation = new Operation(ledger, password.clone(), null, null, null);
            admit(operation);
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
            if (request == null) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            if (request.maxEnsembleChanges() > 0 && replacement == null) {
                return failed(BKException.Code.IllegalOpException);
            }
            long ledger = request.desired().getLedgerId();
            if (active.containsKey(ledger) || active.size() >= maxOpenLedgers
                    || epoch != null && epoch.retiring) {
                return failed(BKException.Code.TooManyRequestsException);
            }
            ProfileDescriptor descriptor;
            try {
                descriptor = request.validateDescriptor(validator);
            } catch (IllegalArgumentException invalid) {
                return failed(BKException.Code.IncorrectParameterException);
            }
            var pending = pendingCreates.get(ledger);
            if (pending != null && !pending.request().equals(request)) {
                return failed(BKException.Code.LedgerExistException);
            }
            if (pending == null) {
                discardIdlePendingCreate();
            }
            operation = new Operation(ledger, null, request, descriptor, pending == null ? null : pending.claim());
            admit(operation);
        }
        operation.start();
        return operation.result;
    }

    private void admit(Operation operation) {
        if (epoch == null) {
            epoch = new Epoch();
        }
        operation.cohort = epoch;
        active.put(operation.ledger, operation);
    }

    /** Drop only bounded idle local retry custody, never its persistent origin or an active claim. */
    private void discardIdlePendingCreate() {
        if (pendingCreates.size() < maxOpenLedgers) {
            return;
        }
        var idle = pendingCreates.keySet().iterator();
        while (idle.hasNext()) {
            if (!active.containsKey(idle.next())) {
                idle.remove();
                return;
            }
        }
        throw new IllegalStateException("No idle Profile create context can be released");
    }

    private boolean cannotWait(long ledger) {
        long thread = Thread.currentThread().getId();
        return pool.inEventLoop() || sdk.getMainWorkerPool().getThreadID(ledger) == thread
                || sdk.getScheduler() != null && sdk.getScheduler().getThreadID(ledger) == thread;
    }

    /** Called before the SDK marks itself closed; failure must leave the SDK usable. */
    synchronized void checkBlockingClose() throws BKException {
        if (!closed.isDone() && active.keySet().stream().anyMatch(this::cannotWait)) {
            throw BKException.create(BKException.Code.IllegalOpException);
        }
    }

    CompletableFuture<Void> closeAsync() {
        List<Operation> stop;
        synchronized (this) {
            closing = true;
            pendingCreates.clear();
            stop = List.copyOf(active.values());
        }
        stop.forEach(Operation::stop);
        finishClose();
        return closed.copy();
    }

    private void finishClose() {
        synchronized (this) {
            if (!closing || !active.isEmpty() || epoch != null || poolCloseStarted) {
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

    private final class Epoch {
        private final CompletableFuture<ProfileRecoveryPeerPool.Lease> ready = new CompletableFuture<>();
        private ProfileRecoveryPeerPool.Acquisition acquisition;
        private ProfileRecoveryPeerPool.Lease lease;
        private boolean retiring;

        void start() {
            ProfileRecoveryPeerPool.Acquisition started;
            synchronized (ProfileLedgerClient.this) {
                if (acquisition != null || retiring) {
                    return;
                }
                acquisition = peers.acquire();
                started = acquisition;
            }
            started.completion().whenComplete((acquired, failure) -> {
                synchronized (ProfileLedgerClient.this) {
                    if (failure == null) {
                        lease = acquired;
                    }
                }
                if (failure == null) {
                    ready.complete(acquired);
                } else {
                    ready.completeExceptionally(failure);
                }
            });
        }

        void stopIfUnneeded() {
            ProfileRecoveryPeerPool.Acquisition stop;
            synchronized (ProfileLedgerClient.this) {
                if (lease != null || active.values().stream().anyMatch(child -> child.cohort == this
                        && !child.stopped)) {
                    return;
                }
                retiring = true; // Stop admission while the last children observe acquisition termination.
                stop = acquisition;
            }
            if (stop == null) {
                ready.completeExceptionally(BKException.create(BKException.Code.ClientClosedException));
            } else {
                stop.closeAsync(); // Last-child cleanup still owns its actual cold/connection drain.
            }
        }

        CompletableFuture<Void> closeAsync() {
            ProfileRecoveryPeerPool.Lease release;
            ProfileRecoveryPeerPool.Acquisition discard;
            synchronized (ProfileLedgerClient.this) {
                retiring = true;
                release = lease;
                discard = acquisition;
            }
            // A waiting child may leave before start; no public cancellation reaches this private future.
            ready.completeExceptionally(BKException.create(BKException.Code.ClientClosedException));
            return release != null ? release.closeAsync() : discard != null ? discard.closeAsync()
                    : CompletableFuture.completedFuture(null);
        }
    }

    private final class Operation {
        private final long ledger;
        private final byte[] password;
        private final ProfileCreateRequest request;
        private final ProfileDescriptor descriptor;
        private final ZKProfileReservationStore.FreshInitialClaim retainedClaim;
        private final CompletableFuture<Void> terminal = new CompletableFuture<>();
        private final CompletableFuture<Void> readyObserved = new CompletableFuture<>();
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
        private Epoch cohort;
        private final Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
        private final Map<String, ProfileInitialControlClient> controls = new HashMap<>();
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
            synchronized (ProfileLedgerClient.this) {
                if (stopped || closing || cleanupStarted) {
                    readyObserved.complete(null); // This child never attaches an acquisition callback.
                    failAfterCleanup(BKException.create(BKException.Code.ClientClosedException));
                    return;
                }
            }
            try {
                cohort.start();
                cohort.ready.whenComplete((acquired, failure) -> {
                    try {
                        acquired(acquired, failure);
                    } finally {
                        readyObserved.complete(null);
                    }
                });
            } catch (RuntimeException failedAdmission) {
                readyObserved.complete(null);
                failAfterCleanup(failedAdmission);
            }
        }

        private void acquired(ProfileRecoveryPeerPool.Lease acquired, Throwable failure) {
            if (failure != null) {
                failAfterCleanup(failure);
                return;
            }
            Throwable rejected = null;
            synchronized (ProfileLedgerClient.this) {
                if (stopped || closing || result.isDone() || cleanupStarted) {
                    rejected = BKException.create(BKException.Code.ClientClosedException);
                } else {
                    try {
                        acquired.data().forEach((bookie, client) -> data.put(bookie, client.borrowScope()));
                        acquired.controls().forEach((bookie, client) -> controls.put(bookie, client.borrowScope()));
                        if (request != null) {
                            createStarted = true;
                            operationDrained = new CompletableFuture<>();
                        } else {
                            cold = readOnlyAdmission.begin(
                                    ledger, sdk, password, Map.copyOf(data), maxReadEntries);
                            operationDrained = cold.drained(); // Mutable core futures never escape this owner.
                        }
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
                    var dataPeer = this.data.get(bookie);
                    var controlPeer = this.controls.get(scope.targetBookie());
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
                var candidates = replacementTargets(data.keySet());
                var closeControls = request.maxEnsembleChanges() == 0 ? Map.copyOf(controls)
                        : Map.copyOf(this.controls);
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
                }).thenCompose(initial -> configureWriter(initial, Map.copyOf(data), closeControls, candidates));
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

        private List<ProfileReplacementCoordinator.Target> replacementTargets(Set<BookieId> initial)
                throws BKException {
            if (request.maxEnsembleChanges() == 0) {
                return List.of();
            }
            var extras = new TreeSet<BookieId>(Comparator.comparing(BookieId::toString));
            extras.addAll(data.keySet());
            controls.keySet().forEach(bookie -> extras.add(BookieId.parse(bookie)));
            extras.removeAll(initial);
            if (extras.isEmpty()) {
                throw BKException.create(BKException.Code.NotEnoughBookiesException);
            }
            if (extras.size() > ProfileReplacementPage.MAX_PUBLICATIONS) {
                throw BKException.create(BKException.Code.IncorrectParameterException);
            }
            var candidates = new ArrayList<ProfileReplacementCoordinator.Target>();
            for (var bookie : extras) {
                var dataPeer = data.get(bookie);
                var controlPeer = controls.get(bookie.toString());
                if (dataPeer == null || controlPeer == null) {
                    throw BKException.create(BKException.Code.NotEnoughBookiesException);
                }
                requirePeer(controlPeer.hello(), bookie.toString(), controlPeer.hello().storageIncarnation());
                requirePeer(dataPeer.hello(), bookie.toString(), controlPeer.hello().storageIncarnation());
                try {
                    candidates.add(new ProfileReplacementCoordinator.Target(controlPeer, dataPeer));
                } catch (IllegalArgumentException invalid) {
                    throw BKException.create(BKException.Code.MetadataVersionException);
                }
            }
            return List.copyOf(candidates);
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
                        if (!pendingCreates.containsKey(ledger)) {
                            discardIdlePendingCreate();
                        }
                        pendingCreates.put(ledger, new PendingCreate(request, fresh.claim()));
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
                Map<BookieId, ProfileNormalDataClient> data, Map<String, ProfileInitialControlClient> controls,
                List<ProfileReplacementCoordinator.Target> candidates) {
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
            var configured = request.maxEnsembleChanges() == 0 ? CompletableFuture.<Void>completedFuture(null)
                    : writer.enableReplacement(replacement, candidates, request.descriptor(),
                            request.maxEnsembleChanges());
            return configured.thenCompose(unused -> writer.enableWriterClose(coordinator, request.descriptor(),
                    validator, controls, limits.maxCloseCoordinates(), limits.closeDuration())).thenApply(unused -> {
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
                    pendingCreates.remove(ledger); // Handoff may now be visible; this origin cannot recreate a writer.
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
            boolean initializing;
            synchronized (ProfileLedgerClient.this) {
                stopped = true;
                stopCold = cold;
                initializing = createStarted;
            }
            cohort.stopIfUnneeded();
            if (stopHandle()) {
                return;
            }
            if (stopCold != null) {
                stopCold.cancel();
            } else if (!initializing) {
                failAfterCleanup(BKException.create(BKException.Code.ClientClosedException));
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
            synchronized (ProfileLedgerClient.this) {
                if (cleanupStarted) {
                    return;
                }
                cleanupStarted = true;
            }
            readyObserved.thenCompose(observed -> {
                synchronized (ProfileLedgerClient.this) {
                    var work = operationDrained == null
                            ? CompletableFuture.<Void>completedFuture(null) : operationDrained;
                    return handleDrained == null ? work : CompletableFuture.allOf(work, handleDrained);
                }
            }).thenCompose(unused -> {
                var drains = new java.util.ArrayList<CompletableFuture<Void>>();
                data.values().forEach(client -> drains.add(client.closeAsync()));
                controls.values().forEach(client -> drains.add(client.closeAsync()));
                return CompletableFuture.allOf(drains.toArray(CompletableFuture[]::new));
            }).whenComplete((unused, failure) -> childDrained(failure));
        }

        private void childDrained(Throwable failure) {
            boolean last;
            synchronized (ProfileLedgerClient.this) {
                if (failure != null) {
                    cohort.retiring = true;
                    last = false;
                } else {
                    last = active.size() == 1 && active.get(ledger) == this;
                    if (last) {
                        cohort.retiring = true;
                    } else {
                        active.remove(ledger, this);
                    }
                }
            }
            if (failure != null) {
                finish(failure);
            } else if (last) {
                cohort.closeAsync().whenComplete((unused, closeFailure) -> {
                    synchronized (ProfileLedgerClient.this) {
                        if (closeFailure == null) {
                            active.remove(ledger, this);
                            if (epoch == cohort) {
                                epoch = null;
                            }
                        }
                    }
                    finish(closeFailure);
                });
            } else {
                finish(null);
            }
        }

        private void finish(Throwable failure) {
            // Reentrant public close callbacks may immediately create/open the next handle.
            if (failure == null) {
                terminal.complete(null);
                finishClose();
            } else {
                synchronized (ProfileLedgerClient.this) {
                    cohort.retiring = true;
                }
                terminal.completeExceptionally(failure);
                closed.completeExceptionally(failure); // Preserve active custody and SDK dependencies.
            }
        }
    }
}

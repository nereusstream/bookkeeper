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
package org.apache.bookkeeper.discover.profile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Experimental resumable initial lifecycle through all-E activation and an availability snapshot.
 * It returns no LedgerHandle/writer, creates no executor/retry/timer and owns no client. Verified peer facts and
 * request-correlated nonblocking clients are supplied by the caller. Availability is not a continuing lease.
 */
public final class ProfileInitialLifecycleCoordinator {
    @FunctionalInterface
    public interface ActivationClient {
        CompletableFuture<ProfileControlResult> activate(ProfileControlRequest request);
    }

    public record Target(ProfileHello.Server hello, ActivationClient client) {
        public Target {
            Objects.requireNonNull(hello, "hello");
            Objects.requireNonNull(client, "client");
        }
    }

    public enum Status { AVAILABLE, NOT_READY, UNCERTAIN, CONFLICT, QUARANTINED, DELETED }

    public record Activation(ProfileControlScope scope, long localGeneration) {
        public Activation {
            Objects.requireNonNull(scope, "scope");
            if (scope.operation() != ProfileControlOperation.ACTIVATE_INITIAL || localGeneration <= 0) {
                throw new IllegalArgumentException("Invalid initial activation completion");
            }
        }
    }

    public record Result(Status status, Versioned<LedgerMetadata> metadata,
            ZKProfileReservationStore.ReadySnapshot ready, List<Activation> activations) {
        public Result {
            Objects.requireNonNull(status, "status");
            activations = List.copyOf(activations);
            if ((status == Status.AVAILABLE) != (metadata != null)
                    || status == Status.AVAILABLE && (ready == null
                            || activations.size() != ready.page().installs().size())) {
                throw new IllegalArgumentException("Availability requires metadata, READY and all-E completions");
            }
        }

        @Override
        public String toString() {
            return "InitialLifecycleResult{status=" + status + ", metadataPresent=" + (metadata != null)
                    + ", activatedTargets=" + activations.size() + '}';
        }
    }

    private record Attempt(Activation active, boolean uncertain) { }

    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ProfileInitialMetadataCoordinator initial;
    private final Executor coldExecutor;

    public ProfileInitialLifecycleCoordinator(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata,
            ProfileDescriptorValidator validator, Executor coldExecutor) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.coldExecutor = Objects.requireNonNull(coldExecutor, "coldExecutor");
        initial = new ProfileInitialMetadataCoordinator(reservations, metadata, validator, coldExecutor);
    }

    public CompletableFuture<Result> initialize(LedgerMetadata desired, ProfileReservation reservation,
            byte[] descriptor, ProtectedProfileCredential credential,
            Map<String, ProfileInitialMetadataCoordinator.Target> installers, Map<String, Target> suppliedActivators,
            byte[] readyOperationId, byte[] readyPayloadIdentity) {
        return initialize(desired, reservation, descriptor, credential, installers, suppliedActivators,
                readyOperationId, readyPayloadIdentity, null);
    }

    /** Only a definite absence of standard metadata permits an attempt to claim a fresh native birth. */
    public CompletableFuture<ZKProfileReservationStore.FreshCreateResult> claimFresh(ProfileReservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        CompletableFuture<Versioned<LedgerMetadata>> current;
        try {
            current = Objects.requireNonNull(metadata.manager().readLedgerMetadata(reservation.backlink().ledgerId()),
                    "metadata read future");
        } catch (RuntimeException unavailable) {
            return CompletableFuture.completedFuture(new ZKProfileReservationStore.FreshCreateResult(
                    ZKProfileReservationStore.CreateStatus.DURABILITY_UNKNOWN, null));
        }
        return current.handle((existing, failed) -> {
            if (failed == null) {
                return CompletableFuture.completedFuture(new ZKProfileReservationStore.FreshCreateResult(
                        ZKProfileReservationStore.CreateStatus.CONFLICT, null));
            }
            if (BKException.getExceptionCode(failed)
                    != BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                return CompletableFuture.completedFuture(new ZKProfileReservationStore.FreshCreateResult(
                        ZKProfileReservationStore.CreateStatus.DURABILITY_UNKNOWN, null));
            }
            return cold(() -> reservations.createFresh(reservation));
        }).thenCompose(future -> future).exceptionally(failed -> new ZKProfileReservationStore.FreshCreateResult(
                ZKProfileReservationStore.CreateStatus.DURABILITY_UNKNOWN, null));
    }

    /** Resume only the same store's acknowledged fresh birth, without reconstructing or recreating its origin. */
    public CompletableFuture<Result> initializeFresh(ZKProfileReservationStore.FreshInitialClaim claim,
            LedgerMetadata desired, ProfileReservation reservation, byte[] descriptor,
            ProtectedProfileCredential credential, Map<String, ProfileInitialMetadataCoordinator.Target> installers,
            Map<String, Target> suppliedActivators, byte[] readyOperationId, byte[] readyPayloadIdentity) {
        Objects.requireNonNull(claim, "claim");
        if (!claim.issuedBy(reservations) || !claim.origin().reservation().equals(reservation)) {
            return completed(Status.CONFLICT, null, List.of());
        }
        return initialize(desired, reservation, descriptor, credential, installers, suppliedActivators,
                readyOperationId, readyPayloadIdentity, claim);
    }

    private CompletableFuture<Result> initialize(LedgerMetadata desired, ProfileReservation reservation,
            byte[] descriptor, ProtectedProfileCredential credential,
            Map<String, ProfileInitialMetadataCoordinator.Target> installers, Map<String, Target> suppliedActivators,
            byte[] readyOperationId, byte[] readyPayloadIdentity, ZKProfileReservationStore.FreshInitialClaim claim) {
        var plan = initial.freeze(desired, reservation, descriptor, credential, installers);
        Map<String, Target> activators = Map.copyOf(suppliedActivators);
        byte[] operation = ProfileLifecycleHead.fixed(readyOperationId, 16, true);
        byte[] payload = ProfileLifecycleHead.fixed(readyPayloadIdentity, 32, false);
        if (!activators.keySet().equals(plan.targets().keySet())
                || Arrays.equals(operation, reservation.installs().get(0).operationId())) {
            throw new IllegalArgumentException("Initial lifecycle target set or operation identity mismatch");
        }
        for (ProfileControlScope scope : reservation.installs()) {
            ProfileHello.Server hello = activators.get(scope.targetBookie()).hello();
            if (hello.activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                    || !hello.bookieId().equals(scope.targetBookie())
                    || !Arrays.equals(hello.storageIncarnation(), scope.targetStorageIncarnation())
                    || plan.descriptor().mandatoryCapabilities().stream().anyMatch(capability ->
                            !hello.capabilities().contains(new ProfileHello.Capability(capability.capabilityId(),
                                    capability.semanticVersion())))) {
                throw new IllegalArgumentException("Initial activation HELLO binding mismatch");
            }
        }
        return cold(() -> reservations.read(reservation.backlink().ledgerId())).handle((read, failed) -> {
            if (failed != null) {
                return completed(Status.UNCERTAIN, null, List.of());
            }
            if (read.status() == ReadStatus.NOT_FOUND) {
                if (claim != null) {
                    return completed(Status.UNCERTAIN, null, List.of());
                }
                return cold(() -> reservations.createIfAbsent(reservation)).thenCompose(created ->
                        created.snapshot() == null ? completed(map(created.status()), null, List.of())
                                : resume(plan, activators, operation, payload, created.snapshot(), null));
            }
            if (read.status() == ReadStatus.FOUND && claim != null && !sameOrigin(claim, read.snapshot())) {
                return completed(Status.CONFLICT, null, List.of());
            }
            return read.status() == ReadStatus.FOUND
                    ? resume(plan, activators, operation, payload, read.snapshot(), claim)
                    : completed(read.status() == ReadStatus.QUARANTINED ? Status.QUARANTINED : Status.UNCERTAIN,
                            null, List.of());
        }).thenCompose(future -> future).exceptionally(failed -> {
            Throwable cause = failed;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof RejectedExecutionException) {
                return result(Status.UNCERTAIN, null, List.of());
            }
            throw new CompletionException(cause);
        });
    }

    private CompletableFuture<Result> resume(ProfileInitialMetadataCoordinator.Plan plan, Map<String,
            Target> activators,
            byte[] operation, byte[] payload, ZKProfileReservationStore.Snapshot origin,
            ZKProfileReservationStore.FreshInitialClaim claim) {
        if (!origin.reservation().equals(plan.reservation())) {
            return completed(Status.CONFLICT, null, List.of());
        }
        if (origin.lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
            return completed(Status.DELETED, null, List.of());
        }
        return cold(() -> reservations.readCandidate(plan.reservation().backlink().ledgerId()))
                .thenCompose(candidate -> {
            if (candidate.page() != null) {
                if (!candidate.page().page().lifecycleBirth().equals(origin.lifecycle().birthId())
                        || !Arrays.equals(operation, candidate.page().page().operationId())
                        || !Arrays.equals(payload, candidate.page().page().payloadIdentity())) {
                    return completed(Status.CONFLICT, candidate.page(), List.of());
                }
                return verifyMetadata(plan).thenCompose(verified -> {
                    if (verified.outcome() != ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED) {
                        return completed(map(verified.outcome()), candidate.page(), List.of());
                    }
                    if (!candidate.page().page().matchesMetadata(verified.metadata())) {
                        return completed(Status.CONFLICT, candidate.page(), List.of());
                    }
                    return publishAndActivate(plan, activators, origin, candidate.page(), claim);
                });
            }
            if (origin.lifecycle().state() != ProfileLifecycleHead.State.PREPARING) {
                return completed(Status.QUARANTINED, null, List.of());
            }
            if (candidate.status() != ZKProfileReservationStore.CreateStatus.CONFLICT) {
                return completed(map(candidate.status()), null, List.of());
            }
            return initial.create(plan, origin).thenCompose(created -> {
                if (created.metadata() == null) {
                    return completed(switch (created.status()) {
                        case QUARANTINED -> Status.QUARANTINED;
                        case INSTALL_UNCERTAIN, RESERVATION_UNKNOWN, METADATA_UNKNOWN -> Status.UNCERTAIN;
                        case RESERVATION_CONFLICT, METADATA_CONFLICT -> Status.CONFLICT;
                        default -> Status.NOT_READY;
                    }, null, List.of());
                }
                return cold(() -> reservations.read(plan.reservation().backlink().ledgerId())).thenCompose(current -> {
                    if (current.status() != ReadStatus.FOUND) {
                        return completed(current.status() == ReadStatus.QUARANTINED
                                ? Status.QUARANTINED : Status.UNCERTAIN, null, List.of());
                    }
                    if (!current.snapshot().reservation().equals(origin.reservation())
                            || !current.snapshot().lifecycle().birthId().equals(origin.lifecycle().birthId())
                            || claim != null && !sameOrigin(claim, current.snapshot())) {
                        return completed(Status.CONFLICT, null, List.of());
                    }
                    if (current.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                        return completed(Status.DELETED, null, List.of());
                    }
                    ProfileReadyPage page = ProfileReadyPage.from(origin, created, operation, payload);
                    return cold(() -> reservations.prepareReady(origin, page,
                            metadata.manager())).thenCompose(prepared ->
                            prepared.page() == null ? completed(map(prepared.status()), null, List.of())
                                    : publishAndActivate(plan, activators, origin, prepared.page(), claim));
                });
            });
        });
    }

    private CompletableFuture<ProfileLedgerMetadataStore.Result> verifyMetadata(
            ProfileInitialMetadataCoordinator.Plan plan) {
        // Candidate/READY is persistent evidence that metadata existed; never create-if-absent in this branch.
        return metadata.readMatching(plan.frozen(), plan.reservation().backlink());
    }

    private CompletableFuture<Result> publishAndActivate(ProfileInitialMetadataCoordinator.Plan plan,
            Map<String, Target> activators, ZKProfileReservationStore.Snapshot origin,
            ZKProfileReservationStore.ReadySnapshot page, ZKProfileReservationStore.FreshInitialClaim claim) {
        return cold(() -> reservations.publishReady(origin, page, metadata.manager())).thenCompose(published -> {
            if (published.snapshot() == null) {
                return completed(map(published.status()), page, List.of());
            }
            if (claim != null && !sameOrigin(claim, published.snapshot())) {
                return completed(Status.CONFLICT, page, List.of());
            }
            if (published.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                return completed(Status.DELETED, page, List.of());
            }
            List<CompletableFuture<Attempt>> pending = new ArrayList<>();
            for (ProfileInstallReceipt install : page.page().installs()) {
                ProfileControlScope scope = page.page().activation(install.scope().targetBookie());
                Target target = activators.get(scope.targetBookie());
                CompletableFuture<ProfileControlResult> call;
                try {
                    call = Objects.requireNonNull(target.client().activate(ProfileControlRequest.operation(scope)),
                            "activation future");
                } catch (RuntimeException unavailable) {
                    call = CompletableFuture.failedFuture(unavailable);
                }
                pending.add(call.handle((result, failed) -> {
                    if (failed != null || result == null
                            || result.outcome() == ProfileControlResult.Outcome.DURABILITY_UNKNOWN) {
                        return new Attempt(null, true);
                    }
                    if (result.outcome() != ProfileControlResult.Outcome.APPLIED
                            && result.outcome() != ProfileControlResult.Outcome.ALREADY_APPLIED) {
                        return new Attempt(null, false);
                    }
                    boolean matches = result.operation() == ProfileControlOperation.ACTIVATE_INITIAL
                            && result.ledgerId() == scope.ledgerId()
                            && result.operationIdHex().equals(HexFormat.of().formatHex(scope.operationId()))
                            && result.authorityGeneration() == scope.authorityGeneration()
                            && result.localGeneration() > install.localInstallGeneration() && result.durable();
                    return matches ? new Attempt(new Activation(scope, result.localGeneration()), false)
                            : new Attempt(null, true);
                }));
            }
            return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).thenCompose(ignored -> {
                List<Attempt> results = pending.stream().map(CompletableFuture::join).toList();
                List<Activation> active = results.stream().map(Attempt::active).filter(Objects::nonNull).toList();
                if (results.stream().anyMatch(Attempt::uncertain)) {
                    return completed(Status.UNCERTAIN, page, active);
                }
                if (active.size() != page.page().installs().size()) {
                    return completed(Status.NOT_READY, page, active);
                }
                return verifyMetadata(plan).thenCompose(verified -> {
                    if (verified.outcome() != ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED
                            || !page.page().matchesMetadata(verified.metadata())) {
                        return completed(verified.outcome() == ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED
                                ? Status.CONFLICT : map(verified.outcome()), page, active);
                    }
                    return cold(() -> reservations.read(plan.reservation().backlink().ledgerId())).thenApply(read -> {
                        if (read.status() != ReadStatus.FOUND) {
                            return result(read.status() == ReadStatus.QUARANTINED
                                    ? Status.QUARANTINED : Status.UNCERTAIN, page, active);
                        }
                        if (claim != null && !sameOrigin(claim, read.snapshot())) {
                            return result(Status.CONFLICT, page, active);
                        }
                        ProfileLifecycleHead head = read.snapshot().lifecycle();
                        if (head.state() == ProfileLifecycleHead.State.DELETE_INTENT) {
                            return result(Status.DELETED, page, active);
                        }
                        if (head.state() != ProfileLifecycleHead.State.READY
                                || !head.birthId().equals(page.page().lifecycleBirth())
                                || head.readyPageVersion() != page.storeVersion()
                                || !Arrays.equals(head.readyPageDigest(), page.digest())) {
                            return result(Status.CONFLICT, page, active);
                        }
                        return new Result(Status.AVAILABLE, verified.metadata(), page, active);
                    });
                });
            });
        });
    }

    private static boolean sameOrigin(ZKProfileReservationStore.FreshInitialClaim claim,
            ZKProfileReservationStore.Snapshot current) {
        var origin = claim.origin();
        return current.birthZxid() == origin.birthZxid()
                && current.storeVersion() == origin.storeVersion()
                && current.rootDataVersion() == origin.rootDataVersion()
                && current.reservation().equals(origin.reservation())
                && current.lifecycle().birthId().equals(origin.lifecycle().birthId());
    }

    private <T> CompletableFuture<T> cold(Supplier<T> operation) {
        try {
            return CompletableFuture.supplyAsync(operation, coldExecutor);
        } catch (RejectedExecutionException rejected) {
            return CompletableFuture.failedFuture(rejected);
        }
    }

    private static Status map(ZKProfileReservationStore.CreateStatus status) {
        return switch (status) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.UNCERTAIN;
        };
    }

    private static Status map(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.UNCERTAIN;
        };
    }

    private static Result result(Status status, ZKProfileReservationStore.ReadySnapshot page, List<Activation> active) {
        return new Result(status, null, page, active);
    }

    private static CompletableFuture<Result> completed(Status status,
            ZKProfileReservationStore.ReadySnapshot page, List<Activation> active) {
        return CompletableFuture.completedFuture(result(status, page, active));
    }
}

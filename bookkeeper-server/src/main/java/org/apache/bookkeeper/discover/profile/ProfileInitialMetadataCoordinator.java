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

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Experimental initial-create coordinator through standard metadata only; no READY, activation or normal writer.
 * Borrows a bounded cold executor and request-correlated nonblocking clients with verified peer HELLO facts.
 * Client completion must correspond to that peer/request. It adds no thread, retry, timer or client ownership.
 */
public final class ProfileInitialMetadataCoordinator {
    @FunctionalInterface
    public interface InstallClient {
        CompletableFuture<ProfileControlResult> install(ProfileControlRequest request);
    }

    public record Target(ProfileHello.Server hello, InstallClient client) {
        public Target {
            Objects.requireNonNull(hello, "hello");
            Objects.requireNonNull(client, "client");
        }
    }

    public enum Status {
        METADATA_CREATED, METADATA_ALREADY_CREATED, INSTALL_NOT_READY, INSTALL_UNCERTAIN,
        RESERVATION_CONFLICT, RESERVATION_UNKNOWN, METADATA_CONFLICT, METADATA_UNKNOWN, QUARANTINED
    }

    public record Result(Status status, Versioned<LedgerMetadata> metadata, List<ProfileInstallReceipt> installs) {
        public Result {
            Objects.requireNonNull(status, "status");
            installs = List.copyOf(installs);
            if ((status == Status.METADATA_CREATED || status == Status.METADATA_ALREADY_CREATED)
                    != (metadata != null)) {
                throw new IllegalArgumentException("Only proved metadata creation carries its snapshot");
            }
        }

        @Override
        public String toString() {
            return "ProfileInitialMetadataResult{status=" + status + ", metadataPresent=" + (metadata != null)
                    + ", installedTargets=" + installs.size() + '}';
        }
    }

    record Plan(LedgerMetadata frozen, ProfileReservation reservation, ProfileDescriptor descriptor,
                Map<String, Target> targets, List<ProfileControlRequest> requests) {
        @Override
        public String toString() {
            return "InitialPlan{ledgerId=" + reservation.backlink().ledgerId() + ", targets=" + targets.size() + '}';
        }
    }

    private record Attempt(ProfileInstallReceipt receipt, boolean uncertain) { }

    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ProfileDescriptorValidator validator;
    private final Executor coldExecutor;

    public ProfileInitialMetadataCoordinator(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata,
            ProfileDescriptorValidator validator, Executor coldExecutor) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.coldExecutor = Objects.requireNonNull(coldExecutor, "coldExecutor");
    }

    public CompletableFuture<Result> create(LedgerMetadata initial, ProfileReservation reservation,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential, Map<String, Target> suppliedTargets) {
        return create(freeze(initial, reservation, canonicalDescriptor, credential, suppliedTargets));
    }

    Plan freeze(LedgerMetadata initial, ProfileReservation reservation, byte[] canonicalDescriptor,
            ProtectedProfileCredential credential, Map<String, Target> suppliedTargets) {
        Objects.requireNonNull(reservation, "reservation");
        Objects.requireNonNull(credential, "credential");
        LedgerMetadata frozen = ProfileLedgerMetadataStore.snapshotInitial(initial, reservation.backlink());
        try {
            if (!credential.matchesWireVerifier(DigestManager.generateMasterKey(frozen.getPassword()))) {
                throw new IllegalArgumentException("Profile credential does not match the standard metadata password");
            }
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required BookKeeper credential digest unavailable", impossible);
        }
        byte[] descriptorBytes = canonicalDescriptor.clone();
        ProfileDescriptor descriptor = ProfileDescriptorCodec.decodeAndVerify(descriptorBytes,
                reservation.installs().get(0).descriptorIdentity(), validator);
        List<String> planned = reservation.installs().stream().map(ProfileControlScope::targetBookie).toList();
        Map<String, Target> targets = Map.copyOf(suppliedTargets);
        if (descriptor.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || descriptor.durabilityMode() != DurabilityMode.SYNC_ON_ACK
                || descriptor.ensembleSize() != planned.size()
                || descriptor.ensembleSize() != frozen.getEnsembleSize()
                || descriptor.writeQuorumSize() != frozen.getWriteQuorumSize()
                || descriptor.ackQuorumSize() != frozen.getAckQuorumSize()
                || !planned.equals(frozen.getAllEnsembles().firstEntry().getValue().stream()
                        .map(BookieId::toString).toList())
                || !targets.keySet().equals(new java.util.HashSet<>(planned))) {
            throw new IllegalArgumentException("Initial Profile metadata does not match the reserved descriptor and E");
        }
        List<ProfileControlRequest> requests = new ArrayList<>();
        for (ProfileControlScope scope : reservation.installs()) {
            Target target = targets.get(scope.targetBookie());
            if (target.hello().activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                    || !target.hello().bookieId().equals(scope.targetBookie())
                    || !Arrays.equals(target.hello().storageIncarnation(), scope.targetStorageIncarnation())
                    || descriptor.mandatoryCapabilities().stream().anyMatch(capability ->
                            !target.hello().capabilities().contains(new ProfileHello.Capability(
                                    capability.capabilityId(), capability.semanticVersion())))) {
                throw new IllegalArgumentException("Initial Profile target HELLO mismatch or missing capability");
            }
            requests.add(ProfileControlRequest.install(scope, descriptorBytes, EngineProfile.SEGMENT_WAL_ENGINE,
                    credential));
        }
        return new Plan(frozen, reservation, descriptor, targets, List.copyOf(requests));
    }

    CompletableFuture<Result> create(Plan plan) {
        return create(plan, null);
    }

    CompletableFuture<Result> create(Plan plan, ZKProfileReservationStore.Snapshot expected) {
        LedgerMetadata frozen = plan.frozen();
        ProfileReservation reservation = plan.reservation();
        Map<String, Target> targets = plan.targets();
        List<ProfileControlRequest> requests = plan.requests();
        return cold(() -> {
            if (expected == null) {
                return reservations.createIfAbsent(reservation);
            }
            // An already-held origin can only be checked, never recreated after deletion.
            var current = reservations.read(reservation.backlink().ledgerId());
            if (current.status() != ReadStatus.FOUND) {
                return new ZKProfileReservationStore.CreateResult(current.status() == ReadStatus.QUARANTINED
                        ? ZKProfileReservationStore.CreateStatus.QUARANTINED
                        : ZKProfileReservationStore.CreateStatus.DURABILITY_UNKNOWN, null);
            }
            return new ZKProfileReservationStore.CreateResult(expected.equals(current.snapshot())
                    ? ZKProfileReservationStore.CreateStatus.ALREADY_APPLIED
                    : ZKProfileReservationStore.CreateStatus.CONFLICT,
                    expected.equals(current.snapshot()) ? current.snapshot() : null);
        }).handle((created, failed) -> {
            if (failed != null) {
                return CompletableFuture.completedFuture(result(Status.RESERVATION_UNKNOWN, List.of()));
            }
            return switch (created.status()) {
                case APPLIED, ALREADY_APPLIED -> installAll(frozen, reservation, created.snapshot(), targets, requests);
                case CONFLICT -> CompletableFuture.completedFuture(result(Status.RESERVATION_CONFLICT, List.of()));
                case QUARANTINED -> CompletableFuture.completedFuture(result(Status.QUARANTINED, List.of()));
                case DURABILITY_UNKNOWN, UNAVAILABLE ->
                        CompletableFuture.completedFuture(result(Status.RESERVATION_UNKNOWN, List.of()));
            };
        }).thenCompose(future -> future);
    }

    private CompletableFuture<Result> installAll(LedgerMetadata frozen, ProfileReservation reservation,
            ZKProfileReservationStore.Snapshot preparing, Map<String, Target> targets,
            List<ProfileControlRequest> requests) {
        List<CompletableFuture<Attempt>> pending = new ArrayList<>();
        // Dispatch the entire E before waiting for any result. Clients must submit nonblocking requests.
        for (ProfileControlRequest request : requests) {
            ProfileControlScope scope = request.scope();
            Target target = targets.get(scope.targetBookie());
            CompletableFuture<ProfileControlResult> future;
            try {
                future = Objects.requireNonNull(target.client().install(request), "install future");
            } catch (RuntimeException unavailable) {
                future = CompletableFuture.failedFuture(unavailable);
            }
            pending.add(future.handle((answer, failed) -> {
                if (failed != null || answer == null
                        || answer.outcome() == ProfileControlResult.Outcome.DURABILITY_UNKNOWN) {
                    return new Attempt(null, true);
                }
                if (answer.outcome() != ProfileControlResult.Outcome.APPLIED
                        && answer.outcome() != ProfileControlResult.Outcome.ALREADY_APPLIED) {
                    return new Attempt(null, false);
                }
                return ProfileInstallReceipt.matches(scope, target.hello(), answer)
                        ? new Attempt(ProfileInstallReceipt.from(scope, target.hello(), answer), false)
                        : new Attempt(null, true);
            }));
        }
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).thenCompose(ignored -> {
            List<Attempt> attempts = pending.stream().map(CompletableFuture::join).toList();
            List<ProfileInstallReceipt> installed = attempts.stream().map(Attempt::receipt)
                    .filter(Objects::nonNull).toList();
            if (attempts.stream().anyMatch(Attempt::uncertain)) {
                return CompletableFuture.completedFuture(result(Status.INSTALL_UNCERTAIN, installed));
            }
            if (installed.size() != requests.size()) {
                return CompletableFuture.completedFuture(result(Status.INSTALL_NOT_READY, installed));
            }
            return cold(() -> reservations.read(reservation.backlink().ledgerId())).handle((current, failed) -> {
                if (failed != null || current.status() == ReadStatus.NOT_FOUND
                        || current.status() == ReadStatus.UNAVAILABLE) {
                    return CompletableFuture.completedFuture(result(Status.RESERVATION_UNKNOWN, installed));
                }
                if (current.status() == ReadStatus.QUARANTINED) {
                    return CompletableFuture.completedFuture(result(Status.QUARANTINED, installed));
                }
                if (!preparing.equals(current.snapshot())) {
                    return CompletableFuture.completedFuture(result(Status.RESERVATION_CONFLICT, installed));
                }
                return createMetadata(frozen, reservation.backlink(), installed);
            }).thenCompose(future -> future);
        });
    }

    private CompletableFuture<Result> createMetadata(LedgerMetadata frozen, ProfileMetadataBacklink backlink,
            List<ProfileInstallReceipt> installed) {
        CompletableFuture<ProfileLedgerMetadataStore.Result> future;
        try {
            future = metadata.createIfAbsent(frozen, backlink);
        } catch (RuntimeException unavailable) {
            return CompletableFuture.completedFuture(result(Status.METADATA_UNKNOWN, installed));
        }
        return future.handle((created, failed) -> {
            if (failed != null) {
                return result(Status.METADATA_UNKNOWN, installed);
            }
            Status status = switch (created.outcome()) {
                case APPLIED -> Status.METADATA_CREATED;
                case ALREADY_APPLIED -> Status.METADATA_ALREADY_CREATED;
                case CONFLICT -> Status.METADATA_CONFLICT;
                case QUARANTINED -> Status.QUARANTINED;
                case DURABILITY_UNKNOWN -> Status.METADATA_UNKNOWN;
            };
            return new Result(status, created.metadata(), installed);
        });
    }

    private <T> CompletableFuture<T> cold(Supplier<T> operation) {
        try {
            return CompletableFuture.supplyAsync(operation, coldExecutor);
        } catch (RejectedExecutionException rejected) {
            return CompletableFuture.failedFuture(rejected);
        }
    }

    private static Result result(Status status, List<ProfileInstallReceipt> installed) {
        return new Result(status, null, installed);
    }
}

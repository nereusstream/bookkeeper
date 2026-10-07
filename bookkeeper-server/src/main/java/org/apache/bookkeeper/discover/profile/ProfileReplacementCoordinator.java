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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.versioning.Versioned;

/** One frozen cold replacement operation. Borrows clients/executor; no automatic reconnect or new operation retry. */
public final class ProfileReplacementCoordinator {
    public record Target(ProfileInitialControlClient control, ProfileNormalDataClient data) {
        public Target {
            Objects.requireNonNull(control, "control");
            Objects.requireNonNull(data, "data");
            var hello = control.hello();
            if (hello.activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                    || data.hello().activeEngine() != hello.activeEngine()
                    || !data.hello().bookieId().equals(hello.bookieId())
                    || !Arrays.equals(data.hello().storageIncarnation(), hello.storageIncarnation())
                    || !hello.capabilities().contains(new ProfileHello.Capability(
                            ProfileInitialControlCodec.REPLACEMENT_CAPABILITY_ID,
                            ProfileInitialControlCodec.REPLACEMENT_SCHEMA))) {
                throw new IllegalArgumentException("Replacement requires matching CONTROL/DATA peers and UIC2");
            }
        }
        public BookieId bookie() {
            return BookieId.parse(control.hello().bookieId());
        }
    }

    public enum Status { AVAILABLE, NOT_READY, UNCERTAIN, CONFLICT, QUARANTINED, DELETED }

    public record Result(Status status, Versioned<LedgerMetadata> metadata,
            ZKProfileReservationStore.ReplacementSnapshot authority, Map<BookieId, ProfileNormalDataClient> clients) {
        public Result {
            Objects.requireNonNull(status, "status");
            clients = Map.copyOf(clients);
            if ((status == Status.AVAILABLE) != (metadata != null) || status == Status.AVAILABLE && authority == null) {
                throw new IllegalArgumentException("Replacement availability requires actual metadata and authority");
            }
        }
        @Override
        public String toString() {
            return "ReplacementResult{status=" + status + ", metadataPresent=" + (metadata != null) + '}';
        }
    }

    public record Operation(ZKProfileReservationStore.Snapshot source,
            ZKProfileReservationStore.ReadySnapshot initial, ProfileLedgerMetadataStore.MembershipSnapshot before,
            ProfileReplacementInstallPage plan, Map<Integer, Target> targets, List<BookieId> ensemble,
            byte[] descriptor, ProtectedProfileCredential credential, byte[] activationId, byte[] activationPayload) {
        public Operation {
            targets = Map.copyOf(targets);
            ensemble = List.copyOf(ensemble);
            descriptor = descriptor.clone();
            activationId = ProfileLifecycleHead.fixed(activationId, 16, true);
            activationPayload = ProfileLifecycleHead.fixed(activationPayload, 32, false);
        }
        @Override
        public byte[] descriptor() {
            return descriptor.clone();
        }
        @Override
        public byte[] activationId() {
            return activationId.clone();
        }
        @Override
        public byte[] activationPayload() {
            return activationPayload.clone();
        }
        @Override
        public String toString() {
            return "ReplacementOperation{ledger=" + source.reservation().backlink().ledgerId()
                    + ", sequence=" + plan.sequence() + ", targets=" + targets.size() + '}';
        }
    }

    private final ZKProfileReservationStore lifecycle;
    private final ProfileLedgerMetadataStore metadata;
    private final ProfileDescriptorValidator validator;
    private final Executor cold;

    public ProfileReplacementCoordinator(ZKProfileReservationStore lifecycle, ProfileLedgerMetadataStore metadata,
            ProfileDescriptorValidator validator, Executor cold) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.cold = Objects.requireNonNull(cold, "cold");
    }

    /** Capture once while the SDK completion gate holds LAC; retain this Operation across UNKNOWN. */
    public CompletableFuture<Operation> begin(Versioned<LedgerMetadata> expected, long start,
            Map<Integer, Target> replacements, byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            byte[] installId, byte[] activationId) {
        Map<Integer, Target> targets = Map.copyOf(replacements);
        if (canonicalDescriptor.length > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Replacement descriptor exceeds the canonical bound");
        }
        byte[] descriptor = canonicalDescriptor.clone();
        byte[] operation = ProfileLifecycleHead.fixed(installId, 16, true);
        byte[] activation = ProfileLifecycleHead.fixed(activationId, 16, true);
        if (targets.isEmpty() || targets.size() > expected.getValue().getEnsembleSize()
                || Arrays.equals(operation, activation)) {
            throw new IllegalArgumentException("Invalid frozen replacement targets or operation identities");
        }
        return cold(() -> {
            long ledger = expected.getValue().getLedgerId();
            var source = lifecycle.read(ledger);
            var observed = metadata.readMembership(ledger);
            var initial = lifecycle.readPublishedPage(ledger);
            if (source.status() != ReadStatus.FOUND || observed.status() != ReadStatus.FOUND
                    || initial.page() == null
                    || !observed.snapshot().metadata().getVersion().equals(expected.getVersion())
                    || !ProfileLedgerMetadataStore.sameMetadata(expected.getValue(),
                        observed.snapshot().metadata().getValue())
                    || source.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT
                    || source.snapshot().lifecycle().sequence() >= 2 + ProfileReplacementPage.MAX_PUBLICATIONS) {
                throw new IllegalStateException("Current replacement lifecycle/metadata unavailable or changed");
            }
            var base = source.snapshot().reservation().installs().get(0);
            var layout = ProfileDescriptorCodec.decodeAndVerify(descriptor, base.descriptorIdentity(), validator);
            if (layout.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                    || layout.ensembleSize() != expected.getValue().getEnsembleSize()
                    || layout.writeQuorumSize() != expected.getValue().getWriteQuorumSize()
                    || layout.ackQuorumSize() != expected.getValue().getAckQuorumSize()
                    || !credentialMatches(credential, expected.getValue().getPassword())) {
                throw new IllegalArgumentException("Replacement descriptor or protected credential does not match");
            }
            var next = new ArrayList<>(expected.getValue().getAllEnsembles().lastEntry().getValue());
            var scopes = new ArrayList<ProfileControlScope>();
            byte[] payload = ProfileLifecycleCodec.digest(
                    ByteBuffer.allocate(24).put(operation).putLong(start).array());
            for (var entry : targets.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
                int slot = entry.getKey();
                var target = entry.getValue();
                if (slot < 0 || slot >= next.size() || next.contains(target.bookie())) {
                    throw new IllegalArgumentException("Replacement target is already in the current ensemble");
                }
                if (layout.mandatoryCapabilities().stream().anyMatch(capability ->
                        !target.control().hello().capabilities().contains(new ProfileHello.Capability(
                                capability.capabilityId(), capability.semanticVersion())))) {
                    throw new IllegalArgumentException("Missing replacement mandatory semantic capability");
                }
                next.set(slot, target.bookie());
                scopes.add(new ProfileControlScope(ProfileControlOperation.INSTALL,
                        ProfileControlOperation.INSTALL.purpose(), ledger, base.ledgerInstanceId(),
                        base.descriptorIdentity(), target.bookie().toString(),
                        target.control().hello().storageIncarnation(),
                        base.authorityDomain(), source.snapshot().lifecycle().controlGeneration(),
                        operation, payload, null));
            }
            var plan = ProfileReplacementInstallPage.from(source.snapshot(), observed.snapshot(), start, next, scopes);
            return new Operation(source.snapshot(), initial.page(), observed.snapshot(), plan, targets, next,
                    descriptor, credential, activation, ProfileLifecycleCodec.digest(activation));
        });
    }

    /** Re-enter with exactly the same Operation after a lost response; all phases resolve existing records. */
    public CompletableFuture<Result> execute(Operation operation) {
        return execute(operation, () -> true);
    }

    public CompletableFuture<Result> execute(Operation operation, BooleanSupplier admitting) {
        return cold(() -> admitting.getAsBoolean()
                ? lifecycle.prepareReplacementInstall(operation.source(), operation.plan(), metadata) : null)
                .thenCompose(prepared -> {
                    if (prepared == null || !admitting.getAsBoolean()) {
                        return CompletableFuture.completedFuture(failed(Status.CONFLICT));
                    }
                    if (prepared.page() == null) {
                        return CompletableFuture.completedFuture(failed(status(prepared.status())));
                    }
                    var installs = operation.plan().installs().stream().map(scope -> {
                        var target = target(operation, scope.targetBookie());
                        return target.control().install(ProfileControlRequest.install(scope, operation.descriptor(),
                                EngineProfile.SEGMENT_WAL_ENGINE, operation.credential()));
                    }).toList();
                    return CompletableFuture.allOf(installs.toArray(CompletableFuture[]::new))
                            .handle((done, failure) -> {
                        if (failure != null) {
                            return null;
                        }
                        var receipts = new ArrayList<ProfileInstallReceipt>();
                        for (int i = 0; i < installs.size(); i++) {
                            var scope = operation.plan().installs().get(i);
                            var result = installs.get(i).join();
                            if (!ProfileInstallReceipt.matches(scope, target(operation, scope.targetBookie())
                                    .control().hello(), result)) {
                                return null;
                            }
                            receipts.add(ProfileInstallReceipt.from(scope, target(operation, scope.targetBookie())
                                    .control().hello(), result));
                        }
                        return List.copyOf(receipts);
                    }).thenCompose(receipts -> receipts == null
                            ? CompletableFuture.completedFuture(failed(Status.UNCERTAIN))
                            : cold(() -> admitting.getAsBoolean()
                                ? publish(operation, receipts) : failed(Status.CONFLICT))
                                .thenCompose(published -> {
                                    if (published.status() != Status.AVAILABLE) {
                                        return CompletableFuture.completedFuture(published);
                                    }
                                    return admitting.getAsBoolean() ? activate(operation, published)
                                            : CompletableFuture.completedFuture(failed(Status.CONFLICT));
                                }));
                }).exceptionally(failure -> failed(Status.UNCERTAIN));
    }

    /** Explicit reconnect only: preserve every frozen field and reject changing target/incarnation. */
    public Operation reconnect(Operation operation, Map<Integer, Target> peers) {
        Map<Integer, Target> targets = Map.copyOf(peers);
        if (!targets.keySet().equals(operation.targets().keySet()) || targets.entrySet().stream().anyMatch(entry -> {
            var original = operation.targets().get(entry.getKey()).control().hello();
            var current = entry.getValue().control().hello();
            return !current.bookieId().equals(original.bookieId())
                    || !Arrays.equals(current.storageIncarnation(), original.storageIncarnation());
        })) {
            throw new IllegalArgumentException("Reconnect must preserve the original target/incarnation and operation");
        }
        return new Operation(operation.source(), operation.initial(), operation.before(), operation.plan(), targets,
                operation.ensemble(), operation.descriptor(), operation.credential(), operation.activationId(),
                operation.activationPayload());
    }

    private Result publish(Operation operation, List<ProfileInstallReceipt> receipts) {
        var changed = metadata.replaceEnsemble(operation.before(),
                operation.plan().fragmentStart(), operation.ensemble());
        if (changed.metadata() == null) {
            return failed(changed.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                    ? Status.CONFLICT : changed.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                        ? Status.QUARANTINED : Status.UNCERTAIN);
        }
        var page = ProfileReplacementPage.from(operation.source(), operation.initial(),
                new ProfileLedgerMetadataStore.MembershipSnapshot(operation.before().birthZxid(), changed.metadata()),
                operation.plan().fragmentStart(), receipts, operation.activationId(), operation.activationPayload());
        var prepared = lifecycle.prepareReplacement(operation.source(), page, metadata);
        if (prepared.page() == null) {
            return failed(status(prepared.status()));
        }
        var published = lifecycle.publishReplacement(operation.source(), prepared.page(), metadata);
        if (published.snapshot() == null) {
            return failed(status(published.status()));
        }
        if (published.snapshot().lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
            return failed(Status.DELETED);
        }
        var clients = new HashMap<BookieId, ProfileNormalDataClient>();
        operation.targets().values().forEach(target -> clients.put(target.bookie(), target.data()));
        return new Result(Status.AVAILABLE, changed.metadata(), prepared.page(), clients);
    }

    private CompletableFuture<Result> activate(Operation operation, Result published) {
        var targets = operation.targets().values().stream()
                .sorted(java.util.Comparator.comparing(target -> target.bookie().toString())).toList();
        var calls = targets.stream().map(target -> target.control().activate(
                ProfileControlRequest.operation(published.authority().page().activation(target.bookie().toString()))))
                .toList();
        return CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)).handle((done, failure) -> {
            if (failure != null) {
                return false;
            }
            for (int i = 0; i < calls.size(); i++) {
                var result = calls.get(i).join();
                var target = targets.get(i);
                var installed = published.authority().page().installs().stream()
                        .filter(value -> value.scope().targetBookie().equals(target.bookie().toString()))
                        .findFirst().orElseThrow();
                if (!result.durable() || result.localGeneration() <= installed.localInstallGeneration()
                        || result.ledgerId() != operation.plan().backlink().ledgerId()
                        || !result.operationIdHex().equals(HexFormat.of().formatHex(operation.activationId()))
                        || result.authorityGeneration() != operation.plan().controlGeneration()
                        || result.operation() != ProfileControlOperation.ACTIVATE_REPLACEMENT
                        || (result.outcome() != ProfileControlResult.Outcome.APPLIED
                            && result.outcome() != ProfileControlResult.Outcome.ALREADY_APPLIED)) {
                    return false;
                }
            }
            return true;
        }).thenCompose(valid -> !valid ? CompletableFuture.completedFuture(failed(Status.UNCERTAIN))
                : cold(() -> {
                    var current = lifecycle.read(operation.plan().backlink().ledgerId());
                    var actual = metadata.readMembership(operation.plan().backlink().ledgerId());
                    if (current.status() != ReadStatus.FOUND || actual.status() != ReadStatus.FOUND
                            || current.snapshot().lifecycle().state() != ProfileLifecycleHead.State.REPLACEMENT
                            || current.snapshot().lifecycle().sequence() != operation.plan().sequence()
                            || !Arrays.equals(current.snapshot().lifecycle().operationId(), operation.activationId())
                            || !published.authority().page().matchesMetadata(actual.snapshot())) {
                        return failed(Status.CONFLICT);
                    }
                    return published;
                }));
    }

    private static boolean credentialMatches(ProtectedProfileCredential credential, byte[] password) {
        try {
            return credential.matchesWireVerifier(DigestManager.generateMasterKey(password));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required master-key digest unavailable", impossible);
        }
    }

    private static Target target(Operation operation, String bookie) {
        return operation.targets().values().stream().filter(target -> target.bookie().toString().equals(bookie))
                .findFirst().orElseThrow();
    }
    private static Result failed(Status status) {
        return new Result(status, null, null, Map.of());
    }
    private static Status status(ZKProfileReservationStore.CreateStatus status) {
        return switch (status) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.UNCERTAIN;
        };
    }
    private <T> CompletableFuture<T> cold(Supplier<T> action) {
        try {
            return CompletableFuture.supplyAsync(action, cold);
        } catch (RuntimeException unavailable) {
            return CompletableFuture.failedFuture(unavailable);
        }
    }
}

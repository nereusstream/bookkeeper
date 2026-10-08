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

import io.netty.util.ReferenceCounted;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.ProfileRecoveryReadHandle;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.net.BookieId;

/** Borrowed bounded recovery coordination. Write-back, terminal cut and matching metadata close are distinct facts. */
public final class ProfileRecoveryCoordinator {
    public enum Status { FENCED_CONTEXT, DEFERRED, CONFLICT, QUARANTINED }

    public record Fence(String bookie, String storageIncarnation) { }

    /** Fences remain historical evidence on non-OK results; no result is RECOVERED_AND_CLOSED. */
    public record Result(Status status, ProfileRecoveryContext context, List<Fence> fences) {
        public Result {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(context, "context");
            fences = List.copyOf(fences);
        }
    }

    private record Prepared(ZKProfileReservationStore.RecoveryResult recovery, List<ProfileInstallReceipt> targets) { }
    private record Reply(String bookie, ProfileInitialControlClient peer, ProfileStatus status, Throwable failure) { }
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final ThreadPoolExecutor cold;
    private final Semaphore active = new Semaphore(1);

    public ProfileRecoveryCoordinator(ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ThreadPoolExecutor cold) {
        this.reservations = Objects.requireNonNull(reservations, "reservations");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.cold = Objects.requireNonNull(cold, "cold");
        if (!(cold.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue<?>)
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Recovery requires a bounded rejecting borrowed cold executor");
        }
    }

    public CompletableFuture<Result> fence(ZKProfileReservationStore.RecoveryResult accepted,
            Map<String, ProfileInitialControlClient> supplied, ProtectedProfileCredential credential) {
        if (accepted.context() == null) {
            throw new IllegalArgumentException("Fencing requires its accepted recovery context");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, accepted.context(), List.of()));
        }
        try {
            return fenceOnce(accepted, supplied, credential).whenComplete((value, failure) -> active.release()).copy();
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private CompletableFuture<Result> fenceOnce(ZKProfileReservationStore.RecoveryResult accepted,
            Map<String, ProfileInitialControlClient> supplied, ProtectedProfileCredential credential) {
        if (accepted.context() == null || supplied.size() > 64) {
            throw new IllegalArgumentException("Fencing requires a proved recovery context and bounded peer pool");
        }
        var context = accepted.context();
        var peers = Map.copyOf(supplied);
        Objects.requireNonNull(credential, "credential");
        CompletableFuture<Result> actual;
        try {
            actual = CompletableFuture.supplyAsync(() -> prepare(context), cold).thenCompose(prepared -> {
                if (prepared.recovery().context() == null
                        || !context.equals(prepared.recovery().context())) {
                    return CompletableFuture.completedFuture(new Result(
                            failureStatus(prepared.recovery().outcome()), context, List.of()));
                }
                var value = prepared.recovery().metadata().metadata().getValue();
                var ensemble = value.getAllEnsembles().lastEntry().getValue();
                List<CompletableFuture<Reply>> pending = new ArrayList<>();
                for (var bookie : ensemble) {
                    String name = bookie.toString();
                    var peer = peers.get(name);
                    boolean matches = peer != null && peer.hello().activeEngine() == 3
                            && peer.hello().bookieId().equals(name)
                            && prepared.targets().stream().anyMatch(receipt ->
                                receipt.scope().targetBookie().equals(name)
                                    && Arrays.equals(receipt.scope().targetStorageIncarnation(),
                                            peer.hello().storageIncarnation()));
                    if (!matches) {
                        pending.add(CompletableFuture.completedFuture(new Reply(name, peer, null, null)));
                        continue;
                    }
                    try {
                        pending.add(peer.fence(context.ledger(), context.operationId(), credential)
                                .handle((status, failure) -> new Reply(name, peer, status, failure)));
                    } catch (RuntimeException failure) {
                        pending.add(CompletableFuture.completedFuture(new Reply(name, peer, null, failure)));
                    }
                }
                return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).thenCompose(unused -> {
                    var replies = pending.stream().map(CompletableFuture::join).toList();
                    return CompletableFuture.supplyAsync(() -> {
                        var coverage = new RoundRobinDistributionSchedule(value.getWriteQuorumSize(),
                                value.getAckQuorumSize(), value.getEnsembleSize()).getCoverageSet();
                        List<Fence> proofs = new ArrayList<>();
                        for (int i = 0; i < replies.size(); i++) {
                            var reply = replies.get(i);
                            boolean fenced = reply.failure() == null && reply.status() != null
                                    && reply.status().statusClass() == ProfileStatus.StatusClass.OK
                                    && (reply.status().durableResult() == ProfileStatus.DurableResult.APPLIED
                                        || reply.status().durableResult()
                                            == ProfileStatus.DurableResult.ALREADY_APPLIED);
                            coverage.addBookie(i, fenced ? BKException.Code.OK : BKException.Code.ReadException);
                            if (fenced) {
                                proofs.add(new Fence(reply.bookie(),
                                        HexFormat.of().formatHex(reply.peer().hello().storageIncarnation())));
                            }
                        }
                        var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                        if (current.context() == null || !context.equals(current.context())) {
                            return new Result(failureStatus(current.outcome()), context, proofs);
                        }
                        return new Result(coverage.checkCovered() ? Status.FENCED_CONTEXT : Status.DEFERRED,
                                context, proofs);
                    }, cold);
                });
            });
        } catch (RejectedExecutionException rejected) {
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, context, List.of()));
        }
        return actual;
    }

    public enum ObservationStatus { OBSERVED_BOUNDARY, DEFERRED, CONFLICT, QUARANTINED }

    /** Read-only observations. Missing above the hint is not a final normal-tail or CLOSED authority. */
    public record Observation(ObservationStatus status, ProfileRecoveryContext context,
            org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence lac,
            List<ProfileRecoveryReadHandle.Probe> candidates, long missingEntry) {
        public Observation {
            candidates = List.copyOf(candidates);
        }
    }

    private record Readers(ZKProfileReservationStore.RecoveryResult recovery, List<ProfileInstallReceipt> permitted) { }

    public CompletableFuture<Observation> observe(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCandidates) {
        if (accepted.context() == null) {
            throw new IllegalArgumentException("Observation requires its accepted recovery context");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new Observation(ObservationStatus.DEFERRED,
                    accepted.context(), null, List.of(), -1));
        }
        try {
            return observeOnce(accepted, sdk, suppliedData, suppliedControl, credential, maxCandidates)
                    .whenComplete((value, failure) -> active.release()).copy();
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private CompletableFuture<Observation> observeOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCandidates) {
        if (maxCandidates < 1 || maxCandidates > 32 || suppliedData.size() > 64) {
            throw new IllegalArgumentException("Recovery observation requires a bounded candidate window and peers");
        }
        var data = Map.copyOf(suppliedData);
        var control = Map.copyOf(suppliedControl);
        var context = accepted.context();
        return fenceOnce(accepted, control, credential).thenCompose(fenced -> {
            if (fenced.status() != Status.FENCED_CONTEXT) {
                return CompletableFuture.completedFuture(new Observation(
                        observationStatus(fenced.status()), context, null, List.of(), -1));
            }
            return CompletableFuture.supplyAsync(() -> readers(context), cold).thenCompose(readers -> {
                if (readers.recovery().context() == null || !context.equals(readers.recovery().context())) {
                    return CompletableFuture.completedFuture(new Observation(observationStatus(
                            failureStatus(readers.recovery().outcome())), context, null, List.of(), -1));
                }
                var controls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
                control.forEach((name, peer) -> controls.put(BookieId.parse(name), peer));
                ProfileRecoveryReadHandle reader;
                try {
                    reader = ProfileRecoveryReadHandle.fromContext(sdk,
                            readers.recovery(), readers.permitted(), data, controls);
                } catch (java.security.GeneralSecurityException invalid) {
                    return CompletableFuture.failedFuture(invalid);
                }
                return reader.readLacEvidence().thenCompose(lac -> scan(reader, context, lac,
                        new ArrayList<>(), lac.prefixEntryId(), lac.prefixLength(), maxCandidates))
                        .thenCompose(observation -> CompletableFuture.supplyAsync(() -> {
                            var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                            return current.context() != null && context.equals(current.context()) ? observation
                                    : new Observation(observationStatus(failureStatus(current.outcome())), context,
                                            observation.lac(), observation.candidates(), -1);
                        }, cold)).whenComplete((value, failure) -> reader.closeAsync());
            });
        });
    }

    private Readers readers(ProfileRecoveryContext context) {
        var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
        if (current.context() == null || !context.equals(current.context())) {
            return new Readers(current, List.of());
        }
        var retained = reservations.retainedActivationTargets(context.ledger().ledgerId());
        if (retained.status()
                != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND) {
            return new Readers(new ZKProfileReservationStore.RecoveryResult(retained.status()
                    == org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.UNAVAILABLE
                        ? ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                        : ProfileLedgerMetadataStore.Outcome.QUARANTINED, null, null), List.of());
        }
        var currentTargets = prepare(context).targets();
        var permitted = new java.util.LinkedHashMap<String, ProfileInstallReceipt>();
        for (var receipt : retained.targets()) {
            var old = permitted.put(receipt.scope().targetBookie(), receipt);
            if (old != null && !Arrays.equals(old.scope().targetStorageIncarnation(),
                    receipt.scope().targetStorageIncarnation())) {
                return new Readers(new ZKProfileReservationStore.RecoveryResult(
                        ProfileLedgerMetadataStore.Outcome.CONFLICT, null, null), List.of());
            }
        }
        currentTargets.forEach(receipt -> permitted.put(receipt.scope().targetBookie(), receipt));
        return new Readers(current, List.copyOf(permitted.values()));
    }

    private CompletableFuture<Observation> scan(ProfileRecoveryReadHandle reader,
            ProfileRecoveryContext context, org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence lac,
            List<ProfileRecoveryReadHandle.Probe> candidates,
            long prefix, long length, int remaining) {
        if (remaining == 0 || prefix == Long.MAX_VALUE) {
            return CompletableFuture.completedFuture(new Observation(ObservationStatus.DEFERRED,
                    context, lac, candidates, -1));
        }
        return reader.probe(prefix + 1).thenCompose(probe -> {
            if (probe.outcome() == ProfileRecoveryReadHandle.ProbeOutcome.ABSENT) {
                return CompletableFuture.completedFuture(new Observation(ObservationStatus.OBSERVED_BOUNDARY,
                        context, lac, candidates, prefix + 1));
            }
            int payload = probe.payload().remaining();
            if (length > Long.MAX_VALUE - payload || probe.length() != length + payload) {
                return CompletableFuture.completedFuture(new Observation(ObservationStatus.QUARANTINED,
                        context, lac, candidates, -1));
            }
            candidates.add(probe);
            return scan(reader, context, lac, candidates, prefix + 1, probe.length(), remaining - 1);
        });
    }

    private static ObservationStatus observationStatus(Status status) {
        return switch (status) {
            case CONFLICT -> ObservationStatus.CONFLICT;
            case QUARANTINED -> ObservationStatus.QUARANTINED;
            default -> ObservationStatus.DEFERRED;
        };
    }

    public enum PrefixStatus { VERIFIED_NORMAL_TAIL, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS }

    public enum PrefixReason {
        NORMAL_TAIL, LIMIT, CANCELLED, DEADLINE, LAC_UNAVAILABLE, SOURCE_UNAVAILABLE, REQUIRED_HOLE,
        PAYLOAD_CONFLICT, LENGTH_CONFLICT, PERMISSION, AUTHORITY_CHANGED,
        AUTHORITY_UNAVAILABLE, AUTHORITY_UNRECOVERABLE
    }

    /** Read evidence only. Required recovery Adds and matching durable CLOSED still precede recovery success. */
    public record PrefixResult(PrefixStatus status, PrefixReason reason, ProfileRecoveryContext context,
            ProfileLedgerMetadataStore.MembershipSnapshot metadata, long requiredFrontier,
            long prefixEntryId, long prefixLength, long unresolvedEntry,
            List<ProfileRecoveryReadHandle.SourceEvidence> sources) {
        public PrefixResult {
            sources = List.copyOf(sources);
        }
    }

    /** Check the complete required prefix and then the open tail, holding one payload and one exact source set. */
    public CompletableFuture<PrefixResult> verifyPrefix(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCoordinates, Duration maxDuration) {
        long durationNanos = maxDuration.toNanos();
        if (accepted.context() == null || maxCoordinates < 1
                || suppliedData.size() > 64 || suppliedControl.size() > 64 || durationNanos <= 0) {
            throw new IllegalArgumentException("Prefix verification requires its frozen context and finite bounds");
        }
        var context = accepted.context();
        if (suppliedData.values().stream().anyMatch(peer -> !peer.hello().capabilities().contains(
                org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.EXPLICIT_CAPABILITY))) {
            return CompletableFuture.completedFuture(new PrefixResult(PrefixStatus.DEFERRED,
                    PrefixReason.LAC_UNAVAILABLE, context, null, -1, -1, 0, 0, List.of()));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.LIMIT,
                    context, null, -1, -1, 0, 0, List.of()));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = verifyPrefixOnce(accepted, sdk, suppliedData, suppliedControl, credential,
                    maxCoordinates, stopped);
            var caller = actual.whenComplete((result, failure) -> active.release()).copy();
            java.util.concurrent.ScheduledFuture<?> deadline;
            try {
                deadline = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    caller.complete(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.DEADLINE,
                            context, null, -1, -1, 0, 0, List.of()));
                }, durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(new PrefixResult(PrefixStatus.DEFERRED, PrefixReason.LIMIT,
                        context, null, -1, -1, 0, 0, List.of()));
                return caller;
            }
            actual.whenComplete((result, failure) -> deadline.cancel(false));
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<PrefixResult> verifyPrefixOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ClientContext sdk, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, ProtectedProfileCredential credential,
            int maxCoordinates, AtomicBoolean stopped) {
        var context = accepted.context();
        var data = Map.copyOf(suppliedData);
        var control = Map.copyOf(suppliedControl);
        return fenceOnce(accepted, control, credential).thenCompose(fenced -> {
            if (stopped.get() || fenced.status() != Status.FENCED_CONTEXT) {
                return CompletableFuture.completedFuture(new PrefixResult(stopped.get()
                        ? PrefixStatus.DEFERRED : prefixStatus(fenced.status()), stopped.get()
                            ? PrefixReason.CANCELLED : PrefixReason.SOURCE_UNAVAILABLE,
                        context, null, -1, -1, 0, 0, List.of()));
            }
            return CompletableFuture.supplyAsync(() -> readers(context), cold).thenCompose(ready -> {
                if (ready.recovery().context() == null || !context.equals(ready.recovery().context())) {
                    return CompletableFuture.completedFuture(new PrefixResult(
                            prefixStatus(failureStatus(ready.recovery().outcome())),
                            authorityReason(ready.recovery().outcome()), context, null, -1, -1, 0, 0, List.of()));
                }
                var controls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
                control.forEach((name, peer) -> controls.put(BookieId.parse(name), peer));
                ProfileRecoveryReadHandle reader;
                try {
                    reader = ProfileRecoveryReadHandle.fromContext(sdk, ready.recovery(), ready.permitted(),
                            data, controls);
                } catch (java.security.GeneralSecurityException invalid) {
                    return CompletableFuture.failedFuture(invalid);
                }
                var verify = new PrefixVerification(reader, context, ready.recovery().metadata(),
                        stopped, maxCoordinates);
                var checked = reader.readLacEvidence().handle((lac, failure) -> {
                    if (failure != null) {
                        if (!(cause(failure) instanceof BKException)) {
                            verify.done.completeExceptionally(failure);
                            return verify.done;
                        }
                        boolean conflict = cause(failure)
                                instanceof org.apache.bookkeeper.client.ProfileLedgerHandle
                                    .LacLengthConflictException;
                        boolean unauthorized = BKException.getExceptionCode(cause(failure))
                                == BKException.Code.UnauthorizedAccessException;
                        verify.finish(conflict ? PrefixStatus.QUARANTINED : unauthorized
                                ? PrefixStatus.CONFLICT : PrefixStatus.DEFERRED,
                                conflict ? PrefixReason.LENGTH_CONFLICT : unauthorized
                                    ? PrefixReason.PERMISSION : PrefixReason.LAC_UNAVAILABLE, List.of());
                    } else {
                        verify.required = lac.prefixEntryId();
                        verify.start();
                    }
                    return verify.done;
                }).thenCompose(value -> value);
                return checked.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                    var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                    if (current.context() == null || !context.equals(current.context())
                            || current.metadata().birthZxid() != result.metadata().birthZxid()
                            || !current.metadata().metadata().getVersion()
                                .equals(result.metadata().metadata().getVersion())
                            || !ProfileLedgerMetadataStore.sameMetadata(
                                    result.metadata().metadata().getValue(),
                                    current.metadata().metadata().getValue())) {
                        return new PrefixResult(current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                ? PrefixStatus.QUARANTINED : current.outcome()
                                    == ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN
                                        ? PrefixStatus.DEFERRED : PrefixStatus.CONFLICT,
                                authorityReason(current.outcome()), context, result.metadata(),
                                result.requiredFrontier(),
                                result.prefixEntryId(), result.prefixLength(), result.unresolvedEntry(),
                                result.sources());
                    }
                    return result;
                }, cold)).whenComplete((result, failure) -> reader.closeAsync());
            });
        });
    }

    private static PrefixStatus prefixStatus(Status status) {
        return switch (status) {
            case CONFLICT -> PrefixStatus.CONFLICT;
            case QUARANTINED -> PrefixStatus.QUARANTINED;
            default -> PrefixStatus.DEFERRED;
        };
    }

    private static PrefixReason authorityReason(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case DURABILITY_UNKNOWN -> PrefixReason.AUTHORITY_UNAVAILABLE;
            case QUARANTINED -> PrefixReason.AUTHORITY_UNRECOVERABLE;
            default -> PrefixReason.AUTHORITY_CHANGED;
        };
    }

    private static Throwable cause(Throwable failure) {
        while ((failure instanceof java.util.concurrent.CompletionException
                || failure instanceof java.util.concurrent.ExecutionException) && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static final class PrefixVerification {
        private final ProfileRecoveryReadHandle reader;
        private final ProfileRecoveryContext context;
        private final ProfileLedgerMetadataStore.MembershipSnapshot metadata;
        private final AtomicBoolean stopped;
        private int remaining;
        private long required = -1;
        private long next;
        private long length;
        private final CompletableFuture<PrefixResult> done = new CompletableFuture<>();

        PrefixVerification(ProfileRecoveryReadHandle reader, ProfileRecoveryContext context,
                ProfileLedgerMetadataStore.MembershipSnapshot metadata, AtomicBoolean stopped, int bound) {
            this.reader = reader;
            this.context = context;
            this.metadata = metadata;
            this.stopped = stopped;
            remaining = bound;
        }

        void start() {
            if (stopped.get() || remaining-- == 0 || next == Long.MAX_VALUE) {
                finish(PrefixStatus.DEFERRED, stopped.get() ? PrefixReason.CANCELLED : PrefixReason.LIMIT, List.of());
                return;
            }
            reader.probeEvidence(next).whenComplete((evidence, failure) -> {
                if (failure != null && !(cause(failure) instanceof BKException)) {
                    done.completeExceptionally(failure);
                } else if (failure != null || stopped.get()) {
                    finish(PrefixStatus.DEFERRED, stopped.get() ? PrefixReason.CANCELLED
                            : PrefixReason.SOURCE_UNAVAILABLE, List.of());
                } else if (evidence.sources().stream().anyMatch(source -> source.outcome()
                        == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                    finish(PrefixStatus.CONFLICT, PrefixReason.PERMISSION, evidence.sources());
                } else if (evidence.conflict()) {
                    finish(PrefixStatus.QUARANTINED, PrefixReason.PAYLOAD_CONFLICT, evidence.sources());
                } else if (evidence.candidate() != null) {
                    var candidate = evidence.candidate();
                    int bytes = candidate.payload().remaining();
                    if (length > Long.MAX_VALUE - bytes || candidate.length() != length + bytes) {
                        finish(PrefixStatus.QUARANTINED, PrefixReason.LENGTH_CONFLICT, evidence.sources());
                    } else {
                        length = candidate.length();
                        next++;
                        start(); // Each probe uses the original ordered executor; no range-sized future chain.
                    }
                } else if (next <= required) {
                    finish(evidence.exhausted() ? PrefixStatus.DATA_LOSS : PrefixStatus.DEFERRED,
                            PrefixReason.REQUIRED_HOLE, evidence.sources());
                } else {
                    finish(evidence.absenceCovered() ? PrefixStatus.VERIFIED_NORMAL_TAIL : PrefixStatus.DEFERRED,
                            evidence.absenceCovered() ? PrefixReason.NORMAL_TAIL : PrefixReason.SOURCE_UNAVAILABLE,
                            evidence.sources());
                }
            });
        }

        void finish(PrefixStatus status, PrefixReason reason, List<ProfileRecoveryReadHandle.SourceEvidence> sources) {
            done.complete(new PrefixResult(status, reason, context, metadata, required,
                    next - 1, length, next, sources));
        }
    }

    private Prepared prepare(ProfileRecoveryContext context) {
        var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
        if (current.context() == null || !context.equals(current.context())) {
            return new Prepared(current, List.of());
        }
        try {
            return new Prepared(current, reservations.recoveryCurrentTargets(context,
                    System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(1)));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new java.util.concurrent.CompletionException(interrupted);
        } catch (java.util.concurrent.TimeoutException unavailable) {
            throw new java.util.concurrent.CompletionException(unavailable);
        }
    }

    public enum RecoveryStatus { RECOVERED_AND_CLOSED, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS, DELETED }

    public record RecoveryResult(long ledgerId, RecoveryStatus status, PrefixReason reason,
            ProfileRecoveryContext context, long prefixEntryId, long prefixLength, PrefixResult evidence,
            ZKProfileReservationStore.ClosedRecoveryResult publication) { }

    private record RecoveryStart(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.ClosedRecoveryResult closed, RecoveryStatus failure, PrefixReason reason) {
        RecoveryStart(ZKProfileReservationStore.RecoveryResult accepted,
                ZKProfileReservationStore.ClosedRecoveryResult closed, RecoveryStatus failure) {
            this(accepted, closed, failure, failure == RecoveryStatus.DEFERRED ? PrefixReason.AUTHORITY_UNAVAILABLE
                    : failure == RecoveryStatus.QUARANTINED ? PrefixReason.AUTHORITY_UNRECOVERABLE
                        : PrefixReason.AUTHORITY_CHANGED);
        }
    }

    /** One actual attempt owns the existing gate across metadata, point oracle, SELF write-back and final close. */
    public CompletableFuture<RecoveryResult> recover(long ledger, byte[] operationId, ClientContext sdk,
            byte[] canonicalDescriptor, org.apache.bookkeeper.common.profile.ProfileDescriptorValidator validator,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, int maxCoordinates, Duration maxDuration) {
        long duration = maxDuration.toNanos();
        if (ledger < 0 || maxCoordinates < 1 || suppliedData.size() > 64 || suppliedControl.size() > 64
                || duration <= 0 || maxDuration.compareTo(Duration.ofMinutes(1)) > 0
                || canonicalDescriptor.length > org.apache.bookkeeper.common.profile.ProfileDescriptorCodec
                    .MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Recovery requires bounded peers, descriptor, coordinates and duration");
        }
        ProfileRecoveryReadHandle.requireCompatibleSdk(sdk);
        if (sdk.isClientClosed()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new RecoveryResult(ledger, RecoveryStatus.DEFERRED,
                    PrefixReason.LIMIT, null, -1, 0, null, null));
        }
        var stopped = new AtomicBoolean();
        var caller = new CompletableFuture<RecoveryResult>();
        var progress = new java.util.concurrent.atomic.AtomicReference<>(new RecoveryResult(ledger,
                RecoveryStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE, null, -1, 0, null, null));
        try {
            byte[] operation = ProfileLifecycleHead.fixed(operationId, 16, true);
            byte[] descriptor = canonicalDescriptor.clone();
            var decoded = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.decode(descriptor, validator);
            byte[] identity = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.identityOf(decoded, validator)
                    .toBytes();
            var key = credential.copy();
            var data = Map.copyOf(suppliedData);
            var controls = Map.copyOf(suppliedControl);
            var actual = CompletableFuture.supplyAsync(
                    () -> startRecovery(ledger, operation, decoded, identity, key, stopped), cold)
                    .thenCompose(start -> {
                if (start.closed() != null) {
                    return CompletableFuture.completedFuture(new RecoveryResult(ledger,
                            RecoveryStatus.RECOVERED_AND_CLOSED, PrefixReason.NORMAL_TAIL,
                            start.accepted().context(),
                            start.closed().metadata().metadata().getValue().getLastEntryId(),
                            start.closed().metadata().metadata().getValue().getLength(), null, start.closed()));
                }
                if (start.accepted() == null || start.accepted().context() == null) {
                    return CompletableFuture.completedFuture(new RecoveryResult(ledger, start.failure(),
                            start.reason(), null, -1, 0, null, null));
                }
                var accepted = start.accepted();
                var context = accepted.context();
                progress.set(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.SOURCE_UNAVAILABLE,
                        context, -1, 0, null, null));
                return CompletableFuture.supplyAsync(() -> reservations.readSelectedPrefix(metadata, context), cold)
                        .thenCompose(selected -> {
                    if (stopped.get()) {
                        return CompletableFuture.completedFuture(progress.get());
                    }
                    if (selected.snapshot() != null) {
                        return recoverSelected(accepted, selected.snapshot(), null, sdk, descriptor, key, data,
                                controls, stopped, progress);
                    }
                    if (selected.status() != ZKProfileReservationStore.CreateStatus.CONFLICT) {
                        return CompletableFuture.completedFuture(recoveryFailure(ledger, context,
                                completionStatus(selected.status()), null));
                    }
                    if (data.values().stream().anyMatch(peer -> !peer.hello().capabilities().contains(
                            org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.EXPLICIT_CAPABILITY))) {
                        return CompletableFuture.completedFuture(progress.get());
                    }
                    return verifyPrefixOnce(accepted, sdk, data, controls, key, maxCoordinates, stopped)
                            .thenCompose(proof -> {
                        if (proof.status() != PrefixStatus.VERIFIED_NORMAL_TAIL || stopped.get()) {
                            return CompletableFuture.completedFuture(new RecoveryResult(ledger,
                                    stopped.get() || proof.status() == PrefixStatus.DEFERRED ? RecoveryStatus.DEFERRED
                                        : proof.status() == PrefixStatus.DATA_LOSS ? RecoveryStatus.DATA_LOSS
                                            : proof.status() == PrefixStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                                                : RecoveryStatus.CONFLICT,
                                    proof.reason(), context, proof.prefixEntryId(), proof.prefixLength(), proof, null));
                        }
                        return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                                : reservations.selectPrefix(metadata, proof), cold).thenCompose(published -> {
                            if (published == null || published.snapshot() == null) {
                                return CompletableFuture.completedFuture(recoveryFailure(ledger, context,
                                        published == null ? CompletionStatus.DEFERRED
                                            : completionStatus(published.status()), proof));
                            }
                            return recoverSelected(accepted, published.snapshot(), proof, sdk, descriptor, key, data,
                                    controls, stopped, progress);
                        });
                    });
                });
            });
            var timer = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.ScheduledFuture<?>>();
            actual.whenComplete((result, failure) -> {
                var scheduled = timer.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                active.release();
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else {
                    caller.complete(result);
                }
            });
            try {
                var scheduled = sdk.getScheduler().schedule(() -> {
                    stopped.set(true);
                    var known = progress.get();
                    caller.complete(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.DEADLINE,
                            known.context(), known.prefixEntryId(), known.prefixLength(), known.evidence(), null));
                }, duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                timer.set(scheduled);
                if (actual.isDone()) {
                    scheduled.cancel(false);
                }
            } catch (RejectedExecutionException unavailable) {
                stopped.set(true);
                caller.complete(new RecoveryResult(ledger, RecoveryStatus.DEFERRED, PrefixReason.LIMIT,
                        null, -1, 0, null, null));
            }
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException invalid) {
            active.release();
            throw invalid;
        }
    }

    private RecoveryStart startRecovery(long ledger, byte[] operation,
            org.apache.bookkeeper.common.profile.ProfileDescriptor descriptor, byte[] identity,
            ProtectedProfileCredential key, AtomicBoolean stopped) {
        if (stopped.get()) {
            return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
        }
        var member = metadata.readMembership(ledger);
        if (member.status() != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND) {
            return new RecoveryStart(null, null, member.status()
                    == org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.UNAVAILABLE
                        ? RecoveryStatus.DEFERRED : RecoveryStatus.QUARANTINED);
        }
        var value = member.snapshot().metadata().getValue();
        byte[] masterKey;
        try {
            masterKey = org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(value.getPassword());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new java.util.concurrent.CompletionException(impossible);
        }
        if (value.getDigestType() != org.apache.bookkeeper.client.api.DigestType.CRC32C
                || value.getEnsembleSize() != 3 || value.getWriteQuorumSize() != 3
                || value.getAckQuorumSize() < 2 || value.getAckQuorumSize() > 3
                || descriptor.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || descriptor.ensembleSize() != value.getEnsembleSize()
                || descriptor.writeQuorumSize() != value.getWriteQuorumSize()
                || descriptor.ackQuorumSize() != value.getAckQuorumSize()) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        if (!key.matchesWireVerifier(masterKey)) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT, PrefixReason.PERMISSION);
        }
        if (value.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.CLOSED) {
            var read = reservations.readClosedForOpen(metadata, ledger);
            if (read.snapshot() == null) {
                return new RecoveryStart(null, null, read.status() == ZKProfileReservationStore.ReadOnlyStatus.DELETED
                        ? RecoveryStatus.DELETED : read.status() == ZKProfileReservationStore.ReadOnlyStatus.UNAVAILABLE
                            ? RecoveryStatus.DEFERRED : RecoveryStatus.QUARANTINED);
            }
            var snapshot = read.snapshot();
            return Arrays.equals(identity, snapshot.context().ledger().descriptorIdentity())
                    ? new RecoveryStart(new ZKProfileReservationStore.RecoveryResult(
                        ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, snapshot.context(),
                        snapshot.closed().metadata()), snapshot.closed(), null)
                    : new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        if (stopped.get()) {
            return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
        }
        ZKProfileReservationStore.RecoveryResult accepted;
        if (value.getState() == org.apache.bookkeeper.client.api.LedgerMetadata.State.IN_RECOVERY) {
            accepted = reservations.readRecovery(metadata, ledger);
        } else {
            var captured = reservations.captureRecovery(metadata, ledger, operation);
            if (captured.plan() == null || !Arrays.equals(identity,
                    captured.plan().context().ledger().descriptorIdentity())) {
                return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
            }
            if (stopped.get()) {
                return new RecoveryStart(null, null, RecoveryStatus.DEFERRED);
            }
            accepted = reservations.beginRecovery(metadata, captured.plan());
        }
        if (accepted.context() != null && !Arrays.equals(identity, accepted.context().ledger().descriptorIdentity())) {
            return new RecoveryStart(null, null, RecoveryStatus.CONFLICT);
        }
        return new RecoveryStart(accepted, null, accepted.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                ? RecoveryStatus.QUARANTINED : accepted.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                    ? RecoveryStatus.CONFLICT : RecoveryStatus.DEFERRED);
    }

    private CompletableFuture<RecoveryResult> recoverSelected(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.BoundarySnapshot selected, PrefixResult proof, ClientContext sdk,
            byte[] descriptor, ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, AtomicBoolean stopped,
            java.util.concurrent.atomic.AtomicReference<RecoveryResult> progress) {
        var context = accepted.context();
        progress.set(new RecoveryResult(context.ledger().ledgerId(), RecoveryStatus.DEFERRED,
                PrefixReason.SOURCE_UNAVAILABLE, context, selected.boundary().prefixEntryId(),
                selected.boundary().prefixLength(), proof, null));
        return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, 0)
                .thenCompose(failed -> {
            if (failed != null || stopped.get()) {
                return CompletableFuture.completedFuture(failed == null ? progress.get() : failed);
            }
            return finishRecoveryOnce(context, controls, stopped).thenApply(closed ->
                    closed.status() == CompletionStatus.CLOSED_METADATA_DURABLE
                        ? new RecoveryResult(context.ledger().ledgerId(), RecoveryStatus.RECOVERED_AND_CLOSED,
                            PrefixReason.NORMAL_TAIL, context, selected.boundary().prefixEntryId(),
                            selected.boundary().prefixLength(), proof, closed.publication())
                        : recoveryFailure(context.ledger().ledgerId(), context, closed.status(), proof));
        });
    }

    private CompletableFuture<RecoveryResult> recoverMembers(ProfileRecoveryContext context,
            ZKProfileReservationStore.BoundarySnapshot selected, ClientContext sdk, byte[] descriptor,
            ProtectedProfileCredential key, Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, AtomicBoolean stopped, int member) {
        if (member == 3 || stopped.get()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.supplyAsync(
                () -> reservations.readRecovery(metadata, context.ledger().ledgerId()), cold)
                .thenCompose(current -> {
            if (current.context() == null || !context.equals(current.context())) {
                return CompletableFuture.completedFuture(recoveryFailure(context.ledger().ledgerId(), context,
                        current.outcome() == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                            ? CompletionStatus.QUARANTINED : CompletionStatus.CONFLICT, null));
            }
            var value = current.metadata().metadata().getValue();
            if (selected.boundary().prefixEntryId() < value.getAllEnsembles().lastKey()) {
                return CompletableFuture.completedFuture(null);
            }
            String name = value.getAllEnsembles().lastEntry().getValue().get(member).toString();
            if (!data.containsKey(BookieId.parse(name)) || !controls.containsKey(name)) {
                return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, member + 1);
            }
            return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                    : reservations.prepareLastSelfRepair(metadata, context, member), cold).thenCompose(plan -> {
                if (plan == null || plan.prepared() == null) {
                    return CompletableFuture.completedFuture(recoveryFailure(context.ledger().ledgerId(), context,
                            plan == null ? CompletionStatus.DEFERRED : completionStatus(plan.status()), null));
                }
                var repairedTarget = plan.committed() != null
                        ? closeRepair(plan.committed(), plan.prepared().intent(), controls.get(name), 0)
                            .thenApply(result -> result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                                ? new RepairResult(RepairStatus.REPAIRED_LAST_FRAGMENT, 0, result.publication())
                                : result)
                        : repairFragmentOnce(plan.recovery(), plan.prepared(), sdk, descriptor, key, data, controls,
                            plan.closeOperationId(), true, stopped);
                return repairedTarget.thenCompose(repaired -> {
                    if (repaired.status() != RepairStatus.REPAIRED_LAST_FRAGMENT) {
                        return CompletableFuture.completedFuture(new RecoveryResult(context.ledger().ledgerId(),
                                repaired.status() == RepairStatus.DATA_LOSS ? RecoveryStatus.DATA_LOSS
                                    : repaired.status() == RepairStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                                        : repaired.status() == RepairStatus.CONFLICT ? RecoveryStatus.CONFLICT
                                            : RecoveryStatus.DEFERRED,
                                PrefixReason.SOURCE_UNAVAILABLE, context, selected.boundary().prefixEntryId(),
                                selected.boundary().prefixLength(), null, null));
                    }
                    return recoverMembers(context, selected, sdk, descriptor, key, data, controls, stopped, member + 1);
                });
            });
        });
    }

    private static RecoveryResult recoveryFailure(long ledger, ProfileRecoveryContext context, CompletionStatus status,
            PrefixResult proof) {
        return new RecoveryResult(ledger, status == CompletionStatus.QUARANTINED ? RecoveryStatus.QUARANTINED
                : status == CompletionStatus.CONFLICT ? RecoveryStatus.CONFLICT : RecoveryStatus.DEFERRED,
                status == CompletionStatus.DEFERRED ? PrefixReason.SOURCE_UNAVAILABLE
                    : status == CompletionStatus.QUARANTINED ? PrefixReason.AUTHORITY_UNRECOVERABLE
                        : PrefixReason.AUTHORITY_CHANGED,
                context, proof == null ? -1 : proof.prefixEntryId(),
                proof == null ? 0 : proof.prefixLength(), proof, null);
    }

    public enum CompletionStatus { CLOSED_METADATA_DURABLE, DEFERRED, CONFLICT, QUARANTINED }

    /** Standard CLOSED fact only; not a production readiness or a globally revoked read lease. */
    public record CompletionResult(CompletionStatus status, ProfileRecoveryContext context,
            ZKProfileReservationStore.ClosedRecoveryResult publication, int terminalMembers) { }

    /** Resolve each current full-copy target through actual UIC4, count distinct original quorum slots, then CAS. */
    public CompletableFuture<CompletionResult> finishRecovery(ProfileRecoveryContext context,
            Map<String, ProfileInitialControlClient> supplied) {
        if (supplied.size() > 64) {
            throw new IllegalArgumentException("Recovery close peer pool exceeds its bound");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new CompletionResult(CompletionStatus.DEFERRED,
                    context, null, 0));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = finishRecoveryOnce(context, supplied, stopped);
            var caller = actual.whenComplete((result, failure) -> active.release()).copy();
            caller.whenComplete((result, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<CompletionResult> finishRecoveryOnce(ProfileRecoveryContext context,
            Map<String, ProfileInitialControlClient> supplied, AtomicBoolean stopped) {
        var peers = Map.copyOf(supplied);
        return CompletableFuture.supplyAsync(() -> reservations.readClosedRecovery(metadata, context), cold)
                .thenCompose(existing -> {
            if (existing.metadata() != null) {
                return CompletableFuture.completedFuture(new CompletionResult(
                        CompletionStatus.CLOSED_METADATA_DURABLE, context, existing, 0));
            }
            return CompletableFuture.supplyAsync(() -> reservations.closingPlan(metadata, context), cold)
                    .thenCompose(prepared -> {
                if (stopped.get() || prepared.plan() == null) {
                    return CompletableFuture.completedFuture(new CompletionResult(
                            stopped.get() ? CompletionStatus.DEFERRED : completionStatus(prepared.status()),
                            context, null, 0));
                }
                var plan = prepared.plan();
                var replies = new ArrayList<CompletableFuture<ZKProfileReservationStore.TerminalAck>>();
                for (var target : plan.targets()) {
                    var peer = peers.get(target.scope().targetBookie());
                    if (peer == null || !peer.hello().capabilities().contains(
                            org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec
                                .terminalCapability(target.terminal() != null))
                            || !Arrays.equals(peer.hello().storageIncarnation(),
                            target.scope().targetStorageIncarnation())) {
                        continue;
                    }
                    try {
                        var requested = target.terminal() != null ? peer.terminalClose(target.request())
                                : peer.recoveryClose(target.request());
                        replies.add(requested.handle((reply, failure) ->
                                        failure == null && reply != null && reply.durable()
                                            && (reply.outcome() == org.apache.bookkeeper.common.profile.control
                                                .ProfileControlResult.Outcome.APPLIED
                                                || reply.outcome() == org.apache.bookkeeper.common.profile.control
                                                    .ProfileControlResult.Outcome.ALREADY_APPLIED)
                                            ? new ZKProfileReservationStore.TerminalAck(
                                                target, peer.hello(), reply) : null));
                    } catch (RuntimeException rejected) {
                        replies.add(CompletableFuture.failedFuture(rejected));
                    }
                }
                return CompletableFuture.allOf(replies.toArray(CompletableFuture[]::new)).thenCompose(unused -> {
                    var acknowledgments = replies.stream().map(CompletableFuture::join)
                            .filter(Objects::nonNull).toList();
                    if (stopped.get()) {
                        return CompletableFuture.completedFuture(new CompletionResult(
                                CompletionStatus.DEFERRED, context, null, acknowledgments.size()));
                    }
                    return CompletableFuture.supplyAsync(() -> stopped.get() ? null
                            : reservations.closeRecovery(metadata, plan, acknowledgments), cold)
                                .thenApply(published -> new CompletionResult(published == null
                                        ? CompletionStatus.DEFERRED : published.metadata() != null
                                            ? CompletionStatus.CLOSED_METADATA_DURABLE
                                            : published.outcome() == ProfileLedgerMetadataStore.Outcome.CONFLICT
                                                ? CompletionStatus.CONFLICT : published.outcome()
                                                    == ProfileLedgerMetadataStore.Outcome.QUARANTINED
                                                        ? CompletionStatus.QUARANTINED : CompletionStatus.DEFERRED,
                                        context, published, acknowledgments.size()));
                });
            });
        });
    }

    /** Independent closed open; supplied clients remain borrowed. Cancellation closes an undelivered handle. */
    public CompletableFuture<org.apache.bookkeeper.client.LedgerHandle> openReadOnly(long ledger, ClientContext sdk,
            byte[] password, Map<BookieId, ProfileNormalDataClient> supplied, int maxReadEntries) {
        if (supplied.size() > 64 || maxReadEntries < 1 || maxReadEntries > 256) {
            throw new IllegalArgumentException("Readonly pool or request bound is invalid");
        }
        if (sdk.isClientClosed()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.ClientClosedException));
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException));
        }
        var caller = new CompletableFuture<org.apache.bookkeeper.client.LedgerHandle>();
        try {
            byte[] owned = password.clone();
            var peers = Map.copyOf(supplied);
            CompletableFuture.supplyAsync(() -> {
                var read = reservations.readClosedForOpen(metadata, ledger);
                if (read.snapshot() == null) {
                    throw new java.util.concurrent.CompletionException(readOnlyFailure(read.status()));
                }
                org.apache.bookkeeper.client.LedgerHandle handle = null;
                try {
                    handle = org.apache.bookkeeper.client.ProfileLedgerHandle.fromClosedRead(sdk, read.snapshot(),
                            owned, peers, maxReadEntries);
                    var after = reservations.readClosedForOpen(metadata, ledger);
                    if (after.snapshot() == null) {
                        throw readOnlyFailure(after.status());
                    }
                    var before = read.snapshot();
                    var next = after.snapshot();
                    if (!before.context().equals(next.context()) || before.closed().metadata().birthZxid()
                            != next.closed().metadata().birthZxid()
                            || !before.closed().metadata().metadata().getVersion()
                                .equals(next.closed().metadata().metadata().getVersion())
                            || !ProfileLedgerMetadataStore.sameMetadata(
                                before.closed().metadata().metadata().getValue(),
                                next.closed().metadata().metadata().getValue())
                            || !before.sources().equals(next.sources())) {
                        throw BKException.create(BKException.Code.MetadataVersionException);
                    }
                    if (sdk.isClientClosed()) {
                        throw BKException.create(BKException.Code.ClientClosedException);
                    }
                    return handle;
                } catch (Exception rejected) {
                    if (handle != null) {
                        handle.asyncClose((code, ignored, ctx) -> { }, null);
                    }
                    throw new java.util.concurrent.CompletionException(rejected);
                }
            }, cold).whenComplete((handle, failure) -> {
                active.release();
                if (failure != null) {
                    caller.completeExceptionally(failure);
                } else if (!caller.complete(handle)) {
                    handle.asyncClose((code, ignored, ctx) -> { }, null);
                }
            });
            return caller;
        } catch (RuntimeException failedAdmission) {
            active.release();
            throw failedAdmission;
        }
    }

    private static BKException readOnlyFailure(ZKProfileReservationStore.ReadOnlyStatus status) {
        return BKException.create(switch (status) {
            case DELETED -> BKException.Code.NoSuchLedgerExistsException;
            case NOT_CLOSED -> BKException.Code.IllegalOpException;
            case UNAVAILABLE -> BKException.Code.ReadException;
            case CONFLICT, QUARANTINED -> BKException.Code.MetadataVersionException;
            case READABLE_CLOSED -> throw new IllegalArgumentException("Readonly admission was successful");
        });
    }

    private static CompletionStatus completionStatus(ZKProfileReservationStore.CreateStatus status) {
        return switch (status) {
            case CONFLICT -> CompletionStatus.CONFLICT;
            case QUARANTINED -> CompletionStatus.QUARANTINED;
            default -> CompletionStatus.DEFERRED;
        };
    }

    public enum RepairStatus {
        REPAIRED_HISTORICAL_FRAGMENT, REPAIRED_LAST_FRAGMENT, DEFERRED, CONFLICT, QUARANTINED, DATA_LOSS
    }

    public record RepairResult(RepairStatus status, long copiedEntries,
            ZKProfileReservationStore.RepairCommitResult publication) { }

    /** One frozen intent. Copy is sequential and only retains one entry/RPC; caller cancellation stops future work,
     * never the child RPC already using buffers. Retry/resume reuses accepted, prepared and closeOperationId. */
    public CompletableFuture<RepairResult> repairHistorical(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId) {
        return repairFragment(accepted, prepared, sdk, canonicalDescriptor, credential,
                suppliedData, suppliedControl, closeOperationId, false);
    }

    /** The published prefix selection fixes the last fragment; this does not publish ledger CLOSED. */
    public CompletableFuture<RepairResult> repairLastFragment(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId) {
        return repairFragment(accepted, prepared, sdk, canonicalDescriptor, credential,
                suppliedData, suppliedControl, closeOperationId, true);
    }

    private CompletableFuture<RepairResult> repairFragment(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            byte[] canonicalDescriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId, boolean last) {
        if (accepted.context() == null || !accepted.context().equals(prepared.intent().recovery())
                || suppliedData.size() > 64 || suppliedControl.size() > 64) {
            throw new IllegalArgumentException("Historical repair requires its frozen context and bounded peers");
        }
        if (canonicalDescriptor.length > org.apache.bookkeeper.common.profile.ProfileDescriptorCodec
                .MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Repair descriptor exceeds its canonical bound");
        }
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
        }
        var stopped = new AtomicBoolean();
        try {
            var actual = repairFragmentOnce(accepted, prepared, sdk, canonicalDescriptor, credential,
                    suppliedData, suppliedControl, closeOperationId, last, stopped);
            var caller = actual.whenComplete((value, failure) -> active.release()).copy();
            caller.whenComplete((value, failure) -> {
                if (caller.isCancelled()) {
                    stopped.set(true);
                }
            });
            return caller;
        } catch (RuntimeException failure) {
            active.release();
            throw failure;
        }
    }

    private CompletableFuture<RepairResult> repairFragmentOnce(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk, byte[] canonicalDescriptor,
            ProtectedProfileCredential credential, Map<BookieId, ProfileNormalDataClient> suppliedData,
            Map<String, ProfileInitialControlClient> suppliedControl, byte[] closeOperationId, boolean last,
            AtomicBoolean stopped) {
        var input = accepted.metadata().metadata().getValue();
        var custom = new java.util.TreeMap<String, byte[]>();
        input.getCustomMetadata().forEach((name, bytes) -> custom.put(name, bytes.clone()));
        var builder = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(input).withCToken(input.getCToken())
                .withCustomMetadata(custom);
        if (input.hasPassword()) {
            builder.withPassword(input.getPassword().clone());
        }
        var frozen = new ZKProfileReservationStore.RecoveryResult(accepted.outcome(), accepted.context(),
                new ProfileLedgerMetadataStore.MembershipSnapshot(accepted.metadata().birthZxid(),
                        new org.apache.bookkeeper.versioning.Versioned<>(builder.build(),
                                accepted.metadata().metadata().getVersion())));
        var intent = prepared.intent();
        var data = Map.copyOf(suppliedData);
        var controls = Map.copyOf(suppliedControl);
        var name = intent.install().targetBookie();
        var target = data.get(BookieId.parse(name));
        var control = controls.get(name);
        if (target == null || control == null || target.hello().activeEngine() != 3
                || control.hello().activeEngine() != 3 || !target.hello().bookieId().equals(name)
                || !control.hello().bookieId().equals(name)
                || !Arrays.equals(target.hello().storageIncarnation(), intent.install().targetStorageIncarnation())
                || !Arrays.equals(control.hello().storageIncarnation(),
                    intent.install().targetStorageIncarnation())) {
            throw new IllegalArgumentException("Repair target must retain its verified control/DATA incarnation");
        }
        byte[] descriptor = canonicalDescriptor.clone();
        byte[] closeId = ProfileLifecycleHead.fixed(closeOperationId, 16, true);
        var key = credential.copy();
        CompletableFuture<RepairResult> actual;
            actual = CompletableFuture.supplyAsync(() -> readers(frozen.context()), cold).thenCompose(source -> {
                if (source.recovery().context() == null) {
                    return CompletableFuture.completedFuture(repairFailure(source.recovery().outcome()));
                }
                var current = source.recovery();
                var value = current.metadata().metadata().getValue();
                Long end = value.getAllEnsembles().higherKey(intent.fragmentStart());
                if (last != (end == null && intent.fragmentStart() == value.getAllEnsembles().lastKey())) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                }
                if (last) {
                    var boundary = reservations.readSelectedPrefix(metadata, current.context());
                    if (boundary.snapshot() == null
                            || intent.range().lastEntryId() != boundary.snapshot().boundary().prefixEntryId()) {
                        return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                    }
                    end = boundary.snapshot().boundary().prefixEntryId() + 1;
                }
                boolean published = ProfileRepairCommit.head(value.getCustomMetadata().get(ProfileRepairCommit.KEY))
                        .stream().anyMatch(ref -> ref.slot() == intent.slot()
                            && ref.intentBirth().equals(prepared.birth())
                            && ref.intentStoreVersion() == prepared.storeVersion());
                if (published) {
                    return CompletableFuture.supplyAsync(() -> reservations.commitRepair(metadata, prepared,
                            frozen.metadata(), closeId), cold).thenCompose(result -> closeRepair(result, intent,
                                    control, 0));
                }
                boolean sameContent;
                try {
                    sameContent = Arrays.equals(ProfileRecoveryContext.metadataDigest(value),
                            ProfileRecoveryContext.metadataDigest(frozen.metadata().metadata().getValue()))
                            && Arrays.equals(value.getCustomMetadata().get(ProfileRepairCommit.KEY),
                                frozen.metadata().metadata().getValue().getCustomMetadata()
                                    .get(ProfileRepairCommit.KEY))
                            && Arrays.equals(value.getCustomMetadata().get(ProfileRecoveryBoundary.KEY),
                                frozen.metadata().metadata().getValue().getCustomMetadata()
                                    .get(ProfileRecoveryBoundary.KEY));
                } catch (java.io.IOException invalid) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.QUARANTINED, 0, null));
                }
                if (!frozen.context().equals(current.context())
                        || !current.metadata().metadata().getVersion()
                            .equals(frozen.metadata().metadata().getVersion())
                        || current.metadata().birthZxid() != frozen.metadata().birthZxid()
                        || !sameContent || end == null || intent.range().firstEntryId() != intent.fragmentStart()
                        || intent.range().lastEntryId() != end - 1 || stopped.get()) {
                    return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
                }
                return fenceOnce(current, controls, key).thenCompose(fenced -> {
                    if (fenced.status() != Status.FENCED_CONTEXT || stopped.get()) {
                        return CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED, 0, null));
                    }
                    return control.install(ProfileControlRequest.install(
                            intent.install(), descriptor, EngineProfile
                                .SEGMENT_WAL_ENGINE, key)).thenCompose(installed -> {
                        if (!installed.durable() || stopped.get()) {
                            return CompletableFuture.completedFuture(
                                    new RepairResult(RepairStatus.DEFERRED, 0, null));
                        }
                        var receipt = ProfileInstallReceipt.from(intent.install(), control.hello(), installed);
                        return CompletableFuture.supplyAsync(
                                () -> reservations.admitRepair(metadata, prepared, receipt), cold)
                                    .thenCompose(admitted -> {
                            if (admitted.status() != ZKProfileReservationStore.CreateStatus.APPLIED
                                    && admitted.status() != ZKProfileReservationStore.CreateStatus.ALREADY_APPLIED
                                    || stopped.get()) {
                                return CompletableFuture.completedFuture(
                                        new RepairResult(RepairStatus.DEFERRED, 0, null));
                            }
                            return control.recoveryGrant(org.apache.bookkeeper.common.profile.control
                                    .ProfileControlRequest.recoveryGrant(intent.grantScope(),
                                        new ProfileControlRequest
                                            .RecoveryBinding(intent.intentId(), intent.intentGeneration(),
                                                intent.grantGeneration()))).thenCompose(granted -> {
                                if (!granted.durable() || stopped.get()) {
                                    return CompletableFuture.completedFuture(new RepairResult(
                                            RepairStatus.DEFERRED, 0, null));
                                }
                                return CompletableFuture.supplyAsync(() -> readers(frozen.context()), cold)
                                        .thenCompose(ready -> copyRepair(frozen, prepared, sdk, data, controls,
                                                target, control, key, closeId, stopped, ready));
                            });
                        });
                    });
                });
            });
        if (last) {
            actual = actual.thenApply(result -> result.status() == RepairStatus.REPAIRED_HISTORICAL_FRAGMENT
                    ? new RepairResult(RepairStatus.REPAIRED_LAST_FRAGMENT, result.copiedEntries(),
                            result.publication()) : result);
        }
        return actual;
    }

    private CompletableFuture<RepairResult> copyRepair(ZKProfileReservationStore.RecoveryResult accepted,
            ZKProfileReservationStore.RepairSnapshot prepared, ClientContext sdk,
            Map<BookieId, ProfileNormalDataClient> data,
            Map<String, ProfileInitialControlClient> controls, ProfileNormalDataClient target,
            ProfileInitialControlClient control, ProtectedProfileCredential credential, byte[] closeId,
            AtomicBoolean stopped, Readers ready) {
        if (ready.recovery().context() == null || !accepted.context().equals(ready.recovery().context())
                || !ready.recovery().metadata().metadata().getVersion()
                    .equals(accepted.metadata().metadata().getVersion())) {
            return CompletableFuture.completedFuture(new RepairResult(RepairStatus.CONFLICT, 0, null));
        }
        var boundControls = new java.util.HashMap<BookieId, ProfileInitialControlClient>();
        controls.forEach((name, peer) -> boundControls.put(BookieId.parse(name), peer));
        ProfileRecoveryReadHandle reader;
        try {
            reader = ProfileRecoveryReadHandle.fromContext(sdk, ready.recovery(),
                    ready.permitted(), data, boundControls);
        } catch (java.security.GeneralSecurityException invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        var copy = new Copy(reader, target, prepared.intent(), credential, stopped);
        copy.step();
        return copy.done.thenCompose(result -> {
            if (result.status() != RepairStatus.REPAIRED_HISTORICAL_FRAGMENT || stopped.get()) {
                return CompletableFuture.completedFuture(stopped.get()
                        ? new RepairResult(RepairStatus.DEFERRED, result.copiedEntries(), null) : result);
            }
            return CompletableFuture.supplyAsync(() -> stopped.get() ? null : reservations.commitRepair(metadata,
                    prepared, accepted.metadata(), closeId), cold).thenCompose(published -> published == null
                        ? CompletableFuture.completedFuture(new RepairResult(RepairStatus.DEFERRED,
                                result.copiedEntries(), null))
                        : closeRepair(published, prepared.intent(), control, result.copiedEntries()));
        }).whenComplete((result, failure) -> reader.closeAsync());
    }

    private CompletableFuture<RepairResult> closeRepair(ZKProfileReservationStore.RepairCommitResult published,
            ProfileRepairIntent intent, ProfileInitialControlClient control, long copied) {
        if (published.status() != ZKProfileReservationStore.CreateStatus.APPLIED
                && published.status() != ZKProfileReservationStore.CreateStatus.ALREADY_APPLIED) {
            return CompletableFuture.completedFuture(new RepairResult(published.status()
                    == ZKProfileReservationStore.CreateStatus.CONFLICT ? RepairStatus.CONFLICT
                    : published.status() == ZKProfileReservationStore.CreateStatus.QUARANTINED
                        ? RepairStatus.QUARANTINED : RepairStatus.DEFERRED, copied, published));
        }
        return control.recoveryClose(ProfileControlRequest.recoveryClose(
                published.closeScope(), new ProfileControlRequest
                    .RecoveryBinding(intent.intentId(), intent.intentGeneration(), intent.grantGeneration()),
                intent.grantOperationId())).handle((closed, failure) -> new RepairResult(failure == null
                        && closed.durable() ? RepairStatus.REPAIRED_HISTORICAL_FRAGMENT : RepairStatus.DEFERRED,
                        copied, published));
    }

    private static RepairResult repairFailure(ProfileLedgerMetadataStore.Outcome outcome) {
        return new RepairResult(outcome == ProfileLedgerMetadataStore.Outcome.CONFLICT ? RepairStatus.CONFLICT
                : outcome == ProfileLedgerMetadataStore.Outcome.QUARANTINED ? RepairStatus.QUARANTINED
                    : RepairStatus.DEFERRED, 0, null);
    }

    private static final class Copy {
        private final ProfileRecoveryReadHandle reader;
        private final ProfileNormalDataClient target;
        private final ProfileRepairIntent intent;
        private final ProtectedProfileCredential credential;
        private final AtomicBoolean stopped;
        private final CompletableFuture<RepairResult> done = new CompletableFuture<>();
        private long next;
        private long copied;

        Copy(ProfileRecoveryReadHandle reader, ProfileNormalDataClient target,
                ProfileRepairIntent intent, ProtectedProfileCredential credential,
                AtomicBoolean stopped) {
            this.reader = reader;
            this.target = target;
            this.intent = intent;
            this.credential = credential;
            this.stopped = stopped;
            next = intent.range().firstEntryId();
        }

        void step() {
            if (stopped.get()) {
                done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                return;
            }
            reader.probeEvidence(next).whenComplete((evidence, readFailure) -> {
                if (readFailure != null && !(cause(readFailure) instanceof BKException)) {
                    done.completeExceptionally(readFailure);
                    return;
                }
                if (readFailure != null || stopped.get()) {
                    done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                    return;
                }
                if (evidence.conflict()) {
                    done.complete(new RepairResult(RepairStatus.QUARANTINED, copied, null));
                    return;
                }
                if (evidence.sources().stream().anyMatch(source -> source.outcome()
                        == ProfileRecoveryReadHandle.SourceOutcome.UNAUTHORIZED)) {
                    done.complete(new RepairResult(RepairStatus.CONFLICT, copied, null));
                    return;
                }
                var probe = evidence.candidate();
                if (probe == null) {
                    done.complete(new RepairResult(evidence.exhausted() ? RepairStatus.DATA_LOSS
                            : RepairStatus.DEFERRED, copied, null));
                    return;
                }
                ReferenceCounted packed;
                try {
                    packed = reader.packageRecoveryEntry(probe);
                } catch (RuntimeException invalid) {
                    done.completeExceptionally(invalid);
                    return;
                }
                try {
                    target.addRecovery(intent, credential, next, packed).whenComplete((reply, failure) -> {
                        packed.release();
                        if (failure != null || reply.statusClass() != ProfileStatus.StatusClass.OK
                                || reply.durableResult() != ProfileStatus.DurableResult.APPLIED
                                    && reply.durableResult() != ProfileStatus.DurableResult.ALREADY_APPLIED) {
                            done.complete(new RepairResult(RepairStatus.DEFERRED, copied, null));
                            return;
                        }
                        copied++;
                        if (next == intent.range().lastEntryId()) {
                            done.complete(new RepairResult(RepairStatus.REPAIRED_HISTORICAL_FRAGMENT, copied, null));
                        } else {
                            next++;
                            step(); // probe schedules on the original SDK ordered executor; no growing future chain.
                        }
                    });
                } catch (RuntimeException failure) {
                    packed.release();
                    done.completeExceptionally(failure);
                }
            });
        }
    }

    private static Status failureStatus(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.DEFERRED;
        };
    }
}

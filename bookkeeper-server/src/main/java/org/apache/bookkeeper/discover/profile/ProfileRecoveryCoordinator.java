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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;

/** Frozen recovery-context fencing only. No recovery Add, frontier, close, retry or resource ownership. */
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
            List<org.apache.bookkeeper.client.ProfileRecoveryReadHandle.Probe> candidates, long missingEntry) {
        public Observation {
            candidates = List.copyOf(candidates);
        }
    }

    private record Readers(ZKProfileReservationStore.RecoveryResult recovery, List<ProfileInstallReceipt> permitted) { }

    public CompletableFuture<Observation> observe(ZKProfileReservationStore.RecoveryResult accepted,
            org.apache.bookkeeper.client.ClientContext sdk,
            Map<org.apache.bookkeeper.net.BookieId, ProfileNormalDataClient> suppliedData,
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
            org.apache.bookkeeper.client.ClientContext sdk,
            Map<org.apache.bookkeeper.net.BookieId, ProfileNormalDataClient> suppliedData,
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
            return CompletableFuture.supplyAsync(() -> {
                var current = reservations.readRecovery(metadata, context.ledger().ledgerId());
                if (current.context() == null || !context.equals(current.context())) {
                    return new Readers(current, List.of());
                }
                var retained = reservations.retainedActivationTargets(context.ledger().ledgerId());
                if (retained.status()
                        != org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND) {
                    throw new IllegalStateException("Required retained recovery source history is unavailable");
                }
                var currentTargets = prepare(context).targets();
                var permitted = new java.util.LinkedHashMap<String, ProfileInstallReceipt>();
                retained.targets().forEach(receipt -> permitted.put(receipt.scope().targetBookie(), receipt));
                currentTargets.forEach(receipt -> permitted.put(receipt.scope().targetBookie(), receipt));
                return new Readers(current, List.copyOf(permitted.values()));
            }, cold).thenCompose(readers -> {
                if (readers.recovery().context() == null || !context.equals(readers.recovery().context())) {
                    return CompletableFuture.completedFuture(new Observation(observationStatus(
                            failureStatus(readers.recovery().outcome())), context, null, List.of(), -1));
                }
                var controls = new java.util.HashMap<org.apache.bookkeeper.net.BookieId, ProfileInitialControlClient>();
                control.forEach((name, peer) -> controls.put(org.apache.bookkeeper.net.BookieId.parse(name), peer));
                org.apache.bookkeeper.client.ProfileRecoveryReadHandle reader;
                try {
                    reader = org.apache.bookkeeper.client.ProfileRecoveryReadHandle.fromContext(sdk,
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

    private CompletableFuture<Observation> scan(org.apache.bookkeeper.client.ProfileRecoveryReadHandle reader,
            ProfileRecoveryContext context, org.apache.bookkeeper.client.ProfileLedgerHandle.LacEvidence lac,
            List<org.apache.bookkeeper.client.ProfileRecoveryReadHandle.Probe> candidates,
            long prefix, long length, int remaining) {
        if (remaining == 0 || prefix == Long.MAX_VALUE) {
            return CompletableFuture.completedFuture(new Observation(ObservationStatus.DEFERRED,
                    context, lac, candidates, -1));
        }
        return reader.probe(prefix + 1).thenCompose(probe -> {
            if (probe.outcome() == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.ProbeOutcome.ABSENT) {
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

    private static Status failureStatus(ProfileLedgerMetadataStore.Outcome outcome) {
        return switch (outcome) {
            case CONFLICT -> Status.CONFLICT;
            case QUARANTINED -> Status.QUARANTINED;
            default -> Status.DEFERRED;
        };
    }
}

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
        if (accepted.context() == null || supplied.size() > 64) {
            throw new IllegalArgumentException("Fencing requires a proved recovery context and bounded peer pool");
        }
        var context = accepted.context();
        var peers = Map.copyOf(supplied);
        Objects.requireNonNull(credential, "credential");
        if (!active.tryAcquire()) {
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, context, List.of()));
        }
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
            active.release();
            return CompletableFuture.completedFuture(new Result(Status.DEFERRED, context, List.of()));
        }
        return actual.whenComplete((result, failure) -> active.release()).copy();
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.bookie.storage.segment;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;

/**
 * Borrowed physical INSTALL adapter for the existing typed endpoint, on bounded cold callers only.
 * The original driver retains actual I/O/owner ownership after timeout or interruption. No executor or retry is added.
 * Principal/AuthZ and descriptor validation remain in the endpoint and original control log.
 */
final class SegmentProfileInstallStore implements ProtectedProfileStateStore {
    private final SegmentBookieControlDriver driver;
    private final SegmentFileHeader identity;
    private final long timeoutNanos;

    SegmentProfileInstallStore(SegmentBookieControlDriver driver, SegmentFileHeader identity, Duration timeout) {
        this.driver = Objects.requireNonNull(driver, "driver");
        this.identity = Objects.requireNonNull(identity, "identity");
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid cold INSTALL timeout");
        }
        timeoutNanos = timeout.toNanos();
    }

    @Override
    public TransitionResult conditionalApply(Transition transition) {
        return conditionalApply(transition, () -> { });
    }

    /** Actual terminal, independent of this cold caller's timeout/interrupt; observer must not throw. */
    TransitionResult conditionalApply(Transition transition, Runnable actualTerminal) {
        Objects.requireNonNull(actualTerminal, "actualTerminal");
        boolean accepted = false;
        try {
            Objects.requireNonNull(transition, "transition");
            if (driver.isOwnerThread()) {
                throw new IllegalStateException("A cold INSTALL wait cannot run on its append owner");
            }
            ProfileControlScope scope = transition.request().scope();
            if (scope.operation() != ProfileControlOperation.INSTALL
                    && scope.operation() != ProfileControlOperation.ACTIVATE_INITIAL
                    && scope.operation() != ProfileControlOperation.ACTIVATE_REPLACEMENT
                    && scope.operation() != ProfileControlOperation.RECOVERY_GRANT
                    && scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE) {
                return result(Outcome.UNSUPPORTED);
            }
            if (!transition.authority().scope().equals(scope)
                    || !transition.authority().permits(scope.operation())
                    || transition.authority().requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                    || !scope.targetBookie().equals(identity.bookieId().toString())
                    || !Arrays.equals(scope.targetStorageIncarnation(), identity.incarnation().value())) {
                return result(Outcome.CONFLICT);
            }
            long deadline = System.nanoTime() + timeoutNanos;
            CompletableFuture<TransitionResult> reply = new CompletableFuture<>();
            SegmentBookieControlDriver.BatchCompletion completion = (results, failure) -> {
                try {
                    if (failure != null || results == null || results.size() != 1) {
                        reply.complete(result(Outcome.DURABILITY_UNKNOWN));
                        return;
                    }
                    SegmentBookieControlLog.Result applied = results.get(0);
                    ByteBuffer operation = ByteBuffer.wrap(scope.operationId());
                    if (!applied.operationId().equals(new UUID(operation.getLong(), operation.getLong()))) {
                        reply.complete(result(Outcome.DURABILITY_UNKNOWN));
                        return;
                    }
                    Outcome outcome = switch (applied.status()) {
                        case APPLIED_DURABLE -> Outcome.APPLIED;
                        case ALREADY_DURABLE -> Outcome.ALREADY_APPLIED;
                        case CONDITION_FAILED, INCOMPATIBLE -> Outcome.CONFLICT;
                    };
                    if ((scope.operation() == ProfileControlOperation.ACTIVATE_INITIAL
                            || scope.operation() == ProfileControlOperation.ACTIVATE_REPLACEMENT)
                            && (outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED)
                            && driver.initialActiveGeneration(scope) == 0) {
                        outcome = Outcome.STALE_OR_COMPACTED;
                    }
                    reply.complete(new TransitionResult(outcome, scope.operation() == ProfileControlOperation.INSTALL
                            ? applied.localInstallGeneration() : applied.localGeneration(),
                            outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED));
                } finally {
                    actualTerminal.run();
                }
            };
            if (scope.operation() == ProfileControlOperation.RECOVERY_GRANT_CLOSE) {
                var binding = transition.request().recoveryBinding().orElseThrow();
                ByteBuffer grantId = ByteBuffer.wrap(transition.request().closedGrantOperationId().orElseThrow());
                accepted = driver.submitRecoveryClose(new SegmentBookieControlLog.RecoveryGrantClose(transition,
                        new UUID(grantId.getLong(), grantId.getLong()), binding.intentId(),
                        binding.intentGeneration(), binding.grantGeneration()), (closed, failure) -> {
                            try {
                                if (failure != null || closed == null) {
                                    reply.complete(result(Outcome.DURABILITY_UNKNOWN));
                                    return;
                                }
                                Outcome outcome = switch (closed) {
                                    case APPLIED_DURABLE -> Outcome.APPLIED;
                                    case ALREADY_DURABLE -> Outcome.ALREADY_APPLIED;
                                    case WAITING_PRECUT -> Outcome.TRANSIENT_RECONCILING;
                                    case CONDITION_FAILED, INCOMPATIBLE -> Outcome.CONFLICT;
                                };
                                long generation = outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED
                                        ? driver.recoveryClosedGeneration(scope) : 0;
                                if ((outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED)
                                        && generation == 0) {
                                    outcome = Outcome.STALE_OR_COMPACTED;
                                }
                                reply.complete(new TransitionResult(outcome, generation,
                                        outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED));
                            } finally {
                                actualTerminal.run();
                            }
                        });
            } else if (scope.operation() == ProfileControlOperation.RECOVERY_GRANT) {
                var binding = transition.request().recoveryBinding().orElseThrow();
                accepted = driver.submitRecoveryGrant(new SegmentBookieControlLog.RecoveryGrant(transition,
                        binding.intentId(), binding.intentGeneration(), binding.grantGeneration()), completion);
            } else {
                accepted = driver.submitBatch(List.of(transition), completion);
            }
            if (!accepted) {
                return result(Outcome.TRANSIENT_RECONCILING);
            }
            try {
                return reply.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return result(Outcome.DURABILITY_UNKNOWN);
            } catch (TimeoutException | ExecutionException unknown) {
                return result(Outcome.DURABILITY_UNKNOWN);
            }
        } finally {
            if (!accepted) {
                actualTerminal.run();
            }
        }
    }

    @Override
    public TransitionResult queryOperationResult(ProfileControlScope scope) {
        Objects.requireNonNull(scope, "scope");
        // The current PREPARING root authorizes retry of the exact INSTALL, not a fabricated STATUS_QUERY authority.
        return result(Outcome.UNSUPPORTED);
    }

    private static TransitionResult result(Outcome outcome) {
        return new TransitionResult(outcome, 0, false);
    }
}

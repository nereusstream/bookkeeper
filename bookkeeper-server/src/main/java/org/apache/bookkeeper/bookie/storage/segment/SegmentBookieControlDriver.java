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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.discover.profile.ProfileDeleteHandoff;

/** Bookie control commands use reserved control I/O and return ordered results to the owner. */
final class SegmentBookieControlDriver {
    @FunctionalInterface
    interface BatchCompletion {
        void complete(List<SegmentBookieControlLog.Result> results, Throwable failure);
    }

    @FunctionalInterface
    private interface DurableAppend {
        SegmentBookieControlLog.DurableBatch append() throws IOException;
    }

    @FunctionalInterface
    interface FenceCompletion {
        void complete(SegmentBookieControlLog.FenceResult result, Throwable failure);
    }

    @FunctionalInterface
    interface RecoveryCloseCompletion {
        void complete(SegmentBookieControlLog.RecoveryCloseStatus status, Throwable failure);
    }

    @FunctionalInterface
    interface LacCompletion {
        void complete(SegmentBookieControlLog.Status status, Throwable failure);
    }

    private final SegmentOwnerLoop owner;
    private final SegmentIoExecutor io;
    private final SegmentBookieControlLog log;
    private final AtomicBoolean busy = new AtomicBoolean();

    SegmentBookieControlDriver(SegmentOwnerLoop owner, SegmentIoExecutor io, SegmentBookieControlLog log) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.io = Objects.requireNonNull(io, "io");
        this.log = Objects.requireNonNull(log, "log");
    }

    boolean isOwnerThread() {
        return owner.isOwnerThread();
    }

    long initialActiveGeneration(ProfileControlScope scope) {
        owner.requireOwner();
        return log.initialActiveGeneration(scope);
    }

    long recoveryClosedGeneration(ProfileControlScope scope) {
        owner.requireOwner();
        return log.recoveryClosedGeneration(scope);
    }

    boolean reserveMaintenance() {
        return busy.compareAndSet(false, true);
    }

    void releaseMaintenance() {
        if (!busy.compareAndSet(true, false)) {
            throw new IllegalStateException("Bookie maintenance reservation was not held");
        }
    }

    boolean submitBatch(List<ProtectedProfileStateStore.Transition> commands, BatchCompletion completion) {
        List<ProtectedProfileStateStore.Transition> batch = List.copyOf(commands);
        return submitDurableBatch(() -> log.appendDurably(batch), completion);
    }

    boolean submitRecoveryGrant(SegmentBookieControlLog.RecoveryGrant grant, BatchCompletion completion) {
        Objects.requireNonNull(grant, "grant");
        return submitDurableBatch(() -> log.appendRecoveryGrantDurably(grant), completion);
    }

    private boolean submitDurableBatch(DurableAppend append, BatchCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        if (log.rotationRequired() || !busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    AtomicReference<SegmentBookieControlLog.DurableBatch> durable = new AtomicReference<>();
                    try {
                        io.submitControl(ticket, () -> durable.set(append.append()), cause -> {
                            owner.requireOwner();
                            if (cause != null) {
                                deliver(completion, null, cause);
                                return;
                            }
                            List<SegmentBookieControlLog.Result> results;
                            try {
                                results = log.applyDurable(durable.get());
                            } catch (Throwable failure) {
                                deliver(completion, null, failure);
                                return;
                            }
                            deliver(completion, results, null);
                        });
                    } catch (Throwable failure) {
                        ticket.complete(() -> deliver(completion, null, failure));
                    }
                }

                @Override
                public void failed(Throwable cause) {
                    owner.requireOwner();
                    deliver(completion, null, cause);
                }
            });
        } catch (RuntimeException | Error failure) {
            busy.set(false);
            throw failure;
        }
        if (!accepted) {
            busy.set(false);
        }
        return accepted;
    }

    /** Cold native handoff adoption uses the same reserved writer and actual owner terminal hook. */
    boolean submitHandoff(ProfileDeleteHandoff.Plan plan, LacCompletion completion, Runnable terminal) {
        Objects.requireNonNull(plan, "plan");
        return submitHandoffAppend(() -> log.appendHandoffDurably(plan), completion, terminal);
    }

    boolean submitEffectiveHandoff(ProfileDeleteHandoff.Effective effective, LacCompletion completion,
            Runnable terminal) {
        Objects.requireNonNull(effective, "effective");
        return submitHandoffAppend(() -> log.appendEffectiveHandoffDurably(effective), completion, terminal);
    }

    @FunctionalInterface
    private interface HandoffAppend {
        SegmentBookieControlLog.HandoffWrite append() throws IOException;
    }

    private boolean submitHandoffAppend(HandoffAppend append, LacCompletion completion, Runnable terminal) {
        Objects.requireNonNull(completion, "completion");
        Objects.requireNonNull(terminal, "terminal");
        if (log.rotationRequired() || !busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    var durable = new AtomicReference<SegmentBookieControlLog.HandoffWrite>();
                    try {
                        io.submitControl(ticket, () -> durable.set(append.append()), failure -> {
                            owner.requireOwner();
                            if (failure != null) {
                                deliver(completion, null, failure);
                                return;
                            }
                            SegmentBookieControlLog.Status result;
                            try {
                                result = log.applyHandoffDurable(durable.get());
                            } catch (Throwable applyFailure) {
                                deliver(completion, null, applyFailure);
                                return;
                            }
                            deliver(completion, result, null);
                        });
                    } catch (Throwable failure) {
                        ticket.complete(() -> deliver(completion, null, failure));
                    }
                }

                @Override
                public void failed(Throwable failure) {
                    owner.requireOwner();
                    deliver(completion, null, failure);
                }

                @Override
                public void terminal() {
                    terminal.run();
                }
            });
        } catch (RuntimeException | Error failure) {
            busy.set(false);
            throw failure;
        }
        if (!accepted) {
            busy.set(false);
        }
        return accepted;
    }

    boolean submitFence(ProfileOperationCodec.FenceLedger request, FenceCompletion completion) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(completion, "completion");
        if (log.rotationRequired() || !busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                private SegmentBookieControlLog.FenceWrite prepared;
                private final AtomicBoolean ioStarted = new AtomicBoolean();

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    SegmentBookieControlLog.FenceAttempt attempt;
                    try {
                        attempt = log.prepareFence(request);
                    } catch (Throwable failure) {
                        ticket.complete(() -> deliver(completion, null, failure));
                        return;
                    }
                    if (attempt.immediate() != null) {
                        ticket.complete(() -> deliver(completion, attempt.immediate(), null));
                        return;
                    }
                    prepared = attempt.write();
                    try {
                        io.submitControl(ticket, () -> {
                            ioStarted.set(true);
                            log.writeFenceDurably(prepared);
                        }, failure -> {
                            owner.requireOwner();
                            if (failure != null) {
                                abortUnstarted(failure);
                                deliver(completion, null, failure);
                                return;
                            }
                            SegmentBookieControlLog.FenceResult result;
                            try {
                                result = log.applyFenceDurable(prepared);
                            } catch (Throwable applyFailure) {
                                deliver(completion, null, applyFailure);
                                return;
                            }
                            deliver(completion, result, null);
                        });
                    } catch (Throwable failure) {
                        abortUnstarted(failure);
                        ticket.complete(() -> deliver(completion, null, failure));
                    }
                }

                private void abortUnstarted(Throwable failure) {
                    if (!ioStarted.get()) {
                        try {
                            log.abortFenceBeforeIo(prepared);
                        } catch (Throwable cleanupFailure) {
                            failure.addSuppressed(cleanupFailure);
                        }
                    }
                }

                @Override
                public void failed(Throwable cause) {
                    owner.requireOwner();
                    deliver(completion, null, cause);
                }
            });
        } catch (RuntimeException | Error failure) {
            busy.set(false);
            throw failure;
        }
        if (!accepted) {
            busy.set(false);
        }
        return accepted;
    }

    boolean submitRecoveryClose(SegmentBookieControlLog.RecoveryGrantClose request,
                                RecoveryCloseCompletion completion) {
        Objects.requireNonNull(request, "request");
        return submitClose(() -> log.prepareRecoveryClose(request), completion);
    }

    boolean submitTerminalClose(ProtectedProfileStateStore.Transition request, RecoveryCloseCompletion completion) {
        Objects.requireNonNull(request, "request");
        return submitClose(() -> log.prepareTerminalClose(request), completion);
    }

    private boolean submitClose(java.util.concurrent.Callable<SegmentBookieControlLog.RecoveryCloseAttempt> prepare,
            RecoveryCloseCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        if (log.rotationRequired() || !busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                private SegmentBookieControlLog.RecoveryCloseWrite prepared;
                private final AtomicBoolean ioStarted = new AtomicBoolean();

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    SegmentBookieControlLog.RecoveryCloseAttempt attempt;
                    try {
                        attempt = prepare.call();
                    } catch (Throwable failure) {
                        ticket.complete(() -> deliver(completion, null, failure));
                        return;
                    }
                    if (attempt.immediate() != null) {
                        ticket.complete(() -> deliver(completion, attempt.immediate(), null));
                        return;
                    }
                    prepared = attempt.write();
                    try {
                        io.submitControl(ticket, () -> {
                            ioStarted.set(true);
                            log.writeRecoveryCloseDurably(prepared);
                        }, failure -> {
                            owner.requireOwner();
                            if (failure != null) {
                                abortUnstarted(failure);
                                deliver(completion, null, failure);
                                return;
                            }
                            SegmentBookieControlLog.RecoveryCloseStatus status;
                            try {
                                status = log.applyRecoveryCloseDurable(prepared);
                            } catch (Throwable applyFailure) {
                                deliver(completion, null, applyFailure);
                                return;
                            }
                            deliver(completion, status, null);
                        });
                    } catch (Throwable failure) {
                        abortUnstarted(failure);
                        ticket.complete(() -> deliver(completion, null, failure));
                    }
                }

                private void abortUnstarted(Throwable failure) {
                    if (!ioStarted.get()) {
                        try {
                            log.abortRecoveryCloseBeforeIo(prepared);
                        } catch (Throwable cleanupFailure) {
                            failure.addSuppressed(cleanupFailure);
                        }
                    }
                }

                @Override
                public void failed(Throwable cause) {
                    owner.requireOwner();
                    deliver(completion, null, cause);
                }
            });
        } catch (RuntimeException | Error failure) {
            busy.set(false);
            throw failure;
        }
        if (!accepted) {
            busy.set(false);
        }
        return accepted;
    }

    boolean submitExplicitLac(ProfileOperationCodec.WriteLac request, LacCompletion completion) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(completion, "completion");
        if (log.rotationRequired() || !busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                private SegmentBookieControlLog.LacWrite prepared;
                private final AtomicBoolean ioStarted = new AtomicBoolean();

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    SegmentBookieControlLog.LacAttempt attempt;
                    try {
                        attempt = log.prepareExplicitLac(request);
                    } catch (Throwable failure) {
                        ticket.complete(() -> deliver(completion, null, failure));
                        return;
                    }
                    if (attempt.immediate() != null) {
                        ticket.complete(() -> deliver(completion, attempt.immediate(), null));
                        return;
                    }
                    prepared = attempt.write();
                    try {
                        io.submitControl(ticket, () -> {
                            ioStarted.set(true);
                            log.writeExplicitLacDurably(prepared);
                        }, failure -> {
                            owner.requireOwner();
                            if (failure != null) {
                                abortUnstarted(failure);
                                deliver(completion, null, failure);
                                return;
                            }
                            SegmentBookieControlLog.Status status;
                            try {
                                status = log.applyExplicitLacDurable(prepared);
                            } catch (Throwable applyFailure) {
                                deliver(completion, null, applyFailure);
                                return;
                            }
                            deliver(completion, status, null);
                        });
                    } catch (Throwable failure) {
                        abortUnstarted(failure);
                        ticket.complete(() -> deliver(completion, null, failure));
                    }
                }

                private void abortUnstarted(Throwable failure) {
                    if (!ioStarted.get()) {
                        try {
                            log.abortExplicitLacBeforeIo(prepared);
                        } catch (Throwable cleanupFailure) {
                            failure.addSuppressed(cleanupFailure);
                        }
                    }
                }

                @Override
                public void failed(Throwable cause) {
                    owner.requireOwner();
                    deliver(completion, null, cause);
                }
            });
        } catch (RuntimeException | Error failure) {
            busy.set(false);
            throw failure;
        }
        if (!accepted) {
            busy.set(false);
        }
        return accepted;
    }

    private void deliver(BatchCompletion completion, List<SegmentBookieControlLog.Result> results, Throwable failure) {
        try {
            completion.complete(results, failure);
        } finally {
            busy.set(false);
        }
    }

    private void deliver(FenceCompletion completion, SegmentBookieControlLog.FenceResult result, Throwable failure) {
        try {
            completion.complete(result, failure);
        } finally {
            busy.set(false);
        }
    }

    private void deliver(RecoveryCloseCompletion completion, SegmentBookieControlLog.RecoveryCloseStatus status,
                         Throwable failure) {
        try {
            completion.complete(status, failure);
        } finally {
            busy.set(false);
        }
    }

    private void deliver(LacCompletion completion, SegmentBookieControlLog.Status status, Throwable failure) {
        try {
            completion.complete(status, failure);
        } finally {
            busy.set(false);
        }
    }
}

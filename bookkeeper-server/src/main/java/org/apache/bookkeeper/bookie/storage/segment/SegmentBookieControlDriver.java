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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;

/** Bookie control commands use reserved control I/O and return ordered results to the owner. */
final class SegmentBookieControlDriver {
    @FunctionalInterface
    interface BatchCompletion {
        void complete(List<SegmentBookieControlLog.Result> results, Throwable failure);
    }

    @FunctionalInterface
    interface FenceCompletion {
        void complete(SegmentBookieControlLog.FenceResult result, Throwable failure);
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

    boolean submitBatch(List<ProtectedProfileStateStore.Transition> commands, BatchCompletion completion) {
        List<ProtectedProfileStateStore.Transition> batch = List.copyOf(commands);
        Objects.requireNonNull(completion, "completion");
        if (!busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    AtomicReference<SegmentBookieControlLog.DurableBatch> durable = new AtomicReference<>();
                    try {
                        io.submitControl(ticket, () -> durable.set(log.appendDurably(batch)), cause -> {
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

    boolean submitFence(ProfileOperationCodec.FenceLedger request, FenceCompletion completion) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(completion, "completion");
        if (!busy.compareAndSet(false, true)) {
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
}

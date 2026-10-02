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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** One in-flight Arena control batch, forced off-owner and applied on its owner. */
final class SegmentArenaControlDriver {
    @FunctionalInterface
    interface Completion {
        void complete(List<SegmentArenaControlLog.Result> results, Throwable failure);
    }

    private final SegmentOwnerLoop owner;
    private final SegmentIoExecutor io;
    private final SegmentArenaControlLog log;
    private final AtomicBoolean busy = new AtomicBoolean();
    private record PoolSubmission(SegmentArenaControlLog.PoolRequest request, Completion completion) { }
    private final Object poolGate = new Object();
    private final List<PoolSubmission> pools = new ArrayList<>();
    private boolean collecting;

    SegmentArenaControlDriver(SegmentOwnerLoop owner, SegmentIoExecutor io, SegmentArenaControlLog log) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.io = Objects.requireNonNull(io, "io");
        this.log = Objects.requireNonNull(log, "log");
    }

    boolean submit(List<SegmentArenaControlLog.Allocation> commands, Completion completion) {
        List<SegmentArenaControlLog.Allocation> batch = List.copyOf(commands);
        Objects.requireNonNull(completion, "completion");
        if (!busy.compareAndSet(false, true)) {
            return false;
        }
        boolean accepted;
        try {
            accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    forceAndApply(ticket, batch, completion);
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

    boolean owns(SegmentArenaControlLog authority) {
        return log == authority;
    }

    /** Merge at most one request per shard until the queued control batch starts; no retry queue. */
    boolean requestPool(SegmentArenaControlLog.PoolRequest request, Completion completion) {
        log.validatePoolRequest(request);
        Objects.requireNonNull(completion, "completion");
        synchronized (poolGate) {
            if (busy.get()) {
                if (!collecting || pools.size() == log.maxPoolRequests()
                        || pools.stream().anyMatch(p -> p.request().shard() == request.shard())) {
                    return false;
                }
                pools.add(new PoolSubmission(request, completion));
                return true;
            }
            if (!busy.compareAndSet(false, true)) {
                return false;
            }
            pools.add(new PoolSubmission(request, completion));
            collecting = true;
            try {
                if (owner.submitControl(new PoolCommand())) {
                    return true;
                }
            } catch (RuntimeException | Error failure) {
                pools.clear();
                collecting = false;
                busy.set(false);
                throw failure;
            }
            pools.clear();
            collecting = false;
            busy.set(false);
            return false;
        }
    }

    private final class PoolCommand implements SegmentOwnerLoop.Command {
        private List<PoolSubmission> batch;
        private List<Optional<SegmentArenaControlLog.Allocation>> planned;

        private void freezeRequests() {
            synchronized (poolGate) {
                if (batch == null) {
                    batch = List.copyOf(pools);
                    pools.clear();
                    collecting = false;
                }
            }
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket ticket) {
            freezeRequests();
            try {
                planned = log.planPools(batch.stream().map(PoolSubmission::request).toList());
            } catch (IOException failure) {
                ticket.complete(() -> deliver(this::completed, null, failure));
                return;
            }
            List<SegmentArenaControlLog.Allocation> commands = planned.stream().flatMap(Optional::stream).toList();
            if (commands.isEmpty()) {
                ticket.complete(() -> deliver(this::completed, List.of(), null));
            } else {
                forceAndApply(ticket, commands, this::completed);
            }
        }

        @Override
        public void failed(Throwable cause) {
            freezeRequests();
            deliver(this::completed, null, cause);
        }

        private void completed(List<SegmentArenaControlLog.Result> results, Throwable failure) {
            Throwable callbackFailure = null;
            int result = 0;
            for (int i = 0; i < batch.size(); i++) {
                List<SegmentArenaControlLog.Result> one = failure == null && planned.get(i).isPresent()
                        ? List.of(results.get(result++)) : List.of();
                try {
                    batch.get(i).completion().complete(one, failure);
                } catch (Throwable cause) {
                    if (callbackFailure == null) {
                        callbackFailure = cause;
                    } else if (callbackFailure != cause) {
                        callbackFailure.addSuppressed(cause);
                    }
                }
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Pool refill callback failed", callbackFailure);
            }
        }
    }

    private void forceAndApply(SegmentOwnerLoop.Ticket ticket,
                               List<SegmentArenaControlLog.Allocation> batch, Completion completion) {
        AtomicReference<SegmentArenaControlLog.DurableBatch> durable = new AtomicReference<>();
        io.submitControl(ticket, () -> durable.set(log.appendDurably(batch)), cause -> {
            owner.requireOwner();
            if (cause != null) {
                deliver(completion, null, cause);
                return;
            }
            List<SegmentArenaControlLog.Result> results;
            try {
                results = log.applyDurable(durable.get());
            } catch (Throwable failure) {
                deliver(completion, null, failure);
                return;
            }
            deliver(completion, results, null);
        });
    }

    private void deliver(Completion completion, List<SegmentArenaControlLog.Result> results, Throwable failure) {
        try {
            completion.complete(results, failure);
        } finally {
            busy.set(false);
        }
    }
}

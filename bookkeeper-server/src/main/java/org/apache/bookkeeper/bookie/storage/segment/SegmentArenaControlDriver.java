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

    private void deliver(Completion completion, List<SegmentArenaControlLog.Result> results, Throwable failure) {
        try {
            completion.complete(results, failure);
        } finally {
            busy.set(false);
        }
    }
}

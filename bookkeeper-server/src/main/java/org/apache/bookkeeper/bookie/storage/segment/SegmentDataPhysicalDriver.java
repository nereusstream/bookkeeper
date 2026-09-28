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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Real DATA I/O to contiguous physical candidates only; no entry result or Bookie ACK authority. */
final class SegmentDataPhysicalDriver {
    @FunctionalInterface
    interface Completion {
        void complete(List<SegmentPointRead.Location> candidates, Throwable failure);
    }

    private final int shard;
    private final SegmentOwnerLoop owner;
    private final SegmentIoExecutor io;
    private final IsolatedSegmentFiles files;
    private final SegmentArenaControlLog authority;
    private final SegmentPhysicalPrefix prefix;
    private final SegmentHotDirectory hot;
    private final Map<IsolatedSegmentFiles.Reservation, Completion> pending = new IdentityHashMap<>();

    SegmentDataPhysicalDriver(int shard, SegmentOwnerLoop owner, SegmentIoExecutor io,
                              IsolatedSegmentFiles files, SegmentArenaControlLog authority,
                              SegmentPhysicalPrefix prefix, SegmentHotDirectory hot) {
        this.shard = shard;
        this.owner = Objects.requireNonNull(owner, "owner");
        this.io = Objects.requireNonNull(io, "io");
        this.files = Objects.requireNonNull(files, "files");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.hot = Objects.requireNonNull(hot, "hot");
    }

    boolean submit(SegmentBatchFormat.FrozenBatch batch, Completion completion) {
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        return owner.submit(shard, new SegmentOwnerLoop.Command() {
            private boolean registered;

            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                SegmentHotDirectory.Admission admission;
                try {
                    admission = hot.reserve(batch);
                } catch (IOException failure) {
                    ticket.complete(() -> completion.complete(null, failure));
                    return;
                }
                IsolatedSegmentFiles.Reservation reservation;
                try {
                    reservation = prefix.reserve(batch, files, authority);
                } catch (IOException failure) {
                    hot.cancelBeforeSubmission(admission);
                    ticket.complete(() -> completion.complete(null, failure));
                    return;
                }
                pending.put(reservation, completion);
                registered = true;
                hot.bind(admission, reservation);
                io.submit(ticket, () -> files.writeReserved(reservation),
                        failure -> physicalFinished(reservation, failure));
            }

            @Override
            public void failed(Throwable failure) {
                owner.requireOwner();
                Throwable callbackFailure = registered ? null : deliver(completion, null, failure, null);
                try {
                    failPending(failure);
                } catch (RuntimeException | Error pendingFailure) {
                    if (callbackFailure != null) {
                        pendingFailure.addSuppressed(callbackFailure);
                    }
                    throw pendingFailure;
                }
                if (callbackFailure != null) {
                    throw new IllegalStateException("Physical failure callback failed", callbackFailure);
                }
            }
        });
    }

    private void physicalFinished(IsolatedSegmentFiles.Reservation reservation, Throwable ioFailure) {
        owner.requireOwner();
        if (owner.failure() != null) {
            failPending(owner.failure());
            return;
        }
        try {
            List<SegmentPhysicalPrefix.DurableBatch> ready = prefix.completed(reservation, ioFailure);
            if (prefix.failed()) {
                failPending(new IOException("Physical stream stopped before contiguous publication", ioFailure));
                return;
            }
            List<Delivery> deliveries = new ArrayList<>(ready.size());
            for (SegmentPhysicalPrefix.DurableBatch batch : ready) {
                List<SegmentPointRead.Location> locations = hot.publish(batch);
                Completion completion = pending.remove(batch.reservation());
                if (completion == null) {
                    throw new IllegalStateException("Missing physical completion obligation");
                }
                deliveries.add(new Delivery(completion, locations));
            }
            Throwable callbackFailure = null;
            for (Delivery delivery : deliveries) {
                callbackFailure = deliver(delivery.completion(), delivery.locations(), null, callbackFailure);
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Physical completion callback failed", callbackFailure);
            }
        } catch (RuntimeException | Error failure) {
            try {
                failPending(failure);
            } catch (RuntimeException | Error callbackFailure) {
                failure.addSuppressed(callbackFailure);
            }
            throw failure;
        }
    }

    private record Delivery(Completion completion, List<SegmentPointRead.Location> locations) { }

    private void failPending(Throwable failure) {
        List<Completion> callbacks = List.copyOf(pending.values());
        pending.clear();
        Throwable callbackFailure = null;
        for (Completion completion : callbacks) {
            callbackFailure = deliver(completion, null, failure, callbackFailure);
        }
        if (callbackFailure != null) {
            throw new IllegalStateException("Physical failure callback failed", callbackFailure);
        }
    }

    private static Throwable deliver(Completion completion, List<SegmentPointRead.Location> candidates,
                                     Throwable failure, Throwable previous) {
        try {
            completion.complete(candidates, failure);
        } catch (Throwable callbackFailure) {
            if (previous == null) {
                return callbackFailure;
            }
            previous.addSuppressed(callbackFailure);
        }
        return previous;
    }
}

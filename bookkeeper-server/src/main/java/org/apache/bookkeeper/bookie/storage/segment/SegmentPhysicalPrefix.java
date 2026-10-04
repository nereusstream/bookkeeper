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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Owner-thread physical prefix only; entry admission, locator publication and ACK remain separate. */
final class SegmentPhysicalPrefix {
    static final class DurableBatch {
        private final IsolatedSegmentFiles.Reservation reservation;

        private DurableBatch(IsolatedSegmentFiles.Reservation reservation) {
            this.reservation = reservation;
        }

        SegmentBatchFormat.Address address() {
            return reservation.address();
        }

        int length() {
            return reservation.length();
        }

        IsolatedSegmentFiles.Reservation reservation() {
            return reservation;
        }
    }

    private static final class Pending {
        private final IsolatedSegmentFiles.Reservation reservation;
        private boolean completed;
        private boolean durable;

        private Pending(IsolatedSegmentFiles.Reservation reservation) {
            this.reservation = reservation;
        }
    }

    private final SegmentOwnerLoop owner;
    private final int shard;
    private final Generation streamGeneration;
    private final long firstSequence;
    private final int capacity;
    private final Map<Long, Pending> pending = new HashMap<>();
    private long nextSequence;
    private long durableThrough;
    private boolean failed;

    SegmentPhysicalPrefix(SegmentOwnerLoop owner, int shard, Generation streamGeneration,
                          long firstSequence, int capacity) {
        if (firstSequence < 0 || capacity < 1) {
            throw new IllegalArgumentException("Invalid stream prefix bounds");
        }
        this.owner = owner;
        this.shard = shard;
        this.streamGeneration = streamGeneration;
        this.firstSequence = firstSequence;
        this.nextSequence = firstSequence;
        this.durableThrough = firstSequence - 1;
        this.capacity = capacity;
    }

    /** Validate a clean-session bootstrap before either this prefix or its hot directory is changed. */
    long restartThrough(SegmentOwnerLoop expectedOwner, int expectedShard,
                         SegmentDataRecovery.PhysicalInventory inventory,
                         SegmentArenaControlLog authority) throws IOException {
        owner.requireOwner();
        if (owner != expectedOwner || shard != expectedShard || failed || !pending.isEmpty()
                || nextSequence != firstSequence || durableThrough != firstSequence - 1) {
            throw new IOException("Physical prefix is not an untouched restart owner");
        }
        SegmentArenaControlLog.SingleStream stream = authority.restartStreamForShard(shard).orElse(null);
        if (stream == null) {
            return firstSequence - 1;
        }
        if (!stream.generation().equals(streamGeneration) || stream.firstSequence() != firstSequence) {
            throw new IOException("Restart prefix differs from the persisted stream");
        }
        return inventory.streamCandidate(authority, shard, streamGeneration).through();
    }

    void restoreCleanThrough(long through) {
        owner.requireOwner();
        durableThrough = through;
        nextSequence = through + 1;
    }

    IsolatedSegmentFiles.Reservation reserve(SegmentBatchFormat.FrozenBatch batch,
                                             IsolatedSegmentFiles files,
                                             SegmentArenaControlLog authority) throws IOException {
        owner.requireOwner();
        SegmentBatchFormat.Address address = batch.address();
        if (failed || pending.size() >= capacity || nextSequence == Long.MAX_VALUE
                || address.shard() != shard || !address.streamGeneration().equals(streamGeneration)
                || address.sequence() != nextSequence
                || authority.streamFirstSequence(shard, streamGeneration.value()).orElse(-1) != firstSequence) {
            throw new IOException("Physical stream cannot admit this batch");
        }
        IsolatedSegmentFiles.Reservation reservation = files.reserveAuthorized(batch, authority);
        pending.put(nextSequence, new Pending(reservation));
        nextSequence++;
        return reservation;
    }

    /** Select only at a nonempty seal; reserve still rechecks the complete authority and advances state. */
    SegmentBatchFormat.Address appendAddress(IsolatedSegmentFiles files, SegmentArenaControlLog authority,
                                             int bytes) throws IOException {
        owner.requireOwner();
        if (failed || nextSequence == Long.MAX_VALUE) {
            throw new IOException("Physical stream has an unresolved failure or exhausted sequence");
        }
        if (pending.size() >= capacity) {
            return null;
        }
        return files.appendAddress(authority, shard, streamGeneration, nextSequence, bytes);
    }

    SegmentArenaControlLog.PoolRequest poolRequest(int blocks) {
        owner.requireOwner();
        return new SegmentArenaControlLog.PoolRequest(shard, streamGeneration, firstSequence, blocks);
    }

    SegmentBatchFormat.Address relocationAddress(SegmentArenaControlLog.Allocation allocation) throws IOException {
        owner.requireOwner();
        if (failed || !pending.isEmpty() || nextSequence == Long.MAX_VALUE || allocation.ownerShard() != shard
                || !allocation.ownerGeneration().equals(streamGeneration)
                || allocation.streamFirstSequence() != firstSequence) {
            throw new IOException("Relocation destination differs from the drained original DATA stream");
        }
        return new SegmentBatchFormat.Address(allocation.file(), shard, allocation.allocationId(),
                new Generation(allocation.generation()), streamGeneration, nextSequence, allocation.offset());
    }

    boolean poolMissing(IsolatedSegmentFiles files, SegmentArenaControlLog authority, int bytes) throws IOException {
        owner.requireOwner();
        return pending.size() < capacity
                && files.poolMissing(authority, shard, streamGeneration, nextSequence, bytes);
    }

    List<DurableBatch> completed(IsolatedSegmentFiles.Reservation reservation, Throwable ioFailure) {
        owner.requireOwner();
        long sequence = reservation.address().sequence();
        Pending current = pending.get(sequence);
        if (current == null || current.reservation != reservation || current.completed) {
            throw new IllegalArgumentException("Unknown or repeated physical completion");
        }
        current.completed = true;
        current.durable = ioFailure == null && reservation.durable();
        if (!current.durable) {
            failed = true;
        }
        List<DurableBatch> advanced = new ArrayList<>();
        while (true) {
            Pending first = pending.get(durableThrough + 1);
            if (first == null || !first.durable) {
                break;
            }
            pending.remove(durableThrough + 1);
            durableThrough++;
            advanced.add(new DurableBatch(first.reservation));
        }
        return List.copyOf(advanced);
    }

    long durableThrough() {
        owner.requireOwner();
        return durableThrough;
    }

    /** Current single-stream proof; MOVE/FREE selector authority is not implemented by this prefix. */
    boolean containsDurable(SegmentPointRead.Location location) {
        owner.requireOwner();
        SegmentBatchFormat.Address address = location.address();
        return address.shard() == shard && address.streamGeneration().equals(streamGeneration)
                && address.sequence() >= firstSequence && address.sequence() <= durableThrough;
    }

    SegmentDataRecovery.StreamCoverage indexCoverage() {
        owner.requireOwner();
        return new SegmentDataRecovery.StreamCoverage(shard, streamGeneration, firstSequence, durableThrough);
    }

    int pending() {
        owner.requireOwner();
        return pending.size();
    }

    boolean failed() {
        owner.requireOwner();
        return failed;
    }
}

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;

/** Group-wide full-capacity source bytes for normal allocations or adopted private recovery snapshots. */
final class SegmentEntryInputs {
    /**
     * Known Input metadata before owner admission, bounded by source bytes rather than DATA tickets.
     * Backing contents, the ledger shell and driver descriptors keep their existing owners; additional
     * graphs supplied by custom allocators remain caller-owned.
     */
    static long metadataCapacity(long inputBudgetBytes) throws IOException {
        return metadataCapacity(inputBudgetBytes, SegmentControlCapacity.Layout.hotSpot());
    }

    static long metadataCapacity(long inputBudgetBytes, SegmentControlCapacity.Layout layout) {
        if (inputBudgetBytes < 1) {
            throw new IllegalArgumentException("Positive source budget required");
        }
        long inputs = inputBudgetBytes / SegmentBatchFormat.ENTRY_OVERHEAD;
        long defaultRoot = Math.max(layout.object("io.netty.buffer.UnpooledByteBufAllocator"
                        + "$InstrumentedUnpooledUnsafeHeapByteBuf"),
                layout.object("io.netty.buffer.UnpooledByteBufAllocator$InstrumentedUnpooledHeapByteBuf"));
        long chargedRoot = SegmentControlCapacity.sum(
                layout.object("org.apache.bookkeeper.proto.profile.ProfileByteBufAllocator$ChargedHeap"),
                layout.object("org.apache.bookkeeper.proto.profile.ProfileReceiveBudget$Credit"));
        long root = Math.max(defaultRoot,
                Math.max(layout.object("io.netty.buffer.UnpooledHeapByteBuf"), chargedRoot));
        // Every nonempty heap root owns one RefCnt; internalNioBuffer may cache one heap NIO wrapper.
        long input = SegmentControlCapacity.sum(layout.object(Input.class), layout.context(), root,
                layout.object("io.netty.util.internal.RefCnt"), layout.byteArrayMetadata(),
                layout.object("java.nio.HeapByteBuffer"));
        return SegmentControlCapacity.sum(SegmentControlCapacity.times(inputs, input), layout.identityMap(inputs));
    }

    final class Input implements AutoCloseable {
        private ProfileLedgerContext context;
        private ByteBuf bytes;
        private final int capacity;
        private boolean claimed;
        private boolean released;

        private Input(ProfileLedgerContext context, ByteBuf bytes) {
            this.context = context;
            this.bytes = bytes;
            capacity = bytes.capacity();
        }

        /** Caller fills this buffer before submission and must stop using all views when ownership transfers. */
        ByteBuf bytes() {
            synchronized (SegmentEntryInputs.this) {
                if (claimed || released) {
                    throw new IllegalStateException("Entry input is no longer caller-owned");
                }
                return bytes;
            }
        }

        SegmentBatchFormat.Data data() {
            synchronized (SegmentEntryInputs.this) {
                if (!claimed || released) {
                    throw new IllegalStateException("Entry input requires driver ownership");
                }
                return SegmentBatchFormat.entry(context, bytes);
            }
        }

        private SegmentEntryInputs budget() {
            return SegmentEntryInputs.this;
        }

        void releaseFromDriver() {
            synchronized (SegmentEntryInputs.this) {
                if (!claimed || released) {
                    throw new IllegalStateException("Entry input is not held by its driver");
                }
                release();
            }
        }

        private void release() {
            // Keep the full allocation charged until the only permitted reference has been released.
            if (bytes.refCnt() != 1) {
                throw new IllegalStateException("Entry input has an unaccounted retained reference");
            }
            bytes.release();
            released = true;
            context = null;
            bytes = null;
            usedBytes -= capacity;
        }

        @Override
        public void close() {
            synchronized (SegmentEntryInputs.this) {
                if (!released) {
                    if (claimed) {
                        throw new IllegalStateException("Submitted entry input belongs to its driver");
                    }
                    release();
                }
            }
        }
    }

    private final long limit;
    private final int maxEntryBytes;
    private long usedBytes;
    private boolean accepting = true;

    SegmentEntryInputs(long limit, int maxEntryBytes) {
        if (limit < 1 || maxEntryBytes < SegmentBatchFormat.ENTRY_OVERHEAD) {
            throw new IllegalArgumentException("Positive source budget and valid entry bound required");
        }
        this.limit = limit;
        this.maxEntryBytes = maxEntryBytes;
    }

    synchronized Input acquire(ProfileLedgerContext context, int bytes) {
        return acquire(context, bytes, UnpooledByteBufAllocator.DEFAULT);
    }

    /** Copy allocation shares the caller's backing ledger while preserving the independent source limit. */
    synchronized Input acquire(ProfileLedgerContext context, int bytes, ByteBufAllocator allocator) {
        Objects.requireNonNull(allocator, "allocator");
        if (!reserve(context, bytes)) {
            return null;
        }
        try {
            // No borrowed slice or pooled parent whose actual retained capacity exceeds this charge.
            return new Input(context, allocator.heapBuffer(bytes, bytes));
        } catch (ProfileReceiveBudget.CapacityExceededException refused) {
            usedBytes -= bytes;
            return null;
        } catch (RuntimeException | Error failure) {
            usedBytes -= bytes;
            throw failure;
        }
    }

    /** Adopt the private recovery snapshot after its admission transfers ownership; never copy its array. */
    synchronized Input adoptRecovery(ProfileLedgerContext context, byte[] bytes) {
        if (!reserve(context, bytes.length)) {
            return null;
        }
        try {
            return new Input(context, Unpooled.wrappedBuffer(bytes));
        } catch (RuntimeException | Error failure) {
            usedBytes -= bytes.length;
            throw failure;
        }
    }

    private boolean reserve(ProfileLedgerContext context, int bytes) {
        Objects.requireNonNull(context, "context");
        if (bytes < SegmentBatchFormat.ENTRY_OVERHEAD || bytes > maxEntryBytes) {
            throw new IllegalArgumentException("Entry input exceeds the configured size bound");
        }
        if (!accepting || bytes > limit - usedBytes) {
            return false;
        }
        usedBytes += bytes;
        return true;
    }

    synchronized void claim(List<Input> inputs) {
        IdentityHashMap<Input, Boolean> distinct = new IdentityHashMap<>();
        for (Input input : inputs) {
            if (input.budget() != this || input.claimed || input.released
                    || input.bytes.refCnt() != 1 || input.bytes.capacity() != input.capacity
                    || input.bytes.readableBytes() != input.capacity
                    || distinct.put(input, true) != null) {
                throw new IllegalArgumentException("Invalid, retained, incomplete or repeated entry input");
            }
        }
        for (Input input : inputs) {
            input.claimed = true;
        }
    }

    long capacityBytes() {
        return limit;
    }

    synchronized long usedBytes() {
        return usedBytes;
    }

    synchronized void stopAdmission() {
        accepting = false;
    }
}

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
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/** Fixed-size aligned pool initialized and reclaimed on its owner. Returns have independent reserved capacity. */
final class SegmentBufferPool implements AutoCloseable {
    final class Lease implements AutoCloseable {
        private final Slot slot;
        private final AtomicBoolean returned = new AtomicBoolean();

        private Lease(Slot slot) {
            this.slot = slot;
        }

        int ownerId() {
            return ownerId;
        }

        long generation() {
            return generation;
        }

        ByteBuffer bytes() {
            if (returned.get()) {
                throw new IllegalStateException("Buffer already returned");
            }
            return slot.view.duplicate();
        }

        @Override
        public void close() {
            if (!returned.compareAndSet(false, true)) {
                throw new IllegalStateException("Buffer returned twice");
            }
            // One return slot per physical buffer was allocated before any lease was admitted.
            if (!returns.offer(slot)) {
                throw new IllegalStateException("Reserved buffer-return capacity violated");
            }
            wakeOwner.run();
        }
    }

    private record Slot(ByteBuf allocation, ByteBuffer view) { }

    private final Thread owner = Thread.currentThread();
    private final int ownerId;
    private final long generation;
    private final Runnable wakeOwner;
    private final ArrayDeque<Slot> free;
    private final ArrayBlockingQueue<Slot> returns;
    private final long reservedBytes;
    private final int bytesPerBuffer;
    private final int bufferCount;
    private volatile int outstanding;
    private boolean closed;

    /**
     * Fixed pool Java graph. Aligned backing bytes, leases/caller views, native allocation, cleaners and
     * leak-tracker graphs retain their separate owners; a leak-aware ByteBuf wrapper shell is included.
     */
    static long metadataCapacity(SegmentControlCapacity.Layout layout, int count) {
        if (count < 1) {
            throw new IllegalArgumentException("Positive fixed pool capacity required");
        }
        long root = Math.max(layout.object("io.netty.buffer.UnpooledByteBufAllocator"
                        + "$InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf"),
                Math.max(layout.object("io.netty.buffer.UnpooledByteBufAllocator"
                                + "$InstrumentedUnpooledUnsafeDirectByteBuf"),
                        layout.object("io.netty.buffer.UnpooledByteBufAllocator"
                                + "$InstrumentedUnpooledDirectByteBuf")));
        long leakWrapper = Math.max(layout.object("io.netty.buffer.SimpleLeakAwareByteBuf"),
                layout.object("io.netty.buffer.AdvancedLeakAwareByteBuf"));
        long nio = Math.max(layout.object("java.nio.DirectByteBuffer"), layout.object("java.nio.DirectByteBufferR"));
        long slot = SegmentControlCapacity.sum(layout.object(Slot.class), root, leakWrapper,
                layout.object("io.netty.util.internal.RefCnt"),
                SegmentControlCapacity.times(2, nio)); // Netty root NIO and persistent Slot.view.
        // Construction within one pool is sequential; old aligned and current region/aligned views may coexist.
        return SegmentControlCapacity.sum(containerMetadataCapacity(layout, count),
                SegmentControlCapacity.times(count, slot), SegmentControlCapacity.times(3, nio),
                layout.object("java.util.ArrayDeque$DeqIterator"));
    }

    /** Successful close releases Slots but leaves these empty fixed containers on the closed pool. */
    static long containerMetadataCapacity(SegmentControlCapacity.Layout layout, int count) {
        if (count < 1) {
            throw new IllegalArgumentException("Positive fixed pool capacity required");
        }
        // JDK 17/21 ArrayDeque(count) keeps count+1 slots, with a special Integer.MAX_VALUE branch.
        long freeSlots = count == Integer.MAX_VALUE ? count : (long) count + 1;
        return SegmentControlCapacity.sum(layout.object(SegmentBufferPool.class), layout.captured(1),
                layout.object(ArrayDeque.class), layout.refs(freeSlots), layout.blockingQueue(count));
    }

    SegmentBufferPool(int ownerId, long generation, int count, int bytesPerBuffer, int alignment,
                      boolean ownerTouch, Runnable wakeOwner) {
        if (ownerId < 0 || generation < 1 || count < 1 || bytesPerBuffer < 1
                || alignment != 4096 || bytesPerBuffer % alignment != 0) {
            throw new IllegalArgumentException("Invalid fixed buffer pool");
        }
        this.ownerId = ownerId;
        this.generation = generation;
        this.bytesPerBuffer = bytesPerBuffer;
        bufferCount = count;
        this.wakeOwner = java.util.Objects.requireNonNull(wakeOwner, "wakeOwner");
        int allocationBytes = Math.addExact(bytesPerBuffer, alignment - 1);
        reservedBytes = Math.multiplyExact((long) count, allocationBytes);
        free = new ArrayDeque<>(count);
        returns = new ArrayBlockingQueue<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ByteBuf root = UnpooledByteBufAllocator.DEFAULT.directBuffer(allocationBytes, allocationBytes);
                try {
                    ByteBuffer aligned = root.nioBuffer(0, allocationBytes).alignedSlice(alignment);
                    aligned.limit(bytesPerBuffer);
                    if (ownerTouch) {
                        // Netty/malloc pages may already have been touched. This is not a page-locality guarantee.
                        for (int offset = 0; offset < bytesPerBuffer; offset += alignment) {
                            aligned.put(offset, (byte) 0);
                        }
                    }
                    free.addLast(new Slot(root, aligned.slice()));
                } catch (RuntimeException | Error failure) {
                    root.release();
                    throw failure;
                }
            }
        } catch (RuntimeException | Error failure) {
            for (Slot slot : free) {
                slot.allocation.release();
            }
            free.clear();
            throw failure;
        }
    }

    Lease acquire() {
        return acquire(1);
    }

    int bufferBytes() {
        return bytesPerBuffer;
    }

    Lease acquire(int minimumBytes) {
        requireOwner();
        if (closed) {
            throw new IllegalStateException("Pool closed");
        }
        if (minimumBytes < 1) {
            throw new IllegalArgumentException("Positive buffer size required");
        }
        drainReturns();
        if (minimumBytes > bytesPerBuffer) {
            return null;
        }
        Slot slot = free.pollFirst();
        if (slot == null) {
            return null;
        }
        outstanding++;
        return new Lease(slot);
    }

    boolean drainReturns() {
        requireOwner();
        boolean work = false;
        Slot slot;
        while ((slot = returns.poll()) != null) {
            outstanding--;
            free.addLast(slot);
            work = true;
        }
        return work;
    }

    int outstanding() {
        requireOwner();
        return outstanding;
    }

    /** Conservative diagnostic; queued returns remain charged until their original owner drains them. */
    boolean availableForMaintenance(int required) {
        return bufferCount - outstanding >= required;
    }

    long reservedBytes() {
        return reservedBytes;
    }

    @Override
    public void close() {
        requireOwner();
        drainReturns();
        if (outstanding != 0) {
            throw new IllegalStateException("Cannot close pool with live leases");
        }
        if (!closed) {
            closed = true;
            for (Slot slot : free) {
                slot.allocation.release();
            }
            free.clear();
        }
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Pool allocation/reclamation belongs to its original owner");
        }
    }
}

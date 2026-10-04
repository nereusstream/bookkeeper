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

import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Experimental immutable DATA framing. No allocator, current-selector or ACK authority. */
final class SegmentBatchFormat {
    static final long MAGIC = 0x5557444154413031L; // UWDATA01
    static final long END_MAGIC = 0x5557454e44303031L; // UWEND001
    static final int RECORD_MAGIC = 0x55575231; // UWR1
    static final int HEADER = 128;
    static final int RECORD_HEADER = 160;
    static final int FOOTER = 32;
    static final int ENTRY_OVERHEAD = 36;
    static final int MAX_BATCH = 2 * 1024 * 1024;
    static final int MAX_RECORDS = 256;
    static final int ENTRY_DATA = 1;
    static final int RELOCATION_COPY = 2;
    // The isolated point-read buffer reserves 128 bytes for its response envelope.
    // A future transport adapter must prove its actual envelope fits that budget before enabling it.
    static final int READ_ENVELOPE_BUDGET = 128;
    static final int READ_FRAME_CAPACITY = 1024 * 1024 + ENTRY_OVERHEAD + READ_ENVELOPE_BUDGET;

    record Address(int fileIndex, int shard, long allocation, Generation generation,
                   Generation streamGeneration, long sequence, long offset) {
        Address {
            Objects.requireNonNull(generation, "generation");
            Objects.requireNonNull(streamGeneration, "streamGeneration");
            if (fileIndex < 0 || shard < 0 || allocation < 0 || sequence < 0 || offset < 4096 || offset % 4096 != 0) {
                throw new IllegalArgumentException("Invalid DATA address");
            }
        }
    }

    record Data(ProfileLedgerContext context, ByteBuf entry, int origin, UUID moveId, long moveGeneration) {
        Data {
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(entry, "entry");
            Objects.requireNonNull(moveId, "moveId");
            boolean zeroMove = moveId.getMostSignificantBits() == 0 && moveId.getLeastSignificantBits() == 0;
            if (origin == ENTRY_DATA ? !zeroMove || moveGeneration != 0
                    : origin != RELOCATION_COPY || zeroMove || moveGeneration <= 0) {
                throw new IllegalArgumentException("Invalid record origin or relocation identity");
            }
        }
    }

    static final class FrozenBatch implements AutoCloseable {
        private final Address address;
        private final ByteBuffer bytes;
        private final SegmentBufferPool.Lease lease;
        private boolean driverOwned;
        private boolean released;

        private FrozenBatch(Address address, ByteBuffer bytes, SegmentBufferPool.Lease lease) {
            this.address = address;
            this.bytes = bytes.asReadOnlyBuffer();
            this.lease = lease;
        }

        Address address() {
            return address;
        }

        synchronized ByteBuffer bytes() {
            if (released) {
                throw new IllegalStateException("Frozen DATA buffer was released");
            }
            return bytes.asReadOnlyBuffer();
        }

        int length() {
            return bytes.remaining();
        }

        synchronized void claimForDriver() {
            if (lease != null) {
                if (driverOwned || released) {
                    throw new IllegalStateException("Pooled DATA buffer is no longer caller-owned");
                }
                driverOwned = true;
            }
        }

        synchronized void requirePhysicalOwner() {
            if (lease != null && (!driverOwned || released)) {
                throw new IllegalStateException("Pooled DATA must be owned by its physical driver");
            }
        }

        synchronized void releaseFromDriver() {
            if (lease != null) {
                requirePhysicalOwner();
                released = true;
                lease.close();
            }
        }

        @Override
        public synchronized void close() {
            if (lease != null && !released) {
                if (driverOwned) {
                    throw new IllegalStateException("Submitted DATA buffer belongs to its driver");
                }
                released = true;
                lease.close();
            }
        }
    }

    static Data entry(ProfileLedgerContext context, ByteBuf entry) {
        return new Data(context, entry, ENTRY_DATA, new UUID(0, 0), 0);
    }

    static Data relocation(ProfileLedgerContext context, ByteBuf entry, UUID moveId, Generation moveGeneration) {
        return new Data(context, entry, RELOCATION_COPY, moveId, moveGeneration.value());
    }

    static int maxPayload(IsolatedSegmentConfig config) {
        int normal = ProfileFrameCodec.ABSOLUTE_FRAME_MAX - 32 - 100 - ENTRY_OVERHEAD;
        int recovery = ProfileFrameCodec.ABSOLUTE_FRAME_MAX - 32 - 144 - ENTRY_OVERHEAD;
        int read = READ_FRAME_CAPACITY - READ_ENVELOPE_BUDGET - ENTRY_OVERHEAD;
        int segment = MAX_BATCH - HEADER - FOOTER - RECORD_HEADER - ENTRY_OVERHEAD;
        return Math.min(config.maxPayloadBytes(), Math.min(Math.min(normal, recovery), Math.min(read, segment)));
    }

    static FrozenBatch encode(IsolatedSegmentConfig config, Address address, List<Data> records) {
        int recordBytes = validate(config, address, records);
        int length = alignedLength(recordBytes);
        return encode(address, records, recordBytes,
                ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN), null);
    }

    /** Acquires fixed owner storage before copying; null means no suitable submission working set. */
    static FrozenBatch encodePooled(IsolatedSegmentConfig config, Address address, List<Data> records,
                                    SegmentBufferPool pool) {
        int recordBytes = validate(config, address, records);
        int length = alignedLength(recordBytes);
        SegmentBufferPool.Lease lease = pool.acquire(length);
        if (lease == null) {
            return null;
        }
        try {
            ByteBuffer bytes = lease.bytes().slice(0, length).order(ByteOrder.BIG_ENDIAN);
            return encode(address, records, recordBytes, bytes, lease);
        } catch (RuntimeException | Error failure) {
            lease.close();
            throw failure;
        }
    }

    static int validate(IsolatedSegmentConfig config, Address address, List<Data> records) {
        if (address.fileIndex() >= config.dataFiles() || address.shard() >= config.logicalShards()
                || address.fileIndex() != address.shard() % config.dataFiles()
                || records.isEmpty() || records.size() > MAX_RECORDS) {
            throw new IllegalArgumentException("Invalid batch bounds");
        }
        int recordBytes = 0;
        for (Data record : records) {
            validateEntry(record, maxPayload(config));
            recordBytes = Math.addExact(recordBytes, Math.addExact(RECORD_HEADER, record.entry().readableBytes()));
        }
        int length = alignedLength(recordBytes);
        if (length > MAX_BATCH || Math.addExact(address.offset(), length) > config.bytesPerFile()) {
            throw new IllegalArgumentException("Batch exceeds fixed file capacity");
        }
        return recordBytes;
    }

    static int alignedLength(int recordBytes) {
        int raw = Math.addExact(Math.addExact(HEADER, recordBytes), FOOTER);
        return Math.multiplyExact(Math.floorDiv(Math.addExact(raw, 4095), 4096), 4096);
    }

    /** One fixed pooled COPY buffer; source ByteBuf views are borrowed only for synchronous append. */
    static final class RelocationBuilder implements AutoCloseable {
        private final IsolatedSegmentConfig config;
        private final Address address;
        private final ByteBuffer bytes;
        private final UUID operation;
        private final Generation generation;
        private SegmentBufferPool.Lease lease;
        private int records;
        private int recordBytes;

        private RelocationBuilder(IsolatedSegmentConfig config, Address address, UUID operation, Generation generation,
                                  SegmentBufferPool.Lease lease, int capacity) {
            this.config = config;
            this.address = address;
            this.operation = operation;
            this.generation = generation;
            this.lease = lease;
            bytes = lease.bytes().slice(0, capacity).order(ByteOrder.BIG_ENDIAN);
            bytes.position(HEADER);
        }

        void append(ProfileLedgerContext context, ByteBuf entry) {
            if (lease == null) {
                throw new IllegalStateException("COPY builder has already transferred or released its buffer");
            }
            Data record = relocation(context, entry, operation, generation);
            validateEntry(record, maxPayload(config));
            int next = Math.addExact(recordBytes, Math.addExact(RECORD_HEADER, entry.readableBytes()));
            int length = alignedLength(next);
            if (records == MAX_RECORDS || length > bytes.capacity()
                    || Math.addExact(address.offset(), length) > config.bytesPerFile()) {
                throw new IllegalArgumentException("COPY chunk exceeds its pooled buffer or destination range");
            }
            writeRecord(address, record, bytes);
            recordBytes = next;
            records++;
        }

        FrozenBatch finish() {
            if (lease == null || records == 0) {
                throw new IllegalStateException("COPY builder must own a nonempty frozen batch");
            }
            FrozenBatch batch = freeze(address, records, recordBytes,
                    bytes.slice(0, alignedLength(recordBytes)).order(ByteOrder.BIG_ENDIAN), lease);
            lease = null;
            return batch;
        }

        @Override
        public void close() {
            if (lease != null) {
                lease.close();
                lease = null;
            }
        }
    }

    static RelocationBuilder relocationBuilder(IsolatedSegmentConfig config, Address address, UUID operation,
                                                Generation generation, SegmentBufferPool pool) {
        if (address.fileIndex() >= config.dataFiles() || address.shard() >= config.logicalShards()
                || address.fileIndex() != address.shard() % config.dataFiles()) {
            throw new IllegalArgumentException("Invalid pooled COPY address");
        }
        int capacity = Math.min(MAX_BATCH, pool.bufferBytes());
        SegmentBufferPool.Lease lease = pool.acquire(capacity);
        if (lease == null) {
            return null;
        }
        try {
            return new RelocationBuilder(config, address, operation, generation, lease, capacity);
        } catch (RuntimeException | Error failure) {
            lease.close();
            throw failure;
        }
    }

    private static FrozenBatch encode(Address address, List<Data> records, int recordBytes,
                                      ByteBuffer bytes, SegmentBufferPool.Lease lease) {
        bytes.position(HEADER);
        for (Data record : records) {
            writeRecord(address, record, bytes);
        }
        return freeze(address, records.size(), recordBytes, bytes, lease);
    }

    private static void writeRecord(Address address, Data record, ByteBuffer bytes) {
        int start = bytes.position();
        int entryLength = record.entry().readableBytes();
        bytes.putInt(RECORD_MAGIC).putShort((short) 1).put((byte) record.origin()).put((byte) 0)
                .putInt(RECORD_HEADER + entryLength).putInt(entryLength)
                .putLong(address.allocation()).putLong(address.generation().value())
                .putLong(Math.addExact(address.offset(), start)).put(record.context().encode())
                .putLong(record.moveId().getMostSignificantBits())
                .putLong(record.moveId().getLeastSignificantBits())
                .putLong(record.moveGeneration()).putInt(address.fileIndex()).putInt(address.shard())
                .putLong(address.streamGeneration().value()).putLong(address.sequence());
        zero(bytes, start + 148, start + 156);
        bytes.putInt(start + 156, checksum(bytes, start, 156));
        bytes.position(start + RECORD_HEADER);
        ByteBuffer target = bytes.slice();
        target.limit(entryLength);
        record.entry().getBytes(record.entry().readerIndex(), target);
        bytes.position(start + RECORD_HEADER + entryLength);
    }

    private static FrozenBatch freeze(Address address, int records, int recordBytes, ByteBuffer bytes,
                                      SegmentBufferPool.Lease lease) {
        int length = bytes.limit();
        bytes.position(0);
        // Only headers and padding need clearing on reuse; payload is copied exactly once.
        zero(bytes, 84, 124);
        bytes.putLong(MAGIC).putInt(1).putInt(HEADER).putInt(address.fileIndex()).putInt(address.shard())
                .putLong(address.allocation()).putLong(address.generation().value())
                .putLong(address.shard()).putLong(address.streamGeneration().value())
                .putLong(address.sequence()).putLong(address.offset()).putInt(length)
                .putInt(records).putInt(recordBytes);
        bytes.putInt(124, checksum(bytes, 0, 124));
        int footer = length - FOOTER;
        zero(bytes, HEADER + recordBytes, footer);
        bytes.position(footer);
        bytes.putLong(END_MAGIC).putLong(address.sequence()).putInt(length)
                .putInt(checksum(bytes, HEADER, footer - HEADER)).putInt(bytes.getInt(124));
        bytes.putInt(checksum(bytes, footer, 28));
        bytes.clear();
        return new FrozenBatch(address, bytes, lease);
    }

    private static void zero(ByteBuffer bytes, int start, int end) {
        for (int offset = start; offset < end; offset++) {
            bytes.put(offset, (byte) 0);
        }
    }

    static void validateEntry(Data record, int maximum) {
        ByteBuf entry = record.entry();
        int length = entry.readableBytes();
        if (length < ENTRY_OVERHEAD || length - ENTRY_OVERHEAD > maximum
                || entry.order() != ByteOrder.BIG_ENDIAN) {
            throw new IllegalArgumentException("Invalid BK CRC32C entry size or byte order");
        }
        int start = entry.readerIndex();
        if (record.context().ledgerId() < 0 || entry.getLong(start) != record.context().ledgerId()
                || entry.getLong(start + 8) < 0 || entry.getLong(start + 16) < -1 || entry.getLong(start + 24) < 0) {
            throw new IllegalArgumentException("Invalid BK entry coordinate or metadata");
        }
        int crc = Crc32cIntChecksum.resumeChecksum(0, entry, start, 32);
        crc = Crc32cIntChecksum.resumeChecksum(crc, entry, start + ENTRY_OVERHEAD, length - ENTRY_OVERHEAD);
        if (crc != entry.getInt(start + 32)) {
            throw new IllegalArgumentException("BK entry CRC32C mismatch");
        }
    }

    private static int checksum(ByteBuffer bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes.slice(offset, length));
        return (int) crc.getValue();
    }

    private SegmentBatchFormat() {
    }
}

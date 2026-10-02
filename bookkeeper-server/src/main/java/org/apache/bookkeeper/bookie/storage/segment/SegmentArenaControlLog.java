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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Isolated single-Arena allocation authority; no DATA, FREE, MOVE or production Bookie access. */
final class SegmentArenaControlLog implements AutoCloseable {
    static final long BLOCK_BYTES = 1024 * 1024;
    private static final long FIRST_RECORD = 4096;
    private static final Generation LOG_GENERATION = new Generation(1);

    record Allocation(SegmentControlFormat.Type type, UUID operationId, Generation operationGeneration,
                      long expectedGeneration, int file, int ownerShard, Generation ownerGeneration,
                      long allocationId, long offset, int blocks, long streamFirstSequence) {
        Allocation {
            if (type != SegmentControlFormat.Type.ALLOC && type != SegmentControlFormat.Type.ALLOC_POOL) {
                throw new IllegalArgumentException("Only ALLOC and ALLOC_POOL are enabled");
            }
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(operationGeneration, "operationGeneration");
            Objects.requireNonNull(ownerGeneration, "ownerGeneration");
            if (operationId.equals(new UUID(0, 0)) || expectedGeneration < 0 || allocationId <= 0 || blocks < 1
                    || streamFirstSequence < 0 || streamFirstSequence == Long.MAX_VALUE
                    || (type == SegmentControlFormat.Type.ALLOC && blocks != 1)) {
                throw new IllegalArgumentException("Invalid allocation command");
            }
        }

        ByteBuffer body() {
            return ByteBuffer.allocate(48).putInt(file).putInt(ownerShard).putLong(ownerGeneration.value())
                    .putLong(allocationId).putLong(offset).putInt(blocks).putInt(0)
                    .putLong(streamFirstSequence).flip();
        }
    }

    enum Status {
        APPLIED_DURABLE, ALREADY_DURABLE, CONDITION_FAILED, INCOMPATIBLE
    }

    record Result(Status status, UUID operationId, long sequence, long generation,
                  long durableThrough, long appliedThrough) { }

    record DurableBatch(long firstSequence, List<Allocation> commands) {
        DurableBatch {
            commands = List.copyOf(commands);
        }
    }

    private record Slot(int file, long index) { }

    private record Stream(int shard, long generation) { }

    record SingleStream(Generation generation, long firstSequence) { }

    private record Grant(Allocation command, long generation) { }

    private record AppliedOperation(Allocation command, Status status) { }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final long maxLogBytes;
    private final SegmentFileIo io;
    private final FileChannel channel;
    private final FileLock lock;
    private final Map<Slot, Grant> allocations = new HashMap<>();
    private final Map<Long, Allocation> allocationIds = new HashMap<>();
    private final Map<Stream, Long> streamStarts = new HashMap<>();
    private final Map<UUID, AppliedOperation> appliedOperations = new HashMap<>();
    private long nextOffset = FIRST_RECORD;
    private long durableThrough = -1;
    private long appliedThrough = -1;
    private DurableBatch pending;
    private boolean writing;
    private boolean failed;
    private boolean closed;

    private SegmentArenaControlLog(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                   long maxLogBytes, SegmentFileIo io,
                                   FileChannel channel, FileLock lock) {
        this.config = config;
        this.identity = identity;
        this.maxLogBytes = maxLogBytes;
        this.io = io;
        this.channel = channel;
        this.lock = lock;
    }

    static SegmentArenaControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                       long maxLogBytes) throws IOException {
        return open(config, identity, maxLogBytes, new SegmentFileIo());
    }

    static SegmentArenaControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                       long maxLogBytes, SegmentFileIo io) throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(io, "io");
        if (maxLogBytes < FIRST_RECORD + 192) {
            throw new IllegalArgumentException("Control log capacity cannot hold one record");
        }
        FileChannel channel = FileChannel.open(config.directory().resolve("arena-control"),
                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        FileLock lock = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Arena control log already has a writer", overlapping);
            }
            if (lock == null) {
                throw new IOException("Arena control log already has a writer");
            }
            SegmentArenaControlLog result = new SegmentArenaControlLog(config, identity, maxLogBytes, io,
                    channel, lock);
            result.validateStorageFiles(identity);
            result.recover(identity);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                channel.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** One physical force covers the whole batch; no result is applied before that force returns. */
    DurableBatch appendDurably(List<Allocation> commands) throws IOException {
        List<Allocation> batch = List.copyOf(commands);
        List<ByteBuffer> encoded;
        long sequence;
        long start;
        synchronized (this) {
            requireWritable();
            if (writing || pending != null || batch.isEmpty() || batch.size() > config.maxPendingEntries()) {
                throw new IllegalArgumentException("Invalid or unresolved control batch");
            }
            encoded = new ArrayList<>(batch.size());
            long bytes = 0;
            sequence = durableThrough + 1;
            for (Allocation command : batch) {
                validateRange(command);
                ByteBuffer record = SegmentControlFormat.encode(command.type(), LOG_GENERATION, sequence++,
                        command.operationId(), command.operationGeneration(), command.expectedGeneration(),
                        command.body());
                bytes = Math.addExact(bytes, record.remaining());
                encoded.add(record);
            }
            if (bytes > maxLogBytes - nextOffset) {
                throw new IOException("Arena control log capacity exhausted");
            }
            start = nextOffset;
            // Reserve the physical suffix before the first write; failure never reuses its coordinates.
            nextOffset += bytes;
            writing = true;
        }
        try {
            long position = start;
            for (ByteBuffer record : encoded) {
                io.writeFully(channel, record, position);
                position += record.remaining();
            }
            io.force(channel, true);
            synchronized (this) {
                durableThrough = sequence - 1;
                pending = new DurableBatch(durableThrough - batch.size() + 1, batch);
                return pending;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
            }
            throw failure;
        } finally {
            synchronized (this) {
                writing = false;
            }
        }
    }

    synchronized List<Result> applyDurable(DurableBatch batch) throws IOException {
        requireWritable();
        if (pending != batch || batch.firstSequence() != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable batch can be applied");
        }
        List<Result> results = new ArrayList<>(batch.commands().size());
        try {
            for (Allocation command : batch.commands()) {
                long sequence = ++appliedThrough;
                Status status = apply(command);
                long generation = status == Status.APPLIED_DURABLE || status == Status.ALREADY_DURABLE
                        ? command.expectedGeneration() + 1 : 0;
                results.add(new Result(status, command.operationId(), sequence, generation,
                        durableThrough, appliedThrough));
            }
        } catch (RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
        pending = null;
        return List.copyOf(results);
    }

    List<Result> submitBatch(List<Allocation> commands) throws IOException {
        return applyDurable(appendDurably(commands));
    }

    synchronized boolean hasAppliedGrant(int file, long offset, long bytes, int ownerShard,
                                         long ownerGeneration, long allocationId, long allocationGeneration) {
        if (failed || closed || file < 0 || file >= config.dataFiles() || offset < FIRST_RECORD
                || offset % config.alignment() != 0 || bytes <= 0 || bytes > config.bytesPerFile() - offset) {
            return false;
        }
        long first = (offset - FIRST_RECORD) / BLOCK_BYTES;
        long last = (offset - FIRST_RECORD + bytes - 1) / BLOCK_BYTES;
        for (long block = first; block <= last; block++) {
            Grant grant = allocations.get(new Slot(file, block));
            if (grant == null || grant.command().ownerShard() != ownerShard
                    || grant.command().ownerGeneration().value() != ownerGeneration
                    || grant.command().allocationId() != allocationId
                    || grant.generation() != allocationGeneration) {
                return false;
            }
        }
        return true;
    }

    /** Select recovery pages from applied allocation ownership; full batch identity is checked separately. */
    synchronized boolean hasAppliedStreamAt(int file, long offset, int shard, long streamGeneration) {
        if (failed || closed || file < 0 || file >= config.dataFiles()
                || offset < FIRST_RECORD || offset >= config.bytesPerFile()
                || offset % config.alignment() != 0) {
            return false;
        }
        Grant grant = allocations.get(new Slot(file, (offset - FIRST_RECORD) / BLOCK_BYTES));
        return grant != null && grant.command().ownerShard() == shard
                && grant.command().ownerGeneration().value() == streamGeneration;
    }

    synchronized OptionalLong streamFirstSequence(int shard, long generation) {
        if (failed || closed) {
            return OptionalLong.empty();
        }
        Long first = streamStarts.get(new Stream(shard, generation));
        return first == null ? OptionalLong.empty() : OptionalLong.of(first);
    }

    /** Earliest whole-batch applied pool at/after this stream's cursor; does not reserve or refill space. */
    synchronized Optional<SegmentBatchFormat.Address> appendAddress(int file, long minimum, int bytes,
                                                                   int shard, Generation stream, long sequence) {
        if (failed || closed || bytes <= 0) {
            return Optional.empty();
        }
        SegmentBatchFormat.Address selected = null;
        for (Allocation command : allocationIds.values()) {
            if (command.file() != file || command.ownerShard() != shard
                    || !command.ownerGeneration().equals(stream)) {
                continue;
            }
            long offset = Math.max(minimum, command.offset());
            long end = command.offset() + command.blocks() * BLOCK_BYTES;
            if (offset <= end - bytes && (selected == null || offset < selected.offset())) {
                selected = new SegmentBatchFormat.Address(file, shard, command.allocationId(),
                        new Generation(command.expectedGeneration() + 1), stream, sequence, offset);
            }
        }
        return Optional.ofNullable(selected);
    }

    /** The isolated allocator has no selector or reuse: multiple generations make read-only selection ambiguous. */
    synchronized Optional<SingleStream> singleStreamForShard(int shard) {
        if (failed || closed || shard < 0 || shard >= config.logicalShards()) {
            return Optional.empty();
        }
        SingleStream selected = null;
        for (Map.Entry<Stream, Long> entry : streamStarts.entrySet()) {
            if (entry.getKey().shard() == shard) {
                if (selected != null) {
                    return Optional.empty();
                }
                selected = new SingleStream(new Generation(entry.getKey().generation()), entry.getValue());
            }
        }
        return Optional.ofNullable(selected);
    }

    /** No FREE exists yet: every applied grant remains unavailable even when its DATA bytes are zero. */
    synchronized long grantedHighWatermark(int file) throws IOException {
        requireWritable();
        if (file < 0 || file >= config.dataFiles()) {
            throw new IllegalArgumentException("Invalid DATA file");
        }
        long end = FIRST_RECORD;
        for (Slot slot : allocations.keySet()) {
            if (slot.file() == file) {
                end = Math.max(end, FIRST_RECORD + (slot.index() + 1) * BLOCK_BYTES);
            }
        }
        return end;
    }

    synchronized boolean matchesScope(IsolatedSegmentConfig otherConfig, SegmentFileHeader otherIdentity) {
        return !closed && !failed && config.equals(otherConfig) && identity.equals(otherIdentity);
    }

    synchronized void requireQuiescentRecovery(IsolatedSegmentConfig otherConfig,
                                               SegmentFileHeader otherIdentity) throws IOException {
        if (!matchesScope(otherConfig, otherIdentity) || writing || pending != null) {
            throw new IOException("Arena authority is unavailable or has unresolved control I/O");
        }
    }

    synchronized long durableThrough() {
        return durableThrough;
    }

    synchronized long appliedThrough() {
        return appliedThrough;
    }

    private void recover(SegmentFileHeader identity) throws IOException {
        long size = channel.size();
        if (size < FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid Arena control log size");
        }
        ByteBuffer header = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(channel, header, 0);
        identity.validate(header, config, -4);
        long sequence = 0;
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence++);
            offset += length;
        }
        // Complete records can include a lost response. Establish durability before exposing replayed grants.
        channel.force(true);
        sequence = 0;
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            Allocation command = readAllocation(offset, length, sequence);
            durableThrough = sequence;
            appliedThrough = sequence;
            apply(command);
            offset += length;
            sequence++;
        }
        nextOffset = size;
    }

    private void validateStorageFiles(SegmentFileHeader identity) throws IOException {
        if (Files.isSymbolicLink(config.directory())) {
            throw new IOException("Isolated storage directory cannot be a symlink");
        }
        validateIdentityFile(config.directory().resolve("superblock-a"), identity, -1, 4096);
        validateIdentityFile(config.directory().resolve("superblock-b"), identity, -2, 4096);
        validateIdentityFile(config.directory().resolve("bookie-control"), identity, -3, -1);
        for (int file = 0; file < config.dataFiles(); file++) {
            validateIdentityFile(IsolatedSegmentFiles.dataPath(config, file), identity, file,
                    config.bytesPerFile());
        }
    }

    private void validateIdentityFile(Path path, SegmentFileHeader identity, int fileId, long expectedSize)
            throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("Isolated storage file cannot be a symlink: " + path);
        }
        try (FileChannel input = FileChannel.open(path, StandardOpenOption.READ)) {
            if ((expectedSize < 0 && input.size() < 4096)
                    || (expectedSize >= 0 && input.size() != expectedSize)) {
                throw new IOException("Isolated storage file size mismatch: " + path);
            }
            ByteBuffer bytes = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(input, bytes, 0);
            identity.validate(bytes, config, fileId);
        }
    }

    private int recordLength(long offset, long size, long sequence) throws IOException {
        if (size - offset < 128) {
            throw new IOException("Incomplete Arena control tail; writable recovery is unsafe");
        }
        ByteBuffer header = ByteBuffer.allocate(128);
        SegmentFileIo.readFully(channel, header, offset);
        int length = SegmentControlParser.recordLength(header, SegmentControlFormat.Owner.ARENA,
                LOG_GENERATION, sequence);
        if (length > size - offset) {
            throw new IOException("Incomplete Arena control record; writable recovery is unsafe");
        }
        return length;
    }

    private Allocation readAllocation(long offset, int length, long sequence) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        SegmentFileIo.readFully(channel, bytes, offset);
        SegmentControlParser.Command parsed = SegmentControlParser.parse(bytes, SegmentControlFormat.Owner.ARENA,
                LOG_GENERATION, sequence);
        if (parsed.type() != SegmentControlFormat.Type.ALLOC
                && parsed.type() != SegmentControlFormat.Type.ALLOC_POOL) {
            throw new IOException("Unsupported Arena command in required control history");
        }
        ByteBuffer body = parsed.body();
        if (body.remaining() != 48) {
            throw new IOException("Unsupported Arena allocation body");
        }
        int file = body.getInt();
        int ownerShard = body.getInt();
        long ownerGeneration = body.getLong();
        long allocationId = body.getLong();
        long position = body.getLong();
        int blocks = body.getInt();
        if (body.getInt() != 0) {
            throw new IOException("Unsupported Arena allocation extension");
        }
        long streamFirstSequence = body.getLong();
        try {
            Allocation command = new Allocation(parsed.type(), parsed.operationId(),
                    parsed.operationGeneration(), parsed.expectedGeneration(), file, ownerShard,
                    new Generation(ownerGeneration), allocationId, position, blocks, streamFirstSequence);
            validateRange(command);
            return command;
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid Arena allocation command", invalid);
        }
    }

    private void validateRange(Allocation command) {
        if (command.file() < 0 || command.file() >= config.dataFiles()
                || command.ownerShard() < 0 || command.ownerShard() >= config.logicalShards()
                || command.file() != command.ownerShard() % config.dataFiles()
                || command.offset() < FIRST_RECORD || (command.offset() - FIRST_RECORD) % BLOCK_BYTES != 0
                || command.offset() > config.bytesPerFile()
                || command.blocks() > (config.bytesPerFile() - command.offset()) / BLOCK_BYTES
                || command.expectedGeneration() != 0) {
            throw new IllegalArgumentException("Invalid or unsupported Arena allocation range/generation");
        }
    }

    private Status apply(Allocation command) {
        AppliedOperation prior = appliedOperations.get(command.operationId());
        if (prior != null) {
            if (!prior.command().equals(command)) {
                return Status.INCOMPATIBLE;
            }
            return prior.status() == Status.APPLIED_DURABLE ? Status.ALREADY_DURABLE : prior.status();
        }
        if (allocationIds.containsKey(command.allocationId())) {
            appliedOperations.put(command.operationId(), new AppliedOperation(command, Status.CONDITION_FAILED));
            return Status.CONDITION_FAILED;
        }
        Stream stream = new Stream(command.ownerShard(), command.ownerGeneration().value());
        Long first = streamStarts.get(stream);
        if (first != null && first != command.streamFirstSequence()) {
            appliedOperations.put(command.operationId(), new AppliedOperation(command, Status.CONDITION_FAILED));
            return Status.CONDITION_FAILED;
        }
        for (int block = 0; block < command.blocks(); block++) {
            if (allocations.containsKey(slot(command.file(), command.offset() + block * BLOCK_BYTES))) {
                appliedOperations.put(command.operationId(), new AppliedOperation(command, Status.CONDITION_FAILED));
                return Status.CONDITION_FAILED;
            }
        }
        Grant grant = new Grant(command, command.expectedGeneration() + 1);
        for (int block = 0; block < command.blocks(); block++) {
            allocations.put(slot(command.file(), command.offset() + block * BLOCK_BYTES), grant);
        }
        allocationIds.put(command.allocationId(), command);
        streamStarts.putIfAbsent(stream, command.streamFirstSequence());
        appliedOperations.put(command.operationId(), new AppliedOperation(command, Status.APPLIED_DURABLE));
        return Status.APPLIED_DURABLE;
    }

    private Slot slot(int file, long offset) {
        return new Slot(file, (offset - FIRST_RECORD) / BLOCK_BYTES);
    }

    private void requireWritable() throws IOException {
        if (closed || failed) {
            throw new IOException("Arena control log is closed or has uncertain I/O");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (writing) {
            throw new IOException("Arena control I/O is still in flight");
        }
        closed = true;
        IOException first = null;
        try {
            lock.release();
        } catch (IOException failure) {
            first = failure;
        }
        try {
            channel.close();
        } catch (IOException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        if (first != null) {
            throw first;
        }
    }
}

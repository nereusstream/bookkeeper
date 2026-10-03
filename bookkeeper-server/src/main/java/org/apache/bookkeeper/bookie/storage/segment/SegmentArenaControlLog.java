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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
    private static final int QUARANTINE_RECORD_BYTES = 128 + 16 + 16;

    sealed interface Command permits Allocation, FileQuarantine, DataSession, CheckpointCommit {
        SegmentControlFormat.Type type();
        UUID operationId();
        Generation operationGeneration();
        long expectedGeneration();
        ByteBuffer body();
    }

    private record CheckpointCommit(UUID operationId, SegmentControlCheckpoint.Reference reference) implements Command {
        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.CHECKPOINT_COMMIT;
        }

        @Override
        public Generation operationGeneration() {
            return new Generation(reference.generation());
        }

        @Override
        public long expectedGeneration() {
            return reference.generation() - 1;
        }

        @Override
        public ByteBuffer body() {
            return reference.encode();
        }
    }

    /** Monotone file quarantine; no clear/reset operation is enabled. */
    record FileQuarantine(UUID operationId, Generation operationGeneration, int file) implements Command {
        FileQuarantine {
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(operationGeneration, "operationGeneration");
            if (operationId.equals(new UUID(0, 0)) || file < 0) {
                throw new IllegalArgumentException("Invalid DATA file quarantine");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.DEVICE_STATE;
        }

        @Override
        public long expectedGeneration() {
            return 0;
        }

        @Override
        public ByteBuffer body() {
            return ByteBuffer.allocate(16).putInt(1).putInt(1).putInt(file).putInt(0).flip();
        }
    }

    /** Arena-wide writer lifecycle; closing a session never clears a file quarantine. */
    record DataSession(UUID operationId, Generation operationGeneration, long expectedGeneration,
                       boolean open) implements Command {
        DataSession {
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(operationGeneration, "operationGeneration");
            if (operationId.equals(new UUID(0, 0)) || expectedGeneration < 0
                    || operationGeneration.value() <= 0
                    || (open ? expectedGeneration != operationGeneration.value() - 1
                            : expectedGeneration != operationGeneration.value())) {
                throw new IllegalArgumentException("Invalid DATA writer session");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.DEVICE_STATE;
        }

        @Override
        public ByteBuffer body() {
            return ByteBuffer.allocate(16).putInt(1).putInt(open ? 2 : 3)
                    .putLong(operationGeneration.value()).flip();
        }
    }

    record Allocation(SegmentControlFormat.Type type, UUID operationId, Generation operationGeneration,
                      long expectedGeneration, int file, int ownerShard, Generation ownerGeneration,
                      long allocationId, long offset, int blocks, long streamFirstSequence) implements Command {
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

        public ByteBuffer body() {
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

    record DurableBatch(long firstSequence, List<Command> commands) {
        DurableBatch {
            commands = List.copyOf(commands);
        }
    }

    private record Slot(int file, long index) { }

    private record Stream(int shard, long generation) { }

    record SingleStream(Generation generation, long firstSequence) { }

    record PoolRequest(int shard, Generation stream, long firstSequence, int blocks) {
        PoolRequest {
            Objects.requireNonNull(stream, "stream");
            if (shard < 0 || firstSequence < 0 || firstSequence == Long.MAX_VALUE || blocks < 1) {
                throw new IllegalArgumentException("Invalid pool request");
            }
        }
    }

    private record Grant(Allocation command, long generation) { }

    private record AppliedOperation(Command command, Status status, long sequence) { }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final long maxLogBytes;
    private final SegmentFileIo io;
    private FileChannel channel;
    private final FileChannel originalChannel;
    private final SegmentDataFileLock writerLock;
    private SegmentControlRotation.Root rotationRoot;
    private Thread rotating;
    private Generation physicalGeneration = LOG_GENERATION;
    private final Map<Slot, Grant> allocations = new HashMap<>();
    private final Map<Long, Allocation> allocationIds = new HashMap<>();
    private final Map<Stream, Long> streamStarts = new HashMap<>();
    private final Map<UUID, AppliedOperation> appliedOperations = new HashMap<>();
    private final boolean[] dataBlocked;
    private final boolean[] durableQuarantine;
    private final boolean[] quarantineReserved;
    private long sessionEpoch;
    private boolean sessionOpen;
    private boolean sessionCloseReserved;
    private IsolatedSegmentFiles sessionOwner;
    private long nextOffset = FIRST_RECORD;
    private long durableThrough = -1;
    private long appliedThrough = -1;
    private long appliedOffset = FIRST_RECORD;
    private long checkpointGeneration;
    private SegmentControlCheckpoint.Reference publishedCheckpoint;
    private boolean checkpointing;
    private DurableBatch pending;
    private boolean writing;
    private boolean failed;
    private boolean closed;

    private SegmentArenaControlLog(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                   long maxLogBytes, SegmentFileIo io,
                                   FileChannel channel, SegmentDataFileLock writerLock) {
        this.config = config;
        this.identity = identity;
        this.maxLogBytes = maxLogBytes;
        this.io = io;
        this.channel = channel;
        this.originalChannel = channel;
        this.writerLock = writerLock;
        dataBlocked = new boolean[config.dataFiles()];
        durableQuarantine = new boolean[config.dataFiles()];
        quarantineReserved = new boolean[config.dataFiles()];
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
        SegmentDataFileLock writerLock = SegmentDataFileLock.openControl(
                config.directory().resolve("arena-control.lock"));
        FileChannel original = null;
        SegmentArenaControlLog result = null;
        try {
            original = FileChannel.open(config.directory().resolve("arena-control"),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            result = new SegmentArenaControlLog(config, identity, maxLogBytes, io, original, writerLock);
            result.validateStorageFiles(identity);
            result.recover(identity);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            if (result != null && result.channel != original) {
                try {
                    result.channel.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (original != null) {
                try {
                    original.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                writerLock.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** One physical force covers the whole batch; no result is applied before that force returns. */
    DurableBatch appendDurably(List<? extends Command> commands) throws IOException {
        List<Command> batch = List.copyOf(commands);
        List<ByteBuffer> encoded;
        long sequence;
        long start;
        synchronized (this) {
            requireControlWritable();
            if (writing || pending != null || batch.isEmpty() || batch.size() > config.maxPendingEntries()) {
                throw new IllegalArgumentException("Invalid or unresolved control batch");
            }
            encoded = new ArrayList<>(batch.size());
            long bytes = 0;
            sequence = durableThrough + 1;
            for (Command command : batch) {
                validateCommand(command);
                ByteBuffer record = SegmentControlFormat.encode(command.type(), physicalGeneration, sequence++,
                        command.operationId(), command.operationGeneration(), command.expectedGeneration(),
                        command.body());
                bytes = Math.addExact(bytes, record.remaining());
                encoded.add(record);
            }
            long reserved = sessionCloseReserved && batch.stream().noneMatch(command ->
                    command instanceof DataSession session && !session.open()) ? QUARANTINE_RECORD_BYTES : 0;
            for (int file = 0; file < quarantineReserved.length; file++) {
                final int target = file;
                if (quarantineReserved[file] && batch.stream().noneMatch(command ->
                        command instanceof FileQuarantine quarantine && quarantine.file() == target)) {
                    reserved += QUARANTINE_RECORD_BYTES;
                }
            }
            if (rotating == null) {
                reserved += rotationReserveBytes();
            }
            if (bytes > maxLogBytes - nextOffset - reserved) {
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
        requireControlWritable();
        if (pending != batch || batch.firstSequence() != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable batch can be applied");
        }
        List<Result> results = new ArrayList<>(batch.commands().size());
        try {
            for (Command command : batch.commands()) {
                long sequence = ++appliedThrough;
                Status status = apply(command);
                appliedOffset += recordBytes(command);
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

    List<Result> submitBatch(List<? extends Command> commands) throws IOException {
        return applyDurable(appendDurably(commands));
    }

    /** Freeze only applied facts. A later durable/unapplied batch belongs to the retained suffix. */
    SegmentControlCheckpoint.Reference checkpoint() throws IOException {
        return checkpoint(new SegmentFileIo());
    }

    SegmentControlCheckpoint.Reference checkpoint(SegmentFileIo checkpointIo) throws IOException {
        Objects.requireNonNull(checkpointIo, "checkpointIo");
        SegmentControlCheckpoint.Snapshot snapshot;
        long generation;
        int slot;
        synchronized (this) {
            requireControlWritable();
            if (checkpointing || checkpointGeneration == Long.MAX_VALUE) {
                throw new IOException("Arena checkpoint is active or its generation is exhausted");
            }
            snapshot = new SegmentControlCheckpoint.Snapshot(appliedThrough, appliedOffset,
                    appliedOperations.values().stream().map(operation -> new SegmentControlCheckpoint.Operation(
                            operation.sequence(), operation.status().ordinal() + 1,
                            SegmentControlFormat.encode(operation.command().type(), LOG_GENERATION,
                                    operation.sequence(), operation.command().operationId(),
                                    operation.command().operationGeneration(), operation.command().expectedGeneration(),
                                    operation.command().body()))).toList());
            generation = checkpointGeneration + 1;
            slot = publishedCheckpoint == null ? 0 : 1 - publishedCheckpoint.slot();
            checkpointing = true;
        }
        boolean committed = false;
        try {
            snapshot = new SegmentControlCheckpoint.Snapshot(snapshot.through(), snapshot.logOffset(),
                    snapshot.operations().stream().sorted(Comparator.comparingLong(
                            SegmentControlCheckpoint.Operation::sequence)).toList());
            SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.reference(
                    generation, slot, snapshot, maxLogBytes);
            SegmentControlCheckpoint.write(config, identity, SegmentControlFormat.Owner.ARENA,
                    reference, snapshot, checkpointIo);
            if (!snapshot.equals(SegmentControlCheckpoint.read(
                    config, identity, SegmentControlFormat.Owner.ARENA, reference, maxLogBytes))) {
                throw new IOException("Arena checkpoint verification differs from its frozen state");
            }
            Result result = submitBatch(List.of(new CheckpointCommit(UUID.randomUUID(), reference))).get(0);
            if (result.status() != Status.APPLIED_DURABLE) {
                throw new IOException("Arena checkpoint was not committed");
            }
            committed = true;
            if (rotationRoot == null) {
                SegmentControlCheckpoint.publish(config, SegmentControlFormat.Owner.ARENA, reference, checkpointIo);
            } else {
                SegmentControlRotation.publish(config, SegmentControlFormat.Owner.ARENA, reference,
                        rotationRoot, checkpointIo);
            }
            if (!reference.equals(SegmentControlCheckpoint.publication(
                    config, SegmentControlFormat.Owner.ARENA, slot))) {
                throw new IOException("Arena checkpoint publication verification failed");
            }
            synchronized (this) {
                publishedCheckpoint = reference;
            }
            return reference;
        } catch (IOException | RuntimeException | Error failure) {
            if (committed) {
                synchronized (this) {
                    failed = true;
                }
            }
            throw failure;
        } finally {
            synchronized (this) {
                checkpointing = false;
            }
        }
    }

    synchronized Optional<SegmentControlCheckpoint.Reference> publishedCheckpoint() {
        return Optional.ofNullable(publishedCheckpoint);
    }

    /** Offline maintenance: two cuts differ only by the first checkpoint's own commit. */
    SegmentControlRotation.Root rotate() throws IOException {
        return rotate(new SegmentFileIo());
    }

    SegmentControlRotation.Root rotate(SegmentFileIo rotationIo) throws IOException {
        Objects.requireNonNull(rotationIo, "rotationIo");
        synchronized (this) {
            requireControlWritable();
            if (rotating != null || checkpointing || writing || pending != null || sessionOwner != null
                    || sessionCloseReserved || checkpointGeneration > Long.MAX_VALUE - 2
                    || physicalGeneration.value() == Long.MAX_VALUE || appliedThrough > Long.MAX_VALUE - 3
                    || rotationReserveBytes() == 0
                    || nextOffset > maxLogBytes - 416) {
                throw new IOException("Arena rotation requires a quiescent writer and two commit slots");
            }
            for (boolean reserved : quarantineReserved) {
                if (reserved) {
                    throw new IOException("Arena quarantine notification is still in flight");
                }
            }
            rotating = Thread.currentThread();
        }
        FileChannel candidate = null;
        Path target = null;
        boolean created = false;
        boolean publicationAttempted = false;
        try {
            SegmentControlCheckpoint.Reference first = checkpoint(rotationIo);
            SegmentControlCheckpoint.Reference second = checkpoint(rotationIo);
            SegmentControlRotation.Root root = new SegmentControlRotation.Root(physicalGeneration.value() + 1,
                    first, checkpointId(first), second, checkpointId(second));
            target = SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, root.generation());
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                // Both fresh publications recover the current journal. Delete only a proven older/orphan file.
                try (FileChannel orphan = FileChannel.open(target, StandardOpenOption.READ,
                        LinkOption.NOFOLLOW_LINKS)) {
                    ByteBuffer header = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(orphan, header, 0);
                    identity.validate(header, config, -4);
                    SegmentControlRotation.Root old = SegmentControlRotation.readHeader(orphan,
                            SegmentControlFormat.Owner.ARENA);
                    if (old.generation() == physicalGeneration.value()
                            || old.generation() > root.generation() || !knownRotation(old)) {
                        throw new IOException("Arena rotation target has unproven authority");
                    }
                }
                Files.delete(target);
                IsolatedSegmentFiles.syncDirectory(config.directory());
            }
            candidate = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            created = true;
            rotationIo.writeFully(candidate, identity.encode(config, -4), 0);
            rotationIo.writeFully(candidate, SegmentControlRotation.header(SegmentControlFormat.Owner.ARENA, root),
                    4096);
            rotationIo.force(candidate, true);
            IsolatedSegmentFiles.syncDirectory(config.directory());
            if (!root.equals(SegmentControlRotation.readHeader(candidate, SegmentControlFormat.Owner.ARENA))) {
                throw new IOException("Arena rotation candidate verification failed");
            }
            publicationAttempted = true;
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.ARENA, first, root, rotationIo);
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.ARENA, second, root, rotationIo);
            // The independent lock remains held. This marker prevents empty-log and old-decoder fallback.
            rotationIo.writeFully(originalChannel,
                    SegmentControlRotation.header(SegmentControlFormat.Owner.ARENA, root),
                    4096);
            originalChannel.truncate(SegmentControlRotation.FIRST_RECORD);
            rotationIo.force(originalChannel, true);
            FileChannel previous = channel;
            long previousGeneration = physicalGeneration.value();
            synchronized (this) {
                channel = candidate;
                candidate = null;
                rotationRoot = root;
                physicalGeneration = new Generation(root.generation());
                nextOffset = SegmentControlRotation.FIRST_RECORD;
                appliedOffset = nextOffset;
            }
            if (previous != originalChannel) {
                previous.close();
                Files.delete(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                        previousGeneration));
                IsolatedSegmentFiles.syncDirectory(config.directory());
            }
            return root;
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
            }
            if (created && !publicationAttempted) {
                try {
                    candidate.close();
                    candidate = null;
                    Files.delete(target);
                    IsolatedSegmentFiles.syncDirectory(config.directory());
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        } finally {
            try {
                if (candidate != null) {
                    candidate.close();
                }
            } finally {
                synchronized (this) {
                    rotating = null;
                }
            }
        }
    }

    private synchronized UUID checkpointId(SegmentControlCheckpoint.Reference reference) throws IOException {
        for (AppliedOperation operation : appliedOperations.values()) {
            if (operation.command() instanceof CheckpointCommit commit && commit.reference().equals(reference)
                    && operation.status() == Status.APPLIED_DURABLE) {
                return commit.operationId();
            }
        }
        throw new IOException("Arena checkpoint commit is missing");
    }

    private boolean knownRotation(SegmentControlRotation.Root root) {
        AppliedOperation first = appliedOperations.get(root.firstId());
        AppliedOperation second = appliedOperations.get(root.secondId());
        return first != null && second != null && first.status() == Status.APPLIED_DURABLE
                && second.status() == Status.APPLIED_DURABLE
                && first.command().equals(new CheckpointCommit(root.firstId(), root.first()))
                && second.command().equals(new CheckpointCommit(root.secondId(), root.second()));
    }

    private long rotationReserveBytes() {
        return maxLogBytes >= SegmentControlRotation.FIRST_RECORD + 416 + 192 ? 416 : 0;
    }

    private static int recordBytes(Command command) {
        return (144 + command.body().remaining() + 7) & ~7;
    }

    /** Called before exposing a new writer, under its existing directory exclusion. */
    void beginDataSession(IsolatedSegmentFiles writer) throws IOException {
        DataSession command;
        synchronized (this) {
            requireQuiescentRecovery(writer.config(), writer.identity());
            if (sessionOwner != null || sessionEpoch == Long.MAX_VALUE
                    || (sessionEpoch == 0 && !allocations.isEmpty())) {
                throw new IOException("DATA session cannot adopt an active or legacy allocated writer");
            }
            command = new DataSession(UUID.randomUUID(), new Generation(sessionEpoch + 1), sessionEpoch, true);
            sessionOwner = writer;
            sessionCloseReserved = true;
        }
        try {
            List<Result> results = submitBatch(List.of(command));
            if (results.get(0).status() != Status.APPLIED_DURABLE) {
                throw new IOException("DATA writer session was not opened");
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                sessionOwner = null;
                sessionCloseReserved = false;
            }
            throw failure;
        }
    }

    /** The caller has closed its DATA admission and drained I/O before attempting this witness. */
    void finishDataSession(IsolatedSegmentFiles writer, boolean healthy) throws IOException {
        try {
            DataSession command;
            synchronized (this) {
                if (sessionOwner != writer || !sessionOpen) {
                    throw new IOException("DATA writer does not own an open session");
                }
                if (!healthy) {
                    throw new IOException("Failed DATA session must remain unclosed");
                }
                requireQuiescentControl(writer.config(), writer.identity());
                command = new DataSession(UUID.randomUUID(), new Generation(sessionEpoch), sessionEpoch, false);
            }
            List<Result> results = submitBatch(List.of(command));
            if (results.get(0).status() != Status.APPLIED_DURABLE) {
                throw new IOException("DATA writer session was not closed");
            }
        } finally {
            synchronized (this) {
                if (sessionOwner == writer) {
                    sessionOwner = null;
                    sessionCloseReserved = false;
                }
            }
        }
    }

    synchronized long dataSessionEpoch() {
        return sessionEpoch;
    }

    synchronized boolean hasUnclosedDataSession() {
        return sessionOpen;
    }

    synchronized boolean permitsDataWriter(IsolatedSegmentFiles writer) {
        return sessionEpoch == 0 && sessionOwner == null || sessionOpen && sessionOwner == writer;
    }

    /** Immediate admission gate; it is not a durable failure witness until DEVICE_STATE is applied. */
    synchronized boolean blockDataFile(int file) {
        if (file < 0 || file >= dataBlocked.length) {
            throw new IllegalArgumentException("Invalid DATA file");
        }
        boolean firstFailure = !dataBlocked[file];
        dataBlocked[file] = true;
        return firstFailure;
    }

    synchronized boolean dataFileAvailable(int file) {
        return !closed && !failed && file >= 0 && file < dataBlocked.length && !dataBlocked[file]
                && (!sessionOpen || sessionOwner != null);
    }

    synchronized boolean hasDurableQuarantine(int file) {
        if (file < 0 || file >= durableQuarantine.length) {
            throw new IllegalArgumentException("Invalid DATA file");
        }
        return durableQuarantine[file];
    }

    int dataFiles() {
        return config.dataFiles();
    }

    /** Reserve before DATA I/O; normal control batches cannot consume these bytes. */
    synchronized boolean reserveQuarantineRecord(int file) {
        if (!dataFileAvailable(file) || quarantineReserved[file]) {
            return false;
        }
        long reserved = QUARANTINE_RECORD_BYTES + (sessionCloseReserved ? QUARANTINE_RECORD_BYTES : 0);
        for (boolean held : quarantineReserved) {
            if (held) {
                reserved += QUARANTINE_RECORD_BYTES;
            }
        }
        if (reserved + rotationReserveBytes() > maxLogBytes - nextOffset) {
            return false;
        }
        quarantineReserved[file] = true;
        return true;
    }

    synchronized void releaseQuarantineRecord(int file) {
        if (file < 0 || file >= quarantineReserved.length || !quarantineReserved[file]) {
            throw new IllegalStateException("No DATA quarantine space reservation");
        }
        quarantineReserved[file] = false;
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

    int maxPoolRequests() {
        return Math.min(config.logicalShards(), config.maxPendingEntries());
    }

    void validatePoolRequest(PoolRequest request) {
        if (request.shard() >= config.logicalShards()
                || request.blocks() > (config.bytesPerFile() - FIRST_RECORD) / BLOCK_BYTES) {
            throw new IllegalArgumentException("Pool request exceeds the fixed Arena configuration");
        }
    }

    /** Plan fresh ranges only; these requests gain authority solely after durable ordered apply. */
    synchronized List<Optional<Allocation>> planPools(List<PoolRequest> requests) throws IOException {
        requireWritable();
        if (writing || pending != null || requests.isEmpty() || requests.size() > maxPoolRequests()) {
            throw new IOException("Arena cannot plan another pool batch");
        }
        long[] offsets = new long[config.dataFiles()];
        for (int file = 0; file < offsets.length; file++) {
            offsets[file] = grantedHighWatermark(file);
        }
        long id = allocationIds.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
        List<Optional<Allocation>> planned = new ArrayList<>(requests.size());
        for (PoolRequest request : requests) {
            validatePoolRequest(request);
            int file = request.shard() % config.dataFiles();
            if (dataBlocked[file] || request.blocks() > (config.bytesPerFile() - offsets[file]) / BLOCK_BYTES) {
                planned.add(Optional.empty());
                continue;
            }
            if (id == Long.MAX_VALUE) {
                throw new IOException("Arena allocation identity exhausted");
            }
            Allocation allocation = new Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    UUID.randomUUID(), new Generation(1), 0, file, request.shard(), request.stream(),
                    ++id, offsets[file], request.blocks(), request.firstSequence());
            planned.add(Optional.of(allocation));
            offsets[file] += request.blocks() * BLOCK_BYTES;
        }
        return List.copyOf(planned);
    }

    /** Earliest whole-batch applied pool at/after this stream's cursor; does not reserve or refill space. */
    synchronized Optional<SegmentBatchFormat.Address> appendAddress(int file, long minimum, int bytes,
                                                                   int shard, Generation stream, long sequence) {
        if (!dataFileAvailable(file) || bytes <= 0) {
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

    /** Clean restart continues the same stream; it cannot choose among unretired generations. */
    synchronized Optional<SingleStream> restartStreamForShard(int shard) throws IOException {
        requireWritable();
        if (shard < 0 || shard >= config.logicalShards()) {
            throw new IllegalArgumentException("Invalid restart shard");
        }
        if (streamStarts.keySet().stream().filter(stream -> stream.shard() == shard).count() > 1) {
            throw new IOException("Restart shard has multiple unretired stream generations");
        }
        return singleStreamForShard(shard);
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
        requireQuiescentControl(otherConfig, otherIdentity);
        if (sessionOpen || sessionOwner != null) {
            throw new IOException("Unclosed DATA writer session prevents physical reopen");
        }
    }

    private void requireQuiescentControl(IsolatedSegmentConfig otherConfig,
                                         SegmentFileHeader otherIdentity) throws IOException {
        if (!matchesScope(otherConfig, otherIdentity) || writing || pending != null || rotating != null) {
            throw new IOException("Arena authority is unavailable or has unresolved control I/O");
        }
        for (boolean blocked : dataBlocked) {
            if (blocked) {
                throw new IOException("Unresolved DATA file quarantine prevents writable physical reopen");
            }
        }
        for (boolean reserved : quarantineReserved) {
            if (reserved) {
                throw new IOException("DATA failure notification is still in flight");
            }
        }
    }

    synchronized long durableThrough() {
        return durableThrough;
    }

    synchronized long appliedThrough() {
        return appliedThrough;
    }

    private void selectRotation() throws IOException {
        for (int slot = 0; slot < 2; slot++) {
            SegmentControlRotation.Root root;
            try {
                SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.ARENA, slot);
                root = SegmentControlRotation.publication(config, SegmentControlFormat.Owner.ARENA, slot);
            } catch (IOException invalidPublication) {
                // An invalid page is optional. A selected generation's journal is never optional.
                continue;
            }
            if (root != null && (rotationRoot == null || root.generation() > rotationRoot.generation())) {
                rotationRoot = root;
            } else if (root != null && rotationRoot.generation() == root.generation()
                    && !root.equals(rotationRoot)) {
                throw new IOException("Conflicting Arena rotation roots");
            }
        }
        if (rotationRoot != null) {
            channel = FileChannel.open(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                    rotationRoot.generation()),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            ByteBuffer header = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(channel, header, 0);
            identity.validate(header, config, -4);
            if (!rotationRoot.equals(SegmentControlRotation.readHeader(channel, SegmentControlFormat.Owner.ARENA))) {
                throw new IOException("Arena rotation publication differs from its journal");
            }
            physicalGeneration = new Generation(rotationRoot.generation());
        }
    }

    private void recoverRotated() throws IOException {
        long size = channel.size();
        if (size < SegmentControlRotation.FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid rotated Arena control log size");
        }
        List<SegmentControlCheckpoint.Reference> candidates = new ArrayList<>();
        candidates.add(rotationRoot.first());
        candidates.add(rotationRoot.second());
        SegmentControlCheckpoint.Reference[] publications = new SegmentControlCheckpoint.Reference[2];
        for (int slot = 0; slot < 2; slot++) {
            boolean valid = false;
            try {
                SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.publication(
                        config, SegmentControlFormat.Owner.ARENA, slot);
                if (rotationRoot.equals(SegmentControlRotation.publication(config,
                        SegmentControlFormat.Owner.ARENA, slot))) {
                    publications[slot] = reference;
                    valid = true;
                }
            } catch (IOException invalidPublication) {
                // Recovery still has to validate one complete publication and every current suffix record.
            }
            if (valid) {
                SegmentControlCheckpoint.syncPublication(config, slot);
            }
        }
        long sequence = rotationRoot.through() + 1;
        long generation = rotationRoot.second().generation();
        HashSet<UUID> operations = new HashSet<>();
        for (long offset = SegmentControlRotation.FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            Command command = readCommand(offset, length, sequence);
            validateCommand(command);
            if (operations.add(command.operationId()) && command instanceof CheckpointCommit commit
                    && commit.expectedGeneration() == generation) {
                SegmentControlCheckpoint.Reference reference = commit.reference();
                if (reference.through() < rotationRoot.through()
                        || reference.logOffset() < SegmentControlRotation.FIRST_RECORD
                        || reference.logOffset() > offset) {
                    throw new IOException("Invalid rotated Arena checkpoint cut");
                }
                recordLength(reference.logOffset(), size, reference.through() + 1);
                generation = reference.generation();
                if (reference.equals(publications[reference.slot()])) {
                    candidates.add(reference);
                }
            }
            offset += length;
            sequence++;
        }
        channel.force(true);
        candidates.sort(Comparator.comparingLong(SegmentControlCheckpoint.Reference::generation).reversed());
        boolean restored = false;
        for (SegmentControlCheckpoint.Reference reference : candidates) {
            try {
                restoreSnapshot(reference);
                if (reference.equals(rotationRoot.first()) || reference.equals(rotationRoot.second())) {
                    if (reference.equals(rotationRoot.first())) {
                        replayRootCommit(rotationRoot.firstId(), rotationRoot.first());
                    }
                    replayRootCommit(rotationRoot.secondId(), rotationRoot.second());
                    appliedOffset = SegmentControlRotation.FIRST_RECORD;
                }
                restored = true;
                break;
            } catch (IOException | IllegalArgumentException invalidImage) {
                resetRecoveredState();
            }
        }
        if (!restored) {
            throw new IOException("No recoverable Arena checkpoint after prefix reclamation");
        }
        sequence = appliedThrough + 1;
        for (long offset = appliedOffset; offset < size;) {
            int length = recordLength(offset, size, sequence);
            Command command = readCommand(offset, length, sequence);
            durableThrough = sequence;
            appliedThrough = sequence++;
            apply(command);
            offset += length;
            appliedOffset = offset;
        }
        nextOffset = size;
    }

    private void replayRootCommit(UUID operationId, SegmentControlCheckpoint.Reference reference) throws IOException {
        durableThrough = ++appliedThrough;
        if (apply(new CheckpointCommit(operationId, reference)) != Status.APPLIED_DURABLE) {
            throw new IOException("Arena rotation root commit cannot be replayed");
        }
    }

    private void restoreSnapshot(SegmentControlCheckpoint.Reference reference) throws IOException {
        SegmentControlCheckpoint.Snapshot snapshot = SegmentControlCheckpoint.read(
                config, identity, SegmentControlFormat.Owner.ARENA, reference, maxLogBytes);
        resetRecoveredState();
        for (SegmentControlCheckpoint.Operation operation : snapshot.operations()) {
            Command command = decodeCommand(operation.record(), operation.sequence());
            validateCommand(command);
            if (appliedOperations.containsKey(command.operationId())) {
                throw new IOException("Repeated operation in Arena checkpoint");
            }
            appliedThrough = operation.sequence();
            if (apply(command).ordinal() + 1 != operation.status()) {
                throw new IOException("Arena checkpoint conditional result mismatch");
            }
        }
        if (checkpointGeneration != reference.generation() - 1) {
            throw new IOException("Arena checkpoint does not retain its committed predecessor");
        }
        appliedThrough = snapshot.through();
        durableThrough = snapshot.through();
        appliedOffset = snapshot.logOffset();
        publishedCheckpoint = reference;
    }

    private void recover(SegmentFileHeader identity) throws IOException {
        ByteBuffer originalIdentity = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(originalChannel, originalIdentity, 0);
        identity.validate(originalIdentity, config, -4);
        selectRotation();
        if (rotationRoot != null) {
            recoverRotated();
            return;
        }
        long size = channel.size();
        if (size < FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid Arena control log size");
        }
        ByteBuffer header = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(channel, header, 0);
        identity.validate(header, config, -4);
        long sequence = 0;
        long committedGeneration = 0;
        HashSet<UUID> operations = new HashSet<>();
        List<SegmentControlCheckpoint.Reference> checkpoints = new ArrayList<>(2);
        SegmentControlCheckpoint.Reference[] publications = new SegmentControlCheckpoint.Reference[2];
        for (int slot = 0; slot < publications.length; slot++) {
            try {
                publications[slot] = SegmentControlCheckpoint.publication(
                        config, SegmentControlFormat.Owner.ARENA, slot);
            } catch (IOException incompletePublication) {
                // The immutable identity page remains required; an incomplete publication uses another slot or log.
            }
        }
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            Command command = readCommand(offset, length, sequence);
            if (operations.add(command.operationId()) && command instanceof CheckpointCommit checkpoint
                    && checkpoint.expectedGeneration() == committedGeneration
                    && checkpoint.reference().through() < sequence) {
                SegmentControlCheckpoint.Reference reference = checkpoint.reference();
                // Its captured cut must end at a real boundary of this exact retained log.
                recordLength(reference.logOffset(), size, reference.through() + 1);
                if (reference.logOffset() > offset) {
                    throw new IOException("Arena checkpoint depends on a later control suffix");
                }
                committedGeneration = reference.generation();
                if (reference.equals(publications[reference.slot()])) {
                    checkpoints.add(reference);
                }
            }
            offset += length;
            sequence++;
        }
        // Complete records can include a lost response. Establish durability before exposing replayed grants.
        channel.force(true);
        recoverCheckpoint(checkpoints);
        sequence = appliedThrough + 1;
        for (long offset = appliedOffset; offset < size;) {
            int length = recordLength(offset, size, sequence);
            Command command = readCommand(offset, length, sequence);
            durableThrough = sequence;
            appliedThrough = sequence;
            apply(command);
            offset += length;
            appliedOffset = offset;
            sequence++;
        }
        nextOffset = size;
    }

    private void recoverCheckpoint(List<SegmentControlCheckpoint.Reference> checkpoints) throws IOException {
        for (int index = checkpoints.size() - 1; index >= 0; index--) {
            SegmentControlCheckpoint.Reference reference = checkpoints.get(index);
            try {
                if (!reference.equals(SegmentControlCheckpoint.publication(
                        config, SegmentControlFormat.Owner.ARENA, reference.slot()))) {
                    continue;
                }
                SegmentControlCheckpoint.Snapshot snapshot = SegmentControlCheckpoint.read(
                        config, identity, SegmentControlFormat.Owner.ARENA, reference, maxLogBytes);
                resetRecoveredState();
                for (SegmentControlCheckpoint.Operation operation : snapshot.operations()) {
                    Command command = decodeCommand(operation.record(), operation.sequence());
                    if (appliedOperations.containsKey(command.operationId())) {
                        throw new IOException("Repeated operation in Arena checkpoint");
                    }
                    appliedThrough = operation.sequence();
                    if (apply(command).ordinal() + 1 != operation.status()) {
                        throw new IOException("Arena checkpoint conditional result mismatch");
                    }
                }
                appliedThrough = snapshot.through();
                durableThrough = snapshot.through();
                appliedOffset = snapshot.logOffset();
                publishedCheckpoint = reference;
            } catch (IOException | IllegalArgumentException invalidCheckpoint) {
                resetRecoveredState();
                continue;
            }
            // A complete but formerly unsynced publication must not be exposed before recovery sync.
            SegmentControlCheckpoint.syncPublication(config, reference.slot());
            return;
        }
        // All original control bytes remain mandatory and were verified above; full replay is the final fallback.
        resetRecoveredState();
    }

    private void resetRecoveredState() {
        allocations.clear();
        allocationIds.clear();
        streamStarts.clear();
        appliedOperations.clear();
        Arrays.fill(dataBlocked, false);
        Arrays.fill(durableQuarantine, false);
        sessionEpoch = 0;
        sessionOpen = false;
        checkpointGeneration = 0;
        publishedCheckpoint = null;
        appliedThrough = -1;
        durableThrough = -1;
        appliedOffset = FIRST_RECORD;
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
            boolean superblock = fileId == -1 || fileId == -2;
            if ((superblock && (input.size() < 4096 || input.size() > 12288))
                    || (!superblock && expectedSize < 0 && input.size() < 4096)
                    || (!superblock && expectedSize >= 0 && input.size() != expectedSize)) {
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
                physicalGeneration, sequence);
        if (length > size - offset) {
            throw new IOException("Incomplete Arena control record; writable recovery is unsafe");
        }
        return length;
    }

    private Command readCommand(long offset, int length, long sequence) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        SegmentFileIo.readFully(channel, bytes, offset);
        SegmentControlParser.Command parsed = SegmentControlParser.parse(bytes, SegmentControlFormat.Owner.ARENA,
                physicalGeneration, sequence);
        return decodeCommand(SegmentControlFormat.encode(parsed.type(), LOG_GENERATION, sequence,
                parsed.operationId(), parsed.operationGeneration(), parsed.expectedGeneration(), parsed.body()),
                sequence);
    }

    Command decodeCommand(ByteBuffer bytes, long sequence) throws IOException {
        SegmentControlParser.Command parsed = SegmentControlParser.parse(bytes, SegmentControlFormat.Owner.ARENA,
                LOG_GENERATION, sequence);
        if (parsed.type() == SegmentControlFormat.Type.CHECKPOINT_COMMIT) {
            SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.decodeReference(parsed.body());
            if (parsed.operationGeneration().value() != reference.generation()
                    || parsed.expectedGeneration() != reference.generation() - 1
                    || reference.payloadBytes() - maxLogBytes > maxLogBytes || reference.through() >= sequence) {
                throw new IOException("Invalid Arena checkpoint commit binding");
            }
            return new CheckpointCommit(parsed.operationId(), reference);
        }
        if (parsed.type() == SegmentControlFormat.Type.DEVICE_STATE) {
            ByteBuffer body = parsed.body();
            if (body.remaining() != 16 || body.getInt() != 1) {
                throw new IOException("Unsupported Arena DEVICE_STATE body");
            }
            int action = body.getInt();
            if (action == 2 || action == 3) {
                long epoch = body.getLong();
                try {
                    DataSession command = new DataSession(parsed.operationId(), parsed.operationGeneration(),
                            parsed.expectedGeneration(), action == 2);
                    if (epoch != command.operationGeneration().value()) {
                        throw new IllegalArgumentException("DATA session epoch differs from its header");
                    }
                    return command;
                } catch (IllegalArgumentException invalid) {
                    throw new IOException("Invalid Arena DATA session", invalid);
                }
            }
            if (action != 1 || parsed.expectedGeneration() != 0) {
                throw new IOException("Unsupported Arena DEVICE_STATE action");
            }
            int file = body.getInt();
            if (body.getInt() != 0) {
                throw new IOException("Unsupported Arena DEVICE_STATE extension");
            }
            try {
                FileQuarantine command = new FileQuarantine(parsed.operationId(), parsed.operationGeneration(), file);
                validateCommand(command);
                return command;
            } catch (IllegalArgumentException invalid) {
                throw new IOException("Invalid DATA file quarantine", invalid);
            }
        }
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

    private void validateCommand(Command command) {
        if (command instanceof Allocation allocation) {
            validateRange(allocation);
        } else if (command instanceof FileQuarantine quarantine && quarantine.file() >= config.dataFiles()) {
            throw new IllegalArgumentException("Invalid quarantined DATA file");
        }
    }

    private Status apply(Command command) {
        AppliedOperation prior = appliedOperations.get(command.operationId());
        if (prior != null) {
            if (!prior.command().equals(command)) {
                return Status.INCOMPATIBLE;
            }
            return prior.status() == Status.APPLIED_DURABLE ? Status.ALREADY_DURABLE : prior.status();
        }
        if (command instanceof CheckpointCommit checkpoint) {
            boolean allowed = checkpoint.expectedGeneration() == checkpointGeneration
                    && checkpoint.reference().through() < appliedThrough;
            Status status = allowed ? Status.APPLIED_DURABLE : Status.CONDITION_FAILED;
            if (allowed) {
                checkpointGeneration = checkpoint.reference().generation();
            }
            appliedOperations.put(command.operationId(), new AppliedOperation(command, status, appliedThrough));
            return status;
        }
        if (command instanceof FileQuarantine quarantine) {
            int file = quarantine.file();
            Status status = durableQuarantine[file] ? Status.ALREADY_DURABLE : Status.APPLIED_DURABLE;
            dataBlocked[file] = true;
            durableQuarantine[file] = true;
            appliedOperations.put(command.operationId(), new AppliedOperation(command, status, appliedThrough));
            return status;
        }
        if (command instanceof DataSession session) {
            boolean allowed = session.expectedGeneration() == sessionEpoch
                    && (session.open() ? !sessionOpen && (sessionEpoch != 0 || allocations.isEmpty()) : sessionOpen);
            for (boolean quarantined : durableQuarantine) {
                allowed &= !quarantined;
            }
            Status status = allowed ? Status.APPLIED_DURABLE : Status.CONDITION_FAILED;
            if (allowed) {
                sessionEpoch = session.operationGeneration().value();
                sessionOpen = session.open();
            }
            appliedOperations.put(command.operationId(), new AppliedOperation(command, status, appliedThrough));
            return status;
        }
        return applyAllocation((Allocation) command);
    }

    private Status applyAllocation(Allocation command) {
        if (durableQuarantine[command.file()] || allocationIds.containsKey(command.allocationId())) {
            appliedOperations.put(command.operationId(),
                    new AppliedOperation(command, Status.CONDITION_FAILED, appliedThrough));
            return Status.CONDITION_FAILED;
        }
        Stream stream = new Stream(command.ownerShard(), command.ownerGeneration().value());
        Long first = streamStarts.get(stream);
        if (first != null && first != command.streamFirstSequence()) {
            appliedOperations.put(command.operationId(),
                    new AppliedOperation(command, Status.CONDITION_FAILED, appliedThrough));
            return Status.CONDITION_FAILED;
        }
        for (int block = 0; block < command.blocks(); block++) {
            if (allocations.containsKey(slot(command.file(), command.offset() + block * BLOCK_BYTES))) {
                appliedOperations.put(command.operationId(),
                        new AppliedOperation(command, Status.CONDITION_FAILED, appliedThrough));
                return Status.CONDITION_FAILED;
            }
        }
        Grant grant = new Grant(command, command.expectedGeneration() + 1);
        for (int block = 0; block < command.blocks(); block++) {
            allocations.put(slot(command.file(), command.offset() + block * BLOCK_BYTES), grant);
        }
        allocationIds.put(command.allocationId(), command);
        streamStarts.putIfAbsent(stream, command.streamFirstSequence());
        appliedOperations.put(command.operationId(),
                new AppliedOperation(command, Status.APPLIED_DURABLE, appliedThrough));
        return Status.APPLIED_DURABLE;
    }

    private Slot slot(int file, long offset) {
        return new Slot(file, (offset - FIRST_RECORD) / BLOCK_BYTES);
    }

    private void requireControlWritable() throws IOException {
        requireWritable();
        if (rotating != null && rotating != Thread.currentThread()) {
            throw new IOException("Arena control rotation is in flight");
        }
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
        if (writing || checkpointing || rotating != null) {
            throw new IOException("Arena control I/O is still in flight");
        }
        if (sessionOwner != null) {
            throw new IOException("DATA writer still owns its session");
        }
        for (boolean reserved : quarantineReserved) {
            if (reserved) {
                throw new IOException("DATA failure notification is still in flight");
            }
        }
        closed = true;
        IOException first = null;
        try {
            if (channel != originalChannel) {
                channel.close();
            }
        } catch (IOException failure) {
            first = failure;
        }
        try {
            originalChannel.close();
        } catch (IOException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        try {
            writerLock.close();
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

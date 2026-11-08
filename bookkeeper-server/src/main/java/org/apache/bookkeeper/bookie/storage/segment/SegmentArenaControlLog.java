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
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction;

/** Isolated single-Arena allocation and MOVE authority; no production Bookie access. */
final class SegmentArenaControlLog implements AutoCloseable {
    static final long BLOCK_BYTES = 1024 * 1024;
    private static final long FIRST_RECORD = 4096;
    private static final Generation LOG_GENERATION = new Generation(1);
    private static final int QUARANTINE_RECORD_BYTES = 128 + 16 + 16;

    sealed interface Command permits Allocation, Free, FileQuarantine, DataSession, ResolveNeverAllocatedSession,
            ResolveGrantFreeSession, ResolveClosedReconstruction, CheckpointCommit, SegmentArenaMove {
        SegmentControlFormat.Type type();
        UUID operationId();
        Generation operationGeneration();
        long expectedGeneration();
        ByteBuffer body();
    }

    record ReconstructionBinding(long ledger, long arenaCut, long bookieCut, long admissionBirth, long readyBirth,
                                 ProfileClosedReconstruction.Image image, String dependencies, String admissionDigest,
                                 String readyDigest) {
        ReconstructionBinding {
            if (ledger < 0 || arenaCut < 0 || bookieCut < 0 || admissionBirth <= 0 || readyBirth <= admissionBirth
                    || !dependencies.matches("[0-9a-f]{64}") || !admissionDigest.matches("[0-9a-f]{64}")
                    || !readyDigest.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Invalid CLOSED reconstruction cut or native birth");
            }
            Objects.requireNonNull(image, "image");
        }
    }

    private record ResolveClosedReconstruction(UUID operationId, Generation operationGeneration,
                                               ReconstructionBinding binding, boolean reconstructTails)
            implements Command {
        private static final int BYTES = 208;
        private static final int LEGACY_ACTION = 6;
        private static final int RECONSTRUCT_TAILS_ACTION = 7;

        ResolveClosedReconstruction {
            if (operationId.equals(new UUID(0, 0)) || operationGeneration.value() <= 0) {
                throw new IllegalArgumentException("Invalid CLOSED reconstruction operation or epoch");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.DEVICE_STATE;
        }

        @Override
        public long expectedGeneration() {
            return operationGeneration.value();
        }

        @Override
        public ByteBuffer body() {
            var image = binding.image();
            var hex = HexFormat.of();
            return ByteBuffer.allocate(BYTES).putInt(2)
                    .putInt(reconstructTails ? RECONSTRUCT_TAILS_ACTION : LEGACY_ACTION).putLong(expectedGeneration())
                    .putLong(binding.ledger()).putLong(binding.arenaCut()).putLong(binding.bookieCut())
                    .putLong(binding.admissionBirth()).putLong(binding.readyBirth()).putLong(image.length())
                    .putLong(image.object().device()).putLong(image.object().inode())
                    .put(hex.parseHex(binding.dependencies())).put(hex.parseHex(binding.admissionDigest()))
                    .put(hex.parseHex(binding.readyDigest())).put(hex.parseHex(image.sha256())).flip();
        }

        private static ResolveClosedReconstruction decode(SegmentControlParser.Command parsed, ByteBuffer body,
                                                          boolean reconstructTails) {
            long epoch = body.getLong();
            if (epoch != parsed.expectedGeneration() || epoch != parsed.operationGeneration().value()) {
                throw new IllegalArgumentException("CLOSED reconstruction epoch differs from its framing");
            }
            long ledger = body.getLong();
            long arena = body.getLong();
            long bookie = body.getLong();
            long admitted = body.getLong();
            long ready = body.getLong();
            long bytes = body.getLong();
            var object = new FileIdentity(body.getLong(), body.getLong());
            String dependencies = readDigest(body);
            String admissionDigest = readDigest(body);
            String readyDigest = readDigest(body);
            return new ResolveClosedReconstruction(parsed.operationId(), parsed.operationGeneration(),
                    new ReconstructionBinding(ledger, arena, bookie, admitted, ready,
                            new ProfileClosedReconstruction.Image(object, bytes, readDigest(body)),
                            dependencies, admissionDigest, readyDigest), reconstructTails);
        }

        private static String readDigest(ByteBuffer input) {
            byte[] bytes = new byte[32];
            input.get(bytes);
            return HexFormat.of().formatHex(bytes);
        }
    }

    record PendingReconstruction(UUID operation, long epoch, ReconstructionBinding binding) { }

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

    enum TailSource { HEALTHY_CLOSE, RECONSTRUCTION }

    private record AllocationTail(long allocationId, long generation, long end, TailSource source) {
        AllocationTail(long allocationId, long generation, long end) {
            this(allocationId, generation, end, TailSource.HEALTHY_CLOSE);
        }
    }

    /** Arena-wide writer lifecycle; closing a session never clears a file quarantine. */
    record DataSession(UUID operationId, Generation operationGeneration, long expectedGeneration,
                       boolean open, List<AllocationTail> tails) implements Command {
        DataSession(UUID operationId, Generation operationGeneration, long expectedGeneration, boolean open) {
            this(operationId, operationGeneration, expectedGeneration, open, List.of());
        }

        DataSession {
            Objects.requireNonNull(operationId, "operationId");
            Objects.requireNonNull(operationGeneration, "operationGeneration");
            tails = List.copyOf(tails);
            if (open && !tails.isEmpty() || tails.size() > (65536 - 144 - 24) / 24
                    || operationId.equals(new UUID(0, 0)) || expectedGeneration < 0
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
            if (tails.isEmpty()) {
                return ByteBuffer.allocate(16).putInt(1).putInt(open ? 2 : 3)
                        .putLong(operationGeneration.value()).flip();
            }
            ByteBuffer body = ByteBuffer.allocate(24 + 24 * tails.size()).putInt(2).putInt(3)
                    .putLong(operationGeneration.value()).putInt(tails.size()).putInt(0);
            tails.forEach(tail -> body.putLong(tail.allocationId()).putLong(tail.generation()).putLong(tail.end()));
            return body.flip();
        }
    }

    private record ResolveNeverAllocatedSession(UUID operationId, Generation operationGeneration) implements Command {
        ResolveNeverAllocatedSession {
            if (operationId.equals(new UUID(0, 0)) || operationGeneration.value() <= 0) {
                throw new IllegalArgumentException("Invalid never-allocated DATA session");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.DEVICE_STATE;
        }

        @Override
        public long expectedGeneration() {
            return operationGeneration.value();
        }

        @Override
        public ByteBuffer body() {
            return ByteBuffer.allocate(16).putInt(1).putInt(4).putLong(operationGeneration.value()).flip();
        }
    }

    private record ResolveGrantFreeSession(UUID operationId, Generation operationGeneration) implements Command {
        ResolveGrantFreeSession {
            if (operationId.equals(new UUID(0, 0)) || operationGeneration.value() <= 0) {
                throw new IllegalArgumentException("Invalid grant-free DATA session");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.DEVICE_STATE;
        }

        @Override
        public long expectedGeneration() {
            return operationGeneration.value();
        }

        @Override
        public ByteBuffer body() {
            return ByteBuffer.allocate(16).putInt(1).putInt(5).putLong(operationGeneration.value()).flip();
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

        long generation() {
            return expectedGeneration == 0 ? 1 : expectedGeneration;
        }
    }

    record AllocationIdentity(int file, int ownerShard, Generation ownerGeneration, long allocationId,
                              long offset, int blocks, long streamFirstSequence) {
        static AllocationIdentity of(Allocation allocation) {
            return new AllocationIdentity(allocation.file(), allocation.ownerShard(), allocation.ownerGeneration(),
                    allocation.allocationId(), allocation.offset(), allocation.blocks(),
                    allocation.streamFirstSequence());
        }

        long extentEnd() {
            return offset + blocks * BLOCK_BYTES;
        }

        ByteBuffer encode() {
            return ByteBuffer.allocate(48).putInt(file).putInt(ownerShard).putLong(ownerGeneration.value())
                    .putLong(allocationId).putLong(offset).putInt(blocks).putInt(0).putLong(streamFirstSequence).flip();
        }
    }

    /** Bound facts established by protected DATA parsing and terminal or successful successor authority. */
    record FreeFacts(AllocationIdentity allocation, long allocationOrder, long firstSequence, long through,
                     long records, long bookieCut, long arenaCut, String digest) {
        FreeFacts(AllocationIdentity allocation, long allocationOrder, long firstSequence, long through,
                  long records, long bookieCut, String digest) {
            this(allocation, allocationOrder, firstSequence, through, records, bookieCut, -1, digest);
        }

        FreeFacts {
            Objects.requireNonNull(allocation, "allocation");
            if (allocationOrder < 0 || records < 0 || bookieCut < -1 || arenaCut < -1
                    || (records == 0 ? firstSequence != -1 || through != -1
                            : firstSequence < allocation.streamFirstSequence() || through < firstSequence
                                || through == Long.MAX_VALUE)
                    || digest == null || digest.length() != 64 || HexFormat.of().parseHex(digest).length != 32) {
                throw new IllegalArgumentException("Invalid whole-allocation FREE facts");
            }
        }
    }

    /** Only prepareFree creates a submit-capable command; replay consumes its durable facts, never old DATA. */
    static final class Free implements Command, AutoCloseable {
        private final UUID operationId;
        private final Generation operationGeneration;
        private final long expectedGeneration;
        private final FreeFacts facts;
        private final IsolatedSegmentFiles.ReclamationGuard protection;
        private final AtomicBoolean runtimeClaimed = new AtomicBoolean();
        private final AtomicBoolean runtimeFinished = new AtomicBoolean();

        private Free(UUID operationId, Generation operationGeneration, long expectedGeneration,
                     FreeFacts facts, IsolatedSegmentFiles.ReclamationGuard protection) {
            this.operationId = Objects.requireNonNull(operationId, "operationId");
            this.operationGeneration = Objects.requireNonNull(operationGeneration, "operationGeneration");
            this.expectedGeneration = expectedGeneration;
            this.facts = Objects.requireNonNull(facts, "facts");
            this.protection = protection;
            if (operationId.equals(new UUID(0, 0)) || expectedGeneration < 1 || expectedGeneration == Long.MAX_VALUE
                    || operationGeneration.value() != expectedGeneration + 1) {
                throw new IllegalArgumentException("Invalid FREE generation");
            }
        }

        @Override
        public SegmentControlFormat.Type type() {
            return SegmentControlFormat.Type.FREE;
        }

        @Override
        public UUID operationId() {
            return operationId;
        }

        @Override
        public Generation operationGeneration() {
            return operationGeneration;
        }

        @Override
        public long expectedGeneration() {
            return expectedGeneration;
        }

        FreeFacts facts() {
            return facts;
        }

        private Free replayFacts() {
            return new Free(operationId, operationGeneration, expectedGeneration, facts, null);
        }

        @Override
        public ByteBuffer body() {
            ByteBuffer body = ByteBuffer.allocate(facts.arenaCut() < 0 ? 128 : 136)
                    .putInt(facts.arenaCut() < 0 ? 1 : 2).putInt(0).put(facts.allocation().encode())
                    .putLong(facts.allocationOrder()).putLong(facts.firstSequence()).putLong(facts.through())
                    .putLong(facts.records()).putLong(facts.bookieCut());
            if (facts.arenaCut() >= 0) {
                body.putLong(facts.arenaCut());
            }
            return body.put(HexFormat.of().parseHex(facts.digest())).flip();
        }

        boolean claim(SegmentArenaControlLog log) throws IOException {
            if (log.resolvedFree(this)) {
                return false;
            }
            if (protection == null || !runtimeClaimed.compareAndSet(false, true)) {
                throw new IOException("Replayed FREE facts are not a new runtime proof");
            }
            try {
                protection.claim(log);
            } catch (IOException | RuntimeException | Error failure) {
                runtimeClaimed.set(false);
                throw failure;
            }
            return true;
        }

        void requireClaimed(SegmentArenaControlLog log) throws IOException {
            if (log.resolvedFree(this)) {
                return;
            }
            if (protection == null || !runtimeClaimed.get() || !protection.claimedBy(log)) {
                throw new IOException("FREE has no protected runtime verification");
            }
        }

        void finish(SegmentArenaControlLog log) {
            if (protection != null && runtimeFinished.compareAndSet(false, true)) {
                protection.finish(!log.dataFileAvailable(facts.allocation().file()), runtimeClaimed.get());
            }
        }

        @Override
        public void close() {
            if (protection != null) {
                protection.close();
            }
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Free free && operationId.equals(free.operationId)
                    && operationGeneration.equals(free.operationGeneration)
                    && expectedGeneration == free.expectedGeneration && facts.equals(free.facts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operationId, operationGeneration, expectedGeneration, facts);
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

    private record Slot(int file, long index) implements Comparable<Slot> {
        @Override
        public int compareTo(Slot other) {
            int byFile = Integer.compare(file, other.file);
            return byFile == 0 ? Long.compare(index, other.index) : byFile;
        }
    }

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

    private record Grant(Allocation command, long generation, long allocationOrder) { }

    private record AppliedOperation(Command command, Status status, long sequence) { }

    /** Applied commands own immutable fields; image generation need not retain another serialized copy. */
    private record CheckpointOperation(AppliedOperation applied) implements SegmentControlCheckpoint.ImageRecord {
        @Override
        public long sequence() {
            return applied.sequence();
        }

        @Override
        public int status() {
            return applied.status().ordinal() + 1;
        }

        @Override
        public ByteBuffer record() {
            Command command = applied.command();
            return SegmentControlFormat.encode(command.type(), LOG_GENERATION, applied.sequence(),
                    command.operationId(), command.operationGeneration(), command.expectedGeneration(), command.body());
        }
    }

    private record EntryKey(ProfileLedgerContext context, long entryId) { }

    private record CopyKey(SegmentPointRead.Location location, UUID moveId, long moveGeneration) { }

    private record SelectedMove(UUID moveId, Generation moveGeneration, long bookieCut,
                                SegmentPointRead.Location target, String entrySha256, long sequence) { }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final long maxLogBytes;
    private final SegmentFileIo io;
    private FileChannel channel;
    private final FileChannel originalChannel;
    private final SegmentDataFileLock writerLock;
    private final boolean inspectionOnly;
    private final SegmentOwnedFiles inspectionFiles;
    private SegmentControlRotation.Root rotationRoot;
    private Thread rotating;
    private Generation physicalGeneration = LOG_GENERATION;
    // Whole allocations are installed, freed and reused atomically; one key names the entire extent.
    private TreeMap<Slot, Grant> allocations = new TreeMap<>();
    private Map<Long, Grant> currentAllocations = new HashMap<>();
    private TreeMap<Long, Long> issuedAllocationIds = new TreeMap<>();
    private long allocationHighWatermark;
    private List<RetiredRange> retiredRanges = new ArrayList<>();
    private long retiredBookieCut = -1;
    private TreeMap<Slot, Free> retiredSlots = new TreeMap<>();
    private Map<Long, AllocationTail> allocationTails = new HashMap<>();
    private Map<Stream, Long> streamStarts = new HashMap<>();
    private Map<UUID, AppliedOperation> appliedOperations = new HashMap<>();
    private Map<EntryKey, SelectedMove> selectedMoves = new HashMap<>();
    private Map<Long, Map<CopyKey, String>> committedCopies = new HashMap<>();
    private final boolean[] dataBlocked;
    private final boolean[] durableQuarantine;
    private final boolean[] quarantineReserved;
    private long sessionEpoch;
    private boolean sessionOpen;
    private boolean grantFreeSession;
    private long reconstructionNoAppendThrough = -1;
    private PendingReconstruction pendingReconstruction;
    private boolean verifiedReconstruction;
    private boolean sessionCloseReserved;
    private final boolean[] spacePressure;
    private IsolatedSegmentFiles sessionOwner;
    private Command preparedSessionEnd;
    private long nextOffset = FIRST_RECORD;
    private boolean rotationRequested;
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
                                   FileChannel channel, SegmentDataFileLock writerLock, boolean inspectionOnly) {
        this(config, identity, maxLogBytes, io, channel, writerLock, inspectionOnly, null);
    }

    private SegmentArenaControlLog(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentFileIo io, FileChannel channel, SegmentDataFileLock writerLock,
            boolean inspectionOnly, SegmentOwnedFiles inspectionFiles) {
        this.inspectionFiles = inspectionFiles;
        this.config = config;
        this.identity = identity;
        this.maxLogBytes = maxLogBytes;
        this.io = io;
        this.channel = channel;
        this.originalChannel = channel;
        this.writerLock = writerLock;
        this.inspectionOnly = inspectionOnly;
        dataBlocked = new boolean[config.dataFiles()];
        spacePressure = new boolean[config.dataFiles()];
        durableQuarantine = new boolean[config.dataFiles()];
        quarantineReserved = new boolean[config.dataFiles()];
    }

    static SegmentArenaControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                       long maxLogBytes) throws IOException {
        return open(config, identity, maxLogBytes, new SegmentFileIo());
    }

    /** One wire envelope for all histories; MOVE rows and CLOSED tails do not each receive a full log. */
    static long controlCapacity(SegmentControlCapacity.Layout layout, long logBytes, int pendingEntries, int files) {
        long history = SegmentControlCapacity.sum(layout.object(AppliedOperation.class),
                layout.object(UUID.class), layout.object(Generation.class), layout.hashEntry());
        long allocationGraph = SegmentControlCapacity.sum(layout.object(Allocation.class),
                layout.object(Generation.class));
        long allocation = SegmentControlCapacity.sum(history, allocationGraph, layout.object(Grant.class),
                layout.object(Slot.class), layout.object(Stream.class),
                SegmentControlCapacity.times(6, layout.object(Long.class)),
                SegmentControlCapacity.times(2, layout.treeEntry()),
                SegmentControlCapacity.times(4, layout.hashEntry()), layout.hashMap(),
                layout.object(AllocationTail.class));
        // Each issued allocation accounts for its outer COPY map, tail slot and possible reconstructed tail.
        long freeGraph = SegmentControlCapacity.sum(layout.object(Free.class), layout.object(FreeFacts.class),
                layout.object(AllocationIdentity.class), layout.object(Generation.class), layout.string(64),
                SegmentControlCapacity.times(2, layout.object(AtomicBoolean.class)));
        long free = SegmentControlCapacity.sum(history, freeGraph, layout.object(Slot.class), layout.treeEntry(),
                // addRetirement can retain old/new ranges and lists while merging and sorting.
                SegmentControlCapacity.times(2, layout.object(RetiredRange.class)), 8 * 8);
        long session = SegmentControlCapacity.sum(history, layout.object(DataSession.class), layout.list());
        long resolutionCommand = Math.max(layout.object(ResolveNeverAllocatedSession.class),
                layout.object(ResolveGrantFreeSession.class));
        long resolution = SegmentControlCapacity.sum(history, resolutionCommand);
        long reconstruction = SegmentControlCapacity.sum(history, layout.object(ResolveClosedReconstruction.class),
                layout.object(ReconstructionBinding.class), layout.object(PendingReconstruction.class),
                layout.object(ProfileClosedReconstruction.Image.class), layout.object(FileIdentity.class),
                SegmentControlCapacity.times(4, layout.string(64)));
        long tail = SegmentControlCapacity.sum(layout.object(AllocationTail.class), 8);
        long moveGraph = SegmentControlCapacity.sum(layout.object(SegmentArenaMove.class), layout.list());
        long moveRowGraph = SegmentControlCapacity.sum(layout.object(SegmentArenaMove.Entry.class),
                layout.object(SegmentArenaMove.Source.class),
                SegmentControlCapacity.times(2, layout.object(SegmentPointRead.Location.class)),
                SegmentControlCapacity.times(2, layout.object(SegmentBatchFormat.Address.class)),
                SegmentControlCapacity.times(4, layout.object(Generation.class)),
                // Runtime preparation accepts equal contexts; it does not require the same object identity.
                SegmentControlCapacity.times(2, layout.context()),
                layout.string(64), 8);
        long moveRow = SegmentControlCapacity.sum(moveRowGraph, layout.object(EntryKey.class),
                layout.object(SelectedMove.class), layout.object(CopyKey.class),
                SegmentControlCapacity.times(2, layout.hashEntry()));
        long quarantine = SegmentControlCapacity.sum(history, layout.object(FileQuarantine.class));
        long checkpoint = SegmentControlCapacity.sum(history, layout.object(CheckpointCommit.class),
                layout.reference());
        long perWire = SegmentControlCapacity.slope(allocation, 192);
        perWire = Math.max(perWire, SegmentControlCapacity.slope(free, 272));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(session, 160));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(resolution, 160));
        perWire = Math.max(perWire,
                SegmentControlCapacity.slope(reconstruction, 144 + ResolveClosedReconstruction.BYTES));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(tail, 24));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(
                SegmentControlCapacity.sum(history, moveGraph), 160));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(moveRow, 252));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(quarantine, 160));
        perWire = Math.max(perWire, SegmentControlCapacity.slope(checkpoint, 208));
        long fixed = SegmentControlCapacity.sum(layout.object(SegmentArenaControlLog.class),
                SegmentControlCapacity.times(6, layout.hashMap()),
                SegmentControlCapacity.times(3, layout.object(TreeMap.class)), layout.list(),
                SegmentControlCapacity.times(4, layout.bytes(files)),
                SegmentControlCapacity.times(2, layout.longs(files)),
                SegmentControlCapacity.times(8, layout.reference()),
                SegmentControlCapacity.times(2, layout.object(SegmentControlRotation.Root.class)));
        // Callers can prepare a full P-command batch before append's capacity precheck. The original
        // command and replayFacts may coexist; never clip this workspace to the remaining journal bytes.
        long maxMove = SegmentControlCapacity.sum(moveGraph,
                SegmentControlCapacity.times(256, SegmentControlCapacity.sum(moveRowGraph,
                        layout.object("org.apache.bookkeeper.bookie.storage.segment.SegmentArenaMove$Key"),
                        layout.hashEntry())), layout.hashSet());
        int maxTails = (SegmentControlCapacity.MAX_RECORD_BYTES - 144 - 24) / 24;
        long maxSession = SegmentControlCapacity.sum(layout.object(DataSession.class), layout.list(),
                SegmentControlCapacity.times(maxTails, tail));
        long maxCommand = Math.max(Math.max(maxMove, maxSession), Math.max(freeGraph, allocationGraph));
        maxCommand = Math.max(maxCommand, resolutionCommand);
        long pendingCommands = SegmentControlCapacity.times(pendingEntries,
                SegmentControlCapacity.sum(maxCommand, layout.object(UUID.class), layout.object(Generation.class),
                        layout.object(Result.class), 8 * 8));
        // prepareFrees has its own 16-member limit, which can exceed a small pendingEntries setting.
        pendingCommands = Math.max(pendingCommands, SegmentControlCapacity.times(16,
                SegmentControlCapacity.sum(freeGraph, layout.object(UUID.class), layout.object(Generation.class),
                        layout.object(Result.class), 8 * 8)));
        long pending = SegmentControlCapacity.sum(pendingCommands, SegmentControlCapacity.times(4, layout.list()));
        return SegmentControlCapacity.sum(fixed,
                SegmentControlCapacity.times(perWire, SegmentControlCapacity.wireBytes(logBytes)),
                layout.snapshot(SegmentControlCapacity.operations(logBytes, 176, 160),
                        layout.object(CheckpointOperation.class)),
                layout.scanner((logBytes - FIRST_RECORD) / 160 + 1), pending);
    }

    static SegmentArenaControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                       long maxLogBytes, SegmentFileIo io) throws IOException {
        return open(config, identity, maxLogBytes, io, false);
    }

    /** Observed replay state only. No recovery sync or usable allocation authority escapes this method. */
    record Inspection(long physicalGeneration, long appliedSequence, long checkpointGeneration,
                      long sessionEpoch, boolean sessionOpen, int currentAllocations, long retiredSlots,
                      List<Integer> recordedQuarantineFiles) { }

    static Inspection inspect(IsolatedSegmentConfig config, SegmentFileHeader identity, long maxLogBytes)
            throws IOException {
        try (SegmentArenaControlLog observed = open(config, identity, maxLogBytes, new SegmentFileIo(), true)) {
            return observed.inspection();
        }
    }

    private Inspection inspection() {
        List<Integer> quarantine = new ArrayList<>();
        for (int file = 0; file < durableQuarantine.length; file++) {
            if (durableQuarantine[file]) {
                quarantine.add(file);
            }
        }
        return new Inspection(physicalGeneration.value(), appliedThrough, checkpointGeneration, sessionEpoch,
                sessionOpen, currentAllocations.size(), retiredBlockCount(), List.copyOf(quarantine));
    }

    record ObservedGrant(Allocation allocation, long controlSequence) { }

    record ObservedStream(int shard, long generation, long firstSequence) { }

    record ObservedRetirement(UUID operationId, Generation generation, FreeFacts facts) { }

    record ObservedTail(AllocationIdentity allocation, long generation, long end, UUID closeOperation,
                        long closeEpoch, long closeSequence) { }

    private static boolean hasFullHealthyTails(long currentReused, Map<Long, ObservedTail> tails) {
        return currentReused == tails.size()
                && tails.values().stream().allMatch(tail -> tail.end() == tail.allocation().extentEnd());
    }

    record ReconstructionHistory(boolean committedMoves, int successfulFrees, int successfulReuses,
                                 long requiredBookieCut) {
        boolean reused() {
            return successfulReuses > 0;
        }

        ProfileClosedReconstruction.HistoryMode mode(long currentReused, Map<Long, ObservedTail> tails) {
            if (reused()) {
                return hasFullHealthyTails(currentReused, tails)
                        ? ProfileClosedReconstruction.HistoryMode.FULLY_SEALED_REUSE
                        : ProfileClosedReconstruction.HistoryMode.RECONSTRUCTED_FULL_REUSE;
            }
            if (successfulFrees > 0) {
                return ProfileClosedReconstruction.HistoryMode.RETIRED;
            }
            return committedMoves ? ProfileClosedReconstruction.HistoryMode.COMMITTED_MOVE
                    : ProfileClosedReconstruction.HistoryMode.ORIGINAL;
        }

        void requireMode(ProfileClosedReconstruction.HistoryMode expected, long bookieCut,
                boolean hasSelectors, int retiredSlots, long currentReused, Map<Long, ObservedTail> tails)
                throws IOException {
            if (mode(currentReused, tails) != expected || retiredSlots != successfulFrees - successfulReuses
                    || bookieCut < requiredBookieCut
                    || (hasSelectors && !committedMoves)
                    || (expected == ProfileClosedReconstruction.HistoryMode.COMMITTED_MOVE && !hasSelectors)) {
                throw new IOException("CLOSED reconstruction mode or original control history differs");
            }
        }
    }

    record RecoveryDependencies(Inspection cut, long allocationHighWatermark, long retiredBookieCut,
                                ReconstructionHistory history, boolean reconstructionCapacity, String historyDigest,
                                List<ObservedGrant> grants, List<ObservedStream> streams,
                                Map<EntryKey, SelectedMove> selections, Map<Long, Map<CopyKey, String>> copies,
                                List<ObservedRetirement> retiredSlots, List<RetiredRange> retiredRanges,
                                Map<Long, ObservedTail> tails) {
        long currentReused() {
            return grants.stream().filter(grant -> grant.allocation().expectedGeneration() > 0).count();
        }

        ProfileClosedReconstruction.HistoryMode mode() {
            return history.mode(currentReused(), tails);
        }

        int committedCopyCount() {
            return copies.values().stream().mapToInt(Map::size).sum();
        }

        Set<SegmentPointRead.Location> selectedLocations() {
            return selections.values().stream().map(SelectedMove::target)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
        }

        boolean isSelectedRecord(SegmentPointRead.Location location, SegmentBatchParser.Record record, String digest)
                throws IOException {
            return SegmentArenaControlLog.isSelectedRecord(
                    selections.get(new EntryKey(location.context(), location.entryId())), location, record, digest);
        }

        boolean wasCommittedCopy(SegmentPointRead.Location location, SegmentBatchParser.Record record, String digest) {
            return digest.equals(committedCopyDigest(copies, location, record));
        }
    }

    synchronized ReconstructionHistory reconstructionHistory() {
        boolean moves = false;
        int successfulFrees = 0;
        int successfulReuses = 0;
        long bookieCut = -1;
        for (var operation : appliedOperations.values()) {
            if (operation.status() != Status.APPLIED_DURABLE) {
                continue;
            }
            if (operation.command() instanceof SegmentArenaMove move) {
                moves = true;
                bookieCut = Math.max(bookieCut, move.bookieCut());
            } else if (operation.command() instanceof Free free) {
                successfulFrees++;
                bookieCut = Math.max(bookieCut, free.facts().bookieCut());
            } else if (operation.command() instanceof Allocation allocation && allocation.expectedGeneration() > 0) {
                successfulReuses++;
            }
        }
        return new ReconstructionHistory(moves, successfulFrees, successfulReuses, bookieCut);
    }

    synchronized void requireReconstructionMode(ProfileClosedReconstruction.HistoryMode mode, long bookieCut)
            throws IOException {
        long currentReused = currentAllocations.values().stream()
                .filter(grant -> grant.command().expectedGeneration() > 0).count();
        reconstructionHistory().requireMode(mode, bookieCut, !selectedMoves.isEmpty(), retiredSlots.size(),
                currentReused, observeReconstructionTails());
    }

    private Map<Long, ObservedTail> observeReconstructionTails() {
        if (allocationTails.isEmpty()) {
            return Map.of();
        }
        long openSequence = -1;
        var tails = new HashMap<Long, ObservedTail>();
        for (var operation : appliedOperations.values()) {
            if (operation.status() != Status.APPLIED_DURABLE || !(operation.command() instanceof DataSession session)) {
                continue;
            }
            long epoch = session.operationGeneration().value();
            if (session.open() && epoch == sessionEpoch) {
                openSequence = operation.sequence();
            } else if (!session.open() && epoch < sessionEpoch) {
                for (var tail : session.tails()) {
                    var grant = currentAllocations.get(tail.allocationId());
                    if (grant == null || grant.command().expectedGeneration() == 0
                            || grant.generation() != tail.generation()) {
                        continue;
                    }
                    var previous = tails.get(tail.allocationId());
                    if (previous == null || previous.closeSequence() < operation.sequence()) {
                        tails.put(tail.allocationId(), new ObservedTail(AllocationIdentity.of(grant.command()),
                                tail.generation(), tail.end(), session.operationId(), epoch, operation.sequence()));
                    }
                }
            }
        }
        long before = openSequence;
        tails.values().removeIf(tail -> tail.closeSequence() >= before);
        return Map.copyOf(tails);
    }

    /** Only the caller's existing lock scope and owned readers participate in this observation. */
    static RecoveryDependencies inspectRecoveryDependencies(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, int maximumRows, SegmentDataFileLock guard, SegmentOwnedFiles files,
            List<Path> readyDataPaths) throws IOException {
        guard.requireHeld(config.directory().toRealPath().resolve("arena-control.lock"));
        Path path = config.directory().resolve("arena-control");
        FileChannel original = SegmentOwnedFiles.openRegularReadOnly(files, path);
        var observed = new SegmentArenaControlLog(config, identity, maxLogBytes, new SegmentFileIo(),
                original, null, true, files);
        observed.validateStorageFiles(identity, readyDataPaths);
        observed.recover(identity);
        long copyRows = observed.committedCopies.values().stream().mapToLong(Map::size).sum();
        long tailRows = observed.currentAllocations.values().stream()
                .filter(grant -> grant.command().expectedGeneration() > 0).count();
        if ((long) observed.currentAllocations.size() + observed.streamStarts.size() + observed.selectedMoves.size()
                + observed.retiredSlots.size() + observed.retiredRanges.size() + copyRows + tailRows > maximumRows) {
            throw new IOException("Arena recovery dependency observation exceeds its row limit");
        }
        var copies = new HashMap<Long, Map<CopyKey, String>>();
        observed.committedCopies.forEach((allocation, rows) -> copies.put(allocation, Map.copyOf(rows)));
        var history = new SegmentControlCheckpoint.Snapshot(observed.appliedThrough, observed.appliedOffset,
                observed.appliedOperations.values().stream()
                        .sorted(Comparator.comparingLong(AppliedOperation::sequence))
                        .map(CheckpointOperation::new).toList());
        var result = new RecoveryDependencies(observed.inspection(), observed.allocationHighWatermark,
                observed.retiredBookieCut, observed.reconstructionHistory(),
                maxLogBytes - observed.nextOffset - observed.rotationReserveBytes()
                        >= 144 + ResolveClosedReconstruction.BYTES,
                SegmentControlCheckpoint.reference(1, 0, history, maxLogBytes).digest(),
                observed.currentAllocations.values().stream()
                        .map(grant -> new ObservedGrant(grant.command(), grant.allocationOrder()))
                        .sorted(Comparator.comparingLong(ObservedGrant::controlSequence)).toList(),
                observed.streamStarts.entrySet().stream().map(entry -> new ObservedStream(entry.getKey().shard(),
                        entry.getKey().generation(), entry.getValue()))
                        .sorted(Comparator.comparingInt(ObservedStream::shard)
                                .thenComparingLong(ObservedStream::generation)).toList(),
                Map.copyOf(observed.selectedMoves), Map.copyOf(copies),
                observed.retiredSlots.values().stream().map(free -> new ObservedRetirement(free.operationId(),
                        free.operationGeneration(), free.facts())).toList(), List.copyOf(observed.retiredRanges),
                tailRows == 0 ? Map.of() : observed.observeReconstructionTails());
        if (observed.channel != original) {
            observed.channel.close();
        }
        original.close();
        return result;
    }

    /** The bootstrap child already holds both existing guards and owns every reader, including failed opens. */
    static void requireUnusedInspection(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentOwnedFiles files) throws IOException {
        Objects.requireNonNull(files, "files");
        if (identity.manifest() == null || identity.bookieScope() || maxLogBytes < FIRST_RECORD + 192) {
            throw new IllegalArgumentException("Bounded manifest Arena inspection required");
        }
        identity.requireStoredManifest(config, -4, files);
        Path path = config.directory().resolve("arena-control");
        FileChannel original = SegmentOwnedFiles.openRegularReadOnly(files, path);
        // No mutable authority or raw handle escapes; the original owner retains all readers on any failure.
        var observed = new SegmentArenaControlLog(config, identity, maxLogBytes, new SegmentFileIo(),
                original, null, true, files);
        observed.validateStorageFiles(identity);
        observed.recover(identity);
        observed.requireUnused();
        if (observed.channel != original) {
            observed.channel.close();
        }
        original.close();
        files.requireHealthy();
    }

    private void requireUnused() throws IOException {
        if (sessionEpoch != 0 || sessionOpen || allocationHighWatermark != 0 || !allocations.isEmpty()
                || !retiredSlots.isEmpty() || !retiredRanges.isEmpty() || !selectedMoves.isEmpty()) {
            throw new IOException("Classic bootstrap requires an Arena never admitted to DATA use");
        }
        for (boolean quarantine : durableQuarantine) {
            if (quarantine) {
                throw new IOException("Classic bootstrap cannot accept a quarantined Arena");
            }
        }
        for (AppliedOperation operation : appliedOperations.values()) {
            if (!(operation.command() instanceof CheckpointCommit)
                    && (operation.status() == Status.APPLIED_DURABLE || operation.status() == Status.ALREADY_DURABLE)) {
                throw new IOException("Classic bootstrap cannot accept applied DATA history");
            }
        }
    }

    private static SegmentArenaControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                               long maxLogBytes, SegmentFileIo io, boolean inspectionOnly)
            throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(io, "io");
        identity.requireStoredManifest(config, -4);
        if (maxLogBytes < FIRST_RECORD + 192) {
            throw new IllegalArgumentException("Control log capacity cannot hold one record");
        }
        Path lockPath = config.directory().resolve("arena-control.lock");
        SegmentDataFileLock writerLock = inspectionOnly ? SegmentDataFileLock.openExistingControl(lockPath)
                : SegmentDataFileLock.openControl(lockPath);
        return openWhileLocked(config, identity, maxLogBytes, io, writerLock, inspectionOnly, null);
    }

    static SegmentArenaControlLog openManifestWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentFileIo io, SegmentDataFileLock guard, SegmentOwnedFiles files,
            boolean inspectionOnly) throws IOException {
        guard.requireHeld(config.directory().resolve("arena-control.lock"));
        identity.requireStoredManifest(config, -4, files);
        return openWhileLocked(config, identity, maxLogBytes, io, null, inspectionOnly, files);
    }

    SegmentOwnedFiles ownedFiles() {
        return inspectionFiles;
    }

    private static SegmentArenaControlLog openWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentFileIo io, SegmentDataFileLock writerLock, boolean inspectionOnly,
            SegmentOwnedFiles files) throws IOException {
        FileChannel original = null;
        SegmentArenaControlLog result = null;
        try {
            Path path = config.directory().resolve("arena-control");
            original = inspectionOnly ? SegmentOwnedFiles.openRegularReadOnly(files, path)
                    : SegmentOwnedFiles.open(files, path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                            LinkOption.NOFOLLOW_LINKS);
            result = new SegmentArenaControlLog(config, identity, maxLogBytes, io, original, writerLock,
                    inspectionOnly, files);
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
            if (writerLock != null) {
                try {
                    writerLock.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /** One physical force covers the whole batch; no result is applied before that force returns. */
    DurableBatch appendDurably(List<? extends Command> commands) throws IOException {
        List<Command> batch = List.copyOf(commands);
        long first;
        long start;
        synchronized (this) {
            requireControlWritable();
            if (writing || pending != null || batch.isEmpty() || batch.size() > config.maxPendingEntries()) {
                throw new IllegalArgumentException("Invalid or unresolved control batch");
            }
            long bytes = 0;
            first = durableThrough + 1;
            long sequence = first;
            // Validate the complete immutable batch before I/O without retaining its serialized payload.
            for (Command command : batch) {
                validateCommand(command);
                if (command instanceof Free free) {
                    free.requireClaimed(this);
                } else if (command instanceof SegmentArenaMove move) {
                    move.requireClaimed(this);
                }
                if (command instanceof DataSession session && !session.tails().isEmpty()
                        && command != preparedSessionEnd) {
                    throw new IOException("Reused allocation tails require the actual healthy writer close");
                }
                if (command instanceof DataSession session && session.open() && pendingReconstruction != null
                        && (!verifiedReconstruction || sessionOwner == null)) {
                    throw new IOException("First OPEN requires the verified original reconstruction and writer");
                }
                if ((command instanceof ResolveNeverAllocatedSession || command instanceof ResolveGrantFreeSession
                        || command instanceof ResolveClosedReconstruction)
                        && command != preparedSessionEnd) {
                    throw new IOException("DATA session resolution requires the original DATA lock");
                }
                bytes = Math.addExact(bytes, SegmentControlFormat.encodedLength(
                        command.type(), physicalGeneration, sequence++,
                        command.operationId(), command.operationGeneration(), command.expectedGeneration(),
                        command.body()));
            }
            long reserved = sessionCloseReserved && batch.stream().noneMatch(command ->
                    command instanceof DataSession session && !session.open())
                    ? sessionCloseBytes(batch.stream().filter(command -> command instanceof Allocation allocation
                            && allocation.expectedGeneration() > 0).count()) : 0;
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
                rotationRequested |= rotating == null && rotationReserveBytes() != 0
                        && bytes <= maxLogBytes - SegmentControlRotation.FIRST_RECORD - reserved;
                throw new SegmentControlCapacityException("Arena control log capacity exhausted");
            }
            start = nextOffset;
            // Reserve the physical suffix before the first write; failure never reuses its coordinates.
            nextOffset += bytes;
            writing = true;
        }
        try {
            long position = start;
            long sequence = first;
            for (Command command : batch) {
                ByteBuffer record = SegmentControlFormat.encode(command.type(), physicalGeneration, sequence++,
                        command.operationId(), command.operationGeneration(), command.expectedGeneration(),
                        command.body());
                io.writeFully(channel, record, position);
                position += record.remaining();
            }
            io.force(channel, true);
            synchronized (this) {
                durableThrough = first + batch.size() - 1;
                pending = new DurableBatch(first, batch);
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
                        ? command instanceof Allocation allocation ? allocation.generation()
                                : command instanceof SegmentArenaMove || command instanceof ResolveNeverAllocatedSession
                                        || command instanceof ResolveGrantFreeSession
                                        || command instanceof ResolveClosedReconstruction
                                        ? command.operationGeneration().value()
                                        : command.expectedGeneration() + 1 : 0;
                results.add(new Result(status, command.operationId(), sequence, generation,
                        durableThrough, appliedThrough));
            }
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
        pending = null;
        return List.copyOf(results);
    }

    List<Result> submitBatch(List<? extends Command> commands) throws IOException {
        claimProtectedCommands(commands);
        try {
            return applyDurable(appendDurably(commands));
        } finally {
            finishProtectedCommands(commands);
        }
    }

    void claimProtectedCommands(List<? extends Command> commands) throws IOException {
        List<Command> claimed = new ArrayList<>();
        try {
            for (Command command : commands) {
                if (command instanceof Free free && free.claim(this)) {
                    claimed.add(free);
                } else if (command instanceof SegmentArenaMove move && move.claim(this)) {
                    claimed.add(move);
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            finishProtectedCommands(claimed);
            throw failure;
        }
    }

    void finishProtectedCommands(List<? extends Command> commands) {
        for (Command command : commands) {
            if (command instanceof Free free) {
                free.finish(this);
            } else if (command instanceof SegmentArenaMove move) {
                move.finish(this);
            }
        }
    }

    private synchronized boolean resolvedFree(Free free) {
        AppliedOperation prior = appliedOperations.get(free.operationId());
        return prior != null && prior.command().equals(free);
    }

    synchronized boolean resolvedMove(SegmentArenaMove move) {
        AppliedOperation prior = appliedOperations.get(move.operationId());
        return prior != null && prior.command().equals(move);
    }

    synchronized void requireMovePreparation(IsolatedSegmentFiles files) throws IOException {
        requireQuiescentControl(files.config(), files.identity());
    }

    synchronized SegmentPointRead.Location resolveLocation(SegmentPointRead.Location hint) {
        requireOpenHistory();
        SelectedMove selected = selectedMoves.get(new EntryKey(hint.context(), hint.entryId()));
        return selected == null ? hint : selected.target();
    }

    synchronized Optional<SegmentPointRead.Location> currentMoveLocation(ProfileLedgerContext context, long entryId) {
        requireOpenHistory();
        SelectedMove selected = selectedMoves.get(new EntryKey(context, entryId));
        return selected == null ? Optional.empty() : Optional.of(selected.target());
    }

    synchronized long currentSelectorSequence(ProfileLedgerContext context, long entryId) {
        requireOpenHistory();
        SelectedMove selected = selectedMoves.get(new EntryKey(context, entryId));
        return selected == null ? -1 : selected.sequence();
    }

    synchronized boolean hasMoves() {
        requireOpenHistory();
        return !selectedMoves.isEmpty();
    }

    synchronized int recoveryMoveCount() {
        requireOpenHistory();
        return selectedMoves.size();
    }

    synchronized int recoveryRetiredRangeCount() {
        requireOpenHistory();
        return retiredRanges.size();
    }

    synchronized int recoveryAllocationCount() {
        requireOpenHistory();
        return currentAllocations.size();
    }

    synchronized List<SegmentPointRead.Location> currentMoveLocations() {
        requireOpenHistory();
        return selectedMoves.values().stream().map(selected -> selected.target()).toList();
    }

    synchronized long requiredMoveBookieCut() {
        requireOpenHistory();
        return selectedMoves.values().stream().mapToLong(SelectedMove::bookieCut).max().orElse(-1);
    }

    /** Successful copies remain proven while their allocation is current, including superseded selectors. */
    synchronized boolean wasCommittedCopy(SegmentPointRead.Location location, SegmentBatchParser.Record record) {
        requireOpenHistory();
        String digest = committedCopyDigest(committedCopies, location, record);
        return digest != null && digest.equals(SegmentArenaMove.digest(record.entry()));
    }

    private static String committedCopyDigest(Map<Long, Map<CopyKey, String>> committed,
            SegmentPointRead.Location location, SegmentBatchParser.Record record) {
        Map<CopyKey, String> copies = committed.get(location.address().allocation());
        return copies == null ? null : copies.get(new CopyKey(location, record.moveId(), record.moveGeneration()));
    }

    synchronized boolean isSelectedRecord(SegmentPointRead.Location location, SegmentBatchParser.Record record)
            throws IOException {
        requireOpenHistory();
        SelectedMove selected = selectedMoves.get(new EntryKey(location.context(), location.entryId()));
        return isSelectedRecord(selected, location, record, null);
    }

    private static boolean isSelectedRecord(SelectedMove selected, SegmentPointRead.Location location,
            SegmentBatchParser.Record record, String digest) throws IOException {
        if (selected == null) {
            return record.origin() == SegmentBatchFormat.ENTRY_DATA;
        }
        if (!selected.target().equals(location)) {
            return false;
        }
        if (record.origin() != SegmentBatchFormat.RELOCATION_COPY
                || !record.moveId().equals(selected.moveId())
                || record.moveGeneration() != selected.moveGeneration().value()
                || !selected.entrySha256().equals(digest == null ? SegmentArenaMove.digest(record.entry()) : digest)) {
            throw new IOException("Current MOVE target does not match its durable identity and digest");
        }
        return true;
    }

    void validateSelectedRecord(SegmentPointRead.Location location, SegmentBatchParser.Record record)
            throws IOException {
        if (!isSelectedRecord(location, record)) {
            throw new IOException("Record is not the current ordinary or successfully moved coordinate");
        }
    }

    /** Metadata decision only; the caller verifies a returned successor with its held DATA protection. */
    synchronized Optional<SegmentPointRead.Location> retirementSuccessor(SegmentPointRead.Location source,
                                                                         SegmentBatchParser.Record record)
            throws IOException {
        requireOpenHistory();
        SelectedMove selected = selectedMoves.get(new EntryKey(source.context(), source.entryId()));
        if (selected == null || selected.target().equals(source)) {
            return Optional.empty();
        }
        SegmentPointRead.Location target = selected.target();
        if (target.address().allocation() == source.address().allocation()) {
            throw new IOException("A live MOVE target still occupies the victim allocation");
        }
        if (record.origin() == SegmentBatchFormat.ENTRY_DATA
                && !selected.entrySha256().equals(SegmentArenaMove.digest(record.entry()))) {
            throw new IOException("MOVE predecessor bytes differ from the selected successor");
        }
        return Optional.of(target);
    }

    /** Resolve the actual graph API retry from retained applied facts without rescanning retired bytes. */
    synchronized Result resolvedFreeResult(UUID operationId, long allocationId) throws IOException {
        requireControlWritable();
        AppliedOperation prior = appliedOperations.get(operationId);
        if (prior == null) {
            return null;
        }
        Status status;
        long generation = 0;
        if (!(prior.command() instanceof Free free) || free.facts().allocation().allocationId() != allocationId) {
            status = Status.INCOMPATIBLE;
        } else {
            status = prior.status() == Status.APPLIED_DURABLE ? Status.ALREADY_DURABLE : prior.status();
            if (status == Status.ALREADY_DURABLE) {
                AllocationIdentity target = free.facts().allocation();
                Free current = retiredSlots.get(slot(target.file(), target.offset()));
                if (current == null || !current.equals(free)) {
                    status = Status.CONDITION_FAILED;
                } else {
                    generation = free.operationGeneration().value();
                }
            }
        }
        return new Result(status, operationId, prior.sequence(), generation, durableThrough, appliedThrough);
    }

    /** Blocking DATA proof; invoke on the existing bounded DATA I/O service, never an append owner. */
    Free prepareFree(UUID operationId, long allocationId, IsolatedSegmentFiles files, SegmentBookieControlLog bookie,
                     ByteBuffer workspace)
            throws IOException {
        return prepareFrees(List.of(new FreeTarget(operationId, allocationId)), files, bookie, workspace).get(0);
    }

    record FreeTarget(UUID operationId, long allocationId) { }

    synchronized Allocation currentAllocation(long allocationId) throws IOException {
        requireQuiescentControl(config, identity);
        Grant grant = currentAllocations.get(allocationId);
        if (grant == null || !dataFileAvailable(grant.command().file())) {
            throw new IOException("Allocation is not current and available");
        }
        return grant.command();
    }

    record CompactionWindow(List<Allocation> allocations, long lastOrder) { }

    /** The retained tail must be the last current grant of this fixed physical stream. */
    synchronized Optional<Allocation> latestAllocationForShard(int shard) throws IOException {
        requireQuiescentControl(config, identity);
        restartStreamForShard(shard);
        Allocation latest = null;
        long order = -1;
        for (Grant grant : currentAllocations.values()) {
            Allocation allocation = grant.command();
            if (allocation.ownerShard() == shard && dataFileAvailable(allocation.file())) {
                long candidate = grant.allocationOrder();
                if (candidate > order && permitsAllocationAppend(candidate)) {
                    latest = allocation;
                    order = candidate;
                }
            }
        }
        return Optional.ofNullable(latest);
    }

    /** Fixed file and bounded control-order interval; this scan cut never authorizes MOVE or FREE. */
    synchronized CompactionWindow compactionCandidates(int shard, long afterOrder, long throughOrder)
            throws IOException {
        requireQuiescentControl(config, identity);
        if (afterOrder < -1 || throughOrder < afterOrder
                || throughOrder > appliedThrough) {
            throw new IllegalArgumentException("Invalid compaction scan interval");
        }
        int file = shard % config.dataFiles();
        if (restartStreamForShard(shard).isEmpty() || !dataFileAvailable(file)) {
            return new CompactionWindow(List.of(), afterOrder);
        }
        TreeMap<Long, Allocation> candidates = new TreeMap<>();
        for (Grant grant : currentAllocations.values()) {
            Allocation allocation = grant.command();
            if (allocation.ownerShard() == shard && allocation.file() == file) {
                long order = grant.allocationOrder();
                if (order > afterOrder && order <= throughOrder) {
                    candidates.put(order, allocation);
                    if (candidates.size() > 16) {
                        candidates.pollLastEntry();
                    }
                }
            }
        }
        return new CompactionWindow(List.copyOf(candidates.values()),
                candidates.isEmpty() ? afterOrder : candidates.lastKey());
    }

    synchronized void requireUnusedOperation(UUID operationId) throws IOException {
        requireQuiescentControl(config, identity);
        if (appliedOperations.containsKey(operationId)) {
            throw new IOException("Internal maintenance operation identity was already used");
        }
    }

    /** One actual proof gate and one ordered CONTROL batch; no per-victim barrier is required. */
    List<Free> prepareFrees(List<FreeTarget> targets, IsolatedSegmentFiles files, SegmentBookieControlLog bookie,
                            ByteBuffer workspace) throws IOException {
        if (!files.matchesScope(config, identity) || !bookie.matchesScope(config, identity)) {
            throw new IOException("Whole-allocation FREE has another storage scope");
        }
        targets = List.copyOf(targets);
        if (targets.isEmpty() || targets.size() > 16
                || targets.stream().map(FreeTarget::allocationId).distinct().count() != targets.size()
                || targets.stream().map(FreeTarget::operationId).distinct().count() != targets.size()) {
            throw new IOException("FREE batch needs 1..16 distinct allocations and operation identities");
        }
        List<Allocation> victims = new ArrayList<>();
        List<Long> orders = new ArrayList<>();
        long arenaCut;
        synchronized (this) {
            requireQuiescentControl(config, identity);
            arenaCut = appliedThrough;
            for (FreeTarget target : targets) {
                Allocation allocation = currentAllocation(target.allocationId());
                if (!victims.isEmpty() && allocation.file() != victims.get(0).file()) {
                    throw new IOException("Protected FREE batch must use one DATA file");
                }
                victims.add(allocation);
                orders.add(currentAllocations.get(allocation.allocationId()).allocationOrder());
            }
        }
        SegmentBookieControlLog.ReclamationLease bookieLease = bookie.protectReclamation(config, identity);
        IsolatedSegmentFiles.ReclamationGuard protection;
        try {
            protection = files.protectReclamation(this, victims.get(0).file(), bookieLease);
        } catch (IOException | RuntimeException | Error failure) {
            bookieLease.close();
            throw failure;
        }
        try {
            List<Free> commands = new ArrayList<>();
            for (int i = 0; i < victims.size(); i++) {
                Allocation allocation = victims.get(i);
                FreeFacts facts = protection.verify(allocation, orders.get(i), bookie, workspace);
                if (facts.arenaCut() >= 0) {
                    if (facts.arenaCut() != arenaCut) {
                        throw new IOException("FREE batch selector changed during verification");
                    }
                    facts = new FreeFacts(facts.allocation(), facts.allocationOrder(), facts.firstSequence(),
                            facts.through(), facts.records(), facts.bookieCut(), Math.addExact(arenaCut, i),
                            facts.digest());
                }
                commands.add(new Free(targets.get(i).operationId(), new Generation(allocation.generation() + 1),
                        allocation.generation(), facts, protection));
            }
            synchronized (this) {
                requireQuiescentControl(config, identity);
                if (appliedThrough != arenaCut || victims.stream().anyMatch(allocation -> {
                    Grant current = currentAllocations.get(allocation.allocationId());
                    return current == null || !allocation.equals(current.command());
                })) {
                    throw new IOException("Whole-allocation FREE predecessor changed during verification");
                }
            }
            protection.expectClaims(commands.size());
            return List.copyOf(commands);
        } catch (IOException | RuntimeException | Error failure) {
            protection.close();
            throw failure;
        }
    }

    synchronized List<Free> retiredAllocations() {
        requireOpenHistory();
        return appliedOperations.values().stream().filter(operation ->
                operation.status() == Status.APPLIED_DURABLE && operation.command() instanceof Free)
                .map(operation -> (Free) operation.command()).toList();
    }

    /** Empty retirements carry no sequence range, but keep their required authority cut. */
    synchronized long requiredRetiredBookieCut() {
        return retiredBookieCut;
    }

    record RetiredRange(int shard, Generation generation, long streamFirstSequence, long allocationOrder,
                        long lastAllocationOrder, long firstSequence, long through, long bookieCut) { }

    /** Consecutive retired DATA can share a recovery range, but never cross a current allocation's control order. */
    synchronized List<RetiredRange> retiredStreamRanges() {
        requireOpenHistory();
        return List.copyOf(retiredRanges);
    }

    private void addRetirement(FreeFacts facts) {
        retiredBookieCut = Math.max(retiredBookieCut, facts.bookieCut());
        if (facts.records() != 0) {
            AllocationIdentity allocation = facts.allocation();
            retiredRanges.add(new RetiredRange(allocation.ownerShard(), allocation.ownerGeneration(),
                    allocation.streamFirstSequence(), facts.allocationOrder(), facts.allocationOrder(),
                    facts.firstSequence(), facts.through(), facts.bookieCut()));
        }
        retiredRanges.sort(Comparator.comparingInt(RetiredRange::shard)
                .thenComparingLong(range -> range.generation().value())
                .thenComparingLong(RetiredRange::allocationOrder));
        List<RetiredRange> merged = new ArrayList<>();
        for (RetiredRange next : retiredRanges) {
            RetiredRange previous = merged.isEmpty() ? null : merged.get(merged.size() - 1);
            boolean join = previous != null && previous.shard() == next.shard()
                    && previous.generation().equals(next.generation())
                    && previous.streamFirstSequence() == next.streamFirstSequence()
                    && previous.lastAllocationOrder() < next.allocationOrder()
                    && previous.through() + 1 == next.firstSequence();
            if (join) {
                for (Grant grant : currentAllocations.values()) {
                    Allocation allocation = grant.command();
                    long order = grant.allocationOrder();
                    if (allocation.ownerShard() == next.shard()
                            && allocation.ownerGeneration().equals(next.generation())
                            && order > previous.allocationOrder() && order < next.lastAllocationOrder()) {
                        join = false;
                        break;
                    }
                }
            }
            if (join) {
                merged.set(merged.size() - 1, new RetiredRange(previous.shard(), previous.generation(),
                        previous.streamFirstSequence(), previous.allocationOrder(), next.lastAllocationOrder(),
                        previous.firstSequence(), next.through(), Math.max(previous.bookieCut(), next.bookieCut())));
            } else {
                merged.add(next);
            }
        }
        retiredRanges.clear();
        retiredRanges.addAll(merged);
    }

    synchronized long retiredExtentEnd(int file, long offset) {
        requireOpenHistory();
        Free free = retiredExtentAt(slot(file, offset));
        return free == null ? -1 : free.facts().allocation().extentEnd();
    }

    synchronized long allocationGeneration(int file, long offset) {
        requireOpenHistory();
        Grant current = grantAt(slot(file, offset));
        Free retired = retiredExtentAt(slot(file, offset));
        return current != null ? current.generation() : retired != null ? retired.operationGeneration().value() : 0;
    }

    synchronized List<Allocation> currentReusedAllocations() {
        requireOpenHistory();
        return currentAllocations.values().stream().map(Grant::command)
                .filter(allocation -> allocation.expectedGeneration() > 0)
                .sorted(Comparator.comparingLong(Allocation::allocationId)).toList();
    }

    synchronized long sealedAllocationEnd(long allocationId) throws IOException {
        return requireAllocationTail(allocationId).end();
    }

    private AllocationTail requireAllocationTail(long allocationId) throws IOException {
        requireOpenHistory();
        AllocationTail tail = allocationTails.get(allocationId);
        if (tail == null) {
            throw new IOException("Current reused allocation has no durable read boundary");
        }
        return tail;
    }

    record ReusedExtent(long usedEnd, long end, TailSource source) {
        boolean isSuffix(long offset) {
            return offset >= usedEnd;
        }

        boolean containsBatch(long offset, int length) {
            return length <= usedEnd - offset;
        }
    }

    synchronized Optional<ReusedExtent> reusedExtent(int file, long offset) throws IOException {
        requireOpenHistory();
        Grant grant = grantAt(slot(file, offset));
        if (grant == null || grant.command().expectedGeneration() == 0) {
            return Optional.empty();
        }
        Allocation allocation = grant.command();
        var tail = requireAllocationTail(allocation.allocationId());
        return Optional.of(new ReusedExtent(tail.end(),
                allocation.offset() + allocation.blocks() * BLOCK_BYTES, tail.source()));
    }

    private long sessionCloseBytes(long additional) throws IOException {
        long count = currentReusedAllocations().size() + additional;
        if (count > (65536 - 144 - 24) / 24) {
            throw new IOException("Reused allocation CLOSED witness capacity exhausted");
        }
        return count == 0 ? QUARANTINE_RECORD_BYTES : 144 + 24 + 24 * count;
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
            Set<UUID> retainedCheckpoints = retainedCheckpointIds();
            snapshot = new SegmentControlCheckpoint.Snapshot(appliedThrough, appliedOffset,
                    appliedOperations.values().stream().filter(operation ->
                            !(operation.command() instanceof CheckpointCommit)
                                    || operation.status() != Status.APPLIED_DURABLE
                                    || retainedCheckpoints.contains(operation.command().operationId()))
                            .map(CheckpointOperation::new).toList(), true);
            generation = checkpointGeneration + 1;
            slot = publishedCheckpoint == null ? 0 : 1 - publishedCheckpoint.slot();
            checkpointing = true;
        }
        boolean committed = false;
        try {
            snapshot = new SegmentControlCheckpoint.Snapshot(snapshot.through(), snapshot.logOffset(),
                    snapshot.operations().stream().sorted(Comparator.comparingLong(
                            SegmentControlCheckpoint.ImageRecord::sequence)).toList(),
                    snapshot.compactedCheckpointHistory());
            SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.reference(
                    generation, slot, snapshot, maxLogBytes);
            SegmentControlCheckpoint.write(config, identity, SegmentControlFormat.Owner.ARENA,
                    reference, snapshot, checkpointIo);
            SegmentControlCheckpoint.verify(config, identity, SegmentControlFormat.Owner.ARENA,
                    reference, snapshot, maxLogBytes);
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
        return rotate(rotationIo, null);
    }

    /** Runtime rotation retains the OPEN session and cursor, with actual DATA I/O and read pins excluded. */
    SegmentControlRotation.Root rotate(SegmentFileIo rotationIo,
                                       IsolatedSegmentFiles.ReclamationGuard protectedWriter) throws IOException {
        Objects.requireNonNull(rotationIo, "rotationIo");
        if (protectedWriter != null) {
            IsolatedSegmentFiles writer;
            synchronized (this) {
                writer = sessionOwner;
            }
            protectedWriter.requireHeld(writer, this);
        }
        synchronized (this) {
            requireControlWritable();
            long closeBytes = sessionCloseReserved ? sessionCloseBytes(0) : 0;
            if (rotating != null || checkpointing || writing || pending != null
                    || (sessionOwner != null || sessionCloseReserved) && protectedWriter == null
                    || checkpointGeneration > Long.MAX_VALUE - 2
                    || physicalGeneration.value() == Long.MAX_VALUE || appliedThrough > Long.MAX_VALUE - 3
                    || rotationReserveBytes() == 0
                    || nextOffset > maxLogBytes - 416 - closeBytes) {
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
                rotationRequested = false;
                appliedOffset = nextOffset;
            }
            if (previous != originalChannel) {
                previous.close();
                Files.delete(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                        previousGeneration));
                IsolatedSegmentFiles.syncDirectory(config.directory());
            }
            synchronized (this) {
                Set<UUID> retained = retainedCheckpointIds();
                appliedOperations.entrySet().removeIf(entry -> entry.getValue().command() instanceof CheckpointCommit
                        && entry.getValue().status() == Status.APPLIED_DURABLE && !retained.contains(entry.getKey()));
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

    /** Retain the active physical roots and latest predecessors; business results remain unchanged. */
    private Set<UUID> retainedCheckpointIds() {
        Set<UUID> retained = new HashSet<>();
        appliedOperations.values().stream().filter(operation -> operation.command() instanceof CheckpointCommit
                && operation.status() == Status.APPLIED_DURABLE)
                .sorted(Comparator.comparingLong(AppliedOperation::sequence).reversed()).limit(2)
                .forEach(operation -> retained.add(operation.command().operationId()));
        if (rotationRoot != null) {
            retained.add(rotationRoot.firstId());
            retained.add(rotationRoot.secondId());
        }
        return retained;
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

    synchronized boolean rotationRequired() {
        return rotationRequested && !closed && !failed;
    }

    private long rotationReserveBytes() {
        return maxLogBytes >= SegmentControlRotation.FIRST_RECORD + 416 + 192 ? 416 : 0;
    }

    private static int recordBytes(Command command) {
        return (144 + command.body().remaining() + 7) & ~7;
    }

    /** Called before exposing a new writer, under its existing directory exclusion. */
    void beginDataSession(IsolatedSegmentFiles writer) throws IOException {
        writer.requireStartingSession(this);
        DataSession command;
        synchronized (this) {
            requireQuiescentRecovery(writer.config(), writer.identity());
            if (sessionOwner != null || sessionEpoch == Long.MAX_VALUE
                    || (sessionEpoch == 0 && !allocations.isEmpty())
                    || pendingReconstruction != null && !verifiedReconstruction) {
                throw new IOException("DATA session cannot adopt an active or legacy allocated writer");
            }
            command = new DataSession(UUID.randomUUID(), new Generation(sessionEpoch + 1), sessionEpoch, true);
            long required = recordBytes(command) + sessionCloseBytes(0) + rotationReserveBytes();
            if (required > maxLogBytes - nextOffset) {
                if (rotationReserveBytes() == 0 || required > maxLogBytes - SegmentControlRotation.FIRST_RECORD) {
                    throw new SegmentControlCapacityException("Arena control log capacity exhausted");
                }
                // Recovery validated and synced DATA under this writer's lock; the session is still CLOSED.
                // Do not rotate Bookie here: the retained inventory is bound to its recovery cut.
                rotate();
            }
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
        if (healthy) {
            writer.requireClosedSession(this);
        }
        try {
            DataSession command;
            List<AllocationTail> tails = new ArrayList<>();
            if (healthy) {
                for (Allocation allocation : currentReusedAllocations()) {
                    tails.add(new AllocationTail(allocation.allocationId(), allocation.generation(),
                            writer.closedAllocationEnd(allocation, this)));
                }
            }
            synchronized (this) {
                if (sessionOwner != writer || !sessionOpen) {
                    throw new IOException("DATA writer does not own an open session");
                }
                if (!healthy) {
                    throw new IOException("Failed DATA session must remain unclosed");
                }
                requireQuiescentControl(writer.config(), writer.identity());
                command = new DataSession(UUID.randomUUID(), new Generation(sessionEpoch), sessionEpoch, false, tails);
                preparedSessionEnd = command;
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
                    preparedSessionEnd = null;
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

    static void resolveReconstruction(IsolatedSegmentConfig config, SegmentFileHeader identity, long maxLogBytes,
            SegmentFileIo io, SegmentDataFileLock guard, SegmentOwnedFiles files,
            SegmentClosedReconstruction.PublishedImage publication) throws IOException {
        try (var arena = reconstructionReplay(config, identity, maxLogBytes, io, guard, files, false)) {
            var command = reconstructionCommand(publication.receipt());
            if (arena.hasExactReconstruction(command)) {
                return;
            }
            if (!arena.canResolveSession() || arena.sessionEpoch != command.expectedGeneration()
                    || arena.appliedThrough != command.binding().arenaCut()
                    || arena.durableThrough != arena.appliedThrough) {
                throw new IOException("READY cannot resolve any other Arena epoch or cut");
            }
            arena.preparedSessionEnd = command;
            if (arena.submitBatch(List.of(command)).get(0).status() != Status.APPLIED_DURABLE) {
                throw new IOException("Original CLOSED reconstruction resolution did not apply");
            }
        }
    }

    static void requireReconstructionApplied(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentDataFileLock guard, SegmentOwnedFiles files,
            ProfileClosedReconstruction.Receipt receipt) throws IOException {
        try (var arena = reconstructionReplay(config, identity, maxLogBytes, new SegmentFileIo(), guard, files,
                true)) {
            if (!arena.hasExactReconstruction(reconstructionCommand(receipt))) {
                throw new IOException("Advanced Arena does not retain the exact original resolution");
            }
        }
    }

    private static SegmentArenaControlLog reconstructionReplay(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maxLogBytes, SegmentFileIo io, SegmentDataFileLock guard, SegmentOwnedFiles files,
            boolean inspection) throws IOException {
        guard.requireHeld(config.directory().toRealPath().resolve("arena-control.lock"));
        Path path = config.directory().resolve("arena-control");
        FileChannel original = inspection ? SegmentOwnedFiles.openRegularReadOnly(files, path)
                : SegmentOwnedFiles.open(files, path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS);
        var log = new SegmentArenaControlLog(config, identity, maxLogBytes, io, original, null, inspection, files);
        try {
            log.validateStorageFiles(identity);
            log.recover(identity);
            return log;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                log.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static ResolveClosedReconstruction reconstructionCommand(ProfileClosedReconstruction.Receipt receipt) {
        var admitted = receipt.admission();
        // The scalar slots anchor the first ledger and data-0. Both native digests bind the full vectors.
        long anchorLedger = admitted.members().get(0).ledger();
        return new ResolveClosedReconstruction(admitted.operation(), new Generation(admitted.scope().epoch()),
                new ReconstructionBinding(anchorLedger, admitted.arenaCut(), admitted.bookieCut(),
                        receipt.admissionBirth(), receipt.readyBirth(), receipt.ready().candidates().get(0).image(),
                        admitted.dependencies(), receipt.admissionDigest(), receipt.readyDigest()),
                admitted.historyMode() == ProfileClosedReconstruction.HistoryMode.RECONSTRUCTED_FULL_REUSE);
    }

    private boolean hasExactReconstruction(ResolveClosedReconstruction command) {
        var existing = appliedOperations.get(command.operationId());
        return existing != null && existing.status() == Status.APPLIED_DURABLE && existing.command().equals(command);
    }

    synchronized PendingReconstruction pendingReconstruction() {
        return pendingReconstruction;
    }

    synchronized void verifyFirstReconstructionOpen(PendingReconstruction expected,
            ProfileClosedReconstruction.Receipt receipt) throws IOException {
        if (!Objects.equals(expected, pendingReconstruction)
                || !expected.binding().equals(reconstructionCommand(receipt).binding())
                || !hasExactReconstruction(reconstructionCommand(receipt))) {
            throw new IOException("First OPEN must match the original verified reconstruction resolution");
        }
        verifiedReconstruction = true;
    }

    /** Only startup under the original DATA lock may settle an OPEN epoch without DATA write authority. */
    synchronized void resolveUnclosedDataSession(SegmentDataFileLock dataLock) throws IOException {
        requireQuiescentControl(config, identity);
        if (checkpointing || durableThrough != appliedThrough || !canResolveSession()) {
            throw new IOException("Unclosed DATA writer session prevents physical reopen");
        }
        var epoch = new Generation(sessionEpoch);
        Command command;
        if (neverAllocated()) {
            command = new ResolveNeverAllocatedSession(UUID.randomUUID(), epoch);
        } else if (grantFreeSession) {
            command = new ResolveGrantFreeSession(UUID.randomUUID(), epoch);
        } else {
            throw new IOException("Unclosed DATA writer session prevents physical reopen");
        }
        synchronized (dataLock) {
            dataLock.requireHeld(config.directory().toRealPath().resolve("isolated.lock"));
            preparedSessionEnd = command;
            try {
                if (submitBatch(List.of(command)).get(0).status() != Status.APPLIED_DURABLE) {
                    throw new IOException("Unclosed DATA session was not resolved");
                }
            } finally {
                preparedSessionEnd = null;
            }
        }
    }

    private boolean neverAllocated() {
        return allocationHighWatermark == 0 && issuedAllocationIds.isEmpty() && allocations.isEmpty()
                && currentAllocations.isEmpty() && streamStarts.isEmpty() && retiredRanges.isEmpty()
                && retiredBookieCut == -1 && retiredSlots.isEmpty() && selectedMoves.isEmpty()
                && committedCopies.isEmpty() && allocationTails.isEmpty();
    }

    private boolean canResolveSession() {
        if (sessionEpoch <= 0 || !sessionOpen || sessionOwner != null) {
            return false;
        }
        for (int file = 0; file < config.dataFiles(); file++) {
            if (dataBlocked[file] || durableQuarantine[file] || quarantineReserved[file]) {
                return false;
            }
        }
        return true;
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
            rotationRequested |= rotationReserveBytes() != 0
                    && reserved + rotationReserveBytes() <= maxLogBytes - SegmentControlRotation.FIRST_RECORD;
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
        Grant grant = currentAllocations.get(allocationId);
        Allocation allocation = grant == null ? null : grant.command();
        return allocation != null && allocation.file() == file && allocation.ownerShard() == ownerShard
                && allocation.ownerGeneration().value() == ownerGeneration
                && grant.generation() == allocationGeneration
                && offset >= allocation.offset()
                && bytes <= allocation.offset() + allocation.blocks() * BLOCK_BYTES - offset;
    }

    /** Select recovery pages from applied allocation ownership; full batch identity is checked separately. */
    synchronized boolean hasAppliedStreamAt(int file, long offset, int shard, long streamGeneration) {
        if (failed || closed || file < 0 || file >= config.dataFiles()
                || offset < FIRST_RECORD || offset >= config.bytesPerFile()
                || offset % config.alignment() != 0) {
            return false;
        }
        Grant grant = grantAt(slot(file, offset));
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
        if (request.shard() >= config.logicalShards() || !identity.ownsShard(request.shard())
                || request.blocks() > (config.bytesPerFile() - FIRST_RECORD) / BLOCK_BYTES) {
            throw new IllegalArgumentException("Pool request exceeds the fixed Arena configuration");
        }
    }

    /** Prefer whole retired extents, then fresh space; authority requires durable ordered apply. */
    synchronized List<Optional<Allocation>> planPools(List<PoolRequest> requests) throws IOException {
        return planPools(requests, 0);
    }

    /** Foreground pools leave capacity for one bounded maintenance destination in each fixed DATA file. */
    synchronized List<Optional<Allocation>> planPools(List<PoolRequest> requests, int maintenanceBlocks)
            throws IOException {
        requireWritable();
        if (maintenanceBlocks < 0) {
            throw new IllegalArgumentException("Negative maintenance reserve");
        }
        if (writing || pending != null || requests.isEmpty() || requests.size() > maxPoolRequests()) {
            throw new IOException("Arena cannot plan another pool batch");
        }
        long[] offsets = new long[config.dataFiles()];
        long[] available = new long[config.dataFiles()];
        for (int file = 0; file < offsets.length; file++) {
            offsets[file] = grantedHighWatermark(file);
            available[file] = (config.bytesPerFile() - offsets[file]) / BLOCK_BYTES;
        }
        for (Free free : retiredSlots.values()) {
            AllocationIdentity allocation = free.facts().allocation();
            available[allocation.file()] = Math.addExact(available[allocation.file()], allocation.blocks());
        }
        long id = allocationHighWatermark;
        List<Optional<Allocation>> planned = new ArrayList<>(requests.size());
        HashSet<Long> selectedRetirements = new HashSet<>();
        for (PoolRequest request : requests) {
            validatePoolRequest(request);
            int file = request.shard() % config.dataFiles();
            Free reusable = retiredSlots.values().stream().filter(free ->
                    free.facts().allocation().file() == file && free.facts().allocation().blocks() == request.blocks()
                            && !selectedRetirements.contains(free.facts().allocation().allocationId()))
                    .min(Comparator.comparingLong(free -> free.facts().allocation().offset())).orElse(null);
            if (dataBlocked[file] || available[file] - request.blocks() < maintenanceBlocks || reusable == null
                    && request.blocks() > (config.bytesPerFile() - offsets[file]) / BLOCK_BYTES) {
                spacePressure[file] |= maintenanceBlocks > 0 && !dataBlocked[file];
                planned.add(Optional.empty());
                continue;
            }
            if (id == Long.MAX_VALUE) {
                throw new IOException("Arena allocation identity exhausted");
            }
            Allocation allocation = new Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    UUID.randomUUID(), new Generation(1), reusable == null ? 0 : reusable.operationGeneration().value(),
                    file, request.shard(), request.stream(), ++id,
                    reusable == null ? offsets[file] : reusable.facts().allocation().offset(),
                    request.blocks(), request.firstSequence());
            planned.add(Optional.of(allocation));
            available[file] -= request.blocks();
            if (reusable == null) {
                offsets[file] += request.blocks() * BLOCK_BYTES;
            } else {
                selectedRetirements.add(reusable.facts().allocation().allocationId());
            }
        }
        return List.copyOf(planned);
    }

    synchronized boolean spacePressure(int file) {
        return spacePressure[file];
    }

    synchronized boolean hasSpacePressure() {
        for (boolean pressured : spacePressure) {
            if (pressured) {
                return true;
            }
        }
        return false;
    }

    /** Actual whole-block capacity, including an extent of the requested width and the maintenance reserve. */
    synchronized boolean restoreForegroundCapacity(int file, int blocks, int maintenanceBlocks) throws IOException {
        long fresh = (config.bytesPerFile() - grantedHighWatermark(file)) / BLOCK_BYTES;
        long available = fresh;
        for (Free free : retiredSlots.values()) {
            if (free.facts().allocation().file() == file) {
                available = Math.addExact(available, free.facts().allocation().blocks());
            }
        }
        boolean reusable = retiredSlots.values().stream().anyMatch(free ->
                free.facts().allocation().file() == file && free.facts().allocation().blocks() == blocks);
        if (dataFileAvailable(file) && available - blocks >= maintenanceBlocks && (fresh >= blocks || reusable)) {
            spacePressure[file] = false;
            return true;
        }
        return false;
    }

    /** Original successful control sequence, retained by checkpoint/rotation; never inferred from file offsets. */
    synchronized long appliedAllocationOrder(SegmentBatchFormat.Address address) {
        Grant grant = currentAllocations.get(address.allocation());
        Allocation command = grant == null ? null : grant.command();
        if (failed || closed || command == null
                || command.file() != address.fileIndex()
                || command.ownerShard() != address.shard()
                || !command.ownerGeneration().equals(address.streamGeneration())
                || command.generation() != address.generation().value()) {
            return -1;
        }
        return grant.allocationOrder();
    }

    synchronized boolean permitsAllocationAppend(long order) {
        return order > reconstructionNoAppendThrough;
    }

    /** Earliest whole-batch applied pool in control order; does not reserve or refill space. */
    synchronized Optional<SegmentBatchFormat.Address> appendAddress(int file, long minimumOrder,
                                                                   long minimumOffset, long rawFloor, int bytes,
                                                                   int shard, Generation stream, long sequence) {
        if (!dataFileAvailable(file) || bytes <= 0) {
            return Optional.empty();
        }
        SegmentBatchFormat.Address selected = null;
        long selectedOrder = Long.MAX_VALUE;
        for (Grant grant : currentAllocations.values()) {
            Allocation command = grant.command();
            if (command.file() != file || command.ownerShard() != shard
                    || !command.ownerGeneration().equals(stream)) {
                continue;
            }
            long order = grant.allocationOrder();
            if (order < minimumOrder || !permitsAllocationAppend(order)) {
                continue;
            }
            long offset = Math.max(command.offset(), order == minimumOrder ? minimumOffset : rawFloor);
            long end = command.offset() + command.blocks() * BLOCK_BYTES;
            if (offset <= end - bytes && order < selectedOrder) {
                selected = new SegmentBatchFormat.Address(file, shard, command.allocationId(),
                        new Generation(command.generation()), stream, sequence, offset);
                selectedOrder = order;
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
        return uniqueRestartStream(shard);
    }

    private Optional<SingleStream> uniqueRestartStream(int shard) throws IOException {
        if (shard < 0 || shard >= config.logicalShards()) {
            throw new IllegalArgumentException("Invalid restart shard");
        }
        if (streamStarts.keySet().stream().filter(stream -> stream.shard() == shard).count() > 1) {
            throw new IOException("Restart shard has multiple unretired stream generations");
        }
        return singleStreamForShard(shard);
    }

    /** Fresh-only refill never treats a retired slot as generation zero; reuse framing remains disabled. */
    synchronized long grantedHighWatermark(int file) throws IOException {
        requireWritable();
        if (file < 0 || file >= config.dataFiles()) {
            throw new IllegalArgumentException("Invalid DATA file");
        }
        long end = FIRST_RECORD;
        Map.Entry<Slot, Grant> current = allocations.floorEntry(new Slot(file, Long.MAX_VALUE));
        if (current != null && current.getKey().file() == file) {
            Allocation allocation = current.getValue().command();
            end = allocation.offset() + allocation.blocks() * BLOCK_BYTES;
        }
        Map.Entry<Slot, Free> retired = retiredSlots.floorEntry(new Slot(file, Long.MAX_VALUE));
        if (retired != null && retired.getKey().file() == file) {
            AllocationIdentity allocation = retired.getValue().facts().allocation();
            end = Math.max(end, allocation.offset() + allocation.blocks() * BLOCK_BYTES);
        }
        return end;
    }

    synchronized boolean matchesScope(IsolatedSegmentConfig otherConfig, SegmentFileHeader otherIdentity) {
        return !inspectionOnly && !closed && !failed && config.equals(otherConfig) && identity.equals(otherIdentity);
    }

    synchronized void requireQuiescentRecovery(IsolatedSegmentConfig otherConfig,
                                               SegmentFileHeader otherIdentity) throws IOException {
        requireQuiescentControl(otherConfig, otherIdentity);
        if (sessionOpen || sessionOwner != null) {
            throw new IOException("Unclosed DATA writer session prevents physical reopen");
        }
    }

    synchronized void requireHealthyManifestRestart(long bookieCut) throws IOException {
        if (!inspectionOnly || identity.manifest() == null || sessionOpen || sessionEpoch == 0
                || pendingReconstruction != null || bookieCut < requiredMoveBookieCut()
                || bookieCut < requiredRetiredBookieCut() || !inspection().recordedQuarantineFiles().isEmpty()
                || appliedOperations.values().stream().noneMatch(operation ->
                        operation.status() == Status.APPLIED_DURABLE
                                && operation.command() instanceof DataSession session && !session.open()
                                && session.operationGeneration().value() == sessionEpoch)) {
            throw new IOException("Manifest Arena requires a healthy closed session and its original Bookie cut");
        }
        for (int shard = 0; shard < config.logicalShards(); shard++) {
            if (identity.ownsShard(shard)) {
                uniqueRestartStream(shard);
            }
        }
    }

    synchronized Path manifestRestartJournal() {
        return rotationRoot == null ? config.directory().resolve("arena-control")
                : SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, rotationRoot.generation());
    }

    synchronized boolean manifestAccepting() {
        if (failed || closed || !sessionOpen || sessionOwner == null) {
            return false;
        }
        for (boolean blocked : dataBlocked) {
            if (blocked) {
                return false;
            }
        }
        return true;
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

    synchronized long requireStartupRecovered(IsolatedSegmentConfig expected, SegmentFileHeader storage)
            throws IOException {
        requireQuiescentControl(expected, storage);
        if (!sessionOpen || sessionOwner == null) {
            throw new IOException("Original recovered DATA session is not active");
        }
        return appliedThrough;
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
                SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.ARENA, slot, inspectionFiles);
                root = SegmentControlRotation.publication(config, SegmentControlFormat.Owner.ARENA,
                        slot, inspectionFiles);
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
            Path path = SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA,
                    rotationRoot.generation());
            channel = inspectionOnly
                    ? SegmentOwnedFiles.openRegularReadOnly(inspectionFiles, path)
                    : SegmentOwnedFiles.open(inspectionFiles, path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                            LinkOption.NOFOLLOW_LINKS);
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
                        config, SegmentControlFormat.Owner.ARENA, slot, inspectionFiles);
                if (rotationRoot.equals(SegmentControlRotation.publication(config,
                        SegmentControlFormat.Owner.ARENA, slot, inspectionFiles))) {
                    publications[slot] = reference;
                    valid = true;
                }
            } catch (IOException invalidPublication) {
                // Recovery still has to validate one complete publication and every current suffix record.
            }
            if (valid && !inspectionOnly) {
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
        if (!inspectionOnly) {
            channel.force(true);
        }
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
        resetRecoveredState();
        SegmentControlCheckpoint.replayRecords(config, identity, SegmentControlFormat.Owner.ARENA,
                reference, maxLogBytes, (operation, compactedHistory) -> {
            Command command = decodeCommand(operation.record(), operation.sequence());
            validateCommand(command);
            if (appliedOperations.containsKey(command.operationId())) {
                throw new IOException("Repeated operation in Arena checkpoint");
            }
            appliedThrough = operation.sequence();
            if (compactedHistory && command instanceof CheckpointCommit commit
                    && operation.status() == Status.APPLIED_DURABLE.ordinal() + 1) {
                if (commit.operationGeneration().value() <= checkpointGeneration
                        || commit.operationGeneration().value() >= reference.generation()) {
                    throw new IOException("Invalid compacted Arena checkpoint predecessor");
                }
                checkpointGeneration = commit.expectedGeneration();
            }
            if (apply(command).ordinal() + 1 != operation.status()) {
                throw new IOException("Arena checkpoint conditional result mismatch");
            }
        }, inspectionFiles);
        if (checkpointGeneration != reference.generation() - 1) {
            throw new IOException("Arena checkpoint does not retain its committed predecessor");
        }
        appliedThrough = reference.through();
        durableThrough = reference.through();
        appliedOffset = reference.logOffset();
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
                        config, SegmentControlFormat.Owner.ARENA, slot, inspectionFiles);
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
        if (!inspectionOnly) {
            channel.force(true);
        }
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
                        config, SegmentControlFormat.Owner.ARENA, reference.slot(), inspectionFiles))) {
                    continue;
                }
                restoreSnapshot(reference);
            } catch (IOException | IllegalArgumentException invalidCheckpoint) {
                resetRecoveredState();
                continue;
            }
            // A complete but formerly unsynced publication must not be exposed before recovery sync.
            if (!inspectionOnly) {
                SegmentControlCheckpoint.syncPublication(config, reference.slot());
            }
            return;
        }
        // All original control bytes remain mandatory and were verified above; full replay is the final fallback.
        resetRecoveredState();
    }

    private void resetRecoveredState() {
        retiredRanges.clear();
        retiredBookieCut = -1;
        retiredSlots.clear();
        allocationTails.clear();
        allocations.clear();
        currentAllocations.clear();
        issuedAllocationIds.clear();
        allocationHighWatermark = 0;
        streamStarts.clear();
        appliedOperations.clear();
        selectedMoves.clear();
        committedCopies.clear();
        Arrays.fill(dataBlocked, false);
        Arrays.fill(durableQuarantine, false);
        sessionEpoch = 0;
        sessionOpen = false;
        grantFreeSession = false;
        reconstructionNoAppendThrough = -1;
        pendingReconstruction = null;
        verifiedReconstruction = false;
        checkpointGeneration = 0;
        publishedCheckpoint = null;
        appliedThrough = -1;
        durableThrough = -1;
        appliedOffset = FIRST_RECORD;
    }

    private void validateStorageFiles(SegmentFileHeader identity) throws IOException {
        validateStorageFiles(identity, List.of());
    }

    private void validateStorageFiles(SegmentFileHeader identity, List<Path> readyDataPaths) throws IOException {
        if (Files.isSymbolicLink(config.directory())) {
            throw new IOException("Isolated storage directory cannot be a symlink");
        }
        validateIdentityFile(config.directory().resolve("superblock-a"), identity, -1, 4096);
        validateIdentityFile(config.directory().resolve("superblock-b"), identity, -2, 4096);
        if (identity.manifest() == null) {
            validateIdentityFile(config.directory().resolve("bookie-control"), identity, -3, -1);
        }
        for (int file = 0; file < config.dataFiles(); file++) {
            validateIdentityFile(readyDataPaths.isEmpty() ? IsolatedSegmentFiles.dataPath(config, file)
                    : readyDataPaths.get(file), identity, file,
                    config.bytesPerFile());
        }
    }

    private void validateIdentityFile(Path path, SegmentFileHeader identity, int fileId, long expectedSize)
            throws IOException {
        try (FileChannel input = SegmentOwnedFiles.openRegularReadOnly(inspectionFiles, path)) {
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
        if (parsed.type() == SegmentControlFormat.Type.MOVE) {
            if (parsed.expectedGeneration() != 0) {
                throw new IOException("MOVE header has an unsupported predecessor generation");
            }
            SegmentArenaMove move = SegmentArenaMove.decode(parsed.operationId(), parsed.operationGeneration(),
                    parsed.body(), sequence);
            try {
                validateCommand(move);
            } catch (IllegalArgumentException invalid) {
                throw new IOException("Invalid MOVE geometry", invalid);
            }
            return move;
        }
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
            if (body.remaining() < 16) {
                throw new IOException("Unsupported Arena DEVICE_STATE body");
            }
            int version = body.getInt();
            int action = body.getInt();
            if (version == 2 && body.remaining() == ResolveClosedReconstruction.BYTES - 8
                    && (action == ResolveClosedReconstruction.LEGACY_ACTION
                        || action == ResolveClosedReconstruction.RECONSTRUCT_TAILS_ACTION)) {
                try {
                    return ResolveClosedReconstruction.decode(parsed, body,
                            action == ResolveClosedReconstruction.RECONSTRUCT_TAILS_ACTION);
                } catch (IllegalArgumentException invalid) {
                    throw new IOException("Invalid CLOSED reconstruction resolution", invalid);
                }
            }
            if (version == 2 && action == 3 && body.remaining() >= 16) {
                long epoch = body.getLong();
                int count = body.getInt();
                if (body.getInt() != 0 || count < 1 || count > (65536 - 144 - 24) / 24
                        || body.remaining() != count * 24) {
                    throw new IOException("Invalid reused allocation CLOSED tails");
                }
                List<AllocationTail> tails = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    tails.add(new AllocationTail(body.getLong(), body.getLong(), body.getLong()));
                }
                try {
                    DataSession command = new DataSession(parsed.operationId(), parsed.operationGeneration(),
                            parsed.expectedGeneration(), false, tails);
                    if (epoch != command.operationGeneration().value()) {
                        throw new IllegalArgumentException("DATA session epoch differs from its header");
                    }
                    return command;
                } catch (IllegalArgumentException invalid) {
                    throw new IOException("Invalid reused allocation CLOSED session", invalid);
                }
            }
            if (version != 1 || body.remaining() != 8) {
                throw new IOException("Unsupported Arena DEVICE_STATE body");
            }
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
            if (action == 4) {
                long epoch = body.getLong();
                if (epoch != parsed.expectedGeneration() || epoch != parsed.operationGeneration().value()) {
                    throw new IOException("Never-allocated DATA session epoch differs from its header");
                }
                try {
                    return new ResolveNeverAllocatedSession(parsed.operationId(), parsed.operationGeneration());
                } catch (IllegalArgumentException invalid) {
                    throw new IOException("Invalid never-allocated DATA session resolution", invalid);
                }
            }
            if (action == 5) {
                long epoch = body.getLong();
                if (epoch != parsed.expectedGeneration() || epoch != parsed.operationGeneration().value()) {
                    throw new IOException("Grant-free DATA session epoch differs from its header");
                }
                try {
                    return new ResolveGrantFreeSession(parsed.operationId(), parsed.operationGeneration());
                } catch (IllegalArgumentException invalid) {
                    throw new IOException("Invalid grant-free DATA session resolution", invalid);
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
        if (parsed.type() == SegmentControlFormat.Type.FREE) {
            ByteBuffer body = parsed.body();
            int bodyBytes = body.remaining();
            if (bodyBytes != 128 && bodyBytes != 136) {
                throw new IOException("Unsupported whole-allocation FREE body");
            }
            int version = body.getInt();
            if ((version != 1 || bodyBytes != 128) && (version != 2 || bodyBytes != 136) || body.getInt() != 0) {
                throw new IOException("Unsupported whole-allocation FREE version");
            }
            try {
                AllocationIdentity target = new AllocationIdentity(body.getInt(), body.getInt(),
                        new Generation(body.getLong()), body.getLong(), body.getLong(), body.getInt(),
                        decodeFreeReservedAndFirst(body));
                long order = body.getLong();
                long first = body.getLong();
                long through = body.getLong();
                long records = body.getLong();
                long bookieCut = body.getLong();
                long arenaCut = version == 2 ? body.getLong() : -1;
                byte[] digest = new byte[32];
                body.get(digest);
                Free free = new Free(parsed.operationId(), parsed.operationGeneration(), parsed.expectedGeneration(),
                        new FreeFacts(target, order, first, through, records, bookieCut, arenaCut,
                                HexFormat.of().formatHex(digest)), null);
                validateCommand(free);
                if (order >= sequence || arenaCut >= sequence || version == 2 && arenaCut < 0) {
                    throw new IllegalArgumentException("FREE predecessor is not an earlier allocation");
                }
                return free;
            } catch (IllegalArgumentException invalid) {
                throw new IOException("Invalid whole-allocation FREE command", invalid);
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

    private static long decodeFreeReservedAndFirst(ByteBuffer body) {
        if (body.getInt() != 0) {
            throw new IllegalArgumentException("Unsupported FREE allocation extension");
        }
        return body.getLong();
    }

    private void validateRange(Allocation command) {
        validateAllocationIdentity(AllocationIdentity.of(command));
        if (command.expectedGeneration() == 1) {
            throw new IllegalArgumentException("Reuse requires an applied FREE generation");
        }
    }

    private void validateAllocationIdentity(AllocationIdentity command) {
        if (command.file() < 0 || command.file() >= config.dataFiles()
                || command.ownerShard() < 0 || command.ownerShard() >= config.logicalShards()
                || !identity.ownsShard(command.ownerShard())
                || command.file() != command.ownerShard() % config.dataFiles()
                || command.ownerGeneration() == null || command.allocationId() <= 0 || command.blocks() < 1
                || command.streamFirstSequence() < 0 || command.streamFirstSequence() == Long.MAX_VALUE
                || command.offset() < FIRST_RECORD || (command.offset() - FIRST_RECORD) % BLOCK_BYTES != 0
                || command.offset() > config.bytesPerFile()
                || command.blocks() > (config.bytesPerFile() - command.offset()) / BLOCK_BYTES) {
            throw new IllegalArgumentException("Invalid or unsupported Arena allocation range/generation");
        }
    }

    private void validateCommand(Command command) {
        if (command instanceof Allocation allocation) {
            validateRange(allocation);
        } else if (command instanceof Free free) {
            validateAllocationIdentity(free.facts().allocation());
        } else if (command instanceof SegmentArenaMove move) {
            for (SegmentArenaMove.Entry entry : move.entries()) {
                validateMoveLocation(entry.source().location());
                validateMoveLocation(entry.target());
            }
        } else if (command instanceof FileQuarantine quarantine && quarantine.file() >= config.dataFiles()) {
            throw new IllegalArgumentException("Invalid quarantined DATA file");
        }
    }

    private Status apply(Command command) throws IOException {
        AppliedOperation prior = appliedOperations.get(command.operationId());
        if (prior != null) {
            if (!prior.command().equals(command)) {
                return Status.INCOMPATIBLE;
            }
            if (prior.status() == Status.APPLIED_DURABLE && command instanceof Free free) {
                AllocationIdentity target = free.facts().allocation();
                if (!free.equals(retiredSlots.get(slot(target.file(), target.offset())))) {
                    return Status.CONDITION_FAILED;
                }
            }
            if (prior.status() == Status.APPLIED_DURABLE && command instanceof SegmentArenaMove move
                    && move.entries().stream().anyMatch(entry -> {
                        SelectedMove selected = selectedMoves.get(new EntryKey(entry.target().context(),
                                entry.target().entryId()));
                        return selected == null || !selected.moveId().equals(move.operationId())
                                || !selected.moveGeneration().equals(move.operationGeneration());
                    })) {
                return Status.CONDITION_FAILED;
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
        if (command instanceof ResolveNeverAllocatedSession || command instanceof ResolveGrantFreeSession) {
            if (sessionEpoch != command.expectedGeneration() || !canResolveSession()
                    || (command instanceof ResolveNeverAllocatedSession ? !neverAllocated() : !grantFreeSession)) {
                throw new IOException("Invalid resolution of an unclosed DATA writer session");
            }
            sessionOpen = false;
            grantFreeSession = false;
            appliedOperations.put(command.operationId(),
                    new AppliedOperation(command, Status.APPLIED_DURABLE, appliedThrough));
            return Status.APPLIED_DURABLE;
        }
        if (command instanceof ResolveClosedReconstruction resolution) {
            var binding = resolution.binding();
            if (identity.manifest() != null || sessionEpoch != resolution.expectedGeneration() || !canResolveSession()
                    || binding.arenaCut() != appliedThrough - 1
                    || currentAllocations.isEmpty() || binding.image().length() != config.bytesPerFile()
                    || binding.arenaCut() <= reconstructionNoAppendThrough) {
                throw new IOException("Invalid resolution of the original CLOSED reconstruction epoch");
            }
            if (resolution.reconstructTails()) {
                var reused = currentReusedAllocations();
                if (hasFullHealthyTails(reused.size(), observeReconstructionTails())) {
                    throw new IOException("Reconstructed tails require a reused extent without a full healthy CLOSE");
                }
                for (var allocation : reused) {
                    allocationTails.put(allocation.allocationId(), new AllocationTail(allocation.allocationId(),
                            allocation.generation(), allocation.offset() + allocation.blocks() * BLOCK_BYTES,
                            TailSource.RECONSTRUCTION));
                }
            }
            sessionOpen = false;
            grantFreeSession = false;
            reconstructionNoAppendThrough = binding.arenaCut();
            pendingReconstruction = new PendingReconstruction(resolution.operationId(), sessionEpoch, binding);
            appliedOperations.put(command.operationId(), new AppliedOperation(command, Status.APPLIED_DURABLE,
                    appliedThrough));
            return Status.APPLIED_DURABLE;
        }
        if (command instanceof DataSession session) {
            boolean allowed = session.expectedGeneration() == sessionEpoch
                    && (session.open() ? !sessionOpen && (sessionEpoch != 0 || allocations.isEmpty()) : sessionOpen);
            for (boolean quarantined : durableQuarantine) {
                allowed &= !quarantined;
            }
            if (!session.open()) {
                List<Allocation> reused = currentReusedAllocations();
                allowed &= session.tails().size() == reused.size();
                for (int i = 0; allowed && i < reused.size(); i++) {
                    Allocation allocation = reused.get(i);
                    AllocationTail tail = session.tails().get(i);
                    allowed = tail.allocationId() == allocation.allocationId()
                            && tail.generation() == allocation.generation() && tail.end() >= allocation.offset()
                            && tail.end() <= allocation.offset() + allocation.blocks() * BLOCK_BYTES
                            && tail.end() % config.alignment() == 0;
                }
            }
            Status status = allowed ? Status.APPLIED_DURABLE : Status.CONDITION_FAILED;
            if (allowed) {
                // A positive predecessor epoch is closed only by a validated CLOSE or resolution.
                grantFreeSession = session.open() && sessionEpoch > 0
                        && allocations.isEmpty() && currentAllocations.isEmpty();
                sessionEpoch = session.operationGeneration().value();
                sessionOpen = session.open();
                if (session.open()) {
                    pendingReconstruction = null;
                    verifiedReconstruction = false;
                }
                if (!session.open()) {
                    session.tails().forEach(tail -> allocationTails.put(tail.allocationId(), tail));
                }
            }
            appliedOperations.put(command.operationId(), new AppliedOperation(command, status, appliedThrough));
            return status;
        }
        if (command instanceof SegmentArenaMove move) {
            return applyMove(move);
        }
        return command instanceof Free free ? applyFree(free) : applyAllocation((Allocation) command);
    }

    private void validateMoveLocation(SegmentPointRead.Location location) {
        SegmentBatchFormat.Address address = location.address();
        if (location.context().ledgerId() < 0 || location.entryId() < 0
                || address.shard() >= config.logicalShards() || address.fileIndex() >= config.dataFiles()
                || address.fileIndex() != address.shard() % config.dataFiles()
                || SegmentOwnerLoop.shardFor(location.context().ledgerId(), config.logicalShards()) != address.shard()
                || location.batchLength() < 4096 || location.batchLength() > SegmentBatchFormat.MAX_BATCH
                || location.batchLength() % config.alignment() != 0
                || address.offset() > config.bytesPerFile() - location.batchLength()
                || location.recordOffset() < address.offset() + SegmentBatchFormat.HEADER
                || location.recordOffset() > address.offset() + location.batchLength() - location.recordLength()) {
            throw new IllegalArgumentException("Invalid MOVE physical geometry");
        }
        try {
            SegmentPointRead.scratchBytes(location);
        } catch (IOException invalid) {
            throw new IllegalArgumentException("Invalid MOVE record span", invalid);
        }
    }

    private boolean currentMoveGrant(SegmentPointRead.Location location, long order) {
        SegmentBatchFormat.Address address = location.address();
        return !durableQuarantine[address.fileIndex()] && appliedAllocationOrder(address) == order
                && hasAppliedGrant(address.fileIndex(), address.offset(), location.batchLength(), address.shard(),
                        address.streamGeneration().value(), address.allocation(), address.generation().value());
    }

    private Status applyMove(SegmentArenaMove move) {
        boolean allowed = true;
        for (SegmentArenaMove.Entry entry : move.entries()) {
            SelectedMove previous = selectedMoves.get(new EntryKey(entry.target().context(), entry.target().entryId()));
            allowed &= previous == null ? entry.source().expectedSelector() == -1
                    : previous.sequence() == entry.source().expectedSelector()
                            && previous.target().equals(entry.source().location());
            allowed &= currentMoveGrant(entry.source().location(), entry.sourceOrder())
                    && currentMoveGrant(entry.target(), entry.targetOrder());
        }
        Status status = allowed ? Status.APPLIED_DURABLE : Status.CONDITION_FAILED;
        SegmentArenaMove retained = move.replayFacts();
        if (allowed) {
            for (SegmentArenaMove.Entry entry : retained.entries()) {
                selectedMoves.put(new EntryKey(entry.target().context(), entry.target().entryId()),
                        new SelectedMove(retained.operationId(), retained.operationGeneration(),
                                retained.bookieCut(), entry.target(), entry.entrySha256(), appliedThrough));
                committedCopies.computeIfAbsent(entry.target().address().allocation(), ignored -> new HashMap<>())
                        .put(new CopyKey(entry.target(), retained.operationId(),
                                retained.operationGeneration().value()), entry.entrySha256());
            }
        }
        appliedOperations.put(move.operationId(), new AppliedOperation(retained, status, appliedThrough));
        return status;
    }

    private Status applyFree(Free free) {
        FreeFacts facts = free.facts();
        AllocationIdentity target = facts.allocation();
        Grant predecessor = currentAllocations.get(target.allocationId());
        Slot start = slot(target.file(), target.offset());
        boolean allowed = predecessor != null && AllocationIdentity.of(predecessor.command()).equals(target)
                && predecessor.generation() == free.expectedGeneration() && !durableQuarantine[target.file()]
                && predecessor.allocationOrder() == facts.allocationOrder()
                && (facts.arenaCut() < 0 || facts.arenaCut() == appliedThrough - 1)
                && predecessor.equals(allocations.get(start));
        Status status = allowed ? Status.APPLIED_DURABLE : Status.CONDITION_FAILED;
        Free retained = free.replayFacts();
        if (allowed) {
            allocations.remove(start);
            retiredSlots.put(start, retained);
            currentAllocations.remove(target.allocationId());
            committedCopies.remove(target.allocationId());
            addRetirement(facts);
            allocationTails.remove(target.allocationId());
            selectedMoves.values().removeIf(selected ->
                    selected.target().address().allocation() == target.allocationId());
        }
        appliedOperations.put(free.operationId(), new AppliedOperation(retained, status, appliedThrough));
        return status;
    }

    private Status applyAllocation(Allocation command) {
        if (durableQuarantine[command.file()] || wasAllocationIdIssued(command.allocationId())
                || command.expectedGeneration() > 0 && (!sessionOpen || sessionEpoch == 0)) {
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
        Slot start = slot(command.file(), command.offset());
        Slot last = new Slot(command.file(), start.index() + command.blocks() - 1);
        Map.Entry<Slot, Grant> current = allocations.floorEntry(last);
        boolean occupied = current != null && current.getKey().file() == command.file()
                && current.getKey().index() + current.getValue().command().blocks() > start.index();
        Map.Entry<Slot, Free> retired = retiredSlots.floorEntry(last);
        boolean overlapsRetired = retired != null && retired.getKey().file() == command.file()
                && retired.getKey().index() + retired.getValue().facts().allocation().blocks() > start.index();
        boolean matching = command.expectedGeneration() == 0 ? !overlapsRetired
                : overlapsRetired && retired.getKey().equals(start)
                        && retired.getValue().operationGeneration().value() == command.expectedGeneration()
                        && retired.getValue().facts().allocation().blocks() == command.blocks();
        if (occupied || !matching) {
            appliedOperations.put(command.operationId(),
                    new AppliedOperation(command, Status.CONDITION_FAILED, appliedThrough));
            return Status.CONDITION_FAILED;
        }
        Grant grant = new Grant(command, command.generation(), appliedThrough);
        allocations.put(start, grant);
        retiredSlots.remove(start);
        currentAllocations.put(command.allocationId(), grant);
        grantFreeSession = false;
        recordIssuedAllocationId(command.allocationId());
        streamStarts.putIfAbsent(stream, command.streamFirstSequence());
        appliedOperations.put(command.operationId(),
                new AppliedOperation(command, Status.APPLIED_DURABLE, appliedThrough));
        return Status.APPLIED_DURABLE;
    }

    private boolean wasAllocationIdIssued(long id) {
        Map.Entry<Long, Long> range = issuedAllocationIds.floorEntry(id);
        return range != null && id <= range.getValue();
    }

    private void recordIssuedAllocationId(long id) {
        long first = id;
        long through = id;
        Map.Entry<Long, Long> previous = issuedAllocationIds.floorEntry(id);
        if (previous != null && previous.getValue() + 1 == id) {
            first = previous.getKey();
            issuedAllocationIds.remove(first);
        }
        Map.Entry<Long, Long> next = issuedAllocationIds.ceilingEntry(id);
        if (id != Long.MAX_VALUE && next != null && id + 1 == next.getKey()) {
            through = next.getValue();
            issuedAllocationIds.remove(next.getKey());
        }
        issuedAllocationIds.put(first, through);
        allocationHighWatermark = Math.max(allocationHighWatermark, id);
    }

    private Grant grantAt(Slot location) {
        Map.Entry<Slot, Grant> entry = allocations.floorEntry(location);
        return entry != null && entry.getKey().file() == location.file()
                && location.index() - entry.getKey().index() < entry.getValue().command().blocks()
                ? entry.getValue() : null;
    }

    private Free retiredExtentAt(Slot location) {
        Map.Entry<Slot, Free> entry = retiredSlots.floorEntry(location);
        return entry != null && entry.getKey().file() == location.file()
                && location.index() - entry.getKey().index() < entry.getValue().facts().allocation().blocks()
                ? entry.getValue() : null;
    }

    private long retiredBlockCount() {
        long count = 0;
        for (Free free : retiredSlots.values()) {
            count = Math.addExact(count, free.facts().allocation().blocks());
        }
        return count;
    }

    private Slot slot(int file, long offset) {
        return new Slot(file, (offset - FIRST_RECORD) / BLOCK_BYTES);
    }

    private void requireOpenHistory() {
        if (closed) {
            throw new IllegalStateException("Closed Arena control history must be replayed by a new owner");
        }
    }

    private void requireControlWritable() throws IOException {
        requireWritable();
        if (rotating != null && rotating != Thread.currentThread()) {
            throw new IOException("Arena control rotation is in flight");
        }
    }

    private void requireWritable() throws IOException {
        if (inspectionOnly) {
            throw new IOException("Arena inspection does not provide writable authority");
        }
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
        if (writerLock != null) {
            try {
                writerLock.close();
            } catch (IOException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        // A closed service may retain this owner, but replay needs a new owner and the durable files.
        allocations = new TreeMap<>();
        currentAllocations = new HashMap<>();
        issuedAllocationIds = new TreeMap<>();
        retiredRanges = new ArrayList<>();
        retiredSlots = new TreeMap<>();
        allocationTails = new HashMap<>();
        streamStarts = new HashMap<>();
        appliedOperations = new HashMap<>();
        selectedMoves = new HashMap<>();
        committedCopies = new HashMap<>();
        pending = null;
        preparedSessionEnd = null;
        if (first != null) {
            throw first;
        }
    }
}

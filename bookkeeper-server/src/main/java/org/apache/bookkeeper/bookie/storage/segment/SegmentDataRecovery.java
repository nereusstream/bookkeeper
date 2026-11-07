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
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;

/** Quiesced isolated DATA validation; zero pages and recovery sync never grant reuse or entry authority. */
final class SegmentDataRecovery {
    /** Inventory survives runtime coordinate retirement, so reserve its old contexts independently of hot keys. */
    static long inventoryCapacity(IsolatedSegmentConfig config, int coordinates) throws IOException {
        if (coordinates < 1) {
            throw new IllegalArgumentException("Positive inventory coordinate geometry required");
        }
        var layout = SegmentControlCapacity.Layout.hotSpot();
        long location = SegmentControlCapacity.sum(layout.object(SegmentPointRead.Location.class),
                layout.object(SegmentBatchFormat.Address.class),
                SegmentControlCapacity.times(2, layout.object(Generation.class)), layout.context());
        long stream = SegmentControlCapacity.sum(layout.object(StreamKey.class), layout.object(StreamPrefix.class),
                layout.object(Generation.class));
        return SegmentControlCapacity.sum(layout.object(PhysicalInventory.class), layout.object(Scan.class),
                layout.immutableList(coordinates), layout.immutableList(config.dataFiles()),
                layout.immutableMap(config.logicalShards()), SegmentControlCapacity.times(coordinates, location),
                SegmentControlCapacity.times(config.dataFiles(), layout.object(Long.class)),
                SegmentControlCapacity.times(config.logicalShards(), stream));
    }

    private static long scanCapacity(IsolatedSegmentConfig config, SegmentArenaControlLog authority,
                                     int entries, int streams, boolean seeded) throws IOException {
        return scanCapacity(SegmentControlCapacity.Layout.hotSpot(), entries,
                Math.max(streams, config.logicalShards()), config.dataFiles(),
                authority.recoveryMoveCount(), authority.recoveryRetiredRangeCount(),
                authority.recoveryAllocationCount(), seeded);
    }

    /** One quiesced scan, including the old optional seed until the new inventory passes all checks. */
    static long scanCapacity(SegmentControlCapacity.Layout layout, int entries, int streams, int files,
                             long moves, long retired, long allocations, boolean seeded) throws IOException {
        if (entries < 1 || streams < 1 || files < 1 || moves < 0 || retired < 0 || allocations < 0) {
            throw new IllegalArgumentException("Positive bounded recovery geometry required");
        }
        try {
            long location = SegmentControlCapacity.sum(layout.object(SegmentPointRead.Location.class),
                    layout.object(SegmentBatchFormat.Address.class),
                    SegmentControlCapacity.times(2, layout.object(Generation.class)), layout.context());
            long stream = SegmentControlCapacity.sum(SegmentControlCapacity.times(2, layout.object(StreamKey.class)),
                    layout.object(Generation.class), layout.object(StreamProgress.class), layout.object(TreeMap.class),
                    layout.object(StreamPrefix.class));
            long range = SegmentControlCapacity.sum(layout.treeEntry(), layout.object(Long.class),
                    layout.object(StreamProgress.SequenceRange.class));
            return SegmentControlCapacity.sum(
                    // The output graph coexists with mutable scan collections before this credit is retired.
                    layout.object(PhysicalInventory.class), layout.object(Scan.class),
                    layout.immutableList(entries), layout.immutableList(files), layout.immutableMap(streams),
                    SegmentControlCapacity.times(files, layout.object(Long.class)),
                    SegmentControlCapacity.times(SegmentControlCapacity.sum(entries, moves), location),
                    SegmentControlCapacity.times(4, layout.hashMap()),
                    SegmentControlCapacity.times(2, layout.hashSet()),
                    SegmentControlCapacity.times(SegmentControlCapacity.sum(3L * streams, 2L * entries, moves),
                            layout.hashEntry()),
                    SegmentControlCapacity.times(streams, stream),
                    SegmentControlCapacity.times(SegmentControlCapacity.sum(allocations, retired), range),
                    SegmentControlCapacity.times(entries,
                            SegmentControlCapacity.sum(layout.object(Integer.class), layout.object(Coordinate.class))),
                    // ArrayList growth/copy, prefix Map.copyOf bridges, and borrowed Arena snapshots.
                    SegmentControlCapacity.times(2, layout.list()),
                    SegmentControlCapacity.times(4, layout.refs(entries)),
                    SegmentControlCapacity.times(2, layout.refs(files)),
                    layout.refs(streams), layout.refs(2L * streams),
                    layout.snapshot(moves, 0), layout.snapshot(retired, 0),
                    // One selected-seed record can be verified with scratch while all retained roots are live.
                    SegmentBatchParser.workspaceCapacity(layout, 1), layout.object(SegmentPointRead.Result.class),
                    SegmentControlCapacity.times(8, layout.buffer()),
                    SegmentControlCapacity.times(8, layout.bytes(0)),
                    SegmentControlCapacity.times(4, layout.object(Long.class)),
                    SegmentControlCapacity.times(16, layout.captured(8)),
                    seeded ? SegmentDerivedIndex.recoveryCapacity(layout, entries, streams, moves) : 0);
        } catch (ArithmeticException | IllegalArgumentException unsupported) {
            throw new IOException("Recovery workspace exceeds the supported heap model", unsupported);
        }
    }

    static long compactionCapacity(IsolatedSegmentConfig config, int coordinates, int bufferBytes) throws IOException {
        return compactionCapacity(config, coordinates, bufferBytes, SegmentControlCapacity.Layout.hotSpot());
    }

    /**
     * One compaction's private plans and bounded preparation graphs, excluding pooled backing and opaque runtimes.
     * Applied MOVE/FREE facts may remain under the Arena CONTROL owner after the operation drops its own roots.
     */
    static long compactionCapacity(IsolatedSegmentConfig config, int coordinates, int bufferBytes,
                                   SegmentControlCapacity.Layout layout) throws IOException {
        if (coordinates < 1 || bufferBytes < config.alignment() || bufferBytes % config.alignment() != 0) {
            throw new IllegalArgumentException("Positive compaction geometry and aligned workspace required");
        }
        try {
            long entries = coordinates;
            long victims = 16;
            int capacity = Math.min(bufferBytes, SegmentBatchFormat.MAX_BATCH);
            // Obsolete/terminal records still need parsing, even when the retained live plan has a small C.
            int scanRecords = Math.min(SegmentBatchFormat.MAX_RECORDS,
                    (capacity - SegmentBatchFormat.HEADER - SegmentBatchFormat.FOOTER)
                            / (SegmentBatchFormat.RECORD_HEADER + SegmentBatchFormat.ENTRY_OVERHEAD));
            long chunk = Math.min(entries, scanRecords);
            long view = Math.max(layout.buffer(), layout.object("java.nio.DirectByteBufferR"));
            long location = SegmentControlCapacity.sum(layout.object(SegmentPointRead.Location.class),
                    layout.object(SegmentBatchFormat.Address.class),
                    SegmentControlCapacity.times(2, layout.object(Generation.class)), layout.context());
            long source = SegmentControlCapacity.sum(layout.object(CompactionEntry.class),
                    layout.object(SegmentArenaMove.Source.class), location, layout.string(64));
            // Each nonempty batch owns one immutable entry-list shell; all batch arrays together hold C entries.
            long plan = SegmentControlCapacity.sum(layout.object(CompactionPlan.class),
                    layout.immutableList(victims), layout.immutableList(entries), layout.refs(entries),
                    SegmentControlCapacity.times(entries, SegmentControlCapacity.sum(source,
                            layout.object(CompactionBatch.class), layout.immutableList(0))));
            // Tail measurement keeps the old field plan while making a new capture. Its final wrapper shares rows.
            long plans = SegmentControlCapacity.sum(SegmentControlCapacity.times(2, plan),
                    layout.object(CompactionPlan.class), layout.object(CompactionTail.class));
            // The two growing ArrayLists retain at most old/new arrays of max(10, floor(1.5*C)) wide slots.
            long grown = Math.max(10, SegmentControlCapacity.sum(entries, entries / 2));
            long capture = SegmentControlCapacity.sum(layout.object(CompactionCapture.class), layout.hashSet(),
                    SegmentControlCapacity.times(entries,
                            SegmentControlCapacity.sum(layout.object(CompactionKey.class), layout.hashEntry())),
                    SegmentControlCapacity.times(2, layout.list()),
                    SegmentControlCapacity.times(4, layout.refs(grown)), layout.refs(entries), layout.refs(chunk),
                    layout.object("java.util.ArrayList$SubList"));
            // COPY destinations have independently parsed contexts/locations and newly measured digest strings.
            long copy = SegmentControlCapacity.sum(layout.object(SegmentBatchFormat.RelocationBuilder.class), view,
                    layout.object(Generation.class), layout.object(java.util.UUID.class),
                    layout.object(SegmentDataPhysicalDriver.DurableRelocation.class), layout.immutableList(chunk),
                    SegmentControlCapacity.times(chunk, SegmentControlCapacity.sum(
                            layout.object(SegmentDataPhysicalDriver.RelocationCopy.class),
                            location, layout.string(64))),
                    layout.list(), SegmentControlCapacity.times(3, layout.refs(chunk)));
            // MOVE rows borrow the plan Source and COPY target/digest; only the row, keys and list bridges are new.
            long move = SegmentControlCapacity.sum(layout.object(SegmentArenaMove.class), layout.immutableList(chunk),
                    layout.hashSet(), SegmentControlCapacity.times(chunk, SegmentControlCapacity.sum(
                            layout.object(SegmentArenaMove.Entry.class),
                            layout.object("org.apache.bookkeeper.bookie.storage.segment.SegmentArenaMove$Key"),
                            layout.hashEntry())), SegmentControlCapacity.times(2, layout.list()),
                    SegmentControlCapacity.times(4, layout.refs(chunk)));
            long free = SegmentControlCapacity.sum(layout.immutableList(victims),
                    SegmentControlCapacity.times(victims, SegmentControlCapacity.sum(
                            layout.object(SegmentArenaControlLog.FreeTarget.class),
                            layout.object(SegmentArenaControlLog.Free.class),
                            SegmentControlCapacity.times(2, layout.object(SegmentArenaControlLog.FreeFacts.class)),
                            layout.object(SegmentArenaControlLog.AllocationIdentity.class),
                            layout.object(java.util.UUID.class), layout.object(Generation.class), layout.string(64),
                            SegmentControlCapacity.times(2,
                                    layout.object(java.util.concurrent.atomic.AtomicBoolean.class)))));
            // ends/dead/deadEnds, then FREE victims/orders/commands; selection and immutable bridges coexist.
            long victimLists = SegmentControlCapacity.sum(SegmentControlCapacity.times(6, layout.list()),
                    SegmentControlCapacity.times(12, layout.refs(victims + victims / 2)),
                    SegmentControlCapacity.times(3, layout.immutableList(victims)),
                    SegmentControlCapacity.times(2 * victims, layout.object(Long.class)));
            // A single maintenance batch parser is already reserved by Driver.parsingCapacity(). FREE can
            // retain that victim batch while verifying one successor with another point-record parser.
            long nestedRead = SegmentControlCapacity.sum(SegmentBatchParser.workspaceCapacity(layout, 1),
                    layout.object(SegmentPointRead.Result.class), layout.hashSet(),
                    SegmentControlCapacity.times(scanRecords, layout.hashEntry()),
                    SegmentControlCapacity.times(2, layout.object(Optional.class)));
            long fixed = SegmentControlCapacity.sum(
                    layout.object("org.apache.bookkeeper.bookie.storage.segment."
                            + "IsolatedSegmentStorage$CompactionOperation"),
                    layout.object(IsolatedSegmentStorage.CompactionRequest.class),
                    layout.object(IsolatedSegmentStorage.CompactionResult.class),
                    layout.object(java.util.concurrent.atomic.AtomicReference.class),
                    layout.object(ProfileReceiveBudget.DataWorkingSet.class),
                    layout.object("org.apache.bookkeeper.proto.profile.ProfileReceiveBudget$Credit"),
                    // A full capture rejects its next source before retaining it in entries/distinct.
                    source,
                    layout.object(SegmentArenaControlLog.Allocation.class),
                    SegmentControlCapacity.times(2, layout.object(IsolatedSegmentFiles.ReclamationGuard.class)),
                    SegmentControlCapacity.times(2, layout.object(SegmentBookieControlLog.ReclamationLease.class)),
                    SegmentControlCapacity.times(2, layout.object(CompactionEnd.class)),
                    layout.bytes(1), layout.bytes(32), layout.string(64));
            return SegmentControlCapacity.sum(plans, capture, copy, move, free, victimLists, nestedRead, fixed);
        } catch (ArithmeticException | IllegalArgumentException unsupported) {
            throw new IOException("Compaction workspace exceeds the supported heap model", unsupported);
        }
    }

    record CompactionEntry(SegmentArenaMove.Source source, String entrySha256) { }
    record CompactionBatch(List<CompactionEntry> entries, int bytes) {
        CompactionBatch {
            entries = List.copyOf(entries);
        }
    }
    private record CompactionKey(ProfileLedgerContext context, long entryId) { }

    record CompactionTail(SegmentArenaControlLog.Allocation allocation, long offset) { }
    private record CompactionEnd(long offset, long through) { }

    static final class CompactionPlan {
        private final List<SegmentArenaControlLog.Allocation> victims;
        private final List<CompactionBatch> batches;
        private final long sourceBytes;
        private final long batchBytes;
        private final long bookieCut;
        private final CompactionTail tail;

        private CompactionPlan(List<SegmentArenaControlLog.Allocation> victims, List<CompactionBatch> batches,
                               long sourceBytes, long bookieCut) {
            this(victims, batches, sourceBytes, bookieCut, null);
        }

        private CompactionPlan(List<SegmentArenaControlLog.Allocation> victims, List<CompactionBatch> batches,
                               long sourceBytes, long bookieCut, CompactionTail tail) {
            this.victims = List.copyOf(victims);
            this.batches = List.copyOf(batches);
            this.sourceBytes = sourceBytes;
            this.bookieCut = bookieCut;
            this.tail = tail;
            batchBytes = batches.stream().mapToLong(CompactionBatch::bytes).sum();
        }

        List<SegmentArenaControlLog.Allocation> victims() {
            return victims;
        }

        List<CompactionBatch> batches() {
            return batches;
        }

        int destinationBlocks() {
            return Math.toIntExact((batchBytes + SegmentArenaControlLog.BLOCK_BYTES - 1)
                    / SegmentArenaControlLog.BLOCK_BYTES);
        }

        boolean hasNetGain() {
            return sourceBytes > (tail == null ? destinationBlocks() * SegmentArenaControlLog.BLOCK_BYTES : 0);
        }

        CompactionTail tail() {
            return tail;
        }
    }

    @FunctionalInterface
    private interface CompactionVisitor {
        void accept(SegmentArenaMove.Source source, SegmentBatchParser.Record record) throws IOException;
    }

    /** Bounded coordinate metadata only; payload is later copied through two existing owner buffers. */
    private static final class CompactionCapture implements CompactionVisitor {
        private final int maximumEntries;
        private final int capacity;
        private final List<CompactionEntry> entries = new ArrayList<>();
        private final List<CompactionBatch> batches = new ArrayList<>();
        private final HashSet<CompactionKey> distinct = new HashSet<>();
        private int recordBytes;
        private int batchStart;

        private CompactionCapture(int maximumEntries, int capacity) {
            this.maximumEntries = maximumEntries;
            this.capacity = capacity;
        }

        @Override
        public void accept(SegmentArenaMove.Source source, SegmentBatchParser.Record record) throws IOException {
            if (entries.size() == maximumEntries) {
                throw new IOException("Compaction coordinate plan exceeds the configured index coordinate limit");
            }
            if (!distinct.add(new CompactionKey(record.context(), record.entryId()))) {
                throw new IOException("Compaction live set repeats a selected coordinate");
            }
            if (entries.size() - batchStart == SegmentBatchFormat.MAX_RECORDS
                    || SegmentBatchFormat.alignedLength(Math.addExact(recordBytes,
                            source.location().recordLength())) > capacity) {
                seal();
            }
            recordBytes = Math.addExact(recordBytes, source.location().recordLength());
            if (SegmentBatchFormat.alignedLength(recordBytes) > capacity) {
                throw new IOException("Compaction entry exceeds its bounded COPY buffer");
            }
            entries.add(new CompactionEntry(source, SegmentArenaMove.digest(record.entry())));
        }

        private void seal() {
            if (batchStart < entries.size()) {
                batches.add(new CompactionBatch(entries.subList(batchStart, entries.size()),
                        SegmentBatchFormat.alignedLength(recordBytes)));
            }
            batchStart = entries.size();
            recordBytes = 0;
        }

        private CompactionPlan finish(List<SegmentArenaControlLog.Allocation> victims, long sourceBytes,
                                      long bookieCut) {
            seal();
            return new CompactionPlan(victims, batches, sourceBytes, bookieCut);
        }
    }

    /** Measure before COPY capture: unrelated current live data must not block direct dead-source FREE. */
    static CompactionPlan inspectCompaction(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                            SegmentArenaControlLog arena, FileChannel data,
                                            List<SegmentArenaControlLog.Allocation> victims, List<Long> ends,
                                            SegmentBookieControlLog bookie, int maximumEntries,
                                            ByteBuffer workspace, IsolatedSegmentFiles files,
                                            IsolatedSegmentFiles.ReclamationGuard protection, boolean selectVictims)
            throws IOException {
        protection.requireHeld(files, arena);
        long arenaCut = arena.appliedThrough();
        long bookieCut = bookie.recoveryReadCut(config, identity);
        List<SegmentArenaControlLog.Allocation> selected = victims;
        List<Long> selectedEnds = ends;
        if (selectVictims) {
            List<SegmentArenaControlLog.Allocation> dead = new ArrayList<>();
            List<Long> deadEnds = new ArrayList<>();
            for (int i = 0; i < victims.size(); i++) {
                boolean[] live = {false};
                scanCompactionAllocation(config, arena, data, victims.get(i), ends.get(i), bookie, workspace,
                        (source, record) -> live[0] = true);
                if (!live[0]) {
                    dead.add(victims.get(i));
                    deadEnds.add(ends.get(i));
                }
            }
            if (!dead.isEmpty()) {
                selected = List.copyOf(dead);
                selectedEnds = List.copyOf(deadEnds);
            }
        }
        CompactionCapture capture = new CompactionCapture(maximumEntries,
                Math.min(workspace.capacity(), SegmentBatchFormat.MAX_BATCH));
        long sourceBytes = 0;
        for (int i = 0; i < selected.size(); i++) {
            SegmentArenaControlLog.Allocation victim = selected.get(i);
            sourceBytes = Math.addExact(sourceBytes, victim.blocks() * SegmentArenaControlLog.BLOCK_BYTES);
            scanCompactionAllocation(config, arena, data, victim, selectedEnds.get(i), bookie, workspace, capture);
        }
        protection.requireHeld(files, arena);
        arena.requireMovePreparation(files);
        if (arena.appliedThrough() != arenaCut || bookie.recoveryReadCut(config, identity) != bookieCut) {
            throw new IOException("Compaction authority changed during source selection/capture");
        }
        return capture.finish(selected, sourceBytes, bookieCut);
    }

    /** Point-read one immutable measured chunk into pooled COPY bytes before any source view is reused. */
    static SegmentBatchFormat.FrozenBatch captureCompaction(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentArenaControlLog arena, SegmentBookieControlLog bookie, IsolatedSegmentFiles files,
            IsolatedSegmentFiles.ReclamationGuard protection, CompactionPlan plan, int batchIndex,
            SegmentBatchFormat.RelocationBuilder builder, ByteBuffer workspace) throws IOException {
        protection.requireHeld(files, arena);
        long arenaCut = arena.appliedThrough();
        if (bookie.recoveryReadCut(config, identity) != plan.bookieCut) {
            throw new IOException("Bookie authority changed after compaction measurement");
        }
        CompactionBatch batch = plan.batches().get(batchIndex);
        for (CompactionEntry entry : batch.entries()) {
            SegmentArenaMove.Source source = entry.source();
            SegmentPointRead.Location location = source.location();
            if (bookie.hasTerminalTombstone(location.context())
                    || !bookie.locallyReadableCoordinate(location.context(), location.entryId())
                    || arena.currentSelectorSequence(location.context(), location.entryId())
                            != source.expectedSelector()) {
                throw new IOException("Compaction source authority changed before chunk capture");
            }
            SegmentBatchParser.Record record = SegmentPointRead.readWithScratch(config, identity, arena,
                    location, workspace).record();
            if (!arena.isSelectedRecord(location, record)
                    || !entry.entrySha256().equals(SegmentArenaMove.digest(record.entry()))) {
                throw new IOException("Compaction source bytes changed after measurement");
            }
            ByteBuf borrowed = Unpooled.wrappedBuffer(record.entry());
            try {
                builder.append(location.context(), borrowed);
            } finally {
                borrowed.release();
            }
        }
        protection.requireHeld(files, arena);
        arena.requireMovePreparation(files);
        if (arena.appliedThrough() != arenaCut || bookie.recoveryReadCut(config, identity) != plan.bookieCut) {
            throw new IOException("Compaction authority changed during chunk capture");
        }
        SegmentBatchFormat.FrozenBatch frozen = builder.finish();
        if (frozen.length() != batch.bytes()) {
            frozen.close();
            throw new IOException("Compaction COPY length differs from its measured destination range");
        }
        return frozen;
    }

    /** Fresh protected measurement; the retained destination is never a member of the source FREE group. */
    static CompactionPlan inspectCompactionTail(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentArenaControlLog arena, FileChannel data, List<SegmentArenaControlLog.Allocation> victims,
            List<Long> ends, SegmentArenaControlLog.Allocation destination, long destinationEnd,
            long nextSequence, SegmentBookieControlLog bookie, int maximumEntries, ByteBuffer workspace,
            IsolatedSegmentFiles files, IsolatedSegmentFiles.ReclamationGuard protection) throws IOException {
        protection.requireHeld(files, arena);
        long arenaCut = arena.appliedThrough();
        long bookieCut = bookie.recoveryReadCut(config, identity);
        if (victims.isEmpty() || victims.contains(destination)
                || !destination.equals(arena.latestAllocationForShard(destination.ownerShard()).orElse(null))) {
            throw new IOException("Retained compaction destination is not the latest separate stream grant");
        }
        boolean[] hasCurrentCopy = {false};
        CompactionEnd tail = scanCompactionAllocation(config, arena, data, destination, destinationEnd,
                bookie, workspace, (source, record) -> {
                    hasCurrentCopy[0] |= record.origin() == SegmentBatchFormat.RELOCATION_COPY;
                });
        if (!hasCurrentCopy[0]) {
            return null;
        }
        if (tail.through() >= 0 && tail.through() != nextSequence - 1) {
            throw new IOException("Retained compaction tail does not end at the drained physical prefix");
        }
        CompactionPlan sources = inspectCompaction(config, identity, arena, data, victims, ends, bookie,
                maximumEntries, workspace, files, protection, false);
        if (arena.appliedThrough() != arenaCut || bookie.recoveryReadCut(config, identity) != bookieCut) {
            throw new IOException("Compaction authority changed during retained tail measurement");
        }
        if (sources.batchBytes > destination.offset() + destination.blocks() * SegmentArenaControlLog.BLOCK_BYTES
                - tail.offset()) {
            return null;
        }
        return new CompactionPlan(sources.victims, sources.batches, sources.sourceBytes, sources.bookieCut,
                new CompactionTail(destination, tail.offset()));
    }

    @FunctionalInterface
    private interface AllocationVisitor {
        void accept(SegmentPointRead.Location location, SegmentBatchParser.Record record) throws IOException;
    }

    private static CompactionEnd scanCompactionAllocation(IsolatedSegmentConfig config, SegmentArenaControlLog arena,
            FileChannel data, SegmentArenaControlLog.Allocation victim, long usedEnd, SegmentBookieControlLog bookie,
            ByteBuffer workspace, CompactionVisitor visitor) throws IOException {
        return scanAllocation(config, arena, data, victim, usedEnd, workspace, (location, record) -> {
            if (bookie.hasTerminalTombstone(record.context()) || !arena.isSelectedRecord(location, record)) {
                return;
            }
            if (!bookie.locallyReadableContext(record.context())) {
                throw new IOException("Compaction source has no locally readable Bookie context");
            }
            if (bookie.locallyReadableCoordinate(record.context(), record.entryId())) {
                visitor.accept(new SegmentArenaMove.Source(location,
                        arena.currentSelectorSequence(record.context(), record.entryId())), record);
            }
        });
    }

    /** Include terminal and obsolete copies: an index miss or selector alone cannot prove physical retirement. */
    static boolean containsAllocatedInstance(IsolatedSegmentConfig config, SegmentArenaControlLog arena,
            FileChannel data, SegmentArenaControlLog.Allocation allocation, long usedEnd, ByteBuffer workspace,
            ProfileLedgerContext context) throws IOException {
        boolean[] found = {false};
        scanAllocation(config, arena, data, allocation, usedEnd, workspace,
                (location, record) -> found[0] |= record.context().equals(context));
        return found[0];
    }

    /** Record bytes borrow the original owner's workspace only during the synchronous visitor call. */
    private static CompactionEnd scanAllocation(IsolatedSegmentConfig config, SegmentArenaControlLog arena,
            FileChannel data, SegmentArenaControlLog.Allocation victim, long usedEnd, ByteBuffer workspace,
            AllocationVisitor visitor) throws IOException {
        if (!victim.equals(arena.currentAllocation(victim.allocationId()))) {
            throw new IOException("Compaction source allocation changed before its capture");
        }
        long end = victim.offset() + victim.blocks() * SegmentArenaControlLog.BLOCK_BYTES;
        long through = -1;
        long writtenEnd = victim.offset();
        if (usedEnd < victim.offset() || usedEnd > end || usedEnd % config.alignment() != 0) {
            throw new IOException("Compaction victim has an invalid current DATA boundary");
        }
        for (long offset = victim.offset(); offset < usedEnd;) {
            ByteBuffer page = workspace.slice(0, 4096);
            SegmentFileIo.readFully(data, page, offset);
            if (allZero(page)) {
                if (victim.expectedGeneration() > 0) {
                    throw new IOException("Reused compaction source has an unwritten required page");
                }
                offset += 4096;
                continue;
            }
            SegmentBatchFormat.Address address = address(page, offset);
            if (address.fileIndex() != victim.file() || address.shard() != victim.ownerShard()
                    || address.allocation() != victim.allocationId()
                    || address.generation().value() != victim.generation()
                    || !address.streamGeneration().equals(victim.ownerGeneration())) {
                throw new IOException("Compaction source has another allocation identity");
            }
            int length = SegmentBatchParser.batchLength(page.slice(0, 128), address, config.bytesPerFile());
            if (length > workspace.capacity() || length > usedEnd - offset
                    || through >= 0 && address.sequence() != through + 1) {
                throw new IOException("Compaction source exceeds its buffer or has a DATA sequence gap");
            }
            through = address.sequence();
            ByteBuffer content = workspace.slice(0, length);
            SegmentFileIo.readFully(data, content, offset);
            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address, config.bytesPerFile());
            List<SegmentPointRead.Location> locations = SegmentPointRead.locations(parsed);
            for (int i = 0; i < locations.size(); i++) {
                SegmentBatchParser.Record record = parsed.records().get(i);
                SegmentPointRead.Location location = locations.get(i);
                if (SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                        != victim.ownerShard()) {
                    throw new IOException("Compaction source record has another logical shard");
                }
                visitor.accept(location, record);
            }
            offset += length;
            writtenEnd = offset;
        }
        return new CompactionEnd(writtenEnd, through);
    }

    /** Called only with the live writer's reclamation gate held; no index absence or caller-supplied liveness. */
    static SegmentArenaControlLog.FreeFacts proveDeadAllocation(IsolatedSegmentConfig config,
                                                               SegmentFileHeader identity,
                                                               SegmentArenaControlLog authority, FileChannel data,
                                                               SegmentArenaControlLog.Allocation allocation,
                                                               long allocationOrder, SegmentBookieControlLog bookie,
                                                               long bookieCut, ByteBuffer workspace, long usedEnd)
            throws IOException {
        if (workspace.remaining() < 4096) {
            throw new IOException("Whole-allocation FREE requires its bounded DATA buffer");
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        long first = -1;
        long through = -1;
        long records = 0;
        long arenaCut = authority.appliedThrough();
        boolean usesSelector = false;
        long end = allocation.offset() + allocation.blocks() * SegmentArenaControlLog.BLOCK_BYTES;
        if (usedEnd < allocation.offset() || usedEnd > end || usedEnd % config.alignment() != 0) {
            throw new IOException("Whole-allocation FREE has an invalid current DATA boundary");
        }
        for (long offset = allocation.offset(); offset < end;) {
            ByteBuffer page = workspace.slice(0, 4096);
            SegmentFileIo.readFully(data, page, offset);
            if (offset >= usedEnd || allZero(page)) {
                if (offset < usedEnd && allocation.expectedGeneration() > 0) {
                    throw new IOException("Current reused DATA prefix contains an unwritten page");
                }
                digest.update(page);
                offset += 4096;
                continue;
            }
            SegmentBatchFormat.Address address = address(page, offset);
            if (address.fileIndex() != allocation.file() || address.shard() != allocation.ownerShard()
                    || address.allocation() != allocation.allocationId()
                    || address.generation().value() != allocation.generation()
                    || !address.streamGeneration().equals(allocation.ownerGeneration())) {
                throw new IOException("Whole-allocation FREE encountered DATA with another allocation identity");
            }
            int length = SegmentBatchParser.batchLength(page.slice(0, 128), address, config.bytesPerFile());
            if (length > workspace.remaining() || length > usedEnd - offset) {
                throw new IOException("Whole-allocation FREE batch exceeds the bounded allocation extent");
            }
            ByteBuffer content = workspace.slice(0, length);
            SegmentFileIo.readFully(data, content, offset);
            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address, config.bytesPerFile());
            List<SegmentPointRead.Location> locations = SegmentPointRead.locations(parsed);
            HashSet<SegmentPointRead.Location> successors = new HashSet<>();
            for (int i = 0; i < parsed.records().size(); i++) {
                SegmentBatchParser.Record record = parsed.records().get(i);
                if (SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                        != allocation.ownerShard()) {
                    throw new IOException("Whole-allocation FREE record belongs to another shard");
                }
                boolean terminal = bookie.hasTerminalExclusion(record.context(), record.entryId());
                usesSelector |= record.origin() == SegmentBatchFormat.RELOCATION_COPY;
                if (!terminal) {
                    Optional<SegmentPointRead.Location> successor = authority.retirementSuccessor(
                            locations.get(i), record);
                    if (successor.isPresent()) {
                        successors.add(successor.get());
                        usesSelector = true;
                    } else if (record.origin() == SegmentBatchFormat.ENTRY_DATA
                            || authority.currentMoveLocation(record.context(), record.entryId())
                                    .filter(locations.get(i)::equals).isPresent()) {
                        throw new IOException("Whole-allocation FREE has a live record"
                                + " without an exact terminal exclusion"
                                + " or applied successor");
                    }
                }
            }
            if (first < 0) {
                first = address.sequence();
            } else if (through == Long.MAX_VALUE || address.sequence() != through + 1) {
                throw new IOException("Whole-allocation FREE has a physical sequence gap or duplicate");
            }
            through = address.sequence();
            records = Math.addExact(records, parsed.records().size());
            digest.update(content);
            // The victim parser borrows workspace. Hash its bytes before target reads reuse that same buffer.
            for (SegmentPointRead.Location successor : successors) {
                SegmentBatchParser.Record selected = SegmentPointRead.readWithScratch(config, identity, authority,
                        successor, workspace).record();
                authority.validateSelectedRecord(successor, selected);
            }
            offset += length;
        }
        return new SegmentArenaControlLog.FreeFacts(SegmentArenaControlLog.AllocationIdentity.of(allocation),
                allocationOrder, first, through, records, bookieCut, usesSelector ? arenaCut : -1,
                HexFormat.of().formatHex(digest.digest()));
    }

    record Scan(long batches, long records, long validatedBytes, long zeroPages,
                long readBytes, long coveredBatches, long skippedBodyBytes) { }

    record ClosedImageEntry(long cumulativeLength, int payloadLength, String payloadSha256, String entrySha256) { }

    record ClosedImage(long bytes, String sha256, List<ClosedImageEntry> entries) { }

    /** Complete single-file audit. No index, retired extent or locally unreadable coordinate can skip bytes. */
    static ClosedImage auditClosedImage(FileChannel data, IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentArenaControlLog.RecoveryDependencies arena, ProfileLedgerContext ledger,
            long prefix, long length, long maximumDataBytes, int maximumEntries) throws IOException {
        long bytes = config.bytesPerFile();
        if (config.dataFiles() != 1 || bytes > maximumDataBytes || data.size() != bytes || length < 0
                || prefix < 0 || prefix >= maximumEntries || maximumEntries > 65536) {
            throw new IOException("CLOSED reconstruction image exceeds its scope or bounds");
        }
        var imageDigest = sha256();
        var entryDigest = sha256();
        var entries = new ClosedImageEntry[(int) prefix + 1];
        var grants = new HashMap<Long, SegmentArenaControlLog.Allocation>();
        for (var grant : arena.grants()) {
            grants.put(grant.allocation().allocationId(), grant.allocation());
        }
        var nextSequence = new HashMap<StreamKey, Long>();
        for (var stream : arena.streams()) {
            nextSequence.put(new StreamKey(stream.shard(), new Generation(stream.generation())),
                    stream.firstSequence());
        }
        ByteBuffer page = ByteBuffer.allocate(config.alignment());
        SegmentFileIo.readFully(data, page, 0);
        identity.validate(page, config, 0);
        imageDigest.update(page.rewind());
        for (long offset = config.alignment(); offset < bytes;) {
            page.clear();
            SegmentFileIo.readFully(data, page, offset);
            if (allZero(page)) {
                imageDigest.update(page);
                offset += config.alignment();
                continue;
            }
            var address = address(page, offset);
            int batchLength = SegmentBatchParser.batchLength(page, address, bytes);
            var grant = grants.get(address.allocation());
            var stream = new StreamKey(address.shard(), address.streamGeneration());
            if (grant == null || address.fileIndex() != 0 || grant.file() != 0
                    || grant.ownerShard() != address.shard()
                    || !grant.ownerGeneration().equals(address.streamGeneration())
                    || grant.generation() != address.generation().value() || offset < grant.offset()
                    || batchLength > grant.offset() + grant.blocks() * SegmentArenaControlLog.BLOCK_BYTES - offset
                    || address.sequence() != nextSequence.getOrDefault(stream, -1L)
                    || address.sequence() == Long.MAX_VALUE) {
                throw new IOException("CLOSED reconstruction batch has no exact grant or continuous stream");
            }
            nextSequence.put(stream, address.sequence() + 1);
            ByteBuffer content = page;
            if (batchLength > page.remaining()) {
                content = ByteBuffer.allocate(batchLength);
                content.put(page);
                SegmentFileIo.readFully(data, content, offset + config.alignment());
            }
            var batch = SegmentBatchParser.parse(content, address, bytes);
            imageDigest.update(content);
            for (var record : batch.records()) {
                long entry = record.entryId();
                if (!ledger.equals(record.context()) || record.origin() != SegmentBatchFormat.ENTRY_DATA
                        || SegmentOwnerLoop.shardFor(ledger.ledgerId(), config.logicalShards()) != address.shard()
                        || entry > prefix) {
                    throw new IOException("CLOSED reconstruction DATA has a foreign coordinate");
                }
                entryDigest.update(record.entry());
                String rawDigest = HexFormat.of().formatHex(entryDigest.digest());
                ByteBuffer payload = record.entry().position(SegmentBatchFormat.ENTRY_OVERHEAD);
                int payloadBytes = payload.remaining();
                entryDigest.update(payload);
                var observed = new ClosedImageEntry(record.ledgerLength(), payloadBytes,
                        HexFormat.of().formatHex(entryDigest.digest()), rawDigest);
                if (entries[(int) entry] != null && !entries[(int) entry].equals(observed)) {
                    throw new IOException("CLOSED reconstruction DATA has a conflicting duplicate");
                }
                entries[(int) entry] = observed;
            }
            offset += batchLength;
        }
        long cumulative = 0;
        for (var entry : entries) {
            if (entry == null || entry.payloadLength() > length - cumulative
                    || entry.cumulativeLength() != cumulative + entry.payloadLength()) {
                throw new IOException("CLOSED reconstruction DATA has a missing entry or inconsistent length");
            }
            cumulative = entry.cumulativeLength();
        }
        if (cumulative != length || data.size() != bytes) {
            throw new IOException("CLOSED reconstruction DATA does not match the complete file and prefix length");
        }
        return new ClosedImage(bytes, HexFormat.of().formatHex(imageDigest.digest()), List.of(entries));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    record StreamCoverage(int shard, Generation generation, long firstSequence, long through) { }

    /** Synced physical evidence only; it does not grant Bookie selection, success or writable recovery. */
    record PhysicalStreamCandidate(IsolatedSegmentConfig sourceConfig, SegmentFileHeader sourceIdentity,
                                   Scan scan, long through, List<SegmentPointRead.Location> entries,
                                   List<Long> grantedHighWatermarks) {
        PhysicalStreamCandidate {
            java.util.Objects.requireNonNull(sourceConfig, "sourceConfig");
            java.util.Objects.requireNonNull(sourceIdentity, "sourceIdentity");
            entries = List.copyOf(entries);
            grantedHighWatermarks = List.copyOf(grantedHighWatermarks);
        }
    }

    record SelectedPhysicalLocation(SegmentArenaControlLog.SingleStream stream, SegmentPointRead.Location location) { }

    @FunctionalInterface
    interface SelectedLocationAction<T> {
        T apply(SelectedPhysicalLocation selected) throws IOException;
    }

    /** Executes the read or comparison under writer exclusion in the isolated no-MOVE/FREE scope. */
    static <T> Optional<T> withLocallyReadableSingleStreamLocation(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, ProfileLedgerContext context,
            long entryId, int maxBatches, int maxEntries, SelectedLocationAction<T> action) throws IOException {
        if (!bookie.matchesScope(config, identity) || !arena.matchesScope(config, identity)) {
            throw new IOException("Recovered location authority belongs to another storage scope");
        }
        try (SegmentDataFileLock files = SegmentDataFileLock.open(config.directory())) {
            int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
            SegmentArenaControlLog.SingleStream stream = arena.singleStreamForShard(shard).orElse(null);
            if (stream == null) {
                return Optional.empty();
            }
            PhysicalStreamCandidate recovered = recoverStreamCandidate(config, identity, arena, bookie,
                    syncIo, shard, stream.generation(), stream.firstSequence(), maxBatches, maxEntries, files);
            SegmentPointRead.Location selected = null;
            for (SegmentPointRead.Location location : recovered.entries()) {
                if (location.context().equals(context) && location.entryId() == entryId) {
                    if (selected != null) {
                        return Optional.empty();
                    }
                    selected = location;
                }
            }
            if (selected == null || !arena.singleStreamForShard(shard).filter(stream::equals).isPresent()) {
                return Optional.empty();
            }
            return Optional.of(action.apply(new SelectedPhysicalLocation(stream, selected)));
        }
    }

    private static final class StreamProbe {
        private final int shard;
        private final Generation generation;
        private final int maxBatches;
        private final List<SegmentBatchFormat.Address> batches = new ArrayList<>();
        private final List<long[]> retiredRanges = new ArrayList<>();

        private StreamProbe(int shard, Generation generation, int maxBatches) {
            this.shard = shard;
            this.generation = generation;
            this.maxBatches = maxBatches;
        }

        private boolean matches(SegmentBatchFormat.Address address) {
            return address.shard() == shard && address.streamGeneration().equals(generation);
        }

        private void observe(SegmentBatchFormat.Address address) throws IOException {
            if (!matches(address)) {
                return;
            }
            if (batches.size() == maxBatches) {
                throw new IOException("Physical stream recovery batch bound exceeded");
            }
            batches.add(address);
        }

        private long contiguousThrough(long firstSequence) throws IOException {
            List<long[]> ranges = new ArrayList<>(retiredRanges);
            batches.forEach(address -> ranges.add(new long[] {address.sequence(), address.sequence()}));
            ranges.sort(Comparator.comparingLong(range -> range[0]));
            long expected = firstSequence;
            for (long[] range : ranges) {
                if (range[0] != expected || range[1] == Long.MAX_VALUE) {
                    throw new IOException("Physical stream has a sequence gap, duplicate or overflow");
                }
                expected = range[1] + 1;
            }
            return expected - 1;
        }
    }

    private record StreamKey(int shard, Generation generation) { }

    private record Coordinate(ProfileLedgerContext context, long entryId) { }

    private record StreamPrefix(long firstSequence, long through, boolean contiguous) { }

    private static final class StreamProgress {
        private final long firstSequence;
        // At most one range per applied allocation, bounded by the fixed DATA/control capacity, not per record.
        private final Map<Long, SequenceRange> ranges = new TreeMap<>();
        private boolean broken;

        private StreamProgress(long firstSequence) {
            this.firstSequence = firstSequence;
        }

        private void observe(long allocationOrder, long sequence) {
            if (broken) {
                return;
            }
            SequenceRange range = ranges.computeIfAbsent(allocationOrder, ignored -> new SequenceRange(sequence));
            if (range.nextSequence == Long.MAX_VALUE || sequence != range.nextSequence) {
                broken = true;
            } else {
                range.nextSequence++;
            }
        }

        private void retire(long allocationOrder, long first, long through) {
            SequenceRange range = new SequenceRange(first);
            range.nextSequence = through + 1;
            broken |= ranges.putIfAbsent(allocationOrder, range) != null;
        }

        private StreamPrefix snapshot() {
            long next = firstSequence;
            for (SequenceRange range : ranges.values()) {
                if (broken || range.firstSequence != next) {
                    return new StreamPrefix(firstSequence, next - 1, false);
                }
                next = range.nextSequence;
            }
            return new StreamPrefix(firstSequence, next - 1, !broken);
        }

        private static final class SequenceRange {
            private final long firstSequence;
            private long nextSequence;

            private SequenceRange(long firstSequence) {
                this.firstSequence = firstSequence;
                nextSequence = firstSequence;
            }
        }
    }

    static final class PhysicalInventory {
        private final IsolatedSegmentConfig sourceConfig;
        private final SegmentFileHeader sourceIdentity;
        private final Scan scan;
        private final List<SegmentPointRead.Location> entries;
        private final List<Long> grantedHighWatermarks;
        private final Map<StreamKey, StreamPrefix> streamPrefixes;
        private final boolean completeStreamPrefixes;
        private final long arenaAppliedThrough;
        private final boolean usesIndexCoverage;

        private PhysicalInventory(IsolatedSegmentConfig sourceConfig, SegmentFileHeader sourceIdentity,
                                  Scan scan, List<SegmentPointRead.Location> entries,
                                  List<Long> grantedHighWatermarks,
                                  Map<StreamKey, StreamPrefix> streamPrefixes, boolean completeStreamPrefixes,
                                  long arenaAppliedThrough, boolean usesIndexCoverage) {
            this.sourceConfig = sourceConfig;
            this.sourceIdentity = sourceIdentity;
            this.scan = scan;
            this.entries = List.copyOf(entries);
            this.grantedHighWatermarks = List.copyOf(grantedHighWatermarks);
            this.streamPrefixes = Map.copyOf(streamPrefixes);
            this.completeStreamPrefixes = completeStreamPrefixes;
            this.arenaAppliedThrough = arenaAppliedThrough;
            this.usesIndexCoverage = usesIndexCoverage;
        }

        IsolatedSegmentConfig sourceConfig() {
            return sourceConfig;
        }

        SegmentFileHeader sourceIdentity() {
            return sourceIdentity;
        }

        Scan scan() {
            return scan;
        }

        boolean usesIndexCoverage() {
            return usesIndexCoverage;
        }

        List<SegmentPointRead.Location> entries() {
            return entries;
        }

        List<Long> grantedHighWatermarks() {
            return grantedHighWatermarks;
        }

        long latestCoordinate(SegmentArenaControlLog authority, ProfileLedgerContext context) throws IOException {
            return latestCoordinate(authority, context, Long.MAX_VALUE);
        }

        long latestCoordinate(SegmentArenaControlLog authority, ProfileLedgerContext context, long upperBound)
                throws IOException {
            requireCurrentArena(authority);
            long latest = -1;
            for (SegmentPointRead.Location location : entries) {
                if (location.context().equals(context) && location.entryId() <= upperBound) {
                    latest = Math.max(latest, location.entryId());
                }
            }
            return latest;
        }

        /** Full physical scans can prove a coordinate absent; a derived-index seed alone cannot. */
        boolean provesAbsent(SegmentArenaControlLog authority, ProfileLedgerContext context, long entryId)
                throws IOException {
            requireCurrentArena(authority);
            if (usesIndexCoverage || scan.skippedBodyBytes() != 0) {
                return false;
            }
            for (Map.Entry<StreamKey, StreamPrefix> stream : streamPrefixes.entrySet()) {
                if (!stream.getValue().contiguous()
                        || authority.streamFirstSequence(stream.getKey().shard(),
                                stream.getKey().generation().value()).orElse(-1) != stream.getValue().firstSequence()) {
                    return false;
                }
            }
            return entries.stream().noneMatch(location -> location.context().equals(context)
                    && (entryId < 0 || location.entryId() == entryId));
        }

        /** Applied control selectors plus a complete scan select a unique current coordinate. */
        SegmentPointRead.Location uniqueLocation(SegmentArenaControlLog authority,
                                                 ProfileLedgerContext context, long entryId) throws IOException {
            requireCurrentArena(authority);
            SegmentPointRead.Location selected = null;
            for (SegmentPointRead.Location location : entries) {
                if (location.context().equals(context) && location.entryId() == entryId) {
                    if (selected != null) {
                        throw new IOException("Recovered read location is ambiguous");
                    }
                    selected = location;
                }
            }
            if (selected == null) {
                throw new IOException("Recovered read location is not ready");
            }
            SegmentBatchFormat.Address address = selected.address();
            StreamPrefix prefix = streamPrefixes.get(new StreamKey(address.shard(), address.streamGeneration()));
            long first = authority.streamFirstSequence(address.shard(), address.streamGeneration().value()).orElse(-1);
            if (prefix == null || !prefix.contiguous() || first < 0 || prefix.firstSequence() != first
                    || address.sequence() > prefix.through()) {
                throw new IOException("Recovered read stream has no validated contiguous prefix");
            }
            return selected;
        }

        void requireCurrentArena(SegmentArenaControlLog authority) throws IOException {
            if (!completeStreamPrefixes || !authority.matchesScope(sourceConfig, sourceIdentity)
                    || authority.appliedThrough() != arenaAppliedThrough) {
                throw new IOException("Startup inventory no longer covers current Arena authority");
            }
        }

        /** Complete same-scan ranges for a derived index; this does not grant DATA or allocator authority. */
        List<StreamCoverage> indexStreams(SegmentArenaControlLog authority) throws IOException {
            requireCurrentArena(authority);
            List<StreamCoverage> result = new ArrayList<>();
            HashSet<Integer> shards = new HashSet<>();
            for (Map.Entry<StreamKey, StreamPrefix> entry : streamPrefixes.entrySet()) {
                StreamKey key = entry.getKey();
                StreamPrefix prefix = entry.getValue();
                if (!prefix.contiguous() || !shards.add(key.shard())
                        || authority.streamFirstSequence(key.shard(), key.generation().value()).orElse(-1)
                                != prefix.firstSequence()) {
                    throw new IOException("Derived coverage needs one complete current stream per shard");
                }
                result.add(new StreamCoverage(key.shard(), key.generation(),
                        prefix.firstSequence(), prefix.through()));
            }
            result.sort(java.util.Comparator.comparingInt(StreamCoverage::shard));
            return List.copyOf(result);
        }

        /** Same-scan physical stream evidence only; Bookie selection and local success remain separate. */
        PhysicalStreamCandidate streamCandidate(SegmentArenaControlLog authority, int shard,
                                                Generation generation) throws IOException {
            if (!completeStreamPrefixes || shard < 0 || shard >= sourceConfig.logicalShards()
                    || generation == null || !authority.matchesScope(sourceConfig, sourceIdentity)) {
                throw new IOException("Startup stream evidence is unavailable for this storage scope");
            }
            long first = authority.streamFirstSequence(shard, generation.value()).orElse(-1);
            if (first < 0) {
                throw new IOException("Physical stream start is not backed by applied Arena control");
            }
            StreamPrefix prefix = streamPrefixes.get(new StreamKey(shard, generation));
            if (prefix != null && (!prefix.contiguous() || prefix.firstSequence() != first)) {
                throw new IOException("Physical stream has a sequence gap, duplicate or changed start");
            }
            List<SegmentPointRead.Location> selected = new ArrayList<>();
            for (SegmentPointRead.Location location : entries) {
                SegmentBatchFormat.Address address = location.address();
                if (address.shard() == shard && address.streamGeneration().equals(generation)) {
                    selected.add(location);
                }
            }
            return new PhysicalStreamCandidate(sourceConfig, sourceIdentity, scan,
                    prefix == null ? first - 1 : prefix.through(), selected, grantedHighWatermarks);
        }

    }

    static Scan inspect(IsolatedSegmentConfig config, SegmentFileHeader identity,
                        SegmentArenaControlLog authority) throws IOException {
        return scan(config, identity, authority, null, 0, 0, null, null).scan();
    }

    /** A successful scan forces the checked DATA files; it does not establish Bookie or locator authority. */
    static Scan validateAndSync(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                SegmentArenaControlLog authority, SegmentFileIo syncIo) throws IOException {
        if (syncIo == null) {
            throw new IllegalArgumentException("DATA recovery sync requires an I/O backend");
        }
        return scan(config, identity, authority, syncIo, 0, 0, null, null).scan();
    }

    /** The caller retains the isolated writer lock while consuming the scan and opening DATA files. */
    static Scan validateAndSyncWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                           SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                           FileLock writerLock) throws IOException {
        if (syncIo == null || writerLock == null) {
            throw new IllegalArgumentException("DATA recovery sync and writer lock required");
        }
        return scanWhileLocked(config, identity, authority, syncIo, 0, 0, null, null, writerLock).scan();
    }

    /** Physical locations only: caller still needs the recovered stream prefix and Bookie selector/permissions. */
    static PhysicalInventory recoverPhysicalInventory(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                       int maxEntries) throws IOException {
        if (syncIo == null || maxEntries < 1) {
            throw new IllegalArgumentException("Bounded DATA recovery sync and inventory required");
        }
        return scan(config, identity, authority, syncIo, maxEntries, 0, null, null);
    }

    /** Complete, bounded inventory under the writer lock already owned by the restart caller. */
    static PhysicalInventory recoverPhysicalInventoryWhileLocked(IsolatedSegmentConfig config,
                                                                 SegmentFileHeader identity,
                                                                 SegmentArenaControlLog authority,
                                                                 SegmentFileIo syncIo, int maxEntries, int maxStreams,
                                                                 FileLock writerLock) throws IOException {
        if (syncIo == null || maxEntries < 1 || maxStreams < 1 || writerLock == null) {
            throw new IllegalArgumentException("Bounded DATA inventory, sync and writer lock required");
        }
        return scanWhileLocked(config, identity, authority, syncIo, maxEntries, maxStreams,
                null, null, writerLock);
    }

    /** Select all locally readable coordinates across streams; retain no durable-duplicate qualification. */
    static PhysicalInventory recoverLocallyReadableInventoryWhileLocked(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, int maxEntries, int maxStreams,
            FileLock writerLock) throws IOException {
        return recoverLocallyReadableInventoryWhileLocked(config, identity, authority, bookie,
                syncIo, maxEntries, maxStreams, writerLock, null);
    }

    static PhysicalInventory recoverLocallyReadableInventoryWhileLocked(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, int maxEntries, int maxStreams,
            FileLock writerLock, SegmentDerivedIndex.RecoveryCoverage seed) throws IOException {
        return recoverLocallyReadableInventoryWhileLocked(config, identity, authority, bookie,
                syncIo, maxEntries, maxStreams, writerLock, seed, null);
    }

    static PhysicalInventory recoverLocallyReadableInventoryWhileLocked(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, int maxEntries, int maxStreams,
            FileLock writerLock, SegmentDerivedIndex.RecoveryCoverage seed, ProfileReceiveBudget recoveryBudget)
            throws IOException {
        if (recoveryBudget == null) {
            return scanLocallyReadableInventory(config, identity, authority, bookie, syncIo,
                    maxEntries, maxStreams, writerLock, seed, null);
        }
        synchronized (authority) {
            try (var scanCredit = recoveryBudget.reserveDataWorkingSet(scanCapacity(config, authority,
                    maxEntries, maxStreams, seed != null))) {
                return scanLocallyReadableInventory(config, identity, authority, bookie, syncIo,
                        maxEntries, maxStreams, writerLock, seed, recoveryBudget);
            }
        }
    }

    /** Keep optional seed construction inside the same credit as the scan and duplicate-coordinate check. */
    static PhysicalInventory recoverIndexedLocallyReadableInventoryWhileLocked(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, int maxEntries, int maxStreams,
            FileLock writerLock, SegmentDerivedIndex index, ProfileReceiveBudget recoveryBudget) throws IOException {
        synchronized (authority) {
            try (var scanCredit = recoveryBudget == null ? null
                    : recoveryBudget.reserveDataWorkingSet(scanCapacity(config, authority,
                            maxEntries, maxStreams, index != null))) {
                SegmentDerivedIndex.RecoveryCoverage seed = index == null ? null
                        : index.recoveryCoverage(authority, bookie, maxEntries).orElse(null);
                return scanLocallyReadableInventory(config, identity, authority, bookie, syncIo,
                        maxEntries, maxStreams, writerLock, seed, recoveryBudget);
            }
        }
    }

    private static PhysicalInventory scanLocallyReadableInventory(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, int maxEntries, int maxStreams,
            FileLock writerLock, SegmentDerivedIndex.RecoveryCoverage seed, ProfileReceiveBudget recoveryBudget)
            throws IOException {
        if (syncIo == null || maxEntries < 1 || maxStreams < 1 || writerLock == null) {
            throw new IllegalArgumentException("Bounded DATA inventory, sync and writer lock required");
        }
        long readCut = bookie.recoveryReadCut(config, identity);
        long arenaCut = authority.appliedThrough();
        PhysicalInventory inventory = scanWhileLocked(config, identity, authority, syncIo,
                maxEntries, maxStreams, null, bookie, writerLock, seed, recoveryBudget);
        if (readCut != bookie.recoveryReadCut(config, identity) || arenaCut != authority.appliedThrough()) {
            throw new IOException("Recovery control cut changed before locator publication");
        }
        for (StreamPrefix prefix : inventory.streamPrefixes.values()) {
            if (!prefix.contiguous()) {
                throw new IOException("Physical stream has a sequence gap, duplicate or overflow");
            }
        }
        HashSet<Coordinate> coordinates = new HashSet<>();
        for (SegmentPointRead.Location location : inventory.entries()) {
            if (!coordinates.add(new Coordinate(location.context(), location.entryId()))) {
                throw new IOException("Required DATA coordinate has multiple physical locations");
            }
        }
        return inventory;
    }

    /** Requires the persisted Arena stream start before returning a read-only physical candidate. */
    static PhysicalStreamCandidate recoverPhysicalStreamCandidate(IsolatedSegmentConfig config,
                                                                  SegmentFileHeader identity,
                                                                  SegmentArenaControlLog authority,
                                                                  SegmentFileIo syncIo, int shard,
                                                                  Generation streamGeneration,
                                                                  long firstSequence, int maxBatches,
                                                                  int maxEntries) throws IOException {
        return recoverStreamCandidate(config, identity, authority, null, syncIo, shard, streamGeneration,
                firstSequence, maxBatches, maxEntries);
    }

    /** Excludes locally terminal routes from a synced candidate; it does not resolve the current selector. */
    static PhysicalStreamCandidate recoverLocallyReadableStreamCandidate(IsolatedSegmentConfig config,
                                                                          SegmentFileHeader identity,
                                                                          SegmentArenaControlLog authority,
                                                                          SegmentBookieControlLog bookie,
                                                                          SegmentFileIo syncIo, int shard,
                                                                          Generation streamGeneration,
                                                                          long firstSequence, int maxBatches,
                                                                          int maxEntries) throws IOException {
        if (bookie == null || !bookie.matchesScope(config, identity)) {
            throw new IOException("Bookie route and DATA files have different storage scopes");
        }
        return recoverStreamCandidate(config, identity, authority, bookie, syncIo, shard, streamGeneration,
                firstSequence, maxBatches, maxEntries);
    }

    private static PhysicalStreamCandidate recoverStreamCandidate(IsolatedSegmentConfig config,
                                                                  SegmentFileHeader identity,
                                                                  SegmentArenaControlLog authority,
                                                                  SegmentBookieControlLog bookie,
                                                                  SegmentFileIo syncIo, int shard,
                                                                  Generation streamGeneration,
                                                                  long firstSequence, int maxBatches,
                                                                  int maxEntries) throws IOException {
        return recoverStreamCandidate(config, identity, authority, bookie, syncIo, shard, streamGeneration,
                firstSequence, maxBatches, maxEntries, null);
    }

    private static PhysicalStreamCandidate recoverStreamCandidate(IsolatedSegmentConfig config,
                                                                  SegmentFileHeader identity,
                                                                  SegmentArenaControlLog authority,
                                                                  SegmentBookieControlLog bookie,
                                                                  SegmentFileIo syncIo, int shard,
                                                                  Generation streamGeneration,
                                                                  long firstSequence, int maxBatches,
                                                                  int maxEntries, SegmentDataFileLock files)
            throws IOException {
        if (syncIo == null || shard < 0 || shard >= config.logicalShards() || streamGeneration == null
                || firstSequence < 0 || maxBatches < 1 || maxEntries < 1) {
            throw new IllegalArgumentException("Bounded physical stream recovery inputs required");
        }
        if (authority.streamFirstSequence(shard, streamGeneration.value()).orElse(-1) != firstSequence) {
            throw new IOException("Physical stream start is not backed by applied Arena control");
        }
        StreamProbe probe = new StreamProbe(shard, streamGeneration, maxBatches);
        PhysicalInventory inventory = files == null
                ? scan(config, identity, authority, syncIo, maxEntries, 0, probe, bookie)
                : scanWhileLocked(config, identity, authority, syncIo, maxEntries, 0, probe, bookie, files.lock());
        if (bookie != null && !bookie.matchesScope(config, identity)) {
            throw new IOException("Bookie read route became unavailable during DATA recovery");
        }
        return new PhysicalStreamCandidate(config, identity, inventory.scan(), probe.contiguousThrough(firstSequence),
                inventory.entries(), inventory.grantedHighWatermarks());
    }

    private static PhysicalInventory scan(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                          SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                          int maxEntries, int maxStreams, StreamProbe stream,
                                          SegmentBookieControlLog bookie) throws IOException {
        try (SegmentDataFileLock files = SegmentDataFileLock.open(config.directory())) {
            return scanWhileLocked(config, identity, authority, syncIo, maxEntries, maxStreams,
                    stream, bookie, files.lock());
        }
    }

    private static PhysicalInventory scanWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                    int maxEntries, int maxStreams, StreamProbe stream,
                                                    SegmentBookieControlLog bookie,
                                                    FileLock writerLock)
            throws IOException {
        return scanWhileLocked(config, identity, authority, syncIo, maxEntries, maxStreams,
                stream, bookie, writerLock, null);
    }

    private static PhysicalInventory scanWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                    int maxEntries, int maxStreams, StreamProbe stream,
                                                    SegmentBookieControlLog bookie, FileLock writerLock,
                                                    SegmentDerivedIndex.RecoveryCoverage seed) throws IOException {
        return scanWhileLocked(config, identity, authority, syncIo, maxEntries, maxStreams,
                stream, bookie, writerLock, seed, null);
    }

    private static PhysicalInventory scanWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                    int maxEntries, int maxStreams, StreamProbe stream,
                                                    SegmentBookieControlLog bookie, FileLock writerLock,
                                                    SegmentDerivedIndex.RecoveryCoverage seed,
                                                    ProfileReceiveBudget recoveryBudget) throws IOException {
        if (!writerLock.isValid() || !authority.matchesScope(config, identity)) {
            throw new IOException("Arena authority does not match the DATA scope");
        }
        if (bookie != null && bookie.indexAppliedCut(config, identity) < authority.requiredMoveBookieCut()) {
            throw new IOException("MOVE has a missing required Bookie authority cut");
        }
        if (bookie != null && bookie.indexAppliedCut(config, identity) < authority.requiredRetiredBookieCut()) {
            throw new IOException("Retired allocation has a missing required Bookie authority cut");
        }
        long batches = 0;
        long records = 0;
        long validatedBytes = 0;
        long zeroPages = 0;
        long readBytes = 0;
        long coveredBatches = 0;
        long skippedBodyBytes = 0;
        var parseLayout = recoveryBudget == null ? null : SegmentControlCapacity.Layout.hotSpot();
        List<SegmentPointRead.Location> entries = new ArrayList<>();
        HashSet<SegmentPointRead.Location> foundMoves = new HashSet<>();
        Map<StreamKey, StreamProgress> streams = new HashMap<>();
        Map<StreamKey, StreamCoverage> covered = new HashMap<>();
        Map<SegmentBatchFormat.Address, Integer> expectedHeaders = new HashMap<>();
        for (SegmentArenaControlLog.RetiredRange range : authority.retiredStreamRanges()) {
            if (stream != null && stream.shard == range.shard()
                    && stream.generation.equals(range.generation())) {
                stream.retiredRanges.add(new long[] {range.firstSequence(), range.through()});
            }
            if (maxStreams > 0) {
                StreamKey key = new StreamKey(range.shard(), range.generation());
                if (!streams.containsKey(key) && streams.size() == maxStreams) {
                    throw new IOException("Retired stream recovery capacity exhausted");
                }
                streams.computeIfAbsent(key, ignored -> new StreamProgress(range.streamFirstSequence()))
                        .retire(range.allocationOrder(), range.firstSequence(), range.through());
            }
        }
        if (seed != null) {
            if (!config.sameStorageScope(seed.sourceConfig()) || !identity.equals(seed.sourceIdentity())) {
                throw new IOException("Covered DATA recovery has another storage scope");
            }
            if (seed.entries().size() > maxEntries || seed.coverage().streams().size() > maxStreams) {
                throw new IOException("Covered DATA recovery exceeds declared capacity");
            }
            for (StreamCoverage cut : seed.coverage().streams()) {
                covered.put(new StreamKey(cut.shard(), cut.generation()), cut);
            }
            for (SegmentPointRead.Location location : seed.entries()) {
                Integer old = expectedHeaders.put(location.address(), location.batchLength());
                if (old != null && old != location.batchLength()) {
                    throw new IOException("Covered locators disagree on batch extent");
                }
                if (bookie.locallyReadableCoordinate(location.context(), location.entryId())) {
                    if (authority.currentSelectorSequence(location.context(), location.entryId()) >= 0) {
                        int scratchBytes = SegmentPointRead.scratchBytes(location);
                        try (var scratchCredit = recoveryBudget == null ? null
                                : recoveryBudget.reserveDataWorkingSet(scratchBytes)) {
                            ByteBuffer scratch = ByteBuffer.allocate(scratchBytes);
                            SegmentPointRead.Result verified = SegmentPointRead.readWithScratch(
                                    config, identity, authority, location, scratch);
                            authority.validateSelectedRecord(location, verified.record());
                            readBytes += verified.bytesRead();
                            foundMoves.add(location);
                        }
                    }
                    entries.add(location);
                }
            }
        }
        for (int file = 0; file < config.dataFiles(); file++) {
            try (FileChannel data = syncIo == null
                    ? FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                            StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
                    : FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                            StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                    var headerCredit = recoveryBudget == null ? null
                            : recoveryBudget.reserveDataWorkingSet(4096)) {
                if (data.size() != config.bytesPerFile()) {
                    throw new IOException("DATA file size mismatch");
                }
                ByteBuffer header = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(data, header, 0);
                identity.validate(header, config, file);
                readBytes += 4096;
                long end = seed == null ? config.bytesPerFile() : authority.grantedHighWatermark(file);
                if (end < 4096 || end > config.bytesPerFile() || end % 4096 != 0) {
                    throw new IOException("Invalid recovery allocation high watermark");
                }
                for (long offset = 4096; offset < end;) {
                    long retiredEnd = authority.retiredExtentEnd(file, offset);
                    if (retiredEnd > offset) {
                        offset = retiredEnd;
                        continue;
                    }
                    Optional<SegmentArenaControlLog.ReusedExtent> reused = authority.reusedExtent(file, offset);
                    if (reused.isPresent() && offset >= reused.get().usedEnd()) {
                        offset = reused.get().end();
                        continue;
                    }
                    if (stream != null && !authority.hasAppliedStreamAt(file, offset,
                            stream.shard, stream.generation.value())) {
                        offset += 4096;
                        continue;
                    }
                    try (var pageCredit = recoveryBudget == null ? null
                            : recoveryBudget.reserveDataWorkingSet(SegmentControlCapacity.sum(
                                    seed == null ? 4096 : SegmentBatchFormat.HEADER,
                                    SegmentBatchParser.workspaceCapacity(parseLayout, 0)))) {
                        ByteBuffer page = ByteBuffer.allocate(seed == null ? 4096 : SegmentBatchFormat.HEADER);
                        SegmentFileIo.readFully(data, page, offset);
                        readBytes += page.remaining();
                        if (allZero(page)) {
                            if (reused.isPresent()) {
                                throw new IOException("Healthy CLOSED reused DATA prefix contains an unwritten page");
                            }
                            if (seed != null) {
                                try (var zeroCredit = recoveryBudget == null ? null
                                        : recoveryBudget.reserveDataWorkingSet(4096)) {
                                    ByteBuffer zero = ByteBuffer.allocate(4096);
                                    SegmentFileIo.readFully(data, zero, offset);
                                    readBytes += 4096;
                                    if (!allZero(zero)) {
                                        throw new IOException("Zero DATA header has nonzero required body");
                                    }
                                }
                            }
                            zeroPages++;
                            offset += 4096;
                            continue;
                        }
                        SegmentBatchFormat.Address address = address(page, offset);
                        if (address.fileIndex() != file
                                || address.shard() % config.dataFiles() != file) {
                            throw new IOException("DATA batch claims a different physical file");
                        }
                        int length = SegmentBatchParser.batchLength(page.slice(0, 128), address,
                                config.bytesPerFile());
                        int parsedRecords = page.getInt(76); // Validated by batchLength before any record allocation.
                        if (reused.isPresent() && length > reused.get().usedEnd() - offset) {
                            throw new IOException("Current reused DATA exceeds its healthy CLOSED tail");
                        }
                        if (!authority.hasAppliedGrant(file, offset, length, address.shard(),
                                address.streamGeneration().value(), address.allocation(),
                                address.generation().value())) {
                            throw new IOException("DATA batch has no matching applied grant");
                        }
                        if (maxStreams > 0) {
                            StreamKey key = new StreamKey(address.shard(), address.streamGeneration());
                            StreamProgress progress = streams.get(key);
                            if (progress == null) {
                                if (streams.size() == maxStreams) {
                                    throw new IOException("Physical recovery stream capacity exhausted");
                                }
                                long first = authority.streamFirstSequence(address.shard(),
                                        address.streamGeneration().value()).orElse(-1);
                                if (first < 0) {
                                    throw new IOException("Physical stream start has no applied Arena authority");
                                }
                                progress = new StreamProgress(first);
                                streams.put(key, progress);
                            }
                            long order = authority.appliedAllocationOrder(address);
                            if (order < 0) {
                                throw new IOException("Physical allocation has no applied control order");
                            }
                            progress.observe(order, address.sequence());
                        }
                        StreamCoverage cut = covered.get(new StreamKey(address.shard(), address.streamGeneration()));
                        if (cut != null && address.sequence() >= cut.firstSequence()
                                && address.sequence() <= cut.through()) {
                            Integer expected = expectedHeaders.remove(address);
                            if (expected != null && expected != length) {
                                throw new IOException("Covered DATA header disagrees with indexed batch length");
                            }
                            coveredBatches++;
                            skippedBodyBytes += length - SegmentBatchFormat.HEADER;
                            offset += length;
                            continue;
                        }
                        // Parsed payload views remain inside this scope; inventory retains only locations.
                        try (var contentCredit = recoveryBudget == null ? null
                                : recoveryBudget.reserveDataWorkingSet(SegmentControlCapacity.sum(length,
                                        SegmentBatchParser.workspaceCapacity(parseLayout, parsedRecords)))) {
                            ByteBuffer content = ByteBuffer.allocate(length);
                            SegmentFileIo.readFully(data, content, offset);
                            readBytes += length;
                            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address,
                                    config.bytesPerFile(), parsedRecords);
                            for (SegmentBatchParser.Record record : parsed.records()) {
                                if (SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                                        != address.shard()) {
                                    throw new IOException("DATA record belongs to another logical shard");
                                }
                            }
                            List<SegmentPointRead.Location> locations = SegmentPointRead.locations(parsed);
                            for (int i = 0; i < parsed.records().size(); i++) {
                                if (authority.isSelectedRecord(locations.get(i), parsed.records().get(i))
                                        && parsed.records().get(i).origin() == SegmentBatchFormat.RELOCATION_COPY) {
                                    foundMoves.add(locations.get(i));
                                }
                            }
                            if (stream != null) {
                                stream.observe(address);
                            }
                            if (maxEntries > 0) {
                                for (int i = 0; i < parsed.records().size(); i++) {
                                    SegmentBatchParser.Record record = parsed.records().get(i);
                                    if (authority.isSelectedRecord(locations.get(i), record)
                                            && (stream == null || stream.matches(address))
                                            && (bookie == null || bookie.locallyReadableCoordinate(
                                                record.context(), record.entryId()))) {
                                        if (entries.size() == maxEntries) {
                                            throw new IOException("Physical recovery inventory capacity exhausted");
                                        }
                                        entries.add(locations.get(i));
                                    }
                                }
                            }
                            batches++;
                            records += parsed.records().size();
                            validatedBytes += length;
                            offset += length;
                        }
                    }
                }
                if (syncIo != null) {
                    syncIo.force(data, false);
                }
            }
        }
        List<Long> grantedHighWatermarks = new ArrayList<>(config.dataFiles());
        for (int file = 0; file < config.dataFiles(); file++) {
            grantedHighWatermarks.add(authority.grantedHighWatermark(file));
        }
        Map<StreamKey, StreamPrefix> prefixes = new HashMap<>();
        streams.forEach((key, progress) -> prefixes.put(key, progress.snapshot()));
        if (!expectedHeaders.isEmpty()) {
            throw new IOException("Covered locator has no matching physical DATA header");
        }
        for (SegmentPointRead.Location selected : authority.currentMoveLocations()) {
            if ((stream == null || stream.matches(selected.address()))
                    && (bookie == null || bookie.locallyReadableCoordinate(selected.context(), selected.entryId()))
                    && !foundMoves.contains(selected)) {
                throw new IOException("Current MOVE target is missing from required DATA");
            }
        }
        for (Map.Entry<StreamKey, StreamCoverage> entry : covered.entrySet()) {
            StreamPrefix prefix = prefixes.get(entry.getKey());
            StreamCoverage cut = entry.getValue();
            if (cut.through() >= cut.firstSequence()
                    && (prefix == null || !prefix.contiguous() || prefix.through() < cut.through())) {
                throw new IOException("Covered physical stream is missing or has a sequence gap");
            }
        }
        return new PhysicalInventory(config, identity,
                new Scan(batches, records, validatedBytes, zeroPages, readBytes, coveredBatches, skippedBodyBytes),
                entries, grantedHighWatermarks, prefixes, maxStreams > 0, authority.appliedThrough(), seed != null);
    }

    private static SegmentBatchFormat.Address address(ByteBuffer page, long offset) throws IOException {
        try {
            return new SegmentBatchFormat.Address(page.getInt(16), page.getInt(20), page.getLong(24),
                    new Generation(page.getLong(32)), new Generation(page.getLong(48)),
                    page.getLong(56), offset);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid DATA batch address", invalid);
        }
    }

    private static boolean allZero(ByteBuffer page) {
        for (int i = 0; i < page.remaining(); i++) {
            if (page.get(i) != 0) {
                return false;
            }
        }
        return true;
    }

    private SegmentDataRecovery() {
    }
}

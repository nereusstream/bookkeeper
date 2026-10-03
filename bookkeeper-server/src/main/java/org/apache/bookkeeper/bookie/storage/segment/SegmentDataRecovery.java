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

/** Quiesced isolated DATA validation; zero pages and recovery sync never grant reuse or entry authority. */
final class SegmentDataRecovery {
    /** Called only with the live writer's reclamation gate held; no index absence or caller-supplied liveness. */
    static SegmentArenaControlLog.FreeFacts proveDeadAllocation(IsolatedSegmentConfig config, FileChannel data,
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
            for (SegmentBatchParser.Record record : parsed.records()) {
                if (record.origin() != SegmentBatchFormat.ENTRY_DATA
                        || SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                            != allocation.ownerShard()
                        || !bookie.hasTerminalTombstone(record.context())) {
                    throw new IOException("Whole-allocation FREE has a record without an exact terminal tombstone");
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
            offset += length;
        }
        return new SegmentArenaControlLog.FreeFacts(SegmentArenaControlLog.AllocationIdentity.of(allocation),
                allocationOrder, first, through, records, bookieCut, HexFormat.of().formatHex(digest.digest()));
    }

    record Scan(long batches, long records, long validatedBytes, long zeroPages,
                long readBytes, long coveredBatches, long skippedBodyBytes) { }

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
        if (syncIo == null || maxEntries < 1 || maxStreams < 1 || writerLock == null) {
            throw new IllegalArgumentException("Bounded DATA inventory, sync and writer lock required");
        }
        long readCut = bookie.recoveryReadCut(config, identity);
        long arenaCut = authority.appliedThrough();
        PhysicalInventory inventory = scanWhileLocked(config, identity, authority, syncIo,
                maxEntries, maxStreams, null, bookie, writerLock, seed);
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
        if (!writerLock.isValid() || !authority.matchesScope(config, identity)) {
            throw new IOException("Arena authority does not match the DATA scope");
        }
        if (bookie != null && bookie.indexAppliedCut(config, identity) < authority.requiredMoveBookieCut()) {
            throw new IOException("MOVE has a missing required Bookie authority cut");
        }
        if (authority.hasMoves()) {
            seed = null; // MOVE coverage publication is not yet wired to the original-owner maintenance cut.
        }
        long batches = 0;
        long records = 0;
        long validatedBytes = 0;
        long zeroPages = 0;
        long readBytes = 0;
        long coveredBatches = 0;
        long skippedBodyBytes = 0;
        List<SegmentPointRead.Location> entries = new ArrayList<>();
        HashSet<SegmentPointRead.Location> foundMoves = new HashSet<>();
        Map<StreamKey, StreamProgress> streams = new HashMap<>();
        Map<StreamKey, StreamCoverage> covered = new HashMap<>();
        Map<SegmentBatchFormat.Address, Integer> expectedHeaders = new HashMap<>();
        for (SegmentArenaControlLog.Free free : authority.retiredAllocations()) {
            SegmentArenaControlLog.FreeFacts facts = free.facts();
            if (bookie != null && bookie.indexAppliedCut(config, identity) < facts.bookieCut()) {
                throw new IOException("Retired allocation has a missing required Bookie authority cut");
            }
            SegmentArenaControlLog.AllocationIdentity allocation = facts.allocation();
            if (facts.records() == 0) {
                continue;
            }
            if (stream != null && stream.shard == allocation.ownerShard()
                    && stream.generation.equals(allocation.ownerGeneration())) {
                stream.retiredRanges.add(new long[] {facts.firstSequence(), facts.through()});
            }
            if (maxStreams > 0) {
                StreamKey key = new StreamKey(allocation.ownerShard(), allocation.ownerGeneration());
                if (!streams.containsKey(key) && streams.size() == maxStreams) {
                    throw new IOException("Retired stream recovery capacity exhausted");
                }
                streams.computeIfAbsent(key, ignored -> new StreamProgress(allocation.streamFirstSequence()))
                        .retire(facts.allocationOrder(), facts.firstSequence(), facts.through());
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
                if (bookie.locallyReadableContext(location.context())) {
                    entries.add(location);
                }
            }
        }
        for (int file = 0; file < config.dataFiles(); file++) {
            try (FileChannel data = syncIo == null
                    ? FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                            StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
                    : FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                            StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
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
                    ByteBuffer page = ByteBuffer.allocate(seed == null ? 4096 : SegmentBatchFormat.HEADER);
                    SegmentFileIo.readFully(data, page, offset);
                    readBytes += page.remaining();
                    if (allZero(page)) {
                        if (reused.isPresent()) {
                            throw new IOException("Healthy CLOSED reused DATA prefix contains an unwritten page");
                        }
                        if (seed != null) {
                            page = ByteBuffer.allocate(4096);
                            SegmentFileIo.readFully(data, page, offset);
                            readBytes += 4096;
                            if (!allZero(page)) {
                                throw new IOException("Zero DATA header has nonzero required body");
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
                    ByteBuffer content = ByteBuffer.allocate(length);
                    SegmentFileIo.readFully(data, content, offset);
                    readBytes += length;
                    SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address,
                            config.bytesPerFile());
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
                                    && (bookie == null || bookie.locallyReadableContext(record.context()))) {
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
                    && (bookie == null || bookie.locallyReadableContext(selected.context()))
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

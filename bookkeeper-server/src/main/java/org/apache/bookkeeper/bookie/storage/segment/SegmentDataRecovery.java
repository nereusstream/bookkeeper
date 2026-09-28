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
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Quiesced isolated DATA validation; zero pages and recovery sync never grant reuse or entry authority. */
final class SegmentDataRecovery {
    record Scan(long batches, long records, long validatedBytes, long zeroPages) { }

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

    private static final class QuiescedFiles implements AutoCloseable {
        private final FileChannel guard;
        private final FileLock lock;

        private QuiescedFiles(FileChannel guard, FileLock lock) {
            this.guard = guard;
            this.lock = lock;
        }

        private static QuiescedFiles acquire(IsolatedSegmentConfig config) throws IOException {
            FileChannel guard = FileChannel.open(config.directory().resolve("isolated.lock"),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            try {
                FileLock lock;
                try {
                    lock = guard.tryLock();
                } catch (OverlappingFileLockException overlapping) {
                    throw new IOException("DATA writer is active", overlapping);
                }
                if (lock == null) {
                    throw new IOException("DATA writer is active");
                }
                return new QuiescedFiles(guard, lock);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    guard.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        @Override
        public void close() throws IOException {
            try {
                lock.release();
            } finally {
                guard.close();
            }
        }
    }

    /** Executes the read or comparison under writer exclusion in the isolated no-MOVE/FREE scope. */
    static <T> Optional<T> withLocallyReadableSingleStreamLocation(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, ProfileLedgerContext context,
            long entryId, int maxBatches, int maxEntries, SelectedLocationAction<T> action) throws IOException {
        if (!bookie.matchesScope(config, identity) || !arena.matchesScope(config, identity)) {
            throw new IOException("Recovered location authority belongs to another storage scope");
        }
        try (QuiescedFiles files = QuiescedFiles.acquire(config)) {
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
            batches.sort(Comparator.comparingLong(SegmentBatchFormat.Address::sequence));
            long expected = firstSequence;
            for (SegmentBatchFormat.Address address : batches) {
                if (address.sequence() != expected || expected == Long.MAX_VALUE) {
                    throw new IOException("Physical stream has a sequence gap, duplicate or overflow");
                }
                expected++;
            }
            return expected - 1;
        }
    }

    static final class PhysicalInventory {
        private final IsolatedSegmentConfig sourceConfig;
        private final SegmentFileHeader sourceIdentity;
        private final Scan scan;
        private final List<SegmentPointRead.Location> entries;
        private final List<Long> grantedHighWatermarks;

        private PhysicalInventory(IsolatedSegmentConfig sourceConfig, SegmentFileHeader sourceIdentity,
                                  Scan scan, List<SegmentPointRead.Location> entries,
                                  List<Long> grantedHighWatermarks) {
            this.sourceConfig = sourceConfig;
            this.sourceIdentity = sourceIdentity;
            this.scan = scan;
            this.entries = List.copyOf(entries);
            this.grantedHighWatermarks = List.copyOf(grantedHighWatermarks);
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

        List<SegmentPointRead.Location> entries() {
            return entries;
        }

        List<Long> grantedHighWatermarks() {
            return grantedHighWatermarks;
        }

    }

    static Scan inspect(IsolatedSegmentConfig config, SegmentFileHeader identity,
                        SegmentArenaControlLog authority) throws IOException {
        return scan(config, identity, authority, null, 0, null, null).scan();
    }

    /** A successful scan forces the checked DATA files; it does not establish Bookie or locator authority. */
    static Scan validateAndSync(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                SegmentArenaControlLog authority, SegmentFileIo syncIo) throws IOException {
        if (syncIo == null) {
            throw new IllegalArgumentException("DATA recovery sync requires an I/O backend");
        }
        return scan(config, identity, authority, syncIo, 0, null, null).scan();
    }

    /** The caller retains the isolated writer lock while consuming the scan and opening DATA files. */
    static Scan validateAndSyncWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                           SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                           FileLock writerLock) throws IOException {
        if (syncIo == null || writerLock == null) {
            throw new IllegalArgumentException("DATA recovery sync and writer lock required");
        }
        return scanWhileLocked(config, identity, authority, syncIo, 0, null, null, writerLock).scan();
    }

    /** Physical locations only: caller still needs the recovered stream prefix and Bookie selector/permissions. */
    static PhysicalInventory recoverPhysicalInventory(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                       int maxEntries) throws IOException {
        if (syncIo == null || maxEntries < 1) {
            throw new IllegalArgumentException("Bounded DATA recovery sync and inventory required");
        }
        return scan(config, identity, authority, syncIo, maxEntries, null, null);
    }

    /** Complete, bounded inventory under the writer lock already owned by the restart caller. */
    static PhysicalInventory recoverPhysicalInventoryWhileLocked(IsolatedSegmentConfig config,
                                                                 SegmentFileHeader identity,
                                                                 SegmentArenaControlLog authority,
                                                                 SegmentFileIo syncIo, int maxEntries,
                                                                 FileLock writerLock) throws IOException {
        if (syncIo == null || maxEntries < 1 || writerLock == null) {
            throw new IllegalArgumentException("Bounded DATA inventory, sync and writer lock required");
        }
        return scanWhileLocked(config, identity, authority, syncIo, maxEntries, null, null, writerLock);
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
                                                                  int maxEntries, QuiescedFiles files)
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
                ? scan(config, identity, authority, syncIo, maxEntries, probe, bookie)
                : scanWhileLocked(config, identity, authority, syncIo, maxEntries, probe, bookie, files.lock);
        if (bookie != null && !bookie.matchesScope(config, identity)) {
            throw new IOException("Bookie read route became unavailable during DATA recovery");
        }
        return new PhysicalStreamCandidate(config, identity, inventory.scan(), probe.contiguousThrough(firstSequence),
                inventory.entries(), inventory.grantedHighWatermarks());
    }

    private static PhysicalInventory scan(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                          SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                          int maxEntries, StreamProbe stream,
                                          SegmentBookieControlLog bookie) throws IOException {
        try (QuiescedFiles files = QuiescedFiles.acquire(config)) {
            return scanWhileLocked(config, identity, authority, syncIo, maxEntries, stream, bookie, files.lock);
        }
    }

    private static PhysicalInventory scanWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentFileIo syncIo,
                                                    int maxEntries, StreamProbe stream,
                                                    SegmentBookieControlLog bookie, FileLock writerLock)
            throws IOException {
        if (!writerLock.isValid() || !authority.matchesScope(config, identity)) {
            throw new IOException("Arena authority does not match the DATA scope");
        }
        long batches = 0;
        long records = 0;
        long validatedBytes = 0;
        long zeroPages = 0;
        List<SegmentPointRead.Location> entries = new ArrayList<>();
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
                for (long offset = 4096; offset < config.bytesPerFile();) {
                    if (stream != null && !authority.hasAppliedStreamAt(file, offset,
                            stream.shard, stream.generation.value())) {
                        offset += 4096;
                        continue;
                    }
                    ByteBuffer page = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(data, page, offset);
                    if (allZero(page)) {
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
                    if (!authority.hasAppliedGrant(file, offset, length, address.shard(),
                            address.streamGeneration().value(), address.allocation(),
                            address.generation().value())) {
                        throw new IOException("DATA batch has no matching applied grant");
                    }
                    ByteBuffer content = ByteBuffer.allocate(length);
                    SegmentFileIo.readFully(data, content, offset);
                    SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address,
                            config.bytesPerFile());
                    for (SegmentBatchParser.Record record : parsed.records()) {
                        if (SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                                != address.shard()) {
                            throw new IOException("DATA record belongs to another logical shard");
                        }
                    }
                    if (stream != null) {
                        stream.observe(address);
                    }
                    if (maxEntries > 0) {
                        List<SegmentPointRead.Location> locations = SegmentPointRead.locations(parsed);
                        for (int i = 0; i < parsed.records().size(); i++) {
                            SegmentBatchParser.Record record = parsed.records().get(i);
                            if (record.origin() == SegmentBatchFormat.ENTRY_DATA
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
        return new PhysicalInventory(config, identity, new Scan(batches, records, validatedBytes, zeroPages),
                entries, grantedHighWatermarks);
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

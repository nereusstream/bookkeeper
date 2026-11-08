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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/** Rebuildable, WAL-disabled locator store. Calls belong to startup/maintenance I/O, never an append owner. */
final class SegmentDerivedIndex implements AutoCloseable {
    static final long BLOCK_CACHE_BYTES = 1024 * 1024;
    private static final long WRITE_BUFFER_BYTES = 1024 * 1024;
    private static final int MAX_WRITE_BUFFERS = 2;
    // Configured capacities only; RocksDB memtable overshoot and other native overhead remain separate.
    static final long CONFIGURED_CAPACITY_BYTES = BLOCK_CACHE_BYTES + WRITE_BUFFER_BYTES * MAX_WRITE_BUFFERS;
    private static final byte[] BINDING_KEY = {0};
    private static final byte[] LOCATION_BEGIN = {16};
    private static final byte[] LOCATION_END = {17};
    private static final long BINDING_MAGIC = 0x5557494442303031L; // UWIDB001
    private static final long KEY_MAGIC = 0x55574944584b3031L; // UWIDXK01
    private static final long LOCATION_MAGIC = 0x55574944584c3031L; // UWIDXL01
    private static final long COVERAGE_MAGIC = 0x555749434f563031L; // UWICOV01
    private static final int LOCATION_BYTES = 160;
    private static final int LOCATION_KEY_BYTES = 77;
    private static final int BINDING_BYTES = 64;
    private static final int BATCH_ENTRIES = 256;

    record Coverage(long publication, UUID indexGeneration, long arenaCut, long bookieCut,
                    long entries, List<SegmentDataRecovery.StreamCoverage> streams) {
        Coverage {
            streams = List.copyOf(streams);
        }
    }

    record RecoveryCoverage(IsolatedSegmentConfig sourceConfig, SegmentFileHeader sourceIdentity,
                            Coverage coverage, List<SegmentPointRead.Location> entries) {
        RecoveryCoverage {
            entries = List.copyOf(entries);
        }
    }

    /** Bounded Java seed graph/codec workspace; opaque digest and RocksDB native state remain separate. */
    static long recoveryCapacity(SegmentControlCapacity.Layout layout, int entries, int streams, long moves) {
        long location = SegmentControlCapacity.sum(layout.object(SegmentPointRead.Location.class),
                layout.object(SegmentBatchFormat.Address.class),
                SegmentControlCapacity.times(2, layout.object(Generation.class)), layout.context());
        long stream = SegmentControlCapacity.sum(layout.object(SegmentDataRecovery.StreamCoverage.class),
                layout.object(Generation.class));
        return SegmentControlCapacity.sum(SegmentControlCapacity.times(entries, location),
                layout.object(RecoveryCoverage.class), SegmentControlCapacity.times(2, layout.object(Coverage.class)),
                SegmentControlCapacity.times(2, layout.object(Published.class)), layout.object(Contents.class),
                SegmentControlCapacity.times(3, layout.object(UUID.class)),
                SegmentControlCapacity.times(2L * streams, stream),
                SegmentControlCapacity.times(3, layout.list()),
                SegmentControlCapacity.times(4, layout.refs(entries)),
                SegmentControlCapacity.times(8, layout.refs(streams)),
                layout.hashSet(), SegmentControlCapacity.times(entries, layout.hashEntry()),
                layout.snapshot(moves, 0),
                // Two candidate publication pages and one row decode/CRC/key comparison stack.
                SegmentControlCapacity.times(2, layout.bytes(4096)),
                SegmentControlCapacity.times(8, layout.buffer()),
                SegmentControlCapacity.times(2, layout.bytes(LOCATION_KEY_BYTES)), layout.bytes(LOCATION_BYTES),
                layout.bytes(60), layout.bytes(16), layout.bytes(36),
                SegmentControlCapacity.times(6, layout.bytes(32)),
                layout.object(java.util.zip.CRC32C.class), location,
                SegmentControlCapacity.times(8, layout.captured(8)));
    }

    /** One fresh point lookup; selected locator ownership continues in its admitted read command. */
    static long pointReadWorkspaceCapacity(SegmentControlCapacity.Layout layout) {
        long locator = SegmentControlCapacity.sum(layout.object(SegmentPointRead.Location.class),
                layout.object(SegmentBatchFormat.Address.class),
                SegmentControlCapacity.times(2, layout.object(Generation.class)), layout.context());
        return SegmentControlCapacity.sum(layout.bytes(LOCATION_KEY_BYTES), layout.bytes(LOCATION_BYTES),
                SegmentControlCapacity.times(2, layout.bytes(60)), layout.bytes(16), layout.bytes(36),
                SegmentControlCapacity.times(6, layout.buffer()), layout.object(java.util.zip.CRC32C.class),
                layout.object(java.util.Optional.class), locator);
    }

    private record Contents(long entries, byte[] digest) { }
    private record Published(Coverage coverage, byte[] digest) { }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final int maxEntries;
    private final byte[] scope;
    private final LRUCache cache;
    private final Options options;
    private final WriteOptions writes;
    private final FlushOptions flush;
    private RocksDB db;
    private UUID indexGeneration;
    private Coverage pendingCoverage;
    private boolean closed;
    private boolean failed;

    static Path directory(IsolatedSegmentConfig config) {
        return config.directory().resolve("derived-index");
    }

    static Path publication(IsolatedSegmentConfig config, int slot) {
        if (slot < 0 || slot > 1) {
            throw new IllegalArgumentException("Invalid index coverage slot");
        }
        return config.directory().resolve("index-coverage-" + (slot == 0 ? "a" : "b"));
    }

    static SegmentDerivedIndex open(IsolatedSegmentConfig config, SegmentFileHeader identity, int maxEntries)
            throws IOException {
        return open(config, identity, maxEntries, null);
    }

    static SegmentDerivedIndex open(IsolatedSegmentConfig config, SegmentFileHeader identity, int maxEntries,
            SegmentOwnedFiles files) throws IOException {
        return new SegmentDerivedIndex(config, identity, maxEntries, files);
    }

    /** A disposable index may be absent. An existing index must belong to this exact member. */
    static void requireExistingScope(IsolatedSegmentConfig config, SegmentFileHeader identity) throws IOException {
        Path directory = directory(config);
        try {
            var attrs = Files.readAttributes(directory, java.nio.file.attribute.BasicFileAttributes.class,
                    LinkOption.NOFOLLOW_LINKS);
            if (!attrs.isDirectory() || !directory.equals(directory.toRealPath())) {
                throw new IOException("Derived index directory must be canonical");
            }
        } catch (java.nio.file.NoSuchFileException absent) {
            return;
        }
        RocksDB.loadLibrary();
        try (var options = new Options().setCreateIfMissing(false);
                var logger = new org.rocksdb.Logger(options) {
                    @Override
                    protected void log(org.rocksdb.InfoLogLevel level, String message) {
                        // Scope inspection does not publish an info log in the existing index.
                    }
                };
                var db = RocksDB.openReadOnly(options.setLogger(logger), directory.toString())) {
            byte[] binding = new byte[BINDING_BYTES];
            if (db.get(BINDING_KEY, binding) != binding.length) {
                throw new IOException("Missing or invalid derived index binding");
            }
            decodeBinding(binding, digest().digest(identity.encode(config, -4).array()));
        } catch (RocksDBException failure) {
            throw new IOException("Cannot inspect existing derived index", failure);
        }
    }

    private final SegmentOwnedFiles ownedFiles;

    private SegmentDerivedIndex(IsolatedSegmentConfig config, SegmentFileHeader identity, int maxEntries,
            SegmentOwnedFiles files) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.ownedFiles = files;
        if (maxEntries < 1) {
            throw new IllegalArgumentException("Positive derived rebuild capacity required");
        }
        this.maxEntries = maxEntries;
        identity.requireStoredManifest(config, -4, files);
        scope = digest().digest(identity.encode(config, identity.manifest() == null ? -3 : -4).array());
        RocksDB.loadLibrary();
        cache = new LRUCache(BLOCK_CACHE_BYTES, -1, true);
        options = new Options().setCreateIfMissing(true).setUseFsync(true)
                .setWriteBufferSize(WRITE_BUFFER_BYTES).setMaxWriteBufferNumber(MAX_WRITE_BUFFERS)
                .setMaxBackgroundJobs(2).setMaxOpenFiles(32)
                .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
        writes = new WriteOptions().setSync(false).setDisableWAL(true);
        flush = new FlushOptions().setWaitForFlush(true);
        try {
            boolean existing = Files.exists(directory(config), LinkOption.NOFOLLOW_LINKS);
            if (Files.isSymbolicLink(directory(config))) {
                throw new IOException("Derived index directory is a symbolic link");
            }
            db = RocksDB.open(options, directory(config).toString());
            byte[] binding = getExact(BINDING_KEY, BINDING_BYTES);
            if (binding == null) {
                if (existing) {
                    throw new IOException("Incomplete derived index binding; a full rebuild is required");
                }
                indexGeneration = UUID.randomUUID();
                db.put(writes, BINDING_KEY, binding());
                db.flush(flush);
                IsolatedSegmentFiles.syncDirectory(config.directory());
            } else {
                indexGeneration = decodeBinding(binding, scope);
            }
        } catch (RocksDBException failure) {
            close();
            throw new IOException("Cannot open derived index", failure);
        } catch (IOException | RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    private byte[] binding() {
        return ByteBuffer.allocate(BINDING_BYTES).putLong(BINDING_MAGIC).putInt(1).putInt(LOCATION_BYTES)
                .putLong(indexGeneration.getMostSignificantBits()).putLong(indexGeneration.getLeastSignificantBits())
                .put(scope).array();
    }

    private static UUID decodeBinding(byte[] bytes, byte[] scope) throws IOException {
        if (bytes.length != 64) {
            throw new IOException("Invalid derived index binding length");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getLong() != BINDING_MAGIC || b.getInt() != 1 || b.getInt() != LOCATION_BYTES
                || !Arrays.equals(scope, Arrays.copyOfRange(bytes, 32, 64))) {
            throw new IOException("Derived index scope or format mismatch");
        }
        var generation = new UUID(b.getLong(), b.getLong());
        if (generation.equals(new UUID(0, 0))) {
            throw new IOException("Invalid derived index generation");
        }
        return generation;
    }

    /** A fresh Get returns a physical locator only, never payload success or permission authority. */
    synchronized Optional<SegmentPointRead.Location> get(ProfileLedgerContext context, long entryId)
            throws IOException {
        requireOpen();
        try {
            byte[] bytes = getExact(key(context, entryId), LOCATION_BYTES);
            if (bytes == null) {
                return Optional.empty();
            }
            SegmentPointRead.Location location = decode(bytes);
            if (!context.equals(location.context()) || entryId != location.entryId()) {
                throw new IOException("Derived index key differs from its location");
            }
            return Optional.of(location);
        } catch (RocksDBException failure) {
            throw new IOException("Derived index Get failed", failure);
        }
    }

    /** Optional startup seed; DATA headers/tail and current Bookie permissions still require verification. */
    synchronized Optional<RecoveryCoverage> recoveryCoverage(SegmentArenaControlLog arena,
                                                             SegmentBookieControlLog bookie) throws IOException {
        return recoveryCoverage(arena, bookie, maxEntries);
    }

    synchronized Optional<RecoveryCoverage> recoveryCoverage(SegmentArenaControlLog arena,
                                                             SegmentBookieControlLog bookie, int capacity)
            throws IOException {
        if (capacity < 1) {
            throw new IllegalArgumentException("Positive optional recovery seed capacity required");
        }
        requireOpen();
        if (!arena.matchesScope(config, identity)) {
            return Optional.empty();
        }
        try {
            Coverage selected = coverage().orElse(null);
            if (selected == null || selected.entries() > capacity || !arena.matchesScope(config, identity)
                    || selected.arenaCut() > arena.appliedThrough()
                    || selected.bookieCut() > bookie.recoveryReadCut(config, identity)) {
                return Optional.empty();
            }
            SegmentDataRecovery.StreamCoverage[] streams =
                    new SegmentDataRecovery.StreamCoverage[config.logicalShards()];
            for (SegmentDataRecovery.StreamCoverage stream : selected.streams()) {
                SegmentArenaControlLog.SingleStream current = arena.restartStreamForShard(stream.shard()).orElse(null);
                if (current == null || !current.generation().equals(stream.generation())
                        || current.firstSequence() != stream.firstSequence()) {
                    return Optional.empty();
                }
                streams[stream.shard()] = stream;
            }
            List<SegmentPointRead.Location> entries = new ArrayList<>();
            try (RocksIterator cursor = db.newIterator()) {
                for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                        cursor.next()) {
                    SegmentPointRead.Location location = decode(boundedValue(cursor));
                    SegmentBatchFormat.Address address = location.address();
                    SegmentDataRecovery.StreamCoverage stream = streams[address.shard()];
                    if (stream == null || !address.streamGeneration().equals(stream.generation())
                            || !arena.resolveLocation(location).equals(location)
                            || arena.currentSelectorSequence(location.context(), location.entryId())
                                    > selected.arenaCut()
                            || address.sequence() < stream.firstSequence() || address.sequence() > stream.through()
                            || !arena.dataFileAvailable(address.fileIndex())
                            || !arena.hasAppliedGrant(address.fileIndex(), address.offset(), location.batchLength(),
                                    address.shard(), address.streamGeneration().value(), address.allocation(),
                                    address.generation().value())) {
                        return Optional.empty();
                    }
                    entries.add(location);
                }
                cursor.status();
            }
            HashSet<SegmentPointRead.Location> indexed = new HashSet<>(entries);
            for (SegmentPointRead.Location target : arena.currentMoveLocations()) {
                if (bookie.locallyReadableContext(target.context()) && !indexed.contains(target)) {
                    return Optional.empty();
                }
            }
            return Optional.of(new RecoveryCoverage(config, identity, selected, entries));
        } catch (IOException | RocksDBException invalidOptionalCoverage) {
            // Discard only the optional proof; the caller must perform the complete mandatory DATA recovery.
            return Optional.empty();
        }
    }

    /** Replace from the complete synced, route-selected DATA scan. No coverage is advanced by WriteBatch. */
    synchronized void replace(SegmentDataRecovery.PhysicalInventory inventory, SegmentArenaControlLog arena,
                              SegmentBookieControlLog bookie) throws IOException {
        requireOpen();
        if (!config.sameStorageScope(inventory.sourceConfig()) || !identity.equals(inventory.sourceIdentity())
                || inventory.entries().size() > maxEntries) {
            throw new IOException("Derived rebuild inventory has another scope or exceeds capacity");
        }
        inventory.requireCurrentArena(arena);
        long arenaCut = arena.appliedThrough();
        long bookieCut = bookie.recoveryReadCut(config, identity);
        List<SegmentDataRecovery.StreamCoverage> streams = inventory.indexStreams(arena);
        List<SegmentPointRead.Location> entries = new ArrayList<>(inventory.entries());
        entries.sort(Comparator.comparing(location -> key(location.context(), location.entryId()),
                Arrays::compareUnsigned));
        byte[] previous = null;
        for (SegmentPointRead.Location location : entries) {
            validate(location);
            byte[] current = key(location.context(), location.entryId());
            if (Arrays.equals(previous, current) || !bookie.locallyReadableContext(location.context())) {
                throw new IOException("Derived rebuild coordinate is duplicate or no longer readable");
            }
            previous = current;
        }
        pendingCoverage = null;
        try {
            // Retire old coverage before destructive edits: the identity and range deletion share one CF batch.
            indexGeneration = UUID.randomUUID();
            try (WriteBatch batch = new WriteBatch()) {
                batch.put(BINDING_KEY, binding());
                batch.deleteRange(LOCATION_BEGIN, LOCATION_END);
                db.write(writes, batch);
            }
            db.flush(flush);
            for (int first = 0; first < entries.size(); first += BATCH_ENTRIES) {
                try (WriteBatch batch = new WriteBatch()) {
                    for (int i = first; i < Math.min(first + BATCH_ENTRIES, entries.size()); i++) {
                        SegmentPointRead.Location location = entries.get(i);
                        batch.put(key(location.context(), location.entryId()),
                                encodeLocation(indexGeneration, location));
                    }
                    db.write(writes, batch);
                }
            }
            inventory.requireCurrentArena(arena);
            if (arenaCut != arena.appliedThrough() || bookieCut != bookie.recoveryReadCut(config, identity)) {
                throw new IOException("Control cut changed during derived rebuild");
            }
            pendingCoverage = new Coverage(0, indexGeneration, arenaCut, bookieCut, entries.size(), streams);
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Derived index replacement failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    synchronized Coverage rebuild(SegmentDataRecovery.PhysicalInventory inventory, SegmentArenaControlLog arena,
                                  SegmentBookieControlLog bookie) throws IOException {
        replace(inventory, arena, bookie);
        return checkpoint(new SegmentFileIo());
    }

    /** Keep the verified index generation; add validated tail rows and retire locally unreadable rows. */
    synchronized Coverage updateFromRecovery(SegmentDataRecovery.PhysicalInventory inventory,
                                             SegmentArenaControlLog arena, SegmentBookieControlLog bookie)
            throws IOException {
        requireOpen();
        if (!inventory.usesIndexCoverage() || !config.sameStorageScope(inventory.sourceConfig())
                || !identity.equals(inventory.sourceIdentity()) || inventory.entries().size() > maxEntries) {
            throw new IOException("Incremental recovery needs a verified bounded index inventory");
        }
        inventory.requireCurrentArena(arena);
        long arenaCut = arena.appliedThrough();
        long bookieCut = bookie.recoveryReadCut(config, identity);
        pendingCoverage = null;
        try {
            try (WriteBatch retired = new WriteBatch(); RocksIterator cursor = db.newIterator()) {
                int count = 0;
                for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                        cursor.next()) {
                    SegmentPointRead.Location location = decode(boundedValue(cursor));
                    if (!bookie.locallyReadableContext(location.context())) {
                        retired.delete(boundedKey(cursor));
                        if (++count == BATCH_ENTRIES) {
                            db.write(writes, retired);
                            retired.clear();
                            count = 0;
                        }
                    }
                }
                cursor.status();
                if (count != 0) {
                    db.write(writes, retired);
                }
            }
            List<SegmentPointRead.Location> missing = new ArrayList<>(BATCH_ENTRIES);
            for (SegmentPointRead.Location location : inventory.entries()) {
                if (!bookie.locallyReadableContext(location.context())) {
                    throw new IOException("Recovered coordinate is no longer readable");
                }
                SegmentPointRead.Location existing = get(location.context(), location.entryId()).orElse(null);
                if (existing == null) {
                    missing.add(location);
                    if (missing.size() == BATCH_ENTRIES) {
                        append(missing);
                        missing.clear();
                    }
                } else if (!existing.equals(location)) {
                    throw new IOException("Recovered DATA conflicts with the covered coordinate");
                }
            }
            if (!missing.isEmpty()) {
                append(missing);
            }
            inventory.requireCurrentArena(arena);
            if (arenaCut != arena.appliedThrough() || bookieCut != bookie.recoveryReadCut(config, identity)) {
                throw new IOException("Control cut changed during incremental derived recovery");
            }
            pendingCoverage = new Coverage(0, indexGeneration, arenaCut, bookieCut,
                    inventory.entries().size(), inventory.indexStreams(arena));
            return checkpoint(new SegmentFileIo());
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Incremental derived recovery failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** All original owners paused maintenance after fully indexing their captured DATA prefixes. */
    synchronized Coverage checkpointRuntime(List<SegmentHotDirectory.IndexCut> cuts,
                                              SegmentArenaControlLog arena, SegmentBookieControlLog bookie,
                                              SegmentFileIo io) throws IOException {
        requireOpen();
        if (cuts.size() != config.logicalShards() || !arena.matchesScope(config, identity)) {
            throw new IOException("Runtime index checkpoint needs the complete original owner set");
        }
        SegmentHotDirectory.IndexCut[] selected = new SegmentHotDirectory.IndexCut[config.logicalShards()];
        List<SegmentDataRecovery.StreamCoverage> streams = new ArrayList<>();
        long expected = 0;
        for (SegmentHotDirectory.IndexCut cut : cuts) {
            SegmentDataRecovery.StreamCoverage stream = cut.stream();
            int shard = stream.shard();
            if (shard < 0 || shard >= selected.length || selected[shard] != null || cut.entries() < 0
                    || stream.firstSequence() < 0 || stream.through() < stream.firstSequence() - 1) {
                throw new IOException("Invalid runtime owner cut");
            }
            selected[shard] = cut;
            expected += cut.entries();
            long first = arena.streamFirstSequence(shard, stream.generation().value()).orElse(-1);
            if (first < 0 && stream.through() == stream.firstSequence() - 1 && cut.entries() == 0) {
                continue; // A shard with no ALLOC/DATA has no physical stream to cover.
            }
            if (first != stream.firstSequence()) {
                throw new IOException("Runtime stream cut differs from applied Arena authority");
            }
            streams.add(stream);
        }
        if (expected > maxEntries) {
            throw new IOException("Runtime index cut exceeds the declared coordinate budget");
        }
        streams.sort(Comparator.comparingInt(SegmentDataRecovery.StreamCoverage::shard));
        long arenaCut = arena.appliedThrough();
        long bookieCut = bookie.indexAppliedCut(config, identity);
        int[] counts = new int[selected.length];
        try (RocksIterator cursor = db.newIterator()) {
            for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                    cursor.next()) {
                SegmentPointRead.Location location = decode(boundedValue(cursor));
                if (!Arrays.equals(boundedKey(cursor), key(location.context(), location.entryId()))) {
                    throw new IOException("Runtime index row differs from its key");
                }
                SegmentBatchFormat.Address address = location.address();
                SegmentHotDirectory.IndexCut cut = selected[address.shard()];
                SegmentDataRecovery.StreamCoverage stream = cut.stream();
                if (++counts[address.shard()] > cut.entries()
                        || !arena.resolveLocation(location).equals(location)
                        || !address.streamGeneration().equals(stream.generation())
                        || address.sequence() < stream.firstSequence() || address.sequence() > stream.through()
                        || !arena.dataFileAvailable(address.fileIndex())
                        || !arena.hasAppliedGrant(address.fileIndex(), address.offset(), location.batchLength(),
                                address.shard(), address.streamGeneration().value(), address.allocation(),
                                address.generation().value())) {
                    throw new IOException("Runtime index row is outside its complete DATA owner cut");
                }
            }
            cursor.status();
        } catch (RocksDBException failure) {
            throw new IOException("Runtime index cut cannot be validated", failure);
        }
        for (int shard = 0; shard < counts.length; shard++) {
            if (counts[shard] != selected[shard].entries()) {
                throw new IOException("Runtime index misses an occupied coordinate");
            }
        }
        pendingCoverage = new Coverage(0, indexGeneration, arenaCut, bookieCut, expected, streams);
        return checkpoint(io, false);
    }

    /** Bounded native deletion while every original index owner and Bookie entry admission is paused. */
    synchronized void retireTerminalEntries(SegmentBookieControlLog.ReclamationLease authority) throws IOException {
        requireOpen();
        pendingCoverage = null;
        try (WriteBatch retired = new WriteBatch(); RocksIterator cursor = db.newIterator()) {
            int count = 0;
            int seen = 0;
            for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                    cursor.next()) {
                SegmentPointRead.Location location = decode(boundedValue(cursor));
                if (++seen > maxEntries
                        || !Arrays.equals(boundedKey(cursor), key(location.context(), location.entryId()))) {
                    throw new IOException("Terminal cleanup encountered an invalid bounded index row");
                }
                if (authority.terminalAtCut(location.context())) {
                    retired.delete(boundedKey(cursor));
                    if (++count == BATCH_ENTRIES) {
                        db.write(writes, retired);
                        retired.clear();
                        count = 0;
                    }
                }
            }
            cursor.status();
            if (count != 0) {
                db.write(writes, retired);
            }
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Terminal derived index cleanup failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** Original owners have drained old Puts and paused; current MOVE authority alone may replace a row. */
    synchronized void refreshMovedLocations(SegmentArenaControlLog arena, IsolatedSegmentFiles files,
                                            IsolatedSegmentFiles.ReclamationGuard protection,
                                            ByteBuffer workspace) throws IOException {
        requireOpen();
        protection.requireHeld(files, arena);
        arena.requireMovePreparation(files);
        long cut = arena.appliedThrough();
        pendingCoverage = null;
        try (WriteBatch moved = new WriteBatch(); RocksIterator cursor = db.newIterator()) {
            int count = 0;
            int seen = 0;
            for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                    cursor.next()) {
                SegmentPointRead.Location old = decode(boundedValue(cursor));
                if (++seen > maxEntries || !Arrays.equals(boundedKey(cursor), key(old.context(), old.entryId()))) {
                    throw new IOException("MOVE maintenance encountered an invalid bounded index row");
                }
                if (arena.currentSelectorSequence(old.context(), old.entryId()) < 0) {
                    continue;
                }
                SegmentPointRead.Location target = arena.resolveLocation(old);
                SegmentPointRead.Result verified = SegmentPointRead.readWithScratch(config, identity, arena,
                        target, workspace);
                arena.validateSelectedRecord(target, verified.record());
                validate(target);
                moved.put(boundedKey(cursor), encodeLocation(indexGeneration, target));
                if (++count == BATCH_ENTRIES) {
                    db.write(writes, moved);
                    moved.clear();
                    count = 0;
                }
            }
            cursor.status();
            if (count != 0) {
                db.write(writes, moved);
            }
            protection.requireHeld(files, arena);
            arena.requireMovePreparation(files);
            if (arena.appliedThrough() != cut) {
                throw new IOException("Arena selector changed during native MOVE maintenance");
            }
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Native MOVE maintenance failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** Query-visible append only. The caller keeps hot ownership; this never advances durable coverage. */
    synchronized void append(List<SegmentPointRead.Location> locations) throws IOException {
        requireOpen();
        if (locations.isEmpty() || locations.size() > BATCH_ENTRIES) {
            throw new IllegalArgumentException("Derived append needs 1..256 locators");
        }
        for (SegmentPointRead.Location location : locations) {
            validate(location);
        }
        try (WriteBatch batch = new WriteBatch()) {
            for (SegmentPointRead.Location location : locations) {
                byte[] key = key(location.context(), location.entryId());
                byte[] value = encodeLocation(indexGeneration, location);
                byte[] old = getExact(key, LOCATION_BYTES);
                if (old != null && !Arrays.equals(old, value)) {
                    throw new IOException("Derived append would replace an existing coordinate");
                }
                batch.put(key, value);
            }
            // The old cold cut cannot describe subsequent runtime updates, even if they later auto-flush.
            pendingCoverage = null;
            db.write(writes, batch);
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Derived index append failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    /** Flush the single CF, then force a bounded independent coverage page. Never publish a memtable cut. */
    synchronized Coverage checkpoint(SegmentFileIo io) throws IOException {
        return checkpoint(io, true);
    }

    private Coverage checkpoint(SegmentFileIo io, boolean failOnPublicationError) throws IOException {
        requireOpen();
        Objects.requireNonNull(io, "io");
        if (pendingCoverage == null) {
            throw new IOException("No complete derived rebuild is available for coverage");
        }
        boolean publishing = false;
        try {
            db.flush(flush);
            Contents contents = contents();
            if (contents.entries() != pendingCoverage.entries()) {
                throw new IOException("Derived index does not contain the complete selected inventory");
            }
            long generation = nextPublication();
            if (generation < 1) {
                throw new IOException("Index coverage publication generation exhausted");
            }
            Coverage published = new Coverage(generation, indexGeneration, pendingCoverage.arenaCut(),
                    pendingCoverage.bookieCut(), contents.entries(), pendingCoverage.streams());
            Path target = publication(config, (int) ((generation - 1) % 2));
            publishing = true;
            try (FileChannel file = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                io.writeFully(file, encodePublication(scope, published, contents.digest()), 0);
                file.truncate(4096);
                io.force(file, true);
            }
            IsolatedSegmentFiles.syncDirectory(config.directory());
            Published verified = readPublication(target);
            if (!published.equals(verified.coverage()) || !Arrays.equals(contents.digest(), verified.digest())) {
                throw new IOException("Index coverage publication verification failed");
            }
            return published;
        } catch (RocksDBException failure) {
            failed = true;
            throw new IOException("Derived index flush failed", failure);
        } catch (IOException | RuntimeException | Error failure) {
            // A failed optional page write does not invalidate already query-visible native rows.
            failed |= !publishing || failOnPublicationError;
            pendingCoverage = null;
            throw failure;
        }
    }

    private long nextPublication() throws IOException {
        long latest = 0;
        for (int slot = 0; slot < 2; slot++) {
            try {
                Coverage current = readPublication(publication(config, slot)).coverage();
                if (current.indexGeneration().equals(indexGeneration)) {
                    latest = Math.max(latest, current.publication());
                }
            } catch (IOException | IllegalArgumentException invalid) {
                // Optional invalid pages do not reset the clock of another valid same-generation page.
            }
        }
        if (latest == Long.MAX_VALUE) {
            throw new IOException("Index coverage publication generation exhausted");
        }
        return latest + 1;
    }

    synchronized Optional<Coverage> coverage() throws IOException {
        requireOpen();
        Published selected = null;
        int selectedSlot = -1;
        for (int slot = 0; slot < 2; slot++) {
            Published candidate;
            try {
                candidate = readPublication(publication(config, slot));
            } catch (IOException | IllegalArgumentException invalid) {
                continue; // Optional derived coverage can be discarded; DATA/control are still mandatory.
            }
            if (!candidate.coverage().indexGeneration().equals(indexGeneration)) {
                continue;
            }
            if (selected != null && candidate.coverage().publication() == selected.coverage().publication()
                    && (!candidate.coverage().equals(selected.coverage())
                            || !Arrays.equals(candidate.digest(), selected.digest()))) {
                throw new IOException("Conflicting derived coverage publications");
            }
            if (selected == null || candidate.coverage().publication() > selected.coverage().publication()) {
                selected = candidate;
                selectedSlot = slot;
            }
        }
        if (selected == null) {
            return Optional.empty();
        }
        try {
            Contents contents = contents();
            if (contents.entries() != selected.coverage().entries()
                    || !Arrays.equals(contents.digest(), selected.digest())) {
                return Optional.empty();
            }
            // A complete page left readable by a failed publisher is synced before recovery exposes coverage.
            try (FileChannel file = FileChannel.open(publication(config, selectedSlot), StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                file.force(true);
            }
            IsolatedSegmentFiles.syncDirectory(config.directory());
            return Optional.of(selected.coverage());
        } catch (RocksDBException failure) {
            throw new IOException("Derived coverage contents cannot be verified", failure);
        }
    }

    private byte[] getExact(byte[] key, int size) throws IOException, RocksDBException {
        byte[] value = new byte[size];
        int actual = db.get(key, value);
        if (actual == RocksDB.NOT_FOUND) {
            return null;
        }
        if (actual != size) {
            throw new IOException("Derived index value has an invalid bounded length");
        }
        return value;
    }

    private static byte[] boundedKey(RocksIterator cursor) throws IOException {
        byte[] key = new byte[LOCATION_KEY_BYTES];
        int actual = cursor.key(key);
        if (actual < 1 || key[0] == LOCATION_BEGIN[0] && actual != key.length) {
            throw new IOException("Derived index location key has an invalid bounded length");
        }
        return key;
    }

    private static byte[] boundedValue(RocksIterator cursor) throws IOException {
        byte[] value = new byte[LOCATION_BYTES];
        if (cursor.value(value) != value.length) {
            throw new IOException("Derived index location value has an invalid bounded length");
        }
        return value;
    }

    private Contents contents() throws IOException, RocksDBException {
        MessageDigest hash = digest();
        long count = 0;
        try (RocksIterator cursor = db.newIterator()) {
            for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && boundedKey(cursor)[0] == LOCATION_BEGIN[0];
                    cursor.next()) {
                if (++count > maxEntries) {
                    throw new IOException("Derived contents exceed declared rebuild capacity");
                }
                byte[] key = boundedKey(cursor);
                byte[] value = boundedValue(cursor);
                SegmentPointRead.Location location = decode(value);
                if (!Arrays.equals(key, key(location.context(), location.entryId()))) {
                    throw new IOException("Derived contents key differs from its location");
                }
                hash.update(key);
                hash.update(value);
            }
            cursor.status();
        }
        return new Contents(count, hash.digest());
    }

    static byte[] key(ProfileLedgerContext context, long entryId) {
        if (entryId < 0) {
            throw new IllegalArgumentException("Negative index coordinate");
        }
        return ByteBuffer.allocate(LOCATION_KEY_BYTES).put(LOCATION_BEGIN[0]).putLong(KEY_MAGIC)
                .put(context.encode()).putLong(entryId).array();
    }

    static byte[] encodeLocation(UUID indexGeneration, SegmentPointRead.Location location) {
        SegmentBatchFormat.Address a = location.address();
        ByteBuffer b = ByteBuffer.allocate(LOCATION_BYTES).putLong(LOCATION_MAGIC)
                .putLong(indexGeneration.getMostSignificantBits()).putLong(indexGeneration.getLeastSignificantBits())
                .putInt(a.fileIndex()).putInt(a.shard()).putLong(a.allocation()).putLong(a.generation().value())
                .putLong(a.streamGeneration().value()).putLong(a.sequence()).putLong(a.offset())
                .putInt(location.batchLength()).putLong(location.recordOffset()).putInt(location.recordLength())
                .put(location.context().encode()).putLong(location.entryId());
        checksum(b);
        return b.array();
    }

    private SegmentPointRead.Location decode(byte[] bytes) throws IOException {
        if (bytes.length != LOCATION_BYTES) {
            throw new IOException("Invalid derived location length");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        verifyChecksum(b);
        if (b.getLong() != LOCATION_MAGIC || b.getLong() != indexGeneration.getMostSignificantBits()
                || b.getLong() != indexGeneration.getLeastSignificantBits()) {
            throw new IOException("Derived location format or generation mismatch");
        }
        try {
            SegmentBatchFormat.Address address = new SegmentBatchFormat.Address(b.getInt(), b.getInt(), b.getLong(),
                    new Generation(b.getLong()), new Generation(b.getLong()), b.getLong(), b.getLong());
            int batchLength = b.getInt();
            long recordOffset = b.getLong();
            int recordLength = b.getInt();
            byte[] context = new byte[60];
            b.get(context);
            SegmentPointRead.Location location = new SegmentPointRead.Location(address, batchLength, recordOffset,
                    recordLength, ProfileLedgerContext.decode(context), b.getLong());
            validate(location);
            return location;
        } catch (IllegalArgumentException | ArithmeticException invalid) {
            throw new IOException("Invalid derived location coordinates", invalid);
        }
    }

    private void validate(SegmentPointRead.Location location) throws IOException {
        SegmentBatchFormat.Address a = location.address();
        long end;
        try {
            end = Math.addExact(a.offset(), location.batchLength());
            if (a.fileIndex() >= config.dataFiles() || a.shard() >= config.logicalShards()
                    || !identity.ownsShard(a.shard())
                    || a.shard() != SegmentOwnerLoop.shardFor(location.context().ledgerId(), config.logicalShards())
                    || a.offset() < 4096 || end > config.bytesPerFile()
                    || location.batchLength() < 4096 || location.batchLength() > 2 * 1024 * 1024
                    || location.batchLength() % 4096 != 0 || location.entryId() < 0
                    || location.recordLength() < SegmentBatchFormat.RECORD_HEADER + 36
                    || location.recordOffset() < a.offset() + SegmentBatchFormat.HEADER
                    || Math.addExact(location.recordOffset(), location.recordLength())
                            > end - SegmentBatchFormat.FOOTER) {
                throw new IOException("Derived location is outside its bounded DATA record");
            }
        } catch (ArithmeticException invalid) {
            throw new IOException("Derived location overflows", invalid);
        }
    }

    static ByteBuffer encodePublication(byte[] scope, Coverage value, byte[] contents) {
        ByteBuffer b = ByteBuffer.allocate(4096).putLong(COVERAGE_MAGIC).putInt(1).putInt(value.streams().size())
                .putLong(value.publication()).putLong(value.indexGeneration().getMostSignificantBits())
                .putLong(value.indexGeneration().getLeastSignificantBits()).put(scope)
                .putLong(value.arenaCut()).putLong(value.bookieCut()).putLong(value.entries()).put(contents);
        for (SegmentDataRecovery.StreamCoverage stream : value.streams()) {
            b.putInt(stream.shard()).putInt(0).putLong(stream.generation().value())
                    .putLong(stream.firstSequence()).putLong(stream.through());
        }
        checksum(b);
        return b.clear();
    }

    private Published readPublication(Path path) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(4096);
        try (FileChannel file = ownedFiles == null
                ? FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
                : SegmentOwnedFiles.openRegularReadOnly(ownedFiles, path)) {
            if (file.size() != 4096) {
                throw new IOException("Invalid index coverage page size");
            }
            SegmentFileIo.readFully(file, b, 0);
        }
        verifyChecksum(b);
        if (b.getLong(0) != COVERAGE_MAGIC || b.getInt(8) != 1
                || !Arrays.equals(scope, Arrays.copyOfRange(b.array(), 40, 72))) {
            throw new IOException("Invalid index coverage scope or format");
        }
        int count = b.getInt(12);
        if (count < 0 || count > config.logicalShards() || b.getLong(16) < 1
                || b.getLong(72) < -1 || b.getLong(80) < -1
                || b.getLong(88) < 0 || b.getLong(88) > maxEntries) {
            throw new IOException("Invalid index coverage bounds");
        }
        List<SegmentDataRecovery.StreamCoverage> streams = new ArrayList<>();
        b.position(128);
        int previous = -1;
        for (int i = 0; i < count; i++) {
            int shard = b.getInt();
            int reserved = b.getInt();
            long generation = b.getLong();
            long first = b.getLong();
            long through = b.getLong();
            if (shard <= previous || shard >= config.logicalShards() || reserved != 0
                    || generation < 1 || first < 0 || through < first - 1) {
                throw new IOException("Invalid index coverage stream");
            }
            previous = shard;
            streams.add(new SegmentDataRecovery.StreamCoverage(shard, new Generation(generation), first, through));
        }
        for (int i = b.position(); i < 4092; i++) {
            if (b.get(i) != 0) {
                throw new IOException("Unsupported index coverage extension");
            }
        }
        return new Published(new Coverage(b.getLong(16), new UUID(b.getLong(24), b.getLong(32)),
                b.getLong(72), b.getLong(80), b.getLong(88), streams), Arrays.copyOfRange(b.array(), 96, 128));
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void checksum(ByteBuffer b) {
        CRC32C crc = new CRC32C();
        crc.update(b.slice(0, b.capacity() - 4));
        b.putInt(b.capacity() - 4, (int) crc.getValue());
    }

    private static void verifyChecksum(ByteBuffer b) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(b.slice(0, b.capacity() - 4));
        if (b.getInt(b.capacity() - 4) != (int) crc.getValue()) {
            throw new IOException("Derived index checksum mismatch");
        }
    }

    private void requireOpen() throws IOException {
        if (closed || failed) {
            throw new IOException("Derived index is closed or maintenance failed");
        }
    }

    synchronized void requireStartupRecovered(IsolatedSegmentConfig expected, SegmentFileHeader storage)
            throws IOException {
        requireOpen();
        if (!config.sameStorageScope(expected) || !identity.equals(storage) || db == null) {
            throw new IOException("Original derived index scope or recovery is unavailable");
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            if (db != null) {
                db.close();
            }
            flush.close();
            writes.close();
            options.close();
            cache.close();
        }
    }
}

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
    private static final byte[] BINDING_KEY = {0};
    private static final byte[] LOCATION_BEGIN = {16};
    private static final byte[] LOCATION_END = {17};
    private static final long BINDING_MAGIC = 0x5557494442303031L; // UWIDB001
    private static final long KEY_MAGIC = 0x55574944584b3031L; // UWIDXK01
    private static final long LOCATION_MAGIC = 0x55574944584c3031L; // UWIDXL01
    private static final long COVERAGE_MAGIC = 0x555749434f563031L; // UWICOV01
    private static final int LOCATION_BYTES = 160;
    private static final int BATCH_ENTRIES = 256;

    record Coverage(long publication, UUID indexGeneration, long arenaCut, long bookieCut,
                    long entries, List<SegmentDataRecovery.StreamCoverage> streams) {
        Coverage {
            streams = List.copyOf(streams);
        }
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
        return new SegmentDerivedIndex(config, identity, maxEntries);
    }

    private SegmentDerivedIndex(IsolatedSegmentConfig config, SegmentFileHeader identity, int maxEntries)
            throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        if (maxEntries < 1) {
            throw new IllegalArgumentException("Positive derived rebuild capacity required");
        }
        this.maxEntries = maxEntries;
        scope = digest().digest(identity.encode(config, -3).array());
        RocksDB.loadLibrary();
        cache = new LRUCache(1024 * 1024);
        options = new Options().setCreateIfMissing(true).setUseFsync(true)
                .setWriteBufferSize(1024 * 1024).setMaxWriteBufferNumber(2)
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
            byte[] binding = db.get(BINDING_KEY);
            if (binding == null) {
                if (existing) {
                    throw new IOException("Incomplete derived index binding; a full rebuild is required");
                }
                indexGeneration = UUID.randomUUID();
                db.put(writes, BINDING_KEY, binding());
                db.flush(flush);
                IsolatedSegmentFiles.syncDirectory(config.directory());
            } else {
                readBinding(binding);
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
        return ByteBuffer.allocate(64).putLong(BINDING_MAGIC).putInt(1).putInt(LOCATION_BYTES)
                .putLong(indexGeneration.getMostSignificantBits()).putLong(indexGeneration.getLeastSignificantBits())
                .put(scope).array();
    }

    private void readBinding(byte[] bytes) throws IOException {
        if (bytes.length != 64) {
            throw new IOException("Invalid derived index binding length");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getLong() != BINDING_MAGIC || b.getInt() != 1 || b.getInt() != LOCATION_BYTES
                || !Arrays.equals(scope, Arrays.copyOfRange(bytes, 32, 64))) {
            throw new IOException("Derived index scope or format mismatch");
        }
        indexGeneration = new UUID(b.getLong(), b.getLong());
        if (indexGeneration.equals(new UUID(0, 0))) {
            throw new IOException("Invalid derived index generation");
        }
    }

    /** A fresh Get returns a physical locator only, never payload success or permission authority. */
    synchronized Optional<SegmentPointRead.Location> get(ProfileLedgerContext context, long entryId)
            throws IOException {
        requireOpen();
        try {
            byte[] bytes = db.get(key(context, entryId));
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
                byte[] old = db.get(key);
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
        requireOpen();
        Objects.requireNonNull(io, "io");
        if (pendingCoverage == null) {
            throw new IOException("No complete derived rebuild is available for coverage");
        }
        try {
            db.flush(flush);
            Contents contents = contents();
            if (contents.entries() != pendingCoverage.entries()) {
                throw new IOException("Derived index does not contain the complete selected inventory");
            }
            long generation = coverage().map(value -> value.publication() + 1).orElse(1L);
            if (generation < 1) {
                throw new IOException("Index coverage publication generation exhausted");
            }
            Coverage published = new Coverage(generation, indexGeneration, pendingCoverage.arenaCut(),
                    pendingCoverage.bookieCut(), contents.entries(), pendingCoverage.streams());
            Path target = publication(config, (int) ((generation - 1) % 2));
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
            failed = true;
            throw failure;
        }
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

    private Contents contents() throws IOException, RocksDBException {
        MessageDigest hash = digest();
        long count = 0;
        try (RocksIterator cursor = db.newIterator()) {
            for (cursor.seek(LOCATION_BEGIN); cursor.isValid() && cursor.key()[0] == LOCATION_BEGIN[0]; cursor.next()) {
                if (++count > maxEntries) {
                    throw new IOException("Derived contents exceed declared rebuild capacity");
                }
                byte[] key = cursor.key();
                byte[] value = cursor.value();
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
        return ByteBuffer.allocate(77).put(LOCATION_BEGIN[0]).putLong(KEY_MAGIC)
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
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
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

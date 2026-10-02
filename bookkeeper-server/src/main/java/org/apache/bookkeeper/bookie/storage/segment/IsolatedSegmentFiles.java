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
import java.util.List;
import java.util.Objects;

/** Disposable fixed-capacity files only. Physical batch durability is not local entry success. */
final class IsolatedSegmentFiles implements AutoCloseable {
    static final class StartupInventoryLease implements AutoCloseable {
        private final IsolatedSegmentFiles owner;
        private final SegmentDataRecovery.PhysicalInventory inventory;
        private boolean closed;

        private StartupInventoryLease(IsolatedSegmentFiles owner,
                                      SegmentDataRecovery.PhysicalInventory inventory) {
            this.owner = owner;
            this.inventory = inventory;
        }

        synchronized SegmentDataRecovery.PhysicalInventory inventory() {
            if (closed) {
                throw new IllegalStateException("Startup inventory lease is closed");
            }
            return inventory;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                owner.releaseStartupInventory();
            }
        }
    }

    static final class Reservation {
        private final IsolatedSegmentFiles owner;
        private final SegmentBatchFormat.FrozenBatch batch;
        private final int length;
        private boolean submitted;
        private volatile boolean durable;

        private Reservation(IsolatedSegmentFiles owner, SegmentBatchFormat.FrozenBatch batch) {
            this.owner = owner;
            this.batch = batch;
            this.length = batch.length();
        }

        SegmentBatchFormat.Address address() {
            return batch.address();
        }

        SegmentBatchFormat.FrozenBatch batch() {
            return batch;
        }

        int length() {
            return length;
        }

        boolean durable() {
            return durable;
        }
    }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final List<FileChannel> data = new ArrayList<>();
    private final Object[] dataIoGates;
    private final long[] nextOffset;
    private final boolean[] failed;
    private final SegmentDataFileLock writerLock;
    private final boolean appendOnlyRecovery;
    private final SegmentDataRecovery.PhysicalInventory startupInventory;
    private int pendingReservations;
    private int activeIo;
    private boolean closed;
    SegmentFileIo dataIo = new SegmentFileIo();

    private IsolatedSegmentFiles(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                 SegmentDataFileLock writerLock, boolean appendOnlyRecovery,
                                 SegmentDataRecovery.PhysicalInventory startupInventory) {
        this.config = config;
        this.identity = identity;
        this.writerLock = writerLock;
        this.appendOnlyRecovery = appendOnlyRecovery;
        this.startupInventory = startupInventory;
        nextOffset = new long[config.dataFiles()];
        dataIoGates = new Object[config.dataFiles()];
        java.util.Arrays.setAll(dataIoGates, ignored -> new Object());
        failed = new boolean[config.dataFiles()];
        java.util.Arrays.fill(nextOffset, 4096);
    }

    static IsolatedSegmentFiles create(IsolatedSegmentConfig config, SegmentFileHeader identity) throws IOException {
        return create(config, identity, new SegmentFileIo());
    }

    static IsolatedSegmentFiles create(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentFileIo io)
            throws IOException {
        // Never adopt, clean or truncate an existing directory, including symlinks to an existing scope.
        Files.createDirectory(config.directory());
        SegmentDataFileLock guard = SegmentDataFileLock.create(config.directory());
        IsolatedSegmentFiles result = null;
        try {
            result = new IsolatedSegmentFiles(config, identity, guard, false, null);
            for (int i = 0; i < config.dataFiles(); i++) {
                FileChannel channel = FileChannel.open(dataPath(config, i), StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
                result.data.add(channel);
                // Write the entire declared capacity, not a sparse seek-to-last-byte allocation.
                ByteBuffer zeros = ByteBuffer.allocate(64 * 1024);
                for (long offset = 0; offset < config.bytesPerFile();) {
                    zeros.clear().limit((int) Math.min(zeros.capacity(), config.bytesPerFile() - offset));
                    int n = zeros.remaining();
                    io.writeFully(channel, zeros, offset);
                    offset += n;
                }
                io.writeFully(channel, identity.encode(config, i), 0);
                io.force(channel, true);
            }
            writeIdentity(config.directory().resolve("superblock-a"), identity.encode(config, -1), io);
            writeIdentity(config.directory().resolve("superblock-b"), identity.encode(config, -2), io);
            writeIdentity(config.directory().resolve("bookie-control"), identity.encode(config, -3), io);
            writeIdentity(config.directory().resolve("arena-control"), identity.encode(config, -4), io);
            io.force(guard.channel(), true);
            syncDirectory(config.directory());
            syncDirectory(config.directory().getParent());
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (result != null) {
                    result.close();
                } else {
                    guard.close();
                }
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            // Leave incomplete initialization on disk for diagnosis; it is never silently reopened.
            throw failure;
        }
    }

    /** Isolated physical restart: abandon every previously granted block, including zero/unknown tails. */
    static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                               SegmentArenaControlLog authority, SegmentFileIo recoveryIo)
            throws IOException {
        return openAppendOnly(config, identity, authority, recoveryIo, 0, 0);
    }

    /** Also retain every ordinary physical coordinate from the same locked startup scan. */
    static IsolatedSegmentFiles openAppendOnlyWithInventory(IsolatedSegmentConfig config,
                                                            SegmentFileHeader identity,
                                                            SegmentArenaControlLog authority,
                                                            SegmentFileIo recoveryIo, int maxEntries,
                                                            int maxStreams)
            throws IOException {
        if (maxEntries < 1 || maxStreams < 1) {
            throw new IllegalArgumentException("Positive restart locator and stream capacities required");
        }
        return openAppendOnly(config, identity, authority, recoveryIo, maxEntries, maxStreams);
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentFileIo recoveryIo,
                                                       int maxEntries, int maxStreams) throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(recoveryIo, "recoveryIo");
        synchronized (authority) {
            authority.requireQuiescentRecovery(config, identity);
            SegmentDataFileLock guard = SegmentDataFileLock.open(config.directory());
            IsolatedSegmentFiles result = null;
            try {
                // Keep the same writer exclusion through validation, sync and the reopened writer lifetime.
                SegmentDataRecovery.PhysicalInventory inventory = null;
                if (maxEntries == 0) {
                    SegmentDataRecovery.validateAndSyncWhileLocked(config, identity, authority,
                            recoveryIo, guard.lock());
                } else {
                    inventory = SegmentDataRecovery.recoverPhysicalInventoryWhileLocked(config, identity,
                            authority, recoveryIo, maxEntries, maxStreams, guard.lock());
                }
                result = new IsolatedSegmentFiles(config, identity, guard, true, inventory);
                for (int file = 0; file < config.dataFiles(); file++) {
                    FileChannel data = FileChannel.open(dataPath(config, file), StandardOpenOption.READ,
                            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                    result.data.add(data);
                    if (data.size() != config.bytesPerFile()) {
                        throw new IOException("DATA file size changed during restart");
                    }
                    ByteBuffer header = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(data, header, 0);
                    identity.validate(header, config, file);
                    long next = authority.grantedHighWatermark(file);
                    if (next < 4096 || next > config.bytesPerFile() || next % config.alignment() != 0) {
                        throw new IOException("Invalid durable allocation high watermark");
                    }
                    result.nextOffset[file] = next;
                }
                return result;
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    if (result == null) {
                        guard.close();
                    } else {
                        result.close();
                    }
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    static Path dataPath(IsolatedSegmentConfig config, int index) {
        return config.directory().resolve("data-" + index);
    }

    void write(SegmentBatchFormat.FrozenBatch batch) throws IOException {
        writeReserved(reserve(batch));
    }

    synchronized Reservation reserve(SegmentBatchFormat.FrozenBatch batch) throws IOException {
        if (appendOnlyRecovery) {
            throw new IOException("Restarted physical writer requires an applied Arena grant");
        }
        return reservePhysical(batch);
    }

    private Reservation reservePhysical(SegmentBatchFormat.FrozenBatch batch) throws IOException {
        batch.requirePhysicalOwner();
        int file = batch.address().fileIndex();
        if (closed || file < 0 || file >= data.size() || failed[file]
                || batch.address().shard() >= config.logicalShards()
                || file != batch.address().shard() % config.dataFiles()
                || pendingReservations >= config.maxPendingEntries()) {
            throw new IOException("File is unavailable");
        }
        ByteBuffer bytes = batch.bytes();
        long offset = batch.address().offset();
        if (offset != nextOffset[file] || offset > config.bytesPerFile()
                || bytes.remaining() > config.bytesPerFile() - offset) {
            throw new IOException("Range is not unused or fixed capacity exhausted");
        }
        // Reserve before submission. Failure never returns the coordinates for reuse.
        nextOffset[file] += bytes.remaining();
        pendingReservations++;
        return new Reservation(this, batch);
    }

    void writeReserved(Reservation reservation) throws IOException {
        int file;
        synchronized (this) {
            if (reservation.owner != this || reservation.submitted) {
                throw new IllegalArgumentException("Reservation belongs to another writer or was already submitted");
            }
            reservation.submitted = true;
            file = reservation.address().fileIndex();
            if (closed || failed[file]) {
                pendingReservations--;
                throw new IOException("Reserved DATA file is unavailable");
            }
            activeIo++;
        }
        try {
            synchronized (dataIoGates[file]) {
                try {
                    synchronized (this) {
                        if (failed[file]) {
                            throw new IOException("Reserved DATA file is unavailable");
                        }
                    }
                    dataIo.writeFully(data.get(file), reservation.batch.bytes(), reservation.address().offset());
                    dataIo.force(data.get(file), false);
                    reservation.durable = true;
                } catch (IOException | RuntimeException | Error failure) {
                    synchronized (this) {
                        failed[file] = true;
                    }
                    throw failure;
                }
            }
        } finally {
            synchronized (this) {
                activeIo--;
                pendingReservations--;
            }
        }
    }

    /** Retire an operation that never reached DATA I/O; its coordinates remain consumed. */
    synchronized void cancelBeforeSubmission(Reservation reservation) {
        if (reservation.owner != this || reservation.submitted) {
            throw new IllegalArgumentException("Reservation was already submitted or belongs to another writer");
        }
        reservation.submitted = true;
        pendingReservations--;
    }

    /** Physical experiment path: require an applied, durable Arena grant for the entire frozen DATA batch. */
    void writeAuthorized(SegmentBatchFormat.FrozenBatch batch, SegmentArenaControlLog authority)
            throws IOException {
        writeReserved(reserveAuthorized(batch, authority));
    }

    synchronized Reservation reserveAuthorized(SegmentBatchFormat.FrozenBatch batch,
                                               SegmentArenaControlLog authority) throws IOException {
        SegmentBatchFormat.Address address = batch.address();
        if (!authority.matchesScope(config, identity)
                || !authority.hasAppliedGrant(address.fileIndex(), address.offset(), batch.bytes().remaining(),
                address.shard(), address.streamGeneration().value(), address.allocation(),
                address.generation().value())) {
            throw new IOException("DATA batch has no applied Arena allocation grant");
        }
        return reservePhysical(batch);
    }

    synchronized long nextOffset(int file) {
        return nextOffset[file];
    }

    IsolatedSegmentConfig config() {
        return config;
    }

    SegmentFileHeader identity() {
        return identity;
    }

    boolean supportsLocalResults() {
        return !appendOnlyRecovery;
    }

    synchronized SegmentDataRecovery.PhysicalInventory startupInventory() {
        if (closed || startupInventory == null) {
            throw new IllegalStateException("No retained DATA startup inventory");
        }
        return startupInventory;
    }

    synchronized StartupInventoryLease pinStartupInventory() throws IOException {
        if (closed || startupInventory == null || !writerLock.lock().isValid()
                || activeIo >= config.maxPendingEntries()) {
            throw new IOException("Startup DATA inventory is unavailable or read capacity exhausted");
        }
        activeIo++;
        return new StartupInventoryLease(this, startupInventory);
    }

    private synchronized void releaseStartupInventory() {
        activeIo--;
    }

    synchronized boolean matchesScope(IsolatedSegmentConfig otherConfig, SegmentFileHeader otherIdentity) {
        return !closed && config.sameStorageScope(otherConfig) && identity.equals(otherIdentity);
    }

    private static void writeIdentity(Path path, ByteBuffer bytes, SegmentFileIo io) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            io.writeFully(channel, bytes, 0);
            io.force(channel, true);
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (activeIo != 0 || pendingReservations != 0) {
            throw new IOException("DATA reservation, I/O or startup read is still in flight");
        }
        closed = true;
        IOException first = null;
        for (FileChannel channel : data) {
            try {
                channel.close();
            } catch (IOException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
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

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;

/** Disposable fixed-capacity files only. Physical batch durability is not local entry success. */
final class IsolatedSegmentFiles implements AutoCloseable {
    private record Stream(int shard, long generation) { }

    private record AppendCursor(long allocationOrder, long offset) { }

    /** A conservative whole-file-set gate: one proof, no new reservations or read leases until owner resolution. */
    static final class ReclamationGuard implements AutoCloseable {
        private final IsolatedSegmentFiles owner;
        private final SegmentArenaControlLog authority;
        private final int file;
        private final SegmentBookieControlLog.ReclamationLease bookieLease;
        private boolean claimed;
        private boolean finished;
        private int members = 1;
        private int claims;
        private int completions;

        private ReclamationGuard(IsolatedSegmentFiles owner, SegmentArenaControlLog authority, int file,
                                  SegmentBookieControlLog.ReclamationLease bookieLease) {
            this.owner = owner;
            this.authority = authority;
            this.file = file;
            this.bookieLease = bookieLease;
        }

        void claim(SegmentArenaControlLog log) throws IOException {
            synchronized (owner) {
                if (log != authority || finished || claims == members || owner.reclamation != this) {
                    throw new IOException("FREE protection is unavailable or already submitted");
                }
                claimed = true;
                claims++;
            }
        }

        void expectClaims(int count) throws IOException {
            synchronized (owner) {
                requireHeld(owner, authority);
                if (count < 1 || count > 16) {
                    throw new IOException("Protected FREE batch exceeds its bounded member set");
                }
                members = count;
            }
        }

        boolean claimedBy(SegmentArenaControlLog log) {
            synchronized (owner) {
                return log == authority && claimed && !finished && owner.reclamation == this;
            }
        }

        void requireHeld(IsolatedSegmentFiles files, SegmentArenaControlLog log) throws IOException {
            synchronized (owner) {
                if (owner != files || authority != log || finished || claimed || owner.reclamation != this) {
                    throw new IOException("Index maintenance has no held DATA and Bookie protection");
                }
            }
        }

        void finish(boolean uncertain) {
            finish(uncertain, true);
        }

        void finish(boolean uncertain, boolean memberClaimed) {
            boolean release;
            synchronized (owner) {
                if (finished) {
                    return;
                }
                owner.failed[file] |= uncertain;
                release = (claims == 0 || memberClaimed && ++completions == claims) && markFinished(false);
            }
            if (release) {
                bookieLease.close();
            }
        }

        private boolean markFinished(boolean uncertain) {
            if (finished) {
                return false;
            }
            owner.failed[file] |= uncertain;
            finished = true;
            owner.reclamation = null;
            return true;
        }

        SegmentArenaControlLog.FreeFacts verify(SegmentArenaControlLog.Allocation allocation,
                                                long allocationOrder, SegmentBookieControlLog bookie,
                                                ByteBuffer workspace)
                throws IOException {
            if (allocation.file() != file) {
                throw new IOException("Protected allocation proof belongs to another DATA file");
            }
            long cut = bookieLease.cut();
            SegmentArenaControlLog.FreeFacts proof = SegmentDataRecovery.proveDeadAllocation(
                    owner.config, owner.identity, authority, owner.data.get(file), allocation,
                    allocationOrder, bookie, cut, workspace,
                    owner.allocationEnd(allocation, authority));
            if (bookie.recoveryReadCut(owner.config, owner.identity) != cut) {
                throw new IOException("Bookie authority changed during whole-allocation verification");
            }
            return proof;
        }

        boolean containsAllocatedInstance(SegmentArenaControlLog.Allocation allocation,
                SegmentBookieControlLog bookie, ProfileLedgerContext context, ByteBuffer workspace) throws IOException {
            requireHeld(owner, authority);
            if (allocation.file() != file || !bookie.hasTerminalTombstone(context)) {
                throw new IOException("Deleted-instance inspection needs its exact terminal context and DATA file");
            }
            boolean found = SegmentDataRecovery.containsAllocatedInstance(owner.config, authority,
                    owner.data.get(file), allocation, owner.allocationEnd(allocation, authority), workspace, context);
            if (bookie.recoveryReadCut(owner.config, owner.identity) != bookieLease.cut()) {
                throw new IOException("Deleted-instance authority changed during its physical inspection");
            }
            return found;
        }

        SegmentDataRecovery.CompactionPlan inspectCompaction(List<SegmentArenaControlLog.Allocation> victims,
                                                              SegmentBookieControlLog bookie,
                                                              int maximumEntries, ByteBuffer workspace,
                                                              boolean selectVictims)
                throws IOException {
            requireHeld(owner, authority);
            List<Long> ends = new ArrayList<>();
            for (SegmentArenaControlLog.Allocation victim : victims) {
                if (victim.file() != file) {
                    throw new IOException("Compaction victim belongs to another DATA file");
                }
                ends.add(owner.allocationEnd(victim, authority));
            }
            return SegmentDataRecovery.inspectCompaction(owner.config, owner.identity, authority,
                    owner.data.get(file), victims, ends, bookie, maximumEntries, workspace, owner, this, selectVictims);
        }

        SegmentBatchFormat.FrozenBatch captureCompaction(SegmentDataRecovery.CompactionPlan plan, int batchIndex,
                SegmentBookieControlLog bookie, SegmentBatchFormat.RelocationBuilder builder, ByteBuffer workspace)
                throws IOException {
            requireHeld(owner, authority);
            if (plan.victims().get(0).file() != file) {
                throw new IOException("Compaction capture belongs to another DATA file");
            }
            return SegmentDataRecovery.captureCompaction(owner.config, owner.identity, authority, bookie, owner, this,
                    plan, batchIndex, builder, workspace);
        }

        SegmentDataRecovery.CompactionPlan inspectCompactionTail(List<SegmentArenaControlLog.Allocation> victims,
                SegmentArenaControlLog.Allocation destination, long nextSequence, SegmentBookieControlLog bookie,
                int maximumEntries, ByteBuffer workspace) throws IOException {
            requireHeld(owner, authority);
            if (destination.file() != file || victims.stream().anyMatch(victim -> victim.file() != file
                    || victim.ownerShard() != destination.ownerShard()
                    || !victim.ownerGeneration().equals(destination.ownerGeneration()))) {
                throw new IOException("Retained compaction tail belongs to another DATA stream");
            }
            List<Long> ends = new ArrayList<>();
            for (SegmentArenaControlLog.Allocation victim : victims) {
                ends.add(owner.allocationEnd(victim, authority));
            }
            SegmentDataRecovery.CompactionPlan plan = SegmentDataRecovery.inspectCompactionTail(owner.config,
                    owner.identity, authority, owner.data.get(file), victims, ends, destination,
                    owner.allocationEnd(destination, authority), nextSequence, bookie, maximumEntries,
                    workspace, owner, this);
            if (plan != null && !plan.batches().isEmpty()) {
                synchronized (owner) {
                    SegmentDataRecovery.CompactionTail tail = plan.tail();
                    SegmentBatchFormat.Address address = new SegmentBatchFormat.Address(file, destination.ownerShard(),
                            destination.allocationId(), new Generation(destination.generation()),
                            destination.ownerGeneration(), nextSequence, tail.offset());
                    long order = authority.appliedAllocationOrder(address);
                    Stream stream = new Stream(address.shard(), address.streamGeneration().value());
                    AppendCursor cursor = owner.streamOffsets.get(stream);
                    if (owner.sessionAuthority != authority || !owner.sessionReady
                            || !authority.permitsAllocationAppend(order)
                            || cursor != null && (cursor.allocationOrder() > order
                                    || cursor.allocationOrder() == order && cursor.offset() != tail.offset())) {
                        throw new IOException("Retained compaction tail differs from the current managed writer");
                    }
                    // Only this protected latest-grant proof may reopen a healthy restored tail.
                    owner.streamOffsets.put(stream, new AppendCursor(order, tail.offset()));
                    owner.allocationEnds.merge(destination.allocationId(), tail.offset(), Math::max);
                }
            }
            return plan;
        }

        @Override
        public void close() {
            boolean release;
            synchronized (owner) {
                if (claimed && !finished) {
                    throw new IllegalStateException("FREE control I/O or original-owner resolution is still pending");
                }
                release = markFinished(false);
            }
            if (release) {
                bookieLease.close();
            }
        }
    }

    static class DataReadLease implements AutoCloseable {
        private final IsolatedSegmentFiles owner;
        private boolean closed;

        private DataReadLease(IsolatedSegmentFiles owner) {
            this.owner = owner;
        }

        synchronized void requireOpen() {
            if (closed) {
                throw new IllegalStateException("DATA read lease is closed");
            }
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                owner.releaseDataRead();
            }
        }
    }

    static final class StartupInventoryLease extends DataReadLease {
        private final SegmentDataRecovery.PhysicalInventory inventory;

        private StartupInventoryLease(IsolatedSegmentFiles owner,
                                      SegmentDataRecovery.PhysicalInventory inventory) {
            super(owner);
            this.inventory = inventory;
        }

        synchronized SegmentDataRecovery.PhysicalInventory inventory() {
            requireOpen();
            return inventory;
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
    // Once an authorized stream starts, raw appends cannot consume another stream's pool.
    private final long[] authorizedFloor;
    // One cursor per applied stream, bounded by the fixed-capacity control log, never per DATA batch.
    private final Map<Stream, AppendCursor> streamOffsets = new HashMap<>();
    // One reservation boundary per allocation, bounded by existing DATA/control capacity.
    private final Map<Long, Long> allocationEnds = new HashMap<>();
    private long restoredAllocationThrough = -1;
    private final boolean[] failed;
    private final SegmentDataFileLock writerLock;
    private final SegmentOwnedFiles ownedFiles;
    private final boolean appendOnlyRecovery;
    private SegmentDataRecovery.PhysicalInventory startupInventory;
    private int pendingReservations;
    private int activeIo;
    private ReclamationGuard reclamation;
    private boolean closed;
    private SegmentArenaControlLog sessionAuthority;
    private boolean sessionReady;
    private SegmentBookieControlLog restartBookie;
    private long restartBookieCut;
    private final boolean[] restoredShards;
    SegmentFileIo dataIo = new SegmentFileIo();

    private IsolatedSegmentFiles(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                 SegmentDataFileLock writerLock, boolean appendOnlyRecovery,
                                 SegmentDataRecovery.PhysicalInventory startupInventory, SegmentOwnedFiles ownedFiles) {
        this.config = config;
        this.identity = identity;
        this.writerLock = writerLock;
        this.ownedFiles = ownedFiles;
        this.appendOnlyRecovery = appendOnlyRecovery;
        this.startupInventory = startupInventory;
        nextOffset = new long[config.dataFiles()];
        authorizedFloor = new long[config.dataFiles()];
        dataIoGates = new Object[config.dataFiles()];
        java.util.Arrays.setAll(dataIoGates, ignored -> new Object());
        failed = new boolean[config.dataFiles()];
        restoredShards = new boolean[config.logicalShards()];
        java.util.Arrays.fill(nextOffset, 4096);
    }

    static IsolatedSegmentFiles create(IsolatedSegmentConfig config, SegmentFileHeader identity) throws IOException {
        return create(config, identity, new SegmentFileIo());
    }

    static IsolatedSegmentFiles create(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentFileIo io)
            throws IOException {
        identity.requireStoredManifest(config, -4);
        // Never adopt, clean or truncate an existing directory, including symlinks to an existing scope.
        Files.createDirectory(config.directory());
        SegmentDataFileLock guard = SegmentDataFileLock.create(config.directory());
        return createWhileLocked(config, identity, io, guard, null);
    }

    static IsolatedSegmentFiles createWhileLocked(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentFileIo io, SegmentDataFileLock guard, SegmentOwnedFiles ownedFiles) throws IOException {
        IsolatedSegmentFiles result = null;
        try {
            if (ownedFiles != null) {
                guard.requireHeld(config.directory().resolve("isolated.lock"));
                identity.requireStoredManifest(config, -4, ownedFiles);
            }
            result = new IsolatedSegmentFiles(config, identity, guard, false, null, ownedFiles);
            for (int i = 0; i < config.dataFiles(); i++) {
                FileChannel channel = SegmentOwnedFiles.open(ownedFiles, dataPath(config, i),
                        StandardOpenOption.CREATE_NEW,
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
            writeIdentity(config.directory().resolve("superblock-a"), identity.encode(config, -1), io, ownedFiles);
            writeIdentity(config.directory().resolve("superblock-b"), identity.encode(config, -2), io, ownedFiles);
            if (identity.manifest() == null) {
                writeIdentity(config.directory().resolve("bookie-control"), identity.encode(config, -3), io,
                        ownedFiles);
            }
            writeIdentity(config.directory().resolve("arena-control"), identity.encode(config, -4), io, ownedFiles);
            io.force(guard.channel(), true);
            syncDirectory(config.directory(), ownedFiles);
            syncDirectory(config.directory().getParent(), ownedFiles);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (result != null) {
                    result.close();
                } else if (ownedFiles == null) {
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
        return openAppendOnly(config, identity, authority, null, recoveryIo, 0, 0);
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
        return openAppendOnly(config, identity, authority, null, recoveryIo, maxEntries, maxStreams);
    }

    /** A bounded no-MOVE/FREE startup read selection from replayed Bookie routes and all physical streams. */
    static IsolatedSegmentFiles openAppendOnlyWithLocallyReadableInventory(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo recoveryIo, int maxEntries, int maxStreams)
            throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        if (maxEntries < 1 || maxStreams < 1) {
            throw new IllegalArgumentException("Positive restart locator and stream capacities required");
        }
        return openAppendOnly(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams);
    }

    /** Clean-session, no-MOVE/FREE restart; owner bootstrap still precedes accepting any local-result request. */
    static IsolatedSegmentFiles openForLocalResults(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                    SegmentFileIo recoveryIo, int maxEntries, int maxStreams)
            throws IOException {
        return openForLocalResults(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams, null);
    }

    static IsolatedSegmentFiles openForLocalResults(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                    SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                    SegmentDerivedIndex index) throws IOException {
        return openForLocalResults(config, identity, authority, bookie,
                recoveryIo, maxEntries, maxStreams, index, null);
    }

    /** Reuse the graph's already-held startup exclusion; failures also close the supplied lock. */
    static IsolatedSegmentFiles openForLocalResults(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                    SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                    SegmentDerivedIndex index, SegmentDataFileLock startupLock)
            throws IOException {
        return openForLocalResults(config, identity, authority, bookie,
                recoveryIo, maxEntries, maxStreams, index, startupLock, null);
    }

    static IsolatedSegmentFiles openForLocalResults(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                    SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                    SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                    SegmentDerivedIndex index, SegmentDataFileLock startupLock,
                                                    ProfileReceiveBudget recoveryBudget) throws IOException {
        try {
            return openLocalResultsWhileExcluded(config, identity, authority, bookie,
                    recoveryIo, maxEntries, maxStreams, index, startupLock, recoveryBudget, null);
        } catch (IOException | RuntimeException | Error failure) {
            if (startupLock != null) {
                try {
                    startupLock.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    static IsolatedSegmentFiles prepareLocalResults(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentArenaControlLog authority, SegmentBookieControlLog bookie, SegmentFileIo recoveryIo,
            int maxEntries, int maxStreams, SegmentDerivedIndex index, SegmentDataFileLock guard,
            SegmentOwnedFiles files) throws IOException {
        guard.requireHeld(config.directory().resolve("isolated.lock"));
        return openLocalResultsWhileExcluded(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams,
                index, guard, null, Objects.requireNonNull(files));
    }

    private static IsolatedSegmentFiles openLocalResultsWhileExcluded(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog authority,
            SegmentBookieControlLog bookie, SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
            SegmentDerivedIndex index, SegmentDataFileLock startupLock,
            ProfileReceiveBudget recoveryBudget, SegmentOwnedFiles ownedFiles) throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        if (maxEntries < 1 || maxStreams < 1) {
            throw new IllegalArgumentException("Positive restart locator and stream capacities required");
        }
        synchronized (authority) {
            authority.requireQuiescentRecovery(config, identity);
            if (authority.dataSessionEpoch() == 0) {
                throw new IOException("Local-result restart requires a closed managed DATA session");
            }
            for (int shard = 0; shard < config.logicalShards(); shard++) {
                if (identity.ownsShard(shard)) {
                    authority.restartStreamForShard(shard);
                }
            }
            long cut = bookie.recoveryReadCut(config, identity);
            IsolatedSegmentFiles result = openAppendOnly(config, identity, authority, bookie,
                    recoveryIo, maxEntries, maxStreams, index, startupLock, recoveryBudget, ownedFiles);
            try {
                if (index != null) {
                    if (result.startupInventory.usesIndexCoverage()) {
                        index.updateFromRecovery(result.startupInventory, authority, bookie);
                    } else {
                        index.rebuild(result.startupInventory, authority, bookie);
                    }
                }
                if (ownedFiles == null) {
                    result.startSession(authority);
                }
                if (cut != bookie.recoveryReadCut(config, identity)) {
                    throw new IOException("Bookie control cut changed during local-result restart");
                }
                result.restartBookie = bookie;
                result.restartBookieCut = cut;
                return result;
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    result.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                       SegmentFileIo recoveryIo,
                                                       int maxEntries, int maxStreams) throws IOException {
        return openAppendOnly(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams, null);
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                       SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                       SegmentDerivedIndex index) throws IOException {
        return openAppendOnly(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams, index, null);
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                       SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                       SegmentDerivedIndex index, SegmentDataFileLock startupLock)
            throws IOException {
        return openAppendOnly(config, identity, authority, bookie,
                recoveryIo, maxEntries, maxStreams, index, startupLock, null);
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                                       SegmentArenaControlLog authority, SegmentBookieControlLog bookie,
                                                       SegmentFileIo recoveryIo, int maxEntries, int maxStreams,
                                                       SegmentDerivedIndex index, SegmentDataFileLock startupLock,
                                                       ProfileReceiveBudget recoveryBudget) throws IOException {
        return openAppendOnly(config, identity, authority, bookie, recoveryIo, maxEntries, maxStreams, index,
                startupLock, recoveryBudget, null);
    }

    private static IsolatedSegmentFiles openAppendOnly(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentArenaControlLog authority, SegmentBookieControlLog bookie, SegmentFileIo recoveryIo,
            int maxEntries, int maxStreams, SegmentDerivedIndex index, SegmentDataFileLock startupLock,
            ProfileReceiveBudget recoveryBudget, SegmentOwnedFiles ownedFiles) throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(recoveryIo, "recoveryIo");
        synchronized (authority) {
            authority.requireQuiescentRecovery(config, identity);
            SegmentDataFileLock guard = startupLock == null
                    ? SegmentDataFileLock.open(config.directory()) : startupLock;
            IsolatedSegmentFiles result = null;
            try {
                // Keep the same writer exclusion through validation, sync and the reopened writer lifetime.
                SegmentDataRecovery.PhysicalInventory inventory = null;
                if (maxEntries == 0) {
                    SegmentDataRecovery.validateAndSyncWhileLocked(config, identity, authority,
                            recoveryIo, guard.lock());
                } else if (bookie == null) {
                    inventory = SegmentDataRecovery.recoverPhysicalInventoryWhileLocked(config, identity,
                            authority, recoveryIo, maxEntries, maxStreams, guard.lock());
                } else {
                    inventory = SegmentDataRecovery.recoverIndexedLocallyReadableInventoryWhileLocked(config,
                            identity, authority, bookie, recoveryIo, maxEntries, maxStreams,
                            guard.lock(), index, recoveryBudget);
                }
                result = new IsolatedSegmentFiles(config, identity, guard, true, inventory, ownedFiles);
                for (int file = 0; file < config.dataFiles(); file++) {
                    FileChannel data = SegmentOwnedFiles.open(ownedFiles, dataPath(config, file),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                    result.data.add(data);
                    if (data.size() != config.bytesPerFile()) {
                        throw new IOException("DATA file size changed during restart");
                    }
                    try (var headerCredit = recoveryBudget == null ? null
                            : recoveryBudget.reserveDataWorkingSet(4096)) {
                        ByteBuffer header = ByteBuffer.allocate(4096);
                        SegmentFileIo.readFully(data, header, 0);
                        identity.validate(header, config, file);
                    }
                    long next = authority.grantedHighWatermark(file);
                    if (next < 4096 || next > config.bytesPerFile() || next % config.alignment() != 0) {
                        throw new IOException("Invalid durable allocation high watermark");
                    }
                    result.nextOffset[file] = next;
                }
                result.restoredAllocationThrough = authority.appliedThrough();
                java.util.Arrays.fill(result.authorizedFloor, 4096);
                return result;
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    if (result == null && ownedFiles == null) {
                        guard.close();
                    } else if (result != null) {
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
        if (appendOnlyRecovery || sessionAuthority != null) {
            throw new IOException("Restarted physical writer requires an applied Arena grant");
        }
        return reservePhysical(batch, false, -1);
    }

    private Reservation reservePhysical(SegmentBatchFormat.FrozenBatch batch, boolean authorized,
                                        long allocationOrder) throws IOException {
        batch.requirePhysicalOwner();
        int file = batch.address().fileIndex();
        if (closed || reclamation != null || sessionAuthority != null && !sessionReady
                || file < 0 || file >= data.size() || failed[file]
                || batch.address().shard() >= config.logicalShards() || !identity.ownsShard(batch.address().shard())
                || file != batch.address().shard() % config.dataFiles()
                || pendingReservations >= config.maxPendingEntries()) {
            throw new IOException("File is unavailable");
        }
        ByteBuffer bytes = batch.bytes();
        long offset = batch.address().offset();
        Stream stream = new Stream(batch.address().shard(), batch.address().streamGeneration().value());
        long floor = authorizedFloor[file] == 0 ? nextOffset[file] : authorizedFloor[file];
        AppendCursor cursor = streamOffsets.get(stream);
        long minimumOrder = cursor == null ? restoredAllocationThrough + 1 : cursor.allocationOrder();
        long minimum = cursor != null && allocationOrder == cursor.allocationOrder() ? cursor.offset() : floor;
        if ((!authorized && (authorizedFloor[file] != 0 || offset != nextOffset[file]))
                || (authorized && (allocationOrder < minimumOrder || offset < minimum))
                || offset > config.bytesPerFile()
                || bytes.remaining() > config.bytesPerFile() - offset) {
            throw new IOException("Range is not unused or fixed capacity exhausted");
        }
        // Reserve before submission. Failure never returns the coordinates for reuse.
        long end = offset + bytes.remaining();
        if (authorized) {
            authorizedFloor[file] = floor;
            streamOffsets.put(stream, new AppendCursor(allocationOrder, end));
            allocationEnds.merge(batch.address().allocation(), end, Math::max);
        }
        nextOffset[file] = Math.max(nextOffset[file], end);
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
                        if (sessionAuthority != null) {
                            sessionAuthority.blockDataFile(file);
                        }
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
        if (sessionAuthority != null) {
            failed[reservation.address().fileIndex()] = true;
            sessionAuthority.blockDataFile(reservation.address().fileIndex());
        }
    }

    /** Physical experiment path: require an applied, durable Arena grant for the entire frozen DATA batch. */
    void writeAuthorized(SegmentBatchFormat.FrozenBatch batch, SegmentArenaControlLog authority)
            throws IOException {
        writeReserved(reserveAuthorized(batch, authority));
    }

    synchronized Reservation reserveAuthorized(SegmentBatchFormat.FrozenBatch batch,
                                               SegmentArenaControlLog authority) throws IOException {
        if (reclamation != null) {
            throw new IOException("DATA admission is closed by whole-allocation verification");
        }
        SegmentBatchFormat.Address address = batch.address();
        long order = authority.appliedAllocationOrder(address);
        if (!authority.matchesScope(config, identity) || !authority.permitsDataWriter(this)
                || !authority.dataFileAvailable(address.fileIndex())
                || !authority.permitsAllocationAppend(order)
                || !authority.hasAppliedGrant(address.fileIndex(), address.offset(), batch.bytes().remaining(),
                address.shard(), address.streamGeneration().value(), address.allocation(),
                address.generation().value())) {
            throw new IOException("DATA batch has no applied Arena allocation grant");
        }
        return reservePhysical(batch, true, order);
    }

    /** Select a whole batch in one applied pool; skips remain consumed when the reservation advances the stream. */
    synchronized SegmentBatchFormat.Address appendAddress(SegmentArenaControlLog authority, int shard,
                                                          Generation stream, long sequence, int bytes)
            throws IOException {
        SegmentBatchFormat.Address address = appliedAppendAddress(authority, shard, stream, sequence, bytes);
        return pendingReservations >= config.maxPendingEntries() ? null : address;
    }

    synchronized boolean poolMissing(SegmentArenaControlLog authority, int shard,
                                     Generation stream, long sequence, int bytes) throws IOException {
        return appliedAppendAddress(authority, shard, stream, sequence, bytes) == null;
    }

    private SegmentBatchFormat.Address appliedAppendAddress(SegmentArenaControlLog authority, int shard,
                                                            Generation stream, long sequence, int bytes)
            throws IOException {
        int file = shard % config.dataFiles();
        if (reclamation != null) {
            throw new IOException("DATA admission is closed by whole-allocation verification");
        }
        if (closed || failed[file] || !authority.matchesScope(config, identity)) {
            throw new IOException("DATA file or Arena authority is unavailable");
        }
        long floor = authorizedFloor[file] == 0 ? nextOffset[file] : authorizedFloor[file];
        AppendCursor cursor = streamOffsets.get(new Stream(shard, stream.value()));
        long minimumOrder = cursor == null ? restoredAllocationThrough + 1 : cursor.allocationOrder();
        long minimumOffset = cursor == null ? floor : cursor.offset();
        return authority.appendAddress(file, minimumOrder, minimumOffset, floor, bytes,
                shard, stream, sequence).orElse(null);
    }

    synchronized long nextOffset(int file) {
        return nextOffset[file];
    }

    private synchronized long allocationEnd(SegmentArenaControlLog.Allocation allocation,
                                            SegmentArenaControlLog authority) throws IOException {
        if (allocation.expectedGeneration() == 0) {
            return allocation.offset() + allocation.blocks() * SegmentArenaControlLog.BLOCK_BYTES;
        }
        Long end = allocationEnds.get(allocation.allocationId());
        if (end != null) {
            return end;
        }
        SegmentBatchFormat.Address start = new SegmentBatchFormat.Address(allocation.file(), allocation.ownerShard(),
                allocation.allocationId(), new Generation(allocation.generation()), allocation.ownerGeneration(),
                allocation.streamFirstSequence(), allocation.offset());
        return authority.appliedAllocationOrder(start) <= restoredAllocationThrough
                ? authority.sealedAllocationEnd(allocation.allocationId()) : allocation.offset();
    }

    synchronized long closedAllocationEnd(SegmentArenaControlLog.Allocation allocation,
                                           SegmentArenaControlLog authority) throws IOException {
        requireClosedSession(authority);
        return allocationEnd(allocation, authority);
    }

    synchronized void requireClosedSession(SegmentArenaControlLog authority) throws IOException {
        if (!closed || activeIo != 0 || pendingReservations != 0 || reclamation != null
                || authority != sessionAuthority || !writerLock.lock().isValid()) {
            throw new IOException("CLOSED tails require actual writer shutdown and I/O termination");
        }
        for (boolean fileFailed : failed) {
            if (fileFailed) {
                throw new IOException("Failed writer cannot attest reused allocation tails");
            }
        }
    }

    IsolatedSegmentConfig config() {
        return config;
    }

    SegmentFileHeader identity() {
        return identity;
    }

    boolean supportsLocalResults() {
        return !appendOnlyRecovery || restartBookie != null && sessionReady;
    }

    SegmentBookieControlLog restartBookie() {
        return restartBookie;
    }

    synchronized SegmentDataRecovery.PhysicalInventory localResultInventory(
            SegmentArenaControlLog authority, int shard) throws IOException {
        if (closed || reclamation != null || !sessionReady || restartBookie == null || authority != sessionAuthority
                || !authority.permitsDataWriter(this) || shard < 0 || shard >= restoredShards.length
                || !identity.ownsShard(shard) || restoredShards[shard]
                || restartBookieCut != restartBookie.recoveryReadCut(config, identity)) {
            throw new IOException("Clean restart owner bootstrap is unavailable or already consumed");
        }
        for (int file = 0; file < config.dataFiles(); file++) {
            if (!authority.dataFileAvailable(file)) {
                throw new IOException("Clean restart DATA file became unavailable");
            }
        }
        return startupInventory;
    }

    synchronized void restoredShard(int shard) {
        restoredShards[shard] = true;
    }

    /** Optional isolated lifecycle, required before this writer's first allocation/DATA submission. */
    synchronized void startSession(SegmentArenaControlLog authority) throws IOException {
        Objects.requireNonNull(authority, "authority");
        if (closed || reclamation != null || sessionAuthority != null || activeIo != 0 || pendingReservations != 0
                || !appendOnlyRecovery && java.util.Arrays.stream(nextOffset).anyMatch(offset -> offset != 4096)) {
            throw new IOException("DATA writer cannot begin another session");
        }
        sessionAuthority = authority;
        authority.beginDataSession(this);
        sessionReady = true;
    }

    /** Startup maintenance must retain the same DATA exclusion from validation through durable OPEN. */
    void requireStartingSession(SegmentArenaControlLog authority) throws IOException {
        if (!Thread.holdsLock(this) || closed || reclamation != null || activeIo != 0 || pendingReservations != 0
                || sessionAuthority != authority || sessionReady || !writerLock.lock().isValid()) {
            throw new IOException("DATA session startup requires the actual protected writer");
        }
        for (boolean fileFailed : failed) {
            if (fileFailed) {
                throw new IOException("Failed DATA writer cannot start another session");
            }
        }
    }

    synchronized SegmentDataRecovery.PhysicalInventory startupInventory() {
        if (closed || startupInventory == null) {
            throw new IllegalStateException("No retained DATA startup inventory");
        }
        return startupInventory;
    }

    /** Successful graph close retires shared recovered contexts after all readers and I/O have ended. */
    synchronized void retireClosedInventory() {
        if (!closed || activeIo != 0 || pendingReservations != 0 || reclamation != null) {
            throw new IllegalStateException("DATA startup inventory still belongs to live files");
        }
        startupInventory = null;
    }

    synchronized StartupInventoryLease pinStartupInventory() throws IOException {
        if (startupInventory == null) {
            throw new IOException("Startup DATA inventory is unavailable");
        }
        reserveDataRead();
        return new StartupInventoryLease(this, startupInventory);
    }

    synchronized DataReadLease pinDataRead() throws IOException {
        reserveDataRead();
        return new DataReadLease(this);
    }

    private void reserveDataRead() throws IOException {
        if (closed || reclamation != null || !writerLock.lock().isValid() || activeIo >= config.maxPendingEntries()) {
            throw new IOException("DATA files are unavailable or read capacity exhausted");
        }
        activeIo++;
    }

    synchronized ReclamationGuard protectReclamation(SegmentArenaControlLog authority, int file,
                                                      SegmentBookieControlLog.ReclamationLease bookieLease)
            throws IOException {
        if (closed || reclamation != null || activeIo != 0 || pendingReservations != 0
                || !writerLock.lock().isValid() || file < 0 || file >= data.size() || failed[file]
                || sessionAuthority != null && (!sessionReady || sessionAuthority != authority)) {
            throw new IOException("Whole-allocation verification has unresolved DATA I/O, reservations or read pins");
        }
        reclamation = new ReclamationGuard(this, authority, file, bookieLease);
        return reclamation;
    }

    /** Diagnostic only; the actual guard is acquired again in CONTROL I/O before rotation. */
    synchronized boolean quiescentForControlRotation(SegmentArenaControlLog authority) {
        return !closed && reclamation == null && activeIo == 0 && pendingReservations == 0
                && writerLock.lock().isValid() && !failed[0] && sessionReady && sessionAuthority == authority;
    }

    private synchronized void releaseDataRead() {
        activeIo--;
    }

    synchronized boolean matchesScope(IsolatedSegmentConfig otherConfig, SegmentFileHeader otherIdentity) {
        return !closed && config.sameStorageScope(otherConfig) && identity.equals(otherIdentity);
    }

    static void writeIdentity(Path path, ByteBuffer bytes, SegmentFileIo io) throws IOException {
        writeIdentity(path, bytes, io, null);
    }

    static void writeIdentity(Path path, ByteBuffer bytes, SegmentFileIo io, SegmentOwnedFiles files)
            throws IOException {
        try (FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            io.writeFully(channel, bytes, 0);
            io.force(channel, true);
        }
    }

    static void syncDirectory(Path directory) throws IOException {
        syncDirectory(directory, null);
    }

    static void syncDirectory(Path directory, SegmentOwnedFiles files) throws IOException {
        try (FileChannel channel = SegmentOwnedFiles.open(files, directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    synchronized boolean closedForDrain() {
        return closed;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (activeIo != 0 || pendingReservations != 0 || reclamation != null) {
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
        if (sessionReady) {
            boolean healthy = first == null;
            for (boolean fileFailed : failed) {
                healthy &= !fileFailed;
            }
            sessionReady = false;
            try {
                // Admission and DATA descriptors are closed, but directory exclusion is retained.
                sessionAuthority.finishDataSession(this, healthy);
            } catch (IOException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        if (ownedFiles == null) {
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
        if (first != null) {
            throw first;
        }
    }
}

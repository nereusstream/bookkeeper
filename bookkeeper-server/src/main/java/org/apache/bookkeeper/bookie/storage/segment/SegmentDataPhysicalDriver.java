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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Real DATA I/O and isolated local results; no production Bookie ACK authority. */
final class SegmentDataPhysicalDriver {
    @FunctionalInterface
    interface Completion {
        void complete(List<SegmentPointRead.Location> candidates, Throwable failure);
    }

    @FunctionalInterface
    interface RelocationCompletion {
        void complete(DurableRelocation copy, Throwable failure);
    }

    record RelocationCopy(SegmentPointRead.Location location, String entrySha256) { }

    /** Actual copy durability only. MOVE must separately validate its predecessor, grants and selector. */
    static final class DurableRelocation {
        private final IsolatedSegmentConfig sourceConfig;
        private final SegmentFileHeader sourceIdentity;
        private final UUID operationId;
        private final Generation operationGeneration;
        private final List<RelocationCopy> copies;

        private DurableRelocation(IsolatedSegmentFiles files, UUID operationId, Generation operationGeneration,
                                   List<RelocationCopy> copies) {
            sourceConfig = files.config();
            sourceIdentity = files.identity();
            this.operationId = operationId;
            this.operationGeneration = operationGeneration;
            this.copies = List.copyOf(copies);
        }

        IsolatedSegmentConfig sourceConfig() {
            return sourceConfig;
        }

        SegmentFileHeader sourceIdentity() {
            return sourceIdentity;
        }

        UUID operationId() {
            return operationId;
        }

        Generation operationGeneration() {
            return operationGeneration;
        }

        List<RelocationCopy> copies() {
            return copies;
        }
    }

    enum NormalStatus {
        LOCAL_DURABLE, STALE_ADMISSION, TRANSIENT_UNAVAILABLE, CONFLICT, UNKNOWN
    }

    record NormalResult(ProfileLedgerContext context, long entryId, NormalStatus status,
                        SegmentPointRead.Location candidate) { }

    enum RecoveryStatus {
        LOCAL_DURABLE, STALE_ADMISSION, TRANSIENT_UNAVAILABLE, CONFLICT, UNKNOWN
    }

    record RecoveryResult(ProfileLedgerContext context, long entryId, RecoveryStatus status,
                          SegmentPointRead.Location candidate) { }

    @FunctionalInterface
    interface RecoveryCompletion {
        void complete(List<RecoveryResult> results, Throwable failure);
    }

    @FunctionalInterface
    interface NormalCompletion {
        void complete(List<NormalResult> results, Throwable failure);
    }

    @FunctionalInterface
    interface NormalEntryCompletion {
        void complete(NormalResult result, Throwable failure);
    }

    /** A caller may stop waiting without cancelling the submitted DATA or its Bookie admission. */
    static final class NormalRequest {
        private final AtomicReference<NormalEntryCompletion> response;

        NormalRequest(NormalEntryCompletion completion) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(NormalResult result, Throwable failure) {
            NormalEntryCompletion completion = response.getAndSet(null);
            if (completion != null) {
                completion.complete(result, failure);
            }
        }
    }

    @FunctionalInterface
    interface RecoveryEntryCompletion {
        void complete(RecoveryResult result, Throwable failure);
    }

    /** Response cancellation never cancels a recovery DATA or grant obligation. */
    static final class RecoveryRequest {
        private final AtomicReference<RecoveryEntryCompletion> response;

        RecoveryRequest(RecoveryEntryCompletion completion) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(RecoveryResult result, Throwable failure) {
            RecoveryEntryCompletion completion = response.getAndSet(null);
            if (completion != null) {
                completion.complete(result, failure);
            }
        }
    }

    enum DuplicateStatus {
        SAME_DURABLE, CONFLICT, STALE_ADMISSION, NOT_READY, UNKNOWN
    }

    @FunctionalInterface
    interface DuplicateCompletion {
        void complete(DuplicateStatus status, Throwable failure);
    }

    /** A duplicate caller may stop waiting while its comparison and Bookie ticket remain in flight. */
    static final class DuplicateRequest {
        private final AtomicReference<DuplicateCompletion> response;

        DuplicateRequest(DuplicateCompletion completion) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        private void complete(DuplicateStatus status, Throwable failure) {
            DuplicateCompletion completion = response.getAndSet(null);
            if (completion != null) {
                completion.complete(status, failure);
            }
        }
    }

    private record NormalKey(ProfileLedgerContext context, long entryId) { }

    private static final class PendingNormal {
        private final NormalKey key;
        private final SegmentEntryIdentity identity;
        private List<WaitingDuplicate> waiters = new ArrayList<>();

        private PendingNormal(SegmentBatchParser.Record record) throws IOException {
            key = new NormalKey(record.context(), record.entryId());
            identity = SegmentEntryIdentity.parse(record.context(), record.entry());
        }
    }

    private record WaitingDuplicate(SegmentOwnerLoop.Ticket ticket, Obligation obligation,
                                    AtomicReference<DuplicateStatus> status,
                                    AtomicReference<SegmentPointRead.Location> selected) { }

    private static final class Obligation {
        private Completion completion;
        private final List<SegmentBookieControlLog.Admission> admissions;
        private final List<SegmentBookieControlLog.RecoveryAdmission> recoveryAdmissions;
        private final List<PendingNormal> normals;
        private SegmentBatchFormat.FrozenBatch batch;
        private SegmentBufferPool.Lease readBuffer;
        private IsolatedSegmentFiles.DataReadLease readPin;
        private Runnable releaseSource;
        private List<SegmentArenaControlDriver.FailureLease> failureLeases = List.of();
        private boolean physicalTerminal;
        private boolean ownerTerminal;
        private boolean released;

        private Obligation(Completion completion, List<SegmentBookieControlLog.Admission> admissions) {
            this(completion, admissions, List.of());
        }

        private Obligation(Completion completion, List<SegmentBookieControlLog.Admission> admissions,
                           List<PendingNormal> normals) {
            this(completion, admissions, List.of(), normals);
        }

        private Obligation(Completion completion, List<SegmentBookieControlLog.Admission> admissions,
                           List<SegmentBookieControlLog.RecoveryAdmission> recoveryAdmissions,
                           List<PendingNormal> normals) {
            this.completion = completion;
            this.admissions = List.copyOf(admissions);
            this.recoveryAdmissions = List.copyOf(recoveryAdmissions);
            this.normals = List.copyOf(normals);
        }

        private Completion takeCompletion() {
            if (ownerTerminal || completion == null) {
                throw new IllegalStateException("DATA result already terminal");
            }
            Completion result = completion;
            completion = null;
            return result;
        }

        synchronized void physicalTerminal() {
            if (physicalTerminal) {
                throw new IllegalStateException("Physical DATA already terminal");
            }
            physicalTerminal = true;
            releaseIfTerminal();
        }

        synchronized void physicalTerminalIfNeeded() {
            if (!physicalTerminal) {
                physicalTerminal();
            }
        }

        synchronized void ownerTerminal() {
            if (ownerTerminal) {
                throw new IllegalStateException("DATA result already terminal");
            }
            ownerTerminal = true;
            releaseIfTerminal();
        }

        void abortBeforeOwner() {
            physicalTerminal();
            ownerTerminal();
        }

        private void releaseIfTerminal() {
            if (physicalTerminal && ownerTerminal && !released) {
                released = true;
                try {
                    for (SegmentBookieControlLog.Admission admission : admissions) {
                        admission.driverTerminal();
                    }
                    for (SegmentBookieControlLog.RecoveryAdmission admission : recoveryAdmissions) {
                        admission.driverTerminal();
                    }
                } finally {
                    try {
                        if (batch != null) {
                            batch.releaseFromDriver();
                        }
                    } finally {
                        try {
                            if (readBuffer != null) {
                                readBuffer.close();
                            }
                        } finally {
                            try {
                                if (readPin != null) {
                                    readPin.close();
                                }
                            } finally {
                                try {
                                    if (releaseSource != null) {
                                        releaseSource.run();
                                    }
                                } finally {
                                    for (SegmentArenaControlDriver.FailureLease lease : failureLeases) {
                                        lease.close();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private final int shard;
    private final SegmentOwnerLoop owner;
    private final SegmentIoExecutor io;
    private final IsolatedSegmentFiles files;
    private final SegmentArenaControlLog authority;
    private final SegmentPhysicalPrefix prefix;
    private final SegmentHotDirectory hot;
    private final Map<IsolatedSegmentFiles.Reservation, Obligation> pending = new IdentityHashMap<>();
    private final Map<NormalKey, PendingNormal> pendingNormals = new HashMap<>();
    private NormalBatcher normalBatcher;

    /** Known service normal/recovery Java graph; raw callers, compaction plans and opaque runtimes are separate. */
    static long metadataCapacity(IsolatedSegmentConfig config, SegmentExecutionGroup.Settings execution)
            throws IOException {
        return metadataCapacity(config, execution, SegmentControlCapacity.Layout.hotSpot());
    }

    static long metadataCapacity(IsolatedSegmentConfig config, SegmentExecutionGroup.Settings execution,
                                 SegmentControlCapacity.Layout layout) {
        long shards = config.logicalShards();
        long pending = config.maxPendingEntries();
        long workers = execution.workers();
        long dataThreads = execution.ioThreads() - 1L;
        long records = Math.min(parsingRecords(execution), pending);
        // Bookie admissions and owner tickets can terminate independently. A ready/failure owner snapshot
        // can retain P records; a returning DATA I/O task retains one batch, rather than the whole snapshot.
        long held = SegmentControlCapacity.sum(SegmentControlCapacity.times(2, pending),
                SegmentControlCapacity.times(pending, workers), SegmentControlCapacity.times(records, dataThreads));
        // A current pool/outer owner ticket and a gate caller can outlive their already completed Entry.
        long commands = SegmentControlCapacity.sum(held, workers, 1);
        long oldRecords = SegmentControlCapacity.sum(pending,
                SegmentControlCapacity.times(workers, pending - records));
        long parsingOverflow = SegmentControlCapacity.sum(
                SegmentControlCapacity.times(oldRecords, pendingNormalMetadataCapacity(layout)),
                SegmentControlCapacity.times(2, layout.refs(oldRecords)));
        long hotOverflow = SegmentControlCapacity.times(SegmentControlCapacity.times(workers, pending - 1),
                SegmentHotDirectory.batchHandleCapacity(layout));
        long request = Math.max(layout.object(NormalRequest.class), layout.object(RecoveryRequest.class));
        long admission = Math.max(layout.object(SegmentBookieControlLog.Admission.class),
                layout.object(SegmentBookieControlLog.RecoveryAdmission.class));
        long core = SegmentControlCapacity.sum(layout.object(NormalBatcher.Entry.class), request, admission,
                layout.object(Obligation.class), layout.object(PhysicalCommand.class),
                layout.object(WaitingDuplicate.class), layout.object(SegmentOwnerLoop.Ticket.class),
                layout.object(ArrayList.class), SegmentControlCapacity.times(2, layout.object(AtomicBoolean.class)),
                SegmentControlCapacity.times(3, layout.object(AtomicReference.class)),
                // Batch/duplicate completion, source release, pool/index resume, physical operation/result,
                // and the I/O return/rejection adapters have at most eight reference-width captures each.
                SegmentControlCapacity.times(8, layout.captured(8)));
        long fixed = SegmentControlCapacity.sum(layout.object(BatchLimits.class), layout.object(Duration.class),
                SegmentControlCapacity.times(shards, SegmentControlCapacity.sum(
                        layout.object(SegmentDataPhysicalDriver.class), layout.object(NormalBatcher.class),
                        layout.object(SegmentPhysicalPrefix.class), layout.object(Generation.class),
                        layout.hashMap(), layout.hashMap(),
                        layout.identityMap(pending), layout.object("java.util.IdentityHashMap$Values"),
                        layout.object("java.util.HashMap$Values"), layout.object(ArrayList.class), layout.captured(1))),
                SegmentControlCapacity.times(SegmentControlCapacity.times(shards, pending),
                        SegmentControlCapacity.sum(SegmentControlCapacity.times(2, layout.hashEntry()),
                                layout.object(Long.class), layout.object("org.apache.bookkeeper.bookie.storage.segment."
                                        + "SegmentPhysicalPrefix$Pending"))));
        for (int shard = 0; shard < config.logicalShards(); shard++) {
            int worker = shard % execution.workers();
            long ownerCapacity = pending / workers + (worker < pending % workers ? 1 : 0);
            long assembling = Math.min(records, ownerCapacity);
            fixed = SegmentControlCapacity.sum(fixed, layout.hashMap(),
                    listBackingCapacity(layout, 1, assembling), SegmentControlCapacity.times(assembling,
                            SegmentControlCapacity.sum(layout.hashEntry(), layout.object(NormalKey.class))));
        }
        // Parent/child ticket ordering bounds duplicates' historical arrays. A removed PendingNormal
        // drops its old waiter container after notification, so a late parent I/O cannot retain that array.
        long historical = SegmentControlCapacity.times(pending,
                SegmentControlCapacity.sum(2, workers, dataThreads));
        long lists = SegmentControlCapacity.sum(listBackingCapacity(layout, commands, historical),
                listBackingCapacity(layout, held, historical),
                // Admission/recovery, failure-lease, captured Entry and returned-location list wrappers.
                SegmentControlCapacity.times(SegmentControlCapacity.times(5, commands), layout.immutableList(0)),
                SegmentControlCapacity.times(4, layout.refs(commands)));
        // Closed leases can retain an older CONTROL notice after its active per-file reservation was reused.
        long closedLeases = SegmentControlCapacity.sum(SegmentControlCapacity.times(commands - pending,
                layout.object(SegmentArenaControlDriver.FailureLease.class)),
                SegmentControlCapacity.times(commands, SegmentArenaControlDriver.failureNoticeCapacity(layout)));
        // A returned location can outlive its evicted hot candidate. Its context/address are not the
        // independently parsed PendingNormal identity already paid by parsingCapacity().
        long returned = SegmentControlCapacity.times(commands, SegmentControlCapacity.sum(
                Math.max(layout.object(NormalResult.class), layout.object(RecoveryResult.class)),
                layout.object(SegmentPointRead.Location.class), layout.context(),
                layout.object(SegmentBatchFormat.Address.class),
                SegmentControlCapacity.times(2, layout.object(Generation.class))));
        long ownerWorkspace = SegmentControlCapacity.times(workers, SegmentControlCapacity.sum(
                SegmentControlCapacity.times(2, layout.list()), SegmentControlCapacity.times(8, layout.refs(pending)),
                SegmentControlCapacity.times(8, layout.list()), SegmentControlCapacity.times(8, layout.refs(records)),
                SegmentControlCapacity.times(4, layout.object("java.util.ArrayList$Itr")),
                SegmentControlCapacity.times(4, layout.object("java.util.ImmutableCollections$ListItr")),
                layout.object("java.util.IdentityHashMap$ValueIterator"),
                layout.object("java.util.HashMap$ValueIterator"),
                // Synchronous encoder context bytes, slices/CRC and its heap validator call-site temporaries.
                layout.bytes(ProfileLedgerContext.ENCODED_LENGTH), layout.buffer(), layout.object(CRC32C.class),
                SegmentControlCapacity.times(2,
                        Math.max(layout.buffer(), layout.object("java.nio.DirectByteBufferR"))),
                layout.refs(4), SegmentControlCapacity.times(4, layout.object(Integer.class)),
                SegmentControlCapacity.times(pending, SegmentControlCapacity.sum(layout.object(Delivery.class),
                        layout.object("org.apache.bookkeeper.bookie.storage.segment."
                                + "SegmentPhysicalPrefix$DurableBatch")))));
        return SegmentControlCapacity.sum(sourceMetadataCapacity(config, execution, layout), fixed,
                SegmentControlCapacity.times(commands, core), lists, closedLeases, returned, ownerWorkspace,
                parsingOverflow, hotOverflow);
    }

    private static long listBackingCapacity(SegmentControlCapacity.Layout layout, long lists, long highWater) {
        // ArrayList's default/growth old+new arrays fit 20+4m wide slots. Include a header and worst
        // alignment tail for each array, even when the lists reach their historical peaks at different times.
        return SegmentControlCapacity.sum(SegmentControlCapacity.times(SegmentControlCapacity.times(2, lists),
                layout.arrayMetadata()), SegmentControlCapacity.times(8, SegmentControlCapacity.sum(
                        SegmentControlCapacity.times(20, lists), SegmentControlCapacity.times(4, highWater))));
    }

    /**
     * Service source descriptors and preparation workspaces, including one old source held by the gate caller.
     * Input core state, pooled parsing, persistent commands and opaque checksum backend internals are separate.
     */
    static long sourceMetadataCapacity(IsolatedSegmentConfig config, SegmentExecutionGroup.Settings execution,
                                       SegmentControlCapacity.Layout layout) {
        long inputs = execution.inputBudgetBytes() / SegmentBatchFormat.ENTRY_OVERHEAD;
        long heapBuffer = layout.buffer();
        long buffer = Math.max(heapBuffer, Math.max(layout.object("java.nio.DirectByteBuffer"),
                layout.object("java.nio.DirectByteBufferR")));
        // Context, backing and the cached root NIO wrapper remain in Input core, rather than each descriptor.
        long descriptor = SegmentControlCapacity.sum(layout.object(SegmentBatchFormat.Data.class),
                layout.object(UUID.class), layout.object(SegmentEntryIdentity.class),
                SegmentControlCapacity.times(2, heapBuffer));
        long maxEntryBytes = SegmentControlCapacity.sum(SegmentBatchFormat.ENTRY_OVERHEAD,
                SegmentBatchFormat.maxPayload(config));
        // The owner can release the Input before submitEntry returns to its still-live caller frame.
        long oldSource = SegmentControlCapacity.sum(SegmentEntryInputs.inputMetadataCapacity(layout),
                descriptor, maxEntryBytes);
        // The outer Profile preparse identity/view can remain while the inner heap source is prepared.
        long outerParsed = SegmentControlCapacity.sum(layout.object(SegmentEntryIdentity.class), buffer);
        // Each service preparation uses two singleton arguments: two Input-claim and two Bookie-claim loops.
        long claims = SegmentControlCapacity.sum(layout.identityMap(1),
                SegmentControlCapacity.times(2, layout.object("java.util.ImmutableCollections$List12")),
                SegmentControlCapacity.times(4, layout.object("java.util.ImmutableCollections$ListItr")));
        // Heap/direct slices and CRC32C belong to one gate preparation, including rejected inputs.
        // Java9IntHash's reflective call site adds a four-slot argument array and three input/one result boxes.
        long preparation = SegmentControlCapacity.sum(SegmentControlCapacity.times(6, buffer),
                layout.object(CRC32C.class), claims, layout.refs(4),
                SegmentControlCapacity.times(4, layout.object(Integer.class)));
        // duplicateView is constructed on each owner, outside the storage gate, using two temporary heap views.
        long duplicateViews = SegmentControlCapacity.times(SegmentControlCapacity.times(2, execution.workers()),
                heapBuffer);
        return SegmentControlCapacity.sum(SegmentControlCapacity.times(inputs, descriptor), oldSource, outerParsed,
                preparation, duplicateViews);
    }

    /** Service pooled parsing plus its separately decoded PendingNormal values; other driver state is separate. */
    static long parsingCapacity(IsolatedSegmentConfig config, SegmentExecutionGroup.Settings execution)
            throws IOException {
        var layout = SegmentControlCapacity.Layout.hotSpot();
        int records = parsingRecords(execution);
        // Each owner parses synchronously; the storage gate permits one concurrent maintenance batch scan.
        long workspaces = SegmentControlCapacity.times(execution.workers() + 1L,
                SegmentHotDirectory.parsingWorkspace(layout, records));
        long returning = SegmentControlCapacity.sum(execution.workers(), execution.ioThreads() - 1L);
        long held = SegmentControlCapacity.sum(config.maxPendingEntries(), SegmentControlCapacity.times(
                Math.min(records, config.maxPendingEntries()), returning));
        long value = pendingNormalMetadataCapacity(layout);
        // Pool callbacks are consumed before invocation, so old CONTROL tasks retain no completed leader.
        return SegmentControlCapacity.sum(workspaces, SegmentControlCapacity.times(held, value),
                SegmentControlCapacity.times(2, layout.refs(held)));
    }

    private static int parsingRecords(SegmentExecutionGroup.Settings execution) {
        int capacity = Math.min(SegmentBatchFormat.MAX_BATCH, execution.bufferBytes());
        return Math.min(SegmentBatchFormat.MAX_RECORDS,
                (capacity - SegmentBatchFormat.HEADER - SegmentBatchFormat.FOOTER)
                        / (SegmentBatchFormat.RECORD_HEADER + SegmentBatchFormat.ENTRY_OVERHEAD));
    }

    private static long pendingNormalMetadataCapacity(SegmentControlCapacity.Layout layout) {
        return SegmentControlCapacity.sum(layout.object(PendingNormal.class), layout.object(NormalKey.class),
                layout.object(SegmentEntryIdentity.class), layout.context(),
                Math.max(layout.buffer(), layout.object("java.nio.DirectByteBufferR")),
                SegmentControlCapacity.times(2, layout.list()));
    }

    SegmentDataPhysicalDriver(int shard, SegmentOwnerLoop owner, SegmentIoExecutor io,
                              IsolatedSegmentFiles files, SegmentArenaControlLog authority,
                              SegmentPhysicalPrefix prefix, SegmentHotDirectory hot) {
        this.shard = shard;
        this.owner = Objects.requireNonNull(owner, "owner");
        this.io = Objects.requireNonNull(io, "io");
        this.files = Objects.requireNonNull(files, "files");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.hot = Objects.requireNonNull(hot, "hot");
        if (!files.supportsLocalResults()) {
            throw new IllegalArgumentException("Append-only physical restart has no local-result authority");
        }
        if (!hot.matchesScope(files)) {
            throw new IllegalArgumentException("Physical driver and hot directory storage scopes differ");
        }
        hot.bindPrefix(prefix, authority);
        SegmentBookieControlLog restartBookie = files.restartBookie();
        if (restartBookie != null) {
            owner.requireOwner();
            synchronized (restartBookie) {
                synchronized (files) {
                    try {
                        SegmentDataRecovery.PhysicalInventory inventory = files.localResultInventory(authority, shard);
                        long through = prefix.restartThrough(owner, shard, inventory, authority);
                        hot.restoreCleanRestart(inventory, shard);
                        prefix.restoreCleanThrough(through);
                        files.restoredShard(shard);
                    } catch (IOException failure) {
                        throw new IllegalArgumentException("Clean restart DATA owner could not be restored", failure);
                    }
                }
            }
        }
    }

    boolean submit(SegmentBatchFormat.FrozenBatch batch, Completion completion) {
        Objects.requireNonNull(completion, "completion");
        batch.claimForDriver();
        return submitPhysical(batch, new Obligation(completion, List.of()));
    }

    SegmentBatchFormat.Address relocationAddress(SegmentArenaControlLog.Allocation allocation, long offset)
            throws IOException {
        return prefix.relocationAddress(allocation, offset);
    }

    /** Consume a copy-only frozen batch through the same DATA I/O, prefix and original-owner obligation. */
    boolean submitRelocation(SegmentBatchFormat.FrozenBatch batch, RelocationCompletion completion)
            throws IOException {
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        batch.claimForDriver();
        List<RelocationCopy> copies;
        UUID operation;
        Generation generation;
        try {
            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(batch.bytes(), batch.address(),
                    files.config().bytesPerFile());
            SegmentBatchParser.Record first = parsed.records().get(0);
            if (first.origin() != SegmentBatchFormat.RELOCATION_COPY) {
                throw new IOException("Relocation submission requires copy-only records");
            }
            operation = first.moveId();
            generation = new Generation(first.moveGeneration());
            List<SegmentPointRead.Location> locations = SegmentPointRead.locations(parsed);
            List<RelocationCopy> validated = new ArrayList<>(locations.size());
            HashSet<NormalKey> coordinates = new HashSet<>();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < parsed.records().size(); i++) {
                SegmentBatchParser.Record record = parsed.records().get(i);
                if (record.origin() != SegmentBatchFormat.RELOCATION_COPY || !operation.equals(record.moveId())
                        || generation.value() != record.moveGeneration()
                        || SegmentOwnerLoop.shardFor(record.context().ledgerId(), files.config().logicalShards())
                            != shard || batch.address().shard() != shard
                        || !coordinates.add(new NormalKey(record.context(), record.entryId()))) {
                    throw new IOException("Relocation batch has mixed origin, operation, coordinate or logical shard");
                }
                digest.update(record.entry().duplicate());
                validated.add(new RelocationCopy(locations.get(i), HexFormat.of().formatHex(digest.digest())));
            }
            copies = List.copyOf(validated);
        } catch (IOException | RuntimeException | Error failure) {
            batch.releaseFromDriver();
            throw failure;
        } catch (NoSuchAlgorithmException impossible) {
            batch.releaseFromDriver();
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
        Completion internal = (ordinary, failure) -> {
            owner.requireOwner();
            if (failure == null && (ordinary == null || !ordinary.isEmpty()
                    || !authority.dataFileAvailable(batch.address().fileIndex())
                    || copies.stream().anyMatch(copy -> !prefix.containsDurable(copy.location())))) {
                failure = new IOException("Relocation copy has no complete current physical durability prefix");
            }
            completion.complete(failure == null ? new DurableRelocation(files, operation, generation, copies) : null,
                    failure);
        };
        return submitPhysical(batch, new Obligation(internal, List.of()));
    }

    /**
     * The caller acquires one Bookie ticket per normal record before submitting the frozen batch.
     * Once claimed, this driver releases every ticket and pooled buffer even when validation or
     * owner admission fails. Caller ownership of a pooled batch ends when this method claims it.
     */
    boolean submitNormal(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                         List<SegmentBookieControlLog.Admission> admissions, NormalCompletion completion)
            throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        List<SegmentBookieControlLog.Admission> input = List.copyOf(admissions);
        batch.claimForDriver();
        try {
            bookie.claimForDriver(input);
        } catch (IOException | RuntimeException | Error failure) {
            batch.releaseFromDriver();
            throw failure;
        }
        Obligation obligation;
        try {
            obligation = prepareNormal(bookie, batch, input, completion);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                finishAdmissions(input);
            } finally {
                batch.releaseFromDriver();
            }
            throw failure;
        }
        return submitPhysical(batch, obligation);
    }

    private Obligation prepareNormal(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                                     List<SegmentBookieControlLog.Admission> input, NormalCompletion completion)
            throws IOException {
        List<SegmentBatchParser.Record> records;
        List<PendingNormal> normals;
        if (!bookie.matchesScope(files)) {
            throw new IOException("Bookie control log and DATA files have different storage scopes");
        }
        SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(batch.bytes(), batch.address(),
                Long.MAX_VALUE);
        if (parsed.records().size() != input.size()) {
            throw new IOException("Normal DATA admission count differs from frozen records");
        }
        for (int i = 0; i < input.size(); i++) {
            SegmentBatchParser.Record record = parsed.records().get(i);
            if (record.origin() != SegmentBatchFormat.ENTRY_DATA
                    || !record.context().equals(input.get(i).context())
                    || SegmentOwnerLoop.shardFor(record.context().ledgerId(),
                            files.config().logicalShards()) != batch.address().shard()) {
                throw new IOException("Normal DATA admission or logical shard does not match frozen record");
            }
        }
        records = parsed.records();
        List<PendingNormal> identities = new ArrayList<>(records.size());
        for (SegmentBatchParser.Record record : records) {
            identities.add(new PendingNormal(record));
        }
        normals = List.copyOf(identities);
        Completion physical = (candidates, failure) -> {
            if (failure == null && (candidates == null || candidates.size() != records.size())) {
                finishPendingNormals(normals, null, new IOException("Normal DATA candidate count mismatch"));
                throw new IllegalStateException("Normal DATA lacks one published candidate per entry");
            }
            finishPendingNormals(normals, candidates, failure);
            List<NormalResult> results = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                SegmentBatchParser.Record record = records.get(i);
                NormalStatus status = failure != null ? NormalStatus.UNKNOWN
                        : bookie.canCompleteNormal(input.get(i)) ? NormalStatus.LOCAL_DURABLE
                                : NormalStatus.STALE_ADMISSION;
                results.add(new NormalResult(record.context(), record.entryId(), status,
                        failure == null ? candidates.get(i) : null));
            }
            completion.complete(List.copyOf(results), failure);
        };
        return new Obligation(physical, input, normals);
    }

    private static void finishAdmissions(List<SegmentBookieControlLog.Admission> admissions) {
        for (SegmentBookieControlLog.Admission admission : admissions) {
            admission.driverTerminal();
        }
    }

    /** Recovery uses the same DATA barrier while each ticket binds its exact wire entry. */
    boolean submitRecovery(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                           List<SegmentBookieControlLog.RecoveryAdmission> admissions,
                           RecoveryCompletion completion) throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        List<SegmentBookieControlLog.RecoveryAdmission> input = List.copyOf(admissions);
        batch.claimForDriver();
        try {
            bookie.claimRecoveryForDriver(input);
        } catch (IOException | RuntimeException | Error failure) {
            batch.releaseFromDriver();
            throw failure;
        }
        List<SegmentBatchParser.Record> records;
        List<PendingNormal> entries;
        try {
            if (!bookie.matchesScope(files)) {
                throw new IOException("Bookie control log and DATA files have different storage scopes");
            }
            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(batch.bytes(), batch.address(),
                    Long.MAX_VALUE);
            if (parsed.records().size() != input.size()) {
                throw new IOException("Recovery DATA admission count differs from frozen records");
            }
            List<PendingNormal> identities = new ArrayList<>(parsed.records().size());
            for (int i = 0; i < input.size(); i++) {
                SegmentBatchParser.Record record = parsed.records().get(i);
                SegmentBookieControlLog.RecoveryAdmission admission = input.get(i);
                if (record.origin() != SegmentBatchFormat.ENTRY_DATA
                        || !record.context().equals(admission.context())
                        || record.entryId() != admission.entryId()
                        || !record.entry().equals(ByteBuffer.wrap(admission.entryPayload()))
                        || SegmentOwnerLoop.shardFor(record.context().ledgerId(),
                                files.config().logicalShards()) != batch.address().shard()) {
                    throw new IOException("Recovery DATA admission does not match frozen record");
                }
                PendingNormal identity = new PendingNormal(record);
                if (identity.identity.entryId() != admission.entryId()) {
                    throw new IOException("Recovery BK entry ID differs from the frozen record");
                }
                identities.add(identity);
            }
            records = parsed.records();
            entries = List.copyOf(identities);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                for (SegmentBookieControlLog.RecoveryAdmission admission : input) {
                    admission.driverTerminal();
                }
            } finally {
                batch.releaseFromDriver();
            }
            throw failure;
        }
        Completion physical = (candidates, failure) -> {
            if (failure == null && (candidates == null || candidates.size() != records.size())) {
                finishPendingNormals(entries, null, new IOException("Recovery DATA candidate count mismatch"));
                throw new IllegalStateException("Recovery DATA lacks one published candidate per entry");
            }
            finishPendingNormals(entries, candidates, failure);
            List<RecoveryResult> results = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                SegmentBatchParser.Record record = records.get(i);
                RecoveryStatus status = failure != null ? RecoveryStatus.UNKNOWN
                        : bookie.canCompleteRecovery(input.get(i)) ? RecoveryStatus.LOCAL_DURABLE
                                : RecoveryStatus.STALE_ADMISSION;
                results.add(new RecoveryResult(record.context(), record.entryId(), status,
                        failure == null ? candidates.get(i) : null));
            }
            completion.complete(List.copyOf(results), failure);
        };
        return submitPhysical(batch, new Obligation(physical, List.of(), input, entries));
    }

    /** Each entry has its own response waiter; abandoning one does not change batch ownership. */
    boolean submitNormal(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                         List<SegmentBookieControlLog.Admission> admissions, List<NormalRequest> requests)
            throws IOException {
        List<NormalRequest> waiters = List.copyOf(requests);
        if (waiters.size() != admissions.size()) {
            throw new IllegalArgumentException("Normal request count differs from admissions");
        }
        return submitNormal(bookie, batch, admissions,
                (results, failure) -> completeNormalRequests(waiters, results, failure));
    }

    private static void completeNormalRequests(List<NormalRequest> waiters, List<NormalResult> results,
                                              Throwable failure) {
        Throwable callbackFailure = null;
        for (int i = 0; i < waiters.size(); i++) {
            try {
                waiters.get(i).complete(results.get(i), failure);
            } catch (Throwable error) {
                if (callbackFailure == null) {
                    callbackFailure = error;
                } else if (callbackFailure != error) {
                    callbackFailure.addSuppressed(error);
                }
            }
        }
        if (callbackFailure != null) {
            throw new IllegalStateException("Normal request callback failed", callbackFailure);
        }
    }

    /**
     * Takes exact-capacity source inputs from this owner's execution group and one pre-cut Bookie ticket
     * per entry. After the atomic source claim, validation failure or owner refusal releases the sources;
     * after the Bookie claim it releases those tickets too. Caller views must not be used after transfer.
     * Conversion and physical submission share one owner/completion ticket; no second DATA queue admission.
     */
    boolean submitNormalInputs(SegmentBookieControlLog bookie, SegmentBatchFormat.Address address,
                               List<SegmentEntryInputs.Input> inputs,
                               List<SegmentBookieControlLog.Admission> admissions, List<NormalRequest> requests)
            throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(address, "address");
        List<SegmentEntryInputs.Input> sources = List.copyOf(inputs);
        List<SegmentBookieControlLog.Admission> permissions = List.copyOf(admissions);
        List<NormalRequest> waiters = List.copyOf(requests);
        if (sources.isEmpty() || sources.size() != permissions.size() || sources.size() != waiters.size()) {
            throw new IllegalArgumentException("Normal source, admission and request counts differ");
        }
        owner.inputs().claim(sources);
        try {
            bookie.claimForDriver(permissions);
        } catch (IOException | RuntimeException | Error failure) {
            releaseInputs(sources);
            throw failure;
        }
        List<SegmentBatchFormat.Data> data;
        List<Long> entryIds;
        try {
            data = sources.stream().map(SegmentEntryInputs.Input::data).toList();
            if (!bookie.matchesScope(files) || address.shard() != shard) {
                throw new IOException("Normal input and physical driver storage scopes or shards differ");
            }
            SegmentBatchFormat.validate(files.config(), address, data);
            for (int i = 0; i < data.size(); i++) {
                if (!data.get(i).context().equals(permissions.get(i).context())
                        || SegmentOwnerLoop.shardFor(data.get(i).context().ledgerId(),
                                files.config().logicalShards()) != shard) {
                    throw new IOException("Normal source does not match its Bookie admission or shard");
                }
            }
            entryIds = data.stream().map(record -> record.entry().getLong(record.entry().readerIndex() + 8)).toList();
        } catch (IOException | RuntimeException | Error failure) {
            try {
                finishAdmissions(permissions);
            } finally {
                releaseInputs(sources);
            }
            throw failure;
        }
        InputCommand command = new InputCommand(bookie, address, sources, data, permissions, entryIds, waiters);
        boolean accepted;
        try {
            SegmentArenaControlDriver allocator = normalBatcher == null ? null : normalBatcher.allocator;
            if (allocator != null) {
                command.failureLease = allocator.reserveFailure(address.fileIndex());
                if (command.failureLease == null) {
                    command.abortBeforeOwner();
                    return false;
                }
            }
            accepted = owner.submit(shard, command);
        } catch (RuntimeException | Error failure) {
            command.abortBeforeOwner();
            throw failure;
        }
        if (!accepted) {
            command.abortBeforeOwner();
        }
        return accepted;
    }

    private static void releaseInputs(List<SegmentEntryInputs.Input> inputs) {
        for (SegmentEntryInputs.Input input : inputs) {
            input.releaseFromDriver();
        }
    }

    private final class InputCommand implements SegmentOwnerLoop.Command {
        private final SegmentBookieControlLog bookie;
        private final SegmentBatchFormat.Address address;
        private List<SegmentEntryInputs.Input> sources;
        private List<SegmentBatchFormat.Data> data;
        private final List<SegmentBookieControlLog.Admission> admissions;
        private final List<Long> entryIds;
        private final List<NormalRequest> requests;
        private boolean sourcesReleased;
        private SegmentBatchFormat.FrozenBatch batch;
        private PhysicalCommand physical;
        private SegmentArenaControlDriver.FailureLease failureLease;

        private InputCommand(SegmentBookieControlLog bookie, SegmentBatchFormat.Address address,
                             List<SegmentEntryInputs.Input> sources, List<SegmentBatchFormat.Data> data,
                             List<SegmentBookieControlLog.Admission> admissions, List<Long> entryIds,
                             List<NormalRequest> requests) {
            this.bookie = bookie;
            this.address = address;
            this.sources = sources;
            this.data = data;
            this.admissions = admissions;
            this.entryIds = entryIds;
            this.requests = requests;
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket ticket) {
            try {
                batch = SegmentBatchFormat.encodePooled(files.config(), address, data, owner.pool());
            } finally {
                releaseSources();
            }
            if (batch == null) {
                ticket.complete(() -> finishBeforePhysical(NormalStatus.TRANSIENT_UNAVAILABLE, null));
                return;
            }
            batch.claimForDriver();
            Obligation obligation;
            try {
                obligation = prepareNormal(bookie, batch, admissions,
                        (results, failure) -> completeNormalRequests(requests, results, failure));
            } catch (IOException failure) {
                ticket.complete(() -> finishBeforePhysical(NormalStatus.UNKNOWN, failure));
                return;
            }
            if (failureLease != null) {
                obligation.failureLeases = List.of(failureLease);
                failureLease = null;
            }
            physical = new PhysicalCommand(batch, obligation);
            physical.run(ticket);
        }

        @Override
        public void failed(Throwable failure) {
            owner.requireOwner();
            if (physical != null) {
                physical.failed(failure);
            } else {
                finishBeforePhysical(NormalStatus.UNKNOWN, failure);
            }
        }

        private void releaseSources() {
            if (!sourcesReleased) {
                data = List.of();
                releaseInputs(sources);
                sources = List.of();
                sourcesReleased = true;
            }
        }

        private void abortBeforeOwner() {
            try {
                releaseSources();
            } finally {
                try {
                    finishAdmissions(admissions);
                } finally {
                    releaseFailure();
                }
            }
        }

        private void finishBeforePhysical(NormalStatus status, Throwable failure) {
            try {
                List<NormalResult> results = new ArrayList<>(admissions.size());
                for (int i = 0; i < admissions.size(); i++) {
                    results.add(new NormalResult(admissions.get(i).context(), entryIds.get(i), status, null));
                }
                completeNormalRequests(requests, results, failure);
            } finally {
                try {
                    releaseSources();
                    if (batch != null) {
                        batch.releaseFromDriver();
                    }
                } finally {
                    try {
                        finishAdmissions(admissions);
                    } finally {
                        releaseFailure();
                    }
                }
            }
        }

        private void releaseFailure() {
            if (failureLease != null) {
                failureLease.close();
                failureLease = null;
            }
        }
    }

    record BatchLimits(int bytes, int entries, Duration delay) {
        BatchLimits {
            Objects.requireNonNull(delay, "delay");
            if (bytes < 4096 || bytes > SegmentBatchFormat.MAX_BATCH || bytes % 4096 != 0
                    || entries < 1 || entries > SegmentBatchFormat.MAX_RECORDS
                    || delay.isNegative() || delay.isZero()) {
                throw new IllegalArgumentException("Invalid normal batching limits");
            }
            delay.toNanos();
        }
    }

    /** One normal/recovery assembling batch and oldest deadline per fixed driver/shard. */
    NormalBatcher enableNormalBatching(SegmentBookieControlLog bookie, BatchLimits limits) {
        owner.requireOwner();
        Objects.requireNonNull(limits, "limits");
        int largestEntry = SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(files.config());
        int largest = SegmentBatchFormat.alignedLength(SegmentBatchFormat.RECORD_HEADER + largestEntry);
        if (normalBatcher != null || !bookie.matchesScope(files) || owner.pool() == null
                || limits.bytes() > owner.pool().bufferBytes() || largest > owner.pool().bufferBytes()
                || largestEntry > owner.inputs().capacityBytes()) {
            throw new IllegalArgumentException(
                    "Batching requires fixed source and pool capacity for the unified bound");
        }
        owner.inputs();
        normalBatcher = new NormalBatcher(bookie, limits);
        return normalBatcher;
    }

    /** Attach the actual Arena control owner for bounded asynchronous, demand-driven pool refill. */
    NormalBatcher enableNormalBatching(SegmentBookieControlLog bookie, BatchLimits limits,
                                       SegmentArenaControlDriver allocator, int poolBlocks) {
        owner.requireOwner();
        int largest = Math.max(limits.bytes(), SegmentBatchFormat.alignedLength(SegmentBatchFormat.RECORD_HEADER
                + SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(files.config())));
        if (!Objects.requireNonNull(allocator, "allocator").owns(authority)
                || !allocator.supportsFailureReservations()
                || poolBlocks < (largest + SegmentArenaControlLog.BLOCK_BYTES - 1)
                        / SegmentArenaControlLog.BLOCK_BYTES) {
            throw new IllegalArgumentException("Pool refill must cover a whole legal batch on this Arena");
        }
        authority.validatePoolRequest(prefix.poolRequest(poolBlocks));
        NormalBatcher batcher = enableNormalBatching(bookie, limits);
        batcher.allocator = allocator;
        batcher.poolBlocks = poolBlocks;
        return batcher;
    }

    final class NormalBatcher {
        private final SegmentBookieControlLog bookie;
        private final BatchLimits limits;
        private final List<Entry> assembling = new ArrayList<>();
        private final Map<NormalKey, Entry> assemblingKeys = new HashMap<>();
        private int recordBytes;
        private SegmentArenaControlDriver allocator;
        private int poolBlocks;
        private boolean awaitingPool;

        private NormalBatcher(SegmentBookieControlLog bookie, BatchLimits limits) {
            this.bookie = bookie;
            this.limits = limits;
        }

        boolean submit(SegmentEntryInputs.Input input, SegmentBookieControlLog.Admission admission,
                       NormalRequest request) throws IOException {
            Objects.requireNonNull(request, "request");
            long arrived = System.nanoTime();
            owner.inputs().claim(List.of(input));
            try {
                bookie.claimForDriver(List.of(admission));
            } catch (IOException | RuntimeException | Error failure) {
                input.releaseFromDriver();
                throw failure;
            }
            Entry entry;
            try {
                SegmentBatchFormat.Data data = input.data();
                if (!bookie.matchesScope(files) || !data.context().equals(admission.context())
                        || SegmentOwnerLoop.shardFor(data.context().ledgerId(),
                                files.config().logicalShards()) != shard) {
                    throw new IOException("Normal entry input differs from its driver or Bookie admission");
                }
                SegmentBatchFormat.validateEntry(data, SegmentBatchFormat.maxPayload(files.config()));
                SegmentEntryIdentity identity = SegmentEntryIdentity.parse(data.context(), data.entry().nioBuffer());
                entry = new Entry(input, admission, null, request, null, arrived, data, identity);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    admission.driverTerminal();
                } finally {
                    input.releaseFromDriver();
                }
                throw failure;
            }
            return submitEntry(entry);
        }

        /** Consume the exact admitted wire snapshot into the existing group-wide source budget. */
        boolean submitRecovery(SegmentBookieControlLog.RecoveryAdmission admission, RecoveryRequest request)
                throws IOException {
            Objects.requireNonNull(request, "request");
            long arrived = System.nanoTime();
            bookie.claimRecoveryForDriver(List.of(admission));
            SegmentEntryInputs.Input input = null;
            boolean claimed = false;
            Entry entry;
            try {
                byte[] payload = admission.takeEntryPayloadForDriver();
                if (payload.length < SegmentBatchFormat.ENTRY_OVERHEAD
                        || payload.length > SegmentBatchFormat.ENTRY_OVERHEAD
                                + SegmentBatchFormat.maxPayload(files.config())) {
                    throw new IOException("Recovery input exceeds the unified entry bound");
                }
                input = owner.inputs().adoptRecovery(admission.context(), payload);
                if (input == null) {
                    admission.driverTerminal();
                    return false;
                }
                owner.inputs().claim(List.of(input));
                claimed = true;
                entry = recoveryEntry(input, admission, request, arrived);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    if (input != null) {
                        if (claimed) {
                            input.releaseFromDriver();
                        } else {
                            input.close();
                        }
                    }
                } finally {
                    admission.driverTerminal();
                }
                throw failure;
            }
            return submitEntry(entry);
        }

        /** Source bytes were charged before copying; both source and exact grant transfer to the actual owner. */
        boolean submitRecovery(SegmentEntryInputs.Input input, SegmentBookieControlLog.RecoveryAdmission admission,
                               RecoveryRequest request) throws IOException {
            Objects.requireNonNull(request, "request");
            long arrived = System.nanoTime();
            owner.inputs().claim(List.of(input));
            try {
                bookie.claimRecoveryForDriver(List.of(admission));
            } catch (IOException | RuntimeException | Error failure) {
                input.releaseFromDriver();
                throw failure;
            }
            Entry entry;
            try {
                entry = recoveryEntry(input, admission, request, arrived);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    admission.driverTerminal();
                } finally {
                    input.releaseFromDriver();
                }
                throw failure;
            }
            return submitEntry(entry);
        }

        private Entry recoveryEntry(SegmentEntryInputs.Input input, SegmentBookieControlLog.RecoveryAdmission admission,
                                    RecoveryRequest request, long arrived) throws IOException {
            SegmentBatchFormat.Data data = input.data();
            if (!bookie.matchesScope(files) || !data.context().equals(admission.context())
                    || SegmentOwnerLoop.shardFor(data.context().ledgerId(), files.config().logicalShards()) != shard) {
                throw new IOException("Recovery entry input differs from its driver or Bookie admission");
            }
            SegmentBatchFormat.validateEntry(data, SegmentBatchFormat.maxPayload(files.config()));
            SegmentEntryIdentity identity = SegmentEntryIdentity.parse(data.context(), data.entry().nioBuffer());
            if (identity.entryId() != admission.entryId()) {
                throw new IOException("Recovery BK entry ID differs from its admission");
            }
            return new Entry(input, null, admission, null, request, arrived, data, identity);
        }

        private boolean submitEntry(Entry entry) {
            boolean accepted;
            try {
                if (allocator != null) {
                    entry.failureLease = allocator.reserveFailure(shard % files.config().dataFiles());
                    if (entry.failureLease == null) {
                        entry.abort();
                        return false;
                    }
                }
                accepted = owner.submit(shard, entry);
            } catch (RuntimeException | Error failure) {
                entry.abort();
                throw failure;
            }
            if (!accepted) {
                entry.abort();
            }
            return accepted;
        }

        private final class Entry implements SegmentOwnerLoop.Command {
            private SegmentEntryInputs.Input input;
            private final SegmentBookieControlLog.Admission admission;
            private final SegmentBookieControlLog.RecoveryAdmission recoveryAdmission;
            private final NormalRequest request;
            private final RecoveryRequest recoveryRequest;
            private final long arrived;
            private final ProfileLedgerContext context;
            private final long entryId;
            private final List<Entry> duplicates = new ArrayList<>();
            private SegmentBatchFormat.Data data;
            private SegmentEntryIdentity identity;
            private ByteBuffer duplicateView;
            private SegmentOwnerLoop.Ticket ticket;
            private PhysicalCommand physical;
            private Obligation duplicate;
            private final AtomicBoolean duplicateStarted = new AtomicBoolean();
            private boolean sourceReleased;
            private boolean settled;
            private SegmentArenaControlDriver.FailureLease failureLease;
            private IsolatedSegmentFiles.DataReadLease lookupPin;

            private Entry(SegmentEntryInputs.Input input, SegmentBookieControlLog.Admission admission,
                          SegmentBookieControlLog.RecoveryAdmission recoveryAdmission, NormalRequest request,
                          RecoveryRequest recoveryRequest, long arrived, SegmentBatchFormat.Data data,
                          SegmentEntryIdentity identity) {
                this.input = input;
                this.admission = admission;
                this.recoveryAdmission = recoveryAdmission;
                this.request = request;
                this.recoveryRequest = recoveryRequest;
                this.arrived = arrived;
                this.data = data;
                this.identity = identity;
                context = admission != null ? admission.context() : recoveryAdmission.context();
                entryId = identity.entryId();
            }

            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                this.ticket = ticket;
                append(this);
            }

            @Override
            public void failed(Throwable failure) {
                owner.requireOwner();
                if (physical != null) {
                    physical.failed(failure);
                } else if (duplicate != null) {
                    duplicate.physicalTerminalIfNeeded();
                    deliverDuplicate(duplicate, failure);
                } else {
                    respond(NormalStatus.UNKNOWN, null, failure, true);
                }
            }

            private void releaseSource() {
                if (!sourceReleased) {
                    // These NIO identity/views retain the heap array independently of ByteBuf.refCnt().
                    identity = null;
                    duplicateView = null;
                    data = null;
                    if (lookupPin != null) {
                        lookupPin.close();
                        lookupPin = null;
                    }
                    input.releaseFromDriver();
                    input = null;
                    sourceReleased = true;
                }
            }

            private void abort() {
                try {
                    releaseSource();
                } finally {
                    finishAdmission();
                }
            }

            private void respond(NormalStatus status, SegmentPointRead.Location candidate, Throwable failure,
                                 boolean releaseAdmission) {
                if (settled) {
                    throw new IllegalStateException("Entry result already terminal");
                }
                settled = true;
                try {
                    if (request != null) {
                        request.complete(new NormalResult(context, entryId, status, candidate), failure);
                    } else {
                        RecoveryStatus recoveryStatus = switch (status) {
                            case LOCAL_DURABLE -> RecoveryStatus.LOCAL_DURABLE;
                            case STALE_ADMISSION -> RecoveryStatus.STALE_ADMISSION;
                            case TRANSIENT_UNAVAILABLE -> RecoveryStatus.TRANSIENT_UNAVAILABLE;
                            case CONFLICT -> RecoveryStatus.CONFLICT;
                            case UNKNOWN -> RecoveryStatus.UNKNOWN;
                        };
                        recoveryRequest.complete(new RecoveryResult(context, entryId, recoveryStatus, candidate),
                                failure);
                    }
                } finally {
                    try {
                        if (duplicate == null) {
                            releaseSource();
                        }
                    } finally {
                        if (releaseAdmission) {
                            finishAdmission();
                        }
                    }
                }
            }

            private void refuse(NormalStatus status, Throwable failure) {
                releaseSource();
                ticket.complete(() -> respond(status == NormalStatus.CONFLICT && !eligible()
                        ? NormalStatus.STALE_ADMISSION : status, null, failure, true));
            }

            private boolean eligible() {
                return admission != null ? bookie.canCompleteNormal(admission)
                        : bookie.canCompleteRecovery(recoveryAdmission);
            }

            private void finishAdmission() {
                try {
                    if (admission != null) {
                        admission.driverTerminal();
                    } else {
                        recoveryAdmission.driverTerminal();
                    }
                } finally {
                    releaseFailure();
                }
            }

            private void releaseFailure() {
                if (failureLease != null) {
                    failureLease.close();
                    failureLease = null;
                }
            }
        }

        private void append(Entry entry) {
            if (!authority.dataFileAvailable(shard % files.config().dataFiles())) {
                entry.refuse(NormalStatus.UNKNOWN, new IOException("DATA file has an unresolved failure"));
                return;
            }
            NormalKey key = new NormalKey(entry.context, entry.entryId);
            SegmentPointRead.Location location = hot.liveDurableCandidate(entry.context, entry.entryId).orElse(null);
            PendingNormal existing = pendingNormals.get(key);
            if (location != null || existing != null) {
                duplicate(entry, location, existing);
                return;
            }
            Entry assemblingEntry = assemblingKeys.get(key);
            if (assemblingEntry != null) {
                if (assemblingEntry.identity.sameLogicalPayload(entry.identity)) {
                    assemblingEntry.duplicates.add(entry);
                    entry.releaseFailure();
                    entry.releaseSource();
                } else {
                    entry.refuse(NormalStatus.CONFLICT, null);
                }
                return;
            }
            if (hot.occupied(entry.context, entry.entryId)) {
                try {
                    entry.lookupPin = files.pinDataRead();
                } catch (IOException failure) {
                    entry.refuse(NormalStatus.UNKNOWN, failure);
                    return;
                }
                hot.freshGet(entry.ticket, entry.context, entry.entryId, (indexed, failure) -> {
                    if (failure != null || owner.failure() != null) {
                        entry.refuse(NormalStatus.UNKNOWN, failure != null ? failure : owner.failure());
                    } else if (indexed == null) {
                        // Missing derived state cannot authorize overwriting an already accepted coordinate.
                        entry.refuse(NormalStatus.TRANSIENT_UNAVAILABLE, null);
                    } else {
                        duplicate(entry, indexed, null);
                    }
                });
                return;
            }
            if (!hot.canAcceptEntries(assembling.size() + 1)) {
                entry.refuse(NormalStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            if (awaitingPool) {
                entry.refuse(NormalStatus.TRANSIENT_UNAVAILABLE, null);
                return;
            }
            int bytes = SegmentBatchFormat.RECORD_HEADER + entry.data.entry().readableBytes();
            if (!assembling.isEmpty()
                    && (long) SegmentBatchFormat.HEADER + SegmentBatchFormat.FOOTER + recordBytes + bytes
                            > limits.bytes()) {
                seal();
                if (owner.failure() != null) {
                    entry.refuse(NormalStatus.UNKNOWN, owner.failure());
                    return;
                }
                if (awaitingPool) {
                    entry.refuse(NormalStatus.TRANSIENT_UNAVAILABLE, null);
                    return;
                }
            }
            assembling.add(entry);
            assemblingKeys.put(key, entry);
            recordBytes = Math.addExact(recordBytes, bytes);
            owner.deadline(shard, entry.arrived + limits.delay().toNanos(), this::seal);
            if (assembling.size() >= limits.entries()
                    || SegmentBatchFormat.HEADER + SegmentBatchFormat.FOOTER + recordBytes >= limits.bytes()) {
                seal();
            }
        }

        private void duplicate(Entry entry, SegmentPointRead.Location location, PendingNormal existing) {
            entry.releaseFailure();
            if (location == null && !existing.identity.sameLogicalPayload(entry.identity)) {
                entry.refuse(NormalStatus.CONFLICT, null);
                return;
            }
            AtomicReference<DuplicateStatus> status = new AtomicReference<>(DuplicateStatus.NOT_READY);
            AtomicReference<SegmentPointRead.Location> selected = new AtomicReference<>(location);
            entry.duplicate = new Obligation((unused, failure) -> {
                NormalStatus result = failure != null ? NormalStatus.UNKNOWN
                        : !authority.dataFileAvailable(shard % files.config().dataFiles()) ? NormalStatus.UNKNOWN
                        : !entry.eligible() ? NormalStatus.STALE_ADMISSION
                        : status.get() == DuplicateStatus.CONFLICT ? NormalStatus.CONFLICT
                        : status.get() == DuplicateStatus.SAME_DURABLE
                                && hot.isCurrentDurable(selected.get()) ? NormalStatus.LOCAL_DURABLE
                                : status.get() == DuplicateStatus.NOT_READY ? NormalStatus.TRANSIENT_UNAVAILABLE
                                : NormalStatus.UNKNOWN;
                entry.respond(result, result == NormalStatus.LOCAL_DURABLE ? selected.get() : null, failure, false);
            }, entry.admission == null ? List.of() : List.of(entry.admission),
                    entry.recoveryAdmission == null ? List.of() : List.of(entry.recoveryAdmission), List.of());
            entry.duplicate.releaseSource = entry::releaseSource;
            if (location == null) {
                existing.waiters.add(new WaitingDuplicate(entry.ticket, entry.duplicate, status, selected));
                entry.duplicate.physicalTerminal();
                entry.releaseSource();
                return;
            }
            try {
                entry.duplicate.readPin = entry.lookupPin != null ? entry.lookupPin : files.pinDataRead();
                entry.lookupPin = null;
                entry.duplicate.readBuffer = owner.pool().acquire(SegmentPointRead.scratchBytes(location));
            } catch (IOException failure) {
                entry.duplicate.physicalTerminal();
                entry.ticket.complete(() -> deliverDuplicate(entry.duplicate, failure));
                return;
            }
            if (entry.duplicate.readBuffer == null) {
                entry.duplicate.physicalTerminal();
                entry.ticket.complete(() -> deliverDuplicate(entry.duplicate, null));
                return;
            }
            entry.duplicateView = entry.data.entry().nioBuffer().asReadOnlyBuffer();
            io.submit(entry.ticket, () -> {
                entry.duplicateStarted.set(true);
                try {
                    boolean same = SegmentDuplicateCheck.sameLogicalPayloadOwned(files.config(), files.identity(),
                            authority, location, entry.context, entry.duplicateView,
                            entry.duplicate.readBuffer.bytes());
                    status.set(same ? DuplicateStatus.SAME_DURABLE : DuplicateStatus.CONFLICT);
                } finally {
                    entry.duplicate.physicalTerminal();
                }
            }, failure -> {
                if (!entry.duplicateStarted.get()) {
                    entry.duplicate.physicalTerminal();
                }
                deliverDuplicate(entry.duplicate, failure);
            });
        }

        private void seal() {
            seal(true);
        }

        private void seal(boolean mayRefill) {
            owner.requireOwner();
            if (assembling.isEmpty() || awaitingPool) {
                return;
            }
            List<Entry> entries = List.copyOf(assembling);
            int length = SegmentBatchFormat.alignedLength(recordBytes);
            owner.cancelDeadline(shard);
            if (owner.failure() != null) {
                clearAssembling();
                refuseBatch(entries, NormalStatus.UNKNOWN, owner.failure());
                return;
            }
            SegmentBatchFormat.FrozenBatch batch = null;
            boolean claimed = false;
            Entry leader = entries.get(0);
            try {
                SegmentBatchFormat.Address address = prefix.appendAddress(files, authority, length);
                if (address == null) {
                    if (mayRefill && allocator != null && prefix.poolMissing(files, authority, length)) {
                        awaitingPool = true;
                        if (allocator.requestPool(prefix.poolRequest(poolBlocks), (results, failure) ->
                                leader.ticket.resume(() -> {
                                    awaitingPool = false;
                                    if (failure != null) {
                                        List<Entry> waiting = List.copyOf(assembling);
                                        clearAssembling();
                                        refuseBatch(waiting, NormalStatus.UNKNOWN, failure);
                                    } else {
                                        seal(false);
                                    }
                                }))) {
                            return;
                        }
                        awaitingPool = false;
                    }
                    clearAssembling();
                    refuseBatch(entries, NormalStatus.TRANSIENT_UNAVAILABLE, null);
                    return;
                }
                clearAssembling();
                batch = encodeEntries(address, entries);
                for (Entry entry : entries) {
                    entry.releaseSource();
                }
                if (batch == null) {
                    refuseBatch(entries, NormalStatus.TRANSIENT_UNAVAILABLE, null);
                    return;
                }
                batch.claimForDriver();
                claimed = true;
                Obligation obligation = prepareBatch(entries, batch);
                leader.physical = new PhysicalCommand(batch, obligation);
                leader.physical.run(leader.ticket);
            } catch (Throwable failure) {
                awaitingPool = false;
                clearAssembling();
                if (leader.physical != null) {
                    leader.ticket.commandFailed(failure);
                } else {
                    if (batch != null) {
                        if (claimed) {
                            batch.releaseFromDriver();
                        } else {
                            batch.close();
                        }
                    }
                    refuseBatch(entries, NormalStatus.UNKNOWN, failure);
                }
            }
        }

        private void clearAssembling() {
            assembling.clear();
            assemblingKeys.clear();
            recordBytes = 0;
        }

        private void refuseBatch(List<Entry> entries, NormalStatus status, Throwable failure) {
            for (Entry entry : entries) {
                entry.refuse(status, failure);
                for (Entry joined : entry.duplicates) {
                    joined.refuse(status, failure);
                }
                entry.duplicates.clear();
            }
        }

        private SegmentBatchFormat.FrozenBatch encodeEntries(SegmentBatchFormat.Address address, List<Entry> entries) {
            List<SegmentBatchFormat.Data> sources = new ArrayList<>(entries.size());
            for (Entry entry : entries) {
                sources.add(entry.data);
            }
            return SegmentBatchFormat.encodePooled(files.config(), address, sources, owner.pool());
        }

        private Obligation prepareBatch(List<Entry> entries, SegmentBatchFormat.FrozenBatch batch) throws IOException {
            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(batch.bytes(), batch.address(), Long.MAX_VALUE);
            if (!bookie.matchesScope(files) || parsed.records().size() != entries.size()) {
                throw new IOException("Batched DATA differs from its Bookie input");
            }
            List<PendingNormal> identities = new ArrayList<>(entries.size());
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                SegmentBatchParser.Record record = parsed.records().get(i);
                if (record.origin() != SegmentBatchFormat.ENTRY_DATA || !record.context().equals(entry.context)
                        || record.entryId() != entry.entryId || batch.address().shard() != shard) {
                    throw new IOException("Batched DATA record differs from its admitted input");
                }
                identities.add(new PendingNormal(record));
            }
            List<PendingNormal> normals = List.copyOf(identities);
            Completion completion = (candidates, failure) -> {
                if (failure == null && (candidates == null || candidates.size() != entries.size())) {
                    finishPendingNormals(normals, null, new IOException("Batched DATA candidate count mismatch"));
                    throw new IllegalStateException("Batched DATA lacks one published candidate per entry");
                }
                finishPendingNormals(normals, candidates, failure);
                completeBatch(entries, candidates, failure);
            };
            List<SegmentBookieControlLog.Admission> admissions = new ArrayList<>(entries.size());
            List<SegmentBookieControlLog.RecoveryAdmission> recovery = new ArrayList<>(entries.size());
            List<SegmentArenaControlDriver.FailureLease> failures = new ArrayList<>(entries.size());
            for (Entry entry : entries) {
                if (entry.admission != null) {
                    admissions.add(entry.admission);
                }
                if (entry.recoveryAdmission != null) {
                    recovery.add(entry.recoveryAdmission);
                }
                if (entry.failureLease != null) {
                    failures.add(entry.failureLease);
                }
            }
            Obligation obligation = new Obligation(completion, admissions, recovery, normals);
            obligation.failureLeases = List.copyOf(failures);
            for (Entry entry : entries) {
                entry.failureLease = null;
            }
            return obligation;
        }

        private void completeBatch(List<Entry> entries, List<SegmentPointRead.Location> candidates, Throwable failure) {
            Throwable callbackFailure = null;
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                SegmentPointRead.Location candidate = failure == null ? candidates.get(i) : null;
                NormalStatus result = failure != null ? NormalStatus.UNKNOWN
                        : entry.eligible() ? NormalStatus.LOCAL_DURABLE : NormalStatus.STALE_ADMISSION;
                try {
                    entry.respond(result, candidate, failure, false);
                } catch (Throwable error) {
                    callbackFailure = appendFailure(callbackFailure, error);
                }
                if (i != 0) {
                    entry.ticket.complete(() -> { });
                }
                for (Entry joined : entry.duplicates) {
                    try {
                        NormalStatus status = failure != null || candidate == null ? NormalStatus.UNKNOWN
                                : joined.eligible() ? NormalStatus.LOCAL_DURABLE
                                        : NormalStatus.STALE_ADMISSION;
                        joined.respond(status, candidate, failure, true);
                    } catch (Throwable error) {
                        callbackFailure = appendFailure(callbackFailure, error);
                    } finally {
                        joined.ticket.complete(() -> { });
                    }
                }
                entry.duplicates.clear();
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Batched entry callback failed", callbackFailure);
            }
        }

        private Throwable appendFailure(Throwable previous, Throwable next) {
            if (previous == null) {
                return next;
            }
            if (previous != next) {
                previous.addSuppressed(next);
            }
            return previous;
        }
    }

    /**
     * Join a matching pending normal write or check a published one. No candidate is not proof of absence.
     * The caller first acquires one Bookie admission; this method owns it after a successful claim.
     */
    boolean checkPublishedNormalDuplicate(SegmentBookieControlLog bookie,
                                          SegmentBookieControlLog.Admission admission,
                                          ByteBuffer incomingEntry, DuplicateRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        return checkPublishedNormalDuplicate(bookie, admission, incomingEntry, request::complete);
    }

    boolean checkPublishedNormalDuplicate(SegmentBookieControlLog bookie,
                                          SegmentBookieControlLog.Admission admission,
                                          ByteBuffer incomingEntry, DuplicateCompletion completion)
            throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(incomingEntry, "incomingEntry");
        Objects.requireNonNull(completion, "completion");
        bookie.claimForDriver(List.of(admission));
        ProfileLedgerContext context = admission.context();
        SegmentEntryIdentity identity;
        ByteBuffer copy;
        try {
            if (!bookie.matchesScope(files)) {
                throw new IOException("Bookie control log and DATA files have different storage scopes");
            }
            int length = incomingEntry.remaining();
            if (length < SegmentBatchFormat.ENTRY_OVERHEAD
                    || length > 1024 * 1024 + SegmentBatchFormat.ENTRY_OVERHEAD) {
                throw new IOException("Invalid duplicate BK entry length");
            }
            copy = ByteBuffer.allocate(length);
            copy.put(incomingEntry.slice()).flip();
            identity = SegmentEntryIdentity.parse(context, copy);
        } catch (IOException | RuntimeException | Error failure) {
            admission.driverTerminal();
            throw failure;
        }
        return submitDuplicate(context, identity, copy, List.of(admission), List.of(),
                () -> bookie.canCompleteNormal(admission), completion);
    }

    boolean checkPublishedRecoveryDuplicate(SegmentBookieControlLog bookie,
                                            SegmentBookieControlLog.RecoveryAdmission admission,
                                            DuplicateRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        return checkPublishedRecoveryDuplicate(bookie, admission, request::complete);
    }

    boolean checkPublishedRecoveryDuplicate(SegmentBookieControlLog bookie,
                                            SegmentBookieControlLog.RecoveryAdmission admission,
                                            DuplicateCompletion completion) throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(completion, "completion");
        bookie.claimRecoveryForDriver(List.of(admission));
        ProfileLedgerContext context = admission.context();
        SegmentEntryIdentity identity;
        ByteBuffer copy;
        try {
            if (!bookie.matchesScope(files)) {
                throw new IOException("Bookie control log and DATA files have different storage scopes");
            }
            copy = ByteBuffer.wrap(admission.entryPayload());
            identity = SegmentEntryIdentity.parse(context, copy);
            if (identity.entryId() != admission.entryId()) {
                throw new IOException("Recovery duplicate entry ID differs from its admission");
            }
        } catch (IOException | RuntimeException | Error failure) {
            admission.driverTerminal();
            throw failure;
        }
        return submitDuplicate(context, identity, copy, List.of(), List.of(admission),
                () -> bookie.canCompleteRecovery(admission), completion);
    }

    private boolean submitDuplicate(ProfileLedgerContext context, SegmentEntryIdentity identity, ByteBuffer copy,
                                    List<SegmentBookieControlLog.Admission> normalAdmissions,
                                    List<SegmentBookieControlLog.RecoveryAdmission> recoveryAdmissions,
                                    BooleanSupplier canComplete, DuplicateCompletion completion) {
        AtomicReference<DuplicateStatus> status = new AtomicReference<>(DuplicateStatus.NOT_READY);
        AtomicReference<SegmentPointRead.Location> selected = new AtomicReference<>();
        long entryId = identity.entryId();
        Obligation obligation = new Obligation((unused, failure) -> {
            DuplicateStatus result = failure == null ? status.get() : DuplicateStatus.UNKNOWN;
            if (result == DuplicateStatus.SAME_DURABLE || result == DuplicateStatus.CONFLICT) {
                if (!authority.dataFileAvailable(shard % files.config().dataFiles())) {
                    result = DuplicateStatus.NOT_READY;
                } else if (!canComplete.getAsBoolean()) {
                    result = DuplicateStatus.STALE_ADMISSION;
                } else if (selected.get() != null && !hot.isCurrentDurable(selected.get())) {
                    result = DuplicateStatus.NOT_READY;
                }
            }
            completion.complete(result, failure);
        }, normalAdmissions, recoveryAdmissions, List.of());
        boolean accepted;
        try {
            accepted = owner.submit(shard, new SegmentOwnerLoop.Command() {
                private final AtomicBoolean started = new AtomicBoolean();

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    SegmentPointRead.Location location = hot.liveDurableCandidate(context, entryId).orElse(null);
                    if (location == null) {
                        PendingNormal pendingNormal = pendingNormals.get(new NormalKey(context, entryId));
                        if (pendingNormal != null) {
                            if (pendingNormal.identity.sameLogicalPayload(identity)) {
                                pendingNormal.waiters.add(new WaitingDuplicate(ticket, obligation, status, selected));
                                obligation.physicalTerminal();
                                return;
                            }
                            status.set(DuplicateStatus.CONFLICT);
                            obligation.physicalTerminal();
                            ticket.complete(() -> deliverDuplicate(obligation, null));
                            return;
                        }
                        if (hot.occupied(context, entryId)) {
                            try {
                                obligation.readPin = files.pinDataRead();
                            } catch (IOException failure) {
                                obligation.physicalTerminal();
                                ticket.complete(() -> deliverDuplicate(obligation, failure));
                                return;
                            }
                            hot.freshGet(ticket, context, entryId, (indexed, failure) -> {
                                if (failure != null || indexed == null || owner.failure() != null) {
                                    obligation.physicalTerminal();
                                    ticket.complete(() -> deliverDuplicate(obligation,
                                            failure != null ? failure : owner.failure()));
                                } else {
                                    compare(ticket, indexed);
                                }
                            });
                            return;
                        }
                        obligation.physicalTerminal();
                        ticket.complete(() -> deliverDuplicate(obligation, null));
                        return;
                    }
                    compare(ticket, location);
                }

                private void compare(SegmentOwnerLoop.Ticket ticket, SegmentPointRead.Location location) {
                    selected.set(location);
                    try {
                        if (obligation.readPin == null) {
                            obligation.readPin = files.pinDataRead();
                        }
                        if (owner.pool() != null) {
                            obligation.readBuffer = owner.pool().acquire(SegmentPointRead.scratchBytes(location));
                            if (obligation.readBuffer == null) {
                                obligation.physicalTerminal();
                                ticket.complete(() -> deliverDuplicate(obligation, null));
                                return;
                            }
                        }
                    } catch (IOException failure) {
                        obligation.physicalTerminal();
                        ticket.complete(() -> deliverDuplicate(obligation, failure));
                        return;
                    }
                    io.submit(ticket, () -> {
                        started.set(true);
                        try {
                            boolean same = obligation.readBuffer == null
                                    ? SegmentDuplicateCheck.sameLogicalPayload(files.config(), files.identity(),
                                            authority, location, context, copy)
                                    : SegmentDuplicateCheck.sameLogicalPayloadOwned(files.config(), files.identity(),
                                            authority, location, context, copy, obligation.readBuffer.bytes());
                            status.set(same ? DuplicateStatus.SAME_DURABLE : DuplicateStatus.CONFLICT);
                        } finally {
                            obligation.physicalTerminal();
                        }
                    }, failure -> {
                        if (!started.get()) {
                            obligation.physicalTerminal();
                        }
                        deliverDuplicate(obligation, failure);
                    });
                }

                @Override
                public void failed(Throwable failure) {
                    owner.requireOwner();
                    obligation.physicalTerminalIfNeeded();
                    deliverDuplicate(obligation, failure);
                }
            });
        } catch (RuntimeException | Error failure) {
            obligation.abortBeforeOwner();
            throw failure;
        }
        if (!accepted) {
            obligation.abortBeforeOwner();
        }
        return accepted;
    }

    private boolean submitPhysical(SegmentBatchFormat.FrozenBatch batch, Obligation obligation) {
        Objects.requireNonNull(batch, "batch");
        obligation.batch = batch;
        boolean accepted;
        try {
            SegmentArenaControlDriver allocator = normalBatcher == null ? null : normalBatcher.allocator;
            if (allocator != null && obligation.failureLeases.isEmpty()) {
                SegmentArenaControlDriver.FailureLease lease = allocator.reserveFailure(batch.address().fileIndex());
                if (lease == null) {
                    obligation.abortBeforeOwner();
                    return false;
                }
                obligation.failureLeases = List.of(lease);
            }
            accepted = owner.submit(shard, new PhysicalCommand(batch, obligation));
        } catch (RuntimeException | Error failure) {
            obligation.abortBeforeOwner();
            throw failure;
        }
        if (!accepted) {
            obligation.abortBeforeOwner();
        }
        return accepted;
    }

    private final class PhysicalCommand implements SegmentOwnerLoop.Command {
        private final SegmentBatchFormat.FrozenBatch batch;
        private final Obligation obligation;

        private PhysicalCommand(SegmentBatchFormat.FrozenBatch batch, Obligation obligation) {
            this.batch = batch;
            this.obligation = obligation;
            obligation.batch = batch;
        }

        private boolean registered;
        private boolean submitted;
        private IsolatedSegmentFiles.Reservation reserved;

        @Override
        public void run(SegmentOwnerLoop.Ticket ticket) {
            SegmentHotDirectory.Admission admission = null;
            try {
                SegmentArenaControlDriver allocator = normalBatcher == null ? null : normalBatcher.allocator;
                if (allocator != null && obligation.failureLeases.isEmpty()) {
                    SegmentArenaControlDriver.FailureLease lease =
                            allocator.reserveFailure(batch.address().fileIndex());
                    if (lease == null) {
                        throw new IOException("No reserved DATA failure notification capacity");
                    }
                    obligation.failureLeases = List.of(lease);
                }
                admission = hot.reserve(batch);
                registerPendingNormals(obligation);
            } catch (IOException failure) {
                if (admission != null) {
                    hot.cancelBeforeSubmission(admission);
                }
                obligation.physicalTerminal();
                ticket.complete(() -> deliver(obligation, null, failure, null));
                return;
            }
            IsolatedSegmentFiles.Reservation reservation;
            try {
                reservation = prefix.reserve(batch, files, authority);
            } catch (IOException failure) {
                hot.cancelBeforeSubmission(admission);
                obligation.physicalTerminal();
                ticket.complete(() -> deliver(obligation, null, failure, null));
                return;
            }
            reserved = reservation;
            pending.put(reservation, obligation);
            registered = true;
            hot.bind(admission, reservation);
            AtomicBoolean operationStarted = new AtomicBoolean();
            io.submit(ticket, () -> {
                operationStarted.set(true);
                try {
                    files.writeReserved(reservation);
                } catch (IOException | RuntimeException | Error failure) {
                    recordPhysicalFailure(obligation, batch.address().fileIndex(), failure);
                    throw failure;
                } finally {
                    obligation.physicalTerminal();
                }
            }, failure -> {
                if (failure != null) {
                    recordPhysicalFailure(obligation, batch.address().fileIndex(), failure);
                }
                if (!operationStarted.get()) {
                    files.cancelBeforeSubmission(reservation);
                    obligation.physicalTerminal();
                }
                physicalFinished(reservation, failure);
            });
            submitted = true;
        }

        @Override
        public void failed(Throwable failure) {
            owner.requireOwner();
            if (!submitted) {
                if (reserved != null) {
                    files.cancelBeforeSubmission(reserved);
                }
                obligation.physicalTerminal();
            }
            if (reserved != null) {
                prefix.retireAfterOwnerFailure(reserved, hot);
            }
            Throwable callbackFailure = registered ? null : deliver(obligation, null, failure, null);
            try {
                failPending(failure);
            } catch (RuntimeException | Error pendingFailure) {
                if (callbackFailure != null) {
                    pendingFailure.addSuppressed(callbackFailure);
                }
                throw pendingFailure;
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Physical failure callback failed", callbackFailure);
            }
        }
    }

    private void registerPendingNormals(Obligation obligation) throws IOException {
        owner.requireOwner();
        for (PendingNormal normal : obligation.normals) {
            if (pendingNormals.containsKey(normal.key)) {
                throw new IOException("Normal coordinate already pending");
            }
        }
        for (PendingNormal normal : obligation.normals) {
            pendingNormals.put(normal.key, normal);
        }
    }

    private void finishPendingNormals(List<PendingNormal> normals, List<SegmentPointRead.Location> candidates,
                                      Throwable failure) {
        owner.requireOwner();
        for (int i = 0; i < normals.size(); i++) {
            PendingNormal normal = normals.get(i);
            if (pendingNormals.remove(normal.key, normal)) {
                for (WaitingDuplicate waiter : normal.waiters) {
                    if (failure == null) {
                        waiter.status.set(DuplicateStatus.SAME_DURABLE);
                        waiter.selected.set(candidates.get(i));
                    } else {
                        waiter.status.set(DuplicateStatus.UNKNOWN);
                    }
                    waiter.ticket.complete(() -> deliverDuplicate(waiter.obligation, failure));
                }
                normal.waiters.clear();
                normal.waiters = List.of();
            }
        }
    }

    private void physicalFinished(IsolatedSegmentFiles.Reservation reservation, Throwable ioFailure) {
        owner.requireOwner();
        int file = reservation.address().fileIndex();
        if (ioFailure == null && !authority.dataFileAvailable(file)) {
            ioFailure = new IOException("DATA file has an unresolved failure");
        }
        if (owner.failure() != null) {
            prefix.retireAfterOwnerFailure(reservation, hot);
            failPending(owner.failure());
            return;
        }
        try {
            List<SegmentPhysicalPrefix.DurableBatch> ready = prefix.completed(reservation, ioFailure, hot);
            if (prefix.failed()) {
                failPending(new IOException("Physical stream stopped before contiguous publication", ioFailure));
                return;
            }
            List<Delivery> deliveries = new ArrayList<>(ready.size());
            for (SegmentPhysicalPrefix.DurableBatch batch : ready) {
                Obligation obligation = pending.get(batch.reservation());
                if (obligation == null) {
                    throw new IllegalStateException("Missing physical completion obligation");
                }
                List<SegmentPointRead.Location> locations = hot.publish(batch);
                deliveries.add(new Delivery(batch.reservation(), obligation, locations));
            }
            Throwable callbackFailure = null;
            for (Delivery delivery : deliveries) {
                pending.remove(delivery.reservation());
                callbackFailure = deliver(delivery.obligation(), delivery.locations(), null, callbackFailure);
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Physical completion callback failed", callbackFailure);
            }
        } catch (RuntimeException | Error failure) {
            try {
                failPending(failure);
            } catch (RuntimeException | Error callbackFailure) {
                failure.addSuppressed(callbackFailure);
            }
            throw failure;
        }
    }

    private record Delivery(IsolatedSegmentFiles.Reservation reservation, Obligation obligation,
                            List<SegmentPointRead.Location> locations) { }

    private void recordPhysicalFailure(Obligation obligation, int file, Throwable failure) {
        if (!obligation.failureLeases.isEmpty()) {
            for (SegmentArenaControlDriver.FailureLease lease : obligation.failureLeases) {
                lease.failed(failure);
            }
            return;
        }
        if (!authority.blockDataFile(file)) {
            return;
        }
        SegmentArenaControlDriver allocator = normalBatcher == null ? null : normalBatcher.allocator;
        if (allocator == null) {
            failure.addSuppressed(new IOException("No Arena control driver for durable DATA quarantine"));
            return;
        }
        try {
            if (!allocator.submitQuarantine(file, (results, cause) -> {
                if (cause != null && cause != failure) {
                    failure.addSuppressed(cause);
                }
            })) {
                failure.addSuppressed(new IOException("Arena control driver refused DATA quarantine"));
            }
        } catch (RuntimeException | Error cause) {
            if (cause != failure) {
                failure.addSuppressed(cause);
            }
        }
    }

    private void failPending(Throwable failure) {
        List<Obligation> callbacks = List.copyOf(pending.values());
        pending.clear();
        Throwable callbackFailure = null;
        for (Obligation obligation : callbacks) {
            callbackFailure = deliver(obligation, null, failure, callbackFailure);
        }
        if (callbackFailure != null) {
            throw new IllegalStateException("Physical failure callback failed", callbackFailure);
        }
    }

    private void deliverDuplicate(Obligation obligation, Throwable failure) {
        Throwable callbackFailure = deliver(obligation, null, failure, null);
        if (callbackFailure != null) {
            throw new IllegalStateException("Duplicate completion callback failed", callbackFailure);
        }
    }

    private Throwable deliver(Obligation obligation, List<SegmentPointRead.Location> candidates,
                               Throwable failure, Throwable previous) {
        if (failure == null && candidates != null) {
            for (SegmentPointRead.Location candidate : candidates) {
                if (!authority.dataFileAvailable(candidate.address().fileIndex())) {
                    candidates = null;
                    failure = new IOException("DATA file has an unresolved failure");
                    break;
                }
            }
        }
        Completion completion = obligation.takeCompletion();
        try {
            completion.complete(candidates, failure);
        } catch (Throwable callbackFailure) {
            if (previous == null) {
                return callbackFailure;
            }
            previous.addSuppressed(callbackFailure);
        } finally {
            obligation.ownerTerminal();
        }
        return previous;
    }
}

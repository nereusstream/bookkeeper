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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Real DATA I/O and isolated local results; no production Bookie ACK authority. */
final class SegmentDataPhysicalDriver {
    @FunctionalInterface
    interface Completion {
        void complete(List<SegmentPointRead.Location> candidates, Throwable failure);
    }

    enum NormalStatus {
        LOCAL_DURABLE, STALE_ADMISSION, TRANSIENT_UNAVAILABLE, CONFLICT, UNKNOWN
    }

    record NormalResult(ProfileLedgerContext context, long entryId, NormalStatus status,
                        SegmentPointRead.Location candidate) { }

    enum RecoveryStatus {
        LOCAL_DURABLE, STALE_ADMISSION, UNKNOWN
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
        private final List<WaitingDuplicate> waiters = new ArrayList<>();

        private PendingNormal(SegmentBatchParser.Record record) throws IOException {
            key = new NormalKey(record.context(), record.entryId());
            identity = SegmentEntryIdentity.parse(record.context(), record.entry());
        }
    }

    private record WaitingDuplicate(SegmentOwnerLoop.Ticket ticket, Obligation obligation,
                                    AtomicReference<DuplicateStatus> status,
                                    AtomicReference<SegmentPointRead.Location> selected) { }

    private static final class Obligation {
        private final Completion completion;
        private final List<SegmentBookieControlLog.Admission> admissions;
        private final List<SegmentBookieControlLog.RecoveryAdmission> recoveryAdmissions;
        private final List<PendingNormal> normals;
        private SegmentBatchFormat.FrozenBatch batch;
        private Runnable releaseSource;
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

        synchronized void physicalTerminal() {
            if (physicalTerminal) {
                throw new IllegalStateException("Physical DATA already terminal");
            }
            physicalTerminal = true;
            releaseIfTerminal();
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
                        if (releaseSource != null) {
                            releaseSource.run();
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
    }

    boolean submit(SegmentBatchFormat.FrozenBatch batch, Completion completion) {
        Objects.requireNonNull(completion, "completion");
        batch.claimForDriver();
        return submitPhysical(batch, new Obligation(completion, List.of()));
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
        private final List<SegmentEntryInputs.Input> sources;
        private final List<SegmentBatchFormat.Data> data;
        private final List<SegmentBookieControlLog.Admission> admissions;
        private final List<Long> entryIds;
        private final List<NormalRequest> requests;
        private boolean sourcesReleased;
        private SegmentBatchFormat.FrozenBatch batch;
        private PhysicalCommand physical;

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
                releaseInputs(sources);
                sourcesReleased = true;
            }
        }

        private void abortBeforeOwner() {
            try {
                releaseSources();
            } finally {
                finishAdmissions(admissions);
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
                    finishAdmissions(admissions);
                }
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

    /** One assembling batch and oldest deadline per fixed driver/shard; no per-ledger permanent buffers. */
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
                entry = new Entry(input, admission, request, arrived, data, identity);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    admission.driverTerminal();
                } finally {
                    input.releaseFromDriver();
                }
                throw failure;
            }
            boolean accepted;
            try {
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
            private final SegmentEntryInputs.Input input;
            private final SegmentBookieControlLog.Admission admission;
            private final NormalRequest request;
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
            private boolean duplicateSubmitted;
            private boolean sourceReleased;
            private boolean settled;

            private Entry(SegmentEntryInputs.Input input, SegmentBookieControlLog.Admission admission,
                          NormalRequest request, long arrived, SegmentBatchFormat.Data data,
                          SegmentEntryIdentity identity) {
                this.input = input;
                this.admission = admission;
                this.request = request;
                this.arrived = arrived;
                this.data = data;
                this.identity = identity;
                context = data.context();
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
                    if (!duplicateSubmitted && !duplicateStarted.get()) {
                        duplicate.physicalTerminal();
                    }
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
                    input.releaseFromDriver();
                    sourceReleased = true;
                }
            }

            private void abort() {
                try {
                    releaseSource();
                } finally {
                    admission.driverTerminal();
                }
            }

            private void respond(NormalStatus status, SegmentPointRead.Location candidate, Throwable failure,
                                 boolean releaseAdmission) {
                if (settled) {
                    throw new IllegalStateException("Normal entry result already terminal");
                }
                settled = true;
                try {
                    request.complete(new NormalResult(context, entryId, status, candidate), failure);
                } finally {
                    try {
                        if (duplicate == null) {
                            releaseSource();
                        }
                    } finally {
                        if (releaseAdmission) {
                            admission.driverTerminal();
                        }
                    }
                }
            }

            private void refuse(NormalStatus status, Throwable failure) {
                releaseSource();
                ticket.complete(() -> respond(status == NormalStatus.CONFLICT && !bookie.canCompleteNormal(admission)
                        ? NormalStatus.STALE_ADMISSION : status, null, failure, true));
            }
        }

        private void append(Entry entry) {
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
                    entry.releaseSource();
                } else {
                    entry.refuse(NormalStatus.CONFLICT, null);
                }
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
            if (location == null && !existing.identity.sameLogicalPayload(entry.identity)) {
                entry.refuse(NormalStatus.CONFLICT, null);
                return;
            }
            AtomicReference<DuplicateStatus> status = new AtomicReference<>(DuplicateStatus.NOT_READY);
            AtomicReference<SegmentPointRead.Location> selected = new AtomicReference<>(location);
            entry.duplicate = new Obligation((unused, failure) -> {
                NormalStatus result = failure != null ? NormalStatus.UNKNOWN
                        : !bookie.canCompleteNormal(entry.admission) ? NormalStatus.STALE_ADMISSION
                        : status.get() == DuplicateStatus.CONFLICT ? NormalStatus.CONFLICT
                        : status.get() == DuplicateStatus.SAME_DURABLE
                                && hot.liveDurableCandidate(entry.context, entry.entryId)
                                        .filter(selected.get()::equals).isPresent() ? NormalStatus.LOCAL_DURABLE
                                : NormalStatus.UNKNOWN;
                entry.respond(result, result == NormalStatus.LOCAL_DURABLE ? selected.get() : null, failure, false);
            }, List.of(entry.admission));
            entry.duplicate.releaseSource = entry::releaseSource;
            if (location == null) {
                existing.waiters.add(new WaitingDuplicate(entry.ticket, entry.duplicate, status, selected));
                entry.duplicate.physicalTerminal();
                entry.duplicateSubmitted = true;
                entry.releaseSource();
                return;
            }
            entry.duplicateView = entry.data.entry().nioBuffer().asReadOnlyBuffer();
            io.submit(entry.ticket, () -> {
                entry.duplicateStarted.set(true);
                try {
                    boolean same = SegmentDuplicateCheck.sameLogicalPayload(files.config(), files.identity(), authority,
                            location, entry.context, entry.duplicateView);
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
            entry.duplicateSubmitted = true;
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
                batch = SegmentBatchFormat.encodePooled(files.config(), address,
                        entries.stream().map(entry -> entry.data).toList(), owner.pool());
                for (Entry entry : entries) {
                    entry.releaseSource();
                }
                if (batch == null) {
                    refuseBatch(entries, NormalStatus.TRANSIENT_UNAVAILABLE, null);
                    return;
                }
                batch.claimForDriver();
                claimed = true;
                Obligation obligation = prepareNormal(bookie, batch,
                        entries.stream().map(entry -> entry.admission).toList(),
                        (results, failure) -> completeBatch(entries, results, failure));
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
            }
        }

        private void completeBatch(List<Entry> entries, List<NormalResult> results, Throwable failure) {
            Throwable callbackFailure = null;
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                NormalResult result = results.get(i);
                try {
                    entry.respond(result.status(), result.candidate(), failure, false);
                } catch (Throwable error) {
                    callbackFailure = appendFailure(callbackFailure, error);
                }
                if (i != 0) {
                    entry.ticket.complete(() -> { });
                }
                for (Entry joined : entry.duplicates) {
                    try {
                        NormalStatus status = failure != null || result.candidate() == null ? NormalStatus.UNKNOWN
                                : bookie.canCompleteNormal(joined.admission) ? NormalStatus.LOCAL_DURABLE
                                        : NormalStatus.STALE_ADMISSION;
                        joined.respond(status, result.candidate(), failure, true);
                    } catch (Throwable error) {
                        callbackFailure = appendFailure(callbackFailure, error);
                    } finally {
                        joined.ticket.complete(() -> { });
                    }
                }
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Batched normal callback failed", callbackFailure);
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
                if (!canComplete.getAsBoolean()) {
                    result = DuplicateStatus.STALE_ADMISSION;
                } else if (selected.get() != null
                        && !hot.liveDurableCandidate(context, entryId).filter(selected.get()::equals).isPresent()) {
                    result = DuplicateStatus.NOT_READY;
                }
            }
            completion.complete(result, failure);
        }, normalAdmissions, recoveryAdmissions, List.of());
        boolean accepted;
        try {
            accepted = owner.submit(shard, new SegmentOwnerLoop.Command() {
                private final AtomicBoolean started = new AtomicBoolean();
                private boolean submitted;

                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    SegmentPointRead.Location location = hot.liveDurableCandidate(context, entryId).orElse(null);
                    if (location == null) {
                        PendingNormal pendingNormal = pendingNormals.get(new NormalKey(context, entryId));
                        if (pendingNormal != null) {
                            if (pendingNormal.identity.sameLogicalPayload(identity)) {
                                pendingNormal.waiters.add(new WaitingDuplicate(ticket, obligation, status, selected));
                                obligation.physicalTerminal();
                                submitted = true;
                                return;
                            }
                            status.set(DuplicateStatus.CONFLICT);
                        }
                        obligation.physicalTerminal();
                        ticket.complete(() -> deliverDuplicate(obligation, null));
                        return;
                    }
                    selected.set(location);
                    io.submit(ticket, () -> {
                        started.set(true);
                        try {
                            boolean same = SegmentDuplicateCheck.sameLogicalPayload(files.config(),
                                    files.identity(), authority, location, context, copy);
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
                    submitted = true;
                }

                @Override
                public void failed(Throwable failure) {
                    owner.requireOwner();
                    if (!submitted && !started.get()) {
                        obligation.physicalTerminal();
                    }
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
                } finally {
                    obligation.physicalTerminal();
                }
            }, failure -> {
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
            }
        }
    }

    private void physicalFinished(IsolatedSegmentFiles.Reservation reservation, Throwable ioFailure) {
        owner.requireOwner();
        if (owner.failure() != null) {
            failPending(owner.failure());
            return;
        }
        try {
            List<SegmentPhysicalPrefix.DurableBatch> ready = prefix.completed(reservation, ioFailure);
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

    private static void deliverDuplicate(Obligation obligation, Throwable failure) {
        Throwable callbackFailure = deliver(obligation, null, failure, null);
        if (callbackFailure != null) {
            throw new IllegalStateException("Duplicate completion callback failed", callbackFailure);
        }
    }

    private static Throwable deliver(Obligation obligation, List<SegmentPointRead.Location> candidates,
                                     Throwable failure, Throwable previous) {
        try {
            obligation.completion.complete(candidates, failure);
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

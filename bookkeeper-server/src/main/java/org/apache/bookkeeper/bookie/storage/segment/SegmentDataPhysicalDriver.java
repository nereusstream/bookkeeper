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
        LOCAL_DURABLE, STALE_ADMISSION, UNKNOWN
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
                for (SegmentBookieControlLog.Admission admission : admissions) {
                    admission.driverTerminal();
                }
                for (SegmentBookieControlLog.RecoveryAdmission admission : recoveryAdmissions) {
                    admission.driverTerminal();
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
        return submitPhysical(batch, new Obligation(Objects.requireNonNull(completion, "completion"), List.of()));
    }

    /**
     * The caller acquires one Bookie ticket per normal record before submitting the frozen batch.
     * Once claimed, this driver releases every ticket even when validation or owner admission fails.
     */
    boolean submitNormal(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                         List<SegmentBookieControlLog.Admission> admissions, NormalCompletion completion)
            throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        List<SegmentBookieControlLog.Admission> input = List.copyOf(admissions);
        bookie.claimForDriver(input);
        List<SegmentBatchParser.Record> records;
        List<PendingNormal> normals;
        try {
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
        } catch (IOException | RuntimeException | Error failure) {
            for (SegmentBookieControlLog.Admission admission : input) {
                admission.driverTerminal();
            }
            throw failure;
        }
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
        Obligation obligation = new Obligation(physical, input, normals);
        return submitPhysical(batch, obligation);
    }

    /** Recovery uses the same DATA barrier while each ticket binds its exact wire entry. */
    boolean submitRecovery(SegmentBookieControlLog bookie, SegmentBatchFormat.FrozenBatch batch,
                           List<SegmentBookieControlLog.RecoveryAdmission> admissions,
                           RecoveryCompletion completion) throws IOException {
        Objects.requireNonNull(bookie, "bookie");
        Objects.requireNonNull(batch, "batch");
        Objects.requireNonNull(completion, "completion");
        List<SegmentBookieControlLog.RecoveryAdmission> input = List.copyOf(admissions);
        bookie.claimRecoveryForDriver(input);
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
            for (SegmentBookieControlLog.RecoveryAdmission admission : input) {
                admission.driverTerminal();
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
        return submitNormal(bookie, batch, admissions, (results, failure) -> {
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
        });
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
        boolean accepted;
        try {
            accepted = owner.submit(shard, new SegmentOwnerLoop.Command() {
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

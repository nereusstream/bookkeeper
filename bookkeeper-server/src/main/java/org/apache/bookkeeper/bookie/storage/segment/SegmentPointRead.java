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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Isolated physical point read; the caller still owns locator publication and Bookie read authority. */
final class SegmentPointRead {
    record Location(SegmentBatchFormat.Address address, int batchLength, long recordOffset, int recordLength,
                    ProfileLedgerContext context, long entryId) { }

    record Result(SegmentBatchParser.Record record, long bytesRead) { }

    enum ReadStatus {
        FOUND, MISSING, NOT_READY, TRANSIENT_UNAVAILABLE, STALE_ADMISSION, UNKNOWN
    }

    @FunctionalInterface
    interface ReadCompletion {
        void complete(ReadStatus status, LeasedResult result, Throwable failure);
    }

    /** The consumer owns the successful result until close, including every view borrowed from it. */
    static final class LeasedResult implements AutoCloseable {
        private final Result result;
        private final SegmentBufferPool.Lease buffer;
        private final SegmentBookieControlLog.ExplicitLac explicit;
        private final AtomicBoolean closed = new AtomicBoolean();

        private LeasedResult(Result result, SegmentBufferPool.Lease buffer,
                SegmentBookieControlLog.ExplicitLac explicit) {
            this.result = result;
            this.buffer = buffer;
            this.explicit = explicit;
        }

        SegmentBookieControlLog.ExplicitLac explicitLac() {
            if (closed.get()) {
                throw new IllegalStateException("LAC result is closed");
            }
            return explicit;
        }

        Result result() {
            if (closed.get()) {
                throw new IllegalStateException("Point-read result is closed");
            }
            return result;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                if (buffer != null) {
                    buffer.close();
                }
            }
        }
    }

    /** Cancelling response delivery leaves the actual read, file pin and buffer obligation intact. */
    static final class ReadRequest {
        private final AtomicReference<ReadCompletion> response;
        private final Runnable actualTerminal;
        private final AtomicBoolean terminalNotified = new AtomicBoolean();

        ReadRequest(ReadCompletion completion) {
            this(completion, () -> { });
        }

        ReadRequest(ReadCompletion completion, Runnable actualTerminal) {
            response = new AtomicReference<>(Objects.requireNonNull(completion, "completion"));
            this.actualTerminal = Objects.requireNonNull(actualTerminal, "actualTerminal");
        }

        boolean stopWaiting() {
            return response.getAndSet(null) != null;
        }

        /** Actual admitted I/O and owner cleanup have ended; a delivered LeasedResult still belongs to its consumer. */
        private void actualTerminal() {
            if (terminalNotified.compareAndSet(false, true)) {
                actualTerminal.run();
            }
        }

        private void complete(ReadStatus status, LeasedResult result, Throwable failure) {
            ReadCompletion completion = response.getAndSet(null);
            if (completion == null) {
                if (result != null) {
                    result.close();
                }
                return;
            }
            try {
                completion.complete(status, result, failure);
            } catch (Throwable cause) {
                if (result != null) {
                    result.close();
                }
                throw cause;
            }
        }
    }

    /** Existing owner/I/O/pool service for live hot locations or the retained, validated restart inventory. */
    static final class Reader {
        private final SegmentOwnerLoop owner;
        private final SegmentIoExecutor io;
        private final IsolatedSegmentFiles files;
        private final SegmentArenaControlLog arena;
        private final SegmentBookieControlLog bookie;
        private final SegmentHotDirectory hot;

        static Reader live(SegmentOwnerLoop owner, SegmentIoExecutor io, IsolatedSegmentFiles files,
                           SegmentArenaControlLog arena, SegmentBookieControlLog bookie, SegmentHotDirectory hot)
                throws IOException {
            return new Reader(owner, io, files, arena, bookie, Objects.requireNonNull(hot, "hot"));
        }

        static Reader restarted(SegmentOwnerLoop owner, SegmentIoExecutor io, IsolatedSegmentFiles files,
                                SegmentArenaControlLog arena, SegmentBookieControlLog bookie) throws IOException {
            return new Reader(owner, io, files, arena, bookie, null);
        }

        private Reader(SegmentOwnerLoop owner, SegmentIoExecutor io, IsolatedSegmentFiles files,
                       SegmentArenaControlLog arena, SegmentBookieControlLog bookie, SegmentHotDirectory hot)
                throws IOException {
            owner.requireOwner();
            if (owner.pool() == null || !bookie.matchesScope(files)
                    || !arena.matchesScope(files.config(), files.identity())) {
                throw new IllegalArgumentException("Point-read service requires matching scope and an owner pool");
            }
            if (hot != null) {
                if (!files.supportsLocalResults() || !hot.matchesScope(files)) {
                    throw new IllegalArgumentException("Live read service requires the live DATA hot directory");
                }
                hot.candidateCount(); // Also verifies that selection belongs to this owner thread.
            } else {
                SegmentDataRecovery.PhysicalInventory inventory = files.startupInventory();
                inventory.requireCurrentArena(arena);
                for (Location location : inventory.entries()) {
                    if (scratchBytes(location) > owner.pool().bufferBytes()) {
                        throw new IllegalArgumentException("Owner pool cannot read retained startup entries");
                    }
                }
            }
            this.owner = owner;
            this.io = Objects.requireNonNull(io, "io");
            this.files = files;
            this.arena = arena;
            this.bookie = bookie;
            this.hot = hot;
        }

        boolean submit(ProfileLedgerContext context, long entryId, ReadRequest request) {
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(request, "request");
            if (entryId < 0) {
                throw new IllegalArgumentException("Point-read entry ID must be nonnegative");
            }
            int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), files.config().logicalShards());
            return owner.submit(shard, new ReadCommand(context, entryId, request, false, false));
        }

        /** Query the latest occupied source through the same physical validation and loan path. */
        boolean submitLatest(ProfileLedgerContext context, ReadRequest request) {
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(request, "request");
            return owner.submit(SegmentOwnerLoop.shardFor(context.ledgerId(), files.config().logicalShards()),
                    new ReadCommand(context, -1, request, true, false));
        }

        boolean submitLac(ProfileLedgerContext context, ReadRequest request) {
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(request, "request");
            return owner.submit(SegmentOwnerLoop.shardFor(context.ledgerId(), files.config().logicalShards()),
                    new ReadCommand(context, -1, request, true, true));
        }

        private final class ReadCommand implements SegmentOwnerLoop.Command {
            private final ProfileLedgerContext context;
            private long entryId;
            private final boolean latest;
            private final boolean explicitComponent;
            private final ReadRequest request;
            private final AtomicBoolean started = new AtomicBoolean();
            private SegmentBookieControlLog.ReadAdmission admission;
            private IsolatedSegmentFiles.DataReadLease pin;
            private SegmentBufferPool.Lease buffer;
            private Location selected;
            private Result result;
            private boolean freshMiss;
            private boolean physicalTerminal;
            private boolean ownerTerminal;

            private ReadCommand(ProfileLedgerContext context, long entryId, ReadRequest request, boolean latest,
                    boolean explicitComponent) {
                this.context = context;
                this.entryId = entryId;
                this.latest = latest;
                this.explicitComponent = explicitComponent;
                this.request = request;
            }

            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                try {
                    admission = bookie.admitRead(context).orElse(null);
                    if (admission == null) {
                        refuse(ticket, ReadStatus.NOT_READY, null);
                        return;
                    }
                    if (hot == null) {
                        IsolatedSegmentFiles.StartupInventoryLease startup = files.pinStartupInventory();
                        pin = startup;
                        if (latest) {
                            entryId = startup.inventory().latestCoordinate(arena, context);
                        }
                        if (startup.inventory().provesAbsent(arena, context, entryId)) {
                            freshMiss = true;
                        } else {
                            selected = startup.inventory().uniqueLocation(arena, context, entryId);
                        }
                    } else {
                        pin = files.pinDataRead();
                        if (latest) {
                            entryId = hot.latestCoordinate(context);
                            if (entryId < 0) {
                                freshMiss = true;
                                readSelected(ticket);
                                return;
                            }
                        }
                        selected = hot.liveDurableCandidate(context, entryId).orElse(null);
                        if (selected == null) {
                            hot.freshGet(ticket, context, entryId, (location, failure) -> {
                                selected = location;
                                freshMiss = failure == null && location == null;
                                if (failure != null) {
                                    refuse(ticket, ReadStatus.UNKNOWN, failure);
                                } else {
                                    readSelected(ticket);
                                }
                            });
                            return;
                        }
                    }
                } catch (IOException unavailable) {
                    refuse(ticket, ReadStatus.NOT_READY, unavailable);
                    return;
                }
                readSelected(ticket);
            }

            private void readSelected(SegmentOwnerLoop.Ticket ticket) {
                if (owner.failure() != null) {
                    refuse(ticket, ReadStatus.UNKNOWN, owner.failure());
                    return;
                }
                ReadStatus status;
                try {
                    if (selected != null) {
                        selected = arena.resolveLocation(selected);
                    }
                    status = prepareBuffer();
                } catch (IOException unavailable) {
                    refuse(ticket, ReadStatus.NOT_READY, unavailable);
                    return;
                }
                if (status != ReadStatus.FOUND) {
                    refuse(ticket, status, null);
                    return;
                }
                io.submit(ticket, () -> {
                    started.set(true);
                    try {
                        result = readWithScratch(files.config(), files.identity(), arena, selected, buffer.bytes());
                        arena.validateSelectedRecord(selected, result.record());
                    } finally {
                        physicalDone();
                    }
                }, failure -> {
                    if (!started.get()) {
                        physicalDone();
                    }
                    deliver(ReadStatus.FOUND, failure);
                });
            }

            private boolean canProveAbsence() {
                return explicitComponent ? bookie.canProveLacDataAbsence(admission, entryId)
                        : bookie.canProveReadAbsence(admission, entryId);
            }

            private ReadStatus prepareBuffer() throws IOException {
                if (selected == null) {
                    return freshMiss && canProveAbsence()
                            && (hot == null || hot.provesAbsent(context, entryId))
                            ? ReadStatus.MISSING : ReadStatus.NOT_READY;
                }
                if (!arena.dataFileAvailable(selected.address().fileIndex())) {
                    return ReadStatus.NOT_READY;
                }
                buffer = owner.pool().acquire(scratchBytes(selected));
                return buffer == null ? ReadStatus.TRANSIENT_UNAVAILABLE : ReadStatus.FOUND;
            }

            private void refuse(SegmentOwnerLoop.Ticket ticket, ReadStatus status, Throwable failure) {
                physicalDone();
                ticket.complete(() -> deliver(status, failure));
            }

            private void deliver(ReadStatus status, Throwable failure) {
                try {
                    if (failure == null && owner.failure() != null) {
                        status = ReadStatus.UNKNOWN;
                        failure = owner.failure();
                    }
                    if (status == ReadStatus.MISSING && failure == null
                            && (!canProveAbsence()
                                || hot != null && !hot.provesAbsent(context, entryId))) {
                        status = ReadStatus.NOT_READY;
                    }
                    if (status == ReadStatus.FOUND && failure == null) {
                        if (!arena.dataFileAvailable(selected.address().fileIndex())) {
                            status = ReadStatus.NOT_READY;
                        } else if (!bookie.canCompleteRead(admission)) {
                            status = ReadStatus.STALE_ADMISSION;
                        } else if (hot != null && !hot.isCurrentDurable(selected)) {
                            status = ReadStatus.NOT_READY;
                        } else if (pin instanceof IsolatedSegmentFiles.StartupInventoryLease startup) {
                            startup.inventory().requireCurrentArena(arena);
                        }
                    }
                    if (failure != null && status == ReadStatus.FOUND) {
                        status = ReadStatus.UNKNOWN;
                    }
                    LeasedResult response = null;
                    if (status == ReadStatus.FOUND || status == ReadStatus.MISSING && explicitComponent) {
                        response = new LeasedResult(result, buffer, explicitComponent
                                ? bookie.explicitLac(context).orElse(null) : null);
                        buffer = null;
                    }
                    request.complete(status, response, failure);
                } catch (IOException changed) {
                    request.complete(ReadStatus.NOT_READY, null, changed);
                } finally {
                    ownerDone();
                }
            }

            @Override
            public void failed(Throwable failure) {
                // Ticket failure dispatch waits for actual I/O termination, including a Get continuation.
                physicalDone();
                try {
                    request.complete(ReadStatus.UNKNOWN, null, failure);
                } finally {
                    ownerDone();
                }
            }

            private synchronized void physicalDone() {
                physicalTerminal = true;
                releaseIfTerminal();
            }

            private synchronized void ownerDone() {
                ownerTerminal = true;
                releaseIfTerminal();
            }

            private void releaseIfTerminal() {
                if (physicalTerminal && ownerTerminal) {
                    try {
                        if (buffer != null) {
                            buffer.close();
                            buffer = null;
                        }
                    } finally {
                        try {
                            if (pin != null) {
                                pin.close();
                                pin = null;
                            }
                        } finally {
                            try {
                                if (admission != null) {
                                    admission.finish();
                                    admission = null;
                                }
                            } finally {
                                request.actualTerminal();
                            }
                        }
                    }
                }
            }
        }
    }

    static List<Location> locations(SegmentBatchParser.Batch batch) {
        List<Location> locations = new ArrayList<>(batch.records().size());
        for (SegmentBatchParser.Record record : batch.records()) {
            locations.add(new Location(batch.address(), batch.length(), record.offset(),
                    SegmentBatchFormat.RECORD_HEADER + record.entry().remaining(), record.context(),
                    record.entryId()));
        }
        return List.copyOf(locations);
    }

    static Result read(IsolatedSegmentConfig config, SegmentFileHeader identity,
                       SegmentArenaControlLog authority, Location location) throws IOException {
        return read(config, identity, authority, location, null);
    }

    /** The returned record borrows scratch until the caller finishes using its lease. */
    static Result readWithScratch(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                  SegmentArenaControlLog authority, Location location, ByteBuffer scratch)
            throws IOException {
        if (scratch.capacity() < scratchBytes(location)) {
            throw new IOException("Point-read scratch capacity is insufficient");
        }
        return read(config, identity, authority, location, scratch);
    }

    static int scratchBytes(Location location) throws IOException {
        if (location.recordOffset() < 0 || location.recordLength() < 196
                || location.recordLength() > SegmentBatchFormat.RECORD_HEADER + 1024 * 1024 + 36
                || location.recordOffset() > Long.MAX_VALUE - location.recordLength()) {
            throw new IOException("Invalid point-read record span");
        }
        long bytes = alignUp(location.recordOffset() + location.recordLength()) - alignDown(location.recordOffset());
        if (bytes > SegmentBatchFormat.MAX_BATCH) {
            throw new IOException("Invalid aligned read span");
        }
        return Math.toIntExact(Math.max(4096, bytes));
    }

    private static Result read(IsolatedSegmentConfig config, SegmentFileHeader identity,
                               SegmentArenaControlLog authority, Location location, ByteBuffer scratch)
            throws IOException {
        SegmentBatchFormat.Address address = location.address();
        if (!authority.matchesScope(config, identity) || !authority.dataFileAvailable(address.fileIndex())
                || address.fileIndex() >= config.dataFiles()
                || address.shard() >= config.logicalShards()
                || address.fileIndex() != address.shard() % config.dataFiles()
                || location.context() == null || location.context().ledgerId() < 0
                || SegmentOwnerLoop.shardFor(location.context().ledgerId(), config.logicalShards()) != address.shard()
                || location.batchLength() < 4096 || location.batchLength() > SegmentBatchFormat.MAX_BATCH
                || location.batchLength() % 4096 != 0
                || address.offset() > config.bytesPerFile() - location.batchLength()
                || !authority.hasAppliedGrant(address.fileIndex(), address.offset(), location.batchLength(),
                        address.shard(), address.streamGeneration().value(), address.allocation(),
                        address.generation().value())) {
            throw new IOException("Point read has no current Arena grant or valid DATA range");
        }
        try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, address.fileIndex()),
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (data.size() != config.bytesPerFile()) {
                throw new IOException("DATA file size mismatch");
            }
            ByteBuffer fileHeader = readAligned(data, 0, 4096, scratch);
            identity.validate(fileHeader, config, address.fileIndex());
            ByteBuffer batchHeader = readAligned(data, address.offset(), address.offset() + 4096, scratch);
            int batchLength = SegmentBatchParser.batchLength(batchHeader, address, config.bytesPerFile());
            if (batchLength != location.batchLength()) {
                throw new IOException("Locator batch length mismatch");
            }
            long recordStart = location.recordOffset();
            long regionEnd = address.offset() + 128L + batchHeader.getInt(80);
            if (recordStart < address.offset() + 128 || location.recordLength() < 196
                    || location.recordLength() > SegmentBatchFormat.RECORD_HEADER + 1024 * 1024 + 36
                    || recordStart > regionEnd - location.recordLength()) {
                throw new IOException("Locator record lies outside the batch record region");
            }
            long headerStart = alignDown(recordStart);
            long headerEnd = alignUp(recordStart + SegmentBatchFormat.RECORD_HEADER);
            ByteBuffer recordHeader = readAligned(data, headerStart, headerEnd, scratch);
            int headerOffset = Math.toIntExact(recordStart - headerStart);
            int recordLength = SegmentBatchParser.recordLength(recordHeader.slice(headerOffset,
                    SegmentBatchFormat.RECORD_HEADER));
            if (recordLength != location.recordLength()) {
                throw new IOException("Locator record length mismatch");
            }
            long recordEnd = alignUp(recordStart + recordLength);
            int headerBytes = recordHeader.remaining();
            ByteBuffer content = readAligned(data, headerStart, recordEnd, scratch);
            SegmentBatchParser.Record record = SegmentBatchParser.parseRecord(
                    content.slice(headerOffset, recordLength), address, recordStart);
            if (!record.context().equals(location.context()) || record.entryId() != location.entryId()) {
                throw new IOException("Locator ledger identity or entry ID mismatch");
            }
            if (!authority.dataFileAvailable(address.fileIndex())) {
                throw new IOException("DATA file became unavailable before read completion");
            }
            return new Result(record, 4096L + 4096 + headerBytes + content.remaining());
        }
    }

    /** Isolated Bookie route gate around a physical candidate; caller still validates its current selector. */
    static Result readLocallyGated(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                   SegmentArenaControlLog arena, SegmentBookieControlLog bookie,
                                   Location location) throws IOException {
        if (!bookie.matchesScope(config, identity)) {
            throw new IOException("Bookie read route and DATA files have different storage scopes");
        }
        SegmentBookieControlLog.ReadAdmission admission = bookie.admitRead(location.context())
                .orElseThrow(() -> new IOException("Bookie read route is unavailable"));
        try {
            Location selected = arena.resolveLocation(location);
            Result result = read(config, identity, arena, selected);
            arena.validateSelectedRecord(selected, result.record());
            if (!bookie.canCompleteRead(admission)) {
                throw new IOException("Bookie read route changed before completion");
            }
            return result;
        } finally {
            admission.finish();
        }
    }

    /** Isolated read-only restart path for one completely recovered stream. */
    static Result readRecoveredSingleStreamLocallyGated(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentFileIo syncIo, ProfileLedgerContext context,
            long entryId, int maxBatches, int maxEntries) throws IOException {
        SegmentBookieControlLog.ReadAdmission admission = bookie.admitRead(context)
                .orElseThrow(() -> new IOException("Bookie read route is unavailable"));
        try {
            return SegmentDataRecovery.withLocallyReadableSingleStreamLocation(config, identity, arena, bookie,
                    syncIo, context, entryId, maxBatches, maxEntries, selected -> {
                        Result result = read(config, identity, arena, selected.location());
                        arena.validateSelectedRecord(selected.location(), result.record());
                        if (!bookie.canCompleteRead(admission)) {
                            throw new IOException("Bookie read route changed before completion");
                        }
                        int shard = selected.location().address().shard();
                        if (!arena.singleStreamForShard(shard).filter(selected.stream()::equals).isPresent()) {
                            throw new IOException("Recovered read stream changed before completion");
                        }
                        return result;
                    }).orElseThrow(() -> new IOException("Recovered read is not ready or is ambiguous"));
        } finally {
            admission.finish();
        }
    }

    /** Read a retained startup candidate while its reopened DATA writer keeps the recovery lock. */
    static Result readRestartedLocallyGated(
            IsolatedSegmentFiles files, SegmentArenaControlLog arena, SegmentBookieControlLog bookie,
            ProfileLedgerContext context, long entryId) throws IOException {
        try (IsolatedSegmentFiles.StartupInventoryLease lease = files.pinStartupInventory()) {
            IsolatedSegmentConfig config = files.config();
            SegmentFileHeader identity = files.identity();
            if (!bookie.matchesScope(files)) {
                throw new IOException("Bookie read route and DATA files have different storage scopes");
            }
            SegmentBookieControlLog.ReadAdmission admission = bookie.admitRead(context)
                    .orElseThrow(() -> new IOException("Bookie read route is unavailable"));
            try {
                Location selected = lease.inventory().uniqueLocation(arena, context, entryId);
                Result result = read(config, identity, arena, selected);
                arena.validateSelectedRecord(selected, result.record());
                lease.inventory().requireCurrentArena(arena);
                if (!bookie.canCompleteRead(admission)) {
                    throw new IOException("Recovered read authority changed before completion");
                }
                return result;
            } finally {
                admission.finish();
            }
        }
    }

    private static ByteBuffer readAligned(FileChannel data, long start, long end, ByteBuffer scratch)
            throws IOException {
        if (end < start || end - start > SegmentBatchFormat.MAX_BATCH) {
            throw new IOException("Invalid aligned read span");
        }
        int length = Math.toIntExact(end - start);
        if (scratch != null && scratch.capacity() < length) {
            throw new IOException("Point-read scratch capacity is insufficient");
        }
        ByteBuffer content = scratch == null ? ByteBuffer.allocate(length)
                : scratch.duplicate().clear().limit(length).slice().order(ByteOrder.BIG_ENDIAN);
        SegmentFileIo.readFully(data, content, start);
        return content;
    }

    private static long alignDown(long value) {
        return value & -4096L;
    }

    private static long alignUp(long value) throws IOException {
        if (value < 0 || value > Long.MAX_VALUE - 4095) {
            throw new IOException("Invalid aligned read end");
        }
        return (value + 4095) & -4096L;
    }

    private SegmentPointRead() {
    }
}

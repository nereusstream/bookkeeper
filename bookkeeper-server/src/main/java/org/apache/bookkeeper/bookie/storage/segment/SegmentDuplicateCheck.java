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
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Isolated blocking duplicate slow path; the caller must establish and recheck Bookie authority separately. */
final class SegmentDuplicateCheck {
    enum PhysicalComparison {
        MATCHED_CANDIDATE, CONFLICTING_CANDIDATE, STALE_ADMISSION, NOT_READY
    }

    /** A normal retry after restart gets physical evidence, never a local durable result. Consumes the ticket. */
    static PhysicalComparison inspectRecoveredNormalCandidate(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentBookieControlLog.Admission admission,
            ByteBuffer incomingEntry, SegmentFileIo syncIo, int maxBatches, int maxEntries) throws IOException {
        Objects.requireNonNull(admission, "admission");
        boolean claimed = false;
        try {
            bookie.claimForDriver(List.of(admission));
            claimed = true;
            ProfileLedgerContext context = admission.context();
            ByteBuffer incoming = copyEntry(incomingEntry);
            long entryId = SegmentEntryIdentity.parse(context, incoming).entryId();
            return inspectRecoveredCandidate(config, identity, arena, bookie, context, entryId, incoming,
                    () -> bookie.canCompleteNormal(admission), syncIo, maxBatches, maxEntries);
        } finally {
            if (claimed) {
                admission.driverTerminal();
            } else {
                admission.physicalTerminal();
            }
        }
    }

    /** Synced bytes only: old DATA I/O failures lack durable classification. Consumes the admission. */
    static PhysicalComparison inspectRecoveredRecoveryCandidate(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentBookieControlLog.RecoveryAdmission admission,
            SegmentFileIo syncIo, int maxBatches, int maxEntries) throws IOException {
        Objects.requireNonNull(admission, "admission");
        try {
            ProfileLedgerContext context = admission.context();
            ByteBuffer incoming = ByteBuffer.wrap(admission.entryPayload());
            SegmentEntryIdentity parsed = SegmentEntryIdentity.parse(context, incoming);
            if (parsed.entryId() != admission.entryId()) {
                throw new IOException("Recovery duplicate entry ID differs from its admission");
            }
            return inspectRecoveredCandidate(config, identity, arena, bookie, context, admission.entryId(), incoming,
                    () -> bookie.canCompleteRecovery(admission), syncIo, maxBatches, maxEntries);
        } finally {
            admission.finish();
        }
    }

    private static PhysicalComparison inspectRecoveredCandidate(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, ProfileLedgerContext context, long entryId, ByteBuffer incoming,
            BooleanSupplier canComplete, SegmentFileIo syncIo, int maxBatches, int maxEntries) throws IOException {
        return SegmentDataRecovery.withLocallyReadableSingleStreamLocation(config, identity, arena, bookie,
                syncIo, context, entryId, maxBatches, maxEntries, selected -> {
                    boolean same = sameLogicalPayload(config, identity, arena, selected.location(), context, incoming);
                    if (!canComplete.getAsBoolean()) {
                        return PhysicalComparison.STALE_ADMISSION;
                    }
                    int shard = selected.location().address().shard();
                    if (!arena.singleStreamForShard(shard).filter(selected.stream()::equals).isPresent()) {
                        return PhysicalComparison.NOT_READY;
                    }
                    return same ? PhysicalComparison.MATCHED_CANDIDATE : PhysicalComparison.CONFLICTING_CANDIDATE;
                }).orElse(PhysicalComparison.NOT_READY);
    }

    static boolean sameLogicalPayload(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                      SegmentArenaControlLog authority, SegmentPointRead.Location location,
                                      ProfileLedgerContext context, ByteBuffer incomingEntry) throws IOException {
        return compare(config, identity, authority, location, context, copyEntry(incomingEntry), null);
    }

    /** Both buffers remain caller-owned until comparison finishes; no additional payload copy or read allocation. */
    static boolean sameLogicalPayloadOwned(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                           SegmentArenaControlLog authority, SegmentPointRead.Location location,
                                           ProfileLedgerContext context, ByteBuffer incomingEntry, ByteBuffer scratch)
            throws IOException {
        return compare(config, identity, authority, location, context, incomingEntry,
                Objects.requireNonNull(scratch, "scratch"));
    }

    private static boolean compare(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                    SegmentArenaControlLog authority, SegmentPointRead.Location location,
                                    ProfileLedgerContext context, ByteBuffer incomingEntry, ByteBuffer scratch)
            throws IOException {
        location = authority.resolveLocation(location);
        if (!context.equals(location.context())) {
            throw new IOException("Duplicate check ledger context mismatch");
        }
        SegmentEntryIdentity incoming = SegmentEntryIdentity.parse(context, incomingEntry);
        if (incoming.entryId() != location.entryId()) {
            throw new IOException("Duplicate check entry coordinate mismatch");
        }
        SegmentBatchParser.Record stored = (scratch == null
                ? SegmentPointRead.read(config, identity, authority, location)
                : SegmentPointRead.readWithScratch(config, identity, authority, location, scratch)).record();
        authority.validateSelectedRecord(location, stored);
        return SegmentEntryIdentity.parse(stored.context(), stored.entry()).sameLogicalPayload(incoming);
    }

    private static ByteBuffer copyEntry(ByteBuffer entry) throws IOException {
        int length = Objects.requireNonNull(entry, "entry").remaining();
        if (length < SegmentBatchFormat.ENTRY_OVERHEAD
                || length > 1024 * 1024 + SegmentBatchFormat.ENTRY_OVERHEAD) {
            throw new IOException("Invalid duplicate BK entry length");
        }
        ByteBuffer copy = ByteBuffer.allocate(length);
        return copy.put(entry.slice()).flip();
    }

    private SegmentDuplicateCheck() {
    }
}

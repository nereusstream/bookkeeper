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
        if (!bookie.matchesScope(config, identity) || !arena.matchesScope(config, identity)) {
            throw new IOException("Recovered duplicate authority belongs to another storage scope");
        }
        int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
        SegmentArenaControlLog.SingleStream stream = arena.singleStreamForShard(shard).orElse(null);
        if (stream == null) {
            return PhysicalComparison.NOT_READY;
        }
        SegmentDataRecovery.PhysicalStreamCandidate recovered =
                SegmentDataRecovery.recoverLocallyReadableStreamCandidate(config, identity, arena,
                        bookie, syncIo, shard, stream.generation(), stream.firstSequence(), maxBatches, maxEntries);
        SegmentPointRead.Location selected = null;
        for (SegmentPointRead.Location location : recovered.entries()) {
            if (location.context().equals(context) && location.entryId() == entryId) {
                if (selected != null) {
                    return PhysicalComparison.NOT_READY;
                }
                selected = location;
            }
        }
        if (selected == null) {
            return PhysicalComparison.NOT_READY;
        }
        boolean same = sameLogicalPayload(config, identity, arena, selected, context, incoming);
        if (!canComplete.getAsBoolean()) {
            return PhysicalComparison.STALE_ADMISSION;
        }
        if (!arena.singleStreamForShard(shard).filter(stream::equals).isPresent()) {
            return PhysicalComparison.NOT_READY;
        }
        return same ? PhysicalComparison.MATCHED_CANDIDATE : PhysicalComparison.CONFLICTING_CANDIDATE;
    }

    static boolean sameLogicalPayload(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                      SegmentArenaControlLog authority, SegmentPointRead.Location location,
                                      ProfileLedgerContext context, ByteBuffer incomingEntry) throws IOException {
        if (!context.equals(location.context())) {
            throw new IOException("Duplicate check ledger context mismatch");
        }
        // The caller may release or reuse its request buffer while the blocking read is in flight.
        ByteBuffer copy = copyEntry(incomingEntry);
        SegmentEntryIdentity incoming = SegmentEntryIdentity.parse(context, copy);
        if (incoming.entryId() != location.entryId()) {
            throw new IOException("Duplicate check entry coordinate mismatch");
        }
        SegmentBatchParser.Record stored = SegmentPointRead.read(config, identity, authority, location).record();
        if (stored.origin() != SegmentBatchFormat.ENTRY_DATA) {
            throw new IOException("Duplicate check requires ordinary DATA");
        }
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

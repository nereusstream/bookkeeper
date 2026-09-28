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
import java.util.Objects;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Isolated blocking duplicate slow path; the caller must establish and recheck Bookie authority separately. */
final class SegmentDuplicateCheck {
    enum PhysicalComparison {
        MATCHED_CANDIDATE, CONFLICTING_CANDIDATE, STALE_ADMISSION, NOT_READY
    }

    /** Synced bytes only: old DATA I/O failures lack durable classification. Consumes the admission. */
    static PhysicalComparison inspectRecoveredRecoveryCandidate(
            IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentArenaControlLog arena,
            SegmentBookieControlLog bookie, SegmentBookieControlLog.RecoveryAdmission admission,
            SegmentFileIo syncIo, int maxBatches, int maxEntries) throws IOException {
        Objects.requireNonNull(admission, "admission");
        try {
            if (!bookie.matchesScope(config, identity) || !arena.matchesScope(config, identity)) {
                throw new IOException("Recovered duplicate authority belongs to another storage scope");
            }
            ProfileLedgerContext context = admission.context();
            ByteBuffer incoming = ByteBuffer.wrap(admission.entryPayload());
            SegmentEntryIdentity parsed = SegmentEntryIdentity.parse(context, incoming);
            if (parsed.entryId() != admission.entryId()) {
                throw new IOException("Recovery duplicate entry ID differs from its admission");
            }
            int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
            SegmentArenaControlLog.SingleStream stream = arena.singleStreamForShard(shard).orElse(null);
            if (stream == null) {
                return PhysicalComparison.NOT_READY;
            }
            SegmentDataRecovery.PhysicalStreamCandidate recovered =
                    SegmentDataRecovery.recoverLocallyReadableStreamCandidate(config, identity, arena,
                            bookie, syncIo, shard, stream.generation(), stream.firstSequence(),
                            maxBatches, maxEntries);
            SegmentPointRead.Location selected = null;
            for (SegmentPointRead.Location location : recovered.entries()) {
                if (location.context().equals(context) && location.entryId() == admission.entryId()) {
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
            if (!bookie.canCompleteRecovery(admission)) {
                return PhysicalComparison.STALE_ADMISSION;
            }
            if (!arena.singleStreamForShard(shard).filter(stream::equals).isPresent()) {
                return PhysicalComparison.NOT_READY;
            }
            return same ? PhysicalComparison.MATCHED_CANDIDATE : PhysicalComparison.CONFLICTING_CANDIDATE;
        } finally {
            admission.finish();
        }
    }

    static boolean sameLogicalPayload(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                      SegmentArenaControlLog authority, SegmentPointRead.Location location,
                                      ProfileLedgerContext context, ByteBuffer incomingEntry) throws IOException {
        if (!context.equals(location.context())) {
            throw new IOException("Duplicate check ledger context mismatch");
        }
        int length = incomingEntry.remaining();
        if (length < SegmentBatchFormat.ENTRY_OVERHEAD
                || length > 1024 * 1024 + SegmentBatchFormat.ENTRY_OVERHEAD) {
            throw new IOException("Invalid duplicate BK entry length");
        }
        // The caller may release or reuse its request buffer while the blocking read is in flight.
        ByteBuffer copy = ByteBuffer.allocate(length);
        copy.put(incomingEntry.slice()).flip();
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

    private SegmentDuplicateCheck() {
    }
}

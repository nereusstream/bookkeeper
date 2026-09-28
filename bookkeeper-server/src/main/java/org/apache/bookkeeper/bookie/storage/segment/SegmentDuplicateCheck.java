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
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Isolated blocking duplicate slow path; the caller must establish and recheck Bookie authority separately. */
final class SegmentDuplicateCheck {
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

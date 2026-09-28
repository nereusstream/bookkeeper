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
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Validated CRC32C BK entry identity; LAC and its digest are transport facts, not logical payload identity. */
final class SegmentEntryIdentity {
    private final ProfileLedgerContext context;
    private final long entryId;
    private final long lac;
    private final long ledgerLength;
    private final ByteBuffer payload;

    private SegmentEntryIdentity(ProfileLedgerContext context, long entryId, long lac,
                                 long ledgerLength, ByteBuffer payload) {
        this.context = context;
        this.entryId = entryId;
        this.lac = lac;
        this.ledgerLength = ledgerLength;
        this.payload = payload.asReadOnlyBuffer();
    }

    static SegmentEntryIdentity parse(ProfileLedgerContext context, ByteBuffer input) throws IOException {
        ByteBuffer entry = input.slice().order(ByteOrder.BIG_ENDIAN);
        if (entry.remaining() < SegmentBatchFormat.ENTRY_OVERHEAD
                || entry.remaining() > 1024 * 1024 + SegmentBatchFormat.ENTRY_OVERHEAD) {
            throw new IOException("Invalid BK entry length");
        }
        long entryId = entry.getLong(8);
        long lac = entry.getLong(16);
        long ledgerLength = entry.getLong(24);
        if (context.ledgerId() < 0 || entry.getLong(0) != context.ledgerId()
                || entryId < 0 || lac < -1 || ledgerLength < 0) {
            throw new IOException("Invalid BK entry metadata");
        }
        CRC32C checksum = new CRC32C();
        checksum.update(entry.slice(0, 32));
        checksum.update(entry.slice(36, entry.remaining() - 36));
        if ((int) checksum.getValue() != entry.getInt(32)) {
            throw new IOException("BK entry CRC32C mismatch");
        }
        return new SegmentEntryIdentity(context, entryId, lac, ledgerLength,
                entry.slice(SegmentBatchFormat.ENTRY_OVERHEAD, entry.remaining() - SegmentBatchFormat.ENTRY_OVERHEAD));
    }

    long entryId() {
        return entryId;
    }

    long lac() {
        return lac;
    }

    long ledgerLength() {
        return ledgerLength;
    }

    boolean sameLogicalPayload(SegmentEntryIdentity other) {
        if (!context.equals(other.context) || entryId != other.entryId) {
            throw new IllegalArgumentException("Different ledger instance or entry coordinate");
        }
        return ledgerLength == other.ledgerLength && payload.equals(other.payload);
    }

}

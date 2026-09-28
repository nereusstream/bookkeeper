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
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Isolated physical point read; the caller still owns locator publication and Bookie read authority. */
final class SegmentPointRead {
    record Location(SegmentBatchFormat.Address address, int batchLength, long recordOffset, int recordLength,
                    ProfileLedgerContext context, long entryId) { }

    record Result(SegmentBatchParser.Record record, long bytesRead) { }

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
        SegmentBatchFormat.Address address = location.address();
        if (!authority.matchesScope(config, identity) || address.fileIndex() >= config.dataFiles()
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
            ByteBuffer fileHeader = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(data, fileHeader, 0);
            identity.validate(fileHeader, config, address.fileIndex());
            ByteBuffer batchHeader = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN);
            SegmentFileIo.readFully(data, batchHeader, address.offset());
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
            ByteBuffer recordHeader = readAligned(data, headerStart, headerEnd);
            int headerOffset = Math.toIntExact(recordStart - headerStart);
            int recordLength = SegmentBatchParser.recordLength(recordHeader.slice(headerOffset,
                    SegmentBatchFormat.RECORD_HEADER));
            if (recordLength != location.recordLength()) {
                throw new IOException("Locator record length mismatch");
            }
            long recordEnd = alignUp(recordStart + recordLength);
            ByteBuffer content = readAligned(data, headerStart, recordEnd);
            SegmentBatchParser.Record record = SegmentBatchParser.parseRecord(
                    content.slice(headerOffset, recordLength), address, recordStart);
            if (!record.context().equals(location.context()) || record.entryId() != location.entryId()) {
                throw new IOException("Locator ledger identity or entry ID mismatch");
            }
            return new Result(record, 4096L + 4096 + recordHeader.remaining() + content.remaining());
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
            Result result = read(config, identity, arena, location);
            if (!bookie.canCompleteRead(admission)) {
                throw new IOException("Bookie read route changed before completion");
            }
            return result;
        } finally {
            admission.finish();
        }
    }

    /** Isolated read-only restart path while the Arena has no MOVE/FREE or multi-stream selector. */
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
    static Result readRestartedSingleStreamLocallyGated(
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
                int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
                SegmentArenaControlLog.SingleStream stream = arena.singleStreamForShard(shard)
                        .orElseThrow(() -> new IOException("Recovered read stream is not unique"));
                SegmentDataRecovery.PhysicalStreamCandidate candidate = lease.inventory()
                        .streamCandidate(arena, shard, stream.generation());
                Location selected = null;
                for (Location location : candidate.entries()) {
                    if (location.context().equals(context) && location.entryId() == entryId) {
                        if (selected != null) {
                            throw new IOException("Recovered read location is ambiguous");
                        }
                        selected = location;
                    }
                }
                if (selected == null) {
                    throw new IOException("Recovered read location is not ready");
                }
                Result result = read(config, identity, arena, selected);
                if (!bookie.canCompleteRead(admission)
                        || !arena.singleStreamForShard(shard).filter(stream::equals).isPresent()) {
                    throw new IOException("Recovered read authority changed before completion");
                }
                return result;
            } finally {
                admission.finish();
            }
        }
    }

    private static ByteBuffer readAligned(FileChannel data, long start, long end) throws IOException {
        if (end < start || end - start > SegmentBatchFormat.MAX_BATCH) {
            throw new IOException("Invalid aligned read span");
        }
        ByteBuffer content = ByteBuffer.allocate((int) (end - start));
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

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** One bounded same-Arena conditional MOVE, carrying verified bytes rather than borrowed DATA buffers. */
final class SegmentArenaMove implements SegmentArenaControlLog.Command, AutoCloseable {
    private static final int ENTRY_BYTES = 252;

    record Source(SegmentPointRead.Location location, long expectedSelector) {
        Source {
            Objects.requireNonNull(location, "location");
            if (expectedSelector < -1) {
                throw new IllegalArgumentException("Invalid MOVE predecessor selector");
            }
        }
    }

    record Entry(Source source, SegmentPointRead.Location target, long sourceOrder, long targetOrder,
                 String entrySha256) {
        Entry {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            if (sourceOrder < 0 || targetOrder < 0 || source.location().equals(target)
                    || !source.location().context().equals(target.context())
                    || source.location().entryId() != target.entryId()
                    || source.location().address().shard() != target.address().shard()
                    || entrySha256 == null || entrySha256.length() != 64
                    || HexFormat.of().parseHex(entrySha256).length != 32) {
                throw new IllegalArgumentException("Invalid MOVE identity or payload binding");
            }
        }
    }

    private record Key(ProfileLedgerContext context, long entryId) { }

    private final UUID operationId;
    private final Generation operationGeneration;
    private final long bookieCut;
    private final List<Entry> entries;
    private final IsolatedSegmentFiles.ReclamationGuard protection;

    private SegmentArenaMove(UUID operationId, Generation operationGeneration, long bookieCut,
                             List<Entry> entries, IsolatedSegmentFiles.ReclamationGuard protection) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.operationGeneration = Objects.requireNonNull(operationGeneration, "operationGeneration");
        this.bookieCut = bookieCut;
        this.entries = List.copyOf(entries);
        this.protection = protection;
        if (operationId.equals(new UUID(0, 0)) || bookieCut < -1 || entries.isEmpty() || entries.size() > 256) {
            throw new IllegalArgumentException("Invalid bounded MOVE command");
        }
        HashSet<Key> keys = new HashSet<>();
        for (Entry entry : entries) {
            if (!keys.add(new Key(entry.target().context(), entry.target().entryId()))) {
                throw new IllegalArgumentException("MOVE repeats a coordinate");
            }
        }
    }

    static SegmentArenaMove prepare(SegmentArenaControlLog arena, IsolatedSegmentFiles files,
                                     SegmentBookieControlLog bookie,
                                     SegmentDataPhysicalDriver.DurableRelocation copy,
                                     List<Source> sources, ByteBuffer workspace) throws IOException {
        if (!arena.matchesScope(copy.sourceConfig(), copy.sourceIdentity())
                || !files.matchesScope(copy.sourceConfig(), copy.sourceIdentity()) || !bookie.matchesScope(files)
                || sources.size() != copy.copies().size()) {
            throw new IOException("MOVE sources, copied DATA and authority have different scopes or counts");
        }
        arena.requireMovePreparation(files);
        for (Source source : sources) {
            if (source.expectedSelector() > arena.appliedThrough()) {
                throw new IOException("MOVE predecessor is not an earlier applied control fact");
            }
        }
        SegmentBookieControlLog.ReclamationLease lease = bookie.protectReclamation(files.config(), files.identity());
        IsolatedSegmentFiles.ReclamationGuard guard;
        try {
            guard = files.protectReclamation(arena, copy.copies().get(0).location().address().fileIndex(), lease);
        } catch (IOException | RuntimeException | Error failure) {
            lease.close();
            throw failure;
        }
        try {
            List<Entry> verified = new ArrayList<>(sources.size());
            for (int i = 0; i < sources.size(); i++) {
                Source source = sources.get(i);
                SegmentDataPhysicalDriver.RelocationCopy destination = copy.copies().get(i);
                SegmentPointRead.Location target = destination.location();
                if (!bookie.locallyReadableContext(source.location().context())) {
                    throw new IOException("MOVE source has no current locally readable Bookie context");
                }
                SegmentBatchParser.Record old = SegmentPointRead.readWithScratch(files.config(), files.identity(),
                        arena, source.location(), workspace).record();
                if (old.origin() != SegmentBatchFormat.ENTRY_DATA
                        && !arena.wasCommittedCopy(source.location(), old)) {
                    throw new IOException("MOVE predecessor is an uncommitted relocation copy");
                }
                if (!digest(old.entry()).equals(destination.entrySha256())) {
                    throw new IOException("MOVE copy differs from the predecessor entry bytes");
                }
                SegmentBatchParser.Record current = SegmentPointRead.readWithScratch(files.config(), files.identity(),
                        arena, target, workspace).record();
                if (current.origin() != SegmentBatchFormat.RELOCATION_COPY
                        || !copy.operationId().equals(current.moveId())
                        || copy.operationGeneration().value() != current.moveGeneration()
                        || !digest(current.entry()).equals(destination.entrySha256())) {
                    throw new IOException("MOVE destination differs from the actual durable copy");
                }
                verified.add(new Entry(source, target, arena.appliedAllocationOrder(source.location().address()),
                        arena.appliedAllocationOrder(target.address()), destination.entrySha256()));
            }
            arena.requireMovePreparation(files);
            if (lease.cut() != bookie.recoveryReadCut(files.config(), files.identity())) {
                throw new IOException("Bookie control authority changed during MOVE verification");
            }
            return new SegmentArenaMove(copy.operationId(), copy.operationGeneration(), lease.cut(), verified, guard);
        } catch (IOException | RuntimeException | Error failure) {
            guard.close();
            throw failure;
        }
    }

    static String digest(ByteBuffer bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes.duplicate());
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    @Override
    public SegmentControlFormat.Type type() {
        return SegmentControlFormat.Type.MOVE;
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public Generation operationGeneration() {
        return operationGeneration;
    }

    @Override
    public long expectedGeneration() {
        return 0; // Per-coordinate predecessor selector and physical generations are encoded in each row.
    }

    long bookieCut() {
        return bookieCut;
    }

    List<Entry> entries() {
        return entries;
    }

    SegmentArenaMove replayFacts() {
        return new SegmentArenaMove(operationId, operationGeneration, bookieCut, entries, null);
    }

    @Override
    public ByteBuffer body() {
        ByteBuffer body = ByteBuffer.allocate(16 + ENTRY_BYTES * entries.size()).putInt(1)
                .putInt(entries.size()).putLong(bookieCut);
        for (Entry entry : entries) {
            body.put(entry.target().context().encode()).putLong(entry.target().entryId());
            putGeometry(body, entry.source().location());
            putGeometry(body, entry.target());
            body.putLong(entry.source().expectedSelector()).putLong(entry.sourceOrder()).putLong(entry.targetOrder())
                    .put(HexFormat.of().parseHex(entry.entrySha256()));
        }
        return body.flip();
    }

    static SegmentArenaMove decode(UUID operation, Generation generation, ByteBuffer body, long sequence)
            throws IOException {
        if (body.remaining() < 16 || body.getInt() != 1) {
            throw new IOException("Unsupported MOVE body");
        }
        int count = body.getInt();
        long cut = body.getLong();
        if (count < 1 || count > 256 || body.remaining() != count * ENTRY_BYTES) {
            throw new IOException("Invalid bounded MOVE row count");
        }
        try {
            List<Entry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                byte[] context = new byte[ProfileLedgerContext.ENCODED_LENGTH];
                body.get(context);
                ProfileLedgerContext key = ProfileLedgerContext.decode(context);
                long entryId = body.getLong();
                SegmentPointRead.Location old = getGeometry(body, key, entryId);
                SegmentPointRead.Location target = getGeometry(body, key, entryId);
                long predecessor = body.getLong();
                long sourceOrder = body.getLong();
                long targetOrder = body.getLong();
                byte[] digest = new byte[32];
                body.get(digest);
                if (predecessor >= sequence || sourceOrder >= sequence || targetOrder >= sequence) {
                    throw new IllegalArgumentException("MOVE authority references are not earlier control facts");
                }
                entries.add(new Entry(new Source(old, predecessor), target, sourceOrder, targetOrder,
                        HexFormat.of().formatHex(digest)));
            }
            return new SegmentArenaMove(operation, generation, cut, entries, null);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid MOVE command", invalid);
        }
    }

    private static void putGeometry(ByteBuffer body, SegmentPointRead.Location location) {
        SegmentBatchFormat.Address address = location.address();
        body.putInt(address.fileIndex()).putInt(address.shard()).putLong(address.allocation())
                .putLong(address.generation().value()).putLong(address.streamGeneration().value())
                .putLong(address.sequence()).putLong(address.offset()).putInt(location.batchLength())
                .putLong(location.recordOffset()).putInt(location.recordLength());
    }

    private static SegmentPointRead.Location getGeometry(ByteBuffer body, ProfileLedgerContext context, long entryId) {
        SegmentBatchFormat.Address address = new SegmentBatchFormat.Address(body.getInt(), body.getInt(),
                body.getLong(), new Generation(body.getLong()), new Generation(body.getLong()),
                body.getLong(), body.getLong());
        return new SegmentPointRead.Location(address, body.getInt(), body.getLong(), body.getInt(), context, entryId);
    }

    boolean claim(SegmentArenaControlLog arena) throws IOException {
        if (arena.resolvedMove(this)) {
            return false;
        }
        if (protection == null) {
            throw new IOException("Decoded MOVE facts are not runtime verification");
        }
        protection.claim(arena);
        return true;
    }

    void requireClaimed(SegmentArenaControlLog arena) throws IOException {
        if (!arena.resolvedMove(this) && (protection == null || !protection.claimedBy(arena))) {
            throw new IOException("MOVE has no held runtime verification");
        }
    }

    void finish(SegmentArenaControlLog arena) {
        if (protection != null) {
            protection.finish(entries.stream().anyMatch(entry ->
                    !arena.dataFileAvailable(entry.target().address().fileIndex())));
        }
    }

    @Override
    public void close() {
        if (protection != null) {
            protection.close();
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SegmentArenaMove move && operationId.equals(move.operationId)
                && operationGeneration.equals(move.operationGeneration) && bookieCut == move.bookieCut
                && entries.equals(move.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationId, operationGeneration, bookieCut, entries);
    }
}

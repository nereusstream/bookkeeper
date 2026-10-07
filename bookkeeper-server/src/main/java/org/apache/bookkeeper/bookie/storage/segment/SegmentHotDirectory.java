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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Bounded owner-only physical candidates; Bookie permission, selector and entry success remain separate. */
final class SegmentHotDirectory {
    private record Key(ProfileLedgerContext context, long entryId) { }

    private record Candidate(SegmentPointRead.Location location, boolean liveDurable) { }

    record IndexCut(SegmentDataRecovery.StreamCoverage stream, int entries) { }

    static final class Admission {
        private final SegmentBatchFormat.FrozenBatch batch;
        private final List<Key> keys;
        private final List<SegmentPointRead.Location> locations;
        private IsolatedSegmentFiles.Reservation physical;

        private Admission(SegmentBatchFormat.FrozenBatch batch, List<Key> keys,
                          List<SegmentPointRead.Location> locations) {
            this.batch = batch;
            this.keys = keys;
            this.locations = locations;
        }
    }

    private final SegmentOwnerLoop owner;
    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final int maxEntries;
    private final int maxCoordinates;
    // Occupancy survives locator eviction and unresolved DATA. It stores no locator or payload.
    private final Set<Key> occupied = new HashSet<>();
    private final Map<Key, Candidate> candidates = new HashMap<>();
    private final Map<Key, Admission> pendingKeys = new HashMap<>();
    private final Map<SegmentBatchFormat.FrozenBatch, Admission> pendingBatches = new IdentityHashMap<>();
    private final Map<IsolatedSegmentFiles.Reservation, Admission> byPhysical = new IdentityHashMap<>();
    private SegmentDerivedIndex index;
    private SegmentIoExecutor indexIo;
    private boolean indexInFlight;
    private boolean indexPaused;
    private int unindexed;
    private Throwable indexFailure;
    private SegmentPhysicalPrefix prefix;
    private SegmentArenaControlLog arena;

    SegmentHotDirectory(SegmentOwnerLoop owner, IsolatedSegmentConfig config, SegmentFileHeader identity,
                        int maxEntries) {
        this(owner, config, identity, maxEntries, maxEntries);
    }

    SegmentHotDirectory(SegmentOwnerLoop owner, IsolatedSegmentConfig config, SegmentFileHeader identity,
                        int maxEntries, int maxCoordinates) {
        if (maxEntries < 1 || maxCoordinates < maxEntries) {
            throw new IllegalArgumentException("Positive hot locator capacity required");
        }
        this.owner = owner;
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.maxEntries = maxEntries;
        this.maxCoordinates = maxCoordinates;
    }

    /** Bound by the DATA driver before its first submission, including an externally constructed driver. */
    void bindPrefix(SegmentPhysicalPrefix physicalPrefix, SegmentArenaControlLog authority) {
        if (prefix != null) {
            throw new IllegalStateException("Hot directory already belongs to a DATA prefix");
        }
        prefix = Objects.requireNonNull(physicalPrefix, "physicalPrefix");
        arena = Objects.requireNonNull(authority, "authority");
    }

    boolean matchesScope(IsolatedSegmentFiles files) {
        return files.matchesScope(config, identity);
    }

    void attachIndex(int shard, SegmentDerivedIndex derived, SegmentIoExecutor io) {
        owner.requireOwner();
        if (index != null) {
            throw new IllegalStateException("Derived index already attached");
        }
        index = Objects.requireNonNull(derived, "derived");
        indexIo = Objects.requireNonNull(io, "io");
        owner.attachIndexMaintenance(shard, () -> indexNextBatch(shard));
    }

    boolean canAcceptEntries(int entries) {
        owner.requireOwner();
        return indexFailure == null && entries >= 0
                && entries <= maxEntries - candidates.size() - pendingKeys.size()
                && entries <= maxCoordinates - occupied.size();
    }

    boolean occupied(ProfileLedgerContext context, long entryId) {
        owner.requireOwner();
        return occupied.contains(new Key(context, entryId));
    }

    /** Pause only after every published locator and physical reservation has reached the native index. */
    IndexCut pauseIndexForCheckpoint() {
        owner.requireOwner();
        if (indexPaused || index == null || prefix == null || indexFailure != null || owner.failure() != null
                || indexInFlight || unindexed != 0 || !pendingBatches.isEmpty() || !candidates.isEmpty()
                || prefix.pending() != 0 || prefix.failed()) {
            return null;
        }
        indexPaused = true;
        return new IndexCut(prefix.indexCoverage(), occupied.size());
    }

    void resumeIndexAfterCheckpoint() {
        owner.requireOwner();
        indexPaused = false;
    }

    /** Called on the original owner only after native deletion has completed under a held Bookie snapshot. */
    IndexCut retireTerminalCoordinates(SegmentBookieControlLog.ReclamationLease authority) throws IOException {
        owner.requireOwner();
        if (!indexPaused || prefix == null || indexInFlight || unindexed != 0 || !pendingBatches.isEmpty()
                || !pendingKeys.isEmpty() || !candidates.isEmpty() || prefix.pending() != 0
                || prefix.failed() || indexFailure != null) {
            throw new IOException("Terminal coordinates still have physical or index obligations");
        }
        var keys = occupied.iterator();
        while (keys.hasNext()) {
            if (authority.terminalAtCut(keys.next().context())) {
                keys.remove();
            }
        }
        return new IndexCut(prefix.indexCoverage(), occupied.size());
    }

    /** Query-visible index rows remain physical hints and must still pass prefix, grant and Bookie checks. */
    void freshGet(SegmentOwnerLoop.Ticket ticket, ProfileLedgerContext context, long entryId,
                  BiConsumer<SegmentPointRead.Location, Throwable> completed) {
        owner.requireOwner();
        if (index == null) {
            ticket.resume(() -> completed.accept(arena == null ? null
                    : arena.currentMoveLocation(context, entryId).filter(this::isCurrentDurable).orElse(null), null));
            return;
        }
        AtomicReference<SegmentPointRead.Location> selected = new AtomicReference<>();
        indexIo.submitContinuing(ticket, () -> selected.set(index.get(context, entryId).orElse(null)), failure -> {
            SegmentPointRead.Location location = selected.get();
            if (failure == null && arena != null) {
                location = location == null ? arena.currentMoveLocation(context, entryId).orElse(null)
                        : arena.resolveLocation(location);
            }
            if (failure == null && location != null && !isCurrentDurable(location)) {
                failure = new IOException("Derived location has no occupied coordinate or current durable prefix");
            }
            completed.accept(failure == null ? location : null, failure);
        });
    }

    /** Called after a fresh miss; pending/uncertain coordinates or failed coverage never prove absence. */
    boolean provesAbsent(ProfileLedgerContext context, long entryId) {
        owner.requireOwner();
        return owner.failure() == null && indexFailure == null && prefix != null && !prefix.failed()
                && prefix.pending() == 0 && !occupied(context, entryId)
                && !pendingKeys.containsKey(new Key(context, entryId))
                && (arena == null || arena.currentMoveLocation(context, entryId).isEmpty());
    }

    boolean isCurrentDurable(SegmentPointRead.Location location) {
        owner.requireOwner();
        if (location == null || arena != null && !arena.resolveLocation(location).equals(location)) {
            return false;
        }
        Candidate candidate = candidates.get(new Key(location.context(), location.entryId()));
        return candidate != null && candidate.liveDurable()
                        && (arena == null ? candidate.location() : arena.resolveLocation(candidate.location()))
                                .equals(location) && (prefix == null || prefix.containsDurable(location))
                || index != null && prefix != null && occupied(location.context(), location.entryId())
                        && prefix.containsDurable(location);
    }

    private void indexNextBatch(int shard) {
        owner.requireOwner();
        if (unindexed == 0 || indexInFlight || indexPaused || indexFailure != null || owner.failure() != null) {
            return;
        }
        List<SegmentPointRead.Location> locations = new ArrayList<>();
        for (Candidate candidate : candidates.values()) {
            if (candidate.liveDurable()) {
                locations.add(candidate.location());
                if (locations.size() == 256) {
                    break;
                }
            }
        }
        if (locations.isEmpty()) {
            return;
        }
        List<SegmentPointRead.Location> batch = List.copyOf(locations);
        indexInFlight = owner.submit(shard, new SegmentOwnerLoop.Command() {
            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                indexIo.submit(ticket, () -> index.append(batch), failure -> {
                    if (failure == null) {
                        for (SegmentPointRead.Location location : batch) {
                            Key key = new Key(location.context(), location.entryId());
                            Candidate current = candidates.get(key);
                            if (current != null && current.location().equals(location)) {
                                candidates.remove(key);
                                unindexed--;
                            }
                        }
                    } else {
                        indexFailure = failure;
                    }
                    indexInFlight = false;
                });
            }

            @Override
            public void failed(Throwable failure) {
                indexFailure = failure;
                indexInFlight = false;
            }
        });
    }

    Throwable indexFailure() {
        owner.requireOwner();
        return indexFailure;
    }

    Admission reserve(SegmentBatchFormat.FrozenBatch batch) throws IOException {
        owner.requireOwner();
        if (indexFailure != null) {
            throw new IOException("Derived index maintenance failed; retained hot entries remain readable",
                    indexFailure);
        }
        if (pendingBatches.size() >= config.maxPendingEntries() || pendingBatches.containsKey(batch)) {
            throw new IOException("Hot locator admission capacity exhausted or batch repeated");
        }
        SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(batch.bytes(), batch.address(),
                config.bytesPerFile());
        List<SegmentPointRead.Location> parsedLocations = SegmentPointRead.locations(parsed);
        List<Key> keys = new ArrayList<>();
        List<SegmentPointRead.Location> locations = new ArrayList<>();
        Set<Key> withinBatch = new HashSet<>();
        for (int i = 0; i < parsed.records().size(); i++) {
            SegmentBatchParser.Record record = parsed.records().get(i);
            if (SegmentOwnerLoop.shardFor(record.context().ledgerId(), config.logicalShards())
                    != batch.address().shard()) {
                throw new IOException("DATA record belongs to another logical shard");
            }
            if (record.origin() != SegmentBatchFormat.ENTRY_DATA) {
                continue;
            }
            Key key = new Key(record.context(), record.entryId());
            if (!withinBatch.add(key) || occupied.contains(key)) {
                throw new IOException("Duplicate logical entry requires unresolved idempotency decision");
            }
            keys.add(key);
            locations.add(parsedLocations.get(i));
        }
        if (keys.size() > maxEntries - candidates.size() - pendingKeys.size()
                || keys.size() > maxCoordinates - occupied.size()) {
            throw new IOException("Hot locator capacity exhausted before DATA submission");
        }
        Admission admission = new Admission(batch, List.copyOf(keys), List.copyOf(locations));
        pendingBatches.put(batch, admission);
        for (Key key : keys) {
            pendingKeys.put(key, admission);
            occupied.add(key);
        }
        return admission;
    }

    void bind(Admission admission, IsolatedSegmentFiles.Reservation physical) {
        owner.requireOwner();
        if (pendingBatches.get(admission.batch) != admission || admission.physical != null
                || physical.batch() != admission.batch || byPhysical.containsKey(physical)) {
            throw new IllegalArgumentException("Hot admission does not match the physical reservation");
        }
        admission.physical = physical;
        byPhysical.put(physical, admission);
    }

    void cancelBeforeSubmission(Admission admission) {
        owner.requireOwner();
        if (pendingBatches.get(admission.batch) != admission || admission.physical != null) {
            throw new IllegalArgumentException("Only an unsubmitted hot admission can be cancelled");
        }
        pendingBatches.remove(admission.batch);
        for (Key key : admission.keys) {
            pendingKeys.remove(key);
            occupied.remove(key);
        }
    }

    List<SegmentPointRead.Location> publish(SegmentPhysicalPrefix.DurableBatch ready) {
        owner.requireOwner();
        Admission admission = byPhysical.get(ready.reservation());
        if (admission == null || pendingBatches.get(admission.batch) != admission) {
            throw new IllegalArgumentException("No hot admission for the contiguous durable batch");
        }
        for (Key key : admission.keys) {
            if (pendingKeys.get(key) != admission || candidates.containsKey(key)) {
                throw new IllegalStateException("Hot locator ownership changed before publication");
            }
        }
        for (int i = 0; i < admission.keys.size(); i++) {
            Key key = admission.keys.get(i);
            pendingKeys.remove(key);
            candidates.put(key, new Candidate(admission.locations.get(i), true));
            unindexed++;
        }
        byPhysical.remove(ready.reservation());
        pendingBatches.remove(admission.batch);
        return admission.locations;
    }

    /** Install only physical candidates from a quiesced, synced scan; no Bookie read or ACK authority. */
    void restorePhysical(SegmentDataRecovery.PhysicalStreamCandidate stream) throws IOException {
        restorePhysical(List.of(stream));
    }

    /** Validate the entire selected stream set before exposing any physical candidate to this owner. */
    void restorePhysical(List<SegmentDataRecovery.PhysicalStreamCandidate> streams) throws IOException {
        owner.requireOwner();
        List<SegmentPointRead.Location> entries = new ArrayList<>();
        for (SegmentDataRecovery.PhysicalStreamCandidate stream : streams) {
            if (!config.sameStorageScope(stream.sourceConfig()) || !identity.equals(stream.sourceIdentity())) {
                throw new IOException("Physical recovery candidate belongs to another storage scope");
            }
            if (stream.entries().size() > maxEntries - candidates.size() - entries.size()) {
                throw new IOException("Hot locator capacity exhausted during physical recovery");
            }
            entries.addAll(stream.entries());
        }
        installRecovered(entries);
    }

    /** Keep every old coordinate occupied after a full locked scan; none gains local-result authority. */
    void restorePhysicalInventory(SegmentDataRecovery.PhysicalInventory inventory) throws IOException {
        owner.requireOwner();
        if (!config.sameStorageScope(inventory.sourceConfig()) || !identity.equals(inventory.sourceIdentity())) {
            throw new IOException("Physical inventory belongs to another storage scope");
        }
        installRecovered(inventory.entries());
    }

    private void installRecovered(List<SegmentPointRead.Location> entries) throws IOException {
        installRecovered(entries, false);
    }

    /** Called only by the clean-session DATA bootstrap, after its Bookie cut and stream checks. */
    void restoreCleanRestart(SegmentDataRecovery.PhysicalInventory inventory, int shard) throws IOException {
        owner.requireOwner();
        if (!config.sameStorageScope(inventory.sourceConfig()) || !identity.equals(inventory.sourceIdentity())) {
            throw new IOException("Clean restart inventory belongs to another storage scope");
        }
        List<SegmentPointRead.Location> entries = inventory.entries().stream()
                .filter(location -> location.address().shard() == shard).toList();
        if (index == null) {
            installRecovered(entries, true);
        } else {
            // The storage graph completed the real native rebuild before opening this clean DATA session.
            validateRecovered(entries, false);
            for (SegmentPointRead.Location location : entries) {
                occupied.add(new Key(location.context(), location.entryId()));
            }
        }
    }

    private void installRecovered(List<SegmentPointRead.Location> entries, boolean localDurable) throws IOException {
        validateRecovered(entries, true);
        for (SegmentPointRead.Location location : entries) {
            Key key = new Key(location.context(), location.entryId());
            occupied.add(key);
            candidates.put(key, new Candidate(location, localDurable));
            if (localDurable) {
                unindexed++;
            }
        }
    }

    private void validateRecovered(List<SegmentPointRead.Location> entries, boolean installHot) throws IOException {
        if (!pendingBatches.isEmpty() || !pendingKeys.isEmpty() || !byPhysical.isEmpty()) {
            throw new IOException("Physical recovery cannot overlap DATA submissions");
        }
        if (entries.size() > maxCoordinates - occupied.size()
                || installHot && entries.size() > maxEntries - candidates.size()) {
            throw new IOException("Hot locator capacity exhausted during physical recovery");
        }
        Set<Key> incoming = new HashSet<>();
        for (SegmentPointRead.Location location : entries) {
            Key key = new Key(location.context(), location.entryId());
            if (!incoming.add(key) || occupied.contains(key)) {
                throw new IOException("Duplicate logical entry in physical recovery candidates");
            }
        }
    }

    Optional<SegmentPointRead.Location> candidate(ProfileLedgerContext context, long entryId) {
        owner.requireOwner();
        Candidate candidate = candidates.get(new Key(context, entryId));
        return candidate == null ? Optional.empty() : Optional.of(candidate.location());
    }

    /** Restored physical candidates lack the stream/selector proof needed for local duplicate success. */
    Optional<SegmentPointRead.Location> liveDurableCandidate(ProfileLedgerContext context, long entryId) {
        owner.requireOwner();
        Candidate candidate = candidates.get(new Key(context, entryId));
        return candidate != null && candidate.liveDurable()
                ? Optional.of(arena == null ? candidate.location() : arena.resolveLocation(candidate.location()))
                        .filter(this::isCurrentDurable) : Optional.empty();
    }

    int pendingBatches() {
        owner.requireOwner();
        return pendingBatches.size();
    }

    int candidateCount() {
        owner.requireOwner();
        return candidates.size();
    }
}

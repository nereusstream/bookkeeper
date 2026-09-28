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
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Bounded owner-only physical candidates; Bookie permission, selector and entry success remain separate. */
final class SegmentHotDirectory {
    private record Key(ProfileLedgerContext context, long entryId) { }

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
    private final int maxEntries;
    private final Map<Key, SegmentPointRead.Location> candidates = new HashMap<>();
    private final Map<Key, Admission> pendingKeys = new HashMap<>();
    private final Map<SegmentBatchFormat.FrozenBatch, Admission> pendingBatches = new IdentityHashMap<>();
    private final Map<IsolatedSegmentFiles.Reservation, Admission> byPhysical = new IdentityHashMap<>();

    SegmentHotDirectory(SegmentOwnerLoop owner, IsolatedSegmentConfig config, int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("Positive hot locator capacity required");
        }
        this.owner = owner;
        this.config = config;
        this.maxEntries = maxEntries;
    }

    Admission reserve(SegmentBatchFormat.FrozenBatch batch) throws IOException {
        owner.requireOwner();
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
            if (record.origin() != SegmentBatchFormat.ENTRY_DATA) {
                continue;
            }
            Key key = new Key(record.context(), record.entryId());
            if (!withinBatch.add(key) || candidates.containsKey(key) || pendingKeys.containsKey(key)) {
                throw new IOException("Duplicate logical entry requires unresolved idempotency decision");
            }
            keys.add(key);
            locations.add(parsedLocations.get(i));
        }
        if (keys.size() > maxEntries - candidates.size() - pendingKeys.size()) {
            throw new IOException("Hot locator capacity exhausted before DATA submission");
        }
        Admission admission = new Admission(batch, List.copyOf(keys), List.copyOf(locations));
        pendingBatches.put(batch, admission);
        for (Key key : keys) {
            pendingKeys.put(key, admission);
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
            candidates.put(key, admission.locations.get(i));
        }
        byPhysical.remove(ready.reservation());
        pendingBatches.remove(admission.batch);
        return admission.locations;
    }

    /** Install only physical candidates from a quiesced, synced scan; no Bookie read or ACK authority. */
    void restorePhysical(SegmentDataRecovery.PhysicalStreamCandidate stream) throws IOException {
        owner.requireOwner();
        if (!pendingBatches.isEmpty() || !pendingKeys.isEmpty() || !byPhysical.isEmpty()) {
            throw new IOException("Physical recovery cannot overlap DATA submissions");
        }
        List<SegmentPointRead.Location> entries = stream.entries();
        if (entries.size() > maxEntries - candidates.size()) {
            throw new IOException("Hot locator capacity exhausted during physical recovery");
        }
        Set<Key> incoming = new HashSet<>();
        for (SegmentPointRead.Location location : entries) {
            Key key = new Key(location.context(), location.entryId());
            if (!incoming.add(key) || candidates.containsKey(key)) {
                throw new IOException("Duplicate logical entry in physical recovery candidates");
            }
        }
        for (SegmentPointRead.Location location : entries) {
            candidates.put(new Key(location.context(), location.entryId()), location);
        }
    }

    Optional<SegmentPointRead.Location> candidate(ProfileLedgerContext context, long entryId) {
        owner.requireOwner();
        return Optional.ofNullable(candidates.get(new Key(context, entryId)));
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

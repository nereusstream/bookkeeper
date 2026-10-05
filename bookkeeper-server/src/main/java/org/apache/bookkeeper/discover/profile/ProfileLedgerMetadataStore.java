/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.discover.profile;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerMetadataUtils;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Experimental standard-metadata creation after the coordinator has verified reservation and all-E install.
 * Borrows the existing manager. It neither retries writes nor publishes READY, activation or a writer.
 */
public final class ProfileLedgerMetadataStore {
    public enum Outcome { APPLIED, ALREADY_APPLIED, CONFLICT, QUARANTINED, DURABILITY_UNKNOWN }

    public record Result(Outcome outcome, Versioned<LedgerMetadata> metadata) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            if ((outcome == Outcome.APPLIED || outcome == Outcome.ALREADY_APPLIED) != (metadata != null)) {
                throw new IllegalArgumentException("Only a proved creation carries metadata");
            }
        }
    }

    private final LedgerManager manager;

    public ProfileLedgerMetadataStore(LedgerManager manager) {
        this.manager = Objects.requireNonNull(manager, "manager");
    }

    public CompletableFuture<Result> createIfAbsent(LedgerMetadata initial, ProfileMetadataBacklink backlink) {
        Objects.requireNonNull(initial, "initial");
        Objects.requireNonNull(backlink, "backlink");
        if (initial.getMetadataFormatVersion() != 3 || initial.getState() != LedgerMetadata.State.OPEN
                || initial.getDigestType() != DigestType.CRC32C || initial.getAllEnsembles().size() != 1
                || initial.getAllEnsembles().firstKey() != 0
                || initial.getEnsembleSize() < 1 || initial.getAckQuorumSize() < 1
                || new HashSet<>(initial.getAllEnsembles().firstEntry().getValue()).size()
                        != initial.getEnsembleSize()) {
            throw new IllegalArgumentException("Invalid initial Profile metadata");
        }
        // Own the reserved reference and copies of custom values before the asynchronous create.
        LedgerMetadata desired = backlink.attachTo(initial);
        // The create callback can return the submitted object, including its NODEEXISTS token-resolution path.
        // One actual snapshot must prove the full reference/content/version even after an acknowledged create.
        return manager.createLedgerMetadata(desired.getLedgerId(), desired).handle((created, failure) ->
                manager.readLedgerMetadata(desired.getLedgerId()).handle((current, readFailure) ->
                        readFailure == null ? classify(desired, backlink, current,
                                failure == null ? Outcome.APPLIED : Outcome.ALREADY_APPLIED)
                                : new Result(Outcome.DURABILITY_UNKNOWN, null))).thenCompose(result -> result);
    }

    private static Result classify(LedgerMetadata desired, ProfileMetadataBacklink backlink,
            Versioned<LedgerMetadata> current, Outcome success) {
        try {
            LedgerMetadata actual = current.getValue();
            if (!actual.getCustomMetadata().containsKey(ProfileMetadataBacklink.KEY)) {
                return new Result(Outcome.CONFLICT, null);
            }
            if (!backlink.equals(ProfileMetadataBacklink.from(actual))) {
                return new Result(Outcome.CONFLICT, null);
            }
            boolean equal = actual.getMetadataFormatVersion() == desired.getMetadataFormatVersion()
                    && actual.getState() == LedgerMetadata.State.OPEN
                    && actual.getEnsembleSize() == desired.getEnsembleSize()
                    && actual.getWriteQuorumSize() == desired.getWriteQuorumSize()
                    && actual.getAckQuorumSize() == desired.getAckQuorumSize()
                    && actual.hasPassword() == desired.hasPassword()
                    && actual.getDigestType() == desired.getDigestType()
                    && Arrays.equals(actual.getPassword(), desired.getPassword())
                    && LedgerMetadataUtils.shouldStoreCtime(actual) == LedgerMetadataUtils.shouldStoreCtime(desired)
                    && (!LedgerMetadataUtils.shouldStoreCtime(desired) || actual.getCtime() == desired.getCtime())
                    && actual.getAllEnsembles().equals(desired.getAllEnsembles())
                    && actual.getCustomMetadata().keySet().equals(desired.getCustomMetadata().keySet())
                    && actual.getCustomMetadata().entrySet().stream().allMatch(entry ->
                            Arrays.equals(entry.getValue(), desired.getCustomMetadata().get(entry.getKey())));
            return equal ? new Result(success, current) : new Result(Outcome.CONFLICT, null);
        } catch (IllegalArgumentException invalid) {
            return new Result(Outcome.QUARANTINED, null);
        }
    }
}

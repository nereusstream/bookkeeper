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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.MembershipSnapshot;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;

/** Immutable next-slot preparation; it grants inactive INSTALL only, never normal DATA or activation. */
public record ProfileReplacementInstallPage(ProfileMetadataBacklink backlink, UUID lifecycleBirth,
        long sequence, long controlGeneration, long metadataBirthZxid, long metadataVersion, long creatorToken,
        long previousFragmentStart, byte[] previousEnsembleDigest, long fragmentStart, byte[] ensembleDigest,
        List<ProfileControlScope> installs) {
    public ProfileReplacementInstallPage {
        Objects.requireNonNull(backlink, "backlink");
        Objects.requireNonNull(lifecycleBirth, "lifecycleBirth");
        previousEnsembleDigest = ProfileLifecycleHead.fixed(previousEnsembleDigest, 32, false);
        ensembleDigest = ProfileLifecycleHead.fixed(ensembleDigest, 32, false);
        installs = List.copyOf(installs);
        new ProfileReservation(backlink, installs);
        if (lifecycleBirth.equals(new UUID(0, 0)) || sequence < 3
                || sequence > 2 + ProfileReplacementPage.MAX_PUBLICATIONS || controlGeneration <= 0
                || metadataBirthZxid <= 0 || metadataVersion < 0 || creatorToken < 0 || previousFragmentStart < 0
                || fragmentStart < previousFragmentStart
                || installs.get(0).authorityGeneration() != controlGeneration) {
            throw new IllegalArgumentException("Invalid bounded replacement install preparation");
        }
    }

    public static ProfileReplacementInstallPage from(ZKProfileReservationStore.Snapshot source,
            MembershipSnapshot before, long start, List<BookieId> ensemble, List<ProfileControlScope> installs) {
        var head = source.lifecycle();
        var metadata = before.metadata().getValue();
        if (head.state() != ProfileLifecycleHead.State.READY && head.state() != ProfileLifecycleHead.State.REPLACEMENT
                || !(before.metadata().getVersion() instanceof LongVersion version)
                || !head.backlink().equals(ProfileMetadataBacklink.from(metadata))) {
            throw new IllegalArgumentException("Preparation requires the current live instance");
        }
        var builder = LedgerMetadataBuilder.from(metadata).withCToken(metadata.getCToken());
        if (metadata.getAllEnsembles().lastKey() == start) {
            builder.replaceEnsembleEntry(start, List.copyOf(ensemble));
        } else {
            builder.newEnsembleEntry(start, List.copyOf(ensemble));
        }
        var desired = builder.build();
        if (installs.stream().anyMatch(scope -> ensemble.stream().noneMatch(
                bookie -> bookie.toString().equals(scope.targetBookie())))) {
            throw new IllegalArgumentException("Prepared targets must be in the proposed mapping");
        }
        return new ProfileReplacementInstallPage(head.backlink(), head.birthId(), head.sequence() + 1,
                head.controlGeneration(), before.birthZxid(), version.getLongVersion(), metadata.getCToken(),
                metadata.getAllEnsembles().lastKey(), ProfileReplacementPage.ensembleDigest(metadata,
                        metadata.getAllEnsembles().lastKey()), start,
                ProfileReplacementPage.ensembleDigest(desired, start), installs);
    }

    boolean matchesBefore(MembershipSnapshot snapshot) {
        return matches(snapshot, metadataVersion, previousFragmentStart, previousEnsembleDigest);
    }

    boolean matchesCommitted(MembershipSnapshot snapshot) {
        return matches(snapshot, metadataVersion + 1, fragmentStart, ensembleDigest);
    }

    private boolean matches(MembershipSnapshot snapshot, long version, long start, byte[] digest) {
        try {
            var metadata = snapshot.metadata().getValue();
            return snapshot.birthZxid() == metadataBirthZxid
                    && snapshot.metadata().getVersion() instanceof LongVersion actual
                    && actual.getLongVersion() == version
                    && metadata.getCToken() == creatorToken && backlink.equals(ProfileMetadataBacklink.from(metadata))
                    && Arrays.equals(digest, ProfileReplacementPage.ensembleDigest(metadata, start))
                    && !metadata.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY)
                    && !metadata.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.PENDING_PUBLICATION_KEY);
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    @Override
    public byte[] previousEnsembleDigest() {
        return previousEnsembleDigest.clone();
    }
    @Override
    public byte[] ensembleDigest() {
        return ensembleDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileReplacementInstallPage value && backlink.equals(value.backlink)
                && lifecycleBirth.equals(value.lifecycleBirth) && sequence == value.sequence
                && controlGeneration == value.controlGeneration && metadataBirthZxid == value.metadataBirthZxid
                && metadataVersion == value.metadataVersion && creatorToken == value.creatorToken
                && previousFragmentStart == value.previousFragmentStart
                && Arrays.equals(previousEnsembleDigest, value.previousEnsembleDigest)
                && fragmentStart == value.fragmentStart && Arrays.equals(ensembleDigest, value.ensembleDigest)
                && installs.equals(value.installs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlink, lifecycleBirth, sequence, controlGeneration, metadataBirthZxid, metadataVersion,
                creatorToken, previousFragmentStart, Arrays.hashCode(previousEnsembleDigest), fragmentStart,
                Arrays.hashCode(ensembleDigest), installs);
    }
}

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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore.MembershipSnapshot;
import org.apache.bookkeeper.versioning.LongVersion;

/** Bounded secret-free post-membership authority and retained targets; a candidate alone is inert. */
public record ProfileReplacementPage(ProfileMetadataBacklink backlink, UUID lifecycleBirth, byte[] operationId,
        byte[] payloadIdentity, long controlGeneration, long sequence, long previousPageVersion,
        byte[] previousPageDigest, long metadataBirthZxid, long metadataVersion, long creatorToken,
        long fragmentStart, byte[] ensembleDigest, long initialPageVersion, byte[] initialPageDigest,
        List<ProfileInstallReceipt> installs) {
    public static final int MAX_PUBLICATIONS = 32;

    public ProfileReplacementPage {
        Objects.requireNonNull(backlink, "backlink");
        Objects.requireNonNull(lifecycleBirth, "lifecycleBirth");
        operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
        payloadIdentity = ProfileLifecycleHead.fixed(payloadIdentity, 32, false);
        previousPageDigest = ProfileLifecycleHead.fixed(previousPageDigest, 32, false);
        ensembleDigest = ProfileLifecycleHead.fixed(ensembleDigest, 32, false);
        initialPageDigest = ProfileLifecycleHead.fixed(initialPageDigest, 32, false);
        installs = List.copyOf(installs);
        if (lifecycleBirth.equals(new UUID(0, 0)) || controlGeneration <= 0 || sequence < 3
                || sequence > 2 + MAX_PUBLICATIONS || previousPageVersion < 0 || initialPageVersion < 0
                || metadataBirthZxid <= 0 || metadataVersion <= 0 || creatorToken < 0 || fragmentStart < 0) {
            throw new IllegalArgumentException("Invalid bounded replacement authority page");
        }
        new ProfileReservation(backlink, installs.stream().map(ProfileInstallReceipt::scope).toList());
        if (installs.get(0).scope().authorityGeneration() != controlGeneration
                || Arrays.equals(operationId, installs.get(0).scope().operationId())) {
            throw new IllegalArgumentException("Replacement activation must use its own bound operation");
        }
    }

    public static ProfileReplacementPage from(ZKProfileReservationStore.Snapshot source,
            ZKProfileReservationStore.ReadySnapshot initial, MembershipSnapshot committed, long fragmentStart,
            List<ProfileInstallReceipt> installs, byte[] operationId, byte[] payloadIdentity) {
        var head = source.lifecycle();
        if (head.state() != ProfileLifecycleHead.State.READY && head.state() != ProfileLifecycleHead.State.REPLACEMENT
                || !(committed.metadata().getVersion() instanceof LongVersion version)
                || !initial.page().matchesQuorum(committed.metadata().getValue())) {
            throw new IllegalArgumentException("Replacement requires live lifecycle and unchanged quorum policy");
        }
        return new ProfileReplacementPage(head.backlink(), head.birthId(), operationId, payloadIdentity,
                head.controlGeneration(), head.sequence() + 1, head.readyPageVersion(), head.readyPageDigest(),
                committed.birthZxid(), version.getLongVersion(), committed.metadata().getValue().getCToken(),
                fragmentStart, ensembleDigest(committed.metadata().getValue(), fragmentStart),
                initial.storeVersion(), initial.digest(), installs);
    }

    public boolean matchesMetadata(MembershipSnapshot snapshot) {
        try {
            var metadata = snapshot.metadata();
            LedgerMetadata value = metadata.getValue();
            return snapshot.birthZxid() == metadataBirthZxid && metadata.getVersion() instanceof LongVersion version
                    && version.getLongVersion() == metadataVersion && value.getCToken() == creatorToken
                    && backlink.equals(ProfileMetadataBacklink.from(value))
                    && Arrays.equals(ensembleDigest, ensembleDigest(value, fragmentStart))
                    && !value.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY)
                    && !value.getCustomMetadata().containsKey(ProfileLedgerMetadataStore.PENDING_PUBLICATION_KEY)
                    && installs.stream().allMatch(receipt -> value.getAllEnsembles().lastEntry().getValue()
                            .stream().anyMatch(bookie -> bookie.toString().equals(receipt.scope().targetBookie())));
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    public ProfileControlScope activation(String targetBookie) {
        ProfileControlScope installed = installs.stream().map(ProfileInstallReceipt::scope)
                .filter(scope -> scope.targetBookie().equals(targetBookie)).findFirst().orElseThrow();
        return new ProfileControlScope(ProfileControlOperation.ACTIVATE_REPLACEMENT,
                ProfileControlOperation.ACTIVATE_REPLACEMENT.purpose(), backlink.ledgerId(), backlink.instanceId(),
                installed.descriptorIdentity(), targetBookie, installed.targetStorageIncarnation(),
                installed.authorityDomain(), controlGeneration, operationId, payloadIdentity, null);
    }

    static byte[] ensembleDigest(LedgerMetadata metadata, long start) {
        if (metadata.getMetadataFormatVersion() != 3 || metadata.getState() != LedgerMetadata.State.OPEN
                || metadata.getDigestType() != DigestType.CRC32C || metadata.getAllEnsembles().lastKey() != start
                || metadata.getEnsembleSize() > ProfileReservation.MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("Replacement requires current OPEN CRC32C fragment");
        }
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update("BookKeeper/ProfileReplacementEnsemble/v1\0".getBytes(StandardCharsets.US_ASCII));
            hash.update(ByteBuffer.allocate(24).putInt(1).putInt(metadata.getEnsembleSize())
                    .putInt(metadata.getWriteQuorumSize()).putInt(metadata.getAckQuorumSize()).putLong(start).array());
            for (var bookie : metadata.getAllEnsembles().lastEntry().getValue()) {
                byte[] name = ProfileAuthorityCodec.text(bookie.toString(), 255);
                hash.update(ByteBuffer.allocate(4).putInt(name.length).array());
                hash.update(name);
            }
            return hash.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable", impossible);
        }
    }

    @Override
    public byte[] operationId() {
        return operationId.clone();
    }
    @Override
    public byte[] payloadIdentity() {
        return payloadIdentity.clone();
    }
    @Override
    public byte[] previousPageDigest() {
        return previousPageDigest.clone();
    }
    @Override
    public byte[] ensembleDigest() {
        return ensembleDigest.clone();
    }
    @Override
    public byte[] initialPageDigest() {
        return initialPageDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileReplacementPage that && backlink.equals(that.backlink)
                && lifecycleBirth.equals(that.lifecycleBirth) && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(payloadIdentity, that.payloadIdentity) && controlGeneration == that.controlGeneration
                && sequence == that.sequence && previousPageVersion == that.previousPageVersion
                && Arrays.equals(previousPageDigest, that.previousPageDigest)
                && metadataBirthZxid == that.metadataBirthZxid && metadataVersion == that.metadataVersion
                && creatorToken == that.creatorToken && fragmentStart == that.fragmentStart
                && Arrays.equals(ensembleDigest, that.ensembleDigest) && initialPageVersion == that.initialPageVersion
                && Arrays.equals(initialPageDigest, that.initialPageDigest) && installs.equals(that.installs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlink, lifecycleBirth, Arrays.hashCode(operationId), Arrays.hashCode(payloadIdentity),
                controlGeneration, sequence, previousPageVersion, Arrays.hashCode(previousPageDigest),
                metadataBirthZxid, metadataVersion, creatorToken, fragmentStart, Arrays.hashCode(ensembleDigest),
                initialPageVersion, Arrays.hashCode(initialPageDigest), installs);
    }
}

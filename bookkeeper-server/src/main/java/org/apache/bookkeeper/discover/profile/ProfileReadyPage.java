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
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/** Immutable bounded initial target/receipt page. Existence alone never grants activation. */
public record ProfileReadyPage(ProfileMetadataBacklink backlink, UUID lifecycleBirth, byte[] operationId,
        byte[] payloadIdentity, long controlGeneration, long metadataVersion, long creatorToken,
        byte[] ensembleDigest, List<ProfileInstallReceipt> installs) {
    public ProfileReadyPage {
        Objects.requireNonNull(backlink, "backlink");
        Objects.requireNonNull(lifecycleBirth, "lifecycleBirth");
        operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
        payloadIdentity = ProfileLifecycleHead.fixed(payloadIdentity, 32, false);
        ensembleDigest = ProfileLifecycleHead.fixed(ensembleDigest, 32, false);
        installs = List.copyOf(installs);
        if (lifecycleBirth.equals(new UUID(0, 0)) || controlGeneration <= 0 || metadataVersion < 0
                || installs.isEmpty() || installs.size() > ProfileReservation.MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("Invalid initial READY page");
        }
        new ProfileReservation(backlink, installs.stream().map(ProfileInstallReceipt::scope).toList());
        if (controlGeneration != installs.get(0).scope().authorityGeneration()
                || Arrays.equals(operationId, installs.get(0).scope().operationId())) {
            throw new IllegalArgumentException("READY must use a distinct local operation identity");
        }
    }

    public static ProfileReadyPage from(ZKProfileReservationStore.Snapshot preparing,
            ProfileInitialMetadataCoordinator.Result created, byte[] operationId, byte[] payloadIdentity) {
        if (created.metadata() == null || preparing.lifecycle().state() != ProfileLifecycleHead.State.PREPARING
                || !created.installs().stream().map(ProfileInstallReceipt::scope).toList()
                        .equals(preparing.reservation().installs())) {
            throw new IllegalArgumentException("READY requires exact all-E metadata and PREPARING facts");
        }
        LedgerMetadata metadata = created.metadata().getValue();
        if (!(created.metadata().getVersion() instanceof LongVersion version)
                || !preparing.reservation().backlink().equals(ProfileMetadataBacklink.from(metadata))) {
            throw new IllegalArgumentException("Invalid standard metadata birth/reference for READY");
        }
        return new ProfileReadyPage(preparing.reservation().backlink(), preparing.lifecycle().birthId(),
                operationId, payloadIdentity, preparing.lifecycle().controlGeneration(), version.getLongVersion(),
                metadata.getCToken(), ensembleDigest(metadata), created.installs());
    }

    public boolean matchesMetadata(Versioned<LedgerMetadata> metadata) {
        try {
            return metadata.getVersion() instanceof LongVersion version && version.getLongVersion() == metadataVersion
                    && metadata.getValue().getCToken() == creatorToken
                    && backlink.equals(ProfileMetadataBacklink.from(metadata.getValue()))
                    && Arrays.equals(ensembleDigest, ensembleDigest(metadata.getValue()))
                    && installs.stream().map(receipt -> receipt.scope().targetBookie()).toList().equals(
                            metadata.getValue().getAllEnsembles().firstEntry().getValue().stream()
                                    .map(Object::toString).toList());
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    public ProfileControlScope activation(String targetBookie) {
        ProfileControlScope install = installs.stream().map(ProfileInstallReceipt::scope)
                .filter(scope -> scope.targetBookie().equals(targetBookie)).findFirst().orElseThrow();
        return new ProfileControlScope(ProfileControlOperation.ACTIVATE_INITIAL,
                ProfileControlOperation.ACTIVATE_INITIAL.purpose(), backlink.ledgerId(), backlink.instanceId(),
                install.descriptorIdentity(), targetBookie, install.targetStorageIncarnation(),
                        install.authorityDomain(),
                controlGeneration, operationId, payloadIdentity, null);
    }

    static byte[] ensembleDigest(LedgerMetadata metadata) {
        if (metadata.getState() != LedgerMetadata.State.OPEN || metadata.getDigestType() != DigestType.CRC32C
                || metadata.getAllEnsembles().size() != 1 || metadata.getAllEnsembles().firstKey() != 0
                || metadata.getEnsembleSize() < 1
                || metadata.getEnsembleSize() > ProfileReservation.MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("READY requires initial OPEN Profile membership");
        }
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update("BookKeeper/ProfileInitialEnsemble/v1\0".getBytes(StandardCharsets.US_ASCII));
            hash.update(ByteBuffer.allocate(16).putInt(1).putInt(metadata.getEnsembleSize())
                    .putInt(metadata.getWriteQuorumSize()).putInt(metadata.getAckQuorumSize()).array());
            for (var bookie : metadata.getAllEnsembles().firstEntry().getValue()) {
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
    public byte[] ensembleDigest() {
        return ensembleDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileReadyPage that && backlink.equals(that.backlink)
                && lifecycleBirth.equals(that.lifecycleBirth) && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(payloadIdentity, that.payloadIdentity) && controlGeneration == that.controlGeneration
                && metadataVersion == that.metadataVersion && creatorToken == that.creatorToken
                && Arrays.equals(ensembleDigest, that.ensembleDigest) && installs.equals(that.installs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlink, lifecycleBirth, Arrays.hashCode(operationId), Arrays.hashCode(payloadIdentity),
                controlGeneration, metadataVersion, creatorToken, Arrays.hashCode(ensembleDigest), installs);
    }
}

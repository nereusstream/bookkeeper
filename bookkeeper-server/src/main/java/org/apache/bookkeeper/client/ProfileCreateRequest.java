/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary;
import org.apache.bookkeeper.discover.profile.ProfileReplacementPage;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.proto.checksum.DigestManager;

/** Explicit immutable initial-create input; it grants no authority to adopt an existing writer. */
public final class ProfileCreateRequest {
    /** Bounds for the existing writer, retry policy and explicit writer-close protocol. */
    public record WriterLimits(int maxPendingEntries, int maxPayloadBytes, int maxAttempts,
            Duration deadline, Duration backoff, int maxCloseCoordinates, Duration closeDuration) {
        public WriterLimits {
            new ProfileBookieClient.Retry(maxAttempts, Objects.requireNonNull(deadline),
                    Objects.requireNonNull(backoff));
            Objects.requireNonNull(closeDuration, "closeDuration");
            if (maxPendingEntries < 1 || maxPendingEntries > 256 || maxPayloadBytes < 1
                    || maxPayloadBytes > 1024 * 1024 || maxCloseCoordinates < 1
                    || closeDuration.isNegative() || closeDuration.isZero()
                    || closeDuration.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("Invalid bounded Profile writer limits");
            }
        }
    }

    private final LedgerMetadata desired;
    private final ProfileReservation reservation;
    private final byte[] descriptor;
    private final ProtectedProfileCredential credential;
    private final byte[] readyOperationId;
    private final byte[] readyPayloadIdentity;
    private final WriterLimits limits;
    private final int maxEnsembleChanges;

    public ProfileCreateRequest(LedgerMetadata desired, ProfileReservation reservation, byte[] descriptor,
            ProtectedProfileCredential credential, byte[] readyOperationId, byte[] readyPayloadIdentity,
            WriterLimits limits) {
        this(desired, reservation, descriptor, credential, readyOperationId, readyPayloadIdentity, limits, 0);
    }

    public ProfileCreateRequest(LedgerMetadata desired, ProfileReservation reservation, byte[] descriptor,
            ProtectedProfileCredential credential, byte[] readyOperationId, byte[] readyPayloadIdentity,
            WriterLimits limits, int maxEnsembleChanges) {
        if (maxEnsembleChanges < 0 || maxEnsembleChanges > ProfileReplacementPage.MAX_PUBLICATIONS) {
            throw new IllegalArgumentException("Invalid bounded Profile ensemble changes");
        }
        this.maxEnsembleChanges = maxEnsembleChanges;
        this.desired = copyMetadata(Objects.requireNonNull(desired, "desired"));
        this.reservation = Objects.requireNonNull(reservation, "reservation");
        Objects.requireNonNull(descriptor, "descriptor");
        if (descriptor.length > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH) {
            throw new IllegalArgumentException("Profile descriptor exceeds its canonical bound");
        }
        this.descriptor = descriptor.clone();
        this.credential = Objects.requireNonNull(credential, "credential").copy();
        this.readyOperationId = fixed(readyOperationId, 16, true);
        this.readyPayloadIdentity = fixed(readyPayloadIdentity, 32, false);
        this.limits = Objects.requireNonNull(limits, "limits");
        var metadata = this.desired;
        var ensemble = metadata.getAllEnsembles().firstEntry();
        if (metadata.getMetadataFormatVersion() != 3 || metadata.getState() != LedgerMetadata.State.OPEN
                || metadata.getDigestType() != DigestType.CRC32C || !metadata.hasPassword()
                || metadata.getCustomMetadata().containsKey(ProfileRecoveryBoundary.WRITER_CLOSE_KEY)
                || metadata.getAllEnsembles().size() != 1 || ensemble.getKey() != 0
                || metadata.getEnsembleSize() < 1 || metadata.getAckQuorumSize() < 1
                || new HashSet<>(ensemble.getValue()).size() != metadata.getEnsembleSize()
                || metadata.getLedgerId() != reservation.backlink().ledgerId()
                || !reservation.installs().stream().map(scope -> scope.targetBookie()).toList()
                        .equals(ensemble.getValue().stream().map(Object::toString).toList())
                || Arrays.equals(this.readyOperationId, reservation.installs().get(0).operationId())) {
            throw new IllegalArgumentException("Profile create requires fresh OPEN metadata and the exact reserved E");
        }
        byte[] backlink = metadata.getCustomMetadata().get(ProfileMetadataBacklink.KEY);
        if (backlink != null && !reservation.backlink().equals(ProfileMetadataBacklink.decode(backlink))) {
            throw new IllegalArgumentException("Profile create backlink does not match its reservation");
        }
        try {
            if (!this.credential.matchesWireVerifier(DigestManager.generateMasterKey(metadata.getPassword()))) {
                throw new IllegalArgumentException("Profile credential does not match its metadata password");
            }
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("BookKeeper credential digest is unavailable", unavailable);
        }
    }

    /** Return a new deep metadata copy; byte-array accessors on LedgerMetadata cannot mutate this request. */
    public LedgerMetadata desired() {
        return copyMetadata(desired);
    }

    public ProfileReservation reservation() {
        return reservation;
    }

    public byte[] descriptor() {
        return descriptor.clone();
    }

    public ProtectedProfileCredential credential() {
        return credential.copy();
    }

    public byte[] readyOperationId() {
        return readyOperationId.clone();
    }

    public byte[] readyPayloadIdentity() {
        return readyPayloadIdentity.clone();
    }

    public WriterLimits limits() {
        return limits;
    }

    public int maxEnsembleChanges() {
        return maxEnsembleChanges;
    }

    ProfileDescriptor validateDescriptor(ProfileDescriptorValidator validator) {
        var decoded = ProfileDescriptorCodec.decodeAndVerify(descriptor,
                reservation.installs().get(0).descriptorIdentity(), validator);
        if (decoded.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || decoded.durabilityMode() != DurabilityMode.SYNC_ON_ACK
                || decoded.ensembleSize() != desired.getEnsembleSize()
                || decoded.writeQuorumSize() != desired.getWriteQuorumSize()
                || decoded.ackQuorumSize() != desired.getAckQuorumSize()) {
            throw new IllegalArgumentException("Profile descriptor and initial E/W/A do not match");
        }
        return decoded;
    }

    private static LedgerMetadata copyMetadata(LedgerMetadata source) {
        var custom = new HashMap<String, byte[]>();
        source.getCustomMetadata().forEach((key, value) -> custom.put(key, value.clone()));
        var builder = LedgerMetadataBuilder.from(source).withCToken(source.getCToken()).withCustomMetadata(custom);
        if (source.hasPassword()) {
            builder.withPassword(source.getPassword().clone());
        }
        return builder.build();
    }

    private static byte[] fixed(byte[] value, int length, boolean nonzero) {
        if (value == null || value.length != length || nonzero && Arrays.equals(value, new byte[length])) {
            throw new IllegalArgumentException("Invalid fixed Profile create identity");
        }
        return value.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileCreateRequest that && sameMetadata(desired, that.desired)
                && reservation.equals(that.reservation) && Arrays.equals(descriptor, that.descriptor)
                && credential.matches(that.credential) && Arrays.equals(readyOperationId, that.readyOperationId)
                && Arrays.equals(readyPayloadIdentity, that.readyPayloadIdentity) && limits.equals(that.limits)
                && maxEnsembleChanges == that.maxEnsembleChanges;
    }

    private static boolean sameMetadata(LedgerMetadata left, LedgerMetadata right) {
        return left.getLedgerId() == right.getLedgerId() && left.getCToken() == right.getCToken()
                && left.getMetadataFormatVersion() == right.getMetadataFormatVersion()
                && left.getState() == right.getState() && left.getLastEntryId() == right.getLastEntryId()
                && left.getLength() == right.getLength() && left.getCtime() == right.getCtime()
                && LedgerMetadataUtils.shouldStoreCtime(left) == LedgerMetadataUtils.shouldStoreCtime(right)
                && left.getEnsembleSize() == right.getEnsembleSize()
                && left.getWriteQuorumSize() == right.getWriteQuorumSize()
                && left.getAckQuorumSize() == right.getAckQuorumSize() && left.hasPassword() == right.hasPassword()
                && left.getDigestType() == right.getDigestType()
                && Arrays.equals(left.getPassword(), right.getPassword())
                && left.getAllEnsembles().equals(right.getAllEnsembles())
                && left.getCustomMetadata().keySet().equals(right.getCustomMetadata().keySet())
                && left.getCustomMetadata().entrySet().stream().allMatch(entry ->
                        Arrays.equals(entry.getValue(), right.getCustomMetadata().get(entry.getKey())));
    }

    @Override
    public int hashCode() {
        int custom = desired.getCustomMetadata().entrySet().stream()
                .mapToInt(entry -> entry.getKey().hashCode() ^ Arrays.hashCode(entry.getValue())).sum();
        return Objects.hash(desired.getLedgerId(), desired.getCToken(), desired.getMetadataFormatVersion(),
                desired.getState(), desired.getLastEntryId(), desired.getLength(), desired.getCtime(),
                LedgerMetadataUtils.shouldStoreCtime(desired), desired.getEnsembleSize(), desired.getWriteQuorumSize(),
                desired.getAckQuorumSize(), desired.hasPassword(), desired.getDigestType(),
                Arrays.hashCode(desired.getPassword()), desired.getAllEnsembles(), custom, reservation,
                Arrays.hashCode(descriptor), Arrays.hashCode(readyOperationId), Arrays.hashCode(readyPayloadIdentity),
                limits, maxEnsembleChanges);
    }
}

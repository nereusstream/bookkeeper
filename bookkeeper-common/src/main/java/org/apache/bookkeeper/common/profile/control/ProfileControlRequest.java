/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.profile.control;

import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_REQUEST_SHAPE;

import java.util.Objects;
import java.util.Optional;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;

/** Bounded typed request; physical control-tail packing remains deliberately unfrozen. */
public final class ProfileControlRequest {

    /** Selected boundary only; carries neither an exact DATA range nor a recovery grant. */
    public record TerminalBinding(long prefixEntryId, long prefixLength, byte[] selectionDigest) {
        public TerminalBinding {
            selectionDigest = Objects.requireNonNull(selectionDigest, "selectionDigest").clone();
            if (prefixEntryId < -1 || prefixEntryId == Long.MAX_VALUE || prefixLength < 0
                    || prefixEntryId == -1 && prefixLength != 0 || selectionDigest.length != 32) {
                throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
            }
        }
        @Override
        public byte[] selectionDigest() {
            return selectionDigest.clone();
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof TerminalBinding that && prefixEntryId == that.prefixEntryId
                    && prefixLength == that.prefixLength
                    && java.util.Arrays.equals(selectionDigest, that.selectionDigest);
        }
        @Override
        public int hashCode() {
            return Objects.hash(prefixEntryId, prefixLength, java.util.Arrays.hashCode(selectionDigest));
        }
    }

    public record RecoveryBinding(byte[] intentId, long intentGeneration, long grantGeneration) {
        public RecoveryBinding {
            intentId = Objects.requireNonNull(intentId, "intentId").clone();
            if (intentId.length != 16 || java.util.Arrays.equals(intentId, new byte[16])
                    || intentGeneration <= 0 || grantGeneration <= 0) {
                throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
            }
        }
        @Override
        public byte[] intentId() {
            return intentId.clone();
        }
    }

    /** Committed initial delete stream coordinate, bound to its original request and cluster tombstone. */
    public record DeleteBinding(byte[] streamId, long streamGeneration, long assignmentGeneration, long sequence,
            byte[] requestId, long deleteEpoch, byte[] tombstoneDigest) {
        public static final int BYTES = 96;
        public DeleteBinding {
            streamId = Objects.requireNonNull(streamId, "streamId").clone();
            requestId = Objects.requireNonNull(requestId, "requestId").clone();
            tombstoneDigest = Objects.requireNonNull(tombstoneDigest, "tombstoneDigest").clone();
            if (streamId.length != 16 || requestId.length != 16 || tombstoneDigest.length != 32
                    || java.util.Arrays.equals(streamId, new byte[16])
                    || java.util.Arrays.equals(requestId, new byte[16]) || streamGeneration != 1
                    || assignmentGeneration != 1 || sequence < 1 || sequence > 4096 || deleteEpoch < 1) {
                throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
            }
        }
        @Override
        public byte[] streamId() {
            return streamId.clone();
        }
        @Override
        public byte[] requestId() {
            return requestId.clone();
        }
        @Override
        public byte[] tombstoneDigest() {
            return tombstoneDigest.clone();
        }
        public byte[] encode() {
            return java.nio.ByteBuffer.allocate(BYTES).put(streamId).putLong(streamGeneration)
                    .putLong(assignmentGeneration).putLong(sequence).put(requestId).putLong(deleteEpoch)
                    .put(tombstoneDigest).array();
        }
        public static DeleteBinding decode(java.nio.ByteBuffer input) {
            if (input.remaining() != BYTES) {
                throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
            }
            byte[] stream = new byte[16];
            input.get(stream);
            long generation = input.getLong();
            long assignment = input.getLong();
            long sequence = input.getLong();
            byte[] request = new byte[16];
            input.get(request);
            long epoch = input.getLong();
            byte[] digest = new byte[32];
            input.get(digest);
            return new DeleteBinding(stream, generation, assignment, sequence, request, epoch, digest);
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof DeleteBinding binding && java.util.Arrays.equals(encode(), binding.encode());
        }
        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(encode());
        }
    }

    private final RecoveryBinding recoveryBinding;
    private final TerminalBinding terminalBinding;
    private final DeleteBinding deleteBinding;
    private final byte[] closedGrantOperationId;
    private final ProfileControlScope scope;
    private final byte[] canonicalDescriptor;
    private final EngineProfile expectedEngine;
    private final ProtectedProfileCredential protectedCredential;

    private ProfileControlRequest(
            ProfileControlScope scope,
            byte[] canonicalDescriptor,
            EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, recovery, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery, byte[] closedGrant) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, recovery, closedGrant, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery, byte[] closedGrant,
            TerminalBinding terminal) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, recovery, closedGrant, terminal, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery, byte[] closedGrant,
            TerminalBinding terminal, DeleteBinding deletion) {
        deleteBinding = deletion;
        terminalBinding = terminal;
        recoveryBinding = recovery;
        closedGrantOperationId = closedGrant == null ? null : closedGrant.clone();
        this.scope = Objects.requireNonNull(scope, "scope");
        this.canonicalDescriptor = canonicalDescriptor == null ? null : canonicalDescriptor.clone();
        this.expectedEngine = expectedEngine;
        this.protectedCredential = protectedCredential == null ? null : protectedCredential.copy();
    }

    public static ProfileControlRequest install(
            ProfileControlScope scope,
            byte[] canonicalDescriptor,
            EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential) {
        if (scope == null
                || scope.operation() != ProfileControlOperation.INSTALL
                || canonicalDescriptor == null
                || canonicalDescriptor.length == 0
                || canonicalDescriptor.length > ProfileDescriptorCodec.ABSOLUTE_INPUT_CAP
                || expectedEngine == null
                || protectedCredential == null) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, canonicalDescriptor, expectedEngine, protectedCredential);
    }

    public static ProfileControlRequest operation(ProfileControlScope scope) {
        if (scope == null || scope.operation() == ProfileControlOperation.INSTALL
                || scope.operation() == ProfileControlOperation.RECOVERY_TERMINAL_CLOSE) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null);
    }

    public static ProfileControlRequest deleteApply(ProfileControlScope scope, DeleteBinding binding) {
        if (scope == null || binding == null || scope.operation() != ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY
                || scope.exactRange() != null || scope.authorityGeneration() != binding.deleteEpoch()
                || !java.util.Arrays.equals(scope.publicSemanticPayloadIdentity(),
                        deletePayloadIdentity(scope, binding))) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, null, null, null, binding);
    }

    public Optional<DeleteBinding> deleteBinding() {
        return Optional.ofNullable(deleteBinding);
    }

    public static byte[] deletePayloadIdentity(ProfileControlScope scope, DeleteBinding binding) {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            digest.update("BookKeeper/ProfileDeleteApply/v1\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            digest.update(new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(scope.ledgerId(),
                    scope.ledgerInstanceId(), scope.descriptorIdentity().toBytes()).encode());
            digest.update(scope.targetBookie().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            digest.update(scope.targetStorageIncarnation());
            digest.update(scope.operationId());
            return digest.digest(binding.encode());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static ProfileControlRequest recoveryGrant(ProfileControlScope scope, RecoveryBinding binding) {
        if (scope == null || scope.operation() != ProfileControlOperation.RECOVERY_GRANT || binding == null
                || scope.authorityGeneration() != binding.grantGeneration() || scope.exactRange() == null) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, binding);
    }

    public Optional<RecoveryBinding> recoveryBinding() {
        return Optional.ofNullable(recoveryBinding);
    }

    public static ProfileControlRequest recoveryClose(ProfileControlScope scope, RecoveryBinding binding,
            byte[] grantOperationId) {
        if (scope == null || scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE || binding == null
                || scope.exactRange() == null || grantOperationId == null || grantOperationId.length != 16
                || java.util.Arrays.equals(grantOperationId, new byte[16])
                || java.util.Arrays.equals(grantOperationId, scope.operationId())) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, binding, grantOperationId);
    }

    public Optional<byte[]> closedGrantOperationId() {
        return closedGrantOperationId == null ? Optional.empty() : Optional.of(closedGrantOperationId.clone());
    }

    public static ProfileControlRequest terminalClose(ProfileControlScope scope, TerminalBinding binding) {
        if (scope == null || scope.operation() != ProfileControlOperation.RECOVERY_TERMINAL_CLOSE
                || scope.exactRange() != null || binding == null
                || !java.util.Arrays.equals(scope.publicSemanticPayloadIdentity(),
                    terminalPayloadIdentity(scope, binding))) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, null, null, binding);
    }

    public Optional<TerminalBinding> terminalBinding() {
        return Optional.ofNullable(terminalBinding);
    }

    public static byte[] terminalPayloadIdentity(ProfileControlScope scope, TerminalBinding binding) {
        byte[] target = scope.targetBookie().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] domain = scope.authorityDomain().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] separator = "UW-RECOVERY-TERMINAL-V1".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        var bytes = java.nio.ByteBuffer.allocate(separator.length + 8 + 16 + 36 + 16 + 8 + 48
                + 4 + target.length + 4 + domain.length).put(separator).putLong(scope.ledgerId())
                .put(scope.ledgerInstanceId()).put(scope.descriptorIdentity().toBytes())
                .put(scope.targetStorageIncarnation()).putLong(scope.authorityGeneration())
                .putLong(binding.prefixEntryId()).putLong(binding.prefixLength()).put(binding.selectionDigest())
                .putInt(target.length).put(target).putInt(domain.length).put(domain).array();
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public ProfileControlScope scope() {
        return scope;
    }

    public Optional<byte[]> canonicalDescriptor() {
        return canonicalDescriptor == null ? Optional.empty() : Optional.of(canonicalDescriptor.clone());
    }

    public Optional<EngineProfile> expectedEngine() {
        return Optional.ofNullable(expectedEngine);
    }

    public Optional<ProtectedProfileCredential> protectedCredential() {
        return protectedCredential == null ? Optional.empty() : Optional.of(protectedCredential.copy());
    }

    @Override
    public String toString() {
        return "ProfileControlRequest{scope=" + scope
                + ", descriptorBytes=" + (canonicalDescriptor == null ? 0 : canonicalDescriptor.length)
                + ", expectedEngine=" + expectedEngine
                + ", protectedCredential=" + (protectedCredential == null ? "absent" : protectedCredential)
                + '}';
    }
}

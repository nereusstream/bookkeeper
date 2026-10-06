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

package org.apache.bookkeeper.common.profile.wire;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;

/**
 * Explicit opt-in, disposable initial-control schema. Generic operation codec remains blocked for control bodies.
 * Target identity is bound by verified HELLO/connection facts, never by a caller-selected metadata path.
 */
public final class ProfileInitialControlCodec {
    public static final long CAPABILITY_ID = 0x55494331L;
    public static final int SCHEMA = 1;
    public static final int COMMON_BYTES = 126;
    public static final int MIN_INSTALL_BYTES = COMMON_BYTES + 1 + 30 + ProfileDescriptorCodec.MIN_CANONICAL_LENGTH;
    public static final int MAX_ACTIVATE_BYTES = COMMON_BYTES + ProfileControlScope.MAX_AUTHORITY_DOMAIN_BYTES;
    public static final int MAX_INSTALL_BYTES = MAX_ACTIVATE_BYTES + 30 + ProfileDescriptorCodec.MAX_CANONICAL_LENGTH;
    public static final int REPLY_BYTES = 60;
    private static final int REQUEST_MAGIC = 0x55574943;
    private static final int REPLY_MAGIC = 0x55574952;

    private ProfileInitialControlCodec() {
    }

    public static boolean supported(ProfileOperation operation) {
        return operation == ProfileOperation.INSTALL || operation == ProfileOperation.ACTIVATE_INITIAL;
    }

    public static byte[] encodeRequest(ProfileControlRequest request) {
        ProfileControlScope scope = request.scope();
        if (scope.operation() != ProfileControlOperation.INSTALL
                && scope.operation() != ProfileControlOperation.ACTIVATE_INITIAL) {
            throw invalid();
        }
        byte[] domain = text(scope.authorityDomain());
        byte[] descriptor = scope.operation() == ProfileControlOperation.INSTALL
                ? request.canonicalDescriptor().orElseThrow() : null;
        if (descriptor != null && (descriptor.length < ProfileDescriptorCodec.MIN_CANONICAL_LENGTH
                || descriptor.length > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH
                || request.expectedEngine().orElse(null) != EngineProfile.SEGMENT_WAL_ENGINE)) {
            throw invalid();
        }
        int size = COMMON_BYTES + domain.length + (descriptor == null ? 0 : 30 + descriptor.length);
        ByteBuffer out = ByteBuffer.allocate(size);
        out.put(new ProfileLedgerContext(scope.ledgerId(), scope.ledgerInstanceId(),
                scope.descriptorIdentity().toBytes()).encode())
                .putInt(REQUEST_MAGIC).putShort((short) SCHEMA).putShort((short) 0)
                .put(scope.operationId()).put(scope.publicSemanticPayloadIdentity())
                .putLong(scope.authorityGeneration())
                .putShort((short) domain.length).put(domain);
        if (descriptor != null) {
            ProtectedProfileCredential credential = request.protectedCredential().orElseThrow();
            out.putInt(descriptor.length).putShort((short) EngineProfile.SEGMENT_WAL_ENGINE.wireValue())
                    .putShort((short) credential.kind()).putShort((short) credential.length());
            credential.writeWireCredential(out);
            out.put(descriptor);
        }
        return out.array();
    }

    public static ProfileControlRequest decodeRequest(ProfileOperation operation, ByteBuffer body,
            ProfileHello.Server verifiedLocalHello) {
        int minimum = operation == ProfileOperation.INSTALL ? MIN_INSTALL_BYTES : COMMON_BYTES + 1;
        int maximum = operation == ProfileOperation.INSTALL ? MAX_INSTALL_BYTES : MAX_ACTIVATE_BYTES;
        if (!supported(operation) || body.remaining() < minimum || body.remaining() > maximum) {
            throw invalid();
        }
        ByteBuffer in = body.slice();
        ByteBuffer contextBytes = in.slice();
        contextBytes.limit(ProfileLedgerContext.ENCODED_LENGTH);
        ProfileLedgerContext context = ProfileLedgerContext.decode(contextBytes);
        in.position(ProfileLedgerContext.ENCODED_LENGTH);
        if (in.getInt() != REQUEST_MAGIC || in.getShort() != SCHEMA || in.getShort() != 0) {
            throw invalid();
        }
        byte[] id = new byte[16];
        byte[] payload = new byte[32];
        in.get(id).get(payload);
        long generation = in.getLong();
        int domainBytes = Short.toUnsignedInt(in.getShort());
        if (domainBytes < 1 || domainBytes > ProfileControlScope.MAX_AUTHORITY_DOMAIN_BYTES
                || domainBytes > in.remaining()) {
            throw invalid();
        }
        byte[] encodedDomain = new byte[domainBytes];
        in.get(encodedDomain);
        String domain = new String(encodedDomain, StandardCharsets.UTF_8);
        if (!Arrays.equals(encodedDomain, text(domain))) {
            throw invalid();
        }
        ProfileControlOperation purpose = operation == ProfileOperation.INSTALL
                ? ProfileControlOperation.INSTALL : ProfileControlOperation.ACTIVATE_INITIAL;
        ProfileControlScope scope = new ProfileControlScope(purpose, purpose.purpose(), context.ledgerId(),
                context.ledgerInstanceId(), ProfileDescriptorIdentity.fromBytes(
                        context.descriptorIdentity()), verifiedLocalHello.bookieId(),
                verifiedLocalHello.storageIncarnation(),
                domain, generation, id, payload, null);
        if (operation == ProfileOperation.ACTIVATE_INITIAL) {
            if (in.hasRemaining()) {
                throw invalid();
            }
            return ProfileControlRequest.operation(scope);
        }
        if (in.remaining() < 31) {
            throw invalid();
        }
        long descriptorBytes = Integer.toUnsignedLong(in.getInt());
        int engine = Short.toUnsignedInt(in.getShort());
        int kind = Short.toUnsignedInt(in.getShort());
        int length = Short.toUnsignedInt(in.getShort());
        if (engine != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                || kind != ProtectedProfileCredential.BK_MASTER_KEY_SHA1
                || length != ProtectedProfileCredential.CREDENTIAL_LENGTH
                || descriptorBytes < ProfileDescriptorCodec.MIN_CANONICAL_LENGTH
                || descriptorBytes > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH
                || in.remaining() != length + descriptorBytes) {
            throw invalid();
        }
        byte[] temporary = new byte[length];
        byte[] descriptor = new byte[(int) descriptorBytes];
        try {
            in.get(temporary).get(descriptor);
            return ProfileControlRequest.install(scope, descriptor, EngineProfile.SEGMENT_WAL_ENGINE,
                    ProtectedProfileCredential.of(kind, temporary));
        } finally {
            Arrays.fill(temporary, (byte) 0);
        }
    }

    public static byte[] encodeReply(ProfileControlResult result) {
        if ((result.operation() != ProfileControlOperation.INSTALL
                && result.operation() != ProfileControlOperation.ACTIVATE_INITIAL)
                || result.operationIdHex().length() != 32) {
            throw invalid();
        }
        ProfileStatus status = status(result.outcome());
        if ((result.outcome() == ProfileControlResult.Outcome.APPLIED
                || result.outcome() == ProfileControlResult.Outcome.ALREADY_APPLIED)
                && (!result.durable() || result.localGeneration() <= 0)) {
            throw invalid();
        }
        return ByteBuffer.allocate(REPLY_BYTES).put(status.encode()).putInt(REPLY_MAGIC).putShort((short) SCHEMA)
                .putShort((short) (outcomeCode(result.outcome()))).putLong(result.ledgerId())
                .put(HexFormat.of().parseHex(result.operationIdHex())).putLong(result.authorityGeneration())
                .putLong(result.localGeneration()).putInt(0).array();
    }

    public static ProfileControlResult decodeReply(ProfileOperation operation, byte[] body) {
        if (!supported(operation) || body.length != REPLY_BYTES) {
            throw invalid();
        }
        ByteBuffer in = ByteBuffer.wrap(body);
        byte[] prefix = new byte[ProfileStatus.ENCODED_LENGTH];
        in.get(prefix);
        ProfileStatus status = ProfileStatus.decode(prefix);
        if (in.getInt() != REPLY_MAGIC || in.getShort() != SCHEMA) {
            throw invalid();
        }
        int kind = Short.toUnsignedInt(in.getShort());
        ProfileControlResult.Outcome outcome = switch (kind) {
            case 1 -> ProfileControlResult.Outcome.APPLIED;
            case 2 -> ProfileControlResult.Outcome.ALREADY_APPLIED;
            case 3 -> ProfileControlResult.Outcome.CONFLICT;
            case 4 -> ProfileControlResult.Outcome.DURABILITY_UNKNOWN;
            case 5 -> ProfileControlResult.Outcome.NOT_FOUND;
            case 6 -> ProfileControlResult.Outcome.STALE_OR_COMPACTED;
            case 7 -> ProfileControlResult.Outcome.DENIED;
            case 8 -> ProfileControlResult.Outcome.TRANSIENT_RECONCILING;
            case 9 -> ProfileControlResult.Outcome.QUARANTINED;
            case 10 -> ProfileControlResult.Outcome.BAD_REQUEST;
            case 11 -> ProfileControlResult.Outcome.UNSUPPORTED;
            default -> throw invalid();
        };
        long ledger = in.getLong();
        byte[] id = new byte[16];
        in.get(id);
        long generation = in.getLong();
        long local = in.getLong();
        if (in.getInt() != 0 || !status.equals(status(outcome))) {
            throw invalid();
        }
        boolean durable = outcome == ProfileControlResult.Outcome.APPLIED
                || outcome == ProfileControlResult.Outcome.ALREADY_APPLIED;
        if (durable && local <= 0) {
            throw invalid();
        }
        return new ProfileControlResult(outcome, operation == ProfileOperation.INSTALL
                ? ProfileControlOperation.INSTALL : ProfileControlOperation.ACTIVATE_INITIAL,
                ledger, HexFormat.of().formatHex(id), generation, local, durable);
    }

    private static int outcomeCode(ProfileControlResult.Outcome outcome) {
        return switch (outcome) {
            case APPLIED -> 1;
            case ALREADY_APPLIED -> 2;
            case CONFLICT -> 3;
            case DURABILITY_UNKNOWN -> 4;
            case NOT_FOUND -> 5;
            case STALE_OR_COMPACTED -> 6;
            case DENIED -> 7;
            case TRANSIENT_RECONCILING -> 8;
            case QUARANTINED -> 9;
            case BAD_REQUEST -> 10;
            case UNSUPPORTED -> 11;
        };
    }

    public static ProfileStatus status(ProfileControlResult.Outcome outcome) {
        return switch (outcome) {
            case APPLIED -> new ProfileStatus(ProfileStatus.StatusClass.OK, ProfileStatus.RetryDisposition.NEVER,
                    ProfileStatus.DurableResult.APPLIED, 0);
            case ALREADY_APPLIED -> new ProfileStatus(ProfileStatus.StatusClass.OK,
                    ProfileStatus.RetryDisposition.NEVER,
                    ProfileStatus.DurableResult.ALREADY_APPLIED, 0);
            case CONFLICT -> new ProfileStatus(ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT,
                    ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);
            case DURABILITY_UNKNOWN -> new ProfileStatus(ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN,
                    ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, ProfileStatus.DurableResult.UNKNOWN, 0);
            case STALE_OR_COMPACTED, NOT_FOUND -> new ProfileStatus(
                    ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                    ProfileStatus.RetryDisposition.AFTER_CONTROL_RECONCILIATION,
                    ProfileStatus.DurableResult.UNKNOWN, 0);
            case DENIED -> new ProfileStatus(ProfileStatus.StatusClass.UNAUTHORIZED,
                    ProfileStatus.RetryDisposition.NEVER,
                    ProfileStatus.DurableResult.NONE, 0);
            case TRANSIENT_RECONCILING -> new ProfileStatus(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                    ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, ProfileStatus.DurableResult.NONE, 0);
            case QUARANTINED -> new ProfileStatus(ProfileStatus.StatusClass.QUARANTINED_OR_UNKNOWN_MANDATORY,
                    ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.UNKNOWN, 0);
            case BAD_REQUEST -> new ProfileStatus(ProfileStatus.StatusClass.BAD_REQUEST,
                    ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);
            case UNSUPPORTED -> ProfileStatus.unsupported();
        };
    }

    private static byte[] text(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.indexOf(0) >= 0 || bytes.length > ProfileControlScope.MAX_AUTHORITY_DOMAIN_BYTES
                || !value.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw invalid();
        }
        return bytes;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid experimental initial control body");
    }
}

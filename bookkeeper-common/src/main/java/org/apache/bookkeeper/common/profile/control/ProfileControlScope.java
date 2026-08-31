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

import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_FIXED_LENGTH;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_GENERATION;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_LEDGER_ID;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_TEXT;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.NULL_FIELD;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.PURPOSE_MISMATCH;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.RANGE_FORBIDDEN;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.TEXT_TOO_LARGE;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.ZERO_IDENTIFIER;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;

/** Exact immutable tuple presented to static and committed-authority authorization. */
public final class ProfileControlScope {

    public static final int OPAQUE_ID_LENGTH = 16;
    public static final int PUBLIC_PAYLOAD_IDENTITY_LENGTH = 32;
    public static final int MAX_TARGET_BOOKIE_BYTES = 255;
    public static final int MAX_AUTHORITY_DOMAIN_BYTES = 128;

    private final ProfileControlOperation operation;
    private final ProfileAuthorityPurpose purpose;
    private final long ledgerId;
    private final byte[] ledgerInstanceId;
    private final ProfileDescriptorIdentity descriptorIdentity;
    private final String targetBookie;
    private final byte[] targetStorageIncarnation;
    private final String authorityDomain;
    private final long authorityGeneration;
    private final byte[] operationId;
    private final byte[] publicSemanticPayloadIdentity;
    private final ProfileExactRange exactRange;

    public ProfileControlScope(
            ProfileControlOperation operation,
            ProfileAuthorityPurpose purpose,
            long ledgerId,
            byte[] ledgerInstanceId,
            ProfileDescriptorIdentity descriptorIdentity,
            String targetBookie,
            byte[] targetStorageIncarnation,
            String authorityDomain,
            long authorityGeneration,
            byte[] operationId,
            byte[] publicSemanticPayloadIdentity,
            ProfileExactRange exactRange) {
        this.operation = requireNonNull(operation);
        this.purpose = requireNonNull(purpose);
        if (operation.purpose() != purpose) {
            throw new ProfileControlValidationException(PURPOSE_MISMATCH);
        }
        if (ledgerId < 0) {
            throw new ProfileControlValidationException(INVALID_LEDGER_ID);
        }
        if (authorityGeneration <= 0) {
            throw new ProfileControlValidationException(INVALID_GENERATION);
        }
        if (!operation.rangeAllowed() && exactRange != null) {
            throw new ProfileControlValidationException(RANGE_FORBIDDEN);
        }
        this.ledgerId = ledgerId;
        this.ledgerInstanceId = fixedNonZero(ledgerInstanceId, OPAQUE_ID_LENGTH);
        this.descriptorIdentity = requireNonNull(descriptorIdentity);
        this.targetBookie = boundedText(targetBookie, MAX_TARGET_BOOKIE_BYTES);
        this.targetStorageIncarnation = fixedNonZero(targetStorageIncarnation, OPAQUE_ID_LENGTH);
        this.authorityDomain = boundedText(authorityDomain, MAX_AUTHORITY_DOMAIN_BYTES);
        this.authorityGeneration = authorityGeneration;
        this.operationId = fixedNonZero(operationId, OPAQUE_ID_LENGTH);
        this.publicSemanticPayloadIdentity =
                fixed(publicSemanticPayloadIdentity, PUBLIC_PAYLOAD_IDENTITY_LENGTH);
        this.exactRange = exactRange;
    }

    public ProfileControlOperation operation() {
        return operation;
    }

    public ProfileAuthorityPurpose purpose() {
        return purpose;
    }

    public long ledgerId() {
        return ledgerId;
    }

    public byte[] ledgerInstanceId() {
        return ledgerInstanceId.clone();
    }

    public ProfileDescriptorIdentity descriptorIdentity() {
        return descriptorIdentity;
    }

    public String targetBookie() {
        return targetBookie;
    }

    public byte[] targetStorageIncarnation() {
        return targetStorageIncarnation.clone();
    }

    public String authorityDomain() {
        return authorityDomain;
    }

    public long authorityGeneration() {
        return authorityGeneration;
    }

    public byte[] operationId() {
        return operationId.clone();
    }

    public byte[] publicSemanticPayloadIdentity() {
        return publicSemanticPayloadIdentity.clone();
    }

    public ProfileExactRange exactRange() {
        return exactRange;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProfileControlScope that)) {
            return false;
        }
        return ledgerId == that.ledgerId
                && authorityGeneration == that.authorityGeneration
                && operation == that.operation
                && purpose == that.purpose
                && Arrays.equals(ledgerInstanceId, that.ledgerInstanceId)
                && descriptorIdentity.equals(that.descriptorIdentity)
                && targetBookie.equals(that.targetBookie)
                && Arrays.equals(targetStorageIncarnation, that.targetStorageIncarnation)
                && authorityDomain.equals(that.authorityDomain)
                && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(publicSemanticPayloadIdentity, that.publicSemanticPayloadIdentity)
                && Objects.equals(exactRange, that.exactRange);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                operation,
                purpose,
                ledgerId,
                descriptorIdentity,
                targetBookie,
                authorityDomain,
                authorityGeneration,
                exactRange);
        result = 31 * result + Arrays.hashCode(ledgerInstanceId);
        result = 31 * result + Arrays.hashCode(targetStorageIncarnation);
        result = 31 * result + Arrays.hashCode(operationId);
        result = 31 * result + Arrays.hashCode(publicSemanticPayloadIdentity);
        return result;
    }

    @Override
    public String toString() {
        return "ProfileControlScope{operation=" + operation
                + ", purpose=" + purpose
                + ", ledgerId=" + ledgerId
                + ", ledgerInstanceId=" + HexFormat.of().formatHex(ledgerInstanceId)
                + ", descriptorIdentity=" + descriptorIdentity
                + ", targetBookie='" + targetBookie + '\''
                + ", targetStorageIncarnation=" + HexFormat.of().formatHex(targetStorageIncarnation)
                + ", authorityDomain='" + authorityDomain + '\''
                + ", authorityGeneration=" + authorityGeneration
                + ", operationId=" + HexFormat.of().formatHex(operationId)
                + ", publicSemanticPayloadIdentity=" + HexFormat.of().formatHex(publicSemanticPayloadIdentity)
                + ", exactRange=" + exactRange
                + '}';
    }

    private static <T> T requireNonNull(T value) {
        if (value == null) {
            throw new ProfileControlValidationException(NULL_FIELD);
        }
        return value;
    }

    private static byte[] fixedNonZero(byte[] value, int length) {
        byte[] copy = fixed(value, length);
        for (byte current : copy) {
            if (current != 0) {
                return copy;
            }
        }
        throw new ProfileControlValidationException(ZERO_IDENTIFIER);
    }

    private static byte[] fixed(byte[] value, int length) {
        if (value == null || value.length != length) {
            throw new ProfileControlValidationException(INVALID_FIXED_LENGTH);
        }
        return value.clone();
    }

    private static String boundedText(String value, int maxBytes) {
        if (value == null || value.isBlank() || value.indexOf('\0') >= 0) {
            throw new ProfileControlValidationException(INVALID_TEXT);
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new ProfileControlValidationException(TEXT_TOO_LARGE);
        }
        return value;
    }
}

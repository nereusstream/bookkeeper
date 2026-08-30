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

package org.apache.bookkeeper.common.profile;

import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_IDENTITY;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** Immutable 36-byte SHA-256 suite identity for canonical descriptor bytes. */
public final class ProfileDescriptorIdentity {

    public static final int HASH_SUITE_ID = 1;
    public static final int DIGEST_LENGTH = 32;
    public static final int IDENTITY_LENGTH = 36;

    private static final byte[] DOMAIN_SEPARATOR =
            "org.apache.bookkeeper/ProfileDescriptor/v1\0".getBytes(StandardCharsets.US_ASCII);

    private final int hashSuiteId;
    private final int semanticSchemaVersion;
    private final byte[] digest;

    private ProfileDescriptorIdentity(int hashSuiteId, int semanticSchemaVersion, byte[] digest) {
        this.hashSuiteId = hashSuiteId;
        this.semanticSchemaVersion = semanticSchemaVersion;
        this.digest = digest.clone();
    }

    /** Computes suite 1 identity over bytes already accepted as canonical by the codec. */
    static ProfileDescriptorIdentity compute(byte[] canonicalBytes) {
        Objects.requireNonNull(canonicalBytes, "canonicalBytes");
        MessageDigest messageDigest = sha256();
        messageDigest.update(DOMAIN_SEPARATOR);
        messageDigest.update(ByteBuffer.allocate(8)
                .putShort((short) HASH_SUITE_ID)
                .putShort((short) ProfileDescriptorCodec.SEMANTIC_SCHEMA_VERSION)
                .putInt(canonicalBytes.length)
                .array());
        messageDigest.update(canonicalBytes);
        return new ProfileDescriptorIdentity(
                HASH_SUITE_ID,
                ProfileDescriptorCodec.SEMANTIC_SCHEMA_VERSION,
                messageDigest.digest());
    }

    /** Parses a frozen 36-byte identity and rejects unknown suite or schema discriminators. */
    public static ProfileDescriptorIdentity fromBytes(byte[] identityBytes) {
        if (identityBytes == null || identityBytes.length != IDENTITY_LENGTH) {
            throw new ProfileDescriptorValidationException(INVALID_IDENTITY);
        }
        ByteBuffer buffer = ByteBuffer.wrap(identityBytes);
        int hashSuiteId = Short.toUnsignedInt(buffer.getShort());
        int semanticSchemaVersion = Short.toUnsignedInt(buffer.getShort());
        if (hashSuiteId != HASH_SUITE_ID
                || semanticSchemaVersion != ProfileDescriptorCodec.SEMANTIC_SCHEMA_VERSION) {
            throw new ProfileDescriptorValidationException(INVALID_IDENTITY);
        }
        byte[] digest = new byte[DIGEST_LENGTH];
        buffer.get(digest);
        return new ProfileDescriptorIdentity(hashSuiteId, semanticSchemaVersion, digest);
    }

    public int hashSuiteId() {
        return hashSuiteId;
    }

    public int semanticSchemaVersion() {
        return semanticSchemaVersion;
    }

    public byte[] digest() {
        return digest.clone();
    }

    public byte[] toBytes() {
        return ByteBuffer.allocate(IDENTITY_LENGTH)
                .putShort((short) hashSuiteId)
                .putShort((short) semanticSchemaVersion)
                .put(digest)
                .array();
    }

    /** Constant-time comparison against the identity recomputed from canonical bytes. */
    boolean verifies(byte[] canonicalBytes) {
        return MessageDigest.isEqual(toBytes(), compute(canonicalBytes).toBytes());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProfileDescriptorIdentity that)) {
            return false;
        }
        return hashSuiteId == that.hashSuiteId
                && semanticSchemaVersion == that.semanticSchemaVersion
                && MessageDigest.isEqual(digest, that.digest);
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(hashSuiteId);
        result = 31 * result + Integer.hashCode(semanticSchemaVersion);
        return 31 * result + Arrays.hashCode(digest);
    }

    @Override
    public String toString() {
        return "ProfileDescriptorIdentity{hashSuiteId=" + hashSuiteId
                + ", semanticSchemaVersion=" + semanticSchemaVersion
                + ", digest=" + HexFormat.of().formatHex(digest) + '}';
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("required SHA-256 digest is unavailable", exception);
        }
    }
}

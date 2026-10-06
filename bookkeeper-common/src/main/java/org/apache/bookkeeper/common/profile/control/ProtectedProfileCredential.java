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

import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_CREDENTIAL;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/** Fixed kind-1 20-byte data credential whose public surfaces are always redacted. */
public final class ProtectedProfileCredential {

    public static final int BK_MASTER_KEY_SHA1 = 1;
    public static final int CREDENTIAL_LENGTH = 20;
    private static final byte[] STORAGE_DOMAIN =
            "BookKeeper/ProfileCredentialStorage/v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_BITS = 128;

    /** Encrypted local record field; plaintext is accessible only through this wrapper after reopening. */
    public static final class SealedStorageValue {
        public static final int LENGTH = 1 + NONCE_LENGTH + CREDENTIAL_LENGTH + TAG_BITS / 8;
        private final byte[] bytes;

        private SealedStorageValue(byte[] bytes) {
            this.bytes = bytes.clone();
        }

        public static SealedStorageValue fromBytes(byte[] bytes) {
            if (bytes == null || bytes.length != LENGTH) {
                throw new IllegalArgumentException("Invalid sealed credential length");
            }
            return new SealedStorageValue(bytes);
        }

        public byte[] toBytes() {
            return bytes.clone();
        }

        @Override
        public String toString() {
            return "<redacted>";
        }
    }

    private final int kind;
    private final byte[] credential;

    private ProtectedProfileCredential(int kind, byte[] credential) {
        this.kind = kind;
        this.credential = credential.clone();
    }

    public static ProtectedProfileCredential of(int kind, byte[] credential) {
        if (kind != BK_MASTER_KEY_SHA1 || credential == null || credential.length != CREDENTIAL_LENGTH) {
            throw new ProfileControlValidationException(INVALID_CREDENTIAL);
        }
        return new ProtectedProfileCredential(kind, credential);
    }

    public int kind() {
        return kind;
    }

    public int length() {
        return credential.length;
    }

    /** Returns a protected wrapper copy without exposing the credential bytes. */
    public ProtectedProfileCredential copy() {
        return new ProtectedProfileCredential(kind, credential);
    }

    /** Serialize only into an authenticated outbound wire buffer; callers must never log this destination. */
    public void writeWireCredential(ByteBuffer destination) {
        Objects.requireNonNull(destination, "destination").put(credential);
    }

    /** Constant-time comparison used by protected local state implementations. */
    public boolean matches(ProtectedProfileCredential other) {
        return other != null
                && kind == other.kind
                && MessageDigest.isEqual(credential, other.credential);
    }

    /** Compares a wire verifier without deriving a new binding or exposing the stored bytes. */
    public boolean matchesWireVerifier(byte[] candidate) {
        return candidate != null && candidate.length == CREDENTIAL_LENGTH
                && MessageDigest.isEqual(credential, candidate);
    }

    /**
     * Produces a protected credential field for the isolated Bookie control log.
     * The AES key must be supplied from outside that storage scope and retained across restarts.
     */
    public SealedStorageValue sealForStorage(SecretKey key, byte[] scopeBinding) {
        Objects.requireNonNull(key, "key");
        requireBinding(scopeBinding);
        try {
            byte[] nonce = new byte[NONCE_LENGTH];
            new SecureRandom().nextBytes(nonce);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(STORAGE_DOMAIN);
            cipher.updateAAD(scopeBinding);
            byte[] encrypted = cipher.doFinal(credential);
            byte[] encoded = new byte[SealedStorageValue.LENGTH];
            encoded[0] = 1;
            System.arraycopy(nonce, 0, encoded, 1, NONCE_LENGTH);
            System.arraycopy(encrypted, 0, encoded, 1 + NONCE_LENGTH, encrypted.length);
            return new SealedStorageValue(encoded);
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("Credential storage protection is unavailable", failure);
        }
    }

    public static ProtectedProfileCredential openFromStorage(
            SecretKey key, byte[] scopeBinding, SealedStorageValue sealed) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(sealed, "sealed");
        requireBinding(scopeBinding);
        byte[] encoded = sealed.bytes;
        if (encoded[0] != 1) {
            throw new IllegalArgumentException("Unknown sealed credential version");
        }
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, encoded, 1, NONCE_LENGTH));
            cipher.updateAAD(STORAGE_DOMAIN);
            cipher.updateAAD(scopeBinding);
            byte[] plaintext = cipher.doFinal(encoded, 1 + NONCE_LENGTH,
                    CREDENTIAL_LENGTH + TAG_BITS / 8);
            try {
                return of(BK_MASTER_KEY_SHA1, plaintext);
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("Sealed credential authentication failed");
        }
    }

    private static void requireBinding(byte[] scopeBinding) {
        if (scopeBinding == null || scopeBinding.length == 0) {
            throw new IllegalArgumentException("A storage-scope binding is required");
        }
    }

    @Override
    public String toString() {
        return "<redacted>";
    }
}

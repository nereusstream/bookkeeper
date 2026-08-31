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

import java.security.MessageDigest;

/** Fixed kind-1 20-byte data credential whose public surfaces are always redacted. */
public final class ProtectedProfileCredential {

    public static final int BK_MASTER_KEY_SHA1 = 1;
    public static final int CREDENTIAL_LENGTH = 20;

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

    /** Constant-time comparison used by protected local state implementations. */
    public boolean matches(ProtectedProfileCredential other) {
        return other != null
                && kind == other.kind
                && MessageDigest.isEqual(credential, other.credential);
    }

    @Override
    public String toString() {
        return "<redacted>";
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/** The current preparation attempt for one immutable manifest CLOSED admission. */
public record ProfileManifestPreparation(long admissionBirth, String admissionDigest, UUID operation,
        UUID firstNonce, UUID nonce) {
    private static final int MAGIC = 0x55574d50; // UWMP
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    static final int ENCODED_BYTES = 96;
    private static final HexFormat HEX = HexFormat.of();
    private static final UUID ZERO = new UUID(0, 0);

    public ProfileManifestPreparation {
        requireId(operation);
        requireId(firstNonce);
        requireId(nonce);
        if (admissionBirth <= 0 || admissionDigest.length() != 2 * DIGEST_BYTES
                || !HEX.formatHex(HEX.parseHex(admissionDigest)).equals(admissionDigest)) {
            throw new IllegalArgumentException("Invalid manifest preparation admission identity");
        }
    }

    public record Receipt(ProfileManifestClosedAdmission.Receipt admitted, ProfileManifestPreparation attempt,
                          long birth, long modified, int version) { }

    byte[] encode() {
        var out = ByteBuffer.allocate(ENCODED_BYTES);
        out.putInt(MAGIC).putInt(VERSION).putLong(admissionBirth).put(HEX.parseHex(admissionDigest));
        putId(out, operation);
        putId(out, firstNonce);
        putId(out, nonce);
        return out.array();
    }

    static ProfileManifestPreparation decode(byte[] bytes) throws IOException {
        if (bytes.length != ENCODED_BYTES) {
            throw new IOException("Invalid manifest preparation record extent");
        }
        try {
            var in = ByteBuffer.wrap(bytes);
            if (in.getInt() != MAGIC || in.getInt() != VERSION) {
                throw new IOException("Unknown manifest preparation format");
            }
            long birth = in.getLong();
            byte[] digest = new byte[DIGEST_BYTES];
            in.get(digest);
            return new ProfileManifestPreparation(birth, HEX.formatHex(digest), getId(in), getId(in), getId(in));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid manifest preparation record", invalid);
        }
    }

    private static void requireId(UUID id) {
        if (ZERO.equals(Objects.requireNonNull(id, "id"))) {
            throw new IllegalArgumentException("Manifest preparation requires nonzero identities");
        }
    }

    private static void putId(ByteBuffer out, UUID id) {
        out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }

    private static UUID getId(ByteBuffer in) {
        return new UUID(in.getLong(), in.getLong());
    }
}

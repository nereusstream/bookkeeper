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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Fixed experimental standard-metadata reference; it carries no descriptor, receipt or authority. */
public final class ProfileMetadataBacklink {
    public static final String KEY = "bookkeeper.experimental.profile.backlink";
    public static final int ENCODED_BYTES = 52;
    private static final int MAGIC = 0x5557424c;
    private final long ledgerId;
    private final byte[] instanceId;
    private final byte[] sidecarId;

    public ProfileMetadataBacklink(long ledgerId, byte[] instanceId, byte[] sidecarId) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("Invalid Profile backlink ledger");
        }
        this.ledgerId = ledgerId;
        this.instanceId = identifier(instanceId);
        this.sidecarId = identifier(sidecarId);
    }

    private static byte[] identifier(byte[] value) {
        Objects.requireNonNull(value, "identifier");
        if (value.length != 16 || Arrays.equals(value, new byte[16])) {
            throw new IllegalArgumentException("Invalid Profile backlink identifier");
        }
        return value.clone();
    }

    public long ledgerId() {
        return ledgerId;
    }

    public byte[] instanceId() {
        return instanceId.clone();
    }

    public byte[] sidecarId() {
        return sidecarId.clone();
    }

    public byte[] encode() {
        ByteBuffer out = ByteBuffer.allocate(ENCODED_BYTES);
        out.putInt(MAGIC).putInt(1).putLong(ledgerId).put(instanceId).put(sidecarId);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, ENCODED_BYTES - 4);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    public static ProfileMetadataBacklink decode(byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_BYTES) {
            throw new IllegalArgumentException("Invalid Profile backlink length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, ENCODED_BYTES - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(ENCODED_BYTES - 4) != (int) crc.getValue()
                || in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("Invalid Profile backlink encoding");
        }
        long ledger = in.getLong();
        byte[] instance = new byte[16];
        byte[] sidecar = new byte[16];
        in.get(instance).get(sidecar);
        return new ProfileMetadataBacklink(ledger, instance, sidecar);
    }

    /** Preserve all user entries and reject replacing an existing or malformed reserved reference. */
    public LedgerMetadata attachTo(LedgerMetadata metadata) {
        if (metadata.getLedgerId() != ledgerId) {
            throw new IllegalArgumentException("Profile backlink scope mismatch");
        }
        byte[] existing = metadata.getCustomMetadata().get(KEY);
        if (existing != null && !equals(decode(existing))) {
            throw new IllegalArgumentException("Profile backlink is immutable");
        }
        Map<String, byte[]> custom = new HashMap<>();
        metadata.getCustomMetadata().forEach((key, value) -> custom.put(key, value.clone()));
        custom.put(KEY, encode());
        return LedgerMetadataBuilder.from(metadata).withCToken(metadata.getCToken()).withCustomMetadata(custom).build();
    }

    public static ProfileMetadataBacklink from(LedgerMetadata metadata) {
        ProfileMetadataBacklink result = decode(metadata.getCustomMetadata().get(KEY));
        if (result.ledgerId != metadata.getLedgerId()) {
            throw new IllegalArgumentException("Profile backlink scope mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileMetadataBacklink that && ledgerId == that.ledgerId
                && Arrays.equals(instanceId, that.instanceId) && Arrays.equals(sidecarId, that.sidecarId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ledgerId, Arrays.hashCode(instanceId), Arrays.hashCode(sidecarId));
    }
}

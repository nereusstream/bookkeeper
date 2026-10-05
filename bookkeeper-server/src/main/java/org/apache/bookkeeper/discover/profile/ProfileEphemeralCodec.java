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
package org.apache.bookkeeper.discover.profile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;

/** Secret-free experimental ephemeral projection, bound to one publisher session. */
final class ProfileEphemeralCodec {
    private static final int MAGIC = 0x55574550;
    private static final int FIXED_BYTES = 80;

    record Envelope(UUID nonce, long sessionId, ProfileServiceInfoProjection projection) { }

    private ProfileEphemeralCodec() { }

    static byte[] encode(Envelope envelope) {
        ProfileServiceInfoProjection p = envelope.projection();
        byte[] name = canonical(p.bookieId());
        byte[] endpoint = canonical(p.endpoint());
        if (envelope.nonce().equals(new UUID(0, 0)) || envelope.sessionId() == 0
                || p.capabilityHints().size()
                > (ProfileReadinessCodec.MAX_BYTES - FIXED_BYTES - name.length - endpoint.length) / 4) {
            throw new IllegalArgumentException("Invalid ephemeral envelope");
        }
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + name.length + endpoint.length
                + 4 * p.capabilityHints().size());
        out.putInt(MAGIC).putInt(1).putLong(envelope.nonce().getMostSignificantBits())
                .putLong(envelope.nonce().getLeastSignificantBits()).putLong(envelope.sessionId())
                .putInt(name.length).put(name).put(p.storageIncarnation().value())
                .putLong(p.readinessGeneration().value()).putLong(p.persistentStoreVersion())
                .putInt(endpoint.length).put(endpoint).putInt(p.capabilityHints().size());
        p.capabilityHints().forEach(out::putInt);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.array();
    }

    static Envelope decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_BYTES + 6 || bytes.length > ProfileReadinessCodec.MAX_BYTES) {
            throw new IllegalArgumentException("Invalid ephemeral envelope length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("Invalid ephemeral envelope identity or checksum");
        }
        in.limit(bytes.length - 4);
        UUID nonce = new UUID(in.getLong(), in.getLong());
        long session = in.getLong();
        if (nonce.equals(new UUID(0, 0)) || session == 0) {
            throw new IllegalArgumentException("Invalid publisher identity");
        }
        String name = text(in, 45);
        byte[] storage = new byte[StorageIncarnation.LENGTH];
        in.get(storage);
        Generation generation = new Generation(in.getLong());
        long version = in.getLong();
        String endpoint = text(in, 8);
        int count = in.getInt();
        if (count < 1 || count > in.remaining() / 4 || in.remaining() != 4 * count) {
            throw new IllegalArgumentException("Invalid ephemeral capability count");
        }
        ArrayList<Integer> hints = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hints.add(in.getInt());
        }
        return new Envelope(nonce, session, new ProfileServiceInfoProjection(name,
                new StorageIncarnation(storage), generation, version, endpoint, hints));
    }

    private static String text(ByteBuffer in, int minimumTail) {
        int length = in.getInt();
        if (length < 1 || length > 255 || length > in.remaining() - minimumTail) {
            throw new IllegalArgumentException("Invalid ephemeral text length");
        }
        byte[] value = new byte[length];
        in.get(value);
        String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(value, result.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Non-canonical ephemeral text");
        }
        return result;
    }

    private static byte[] canonical(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Non-canonical ephemeral text");
        }
        return bytes;
    }
}

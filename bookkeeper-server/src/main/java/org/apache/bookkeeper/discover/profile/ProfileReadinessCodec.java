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
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;

/** Experimental persistent readiness envelope; no credential or service registration authority. */
final class ProfileReadinessCodec {
    static final int MAX_BYTES = 65536;
    private static final int FIXED_BYTES = 148;
    private static final int MAGIC = 0x55575244;

    record Envelope(UUID birthId, ProfileReadinessRecord record) { }

    private ProfileReadinessCodec() { }

    static byte[] encode(Envelope envelope) {
        ProfileReadinessRecord record = envelope.record();
        byte[] name = record.bookieId().getBytes(StandardCharsets.UTF_8);
        int count = record.capabilityHints().size();
        if (envelope.birthId().equals(new UUID(0, 0))
                || !record.bookieId().equals(new String(name, StandardCharsets.UTF_8))
                || count > (MAX_BYTES - FIXED_BYTES - name.length) / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid or oversized readiness envelope");
        }
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + name.length + count * Integer.BYTES);
        out.putInt(MAGIC).putInt(1).putLong(envelope.birthId().getMostSignificantBits())
                .putLong(envelope.birthId().getLeastSignificantBits()).putInt(name.length).put(name)
                .put(record.storageIncarnation().value()).putInt(record.engine().wireValue())
                .putInt(record.protocolMajor()).putInt(record.protocolMinor())
                .putLong(record.capabilityGeneration().value()).putLong(record.localFormatGeneration().value())
                .put(record.deviceManifestIdentity().value()).putLong(record.deviceManifestGeneration().value())
                .putLong(record.effectiveAssignmentGeneration().value())
                .putLong(record.localReadinessGeneration().value())
                .putInt(record.minimumReaderVersion()).putInt(record.minimumWriterVersion()).putInt(count);
        record.capabilityHints().forEach(out::putInt);
        out.putInt(record.state() == ProfileReadinessRecord.State.READY ? 1 : 2);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.array();
    }

    static Envelope decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_BYTES + 1 || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid readiness envelope length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - Integer.BYTES) != (int) crc.getValue()
                || in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("Invalid readiness envelope identity or checksum");
        }
        UUID birth = new UUID(in.getLong(), in.getLong());
        int nameLength = in.getInt();
        if (birth.equals(new UUID(0, 0)) || nameLength < 1
                || nameLength > BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES
                || nameLength > in.remaining() - (FIXED_BYTES - 28)) {
            throw new IllegalArgumentException("Invalid readiness scope");
        }
        byte[] name = new byte[nameLength];
        in.get(name);
        String bookie = new String(name, StandardCharsets.UTF_8);
        if (!Arrays.equals(name, bookie.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Non-canonical readiness scope");
        }
        byte[] storage = new byte[StorageIncarnation.LENGTH];
        in.get(storage);
        EngineProfile engine = switch (in.getInt()) {
            case 1 -> EngineProfile.CLASSIC_ENGINE;
            case 2 -> EngineProfile.DIRECT_JOURNAL_ENGINE;
            case 3 -> EngineProfile.SEGMENT_WAL_ENGINE;
            default -> throw new IllegalArgumentException("Unknown readiness engine");
        };
        int major = in.getInt();
        int minor = in.getInt();
        Generation capability = new Generation(in.getLong());
        Generation format = new Generation(in.getLong());
        byte[] manifest = new byte[ManifestIdentity.LENGTH];
        in.get(manifest);
        Generation device = new Generation(in.getLong());
        Generation assignment = new Generation(in.getLong());
        Generation readiness = new Generation(in.getLong());
        int reader = in.getInt();
        int writer = in.getInt();
        int count = in.getInt();
        if (count < 1 || count > (in.remaining() - 8) / Integer.BYTES
                || in.remaining() != count * Integer.BYTES + 8) {
            throw new IllegalArgumentException("Invalid readiness capability count");
        }
        List<Integer> hints = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            hints.add(in.getInt());
        }
        ProfileReadinessRecord.State state = switch (in.getInt()) {
            case 1 -> ProfileReadinessRecord.State.READY;
            case 2 -> ProfileReadinessRecord.State.NON_WRITABLE;
            default -> throw new IllegalArgumentException("Unknown readiness state");
        };
        return new Envelope(birth, new ProfileReadinessRecord(bookie, new StorageIncarnation(storage), engine,
                major, minor, capability, format, new ManifestIdentity(manifest), device, assignment,
                readiness, reader, writer, hints, state));
    }
}

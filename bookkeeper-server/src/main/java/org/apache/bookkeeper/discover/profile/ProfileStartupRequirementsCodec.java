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
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;

/** Bounded experimental declared startup requirements. Device READY here is a requirement, not an observation. */
final class ProfileStartupRequirementsCodec {
    static final int MAX_BYTES = 4096;
    static final int SINGLE_ARENA = 1;
    static final int FIXED_MANIFEST = 2;
    private static final int MAX_MANIFEST_BYTES = 32 * 1024;
    private static final int MAX_ARENAS = 64;
    private static final int MAGIC = 0x55575351; // UWSQ
    record Envelope(int schema, UUID birth, BookieCompatibilityFact compatibility, RequiredDeviceManifest devices) {
        Envelope(UUID birth, BookieCompatibilityFact compatibility, RequiredDeviceManifest devices) {
            this(SINGLE_ARENA, birth, compatibility, devices);
        }
    }

    private ProfileStartupRequirementsCodec() { }

    static byte[] encode(Envelope envelope) {
        var c = envelope.compatibility();
        var devices = envelope.devices();
        int count = devices.devices().size();
        int schema = envelope.schema();
        if (envelope.birth().equals(new UUID(0, 0))
                || (schema == SINGLE_ARENA ? count != 1 : schema != FIXED_MANIFEST || count > MAX_ARENAS)
                || !c.deviceManifestIdentity().equals(devices.identity())
                || !c.deviceManifestGeneration().equals(devices.generation())) {
            throw new IllegalArgumentException("Invalid initial startup requirements");
        }
        ByteBuffer out = ByteBuffer.allocate(schema == SINGLE_ARENA ? MAX_BYTES : MAX_MANIFEST_BYTES);
        out.putInt(MAGIC).putInt(schema).putLong(envelope.birth().getMostSignificantBits())
                .putLong(envelope.birth().getLeastSignificantBits());
        text(out, c.bookieId(), BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
        out.put(c.storageIncarnation().value()).putInt(c.activeEngine().wireValue())
                .putInt(c.profileWireMajor()).putInt(c.profileWireMinor()).putInt(c.descriptorHashSuite());
        ids(out, c.mandatoryLocalFeatures());
        out.put(c.deviceManifestIdentity().value()).putLong(c.deviceManifestGeneration().value())
                .putLong(c.localFormatGeneration().value()).putLong(c.effectiveDeleteAssignmentGeneration().value())
                .putInt(c.minimumReaderVersion()).putInt(c.minimumWriterVersion())
                .putInt(switch (c.migrationState()) {
                    case PREPARED -> 1;
                    case FORMAT_READY -> 2;
                    case QUARANTINED -> 3;
                    case UNKNOWN_MANDATORY -> 4;
                }).putLong(c.migrationGeneration().value());
        if (schema == FIXED_MANIFEST) {
            out.putInt(count);
        }
        for (var d : devices.devices()) {
            if (d.status() != RequiredDeviceManifest.DeviceStatus.READY
                    || !d.storageIncarnation().equals(c.storageIncarnation())
                    || !d.manifestGeneration().equals(devices.generation())) {
                throw new IllegalArgumentException("Invalid expected device scope");
            }
            text(out, d.deviceId(), RequiredDeviceManifest.RequiredDeviceFact.MAX_DEVICE_ID_BYTES);
            out.put(d.storageIncarnation().value()).putLong(d.manifestGeneration().value())
                    .putInt(d.arenaFormatVersion()).putInt(d.controlFormatVersion()).putInt(d.checkpointFormatVersion())
                    .putLong(d.superblockGeneration().value());
            ids(out, d.mandatoryFeatures());
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return Arrays.copyOf(out.array(), out.position());
    }

    static Envelope decode(byte[] bytes) {
        if (bytes == null || bytes.length < 184 || bytes.length > MAX_MANIFEST_BYTES) {
            throw new IllegalArgumentException("Invalid startup requirements size");
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        int schema = in.getInt(4);
        if (schema == SINGLE_ARENA ? bytes.length > MAX_BYTES : schema != FIXED_MANIFEST) {
            throw new IllegalArgumentException("Unsupported startup requirements schema or length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        if (in.getInt(bytes.length - Integer.BYTES) != (int) crc.getValue()
                || in.getInt() != MAGIC) {
            throw new IllegalArgumentException("Invalid startup requirements envelope");
        }
        in.position(8);
        try {
            var birth = new UUID(in.getLong(), in.getLong());
            String bookie = text(in, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
            var incarnation = new StorageIncarnation(fixed(in, StorageIncarnation.LENGTH));
            if (in.getInt() != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()) {
                throw new IllegalArgumentException("Unsupported startup engine");
            }
            int major = in.getInt();
            int minor = in.getInt();
            int suite = in.getInt();
            var features = ids(in);
            var manifest = new ManifestIdentity(fixed(in, ManifestIdentity.LENGTH));
            var deviceGeneration = new Generation(in.getLong());
            var formatGeneration = new Generation(in.getLong());
            var assignment = new Generation(in.getLong());
            int reader = in.getInt();
            int writer = in.getInt();
            var state = switch (in.getInt()) {
                case 1 -> BookieCompatibilityFact.MigrationState.PREPARED;
                case 2 -> BookieCompatibilityFact.MigrationState.FORMAT_READY;
                case 3 -> BookieCompatibilityFact.MigrationState.QUARANTINED;
                case 4 -> BookieCompatibilityFact.MigrationState.UNKNOWN_MANDATORY;
                default -> throw new IllegalArgumentException("Unknown startup migration state");
            };
            var migration = new Generation(in.getLong());
            var c = new BookieCompatibilityFact(bookie, incarnation, EngineProfile.SEGMENT_WAL_ENGINE, major, minor,
                    suite, features, manifest, deviceGeneration, formatGeneration, assignment, reader, writer,
                    state, migration);
            int count = schema == SINGLE_ARENA ? 1 : in.getInt();
            if (count < 1 || count > MAX_ARENAS) {
                throw new IllegalArgumentException("Invalid fixed manifest device count");
            }
            var devices = new ArrayList<RequiredDeviceManifest.RequiredDeviceFact>(count);
            for (int i = 0; i < count; i++) {
                devices.add(new RequiredDeviceManifest.RequiredDeviceFact(
                        text(in, RequiredDeviceManifest.RequiredDeviceFact.MAX_DEVICE_ID_BYTES),
                        new StorageIncarnation(fixed(in, StorageIncarnation.LENGTH)), new Generation(in.getLong()),
                        RequiredDeviceManifest.DeviceStatus.READY, in.getInt(), in.getInt(), in.getInt(),
                        new Generation(in.getLong()), ids(in)));
            }
            var result = new Envelope(schema, birth, c,
                    new RequiredDeviceManifest(manifest, deviceGeneration, devices));
            if (in.remaining() != Integer.BYTES || !Arrays.equals(bytes, encode(result))) {
                throw new IllegalArgumentException("Non-canonical startup requirements");
            }
            return result;
        } catch (java.nio.BufferUnderflowException invalid) {
            throw new IllegalArgumentException("Truncated startup requirements", invalid);
        }
    }

    private static byte[] fixed(ByteBuffer in, int count) {
        byte[] value = new byte[count];
        in.get(value);
        return value;
    }

    private static void text(ByteBuffer out, String text, int max) {
        byte[] value = text.getBytes(StandardCharsets.UTF_8);
        if (value.length < 1 || value.length > max || !text.equals(new String(value, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid startup text");
        }
        out.putInt(value.length).put(value);
    }

    private static String text(ByteBuffer in, int max) {
        int count = in.getInt();
        if (count < 1 || count > max || count > in.remaining()) {
            throw new IllegalArgumentException("Invalid startup text length");
        }
        byte[] value = fixed(in, count);
        String text = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(value, text.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Non-canonical startup text");
        }
        return text;
    }

    private static void ids(ByteBuffer out, List<Integer> ids) {
        if (ids.size() < 1 || ids.size() > 64) {
            throw new IllegalArgumentException("Invalid initial startup feature count");
        }
        out.putInt(ids.size());
        ids.forEach(out::putInt);
    }

    private static List<Integer> ids(ByteBuffer in) {
        int count = in.getInt();
        if (count < 1 || count > 64 || count > in.remaining() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid startup feature count");
        }
        List<Integer> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(in.getInt());
        }
        return ids;
    }
}

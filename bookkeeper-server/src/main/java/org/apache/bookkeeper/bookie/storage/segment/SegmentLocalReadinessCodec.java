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
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.DurableLocalReadiness;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;

/** Bounded experimental local receipt, independent of control/DATA and wire formats. */
final class SegmentLocalReadinessCodec {
    static final int FIXED_BYTES = 112;
    static final int MAX_BYTES = FIXED_BYTES + 255;
    private static final long MAGIC = 0x55574c5244303031L;

    private SegmentLocalReadinessCodec() { }

    static byte[] name(String bookie) {
        byte[] bytes = bookie.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > 255 || bookie.indexOf(0) >= 0
                || !bookie.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid local readiness scope");
        }
        return bytes;
    }

    static ByteBuffer encode(DurableLocalReadiness record) {
        byte[] name = name(record.bookieId());
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + name.length);
        out.putLong(MAGIC).putInt(1).putInt(out.capacity()).putInt(name.length).put(name)
                .put(record.storageIncarnation().value()).put(record.deviceManifestIdentity().value())
                .putLong(record.deviceManifestGeneration().value()).putLong(record.localFormatGeneration().value())
                .putLong(record.effectiveDeleteAssignmentGeneration().value())
                .putLong(record.readinessGeneration().value())
                .putInt(record.status() == DurableLocalReadiness.Status.READY ? 1 : 2)
                .putInt(record.durable() ? 1 : 0);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.flip();
    }

    static DurableLocalReadiness decode(ByteBuffer input) throws IOException {
        ByteBuffer in = input.slice();
        int length = in.remaining();
        if (length < FIXED_BYTES + 1 || length > MAX_BYTES) {
            throw new IOException("Invalid local readiness length");
        }
        CRC32C crc = new CRC32C();
        crc.update(in.slice(0, length - 4));
        if (in.getInt(length - 4) != (int) crc.getValue()
                || in.getLong() != MAGIC || in.getInt() != 1 || in.getInt() != length) {
            throw new IOException("Invalid local readiness envelope or checksum");
        }
        int nameLength = in.getInt();
        if (nameLength < 1 || nameLength > 255 || length != FIXED_BYTES + nameLength) {
            throw new IOException("Invalid local readiness scope length");
        }
        byte[] name = new byte[nameLength];
        in.get(name);
        String bookie = new String(name, StandardCharsets.UTF_8);
        if (!Arrays.equals(name, bookie.getBytes(StandardCharsets.UTF_8))) {
            throw new IOException("Non-canonical local readiness scope");
        }
        byte[] incarnation = new byte[StorageIncarnation.LENGTH];
        byte[] manifest = new byte[ManifestIdentity.LENGTH];
        in.get(incarnation).get(manifest);
        try {
            Generation device = new Generation(in.getLong());
            Generation format = new Generation(in.getLong());
            Generation delete = new Generation(in.getLong());
            Generation readiness = new Generation(in.getLong());
            DurableLocalReadiness.Status status = switch (in.getInt()) {
                case 1 -> DurableLocalReadiness.Status.READY;
                case 2 -> DurableLocalReadiness.Status.QUARANTINED;
                default -> throw new IllegalArgumentException("Unknown local readiness status");
            };
            boolean durable = switch (in.getInt()) {
                case 0 -> false;
                case 1 -> true;
                default -> throw new IllegalArgumentException("Unknown local readiness durability");
            };
            return new DurableLocalReadiness(bookie, new StorageIncarnation(incarnation),
                    new ManifestIdentity(manifest), device, format, delete, readiness, status, durable);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid local readiness facts", invalid);
        }
    }
}

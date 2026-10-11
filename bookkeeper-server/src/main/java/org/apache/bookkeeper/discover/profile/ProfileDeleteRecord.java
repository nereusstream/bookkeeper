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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;

/** Experimental same-record membership seal and durable delete target obligation; no physical result. */
public final class ProfileDeleteRecord {
    public static final String SEAL_KEY = ProfileLedgerMetadataStore.MEMBERSHIP_FROZEN_KEY;
    public static final String TOMBSTONE_KEY = "bookkeeper.experimental.profile.deleted";
    static final int MAX_TARGETS = ProfileReservation.MAX_INITIAL_TARGETS
            * (ProfileReplacementPage.MAX_PUBLICATIONS + 1) + ProfileRepairIntent.MAX_INTENTS;
    static final int MAX_BYTES = 512 * 1024;
    static final int MANIFEST_PREFIX_BYTES = 8 + ProfileLedgerContext.ENCODED_LENGTH + 88 + 4 + 4;

    private ProfileDeleteRecord() { }

    public record Seal(byte[] requestId, long epoch, int beforeVersion, long metadataBirth,
            long lifecycleBirth, byte[] closedDigest) {
        public Seal {
            requestId = ProfileLifecycleHead.fixed(requestId, 16, true);
            closedDigest = ProfileLifecycleHead.fixed(closedDigest, 32, false);
            if (epoch <= 0 || beforeVersion < 0 || beforeVersion > Integer.MAX_VALUE - 2
                    || metadataBirth <= 0 || lifecycleBirth <= 0) {
                throw new IllegalArgumentException("Invalid Profile deletion seal");
            }
        }
        @Override
        public byte[] requestId() {
            return requestId.clone();
        }
        @Override
        public byte[] closedDigest() {
            return closedDigest.clone();
        }
        public byte[] encode() {
            return finish(header(88, 0x55574453).put(requestId).putLong(epoch).putInt(beforeVersion)
                    .putLong(metadataBirth).putLong(lifecycleBirth).put(closedDigest));
        }
        public static Seal decode(byte[] bytes) {
            var input = checked(bytes, 88, 0x55574453);
            byte[] request = new byte[16];
            input.get(request);
            long epoch = input.getLong();
            int version = input.getInt();
            long birth = input.getLong();
            long root = input.getLong();
            byte[] digest = new byte[32];
            input.get(digest);
            return new Seal(request, epoch, version, birth, root, digest);
        }
        boolean matches(ZKProfileReservationStore.Snapshot root) {
            return root.birthZxid() == lifecycleBirth
                    && root.lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT
                    && root.lifecycle().controlGeneration() == epoch
                    && Arrays.equals(root.lifecycle().operationId(), requestId);
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Seal seal && Arrays.equals(encode(), seal.encode());
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }
    }

    public record Target(String bookie, byte[] incarnation) implements Comparable<Target> {
        public Target {
            ProfileAuthorityCodec.text(bookie, ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
            incarnation = ProfileLifecycleHead.fixed(incarnation, 16, true);
        }
        @Override
        public byte[] incarnation() {
            return incarnation.clone();
        }
        @Override
        public int compareTo(Target other) {
            int compared = Arrays.compareUnsigned(bookie.getBytes(StandardCharsets.UTF_8),
                    other.bookie.getBytes(StandardCharsets.UTF_8));
            return compared == 0 ? Arrays.compareUnsigned(incarnation, other.incarnation) : compared;
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Target target && bookie.equals(target.bookie)
                    && Arrays.equals(incarnation, target.incarnation);
        }
        @Override
        public int hashCode() {
            return 31 * bookie.hashCode() + Arrays.hashCode(incarnation);
        }
    }

    public record Manifest(ProfileLedgerContext ledger, Seal seal, List<Target> targets) {
        public Manifest {
            java.util.Objects.requireNonNull(ledger, "ledger");
            java.util.Objects.requireNonNull(seal, "seal");
            targets = List.copyOf(targets);
            if (targets.isEmpty() || targets.size() > MAX_TARGETS) {
                throw new IllegalArgumentException("Deletion target set exceeds its bounded history");
            }
            for (int index = 1; index < targets.size(); index++) {
                if (targets.get(index - 1).compareTo(targets.get(index)) >= 0) {
                    throw new IllegalArgumentException("Deletion targets must be unique and canonically ordered");
                }
            }
        }
        public byte[] encode() {
            int size = MANIFEST_PREFIX_BYTES;
            for (var target : targets) {
                size += 18 + target.bookie.getBytes(StandardCharsets.UTF_8).length;
            }
            if (size > MAX_BYTES) {
                throw new IllegalArgumentException("Deletion manifest exceeds its byte bound");
            }
            var output = header(size, 0x5557444d).put(ledger.encode()).put(seal.encode()).putInt(targets.size());
            for (var target : targets) {
                byte[] name = target.bookie.getBytes(StandardCharsets.UTF_8);
                output.putShort((short) name.length).put(name).put(target.incarnation);
            }
            return finish(output);
        }
        public static Manifest decode(byte[] bytes) {
            if (bytes == null || bytes.length < MANIFEST_PREFIX_BYTES + 19 || bytes.length > MAX_BYTES) {
                throw new IllegalArgumentException("Invalid bounded deletion manifest length");
            }
            var input = checked(bytes, bytes.length, 0x5557444d);
            var ledger = ProfileLedgerContext.decode(input);
            byte[] sealBytes = new byte[88];
            input.get(sealBytes);
            var seal = Seal.decode(sealBytes);
            int count = input.getInt();
            if (count < 1 || count > MAX_TARGETS || input.remaining() < count * 19 + 4) {
                throw new IllegalArgumentException("Invalid deletion target count before allocation");
            }
            var targets = new java.util.ArrayList<Target>(count);
            for (int index = 0; index < count; index++) {
                int length = input.getShort() & 0xffff;
                if (length < 1 || length > ProfileControlScope.MAX_TARGET_BOOKIE_BYTES
                        || input.remaining() < length + 16 + (count - index - 1) * 19 + 4) {
                    throw new IllegalArgumentException("Invalid deletion target name length");
                }
                byte[] name = new byte[length];
                input.get(name);
                String bookie = new String(name, StandardCharsets.UTF_8);
                if (!Arrays.equals(name, bookie.getBytes(StandardCharsets.UTF_8))) {
                    throw new IllegalArgumentException("Noncanonical deletion target UTF-8");
                }
                byte[] incarnation = new byte[16];
                input.get(incarnation);
                targets.add(new Target(bookie, incarnation));
            }
            if (input.remaining() != 4) {
                throw new IllegalArgumentException("Trailing deletion manifest bytes");
            }
            return new Manifest(ledger, seal, targets);
        }
    }

    public record Reference(long birth, byte[] digest, byte[] sealDigest) {
        public Reference {
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            sealDigest = ProfileLifecycleHead.fixed(sealDigest, 32, false);
            if (birth <= 0) {
                throw new IllegalArgumentException("Invalid deletion manifest birth");
            }
        }
        @Override
        public byte[] digest() {
            return digest.clone();
        }
        @Override
        public byte[] sealDigest() {
            return sealDigest.clone();
        }
        public byte[] encode() {
            return finish(header(88, 0x55574454).putLong(birth).put(digest).put(sealDigest).putInt(0));
        }
        public static Reference decode(byte[] bytes) {
            var input = checked(bytes, 88, 0x55574454);
            long birth = input.getLong();
            byte[] digest = new byte[32];
            byte[] seal = new byte[32];
            input.get(digest).get(seal);
            if (input.getInt() != 0) {
                throw new IllegalArgumentException("Invalid deletion reference reserved bytes");
            }
            return new Reference(birth, digest, seal);
        }
    }

    static LedgerMetadata strip(LedgerMetadata value) {
        var custom = new java.util.TreeMap<>(value.getCustomMetadata());
        custom.remove(SEAL_KEY);
        custom.remove(TOMBSTONE_KEY);
        return LedgerMetadataBuilder.from(value).withCToken(value.getCToken()).withCustomMetadata(custom).build();
    }

    static byte[] closedDigest(LedgerMetadata value) throws IOException {
        var domain = "BookKeeper/ProfileDeleteClosedMetadata/v1\0".getBytes(StandardCharsets.US_ASCII);
        byte[] bytes = new LedgerMetadataSerDe().serialize(strip(value));
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            digest.update(domain);
            return digest.digest(bytes);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static ByteBuffer header(int size, int magic) {
        return ByteBuffer.allocate(size).putInt(magic).putShort((short) 1).putShort((short) 0);
    }

    private static byte[] finish(ByteBuffer output) {
        if (output.position() != output.capacity() - 4) {
            throw new IllegalArgumentException("Incomplete deletion record body");
        }
        var crc = new CRC32C();
        crc.update(output.array(), 0, output.capacity() - 4);
        return output.putInt((int) crc.getValue()).array();
    }

    private static ByteBuffer checked(byte[] bytes, int size, int magic) {
        if (bytes == null || bytes.length != size) {
            throw new IllegalArgumentException("Invalid deletion record width");
        }
        var crc = new CRC32C();
        crc.update(bytes, 0, size - 4);
        var input = ByteBuffer.wrap(bytes);
        if (input.getInt(size - 4) != (int) crc.getValue() || input.getInt() != magic
                || input.getShort() != 1 || input.getShort() != 0) {
            throw new IllegalArgumentException("Invalid deletion record schema/CRC");
        }
        return input;
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;

/** Immutable inverse/digest receipt for one guarded repair membership CAS, never a second membership authority. */
public record ProfileRepairCommit(int beforeVersion, byte[] beforeDigest, byte[] afterDigest,
        String oldBookie, byte[] closeOperationId, long closeGeneration) {
    public static final String KEY = "bookkeeper.experimental.profile.repair-commits";
    public static final int MAX_BYTES = 361;
    private static final int MAGIC = 0x5557524d; // UWRM, distinct from the UWRC recovery context
    private static final int HEAD_MAGIC = 0x55575248;

    public ProfileRepairCommit {
        beforeDigest = ProfileLifecycleHead.fixed(beforeDigest, 32, false);
        afterDigest = ProfileLifecycleHead.fixed(afterDigest, 32, false);
        closeOperationId = ProfileLifecycleHead.fixed(closeOperationId, 16, true);
        ProfileAuthorityCodec.text(oldBookie, 255);
        if (beforeVersion < 0 || beforeVersion == Integer.MAX_VALUE || closeGeneration <= 0) {
            throw new IllegalArgumentException("Invalid repair commit version/generation");
        }
    }

    @Override
    public byte[] beforeDigest() {
        return beforeDigest.clone();
    }
    @Override
    public byte[] afterDigest() {
        return afterDigest.clone();
    }
    @Override
    public byte[] closeOperationId() {
        return closeOperationId.clone();
    }

    public record Reference(int slot, UUID intentBirth, long intentStoreVersion, byte[] digest) {
        public Reference {
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            if (slot < 0 || slot >= ProfileRepairIntent.MAX_INTENTS || intentStoreVersion < 0 || intentBirth == null
                    || intentBirth.equals(new UUID(0, 0))) {
                throw new IllegalArgumentException("Invalid repair commit reference");
            }
        }
        @Override
        public byte[] digest() {
            return digest.clone();
        }
    }

    public byte[] encode() {
        byte[] old = ProfileAuthorityCodec.text(oldBookie, 255);
        var out = ByteBuffer.allocate(106 + old.length).putInt(MAGIC).putShort((short) 1).putShort((short) 0)
                .putInt(beforeVersion).put(beforeDigest).put(afterDigest).put(closeOperationId)
                .putLong(closeGeneration).putShort((short) old.length).put(old);
        return finish(out);
    }

    public static ProfileRepairCommit decode(byte[] bytes) {
        var in = checked(bytes, 107, MAX_BYTES, MAGIC);
        int version = in.getInt();
        byte[] before = new byte[32];
        byte[] after = new byte[32];
        byte[] operation = new byte[16];
        in.get(before).get(after).get(operation);
        long generation = in.getLong();
        int size = Short.toUnsignedInt(in.getShort());
        if (size < 1 || size > 255 || size != in.remaining() - 4) {
            throw new IllegalArgumentException("Invalid repair commit old member length");
        }
        byte[] old = new byte[size];
        in.get(old);
        String member = new String(old, StandardCharsets.UTF_8);
        if (!Arrays.equals(old, ProfileAuthorityCodec.text(member, 255))) {
            throw new IllegalArgumentException("Noncanonical repair commit member");
        }
        return new ProfileRepairCommit(version, before, after, member, operation, generation);
    }

    public static List<Reference> head(byte[] bytes) {
        if (bytes == null) {
            return List.of();
        }
        var in = checked(bytes, 76, 16 + 60 * ProfileRepairIntent.MAX_INTENTS, HEAD_MAGIC);
        int count = in.getInt();
        if (count < 1 || count > ProfileRepairIntent.MAX_INTENTS || in.remaining() != 60 * count + 4) {
            throw new IllegalArgumentException("Invalid bounded repair commit head");
        }
        List<Reference> result = new ArrayList<>();
        var slots = new HashSet<Integer>();
        for (int i = 0; i < count; i++) {
            int slot = in.getInt();
            UUID birth = new UUID(in.getLong(), in.getLong());
            long storeVersion = in.getLong();
            byte[] digest = new byte[32];
            in.get(digest);
            if (!slots.add(slot)) {
                throw new IllegalArgumentException("Repeated repair slot in commit head");
            }
            result.add(new Reference(slot, birth, storeVersion, digest));
        }
        return List.copyOf(result);
    }

    public static byte[] head(List<Reference> references) {
        if (references.isEmpty() || references.size() > ProfileRepairIntent.MAX_INTENTS) {
            throw new IllegalArgumentException("Invalid bounded repair commit head count");
        }
        var out = ByteBuffer.allocate(16 + 60 * references.size()).putInt(HEAD_MAGIC)
                .putShort((short) 1).putShort((short) 0).putInt(references.size());
        var slots = new HashSet<Integer>();
        for (var reference : references) {
            if (!slots.add(reference.slot())) {
                throw new IllegalArgumentException("Repeated repair commit slot");
            }
            out.putInt(reference.slot()).putLong(reference.intentBirth().getMostSignificantBits())
                    .putLong(reference.intentBirth().getLeastSignificantBits()).putLong(reference.intentStoreVersion())
                    .put(reference.digest());
        }
        return finish(out);
    }

    static LedgerMetadata replace(LedgerMetadata input, ProfileRepairIntent intent, String member) {
        var ensemble = new ArrayList<>(input.getAllEnsembles().get(intent.fragmentStart()));
        ensemble.set(intent.replacedMemberSlot(), BookieId.parse(member));
        if (new HashSet<>(ensemble).size() != ensemble.size()) {
            throw new IllegalArgumentException("Repair would duplicate an ensemble member");
        }
        return LedgerMetadataBuilder.from(input).withCToken(input.getCToken())
                .replaceEnsembleEntry(intent.fragmentStart(), ensemble).build();
    }

    private static ByteBuffer checked(byte[] bytes, int minimum, int maximum, int magic) {
        if (bytes == null || bytes.length < minimum || bytes.length > maximum) {
            throw new IllegalArgumentException("Invalid repair commit byte bound");
        }
        var in = ByteBuffer.wrap(bytes);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != magic
                || in.getShort() != 1 || in.getShort() != 0) {
            throw new IllegalArgumentException("Invalid repair commit schema/CRC");
        }
        return in;
    }

    private static byte[] finish(ByteBuffer out) {
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }
}

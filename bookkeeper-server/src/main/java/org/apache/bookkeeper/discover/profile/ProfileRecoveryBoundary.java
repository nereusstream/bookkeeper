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
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.api.LedgerMetadata;

/** Fixed operation selection. Read proof fixes the range; recovery Adds and matching CLOSED remain separate. */
public record ProfileRecoveryBoundary(ProfileRecoveryContext context, int beforeVersion, byte[] metadataDigest,
        long requiredFrontier, long prefixEntryId, long prefixLength) {
    public static final String KEY = "bookkeeper.experimental.profile.recovery-prefix";
    public static final int BYTES = 256;
    public static final int REFERENCE_BYTES = 56;
    private static final int MAGIC = 0x55575246; // UWRF
    private static final int REFERENCE_MAGIC = 0x55575253; // UWRS

    public ProfileRecoveryBoundary {
        java.util.Objects.requireNonNull(context, "context");
        metadataDigest = ProfileLifecycleHead.fixed(metadataDigest, 32, false);
        if (beforeVersion < context.generation()
                || beforeVersion > context.generation() + ProfileRepairIntent.MAX_INTENTS
                || requiredFrontier < -1 || prefixEntryId < requiredFrontier || prefixLength < 0
                || prefixEntryId == -1 && prefixLength != 0 || prefixEntryId == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid fixed recovery prefix selection");
        }
    }

    @Override
    public byte[] metadataDigest() {
        return metadataDigest.clone();
    }

    public byte[] encode() {
        return finish(ByteBuffer.allocate(BYTES).putInt(MAGIC).putShort((short) 1).putShort((short) 0)
                .put(context.encode()).putInt(beforeVersion).put(metadataDigest).putLong(requiredFrontier)
                .putLong(prefixEntryId).putLong(prefixLength));
    }

    public static ProfileRecoveryBoundary decode(byte[] bytes) {
        var in = checked(bytes, BYTES, MAGIC);
        byte[] context = new byte[ProfileRecoveryContext.ENCODED_BYTES];
        in.get(context);
        int version = in.getInt();
        byte[] hash = new byte[32];
        in.get(hash);
        return new ProfileRecoveryBoundary(ProfileRecoveryContext.decode(context), version, hash,
                in.getLong(), in.getLong(), in.getLong());
    }

    public record Reference(int publicationBeforeVersion, long birthZxid, byte[] digest) {
        public Reference {
            digest = ProfileLifecycleHead.fixed(digest, 32, false);
            if (publicationBeforeVersion < 0 || publicationBeforeVersion == Integer.MAX_VALUE || birthZxid <= 0) {
                throw new IllegalArgumentException("Invalid recovery prefix reference");
            }
        }
        @Override
        public byte[] digest() {
            return digest.clone();
        }
        public byte[] encode() {
            return finish(ByteBuffer.allocate(REFERENCE_BYTES).putInt(REFERENCE_MAGIC)
                    .putShort((short) 1).putShort((short) 0).putInt(publicationBeforeVersion)
                    .putLong(birthZxid).put(digest));
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Reference reference && Arrays.equals(encode(), reference.encode());
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }
    }

    public static Reference reference(byte[] bytes) {
        var in = checked(bytes, REFERENCE_BYTES, REFERENCE_MAGIC);
        int version = in.getInt();
        long birth = in.getLong();
        byte[] digest = new byte[32];
        in.get(digest);
        return new Reference(version, birth, digest);
    }

    public static int versionOffset(LedgerMetadata metadata) {
        byte[] bytes = metadata.getCustomMetadata().get(KEY);
        if (bytes == null) {
            return 0;
        }
        reference(bytes);
        return 1;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileRecoveryBoundary boundary && Arrays.equals(encode(), boundary.encode());
    }
    @Override
    public int hashCode() {
        return Arrays.hashCode(encode());
    }
    @Override
    public String toString() {
        return "RecoveryBoundary{ledger=" + context.ledger().ledgerId() + ", prefix=" + prefixEntryId + '}';
    }

    private static ByteBuffer checked(byte[] bytes, int size, int magic) {
        if (bytes == null || bytes.length != size) {
            throw new IllegalArgumentException("Invalid recovery prefix byte bound");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        var in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != magic
                || in.getShort() != 1 || in.getShort() != 0) {
            throw new IllegalArgumentException("Invalid recovery prefix schema/checksum");
        }
        return in;
    }

    private static byte[] finish(ByteBuffer out) {
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }
}

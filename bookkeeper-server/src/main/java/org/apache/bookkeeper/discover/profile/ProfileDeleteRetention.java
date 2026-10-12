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
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/** Explicit cluster windows frozen at logical deletion. Neither elapsed time nor this receipt authorizes GC. */
public final class ProfileDeleteRetention {
    public static final String KEY = "bookkeeper.experimental.profile.delete-retention";
    private static final int POLICY_BYTES = 60;
    private static final int RECEIPT_BYTES = 200;

    private ProfileDeleteRetention() { }

    public record Policy(long generation, UUID revision, long offlineRejoinMillis,
            long metadataPropagationMillis, long backupRestoreMillis) {
        public Policy {
            Objects.requireNonNull(revision, "revision");
            if (generation < 1 || generation > Integer.MAX_VALUE || revision.equals(new UUID(0, 0))
                    || offlineRejoinMillis <= 0 || metadataPropagationMillis < 0 || backupRestoreMillis <= 0) {
                throw invalid();
            }
            try {
                Math.addExact(Math.addExact(offlineRejoinMillis, metadataPropagationMillis), backupRestoreMillis);
            } catch (ArithmeticException overflow) {
                throw invalid();
            }
        }

        public long totalWindowMillis() {
            return Math.addExact(Math.addExact(offlineRejoinMillis, metadataPropagationMillis), backupRestoreMillis);
        }

        public byte[] encode() {
            return finish(header(POLICY_BYTES, 0x55575250).putLong(generation)
                    .putLong(revision.getMostSignificantBits()).putLong(revision.getLeastSignificantBits())
                    .putLong(offlineRejoinMillis).putLong(metadataPropagationMillis).putLong(backupRestoreMillis));
        }

        public static Policy decode(byte[] bytes) {
            var in = checked(bytes, POLICY_BYTES, 0x55575250);
            return new Policy(in.getLong(), new UUID(in.getLong(), in.getLong()),
                    in.getLong(), in.getLong(), in.getLong());
        }
    }

    public record Receipt(UUID birthId, Policy policy, long policyBirth, ProfileDeleteRecord.Reference tombstone) {
        public Receipt {
            Objects.requireNonNull(birthId, "birthId");
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(tombstone, "tombstone");
            if (birthId.equals(new UUID(0, 0)) || policyBirth <= 0 || tombstone.publicationId() == null) {
                throw invalid();
            }
        }

        public byte[] encode() {
            return finish(header(RECEIPT_BYTES, 0x55575252)
                    .putLong(birthId.getMostSignificantBits()).putLong(birthId.getLeastSignificantBits())
                    .put(policy.encode()).putLong(policyBirth).put(tombstone.encode()));
        }

        public static Receipt decode(byte[] bytes) {
            var in = checked(bytes, RECEIPT_BYTES, 0x55575252);
            UUID birth = new UUID(in.getLong(), in.getLong());
            byte[] policy = new byte[POLICY_BYTES];
            in.get(policy);
            long policyBirth = in.getLong();
            byte[] reference = new byte[104];
            in.get(reference);
            return new Receipt(birth, Policy.decode(policy), policyBirth,
                    ProfileDeleteRecord.Reference.decode(reference));
        }

        boolean matches(ProfileDeleteRecord.Reference reference) {
            return Arrays.equals(tombstone.encode(), reference.encode());
        }
    }

    private static ByteBuffer header(int size, int magic) {
        return ByteBuffer.allocate(size).putInt(magic).putShort((short) 1).putShort((short) 0);
    }

    private static byte[] finish(ByteBuffer out) {
        if (out.position() != out.capacity() - 4) {
            throw invalid();
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }

    private static ByteBuffer checked(byte[] bytes, int size, int magic) {
        if (bytes == null || bytes.length != size) {
            throw invalid();
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        var in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue()
                || in.getInt() != magic || in.getShort() != 1 || in.getShort() != 0) {
            throw invalid();
        }
        return in;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid bounded Profile delete retention record");
    }
}

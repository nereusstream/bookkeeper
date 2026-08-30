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

package org.apache.bookkeeper.common.profile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Test-only verifier independent of production decoder iteration and hash helpers. */
final class IndependentProfileDescriptorVerifier {

    private static final byte[] OWN_MAGIC = {0x42, 0x4b, 0x50, 0x44};
    private static final byte[] OWN_DOMAIN =
            "org.apache.bookkeeper/ProfileDescriptor/v1\0".getBytes(StandardCharsets.US_ASCII);
    private static final int[] OWN_TYPES = {1, 1, 1, 1, 1, 1, 1, 2, 3, 4};
    private static final int[] OWN_FIXED_LENGTHS = {2, 2, 2, 2, 2, 2, 2, 4, 8};

    private IndependentProfileDescriptorVerifier() {}

    static Verification verify(
            byte[] input,
            ProfileCapabilityRegistry capabilityRegistry,
            FailureDomainPolicyRegistry failureDomainPolicyRegistry) {
        if (input == null || input.length > 1024 || input.length < 124 || input.length > 508
                || (input.length - 124) % 6 != 0) {
            throw invalid();
        }
        OwnCursor cursor = new OwnCursor(input);
        for (byte expected : OWN_MAGIC) {
            if (cursor.u8() != Byte.toUnsignedInt(expected)) {
                throw invalid();
            }
        }
        int codecVersion = cursor.u16();
        int schemaVersion = cursor.u16();
        long totalLength = cursor.u32();
        int fieldCount = cursor.u16();
        int flags = cursor.u16();
        if (codecVersion != 1 || schemaVersion != 1 || totalLength != input.length
                || fieldCount != 10 || flags != 0) {
            throw invalid();
        }

        long[] scalarValues = new long[9];
        StringBuilder dump = new StringBuilder();
        dump.append("magic=BKPD\n")
                .append("codecVersion=").append(codecVersion).append('\n')
                .append("semanticSchemaVersion=").append(schemaVersion).append('\n')
                .append("totalLength=").append(totalLength).append('\n')
                .append("fieldCount=").append(fieldCount).append('\n')
                .append("flags=").append(flags).append('\n');
        for (int index = 0; index < 9; index++) {
            int expectedFieldId = index + 1;
            int fieldId = cursor.u16();
            int type = cursor.u16();
            long valueLength = cursor.u32();
            if (fieldId != expectedFieldId
                    || type != OWN_TYPES[index]
                    || valueLength != OWN_FIXED_LENGTHS[index]) {
                throw invalid();
            }
            scalarValues[index] = switch (type) {
                case 1 -> cursor.u16();
                case 2 -> cursor.u32();
                case 3 -> cursor.u64();
                default -> throw invalid();
            };
            dumpField(dump, fieldId, type, valueLength, scalarValues[index]);
        }

        int capabilityFieldId = cursor.u16();
        int capabilityType = cursor.u16();
        long capabilityValueLength = cursor.u32();
        if (capabilityFieldId != 10 || capabilityType != 4 || capabilityValueLength < 2) {
            throw invalid();
        }
        int capabilityCount = cursor.u16();
        if (capabilityCount > 64
                || capabilityValueLength != 2L + 6L * capabilityCount
                || input.length != 124 + 6 * capabilityCount) {
            throw invalid();
        }
        dump.append("field.10.type=4\n")
                .append("field.10.length=").append(capabilityValueLength).append('\n')
                .append("mandatoryCapabilities.count=").append(capabilityCount).append('\n');
        long previousCapabilityId = 0;
        for (int index = 0; index < capabilityCount; index++) {
            long capabilityId = cursor.u32();
            int semanticVersion = cursor.u16();
            if (capabilityId == 0 || semanticVersion == 0 || capabilityId <= previousCapabilityId
                    || !capabilityRegistry.contains(capabilityId, semanticVersion)) {
                throw invalid();
            }
            dump.append("mandatoryCapabilities.").append(index)
                    .append('=').append(capabilityId).append(':').append(semanticVersion).append('\n');
            previousCapabilityId = capabilityId;
        }
        if (cursor.remaining() != 0) {
            throw invalid();
        }

        if (scalarValues[0] < 1 || scalarValues[0] > 3
                || scalarValues[1] < 1 || scalarValues[1] > 2
                || scalarValues[2] < 1 || scalarValues[2] > 2) {
            throw invalid();
        }
        long ensemble = scalarValues[3];
        long writeQuorum = scalarValues[4];
        long ackQuorum = scalarValues[5];
        long lossBudget = scalarValues[6];
        long policyId = scalarValues[7];
        long policyGeneration = scalarValues[8];
        if (ensemble < 1 || writeQuorum < 1 || writeQuorum > ensemble
                || ackQuorum < 1 || ackQuorum > writeQuorum
                || lossBudget >= ackQuorum || policyId == 0 || policyGeneration == 0
                || !failureDomainPolicyRegistry.contains(policyId, policyGeneration)) {
            throw invalid();
        }

        MessageDigest messageDigest = ownSha256();
        messageDigest.update(OWN_DOMAIN);
        messageDigest.update(new byte[] {0, 1, 0, 1});
        messageDigest.update(ByteBuffer.allocate(4).putInt(input.length).array());
        messageDigest.update(input);
        byte[] digest = messageDigest.digest();
        byte[] identity = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1).put(digest).array();
        return new Verification(digest, identity, dump.toString());
    }

    private static void dumpField(StringBuilder dump, int id, int type, long length, long value) {
        dump.append("field.").append(id).append(".type=").append(type).append('\n')
                .append("field.").append(id).append(".length=").append(length).append('\n')
                .append("field.").append(id).append(".value=").append(Long.toUnsignedString(value)).append('\n');
    }

    private static MessageDigest ownSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new AssertionError(exception);
        }
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("independent verification rejected descriptor");
    }

    static final class Verification {
        private final byte[] digest;
        private final byte[] identity;
        private final String fieldDump;

        private Verification(byte[] digest, byte[] identity, String fieldDump) {
            this.digest = digest.clone();
            this.identity = identity.clone();
            this.fieldDump = fieldDump;
        }

        byte[] digest() {
            return digest.clone();
        }

        byte[] identity() {
            return identity.clone();
        }

        String fieldDump() {
            return fieldDump;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Verification that)) {
                return false;
            }
            return Arrays.equals(digest, that.digest)
                    && Arrays.equals(identity, that.identity)
                    && fieldDump.equals(that.fieldDump);
        }

        @Override
        public int hashCode() {
            return 31 * (31 * Arrays.hashCode(digest) + Arrays.hashCode(identity)) + fieldDump.hashCode();
        }
    }

    private static final class OwnCursor {
        private final byte[] input;
        private int position;

        private OwnCursor(byte[] input) {
            this.input = input;
        }

        private int u8() {
            require(1);
            return Byte.toUnsignedInt(input[position++]);
        }

        private int u16() {
            return (u8() << 8) | u8();
        }

        private long u32() {
            return ((long) u8() << 24) | ((long) u8() << 16) | ((long) u8() << 8) | u8();
        }

        private long u64() {
            long result = 0;
            for (int index = 0; index < 8; index++) {
                result = (result << 8) | u8();
            }
            return result;
        }

        private int remaining() {
            return input.length - position;
        }

        private void require(int length) {
            if (remaining() < length) {
                throw invalid();
            }
        }
    }
}

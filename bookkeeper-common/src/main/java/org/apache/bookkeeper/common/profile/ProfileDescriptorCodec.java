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

import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.BAD_MAGIC;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.CAPABILITY_COUNT_OUT_OF_RANGE;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.DUPLICATE_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.DUPLICATE_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.IDENTITY_MISMATCH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.MISSING_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NONZERO_FLAGS;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NON_CANONICAL_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NULL_INPUT;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.TRAILING_BYTES;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.TRUNCATED;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_ENUM;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_TYPE;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNSUPPORTED_CODEC_VERSION;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNSUPPORTED_SCHEMA_VERSION;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_CAPABILITY_SET_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_FIELD_COUNT;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_TOTAL_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_VALUE_LENGTH;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;

/** Frozen strict-flat-TLV canonical codec for Profile descriptors. */
public final class ProfileDescriptorCodec {

    public static final int CODEC_VERSION = 1;
    public static final int SEMANTIC_SCHEMA_VERSION = 1;
    public static final int ABSOLUTE_INPUT_CAP = 1024;
    public static final int MIN_CANONICAL_LENGTH = 124;
    public static final int MAX_CANONICAL_LENGTH = 508;

    private static final byte[] MAGIC = {'B', 'K', 'P', 'D'};
    private static final int HEADER_LENGTH = 16;
    private static final int FIELD_COUNT = 10;
    private static final int FIELD_HEADER_LENGTH = 8;
    private static final int TYPE_U16 = 1;
    private static final int TYPE_U32 = 2;
    private static final int TYPE_U64 = 3;
    private static final int TYPE_CAPABILITY_SET = 4;

    private ProfileDescriptorCodec() {}

    /** Encodes the only canonical byte representation after strict semantic validation. */
    public static byte[] encode(ProfileDescriptor descriptor, ProfileDescriptorValidator validator) {
        Objects.requireNonNull(validator, "validator").validate(descriptor);
        int capabilityCount = descriptor.mandatoryCapabilities().size();
        int totalLength = MIN_CANONICAL_LENGTH + 6 * capabilityCount;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.put(MAGIC);
        buffer.putShort((short) CODEC_VERSION);
        buffer.putShort((short) SEMANTIC_SCHEMA_VERSION);
        buffer.putInt(totalLength);
        buffer.putShort((short) FIELD_COUNT);
        buffer.putShort((short) 0);

        putU16Field(buffer, 1, descriptor.requiredEngine().wireValue());
        putU16Field(buffer, 2, descriptor.payloadFormat().wireValue());
        putU16Field(buffer, 3, descriptor.durabilityMode().wireValue());
        putU16Field(buffer, 4, descriptor.ensembleSize());
        putU16Field(buffer, 5, descriptor.writeQuorumSize());
        putU16Field(buffer, 6, descriptor.ackQuorumSize());
        putU16Field(buffer, 7, descriptor.permanentLossBudget());
        putFieldHeader(buffer, 8, TYPE_U32, 4);
        buffer.putInt((int) descriptor.failureDomainPolicyId());
        putFieldHeader(buffer, 9, TYPE_U64, 8);
        buffer.putLong(descriptor.failureDomainPolicyGeneration());
        putFieldHeader(buffer, 10, TYPE_CAPABILITY_SET, 2 + 6 * capabilityCount);
        buffer.putShort((short) capabilityCount);
        for (ProfileCapability capability : descriptor.mandatoryCapabilities()) {
            buffer.putInt((int) capability.capabilityId());
            buffer.putShort((short) capability.semanticVersion());
        }
        return buffer.array();
    }

    /** Strictly decodes input bytes without normalization or re-encoding acceptance. */
    public static ProfileDescriptor decode(byte[] input, ProfileDescriptorValidator validator) {
        return decode(input, validator, ArrayList::new);
    }

    /** Strictly decodes and constant-time checks a separately declared 36-byte identity. */
    public static ProfileDescriptor decodeAndVerify(
            byte[] input,
            ProfileDescriptorIdentity declaredIdentity,
            ProfileDescriptorValidator validator) {
        ProfileDescriptor descriptor = decode(input, validator);
        if (declaredIdentity == null || !declaredIdentity.verifies(input)) {
            throw failure(IDENTITY_MISMATCH);
        }
        return descriptor;
    }

    static ProfileDescriptor decode(
            byte[] input,
            ProfileDescriptorValidator validator,
            IntFunction<List<ProfileCapability>> capabilityListFactory) {
        if (input == null) {
            throw failure(NULL_INPUT);
        }
        if (input.length > ABSOLUTE_INPUT_CAP) {
            throw failure(ProfileDescriptorValidationException.Reason.INPUT_TOO_LARGE);
        }
        Objects.requireNonNull(validator, "validator");
        Objects.requireNonNull(capabilityListFactory, "capabilityListFactory");
        if (input.length < HEADER_LENGTH) {
            throw failure(TRUNCATED);
        }

        Cursor cursor = new Cursor(input);
        for (byte expected : MAGIC) {
            if (cursor.readByte() != expected) {
                throw failure(BAD_MAGIC);
            }
        }
        if (cursor.readU16() != CODEC_VERSION) {
            throw failure(UNSUPPORTED_CODEC_VERSION);
        }
        if (cursor.readU16() != SEMANTIC_SCHEMA_VERSION) {
            throw failure(UNSUPPORTED_SCHEMA_VERSION);
        }
        if (cursor.readU32() != input.length) {
            throw failure(WRONG_TOTAL_LENGTH);
        }
        if (cursor.readU16() != FIELD_COUNT) {
            throw failure(WRONG_FIELD_COUNT);
        }
        if (cursor.readU16() != 0) {
            throw failure(NONZERO_FLAGS);
        }
        if (input.length < MIN_CANONICAL_LENGTH
                || input.length > MAX_CANONICAL_LENGTH
                || (input.length - MIN_CANONICAL_LENGTH) % 6 != 0) {
            throw failure(NON_CANONICAL_LENGTH);
        }

        int previousFieldId = 0;
        previousFieldId = readFieldHeader(cursor, 1, previousFieldId, TYPE_U16, 2);
        EngineProfile requiredEngine = readEngineProfile(cursor.readU16());
        previousFieldId = readFieldHeader(cursor, 2, previousFieldId, TYPE_U16, 2);
        PayloadFormat payloadFormat = readPayloadFormat(cursor.readU16());
        previousFieldId = readFieldHeader(cursor, 3, previousFieldId, TYPE_U16, 2);
        DurabilityMode durabilityMode = readDurabilityMode(cursor.readU16());
        previousFieldId = readFieldHeader(cursor, 4, previousFieldId, TYPE_U16, 2);
        int ensembleSize = cursor.readU16();
        previousFieldId = readFieldHeader(cursor, 5, previousFieldId, TYPE_U16, 2);
        int writeQuorumSize = cursor.readU16();
        previousFieldId = readFieldHeader(cursor, 6, previousFieldId, TYPE_U16, 2);
        int ackQuorumSize = cursor.readU16();
        previousFieldId = readFieldHeader(cursor, 7, previousFieldId, TYPE_U16, 2);
        int permanentLossBudget = cursor.readU16();
        previousFieldId = readFieldHeader(cursor, 8, previousFieldId, TYPE_U32, 4);
        long failureDomainPolicyId = cursor.readU32();
        previousFieldId = readFieldHeader(cursor, 9, previousFieldId, TYPE_U64, 8);
        long failureDomainPolicyGeneration = cursor.readU64();

        int capabilitySetLength = readVariableFieldHeader(
                cursor, 10, previousFieldId, TYPE_CAPABILITY_SET);
        if (capabilitySetLength < 2) {
            throw failure(WRONG_CAPABILITY_SET_LENGTH);
        }
        cursor.require(capabilitySetLength);
        int capabilityCount = cursor.readU16();
        if (capabilityCount > ProfileDescriptorValidator.MAX_CAPABILITIES) {
            throw failure(CAPABILITY_COUNT_OUT_OF_RANGE);
        }
        if (capabilitySetLength != 2 + capabilityCount * 6
                || input.length != MIN_CANONICAL_LENGTH + capabilityCount * 6) {
            throw failure(WRONG_CAPABILITY_SET_LENGTH);
        }

        List<ProfileCapability> capabilities = capabilityListFactory.apply(capabilityCount);
        if (capabilities == null) {
            throw new IllegalStateException("capability list factory returned null");
        }
        long previousCapabilityId = 0;
        for (int index = 0; index < capabilityCount; index++) {
            long capabilityId = cursor.readU32();
            int semanticVersion = cursor.readU16();
            if (capabilityId == 0 || semanticVersion == 0) {
                throw failure(INVALID_CAPABILITY);
            }
            if (capabilityId == previousCapabilityId) {
                throw failure(DUPLICATE_CAPABILITY);
            }
            if (capabilityId < previousCapabilityId) {
                throw failure(OUT_OF_ORDER_CAPABILITY);
            }
            capabilities.add(new ProfileCapability(capabilityId, semanticVersion));
            previousCapabilityId = capabilityId;
        }
        if (cursor.remaining() != 0) {
            throw failure(TRAILING_BYTES);
        }

        ProfileDescriptor descriptor = new ProfileDescriptor(
                requiredEngine,
                payloadFormat,
                durabilityMode,
                ensembleSize,
                writeQuorumSize,
                ackQuorumSize,
                permanentLossBudget,
                failureDomainPolicyId,
                failureDomainPolicyGeneration,
                capabilities);
        validator.validate(descriptor);
        return descriptor;
    }

    private static void putU16Field(ByteBuffer buffer, int fieldId, int value) {
        putFieldHeader(buffer, fieldId, TYPE_U16, 2);
        buffer.putShort((short) value);
    }

    private static void putFieldHeader(ByteBuffer buffer, int fieldId, int type, int valueLength) {
        buffer.putShort((short) fieldId);
        buffer.putShort((short) type);
        buffer.putInt(valueLength);
    }

    private static int readFieldHeader(
            Cursor cursor, int expectedFieldId, int previousFieldId, int expectedType, int expectedLength) {
        int valueLength = readVariableFieldHeader(
                cursor, expectedFieldId, previousFieldId, expectedType);
        if (valueLength != expectedLength) {
            throw failure(WRONG_VALUE_LENGTH);
        }
        cursor.require(valueLength);
        return expectedFieldId;
    }

    private static int readVariableFieldHeader(
            Cursor cursor, int expectedFieldId, int previousFieldId, int expectedType) {
        cursor.require(FIELD_HEADER_LENGTH);
        int fieldId = cursor.readU16();
        int type = cursor.readU16();
        long valueLength = cursor.readU32();
        if (fieldId < 1 || fieldId > FIELD_COUNT) {
            throw failure(UNKNOWN_FIELD);
        }
        if (fieldId == previousFieldId) {
            throw failure(DUPLICATE_FIELD);
        }
        if (fieldId < expectedFieldId) {
            throw failure(OUT_OF_ORDER_FIELD);
        }
        if (fieldId > expectedFieldId) {
            throw failure(MISSING_FIELD);
        }
        if (type != expectedType) {
            throw failure(UNKNOWN_TYPE);
        }
        if (valueLength > Integer.MAX_VALUE) {
            throw failure(WRONG_VALUE_LENGTH);
        }
        return (int) valueLength;
    }

    private static EngineProfile readEngineProfile(int wireValue) {
        try {
            return EngineProfile.fromWireValue(wireValue);
        } catch (IllegalArgumentException exception) {
            throw failure(UNKNOWN_ENUM);
        }
    }

    private static PayloadFormat readPayloadFormat(int wireValue) {
        try {
            return PayloadFormat.fromWireValue(wireValue);
        } catch (IllegalArgumentException exception) {
            throw failure(UNKNOWN_ENUM);
        }
    }

    private static DurabilityMode readDurabilityMode(int wireValue) {
        try {
            return DurabilityMode.fromWireValue(wireValue);
        } catch (IllegalArgumentException exception) {
            throw failure(UNKNOWN_ENUM);
        }
    }

    private static ProfileDescriptorValidationException failure(
            ProfileDescriptorValidationException.Reason reason) {
        return new ProfileDescriptorValidationException(reason);
    }

    private static final class Cursor {
        private final byte[] input;
        private int position;

        private Cursor(byte[] input) {
            this.input = input;
        }

        private byte readByte() {
            require(1);
            return input[position++];
        }

        private int readU16() {
            require(2);
            int result = (Byte.toUnsignedInt(input[position]) << 8)
                    | Byte.toUnsignedInt(input[position + 1]);
            position += 2;
            return result;
        }

        private long readU32() {
            require(4);
            long result = ((long) Byte.toUnsignedInt(input[position]) << 24)
                    | ((long) Byte.toUnsignedInt(input[position + 1]) << 16)
                    | ((long) Byte.toUnsignedInt(input[position + 2]) << 8)
                    | Byte.toUnsignedInt(input[position + 3]);
            position += 4;
            return result;
        }

        private long readU64() {
            require(8);
            long result = 0;
            for (int index = 0; index < Long.BYTES; index++) {
                result = (result << 8) | Byte.toUnsignedLong(input[position + index]);
            }
            position += Long.BYTES;
            return result;
        }

        private int remaining() {
            return input.length - position;
        }

        private void require(int length) {
            if (length < 0 || remaining() < length) {
                throw failure(TRUNCATED);
            }
        }
    }
}

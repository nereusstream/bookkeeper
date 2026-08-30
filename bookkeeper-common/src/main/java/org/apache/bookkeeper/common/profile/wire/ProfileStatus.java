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

package org.apache.bookkeeper.common.profile.wire;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Frozen semantic status prefix for experimental Profile responses. */
public record ProfileStatus(
        StatusClass statusClass,
        RetryDisposition retryDisposition,
        DurableResult durableResult,
        int detailCode) {

    public static final int ENCODED_LENGTH = 8;

    public enum StatusClass implements WireEnum {
        OK(0),
        UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE(1),
        PROFILE_IDENTITY_CONFLICT(2),
        PROFILE_NOT_READY_OR_STALE(3),
        FENCED(4),
        TOMBSTONED_OR_DELETED(5),
        RECOVERY_GRANT_INVALID(6),
        TRANSIENT_UNAVAILABLE(7),
        DURABILITY_RESULT_UNKNOWN(8),
        QUARANTINED_OR_UNKNOWN_MANDATORY(9),
        UNAUTHORIZED(10),
        BAD_REQUEST(11);

        private final int wireValue;

        StatusClass(int wireValue) {
            this.wireValue = wireValue;
        }

        @Override
        public int wireValue() {
            return wireValue;
        }
    }

    public enum RetryDisposition implements WireEnum {
        NEVER(0),
        SAME_PROFILE_OPERATION(1),
        AFTER_CONTROL_RECONCILIATION(2),
        REPLACE_TARGET(3),
        ATTEMPT_BOUNDED_OR_CANCELLED(4);

        private final int wireValue;

        RetryDisposition(int wireValue) {
            this.wireValue = wireValue;
        }

        @Override
        public int wireValue() {
            return wireValue;
        }
    }

    public enum DurableResult implements WireEnum {
        NONE(0),
        APPLIED(1),
        ALREADY_APPLIED(2),
        UNKNOWN(3);

        private final int wireValue;

        DurableResult(int wireValue) {
            this.wireValue = wireValue;
        }

        @Override
        public int wireValue() {
            return wireValue;
        }
    }

    private static final Map<Integer, StatusClass> STATUS_CLASSES = index(StatusClass.values());
    private static final Map<Integer, RetryDisposition> RETRY_DISPOSITIONS = index(RetryDisposition.values());
    private static final Map<Integer, DurableResult> DURABLE_RESULTS = index(DurableResult.values());

    public ProfileStatus {
        Objects.requireNonNull(statusClass, "statusClass");
        Objects.requireNonNull(retryDisposition, "retryDisposition");
        Objects.requireNonNull(durableResult, "durableResult");
        if ((detailCode & 0xffff_0000) != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_STATUS_LENGTH);
        }
    }

    public byte[] encode() {
        return ByteBuffer.allocate(ENCODED_LENGTH)
                .putShort((short) statusClass.wireValue())
                .put((byte) retryDisposition.wireValue())
                .put((byte) durableResult.wireValue())
                .putShort((short) detailCode)
                .putShort((short) 0)
                .array();
    }

    public static ProfileStatus decode(byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_STATUS_LENGTH);
        }
        return decodePrefix(ByteBuffer.wrap(bytes));
    }

    public static ProfileStatus decodePrefix(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_STATUS_LENGTH);
        }
        int status = Short.toUnsignedInt(buffer.getShort());
        int retry = Byte.toUnsignedInt(buffer.get());
        int durable = Byte.toUnsignedInt(buffer.get());
        int detail = Short.toUnsignedInt(buffer.getShort());
        if (buffer.getShort() != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NONZERO_RESERVED);
        }
        return new ProfileStatus(
                lookup(STATUS_CLASSES, status, ProfileWireValidationException.Reason.UNKNOWN_STATUS_CLASS),
                lookup(RETRY_DISPOSITIONS, retry, ProfileWireValidationException.Reason.UNKNOWN_RETRY_DISPOSITION),
                lookup(DURABLE_RESULTS, durable, ProfileWireValidationException.Reason.UNKNOWN_DURABLE_RESULT),
                detail);
    }

    public static ProfileStatus unsupported() {
        return new ProfileStatus(
                StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                RetryDisposition.NEVER,
                DurableResult.NONE,
                0);
    }

    private static <T extends Enum<T> & WireEnum> Map<Integer, T> index(T[] values) {
        Map<Integer, T> indexed = new HashMap<>();
        for (T value : values) {
            indexed.put(value.wireValue(), value);
        }
        return Map.copyOf(indexed);
    }

    private static <T> T lookup(
            Map<Integer, T> values, int wireValue, ProfileWireValidationException.Reason reason) {
        T value = values.get(wireValue);
        if (value == null) {
            throw ProfileWireCodecSupport.failure(reason);
        }
        return value;
    }

    private interface WireEnum {
        int wireValue();
    }
}

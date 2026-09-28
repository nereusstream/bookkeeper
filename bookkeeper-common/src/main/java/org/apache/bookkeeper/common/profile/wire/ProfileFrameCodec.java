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
import java.util.Objects;
import java.util.function.IntFunction;

/** Strict pure-JDK codec for the experimental Profile frame test manifest. */
public final class ProfileFrameCodec {

    public static final int OUTER_LENGTH_PREFIX = 4;
    public static final int PRE_HELLO_FRAME_MAX = 4_096;
    public static final int CONTROL_FRAME_MAX = 65_536;
    public static final int ABSOLUTE_FRAME_MAX = 5_242_880;

    public enum DecodePhase {
        PRE_HELLO,
        ESTABLISHED
    }

    /** Fixed-header result for receivers that must reserve body capacity before accepting it. */
    public record InspectedHeader(int outerLength, ProfileFrameHeader header) {
        public InspectedHeader {
            Objects.requireNonNull(header, "header");
            if (outerLength < ProfileFrameHeader.ENCODED_LENGTH || outerLength > ABSOLUTE_FRAME_MAX
                    || outerLength != ProfileFrameHeader.ENCODED_LENGTH + header.bodyLength()) {
                throw new IllegalArgumentException("Invalid inspected frame length");
            }
        }

        public int frameBytes() {
            return OUTER_LENGTH_PREFIX + outerLength;
        }
    }

    private ProfileFrameCodec() {}

    public static byte[] encode(ProfileFrame frame) {
        Objects.requireNonNull(frame, "frame");
        ProfileFrameHeader header = frame.header();
        byte[] body = frame.body();
        int outerLength = ProfileFrameHeader.ENCODED_LENGTH + body.length;
        validateFrameLimit(outerLength, header.operation(), header.operation() == ProfileOperation.HELLO);
        if (header.operation().isReservedDisabled() && !header.isResponse() && body.length != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.RESERVED_OPERATION_BODY);
        }
        ByteBuffer buffer = ByteBuffer.allocate(OUTER_LENGTH_PREFIX + outerLength);
        buffer.putInt(outerLength);
        buffer.putInt(ProfileFrameHeader.MAGIC);
        buffer.putShort((short) header.protocolMajor());
        buffer.putShort((short) header.protocolMinor());
        buffer.putShort((short) header.headerLength());
        buffer.putShort((short) header.operation().wireValue());
        buffer.putInt(header.flags());
        buffer.putLong(header.requestId());
        buffer.putInt(header.bodyLength());
        buffer.putInt((int) header.reserved());
        buffer.put(body);
        return buffer.array();
    }

    public static ProfileFrame decode(byte[] bytes, DecodePhase phase) {
        return decode(bytes, phase, byte[]::new);
    }

    static ProfileFrame decode(byte[] bytes, DecodePhase phase, IntFunction<byte[]> bodyAllocator) {
        if (bytes == null) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NULL_INPUT);
        }
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(bodyAllocator, "bodyAllocator");
        InspectedHeader inspected = inspectHeader(bytes, phase);
        int bodyLength = inspected.header().bodyLength();
        ProfileOperation operation = inspected.header().operation();
        long expectedInputLength = inspected.frameBytes();
        if (bytes.length < expectedInputLength) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.TRUNCATED_BODY);
        }
        if (bytes.length > expectedInputLength) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.TRAILING_BYTES);
        }
        if (operation.isReservedDisabled() && !inspected.header().isResponse() && bodyLength != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.RESERVED_OPERATION_BODY);
        }

        byte[] body = bodyAllocator.apply(bodyLength);
        if (body == null || body.length != bodyLength) {
            throw new IllegalStateException("body allocator returned wrong length");
        }
        System.arraycopy(bytes, OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH, body, 0, bodyLength);
        return new ProfileFrame(inspected.header(), body);
    }

    /** Validate the outer length and fixed header without waiting for, or allocating, the declared body. */
    public static InspectedHeader inspectHeader(byte[] bytes, DecodePhase phase) {
        if (bytes == null) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NULL_INPUT);
        }
        Objects.requireNonNull(phase, "phase");
        if (bytes.length < OUTER_LENGTH_PREFIX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.TRUNCATED_OUTER_LENGTH);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long outerLengthUnsigned = Integer.toUnsignedLong(buffer.getInt());
        if (outerLengthUnsigned > Integer.MAX_VALUE) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.OUTER_LENGTH_OVERFLOW);
        }
        int outerLength = (int) outerLengthUnsigned;
        if (outerLength < ProfileFrameHeader.ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        }
        if (outerLength > ABSOLUTE_FRAME_MAX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.ABSOLUTE_FRAME_TOO_LARGE);
        }
        if (phase == DecodePhase.PRE_HELLO && outerLength > PRE_HELLO_FRAME_MAX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.PRE_HELLO_FRAME_TOO_LARGE);
        }
        if (bytes.length < OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.TRUNCATED_HEADER);
        }

        int magic = buffer.getInt();
        if (magic != ProfileFrameHeader.MAGIC) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.BAD_MAGIC);
        }
        int major = Short.toUnsignedInt(buffer.getShort());
        int minor = Short.toUnsignedInt(buffer.getShort());
        int headerLength = Short.toUnsignedInt(buffer.getShort());
        int subtype = Short.toUnsignedInt(buffer.getShort());
        int flags = buffer.getInt();
        long requestId = buffer.getLong();
        long bodyLengthUnsigned = Integer.toUnsignedLong(buffer.getInt());
        long reserved = Integer.toUnsignedLong(buffer.getInt());

        if (major != ProfileFrameHeader.PROTOCOL_MAJOR || minor != ProfileFrameHeader.PROTOCOL_MINOR) {
            throw ProfileWireCodecSupport.failure(
                    ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        }
        if (headerLength != ProfileFrameHeader.ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.WRONG_HEADER_LENGTH);
        }
        ProfileOperation operation = ProfileOperation.fromWireValue(subtype);
        if ((flags & ~ProfileFrameHeader.ALLOWED_FLAGS) != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.UNKNOWN_FLAGS);
        }
        if (reserved != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NONZERO_RESERVED);
        }
        if (bodyLengthUnsigned > Integer.MAX_VALUE) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.BODY_LENGTH_OVERFLOW);
        }
        int bodyLength = (int) bodyLengthUnsigned;
        if ((long) ProfileFrameHeader.ENCODED_LENGTH + bodyLength != outerLength) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.OUTER_LENGTH_MISMATCH);
        }
        if (phase == DecodePhase.PRE_HELLO && operation != ProfileOperation.HELLO) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.HELLO_NOT_FIRST);
        }
        validateFrameLimit(outerLength, operation, phase == DecodePhase.PRE_HELLO);
        ProfileFrameHeader header = new ProfileFrameHeader(
                major, minor, headerLength, operation, flags, requestId, bodyLength, reserved);
        return new InspectedHeader(outerLength, header);
    }

    private static void validateFrameLimit(int outerLength, ProfileOperation operation, boolean preHello) {
        if (outerLength > ABSOLUTE_FRAME_MAX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.ABSOLUTE_FRAME_TOO_LARGE);
        }
        if ((preHello || operation == ProfileOperation.HELLO) && outerLength > PRE_HELLO_FRAME_MAX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.PRE_HELLO_FRAME_TOO_LARGE);
        }
        if (operation.isControl() && outerLength > CONTROL_FRAME_MAX) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.CONTROL_FRAME_TOO_LARGE);
        }
    }
}

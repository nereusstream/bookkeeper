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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ProfileFrameCodecTest {

    @Test
    public void roundTripsEveryFrozenSubtype() {
        for (ProfileOperation operation : ProfileOperation.values()) {
            ProfileFrame original = ProfileFrame.request(operation, 0x0102_0304_0506_0708L, new byte[0]);
            byte[] encoded = ProfileFrameCodec.encode(original);
            ProfileFrame decoded = ProfileFrameCodec.decode(
                    encoded,
                    operation == ProfileOperation.HELLO
                            ? ProfileFrameCodec.DecodePhase.PRE_HELLO
                            : ProfileFrameCodec.DecodePhase.ESTABLISHED);
            assertEquals(original, decoded);
            assertEquals(36, encoded.length);
            assertEquals(32, ByteBuffer.wrap(encoded).getInt());
            assertEquals(ProfileFrameHeader.MAGIC, ByteBuffer.wrap(encoded, 4, 4).getInt());
        }
    }

    @Test
    public void headerIsByteExact() {
        byte[] body = {1, 2, 3};
        byte[] encoded = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 7, body));
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        assertEquals(35, buffer.getInt());
        assertEquals(ProfileFrameHeader.MAGIC, buffer.getInt());
        assertEquals(1, Short.toUnsignedInt(buffer.getShort()));
        assertEquals(0, Short.toUnsignedInt(buffer.getShort()));
        assertEquals(32, Short.toUnsignedInt(buffer.getShort()));
        assertEquals(ProfileOperation.HELLO.wireValue(), Short.toUnsignedInt(buffer.getShort()));
        assertEquals(0, buffer.getInt());
        assertEquals(7, buffer.getLong());
        assertEquals(3, buffer.getInt());
        assertEquals(0, buffer.getInt());
        assertArrayEquals(body, new byte[] {buffer.get(), buffer.get(), buffer.get()});
    }

    @Test
    public void rejectsStrictHeaderMutationsBeforeBodyAllocation() {
        byte[] valid = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 1, new byte[0]));
        assertRejectedBeforeAllocation(mutateU32(valid, 4, 0), ProfileWireValidationException.Reason.BAD_MAGIC);
        assertRejectedBeforeAllocation(
                mutateU16(valid, 8, 2),
                ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        assertRejectedBeforeAllocation(
                mutateU16(valid, 10, 1),
                ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        assertRejectedBeforeAllocation(
                mutateU16(valid, 12, 31), ProfileWireValidationException.Reason.WRONG_HEADER_LENGTH);
        assertRejectedBeforeAllocation(
                mutateU16(valid, 14, 0xffff), ProfileWireValidationException.Reason.UNKNOWN_SUBTYPE);
        assertRejectedBeforeAllocation(
                mutateU32(valid, 16, 8), ProfileWireValidationException.Reason.UNKNOWN_FLAGS);
        assertRejectedBeforeAllocation(
                mutateU32(valid, 32, 1), ProfileWireValidationException.Reason.NONZERO_RESERVED);
        assertRejectedBeforeAllocation(
                mutateU32(valid, 28, 1), ProfileWireValidationException.Reason.OUTER_LENGTH_MISMATCH);
    }

    @Test
    public void enforcesLengthsAndLimitsBeforeAllocation() {
        byte[] valid = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 1, new byte[0]));
        assertReason(
                ProfileWireValidationException.Reason.TRUNCATED_OUTER_LENGTH,
                () -> ProfileFrameCodec.decode(new byte[3], ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertReason(
                ProfileWireValidationException.Reason.TRUNCATED_HEADER,
                () -> ProfileFrameCodec.decode(new byte[] {0, 0, 0, 32}, ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertReason(
                ProfileWireValidationException.Reason.FRAME_TOO_SMALL,
                () -> ProfileFrameCodec.decode(
                        mutateU32(valid, 0, 31), ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertReason(
                ProfileWireValidationException.Reason.TRAILING_BYTES,
                () -> ProfileFrameCodec.decode(
                        java.util.Arrays.copyOf(valid, valid.length + 1), ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertReason(
                ProfileWireValidationException.Reason.PRE_HELLO_FRAME_TOO_LARGE,
                () -> ProfileFrameCodec.decode(
                        lengthOnly(ProfileFrameCodec.PRE_HELLO_FRAME_MAX + 1),
                        ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertReason(
                ProfileWireValidationException.Reason.ABSOLUTE_FRAME_TOO_LARGE,
                () -> ProfileFrameCodec.decode(
                        lengthOnly(ProfileFrameCodec.ABSOLUTE_FRAME_MAX + 1),
                        ProfileFrameCodec.DecodePhase.ESTABLISHED));
    }

    @Test
    public void enforcesHelloFirstAndDisabledReservations() {
        byte[] read = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.READ_ENTRY, 1, new byte[0]));
        assertReason(
                ProfileWireValidationException.Reason.HELLO_NOT_FIRST,
                () -> ProfileFrameCodec.decode(read, ProfileFrameCodec.DecodePhase.PRE_HELLO));
        assertThrows(
                ProfileWireValidationException.class,
                () -> ProfileFrame.request(ProfileOperation.RANGE_READ, 1, new byte[] {1}));
        ProfileOperationCodec.Unsupported unsupported = (ProfileOperationCodec.Unsupported)
                ProfileOperationCodec.decode(ProfileOperation.RANGE_READ, new byte[0]);
        assertEquals(ProfileStatus.unsupported(), unsupported.status());
    }

    private static void assertRejectedBeforeAllocation(byte[] bytes, ProfileWireValidationException.Reason reason) {
        AtomicInteger allocations = new AtomicInteger();
        ProfileWireValidationException exception = assertThrows(
                ProfileWireValidationException.class,
                () -> ProfileFrameCodec.decode(
                        bytes,
                        ProfileFrameCodec.DecodePhase.PRE_HELLO,
                        size -> {
                            allocations.incrementAndGet();
                            return new byte[size];
                        }));
        assertEquals(reason, exception.reason());
        assertEquals(0, allocations.get());
    }

    private static void assertReason(ProfileWireValidationException.Reason reason, Runnable action) {
        assertEquals(reason, assertThrows(ProfileWireValidationException.class, action::run).reason());
    }

    private static byte[] lengthOnly(int outerLength) {
        return ByteBuffer.allocate(4).putInt(outerLength).array();
    }

    private static byte[] mutateU16(byte[] original, int offset, int value) {
        byte[] copy = original.clone();
        ByteBuffer.wrap(copy).putShort(offset, (short) value);
        return copy;
    }

    private static byte[] mutateU32(byte[] original, int offset, int value) {
        byte[] copy = original.clone();
        ByteBuffer.wrap(copy).putInt(offset, value);
        return copy;
    }
}

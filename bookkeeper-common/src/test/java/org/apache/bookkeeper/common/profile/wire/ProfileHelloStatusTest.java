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
import java.util.List;
import org.junit.Test;

public class ProfileHelloStatusTest {

    @Test
    public void clientHelloIsByteExactAndUnsignedOrdered() {
        ProfileHello.Client client = new ProfileHello.Client(List.of(
                new ProfileHello.Capability(1, 1),
                new ProfileHello.Capability(0xffff_ffffL, 0xffff)));
        byte[] encoded = client.encode();
        assertEquals(28, encoded.length);
        assertEquals(client, ProfileHello.decodeClient(encoded));
        assertEquals(0xffff_ffffL, ProfileHello.decodeClient(encoded).capabilities().get(1).capabilityId());

        byte[] duplicate = encoded.clone();
        ByteBuffer.wrap(duplicate).putInt(20, 1);
        assertReason(
                ProfileWireValidationException.Reason.DUPLICATE_CAPABILITY,
                () -> ProfileHello.decodeClient(duplicate));
        byte[] outOfOrder = encoded.clone();
        ByteBuffer.wrap(outOfOrder).putInt(12, -1).putInt(20, 1);
        assertReason(
                ProfileWireValidationException.Reason.OUT_OF_ORDER_CAPABILITY,
                () -> ProfileHello.decodeClient(outOfOrder));
    }

    @Test
    public void serverHelloIsByteExactAndStrictUtf8() {
        byte[] incarnation = sequence(16, 1);
        ProfileHello.Server server = new ProfileHello.Server(
                2,
                incarnation,
                0x0102_0304_0506_0708L,
                "bk1",
                List.of(new ProfileHello.Capability(7, 2)));
        byte[] encoded = server.encode();
        assertEquals(47, encoded.length);
        assertEquals(server, ProfileHello.decodeServer(encoded));
        assertArrayEquals(incarnation, ProfileHello.decodeServer(encoded).storageIncarnation());

        byte[] nul = encoded.clone();
        nul[36] = 0;
        assertReason(ProfileWireValidationException.Reason.NUL_IN_BOOKIE_ID, () -> ProfileHello.decodeServer(nul));
        byte[] invalidUtf8 = encoded.clone();
        invalidUtf8[36] = (byte) 0xc0;
        assertReason(ProfileWireValidationException.Reason.INVALID_UTF8, () -> ProfileHello.decodeServer(invalidUtf8));
        byte[] zeroIncarnation = encoded.clone();
        java.util.Arrays.fill(zeroIncarnation, 12, 28, (byte) 0);
        assertReason(
                ProfileWireValidationException.Reason.ZERO_STORAGE_INCARNATION,
                () -> ProfileHello.decodeServer(zeroIncarnation));
    }

    @Test
    public void statusMatrixUsesFrozenNumericValues() {
        for (ProfileStatus.StatusClass statusClass : ProfileStatus.StatusClass.values()) {
            for (ProfileStatus.RetryDisposition retry : ProfileStatus.RetryDisposition.values()) {
                for (ProfileStatus.DurableResult durable : ProfileStatus.DurableResult.values()) {
                    ProfileStatus status = new ProfileStatus(statusClass, retry, durable, 0xabcd);
                    byte[] bytes = status.encode();
                    assertEquals(8, bytes.length);
                    assertEquals(status, ProfileStatus.decode(bytes));
                    assertEquals(statusClass.wireValue(), Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort()));
                }
            }
        }
        byte[] invalid = ProfileStatus.unsupported().encode();
        invalid[7] = 1;
        assertReason(ProfileWireValidationException.Reason.NONZERO_RESERVED, () -> ProfileStatus.decode(invalid));
    }

    private static byte[] sequence(int length, int start) {
        byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (start + index);
        }
        return result;
    }

    private static void assertReason(ProfileWireValidationException.Reason reason, Runnable action) {
        assertEquals(reason, assertThrows(ProfileWireValidationException.class, action::run).reason());
    }
}

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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ProfileOperationCodecTest {

    private static final ProfileLedgerContext CONTEXT = new ProfileLedgerContext(
            0x0102_0304_0506_0708L, sequence(16, 1), sequence(36, 17));
    private static final byte[] CREDENTIAL = sequence(20, 71);

    @Test
    public void roundTripsDistinctFrozenDataBodies() {
        ProfileOperationCodec.AddNormal normal = new ProfileOperationCodec.AddNormal(
                CONTEXT, 11, 0xffff_ffffL, CREDENTIAL, new byte[] {1, 2, 3});
        ProfileOperationCodec.AddNormal decodedNormal = (ProfileOperationCodec.AddNormal)
                roundTrip(ProfileOperation.ADD_NORMAL, normal);
        assertEquals(11, decodedNormal.entryId());
        assertEquals(0xffff_ffffL, decodedNormal.writeFlags());
        assertArrayEquals(new byte[] {1, 2, 3}, decodedNormal.entryPayload());

        ProfileOperationCodec.AddRecovery recovery = new ProfileOperationCodec.AddRecovery(
                CONTEXT, sequence(16, 101), 3, 4, 10, 20, 11, CREDENTIAL, new byte[] {4, 5});
        ProfileOperationCodec.AddRecovery decodedRecovery = (ProfileOperationCodec.AddRecovery)
                roundTrip(ProfileOperation.ADD_RECOVERY, recovery);
        assertEquals(10, decodedRecovery.rangeStart());
        assertEquals(20, decodedRecovery.rangeEnd());
        assertEquals(11, decodedRecovery.entryId());

        roundTrip(ProfileOperation.READ_ENTRY, new ProfileOperationCodec.ReadEntry(CONTEXT, 11, 3));
        roundTrip(
                ProfileOperation.FENCE_LEDGER,
                new ProfileOperationCodec.FenceLedger(CONTEXT, sequence(16, 121), CREDENTIAL));
        roundTrip(ProfileOperation.READ_LAC, new ProfileOperationCodec.ReadLac(CONTEXT));
        roundTrip(
                ProfileOperation.WRITE_LAC,
                new ProfileOperationCodec.WriteLac(CONTEXT, 9, CREDENTIAL, new byte[] {6}));
        roundTrip(ProfileOperation.FORCE_LEDGER, new ProfileOperationCodec.ForceLedger(CONTEXT, sequence(16, 31)));
        roundTrip(ProfileOperation.LIST_ENTRIES, new ProfileOperationCodec.ListEntries(CONTEXT));
    }

    @Test
    public void normalAndRecoveryAreNotInterchangeable() {
        byte[] normal = ProfileOperationCodec.encode(
                ProfileOperation.ADD_NORMAL,
                new ProfileOperationCodec.AddNormal(CONTEXT, 11, 0, CREDENTIAL, new byte[] {1}));
        byte[] recovery = ProfileOperationCodec.encode(
                ProfileOperation.ADD_RECOVERY,
                new ProfileOperationCodec.AddRecovery(
                        CONTEXT, sequence(16, 101), 3, 4, 10, 20, 11, CREDENTIAL, new byte[] {1}));
        assertThrows(
                ProfileWireValidationException.class,
                () -> ProfileOperationCodec.decode(ProfileOperation.ADD_RECOVERY, normal));
        assertThrows(
                ProfileWireValidationException.class,
                () -> ProfileOperationCodec.decode(ProfileOperation.ADD_NORMAL, recovery));
    }

    @Test
    public void controlTailsStayBlockedAndReservationsStayUnsupported() {
        for (ProfileOperation operation : ProfileOperation.values()) {
            if (operation.isControl()) {
                assertEquals(
                        ProfileWireValidationException.Reason.BLOCKED_UNFROZEN_CONTROL_BODY,
                        assertThrows(
                                        ProfileWireValidationException.class,
                                        () -> ProfileOperationCodec.decode(operation, new byte[0]))
                                .reason());
            }
        }
        for (ProfileOperation operation : new ProfileOperation[] {
            ProfileOperation.RANGE_READ, ProfileOperation.BATCH_RECOVERY_ADD
        }) {
            ProfileOperationCodec.Unsupported unsupported =
                    (ProfileOperationCodec.Unsupported) ProfileOperationCodec.decode(operation, new byte[0]);
            assertEquals(ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                    unsupported.status().statusClass());
            assertArrayEquals(new byte[0], ProfileOperationCodec.encode(operation, unsupported));
        }
    }

    @Test
    public void ledgerContextIsExactlySixtyBytesAndSecretFreeInText() {
        assertEquals(60, CONTEXT.encode().length);
        assertEquals(CONTEXT, ProfileLedgerContext.decode(CONTEXT.encode()));
        assertTrue(CONTEXT.toString().contains("<opaque>"));
    }

    private static ProfileOperationCodec.Body roundTrip(
            ProfileOperation operation, ProfileOperationCodec.Body original) {
        byte[] encoded = ProfileOperationCodec.encode(operation, original);
        return ProfileOperationCodec.decode(operation, encoded);
    }

    private static byte[] sequence(int length, int start) {
        byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (start + index);
        }
        return result;
    }
}

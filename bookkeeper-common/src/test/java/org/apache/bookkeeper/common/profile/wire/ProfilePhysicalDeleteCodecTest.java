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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.junit.Test;

public class ProfilePhysicalDeleteCodecTest {
    private static final ProfileHello.Server HELLO = new ProfileHello.Server(3, bytes(16, 7), 0,
            "isolated:3181", List.of(ProfilePhysicalDeleteCodec.CAPABILITY));

    private static byte[] bytes(int size, int value) {
        byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfilePhysicalDeleteCodec.Request request(int maximum) {
        var validator = new ProfileDescriptorValidator((id, version) -> false,
                (id, generation) -> id == 7 && generation == 11);
        var descriptor = new ProfileDescriptor(EngineProfile.SEGMENT_WAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK, 3, 3, 2, 0, 7, 11, List.of());
        var operation = ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY;
        var base = new ProfileControlScope(operation, operation.purpose(), 44, bytes(16, 1),
                ProfileDescriptorCodec.identityOf(descriptor, validator), HELLO.bookieId(),
                HELLO.storageIncarnation(), "ledger-profile", 1, bytes(16, 3), bytes(32, 4), null);
        var binding = new ProfileControlRequest.DeleteBinding(bytes(16, 8), 1, 1, 2, bytes(16, 9), 1, bytes(32, 10));
        var scope = new ProfileControlScope(operation, operation.purpose(), base.ledgerId(), base.ledgerInstanceId(),
                base.descriptorIdentity(), base.targetBookie(), base.targetStorageIncarnation(), base.authorityDomain(),
                base.authorityGeneration(), base.operationId(),
                ProfileControlRequest.deletePayloadIdentity(base, binding), null);
        return new ProfilePhysicalDeleteCodec.Request(ProfileControlRequest.deleteApply(scope, binding),
                maximum, 8, 1000);
    }

    @Test
    public void boundedRequestKeepsExactEventAndRejectsMalformedLimitsAndShape() {
        var request = request(64);
        byte[] encoded = ProfilePhysicalDeleteCodec.encodeRequest(request);
        var decoded = ProfilePhysicalDeleteCodec.decodeRequest(ByteBuffer.wrap(encoded), HELLO);
        assertEquals(request.deletion().scope(), decoded.deletion().scope());
        assertEquals(request.deletion().deleteBinding(), decoded.deletion().deleteBinding());
        assertEquals(request.maximumAllocations(), decoded.maximumAllocations());
        for (int size = 0; size < encoded.length; size++) {
            byte[] cut = Arrays.copyOf(encoded, size);
            assertThrows(IllegalArgumentException.class,
                    () -> ProfilePhysicalDeleteCodec.decodeRequest(ByteBuffer.wrap(cut), HELLO));
        }
        for (int offset : List.of(0, 4, 6, 8, 12, 16, 20)) {
            byte[] wrong = encoded.clone();
            Arrays.fill(wrong, offset, offset + (offset == 4 || offset == 6 ? 2 : 4), (byte) 0xff);
            assertThrows(IllegalArgumentException.class,
                    () -> ProfilePhysicalDeleteCodec.decodeRequest(ByteBuffer.wrap(wrong), HELLO));
        }
        assertThrows(IllegalArgumentException.class, () -> request(0));
        assertThrows(IllegalArgumentException.class, () -> request(4097));
    }

    @Test
    public void replyBindsWholeRequestAndRejectsCorruptionAndFalseSuccess() {
        var request = request(64);
        var reply = new ProfilePhysicalDeleteCodec.Reply(
                ProfilePhysicalDeleteCodec.status(ProfilePhysicalDeleteCodec.Outcome.REUSABLE),
                ProfilePhysicalDeleteCodec.Outcome.REUSABLE, 4, 1, 1, 4096, 0, 12, 3);
        byte[] encoded = ProfilePhysicalDeleteCodec.encodeReply(request, reply);
        assertEquals(reply, ProfilePhysicalDeleteCodec.decodeReply(ByteBuffer.wrap(encoded),
                ProfilePhysicalDeleteCodec.requestIdentity(request)));
        assertThrows(IllegalArgumentException.class, () -> ProfilePhysicalDeleteCodec.decodeReply(
                ByteBuffer.wrap(encoded), ProfilePhysicalDeleteCodec.requestIdentity(request(65))));
        for (int offset = 0; offset < encoded.length; offset++) {
            byte[] wrong = encoded.clone();
            wrong[offset] ^= 1;
            assertThrows(IllegalArgumentException.class, () -> ProfilePhysicalDeleteCodec.decodeReply(
                    ByteBuffer.wrap(wrong), ProfilePhysicalDeleteCodec.requestIdentity(request)));
        }
        byte[] wrongStatus = encoded.clone();
        System.arraycopy(ProfileStatus.unsupported().encode(), 0, wrongStatus, 0, ProfileStatus.ENCODED_LENGTH);
        CRC32C crc = new CRC32C();
        crc.update(wrongStatus, 0, wrongStatus.length - 4);
        ByteBuffer.wrap(wrongStatus).putInt(wrongStatus.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfilePhysicalDeleteCodec.decodeReply(
                ByteBuffer.wrap(wrongStatus), ProfilePhysicalDeleteCodec.requestIdentity(request)));
        assertThrows(IllegalArgumentException.class, () -> new ProfilePhysicalDeleteCodec.Reply(
                reply.status(), reply.outcome(), 0, 0, 0, 0, 0, -1, -1));
        assertEquals(ProfileStatus.DurableResult.UNKNOWN,
                ProfilePhysicalDeleteCodec.status(ProfilePhysicalDeleteCodec.Outcome.PENDING).durableResult());
    }
}

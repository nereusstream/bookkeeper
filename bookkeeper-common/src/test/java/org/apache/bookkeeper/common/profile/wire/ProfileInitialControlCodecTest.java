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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.junit.Test;

public class ProfileInitialControlCodecTest {
    private static final ProfileDescriptorValidator VALIDATOR = new ProfileDescriptorValidator(
            (id, version) -> false, (id, generation) -> id == 7 && generation == 11);
    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(EngineProfile.SEGMENT_WAL_ENGINE,
            PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK, 3, 3, 2, 0, 7, 11, List.of());
    private static final ProfileHello.Server HELLO = new ProfileHello.Server(3, bytes(16, 7), 0,
            "isolated:3181", List.of(new ProfileHello.Capability(ProfileInitialControlCodec.CAPABILITY_ID, 1)));

    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileControlScope scope(ProfileControlOperation operation) {
        return new ProfileControlScope(operation, operation.purpose(), 44, bytes(16, 1),
                ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR), HELLO.bookieId(),
                HELLO.storageIncarnation(), "ledger-profile", 1, bytes(16, 3), bytes(32, 4), null);
    }

    private static ProfileControlRequest install() {
        return ProfileControlRequest.install(scope(ProfileControlOperation.INSTALL),
                ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR), EngineProfile.SEGMENT_WAL_ENGINE,
                ProtectedProfileCredential.of(1, bytes(20, 5)));
    }

    private static byte[] vector(String name) throws IOException {
        try (InputStream input = ProfileInitialControlCodecTest.class.getClassLoader()
                .getResourceAsStream("profile-wire/initial-control/" + name + "-schema1.bin")) {
            return input.readAllBytes();
        }
    }

    @Test
    public void standaloneBoundaryCloseOwnsEmptyPrefixAndCannotReplayAsGrantClose() throws Exception {
        byte[] vector;
        try (var in = getClass().getResourceAsStream("/profile-wire/terminal-boundary-close.bin")) {
            vector = in.readAllBytes();
        }
        var request = ProfileInitialControlCodec.decodeRequest(ProfileOperation.RECOVERY_TERMINAL_CLOSE,
                ByteBuffer.wrap(vector), HELLO);
        assertEquals(188, vector.length);
        assertEquals(-1, request.terminalBinding().orElseThrow().prefixEntryId());
        assertEquals(0, request.terminalBinding().orElseThrow().prefixLength());
        assertTrue(request.recoveryBinding().isEmpty());
        assertTrue(request.closedGrantOperationId().isEmpty());
        assertEquals(null, request.scope().exactRange());
        assertArrayEquals(vector, ProfileInitialControlCodec.encodeRequest(request));
        byte[] owned = request.terminalBinding().orElseThrow().selectionDigest();
        owned[0] = 0;
        assertArrayEquals(bytes(32, 9), request.terminalBinding().orElseThrow().selectionDigest());
        for (int size = 0; size < vector.length; size++) {
            byte[] cut = Arrays.copyOf(vector, size);
            assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                    ProfileOperation.RECOVERY_TERMINAL_CLOSE, ByteBuffer.wrap(cut), HELLO));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.RECOVERY_GRANT_CLOSE, ByteBuffer.wrap(vector), HELLO));
        byte[] changed = vector.clone();
        ByteBuffer.wrap(changed).putLong(vector.length - 48, 0);
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.RECOVERY_TERMINAL_CLOSE, ByteBuffer.wrap(changed), HELLO));
        assertThrows(IllegalArgumentException.class, () -> new ProfileControlRequest.TerminalBinding(-1, 1,
                bytes(32, 9)));
        assertThrows(IllegalArgumentException.class, () -> new ProfileControlRequest.TerminalBinding(Long.MAX_VALUE,
                0, bytes(32, 9)));
        var result = new ProfileControlResult(ProfileControlResult.Outcome.APPLIED, request.scope().operation(),
                44, HexFormat.of().formatHex(request.scope().operationId()), 2, 1, true);
        byte[] reply = ProfileInitialControlCodec.encodeReply(result);
        assertEquals(result, ProfileInitialControlCodec.decodeReply(ProfileOperation.RECOVERY_TERMINAL_CLOSE, reply));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeReply(
                ProfileOperation.RECOVERY_GRANT_CLOSE, reply));
    }

    @Test
    public void recoveryCloseUsesIndependentSchemaFourAndCannotReplayAsGrant() throws Exception {
        byte[] bytes;
        try (var in = getClass().getResourceAsStream("/profile-wire/recovery-close.bin")) {
            bytes = in.readAllBytes();
        }
        var request = ProfileInitialControlCodec.decodeRequest(ProfileOperation.RECOVERY_GRANT_CLOSE,
                ByteBuffer.wrap(bytes), HELLO);
        assertEquals(191, bytes.length);
        assertEquals(ProfileControlOperation.RECOVERY_GRANT_CLOSE, request.scope().operation());
        assertEquals(2, request.scope().authorityGeneration());
        assertArrayEquals(bytes(16, 8), request.closedGrantOperationId().orElseThrow());
        byte[] borrowed = request.closedGrantOperationId().orElseThrow();
        borrowed[0] = 0;
        assertArrayEquals(bytes(16, 8), request.closedGrantOperationId().orElseThrow());
        assertArrayEquals(bytes, ProfileInitialControlCodec.encodeRequest(request));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.RECOVERY_GRANT, ByteBuffer.wrap(bytes), HELLO));
        for (int length : List.of(190, 319)) {
            assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                    ProfileOperation.RECOVERY_GRANT_CLOSE, ByteBuffer.wrap(Arrays.copyOf(bytes, length)), HELLO));
        }
        byte[] oldSchema = bytes.clone();
        ByteBuffer.wrap(oldSchema).putShort(64, (short) 3);
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.RECOVERY_GRANT_CLOSE, ByteBuffer.wrap(oldSchema), HELLO));
        assertThrows(IllegalArgumentException.class, () -> ProfileControlRequest.recoveryClose(request.scope(),
                request.recoveryBinding().orElseThrow(), request.scope().operationId()));
        var result = new ProfileControlResult(ProfileControlResult.Outcome.APPLIED, request.scope().operation(),
                44, HexFormat.of().formatHex(request.scope().operationId()), 2, 1, true);
        byte[] reply = ProfileInitialControlCodec.encodeReply(result);
        assertEquals(result, ProfileInitialControlCodec.decodeReply(ProfileOperation.RECOVERY_GRANT_CLOSE, reply));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeReply(
                ProfileOperation.RECOVERY_GRANT, reply));
    }

    @Test
    public void recoveryGrantUsesIndependentSchemaThreeVectorAndBoundedIntentFields() throws Exception {
        byte[] bytes;
        try (var in = getClass().getResourceAsStream("/profile-wire/recovery-grant.bin")) {
            bytes = in.readAllBytes();
        }
        var request = ProfileInitialControlCodec.decodeRequest(ProfileOperation.RECOVERY_GRANT,
                ByteBuffer.wrap(bytes), HELLO);
        assertEquals(175, bytes.length);
        assertEquals(ProfileControlOperation.RECOVERY_GRANT, request.scope().operation());
        assertEquals(0, request.scope().exactRange().firstEntryId());
        assertEquals(4, request.scope().exactRange().lastEntryId());
        assertEquals(1, request.recoveryBinding().orElseThrow().intentGeneration());
        assertArrayEquals(bytes, ProfileInitialControlCodec.encodeRequest(request));
        byte[] altered = bytes.clone();
        ByteBuffer.wrap(altered).putShort(64, (short) 1);
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.RECOVERY_GRANT, ByteBuffer.wrap(altered), HELLO));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.ACTIVATE_INITIAL, ByteBuffer.wrap(bytes), HELLO));
        var result = new ProfileControlResult(ProfileControlResult.Outcome.APPLIED, request.scope().operation(),
                request.scope().ledgerId(), HexFormat.of().formatHex(request.scope().operationId()),
                request.scope().authorityGeneration(), 2, true);
        assertEquals(result, ProfileInitialControlCodec.decodeReply(ProfileOperation.RECOVERY_GRANT,
                ProfileInitialControlCodec.encodeReply(result)));
    }

    @Test
    public void replacementSchemaHasItsOwnFixedBytesAndCannotReplayInitialActivation() throws Exception {
        var initial = scope(ProfileControlOperation.ACTIVATE_INITIAL);
        var replacement = new ProfileControlScope(ProfileControlOperation.ACTIVATE_REPLACEMENT,
                ProfileControlOperation.ACTIVATE_REPLACEMENT.purpose(), initial.ledgerId(), initial.ledgerInstanceId(),
                initial.descriptorIdentity(), initial.targetBookie(), initial.targetStorageIncarnation(),
                initial.authorityDomain(), initial.authorityGeneration(), initial.operationId(),
                initial.publicSemanticPayloadIdentity(), null);
        byte[] expected = vector("activate");
        ByteBuffer.wrap(expected).putShort(64, (short) 2);
        assertArrayEquals(expected,
                ProfileInitialControlCodec.encodeRequest(ProfileControlRequest.operation(replacement)));
        assertEquals(replacement, ProfileInitialControlCodec.decodeRequest(ProfileOperation.ACTIVATE_REPLACEMENT,
                ByteBuffer.wrap(expected), HELLO).scope());
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.ACTIVATE_INITIAL, ByteBuffer.wrap(expected), HELLO));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.ACTIVATE_REPLACEMENT, ByteBuffer.wrap(vector("activate")), HELLO));
        var result = new ProfileControlResult(ProfileControlResult.Outcome.APPLIED,
                ProfileControlOperation.ACTIVATE_REPLACEMENT, replacement.ledgerId(),
                HexFormat.of().formatHex(replacement.operationId()), replacement.authorityGeneration(),
                7, true);
        byte[] reply = vector("applied");
        ByteBuffer.wrap(reply).putShort(12, (short) 2);
        assertArrayEquals(reply, ProfileInitialControlCodec.encodeReply(result));
        assertEquals(result, ProfileInitialControlCodec.decodeReply(ProfileOperation.ACTIVATE_REPLACEMENT, reply));
        assertThrows(IllegalArgumentException.class,
                () -> ProfileInitialControlCodec.decodeReply(ProfileOperation.ACTIVATE_INITIAL, reply));
    }

    @Test
    public void exactLayoutRoundTripsAndBindsOnlyTheVerifiedPeer() throws Exception {
        byte[] body = ProfileInitialControlCodec.encodeRequest(install());
        assertArrayEquals(vector("install"), body);
        assertArrayEquals(vector("activate"), ProfileInitialControlCodec.encodeRequest(
                ProfileControlRequest.operation(scope(ProfileControlOperation.ACTIVATE_INITIAL))));
        ProfileControlResult result = new ProfileControlResult(ProfileControlResult.Outcome.APPLIED,
                ProfileControlOperation.INSTALL, 44, "03030303030303030303030303030303", 1, 7, true);
        assertArrayEquals(vector("applied"), ProfileInitialControlCodec.encodeReply(result));
        ByteBuffer independent = ByteBuffer.wrap(body);
        assertEquals(44, independent.getLong());
        assertEquals(0x55574943, independent.getInt(60));
        assertEquals(1, independent.getShort(64));
        assertEquals(0, independent.getShort(66));
        assertEquals(1, independent.getLong(116));
        assertEquals(14, independent.getShort(124));
        assertEquals(3, independent.getShort(144));
        assertEquals(1, independent.getShort(146));
        assertEquals(20, independent.getShort(148));
        assertArrayEquals(bytes(20, 5), Arrays.copyOfRange(body, 150, 170));
        ProfileControlRequest decoded = ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.INSTALL, ByteBuffer.wrap(body), HELLO);
        assertEquals(install().scope(), decoded.scope());
        assertTrue(decoded.protectedCredential().orElseThrow().matchesWireVerifier(bytes(20, 5)));
        assertArrayEquals(install().canonicalDescriptor().orElseThrow(), decoded.canonicalDescriptor().orElseThrow());
        assertArrayEquals(body, ProfileInitialControlCodec.encodeRequest(decoded));
        assertThrows(ProfileWireValidationException.class,
                () -> ProfileOperationCodec.decode(ProfileOperation.INSTALL, body));
        ProfileHello.Server different = new ProfileHello.Server(3, bytes(16, 9), 0, "other:3181", List.of());
        assertEquals("other:3181", ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.INSTALL, ByteBuffer.wrap(body), different).scope().targetBookie());
    }

    @Test
    public void rejectsReservedUnknownLengthTrailingAndMalformedUtf8() {
        byte[] original = ProfileInitialControlCodec.encodeRequest(install());
        for (int position : new int[] {60, 64, 66, 146, 148}) {
            byte[] bad = original.clone();
            bad[position] ^= 0x40;
            assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                    ProfileOperation.INSTALL, ByteBuffer.wrap(bad), HELLO));
        }
        byte[] utf8 = original.clone();
        utf8[126] = (byte) 0xff;
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.INSTALL, ByteBuffer.wrap(utf8), HELLO));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.INSTALL, ByteBuffer.wrap(Arrays.copyOf(original, original.length + 1)), HELLO));
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.ACTIVATE_INITIAL, ByteBuffer.wrap(original), HELLO));
        byte[] activation = ProfileInitialControlCodec.encodeRequest(
                ProfileControlRequest.operation(scope(ProfileControlOperation.ACTIVATE_INITIAL)));
        assertEquals(140, activation.length);
        assertEquals(scope(ProfileControlOperation.ACTIVATE_INITIAL), ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.ACTIVATE_INITIAL, ByteBuffer.wrap(activation), HELLO).scope());
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.decodeRequest(
                ProfileOperation.INSTALL, ByteBuffer.wrap(activation), HELLO));
    }

    @Test
    public void repliesPreserveUnknownAndRejectFakeDurabilityOrInconsistentStatus() {
        for (ProfileControlResult.Outcome outcome : ProfileControlResult.Outcome.values()) {
            boolean success = outcome == ProfileControlResult.Outcome.APPLIED
                    || outcome == ProfileControlResult.Outcome.ALREADY_APPLIED;
            ProfileControlResult result = new ProfileControlResult(outcome, ProfileControlOperation.INSTALL,
                    44, "03030303030303030303030303030303", 1, success ? 7 : 0, success);
            byte[] body = ProfileInitialControlCodec.encodeReply(result);
            assertEquals(60, body.length);
            assertEquals(result, ProfileInitialControlCodec.decodeReply(ProfileOperation.INSTALL, body));
            body[3] ^= 1;
            assertThrows(IllegalArgumentException.class,
                    () -> ProfileInitialControlCodec.decodeReply(ProfileOperation.INSTALL, body));
        }
        assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlCodec.encodeReply(
                new ProfileControlResult(ProfileControlResult.Outcome.APPLIED, ProfileControlOperation.INSTALL,
                        44, "03030303030303030303030303030303", 1, 7, false)));
    }
}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Deterministic authoritative Profile wire corpus builder. */
final class ProfileWireCorpusFixtures {

    static final long FUZZ_SEED = 0x5057_4655_5a5a_0001L;
    static final int FUZZ_ITERATIONS = 256;
    static final String LICENSE = "Licensed to the Apache Software Foundation (ASF) under one or more "
            + "contributor license agreements. See the NOTICE file distributed with this work for additional "
            + "information regarding copyright ownership. The ASF licenses this file to you under the Apache "
            + "License, Version 2.0.";

    private static final ProfileLedgerContext CONTEXT = new ProfileLedgerContext(
            0x0102_0304_0506_0708L, sequence(16, 1), sequence(36, 17));
    private static final byte[] CREDENTIAL = sequence(20, 71);

    private ProfileWireCorpusFixtures() {}

    static List<Vector> allVectors() {
        List<Vector> vectors = new ArrayList<>();
        addValid(vectors);
        addInvalidFraming(vectors);
        addInvalidHello(vectors);
        addInvalidTypedBodies(vectors);
        addAdversarialLegacyPrefixes(vectors);
        addFuzz(vectors);
        return List.copyOf(vectors);
    }

    private static void addValid(List<Vector> vectors) {
        ProfileHello.Client clientHello = new ProfileHello.Client(List.of(
                new ProfileHello.Capability(1, 1),
                new ProfileHello.Capability(0xffff_ffffL, 0xffff)));
        addAccept(vectors, "valid/hello-client", frame(ProfileOperation.HELLO, 1, clientHello.encode()),
                Phase.PRE_HELLO, Typed.HELLO_CLIENT, "-");

        ProfileHello.Server serverHello = new ProfileHello.Server(
                2,
                sequence(16, 91),
                0x1112_1314_1516_1718L,
                "bookie-1.example:3181",
                List.of(new ProfileHello.Capability(1, 1)));
        ProfileFrameHeader serverHeader = new ProfileFrameHeader(
                1,
                0,
                32,
                ProfileOperation.HELLO,
                ProfileFrameHeader.RESPONSE,
                1,
                serverHello.encode().length,
                0);
        addAccept(vectors, "valid/hello-server", ProfileFrameCodec.encode(
                new ProfileFrame(serverHeader, serverHello.encode())), Phase.PRE_HELLO, Typed.HELLO_SERVER, "-");

        ProfileOperationCodec.AddNormal normal = new ProfileOperationCodec.AddNormal(
                CONTEXT, 11, 0xffff_ffffL, CREDENTIAL, new byte[] {1, 2, 3, 4});
        addData(vectors, "add-normal", ProfileOperation.ADD_NORMAL, normal);
        ProfileOperationCodec.AddRecovery recovery = new ProfileOperationCodec.AddRecovery(
                CONTEXT, sequence(16, 101), 3, 4, 10, 20, 11, CREDENTIAL, new byte[] {5, 6, 7});
        addData(vectors, "add-recovery", ProfileOperation.ADD_RECOVERY, recovery);
        addData(vectors, "read-entry", ProfileOperation.READ_ENTRY,
                new ProfileOperationCodec.ReadEntry(CONTEXT, 11, 3));
        addData(vectors, "fence-ledger", ProfileOperation.FENCE_LEDGER,
                new ProfileOperationCodec.FenceLedger(CONTEXT, sequence(16, 121), CREDENTIAL));
        addData(vectors, "read-lac", ProfileOperation.READ_LAC, new ProfileOperationCodec.ReadLac(CONTEXT));
        addData(vectors, "write-lac", ProfileOperation.WRITE_LAC,
                new ProfileOperationCodec.WriteLac(CONTEXT, 9, CREDENTIAL, new byte[] {8, 9}));
        addData(vectors, "force-ledger", ProfileOperation.FORCE_LEDGER,
                new ProfileOperationCodec.ForceLedger(CONTEXT, sequence(16, 31)));
        addData(vectors, "list-entries", ProfileOperation.LIST_ENTRIES,
                new ProfileOperationCodec.ListEntries(CONTEXT));

        for (ProfileOperation operation : ProfileOperation.values()) {
            if (operation.isControl()) {
                addAccept(vectors, "valid/control-header-" + lower(operation), frame(operation, 100, new byte[0]),
                        Phase.ESTABLISHED, Typed.BLOCKED_CONTROL,
                        ProfileWireValidationException.Reason.BLOCKED_UNFROZEN_CONTROL_BODY.name());
            } else if (operation.isReservedDisabled()) {
                addAccept(vectors, "valid/reserved-disabled-" + lower(operation), frame(operation, 101, new byte[0]),
                        Phase.ESTABLISHED, Typed.UNSUPPORTED, "1/0/0/0");
            }
        }

        for (ProfileStatus.StatusClass statusClass : ProfileStatus.StatusClass.values()) {
            for (ProfileStatus.RetryDisposition retry : ProfileStatus.RetryDisposition.values()) {
                for (ProfileStatus.DurableResult durable : ProfileStatus.DurableResult.values()) {
                    ProfileStatus status = new ProfileStatus(statusClass, retry, durable, 0x1234);
                    ProfileFrameHeader header = new ProfileFrameHeader(
                            1,
                            0,
                            32,
                            ProfileOperation.OPERATION_STATUS,
                            ProfileFrameHeader.RESPONSE | (statusClass == ProfileStatus.StatusClass.OK
                                    ? 0
                                    : ProfileFrameHeader.ERROR),
                            200,
                            ProfileStatus.ENCODED_LENGTH,
                            0);
                    addAccept(
                            vectors,
                            "valid/status-" + statusClass.wireValue() + '-' + retry.wireValue() + '-'
                                    + durable.wireValue(),
                            ProfileFrameCodec.encode(new ProfileFrame(header, status.encode())),
                            Phase.ESTABLISHED,
                            Typed.STATUS,
                            statusClass.wireValue() + "/" + retry.wireValue() + "/" + durable.wireValue()
                                    + "/4660");
                }
            }
        }
    }

    private static void addInvalidFraming(List<Vector> vectors) {
        byte[] hello = frame(ProfileOperation.HELLO, 1, new ProfileHello.Client(List.of()).encode());
        for (int index = 0; index < 4; index++) {
            byte[] mutated = hello.clone();
            mutated[4 + index] ^= (byte) 0xff;
            addFrameReject(vectors, "invalid/magic-flip-" + index, mutated, Phase.PRE_HELLO,
                    ProfileWireValidationException.Reason.BAD_MAGIC);
        }
        for (int length = 1; length <= 31; length++) {
            byte[] truncated = Arrays.copyOf(hello, 4 + length);
            ByteBuffer.wrap(truncated).putInt(0, 32);
            addFrameReject(vectors, "invalid/header-truncation-" + String.format("%02d", length), truncated,
                    Phase.PRE_HELLO, ProfileWireValidationException.Reason.TRUNCATED_HEADER);
        }
        int bodyLength = hello.length - 36;
        for (int length = 0; length < bodyLength; length++) {
            addFrameReject(vectors, "invalid/body-truncation-" + String.format("%02d", length),
                    Arrays.copyOf(hello, 36 + length), Phase.PRE_HELLO,
                    ProfileWireValidationException.Reason.TRUNCATED_BODY);
        }
        addFrameReject(vectors, "invalid/outer-too-small", mutateU32(hello, 0, 31), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "invalid/outer-body-mismatch", mutateU32(hello, 0, hello.length - 5),
                Phase.PRE_HELLO, ProfileWireValidationException.Reason.OUTER_LENGTH_MISMATCH);
        addFrameReject(vectors, "invalid/header-body-mismatch", mutateU32(hello, 28, bodyLength + 1),
                Phase.PRE_HELLO, ProfileWireValidationException.Reason.OUTER_LENGTH_MISMATCH);
        addFrameReject(vectors, "invalid/trailing-byte", Arrays.copyOf(hello, hello.length + 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.TRAILING_BYTES);
        addFrameReject(vectors, "invalid/protocol-major", mutateU16(hello, 8, 2), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        addFrameReject(vectors, "invalid/protocol-minor", mutateU16(hello, 10, 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        addFrameReject(vectors, "invalid/header-length", mutateU16(hello, 12, 31), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.WRONG_HEADER_LENGTH);
        addFrameReject(vectors, "invalid/unknown-flags", mutateU32(hello, 16, 8), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.UNKNOWN_FLAGS);
        addFrameReject(vectors, "invalid/header-reserved", mutateU32(hello, 32, 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.NONZERO_RESERVED);
        addFrameReject(vectors, "invalid/unknown-subtype", mutateU16(hello, 14, 0xffff), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.UNKNOWN_SUBTYPE);
        addFrameReject(vectors, "invalid/hello-not-first", frame(ProfileOperation.READ_LAC, 1, new byte[0]),
                Phase.PRE_HELLO, ProfileWireValidationException.Reason.HELLO_NOT_FIRST);
        addFrameReject(vectors, "invalid/pre-hello-oversize",
                declaredFrame(ProfileOperation.HELLO, ProfileFrameCodec.PRE_HELLO_FRAME_MAX + 1),
                Phase.PRE_HELLO, ProfileWireValidationException.Reason.PRE_HELLO_FRAME_TOO_LARGE);
        addFrameReject(vectors, "invalid/control-oversize",
                declaredFrame(ProfileOperation.INSTALL, ProfileFrameCodec.CONTROL_FRAME_MAX + 1),
                Phase.ESTABLISHED, ProfileWireValidationException.Reason.CONTROL_FRAME_TOO_LARGE);
        addFrameReject(vectors, "invalid/absolute-oversize",
                lengthOnly(ProfileFrameCodec.ABSOLUTE_FRAME_MAX + 1), Phase.ESTABLISHED,
                ProfileWireValidationException.Reason.ABSOLUTE_FRAME_TOO_LARGE);
        addFrameReject(vectors, "invalid/outer-u32-overflow", lengthOnly(-1), Phase.ESTABLISHED,
                ProfileWireValidationException.Reason.OUTER_LENGTH_OVERFLOW);
        addFrameReject(vectors, "invalid/reserved-range-body",
                frameUnchecked(ProfileOperation.RANGE_READ, new byte[] {1}),
                Phase.ESTABLISHED, ProfileWireValidationException.Reason.RESERVED_OPERATION_BODY);
    }

    private static void addInvalidHello(List<Vector> vectors) {
        byte[] clientBody = new ProfileHello.Client(List.of(
                new ProfileHello.Capability(1, 1), new ProfileHello.Capability(2, 1))).encode();
        addTypedReject(vectors, "invalid/hello-client-reserved", ProfileOperation.HELLO,
                mutateU16(clientBody, 10, 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.NONZERO_RESERVED);
        addTypedReject(vectors, "invalid/hello-client-count-65", ProfileOperation.HELLO,
                mutateU16(clientBody, 8, 65), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.CAPABILITY_COUNT_OUT_OF_RANGE);
        addTypedReject(vectors, "invalid/hello-client-zero-capability", ProfileOperation.HELLO,
                mutateU32(clientBody, 12, 0), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.INVALID_CAPABILITY);
        addTypedReject(vectors, "invalid/hello-client-zero-version", ProfileOperation.HELLO,
                mutateU16(clientBody, 16, 0), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.INVALID_CAPABILITY);
        addTypedReject(vectors, "invalid/hello-client-capability-flags", ProfileOperation.HELLO,
                mutateU16(clientBody, 18, 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.INVALID_HELLO_FLAGS);
        addTypedReject(vectors, "invalid/hello-client-duplicate", ProfileOperation.HELLO,
                mutateU32(clientBody, 20, 1), Phase.PRE_HELLO,
                ProfileWireValidationException.Reason.DUPLICATE_CAPABILITY);
        byte[] outOfOrder = clientBody.clone();
        ByteBuffer.wrap(outOfOrder).putInt(12, 2).putInt(20, 1);
        addTypedReject(vectors, "invalid/hello-client-out-of-order", ProfileOperation.HELLO, outOfOrder,
                Phase.PRE_HELLO, ProfileWireValidationException.Reason.OUT_OF_ORDER_CAPABILITY);

        ProfileHello.Server server = new ProfileHello.Server(
                1, sequence(16, 91), 7, "bk1", List.of(new ProfileHello.Capability(1, 1)));
        byte[] serverBody = server.encode();
        addTypedServerReject(vectors, "invalid/hello-server-reserved", mutateU16(serverBody, 6, 1),
                ProfileWireValidationException.Reason.NONZERO_RESERVED);
        byte[] zeroIncarnation = serverBody.clone();
        Arrays.fill(zeroIncarnation, 12, 28, (byte) 0);
        addTypedServerReject(vectors, "invalid/hello-server-zero-incarnation", zeroIncarnation,
                ProfileWireValidationException.Reason.ZERO_STORAGE_INCARNATION);
        byte[] nulBookie = serverBody.clone();
        nulBookie[36] = 0;
        addTypedServerReject(vectors, "invalid/hello-server-nul-bookie", nulBookie,
                ProfileWireValidationException.Reason.NUL_IN_BOOKIE_ID);
        byte[] invalidUtf8 = serverBody.clone();
        invalidUtf8[36] = (byte) 0xc0;
        addTypedServerReject(vectors, "invalid/hello-server-invalid-utf8", invalidUtf8,
                ProfileWireValidationException.Reason.INVALID_UTF8);
        addTypedServerReject(vectors, "invalid/hello-server-zero-bookie-length", mutateU16(serverBody, 8, 0),
                ProfileWireValidationException.Reason.INVALID_BOOKIE_ID_LENGTH);

        for (int offset : new int[] {0, 1, 2, 3}) {
            byte[] status = ProfileStatus.unsupported().encode();
            if (offset == 0) {
                status[1] = 12;
            } else if (offset == 1) {
                status[2] = 5;
            } else if (offset == 2) {
                status[3] = 4;
            } else {
                status[7] = 1;
            }
            ProfileWireValidationException.Reason reason = switch (offset) {
                case 0 -> ProfileWireValidationException.Reason.UNKNOWN_STATUS_CLASS;
                case 1 -> ProfileWireValidationException.Reason.UNKNOWN_RETRY_DISPOSITION;
                case 2 -> ProfileWireValidationException.Reason.UNKNOWN_DURABLE_RESULT;
                default -> ProfileWireValidationException.Reason.NONZERO_RESERVED;
            };
            addStatusReject(vectors, "invalid/status-" + offset, status, reason);
        }
    }

    private static void addInvalidTypedBodies(List<Vector> vectors) {
        byte[] normal = ProfileOperationCodec.encode(
                ProfileOperation.ADD_NORMAL,
                new ProfileOperationCodec.AddNormal(CONTEXT, 11, 0, CREDENTIAL, new byte[] {1}));
        byte[] recovery = ProfileOperationCodec.encode(
                ProfileOperation.ADD_RECOVERY,
                new ProfileOperationCodec.AddRecovery(
                        CONTEXT, sequence(16, 101), 3, 4, 10, 20, 11, CREDENTIAL, new byte[] {1}));
        addTypedDataReject(vectors, "invalid/normal-as-recovery", ProfileOperation.ADD_RECOVERY, normal,
                ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        addTypedDataReject(vectors, "invalid/recovery-as-normal", ProfileOperation.ADD_NORMAL, recovery,
                ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        addTypedDataReject(vectors, "invalid/normal-credential-kind", ProfileOperation.ADD_NORMAL,
                mutateU16(normal, 72, 2), ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        addTypedDataReject(vectors, "invalid/normal-entry-length", ProfileOperation.ADD_NORMAL,
                mutateU32(normal, 96, 2), ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        byte[] zeroInstance = normal.clone();
        Arrays.fill(zeroInstance, 8, 24, (byte) 0);
        addTypedDataReject(vectors, "invalid/normal-zero-instance", ProfileOperation.ADD_NORMAL, zeroInstance,
                ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        byte[] invalidRange = recovery.clone();
        ByteBuffer.wrap(invalidRange).putLong(92, 30).putLong(100, 20);
        addTypedDataReject(vectors, "invalid/recovery-range", ProfileOperation.ADD_RECOVERY, invalidRange,
                ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        byte[] zeroIntent = recovery.clone();
        Arrays.fill(zeroIntent, 60, 76, (byte) 0);
        addTypedDataReject(vectors, "invalid/recovery-zero-intent", ProfileOperation.ADD_RECOVERY, zeroIntent,
                ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
    }

    private static void addAdversarialLegacyPrefixes(List<Vector> vectors) {
        addFrameReject(vectors, "adversarial/tls-client-hello",
                hex("160301002e0100002a0303aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"),
                Phase.ESTABLISHED, ProfileWireValidationException.Reason.ABSOLUTE_FRAME_TOO_LARGE);
        addFrameReject(vectors, "adversarial/v3-prefix-profile-magic",
                hex("0000000c0803100118000ffe4250"), Phase.ESTABLISHED,
                ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/v3-invalid-wire-type",
                hex("000000010f"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/v3-truncated-varint",
                hex("0000000180"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/v3-invalid-length-delimited",
                hex("0000000212ff"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/prev3-version-zero-add-prefix",
                hex("0000000400010000"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/prev3-version-two-add-prefix",
                hex("0000000402010000"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        addFrameReject(vectors, "adversarial/prev3-near-add-opcode",
                hex("0000000402fe0000"), Phase.ESTABLISHED, ProfileWireValidationException.Reason.FRAME_TOO_SMALL);
        byte[] profileThenLegacy = concatenate(
                frame(ProfileOperation.READ_LAC, 77, CONTEXT.encode()),
                hex("0000000402010000"));
        addFrameReject(vectors, "adversarial/profile-then-legacy-add-prefix", profileThenLegacy,
                Phase.ESTABLISHED, ProfileWireValidationException.Reason.TRAILING_BYTES);
    }

    private static void addFuzz(List<Vector> vectors) {
        Random random = new Random(FUZZ_SEED);
        for (int iteration = 0; iteration < FUZZ_ITERATIONS; iteration++) {
            byte[] bytes = new byte[random.nextInt(1_025)];
            random.nextBytes(bytes);
            Phase phase = random.nextBoolean() ? Phase.PRE_HELLO : Phase.ESTABLISHED;
            ProfileWireValidationException.Reason reason;
            try {
                ProfileFrameCodec.decode(bytes, phase.decodePhase());
                addAccept(vectors, "fuzz/seeded-" + String.format("%03d", iteration), bytes, phase,
                        Typed.NONE, "-");
                continue;
            } catch (ProfileWireValidationException exception) {
                reason = exception.reason();
            }
            addFrameReject(vectors, "fuzz/seeded-" + String.format("%03d", iteration), bytes, phase, reason);
        }
    }

    private static void addData(
            List<Vector> vectors, String name, ProfileOperation operation, ProfileOperationCodec.Body body) {
        addAccept(vectors, "valid/" + name,
                frame(operation, 10, ProfileOperationCodec.encode(operation, body)),
                Phase.ESTABLISHED, Typed.DATA, "-");
    }

    private static void addTypedReject(
            List<Vector> vectors,
            String name,
            ProfileOperation operation,
            byte[] body,
            Phase phase,
            ProfileWireValidationException.Reason reason) {
        addAccept(vectors, name, frame(operation, 301, body), phase, Typed.HELLO_CLIENT_REJECT, reason.name(), true);
    }

    private static void addTypedServerReject(
            List<Vector> vectors, String name, byte[] body, ProfileWireValidationException.Reason reason) {
        ProfileFrameHeader header = new ProfileFrameHeader(
                1, 0, 32, ProfileOperation.HELLO, ProfileFrameHeader.RESPONSE, 302, body.length, 0);
        addAccept(vectors, name, ProfileFrameCodec.encode(new ProfileFrame(header, body)), Phase.PRE_HELLO,
                Typed.HELLO_SERVER_REJECT, reason.name(), true);
    }

    private static void addStatusReject(
            List<Vector> vectors, String name, byte[] status, ProfileWireValidationException.Reason reason) {
        ProfileFrameHeader header = new ProfileFrameHeader(
                1, 0, 32, ProfileOperation.OPERATION_STATUS, ProfileFrameHeader.RESPONSE, 303, status.length, 0);
        addAccept(vectors, name, ProfileFrameCodec.encode(new ProfileFrame(header, status)), Phase.ESTABLISHED,
                Typed.STATUS_REJECT, reason.name(), true);
    }

    private static void addTypedDataReject(
            List<Vector> vectors,
            String name,
            ProfileOperation operation,
            byte[] body,
            ProfileWireValidationException.Reason reason) {
        addAccept(vectors, name, frame(operation, 304, body), Phase.ESTABLISHED,
                Typed.DATA_REJECT, reason.name(), true);
    }

    private static void addFrameReject(
            List<Vector> vectors,
            String name,
            byte[] bytes,
            Phase phase,
            ProfileWireValidationException.Reason reason) {
        vectors.add(new Vector(name, bytes, phase, FrameResult.REJECT, reason.name(), Typed.SKIP, "-", true));
    }

    private static void addAccept(
            List<Vector> vectors, String name, byte[] bytes, Phase phase, Typed typed, String typedExpectation) {
        addAccept(vectors, name, bytes, phase, typed, typedExpectation, false);
    }

    private static void addAccept(
            List<Vector> vectors,
            String name,
            byte[] bytes,
            Phase phase,
            Typed typed,
            String typedExpectation,
            boolean close) {
        vectors.add(new Vector(name, bytes, phase, FrameResult.ACCEPT, "-", typed, typedExpectation, close));
    }

    private static byte[] frame(ProfileOperation operation, long requestId, byte[] body) {
        return ProfileFrameCodec.encode(ProfileFrame.request(operation, requestId, body));
    }

    private static byte[] frameUnchecked(ProfileOperation operation, byte[] body) {
        byte[] bytes = frame(ProfileOperation.READ_ENTRY, 1, body);
        return mutateU16(bytes, 14, operation.wireValue());
    }

    private static byte[] declaredFrame(ProfileOperation operation, int outerLength) {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(outerLength);
        buffer.putInt(ProfileFrameHeader.MAGIC);
        buffer.putShort((short) 1);
        buffer.putShort((short) 0);
        buffer.putShort((short) 32);
        buffer.putShort((short) operation.wireValue());
        buffer.putInt(0);
        buffer.putLong(1);
        buffer.putInt(outerLength - 32);
        buffer.putInt(0);
        return buffer.array();
    }

    private static byte[] lengthOnly(int length) {
        return ByteBuffer.allocate(4).putInt(length).array();
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

    private static byte[] concatenate(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static byte[] sequence(int length, int start) {
        byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (start + index);
        }
        return result;
    }

    private static byte[] hex(String value) {
        return java.util.HexFormat.of().parseHex(value);
    }

    private static String lower(ProfileOperation operation) {
        return operation.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
    }

    enum Phase {
        PRE_HELLO,
        ESTABLISHED;

        ProfileFrameCodec.DecodePhase decodePhase() {
            return ProfileFrameCodec.DecodePhase.valueOf(name());
        }
    }

    enum FrameResult {
        ACCEPT,
        REJECT
    }

    enum Typed {
        NONE,
        SKIP,
        HELLO_CLIENT,
        HELLO_SERVER,
        HELLO_CLIENT_REJECT,
        HELLO_SERVER_REJECT,
        DATA,
        DATA_REJECT,
        BLOCKED_CONTROL,
        UNSUPPORTED,
        STATUS,
        STATUS_REJECT
    }

    record Vector(
            String id,
            byte[] bytes,
            Phase phase,
            FrameResult frameResult,
            String frameReason,
            Typed typed,
            String typedExpectation,
            boolean close) {

        Vector {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        String resourcePath() {
            return id + ".bin";
        }

        String fixtureLine() {
            return String.join(
                    "\t",
                    id,
                    resourcePath(),
                    phase.name(),
                    frameResult.name(),
                    frameReason,
                    typed.name(),
                    typedExpectation,
                    Boolean.toString(close),
                    Integer.toString(bytes.length));
        }
    }
}

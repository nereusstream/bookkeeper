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

/** Typed codec for only the data operation bodies frozen by the executable test manifest. */
public final class ProfileOperationCodec {

    public static final int CREDENTIAL_KIND = 1;
    public static final int CREDENTIAL_LENGTH = 20;
    public static final int OPERATION_ID_LENGTH = 16;

    public interface Body {}

    public record ReadEntry(ProfileLedgerContext context, long entryId, long readFlags) implements Body {
        public ReadEntry {
            Objects.requireNonNull(context, "context");
            requireU32(readFlags);
        }
    }

    public record ReadLac(ProfileLedgerContext context) implements Body {
        public ReadLac {
            Objects.requireNonNull(context, "context");
        }
    }

    public record ListEntries(ProfileLedgerContext context) implements Body {
        public ListEntries {
            Objects.requireNonNull(context, "context");
        }
    }

    public record Unsupported(ProfileOperation operation, ProfileStatus status) implements Body {
        public Unsupported {
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(status, "status");
            if (!operation.isReservedDisabled()) {
                throw failure(ProfileWireValidationException.Reason.OPERATION_BODY_MISMATCH);
            }
        }
    }

    public static final class AddNormal implements Body {
        private final ProfileLedgerContext context;
        private final long entryId;
        private final long writeFlags;
        private final byte[] credential;
        private final byte[] entryPayload;

        public AddNormal(
                ProfileLedgerContext context,
                long entryId,
                long writeFlags,
                byte[] credential,
                byte[] entryPayload) {
            this.context = Objects.requireNonNull(context, "context");
            this.entryId = entryId;
            this.writeFlags = requireU32(writeFlags);
            this.credential = exactCopy(credential, CREDENTIAL_LENGTH);
            this.entryPayload = Objects.requireNonNull(entryPayload, "entryPayload").clone();
        }

        public ProfileLedgerContext context() {
            return context;
        }

        public long entryId() {
            return entryId;
        }

        public long writeFlags() {
            return writeFlags;
        }

        public byte[] credential() {
            return credential.clone();
        }

        public byte[] entryPayload() {
            return entryPayload.clone();
        }
    }

    public static final class AddRecovery implements Body {
        private final ProfileLedgerContext context;
        private final byte[] repairIntentId;
        private final long repairIntentGeneration;
        private final long grantGeneration;
        private final long rangeStart;
        private final long rangeEnd;
        private final long entryId;
        private final byte[] credential;
        private final byte[] entryPayload;

        public AddRecovery(
                ProfileLedgerContext context,
                byte[] repairIntentId,
                long repairIntentGeneration,
                long grantGeneration,
                long rangeStart,
                long rangeEnd,
                long entryId,
                byte[] credential,
                byte[] entryPayload) {
            this.context = Objects.requireNonNull(context, "context");
            this.repairIntentId = nonzeroId(repairIntentId);
            this.repairIntentGeneration = repairIntentGeneration;
            this.grantGeneration = grantGeneration;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.entryId = entryId;
            if (rangeStart > rangeEnd || entryId < rangeStart || entryId > rangeEnd) {
                throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
            }
            this.credential = exactCopy(credential, CREDENTIAL_LENGTH);
            this.entryPayload = Objects.requireNonNull(entryPayload, "entryPayload").clone();
        }

        public ProfileLedgerContext context() {
            return context;
        }

        public byte[] repairIntentId() {
            return repairIntentId.clone();
        }

        public long repairIntentGeneration() {
            return repairIntentGeneration;
        }

        public long grantGeneration() {
            return grantGeneration;
        }

        public long rangeStart() {
            return rangeStart;
        }

        public long rangeEnd() {
            return rangeEnd;
        }

        public long entryId() {
            return entryId;
        }

        public byte[] credential() {
            return credential.clone();
        }

        public byte[] entryPayload() {
            return entryPayload.clone();
        }
    }

    public static final class FenceLedger implements Body {
        private final ProfileLedgerContext context;
        private final byte[] operationId;
        private final byte[] credential;

        public FenceLedger(ProfileLedgerContext context, byte[] operationId, byte[] credential) {
            this.context = Objects.requireNonNull(context, "context");
            this.operationId = nonzeroId(operationId);
            this.credential = exactCopy(credential, CREDENTIAL_LENGTH);
        }

        public ProfileLedgerContext context() {
            return context;
        }

        public byte[] operationId() {
            return operationId.clone();
        }

        public byte[] credential() {
            return credential.clone();
        }
    }

    public static final class WriteLac implements Body {
        private final ProfileLedgerContext context;
        private final long lac;
        private final byte[] credential;
        private final byte[] body;

        public WriteLac(ProfileLedgerContext context, long lac, byte[] credential, byte[] body) {
            this.context = Objects.requireNonNull(context, "context");
            this.lac = lac;
            this.credential = exactCopy(credential, CREDENTIAL_LENGTH);
            this.body = Objects.requireNonNull(body, "body").clone();
        }

        public ProfileLedgerContext context() {
            return context;
        }

        public long lac() {
            return lac;
        }

        public byte[] credential() {
            return credential.clone();
        }

        public byte[] body() {
            return body.clone();
        }
    }

    public static final class ForceLedger implements Body {
        private final ProfileLedgerContext context;
        private final byte[] operationId;

        public ForceLedger(ProfileLedgerContext context, byte[] operationId) {
            this.context = Objects.requireNonNull(context, "context");
            this.operationId = nonzeroId(operationId);
        }

        public ProfileLedgerContext context() {
            return context;
        }

        public byte[] operationId() {
            return operationId.clone();
        }
    }

    private ProfileOperationCodec() {}

    public static byte[] encode(ProfileOperation operation, Body body) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(body, "body");
        return switch (operation) {
            case ADD_NORMAL -> encodeAddNormal(require(body, AddNormal.class));
            case ADD_RECOVERY -> encodeAddRecovery(require(body, AddRecovery.class));
            case READ_ENTRY -> encodeReadEntry(require(body, ReadEntry.class));
            case FENCE_LEDGER -> encodeFence(require(body, FenceLedger.class));
            case READ_LAC -> require(body, ReadLac.class).context().encode();
            case WRITE_LAC -> encodeWriteLac(require(body, WriteLac.class));
            case FORCE_LEDGER -> encodeForce(require(body, ForceLedger.class));
            case LIST_ENTRIES -> require(body, ListEntries.class).context().encode();
            case RANGE_READ, BATCH_RECOVERY_ADD -> {
                require(body, Unsupported.class);
                yield new byte[0];
            }
            case INSTALL,
                    ACTIVATE_INITIAL,
                    ACTIVATE_REPLACEMENT,
                    RECOVERY_GRANT,
                    RECOVERY_GRANT_CLOSE,
                    TOMBSTONE_OR_DELETE_APPLY,
                    OPERATION_STATUS -> throw failure(
                    ProfileWireValidationException.Reason.BLOCKED_UNFROZEN_CONTROL_BODY);
            case HELLO -> throw failure(ProfileWireValidationException.Reason.OPERATION_BODY_MISMATCH);
        };
    }

    public static Body decode(ProfileOperation operation, byte[] body) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(body, "body");
        return switch (operation) {
            case ADD_NORMAL -> decodeAddNormal(body);
            case ADD_RECOVERY -> decodeAddRecovery(body);
            case READ_ENTRY -> decodeReadEntry(body);
            case FENCE_LEDGER -> decodeFence(body);
            case READ_LAC -> new ReadLac(decodeContextOnly(body));
            case WRITE_LAC -> decodeWriteLac(body);
            case FORCE_LEDGER -> decodeForce(body);
            case LIST_ENTRIES -> new ListEntries(decodeContextOnly(body));
            case RANGE_READ, BATCH_RECOVERY_ADD -> {
                if (body.length != 0) {
                    throw failure(ProfileWireValidationException.Reason.RESERVED_OPERATION_BODY);
                }
                yield new Unsupported(operation, ProfileStatus.unsupported());
            }
            case INSTALL,
                    ACTIVATE_INITIAL,
                    ACTIVATE_REPLACEMENT,
                    RECOVERY_GRANT,
                    RECOVERY_GRANT_CLOSE,
                    TOMBSTONE_OR_DELETE_APPLY,
                    OPERATION_STATUS -> throw failure(
                    ProfileWireValidationException.Reason.BLOCKED_UNFROZEN_CONTROL_BODY);
            case HELLO -> throw failure(ProfileWireValidationException.Reason.OPERATION_BODY_MISMATCH);
        };
    }

    private static byte[] encodeAddNormal(AddNormal value) {
        byte[] payload = value.entryPayload();
        ByteBuffer buffer = allocate(100, payload.length);
        putContext(buffer, value.context());
        buffer.putLong(value.entryId());
        buffer.putInt((int) value.writeFlags());
        putCredential(buffer, value.credential());
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    private static AddNormal decodeAddNormal(byte[] bytes) {
        ByteBuffer buffer = requireMinimum(bytes, 100);
        ProfileLedgerContext context = ProfileLedgerContext.decode(buffer);
        long entryId = buffer.getLong();
        long writeFlags = Integer.toUnsignedLong(buffer.getInt());
        byte[] credential = readCredential(buffer);
        byte[] payload = readLengthPrefixedTail(buffer);
        return new AddNormal(context, entryId, writeFlags, credential, payload);
    }

    private static byte[] encodeAddRecovery(AddRecovery value) {
        byte[] payload = value.entryPayload();
        ByteBuffer buffer = allocate(144, payload.length);
        putContext(buffer, value.context());
        buffer.put(value.repairIntentId());
        buffer.putLong(value.repairIntentGeneration());
        buffer.putLong(value.grantGeneration());
        buffer.putLong(value.rangeStart());
        buffer.putLong(value.rangeEnd());
        buffer.putLong(value.entryId());
        putCredential(buffer, value.credential());
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    private static AddRecovery decodeAddRecovery(byte[] bytes) {
        ByteBuffer buffer = requireMinimum(bytes, 144);
        ProfileLedgerContext context = ProfileLedgerContext.decode(buffer);
        byte[] repairIntent = readBytes(buffer, OPERATION_ID_LENGTH);
        long repairIntentGeneration = buffer.getLong();
        long grantGeneration = buffer.getLong();
        long rangeStart = buffer.getLong();
        long rangeEnd = buffer.getLong();
        long entryId = buffer.getLong();
        byte[] credential = readCredential(buffer);
        byte[] payload = readLengthPrefixedTail(buffer);
        return new AddRecovery(
                context,
                repairIntent,
                repairIntentGeneration,
                grantGeneration,
                rangeStart,
                rangeEnd,
                entryId,
                credential,
                payload);
    }

    private static byte[] encodeReadEntry(ReadEntry value) {
        ByteBuffer buffer = ByteBuffer.allocate(72);
        putContext(buffer, value.context());
        buffer.putLong(value.entryId());
        buffer.putInt((int) value.readFlags());
        return buffer.array();
    }

    private static ReadEntry decodeReadEntry(byte[] bytes) {
        ByteBuffer buffer = requireExact(bytes, 72);
        return new ReadEntry(
                ProfileLedgerContext.decode(buffer), buffer.getLong(), Integer.toUnsignedLong(buffer.getInt()));
    }

    private static byte[] encodeFence(FenceLedger value) {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        putContext(buffer, value.context());
        buffer.put(value.operationId());
        putCredential(buffer, value.credential());
        return buffer.array();
    }

    private static FenceLedger decodeFence(byte[] bytes) {
        ByteBuffer buffer = requireExact(bytes, 100);
        return new FenceLedger(
                ProfileLedgerContext.decode(buffer),
                readBytes(buffer, OPERATION_ID_LENGTH),
                readCredential(buffer));
    }

    private static byte[] encodeWriteLac(WriteLac value) {
        byte[] body = value.body();
        ByteBuffer buffer = allocate(96, body.length);
        putContext(buffer, value.context());
        buffer.putLong(value.lac());
        putCredential(buffer, value.credential());
        buffer.putInt(body.length);
        buffer.put(body);
        return buffer.array();
    }

    private static WriteLac decodeWriteLac(byte[] bytes) {
        ByteBuffer buffer = requireMinimum(bytes, 96);
        ProfileLedgerContext context = ProfileLedgerContext.decode(buffer);
        long lac = buffer.getLong();
        byte[] credential = readCredential(buffer);
        return new WriteLac(context, lac, credential, readLengthPrefixedTail(buffer));
    }

    private static byte[] encodeForce(ForceLedger value) {
        ByteBuffer buffer = ByteBuffer.allocate(76);
        putContext(buffer, value.context());
        buffer.put(value.operationId());
        return buffer.array();
    }

    private static ForceLedger decodeForce(byte[] bytes) {
        ByteBuffer buffer = requireExact(bytes, 76);
        return new ForceLedger(ProfileLedgerContext.decode(buffer), readBytes(buffer, OPERATION_ID_LENGTH));
    }

    private static ProfileLedgerContext decodeContextOnly(byte[] bytes) {
        return ProfileLedgerContext.decode(requireExact(bytes, ProfileLedgerContext.ENCODED_LENGTH));
    }

    private static void putContext(ByteBuffer buffer, ProfileLedgerContext context) {
        buffer.put(context.encode());
    }

    private static void putCredential(ByteBuffer buffer, byte[] credential) {
        buffer.putShort((short) CREDENTIAL_KIND);
        buffer.putShort((short) CREDENTIAL_LENGTH);
        buffer.put(credential);
    }

    private static byte[] readCredential(ByteBuffer buffer) {
        if (buffer.remaining() < 4 + CREDENTIAL_LENGTH
                || Short.toUnsignedInt(buffer.getShort()) != CREDENTIAL_KIND
                || Short.toUnsignedInt(buffer.getShort()) != CREDENTIAL_LENGTH) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return readBytes(buffer, CREDENTIAL_LENGTH);
    }

    private static byte[] readLengthPrefixedTail(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        long length = Integer.toUnsignedLong(buffer.getInt());
        if (length != buffer.remaining()) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return readBytes(buffer, (int) length);
    }

    private static byte[] readBytes(ByteBuffer buffer, int length) {
        if (length < 0 || buffer.remaining() < length) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        byte[] result = new byte[length];
        buffer.get(result);
        return result;
    }

    private static ByteBuffer requireExact(byte[] bytes, int length) {
        if (bytes.length != length) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return ByteBuffer.wrap(bytes);
    }

    private static ByteBuffer requireMinimum(byte[] bytes, int minimum) {
        if (bytes.length < minimum) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return ByteBuffer.wrap(bytes);
    }

    private static ByteBuffer allocate(int fixedLength, int variableLength) {
        long total = (long) fixedLength + variableLength;
        if (variableLength < 0 || total > Integer.MAX_VALUE) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return ByteBuffer.allocate((int) total);
    }

    private static byte[] exactCopy(byte[] value, int length) {
        if (value == null || value.length != length) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return value.clone();
    }

    private static byte[] nonzeroId(byte[] value) {
        byte[] copy = exactCopy(value, OPERATION_ID_LENGTH);
        if (ProfileWireCodecSupport.allZero(copy)) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return copy;
    }

    private static long requireU32(long value) {
        if ((value & 0xffff_ffff_0000_0000L) != 0) {
            throw failure(ProfileWireValidationException.Reason.INVALID_OPERATION_BODY);
        }
        return value;
    }

    private static <T extends Body> T require(Body body, Class<T> type) {
        if (!type.isInstance(body)) {
            throw failure(ProfileWireValidationException.Reason.OPERATION_BODY_MISMATCH);
        }
        return type.cast(body);
    }

    private static ProfileWireValidationException failure(ProfileWireValidationException.Reason reason) {
        return ProfileWireCodecSupport.failure(reason);
    }
}

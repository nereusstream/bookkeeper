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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;

/** UIC8 opt-in bounded local physical retirement, bound to a committed UIC7 delete event.
 * REUSABLE means Arena reuse only; it grants neither global deletion nor history GC. */
public final class ProfilePhysicalDeleteCodec {
    public static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(0x55494338L, 1);
    public static final int PREFIX_BYTES = 24;
    public static final int MIN_REQUEST_BYTES = PREFIX_BYTES
            + ProfileInitialControlCodec.minimumRequestBytes(ProfileOperation.TOMBSTONE_OR_DELETE_APPLY);
    public static final int MAX_REQUEST_BYTES = PREFIX_BYTES
            + ProfileInitialControlCodec.maximumRequestBytes(ProfileOperation.TOMBSTONE_OR_DELETE_APPLY);
    public static final int REPLY_BYTES = 96;
    private static final int MAGIC = 0x55575044;

    public enum Outcome { REUSABLE, NOT_DELETED, PENDING, LIMIT, UNKNOWN }

    public record Request(ProfileControlRequest deletion, int maximumAllocations,
            int maximumCompactions, int timeoutMillis) {
        public Request {
            Objects.requireNonNull(deletion, "deletion");
            if (deletion.scope().operation() != ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY
                    || deletion.deleteBinding().isEmpty() || maximumAllocations < 1 || maximumAllocations > 4096
                    || maximumCompactions < 1 || maximumCompactions > 256 || timeoutMillis < 1
                    || timeoutMillis > 60_000) {
                throw invalid();
            }
        }
    }

    public record Reply(ProfileStatus status, Outcome outcome, int scannedAllocations, int compactions,
            int freedAllocations, long sourceBytesRetired, long destinationBytes, long bookieCut, long arenaCut) {
        public Reply {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(outcome, "outcome");
            if (!status.equals(ProfilePhysicalDeleteCodec.status(outcome)) || scannedAllocations < 0 || compactions < 0
                    || freedAllocations < 0 || sourceBytesRetired < 0 || destinationBytes < 0
                    || bookieCut < -1 || arenaCut < -1
                    || outcome == Outcome.REUSABLE && (bookieCut < 0 || arenaCut < 0)) {
                throw invalid();
            }
        }
    }

    private ProfilePhysicalDeleteCodec() { }

    public static ProfileStatus status(Outcome outcome) {
        return switch (outcome) {
            case REUSABLE -> new ProfileStatus(ProfileStatus.StatusClass.OK,
                    ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.APPLIED, 0);
            case NOT_DELETED -> new ProfileStatus(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                    ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, ProfileStatus.DurableResult.NONE, 0);
            case PENDING, LIMIT, UNKNOWN -> new ProfileStatus(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                    ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, ProfileStatus.DurableResult.UNKNOWN, 0);
        };
    }

    public static byte[] encodeRequest(Request request) {
        byte[] event = ProfileInitialControlCodec.encodeRequest(request.deletion());
        return ByteBuffer.allocate(PREFIX_BYTES + event.length).putInt(MAGIC).putShort((short) 1)
                .putShort((short) 0).putInt(event.length).putInt(request.maximumAllocations())
                .putInt(request.maximumCompactions()).putInt(request.timeoutMillis()).put(event).array();
    }

    public static Request decodeRequest(ByteBuffer body, ProfileHello.Server verifiedLocalHello) {
        if (body.remaining() < MIN_REQUEST_BYTES || body.remaining() > MAX_REQUEST_BYTES) {
            throw invalid();
        }
        ByteBuffer in = body.slice();
        if (in.getInt() != MAGIC || in.getShort() != 1 || in.getShort() != 0
                || in.getInt() != in.remaining() - 12) {
            throw invalid();
        }
        int allocations = in.getInt();
        int compactions = in.getInt();
        int timeout = in.getInt();
        var deletion = ProfileInitialControlCodec.decodeRequest(ProfileOperation.TOMBSTONE_OR_DELETE_APPLY,
                in.slice(), verifiedLocalHello);
        return new Request(deletion, allocations, compactions, timeout);
    }

    public static byte[] requestIdentity(Request request) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(encodeRequest(request));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException(unavailable);
        }
    }

    public static byte[] encodeReply(Request request, Reply reply) {
        ByteBuffer out = ByteBuffer.allocate(REPLY_BYTES).put(reply.status().encode()).putInt(MAGIC)
                .putShort((short) 1).putShort((short) reply.outcome().ordinal()).put(requestIdentity(request))
                .putInt(reply.scannedAllocations()).putInt(reply.compactions()).putInt(reply.freedAllocations())
                .putLong(reply.sourceBytesRetired()).putLong(reply.destinationBytes())
                .putLong(reply.bookieCut()).putLong(reply.arenaCut());
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, REPLY_BYTES - 4);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    public static Reply decodeReply(ByteBuffer body, byte[] expectedRequestIdentity) {
        if (body.remaining() != REPLY_BYTES || expectedRequestIdentity == null
                || expectedRequestIdentity.length != 32) {
            throw invalid();
        }
        ByteBuffer in = body.slice();
        CRC32C crc = new CRC32C();
        crc.update(in.duplicate().limit(REPLY_BYTES - 4));
        if (in.getInt(REPLY_BYTES - 4) != (int) crc.getValue()) {
            throw invalid();
        }
        byte[] encodedStatus = new byte[ProfileStatus.ENCODED_LENGTH];
        in.get(encodedStatus);
        var status = ProfileStatus.decode(encodedStatus);
        if (in.getInt() != MAGIC || in.getShort() != 1) {
            throw invalid();
        }
        int kind = in.getShort() & 0xffff;
        byte[] identity = new byte[32];
        in.get(identity);
        if (kind >= Outcome.values().length || !Arrays.equals(identity, expectedRequestIdentity)) {
            throw invalid();
        }
        return new Reply(status, Outcome.values()[kind], in.getInt(), in.getInt(), in.getInt(),
                in.getLong(), in.getLong(), in.getLong(), in.getLong());
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid bounded Profile physical delete body");
    }
}

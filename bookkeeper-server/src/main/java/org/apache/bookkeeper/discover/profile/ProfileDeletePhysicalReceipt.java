/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfilePhysicalDeleteCodec;

/** Immutable bounded receipt collected from the actual verified UIC8 peer.
 * Stored local Arena reuse is not secure erase, a decommission proof or history-GC authority. */
public final class ProfileDeletePhysicalReceipt {
    private static final int MAGIC = 0x55575054;
    private static final int PREFIX_BYTES = 132;
    static final int MAX_BYTES = PREFIX_BYTES + ProfileDeleteDelivery.MAX_BYTES
            + ProfilePhysicalDeleteCodec.MAX_REQUEST_BYTES;
    private final byte[] birthId;
    private final ProfileDeleteDelivery.Event event;
    private final ProfilePhysicalDeleteCodec.Request request;
    private final ProfilePhysicalDeleteCodec.Reply reply;

    private ProfileDeletePhysicalReceipt(byte[] birthId, ProfileDeleteDelivery.Event event,
            ProfilePhysicalDeleteCodec.Request request, ProfilePhysicalDeleteCodec.Reply reply) {
        this.birthId = ProfileLifecycleHead.fixed(birthId, 16, true);
        this.event = Objects.requireNonNull(event, "event");
        this.request = Objects.requireNonNull(request, "request");
        this.reply = Objects.requireNonNull(reply, "reply");
        if (reply.outcome() != ProfilePhysicalDeleteCodec.Outcome.REUSABLE
                || !request.deletion().scope().equals(event.deleteApply(
                    request.deletion().scope().authorityDomain()).scope())
                || !request.deletion().deleteBinding().equals(event.deleteApply(
                    request.deletion().scope().authorityDomain()).deleteBinding())) {
            throw invalid();
        }
    }

    static ProfileDeletePhysicalReceipt fromVerifiedReply(ProfileDeleteDelivery.Event event,
            ProfileHello.Server hello, ProfilePhysicalDeleteCodec.Request request,
            ProfilePhysicalDeleteCodec.Reply reply) {
        if (hello.activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                || !hello.capabilities().contains(ProfilePhysicalDeleteCodec.CAPABILITY)
                || !hello.bookieId().equals(event.target().bookie())
                || !Arrays.equals(hello.storageIncarnation(), event.target().incarnation())) {
            throw invalid();
        }
        UUID id = UUID.randomUUID();
        return new ProfileDeletePhysicalReceipt(ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(),
                event, request, reply);
    }

    public byte[] birthId() {
        return birthId.clone();
    }

    public ProfileDeleteDelivery.Event event() {
        return event;
    }

    public ProfilePhysicalDeleteCodec.Request request() {
        return request;
    }

    public ProfilePhysicalDeleteCodec.Reply reply() {
        return reply;
    }

    boolean matches(ProfileDeleteRecord.Manifest manifest, String authorityDomain) {
        return event.ledger().equals(manifest.ledger()) && manifest.targets().contains(event.target())
                && event.deleteEpoch() == manifest.seal().epoch()
                && Arrays.equals(event.requestId(), manifest.seal().requestId())
                && Arrays.equals(event.tombstone().digest(), ProfileRepairIntent.digest(manifest.encode()))
                && Arrays.equals(event.tombstone().sealDigest(), ProfileRepairIntent.digest(manifest.seal().encode()))
                && request.deletion().scope().authorityDomain().equals(authorityDomain);
    }

    public byte[] encode() {
        byte[] eventBytes = event.encode();
        byte[] requestBytes = ProfilePhysicalDeleteCodec.encodeRequest(request);
        var out = ByteBuffer.allocate(PREFIX_BYTES + eventBytes.length + requestBytes.length)
                .putInt(MAGIC).putShort((short) 1).putShort((short) 0).put(birthId)
                .putInt(eventBytes.length).put(eventBytes).putInt(requestBytes.length).put(requestBytes)
                .put(ProfilePhysicalDeleteCodec.encodeReply(request, reply));
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        if (out.position() != out.capacity() - 4) {
            throw invalid();
        }
        return out.putInt((int) crc.getValue()).array();
    }

    static ProfileDeletePhysicalReceipt decode(byte[] bytes) {
        if (bytes == null || bytes.length < PREFIX_BYTES + ProfilePhysicalDeleteCodec.MIN_REQUEST_BYTES + 1
                || bytes.length > MAX_BYTES) {
            throw invalid();
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        var in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC
                || in.getShort() != 1 || in.getShort() != 0) {
            throw invalid();
        }
        byte[] birth = new byte[16];
        in.get(birth);
        int eventLength = in.getInt();
        if (eventLength < 1 || eventLength > ProfileDeleteDelivery.MAX_BYTES
                || in.remaining() < eventLength + 4 + ProfilePhysicalDeleteCodec.MIN_REQUEST_BYTES
                    + ProfilePhysicalDeleteCodec.REPLY_BYTES + 4) {
            throw invalid();
        }
        byte[] eventBytes = new byte[eventLength];
        in.get(eventBytes);
        var event = ProfileDeleteDelivery.Event.decode(eventBytes);
        int requestLength = in.getInt();
        if (requestLength < ProfilePhysicalDeleteCodec.MIN_REQUEST_BYTES
                || requestLength > ProfilePhysicalDeleteCodec.MAX_REQUEST_BYTES
                || in.remaining() != requestLength + ProfilePhysicalDeleteCodec.REPLY_BYTES + 4) {
            throw invalid();
        }
        // Storage syntax only. Authentication happened in the collector, never in this decoder.
        var storedTarget = new ProfileHello.Server(EngineProfile.SEGMENT_WAL_ENGINE.wireValue(),
                event.target().incarnation(), 0, event.target().bookie(),
                List.of(ProfilePhysicalDeleteCodec.CAPABILITY));
        var request = ProfilePhysicalDeleteCodec.decodeRequest(in.slice().limit(requestLength), storedTarget);
        in.position(in.position() + requestLength);
        var reply = ProfilePhysicalDeleteCodec.decodeReply(in.slice().limit(ProfilePhysicalDeleteCodec.REPLY_BYTES),
                ProfilePhysicalDeleteCodec.requestIdentity(request));
        var result = new ProfileDeletePhysicalReceipt(birth, event, request, reply);
        if (!Arrays.equals(result.encode(), bytes)) {
            throw invalid();
        }
        return result;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid scoped Profile physical delete receipt");
    }
}

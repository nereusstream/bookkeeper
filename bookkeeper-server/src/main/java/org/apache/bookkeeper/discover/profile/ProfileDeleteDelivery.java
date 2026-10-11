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
package org.apache.bookkeeper.discover.profile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/** Experimental one shared stream per Bookie/incarnation. It is a delivery obligation, not a local cursor. */
public final class ProfileDeleteDelivery {
    static final int MAX_EVENTS = 4096;
    static final int MAX_BYTES = 258 + ProfileControlScope.MAX_TARGET_BOOKIE_BYTES;

    private ProfileDeleteDelivery() { }

    public record Head(byte[] streamId, long streamGeneration, long assignmentGeneration, long committedThrough,
            ProfileDeleteRecord.Target target) {
        public Head {
            streamId = ProfileLifecycleHead.fixed(streamId, 16, true);
            java.util.Objects.requireNonNull(target, "target");
            if (streamGeneration != 1 || assignmentGeneration != 1 || committedThrough < 0
                    || committedThrough > MAX_EVENTS) {
                throw new IllegalArgumentException("Unsupported initial delete delivery assignment or head");
            }
        }
        @Override
        public byte[] streamId() {
            return streamId.clone();
        }
        public byte[] encode() {
            byte[] name = target.bookie().getBytes(StandardCharsets.UTF_8);
            return finish(header(70 + name.length, 0x55574448).put(streamId).putLong(streamGeneration)
                    .putLong(assignmentGeneration).putLong(committedThrough).putShort((short) name.length)
                    .put(name).put(target.incarnation()));
        }
        public static Head decode(byte[] bytes) {
            var input = checked(bytes, 71, 0x55574448);
            byte[] stream = new byte[16];
            input.get(stream);
            long generation = input.getLong();
            long assignment = input.getLong();
            long sequence = input.getLong();
            var target = ProfileDeleteDelivery.target(input);
            end(input);
            return new Head(stream, generation, assignment, sequence, target);
        }
        Head next() {
            return new Head(streamId, streamGeneration, assignmentGeneration, committedThrough + 1, target);
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Head head && Arrays.equals(encode(), head.encode());
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }
    }

    public record Event(byte[] streamId, long streamGeneration, long assignmentGeneration, long sequence,
            ProfileLedgerContext ledger, ProfileDeleteRecord.Target target, ProfileDeleteRecord.Reference tombstone,
            byte[] requestId, long deleteEpoch) {
        public Event {
            streamId = ProfileLifecycleHead.fixed(streamId, 16, true);
            requestId = ProfileLifecycleHead.fixed(requestId, 16, true);
            java.util.Objects.requireNonNull(ledger, "ledger");
            java.util.Objects.requireNonNull(target, "target");
            java.util.Objects.requireNonNull(tombstone, "tombstone");
            if (streamGeneration != 1 || assignmentGeneration != 1 || sequence < 1
                    || sequence > MAX_EVENTS || deleteEpoch < 1) {
                throw new IllegalArgumentException("Invalid bounded initial delete delivery event");
            }
        }
        @Override
        public byte[] streamId() {
            return streamId.clone();
        }
        @Override
        public byte[] requestId() {
            return requestId.clone();
        }
        public byte[] encode() {
            byte[] name = target.bookie().getBytes(StandardCharsets.UTF_8);
            byte[] reference = tombstone.encode();
            return finish(header(154 + reference.length + name.length, 0x55574445,
                    tombstone.publicationId() == null ? 1 : 2).put(streamId).putLong(streamGeneration)
                    .putLong(assignmentGeneration).putLong(sequence).put(ledger.encode()).put(reference)
                    .put(requestId).putLong(deleteEpoch).putShort((short) name.length).put(name)
                    .put(target.incarnation()));
        }
        public static Event decode(byte[] bytes) {
            int schema = bytes != null && bytes.length >= 8 ? ByteBuffer.wrap(bytes).getShort(4) : 0;
            var input = checked(bytes, schema == 2 ? 259 : 243, 0x55574445, schema);
            byte[] stream = new byte[16];
            input.get(stream);
            long generation = input.getLong();
            long assignment = input.getLong();
            long sequence = input.getLong();
            var ledger = ProfileLedgerContext.decode(input);
            byte[] ref = new byte[schema == 2 ? 104 : 88];
            input.get(ref);
            byte[] request = new byte[16];
            input.get(request);
            long epoch = input.getLong();
            var target = ProfileDeleteDelivery.target(input);
            end(input);
            return new Event(stream, generation, assignment, sequence, ledger, target,
                    ProfileDeleteRecord.Reference.decode(ref), request, epoch);
        }
        /** Deterministic exact request; the server independently reads the committed event before authorization. */
        public ProfileControlRequest deleteApply(String authorityDomain) {
            byte[] prefix = "BookKeeper/DeleteDelivery/v1\0".getBytes(StandardCharsets.US_ASCII);
            byte[] encoded = encode();
            byte[] operation = Arrays.copyOf(ProfileRepairIntent.digest(
                    ByteBuffer.allocate(encoded.length + prefix.length).put(prefix).put(encoded).array()), 16);
            var binding = new ProfileControlRequest.DeleteBinding(streamId, streamGeneration, assignmentGeneration,
                    sequence, requestId, deleteEpoch, ProfileRepairIntent.digest(tombstone.encode()));
            var operationKind = ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY;
            var scope = new ProfileControlScope(operationKind, operationKind.purpose(), ledger.ledgerId(),
                    ledger.ledgerInstanceId(), ProfileDescriptorIdentity.fromBytes(ledger.descriptorIdentity()),
                    target.bookie(), target.incarnation(), authorityDomain, deleteEpoch, operation, new byte[32], null);
            return ProfileControlRequest.deleteApply(new ProfileControlScope(operationKind, operationKind.purpose(),
                    ledger.ledgerId(), ledger.ledgerInstanceId(), scope.descriptorIdentity(), target.bookie(),
                    target.incarnation(), authorityDomain, deleteEpoch, operation,
                    ProfileControlRequest.deletePayloadIdentity(scope, binding), null), binding);
        }

        boolean belongsTo(Head head) {
            return Arrays.equals(streamId, head.streamId) && streamGeneration == head.streamGeneration
                    && assignmentGeneration == head.assignmentGeneration && target.equals(head.target)
                    && sequence <= head.committedThrough;
        }
        @Override
        public boolean equals(Object other) {
            return other instanceof Event event && Arrays.equals(encode(), event.encode());
        }
        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }
    }

    private static ProfileDeleteRecord.Target target(ByteBuffer input) {
        int length = input.getShort() & 0xffff;
        if (length < 1 || length > org.apache.bookkeeper.common.profile.control.ProfileControlScope
                .MAX_TARGET_BOOKIE_BYTES
                || input.remaining() < length + 20) {
            throw new IllegalArgumentException("Invalid delete delivery target width");
        }
        byte[] name = new byte[length];
        input.get(name);
        String bookie = new String(name, StandardCharsets.UTF_8);
        if (!Arrays.equals(name, bookie.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid delete delivery target UTF-8");
        }
        byte[] incarnation = new byte[16];
        input.get(incarnation);
        return new ProfileDeleteRecord.Target(bookie, incarnation);
    }

    private static ByteBuffer header(int size, int magic) {
        return header(size, magic, 1);
    }

    private static ByteBuffer header(int size, int magic, int schema) {
        return ByteBuffer.allocate(size).putInt(magic).putShort((short) schema).putShort((short) 0);
    }

    private static byte[] finish(ByteBuffer output) {
        if (output.position() != output.capacity() - 4) {
            throw new IllegalArgumentException("Incomplete delete delivery record");
        }
        var crc = new CRC32C();
        crc.update(output.array(), 0, output.capacity() - 4);
        return output.putInt((int) crc.getValue()).array();
    }

    private static ByteBuffer checked(byte[] bytes, int min, int magic) {
        return checked(bytes, min, magic, 1);
    }

    private static ByteBuffer checked(byte[] bytes, int min, int magic, int schema) {
        if (schema != 1 && schema != 2 || bytes == null || bytes.length < min || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid delete delivery byte bound before allocation");
        }
        var input = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        if (input.getInt(bytes.length - 4) != (int) crc.getValue() || input.getInt() != magic
                || input.getShort() != schema || input.getShort() != 0) {
            throw new IllegalArgumentException("Invalid delete delivery schema/CRC");
        }
        return input;
    }

    private static void end(ByteBuffer input) {
        if (input.remaining() != 4) {
            throw new IllegalArgumentException("Trailing delete delivery bytes");
        }
    }
}

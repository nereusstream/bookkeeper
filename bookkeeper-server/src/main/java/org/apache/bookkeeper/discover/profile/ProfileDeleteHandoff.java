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
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/** Immutable PREPARED handoff cut. It neither advances a local cursor nor makes the next assignment effective. */
public final class ProfileDeleteHandoff {
    static final int MAX_BYTES = 96 + ProfileDeleteDelivery.MAX_BYTES;
    private ProfileDeleteHandoff() { }

    public record Plan(byte[] operationId, ProfileDeleteDelivery.Head predecessor, long headBirth,
            long nextGeneration, long snapshotGeneration, long snapshotBirth, byte[] snapshotDigest) {
        public Plan {
            operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
            Objects.requireNonNull(predecessor, "predecessor");
            snapshotDigest = ProfileLifecycleHead.fixed(snapshotDigest, 32, false);
            if (headBirth <= 0 || nextGeneration != predecessor.assignmentGeneration() + 1
                    || nextGeneration > ProfileDeleteSnapshot.MAX_GENERATIONS
                    || predecessor.committedThrough() == 0
                        && (snapshotGeneration != 0 || snapshotBirth != 0 || !zero(snapshotDigest))
                    || predecessor.committedThrough() != 0
                        && (snapshotGeneration < 1 || snapshotGeneration > ProfileDeleteSnapshot.MAX_GENERATIONS
                            || snapshotBirth <= 0)) {
                throw invalid();
            }
        }
        @Override
        public byte[] operationId() {
            return operationId.clone();
        }
        @Override
        public byte[] snapshotDigest() {
            return snapshotDigest.clone();
        }
        public byte[] encode() {
            byte[] head = predecessor.encode();
            return finish(header(96 + head.length, 0x55574850).put(operationId).putLong(nextGeneration)
                    .putLong(headBirth).putLong(snapshotGeneration).putLong(snapshotBirth).put(snapshotDigest)
                    .putInt(head.length).put(head));
        }
        public static Plan decode(byte[] bytes) {
            var in = checked(bytes, 96 + 71, MAX_BYTES, 0x55574850);
            byte[] operation = new byte[16];
            in.get(operation);
            long next = in.getLong();
            long headBirth = in.getLong();
            long snapshotGeneration = in.getLong();
            long snapshotBirth = in.getLong();
            byte[] digest = new byte[32];
            in.get(digest);
            int length = in.getInt();
            if (length < 71 || length > ProfileDeleteDelivery.MAX_BYTES || in.remaining() != length + 4) {
                throw invalid();
            }
            byte[] head = new byte[length];
            in.get(head);
            var result = new Plan(operation, ProfileDeleteDelivery.Head.decode(head), headBirth,
                    next, snapshotGeneration, snapshotBirth, digest);
            if (!Arrays.equals(result.encode(), bytes)) {
                throw invalid();
            }
            return result;
        }
    }

    /** Immutable native acknowledgement of the original owner's local force/apply result. */
    public static final class Acknowledgement {
        private final Plan plan;
        private final long controlSequence;
        private final byte[] birthId;

        private Acknowledgement(Plan plan, long controlSequence, byte[] birthId) {
            this.plan = Objects.requireNonNull(plan, "plan");
            this.birthId = ProfileLifecycleHead.fixed(birthId, 16, true);
            if (controlSequence < 0) {
                throw invalid();
            }
            this.controlSequence = controlSequence;
        }

        static Acknowledgement recorded(Plan plan, long controlSequence) {
            UUID id = UUID.randomUUID();
            return new Acknowledgement(plan, controlSequence, ByteBuffer.allocate(16)
                    .putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());
        }

        public Plan plan() {
            return plan;
        }

        public long controlSequence() {
            return controlSequence;
        }

        public byte[] birthId() {
            return birthId.clone();
        }

        boolean matches(Plan expected) {
            return Arrays.equals(plan.encode(), expected.encode());
        }

        public byte[] encode() {
            byte[] body = plan.encode();
            return finish(header(40 + body.length, 0x5557484b).putLong(controlSequence)
                    .put(birthId).putInt(body.length).put(body));
        }

        public static Acknowledgement decode(byte[] bytes) {
            var in = checked(bytes, 40 + 167, 40 + MAX_BYTES, 0x5557484b);
            long sequence = in.getLong();
            byte[] birth = new byte[16];
            in.get(birth);
            int size = in.getInt();
            if (size < 167 || size > MAX_BYTES || in.remaining() != size + 4) {
                throw invalid();
            }
            byte[] body = new byte[size];
            in.get(body);
            var result = new Acknowledgement(Plan.decode(body), sequence, birth);
            if (!Arrays.equals(result.encode(), bytes)) {
                throw invalid();
            }
            return result;
        }
    }

    /** Native admission CAS body; the scope digest binds the stable stream/target, not a caller metadata path. */
    record Admission(long activeGeneration, byte[] scopeDigest, byte[] preparedDigest) {
        Admission {
            scopeDigest = ProfileLifecycleHead.fixed(scopeDigest, 32, false);
            preparedDigest = ProfileLifecycleHead.fixed(preparedDigest, 32, false);
            if (activeGeneration < 1 || activeGeneration > ProfileDeleteSnapshot.MAX_GENERATIONS) {
                throw invalid();
            }
        }
        boolean frozen() {
            return !zero(preparedDigest);
        }
        byte[] encode() {
            return finish(header(84, 0x55574841).putLong(activeGeneration).put(scopeDigest).put(preparedDigest));
        }
        static Admission decode(byte[] bytes) {
            var in = checked(bytes, 84, 84, 0x55574841);
            long generation = in.getLong();
            byte[] scope = new byte[32];
            byte[] prepared = new byte[32];
            in.get(scope).get(prepared);
            return new Admission(generation, scope, prepared);
        }
    }

    static byte[] scopeDigest(ProfileDeleteDelivery.Head head) {
        return ProfileRepairIntent.digest(new ProfileDeleteDelivery.Head(head.streamId(), head.streamGeneration(),
                head.assignmentGeneration(), 0, head.target()).encode());
    }

    private static boolean zero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    private static ByteBuffer header(int size, int magic) {
        return ByteBuffer.allocate(size).putInt(magic).putShort((short) 1).putShort((short) 0);
    }

    private static byte[] finish(ByteBuffer out) {
        if (out.position() != out.capacity() - 4) {
            throw invalid();
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }

    private static ByteBuffer checked(byte[] bytes, int minimum, int maximum, int magic) {
        if (bytes == null || bytes.length < minimum || bytes.length > maximum) {
            throw invalid();
        }
        var in = ByteBuffer.wrap(bytes);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != magic
                || in.getShort() != 1 || in.getShort() != 0) {
            throw invalid();
        }
        return in;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid bound Profile delete handoff preparation");
    }
}

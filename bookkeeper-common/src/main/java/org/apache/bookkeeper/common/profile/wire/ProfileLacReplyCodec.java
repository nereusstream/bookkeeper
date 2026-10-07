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
import java.nio.ByteOrder;
import java.util.Objects;

/** Experimental READ_LAC source evidence. Source length is not the LAC-prefix length. */
public final class ProfileLacReplyCodec {
    public static final int CAPABILITY_ID = 0x55524c31; // URL1
    public static final int SCHEMA = 1;
    public static final int MAGIC = 0x55574c51; // UWLQ
    public static final int PREFIX_BYTES = 88;
    public static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(CAPABILITY_ID, SCHEMA);
    private static final ProfileStatus QUERY_OK = new ProfileStatus(ProfileStatus.StatusClass.OK,
            ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);

    public enum Outcome { SOURCE, EMPTY, REFUSED }

    public static final class Reply {
        private final ProfileStatus status;
        private final Outcome outcome;
        private final byte[] entry;

        private Reply(ProfileStatus status, Outcome outcome, byte[] entry) {
            this.status = status;
            this.outcome = outcome;
            this.entry = entry;
        }

        public ProfileStatus status() {
            return status;
        }
        public Outcome outcome() {
            return outcome;
        }
        public ByteBuffer entry() {
            return ByteBuffer.wrap(entry).asReadOnlyBuffer();
        }
        public long sourceEntryId() {
            return outcome == Outcome.SOURCE ? entry().getLong(8) : -1;
        }
        public long piggybackLac() {
            return outcome == Outcome.SOURCE ? entry().getLong(16) : -1;
        }
        public long sourceLength() {
            return outcome == Outcome.SOURCE ? entry().getLong(24) : 0;
        }

        @Override
        public String toString() {
            return "LacReply{outcome=" + outcome + ", source=" + sourceEntryId() + ", lac=" + piggybackLac()
                    + ", sourceLength=" + sourceLength() + '}';
        }
    }

    private ProfileLacReplyCodec() { }

    public static Reply refused(ProfileStatus status) {
        ProfileReadEntryReplyCodec.refused(status);
        return new Reply(status, Outcome.REFUSED, new byte[0]);
    }

    public static void writePrefix(ByteBuffer output, ProfileLedgerContext context, long sourceEntryId,
            Outcome outcome, int size, int bound) {
        Objects.requireNonNull(context, "context");
        requireShape(sourceEntryId, outcome, size, bound);
        if (output.remaining() != PREFIX_BYTES) {
            throw new IllegalArgumentException("READ_LAC prefix requires exactly 88 output bytes");
        }
        output.order(ByteOrder.BIG_ENDIAN).put(QUERY_OK.encode()).putInt(MAGIC).putShort((short) SCHEMA)
                .putShort((short) (outcome == Outcome.SOURCE ? 1 : 2)).put(context.encode())
                .putLong(sourceEntryId).putInt(size);
    }

    public static Reply decode(ByteBuffer input, ProfileLedgerContext expected, int bound) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(expected, "expected");
        if (bound < 36 || bound > ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES) {
            throw new IllegalArgumentException("Invalid installed READ_LAC bound");
        }
        ByteBuffer body = input.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        if (body.remaining() < ProfileStatus.ENCODED_LENGTH) {
            throw new IllegalArgumentException("Truncated READ_LAC status");
        }
        byte[] statusBytes = new byte[8];
        body.get(statusBytes);
        ProfileStatus status = ProfileStatus.decode(statusBytes);
        if (status.statusClass() != ProfileStatus.StatusClass.OK) {
            if (body.hasRemaining()) {
                throw new IllegalArgumentException("READ_LAC refusal carries a payload");
            }
            return refused(status);
        }
        if (!status.equals(QUERY_OK) || body.remaining() < PREFIX_BYTES - 8
                || body.getInt() != MAGIC || Short.toUnsignedInt(body.getShort()) != SCHEMA) {
            throw new IllegalArgumentException("Invalid READ_LAC query status or schema");
        }
        int tag = Short.toUnsignedInt(body.getShort());
        Outcome outcome = tag == 1 ? Outcome.SOURCE : tag == 2 ? Outcome.EMPTY : Outcome.REFUSED;
        ProfileLedgerContext context = ProfileLedgerContext.decode(body);
        long sourceEntryId = body.getLong();
        long size = Integer.toUnsignedLong(body.getInt());
        if (!context.equals(expected) || size > Integer.MAX_VALUE || size != body.remaining()) {
            throw new IllegalArgumentException("READ_LAC scope or length mismatch");
        }
        requireShape(sourceEntryId, outcome, (int) size, bound);
        if (outcome == Outcome.SOURCE && (body.getLong(body.position()) != expected.ledgerId()
                || body.getLong(body.position() + 8) != sourceEntryId
                || body.getLong(body.position() + 16) < -1 || body.getLong(body.position() + 24) < 0)) {
            throw new IllegalArgumentException("Invalid READ_LAC source identity or metadata");
        }
        byte[] entry = new byte[(int) size];
        body.get(entry);
        return new Reply(status, outcome, entry);
    }

    private static void requireShape(long source, Outcome outcome, int size, int bound) {
        if (bound < 36 || bound > ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES
                || outcome == null || outcome == Outcome.REFUSED
                || (outcome == Outcome.SOURCE ? source < 0 || size < 36 || size > bound : source != -1 || size != 0)) {
            throw new IllegalArgumentException("Invalid bounded READ_LAC result");
        }
    }
}

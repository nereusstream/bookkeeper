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

/** Explicit experimental READ_ENTRY reply, negotiated independently from initial control and ordinary Add. */
public final class ProfileReadEntryReplyCodec {
    public static final int CAPABILITY_ID = 0x55524431; // URD1
    public static final int SCHEMA = 1;
    public static final int MAGIC = 0x55575244; // UWRD
    public static final int PREFIX_BYTES = 88;
    public static final int MIN_ENTRY_BYTES = 36;
    public static final int MAX_ENTRY_BYTES = 1024 * 1024 + MIN_ENTRY_BYTES;
    public static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(CAPABILITY_ID, SCHEMA);
    private static final ProfileStatus QUERY_OK = new ProfileStatus(ProfileStatus.StatusClass.OK,
            ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);

    public enum Outcome { FOUND, MISSING, REFUSED }

    /** Owns its decoded heap bytes; every exposed view is read-only and independent of the network frame. */
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

        @Override
        public String toString() {
            return "ReadReply{outcome=" + outcome + ", status=" + status + ", entryBytes=" + entry.length + '}';
        }
    }

    private ProfileReadEntryReplyCodec() { }

    public static Reply refused(ProfileStatus status) {
        requireRefusal(status);
        return new Reply(status, Outcome.REFUSED, new byte[0]);
    }

    /** Write only the fixed prefix; the adapter copies a borrowed, validated entry directly into its output. */
    public static void writePrefix(ByteBuffer output, ProfileLedgerContext context, long entryId,
            Outcome outcome, int entryBytes, int installedEntryBound) {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(context, "context");
        requireShape(entryId, outcome, entryBytes, installedEntryBound);
        if (output.remaining() != PREFIX_BYTES) {
            throw new IllegalArgumentException("READ_ENTRY prefix requires exactly 88 output bytes");
        }
        output.order(ByteOrder.BIG_ENDIAN).put(QUERY_OK.encode()).putInt(MAGIC).putShort((short) SCHEMA)
                .putShort((short) (outcome == Outcome.FOUND ? 1 : 2)).put(context.encode())
                .putLong(entryId).putInt(entryBytes);
    }

    /** Validate shape, scope and coordinate before allocating the bounded entry copy. */
    public static Reply decode(ByteBuffer input, ProfileLedgerContext expected, long entryId,
            int installedEntryBound) {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(expected, "expected");
        if (entryId < 0 || installedEntryBound < MIN_ENTRY_BYTES || installedEntryBound > MAX_ENTRY_BYTES) {
            throw new IllegalArgumentException("Invalid READ_ENTRY coordinate or installed bound");
        }
        ByteBuffer body = input.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        if (body.remaining() < ProfileStatus.ENCODED_LENGTH) {
            throw new IllegalArgumentException("Truncated READ_ENTRY status");
        }
        byte[] statusBytes = new byte[ProfileStatus.ENCODED_LENGTH];
        body.get(statusBytes);
        ProfileStatus status = ProfileStatus.decode(statusBytes);
        if (status.statusClass() != ProfileStatus.StatusClass.OK) {
            if (body.hasRemaining()) {
                throw new IllegalArgumentException("READ_ENTRY refusal cannot carry an entry");
            }
            return refused(status);
        }
        if (!status.equals(QUERY_OK) || body.remaining() < PREFIX_BYTES - ProfileStatus.ENCODED_LENGTH
                || body.getInt() != MAGIC || Short.toUnsignedInt(body.getShort()) != SCHEMA) {
            throw new IllegalArgumentException("Invalid READ_ENTRY reply schema or query status");
        }
        int tag = Short.toUnsignedInt(body.getShort());
        Outcome outcome = tag == 1 ? Outcome.FOUND : tag == 2 ? Outcome.MISSING : Outcome.REFUSED;
        ProfileLedgerContext context = ProfileLedgerContext.decode(body);
        long coordinate = body.getLong();
        long size = Integer.toUnsignedLong(body.getInt());
        if (!context.equals(expected) || coordinate != entryId || size > Integer.MAX_VALUE
                || size != body.remaining()) {
            throw new IllegalArgumentException("READ_ENTRY reply scope, coordinate or length mismatch");
        }
        requireShape(coordinate, outcome, (int) size, installedEntryBound);
        if (outcome == Outcome.FOUND && (body.getLong(body.position()) != context.ledgerId()
                || body.getLong(body.position() + Long.BYTES) != coordinate)) {
            throw new IllegalArgumentException("READ_ENTRY payload is bound to another ledger or entry");
        }
        byte[] entry = new byte[(int) size];
        body.get(entry);
        return new Reply(status, outcome, entry);
    }

    private static void requireShape(long entryId, Outcome outcome, int size, int bound) {
        if (entryId < 0 || bound < MIN_ENTRY_BYTES || bound > MAX_ENTRY_BYTES
                || outcome == null || outcome == Outcome.REFUSED
                || (outcome == Outcome.FOUND ? size < MIN_ENTRY_BYTES || size > bound : size != 0)) {
            throw new IllegalArgumentException("Invalid bounded READ_ENTRY result");
        }
    }

    private static void requireRefusal(ProfileStatus status) {
        if (status == null || status.statusClass() == ProfileStatus.StatusClass.OK
                || status.durableResult() != ProfileStatus.DurableResult.NONE) {
            throw new IllegalArgumentException("Read refusal must be a non-OK query with no durable mutation");
        }
    }
}

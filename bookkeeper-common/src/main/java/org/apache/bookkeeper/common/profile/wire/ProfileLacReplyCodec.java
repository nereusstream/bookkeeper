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
    public static final int EXPLICIT_CAPABILITY_ID = 0x55524c32; // URL2
    public static final int EXPLICIT_SCHEMA = 2;
    public static final int EXPLICIT_PREFIX_BYTES = PREFIX_BYTES + 12;
    public static final int EXPLICIT_BYTES = 20; // original CRC32C ledger/LAC digest, no prefix length
    public static final ProfileHello.Capability EXPLICIT_CAPABILITY = new ProfileHello.Capability(
            EXPLICIT_CAPABILITY_ID, EXPLICIT_SCHEMA);
    public static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(CAPABILITY_ID, SCHEMA);
    private static final ProfileStatus QUERY_OK = new ProfileStatus(ProfileStatus.StatusClass.OK,
            ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);

    public enum Outcome { SOURCE, EMPTY, EXPLICIT, REFUSED }

    public static final class Reply {
        private final ProfileStatus status;
        private final Outcome outcome;
        private final byte[] entry;
        private final byte[] explicit;
        private final long explicitLac;

        private Reply(ProfileStatus status, Outcome outcome, byte[] entry) {
            this(status, outcome, entry, new byte[0], -1);
        }

        private Reply(ProfileStatus status, Outcome outcome, byte[] entry, byte[] explicit, long explicitLac) {
            this.status = status;
            this.outcome = outcome;
            this.entry = entry;
            this.explicit = explicit;
            this.explicitLac = explicitLac;
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
        public boolean hasExplicitLac() {
            return explicit.length != 0;
        }

        public long explicitLac() {
            return explicitLac;
        }

        public ByteBuffer explicitBody() {
            return ByteBuffer.wrap(explicit).asReadOnlyBuffer();
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

    /** Strictly validated borrowed payloads; their input owner must remain live while a view is used. */
    public record View(ProfileStatus status, Outcome outcome, ByteBuffer entry, ByteBuffer explicitBody,
                       long explicitLac) {
        public View {
            entry = entry.asReadOnlyBuffer();
            explicitBody = explicitBody.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer entry() {
            return entry.asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer explicitBody() {
            return explicitBody.asReadOnlyBuffer();
        }

        public long sourceEntryId() {
            return outcome == Outcome.SOURCE ? entry.getLong(8) : -1;
        }

        public long piggybackLac() {
            return outcome == Outcome.SOURCE ? entry.getLong(16) : -1;
        }

        public long sourceLength() {
            return outcome == Outcome.SOURCE ? entry.getLong(24) : 0;
        }
    }

    private static Reply copy(View view) {
        byte[] entry = new byte[view.entry.remaining()];
        byte[] explicit = new byte[view.explicitBody.remaining()];
        view.entry().get(entry);
        view.explicitBody().get(explicit);
        return new Reply(view.status(), view.outcome(), entry, explicit, view.explicitLac());
    }

    private static View refusedView(ProfileStatus status) {
        ProfileReadEntryReplyCodec.refused(status);
        return new View(status, Outcome.REFUSED, ByteBuffer.allocate(0), ByteBuffer.allocate(0), -1);
    }

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
        return copy(decodeView(input, expected, bound));
    }

    public static View decodeView(ByteBuffer input, ProfileLedgerContext expected, int bound) {
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
            return refusedView(status);
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
        return new View(status, outcome, body.slice(), ByteBuffer.allocate(0), -1);
    }

    public static void writeExplicitPrefix(ByteBuffer output, ProfileLedgerContext context, long sourceEntryId,
            int entryBytes, long explicitLac, int explicitBytes, int bound) {
        requireExplicitShape(sourceEntryId, entryBytes, explicitLac, explicitBytes, bound);
        if (output.remaining() != EXPLICIT_PREFIX_BYTES) {
            throw new IllegalArgumentException("Explicit READ_LAC prefix needs exactly 100 bytes");
        }
        int tag = entryBytes != 0 ? explicitBytes != 0 ? 4 : 1 : explicitBytes != 0 ? 3 : 2;
        output.order(ByteOrder.BIG_ENDIAN).put(QUERY_OK.encode()).putInt(MAGIC).putShort((short) EXPLICIT_SCHEMA)
                .putShort((short) tag).put(context.encode()).putLong(sourceEntryId).putInt(entryBytes)
                .putLong(explicitLac).putInt(explicitBytes);
    }

    public static Reply decodeExplicit(ByteBuffer input, ProfileLedgerContext expected, int bound) {
        return copy(decodeExplicitView(input, expected, bound));
    }

    public static View decodeExplicitView(ByteBuffer input, ProfileLedgerContext expected, int bound) {
        ByteBuffer in = input.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        if (in.remaining() < 8) {
            throw new IllegalArgumentException("Truncated explicit READ_LAC reply");
        }
        byte[] statusBytes = new byte[8];
        in.get(statusBytes);
        var status = ProfileStatus.decode(statusBytes);
        if (status.statusClass() != ProfileStatus.StatusClass.OK) {
            if (in.hasRemaining()) {
                throw new IllegalArgumentException("Explicit READ_LAC refusal carries body");
            }
            return refusedView(status);
        }
        if (!status.equals(QUERY_OK) || in.remaining() < EXPLICIT_PREFIX_BYTES - 8 || in.getInt() != MAGIC
                || Short.toUnsignedInt(in.getShort()) != EXPLICIT_SCHEMA) {
            throw new IllegalArgumentException("Invalid explicit READ_LAC schema/status");
        }
        int tag = Short.toUnsignedInt(in.getShort());
        var context = ProfileLedgerContext.decode(in);
        long source = in.getLong();
        long entryBytes = Integer.toUnsignedLong(in.getInt());
        long lac = in.getLong();
        long explicitBytes = Integer.toUnsignedLong(in.getInt());
        if (!context.equals(expected) || entryBytes > Integer.MAX_VALUE || explicitBytes > Integer.MAX_VALUE
                || entryBytes + explicitBytes != in.remaining()) {
            throw new IllegalArgumentException("Explicit READ_LAC scope/length mismatch");
        }
        requireExplicitShape(source, (int) entryBytes, lac, (int) explicitBytes, bound);
        int expectedTag = entryBytes != 0 ? explicitBytes != 0 ? 4 : 1 : explicitBytes != 0 ? 3 : 2;
        if (tag != expectedTag || entryBytes != 0 && (in.getLong(in.position()) != expected.ledgerId()
                || in.getLong(in.position() + 8) != source || in.getLong(in.position() + 16) < -1
                || in.getLong(in.position() + 24) < 0)) {
            throw new IllegalArgumentException("Explicit READ_LAC tags/source disagree");
        }
        ByteBuffer entry = in.slice(in.position(), (int) entryBytes);
        ByteBuffer explicit = in.slice(in.position() + (int) entryBytes, (int) explicitBytes);
        if (explicit.hasRemaining() && (explicit.getLong() != expected.ledgerId()
                || explicit.getLong(8) != lac)) {
            throw new IllegalArgumentException("Explicit LAC body does not bind ledger/value");
        }
        explicit.position(0);
        return new View(status, entry.hasRemaining() ? Outcome.SOURCE : explicit.hasRemaining() ? Outcome.EXPLICIT
                : Outcome.EMPTY, entry, explicit, lac);
    }

    private static void requireExplicitShape(long source, int size, long lac, int explicit, int bound) {
        if (bound < 36 || bound > ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES
                || (size == 0 ? source != -1 : source < 0 || size < 36 || size > bound)
                || (explicit == 0 ? lac != -1 : explicit != EXPLICIT_BYTES || lac < -1)) {
            throw new IllegalArgumentException("Invalid bounded explicit LAC shape");
        }
    }

    private static void requireShape(long source, Outcome outcome, int size, int bound) {
        if (bound < 36 || bound > ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES
                || outcome == null || outcome != Outcome.SOURCE && outcome != Outcome.EMPTY
                || (outcome == Outcome.SOURCE ? source < 0 || size < 36 || size > bound : source != -1 || size != 0)) {
            throw new IllegalArgumentException("Invalid bounded READ_LAC result");
        }
    }
}

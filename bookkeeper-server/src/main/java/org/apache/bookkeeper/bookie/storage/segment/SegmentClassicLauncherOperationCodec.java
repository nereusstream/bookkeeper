/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Fixed operation-channel data only; no peer authentication, acceptance, drain or handoff authority. */
final class SegmentClassicLauncherOperationCodec {
    static final int HEADER_BYTES = 16;
    static final int MAX_BYTES = HEADER_BYTES + 32;
    private static final long MAGIC = 0x5557434c4f303031L; // UWCLO001

    enum Kind {
        GREETING(1, 16), STOP(2, 16), HANDOFF(3, 32), REPLY(4, 20);

        private final int code;
        private final int bodyBytes;

        Kind(int code, int bodyBytes) {
            this.code = code;
            this.bodyBytes = bodyBytes;
        }

        private static Kind decode(int code) {
            return switch (code) {
                case 1 -> GREETING;
                case 2 -> STOP;
                case 3 -> HANDOFF;
                case 4 -> REPLY;
                default -> throw new IllegalArgumentException("Unknown launcher operation kind");
            };
        }
    }

    enum Disposition { ACCEPTED, REJECTED }

    record Frame(Kind kind, UUID episode, UUID bindingId, Disposition disposition) {
        Frame {
            Objects.requireNonNull(kind, "kind");
            requireIdentity(episode);
            if (kind == Kind.HANDOFF) {
                requireIdentity(bindingId);
            } else if (bindingId != null) {
                throw new IllegalArgumentException("Only HANDOFF carries a binding ID");
            }
            if (kind == Kind.REPLY) {
                Objects.requireNonNull(disposition, "disposition");
            } else if (disposition != null) {
                throw new IllegalArgumentException("Only REPLY carries an acceptance result");
            }
        }

        void requireEpisode(UUID original) throws IOException {
            if (!episode.equals(original)) {
                throw new IOException("Operation refers to a different launcher episode");
            }
        }
    }

    private SegmentClassicLauncherOperationCodec() { }

    static byte[] encode(Frame frame) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + frame.kind().bodyBytes);
        buffer.putLong(MAGIC).putShort((short) 1).put((byte) frame.kind().code).put((byte) 0)
                .putInt(frame.kind().bodyBytes);
        identity(buffer, frame.episode());
        if (frame.kind() == Kind.HANDOFF) {
            identity(buffer, frame.bindingId());
        } else if (frame.kind() == Kind.REPLY) {
            buffer.putInt(frame.disposition() == Disposition.ACCEPTED ? 1 : 2);
        }
        return buffer.array();
    }

    /** Validates all fixed header fields before a transport reserves or allocates any body. */
    static int bodyBytes(byte[] header) throws IOException {
        if (header == null || header.length != HEADER_BYTES) {
            throw new IOException("Launcher operation requires its exact fixed header");
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(header.clone());
            if (buffer.getLong() != MAGIC || buffer.getShort() != 1) {
                throw new IllegalArgumentException("Unknown launcher operation framing");
            }
            Kind kind = Kind.decode(Byte.toUnsignedInt(buffer.get()));
            if (buffer.get() != 0 || buffer.getInt() != kind.bodyBytes) {
                throw new IllegalArgumentException("Noncanonical launcher operation flags or body length");
            }
            return kind.bodyBytes;
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Invalid launcher operation header", malformed);
        }
    }

    static Frame decode(byte[] encoded) throws IOException {
        if (encoded == null || encoded.length < HEADER_BYTES || encoded.length > MAX_BYTES) {
            throw new IOException("Launcher operation exceeds its fixed bounds");
        }
        byte[] original = encoded.clone();
        int body = bodyBytes(Arrays.copyOf(original, HEADER_BYTES));
        if (original.length != HEADER_BYTES + body) {
            throw new IOException("Truncated or trailing launcher operation data");
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(original);
            Kind kind = Kind.decode(Byte.toUnsignedInt(buffer.get(10)));
            buffer.position(HEADER_BYTES);
            UUID episode = identity(buffer);
            UUID binding = kind == Kind.HANDOFF ? identity(buffer) : null;
            Disposition disposition = kind == Kind.REPLY ? switch (buffer.getInt()) {
                case 1 -> Disposition.ACCEPTED;
                case 2 -> Disposition.REJECTED;
                default -> throw new IllegalArgumentException("Unknown launcher operation disposition");
            } : null;
            return new Frame(kind, episode, binding, disposition);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("Invalid launcher operation body", malformed);
        }
    }

    private static void requireIdentity(UUID value) {
        if (value == null || value.getMostSignificantBits() == 0 && value.getLeastSignificantBits() == 0) {
            throw new IllegalArgumentException("Original nonzero operation identity required");
        }
    }

    private static UUID identity(ByteBuffer buffer) {
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static void identity(ByteBuffer buffer, UUID value) {
        buffer.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits());
    }
}

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
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Byte-exact HELLO request and response bodies for the experimental Profile manifest. */
public final class ProfileHello {

    public static final int PROTOCOL_MAJOR = 1;
    public static final int PROTOCOL_MINOR = 0;
    public static final int MAX_CAPABILITIES = 64;
    public static final int MAX_BOOKIE_ID_BYTES = 255;
    public static final int STORAGE_INCARNATION_LENGTH = 16;
    private static final int CAPABILITY_LENGTH = 8;
    private static final int CLIENT_FIXED_LENGTH = 12;
    private static final int SERVER_FIXED_LENGTH = 36;

    private ProfileHello() {}

    public record Capability(long capabilityId, int semanticVersion) {
        public Capability {
            if (capabilityId == 0
                    || (capabilityId & 0xffff_ffff_0000_0000L) != 0
                    || semanticVersion < 1
                    || semanticVersion > 0xffff) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_CAPABILITY);
            }
        }
    }

    public record Client(List<Capability> capabilities) {
        public Client {
            capabilities = validatedCapabilities(capabilities);
        }

        public byte[] encode() {
            ByteBuffer buffer = ByteBuffer.allocate(CLIENT_FIXED_LENGTH + CAPABILITY_LENGTH * capabilities.size());
            buffer.putShort((short) PROTOCOL_MAJOR);
            buffer.putShort((short) PROTOCOL_MAJOR);
            buffer.putShort((short) PROTOCOL_MINOR);
            buffer.putShort((short) PROTOCOL_MINOR);
            buffer.putShort((short) capabilities.size());
            buffer.putShort((short) 0);
            putCapabilities(buffer, capabilities);
            return buffer.array();
        }
    }

    public static final class Server {
        private final int activeEngine;
        private final byte[] storageIncarnation;
        private final long readinessGeneration;
        private final String bookieId;
        private final List<Capability> capabilities;

        public Server(
                int activeEngine,
                byte[] storageIncarnation,
                long readinessGeneration,
                String bookieId,
                List<Capability> capabilities) {
            if (activeEngine < 1 || activeEngine > 0xffff) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_LENGTH);
            }
            if (storageIncarnation == null || storageIncarnation.length != STORAGE_INCARNATION_LENGTH) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.ZERO_STORAGE_INCARNATION);
            }
            this.storageIncarnation = storageIncarnation.clone();
            if (ProfileWireCodecSupport.allZero(this.storageIncarnation)) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.ZERO_STORAGE_INCARNATION);
            }
            this.activeEngine = activeEngine;
            this.readinessGeneration = readinessGeneration;
            this.bookieId = validateBookieId(bookieId);
            this.capabilities = validatedCapabilities(capabilities);
        }

        public int activeEngine() {
            return activeEngine;
        }

        public byte[] storageIncarnation() {
            return storageIncarnation.clone();
        }

        public long readinessGeneration() {
            return readinessGeneration;
        }

        public String bookieId() {
            return bookieId;
        }

        public List<Capability> capabilities() {
            return capabilities;
        }

        public byte[] encode() {
            byte[] bookieIdBytes = bookieId.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(
                    SERVER_FIXED_LENGTH + bookieIdBytes.length + CAPABILITY_LENGTH * capabilities.size());
            buffer.putShort((short) PROTOCOL_MAJOR);
            buffer.putShort((short) PROTOCOL_MINOR);
            buffer.putShort((short) activeEngine);
            buffer.putShort((short) 0);
            buffer.putShort((short) bookieIdBytes.length);
            buffer.putShort((short) capabilities.size());
            buffer.put(storageIncarnation);
            buffer.putLong(readinessGeneration);
            buffer.put(bookieIdBytes);
            putCapabilities(buffer, capabilities);
            return buffer.array();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Server that)) {
                return false;
            }
            return activeEngine == that.activeEngine
                    && readinessGeneration == that.readinessGeneration
                    && Arrays.equals(storageIncarnation, that.storageIncarnation)
                    && bookieId.equals(that.bookieId)
                    && capabilities.equals(that.capabilities);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(activeEngine);
            result = 31 * result + Arrays.hashCode(storageIncarnation);
            result = 31 * result + Long.hashCode(readinessGeneration);
            result = 31 * result + bookieId.hashCode();
            result = 31 * result + capabilities.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Server{activeEngine=" + activeEngine + ", storageIncarnation=<opaque>, readinessGeneration="
                    + Long.toUnsignedString(readinessGeneration) + ", bookieId=" + bookieId
                    + ", capabilities=" + capabilities + '}';
        }
    }

    public static Client decodeClient(byte[] body) {
        if (body == null || body.length < CLIENT_FIXED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_LENGTH);
        }
        ByteBuffer buffer = ByteBuffer.wrap(body);
        if (readU16(buffer) != PROTOCOL_MAJOR
                || readU16(buffer) != PROTOCOL_MAJOR
                || readU16(buffer) != PROTOCOL_MINOR
                || readU16(buffer) != PROTOCOL_MINOR) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_VERSION_RANGE);
        }
        int count = readU16(buffer);
        if (buffer.getShort() != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NONZERO_RESERVED);
        }
        requireExactLength(body.length, CLIENT_FIXED_LENGTH, count);
        return new Client(readCapabilities(buffer, count));
    }

    public static Server decodeServer(byte[] body) {
        if (body == null || body.length < SERVER_FIXED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_LENGTH);
        }
        ByteBuffer buffer = ByteBuffer.wrap(body);
        if (readU16(buffer) != PROTOCOL_MAJOR || readU16(buffer) != PROTOCOL_MINOR) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_VERSION_RANGE);
        }
        int activeEngine = readU16(buffer);
        if (buffer.getShort() != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NONZERO_RESERVED);
        }
        int bookieIdLength = readU16(buffer);
        int capabilityCount = readU16(buffer);
        if (bookieIdLength < 1 || bookieIdLength > MAX_BOOKIE_ID_BYTES) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_BOOKIE_ID_LENGTH);
        }
        requireCapabilityCount(capabilityCount);
        long expectedLength = (long) SERVER_FIXED_LENGTH
                + bookieIdLength
                + (long) CAPABILITY_LENGTH * capabilityCount;
        if (expectedLength != body.length) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_LENGTH);
        }
        byte[] storageIncarnation = new byte[STORAGE_INCARNATION_LENGTH];
        buffer.get(storageIncarnation);
        if (ProfileWireCodecSupport.allZero(storageIncarnation)) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.ZERO_STORAGE_INCARNATION);
        }
        long readinessGeneration = buffer.getLong();
        byte[] bookieIdBytes = new byte[bookieIdLength];
        buffer.get(bookieIdBytes);
        String bookieId = decodeBookieId(bookieIdBytes);
        return new Server(
                activeEngine,
                storageIncarnation,
                readinessGeneration,
                bookieId,
                readCapabilities(buffer, capabilityCount));
    }

    private static void putCapabilities(ByteBuffer buffer, List<Capability> capabilities) {
        for (Capability capability : capabilities) {
            buffer.putInt((int) capability.capabilityId());
            buffer.putShort((short) capability.semanticVersion());
            buffer.putShort((short) 0);
        }
    }

    private static List<Capability> readCapabilities(ByteBuffer buffer, int count) {
        requireCapabilityCount(count);
        Capability[] capabilities = new Capability[count];
        long previous = 0;
        for (int index = 0; index < count; index++) {
            long id = Integer.toUnsignedLong(buffer.getInt());
            int semanticVersion = readU16(buffer);
            if (buffer.getShort() != 0) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_FLAGS);
            }
            Capability capability = new Capability(id, semanticVersion);
            if (id == previous) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.DUPLICATE_CAPABILITY);
            }
            if (Long.compareUnsigned(id, previous) < 0) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.OUT_OF_ORDER_CAPABILITY);
            }
            capabilities[index] = capability;
            previous = id;
        }
        return List.of(capabilities);
    }

    private static List<Capability> validatedCapabilities(List<Capability> capabilities) {
        Objects.requireNonNull(capabilities, "capabilities");
        requireCapabilityCount(capabilities.size());
        List<Capability> copy = List.copyOf(capabilities);
        long previous = 0;
        for (Capability capability : copy) {
            Objects.requireNonNull(capability, "capability");
            long id = capability.capabilityId();
            if (id == previous) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.DUPLICATE_CAPABILITY);
            }
            if (Long.compareUnsigned(id, previous) < 0) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.OUT_OF_ORDER_CAPABILITY);
            }
            previous = id;
        }
        return copy;
    }

    private static void requireCapabilityCount(int count) {
        if (count < 0 || count > MAX_CAPABILITIES) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.CAPABILITY_COUNT_OUT_OF_RANGE);
        }
    }

    private static void requireExactLength(int actual, int fixed, int count) {
        requireCapabilityCount(count);
        if ((long) fixed + (long) CAPABILITY_LENGTH * count != actual) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_HELLO_LENGTH);
        }
    }

    private static String validateBookieId(String bookieId) {
        Objects.requireNonNull(bookieId, "bookieId");
        return decodeBookieId(bookieId.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeBookieId(byte[] bytes) {
        if (bytes.length < 1 || bytes.length > MAX_BOOKIE_ID_BYTES) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_BOOKIE_ID_LENGTH);
        }
        for (byte value : bytes) {
            if (value == 0) {
                throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NUL_IN_BOOKIE_ID);
            }
        }
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException exception) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_UTF8);
        }
    }

    private static int readU16(ByteBuffer buffer) {
        return Short.toUnsignedInt(buffer.getShort());
    }
}

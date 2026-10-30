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
package org.apache.bookkeeper.discover;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;

/** Persistent common-scope enable binding. A value alone is not commissioning or storage-recovery authority. */
public record BookieRouteBinding(String clusterId, long clusterBirthZxid, BookieId bookieId,
        StorageIncarnation incarnation, UUID bindingId, ManifestIdentity manifest, String controlDirectory,
        SourceCookie sourceCookie, Phase phase, Optional<BootstrapSeal> seal) {
    public static final int MAX_BYTES = 8192;
    private static final long MAGIC = 0x5557425242303031L; // UWBRB001
    private static final UUID ZERO = new UUID(0, 0);

    public enum Phase { PREPARED, ENABLED }
    public enum ReadStatus { ABSENT, FOUND, UNSUPPORTED, UNAVAILABLE, CORRUPT, INCOMPATIBLE }
    public enum WriteStatus {
        APPLIED, ALREADY_APPLIED, CONFLICT, UNSUPPORTED, UNAVAILABLE, RESPONSE_LOST, INCOMPATIBLE
    }

    /** Original metadata Cookie identity, not a process episode or a second Classic incarnation. */
    public record SourceCookie(long birthZxid, int version, byte[] sha256) {
        public SourceCookie {
            sha256 = sha256.clone();
            if (birthZxid <= 0 || version < 0 || sha256.length != 32) {
                throw new IllegalArgumentException("Invalid commissioned source Cookie identity");
            }
        }

        @Override
        public byte[] sha256() {
            return sha256.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SourceCookie source && birthZxid == source.birthZxid
                    && version == source.version && Arrays.equals(sha256, source.sha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(birthZxid, version, Arrays.hashCode(sha256));
        }
    }

    /** Reference to the actual retained BOOTSTRAP_COMPLETE control record. */
    public record BootstrapSeal(UUID operationId, long sequence, long appliedCut) {
        public BootstrapSeal {
            Objects.requireNonNull(operationId, "seal operation");
            if (operationId.equals(ZERO) || sequence < 0 || appliedCut != sequence - 1) {
                throw new IllegalArgumentException("Invalid common bootstrap seal reference");
            }
        }
    }

    /** Native physical identity and version must be checked along with the decoded bytes. */
    public record Observation(BookieRouteBinding binding, UUID birthId, long birthZxid,
                              long modifiedZxid, int version) {
        public Observation {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(birthId, "native birth");
            if (birthId.equals(ZERO) || birthZxid <= 0 || modifiedZxid < birthZxid
                    || version != (binding.phase() == Phase.PREPARED ? 0 : 1)
                    || version == 0 && modifiedZxid != birthZxid || version == 1 && modifiedZxid == birthZxid) {
                throw new IllegalArgumentException("Invalid native common binding observation");
            }
        }
    }

    public record ReadResult(ReadStatus status, Optional<Observation> observation, String detail) {
        public ReadResult {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(detail, "detail");
            if ((status == ReadStatus.FOUND) != observation.isPresent()) {
                throw new IllegalArgumentException("Common binding read has no matching result");
            }
        }

        public static ReadResult found(Observation observation) {
            return new ReadResult(ReadStatus.FOUND, Optional.of(observation), "");
        }

        public static ReadResult status(ReadStatus status, String detail) {
            return new ReadResult(status, Optional.empty(), detail);
        }
    }

    public record WriteResult(WriteStatus status, Optional<Observation> observation, String detail) {
        public WriteResult {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(detail, "detail");
            if ((status == WriteStatus.APPLIED || status == WriteStatus.ALREADY_APPLIED) != observation.isPresent()) {
                throw new IllegalArgumentException("Common binding write has no matching result");
            }
        }

        public static WriteResult observed(WriteStatus status, Observation observation) {
            return new WriteResult(status, Optional.of(observation), "");
        }

        public static WriteResult status(WriteStatus status, String detail) {
            return new WriteResult(status, Optional.empty(), detail);
        }
    }

    public BookieRouteBinding {
        text(clusterId, 1024);
        Objects.requireNonNull(bookieId, "bookie");
        text(bookieId.toString(), 255);
        Objects.requireNonNull(incarnation, "commissioned common incarnation");
        Objects.requireNonNull(bindingId, "enable operation");
        Objects.requireNonNull(manifest, "complete common manifest");
        text(controlDirectory, 4096);
        Path root = Path.of(controlDirectory);
        Objects.requireNonNull(sourceCookie, "source Cookie");
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(seal, "seal");
        if (clusterId.equals("null") || clusterBirthZxid <= 0 || bindingId.equals(ZERO)
                || !root.isAbsolute() || !root.normalize().equals(root) || root.getParent() == null
                || (phase == Phase.ENABLED) != seal.isPresent()) {
            throw new IllegalArgumentException("Invalid persistent common scope binding");
        }
    }

    /** Immutable scope/source comparison shared by PREPARED and ENABLED. */
    public BookieRouteBinding prepared() {
        return phase == Phase.PREPARED ? this : new BookieRouteBinding(clusterId, clusterBirthZxid, bookieId,
                incarnation, bindingId, manifest, controlDirectory, sourceCookie, Phase.PREPARED, Optional.empty());
    }

    public BookieRouteBinding enabled(BootstrapSeal bootstrapSeal) {
        return new BookieRouteBinding(clusterId, clusterBirthZxid, bookieId, incarnation, bindingId, manifest,
                controlDirectory, sourceCookie, Phase.ENABLED, Optional.of(bootstrapSeal));
    }

    /** Bounded canonical bytes; contains neither a master key nor a native connection reference. */
    public byte[] encode() {
        byte[] cluster = clusterId.getBytes(StandardCharsets.UTF_8);
        byte[] bookie = bookieId.toString().getBytes(StandardCharsets.UTF_8);
        byte[] control = controlDirectory.getBytes(StandardCharsets.UTF_8);
        ByteBuffer out = ByteBuffer.allocate(144 + cluster.length + bookie.length + control.length
                + (seal.isPresent() ? 32 : 0));
        out.putLong(MAGIC).putInt(1).putInt(phase == Phase.PREPARED ? 1 : 2);
        putText(out, cluster);
        out.putLong(clusterBirthZxid);
        putText(out, bookie);
        out.put(incarnation.value()).putLong(bindingId.getMostSignificantBits())
                .putLong(bindingId.getLeastSignificantBits()).put(manifest.value());
        putText(out, control);
        out.putLong(sourceCookie.birthZxid()).putInt(sourceCookie.version()).put(sourceCookie.sha256());
        seal.ifPresent(value -> out.putLong(value.operationId().getMostSignificantBits())
                .putLong(value.operationId().getLeastSignificantBits()).putLong(value.sequence())
                .putLong(value.appliedCut()));
        return out.array();
    }

    public static BookieRouteBinding decode(byte[] encoded) {
        if (encoded.length < 16 || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("Common binding length exceeds its fixed bounds");
        }
        byte[] stable = encoded.clone();
        ByteBuffer in = ByteBuffer.wrap(stable);
        try {
            if (in.getLong() != MAGIC) {
                throw new IllegalArgumentException("Unknown common binding magic");
            }
            if (in.getInt() != 1) {
                throw new UnsupportedSchemaException();
            }
            Phase phase = switch (in.getInt()) {
                case 1 -> Phase.PREPARED;
                case 2 -> Phase.ENABLED;
                default -> throw new IllegalArgumentException("Unknown common binding phase");
            };
            String cluster = readText(in, 1024);
            long clusterBirth = in.getLong();
            BookieId bookie = BookieId.parse(readText(in, 255));
            byte[] incarnation = new byte[16];
            in.get(incarnation);
            UUID operation = new UUID(in.getLong(), in.getLong());
            byte[] manifest = new byte[32];
            in.get(manifest);
            String control = readText(in, 4096);
            long cookieBirth = in.getLong();
            int cookieVersion = in.getInt();
            byte[] cookieHash = new byte[32];
            in.get(cookieHash);
            Optional<BootstrapSeal> seal = phase == Phase.PREPARED ? Optional.empty()
                    : Optional.of(new BootstrapSeal(new UUID(in.getLong(), in.getLong()), in.getLong(), in.getLong()));
            var result = new BookieRouteBinding(cluster, clusterBirth, bookie, new StorageIncarnation(incarnation),
                    operation, new ManifestIdentity(manifest), control,
                    new SourceCookie(cookieBirth, cookieVersion, cookieHash), phase, seal);
            if (in.hasRemaining() || !Arrays.equals(stable, result.encode())) {
                throw new IllegalArgumentException("Noncanonical common binding bytes");
            }
            return result;
        } catch (BufferUnderflowException malformed) {
            throw new IllegalArgumentException("Incomplete common binding", malformed);
        }
    }

    public static final class UnsupportedSchemaException extends IllegalArgumentException {
        private UnsupportedSchemaException() {
            super("Unsupported persistent common binding schema");
        }
    }

    private static void text(String value, int maximum) {
        Objects.requireNonNull(value, "binding text");
        if (value.isEmpty() || value.indexOf(0) >= 0 || value.getBytes(StandardCharsets.UTF_8).length > maximum
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Invalid common binding text");
        }
    }

    private static void putText(ByteBuffer out, byte[] text) {
        out.putInt(text.length).put(text);
    }

    private static String readText(ByteBuffer in, int maximum) {
        int length = in.getInt();
        if (length < 1 || length > maximum || length > in.remaining()) {
            throw new IllegalArgumentException("Invalid common binding text length");
        }
        ByteBuffer text = in.slice(in.position(), length);
        in.position(in.position() + length);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(text).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("Malformed common binding UTF-8", malformed);
        }
    }
}

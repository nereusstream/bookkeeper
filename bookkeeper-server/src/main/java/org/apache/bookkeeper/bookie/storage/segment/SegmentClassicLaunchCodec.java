/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.net.BookieId;
import org.apache.zookeeper.common.PathUtils;

/** Fixed launch bytes only. A verified packet is not a startup permit, native ABSENT or a drain result. */
final class SegmentClassicLaunchCodec {
    static final int MAX_HELLO_BYTES = 512;
    static final int MAX_START_BYTES = 4096;
    private static final long HELLO_MAGIC = 0x5557434c48303031L; // UWCLH001
    private static final long START_MAGIC = 0x5557434c41303031L; // UWCLA001
    private static final int HEADER_BYTES = 16;
    private static final int START_HEADER_BYTES = HEADER_BYTES + 32;
    private static final int SIGNATURE_BYTES = 64;
    private static final byte[] START_DOMAIN = "BookKeeper/ControlledClassicLaunch/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SCOPE_DOMAIN = "BookKeeper/ControlledClassicSource/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final UUID ZERO = new UUID(0, 0);

    private SegmentClassicLaunchCodec() { }

    /** Original namespace includes the actual client chroot and ledgers root, not a future binding path. */
    record OriginalScope(String namespace, String clusterId, long clusterBirthZxid, BookieId bookie,
                         BookieRouteBinding.SourceCookie cookie, List<SegmentArenaManifest.ClassicRoot> roots) {
        OriginalScope {
            text(namespace, 4096);
            PathUtils.validatePath(namespace);
            text(clusterId, 1024);
            Objects.requireNonNull(bookie, "bookie");
            text(bookie.toString(), 255);
            Objects.requireNonNull(cookie, "cookie");
            roots = List.copyOf(roots);
            if (clusterId.equals("null") || clusterBirthZxid <= 0 || roots.isEmpty() || roots.size() > 192) {
                throw new IllegalArgumentException("Original Classic source and complete bounded roots required");
            }
            String previous = null;
            int roles = 0;
            for (int i = 0; i < roots.size(); i++) {
                var root = roots.get(i);
                String name = root.directory().toString();
                text(name, 4096);
                if (previous != null && previous.compareTo(name) >= 0) {
                    throw new IllegalArgumentException("Original Classic roots must be uniquely ordered");
                }
                for (int j = 0; j < i; j++) {
                    Path other = roots.get(j).directory();
                    if (root.directory().startsWith(other) || other.startsWith(root.directory())) {
                        throw new IllegalArgumentException("Original Classic roots cannot overlap");
                    }
                }
                previous = name;
                roles |= roleBits(root);
            }
            if (roles != 7) {
                throw new IllegalArgumentException("All original Classic root roles required");
            }
        }

        SegmentClassicHandoff.Digest identity() {
            ByteBuffer b = ByteBuffer.allocate(1024 * 1024);
            putText(b, namespace, 4096);
            putText(b, clusterId, 1024);
            b.putLong(clusterBirthZxid);
            putText(b, bookie.toString(), 255);
            b.putLong(cookie.birthZxid()).putInt(cookie.version()).put(cookie.sha256());
            b.putShort((short) roots.size());
            for (var root : roots) {
                putText(b, root.directory().toString(), 4096);
                b.put((byte) roleBits(root));
            }
            return SegmentClassicHandoff.digest(SCOPE_DOMAIN, Arrays.copyOf(b.array(), b.position()));
        }
    }

    /** All values are observations to compare; construction never establishes their provenance. */
    record Hello(String recipeId, SegmentClassicHandoff.Digest recipe, SegmentClassicHandoff.Digest program,
                 SegmentClassicHandoff.Digest configuration, SegmentClassicHandoff.Digest originalScope,
                 long oldUid, long pid, Instant started, SegmentClassicHandoff.Digest challenge) {
        Hello {
            SegmentClassicLaunchCodec.recipeId(recipeId);
            Objects.requireNonNull(recipe, "recipe");
            Objects.requireNonNull(program, "program");
            Objects.requireNonNull(configuration, "configuration");
            Objects.requireNonNull(originalScope, "originalScope");
            Objects.requireNonNull(started, "started");
            Objects.requireNonNull(challenge, "challenge");
            if (oldUid <= 0 || oldUid > 0xffff_ffffL || pid <= 0 || started.isBefore(Instant.EPOCH)) {
                throw new IllegalArgumentException("A nonprivileged old UID and actual process identity required");
            }
        }
    }

    record Start(SegmentClassicHandoff.Digest signerKeyId, Hello hello, UUID launcherEpisode) {
        Start {
            Objects.requireNonNull(signerKeyId, "signerKeyId");
            Objects.requireNonNull(hello, "hello");
            if (Objects.requireNonNull(launcherEpisode, "launcherEpisode").equals(ZERO)) {
                throw new IllegalArgumentException("Launch authorization requires its original launcher episode");
            }
        }
    }

    static byte[] encodeHello(Hello hello) {
        ByteBuffer b = header(MAX_HELLO_BYTES, HELLO_MAGIC);
        putHello(b, hello);
        b.putInt(12, b.position() - HEADER_BYTES);
        return Arrays.copyOf(b.array(), b.position());
    }

    static Hello decodeHello(byte[] encoded) throws IOException {
        byte[] stable = stable(encoded, MAX_HELLO_BYTES, HEADER_BYTES);
        try {
            ByteBuffer b = input(stable, HELLO_MAGIC);
            if (b.getInt() != stable.length - HEADER_BYTES) {
                throw new IllegalArgumentException("HELLO length mismatch");
            }
            Hello hello = getHello(b);
            if (b.hasRemaining() || !Arrays.equals(stable, encodeHello(hello))) {
                throw new IllegalArgumentException("Noncanonical Classic HELLO");
            }
            return hello;
        } catch (IllegalArgumentException | BufferUnderflowException | DateTimeException malformed) {
            throw new IOException("Invalid fixed Classic HELLO", malformed);
        }
    }

    /** No signing API. Trust and current process/source checks belong to the retained fixed launcher/child. */
    static Start verifyStart(byte[] encoded, SegmentClassicHandoff.Digest signer, PublicKey trustedKey,
                             Hello expected) throws IOException {
        byte[] stable = stable(encoded, MAX_START_BYTES, START_HEADER_BYTES + SIGNATURE_BYTES);
        try {
            ByteBuffer b = input(stable, START_MAGIC);
            int payload = b.getInt();
            var keyId = digest(b);
            int unsignedLength = stable.length - SIGNATURE_BYTES;
            if (payload != unsignedLength - START_HEADER_BYTES || !keyId.equals(signer)) {
                throw new IllegalArgumentException("START length or fixed signer mismatch");
            }
            Signature signature = Signature.getInstance("Ed25519");
            signature.initVerify(trustedKey);
            signature.update(START_DOMAIN);
            signature.update(stable, 0, unsignedLength);
            if (!signature.verify(stable, unsignedLength, SIGNATURE_BYTES)) {
                throw new IOException("Classic START signature rejected");
            }
            b.limit(unsignedLength);
            if (b.getInt() != 1) { // START only, never an offline bootstrap/retirement/drain purpose.
                throw new IllegalArgumentException("Unknown Classic launch purpose");
            }
            var result = new Start(keyId, getHello(b), new UUID(b.getLong(), b.getLong()));
            if (b.hasRemaining() || !result.hello().equals(expected)) {
                throw new IllegalArgumentException("START differs from the current child challenge or observations");
            }
            return result;
        } catch (IllegalArgumentException | BufferUnderflowException | DateTimeException
                | GeneralSecurityException malformed) {
            throw new IOException("Invalid fixed Classic START", malformed);
        }
    }

    static void recipeId(String value) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) {
            throw new IllegalArgumentException("Recipe ID must select a bounded approved name, never a path");
        }
    }

    private static void putHello(ByteBuffer b, Hello h) {
        putText(b, h.recipeId(), 128);
        b.put(h.recipe().value()).put(h.program().value()).put(h.configuration().value())
                .put(h.originalScope().value()).putInt((int) h.oldUid()).putLong(h.pid())
                .putLong(h.started().getEpochSecond()).putInt(h.started().getNano()).put(h.challenge().value());
    }

    private static Hello getHello(ByteBuffer b) {
        String id = getText(b, 128);
        var recipe = digest(b);
        var program = digest(b);
        var config = digest(b);
        var scope = digest(b);
        long uid = Integer.toUnsignedLong(b.getInt());
        long pid = b.getLong();
        long seconds = b.getLong();
        int nanos = b.getInt();
        if (nanos < 0 || nanos >= 1_000_000_000) {
            throw new IllegalArgumentException("Noncanonical process start time");
        }
        return new Hello(id, recipe, program, config, scope, uid, pid,
                Instant.ofEpochSecond(seconds, nanos), digest(b));
    }

    private static SegmentClassicHandoff.Digest digest(ByteBuffer b) {
        byte[] bytes = new byte[32];
        b.get(bytes);
        return new SegmentClassicHandoff.Digest(bytes);
    }

    private static int roleBits(SegmentArenaManifest.ClassicRoot root) {
        return root.roles().stream().mapToInt(role -> role.bit).reduce(0, (a, b) -> a | b);
    }

    private static ByteBuffer header(int maximum, long magic) {
        return ByteBuffer.allocate(maximum).putLong(magic).putShort((short) 1).putShort((short) 0).putInt(0);
    }

    private static ByteBuffer input(byte[] stable, long magic) {
        ByteBuffer b = ByteBuffer.wrap(stable);
        if (b.getLong() != magic || b.getShort() != 1 || b.getShort() != 0) {
            throw new IllegalArgumentException("Unknown Classic launch magic/schema/flags");
        }
        return b;
    }

    private static byte[] stable(byte[] encoded, int maximum, int minimum) throws IOException {
        if (encoded.length < minimum || encoded.length > maximum) {
            throw new IOException("Classic launch packet exceeds fixed bounds");
        }
        return encoded.clone();
    }

    private static byte[] text(String value, int maximum) {
        if (value == null || value.isBlank() || value.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Invalid canonical launch text");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maximum) {
            throw new IllegalArgumentException("Launch text exceeds fixed bounds");
        }
        return bytes;
    }

    private static void putText(ByteBuffer b, String value, int maximum) {
        byte[] bytes = text(value, maximum);
        b.putShort((short) bytes.length).put(bytes);
    }

    private static String getText(ByteBuffer b, int maximum) {
        int length = Short.toUnsignedInt(b.getShort());
        if (length < 1 || length > maximum || length > b.remaining()) {
            throw new IllegalArgumentException("Invalid bounded launch text length");
        }
        byte[] bytes = new byte[length];
        b.get(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("Invalid launch UTF-8", malformed);
        }
    }
}

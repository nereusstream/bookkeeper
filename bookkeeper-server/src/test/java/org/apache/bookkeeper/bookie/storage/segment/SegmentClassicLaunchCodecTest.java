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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Test;

/** Independent byte/crypto fixtures only: no startup trust root, native absence or qualified child. */
public class SegmentClassicLaunchCodecTest {
    private static SegmentClassicHandoff.Digest digest(int value) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) value);
        return new SegmentClassicHandoff.Digest(bytes);
    }

    private static SegmentClassicLaunchCodec.Hello hello() {
        return new SegmentClassicLaunchCodec.Hello("classic-1", digest(1), digest(2), digest(3), digest(4),
                1001, 99, Instant.ofEpochSecond(1000, 123), digest(5));
    }

    private static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicLaunchCodecTest.class.getResourceAsStream("/unified-wal-segment/" + name)) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                    .replaceAll("\\s", ""));
        }
    }

    private static PublicKey publicKey() throws Exception {
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(HexFormat.of().parseHex(
                "302a300506032b6570032100d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")));
    }

    /** Test signs exact mutated bytes, without a production signer or caller-supplied verifier callback. */
    private static byte[] resign(byte[] bytes) throws Exception {
        byte[] result = bytes.clone();
        var key = KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(HexFormat.of().parseHex(
                "302e020100300506032b6570042204209d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")));
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(key);
        signer.update("BookKeeper/ControlledClassicLaunch/v1\0".getBytes(StandardCharsets.US_ASCII));
        signer.update(result, 0, result.length - 64);
        System.arraycopy(signer.sign(), 0, result, result.length - 64, 64);
        return result;
    }

    @Test
    public void helloAndStartMatchIndependentPythonAndOpenSslVectors() throws Exception {
        byte[] h = vector("classic-launch-hello-v1.hex");
        byte[] s = vector("classic-launch-start-v1.hex");
        assertEquals(211, h.length);
        assertEquals(327, s.length);
        assertArrayEquals(h, SegmentClassicLaunchCodec.encodeHello(hello()));
        assertEquals(hello(), SegmentClassicLaunchCodec.decodeHello(h));
        assertArrayEquals(s, resign(s));
        assertEquals(new SegmentClassicLaunchCodec.Start(digest(9), hello(), new UUID(0, 7)),
                SegmentClassicLaunchCodec.verifyStart(s, digest(9), publicKey(), hello()));
    }

    @Test
    public void everyChangedOrTruncatedSignedByteAndUntrustedSignerIsRejected() throws Exception {
        byte[] s = vector("classic-launch-start-v1.hex");
        PublicKey key = publicKey();
        for (int i = 0; i < s.length; i++) {
            byte[] changed = s.clone();
            changed[i] ^= 1;
            assertThrows("Changed byte " + i, IOException.class,
                    () -> SegmentClassicLaunchCodec.verifyStart(changed, digest(9), key, hello()));
            byte[] truncated = Arrays.copyOf(s, i);
            assertThrows("Truncated at " + i, IOException.class,
                    () -> SegmentClassicLaunchCodec.verifyStart(truncated, digest(9), key, hello()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.verifyStart(s, digest(8), key, hello()));
        PublicKey other = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
        assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.verifyStart(s, digest(9), other, hello()));
        assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.verifyStart(
                new byte[4097], digest(9), key, hello()));
    }

    @Test
    public void validSignaturesCannotChangePurposeOrCanonicalFieldRules() throws Exception {
        byte[] s = vector("classic-launch-start-v1.hex");
        for (int[] mutation : List.of(new int[]{8, 0x00020000}, new int[]{12, 0}, new int[]{48, 2},
                new int[]{191, 0}, new int[]{195, 0x80000000}, new int[]{211, 1_000_000_000})) {
            byte[] changed = s.clone();
            ByteBuffer.wrap(changed).putInt(mutation[0], mutation[1]);
            byte[] signed = resign(changed);
            assertThrows(IOException.class,
                    () -> SegmentClassicLaunchCodec.verifyStart(signed, digest(9), publicKey(), hello()));
        }
        for (int index : new int[]{54, 55}) {
            byte[] changed = s.clone();
            changed[index] = (byte) (index == 54 ? '/' : 0xc0);
            byte[] signed = resign(changed);
            assertThrows(IOException.class,
                    () -> SegmentClassicLaunchCodec.verifyStart(signed, digest(9), publicKey(), hello()));
        }
        byte[] zeroEpisode = s.clone();
        Arrays.fill(zeroEpisode, 247, 263, (byte) 0);
        byte[] signed = resign(zeroEpisode);
        assertThrows(IOException.class,
                () -> SegmentClassicLaunchCodec.verifyStart(signed, digest(9), publicKey(), hello()));
    }

    @Test
    public void signedStartMustMatchEveryCurrentChildObservation() throws Exception {
        byte[] s = vector("classic-launch-start-v1.hex");
        var h = hello();
        var changed = List.of(
                new SegmentClassicLaunchCodec.Hello("classic-2", h.recipe(), h.program(), h.configuration(),
                        h.originalScope(), h.oldUid(), h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), digest(6), h.program(), h.configuration(),
                        h.originalScope(), h.oldUid(), h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), digest(6), h.configuration(),
                        h.originalScope(), h.oldUid(), h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), digest(6),
                        h.originalScope(), h.oldUid(), h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), h.configuration(),
                        digest(6), h.oldUid(), h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), h.configuration(),
                        h.originalScope(), 1002, h.pid(), h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), h.configuration(),
                        h.originalScope(), h.oldUid(), 100, h.started(), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), h.configuration(),
                        h.originalScope(), h.oldUid(), h.pid(), h.started().plusNanos(1), h.challenge()),
                new SegmentClassicLaunchCodec.Hello(h.recipeId(), h.recipe(), h.program(), h.configuration(),
                        h.originalScope(), h.oldUid(), h.pid(), h.started(), digest(6)));
        for (var expected : changed) {
            assertThrows(IOException.class,
                    () -> SegmentClassicLaunchCodec.verifyStart(s, digest(9), publicKey(), expected));
        }
    }

    @Test
    public void helloHasBoundedCanonicalFieldsAndCannotSelectAPath() throws Exception {
        byte[] h = vector("classic-launch-hello-v1.hex");
        for (int i = 0; i < h.length; i++) {
            byte[] truncated = Arrays.copyOf(h, i);
            assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.decodeHello(truncated));
        }
        assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.decodeHello(new byte[513]));
        byte[] extra = Arrays.copyOf(h, h.length + 1);
        ByteBuffer.wrap(extra).putInt(12, extra.length - 16);
        assertThrows(IOException.class, () -> SegmentClassicLaunchCodec.decodeHello(extra));
        for (String id : List.of("", ".", "../classic-1", "/classic-1", "classic.1", "x".repeat(129))) {
            assertThrows(IllegalArgumentException.class, () -> SegmentClassicLaunchCodec.recipeId(id));
        }
        var maximum = new SegmentClassicLaunchCodec.Hello("x".repeat(128), digest(1), digest(2), digest(3),
                digest(4), 0xffff_ffffL, Long.MAX_VALUE, Instant.ofEpochSecond(1000, 999_999_999), digest(5));
        assertEquals(maximum, SegmentClassicLaunchCodec.decodeHello(SegmentClassicLaunchCodec.encodeHello(maximum)));
    }

    private static SegmentClassicLaunchCodec.OriginalScope scope(String namespace,
            BookieRouteBinding.SourceCookie cookie, List<SegmentArenaManifest.ClassicRoot> roots) {
        return new SegmentClassicLaunchCodec.OriginalScope(namespace, "cluster-1", 100,
                BookieId.parse("classic:3181"), cookie, roots);
    }

    @Test
    public void originalScopeDigestIncludesNamespaceCookieAndAllOrderedRootRoles() {
        var cookie = new BookieRouteBinding.SourceCookie(102, 3, digest(6).value());
        var roots = List.of(new SegmentArenaManifest.ClassicRoot(Path.of("/classic/a"),
                        Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                new SegmentArenaManifest.ClassicRoot(Path.of("/classic/b"),
                        Set.of(SegmentArenaManifest.ClassicRole.LEDGER, SegmentArenaManifest.ClassicRole.INDEX)));
        var s = scope("/tenant/ledgers", cookie, roots);
        assertEquals("6e1b58eff40f3188d55d8b5cb1b95583d25af5c68d4446f1c5d0b5840fd7a78d",
                HexFormat.of().formatHex(s.identity().value()));
        assertNotEquals(s.identity(), scope("/other/ledgers", cookie, roots).identity());
        assertNotEquals(s.identity(), scope("/tenant/ledgers",
                new BookieRouteBinding.SourceCookie(103, 3, digest(6).value()), roots).identity());
        assertNotEquals(s.identity(), scope("/tenant/ledgers",
                new BookieRouteBinding.SourceCookie(102, 4, digest(6).value()), roots).identity());
        assertNotEquals(s.identity(), scope("/tenant/ledgers",
                new BookieRouteBinding.SourceCookie(102, 3, digest(7).value()), roots).identity());
        assertThrows(IllegalArgumentException.class,
                () -> scope("/tenant/ledgers", cookie, List.of(roots.get(1), roots.get(0))));
        assertThrows(IllegalArgumentException.class,
                () -> scope("/tenant/ledgers", cookie, List.of(roots.get(0))));
    }

    @Test
    public void launchAndTerminalHandoffDomainsAreMutuallyRejected() throws Exception {
        byte[] start = vector("classic-launch-start-v1.hex");
        byte[] terminal = vector("classic-handoff-proof-v1.hex");
        assertThrows(IOException.class,
                () -> SegmentClassicHandoff.verify(start, digest(9), publicKey()));
        assertThrows(IOException.class,
                () -> SegmentClassicLaunchCodec.verifyStart(terminal, digest(9), publicKey(), hello()));
    }
}

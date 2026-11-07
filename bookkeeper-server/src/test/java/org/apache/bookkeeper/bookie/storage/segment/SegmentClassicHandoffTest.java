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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Test;

/** Byte/crypto fixtures only. No supervised process, UID isolation or adoptable session is established here. */
public class SegmentClassicHandoffTest {
    private static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    private static final byte[] PUBLIC = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");

    private static byte[] filled(int length, int value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static SegmentClassicHandoff.Digest digest(int value) {
        return new SegmentClassicHandoff.Digest(filled(32, value));
    }

    private static StorageIncarnation incarnation() {
        byte[] bytes = new byte[16];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (index + 1);
        }
        return new StorageIncarnation(bytes);
    }

    private static SegmentArenaManifest manifest() {
        return new SegmentArenaManifest(incarnation(), BookieId.parse("handoff:3181"), List.of(
                new SegmentArenaManifest.Arena(new UUID(0, 11), Path.of("/handoff/arena-a"), 1, 16384),
                new SegmentArenaManifest.Arena(new UUID(0, 12), Path.of("/handoff/arena-b"), 2, 32768)),
                Path.of("/handoff/control"), List.of(1, 0, 1, 0), List.of(
                        new SegmentArenaManifest.ClassicRoot(Path.of("/handoff/classic-0"),
                                Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(Path.of("/handoff/classic-1"),
                                Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                        SegmentArenaManifest.ClassicRole.INDEX))));
    }

    private static SegmentClassicHandoff.Policy policy() {
        var manifest = manifest();
        return new SegmentClassicHandoff.Policy(new UUID(0, 2), manifest.bookieId(), manifest.incarnation(),
                manifest.identity(), 1002);
    }

    private static SegmentClassicHandoff.Operation operation() {
        var manifest = manifest();
        var nativeBinding = new BookieRouteBinding("cluster-1", 100, manifest.bookieId(), manifest.incarnation(),
                new UUID(0, 2), manifest.identity(), manifest.controlDirectory().toString(),
                new BookieRouteBinding.SourceCookie(102, 3, filled(32, 3)), BookieRouteBinding.Phase.PREPARED,
                Optional.empty());
        var observed = new BookieRouteBinding.Observation(nativeBinding, new UUID(0, 4), 104, 104, 0);
        var source = new ZKProfileStartupRequirementsStore.SourceIdentity("/commissioned/aGFuZG9mZjozMTgx",
                new UUID(0, 5), 106, 106, 0, filled(32, 6));
        return new SegmentClassicHandoff.Operation("deployment-1", digest(8), digest(9), observed,
                new SegmentClassicHandoff.Requirements(source, 1, 2, 3, 4, 2, 2),
                new SegmentClassicHandoff.Episode(new UUID(0, 7), 99, Instant.ofEpochSecond(1000, 123),
                        108, 0xfedcba9876543210L), 1001, 1002, SegmentClassicHandoff.keyIdentity(filled(32, 10)),
                SegmentClassicHandoff.digest(SegmentClassicHandoff.encodePolicy(policy())));
    }

    private static List<SegmentClassicHandoff.Member> members() {
        var result = new ArrayList<SegmentClassicHandoff.Member>();
        int inode = 100;
        for (int[] role : List.of(new int[]{1, 0}, new int[]{1, 1}, new int[]{2, 0},
                new int[]{3, 0}, new int[]{3, 1})) {
            result.add(new SegmentClassicHandoff.Member(new SegmentClassicHandoff.Physical(role[0], role[1],
                    1, inode++), 1002, 1002, 0700));
        }
        return result;
    }

    private static SegmentClassicHandoff.Proof proof(SegmentClassicHandoff.Operation operation,
                                                    List<SegmentClassicHandoff.Member> members) {
        var intent = new SegmentClassicHandoff.Retirement(operation,
                members.stream().map(SegmentClassicHandoff.Member::physical).toList());
        return new SegmentClassicHandoff.Proof(operation, members,
                new SegmentClassicHandoff.Terminal(12, Instant.ofEpochSecond(1001, 456), digest(11)),
                digest(12),
                SegmentClassicHandoff.digest(SegmentClassicHandoff.encodeRetirement(intent)));
    }

    private static SegmentClassicHandoff.Proof proof() {
        return proof(operation(), members());
    }

    private static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicHandoffTest.class.getResourceAsStream("/unified-wal-segment/" + name)) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII)
                    .replaceAll("\\s", ""));
        }
    }

    private static PublicKey publicKey() throws Exception {
        byte[] encoded = ByteBuffer.allocate(44).put(HexFormat.of().parseHex("302a300506032b6570032100"))
                .put(PUBLIC).array();
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
    }

    private static PrivateKey privateKey() throws Exception {
        byte[] encoded = ByteBuffer.allocate(48).put(HexFormat.of().parseHex("302e020100300506032b657004220420"))
                .put(SEED).array();
        return KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(encoded));
    }

    /** The test signs explicit bytes. Production codec has no sign-facts API or private key input. */
    private static byte[] resign(byte[] encoded) throws Exception {
        return resign(encoded, 1);
    }

    private static byte[] resign(byte[] encoded, int schema) throws Exception {
        byte[] result = encoded.clone();
        int length = result.length - 64;
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(privateKey());
        signer.update(("BookKeeper/CommonClassicHandoff/v" + schema + "\0").getBytes(StandardCharsets.US_ASCII));
        signer.update(result, 0, length);
        byte[] signature = signer.sign();
        System.arraycopy(signature, 0, result, length, signature.length);
        return result;
    }

    private static SegmentClassicHandoff.HostViewBinding hostBinding() {
        return new SegmentClassicHandoff.HostViewBinding(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"),
                0xf123456789abcdefL, new NativeIO.LinuxFilesystemView(0xf0000017L,
                        new NativeIO.FileIdentity(0x0123456789abcdefL, 0xfedcba9876543210L), 77));
    }

    @Test
    public void hostBoundProofMatchesIndependentFixedBytesAndUsesItsOwnSignatureDomain() throws Exception {
        var original = proof();
        var bound = new SegmentClassicHandoff.Proof(original.operation(), original.members(), original.terminal(),
                original.unixIsolationReceipt(), original.retirement(), Optional.of(hostBinding()));
        byte[] legacy = vector("classic-handoff-proof-v1.hex");
        byte[] fields = HexFormat.of().parseHex("0123456789abcdef0123456789abcdeff123456789abcdeff0000017"
                + "0123456789abcdeffedcba98765432100000004d");
        // Fixed Ed25519 signature generated independently with OpenSSL over the v2 domain and explicit byte layout.
        byte[] signature = HexFormat.of().parseHex("b1a15ee0fcda5cc7ec248426caecf1f6e60a1aa277d8e9e4c613d3f2bf17eeba7"
                + "5633494c3052fa0f66efa108b4b8f24ab1077910790e0b289da807efa40ce0f");
        int oldUnsigned = legacy.length - SegmentClassicHandoff.SIGNATURE_BYTES;
        int newUnsigned = oldUnsigned + fields.length;
        byte[] expected = ByteBuffer.allocate(newUnsigned + signature.length).put(legacy, 0, oldUnsigned)
                .put(fields).put(signature).putLong(0, 0x555743484f463032L).putShort(8, (short) 2)
                .putInt(12, newUnsigned - SegmentClassicHandoff.HEADER_BYTES).array();
        assertArrayEquals(expected, SegmentClassicHandoff.envelope(bound, signature));
        assertArrayEquals(expected, resign(expected, 2));
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(privateKey());
        signer.update(SegmentClassicHandoff.signingMessage(bound));
        assertArrayEquals(signature, signer.sign());
        var key = publicKey();
        assertEquals(bound, SegmentClassicHandoff.verify(expected, digest(9), key));
        assertEquals(original, SegmentClassicHandoff.verify(legacy, digest(9), key));
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(resign(expected), digest(9), key));
        for (int offset = oldUnsigned; offset < newUnsigned; offset++) {
            byte[] changed = expected.clone();
            changed[offset] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(changed, digest(9), key));
        }
        for (int offset : new int[]{8, 10}) {
            byte[] changed = expected.clone();
            changed[offset + 1] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(resign(changed, 2), digest(9), key));
        }
        byte[] missing = Arrays.copyOf(expected, legacy.length);
        ByteBuffer.wrap(missing).putInt(12, oldUnsigned - SegmentClassicHandoff.HEADER_BYTES);
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(resign(missing, 2), digest(9), key));
        for (int[] invalid : List.of(new int[]{0, 16}, new int[]{24, 4}, new int[]{44, 4})) {
            byte[] changed = expected.clone();
            Arrays.fill(changed, oldUnsigned + invalid[0], oldUnsigned + invalid[0] + invalid[1], (byte) 0);
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(resign(changed, 2), digest(9), key));
        }
    }

    @Test
    public void linuxColdViewComparisonRejectsBootBirthNamespaceAndRootMismatches() throws Exception {
        var host = hostBinding();
        var namespaces = new NativeIO.LinuxThreadNamespaces(0xefff_fffcL, 0xefff_fffdL);
        var birth = new NativeIO.LinuxProcessBirth(81, 0, host.pid1StartTicks());
        var view = host.view();
        SegmentClassicHandoffMaterials.requireMatchingHostView(host, host.bootId(), namespaces, birth, view);
        assertThrows(IOException.class, () -> SegmentClassicHandoffMaterials.requireMatchingHostView(
                host, new UUID(0, 91), namespaces, birth, view));
        assertThrows(IOException.class, () -> SegmentClassicHandoffMaterials.requireMatchingHostView(
                host, host.bootId(), namespaces, new NativeIO.LinuxProcessBirth(81, 0, 0), view));
        for (var wrong : List.of(new NativeIO.LinuxThreadNamespaces(0xf000_0001L, namespaces.userInode()),
                new NativeIO.LinuxThreadNamespaces(namespaces.pidInode(), 0xf000_0002L))) {
            assertThrows(IOException.class, () -> SegmentClassicHandoffMaterials.requireMatchingHostView(
                    host, host.bootId(), wrong, birth, view));
        }
        for (var wrong : List.of(new NativeIO.LinuxFilesystemView(1, view.root(), view.rootMountId()),
                new NativeIO.LinuxFilesystemView(view.mountNamespaceInode(), view.root(), 5),
                new NativeIO.LinuxFilesystemView(view.mountNamespaceInode(), new NativeIO.FileIdentity(1, 2),
                        view.rootMountId()))) {
            assertThrows(IOException.class, () -> SegmentClassicHandoffMaterials.requireMatchingHostView(
                    host, host.bootId(), namespaces, birth, wrong));
        }
    }

    @Test
    public void policyIntentAndSignedProofMatchIndependentPythonAndOpenSslVectors() throws Exception {
        byte[] policy = vector("classic-handoff-policy-v1.hex");
        byte[] intent = vector("classic-handoff-retirement-v1.hex");
        byte[] encoded = vector("classic-handoff-proof-v1.hex");
        assertEquals(100, policy.length);
        assertEquals(685, intent.length);
        assertEquals(973, encoded.length);
        assertArrayEquals(policy, SegmentClassicHandoff.encodePolicy(policy()));
        assertEquals(policy(), SegmentClassicHandoff.decodePolicy(policy));
        var retirement = new SegmentClassicHandoff.Retirement(operation(),
                members().stream().map(SegmentClassicHandoff.Member::physical).toList());
        assertArrayEquals(intent, SegmentClassicHandoff.encodeRetirement(retirement));
        assertEquals(retirement, SegmentClassicHandoff.decodeRetirement(intent));
        assertArrayEquals(encoded, SegmentClassicHandoff.envelope(proof(), Arrays.copyOfRange(encoded, 909, 973)));
        assertArrayEquals(encoded, resign(encoded));
        assertEquals(proof(), SegmentClassicHandoff.verify(encoded, digest(9), publicKey()));
        ByteBuffer header = ByteBuffer.wrap(encoded);
        assertEquals(0x555743484f463031L, header.getLong());
        assertEquals(1, header.getShort());
        assertEquals(0, header.getShort());
        assertEquals(encoded.length - 48 - 64, header.getInt());
        byte[] message = SegmentClassicHandoff.signingMessage(proof());
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(privateKey());
        signer.update(message);
        assertArrayEquals(Arrays.copyOfRange(encoded, 909, 973), signer.sign());
        assertArrayEquals(encoded, resign(SegmentClassicHandoff.envelope(proof(), new byte[64])));
        assertArrayEquals(Arrays.copyOf(encoded, encoded.length - 64),
                Arrays.copyOfRange(message, message.length - 909, message.length));
        SegmentClassicHandoff.requireMaterials(proof(), policy(), filled(32, 10), manifest());
    }

    @Test
    public void everyEnvelopeByteIsCoveredAndWrongSignerOrTrustedKeyRejects() throws Exception {
        byte[] vector = vector("classic-handoff-proof-v1.hex");
        PublicKey key = publicKey();
        for (int offset = 0; offset < vector.length; offset++) {
            byte[] changed = vector.clone();
            changed[offset] ^= 1;
            assertThrows("tampered byte " + offset, IOException.class,
                    () -> SegmentClassicHandoff.verify(changed, digest(9), key));
        }
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(vector, digest(8), key));
        var wrongKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic();
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(vector, digest(9), wrongKey));
    }

    @Test
    public void validSignaturesCannotHideUnknownSemanticsOverflowOrInvalidUtf8() throws Exception {
        byte[] vector = vector("classic-handoff-proof-v1.hex");
        // Offsets come from the independent Python field writer, not production parser internals.
        for (int[] change : List.of(new int[]{48, 2}, new int[]{132, Integer.MAX_VALUE},
                new int[]{353, Integer.MAX_VALUE}, new int[]{547, 0}, new int[]{649, 0x10000},
                new int[]{781, 2}, new int[]{785, 1}, new int[]{805, 1000000000}, new int[]{841, 2})) {
            byte[] changed = vector.clone();
            ByteBuffer.wrap(changed).putInt(change[0], change[1]);
            byte[] signed = resign(changed);
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(signed, digest(9), publicKey()));
        }
        for (int offset : List.of(619, 621)) {
            byte[] changed = vector.clone();
            ByteBuffer.wrap(changed).putShort(offset, (short) 0xffff);
            byte[] signed = resign(changed);
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(signed, digest(9), publicKey()));
        }
        byte[] invalidText = vector.clone();
        invalidText[56] = (byte) 0xff;
        byte[] signedText = resign(invalidText);
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(signedText, digest(9), publicKey()));
        byte[] unknownFlags = vector.clone();
        unknownFlags[11] = 1;
        byte[] signedFlags = resign(unknownFlags);
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(signedFlags, digest(9), publicKey()));
        byte[] extra = new byte[vector.length + 1];
        System.arraycopy(vector, 0, extra, 0, vector.length - 64);
        ByteBuffer.wrap(extra).putInt(12, vector.length - 48 - 64 + 1);
        byte[] signedExtra = resign(extra);
        assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(signedExtra, digest(9), publicKey()));
    }

    @Test
    public void proofAndMaterialFramesRejectTruncationOversizeUnknownFieldsAndTrailingBytes() throws Exception {
        byte[] vector = vector("classic-handoff-proof-v1.hex");
        for (int length : List.of(0, 1, 111, vector.length - 1, vector.length + 1, 65537)) {
            byte[] changed = Arrays.copyOf(vector, length);
            assertThrows(IOException.class, () -> SegmentClassicHandoff.verify(changed, digest(9), publicKey()));
        }
        byte[] policy = vector("classic-handoff-policy-v1.hex");
        byte[] intent = vector("classic-handoff-retirement-v1.hex");
        for (byte[] frame : List.of(policy, intent)) {
            for (int length : List.of(0, 11, frame.length - 1, frame.length + 1)) {
                byte[] changed = Arrays.copyOf(frame, length);
                if (frame == policy) {
                    assertThrows(IOException.class, () -> SegmentClassicHandoff.decodePolicy(changed));
                } else {
                    assertThrows(IOException.class, () -> SegmentClassicHandoff.decodeRetirement(changed));
                }
            }
            byte[] changed = frame.clone();
            changed[11] = 1;
            if (frame == policy) {
                assertThrows(IOException.class, () -> SegmentClassicHandoff.decodePolicy(changed));
            } else {
                assertThrows(IOException.class, () -> SegmentClassicHandoff.decodeRetirement(changed));
            }
        }
    }

    @Test
    public void exactPolicyAndIndependentKeyNeverAcceptAnotherScopeOrMaterial() throws Exception {
        var policy = policy();
        for (var changed : List.of(
                new SegmentClassicHandoff.Policy(new UUID(0, 3), policy.bookie(), policy.incarnation(),
                        policy.manifest(), policy.targetUid()),
                new SegmentClassicHandoff.Policy(policy.bindingId(), BookieId.parse("wrong:3181"), policy.incarnation(),
                        policy.manifest(), policy.targetUid()),
                new SegmentClassicHandoff.Policy(policy.bindingId(), policy.bookie(),
                        new StorageIncarnation(filled(16, 2)), policy.manifest(), policy.targetUid()),
                new SegmentClassicHandoff.Policy(policy.bindingId(), policy.bookie(), policy.incarnation(),
                        new ManifestIdentity(filled(32, 2)), policy.targetUid()),
                new SegmentClassicHandoff.Policy(policy.bindingId(), policy.bookie(), policy.incarnation(),
                        policy.manifest(), 1003))) {
            assertThrows(IOException.class, () -> SegmentClassicHandoff.requireMaterials(proof(), changed,
                    filled(32, 10), manifest()));
        }
        for (byte[] key : List.of(new byte[0], filled(16, 10), filled(32, 11))) {
            assertThrows(IOException.class, () -> SegmentClassicHandoff.requireMaterials(proof(), policy,
                    key, manifest()));
        }
        assertNotEquals(SegmentClassicHandoff.digest(filled(32, 10)),
                SegmentClassicHandoff.keyIdentity(filled(32, 10)));
        assertEquals("d7f8ede1b007bdc76e40ee5fb53799416d6dc2aaa83222d6a2eb7ad61d592ec2",
                HexFormat.of().formatHex(SegmentClassicHandoff.keyIdentity(filled(32, 10)).value()));
    }

    @Test
    public void signedMemberOmissionsStillRejectAgainstTheCompleteManifestAndAliasesCannotBeEncoded() throws Exception {
        var members = members();
        var missing = proof(operation(), members.subList(0, 4));
        byte[] signed = resign(SegmentClassicHandoff.envelope(missing, new byte[64]));
        var parsed = SegmentClassicHandoff.verify(signed, digest(9), publicKey());
        assertThrows(IOException.class, () -> SegmentClassicHandoff.requireMaterials(parsed, policy(),
                filled(32, 10), manifest()));
        var alias = new ArrayList<>(members);
        alias.set(1, new SegmentClassicHandoff.Member(new SegmentClassicHandoff.Physical(1, 1, 1, 100),
                1002, 1002, 0700));
        assertThrows(IllegalArgumentException.class, () -> proof(operation(), alias));
        var reordered = new ArrayList<>(members);
        java.util.Collections.swap(reordered, 0, 1);
        assertThrows(IllegalArgumentException.class, () -> proof(operation(), reordered));
        assertThrows(IllegalArgumentException.class, () -> proof(operation(), members.subList(2, 5)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicHandoff.Proof(operation(), members,
                proof().terminal(), digest(12), digest(1)));
    }

    @Test
    public void identitiesAreImmutableAndPacketTimeDoesNotIssueAnExpiryLease() throws Exception {
        var list = members();
        var proof = proof(operation(), list);
        list.clear();
        assertEquals(5, proof.members().size());
        byte[] key = proof.operation().aesKey().value();
        key[0] ^= 1;
        assertEquals(SegmentClassicHandoff.keyIdentity(filled(32, 10)), proof.operation().aesKey());
        assertThrows(UnsupportedOperationException.class, () -> proof.members().clear());
        var archival = new SegmentClassicHandoff.Proof(proof.operation(), proof.members(),
                new SegmentClassicHandoff.Terminal(12, Instant.ofEpochSecond(-1), digest(11)),
                proof.unixIsolationReceipt(), proof.retirement());
        byte[] signed = resign(SegmentClassicHandoff.envelope(archival, new byte[64]));
        assertEquals(archival, SegmentClassicHandoff.verify(signed, digest(9), publicKey()));
    }

    @Test
    public void completeMaximumPhysicalListRemainsBoundedAndUidEncodingIsUnsigned() throws Exception {
        var members = new ArrayList<SegmentClassicHandoff.Member>();
        int inode = 1;
        for (int[] role : List.of(new int[]{1, 192}, new int[]{2, 1}, new int[]{3, 64})) {
            for (int member = 0; member < role[1]; member++) {
                members.add(new SegmentClassicHandoff.Member(new SegmentClassicHandoff.Physical(role[0], member,
                        Long.MIN_VALUE, inode++), 0xffff_ffffL, 0xffff_ffffL, 0700));
            }
        }
        var proof = proof(operation(), members);
        byte[] signed = resign(SegmentClassicHandoff.envelope(proof, new byte[64]));
        assertTrue(signed.length <= SegmentClassicHandoff.MAX_BYTES);
        var parsed = SegmentClassicHandoff.verify(signed, digest(9), publicKey());
        assertEquals(257, parsed.members().size());
        assertEquals(0xffff_ffffL, parsed.members().get(0).uid());
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentClassicHandoff.Member(members.get(0).physical(), 0x1_0000_0000L, 1, 0700));
    }
}

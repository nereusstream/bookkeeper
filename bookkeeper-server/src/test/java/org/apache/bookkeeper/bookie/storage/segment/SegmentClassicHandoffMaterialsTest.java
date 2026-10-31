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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Signed I/O fixtures only. No actual old-service issuer, live metadata/Unix isolation or adoptable session. */
public class SegmentClassicHandoffMaterialsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private long uid;
    private long oldUid;
    private KeyPair signing;
    private SegmentArenaManifest manifest;
    private BookieRouteBinding.Observation nativeScope;
    private SegmentClassicHandoffMaterials.References references;
    private SegmentClassicHandoffMaterials.Expected expected;
    private Path guardPath;
    private Path intentPath;

    private static byte[] filled(int length, int value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static SegmentClassicHandoff.Digest digest(int value) {
        return new SegmentClassicHandoff.Digest(filled(32, value));
    }

    private static void privateFile(Path path, byte[] value) throws IOException {
        Files.write(path, value);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
    }

    private static Path directory(Path path) throws IOException {
        return Files.createDirectory(path, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwx------")));
    }

    @Before
    public void materialFiles() throws Exception {
        uid = new UnixSystem().getUid();
        oldUid = uid == 1001 ? 1003 : 1001;
        signing = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Path root = temporary.getRoot().toPath().toRealPath();
        Path journal = directory(root.resolve("classic-0"));
        Path ledger = directory(root.resolve("classic-1"));
        Path control = directory(root.resolve("control"));
        Path arena = directory(root.resolve("arena"));
        manifest = new SegmentArenaManifest(new StorageIncarnation(filled(16, 1)), BookieId.parse("materials:3181"),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 11), arena, 1, 16384)),
                control, List.of(0), List.of(
                        new SegmentArenaManifest.ClassicRoot(journal, Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(ledger, Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                SegmentArenaManifest.ClassicRole.INDEX))));
        // Only local guard files are needed for the file reader's alias exclusion, not a formatted service graph.
        for (var member : manifest.classicRoots()) {
            Path current = directory(member.directory().resolve("current"));
            privateFile(current.resolve(SegmentClassicRootBindings.LOCK_NAME), new byte[0]);
        }
        privateFile(control.resolve("bookie-control.lock"), new byte[0]);
        privateFile(arena.resolve("isolated.lock"), new byte[0]);
        privateFile(arena.resolve("arena-control.lock"), new byte[0]);
        var binding = new BookieRouteBinding("material-cluster", 100, manifest.bookieId(), manifest.incarnation(),
                new UUID(0, 2), manifest.identity(), control.toString(),
                new BookieRouteBinding.SourceCookie(102, 3, filled(32, 3)), BookieRouteBinding.Phase.PREPARED,
                Optional.empty());
        nativeScope = new BookieRouteBinding.Observation(binding, new UUID(0, 4), 104, 104, 0);
        var requirements = new SegmentClassicHandoff.Requirements(
                new ZKProfileStartupRequirementsStore.SourceIdentity("/commissioned/materials", new UUID(0, 5),
                        106, 106, 0, filled(32, 6)), 1, 2, 3, 4, 1, 1);
        expected = new SegmentClassicHandoffMaterials.Expected("deployment", digest(8), digest(9), signing.getPublic(),
                nativeScope, requirements, oldUid, uid, uid);
        Path materials = directory(root.resolve("materials"));
        references = new SegmentClassicHandoffMaterials.References(materials, materials.resolve("proof"),
                materials.resolve("policy"), materials.resolve("key"));
        String stem = SegmentClassicHandoffMaterials.scopeName(binding);
        guardPath = materials.resolve(stem + ".lock");
        intentPath = materials.resolve(stem + ".retirement");
        privateFile(guardPath, new byte[0]);
        var policy = new SegmentClassicHandoff.Policy(binding.bindingId(), binding.bookieId(), binding.incarnation(),
                binding.manifest(), uid);
        var operation = new SegmentClassicHandoff.Operation("deployment", digest(8), digest(9), nativeScope,
                requirements, new SegmentClassicHandoff.Episode(new UUID(0, 7), 99, Instant.ofEpochSecond(1000),
                        108, 109), oldUid, uid, SegmentClassicHandoff.keyIdentity(filled(32, 10)),
                SegmentClassicHandoff.digest(SegmentClassicHandoff.encodePolicy(policy)));
        var members = new ArrayList<SegmentClassicHandoff.Member>();
        int member = 0;
        for (Path path : List.of(journal, ledger, control, arena)) {
            int kind = member < 2 ? 1 : member == 2 ? 2 : 3;
            int index = member < 2 ? member : 0;
            members.add(new SegmentClassicHandoff.Member(new SegmentClassicHandoff.Physical(kind, index,
                    ((Number) Files.getAttribute(path, "unix:dev")).longValue(),
                    ((Number) Files.getAttribute(path, "unix:ino")).longValue()), uid,
                    new UnixSystem().getGid(), 0700));
            member++;
        }
        var retirement = new SegmentClassicHandoff.Retirement(operation,
                members.stream().map(SegmentClassicHandoff.Member::physical).toList());
        byte[] intent = SegmentClassicHandoff.encodeRetirement(retirement);
        var proof = new SegmentClassicHandoff.Proof(operation, members,
                new SegmentClassicHandoff.Terminal(12, Instant.ofEpochSecond(1001), digest(11)), digest(12),
                SegmentClassicHandoff.digest(intent));
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(signing.getPrivate());
        signer.update(SegmentClassicHandoff.signingMessage(proof));
        privateFile(references.proof(), SegmentClassicHandoff.envelope(proof, signer.sign()));
        privateFile(references.policy(), SegmentClassicHandoff.encodePolicy(policy));
        privateFile(references.aesKey(), filled(32, 10));
        privateFile(intentPath, intent);
    }

    private SegmentClassicHandoffMaterials owner() {
        return new SegmentClassicHandoffMaterials(references, expected, manifest);
    }

    private void rejected() throws Exception {
        var owner = owner();
        assertThrows(IOException.class, owner::initialize);
        assertThrows(IOException.class, owner::parsed);
        var closed = owner.closeAndDrain();
        assertTrue(closed.drained());
        assertNotNull(closed.initializationFailure());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void ownedInputsRetainSupervisorUntilCloseAndAllowTheSameOriginalPreparedProjection() throws Exception {
        byte[] proof = Files.readAllBytes(references.proof());
        byte[] key = Files.readAllBytes(references.aesKey());
        var owner = owner();
        owner.initialize();
        assertArrayEquals(key, owner.parsed().key().getEncoded());
        assertEquals(manifest.identity(), owner.parsed().proof().operation().prepared().binding().manifest());
        probe("HELD");
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class, owner::parsed);
        probe("AVAILABLE");
        var enabled = nativeScope.binding().enabled(
                new BookieRouteBinding.BootstrapSeal(new UUID(0, 20), 0, -1));
        expected = new SegmentClassicHandoffMaterials.Expected(expected.deployment(), expected.programConfiguration(),
                expected.signerKeyId(), expected.trustedSigner(), new BookieRouteBinding.Observation(enabled,
                        nativeScope.birthId(), nativeScope.birthZxid(), 105, 1),
                expected.requirements(), oldUid, uid, uid);
        owner = owner();
        owner.initialize();
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(proof, Files.readAllBytes(references.proof()));
        assertArrayEquals(key, Files.readAllBytes(references.aesKey()));
    }

    @Test
    public void supervisorAndIntentNamesCannotChangeWithAttemptOrBinding() throws Exception {
        var b = nativeScope.binding();
        var other = new BookieRouteBinding(b.clusterId(), b.clusterBirthZxid(), b.bookieId(), b.incarnation(),
                new UUID(0, 99), b.manifest(), b.controlDirectory(), b.sourceCookie(), b.phase(), b.seal());
        assertEquals(SegmentClassicHandoffMaterials.scopeName(b), SegmentClassicHandoffMaterials.scopeName(other));
        assertNotEquals(SegmentClassicHandoffMaterials.scopeName(b), SegmentClassicHandoffMaterials.scopeName(
                new BookieRouteBinding(b.clusterId(), b.clusterBirthZxid() + 1, b.bookieId(), b.incarnation(),
                        b.bindingId(), b.manifest(), b.controlDirectory(), b.sourceCookie(), b.phase(), b.seal())));
        Path moved = references.directory().resolveSibling("moved-materials");
        Files.move(references.directory(), moved);
        var owner = owner(); // The constructor still does not touch the missing path.
        assertFalse(Files.exists(references.directory()));
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertFalse(Files.exists(references.directory()));
    }

    @Test
    public void missingGuardOrIntentWrongSizeAndBadPrivateModeNeverCreateOrRepairInputs() throws Exception {
        for (Path path : List.of(guardPath, intentPath, references.aesKey())) {
            Path held = path.resolveSibling("held");
            Files.move(path, held);
            rejected();
            assertFalse(Files.exists(path));
            Files.move(held, path);
        }
        privateFile(references.aesKey(), filled(31, 10));
        rejected();
        privateFile(references.aesKey(), filled(32, 10));
        Files.setPosixFilePermissions(references.aesKey(), PosixFilePermissions.fromString("rw-r--r--"));
        rejected();
        Files.setPosixFilePermissions(references.aesKey(), PosixFilePermissions.fromString("rw-------"));
        privateFile(references.proof(), new byte[65537]);
        rejected();
        assertEquals(65537, Files.size(references.proof()));
    }

    @Test
    public void aliasesAndWritableParentChainsAreRejectedBeforeDataPublication() throws Exception {
        Path key = references.aesKey();
        Path saved = key.resolveSibling("saved-key");
        Files.move(key, saved);
        Files.createSymbolicLink(key, saved);
        rejected();
        Files.delete(key);
        Files.createLink(key, saved);
        rejected();
        Files.delete(key);
        Files.move(saved, key);
        Files.setPosixFilePermissions(references.directory(), PosixFilePermissions.fromString("rwxrwxrwx"));
        rejected();
        Files.setPosixFilePermissions(references.directory(), PosixFilePermissions.fromString("rwx------"));
        Path alias = references.directory().resolveSibling("alias");
        Files.createSymbolicLink(alias, references.directory());
        references = new SegmentClassicHandoffMaterials.References(alias, alias.resolve("proof"),
                alias.resolve("policy"), alias.resolve("key"));
        rejected();
    }

    @Test
    public void wrongActualUidChangedNativeBirthOrProgramAndWrongKeyCannotPublishMaterials()
            throws Exception {
        var original = expected;
        long anotherUid = uid == 0 ? 1 : 0;
        expected = new SegmentClassicHandoffMaterials.Expected(
                original.deployment(), original.programConfiguration(),
                original.signerKeyId(), original.trustedSigner(), nativeScope, original.requirements(),
                oldUid, anotherUid, uid);
        var wrongUid = owner();
        var wrongUidFailure = assertThrows(IOException.class, wrongUid::initialize);
        assertTrue(wrongUidFailure.getMessage().contains("actual target UID"));
        assertTrue(wrongUid.closeAndDrain().drained());
        probe("AVAILABLE");
        expected = new SegmentClassicHandoffMaterials.Expected(expected.deployment(), digest(77),
                original.signerKeyId(), original.trustedSigner(), nativeScope, original.requirements(),
                oldUid, uid, uid);
        rejected();
        expected = new SegmentClassicHandoffMaterials.Expected(
                original.deployment(), original.programConfiguration(),
                original.signerKeyId(), original.trustedSigner(), new BookieRouteBinding.Observation(
                        nativeScope.binding(), new UUID(0, 77), 107, 107, 0),
                original.requirements(), oldUid, uid, uid);
        rejected();
        expected = original;
        privateFile(references.aesKey(), filled(32, 99));
        rejected();
        privateFile(references.aesKey(), filled(32, 10));
        byte[] intent = Files.readAllBytes(intentPath);
        intent[intent.length - 1] ^= 1;
        privateFile(intentPath, intent);
        rejected();
    }

    @Test
    public void replacementAfterOpenIsDetectedAndTheOldReadBufferIsErased() throws Exception {
        var owner = owner();
        var observed = new AtomicReference<byte[]>();
        try (var reads = mockStatic(SegmentFileIo.class, invocation -> {
            if (invocation.getMethod().getName().equals("readFully")) {
                ByteBuffer buffer = invocation.getArgument(1);
                if (buffer.capacity() == 32) {
                    observed.set(buffer.array());
                }
            }
            return invocation.callRealMethod();
        }); var opens = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
                Path saved = references.aesKey().resolveSibling("old-key");
                Files.move(references.aesKey(), saved);
                privateFile(references.aesKey(), filled(32, 10));
            }
            return result;
        })) {
            assertThrows(IOException.class, owner::initialize);
        }
        assertArrayEquals(new byte[32], observed.get());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void failedNativeCloseCannotExposeKeyOrReleaseSupervisorAndKeepsTheFirstFailure() throws Exception {
        var owner = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        var buffer = new AtomicReference<byte[]>();
        IOException failure = new IOException("key reader native close");
        try (var reads = mockStatic(SegmentFileIo.class, invocation -> {
            if (invocation.getMethod().getName().equals("readFully")) {
                ByteBuffer value = invocation.getArgument(1);
                if (value.capacity() == 32) {
                    buffer.set(value.array());
                }
            }
            return invocation.callRealMethod();
        }); var opens = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) result);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return result;
        })) {
            assertSame(failure, assertThrows(IOException.class, owner::initialize));
        }
        try {
            assertArrayEquals(new byte[32], buffer.get());
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThrows(IOException.class, owner::parsed);
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.initializationFailure());
                assertSame(failure, result.files().closeFailure());
                assertEquals(1, captured.get().closes.get());
                assertTrue(captured.get().actual.isOpen());
                probe("HELD");
            }
        } finally {
            captured.get().actual.close();
            guard(owner).close(); // Actual fixture cleanup; original failed file result remains unresolved.
        }
        assertFalse(owner.closeAndDrain().drained());
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void actualMaterialOpenCannotDrainItselfAndExternalCloseWaitsForItsReturn() throws Exception {
        var owner = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var initializer = new FutureTask<Void>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                        invocation.getArgument(0))) {
                    assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                }
                return invocation.callRealMethod();
            })) {
                owner.initialize();
                return null;
            }
        });
        var drained = new FutureTask<>(owner::closeAndDrain);
        var reader = new Thread(initializer);
        var closer = new Thread(drained);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(drained.isDone());
            probe("HELD");
            release.countDown();
            initializer.get(5, TimeUnit.SECONDS);
            assertTrue(drained.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
            assertTrue(owner.closeAndDrain().drained());
        }
        assertThrows(IOException.class, owner::parsed);
    }

    private boolean isKeyOpen(String name, Class<?>[] parameters, Object path) {
        return name.equals("open") && parameters.length == 2 && parameters[1] == OpenOption[].class
                && references.aesKey().equals(path);
    }

    private static SegmentDataFileLock guard(SegmentClassicHandoffMaterials owner) throws Exception {
        var field = SegmentClassicHandoffMaterials.class.getDeclaredField("guard");
        field.setAccessible(true);
        return (SegmentDataFileLock) field.get(owner);
    }

    private void probe(String expected) throws Exception {
        var process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                SegmentOwnedBookieControlLogTest.GuardProbe.class.getName(),
                guardPath.toString()).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(
                    process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(output, 0, process.exitValue());
            assertEquals(expected, output.trim());
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }
}

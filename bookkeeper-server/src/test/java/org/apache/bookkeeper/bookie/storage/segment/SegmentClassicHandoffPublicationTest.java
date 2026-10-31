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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Signed file-I/O fixtures only. No real old service, metadata/Unix qualification or adoptable session. */
public class SegmentClassicHandoffPublicationTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static byte[] digestBytes(int value) {
        byte[] bytes = new byte[32];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] sign(SegmentClassicHandoff.Proof proof) throws Exception {
        byte[] encoded = HexFormat.of().parseHex("302e020100300506032b657004220420"
                + "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
        var signer = Signature.getInstance("Ed25519");
        signer.initSign(KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(encoded)));
        signer.update(SegmentClassicHandoff.signingMessage(proof));
        return SegmentClassicHandoff.envelope(proof, signer.sign());
    }

    private static PublicKey publicKey() throws Exception {
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(HexFormat.of().parseHex(
                "302a300506032b6570032100d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")));
    }

    private static final class Fixture implements AutoCloseable {
        private final SegmentClassicHandoffMaterials.References references;
        private final SegmentClassicHandoff.SupervisorScope scope;
        private final SegmentClassicHandoffPublication.Access access;
        private final SegmentClassicHandoff.Retirement retirement;
        private final SegmentClassicHandoff.Proof proof;
        private final byte[] packet;
        private final Path guardPath;
        private final Path intentPath;
        private final SegmentDataFileLock guard;

        private Fixture(Path root, boolean provision, boolean acquire) throws Exception {
            this(root, provision, acquire, new UnixSystem().getUid());
        }

        private Fixture(Path root, boolean provision, boolean acquire, long uid) throws Exception {
            root = root.toRealPath();
            long issuerUid = new UnixSystem().getUid();
            long oldUid = uid == 1001 ? 1003 : 1001;
            SegmentClassicHandoff.Retirement vector;
            try (var input = getClass().getResourceAsStream(
                    "/unified-wal-segment/classic-handoff-retirement-v1.hex")) {
                assertNotNull(input);
                vector = SegmentClassicHandoff.decodeRetirement(HexFormat.of().parseHex(
                        new String(input.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s", "")));
            }
            var base = vector.operation();
            var nativeScope = base.prepared().binding();
            var policy = new SegmentClassicHandoff.Policy(nativeScope.bindingId(), nativeScope.bookieId(),
                    nativeScope.incarnation(), nativeScope.manifest(), uid);
            var operation = new SegmentClassicHandoff.Operation(
                    base.deployment(), base.programConfiguration(),
                    base.signerKeyId(), base.prepared(), base.requirements(), base.episode(),
                    oldUid, uid, base.aesKey(),
                    SegmentClassicHandoff.digest(SegmentClassicHandoff.encodePolicy(policy)));
            retirement = new SegmentClassicHandoff.Retirement(operation, vector.members());
            proof = new SegmentClassicHandoff.Proof(operation, vector.members().stream().map(member ->
                    new SegmentClassicHandoff.Member(member, uid, new UnixSystem().getGid(), 0700)).toList(),
                    new SegmentClassicHandoff.Terminal(12, Instant.ofEpochSecond(1001),
                            new SegmentClassicHandoff.Digest(digestBytes(11))),
                    new SegmentClassicHandoff.Digest(digestBytes(12)),
                    SegmentClassicHandoff.digest(SegmentClassicHandoff.encodeRetirement(retirement)));
            packet = sign(proof);
            Path directory = root.resolve("materials");
            if (provision) {
                Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
            }
            references = new SegmentClassicHandoffMaterials.References(directory, directory.resolve("handoff.proof"),
                    directory.resolve("policy"), directory.resolve("key"));
            scope = new SegmentClassicHandoff.SupervisorScope(nativeScope.clusterId(), nativeScope.clusterBirthZxid(),
                    nativeScope.bookieId());
            access = new SegmentClassicHandoffPublication.Access(issuerUid, oldUid, uid, uid);
            String name = SegmentClassicHandoffMaterials.scopeName(scope);
            guardPath = directory.resolve(name + ".lock");
            intentPath = directory.resolve(name + ".retirement");
            if (provision) {
                Files.createFile(guardPath, PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-------")));
            }
            guard = SegmentDataFileLock.existingControlOwner(guardPath);
            if (acquire) {
                guard.initialize();
            }
        }

        private SegmentClassicHandoffPublication owner(SegmentFileIo io) throws Exception {
            return new SegmentClassicHandoffPublication(references, scope, access, retirement.operation().signerKeyId(),
                    publicKey(), guard, io);
        }

        @Override
        public void close() throws Exception {
            guard.close();
        }
    }

    private Fixture fixture() throws Exception {
        return new Fixture(temporary.newFolder().toPath(), true, true);
    }

    @Test
    public void originalOwnerPublishesExactPacketsAndSuccessorKeepsTheBorrowedGuard() throws Exception {
        try (var f = fixture()) {
            var owner = f.owner(new SegmentFileIo());
            owner.initialize();
            assertFalse(owner.state().retirementPresent());
            assertFalse(owner.state().proofPresent());
            assertEquals(f.intentPath, owner.retire(f.retirement));
            assertEquals(f.references.proof(), owner.publishProof(f.packet));
            Object inode = Files.getAttribute(f.references.proof(), "unix:ino");
            owner.retire(f.retirement);
            owner.publishProof(f.packet.clone());
            assertEquals(inode, Files.getAttribute(f.references.proof(), "unix:ino"));
            assertArrayEquals(f.packet, Files.readAllBytes(f.references.proof()));
            assertTrue(owner.state().proofDurable());
            assertTrue(owner.closeAndDrain().drained());
            probe(f.guardPath, "HELD");
            var successor = f.owner(new SegmentFileIo());
            successor.initialize();
            assertEquals(f.proof, successor.recoverProof(f.retirement));
            assertTrue(successor.state().proofDurable());
            assertTrue(successor.closeAndDrain().drained());
            assertEquals(1, ((Number) Files.getAttribute(f.references.proof(), "unix:nlink")).intValue());
            probe(f.guardPath, "HELD");
        }
    }

    @Test
    public void successorCannotPublishOrPromoteEvenAnOriginalSignedOrphanCandidate() throws Exception {
        try (var f = fixture()) {
            var first = f.owner(new SegmentFileIo());
            first.initialize();
            first.retire(f.retirement);
            assertTrue(first.closeAndDrain().drained());
            Path orphan = f.references.directory().resolve(".handoff-candidate-orphan");
            privateFile(orphan, f.packet);
            var successor = f.owner(new SegmentFileIo());
            successor.initialize();
            successor.retire(f.retirement);
            assertThrows(IOException.class, () -> successor.publishProof(f.packet));
            assertThrows(IOException.class, () -> successor.recoverProof(f.retirement));
            assertArrayEquals(f.packet, Files.readAllBytes(orphan));
            assertFalse(Files.exists(f.references.proof()));
            assertTrue(successor.closeAndDrain().drained());
        }
    }

    @Test
    public void conflictingExistingIntentOrProofIsNeverReplacedOrDeleted() throws Exception {
        try (var f = fixture()) {
            privateFile(f.intentPath, new byte[]{7});
            var owner = f.owner(new SegmentFileIo());
            owner.initialize();
            assertThrows(IOException.class, () -> owner.retire(f.retirement));
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(f.intentPath));
            assertTrue(owner.closeAndDrain().drained());
        }
        try (var f = fixture()) {
            var owner = f.owner(new SegmentFileIo());
            owner.initialize();
            owner.retire(f.retirement);
            privateFile(f.references.proof(), new byte[]{9});
            assertThrows(IOException.class, () -> owner.publishProof(f.packet));
            assertArrayEquals(new byte[]{9}, Files.readAllBytes(f.references.proof()));
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void forceFailuresKeepActualFinalStateAndOnlyDirectorySyncCanRetry() throws Exception {
        for (int failForce : new int[]{1, 2, 3, 4}) {
            try (var f = fixture()) {
                var owner = f.owner(new SegmentFileIo(Integer.MAX_VALUE, -1, failForce, false));
                owner.initialize();
                IOException failure;
                if (failForce <= 2) {
                    failure = assertThrows(IOException.class, () -> owner.retire(f.retirement));
                    assertEquals(failForce == 2, owner.state().retirementPresent());
                    assertFalse(owner.state().retirementDurable());
                    if (failForce == 2) {
                        owner.retire(f.retirement);
                    } else {
                        assertThrows(IOException.class, () -> owner.retire(f.retirement));
                    }
                } else {
                    owner.retire(f.retirement);
                    failure = assertThrows(IOException.class, () -> owner.publishProof(f.packet));
                    assertEquals(failForce == 4, owner.state().proofPresent());
                    assertFalse(owner.state().proofDurable());
                    if (failForce == 4) {
                        owner.publishProof(f.packet);
                    } else {
                        assertThrows(IOException.class, () -> owner.publishProof(f.packet));
                    }
                }
                assertSame(failure, owner.state().firstFailure());
                assertEquals(failForce != 1, owner.state().retirementDurable());
                assertTrue(owner.closeAndDrain().drained());
                assertEquals(failForce != 1, Files.exists(f.intentPath));
                if (failForce == 4) {
                    assertArrayEquals(f.packet, Files.readAllBytes(f.references.proof()));
                }
            }
        }
    }

    @Test
    public void unknownRenameReplyCanOnlyResolveTheOriginalFinalWithoutAnotherRename() throws Exception {
        try (var f = fixture()) {
            var real = new NativeIOImpl();
            var once = new AtomicBoolean();
            var calls = new AtomicInteger();
            var failure = new NativeIOException("rename reply outcome unknown");
            try (var construction = mockConstruction(NativeIOImpl.class, (mock, context) -> {
                when(mock.renameNoReplace(anyString(), anyString())).thenAnswer(invocation -> {
                    calls.incrementAndGet();
                    int result = real.renameNoReplace(invocation.getArgument(0), invocation.getArgument(1));
                    if (once.compareAndSet(false, true)) {
                        throw failure;
                    }
                    return result;
                });
            })) {
                var owner = f.owner(new SegmentFileIo());
                owner.initialize();
                assertSame(failure, assertThrows(IOException.class, () -> owner.retire(f.retirement)));
                assertTrue(Files.exists(f.intentPath));
                owner.retire(f.retirement);
                assertEquals(1, calls.get());
                assertSame(failure, owner.state().firstFailure());
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void firstCandidateOrDirectoryCloseFailureCannotReportSuccessOrBeRetried() throws Exception {
        for (boolean directory : new boolean[]{false, true}) {
            try (var f = fixture()) {
                var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
                var failure = new IOException("original native close");
                var owner = f.owner(new SegmentFileIo());
                owner.initialize();
                try (var factory = mockStatic(FileChannel.class, invocation -> {
                    Object result = invocation.callRealMethod();
                    if (invocation.getMethod().getName().equals("open")) {
                        Path path = invocation.getArgument(0);
                        boolean selected = directory ? path.equals(f.references.directory())
                                : path.getFileName().toString().startsWith(".handoff-candidate-");
                        if (selected && captured.get() == null) {
                            var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) result);
                            fault.closeFailure = failure;
                            captured.set(fault);
                            return fault;
                        }
                    }
                    return result;
                })) {
                    assertSame(failure, assertThrows(IOException.class, () -> owner.retire(f.retirement)));
                }
                try {
                    for (int attempt = 0; attempt < 2; attempt++) {
                        var closed = owner.closeAndDrain();
                        assertFalse(closed.drained());
                        assertSame(failure, closed.firstFailure());
                        assertSame(failure, closed.files().closeFailure());
                        assertEquals(1, captured.get().closes.get());
                        assertTrue(captured.get().actual.isOpen());
                        probe(f.guardPath, "HELD");
                    }
                    assertEquals(directory, Files.exists(f.intentPath));
                } finally {
                    // Fixture cleanup only; original first-close result stays unresolved.
                    captured.get().actual.close();
                }
                assertFalse(owner.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void actualIoCannotDrainItselfAndCallerTimeoutKeepsTheBorrowedGuard() throws Exception {
        try (var f = fixture()) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var reference = new AtomicReference<SegmentClassicHandoffPublication>();
            var once = new AtomicBoolean();
            var io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (once.compareAndSet(false, true)) {
                    assertThrows(IllegalStateException.class, () -> reference.get().closeAndDrain());
                    entered.countDown();
                    try {
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            });
            var owner = f.owner(io);
            reference.set(owner);
            owner.initialize();
            var publish = new FutureTask<>(() -> owner.retire(f.retirement));
            var close = new FutureTask<>(owner::closeAndDrain);
            var writer = new Thread(publish);
            var closer = new Thread(close);
            try {
                writer.start();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closer.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
                assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
                assertFalse(close.isDone());
                probe(f.guardPath, "HELD");
                release.countDown();
                publish.get(5, TimeUnit.SECONDS);
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            } finally {
                release.countDown();
                writer.join(5000);
                if (closer.getState() != Thread.State.NEW) {
                    closer.join(5000);
                }
                assertFalse(writer.isAlive());
                assertFalse(closer.isAlive());
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void missingOrWritableDirectoryWrongGuardAndInvalidSignatureFailWithoutRepair() throws Exception {
        try (var f = fixture()) {
            var owner = f.owner(new SegmentFileIo());
            Path moved = f.references.directory().resolveSibling("moved");
            Files.move(f.references.directory(), moved);
            assertThrows(IOException.class, owner::initialize);
            assertFalse(Files.exists(f.references.directory()));
            assertTrue(owner.closeAndDrain().drained());
            Files.move(moved, f.references.directory());
        }
        try (var f = fixture()) {
            Files.setPosixFilePermissions(f.references.directory(), PosixFilePermissions.fromString("rwxrwxrwx"));
            var owner = f.owner(new SegmentFileIo());
            assertThrows(IOException.class, owner::initialize);
            assertTrue(owner.closeAndDrain().drained());
            Files.setPosixFilePermissions(f.references.directory(), PosixFilePermissions.fromString("rwx------"));
        }
        try (var f = fixture()) {
            var other = new SegmentClassicHandoff.SupervisorScope(f.scope.clusterId(),
                    f.scope.clusterBirthZxid() + 1, f.scope.bookieId());
            var owner = new SegmentClassicHandoffPublication(f.references, other, f.access,
                    f.retirement.operation().signerKeyId(), publicKey(), f.guard, new SegmentFileIo());
            assertThrows(IOException.class, owner::initialize);
            assertTrue(owner.closeAndDrain().drained());
        }
        try (var f = fixture()) {
            var owner = f.owner(new SegmentFileIo());
            owner.initialize();
            owner.retire(f.retirement);
            byte[] invalid = f.packet.clone();
            invalid[invalid.length - 1] ^= 1;
            assertThrows(IOException.class, () -> owner.publishProof(invalid));
            assertFalse(Files.exists(f.references.proof()));
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 45000)
    public void processCutsNeverPromoteSignedTempAndExistingFinalCanCompleteRecoverySync() throws Exception {
        for (int force : new int[]{3, 4}) {
            Path root = temporary.newFolder().toPath().toRealPath();
            try (var original = new Fixture(root, true, false)) {
                var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                        "-Dbookkeeper.native.io.library.path="
                                + System.getProperty("bookkeeper.native.io.library.path"),
                        "-cp", System.getProperty("java.class.path"), PublicationProcess.class.getName(),
                        root.toString(), Integer.toString(force));
                var process = new ProcessBuilder(command).redirectErrorStream(true).start();
                try {
                    ready(process);
                    process.destroyForcibly();
                    assertTrue(process.waitFor(10, TimeUnit.SECONDS));
                } finally {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                        assertTrue(process.waitFor(5, TimeUnit.SECONDS));
                    }
                    process.getInputStream().close();
                    process.getOutputStream().close();
                    process.getErrorStream().close();
                }
                try (var resumed = new Fixture(root, false, true)) {
                    var owner = resumed.owner(new SegmentFileIo());
                    owner.initialize();
                    if (force == 3) {
                        assertThrows(IOException.class, () -> owner.recoverProof(resumed.retirement));
                        List<Path> candidates;
                        try (var list = Files.list(resumed.references.directory())) {
                            candidates = list.filter(p -> p.getFileName().toString()
                                    .startsWith(".handoff-candidate-")).toList();
                        }
                        assertEquals(1, candidates.size());
                        byte[] bytes = Files.readAllBytes(candidates.get(0));
                        assertEquals(resumed.proof, SegmentClassicHandoff.verify(bytes,
                                resumed.retirement.operation().signerKeyId(), publicKey()));
                        assertFalse(Files.exists(resumed.references.proof()));
                        owner.retire(resumed.retirement);
                        assertThrows(IOException.class, () -> owner.publishProof(bytes));
                        assertArrayEquals(bytes, Files.readAllBytes(candidates.get(0)));
                    } else {
                        assertEquals(resumed.proof, owner.recoverProof(resumed.retirement));
                        assertTrue(owner.state().proofDurable());
                        assertArrayEquals(original.packet, Files.readAllBytes(resumed.references.proof()));
                    }
                    assertTrue(owner.closeAndDrain().drained());
                }
            }
        }
    }

    private static void privateFile(Path path, byte[] value) throws IOException {
        Files.write(path, value);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
    }

    private static void ready(Process process) throws Exception {
        byte[] marker = "READY\n".getBytes(StandardCharsets.US_ASCII);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (process.isAlive() && process.getInputStream().available() < marker.length
                && deadline - System.nanoTime() > 0) {
            Thread.sleep(10);
        }
        assertTrue("Publication child did not reach the actual fault point",
                process.getInputStream().available() >= marker.length);
        assertArrayEquals(marker, process.getInputStream().readNBytes(marker.length));
    }

    private static void probe(Path path, String expected) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                SegmentOwnedBookieControlLogTest.GuardProbe.class.getName(),
                path.toString()).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            assertEquals(output, 0, process.exitValue());
            assertEquals(expected, output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    /** Explicit three-process credential probe; never invoked by the normal test suite. */
    public static final class UidPublicationProcess {
        public static void main(String[] args) throws Exception {
            long target = Long.parseLong(args[2]);
            long actual = new UnixSystem().getUid();
            boolean prepare = args[0].equals("prepare");
            try (var f = new Fixture(Path.of(args[1]), prepare, !prepare, target)) {
                if (!prepare) {
                    var owner = f.owner(new SegmentFileIo());
                    owner.initialize();
                    if (args[0].equals("publish")) {
                        owner.retire(f.retirement);
                        owner.publishProof(f.packet);
                    } else {
                        var decoded = owner.recoverProof(f.retirement);
                        assertEquals(f.retirement.operation(), decoded.operation());
                    }
                    assertTrue(owner.closeAndDrain().drained());
                    assertEquals(target, ((Number) Files.getAttribute(f.intentPath, "unix:uid")).longValue());
                    assertEquals(target, ((Number) Files.getAttribute(f.references.proof(), "unix:uid")).longValue());
                    assertTrue(Files.readAllBytes(f.references.proof()).length > 112);
                }
            }
            System.out.println(args[0] + " actual=" + actual + " target=" + target);
        }
    }

    public static final class PublicationProcess {
        public static void main(String[] args) throws Exception {
            int selected = Integer.parseInt(args[1]);
            var count = new AtomicInteger();
            try (var f = new Fixture(Path.of(args[0]), false, true)) {
                var io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (count.incrementAndGet() == selected) {
                        System.out.print("READY\n");
                        System.out.flush();
                        try {
                            if (System.in.read() < 0) {
                                throw new IOException("Controlled process cut channel closed");
                            }
                        } catch (IOException failure) {
                            throw new IllegalStateException(failure);
                        }
                    }
                });
                var owner = f.owner(io);
                owner.initialize();
                owner.retire(f.retirement);
                owner.publishProof(f.packet);
                throw new AssertionError("Selected process cut was not held");
            }
        }
    }
}

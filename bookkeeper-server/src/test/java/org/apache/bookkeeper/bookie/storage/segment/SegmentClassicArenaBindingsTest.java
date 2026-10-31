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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Local Arena input condition only; no external commissioning or complete Classic bootstrap is asserted. */
public class SegmentClassicArenaBindingsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private SegmentArenaManifest manifest;
    private final SecretKeySpec key = new SecretKeySpec(new byte[32], "AES");
    private final ProfileDescriptorValidator validator = new ProfileDescriptorValidator((a, b) -> false,
            (a, b) -> false);

    @Before
    public void formattedMembers() throws Exception {
        Path root = temporary.getRoot().toPath().toRealPath();
        Path journal = Files.createDirectory(root.resolve("classic-0-journal"));
        Path ledger = Files.createDirectory(root.resolve("classic-1-ledger-index"));
        byte[] incarnation = new byte[16];
        incarnation[0] = 1;
        manifest = new SegmentArenaManifest(new StorageIncarnation(incarnation), BookieId.parse("arenas:3181"),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 1), root.resolve("arena-a"), 1,
                                4096 + SegmentArenaControlLog.BLOCK_BYTES),
                        new SegmentArenaManifest.Arena(new UUID(0, 2), root.resolve("arena-b"), 2, 16384)),
                root.resolve("control"), List.of(0, 1, 1, 0), List.of(
                        new SegmentArenaManifest.ClassicRoot(journal, Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(ledger, Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                SegmentArenaManifest.ClassicRole.INDEX))));
        // Local file fixture only: these guard files are not a native commissioning or old-writer proof.
        for (var rootMember : manifest.classicRoots()) {
            Path current = Files.createDirectory(rootMember.directory().resolve("current"));
            Files.createFile(current.resolve(SegmentClassicRootBindings.LOCK_NAME));
        }
        try (var ignored = SegmentBookieControlLog.createManifestControl(control(), manifest, manifest.identity(),
                65536, key, validator, new SegmentFileIo(), binding())) {
            assertFalse(ignored.bootstrapComplete());
        }
        for (int member = 0; member < 2; member++) {
            try (var files = IsolatedSegmentFiles.create(config(member), identity(member));
                    var log = arena(member)) {
                assertEquals(-1, log.appliedThrough());
            }
        }
    }

    private SegmentBookieControlLog.RouteBinding binding() {
        return new SegmentBookieControlLog.RouteBinding(new UUID(0, 9), manifest.identity());
    }

    private IsolatedSegmentConfig control() {
        return new IsolatedSegmentConfig(manifest.controlDirectory(), 1, 4, 1, 16384, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private IsolatedSegmentConfig config(int member) {
        var arena = manifest.arenas().get(member);
        return new IsolatedSegmentConfig(arena.directory(), 1, 4, arena.dataFiles(), arena.bytesPerFile(),
                4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private SegmentFileHeader identity(int member) {
        return SegmentFileHeader.forArena(manifest, member);
    }

    private SegmentArenaControlLog arena(int member) throws IOException {
        return SegmentArenaControlLog.open(config(member), identity(member), 65536);
    }

    private SegmentClassicArenaBindings owner() throws IOException {
        return new SegmentClassicArenaBindings(manifest, manifest.identity(), 65536);
    }

    private Map<String, String> snapshot() throws Exception {
        var result = new LinkedHashMap<String, String>();
        Path root = temporary.getRoot().toPath();
        try (var paths = Files.walk(root)) {
            for (Path file : paths.filter(Files::isRegularFile).sorted().toList()) {
                result.put(root.relativize(file).toString(), HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
            }
        }
        return result;
    }

    private void rejectWithoutMutation() throws Exception {
        var before = snapshot();
        var owner = owner();
        assertThrows(IOException.class, owner::initialize);
        assertFalse(owner.verified());
        var result = owner.closeAndDrain();
        assertNotNull(result.initializationFailure());
        assertTrue(result.drained());
        assertTrue(owner.closeAndDrain().drained());
        assertEquals(before, snapshot());
    }

    @Test
    public void completeUnusedSetRemainsExcludedUntilActualCloseAndCanBeVerifiedAgain() throws Exception {
        var before = snapshot();
        for (int repeat = 0; repeat < 2; repeat++) {
            var owner = owner();
            assertFalse(owner.verified());
            owner.initialize();
            assertTrue(owner.verified());
            assertThrows(IOException.class, owner::initialize);
            for (var arena : manifest.arenas()) {
                probe(arena.directory().resolve("isolated.lock"), "HELD");
                probe(arena.directory().resolve("arena-control.lock"), "HELD");
            }
            assertTrue(owner.closeAndDrain().drained());
            assertFalse(owner.verified());
            assertThrows(IOException.class, owner::initialize);
        }
        assertEquals(before, snapshot());
        assertFalse(Files.exists(config(0).directory().resolve("index")));
        probe(config(1).directory().resolve("isolated.lock"), "AVAILABLE");
    }

    @Test
    public void pureMaintenanceAndConditionFailedHistoryRemainEligibleAcrossRotationAndFallback() throws Exception {
        SegmentControlRotation.Root root;
        try (var log = arena(1)) {
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(new SegmentArenaControlLog.DataSession(new UUID(0, 21),
                            new Generation(2), 1, true))).get(0).status());
            log.checkpoint();
            root = log.rotate();
            log.rotate();
        }
        // The retained original log is mandatory even after selecting the newest rotation.
        var before = snapshot();
        var owner = owner();
        owner.initialize();
        assertTrue(owner.verified());
        assertTrue(owner.closeAndDrain().drained());
        assertEquals(before, snapshot());
        try (var log = arena(0)) {
            root = log.rotate();
        }
        Path checkpoint = SegmentControlCheckpoint.path(config(0), SegmentControlFormat.Owner.ARENA,
                root.second().slot());
        try (var channel = FileChannel.open(checkpoint, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[]{1}), 4224);
        }
        before = snapshot();
        owner = owner();
        owner.initialize();
        assertTrue(owner.closeAndDrain().drained());
        assertEquals(before, snapshot());
    }

    @Test
    public void missingMemberOrGuardIsNeverRecreatedAndConstructionPerformsNoLookup() throws Exception {
        Path root = config(1).directory();
        Path moved = root.resolveSibling("moved");
        Files.move(root, moved);
        var owner = owner();
        assertFalse(Files.exists(root));
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertFalse(Files.exists(root));
        Files.move(moved, root);
        for (String name : List.of("data-1", "arena-control.lock")) {
            Path original = root.resolve(name);
            Path held = root.resolveSibling("held-" + name);
            Files.move(original, held);
            rejectWithoutMutation();
            assertFalse(Files.exists(original));
            Files.move(held, original);
        }
        Path control = root.resolve("arena-control");
        Path saved = root.resolveSibling("saved-control");
        Files.move(control, saved);
        Files.createDirectory(control);
        rejectWithoutMutation();
        assertTrue(Files.isDirectory(control));
        Files.delete(control);
        Files.move(saved, control);
    }

    @Test
    public void swappedMembersWrongDataRoleAndWrongGeometryRejectWithoutRepair() throws Exception {
        Path first = config(0).directory();
        Path second = config(1).directory();
        Path tmp = first.resolveSibling("swapping");
        Files.move(first, tmp);
        Files.move(second, first);
        Files.move(tmp, second);
        rejectWithoutMutation();
        Files.move(second, tmp);
        Files.move(first, second);
        Files.move(tmp, first);
        Path data = second.resolve("data-1");
        byte[] original = Files.readAllBytes(data);
        Files.write(data, Files.readAllBytes(second.resolve("data-0")));
        rejectWithoutMutation();
        Files.write(data, original);
        try (var file = FileChannel.open(data, StandardOpenOption.WRITE)) {
            file.truncate(original.length - 1);
        }
        rejectWithoutMutation();
    }

    @Test
    public void priorAllocationAndFreeRemainUsedAfterCheckpointAndRotation() throws Exception {
        try (var log = arena(0);
                var bookie = SegmentBookieControlLog.open(control(), SegmentFileHeader.forBookie(manifest),
                        65536, key, validator, new SegmentFileIo(), binding());
                var files = IsolatedSegmentFiles.openAppendOnly(config(0), identity(0), log, new SegmentFileIo())) {
            var allocation = new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 31), new Generation(1), 0, 0, 0, new Generation(1), 1, 4096, 1, 0);
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    log.submitBatch(List.of(allocation)).get(0).status());
            try (var free = log.prepareFree(new UUID(0, 32), 1, files, bookie, ByteBuffer.allocate(4096))) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        log.submitBatch(List.of(free)).get(0).status());
            }
            log.checkpoint();
            log.rotate();
        }
        assertEquals(0, SegmentArenaControlLog.inspect(config(0), identity(0), 65536).currentAllocations());
        rejectWithoutMutation();
    }

    @Test
    public void emptyDataSessionHistoryAndQuarantineRejectEvenWithNoCurrentAllocation() throws Exception {
        try (var log = arena(0)) {
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, log.submitBatch(List.of(
                    new SegmentArenaControlLog.DataSession(new UUID(0, 41), new Generation(1), 0, true),
                    new SegmentArenaControlLog.DataSession(new UUID(0, 42), new Generation(1), 1, false)))
                    .get(1).status());
            log.rotate();
        }
        rejectWithoutMutation();
        // Use a separate member so quarantine has an independent refusing history.
        try (var log = arena(1)) {
            log.submitBatch(List.of(new SegmentArenaControlLog.FileQuarantine(new UUID(0, 43),
                    new Generation(1), 1)));
        }
        var files = new SegmentOwnedFiles();
        assertThrows(IOException.class, () -> SegmentArenaControlLog.requireUnusedInspection(
                config(1), identity(1), 65536, files));
        assertTrue(files.drain().drained());
    }

    @Test
    public void missingRequiredSuffixAndRootAliasRejectWithoutChangingInputs() throws Exception {
        try (var log = arena(1)) {
            log.rotate();
            log.submitBatch(List.of(new SegmentArenaControlLog.DataSession(new UUID(0, 51),
                    new Generation(2), 1, true)));
        }
        var rotation = SegmentControlRotation.publication(config(1), SegmentControlFormat.Owner.ARENA, 0);
        Path current = SegmentControlRotation.path(config(1), SegmentControlFormat.Owner.ARENA,
                rotation.generation());
        try (var file = FileChannel.open(current, StandardOpenOption.WRITE)) {
            file.truncate(file.size() - 1);
        }
        rejectWithoutMutation();
        Path root = config(1).directory();
        Path moved = root.resolveSibling("moved");
        Files.move(root, moved);
        Files.createSymbolicLink(root, moved);
        var owner = owner();
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertTrue(Files.isSymbolicLink(root));
    }

    @Test
    public void readerAliasOfGuardIsRejectedBeforeOpeningASecondDescriptor() throws Exception {
        Path guard = config(1).directory().resolve("isolated.lock");
        Path data = config(1).directory().resolve("data-1");
        Path control = config(1).directory().resolve("arena-control.lock");
        Path saved = control.resolveSibling("saved-control-lock");
        Files.move(control, saved);
        Files.createLink(control, guard);
        rejectWithoutMutation();
        Files.delete(control);
        Files.move(saved, control);
        Files.delete(data);
        Files.createLink(data, guard);
        var owner = owner();
        assertThrows(IOException.class, owner::initialize);
        probe(guard, "HELD");
        assertTrue(owner.closeAndDrain().drained());
        probe(guard, "AVAILABLE");
        Path classicGuard = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        Files.delete(data);
        Files.createLink(data, classicGuard);
        try (var outer = SegmentDataFileLock.openExistingControl(classicGuard)) {
            owner = owner();
            assertThrows(IOException.class, owner::initialize);
            probe(classicGuard, "HELD");
            assertTrue(owner.closeAndDrain().drained());
        }
        probe(classicGuard, "AVAILABLE");
    }

    @Test
    public void nativeReaderCloseFailureRetainsAllGuardsAndOriginalFailureAcrossRepeatedDrain() throws Exception {
        Path path = config(1).directory().resolve("data-1");
        var owner = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("reader native close");
        try (var factory = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open")
                    && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                    && path.equals(invocation.getArgument(0))) {
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
            for (int attempt = 0; attempt < 2; attempt++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.initializationFailure());
                assertSame(failure, result.files().closeFailure());
                assertEquals(1, captured.get().closes.get());
                assertTrue(captured.get().actual.isOpen());
                probe(config(0).directory().resolve("isolated.lock"), "HELD");
                probe(config(1).directory().resolve("arena-control.lock"), "HELD");
            }
        } finally {
            captured.get().actual.close();
            externalGuardCleanup(owner);
        }
        assertFalse(owner.closeAndDrain().drained());
        probe(config(1).directory().resolve("arena-control.lock"), "AVAILABLE");
    }

    @Test
    public void firstGuardChannelCloseFailureCannotBecomeDrainedOnTheSecondLogicalClose() throws Exception {
        var owner = owner();
        owner.initialize();
        var leases = guardOwners(owner);
        var last = leases.get(leases.size() - 1);
        var field = SegmentDataFileLock.class.getDeclaredField("channel");
        field.setAccessible(true);
        var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) field.get(last));
        IOException failure = new IOException("guard channel close");
        fault.closeFailure = failure;
        field.set(last, fault);
        try {
            for (int attempt = 0; attempt < 2; attempt++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertTrue(result.files().drained());
                assertSame(failure, result.guards().get(result.guards().size() - 1).result().channel().failure());
                assertEquals(1, fault.closes.get());
                assertTrue(fault.actual.isOpen());
                assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(
                        config(1).directory().resolve("arena-control.lock")));
                // release actually succeeded; unresolved fd close is not evidence that the OS lock remains held.
                probe(config(1).directory().resolve("arena-control.lock"), "AVAILABLE");
            }
        } finally {
            fault.actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    @Test(timeout = 30000)
    public void actualInitializerMustReturnBeforeExternalDrainAndCannotAwaitItself() throws Exception {
        var owner = owner();
        Path path = config(1).directory().resolve("data-1");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var init = new FutureTask<Void>(() -> {
            try (var factory = mockStatic(FileChannel.class, invocation -> {
                if (invocation.getMethod().getName().equals("open")
                        && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                        && path.equals(invocation.getArgument(0))) {
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
        var initializer = new Thread(init);
        var closer = new Thread(drained);
        try {
            initializer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(drained.isDone());
            probe(config(0).directory().resolve("isolated.lock"), "HELD");
            release.countDown();
            init.get(5, TimeUnit.SECONDS);
            assertTrue(drained.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            initializer.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertFalse(initializer.isAlive());
            assertFalse(closer.isAlive());
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(owner.verified());
    }

    private static List<SegmentDataFileLock> guardOwners(SegmentClassicArenaBindings owner) throws Exception {
        var result = new java.util.ArrayList<SegmentDataFileLock>();
        var field = SegmentClassicArenaBindings.class.getDeclaredField("guards");
        field.setAccessible(true);
        for (Object guard : (List<?>) field.get(owner)) {
            var method = guard.getClass().getDeclaredMethod("owner");
            method.setAccessible(true);
            result.add((SegmentDataFileLock) method.invoke(guard));
        }
        return result;
    }

    private static void externalGuardCleanup(SegmentClassicArenaBindings owner) throws Exception {
        for (var guard : guardOwners(owner)) {
            guard.close();
        }
    }

    private static void probe(Path path, String expected) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                SegmentOwnedBookieControlLogTest.GuardProbe.class.getName(), path.toString())
                .redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8);
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

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;

/** Control child only. No complete Classic bootstrap, Arena readiness or external commissioning is asserted. */
public class SegmentOwnedBookieControlLogTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private SegmentArenaManifest manifest;
    private IsolatedSegmentConfig config;
    private SegmentBookieControlLog.RouteBinding binding;
    private final SecretKeySpec key = new SecretKeySpec(new byte[32], "AES");
    private final ProfileDescriptorValidator validator = new ProfileDescriptorValidator((i, v) -> false,
            (i, v) -> false);

    @Before
    public void formattedControl() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path journal = Files.createDirectory(parent.resolve("classic-0-journal"));
        Path ledger = Files.createDirectory(parent.resolve("classic-1-ledger-index"));
        byte[] incarnation = new byte[16];
        incarnation[0] = 1;
        manifest = new SegmentArenaManifest(new StorageIncarnation(incarnation), BookieId.parse("owned:3181"),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 9), parent.resolve("arena"), 1, 16384)),
                parent.resolve("control"), List.of(0), List.of(
                        new SegmentArenaManifest.ClassicRoot(journal, Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(ledger, Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                SegmentArenaManifest.ClassicRole.INDEX))));
        config = new IsolatedSegmentConfig(manifest.controlDirectory(), 1, 1, 1, 16384, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        binding = new SegmentBookieControlLog.RouteBinding(new UUID(0, 20), manifest.identity());
        try (var created = SegmentBookieControlLog.createManifestControl(config, manifest, manifest.identity(),
                65536, key, validator, new SegmentFileIo(), binding)) {
            assertFalse(created.bootstrapComplete());
        }
    }

    private SegmentBookieControlLog.Owned owned() throws IOException {
        return owned(new SegmentFileIo());
    }

    private SegmentBookieControlLog.Owned owned(SegmentFileIo io) throws IOException {
        return SegmentBookieControlLog.ownedExisting(config, manifest, manifest.identity(), 65536,
                key, validator, io, binding);
    }

    private SegmentBookieControlLog.RouteChange claim() {
        return new SegmentBookieControlLog.RouteChange(SegmentBookieControlLog.RouteAction.CLAIM_CLASSIC,
                44, binding, new UUID(0, 44));
    }

    private void claimAndSeal(SegmentBookieControlLog log) throws Exception {
        try (var driver = new Driver(log)) {
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, driver.submit(claim()).get(0).status());
            assertEquals(SegmentBookieControlLog.RouteState.UNAVAILABLE, log.routeState(44));
            var seal = new SegmentBookieControlLog.RouteChange(SegmentBookieControlLog.RouteAction.BOOTSTRAP_COMPLETE,
                    log.appliedThrough(), binding, new UUID(0, 45));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, driver.submit(seal).get(0).status());
        }
    }

    @Test
    public void driverClaimsAndSealSurviveCheckpointRotationAndHealthyOwnedRestart() throws Exception {
        var owner = owned();
        owner.initialize();
        var log = owner.log();
        try {
            claimAndSeal(log);
            assertEquals(SegmentBookieControlLog.RouteState.CLASSIC, log.routeState(44));
            var seal = log.bootstrapSeal();
            log.checkpoint();
            log.rotate();
            log.rotate();
            assertThrows(IllegalStateException.class, log::close);
            var closed = owner.closeAndDrain();
            assertTrue(closed.drained());
            assertTrue(closed.files().retained().isEmpty());
            assertTrue(closed.guard().drained());
            probe("AVAILABLE");
            assertTrue(owner.closeAndDrain().drained());
            var reopened = owned();
            try {
                reopened.initialize();
                assertEquals(seal, reopened.log().bootstrapSeal());
                assertEquals(SegmentBookieControlLog.RouteState.CLASSIC, reopened.log().routeState(44));
            } finally {
                assertTrue(reopened.closeAndDrain().drained());
            }
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void bootstrapClaimIdentityUsesTheFixedBindingManifestAndLedgerBytes() {
        var digest = HexFormat.of().parseHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");
        var manifest = new ManifestIdentity(digest);
        var enableId = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");
        var scope = new SegmentBookieControlLog.RouteBinding(enableId, manifest);
        long ledger = 0x1020304050607080L;
        var claim = scope.bootstrapClaim(ledger);
        // Independently computed with Python hashlib/struct using the specified big-endian input.
        assertEquals(UUID.fromString("29829e31-4e58-030e-9dc2-f9fa525c7c81"), claim.operationId());
        assertEquals(SegmentBookieControlLog.RouteAction.CLAIM_CLASSIC, claim.action());
        assertEquals(ledger, claim.value());
        assertEquals(scope, claim.binding());
        assertEquals(claim, new SegmentBookieControlLog.RouteBinding(enableId, new ManifestIdentity(digest))
                .bootstrapClaim(ledger));
        assertNotEquals(claim.operationId(), scope.bootstrapClaim(ledger + 1).operationId());
        assertNotEquals(claim.operationId(), new SegmentBookieControlLog.RouteBinding(
                new UUID(enableId.getMostSignificantBits(), enableId.getLeastSignificantBits() + 1), manifest)
                .bootstrapClaim(ledger).operationId());
        digest[0] ^= 1;
        assertNotEquals(claim.operationId(), new SegmentBookieControlLog.RouteBinding(
                enableId, new ManifestIdentity(digest)).bootstrapClaim(ledger).operationId());
        assertThrows(IllegalArgumentException.class, () -> scope.bootstrapClaim(-1));
    }

    @Test
    public void bootstrapClaimReusesTheFirstReceiptAndLegacyIdentityThroughRotationAndReopen() throws Exception {
        var io = new SegmentFileIo();
        var owner = owned(io);
        owner.initialize();
        var log = owner.log();
        var candidate = log.prepareBootstrapClaim(47);
        assertTrue(candidate.applied().isEmpty());
        assertEquals(binding.bootstrapClaim(47), candidate.command());
        var legacy = claim();
        SegmentBookieControlLog.Result original;
        SegmentBookieControlLog.Result old;
        try {
            try (var driver = new Driver(log)) {
                original = driver.submit(candidate.command()).get(0);
                old = driver.submit(legacy).get(0);
            }
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, original.status());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, old.status());
            assertEquals(SegmentBookieControlLog.RouteState.UNAVAILABLE, log.routeState(47));
            var file = config.directory().resolve("bookie-control");
            var before = Files.readAllBytes(file);
            int writes = io.writes;
            int forces = io.forces;
            assertEquals(candidate.command(), log.prepareBootstrapClaim(47).command());
            assertEquals(original, log.prepareBootstrapClaim(47).applied().orElseThrow());
            assertEquals(legacy, log.prepareBootstrapClaim(44).command());
            assertEquals(old, log.prepareBootstrapClaim(44).applied().orElseThrow());
            assertEquals(writes, io.writes);
            assertEquals(forces, io.forces);
            assertArrayEquals(before, Files.readAllBytes(file));
            log.checkpoint();
            log.rotate();
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        var reopened = owned();
        try {
            reopened.initialize();
            var restored = reopened.log();
            assertEquals(original, restored.prepareBootstrapClaim(47).applied().orElseThrow());
            assertEquals(legacy, restored.prepareBootstrapClaim(44).command());
            assertEquals(old, restored.prepareBootstrapClaim(44).applied().orElseThrow());
            try (var driver = new Driver(restored)) {
                var seal = new SegmentBookieControlLog.RouteChange(
                        SegmentBookieControlLog.RouteAction.BOOTSTRAP_COMPLETE,
                        restored.appliedThrough(), binding, new UUID(0, 45));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, driver.submit(seal).get(0).status());
            }
            assertEquals(original, restored.prepareBootstrapClaim(47).applied().orElseThrow());
            assertThrows(IOException.class, () -> restored.prepareBootstrapClaim(48));
        } finally {
            assertTrue(reopened.closeAndDrain().drained());
        }
    }

    @Test
    public void bootstrapClaimRejectsPendingApplyAndAnOccupiedDerivedIdentity() throws Exception {
        var owner = owned();
        owner.initialize();
        try {
            var log = owner.log();
            var candidate = log.prepareBootstrapClaim(47).command();
            var occupant = new SegmentBookieControlLog.RouteChange(SegmentBookieControlLog.RouteAction.CLAIM_CLASSIC,
                    48, binding, candidate.operationId());
            var durable = log.appendChangesDurably(List.of(occupant));
            assertThrows(IOException.class, () -> log.prepareBootstrapClaim(48));
            assertThrows(IOException.class, () -> log.prepareBootstrapClaim(47));
            var original = log.applyDurable(durable).get(0);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, original.status());
            var failure = assertThrows(IOException.class, () -> log.prepareBootstrapClaim(47));
            assertTrue(failure.getMessage().contains("operation is already occupied"));
            assertEquals(occupant, log.prepareBootstrapClaim(48).command());
            assertEquals(original, log.prepareBootstrapClaim(48).applied().orElseThrow());
            assertEquals(original.sequence(), log.appliedThrough());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void bootstrapClaimRequiresOwnedRecoveryAfterAnUncertainForce() throws Exception {
        var forcing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var io = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false, () -> {
            forcing.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new AssertionError(failure);
            }
        });
        var owner = owned(io);
        owner.initialize();
        var log = owner.log();
        var claim = log.prepareBootstrapClaim(47).command();
        try (var driver = new Driver(log)) {
            var terminal = new CompletableFuture<Void>();
            var terminalCalls = new AtomicInteger();
            var pending = driver.begin(claim, () -> {
                terminalCalls.incrementAndGet();
                terminal.complete(null);
            });
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> log.prepareBootstrapClaim(47));
                assertThrows(IOException.class, () -> log.prepareBootstrapClaim(48));
                assertFalse(pending.isDone());
                assertFalse(terminal.isDone());
            } finally {
                release.countDown();
            }
            assertTrue(assertThrows(ExecutionException.class,
                    () -> pending.get(5, TimeUnit.SECONDS)).getCause() instanceof IOException);
            terminal.get(5, TimeUnit.SECONDS);
            assertEquals(0, driver.loop.controlPending());
            assertEquals(1, terminalCalls.get());
            assertEquals(1, io.forces);
            assertThrows(IOException.class, () -> log.prepareBootstrapClaim(47));
            assertThrows(IOException.class, () -> log.prepareBootstrapClaim(48));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        var reopened = owned();
        try {
            reopened.initialize();
            var restored = reopened.log().prepareBootstrapClaim(47);
            assertEquals(claim, restored.command());
            var result = restored.applied().orElseThrow();
            assertEquals(claim.operationId(), result.operationId());
            assertEquals(0, result.sequence());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, result.status());
        } finally {
            assertTrue(reopened.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void batchTerminalReturnsTheOnlyControlSlotBeforeTheNextClaimBatch() throws Exception {
        var io = new SegmentFileIo();
        var owner = owned(io);
        owner.initialize();
        var log = owner.log();
        try (var driver = new Driver(log, 1)) {
            var first = log.prepareBootstrapClaim(47).command();
            var second = log.prepareBootstrapClaim(48).command();
            var firstResult = new CompletableFuture<List<SegmentBookieControlLog.Result>>();
            var terminal = new CompletableFuture<Integer>();
            var terminalCalls = new AtomicInteger();
            var releaseResult = new CountDownLatch(1);
            var rejectedCalls = new AtomicInteger();
            assertTrue(driver.driver.submitChanges(List.of(first), (results, failure) -> {
                assertNull(failure);
                firstResult.complete(results);
                try {
                    assertTrue(releaseResult.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }, () -> {
                assertTrue(driver.driver.isOwnerThread());
                terminalCalls.incrementAndGet();
                terminal.complete(driver.loop.controlPending());
            }));
            SegmentBookieControlLog.Result original;
            try {
                original = firstResult.get(5, TimeUnit.SECONDS).get(0);
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, original.status());
                assertEquals(first.operationId(), original.operationId());
                assertEquals(0, original.sequence());
                assertEquals(1, driver.loop.controlPending());
                assertFalse(terminal.isDone());
                assertFalse(driver.driver.submitChanges(List.of(second),
                        (ignored, failure) -> rejectedCalls.incrementAndGet(), rejectedCalls::incrementAndGet));
            } finally {
                releaseResult.countDown();
            }
            assertEquals(0, terminal.get(5, TimeUnit.SECONDS).intValue());
            var applied = driver.submit(second).get(0);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, applied.status());
            assertEquals(second.operationId(), applied.operationId());
            assertEquals(1, applied.sequence());
            int writes = io.writes;
            int forces = io.forces;
            assertEquals(original, log.prepareBootstrapClaim(47).applied().orElseThrow());
            assertEquals(applied, log.prepareBootstrapClaim(48).applied().orElseThrow());
            assertEquals(writes, io.writes);
            assertEquals(forces, io.forces);
            assertEquals(1, terminalCalls.get());
            assertEquals(0, rejectedCalls.get());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void batchTerminalStillReturnsTheControlSlotWhenTheResultCallbackThrows() throws Exception {
        var owner = owned();
        owner.initialize();
        var log = owner.log();
        var failure = new IllegalStateException("result callback failure");
        try (var driver = new Driver(log, 1)) {
            driver.expectedLoopFailure = failure;
            var claim = log.prepareBootstrapClaim(47).command();
            var result = new CompletableFuture<SegmentBookieControlLog.Result>();
            var terminal = new CompletableFuture<Integer>();
            var resultCalls = new AtomicInteger();
            var terminalCalls = new AtomicInteger();
            var rejectedCalls = new AtomicInteger();
            assertTrue(driver.driver.submitChanges(List.of(claim), (results, cause) -> {
                assertNull(cause);
                resultCalls.incrementAndGet();
                result.complete(results.get(0));
                throw failure;
            }, () -> {
                terminalCalls.incrementAndGet();
                terminal.complete(driver.loop.controlPending());
            }));
            assertEquals(0, terminal.get(5, TimeUnit.SECONDS).intValue());
            var applied = result.get(5, TimeUnit.SECONDS);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, applied.status());
            assertEquals(applied, log.prepareBootstrapClaim(47).applied().orElseThrow());
            assertSame(failure, driver.loop.failure());
            assertTrue(driver.driver.reserveMaintenance());
            driver.driver.releaseMaintenance();
            var next = log.prepareBootstrapClaim(48).command();
            assertFalse(driver.driver.submitChanges(List.of(next),
                    (ignored, cause) -> rejectedCalls.incrementAndGet(), rejectedCalls::incrementAndGet));
            assertEquals(1, resultCalls.get());
            assertEquals(1, terminalCalls.get());
            assertEquals(0, rejectedCalls.get());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void missingExistingGuardNeverCreatesAReplacementAndTheOriginalFailureSurvivesDrain() throws Exception {
        Path path = config.directory().resolve("bookie-control.lock");
        Files.delete(path);
        var owner = owned();
        assertFalse(Files.exists(path));
        IOException failure = assertThrows(IOException.class, owner::initialize);
        assertFalse(Files.exists(path));
        var closed = owner.closeAndDrain();
        assertTrue(closed.drained());
        assertSame(failure, closed.initializationFailure());
        assertFalse(Files.exists(path));
    }

    @Test
    public void invalidIdentityCanDrainItsResourcesWithoutErasingInitializationFailure() throws Exception {
        Path path = config.directory().resolve("superblock-b");
        byte[] bytes = Files.readAllBytes(path);
        bytes[4092] ^= 1;
        Files.write(path, bytes);
        var owner = owned();
        IOException failure = assertThrows(IOException.class, owner::initialize);
        var closed = owner.closeAndDrain();
        assertTrue(closed.drained());
        assertSame(failure, closed.initializationFailure());
        assertArrayEquals(bytes, Files.readAllBytes(path));
        assertThrows(IOException.class, owner::log);
    }

    @Test
    public void failedMainCloseRetainsItsNativeHandleAndTheIndependentGuardAcrossRetries() throws Exception {
        var owner = owned();
        owner.initialize();
        var log = owner.log();
        var fault = replaceNative(channel(log, "originalChannel"));
        fault.closeFailure = new IOException("main native close failed");
        try {
            var first = owner.closeAndDrain();
            assertFalse(first.drained());
            assertSame(fault.closeFailure, first.files().closeFailure());
            assertSame(fault.closeFailure, assertThrows(IOException.class, () -> log.prepareBootstrapClaim(44)));
            assertFalse(first.guard().lock().started());
            assertTrue(fault.actual.isOpen());
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, fault.closes.get());
            assertTrue(guard(owner).lock().isValid());
            probe("HELD");
            assertThrows(IOException.class, () -> SegmentDataFileLock.openExistingControl(
                    config.directory().resolve("bookie-control.lock")));
            assertThrows(IOException.class, owner::log);
        } finally {
            fault.actual.close();
            // Test-only physical cleanup does not requalify the failed file receipt or this owner.
            guard(owner).close();
        }
        assertFalse(owner.closeAndDrain().drained());
        probe("AVAILABLE");
    }

    @Test
    public void failedTemporaryReaderCloseCannotBeLostDuringInitialization() throws Exception {
        var owner = owned();
        var fault = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("identity reader close failed");
        try {
            try (var factory = failingReader(config.directory().resolve("superblock-a"), 1, failure, fault)) {
                assertSame(failure, assertThrows(IOException.class, owner::initialize));
            }
            assertNotNull(fault.get());
            assertFalse(owner.closeAndDrain().drained());
            assertTrue(fault.get().actual.isOpen());
            assertEquals(1, fault.get().closes.get());
            assertTrue(guard(owner).lock().isValid());
            probe("HELD");
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, fault.get().closes.get());
        } finally {
            if (fault.get() != null) {
                fault.get().actual.close();
            }
            guard(owner).close();
        }
    }

    @Test
    public void optionalPublicationFallbackCannotMaskAnUnresolvedNativeReaderClose() throws Exception {
        var owner = owned();
        var fault = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("optional publication reader close failed");
        byte[] original = Files.readAllBytes(config.directory().resolve("bookie-control"));
        try {
            try (var factory = failingReader(config.directory().resolve("superblock-a"), 2, failure, fault)) {
                assertThrows(IOException.class, owner::initialize);
            }
            assertNotNull(fault.get());
            assertFalse(owner.closeAndDrain().drained());
            assertSame(failure, owner.closeAndDrain().files().closeFailure());
            assertTrue(guard(owner).lock().isValid());
            probe("HELD");
            assertArrayEquals(original, Files.readAllBytes(config.directory().resolve("bookie-control")));
        } finally {
            if (fault.get() != null) {
                fault.get().actual.close();
            }
            guard(owner).close();
        }
    }

    @Test
    public void publishedRotationRetainsAFailedPreviousChannelAndDoesNotDeleteItsFile() throws Exception {
        var owner = owned();
        owner.initialize();
        var log = owner.log();
        claimAndSeal(log);
        var first = log.rotate();
        var fault = replaceNative(channel(log, "channel"));
        fault.closeFailure = new IOException("previous rotation native close failed");
        fault.beforeClose = () -> {
            var failure = assertThrows(IOException.class, () -> log.prepareBootstrapClaim(44));
            assertTrue(failure.getMessage().contains("quiescent"));
        };
        try {
            assertSame(fault.closeFailure, assertThrows(IOException.class, log::rotate));
            assertTrue(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE,
                    first.generation())));
            assertEquals(SegmentBookieControlLog.RouteState.UNAVAILABLE, log.routeState(44));
            assertFalse(owner.closeAndDrain().drained());
            assertTrue(fault.actual.isOpen());
            assertEquals(1, fault.closes.get());
            assertTrue(guard(owner).lock().isValid());
            probe("HELD");
        } finally {
            fault.actual.close();
            guard(owner).close();
        }
    }

    @Test(timeout = 30000)
    public void heldControlForceIsNotCancelledByAnEarlyCloseAttempt() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var owner = owned(new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        }));
        owner.initialize();
        try (var driver = new Driver(owner.log())) {
            var future = driver.begin(claim());
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertFalse(owner.closeAndDrain().drained());
                assertTrue(guard(owner).lock().isValid());
            probe("HELD");
                assertFalse(future.isDone());
                release.countDown();
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        future.get(5, TimeUnit.SECONDS).get(0).status());
            } finally {
                release.countDown();
            }
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void initializingThreadCannotDrainItselfAndExternalCloseWaitsForItsActualReturn() throws Exception {
        var owner = owned();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failed = new AtomicReference<>();
        Thread initializer = new Thread(() -> {
            try (var factory = mockStatic(FileChannel.class, invocation -> {
                Object result = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open")
                        && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                        && config.directory().resolve("bookie-control").equals(invocation.getArgument(0))
                        && captured.get() == null) {
                    var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) result);
                    fault.beforeForce = () -> {
                        assertThrows(IllegalStateException.class, owner::closeAndDrain);
                        entered.countDown();
                        try {
                            assertTrue(release.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                    };
                    captured.set(fault);
                    return fault;
                }
                return result;
            })) {
                owner.initialize();
            } catch (Throwable failure) {
                failed.set(failure);
            }
        });
        var closed = new FutureTask<>(owner::closeAndDrain);
        Thread closer = new Thread(closed);
        try {
            initializer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(closed.isDone());
            assertTrue(captured.get().actual.isOpen());
            release.countDown();
            initializer.join(5000);
            assertTrue(closed.get(5, TimeUnit.SECONDS).drained());
            closer.join(5000);
            assertNull(failed.get());
            assertThrows(IOException.class, owner::log);
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
    }

    private void probe(String expected) throws Exception {
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), GuardProbe.class.getName(),
                config.directory().resolve("bookie-control.lock").toString()).redirectErrorStream(true).start();
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

    public static final class GuardProbe {
        public static void main(String[] args) throws Exception {
            try (var channel = FileChannel.open(Path.of(args[0]), java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE); var held = channel.tryLock()) {
                System.out.println(held == null ? "HELD" : "AVAILABLE");
            }
        }
    }

    private static FileChannel channel(SegmentBookieControlLog log, String name) throws Exception {
        var member = SegmentBookieControlLog.class.getDeclaredField(name);
        member.setAccessible(true);
        return (FileChannel) member.get(log);
    }

    private static SegmentDataFileLockTest.FaultChannel replaceNative(FileChannel wrapper) throws Exception {
        var field = wrapper.getClass().getDeclaredField("entry");
        field.setAccessible(true);
        Object entry = field.get(wrapper);
        var raw = entry.getClass().getDeclaredField("resource");
        raw.setAccessible(true);
        var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) raw.get(entry));
        raw.set(entry, fault);
        return fault;
    }

    private static SegmentDataFileLock guard(SegmentBookieControlLog.Owned owner) throws Exception {
        var member = owner.getClass().getDeclaredField("guard");
        member.setAccessible(true);
        return (SegmentDataFileLock) member.get(owner);
    }

    private static MockedStatic<FileChannel> failingReader(Path path, int occurrence, IOException failure,
            AtomicReference<SegmentDataFileLockTest.FaultChannel> captured) {
        AtomicInteger opened = new AtomicInteger();
        return mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            // FileChannel.open(varargs) delegates to its Set overload; intercept only the outer call.
            if (invocation.getMethod().getName().equals("open")
                    && invocation.getMethod().getParameterTypes()[1] == OpenOption[].class
                    && path.equals(invocation.getArgument(0)) && opened.incrementAndGet() == occurrence) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) result);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return result;
        });
    }

    private static final class Driver implements AutoCloseable {
        private final SegmentOwnerLoop loop;
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final SegmentIoExecutor io = new SegmentIoExecutor(1, 4, 1, 4, action -> {
            Thread thread = new Thread(action, "owned-control-test-io");
            threads.add(thread);
            return thread;
        }, null);
        private final SegmentBookieControlDriver driver;
        private Throwable expectedLoopFailure;

        Driver(SegmentBookieControlLog log) {
            this(log, 4);
        }

        Driver(SegmentBookieControlLog log, int controlCapacity) {
            loop = new SegmentOwnerLoop(1, 1, 0, 4, controlCapacity);
            driver = new SegmentBookieControlDriver(loop, io, log);
            loop.start();
        }

        CompletableFuture<List<SegmentBookieControlLog.Result>> begin(SegmentBookieControlLog.Change change) {
            return begin(change, () -> { });
        }

        CompletableFuture<List<SegmentBookieControlLog.Result>> begin(SegmentBookieControlLog.Change change,
                Runnable terminal) {
            var future = new CompletableFuture<List<SegmentBookieControlLog.Result>>();
            assertTrue(driver.submitChanges(List.of(change), (result, failure) -> {
                if (failure != null) {
                    future.completeExceptionally(failure);
                } else {
                    future.complete(result);
                }
            }, terminal));
            return future;
        }

        List<SegmentBookieControlLog.Result> submit(SegmentBookieControlLog.Change change) throws Exception {
            var terminal = new CompletableFuture<Void>();
            var result = begin(change, () -> terminal.complete(null));
            terminal.get(5, TimeUnit.SECONDS);
            return result.get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            loop.stopAdmission();
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            for (Thread thread : threads) {
                thread.join(5000);
                assertFalse(thread.isAlive());
            }
            assertTrue(loop.awaitTermination(Duration.ofSeconds(5)));
            assertSame(expectedLoopFailure, loop.failure());
        }
    }
}

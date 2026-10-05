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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.IsolatedProfileControlEndpoint;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.control.ProfileTransportContext;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentProfileInstallStoreTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(
            EngineProfile.SEGMENT_WAL_ENGINE, PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK,
            3, 3, 2, 0, 7, 11, List.of());
    private static final ProfileDescriptorValidator VALIDATOR = new ProfileDescriptorValidator(
            (id, version) -> false, (id, generation) -> id == 7 && generation == 11);
    private static final byte[] DESCRIPTOR_BYTES = ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR);
    private static final ProfileDescriptorIdentity DESCRIPTOR_ID =
            ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR);

    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileControlScope scope(String bookie, int incarnation) {
        return new ProfileControlScope(ProfileControlOperation.INSTALL, ProfileControlOperation.INSTALL.purpose(),
                44, bytes(16, 1), DESCRIPTOR_ID, bookie, bytes(16, incarnation), "ledger-profile", 1,
                bytes(16, 3), bytes(32, 4), null);
    }

    private static ProfileControlRequest request(ProfileControlScope scope, int verifier) {
        return ProfileControlRequest.install(scope, DESCRIPTOR_BYTES, EngineProfile.SEGMENT_WAL_ENGINE,
                ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1, bytes(20, verifier)));
    }

    private static ProtectedProfileStateStore.Transition transition(ProfileControlScope scope, int verifier) {
        return new ProtectedProfileStateStore.Transition(request(scope, verifier),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 1, true), DESCRIPTOR);
    }

    private static ProfileLedgerContext context() {
        return new ProfileLedgerContext(44, bytes(16, 1), DESCRIPTOR_ID.toBytes());
    }

    private final class Local implements AutoCloseable {
        final IsolatedSegmentConfig config;
        final SegmentFileHeader identity;
        final IsolatedSegmentFiles files;
        final SegmentBookieControlLog log;
        final SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 1, 2);
        final SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
        final SegmentBookieControlDriver driver;
        final SegmentProfileInstallStore store;

        Local(String name, int incarnation, SegmentFileIo faultIo, Duration timeout) throws Exception {
            config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve(name), 1, 1, 1,
                    4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            identity = new SegmentFileHeader(new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(
                    bytes(16, incarnation)), new UUID(0, 9), BookieId.parse(name + ":3181"));
            files = IsolatedSegmentFiles.create(config, identity);
            log = SegmentBookieControlLog.open(config, identity, 1024 * 1024,
                    new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, faultIo);
            driver = new SegmentBookieControlDriver(owner, io, log);
            store = new SegmentProfileInstallStore(driver, identity, timeout);
            owner.start();
        }

        @Override
        public void close() throws Exception {
            io.shutdown();
            assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            owner.stopAdmission();
            assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            try {
                log.close();
            } finally {
                files.close();
            }
        }
    }

    private static void waitFor(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Test force was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }

    private static void awaitApplied(SegmentBookieControlLog log, long before) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (log.appliedThrough() <= before) {
            assertTrue("Original control operation did not finish", System.nanoTime() < deadline);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private static ProtectedProfileStateStore.TransitionResult retry(Local local,
            ProtectedProfileStateStore.Transition transition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (;;) {
            var result = local.store.conditionalApply(transition);
            if (result.outcome() != Outcome.TRANSIENT_RECONCILING) {
                return result;
            }
            assertTrue("Original driver did not release its reservation", System.nanoTime() < deadline);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    @Test
    public void actualForceAndOwnerApplyPrecedeInstallSuccessWithoutNormalActivation() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean();
        SegmentFileIo fault = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (armed.getAndSet(false)) {
                held.countDown();
                waitFor(release);
            }
        });
        try (Local local = new Local("b1", 5, fault, Duration.ofSeconds(5))) {
            long before = local.log.appliedThrough();
            var install = transition(scope("b1:3181", 5), 6);
            armed.set(true);
            var pending = CompletableFuture.supplyAsync(() -> local.store.conditionalApply(install));
            try {
                assertTrue(held.await(5, TimeUnit.SECONDS));
                assertFalse(pending.isDone());
                assertEquals(before, local.log.appliedThrough());
                assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
                CountDownLatch ownerProgress = new CountDownLatch(1);
                assertTrue(local.owner.control(ownerProgress::countDown));
                assertTrue(ownerProgress.await(5, TimeUnit.SECONDS));
                assertEquals(Outcome.TRANSIENT_RECONCILING, local.store.conditionalApply(install).outcome());
            } finally {
                release.countDown();
            }
            var result = pending.get(5, TimeUnit.SECONDS);
            assertEquals(Outcome.APPLIED, result.outcome());
            assertTrue(result.durable());
            assertEquals(1, result.localGeneration());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
            assertEquals(Outcome.ALREADY_APPLIED, retry(local, install).outcome());
            assertEquals(Outcome.CONFLICT, retry(local, transition(scope("b1:3181", 5), 9)).outcome());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 9)));
        }
    }

    @Test
    public void timeoutKeepsActualControlOwnershipUntilForceAndCanResolveTheExactInstall() throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean();
        SegmentFileIo fault = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (armed.getAndSet(false)) {
                held.countDown();
                waitFor(release);
            }
        });
        try (Local local = new Local("b1", 5, fault, Duration.ofMillis(200))) {
            var install = transition(scope("b1:3181", 5), 6);
            long before = local.log.appliedThrough();
            armed.set(true);
            var pending = CompletableFuture.supplyAsync(() -> local.store.conditionalApply(install));
            try {
                assertTrue(held.await(5, TimeUnit.SECONDS));
                var unknown = pending.get(5, TimeUnit.SECONDS);
                assertEquals(Outcome.DURABILITY_UNKNOWN, unknown.outcome());
                assertFalse(unknown.durable());
                assertEquals(Outcome.TRANSIENT_RECONCILING, local.store.conditionalApply(install).outcome());
                assertThrows(IOException.class, local.log::close);
                assertThrows(IOException.class, () -> SegmentBookieControlLog.open(local.config, local.identity,
                        1024 * 1024, new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR));
            } finally {
                release.countDown();
            }
            awaitApplied(local.log, before);
            assertEquals(Outcome.ALREADY_APPLIED, retry(local, install).outcome());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
        }
    }

    @Test
    public void actualForceFailureCannotProduceInstallSuccessOrNormalAdmission() throws Exception {
        AtomicBoolean fail = new AtomicBoolean();
        SegmentFileIo fault = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (fail.get()) {
                throw new IllegalStateException("Injected install barrier failure");
            }
        });
        try (Local local = new Local("b1", 5, fault, Duration.ofSeconds(5))) {
            long before = local.log.appliedThrough();
            fail.set(true);
            var result = local.store.conditionalApply(transition(scope("b1:3181", 5), 6));
            assertEquals(Outcome.DURABILITY_UNKNOWN, result.outcome());
            assertFalse(result.durable());
            assertEquals(before, local.log.appliedThrough());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
        }
    }

    @Test
    public void wrongPhysicalTargetUnsupportedOperationsAndOwnerWaitAreRejectedBeforeSubmission() throws Exception {
        try (Local local = new Local("b1", 5, new SegmentFileIo(), Duration.ofSeconds(5))) {
            long before = local.log.appliedThrough();
            assertEquals(Outcome.CONFLICT, local.store.conditionalApply(
                    transition(scope("other:3181", 5), 6)).outcome());
            assertEquals(Outcome.CONFLICT, local.store.conditionalApply(transition(scope("b1:3181", 7), 6)).outcome());
            var old = scope("b1:3181", 5);
            var activation = new ProfileControlScope(ProfileControlOperation.ACTIVATE_INITIAL,
                    ProfileControlOperation.ACTIVATE_INITIAL.purpose(), old.ledgerId(), old.ledgerInstanceId(),
                    old.descriptorIdentity(), old.targetBookie(), old.targetStorageIncarnation(), old.authorityDomain(),
                    2, bytes(16, 9), old.publicSemanticPayloadIdentity(), null);
            var unsupported = new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(activation),
                    new CommittedProfileAuthority(activation, CommittedProfileAuthority.State.READY_INITIAL,
                            EngineProfile.SEGMENT_WAL_ENGINE, 1, true), null);
            assertEquals(Outcome.UNSUPPORTED, local.store.conditionalApply(unsupported).outcome());
            assertEquals(Outcome.UNSUPPORTED, local.store.queryOperationResult(old).outcome());
            CompletableFuture<Boolean> ownerRefused = new CompletableFuture<>();
            assertTrue(local.owner.control(() -> {
                assertThrows(IllegalStateException.class,
                        () -> local.store.conditionalApply(transition(old, 6)));
                ownerRefused.complete(true);
            }));
            assertTrue(ownerRefused.get(5, TimeUnit.SECONDS));
            assertEquals(before, local.log.appliedThrough());
        }
    }

    @Test
    public void realZkPreparingRootInstallsAllETargetsConcurrentlyThroughTheTypedEndpoint() throws Exception {
        ZooKeeperUtil cluster = new ZooKeeperUtil() {
            @Override
            public String getZooKeeperConnectString() {
                String address = super.getZooKeeperConnectString();
                return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
            }
        };
        ZooKeeper zk = null;
        List<Local> locals = new ArrayList<>();
        CountDownLatch allForces = new CountDownLatch(3);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean();
        try {
            cluster.startCluster();
            ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
            String address = cluster.getZooKeeperConnectString();
            zk = new ZooKeeper("127.0.0.1" + address.substring(address.lastIndexOf(':')), 10000, watcher);
            watcher.waitForConnection();
            String root = "/profile-install-reservation";
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            var reservations = new ZKProfileReservationStore(zk, root, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            List<ProfileControlScope> targets = new ArrayList<>();
            for (int target = 1; target <= 3; target++) {
                SegmentFileIo fault = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (armed.get()) {
                        allForces.countDown();
                        waitFor(release);
                    }
                });
                locals.add(new Local("b" + target, target + 4, fault, Duration.ofSeconds(5)));
                targets.add(scope("b" + target + ":3181", target + 4));
            }
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, reservations.createIfAbsent(
                    new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 2)),
                            targets)).status());
            ProfilePrincipal principal = new ProfilePrincipal(
                    ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                    "operators", "cn=test-ca", "cn=controller");
            ProfileTransportContext transport = new ProfileTransportContext("bookie-profile", "TLSv1.3",
                    true, true, true, principal);
            armed.set(true);
            List<CompletableFuture<ProfileControlResult>> results = new ArrayList<>();
            for (int index = 0; index < locals.size(); index++) {
                Local local = locals.get(index);
                var endpoint = new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE, VALIDATOR,
                        (caller, operation) -> caller.equals(principal) && operation == ProfileControlOperation.INSTALL,
                        reservations.installAuthorityReader(local.identity.bookieId().toString()),
                        (caller, requested, authority) -> caller.equals(principal)
                                && requested.equals(authority.scope())
                                ? Decision.ALLOW : Decision.DENY, local.store);
                ProfileControlRequest request = request(targets.get(index), 6);
                results.add(CompletableFuture.supplyAsync(() -> endpoint.handle(transport, request)));
            }
            assertTrue("All E must enter the same phase before any barrier is released",
                    allForces.await(5, TimeUnit.SECONDS));
            assertTrue(results.stream().noneMatch(CompletableFuture::isDone));
            for (Local local : locals) {
                assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
            }
            armed.set(false);
            release.countDown();
            for (var result : results) {
                assertEquals(Outcome.APPLIED, result.get(5, TimeUnit.SECONDS).outcome());
                assertTrue(result.get().durable());
            }
            for (Local local : locals) {
                assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
            }
        } finally {
            armed.set(false);
            release.countDown();
            for (Local local : locals) {
                local.close();
            }
            if (zk != null) {
                zk.close();
            }
            cluster.stopCluster();
        }
    }

    @Test
    public void storageFactoryBorrowsItsOriginalControlGraphWithoutCreatingAnotherDriver() throws Exception {
        SegmentFileHeader identity = new SegmentFileHeader(
                new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(bytes(16, 5)),
                new UUID(0, 9), BookieId.parse("graph:3181"));
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("graph"),
                1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = new IsolatedSegmentStorage(config, identity,
                new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096,
                        2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 4, 8, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
            storage.startNew();
            var borrowed = storage.profileInstallStore(Duration.ofSeconds(5));
            var result = borrowed.conditionalApply(transition(scope("graph:3181", 5), 6));
            assertEquals(Outcome.APPLIED, result.outcome());
            assertTrue(result.durable());
        }
    }
}

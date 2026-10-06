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
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.IsolatedProfileControlEndpoint;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.control.ProfileTransportContext;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInitialMetadataCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInitialMetadataCoordinator.Status;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ProfileLifecycleHead;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileReadyPage;
import org.apache.bookkeeper.discover.profile.ProfileReplacementPage;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
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
            this(name, incarnation, faultIo, timeout, true);
        }

        Local(String name, int incarnation, SegmentFileIo faultIo, Duration timeout,
                boolean createFiles) throws Exception {
            config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve(name), 1, 1, 1,
                    4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            identity = new SegmentFileHeader(new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(
                    bytes(16, incarnation)), new UUID(0, 9), BookieId.parse(name + ":3181"));
            files = createFiles ? IsolatedSegmentFiles.create(config, identity) : null;
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
                if (files != null) {
                    files.close();
                }
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
            var activation = new ProfileControlScope(ProfileControlOperation.STATUS_QUERY,
                    ProfileControlOperation.STATUS_QUERY.purpose(), old.ledgerId(), old.ledgerInstanceId(),
                    old.descriptorIdentity(), old.targetBookie(), old.targetStorageIncarnation(), old.authorityDomain(),
                    2, bytes(16, 9), old.publicSemanticPayloadIdentity(), null);
            var unsupported = new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(activation),
                    new CommittedProfileAuthority(activation,
                            CommittedProfileAuthority.State.STATUS_QUERYABLE,
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
    private final class InitialCreate implements AutoCloseable {
        final ZooKeeperUtil cluster = new ZooKeeperUtil() {
            @Override
            public String getZooKeeperConnectString() {
                String address = super.getZooKeeperConnectString();
                return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
            }
        };
        final List<Local> locals = new ArrayList<>();
        final List<ProfileControlScope> scopes = new ArrayList<>();
        final Map<String, ProfileInitialMetadataCoordinator.Target> targets = new HashMap<>();
        final ExecutorService cold = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(32));
        final CountDownLatch allForces = new CountDownLatch(3);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean armed = new AtomicBoolean();
        final ZooKeeper zk;
        final LedgerManager manager;
        final ZKProfileReservationStore reservations;
        final ProfileReservation reservation;
        final ProfileInitialMetadataCoordinator coordinator;
        final byte[] password = "initial-create-password".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final ProtectedProfileCredential credential;
        final String root = "/initial-profile-reservation";

        InitialCreate(Duration installTimeout) throws Exception {
            cluster.startCluster();
            ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
            zk = new ZooKeeper(cluster.getZooKeeperConnectString(), 10000, watcher);
            watcher.waitForConnection();
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/initial-profile-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ClientConfiguration conf = new ClientConfiguration();
            conf.setMetadataServiceUri("zk+longhierarchical://127.0.0.1:2181/initial-profile-ledgers");
            var factory = new LongHierarchicalLedgerManagerFactory();
            factory.initialize(conf, new ZkLayoutManager(zk, "/initial-profile-ledgers", ZooDefs.Ids.OPEN_ACL_UNSAFE),
                    factory.getCurrentVersion());
            manager = factory.newLedgerManager();
            reservations = new ZKProfileReservationStore(zk, root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            credential = ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1,
                    DigestManager.generateMasterKey(password));
            ProfilePrincipal principal = new ProfilePrincipal(
                    ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                    "operators", "cn=test-ca", "cn=initial-controller");
            ProfileTransportContext transport = new ProfileTransportContext("bookie-profile", "TLSv1.3",
                    true, true, true, principal);
            for (int target = 1; target <= 3; target++) {
                SegmentFileIo fault = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (armed.get()) {
                        allForces.countDown();
                        waitFor(release);
                    }
                });
                Local local = new Local("c" + target, target + 4, fault, installTimeout);
                locals.add(local);
                ProfileControlScope scope = scope(local.identity.bookieId().toString(), target + 4);
                scopes.add(scope);
                var endpoint = new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE, VALIDATOR,
                        (caller, operation) -> caller.equals(principal) && operation == ProfileControlOperation.INSTALL,
                        reservations.installAuthorityReader(scope.targetBookie()),
                        (caller, requested, authority) -> caller.equals(principal)
                                && requested.equals(authority.scope()) ? Decision.ALLOW : Decision.DENY, local.store);
                ProfileHello.Server hello = new ProfileHello.Server(EngineProfile.SEGMENT_WAL_ENGINE.wireValue(),
                        local.identity.incarnation().value(), 0, local.identity.bookieId().toString(), List.of());
                targets.put(scope.targetBookie(), new ProfileInitialMetadataCoordinator.Target(hello,
                        request -> CompletableFuture.supplyAsync(() -> endpoint.handle(transport, request), cold)));
            }
            reservation = new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 2)), scopes);
            coordinator = new ProfileInitialMetadataCoordinator(reservations, new ProfileLedgerMetadataStore(manager),
                    VALIDATOR, cold);
        }

        LedgerMetadata metadata() {
            return LedgerMetadataBuilder.create().withId(44).withEnsembleSize(3).withWriteQuorumSize(3)
                    .withAckQuorumSize(2).withDigestType(org.apache.bookkeeper.client.api.DigestType.CRC32C)
                    .withPassword(password).withCustomMetadata(Map.of("user", new byte[]{8}))
                    .newEnsembleEntry(0, scopes.stream().map(scope -> BookieId.parse(scope.targetBookie())).toList())
                    .build();
        }

        CompletableFuture<ProfileInitialMetadataCoordinator.Result> create(LedgerMetadata initial) {
            return coordinator.create(initial, reservation, DESCRIPTOR_BYTES, credential, targets);
        }

        String metadataPath() {
            return ((AbstractZkLedgerManager) manager).getLedgerPath(44);
        }

        @Override
        public void close() throws Exception {
            armed.set(false);
            release.countDown();
            cold.shutdown();
            assertTrue(cold.awaitTermination(5, TimeUnit.SECONDS));
            try {
                for (Local local : locals) {
                    local.close();
                }
                manager.close();
            } finally {
                zk.close();
                cluster.stopCluster();
            }
        }
    }

    @Test
    public void coordinatorWaitsForAllEAndPublishesOnlyFrozenExactInitialMetadata() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            LedgerMetadata input = initial.metadata();
            initial.armed.set(true);
            var pending = initial.create(input);
            try {
                assertTrue(initial.allForces.await(5, TimeUnit.SECONDS));
                assertFalse(pending.isDone());
                assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
                input.getCustomMetadata().get("user")[0] = 99;
            } finally {
                initial.armed.set(false);
                initial.release.countDown();
            }
            var created = pending.get(5, TimeUnit.SECONDS);
            assertEquals(Status.METADATA_CREATED, created.status());
            assertEquals(3, created.installs().size());
            assertEquals(8, created.metadata().getValue().getCustomMetadata().get("user")[0]);
            assertEquals(initial.reservation.backlink(), ProfileMetadataBacklink.from(created.metadata().getValue()));
            for (int index = 0; index < 3; index++) {
                assertEquals(initial.scopes.get(index), created.installs().get(index).scope());
                assertEquals(1, created.installs().get(index).localInstallGeneration());
                assertEquals("d6ff18b72491b4622e7e0d9270c7a90b007e8af46a3d84d598e889deda594bdf",
                        HexFormat.of().formatHex(created.installs().get(index).capabilityDigest()));
                assertFalse(initial.locals.get(index).log.normalAdmission(context(),
                        DigestManager.generateMasterKey(initial.password)));
            }
            assertFalse(created.toString().contains("password"));
            assertFalse(created.toString().contains("user"));
            var retried = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
            assertEquals(Status.METADATA_ALREADY_CREATED, retried.status());
            assertEquals(created.installs(), retried.installs());
            assertEquals(created.metadata().getVersion(), retried.metadata().getVersion());
        }
    }

    @Test
    public void coordinatorRequiresAllENotAckQuorumAndRejectsWrongSuccessfulResultBinding() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            String last = initial.scopes.get(2).targetBookie();
            var target = initial.targets.get(last);
            initial.targets.put(last, new ProfileInitialMetadataCoordinator.Target(target.hello(), request ->
                    CompletableFuture.completedFuture(new ProfileControlResult(Outcome.TRANSIENT_RECONCILING,
                            request.scope().operation(), 44, HexFormat.of().formatHex(request.scope().operationId()),
                            1, 0, false))));
            var partial = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
            assertEquals(Status.INSTALL_NOT_READY, partial.status());
            assertEquals(2, partial.installs().size());
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
            initial.targets.put(last, new ProfileInitialMetadataCoordinator.Target(target.hello(), request ->
                    target.client().install(request).thenApply(result -> new ProfileControlResult(result.outcome(),
                            result.operation(), 45, result.operationIdHex(), result.authorityGeneration(),
                            result.localGeneration(), result.durable()))));
            var misbound = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
            assertEquals(Status.INSTALL_UNCERTAIN, misbound.status());
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
            initial.targets.put(last, target);
            assertEquals(Status.METADATA_CREATED, initial.create(initial.metadata()).get(5, TimeUnit.SECONDS).status());
        }
    }

    @Test
    public void coordinatorKeepsMetadataAbsentAfterSubmittedInstallTimeoutAndResolvesExactRetry() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofMillis(200))) {
            initial.armed.set(true);
            var pending = initial.create(initial.metadata());
            try {
                assertTrue(initial.allForces.await(5, TimeUnit.SECONDS));
                var unknown = pending.get(5, TimeUnit.SECONDS);
                assertEquals(Status.INSTALL_UNCERTAIN, unknown.status());
                assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
                for (Local local : initial.locals) {
                    assertThrows(IOException.class, local.log::close);
                }
            } finally {
                initial.armed.set(false);
                initial.release.countDown();
            }
            for (Local local : initial.locals) {
                awaitApplied(local.log, -1);
            }
            assertEquals(Status.METADATA_CREATED, initial.create(initial.metadata()).get(5, TimeUnit.SECONDS).status());
        }
    }

    @Test
    public void coordinatorRechecksTheSameRootSnapshotBeforeCreatingStandardMetadata() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            initial.armed.set(true);
            var pending = initial.create(initial.metadata());
            try {
                assertTrue(initial.allForces.await(5, TimeUnit.SECONDS));
                byte[] bytes = initial.zk.getData(initial.root + "/2c", false, null);
                initial.zk.setData(initial.root + "/2c", bytes, 0);
            } finally {
                initial.armed.set(false);
                initial.release.countDown();
            }
            assertEquals(Status.RESERVATION_CONFLICT, pending.get(5, TimeUnit.SECONDS).status());
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
        }
    }

    @Test
    public void coordinatorRejectsWrongPlanQuorumCredentialAndPeerBeforeAnyReservationWrite() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var metadata = initial.metadata();
            assertThrows(IllegalArgumentException.class, () -> initial.create(
                    LedgerMetadataBuilder.from(metadata).withAckQuorumSize(3).build()));
            assertThrows(IllegalArgumentException.class, () -> initial.create(
                    LedgerMetadataBuilder.from(metadata).replaceEnsembleEntry(0, List.of(
                            BookieId.parse("c2:3181"), BookieId.parse("c1:3181"), BookieId.parse("c3:3181"))).build()));
            assertThrows(IllegalArgumentException.class, () -> initial.coordinator.create(metadata, initial.reservation,
                    DESCRIPTOR_BYTES, ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1,
                            bytes(20, 6)), initial.targets));
            var target = initial.targets.get("c3:3181");
            initial.targets.put("c3:3181", new ProfileInitialMetadataCoordinator.Target(new ProfileHello.Server(
                    EngineProfile.SEGMENT_WAL_ENGINE.wireValue(), bytes(16, 9), 0, "c3:3181", List.of()),
                    target.client()));
            assertThrows(IllegalArgumentException.class, () -> initial.create(metadata));
            assertEquals(null, initial.zk.exists(initial.root + "/2c", false));
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
        }
    }

    @Test
    public void installReceiptKeepsItsBirthGenerationAfterActivationAndFileReplay() throws Exception {
        IsolatedSegmentConfig config;
        SegmentFileHeader identity;
        var install = transition(scope("b1:3181", 5), 6);
        try (Local local = new Local("b1", 5, new SegmentFileIo(), Duration.ofSeconds(5))) {
            config = local.config;
            identity = local.identity;
            assertEquals(1, retry(local, install).localGeneration());
            var old = install.request().scope();
            var active = new ProfileControlScope(ProfileControlOperation.ACTIVATE_INITIAL,
                    ProfileControlOperation.ACTIVATE_INITIAL.purpose(), 44, old.ledgerInstanceId(), DESCRIPTOR_ID,
                    old.targetBookie(), old.targetStorageIncarnation(), old.authorityDomain(), 2,
                    bytes(16, 9), old.publicSemanticPayloadIdentity(), null);
            var activation = new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(active),
                    new CommittedProfileAuthority(active, CommittedProfileAuthority.State.READY_INITIAL,
                            EngineProfile.SEGMENT_WAL_ENGINE, 1, true), null);
            CompletableFuture<List<SegmentBookieControlLog.Result>> result = new CompletableFuture<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!local.driver.submitBatch(List.of(activation), (value, failed) -> {
                if (failed == null) {
                    result.complete(value);
                } else {
                    result.completeExceptionally(failed);
                }
            })) {
                assertTrue(System.nanoTime() < deadline);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertEquals(2, result.get(5, TimeUnit.SECONDS).get(0).localGeneration());
            assertEquals(1, retry(local, install).localGeneration());
            assertTrue(local.log.normalAdmission(context(), bytes(20, 6)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(1, 1, 0, 1, 2);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            try {
                owner.start();
                var driver = new SegmentBookieControlDriver(owner, io, replay);
                var store = new SegmentProfileInstallStore(driver, identity, Duration.ofSeconds(5));
                assertEquals(1, store.conditionalApply(install).localGeneration());
                assertTrue(replay.normalAdmission(context(), bytes(20, 6)));
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void realInitialChainPublishesReadyActivatesAllEAndRetainsDeleteDiscovery() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var created = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
            assertEquals(Status.METADATA_CREATED, created.status());
            var preparing = initial.reservations.read(44).snapshot();
            var page = ProfileReadyPage.from(preparing, created, bytes(16, 4), bytes(32, 5));
            var prepared = initial.reservations.prepareReady(preparing, page, initial.manager);
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, prepared.status());
            var firstScope = page.activation(initial.scopes.get(0).targetBookie());
            assertEquals(ReadStatus.NOT_FOUND,
                    initial.reservations.activationAuthorityReader(firstScope.targetBookie(), initial.manager)
                            .read(ProfileAuthorityKey.fromScope(firstScope)).status());
            var ready = initial.reservations.publishReady(preparing, prepared.page(), initial.manager);
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, ready.status());
            assertEquals(ProfileLifecycleHead.State.READY, ready.snapshot().lifecycle().state());
            ProfilePrincipal principal = new ProfilePrincipal(
                    ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                    "operators", "cn=test-ca", "cn=activation-controller");
            ProfileTransportContext transport = new ProfileTransportContext("bookie-profile", "TLSv1.3",
                    true, true, true, principal);
            List<CompletableFuture<ProfileControlResult>> pending = new ArrayList<>();
            for (Local local : initial.locals) {
                ProfileControlScope scope = page.activation(local.identity.bookieId().toString());
                var endpoint = new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE, VALIDATOR,
                        (caller, operation) -> caller.equals(principal)
                                && operation == ProfileControlOperation.ACTIVATE_INITIAL,
                        initial.reservations.activationAuthorityReader(scope.targetBookie(), initial.manager),
                        (caller, requested, authority) -> caller.equals(principal)
                                && requested.equals(authority.scope()) ? Decision.ALLOW : Decision.DENY, local.store);
                pending.add(CompletableFuture.supplyAsync(() -> endpoint.handle(transport,
                        ProfileControlRequest.operation(scope)), initial.cold));
            }
            for (var result : pending) {
                assertEquals(Outcome.APPLIED, result.get(5, TimeUnit.SECONDS).outcome());
                assertTrue(result.get().durable());
                assertEquals(2, result.get().localGeneration());
            }
            for (Local local : initial.locals) {
                assertTrue(local.log.normalAdmission(context(), DigestManager.generateMasterKey(initial.password)));
            }
            var cut = initial.reservations.cutDelete(ready.snapshot(), bytes(16, 5), bytes(32, 6));
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, cut.status());
            assertEquals(3, initial.reservations.readPublishedPage(44).page().page().installs().size());
            assertEquals(ReadStatus.NOT_FOUND,
                    initial.reservations.activationAuthorityReader(firstScope.targetBookie(), initial.manager)
                            .read(ProfileAuthorityKey.fromScope(firstScope)).status());
            // Admission cut is not an instantaneous cross-store revocation or a physical delete.
            for (Local local : initial.locals) {
                assertTrue(local.log.normalAdmission(context(), DigestManager.generateMasterKey(initial.password)));
            }
        }
    }

    @Test
    public void historicalActivationAfterFenceIsNotCurrentAvailability() throws Exception {
        try (Local local = new Local("b1", 5, new SegmentFileIo(), Duration.ofSeconds(5))) {
            var install = transition(scope("b1:3181", 5), 6);
            assertEquals(Outcome.APPLIED, retry(local, install).outcome());
            var old = install.request().scope();
            var active = new ProfileControlScope(ProfileControlOperation.ACTIVATE_INITIAL,
                    ProfileControlOperation.ACTIVATE_INITIAL.purpose(), 44, old.ledgerInstanceId(), DESCRIPTOR_ID,
                    old.targetBookie(), old.targetStorageIncarnation(), old.authorityDomain(), 2,
                    bytes(16, 9), old.publicSemanticPayloadIdentity(), null);
            var activation = new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(active),
                    new CommittedProfileAuthority(active, CommittedProfileAuthority.State.READY_INITIAL,
                            EngineProfile.SEGMENT_WAL_ENGINE, 1, true), null);
            var first = local.store.conditionalApply(activation);
            if (first.outcome() == Outcome.TRANSIENT_RECONCILING) {
                CountDownLatch settled = new CountDownLatch(1);
                assertTrue(local.owner.control(settled::countDown));
                assertTrue(settled.await(5, TimeUnit.SECONDS));
                first = local.store.conditionalApply(activation);
            }
            assertEquals(Outcome.APPLIED, first.outcome());
            assertTrue(local.log.normalAdmission(context(), bytes(20, 6)));
            var fence = new ProfileOperationCodec.FenceLedger(context(), bytes(16, 10), bytes(20, 6));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE, local.log.submitFence(fence).status());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
            var replayed = local.store.conditionalApply(activation);
            assertEquals(Outcome.STALE_OR_COMPACTED, replayed.outcome());
            assertFalse(replayed.durable());
            assertFalse(local.log.normalAdmission(context(), bytes(20, 6)));
        }
    }

    private Map<String, ProfileInitialLifecycleCoordinator.Target> activators(InitialCreate initial) {
        Map<String, ProfileInitialLifecycleCoordinator.Target> result = new HashMap<>();
        ProfilePrincipal principal = new ProfilePrincipal(ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                "operators", "cn=test-ca", "cn=resuming-controller");
        ProfileTransportContext transport = new ProfileTransportContext("bookie-profile", "TLSv1.3",
                true, true, true, principal);
        for (Local local : initial.locals) {
            String bookie = local.identity.bookieId().toString();
            var endpoint = new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE, VALIDATOR,
                    (caller, operation) -> caller.equals(principal)
                            && operation == ProfileControlOperation.ACTIVATE_INITIAL,
                    initial.reservations.activationAuthorityReader(bookie, initial.manager),
                    (caller, requested, authority) -> caller.equals(principal)
                            && requested.equals(authority.scope()) ? Decision.ALLOW : Decision.DENY, local.store);
            result.put(bookie, new ProfileInitialLifecycleCoordinator.Target(initial.targets.get(bookie).hello(),
                    request -> CompletableFuture.supplyAsync(() -> endpoint.handle(transport, request), initial.cold)));
        }
        return result;
    }

    private ProfileInitialLifecycleCoordinator lifecycle(InitialCreate initial) {
        return new ProfileInitialLifecycleCoordinator(initial.reservations,
                new ProfileLedgerMetadataStore(initial.manager), VALIDATOR, initial.cold);
    }

    private CompletableFuture<ProfileInitialLifecycleCoordinator.Result> initialize(InitialCreate initial,
            ProfileInitialLifecycleCoordinator coordinator, Map<String,
                    ProfileInitialLifecycleCoordinator.Target> targets) {
        return coordinator.initialize(initial.metadata(), initial.reservation, DESCRIPTOR_BYTES, initial.credential,
                initial.targets, targets, bytes(16, 4), bytes(32, 5));
    }

    private ZKProfileReservationStore.ReadySnapshot candidate(InitialCreate initial) throws Exception {
        var created = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
        assertEquals(Status.METADATA_CREATED, created.status());
        var root = initial.reservations.read(44).snapshot();
        var page = ProfileReadyPage.from(root, created, bytes(16, 4), bytes(32, 5));
        return initial.reservations.prepareReady(root, page, initial.manager).page();
    }

    @Test
    public void publishedReplacementAuthorityActivatesActualInstalledFileAndRetainsItAfterDelete() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var created = initial.create(initial.metadata()).get(5, TimeUnit.SECONDS);
            assertEquals(Status.METADATA_CREATED, created.status());
            var preparing = initial.reservations.read(44).snapshot();
            var readyPage = ProfileReadyPage.from(preparing, created, bytes(16, 4), bytes(32, 5));
            var prepared = initial.reservations.prepareReady(preparing, readyPage, initial.manager).page();
            var ready = initial.reservations.publishReady(preparing, prepared, initial.manager).snapshot();
            var metadata = new ProfileLedgerMetadataStore(initial.zk, (AbstractZkLedgerManager) initial.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var before = metadata.readMembership(44).snapshot();
            var changed = metadata.replaceEnsemble(before, 0, List.of(BookieId.parse("c2:3181"),
                    BookieId.parse("c1:3181"), BookieId.parse("c3:3181")));
            assertEquals(ProfileLedgerMetadataStore.Outcome.APPLIED, changed.outcome());
            var receipt = created.installs().stream().filter(value -> value.scope().targetBookie().equals("c1:3181"))
                    .findFirst().orElseThrow();
            var page = ProfileReplacementPage.from(ready, prepared,
                    new ProfileLedgerMetadataStore.MembershipSnapshot(before.birthZxid(), changed.metadata()),
                    0, List.of(receipt), bytes(16, 9), bytes(32, 9));
            var candidate = initial.reservations.prepareReplacement(ready, page, metadata).page();
            var local = initial.locals.get(0);
            byte[] key = DigestManager.generateMasterKey(initial.password);
            assertFalse(local.log.normalAdmission(context(), key));
            var principal = new ProfilePrincipal(ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                    "operators", "cn=test-ca", "cn=replacement-controller");
            var transport = new ProfileTransportContext("bookie-profile", "TLSv1.3", true, true, true, principal);
            var endpoint = new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE, VALIDATOR,
                    (caller, operation) -> caller.equals(principal)
                            && operation == ProfileControlOperation.ACTIVATE_REPLACEMENT,
                    initial.reservations.replacementAuthorityReader("c1:3181", metadata),
                    (caller, requested, authority) -> caller.equals(principal)
                            && requested.equals(authority.scope()) ? Decision.ALLOW : Decision.DENY, local.store);
            var request = ProfileControlRequest.operation(page.activation("c1:3181"));
            assertEquals(Outcome.NOT_FOUND, endpoint.handle(transport, request).outcome());
            assertFalse(local.log.normalAdmission(context(), key));
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    initial.reservations.publishReplacement(ready, candidate, metadata).status());
            var active = endpoint.handle(transport, request);
            assertEquals(Outcome.APPLIED, active.outcome());
            assertEquals(2, active.localGeneration());
            assertTrue(local.log.normalAdmission(context(), key));
            assertEquals(Outcome.ALREADY_APPLIED, endpoint.handle(transport, request).outcome());
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, initial.reservations.cutDelete(
                    initial.reservations.read(44).snapshot(), bytes(16, 10), bytes(32, 10)).status());
            assertEquals(Outcome.NOT_FOUND, endpoint.handle(transport, request).outcome());
            // Remote DELETE_INTENT is admission closure, not a local fence/tombstone or physical delete.
            assertTrue(local.log.normalAdmission(context(), key));
            assertEquals(3, initial.reservations.retainedActivationTargets(44).targets().size());
        }
    }

    @Test
    public void fullInitialLifecycleResumesReadyWithNewCoordinatorAndReplayedTarget() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var first = initialize(initial, lifecycle(initial), activators(initial)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, first.status());
            assertEquals(3, first.activations().size());
            assertEquals(2, first.activations().get(0).localGeneration());
            var version = initial.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            Local old = initial.locals.get(2);
            old.close();
            Local reopened = new Local("c3", 7, new SegmentFileIo(), Duration.ofSeconds(5), false);
            initial.locals.set(2, reopened);
            assertTrue(reopened.log.normalAdmission(context(), DigestManager.generateMasterKey(initial.password)));
            ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
            try (ZooKeeper freshClient = new ZooKeeper(initial.cluster.getZooKeeperConnectString(), 10000, watcher)) {
                watcher.waitForConnection();
                var freshStore = new ZKProfileReservationStore(freshClient, initial.root,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var restart = new ProfileInitialLifecycleCoordinator(freshStore,
                        new ProfileLedgerMetadataStore(initial.manager), VALIDATOR, initial.cold);
                var again = initialize(initial, restart, activators(initial)).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, again.status());
                assertEquals(first.ready(), again.ready());
                assertEquals(first.activations(), again.activations());
                assertEquals(version.getVersion(), again.metadata().getVersion());
                assertEquals(version.getValue().getCToken(), again.metadata().getValue().getCToken());
                assertFalse(again.toString().contains("password"));
            }
        }
    }

    @Test
    public void persistedCandidateRecoveryUsesItsReceiptsWithoutReinstalling() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var prepared = candidate(initial);
            initial.targets.replaceAll((bookie, target) -> new ProfileInitialMetadataCoordinator.Target(target.hello(),
                    request -> CompletableFuture.failedFuture(new IllegalStateException("Unexpected reinstallation"))));
            var completed = initialize(initial, lifecycle(initial), activators(initial)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, completed.status());
            assertEquals(prepared, completed.ready());
            assertEquals(3, completed.activations().size());
        }
    }

    @Test
    public void persistedCandidateDoesNotRebuildMissingRequiredMetadata() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            candidate(initial);
            var stored = initial.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            initial.manager.removeLedgerMetadata(44, stored.getVersion()).get(5, TimeUnit.SECONDS);
            var stopped = initialize(initial, lifecycle(initial), activators(initial)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.QUARANTINED, stopped.status());
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
            assertEquals(ProfileLifecycleHead.State.PREPARING,
                    initial.reservations.read(44).snapshot().lifecycle().state());
        }
    }

    @Test
    public void initialAvailabilityRequiresAllCurrentBoundActivationsInsteadOfAckQuorum() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var targets = activators(initial);
            var last = targets.get("c3:3181");
            targets.put("c3:3181", new ProfileInitialLifecycleCoordinator.Target(last.hello(), request ->
                    CompletableFuture.completedFuture(new ProfileControlResult(Outcome.TRANSIENT_RECONCILING,
                            request.scope().operation(), 44, HexFormat.of().formatHex(request.scope().operationId()),
                            1, 0, false))));
            var partial = initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.NOT_READY, partial.status());
            assertEquals(2, partial.activations().size());
            targets.put("c3:3181", new ProfileInitialLifecycleCoordinator.Target(last.hello(), request ->
                    last.client().activate(request).thenApply(result -> new ProfileControlResult(result.outcome(),
                            result.operation(), 45, result.operationIdHex(), result.authorityGeneration(),
                            result.localGeneration(), result.durable()))));
            var misbound = initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.UNCERTAIN, misbound.status());
            targets.put("c3:3181", last);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS).status());
        }
    }

    @Test
    public void submittedActivationUnknownKeepsOwnershipAndResumesTheSameReadyOperation() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofMillis(200))) {
            var prepared = candidate(initial);
            var root = initial.reservations.read(44).snapshot();
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    initial.reservations.publishReady(root, prepared, initial.manager).status());
            long[] before = initial.locals.stream().mapToLong(local -> local.log.appliedThrough()).toArray();
            initial.armed.set(true);
            var pending = initialize(initial, lifecycle(initial), activators(initial));
            try {
                assertTrue(initial.allForces.await(5, TimeUnit.SECONDS));
                assertEquals(ProfileInitialLifecycleCoordinator.Status.UNCERTAIN,
                        pending.get(5, TimeUnit.SECONDS).status());
                for (Local local : initial.locals) {
                    assertThrows(IOException.class, local.log::close);
                }
            } finally {
                initial.armed.set(false);
                initial.release.countDown();
            }
            for (int index = 0; index < 3; index++) {
                awaitApplied(initial.locals.get(index).log, before[index]);
            }
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    initialize(initial, lifecycle(initial), activators(initial)).get(5, TimeUnit.SECONDS).status());
            assertEquals(prepared, initial.reservations.readPublishedPage(44).page());
        }
    }

    @Test
    public void initialAvailabilityRechecksTheDeleteCutAfterAllActivationReplies() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var targets = activators(initial);
            var last = targets.get("c3:3181");
            targets.put("c3:3181", new ProfileInitialLifecycleCoordinator.Target(last.hello(), request ->
                    last.client().activate(request).thenApply(result -> {
                        var ready = initial.reservations.read(44).snapshot();
                        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                initial.reservations.cutDelete(ready, bytes(16, 5), bytes(32, 6)).status());
                        return result;
                    })));
            var cut = initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.DELETED, cut.status());
            assertEquals(null, cut.metadata());
            assertEquals(3, initial.reservations.readPublishedPage(44).page().page().installs().size());
            assertEquals(ProfileInitialLifecycleCoordinator.Status.DELETED,
                    initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS).status());
        }
    }

    @Test
    public void initialAvailabilityCannotCountHistoricalActivationAfterOneTargetIsFenced() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var targets = activators(initial);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS).status());
            Local last = initial.locals.get(2);
            var fence = new ProfileOperationCodec.FenceLedger(context(), bytes(16, 10),
                    DigestManager.generateMasterKey(initial.password));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE, last.log.submitFence(fence).status());
            assertFalse(last.log.normalAdmission(context(), DigestManager.generateMasterKey(initial.password)));
            var stopped = initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.NOT_READY, stopped.status());
            assertEquals(2, stopped.activations().size());
            assertEquals(null, stopped.metadata());
        }
    }

    @Test
    public void readyRecoveryRejectsConflictingOperationAndDoesNotRebuildMissingMetadata() throws Exception {
        try (InitialCreate initial = new InitialCreate(Duration.ofSeconds(5))) {
            var targets = activators(initial);
            var coordinator = lifecycle(initial);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    initialize(initial, coordinator, targets).get(5, TimeUnit.SECONDS).status());
            var conflict = coordinator.initialize(initial.metadata(), initial.reservation, DESCRIPTOR_BYTES,
                    initial.credential, initial.targets, targets, bytes(16, 4), bytes(32, 9)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.CONFLICT, conflict.status());
            var stored = initial.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            initial.manager.removeLedgerMetadata(44, stored.getVersion()).get(5, TimeUnit.SECONDS);
            var stopped = initialize(initial, lifecycle(initial), targets).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.QUARANTINED, stopped.status());
            assertEquals(null, initial.zk.exists(initial.metadataPath(), false));
        }
    }

}

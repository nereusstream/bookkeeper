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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityPurpose;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInitialMetadataCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ProfileLifecycleHead;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual 3/3/2 file/ZK/TLS composition; no transport facts or successful authority fixtures are injected. */
public class SegmentProfileInitialSocketClientTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private static final ProfileDescriptorValidator VALIDATOR = new ProfileDescriptorValidator(
            (id, version) -> false, (id, generation) -> id == 7 && generation == 11);
    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(EngineProfile.SEGMENT_WAL_ENGINE,
            PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK, 3, 3, 2, 0, 7, 11, List.of());
    private static final byte[] DESCRIPTOR_BYTES = ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR);

    private static byte[] bytes(int size, int value) {
        byte[] out = new byte[size];
        Arrays.fill(out, (byte) value);
        return out;
    }

    private static String certificate(String name) throws Exception {
        return Paths.get(SegmentProfileInitialSocketClientTest.class.getClassLoader().getResource(name).toURI())
                .toString();
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    private static SegmentBookieControlLog bookie(IsolatedSegmentStorage storage) throws Exception {
        var field = IsolatedSegmentStorage.class.getDeclaredField("bookie");
        field.setAccessible(true);
        return (SegmentBookieControlLog) field.get(storage);
    }

    private final class Fixture implements AutoCloseable {
        final EventLoopGroup acceptor = new NioEventLoopGroup(1);
        final EventLoopGroup worker = new NioEventLoopGroup(2);
        final EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        final ThreadPoolExecutor cold = new ThreadPoolExecutor(8, 8, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(64));
        final ZooKeeperUtil cluster = new ZooKeeperUtil() {
            @Override
            public String getZooKeeperConnectString() {
                String address = super.getZooKeeperConnectString();
                return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
            }
        };
        final List<IsolatedSegmentStorage> nodes = new ArrayList<>();
        final List<SegmentFileIo> nodeIo = new ArrayList<>();
        final List<SegmentFileHeader> identities = new ArrayList<>();
        final List<ProfileReceiveBudget> budgets = new ArrayList<>();
        final List<Channel> listeners = new ArrayList<>();
        final List<Channel> accepted = new CopyOnWriteArrayList<>();
        final Map<String, ProfileInitialControlClient> clients = new HashMap<>();
        final List<ProfileControlScope> scopes = new ArrayList<>();
        final List<CountDownLatch> installRelease = new ArrayList<>();
        final List<CountDownLatch> activateRelease = new ArrayList<>();
        final CountDownLatch installing = new CountDownLatch(3);
        final CountDownLatch activating = new CountDownLatch(3);
        final AtomicInteger replyFault = new AtomicInteger();
        final AtomicInteger faultTarget = new AtomicInteger(2);
        ZooKeeper zk;
        LedgerManager manager;
        ZKProfileReservationStore reservations;
        ProfileReservation reservation;
        SslContext peerTls;
        final byte[] password = "socket-initial-password".getBytes(StandardCharsets.UTF_8);
        ProtectedProfileCredential credential;

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl) throws Exception {
            try {
                cluster.startCluster();
                ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
                zk = new ZooKeeper(cluster.getZooKeeperConnectString(), 10000, watcher);
                watcher.waitForConnection();
                zk.create("/socket-initial", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create("/socket-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                ClientConfiguration conf = new ClientConfiguration();
                conf.setMetadataServiceUri("zk+longhierarchical://127.0.0.1:2181/socket-ledgers");
                var factory = new LongHierarchicalLedgerManagerFactory();
                factory.initialize(conf, new ZkLayoutManager(zk, "/socket-ledgers", ZooDefs.Ids.OPEN_ACL_UNSAFE),
                        factory.getCurrentVersion());
                manager = factory.newLedgerManager();
                reservations = new ZKProfileReservationStore(zk, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                credential = ProtectedProfileCredential.of(1, DigestManager.generateMasterKey(password));
                ServerConfiguration tlsConf = new ServerConfiguration();
                tlsConf.setTLSKeyStoreType("PEM");
                tlsConf.setTLSKeyStore(certificate("server-key.pem"));
                tlsConf.setTLSCertificatePath(certificate("server-san-cert.pem"));
                tlsConf.setTLSTrustStoreType("PEM");
                tlsConf.setTLSTrustStore(certificate("client-cert.pem"));
                TLSContextFactory serverTls = TLSContextFactory.forProfileServer();
                serverTls.init(SecurityHandlerFactory.NodeType.Server, tlsConf, UnpooledByteBufAllocator.DEFAULT);
                peerTls = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                        .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                        .trustManager(new File(certificate("server-san-cert.pem"))).protocols("TLSv1.3").build();
                java.security.cert.X509Certificate cert;
                try (var input = Files.newInputStream(Path.of(certificate("client-cert.pem")))) {
                    cert = (java.security.cert.X509Certificate)
                            java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(input);
                }
                ProfilePrincipal principal = new ProfilePrincipal(
                        ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT, "socket-test",
                        cert.getIssuerX500Principal().getName(javax.security.auth.x500.X500Principal.CANONICAL),
                        cert.getSubjectX500Principal().getName(javax.security.auth.x500.X500Principal.CANONICAL));
                Path fixtureDirectory = temporary.newFolder().toPath();
                for (int index = 0; index < 3; index++) {
                    final int target = index;
                    SegmentFileHeader identity = new SegmentFileHeader(new StorageIncarnation(bytes(16, index + 5)),
                            new UUID(0, 9), BookieId.parse("socket" + index + ":3181"));
                    identities.add(identity);
                    IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                            fixtureDirectory.resolve("node-" + index), 1, 1, 1, 4L * 1024 * 1024,
                            4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
                    var storage = new IsolatedSegmentStorage(config, identity, new SecretKeySpec(bytes(32, 7), "AES"),
                            VALIDATOR, new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                            2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                            new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))));
                    nodes.add(storage);
                    storage.startNew();
                    var installGate = new CountDownLatch(blockInstall ? 1 : 0);
                    var activeGate = new CountDownLatch(blockActivate ? 1 : 0);
                    installRelease.add(installGate);
                    activateRelease.add(activeGate);
                    AtomicInteger force = new AtomicInteger();
                    SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        int phase = force.incrementAndGet();
                        if (phase == 1) {
                            installing.countDown();
                            await(installGate);
                        } else if (phase == 2) {
                            activating.countDown();
                            await(activeGate);
                        }
                    });
                    nodeIo.add(io);
                    var field = SegmentBookieControlLog.class.getDeclaredField("io");
                    field.setAccessible(true);
                    field.set(bookie(storage), io);
                    ProfileControlScope scope = new ProfileControlScope(ProfileControlOperation.INSTALL,
                            ProfileAuthorityPurpose.PREPARING_INSTALL, 44, bytes(16, 1),
                            ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR), identity.bookieId().toString(),
                            identity.incarnation().value(), "ledger-profile", 1, bytes(16, 3), bytes(32, 4), null);
                    scopes.add(scope);
                    var installReader = reservations.installAuthorityReader(scope.targetBookie());
                    var activeReader = reservations.activationAuthorityReader(scope.targetBookie(), manager);
                    var control = new SegmentProfileControlConfig(VALIDATOR,
                            (caller, operation) -> caller.equals(principal)
                                    && (operation == ProfileControlOperation.INSTALL
                                        || operation == ProfileControlOperation.ACTIVATE_INITIAL),
                            key -> key.purpose() == ProfileAuthorityPurpose.PREPARING_INSTALL
                                    ? installReader.read(key) : activeReader.read(key),
                            (caller, requested, authority) -> caller.equals(principal)
                                    && requested.equals(authority.scope()) ? Decision.ALLOW : Decision.DENY,
                            cold, Duration.ofSeconds(5));
                    ProfileReceiveBudget budget = new ProfileReceiveBudget(524288, 131072, 262144, 8, 8, 8);
                    budgets.add(budget);
                    listeners.add(new ServerBootstrap().group(acceptor, worker).channel(NioServerSocketChannel.class)
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel socket) throws Exception {
                                    accepted.add(socket);
                                    if (!storage.installProfileConnection(socket.pipeline(), serverTls, budget,
                                            131072, 64, "socket-test", 0, enableControl ? control : null)) {
                                        return;
                                    }
                                    socket.pipeline().addBefore("profileApplication", "responseFault",
                                            new ChannelOutboundHandlerAdapter() {
                                                @Override
                                                public void write(ChannelHandlerContext ctx, Object message,
                                                                  ChannelPromise promise) {
                                                    ByteBuf frame = (ByteBuf) message;
                                                    int op = frame.getUnsignedShort(frame.readerIndex() + 14);
                                                    int fault = target == faultTarget.get() ? replyFault.get() : 0;
                                                    if (fault == 1
                                                            && op == ProfileOperation.ACTIVATE_INITIAL.wireValue()) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        return;
                                                    }
                                                    if (op == ProfileOperation.INSTALL.wireValue()) {
                                                        if (fault == 2) {
                                                            frame.setLong(frame.readerIndex() + 20, 99);
                                                        } else if (fault == 3) {
                                                            frame.setByte(frame.readerIndex() + 60, 9);
                                                        } else if (fault == 4) {
                                                            frame.setLong(frame.readerIndex() + 76, 99);
                                                        } else if (fault == 5) {
                                                            frame.setLong(frame.readerIndex() + 84, 0);
                                                        } else if (fault == 6) {
                                                            frame.setInt(frame.readerIndex() + 16, 3);
                                                        }
                                                    }
                                                    ctx.write(frame, promise);
                                                }
                                            });
                                }
                            }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel());
                }
                reservation = new ProfileReservation(new ProfileMetadataBacklink(44, bytes(16, 1), bytes(16, 2)),
                        scopes);
            } catch (Exception | Error failure) {
                close();
                throw failure;
            }
        }

        CompletableFuture<ProfileInitialControlClient> connect(int target, int maxPending, Duration timeout,
                String hostname, String bookie, byte[] incarnation) {
            return ProfileInitialControlClient.connect(clientGroup, peerTls,
                    (InetSocketAddress) listeners.get(target).localAddress(), hostname, bookie, incarnation,
                    maxPending, timeout);
        }

        ProfileInitialControlClient connect(int target, int maxPending, Duration timeout) throws Exception {
            var identity = identities.get(target);
            var client = connect(target, maxPending, timeout, "apache.bookkeeper.org",
                    identity.bookieId().toString(), identity.incarnation().value()).get(5, TimeUnit.SECONDS);
            ProfileInitialControlClient old = clients.put(identity.bookieId().toString(), client);
            if (old != null) {
                old.close();
            }
            return client;
        }

        void connectAll(Duration timeout) throws Exception {
            for (int target = 0; target < 3; target++) {
                connect(target, 4, timeout);
            }
        }

        ProfileControlRequest install(int target) {
            return ProfileControlRequest.install(scopes.get(target), DESCRIPTOR_BYTES,
                    EngineProfile.SEGMENT_WAL_ENGINE, credential);
        }

        LedgerMetadata metadata() {
            return LedgerMetadataBuilder.create().withId(44).withEnsembleSize(3).withWriteQuorumSize(3)
                    .withAckQuorumSize(2).withDigestType(DigestType.CRC32C).withPassword(password)
                    .newEnsembleEntry(0, scopes.stream().map(s -> BookieId.parse(s.targetBookie())).toList()).build();
        }

        CompletableFuture<ProfileInitialLifecycleCoordinator.Result> initialize() {
            Map<String, ProfileInitialMetadataCoordinator.Target> installs = new HashMap<>();
            Map<String, ProfileInitialLifecycleCoordinator.Target> activations = new HashMap<>();
            clients.forEach((bookie, client) -> {
                installs.put(bookie, new ProfileInitialMetadataCoordinator.Target(client.hello(), client));
                activations.put(bookie, new ProfileInitialLifecycleCoordinator.Target(client.hello(), client));
            });
            return new ProfileInitialLifecycleCoordinator(reservations, new ProfileLedgerMetadataStore(manager),
                    VALIDATOR, cold).initialize(metadata(), reservation, DESCRIPTOR_BYTES, credential, installs,
                    activations, bytes(16, 4), bytes(32, 5));
        }

        ProfileLedgerContext context() {
            return new ProfileLedgerContext(44, bytes(16, 1),
                    ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR).toBytes());
        }

        boolean normal(int target) throws Exception {
            return bookie(nodes.get(target)).normalAdmission(context(), DigestManager.generateMasterKey(password));
        }

        @Override
        public void close() throws Exception {
            installRelease.forEach(CountDownLatch::countDown);
            activateRelease.forEach(CountDownLatch::countDown);
            clients.values().forEach(ProfileInitialControlClient::close);
            for (Channel channel : accepted) {
                channel.close().syncUninterruptibly();
            }
            for (Channel channel : listeners) {
                channel.close().syncUninterruptibly();
            }
            for (IsolatedSegmentStorage storage : nodes) {
                assertTrue(storage.close(Duration.ofSeconds(5)));
            }
            cold.shutdown();
            assertTrue(cold.awaitTermination(5, TimeUnit.SECONDS));
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            worker.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            acceptor.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            if (manager != null) {
                manager.close();
            }
            if (zk != null) {
                zk.close();
            }
            cluster.stopCluster();
            for (ProfileReceiveBudget budget : budgets) {
                for (ProfileReceiveBudget.Lane lane : ProfileReceiveBudget.Lane.values()) {
                    assertEquals(0, budget.used(lane));
                    assertEquals(0, budget.usedConnections(lane));
                }
            }
        }
    }

    @Test
    public void actualAllESocketsWaitForBothPhasesAndReturnSameReadyAvailability() throws Exception {
        try (Fixture fixture = new Fixture(true, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize();
            try {
                assertTrue(fixture.installing.await(5, TimeUnit.SECONDS));
                assertFalse(available.isDone());
                assertEquals(null, fixture.zk.exists(
                        ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44), false));
                for (int target = 0; target < 3; target++) {
                    assertFalse(fixture.normal(target));
                    fixture.installRelease.get(target).countDown();
                }
                assertTrue(fixture.activating.await(5, TimeUnit.SECONDS));
                assertEquals(ProfileLifecycleHead.State.READY,
                        fixture.reservations.read(44).snapshot().lifecycle().state());
                assertFalse(available.isDone());
                fixture.activateRelease.get(0).countDown();
                fixture.activateRelease.get(1).countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!fixture.normal(0) || !fixture.normal(1)) {
                    assertTrue(System.nanoTime() < deadline);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertFalse(available.isDone()); // A=2 is insufficient for initial activation E=3.
                fixture.activateRelease.get(2).countDown();
                var result = available.get(5, TimeUnit.SECONDS);
                assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, result.status());
                assertEquals(3, result.activations().size());
                assertEquals(3, result.ready().page().installs().size());
                assertEquals(fixture.reservation.backlink(),
                        ProfileMetadataBacklink.from(result.metadata().getValue()));
                for (int target = 0; target < 3; target++) {
                    assertTrue(fixture.normal(target));
                    assertEquals(2, result.activations().get(target).localGeneration());
                }
            } finally {
                fixture.installRelease.forEach(CountDownLatch::countDown);
                fixture.activateRelease.forEach(CountDownLatch::countDown);
            }
        }
    }

    @Test
    public void lostActivationResponseReconcilesSameReadyAndMetadataViaNewClient() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.replyFault.set(1);
            fixture.connectAll(Duration.ofMillis(500));
            var uncertain = fixture.initialize().get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.UNCERTAIN, uncertain.status());
            var original = fixture.reservations.readPublishedPage(44).page();
            assertNotNull(original);
            var metadata = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            for (int target = 0; target < 3; target++) {
                assertTrue(fixture.normal(target));
            }
            fixture.replyFault.set(0);
            fixture.connect(2, 4, Duration.ofSeconds(5));
            var recovered = fixture.initialize().get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, recovered.status());
            assertEquals(original, recovered.ready());
            assertEquals(metadata.getVersion(), recovered.metadata().getVersion());
            assertEquals(metadata.getValue().getCToken(), recovered.metadata().getValue().getCToken());
            assertEquals(fixture.reservation.backlink(), ProfileMetadataBacklink.from(recovered.metadata().getValue()));
        }
    }

    @Test
    public void requiredMetadataLossAfterSocketAvailabilityNeverRebuildsIt() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, available.status());
            List<Integer> before = fixture.nodeIo.stream().map(io -> io.writes).toList();
            fixture.manager.removeLedgerMetadata(44, available.metadata().getVersion()).get(5, TimeUnit.SECONDS);
            var recovered = fixture.initialize().get(5, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.QUARANTINED, recovered.status());
            assertEquals(null, fixture.zk.exists(
                        ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44), false));
            assertEquals(before, fixture.nodeIo.stream().map(io -> io.writes).toList());
        }
    }

    @Test
    public void verifiedHelloRejectsWrongPeerIncarnationHostnameAndMissingCapabilityBeforeControl() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            var identity = fixture.identities.get(0);
            assertThrows(IllegalArgumentException.class, () -> ProfileInitialControlClient.connect(
                    fixture.clientGroup, fixture.peerTls, InetSocketAddress.createUnresolved("pending.invalid", 3181),
                    "apache.bookkeeper.org", identity.bookieId().toString(), identity.incarnation().value(),
                    1, Duration.ofSeconds(2)));
            assertThrows(ExecutionException.class, () -> fixture.connect(0, 1, Duration.ofSeconds(2),
                    "apache.bookkeeper.org", "wrong:3181", identity.incarnation().value()).get(5, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> fixture.connect(0, 1, Duration.ofSeconds(2),
                    "apache.bookkeeper.org", identity.bookieId().toString(), bytes(16, 99)).get(5, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> fixture.connect(0, 1, Duration.ofSeconds(2),
                    "wrong.example.invalid", identity.bookieId().toString(), identity.incarnation().value())
                    .get(5, TimeUnit.SECONDS));
            ProfileInitialControlClient client = fixture.connect(0, 1, Duration.ofSeconds(2));
            assertThrows(IllegalArgumentException.class, () -> client.activate(fixture.install(0)));
            assertThrows(IllegalArgumentException.class, () -> client.install(fixture.install(1)));
            assertEquals(0, fixture.nodeIo.get(0).writes);
        }
        try (Fixture fixture = new Fixture(false, false, false)) {
            var identity = fixture.identities.get(0);
            assertThrows(ExecutionException.class, () -> fixture.connect(0, 1, Duration.ofSeconds(2),
                    "apache.bookkeeper.org", identity.bookieId().toString(), identity.incarnation().value())
                    .get(5, TimeUnit.SECONDS));
            assertEquals(0, fixture.nodeIo.get(0).writes);
        }
    }

    @Test
    public void wrongReplyCorrelationScopeGenerationAndErrorFlagsNeverProduceSuccess() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    fixture.reservations.createIfAbsent(fixture.reservation).status());
            for (int fault = 2; fault <= 6; fault++) {
                fixture.replyFault.set(fault);
                ProfileInitialControlClient client = fixture.connect(2, 1, Duration.ofSeconds(2));
                assertThrows(ExecutionException.class,
                        () -> client.install(fixture.install(2)).get(5, TimeUnit.SECONDS));
                assertFalse(fixture.normal(2));
            }
        }
    }

    @Test
    public void correlatedStatusOnlyColdRefusalIsKnownNoneAndNeverFakeDurability() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    fixture.reservations.createIfAbsent(fixture.reservation).status());
            var client = fixture.connect(0, 1, Duration.ofSeconds(5));
            CountDownLatch allBusy = new CountDownLatch(8);
            CountDownLatch release = new CountDownLatch(1);
            try {
                for (int worker = 0; worker < 8; worker++) {
                    fixture.cold.execute(() -> {
                        allBusy.countDown();
                        await(release);
                    });
                }
                assertTrue(allBusy.await(5, TimeUnit.SECONDS));
                for (int queued = 0; queued < 64; queued++) {
                    fixture.cold.execute(() -> { });
                }
                ProfileControlResult refused = client.install(fixture.install(0)).get(5, TimeUnit.SECONDS);
                assertEquals(Outcome.TRANSIENT_RECONCILING, refused.outcome());
                assertEquals(fixture.scopes.get(0).ledgerId(), refused.ledgerId());
                assertFalse(refused.durable());
                assertEquals(0, refused.localGeneration());
                assertEquals(0, fixture.nodeIo.get(0).writes);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void validatedReplyDoesNotReleaseSerializedSlotBeforeActualWritePromise() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    fixture.reservations.createIfAbsent(fixture.reservation).status());
            var client = fixture.connect(0, 1, Duration.ofSeconds(5));
            var field = ProfileInitialControlClient.class.getDeclaredField("channel");
            field.setAccessible(true);
            Channel channel = (Channel) field.get(client);
            AtomicReference<ChannelPromise> held = new AtomicReference<>();
            AtomicReference<ByteBuf> observed = new AtomicReference<>();
            CountDownLatch actualWrite = new CountDownLatch(1);
            channel.eventLoop().submit(() -> channel.pipeline().addAfter(
                    channel.pipeline().context(SslHandler.class).name(), "holdWritePromise",
                    new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                            ByteBuf body = (ByteBuf) message;
                            if (body.getUnsignedShort(body.readerIndex() + 14) == ProfileOperation.INSTALL.wireValue()
                                    && held.compareAndSet(null, promise)) {
                                observed.set(body.retainedDuplicate());
                                ChannelPromise output = ctx.newPromise();
                                output.addListener(sent -> {
                                    assertTrue(sent.isSuccess());
                                    actualWrite.countDown();
                                });
                                ctx.write(body, output);
                            } else {
                                ctx.write(body, promise);
                            }
                        }
                    })).get(5, TimeUnit.SECONDS);
            try {
                var pending = client.install(fixture.install(0));
                assertTrue(actualWrite.await(5, TimeUnit.SECONDS));
                assertEquals(Outcome.APPLIED, pending.get(5, TimeUnit.SECONDS).outcome());
                assertEquals(Outcome.TRANSIENT_RECONCILING, client.install(fixture.install(0)).get().outcome());
                assertEquals(1, fixture.nodeIo.get(0).writes);
                assertTrue(observed.get().getInt(4) != 0); // Raw owned output is still in use by its promise.
                held.get().setSuccess();
                channel.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
                for (int offset = 0; offset < observed.get().capacity(); offset++) {
                    assertEquals(0, observed.get().getByte(offset));
                }
                assertEquals(Outcome.ALREADY_APPLIED, client.install(fixture.install(0))
                        .get(5, TimeUnit.SECONDS).outcome());
            } finally {
                if (held.get() != null) {
                    held.get().trySuccess();
                }
                if (observed.get() != null) {
                    observed.get().release();
                }
            }
        }
    }

    @Test
    public void cancelledWaitingRetainsBoundedPendingUntilOriginalWireTerminal() throws Exception {
        try (Fixture fixture = new Fixture(true, false, true)) {
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    fixture.reservations.createIfAbsent(fixture.reservation).status());
            var client = fixture.connect(0, 1, Duration.ofSeconds(5));
            var submitted = client.install(fixture.install(0));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (fixture.installing.getCount() == 3) {
                assertTrue(System.nanoTime() < deadline);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertTrue(submitted.cancel(true));
            assertEquals(Outcome.TRANSIENT_RECONCILING, client.install(fixture.install(0)).get().outcome());
            assertEquals(1, fixture.nodeIo.get(0).writes);
            fixture.installRelease.get(0).countDown();
            ProfileControlResult repeated;
            do {
                repeated = client.install(fixture.install(0)).get(5, TimeUnit.SECONDS);
                if (repeated.outcome() == Outcome.TRANSIENT_RECONCILING) {
                    assertTrue(System.nanoTime() < deadline);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            } while (repeated.outcome() == Outcome.TRANSIENT_RECONCILING);
            assertEquals(Outcome.ALREADY_APPLIED, repeated.outcome());
            assertTrue(submitted.isCancelled());
            assertFalse(fixture.normal(0));
        }
    }
}

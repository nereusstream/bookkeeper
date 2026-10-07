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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import io.netty.util.ReferenceCounted;
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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.ProfileLedgerHandle;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
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
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInitialMetadataCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileInstallReceipt;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ProfileLifecycleHead;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileReplacementCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileReplacementInstallPage;
import org.apache.bookkeeper.discover.profile.ProfileReplacementPage;
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

/** Actual 3/3/2 and 3/3/3 file/ZK/TLS composition; no transport facts or successful authority fixtures are injected. */
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
        final java.util.Set<Integer> expectedFailedDataSessions = new java.util.HashSet<>();
        final List<ProfileNormalDataClient> dataClients = new ArrayList<>();
        final AtomicInteger authorityReads = new AtomicInteger();
        final java.util.concurrent.atomic.AtomicBoolean readAllowed =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        final List<ProfileControlScope> scopes = new ArrayList<>();
        final List<CountDownLatch> installRelease = new ArrayList<>();
        final List<CountDownLatch> activateRelease = new ArrayList<>();
        final CountDownLatch installing = new CountDownLatch(3);
        final CountDownLatch activating = new CountDownLatch(3);
        final List<Runnable> heldReplies = new CopyOnWriteArrayList<>();
        final CountDownLatch firstReplies = new CountDownLatch(3);
        final CountDownLatch secondReplies = new CountDownLatch(3);
        final List<List<Integer>> dataResults = new ArrayList<>();
        volatile boolean holdFirstReplies;
        final AtomicInteger replyFault = new AtomicInteger();
        final AtomicInteger faultTarget = new AtomicInteger(2);
        ZooKeeper zk;
        LedgerManager manager;
        ZKProfileReservationStore reservations;
        ProfileReservation reservation;
        SslContext peerTls;
        final byte[] password = "socket-initial-password".getBytes(StandardCharsets.UTF_8);
        ProtectedProfileCredential credential;
        final ProfileDescriptor descriptor;
        final byte[] descriptorBytes;

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl) throws Exception {
            this(blockInstall, blockActivate, enableControl, 3);
        }

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl, int count) throws Exception {
            this(blockInstall, blockActivate, enableControl, count, 2);
        }

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl, int count, int ackQuorum)
                throws Exception {
            this(blockInstall, blockActivate, enableControl, count, ackQuorum, false);
        }

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl, int count, int ackQuorum,
                boolean enableRead) throws Exception {
            this(blockInstall, blockActivate, enableControl, count, ackQuorum, enableRead, false);
        }

        Fixture(boolean blockInstall, boolean blockActivate, boolean enableControl, int count, int ackQuorum,
                boolean enableRead, boolean enableLac) throws Exception {
            descriptor = new ProfileDescriptor(EngineProfile.SEGMENT_WAL_ENGINE,
                    PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK, 3, 3, ackQuorum, 0, 7, 11, List.of());
            descriptorBytes = ProfileDescriptorCodec.encode(descriptor, VALIDATOR);
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
                for (int index = 0; index < count; index++) {
                    final int target = index;
                    dataResults.add(new CopyOnWriteArrayList<>());
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
                            ProfileDescriptorCodec.identityOf(descriptor, VALIDATOR), identity.bookieId().toString(),
                            identity.incarnation().value(), "ledger-profile", 1, bytes(16, 3), bytes(32, 4), null);
                    if (index < 3) {
                        scopes.add(scope);
                    }
                    var metadataContext = new ProfileLedgerMetadataStore(zk, (AbstractZkLedgerManager) manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var installReader = reservations.installAuthorityReader(scope.targetBookie(), metadataContext);
                    var activeReader = reservations.activationAuthorityReader(scope.targetBookie(), manager);
                    var replacementReader = reservations.replacementAuthorityReader(
                            scope.targetBookie(), metadataContext);
                    var control = new SegmentProfileControlConfig(VALIDATOR,
                            (caller, operation) -> caller.equals(principal)
                                    && (operation == ProfileControlOperation.INSTALL
                                        || operation == ProfileControlOperation.ACTIVATE_INITIAL
                                        || operation == ProfileControlOperation.ACTIVATE_REPLACEMENT),
                            key -> {
                                authorityReads.incrementAndGet();
                                return key.purpose() == ProfileAuthorityPurpose.PREPARING_INSTALL
                                        ? installReader.read(key)
                                        : key.purpose() == ProfileAuthorityPurpose.POST_MEMBERSHIP_REPLACEMENT
                                            ? replacementReader.read(key) : activeReader.read(key);
                            },
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
                                            131072, 64, "socket-test", 0, enableControl ? control : null,
                                            enableRead ? new SegmentProfileReadConfig((caller, query) ->
                                                readAllowed.get() && caller.equals(principal)
                                                    && query.context().equals(context()),
                                                enableLac ? (caller, query) -> readAllowed.get()
                                                    && caller.equals(principal)
                                                    && query.context().equals(context()) : null) : null)) {
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
                                                            && (op == ProfileOperation.ACTIVATE_INITIAL.wireValue()
                                                                || op == ProfileOperation.ACTIVATE_REPLACEMENT
                                                                    .wireValue())) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        return;
                                                    }
                                                    if (op == ProfileOperation.ADD_NORMAL.wireValue()) {
                                                        dataResults.get(target).add(
                                                                (int) frame.getUnsignedByte(frame.readerIndex() + 39));
                                                        long request = frame.getLong(frame.readerIndex() + 20);
                                                        if (holdFirstReplies && request == 2) {
                                                            heldReplies.add(() -> ctx.executor().execute(
                                                                    () -> ctx.writeAndFlush(frame, promise)));
                                                            firstReplies.countDown();
                                                            return;
                                                        }
                                                        if (request == 3) {
                                                            secondReplies.countDown();
                                                        }
                                                        if (fault == 7) {
                                                            frame.setLong(frame.readerIndex() + 20, 99);
                                                        } else if (fault == 8) {
                                                            frame.setByte(frame.readerIndex() + 39, 0);
                                                        } else if (fault == 9) {
                                                            frame.setInt(frame.readerIndex() + 16, 3);
                                                        }
                                                    }
                                                    if (op == ProfileOperation.READ_ENTRY.wireValue()
                                                            || op == ProfileOperation.READ_LAC.wireValue()) {
                                                        if (fault == 10) {
                                                            frame.setInt(frame.readerIndex() + 44, 0);
                                                        } else if (fault == 11) {
                                                            frame.setByte(frame.readerIndex() + 60, 99);
                                                        } else if (fault == 12) {
                                                            frame.setLong(frame.readerIndex() + 112, 99);
                                                        } else if (fault == 13) {
                                                            frame.setLong(frame.readerIndex() + 124, 99);
                                                        } else if (fault == 14) {
                                                            frame.setInt(frame.readerIndex() + 16, 3);
                                                        } else if (fault == 15) {
                                                            frame.setInt(frame.readerIndex() + 120, 1024 * 1024 + 37);
                                                        } else if (fault == 16) {
                                                            int last = frame.writerIndex() - 1;
                                                            frame.setByte(last, frame.getByte(last) ^ 1);
                                                        } else if (fault == 17
                                                                && op == ProfileOperation.READ_LAC.wireValue()) {
                                                            heldReplies.add(() -> ctx.executor().execute(
                                                                    () -> ctx.writeAndFlush(frame, promise)));
                                                            return;
                                                        }
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
            return ProfileControlRequest.install(scopes.get(target), descriptorBytes,
                    EngineProfile.SEGMENT_WAL_ENGINE, credential);
        }

        LedgerMetadata metadata() {
            return LedgerMetadataBuilder.create().withId(44).withEnsembleSize(3).withWriteQuorumSize(3)
                    .withAckQuorumSize(descriptor.ackQuorumSize()).withDigestType(DigestType.CRC32C)
                    .withPassword(password)
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
                    VALIDATOR, cold).initialize(metadata(), reservation, descriptorBytes, credential, installs,
                    activations, bytes(16, 4), bytes(32, 5));
        }

        ProfileLedgerContext context() {
            return new ProfileLedgerContext(44, bytes(16, 1),
                    ProfileDescriptorCodec.identityOf(descriptor, VALIDATOR).toBytes());
        }

        boolean normal(int target) throws Exception {
            return bookie(nodes.get(target)).normalAdmission(context(), DigestManager.generateMasterKey(password));
        }

        void releaseReplies() {
            holdFirstReplies = false;
            heldReplies.forEach(Runnable::run);
            heldReplies.clear();
        }

        @Override
        public void close() throws Exception {
            releaseReplies();
            installRelease.forEach(CountDownLatch::countDown);
            activateRelease.forEach(CountDownLatch::countDown);
            clients.values().forEach(ProfileInitialControlClient::close);
            dataClients.forEach(ProfileNormalDataClient::close);
            for (Channel channel : accepted) {
                channel.close().syncUninterruptibly();
            }
            for (Channel channel : listeners) {
                channel.close().syncUninterruptibly();
            }
            for (int target = 0; target < nodes.size(); target++) {
                IsolatedSegmentStorage storage = nodes.get(target);
                if (expectedFailedDataSessions.contains(target)) {
                    assertThrows(java.io.IOException.class, () -> storage.close(Duration.ofSeconds(5)));
                } else {
                    assertTrue(storage.close(Duration.ofSeconds(5)));
                }
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

    private static ReferenceCounted entry(Fixture fixture, long entryId, int payloadBytes) throws Exception {
        var digest = DigestManager.instantiate(44, fixture.password,
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                UnpooledByteBufAllocator.DEFAULT, false);
        return digest.computeDigestAndPackageForSending(entryId, -1, payloadBytes,
                Unpooled.wrappedBuffer(bytes(payloadBytes, 8)), DigestManager.generateMasterKey(fixture.password), 0);
    }

    private static ProfileNormalDataClient dataClient(Fixture fixture, int target, int pending, Duration timeout)
            throws Exception {
        var identity = fixture.identities.get(target);
        var client = ProfileNormalDataClient.connect(fixture.clientGroup, fixture.peerTls,
                (InetSocketAddress) fixture.listeners.get(target).localAddress(), "apache.bookkeeper.org",
                identity.bookieId().toString(), identity.incarnation().value(), pending, 1060, timeout)
                .get(5, TimeUnit.SECONDS);
        fixture.dataClients.add(client);
        return client;
    }

    private static IsolatedSegmentFiles files(IsolatedSegmentStorage storage) throws Exception {
        var field = IsolatedSegmentStorage.class.getDeclaredField("files");
        field.setAccessible(true);
        return (IsolatedSegmentFiles) field.get(storage);
    }

    @Test
    public void ordinaryDataClientUsesRealDigestAndDurableResponsesInOriginalAckSet() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            int reads = fixture.authorityReads.get();
            List<CountDownLatch> release = new ArrayList<>();
            CountDownLatch allForcing = new CountDownLatch(3);
            List<CompletableFuture<ProfileStatus>> replies = new ArrayList<>();
            for (int target = 0; target < 3; target++) {
                CountDownLatch gate = new CountDownLatch(1);
                release.add(gate);
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    allForcing.countDown();
                    await(gate);
                });
                var client = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                ReferenceCounted source = entry(fixture, 0, 16);
                try {
                    replies.add(client.addNormal(fixture.context(), fixture.credential, 0, source));
                } finally {
                    source.release(); // Client owns a stable retained view until its real output copy.
                }
            }
            try {
                assertTrue(allForcing.await(5, TimeUnit.SECONDS));
                assertTrue(replies.stream().noneMatch(CompletableFuture::isDone));
                var acks = new RoundRobinDistributionSchedule(3, 2, 3).getAckSet();
                release.get(0).countDown();
                ProfileStatus first = replies.get(0).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.OK, first.statusClass());
                assertEquals(ProfileStatus.DurableResult.APPLIED, first.durableResult());
                assertFalse(acks.completeBookieAndCheck(0));
                assertFalse(acks.completeBookieAndCheck(0)); // Duplicate target is not another durable replica.
                release.get(1).countDown();
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        replies.get(1).get(5, TimeUnit.SECONDS).durableResult());
                assertTrue(acks.completeBookieAndCheck(1));
                assertFalse(replies.get(2).isDone());
                release.get(2).countDown();
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        replies.get(2).get(5, TimeUnit.SECONDS).durableResult());
                assertEquals(reads, fixture.authorityReads.get());
            } finally {
                release.forEach(CountDownLatch::countDown);
            }
        }
    }

    @Test
    public void ordinarySourceQuotaAndCancellationKeepLogicalUnknownSeparateFromCurrentNone() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            files(fixture.nodes.get(0)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            var client = dataClient(fixture, 0, 1, Duration.ofSeconds(5));
            ReferenceCounted source = entry(fixture, 0, 16);
            try {
                var pending = client.addNormal(fixture.context(), fixture.credential, 0, source);
                source.release();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(pending.cancel(true));
                ReferenceCounted refusedSource = entry(fixture, 0, 16);
                try {
                    ProfileStatus current = client.addNormal(fixture.context(), fixture.credential, 0,
                            refusedSource).get();
                    assertEquals(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE, current.statusClass());
                    assertEquals(ProfileStatus.DurableResult.NONE, current.durableResult());
                    assertEquals(1, refusedSource.refCnt());
                } finally {
                    refusedSource.release();
                }
                release.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                ProfileStatus reconciled;
                do {
                    ReferenceCounted retry = entry(fixture, 0, 16);
                    try {
                        reconciled = client.addNormal(fixture.context(), fixture.credential, 0, retry)
                                .get(5, TimeUnit.SECONDS);
                    } finally {
                        retry.release();
                    }
                    if (reconciled.statusClass() == ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE) {
                        assertTrue(System.nanoTime() < deadline);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                } while (reconciled.statusClass() == ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE);
                assertEquals(ProfileStatus.DurableResult.APPLIED, reconciled.durableResult());
                assertTrue(pending.isCancelled());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void ordinaryDataRejectionAndEntryBoundNeverBecomeSuccessfulWrites() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            var client = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            ReferenceCounted source = entry(fixture, 0, 16);
            try {
                ProfileStatus denied = client.addNormal(fixture.context(), ProtectedProfileCredential.of(1,
                        bytes(20, 99)), 0, source).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.UNAUTHORIZED, denied.statusClass());
                assertEquals(ProfileStatus.DurableResult.NONE, denied.durableResult());
                assertEquals(1, source.refCnt());
                var wrong = new ProfileLedgerContext(44, bytes(16, 9), fixture.context().descriptorIdentity());
                ProfileStatus conflict = client.addNormal(wrong, fixture.credential, 0, source)
                        .get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT, conflict.statusClass());
                assertEquals(ProfileStatus.DurableResult.NONE, conflict.durableResult());
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        client.addNormal(fixture.context(), fixture.credential, 0, source)
                                .get(5, TimeUnit.SECONDS).durableResult());
            } finally {
                source.release();
            }
            ReferenceCounted oversized = entry(fixture, 1, 1025);
            try {
                assertThrows(IllegalArgumentException.class,
                        () -> client.addNormal(fixture.context(), fixture.credential, 1, oversized));
                assertEquals(1, oversized.refCnt());
            } finally {
                oversized.release();
            }
        }
    }

    @Test
    public void ordinaryTimeoutAfterSubmissionKeepsServerIoAliveAndSameCoordinateCanReconcile() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            files(fixture.nodes.get(0)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            var client = dataClient(fixture, 0, 1, Duration.ofMillis(100));
            ReferenceCounted source = entry(fixture, 0, 16);
            try {
                var unknown = client.addNormal(fixture.context(), fixture.credential, 0, source);
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(ExecutionException.class, () -> unknown.get(5, TimeUnit.SECONDS));
                assertEquals(0, files(fixture.nodes.get(0)).dataIo.forces);
                var configField = IsolatedSegmentStorage.class.getDeclaredField("config");
                configField.setAccessible(true);
                Path ownedDirectory = ((IsolatedSegmentConfig) configField.get(fixture.nodes.get(0))).directory();
                assertThrows(java.io.IOException.class, () -> SegmentDataFileLock.open(ownedDirectory));
                var retrying = dataClient(fixture, 0, 1, Duration.ofSeconds(5));
                var originalCoordinate = retrying.addNormal(fixture.context(), fixture.credential, 0, source);
                ProfileStatus currentNone = retrying.addNormal(fixture.context(), fixture.credential, 0, source).get();
                assertEquals(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE, currentNone.statusClass());
                assertEquals(ProfileStatus.DurableResult.NONE, currentNone.durableResult());
                assertTrue(unknown.isCompletedExceptionally());
                release.countDown();
                ProfileStatus resolved = originalCoordinate.get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.DurableResult.APPLIED, resolved.durableResult());
                assertTrue(unknown.isCompletedExceptionally());
            } finally {
                release.countDown();
                source.release();
            }
        }
    }

    @Test
    public void ordinaryServerForceFailureKeepsFullUnknownStatusAndNeverAcks() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            fixture.expectedFailedDataSessions.add(0);
            files(fixture.nodes.get(0)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            var client = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            ReferenceCounted source = entry(fixture, 0, 16);
            try {
                ProfileStatus unknown = client.addNormal(fixture.context(), fixture.credential, 0, source)
                        .get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN, unknown.statusClass());
                assertEquals(ProfileStatus.DurableResult.UNKNOWN, unknown.durableResult());
                assertEquals(ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, unknown.retryDisposition());
                assertEquals(1, source.refCnt());
            } finally {
                source.release();
            }
        }
    }

    @Test
    public void ordinaryReplyWrongIdFakeOkNoneAndFlagsCannotBecomeAcknowledgments() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE,
                    fixture.initialize().get(5, TimeUnit.SECONDS).status());
            fixture.faultTarget.set(0);
            ReferenceCounted source = entry(fixture, 0, 16);
            try {
                for (int fault = 7; fault <= 9; fault++) {
                    fixture.replyFault.set(fault);
                    var client = dataClient(fixture, 0, 1, Duration.ofSeconds(5));
                    assertThrows(ExecutionException.class, () -> client.addNormal(fixture.context(),
                            fixture.credential, 0, source).get(5, TimeUnit.SECONDS));
                }
            } finally {
                source.release();
            }
        }
    }

    private record PreparedReplacement(ProfileReplacementPage page,
            ZKProfileReservationStore.Snapshot source, ZKProfileReservationStore.ReplacementSnapshot candidate,
            ProfileInitialControlClient control) { }

    private PreparedReplacement replacementOverSocket(Fixture fixture) throws Exception {
        var coordinator = fixture.reservations;
        var source = coordinator.read(44).snapshot();
        var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        var before = metadata.readMembership(44).snapshot();
        var original = before.metadata().getValue().getAllEnsembles().lastEntry().getValue();
        var replacement = fixture.identities.get(3);
        var scope = new ProfileControlScope(ProfileControlOperation.INSTALL, ProfileControlOperation.INSTALL.purpose(),
                44, bytes(16, 1), ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR),
                replacement.bookieId().toString(), replacement.incarnation().value(), "ledger-profile", 1,
                bytes(16, 10), bytes(32, 10), null);
        var next = List.of(original.get(0), original.get(1), replacement.bookieId());
        var plan = ProfileReplacementInstallPage.from(source, before, 100, next, List.of(scope));
        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                coordinator.prepareReplacementInstall(source, plan, metadata).status());
        var control = fixture.connect(3, 4, Duration.ofSeconds(5));
        var installed = control.install(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                EngineProfile.SEGMENT_WAL_ENGINE, fixture.credential)).get(5, TimeUnit.SECONDS);
        assertEquals(Outcome.APPLIED, installed.outcome());
        assertFalse(fixture.normal(3));
        var changed = metadata.replaceEnsemble(before, 100, next);
        assertEquals(ProfileLedgerMetadataStore.Outcome.APPLIED, changed.outcome());
        var receipt = ProfileInstallReceipt.from(scope, control.hello(), installed);
        var page = ProfileReplacementPage.from(source, coordinator.readPublishedPage(44).page(),
                new ProfileLedgerMetadataStore.MembershipSnapshot(before.birthZxid(), changed.metadata()), 100,
                List.of(receipt), bytes(16, 11), bytes(32, 11));
        var prepared = coordinator.prepareReplacement(source, page, metadata);
        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, prepared.status());
        return new PreparedReplacement(page, source, prepared.page(), control);
    }

    @Test
    public void newPeerInactiveInstallAndReplacementActivationUseActualTlsMetadataAndFileIo() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var prepared = replacementOverSocket(fixture);
            var request = ProfileControlRequest.operation(prepared.page().activation("socket3:3181"));
            assertEquals(Outcome.NOT_FOUND, prepared.control().activate(request).get(5, TimeUnit.SECONDS).outcome());
            assertFalse(fixture.normal(3));
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, fixture.reservations.publishReplacement(
                    prepared.source(), prepared.candidate(), metadata).status());
            assertEquals(Outcome.APPLIED, prepared.control().activate(request).get(5, TimeUnit.SECONDS).outcome());
            assertTrue(fixture.normal(3));
            var client = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
            var source = entry(fixture, 100, 16);
            try {
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        client.addNormal(fixture.context(), fixture.credential, 100, source).get(5, TimeUnit.SECONDS)
                                .durableResult());
            } finally {
                source.release();
            }
            assertEquals(4, fixture.reservations.retainedActivationTargets(44).targets().size());
        }
    }

    @Test
    public void lostReplacementSocketResponseReconcilesSamePublishedOperationWithNewControlClient() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var prepared = replacementOverSocket(fixture);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            fixture.reservations.publishReplacement(prepared.source(), prepared.candidate(), metadata);
            prepared.control().close();
            var failing = fixture.connect(3, 1, Duration.ofMillis(150));
            fixture.faultTarget.set(3);
            fixture.replyFault.set(1);
            var request = ProfileControlRequest.operation(prepared.page().activation("socket3:3181"));
            assertThrows(ExecutionException.class, () -> failing.activate(request).get(5, TimeUnit.SECONDS));
            assertTrue(fixture.normal(3));
            fixture.replyFault.set(0);
            var retry = fixture.connect(3, 1, Duration.ofSeconds(5));
            assertEquals(Outcome.ALREADY_APPLIED, retry.activate(request).get(5, TimeUnit.SECONDS).outcome());
            assertEquals(4, fixture.reservations.retainedActivationTargets(44).targets().size());
        }
    }

    private static BookKeeper sdk(Fixture fixture) throws Exception {
        if (fixture.zk.exists("/socket-ledgers/available", false) == null) {
            fixture.zk.create("/socket-ledgers/available", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fixture.zk.create("/socket-ledgers/available/readonly", new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        ClientConfiguration config = new ClientConfiguration();
        config.setMetadataServiceUri("zk+longhierarchical://" + fixture.cluster.getZooKeeperConnectString()
                + "/socket-ledgers");
        config.setUseV2WireProtocol(false);
        config.setExplictLacInterval(0);
        config.setDelayEnsembleChange(true);
        config.setAddEntryQuorumTimeout(5);
        return BookKeeper.forConfig(config).zk(fixture.zk).eventLoopGroup(fixture.clientGroup).build();
    }

    private static ProfileLedgerHandle writer(Fixture fixture, BookKeeper sdk,
            org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator.Result available,
            int entries, int attempts, Duration deadline, Duration backoff) throws Exception {
        Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
        for (int target = 0; target < 3; target++) {
            data.put(fixture.identities.get(target).bookieId(), dataClient(fixture, target, 8, Duration.ofSeconds(5)));
        }
        return ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, entries,
                1024, attempts, deadline, backoff);
    }

    @Test
    public void originalSdkAllocatesEntriesQuorumAndContiguousCallbacksOverActualProfileSockets() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            fixture.holdFirstReplies = true;
            CountDownLatch force = new CountDownLatch(3);
            CountDownLatch release = new CountDownLatch(1);
            for (int target = 0; target < 3; target++) {
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    force.countDown();
                    await(release);
                });
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(3), Duration.ofMillis(5));
                List<Long> callbacks = new CopyOnWriteArrayList<>();
                CompletableFuture<Long> first = new CompletableFuture<>();
                CompletableFuture<Long> second = new CompletableFuture<>();
                handle.asyncAddEntry(bytes(16, 8), (code, ledger, entryId, ctx) -> {
                    callbacks.add(entryId);
                    if (code == 0) {
                        first.complete(entryId);
                    } else {
                        first.completeExceptionally(org.apache.bookkeeper.client.BKException.create(code));
                    }
                }, null);
                assertTrue(force.await(5, TimeUnit.SECONDS));
                handle.asyncAddEntry(bytes(16, 9), (code, ledger, entryId, ctx) -> {
                    callbacks.add(entryId);
                    if (code == 0) {
                        second.complete(entryId);
                    } else {
                        second.completeExceptionally(org.apache.bookkeeper.client.BKException.create(code));
                    }
                }, null);
                try {
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    release.countDown();
                    assertTrue(fixture.firstReplies.await(5, TimeUnit.SECONDS));
                    assertTrue(fixture.secondReplies.await(5, TimeUnit.SECONDS));
                    Object later = handle.getPendingAddOps().stream().skip(1).findFirst().orElseThrow();
                    var completed = later.getClass().getDeclaredField("completed");
                    completed.setAccessible(true);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    boolean laterHasQuorum;
                    do {
                        synchronized (later) {
                            laterHasQuorum = completed.getBoolean(later);
                        }
                        if (!laterHasQuorum) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                    } while (!laterHasQuorum && System.nanoTime() < deadline);
                    assertTrue(laterHasQuorum);
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    fixture.releaseReplies();
                    assertEquals(0, first.get(5, TimeUnit.SECONDS).longValue());
                    assertEquals(1, second.get(5, TimeUnit.SECONDS).longValue());
                    assertEquals(List.of(0L, 1L), callbacks);
                    assertEquals(1, handle.getLastAddConfirmed());
                    assertEquals(32, handle.getLength());
                    var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    assertEquals(LedgerMetadata.State.OPEN, actual.getValue().getState());
                    assertEquals(available.metadata().getVersion(), actual.getVersion());
                    assertThrows(ExecutionException.class, () -> handle.closeAsync().get(5, TimeUnit.SECONDS));
                    assertEquals(LedgerMetadata.State.OPEN,
                            fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
                } finally {
                    release.countDown();
                    fixture.releaseReplies();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void evidenceEntry(Fixture fixture, ProfileNormalDataClient client,
            long id, long lac, long length, int payload) throws Exception {
        var digest = DigestManager.instantiate(44, fixture.password,
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                UnpooledByteBufAllocator.DEFAULT, false);
        var source = digest.computeDigestAndPackageForSending(id, lac, length,
                Unpooled.wrappedBuffer(bytes(payload, 8)), DigestManager.generateMasterKey(fixture.password), 0);
        try {
            assertEquals(ProfileStatus.DurableResult.APPLIED, client.addNormal(fixture.context(), fixture.credential,
                    id, source).get(5, TimeUnit.SECONDS).durableResult());
        } finally {
            source.release();
        }
    }

    @Test
    public void recoveryContextIsRealSameRecordCasAndResumesItsExactOperation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var capture = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31));
            assertEquals(org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND,
                    capture.status());
            var plan = capture.plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            assertEquals(ProfileLedgerMetadataStore.Outcome.APPLIED, entered.outcome());
            assertEquals(plan.context(), entered.context());
            assertEquals(fixture.context(), entered.context().ledger());
            var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertEquals(LedgerMetadata.State.IN_RECOVERY, actual.getValue().getState());
            assertEquals(available.metadata().getValue().getAllEnsembles(), actual.getValue().getAllEnsembles());
            assertEquals(available.metadata().getValue().getCToken(), actual.getValue().getCToken());
            assertEquals(184, actual.getValue().getCustomMetadata().get(
                    org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY).length);
            assertTrue(fixture.normal(0)); // Metadata is not a physical fence or grant.
            var again = fixture.reservations.beginRecovery(metadata, plan);
            assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, again.outcome());
            assertEquals(actual.getVersion(), again.metadata().metadata().getVersion());
            var resumed = new ZKProfileReservationStore(fixture.zk, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5)).readRecovery(metadata, 44);
            assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, resumed.outcome());
            assertEquals(entered.context(), resumed.context());
            assertEquals(actual.getVersion(), resumed.metadata().metadata().getVersion());
            var fencing = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                    fixture.reservations, metadata, fixture.cold);
            var fenced = fencing.fence(resumed, fixture.clients, fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.Status.FENCED_CONTEXT,
                    fenced.status());
            assertEquals(3, fenced.fences().size());
            assertFalse(fixture.normal(0));
            assertFalse(fixture.normal(1));
            assertFalse(fixture.normal(2));
            var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
            var repeated = fencing.fence(resumed, fixture.clients, fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(fenced, repeated);
            assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
            assertEquals(LedgerMetadata.State.IN_RECOVERY,
                    fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
        }
    }

    @Test
    public void recoveryFencingCountsOnlyCurrentVerifiedPeersAndNeverClaimsAnUnfencedSource() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            var fencing = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                    fixture.reservations, metadata, fixture.cold);
            var supplied = new HashMap<>(fixture.clients);
            supplied.put("socket0:3181", fixture.clients.get("socket1:3181"));
            var enough = fencing.fence(entered, supplied, fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.Status.FENCED_CONTEXT,
                    enough.status());
            assertEquals(List.of("socket1:3181", "socket2:3181"),
                    enough.fences().stream().map(value -> value.bookie()).toList());
            assertTrue(fixture.normal(0));
            assertFalse(fixture.normal(1));
            assertFalse(fixture.normal(2));
            var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
            var missing = fencing.fence(entered, Map.of("socket0:3181", fixture.clients.get("socket1:3181")),
                    fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.Status.DEFERRED,
                    missing.status());
            assertTrue(missing.fences().isEmpty());
            {
                var wrong = ProtectedProfileCredential.of(1, bytes(20, 99));
                var denied = fencing.fence(entered, fixture.clients, wrong).get(5, TimeUnit.SECONDS);
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.Status.DEFERRED,
                        denied.status());
                assertTrue(denied.fences().isEmpty());
            }
            assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
            assertEquals(LedgerMetadata.State.IN_RECOVERY,
                    fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
        }
    }

    @Test
    public void sdkLacEvidenceUsesQuorumAndMatchingPrefixLengthWithoutChangingWriterState() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            for (int target : List.of(0, 1)) {
                var data = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                evidenceEntry(fixture, data, 99, 98, 1000, 16);
                evidenceEntry(fixture, data, 100, 99, 1100, 100);
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    int authorities = fixture.authorityReads.get();
                    var before = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var evidence = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                    assertEquals(available.metadata(), evidence.metadata());
                    assertEquals(99, evidence.maxObservedLac());
                    assertEquals(99, evidence.prefixEntryId());
                    assertEquals(1000, evidence.prefixLength());
                    assertEquals(2, evidence.sources().size());
                    for (var source : evidence.sources()) {
                        assertEquals(100, source.sourceEntryId());
                        assertEquals(99, source.piggybackLac());
                        assertEquals(1100, source.sourceLength());
                        assertEquals(100, source.payloadBytes());
                    }
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertEquals(0, handle.getLength());
                    assertEquals(authorities, fixture.authorityReads.get());
                    assertEquals(before, fixture.nodeIo.stream().map(io -> io.forces).toList());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkLacEvidenceRejectsIncompleteCoverageMissingAnchorDigestOrLengthConflict() throws Exception {
        for (int mode = 0; mode < 5; mode++) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                for (int target : mode == 0 ? List.of(0) : List.of(0, 1)) {
                    var data = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                    if (mode != 1) {
                        evidenceEntry(fixture, data, 99, 98, mode == 3 && target == 1 ? 1001 : 1000, 16);
                    }
                    evidenceEntry(fixture, data, 100, 99, mode == 4 && target == 1 ? 1101 : 1100, 100);
                }
                if (mode == 2) {
                    fixture.faultTarget.set(1);
                    fixture.replyFault.set(16);
                }
                try (BookKeeper sdk = sdk(fixture)) {
                    var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                    try {
                        ExecutionException failed = assertThrows(ExecutionException.class,
                                () -> handle.readLacEvidence().get(5, TimeUnit.SECONDS));
                        Throwable cause = failed.getCause();
                        while (cause.getCause() != null) {
                            cause = cause.getCause();
                        }
                        assertTrue(cause instanceof org.apache.bookkeeper.client.BKException);
                        assertEquals(mode < 2 ? org.apache.bookkeeper.client.BKException.Code.ReadException
                                : org.apache.bookkeeper.client.BKException.Code.DigestMatchException,
                                ((org.apache.bookkeeper.client.BKException) cause).getCode());
                        assertEquals(-1, handle.getLastAddConfirmed());
                        assertEquals(0, handle.getLength());
                    } finally {
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    @Test
    public void sdkLacEvidenceEmptyCoverageUsesTheOriginalA2AndA3Thresholds() throws Exception {
        for (int ack : List.of(2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, ack, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                for (int target = 0; target < 4 - ack; target++) {
                    var fenced = fixture.clients.get("socket" + target + ":3181")
                            .fence(fixture.context(), bytes(16, 24), fixture.credential).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileStatus.DurableResult.APPLIED, fenced.durableResult());
                }
                try (BookKeeper sdk = sdk(fixture)) {
                    var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                    try {
                        var evidence = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                        assertEquals(-1, evidence.maxObservedLac());
                        assertEquals(-1, evidence.prefixEntryId());
                        assertEquals(0, evidence.prefixLength());
                        assertEquals(4 - ack, evidence.sources().size());
                        assertEquals(LedgerMetadata.State.OPEN,
                                fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
                    } finally {
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    @Test
    public void sdkLacEvidenceCallerCancellationKeepsItsSingleQueryReservationUntilWireDrain() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            for (int target : List.of(0, 1)) {
                var data = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                evidenceEntry(fixture, data, 99, 98, 1000, 16);
                evidenceEntry(fixture, data, 100, 99, 1100, 100);
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    fixture.faultTarget.set(0);
                    fixture.replyFault.set(17);
                    var caller = handle.readLacEvidence();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(1, fixture.heldReplies.size());
                    assertTrue(caller.cancel(true));
                    ExecutionException refused = assertThrows(ExecutionException.class,
                            () -> handle.readLacEvidence().get(5, TimeUnit.SECONDS));
                    assertEquals(org.apache.bookkeeper.client.BKException.Code.TooManyRequestsException,
                            ((org.apache.bookkeeper.client.BKException) refused.getCause()).getCode());
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                    ProfileLedgerHandle.LacEvidence next = null;
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    do {
                        try {
                            next = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                        } catch (ExecutionException busy) {
                            assertEquals(org.apache.bookkeeper.client.BKException.Code.TooManyRequestsException,
                                    ((org.apache.bookkeeper.client.BKException) busy.getCause()).getCode());
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                    } while (next == null && System.nanoTime() < until);
                    assertNotNull(next);
                    assertEquals(99, next.prefixEntryId());
                    assertEquals(1000, next.prefixLength());
                    assertTrue(caller.isCancelled());
                    assertEquals(-1, handle.getLastAddConfirmed());
                } finally {
                    fixture.releaseReplies();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void lacSourcePreservesItsEntryIdAndLengthWithoutInventingAPrefixLength() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            var digest = DigestManager.instantiate(44, fixture.password,
                    org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                            org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                    UnpooledByteBufAllocator.DEFAULT, false);
            var source = digest.computeDigestAndPackageForSending(100, 99, 1100,
                    Unpooled.wrappedBuffer(bytes(16, 8)), DigestManager.generateMasterKey(fixture.password), 0);
            try {
                assertEquals(ProfileStatus.DurableResult.APPLIED, data.addNormal(fixture.context(), fixture.credential,
                        100, source).get(5, TimeUnit.SECONDS).durableResult());
            } finally {
                source.release();
            }
            int authorities = fixture.authorityReads.get();
            var before = fixture.nodeIo.stream().map(io -> io.forces).toList();
            var evidence = data.readLac(fixture.context()).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.Outcome.SOURCE,
                    evidence.outcome());
            assertEquals(100, evidence.sourceEntryId());
            assertEquals(99, evidence.piggybackLac());
            assertEquals(1100, evidence.sourceLength());
            var actual = data.readEntry(fixture.context(), 100).get(5, TimeUnit.SECONDS);
            assertEquals(actual.entry(), evidence.entry());
            assertEquals(authorities, fixture.authorityReads.get());
            assertEquals(before, fixture.nodeIo.stream().map(io -> io.forces).toList());
            var emptyBeforeFence = dataClient(fixture, 1, 4, Duration.ofSeconds(5));
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.Outcome.REFUSED,
                    emptyBeforeFence.readLac(fixture.context()).get(5, TimeUnit.SECONDS).outcome());
            var fence = fixture.clients.get("socket1:3181")
                    .fence(fixture.context(), bytes(16, 24), fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileStatus.StatusClass.OK, fence.statusClass());
            assertEquals(ProfileStatus.DurableResult.APPLIED, fence.durableResult());
            var empty = emptyBeforeFence.readLac(fixture.context()).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.Outcome.EMPTY, empty.outcome());
            assertEquals(-1, empty.sourceEntryId());
            assertEquals(-1, empty.piggybackLac());
            assertEquals(0, empty.sourceLength());
            fixture.readAllowed.set(false);
            assertEquals(ProfileStatus.StatusClass.UNAUTHORIZED,
                    data.readLac(fixture.context()).get(5, TimeUnit.SECONDS).status().statusClass());
        }
    }

    @Test
    public void actualProfilePointReadReturnsStoredBytesThroughTheOriginalSdkWithoutControlIo() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                    int reads = fixture.authorityReads.get();
                    List<Integer> forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var entries = handle.readEntries(0, 0);
                    var first = entries.nextElement();
                    assertEquals(0, first.getEntryId());
                    assertTrue(Arrays.equals(bytes(16, 8), first.getEntry()));
                    assertFalse(entries.hasMoreElements());
                    assertEquals(reads, fixture.authorityReads.get());
                    assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void actualPointReadSeparatesUnknownFencedMissingAndScopeDenial() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var client = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            var source = entry(fixture, 0, 16);
            try {
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        client.addNormal(fixture.context(), fixture.credential, 0, source)
                                .get(5, TimeUnit.SECONDS).durableResult());
            } finally {
                source.release();
            }
            var found = client.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                    found.outcome());
            assertEquals(44, found.entry().getLong(0));
            assertEquals(0, found.entry().getLong(8));
            var uncertain = client.readEntry(fixture.context(), 99).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.REFUSED,
                    uncertain.outcome());
            assertEquals(ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE, uncertain.status().statusClass());
            byte[] fenceId = bytes(16, 12);
            var fenced = fixture.clients.get("socket0:3181").fence(fixture.context(), fenceId, fixture.credential)
                    .get(5, TimeUnit.SECONDS);
            assertEquals(ProfileStatus.StatusClass.OK, fenced.statusClass());
            assertEquals(ProfileStatus.DurableResult.APPLIED, fenced.durableResult());
            int force = fixture.nodeIo.get(0).forces;
            var missing = client.readEntry(fixture.context(), 99).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.MISSING,
                    missing.outcome());
            assertEquals(0, missing.entry().remaining());
            assertEquals(force, fixture.nodeIo.get(0).forces);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                    client.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
            fixture.readAllowed.set(false);
            assertEquals(ProfileStatus.StatusClass.UNAUTHORIZED,
                    client.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).status().statusClass());
        }
    }

    @Test
    public void readCapabilityAndExactScopeAreRequiredBeforeReturningAnyEntry() throws Exception {
        for (boolean enabled : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, enabled)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var client = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
                int force = fixture.nodeIo.get(0).forces;
                int authorities = fixture.authorityReads.get();
                ProfileLedgerContext requested = enabled
                        ? new ProfileLedgerContext(44, bytes(16, 22), fixture.context().descriptorIdentity())
                        : fixture.context();
                var reply = client.readEntry(requested, 0).get(5, TimeUnit.SECONDS);
                assertEquals(enabled ? ProfileStatus.StatusClass.UNAUTHORIZED
                        : ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                        reply.status().statusClass());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.REFUSED,
                        reply.outcome());
                assertEquals(0, reply.entry().remaining());
                assertEquals(force, fixture.nodeIo.get(0).forces);
                assertEquals(authorities, fixture.authorityReads.get());
                var lac = client.readLac(fixture.context()).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                        lac.status().statusClass());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.Outcome.REFUSED,
                        lac.outcome());
            }
        }
    }

    @Test
    public void readMalformedRepliesNeverBecomeEntrySuccessOrMissing() throws Exception {
        malformedReadReplies(false);
    }

    @Test
    public void lacMalformedRepliesNeverBecomeSourceOrEmpty() throws Exception {
        malformedReadReplies(true);
    }

    private void malformedReadReplies(boolean latest) throws Exception {
        for (int fault : List.of(10, 11, 12, 13, 14, 15)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, latest)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var client = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
                var source = entry(fixture, 0, 16);
                try {
                    assertEquals(ProfileStatus.DurableResult.APPLIED, client.addNormal(fixture.context(),
                            fixture.credential, 0, source).get(5, TimeUnit.SECONDS).durableResult());
                } finally {
                    source.release();
                }
                fixture.faultTarget.set(0);
                fixture.replyFault.set(fault);
                assertThrows(ExecutionException.class,
                        () -> (latest ? client.readLac(fixture.context())
                                : client.readEntry(fixture.context(), 0)).get(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void actualControlFenceUsesOneOperationAndDoesNotAcceptWrongCredential() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var control = fixture.clients.get("socket0:3181");
            var denied = control.fence(fixture.context(), bytes(16, 19),
                    ProtectedProfileCredential.of(1, bytes(20, 20))).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileStatus.StatusClass.UNAUTHORIZED, denied.statusClass());
            int before = fixture.nodeIo.get(0).forces;
            byte[] operation = bytes(16, 21);
            var fenced = control.fence(fixture.context(), operation, fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileStatus.DurableResult.APPLIED, fenced.durableResult());
            assertEquals(before + 1, fixture.nodeIo.get(0).forces);
            var repeated = control.fence(fixture.context(), operation, fixture.credential).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileStatus.DurableResult.ALREADY_APPLIED, repeated.durableResult());
            assertEquals(before + 1, fixture.nodeIo.get(0).forces);
            assertFalse(fixture.normal(0));
            assertTrue(fixture.normal(1));
        }
    }

    @Test
    public void fenceSocketUnknownRetainsPhysicalControlAndResolvesTheSameOperation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var control = fixture.connect(0, 4, Duration.ofMillis(300));
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            var field = SegmentBookieControlLog.class.getDeclaredField("io");
            field.setAccessible(true);
            var actualIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            field.set(bookie(fixture.nodes.get(0)), actualIo);
            byte[] operation = bytes(16, 23);
            try {
                var pending = control.fence(fixture.context(), operation, fixture.credential);
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
                assertEquals(0, actualIo.forces);
                assertThrows(java.io.IOException.class, bookie(fixture.nodes.get(0))::close);
                release.countDown();
                var reconnected = fixture.connect(0, 4, Duration.ofSeconds(5));
                ProfileStatus resolved;
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                do {
                    resolved = reconnected.fence(fixture.context(), operation, fixture.credential)
                            .get(5, TimeUnit.SECONDS);
                    if (resolved.statusClass() != ProfileStatus.StatusClass.OK) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                    }
                } while (resolved.statusClass() != ProfileStatus.StatusClass.OK && System.nanoTime() < until);
                assertEquals(ProfileStatus.DurableResult.ALREADY_APPLIED, resolved.durableResult());
                assertEquals(1, actualIo.forces);
                assertTrue(pending.isCompletedExceptionally());
                assertFalse(fixture.normal(0));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void readTimeoutKeepsFrameAndFilePinThroughNativeGetUntilActualReadTerminates() throws Exception {
        readTimeoutKeepsActualCustody(false);
    }

    @Test
    public void lacTimeoutKeepsFrameAndFilePinUntilActualReadTerminates() throws Exception {
        readTimeoutKeepsActualCustody(true);
    }

    private void readTimeoutKeepsActualCustody(boolean latest) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, latest)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var client = dataClient(fixture, 0, 4, Duration.ofMillis(300));
            var source = entry(fixture, 0, 16);
            try {
                assertEquals(ProfileStatus.DurableResult.APPLIED, client.addNormal(fixture.context(),
                        fixture.credential, 0, source).get(5, TimeUnit.SECONDS).durableResult());
            } finally {
                source.release();
            }
            var field = IsolatedSegmentStorage.class.getDeclaredField("execution");
            field.setAccessible(true);
            SegmentExecutionGroup execution = (SegmentExecutionGroup) field.get(fixture.nodes.get(0));
            SegmentOwnerLoop owner = execution.owner(0);
            CountDownLatch blocked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                @Override
                public void run(SegmentOwnerLoop.Ticket ticket) {
                    execution.io().submit(ticket, () -> {
                        blocked.countDown();
                        await(release);
                    }, cause -> failure.set(cause));
                }

                @Override
                public void failed(Throwable cause) {
                    failure.set(cause);
                }
            }));
            try {
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                var pending = latest ? client.readLac(fixture.context()) : client.readEntry(fixture.context(), 0);
                var ioField = IsolatedSegmentFiles.class.getDeclaredField("activeIo");
                ioField.setAccessible(true);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                int pins;
                do {
                    var sampled = new java.util.concurrent.FutureTask<Integer>(() -> {
                        var actualFiles = files(fixture.nodes.get(0));
                        synchronized (actualFiles) {
                            return ioField.getInt(actualFiles);
                        }
                    });
                    assertTrue(owner.control(sampled::run));
                    pins = sampled.get(5, TimeUnit.SECONDS);
                    if (pins == 0) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                } while (pins == 0 && System.nanoTime() < until);
                assertEquals(1, pins);
                assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
                assertTrue(fixture.budgets.get(0).used(ProfileReceiveBudget.Lane.DATA) > 0);
                assertThrows(java.io.IOException.class, files(fixture.nodes.get(0))::close);
                release.countDown();
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.budgets.get(0).used(ProfileReceiveBudget.Lane.DATA) != 0
                        && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(0, fixture.budgets.get(0).used(ProfileReceiveBudget.Lane.DATA));
                assertEquals(null, failure.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void sdkThreeOfThreeRequiresTheThirdActualDurableReplica() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 3)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            CountDownLatch thirdForcing = new CountDownLatch(1);
            CountDownLatch releaseThird = new CountDownLatch(1);
            files(fixture.nodes.get(2)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                thirdForcing.countDown();
                await(releaseThird);
            });
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    var first = sdkAdd(handle, bytes(16, 8));
                    assertTrue(thirdForcing.await(5, TimeUnit.SECONDS));
                    var transportField = ProfileLedgerHandle.class.getDeclaredField("transport");
                    transportField.setAccessible(true);
                    var transport = (org.apache.bookkeeper.proto.BookieClient) transportField.get(handle);
                    // Both wire responses and original logical requests must terminate before the negative check.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while ((fixture.dataResults.get(0).isEmpty() || fixture.dataResults.get(1).isEmpty()
                            || transport.getNumPendingRequests(fixture.identities.get(0).bookieId(), 44) != 0
                            || transport.getNumPendingRequests(fixture.identities.get(1).bookieId(), 44) != 0)
                            && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(1, fixture.dataResults.get(0).size());
                    assertEquals(1, fixture.dataResults.get(1).size());
                    assertEquals(0, transport.getNumPendingRequests(fixture.identities.get(0).bookieId(), 44));
                    assertEquals(0, transport.getNumPendingRequests(fixture.identities.get(1).bookieId(), 44));
                    sdk.getClientCtx().getMainWorkerPool().submitOrdered(44, () -> null).get(5, TimeUnit.SECONDS);
                    assertFalse(first.isDone());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    releaseThird.countDown();
                    assertEquals(0, first.get(5, TimeUnit.SECONDS).longValue());
                    assertEquals(0, handle.getLastAddConfirmed());
                    assertEquals(16, handle.getLength());
                    assertEquals(3, handle.getLedgerMetadata().getAckQuorumSize());
                } finally {
                    releaseThird.countDown();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkThreeOfThreeReplacesTwiceAtTheSameStartWithoutResendingUnchangedReplicas() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5, 3)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var pool = new ArrayList<ProfileReplacementCoordinator.Target>();
            for (int target : List.of(3, 4)) {
                pool.add(new ProfileReplacementCoordinator.Target(fixture.connect(target, 4, Duration.ofSeconds(5)),
                        dataClient(fixture, target, 8, Duration.ofSeconds(5))));
            }
            for (int target : List.of(1, 3)) {
                fixture.expectedFailedDataSessions.add(target);
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            }
            CountDownLatch firstActivation = new CountDownLatch(1);
            CountDownLatch secondActivation = new CountDownLatch(1);
            CountDownLatch releaseFirst = new CountDownLatch(1);
            CountDownLatch releaseSecond = new CountDownLatch(1);
            for (int target : List.of(3, 4)) {
                AtomicInteger phase = new AtomicInteger();
                var ioField = SegmentBookieControlLog.class.getDeclaredField("io");
                ioField.setAccessible(true);
                CountDownLatch entered = target == 3 ? firstActivation : secondActivation;
                CountDownLatch release = target == 3 ? releaseFirst : releaseSecond;
                ioField.set(bookie(fixture.nodes.get(target)), new SegmentFileIo(Integer.MAX_VALUE, -1, -1,
                        false, () -> {
                            if (phase.incrementAndGet() == 2) {
                                entered.countDown();
                                await(release);
                            }
                        }));
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                        fixture.cold);
                handle.enableReplacement(coordinator, pool, fixture.descriptorBytes, 2).get(5, TimeUnit.SECONDS);
                try {
                    List<Long> callbacks = new CopyOnWriteArrayList<>();
                    var first = sdkAdd(handle, bytes(16, 8), callbacks);
                    assertTrue(firstActivation.await(5, TimeUnit.SECONDS));
                    var second = sdkAdd(handle, bytes(16, 9), callbacks);
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    releaseFirst.countDown();
                    assertTrue(secondActivation.await(5, TimeUnit.SECONDS));
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    assertEquals(1, actual.getValue().getAllEnsembles().size());
                    assertEquals(0, actual.getValue().getAllEnsembles().lastKey().longValue());
                    assertEquals(List.of("socket0:3181", "socket4:3181", "socket2:3181"),
                            actual.getValue().getAllEnsembles().lastEntry().getValue().stream()
                                    .map(Object::toString).toList());
                    assertEquals(4, fixture.reservations.read(44).snapshot().lifecycle().sequence());
                    assertNotNull(fixture.zk.exists("/socket-initial/2c/replacement-3", false));
                    assertNotNull(fixture.zk.exists("/socket-initial/2c/replacement-4", false));
                    assertEquals(5, fixture.reservations.retainedActivationTargets(44).targets().size());
                    assertEquals(2, fixture.dataResults.get(0).size());
                    assertEquals(2, fixture.dataResults.get(2).size());
                    releaseSecond.countDown();
                    assertEquals(0, first.get(10, TimeUnit.SECONDS).longValue());
                    assertEquals(1, second.get(10, TimeUnit.SECONDS).longValue());
                    assertEquals(List.of(0L, 1L), callbacks);
                    assertEquals(1, handle.getLastAddConfirmed());
                    assertEquals(32, handle.getLength());
                    var changedReplies = fixture.dataResults.get(4);
                    assertEquals(changedReplies.toString(), 2, changedReplies.stream().filter(value ->
                            value == ProfileStatus.DurableResult.APPLIED.wireValue()
                                    || value == ProfileStatus.DurableResult.ALREADY_APPLIED.wireValue()).count());
                    assertTrue(changedReplies.toString(), changedReplies.size() <= 6);
                    assertTrue(changedReplies.toString(), changedReplies.stream().allMatch(value ->
                            value == ProfileStatus.DurableResult.NONE.wireValue()
                                    || value == ProfileStatus.DurableResult.APPLIED.wireValue()
                                    || value == ProfileStatus.DurableResult.ALREADY_APPLIED.wireValue()));
                    assertEquals(2, fixture.dataResults.get(0).size());
                    assertEquals(2, fixture.dataResults.get(2).size());
                    assertTrue(handle.hasUncertainWrites());
                    assertEquals(LedgerMetadata.State.OPEN, actual.getValue().getState());
                } finally {
                    releaseFirst.countDown();
                    releaseSecond.countDown();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkReplacesTwoFailedTargetsOverRealControlChainAndResendsTheSamePendingEntry() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var pool = new ArrayList<ProfileReplacementCoordinator.Target>();
            for (int target = 3; target < 5; target++) {
                pool.add(new ProfileReplacementCoordinator.Target(fixture.connect(target, 4, Duration.ofSeconds(5)),
                        dataClient(fixture, target, 8, Duration.ofSeconds(5))));
            }
            for (int target : List.of(1, 2)) {
                fixture.expectedFailedDataSessions.add(target);
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            }
            CountDownLatch installs = new CountDownLatch(2);
            CountDownLatch activations = new CountDownLatch(2);
            CountDownLatch releaseInstall = new CountDownLatch(1);
            CountDownLatch releaseActivation = new CountDownLatch(1);
            for (int target : List.of(3, 4)) {
                AtomicInteger phase = new AtomicInteger();
                var ioField = SegmentBookieControlLog.class.getDeclaredField("io");
                ioField.setAccessible(true);
                ioField.set(bookie(fixture.nodes.get(target)), new SegmentFileIo(Integer.MAX_VALUE, -1, -1,
                        false, () -> {
                            if (phase.incrementAndGet() == 1) {
                                installs.countDown();
                                await(releaseInstall);
                            } else {
                                activations.countDown();
                                await(releaseActivation);
                            }
                        }));
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(3), Duration.ofMillis(5));
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                        fixture.cold);
                handle.enableReplacement(coordinator, pool, DESCRIPTOR_BYTES, 4).get(5, TimeUnit.SECONDS);
                try {
                    List<Long> callbacks = new CopyOnWriteArrayList<>();
                    var first = new CompletableFuture<Long>();
                    handle.asyncAddEntry(bytes(16, 8), (code, ledger, id, ctx) -> {
                        callbacks.add(id);
                        if (code == 0) {
                            first.complete(id);
                        } else {
                            first.completeExceptionally(BKException.create(code));
                        }
                    }, null);
                    assertTrue(installs.await(5, TimeUnit.SECONDS));
                    var second = new CompletableFuture<Long>();
                    handle.asyncAddEntry(bytes(16, 9), (code, ledger, id, ctx) -> {
                        callbacks.add(id);
                        if (code == 0) {
                            second.complete(id);
                        } else {
                            second.completeExceptionally(BKException.create(code));
                        }
                    }, null);
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    assertEquals(available.metadata().getVersion(), fixture.manager.readLedgerMetadata(44)
                            .get(5, TimeUnit.SECONDS).getVersion());
                    releaseInstall.countDown();
                    assertTrue(activations.await(5, TimeUnit.SECONDS));
                    assertFalse(fixture.normal(3));
                    assertFalse(fixture.normal(4));
                    assertFalse(first.isDone());
                    assertFalse(second.isDone());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    releaseActivation.countDown();
                    assertEquals(0, first.get(10, TimeUnit.SECONDS).longValue());
                    assertEquals(1, second.get(10, TimeUnit.SECONDS).longValue());
                    assertEquals(List.of(0L, 1L), callbacks);
                    assertEquals(1, handle.getLastAddConfirmed());
                    assertEquals(32, handle.getLength());
                    assertTrue(handle.hasUncertainWrites());
                    assertEquals(2, fixture.dataResults.get(0).size()); // Unchanged target was never resent.
                    var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    assertEquals(LedgerMetadata.State.OPEN, actual.getValue().getState());
                    assertEquals(List.of("socket0:3181", "socket3:3181", "socket4:3181"),
                            actual.getValue().getAllEnsembles().lastEntry().getValue().stream()
                                    .map(Object::toString).toList());
                    assertEquals(5, fixture.reservations.retainedActivationTargets(44).targets().size());
                    assertTrue(fixture.normal(3));
                    assertTrue(fixture.normal(4));
                    assertEquals(2, handle.getLength() / 16);
                } finally {
                    releaseInstall.countDown();
                    releaseActivation.countDown();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkControlUnknownStopsWriterAndReconcilesOnlyTheOriginalFrozenOperation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var pool = new ArrayList<ProfileReplacementCoordinator.Target>();
            for (int target : List.of(3, 4)) {
                pool.add(new ProfileReplacementCoordinator.Target(fixture.connect(target, 4,
                        target == 4 ? Duration.ofMillis(250) : Duration.ofSeconds(5)),
                        dataClient(fixture, target, 8, Duration.ofSeconds(5))));
            }
            fixture.faultTarget.set(4);
            fixture.replyFault.set(1); // Drop only actual ACTV; INSTALL and physical operations remain real.
            for (int target : List.of(1, 2)) {
                fixture.expectedFailedDataSessions.add(target);
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(3), Duration.ofMillis(5));
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                        fixture.cold);
                handle.enableReplacement(coordinator, pool, DESCRIPTOR_BYTES, 4).get(5, TimeUnit.SECONDS);
                try {
                    var entry = sdkAdd(handle, bytes(16, 8));
                    assertThrows(ExecutionException.class, () -> entry.get(10, TimeUnit.SECONDS));
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertTrue(handle.hasUncertainWrites());
                    var operation = handle.unresolvedReplacement();
                    assertNotNull(operation);
                    var version = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getVersion();
                    fixture.replyFault.set(0);
                    var peers = new HashMap<Integer, ProfileReplacementCoordinator.Target>();
                    for (var original : operation.targets().entrySet()) {
                        int target = original.getValue().bookie().toString().equals("socket3:3181") ? 3 : 4;
                        peers.put(original.getKey(), new ProfileReplacementCoordinator.Target(
                                fixture.connect(target, 4, Duration.ofSeconds(5)),
                                dataClient(fixture, target, 8, Duration.ofSeconds(5))));
                    }
                    var resumed = coordinator.reconnect(operation, peers);
                    assertEquals(operation.plan(), resumed.plan());
                    var resolved = coordinator.execute(resumed).get(10, TimeUnit.SECONDS);
                    assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE, resolved.status());
                    assertEquals(version, resolved.metadata().getVersion());
                    assertTrue(entry.isCompletedExceptionally());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertThrows(ExecutionException.class, () -> sdkAdd(handle, bytes(16, 9)).get(5, TimeUnit.SECONDS));
                    assertEquals(5, fixture.reservations.retainedActivationTargets(44).targets().size());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void coordinatorCancelledAdmissionCreatesNoPreparationOrMembershipChange() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                    fixture.cold);
            var target = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                    dataClient(fixture, 3, 8, Duration.ofSeconds(5)));
            var operation = coordinator.begin(available.metadata(), 0, Map.of(2, target), DESCRIPTOR_BYTES,
                    fixture.credential, bytes(16, 10), bytes(16, 11)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.CONFLICT,
                    coordinator.execute(operation, () -> false).get(5, TimeUnit.SECONDS).status());
            assertEquals(available.metadata().getVersion(),
                    fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getVersion());
            assertFalse(fixture.normal(3));
            assertEquals(3, fixture.reservations.retainedActivationTargets(44).targets().size());
            assertEquals(null, fixture.zk.exists("/socket-initial/2c/replacement-install-3", false));
        }
    }

    @Test
    public void coordinatorStoppedAfterMembershipCannotReportAvailabilityBeforeActivation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                    fixture.cold);
            var target = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                    dataClient(fixture, 3, 8, Duration.ofSeconds(5)));
            var operation = coordinator.begin(available.metadata(), 0, Map.of(2, target), DESCRIPTOR_BYTES,
                    fixture.credential, bytes(16, 12), bytes(16, 13)).get(5, TimeUnit.SECONDS);
            var stopped = coordinator.execute(operation, () -> metadata.readMembership(44).snapshot().metadata()
                    .getVersion().equals(available.metadata().getVersion())).get(10, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.CONFLICT, stopped.status());
            assertFalse(fixture.normal(3));
            var committed = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
            assertNotEquals(available.metadata().getVersion(), committed.getVersion());
            assertEquals(4, fixture.reservations.retainedActivationTargets(44).targets().size());
            var resolved = coordinator.execute(operation).get(10, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE, resolved.status());
            assertEquals(committed.getVersion(), resolved.metadata().getVersion());
            assertTrue(fixture.normal(3));
        }
    }

    private static CompletableFuture<Long> sdkAdd(ProfileLedgerHandle handle, byte[] payload) {
        return sdkAdd(handle, payload, null);
    }

    private static CompletableFuture<Long> sdkAdd(ProfileLedgerHandle handle, byte[] payload, List<Long> callbacks) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        handle.asyncAddEntry(payload, (code, ledger, entryId, ctx) -> {
            if (callbacks != null) {
                callbacks.add(entryId);
            }
            if (code == BKException.Code.OK) {
                result.complete(entryId);
            } else {
                result.completeExceptionally(BKException.create(code));
            }
        }, null);
        return result;
    }

    @Test
    public void sdkResourceBackoffReusesCoordinateAndRejectsFrontQuotaBeforeAllocation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            CountDownLatch forcing = new CountDownLatch(3);
            CountDownLatch release = new CountDownLatch(1);
            for (var storage : fixture.nodes) {
                files(storage).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    forcing.countDown();
                    await(release);
                });
            }
            Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
            List<CompletableFuture<ProfileStatus>> occupied = new ArrayList<>();
            for (int target = 0; target < 3; target++) {
                var client = dataClient(fixture, target, 1, Duration.ofSeconds(5));
                data.put(fixture.identities.get(target).bookieId(), client);
                var source = entry(fixture, 9, 16);
                try {
                    occupied.add(client.addNormal(fixture.context(), fixture.credential, 9, source));
                } finally {
                    source.release();
                }
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data,
                        1, 1024, 16, Duration.ofSeconds(3), Duration.ofMillis(25));
                try {
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    var first = sdkAdd(handle, bytes(16, 8));
                    var refused = assertThrows(ExecutionException.class,
                            () -> sdkAdd(handle, bytes(16, 9)).get(5, TimeUnit.SECONDS));
                    assertEquals(BKException.Code.TooManyRequestsException,
                            ((BKException) refused.getCause()).getCode());
                    // Keep every actual DATA-client slot occupied across several bounded retry periods.
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    assertFalse(first.isDone());
                    assertFalse(handle.hasUncertainWrites());
                    release.countDown();
                    for (var request : occupied) {
                        assertEquals(ProfileStatus.DurableResult.APPLIED, request.get(5, TimeUnit.SECONDS)
                                .durableResult());
                    }
                    assertEquals(0, first.get(5, TimeUnit.SECONDS).longValue());
                    assertEquals(1, sdkAdd(handle, bytes(16, 9)).get(5, TimeUnit.SECONDS).longValue());
                    assertEquals(1, handle.getLastAddConfirmed());
                    assertEquals(32, handle.getLength());
                    assertEquals(available.metadata().getVersion(),
                            fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getVersion());
                } finally {
                    release.countDown();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkDeadlineKeepsUnknownRemoteIoAndNeverPublishesClassicClosedMetadata() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            CountDownLatch forcing = new CountDownLatch(3);
            CountDownLatch release = new CountDownLatch(1);
            for (var storage : fixture.nodes) {
                files(storage).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    forcing.countDown();
                    await(release);
                });
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofMillis(250), Duration.ofMillis(5));
                try {
                    var request = sdkAdd(handle, bytes(16, 8));
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class, () -> request.get(5, TimeUnit.SECONDS));
                    assertTrue(handle.hasUncertainWrites());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    for (var storage : fixture.nodes) {
                        assertEquals(0, files(storage).dataIo.forces);
                    }
                    var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    assertEquals(LedgerMetadata.State.OPEN, actual.getValue().getState());
                    assertEquals(available.metadata().getVersion(), actual.getVersion());
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    release.countDown();
                    for (var storage : fixture.nodes) {
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (files(storage).dataIo.forces == 0 && System.nanoTime() < end) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        assertEquals(1, files(storage).dataIo.forces);
                    }
                    assertTrue(request.isCompletedExceptionally());
                } finally {
                    release.countDown();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkForceUnknownSurvivesSubsequentKnownNoneAndCannotBecomeAnAck() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            for (int target = 0; target < 3; target++) {
                fixture.expectedFailedDataSessions.add(target);
                files(fixture.nodes.get(target)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(3), Duration.ofMillis(25));
                try {
                    assertThrows(ExecutionException.class,
                            () -> sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS));
                    assertTrue(handle.hasUncertainWrites());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertTrue(fixture.dataResults.stream().anyMatch(results ->
                            results.contains(ProfileStatus.DurableResult.UNKNOWN.wireValue())
                                    && results.contains(ProfileStatus.DurableResult.NONE.wireValue())));
                    var actual = fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS);
                    assertEquals(LedgerMetadata.State.OPEN, actual.getValue().getState());
                    assertEquals(available.metadata().getVersion(), actual.getVersion());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void sdkRejectedSchedulerCompletesAcceptedSourcesAndLocalStop() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(3), Duration.ofMillis(5));
                sdk.getClientCtx().getScheduler().shutdown();
                try {
                    assertThrows(ExecutionException.class,
                            () -> sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS));
                    assertFalse(handle.hasUncertainWrites());
                    assertEquals(-1, handle.getLastAddConfirmed());
                    assertTrue(fixture.dataResults.stream().allMatch(List::isEmpty));
                    assertEquals(available.metadata().getVersion(),
                            fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getVersion());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
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
            var ownerField = ProfileInitialControlClient.class.getDeclaredField("socket");
            ownerField.setAccessible(true);
            Object owner = ownerField.get(client);
            var field = owner.getClass().getDeclaredField("channel");
            field.setAccessible(true);
            Channel channel = (Channel) field.get(owner);
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

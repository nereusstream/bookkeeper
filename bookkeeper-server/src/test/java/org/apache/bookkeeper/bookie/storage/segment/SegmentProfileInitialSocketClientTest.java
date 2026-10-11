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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import org.apache.bookkeeper.client.ProfileRecoveryReadHandle;
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
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
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
        // A supplied close-authority input for physical wire/driver tests; no metadata publication is inferred.
        volatile org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority closeAuthority;
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
        volatile long helloReadinessGeneration;
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
                    var repairReader = reservations.repairAuthorityReader(scope.targetBookie(), metadataContext);
                    var control = new SegmentProfileControlConfig(VALIDATOR,
                            (caller, operation) -> caller.equals(principal)
                                    && (operation == ProfileControlOperation.INSTALL
                                        || operation == ProfileControlOperation.ACTIVATE_INITIAL
                                        || operation == ProfileControlOperation.ACTIVATE_REPLACEMENT
                                        || operation == ProfileControlOperation.RECOVERY_GRANT
                                        || operation == ProfileControlOperation.RECOVERY_GRANT_CLOSE
                                        || operation == ProfileControlOperation.RECOVERY_TERMINAL_CLOSE),
                            key -> {
                                authorityReads.incrementAndGet();
                                if (key.purpose() == ProfileAuthorityPurpose.RECOVERY_GRANT_CLOSE) {
                                    var supplied = closeAuthority;
                                    return supplied != null && key.equals(
                                            org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey
                                                    .fromScope(supplied.scope()))
                                            ? new org.apache.bookkeeper.common.profile.control.ProfileControlStore
                                                .ReadResult(org.apache.bookkeeper.common.profile.control
                                                    .ProfileControlStore.ReadStatus.FOUND, supplied)
                                            : repairReader.read(key);
                                }
                                if (key.purpose() == ProfileAuthorityPurpose.RECOVERY_TERMINAL_CLOSE
                                        || key.purpose() == ProfileAuthorityPurpose.RECOVERY_GRANT) {
                                    return repairReader.read(key);
                                }
                                if (key.purpose() == ProfileAuthorityPurpose.PREPARING_INSTALL) {
                                    var initial = installReader.read(key);
                                    return initial.status()
                                            == org.apache.bookkeeper.common.profile.control.ProfileControlStore
                                            .ReadStatus.NOT_FOUND ? repairReader.read(key) : initial;
                                }
                                return key.purpose() == ProfileAuthorityPurpose.POST_MEMBERSHIP_REPLACEMENT
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
                                            131072, 64, "socket-test", helloReadinessGeneration,
                                            enableControl ? control : null,
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
                                                    int fault = replyFault.get() == 21 ? 21
                                                            : target == faultTarget.get() ? replyFault.get() : 0;
                                                    if (fault == 1
                                                            && (op == ProfileOperation.ACTIVATE_INITIAL.wireValue()
                                                                || op == ProfileOperation.ACTIVATE_REPLACEMENT
                                                                    .wireValue())) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        return;
                                                    }
                                                    if (fault == 23
                                                            && op == ProfileOperation.FENCE_LEDGER.wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
                                                        return;
                                                    }
                                                    if (fault == 22 && op == ProfileOperation.HELLO.wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
                                                        return;
                                                    }
                                                    if (fault == 21 && op == ProfileOperation.READ_ENTRY.wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
                                                        return;
                                                    }
                                                    if (fault == 20 && op == ProfileOperation.RECOVERY_TERMINAL_CLOSE
                                                            .wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
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
                                                                && op == ProfileOperation.READ_LAC.wireValue()
                                                                || fault == 19
                                                                    && op == ProfileOperation.READ_ENTRY.wireValue()) {
                                                            heldReplies.add(() -> ctx.executor().execute(
                                                                    () -> ctx.writeAndFlush(frame, promise)));
                                                            return;
                                                        }
                                                    }
                                                    if (fault == 22 && op == ProfileOperation.RECOVERY_GRANT_CLOSE
                                                            .wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
                                                        return;
                                                    }
                                                    if (fault == 18
                                                            && op == ProfileOperation.RECOVERY_GRANT_CLOSE
                                                                .wireValue()) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        ctx.close();
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
        evidenceEntry(fixture, client, id, lac, length, bytes(payload, 8));
    }

    private static void evidenceEntry(Fixture fixture, ProfileNormalDataClient client,
            long id, long lac, long length, byte[] application) throws Exception {
        var digest = DigestManager.instantiate(44, fixture.password,
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                UnpooledByteBufAllocator.DEFAULT, false);
        var source = digest.computeDigestAndPackageForSending(id, lac, length,
                Unpooled.wrappedBuffer(application), DigestManager.generateMasterKey(fixture.password), 0);
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
    public void prefixVerifierChecksEveryCoordinateAndMatchesNormalTailForA2AndA3() throws Exception {
        for (int ack : List.of(2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, ack, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    if (node < ack) {
                        for (int entry = 0; entry < 3; entry++) {
                            evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                        }
                    }
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var accepted = fixture.reservations.beginRecovery(metadata,
                        fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                try (BookKeeper sdk = sdk(fixture)) {
                    var verified = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, verified.status());
                    assertEquals(1, verified.requiredFrontier());
                    assertEquals(2, verified.prefixEntryId());
                    assertEquals(48, verified.prefixLength());
                    assertEquals(3, verified.unresolvedEntry());
                    assertTrue(verified.sources().stream().allMatch(source -> source.outcome()
                            == ProfileRecoveryReadHandle.SourceOutcome.MISSING));
                    var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var limited = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 2, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.PrefixStatus.DEFERRED, limited.status());
                    assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT, limited.reason());
                    assertEquals(1, limited.prefixEntryId());
                    assertEquals(32, limited.prefixLength());
                    assertEquals(2, limited.unresolvedEntry());
                    assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                }
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            }
        }
    }

    @Test
    public void prefixVerifierSeparatesRequiredLossUnknownAndUnrequiredSuffix() throws Exception {
        for (int mode = 0; mode < 4; mode++) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    evidenceEntry(fixture, peer, 2, -1, 48, 16);
                    if (mode == 3 && node == 2) {
                        evidenceEntry(fixture, peer, 0, -1, 16, 16);
                    }
                    if (mode != 1) {
                        persistExplicitLac(fixture, node, 2, false);
                    }
                }
                if (mode == 2) {
                    data.remove(fixture.identities.get(2).bookieId());
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var accepted = fixture.reservations.beginRecovery(metadata,
                        fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                try (BookKeeper sdk = sdk(fixture)) {
                    var result = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                    // Independent oracle at x=0: a required coordinate needs all finite sources for loss.
                    boolean required = mode != 1;
                    boolean allFinite = mode != 2;
                    assertEquals(!required ? ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL
                            : allFinite ? ProfileRecoveryCoordinator.PrefixStatus.DATA_LOSS
                                : ProfileRecoveryCoordinator.PrefixStatus.DEFERRED, result.status());
                    assertEquals(required ? 2 : -1, result.requiredFrontier());
                    assertEquals(mode == 3 ? 0 : -1, result.prefixEntryId());
                    assertEquals(mode == 3 ? 16 : 0, result.prefixLength());
                    assertEquals(mode == 3 ? 1 : 0, result.unresolvedEntry());
                    assertEquals(3, result.sources().size());
                    assertEquals(mode == 2 ? 1 : 0, result.sources().stream().filter(source -> source.outcome()
                            == ProfileRecoveryReadHandle.SourceOutcome.UNAVAILABLE).count());
                }
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            }
        }
    }

    @Test
    public void prefixVerifierDetectsValidConflictsAndContinuesPastOneCorruptSource() throws Exception {
        for (int mode = 0; mode < 4; mode++) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    evidenceEntry(fixture, peer, 0, -1, mode == 2 && node == 1 ? 17 : 16,
                            bytes(16, mode == 0 && node == 1 ? 9 : 8));
                    evidenceEntry(fixture, peer, 1, mode == 1 ? -1 : 0, mode == 1 ? 40 : 32, 16);
                }
                if (mode == 3) {
                    fixture.faultTarget.set(0);
                    fixture.replyFault.set(16);
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var accepted = fixture.reservations.beginRecovery(metadata,
                        fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                try (BookKeeper sdk = sdk(fixture)) {
                    var result = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                    assertEquals(mode == 3 ? ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL
                            : ProfileRecoveryCoordinator.PrefixStatus.QUARANTINED, result.status());
                    assertEquals(mode == 0 ? ProfileRecoveryCoordinator.PrefixReason.PAYLOAD_CONFLICT
                            : mode == 3 ? ProfileRecoveryCoordinator.PrefixReason.NORMAL_TAIL
                                : ProfileRecoveryCoordinator.PrefixReason.LENGTH_CONFLICT, result.reason());
                    if (mode == 3) {
                        assertEquals(1, result.prefixEntryId());
                        assertEquals(32, result.prefixLength());
                    }
                }
            }
        }
    }

    @Test
    public void prefixVerifierEmptyCancellationAndDeleteKeepTheActualAttemptReserved() throws Exception {
        for (int mode = 0; mode < 5; mode++) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, mode != 4)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    if (mode != 0) {
                        evidenceEntry(fixture, peer, 0, -1, 16, 16);
                    }
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var accepted = fixture.reservations.beginRecovery(metadata,
                        fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                try (BookKeeper sdk = sdk(fixture)) {
                    if (mode != 0 && mode != 4) {
                        fixture.faultTarget.set(2);
                        fixture.replyFault.set(19);
                    }
                    var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var caller = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, mode == 0 ? Integer.MAX_VALUE : 4,
                            Duration.ofSeconds(mode == 3 ? 1 : 5));
                    if (mode == 4) {
                        var unsupported = caller.get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixStatus.DEFERRED, unsupported.status());
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.LAC_UNAVAILABLE, unsupported.reason());
                        assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                        continue;
                    }
                    if (mode == 0) {
                        var result = caller.get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, result.status());
                        assertEquals(-1, result.prefixEntryId());
                        assertEquals(0, result.prefixLength());
                        continue;
                    }
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.isEmpty() && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(1, fixture.heldReplies.size());
                    if (mode == 1) {
                        assertTrue(caller.cancel(true));
                        var busy = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                                fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT, busy.reason());
                    } else if (mode == 2) {
                        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                        bytes(16, 88), bytes(32, 89)).status());
                    } else {
                        var timedOut = caller.get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.DEADLINE, timedOut.reason());
                        assertEquals(ProfileRecoveryCoordinator.PrefixStatus.DEFERRED, timedOut.status());
                        var busy = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                                fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT, busy.reason());
                    }
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                    if (mode == 2) {
                        var changed = caller.get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileRecoveryCoordinator.PrefixStatus.CONFLICT, changed.status());
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.AUTHORITY_CHANGED, changed.reason());
                    } else {
                        ProfileRecoveryCoordinator.PrefixResult next;
                        do {
                            next = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                                    fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                        } while (next.reason() == ProfileRecoveryCoordinator.PrefixReason.LIMIT
                                && System.nanoTime() < deadline);
                        assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, next.status());
                        assertEquals(mode == 1, caller.isCancelled());
                    }
                } finally {
                    fixture.releaseReplies();
                }
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            }
        }
    }

    @Test
    public void recoveryReadonlySessionUsesTheOriginalFencingReadAndDoesNotWriteOrCloseMetadata() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
            Map<BookieId, ProfileInitialControlClient> controls = new HashMap<>();
            for (int target = 0; target < 3; target++) {
                var peer = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(target).bookieId(), peer);
                controls.put(fixture.identities.get(target).bookieId(),
                        fixture.clients.get("socket" + target + ":3181"));
                if (target < 2) {
                    evidenceEntry(fixture, peer, 99, 98, 1000, 16);
                    evidenceEntry(fixture, peer, 100, 99, 1100, 100);
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            var coordinator = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                    fixture.reservations, metadata, fixture.cold);
            assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.Status.FENCED_CONTEXT,
                    coordinator.fence(entered, fixture.clients, fixture.credential).get(5, TimeUnit.SECONDS).status());
            try (BookKeeper sdk = sdk(fixture)) {
                var observed = coordinator.observe(entered, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 4).get(5, TimeUnit.SECONDS);
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator
                        .ObservationStatus.OBSERVED_BOUNDARY, observed.status());
                assertEquals(99, observed.lac().prefixEntryId());
                assertEquals(1000, observed.lac().prefixLength());
                assertEquals(1, observed.candidates().size());
                assertEquals(100, observed.candidates().get(0).entryId());
                assertEquals(1100, observed.candidates().get(0).length());
                assertEquals(101, observed.missingEntry());
                var limited = coordinator.observe(entered, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 1).get(5, TimeUnit.SECONDS);
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator
                        .ObservationStatus.DEFERRED, limited.status());
                assertEquals(1, limited.candidates().size());
                assertEquals(-1, limited.missingEntry());
                var handle = org.apache.bookkeeper.client.ProfileRecoveryReadHandle.fromContext(sdk.getClientCtx(),
                        entered, available.ready().page().installs(), data, controls);
                try {
                    var evidence = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                    assertEquals(99, evidence.prefixEntryId());
                    assertEquals(1000, evidence.prefixLength());
                    var candidate = handle.probe(100).get(5, TimeUnit.SECONDS);
                    assertEquals(org.apache.bookkeeper.client.ProfileRecoveryReadHandle.ProbeOutcome.CANDIDATE,
                            candidate.outcome());
                    assertEquals(1100, candidate.length());
                    assertEquals(100, candidate.payload().remaining());
                    var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var missing = handle.probe(101).get(5, TimeUnit.SECONDS);
                    assertEquals(org.apache.bookkeeper.client.ProfileRecoveryReadHandle.ProbeOutcome.ABSENT,
                            missing.outcome());
                    assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                    assertThrows(ExecutionException.class,
                            () -> handle.appendAsync(java.nio.ByteBuffer.wrap(bytes(16, 3))).get(5, TimeUnit.SECONDS));
                    assertEquals(-1, handle.getLastAddConfirmed());
                } finally {
                    handle.closeAsync().get(5, TimeUnit.SECONDS);
                }
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            }
        }
    }

    @Test
    public void recoveryMissingDoesNotCountASourceWithoutItsOwnDurableFence() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
            for (int target = 0; target < 3; target++) {
                data.put(fixture.identities.get(target).bookieId(),
                        dataClient(fixture, target, 4, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = org.apache.bookkeeper.client.ProfileRecoveryReadHandle.fromContext(sdk.getClientCtx(),
                        entered, available.ready().page().installs(), data,
                        Map.of(fixture.identities.get(1).bookieId(), fixture.clients.get("socket1:3181")));
                try {
                    assertThrows(ExecutionException.class, () -> handle.probe(0).get(5, TimeUnit.SECONDS));
                    assertTrue(fixture.normal(0));
                    assertFalse(fixture.normal(1));
                    assertTrue(fixture.normal(2));
                    assertEquals(-1, handle.getLastAddConfirmed());
                } finally {
                    handle.closeAsync().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void recoveryObservationRejectsAdjacentLengthConflictAndChangedContext() throws Exception {
        for (boolean changed : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                for (int target = 0; target < 3; target++) {
                    var peer = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(target).bookieId(), peer);
                    if (target < 2) {
                        evidenceEntry(fixture, peer, 99, 98, 1000, 16);
                        evidenceEntry(fixture, peer, 100, 99, 1100, 100);
                        if (!changed) {
                            evidenceEntry(fixture, peer, 101, 99, 1300, 100);
                        }
                    }
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
                var entered = fixture.reservations.beginRecovery(metadata, plan);
                var coordinator = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                        fixture.reservations, metadata, fixture.cold);
                try (BookKeeper sdk = sdk(fixture)) {
                    if (changed) {
                        fixture.faultTarget.set(0);
                        fixture.replyFault.set(17);
                    }
                    var pending = coordinator.observe(entered, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 4);
                    if (changed) {
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        assertEquals(1, fixture.heldReplies.size());
                        var busy = coordinator.observe(entered, sdk.getClientCtx(), data, fixture.clients,
                                fixture.credential, 4).get(5, TimeUnit.SECONDS);
                        assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator
                                .ObservationStatus.DEFERRED, busy.status());
                        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                fixture.reservations.cutDelete(plan.lifecycle(),
                                        bytes(16, 32), bytes(32, 32)).status());
                        fixture.replyFault.set(0);
                        fixture.releaseReplies();
                    }
                    var observed = pending.get(5, TimeUnit.SECONDS);
                    assertEquals(changed ? org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator
                            .ObservationStatus.CONFLICT
                            : org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator
                            .ObservationStatus.QUARANTINED, observed.status());
                    assertEquals(-1, observed.missingEntry());
                    assertEquals(1, observed.candidates().size());
                    assertEquals(LedgerMetadata.State.IN_RECOVERY,
                            fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
                } finally {
                    fixture.releaseReplies();
                }
            }
        }
    }

    @Test
    public void admittedRepairInstallsInactiveTargetAndGrantsActualRecoveryAddOverVerifiedSockets() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            var identity = fixture.identities.get(3);
            var scope = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL,
                    44, fixture.context().ledgerInstanceId(), ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR),
                    identity.bookieId().toString(), identity.incarnation().value(), "ledger-profile",
                    entered.context().controlGeneration(), bytes(16, 40), bytes(32, 40), null);
            var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(entered.context(), 0,
                    bytes(16, 41), 0, 1, ZKProfileReservationStore.repairEnsembleDigest(
                            entered.metadata().metadata().getValue(), 0), scope, bytes(16, 42),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 1));
            var prepared = fixture.reservations.prepareRepair(metadata, intent);
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, prepared.status());
            var control = fixture.connect(3, 4, Duration.ofSeconds(5));
            var request = ProfileControlRequest.recoveryGrant(intent.grantScope(),
                    new ProfileControlRequest.RecoveryBinding(intent.intentId(),
                            intent.intentGeneration(), intent.grantGeneration()));
            assertFalse(control.recoveryGrant(request).get(5, TimeUnit.SECONDS).durable());
            control = fixture.connect(3, 4, Duration.ofSeconds(5));
            var installed = control.install(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                    EngineProfile.SEGMENT_WAL_ENGINE, fixture.credential)).get(5, TimeUnit.SECONDS);
            assertTrue(installed.durable());
            assertFalse(fixture.normal(3));
            var receipt = org.apache.bookkeeper.discover.profile.ProfileInstallReceipt.from(
                    scope, control.hello(), installed);
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                    fixture.reservations.admitRepair(metadata, prepared.snapshot(), receipt).status());
            var granted = control.recoveryGrant(request).get(5, TimeUnit.SECONDS);
            assertTrue(granted.durable());
            assertFalse(fixture.normal(3));
            var repeatedGrant = control.recoveryGrant(request).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED, repeatedGrant.outcome());
            assertEquals(granted.localGeneration(), repeatedGrant.localGeneration());
            var data = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
            var source = entry(fixture, 0, 16);
            try {
                var copied = data.addRecovery(intent, fixture.credential, 0, source).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.DurableResult.APPLIED, copied.durableResult());
                int dataForces = files(fixture.nodes.get(3)).dataIo.forces;
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        data.addRecovery(intent, fixture.credential, 0, source)
                                .get(5, TimeUnit.SECONDS).durableResult());
                assertEquals(dataForces, files(fixture.nodes.get(3)).dataIo.forces);
            } finally {
                source.release();
            }
            assertFalse(fixture.normal(3));
            assertEquals(4, fixture.reservations.retainedActivationTargets(44).targets().size());
            assertEquals(LedgerMetadata.State.IN_RECOVERY,
                    fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
        }
    }

    @Test
    public void historicalRepairCopiesActualDataPublishesZkMembershipAndClosesThroughRealAuthority()
            throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var survivor = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            var original = entry(fixture, 0, 16);
            try {
                for (int node : List.of(0, 1, 2)) {
                    var writer = node == 0 ? survivor : dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    assertEquals(ProfileStatus.DurableResult.APPLIED, writer.addNormal(
                            fixture.context(), fixture.credential, 0, original)
                            .get(5, TimeUnit.SECONDS).durableResult());
                }
            } finally {
                original.release();
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                    fixture.cold);
            var target = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                    dataClient(fixture, 3, 4, Duration.ofSeconds(5)));
            var operation = replacement.begin(available.metadata(), 1, Map.of(2, target), DESCRIPTOR_BYTES,
                    fixture.credential, bytes(16, 20), bytes(16, 21)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                    replacement.execute(operation).get(5, TimeUnit.SECONDS).status());
            var entered = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            for (int node : List.of(0, 1, 3)) {
                assertEquals(ProfileStatus.StatusClass.OK, fixture.clients.get("socket" + node + ":3181")
                        .fence(fixture.context(), entered.context().operationId(), fixture.credential)
                        .get(5, TimeUnit.SECONDS).statusClass());
            }
            var source = survivor.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                    source.outcome());
            var identity = fixture.identities.get(4);
            var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, 44, fixture.context().ledgerInstanceId(),
                    ProfileDescriptorCodec.identityOf(fixture.descriptor, VALIDATOR),
                        identity.bookieId().toString(),
                    identity.incarnation().value(),
                    "ledger-profile", entered.context().controlGeneration(), bytes(16, 40), bytes(32, 40), null);
            var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(entered.context(), 0,
                    bytes(16, 41), 0, 1, ZKProfileReservationStore.repairEnsembleDigest(
                            entered.metadata().metadata().getValue(), 0), install, bytes(16, 42),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 0));
            var prepared = fixture.reservations.prepareRepair(metadata, intent);
            var control = fixture.connect(4, 4, Duration.ofSeconds(5));
            var installed = control.install(ProfileControlRequest.install(install, DESCRIPTOR_BYTES,
                    EngineProfile.SEGMENT_WAL_ENGINE, fixture.credential)).get(5, TimeUnit.SECONDS);
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, fixture.reservations.admitRepair(metadata,
                    prepared.snapshot(), org.apache.bookkeeper.discover.profile.ProfileInstallReceipt.from(
                            install, control.hello(), installed)).status());
            assertTrue(control.recoveryGrant(ProfileControlRequest.recoveryGrant(intent.grantScope(),
                    new ProfileControlRequest.RecoveryBinding(intent.intentId(), intent.intentGeneration(),
                            intent.grantGeneration()))).get(5, TimeUnit.SECONDS).durable());
            var data = dataClient(fixture, 4, 4, Duration.ofSeconds(5));
            var copied = Unpooled.wrappedBuffer(source.entry());
            try {
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        data.addRecovery(intent, fixture.credential, 0, copied)
                                .get(5, TimeUnit.SECONDS).durableResult());
                var committed = fixture.reservations.commitRepair(metadata, prepared.snapshot(), entered.metadata(),
                        bytes(16, 43));
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, committed.status());
                assertEquals(identity.bookieId(), committed.metadata().metadata().getValue()
                        .getAllEnsembles().get(0L).get(1));
                assertEquals(entered.context(), fixture.reservations.readRecovery(metadata, 44).context());
                assertTrue(control.recoveryClose(ProfileControlRequest.recoveryClose(committed.closeScope(),
                        new ProfileControlRequest.RecoveryBinding(intent.intentId(), intent.intentGeneration(),
                                intent.grantGeneration()), intent.grantOperationId()))
                        .get(5, TimeUnit.SECONDS).durable());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        data.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
                assertFalse(fixture.normal(4));
                assertEquals(5, fixture.reservations.retainedActivationTargets(44).targets().size());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            } finally {
                copied.release();
            }
        }
    }

    private static final class LostCloseReplyClient extends ZooKeeper {
        private boolean lose = true;

        LostCloseReplyClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var ops = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(ops::add);
            boolean drop = lose && ops.stream().anyMatch(op -> op.getType() == ZooDefs.OpCode.setData);
            if (drop) {
                lose = false;
            }
            super.multi(ops, (code, path, ignored, results) -> callback.processResult(
                    drop && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()
                        ? org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue() : code,
                    path, context, results), context);
        }
    }

    @Test
    public void cancelledEmptyBoundaryCloseKeepsActualAttemptUntilTerminalReply() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 4, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var entered = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            try (BookKeeper sdk = sdk(fixture)) {
                var proof = coordinator.verifyPrefix(entered, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, proof.status());
                assertEquals(-1, proof.prefixEntryId());
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        fixture.reservations.selectPrefix(metadata, proof).status());
                fixture.replyFault.set(20);
                var pending = coordinator.finishRecovery(entered.context(), fixture.clients);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, fixture.heldReplies.size());
                assertTrue(pending.cancel(false));
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.DEFERRED,
                        coordinator.finishRecovery(entered.context(), fixture.clients)
                            .get(5, TimeUnit.SECONDS).status());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                fixture.replyFault.set(0);
                fixture.releaseReplies();
                // Only actual child termination permits a new attempt; cancellation itself does not.
                ProfileRecoveryCoordinator.CompletionResult result;
                do {
                    result = coordinator.finishRecovery(entered.context(), fixture.clients).get(5, TimeUnit.SECONDS);
                    if (result.status() == ProfileRecoveryCoordinator.CompletionStatus.DEFERRED) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                } while (result.status() == ProfileRecoveryCoordinator.CompletionStatus.DEFERRED
                        && System.nanoTime() < until);
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE, result.status());
                assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                assertEquals(-1, fixture.manager.readLedgerMetadata(44).get().getValue().getLastEntryId());
                assertEquals(0, fixture.manager.readLedgerMetadata(44).get().getValue().getLength());
            }
        }
    }

    @Test
    public void wholePointRecoveryClosesEmptyAndNonemptyFromOpenWithOriginalSdkAndQuorum() throws Exception {
        for (int ack : List.of(2, 3)) {
            for (int mode : List.of(0, 1, 2, 3, 4)) {
                try (Fixture fixture = new Fixture(false, false, true, mode >= 3 ? 4 : 3, ack, true, true);
                        BookKeeper sdk = sdk(fixture)) {
                    fixture.connectAll(Duration.ofSeconds(5));
                    var initial = fixture.initialize().get(5, TimeUnit.SECONDS);
                    var data = new HashMap<BookieId, ProfileNormalDataClient>();
                    for (int node = 0; node < 3; node++) {
                        var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                        data.put(fixture.identities.get(node).bookieId(), peer);
                        if (mode != 0) {
                            for (long entry = 0; entry < 3; entry++) {
                                evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                            }
                        }
                    }
                    var allData = new HashMap<>(data);
                    var controls = new HashMap<>(fixture.clients);
                    if (mode == 2) {
                        data.remove(fixture.identities.get(2).bookieId());
                        controls.remove(fixture.identities.get(2).bookieId().toString());
                    }
                    var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    if (mode >= 3) {
                        var nextControl = fixture.connect(3, 4, Duration.ofSeconds(5));
                        var nextData = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
                        data.put(fixture.identities.get(3).bookieId(), nextData);
                        allData.put(fixture.identities.get(3).bookieId(), nextData);
                        controls.put(fixture.identities.get(3).bookieId().toString(), nextControl);
                        var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                                fixture.cold);
                        var operation = replacement.begin(initial.metadata(), 3,
                                Map.of(2, new ProfileReplacementCoordinator.Target(nextControl, nextData)),
                                fixture.descriptorBytes, fixture.credential, bytes(16, 20), bytes(16, 21))
                                    .get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                                replacement.execute(operation).get(5, TimeUnit.SECONDS).status());
                        if (mode == 3) {
                            for (int node : List.of(0, 1, 3)) {
                                evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), 3, 2, 64, 16);
                            }
                        }
                    }
                    var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                    var denied = coordinator.recover(44, bytes(16, 30), sdk.getClientCtx(), fixture.descriptorBytes,
                            VALIDATOR, ProtectedProfileCredential.of(1, bytes(20, 99)), data, controls, 6,
                            Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.CONFLICT, denied.status());
                    assertEquals(ProfileRecoveryCoordinator.PrefixReason.PERMISSION, denied.reason());
                    assertEquals(LedgerMetadata.State.OPEN,
                            fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                    var before = fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles();
                    var recovered = coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                            VALIDATOR, fixture.credential, data, controls, 6, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS);
                    if (ack == 3 && mode == 2) {
                        assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.DEFERRED, recovered.status());
                        assertNull(recovered.publication());
                        assertEquals(LedgerMetadata.State.IN_RECOVERY,
                                fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                        recovered = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                                .recover(44, bytes(16, 98), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                        fixture.credential, allData, fixture.clients, 6, Duration.ofSeconds(30))
                                    .get(30, TimeUnit.SECONDS);
                    }
                    assertEquals("ack=" + ack + " mode=" + mode + " reason=" + recovered.reason(),
                            ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED, recovered.status());
                    assertNotNull(recovered.publication());
                    assertEquals(mode == 0 ? -1 : mode == 3 ? 3 : 2, recovered.prefixEntryId());
                    assertEquals(mode == 0 ? 0 : mode == 3 ? 64 : 48, recovered.prefixLength());
                    var closed = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertEquals(LedgerMetadata.State.CLOSED, closed.getState());
                    assertEquals(before, closed.getAllEnsembles());
                    assertEquals(recovered.prefixEntryId(), closed.getLastEntryId());
                    assertEquals(recovered.prefixLength(), closed.getLength());
                    var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                    var again = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                            .recover(44, bytes(16, 99), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                    fixture.credential, data, controls, 6, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED, again.status());
                    assertEquals(recovered.context(), again.context());
                    assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                    var health = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential,
                            data, controls, 6, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                    assertEquals(mode == 2 ? ProfileRecoveryCoordinator.ReplicaHealthStatus.DEFERRED
                            : ProfileRecoveryCoordinator.ReplicaHealthStatus.FULLY_REPLICATED, health.status());
                    assertEquals(mode != 2, health.proof() != null);
                    var allHealth = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential,
                            allData, fixture.clients, 6, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.FULLY_REPLICATED, allHealth.status());
                    assertNotNull(allHealth.proof());
                    assertEquals(closed.getLastEntryId(), allHealth.proof().snapshot().closed().metadata()
                            .metadata().getValue().getLastEntryId());
                    assertEquals(ack == 2 && mode == 2
                            ? List.of(forces.get(0), forces.get(1), forces.get(2) + 1) : forces,
                            fixture.nodeIo.stream().map(io -> io.forces).toList());
                }
            }
        }
    }

    @Test
    public void wholeRecoveryCancelAndDeadlineKeepActualGateAndResumeFixedPublicationWithoutRecopy() throws Exception {
        for (boolean deadline : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                var dataForces = new ArrayList<Integer>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    for (long entry = 0; entry < 3; entry++) {
                        evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                    }
                    dataForces.add(files(fixture.nodes.get(node)).dataIo.forces);
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                fixture.faultTarget.set(0);
                fixture.replyFault.set(22);
                var pending = coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                        VALIDATOR, fixture.credential, data, fixture.clients, 6,
                        deadline ? Duration.ofSeconds(2) : Duration.ofSeconds(30));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertFalse(fixture.heldReplies.isEmpty());
                if (deadline) {
                    var deferred = pending.get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.DEFERRED, deferred.status());
                    assertEquals(ProfileRecoveryCoordinator.PrefixReason.DEADLINE, deferred.reason());
                    assertEquals(2, deferred.prefixEntryId());
                } else {
                    assertTrue(pending.cancel(false));
                }
                var busy = coordinator.recover(44, bytes(16, 99), sdk.getClientCtx(), fixture.descriptorBytes,
                        VALIDATOR, fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30)).get();
                assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.DEFERRED, busy.status());
                assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT, busy.reason());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                assertEquals(1, org.apache.bookkeeper.discover.profile.ProfileRepairCommit.head(
                        fixture.manager.readLedgerMetadata(44).get().getValue().getCustomMetadata()
                            .get(org.apache.bookkeeper.discover.profile.ProfileRepairCommit.KEY)).size());
                var context = fixture.reservations.readRecovery(metadata, 44).context();
                fixture.replyFault.set(0);
                fixture.releaseReplies();
                ProfileRecoveryCoordinator.RecoveryResult recovered;
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                do {
                    recovered = coordinator.recover(44, bytes(16, 98), sdk.getClientCtx(), fixture.descriptorBytes,
                            VALIDATOR, fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS);
                    if (recovered.reason() == ProfileRecoveryCoordinator.PrefixReason.LIMIT) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                } while (recovered.reason() == ProfileRecoveryCoordinator.PrefixReason.LIMIT
                        && System.nanoTime() < until);
                assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED, recovered.status());
                assertEquals(context, recovered.context());
                for (int node = 0; node < 3; node++) {
                    assertEquals(dataForces.get(node).intValue(), files(fixture.nodes.get(node)).dataIo.forces);
                }
            }
        }
    }

    @Test
    public void wholeRecoveryPreservesLimitAndRequiredHoleOutcomesWithoutFalseClose() throws Exception {
        for (boolean loss : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    for (long entry = loss ? 2 : 0; entry < 3; entry++) {
                        evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                    }
                    if (loss) {
                        persistExplicitLac(fixture, node, 2, false);
                    }
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var recovered = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                        .recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                fixture.credential, data, fixture.clients, loss ? 6 : 1, Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS);
                assertEquals(loss ? ProfileRecoveryCoordinator.RecoveryStatus.DATA_LOSS
                        : ProfileRecoveryCoordinator.RecoveryStatus.DEFERRED, recovered.status());
                assertEquals(loss ? ProfileRecoveryCoordinator.PrefixReason.REQUIRED_HOLE
                        : ProfileRecoveryCoordinator.PrefixReason.LIMIT, recovered.reason());
                assertNull(recovered.publication());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            }
        }
    }

    @Test
    public void closedRepairCopiesToNewTargetsPreservingOriginalCloseAcrossRepeatedMembershipChanges()
            throws Exception {
        for (int ack : List.of(2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 5, ack, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    for (long entry = 0; entry < 3; entry++) {
                        evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                    }
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                var closed = coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                        VALIDATOR, fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED, closed.status());
                var original = fixture.manager.readLedgerMetadata(44).get().getValue();
                byte[] cut = original.getCustomMetadata().get(
                        org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CLOSE_KEY);
                assertNotNull(cut);
                for (int target : List.of(3, 4)) {
                    var accepted = fixture.reservations.readClosedForOpen(metadata, 44).snapshot();
                    assertNotNull(accepted);
                    var control = fixture.connect(target, 4, Duration.ofSeconds(5));
                    var peer = dataClient(fixture, target, 4, Duration.ofSeconds(5));
                    var identity = fixture.identities.get(target);
                    var context = accepted.context();
                    var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                            ProfileAuthorityPurpose.PREPARING_INSTALL, 44, context.ledger().ledgerInstanceId(),
                            ProfileDescriptorCodec.identityOf(fixture.descriptor, VALIDATOR),
                            identity.bookieId().toString(),
                            identity.incarnation().value(), "closed-repair", context.controlGeneration(),
                            bytes(16, 40 + target), bytes(32, 40 + target), null);
                    var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(context, target,
                            bytes(16, 50 + target), 0, target == 3 ? 2 : 1,
                            ZKProfileReservationStore.repairEnsembleDigest(
                                    accepted.closed().metadata().metadata().getValue(), 0), install,
                            bytes(16, 60 + target),
                            new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
                    var prepared = fixture.reservations.prepareRepair(metadata, intent).snapshot();
                    assertNotNull(prepared);
                    data.put(identity.bookieId(), peer);
                    if (target == 3) {
                        fixture.faultTarget.set(target);
                        fixture.replyFault.set(18);
                    }
                    var copied = coordinator.repairClosedFragment(accepted, prepared, sdk.getClientCtx(),
                            fixture.descriptorBytes, fixture.credential, data, fixture.clients, bytes(16, 70 + target))
                                .get(10, TimeUnit.SECONDS);
                    if (target == 3) {
                        assertEquals(ProfileRecoveryCoordinator.RepairStatus.DEFERRED, copied.status());
                        assertEquals(3, copied.copiedEntries());
                        int forces = files(fixture.nodes.get(target)).dataIo.forces;
                        fixture.replyFault.set(0);
                        fixture.connect(target, 4, Duration.ofSeconds(5));
                        var fresh = fixture.reservations.readClosedForOpen(metadata, 44).snapshot();
                        copied = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                                .repairClosedFragment(fresh, prepared, sdk.getClientCtx(), fixture.descriptorBytes,
                                        fixture.credential, data, fixture.clients, bytes(16, 70 + target))
                                    .get(10, TimeUnit.SECONDS);
                        assertEquals(0, copied.copiedEntries());
                        assertEquals(forces, files(fixture.nodes.get(target)).dataIo.forces);
                    }
                    assertEquals(ProfileRecoveryCoordinator.RepairStatus.REPAIRED_LAST_FRAGMENT, copied.status());
                    assertEquals(target == 3 ? 0 : 3, copied.copiedEntries());
                    var result = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertEquals(LedgerMetadata.State.CLOSED, result.getState());
                    assertEquals(2, result.getLastEntryId());
                    assertEquals(48, result.getLength());
                    assertEquals(original.getCToken(), result.getCToken());
                    assertArrayEquals(cut, result.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CLOSE_KEY));
                    assertEquals(identity.bookieId(), result.getAllEnsembles().get(0L).get(target == 3 ? 2 : 1));
                    assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                            peer.readEntry(fixture.context(), 2).get(5, TimeUnit.SECONDS).outcome());
                    assertFalse(fixture.normal(target));
                    assertEquals(context, fixture.reservations.readClosedForOpen(metadata, 44).snapshot().context());
                    assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                            fixture.reservations.readRecovery(metadata, 44).outcome());
                    var health = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential,
                            data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.FULLY_REPLICATED, health.status());
                    assertNotNull(health.proof());
                    assertEquals(context, health.proof().snapshot().context());
                }
                var config = files(fixture.nodes.get(4)).config();
                fixture.nodes.get(4).close();
                try (var replay = new IsolatedSegmentStorage(config, fixture.identities.get(4),
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                        new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                        new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                    replay.restart();
                    assertTrue(replay.profileReadVisible(fixture.context(), 2));
                    assertFalse(replay.profileReadVisible(fixture.context(), 3));
                    assertEquals(config.bytesPerFile(), java.nio.file.Files.size(config.directory().resolve("data-0")));
                }
            }
        }
    }

    @Test
    public void closedReplicaHealthFindsHistoricalHoleAndRequiresAllCopiesAfterRepair() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var initial = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 4; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 4, Duration.ofSeconds(5)));
                if (node < 2) {
                    for (long entry = 0; entry < 3; entry++) {
                        evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), entry,
                                entry - 1, (entry + 1) * 16, 16);
                    }
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                    fixture.cold);
            var nextControl = fixture.connect(3, 4, Duration.ofSeconds(5));
            var operation = replacement.begin(initial.metadata(), 3,
                    Map.of(2, new ProfileReplacementCoordinator.Target(nextControl,
                            data.get(fixture.identities.get(3).bookieId()))), fixture.descriptorBytes,
                    fixture.credential, bytes(16, 20), bytes(16, 21)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                    replacement.execute(operation).get(5, TimeUnit.SECONDS).status());
            for (int node : List.of(0, 1, 3)) {
                evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), 3, 2, 64, 16);
            }
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                    coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                            fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30))
                        .get(30, TimeUnit.SECONDS).status());
            var health = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                    fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.UNDERREPLICATED, health.status());
            assertEquals(0, health.unresolvedEntry());
            assertNull(health.proof());
            assertEquals(3, health.sources().size());
            assertEquals(1, health.sources().stream().filter(source -> source.outcome()
                    == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.MISSING).count());
            var missing = new HashMap<>(data);
            missing.remove(fixture.identities.get(2).bookieId());
            assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.DEFERRED,
                    coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, missing,
                            fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS).status());
            assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT,
                    coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                            fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS).reason());
            assertEquals(ProfileRecoveryCoordinator.PrefixReason.PERMISSION,
                    coordinator.verifyClosedReplicas(44, sdk.getClientCtx(),
                            ProtectedProfileCredential.of(1, bytes(20, 99)), data, fixture.clients, 4,
                            Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS).reason());
            var accepted = fixture.reservations.readClosedForOpen(metadata, 44).snapshot();
            var context = accepted.context();
            fixture.connect(4, 4, Duration.ofSeconds(5));
            var identity = fixture.identities.get(4);
            data.put(identity.bookieId(), dataClient(fixture, 4, 4, Duration.ofSeconds(5)));
            var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, 44, context.ledger().ledgerInstanceId(),
                    ProfileDescriptorCodec.identityOf(fixture.descriptor, VALIDATOR), identity.bookieId().toString(),
                    identity.incarnation().value(), "closed-repair", context.controlGeneration(),
                    bytes(16, 44), bytes(32, 44), null);
            var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(context, 4,
                    bytes(16, 54), 0, 2, ZKProfileReservationStore.repairEnsembleDigest(
                            accepted.closed().metadata().metadata().getValue(), 0), install, bytes(16, 64),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
            var prepared = fixture.reservations.prepareRepair(metadata, intent).snapshot();
            assertNotNull(prepared);
            assertEquals(ProfileRecoveryCoordinator.RepairStatus.REPAIRED_HISTORICAL_FRAGMENT,
                    coordinator.repairClosedFragment(accepted, prepared, sdk.getClientCtx(), fixture.descriptorBytes,
                            fixture.credential, data, fixture.clients, bytes(16, 74))
                        .get(30, TimeUnit.SECONDS).status());
            health = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                    fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.FULLY_REPLICATED, health.status());
            assertNotNull(health.proof());
            assertEquals(3, health.proof().snapshot().closed().metadata().metadata().getValue().getLastEntryId());
            fixture.faultTarget.set(4);
            fixture.replyFault.set(16); // One actual TLS reply has a corrupt SDK entry digest.
            var corrupt = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                    fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.UNDERREPLICATED, corrupt.status());
            assertNull(corrupt.proof());
            fixture.replyFault.set(0);
            fixture.readAllowed.set(false);
            var denied = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                    fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.CONFLICT, denied.status());
            assertEquals(ProfileRecoveryCoordinator.PrefixReason.PERMISSION, denied.reason());
        }
    }

    @Test
    public void closedReplicaHealthCancellationAndAuthorityChangesNeverDeliverProof() throws Exception {
        for (int mode : List.of(0, 1, 2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    evidenceEntry(fixture, peer, 0, -1, 16, 16);
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                        coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                fixture.replyFault.set(21);
                var pending = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                        fixture.clients, 2, mode == 1 ? Duration.ofSeconds(2) : Duration.ofSeconds(30));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                if (mode == 0) {
                    assertTrue(pending.cancel(false));
                } else if (mode == 1) {
                    var timed = pending.get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.PrefixReason.DEADLINE, timed.reason());
                    assertNull(timed.proof());
                } else if (mode == 2) {
                    assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                            fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                    bytes(16, 101), bytes(32, 102)).status());
                } else {
                    String path = ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44);
                    byte[] original = fixture.zk.getData(path, false, null);
                    org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, path);
                    fixture.zk.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT,
                        coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                                fixture.clients, 2, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS).reason());
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                if (mode >= 2) {
                    var rejected = pending.get(5, TimeUnit.SECONDS);
                    assertEquals(mode == 2 ? ProfileRecoveryCoordinator.ReplicaHealthStatus.DELETED
                            : ProfileRecoveryCoordinator.ReplicaHealthStatus.QUARANTINED, rejected.status());
                    assertNull(rejected.proof());
                } else {
                    ProfileRecoveryCoordinator.ReplicaHealthResult resumed = null;
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (System.nanoTime() < until) {
                        resumed = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                                fixture.clients, 2, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
                        if (resumed.proof() != null) {
                            break;
                        }
                        assertEquals(ProfileRecoveryCoordinator.PrefixReason.LIMIT, resumed.reason());
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNotNull(resumed);
                    assertNotNull(resumed.proof());
                }
            }
        }
    }

    @Test
    public void closedHealthProofClearsOnlyMatchingMarkerThroughItsIssuerAndOriginalLock() throws Exception {
        for (int mode : List.of(0, 1, 2, 3, 4, 5, 6, 7, 8)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    evidenceEntry(fixture, peer, 0, -1, 16, 16);
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                        coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                var conf = new ServerConfiguration().setMetadataServiceUri("zk+longhierarchical://"
                        + fixture.cluster.getZooKeeperConnectString() + "/socket-ledgers");
                try (var foreign = mode == 4 ? new org.apache.zookeeper.ZooKeeper(
                            fixture.cluster.getZooKeeperConnectString(), 10000, ignored -> { }) : null;
                        var tasks = new org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager(conf,
                            mode == 4 ? foreign : fixture.zk)) {
                    tasks.enableProfileCleanupGuards();
                    tasks.markLedgerUnderreplicated(44, "missing:3181");
                    assertEquals(44, tasks.pollLedgerToRereplicate());
                    var proof = coordinator.verifyClosedReplicas(44, sdk.getClientCtx(), fixture.credential, data,
                            fixture.clients, 2, mode == 6 ? Duration.ofSeconds(2) : Duration.ofSeconds(30),
                            mode == 8 ? null : tasks)
                                .get(5, TimeUnit.SECONDS).proof();
                    assertNotNull(proof);
                    if (mode == 1) {
                        tasks.markLedgerUnderreplicated(44, "additional:3181");
                    } else if (mode == 2) {
                        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                        bytes(16, 101), bytes(32, 102)).status());
                    } else if (mode == 5) {
                        String path = ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44);
                        byte[] bytes = fixture.zk.getData(path, false, null);
                        org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, path);
                        fixture.zk.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else if (mode == 6) {
                        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
                    } else if (mode == 7) {
                        tasks.releaseUnderreplicatedLedger(44);
                        assertEquals(44, tasks.pollLedgerToRereplicate());
                    }
                    var consumer = mode == 3
                            ? new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                            : coordinator;
                    var result = consumer.completeReplication(proof, tasks).get(5, TimeUnit.SECONDS);
                    assertEquals(mode == 0 ? ProfileRecoveryCoordinator.MarkerCleanupStatus.CLEARED
                            : mode == 2 ? ProfileRecoveryCoordinator.MarkerCleanupStatus.DELETED
                                : mode == 5 ? ProfileRecoveryCoordinator.MarkerCleanupStatus.QUARANTINED
                                    : ProfileRecoveryCoordinator.MarkerCleanupStatus.CONFLICT, result);
                    if (mode == 0) {
                        assertEquals(-1, tasks.pollLedgerToRereplicate());
                        assertFalse(tasks.isLedgerBeingReplicated(44));
                        assertEquals(ProfileRecoveryCoordinator.MarkerCleanupStatus.CONFLICT,
                                coordinator.completeReplication(proof, tasks).get(5, TimeUnit.SECONDS));
                    } else {
                        assertTrue(tasks.isLedgerBeingReplicated(44));
                    }
                }
            }
        }
    }

    @Test
    public void replicationWorkerUsesActualProfileRecoveryCopyHealthAndGuardedMarkerRemoval() throws Exception {
        for (int ack : List.of(2, 3)) {
            for (int mode : List.of(0, 1, 2, 3, 4)) {
                try (Fixture fixture = new Fixture(false, false, true, mode == 4 ? 5 : 4, ack, true, true);
                        BookKeeper sdk = sdk(fixture)) {
                    fixture.connectAll(Duration.ofSeconds(5));
                    var initialized = fixture.initialize().get(5, TimeUnit.SECONDS);
                    var all = new HashMap<BookieId, ProfileNormalDataClient>();
                    for (int node = 0; node < 3; node++) {
                        var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                        all.put(fixture.identities.get(node).bookieId(), peer);
                        for (long entry = 0; mode != 3 && !(mode == 4 && node == 2) && entry < 3; entry++) {
                            evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                        }
                    }
                    var metadata = new ProfileLedgerMetadataStore(fixture.zk,
                            (AbstractZkLedgerManager) fixture.manager, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                    if (mode == 4) {
                        var control = fixture.connect(3, 4, Duration.ofSeconds(5));
                        var peer = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
                        all.put(fixture.identities.get(3).bookieId(), peer);
                        var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata,
                                VALIDATOR, fixture.cold);
                        var plan = replacement.begin(initialized.metadata(), 3,
                                Map.of(2, new ProfileReplacementCoordinator.Target(control, peer)),
                                fixture.descriptorBytes, fixture.credential, bytes(16, 20), bytes(16, 21))
                                    .get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                                replacement.execute(plan).get(5, TimeUnit.SECONDS).status());
                        for (int member : List.of(0, 1, 3)) {
                            evidenceEntry(fixture, all.get(fixture.identities.get(member).bookieId()), 3, 2, 64, 16);
                        }
                    }
                    if (mode == 0 || mode == 2 || mode == 4) {
                        assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                                coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                                        VALIDATOR, fixture.credential, all, fixture.clients, 6, Duration.ofSeconds(30))
                                    .get(30, TimeUnit.SECONDS).status());
                    }
                    var controls = new HashMap<>(fixture.clients);
                    if (mode == 1 || mode == 2) {
                        all.remove(fixture.identities.get(2).bookieId());
                        controls.remove(fixture.identities.get(2).bookieId().toString());
                        fixture.connect(3, 4, Duration.ofSeconds(5));
                        controls.put(fixture.identities.get(3).bookieId().toString(),
                                fixture.clients.get("socket3:3181"));
                        all.put(fixture.identities.get(3).bookieId(), dataClient(fixture, 3, 4, Duration.ofSeconds(5)));
                    }
                    if (mode == 4) {
                        controls.put("socket4:3181", fixture.connect(4, 4, Duration.ofSeconds(5)));
                        all.put(fixture.identities.get(4).bookieId(), dataClient(fixture, 4, 4, Duration.ofSeconds(5)));
                    }
                    var conf = profileWorkerConfiguration(fixture);
                    var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                            all, controls, 6, 8, Duration.ofSeconds(30));
                    try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                        monitor.markLedgerUnderreplicated(44, "socket2:3181");
                        var worker = new org.apache.bookkeeper.replication.ReplicationWorker(conf, sdk,
                                org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                        worker.start();
                        try {
                            awaitProfileWorkerStarted(worker);
                            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                            while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                            }
                            assertNull("ack=" + ack + " mode=" + mode, monitor.getLedgerUnreplicationInfo(44));
                            var value = fixture.manager.readLedgerMetadata(44).get().getValue();
                            assertEquals(LedgerMetadata.State.CLOSED, value.getState());
                            assertEquals(mode == 3 ? -1 : mode == 4 ? 3 : 2, value.getLastEntryId());
                            assertEquals(mode == 3 ? 0 : mode == 4 ? 64 : 48, value.getLength());
                            if (mode == 1 || mode == 2 || mode == 4) {
                                int target = mode == 4 ? 4 : 3;
                                assertEquals(fixture.identities.get(target).bookieId(),
                                        value.getAllEnsembles().get(0L).get(2));
                                assertFalse(fixture.normal(target));
                                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec
                                        .Outcome.FOUND, all.get(fixture.identities.get(target).bookieId())
                                            .readEntry(fixture.context(), 2).get(5, TimeUnit.SECONDS).outcome());
                            }
                        } finally {
                            worker.shutdown();
                        }
                    }
                }
            }
        }
    }

    @Test
    public void replicationWorkerRetainsDisabledMalformedMissingMetadataAndUnsupportedEmptyTasks() throws Exception {
        for (int mode : List.of(0, 1, 2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 4, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                if (mode == 3) {
                    assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                                    VALIDATOR, fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS).status());
                    data.remove(fixture.identities.get(2).bookieId());
                    // No unused candidate is supplied: this remains a bounded DEFERRED task.
                }
                var controls = new HashMap<>(fixture.clients);
                if (mode == 3) {
                    controls.remove("socket2:3181");
                }
                if (mode == 1) {
                    var before = fixture.manager.readLedgerMetadata(44).get();
                    var custom = new java.util.TreeMap<>(before.getValue().getCustomMetadata());
                    custom.put(org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink.KEY, new byte[] {1});
                    fixture.manager.writeLedgerMetadata(44, LedgerMetadataBuilder.from(before.getValue())
                            .withCToken(before.getValue().getCToken()).withCustomMetadata(custom).build(),
                            before.getVersion()).get();
                } else if (mode == 2) {
                    org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk,
                            ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44));
                }
                var stats = new org.apache.bookkeeper.test.TestStatsProvider().getStatsLogger("worker");
                var conf = profileWorkerConfiguration(fixture);
                try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                    monitor.markLedgerUnderreplicated(44, "socket2:3181");
                    var worker = mode == 0 ? new org.apache.bookkeeper.replication.ReplicationWorker(conf, stats)
                            : new org.apache.bookkeeper.replication.ReplicationWorker(conf, sdk, stats, coordinator,
                                new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                        data, controls, 3, 8, Duration.ofSeconds(30)));
                    worker.start();
                    try {
                        awaitProfileWorkerStarted(worker);
                        String counter = mode == 0 ? "ProfileWorkerDisabled" : mode == 1 ? "ProfileQUARANTINED"
                                : mode == 2 ? "ProfileMetadataUnavailable" : "ProfileDEFERRED";
                        var errors = stats.scope(org.apache.bookkeeper.replication.ReplicationStats.REPLICATE_EXCEPTION)
                                .getCounter(counter);
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (errors.get() == 0 && System.nanoTime() < until) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        assertTrue("mode=" + mode, errors.get() > 0);
                        assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                        if (mode == 3) {
                            assertEquals(0, fixture.nodeIo.get(3).forces);
                        }
                    } finally {
                        worker.shutdown();
                    }
                    assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                }
            }
        }
    }

    @Test
    public void replicationWorkerShutdownWaitsForActualReadDrainAndLeavesBorrowedPoolsUsable() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), peer);
                evidenceEntry(fixture, peer, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                    fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                    data, fixture.clients, 3, 8, Duration.ofSeconds(30));
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                fixture.replyFault.set(21);
                var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                worker.start();
                awaitProfileWorkerStarted(worker);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                var stopping = stopper.submit(worker::shutdown);
                try {
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> stopping.get(150, TimeUnit.MILLISECONDS));
                    assertTrue(monitor.isLedgerBeingReplicated(44));
                    assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                } finally {
                    fixture.replyFault.set(0);
                    fixture.heldReplies.forEach(Runnable::run);
                }
                stopping.get(5, TimeUnit.SECONDS);
                assertFalse(monitor.isLedgerBeingReplicated(44));
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertFalse(sdk.getClientCtx().isClientClosed());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        data.values().iterator().next().readEntry(fixture.context(), 0)
                            .get(5, TimeUnit.SECONDS).outcome());
                var resumed = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                resumed.start();
                try {
                    awaitProfileWorkerStarted(resumed);
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                } finally {
                    resumed.shutdown();
                }
            } finally {
                stopper.shutdownNow();
            }
        }
    }

    @Test
    public void replicationWorkerSelfStopDrainsTheActualAttemptWithoutJoiningItself() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), peer);
                evidenceEntry(fixture, peer, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                    fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                    data, fixture.clients, 3, 8, Duration.ofSeconds(30));
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                fixture.replyFault.set(21);
                var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                worker.start();
                awaitProfileWorkerStarted(worker);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                var threadField = worker.getClass().getDeclaredField("workerThread");
                threadField.setAccessible(true);
                Thread actualWorker = (Thread) threadField.get(worker);
                actualWorker.interrupt();
                var stopping = stopper.submit(() -> {
                    actualWorker.join();
                    return actualWorker.isAlive();
                });
                try {
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> stopping.get(150, TimeUnit.MILLISECONDS));
                    assertTrue(monitor.isLedgerBeingReplicated(44));
                    assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                } finally {
                    fixture.replyFault.set(0);
                    fixture.heldReplies.forEach(Runnable::run);
                }
                assertFalse(stopping.get(5, TimeUnit.SECONDS));
                assertFalse(monitor.isLedgerBeingReplicated(44));
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertFalse(sdk.getClientCtx().isClientClosed());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        data.values().iterator().next().readEntry(fixture.context(), 0)
                            .get(5, TimeUnit.SECONDS).outcome());
                var resumed = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                resumed.start();
                try {
                    awaitProfileWorkerStarted(resumed);
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                } finally {
                    resumed.shutdown();
                }
            } finally {
                stopper.shutdownNow();
            }
        }
    }

    @Test
    public void replicationWorkerInterruptedAndConcurrentShutdownKeepsOriginalTaskUntilActualDrain() throws Exception {
        for (int mode : List.of(0, 1)) {
            final int scenario = mode;
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    evidenceEntry(fixture, peer, 0, -1, 16, 16);
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                        fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        data, fixture.clients, 3, 8, Duration.ofSeconds(30));
                var stopper = java.util.concurrent.Executors.newFixedThreadPool(2);
                try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                    monitor.markLedgerUnderreplicated(44, "socket2:3181");
                    fixture.replyFault.set(21);
                    var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                            profileWorkerConfiguration(fixture),
                            sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                    worker.start();
                    awaitProfileWorkerStarted(worker);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(3, fixture.heldReplies.size());
                    var caller = new AtomicReference<Thread>();
                    var stopping = stopper.submit(() -> {
                        caller.set(Thread.currentThread());
                        if (scenario == 0) {
                            Thread.currentThread().interrupt();
                        }
                        worker.shutdown();
                        return Thread.currentThread().isInterrupted();
                    });
                    java.util.concurrent.Future<Boolean> concurrent = null;
                    try {
                        assertThrows(java.util.concurrent.TimeoutException.class,
                                () -> stopping.get(150, TimeUnit.MILLISECONDS));
                        assertTrue(monitor.isLedgerBeingReplicated(44));
                        assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                        if (scenario == 1) {
                            caller.get().interrupt();
                            concurrent = stopper.submit(() -> {
                                worker.shutdown();
                                return Thread.currentThread().isInterrupted();
                            });
                            var waiting = concurrent;
                            assertThrows(java.util.concurrent.TimeoutException.class,
                                    () -> waiting.get(150, TimeUnit.MILLISECONDS));
                        }
                        assertThrows(java.util.concurrent.TimeoutException.class,
                                () -> stopping.get(150, TimeUnit.MILLISECONDS));
                        assertTrue(monitor.isLedgerBeingReplicated(44));
                    } finally {
                        fixture.replyFault.set(0);
                        fixture.heldReplies.forEach(Runnable::run);
                    }
                    assertTrue(stopping.get(5, TimeUnit.SECONDS));
                    if (concurrent != null) {
                        assertFalse(concurrent.get(5, TimeUnit.SECONDS));
                    }
                    assertFalse(monitor.isLedgerBeingReplicated(44));
                    assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                    assertFalse(sdk.getClientCtx().isClientClosed());
                    assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                            data.values().iterator().next().readEntry(fixture.context(), 0)
                                .get(5, TimeUnit.SECONDS).outcome());
                    var resumed = new org.apache.bookkeeper.replication.ReplicationWorker(
                            profileWorkerConfiguration(fixture),
                            sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, coordinator, options);
                    resumed.start();
                    try {
                        awaitProfileWorkerStarted(resumed);
                        until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        assertNull(monitor.getLedgerUnreplicationInfo(44));
                    } finally {
                        resumed.shutdown();
                    }
                } finally {
                    stopper.shutdownNow();
                }
            }
        }
    }

    @Test
    public void replicationWorkerRepairsEmptyLedgerAndEmptyLastWithoutDataOrGrantForA2AndA3() throws Exception {
        for (int ack : List.of(2, 3)) {
            for (int mode : List.of(0, 1, 2, 3)) {
                try (Fixture fixture = new Fixture(false, false, true, mode >= 2 ? 5 : 4, ack, true, true);
                        BookKeeper sdk = sdk(fixture)) {
                    fixture.connectAll(Duration.ofSeconds(5));
                    var initial = fixture.initialize().get(5, TimeUnit.SECONDS);
                    var data = new HashMap<BookieId, ProfileNormalDataClient>();
                    for (int node = 0; node < 3; node++) {
                        var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                        data.put(fixture.identities.get(node).bookieId(), peer);
                        if (mode >= 2) {
                            for (long entry = 0; entry < 3; entry++) {
                                evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                            }
                        }
                    }
                    var metadata = new ProfileLedgerMetadataStore(fixture.zk,
                            (AbstractZkLedgerManager) fixture.manager, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    if (mode >= 2) {
                        var control = fixture.connect(3, 4, Duration.ofSeconds(5));
                        var peer = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
                        data.put(fixture.identities.get(3).bookieId(), peer);
                        var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata,
                                VALIDATOR, fixture.cold);
                        var plan = replacement.begin(initial.metadata(), 3,
                                Map.of(2, new ProfileReplacementCoordinator.Target(control, peer)),
                                fixture.descriptorBytes, fixture.credential, bytes(16, 20), bytes(16, 21))
                                    .get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                                replacement.execute(plan).get(5, TimeUnit.SECONDS).status());
                    }
                    if (mode == 1 || mode == 3) {
                        assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                                new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                                    .recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes,
                                        VALIDATOR, fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30))
                                    .get(30, TimeUnit.SECONDS).status());
                    }
                    int lost = mode >= 2 ? 3 : 2;
                    int target = mode >= 2 ? 4 : 3;
                    data.remove(fixture.identities.get(lost).bookieId());
                    var controls = new HashMap<>(fixture.clients);
                    controls.remove(fixture.identities.get(lost).bookieId().toString());
                    controls.put(fixture.identities.get(target).bookieId().toString(),
                            fixture.connect(target, 4, Duration.ofSeconds(5)));
                    data.put(fixture.identities.get(target).bookieId(),
                            dataClient(fixture, target, 4, Duration.ofSeconds(5)));
                    int dataForces = files(fixture.nodes.get(target)).dataIo.forces;
                    try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                        monitor.markLedgerUnderreplicated(44, fixture.identities.get(lost).bookieId().toString());
                        var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                                profileWorkerConfiguration(fixture), sdk,
                                org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                                new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold),
                                new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                        data, controls, 6, 8, Duration.ofSeconds(30)));
                        worker.start();
                        try {
                            awaitProfileWorkerStarted(worker);
                            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                            }
                            assertNull("ack=" + ack + " mode=" + mode, monitor.getLedgerUnreplicationInfo(44));
                            var value = fixture.manager.readLedgerMetadata(44).get().getValue();
                            assertEquals(LedgerMetadata.State.CLOSED, value.getState());
                            assertEquals(mode >= 2 ? 2 : -1, value.getLastEntryId());
                            assertEquals(mode >= 2 ? 48 : 0, value.getLength());
                            assertEquals(fixture.identities.get(target).bookieId(),
                                    value.getAllEnsembles().lastEntry().getValue().get(2));
                            assertEquals(dataForces, files(fixture.nodes.get(target)).dataIo.forces);
                            assertFalse(fixture.normal(target));
                            var child = "/socket-initial/2c/repair-";
                            // Locate the fixed intent through the actual standard commit head.
                            var head = org.apache.bookkeeper.discover.profile.ProfileRepairCommit.head(
                                    value.getCustomMetadata().get(
                                            org.apache.bookkeeper.discover.profile.ProfileRepairCommit.KEY));
                            var empty = head.stream().filter(ref -> {
                                try {
                                    byte[] bytes = fixture.zk.getData(child + ref.slot(), false, null);
                                    return org.apache.bookkeeper.discover.profile.ProfileRepairIntent.decode(
                                            Arrays.copyOfRange(bytes, 16, bytes.length)).empty();
                                } catch (Exception bad) {
                                    throw new AssertionError(bad);
                                }
                            }).findFirst().orElseThrow();
                            assertNotNull(fixture.zk.exists(child + empty.slot() + "/empty-boundary", false));
                        } finally {
                            worker.shutdown();
                        }
                    }
                    var config = files(fixture.nodes.get(target)).config();
                    fixture.nodes.get(target).close();
                    try (var replay = new IsolatedSegmentStorage(config, fixture.identities.get(target),
                            new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                            new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                    2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                            new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                        replay.restart();
                        assertTrue(bookie(replay).hasTerminalExclusion(fixture.context(), mode >= 2 ? 3 : 0));
                        assertFalse(replay.profileReadVisible(fixture.context(), mode >= 2 ? 3 : 0));
                        assertFalse(bookie(replay).normalAdmission(fixture.context(),
                                DigestManager.generateMasterKey(fixture.password)));
                        assertEquals(config.bytesPerFile(),
                                java.nio.file.Files.size(config.directory().resolve("data-0")));
                    }
                }
            }
        }
    }

    @Test
    public void emptyRepairForceFailureRetainsMembershipAndOriginalTask() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(),
                        dataClient(fixture, node, 4, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                    coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                            fixture.credential, data, fixture.clients, 4, Duration.ofSeconds(30))
                        .get(30, TimeUnit.SECONDS).status());
            data.remove(fixture.identities.get(2).bookieId());
            data.put(fixture.identities.get(3).bookieId(), dataClient(fixture, 3, 4, Duration.ofSeconds(5)));
            var controls = new HashMap<>(fixture.clients);
            controls.remove("socket2:3181");
            controls.put("socket3:3181", fixture.connect(3, 4, Duration.ofSeconds(5)));
            var field = SegmentBookieControlLog.class.getDeclaredField("io");
            field.setAccessible(true);
            var failing = new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false);
            field.set(bookie(fixture.nodes.get(3)), failing);
            try (var tasks = new org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager(
                    profileWorkerConfiguration(fixture), fixture.zk)) {
                tasks.enableProfileCleanupGuards();
                tasks.markLedgerUnderreplicated(44, "socket2:3181");
                assertEquals(44, tasks.pollLedgerToRereplicate());
                var attempt = coordinator.beginWorker(44, sdk.getClientCtx(),
                        new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                data, controls, 4, 8, Duration.ofSeconds(30)), tasks);
                assertEquals(ProfileRecoveryCoordinator.WorkerStatus.DEFERRED,
                        attempt.completion().get(10, TimeUnit.SECONDS).status());
                assertEquals(2, failing.forces);
                assertNotNull(fixture.zk.exists("/socket-initial/2c/repair-0/admitted", false));
                assertNull(fixture.zk.exists("/socket-initial/2c/repair-0/empty-boundary", false));
                assertEquals(fixture.identities.get(2).bookieId(),
                        fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                assertNotNull(tasks.getLedgerUnreplicationInfo(44));
                assertTrue(tasks.isLedgerBeingReplicated(44));
                assertEquals(0, files(fixture.nodes.get(3)).dataIo.forces);
                attempt.releaseTask();
                assertFalse(tasks.isLedgerBeingReplicated(44));
            }
        }
    }

    @Test
    public void emptyRepairLostBoundaryReplyResumesOriginalOperationAndDeleteNeverPublishes() throws Exception {
        for (boolean deleting : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 4, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                        .recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                fixture.credential, data, fixture.clients, 4, Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS);
                data.remove(fixture.identities.get(2).bookieId());
                data.put(fixture.identities.get(3).bookieId(), dataClient(fixture, 3, 4, Duration.ofSeconds(5)));
                var controls = new HashMap<>(fixture.clients);
                controls.remove("socket2:3181");
                controls.put("socket3:3181", fixture.connect(3, 4, Duration.ofSeconds(5)));
                fixture.faultTarget.set(3);
                fixture.replyFault.set(20);
                try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                    monitor.markLedgerUnderreplicated(44, "socket2:3181");
                    var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                    var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                            data, controls, 4, 8, Duration.ofSeconds(30));
                    var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                            profileWorkerConfiguration(fixture), sdk,
                            org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                            coordinator, options);
                    worker.start();
                    awaitProfileWorkerStarted(worker);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(1, fixture.heldReplies.size());
                    String intentPath = "/socket-initial/2c/repair-0";
                    byte[] planned = fixture.zk.getData(intentPath, false, null);
                    assertNotNull(fixture.zk.exists(intentPath + "/admitted", false));
                    assertNull(fixture.zk.exists(intentPath + "/empty-boundary", false));
                    assertEquals(fixture.identities.get(2).bookieId(),
                            fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                    int forces = files(fixture.nodes.get(3)).dataIo.forces;
                    if (deleting) {
                        assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                        bytes(16, 101), bytes(32, 102)).status());
                        fixture.replyFault.set(0);
                        fixture.heldReplies.forEach(Runnable::run);
                        until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (monitor.isLedgerBeingReplicated(44) && System.nanoTime() < until) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                        worker.shutdown();
                        assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                        assertNull(fixture.zk.exists(intentPath + "/empty-boundary", false));
                        assertEquals(fixture.identities.get(2).bookieId(),
                                fixture.manager.readLedgerMetadata(44).get().getValue()
                                    .getAllEnsembles().get(0L).get(2));
                    } else {
                        var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
                        try {
                            var stopping = stopper.submit(worker::shutdown);
                            assertThrows(java.util.concurrent.TimeoutException.class,
                                    () -> stopping.get(150, TimeUnit.MILLISECONDS));
                            assertTrue(monitor.isLedgerBeingReplicated(44));
                            fixture.replyFault.set(0);
                            fixture.heldReplies.forEach(Runnable::run);
                            stopping.get(5, TimeUnit.SECONDS);
                        } finally {
                            stopper.shutdownNow();
                        }
                        var resumed = new org.apache.bookkeeper.replication.ReplicationWorker(
                                profileWorkerConfiguration(fixture), sdk,
                                org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                                new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold), options);
                        resumed.start();
                        try {
                            awaitProfileWorkerStarted(resumed);
                            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                            while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                            }
                            assertNull(monitor.getLedgerUnreplicationInfo(44));
                            assertArrayEquals(planned, fixture.zk.getData(intentPath, false, null));
                            assertNotNull(fixture.zk.exists(intentPath + "/empty-boundary", false));
                            assertEquals(forces, files(fixture.nodes.get(3)).dataIo.forces);
                        } finally {
                            resumed.shutdown();
                        }
                        org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, intentPath + "/empty-boundary");
                        assertEquals(ZKProfileReservationStore.ReadOnlyStatus.QUARANTINED,
                                fixture.reservations.readClosedForOpen(metadata, 44).status());
                        byte[] bound = new byte[124];
                        fixture.zk.create(intentPath + "/empty-boundary", bound, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        assertEquals(ZKProfileReservationStore.ReadOnlyStatus.QUARANTINED,
                                fixture.reservations.readClosedForOpen(metadata, 44).status());
                    }
                }
            }
        }
    }

    @Test
    public void replicationWorkerNewCoordinatorResumesPublishedCloseWithoutRecopyAndRepairsLaterLoss()
            throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 6, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), peer);
                for (long entry = 0; entry < 3; entry++) {
                    evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var initial = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                    .recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                            fixture.credential, data, fixture.clients, 6, Duration.ofSeconds(30))
                        .get(30, TimeUnit.SECONDS);
            assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED, initial.status());
            var original = fixture.manager.readLedgerMetadata(44).get().getValue();
            data.remove(fixture.identities.get(2).bookieId());
            for (int target : List.of(3, 4, 5)) {
                fixture.connect(target, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(target).bookieId(),
                        dataClient(fixture, target, 4, Duration.ofSeconds(5)));
            }
            var controls = new HashMap<>(fixture.clients);
            controls.remove("socket2:3181");
            fixture.faultTarget.set(3);
            fixture.replyFault.set(18);
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                        new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold),
                        new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                data, controls, 6, 1, Duration.ofSeconds(30)));
                worker.start();
                int forces;
                try {
                    awaitProfileWorkerStarted(worker);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (!fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L)
                            .get(2).equals(fixture.identities.get(3).bookieId()) && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertEquals(fixture.identities.get(3).bookieId(),
                            fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                    assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                    forces = files(fixture.nodes.get(3)).dataIo.forces;
                } finally {
                    worker.shutdown();
                }
                fixture.replyFault.set(16); // A published close resume must not consume the new-copy budget.
                controls.put("socket3:3181", fixture.connect(3, 4, Duration.ofSeconds(5)));
                var resumed = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture),
                        sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                        new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold),
                        new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                data, controls, 6, 1, Duration.ofSeconds(30)));
                resumed.start();
                try {
                    awaitProfileWorkerStarted(resumed);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                    assertEquals(forces, files(fixture.nodes.get(3)).dataIo.forces);
                } finally {
                    resumed.shutdown();
                }
                fixture.replyFault.set(0);
                data.remove(fixture.identities.get(4).bookieId());
                controls.remove("socket4:3181");
                monitor.markLedgerUnderreplicated(44, "socket4:3181");
                var replacement = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture), sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                        new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold),
                        new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                data, controls, 6, 1, Duration.ofSeconds(30)));
                replacement.start();
                try {
                    awaitProfileWorkerStarted(replacement);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                    var value = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertEquals(fixture.identities.get(5).bookieId(), value.getAllEnsembles().get(0L).get(2));
                    assertEquals(original.getLastEntryId(), value.getLastEntryId());
                    assertEquals(original.getLength(), value.getLength());
                    assertArrayEquals(original.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CLOSE_KEY),
                            value.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.CLOSE_KEY));
                    assertEquals(6, org.apache.bookkeeper.discover.profile.ProfileRepairCommit.head(
                            value.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRepairCommit.KEY))
                                .size());
                } finally {
                    replacement.shutdown();
                }
            }
        }
    }

    private static ServerConfiguration profileWorkerConfiguration(Fixture fixture) {
        var conf = new ServerConfiguration().setMetadataServiceUri("zk+longhierarchical://"
                + fixture.cluster.getZooKeeperConnectString() + "/socket-ledgers");
        conf.setRwRereplicateBackoffMs(20);
        return conf;
    }

    private static void awaitProfileWorkerStarted(org.apache.bookkeeper.replication.ReplicationWorker worker) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!worker.isRunning() && System.nanoTime() < until) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertTrue(worker.isRunning());
    }

    private static final class HoldReadonlyMetadataClient extends org.apache.zookeeper.ZooKeeper {
        final java.util.concurrent.atomic.AtomicInteger observations = new java.util.concurrent.atomic.AtomicInteger();
        final CountDownLatch held = new CountDownLatch(1);
        volatile String metadataPath;
        volatile Runnable release;

        HoldReadonlyMetadataClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void getData(String path, boolean watch, org.apache.zookeeper.AsyncCallback.DataCallback callback,
                Object context) {
            super.getData(path, watch, (code, name, ignored, data, stat) -> {
                if (path.equals(metadataPath) && observations.incrementAndGet() == 4) {
                    release = () -> callback.processResult(code, name, context, data, stat);
                    held.countDown();
                } else {
                    callback.processResult(code, name, context, data, stat);
                }
            }, context);
        }
    }

    @Test
    public void readonlyOpenRechecksDeleteAfterHandleConstructionAndRejectsMetadataRecreation() throws Exception {
        for (int mode : List.of(0, 1, 2)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                    BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 4, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var accepted = fixture.reservations.beginRecovery(metadata,
                        fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                var proof = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        fixture.reservations.selectPrefix(metadata, proof).status());
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE,
                        coordinator.finishRecovery(accepted.context(), fixture.clients)
                            .get(5, TimeUnit.SECONDS).status());
                var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                String path = ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44);
                if (mode == 1) {
                    byte[] original = fixture.zk.getData(path, false, null);
                    org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, path);
                    fixture.zk.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            org.apache.zookeeper.CreateMode.PERSISTENT);
                    assertEquals(ZKProfileReservationStore.ReadOnlyStatus.QUARANTINED,
                            fixture.reservations.readClosedForOpen(metadata, 44).status());
                    assertEquals(BKException.Code.MetadataVersionException, BKException.getExceptionCode(assertThrows(
                            java.util.concurrent.ExecutionException.class, () -> coordinator.openReadOnly(44,
                                    sdk.getClientCtx(), fixture.password, data, 3).get(5, TimeUnit.SECONDS))));
                } else {
                    var watcher = new ZooKeeperWatcherBase(10000, false);
                    try (var held = new HoldReadonlyMetadataClient(
                            fixture.cluster.getZooKeeperConnectString(), watcher)) {
                        held.metadataPath = path;
                        var heldMetadata = new ProfileLedgerMetadataStore(held,
                                (AbstractZkLedgerManager) fixture.manager,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                        var heldStore = new ZKProfileReservationStore(held, "/socket-initial",
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                Duration.ofSeconds(5));
                        var heldCoordinator = new ProfileRecoveryCoordinator(heldStore, heldMetadata, fixture.cold);
                        var pending = heldCoordinator.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 3);
                        assertTrue(held.held.await(5, TimeUnit.SECONDS));
                        if (mode == 0) {
                            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                                    fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                            bytes(16, 101), bytes(32, 102)).status());
                            held.release.run();
                            assertEquals(BKException.Code.NoSuchLedgerExistsException,
                                    BKException.getExceptionCode(assertThrows(
                                    java.util.concurrent.ExecutionException.class,
                                            () -> pending.get(5, TimeUnit.SECONDS))));
                        } else {
                            assertTrue(pending.cancel(false));
                            assertEquals(BKException.Code.TooManyRequestsException,
                                    BKException.getExceptionCode(assertThrows(
                                    java.util.concurrent.ExecutionException.class,
                                        () -> heldCoordinator.openReadOnly(44, sdk.getClientCtx(),
                                                fixture.password, data, 3)
                                            .get(5, TimeUnit.SECONDS))));
                            held.release.run();
                            boolean reopened = false;
                            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                            while (!reopened && System.nanoTime() < until) {
                                try (var reader = heldCoordinator.openReadOnly(44, sdk.getClientCtx(), fixture.password,
                                        data, 3).get(5, TimeUnit.SECONDS)) {
                                    assertEquals(-1, reader.getLastAddConfirmed());
                                    reopened = true;
                                } catch (java.util.concurrent.ExecutionException busy) {
                                    if (BKException.getExceptionCode(busy)
                                            != BKException.Code.TooManyRequestsException) {
                                        throw busy;
                                    }
                                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                                }
                            }
                            assertTrue(reopened);
                        }
                    }
                }
                assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                assertEquals(LedgerMetadata.State.CLOSED,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            }
        }
    }

    private static void verifyClosedReadonly(Fixture fixture, ProfileLedgerMetadataStore metadata,
            Map<BookieId, ProfileNormalDataClient> data, long prefix, long length) throws Exception {
        try (BookKeeper reopened = sdk(fixture)) {
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            assertEquals(BKException.Code.UnauthorizedAccessException, BKException.getExceptionCode(assertThrows(
                    java.util.concurrent.ExecutionException.class, () -> coordinator.openReadOnly(44,
                            reopened.getClientCtx(), bytes(8, 99), data, 3).get(5, TimeUnit.SECONDS))));
            var controlForces = fixture.nodeIo.stream().map(io -> io.forces).toList();
            var stored = fixture.manager.readLedgerMetadata(44).get();
            try (var reader = coordinator.openReadOnly(44, reopened.getClientCtx(), fixture.password, data, 3)
                    .get(5, TimeUnit.SECONDS)) {
                assertEquals(prefix, reader.getLastAddConfirmed());
                assertEquals(length, reader.getLength());
                assertEquals(prefix, reader.readLastConfirmed());
                if (prefix >= 0) {
                    try (var entries = reader.readAsync(0, prefix).get(5, TimeUnit.SECONDS)) {
                        for (long entry = 0; entry <= prefix; entry++) {
                            assertArrayEquals(bytes(16, 8), entries.getEntry(entry).getEntryBytes());
                        }
                    }
                    var legacy = reader.readUnconfirmedEntries(0, prefix);
                    while (legacy.hasMoreElements()) {
                        assertArrayEquals(bytes(16, 8), legacy.nextElement().getEntry());
                    }
                }
                assertEquals(BKException.Code.ReadException, BKException.getExceptionCode(assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> reader.readUnconfirmedAsync(prefix + 1, prefix + 2).get(5, TimeUnit.SECONDS))));
                assertEquals(BKException.Code.IllegalOpException, BKException.getExceptionCode(assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> reader.batchReadAsync(0, 10, 1024).get(5, TimeUnit.SECONDS))));
                assertEquals(BKException.Code.IllegalOpException, assertThrows(BKException.class,
                        () -> reader.addEntry(bytes(16, 8))).getCode());
                assertEquals(prefix, reader.getLastAddConfirmed());
                assertEquals(length, reader.getLength());
            }
            if (prefix >= 0) {
                try (var reader = coordinator.openReadOnly(44, reopened.getClientCtx(), fixture.password, data, 1)
                        .get(5, TimeUnit.SECONDS)) {
                    fixture.replyFault.set(21);
                    var pending = reader.readAsync(0, 0);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertFalse(fixture.heldReplies.isEmpty());
                    assertTrue(pending.cancel(false));
                    assertEquals(BKException.Code.TooManyRequestsException, BKException.getExceptionCode(assertThrows(
                            java.util.concurrent.ExecutionException.class,
                            () -> reader.readAsync(0, 0).get(5, TimeUnit.SECONDS))));
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                    boolean completed = false;
                    while (!completed && System.nanoTime() < until) {
                        try (var entries = reader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                            assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
                            completed = true;
                        } catch (java.util.concurrent.ExecutionException busy) {
                            if (BKException.getExceptionCode(busy) != BKException.Code.TooManyRequestsException) {
                                throw busy;
                            }
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                    }
                    assertTrue(completed);
                } finally {
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                }
            }
            assertEquals(stored.getVersion(), fixture.manager.readLedgerMetadata(44).get().getVersion());
            assertEquals(controlForces, fixture.nodeIo.stream().map(io -> io.forces).toList());
        }
    }

    @Test
    public void emptyLastFragmentClosesMatchingBoundaryWithoutGrantOrCopy() throws Exception {
        for (int ack : List.of(2, 3)) {
            for (boolean historical : List.of(false, true)) {
                closeEmptyLastFragment(ack, historical);
            }
        }
    }

    private void closeEmptyLastFragment(int ack, boolean historical) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, historical ? 4 : 3, ack, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < fixture.nodes.size(); node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 4, Duration.ofSeconds(5)));
            }
            if (historical) {
                for (int node : List.of(0, 1, 2)) {
                    for (long entry = 0; entry < 3; entry++) {
                        evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()),
                                entry, entry - 1, (entry + 1) * 16, 16);
                    }
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            if (historical) {
                var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                        fixture.cold);
                var target = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                        data.get(fixture.identities.get(3).bookieId()));
                var pending = replacement.begin(available.metadata(), 3, Map.of(2, target), fixture.descriptorBytes,
                        fixture.credential, bytes(16, 20), bytes(16, 21)).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                        replacement.execute(pending).get(5, TimeUnit.SECONDS).status());
            }
            int suffixNode = historical ? 3 : 2;
            long prefix = historical ? 2 : -1;
            long length = historical ? 48 : 0;
            long suffix = prefix + 2;
            var tailPeer = data.get(fixture.identities.get(suffixNode).bookieId());
            evidenceEntry(fixture, tailPeer, suffix, -1, (suffix + 1) * 16, 16);
            var entered = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            try (BookKeeper sdk = sdk(fixture)) {
                var proof = coordinator.verifyPrefix(entered, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 6, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, proof.status());
                assertEquals(prefix, proof.prefixEntryId());
                assertEquals(length, proof.prefixLength());
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        fixture.reservations.selectPrefix(metadata, proof).status());
                var plan = fixture.reservations.closingPlan(metadata, entered.context()).plan();
                assertNotNull(plan);
                assertEquals(3, plan.targets().size());
                var dataForces = fixture.nodes.stream().map(node -> {
                    try {
                        return files(node).dataIo.forces;
                    } catch (Exception failure) {
                        throw new RuntimeException(failure);
                    }
                }).toList();
                var receipts = new ArrayList<ZKProfileReservationStore.TerminalAck>();
                for (var target : plan.targets()) {
                    assertNull(target.intent());
                    assertNull(target.scope().exactRange());
                    assertNotNull(target.terminal());
                    var peer = fixture.clients.get(target.scope().targetBookie());
                    var reply = peer.terminalClose(target.request()).get(5, TimeUnit.SECONDS);
                    assertTrue(reply.durable());
                    receipts.add(new ZKProfileReservationStore.TerminalAck(target, peer.hello(), reply));
                }
                assertEquals(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN,
                        fixture.reservations.closeRecovery(metadata, plan,
                                List.of(receipts.get(0), receipts.get(0))).outcome());
                var legacy = receipts.stream().map(evidence -> {
                    var hello = evidence.hello();
                    return new ZKProfileReservationStore.TerminalAck(evidence.target(),
                            new org.apache.bookkeeper.common.profile.wire.ProfileHello.Server(hello.activeEngine(),
                                    hello.storageIncarnation(), hello.readinessGeneration(), hello.bookieId(),
                                    hello.capabilities().stream().filter(cap -> !cap.equals(
                                        org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec
                                            .TERMINAL_BOUNDARY_CAPABILITY)).toList()), evidence.result());
                }).toList();
                assertEquals(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN,
                        fixture.reservations.closeRecovery(metadata, plan, legacy).outcome());
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.DEFERRED,
                        coordinator.finishRecovery(entered.context(), Map.of(
                                plan.targets().get(0).scope().targetBookie(),
                                fixture.clients.get(plan.targets().get(0).scope().targetBookie())))
                                    .get(5, TimeUnit.SECONDS).status());
                ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
                try (LostCloseReplyClient lost = new LostCloseReplyClient(fixture.cluster.getZooKeeperConnectString(),
                        watcher)) {
                    var lostMetadata = new ProfileLedgerMetadataStore(lost, (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var lostReservations = new ZKProfileReservationStore(lost, "/socket-initial",
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var closed = new ProfileRecoveryCoordinator(lostReservations, lostMetadata, fixture.cold)
                            .finishRecovery(entered.context(), fixture.clients).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE,
                            closed.status());
                    assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, closed.publication().outcome());
                }
                var finished = fixture.manager.readLedgerMetadata(44).get().getValue();
                assertEquals(LedgerMetadata.State.CLOSED, finished.getState());
                assertEquals(prefix, finished.getLastEntryId());
                assertEquals(length, finished.getLength());
                assertEquals(0, org.apache.bookkeeper.discover.profile.ProfileRepairCommit.head(
                        finished.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileRepairCommit.KEY))
                            .size());
                for (int node = 0; node < fixture.nodes.size(); node++) {
                    assertEquals(dataForces.get(node).intValue(), files(fixture.nodes.get(node)).dataIo.forces);
                }
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.REFUSED,
                        tailPeer.readEntry(fixture.context(), suffix).get().outcome());
                var controlForces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE,
                        new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                            .finishRecovery(entered.context(), fixture.clients).get(5, TimeUnit.SECONDS).status());
                assertEquals(controlForces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                verifyClosedReadonly(fixture, metadata, data, prefix, length);
                var restartConfig = files(fixture.nodes.get(suffixNode)).config();
                fixture.nodes.get(suffixNode).close();
                try (var replay = new IsolatedSegmentStorage(restartConfig, fixture.identities.get(suffixNode),
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                        new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                        new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                    replay.restart();
                    assertFalse(replay.profileReadVisible(fixture.context(), suffix));
                    assertEquals(restartConfig.bytesPerFile(), java.nio.file.Files.size(
                            restartConfig.directory().resolve("data-0")));
                }
            }
        }
    }

    @Test
    public void selectedLastFragmentWritesBackToTheExistingMembersWithoutChangingMembership() throws Exception {
        closeExistingMembers(2);
        closeExistingMembers(3);
    }

    private void closeExistingMembers(int ack) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, ack, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), peer);
                for (int entry = 0; entry < 3; entry++) {
                    evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                }
                if (node == 2) {
                    // Physical entry4 after hole3 is an unrequired suffix, never part of selected P2.
                    evidenceEntry(fixture, peer, 4, 1, 80, 16);
                    assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                            peer.readEntry(fixture.context(), 4).get().outcome());
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var entered = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            try (BookKeeper sdk = sdk(fixture)) {
                var proof = coordinator.verifyPrefix(entered, sdk.getClientCtx(), data, fixture.clients,
                        fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        fixture.reservations.selectPrefix(metadata, proof).status());
                for (int member = 0; member < 3; member++) {
                    var current = fixture.reservations.readRecovery(metadata, 44);
                    var original = fixture.scopes.get(member);
                    var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                            ProfileAuthorityPurpose.PREPARING_INSTALL, 44, original.ledgerInstanceId(),
                            original.descriptorIdentity(), original.targetBookie(), original.targetStorageIncarnation(),
                            original.authorityDomain(), current.context().controlGeneration(), bytes(16, 60 + member),
                            original.publicSemanticPayloadIdentity(), null);
                    var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(
                            current.context(), member,
                            bytes(16, 70 + member), 0, member, ZKProfileReservationStore.repairEnsembleDigest(
                                    current.metadata().metadata().getValue(), 0), install, bytes(16, 80 + member),
                            new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
                    var prepared = fixture.reservations.prepareRepair(metadata, intent).snapshot();
                    assertNotNull(prepared);
                    int before = files(fixture.nodes.get(member)).dataIo.forces;
                    var result = coordinator.repairLastFragment(current, prepared, sdk.getClientCtx(),
                            fixture.descriptorBytes, fixture.credential, data, fixture.clients, bytes(16, 90 + member))
                                .get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.RepairStatus.REPAIRED_LAST_FRAGMENT, result.status());
                    assertEquals(3, result.copiedEntries());
                    assertEquals(before, files(fixture.nodes.get(member)).dataIo.forces);
                    assertEquals(entered.metadata().metadata().getValue().getAllEnsembles(),
                            fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles());
                    assertFalse(fixture.normal(member));
                }
                assertEquals(3, org.apache.bookkeeper.discover.profile.ProfileRepairCommit.head(
                        fixture.manager.readLedgerMetadata(44).get().getValue().getCustomMetadata()
                            .get(org.apache.bookkeeper.discover.profile.ProfileRepairCommit.KEY)).size());
                assertEquals(entered.context(), fixture.reservations.readRecovery(metadata, 44).context());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.REFUSED,
                        data.get(fixture.identities.get(2).bookieId()).readEntry(fixture.context(), 4).get().outcome());
                assertEquals(2, data.get(fixture.identities.get(2).bookieId())
                        .readLac(fixture.context()).get().sourceEntryId());
                var closing = fixture.reservations.closingPlan(metadata, entered.context()).plan();
                assertNotNull(closing);
                var acknowledgments = new ArrayList<ZKProfileReservationStore.TerminalAck>();
                for (var target : closing.targets().subList(0, 2)) {
                    var peer = fixture.clients.get(target.scope().targetBookie());
                    var result = peer.recoveryClose(ProfileControlRequest.recoveryClose(target.scope(),
                            new ProfileControlRequest.RecoveryBinding(target.intent().intentId(),
                                    target.intent().intentGeneration(), target.intent().grantGeneration()),
                            target.intent().grantOperationId())).get();
                    assertTrue(result.durable());
                    acknowledgments.add(new ZKProfileReservationStore.TerminalAck(target, peer.hello(), result));
                }
                assertEquals(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN,
                        fixture.reservations.closeRecovery(metadata, closing, ack == 3
                                ? List.of(acknowledgments.get(0), acknowledgments.get(0), acknowledgments.get(1))
                                : List.of(acknowledgments.get(0), acknowledgments.get(0))).outcome());
                var legacy = acknowledgments.stream().map(evidence -> {
                    var hello = evidence.hello();
                    var caps = hello.capabilities().stream().filter(capability -> !capability.equals(
                            org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec
                                .TERMINAL_CLOSE_CAPABILITY)).toList();
                    return new ZKProfileReservationStore.TerminalAck(evidence.target(),
                            new org.apache.bookkeeper.common.profile.wire.ProfileHello.Server(hello.activeEngine(),
                                    hello.storageIncarnation(), hello.readinessGeneration(), hello.bookieId(), caps),
                            evidence.result());
                }).toList();
                assertEquals(ProfileLedgerMetadataStore.Outcome.DURABILITY_UNKNOWN,
                        fixture.reservations.closeRecovery(metadata, closing, legacy).outcome());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                var insufficient = new HashMap<>(fixture.clients);
                insufficient.remove(fixture.identities.get(2).bookieId().toString());
                if (ack == 2) {
                    insufficient.remove(fixture.identities.get(1).bookieId().toString());
                }
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.DEFERRED,
                        coordinator.finishRecovery(entered.context(), insufficient).get(5, TimeUnit.SECONDS).status());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                var closingPeers = new HashMap<>(fixture.clients);
                if (ack == 2) {
                    closingPeers.remove(fixture.identities.get(2).bookieId().toString());
                }
                ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
                ProfileRecoveryCoordinator.CompletionResult closed;
                try (LostCloseReplyClient lost = new LostCloseReplyClient(fixture.cluster.getZooKeeperConnectString(),
                        watcher)) {
                    var lostMetadata = new ProfileLedgerMetadataStore(lost, (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var lostReservations = new ZKProfileReservationStore(lost, "/socket-initial",
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    closed = new ProfileRecoveryCoordinator(lostReservations, lostMetadata, fixture.cold)
                            .finishRecovery(entered.context(), closingPeers).get(5, TimeUnit.SECONDS);
                }
                assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED, closed.publication().outcome());
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE, closed.status());
                assertEquals(ack, closed.terminalMembers());
                var finalMetadata = fixture.manager.readLedgerMetadata(44).get().getValue();
                assertEquals(LedgerMetadata.State.CLOSED, finalMetadata.getState());
                assertEquals(2, finalMetadata.getLastEntryId());
                assertEquals(48, finalMetadata.getLength());
                var forces = fixture.nodeIo.stream().map(io -> io.forces).toList();
                var again = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold)
                        .finishRecovery(entered.context(), fixture.clients).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE, again.status());
                assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                assertEquals(ProfileLedgerMetadataStore.Outcome.CONFLICT,
                        fixture.reservations.readRecovery(metadata, 44).outcome());
                verifyClosedReadonly(fixture, metadata, data, 2, 48);
                var restartConfig = files(fixture.nodes.get(2)).config();
                fixture.nodes.get(2).close();
                try (var replay = new IsolatedSegmentStorage(restartConfig, fixture.identities.get(2),
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                        new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                        new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                    replay.restart();
                    assertFalse(replay.profileReadVisible(fixture.context(), 4));
                    assertTrue(replay.profileReadVisible(fixture.context(), 2));
                    assertEquals(restartConfig.bytesPerFile(), java.nio.file.Files.size(
                            restartConfig.directory().resolve("data-0")));
                }
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        fixture.reservations.cutDelete(fixture.reservations.read(44).snapshot(),
                                bytes(16, 101), bytes(32, 102)).status());
                var historical = coordinator.finishRecovery(entered.context(), fixture.clients)
                        .get(5, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.CompletionStatus.CLOSED_METADATA_DURABLE, historical.status());
                assertEquals(ZKProfileReservationStore.ReadOnlyStatus.DELETED,
                        fixture.reservations.readClosedForOpen(metadata, 44).status());
                assertEquals(BKException.Code.NoSuchLedgerExistsException,
                        BKException.getExceptionCode(assertThrows(java.util.concurrent.ExecutionException.class,
                            () -> coordinator.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 3)
                                .get(5, TimeUnit.SECONDS))));
                assertEquals(0, historical.terminalMembers());
                assertEquals(forces, fixture.nodeIo.stream().map(io -> io.forces).toList());
                assertEquals(LedgerMetadata.State.CLOSED,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            }
        }
    }

    @Test
    public void selectedLastFragmentCopiesActualEntriesAndResolvesCloseReplyLossWithoutRecopy() throws Exception {
        lastFragmentRepair(0);
        lastFragmentRepair(2);
    }

    @Test
    public void selectedLastFragmentCancellationKeepsActualDataAndTheAttemptPermit() throws Exception {
        lastFragmentRepair(1);
    }

    private void lastFragmentRepair(int fault) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 4; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), peer);
                if (node < 3) {
                    for (int entry = 0; entry < 3; entry++) {
                        evidenceEntry(fixture, peer, entry, entry - 1, (entry + 1) * 16, 16);
                    }
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var accepted = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(fault == 1 ? 1 : 0);
            try (BookKeeper sdk = sdk(fixture)) {
                var sources = new HashMap<>(data);
                sources.remove(fixture.identities.get(3).bookieId());
                var proof = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), sources, fixture.clients,
                        fixture.credential, 4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, proof.status());
                var selected = CompletableFuture.supplyAsync(() -> fixture.reservations.selectPrefix(metadata, proof),
                        fixture.cold).get(5, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, selected.status());
                assertEquals(2, selected.snapshot().boundary().prefixEntryId());
                assertEquals(48, selected.snapshot().boundary().prefixLength());
                var current = fixture.reservations.readRecovery(metadata, 44);
                assertEquals(accepted.context(), current.context());
                var identity = fixture.identities.get(3);
                var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                        ProfileAuthorityPurpose.PREPARING_INSTALL, 44, fixture.context().ledgerInstanceId(),
                        ProfileDescriptorCodec.identityOf(fixture.descriptor, VALIDATOR),
                        identity.bookieId().toString(),
                        identity.incarnation().value(), "ledger-profile",
                        current.context().controlGeneration(),
                        bytes(16, 40), bytes(32, 40), null);
                var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(current.context(), 0,
                        bytes(16, 41), 0, 1, ZKProfileReservationStore.repairEnsembleDigest(
                                current.metadata().metadata().getValue(), 0), install, bytes(16, 42),
                        new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
                var prepared = fixture.reservations.prepareRepair(metadata, intent).snapshot();
                assertNotNull(prepared);
                fixture.connect(3, 4, Duration.ofSeconds(5));
                if (fault == 1) {
                    files(fixture.nodes.get(3)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(release);
                    });
                } else if (fault == 2) {
                    fixture.faultTarget.set(3);
                    fixture.replyFault.set(18);
                }
                var copy = coordinator.repairLastFragment(current, prepared, sdk.getClientCtx(),
                        fixture.descriptorBytes, fixture.credential, data, fixture.clients, bytes(16, 43));
                if (fault == 1) {
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertTrue(copy.cancel(true));
                    var busy = coordinator.repairLastFragment(current, prepared, sdk.getClientCtx(),
                            fixture.descriptorBytes, fixture.credential, data, fixture.clients, bytes(16, 43)).get();
                    assertEquals(ProfileRecoveryCoordinator.RepairStatus.DEFERRED, busy.status());
                    assertEquals(current.metadata().metadata().getVersion(),
                            metadata.readMembership(44).snapshot().metadata().getVersion());
                    release.countDown();
                } else {
                    var result = copy.get(5, TimeUnit.SECONDS);
                    assertEquals(fault == 2 ? ProfileRecoveryCoordinator.RepairStatus.DEFERRED
                            : ProfileRecoveryCoordinator.RepairStatus.REPAIRED_LAST_FRAGMENT, result.status());
                    assertEquals(3, result.copiedEntries());
                }
                fixture.replyFault.set(0);
                fixture.connect(3, 4, Duration.ofSeconds(5));
                var resumedCoordinator = fault == 2 ? new ProfileRecoveryCoordinator(fixture.reservations,
                        metadata, fixture.cold) : coordinator;
                ProfileRecoveryCoordinator.RepairResult resumed;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                do {
                    resumed = resumedCoordinator.repairLastFragment(current, prepared, sdk.getClientCtx(),
                            fixture.descriptorBytes, fixture.credential, data, fixture.clients, bytes(16, 43))
                                .get(5, TimeUnit.SECONDS);
                    if (resumed.status() == ProfileRecoveryCoordinator.RepairStatus.DEFERRED) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                    }
                } while (resumed.status() == ProfileRecoveryCoordinator.RepairStatus.DEFERRED
                        && System.nanoTime() < deadline);
                assertEquals(ProfileRecoveryCoordinator.RepairStatus.REPAIRED_LAST_FRAGMENT, resumed.status());
                if (fault == 2) {
                    assertEquals(0, resumed.copiedEntries());
                }
                assertFalse(fixture.normal(3));
                assertEquals(48, data.get(identity.bookieId()).readEntry(fixture.context(), 2)
                        .get(5, TimeUnit.SECONDS).entry().getLong(24));
                assertEquals(current.context(), fixture.reservations.readRecovery(metadata, 44).context());
                assertEquals(selected.snapshot(), fixture.reservations.readSelectedPrefix(metadata,
                        current.context()).snapshot());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void historicalRepairCoordinatorCopiesMultipleEntriesAndResumesPublishedCloseWithoutRecopy()
            throws Exception {
        coordinatedHistoricalRepair(0);
    }

    @Test
    public void historicalRepairCoordinatorCallerCancelKeepsPermitUntilActualEntryRpcEnds() throws Exception {
        coordinatedHistoricalRepair(1);
    }

    @Test
    public void historicalRepairCoordinatorCopyFailureNeverPublishesMembershipOrNormalAuthority() throws Exception {
        coordinatedHistoricalRepair(2);
    }

    @Test
    public void historicalRepairCoordinatorNewInstanceResolvesLostCloseReplyWithoutRecopy() throws Exception {
        coordinatedHistoricalRepair(3);
    }

    private void coordinatedHistoricalRepair(int fault) throws Exception {
        boolean cancel = fault == 1;
        try (Fixture fixture = new Fixture(false, false, true, 5, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 5; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 4, Duration.ofSeconds(5)));
            }
            for (long entryId = 0; entryId < 3; entryId++) {
                for (int node : List.of(0, 1, 2)) {
                    evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), entryId,
                            entryId - 1, (entryId + 1) * 16, 16);
                }
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var replacement = new ProfileReplacementCoordinator(fixture.reservations, metadata, VALIDATOR,
                    fixture.cold);
            var peer = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                    data.get(fixture.identities.get(3).bookieId()));
            var operation = replacement.begin(available.metadata(), 3, Map.of(2, peer), DESCRIPTOR_BYTES,
                    fixture.credential, bytes(16, 20), bytes(16, 21)).get(5, TimeUnit.SECONDS);
            assertEquals(ProfileReplacementCoordinator.Status.AVAILABLE,
                    replacement.execute(operation).get(5, TimeUnit.SECONDS).status());
            var entered = fixture.reservations.beginRecovery(metadata,
                    fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
            var identity = fixture.identities.get(4);
            var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, 44, fixture.context().ledgerInstanceId(),
                    ProfileDescriptorCodec.identityOf(fixture.descriptor, VALIDATOR), identity.bookieId().toString(),
                    identity.incarnation().value(), "ledger-profile", entered.context().controlGeneration(),
                    bytes(16, 40), bytes(32, 40), null);
            var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(entered.context(), 0,
                    bytes(16, 41), 0, 1, ZKProfileReservationStore.repairEnsembleDigest(
                            entered.metadata().metadata().getValue(), 0), install, bytes(16, 42),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 2));
            var prepared = fixture.reservations.prepareRepair(metadata, intent).snapshot();
            fixture.connect(4, 4, Duration.ofSeconds(5));
            var coordinator = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                    fixture.reservations, metadata, fixture.cold);
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(cancel ? 1 : 0);
            if (cancel) {
                files(fixture.nodes.get(4)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    forcing.countDown();
                    await(release);
                });
            }
            if (fault == 2) {
                files(fixture.nodes.get(4)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                fixture.expectedFailedDataSessions.add(4);
            } else if (fault == 3) {
                fixture.faultTarget.set(4);
                fixture.replyFault.set(18);
            }
            try (BookKeeper sdk = sdk(fixture)) {
                var pending = coordinator.repairHistorical(entered, prepared, sdk.getClientCtx(), DESCRIPTOR_BYTES,
                        fixture.credential, data, fixture.clients, bytes(16, 43));
                if (cancel) {
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertTrue(pending.cancel(false));
                    assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                            .DEFERRED,
                            coordinator.repairHistorical(entered, prepared, sdk.getClientCtx(), DESCRIPTOR_BYTES,
                                    fixture.credential, data, fixture.clients, bytes(16, 43)).get().status());
                    assertEquals(entered.metadata().metadata().getVersion(), metadata.readMembership(44)
                            .snapshot().metadata().getVersion());
                    release.countDown();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    do {
                        var attempted = coordinator.repairHistorical(entered, prepared, sdk.getClientCtx(),
                                DESCRIPTOR_BYTES, fixture.credential, data, fixture.clients, bytes(16, 43));
                        if (!attempted.isDone()) {
                            pending = attempted;
                            break;
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                    } while (System.nanoTime() < deadline);
                }
                var complete = pending.get(10, TimeUnit.SECONDS);
                if (fault == 2) {
                    assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                            .DEFERRED,
                            complete.status());
                    assertEquals(0, complete.copiedEntries());
                    assertEquals(entered.metadata().metadata().getVersion(),
                            metadata.readMembership(44).snapshot().metadata().getVersion());
                    assertFalse(fixture.normal(4));
                    assertEquals(5, fixture.reservations.retainedActivationTargets(44).targets().size());
                    return;
                }
                if (fault == 3) {
                    assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                            .DEFERRED,
                            complete.status());
                    assertEquals(3, complete.copiedEntries());
                    assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, complete.publication().status());
                    fixture.replyFault.set(0);
                    fixture.connect(4, 4, Duration.ofSeconds(5));
                    int forces = files(fixture.nodes.get(4)).dataIo.forces;
                    var reloaded = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                            fixture.reservations, metadata, fixture.cold);
                    var resolved = reloaded.repairHistorical(entered, prepared, sdk.getClientCtx(), DESCRIPTOR_BYTES,
                            fixture.credential, data, fixture.clients, bytes(16, 43)).get(5, TimeUnit.SECONDS);
                    assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                            .REPAIRED_HISTORICAL_FRAGMENT, resolved.status());
                    assertEquals(0, resolved.copiedEntries());
                    assertEquals(forces, files(fixture.nodes.get(4)).dataIo.forces);
                    assertFalse(fixture.normal(4));
                    return;
                }
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                        .REPAIRED_HISTORICAL_FRAGMENT, complete.status());
                assertEquals(3, complete.copiedEntries());
                assertEquals(identity.bookieId(), complete.publication().metadata().metadata().getValue()
                        .getAllEnsembles().get(0L).get(1));
                assertFalse(fixture.normal(4));
                int before = files(fixture.nodes.get(4)).dataIo.forces;
                var resumed = coordinator.repairHistorical(entered, prepared, sdk.getClientCtx(), DESCRIPTOR_BYTES,
                        fixture.credential, data, fixture.clients, bytes(16, 43)).get(5, TimeUnit.SECONDS);
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.RepairStatus
                        .REPAIRED_HISTORICAL_FRAGMENT, resumed.status());
                assertEquals(0, resumed.copiedEntries());
                assertEquals(before, files(fixture.nodes.get(4)).dataIo.forces);
                var adopted = fixture.reservations.readRecovery(metadata, 44);
                assertEquals(entered.context(), adopted.context());
                var controls = new HashMap<BookieId, ProfileInitialControlClient>();
                fixture.clients.forEach((name, client) -> controls.put(BookieId.parse(name), client));
                var reader = org.apache.bookkeeper.client.ProfileRecoveryReadHandle.fromContext(sdk.getClientCtx(),
                        adopted, fixture.reservations.retainedActivationTargets(44).targets(), data, controls);
                try {
                    assertEquals(org.apache.bookkeeper.client.ProfileRecoveryReadHandle.ProbeOutcome.CANDIDATE,
                            reader.probe(0).get(5, TimeUnit.SECONDS).outcome());
                } finally {
                    reader.closeAsync().get(5, TimeUnit.SECONDS);
                }
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void recoveryCloseRequiresIndependentAuthorityAndReplaysItsDurableResult() throws Exception {
        recoveryCloseOverSocket(0);
    }

    @Test
    public void recoveryCloseWaitsForActualPrecutDataAndDoesNotPublishEarlyReadable() throws Exception {
        recoveryCloseOverSocket(1);
    }

    @Test
    public void recoveryCloseTimeoutKeepsActualControlIoAndResolvesTheSameOperation() throws Exception {
        recoveryCloseOverSocket(2);
    }

    @Test
    public void recoveryCloseForceFailureNeverReturnsDurableSuccessOrReopensRecovery() throws Exception {
        recoveryCloseOverSocket(3);
    }

    private void recoveryCloseOverSocket(int fault) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var plan = fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan();
            var entered = fixture.reservations.beginRecovery(metadata, plan);
            var identity = fixture.identities.get(3);
            var scope = new ProfileControlScope(ProfileControlOperation.INSTALL,
                    ProfileAuthorityPurpose.PREPARING_INSTALL, 44, fixture.context().ledgerInstanceId(),
                    ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR), identity.bookieId().toString(),
                    identity.incarnation().value(), "ledger-profile", entered.context().controlGeneration(),
                    bytes(16, 40), bytes(32, 40), null);
            var intent = new org.apache.bookkeeper.discover.profile.ProfileRepairIntent(entered.context(), 0,
                    bytes(16, 41), 0, 1, ZKProfileReservationStore.repairEnsembleDigest(
                            entered.metadata().metadata().getValue(), 0), scope, bytes(16, 42),
                    new org.apache.bookkeeper.common.profile.control.ProfileExactRange(0, 0));
            var prepared = fixture.reservations.prepareRepair(metadata, intent);
            var control = fixture.connect(3, 4, Duration.ofSeconds(5));
            var installed = control.install(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                    EngineProfile.SEGMENT_WAL_ENGINE, fixture.credential)).get(5, TimeUnit.SECONDS);
            fixture.reservations.admitRepair(metadata, prepared.snapshot(),
                    org.apache.bookkeeper.discover.profile.ProfileInstallReceipt.from(
                            scope, control.hello(), installed));
            assertTrue(control.recoveryGrant(ProfileControlRequest.recoveryGrant(intent.grantScope(),
                    new ProfileControlRequest.RecoveryBinding(intent.intentId(), intent.intentGeneration(),
                            intent.grantGeneration()))).get(5, TimeUnit.SECONDS).durable());
            var grantId = java.nio.ByteBuffer.wrap(intent.grantOperationId());
            UUID grantOperation = new UUID(grantId.getLong(), grantId.getLong());
            var unbound = new ProfileControlScope(ProfileControlOperation.RECOVERY_GRANT_CLOSE,
                    ProfileAuthorityPurpose.RECOVERY_GRANT_CLOSE, 44, scope.ledgerInstanceId(),
                    scope.descriptorIdentity(), scope.targetBookie(), scope.targetStorageIncarnation(),
                    scope.authorityDomain(), scope.authorityGeneration() + 1,
                    bytes(16, 43), bytes(32, 0), intent.range());
            var closeScope = new ProfileControlScope(unbound.operation(), unbound.purpose(), unbound.ledgerId(),
                    unbound.ledgerInstanceId(), unbound.descriptorIdentity(), unbound.targetBookie(),
                    unbound.targetStorageIncarnation(), unbound.authorityDomain(), unbound.authorityGeneration(),
                    unbound.operationId(), SegmentBookieControlLog.recoveryClosePayloadIdentity(unbound,
                            grantOperation, intent.intentId(), intent.intentGeneration(), intent.grantGeneration()),
                    intent.range());
            var close = ProfileControlRequest.recoveryClose(closeScope, new ProfileControlRequest.RecoveryBinding(
                    intent.intentId(), intent.intentGeneration(), intent.grantGeneration()), intent.grantOperationId());
            int before = fixture.nodeIo.get(3).forces;
            assertFalse(control.recoveryClose(close).get(5, TimeUnit.SECONDS).durable());
            assertEquals(before, fixture.nodeIo.get(3).forces);
            fixture.closeAuthority = new org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority(
                    closeScope, org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority.State
                            .RECOVERY_OPEN, EngineProfile.SEGMENT_WAL_ENGINE, 1, true);
            assertFalse(control.recoveryClose(close).get(5, TimeUnit.SECONDS).durable());
            assertEquals(before, fixture.nodeIo.get(3).forces);
            fixture.closeAuthority = new org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority(
                    closeScope, org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority.State
                            .RECOVERY_CLOSE_AUTHORIZED, EngineProfile.SEGMENT_WAL_ENGINE, 1, true);
            var data = dataClient(fixture, 3, 4, Duration.ofSeconds(5));
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            var source = entry(fixture, 0, 16);
            try {
                if (fault == 1) {
                    files(fixture.nodes.get(3)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(release);
                    });
                }
                var copied = data.addRecovery(intent, fixture.credential, 0, source);
                if (fault == 1) {
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    var waiting = control.recoveryClose(close).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileControlResult.Outcome.TRANSIENT_RECONCILING, waiting.outcome());
                    assertFalse(waiting.durable());
                    assertFalse(bookie(fixture.nodes.get(3)).locallyCommittedReadable(fixture.context(), 0));
                    assertEquals(before, fixture.nodeIo.get(3).forces);
                    release.countDown();
                    assertEquals(ProfileStatus.DurableResult.UNKNOWN,
                            copied.get(5, TimeUnit.SECONDS).durableResult());
                } else {
                    assertEquals(ProfileStatus.DurableResult.APPLIED,
                            copied.get(5, TimeUnit.SECONDS).durableResult());
                }
                if (fault == 3) {
                    var field = SegmentBookieControlLog.class.getDeclaredField("io");
                    field.setAccessible(true);
                    var failedIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                    field.set(bookie(fixture.nodes.get(3)), failedIo);
                    var uncertain = control.recoveryClose(close).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileControlResult.Outcome.DURABILITY_UNKNOWN, uncertain.outcome());
                    assertFalse(uncertain.durable());
                    assertEquals(1, failedIo.forces);
                    assertFalse(bookie(fixture.nodes.get(3)).locallyCommittedReadable(fixture.context(), 0));
                    assertFalse(fixture.normal(3));
                    return;
                }
                if (fault == 2) {
                    control = fixture.connect(3, 4, Duration.ofMillis(300));
                    var field = SegmentBookieControlLog.class.getDeclaredField("io");
                    field.setAccessible(true);
                    var actualIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(release);
                    });
                    field.set(bookie(fixture.nodes.get(3)), actualIo);
                    var unknown = control.recoveryClose(close);
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class, () -> unknown.get(5, TimeUnit.SECONDS));
                    assertEquals(0, actualIo.forces);
                    assertThrows(java.io.IOException.class, bookie(fixture.nodes.get(3))::close);
                    assertFalse(bookie(fixture.nodes.get(3)).locallyCommittedReadable(fixture.context(), 0));
                    release.countDown();
                    control = fixture.connect(3, 4, Duration.ofSeconds(5));
                }
                ProfileControlResult closed;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                do {
                    closed = control.recoveryClose(close).get(5, TimeUnit.SECONDS);
                    if (!closed.durable()) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                    }
                } while (!closed.durable() && System.nanoTime() < deadline);
                assertTrue(closed.durable());
                assertEquals(fault == 2 ? ProfileControlResult.Outcome.ALREADY_APPLIED
                        : ProfileControlResult.Outcome.APPLIED, closed.outcome());
                var actualField = SegmentBookieControlLog.class.getDeclaredField("io");
                actualField.setAccessible(true);
                var actualControlIo = (SegmentFileIo) actualField.get(bookie(fixture.nodes.get(3)));
                int closedForces = actualControlIo.forces;
                var repeated = control.recoveryClose(close).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED, repeated.outcome());
                assertEquals(closedForces, actualControlIo.forces);
                assertEquals(closed.localGeneration(), repeated.localGeneration());
                assertTrue(bookie(fixture.nodes.get(3)).locallyCommittedReadable(fixture.context(), 0));
                assertFalse(fixture.normal(3));
                assertEquals(ProfileStatus.DurableResult.NONE,
                        data.addRecovery(intent, fixture.credential, 0, source)
                                .get(5, TimeUnit.SECONDS).durableResult());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        data.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
                assertEquals(4, fixture.reservations.retainedActivationTargets(44).targets().size());
                assertEquals(LedgerMetadata.State.IN_RECOVERY,
                        fixture.manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue().getState());
            } finally {
                release.countDown();
                source.release();
            }
        }
    }

    private static void persistExplicitLac(Fixture fixture, int node, long value, boolean corrupt) throws Exception {
        var digest = DigestManager.instantiate(44, fixture.password,
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                UnpooledByteBufAllocator.DEFAULT, false);
        var packed = digest.computeDigestAndPackageForSendingLac(value);
        byte[] body = new byte[packed.readableBytes()];
        try {
            packed.getBuffer(0).getBytes(packed.getBuffer(0).readerIndex(), body);
        } finally {
            packed.release();
        }
        if (corrupt) {
            body[body.length - 1] ^= 1;
        }
        CompletableFuture<SegmentBookieControlLog.Status> applied = new CompletableFuture<>();
        assertTrue(fixture.nodes.get(node).controls().submitExplicitLac(
                new org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec.WriteLac(fixture.context(), value,
                        DigestManager.generateMasterKey(fixture.password), body),
                (status, failure) -> {
                    if (failure == null) {
                        applied.complete(status);
                    } else {
                        applied.completeExceptionally(failure);
                    }
                }));
        assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, applied.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void explicitLacWireAndSdkKeepSeparateWitnessesAndUseMatchingPointLength() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new ArrayList<ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.add(peer);
                evidenceEntry(fixture, peer, 99, 98, 1000, 16);
                evidenceEntry(fixture, peer, 100, 98, 1100, 100);
                persistExplicitLac(fixture, node, 99, false);
            }
            var raw = data.get(0).readLac(fixture.context()).get(5, TimeUnit.SECONDS);
            assertEquals(100, raw.sourceEntryId());
            assertEquals(98, raw.piggybackLac());
            assertEquals(1100, raw.sourceLength());
            assertTrue(raw.hasExplicitLac());
            assertEquals(99, raw.explicitLac());
            assertEquals(20, raw.explicitBody().remaining());
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    var proof = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                    assertEquals(99, proof.maxObservedLac());
                    assertEquals(99, proof.prefixEntryId());
                    assertEquals(1000, proof.prefixLength());
                    assertEquals(3, proof.explicitSources().size());
                    assertEquals(-1, handle.getLastAddConfirmed());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void explicitOnlyLacDoesNotBecomeWholeLedgerEmptyOrInventPrefixLength() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            for (int node = 0; node < 3; node++) {
                persistExplicitLac(fixture, node, 99, false);
                assertEquals(ProfileStatus.StatusClass.OK, fixture.clients.get("socket" + node + ":3181")
                        .fence(fixture.context(), bytes(16, 25), fixture.credential)
                        .get(5, TimeUnit.SECONDS).statusClass());
            }
            var data = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            var reply = data.readLac(fixture.context()).get(5, TimeUnit.SECONDS);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec.Outcome.EXPLICIT,
                    reply.outcome());
            assertEquals(-1, reply.sourceEntryId());
            assertEquals(99, reply.explicitLac());
            assertEquals(0, reply.sourceLength());
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    assertThrows(ExecutionException.class, () -> handle.readLacEvidence().get(5, TimeUnit.SECONDS));
                    assertEquals(-1, handle.getLastAddConfirmed());
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void corruptExplicitLacExcludesTheEndpointAndRequiresRemainingCoverage() throws Exception {
        for (int corrupt = 1; corrupt <= 2; corrupt++) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                    evidenceEntry(fixture, peer, 99, 98, 1000, 16);
                    evidenceEntry(fixture, peer, 100, 99, 1100, 100);
                    persistExplicitLac(fixture, node, 100, node < corrupt);
                }
                try (BookKeeper sdk = sdk(fixture)) {
                    var handle = writer(fixture, sdk, available, 8, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                    try {
                        if (corrupt == 1) {
                            var proof = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                            assertEquals(100, proof.maxObservedLac());
                            assertEquals(100, proof.prefixEntryId());
                            assertEquals(1100, proof.prefixLength());
                            assertEquals(2, proof.sources().size());
                            assertEquals(2, proof.explicitSources().size());
                            assertTrue(proof.sources().stream().noneMatch(source -> source.bookie().toString()
                                    .equals("socket0:3181")));
                        } else {
                            assertThrows(ExecutionException.class,
                                    () -> handle.readLacEvidence().get(5, TimeUnit.SECONDS));
                        }
                        assertEquals(-1, handle.getLastAddConfirmed());
                    } finally {
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                }
            }
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

    private static org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registeredPeer(
            Fixture fixture, org.apache.zookeeper.ZooKeeper reader, String endpoint, long generation,
            byte[] incarnation) throws Exception {
        return registeredPeer(fixture, reader, 0, endpoint, generation, incarnation);
    }

    private static org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registeredPeer(
            Fixture fixture, org.apache.zookeeper.ZooKeeper reader, int target, String endpoint, long generation,
            byte[] incarnation) throws Exception {
        String root = "/socket-readiness";
        if (fixture.zk.exists(root, false) == null) {
            fixture.zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        var persistent = new org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore(
                fixture.zk, root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        var gen = new org.apache.bookkeeper.common.profile.startup.Generation(1);
        var record = new org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord(
                fixture.identities.get(target).bookieId().toString(),
                new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(incarnation),
                EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, gen, gen,
                new org.apache.bookkeeper.common.profile.startup.ManifestIdentity(bytes(32, 7)), gen, gen,
                new org.apache.bookkeeper.common.profile.startup.Generation(generation), 1, 1, List.of(1),
                org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord.State.READY);
        assertEquals(org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore
                            .CasStatus.APPLIED,
                persistent.compareAndSet(record.bookieId(), -1, 0, record).status());
        var ready = persistent.read(record.bookieId()).versionedRecord().orElseThrow();
        var projection = new org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection(
                record.bookieId(), record.storageIncarnation(), record.localReadinessGeneration(),
                ready.storeVersion(), endpoint, record.capabilityHints());
        var publisher = new org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore(
                fixture.zk, root, record.bookieId(), ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                .PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
        assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                .PublicationStatus.APPLIED, publisher.registerWritable(
                    new org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                        .WritableRegistration(record.bookieId(), record.storageIncarnation(),
                                record.localReadinessGeneration(), ready.storeVersion())).status());
        return new org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore(
                reader, root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private static ZooKeeper registrationReader(Fixture fixture) throws Exception {
        var watcher = new ZooKeeperWatcherBase(10000, false);
        var reader = new ZooKeeper(fixture.cluster.getZooKeeperConnectString(), 10000, watcher);
        watcher.waitForConnection();
        return reader;
    }

    @Test
    public void registeredPeerPoolPinsTheLeaseAndKeepsCapacityUntilActualClose() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    fixture.clientGroup, fixture.peerTls, fixture.cold, List.of(fixture.identities.get(0).bookieId()),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            2, 4, 2, 1060, Duration.ofSeconds(5)));
            var acquisition = pool.acquire();
            var lease = acquisition.completion().get(5, TimeUnit.SECONDS);
            assertEquals(2, pool.usage().reservedConnections());
            assertEquals(4, pool.usage().reservedRpcSlots());
            assertTrue(pool.usage().borrowed());
            var unavailable = pool.acquire();
            assertThrows(ExecutionException.class, () -> unavailable.completion().get(5, TimeUnit.SECONDS));
            unavailable.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(2, pool.usage().reservedConnections());
            var closing = pool.closeAsync();
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> closing.get(150, TimeUnit.MILLISECONDS));
            var source = lease.data().get(fixture.identities.get(0).bookieId());
            evidenceEntry(fixture, source, 0, -1, 16, 16);
            assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                    source.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
            lease.closeAsync().get(5, TimeUnit.SECONDS);
            closing.get(5, TimeUnit.SECONDS);
            assertEquals(0, pool.usage().reservedConnections());
            assertEquals(0, pool.usage().reservedRpcSlots());
            assertThrows(IllegalStateException.class, lease::data);
            assertFalse(fixture.clientGroup.isShuttingDown());
        }
    }

    @Test
    public void replicationWorkerConsumesRegisteredPoolAndRepairsAnActuallyUnavailablePeer() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var client = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), client);
                evidenceEntry(fixture, client, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                    fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registry = null;
            for (int node = 0; node < 4; node++) {
                String endpoint = "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort();
                registry = registeredPeer(fixture, reader, node, endpoint, 1,
                        fixture.identities.get(node).incarnation().value());
            }
            fixture.listeners.get(2).close().syncUninterruptibly();
            var pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    fixture.clientGroup, fixture.peerTls, fixture.cold,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            8, 16, 2, 1060, Duration.ofSeconds(5)));
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30));
                var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture), sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                        coordinator, options, pool);
                worker.start();
                try {
                    awaitProfileWorkerStarted(worker);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                    assertEquals(fixture.identities.get(3).bookieId(),
                            fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                    assertEquals(0, pool.usage().reservedConnections());
                } finally {
                    worker.shutdown();
                    pool.closeAsync().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void profileAutoRecoveryServiceOwnsItsSdkPoolAndRepairsThroughRegisteredPeers() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var client = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), client);
                evidenceEntry(fixture, client, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                    fixture.credential, data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registry = null;
            for (int node = 0; node < 4; node++) {
                String endpoint = "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort();
                registry = registeredPeer(fixture, reader, node, endpoint, 1,
                        fixture.identities.get(node).incarnation().value());
            }
            fixture.listeners.get(2).close().syncUninterruptibly();

            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30));
                var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                        "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                        fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                        new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                                8, 16, 2, 1060, Duration.ofSeconds(5)), options);
                var conf = profileWorkerConfiguration(fixture);
                assertThrows(IllegalArgumentException.class,
                        () -> new org.apache.bookkeeper.replication.AutoRecoveryMain(conf,
                                org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings));
                conf.setAllowLoopback(true);
                conf.setAuditorPeriodicCheckInterval(0);
                conf.setAuditorPeriodicReplicasCheckInterval(0);
                conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
                var service = new org.apache.bookkeeper.server.service.AutoRecoveryService(
                        new org.apache.bookkeeper.server.conf.BookieConfiguration(conf),
                        org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings);
                var main = service.getAutoRecoveryServer();
                assertTrue(main.isProfileEnabled());
                var worker = main.getReplicationWorker();
                service.start();
                try {
                    awaitProfileWorkerStarted(worker);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                    while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    assertNull(monitor.getLedgerUnreplicationInfo(44));
                    assertEquals(fixture.identities.get(3).bookieId(),
                            fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                    assertEquals(0, main.getProfilePeerUsage().reservedConnections());
                } finally {
                    service.close();
                }
                assertTrue(main.getProfilePeerUsage().closing());
                assertFalse(worker.isRunning());
                assertFalse(sdk.getClientCtx().isClientClosed());
                var resourcesField = main.getClass().getDeclaredField("profileResources");
                resourcesField.setAccessible(true);
                var resources = resourcesField.get(main);
                var groupField = resources.getClass().getDeclaredField("group");
                var coldField = resources.getClass().getDeclaredField("cold");
                groupField.setAccessible(true);
                coldField.setAccessible(true);
                assertTrue(((EventLoopGroup) groupField.get(resources)).isTerminated());
                assertTrue(((ThreadPoolExecutor) coldField.get(resources)).isTerminated());
            }
        }
    }

    @Test
    public void registeredPoolDrainPrecedesOriginalTaskAndMarkerRemoval() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var original = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var client = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                original.put(fixture.identities.get(node).bookieId(), client);
                evidenceEntry(fixture, client, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            coordinator.recover(44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                    fixture.credential, original, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registry = null;
            for (int node = 0; node < 3; node++) {
                registry = registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            var pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    fixture.clientGroup, fixture.peerTls, fixture.cold,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            6, 6, 1, 1060, Duration.ofSeconds(5)));
            var lease = pool.acquire().completion().get(5, TimeUnit.SECONDS);
            var socketField = ProfileNormalDataClient.class.getDeclaredField("socket");
            socketField.setAccessible(true);
            var owner = socketField.get(lease.data().get(fixture.identities.get(0).bookieId()));
            var channelField = owner.getClass().getDeclaredField("channel");
            channelField.setAccessible(true);
            Channel channel = (Channel) channelField.get(owner);
            var held = new AtomicReference<ChannelPromise>();
            var written = new CountDownLatch(1);
            channel.eventLoop().submit(() -> channel.pipeline().addAfter(
                    channel.pipeline().context(SslHandler.class).name(), "holdPoolReadWritePromise",
                    new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                            ByteBuf bytes = (ByteBuf) message;
                            if (bytes.getUnsignedShort(bytes.readerIndex() + 14)
                                    == ProfileOperation.READ_ENTRY.wireValue()
                                    && held.compareAndSet(null, promise)) {
                                var actual = ctx.newPromise();
                                actual.addListener(done -> written.countDown());
                                ctx.write(bytes, actual);
                            } else {
                                ctx.write(bytes, promise);
                            }
                        }
                    })).get(5, TimeUnit.SECONDS);
            try (var tasks = new org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager(
                    profileWorkerConfiguration(fixture), fixture.zk)) {
                tasks.enableProfileCleanupGuards();
                tasks.markLedgerUnderreplicated(44, "socket2:3181");
                assertEquals(44, tasks.pollLedgerToRereplicate());
                var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30));
                var attempt = coordinator.beginWorker(44, sdk.getClientCtx(), options, tasks, lease);
                try {
                    assertTrue(written.await(5, TimeUnit.SECONDS));
                    channel.closeFuture().get(5, TimeUnit.SECONDS); // Health reached the lease drain barrier.
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> attempt.completion().get(150, TimeUnit.MILLISECONDS));
                    assertTrue(tasks.isLedgerBeingReplicated(44));
                    assertNotNull(tasks.getLedgerUnreplicationInfo(44));
                    assertEquals(6, pool.usage().reservedConnections());
                    held.get().setSuccess();
                    assertEquals(ProfileRecoveryCoordinator.WorkerStatus.REPLICATED,
                            attempt.completion().get(5, TimeUnit.SECONDS).status());
                    assertNull(tasks.getLedgerUnreplicationInfo(44));
                    assertEquals(0, pool.usage().reservedConnections());
                    attempt.releaseTask();
                } finally {
                    if (held.get() != null) {
                        held.get().trySuccess();
                    }
                    lease.closeAsync().get(5, TimeUnit.SECONDS);
                    pool.closeAsync().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void registeredPoolAcquisitionShutdownKeepsTaskAndCapacityWhileColdWorkIsQueued() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var registry = registeredPeer(fixture, reader, "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort(), 1,
                    fixture.identities.get(0).incarnation().value());
            var cold = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(8));
            var release = new CountDownLatch(1);
            var entered = new CountDownLatch(1);
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            var pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    fixture.clientGroup, fixture.peerTls, cold, List.of(fixture.identities.get(0).bookieId()),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            2, 2, 1, 1060, Duration.ofSeconds(5)));
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                cold.execute(() -> {
                    entered.countDown();
                    await(release);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var worker = new org.apache.bookkeeper.replication.ReplicationWorker(
                        profileWorkerConfiguration(fixture), sdk, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                        new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold),
                        new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                                Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30)), pool);
                worker.start();
                awaitProfileWorkerStarted(worker);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (pool.usage().reservedConnections() == 0 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(2, pool.usage().reservedConnections());
                var stopping = stopper.submit(worker::shutdown);
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertTrue(monitor.isLedgerBeingReplicated(44));
                assertEquals(2, pool.usage().reservedConnections());
                release.countDown();
                stopping.get(5, TimeUnit.SECONDS);
                assertEquals(0, pool.usage().reservedConnections());
                assertFalse(monitor.isLedgerBeingReplicated(44));
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertFalse(sdk.getClientCtx().isClientClosed());
            } finally {
                release.countDown();
                pool.closeAsync().get(5, TimeUnit.SECONDS);
                stopper.shutdownNow();
                cold.shutdownNow();
            }
        }
    }

    @Test
    public void registeredPoolRefreshesSameIncarnationEpisodesAndRejectsSilentRejoin() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var cohort = List.of(fixture.identities.get(0).bookieId());
            assertThrows(IllegalArgumentException.class,
                    () -> new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                            fixture.clientGroup, fixture.peerTls, fixture.cold, cohort,
                            new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                                    2, 2, 2, 1060, Duration.ofSeconds(5))));
            var pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    fixture.clientGroup, fixture.peerTls, fixture.cold, cohort,
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            2, 4, 2, 1060, Duration.ofSeconds(5)));
            try {
                var first = pool.acquire().completion().get(5, TimeUnit.SECONDS);
                first.closeAsync().get(5, TimeUnit.SECONDS);
                for (int generation : List.of(2, 3)) {
                    String path = "/socket-readiness/" + java.util.Base64.getUrlEncoder().withoutPadding()
                            .encodeToString("socket0:3181".getBytes(StandardCharsets.UTF_8));
                    for (String child : fixture.zk.getChildren(path, false)) {
                        if (child.equals("service-info") || child.equals("writable") || child.startsWith("session-")) {
                            fixture.zk.delete(path + "/" + child, -1);
                        }
                    }
                    var before = registry.read("socket0:3181").versionedRecord().orElseThrow();
                    var old = before.record();
                    var record = new org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord(
                            old.bookieId(), new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(
                                    generation == 3 ? bytes(16, 78) : old.storageIncarnation().value()), old.engine(),
                            old.protocolMajor(), old.protocolMinor(), old.capabilityGeneration(),
                            old.localFormatGeneration(),
                            old.deviceManifestIdentity(), old.deviceManifestGeneration(),
                            old.effectiveAssignmentGeneration(),
                            new org.apache.bookkeeper.common.profile.startup.Generation(generation),
                            old.minimumReaderVersion(), old.minimumWriterVersion(), old.capabilityHints(), old.state());
                    assertEquals(org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore
                            .CasStatus.APPLIED,
                            registry.compareAndSet("socket0:3181", before.storeVersion(),
                                    generation - 1, record).status());
                    var ready = registry.read("socket0:3181").versionedRecord().orElseThrow();
                    var projection = new org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection(
                            old.bookieId(), record.storageIncarnation(), record.localReadinessGeneration(),
                            ready.storeVersion(), endpoint, record.capabilityHints());
                    var publisher = new org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore(
                            fixture.zk, "/socket-readiness", old.bookieId(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.APPLIED, publisher.registerWritable(
                                new org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                                    .WritableRegistration(old.bookieId(), record.storageIncarnation(),
                                            record.localReadinessGeneration(), ready.storeVersion())).status());
                    fixture.helloReadinessGeneration = generation;
                    var acquisition = pool.acquire();
                    if (generation == 2) {
                        var lease = acquisition.completion().get(5, TimeUnit.SECONDS);
                        assertEquals(2, lease.controls().get("socket0:3181").hello().readinessGeneration());
                        lease.closeAsync().get(5, TimeUnit.SECONDS);
                    } else {
                        assertThrows(ExecutionException.class, () -> acquisition.completion().get(5, TimeUnit.SECONDS));
                        acquisition.closeAsync().get(5, TimeUnit.SECONDS);
                    }
                    assertEquals(0, pool.usage().reservedConnections());
                }
            } finally {
                pool.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void profileServiceShutdownWaitsForItsActualLeaseBeforeClosingOwnedSdkAndOwners() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                var client = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                data.put(fixture.identities.get(node).bookieId(), client);
                evidenceEntry(fixture, client, 0, -1, 16, 16);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(
                    44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential,
                    data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            for (int node = 0; node < 3; node++) {
                registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            var conf = profileWorkerConfiguration(fixture);
            conf.setAllowLoopback(true);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(0);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                    Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30));
            var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                    "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            6, 12, 2, 1060, Duration.ofSeconds(5)), options);
            var main = new org.apache.bookkeeper.replication.AutoRecoveryMain(
                    conf, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings);
            var clientField = main.getClass().getDeclaredField("bkc");
            clientField.setAccessible(true);
            BookKeeper ownedSdk = (BookKeeper) clientField.get(main);
            var ownedZk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                    ownedSdk.getMetadataClientDriver()).getZk();
            var resourcesField = main.getClass().getDeclaredField("profileResources");
            resourcesField.setAccessible(true);
            var resources = resourcesField.get(main);
            var groupField = resources.getClass().getDeclaredField("group");
            var coldField = resources.getClass().getDeclaredField("cold");
            groupField.setAccessible(true);
            coldField.setAccessible(true);
            var group = (EventLoopGroup) groupField.get(resources);
            var cold = (ThreadPoolExecutor) coldField.get(resources);
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                fixture.replyFault.set(21);
                main.start();
                awaitProfileWorkerStarted(main.getReplicationWorker());
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                var stopping = stopper.submit(() -> {
                    Thread.currentThread().interrupt();
                    main.shutdown();
                    return Thread.currentThread().isInterrupted();
                });
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertTrue(monitor.isLedgerBeingReplicated(44));
                assertFalse(ownedSdk.getClientCtx().isClientClosed());
                assertFalse(group.isShuttingDown());
                assertFalse(cold.isShutdown());
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                assertTrue(stopping.get(10, TimeUnit.SECONDS));
                assertEquals(ZooKeeper.States.CLOSED, ownedZk.getState());
                assertTrue(group.isTerminated());
                assertTrue(cold.isTerminated());
                assertEquals(0, main.getProfilePeerUsage().reservedConnections());
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertFalse(sdk.getClientCtx().isClientClosed());
            } finally {
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                main.shutdown();
                stopper.shutdownNow();
            }
        }
    }

    @Test
    public void profileShutdownInterruptDuringSdkCloseWaitsForActualMetadataClose() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper externalSdk = sdk(fixture)) {
            var conf = profileWorkerConfiguration(fixture);
            conf.setAllowLoopback(true);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(0);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                    "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            6, 12, 2, 1060, Duration.ofSeconds(5)),
                    new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                            Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30)));
            var main = new org.apache.bookkeeper.replication.AutoRecoveryMain(
                    conf, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings);
            var clientField = main.getClass().getDeclaredField("bkc");
            clientField.setAccessible(true);
            var ownedSdk = (BookKeeper) clientField.get(main);
            var ownedZk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                    ownedSdk.getMetadataClientDriver()).getZk();
            var resourcesField = main.getClass().getDeclaredField("profileResources");
            resourcesField.setAccessible(true);
            var resources = resourcesField.get(main);
            var groupField = resources.getClass().getDeclaredField("group");
            var coldField = resources.getClass().getDeclaredField("cold");
            groupField.setAccessible(true);
            coldField.setAccessible(true);
            var group = (EventLoopGroup) groupField.get(resources);
            var cold = (ThreadPoolExecutor) coldField.get(resources);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var owner = new java.util.concurrent.atomic.AtomicReference<Thread>();
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                ownedSdk.getClientCtx().getScheduler().execute(() -> {
                    entered.countDown();
                    await(release);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var stopping = stopper.submit(() -> {
                    owner.set(Thread.currentThread());
                    main.shutdown();
                    return Thread.currentThread().isInterrupted();
                });
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!ownedSdk.getClientCtx().isClientClosed() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertTrue(ownedSdk.getClientCtx().isClientClosed()); // This is admission, not SDK close completion.
                assertEquals(ZooKeeper.States.CONNECTED, ownedZk.getState());
                owner.get().interrupt(); // During close, after its irreversible closed flag.
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertEquals(ZooKeeper.States.CONNECTED, ownedZk.getState());
                assertFalse(group.isShuttingDown());
                assertFalse(cold.isShutdown());
                release.countDown();
                assertTrue(stopping.get(10, TimeUnit.SECONDS));
                assertEquals(ZooKeeper.States.CLOSED, ownedZk.getState());
                assertTrue(group.isTerminated());
                assertTrue(cold.isTerminated());
                assertFalse(externalSdk.getClientCtx().isClientClosed());
            } finally {
                release.countDown();
                main.shutdown();
                ownedSdk.getMetadataClientDriver().close(); // Also reclaim the RED run's stranded session.
                stopper.shutdownNow();
            }
        }
    }

    @Test
    public void profileBootstrapInitializationFailureReclaimsItsNewOwnerThreads() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper sdk = sdk(fixture)) {
            var options = new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                    Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30));
            var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                    "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            6, 12, 2, 1060, Duration.ofSeconds(5)), options);
            var conf = profileWorkerConfiguration(fixture);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(0);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            conf.setAdvertisedAddress("127.0.0.1");
            conf.setAllowLoopback(false); // Fail after owned SDK/pool initialization, at auditor identity creation.
            var existing = java.util.Set.copyOf(Thread.getAllStackTraces().keySet());
            assertThrows(java.net.UnknownHostException.class,
                    () -> new org.apache.bookkeeper.replication.AutoRecoveryMain(conf,
                            org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            java.util.List<Thread> remaining;
            do {
                remaining = Thread.getAllStackTraces().keySet().stream()
                        .filter(thread -> !existing.contains(thread) && thread.isAlive()
                            && thread.getName().startsWith("profile-autorecovery")).toList();
                if (!remaining.isEmpty()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            } while (!remaining.isEmpty() && System.nanoTime() < until);
            assertTrue(remaining.toString(), remaining.isEmpty());
            assertFalse(sdk.getClientCtx().isClientClosed());
        }
    }

    @Test
    public void registeredControlAndDataConnectThroughActualHelloAndFinalColdReconciliation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var peer = registry.readPeer("socket0:3181").peer();
            assertNotNull(peer);
            var control = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                    registry, peer, fixture.cold, 2, Duration.ofSeconds(5));
            var data = ProfileNormalDataClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                    registry, peer, fixture.cold, 2, 1060, Duration.ofSeconds(5));
            try {
                var connected = control.completion().get(5, TimeUnit.SECONDS);
                assertEquals(1, connected.hello().readinessGeneration());
                var source = data.completion().get(5, TimeUnit.SECONDS);
                evidenceEntry(fixture, source, 0, -1, 16, 16);
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        source.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        connected.fence(fixture.context(), bytes(16, 71), fixture.credential)
                            .get(5, TimeUnit.SECONDS).durableResult());
            } finally {
                control.closeAsync().get(5, TimeUnit.SECONDS);
                data.closeAsync().get(5, TimeUnit.SECONDS);
            }
            assertFalse(fixture.clientGroup.isShuttingDown());
        }
    }

    @Test
    public void registeredConnectionDoesNotReuseRemainingHelloBudgetAsRpcTimeout() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var cold = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2));
            var release = new CountDownLatch(1);
            var entered = new CountDownLatch(1);
            try {
                cold.execute(() -> {
                    entered.countDown();
                    await(release);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var attempt = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                        registry, registry.readPeer("socket0:3181").peer(), cold, 1, Duration.ofSeconds(4));
                Thread.sleep(2500); // Consume connection budget without changing the configured per-RPC duration.
                release.countDown();
                var client = attempt.completion().get(5, TimeUnit.SECONDS);
                fixture.faultTarget.set(0);
                fixture.replyFault.set(23);
                var pending = client.fence(fixture.context(), bytes(16, 72), fixture.credential);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, fixture.heldReplies.size());
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> pending.get(2, TimeUnit.SECONDS));
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        pending.get(2, TimeUnit.SECONDS).durableResult());
                attempt.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                cold.shutdownNow();
            }
        }
    }

    @Test
    public void registeredConnectionRejectsEndpointTlsIdentityIncarnationAndReadinessMismatch() throws Exception {
        for (int fault : List.of(0, 1, 2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true);
                    var reader = registrationReader(fixture)) {
                fixture.helloReadinessGeneration = 1;
                int port = ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
                String endpoint = (fault == 0 ? "http://127.0.0.1:" : "tls://127.0.0.1:") + port;
                SslContext tls = fault == 1 ? SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                        .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                        .trustManager(new File(certificate("client-cert.pem"))).protocols("TLSv1.3").build()
                        : fixture.peerTls;
                var registry = registeredPeer(fixture, reader, endpoint, fault == 3 ? 2 : 1,
                        fault == 2 ? bytes(16, 77) : fixture.identities.get(0).incarnation().value());
                var attempt = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, tls,
                        registry, registry.readPeer("socket0:3181").peer(), fixture.cold, 1, Duration.ofSeconds(5));
                assertThrows(ExecutionException.class, () -> attempt.completion().get(5, TimeUnit.SECONDS));
                attempt.closeAsync().get(5, TimeUnit.SECONDS);
                assertEquals(0, fixture.nodeIo.get(0).forces);
                assertFalse(fixture.clientGroup.isShuttingDown());
            }
        }
    }

    @Test
    public void registeredConnectionRejectsMetadataChangedWhileActualHelloReplyIsHeld() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.faultTarget.set(0);
            fixture.replyFault.set(22);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var attempt = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                    registry, registry.readPeer("socket0:3181").peer(), fixture.cold, 1, Duration.ofSeconds(5));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertEquals(1, fixture.heldReplies.size());
            String path = "/socket-readiness/" + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString("socket0:3181".getBytes(StandardCharsets.UTF_8));
            fixture.zk.delete(path + "/writable", -1);
            fixture.replyFault.set(0);
            fixture.heldReplies.forEach(Runnable::run);
            assertThrows(ExecutionException.class, () -> attempt.completion().get(5, TimeUnit.SECONDS));
            attempt.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(0, fixture.nodeIo.get(0).forces);
        }
    }

    @Test
    public void registeredCancellationRetainsQueuedColdWorkUntilActualTerminal() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.faultTarget.set(0);
            fixture.replyFault.set(22);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var cold = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
            var release = new CountDownLatch(1);
            try {
                var attempt = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                        registry, registry.readPeer("socket0:3181").peer(), cold, 1, Duration.ofSeconds(5));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, fixture.heldReplies.size());
                var blocking = new CountDownLatch(1);
                cold.execute(() -> {
                    blocking.countDown();
                    await(release);
                });
                assertTrue(blocking.await(5, TimeUnit.SECONDS));
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (cold.getQueue().isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, cold.getQueue().size());
                assertTrue(attempt.completion().cancel(false));
                var closing = attempt.closeAsync();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> closing.get(150, TimeUnit.MILLISECONDS));
                release.countDown();
                closing.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                cold.shutdownNow();
            }
        }
    }

    @Test
    public void registeredCloseCallbackCanAwaitDrainWithoutHoldingLifecycleLock() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.faultTarget.set(0);
            fixture.replyFault.set(22);
            String endpoint = "tls://127.0.0.1:"
                    + ((InetSocketAddress) fixture.listeners.get(0).localAddress()).getPort();
            var registry = registeredPeer(fixture, reader, endpoint, 1,
                    fixture.identities.get(0).incarnation().value());
            var cold = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
            var release = new CountDownLatch(1);
            try {
                var attempt = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                        registry, registry.readPeer("socket0:3181").peer(), cold, 1, Duration.ofSeconds(5));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, fixture.heldReplies.size());
                var blocking = new CountDownLatch(1);
                cold.execute(() -> {
                    blocking.countDown();
                    await(release);
                });
                assertTrue(blocking.await(5, TimeUnit.SECONDS));
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (cold.getQueue().isEmpty() && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(1, cold.getQueue().size());
                var callbackEntered = new CountDownLatch(1);
                attempt.completion().whenComplete((value, failure) -> {
                    callbackEntered.countDown();
                    try {
                        attempt.closeAsync().get(3, TimeUnit.SECONDS);
                    } catch (Exception blocked) {
                        throw new java.util.concurrent.CompletionException(blocked);
                    }
                });
                var stopping = CompletableFuture.runAsync(() -> attempt.closeAsync());
                assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
                var socketField = attempt.getClass().getDeclaredField("socket");
                socketField.setAccessible(true);
                var owner = socketField.get(attempt);
                var channelField = owner.getClass().getDeclaredField("channel");
                channelField.setAccessible(true);
                Channel channel = (Channel) channelField.get(owner);
                channel.closeFuture().get(5, TimeUnit.SECONDS);
                channel.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
                assertFalse(stopping.isDone());
                release.countDown();
                stopping.get(2, TimeUnit.SECONDS);
                attempt.closeAsync().get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                cold.shutdownNow();
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
    public void closeWaitsForQueuedDataBorrowAndDoesNotChangeTheOriginalInput() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var client = dataClient(fixture, 0, 1, Duration.ofSeconds(5));
            var ownerField = ProfileNormalDataClient.class.getDeclaredField("socket");
            ownerField.setAccessible(true);
            var owner = ownerField.get(client);
            var channelField = owner.getClass().getDeclaredField("channel");
            channelField.setAccessible(true);
            Channel channel = (Channel) channelField.get(owner);
            var queued = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var source = (org.apache.bookkeeper.util.ByteBufList) entry(fixture, 0, 16);
            byte[] original = new byte[source.readableBytes()];
            source.getBytes(original);
            int forces = files(fixture.nodes.get(0)).dataIo.forces;
            try {
                channel.eventLoop().execute(() -> {
                    queued.countDown();
                    await(release);
                });
                assertTrue(queued.await(5, TimeUnit.SECONDS));
                var pending = client.addNormal(fixture.context(), fixture.credential, 0, source);
                assertEquals(2, source.getBuffer(0).refCnt());
                var closed = client.closeAsync();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> closed.get(150, TimeUnit.MILLISECONDS));
                release.countDown();
                closed.get(5, TimeUnit.SECONDS);
                assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
                assertEquals(1, source.getBuffer(0).refCnt());
                byte[] after = new byte[source.readableBytes()];
                source.getBytes(after);
                assertArrayEquals(original, after);
                assertEquals(forces, files(fixture.nodes.get(0)).dataIo.forces);
            } finally {
                release.countDown();
                source.release();
            }
        }
    }

    @Test
    public void closeWaitsForActualWritePromiseEvenAfterReplyAndTransportClose() throws Exception {
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
                var closed = client.closeAsync();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> closed.get(150, TimeUnit.MILLISECONDS));
                assertTrue(observed.get().getInt(4) != 0); // Raw owned output is still in use by its promise.
                held.get().setSuccess();
                channel.eventLoop().submit(() -> { }).get(5, TimeUnit.SECONDS);
                for (int offset = 0; offset < observed.get().capacity(); offset++) {
                    assertEquals(0, observed.get().getByte(offset));
                }
                closed.get(5, TimeUnit.SECONDS);
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

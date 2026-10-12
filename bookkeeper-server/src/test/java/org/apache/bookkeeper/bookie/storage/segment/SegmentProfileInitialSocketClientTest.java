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
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
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
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
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
        final List<SegmentProfileControlConfig> controlConfigs = new ArrayList<>();
        TLSContextFactory serverTls;
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
                serverTls = TLSContextFactory.forProfileServer();
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
                    var deleteReader = reservations.deleteAuthorityReader(scope.targetBookie(),
                            scope.targetStorageIncarnation(), metadataContext);
                    var control = new SegmentProfileControlConfig(VALIDATOR,
                            (caller, operation) -> caller.equals(principal)
                                    && (operation == ProfileControlOperation.INSTALL
                                        || operation == ProfileControlOperation.ACTIVATE_INITIAL
                                        || operation == ProfileControlOperation.ACTIVATE_REPLACEMENT
                                        || operation == ProfileControlOperation.RECOVERY_GRANT
                                        || operation == ProfileControlOperation.RECOVERY_GRANT_CLOSE
                                        || operation == ProfileControlOperation.RECOVERY_TERMINAL_CLOSE
                                        || operation == ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY),
                            key -> {
                                authorityReads.incrementAndGet();
                                if (key.purpose() == ProfileAuthorityPurpose.TOMBSTONE_OR_DELETE) {
                                    return deleteReader.read(key);
                                }
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
                    controlConfigs.add(control);
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
                                                    int fault = replyFault.get() == 21 || replyFault.get() == 26
                                                            ? replyFault.get()
                                                            : target == faultTarget.get() ? replyFault.get() : 0;
                                                    if (fault == 1
                                                            && (op == ProfileOperation.ACTIVATE_INITIAL.wireValue()
                                                                || op == ProfileOperation.ACTIVATE_REPLACEMENT
                                                                    .wireValue())) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        return;
                                                    }
                                                    if (fault == 26 && op == ProfileOperation.WRITE_LAC.wireValue()) {
                                                        heldReplies.add(() -> ctx.executor().execute(
                                                                () -> ctx.writeAndFlush(frame, promise)));
                                                        return;
                                                    }
                                                    if (fault == 25 && op == ProfileOperation.WRITE_LAC.wireValue()) {
                                                        replyFault.compareAndSet(25, 0);
                                                        frame.release();
                                                        promise.setSuccess();
                                                        ctx.close();
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
                                                    if (fault == 24 && op == ProfileOperation.TOMBSTONE_OR_DELETE_APPLY
                                                            .wireValue()) {
                                                        frame.release();
                                                        promise.setSuccess();
                                                        ctx.close();
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
                if (channel.isOpen()) {
                    channel.close().syncUninterruptibly();
                } else {
                    channel.closeFuture().syncUninterruptibly();
                }
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

    @Test
    public void writerStopWaitsForActualDataWritePromiseAfterSdkQuorum() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(),
                        dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                    1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
            Channel channel = dataChannel(data.get(fixture.identities.get(0).bookieId()));
            var held = new AtomicReference<ChannelPromise>();
            var observed = new AtomicReference<ByteBuf>();
            var written = new CountDownLatch(1);
            channel.eventLoop().submit(() -> channel.pipeline().addAfter(
                    channel.pipeline().context(SslHandler.class).name(), "holdWriterAddPromise",
                    new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                            ByteBuf body = (ByteBuf) message;
                            if (body.getUnsignedShort(body.readerIndex() + 14)
                                    == ProfileOperation.ADD_NORMAL.wireValue() && held.compareAndSet(null, promise)) {
                                observed.set(body.retainedDuplicate());
                                var actual = ctx.newPromise();
                                actual.addListener(done -> written.countDown());
                                ctx.write(body, actual);
                            } else {
                                ctx.write(body, promise);
                            }
                        }
                    })).get(5, TimeUnit.SECONDS);
            try {
                assertEquals(0, handle.appendAsync(Unpooled.wrappedBuffer(bytes(16, 8)))
                        .get(5, TimeUnit.SECONDS).longValue());
                assertTrue(written.await(5, TimeUnit.SECONDS));
                var firstStop = handle.stopLocalWrites();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> firstStop.get(150, TimeUnit.MILLISECONDS));
                assertTrue(firstStop.cancel(false));
                var actualStop = handle.stopLocalWrites();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> actualStop.get(150, TimeUnit.MILLISECONDS));
                assertTrue(channel.isActive()); // The handle borrows this peer; it must not close its socket.
                assertTrue(observed.get().getInt(4) != 0);
                held.get().setSuccess();
                actualStop.get(5, TimeUnit.SECONDS);
                for (int offset = 0; offset < observed.get().capacity(); offset++) {
                    assertEquals(0, observed.get().getByte(offset));
                }
                assertEquals(LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                evidenceEntry(fixture, data.get(fixture.identities.get(0).bookieId()), 0, -1, 16, 16);
            } finally {
                if (held.get() != null) {
                    held.get().trySuccess();
                }
                if (observed.get() != null) {
                    observed.get().release();
                }
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void writerStopWaitsForQueuedDataBorrowAndFailsPendingAppend() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(),
                        dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                    1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
            Channel channel = dataChannel(data.get(fixture.identities.get(0).bookieId()));
            var queued = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            try {
                channel.eventLoop().execute(() -> {
                    queued.countDown();
                    await(release);
                });
                assertTrue(queued.await(5, TimeUnit.SECONDS));
                var append = sdkAdd(handle, bytes(16, 8));
                sdk.getClientCtx().getMainWorkerPool().submitOrdered(44L, () -> null).get(5, TimeUnit.SECONDS);
                assertFalse(append.isDone());
                assertTrue(fixture.dataResults.get(0).isEmpty());
                var stopping = handle.stopLocalWrites();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertThrows(ExecutionException.class, () -> append.get(5, TimeUnit.SECONDS));
                assertTrue(handle.hasUncertainWrites());
                release.countDown();
                stopping.get(5, TimeUnit.SECONDS);
                assertEquals(1, fixture.dataResults.get(0).size());
                assertTrue(channel.isActive());
                assertEquals(-1, handle.getLastAddConfirmed());
                assertEquals(0, handle.getLength());
                assertEquals(LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            } finally {
                release.countDown();
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void ordinaryWriterCloseFreezesConfirmedPrefixAndIgnoresPhysicalUnconfirmedTail() throws Exception {
        for (int ack : List.of(2, 3)) {
            for (boolean empty : List.of(false, true)) {
                try (Fixture fixture = new Fixture(false, false, true, 3, ack, true, true);
                        BookKeeper sdk = sdk(fixture)) {
                    fixture.connectAll(Duration.ofSeconds(5));
                    var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                    var data = new HashMap<BookieId, ProfileNormalDataClient>();
                    for (int node = 0; node < 3; node++) {
                        data.put(fixture.identities.get(node).bookieId(),
                                dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                    }
                    var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                            1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
                    var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                    handle.enableWriterClose(coordinator, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                            4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
                    long prefix = empty ? -1 : 0;
                    long length = empty ? 0 : 16;
                    if (!empty) {
                        assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                    }
                    for (var peer : data.values()) {
                        evidenceEntry(fixture, peer, prefix + 1, prefix, length + 16, 16);
                    }
                    assertEquals(prefix, handle.getLastAddConfirmed());
                    handle.closeAsync().get(30, TimeUnit.SECONDS);
                    handle.closeAsync().get(5, TimeUnit.SECONDS);
                    var closed = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertEquals(LedgerMetadata.State.CLOSED, closed.getState());
                    assertEquals(prefix, closed.getLastEntryId());
                    assertEquals(length, closed.getLength());
                    var context = org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.decode(
                            closed.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY));
                    assertTrue(context.writerClose());
                    assertEquals(prefix, handle.writerCloseRequest().prefixEntryId());
                    assertEquals(length, handle.writerCloseRequest().prefixLength());
                    var cut = org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.WriterClose.decode(
                            closed.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.WRITER_CLOSE_KEY));
                    assertEquals(prefix, cut.prefixEntryId());
                    assertEquals(length, cut.prefixLength());
                    var reader = coordinator.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                            .get(5, TimeUnit.SECONDS);
                    assertEquals(prefix, reader.getLastAddConfirmed());
                    assertEquals(length, reader.getLength());
                    if (!empty) {
                        try (var entries = reader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                            assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
                        }
                    }
                    assertThrows(ExecutionException.class,
                            () -> reader.readUnconfirmedAsync(prefix + 1, prefix + 1).get(5, TimeUnit.SECONDS));
                    reader.closeAsync().get(5, TimeUnit.SECONDS);
                    for (int node = 0; node < 3; node++) {
                        var config = files(fixture.nodes.get(node)).config();
                        fixture.nodes.get(node).close();
                        try (var replay = new IsolatedSegmentStorage(config, fixture.identities.get(node),
                                new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                                new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                        2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                                new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                            replay.restart();
                            assertFalse(replay.profileReadVisible(fixture.context(), prefix + 1));
                            if (!empty) {
                                assertTrue(replay.profileReadVisible(fixture.context(), prefix));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void ordinaryWriterCloseWaitsForActualDataOutputAndSurvivesCallerCancellation() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                    1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            handle.enableWriterClose(coordinator, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                    4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
            Channel channel = dataChannel(data.get(fixture.identities.get(0).bookieId()));
            var held = new AtomicReference<ChannelPromise>();
            var observed = new AtomicReference<ByteBuf>();
            var written = new CountDownLatch(1);
            channel.eventLoop().submit(() -> channel.pipeline().addAfter(
                    channel.pipeline().context(SslHandler.class).name(), "holdNormalCloseAddPromise",
                    new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                            ByteBuf body = (ByteBuf) message;
                            if (body.getUnsignedShort(body.readerIndex() + 14)
                                    == ProfileOperation.ADD_NORMAL.wireValue() && held.compareAndSet(null, promise)) {
                                observed.set(body.retainedDuplicate());
                                var actual = ctx.newPromise();
                                actual.addListener(done -> written.countDown());
                                ctx.write(body, actual);
                            } else {
                                ctx.write(body, promise);
                            }
                        }
                    })).get(5, TimeUnit.SECONDS);
            try {
                assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                assertTrue(written.await(5, TimeUnit.SECONDS));
                var first = handle.closeAsync();
                assertThrows(java.util.concurrent.TimeoutException.class, () -> first.get(150, TimeUnit.MILLISECONDS));
                assertTrue(first.cancel(false));
                var actual = handle.closeAsync();
                assertThrows(java.util.concurrent.TimeoutException.class, () -> actual.get(150, TimeUnit.MILLISECONDS));
                assertEquals(available.metadata().getVersion(),
                        fixture.manager.readLedgerMetadata(44).get().getVersion());
                assertEquals(0, handle.writerCloseRequest().prefixEntryId());
                assertEquals(16, handle.writerCloseRequest().prefixLength());
                assertThrows(ExecutionException.class,
                        () -> sdkAdd(handle, bytes(16, 9)).get(5, TimeUnit.SECONDS));
                held.get().setSuccess();
                actual.get(30, TimeUnit.SECONDS);
                assertTrue(channel.isActive());
                assertEquals(LedgerMetadata.State.CLOSED,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                for (int offset = 0; offset < observed.get().capacity(); offset++) {
                    assertEquals(0, observed.get().getByte(offset));
                }
            } finally {
                if (held.get() != null) {
                    held.get().trySuccess();
                }
                if (observed.get() != null) {
                    observed.get().release();
                }
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class HoldWriterEnterClient extends ZooKeeper {
        final CountDownLatch entered = new CountDownLatch(1);
        volatile Runnable release;
        private boolean hold = true;

        HoldWriterEnterClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var ops = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(ops::add);
            boolean pause = hold && ops.stream().anyMatch(op -> op.getType() == ZooDefs.OpCode.setData);
            if (pause) {
                hold = false;
            }
            super.multi(ops, (code, path, ignored, results) -> {
                if (pause && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()) {
                    release = () -> callback.processResult(code, path, context, results);
                    entered.countDown();
                } else {
                    callback.processResult(code, path, context, results);
                }
            }, context);
        }
    }

    @Test
    public void ordinaryWriterCloseCutSurvivesTheEnterReplyWindowAndFreshRecoverySession() throws Exception {
        for (int mode : List.of(0, 1, 2)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                }
                var watcher = new ZooKeeperWatcherBase(10000, false);
                try (var held = new HoldWriterEnterClient(fixture.cluster.getZooKeeperConnectString(), watcher)) {
                    var metadata = new ProfileLedgerMetadataStore(held, (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var store = new ZKProfileReservationStore(held, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    var coordinator = new ProfileRecoveryCoordinator(store, metadata, fixture.cold);
                    var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                            1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
                    handle.enableWriterClose(coordinator, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                            4, Duration.ofMillis(500)).get(5, TimeUnit.SECONDS);
                    assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                    for (var peer : data.values()) {
                        evidenceEntry(fixture, peer, 1, 0, 32, 16);
                    }
                    var closing = handle.closeAsync();
                    try {
                        assertTrue(held.entered.await(5, TimeUnit.SECONDS));
                        var frozen = fixture.manager.readLedgerMetadata(44).get();
                        var value = frozen.getValue();
                        assertEquals(LedgerMetadata.State.IN_RECOVERY, value.getState());
                        var original = value.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY);
                        assertNotNull(value.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.WRITER_CLOSE_KEY));
                        assertNull(value.getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary.KEY));
                        assertThrows(ExecutionException.class, () -> closing.get(5, TimeUnit.SECONDS));
                        if (mode != 0) {
                            var custom = new java.util.TreeMap<>(value.getCustomMetadata());
                            if (mode == 1) {
                                custom.remove(org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary
                                        .WRITER_CLOSE_KEY);
                            } else {
                                custom.put(org.apache.bookkeeper.discover.profile.ProfileRecoveryBoundary
                                        .WRITER_CLOSE_KEY, new org.apache.bookkeeper.discover.profile
                                            .ProfileRecoveryBoundary.WriterClose(1, 32).encode());
                            }
                            fixture.manager.writeLedgerMetadata(44, LedgerMetadataBuilder.from(value)
                                    .withCustomMetadata(custom).build(), frozen.getVersion()).get();
                        }
                        held.release.run();
                        held.release = null;
                        var freshMetadata = new ProfileLedgerMetadataStore(fixture.zk,
                                (AbstractZkLedgerManager) fixture.manager, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                Duration.ofSeconds(5));
                        var fresh = new ProfileRecoveryCoordinator(fixture.reservations, freshMetadata, fixture.cold);
                        var result = fresh.recover(44, bytes(16, 45), sdk.getClientCtx(), fixture.descriptorBytes,
                                VALIDATOR, fixture.credential, data, fixture.clients, 4, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS);
                        if (mode == 0) {
                            assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                                    result.status());
                            assertEquals(0, result.prefixEntryId());
                            assertEquals(16, result.prefixLength());
                            var closed = fixture.manager.readLedgerMetadata(44).get().getValue();
                            assertArrayEquals(original, closed.getCustomMetadata().get(
                                    org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY));
                            assertEquals(0, closed.getLastEntryId());
                            assertEquals(16, closed.getLength());
                        } else {
                            assertTrue(result.status() == ProfileRecoveryCoordinator.RecoveryStatus.QUARANTINED
                                    || result.status() == ProfileRecoveryCoordinator.RecoveryStatus.CONFLICT);
                            assertEquals(LedgerMetadata.State.IN_RECOVERY,
                                    fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                        }
                    } finally {
                        if (held.release != null) {
                            held.release.run();
                        }
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    private static final class LostWriterClosedReplyClient extends ZooKeeper {
        private final String metadataPath;
        volatile boolean dropped;

        LostWriterClosedReplyClient(String connect, ZooKeeperWatcherBase watcher, String metadataPath)
                throws Exception {
            super(connect, 10000, watcher);
            this.metadataPath = metadataPath;
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var ops = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(ops::add);
            boolean lose = false;
            for (var op : ops) {
                if (!dropped && op.getType() == ZooDefs.OpCode.setData && op.getPath().equals(metadataPath)) {
                    try {
                        var request = (org.apache.zookeeper.proto.SetDataRequest) op.toRequestRecord();
                        var value = new org.apache.bookkeeper.meta.LedgerMetadataSerDe()
                                .parseConfig(request.getData(), 44, java.util.Optional.empty());
                        lose = value.getState() == LedgerMetadata.State.CLOSED;
                    } catch (java.io.IOException malformed) {
                        throw new IllegalArgumentException(malformed);
                    }
                }
            }
            boolean lost = lose;
            super.multi(ops, (code, path, ignored, results) -> {
                if (lost && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()) {
                    dropped = true;
                    callback.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue(),
                            path, context, results);
                } else {
                    callback.processResult(code, path, context, results);
                }
            }, context);
        }
    }

    @Test
    public void ordinaryWriterClosePreservesHistoricalReplacementAndReconcilesLostClosedReply() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 4, 3, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 4; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var next = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                    data.get(fixture.identities.get(3).bookieId()));
            var current = new HashMap<>(data);
            current.remove(fixture.identities.get(3).bookieId());
            var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, current, 8,
                    1024, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
            var replacementMetadata = new ProfileLedgerMetadataStore(fixture.zk,
                    (AbstractZkLedgerManager) fixture.manager, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            handle.enableReplacement(new ProfileReplacementCoordinator(fixture.reservations, replacementMetadata,
                    VALIDATOR, fixture.cold), List.of(next), fixture.descriptorBytes, 1).get(5, TimeUnit.SECONDS);
            var watcher = new ZooKeeperWatcherBase(10000, false);
            try (var lost = new LostWriterClosedReplyClient(fixture.cluster.getZooKeeperConnectString(), watcher,
                    ((AbstractZkLedgerManager) fixture.manager).getLedgerPath(44))) {
                var metadata = new ProfileLedgerMetadataStore(lost, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var store = new ZKProfileReservationStore(lost, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                var coordinator = new ProfileRecoveryCoordinator(store, metadata, fixture.cold);
                handle.enableWriterClose(coordinator, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                        4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
                try {
                    assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                    fixture.expectedFailedDataSessions.add(2);
                    files(fixture.nodes.get(2)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                    assertEquals(1, sdkAdd(handle, bytes(16, 9)).get(10, TimeUnit.SECONDS).longValue());
                    var expected = fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles();
                    assertEquals(List.of(0L, 1L), new ArrayList<>(expected.keySet()));
                    for (int node : List.of(0, 1, 3)) {
                        evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), 2, 1, 48, 16);
                    }
                    handle.closeAsync().get(30, TimeUnit.SECONDS);
                    assertTrue(lost.dropped);
                    var closed = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertEquals(LedgerMetadata.State.CLOSED, closed.getState());
                    assertEquals(expected, closed.getAllEnsembles());
                    assertEquals(1, closed.getLastEntryId());
                    assertEquals(32, closed.getLength());
                    var read = coordinator.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                            .get(5, TimeUnit.SECONDS);
                    try (var entries = read.readAsync(0, 1).get(5, TimeUnit.SECONDS)) {
                        assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
                        assertArrayEquals(bytes(16, 9), entries.getEntry(1).getEntryBytes());
                    }
                    read.closeAsync().get(5, TimeUnit.SECONDS);
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void logicalDeleteKeepsOfflineObligationsAndDoesNotPromiseAnOldReaderBarrier() throws Exception {
        for (boolean writerClose : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var recovery = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                        fixture.reservations, metadata, fixture.cold);
                var originalRoot = fixture.reservations.read(44).snapshot();
                assertEquals(ZKProfileReservationStore.DeleteStatus.PRECONDITION_FAILED, deletion.deleteLedgerAsync(
                        44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                assertEquals(originalRoot, fixture.reservations.read(44).snapshot());
                if (writerClose) {
                    var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                            1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
                    handle.enableWriterClose(recovery, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                            4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
                    assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(5, TimeUnit.SECONDS).longValue());
                    handle.closeAsync().get(30, TimeUnit.SECONDS);
                } else {
                    for (var peer : data.values()) {
                        evidenceEntry(fixture, peer, 0, -1, 16, 16);
                    }
                    assertEquals(ProfileRecoveryCoordinator.RecoveryStatus.RECOVERED_AND_CLOSED,
                            recovery.recover(44, bytes(16, 51), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                                    fixture.credential, data, fixture.clients, 4, Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS).status());
                }
                var closed = fixture.manager.readLedgerMetadata(44).get();
                var context = org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.decode(closed.getValue()
                        .getCustomMetadata().get(org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY));
                var reader = recovery.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                        .get(5, TimeUnit.SECONDS);
                try {
                    fixture.listeners.get(2).close().syncUninterruptibly();
                    fixture.clients.get("socket2:3181").closeAsync().get(5, TimeUnit.SECONDS);
                    data.get(fixture.identities.get(2).bookieId()).closeAsync().get(5, TimeUnit.SECONDS);
                    var result = deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                            Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS);
                    assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, result.status());
                    assertEquals(3, result.manifest().targets().size());
                    assertTrue(result.manifest().targets().stream().anyMatch(target ->
                            target.bookie().equals("socket2:3181") && java.util.Arrays.equals(target.incarnation(),
                                fixture.identities.get(2).incarnation().value())));
                    var deleted = fixture.manager.readLedgerMetadata(44).get();
                    assertEquals(LedgerMetadata.State.CLOSED, deleted.getValue().getState());
                    assertEquals(0, deleted.getValue().getLastEntryId());
                    assertEquals(16, deleted.getValue().getLength());
                    assertEquals(closed.getValue().getAllEnsembles(), deleted.getValue().getAllEnsembles());
                    assertEquals(((org.apache.bookkeeper.versioning.LongVersion) closed.getVersion())
                            .getLongVersion() + 2,
                            ((org.apache.bookkeeper.versioning.LongVersion) deleted.getVersion()).getLongVersion());
                    assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED,
                            deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS).status());
                    assertEquals(deleted.getVersion(), fixture.manager.readLedgerMetadata(44).get().getVersion());
                    assertEquals(ZKProfileReservationStore.DeleteStatus.CONFLICT,
                            deletion.deleteLedgerAsync(44, bytes(16, 102), sdk.getClientCtx(), Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS).status());
                    assertThrows(ExecutionException.class,
                            () -> recovery.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                                .get(5, TimeUnit.SECONDS));
                    try (var entries = reader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                        assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
                    }
                    assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED,
                            fixture.reservations.readClosedRecovery(metadata, context).outcome());
                } finally {
                    reader.closeAsync().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static final class DeleteRegistrationCutClient extends ZooKeeper {
        volatile Runnable beforeWritable;
        volatile boolean loseWritable;
        volatile boolean dropped;

        DeleteRegistrationCutClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var copied = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(copied::add);
            boolean writable = copied.stream().anyMatch(op -> op.getType() == org.apache.zookeeper.ZooDefs.OpCode.create
                    && op.getPath().endsWith("/writable"));
            var before = writable ? beforeWritable : null;
            if (before != null) {
                beforeWritable = null;
                before.run();
            }
            super.multi(copied, (code, path, ignored, results) -> {
                if (writable && loseWritable && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()) {
                    loseWritable = false;
                    dropped = true;
                    callback.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue(),
                            path, context, results);
                } else {
                    callback.processResult(code, path, context, results);
                }
            }, context);
        }
    }

    private static StartupReadinessInput startupPolicy(
            IsolatedSegmentConfig config, SegmentFileHeader identity) {
        var gen = new Generation(1);
        var manifest = IsolatedSegmentStorage.startupDeviceManifestIdentity(config, identity);
        var compatibility = new BookieCompatibilityFact(
                identity.bookieId().toString(), identity.incarnation(), EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, 1,
                List.of(IsolatedSegmentStorage.STARTUP_BASE_FORMAT_FEATURE), manifest, gen, gen, gen, 1, 1,
                BookieCompatibilityFact.MigrationState.FORMAT_READY, gen);
        var devices = new RequiredDeviceManifest(manifest, gen, List.of(
                new RequiredDeviceManifest.RequiredDeviceFact(
                        identity.arenaId().toString(), identity.incarnation(), gen,
                        RequiredDeviceManifest.DeviceStatus.CORRUPT,
                        1, 1, 1, gen, List.of(IsolatedSegmentStorage.STARTUP_BASE_FORMAT_FEATURE))));
        var missing = LocalRecoveryFacts.RecoveryStatus.MISSING;
        var recovery = new LocalRecoveryFacts(identity.incarnation(),
                gen, missing, missing, missing, gen);
        var oldRoots = java.util.Arrays.stream(
                NewScopeFallbackManifest.RootKind.values())
                .map(kind -> new NewScopeFallbackManifest.StorageRoot(
                        kind, "/old/" + kind.name())).sorted(java.util.Comparator.comparing(
                            NewScopeFallbackManifest.StorageRoot::opaquePath))
                .toList();
        var newRoots = oldRoots.stream().map(root ->
                new NewScopeFallbackManifest.StorageRoot(
                        root.kind(), "/new/" + root.kind().name())).toList();
        var fallback = new NewScopeFallbackManifest("old",
                identity.bookieId().toString(), new StorageIncarnation(bytes(16, 111)),
                identity.incarnation(), oldRoots, newRoots,
                new NewScopeFallbackManifest.CredentialScope("old-user"),
                new NewScopeFallbackManifest.CredentialScope("new-user"),
                NewScopeFallbackManifest.OldScopeDisposition.DRAINED,
                NewScopeFallbackManifest.AccessLevel.NONE,
                NewScopeFallbackManifest.AccessLevel.READ_WRITE);
        return new StartupReadinessInput(compatibility, devices, recovery,
                fallback, new RollbackSafetyFacts(
                        false, false, false, false, false, false, false), gen, gen, 1, 1,
                List.of(IsolatedSegmentStorage.STARTUP_BASE_FORMAT_FEATURE), "startup", List.of(1));
    }

    @Test
    public void ownedProfileListenerWaitsForActualColdRequestAndRetiresConnectionAdmission() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var storage = fixture.nodes.get(0);
            var old = fixture.controlConfigs.get(0);
            var held = new SegmentProfileControlConfig(old.validator(), old.staticAuthorizer(), key -> {
                entered.countDown();
                try {
                    assertTrue(release.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return old.controlStore().read(key);
            }, old.exactAuthorizer(), old.executor(), old.timeout());
            fixture.listeners.get(0).close().sync();
            var budget = fixture.budgets.get(0);
            var listener = new SegmentProfileListener(storage, fixture.serverTls, budget,
                    new SegmentProfileListener.Settings(131072, 64, "socket-test", 0),
                    new InetSocketAddress("127.0.0.1", 0), fixture.acceptor, fixture.worker, held, null);
            try {
                var address = listener.bound().get(5, TimeUnit.SECONDS);
                var identity = fixture.identities.get(0);
                try (var client = ProfileInitialControlClient.connect(fixture.clientGroup, fixture.peerTls, address,
                        "apache.bookkeeper.org", identity.bookieId().toString(), identity.incarnation().value(),
                        4, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS)) {
                    var request = client.install(fixture.install(0));
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    var caller = listener.closeAsync();
                    assertFalse(caller.isDone());
                    assertTrue(budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED) > 0);
                    caller.complete(null);
                    assertFalse(listener.closeAsync().isDone());
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> listener.closeAsync().get(100, TimeUnit.MILLISECONDS));
                    release.countDown();
                    listener.closeAsync().get(10, TimeUnit.SECONDS);
                    assertTrue(request.isDone());
                    for (var lane : ProfileReceiveBudget.Lane.values()) {
                        assertEquals(0, budget.used(lane));
                        assertEquals(0, budget.usedConnections(lane));
                    }
                    assertFalse(fixture.worker.isShuttingDown());
                    assertFalse(fixture.cold.isShutdown());
                    assertThrows(java.io.IOException.class, () -> {
                        try (var connection = new java.net.Socket()) {
                            connection.connect(address, 1000);
                        }
                    });
                }
            } finally {
                release.countDown();
                listener.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void ownedProfileListenerBindFailureDrainsItsBudgetAndKeepsBorrowedGraphAlive() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            var budget = new ProfileReceiveBudget(524288, 131072, 262144, 8, 8, 8);
            var listener = new SegmentProfileListener(fixture.nodes.get(0), fixture.serverTls, budget,
                    new SegmentProfileListener.Settings(131072, 64, "socket-test", 0),
                    (InetSocketAddress) fixture.listeners.get(0).localAddress(), fixture.acceptor, fixture.worker,
                    fixture.controlConfigs.get(0), null);
            assertThrows(ExecutionException.class, () -> listener.bound().get(5, TimeUnit.SECONDS));
            listener.closeAsync().get(5, TimeUnit.SECONDS);
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(10, TimeUnit.SECONDS);
            for (var lane : ProfileReceiveBudget.Lane.values()) {
                assertEquals(0, budget.used(lane));
                assertEquals(0, budget.usedConnections(lane));
            }
        }
    }

    @Test
    public void ownedProfileListenerWaitsForAcceptedSocketAlreadyQueuedToWorkers() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true)) {
            fixture.listeners.get(0).close().sync();
            var budget = fixture.budgets.get(0);
            var listener = new SegmentProfileListener(fixture.nodes.get(0), fixture.serverTls, budget,
                    new SegmentProfileListener.Settings(131072, 64, "socket-test", 0),
                    new InetSocketAddress("127.0.0.1", 0), fixture.acceptor, fixture.worker,
                    fixture.controlConfigs.get(0), null);
            var release = new CountDownLatch(1);
            var blocked = new CountDownLatch(2);
            var accepted = new CountDownLatch(1);
            try {
                var address = listener.bound().get(5, TimeUnit.SECONDS);
                var field = SegmentProfileListener.class.getDeclaredField("listener");
                field.setAccessible(true);
                Channel parent = (Channel) field.get(listener);
                parent.eventLoop().submit(() -> parent.pipeline().addFirst("observeAccepted",
                        new io.netty.channel.ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
                                accepted.countDown();
                                context.fireChannelRead(message);
                            }
                        })).get(5, TimeUnit.SECONDS);
                for (var loop : fixture.worker) {
                    loop.execute(() -> {
                        blocked.countDown();
                        try {
                            assertTrue(release.await(20, TimeUnit.SECONDS));
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    });
                }
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                try (var connection = new java.net.Socket()) {
                    connection.connect(address, 1000);
                    assertTrue(accepted.await(5, TimeUnit.SECONDS));
                    var closing = listener.closeAsync();
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> closing.get(100, TimeUnit.MILLISECONDS));
                    for (var lane : ProfileReceiveBudget.Lane.values()) {
                        assertEquals(0, budget.usedConnections(lane));
                    }
                    release.countDown();
                    closing.get(5, TimeUnit.SECONDS);
                    connection.setSoTimeout(1000);
                    assertEquals(-1, connection.getInputStream().read());
                }
            } finally {
                release.countDown();
                listener.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class HeldServicePublicationClient extends ZooKeeper {
        final CountDownLatch submitted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean armed;

        HeldServicePublicationClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var copied = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(copied::add);
            if (armed && copied.stream().anyMatch(op -> op.getPath().startsWith("/socket-service-readiness/"))) {
                armed = false;
                submitted.countDown();
                await(release);
            }
            super.multi(copied, callback, context);
        }
    }

    private final class ServiceEpisode implements AutoCloseable {
        final ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
        final ZooKeeper client;
        final ZKProfileStartupRequirementsStore requirements;
        final ZKProfileRegistrationStore persistent;
        final ZKProfileEphemeralRegistrationStore publisher;
        final SegmentProfileService service;
        final ProfileReceiveBudget budget = new ProfileReceiveBudget(524288, 131072, 262144, 8, 8, 8);
        final IsolatedSegmentConfig config;
        final ServerConfiguration bootstrapConfiguration;
        final SegmentProfileService.Startup startup;
        final SegmentProfileService.Transport transport;
        final boolean ownsResources;
        org.apache.bookkeeper.server.EmbeddedServer embedded;

        ServiceEpisode(Fixture fixture, int target, IsolatedSegmentConfig config,
                StartupReadinessInput requested, BookieCompatibilityFact requiredCompatibility,
                InetSocketAddress address, java.util.concurrent.ScheduledExecutorService scheduler) throws Exception {
            this(fixture, target, config, requested, requiredCompatibility, address, scheduler, false);
        }

        ServiceEpisode(Fixture fixture, int target, IsolatedSegmentConfig config,
                StartupReadinessInput requested, BookieCompatibilityFact requiredCompatibility,
                InetSocketAddress address, java.util.concurrent.ScheduledExecutorService scheduler,
                boolean holdPublication) throws Exception {
            this(fixture, target, config, requested, requiredCompatibility, address, scheduler, holdPublication, false);
        }

        ServiceEpisode(Fixture fixture, int target, IsolatedSegmentConfig config,
                StartupReadinessInput requested, BookieCompatibilityFact requiredCompatibility,
                InetSocketAddress address, java.util.concurrent.ScheduledExecutorService scheduler,
                boolean holdPublication, boolean ownsResources) throws Exception {
            this.config = config;
            this.ownsResources = ownsResources;
            client = holdPublication
                    ? new HeldServicePublicationClient(fixture.cluster.getZooKeeperConnectString(), watcher)
                    : new ZooKeeper(fixture.cluster.getZooKeeperConnectString(), 10000, watcher);
            watcher.waitForConnection();
            String root = "/socket-service-readiness";
            String requiredRoot = "/socket-service-required-" + target + "-"
                    + Integer.toUnsignedString(config.directory().hashCode());
            for (String path : List.of(root, requiredRoot)) {
                if (client.exists(path, false) == null) {
                    client.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
            var identity = fixture.identities.get(target);
            requirements = new ZKProfileStartupRequirementsStore(client, requiredRoot,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            if (requiredCompatibility != null) {
                assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                        requirements.provision(requiredCompatibility, requested.requiredDevices()));
            }
            persistent = new ZKProfileRegistrationStore(client, root, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            publisher = new ZKProfileEphemeralRegistrationStore(client, root, identity.bookieId().toString(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var source = new ZKProfileReservationStore(client, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            assertEquals(org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND,
                    source.createInitialDeleteAssignment(new org.apache.bookkeeper.discover.profile
                            .ProfileDeleteRecord.Target(identity.bookieId().toString(), identity.incarnation().value()))
                            .status());
            var metadata = new ProfileLedgerMetadataStore(client, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            startup = new SegmentProfileService.Startup(requirements, requested, persistent, publisher,
                    source, metadata, watcher, "ledger-profile", scheduler, fixture.cold, 4,
                    Duration.ofSeconds(5), Duration.ofMillis(100));
            var control = fixture.controlConfigs.get(target);
            var read = new SegmentProfileReadConfig((caller, query) -> fixture.readAllowed.get()
                    && control.staticAuthorizer().allows(caller, ProfileControlOperation.INSTALL)
                    && query.context().equals(fixture.context()),
                    (caller, query) -> fixture.readAllowed.get()
                    && control.staticAuthorizer().allows(caller, ProfileControlOperation.INSTALL)
                    && query.context().equals(fixture.context()));
            transport = new SegmentProfileService.Transport(fixture.serverTls, budget,
                    131072, 64, "socket-test", requested.targetReadinessGeneration().value(),
                    address, fixture.acceptor, fixture.worker, control, read);
            Map<String, Object> settings = Map.ofEntries(
                    Map.entry("Mode", "ISOLATED"), Map.entry("Directory", config.directory().toString()),
                    Map.entry("LogicalShards", config.logicalShards()), Map.entry("DataFiles", config.dataFiles()),
                    Map.entry("BytesPerFile", config.bytesPerFile()),
                    Map.entry("MaxPayloadBytes", config.maxPayloadBytes()),
                    Map.entry("MaxPendingEntries", config.maxPendingEntries()), Map.entry("Workers", 1),
                    Map.entry("IoThreads", 2), Map.entry("ControlSlots", 6), Map.entry("Buffers", 2),
                    Map.entry("BufferBytes", 4096), Map.entry("InputBudgetBytes", 4096),
                    Map.entry("BufferBudgetBytes", 2 * (4096L + 4095) + 4096), Map.entry("StartupTimeoutMillis", 5000),
                    Map.entry("ControlLogBytes", 1024 * 1024), Map.entry("HotEntries", 8),
                    Map.entry("IndexedEntries", 16),
                    Map.entry("PoolBlocks", 1), Map.entry("MaintenanceBlocks", 1), Map.entry("BatchBytes", 4096),
                    Map.entry("BatchEntries", 1), Map.entry("BatchDelayMillis", 30000));
            var properties = new java.util.Properties();
            settings.forEach((name, value) -> properties.setProperty(
                    SegmentProfileService.CONFIG_PREFIX + name, value.toString()));
            var input = temporary.newFolder().toPath().resolve("bookie.properties");
            try {
                try (var output = Files.newOutputStream(input)) {
                    properties.store(output, "Disposable Profile bootstrap bounds; authority is supplied separately");
                }
                bootstrapConfiguration = new ServerConfiguration();
                bootstrapConfiguration.loadConf(input.toUri().toURL());
                System.out.println("UW_PROFILE_CONFIG_RECEIPT " + new com.fasterxml.jackson.databind.ObjectMapper()
                        .writeValueAsString(Map.of("settings", settings, "propertiesSha256",
                                java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                                        .digest(Files.readAllBytes(input))), "schema", 1)));
            } finally {
                Files.deleteIfExists(input);
            }
            if (ownsResources) {
                bootstrapConfiguration.setMetadataServiceUri("zk+longhierarchical://"
                        + fixture.cluster.getZooKeeperConnectString() + "/socket-ledgers");
                bootstrapConfiguration.setZkTimeout(5000);
                var owned = new SegmentProfileOwnedResources.Settings(requested, requiredRoot, root,
                        "/socket-initial",
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, "ledger-profile", 4, Duration.ofSeconds(5),
                        Duration.ofMillis(100), 2, 4, 64);
                service = SegmentProfileService.withOwnedResources(bootstrapConfiguration,
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, owned, fixture.serverTls, budget,
                        131072, 64, "socket-test", address, control.staticAuthorizer(),
                        control.exactAuthorizer(), read);
            } else {
                service = SegmentProfileService.fromConfiguration(bootstrapConfiguration,
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, startup, transport);
            }
        }

        IsolatedSegmentStorage storage() throws Exception {
            var field = SegmentProfileService.class.getDeclaredField("storage");
            field.setAccessible(true);
            return (IsolatedSegmentStorage) field.get(service);
        }

        void replaceNode(Fixture fixture, int target, boolean existing) throws Exception {
            replaceNode(fixture, target, existing, false);
        }

        org.apache.bookkeeper.server.EmbeddedServer assembleEmbedded() throws Exception {
            var classic = temporary.newFolder().toPath().resolve("classic-must-stay-absent");
            var configuration = new ServerConfiguration(bootstrapConfiguration)
                    .setMetadataServiceUri("do-not-open-classic://invalid")
                    .setJournalDirName(classic.resolve("journal").toString())
                    .setLedgerDirNames(new String[] {classic.resolve("ledger").toString()})
                    .setIndexDirName(new String[] {classic.resolve("index").toString()});
            var server = org.apache.bookkeeper.server.EmbeddedServer.builder(
                    new org.apache.bookkeeper.server.conf.BookieConfiguration(configuration))
                    .experimentalProfileService(service, false).build();
            assertFalse(Files.exists(classic));
            assertNull(server.getBookieService());
            assertNull(server.getRegistrationManager());
            assertNull(server.getLedgerManagerFactory());
            return server;
        }

        void replaceNode(Fixture fixture, int target, boolean existing, boolean useEmbedded) throws Exception {
            var previous = fixture.listeners.get(target);
            if (previous.isOpen()) {
                previous.close().sync();
            } else {
                previous.closeFuture().sync();
            }
            fixture.nodes.get(target).close();
            if (useEmbedded) {
                if (existing) {
                    var configuration = new ServerConfiguration(bootstrapConfiguration)
                            .setMetadataServiceUri("do-not-open-classic://invalid");
                    embedded = org.apache.bookkeeper.server.EmbeddedServer.builder(
                            new org.apache.bookkeeper.server.conf.BookieConfiguration(configuration))
                            .experimentalProfileService(service, true).build();
                } else {
                    embedded = assembleEmbedded();
                }
                embedded.getLifecycleComponentStack().start();
            } else {
                service.start(existing);
            }
            var field = SegmentProfileService.class.getDeclaredField("listener");
            field.setAccessible(true);
            var listenerField = SegmentProfileListener.class.getDeclaredField("listener");
            listenerField.setAccessible(true);
            fixture.listeners.set(target, (Channel) listenerField.get(field.get(service)));
            var bound = (InetSocketAddress) fixture.listeners.get(target).localAddress();
            fixture.nodes.set(target, storage());
            fixture.budgets.set(target, budget);
            if (ownsResources) {
                var peer = persistent.readPeer(fixture.identities.get(target).bookieId().toString()).peer();
                assertNotNull(peer);
                assertEquals("tls://127.0.0.1:" + bound.getPort(), peer.projection().endpoint());
            } else {
                assertEquals("tls://127.0.0.1:" + bound.getPort(), publisher.readServiceInfo(
                        fixture.identities.get(target).bookieId().toString()).orElseThrow().endpoint());
                assertTrue(publisher.readWritable(fixture.identities.get(target).bookieId().toString()).isPresent());
            }
        }

        Object ownedResources() throws Exception {
            var field = SegmentProfileService.class.getDeclaredField("ownedResources");
            field.setAccessible(true);
            return field.get(service);
        }

        Object ownedField(String name) throws Exception {
            var field = SegmentProfileOwnedResources.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(ownedResources());
        }

        void assertOwnedResourcesTerminal(boolean terminal) throws Exception {
            assertEquals(terminal, ((EventLoopGroup) ownedField("worker")).isTerminated());
            assertEquals(terminal, ((EventLoopGroup) ownedField("acceptor")).isTerminated());
            assertEquals(terminal, ((ThreadPoolExecutor) ownedField("cold")).isTerminated());
            assertEquals(terminal, ((java.util.concurrent.ScheduledThreadPoolExecutor) ownedField("scheduler"))
                    .isTerminated());
            assertEquals(terminal, ownedField("zkClosed"));
            assertEquals(terminal, ownedField("managerClosed"));
            assertEquals(terminal, ownedField("driverClosed"));
        }

        @Override
        public void close() throws Exception {
            if (embedded != null) {
                embedded.getLifecycleComponentStack().close();
            }
            service.close();
            client.close();
        }
    }

    private static StartupReadinessInput servicePolicy(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long generation) throws Exception {
        var policy = startupPolicy(config, identity);
        var device = policy.requiredDevices().devices().get(0);
        var devices = new RequiredDeviceManifest(policy.requiredDevices().identity(),
                policy.requiredDevices().generation(),
                List.of(new RequiredDeviceManifest.RequiredDeviceFact(device.deviceId(), device.storageIncarnation(),
                        device.manifestGeneration(), RequiredDeviceManifest.DeviceStatus.READY,
                        device.arenaFormatVersion(), device.controlFormatVersion(), device.checkpointFormatVersion(),
                        device.superblockGeneration(), device.mandatoryFeatures())));
        var f = policy.newScopeFallback();
        var arena = config.directory().getParent().toRealPath().resolve(config.directory().getFileName());
        var roots = f.newRoots().stream().map(root -> new NewScopeFallbackManifest.StorageRoot(root.kind(),
                switch (root.kind()) {
                    case ARENA -> arena.toString();
                    case INDEX -> arena.resolve("derived-index").toString();
                    default -> root.opaquePath();
                })).sorted(java.util.Comparator.comparing(NewScopeFallbackManifest.StorageRoot::opaquePath)).toList();
        var fallback = new NewScopeFallbackManifest(f.oldBookieId(), f.newBookieId(), f.oldStorageIncarnation(),
                f.newStorageIncarnation(), f.oldRoots(), roots, f.oldCredentialScope(), f.newCredentialScope(),
                f.oldScopeDisposition(), f.oldCredentialAccessToNewScope(), f.newCredentialAccessToNewScope());
        return new StartupReadinessInput(policy.compatibility(), devices, policy.recoveryFacts(), fallback,
                policy.rollbackSafetyFacts(), new Generation(generation), policy.capabilityGeneration(),
                policy.runtimeReaderVersion(), policy.runtimeWriterVersion(), policy.supportedMandatoryFeatures(),
                policy.profileEndpoint(), policy.capabilityHints());
    }

    @Test
    public void ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            for (int failure = 0; failure < 6; failure++) {
                var identity = fixture.identities.get(failure % 3);
                var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("unopened"),
                        1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
                var requested = servicePolicy(config, identity, 1);
                var c = requested.compatibility();
                var required = failure == 0 ? null : new BookieCompatibilityFact(c.bookieId(), c.storageIncarnation(),
                        c.activeEngine(), c.profileWireMajor(), c.profileWireMinor(), c.descriptorHashSuite(),
                        c.mandatoryLocalFeatures(), c.deviceManifestIdentity(), c.deviceManifestGeneration(),
                        c.localFormatGeneration(), c.effectiveDeleteAssignmentGeneration(), failure == 2 ? 2 : 1,
                        c.minimumWriterVersion(), failure == 1 ? BookieCompatibilityFact.MigrationState.PREPARED
                                : BookieCompatibilityFact.MigrationState.FORMAT_READY, c.migrationGeneration());
                if (failure == 3 || failure == 5) {
                    var f = requested.newScopeFallback();
                    var roots = failure == 5 ? f.newRoots().stream().map(root -> root.kind()
                            == NewScopeFallbackManifest.RootKind.ARENA
                            ? new NewScopeFallbackManifest.StorageRoot(root.kind(), root.opaquePath() + "-other")
                            : root)
                            .sorted(java.util.Comparator.comparing(NewScopeFallbackManifest.StorageRoot::opaquePath))
                            .toList() : f.newRoots();
                    var unsafe = new NewScopeFallbackManifest(f.oldBookieId(), f.newBookieId(),
                            f.oldStorageIncarnation(), f.newStorageIncarnation(), f.oldRoots(), roots,
                            f.oldCredentialScope(), f.newCredentialScope(), failure == 3
                                    ? NewScopeFallbackManifest.OldScopeDisposition.UNKNOWN : f.oldScopeDisposition(),
                            f.oldCredentialAccessToNewScope(), f.newCredentialAccessToNewScope());
                    requested = new StartupReadinessInput(requested.compatibility(), requested.requiredDevices(),
                            requested.recoveryFacts(), unsafe, requested.rollbackSafetyFacts(),
                            requested.targetReadinessGeneration(), requested.capabilityGeneration(),
                            requested.runtimeReaderVersion(), requested.runtimeWriterVersion(),
                            requested.supportedMandatoryFeatures(), requested.profileEndpoint(),
                            requested.capabilityHints());
                }
                if (failure == 4) {
                    // Existing native endpoint: storage may open, but no readiness receipt may be published.
                    try (var episode = new ServiceEpisode(fixture, failure % 3, config, requested, required,
                            (InetSocketAddress) fixture.listeners.get(0).localAddress(),
                            sdk.getClientCtx().getScheduler())) {
                        assertThrows(java.io.IOException.class, () -> episode.service.start(false));
                        assertTrue(episode.service.close(Duration.ofSeconds(5)));
                        assertTrue(episode.persistent.read(identity.bookieId().toString()).versionedRecord().isEmpty());
                        try (var reopened = new IsolatedSegmentStorage(config, identity,
                                new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                                new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                        2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                                new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                            reopened.restart();
                        }
                    }
                } else {
                    try (var episode = new ServiceEpisode(fixture, failure % 3, config, requested, required,
                            new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler())) {
                        assertThrows(java.io.IOException.class, () -> episode.service.start(false));
                        assertFalse(Files.exists(config.directory()));
                        assertNull(episode.storage());
                        assertTrue(episode.persistent.read(identity.bookieId().toString()).versionedRecord().isEmpty());
                    }
                }
            }
            assertFalse(fixture.worker.isShuttingDown());
            assertFalse(fixture.cold.isShutdown());
        }
    }

    @Test
    public void profileConfigurationRejectsInvalidBoundsBeforeOpenAndFreezesValidSettings() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("unopened"),
                    1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            var requested = servicePolicy(config, fixture.identities.get(0), 1);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, requested.compatibility(),
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler())) {
                for (var bad : Map.<String, Object>ofEntries(Map.entry("Buffers", 0), Map.entry("Workers", 2),
                        Map.entry("BufferBytes", 4096.5), Map.entry("InputBudgetBytes", 1),
                        Map.entry("BufferBudgetBytes", 4096), Map.entry("HotEntries", 0),
                        Map.entry("PoolBlocks", 1024), Map.entry("BatchBytes", 8192),
                        Map.entry("Backend", "IO_URING"), Map.entry("WaitStrategy", "BOUNDED_SPIN"),
                        Map.entry("Directory", "relative"), Map.entry("AdvancedRangeEnabled", true)).entrySet()) {
                    var invalid = new ServerConfiguration(episode.bootstrapConfiguration);
                    invalid.setProperty(SegmentProfileService.CONFIG_PREFIX + bad.getKey(), bad.getValue());
                    assertThrows(RuntimeException.class, () -> SegmentProfileService.fromConfiguration(invalid,
                            new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, episode.startup, episode.transport));
                    assertFalse(Files.exists(config.directory()));
                }
                var missing = new ServerConfiguration(episode.bootstrapConfiguration);
                missing.clearProperty(SegmentProfileService.CONFIG_PREFIX + "IndexedEntries");
                assertThrows(IllegalArgumentException.class, () -> SegmentProfileService.fromConfiguration(missing,
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, episode.startup, episode.transport));
                assertNull(episode.storage());
                assertTrue(episode.persistent.read("socket0:3181").versionedRecord().isEmpty());
                // Mutating the original configuration cannot change the already frozen owner.
                episode.bootstrapConfiguration.setProperty(SegmentProfileService.CONFIG_PREFIX + "BufferBytes", 1);
                episode.bootstrapConfiguration.setProperty(SegmentProfileService.CONFIG_PREFIX + "Affinity", "REQUIRE");
                episode.replaceNode(fixture, 0, false, true);
                assertEquals("OFF", files(episode.storage()).config().affinity());
                assertEquals("SYSTEM", files(episode.storage()).config().numaPolicy());
                assertEquals("PARK", files(episode.storage()).config().waitStrategy());
            }
        }
    }

    @Test
    public void ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart() throws Exception {
        runOwnedProfileService(false);
    }

    @Test
    public void embeddedProfileServiceRunsSdkWithoutClassicOpenAndRetainsStackUntilActualDrain() throws Exception {
        runOwnedProfileService(true);
    }

    @Test
    public void ownedProfileResourcesRunSdkAndWaitForActualExecutorsMetadataAndNetworkDrain() throws Exception {
        runOwnedProfileService(true, true);
    }

    @Test
    public void ownedProfileResourcesKeepZkAndNetworkUntilActualMetadataSchedulerTerminal() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("owned-meta"),
                    1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            var requested = servicePolicy(config, fixture.identities.get(0), 1);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, requested.compatibility(),
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler(), false, true)) {
                episode.replaceNode(fixture, 0, false, true);
                var manager = (AbstractZkLedgerManager) episode.ownedField("manager");
                var field = AbstractZkLedgerManager.class.getDeclaredField("scheduler");
                field.setAccessible(true);
                var scheduler = (java.util.concurrent.ScheduledExecutorService) field.get(manager);
                var entered = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var actual = scheduler.submit(() -> {
                    entered.countDown();
                    await(release);
                });
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    assertFalse(episode.service.close(Duration.ofMillis(200)));
                    assertFalse(actual.isDone());
                    assertEquals(false, episode.ownedField("managerClosed"));
                    assertEquals(false, episode.ownedField("zkClosed"));
                    assertFalse(((EventLoopGroup) episode.ownedField("worker")).isShuttingDown());
                    release.countDown();
                    actual.get(5, TimeUnit.SECONDS);
                    assertTrue(episode.service.close(Duration.ofSeconds(5)));
                    episode.assertOwnedResourcesTerminal(true);
                } finally {
                    release.countDown();
                }
            }
        }
    }

    @Test
    public void ownedProfileInitializationFailureRetainsHandleAndDrainsActualPartialResources() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("owned-unopened"),
                    1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            var requested = servicePolicy(config, fixture.identities.get(0), 1);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, null,
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler(), false, true)) {
                assertThrows(java.io.IOException.class, () -> episode.service.start(false));
                assertFalse(Files.exists(config.directory()));
                assertNull(episode.storage());
                assertTrue(episode.service.close(Duration.ofSeconds(5)));
                episode.assertOwnedResourcesTerminal(true);
                var settings = new SegmentProfileOwnedResources.Settings(requested, "/missing-startup",
                        "/missing-readiness", "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE, "ledger-profile",
                        4, Duration.ofSeconds(5), Duration.ofMillis(100), 1, 1, 4);
                try (var noZk = new java.net.ServerSocket(0)) {
                    var invalid = new ServerConfiguration(episode.bootstrapConfiguration);
                    invalid.setMetadataServiceUri("zk+longhierarchical://127.0.0.1:" + noZk.getLocalPort()
                            + "/socket-ledgers");
                    invalid.setZkTimeout(1000);
                    var control = fixture.controlConfigs.get(0);
                    var failed = SegmentProfileService.withOwnedResources(invalid,
                            new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR, settings, fixture.serverTls,
                            new ProfileReceiveBudget(524288, 131072, 262144, 8, 8, 8), 131072, 64, "socket-test",
                            new InetSocketAddress("127.0.0.1", 0), control.staticAuthorizer(),
                            control.exactAuthorizer(), null);
                    try {
                        assertThrows(java.io.IOException.class, () -> failed.start(false));
                        assertTrue(failed.close(Duration.ofSeconds(5)));
                        var resourcesField = SegmentProfileService.class.getDeclaredField("ownedResources");
                        resourcesField.setAccessible(true);
                        var resources = resourcesField.get(failed);
                        for (String name : List.of("worker", "acceptor")) {
                            var terminal = SegmentProfileOwnedResources.class.getDeclaredField(name);
                            terminal.setAccessible(true);
                            assertTrue(((EventLoopGroup) terminal.get(resources)).isTerminated());
                        }
                        assertFalse(Files.exists(config.directory()));
                    } finally {
                        failed.close();
                    }
                }
            }
        }
    }

    private void runOwnedProfileService(boolean embedded) throws Exception {
        runOwnedProfileService(embedded, false);
    }

    private void runOwnedProfileService(boolean embedded, boolean ownsResources) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture)) {
            var episodes = new ArrayList<ServiceEpisode>();
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var release = new CountDownLatch(1);
            ProfileLedgerHandle handle = null;
            try {
                for (int target = 0; target < 3; target++) {
                    var original = files(fixture.nodes.get(target)).config();
                    var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("new-scope"),
                            1, 1, 1, original.bytesPerFile(), 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
                    var requested = servicePolicy(config, fixture.identities.get(target), 1);
                    var episode = new ServiceEpisode(fixture, target, config, requested, requested.compatibility(),
                            new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler(), false,
                            ownsResources);
                    episodes.add(episode);
                    episode.replaceNode(fixture, target, false, embedded);
                    var peer = episode.persistent.readPeer(fixture.identities.get(target).bookieId().toString()).peer();
                    var connected = ProfileInitialControlClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                            episode.persistent, peer, fixture.cold, 4, Duration.ofSeconds(5));
                    fixture.clients.put(peer.projection().bookieId(), connected.completion().get(5, TimeUnit.SECONDS));
                    var ordinary = ProfileNormalDataClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                            episode.persistent, peer, fixture.cold, 8, 1060, Duration.ofSeconds(5));
                    var normal = ordinary.completion().get(5, TimeUnit.SECONDS);
                    fixture.dataClients.add(normal);
                    data.put(fixture.identities.get(target).bookieId(), normal);
                }
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data,
                        8, 1024, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                assertEquals(0, handle.addEntry(bytes(16, 8)));
                var normal = data.get(fixture.identities.get(0).bookieId());
                // A=2 does not establish completion at this particular target. Confirm the exact retry there.
                var originalEntry = entry(fixture, 0, 16);
                try {
                    var confirmed = normal.addNormal(fixture.context(), fixture.credential, 0, originalEntry)
                            .get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileStatus.StatusClass.OK, confirmed.statusClass());
                    assertTrue(confirmed.durableResult() == ProfileStatus.DurableResult.APPLIED
                            || confirmed.durableResult() == ProfileStatus.DurableResult.ALREADY_APPLIED);
                } finally {
                    originalEntry.release();
                }
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                        normal.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
                var forcing = new CountDownLatch(1);
                files(episodes.get(0).storage()).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    forcing.countDown();
                    await(release);
                });
                var second = new CompletableFuture<Long>();
                handle.asyncAddEntry(bytes(16, 9), (code, ledger, entry, context) -> {
                    if (code == 0) {
                        second.complete(entry);
                    } else {
                        second.completeExceptionally(BKException.create(code));
                    }
                }, null);
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertFalse(episodes.get(0).service.close(Duration.ofMillis(100)));
                if (ownsResources) {
                    episodes.get(0).assertOwnedResourcesTerminal(false);
                }
                if (embedded) {
                    var stack = episodes.get(0).embedded.getLifecycleComponentStack();
                    assertThrows(java.io.UncheckedIOException.class, stack::close);
                    assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.STARTED,
                            stack.getComponent(stack.getNumComponents() - 1).lifecycleState());
                    assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.STARTED,
                            stack.getComponent(0).lifecycleState());
                }
                assertThrows(java.io.IOException.class,
                        () -> SegmentDataFileLock.open(episodes.get(0).config.directory()));
                if (ownsResources) {
                    assertNull(episodes.get(0).persistent.readPeer("socket0:3181").peer());
                } else {
                    assertThrows(IllegalStateException.class,
                            () -> episodes.get(0).publisher.readWritable("socket0:3181"));
                }
                assertFalse(fixture.worker.isShuttingDown());
                release.countDown();
                assertEquals(1, second.get(5, TimeUnit.SECONDS).longValue());
                assertTrue(episodes.get(0).service.close(Duration.ofSeconds(5)));
                if (ownsResources) {
                    episodes.get(0).assertOwnedResourcesTerminal(true);
                }
                if (embedded) {
                    var stack = episodes.get(0).embedded.getLifecycleComponentStack();
                    stack.close();
                    assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.CLOSED,
                            stack.getComponent(stack.getNumComponents() - 1).lifecycleState());
                }
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                handle = null;
                var requested = servicePolicy(episodes.get(0).config, fixture.identities.get(0), 2);
                var restarted = new ServiceEpisode(fixture, 0, episodes.get(0).config, requested, null,
                        new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler(), false, ownsResources);
                episodes.add(restarted);
                restarted.replaceNode(fixture, 0, true, embedded);
                var peer = restarted.persistent.readPeer("socket0:3181").peer();
                var connected = ProfileNormalDataClient.connectRegistered(fixture.clientGroup, fixture.peerTls,
                        restarted.persistent, peer, fixture.cold, 4, 1060, Duration.ofSeconds(5));
                try {
                    var recovered = connected.completion().get(5, TimeUnit.SECONDS);
                    assertEquals(2, recovered.hello().readinessGeneration());
                    assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.FOUND,
                            recovered.readEntry(fixture.context(), 0).get(5, TimeUnit.SECONDS).outcome());
                } finally {
                    connected.closeAsync().get(5, TimeUnit.SECONDS);
                }
            } finally {
                release.countDown();
                if (handle != null) {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
                for (var episode : episodes) {
                    episode.close();
                }
            }
        }
    }

    @Test
    public void embeddedProfileStartupFailureCannotBecomeStartedEvenWhenExceptionHandlerReturns() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("unopened"),
                    1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            var requested = servicePolicy(config, fixture.identities.get(0), 1);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, null,
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler())) {
                var conflict = new ServerConfiguration().setAutoRecoveryDaemonEnabled(true);
                assertThrows(IllegalArgumentException.class, () -> org.apache.bookkeeper.server.EmbeddedServer.builder(
                        new org.apache.bookkeeper.server.conf.BookieConfiguration(conflict))
                        .experimentalProfileService(episode.service, false).build());
                var stack = episode.assembleEmbedded().getLifecycleComponentStack();
                var notified = new AtomicInteger();
                stack.setExceptionHandler((thread, failure) -> notified.incrementAndGet());
                assertThrows(IllegalStateException.class, stack::start);
                assertEquals(1, notified.get());
                assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.INITIALIZED,
                        stack.getComponent(stack.getNumComponents() - 1).lifecycleState());
                assertFalse(Files.exists(config.directory()));
                assertNull(episode.storage());
                stack.close();
                assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.CLOSED,
                        stack.getComponent(stack.getNumComponents() - 1).lifecycleState());
            }
        }
    }

    @Test
    public void ownedProfileServiceStopDuringPublicationKeepsActualOwnerUntilLateRpcTerminates() throws Exception {
        runProfilePublicationStop(false);
    }

    @Test
    public void embeddedProfileStopDuringPublicationKeepsOriginalEpisodeAndNeverStartsLate() throws Exception {
        runProfilePublicationStop(true);
    }

    private void runProfilePublicationStop(boolean embedded) throws Exception {
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture)) {
            var config = new IsolatedSegmentConfig(temporary.newFolder().toPath().resolve("new-scope"),
                    1, 1, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            var requested = servicePolicy(config, fixture.identities.get(0), 1);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, requested.compatibility(),
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler(), true)) {
                var client = (HeldServicePublicationClient) episode.client;
                client.armed = true;
                if (embedded) {
                    episode.embedded = episode.assembleEmbedded();
                }
                var starting = java.util.concurrent.Executors.newSingleThreadExecutor();
                try {
                    var actual = starting.submit(() -> {
                        if (embedded) {
                            episode.embedded.getLifecycleComponentStack().start();
                        } else {
                            episode.service.start(false);
                        }
                        return null;
                    });
                    assertTrue(client.submitted.await(5, TimeUnit.SECONDS));
                    assertFalse(episode.service.close(Duration.ofMillis(100)));
                    if (embedded) {
                        var stack = episode.embedded.getLifecycleComponentStack();
                        assertThrows(java.io.UncheckedIOException.class, stack::stop);
                        assertEquals(org.apache.bookkeeper.common.component.Lifecycle.State.INITIALIZED,
                                stack.getComponent(stack.getNumComponents() - 1).lifecycleState());
                    }
                    assertFalse(actual.isDone());
                    assertNotNull(episode.storage());
                    assertThrows(java.io.IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                    assertThrows(IllegalStateException.class,
                            () -> episode.publisher.readWritable("socket0:3181"));
                    client.release.countDown();
                    assertThrows(ExecutionException.class, () -> actual.get(10, TimeUnit.SECONDS));
                    assertTrue(episode.service.close(Duration.ofSeconds(5)));
                    assertNull(client.exists("/socket-service-readiness/"
                            + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                                    "socket0:3181".getBytes(StandardCharsets.UTF_8)) + "/writable", false));
                    try (var lock = SegmentDataFileLock.open(config.directory())) {
                        assertNotNull(lock);
                    }
                    assertFalse(fixture.worker.isShuttingDown());
                    assertFalse(fixture.cold.isShutdown());
                } finally {
                    client.release.countDown();
                    starting.shutdown();
                    assertTrue(starting.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    public void ownedProfileServiceConsumesActualOldClassicDrainAndUnixAccessBeforeRestart() throws Exception {
        SegmentNewScopeAccessTest.requireCredentials();
        try (Fixture fixture = new Fixture(false, false, true); BookKeeper sdk = sdk(fixture);
                var old = new SegmentClassicScopeEpisodeTest.Episode(
                        fixture.cluster.getZooKeeperConnectString(), "/old-classic-service")) {
            var config = files(fixture.nodes.get(0)).config();
            fixture.listeners.get(0).close().sync();
            fixture.nodes.get(0).close();
            old.writeAndReadConfirmedEntry();
            old.requestCloseAndHoldProcess();
            assertThrows(IllegalStateException.class, old::drained);
            old.exit();
            old.drained();
            var requested = SegmentNewScopeAccessTest.actualFallback(
                    servicePolicy(config, fixture.identities.get(0), 1), config, old);
            try (var episode = new ServiceEpisode(fixture, 0, config, requested, requested.compatibility(),
                    new InetSocketAddress("127.0.0.1", 0), sdk.getClientCtx().getScheduler())) {
                episode.replaceNode(fixture, 0, true);
                assertTrue(episode.service.close(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void readinessBridgeUsesActualRecoveredFactsAndOriginalPublicationSequence() throws Exception {
        runReadinessBridge(false, false);
    }

    @Test
    public void physicalCredentialAccessFeedsOriginalReadinessBridge() throws Exception {
        SegmentNewScopeAccessTest.requireCredentials();
        runReadinessBridge(true, false);
    }

    @Test
    public void actualOldClassicDrainAndCredentialAccessFeedOriginalReadinessBridge() throws Exception {
        SegmentNewScopeAccessTest.requireCredentials();
        runReadinessBridge(true, true);
    }

    private void runReadinessBridge(boolean physicalAccess, boolean actualOldService) throws Exception {
        for (boolean rejectedScheduler : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture);
                    var oldEpisode = actualOldService ? new SegmentClassicScopeEpisodeTest.Episode(
                            fixture.cluster.getZooKeeperConnectString(), "/old-classic-rejoin") : null) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk,
                        (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(44, bytes(16, 51),
                        sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential,
                        data, fixture.clients,
                        4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                var deleted = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(fixture.reservations,
                        metadata, fixture.cold).deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                                Duration.ofSeconds(30))
                        .get(30, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, deleted.status());
                var identity = fixture.identities.get(0);
                var config = files(fixture.nodes.get(0)).config();
                fixture.nodes.get(0).close();
                if (oldEpisode != null) {
                    oldEpisode.writeAndReadConfirmedEntry();
                    oldEpisode.requestCloseAndHoldProcess();
                    assertThrows(IllegalStateException.class, oldEpisode::drained);
                    oldEpisode.exit();
                    oldEpisode.drained();
                }
                var watcher = new ZooKeeperWatcherBase(10000, false);
                try (var client = new ZooKeeper(fixture.cluster.getZooKeeperConnectString(), 10000, watcher)) {
                    watcher.waitForConnection();
                    String root = "/socket-readiness-bridge";
                    client.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    var source = new ZKProfileReservationStore(client, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    var sourceMetadata = new ProfileLedgerMetadataStore(client,
                            (AbstractZkLedgerManager) fixture.manager,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var persistent = new ZKProfileRegistrationStore(client, root,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var publisher = new ZKProfileEphemeralRegistrationStore(client,
                            root, identity.bookieId().toString(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    client.create(root + "-startup", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    var requirements = new ZKProfileStartupRequirementsStore(client, root + "-startup",
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                    var requestedPolicy = startupPolicy(config, identity);
                    var requested = physicalAccess
                            ? SegmentNewScopeAccessTest.actualFallback(requestedPolicy, config, oldEpisode)
                            : requestedPolicy;
                    var expectedDevice = requested.requiredDevices().devices().get(0);
                    var required = new RequiredDeviceManifest(requested.requiredDevices().identity(),
                            requested.requiredDevices().generation(),
                            List.of(new RequiredDeviceManifest.RequiredDeviceFact(
                                    expectedDevice.deviceId(), expectedDevice.storageIncarnation(),
                                    expectedDevice.manifestGeneration(), RequiredDeviceManifest.DeviceStatus.READY,
                                    expectedDevice.arenaFormatVersion(), expectedDevice.controlFormatVersion(),
                                    expectedDevice.checkpointFormatVersion(), expectedDevice.superblockGeneration(),
                                    expectedDevice.mandatoryFeatures())));
                    assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                            requirements.provision(requested.compatibility(), required));
                    var startupSource = requirements.read(identity.bookieId().toString()).observation().orElseThrow();
                    // The source is read and checked before constructing/opening the replay graph.
                    var policy = IsolatedSegmentStorage.startupPolicy(startupSource, requested, config, identity);
                    for (boolean prepared : List.of(true, false)) {
                        String rejectedRoot = root + "-unsupported-" + prepared;
                        client.create(rejectedRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        var rejected = new ZKProfileStartupRequirementsStore(client, rejectedRoot,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                        var c = requested.compatibility();
                        var unsupported = new BookieCompatibilityFact(c.bookieId(), c.storageIncarnation(),
                                c.activeEngine(), c.profileWireMajor(), c.profileWireMinor(), c.descriptorHashSuite(),
                                c.mandatoryLocalFeatures(), c.deviceManifestIdentity(), c.deviceManifestGeneration(),
                                c.localFormatGeneration(), c.effectiveDeleteAssignmentGeneration(), prepared ? 1 : 2,
                                c.minimumWriterVersion(), prepared ? BookieCompatibilityFact.MigrationState.PREPARED
                                        : BookieCompatibilityFact.MigrationState.FORMAT_READY, c.migrationGeneration());
                        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                                rejected.provision(unsupported, required));
                        var observed = rejected.read(identity.bookieId().toString()).observation().orElseThrow();
                        assertThrows(java.io.IOException.class,
                                () -> IsolatedSegmentStorage.startupPolicy(observed, requested, config, identity));
                        var claimedFutureRuntime = new StartupReadinessInput(
                                requested.compatibility(), requested.requiredDevices(), requested.recoveryFacts(),
                                requested.newScopeFallback(), requested.rollbackSafetyFacts(),
                                requested.targetReadinessGeneration(), requested.capabilityGeneration(),
                                99, 99, List.of(1), requested.profileEndpoint(), requested.capabilityHints());
                        assertThrows(java.io.IOException.class, () -> IsolatedSegmentStorage.startupPolicy(
                                observed, claimedFutureRuntime, config, identity));
                    }
                    publisher.bindStartupRequirements(requirements, startupSource);
                    try (var replay = new IsolatedSegmentStorage(config, identity,
                            new SecretKeySpec(bytes(32, 7), "AES"),
                            VALIDATOR, new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                    2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                            new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                        publisher.bindDeleteDelivery(source, identity.incarnation(), replay::profileDeleteProgress);
                        try (var admission = publisher.watchAdmission(watcher)) {
                            replay.bindProfileRegistration(admission);
                            replay.restart();
                            var wrongManifest = new RequiredDeviceManifest(
                                    new ManifestIdentity(bytes(32, 19)),
                                    policy.requiredDevices().generation(), policy.requiredDevices().devices());
                            var wrongPolicy = new StartupReadinessInput(
                                    policy.compatibility(), wrongManifest, policy.recoveryFacts(),
                                    policy.newScopeFallback(),
                                    policy.rollbackSafetyFacts(), policy.targetReadinessGeneration(),
                                    policy.capabilityGeneration(),
                                    policy.runtimeReaderVersion(),
                                    policy.runtimeWriterVersion(),
                                    policy.supportedMandatoryFeatures(), policy.profileEndpoint(),
                                    policy.capabilityHints());
                            assertThrows(java.io.IOException.class, () -> replay.recoveredStartupFacts(
                                    wrongPolicy, source,
                                    sourceMetadata, "ledger-profile", 4, Duration.ofSeconds(5)));
                            assertNull(bookie(replay).deleteAppliedCursor());
                            var facts = CompletableFuture.supplyAsync(() -> {
                                try {
                                    return replay.recoveredStartupFacts(policy, source, sourceMetadata,
                                            "ledger-profile", 4,
                                            Duration.ofSeconds(5));
                                } catch (java.io.IOException failure) {
                                    throw new java.util.concurrent.CompletionException(failure);
                                }
                            }, fixture.cold).get(10, TimeUnit.SECONDS);
                            assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP, facts.deleteStatus());
                            assertEquals(LocalRecoveryFacts.RecoveryStatus.RECOVERED,
                                    facts.input().recoveryFacts().allocatorRecovery());
                            assertEquals(IsolatedSegmentStorage.STARTUP_READER_VERSION,
                                    facts.input().runtimeReaderVersion());
                            assertEquals(IsolatedSegmentStorage.STARTUP_WRITER_VERSION,
                                    facts.input().runtimeWriterVersion());
                            assertEquals(RequiredDeviceManifest.DeviceStatus.READY,
                                    facts.input().requiredDevices().devices().get(0).status());
                            assertTrue(facts.input().rollbackSafetyFacts().tombstone());
                            assertTrue(facts.input().rollbackSafetyFacts().arenaAuthority());
                            assertFalse(admission.getAsBoolean());
                            try (var local = new SegmentLocalReadinessStore(config.directory(),
                                    identity.bookieId().toString(), identity.incarnation())) {
                                assertThrows(IllegalArgumentException.class, () -> replay.reconcileReadiness(
                                        requested, local, persistent, publisher, source, sourceMetadata,
                                        "ledger-profile",
                                        sdk.getClientCtx().getScheduler(), fixture.cold, 4, Duration.ofSeconds(5),
                                        Duration.ofMillis(100), () -> true));
                                assertTrue(local.read(identity.bookieId().toString()).isEmpty());
                                assertThrows(IllegalArgumentException.class, () -> replay.reconcileReadiness(
                                        policy, local,
                                        new ZKProfileRegistrationStore(fixture.zk,
                                                root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5)),
                                        publisher, source, sourceMetadata, "ledger-profile",
                                        sdk.getClientCtx().getScheduler(),
                                        fixture.cold, 4, Duration.ofSeconds(5), Duration.ofMillis(100), () -> true));
                                assertTrue(local.read(identity.bookieId().toString()).isEmpty());
                                var scheduler = new java.util.concurrent.ScheduledThreadPoolExecutor(1);
                                scheduler.shutdown();
                                var startup = CompletableFuture.supplyAsync(() -> {
                                    try {
                                        return replay.reconcileReadiness(policy, local, persistent, publisher, source,
                                                sourceMetadata, "ledger-profile", rejectedScheduler ? scheduler
                                                        : sdk.getClientCtx().getScheduler(), fixture.cold, 4,
                                                Duration.ofSeconds(5), Duration.ofMillis(100), () -> true);
                                    } catch (java.io.IOException failure) {
                                        throw new java.util.concurrent.CompletionException(failure);
                                    }
                                }, fixture.cold);
                                if (rejectedScheduler) {
                                    var failure = assertThrows(ExecutionException.class,
                                            () -> startup.get(10, TimeUnit.SECONDS));
                                    assertTrue(failure.getCause() instanceof IllegalStateException);
                                    assertFalse(admission.getAsBoolean());
                                    assertThrows(IllegalStateException.class,
                                            () -> publisher.readWritable(identity.bookieId().toString()));
                                    assertTrue(replay.close(Duration.ofSeconds(5)));
                                    continue;
                                }
                                var attempt = startup.get(10, TimeUnit.SECONDS);
                                attempt.completion().get(5, TimeUnit.SECONDS);
                                assertEquals(StartupReadinessResult.Outcome.WRITABLE,
                                        attempt.readiness().outcome());
                                assertTrue(admission.getAsBoolean());
                                assertTrue(local.read(identity.bookieId().toString()).orElseThrow().durable());
                                assertEquals(policy.compatibility().deviceManifestIdentity(), persistent.read(
                                        identity.bookieId().toString()).versionedRecord().orElseThrow().record()
                                                .deviceManifestIdentity());
                                assertTrue(publisher.readWritable(identity.bookieId().toString()).isPresent());
                                assertFalse(replay.profileReadVisible(fixture.context(), 0));
                            }
                            assertTrue(replay.close(Duration.ofSeconds(5)));
                            assertFalse(admission.getAsBoolean());
                            publisher.demote(identity.bookieId().toString());
                            var unavailable = replay.recoveredStartupFacts(policy, source, sourceMetadata,
                                    "ledger-profile", 4, Duration.ofSeconds(5));
                            assertNull(unavailable.input());
                            assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.QUARANTINED,
                                    unavailable.deleteStatus());
                        }
                    }
                }
            }
        }
    }

    private static final class PendingLogicalDeleteClient extends ZooKeeper {
        final CountDownLatch prepared = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private boolean held;

        PendingLogicalDeleteClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var copied = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(copied::add);
            boolean logical = copied.stream().anyMatch(op -> op.getType() == ZooDefs.OpCode.setData
                    && op.getPath().startsWith("/socket-ledgers/")
                    && new String(((org.apache.zookeeper.proto.SetDataRequest) op.toRequestRecord()).getData(),
                            java.nio.charset.StandardCharsets.ISO_8859_1)
                        .contains(org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.TOMBSTONE_KEY));
            if (logical && !held) {
                held = true;
                prepared.countDown();
                await(release);
            }
            super.multi(copied, callback, context);
        }
    }

    @Test
    public void automaticDeletePublicationAndOnlineFollowerKeepPreparedInactiveAndDrainUnknown() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(44, bytes(16, 51),
                    sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential, data, fixture.clients,
                    4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            int node = 0;
            var identity = fixture.identities.get(node);
            var target = new org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.Target(
                    identity.bookieId().toString(), identity.incarnation().value());
            var config = files(fixture.nodes.get(node)).config();
            fixture.nodes.get(node).close();
            var watcher = new ZooKeeperWatcherBase(10000, false);
            try (var client = new PendingLogicalDeleteClient(fixture.cluster.getZooKeeperConnectString(), watcher);
                    var replay = new IsolatedSegmentStorage(config, identity, new SecretKeySpec(bytes(32, 7), "AES"),
                            VALIDATOR, new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                    2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                            new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                var source = new ZKProfileReservationStore(client, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                var sourceMetadata = new ProfileLedgerMetadataStore(client, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                assertEquals(org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus.FOUND,
                        source.createInitialDeleteAssignment(target).status());
                String root = "/socket-online-delete";
                client.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                var persistent = new org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore(
                        client, root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var publisher = new org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore(
                        client, root, target.bookie(), ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                publisher.bindDeleteDelivery(source, identity.incarnation(), replay::profileDeleteProgress);
                try (var admission = publisher.watchAdmission(watcher)) {
                    replay.bindProfileRegistration(admission);
                    replay.restart();
                    var gen = new org.apache.bookkeeper.common.profile.startup.Generation(1);
                    var record = new org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord(
                            target.bookie(),
                            identity.incarnation(), EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, gen, gen,
                            new org.apache.bookkeeper.common.profile.startup.ManifestIdentity(bytes(32, 7)),
                            gen, gen, gen,
                            1, 1, List.of(1),
                            org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord.State.READY);
                    assertEquals(org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore
                            .CasStatus.APPLIED,
                            persistent.compareAndSet(target.bookie(), -1, 0, record).status());
                    var ready = persistent.read(target.bookie()).versionedRecord().orElseThrow();
                    var projection = new org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection(
                            target.bookie(), identity.incarnation(), gen, ready.storeVersion(), "online", List.of(1));
                    var registration = new org.apache.bookkeeper.common.profile.startup
                            .EphemeralProfileRegistrationStore.WritableRegistration(target.bookie(),
                                    identity.incarnation(),
                                    gen, ready.storeVersion());
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.APPLIED, publisher.registerWritable(registration).status());
                    assertTrue(admission.openAfterReconciliation(registration));
                    var forcing = new CountDownLatch(1);
                    var release = new CountDownLatch(1);
                    var held = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(release);
                    });
                    var field = SegmentBookieControlLog.class.getDeclaredField("io");
                    field.setAccessible(true);
                    field.set(bookie(replay), held);
                    var follower = replay.startDeleteFollower(source, sourceMetadata, "ledger-profile",
                            sdk.getClientCtx().getScheduler(), fixture.cold, 1, Duration.ofSeconds(1),
                            Duration.ofMillis(20));
                    var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                            source, sourceMetadata, fixture.cold).beginDelete(44, bytes(16, 101), sdk.getClientCtx(),
                                    Duration.ofSeconds(30));
                    try {
                        assertTrue(client.prepared.await(5, TimeUnit.SECONDS));
                        var pending = fixture.reservations.readDeleteDelivery(metadata, target, 1);
                        assertEquals(ZKProfileReservationStore.DeliveryStatus.PREPARED, pending.status());
                        assertNotNull(pending.event().tombstone().publicationId());
                        assertEquals(104, pending.event().tombstone().encode().length);
                        assertEquals(org.apache.bookkeeper.common.profile.control.ProfileControlStore
                                .ReadStatus.UNAVAILABLE,
                                source.deleteAuthorityReader(target.bookie(), target.incarnation(), sourceMetadata)
                                    .read(org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey.fromScope(
                                        pending.event().deleteApply("ledger-profile").scope())).status());
                        assertTrue(admission.getAsBoolean());
                        assertEquals(0, held.forces);
                        assertNull(bookie(replay).deleteAppliedCursor());
                        client.release.countDown();
                        var deleted = deletion.result().get(10, TimeUnit.SECONDS);
                        deletion.completion().get(5, TimeUnit.SECONDS);
                        assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, deleted.status());
                        for (var obligated : deleted.manifest().targets()) {
                            assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED,
                                    fixture.reservations.readDeleteDelivery(metadata, obligated, 1).status());
                        }
                        assertTrue(forcing.await(5, TimeUnit.SECONDS));
                        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (follower.lastStatus() != IsolatedSegmentStorage.DeleteCatchUpStatus.UNKNOWN
                                && System.nanoTime() < until) {
                            Thread.sleep(5);
                        }
                        assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.UNKNOWN, follower.lastStatus());
                        assertTrue(admission.getAsBoolean());
                        var stopping = follower.closeAsync();
                        assertFalse(admission.getAsBoolean());
                        assertFalse(stopping.isDone());
                        stopping.complete(null);
                        assertFalse(follower.closeAsync().isDone());
                        assertFalse(replay.close(Duration.ofMillis(100)));
                        assertThrows(java.io.IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                        release.countDown();
                        follower.closeAsync().get(5, TimeUnit.SECONDS);
                        assertEquals(1, bookie(replay).deleteAppliedCursor().sequence());
                        assertTrue(bookie(replay).hasTerminalTombstone(fixture.context()));
                        assertNull(follower.failure());
                        assertTrue(replay.close(Duration.ofSeconds(5)));
                        publisher.demote(target.bookie());
                    } finally {
                        client.release.countDown();
                        release.countDown();
                        deletion.completion().get(10, TimeUnit.SECONDS);
                        follower.closeAsync().get(10, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    private static final class LastPreparedReplyClient extends ZooKeeper {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        LastPreparedReplyClient(String connect, ZooKeeperWatcherBase watcher) throws Exception {
            super(connect, 10000, watcher);
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var copied = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(copied::add);
            boolean last = copied.stream().anyMatch(op -> op.getType() == ZooDefs.OpCode.setData
                    && op.getPath().endsWith("/delivery-prepared")
                    && ((org.apache.zookeeper.proto.SetDataRequest) op.toRequestRecord()).getVersion() == 2);
            super.multi(copied, (code, path, ignored, results) -> {
                if (last && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()) {
                    held.countDown();
                    await(release);
                }
                callback.processResult(code, path, context, results);
            }, context);
        }
    }

    @Test
    public void preparedDeleteCancellationResumesSameEventsAndMissingPublicationBirthQuarantines() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(44, bytes(16, 51),
                    sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential, data, fixture.clients,
                    4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            var watcher = new ZooKeeperWatcherBase(10000, false);
            try (var held = new LastPreparedReplyClient(fixture.cluster.getZooKeeperConnectString(), watcher)) {
                var store = new ZKProfileReservationStore(held, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                var heldMetadata = new ProfileLedgerMetadataStore(held, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var coordinator = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                        store, heldMetadata, fixture.cold);
                var attempt = coordinator.beginDelete(44, bytes(16, 101), sdk.getClientCtx(),
                        Duration.ofSeconds(30));
                try {
                    assertTrue(held.held.await(5, TimeUnit.SECONDS));
                    assertTrue(attempt.result().cancel(false));
                    assertFalse(attempt.completion().isDone());
                    var sealed = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertFalse(sealed.getCustomMetadata().containsKey(
                            org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.TOMBSTONE_KEY));
                } finally {
                    held.release.countDown();
                }
                attempt.completion().get(10, TimeUnit.SECONDS);
                assertFalse(fixture.manager.readLedgerMetadata(44).get().getValue().getCustomMetadata()
                        .containsKey(org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.TOMBSTONE_KEY));
                var target = new org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.Target(
                        fixture.identities.get(0).bookieId().toString(),
                        fixture.identities.get(0).incarnation().value());
                var before = fixture.reservations.readDeleteDelivery(metadata, target, 1);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.PREPARED, before.status());
                var resumed = coordinator.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                        Duration.ofSeconds(30))
                        .get(30, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, resumed.status());
                var after = fixture.reservations.readDeleteDelivery(metadata, target, 1);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, after.status());
                assertEquals(before.event(), after.event());
                assertEquals(1, fixture.reservations.readDeleteHead(target).head().committedThrough());
                String indexName = "/socket-initial/2c/" + fixture.zk.getChildren("/socket-initial/2c", false).stream()
                        .filter(child -> child.startsWith("delete-delivery-"))
                        .filter(child -> {
                            try {
                                return org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery.Event.decode(
                                        fixture.zk.getData("/socket-initial/2c/" + child, false, null))
                                            .target().equals(target);
                            } catch (Exception failure) {
                                throw new IllegalStateException(failure);
                            }
                        }).findFirst().orElseThrow();
                fixture.zk.delete(indexName, 0);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                        fixture.reservations.appendDeleteDelivery(metadata, 44, target).status());
                assertEquals(1, fixture.reservations.readDeleteHead(target).head().committedThrough());
                var intact = resumed.manifest().targets().get(1);
                String publication = "/socket-initial/2c/delete-targets/delivery-prepared";
                org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, publication);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                        fixture.reservations.readDeleteDelivery(metadata, intact, 1).status());
                assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                        fixture.reservations.appendDeleteDelivery(metadata, 44, intact).status());
                assertEquals(ZKProfileReservationStore.DeleteStatus.QUARANTINED,
                        coordinator.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                assertNull(fixture.zk.exists(publication, false));
            }
        }
    }

    private static final class LostDeleteReplyClient extends ZooKeeper {
        private final int dropAt;
        private final boolean journal;
        private int commits;
        volatile boolean dropped;

        LostDeleteReplyClient(String connect, ZooKeeperWatcherBase watcher, int dropAt) throws Exception {
            this(connect, watcher, dropAt, false);
        }

        LostDeleteReplyClient(String connect, ZooKeeperWatcherBase watcher, int dropAt, boolean journal)
                throws Exception {
            super(connect, 10000, watcher);
            this.dropAt = dropAt;
            this.journal = journal;
            watcher.waitForConnection();
        }

        @Override
        public void multi(Iterable<org.apache.zookeeper.Op> operations,
                org.apache.zookeeper.AsyncCallback.MultiCallback callback, Object context) {
            var ops = new ArrayList<org.apache.zookeeper.Op>();
            operations.forEach(ops::add);
            boolean mutation = ops.stream().anyMatch(op -> op.getType() == ZooDefs.OpCode.setData
                    && (journal ? op.getPath().contains("/delete-delivery/")
                        : !op.getPath().contains("/delete-delivery/")
                            && !op.getPath().contains("/delivery-prepared")));
            super.multi(ops, (code, name, ignored, results) -> {
                if (mutation && code == org.apache.zookeeper.KeeperException.Code.OK.intValue()
                        && ++commits == dropAt) {
                    dropped = true;
                    callback.processResult(org.apache.zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue(),
                            name, context, results);
                } else {
                    callback.processResult(code, name, context, results);
                }
            }, context);
        }
    }

    @Test
    public void logicalDeleteResolvesEachMetadataReplyLossAndRetainsOverwrittenActivationTargets() throws Exception {
        for (int drop : List.of(1, 2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 4, 3, true, true); BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 4; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                }
                var next = new ProfileReplacementCoordinator.Target(fixture.connect(3, 4, Duration.ofSeconds(5)),
                        data.get(fixture.identities.get(3).bookieId()));
                var current = new HashMap<>(data);
                current.remove(fixture.identities.get(3).bookieId());
                var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, current, 8,
                        1024, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var recovery = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                handle.enableReplacement(new ProfileReplacementCoordinator(fixture.reservations, metadata,
                        VALIDATOR, fixture.cold), List.of(next), fixture.descriptorBytes, 1).get(5, TimeUnit.SECONDS);
                handle.enableWriterClose(recovery, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                        4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
                fixture.expectedFailedDataSessions.add(2);
                files(fixture.nodes.get(2)).dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                try {
                    assertEquals(0, sdkAdd(handle, bytes(16, 8)).get(10, TimeUnit.SECONDS).longValue());
                    assertEquals(List.of(0L), new ArrayList<>(handle.getLedgerMetadata().getAllEnsembles().keySet()));
                    assertFalse(handle.getLedgerMetadata().getAllEnsembles().values().stream().flatMap(List::stream)
                            .anyMatch(bookie -> bookie.toString().equals("socket2:3181")));
                    handle.closeAsync().get(30, TimeUnit.SECONDS);
                    var context = org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.decode(
                            handle.getLedgerMetadata().getCustomMetadata().get(
                                org.apache.bookkeeper.discover.profile.ProfileRecoveryContext.KEY));
                    var watcher = new ZooKeeperWatcherBase(10000, false);
                    try (var lost = new LostDeleteReplyClient(fixture.cluster.getZooKeeperConnectString(),
                            watcher, drop)) {
                        var store = new ZKProfileReservationStore(lost, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                Duration.ofSeconds(5));
                        var lostMetadata = new ProfileLedgerMetadataStore(lost,
                                (AbstractZkLedgerManager) fixture.manager,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                        var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                                store, lostMetadata, fixture.cold);
                        var deleted = deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                                Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS);
                        assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, deleted.status());
                        assertTrue(lost.dropped);
                        assertEquals(4, deleted.manifest().targets().size());
                        assertTrue(deleted.manifest().targets().stream()
                                .anyMatch(target -> target.bookie().equals("socket2:3181")));
                        assertEquals(ProfileLedgerMetadataStore.Outcome.ALREADY_APPLIED,
                                store.readClosedRecovery(lostMetadata, context).outcome());
                    }
                } finally {
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    public void logicalDeleteCancellationKeepsActualColdAttemptAndResumesTheSameRequest() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(44, bytes(16, 51),
                    sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential, data, fixture.clients,
                    4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            var watcher = new ZooKeeperWatcherBase(10000, false);
            try (var held = new HoldWriterEnterClient(fixture.cluster.getZooKeeperConnectString(), watcher)) {
                var store = new ZKProfileReservationStore(held, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                var heldMetadata = new ProfileLedgerMetadataStore(held, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                        store, heldMetadata, fixture.cold);
                var attempt = deletion.beginDelete(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30));
                var pending = attempt.result();
                try {
                    assertTrue(held.entered.await(5, TimeUnit.SECONDS));
                    assertTrue(pending.cancel(false));
                    assertFalse(attempt.completion().isDone());
                    assertEquals(ZKProfileReservationStore.DeleteStatus.UNAVAILABLE,
                            deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                                .get(5, TimeUnit.SECONDS).status());
                    var partial = fixture.manager.readLedgerMetadata(44).get().getValue();
                    assertNull(partial.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.SEAL_KEY));
                    assertNull(partial.getCustomMetadata().get(
                            org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.TOMBSTONE_KEY));
                    assertEquals(org.apache.bookkeeper.discover.profile.ProfileLifecycleHead.State.DELETE_INTENT,
                            fixture.reservations.read(44).snapshot().lifecycle().state());
                    held.release.run();
                    held.release = null;
                    attempt.completion().get(5, TimeUnit.SECONDS);
                    var fresh = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                            fixture.reservations, metadata, fixture.cold);
                    assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED,
                            fresh.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                                .get(30, TimeUnit.SECONDS).status());
                } finally {
                    if (held.release != null) {
                        held.release.run();
                    }
                }
            }
        }
    }

    @Test
    public void logicalDeleteCannotReuseARecreatedManifestOrLoseItsPublishedObligation() throws Exception {
        for (boolean recreate : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var data = new HashMap<BookieId, ProfileNormalDataClient>();
                for (int node = 0; node < 3; node++) {
                    data.put(fixture.identities.get(node).bookieId(),
                            dataClient(fixture, node, 8, Duration.ofSeconds(5)));
                }
                var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var recovery = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                recovery.recover(44, bytes(16, 51), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR,
                        fixture.credential, data, fixture.clients, 4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                        fixture.reservations, metadata, fixture.cold);
                assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED,
                        deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                String name = "/socket-initial/2c/delete-targets";
                byte[] original = fixture.zk.getData(name, false, null);
                org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, name);
                if (recreate) {
                    fixture.zk.create(name, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                assertEquals(ZKProfileReservationStore.DeleteStatus.QUARANTINED,
                        deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(), Duration.ofSeconds(30))
                            .get(30, TimeUnit.SECONDS).status());
                assertEquals(org.apache.bookkeeper.discover.profile.ProfileLifecycleHead.State.DELETE_INTENT,
                        fixture.reservations.read(44).snapshot().lifecycle().state());
                assertThrows(ExecutionException.class,
                        () -> recovery.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                            .get(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void committedDeleteDeliveriesShareOneIncarnationStreamAcrossTwoActualLedgers() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var recovery = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
            var initialWriter = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                    1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
            initialWriter.enableWriterClose(recovery, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                    4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
            assertEquals(0, initialWriter.appendAsync(Unpooled.wrappedBuffer(bytes(16, 8)))
                    .get(5, TimeUnit.SECONDS).longValue());
            initialWriter.closeAsync().get(30, TimeUnit.SECONDS);
            var oldReader = recovery.openReadOnly(44, sdk.getClientCtx(), fixture.password, data, 4)
                    .get(5, TimeUnit.SECONDS);
            try (var entries = oldReader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
            }
            var scopes = fixture.scopes.stream().map(scope -> new ProfileControlScope(scope.operation(),
                    scope.purpose(), 45, bytes(16, 61), scope.descriptorIdentity(), scope.targetBookie(),
                    scope.targetStorageIncarnation(),
                    scope.authorityDomain(), scope.authorityGeneration(), bytes(16, 73), bytes(32, 74), null)).toList();
            var reservation = new ProfileReservation(new ProfileMetadataBacklink(45,
                    bytes(16, 61), bytes(16, 62)), scopes);
            var nextMetadata = LedgerMetadataBuilder.create().withId(45).withEnsembleSize(3).withWriteQuorumSize(3)
                    .withAckQuorumSize(2).withDigestType(DigestType.CRC32C).withPassword(fixture.password)
                    .newEnsembleEntry(0, scopes.stream().map(scope -> BookieId.parse(scope.targetBookie()))
                        .toList()).build();
            var installs = new HashMap<String, ProfileInitialMetadataCoordinator.Target>();
            var activations = new HashMap<String, ProfileInitialLifecycleCoordinator.Target>();
            fixture.clients.forEach((name, client) -> {
                installs.put(name, new ProfileInitialMetadataCoordinator.Target(client.hello(), client));
                activations.put(name, new ProfileInitialLifecycleCoordinator.Target(client.hello(), client));
            });
            var second = new ProfileInitialLifecycleCoordinator(fixture.reservations, metadata, VALIDATOR, fixture.cold)
                    .initialize(nextMetadata, reservation, fixture.descriptorBytes, fixture.credential, installs,
                            activations, bytes(16, 75), bytes(32, 76)).get(10, TimeUnit.SECONDS);
            assertEquals(ProfileInitialLifecycleCoordinator.Status.AVAILABLE, second.status());
            var secondWriter = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), second, data, 8,
                    1024, 1, Duration.ofSeconds(3), Duration.ofMillis(5));
            secondWriter.enableWriterClose(recovery, fixture.descriptorBytes, VALIDATOR, fixture.clients,
                    4, Duration.ofSeconds(30)).get(5, TimeUnit.SECONDS);
            secondWriter.closeAsync().get(30, TimeUnit.SECONDS);
            var deletion = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                    fixture.reservations, metadata, fixture.cold);
            var firstDeleted = deletion.deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                    Duration.ofSeconds(30))
                    .get(30, TimeUnit.SECONDS);
            assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, firstDeleted.status());
            try (var entries = oldReader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                assertArrayEquals(bytes(16, 8), entries.getEntry(0).getEntryBytes());
            }
            var target = firstDeleted.manifest().targets().get(0);
            var firstEvent = fixture.reservations.appendDeleteDelivery(metadata, 44, target);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, firstEvent.status());
            assertEquals(1, firstEvent.event().sequence());
            verifyActualDeleteRejoinCut(fixture, sdk, metadata, firstDeleted.manifest().targets().get(1));
            var secondEvent = fixture.reservations.appendDeleteDelivery(metadata, 45, target);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, secondEvent.status());
            assertEquals(2, secondEvent.event().sequence());
            assertArrayEquals(firstEvent.event().streamId(), secondEvent.event().streamId());
            assertEquals(44, firstEvent.event().ledger().ledgerId());
            assertEquals(45, secondEvent.event().ledger().ledgerId());
            assertEquals(firstEvent.event(), fixture.reservations.appendDeleteDelivery(metadata, 44, target).event());
            assertEquals(secondEvent.event(), fixture.reservations.appendDeleteDelivery(metadata, 45, target).event());
            assertEquals(firstEvent.event(), fixture.reservations.readDeleteDelivery(metadata, target, 1).event());
            assertEquals(secondEvent.event(), fixture.reservations.readDeleteDelivery(metadata, target, 2).event());
            verifyActualDeleteCatchUpLimitAndUnknown(fixture, metadata, firstDeleted.manifest().targets().get(2));
            var client = fixture.clients.get(target.bookie());
            var firstRequest = firstEvent.event().deleteApply("ledger-profile");
            var secondRequest = secondEvent.event().deleteApply("ledger-profile");
            int node = fixture.identities.stream().map(identity -> identity.bookieId().toString()).toList()
                    .indexOf(target.bookie());
            var local = bookie(fixture.nodes.get(node));
            var event = firstEvent.event();
            var forged = new org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery.Event(bytes(16, 99),
                    event.streamGeneration(), event.assignmentGeneration(), event.sequence(), event.ledger(),
                    event.target(), event.tombstone(), event.requestId(), event.deleteEpoch());
            assertEquals(ProfileControlResult.Outcome.DENIED,
                    client.deleteApply(forged.deleteApply("ledger-profile")).get(5, TimeUnit.SECONDS).outcome());
            assertNull(local.deleteAppliedCursor());
            assertEquals(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                    client.deleteApply(secondRequest).get(5, TimeUnit.SECONDS).outcome());
            assertNull(local.deleteAppliedCursor());
            try (var oldPin = files(fixture.nodes.get(node)).pinDataRead()) {
                var firstApplied = client.deleteApply(firstRequest).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileControlResult.Outcome.APPLIED, firstApplied.outcome());
                assertTrue(firstApplied.durable());
                assertEquals(1, firstApplied.localGeneration());
                assertEquals(1, local.deleteAppliedCursor().sequence());
                assertTrue(local.hasTerminalTombstone(fixture.context()));
                var refused = data.get(fixture.identities.get(node).bookieId()).readEntry(fixture.context(), 0)
                        .get(5, TimeUnit.SECONDS);
                assertEquals(ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE, refused.status().statusClass());
                assertEquals(org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec.Outcome.REFUSED,
                        refused.outcome());
                assertEquals(0, refused.entry().remaining());
                assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED,
                        client.deleteApply(firstRequest).get(5, TimeUnit.SECONDS).outcome());
                assertEquals(ProfileControlResult.Outcome.APPLIED,
                        client.deleteApply(secondRequest).get(5, TimeUnit.SECONDS).outcome());
                assertEquals(2, local.deleteAppliedCursor().sequence());
                assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED,
                        client.deleteApply(firstRequest).get(5, TimeUnit.SECONDS).outcome());
                assertEquals(2, local.deleteAppliedCursor().sequence());
            }
            var wrong = new org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.Target(
                    target.bookie(), bytes(16, 103));
            assertEquals(ZKProfileReservationStore.DeliveryStatus.NOT_APPLICABLE,
                    fixture.reservations.appendDeleteDelivery(metadata, 44, wrong).status());
            var children = fixture.zk.getChildren("/socket-initial/delete-delivery", false);
            assertEquals(6, children.size()); // One head/assignment per incarnation, shared across both ledgers.
            String headName = "/socket-initial/delete-delivery/" + children.stream()
                    .filter(child -> !child.endsWith("-assignment"))
                    .filter(child -> {
                        try {
                            return org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery.Head.decode(
                                    fixture.zk.getData("/socket-initial/delete-delivery/" + child, false, null))
                                        .target().equals(target);
                        } catch (Exception failure) {
                            throw new IllegalStateException(failure);
                        }
                    }).findFirst().orElseThrow();
            byte[] firstBytes = fixture.zk.getData(headName + "/event-1", false, null);
            fixture.zk.delete(headName + "/event-1", 0);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                    fixture.reservations.readDeleteDelivery(metadata, target, 1).status());
            fixture.zk.create(headName + "/event-1", firstBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                    fixture.reservations.readDeleteDelivery(metadata, target, 1).status());
            assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                    fixture.reservations.appendDeleteDelivery(metadata, 44, target).status());
            assertEquals(ProfileControlResult.Outcome.QUARANTINED,
                    client.deleteApply(firstRequest).get(5, TimeUnit.SECONDS).outcome());
            oldReader.closeAsync().get(5, TimeUnit.SECONDS);
            var restartConfig = files(fixture.nodes.get(node)).config();
            fixture.nodes.get(node).close();
            try (var replay = new IsolatedSegmentStorage(restartConfig, fixture.identities.get(node),
                    new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                    new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                            2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                    new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                            new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                replay.restart();
                assertEquals(2, bookie(replay).deleteAppliedCursor().sequence());
                assertTrue(bookie(replay).hasTerminalTombstone(fixture.context()));
                assertTrue(bookie(replay).hasTerminalTombstone(secondEvent.event().ledger()));
            }
            byte[] lastHead = fixture.zk.getData(headName, false, null);
            org.apache.zookeeper.ZKUtil.deleteRecursive(fixture.zk, headName);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                    fixture.reservations.appendDeleteDelivery(metadata, 45, target).status());
            fixture.zk.create(headName, lastHead, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEquals(ZKProfileReservationStore.DeliveryStatus.QUARANTINED,
                    fixture.reservations.appendDeleteDelivery(metadata, 45, target).status());
        }
    }

    private static void verifyActualDeleteCatchUpLimitAndUnknown(Fixture fixture, ProfileLedgerMetadataStore metadata,
            org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.Target target) throws Exception {
        assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED,
                fixture.reservations.appendDeleteDelivery(metadata, 44, target).status());
        assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED,
                fixture.reservations.appendDeleteDelivery(metadata, 45, target).status());
        int node = fixture.identities.stream().map(identity -> identity.bookieId().toString()).toList()
                .indexOf(target.bookie());
        var config = files(fixture.nodes.get(node)).config();
        fixture.nodes.get(node).close();
        try (var replay = new IsolatedSegmentStorage(config, fixture.identities.get(node),
                new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                        2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
            replay.restart();
            var limited = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                    fixture.reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(5)), fixture.cold)
                    .get(10, TimeUnit.SECONDS);
            assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.LIMIT, limited.status());
            limited.completion().get(5, TimeUnit.SECONDS);
            assertEquals(1, replay.profileDeleteProgress().sequence());
            var forcing = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var held = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            var field = SegmentBookieControlLog.class.getDeclaredField("io");
            field.setAccessible(true);
            field.set(bookie(replay), held);
            var pending = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                    fixture.reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(1)), fixture.cold);
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                var unknown = pending.get(5, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.UNKNOWN, unknown.status());
                assertFalse(unknown.completion().isDone());
                assertEquals(1, replay.profileDeleteProgress().sequence());
                var callerView = unknown.completion();
                callerView.complete(null);
                assertFalse(unknown.completion().isDone());
                release.countDown();
                unknown.completion().get(5, TimeUnit.SECONDS);
                assertEquals(2, replay.profileDeleteProgress().sequence());
                var reconciled = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                        fixture.reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(5)), fixture.cold)
                        .get(10, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP, reconciled.status());
                reconciled.completion().get(5, TimeUnit.SECONDS);
                assertEquals(1, held.forces);
            } finally {
                release.countDown();
                pending.get(10, TimeUnit.SECONDS).completion().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void verifyActualDeleteRejoinCut(Fixture fixture, BookKeeper sdk,
            ProfileLedgerMetadataStore originalMetadata,
            org.apache.bookkeeper.discover.profile.ProfileDeleteRecord.Target target) throws Exception {
        var firstEvent = fixture.reservations.appendDeleteDelivery(originalMetadata, 44, target);
        assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, firstEvent.status());
        int node = fixture.identities.stream().map(identity -> identity.bookieId().toString()).toList()
                .indexOf(target.bookie());
        var config = files(fixture.nodes.get(node)).config();
        fixture.nodes.get(node).close();
        var watcher = new ZooKeeperWatcherBase(10000, false);
        try (var client = new DeleteRegistrationCutClient(fixture.cluster.getZooKeeperConnectString(), watcher);
                var replay = new IsolatedSegmentStorage(config, fixture.identities.get(node),
                        new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                        new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                        new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
            String root = "/socket-delete-rejoin";
            client.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            var reservations = new ZKProfileReservationStore(client, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            var metadata = new ProfileLedgerMetadataStore(client, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var persistent = new org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore(
                    client, root, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var publisher = new org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore(
                    client, root, target.bookie(), ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            publisher.bindDeleteDelivery(reservations, fixture.identities.get(node).incarnation(),
                    replay::profileDeleteProgress);
            try (var admission = publisher.watchAdmission(watcher)) {
                replay.bindProfileRegistration(admission);
                replay.restart();
                assertFalse(admission.getAsBoolean());
                assertNull(replay.profileDeleteProgress());
                assertFalse(bookie(replay).hasTerminalTombstone(fixture.context()));
                var gen = new org.apache.bookkeeper.common.profile.startup.Generation(1);
                var record = new org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord(target.bookie(),
                        fixture.identities.get(node).incarnation(), EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, gen, gen,
                        new org.apache.bookkeeper.common.profile.startup.ManifestIdentity(bytes(32, 7)), gen, gen, gen,
                        1, 1, List.of(1),
                        org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord.State.READY);
                assertEquals(org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus.APPLIED,
                        persistent.compareAndSet(target.bookie(), -1, 0, record).status());
                var ready = persistent.read(target.bookie()).versionedRecord().orElseThrow();
                var projection = new org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection(
                        target.bookie(), record.storageIncarnation(), gen, ready.storeVersion(), "rejoin", List.of(1));
                var registration = new org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                        .WritableRegistration(target.bookie(), record.storageIncarnation(), gen, ready.storeVersion());
                assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                        .PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
                assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                        .PublicationStatus.UNAVAILABLE, publisher.registerWritable(registration).status());
                assertTrue(publisher.readWritable(target.bookie()).isEmpty());
                var first = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                        reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(5)), fixture.cold)
                        .get(10, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP, first.status());
                first.completion().get(5, TimeUnit.SECONDS);
                assertEquals(1, replay.profileDeleteProgress().sequence());
                assertTrue(bookie(replay).hasTerminalTombstone(fixture.context()));
                try (var localReady = new SegmentLocalReadinessStore(config.directory(), target.bookie(),
                        record.storageIncarnation())) {
                    var receipt = new org.apache.bookkeeper.common.profile.startup.DurableLocalReadiness(
                            target.bookie(), record.storageIncarnation(), record.deviceManifestIdentity(),
                            gen, gen, gen,
                            gen, org.apache.bookkeeper.common.profile.startup.DurableLocalReadiness.Status.READY, true);
                    assertEquals(org.apache.bookkeeper.common.profile.startup.LocalReadinessStore.WriteStatus.APPLIED,
                            localReady.write(receipt).status());
                    assertEquals(receipt, localReady.read(target.bookie()).orElseThrow());
                }

                client.beforeWritable = () -> {
                    try {
                        var deleted = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                                fixture.reservations, originalMetadata, fixture.cold)
                                .deleteLedgerAsync(45, bytes(16, 102), sdk.getClientCtx(),
                                        Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                        assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, deleted.status());
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    }
                };
                assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                        .PublicationStatus.CONFLICT, publisher.registerWritable(registration).status());
                assertTrue(publisher.readWritable(target.bookie()).isEmpty());
                assertFalse(admission.getAsBoolean());
                try (var heldPin = files(replay).pinDataRead()) {
                    var second = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                            reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(5)), fixture.cold)
                            .get(10, TimeUnit.SECONDS);
                    assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP, second.status());
                    second.completion().get(5, TimeUnit.SECONDS);
                    assertEquals(2, replay.profileDeleteProgress().sequence());
                    client.loseWritable = true;
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.RESPONSE_LOST, publisher.registerWritable(registration).status());
                    assertTrue(client.dropped);
                    assertTrue(publisher.readWritable(target.bookie()).filter(registration::equals).isPresent());
                    assertEquals(org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore
                            .PublicationStatus.ALREADY_APPLIED, publisher.registerWritable(registration).status());
                    assertTrue(admission.openAfterReconciliation(registration));
                    assertTrue(admission.allows(target.bookie(), record.storageIncarnation()));
                }
                var noRepeat = CompletableFuture.supplyAsync(() -> replay.catchUpProfileDeletes(
                        reservations, metadata, "ledger-profile", 1, Duration.ofSeconds(5)), fixture.cold)
                        .get(10, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP, noRepeat.status());
                assertTrue(admission.getAsBoolean());
                assertFalse(replay.profileReadVisible(fixture.context(), 0));
                publisher.demote(target.bookie());
                assertFalse(admission.getAsBoolean());
            }
        }
    }

    @Test
    public void committedDeleteDeliveryReconcilesLostReplyAndOneConcurrentSequenceWinner() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (int node = 0; node < 3; node++) {
                data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(44, bytes(16, 51),
                    sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential, data, fixture.clients,
                    4, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            var watcher = new ZooKeeperWatcherBase(10000, false);
            try (var lost = new LostDeleteReplyClient(fixture.cluster.getZooKeeperConnectString(), watcher, 1, true)) {
                var store = new ZKProfileReservationStore(lost, "/socket-initial", ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                var lostMetadata = new ProfileLedgerMetadataStore(lost, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
                var deleted = new org.apache.bookkeeper.discover.profile.ProfileDeleteCoordinator(
                        store, lostMetadata, fixture.cold).deleteLedgerAsync(44, bytes(16, 101), sdk.getClientCtx(),
                                Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                assertEquals(ZKProfileReservationStore.DeleteStatus.LOGICALLY_DELETED, deleted.status());
                var target = deleted.manifest().targets().get(0);
                var loss = store.appendDeleteDelivery(lostMetadata, 44, target);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, loss.status());
                assertTrue(lost.dropped);
                int appliedNode = fixture.identities.stream().map(identity -> identity.bookieId().toString()).toList()
                        .indexOf(target.bookie());
                fixture.faultTarget.set(appliedNode);
                fixture.replyFault.set(24);
                var request = loss.event().deleteApply("ledger-profile");
                assertThrows(ExecutionException.class,
                        () -> fixture.clients.get(target.bookie()).deleteApply(request).get(5, TimeUnit.SECONDS));
                assertEquals(1, bookie(fixture.nodes.get(appliedNode)).deleteAppliedCursor().sequence());
                fixture.replyFault.set(0);
                var freshControl = fixture.connect(appliedNode, 4, Duration.ofSeconds(5));
                assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED,
                        freshControl.deleteApply(request).get(5, TimeUnit.SECONDS).outcome());
                var racedTarget = deleted.manifest().targets().get(1);
                var first = CompletableFuture.supplyAsync(
                        () -> store.appendDeleteDelivery(lostMetadata, 44, racedTarget),
                        fixture.cold);
                var other = CompletableFuture.supplyAsync(
                        () -> fixture.reservations.appendDeleteDelivery(metadata, 44, racedTarget), fixture.cold);
                var firstResult = first.get(10, TimeUnit.SECONDS);
                var otherResult = other.get(10, TimeUnit.SECONDS);
                assertTrue(firstResult.status() == ZKProfileReservationStore.DeliveryStatus.COMMITTED
                        || otherResult.status() == ZKProfileReservationStore.DeliveryStatus.COMMITTED);
                var resolved = store.appendDeleteDelivery(lostMetadata, 44, racedTarget);
                assertEquals(ZKProfileReservationStore.DeliveryStatus.COMMITTED, resolved.status());
                assertEquals(1, resolved.event().sequence());
                assertEquals(resolved.event(), fixture.reservations
                        .appendDeleteDelivery(metadata, 44, racedTarget).event());
                var children = fixture.zk.getChildren("/socket-initial/delete-delivery", false);
                String headName = "/socket-initial/delete-delivery/" + children.stream()
                        .filter(child -> !child.endsWith("-assignment"))
                        .filter(child -> {
                            try {
                                return org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery.Head.decode(
                                        fixture.zk.getData("/socket-initial/delete-delivery/" + child, false, null))
                                            .target().equals(racedTarget);
                            } catch (Exception failed) {
                                throw new IllegalStateException(failed);
                            }
                        }).findFirst().orElseThrow();
                var head = org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery.Head.decode(
                        fixture.zk.getData(headName, false, null));
                assertEquals(1, head.committedThrough());
                assertEquals(1, fixture.zk.getChildren(headName, false).stream()
                        .filter(child -> child.startsWith("event-")).count());
                int failedNode = fixture.identities.stream().map(identity -> identity.bookieId().toString()).toList()
                        .indexOf(racedTarget.bookie());
                var failedIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                var ioField = SegmentBookieControlLog.class.getDeclaredField("io");
                ioField.setAccessible(true);
                ioField.set(bookie(fixture.nodes.get(failedNode)), failedIo);
                var uncertain = fixture.clients.get(racedTarget.bookie())
                        .deleteApply(resolved.event().deleteApply("ledger-profile")).get(5, TimeUnit.SECONDS);
                assertEquals(ProfileControlResult.Outcome.DURABILITY_UNKNOWN, uncertain.outcome());
                assertFalse(uncertain.durable());
                assertEquals(0, uncertain.localGeneration());
                assertNull(bookie(fixture.nodes.get(failedNode)).deleteAppliedCursor());
                assertEquals(1, failedIo.forces);

            }
        }
    }

    private static Channel dataChannel(ProfileNormalDataClient client) throws Exception {
        var ownerField = ProfileNormalDataClient.class.getDeclaredField("socket");
        ownerField.setAccessible(true);
        var owner = ownerField.get(client);
        var channelField = owner.getClass().getDeclaredField("channel");
        channelField.setAccessible(true);
        return (Channel) channelField.get(owner);
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
                        var lac = lacDigest(fixture, 2);
                        try {
                            assertEquals(ProfileStatus.DurableResult.APPLIED,
                                    peer.beginWriteLac(fixture.context(), fixture.credential, 2, lac).result().get()
                                        .durableResult());
                        } finally {
                            lac.release();
                        }
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

    private static org.apache.bookkeeper.util.ByteBufList lacDigest(Fixture fixture, long value) throws Exception {
        return DigestManager.instantiate(44, fixture.password,
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32C),
                UnpooledByteBufAllocator.DEFAULT, false).computeDigestAndPackageForSendingLac(value);
    }

    @Test
    public void writeLacSdkPublishesConfirmedPrefixAndRecoveryConsumesItForA2AndA3() throws Exception {
        for (int ack : List.of(2, 3)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, ack, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                try (BookKeeper sdk = sdk(fixture)) {
                    var handle = writer(fixture, sdk, available, 8, 3,
                            Duration.ofSeconds(5), Duration.ofMillis(5));
                    try {
                        assertThrows(ExecutionException.class, () -> handle.flushConfirmedLac().get());
                        assertEquals(0, handle.addEntry(bytes(16, 8)));
                        assertEquals(1, handle.addEntry(bytes(16, 9)));
                        assertEquals(0, handle.readLacEvidence().get(5, TimeUnit.SECONDS).prefixEntryId());
                        assertEquals(1L, handle.flushConfirmedLac().get(5, TimeUnit.SECONDS).longValue());
                        var observed = handle.readLacEvidence().get(5, TimeUnit.SECONDS);
                        assertEquals(1, observed.prefixEntryId());
                        assertEquals(32, observed.prefixLength());
                        assertFalse(observed.explicitSources().isEmpty());
                        assertEquals(1, handle.getLastAddConfirmed());
                        assertEquals(LedgerMetadata.State.OPEN,
                                fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                    } finally {
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                    Map<BookieId, ProfileNormalDataClient> data = new HashMap<>();
                    for (int node = 0; node < 3; node++) {
                        var peer = dataClient(fixture, node, 4, Duration.ofSeconds(5));
                        data.put(fixture.identities.get(node).bookieId(), peer);
                        assertEquals(1, peer.readLac(fixture.context()).get().explicitLac());
                        // Later payload is speculative; its -1 hint cannot erase the accepted explicit authority.
                        evidenceEntry(fixture, peer, 3, -1, 48, 16);
                    }
                    var metadata = new ProfileLedgerMetadataStore(fixture.zk,
                            (AbstractZkLedgerManager) fixture.manager, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            Duration.ofSeconds(5));
                    var accepted = fixture.reservations.beginRecovery(metadata,
                            fixture.reservations.captureRecovery(metadata, 44, bytes(16, 31)).plan());
                    var coordinator = new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold);
                    var proof = coordinator.verifyPrefix(accepted, sdk.getClientCtx(), data, fixture.clients,
                            fixture.credential, 5, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);
                    assertEquals(ProfileRecoveryCoordinator.PrefixStatus.VERIFIED_NORMAL_TAIL, proof.status());
                    assertEquals(1, proof.requiredFrontier());
                    assertEquals(1, proof.prefixEntryId());
                    assertEquals(32, proof.prefixLength());
                    assertEquals(2, proof.unresolvedEntry());
                    assertEquals(LedgerMetadata.State.IN_RECOVERY,
                            fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                    var restartConfig = files(fixture.nodes.get(0)).config();
                    fixture.nodes.get(0).close();
                    try (var replay = new IsolatedSegmentStorage(restartConfig, fixture.identities.get(0),
                            new SecretKeySpec(bytes(32, 7), "AES"), VALIDATOR,
                            new SegmentExecutionGroup.Settings(1, 2, 6, 2, 4096, 4096,
                                    2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                            new IsolatedSegmentStorage.Limits(1024 * 1024, 8, 16, 1, 1,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
                        replay.restart();
                        assertEquals(1, bookie(replay).explicitLac(fixture.context()).orElseThrow().lac());
                        assertFalse(bookie(replay).normalAdmission(fixture.context(),
                                DigestManager.generateMasterKey(fixture.password)));
                    }
                }
            }
        }
    }

    @Test
    public void writeLacWireValidatesDigestCredentialScopeAndResolvesDuplicatesAfterFence() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var peer = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            var source = lacDigest(fixture, 1);
            var changed = lacDigest(fixture, 2);
            try {
                int before = fixture.nodeIo.get(0).forces;
                assertEquals(ProfileStatus.StatusClass.BAD_REQUEST,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 2, source)
                            .result().get().statusClass());
                assertEquals(ProfileStatus.StatusClass.UNAUTHORIZED,
                        peer.beginWriteLac(fixture.context(), ProtectedProfileCredential.of(1, bytes(20, 99)), 1,
                                source).result().get().statusClass());
                var wrong = new org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext(44,
                        bytes(16, 98), fixture.context().descriptorIdentity());
                assertEquals(ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT,
                        peer.beginWriteLac(wrong, fixture.credential, 1, source).result().get().statusClass());
                changed.getBuffer(0).setByte(19, changed.getBuffer(0).getByte(19) ^ 1);
                assertEquals(ProfileStatus.StatusClass.BAD_REQUEST,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 2, changed)
                            .result().get().statusClass());
                assertEquals(before, fixture.nodeIo.get(0).forces);
                assertEquals(ProfileStatus.DurableResult.APPLIED,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 1, source).result().get()
                            .durableResult());
                int applied = fixture.nodeIo.get(0).forces;
                assertEquals(before + 1, applied);
                assertEquals(ProfileStatus.DurableResult.ALREADY_APPLIED,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 1, source).result().get()
                            .durableResult());
                assertEquals(applied, fixture.nodeIo.get(0).forces);
                assertEquals(ProfileStatus.StatusClass.OK,
                        fixture.clients.get("socket0:3181").fence(fixture.context(), bytes(16, 27),
                                fixture.credential).get().statusClass());
                int fenced = fixture.nodeIo.get(0).forces;
                assertEquals(ProfileStatus.DurableResult.ALREADY_APPLIED,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 1, source).result().get()
                            .durableResult());
                changed.release();
                changed = lacDigest(fixture, 2);
                assertEquals(ProfileStatus.StatusClass.FENCED,
                        peer.beginWriteLac(fixture.context(), fixture.credential, 2, changed)
                            .result().get().statusClass());
                assertEquals(fenced, fixture.nodeIo.get(0).forces);
            } finally {
                source.release();
                changed.release();
            }
        }
    }

    @Test
    public void writeLacSdkNeedsDurableAckQuorumAndStopWaitsActualOutputAfterCancellation() throws Exception {
        for (boolean cancel : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                var available = fixture.initialize().get(5, TimeUnit.SECONDS);
                try (BookKeeper sdk = sdk(fixture)) {
                    var handle = writer(fixture, sdk, available, 8, 3,
                            Duration.ofSeconds(5), Duration.ofMillis(5));
                    var releases = List.of(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1));
                    var forcing = new CountDownLatch(cancel ? 3 : 2);
                    try {
                        handle.addEntry(bytes(16, 8));
                        var field = SegmentBookieControlLog.class.getDeclaredField("io");
                        field.setAccessible(true);
                        for (int node = cancel ? 0 : 1; node < 3; node++) {
                            var release = releases.get(node);
                            field.set(bookie(fixture.nodes.get(node)),
                                    new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                                        forcing.countDown();
                                        await(release);
                                    }));
                        }
                        var pending = handle.flushConfirmedLac();
                        assertTrue(forcing.await(5, TimeUnit.SECONDS));
                        assertThrows(java.util.concurrent.TimeoutException.class,
                                () -> pending.get(150, TimeUnit.MILLISECONDS));
                        if (cancel) {
                            assertTrue(pending.cancel(false));
                        } else {
                            releases.get(1).countDown();
                            assertEquals(0L, pending.get(5, TimeUnit.SECONDS).longValue());
                        }
                        var stopped = handle.stopLocalWrites();
                        assertThrows(java.util.concurrent.TimeoutException.class,
                                () -> stopped.get(150, TimeUnit.MILLISECONDS));
                        assertFalse(sdk.getClientCtx().isClientClosed());
                        releases.forEach(CountDownLatch::countDown);
                        stopped.get(5, TimeUnit.SECONDS);
                        assertEquals(LedgerMetadata.State.OPEN,
                                fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                    } finally {
                        releases.forEach(CountDownLatch::countDown);
                        handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    @Test
    public void writeLacLostReplyResolvesSameDurableBytesAndForceFailureNeverAcknowledges() throws Exception {
        for (boolean fail : List.of(false, true)) {
            try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
                fixture.connectAll(Duration.ofSeconds(5));
                fixture.initialize().get(5, TimeUnit.SECONDS);
                var peer = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
                var source = lacDigest(fixture, 1);
                try {
                    int before = fixture.nodeIo.get(0).forces;
                    if (fail) {
                        var field = SegmentBookieControlLog.class.getDeclaredField("io");
                        field.setAccessible(true);
                        var broken = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                        field.set(bookie(fixture.nodes.get(0)), broken);
                        var response = peer.beginWriteLac(fixture.context(), fixture.credential, 1, source)
                                .result().get(5, TimeUnit.SECONDS);
                        assertEquals(ProfileStatus.DurableResult.UNKNOWN, response.durableResult());
                        assertNotEquals(ProfileStatus.StatusClass.OK, response.statusClass());
                        assertEquals(1, broken.forces);
                        assertTrue(bookie(fixture.nodes.get(0)).explicitLac(fixture.context()).isEmpty());
                    } else {
                        fixture.faultTarget.set(0);
                        fixture.replyFault.set(25);
                        var unknown = peer.beginWriteLac(fixture.context(), fixture.credential, 1, source);
                        assertThrows(ExecutionException.class, () -> unknown.result().get(5, TimeUnit.SECONDS));
                        unknown.completion().get(5, TimeUnit.SECONDS);
                        assertEquals(before + 1, fixture.nodeIo.get(0).forces);
                        var fresh = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
                        assertEquals(ProfileStatus.DurableResult.ALREADY_APPLIED,
                                fresh.beginWriteLac(fixture.context(), fixture.credential, 1, source)
                                    .result().get().durableResult());
                        assertEquals(before + 1, fixture.nodeIo.get(0).forces);
                    }
                } finally {
                    source.release();
                }
            }
        }
    }

    @Test
    public void writeLacUnsupportedPeerGrantsNoDurableResult() throws Exception {
        try (Fixture fixture = new Fixture(false, false, false)) {
            var peer = dataClient(fixture, 0, 4, Duration.ofSeconds(5));
            assertFalse(peer.hello().capabilities().contains(
                    org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec.WRITE_LAC_CAPABILITY));
            var source = lacDigest(fixture, 1);
            try {
                var refused = peer.beginWriteLac(fixture.context(), fixture.credential, 1, source);
                assertEquals(ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                        refused.result().get().statusClass());
                assertEquals(ProfileStatus.DurableResult.NONE, refused.result().get().durableResult());
                refused.completion().get(5, TimeUnit.SECONDS);
                assertEquals(0, fixture.nodeIo.get(0).forces);
            } finally {
                source.release();
            }
        }
    }

    @Test
    public void writeLacCanceledQuorumWaitKeepsActualReplyCustodyUntilStopDrains() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true)) {
            fixture.connectAll(Duration.ofSeconds(5));
            var available = fixture.initialize().get(5, TimeUnit.SECONDS);
            try (BookKeeper sdk = sdk(fixture)) {
                var handle = writer(fixture, sdk, available, 8, 3,
                        Duration.ofSeconds(5), Duration.ofMillis(5));
                try {
                    handle.addEntry(bytes(16, 8));
                    fixture.replyFault.set(26);
                    var pending = handle.flushConfirmedLac();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                    }
                    assertEquals(3, fixture.heldReplies.size());
                    for (var node : fixture.nodes) {
                        assertEquals(0, bookie(node).explicitLac(fixture.context()).orElseThrow().lac());
                    }
                    assertTrue(pending.cancel(false));
                    var stopped = handle.stopLocalWrites();
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> stopped.get(150, TimeUnit.MILLISECONDS));
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                    stopped.get(5, TimeUnit.SECONDS);
                    assertTrue(pending.isCancelled());
                    assertEquals(LedgerMetadata.State.OPEN,
                            fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                } finally {
                    fixture.replyFault.set(0);
                    fixture.releaseReplies();
                    handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
                }
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
                    var stopping = handle.stopLocalWrites();
                    assertThrows(java.util.concurrent.TimeoutException.class,
                            () -> stopping.get(150, TimeUnit.MILLISECONDS));
                    release.countDown();
                    stopping.get(5, TimeUnit.SECONDS);
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
    public void profileReplicaAuditHealthyAndUnknownNeverClearAnExistingTask() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            prepareHealthyReplicaAudit(fixture, sdk, reader);
            var stats = new org.apache.bookkeeper.test.TestStatsProvider();
            var main = replicaAuditMain(fixture, stats.getStatsLogger(""), Duration.ofSeconds(30));
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                monitor.markLedgerUnderreplicated(44, "socket2:3181");
                startOnlyProfileAuditor(main);
                assertTrue(stats.getCounter("auditor.profile_data_audit_healthy").get() > 0);
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                fixture.readAllowed.set(false);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while ((stats.getCounter("auditor.profile_data_audit_deferred") == null
                        || stats.getCounter("auditor.profile_data_audit_deferred").get() == 0)
                        && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(stats.getCounter("auditor.profile_data_audit_deferred"));
                assertTrue(stats.getCounter("auditor.profile_data_audit_deferred").get() > 0);
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(0, main.getProfilePeerUsage().reservedConnections());
            } finally {
                fixture.readAllowed.set(true);
                main.shutdown();
            }
        }
    }

    @Test
    public void profileReplicaAuditDeadlineAndShutdownWaitForActualHealthAndLeaseDrain() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            prepareHealthyReplicaAudit(fixture, sdk, reader);
            var main = replicaAuditMain(fixture, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                    Duration.ofMillis(200));
            var clientField = main.getClass().getDeclaredField("bkc");
            clientField.setAccessible(true);
            var ownedSdk = (BookKeeper) clientField.get(main);
            var ownedZk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                    ownedSdk.getMetadataClientDriver()).getZk();
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                fixture.replyFault.set(21); // Actual READ replies held after receiver I/O, not a mock success.
                var electorField = main.getClass().getDeclaredField("auditorElector");
                electorField.setAccessible(true);
                ((org.apache.bookkeeper.replication.AuditorElector) electorField.get(main)).start()
                        .get(5, TimeUnit.SECONDS);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250)); // Beyond the caller deadline.
                assertEquals(6, main.getProfilePeerUsage().reservedConnections());
                var stopping = stopper.submit(() -> {
                    Thread.currentThread().interrupt();
                    main.shutdown();
                    return Thread.currentThread().isInterrupted();
                });
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertFalse(ownedSdk.getClientCtx().isClientClosed());
                assertEquals(ZooKeeper.States.CONNECTED, ownedZk.getState());
                assertEquals(6, main.getProfilePeerUsage().reservedConnections());
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                assertTrue(stopping.get(10, TimeUnit.SECONDS));
                assertEquals(ZooKeeper.States.CLOSED, ownedZk.getState());
                assertEquals(0, main.getProfilePeerUsage().reservedConnections());
                assertNull(monitor.getLedgerUnreplicationInfo(44));
            } finally {
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                main.shutdown();
                stopper.shutdownNow();
            }
        }
    }

    private static ProfileLedgerHandle prepareOpenReplicaAudit(Fixture fixture, BookKeeper sdk,
            ZooKeeper reader, Map<BookieId, ProfileNormalDataClient> data) throws Exception {
        fixture.helloReadinessGeneration = 1;
        fixture.connectAll(Duration.ofSeconds(5));
        var available = fixture.initialize().get(5, TimeUnit.SECONDS);
        for (int node = 0; node < 3; node++) {
            data.put(fixture.identities.get(node).bookieId(), dataClient(fixture, node, 8, Duration.ofSeconds(5)));
        }
        var handle = ProfileLedgerHandle.fromInitialAvailability(sdk.getClientCtx(), available, data, 8,
                1024, 3, Duration.ofSeconds(5), Duration.ofMillis(5));
        try {
            assertEquals(0, handle.addEntry(bytes(16, 8)));
            assertEquals(1, handle.addEntry(bytes(16, 8)));
            // Independently confirm the latest DATA source at all queried endpoints, beyond SDK A=2.
            for (var peer : data.values()) {
                evidenceEntry(fixture, peer, 1, 0, 32, 16);
            }
            for (int node = 0; node < 3; node++) {
                registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            return handle;
        } catch (Exception | Error failure) {
            handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            throw failure;
        }
    }

    private static ProfileRecoveryCoordinator openAuditCoordinator(Fixture fixture) {
        return new ProfileRecoveryCoordinator(fixture.reservations,
                new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5)), fixture.cold);
    }

    @Test
    public void openReplicaAuditKeepsFixedLacPrefixWhileWriterContinuesAndReturnsNoClosedProof() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var handle = prepareOpenReplicaAudit(fixture, sdk, reader, data);
            try {
                fixture.replyFault.set(21); // Actual anchor READ replies, after native I/O.
                var attempt = openAuditCoordinator(fixture).beginVerifyOpenReplicas(44, sdk.getClientCtx(),
                        fixture.credential, data, 8, Duration.ofSeconds(30));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                assertEquals(2, handle.addEntry(bytes(16, 8))); // An audit must not FENCE this writer.
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                var result = attempt.completion().get(10, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.VERIFIED_OPEN_PREFIX, result.status());
                assertEquals(0, result.openCut().prefixEntryId());
                assertEquals(16, result.openCut().prefixLength());
                assertNull(result.proof());
                assertEquals(LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                assertEquals(3, handle.addEntry(bytes(16, 8)));
            } finally {
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void openReplicaAuditReportsCorruptSourceAndRejectsPermissionAndChangingMetadata() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var handle = prepareOpenReplicaAudit(fixture, sdk, reader, data);
            try {
                var coordinator = openAuditCoordinator(fixture);
                fixture.replyFault.set(16); // One native READ payload corrupted in transit, valid scope/framing.
                var corrupt = coordinator.beginVerifyOpenReplicas(44, sdk.getClientCtx(), fixture.credential,
                        data, 8, Duration.ofSeconds(30)).completion().get(10, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.UNDERREPLICATED, corrupt.status());
                assertTrue(corrupt.sources().stream().anyMatch(source -> source.bookie().equals(
                        fixture.identities.get(2).bookieId()) && source.outcome()
                        == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.CORRUPT));
                assertNull(corrupt.proof());
                fixture.replyFault.set(0);
                fixture.readAllowed.set(false);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.CONFLICT,
                        coordinator.beginVerifyOpenReplicas(44, sdk.getClientCtx(), fixture.credential,
                                data, 8, Duration.ofSeconds(30)).completion().get(10, TimeUnit.SECONDS).status());
                fixture.readAllowed.set(true);
                fixture.replyFault.set(21);
                var changing = coordinator.beginVerifyOpenReplicas(44, sdk.getClientCtx(), fixture.credential,
                        data, 8, Duration.ofSeconds(30));
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                var before = fixture.manager.readLedgerMetadata(44).get();
                var custom = new HashMap<>(before.getValue().getCustomMetadata());
                custom.put("open-audit-change", bytes(1, 77));
                fixture.manager.writeLedgerMetadata(44, LedgerMetadataBuilder.from(before.getValue())
                        .withCustomMetadata(custom).build(), before.getVersion()).get(5, TimeUnit.SECONDS);
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.CONFLICT,
                        changing.completion().get(10, TimeUnit.SECONDS).status());
            } finally {
                fixture.readAllowed.set(true);
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void openReplicaAuditPeriodicTaskMarksCorruptionAndKeepsWriterOpen() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var handle = prepareOpenReplicaAudit(fixture, sdk, reader, data);
            var stats = new org.apache.bookkeeper.test.TestStatsProvider();
            var main = replicaAuditMain(fixture, stats.getStatsLogger(""), Duration.ofSeconds(30));
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                startOnlyProfileAuditor(main);
                assertTrue(stats.getCounter("auditor.profile_open_prefix_audit_healthy").get() > 0);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                fixture.replyFault.set(16);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
                while (monitor.getLedgerUnreplicationInfo(44) == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertTrue(monitor.getLedgerUnreplicationInfo(44).getReplicaList().contains("socket2:3181"));
                assertEquals(LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                assertEquals(2, handle.addEntry(bytes(16, 8)));
                fixture.replyFault.set(0);
                fixture.readAllowed.set(false);
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while ((stats.getCounter("auditor.profile_data_audit_deferred") == null
                        || stats.getCounter("auditor.profile_data_audit_deferred").get() == 0)
                        && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertTrue(stats.getCounter("auditor.profile_data_audit_deferred").get() > 0);
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
            } finally {
                fixture.readAllowed.set(true);
                fixture.replyFault.set(0);
                main.shutdown();
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void openReplicaAuditDistinguishesUnfencedUnknownFromDurablyFencedAbsence() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture)) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var acks = new RoundRobinDistributionSchedule(3, 2, 3).getAckSet();
            try {
                for (int node = 0; node < 3; node++) {
                    var peer = dataClient(fixture, node, 8, Duration.ofSeconds(5));
                    data.put(fixture.identities.get(node).bookieId(), peer);
                    if (node < 2) {
                        evidenceEntry(fixture, peer, 0, -1, 16, 16);
                        assertEquals(node == 1, acks.completeBookieAndCheck(node));
                    }
                }
                for (int node = 0; node < 2; node++) {
                    evidenceEntry(fixture, data.get(fixture.identities.get(node).bookieId()), 1, 0, 32, 16);
                }
                var coordinator = openAuditCoordinator(fixture);
                var unknown = coordinator.beginVerifyOpenReplicas(44, sdk.getClientCtx(), fixture.credential,
                        data, 8, Duration.ofSeconds(30)).completion().get(10, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.DEFERRED, unknown.status());
                assertTrue(unknown.sources().stream().anyMatch(source -> source.bookie().equals(
                        fixture.identities.get(2).bookieId()) && source.outcome()
                        == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.UNAVAILABLE));
                assertFalse(unknown.sources().stream().anyMatch(source -> source.outcome()
                        == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.MISSING));
                fixture.clients.get("socket2:3181").fence(fixture.context(), bytes(16, 75), fixture.credential)
                        .get(5, TimeUnit.SECONDS);
                var stable = coordinator.beginVerifyOpenReplicas(44, sdk.getClientCtx(), fixture.credential,
                        data, 8, Duration.ofSeconds(30)).completion().get(10, TimeUnit.SECONDS);
                assertEquals(ProfileRecoveryCoordinator.ReplicaHealthStatus.UNDERREPLICATED, stable.status());
                assertTrue(stable.sources().stream().anyMatch(source -> source.bookie().equals(
                        fixture.identities.get(2).bookieId()) && source.outcome()
                        == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.MISSING));
                assertNull(stable.proof());
                assertEquals(LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
            } finally {
                acks.recycle();
            }
        }
    }

    @Test
    public void openReplicaAuditDeadlineAndShutdownWaitForActualAnchorAndLeaseDrain() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 3, 2, true, true); BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            var handle = prepareOpenReplicaAudit(fixture, sdk, reader, data);
            var main = replicaAuditMain(fixture, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE,
                    Duration.ofMillis(200));
            var clientField = main.getClass().getDeclaredField("bkc");
            clientField.setAccessible(true);
            var ownedSdk = (BookKeeper) clientField.get(main);
            var ownedZk = ((org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver)
                    ownedSdk.getMetadataClientDriver()).getZk();
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                fixture.replyFault.set(21);
                var electorField = main.getClass().getDeclaredField("auditorElector");
                electorField.setAccessible(true);
                ((org.apache.bookkeeper.replication.AuditorElector) electorField.get(main)).start()
                        .get(5, TimeUnit.SECONDS);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fixture.heldReplies.size() < 3 && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertEquals(3, fixture.heldReplies.size());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
                assertEquals(6, main.getProfilePeerUsage().reservedConnections());
                var stopping = stopper.submit(() -> {
                    Thread.currentThread().interrupt();
                    main.shutdown();
                    return Thread.currentThread().isInterrupted();
                });
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertFalse(ownedSdk.getClientCtx().isClientClosed());
                assertEquals(ZooKeeper.States.CONNECTED, ownedZk.getState());
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                assertTrue(stopping.get(10, TimeUnit.SECONDS));
                assertEquals(ZooKeeper.States.CLOSED, ownedZk.getState());
                assertEquals(0, main.getProfilePeerUsage().reservedConnections());
                assertNull(monitor.getLedgerUnreplicationInfo(44));
            } finally {
                fixture.replyFault.set(0);
                fixture.heldReplies.forEach(Runnable::run);
                main.shutdown();
                stopper.shutdownNow();
                handle.stopLocalWrites().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void prepareHealthyReplicaAudit(Fixture fixture, BookKeeper sdk,
            org.apache.zookeeper.ZooKeeper reader) throws Exception {
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
    }

    private static org.apache.bookkeeper.replication.AutoRecoveryMain replicaAuditMain(Fixture fixture,
            org.apache.bookkeeper.stats.StatsLogger stats, Duration deadline) throws Exception {
        var conf = profileWorkerConfiguration(fixture);
        conf.setAllowLoopback(true);
        conf.setLostBookieRecoveryDelay(0);
        conf.setAuditorPeriodicBookieCheckInterval(0);
        conf.setAuditorPeriodicCheckInterval(0);
        conf.setAuditorPeriodicReplicasCheckInterval(1);
        conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                        2 * fixture.identities.size(), 4 * fixture.identities.size(), 2, 1060, Duration.ofSeconds(5)),
                new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        Map.of(), Map.of(), 6, 8, deadline));
        return new org.apache.bookkeeper.replication.AutoRecoveryMain(conf, stats, settings);
    }

    @Test
    public void profilePeriodicReplicaAuditFindsRealHistoricalDataHoleAndWorkerRepairsIt() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true, 5, 2, true, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
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

            for (int node = 0; node < 5; node++) {
                registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            var conf = profileWorkerConfiguration(fixture);
            conf.setAllowLoopback(true);
            conf.setLostBookieRecoveryDelay(0);
            conf.setAuditorPeriodicBookieCheckInterval(0);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(1);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                    "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                    fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                    new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                            10, 20, 2, 1060, Duration.ofSeconds(5)),
                    new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                            Map.of(), Map.of(), 6, 8, Duration.ofSeconds(30)));
            var main = new org.apache.bookkeeper.replication.AutoRecoveryMain(
                    conf, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, settings);
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                startOnlyProfileAuditor(main);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (monitor.getLedgerUnreplicationInfo(44) == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(List.of("socket2:3181"), monitor.getLedgerUnreplicationInfo(44).getReplicaList());
                assertEquals(0, main.getProfilePeerUsage().reservedConnections());
                main.getReplicationWorker().start();
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(fixture.identities.get(4).bookieId(),
                        fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                assertEquals(3, fixture.manager.readLedgerMetadata(44).get().getValue().getLastEntryId());
                assertEquals(64, fixture.manager.readLedgerMetadata(44).get().getValue().getLength());
            } finally {
                main.shutdown();
            }
        }
    }

    @Test
    public void sharedIndexRetainsAllReferencesWhileClassicAuditorOnlyMarksClassicLedgers() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper sdk = sdk(fixture);
                var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var original = fixture.manager.readLedgerMetadata(44).get().getValue();
            fixture.manager.createLedgerMetadata(43, LedgerMetadataBuilder.from(original).withId(43)
                    .withCustomMetadata(Map.of()).build()).get(5, TimeUnit.SECONDS);
            var index = new org.apache.bookkeeper.replication.BookieLedgerIndexer(fixture.manager)
                    .getBookieToLedgerIndex();
            for (int node = 0; node < 3; node++) {
                assertEquals(java.util.Set.of(43L, 44L), index.get("socket" + node + ":3181"));
            }
            // The borrowed-legacy-ZK SDK's Classic cookie registry uses its configured default root.
            if (fixture.zk.exists("/ledgers/cookies", false) == null) {
                fixture.zk.create("/ledgers/cookies", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            var conf = profileWorkerConfiguration(fixture);
            conf.setLostBookieRecoveryDelay(0);
            conf.setAuditorPeriodicBookieCheckInterval(0);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(0);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            try (var auditor = new org.apache.bookkeeper.replication.Auditor("classic-auditor:3181", conf, sdk,
                    false, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE)) {
                auditor.start();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (monitor.getLedgerUnreplicationInfo(43) == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(monitor.getLedgerUnreplicationInfo(43));
                assertNull(monitor.getLedgerUnreplicationInfo(44));
            }
            assertFalse(sdk.getClientCtx().isClientClosed());
        }
    }

    @Test
    public void legacyDecommissionRejectsStillReferencedProfileWithoutMarker() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper sdk = sdk(fixture);
                org.apache.bookkeeper.client.BookKeeperAdmin admin =
                        new org.apache.bookkeeper.client.BookKeeperAdmin(sdk);
                var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            var conf = profileWorkerConfiguration(fixture);
            conf.setLostBookieRecoveryDelay(0);
            conf.setAuditorPeriodicBookieCheckInterval(0);
            conf.setAuditorPeriodicCheckInterval(0);
            conf.setAuditorPeriodicReplicasCheckInterval(0);
            conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
            var elector = new org.apache.bookkeeper.replication.AuditorElector(
                    "legacy-auditor:3181", conf, sdk, false);
            try {
                elector.start().get(5, TimeUnit.SECONDS);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                var rejected = assertThrows(BKException.class,
                        () -> admin.decommissionBookie(fixture.identities.get(2).bookieId()));
                assertEquals(BKException.Code.IllegalOpException, rejected.getCode());
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(fixture.identities.get(2).bookieId(),
                        fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
                assertFalse(sdk.getClientCtx().isClientClosed());
            } finally {
                elector.shutdown();
            }
        }
    }

    @Test
    public void profileAuditorWatchCreatesOriginalTaskAndWorkerRepairsWithoutManualMarker() throws Exception {
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
                fixture.zk.create("/socket-ledgers/available/socket" + node + ":3181", new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            var metadata = new ProfileLedgerMetadataStore(fixture.zk, (AbstractZkLedgerManager) fixture.manager,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            new ProfileRecoveryCoordinator(fixture.reservations, metadata, fixture.cold).recover(
                    44, bytes(16, 31), sdk.getClientCtx(), fixture.descriptorBytes, VALIDATOR, fixture.credential,
                    data, fixture.clients, 3, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            for (int node = 0; node < 4; node++) {
                registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            var main = profileMembershipMain(fixture, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, 0);
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                startOnlyProfileAuditor(main);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                String path = "/socket-readiness/" + java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString("socket2:3181".getBytes(StandardCharsets.UTF_8));
                fixture.zk.delete(path + "/writable", -1);
                fixture.listeners.get(2).close().syncUninterruptibly();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (monitor.getLedgerUnreplicationInfo(44) == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(List.of("socket2:3181"), monitor.getLedgerUnreplicationInfo(44).getReplicaList());
                main.getReplicationWorker().start();
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (monitor.getLedgerUnreplicationInfo(44) != null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(fixture.identities.get(3).bookieId(),
                        fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2));
            } finally {
                main.shutdown();
            }
        }
    }

    @Test
    public void profileAuditorPeriodicUnknownDefersAndClassicIndexKeepsItsOwnLedgers() throws Exception {
        try (Fixture fixture = new Fixture(false, false, true);
                BookKeeper sdk = sdk(fixture);
                var reader = registrationReader(fixture)) {
            fixture.helloReadinessGeneration = 1;
            fixture.connectAll(Duration.ofSeconds(5));
            fixture.initialize().get(5, TimeUnit.SECONDS);
            org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore registry = null;
            for (int node = 0; node < 3; node++) {
                registry = registeredPeer(fixture, reader, node, "tls://127.0.0.1:"
                        + ((InetSocketAddress) fixture.listeners.get(node).localAddress()).getPort(), 1,
                        fixture.identities.get(node).incarnation().value());
            }
            var actual = fixture.manager.readLedgerMetadata(44).get().getValue();
            fixture.manager.createLedgerMetadata(43, LedgerMetadataBuilder.from(actual).withId(43)
                    .withCustomMetadata(Map.of()).build()).get(5, TimeUnit.SECONDS);
            var index = new org.apache.bookkeeper.replication.BookieLedgerIndexer(fixture.manager)
                    .getBookieToLedgerIndex();
            for (int node = 0; node < 3; node++) {
                assertEquals(java.util.Set.of(43L, 44L), index.get("socket" + node + ":3181"));
            }
            var stats = new org.apache.bookkeeper.test.TestStatsProvider();
            var main = profileMembershipMain(fixture, stats.getStatsLogger(""), 1);
            try (var monitor = sdk.getLedgerManagerFactory().newLedgerUnderreplicationManager()) {
                startOnlyProfileAuditor(main);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                assertNull(monitor.getLedgerUnreplicationInfo(43));
                String path = "/socket-readiness/" + java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString("socket2:3181".getBytes(StandardCharsets.UTF_8)) + "/service-info";
                byte[] previous = fixture.zk.getData(path, false, null);
                fixture.zk.setData(path, new byte[]{1}, -1);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while ((stats.getCounter("auditor.profile_audit_deferred") == null
                        || stats.getCounter("auditor.profile_audit_deferred").get() == 0)
                        && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(stats.getCounter("auditor.profile_audit_deferred"));
                assertTrue(stats.getCounter("auditor.profile_audit_deferred").get() > 0);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                assertNull(monitor.getLedgerUnreplicationInfo(43));
                assertEquals(org.apache.bookkeeper.client.api.LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());
                long runs = stats.getCounter("auditor.profile_audit_runs").get();
                fixture.zk.setData(path, previous, -1);
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (stats.getCounter("auditor.profile_audit_runs").get() <= runs && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertTrue(stats.getCounter("auditor.profile_audit_runs").get() > runs);
                assertNull(monitor.getLedgerUnreplicationInfo(44));
                String readiness = path.substring(0, path.lastIndexOf('/'));
                for (String child : fixture.zk.getChildren(readiness, false)) {
                    if (child.equals("service-info") || child.equals("writable") || child.startsWith("session-")) {
                        fixture.zk.delete(readiness + "/" + child, -1);
                    }
                }
                var before = registry.read("socket2:3181").versionedRecord().orElseThrow();
                var old = before.record();
                var changed = new org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord(
                        old.bookieId(), new org.apache.bookkeeper.common.profile.startup.StorageIncarnation(
                                bytes(16, 78)), old.engine(), old.protocolMajor(), old.protocolMinor(),
                        old.capabilityGeneration(), old.localFormatGeneration(), old.deviceManifestIdentity(),
                        old.deviceManifestGeneration(), old.effectiveAssignmentGeneration(),
                        new org.apache.bookkeeper.common.profile.startup.Generation(2),
                        old.minimumReaderVersion(), old.minimumWriterVersion(), old.capabilityHints(), old.state());
                assertEquals(org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus.APPLIED,
                        registry.compareAndSet("socket2:3181", before.storeVersion(), 1, changed).status());
                var ready = registry.read("socket2:3181").versionedRecord().orElseThrow();
                var projection = new org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection(
                        old.bookieId(), changed.storageIncarnation(), changed.localReadinessGeneration(),
                        ready.storeVersion(), "tls://127.0.0.1:"
                            + ((InetSocketAddress) fixture.listeners.get(2).localAddress()).getPort(),
                        changed.capabilityHints());
                var publisher = new org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore(
                        fixture.zk, "/socket-readiness", old.bookieId(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        Duration.ofSeconds(5));
                publisher.publishServiceInfo(projection);
                publisher.registerWritable(new org.apache.bookkeeper.common.profile.startup
                        .EphemeralProfileRegistrationStore.WritableRegistration(old.bookieId(),
                                changed.storageIncarnation(), changed.localReadinessGeneration(),
                                ready.storeVersion()));
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (monitor.getLedgerUnreplicationInfo(44) == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(monitor.getLedgerUnreplicationInfo(44));
                assertEquals(List.of("socket2:3181"), monitor.getLedgerUnreplicationInfo(44).getReplicaList());
                assertEquals(org.apache.bookkeeper.client.api.LedgerMetadata.State.OPEN,
                        fixture.manager.readLedgerMetadata(44).get().getValue().getState());

            } finally {
                main.shutdown();
            }
        }
    }

    private static org.apache.bookkeeper.replication.AutoRecoveryMain profileMembershipMain(
            Fixture fixture, org.apache.bookkeeper.stats.StatsLogger stats, int interval) throws Exception {
        var conf = profileWorkerConfiguration(fixture);
        conf.setAllowLoopback(true);
        conf.setLostBookieRecoveryDelay(0);
        conf.setAuditorPeriodicBookieCheckInterval(interval);
        conf.setAuditorPeriodicCheckInterval(0);
        conf.setAuditorPeriodicReplicasCheckInterval(0);
        conf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        var settings = new org.apache.bookkeeper.replication.AutoRecoveryMain.ProfileSettings(
                "/socket-initial", "/socket-readiness", ZooDefs.Ids.OPEN_ACL_UNSAFE, fixture.peerTls,
                fixture.identities.stream().map(identity -> identity.bookieId()).toList(),
                new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits(
                        2 * fixture.identities.size(), 4 * fixture.identities.size(), 2, 1060, Duration.ofSeconds(5)),
                new ProfileRecoveryCoordinator.WorkerOptions(fixture.descriptorBytes, VALIDATOR,
                        Map.of(), Map.of(), 3, 8, Duration.ofSeconds(30)));
        return new org.apache.bookkeeper.replication.AutoRecoveryMain(conf, stats, settings);
    }

    private static void startOnlyProfileAuditor(org.apache.bookkeeper.replication.AutoRecoveryMain main)
            throws Exception {
        var electorField = main.getClass().getDeclaredField("auditorElector");
        electorField.setAccessible(true);
        ((org.apache.bookkeeper.replication.AuditorElector) electorField.get(main)).start()
                .get(5, TimeUnit.SECONDS);
        var executorField = main.getAuditor().getClass().getDeclaredField("executor");
        executorField.setAccessible(true);
        ((java.util.concurrent.ScheduledExecutorService) executorField.get(main.getAuditor()))
                .submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    @Test
    public void profileAuditorStartupFindsMissingProfilePublisherDespiteClassicRegistration() throws Exception {
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
                String readiness = "/socket-readiness/" + java.util.Base64.getUrlEncoder().withoutPadding()
                        .encodeToString("socket2:3181".getBytes(StandardCharsets.UTF_8));
                fixture.zk.delete(readiness + "/writable", -1);
                for (int node = 0; node < 3; node++) {
                    fixture.zk.create("/socket-ledgers/available/socket" + node + ":3181", new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }
                assertNull(monitor.getLedgerUnreplicationInfo(44));
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
                conf.setLostBookieRecoveryDelay(0);
                conf.setAuditorPeriodicBookieCheckInterval(0);
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
                    while ((monitor.getLedgerUnreplicationInfo(44) != null
                            || !fixture.manager.readLedgerMetadata(44).get().getValue().getAllEnsembles().get(0L).get(2)
                                .equals(fixture.identities.get(3).bookieId())) && System.nanoTime() < until) {
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
    public void profileShutdownInterruptDuringAuditorWaitKeepsItsActualScanAndSdkAlive() throws Exception {
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
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var owner = new java.util.concurrent.atomic.AtomicReference<Thread>();
            var stopper = java.util.concurrent.Executors.newSingleThreadExecutor();
            org.apache.bookkeeper.replication.Auditor auditor = null;
            try {
                main.start();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (main.getAuditor() == null && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                auditor = main.getAuditor();
                assertNotNull(auditor);
                var executorField = auditor.getClass().getDeclaredField("executor");
                executorField.setAccessible(true);
                var executor = (java.util.concurrent.ScheduledExecutorService) executorField.get(auditor);
                var scan = executor.submit(() -> {
                    entered.countDown();
                    await(release);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                var stopping = stopper.submit(() -> {
                    owner.set(Thread.currentThread());
                    main.shutdown();
                    return Thread.currentThread().isInterrupted();
                });
                until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while ((owner.get() == null || java.util.Arrays.stream(owner.get().getStackTrace())
                        .noneMatch(frame -> frame.getClassName().equals(auditorClassName())
                            && frame.getMethodName().equals("shutdown"))) && System.nanoTime() < until) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertNotNull(owner.get());
                assertTrue(java.util.Arrays.stream(owner.get().getStackTrace()).anyMatch(frame ->
                        frame.getClassName().equals(auditorClassName()) && frame.getMethodName().equals("shutdown")));
                owner.get().interrupt();
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> stopping.get(150, TimeUnit.MILLISECONDS));
                assertFalse(scan.isDone());
                assertFalse(ownedSdk.getClientCtx().isClientClosed());
                assertEquals(ZooKeeper.States.CONNECTED, ownedZk.getState());
                release.countDown();
                assertTrue(stopping.get(10, TimeUnit.SECONDS));
                assertTrue(scan.isDone());
                assertTrue(executor.isTerminated());
                assertEquals(ZooKeeper.States.CLOSED, ownedZk.getState());
                assertFalse(externalSdk.getClientCtx().isClientClosed());
            } finally {
                release.countDown();
                main.shutdown();
                if (auditor != null) {
                    auditor.close(); // Also reclaim the RED run's abandoned auditor resources.
                }
                stopper.shutdownNow();
            }
        }
    }

    private static String auditorClassName() {
        return org.apache.bookkeeper.replication.Auditor.class.getName();
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

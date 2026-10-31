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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.Watchable;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.discover.profile.ZKBookieRouteBindingStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.apache.bookkeeper.util.PortManager;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Package storage and real SDK/network fixtures; no production START or qualified child. */
public class ClassicChildResourcesTest {
    private static final String ROOT = "/classic-child-resources";
    private static final BookieId SELF = BookieId.parse("history-resource:3181");
    private static final byte[] KEY = new byte[]{1, 2, 3};
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static ByteBuf entry(long id) {
        return Unpooled.buffer(32).writeLong(7).writeLong(id).writeLong(id).writeLong(900 + id);
    }

    @Test(timeout = 30000)
    public void existingHistoryReopensAppendsAndRestartsWithoutChangingCookieOrOwningMetadata() throws Exception {
        try (var f = new Fixture()) {
            Path version = f.conf.getLedgerDirs()[0].toPath().resolve("current/VERSION");
            byte[] original = Files.readAllBytes(version);
            Object inode = Files.getAttribute(version, "unix:ino");
            var first = f.open();
            ByteBuf read = first.bookieForIoTests().readEntry(7, 0);
            try {
                assertEquals(900, read.getLong(24));
            } finally {
                read.release();
            }
            var written = new CompletableFuture<Integer>();
            ByteBuf input = entry(1);
            first.bookieForIoTests().addEntry(input, false, (rc, ledger, id, address, ignored) -> {
                assertThrows(IllegalStateException.class, first::closeAndDrain);
                written.complete(rc);
            }, null, KEY);
            assertEquals(0, (int) written.get(5, TimeUnit.SECONDS));
            assertEquals(ClassicChildResources.State.DRAINED, first.closeAndDrain().state());
            assertEquals(0, input.refCnt());
            assertNotNull(f.manager.readLedgerMetadata(7).get(5, TimeUnit.SECONDS));
            assertFalse(f.replication.isLedgerBeingReplicated(7));
            assertEquals(inode, Files.getAttribute(version, "unix:ino"));
            assertArrayEquals(original, Files.readAllBytes(version));

            var restarted = f.open();
            ByteBuf recovered = restarted.bookieForIoTests().readEntry(7, 1);
            try {
                assertEquals(901, recovered.getLong(24));
            } finally {
                recovered.release();
            }
            assertEquals(ClassicChildResources.State.DRAINED, restarted.closeAndDrain().state());
            assertArrayEquals(original, Files.readAllBytes(version));
        }
    }

    @Test(timeout = 30000)
    public void missingOrWrongExistingCookieRejectsWithoutCreatingOrRepairingHistory() throws Exception {
        try (var f = new Fixture()) {
            Path version = f.conf.getJournalDirs()[0].toPath().resolve("current/VERSION");
            Files.delete(version);
            var missing = ClassicChildResources.prepare();
            f.resources.add(missing);
            assertThrows(IOException.class,
                    () -> missing.initializeForIoTests(f.conf, f.registration, f.manager, f.replication));
            assertFalse(Files.exists(version));
            assertEquals(ClassicChildResources.State.DRAINED, missing.closeAndDrain().state());
            Files.writeString(version, "not a cookie", StandardCharsets.UTF_8);
            var malformed = ClassicChildResources.prepare();
            f.resources.add(malformed);
            assertThrows(Exception.class,
                    () -> malformed.initializeForIoTests(f.conf, f.registration, f.manager, f.replication));
            assertEquals("not a cookie", Files.readString(version));
            assertEquals(ClassicChildResources.State.DRAINED, malformed.closeAndDrain().state());
        }
    }

    @Test(timeout = 30000)
    public void freshStillRejectsTheHistoricalRootsAndCannotBorrowTheNewResourcePermission() throws Exception {
        try (var f = new Fixture()) {
            var fresh = FreshClassicOwner.create(f.conf);
            assertThrows(IOException.class, fresh::start);
            assertTrue(fresh.closeAndAwait(Duration.ofSeconds(5)).drained());
            assertEquals(FreshClassicOwner.State.DRAINED, fresh.status().state());
            var historical = f.open();
            assertEquals(ClassicChildResources.State.STORAGE_STARTED, historical.status().state());
            assertThrows(IOException.class,
                    () -> historical.initializeForIoTests(f.conf, f.registration, f.manager, f.replication));
        }
    }

    @Test(timeout = 30000)
    public void historicalModeChangesCannotRegisterBeforeNetworkAdmission() throws Exception {
        try (var f = new Fixture()) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var resource = f.open();
            var state = ((BookieImpl) resource.bookieForIoTests()).getStateManager();
            assertFalse(f.registration.isBookieRegistered(SELF));
            state.transitionToReadOnlyMode().get(5, TimeUnit.SECONDS);
            assertTrue(state.isReadOnly());
            assertFalse("A storage-only assembly must not publish a service registration",
                    f.registration.isBookieRegistered(SELF));
            state.transitionToWritableMode().get(5, TimeUnit.SECONDS);
            assertFalse(state.isReadOnly());
            assertFalse(f.registration.isBookieRegistered(SELF));
            assertThrows(Exception.class, () -> state.registerBookie(true).get(5, TimeUnit.SECONDS));
            assertFalse(f.registration.isBookieRegistered(SELF));
        }
    }

    @Test(timeout = 60000)
    public void registeredModeChangeStopsTheEpisodeAndLeavesItsOriginalNodeForSessionClose() throws Exception {
        for (boolean readOnly : new boolean[]{false, true}) {
            try (var f = new Fixture()) {
                f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                var resource = f.open();
                var state = ((BookieImpl) resource.bookieForIoTests()).getStateManager();
                if (readOnly) {
                    state.transitionToReadOnlyMode().get(5, TimeUnit.SECONDS);
                }
                // Direct package component exercise, not a TCP bind, START or qualified READY.
                state.registerAfterBindForClassicChild().get(5, TimeUnit.SECONDS);
                state.requireCurrentRegistrationForClassicChild();
                String path = ROOT + "/available/" + (readOnly ? "readonly/" : "") + SELF;
                String other = ROOT + "/available/" + (readOnly ? "" : "readonly/") + SELF;
                var original = f.zk.getZooKeeperClient().exists(path, false);
                assertNotNull(original);
                assertEquals(f.metadataClient.getSessionId(), original.getEphemeralOwner());
                assertThrows(IOException.class, state::registerAfterBindForClassicChild);
                if (readOnly) {
                    state.transitionToReadOnlyMode().get(5, TimeUnit.SECONDS);
                } else {
                    state.transitionToWritableMode().get(5, TimeUnit.SECONDS);
                }
                assertFalse(resource.isClosing());
                state.requireCurrentRegistrationForClassicChild();
                if (readOnly) {
                    state.transitionToWritableMode().get(5, TimeUnit.SECONDS);
                } else {
                    state.transitionToReadOnlyMode().get(5, TimeUnit.SECONDS);
                }
                assertTrue(resource.isClosing());
                assertNotNull(resource.status().operationFailure());
                assertFalse(state.isRegistered());
                assertThrows(IOException.class, state::requireCurrentRegistrationForClassicChild);
                var remaining = f.zk.getZooKeeperClient().exists(path, false);
                assertEquals(original.getCzxid(), remaining.getCzxid());
                assertEquals(original.getEphemeralOwner(), remaining.getEphemeralOwner());
                assertEquals(null, f.zk.getZooKeeperClient().exists(other, false));
                assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
                assertNotNull(resource.status().operationFailure());
                assertNotNull(f.zk.getZooKeeperClient().exists(path, false));
            }
        }
    }

    @Test(timeout = 60000)
    public void actualExpiryBeforeOrAfterRegistrationStopsAndNeverDeletesAnotherSessionNode() throws Exception {
        for (boolean published : new boolean[]{false, true}) {
            try (var f = new Fixture();
                 var other = ZooKeeperClient.newBuilder().connectString(f.zk.getZooKeeperConnectString())
                         .sessionTimeoutMs(10000).build()) {
                f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                var resource = f.open();
                var bookie = (BookieImpl) resource.bookieForIoTests();
                var state = bookie.getStateManager();
                if (published) {
                    state.registerAfterBindForClassicChild().get(5, TimeUnit.SECONDS);
                    resource.requestClose();
                    assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
                    assertEquals(null, resource.status().operationFailure());
                }
                f.zk.expireSession(f.metadataClient);
                Awaitility.await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> resource.status().operationFailure() != null);
                assertTrue(resource.status().operationFailure().getMessage().contains("SESSION_EXPIRED"));
                assertThrows(IOException.class, state::registerAfterBindForClassicChild);
                assertThrows(IOException.class, state::requireCurrentRegistrationForClassicChild);
                assertThrows(java.util.concurrent.RejectedExecutionException.class, () -> bookie.readEntry(7, 0));
                String path = ROOT + "/available/" + SELF;
                Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> other.exists(path, false) == null);
                other.create(path, new byte[]{42}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                var replacement = other.exists(path, false);
                assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
                var remaining = other.exists(path, false);
                assertNotNull(remaining);
                assertEquals(replacement.getCzxid(), remaining.getCzxid());
                assertEquals(other.getSessionId(), remaining.getEphemeralOwner());
                assertTrue(resource.status().operationFailure().getMessage().contains("SESSION_EXPIRED"));
            }
        }
    }

    @Test(timeout = 30000)
    public void retryableMetadataClientCannotQualifyTheControlledRegistrationPath() throws Exception {
        try (var f = new Fixture()) {
            var retryable = new ZKRegistrationManager(f.conf, f.zk.getZooKeeperClient(), ROOT);
            assertThrows(IOException.class, () -> retryable.addClassicChildRegistrationListener(() -> { }));
            var unqualified = ClassicChildResources.prepare();
            f.resources.add(unqualified);
            assertThrows(IOException.class,
                    () -> unqualified.initializeForIoTests(f.conf, retryable, f.manager, f.replication));
            assertFalse(retryable.isBookieRegistered(SELF));
            assertThrows(IllegalStateException.class,
                    () -> f.driver.initialize(f.conf, NullStatsLogger.INSTANCE));
            assertThrows(IllegalStateException.class, f.driver::createRegistrationManager);
            var driver = new ZKMetadataBookieDriver();
            assertThrows(IllegalStateException.class,
                    () -> driver.initializeForClassicChild(f.conf, NullStatsLogger.INSTANCE,
                            f.zk.getZooKeeperClient()));
            assertEquals(null, f.zk.getZooKeeperClient().exists(ROOT + "/available", false));
        }
    }

    @Test(timeout = 30000)
    public void lostCreateReturnNeverRetriesOrAdoptsTheLaterVisibleRegistration() throws Exception {
        try (var f = new Fixture(true)) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var resource = f.open();
            var state = ((BookieImpl) resource.bookieForIoTests()).getStateManager();
            String path = ROOT + "/available/" + SELF;
            AtomicInteger creates = new AtomicInteger();
            doAnswer(invocation -> {
                creates.incrementAndGet();
                invocation.callRealMethod();
                throw new KeeperException.ConnectionLossException();
            }).when(f.metadataClient).create(org.mockito.ArgumentMatchers.eq(path),
                    org.mockito.ArgumentMatchers.any(byte[].class), org.mockito.ArgumentMatchers.anyList(),
                    org.mockito.ArgumentMatchers.eq(CreateMode.EPHEMERAL));
            assertThrows(Exception.class, () -> state.registerAfterBindForClassicChild().get(5, TimeUnit.SECONDS));
            assertTrue(resource.isClosing());
            assertFalse(state.isRegistered());
            assertNotNull(f.zk.getZooKeeperClient().exists(path, false));
            assertThrows(IOException.class, state::registerAfterBindForClassicChild);
            assertThrows(IOException.class, state::requireCurrentRegistrationForClassicChild);
            assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
            assertNotNull(resource.status().operationFailure());
            assertEquals(1, creates.get());
            assertNotNull(f.zk.getZooKeeperClient().exists(path, false));
        }
    }

    @Test(timeout = 60000)
    public void heldActualCreateReturnCannotOutrunModeRetirementOrActualExpiryAndDrain() throws Exception {
        for (boolean expired : new boolean[]{false, true}) {
            try (var f = new Fixture(true)) {
                f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                var resource = f.open();
                var state = ((BookieImpl) resource.bookieForIoTests()).getStateManager();
                String path = ROOT + "/available/" + SELF;
                long originalSession = f.metadataClient.getSessionId();
                CountDownLatch created = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                AtomicInteger creates = new AtomicInteger();
                var worker = Executors.newSingleThreadExecutor();
                try {
                    doAnswer(invocation -> {
                        creates.incrementAndGet();
                        Object result = invocation.callRealMethod();
                        created.countDown();
                        release.await();
                        return result;
                    }).when(f.metadataClient).create(org.mockito.ArgumentMatchers.eq(path),
                            org.mockito.ArgumentMatchers.any(byte[].class), org.mockito.ArgumentMatchers.anyList(),
                            org.mockito.ArgumentMatchers.eq(CreateMode.EPHEMERAL));
                    var registering = state.registerAfterBindForClassicChild();
                    assertTrue(created.await(5, TimeUnit.SECONDS));
                    if (expired) {
                        f.zk.expireSession(f.metadataClient);
                        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(resource::isClosing);
                        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                                .until(() -> f.zk.getZooKeeperClient().exists(path, false) == null);
                        f.zk.getZooKeeperClient().create(path, new byte[]{42}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL);
                    } else {
                        state.transitionToReadOnlyMode().get(5, TimeUnit.SECONDS);
                        assertTrue(resource.isClosing());
                    }
                    var retained = f.zk.getZooKeeperClient().exists(path, false);
                    var closing = worker.submit(resource::closeAndDrain);
                    assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
                    assertEquals(ClassicChildResources.State.CLOSING, resource.status().state());
                    release.countDown();
                    assertThrows(Exception.class, () -> registering.get(5, TimeUnit.SECONDS));
                    assertEquals(ClassicChildResources.State.DRAINED, closing.get(5, TimeUnit.SECONDS).state());
                    assertFalse(state.isRegistered());
                    assertNotNull(resource.status().operationFailure());
                    assertEquals(originalSession, f.metadataClient.getSessionId());
                    assertEquals(1, creates.get());
                    var remaining = f.zk.getZooKeeperClient().exists(path, false);
                    assertEquals(retained.getCzxid(), remaining.getCzxid());
                    assertEquals(retained.getEphemeralOwner(), remaining.getEphemeralOwner());
                    assertThrows(IOException.class, state::registerAfterBindForClassicChild);
                } finally {
                    release.countDown();
                    worker.shutdown();
                    assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test(timeout = 60000)
    public void controlledV3SdkWritesReadsAndRestartsWithANewMetadataEpisode() throws Exception {
        sdkWriteReadRestart(false);
    }

    @Test(timeout = 30000)
    public void actualCallbackFailureRemainsAnEpisodeFailureAfterTheResourcesReallyDrain() throws Exception {
        try (var f = new Fixture()) {
            var resource = f.open();
            var observed = new CountDownLatch(1);
            var failure = new IllegalStateException("Original callback failed after its actual journal completion");
            ByteBuf input = entry(1);
            resource.bookieForIoTests().addEntry(input, false, (rc, ledger, id, address, context) -> {
                observed.countDown();
                throw failure;
            }, null, KEY);
            assertTrue(observed.await(5, TimeUnit.SECONDS));
            var result = resource.closeAndDrain();
            assertEquals(ClassicChildResources.State.DRAINED, result.state());
            assertSame(failure, result.operationFailure());
            assertEquals(0, input.refCnt());
        }
    }

    @Test(timeout = 60000)
    public void controlledV2SdkWritesReadsAndRestartsWithANewMetadataEpisode() throws Exception {
        sdkWriteReadRestart(true);
    }

    @Test(timeout = 30000)
    public void finalSourcesRequireThisActualRegistrationSessionBirthAndMode() throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var store = new ZKBookieRouteBindingStore(f.metadataClient, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            var sources = store.borrowClassicLaunchSources(SELF, Duration.ofSeconds(5));
            var absenceOnly = store.borrowClassicLaunchSources(SELF, Duration.ofSeconds(5));
            var wrongBirth = store.borrowClassicLaunchSources(SELF, Duration.ofSeconds(5));
            try {
                var original = sources.read();
                var resource = f.open();
                resource.startNetworkForIoTests();
                var episode = resource.requireServing();
                assertEquals(f.metadataClient.getSessionId(), episode.session());
                var finalSource = sources.readForServing(episode.session(), episode.birth(), episode.readOnly());
                assertEquals(original.clusterBirthZxid(), finalSource.clusterBirthZxid());
                assertArrayEquals(original.cookieBytes(), finalSource.cookieBytes());
                assertEquals(original.cookieIdentity().birthZxid(), finalSource.cookieIdentity().birthZxid());
                assertThrows(IOException.class, absenceOnly::read);
                assertThrows(IOException.class,
                        () -> sources.readForServing(episode.session() + 1, episode.birth(), episode.readOnly()));
                assertThrows(IOException.class,
                        () -> wrongBirth.readForServing(episode.session(), episode.birth() + 1, episode.readOnly()));
            } finally {
                for (var owner : List.of(sources, absenceOnly, wrongBirth)) {
                    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> owner.closeAndDrain().drained());
                }
            }
        }
    }

    @Test(timeout = 30000)
    public void controlledDriverNeverProvisionsMissingLayoutAndRetainsItsFirstFactoryCloseFailure() throws Exception {
        try (var f = new Fixture()) {
            f.zk.getZooKeeperClient().delete(ROOT + "/LAYOUT", -1);
            var empty = new ZKMetadataBookieDriver();
            empty.initializeForClassicChild(f.conf, NullStatsLogger.INSTANCE, f.metadataClient);
            assertThrows(Exception.class, empty::getLedgerManagerFactory);
            assertEquals(null, f.zk.getZooKeeperClient().exists(ROOT + "/LAYOUT", false));
            empty.closeForClassicChild();
            var original = spy(f.factory);
            var closes = new AtomicInteger();
            var failure = new IOException("Actual original factory close return was lost");
            doAnswer(invocation -> {
                closes.incrementAndGet();
                invocation.callRealMethod();
                f.factoryClosed = true; // Fixture knows its actual cleanup; the driver never observed this result.
                throw failure;
            }).when(original).close();
            var field = org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase.class.getDeclaredField("lmFactory");
            field.setAccessible(true);
            field.set(f.driver, original);
            assertSame(failure, assertThrows(IOException.class, f.driver::closeForClassicChild));
            assertSame(failure, assertThrows(IOException.class, f.driver::closeForClassicChild));
            assertEquals(1, closes.get());
            assertThrows(IllegalStateException.class, f.driver::getLedgerManagerFactory);
            assertTrue(f.metadataClient.getState().isAlive());
        }
    }

    @Test(timeout = 30000)
    public void controlledFactorySelectionPreservesOnlyExistingAliasesAndExactBuiltinClasses() throws Exception {
        try (var f = new Fixture()) {
            String prefix = "org.apache.bookkeeper.meta.";
            for (String[] choice : List.of(new String[]{"1\nflat:1", "FlatLedgerManagerFactory"},
                    new String[]{"1\nhierarchical:1", "HierarchicalLedgerManagerFactory"},
                    new String[]{"2\n" + prefix + "FlatLedgerManagerFactory:1", "FlatLedgerManagerFactory"},
                    new String[]{"2\n" + prefix + "HierarchicalLedgerManagerFactory:1",
                            "HierarchicalLedgerManagerFactory"},
                    new String[]{"2\n" + prefix + "LegacyHierarchicalLedgerManagerFactory:1",
                            "LegacyHierarchicalLedgerManagerFactory"},
                    new String[]{"2\n" + prefix + "LongHierarchicalLedgerManagerFactory:1",
                            "LongHierarchicalLedgerManagerFactory"})) {
                f.zk.getZooKeeperClient().setData(ROOT + "/LAYOUT", choice[0].getBytes(StandardCharsets.UTF_8), -1);
                var conf = new ServerConfiguration(f.conf)
                        .setMetadataServiceUri(f.zk.getMetadataServiceUri(ROOT, "null"));
                var candidate = new ZKMetadataBookieDriver();
                candidate.initializeForClassicChild(conf, NullStatsLogger.INSTANCE, f.metadataClient);
                try {
                    assertEquals(prefix + choice[1], candidate.getLedgerManagerFactory().getClass().getName());
                } finally {
                    candidate.closeForClassicChild();
                }
                assertArrayEquals(choice[0].getBytes(StandardCharsets.UTF_8),
                        f.zk.getZooKeeperClient().getData(ROOT + "/LAYOUT", false, null));
            }
        }
    }

    @Test(timeout = 30000)
    public void customFactoryAndConflictingSelectorsRejectBeforeTheirInitializerAndNeverChangeLayout()
            throws Exception {
        String key = "bookkeeper.test.classic.customFactory.initialized";
        String before = System.getProperty(key);
        System.clearProperty(key);
        try (var f = new Fixture()) {
            String custom = ClassicChildResourcesTest.class.getName() + "$UnsupportedFactory";
            byte[] original = f.zk.getZooKeeperClient().getData(ROOT + "/LAYOUT", false, null);
            for (String[] setting : List.of(new String[]{"ledgerManagerFactoryClass", custom},
                    new String[]{"ledgerManagerFactoryClass", "org.apache.bookkeeper.meta.FlatLedgerManagerFactory"},
                    new String[]{"ledgerManagerType", "flat"},
                    new String[]{"ledgerManagerFactoryDisableClassCheck", "false"},
                    new String[]{"metadataServiceUri", "zk+legacyhierarchical://127.0.0.1:1" + ROOT})) {
                var conf = new ServerConfiguration(f.conf);
                conf.setProperty(setting[0], setting[1]);
                var candidate = new ZKMetadataBookieDriver();
                assertThrows(IOException.class,
                        () -> candidate.initializeForClassicChild(conf, NullStatsLogger.INSTANCE, f.metadataClient));
                candidate.closeForClassicChild();
                assertEquals(null, System.getProperty(key));
                assertArrayEquals(original, f.zk.getZooKeeperClient().getData(ROOT + "/LAYOUT", false, null));
            }
            for (String layout : List.of("1\nlegacyhierarchical:1", "1\nlonghierarchical:1",
                    "2\n" + custom + ":1", "2\norg.apache.bookkeeper.meta.FlatLedgerManagerFactory:2")) {
                f.zk.getZooKeeperClient().setData(ROOT + "/LAYOUT", layout.getBytes(StandardCharsets.UTF_8), -1);
                var candidate = new ZKMetadataBookieDriver();
                candidate.initializeForClassicChild(new ServerConfiguration(f.conf)
                        .setMetadataServiceUri(f.zk.getMetadataServiceUri(ROOT, "null")),
                        NullStatsLogger.INSTANCE, f.metadataClient);
                assertThrows(Exception.class, candidate::getLedgerManagerFactory);
                candidate.closeForClassicChild();
                assertEquals(null, System.getProperty(key));
                assertArrayEquals(layout.getBytes(StandardCharsets.UTF_8),
                        f.zk.getZooKeeperClient().getData(ROOT + "/LAYOUT", false, null));
            }
            f.zk.getZooKeeperClient().setData(ROOT + "/LAYOUT", original, -1);
            var changed = new ZKMetadataBookieDriver();
            changed.initializeForClassicChild(f.conf, NullStatsLogger.INSTANCE, f.metadataClient);
            changed.getLayoutManager().readLedgerLayout();
            byte[] other = "2\norg.apache.bookkeeper.meta.FlatLedgerManagerFactory:1".getBytes(StandardCharsets.UTF_8);
            f.zk.getZooKeeperClient().setData(ROOT + "/LAYOUT", other, -1);
            assertThrows(Exception.class, changed::getLedgerManagerFactory);
            changed.closeForClassicChild();
            assertArrayEquals(other, f.zk.getZooKeeperClient().getData(ROOT + "/LAYOUT", false, null));
        } finally {
            if (before == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, before);
            }
        }
    }

    public static final class UnsupportedFactory extends org.apache.bookkeeper.meta.FlatLedgerManagerFactory {
        static {
            System.setProperty("bookkeeper.test.classic.customFactory.initialized", "unwanted");
        }
    }

    private void sdkWriteReadRestart(boolean v2) throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var first = f.open();
            first.startNetworkForIoTests();
            assertEquals(ClassicChildResources.State.SERVING, first.status().state());
            assertTrue(f.registration.isBookieRegistered(SELF));
            var client = new ClientConfiguration().setMetadataServiceUri(f.conf.getMetadataServiceUri())
                    .setUseV2WireProtocol(v2).setNumIOThreads(1).setNumWorkerThreads(1)
                    .setReadTimeout(3).setAddEntryTimeout(3);
            byte[] password = "controlled-history".getBytes(StandardCharsets.UTF_8);
            byte[] payload = "real controlled Classic SDK payload".getBytes(StandardCharsets.UTF_8);
            long ledger;
            try (var sdk = new BookKeeper(client);
                 var handle = sdk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, password)) {
                ledger = handle.getId();
                assertEquals(0, handle.addEntry(payload));
                assertArrayEquals(payload, handle.readEntries(0, 0).nextElement().getEntry());
            }
            long oldSession = f.metadataClient.getSessionId();
            assertEquals(ClassicChildResources.State.DRAINED, first.closeAndDrain().state());
            assertEquals(null, first.status().operationFailure());
            f.restartMetadata();
            assertFalse(oldSession == f.metadataClient.getSessionId());
            assertFalse(f.registration.isBookieRegistered(SELF));
            var reopened = f.open();
            reopened.startNetworkForIoTests();
            try (var sdk = new BookKeeper(client);
                 var reader = sdk.openLedger(ledger, BookKeeper.DigestType.CRC32C, password)) {
                assertArrayEquals(payload, reader.readEntries(0, 0).nextElement().getEntry());
            }
            assertEquals(ClassicChildResources.State.DRAINED, reopened.closeAndDrain().state());
            assertEquals(null, reopened.status().operationFailure());
        }
    }

    @Test(timeout = 60000)
    public void sdkOutputHeldBeforeNativeSubmissionMustReleaseAfterItsChannelCloses() throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var resource = f.open();
            resource.startNetworkForIoTests();
            var client = new ClientConfiguration().setMetadataServiceUri(f.conf.getMetadataServiceUri())
                    .setNumIOThreads(1).setNumWorkerThreads(1).setNumChannelsPerBookie(1)
                    .setReadTimeout(3).setAddEntryTimeout(3);
            try (var sdk = new BookKeeper(client);
                 var handle = sdk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, new byte[0])) {
                assertEquals(0, handle.addEntry(new byte[]{1, 2, 3}));
                var listenerField = ClassicChildResources.class.getDeclaredField("listener");
                listenerField.setAccessible(true);
                Object server = listenerField.get(resource);
                var networkField = server.getClass().getDeclaredField("nettyServer");
                networkField.setAccessible(true);
                Object network = networkField.get(server);
                var channelsField = network.getClass().getDeclaredField("allChannels");
                channelsField.setAccessible(true);
                Channel channel = ((ChannelGroup) channelsField.get(network)).stream()
                        .filter(candidate -> candidate instanceof SocketChannel).findFirst().orElseThrow();
                var held = new CompletableFuture<Object>();
                var promise = new CompletableFuture<ChannelPromise>();
                var closingWorker = Executors.newSingleThreadExecutor();
                try {
                    channel.eventLoop().submit(() -> channel.pipeline().addAfter("classic-owned-output",
                            "hold-before-native", new ChannelOutboundHandlerAdapter() {
                                @Override
                                public void write(ChannelHandlerContext context, Object message,
                                                  ChannelPromise actual) {
                                    promise.complete(actual);
                                    held.complete(message);
                                }
                            })).get(5, TimeUnit.SECONDS);
                    var read = new CompletableFuture<Integer>();
                    handle.asyncReadEntries(0, 0, (rc, ledger, entries, context) -> read.complete(rc), null);
                    Object original = held.get(5, TimeUnit.SECONDS);
                    ChannelPromise actual = promise.get(5, TimeUnit.SECONDS);
                    assertFalse(actual.cancel(false));
                    var closed = closingWorker.submit(resource::closeAndDrain);
                    channel.closeFuture().get(5, TimeUnit.SECONDS);
                    assertThrows(TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
                    assertEquals(ClassicChildResources.State.CLOSING, resource.status().state());
                    assertEquals(1, ReferenceCountUtil.refCnt(original));
                    channel.eventLoop().submit(() -> {
                        // The fixture owns a never-submitted output, so no native completion is invented.
                        ReferenceCountUtil.release(original);
                        actual.setFailure(new ClosedChannelException());
                    }).get(5, TimeUnit.SECONDS);
                    assertEquals(ClassicChildResources.State.DRAINED, closed.get(5, TimeUnit.SECONDS).state());
                    assertEquals(0, ReferenceCountUtil.refCnt(original));
                    assertEquals(null, resource.status().operationFailure());
                    assertFalse(read.get(5, TimeUnit.SECONDS) == 0);
                } finally {
                    if (held.isDone() && promise.isDone() && !promise.get().isDone()) {
                        channel.eventLoop().submit(() -> {
                            ReferenceCountUtil.release(held.join());
                            promise.join().setFailure(new ClosedChannelException());
                        }).get(5, TimeUnit.SECONDS);
                    }
                    closingWorker.shutdown();
                    assertTrue(closingWorker.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test(timeout = 60000)
    public void sdkLongPollWatchIsRetiredWithoutWaitingForItsRequestTimeout() throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            f.zk.getZooKeeperClient().create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            var resource = f.open();
            resource.startNetworkForIoTests();
            var client = new ClientConfiguration().setMetadataServiceUri(f.conf.getMetadataServiceUri())
                    .setNumIOThreads(1).setNumWorkerThreads(1).setReadTimeout(3).setAddEntryTimeout(3);
            try (var sdk = new BookKeeper(client);
                 var handle = sdk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, new byte[0])) {
                assertEquals(0, handle.addEntry(new byte[]{1, 2, 3}));
                var storage = (InterleavedLedgerStorage) resource.bookieForIoTests().getLedgerStorage();
                var index = ((LedgerCacheImpl) storage.ledgerCache).getIndexPersistenceManager();
                var fileInfo = index.getFileInfo(handle.getId(), null);
                fileInfo.release();
                var watchCount = Watchable.class.getDeclaredMethod("getNumWatchers");
                watchCount.setAccessible(true);
                var pending = handle.readLastAddConfirmedAndEntryAsync(1, TimeUnit.MINUTES.toMillis(1), false);
                Awaitility.await().atMost(Duration.ofSeconds(5))
                        .until(() -> (int) watchCount.invoke(fileInfo) == 1);
                var worker = Executors.newSingleThreadExecutor();
                try {
                    var closed = worker.submit(resource::closeAndDrain);
                    assertEquals(ClassicChildResources.State.DRAINED, closed.get(5, TimeUnit.SECONDS).state());
                    assertEquals(0, (int) watchCount.invoke(fileInfo));
                    assertEquals(null, resource.status().operationFailure());
                    assertThrows(Exception.class, () -> pending.get(5, TimeUnit.SECONDS));
                } finally {
                    worker.shutdown();
                    assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test(timeout = 30000)
    public void lostProcessorExecutorReturnKeepsFailureWhileKnownPrefixesActuallyClose() throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            var resource = f.open();
            var bookie = (BookieImpl) resource.bookieForIoTests();
            f.unresolved.add(resource);
            var prefix = new AtomicReference<OrderedExecutor>();
            var creations = new AtomicInteger();
            var failure = new IllegalStateException("Actual write executor return was lost");
            try (var builders = mockStatic(OrderedExecutor.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (!invocation.getMethod().getName().equals("newBuilder")) {
                    return actual;
                }
                var builder = spy((OrderedExecutor.Builder) actual);
                doAnswer(build -> {
                    var executor = (OrderedExecutor) build.callRealMethod();
                    if (creations.incrementAndGet() == 1) {
                        prefix.set(executor);
                        return executor;
                    }
                    executor.shutdown();
                    Awaitility.await().atMost(Duration.ofSeconds(5)).until(executor::isTerminated);
                    // Fixture cleanup is not an acquisition/close result observed by the child.
                    throw failure;
                }).when(builder).build();
                return builder;
            })) {
                assertSame(failure, assertThrows(IllegalStateException.class, resource::startNetworkForIoTests));
            }
            assertFalse(prefix.get().isShutdown());
            var result = resource.closeAndDrain();
            assertEquals(ClassicChildResources.State.CLOSING, result.state());
            assertSame(failure, result.closeFailure());
            assertTrue(prefix.get().isTerminated());
            assertTrue(bookie.closeForFresh().drained());
            try (var connection = new Socket()) {
                assertThrows(IOException.class, () -> connection.connect(
                        new InetSocketAddress("127.0.0.1", f.conf.getBookiePort()), 1000));
            }
            assertFalse(f.registration.isBookieRegistered(SELF));
        }
    }

    @Test(timeout = 30000)
    public void listenerStageBindsRealTcpRejectsBeforeActivationAndDrainsWithoutRegistration() throws Exception {
        int port = PortManager.nextFreePort();
        try (var f = new Fixture(false, port)) {
            var resource = f.open();
            resource.bindListenerForIoTests();
            assertEquals(ClassicChildResources.State.TCP_BOUND, resource.status().state());
            assertFalse(f.registration.isBookieRegistered(SELF));
            assertThrows(IOException.class, resource::bindListenerForIoTests);
            try (var client = new Socket()) {
                client.connect(new InetSocketAddress("127.0.0.1", port), 5000);
                client.setSoTimeout(5000);
                int first;
                try {
                    first = client.getInputStream().read();
                } catch (SocketException closed) {
                    first = -1; // An established TCP connection may close with RST under the configured SO_LINGER.
                }
                assertEquals(-1, first);
            }
            // Enter the actual owned loop without adding a production raw-resource getter.
            var serverField = ClassicChildResources.class.getDeclaredField("listener");
            serverField.setAccessible(true);
            Object server = serverField.get(resource);
            var nettyField = server.getClass().getDeclaredField("nettyServer");
            nettyField.setAccessible(true);
            Object netty = nettyField.get(server);
            var loopsField = netty.getClass().getDeclaredField("eventLoopGroup");
            loopsField.setAccessible(true);
            var loops = (io.netty.channel.EventLoopGroup) loopsField.get(netty);
            loops.submit(() -> assertThrows(IllegalStateException.class, resource::closeAndDrain))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(ClassicChildResources.State.TCP_BOUND, resource.status().state());
            assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
            try (var probe = new ServerSocket()) {
                probe.setReuseAddress(true);
                probe.bind(new InetSocketAddress("127.0.0.1", port));
            }
            assertFalse(f.registration.isBookieRegistered(SELF));
        }
    }

    @Test(timeout = 30000)
    public void occupiedTcpPortFailsWithItsKnownBindFutureAndDrainsTheOriginalGroups() throws Exception {
        try (var busy = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
             var f = new Fixture(false, busy.getLocalPort())) {
            var resource = f.open();
            assertThrows(IOException.class, resource::bindListenerForIoTests);
            assertNotNull(resource.status().initializationFailure());
            assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
            assertFalse(f.registration.isBookieRegistered(SELF));
            assertTrue(busy.isBound());
            assertFalse(busy.isClosed());
        }
    }

    @Test(timeout = 30000)
    public void unknownAcceptorConstructionRetainsItsOriginalFailureAfterKnownPrefixesDrain() throws Exception {
        try (var f = new Fixture(false, PortManager.nextFreePort())) {
            var resource = f.open();
            f.unresolved.add(resource);
            IOException failure = new IOException("Actual acceptor group return was lost");
            try (var groups = mockStatic(EventLoopUtil.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("getServerAcceptorGroup")) {
                    var acquired = (io.netty.channel.EventLoopGroup) actual;
                    acquired.shutdownGracefully(0, Long.MAX_VALUE, TimeUnit.NANOSECONDS).sync();
                    // Fixture cleanup cannot supply the constructor result or original close to the owner.
                    throw failure;
                }
                return actual;
            })) {
                assertSame(failure, assertThrows(IOException.class, resource::bindListenerForIoTests));
            }
            for (int i = 0; i < 2; i++) {
                var result = resource.closeAndDrain();
                assertEquals(ClassicChildResources.State.CLOSING, result.state());
                assertSame(failure, result.closeFailure());
            }
            assertFalse(f.registration.isBookieRegistered(SELF));
        }
    }

    @Test(timeout = 30000)
    public void acquisitionFailureWithoutAHandleCannotInventAnOriginalCloseResult() throws Exception {
        try (var f = new Fixture()) {
            Path version = f.conf.getLedgerDirs()[0].toPath().resolve("current/VERSION");
            var resource = ClassicChildResources.prepare();
            f.resources.add(resource);
            f.unresolved.add(resource);
            IOException failure = new IOException("Cookie acquisition lost its actual return");
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open")
                        && invocation.getMethod().getParameterCount() == 3
                        && version.equals(invocation.getArgument(0))) {
                    ((FileChannel) actual).close(); // Fixture cleanup, not a native result observed by the owner.
                    throw failure;
                }
                return actual;
            })) {
                assertSame(failure, assertThrows(IOException.class,
                        () -> resource.initializeForIoTests(f.conf, f.registration, f.manager, f.replication)));
            }
            for (int i = 0; i < 2; i++) {
                var result = resource.closeAndDrain();
                assertEquals(ClassicChildResources.State.CLOSING, result.state());
                assertSame(failure, result.closeFailure());
            }
        }
    }

    @Test(timeout = 30000)
    public void malformedHistoricalMetadataAndUnknownSpecialEntriesRejectBeforeSuccessfulStartup() throws Exception {
        for (boolean unknown : new boolean[]{false, true}) {
            try (var f = new Fixture()) {
                ByteBuf record = Unpooled.buffer(20).writeLong(7)
                        .writeLong(unknown ? -999 : BookieImpl.METAENTRY_ID_LEDGER_KEY);
                if (!unknown) {
                    record.writeInt(-1);
                }
                f.appendJournalRecord(record);
                var resource = ClassicChildResources.prepare();
                f.resources.add(resource);
                assertThrows(IOException.class,
                        () -> resource.initializeForIoTests(f.conf, f.registration, f.manager, f.replication));
                assertNotNull(resource.status().initializationFailure());
                assertEquals(ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
            }
        }
    }

    private final class Fixture implements AutoCloseable {
        private boolean factoryClosed;
        final ZooKeeperUtil zk = new ZooKeeperUtil();
        final ServerConfiguration conf;
        LedgerManagerFactory factory;
        AbstractZkLedgerManager manager;
        ZkLedgerUnderreplicationManager replication;
        ZKRegistrationManager registration;
        ZooKeeper metadataClient;
        ZKMetadataBookieDriver driver;
        final List<ClassicChildResources> resources = new ArrayList<>();
        final Set<ClassicChildResources> unresolved = new HashSet<>();

        Fixture() throws Exception {
            this(false, 0);
        }

        Fixture(boolean interceptRegistration) throws Exception {
            this(interceptRegistration, 0);
        }

        Fixture(boolean interceptRegistration, int listenerPort) throws Exception {
            zk.startCluster();
            zk.getZooKeeperClient().create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            String cluster = UUID.randomUUID().toString();
            zk.getZooKeeperClient().create(ROOT + "/INSTANCEID", cluster.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            File journal = temporary.newFolder();
            File ledger = temporary.newFolder();
            File index = temporary.newFolder();
            for (File root : List.of(journal, ledger, index)) {
                Files.createDirectory(root.toPath().resolve("current"));
            }
            conf = new ServerConfiguration().setBookieId(SELF.toString()).setAllowLoopback(true)
                    .setJournalDirName(journal.getCanonicalPath())
                    .setLedgerDirNames(new String[]{ledger.getCanonicalPath()})
                    .setIndexDirName(new String[]{index.getCanonicalPath()})
                    .setLedgerStorageClass(InterleavedLedgerStorage.class.getName())
                    .setMetadataServiceUri(zk.getMetadataServiceUri(ROOT, "hierarchical"))
                    .setJournalMaxMemorySizeMb(1).setJournalQueueSize(4).setJournalWriteBufferSizeKB(4)
                    .setJournalBufferedWritesThreshold(1).setJournalMaxGroupWaitMSec(1)
                    .setJournalFlushWhenQueueEmpty(true).setPageLimit(8).setOpenFileLimit(4)
                    .setFileInfoFormatVersionToWrite(1).setJournalFormatVersionToWrite(6)
                    .setAllowMultipleDirsUnderSameDiskPartition(true).setVerifyMetadataOnGc(true)
                    .setGcWaitTime(TimeUnit.HOURS.toMillis(1)).setEntryLogFilePreAllocationEnabled(false)
                    .setWriteBufferBytes(4096).setReadBufferBytes(4096)
                    .setDiskUsageThreshold(.999f).setDiskUsageWarnThreshold(.99f);
            conf.setProperty("journalPreAllocSizeMB", 1);
            if (listenerPort != 0) {
                conf.setBookiePort(listenerPort).setAdvertisedAddress("127.0.0.1")
                        .setListeningInterface(NetworkInterface.getByInetAddress(InetAddress.getLoopbackAddress())
                                .getName()).setServerNumIOThreads(1)
                        .setNumAddWorkerThreads(1).setNumReadWorkerThreads(1)
                        .setNumLongPollWorkerThreads(1).setNumHighPriorityWorkerThreads(1)
                        .setEnableLocalTransport(false);
                conf.setProperty("serverNumAcceptorThreads", 1);
            }
            // Cluster administration provisions history; the controlled driver never formats missing layout.
            new ZkLayoutManager(zk.getZooKeeperClient(), ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .storeLedgerLayout(new LedgerLayout(HierarchicalLedgerManagerFactory.class.getName(),
                            HierarchicalLedgerManagerFactory.CUR_VERSION));
            initializeMetadata(interceptRegistration);
            assertEquals(null, zk.getZooKeeperClient().exists(ROOT + "/available", false));
            var metadata = LedgerMetadataBuilder.create().withId(7).withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[0]).withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                    .newEnsembleEntry(0, List.of(SELF)).build();
            manager.createLedgerMetadata(7, metadata).get(5, TimeUnit.SECONDS);
            var ledgers = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
            var indexes = new LedgerDirsManager(conf, conf.getIndexDirs(), new DiskChecker(.999f, .99f));
            var storage = InterleavedLedgerStorage.forFresh();
            storage.setCheckpointSource(CheckpointSource.DEFAULT);
            storage.setCheckpointer(Checkpointer.NULL);
            try {
                storage.initializeForFresh(conf, ledgers, indexes, NullStatsLogger.INSTANCE,
                        UnpooledByteBufAllocator.DEFAULT);
                storage.setMasterKey(7, KEY);
                ByteBuf input = entry(0);
                try {
                    storage.addEntry(input);
                    storage.flush();
                } finally {
                    input.release();
                }
            } finally {
                assertTrue(storage.closeForFresh().drained());
            }
            var cookie = Cookie.generateCookie(conf).setInstanceId(cluster).build();
            cookie.writeToRegistrationManager(registration, conf, Version.NEW);
            for (File root : List.of(journal, ledger, index)) {
                cookie.writeToDirectory(new File(root, "current"));
            }
        }

        private void initializeMetadata(boolean interceptRegistration) throws Exception {
            var watcher = new ZooKeeperWatcherBase(10000, false);
            ZooKeeper raw = new ZooKeeper(zk.getZooKeeperConnectString(), 10000, watcher);
            watcher.waitForConnection();
            metadataClient = interceptRegistration ? spy(raw) : raw;
            assertEquals(ZooKeeper.class, metadataClient.getClass());
            driver = new ZKMetadataBookieDriver();
            driver.initializeForClassicChild(conf, NullStatsLogger.INSTANCE, metadataClient);
            factory = driver.getLedgerManagerFactory();
            manager = (AbstractZkLedgerManager) factory.newLedgerManager();
            replication = new ZkLedgerUnderreplicationManager(conf, metadataClient);
            registration = (ZKRegistrationManager) driver.createRegistrationManager();
        }

        private void closeMetadata() throws Exception {
            replication.close();
            assertTrue(manager.closeAndAwaitTermination(Duration.ofSeconds(5)));
            if (!factoryClosed) {
                factory.close();
            }
            registration.close();
            metadataClient.close();
        }

        void restartMetadata() throws Exception {
            closeMetadata();
            initializeMetadata(false);
        }

        ClassicChildResources open() throws Exception {
            var resource = ClassicChildResources.prepare();
            resources.add(resource);
            resource.initializeForIoTests(conf, registration, manager, replication);
            return resource;
        }

        void appendJournalRecord(ByteBuf input) throws Exception {
            var dirs = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
            var journal = Journal.forClassicHistory(0, new File(conf.getJournalDirs()[0], "current"), conf, dirs,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT, () -> { });
            try {
                journal.initializeForClassicHistory();
                journal.start();
                journal.awaitStartedForFresh();
                var written = new CompletableFuture<Integer>();
                journal.logAddEntry(7, input.getLong(8), input, false,
                        (rc, ledger, entry, address, context) -> written.complete(rc), null);
                assertEquals(0, (int) written.get(5, TimeUnit.SECONDS));
            } finally {
                input.release();
                assertTrue(journal.closeForFresh().drained());
            }
        }

        @Override
        public void close() throws Exception {
            try {
                for (var resource : resources) {
                    assertEquals(unresolved.contains(resource) ? ClassicChildResources.State.CLOSING
                            : ClassicChildResources.State.DRAINED, resource.closeAndDrain().state());
                }
                closeMetadata();
            } finally {
                try {
                    metadataClient.close();
                } finally {
                    zk.stopCluster();
                }
            }
        }
    }
}

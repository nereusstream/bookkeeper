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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Native filesystem/metadata combination only; no old-process issuer or complete Classic engine bootstrap. */
public class SegmentClassicRootBindingsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private final ZooKeeperUtil cluster = new ZooKeeperUtil();
    private ZooKeeper zk;
    private ZKRegistrationManager manager;
    private ServerConfiguration classic;
    private SegmentArenaManifest manifest;
    private IsolatedSegmentConfig config;
    private BookieRouteBinding.Observation prepared;
    private SegmentBookieControlLog.RouteBinding route;

    @Before
    public void setup() throws Exception {
        cluster.startCluster();
        zk = cluster.getZooKeeperClient();
        String instance = "/ledgers/INSTANCEID";
        if (zk.exists(instance, false) == null) {
            zk.create(instance, "root-native-cluster".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String clusterId = new String(zk.getData(instance, false, null), StandardCharsets.UTF_8);
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path journal = Files.createDirectory(parent.resolve("classic-0-journal"));
        Path ledger = Files.createDirectory(parent.resolve("classic-1-ledger-index"));
        classic = new ServerConfiguration().setBookieId("common-root:3181")
                .setJournalDirsName(new String[]{journal.toString()})
                .setLedgerDirNames(new String[]{ledger.toString()});
        manager = new ZKRegistrationManager(classic, zk, "/ledgers");
        Cookie source = Cookie.generateCookie(classic).setInstanceId(clusterId).build();
        source.writeToDirectory(Files.createDirectory(journal.resolve("current")).toFile());
        source.writeToDirectory(Files.createDirectory(ledger.resolve("current")).toFile());
        source.writeToRegistrationManager(manager, classic, Version.NEW);
        byte[] incarnation = new byte[16];
        incarnation[0] = 1;
        manifest = new SegmentArenaManifest(new StorageIncarnation(incarnation), BookieId.parse(classic.getBookieId()),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 9), parent.resolve("arena"), 1, 16384)),
                parent.resolve("control"), List.of(0), List.of(
                        new SegmentArenaManifest.ClassicRoot(journal, Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(ledger, Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                SegmentArenaManifest.ClassicRole.INDEX))));
        config = new IsolatedSegmentConfig(manifest.controlDirectory(), 1, 1, 1, 16384, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        var cookieStat = zk.exists(manager.getCookiePath(manifest.bookieId()), false);
        var binding = new BookieRouteBinding(clusterId, zk.exists(instance, false).getCzxid(), manifest.bookieId(),
                manifest.incarnation(), new UUID(0, 20), manifest.identity(), manifest.controlDirectory().toString(),
                new BookieRouteBinding.SourceCookie(cookieStat.getCzxid(), cookieStat.getVersion(),
                        MessageDigest.getInstance("SHA-256").digest(
                                source.toString().getBytes(StandardCharsets.UTF_8))),
                BookieRouteBinding.Phase.PREPARED, Optional.empty());
        prepared = manager.prepareBookieRouteBinding(binding).observation().orElseThrow();
        route = new SegmentBookieControlLog.RouteBinding(binding.bindingId(), manifest.identity());
    }

    @After
    public void cleanup() throws Exception {
        cluster.stopCluster();
    }

    private SegmentClassicRootBindings roots(BookieRouteBinding.Observation observation) throws IOException {
        return new SegmentClassicRootBindings(classic, manager, manifest, observation);
    }

    private SegmentBookieControlLog createControl() throws IOException {
        return SegmentBookieControlLog.createManifestControl(config, manifest, manifest.identity(), 32768,
                new SecretKeySpec(new byte[32], "AES"),
                new ProfileDescriptorValidator((i, v) -> false, (i, v) -> false),
                new SegmentFileIo(), route);
    }

    private SegmentBookieControlLog openControl() throws IOException {
        return SegmentBookieControlLog.open(config, SegmentFileHeader.forBookie(manifest), 32768,
                new SecretKeySpec(new byte[32], "AES"),
                new ProfileDescriptorValidator((i, v) -> false, (i, v) -> false),
                new SegmentFileIo(), route);
    }

    private Path marker(int member) {
        return member < 0 ? manifest.controlDirectory().resolve(SegmentClassicRootBindings.FILE_NAME)
                : manifest.classicRoots().get(member).directory().resolve("current")
                        .resolve(SegmentClassicRootBindings.FILE_NAME);
    }

    @Test
    public void offAndDirectConstructionRefuseNativeBindingBeforeAnyClassicSideEffect() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path missingJournal = parent.resolve("must-not-create-journal");
        Path missingLedger = parent.resolve("must-not-create-ledger");
        var requested = new ServerConfiguration(classic).setJournalDirsName(new String[]{missingJournal.toString()})
                .setLedgerDirNames(new String[]{missingLedger.toString()});
        requested.setProperty(SegmentProfileService.CONFIG_PREFIX + "Mode", "OFF");
        for (var phase : BookieRouteBinding.Phase.values()) {
            if (phase == BookieRouteBinding.Phase.ENABLED) {
                manager.completeBookieRouteBinding(prepared,
                        new BookieRouteBinding.BootstrapSeal(UUID.randomUUID(), 0, -1)).observation().orElseThrow();
            }
            assertThrows(IOException.class, () -> ClassicRouteStartup.preflight(requested, manager));
            assertThrows(IOException.class,
                    () -> new BookieImpl(requested, manager, null, null, null, null, null, null, null));
            assertThrows(IOException.class, () -> new org.apache.bookkeeper.bookie.ReadOnlyBookie(
                    requested, manager, null, null, null, null, null, null, null));
            assertThrows(IOException.class, () -> org.apache.bookkeeper.server.EmbeddedServer.builder(
                    new org.apache.bookkeeper.server.conf.BookieConfiguration(requested))
                    .registrationManager(manager)
                    .ledgerManagerFactory(new NullMetadataBookieDriver.NullLedgerManagerFactory()).build());
        }
        assertThrows(IOException.class,
                () -> new BookieImpl(requested, null, null, null, null, null, null, null, null));
        assertFalse(Files.exists(missingJournal));
        assertFalse(Files.exists(missingLedger));
    }

    @Test
    public void nativeAbsenceKeepsRealClassicCookieAndJournalAcrossReadonlyRestart() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path journal = parent.resolve("unbound-real-journal");
        Path ledger = parent.resolve("unbound-real-ledger");
        int port = org.apache.bookkeeper.util.PortManager.nextFreePort();
        var configuration = new ServerConfiguration().setMetadataServiceUri(cluster.getMetadataServiceUri())
                .setBookiePort(port).setBookieId("127.0.0.1:" + port).setAllowLoopback(true)
                .setJournalDirName(journal.toString()).setLedgerDirNames(new String[]{ledger.toString()})
                .setAutoRecoveryDaemonEnabled(false).setHttpServerEnabled(false);
        byte[] entry = ByteBuffer.allocate(24).putLong(123).putLong(0).putLong(456).array();
        byte[] key = new byte[]{7};
        byte[] originalCookie = null;
        for (boolean restart : List.of(false, true)) {
            configuration.setForceReadOnlyBookie(restart);
            var server = org.apache.bookkeeper.server.EmbeddedServer.builder(
                    new org.apache.bookkeeper.server.conf.BookieConfiguration(configuration)).build();
            try {
                server.getLifecycleComponentStack().start();
                var bookie = server.getBookieService().getServer().getBookie();
                assertEquals(restart, bookie.isReadOnly());
                if (!restart) {
                    var result = new java.util.concurrent.CompletableFuture<Integer>();
                    bookie.addEntry(io.netty.buffer.Unpooled.wrappedBuffer(entry), false,
                            (rc, ledgerId, entryId, address, context) -> result.complete(rc), null, key);
                    assertEquals(Integer.valueOf(0), result.get(5, TimeUnit.SECONDS));
                    originalCookie = Files.readAllBytes(journal.resolve("current/VERSION"));
                } else {
                    var read = bookie.readEntry(123, 0);
                    try {
                        byte[] actual = new byte[read.readableBytes()];
                        read.readBytes(actual);
                        assertArrayEquals(entry, actual);
                    } finally {
                        read.release();
                    }
                    assertArrayEquals(originalCookie, Files.readAllBytes(journal.resolve("current/VERSION")));
                }
                assertEquals(BookieRouteBinding.ReadStatus.ABSENT,
                        server.getRegistrationManager().readBookieRouteBinding(
                                BookieId.parse(configuration.getBookieId())).status());
                assertFalse(Files.exists(journal.resolve("current/" + SegmentClassicRootBindings.FILE_NAME)));
                assertFalse(Files.exists(ledger.resolve("current/" + SegmentClassicRootBindings.FILE_NAME)));
            } finally {
                server.getLifecycleComponentStack().close();
            }
        }
    }

    @Test
    public void nativeAbsenceContextIsOneEpisodeAndCannotChangeManagerBookieOrRoots() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        var unbound = new ServerConfiguration(classic).setBookieId("unbound:3181")
                .setJournalDirsName(new String[]{parent.resolve("fresh-journal").toString()})
                .setLedgerDirNames(new String[]{parent.resolve("fresh-ledger").toString()});
        var context = ClassicRouteStartup.preflight(unbound, manager);
        assertThrows(IOException.class,
                () -> context.consume(unbound, new ZKRegistrationManager(classic, zk, "/ledgers")));
        assertThrows(IOException.class,
                () -> context.consume(new ServerConfiguration(unbound).setBookieId("other:3181"), manager));
        assertThrows(IOException.class, () -> context.consume(new ServerConfiguration(unbound)
                .setLedgerDirNames(new String[]{parent.resolve("other-root").toString()}), manager));
        context.consume(unbound, manager);
        assertThrows(IOException.class, () -> context.consume(unbound, manager));
        assertFalse(Files.exists(parent.resolve("fresh-journal")));
        assertFalse(Files.exists(parent.resolve("fresh-ledger")));
        assertThrows(IOException.class, () -> ClassicRouteStartup.preflight(unbound,
                new NullMetadataBookieDriver.NullRegistrationManager()));
    }

    @Test
    public void nativeAbsenceDoesNotEraseRetainedLocalMarkers() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path journal = Files.createDirectories(parent.resolve("orphan-journal/current"));
        Files.write(journal.resolve(SegmentClassicRootBindings.FILE_NAME), new byte[]{1});
        var unbound = new ServerConfiguration(classic).setBookieId("unbound:3181")
                .setJournalDirsName(new String[]{journal.getParent().toString()})
                .setLedgerDirNames(new String[]{parent.resolve("fresh-ledger").toString()});
        assertThrows(IOException.class, () -> ClassicRouteStartup.preflight(unbound, manager));
        assertArrayEquals(new byte[]{1}, Files.readAllBytes(journal.resolve(SegmentClassicRootBindings.FILE_NAME)));
        assertFalse(Files.exists(parent.resolve("fresh-ledger")));
    }

    @Test
    public void neitherStartupGuardInterpretsUnknownOrNullAsNativeAbsence() throws Exception {
        Path parent = temporary.getRoot().toPath().toRealPath();
        var unbound = new ServerConfiguration(classic).setBookieId("unbound:3181")
                .setJournalDirName(parent.resolve("unopened-journal").toString())
                .setLedgerDirNames(new String[]{parent.resolve("unopened-ledger").toString()});
        var profile = new IsolatedSegmentConfig(parent.resolve("unopened-profile"), 1, 1, 1,
                16384, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        var results = new java.util.ArrayList<BookieRouteBinding.ReadResult>();
        for (var status : List.of(BookieRouteBinding.ReadStatus.UNSUPPORTED, BookieRouteBinding.ReadStatus.UNAVAILABLE,
                BookieRouteBinding.ReadStatus.CORRUPT, BookieRouteBinding.ReadStatus.INCOMPATIBLE)) {
            results.add(BookieRouteBinding.ReadResult.status(status, "unavailable authority"));
        }
        results.add(null);
        for (var result : results) {
            var registration = org.mockito.Mockito.mock(org.apache.bookkeeper.discover.RegistrationManager.class);
            org.mockito.Mockito.when(registration.readBookieRouteBinding(org.mockito.ArgumentMatchers.any()))
                    .thenReturn(result);
            assertThrows(IOException.class, () -> ClassicRouteStartup.preflight(unbound, registration));
            var manager = org.mockito.Mockito.mock(org.apache.bookkeeper.meta.AbstractZkLedgerManager.class);
            org.mockito.Mockito.when(manager.readBookieRouteBinding(org.mockito.ArgumentMatchers.any(),
                    org.mockito.ArgumentMatchers.any())).thenReturn(result);
            var metadata = new org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore(manager);
            assertThrows(IOException.class, () -> ClassicRouteStartup.requireUnboundProfile(
                    metadata, BookieId.parse(unbound.getBookieId()), java.time.Duration.ofSeconds(5), profile));
        }
        assertFalse(Files.exists(parent.resolve("unopened-journal")));
        assertFalse(Files.exists(parent.resolve("unopened-ledger")));
        assertFalse(Files.exists(profile.directory()));
    }

    @Test
    public void markersAndActualControlSealEnableSameHeldRootsAndRecoverWithoutRepair() throws Exception {
        BookieRouteBinding.Observation enabled;
        try (var roots = roots(prepared)) {
            assertThrows(IOException.class, roots::verifyExisting);
            assertFalse(Files.exists(manifest.controlDirectory()));
            roots.openLocks(new SegmentFileIo());
            try (var bookie = createControl()) {
                var io = new SegmentFileIo(7, -1, -1, false);
                roots.createMarkers(bookie, io);
                assertEquals(3, io.forces);
                assertTrue(io.writes > 3);
                roots.verifyExisting();
                assertThrows(IOException.class, () -> roots.verifyEnabledOwner(bookie));
                for (int member = -1; member < manifest.classicRoots().size(); member++) {
                    byte[] raw = Files.readAllBytes(marker(member));
                    ByteBuffer in = ByteBuffer.wrap(raw);
                    assertEquals(0x5557435242303031L, in.getLong());
                    assertEquals(1, in.getInt());
                    assertEquals(raw.length, in.getInt());
                    assertEquals(member, in.getInt());
                    assertEquals(member < 0 ? 0 : member == 0 ? 1 : 6, in.getInt());
                    assertEquals(prepared.birthId(), new UUID(in.getLong(), in.getLong()));
                    assertEquals(prepared.birthZxid(), in.getLong());
                    byte[] root = new byte[in.getInt()];
                    in.get(root);
                    Path expected = member < 0 ? manifest.controlDirectory()
                            : manifest.classicRoots().get(member).directory();
                    assertEquals(expected.toString(), new String(root, StandardCharsets.UTF_8));
                    byte[] binding = new byte[in.getInt()];
                    in.get(binding);
                    assertEquals(prepared.binding(), BookieRouteBinding.decode(binding));
                    CRC32C crc = new CRC32C();
                    crc.update(raw, 0, raw.length - 4);
                    assertEquals((int) crc.getValue(), in.getInt());
                    assertFalse(in.hasRemaining());
                    assertEquals(PosixFilePermissions.fromString("rw-------"),
                            Files.getPosixFilePermissions(marker(member)));
                }
                bookie.applyDurable(bookie.appendChangesDurably(List.of(new SegmentBookieControlLog.RouteChange(
                        SegmentBookieControlLog.RouteAction.BOOTSTRAP_COMPLETE, -1, route, new UUID(0, 21)))));
                var seal = bookie.bootstrapSeal().orElseThrow();
                assertEquals(new BookieRouteBinding.BootstrapSeal(new UUID(0, 21), 0, -1), seal);
                enabled = manager.completeBookieRouteBinding(prepared, seal).observation().orElseThrow();
                roots.adoptEnabled(enabled, bookie);
                roots.verifyEnabledOwner(bookie);
                assertThrows(IOException.class, () -> roots.createMarkers(bookie, new SegmentFileIo()));
            }
        }
        try (var roots = roots(enabled)) {
            roots.verifyExisting();
            roots.openLocks(new SegmentFileIo());
            try (var bookie = openControl()) {
                roots.verifyEnabledOwner(bookie);
                assertEquals(enabled.binding().seal(), bookie.bootstrapSeal());
            }
        }
    }

    @Test
    public void heldMarkerForceKeepsRootLocksAndCloseWaitsForTheActualReturn() throws Exception {
        var roots = roots(prepared);
        roots.openLocks(new SegmentFileIo());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean once = new AtomicBoolean();
        var io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (once.compareAndSet(false, true)) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Marker force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        });
        try (var bookie = createControl()) {
            FutureTask<Void> write = new FutureTask<>(() -> {
                roots.createMarkers(bookie, io);
                return null;
            });
            Thread writer = new Thread(write, "common-marker-test-write");
            writer.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            try (var competing = roots(prepared)) {
                assertThrows(IOException.class, () -> competing.openLocks(new SegmentFileIo()));
            }
            CountDownLatch closing = new CountDownLatch(1);
            FutureTask<Void> close = new FutureTask<>(() -> {
                closing.countDown();
                roots.close();
                return null;
            });
            Thread closer = new Thread(close, "common-marker-test-close");
            closer.start();
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            try {
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            } finally {
                release.countDown();
            }
            write.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            writer.join(5000);
            closer.join(5000);
            assertFalse(writer.isAlive());
            assertFalse(closer.isAlive());
            assertThrows(IOException.class, roots::verifyExisting);
        } finally {
            release.countDown();
            roots.close();
        }
    }

    @Test
    public void partialCandidatesAreRetainedAndCompleteUnknownCandidatesCanBeResynchronized() throws Exception {
        try (var roots = roots(prepared)) {
            roots.openLocks(new SegmentFileIo());
            try (var bookie = createControl()) {
                assertThrows(IOException.class,
                        () -> roots.createMarkers(bookie, new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
                byte[] candidate = Files.readAllBytes(marker(-1));
                roots.createMarkers(bookie, new SegmentFileIo());
                assertArrayEquals(candidate, Files.readAllBytes(marker(-1)));
                roots.verifyExisting();
                Files.write(marker(0), new byte[]{1, 2, 3});
                byte[] partial = Files.readAllBytes(marker(0));
                assertThrows(IOException.class, () -> roots.createMarkers(bookie, new SegmentFileIo()));
                assertArrayEquals(partial, Files.readAllBytes(marker(0)));
                assertThrows(IOException.class, roots::verifyExisting);
            }
        }
    }

    @Test
    public void missingMarkerAfterControlHistoryCannotBeReinitialized() throws Exception {
        try (var roots = roots(prepared)) {
            roots.openLocks(new SegmentFileIo());
            try (var bookie = createControl()) {
                roots.createMarkers(bookie, new SegmentFileIo());
                bookie.applyDurable(bookie.appendChangesDurably(List.of(new SegmentBookieControlLog.RouteChange(
                        SegmentBookieControlLog.RouteAction.CLAIM_CLASSIC, 0, route, new UUID(0, 21)))));
                Files.delete(marker(0));
                assertThrows(IOException.class, () -> roots.createMarkers(bookie, new SegmentFileIo()));
                assertFalse(Files.exists(marker(0)));
            }
        }
    }

    @Test
    public void memberSwapsPublicFilesAndCookieChangesCannotBeAcceptedAsTheOriginalScope() throws Exception {
        try (var roots = roots(prepared)) {
            roots.openLocks(new SegmentFileIo());
            try (var bookie = createControl()) {
                roots.createMarkers(bookie, new SegmentFileIo());
                byte[] original = Files.readAllBytes(marker(0));
                Files.write(marker(0), Files.readAllBytes(marker(-1)));
                assertThrows(IOException.class, roots::verifyExisting);
                Files.write(marker(0), original);
                Files.setPosixFilePermissions(marker(0), PosixFilePermissions.fromString("rw-r--r--"));
                assertThrows(IOException.class, roots::verifyExisting);
                Files.setPosixFilePermissions(marker(0), PosixFilePermissions.fromString("rw-------"));
                Path cookie = manifest.classicRoots().get(0).directory().resolve("current/VERSION");
                Files.write(cookie, new byte[]{1});
                assertThrows(IOException.class, roots::verifyExisting);
            }
        }
    }

    @Test
    public void recreatedNativeBirthCannotReuseExistingMarkers() throws Exception {
        try (var roots = roots(prepared)) {
            roots.openLocks(new SegmentFileIo());
            try (var bookie = createControl()) {
                roots.createMarkers(bookie, new SegmentFileIo());
            }
        }
        String encoded = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(manifest.bookieId().toString().getBytes(StandardCharsets.UTF_8));
        String path = "/ledgers/bookie-route-bindings/" + encoded;
        for (String child : zk.getChildren(path, false)) {
            zk.delete(path + "/" + child, 0);
        }
        zk.delete(path, 0);
        var replacement = manager.prepareBookieRouteBinding(prepared.binding()).observation().orElseThrow();
        assertNotEquals(prepared.birthId(), replacement.birthId());
        try (var roots = roots(replacement)) {
            assertThrows(IOException.class, roots::verifyExisting);
        }
    }
}

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

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.bookie.ClassicClaimTestHistory;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;

/** Handoff material I/O and cold native source binding under the original supervisor guard. */
public class SegmentClassicHandoffMaterialsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private long uid;
    private long oldUid;
    private KeyPair signing;
    private SegmentArenaManifest manifest;
    private BookieRouteBinding.Observation nativeScope;
    private SegmentClassicHandoffMaterials.References references;
    private SegmentClassicHandoffMaterials.Expected expected;
    private Path guardPath;
    private Path intentPath;
    private ZooKeeperUtil cluster;
    private HeldReadClient client;

    private static byte[] filled(int length, int value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static SegmentClassicHandoff.Digest digest(int value) {
        return new SegmentClassicHandoff.Digest(filled(32, value));
    }

    private static void privateFile(Path path, byte[] value) throws IOException {
        Files.write(path, value);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
    }

    private static Path directory(Path path) throws IOException {
        return Files.createDirectory(path, PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwx------")));
    }

    @Before
    public void materialFiles() throws Exception {
        uid = new UnixSystem().getUid();
        oldUid = uid == 1001 ? 1003 : 1001;
        signing = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Path root = temporary.getRoot().toPath().toRealPath();
        Path journal = directory(root.resolve("classic-0"));
        Path ledger = directory(root.resolve("classic-1"));
        Path control = directory(root.resolve("control"));
        Path arena = directory(root.resolve("arena"));
        manifest = new SegmentArenaManifest(new StorageIncarnation(filled(16, 1)), BookieId.parse("materials:3181"),
                List.of(new SegmentArenaManifest.Arena(new UUID(0, 11), arena, 1, 16384)),
                control, List.of(0), List.of(
                        new SegmentArenaManifest.ClassicRoot(journal, Set.of(SegmentArenaManifest.ClassicRole.JOURNAL)),
                        new SegmentArenaManifest.ClassicRoot(ledger, Set.of(SegmentArenaManifest.ClassicRole.LEDGER,
                                SegmentArenaManifest.ClassicRole.INDEX))));
        // Only local guard files are needed for the file reader's alias exclusion, not a formatted service graph.
        for (var member : manifest.classicRoots()) {
            Path current = directory(member.directory().resolve("current"));
            privateFile(current.resolve(SegmentClassicRootBindings.LOCK_NAME), new byte[0]);
        }
        privateFile(control.resolve("bookie-control.lock"), new byte[0]);
        privateFile(arena.resolve("isolated.lock"), new byte[0]);
        privateFile(arena.resolve("arena-control.lock"), new byte[0]);
        var binding = new BookieRouteBinding("material-cluster", 100, manifest.bookieId(), manifest.incarnation(),
                new UUID(0, 2), manifest.identity(), control.toString(),
                new BookieRouteBinding.SourceCookie(102, 3, filled(32, 3)), BookieRouteBinding.Phase.PREPARED,
                Optional.empty());
        nativeScope = new BookieRouteBinding.Observation(binding, new UUID(0, 4), 104, 104, 0);
        var requirements = new SegmentClassicHandoff.Requirements(
                new ZKProfileStartupRequirementsStore.SourceIdentity("/commissioned/materials", new UUID(0, 5),
                        106, 106, 0, filled(32, 6)), 1, 2, 3, 4, 1, 1);
        Path materials = directory(root.resolve("materials"));
        references = new SegmentClassicHandoffMaterials.References(materials, materials.resolve("proof"),
                materials.resolve("policy"), materials.resolve("key"));
        writeMaterials(requirements);
    }

    private void writeMaterials(SegmentClassicHandoff.Requirements requirements) throws Exception {
        expected = new SegmentClassicHandoffMaterials.Expected("deployment", digest(8), digest(9), signing.getPublic(),
                nativeScope, requirements, oldUid, uid, uid);
        var binding = nativeScope.binding();
        String stem = SegmentClassicHandoffMaterials.scopeName(binding);
        guardPath = references.directory().resolve(stem + ".lock");
        intentPath = references.directory().resolve(stem + ".retirement");
        privateFile(guardPath, new byte[0]);
        var policy = new SegmentClassicHandoff.Policy(binding.bindingId(), binding.bookieId(), binding.incarnation(),
                binding.manifest(), uid);
        var operation = new SegmentClassicHandoff.Operation("deployment", digest(8), digest(9), nativeScope,
                requirements, new SegmentClassicHandoff.Episode(new UUID(0, 7), 99, Instant.ofEpochSecond(1000),
                        108, 109), oldUid, uid, SegmentClassicHandoff.keyIdentity(filled(32, 10)),
                SegmentClassicHandoff.digest(SegmentClassicHandoff.encodePolicy(policy)));
        var members = new ArrayList<SegmentClassicHandoff.Member>();
        for (int member = 0; member < manifest.classicRoots().size(); member++) {
            members.add(signedMember(manifest.classicRoots().get(member).directory(), SegmentClassicHandoff.CLASSIC,
                    member));
        }
        members.add(signedMember(manifest.controlDirectory(), SegmentClassicHandoff.CONTROL, 0));
        for (int member = 0; member < manifest.arenas().size(); member++) {
            members.add(signedMember(manifest.arenas().get(member).directory(), SegmentClassicHandoff.ARENA, member));
        }
        var retirement = new SegmentClassicHandoff.Retirement(operation,
                members.stream().map(SegmentClassicHandoff.Member::physical).toList());
        byte[] intent = SegmentClassicHandoff.encodeRetirement(retirement);
        var proof = new SegmentClassicHandoff.Proof(operation, members,
                new SegmentClassicHandoff.Terminal(12, Instant.ofEpochSecond(1001), digest(11)), digest(12),
                SegmentClassicHandoff.digest(intent));
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(signing.getPrivate());
        signer.update(SegmentClassicHandoff.signingMessage(proof));
        privateFile(references.proof(), SegmentClassicHandoff.envelope(proof, signer.sign()));
        privateFile(references.policy(), SegmentClassicHandoff.encodePolicy(policy));
        privateFile(references.aesKey(), filled(32, 10));
        privateFile(intentPath, intent);
    }

    private SegmentClassicHandoff.Member signedMember(Path path, int kind, int index) throws IOException {
        return new SegmentClassicHandoff.Member(new SegmentClassicHandoff.Physical(kind, index,
                ((Number) Files.getAttribute(path, "unix:dev")).longValue(),
                ((Number) Files.getAttribute(path, "unix:ino")).longValue()), uid,
                Integer.toUnsignedLong((Integer) Files.getAttribute(path, "unix:gid")), 0700);
    }

    private record NativeSources(ZKRegistrationManager manager, ZKProfileStartupRequirementsStore requirements) { }

    private NativeSources realSources() throws Exception {
        cluster = new ZooKeeperUtil();
        cluster.startCluster();
        var connected = new CountDownLatch(1);
        client = new HeldReadClient(cluster.getZooKeeperConnectString(), connected);
        assertTrue(connected.await(10, TimeUnit.SECONDS));
        String ledgers = "/ledgers";
        String instancePath = ledgers + "/INSTANCEID";
        if (client.exists(instancePath, false) == null) {
            client.create(instancePath, "bootstrap-preparation".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        var instance = new String(client.getData(instancePath, false, null), StandardCharsets.UTF_8);
        var conf = classicConfiguration();
        var manager = new ZKRegistrationManager(conf, client, ledgers);
        var cookie = Cookie.generateCookie(conf).setInstanceId(instance).build();
        cookie.writeToRegistrationManager(manager, conf, Version.NEW);
        for (var member : manifest.classicRoots()) {
            cookie.writeToDirectory(member.directory().resolve("current").toFile());
        }
        var nativeCookie = client.exists(manager.getCookiePath(manifest.bookieId()), false);
        var digest = MessageDigest.getInstance("SHA-256").digest(manager.readCookie(manifest.bookieId()).getValue());
        var binding = new BookieRouteBinding(instance, client.exists(instancePath, false).getCzxid(),
                manifest.bookieId(), manifest.incarnation(), new UUID(0, 2), manifest.identity(),
                manifest.controlDirectory().toString(), new BookieRouteBinding.SourceCookie(
                        nativeCookie.getCzxid(), nativeCookie.getVersion(), digest),
                BookieRouteBinding.Phase.PREPARED, Optional.empty());
        nativeScope = manager.prepareBookieRouteBinding(binding).observation().orElseThrow();
        String requirementsRoot = ledgers + "/commissioned-test";
        client.create(requirementsRoot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var store = new ZKProfileStartupRequirementsStore(client, requirementsRoot, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5));
        var one = new Generation(1);
        var compatibility = new BookieCompatibilityFact(manifest.bookieId().toString(), manifest.incarnation(),
                EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, 1, List.of(1), manifest.identity(),
                one, one, one, 1, 1, BookieCompatibilityFact.MigrationState.FORMAT_READY, one);
        var devices = new RequiredDeviceManifest(manifest.identity(), one, List.of(
                new RequiredDeviceManifest.RequiredDeviceFact(manifest.arenas().get(0).id().toString(),
                        manifest.incarnation(), one, RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1,
                        one, List.of(1))));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                store.provision(compatibility, devices));
        writeMaterials(SegmentClassicHandoff.Requirements.from(
                store.read(manifest.bookieId().toString()).observation().orElseThrow()));
        return new NativeSources(manager, store);
    }

    private ServerConfiguration classicConfiguration() {
        var conf = new ServerConfiguration().setBookieId(manifest.bookieId().toString())
                .setJournalDirsName(new String[]{manifest.classicRoots().get(0).directory().toString()})
                .setLedgerDirNames(new String[]{manifest.classicRoots().get(1).directory().toString()});
        if (manifest.classicRoots().size() == 4) {
            conf.setJournalDirsName(new String[]{manifest.classicRoots().get(3).directory().toString(),
                    manifest.classicRoots().get(0).directory().toString()});
            conf.setIndexDirName(new String[]{manifest.classicRoots().get(2).directory().toString()});
        }
        return conf;
    }

    @After
    public void closeNativeFixture() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (cluster != null) {
                cluster.killCluster();
            }
        }
    }

    private SegmentClassicBootstrap prepare(NativeSources nativeSources, Duration timeout) {
        var trust = new SegmentClassicBootstrap.Trust(expected.deployment(),
                expected.programConfiguration(), expected.signerKeyId(), expected.trustedSigner(), oldUid, uid, uid);
        return new SegmentClassicBootstrap(nativeSources.manager(), nativeSources.requirements(),
                manifest, references, trust, timeout);
    }

    @Test(timeout = 30000)
    public void preparationBindsRealNativeInputsAndHoldsTheOriginalGuardWithoutStorageWrites() throws Exception {
        var nativeSources = realSources();
        Path journal = manifest.classicRoots().get(0).directory().resolve("current/1.txn");
        Path indexRoot = manifest.classicRoots().get(1).directory().resolve("current");
        ClassicClaimTestHistory.writeJournal(journal, 11);
        ClassicClaimTestHistory.writeIndex(indexRoot, 11);
        Path index = indexRoot.resolve(IndexPersistenceMgr.getLedgerName(11));
        byte[] journalBytes = Files.readAllBytes(journal);
        byte[] indexBytes = Files.readAllBytes(index);
        var preparation = prepare(nativeSources, Duration.ofSeconds(5));
        assertFalse(preparation.inputsBound());
        probe("AVAILABLE");
        try {
            preparation.initialize();
            assertTrue(preparation.inputsBound());
            probe("HELD");
            assertThrows(IOException.class, preparation::initialize);
            assertArrayEquals(journalBytes, Files.readAllBytes(journal));
            assertArrayEquals(indexBytes, Files.readAllBytes(index));
            assertEquals(nativeScope, nativeSources.manager().readBookieRouteBinding(manifest.bookieId())
                    .observation().orElseThrow());
            for (var root : manifest.classicRoots()) {
                Path marker = root.directory().resolve("current").resolve(SegmentClassicRootBindings.FILE_NAME);
                assertFalse(Files.exists(marker));
            }
            assertFalse(Files.exists(manifest.controlDirectory().resolve(SegmentClassicRootBindings.FILE_NAME)));
            assertFalse(Files.exists(manifest.controlDirectory().resolve("bookie-control")));
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
        assertFalse(preparation.inputsBound());
        probe("AVAILABLE");
        assertTrue(client.getState().isAlive());
    }

    private record SealedScope(NativeSources sources, IsolatedSegmentConfig control,
                               SegmentBookieControlLog.RouteBinding route,
                               BookieRouteBinding.BootstrapSeal localSeal,
                               BookieRouteBinding.Observation enabled) { }

    private SealedScope sealScope(boolean exactSeal) throws Exception {
        Path controlRoot = manifest.controlDirectory();
        Files.delete(controlRoot.resolve("bookie-control.lock"));
        Files.delete(controlRoot);
        var classic = new ArrayList<>(manifest.classicRoots());
        classic.set(1, new SegmentArenaManifest.ClassicRoot(classic.get(1).directory(),
                Set.of(SegmentArenaManifest.ClassicRole.LEDGER)));
        var index = additionalClassicRoot(controlRoot.resolveSibling("classic-2"),
                SegmentArenaManifest.ClassicRole.INDEX);
        var secondJournal = additionalClassicRoot(controlRoot.resolveSibling("classic-3"),
                SegmentArenaManifest.ClassicRole.JOURNAL);
        classic.add(index);
        classic.add(secondJournal);
        manifest = new SegmentArenaManifest(manifest.incarnation(), manifest.bookieId(), manifest.arenas(),
                controlRoot, manifest.shardArenas(), classic);
        Path journal = manifest.classicRoots().get(0).directory().resolve("current/1.txn");
        Path ledger = manifest.classicRoots().get(1).directory().resolve("current");
        ClassicClaimTestHistory.writeJournal(journal, 97);
        long markedOffset = Files.size(journal);
        ClassicClaimTestHistory.writeJournal(journal, 97, 13, 13);
        // classic-0 is configured second, independently of the manifest's physical sort order.
        Files.write(ledger.resolve("lastMark.1"), ByteBuffer.allocate(16).putLong(1).putLong(markedOffset).array());
        ClassicClaimTestHistory.writeJournal(secondJournal.directory().resolve("current/7.txn"), 13);
        ClassicClaimTestHistory.writeIndex(index.directory().resolve("current"), 11);
        var config = new IsolatedSegmentConfig(controlRoot, 1, 1, 1, 16384, 4096, 1024, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        var route = new SegmentBookieControlLog.RouteBinding(nativeScope.binding().bindingId(), manifest.identity());
        var io = new SegmentFileIo();
        BookieRouteBinding.BootstrapSeal seal;
        NativeSources sources;
        try (var bookie = SegmentBookieControlLog.createManifestControl(config, manifest, manifest.identity(), 32768,
                new SecretKeySpec(filled(32, 10), "AES"), new ProfileDescriptorValidator((i, v) -> false,
                        (i, v) -> false), io, route)) {
            var arena = manifest.arenaConfig(config, 0);
            Files.delete(arena.directory().resolve("isolated.lock"));
            Files.delete(arena.directory().resolve("arena-control.lock"));
            Files.delete(arena.directory());
            try (var files = IsolatedSegmentFiles.create(arena, SegmentFileHeader.forArena(manifest, 0));
                 var arenaControl = SegmentArenaControlLog.open(arena, SegmentFileHeader.forArena(manifest, 0),
                         32768)) {
                assertEquals(-1, arenaControl.appliedThrough());
            }
            Files.setPosixFilePermissions(arena.directory(), PosixFilePermissions.fromString("rwx------"));
            sources = realSources();
            try (var roots = new SegmentClassicRootBindings(classicConfiguration(), sources.manager(), manifest,
                    nativeScope)) {
                roots.openLocks(io);
                roots.createMarkers(bookie, io);
                var indexClaim = bookie.prepareBootstrapClaim(11).command();
                var journalClaim = bookie.prepareBootstrapClaim(13).command();
                var complete = new SegmentBookieControlLog.RouteChange(
                        SegmentBookieControlLog.RouteAction.BOOTSTRAP_COMPLETE, 1, route, new UUID(0, 20));
                var results = bookie.applyDurable(bookie.appendChangesDurably(List.of(indexClaim, journalClaim,
                        complete)));
                assertEquals(List.of(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        SegmentBookieControlLog.Status.APPLIED_DURABLE),
                        results.stream().map(SegmentBookieControlLog.Result::status).toList());
                seal = bookie.bootstrapSeal().orElseThrow();
            }
        }
        var published = exactSeal ? seal
                : new BookieRouteBinding.BootstrapSeal(new UUID(0, 21), seal.sequence(), seal.appliedCut());
        var enabled = sources.manager().completeBookieRouteBinding(nativeScope, published).observation().orElseThrow();
        return new SealedScope(sources, config, route, seal, enabled);
    }

    private static SegmentArenaManifest.ClassicRoot additionalClassicRoot(Path path,
            SegmentArenaManifest.ClassicRole role) throws IOException {
        Path current = directory(directory(path).resolve("current"));
        privateFile(current.resolve(SegmentClassicRootBindings.LOCK_NAME), new byte[0]);
        return new SegmentArenaManifest.ClassicRoot(path, Set.of(role));
    }

    private SegmentBookieControlLog.Owned coldControl(SealedScope scope, SegmentFileIo io) throws IOException {
        return SegmentBookieControlLog.ownedExisting(scope.control(), manifest, manifest.identity(), 32768,
                new SecretKeySpec(filled(32, 10), "AES"), new ProfileDescriptorValidator((i, v) -> false,
                        (i, v) -> false), io, scope.route());
    }

    private void verifyColdEnabled(SealedScope scope, SegmentClassicBootstrap preparation)
            throws Exception {
        withColdEnabled(scope, preparation, (roots, arenas, control, io) -> {
            var bookie = control.log();
            assertEquals(Optional.of(scope.localSeal()), bookie.bootstrapSeal());
            var claim = bookie.prepareBootstrapClaim(11).applied().orElseThrow();
            assertEquals(0, claim.sequence());
            preparation.verifyEnabled(roots, arenas, control);
            assertEquals(claim, bookie.prepareBootstrapClaim(11).applied().orElseThrow());
            assertEquals(1, bookie.prepareBootstrapClaim(13).applied().orElseThrow().sequence());
            assertEquals(scope.localSeal().sequence(), bookie.appliedThrough());
            assertThrows(IOException.class, () -> bookie.prepareBootstrapClaim(97));
            assertEquals(0, io.writes);
        });
    }

    @FunctionalInterface
    private interface ColdEnabledOperation {
        void run(SegmentClassicRootBindings roots, SegmentClassicArenaBindings arenas,
                 SegmentBookieControlLog.Owned control, SegmentFileIo io) throws Exception;
    }

    private void withColdEnabled(SealedScope scope, SegmentClassicBootstrap preparation, ColdEnabledOperation action)
            throws Exception {
        assertEquals(scope.enabled(), preparation.boundNative());
        var io = new SegmentFileIo();
        try (var roots = new SegmentClassicRootBindings(classicConfiguration(), scope.sources().manager(), manifest,
                preparation.boundNative())) {
            roots.openLocks(io);
            var control = coldControl(scope, io);
            var arenas = new SegmentClassicArenaBindings(manifest, manifest.identity(), 32768);
            try {
                control.initialize();
                arenas.initialize();
                action.run(roots, arenas, control, io);
            } finally {
                assertTrue(arenas.closeAndDrain().drained());
                assertTrue(control.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void enabledInventoryCloseWaitsForTheActualIndexReaderAndRejectsLateSuccess() throws Exception {
        var scope = sealScope(true);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        Path index = manifest.classicRoots().get(2).directory().resolve("current")
                .resolve(IndexPersistenceMgr.getLedgerName(11));
        try {
            preparation.initialize();
            withColdEnabled(scope, preparation, (roots, arenas, control, io) -> {
                var entered = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var opened = new AtomicReference<FileChannel>();
                var task = new FutureTask<Void>(() -> {
                    try (var opens = mockStatic(FileChannel.class, invocation -> {
                        Object actual = invocation.callRealMethod();
                        if (invocation.getMethod().getName().equals("open")
                                && invocation.getMethod().getParameterCount() == 2
                                && index.equals(invocation.getArgument(0))) {
                            opened.set((FileChannel) actual);
                            entered.countDown();
                            assertTrue(release.await(10, TimeUnit.SECONDS));
                        }
                        return actual;
                    })) {
                        preparation.verifyEnabled(roots, arenas, control);
                    }
                    return null;
                });
                var scanner = new Thread(task, "held-classic-bootstrap-index");
                scanner.start();
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    assertTrue(opened.get().isOpen());
                    assertFalse(preparation.closeAndDrain().drained());
                    assertFalse(preparation.inputsBound());
                    assertTrue(arenas.verified());
                    probe("HELD");
                } finally {
                    release.countDown();
                    scanner.join(5000);
                    assertFalse(scanner.isAlive());
                }
                var failure = assertThrows(ExecutionException.class, () -> task.get(5, TimeUnit.SECONDS)).getCause();
                assertTrue(failure.getMessage(), failure.getMessage().contains("preparation is closing"));
                assertFalse(opened.get().isOpen());
                assertEquals(0, io.writes);
                probe("HELD");
            });
        } finally {
            var result = preparation.closeAndDrain();
            assertTrue(result.drained());
            assertTrue(result.inventory().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void enabledInventoryKeepsTheSupervisorWhenTheFirstIndexCloseFails() throws Exception {
        var scope = sealScope(true);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        Path index = manifest.classicRoots().get(2).directory().resolve("current")
                .resolve(IndexPersistenceMgr.getLedgerName(11));
        var firstClose = new IOException("Classic bootstrap first index close failed");
        var opened = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try {
            preparation.initialize();
            withColdEnabled(scope, preparation, (roots, arenas, control, io) -> {
                try (var opens = mockStatic(FileChannel.class, invocation -> {
                    Object actual = invocation.callRealMethod();
                    if (invocation.getMethod().getName().equals("open")
                            && invocation.getMethod().getParameterCount() == 2
                            && index.equals(invocation.getArgument(0))) {
                        var wrapped = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                        wrapped.closeFailure = firstClose;
                        opened.set(wrapped);
                        return wrapped;
                    }
                    return actual;
                })) {
                    assertSame(firstClose,
                            assertThrows(IOException.class, () -> preparation.verifyEnabled(roots, arenas, control)));
                }
                for (int i = 0; i < 2; i++) {
                    var result = preparation.closeAndDrain();
                    assertFalse(result.drained());
                    assertSame(firstClose, result.initializationFailure());
                    assertSame(firstClose, result.inventory().closeFailure());
                    assertTrue(result.sources().drained());
                    assertNull(result.materials());
                    assertTrue(opened.get().actual.isOpen());
                    assertEquals(1, opened.get().closes.get());
                    probe("HELD");
                }
            });
        } finally {
            if (opened.get() != null) {
                opened.get().actual.close();
                var member = SegmentClassicBootstrap.class.getDeclaredField("materials");
                member.setAccessible(true);
                guard((SegmentClassicHandoffMaterials) member.get(preparation)).close();
            } else {
                preparation.closeAndDrain();
            }
        }
        assertFalse(preparation.closeAndDrain().drained());
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationReusesOriginalSignedProofAfterNativeEnable() throws Exception {
        var scope = sealScope(true);
        byte[] proof = Files.readAllBytes(references.proof());
        byte[] intent = Files.readAllBytes(intentPath);
        Path journal = manifest.classicRoots().get(0).directory().resolve("current/1.txn");
        Path index = manifest.classicRoots().get(2).directory().resolve("current")
                .resolve(IndexPersistenceMgr.getLedgerName(11));
        byte[] journalBytes = Files.readAllBytes(journal);
        byte[] indexBytes = Files.readAllBytes(index);
        Path controlFile = manifest.controlDirectory().resolve("bookie-control");
        byte[] controlBytes = Files.readAllBytes(controlFile);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            assertTrue(preparation.inputsBound());
            probe("HELD");
            verifyColdEnabled(scope, preparation);
            probe("HELD");
            assertEquals(scope.enabled(), scope.sources().manager().readBookieRouteBinding(manifest.bookieId())
                    .observation().orElseThrow());
            assertArrayEquals(proof, Files.readAllBytes(references.proof()));
            assertArrayEquals(intent, Files.readAllBytes(intentPath));
            assertArrayEquals(journalBytes, Files.readAllBytes(journal));
            assertArrayEquals(indexBytes, Files.readAllBytes(index));
            assertArrayEquals(controlBytes, Files.readAllBytes(controlFile));
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void enabledVerificationRejectsARealNativeSealThatDiffersFromTheRecoveredControl() throws Exception {
        var scope = sealScope(false);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            var failure = assertThrows(IOException.class, () -> verifyColdEnabled(scope, preparation));
            assertTrue(failure.getMessage(), failure.getMessage().contains("retained control seal differ"));
            assertFalse(preparation.inputsBound());
            assertThrows(IOException.class, preparation::boundNative);
            probe("HELD");
            assertSame(failure, preparation.closeAndDrain().initializationFailure());
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void enabledVerificationRejectsAnIndexWithoutAnOriginalClassicClaim() throws Exception {
        var scope = sealScope(true);
        Path index = manifest.classicRoots().get(2).directory().resolve("current");
        ClassicClaimTestHistory.writeIndex(index, 19);
        Path unclaimed = index.resolve(IndexPersistenceMgr.getLedgerName(19));
        byte[] original = Files.readAllBytes(unclaimed);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            var failure = assertThrows(IOException.class, () -> verifyColdEnabled(scope, preparation));
            assertTrue(failure.getMessage(), failure.getMessage().contains("ledger 19"));
            assertFalse(preparation.inputsBound());
            assertArrayEquals(original, Files.readAllBytes(unclaimed));
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void enabledVerificationRejectsAnUnclaimedLedgerInTheRequiredJournalSuffix() throws Exception {
        var scope = sealScope(true);
        Path unclaimed = manifest.classicRoots().get(3).directory().resolve("current/8.txn");
        ClassicClaimTestHistory.writeJournal(unclaimed, 19);
        byte[] original = Files.readAllBytes(unclaimed);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            var failure = assertThrows(IOException.class, () -> verifyColdEnabled(scope, preparation));
            assertTrue(failure.getMessage(), failure.getMessage().contains("ledger 19"));
            assertFalse(preparation.inputsBound());
            assertArrayEquals(original, Files.readAllBytes(unclaimed));
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void enabledVerificationRejectsAClaimAppliedAfterTheOriginalSeal() throws Exception {
        var scope = sealScope(true);
        var control = coldControl(scope, new SegmentFileIo());
        try {
            control.initialize();
            var bookie = control.log();
            var late = new SegmentBookieControlLog.RouteChange(SegmentBookieControlLog.RouteAction.CLAIM_CLASSIC,
                    19, scope.route(), new UUID(0, 23));
            var receipt = bookie.applyDurable(bookie.appendChangesDurably(List.of(late))).get(0);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, receipt.status());
            assertTrue(receipt.sequence() > scope.localSeal().sequence());
        } finally {
            assertTrue(control.closeAndDrain().drained());
        }
        Path index = manifest.classicRoots().get(2).directory().resolve("current");
        ClassicClaimTestHistory.writeIndex(index, 19);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            var failure = assertThrows(IOException.class, () -> verifyColdEnabled(scope, preparation));
            assertEquals("Original bootstrap cut does not include the claim for ledger 19", failure.getMessage());
            assertFalse(preparation.inputsBound());
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void enabledVerificationRejectsRegistrationAfterTheOriginalProofWasBound() throws Exception {
        var scope = sealScope(true);
        var preparation = prepare(scope.sources(), Duration.ofSeconds(5));
        try {
            preparation.initialize();
            String registration = "/ledgers/available/readonly/" + manifest.bookieId();
            client.create(registration, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            var failure = assertThrows(IOException.class, () -> verifyColdEnabled(scope, preparation));
            assertTrue(failure.getMessage(), failure.getMessage().contains("still has a Bookie registration"));
            assertFalse(preparation.inputsBound());
            assertNotNull(client.exists(registration, false));
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void preparationReadsAnUnmarkedPreparedScopeWithNoClassicGuards() throws Exception {
        var nativeSources = realSources();
        var classicGuards = manifest.classicRoots().stream()
                .map(root -> root.directory().resolve("current").resolve(SegmentClassicRootBindings.LOCK_NAME))
                .toList();
        for (Path path : classicGuards) {
            Files.delete(path);
        }
        var preparation = prepare(nativeSources, Duration.ofSeconds(5));
        try {
            preparation.initialize();
            assertTrue(preparation.inputsBound());
            probe("HELD");
            for (Path path : classicGuards) {
                assertTrue(Files.notExists(path, LinkOption.NOFOLLOW_LINKS));
                assertTrue(Files.notExists(path.resolveSibling(SegmentClassicRootBindings.FILE_NAME)));
            }
            assertTrue(Files.notExists(manifest.controlDirectory().resolve(SegmentClassicRootBindings.FILE_NAME)));
            assertTrue(Files.notExists(manifest.controlDirectory().resolve("bookie-control")));
            assertEquals(nativeScope, nativeSources.manager().readBookieRouteBinding(manifest.bookieId())
                    .observation().orElseThrow());
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test
    public void missingClassicGuardWithAnyBindingMarkerOrMissingCurrentIsRejected() throws Exception {
        var firstCurrent = manifest.classicRoots().get(0).directory().resolve("current");
        var otherCurrent = manifest.classicRoots().get(1).directory().resolve("current");
        Path missing = firstCurrent.resolve(SegmentClassicRootBindings.LOCK_NAME);
        Files.delete(missing);
        for (Path parent : List.of(manifest.controlDirectory(), otherCurrent)) {
            Path marker = parent.resolve(SegmentClassicRootBindings.FILE_NAME);
            Files.createSymbolicLink(marker, parent.resolve("unreadable-marker-target"));
            rejected();
            assertTrue(Files.notExists(missing, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isSymbolicLink(marker));
            Files.delete(marker);
        }
        Path marker = otherCurrent.resolve(SegmentClassicRootBindings.FILE_NAME);
        try (var opens = whileOpeningKey(() -> privateFile(marker, new byte[]{1}))) {
            rejected();
        }
        assertArrayEquals(new byte[]{1}, Files.readAllBytes(marker));
        Files.delete(marker);
        Files.delete(firstCurrent);
        rejected();
        assertTrue(Files.notExists(firstCurrent, LinkOption.NOFOLLOW_LINKS));
        Files.createSymbolicLink(firstCurrent, directory(firstCurrent.resolveSibling("aliased-current")));
        rejected();
        assertTrue(Files.isSymbolicLink(firstCurrent));
    }

    @Test
    public void existingClassicGuardMustBeRegularAndMustNotAliasMaterial() throws Exception {
        Path classic = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        Path saved = classic.resolveSibling("saved-guard");
        Files.move(classic, saved);
        for (Path target : List.of(saved, saved.resolveSibling("absent"))) {
            Files.createSymbolicLink(classic, target);
            rejected();
            assertTrue(Files.isSymbolicLink(classic));
            Files.delete(classic);
        }
        Path proof = references.proof();
        Files.move(proof, classic);
        try (var lock = SegmentDataFileLock.existingControlOwner(classic)) {
            lock.initialize();
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                if (invocation.getMethod().getName().equals("open")
                        && (classic.equals(invocation.getArgument(0)) || proof.equals(invocation.getArgument(0)))) {
                    throw new AssertionError("Material must not open a Classic guard inode");
                }
                return invocation.callRealMethod();
            })) {
                Files.createLink(proof, classic);
                rejected();
                Files.delete(proof);
                references = new SegmentClassicHandoffMaterials.References(
                        references.directory(), classic, references.policy(), references.aesKey());
                rejected();
            }
            probe(classic, "HELD");
        }
        probe(classic, "AVAILABLE");
    }

    @Test
    public void requiredGuardsAndInconclusiveClassicLookupsDoNotQualifyAsMissingLeaves() throws Exception {
        var arena = manifest.arenas().get(0).directory();
        for (Path lock : List.of(manifest.controlDirectory().resolve("bookie-control.lock"),
                arena.resolve("isolated.lock"), arena.resolve("arena-control.lock"))) {
            Path saved = lock.resolveSibling("saved-guard");
            Files.move(lock, saved);
            try {
                rejected();
                assertTrue(Files.notExists(lock, LinkOption.NOFOLLOW_LINKS));
            } finally {
                Files.move(saved, lock);
            }
        }
        Path classic = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        Files.delete(classic);
        for (IOException unavailable : List.of(new AccessDeniedException(classic.toString()),
                new NoSuchFileException(classic.getParent().toString()))) {
            var materialOwner = owner();
            try (var reads = mockStatic(Files.class, invocation -> {
                if (invocation.getMethod().getName().equals("readAttributes")
                        && classic.equals(invocation.getArgument(0))
                        && invocation.getArgument(1) == BasicFileAttributes.class) {
                    throw unavailable;
                }
                return invocation.callRealMethod();
            })) {
                assertSame(unavailable, assertThrows(IOException.class, materialOwner::initialize));
            } finally {
                assertTrue(materialOwner.closeAndDrain().drained());
            }
            assertTrue(Files.notExists(classic, LinkOption.NOFOLLOW_LINKS));
        }
    }

    @Test
    public void aGuardAppearingBetweenMaterialReadsParticipatesInTheNextAliasCheck() throws Exception {
        Path classic = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        Files.delete(classic);
        Path originalKey = references.aesKey();
        Path proof = references.proof();
        references = new SegmentClassicHandoffMaterials.References(
                references.directory(), proof, references.policy(), classic);
        var materialOwner = owner();
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            String name = invocation.getMethod().getName();
            var parameters = invocation.getMethod().getParameterTypes();
            Object path = invocation.getArgument(0);
            if (isMaterialOpen(proof, name, parameters, path)) {
                Files.move(originalKey, classic);
            }
            if (isKeyOpen(name, parameters, path)) {
                throw new AssertionError("Newly observed Classic guard must not open as key material");
            }
            return invocation.callRealMethod();
        })) {
            var failure = assertThrows(IOException.class, materialOwner::initialize);
            assertTrue(failure.getMessage().contains("aliases a Classic guard"));
            assertThrows(IOException.class, materialOwner::parsed);
            assertArrayEquals(filled(32, 10), Files.readAllBytes(classic));
        } finally {
            assertTrue(materialOwner.closeAndDrain().drained());
        }
    }

    @Test
    public void aPreviouslyObservedGuardCannotDisappearOrBeReplacedBeforeMaterialPublication() throws Exception {
        Path classic = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        Path saved = classic.resolveSibling("original-guard");
        for (boolean replace : List.of(false, true)) {
            var materialOwner = owner();
            try (var opens = whileOpeningKey(() -> {
                Files.move(classic, saved);
                if (replace) {
                    privateFile(classic, new byte[0]);
                }
            })) {
                var failure = assertThrows(IOException.class, materialOwner::initialize);
                assertTrue(failure.getMessage().contains("Observed Classic guard or its parent changed"));
                assertThrows(IOException.class, materialOwner::parsed);
            } finally {
                assertTrue(materialOwner.closeAndDrain().drained());
            }
            Files.deleteIfExists(classic);
            Files.move(saved, classic);
        }
    }

    @Test(timeout = 30000)
    public void preparationRechecksClassicGuardsAfterTheSecondNativeRead() throws Exception {
        var preparation = prepare(realSources(), Duration.ofSeconds(5));
        Path classic = manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME);
        var task = new FutureTask<Void>(() -> {
            try (var opens = whileOpeningKey(() -> client.holdNextRead.set(true))) {
                preparation.initialize();
            }
            return null;
        });
        var reader = new Thread(task, "bootstrap-classic-guard-recheck");
        reader.start();
        try {
            assertTrue(client.replyHeld.await(5, TimeUnit.SECONDS));
            Files.delete(classic);
            client.lateReply.getAndSet(null).run();
            var failure = assertThrows(ExecutionException.class, () -> task.get(5, TimeUnit.SECONDS)).getCause();
            assertTrue(failure.getMessage().contains("Observed Classic guard or its parent changed"));
            assertFalse(preparation.inputsBound());
            probe("HELD");
        } finally {
            Runnable reply = client.lateReply.getAndSet(null);
            if (reply != null) {
                reply.run();
            }
            reader.join(5000);
            assertFalse(reader.isAlive());
            assertTrue(preparation.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationRejectsRegistrationAndRecreatedRequirementsDuringTheMaterialRead() throws Exception {
        var nativeSources = realSources();
        String registration = "/ledgers/available/readonly/" + manifest.bookieId();
        var first = prepare(nativeSources, Duration.ofSeconds(5));
        try (var opens = whileOpeningKey(() -> client.create(registration, new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL))) {
            var failure = assertThrows(IOException.class, first::initialize);
            assertTrue(failure.getMessage(), failure.getMessage().contains("still has a Bookie registration"));
            assertFalse(first.inputsBound());
        } finally {
            assertTrue(first.closeAndDrain().drained());
        }
        assertNotNull(client.exists(registration, false));
        client.delete(registration, 0);

        var source = expected.requirements().source();
        String path = source.nodePath();
        String marker = path + "/birth-" + source.birthId();
        byte[] bytes = client.getData(path, false, null);
        var second = prepare(nativeSources, Duration.ofSeconds(5));
        try (var opens = whileOpeningKey(() -> client.multi(List.of(Op.delete(marker, 0), Op.delete(path, 0),
                Op.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(marker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))))) {
            var failure = assertThrows(IOException.class, second::initialize);
            assertTrue(failure.getMessage(), failure.getMessage().contains("changed under the supervisor guard"));
            assertFalse(second.inputsBound());
            assertNotEquals(source.birthZxid(), client.exists(path, false).getCzxid());
        } finally {
            assertTrue(second.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationRejectsAReplacementAtTheSameArenaPath() throws Exception {
        var nativeSources = realSources();
        var arena = manifest.arenas().get(0).directory();
        Files.move(arena, arena.resolveSibling("original-arena"));
        directory(arena);
        privateFile(arena.resolve("isolated.lock"), new byte[0]);
        privateFile(arena.resolve("arena-control.lock"), new byte[0]);
        var preparation = prepare(nativeSources, Duration.ofSeconds(5));
        try {
            var failure = assertThrows(IOException.class, preparation::initialize);
            assertTrue(failure.getMessage().contains("physical member differs"));
            assertFalse(preparation.inputsBound());
            probe("HELD");
        } finally {
            assertTrue(preparation.closeAndDrain().drained());
        }
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationCloseDuringActualMaterialReadRejectsLateSuccess() throws Exception {
        var preparation = prepare(realSources(), Duration.ofSeconds(5));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var task = new FutureTask<Void>(() -> {
            try (var opens = whileOpeningKey(() -> {
                assertThrows(IllegalStateException.class, preparation::closeAndDrain);
                entered.countDown();
                assertTrue(release.await(10, TimeUnit.SECONDS));
            })) {
                preparation.initialize();
            }
            return null;
        });
        var reader = new Thread(task, "bootstrap-material-preparation");
        reader.start();
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertFalse(preparation.closeAndDrain().drained());
            assertFalse(preparation.inputsBound());
            assertFalse(task.isDone());
            probe("HELD");
        } finally {
            release.countDown();
            reader.join(5000);
            assertFalse(reader.isAlive());
        }
        var failure = assertThrows(ExecutionException.class, () -> task.get(5, TimeUnit.SECONDS)).getCause();
        assertTrue(failure.getMessage(), failure.getMessage().contains("preparation is closing"));
        var drain = preparation.closeAndDrain();
        assertTrue(drain.drained());
        assertSame(failure, drain.initializationFailure());
        assertFalse(preparation.inputsBound());
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationRetainsSupervisorThroughTheOriginalLateZooKeeperCallback() throws Exception {
        var preparation = prepare(realSources(), Duration.ofSeconds(2));
        var task = new FutureTask<Void>(() -> {
            try (var opens = whileOpeningKey(() -> client.holdNextRead.set(true))) {
                preparation.initialize();
            }
            return null;
        });
        var reader = new Thread(task, "bootstrap-zookeeper-preparation");
        reader.start();
        try {
            assertTrue(client.replyHeld.await(5, TimeUnit.SECONDS));
            assertFalse(preparation.closeAndDrain().drained());
            assertFalse(preparation.inputsBound());
            var failure = assertThrows(ExecutionException.class, () -> task.get(5, TimeUnit.SECONDS)).getCause();
            var pending = preparation.closeAndDrain();
            assertFalse(pending.drained());
            assertEquals(1, pending.sources().pendingCallbacks());
            assertSame(failure, pending.sources().firstReadFailure());
            assertNull(pending.materials());
            probe("HELD");
        } finally {
            Runnable reply = client.lateReply.getAndSet(null);
            if (reply != null) {
                reply.run();
            }
            reader.join(5000);
            assertFalse(reader.isAlive());
            assertTrue(preparation.closeAndDrain().drained());
        }
        assertFalse(preparation.inputsBound());
        assertTrue(client.getState().isAlive());
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void preparationKeepsTheFirstMaterialCloseFailureAndSupervisor() throws Exception {
        var preparation = prepare(realSources(), Duration.ofSeconds(5));
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        var firstClose = new IOException("bootstrap key first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) result);
                fault.closeFailure = firstClose;
                captured.set(fault);
                return fault;
            }
            return result;
        })) {
            assertSame(firstClose, assertThrows(IOException.class, preparation::initialize));
        }
        try {
            var drain = preparation.closeAndDrain();
            assertFalse(drain.drained());
            assertTrue(drain.sources().drained());
            assertSame(firstClose, drain.initializationFailure());
            assertSame(firstClose, drain.materials().files().closeFailure());
            assertSame(drain, preparation.closeAndDrain());
            assertEquals(1, captured.get().closes.get());
            assertFalse(preparation.inputsBound());
            assertTrue(client.getState().isAlive());
            probe("HELD");
        } finally {
            captured.get().actual.close();
            var member = SegmentClassicBootstrap.class.getDeclaredField("materials");
            member.setAccessible(true);
            guard((SegmentClassicHandoffMaterials) member.get(preparation)).close();
        }
        assertFalse(preparation.closeAndDrain().drained());
        probe("AVAILABLE");
    }

    @FunctionalInterface
    private interface KeyOpening {
        void run() throws Exception;
    }

    private MockedStatic<FileChannel> whileOpeningKey(KeyOpening action) {
        return mockStatic(FileChannel.class, invocation -> {
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
                action.run();
            }
            return invocation.callRealMethod();
        });
    }

    private static final class HeldReadClient extends ZooKeeper {
        final AtomicBoolean holdNextRead = new AtomicBoolean();
        final AtomicReference<Runnable> lateReply = new AtomicReference<>();
        final CountDownLatch replyHeld = new CountDownLatch(1);

        HeldReadClient(String connect, CountDownLatch connected) throws IOException {
            super(connect, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
        }

        @Override
        public void getData(String path, boolean watch, DataCallback callback, Object context) {
            boolean hold = holdNextRead.getAndSet(false);
            super.getData(path, watch, (code, returnedPath, original, bytes, stat) -> {
                if (hold) {
                    lateReply.set(() -> callback.processResult(code, returnedPath, original, bytes, stat));
                    replyHeld.countDown();
                } else {
                    callback.processResult(code, returnedPath, original, bytes, stat);
                }
            }, context);
        }
    }

    private SegmentClassicHandoffMaterials owner() {
        return new SegmentClassicHandoffMaterials(references, expected, manifest);
    }

    private void rejected() throws Exception {
        var owner = owner();
        assertThrows(IOException.class, owner::initialize);
        assertThrows(IOException.class, owner::parsed);
        var closed = owner.closeAndDrain();
        assertTrue(closed.drained());
        assertNotNull(closed.initializationFailure());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void ownedInputsRetainSupervisorUntilCloseAndAllowTheSameOriginalPreparedProjection() throws Exception {
        byte[] proof = Files.readAllBytes(references.proof());
        byte[] key = Files.readAllBytes(references.aesKey());
        var owner = owner();
        owner.initialize();
        assertArrayEquals(key, owner.parsed().key().getEncoded());
        assertEquals(manifest.identity(), owner.parsed().proof().operation().prepared().binding().manifest());
        probe("HELD");
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class, owner::parsed);
        probe("AVAILABLE");
        var enabled = nativeScope.binding().enabled(
                new BookieRouteBinding.BootstrapSeal(new UUID(0, 20), 0, -1));
        expected = new SegmentClassicHandoffMaterials.Expected(expected.deployment(), expected.programConfiguration(),
                expected.signerKeyId(), expected.trustedSigner(), new BookieRouteBinding.Observation(enabled,
                        nativeScope.birthId(), nativeScope.birthZxid(), 105, 1),
                expected.requirements(), oldUid, uid, uid);
        owner = owner();
        owner.initialize();
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(proof, Files.readAllBytes(references.proof()));
        assertArrayEquals(key, Files.readAllBytes(references.aesKey()));
        Files.delete(manifest.classicRoots().get(0).directory().resolve("current")
                .resolve(SegmentClassicRootBindings.LOCK_NAME));
        rejected();
    }

    @Test
    public void supervisorAndIntentNamesCannotChangeWithAttemptOrBinding() throws Exception {
        var b = nativeScope.binding();
        var other = new BookieRouteBinding(b.clusterId(), b.clusterBirthZxid(), b.bookieId(), b.incarnation(),
                new UUID(0, 99), b.manifest(), b.controlDirectory(), b.sourceCookie(), b.phase(), b.seal());
        assertEquals(SegmentClassicHandoffMaterials.scopeName(b), SegmentClassicHandoffMaterials.scopeName(other));
        assertNotEquals(SegmentClassicHandoffMaterials.scopeName(b), SegmentClassicHandoffMaterials.scopeName(
                new BookieRouteBinding(b.clusterId(), b.clusterBirthZxid() + 1, b.bookieId(), b.incarnation(),
                        b.bindingId(), b.manifest(), b.controlDirectory(), b.sourceCookie(), b.phase(), b.seal())));
        Path moved = references.directory().resolveSibling("moved-materials");
        Files.move(references.directory(), moved);
        var owner = owner(); // The constructor still does not touch the missing path.
        assertFalse(Files.exists(references.directory()));
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertFalse(Files.exists(references.directory()));
    }

    @Test
    public void missingGuardOrIntentWrongSizeAndBadPrivateModeNeverCreateOrRepairInputs() throws Exception {
        for (Path path : List.of(guardPath, intentPath, references.aesKey())) {
            Path held = path.resolveSibling("held");
            Files.move(path, held);
            rejected();
            assertFalse(Files.exists(path));
            Files.move(held, path);
        }
        privateFile(references.aesKey(), filled(31, 10));
        rejected();
        privateFile(references.aesKey(), filled(32, 10));
        Files.setPosixFilePermissions(references.aesKey(), PosixFilePermissions.fromString("rw-r--r--"));
        rejected();
        Files.setPosixFilePermissions(references.aesKey(), PosixFilePermissions.fromString("rw-------"));
        privateFile(references.proof(), new byte[65537]);
        rejected();
        assertEquals(65537, Files.size(references.proof()));
    }

    @Test
    public void aliasesAndWritableParentChainsAreRejectedBeforeDataPublication() throws Exception {
        Path key = references.aesKey();
        Path saved = key.resolveSibling("saved-key");
        Files.move(key, saved);
        Files.createSymbolicLink(key, saved);
        rejected();
        Files.delete(key);
        Files.createLink(key, saved);
        rejected();
        Files.delete(key);
        Files.move(saved, key);
        Files.setPosixFilePermissions(references.directory(), PosixFilePermissions.fromString("rwxrwxrwx"));
        rejected();
        Files.setPosixFilePermissions(references.directory(), PosixFilePermissions.fromString("rwx------"));
        Path alias = references.directory().resolveSibling("alias");
        Files.createSymbolicLink(alias, references.directory());
        references = new SegmentClassicHandoffMaterials.References(alias, alias.resolve("proof"),
                alias.resolve("policy"), alias.resolve("key"));
        rejected();
    }

    @Test
    public void wrongActualUidChangedNativeBirthOrProgramAndWrongKeyCannotPublishMaterials()
            throws Exception {
        var original = expected;
        long anotherUid = uid == 0 ? 1 : 0;
        expected = new SegmentClassicHandoffMaterials.Expected(
                original.deployment(), original.programConfiguration(),
                original.signerKeyId(), original.trustedSigner(), nativeScope, original.requirements(),
                oldUid, anotherUid, uid);
        var wrongUid = owner();
        var wrongUidFailure = assertThrows(IOException.class, wrongUid::initialize);
        assertTrue(wrongUidFailure.getMessage().contains("actual target UID"));
        assertTrue(wrongUid.closeAndDrain().drained());
        probe("AVAILABLE");
        expected = new SegmentClassicHandoffMaterials.Expected(expected.deployment(), digest(77),
                original.signerKeyId(), original.trustedSigner(), nativeScope, original.requirements(),
                oldUid, uid, uid);
        rejected();
        expected = new SegmentClassicHandoffMaterials.Expected(
                original.deployment(), original.programConfiguration(),
                original.signerKeyId(), original.trustedSigner(), new BookieRouteBinding.Observation(
                        nativeScope.binding(), new UUID(0, 77), 107, 107, 0),
                original.requirements(), oldUid, uid, uid);
        rejected();
        expected = original;
        privateFile(references.aesKey(), filled(32, 99));
        rejected();
        privateFile(references.aesKey(), filled(32, 10));
        byte[] intent = Files.readAllBytes(intentPath);
        intent[intent.length - 1] ^= 1;
        privateFile(intentPath, intent);
        rejected();
    }

    @Test
    public void replacementAfterOpenIsDetectedAndTheOldReadBufferIsErased() throws Exception {
        var owner = owner();
        var observed = new AtomicReference<byte[]>();
        try (var reads = mockStatic(SegmentFileIo.class, invocation -> {
            if (invocation.getMethod().getName().equals("readFully")) {
                ByteBuffer buffer = invocation.getArgument(1);
                if (buffer.capacity() == 32) {
                    observed.set(buffer.array());
                }
            }
            return invocation.callRealMethod();
        }); var opens = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
                Path saved = references.aesKey().resolveSibling("old-key");
                Files.move(references.aesKey(), saved);
                privateFile(references.aesKey(), filled(32, 10));
            }
            return result;
        })) {
            assertThrows(IOException.class, owner::initialize);
        }
        assertArrayEquals(new byte[32], observed.get());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void failedNativeCloseCannotExposeKeyOrReleaseSupervisorAndKeepsTheFirstFailure() throws Exception {
        var owner = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        var buffer = new AtomicReference<byte[]>();
        IOException failure = new IOException("key reader native close");
        try (var reads = mockStatic(SegmentFileIo.class, invocation -> {
            if (invocation.getMethod().getName().equals("readFully")) {
                ByteBuffer value = invocation.getArgument(1);
                if (value.capacity() == 32) {
                    buffer.set(value.array());
                }
            }
            return invocation.callRealMethod();
        }); var opens = mockStatic(FileChannel.class, invocation -> {
            Object result = invocation.callRealMethod();
            if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                    invocation.getArgument(0))) {
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
            assertArrayEquals(new byte[32], buffer.get());
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThrows(IOException.class, owner::parsed);
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.initializationFailure());
                assertSame(failure, result.files().closeFailure());
                assertEquals(1, captured.get().closes.get());
                assertTrue(captured.get().actual.isOpen());
                probe("HELD");
            }
        } finally {
            captured.get().actual.close();
            guard(owner).close(); // Actual fixture cleanup; original failed file result remains unresolved.
        }
        assertFalse(owner.closeAndDrain().drained());
        probe("AVAILABLE");
    }

    @Test(timeout = 30000)
    public void actualMaterialOpenCannotDrainItselfAndExternalCloseWaitsForItsReturn() throws Exception {
        var owner = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var initializer = new FutureTask<Void>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                if (isKeyOpen(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes(),
                        invocation.getArgument(0))) {
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
        var reader = new Thread(initializer);
        var closer = new Thread(drained);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(drained.isDone());
            probe("HELD");
            release.countDown();
            initializer.get(5, TimeUnit.SECONDS);
            assertTrue(drained.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
            assertTrue(owner.closeAndDrain().drained());
        }
        assertThrows(IOException.class, owner::parsed);
    }

    private boolean isKeyOpen(String name, Class<?>[] parameters, Object path) {
        return isMaterialOpen(references.aesKey(), name, parameters, path);
    }

    private static boolean isMaterialOpen(Path material, String name, Class<?>[] parameters, Object path) {
        return name.equals("open") && parameters.length == 2 && parameters[1] == OpenOption[].class
                && material.equals(path);
    }

    private static SegmentDataFileLock guard(SegmentClassicHandoffMaterials owner) throws Exception {
        var field = SegmentClassicHandoffMaterials.class.getDeclaredField("guard");
        field.setAccessible(true);
        return (SegmentDataFileLock) field.get(owner);
    }

    private void probe(String expected) throws Exception {
        probe(guardPath, expected);
    }

    private static void probe(Path path, String expected) throws Exception {
        var process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                SegmentOwnedBookieControlLogTest.GuardProbe.class.getName(),
                path.toString()).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            String output = new String(
                    process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
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

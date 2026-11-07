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
package org.apache.bookkeeper.discover.profile;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

/** Borrowed ZooKeeper source reads and conditional completion against the actual native store. */
public class ZKClassicBootstrapSourcesTest extends ProfileZKTestBase {
    private static final BookieId TARGET = BookieId.parse(BOOKIE);
    private static final String CLUSTER = "bootstrap-cold-cluster";
    private ZKRegistrationManager manager;
    private ZKProfileStartupRequirementsStore requirements;
    private BookieRouteBinding binding;
    private BookieCompatibilityFact compatibility;
    private byte[] cookie;

    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    @Before
    public void sources() throws Exception {
        first.create(ROOT + "/INSTANCEID", CLUSTER.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.create(ROOT + "/available/readonly", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var conf = new ServerConfiguration().setBookieId(BOOKIE)
                .setJournalDirsName(new String[]{"/cold-fixture/journal"})
                .setLedgerDirNames(new String[]{"/cold-fixture/ledger"});
        manager = new ZKRegistrationManager(conf, first, ROOT);
        cookie = Cookie.generateCookie(conf).setInstanceId(CLUSTER).build().toString()
                .getBytes(StandardCharsets.UTF_8);
        manager.writeCookie(TARGET, new Versioned<>(cookie, Version.NEW));
        Stat nativeCookie = first.exists(ROOT + "/cookies/" + TARGET, false);
        binding = new BookieRouteBinding(CLUSTER, first.exists(ROOT + "/INSTANCEID", false).getCzxid(), TARGET,
                new StorageIncarnation(bytes(16, 1)), new UUID(0, 9), new ManifestIdentity(bytes(32, 2)),
                "/cold-fixture/control", new BookieRouteBinding.SourceCookie(nativeCookie.getCzxid(),
                        nativeCookie.getVersion(), MessageDigest.getInstance("SHA-256").digest(cookie)),
                BookieRouteBinding.Phase.PREPARED, Optional.empty());
        assertEquals(BookieRouteBinding.WriteStatus.APPLIED, manager.prepareBookieRouteBinding(binding).status());
        first.create(ROOT + "/requirements", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        requirements = new ZKProfileStartupRequirementsStore(first, ROOT + "/requirements",
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        var one = new Generation(1);
        compatibility = new BookieCompatibilityFact(BOOKIE, binding.incarnation(), EngineProfile.SEGMENT_WAL_ENGINE,
                1, 0, 1, List.of(1), binding.manifest(), one, one, one, 1, 1,
                BookieCompatibilityFact.MigrationState.FORMAT_READY, one);
        var devices = new RequiredDeviceManifest(binding.manifest(), one,
                List.of(new RequiredDeviceManifest.RequiredDeviceFact("arena", binding.incarnation(), one,
                        RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1, one, List.of(1))));
        assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                requirements.provision(compatibility, devices));
    }

    private ZKProfileStartupRequirementsStore.ClassicBootstrapSources owner(Duration timeout) {
        return manager.borrowClassicBootstrapSources(requirements, TARGET, timeout);
    }

    private static void drained(ZKProfileStartupRequirementsStore.ClassicBootstrapSources owner) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> owner.closeAndDrain().drained());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void heapOnlyBorrowAndFreshPreparedEnabledReadsKeepOriginalSourcesAndDoNotCloseBorrowedClient()
            throws Exception {
        int before = first.reads.get();
        var owner = owner(Duration.ofSeconds(5));
        assertEquals(before, first.reads.get());
        var original = requirements.read(BOOKIE).observation().orElseThrow();
        var prepared = owner.read();
        assertEquals(binding, prepared.nativeBinding().binding());
        assertEquals(compatibility, prepared.commissionedCompatibility());
        assertEquals(original.sourceIdentity(), prepared.requirementsSource());
        assertEquals(original.requiredDevices(), prepared.requiredDevices());
        var seal = new BookieRouteBinding.BootstrapSeal(new UUID(0, 20), 4, 3);
        assertEquals(BookieRouteBinding.WriteStatus.APPLIED,
                manager.completeBookieRouteBinding(prepared.nativeBinding(), seal).status());
        var enabled = owner.read();
        assertEquals(binding.enabled(seal), enabled.nativeBinding().binding());
        assertEquals(prepared.nativeBinding().birthId(), enabled.nativeBinding().birthId());
        assertEquals(prepared.nativeBinding().birthZxid(), enabled.nativeBinding().birthZxid());
        assertEquals(1, enabled.nativeBinding().version());
        assertEquals(original.sourceIdentity(), enabled.requirementsSource());
        drained(owner);
        assertThrows(IOException.class, owner::read);
        assertTrue(first.getState().isAlive());
        assertEquals(ZKProfileStartupRequirementsStore.Status.FOUND, requirements.read(BOOKIE).status());
        assertArrayEquals(cookie, manager.readCookie(TARGET).getValue());
    }

    @Test
    public void foreignRequirementsClientAndNativeNamespaceCannotSupplyTheBorrowedSource() {
        var foreign = new ZKProfileStartupRequirementsStore(second, ROOT + "/requirements",
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        assertThrows(IllegalArgumentException.class,
                () -> manager.borrowClassicBootstrapSources(foreign, TARGET, Duration.ofSeconds(5)));
        var wrongRoot = new ZKBookieRouteBindingStore(first, ROOT + "/other",
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
        assertThrows(IllegalArgumentException.class, () -> requirements.borrowClassicBootstrapSources(first,
                wrongRoot, ROOT, TARGET, Duration.ofSeconds(5)));
        assertThrows(IllegalArgumentException.class, () -> owner(Duration.ZERO));
    }

    @Test
    public void eitherRegistrationAndUnknownReadsRejectWithoutDeletingOrPublishingAnything() throws Exception {
        for (String prefix : List.of("/available/", "/available/readonly/")) {
            String path = ROOT + prefix + TARGET;
            first.create(path, new byte[]{7}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            var owner = owner(Duration.ofSeconds(5));
            assertTrue(assertThrows(IOException.class, owner::read).getMessage().contains("registration"));
            drained(owner);
            assertArrayEquals(new byte[]{7}, first.getData(path, false, null));
            first.delete(path, 0);
        }
        var unavailable = owner(Duration.ofNanos(1));
        assertThrows(IOException.class, unavailable::read);
        drained(unavailable);
        assertFalse(manager.isBookieRegistered(TARGET));
        var identity = requirements.read(BOOKIE).observation().orElseThrow().sourceIdentity();
        first.multi(List.of(Op.delete(identity.nodePath() + "/birth-" + identity.birthId(), 0),
                Op.delete(identity.nodePath(), 0)));
        var missing = owner(Duration.ofSeconds(5));
        assertTrue(assertThrows(IOException.class, missing::read).getMessage().contains("requirements are missing"));
        drained(missing);
        assertEquals(null, first.exists(identity.nodePath(), false));
    }

    @Test
    public void nativeBirthRequirementsBirthAndCookieChangesAreColdReadInsteadOfUsingPriorValues() throws Exception {
        var owner = owner(Duration.ofSeconds(5));
        var original = owner.read();
        String path = new ZKBookieRouteBindingStore(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofSeconds(5)).path(TARGET);
        String marker = path + "/birth-" + original.nativeBinding().birthId();
        byte[] nativeBytes = first.getData(path, false, null);
        first.multi(List.of(Op.delete(marker, 0), Op.delete(path, 0)));
        first.multi(List.of(Op.create(path, nativeBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(marker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
        var recreated = owner.read();
        assertEquals(original.nativeBinding().binding(), recreated.nativeBinding().binding());
        assertEquals(original.nativeBinding().birthId(), recreated.nativeBinding().birthId());
        assertNotEquals(original.nativeBinding().birthZxid(), recreated.nativeBinding().birthZxid());
        String requiredPath = requirements.path(BOOKIE);
        byte[] requiredBytes = first.getData(requiredPath, false, null);
        String requiredMarker = requiredPath + "/birth-" + original.requirementsSource().birthId();
        first.multi(List.of(Op.delete(requiredMarker, 0), Op.delete(requiredPath, 0)));
        first.multi(List.of(Op.create(requiredPath, requiredBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(requiredMarker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
        var changed = owner.read();
        assertNotEquals(original.requirementsSource(), changed.requirementsSource());
        assertEquals(original.commissionedCompatibility(), changed.commissionedCompatibility());
        first.setData(ROOT + "/cookies/" + TARGET, cookie, 0);
        assertTrue(assertThrows(IOException.class, owner::read).getMessage().contains("INCOMPATIBLE"));
        drained(owner);
        assertEquals(1, first.exists(ROOT + "/cookies/" + TARGET, false).getVersion());
        first.multi(List.of(Op.delete(marker, 0), Op.delete(path, 0)));
        var absent = owner(Duration.ofSeconds(5));
        assertTrue(assertThrows(IOException.class, absent::read).getMessage().contains("ABSENT"));
        drained(absent);
        assertEquals(null, first.exists(path, false));
    }

    @Test
    public void borrowedCompletionRejectsRecreatedRequirementsBeforeChangingTheNativeBinding() throws Exception {
        var owner = owner(Duration.ofSeconds(5));
        var prepared = owner.read();
        var source = prepared.requirementsSource();
        String marker = source.nodePath() + "/birth-" + source.birthId();
        byte[] original = first.getData(source.nodePath(), false, null);
        first.multi(List.of(Op.delete(marker, 0), Op.delete(source.nodePath(), 0),
                Op.create(source.nodePath(), original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(marker, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
        assertNotEquals(source.birthZxid(), first.exists(source.nodePath(), false).getCzxid());
        var seal = new BookieRouteBinding.BootstrapSeal(new UUID(0, 20), 4, 3);
        try {
            var failure = assertThrows(IOException.class, () -> owner.completePrepared(prepared, seal));
            assertTrue(failure.getMessage().contains("changed before native completion"));
            assertEquals(prepared.nativeBinding(), manager.readBookieRouteBinding(TARGET).observation().orElseThrow());
        } finally {
            drained(owner);
        }
    }

    @Test
    public void otherCommissionedIncarnationOrManifestCannotBeCombinedWithTheNativeScope() throws Exception {
        for (int mismatch = 0; mismatch < 2; mismatch++) {
            String path = ROOT + "/other-requirements-" + mismatch;
            first.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            requirements = new ZKProfileStartupRequirementsStore(first, path,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var incarnation = mismatch == 0 ? new StorageIncarnation(bytes(16, 77)) : binding.incarnation();
            var manifest = mismatch == 1 ? new ManifestIdentity(bytes(32, 77)) : binding.manifest();
            var one = new Generation(1);
            var other = new BookieCompatibilityFact(BOOKIE, incarnation, EngineProfile.SEGMENT_WAL_ENGINE,
                    1, 0, 1, List.of(1), manifest, one, one, one, 1, 1,
                    BookieCompatibilityFact.MigrationState.FORMAT_READY, one);
            var devices = new RequiredDeviceManifest(manifest, one,
                    List.of(new RequiredDeviceManifest.RequiredDeviceFact("arena", incarnation, one,
                            RequiredDeviceManifest.DeviceStatus.READY, 1, 1, 1, one, List.of(1))));
            assertEquals(ZKProfileStartupRequirementsStore.ProvisionStatus.APPLIED,
                    requirements.provision(other, devices));
            var owner = owner(Duration.ofSeconds(5));
            assertTrue(assertThrows(IOException.class, owner::read).getMessage().contains("native bootstrap scope"));
            drained(owner);
        }
    }

    @Test
    public void timedOutCallerRetainsTheOriginalCallbackUntilItsActualReturn() throws Exception {
        var owner = owner(Duration.ofMillis(100));
        first.holdReadReply.set(true);
        IOException failure = assertThrows(IOException.class, owner::read);
        assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
        var pending = owner.closeAndDrain();
        assertFalse(pending.drained());
        assertEquals(1, pending.pendingCallbacks());
        assertSame(failure, pending.firstReadFailure());
        assertThrows(IOException.class, owner::read);
        first.lateReadReply.getAndSet(null).run();
        drained(owner);
        assertSame(failure, owner.closeAndDrain().firstReadFailure());
        assertTrue(first.getState().isAlive());
    }

    @Test(timeout = 30000)
    public void interruptionDoesNotCancelTheActualHeldCallback() throws Exception {
        var owner = owner(Duration.ofSeconds(10));
        first.holdReadReply.set(true);
        var task = new FutureTask<Boolean>(() -> {
            assertThrows(IOException.class, owner::read);
            return Thread.currentThread().isInterrupted();
        });
        var thread = new Thread(task);
        try {
            thread.start();
            assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
            thread.interrupt();
            assertTrue(task.get(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, owner.closeAndDrain().pendingCallbacks());
        } finally {
            thread.interrupt();
            thread.join(5000);
            Runnable actual = first.lateReadReply.getAndSet(null);
            if (actual != null) {
                actual.run();
            }
            drained(owner);
        }
        assertFalse(thread.isAlive());
    }

    @Test(timeout = 30000)
    public void actualReadCannotDrainItselfAndExternalCloseWaitsForItsRealReturn() throws Exception {
        var owner = owner(Duration.ofSeconds(10));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        first.beforeRead.set(() -> {
            assertThrows(IOException.class, owner::read);
            assertThrows(IllegalStateException.class, owner::closeAndDrain);
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        var read = new FutureTask<>(owner::read);
        var close = new FutureTask<>(owner::closeAndDrain);
        var reader = new Thread(read);
        var closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(close.isDone());
            release.countDown();
            assertEquals(binding, read.get(5, TimeUnit.SECONDS).nativeBinding().binding());
            close.get(5, TimeUnit.SECONDS);
            drained(owner);
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
            drained(owner);
        }
    }

    @Test
    public void unknownSynchronousSubmissionFailureDoesNotInventCallbackTermination() {
        var owner = owner(Duration.ofSeconds(5));
        var failure = new IllegalStateException("submission disposition unknown");
        first.beforeRead.set(() -> {
            throw failure;
        });
        assertSame(failure, assertThrows(IllegalStateException.class, owner::read));
        for (int attempt = 0; attempt < 2; attempt++) {
            var close = owner.closeAndDrain();
            assertFalse(close.drained());
            assertEquals(1, close.pendingCallbacks());
            assertSame(failure, close.firstReadFailure());
        }
        assertTrue(first.getState().isAlive());
    }
}

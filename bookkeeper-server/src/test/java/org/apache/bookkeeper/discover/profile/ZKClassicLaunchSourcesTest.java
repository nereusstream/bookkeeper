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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

/** Real original cold reads only. No trust material, launch permit or qualified process is established. */
public class ZKClassicLaunchSourcesTest extends ProfileZKTestBase {
    private static final BookieId TARGET = BookieId.parse(BOOKIE);
    private static final String CLUSTER = "classic-launch-cluster";
    private ZKRegistrationManager manager;
    private byte[] cookie;

    @Before
    public void originalSources() throws Exception {
        first.create(ROOT + "/INSTANCEID", CLUSTER.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.create(ROOT + "/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.create(ROOT + "/available/readonly", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var conf = new ServerConfiguration().setBookieId(BOOKIE)
                .setJournalDirsName(new String[]{"/launch-fixture/journal"})
                .setLedgerDirNames(new String[]{"/launch-fixture/ledger"});
        manager = new ZKRegistrationManager(conf, first, ROOT);
        cookie = Cookie.generateCookie(conf).setInstanceId(CLUSTER).build().toString()
                .getBytes(StandardCharsets.UTF_8);
        manager.writeCookie(TARGET, new Versioned<>(cookie, Version.NEW));
    }

    private ZKBookieRouteBindingStore.ClassicLaunchSources owner(Duration timeout) {
        return manager.borrowClassicLaunchSources(TARGET, timeout);
    }

    private static void drained(ZKBookieRouteBindingStore.ClassicLaunchSources owner) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> owner.closeAndDrain().drained());
        assertTrue(owner.closeAndDrain().drained());
    }

    private static BookieRouteBinding prepared(ZKBookieRouteBindingStore.ClassicLaunchSnapshot actual) {
        byte[] incarnation = new byte[16];
        byte[] manifest = new byte[32];
        Arrays.fill(incarnation, (byte) 1);
        Arrays.fill(manifest, (byte) 2);
        return new BookieRouteBinding(actual.clusterId(), actual.clusterBirthZxid(), actual.bookie(),
                new StorageIncarnation(incarnation), new UUID(0, 9), new ManifestIdentity(manifest),
                "/launch-fixture/control", actual.cookieIdentity(), BookieRouteBinding.Phase.PREPARED,
                Optional.empty());
    }

    @Test(timeout = 30000)
    public void independentRawObserverChecksAnotherSessionsExactRegistrationWithoutAdoptingIt() throws Exception {
        var connected = new CountDownLatch(1);
        try (var observer = new ZooKeeper(cluster.getZooKeeperConnectString(), 10000, event -> {
            if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        })) {
            assertTrue(connected.await(10, TimeUnit.SECONDS));
            assertNotEquals(first.getSessionId(), observer.getSessionId());
            var store = new ZKBookieRouteBindingStore(observer, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            var owner = store.borrowClassicLaunchSources(TARGET, Duration.ofSeconds(5));
            var original = owner.read();
            String path = ROOT + "/available/" + TARGET;
            first.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            long birth = first.exists(path, false).getCzxid();
            var observed = owner.readForObservedRegistration(first.getSessionId(), birth, false);
            assertEquals(original.cookieIdentity(), observed.cookieIdentity());
            assertEquals(original.clusterBirthZxid(), observed.clusterBirthZxid());
            assertThrows(IOException.class, () -> owner.readForServing(first.getSessionId(), birth, false));
            assertThrows(IOException.class,
                    () -> owner.readForObservedRegistration(observer.getSessionId(), birth, false));
            assertEquals(first.getSessionId(), observer.exists(path, false).getEphemeralOwner());
            drained(owner);
            var wrong = store.borrowClassicLaunchSources(TARGET, Duration.ofSeconds(5));
            assertThrows(IOException.class,
                    () -> wrong.readForObservedRegistration(first.getSessionId(), birth + 1, false));
            drained(wrong);
            assertEquals(birth, first.exists(path, false).getCzxid());
            var stillRunning = store.borrowClassicLaunchSources(TARGET, Duration.ofSeconds(5));
            assertThrows(IOException.class, stillRunning::readAfterServiceStopped);
            drained(stillRunning);
            first.delete(path, 0);
            assertEquals(BookieRouteBinding.WriteStatus.APPLIED,
                    manager.prepareBookieRouteBinding(prepared(original)).status());
            var stopped = store.borrowClassicLaunchSources(TARGET, Duration.ofSeconds(5));
            assertEquals(original.cookieIdentity(), stopped.readAfterServiceStopped().cookieIdentity());
            drained(stopped);
            var newLaunch = store.borrowClassicLaunchSources(TARGET, Duration.ofSeconds(5));
            assertThrows(IOException.class, newLaunch::read);
            drained(newLaunch);
            assertEquals(BookieRouteBinding.ReadStatus.FOUND, manager.readBookieRouteBinding(TARGET).status());
        }
    }

    @Test
    public void heapOnlyBorrowReadsActualAbsentOriginalSourcesWithoutFutureCommissioning() throws Exception {
        int reads = first.reads.get();
        var owner = owner(Duration.ofSeconds(5));
        assertEquals(reads, first.reads.get());
        var original = owner.read();
        assertEquals(ROOT, original.ledgersRoot());
        assertEquals(TARGET, original.bookie());
        assertEquals(CLUSTER, original.clusterId());
        assertEquals(first.exists(ROOT + "/INSTANCEID", false).getCzxid(), original.clusterBirthZxid());
        assertEquals(first.exists(ROOT + "/cookies/" + TARGET, false).getCzxid(),
                original.cookieIdentity().birthZxid());
        assertArrayEquals(cookie, original.cookieBytes());
        byte[] callerCopy = original.cookieBytes();
        Arrays.fill(callerCopy, (byte) 0);
        assertArrayEquals(cookie, original.cookieBytes());
        first.setData(ROOT + "/cookies/" + TARGET, cookie, 0);
        var changed = owner.read();
        assertNotEquals(original.cookieIdentity(), changed.cookieIdentity());
        assertEquals(1, changed.cookieIdentity().version());
        drained(owner);
        assertThrows(IOException.class, owner::read);
        assertTrue(first.getState().isAlive());
        assertArrayEquals(cookie, manager.readCookie(TARGET).getValue());
        assertEquals(null, first.exists(ROOT + "/bookie-route-bindings", false));
        assertFalse(manager.isBookieRegistered(TARGET));
        assertThrows(IllegalArgumentException.class, () -> owner(Duration.ZERO));
    }

    @Test
    public void nativeFoundBeforeOrDuringObservationCannotReturnLaunchSources() throws Exception {
        var observing = owner(Duration.ofSeconds(5));
        var original = observing.read();
        drained(observing);
        var racing = owner(Duration.ofSeconds(5));
        // First native get returns ABSENT; commission before the following original cluster get.
        first.beforeRead.set(() -> first.beforeRead.set(() -> assertEquals(BookieRouteBinding.WriteStatus.APPLIED,
                manager.prepareBookieRouteBinding(prepared(original)).status())));
        assertTrue(assertThrows(IOException.class, racing::read).getMessage().contains("FOUND"));
        drained(racing);
        var alreadyFound = owner(Duration.ofSeconds(5));
        assertTrue(assertThrows(IOException.class, alreadyFound::read).getMessage().contains("FOUND"));
        drained(alreadyFound);
        assertEquals(BookieRouteBinding.ReadStatus.FOUND, manager.readBookieRouteBinding(TARGET).status());
        assertArrayEquals(cookie, manager.readCookie(TARGET).getValue());
    }

    @Test
    public void registrationsMissingCookieAndMutableClusterRejectWithoutRepair() throws Exception {
        for (String prefix : List.of("/available/", "/available/readonly/")) {
            String path = ROOT + prefix + TARGET;
            first.create(path, new byte[]{7}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            var owner = owner(Duration.ofSeconds(5));
            assertTrue(assertThrows(IOException.class, owner::read).getMessage().contains("registration"));
            drained(owner);
            assertArrayEquals(new byte[]{7}, first.getData(path, false, null));
            first.delete(path, 0);
        }
        first.delete(ROOT + "/cookies/" + TARGET, 0);
        var missing = owner(Duration.ofSeconds(5));
        assertTrue(assertThrows(IOException.class, missing::read).getMessage().contains("Cookie"));
        drained(missing);
        assertEquals(null, first.exists(ROOT + "/cookies/" + TARGET, false));
        manager.writeCookie(TARGET, new Versioned<>(cookie, Version.NEW));
        first.setData(ROOT + "/INSTANCEID", CLUSTER.getBytes(StandardCharsets.UTF_8), 0);
        var mutable = owner(Duration.ofSeconds(5));
        assertTrue(assertThrows(IOException.class, mutable::read).getMessage().contains("cluster"));
        drained(mutable);
        assertEquals(1, first.exists(ROOT + "/INSTANCEID", false).getVersion());
        assertEquals(null, first.exists(ROOT + "/bookie-route-bindings", false));
    }

    @Test
    public void timedOutReadRetainsItsActualNetworkCallback() throws Exception {
        var owner = owner(Duration.ofMillis(100));
        first.holdReadReply.set(true);
        IOException failure = assertThrows(IOException.class, owner::read);
        try {
            assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, owner.closeAndDrain().pendingCallbacks());
            assertSame(failure, owner.closeAndDrain().firstReadFailure());
            assertThrows(IOException.class, owner::read);
        } finally {
            first.lateReadReply.getAndSet(null).run();
            drained(owner);
        }
        assertTrue(first.getState().isAlive());
    }

    @Test(timeout = 30000)
    public void interruptedObservationDoesNotCancelOriginalCallback() throws Exception {
        var owner = owner(Duration.ofSeconds(10));
        first.holdReadReply.set(true);
        var read = new FutureTask<>(() -> {
            assertThrows(IOException.class, owner::read);
            return Thread.currentThread().isInterrupted();
        });
        Thread reader = new Thread(read);
        try {
            reader.start();
            assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
            reader.interrupt();
            assertTrue(read.get(5, TimeUnit.SECONDS));
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, owner.closeAndDrain().pendingCallbacks());
        } finally {
            reader.interrupt();
            reader.join(5000);
            Runnable actual = first.lateReadReply.getAndSet(null);
            if (actual != null) {
                actual.run();
            }
            drained(owner);
        }
        assertFalse(reader.isAlive());
    }

    @Test(timeout = 30000)
    public void actualSubmissionBodyCannotDrainItselfAndExternalCloseWaits() throws Exception {
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
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(close.isDone());
            release.countDown();
            assertEquals(CLUSTER, read.get(5, TimeUnit.SECONDS).clusterId());
            close.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            drained(owner);
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    @Test
    public void unknownSynchronousSubmissionDoesNotInventCallbackTermination() {
        var owner = owner(Duration.ofSeconds(5));
        var failure = new IllegalStateException("actual submission result unknown");
        first.beforeRead.set(() -> {
            throw failure;
        });
        assertSame(failure, assertThrows(IllegalStateException.class, owner::read));
        var result = owner.closeAndDrain();
        assertFalse(result.drained());
        assertEquals(1, result.pendingCallbacks());
        assertSame(failure, result.firstReadFailure());
        assertFalse(owner.closeAndDrain().drained());
        assertTrue(first.getState().isAlive());
    }
}

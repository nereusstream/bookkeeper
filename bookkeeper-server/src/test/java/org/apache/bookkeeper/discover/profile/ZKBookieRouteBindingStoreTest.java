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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.BookieRouteBinding.ReadStatus;
import org.apache.bookkeeper.discover.BookieRouteBinding.WriteStatus;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class ZKBookieRouteBindingStoreTest extends ProfileZKTestBase {
    private static final BookieId TARGET = BookieId.parse(BOOKIE);
    private static final String CLUSTER = "common-route-native-cluster";
    private byte[] cookie;
    private BookieRouteBinding binding;

    private static byte[] bytes(int length, int value) {
        byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ZKRegistrationManager manager(ZooKeeper client) {
        return new ZKRegistrationManager(new ServerConfiguration().setBookieId(BOOKIE), client, ROOT);
    }

    private static ZKBookieRouteBindingStore store(ZooKeeper client) {
        return new ZKBookieRouteBindingStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    @Before
    public void source() throws Exception {
        first.create(ROOT + "/INSTANCEID", CLUSTER.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var conf = new ServerConfiguration().setBookieId(BOOKIE)
                .setJournalDirsName(new String[]{"/classic/journal"})
                .setLedgerDirNames(new String[]{"/classic/ledger"});
        cookie = Cookie.generateCookie(conf).setInstanceId(CLUSTER).build().toString()
                .getBytes(StandardCharsets.UTF_8);
        manager(first).writeCookie(TARGET, new Versioned<>(cookie, Version.NEW));
        Stat source = first.exists(ROOT + "/cookies/" + TARGET, false);
        binding = new BookieRouteBinding(CLUSTER, first.exists(ROOT + "/INSTANCEID", false).getCzxid(), TARGET,
                new StorageIncarnation(bytes(16, 1)), new UUID(0, 9), new ManifestIdentity(bytes(32, 2)),
                "/common/control", new BookieRouteBinding.SourceCookie(source.getCzxid(), source.getVersion(),
                        MessageDigest.getInstance("SHA-256").digest(cookie)),
                BookieRouteBinding.Phase.PREPARED, Optional.empty());
    }

    @Test
    public void publicNativeReadIsNoncreatingAndBothLostTransitionsResolveTheSameOperation() throws Exception {
        var firstManager = manager(first);
        var secondManager = manager(second);
        List<String> before = first.getChildren(ROOT, false);
        assertEquals(ReadStatus.ABSENT, firstManager.readBookieRouteBinding(TARGET).status());
        assertEquals(java.util.Set.copyOf(before), java.util.Set.copyOf(first.getChildren(ROOT, false)));
        assertNull(first.exists(ROOT + "/bookie-route-bindings", false));
        first.loseReply.set(true);
        assertEquals(WriteStatus.RESPONSE_LOST, firstManager.prepareBookieRouteBinding(binding).status());
        var prepared = secondManager.readBookieRouteBinding(TARGET).observation().orElseThrow();
        assertEquals(binding, prepared.binding());
        assertEquals(0, prepared.version());
        assertEquals(WriteStatus.ALREADY_APPLIED, firstManager.prepareBookieRouteBinding(binding).status());
        var seal = new BookieRouteBinding.BootstrapSeal(new UUID(0, 10), 4, 3);
        first.loseSetDataMultiReply.set(true);
        assertEquals(WriteStatus.RESPONSE_LOST, firstManager.completeBookieRouteBinding(prepared, seal).status());
        var enabled = secondManager.readBookieRouteBinding(TARGET).observation().orElseThrow();
        assertEquals(binding.enabled(seal), enabled.binding());
        assertEquals(prepared.birthId(), enabled.birthId());
        assertEquals(prepared.birthZxid(), enabled.birthZxid());
        assertEquals(1, enabled.version());
        assertEquals(WriteStatus.ALREADY_APPLIED, firstManager.completeBookieRouteBinding(prepared, seal).status());
        assertEquals(WriteStatus.CONFLICT, firstManager.completeBookieRouteBinding(prepared,
                new BookieRouteBinding.BootstrapSeal(new UUID(0, 11), 4, 3)).status());
        assertArrayEquals(cookie, first.getData(ROOT + "/cookies/" + TARGET, false, null));
        assertEquals(0, first.exists(ROOT + "/cookies/" + TARGET, false).getVersion());
    }

    @Test
    public void concurrentCommissioningHasOneWinnerAndCannotChangeScopeInPlace() throws Exception {
        var other = new BookieRouteBinding(binding.clusterId(), binding.clusterBirthZxid(), binding.bookieId(),
                binding.incarnation(), new UUID(0, 99), binding.manifest(), binding.controlDirectory(),
                binding.sourceCookie(), binding.phase(), binding.seal());
        first.beforeMulti.set(() -> assertEquals(WriteStatus.APPLIED,
                manager(second).prepareBookieRouteBinding(other).status()));
        assertEquals(WriteStatus.CONFLICT, manager(first).prepareBookieRouteBinding(binding).status());
        assertEquals(other, manager(second).readBookieRouteBinding(TARGET).observation().orElseThrow().binding());
        assertEquals(WriteStatus.CONFLICT, manager(first).prepareBookieRouteBinding(binding).status());
    }

    @Test
    public void originalCookieMutationStopsCompletionAndRemovingCookieKeepsTheNativeBinding() throws Exception {
        var rm = manager(first);
        var prepared = rm.prepareBookieRouteBinding(binding).observation().orElseThrow();
        first.beforeSetDataMulti.set(() -> {
            try {
                second.setData(ROOT + "/cookies/" + TARGET, cookie, 0);
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
        var seal = new BookieRouteBinding.BootstrapSeal(new UUID(0, 10), 0, -1);
        assertEquals(WriteStatus.CONFLICT, rm.completeBookieRouteBinding(prepared, seal).status());
        assertEquals(ReadStatus.INCOMPATIBLE, rm.readBookieRouteBinding(TARGET).status());
        String path = store(first).path(TARGET);
        byte[] record = first.getData(path, false, null);
        assertEquals(BookieRouteBinding.Phase.PREPARED,
                BookieRouteBinding.decode(Arrays.copyOfRange(record, 24, record.length)).phase());
        rm.removeCookie(TARGET, new LongVersion(1));
        assertNotNull(first.exists(path, false));
        assertEquals(ReadStatus.INCOMPATIBLE, rm.readBookieRouteBinding(TARGET).status());
        assertNotEquals(ReadStatus.ABSENT, rm.readBookieRouteBinding(TARGET).status());
    }

    @Test
    public void physicalRecreationWithTheSameBindingBytesAndVersionCannotUseOldObservation() throws Exception {
        var rm = manager(first);
        var prepared = rm.prepareBookieRouteBinding(binding).observation().orElseThrow();
        String path = store(first).path(TARGET);
        byte[] original = first.getData(path, false, null);
        String marker = first.getChildren(path, false).get(0);
        first.delete(path + "/" + marker, 0);
        first.delete(path, 0);
        first.multi(List.of(Op.create(path, original, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create(path + "/" + marker, new byte[0], ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT)));
        var replacement = rm.readBookieRouteBinding(TARGET).observation().orElseThrow();
        assertEquals(prepared.binding(), replacement.binding());
        assertEquals(prepared.version(), replacement.version());
        assertEquals(prepared.birthId(), replacement.birthId());
        assertNotEquals(prepared.birthZxid(), replacement.birthZxid());
        assertEquals(WriteStatus.CONFLICT, rm.completeBookieRouteBinding(prepared,
                new BookieRouteBinding.BootstrapSeal(new UUID(0, 10), 0, -1)).status());
    }

    @Test
    public void cookiePhysicalRecreationDuringCommissioningCannotAcknowledgeAValidSource() throws Exception {
        first.beforeMulti.set(() -> {
            try {
                second.delete(ROOT + "/cookies/" + TARGET, 0);
                second.create(ROOT + "/cookies/" + TARGET, cookie, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
        assertEquals(WriteStatus.INCOMPATIBLE, manager(first).prepareBookieRouteBinding(binding).status());
        assertNotNull(first.exists(store(first).path(TARGET), false));
        assertEquals(ReadStatus.INCOMPATIBLE, manager(second).readBookieRouteBinding(TARGET).status());
        assertArrayEquals(cookie, second.getData(ROOT + "/cookies/" + TARGET, false, null));
        assertEquals(0, second.exists(ROOT + "/cookies/" + TARGET, false).getVersion());
        assertNotEquals(binding.sourceCookie().birthZxid(),
                second.exists(ROOT + "/cookies/" + TARGET, false).getCzxid());
    }

    @Test
    public void nativePhysicalRecreationDuringCompletionCannotAcknowledgeTheOldObservation() throws Exception {
        var rm = manager(first);
        var prepared = rm.prepareBookieRouteBinding(binding).observation().orElseThrow();
        String path = store(first).path(TARGET);
        byte[] record = first.getData(path, false, null);
        String marker = first.getChildren(path, false).get(0);
        first.beforeSetDataMulti.set(() -> {
            try {
                second.delete(path + "/" + marker, 0);
                second.delete(path, 0);
                second.multi(List.of(Op.create(path, record, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                        Op.create(path + "/" + marker, new byte[0], ZooDefs.Ids.READ_ACL_UNSAFE,
                                CreateMode.PERSISTENT)));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
        var seal = new BookieRouteBinding.BootstrapSeal(new UUID(0, 10), 0, -1);
        assertEquals(WriteStatus.CONFLICT, rm.completeBookieRouteBinding(prepared, seal).status());
        var replacement = rm.readBookieRouteBinding(TARGET).observation().orElseThrow();
        assertEquals(prepared.birthId(), replacement.birthId());
        assertNotEquals(prepared.birthZxid(), replacement.birthZxid());
        assertEquals(binding.enabled(seal), replacement.binding());
        assertEquals(WriteStatus.CONFLICT, rm.completeBookieRouteBinding(prepared, seal).status());
    }

    @Test
    public void permissionFailureNeverReportsAnUnboundBookie() throws Exception {
        var rm = manager(first);
        assertEquals(WriteStatus.APPLIED, rm.prepareBookieRouteBinding(binding).status());
        first.addAuthInfo("digest", "route-owner:secret".getBytes(StandardCharsets.UTF_8));
        first.setACL(store(first).path(TARGET), ZooDefs.Ids.CREATOR_ALL_ACL, -1);
        assertEquals(ReadStatus.UNAVAILABLE, manager(second).readBookieRouteBinding(TARGET).status());
        assertEquals(ReadStatus.FOUND, rm.readBookieRouteBinding(TARGET).status());
    }

    @Test
    public void corruptBirthSchemaAndUnavailableQueriesNeverBecomeAbsent() throws Exception {
        var rm = manager(first);
        rm.prepareBookieRouteBinding(binding);
        String path = store(first).path(TARGET);
        String marker = first.getChildren(path, false).get(0);
        first.delete(path + "/" + marker, 0);
        assertEquals(ReadStatus.CORRUPT, rm.readBookieRouteBinding(TARGET).status());
        byte[] original = first.getData(path, false, null);
        ByteBuffer.wrap(original).putInt(32, 2);
        first.setData(path, original, 0);
        assertEquals(ReadStatus.INCOMPATIBLE, rm.readBookieRouteBinding(TARGET).status());
        var timed = new ZKBookieRouteBindingStore(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                Duration.ofMillis(100));
        first.holdReadReply.set(true);
        assertEquals(ReadStatus.UNAVAILABLE, timed.read(TARGET).status());
        assertTrue(first.readReplyHeld.await(5, java.util.concurrent.TimeUnit.SECONDS));
        first.lateReadReply.getAndSet(null).run();
        assertEquals(ReadStatus.UNSUPPORTED,
                new NullMetadataBookieDriver.NullRegistrationManager().readBookieRouteBinding(TARGET).status());
    }

    @Test
    public void bindingCodecHasAnIndependentCanonicalVectorAndRejectsInvalidBounds() throws Exception {
        byte[] cluster = binding.clusterId().getBytes(StandardCharsets.UTF_8);
        byte[] bookie = binding.bookieId().toString().getBytes(StandardCharsets.UTF_8);
        byte[] control = binding.controlDirectory().getBytes(StandardCharsets.UTF_8);
        ByteBuffer expected = ByteBuffer.allocate(144 + cluster.length + bookie.length + control.length);
        expected.putLong(0x5557425242303031L).putInt(1).putInt(1)
                .putInt(cluster.length).put(cluster).putLong(binding.clusterBirthZxid())
                .putInt(bookie.length).put(bookie).put(bytes(16, 1)).putLong(0).putLong(9).put(bytes(32, 2))
                .putInt(control.length).put(control).putLong(binding.sourceCookie().birthZxid())
                .putInt(0).put(MessageDigest.getInstance("SHA-256").digest(cookie));
        assertFalse(expected.hasRemaining());
        assertArrayEquals(expected.array(), binding.encode());
        assertEquals(binding, BookieRouteBinding.decode(expected.array()));
        byte[] copy = binding.sourceCookie().sha256();
        copy[0] ^= 1;
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(cookie), binding.sourceCookie().sha256());
        assertThrows(IllegalArgumentException.class, () -> BookieRouteBinding.decode(new byte[8193]));
        assertThrows(IllegalArgumentException.class,
                () -> BookieRouteBinding.decode(Arrays.copyOf(expected.array(), 20)));
        assertThrows(IllegalArgumentException.class, () -> new BookieRouteBinding.BootstrapSeal(new UUID(0, 1), 0, 0));
    }

    @Test
    public void standardManagerOnlyReadUsesActualLedgerRootWithoutGrantingMutation() throws Exception {
        var configuration = new ClientConfiguration()
                .setMetadataServiceUri("zk+longhierarchical://127.0.0.1:2181" + ROOT);
        var factory = new LongHierarchicalLedgerManagerFactory();
        factory.initialize(configuration, new ZkLayoutManager(first, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE),
                factory.getCurrentVersion());
        try (var ledgers = factory.newLedgerManager()) {
            var metadata = new ProfileLedgerMetadataStore(ledgers);
            assertEquals(ReadStatus.ABSENT, metadata.readBookieRouteBinding(TARGET, Duration.ofSeconds(5)).status());
            assertEquals(WriteStatus.APPLIED, manager(first).prepareBookieRouteBinding(binding).status());
            assertEquals(binding, metadata.readBookieRouteBinding(TARGET, Duration.ofSeconds(5))
                    .observation().orElseThrow().binding());
            assertThrows(IllegalArgumentException.class, () -> metadata.readMembership(0));
        } finally {
            factory.close();
        }
        var unsupported = new ProfileLedgerMetadataStore(new NullMetadataBookieDriver.NullLedgerManager());
        assertEquals(ReadStatus.UNSUPPORTED,
                unsupported.readBookieRouteBinding(TARGET, Duration.ofSeconds(5)).status());
    }
}

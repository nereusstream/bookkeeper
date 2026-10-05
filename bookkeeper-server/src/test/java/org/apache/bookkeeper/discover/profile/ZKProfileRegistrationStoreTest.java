/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.VersionedRecord;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZKProfileRegistrationStoreTest {
    private static final String ROOT = "/profile-readiness-test";
    private static final String BOOKIE = "bookie:[::1]:3181";
    private final ZooKeeperUtil cluster = new ZooKeeperUtil() {
        @Override
        public String getZooKeeperConnectString() {
            String address = super.getZooKeeperConnectString();
            return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
        }
    };
    private TestClient first;
    private TestClient second;

    @Before
    public void setup() throws Exception {
        cluster.startCluster();
        first = connect();
        second = connect();
        first.create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @After
    public void cleanup() throws Exception {
        try {
            if (first != null) {
                first.close();
            }
        } finally {
            try {
                if (second != null) {
                    second.close();
                }
            } finally {
                cluster.stopCluster();
            }
        }
    }

    private TestClient connect() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        TestClient client = new TestClient(cluster.getZooKeeperConnectString(), event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(10, TimeUnit.SECONDS)) {
            client.close();
            throw new IOException("ZooKeeper test client did not connect");
        }
        return client;
    }

    private static ZKProfileRegistrationStore store(ZooKeeper client) {
        return new ZKProfileRegistrationStore(client, ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
    }

    private static ProfileReadinessRecord record(long generation) {
        byte[] storage = new byte[16];
        byte[] manifest = new byte[32];
        Arrays.fill(storage, (byte) 1);
        Arrays.fill(manifest, (byte) 2);
        return new ProfileReadinessRecord(BOOKIE, new StorageIncarnation(storage), EngineProfile.SEGMENT_WAL_ENGINE,
                1, 2, new Generation(3), new Generation(4), new ManifestIdentity(manifest), new Generation(5),
                new Generation(6), new Generation(generation), 7, 8, List.of(11, 13),
                ProfileReadinessRecord.State.READY);
    }

    private VersionedRecord create() {
        ZKProfileRegistrationStore writer = store(first);
        assertEquals(CasStatus.APPLIED, writer.compareAndSet(BOOKIE, -1, 0, record(1)).status());
        return store(second).read(BOOKIE).versionedRecord().orElseThrow();
    }

    private String path() throws Exception {
        List<String> children = first.getChildren(ROOT, false);
        assertEquals(1, children.size());
        return ROOT + "/" + children.get(0);
    }

    @Test
    public void realStorePreservesEveryFieldAndChecksBothVersions() throws Exception {
        assertEquals(ReadStatus.NOT_FOUND, store(first).read(BOOKIE).status());
        VersionedRecord initial = create();
        assertEquals(record(1), initial.record());
        assertEquals(0, first.exists(path(), false).getVersion());
        assertEquals(CasStatus.CONFLICT, store(first).compareAndSet(BOOKIE,
                initial.storeVersion(), 99, record(2)).status());
        assertEquals(CasStatus.APPLIED, store(first).compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        VersionedRecord updated = store(second).read(BOOKIE).versionedRecord().orElseThrow();
        assertEquals(record(2), updated.record());
        assertTrue(updated.storeVersion() > initial.storeVersion());
        assertEquals(CasStatus.CONFLICT, store(second).compareAndSet(BOOKIE,
                initial.storeVersion(), 2, record(3)).status());
        assertEquals(updated, store(first).read(BOOKIE).versionedRecord().orElseThrow());
    }

    @Test
    public void simultaneousRealTransactionsHaveOneWinner() throws Exception {
        VersionedRecord initial = create();
        CountDownLatch ready = new CountDownLatch(2);
        Runnable meet = () -> {
            ready.countDown();
            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        first.beforeMulti.set(meet);
        second.beforeMulti.set(meet);
        CompletableFuture<CasStatus> a = CompletableFuture.supplyAsync(() -> store(first).compareAndSet(
                BOOKIE, initial.storeVersion(), 1, record(2)).status());
        CompletableFuture<CasStatus> b = CompletableFuture.supplyAsync(() -> store(second).compareAndSet(
                BOOKIE, initial.storeVersion(), 1, record(3)).status());
        List<CasStatus> results = List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS));
        assertEquals(1, results.stream().filter(status -> status == CasStatus.APPLIED).count());
        assertEquals(1, results.stream().filter(status -> status == CasStatus.CONFLICT).count());
        long generation = store(first).read(BOOKIE).versionedRecord().orElseThrow()
                .record().localReadinessGeneration().value();
        assertTrue(generation == 2 || generation == 3);
    }

    @Test
    public void lostCreateAndUpdateRepliesResolveThroughFreshRealReads() {
        ZKProfileRegistrationStore writer = store(first);
        first.loseReply.set(true);
        assertEquals(CasStatus.RESPONSE_LOST, writer.compareAndSet(BOOKIE, -1, 0, record(1)).status());
        VersionedRecord initial = store(second).read(BOOKIE).versionedRecord().orElseThrow();
        assertEquals(record(1), initial.record());
        first.loseReply.set(true);
        assertEquals(CasStatus.RESPONSE_LOST, writer.compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        VersionedRecord updated = store(second).read(BOOKIE).versionedRecord().orElseThrow();
        assertEquals(record(2), updated.record());
        assertEquals(CasStatus.CONFLICT, writer.compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        assertEquals(updated, store(second).read(BOOKIE).versionedRecord().orElseThrow());
    }

    @Test
    public void deletionAndRecreationBetweenReadAndCommitCannotPassOldCas() throws Exception {
        VersionedRecord initial = create();
        String path = path();
        first.beforeMulti.set(() -> {
            try {
                for (String child : second.getChildren(path, false)) {
                    second.delete(path + "/" + child, -1);
                }
                second.delete(path, -1);
                assertEquals(CasStatus.APPLIED, store(second).compareAndSet(BOOKIE, -1, 0, record(1)).status());
                assertEquals(0, second.exists(path, false).getVersion());
            } catch (Exception failure) {
                throw new AssertionError(failure);
            }
        });
        assertEquals(CasStatus.CONFLICT, store(first).compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        VersionedRecord replacement = store(second).read(BOOKIE).versionedRecord().orElseThrow();
        assertEquals(record(1), replacement.record());
        assertNotEquals(initial.storeVersion(), replacement.storeVersion());
    }

    @Test
    public void corruptPayloadOrMutableBirthMarkerCannotBeRepairedByCas() throws Exception {
        VersionedRecord initial = create();
        String path = path();
        byte[] original = first.getData(path, false, new Stat());
        byte[] corrupt = original.clone();
        corrupt[0] ^= 1;
        first.setData(path, corrupt, -1);
        assertEquals(ReadStatus.CORRUPT, store(second).read(BOOKIE).status());
        assertEquals(CasStatus.UNAVAILABLE, store(second).compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        first.setData(path, original, -1);
        VersionedRecord repaired = store(second).read(BOOKIE).versionedRecord().orElseThrow();
        String birth = first.getChildren(path, false).get(0);
        assertThrows(org.apache.zookeeper.KeeperException.NoAuthException.class,
                () -> first.setData(path + "/" + birth, new byte[0], 0));
        // The deliberately unsafe administrative parent ACL can remove and replace a child.
        first.delete(path + "/" + birth, 0);
        first.create(path + "/" + birth, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.setData(path + "/" + birth, new byte[0], 0);
        assertEquals(ReadStatus.CORRUPT, store(second).read(BOOKIE).status());
        assertEquals(CasStatus.UNAVAILABLE, store(second).compareAndSet(BOOKIE,
                repaired.storeVersion(), 1, record(2)).status());
        assertEquals(record(1), ProfileReadinessCodec.decode(first.getData(path, false, null)).record());
    }

    @Test
    public void actualAclDenialDoesNotChangePersistentReadiness() throws Exception {
        VersionedRecord initial = create();
        String path = path();
        first.setACL(path, Arrays.asList(new org.apache.zookeeper.data.ACL(
                ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, ZooDefs.Ids.ANYONE_ID_UNSAFE)), -1);
        assertEquals(CasStatus.UNAVAILABLE, store(second).compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        assertEquals(initial, store(first).read(BOOKIE).versionedRecord().orElseThrow());
    }

    @Test
    public void actualCommittedTransactionRemainsUnknownWhenCallbackExceedsDeadline() throws Exception {
        VersionedRecord initial = create();
        first.holdReply.set(true);
        ZKProfileRegistrationStore bounded = new ZKProfileRegistrationStore(first, ROOT,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(2));
        assertEquals(CasStatus.RESPONSE_LOST, bounded.compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
        assertEquals(record(2), store(second).read(BOOKIE).versionedRecord().orElseThrow().record());
        first.lateReply.getAndSet(null).run();
        assertEquals(CasStatus.CONFLICT, bounded.compareAndSet(BOOKIE,
                initial.storeVersion(), 1, record(2)).status());
    }

    @Test
    public void codecRejectsBadChecksumSchemaCountAndTrailingBytes() {
        ProfileReadinessCodec.Envelope envelope = new ProfileReadinessCodec.Envelope(new UUID(1, 2), record(1));
        byte[] bytes = ProfileReadinessCodec.encode(envelope);
        assertEquals(148 + BOOKIE.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 8, bytes.length);
        assertEquals(envelope, ProfileReadinessCodec.decode(bytes));
        byte[] checksum = bytes.clone();
        checksum[50] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileReadinessCodec.decode(checksum));
        byte[] schema = bytes.clone();
        ByteBuffer.wrap(schema).putInt(4, 2);
        checksum(schema);
        assertThrows(IllegalArgumentException.class, () -> ProfileReadinessCodec.decode(schema));
        byte[] count = bytes.clone();
        ByteBuffer.wrap(count).putInt(count.length - 20, Integer.MAX_VALUE);
        checksum(count);
        assertThrows(IllegalArgumentException.class, () -> ProfileReadinessCodec.decode(count));
        byte[] trailing = Arrays.copyOf(bytes, bytes.length + 4);
        checksum(trailing);
        assertThrows(IllegalArgumentException.class, () -> ProfileReadinessCodec.decode(trailing));
    }

    private static void checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }

    /** Faults affect only response delivery or the pre-submit race; every successful write is a real ZK transaction. */
    private static final class TestClient extends ZooKeeper {
        private final AtomicBoolean loseReply = new AtomicBoolean();
        private final AtomicBoolean holdReply = new AtomicBoolean();
        private final AtomicReference<Runnable> beforeMulti = new AtomicReference<>();
        private final AtomicReference<Runnable> lateReply = new AtomicReference<>();

        private TestClient(String connect, Watcher watcher) throws IOException {
            super(connect, 10000, watcher);
        }

        @Override
        public void multi(Iterable<Op> operations, MultiCallback callback, Object context) {
            Runnable before = beforeMulti.getAndSet(null);
            if (before != null) {
                before.run();
            }
            boolean lose = loseReply.getAndSet(false);
            boolean hold = holdReply.getAndSet(false);
            super.multi(operations, (code, path, ignored, results) -> {
                Runnable reply = () -> callback.processResult(lose && code == Code.OK.intValue()
                        ? Code.CONNECTIONLOSS.intValue() : code, path, context, results);
                if (hold) {
                    lateReply.set(reply);
                } else {
                    reply.run();
                }
            }, context);
        }
    }
}

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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real local history and ZK GC resources; no Classic launcher, registration, Unix or adoption qualification. */
public class ClassicHistoryGcTest {
    private static final BookieId SELF = BookieId.parse("127.0.0.1:3181");
    private static final String ROOT = "/classic-owned-gc";

    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 30000)
    public void realHistoryAndClusterMetadataSurviveGcAndBorrowedSourcesStayOpen() throws Exception {
        try (var f = new Fixture()) {
            f.createMetadata(7, SELF);
            f.openHistory();
            f.storage.gcThread.triggerGC().get(5, TimeUnit.SECONDS);
            assertTrue(f.storage.ledgerExists(7));
            f.assertReadable();
            assertTrue(f.storage.closeForFresh().drained());
            assertTrue(f.metadata.state().drained());
            assertNotNull(f.raw.readLedgerMetadata(7).get(5, TimeUnit.SECONDS));
            assertFalse(f.replication.isLedgerBeingReplicated(7));
        }
    }

    @Test(timeout = 30000)
    public void timedOutVerificationOutlivesTheGcTaskAndInterruptedCloseDoesNotCancelIt() throws Exception {
        try (var f = new Fixture()) {
            var existing = f.createMetadata(7, SELF);
            f.raw.removeLedgerMetadata(7, existing.getVersion()).get(5, TimeUnit.SECONDS);
            var gate = new CompletableFuture<Versioned<LedgerMetadata>>();
            var observed = new AtomicReference<Throwable>();
            var delivered = new CountDownLatch(1);
            doAnswer(call -> {
                @SuppressWarnings("unchecked")
                var actual = (CompletableFuture<Versioned<LedgerMetadata>>) call.callRealMethod();
                actual.whenComplete((value, failure) -> {
                    observed.set(failure);
                    delivered.countDown();
                });
                return gate;
            }).when(f.source).readLedgerMetadata(7);
            f.conf.setZkTimeout(50); // Only the GC observation timeout; the real client is already connected.
            f.openHistory();
            var closeFailure = new AtomicReference<Throwable>();
            var interrupted = new AtomicBoolean();
            Thread firstCloser = new Thread(() -> {
                try {
                    f.storage.quiesceMaintenanceForFresh();
                } catch (InterruptedException expected) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                } catch (Throwable unexpected) {
                    closeFailure.set(unexpected);
                }
            }, "historical-gc-interrupted-close");
            FutureTask<InterleavedLedgerStorage.FreshDrainResult> close = new FutureTask<>(f.storage::closeForFresh);
            Thread finalCloser = new Thread(close, "historical-gc-final-close");
            try {
                f.storage.gcThread.triggerGC().get(5, TimeUnit.SECONDS);
                assertTrue(delivered.await(5, TimeUnit.SECONDS));
                assertNotNull(observed.get());
                assertTrue(f.storage.ledgerExists(7));
                assertEquals(1, f.metadata.state().pendingReads());
                f.storage.gcThread.getEntryLogMetaMap().put(999, new EntryLogMetadata(999));
                firstCloser.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> firstCloser.getState() == Thread.State.WAITING);
                firstCloser.interrupt();
                firstCloser.join(5000);
                assertTrue(interrupted.get());
                assertNull(closeFailure.get());
                assertFalse(gate.isCancelled());
                assertFalse(f.storage.gcThread.freshUsersTerminated());
                assertTrue(f.storage.gcThread.getEntryLogMetaMap().containsKey(999));
                finalCloser.start();
                assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
                assertFalse(gate.isDone());
            } finally {
                gate.completeExceptionally(
                        observed.get() == null ? new IOException("fixture cleanup") : observed.get());
                firstCloser.join(5000);
                finalCloser.join(5000);
            }
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            assertTrue(f.metadata.state().drained());
        }
    }

    @Test(timeout = 30000)
    public void overreplicatedGcKeepsItsPermitLatchAndMapUntilActualUnlockReturns() throws Exception {
        try (var f = new Fixture()) {
            f.createMetadata(7, BookieId.parse("127.0.0.1:3182"));
            f.conf.setGcOverreplicatedLedgerWaitTime(1, TimeUnit.MILLISECONDS);
            f.openHistory();
            var unlocked = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            doAnswer(call -> {
                call.callRealMethod();
                assertThrows(IllegalStateException.class, f.storage::closeForFresh);
                unlocked.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
                return null;
            }).when(f.replication).releaseUnderreplicatedLedger(7);
            var close = new FutureTask<>(f.storage::closeForFresh);
            Thread closing = new Thread(close, "historical-unlock-close");
            try {
                long started = System.nanoTime();
                await().atMost(5, TimeUnit.SECONDS).until(() -> System.nanoTime() - started > 5_000_000);
                var cycle = f.storage.gcThread.triggerGC();
                assertTrue(unlocked.await(5, TimeUnit.SECONDS));
                assertFalse(cycle.isDone());
                assertEquals(1, f.metadata.state().heldLocks());
                // Already-complete sources may run cleanup on the GC executor, after retiring the source slot.
                assertTrue(f.metadata.state().actualBodies() > 0);
                closing.start();
                assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
                assertFalse(f.storage.gcThread.freshUsersTerminated());
            } finally {
                release.countDown();
                closing.join(5000);
            }
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            assertEquals(0, f.metadata.state().heldLocks());
            assertTrue(f.metadata.state().drained());
            assertFalse(f.replication.isLedgerBeingReplicated(7));
        }
    }

    @Test(timeout = 30000)
    public void pendingMetadataQueriesAreBoundedAndRejectionDoesNotBecomeAbsence() throws Exception {
        try (var f = new Fixture(1)) {
            f.createMetadata(7, SELF);
            f.createMetadata(8, SELF);
            var gate = new CompletableFuture<Versioned<LedgerMetadata>>();
            var actual = new AtomicReference<CompletableFuture<Versioned<LedgerMetadata>>>();
            doAnswer(call -> {
                @SuppressWarnings("unchecked")
                var read = (CompletableFuture<Versioned<LedgerMetadata>>) call.callRealMethod();
                actual.set(read);
                return gate;
            }).when(f.source).readLedgerMetadata(7);
            var first = f.metadata.read(7);
            try {
                assertEquals(1, f.metadata.state().pendingReads());
                ExecutionException rejected = assertThrows(ExecutionException.class,
                        () -> f.metadata.read(8).get(5, TimeUnit.SECONDS));
                assertEquals(BKException.Code.MetaStoreException, ((BKException) rejected.getCause()).getCode());
                assertEquals(1, f.metadata.state().pendingReads());
            } finally {
                gate.complete(actual.get().get(5, TimeUnit.SECONDS));
            }
            assertEquals(7, first.get(5, TimeUnit.SECONDS).getValue().getLedgerId());
            assertTrue(f.metadata.closeAndDrain().drained());
            assertNotNull(f.raw.readLedgerMetadata(8).get(5, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 30000)
    public void unknownSubmissionRetainsItsOriginalSlotAfterTheActualReadHasFinished() throws Exception {
        try (var f = new Fixture()) {
            f.createMetadata(7, SELF);
            var actual = new AtomicReference<CompletableFuture<Versioned<LedgerMetadata>>>();
            var failure = new IllegalStateException("result lost after actual metadata submission");
            doAnswer(call -> {
                assertThrows(IllegalStateException.class, f.metadata::closeAndDrain);
                @SuppressWarnings("unchecked")
                var submitted = (CompletableFuture<Versioned<LedgerMetadata>>) call.callRealMethod();
                actual.set(submitted);
                throw failure;
            }).when(f.source).readLedgerMetadata(7);
            f.allowUnresolved = true;
            assertSame(failure, assertThrows(ExecutionException.class,
                    () -> f.metadata.read(7).get(5, TimeUnit.SECONDS)).getCause());
            assertNotNull(actual.get().get(5, TimeUnit.SECONDS));
            assertFalse(f.metadata.closeAndDrain().drained());
            assertSame(failure, f.metadata.state().firstFailure());
            assertEquals(1, f.metadata.state().pendingReads());
            assertFalse(f.metadata.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void knownUnusedLeaseGetsOneReleaseButFailedAcquireOrReleaseNeverPretendsToDrain() throws Exception {
        try (var f = new Fixture()) {
            f.metadata.acquire(7);
            assertTrue(f.replication.isLedgerBeingReplicated(7));
            assertTrue(f.metadata.closeAndDrain().drained());
            assertFalse(f.replication.isLedgerBeingReplicated(7));
        }
        for (boolean afterNativeDelete : new boolean[]{false, true}) {
            try (var f = new Fixture()) {
                var lease = f.metadata.acquire(7);
                var attempts = new AtomicInteger();
                var failure = new ReplicationException.UnavailableException(
                        "first release has no definite result", new IOException("controlled response failure"));
                doAnswer(call -> {
                    attempts.incrementAndGet();
                    if (afterNativeDelete) {
                        call.callRealMethod();
                    }
                    throw failure;
                }).when(f.replication).releaseUnderreplicatedLedger(7);
                f.allowUnresolved = true;
                assertSame(failure, assertThrows(ReplicationException.UnavailableException.class,
                        () -> f.metadata.release(lease)));
                assertThrows(IOException.class, () -> f.metadata.release(lease));
                assertFalse(f.metadata.closeAndDrain().drained());
                assertFalse(f.metadata.closeAndDrain().drained());
                assertEquals(1, attempts.get());
                assertEquals(!afterNativeDelete, f.replication.isLedgerBeingReplicated(7));
            }
        }
        try (var f = new Fixture()) {
            var failure = new ReplicationException.UnavailableException(
                    "acquire result lost after actual guarded create", new IOException("controlled response failure"));
            doAnswer(call -> {
                call.callRealMethod();
                throw failure;
            }).when(f.replication).acquireUnderreplicatedLedger(7);
            f.allowUnresolved = true;
            assertSame(failure, assertThrows(ReplicationException.UnavailableException.class,
                    () -> f.metadata.acquire(7)));
            assertTrue(f.replication.isLedgerBeingReplicated(7));
            assertFalse(f.metadata.closeAndDrain().drained());
            assertEquals(1, f.metadata.state().heldLocks());
        }
    }

    @Test(timeout = 30000)
    public void definiteOccupiedLockDoesNotDeleteTheOtherTaskOrPoisonAdmission() throws Exception {
        try (var f = new Fixture()) {
            var other = new ZkLedgerUnderreplicationManager(f.conf, f.zk.getZooKeeperClient());
            try {
                other.acquireUnderreplicatedLedger(7);
                assertThrows(Exception.class, () -> f.metadata.acquire(7));
                assertEquals(0, f.metadata.state().heldLocks());
                assertNull(f.metadata.state().firstFailure());
                assertTrue(f.metadata.closeAndDrain().drained());
                assertTrue(other.isLedgerBeingReplicated(7));
            } finally {
                other.close();
            }
        }
    }

    private final class Fixture implements AutoCloseable {
        private final ZooKeeperUtil zk = new ZooKeeperUtil();
        private final ServerConfiguration conf;
        private final LedgerManagerFactory factory;
        private final AbstractZkLedgerManager raw;
        private final AbstractZkLedgerManager source;
        private final ZkLedgerUnderreplicationManager replication;
        private final ClassicGcMetadata metadata;
        private final LedgerDirsManager ledgerDirectories;
        private final LedgerDirsManager indexDirectories;
        private InterleavedLedgerStorage storage;
        private boolean allowUnresolved;

        private Fixture() throws Exception {
            this(2);
        }

        private Fixture(int limit) throws Exception {
            zk.startCluster();
            zk.getZooKeeperClient().create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            var ledger = temporary.newFolder();
            var index = temporary.newFolder();
            Files.createDirectory(ledger.toPath().resolve("current"));
            Files.createDirectory(index.toPath().resolve("current"));
            conf = new ServerConfiguration().setBookieId(SELF.toString()).setAllowLoopback(true)
                    .setLedgerDirNames(new String[]{ledger.getCanonicalPath()})
                    .setIndexDirName(new String[]{index.getCanonicalPath()})
                    .setJournalDirName(temporary.newFolder().getCanonicalPath())
                    .setMetadataServiceUri(zk.getMetadataServiceUri(ROOT, "hierarchical"))
                    .setFileInfoFormatVersionToWrite(1).setJournalFormatVersionToWrite(6)
                    .setGcWaitTime(TimeUnit.HOURS.toMillis(1)).setVerifyMetadataOnGc(true)
                    .setGcOverreplicatedLedgerMaxConcurrentRequests(limit)
                    .setEntryLogFilePreAllocationEnabled(false).setWriteBufferBytes(4096).setReadBufferBytes(4096);
            factory = AbstractZkLedgerManagerFactory.newLedgerManagerFactory(conf,
                    new ZkLayoutManager(zk.getZooKeeperClient(), ROOT, ZooDefs.Ids.OPEN_ACL_UNSAFE));
            raw = (AbstractZkLedgerManager) factory.newLedgerManager();
            source = spy(raw);
            replication = spy(new ZkLedgerUnderreplicationManager(conf, zk.getZooKeeperClient()));
            metadata = new ClassicGcMetadata(source, replication, limit);
            ledgerDirectories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
            indexDirectories = new LedgerDirsManager(conf, conf.getIndexDirs(), new DiskChecker(.999f, .99f));
        }

        private Versioned<LedgerMetadata> createMetadata(long ledger, BookieId bookie) throws Exception {
            var value = LedgerMetadataBuilder.create().withId(ledger).withDigestType(DigestType.CRC32C)
                    .withPassword(new byte[0]).withEnsembleSize(1).withWriteQuorumSize(1).withAckQuorumSize(1)
                    .newEnsembleEntry(0, List.of(bookie)).withClosedState().withLastEntryId(0).withLength(24).build();
            return raw.createLedgerMetadata(ledger, value).get(5, TimeUnit.SECONDS);
        }

        private void openHistory() throws Exception {
            InterleavedLedgerStorage first = InterleavedLedgerStorage.forFresh();
            first.setCheckpointSource(CheckpointSource.DEFAULT);
            first.setCheckpointer(Checkpointer.NULL);
            try {
                first.initializeForFresh(conf, ledgerDirectories, indexDirectories,
                        NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
                first.setMasterKey(7, new byte[]{1, 2, 3});
                ByteBuf entry = Unpooled.buffer(24).writeLong(7).writeLong(0).writeLong(-1);
                try {
                    first.addEntry(entry);
                } finally {
                    entry.release();
                }
                first.flush();
            } finally {
                assertTrue(first.closeForFresh().drained());
            }
            storage = InterleavedLedgerStorage.forClassicHistory();
            storage.setCheckpointSource(CheckpointSource.DEFAULT);
            storage.setCheckpointer(Checkpointer.NULL);
            storage.initializeForClassicHistory(conf, metadata, ledgerDirectories, indexDirectories,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
            storage.start();
        }

        private void assertReadable() throws Exception {
            ByteBuf read = storage.getEntry(7, 0);
            try {
                assertEquals(7, read.getLong(0));
                assertEquals(0, read.getLong(8));
            } finally {
                read.release();
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (storage != null) {
                    assertTrue(storage.closeForFresh().drained());
                }
                if (!allowUnresolved) {
                    assertTrue(metadata.closeAndDrain().drained());
                }
            } finally {
                try {
                    replication.close();
                    assertTrue(raw.closeAndAwaitTermination(Duration.ofSeconds(5)));
                    factory.close();
                } finally {
                    zk.stopCluster();
                }
            }
        }
    }
}

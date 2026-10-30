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
package org.apache.bookkeeper.bookie;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual local storage plus the fresh GC child, not the still-unwired complete fresh lifecycle. */
public class FreshClassicGcDrainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void localLedgerSurvivesAnActualGcCycleWithoutClusterDeleteAuthority() throws Exception {
        var scans = new AtomicBoolean();
        try (var fixture = new Fixture(() -> scans.set(true))) {
            assertFalse(fixture.conf.getVerifyMetadataOnGC());
            assertTrue(fixture.conf.isEntryLogFilePreAllocationEnabled());
            assertTrue(fixture.gc.enableMajorCompaction);
            assertTrue(fixture.gc.enableMinorCompaction);
            fixture.writeLedger();
            fixture.gc.start();
            fixture.gc.triggerGC().get(5, TimeUnit.SECONDS);
            assertTrue(scans.get());
            assertFalse(fixture.gc.getEntryLogMetaMap().isEmpty());
            assertTrue(fixture.storage.ledgerExists(7));
            fixture.assertReadable();
        }
    }

    @Test
    public void cancelledScanAndInterruptedCloserRetainMapUntilActualFileScanReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var scanInterrupted = new AtomicBoolean();
        try (var fixture = new Fixture(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException unexpected) {
                scanInterrupted.set(true);
                Thread.currentThread().interrupt();
                throw new AssertionError(unexpected);
            }
        })) {
            fixture.writeLedger();
            fixture.gc.getEntryLogMetaMap().put(999, new EntryLogMetadata(999));
            var interrupted = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var closer = new Thread(() -> {
                try {
                    fixture.gc.shutdownForFresh();
                } catch (InterruptedException expected) {
                    interrupted.set(Thread.currentThread().isInterrupted());
                } catch (Throwable unexpected) {
                    failure.set(unexpected);
                }
            });
            var secondClose = new FutureTask<Void>(() -> {
                fixture.gc.shutdownForFresh();
                return null;
            });
            var secondCloser = new Thread(secondClose);
            try {
                var scan = fixture.gc.triggerGC();
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertTrue(scan.cancel(false));
                assertTrue(scan.isDone());
                closer.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> !fixture.gc.running);
                closer.interrupt();
                closer.join(5000);
                assertFalse(closer.isAlive());
                assertTrue(interrupted.get());
                assertTrue(failure.get() == null);
                assertTrue(fixture.gc.getEntryLogMetaMap().containsKey(999));
                secondCloser.start();
                assertThrows(TimeoutException.class, () -> secondClose.get(100, TimeUnit.MILLISECONDS));
                assertTrue(fixture.gc.getEntryLogMetaMap().containsKey(999));
                release.countDown();
                secondClose.get(5, TimeUnit.SECONDS);
                assertFalse(scanInterrupted.get());
                assertFalse(fixture.scanThread.isAlive());
                assertTrue(fixture.gc.getEntryLogMetaMap().isEmpty());
                fixture.gc.shutdownForFresh();
                fixture.assertReadable();
            } finally {
                release.countDown();
                closer.join(5000);
                secondCloser.join(5000);
            }
        }
    }

    @Test
    public void gcSelfWaitUsesActualThreadIdentityAndUnstartedCloseIsRepeatable() throws Exception {
        var reference = new AtomicReference<GarbageCollectorThread>();
        var checked = new AtomicBoolean();
        try (var fixture = new Fixture(() -> {
            Thread.currentThread().setName("renamed-fresh-gc");
            assertTrue(reference.get().isCurrentExecutorThread());
            assertThrows(IllegalStateException.class, reference.get()::shutdownForFresh);
            assertThrows(IllegalStateException.class, reference.get()::shutdown);
            checked.set(true);
        })) {
            reference.set(fixture.gc);
            fixture.writeLedger();
            fixture.gc.triggerGC().get(5, TimeUnit.SECONDS);
            assertTrue(checked.get());
            assertTrue(fixture.gc.running);
        }
        try (var fixture = new Fixture(() -> { })) {
            assertThrows(IllegalStateException.class, () -> {
                var ordinary = new GarbageCollectorThread(fixture.conf, null, fixture.directories,
                        fixture.storage, fixture.logger, NullStatsLogger.INSTANCE);
                try {
                    ordinary.shutdownForFresh();
                } finally {
                    ordinary.shutdown();
                }
            });
            fixture.gc.shutdownForFresh();
            fixture.gc.shutdownForFresh();
        }
    }

    @Test
    public void persistentMapAndAnyNonemptyCachePathAreRejectedBeforeConstruction() {
        var conf = new ServerConfiguration().setGcEntryLogMetadataCacheEnabled(true);
        assertThrows(IllegalArgumentException.class,
                () -> GarbageCollectorThread.forFresh(conf, null, null, null, NullStatsLogger.INSTANCE));
        assertTrue(conf.isGcEntryLogMetadataCacheEnabled());
        conf.setGcEntryLogMetadataCacheEnabled(false);
        conf.setGcEntryLogMetadataCachePath(" ");
        assertEquals("", conf.getGcEntryLogMetadataCachePath());
        // Preserve the supplied value; the default handler trims whitespace before create sees it.
        conf.setListDelimiterHandler(DisabledListDelimiterHandler.INSTANCE);
        for (Object path : new Object[]{"unused-cache", " ", List.of("", "unused-cache"), 1}) {
            conf.setProperty("gcEntryLogMetadataCachePath", path);
            assertEquals(path, conf.getProperty("gcEntryLogMetadataCachePath"));
            assertThrows(IllegalArgumentException.class,
                    () -> GarbageCollectorThread.forFresh(conf, null, null, null, NullStatsLogger.INSTANCE));
            assertEquals(path, conf.getProperty("gcEntryLogMetadataCachePath"));
        }
    }

    private final class Fixture implements AutoCloseable {
        private final ServerConfiguration conf;
        private final LedgerDirsManager directories;
        private final DefaultEntryLogger logger;
        private final InterleavedLedgerStorage storage = new InterleavedLedgerStorage();
        private final GarbageCollectorThread gc;
        private volatile Thread scanThread;

        private Fixture(Runnable beforeScan) throws Exception {
            var root = temporary.newFolder();
            Files.createDirectory(root.toPath().resolve("current"));
            conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()})
                    .setJournalDirName(root.getCanonicalPath()).setAllowLoopback(true)
                    .setAdvertisedAddress("127.0.0.1").setEntryLogSizeLimit(20000).setVerifyMetadataOnGc(false);
            directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.999f, 0.99f));
            logger = new DefaultEntryLogger(conf, directories, null, NullStatsLogger.INSTANCE,
                    UnpooledByteBufAllocator.DEFAULT) {
                @Override
                public EntryLogMetadata getEntryLogMetadata(long entryLogId, AbstractLogCompactor.Throttler throttler)
                        throws IOException {
                    scanThread = Thread.currentThread();
                    beforeScan.run();
                    return super.getEntryLogMetadata(entryLogId, throttler);
                }
            };
            storage.initializeWithEntryLogger(conf, null, directories, null, logger, NullStatsLogger.INSTANCE);
            // Replace only the dormant child in test source; the production fresh owner is not wired yet.
            storage.gcThread.shutdown();
            gc = GarbageCollectorThread.forFresh(conf, directories, storage, logger, NullStatsLogger.INSTANCE);
            storage.gcThread = gc;
        }

        private void writeLedger() throws Exception {
            storage.setMasterKey(7, new byte[]{1, 2, 3});
            for (int id = 0; id < 5; id++) {
                ByteBuf entry = Unpooled.buffer(5000).writeZero(5000);
                try {
                    entry.setLong(0, 7).setLong(8, id).setLong(16, id - 1);
                    storage.addEntry(entry);
                } finally {
                    entry.release();
                }
            }
            storage.flush();
            assertFalse(logger.getFlushedLogIds().isEmpty());
        }

        private void assertReadable() throws Exception {
            for (int id = 0; id < 5; id++) {
                ByteBuf entry = storage.getEntry(7, id);
                try {
                    assertEquals(7, entry.getLong(0));
                    assertEquals(id, entry.getLong(8));
                    assertEquals(5000, entry.readableBytes());
                } finally {
                    entry.release();
                }
            }
        }

        @Override
        public void close() throws Exception {
            storage.shutdown();
        }
    }
}

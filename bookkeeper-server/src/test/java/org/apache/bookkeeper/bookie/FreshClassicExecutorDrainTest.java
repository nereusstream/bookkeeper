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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Child-executor termination only; not the still-unwired complete fresh owner or Bookie drain. */
public class FreshClassicExecutorDrainTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void stateManagerInterruptionRetainsTheOriginalExecutorUntilItsActualTaskReturns() throws Exception {
        var manager = new BookieStateManager(new ServerConfiguration(), NullStatsLogger.INSTANCE,
                new NullMetadataBookieDriver.NullRegistrationManager(), List.of(),
                () -> BookieId.parse("fresh-state:3181"), BookieServiceInfo::new);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interrupted = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        manager.stateService.submit(() -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException unexpected) {
                Thread.currentThread().interrupt();
                failure.set(unexpected);
            }
        });
        var closer = new Thread(() -> {
            try {
                manager.closeForFresh();
            } catch (InterruptedException expected) {
                interrupted.set(Thread.currentThread().isInterrupted());
            } catch (Throwable unexpected) {
                failure.set(unexpected);
            }
        });
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            closer.interrupt();
            closer.join(5000);
            assertFalse(closer.isAlive());
            assertTrue(interrupted.get());
            assertTrue(manager.stateService.isShutdown());
            assertFalse(manager.stateService.isTerminated());
            assertTrue(failure.get() == null);
            release.countDown();
            manager.closeForFresh();
            manager.closeForFresh();
            assertTrue(manager.stateService.isTerminated());
        } finally {
            release.countDown();
            closer.join(5000);
            manager.closeForFresh();
        }
    }

    @Test
    public void stateManagerCannotAwaitItsOwnThreadAfterRename() throws Exception {
        var manager = new BookieStateManager(new ServerConfiguration(), NullStatsLogger.INSTANCE,
                new NullMetadataBookieDriver.NullRegistrationManager(), List.of(),
                () -> BookieId.parse("fresh-state:3181"), BookieServiceInfo::new);
        try {
            manager.stateService.submit(() -> {
                Thread.currentThread().setName("renamed-state-service");
                assertTrue(manager.isCurrentExecutorThread());
                assertThrows(IllegalStateException.class, manager::closeForFresh);
            }).get(5, TimeUnit.SECONDS);
            assertFalse(manager.stateService.isShutdown());
        } finally {
            manager.closeForFresh();
        }
    }

    @Test
    public void cancelledDiskCheckCannotBeReportedDrainedUntilItsActualIoReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var armed = new AtomicBoolean();
        var observedCancellation = new AtomicBoolean();
        var cancellationSeen = new CountDownLatch(1);
        var checker = new DiskChecker(0.999f, 0.99f) {
            @Override
            public float checkDir(File directory) throws DiskErrorException,
                    DiskOutOfSpaceException, DiskWarnThresholdException {
                if (armed.get()) {
                    entered.countDown();
                    boolean interrupted = false;
                    while (true) {
                        try {
                            release.await();
                            break;
                        } catch (InterruptedException cancellation) {
                            interrupted = true;
                            observedCancellation.set(true);
                            cancellationSeen.countDown();
                        }
                    }
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.checkDir(directory);
            }
        };
        var root = temporary.newFolder("disk-monitor");
        var conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()})
                .setDiskCheckInterval(10).setDiskUsageThreshold(0.999f).setDiskUsageWarnThreshold(0.99f);
        var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), checker, NullStatsLogger.INSTANCE);
        var monitor = new LedgerDirsMonitor(conf, checker, List.of(directories));
        var closing = new FutureTask<Void>(() -> {
            monitor.shutdownForFresh();
            return null;
        });
        var closer = new Thread(closing);
        try {
            armed.set(true);
            monitor.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertTrue(cancellationSeen.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            assertFalse(closing.isDone());
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);
            assertTrue(observedCancellation.get());
            monitor.shutdownForFresh();
        } finally {
            release.countDown();
            closer.join(5000);
            monitor.shutdownForFresh();
        }
    }

    @Test
    public void diskMonitorSelfWaitIsRejectedAndAnUnstartedMonitorCanClose() throws Exception {
        var reference = new AtomicReference<LedgerDirsMonitor>();
        var checked = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var checker = new DiskChecker(0.999f, 0.99f) {
            @Override
            public float checkDir(File directory) throws DiskErrorException,
                    DiskOutOfSpaceException, DiskWarnThresholdException {
                if (reference.get() != null) {
                    try {
                        assertTrue(reference.get().isCurrentExecutorThread());
                        assertThrows(IllegalStateException.class, reference.get()::shutdownForFresh);
                    } catch (Throwable unexpected) {
                        failure.set(unexpected);
                    } finally {
                        checked.countDown();
                    }
                }
                return super.checkDir(directory);
            }
        };
        var root = temporary.newFolder("self-monitor");
        var conf = new ServerConfiguration().setLedgerDirNames(new String[]{root.getCanonicalPath()})
                .setDiskCheckInterval(10).setDiskUsageThreshold(0.999f).setDiskUsageWarnThreshold(0.99f);
        var directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), checker, NullStatsLogger.INSTANCE);
        var unstarted = new LedgerDirsMonitor(conf, checker, List.of(directories));
        unstarted.shutdownForFresh();
        var monitor = new LedgerDirsMonitor(conf, checker, List.of(directories));
        reference.set(monitor);
        try {
            monitor.start();
            assertTrue(checked.await(5, TimeUnit.SECONDS));
            assertTrue(failure.get() == null);
        } finally {
            monitor.shutdownForFresh();
        }
    }
}

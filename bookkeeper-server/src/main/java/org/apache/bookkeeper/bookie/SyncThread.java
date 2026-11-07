/*
 *
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
 *
 */

package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;

/**
 * SyncThread is a background thread which help checkpointing ledger storage
 * when a checkpoint is requested. After a ledger storage is checkpointed,
 * the journal files added before checkpoint will be garbage collected.
 * <p>
 * After all data has been persisted to ledger index files and entry
 * loggers, it is safe to complete a checkpoint by persisting the log marker
 * to disk. If bookie failed after persist log mark, bookie is able to relay
 * journal entries started from last log mark without losing any entries.
 * </p>
 * <p>
 * Those journal files whose id are less than the log id in last log mark,
 * could be removed safely after persisting last log mark. We provide a
 * setting to let user keeping number of old journal files which may be used
 * for manual recovery in critical disaster.
 * </p>
 */
@CustomLog
class SyncThread implements Checkpointer {

    @Getter(AccessLevel.PACKAGE)
    final ScheduledExecutorService executor;
    final LedgerStorage ledgerStorage;
    final LedgerDirsListener dirsListener;
    final CheckpointSource checkpointSource;

    private final Object suspensionLock = new Object();
    private boolean suspended = false;
    private boolean disableCheckpoint = false;
    private final Counter syncExecutorTime;
    private static String executorName = "SyncThread";
    private final boolean freshLifecycle;
    private volatile Thread freshExecutorThread;
    private Future<?> freshFinalFlush;
    private boolean freshFlushObserved;
    private boolean freshFlushComplete;

    public SyncThread(ServerConfiguration conf,
                      LedgerDirsListener dirsListener,
                      LedgerStorage ledgerStorage,
                      CheckpointSource checkpointSource,
                      StatsLogger statsLogger) {
        this(conf, dirsListener, ledgerStorage, checkpointSource, statsLogger, false);
    }

    private SyncThread(ServerConfiguration conf, LedgerDirsListener dirsListener, LedgerStorage ledgerStorage,
            CheckpointSource checkpointSource, StatsLogger statsLogger, boolean freshLifecycle) {
        this.dirsListener = dirsListener;
        this.ledgerStorage = ledgerStorage;
        this.checkpointSource = checkpointSource;
        this.freshLifecycle = freshLifecycle;
        this.executor = freshLifecycle ? newFreshExecutor() : newExecutor();
        this.syncExecutorTime = statsLogger.getThreadScopedCounter("sync-thread-time");
    }

    /** Resource construction only; the fresh owner registers this child before starting any work. */
    static SyncThread forFresh(ServerConfiguration conf, LedgerDirsListener dirsListener, LedgerStorage storage,
            CheckpointSource checkpoints, StatsLogger stats) {
        return new SyncThread(conf, dirsListener, storage, checkpoints, stats, true);
    }

    private ScheduledExecutorService newFreshExecutor() {
        return Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory(executorName) {
            @Override
            protected Thread newThread(Runnable runnable, String name) {
                Thread thread = super.newThread(ThreadRegistry.registerThread(runnable, executorName), name);
                freshExecutorThread = thread;
                return thread;
            }
        });
    }

    @VisibleForTesting
    static ScheduledExecutorService newExecutor() {
        return Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory(executorName) {
            @Override
            protected Thread newThread(Runnable r, String name) {
                return super.newThread(ThreadRegistry.registerThread(r, executorName), name);
            }
        });
    }

    @Override
    public void startCheckpoint(Checkpoint checkpoint) {
        doCheckpoint(checkpoint);
    }

    protected void doCheckpoint(Checkpoint checkpoint) {
        executor.submit(() -> {
            long startTime = System.nanoTime();
            try {
                synchronized (suspensionLock) {
                    while (suspended) {
                        try {
                            suspensionLock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            continue;
                        }
                    }
                }
                if (!disableCheckpoint) {
                    checkpoint(checkpoint);
                }
            } catch (Throwable t) {
                log.error().exception(t).log("Exception in SyncThread");
                dirsListener.fatalError();
            } finally {
                syncExecutorTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            }
        });
    }

    public Future requestFlush() {
        return executor.submit(() -> {
            long startTime = System.nanoTime();
            try {
                flush();
            } catch (Throwable t) {
                log.error().exception(t).log("Exception flushing ledgers");
            } finally {
                syncExecutorTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            }
        });
    }

    private void flush() {
        Checkpoint checkpoint = checkpointSource.newCheckpoint();
        try {
            ledgerStorage.flush();
        } catch (NoWritableLedgerDirException e) {
            log.error().exception(e).log("No writeable ledger directories");
            dirsListener.allDisksFull(true);
            return;
        } catch (IOException e) {
            log.error().exception(e).log("Exception flushing ledgers");
            return;
        }

        if (disableCheckpoint) {
            return;
        }

        log.info().attr("checkpoint", checkpoint).log("Flush ledger storage at checkpoint");
        try {
            checkpointSource.checkpointComplete(checkpoint, false);
        } catch (IOException e) {
            log.error().exception(e).log("Exception marking checkpoint as complete");
            dirsListener.allDisksFull(true);
        }
    }

    @VisibleForTesting
    public void checkpoint(Checkpoint checkpoint) {
        if (null == checkpoint) {
            // do nothing if checkpoint is null
            return;
        }

        try {
            ledgerStorage.checkpoint(checkpoint);
        } catch (NoWritableLedgerDirException e) {
            log.error().exception(e).log("No writeable ledger directories");
            dirsListener.allDisksFull(true);
            return;
        } catch (IOException e) {
            log.error().exception(e).log("Exception flushing ledgers");
            return;
        }

        try {
            checkpointSource.checkpointComplete(checkpoint, true);
        } catch (IOException e) {
            log.error().exception(e).log("Exception marking checkpoint as complete");
            dirsListener.allDisksFull(true);
        }
    }

    @Override
    public void start() {
        // no-op
    }

    /**
     * Suspend sync thread. (for testing)
     */
    @VisibleForTesting
    public void suspendSync() {
        synchronized (suspensionLock) {
            suspended = true;
        }
    }

    /**
     * Resume sync thread. (for testing)
     */
    @VisibleForTesting
    public void resumeSync() {
        synchronized (suspensionLock) {
            suspended = false;
            suspensionLock.notify();
        }
    }

    @VisibleForTesting
    public void disableCheckpoint() {
        disableCheckpoint = true;
    }

    // shutdown sync thread
    void shutdown() throws InterruptedException {
        log.info("Shutting down SyncThread");
        requestFlush();

        executor.shutdown();
        long start = System.currentTimeMillis();
        while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            long now = System.currentTimeMillis();
            log.info().attr("elapsedMs", now - start)
                .log("SyncThread taking a long time to shutdown");
        }
    }

    /**
     * Fresh owner's external close worker only, after producers stop submitting checkpoints.
     * Interruption retains the original flush and executor; a failed final flush is reported and can be
     * retried directly only after that executor has actually terminated. No closed-executor resubmission.
     */
    void shutdownForFresh() throws IOException, InterruptedException {
        if (!freshLifecycle || Thread.currentThread() == freshExecutorThread) {
            throw new IllegalStateException("Fresh SyncThread drain requires its external lifecycle owner");
        }
        try {
            synchronized (this) {
                if (freshFlushComplete) {
                    return;
                }
                if (freshFinalFlush == null) {
                    freshFinalFlush = requestFlushForFresh();
                    executor.shutdown();
                }
                while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    log.info("Fresh SyncThread is still draining its original executor");
                }
                if (freshExecutorThread != null) {
                    freshExecutorThread.join();
                }
                if (!freshFlushObserved) {
                    freshFlushObserved = true;
                    try {
                        freshFinalFlush.get();
                    } catch (ExecutionException failed) {
                        Throwable cause = failed.getCause();
                        if (cause instanceof IOException failure) {
                            throw failure;
                        }
                        if (cause instanceof RuntimeException failure) {
                            throw failure;
                        }
                        if (cause instanceof Error failure) {
                            throw failure;
                        }
                        throw new IOException("Fresh final flush failed", cause);
                    }
                } else {
                    // The original attempt failed, and no executor task can still access this storage.
                    flushForFresh();
                }
                freshFlushComplete = true;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    /** Report the actual flush and checkpoint result to the owned Classic lifecycle. */
    Future<?> requestFlushForFresh() {
        if (!freshLifecycle) {
            throw new IllegalStateException("Strict flush requires the owned Classic SyncThread");
        }
        return executor.submit(() -> {
            flushForFresh();
            return null;
        });
    }

    private void flushForFresh() throws IOException {
        Checkpoint checkpoint = checkpointSource.newCheckpoint();
        ledgerStorage.flush();
        if (!disableCheckpoint) {
            checkpointSource.checkpointComplete(checkpoint, false);
        }
    }

    boolean isCurrentExecutorThread() {
        return Thread.currentThread() == freshExecutorThread;
    }
}

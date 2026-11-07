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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.bookie.TransactionalEntryLogCompactor.COMPACTING_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * An allocator pre-allocates entry log files.
 */
@CustomLog
class EntryLoggerAllocator {

    private long preallocatedLogId;
    Future<BufferedLogChannel> preallocation = null;
    ExecutorService allocatorExecutor;
    private final ServerConfiguration conf;
    private final LedgerDirsManager ledgerDirsManager;
    private final Object createEntryLogLock = new Object();
    private final Object createCompactionLogLock = new Object();
    private final DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private final boolean entryLogPreAllocationEnabled;
    private final ByteBufAllocator byteBufAllocator;
    final ByteBuf logfileHeader = Unpooled.buffer(DefaultEntryLogger.LOGFILE_HEADER_SIZE);
    private volatile long writingLogId = -1;
    private volatile long writingCompactingLogId = -1;
    private final boolean freshLifecycle;
    private final List<FreshAllocation> freshAllocations;
    private final Object freshCloseLock = new Object();
    private final Object freshSubmissionLock = new Object();
    private volatile Thread freshExecutorThread;
    private volatile boolean freshStopRequested;
    private boolean freshHeaderReleased;
    private Throwable freshAllocationFailure;
    private Throwable freshHeaderFailure;
    private FreshDrainResult freshDrainResult;

    EntryLoggerAllocator(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
                         DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus, long logId,
                         ByteBufAllocator byteBufAllocator) {
        this(conf, ledgerDirsManager, recentlyCreatedEntryLogsStatus, logId, byteBufAllocator, false);
    }

    static EntryLoggerAllocator forFresh(ServerConfiguration conf, LedgerDirsManager directories,
            DefaultEntryLogger.RecentEntryLogsStatus status, long logId, ByteBufAllocator allocator) {
        return new EntryLoggerAllocator(conf, directories, status, logId, allocator, true);
    }

    private EntryLoggerAllocator(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus, long logId,
            ByteBufAllocator byteBufAllocator, boolean freshLifecycle) {
        this.freshLifecycle = freshLifecycle;
        this.freshAllocations = freshLifecycle ? new ArrayList<>() : null;
        this.conf = conf;
        this.byteBufAllocator = byteBufAllocator;
        this.ledgerDirsManager = ledgerDirsManager;
        this.preallocatedLogId = logId;
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();
        this.allocatorExecutor = freshLifecycle ? newFreshExecutor() : Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("EntryLoggerAllocator"));

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        logfileHeader.setZero(0, DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        logfileHeader.writeBytes("BKLO".getBytes(UTF_8));
        logfileHeader.writeInt(DefaultEntryLogger.HEADER_CURRENT_VERSION);
        logfileHeader.writerIndex(DefaultEntryLogger.LOGFILE_HEADER_SIZE);

    }

    private ExecutorService newFreshExecutor() {
        return Executors.newSingleThreadExecutor(new DefaultThreadFactory("EntryLoggerAllocator") {
            @Override
            protected Thread newThread(Runnable runnable, String name) {
                Thread thread = super.newThread(runnable, name);
                freshExecutorThread = thread;
                return thread;
            }
        });
    }

    boolean isCurrentExecutorThread() {
        return Thread.currentThread() == freshExecutorThread;
    }

    synchronized long getPreallocatedLogId() {
        return preallocatedLogId;
    }

    public boolean isSealed(long logId) {
        return logId != writingLogId && logId != writingCompactingLogId;
    }

    BufferedLogChannel createNewLog(File dirForNextEntryLog) throws IOException {
        synchronized (createEntryLogLock) {
            checkFreshAdmission();
            BufferedLogChannel bc;
            if (!entryLogPreAllocationEnabled) {
                // create a new log directly
                bc = allocateNewLog(dirForNextEntryLog);
            } else {
                // allocate directly to response request
                if (null == preallocation) {
                    bc = allocateNewLog(dirForNextEntryLog);
                } else {
                    // has a preallocated entry log
                    try {
                        bc = preallocation.get();
                    } catch (ExecutionException ee) {
                        if (ee.getCause() instanceof IOException) {
                            throw (IOException) (ee.getCause());
                        } else {
                            throw new IOException("Error to execute entry log allocation.", ee);
                        }
                    } catch (CancellationException ce) {
                        throw new IOException("Task to allocate a new entry log is cancelled.", ce);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted when waiting a new entry log to be allocated.", ie);
                    }
                }
                // preallocate a new log in background upon every call
                if (freshLifecycle) {
                    synchronized (freshSubmissionLock) {
                        if (!freshStopRequested) {
                            preallocation = allocatorExecutor.submit(() -> allocateNewLog(dirForNextEntryLog));
                        }
                    }
                } else {
                    preallocation = allocatorExecutor.submit(() -> allocateNewLog(dirForNextEntryLog));
                }
            }
            handoffForFresh(bc);
            return bc;
        }
    }

    BufferedLogChannel createNewLogForCompaction(File dirForNextEntryLog) throws IOException {
        synchronized (createCompactionLogLock) {
            checkFreshAdmission();
            BufferedLogChannel channel = allocateNewLog(dirForNextEntryLog, COMPACTING_SUFFIX);
            handoffForFresh(channel);
            return channel;
        }
    }

    private void checkFreshAdmission() throws IOException {
        if (freshLifecycle && freshStopRequested) {
            throw new IOException("Fresh entry-log allocation is stopped");
        }
    }

    private synchronized void handoffForFresh(BufferedLogChannel channel) {
        if (freshLifecycle) {
            boolean removed = freshAllocations.removeIf(allocation -> allocation.buffered == channel);
            if (!removed) {
                throw new IllegalStateException("Fresh entry-log candidate has no owner");
            }
        }
    }

    void setWritingLogId(long logId) {
        this.writingLogId = logId;
    }

    void setWritingCompactingLogId(long logId) {
        this.writingCompactingLogId = logId;
    }

    void clearCompactingLogId() {
        writingCompactingLogId = -1;
    }

    private synchronized BufferedLogChannel allocateNewLog(File dirForNextEntryLog) throws IOException {
        return allocateNewLog(dirForNextEntryLog, ".log");
    }

    /**
     * Allocate a new log file.
     */
    private synchronized BufferedLogChannel allocateNewLog(File dirForNextEntryLog, String suffix) throws IOException {
        if (freshLifecycle && freshAllocationFailure != null) {
            throwFreshFailure(freshAllocationFailure);
        }
        List<File> ledgersDirs = ledgerDirsManager.getAllLedgerDirs();
        String logFileName;
        // It would better not to overwrite existing entry log files
        File testLogFile = null;
        do {
            if (preallocatedLogId >= Integer.MAX_VALUE) {
                preallocatedLogId = 0;
            } else {
                ++preallocatedLogId;
            }
            logFileName = Long.toHexString(preallocatedLogId) + suffix;
            for (File dir : ledgersDirs) {
                testLogFile = new File(dir, logFileName);
                if (testLogFile.exists()) {
                    log.warn().attr("file", testLogFile)
                        .log("Found existing entry log when trying to create it as a new log.");
                    testLogFile = null;
                    break;
                }
            }
        } while (testLogFile == null);

        File newLogFile = new File(dirForNextEntryLog, logFileName);
        if (freshLifecycle) {
            return allocateForFresh(newLogFile, suffix, ledgersDirs);
        }
        FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();

        BufferedLogChannel logChannel = new BufferedLogChannel(byteBufAllocator, channel, conf.getWriteBufferBytes(),
                conf.getReadBufferBytes(), preallocatedLogId, newLogFile, conf.getFlushIntervalInBytes());
        logfileHeader.readerIndex(0);
        logChannel.write(logfileHeader);

        for (File f : ledgersDirs) {
            setLastLogId(f, preallocatedLogId);
        }

        if (suffix.equals(DefaultEntryLogger.LOG_FILE_SUFFIX)) {
            recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
        }

        log.info()
                .attr("file", newLogFile)
                .attr("logId", preallocatedLogId)
                .log("Created new entry log file");
        return logChannel;
    }

    private BufferedLogChannel allocateForFresh(File file, String suffix, List<File> directories) throws IOException {
        // Register a heap-only slot before open. No native resource can become owned only by a Future result.
        FreshAllocation allocation = new FreshAllocation();
        freshAllocations.add(allocation);
        try {
            allocation.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            allocation.buffered = BufferedLogChannel.forFresh(byteBufAllocator, allocation.channel,
                    conf.getWriteBufferBytes(), conf.getReadBufferBytes(), preallocatedLogId, file,
                    conf.getFlushIntervalInBytes());
            logfileHeader.readerIndex(0);
            allocation.buffered.write(logfileHeader);
            for (File directory : directories) {
                setLastLogIdForFresh(directory, preallocatedLogId);
            }
            if (suffix.equals(DefaultEntryLogger.LOG_FILE_SUFFIX)) {
                recentlyCreatedEntryLogsStatus.createdEntryLog(preallocatedLogId);
            }
            return allocation.buffered;
        } catch (IOException | RuntimeException | Error failure) {
            freshAllocationFailure = failure;
            throw failure;
        }
    }

    private void setLastLogIdForFresh(File directory, long logId) throws IOException {
        ByteBuffer bytes = ByteBuffer.wrap((Long.toHexString(logId) + "\n").getBytes(UTF_8));
        FreshAllocation allocation = new FreshAllocation();
        freshAllocations.add(allocation);
        allocation.channel = FileChannel.open(directory.toPath().resolve("lastId"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS);
        while (bytes.hasRemaining()) {
            if (allocation.channel.write(bytes) <= 0) {
                throw new IOException("Fresh lastId write made no progress");
            }
        }
        allocation.close();
        freshAllocations.remove(allocation);
    }


    private synchronized void closePreAllocateLog() {
        if (preallocation != null) {
            // if preallocate new log success, release the file channel
            try {
                BufferedLogChannel bufferedLogChannel = getPreallocationFuture().get(3, TimeUnit.SECONDS);
                if (bufferedLogChannel != null) {
                    bufferedLogChannel.close();
                }
            } catch (InterruptedException e) {
                log.warn("interrupted while release preAllocate log");
                Thread.currentThread().interrupt();
            } catch (IOException | ExecutionException | TimeoutException e) {
                log.warn("release preAllocate log failed, ignore error");
            }
        }
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    @VisibleForTesting
    void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } catch (IOException e) {
            log.warn("Failed write lastId file");
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                log.error().attr("path", dir.getPath()).log("Could not close lastId file");
            }
        }
    }

    /**
     * Stop the allocator.
     */
    void stop() {
        if (freshLifecycle) {
            throw new IllegalStateException("Fresh allocator requires stopForFresh and its actual close result");
        }
        // wait until the preallocation finished.
        allocatorExecutor.execute(this::closePreAllocateLog);
        allocatorExecutor.shutdown();
        try {
            if (!allocatorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Timedout while awaiting for allocatorExecutor's termination, so force shuttingdown");
            }
        } catch (InterruptedException e) {
            log.warn("Got InterruptedException while awaiting termination of allocatorExecutor, so force shuttingdown");
            Thread.currentThread().interrupt();
        }
        allocatorExecutor.shutdownNow();
        log.info("Stopped entry logger preallocator.");
    }

    /** The owner stops producers first, retains every failure, and releases roots only after all children drain. */
    FreshDrainResult stopForFresh() throws InterruptedException {
        if (!freshLifecycle || isCurrentExecutorThread()) {
            throw new IllegalStateException("Fresh allocator drain requires an external fresh close worker");
        }
        synchronized (freshCloseLock) {
            if (freshDrainResult != null) {
                return freshDrainResult;
            }
            synchronized (freshSubmissionLock) {
                freshStopRequested = true;
            }
            try {
                synchronized (createEntryLogLock) {
                    synchronized (createCompactionLogLock) {
                        allocatorExecutor.shutdown();
                    }
                }
                while (!allocatorExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                    log.info("Waiting for the original fresh entry-log allocator to terminate");
                }
                Thread thread = freshExecutorThread;
                if (thread != null) {
                    thread.join();
                }
                synchronized (this) {
                    Throwable closeFailure = freshHeaderFailure;
                    for (Iterator<FreshAllocation> iterator = freshAllocations.iterator(); iterator.hasNext();) {
                        FreshAllocation allocation = iterator.next();
                        try {
                            allocation.close();
                            iterator.remove();
                        } catch (IOException | RuntimeException | Error failure) {
                            if (closeFailure == null) {
                                closeFailure = failure;
                            } else if (closeFailure != failure) {
                                closeFailure.addSuppressed(failure);
                            }
                        }
                    }
                    if (!freshHeaderReleased) {
                        freshHeaderReleased = true;
                        try {
                            logfileHeader.release();
                        } catch (RuntimeException | Error failure) {
                            freshHeaderFailure = failure;
                            if (closeFailure == null) {
                                closeFailure = failure;
                            } else if (closeFailure != failure) {
                                closeFailure.addSuppressed(failure);
                            }
                        }
                    }
                    freshDrainResult = new FreshDrainResult(closeFailure == null && freshAllocations.isEmpty(),
                            freshAllocationFailure, closeFailure);
                    return freshDrainResult;
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw failure;
            }
        }
    }

    /** Only allocator-owned resources; transferred entry-log channels still belong to their receiver. */
    record FreshDrainResult(boolean drained, Throwable allocationFailure, Throwable closeFailure) { }

    private static void throwFreshFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        } else if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        } else if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("Fresh allocation failed", failure);
    }

    private static final class FreshAllocation {
        private FileChannel channel;
        private BufferedLogChannel buffered;
        private boolean closeStarted;
        private Throwable closeFailure;

        private void close() throws IOException {
            if (buffered != null) {
                // The buffered alias owns the one native-close result for this channel.
                buffered.closeForFresh();
            } else {
                if (!closeStarted) {
                    closeStarted = true;
                    try {
                        if (channel != null) {
                            if (!channel.isOpen()) {
                                throw new IOException("Fresh partial channel closed without a recorded native result");
                            }
                            channel.close();
                        }
                    } catch (IOException | RuntimeException | Error failure) {
                        closeFailure = failure;
                    }
                }
                if (closeFailure != null) {
                    throwFreshFailure(closeFailure);
                }
            }
        }
    }

    /**
     * get the preallocation for tests.
     */
    Future<BufferedLogChannel> getPreallocationFuture(){
        return preallocation;
    }
}

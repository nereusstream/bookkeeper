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

import static org.apache.bookkeeper.bookie.DefaultEntryLogger.INVALID_LID;
import static org.apache.bookkeeper.bookie.DefaultEntryLogger.UNASSIGNED_LEDGERID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;

@CustomLog
class EntryLogManagerForSingleEntryLog extends EntryLogManagerBase {

    private volatile BufferedLogChannel activeLogChannel;
    private long logIdBeforeFlush = INVALID_LID;
    private final AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);
    private final DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private final boolean freshLifecycle;
    private volatile boolean freshClosing;
    private BufferedLogChannel freshPendingCandidate;
    private Throwable freshOperationFailure;
    private final Object freshFlushLock = new Object();
    private List<BufferedLogChannel> freshInFlightRotated;
    private final ByteBuf freshSizeBuffer;
    private boolean freshSizeReleased;
    private Throwable freshSizeFailure;

    EntryLogManagerForSingleEntryLog(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<DefaultEntryLogger.EntryLogListener> listeners,
            DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus) {
        this(conf, ledgerDirsManager, entryLoggerAllocator, listeners, recentlyCreatedEntryLogsStatus, false);
    }

    static EntryLogManagerForSingleEntryLog forFresh(ServerConfiguration conf, LedgerDirsManager directories,
            EntryLoggerAllocator allocator, List<DefaultEntryLogger.EntryLogListener> listeners,
            DefaultEntryLogger.RecentEntryLogsStatus status) {
        return new EntryLogManagerForSingleEntryLog(conf, directories, allocator, listeners, status, true);
    }

    private EntryLogManagerForSingleEntryLog(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager,
            EntryLoggerAllocator entryLoggerAllocator, List<DefaultEntryLogger.EntryLogListener> listeners,
            DefaultEntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus, boolean freshLifecycle) {
        super(conf, ledgerDirsManager, entryLoggerAllocator, listeners);
        this.freshLifecycle = freshLifecycle;
        this.freshSizeBuffer = freshLifecycle ? Unpooled.buffer(Integer.BYTES) : null;
        this.rotatedLogChannels = new LinkedList<BufferedLogChannel>();
        this.recentlyCreatedEntryLogsStatus = recentlyCreatedEntryLogsStatus;
        // Register listener for disk full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new
                // entry log.
                BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                if (currentActiveLogChannel != null
                        && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskAlmostFull(File disk) {
                // If the current entry log disk is almost full, then create new entry
                // log.
                BufferedLogChannel currentActiveLogChannel = activeLogChannel;
                if (currentActiveLogChannel != null
                        && currentActiveLogChannel.getLogFile().getParentFile().equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }
        };
    }

    @Override
    public synchronized long addEntry(long ledger, ByteBuf entry, boolean rollLog) throws IOException {
        checkFreshAdmission();
        return super.addEntry(ledger, entry, rollLog);
    }

    @Override
    ByteBuf sizeBufferForAdd() {
        return freshLifecycle ? freshSizeBuffer : super.sizeBufferForAdd();
    }

    private void checkFreshAdmission() throws IOException {
        if (freshLifecycle && freshClosing) {
            throw new IOException("Fresh entry-log manager is closing");
        }
    }

    @Override
    synchronized BufferedLogChannel getCurrentLogForLedgerForAddEntry(long ledgerId, int entrySize,
            boolean rollLog) throws IOException {
        if (null == activeLogChannel) {
            // log channel can be null because the file is deferred to be created
            createNewLog(UNASSIGNED_LEDGERID, "because current active log channel has not initialized yet");
            return activeLogChannel;
        }

        boolean reachEntryLogLimit = rollLog ? reachEntryLogLimit(activeLogChannel, entrySize)
                : readEntryLogHardLimit(activeLogChannel, entrySize);
        // Create new log if logSizeLimit reached or current disk is full
        boolean createNewLog = shouldCreateNewEntryLog.get();
        if (createNewLog || reachEntryLogLimit) {
            if (activeLogChannel != null) {
                activeLogChannel.flushAndForceWriteIfRegularFlush(false);
            }
            createNewLog(UNASSIGNED_LEDGERID,
                ": createNewLog = " + createNewLog + ", reachEntryLogLimit = " + reachEntryLogLimit);
            // Reset the flag
            if (createNewLog) {
                shouldCreateNewEntryLog.set(false);
            }
        }
        return activeLogChannel;
    }

    @Override
    synchronized void createNewLog(long ledgerId) throws IOException {
        checkFreshAdmission();
        super.createNewLog(ledgerId);
    }

    @Override
    void createNewLog(long ledgerId, String reason) throws IOException {
        if (freshLifecycle) {
            synchronized (this) {
                checkFreshAdmission();
                super.createNewLog(ledgerId, reason);
            }
        } else {
            super.createNewLog(ledgerId, reason);
        }
    }

    @Override
    public synchronized void setCurrentLogForLedgerAndAddToRotate(long ledgerId, BufferedLogChannel logChannel) {
        BufferedLogChannel hasToRotateLogChannel = activeLogChannel;
        if (freshLifecycle) {
            // The allocator has transferred this candidate. Retain it before any list allocation can fail.
            freshPendingCandidate = logChannel;
            if (hasToRotateLogChannel != null) {
                rotatedLogChannels.add(hasToRotateLogChannel);
            }
            activeLogChannel = logChannel;
            freshPendingCandidate = null;
            return;
        }
        activeLogChannel = logChannel;
        if (hasToRotateLogChannel != null) {
            rotatedLogChannels.add(hasToRotateLogChannel);
        }
    }

    @Override
    public BufferedLogChannel getCurrentLogForLedger(long ledgerId) {
        return activeLogChannel;
    }

    @Override
    public BufferedLogChannel getCurrentLogIfPresent(long entryLogId) {
        BufferedLogChannel activeLogChannelTemp = activeLogChannel;
        if ((activeLogChannelTemp != null) && (activeLogChannelTemp.getLogId() == entryLogId)) {
            return activeLogChannelTemp;
        }
        return null;
    }

    @Override
    public File getDirForNextEntryLog(List<File> writableLedgerDirs) {
        Collections.shuffle(writableLedgerDirs);
        return writableLedgerDirs.get(0);
    }

    @Override
    public void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    public long getCurrentLogId() {
        BufferedLogChannel currentActiveLogChannel = activeLogChannel;
        if (currentActiveLogChannel != null) {
            return currentActiveLogChannel.getLogId();
        } else {
            return DefaultEntryLogger.UNINITIALIZED_LOG_ID;
        }
    }

    @Override
    public void flushCurrentLogs() throws IOException {
        checkFreshAdmission();
        BufferedLogChannel currentActiveLogChannel = activeLogChannel;
        if (currentActiveLogChannel != null) {
            /**
             * flushCurrentLogs method is called during checkpoint, so
             * metadata of the file also should be force written.
             */
            flushLogChannel(currentActiveLogChannel, true);
        }
    }

    @Override
    void flushRotatedLogs() throws IOException {
        if (freshLifecycle) {
            checkFreshAdmission();
            flushRotatedLogsForFresh();
            return;
        }
        List<BufferedLogChannel> channels = null;
        synchronized (this) {
            channels = rotatedLogChannels;
            rotatedLogChannels = new LinkedList<BufferedLogChannel>();
        }
        if (null == channels) {
            return;
        }
        Iterator<BufferedLogChannel> chIter = channels.iterator();
        while (chIter.hasNext()) {
            BufferedLogChannel channel = chIter.next();
            try {
                channel.flushAndForceWrite(true);
            } catch (IOException ioe) {
                // rescue from flush exception, add unflushed channels back
                synchronized (this) {
                    if (null == rotatedLogChannels) {
                        rotatedLogChannels = channels;
                    } else {
                        rotatedLogChannels.addAll(0, channels);
                    }
                }
                throw ioe;
            }
            // remove the channel from the list after it is successfully flushed
            chIter.remove();
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            channel.close();
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            log.info().attr("logId", channel.getLogId()).log("Synced entry logger to disk.");
        }
    }

    private void flushRotatedLogsForFresh() throws IOException {
        synchronized (freshFlushLock) {
            checkFreshAdmission();
            // A failed flush keeps its exact remaining list in the manager, rather than only on
            // the failed task's stack. GC and SyncThread may both request a flush.
            if (freshInFlightRotated != null) {
                drainFreshRotatedBatch();
            }
            synchronized (this) {
                List<BufferedLogChannel> replacement = new LinkedList<>();
                freshInFlightRotated = rotatedLogChannels;
                rotatedLogChannels = replacement;
            }
            drainFreshRotatedBatch();
        }
    }

    private void drainFreshRotatedBatch() throws IOException {
        for (Iterator<BufferedLogChannel> iterator = freshInFlightRotated.iterator(); iterator.hasNext();) {
            BufferedLogChannel channel = iterator.next();
            channel.flushAndCloseForFresh();
            recentlyCreatedEntryLogsStatus.flushRotatedEntryLog(channel.getLogId());
            iterator.remove();
        }
        freshInFlightRotated = null;
    }

    /** Requires the enclosing owner to have drained request, GC and sync tasks first. */
    DefaultEntryLogger.FreshDrainResult closeForFresh() {
        if (!freshLifecycle) {
            throw new IllegalStateException("Strict close requires a fresh entry-log manager");
        }
        freshClosing = true;
        synchronized (freshFlushLock) {
            return closeFreshChannels();
        }
    }

    private DefaultEntryLogger.FreshDrainResult closeFreshChannels() {
        Set<BufferedLogChannel> channels = Collections.newSetFromMap(new IdentityHashMap<>());
        synchronized (this) {
            if (activeLogChannel != null) {
                channels.add(activeLogChannel);
            }
            if (freshPendingCandidate != null) {
                channels.add(freshPendingCandidate);
            }
            channels.addAll(rotatedLogChannels);
            if (freshInFlightRotated != null) {
                channels.addAll(freshInFlightRotated);
            }
        }
        Throwable flushFailure = null;
        Throwable closeFailure = freshSizeFailure;
        for (BufferedLogChannel channel : channels) {
            freshOperationFailure = DefaultEntryLogger.mergeFreshFailure(
                    freshOperationFailure, channel.freshOperationFailure());
            try {
                channel.flushAndCloseForFresh();
                synchronized (this) {
                    if (activeLogChannel == channel) {
                        activeLogChannel = null;
                    }
                    if (freshPendingCandidate == channel) {
                        freshPendingCandidate = null;
                    }
                    rotatedLogChannels.removeIf(candidate -> candidate == channel);
                    if (freshInFlightRotated != null) {
                        freshInFlightRotated.removeIf(candidate -> candidate == channel);
                    }
                }
            } catch (IOException | RuntimeException | Error failure) {
                if (channel.freshCloseStageReached()) {
                    closeFailure = DefaultEntryLogger.mergeFreshFailure(closeFailure, failure);
                } else {
                    flushFailure = DefaultEntryLogger.mergeFreshFailure(flushFailure, failure);
                }
            }
        }
        if (!freshSizeReleased) {
            freshSizeReleased = true;
            try {
                freshSizeBuffer.release();
            } catch (RuntimeException | Error failure) {
                freshSizeFailure = failure;
                closeFailure = DefaultEntryLogger.mergeFreshFailure(closeFailure, failure);
            }
        }
        synchronized (this) {
            return new DefaultEntryLogger.FreshDrainResult(activeLogChannel == null && freshPendingCandidate == null
                    && rotatedLogChannels.isEmpty()
                    && (freshInFlightRotated == null || freshInFlightRotated.isEmpty()) && freshSizeFailure == null,
                    freshOperationFailure, flushFailure, closeFailure);
        }
    }

    @Override
    public void close() throws IOException {
        if (freshLifecycle) {
            throw new IllegalStateException("Fresh manager requires closeForFresh and its complete result");
        }
        if (activeLogChannel != null) {
            activeLogChannel.close();
        }
    }

    @Override
    public void forceClose() {
        if (freshLifecycle) {
            throw new IllegalStateException("Fresh manager cannot discard its actual close result");
        }
        IOUtils.close(log, activeLogChannel);
    }

    @Override
    public void prepareEntryMemTableFlush() {
        logIdBeforeFlush = getCurrentLogId();
    }

    @Override
    public boolean commitEntryMemTableFlush() throws IOException {
        long logIdAfterFlush = getCurrentLogId();
        /*
         * in any case that an entry log reaches the limit, we roll the log
         * and start checkpointing. if a memory table is flushed spanning
         * over two entry log files, we also roll log. this is for
         * performance consideration: since we don't wanna checkpoint a new
         * log file that ledger storage is writing to.
         */
        if (reachEntryLogLimit(activeLogChannel, 0L) || logIdAfterFlush != logIdBeforeFlush) {
            log.info("Rolling entry logger since it reached size limitation");
            createNewLog(UNASSIGNED_LEDGERID,
                "due to reaching log limit after flushing memtable : logIdBeforeFlush = "
                    + logIdBeforeFlush + ", logIdAfterFlush = " + logIdAfterFlush);
            return true;
        }
        return false;
    }

    @Override
    public void prepareSortedLedgerStorageCheckpoint(long numBytesFlushed) throws IOException{
        if (numBytesFlushed > 0) {
            // if bytes are added between previous flush and this checkpoint,
            // it means bytes might live at current active entry log, we need
            // roll current entry log and then issue checkpoint to underlying
            // interleaved ledger storage.
            createNewLog(UNASSIGNED_LEDGERID,
                "due to preparing checkpoint : numBytesFlushed = " + numBytesFlushed);
        }
    }

    @Override
    public DefaultEntryLogger.BufferedLogChannel createNewLogForCompaction() throws IOException {
        BufferedLogChannel newLogForCompaction = entryLoggerAllocator.createNewLogForCompaction(
                selectDirForNextEntryLog());
        entryLoggerAllocator.setWritingCompactingLogId(newLogForCompaction.getLogId());
        return newLogForCompaction;
    }
}

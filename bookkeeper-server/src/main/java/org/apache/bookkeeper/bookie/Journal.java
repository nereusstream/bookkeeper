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

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.JournalStats;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BatchedBlockingQueue;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieRequestHandler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.IOUtils;

/**
 * Provide journal related management.
 */
@CustomLog
public class Journal implements CheckpointSource {

    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
        new RecyclableArrayList.Recycler<QueueEntry>();

    private BookieCriticalThread thread;
    private final FreshState fresh;
    private final ClassicJournalRecovery history;

    private static final class FreshState {
        final Object gate = new Object();
        final Object closeLock = new Object();
        final Set<QueueEntry> entries = Collections.newSetFromMap(new IdentityHashMap<>());
        final Set<ForceWriteRequest> requests = Collections.newSetFromMap(new IdentityHashMap<>());
        final Set<RecyclableArrayList<QueueEntry>> lists = Collections.newSetFromMap(new IdentityHashMap<>());
        final List<JournalChannel> channels = new ArrayList<>();
        final List<FreshMarkWrite> markWrites = new ArrayList<>();
        final Set<Path> markFiles = new java.util.HashSet<>();
        final Map<Thread, Integer> producerThreads = new IdentityHashMap<>();
        final Map<Thread, Integer> checkpointThreads = new IdentityHashMap<>();
        List<File> markDirectories;
        LastLogMark pendingMark;
        int preparedMarkDirectories;
        boolean initAttempted;
        boolean initializing;
        boolean initialized;
        boolean started;
        boolean closing;
        boolean writerDone;
        boolean writerReady;
        boolean checkpointClosing;
        boolean providerClosed;
        int producers;
        int checkpoints;
        Thread initializingThread;
        Thread drainingThread;
        ByteBuf lengthBuffer;
        ByteBuf paddingBuffer;
        boolean lengthReleased;
        boolean paddingReleased;
        QueueEntry[] writerLocal;
        ForceWriteRequest[] forceLocal;
        Throwable initializationFailure;
        Throwable operationFailure;
        Throwable callbackFailure;
        Throwable checkpointFailure;
        Throwable cleanupFailure;
        Throwable releaseFailure;
        Throwable closeFailure;
        java.nio.file.DirectoryStream<Path> listing;
        boolean listingCloseAttempted;
        Throwable listingCloseFailure;
    }

    private static final class FreshMarkWrite {
        final Path path;
        final ByteBuffer bytes;
        final boolean existingOwned;
        FileChannel channel;
        boolean forced;
        boolean closeAttempted;
        Throwable closeFailure;

        FreshMarkWrite(Path path, ByteBuffer bytes, boolean existingOwned) {
            this.path = path;
            this.bytes = bytes.asReadOnlyBuffer();
            this.existingOwned = existingOwned;
        }

        void complete(FreshState state) throws IOException {
            if (closeFailure != null) {
                throw freshIo("Unresolved lastMark native close", closeFailure);
            }
            if (channel == null) {
                channel = existingOwned
                        ? FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                                LinkOption.NOFOLLOW_LINKS)
                        : FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                state.markFiles.add(path);
            }
            if (!channel.isOpen() && !closeAttempted) {
                close();
            }
            if (!forced) {
                ByteBuffer remaining = bytes.duplicate();
                remaining.rewind();
                while (remaining.hasRemaining()) {
                    if (channel.write(remaining, remaining.position()) <= 0) {
                        throw new IOException("Fresh lastMark write made no progress");
                    }
                }
                channel.force(true);
                forced = true;
            }
            close();
        }

        private void close() throws IOException {
            if (!closeAttempted) {
                closeAttempted = true;
                try {
                    if (!channel.isOpen()) {
                        throw new IOException("Fresh lastMark channel closed without an observed native close result");
                    }
                    channel.close();
                } catch (IOException | RuntimeException | Error failure) {
                    closeFailure = failure;
                }
            }
            if (closeFailure != null) {
                throw freshIo("Fresh lastMark native close failed", closeFailure);
            }
        }
    }

    record FreshQuiesceResult(boolean quiesced, Throwable operationFailure,
                              Throwable callbackFailure, Throwable releaseFailure) { }

    record FreshDrainResult(boolean drained, Throwable initializationFailure, Throwable operationFailure,
                            Throwable callbackFailure, Throwable checkpointFailure,
                            Throwable flushFailure, Throwable closeFailure) { }

    /**
     * Filter to pickup journals.
     */
    public interface JournalIdFilter {
        boolean accept(long journalId);
    }

    /**
     * For testability.
     */
    @FunctionalInterface
    public interface BufferedChannelBuilder {
        BufferedChannelBuilder DEFAULT_BCBUILDER = (FileChannel fc,
                int capacity) -> new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);

        BufferedChannel create(FileChannel fc, int capacity) throws IOException;
    }



    /**
     * List all journal ids by a specified journal id filer.
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    public static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File[] logFiles = journalDir.listFiles();
        if (logFiles == null || logFiles.length == 0) {
            return Collections.emptyList();
        }
        List<Long> logs = new ArrayList<Long>();
        for (File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * A wrapper over log mark to provide a checkpoint for users of journal
     * to do checkpointing.
     */
    private static class LogMarkCheckpoint implements Checkpoint {
        final LastLogMark mark;

        public LogMarkCheckpoint(LastLogMark checkpoint) {
            this.mark = checkpoint;
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (o == Checkpoint.MAX) {
                return -1;
            } else if (o == Checkpoint.MIN) {
                return 1;
            }
            return mark.getCurMark().compare(((LogMarkCheckpoint) o).mark.getCurMark());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LogMarkCheckpoint)) {
                return false;
            }
            return 0 == compareTo((LogMarkCheckpoint) o);
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }

        @Override
        public String toString() {
            return mark.toString();
        }
    }

    /**
     * Last Log Mark.
     */
    public class LastLogMark {
        private final LogMark curMark;
        private final Journal source = Journal.this;

        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }

        void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }

        LastLogMark markLog() {
            if (fresh != null) {
                synchronized (curMark) {
                    return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
                }
            }
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }

        public LogMark getCurMark() {
            return curMark;
        }

        void rollLog(LastLogMark lastMark) throws NoWritableLedgerDirException {
            if (fresh != null) {
                throw new IllegalStateException("Fresh lastMark writes require the owned checkpoint path");
            }
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            lastMark.getCurMark().writeLogMark(bb);
            log.debug().attr("curMark", () -> lastMark.getCurMark()).log("RollLog to persist last marked log");

            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirsForNewLog();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, lastMarkFileName);
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    log.error()
                            .attr("file", file)
                            .exception(e)
                            .log("Problems writing to file");
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(log, fos);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        public void readLog() {
            if (fresh != null) {
                throw new IllegalStateException("Fresh Journal does not replay historical lastMark files");
            }
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            LogMark mark = new LogMark();
            for (File dir: ledgerDirsManager.getAllLedgerDirs()) {
                File file = new File(dir, lastMarkFileName);
                try {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    }
                    bb.clear();
                    mark.readLogMark(bb);
                    if (curMark.compare(mark) < 0) {
                        curMark.setLogMark(mark.getLogFileId(), mark.getLogFileOffset());
                    }
                } catch (IOException e) {
                    log.error()
                            .exception(e)
                            .attr("file", file)
                            .log("Problems reading from file "
                                    + "(this is okay if it is the first time starting this bookie)");
                }
            }
        }

        @Override
        public String toString() {
            return curMark.toString();
        }
    }

    /**
     * Filter to return list of journals for rolling.
     */
    private static class JournalRollingFilter implements JournalIdFilter {

        final LastLogMark lastMark;

        JournalRollingFilter(LastLogMark lastMark) {
            this.lastMark = lastMark;
        }

        @Override
        public boolean accept(long journalId) {
            return journalId < lastMark.getCurMark().getLogFileId();
        }
    }

    /**
     * Scanner used to scan a journal.
     */
    public interface JournalScanner {
        /**
         * Process a journal entry.
         *
         * @param journalVersion Journal Version
         * @param offset File offset of the journal entry
         * @param entry Journal Entry
         * @throws IOException
         */
        void process(int journalVersion, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Journal Entry to Record.
     */
    /** Only fresh admission allocates this ticket; ordinary QueueEntry keeps its existing data fields. */
    private static final class FreshTicket {
        final Journal owner;
        long reservedBytes;
        boolean retained;
        boolean releaseAttempted;
        boolean memoryReleaseAttempted;
        boolean published;
        boolean completed;
        boolean durable;
        Throwable releaseFailure;

        FreshTicket(Journal owner) {
            this.owner = owner;
        }
    }

    static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;
        FreshTicket freshTicket;

        OpStatsLogger journalAddEntryStats;
        Counter callbackTime;

        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                WriteCallback cb, Object ctx, long enqueueTime, OpStatsLogger journalAddEntryStats,
                Counter callbackTime) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            qe.journalAddEntryStats = journalAddEntryStats;
            qe.callbackTime = callbackTime;
            return qe;
        }

        @Override
        public void run() {
            if (freshTicket != null) {
                freshTicket.owner.completeFreshEntry(this,
                        freshTicket.durable ? BookieProtocol.EOK : BookieProtocol.EIO);
                return;
            }
            long startTime = System.nanoTime();
            log.debug()
                    .attr("ledgerId", ledgerId)
                    .attr("entryId", entryId)
                    .log("Acknowledge");
            journalAddEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            callbackTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
            recycle();
        }

        private Object getCtx() {
            return ctx;
        }

        private final Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            @Override
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            this.entry = null;
            this.cb = null;
            this.ctx = null;
            this.journalAddEntryStats = null;
            this.callbackTime = null;
            this.freshTicket = null;
            recyclerHandle.recycle(this);
        }
    }

    /**
     * Token which represents the need to force a write to the Journal.
     */
    @VisibleForTesting
    public static class ForceWriteRequest {
        private JournalChannel logFile;
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private long lastFlushedPosition;
        private long logId;
        private boolean flushed;
        private Journal freshOwner;

        public int process(ObjectHashSet<BookieRequestHandler> writeHandlers) {
            if (freshOwner != null) {
                freshOwner.closeRolledForFresh(this);
                int size = forceWriteWaiters.size();
                for (int i = 0; i < size; i++) {
                    QueueEntry qe = forceWriteWaiters.set(i, null);
                    if (qe != null) {
                        BookieRequestHandler handler = BookieImpl.responseHandlerForOwnedContext(qe.getCtx());
                        if (handler != null && qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                            writeHandlers.add(handler);
                        }
                        qe.run();
                    }
                }
                return size;
            }
            closeFileIfNecessary();

            // Notify the waiters that the force write succeeded
            for (int i = 0; i < forceWriteWaiters.size(); i++) {
                QueueEntry qe = forceWriteWaiters.get(i);
                if (qe != null) {
                    if (qe.getCtx() instanceof BookieRequestHandler
                            && qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                        writeHandlers.add((BookieRequestHandler) qe.getCtx());
                    }
                    qe.run();
                }
            }

            return forceWriteWaiters.size();
        }

        private void flushFileToDisk() throws IOException {
            if (!flushed) {
                if (freshOwner != null) {
                    logFile.forceThroughForFresh(lastFlushedPosition);
                } else {
                    logFile.forceWrite(false);
                }
                flushed = true;
            }
        }

        public void closeFileIfNecessary() {
            if (freshOwner != null) {
                freshOwner.closeRolledForFresh(this);
                return;
            }
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    flushFileToDisk();
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    log.error().exception(ioe).log("I/O exception while closing file");
                }
            }
        }

        private final Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            Journal owner = freshOwner;
            if (owner != null) {
                owner.releaseFreshRequest(this);
                freshOwner = null;
            }
            logFile = null;
            flushed = false;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    private ForceWriteRequest createForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          RecyclableArrayList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.forceWriteWaiters = forceWriteWaiters;
        req.logFile = logFile;
        req.logId = logId;
        req.lastFlushedPosition = lastFlushedPosition;
        req.shouldClose = shouldClose;
        if (fresh != null) {
            req.freshOwner = this;
            synchronized (fresh.gate) {
                fresh.requests.add(req);
            }
        }
        journalStats.getForceWriteQueueSize().inc();
        return req;
    }

    private static final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
                @Override
                protected ForceWriteRequest newObject(
                        Recycler.Handle<ForceWriteRequest> handle) {
                    return new ForceWriteRequest(handle);
                }
            };

    /**
     * ForceWriteThread is a background thread which makes the journal durable periodically.
     *
     */
    private class ForceWriteThread extends BookieCriticalThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Consumer<Void> threadToNotifyOnEx;

        // should we group force writes
        private final boolean enableGroupForceWrites;
        private final Counter forceWriteThreadTime;

        public ForceWriteThread(Consumer<Void> threadToNotifyOnEx,
                                boolean enableGroupForceWrites,
                                StatsLogger statsLogger) {
            super("ForceWriteThread");
            this.setPriority(Thread.MAX_PRIORITY);
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
            this.forceWriteThreadTime = statsLogger.getThreadScopedCounter("force-write-thread-time");
        }
        @Override
        public void run() {
            if (fresh != null) {
                runForFresh();
                return;
            }
            log.info("ForceWrite Thread started");
            ThreadRegistry.register(super.getName());

            if (conf.isBusyWaitEnabled()) {
                try {
                    CpuAffinity.acquireCore();
                } catch (Exception e) {
                    log.warn()
                            .exception(e)
                            .exceptionMessage(e)
                            .log("Unable to acquire CPU core for Journal ForceWrite thread");
                }
            }

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            final ForceWriteRequest[] localRequests = new ForceWriteRequest[conf.getJournalQueueSize()];

            while (running) {
                try {
                    int numEntriesInLastForceWrite = 0;

                    int requestsCount = forceWriteRequests.takeAll(localRequests);

                    journalStats.getForceWriteQueueSize().addCount(-requestsCount);

                    // Sync and mark the journal up to the position of the last entry in the batch
                    ForceWriteRequest lastRequest = localRequests[requestsCount - 1];
                    syncJournal(lastRequest);

                    // All the requests in the batch are now fully-synced. We can trigger sending the
                    // responses
                    for (int i = 0; i < requestsCount; i++) {
                        ForceWriteRequest req = localRequests[i];
                        numEntriesInLastForceWrite += req.process(writeHandlers);
                        localRequests[i] = null;
                        req.recycle();
                    }

                    journalStats.getForceWriteGroupingCountStats()
                            .registerSuccessfulValue(numEntriesInLastForceWrite);
                    writeHandlers.forEach(
                            (ObjectProcedure<? super BookieRequestHandler>)
                                    BookieRequestHandler::flushPendingResponse);
                    writeHandlers.clear();
                } catch (IOException ioe) {
                    log.error().exception(ioe).log("I/O exception in ForceWrite thread");
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("ForceWrite thread interrupted");
                    running = false;
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.accept(null);
        }

        private void runForFresh() {
            if (Thread.currentThread() != this) {
                throw new IllegalStateException("Fresh force work must run on its original owned thread");
            }
            try {
                ThreadRegistry.register(super.getName());
                ObjectHashSet<BookieRequestHandler> handlers = new ObjectHashSet<>();
                ForceWriteRequest[] local = new ForceWriteRequest[conf.getJournalQueueSize()];
                fresh.forceLocal = local;
                while (!hasFreshFailure()) {
                    int count = forceWriteRequests.pollAll(local, 50, TimeUnit.MILLISECONDS);
                    if (count == 0) {
                        synchronized (fresh.gate) {
                            if (fresh.writerDone && forceWriteRequests.isEmpty()) {
                                break;
                            }
                        }
                        continue;
                    }
                    journalStats.getForceWriteQueueSize().addCount(-count);
                    syncFreshBatch(local, count);
                    int entries = 0;
                    for (int i = 0; i < count; i++) {
                        ForceWriteRequest request = local[i];
                        local[i] = null;
                        entries += request.process(handlers);
                        request.recycle();
                    }
                    try {
                        journalStats.getForceWriteGroupingCountStats().registerSuccessfulValue(entries);
                    } catch (Throwable failure) {
                        rememberFreshCallbackFailure(failure);
                    }
                    handlers.forEach((ObjectProcedure<? super BookieRequestHandler>) handler -> {
                        try {
                            handler.flushPendingResponse();
                        } catch (Throwable failure) {
                            rememberFreshCallbackFailure(failure);
                        }
                    });
                    handlers.clear();
                }
            } catch (Throwable failure) {
                failFresh(failure);
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                running = false;
                synchronized (fresh.gate) {
                    fresh.gate.notifyAll();
                }
            }
        }

        private void syncFreshBatch(ForceWriteRequest[] requests, int count) throws IOException {
            int first = 0;
            while (first < count) {
                int last = first;
                while (last + 1 < count && requests[last + 1].logFile == requests[first].logFile) {
                    last++;
                }
                ForceWriteRequest covering = requests[last];
                long start = MathUtils.nowInNano();
                try {
                    covering.flushFileToDisk();
                } catch (IOException | RuntimeException | Error failure) {
                    try {
                        journalStats.getJournalSyncStats().registerFailedEvent(
                                MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
                    } catch (Throwable statsFailure) {
                        rememberFreshCallbackFailure(statsFailure);
                    }
                    throw failure;
                }
                // A later file's force cannot prove this file's prefix. Commit each covered group in order.
                for (int i = first; i <= last; i++) {
                    ForceWriteRequest request = requests[i];
                    request.flushed = true;
                    for (QueueEntry ticket : request.forceWriteWaiters) {
                        if (ticket != null) {
                            ticket.freshTicket.durable = true;
                        }
                    }
                }
                lastLogMark.setCurLogMark(covering.logId, covering.lastFlushedPosition);
                try {
                    journalStats.getJournalSyncStats().registerSuccessfulEvent(
                            MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
                } catch (Throwable failure) {
                    rememberFreshCallbackFailure(failure);
                }
                first = last + 1;
            }
        }

        private void syncJournal(ForceWriteRequest lastRequest) throws IOException {
            long fsyncStartTime = MathUtils.nowInNano();
            try {
                lastRequest.flushFileToDisk();
                journalStats.getJournalSyncStats().registerSuccessfulEvent(MathUtils.elapsedNanos(fsyncStartTime),
                        TimeUnit.NANOSECONDS);
                lastLogMark.setCurLogMark(lastRequest.logId, lastRequest.lastFlushedPosition);
            } catch (IOException ioe) {
                journalStats.getJournalSyncStats()
                        .registerFailedEvent(MathUtils.elapsedNanos(fsyncStartTime), TimeUnit.NANOSECONDS);
                throw ioe;
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuf paddingBuffer, int journalAlignSize)
            throws IOException {
        int bytesToAlign = (int) (jc.bc.position() % journalAlignSize);
        if (0 != bytesToAlign) {
            int paddingBytes = journalAlignSize - bytesToAlign;
            if (paddingBytes < 8) {
                paddingBytes = journalAlignSize - (8 - paddingBytes);
            } else {
                paddingBytes -= 8;
            }
            paddingBuffer.clear();
            // padding mask
            paddingBuffer.writeInt(PADDING_MASK);
            // padding len
            paddingBuffer.writeInt(paddingBytes);
            // padding bytes
            paddingBuffer.writerIndex(paddingBuffer.writerIndex() + paddingBytes);

            jc.preAllocIfNeeded(paddingBuffer.readableBytes());
            // write padding bytes
            jc.bc.write(paddingBuffer);
        }
    }

    static final long MB = 1024 * 1024L;
    static final int KB = 1024;
    // max journal file size
    final long maxJournalSize;
    // pre-allocation size for the journal files
    final long journalPreAllocSize;
    // write buffer size for the journal files
    final int journalWriteBufferSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final ServerConfiguration conf;
    final ForceWriteThread forceWriteThread;
    final FileChannelProvider fileChannelProvider;

    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos;
    // Threshold after which we flush any buffered journal entries
    private final long bufferedEntriesThreshold;
    // Threshold after which we flush any buffered journal writes
    private final long bufferedWritesThreshold;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // should we hint the filesystem to remove pages from cache after force write
    private final boolean removePagesFromCache;
    private final int journalFormatVersionToWrite;
    private final int journalAlignmentSize;
    // control PageCache flush interval when syncData disabled to reduce disk io util
    private final long journalPageCacheFlushIntervalMSec;
    // Whether reuse journal files, it will use maxBackupJournal as the journal file pool.
    private final boolean journalReuseFiles;

    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

    // Ensures lastMark only advances forward across concurrent checkpointComplete calls.
    private final Object checkpointLock = new Object();
    private final LogMark lastPersistedMark = new LogMark(0, 0);

    private static final String LAST_MARK_DEFAULT_NAME = "lastMark";

    private final String lastMarkFileName;

    private final Counter callbackTime;
    private static final String journalThreadName = "BookieJournal";

    // journal entry queue to commit
    final BatchedBlockingQueue<QueueEntry> queue;
    BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests;

    volatile boolean running = true;
    private final LedgerDirsManager ledgerDirsManager;
    private final ByteBufAllocator allocator;

    // Expose Stats
    private final JournalStats journalStats;

    private JournalAliveListener journalAliveListener;

    private MemoryLimitController memoryLimitController;


    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT);
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger, ByteBufAllocator allocator) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator, false);
    }

    static Journal forFresh(int journalIndex, File directory, ServerConfiguration configuration,
            LedgerDirsManager directories, StatsLogger stats, ByteBufAllocator allocator,
            JournalAliveListener listener) {
        validateFreshConfiguration(configuration);
        Journal journal = new Journal(journalIndex, directory, configuration, directories, stats, allocator, true);
        journal.journalAliveListener = listener;
        return journal;
    }

    /** Owned resources only; the enclosing controlled episode must establish its own startup context. */
    static Journal forClassicHistory(int journalIndex, File directory, ServerConfiguration configuration,
            LedgerDirsManager directories, StatsLogger stats, ByteBufAllocator allocator,
            JournalAliveListener listener) {
        validateFreshConfiguration(configuration);
        if (configuration.isSkipReplayJournalInvalidRecord()) {
            throw new IllegalArgumentException("Controlled Classic history cannot skip invalid Journal records");
        }
        Journal journal = new Journal(
                journalIndex, directory, configuration, directories, stats, allocator, true, true);
        journal.journalAliveListener = listener;
        return journal;
    }

    static void validateFreshConfiguration(ServerConfiguration configuration) {
        Object provider = configuration.getProperty("journalChannelProvider");
        if (configuration.isBusyWaitEnabled() || !configuration.getJournalSyncData()
                || !configuration.getJournalWriteData() || configuration.getJournalFormatVersionToWrite() != 6
                || configuration.getFileInfoFormatVersionToWrite() != 1
                || provider != null && !DefaultFileChannelProvider.class.getName().equals(provider)) {
            throw new IllegalArgumentException("Fresh Journal requires V6/V1, sync data and the default provider");
        }
        if (configuration.getProperty("journalDirectories") == null
                && configuration.getProperty("journalDirectory") == null) {
            throw new IllegalArgumentException("Fresh Journal requires explicitly configured journal directories");
        }
        String[] explicit = configuration.getStringArray("journalDirectories");
        if (explicit.length == 0) {
            Object single = configuration.getProperty("journalDirectory");
            if (!(single instanceof String name) || name.isBlank()) {
                throw new IllegalArgumentException("Fresh Journal requires nonempty explicit journal directories");
            }
        } else {
            for (String configured : explicit) {
                if (configured == null || configured.isBlank()) {
                    throw new IllegalArgumentException("Fresh Journal directory entries cannot be empty");
                }
            }
        }
        int count = configuration.getJournalDirNames().length;
        long memory = Math.multiplyExact(configuration.getJournalMaxMemorySizeMb() / count, MB);
        if (memory <= 0 || configuration.getJournalQueueSize() <= 0
                || Math.multiplyExact(configuration.getJournalWriteBufferSizeKB(), KB) <= 0
                || Math.multiplyExact(configuration.getJournalAlignmentSize(), 2) <= 0
                || configuration.getJournalAlignmentSize() < JournalChannel.SECTOR_SIZE
                || configuration.getJournalAlignmentSize() % JournalChannel.SECTOR_SIZE != 0
                || Math.multiplyExact((long) configuration.getJournalPreAllocSizeMB(), MB)
                        < JournalChannel.HEADER_SIZE + (long) configuration.getJournalAlignmentSize()) {
            throw new IllegalArgumentException("Fresh Journal requires finite positive queues, buffers and memory");
        }
    }

    private Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
            ByteBufAllocator allocator, boolean freshLifecycle) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator, freshLifecycle, false);
    }

    private Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
            ByteBufAllocator allocator, boolean freshLifecycle, boolean historicalLifecycle) {
        fresh = freshLifecycle ? new FreshState() : null;
        this.allocator = allocator;

        StatsLogger journalStatsLogger = statsLogger.scopeLabel("journalIndex", String.valueOf(journalIndex));

        if (conf.isBusyWaitEnabled()) {
            // To achieve lower latency, use busy-wait blocking queue implementation
            queue = new BlockingMpscQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BlockingMpscQueue<>(conf.getJournalQueueSize());
        } else {
            queue = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BatchedArrayBlockingQueue<>(conf.getJournalQueueSize());
        }

        // Adjust the journal max memory in case there are multiple journals configured.
        long journalMaxMemory = conf.getJournalMaxMemorySizeMb() / conf.getJournalDirNames().length * 1024 * 1024;
        this.memoryLimitController = new MemoryLimitController(journalMaxMemory);
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = journalDirectory;
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.syncData = conf.getJournalSyncData();
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread((__) -> this.interruptThread(),
                conf.getJournalAdaptiveGroupWrites(), journalStatsLogger);
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getJournalMaxGroupWaitMSec());
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.bufferedEntriesThreshold = conf.getJournalBufferedEntriesThreshold();
        this.journalFormatVersionToWrite = conf.getJournalFormatVersionToWrite();
        this.journalAlignmentSize = conf.getJournalAlignmentSize();
        this.journalPageCacheFlushIntervalMSec = conf.getJournalPageCacheFlushIntervalMSec();
        this.journalReuseFiles = conf.getJournalReuseFiles();
        this.callbackTime = journalStatsLogger.getThreadScopedCounter("callback-time");
        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // read last log mark
        lastMarkFileName = lastMarkFileName(journalIndex, conf.getJournalDirs().length);
        history = historicalLifecycle ? new ClassicJournalRecovery(journalDirectory.toPath(),
                ledgerDirsManager.getAllLedgerDirs().stream().map(File::toPath).toList(), lastMarkFileName,
                conf.getNettyMaxFrameSizeBytes(), journalAlignmentSize) : null;
        if (fresh == null) {
            lastLogMark.readLog();
        }
        lastPersistedMark.setLogMark(
                lastLogMark.getCurMark().getLogFileId(), lastLogMark.getCurMark().getLogFileOffset());
        log.debug().attr("lastMark", () -> lastLogMark.getCurMark()).log("Last Log Mark");

        try {
            this.fileChannelProvider = fresh != null ? new DefaultFileChannelProvider()
                    : FileChannelProvider.newProvider(conf.getJournalChannelProvider());
        } catch (IOException e) {
            log.error()
                    .attr("journalChannelProvider", conf.getJournalChannelProvider())
                    .log("Failed to initiate file channel provider");
            throw new RuntimeException(e);
        }

        // Expose Stats
        this.journalStats = new JournalStats(journalStatsLogger, journalMaxMemory,
                () -> memoryLimitController.currentUsage());
    }

    static String lastMarkFileName(int journalIndex, int journalCount) {
        return journalCount == 1 ? LAST_MARK_DEFAULT_NAME : LAST_MARK_DEFAULT_NAME + "." + journalIndex;
    }

    void initializeForFresh() throws IOException {
        initializeOwnedJournal(false);
    }

    void initializeForClassicHistory() throws IOException {
        initializeOwnedJournal(true);
    }

    private void initializeOwnedJournal(boolean historical) throws IOException {
        if (fresh == null || historical != (history != null)) {
            throw new IllegalStateException("Journal initializer does not match its original owned context");
        }
        synchronized (fresh.gate) {
            if (fresh.initAttempted || fresh.closing) {
                throw new IllegalStateException("Fresh Journal initialization is one-use");
            }
            fresh.initAttempted = true;
            fresh.initializing = true;
            fresh.initializingThread = Thread.currentThread();
        }
        try {
            if (!Files.isDirectory(journalDirectory.toPath())) {
                throw new IOException("Fresh Journal needs an existing current directory");
            }
            if (history == null) {
                fresh.listing = Files.newDirectoryStream(journalDirectory.toPath());
                try {
                    if (fresh.listing.iterator().hasNext()) {
                        throw new IOException("Fresh Journal current directory contains history");
                    }
                } finally {
                    closeFreshListing();
                }
                if (fresh.listingCloseFailure != null) {
                    throw freshIo("Fresh Journal directory listing did not close", fresh.listingCloseFailure);
                }
            } else {
                history.lastJournalId();
            }
            fresh.markDirectories = List.copyOf(ledgerDirsManager.getAllLedgerDirs());
            if (fresh.markDirectories.isEmpty()) {
                throw new IOException("Fresh Journal requires its complete lastMark directory set");
            }
            for (File directory : fresh.markDirectories) {
                if (!Files.isDirectory(directory.toPath())
                        || history == null && Files.exists(directory.toPath().resolve(lastMarkFileName))) {
                    throw new IOException("Fresh Journal lastMark directories are missing or contain history");
                }
            }
            if (history != null) {
                var observed = history.marks();
                lastLogMark.setCurLogMark(observed.logId(), observed.offset());
                lastPersistedMark.setLogMark(observed.logId(), observed.offset());
                fresh.markFiles.addAll(observed.existing());
            }
            synchronized (fresh.gate) {
                fresh.initialized = !fresh.closing;
            }
        } catch (IOException | RuntimeException | Error failure) {
            fresh.initializationFailure = failure;
            throw failure;
        } finally {
            synchronized (fresh.gate) {
                fresh.initializing = false;
                fresh.initializingThread = null;
                fresh.gate.notifyAll();
            }
        }
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
                   LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
                   ByteBufAllocator allocator, JournalAliveListener journalAliveListener) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator);
        this.journalAliveListener = journalAliveListener;
    }

    @VisibleForTesting
    static Journal newJournal(int journalIndex, File journalDirectory, ServerConfiguration conf,
                                     LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger,
                                     ByteBufAllocator allocator, JournalAliveListener journalAliveListener) {
        return new Journal(journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator,
                journalAliveListener);
    }

    JournalStats getJournalStats() {
        return this.journalStats;
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Update lastLogMark of the journal
     * Indicates that the file has been processed.
     * @param id
     * @param scanOffset
     */
    void setLastLogMark(long id, long scanOffset) {
        lastLogMark.setCurLogMark(id, scanOffset);
    }

    /**
     * Application tried to schedule a checkpoint. After all the txns added
     * before checkpoint are persisted, a <i>checkpoint</i> will be returned
     * to application. Application could use <i>checkpoint</i> to do its logic.
     */
    @Override
    public Checkpoint newCheckpoint() {
        if (fresh != null) {
            synchronized (fresh.gate) {
                if (!fresh.initialized || fresh.checkpointClosing) {
                    throw new IllegalStateException("Fresh Journal checkpoint entry is not available");
                }
            }
        }
        return new LogMarkCheckpoint(lastLogMark.markLog());
    }

    /**
     * Telling journal a checkpoint is finished.
     *
     * <p>Skips if the checkpoint is older than the last persisted mark,
     * preventing lastMark from regressing backwards.
     *
     * @throws IOException
     */
    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        if (fresh != null) {
            checkpointCompleteForFresh(checkpoint, compact);
            return;
        }
        if (!(checkpoint instanceof LogMarkCheckpoint)) {
            return; // we didn't create this checkpoint, so dont do anything with it
        }
        LogMarkCheckpoint lmcheckpoint = (LogMarkCheckpoint) checkpoint;
        LastLogMark mark = lmcheckpoint.mark;

        // See #4105: keep the monotonic decision and lastMark persistence together.
        synchronized (checkpointLock) {
            if (mark.getCurMark().compare(lastPersistedMark) < 0) {
                return;
            }
            persistLastLogMark(mark);
            lastPersistedMark.setLogMark(
                    mark.getCurMark().getLogFileId(), mark.getCurMark().getLogFileOffset());
        }

        if (compact) {
            compactJournals(mark);
        }
    }

    private void compactJournals(LastLogMark mark) {
        // list the journals that have been marked
        List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter(mark));
        // keep MAX_BACKUP_JOURNALS journal files before marked journal
        if (logs.size() >= maxBackupJournals) {
            int maxIdx = logs.size() - maxBackupJournals;
            for (int i = 0; i < maxIdx; i++) {
                long id = logs.get(i);
                if (fresh != null) {
                    synchronized (fresh.gate) {
                        if (fresh.channels.stream().anyMatch(channel -> channel.ownsFreshLog(id))) {
                            continue; // Never unlink a rolled channel whose original close is still unresolved.
                        }
                    }
                }
                // make sure the journal id is smaller than marked journal id
                if (id < mark.getCurMark().getLogFileId()) {
                    File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                    if (!journalFile.delete()) {
                        log.warn().attr("journalFile", journalFile).log("Could not delete old journal file");
                    }
                    log.info().attr("journalFile", journalFile.getName()).log("garbage collected journal");
                }
            }
        }
    }

    private void checkpointCompleteForFresh(Checkpoint checkpoint, boolean compact) throws IOException {
        synchronized (fresh.gate) {
            if (!fresh.initialized || fresh.checkpointClosing) {
                throw new IOException("Fresh Journal checkpoint entry is closed");
            }
            if (compact && history != null && !fresh.started) {
                // Replay rotations may flush storage, but the replay suffix must remain intact until the full flush.
                return;
            }
            Thread current = Thread.currentThread();
            fresh.checkpointThreads.put(current, fresh.checkpointThreads.getOrDefault(current, 0) + 1);
            fresh.checkpoints++;
        }
        try {
            if (!(checkpoint instanceof LogMarkCheckpoint selected)) {
                return;
            }
            if (selected.mark.source != this) {
                throw new IOException("Checkpoint belongs to another Journal");
            }
            LastLogMark committed;
            synchronized (checkpointLock) {
                finishFreshMark();
                if (selected.mark.getCurMark().compare(lastPersistedMark) > 0) {
                    fresh.pendingMark = selected.mark.markLog();
                    fresh.preparedMarkDirectories = 0;
                    finishFreshMark();
                }
                committed = new LastLogMark(lastPersistedMark.getLogFileId(), lastPersistedMark.getLogFileOffset());
            }
            if (compact) {
                compactJournals(committed);
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (fresh.gate) {
                if (fresh.checkpointFailure == null) {
                    fresh.checkpointFailure = failure;
                }
            }
            throw failure;
        } finally {
            synchronized (fresh.gate) {
                decrementFreshThread(fresh.checkpointThreads);
                fresh.checkpoints--;
                fresh.gate.notifyAll();
            }
        }
    }

    private void finishFreshMark() throws IOException {
        if (fresh.pendingMark == null) {
            return;
        }
        ByteBuffer bytes = ByteBuffer.allocate(16);
        fresh.pendingMark.getCurMark().writeLogMark(bytes);
        bytes.rewind();
        while (fresh.preparedMarkDirectories < fresh.markDirectories.size()) {
            Path path = fresh.markDirectories.get(fresh.preparedMarkDirectories).toPath().resolve(lastMarkFileName);
            FreshMarkWrite resource = new FreshMarkWrite(path, bytes, fresh.markFiles.contains(path));
            fresh.markWrites.add(resource);
            fresh.preparedMarkDirectories++;
        }
        Throwable first = null;
        for (FreshMarkWrite resource : List.copyOf(fresh.markWrites)) {
            try {
                resource.complete(fresh);
                fresh.markWrites.remove(resource);
            } catch (IOException | RuntimeException | Error failure) {
                if (first == null) {
                    first = failure;
                }
            }
        }
        if (first != null) {
            throw freshIo("Fresh Journal lastMark did not complete", first);
        }
        lastPersistedMark.setLogMark(fresh.pendingMark.getCurMark().getLogFileId(),
                fresh.pendingMark.getCurMark().getLogFileOffset());
        fresh.pendingMark = null;
        fresh.preparedMarkDirectories = 0;
    }

    @VisibleForTesting
    protected void persistLastLogMark(LastLogMark mark) throws NoWritableLedgerDirException {
        mark.rollLog(mark);
    }

    @VisibleForTesting
    protected boolean isCheckpointCompleteLockHeldByCurrentThread() {
        return Thread.holdsLock(checkpointLock);
    }

    /**
     * Scan the journal.
     *
     * @param journalId         Journal Log Id
     * @param journalPos        Offset to start scanning
     * @param scanner           Scanner to handle entries
     * @param skipInvalidRecord when invalid record,should we skip it or not
     * @return scanOffset - represents the byte till which journal was read
     * @throws IOException
     */
    public long scanJournal(long journalId, long journalPos, JournalScanner scanner, boolean skipInvalidRecord)
        throws IOException {
        if (fresh != null) {
            if (history == null || skipInvalidRecord) {
                throw new IOException("This owned Journal does not admit the requested historical scan");
            }
            beginHistoricalRead();
            try {
                return history.scan(journalId, journalPos, scanner);
            } finally {
                endHistoricalRead();
            }
        }
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                conf, fileChannelProvider);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                    journalPos, conf, fileChannelProvider);
        }
        int journalVersion = recLog.getFormatVersion();
        try {
            return scanRecords(recLog.fc, journalVersion, scanner, 0, 0);
        } catch (IOException e) {
            if (skipInvalidRecord) {
                log.warn("Failed to parse journal file, and skipInvalidRecord is true, skip this journal file reply");
            } else {
                throw e;
            }
            return recLog.fc.position();
        } finally {
            recLog.close();
        }
    }

    /** The legacy record loop; bounded owned recovery uses the same padding and partial-tail semantics. */
    static long scanRecords(FileChannel channel, int journalVersion, JournalScanner scanner,
            int maximumRecord, int alignment) throws IOException {
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        ByteBuffer recBuff = ByteBuffer.allocate(64 * 1024);
        while (true) {
            // entry start offset
            long offset = channel.position();
            // start reading entry
            lenBuff.clear();
            fullRead(channel, lenBuff);
            if (lenBuff.remaining() != 0) {
                break;
            }
            lenBuff.flip();
            int len = lenBuff.getInt();
            if (len == 0) {
                break;
            }
            boolean isPaddingRecord = false;
            if (len < 0) {
                if (len == PADDING_MASK && journalVersion >= JournalChannel.V5) {
                    // skip padding bytes
                    lenBuff.clear();
                    fullRead(channel, lenBuff);
                    if (lenBuff.remaining() != 0) {
                        break;
                    }
                    lenBuff.flip();
                    len = lenBuff.getInt();
                    if (len == 0) {
                        continue;
                    }
                    isPaddingRecord = true;
                } else {
                    log.error().attr("len", len).log("Invalid record found with negative length");
                    throw new IOException("Invalid record found with negative length " + len);
                }
            }
            if (maximumRecord > 0 && (isPaddingRecord ? len < 0 || len > alignment
                    : len < 16 || len > maximumRecord)) {
                throw new IOException("Historical Journal record exceeds its supported bounds: " + len);
            }
            recBuff.clear();
            if (recBuff.remaining() < len) {
                recBuff = ByteBuffer.allocate(len);
            }
            recBuff.limit(len);
            if (fullRead(channel, recBuff) != len) {
                // This seems scary, but it just means that this is where we
                // left off writing
                break;
            }
            recBuff.flip();
            if (!isPaddingRecord) {
                scanner.process(journalVersion, offset, recBuff);
            }
        }
        return channel.position();
    }

    private void beginHistoricalRead() throws IOException {
        synchronized (fresh.gate) {
            if (!fresh.initialized || fresh.started || fresh.closing) {
                throw new IOException("Historical Journal replay is only admitted before its writer starts");
            }
            Thread current = Thread.currentThread();
            fresh.producerThreads.put(current, fresh.producerThreads.getOrDefault(current, 0) + 1);
            fresh.producers++;
        }
    }

    private void endHistoricalRead() {
        synchronized (fresh.gate) {
            decrementFreshThread(fresh.producerThreads);
            fresh.producers--;
            fresh.gate.notifyAll();
        }
    }

    /** Replay the owned V6 suffix as one historical reader, retaining only a page of file IDs. */
    boolean replayHistorical(JournalScanner scanner, boolean skipInvalidRecord) throws IOException {
        if (history == null) {
            return false;
        }
        if (skipInvalidRecord) {
            throw new IOException("This owned Journal does not admit the requested historical scan");
        }
        beginHistoricalRead();
        try {
            var mark = lastLogMark.markLog().getCurMark();
            history.replaySuffix(mark.getLogFileId(), mark.getLogFileOffset(), scanner, this::setLastLogMark);
            return true;
        } finally {
            endHistoricalRead();
        }
    }

    List<Long> journalIdsForRecovery(long minimum) throws IOException {
        if (history == null) {
            if (fresh != null) {
                throw new IOException("Fresh Journal has no historical recovery context");
            }
            return listJournalIds(journalDirectory, id -> id >= minimum);
        }
        beginHistoricalRead();
        try {
            return history.ids(minimum);
        } finally {
            endHistoricalRead();
        }
    }

    /**
     * record an add entry operation in journal.
     */
    public void logAddEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        logAddEntry(ledgerId, entryId, entry, ackBeforeSync, cb, ctx);
    }

    @VisibleForTesting
    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        if (fresh != null) {
            if (ackBeforeSync) {
                throw new IllegalArgumentException("Fresh Journal never acknowledges before sync");
            }
            enqueueFresh(ledgerId, entryId, entry, cb, ctx, journalStats.getJournalAddEntryStats(), true);
            return;
        }
        // Retain until publication, then transfer the reference and reservation to the queue entry.
        long bytes = entry.readableBytes();
        entry.retain();
        boolean counted = false;
        boolean reserved = false;
        boolean published = false;
        QueueEntry ticket = null;
        Throwable submissionFailure = null;
        try {
            journalStats.getJournalQueueSize().inc();
            counted = true;
            memoryLimitController.reserveMemory(bytes);
            reserved = true;
            ticket = QueueEntry.create(entry, ackBeforeSync, ledgerId, entryId, cb, ctx, MathUtils.nowInNano(),
                    journalStats.getJournalAddEntryStats(), callbackTime);
            queue.put(ticket);
            published = true;
        } catch (Throwable failure) {
            submissionFailure = failure;
            throw failure;
        } finally {
            if (!published) {
                if (ticket != null) {
                    try {
                        ticket.recycle();
                    } catch (Throwable failure) {
                        suppressSubmissionCleanup(submissionFailure, failure);
                    }
                }
                if (reserved) {
                    try {
                        memoryLimitController.releaseMemory(bytes);
                    } catch (Throwable failure) {
                        suppressSubmissionCleanup(submissionFailure, failure);
                    }
                }
                try {
                    entry.release();
                } catch (Throwable failure) {
                    suppressSubmissionCleanup(submissionFailure, failure);
                }
                if (counted) {
                    try {
                        journalStats.getJournalQueueSize().dec();
                    } catch (Throwable failure) {
                        suppressSubmissionCleanup(submissionFailure, failure);
                    }
                }
            }
        }
    }

    private static void suppressSubmissionCleanup(Throwable original, Throwable cleanup) {
        if (original != cleanup) {
            original.addSuppressed(cleanup);
        }
    }

    void forceLedger(long ledgerId, WriteCallback cb, Object ctx) {
        if (fresh != null) {
            try {
                enqueueFresh(ledgerId, BookieImpl.METAENTRY_ID_FORCE_LEDGER, null, cb, ctx,
                        journalStats.getJournalForceLedgerStats(), false);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.RejectedExecutionException(interrupted);
            }
            return;
        }
        queue.add(QueueEntry.create(
                null, false /* ackBeforeSync */, ledgerId,
                BookieImpl.METAENTRY_ID_FORCE_LEDGER, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalForceLedgerStats(),
                callbackTime));
        // Increment afterwards because the add operation could fail.
        journalStats.getJournalQueueSize().inc();
    }

    private void enqueueFresh(long ledgerId, long entryId, ByteBuf entry, WriteCallback cb, Object ctx,
            OpStatsLogger stats, boolean blocking) throws InterruptedException {
        java.util.Objects.requireNonNull(cb);
        synchronized (fresh.gate) {
            requireFreshSubmission();
            Thread current = Thread.currentThread();
            fresh.producerThreads.put(current, fresh.producerThreads.getOrDefault(current, 0) + 1);
            fresh.producers++;
        }
        QueueEntry ticket = null;
        boolean published = false;
        try {
            ticket = QueueEntry.create(entry, false, ledgerId, entryId, cb, ctx,
                    MathUtils.nowInNano(), stats, callbackTime);
            ticket.freshTicket = new FreshTicket(this);
            synchronized (fresh.gate) {
                fresh.entries.add(ticket);
                requireFreshSubmission();
                long bytes = entry == null ? 0 : entry.readableBytes();
                while (!memoryLimitController.tryReserveMemory(bytes)) {
                    if (!blocking) {
                        throw new java.util.concurrent.RejectedExecutionException("Fresh Journal memory is full");
                    }
                    fresh.gate.wait();
                    requireFreshSubmission();
                }
                ticket.freshTicket.reservedBytes = bytes;
            }
            if (entry != null) {
                entry.retain();
                ticket.freshTicket.retained = true;
            }
            synchronized (fresh.gate) {
                requireFreshSubmission();
                while (!queue.offer(ticket)) {
                    if (!blocking) {
                        throw new java.util.concurrent.RejectedExecutionException("Fresh Journal queue is full");
                    }
                    fresh.gate.wait();
                    requireFreshSubmission();
                }
                ticket.freshTicket.published = true;
                published = true;
            }
            try {
                journalStats.getJournalQueueSize().inc();
            } catch (Throwable failure) {
                failFresh(failure);
            }
        } finally {
            try {
                if (!published && ticket != null) {
                    if (ticket.freshTicket == null) {
                        ticket.recycle(); // No ticket state means no retain, reservation or publication occurred.
                    } else {
                        ticket.freshTicket.completed = true;
                        ticket.cb = null;
                        ticket.ctx = null;
                        releaseFreshSource(ticket);
                        retireFreshEntry(ticket);
                    }
                }
            } finally {
                synchronized (fresh.gate) {
                    decrementFreshThread(fresh.producerThreads);
                    fresh.producers--;
                    fresh.gate.notifyAll();
                }
            }
        }
    }

    private void requireFreshSubmission() {
        if (!fresh.initialized || !fresh.started || fresh.closing || fresh.operationFailure != null) {
            throw new java.util.concurrent.RejectedExecutionException(
                    "Fresh Journal is not running or is closing", fresh.operationFailure);
        }
    }

    private void failFresh(Throwable failure) {
        synchronized (fresh.gate) {
            if (fresh.operationFailure == null) {
                fresh.operationFailure = failure;
            }
            fresh.closing = true;
            fresh.gate.notifyAll();
        }
    }

    private boolean hasFreshFailure() {
        synchronized (fresh.gate) {
            return fresh.operationFailure != null;
        }
    }

    private void rememberFreshCallbackFailure(Throwable failure) {
        synchronized (fresh.gate) {
            if (fresh.callbackFailure == null) {
                fresh.callbackFailure = failure;
            }
        }
    }

    private void rememberFreshReleaseFailure(QueueEntry ticket, Throwable failure) {
        ticket.freshTicket.releaseFailure = failure;
        synchronized (fresh.gate) {
            if (fresh.releaseFailure == null) {
                fresh.releaseFailure = failure;
            }
            fresh.closing = true;
            fresh.gate.notifyAll();
        }
    }

    private void releaseFreshSource(QueueEntry ticket) {
        if (ticket.freshTicket.retained && !ticket.freshTicket.releaseAttempted) {
            ticket.freshTicket.releaseAttempted = true;
            try {
                ticket.entry.release();
                ticket.entry = null;
            } catch (Throwable failure) {
                rememberFreshReleaseFailure(ticket, failure);
            }
        }
        if (ticket.freshTicket.releaseFailure == null && !ticket.freshTicket.memoryReleaseAttempted) {
            ticket.freshTicket.memoryReleaseAttempted = true;
            try {
                memoryLimitController.releaseMemory(ticket.freshTicket.reservedBytes);
            } catch (Throwable failure) {
                rememberFreshReleaseFailure(ticket, failure);
            }
        }
        synchronized (fresh.gate) {
            fresh.gate.notifyAll();
        }
    }

    private void completeFreshEntry(QueueEntry ticket, int rc) {
        if (ticket.freshTicket.completed) {
            return;
        }
        ticket.freshTicket.completed = true;
        WriteCallback callback = ticket.cb;
        Object context = ticket.ctx;
        ticket.cb = null;
        ticket.ctx = null;
        long start = MathUtils.nowInNano();
        try {
            try {
                if (rc == BookieProtocol.EOK) {
                    ticket.journalAddEntryStats.registerSuccessfulEvent(
                            MathUtils.elapsedNanos(ticket.enqueueTime), TimeUnit.NANOSECONDS);
                } else {
                    ticket.journalAddEntryStats.registerFailedEvent(
                            MathUtils.elapsedNanos(ticket.enqueueTime), TimeUnit.NANOSECONDS);
                }
            } catch (Throwable failure) {
                rememberFreshCallbackFailure(failure);
            }
            try {
                callback.writeComplete(rc, ticket.ledgerId, ticket.entryId, null, context);
            } catch (Throwable failure) {
                rememberFreshCallbackFailure(failure);
            }
        } finally {
            releaseFreshSource(ticket);
            try {
                ticket.callbackTime.addLatency(MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
            } catch (Throwable failure) {
                rememberFreshCallbackFailure(failure);
            }
            retireFreshEntry(ticket);
        }
    }

    private void retireFreshEntry(QueueEntry ticket) {
        if (ticket.freshTicket.releaseFailure != null) {
            return; // Keep the uncertain owned reference and its first result; never release it twice.
        }
        synchronized (fresh.gate) {
            fresh.entries.remove(ticket);
        }
        try {
            ticket.recycle();
        } catch (Throwable failure) {
            synchronized (fresh.gate) {
                if (fresh.releaseFailure == null) {
                    fresh.releaseFailure = failure;
                }
            }
        }
    }

    private RecyclableArrayList<QueueEntry> newEntryList() {
        RecyclableArrayList<QueueEntry> list = entryListRecycler.newInstance();
        if (fresh != null) {
            synchronized (fresh.gate) {
                fresh.lists.add(list);
            }
        }
        return list;
    }

    private void releaseFreshList(RecyclableArrayList<QueueEntry> list) {
        if (list == null) {
            return;
        }
        synchronized (fresh.gate) {
            fresh.lists.remove(list);
        }
        try {
            list.recycle();
        } catch (Throwable failure) {
            synchronized (fresh.gate) {
                if (fresh.releaseFailure == null) {
                    fresh.releaseFailure = failure;
                }
            }
        }
    }

    private void releaseFreshRequest(ForceWriteRequest request) {
        releaseFreshList(request.forceWriteWaiters);
        request.forceWriteWaiters = null;
        synchronized (fresh.gate) {
            fresh.requests.remove(request);
        }
    }

    private void closeRolledForFresh(ForceWriteRequest request) {
        if (!request.shouldClose) {
            return;
        }
        JournalChannel.FreshCloseResult result = request.logFile.closeForFresh();
        synchronized (fresh.gate) {
            if (result.drained()) {
                fresh.channels.remove(request.logFile);
                request.shouldClose = false;
            }
            Throwable failure = result.closeFailure() != null ? result.closeFailure()
                    : result.flushFailure() != null ? result.flushFailure() : result.operationFailure();
            if (failure != null) {
                if (fresh.cleanupFailure == null) {
                    fresh.cleanupFailure = failure;
                }
                if (fresh.closeFailure == null) {
                    fresh.closeFailure = result.closeFailure();
                }
                fresh.closing = true;
                fresh.gate.notifyAll();
            }
        }
    }

    /**
     * Get the length of journal entries queue.
     *
     * @return length of journal entry queue.
     */
    public int getJournalQueueLength() {
        return queue.size();
    }

    @VisibleForTesting
    JournalChannel newLogFile(long logId, Long replaceLogId) throws IOException {
        if (fresh != null) {
            JournalChannel channel = JournalChannel.forFresh(journalDirectory, logId,
                    journalPreAllocSize, journalWriteBufferSize, journalAlignmentSize, conf, allocator);
            synchronized (fresh.gate) {
                fresh.channels.add(channel);
            }
            channel.initializeForFresh((DefaultFileChannelProvider) fileChannelProvider);
            return channel;
        }
        return new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                journalAlignmentSize, removePagesFromCache,
                journalFormatVersionToWrite, getBufferedChannelBuilder(),
                conf, fileChannelProvider, replaceLogId);
    }

    /**
     * A thread used for persisting journal entries to journal files.
     *
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     * @see org.apache.bookkeeper.bookie.SyncThread
     */
    public void run() {
        if (fresh == null) {
            runJournalLoop();
            return;
        }
        if (Thread.currentThread() != thread) {
            throw new IllegalStateException("Fresh Journal must run on its original owned thread");
        }
        try {
            runJournalLoop();
        } catch (Throwable failure) {
            failFresh(failure);
        } finally {
            synchronized (fresh.gate) {
                fresh.writerDone = true;
                running = false;
                fresh.gate.notifyAll();
            }
            if (journalAliveListener != null) {
                try {
                    journalAliveListener.onJournalExit();
                } catch (Throwable failure) {
                    rememberFreshCallbackFailure(failure);
                }
            }
        }
    }

    private void runJournalLoop() {
        log.info().attr("journalDirectory", journalDirectory).log("Starting journal");
        ThreadRegistry.register(journalThreadName);

        if (conf.isBusyWaitEnabled()) {
            try {
                CpuAffinity.acquireCore();
            } catch (Exception e) {
                log.warn()
                        .exception(e)
                        .exceptionMessage(e)
                        .log("Unable to acquire CPU core for Journal thread");
            }
        }

        RecyclableArrayList<QueueEntry> toFlush = newEntryList();
        int numEntriesToFlush = 0;
        ByteBuf lenBuff = Unpooled.buffer(4);
        if (fresh != null) {
            fresh.lengthBuffer = lenBuff;
        }
        ByteBuf paddingBuff = Unpooled.buffer(2 * conf.getJournalAlignmentSize());
        if (fresh != null) {
            fresh.paddingBuffer = paddingBuff;
        }
        paddingBuff.writeZero(paddingBuff.capacity());

        BufferedChannel bc = null;
        JournalChannel logFile = null;
        forceWriteThread.start();
        Stopwatch journalCreationWatcher = Stopwatch.createUnstarted();
        Stopwatch journalFlushWatcher = Stopwatch.createUnstarted();
        long batchSize = 0;
        try {
            // Should not use MathUtils.now(), which use System.nanoTime() and
            // could only be used to measure elapsed time.
            // http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
            long logId;
            if (history == null) {
                var journalIds = listJournalIds(journalDirectory, null);
                logId = journalIds.isEmpty() ? System.currentTimeMillis() : journalIds.get(journalIds.size() - 1);
            } else {
                long last = history.lastJournalId();
                logId = last == -1 ? System.currentTimeMillis() : last;
            }
            long lastFlushPosition = 0;
            boolean groupWhenTimeout = false;

            long dequeueStartTime = 0L;
            long lastFlushTimeMs = System.currentTimeMillis();

            final ObjectHashSet<BookieRequestHandler> writeHandlers = new ObjectHashSet<>();
            QueueEntry[] localQueueEntries = new QueueEntry[conf.getJournalQueueSize()];
            if (fresh != null) {
                fresh.writerLocal = localQueueEntries;
            }
            int localQueueEntriesIdx = 0;
            int localQueueEntriesLen = 0;
            QueueEntry qe = null;
            while (true) {
                if (fresh != null && hasFreshFailure()) {
                    break;
                }
                // new journal file to write
                if (null == logFile) {
                    logId = logId + 1;
                    List<Long> journalIds = listJournalIds(journalDirectory, null);
                    Long replaceLogId = fileChannelProvider.supportReuseFile() && journalReuseFiles
                        && journalIds.size() >= maxBackupJournals
                        && journalIds.get(0) < lastLogMark.getCurMark().getLogFileId()
                        ? journalIds.get(0) : null;

                    journalCreationWatcher.reset().start();
                    logFile = newLogFile(logId, replaceLogId);

                    journalStats.getJournalCreationStats().registerSuccessfulEvent(
                            journalCreationWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = bc.position();
                    if (fresh != null) {
                        synchronized (fresh.gate) {
                            fresh.writerReady = true;
                            fresh.gate.notifyAll();
                        }
                    }
                }

                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        journalStats.getJournalProcessTimeStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime), TimeUnit.NANOSECONDS);
                    }

                    // At this point the local queue will always be empty, otherwise we would have
                    // advanced to the next `qe` at the end of the loop
                    localQueueEntriesIdx = 0;
                    if (numEntriesToFlush == 0) {
                        // There are no entries pending. We can wait indefinitely until the next
                        // one is available
                        localQueueEntriesLen = fresh == null ? queue.takeAll(localQueueEntries)
                                : queue.pollAll(localQueueEntries, 50, TimeUnit.MILLISECONDS);
                    } else {
                        // There are already some entries pending. We must adjust
                        // the waiting time to the remaining groupWait time
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            pollWaitTimeNanos = 0;
                        }
                        if (fresh != null) {
                            pollWaitTimeNanos = Math.min(pollWaitTimeNanos, TimeUnit.MILLISECONDS.toNanos(50));
                        }

                        localQueueEntriesLen = queue.pollAll(localQueueEntries,
                                pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                    }

                    dequeueStartTime = MathUtils.nowInNano();
                    if (fresh != null) {
                        synchronized (fresh.gate) {
                            fresh.gate.notifyAll(); // Space for a pre-publication producer, without interrupting I/O.
                        }
                    }

                    if (localQueueEntriesLen > 0) {
                        qe = localQueueEntries[localQueueEntriesIdx];
                        localQueueEntries[localQueueEntriesIdx++] = null;
                    }
                }

                if (numEntriesToFlush > 0) {
                    boolean shouldFlush = fresh != null && freshWriterCanFinish(qe);
                    // We should issue a forceWrite if any of the three conditions below holds good
                    // 1. If the oldest pending entry has been pending for longer than the max wait time
                    if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                            .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                        groupWhenTimeout = true;
                    } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                        && (qe == null // no entry to group
                            || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                        // when group timeout, it would be better to look forward, as there might be lots of
                        // entries already timeout
                        // due to a previous slow write (writing to filesystem which impacted by force write).
                        // Group those entries in the queue
                        // a) already timeout
                        // b) limit the number of entries to group
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxWaitCounter().inc();
                    } else if (qe != null
                            && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                            || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                        // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushMaxOutstandingBytesCounter().inc();
                    } else if (qe == null && flushWhenQueueEmpty) {
                        // We should get here only if we flushWhenQueueEmpty is true else we would wait
                        // for timeout that would put is past the maxWait threshold
                        // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                        // publish at a time - common case in tests.
                        groupWhenTimeout = false;
                        shouldFlush = true;
                        journalStats.getFlushEmptyQueueCounter().inc();
                    }

                    // toFlush is non null and not empty so should be safe to access getFirst
                    if (shouldFlush) {
                        if (journalFormatVersionToWrite >= JournalChannel.V5) {
                            writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                        }
                        journalFlushWatcher.reset().start();
                        bc.flush();

                        for (int i = 0; i < toFlush.size(); i++) {
                            QueueEntry entry = toFlush.get(i);
                            if (entry != null && (!syncData || entry.ackBeforeSync)) {
                                toFlush.set(i, null);
                                numEntriesToFlush--;
                                if (entry.getCtx() instanceof BookieRequestHandler
                                        && entry.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                                    writeHandlers.add((BookieRequestHandler) entry.getCtx());
                                }
                                entry.run();
                            }
                        }
                        writeHandlers.forEach(
                                (ObjectProcedure<? super BookieRequestHandler>)
                                        BookieRequestHandler::flushPendingResponse);
                        writeHandlers.clear();

                        lastFlushPosition = bc.position();
                        journalStats.getJournalFlushStats().registerSuccessfulEvent(
                                journalFlushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                        // Trace the lifetime of entries through persistence
                        RecyclableArrayList<QueueEntry> flushedEntries = toFlush;
                        log.debug(e -> {
                            for (QueueEntry entry : flushedEntries) {
                                if (entry != null) {
                                    log.debug()
                                            .attr("ledgerId", entry.ledgerId)
                                            .attr("entryId", entry.entryId)
                                            .log("Written and queuing for flush");
                                }
                            }
                        });

                        journalStats.getForceWriteBatchEntriesStats()
                            .registerSuccessfulValue(numEntriesToFlush);
                        journalStats.getForceWriteBatchBytesStats()
                            .registerSuccessfulValue(batchSize);
                        boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                        // Trigger data sync to disk in the "Force-Write" thread.
                        // Trigger data sync to disk has three situations:
                        // 1. journalSyncData enabled, usually for SSD used as journal storage
                        // 2. shouldRolloverJournal is true, that is the journal file reaches maxJournalSize
                        // 3. if journalSyncData disabled and shouldRolloverJournal is false, we can use
                        //   journalPageCacheFlushIntervalMSec to control sync frequency, preventing disk
                        //   synchronize frequently, which will increase disk io util.
                        //   when flush interval reaches journalPageCacheFlushIntervalMSec (default: 1s),
                        //   it will trigger data sync to disk
                        if (syncData
                                || shouldRolloverJournal
                                || (System.currentTimeMillis() - lastFlushTimeMs
                                >= journalPageCacheFlushIntervalMSec)) {
                            ForceWriteRequest request = createForceWriteRequest(
                                    logFile, logId, lastFlushPosition, toFlush, shouldRolloverJournal);
                            if (fresh == null) {
                                forceWriteRequests.put(request);
                            } else {
                                publishForceForFresh(request);
                            }
                            lastFlushTimeMs = System.currentTimeMillis();
                        }
                        toFlush = newEntryList();
                        numEntriesToFlush = 0;

                        batchSize = 0L;
                        // check whether journal file is over file limit
                        if (shouldRolloverJournal) {
                            // if the journal file is rolled over, the journal file will be closed after last
                            // entry is force written to disk.
                            logFile = null;
                            if (fresh == null || !freshWriterCanFinish(qe)) {
                                continue;
                            }
                        }
                    }
                }

                if (fresh == null ? !running : freshWriterCanFinish(qe)) {
                    log.info("Journal Manager is asked to shut down, quit.");
                    break;
                }

                if (qe == null) { // no more queue entry
                    continue;
                }

                journalStats.getJournalQueueSize().dec();
                journalStats.getJournalQueueStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);

                if ((qe.entryId == BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC)
                        && (journalFormatVersionToWrite < JournalChannel.V6)) {
                    /*
                     * this means we are using new code which supports
                     * persisting explicitLac, but "journalFormatVersionToWrite"
                     * is set to some older value (< V6). In this case we
                     * shouldn't write this special entry
                     * (METAENTRY_ID_LEDGER_EXPLICITLAC) to Journal.
                     */
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    ReferenceCountUtil.release(qe.entry);
                } else if (qe.entryId != BookieImpl.METAENTRY_ID_FORCE_LEDGER) {
                    int entrySize = qe.entry.readableBytes();
                    journalStats.getJournalWriteBytes().addCount(entrySize);

                    batchSize += fresh == null ? 4 + entrySize : 4L + entrySize;

                    lenBuff.clear();
                    lenBuff.writeInt(entrySize);

                    // preAlloc based on size
                    logFile.preAllocIfNeeded(fresh == null ? 4 + entrySize : 4L + entrySize);

                    bc.write(lenBuff, qe.entry);
                    if (fresh == null) {
                        memoryLimitController.releaseMemory(qe.entry.readableBytes());
                        ReferenceCountUtil.release(qe.entry);
                    } else {
                        releaseFreshSource(qe);
                    }
                }

                toFlush.add(qe);
                numEntriesToFlush++;

                if (localQueueEntriesIdx < localQueueEntriesLen) {
                    qe = localQueueEntries[localQueueEntriesIdx];
                    localQueueEntries[localQueueEntriesIdx++] = null;
                } else {
                    qe = null;
                }
            }
        } catch (IOException ioe) {
            if (fresh != null) {
                failFresh(ioe);
            }
            log.error().exception(ioe).log("I/O exception in Journal thread!");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            if (fresh != null) {
                failFresh(ie);
            }
            log.info("Journal exits when shutting down");
        } finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            if (fresh == null) {
                IOUtils.close(log, bc);
            }
            if (journalAliveListener != null) {
                if (fresh == null) {
                    journalAliveListener.onJournalExit();
                }
            }
        }
        log.info("Journal exited loop!");
    }

    public BufferedChannelBuilder getBufferedChannelBuilder() {
        return (FileChannel fc, int capacity) -> new BufferedChannel(allocator, fc, capacity);
    }

    /**
     * Shuts down the journal.
     */
    public synchronized void shutdown() {
        if (fresh != null) {
            throw new IllegalStateException("Use the explicit fresh Journal quiesce/close results");
        }
        try {
            if (!running) {
                return;
            }
            log.info("Shutting down Journal");
            if (fileChannelProvider != null) {
                fileChannelProvider.close();
            }

            forceWriteThread.shutdown();

            running = false;
            this.interruptThread();
            this.joinThread();
            log.info("Finished Shutting down Journal thread");
        } catch (IOException | InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn().exception(ie).log("Interrupted during shutting down journal");
        }
    }

    private static int fullRead(FileChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while (bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }

    /**
     * Wait for the Journal thread to exit.
     * This is method is needed in order to mock the journal, we can't mock final method of java.lang.Thread class
     *
     * @throws InterruptedException
     */
    @VisibleForTesting
    public void joinThread() throws InterruptedException {
        if (fresh != null && Thread.currentThread() == thread) {
            throw new IllegalStateException("Fresh Journal cannot join itself");
        }
        if (thread != null) {
            thread.join();
        }
    }

    void awaitStartedForFresh() throws IOException, InterruptedException {
        if (fresh == null || isCurrentResourceActivityForFresh()) {
            throw new IllegalStateException("Fresh Journal startup must be observed by its external owner");
        }
        synchronized (fresh.gate) {
            while (!fresh.writerReady && !fresh.writerDone && fresh.operationFailure == null) {
                fresh.gate.wait();
            }
            if (!fresh.writerReady || fresh.operationFailure != null) {
                throw new IOException("Fresh Journal did not complete its initial channel", fresh.operationFailure);
            }
        }
    }

    public void interruptThread() {
        if (fresh != null) {
            throw new IllegalStateException("Fresh Journal shutdown must not interrupt file I/O");
        }
        if (thread != null) {
            thread.interrupt();
        }
    }

    public synchronized void start() {
        if (fresh != null) {
            synchronized (fresh.gate) {
                if (!fresh.initialized || fresh.started || fresh.closing
                        || history != null && (fresh.producers != 0 || !history.healthy())) {
                    throw new IllegalStateException("Fresh Journal is not ready or is closing");
                }
                thread = new BookieCriticalThread(() -> run(), journalThreadName + "-" + conf.getBookiePort());
                thread.setPriority(Thread.MAX_PRIORITY);
                fresh.started = true;
                try {
                    thread.start();
                } catch (RuntimeException | Error failure) {
                    failFresh(failure);
                    throw failure;
                }
            }
            return;
        }
        thread = new BookieCriticalThread(() -> run(), journalThreadName + "-" + conf.getBookiePort());
        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
    }

    long getMemoryUsage() {
        return memoryLimitController.currentUsage();
    }

    @VisibleForTesting
    void setMemoryLimitController(MemoryLimitController memoryLimitController) {
        if (fresh != null) {
            throw new IllegalStateException("Fresh Journal owns its original memory reservations");
        }
        this.memoryLimitController = memoryLimitController;
    }

    @VisibleForTesting
    public void setForceWriteRequests(BatchedBlockingQueue<ForceWriteRequest> forceWriteRequests) {
        if (fresh != null) {
            throw new IllegalStateException("Fresh Journal owns its original force queue");
        }
        this.forceWriteRequests = forceWriteRequests;
    }

    private boolean freshWriterCanFinish(QueueEntry current) {
        synchronized (fresh.gate) {
            return fresh.closing && fresh.producers == 0 && current == null && queue.isEmpty();
        }
    }

    boolean isCurrentResourceActivityForFresh() {
        if (fresh == null) {
            return false;
        }
        synchronized (fresh.gate) {
            Thread current = Thread.currentThread();
            return current == thread || current == forceWriteThread || current == fresh.initializingThread
                    || current == fresh.drainingThread || fresh.producerThreads.containsKey(current)
                    || fresh.checkpointThreads.containsKey(current);
        }
    }

    private static void decrementFreshThread(Map<Thread, Integer> threads) {
        Thread current = Thread.currentThread();
        int depth = threads.get(current);
        if (depth == 1) {
            threads.remove(current);
        } else {
            threads.put(current, depth - 1);
        }
    }

    /** Append/callback quiescence only; checkpoint/lastMark users remain admitted until final close. */
    FreshQuiesceResult quiesceForFresh() throws InterruptedException {
        if (fresh == null || isCurrentResourceActivityForFresh()) {
            throw new IllegalStateException("Fresh Journal quiesce requires an external owner worker");
        }
        synchronized (fresh.closeLock) {
            fresh.drainingThread = Thread.currentThread();
            try {
                return quiesceFreshInternal();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                fresh.drainingThread = null;
            }
        }
    }

    private FreshQuiesceResult quiesceFreshInternal() throws InterruptedException {
        synchronized (fresh.gate) {
            fresh.closing = true;
            fresh.gate.notifyAll();
            while (fresh.initializing || fresh.producers != 0) {
                fresh.gate.wait();
            }
        }
        if (thread != null) {
            thread.join();
        }
        synchronized (fresh.gate) {
            fresh.writerDone = true;
            fresh.gate.notifyAll();
        }
        forceWriteThread.join();
        // Both actual threads have returned. No buffer is retired while a writer or callback can still use it.
        for (QueueEntry ticket : List.copyOf(fresh.entries)) {
            if (!ticket.freshTicket.completed) {
                completeFreshEntry(ticket, ticket.freshTicket.durable ? BookieProtocol.EOK : BookieProtocol.EIO);
            }
        }
        for (ForceWriteRequest request : List.copyOf(fresh.requests)) {
            try {
                request.recycle();
            } catch (Throwable failure) {
                if (fresh.releaseFailure == null) {
                    fresh.releaseFailure = failure;
                }
            }
        }
        for (RecyclableArrayList<QueueEntry> list : List.copyOf(fresh.lists)) {
            releaseFreshList(list);
        }
        queue.clear();
        forceWriteRequests.clear();
        if (fresh.writerLocal != null) {
            java.util.Arrays.fill(fresh.writerLocal, null);
        }
        if (fresh.forceLocal != null) {
            java.util.Arrays.fill(fresh.forceLocal, null);
        }
        if (fresh.lengthBuffer != null && !fresh.lengthReleased) {
            fresh.lengthReleased = true;
            try {
                fresh.lengthBuffer.release();
                fresh.lengthBuffer = null;
            } catch (Throwable failure) {
                if (fresh.releaseFailure == null) {
                    fresh.releaseFailure = failure;
                }
            }
        }
        if (fresh.paddingBuffer != null && !fresh.paddingReleased) {
            fresh.paddingReleased = true;
            try {
                fresh.paddingBuffer.release();
                fresh.paddingBuffer = null;
            } catch (Throwable failure) {
                if (fresh.releaseFailure == null) {
                    fresh.releaseFailure = failure;
                }
            }
        }
        return new FreshQuiesceResult(fresh.entries.isEmpty() && fresh.requests.isEmpty()
                && fresh.lists.isEmpty() && fresh.releaseFailure == null,
                fresh.operationFailure, fresh.callbackFailure, fresh.releaseFailure);
    }

    /** Caller must first stop external Sync/checkpoint users; append quiescence alone does not do that. */
    FreshDrainResult closeForFresh() throws InterruptedException {
        if (fresh == null || isCurrentResourceActivityForFresh()) {
            throw new IllegalStateException("Fresh Journal close requires an external owner worker");
        }
        synchronized (fresh.closeLock) {
            fresh.drainingThread = Thread.currentThread();
            try {
                FreshQuiesceResult quiet = quiesceFreshInternal();
                synchronized (fresh.gate) {
                    fresh.checkpointClosing = true;
                    while (fresh.checkpoints != 0) {
                        fresh.gate.wait();
                    }
                }
                Throwable flushFailure = null;
                for (JournalChannel channel : List.copyOf(fresh.channels)) {
                    JournalChannel.FreshCloseResult result = channel.closeForFresh();
                    if (result.drained()) {
                        fresh.channels.remove(channel);
                    }
                    if (fresh.cleanupFailure == null) {
                        fresh.cleanupFailure = result.operationFailure() != null
                                ? result.operationFailure() : result.flushFailure();
                    }
                    if (flushFailure == null) {
                        flushFailure = result.flushFailure();
                    }
                    if (fresh.closeFailure == null) {
                        fresh.closeFailure = result.closeFailure();
                    }
                }
                synchronized (checkpointLock) {
                    // Resolve only the already frozen writes. Closing never manufactures a checkpoint/GC receipt.
                    for (FreshMarkWrite mark : List.copyOf(fresh.markWrites)) {
                        try {
                            mark.complete(fresh);
                            fresh.markWrites.remove(mark);
                        } catch (Throwable failure) {
                            if (mark.closeFailure != null) {
                                if (fresh.closeFailure == null) {
                                    fresh.closeFailure = mark.closeFailure;
                                }
                            } else if (flushFailure == null) {
                                flushFailure = failure;
                            }
                        }
                    }
                }
                closeFreshListing();
                if (history != null) {
                    var recovered = history.closeAndDrain();
                    if (fresh.operationFailure == null) {
                        fresh.operationFailure = recovered.readFailure();
                    }
                    if (fresh.closeFailure == null) {
                        fresh.closeFailure = recovered.closeFailure();
                    }
                }
                if (fresh.channels.isEmpty() && fresh.markWrites.isEmpty() && !fresh.providerClosed) {
                    try {
                        fileChannelProvider.close();
                        fresh.providerClosed = true;
                    } catch (Throwable failure) {
                        if (fresh.closeFailure == null) {
                            fresh.closeFailure = failure;
                        }
                    }
                }
                boolean drained = quiet.quiesced() && fresh.channels.isEmpty() && fresh.markWrites.isEmpty()
                        && fresh.providerClosed && flushFailure == null && fresh.closeFailure == null;
                return new FreshDrainResult(drained, fresh.initializationFailure,
                        fresh.operationFailure != null ? fresh.operationFailure : fresh.cleanupFailure,
                        fresh.callbackFailure, fresh.checkpointFailure, flushFailure,
                        fresh.closeFailure != null ? fresh.closeFailure : fresh.releaseFailure);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                fresh.drainingThread = null;
            }
        }
    }

    private static IOException freshIo(String message, Throwable failure) {
        return failure instanceof IOException io ? io : new IOException(message, failure);
    }

    private void closeFreshListing() {
        if (fresh.listing != null && !fresh.listingCloseAttempted) {
            fresh.listingCloseAttempted = true;
            try {
                fresh.listing.close();
                fresh.listing = null;
            } catch (Throwable failure) {
                fresh.listingCloseFailure = failure;
                if (fresh.closeFailure == null) {
                    fresh.closeFailure = failure;
                }
            }
        }
    }

    private void publishForceForFresh(ForceWriteRequest request) throws IOException, InterruptedException {
        while (!forceWriteRequests.offer(request, 50, TimeUnit.MILLISECONDS)) {
            if (hasFreshFailure() || !forceWriteThread.isAlive()) {
                throw new IOException("Fresh ForceWrite thread stopped before accepting the batch");
            }
        }
    }
}

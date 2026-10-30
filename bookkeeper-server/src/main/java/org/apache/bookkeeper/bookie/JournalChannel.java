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

import io.netty.buffer.ByteBufAllocator;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import lombok.CustomLog;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.PageCacheUtil;
import org.apache.bookkeeper.util.ZeroBuffer;

/**
 * Simple wrapper around FileChannel to add versioning
 * information to the file.
 */
@CustomLog
class JournalChannel implements Closeable {

    static final long MB = 1024 * 1024L;
    BookieFileChannel channel;
    int fd;
    FileChannel fc;
    int formatVersion;
    BufferedChannel bc;
    long nextPrealloc = 0;

    final byte[] magicWord = "BKLG".getBytes(UTF_8);

    static final int SECTOR_SIZE = 512;
    private static final int START_OF_FILE = -12345;
    private static final long cacheDropLagBytes = 8 * MB;

    // No header
    static final int V1 = 1;
    // Adding header
    static final int V2 = 2;
    // Adding ledger key
    static final int V3 = 3;
    // Adding fencing key
    static final int V4 = 4;
    // 1) expanding header to 512
    // 2) Padding writes to align sector size
    static final int V5 = 5;
    // Adding explicit lac entry
    public static final int V6 = 6;

    static final int HEADER_SIZE = SECTOR_SIZE; // align header to sector size
    static final int VERSION_HEADER_SIZE = 8; // 4byte magic word, 4 byte version
    static final int MIN_COMPAT_JOURNAL_FORMAT_VERSION = V1;
    static final int CURRENT_JOURNAL_FORMAT_VERSION = V6;

    private final long preAllocSize;
    private final int journalAlignSize;
    private final boolean fRemoveFromPageCache;
    public final ByteBuffer zeros;

    // The position of the file channel's last drop position
    private long lastDropPosition = 0L;

    final ServerConfiguration configuration;
    private boolean freshLifecycle;
    private File freshFile;
    private long freshLogId;
    private Object freshForceLock;
    private int freshWriteCapacity;
    private ByteBufAllocator freshAllocator;
    private boolean freshInitAttempted;
    private volatile boolean freshClosing;
    private boolean freshFinalForceComplete;
    private boolean freshMetadataForced;
    private long freshDurablePosition;
    private Throwable freshOperationFailure;
    private Throwable freshFlushFailure;
    private Throwable freshCloseFailure;

    record FreshCloseResult(boolean drained, Throwable operationFailure,
                            Throwable flushFailure, Throwable closeFailure) { }

    /** Pure owner, registered by Journal before initialization can acquire its RAF/fd. */
    static JournalChannel forFresh(File directory, long logId, long preAllocSize, int writeCapacity,
            int alignment, ServerConfiguration configuration, ByteBufAllocator allocator) {
        return new JournalChannel(directory, logId, preAllocSize, writeCapacity, alignment, configuration, allocator);
    }

    private JournalChannel(File directory, long logId, long preAllocSize, int writeCapacity,
            int alignment, ServerConfiguration configuration, ByteBufAllocator allocator) {
        if (alignment <= 0 || preAllocSize < HEADER_SIZE + (long) alignment || writeCapacity <= 0
                || configuration.getJournalFormatVersionToWrite() != V6) {
            throw new IllegalArgumentException("Invalid fresh Journal V6 channel configuration");
        }
        freshLifecycle = true;
        freshForceLock = new Object();
        freshFile = new File(directory, Long.toHexString(logId) + ".txn");
        freshLogId = logId;
        freshWriteCapacity = writeCapacity;
        freshAllocator = allocator;
        this.preAllocSize = preAllocSize - preAllocSize % alignment;
        journalAlignSize = alignment;
        zeros = ByteBuffer.allocate(alignment);
        fRemoveFromPageCache = configuration.getJournalRemovePagesFromCache();
        this.configuration = configuration;
        formatVersion = V6;
        fd = -1;
    }

    synchronized void initializeForFresh(DefaultFileChannelProvider provider) throws IOException {
        if (!freshLifecycle || freshInitAttempted || freshClosing) {
            throw new IllegalStateException("Fresh journal channel initialization is one-use");
        }
        freshInitAttempted = true;
        try {
            // The default provider is heap-only. Keep its lazy wrapper before acquiring the fd.
            channel = provider.open(freshFile, configuration);
            Files.createFile(freshFile.toPath());
            fc = channel.getFileChannel();
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.put(magicWord).putInt(V6).rewind();
            writeFullyAt(header, 0);
            fc.position(HEADER_SIZE);
            bc = new BufferedChannel(freshAllocator, fc, freshWriteCapacity, freshWriteCapacity, 0, true);
            forceWrite(true);
            writeFullyAt(zeros.duplicate(), preAllocSize - journalAlignSize);
            nextPrealloc = preAllocSize;
            if (fRemoveFromPageCache) {
                fd = PageCacheUtil.getSysFileDescriptor(channel.getFD());
            }
        } catch (IOException | RuntimeException | Error failure) {
            freshOperationFailure = failure;
            throw failure;
        }
    }

    private void writeFullyAt(ByteBuffer bytes, long position) throws IOException {
        while (bytes.hasRemaining()) {
            int written = fc.write(bytes, Math.addExact(position, bytes.position()));
            if (written <= 0) {
                throw new IOException("Fresh journal positional write made no progress");
            }
        }
    }

    // Mostly used by tests
    JournalChannel(File journalDirectory, long logId) throws IOException {
        this(journalDirectory, logId, 4 * MB, 65536, START_OF_FILE, new ServerConfiguration(),
            new DefaultFileChannelProvider());
    }

    // Open journal for scanning starting from the first record in journal.
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, ServerConfiguration conf,
                   FileChannelProvider provider) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, START_OF_FILE, conf, provider);
    }

    // Open journal for scanning starting from given position.
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, long position, ServerConfiguration conf,
                   FileChannelProvider provider) throws IOException {
         this(journalDirectory, logId, preAllocSize, writeBufferSize, SECTOR_SIZE,
                 position, false, V5, Journal.BufferedChannelBuilder.DEFAULT_BCBUILDER,
             conf, provider, null);
    }

    // Open journal to write
    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, int journalAlignSize,
                   boolean fRemoveFromPageCache, int formatVersionToWrite,
                   ServerConfiguration conf, FileChannelProvider provider) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, journalAlignSize, fRemoveFromPageCache,
                formatVersionToWrite, Journal.BufferedChannelBuilder.DEFAULT_BCBUILDER, conf, provider, null);
    }

    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, int journalAlignSize,
                   boolean fRemoveFromPageCache, int formatVersionToWrite,
                   Journal.BufferedChannelBuilder bcBuilder, ServerConfiguration conf,
                   FileChannelProvider provider, Long toReplaceLogId) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, journalAlignSize,
                START_OF_FILE, fRemoveFromPageCache, formatVersionToWrite, bcBuilder, conf, provider, toReplaceLogId);
    }

    /**
     * Create a journal file.
     * Allows injection of BufferedChannelBuilder for testing purposes.
     *
     * @param journalDirectory
     *          directory to store the journal file.
     * @param logId
     *          log id for the journal file.
     * @param preAllocSize
     *          pre allocation size.
     * @param writeBufferSize
     *          write buffer size.
     * @param journalAlignSize
     *          size to align journal writes.
     * @param position
     *          position to start read/write
     * @param fRemoveFromPageCache
     *          whether to remove cached pages from page cache.
     * @param formatVersionToWrite
     *          format version to write
     * @throws IOException
     */
    private JournalChannel(File journalDirectory, long logId,
                           long preAllocSize, int writeBufferSize, int journalAlignSize,
                           long position, boolean fRemoveFromPageCache,
                           int formatVersionToWrite, Journal.BufferedChannelBuilder bcBuilder,
                           ServerConfiguration conf,
                           FileChannelProvider provider, Long toReplaceLogId) throws IOException {
        this.journalAlignSize = journalAlignSize;
        this.zeros = ByteBuffer.allocate(journalAlignSize);
        this.preAllocSize = preAllocSize - preAllocSize % journalAlignSize;
        this.fRemoveFromPageCache = fRemoveFromPageCache;
        this.configuration = conf;

        boolean reuseFile = false;
        File fn = new File(journalDirectory, Long.toHexString(logId) + ".txn");
        if (toReplaceLogId != null && logId != toReplaceLogId && provider.supportReuseFile()) {
            File toReplaceFile = new File(journalDirectory, Long.toHexString(toReplaceLogId) + ".txn");
            if (toReplaceFile.exists()) {
                renameJournalFile(toReplaceFile, fn);
                provider.notifyRename(toReplaceFile, fn);
                reuseFile = true;
            }
        }
        channel = provider.open(fn, configuration);

        if (formatVersionToWrite < V4) {
            throw new IOException("Invalid journal format to write : version = " + formatVersionToWrite);
        }

        log.info().attr("journal", fn).log("Opening journal");
        if (!channel.fileExists(fn)) { // create new journal file to write, write version
            if (!fn.createNewFile()) {
                log.error().attr("journal", fn)
                    .log("Journal file, that shouldn't exist, already exists."
                          + " is there another bookie process running?");
                throw new IOException("File " + fn
                        + " suddenly appeared, is another bookie process running?");
            }
            fc = channel.getFileChannel();
            formatVersion = formatVersionToWrite;
            writeHeader(bcBuilder, writeBufferSize);
        } else if (reuseFile) { // Open an existing journal to write, it needs fileChannelProvider support reuse file.
            fc = channel.getFileChannel();
            formatVersion = formatVersionToWrite;
            writeHeader(bcBuilder, writeBufferSize);
        } else {  // open an existing file to read.
            fc = channel.getFileChannel();
            // readonly, use fileChannel directly, no need to use BufferedChannel

            ByteBuffer bb = ByteBuffer.allocate(VERSION_HEADER_SIZE);
            int c = fc.read(bb);
            bb.flip();

            if (c == VERSION_HEADER_SIZE) {
                byte[] first4 = new byte[4];
                bb.get(first4);

                if (Arrays.equals(first4, magicWord)) {
                    formatVersion = bb.getInt();
                } else {
                    // pre magic word journal, reset to 0;
                    formatVersion = V1;
                }
            } else {
                // no header, must be old version
                formatVersion = V1;
            }

            if (formatVersion < MIN_COMPAT_JOURNAL_FORMAT_VERSION
                || formatVersion > CURRENT_JOURNAL_FORMAT_VERSION) {
                String err = String.format("Invalid journal version, unable to read."
                        + " Expected between (%d) and (%d), got (%d)",
                        MIN_COMPAT_JOURNAL_FORMAT_VERSION, CURRENT_JOURNAL_FORMAT_VERSION,
                        formatVersion);
                log.error(err);
                throw new IOException(err);
            }

            try {
                if (position == START_OF_FILE) {
                    if (formatVersion >= V5) {
                        fc.position(HEADER_SIZE);
                    } else if (formatVersion >= V2) {
                        fc.position(VERSION_HEADER_SIZE);
                    } else {
                        fc.position(0);
                    }
                } else {
                    fc.position(position);
                }
            } catch (IOException e) {
                log.error().exception(e).log("Bookie journal file can seek to position");
                throw e;
            }
        }
        if (fRemoveFromPageCache) {
            this.fd = PageCacheUtil.getSysFileDescriptor(channel.getFD());
        } else {
            this.fd = -1;
        }
    }

    private void writeHeader(Journal.BufferedChannelBuilder bcBuilder,
                             int writeBufferSize) throws IOException {
        int headerSize = (V4 == formatVersion) ? VERSION_HEADER_SIZE : HEADER_SIZE;
        ByteBuffer bb = ByteBuffer.allocate(headerSize);
        ZeroBuffer.put(bb);
        bb.clear();
        bb.put(magicWord);
        bb.putInt(formatVersion);
        bb.clear();
        fc.write(bb);

        bc = bcBuilder.create(fc, writeBufferSize);
        forceWrite(true);
        nextPrealloc = this.preAllocSize;
        fc.write(zeros, nextPrealloc - journalAlignSize);
    }

    public static void renameJournalFile(File source, File target) throws IOException {
        if (source == null || target == null || !source.renameTo(target)) {
            log.error()
                    .attr("source", source)
                    .attr("target", target)
                    .log("Failed to rename file");
            throw new IOException("Failed to rename file " + source + " to " + target);
        }
    }

    int getFormatVersion() {
        return formatVersion;
    }

    BufferedChannel getBufferedChannel() throws IOException {
        if (bc == null) {
            throw new IOException("Read only journal channel");
        }
        return bc;
    }

    void preAllocIfNeeded(long size) throws IOException {
        if (freshLifecycle) {
            synchronized (this) {
                requireFreshWrite();
                long end = Math.addExact(bc.position(), size);
                if (end > nextPrealloc) {
                    long candidate = nextPrealloc;
                    while (end > candidate) {
                        candidate = Math.addExact(candidate, preAllocSize);
                    }
                    writeFullyAt(zeros.duplicate(), candidate - journalAlignSize);
                    nextPrealloc = candidate;
                }
                return;
            }
        }
        if (bc.position() + size > nextPrealloc) {
            nextPrealloc += preAllocSize;
            zeros.clear();
            fc.write(zeros, nextPrealloc - journalAlignSize);
        }
    }

    int read(ByteBuffer dst)
            throws IOException {
        return fc.read(dst);
    }

    @Override
    public void close() throws IOException {
        if (freshLifecycle) {
            throw new IllegalStateException("Use the explicit fresh journal channel close result");
        }
        if (bc != null) {
            bc.close();
        } else if (fc != null) {
            fc.close();
        }
    }

    public void forceWrite(boolean forceMetadata) throws IOException {
        if (freshLifecycle) {
            synchronized (freshForceLock) {
                if (freshClosing) {
                    throw new IOException("Fresh journal channel is closing");
                }
                long cut = bc.forceWrite(forceMetadata);
                freshDurablePosition = Math.max(freshDurablePosition, cut);
                freshMetadataForced |= forceMetadata;
                dropCache(cut);
            }
            return;
        }
        log.debug("Journal ForceWrite");
        long newForceWritePosition = bc.forceWrite(forceMetadata);
        dropCache(newForceWritePosition);
    }

    private void dropCache(long newForceWritePosition) {
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning
        // of the file to a position prior to the current position.
        //
        // The cacheDropLagBytes is to prevent dropping a page that will
        // be appended again, which would introduce random seeking on journal
        // device.
        //
        // <======== drop ==========>
        //                           <-----------LAG------------>
        // +------------------------+---------------------------O
        // lastDropPosition     newDropPos             lastForceWritePosition
        //
        if (fRemoveFromPageCache) {
            long newDropPos = newForceWritePosition - cacheDropLagBytes;
            if (lastDropPosition < newDropPos) {
                PageCacheUtil.bestEffortRemoveFromPageCache(fd, lastDropPosition, newDropPos - lastDropPosition);
            }
            this.lastDropPosition = newDropPos;
        }
    }

    void forceThroughForFresh(long cut) throws IOException {
        if (!freshLifecycle) {
            throw new IllegalStateException("Not a fresh journal channel");
        }
        synchronized (freshForceLock) {
            if (freshClosing) {
                if (freshMetadataForced && freshDurablePosition >= cut && channel instanceof DefaultFileChannel file
                        && file.closedForFresh() && freshCloseFailure == null && freshFlushFailure == null) {
                    return; // Only the original completed barrier/close may satisfy a retired channel.
                }
                throw new IOException("Fresh journal channel has not completed this durable cut", freshCloseFailure);
            }
            forceWrite(false);
            if (freshDurablePosition < cut) {
                throw new IOException("Fresh journal force did not cover the requested cut");
            }
        }
    }

    synchronized FreshCloseResult closeForFresh() {
        if (!freshLifecycle) {
            throw new IllegalStateException("Not a fresh journal channel");
        }
        synchronized (freshForceLock) {
            return closeFreshInternal();
        }
    }

    private FreshCloseResult closeFreshInternal() {
        freshClosing = true;
        DefaultFileChannel file = (DefaultFileChannel) channel;
        if (file != null && fc != null && !fc.isOpen() && !file.closedForFresh()) {
            closeNativeForFresh(file); // Do not requalify an unobserved automatic close.
        }
        if (freshCloseFailure == null && bc != null && !freshFinalForceComplete) {
            try {
                bc.flushAndForceWrite(true);
                freshDurablePosition = Math.max(freshDurablePosition, bc.getFileChannelPosition());
                freshMetadataForced = true;
                freshFinalForceComplete = true;
                freshFlushFailure = null;
            } catch (IOException | RuntimeException | Error failure) {
                freshFlushFailure = failure;
            }
        }
        if (freshFlushFailure == null || freshCloseFailure != null) {
            if (file != null) {
                closeNativeForFresh(file);
            }
            if (bc != null) {
                try {
                    bc.releaseBuffersForFresh();
                } catch (IOException | RuntimeException | Error failure) {
                    if (freshCloseFailure == null) {
                        freshCloseFailure = failure;
                    }
                }
            }
        }
        Throwable writeFailure = bc == null ? null : bc.freshWriteFailure();
        return new FreshCloseResult(freshFlushFailure == null && freshCloseFailure == null
                && (file == null || file.closedForFresh()),
                freshOperationFailure != null ? freshOperationFailure : writeFailure,
                freshFlushFailure, freshCloseFailure);
    }

    private void closeNativeForFresh(DefaultFileChannel file) {
        try {
            file.closeForFresh();
        } catch (IOException | RuntimeException | Error failure) {
            if (freshCloseFailure == null) {
                freshCloseFailure = failure;
            }
        }
    }

    private void requireFreshWrite() throws IOException {
        if (freshClosing || freshOperationFailure != null) {
            throw new IOException("Fresh journal channel is closing or initialization failed", freshOperationFailure);
        }
    }

    boolean ownsFreshLog(long id) {
        return freshLifecycle && freshLogId == id;
    }

}

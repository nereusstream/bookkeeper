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
import static org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification.WATCHER_RECYCLER;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.Watchable;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.proto.checksum.DigestManager;

/**
 * This is the file handle for a ledger's index file that maps entry ids to location.
 * It is used by LedgerCache.
 *
 * <p>
 * Ledger index file is made of a header and several fixed-length index pages, which records the offsets of data stored
 * in entry loggers
 * <pre>&lt;header&gt;&lt;index pages&gt;</pre>
 * <b>Header</b> is formatted as below:
 * <pre>&lt;magic bytes&gt;&lt;len of master key&gt;&lt;master key&gt;</pre>
 * <ul>
 * <li>magic bytes: 4 bytes, 'BKLE', version: 4 bytes
 * <li>len of master key: indicates length of master key. -1 means no master key stored in header.
 * <li>master key: master key
 * <li>state: bit map to indicate the state, 32 bits.
 * </ul>
 * <b>Index page</b> is a fixed-length page, which contains several entries which point to the offsets of data stored
 * in entry loggers.
 * </p>
 */
@CustomLog
class FileInfo extends Watchable<LastAddConfirmedUpdateNotification> {

    static final int NO_MASTER_KEY = -1;
    static final int STATE_FENCED_BIT = 0x1;

    private FileChannel fc;
    private File lf;
    private ByteBuffer explicitLac = null;

    byte[] masterKey;

    /**
     * The fingerprint of a ledger index file.
     */
    public static final int SIGNATURE = ByteBuffer.wrap("BKLE".getBytes(UTF_8)).getInt();

    // No explicitLac
    static final int V0 = 0;
    // Adding explicitLac
    static final int V1 = 1;
    // current version of FileInfo header is V1
    public static final int CURRENT_HEADER_VERSION = V1;

    static final long START_OF_DATA = 1024;
    private long size;
    private boolean isClosed;
    private long sizeSinceLastWrite;

    // bit map for states of the ledger.
    private int stateBits;
    private boolean needFlushHeader = false;

    // lac
    private Long lac = null;

    // file access mode
    protected String mode;

    // this FileInfo Header Version
    int headerVersion;

    private boolean deleted;
    private final FreshState fresh;
    private final boolean historical;

    record FreshCloseResult(boolean drained, Throwable operationFailure,
                            Throwable flushFailure, Throwable closeFailure) { }

    private static final class FreshChannel {
        FileChannel channel;
        boolean closeAttempted;
        Throwable closeFailure;
    }

    private static final class FreshState {
        final List<FreshChannel> channels = new ArrayList<>();
        volatile boolean closing;
        boolean notifyClose;
        boolean notificationRunning;
        Boolean forceClose;
        ByteBuffer closeHeader;
        Throwable operationFailure;
        Throwable flushFailure;
        Throwable closeFailure;
        PendingIndexWrite pendingWrite;
        Throwable priorWriteFailure;
    }

    private static final class PendingIndexWrite {
        final long position;
        final List<ByteBuffer> bytes = new ArrayList<>();
        long length;

        PendingIndexWrite(long position, ByteBuffer[] source) {
            this.position = position;
            for (ByteBuffer buffer : source) {
                ByteBuffer copy = ByteBuffer.allocate(buffer.remaining());
                copy.put(buffer.duplicate()).flip();
                length = Math.addExact(length, copy.remaining());
                bytes.add(copy);
            }
        }

        boolean matches(long candidatePosition, ByteBuffer[] source) {
            if (position != candidatePosition || source.length != bytes.size()) {
                return false;
            }
            for (int i = 0; i < source.length; i++) {
                ByteBuffer expected = bytes.get(i).duplicate();
                expected.rewind();
                if (!expected.equals(source[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    public FileInfo(File lf, byte[] masterKey, int fileInfoVersionToWrite) throws IOException {
        this(lf, masterKey, fileInfoVersionToWrite, false);
    }

    static FileInfo forFresh(File lf, byte[] masterKey, int fileInfoVersionToWrite) throws IOException {
        return new FileInfo(lf, masterKey, fileInfoVersionToWrite, true);
    }

    static FileInfo forClassicHistory(File file, byte[] masterKey, int version) throws IOException {
        return new FileInfo(file, masterKey, version, true, true);
    }

    FileInfo(File lf, byte[] masterKey, int fileInfoVersionToWrite, boolean freshLifecycle) throws IOException {
        this(lf, masterKey, fileInfoVersionToWrite, freshLifecycle, false);
    }

    FileInfo(File lf, byte[] masterKey, int fileInfoVersionToWrite, boolean freshLifecycle, boolean historical)
            throws IOException {
        super(WATCHER_RECYCLER);

        fresh = freshLifecycle ? new FreshState() : null;
        this.historical = historical;
        this.lf = lf;
        this.masterKey = masterKey;
        mode = "rw";
        this.headerVersion = fileInfoVersionToWrite;
        this.deleted = false;
    }

    synchronized Long getLastAddConfirmed() {
        requireFreshMemoryAccess();
        return lac;
    }

    long setLastAddConfirmed(long lac) {
        long lacToReturn;
        boolean changed = false;
        synchronized (this) {
            requireFreshMemoryAccess();
            if (null == this.lac || this.lac < lac) {
                this.lac = lac;
                changed = true;
            }
            lacToReturn = this.lac;
        }
        log.trace()
                .attr("currentLac", lacToReturn)
                .attr("newLac", lac)
                .log("Updating LAC");

        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, lacToReturn);
        }
        return lacToReturn;
    }

    synchronized boolean waitForLastAddConfirmedUpdate(long previousLAC,
                                                       Watcher<LastAddConfirmedUpdateNotification> watcher) {
        if ((null != lac && lac > previousLAC) || isClosed) {
            log.trace()
                    .attr("lac", this.lac)
                    .attr("previousLAC", previousLAC)
                    .log("Wait For LAC");
            return false;
        }

        addWatcher(watcher);
        return true;
    }

    synchronized void cancelWaitForLastAddConfirmedUpdate(Watcher<LastAddConfirmedUpdateNotification> watcher) {
        deleteWatcher(watcher);
    }

    public boolean isClosed() {
        if (fresh != null) {
            return fresh.closing;
        }
        return isClosed;
    }

    public synchronized File getLf() {
        return lf;
    }

    public long getSizeSinceLastWrite() {
        return sizeSinceLastWrite;
    }

    public ByteBuf getExplicitLac() {
        ByteBuf retLac = null;
        synchronized (this) {
            requireFreshMemoryAccess();
            log.debug().attr("explicitLac", explicitLac).log("fileInfo:GetLac");
            if (explicitLac != null) {
                retLac = Unpooled.buffer(explicitLac.capacity());
                explicitLac.rewind(); //copy from the beginning
                retLac.writeBytes(explicitLac);
                explicitLac.rewind();
                return retLac;
            }
        }
        return retLac;
    }

    public void setExplicitLac(ByteBuf lac) {
        long explicitLacValue;
        synchronized (this) {
            requireFreshMemoryAccess();
            if (explicitLac == null) {
                explicitLac = ByteBuffer.allocate(lac.capacity());
            }
            lac.readBytes(explicitLac);
            explicitLac.rewind();

            // skip the ledger id
            explicitLac.getLong();
            explicitLacValue = explicitLac.getLong();
            explicitLac.rewind();
            log.debug().attr("explicitLac", explicitLac).log("fileInfo:SetLac");
            needFlushHeader = true;
        }
        setLastAddConfirmed(explicitLacValue);
    }

    public synchronized void readHeader() throws IOException {
        requireFreshAccess();
        try {
            readHeaderInternal();
        } catch (IOException | RuntimeException | Error failure) {
            recordFreshOperationFailure(failure);
            throw failure;
        }
    }

    private void readHeaderInternal() throws IOException {
        if (lf.exists()) {
            if (fc != null) {
                return;
            }

            fc = openIndexFile(lf, false);
            size = fc.size();
            sizeSinceLastWrite = size;

            // avoid hang on reading partial index
            ByteBuffer bb = ByteBuffer.allocate((int) (Math.min(size, START_OF_DATA)));
            while (bb.hasRemaining()) {
                if (fc.read(bb) <= 0 && fresh != null) {
                    throw new ShortReadException("Short index header at " + lf);
                }
            }
            bb.flip();
            if (historical) {
                requireHistoricalHeader(bb.duplicate(), size);
            }
            if (bb.getInt() != SIGNATURE) {
                throw new IOException("Missing ledger signature while reading header for " + lf);
            }
            int version = bb.getInt();
            if (version > CURRENT_HEADER_VERSION) {
                throw new IOException("Incompatible ledger version " + version + " while reading header for " + lf);
            }
            this.headerVersion = version;

            int length = bb.getInt();
            if (length < 0) {
                throw new IOException("Length " + length + " is invalid while reading header for " + lf);
            } else if (length > bb.remaining()) {
                throw new BufferUnderflowException();
            }
            masterKey = new byte[length];
            bb.get(masterKey);
            stateBits = bb.getInt();

            if (this.headerVersion >= V1) {
                int explicitLacBufLength = bb.getInt();
                if (explicitLacBufLength == 0) {
                    explicitLac = null;
                } else if (explicitLacBufLength >= DigestManager.LAC_METADATA_LENGTH) {
                    if (fresh != null && explicitLacBufLength > bb.remaining()) {
                        throw new IOException("Explicit LAC exceeds the owned index header at " + lf);
                    }
                    if (explicitLac == null) {
                        explicitLac = ByteBuffer.allocate(explicitLacBufLength);
                    }
                    byte[] explicitLacBufArray = new byte[explicitLacBufLength];
                    bb.get(explicitLacBufArray);
                    explicitLac.put(explicitLacBufArray);
                    explicitLac.rewind();
                } else {
                    throw new IOException("ExplicitLacBufLength " + explicitLacBufLength
                            + " is invalid while reading header for " + lf);
                }
            }

            needFlushHeader = false;
        } else {
            throw new IOException("Ledger index file " + lf + " does not exist");
        }
    }

    public synchronized boolean isDeleted() {
        return deleted;
    }

    public static class FileInfoDeletedException extends IOException {
        FileInfoDeletedException() {
            super("FileInfo already deleted");
        }
    }

    @VisibleForTesting
    void checkOpen(boolean create) throws IOException {
        checkOpen(create, false);
    }

    private synchronized void checkOpen(boolean create, boolean openBeforeClose)
            throws IOException {
        if (!openBeforeClose) {
            requireFreshAccess();
        }
        try {
            checkOpenInternal(create, openBeforeClose);
        } catch (IOException | RuntimeException | Error failure) {
            recordFreshOperationFailure(failure);
            throw failure;
        }
    }

    private void checkOpenInternal(boolean create, boolean openBeforeClose) throws IOException {
        if (deleted) {
            throw new FileInfoDeletedException();
        }
        if (fc != null) {
            return;
        }
        boolean exists = lf.exists();
        if (masterKey == null && !exists) {
            throw new IOException(lf + " not found");
        }

        if (!exists) {
            if (create) {
                // delayed the creation of parents directories
                checkParents(lf);
                fc = openIndexFile(lf, true);
                size = fc.size();
                if (size == 0) {
                    writeHeader();
                }
            }
        } else {
            if (openBeforeClose) {
                // if it is checking for close, skip reading header
                return;
            }
            try {
                readHeader();
            } catch (BufferUnderflowException buf) {
                if (fresh != null) {
                    throw new IOException("Incomplete fresh index header " + lf, buf);
                }
                log.warn()
                        .exception(buf)
                        .attr("file", lf)
                        .log("Exception when reading header");
                if (null != masterKey) {
                    log.warn().attr("file", lf).log("Attempting to write header again.");
                    writeHeader();
                } else {
                    throw new IOException("Error reading header " + lf);
                }
            }
        }
    }

    private void writeHeader() throws IOException {
        ByteBuffer bb = headerBytes();
        if (fresh != null) {
            try {
                writeHeaderAtFixedPosition(bb);
            } catch (Throwable failure) {
                recordFreshOperationFailure(failure);
                throw asIOException(failure);
            }
            return;
        }
        fc.position(0);
        fc.write(bb);
    }

    private ByteBuffer headerBytes() {
        ByteBuffer bb = ByteBuffer.allocate((int) START_OF_DATA);
        bb.putInt(SIGNATURE);
        bb.putInt(this.headerVersion);
        bb.putInt(masterKey.length);
        bb.put(masterKey);
        bb.putInt(stateBits);
        if (this.headerVersion >= V1) {
            if (explicitLac != null) {
                explicitLac.rewind();
                bb.putInt(explicitLac.capacity());
                bb.put(explicitLac);
                explicitLac.rewind();
            } else {
                bb.putInt(0);
            }
        }
        bb.rewind();
        return bb;
    }

    private void writeHeaderAtFixedPosition(ByteBuffer header) throws IOException {
        ByteBuffer remaining = header.duplicate();
        remaining.rewind();
        while (remaining.hasRemaining()) {
            if (fc.write(remaining, remaining.position()) <= 0) {
                throw new IOException("Short index header write at " + lf);
            }
        }
    }

    public synchronized boolean isFenced() throws IOException {
        checkOpen(false);
        return (stateBits & STATE_FENCED_BIT) == STATE_FENCED_BIT;
    }

    /**
     * @return true if set fence succeed, otherwise false when
     * it already fenced or failed to set fenced.
     */
    public boolean setFenced() throws IOException {
        boolean returnVal = false;
        boolean changed = false;
        synchronized (this) {
            checkOpen(false);
            log.debug()
                    .attr("file", lf)
                    .attr("stateBits", stateBits)
                    .log("Try to set fenced state in file info");
            if ((stateBits & STATE_FENCED_BIT) != STATE_FENCED_BIT) {
                // not fenced yet
                stateBits |= STATE_FENCED_BIT;
                needFlushHeader = true;
                changed = true;
                returnVal = true;
            }
        }
        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, Long.MAX_VALUE);
        }
        return returnVal;
    }

    // flush the header when header is changed
    public synchronized void flushHeader() throws IOException {
        if (fresh != null && fresh.pendingWrite != null && !needFlushHeader && !fresh.closing
                && fresh.operationFailure == null && fresh.closeFailure == null) {
            // The header preceding this fixed-coordinate write already completed. Only its exact DATA range retries.
            finishPendingIndexWrite();
            return;
        }
        requireFreshAccess();
        if (needFlushHeader) {
            checkOpen(true);
            writeHeader();
            needFlushHeader = false;
        }
    }

    public synchronized long size() throws IOException {
        checkOpen(false);
        long rc = size - START_OF_DATA;
        if (rc < 0) {
            rc = 0;
        }
        return rc;
    }

    public int read(ByteBuffer bb, long position, boolean bestEffort)
            throws IOException {
        return readAbsolute(bb, position + START_OF_DATA, bestEffort);
    }

    /**
     * Read data from position <i>start</i> to fill the byte buffer <i>bb</i>.
     * If <i>bestEffort </i> is provided, it would return when it reaches EOF.
     * Otherwise, it would throw {@link org.apache.bookkeeper.bookie.ShortReadException}
     * if it reaches EOF.
     *
     * @param bb
     *          byte buffer of data
     * @param start
     *          start position to read data
     * @param bestEffort
     *          flag indicates if it is a best-effort read
     * @return number of bytes read
     * @throws IOException
     */
    private int readAbsolute(ByteBuffer bb, long start, boolean bestEffort)
            throws IOException {
        if (fresh != null) {
            synchronized (this) {
                try {
                    return readAbsoluteInternal(bb, start, bestEffort);
                } catch (IOException | RuntimeException | Error failure) {
                    recordFreshOperationFailure(failure);
                    throw failure;
                }
            }
        }
        return readAbsoluteInternal(bb, start, bestEffort);
    }

    private int readAbsoluteInternal(ByteBuffer bb, long start, boolean bestEffort) throws IOException {
        checkOpen(false);
        synchronized (this) {
            if (fc == null) {
                return 0;
            }
        }
        int total = 0;
        int rc = 0;
        while (bb.remaining() > 0) {
            synchronized (this) {
                rc = fc.read(bb, start);
            }
            if (rc <= 0) {
                if (bestEffort) {
                    return total;
                } else {
                    throw new ShortReadException("Short read at " + getLf().getPath() + "@" + start);
                }
            }
            total += rc;
            // should move read position
            start += rc;
        }
        return total;
    }

    /**
     * Close a file info. Generally, force should be set to true. If set to false metadata will not be flushed and
     * accessing metadata before restart and recovery will be unsafe (since reloading from the index file will
     * cause metadata to be lost). Setting force=false helps avoid expensive file create during shutdown with many
     * dirty ledgers, and is safe because ledger metadata will be recovered before being accessed again.
     *
     * @param force
     *          if set to true, the index is forced to create before closed,
     *          if set to false, the index is not forced to create.
     */
    public void close(boolean force) throws IOException {
        if (fresh != null) {
            FreshCloseResult result = closeForFresh(force);
            Throwable failure = result.closeFailure() != null ? result.closeFailure()
                    : result.flushFailure() != null ? result.flushFailure() : result.operationFailure();
            if (failure != null) {
                throw asIOException(failure);
            }
            return;
        }
        boolean changed = false;
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
            checkOpen(force, true);
            // Any time when we force close a file, we should try to flush header.
            // otherwise, we might lose fence bit.
            if (force) {
                flushHeader();
            }
            changed = true;
            if (fc != null) {
                fc.close();
            }
            fc = null;
        }
        if (changed) {
            notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, Long.MAX_VALUE);
        }
    }

    public synchronized long write(ByteBuffer[] buffs, long position) throws IOException {
        if (fresh != null) {
            if (fresh.closing || fresh.operationFailure != null || fresh.closeFailure != null) {
                throw new IOException("Fresh index cannot accept a write", fresh.operationFailure);
            }
            if (fresh.pendingWrite == null) {
                checkOpen(true);
                fresh.pendingWrite = new PendingIndexWrite(position, buffs);
            } else if (!fresh.pendingWrite.matches(position, buffs)) {
                throw new IOException("Fresh index has a different unresolved write", fresh.priorWriteFailure);
            }
            long length = finishPendingIndexWrite();
            for (ByteBuffer buffer : buffs) {
                buffer.position(buffer.limit());
            }
            return length;
        }
        checkOpen(true);
        return writeInternal(buffs, position);
    }

    private long finishPendingIndexWrite() throws IOException {
        PendingIndexWrite write = fresh.pendingWrite;
        try {
            long offset = Math.addExact(write.position, START_OF_DATA);
            for (ByteBuffer buffer : write.bytes) {
                while (buffer.hasRemaining()) {
                    if (fc.write(buffer, Math.addExact(offset, buffer.position())) <= 0) {
                        throw new IOException("Short fresh index write at " + lf);
                    }
                }
                offset = Math.addExact(offset, buffer.limit());
            }
            fc.force(true);
            long writtenSize = fc.size();
            size = Math.max(size, Math.addExact(Math.addExact(write.position, START_OF_DATA), write.length));
            sizeSinceLastWrite = writtenSize;
            fresh.pendingWrite = null;
            fresh.flushFailure = null;
            return write.length;
        } catch (Throwable failure) {
            if (fresh.priorWriteFailure == null) {
                fresh.priorWriteFailure = failure;
            }
            fresh.flushFailure = failure;
            throw asIOException(failure);
        }
    }

    private long writeInternal(ByteBuffer[] buffs, long position) throws IOException {
        long total = 0;
        try {
            fc.position(position + START_OF_DATA);
            while (buffs[buffs.length - 1].remaining() > 0) {
                long rc = fc.write(buffs);
                if (rc <= 0) {
                    throw new IOException("Short write");
                }
                total += rc;
            }
        } catch (IOException | RuntimeException | Error failure) {
            recordFreshOperationFailure(failure);
            throw failure;
        } finally {
            try {
                fc.force(true);
            } catch (IOException | RuntimeException | Error failure) {
                recordFreshOperationFailure(failure);
                throw failure;
            }
            long newsize = position + START_OF_DATA + total;
            if (newsize > size) {
                size = newsize;
            }
        }
        sizeSinceLastWrite = fc.size();
        return total;
    }

    /**
     * Copies current file contents upto specified size to the target file and
     * deletes the current file. If size not known then pass size as
     * Long.MAX_VALUE to copy complete file.
     */
    public synchronized void moveToNewLocation(File newFile, long size) throws IOException {
        checkOpen(false);
        if (fresh != null) {
            try {
                moveToNewLocationInternal(newFile, size);
            } catch (Throwable failure) {
                recordFreshOperationFailure(failure);
                throw asIOException(failure);
            }
            return;
        }
        moveToNewLocationInternal(newFile, size);
    }

    private void moveToNewLocationInternal(File newFile, long size) throws IOException {
        // If the channel is null, or same file path, just return.
        if (null == fc || isSameFile(newFile)) {
            return;
        }
        if (size > fc.size()) {
            size = fc.size();
        }
        File rlocFile = new File(newFile.getParentFile(), newFile.getName() + IndexPersistenceMgr.RLOC);
        if (!rlocFile.exists()) {
            checkParents(rlocFile);
            if (!rlocFile.createNewFile()) {
                throw new IOException("Creating new cache index file " + rlocFile + " failed ");
            }
        }
        // copy contents from old.idx to new.idx.rloc
        FileChannel newFc = openIndexFile(rlocFile, false);
        try {
            long written = 0;
            while (written < size) {
                long count = fc.transferTo(written, fresh == null ? size : size - written, newFc);
                if (count <= 0) {
                    throw new IOException("Copying to new location " + rlocFile + " failed");
                }
                written += count;
            }
            if (written <= 0 && size > 0) {
                throw new IOException("Copying to new location " + rlocFile + " failed");
            }
        } catch (IOException | RuntimeException | Error failure) {
            recordFreshOperationFailure(failure);
            throw failure;
        } finally {
            if (fresh == null) {
                newFc.force(true);
                newFc.close();
            } else {
                try {
                    newFc.force(true);
                } catch (IOException | RuntimeException | Error failure) {
                    recordFreshOperationFailure(failure);
                    throw failure;
                } finally {
                    closeIndexFile(newFc);
                }
            }
        }
        // delete old.idx
        if (fresh == null) {
            fc.close();
        } else {
            closeIndexFile(fc);
            fc = null;
        }
        if (!delete()) {
            log.error().attr("file", lf).log("Failed to delete the previous index file");
            throw new IOException("Failed to delete the previous index file " + lf);
        }

        // rename new.idx.rloc to new.idx
        if (!rlocFile.renameTo(newFile)) {
            log.error()
                    .attr("source", rlocFile)
                    .attr("target", newFile)
                    .log("Failed to rename file");
            throw new IOException("Failed to rename " + rlocFile + " to " + newFile);
        }
        fc = openIndexFile(newFile, false);
        lf = newFile;
        deleted = false;
    }

    public synchronized byte[] getMasterKey() throws IOException {
        checkOpen(false);
        return masterKey;
    }

    public synchronized boolean delete() {
        if (fresh != null && !fresh.channels.isEmpty()) {
            throw new IllegalStateException("Fresh index resources must close successfully before deletion");
        }
        deleted = true;
        return lf.delete();
    }

    /** Cache admission and reference ownership are handled by the backing cache, before this method is called. */
    FreshCloseResult closeForFresh(boolean force) {
        boolean notify;
        synchronized (this) {
            if (fresh == null) {
                throw new IllegalStateException("Not a fresh index file owner");
            }
            fresh.closing = true;
            isClosed = true;
            notify = !fresh.notifyClose;
            fresh.notifyClose = true;
            if (notify) {
                fresh.notificationRunning = true;
            }
            if (fresh.forceClose == null) {
                fresh.forceClose = force;
            }
            fresh.flushFailure = null;
            for (FreshChannel resource : List.copyOf(fresh.channels)) {
                if (!resource.closeAttempted && !resource.channel.isOpen()) {
                    try {
                        closeIndexFile(resource.channel);
                    } catch (IOException failure) {
                        // Record unobserved automatic close even when a pending flush cannot finish.
                    }
                }
            }
            if (fresh.pendingWrite != null) {
                try {
                    finishPendingIndexWrite();
                } catch (IOException failure) {
                    fresh.flushFailure = failure;
                }
            }
            if (fresh.flushFailure == null && fresh.forceClose && !deleted
                    && fresh.operationFailure == null && fresh.closeFailure == null) {
                try {
                    if (fc == null && (!lf.exists() || needFlushHeader)) {
                        checkParents(lf);
                        fc = openIndexFile(lf, !lf.exists());
                        size = fc.size();
                        if (size == 0) {
                            needFlushHeader = true;
                        }
                    }
                    if (fc != null) {
                        if (needFlushHeader) {
                            if (fresh.closeHeader == null) {
                                fresh.closeHeader = headerBytes();
                            }
                            writeHeaderAtFixedPosition(fresh.closeHeader);
                        }
                        fc.force(true);
                        needFlushHeader = false;
                    }
                } catch (Throwable failure) {
                    fresh.flushFailure = failure;
                }
            }
            if (fresh.flushFailure == null) {
                // Every candidate was registered before open, including failed relocation candidates.
                for (FreshChannel resource : List.copyOf(fresh.channels)) {
                    try {
                        closeIndexFile(resource.channel);
                    } catch (IOException failure) {
                        // The original result stays on the resource. Independent handles still drain.
                    }
                }
                if (fresh.channels.isEmpty()) {
                    fc = null;
                }
            }
        }
        if (notify) {
            try {
                notifyWatchers(LastAddConfirmedUpdateNotification.FUNC, Long.MAX_VALUE);
            } catch (Throwable failure) {
                synchronized (this) {
                    recordFreshOperationFailure(failure);
                }
            } finally {
                synchronized (this) {
                    fresh.notificationRunning = false;
                    notifyAll();
                }
            }
        }
        synchronized (this) {
            return new FreshCloseResult(fresh.channels.isEmpty() && fresh.flushFailure == null
                    && fresh.closeFailure == null && !fresh.notificationRunning,
                    fresh.operationFailure != null ? fresh.operationFailure : fresh.priorWriteFailure,
                    fresh.flushFailure, fresh.closeFailure);
        }
    }

    synchronized void beginClosingForFresh() {
        if (fresh == null) {
            throw new IllegalStateException("Not a fresh index file owner");
        }
        fresh.closing = true;
        isClosed = true;
    }

    /** Bounded V1 validation before historical index recovery can mutate a relocation candidate. */
    static void requireHistoricalHeader(ByteBuffer source, long fileSize) throws IOException {
        ByteBuffer bytes = source.duplicate();
        if (fileSize < START_OF_DATA || (fileSize - START_OF_DATA) % Long.BYTES != 0
                || bytes.remaining() != START_OF_DATA || bytes.getInt() != SIGNATURE || bytes.getInt() != V1) {
            throw new IOException("Controlled Classic history requires a complete V1 index");
        }
        int keyLength = bytes.getInt();
        if (keyLength < 0 || keyLength > bytes.remaining() - 8) {
            throw new IOException("Historical index master key exceeds its header");
        }
        bytes.position(bytes.position() + keyLength + 4);
        int lacLength = bytes.getInt();
        if (lacLength != 0 && (lacLength < DigestManager.LAC_METADATA_LENGTH || lacLength > bytes.remaining())) {
            throw new IOException("Historical index explicit LAC exceeds its header");
        }
    }

    private FileChannel openIndexFile(File file, boolean create) throws IOException {
        if (fresh == null) {
            return new RandomAccessFile(file, mode).getChannel();
        }
        FreshChannel resource = new FreshChannel();
        fresh.channels.add(resource);
        try {
            if (historical) {
                if (create) {
                    resource.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                } else {
                    ClassicHistoryFiles.fileIdentity(file.toPath());
                    resource.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                }
            } else {
                resource.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
            return resource.channel;
        } catch (Throwable failure) {
            fresh.channels.remove(resource); // No channel was returned; there is no acquired handle.
            throw asIOException(failure);
        }
    }

    private void closeIndexFile(FileChannel channel) throws IOException {
        FreshChannel resource = null;
        for (FreshChannel candidate : fresh.channels) {
            if (candidate.channel == channel) {
                resource = candidate;
                break;
            }
        }
        if (resource == null) {
            throw new IOException("Unregistered fresh index channel");
        }
        if (!resource.closeAttempted) {
            resource.closeAttempted = true;
            try {
                if (!channel.isOpen()) {
                    throw new IOException("Fresh index channel closed without an observed native close result");
                }
                channel.close();
                fresh.channels.remove(resource);
                return;
            } catch (Throwable failure) {
                resource.closeFailure = failure;
                if (fresh.closeFailure == null) {
                    fresh.closeFailure = failure;
                }
            }
        }
        throw asIOException(resource.closeFailure);
    }

    private void requireFreshAccess() throws IOException {
        if (fresh != null) {
            if (fresh.closing) {
                throw new IOException("Fresh index file is closing");
            }
            Throwable failure = fresh.operationFailure != null ? fresh.operationFailure : fresh.closeFailure;
            if (failure == null && fresh.pendingWrite != null) {
                failure = fresh.flushFailure;
            }
            if (failure != null) {
                throw new IOException("Fresh index file has an unresolved operation", failure);
            }
        }
    }

    private void requireFreshMemoryAccess() {
        try {
            requireFreshAccess();
        } catch (IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private void recordFreshOperationFailure(Throwable failure) {
        if (fresh != null && fresh.operationFailure == null) {
            fresh.operationFailure = failure;
        }
    }

    private static IOException asIOException(Throwable failure) {
        return failure instanceof IOException io ? io : new IOException("Fresh index operation failed", failure);
    }

    private static void checkParents(File f) throws IOException {
        File parent = f.getParentFile();
        if (parent.exists()) {
            return;
        }
        if (!parent.mkdirs()) {
            throw new IOException("Couldn't mkdirs for " + parent);
        }
    }

    public synchronized boolean isSameFile(File f) {
        return this.lf.equals(f);
    }
}

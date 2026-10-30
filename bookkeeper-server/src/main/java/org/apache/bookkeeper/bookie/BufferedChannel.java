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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a buffering layer in front of a FileChannel.
 */
public class BufferedChannel extends BufferedReadChannel implements Closeable {
    /**
     * The capacity of the write buffer.
     */
    protected final int writeCapacity;
    /**
     * The position of the file channel's write pointer.
     */
    protected AtomicLong writeBufferStartPosition;
    /**
     * The buffer used to write operations.
     */
    protected final ByteBuf writeBuffer;
    /**
     * The absolute position of the next write operation.
     */
    protected volatile long position;

    /*
     * if unpersistedBytesBound is non-zero value, then after writing to
     * writeBuffer, it will check if the unpersistedBytes is greater than
     * unpersistedBytesBound and then calls flush method if it is greater.
     *
     * It is a best-effort feature, since 'forceWrite' method is not
     * synchronized and unpersistedBytes is reset in 'forceWrite' method before
     * calling fileChannel.force
     */
    protected final long unpersistedBytesBound;
    private final boolean doRegularFlushes;

    /*
     * it tracks the number of bytes which are not persisted yet by force
     * writing the FileChannel. The unpersisted bytes could be in writeBuffer or
     * in fileChannel system cache.
     */
    protected final AtomicLong unpersistedBytes;

    private boolean closed = false;
    private final boolean freshLifecycle;
    private boolean freshCloseStarted;
    private boolean freshCloseReturned;
    private boolean freshWriteReleased;
    private boolean freshReadReleased;
    private Throwable freshCloseFailure;
    private ByteBuffer freshPendingFlush;
    private long freshPendingFlushStart;

    // make constructor to be public for unit test
    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int capacity) throws IOException {
        // Use the same capacity for read and write buffers.
        this(allocator, fc, capacity, 0L);
    }

    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int capacity, long unpersistedBytesBound)
            throws IOException {
        // Use the same capacity for read and write buffers.
        this(allocator, fc, capacity, capacity, unpersistedBytesBound);
    }

    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity,
            long unpersistedBytesBound) throws IOException {
        this(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound, false);
    }

    /** The fresh allocator retains the raw channel if construction fails. */
    BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity,
            long unpersistedBytesBound, boolean freshLifecycle) throws IOException {
        super(fc, readCapacity);
        this.freshLifecycle = freshLifecycle;
        this.writeCapacity = writeCapacity;
        ByteBuf allocatedWrite = null;
        try {
            this.position = fc.position();
            this.writeBufferStartPosition = new AtomicLong(position);
            allocatedWrite = allocator.directBuffer(writeCapacity);
            this.writeBuffer = allocatedWrite;
            this.unpersistedBytes = new AtomicLong(0);
            this.unpersistedBytesBound = unpersistedBytesBound;
            this.doRegularFlushes = unpersistedBytesBound > 0;
        } catch (IOException | RuntimeException | Error failure) {
            if (freshLifecycle) {
                try {
                    ReferenceCountUtil.release(allocatedWrite);
                } catch (RuntimeException | Error releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
                try {
                    readBuffer.release();
                } catch (RuntimeException | Error releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (freshLifecycle) {
            closeForFresh();
            return;
        }
        if (closed) {
            return;
        }
        ReferenceCountUtil.release(writeBuffer);
        fileChannel.close();
        closed = true;
    }

    /** All readers/writers must have actually returned before the fresh close worker calls this. */
    synchronized void closeForFresh() throws IOException {
        if (!freshLifecycle) {
            throw new IllegalStateException("Strict close requires a fresh buffered channel");
        }
        if (freshCloseStarted && !freshCloseReturned) {
            throw new IllegalStateException("Fresh native close is still executing");
        }
        if (!freshCloseStarted) {
            freshCloseStarted = true;
            try {
                fileChannel.close();
            } catch (IOException | RuntimeException | Error failure) {
                freshCloseFailure = failure;
            } finally {
                freshCloseReturned = true;
            }
        }
        // Buffer ownership and native-close success are separate. Never repeat an attempted release.
        if (!freshWriteReleased) {
            freshWriteReleased = true;
            try {
                writeBuffer.release();
            } catch (RuntimeException | Error failure) {
                recordFreshCloseFailure(failure);
            }
        }
        if (!freshReadReleased) {
            freshReadReleased = true;
            try {
                readBuffer.release();
            } catch (RuntimeException | Error failure) {
                recordFreshCloseFailure(failure);
            }
        }
        closed = true;
        if (freshCloseFailure instanceof IOException) {
            throw (IOException) freshCloseFailure;
        } else if (freshCloseFailure instanceof RuntimeException) {
            throw (RuntimeException) freshCloseFailure;
        } else if (freshCloseFailure instanceof Error) {
            throw (Error) freshCloseFailure;
        }
    }

    private void recordFreshCloseFailure(Throwable failure) {
        if (freshCloseFailure == null) {
            freshCloseFailure = failure;
        } else if (freshCloseFailure != failure) {
            freshCloseFailure.addSuppressed(failure);
        }
    }

    /**
     * Write all the data in src to the {@link FileChannel}. Note that this function can
     * buffer or re-order writes based on the implementation. These writes will be flushed
     * to the disk only when flush() is invoked.
     *
     * @param src The source ByteBuffer which contains the data to be written.
     * @throws IOException if a write operation fails.
     */
    public void write(ByteBuf src) throws IOException {
        boolean shouldForceWrite = false;
        synchronized (this) {
            int copied = copyIntoWriteBuffer(src);
            shouldForceWrite = updatePositionAndFlushIfNeeded(copied);
        }
        if (shouldForceWrite) {
            forceWrite(false);
        }
    }

    /**
     * Write all the data in src1 and src2, in order, to the {@link FileChannel}, taking the
     * lock only once. This avoids the overhead of two lock acquisitions when writing an
     * entry preceded by its length prefix.
     *
     * @param src1 The first source buffer which contains the data to be written.
     * @param src2 The second source buffer which contains the data to be written.
     * @throws IOException if a write operation fails.
     */
    public void write(ByteBuf src1, ByteBuf src2) throws IOException {
        boolean shouldForceWrite = false;
        synchronized (this) {
            int copied = copyIntoWriteBuffer(src1);
            copied += copyIntoWriteBuffer(src2);
            shouldForceWrite = updatePositionAndFlushIfNeeded(copied);
        }
        if (shouldForceWrite) {
            forceWrite(false);
        }
    }

    // must be called while holding the lock on this instance
    private int copyIntoWriteBuffer(ByteBuf src) throws IOException {
        if (freshLifecycle && (freshCloseStarted || freshPendingFlush != null)) {
            throw new IOException("Fresh channel is closing or requires the pending flush to finish first");
        }
        int copied = 0;
        int len = src.readableBytes();
        while (copied < len) {
            int bytesToCopy = Math.min(src.readableBytes() - copied, writeBuffer.writableBytes());
            writeBuffer.writeBytes(src, src.readerIndex() + copied, bytesToCopy);
            copied += bytesToCopy;

            // if we have run out of buffer space, we should flush to the
            // file
            if (!writeBuffer.isWritable()) {
                flush();
            }
        }
        return copied;
    }

    // must be called while holding the lock on this instance
    private boolean updatePositionAndFlushIfNeeded(int copied) throws IOException {
        position += copied;
        if (doRegularFlushes) {
            unpersistedBytes.addAndGet(copied);
            if (unpersistedBytes.get() >= unpersistedBytesBound) {
                flush();
                return true;
            }
        }
        return false;
    }

    /**
     * Get the position where the next write operation will begin writing from.
     * @return
     */
    public long position() {
        return position;
    }

    /**
     * Get the position of the file channel's write pointer.
     * @return
     */
    public long getFileChannelPosition() {
        return writeBufferStartPosition.get();
    }

    /**
     * calls both flush and forceWrite methods.
     *
     * @param forceMetadata
     *            - If true then this method is required to force changes to
     *            both the file's content and metadata to be written to storage;
     *            otherwise, it need only force content changes to be written
     * @throws IOException
     */
    public void flushAndForceWrite(boolean forceMetadata) throws IOException {
        flush();
        forceWrite(forceMetadata);
    }

    /**
     * calls both flush and forceWrite methods if regular flush is enabled.
     *
     * @param forceMetadata
     *            - If true then this method is required to force changes to
     *            both the file's content and metadata to be written to storage;
     *            otherwise, it need only force content changes to be written
     * @throws IOException
     */
    public void flushAndForceWriteIfRegularFlush(boolean forceMetadata) throws IOException {
        if (doRegularFlushes) {
            flushAndForceWrite(forceMetadata);
        }
    }

    /**
     * Write any data in the buffer to the file and advance the writeBufferPosition.
     * Callers are expected to synchronize appropriately
     *
     * @throws IOException if the write fails.
     */
    public synchronized void flush() throws IOException {
        if (freshLifecycle) {
            flushForFresh();
            return;
        }
        ByteBuffer toWrite = writeBuffer.internalNioBuffer(0, writeBuffer.writerIndex());
        do {
            fileChannel.write(toWrite);
        } while (toWrite.hasRemaining());
        writeBuffer.clear();
        writeBufferStartPosition.set(fileChannel.position());
    }

    private void flushForFresh() throws IOException {
        if (freshCloseStarted) {
            throw new IOException("Fresh channel close has already started");
        }
        if (freshPendingFlush == null) {
            freshPendingFlushStart = writeBufferStartPosition.get();
            freshPendingFlush = writeBuffer.nioBuffer(0, writeBuffer.writerIndex()).asReadOnlyBuffer();
        }
        try {
            while (freshPendingFlush.hasRemaining()) {
                int written = fileChannel.write(freshPendingFlush,
                        Math.addExact(freshPendingFlushStart, freshPendingFlush.position()));
                if (written <= 0) {
                    throw new IOException("Fresh positional flush made no progress");
                }
            }
            long end = Math.addExact(freshPendingFlushStart, freshPendingFlush.limit());
            fileChannel.position(end);
            writeBufferStartPosition.set(end);
            writeBuffer.clear();
            freshPendingFlush = null;
        } catch (IOException | RuntimeException | Error failure) {
            // A failed syscall may have written bytes without reporting progress. Overwrite the
            // same frozen range on retry; never append that range at an advanced relative position.
            freshPendingFlush.rewind();
            throw failure;
        }
    }

    /**
     * force a sync operation so that data is persisted to the disk.
     * @param forceMetadata
     * @return
     * @throws IOException
     */
    public long forceWrite(boolean forceMetadata) throws IOException {
        // This is the point up to which we had flushed to the file system page cache
        // before issuing this force write hence is guaranteed to be made durable by
        // the force write, any flush that happens after this may or may
        // not be flushed
        long positionForceWrite = writeBufferStartPosition.get();
        /*
         * since forceWrite method is not called in synchronized block, to make
         * sure we are not undercounting unpersistedBytes, setting
         * unpersistedBytes to the current number of bytes in writeBuffer.
         *
         * since we are calling fileChannel.force, bytes which are written to
         * filechannel (system filecache) will be persisted to the disk. So we
         * dont need to consider those bytes for setting value to
         * unpersistedBytes.
         *
         * In this method fileChannel.force is not called in synchronized block, so
         * we are doing best efforts to not overcount or undercount unpersistedBytes.
         * Hence setting writeBuffer.readableBytes() to unpersistedBytes.
         *
         */
        if (unpersistedBytesBound > 0) {
            synchronized (this) {
                unpersistedBytes.set(writeBuffer.readableBytes());
            }
        }

        fileChannel.force(forceMetadata);
        return positionForceWrite;
    }

    @Override
    public synchronized int read(ByteBuf dest, long pos, int length) throws IOException {
        if (dest.writableBytes() < length) {
            throw new IllegalArgumentException("dest buffer remaining capacity is not enough"
                    + "(must be at least as \"length\"=" + length + ")");
        }

        long prevPos = pos;
        while (length > 0) {
            // check if it is in the write buffer
            if (writeBuffer != null && writeBufferStartPosition.get() <= pos) {
                int positionInBuffer = (int) (pos - writeBufferStartPosition.get());
                int bytesToCopy = Math.min(writeBuffer.writerIndex() - positionInBuffer, dest.writableBytes());

                if (bytesToCopy == 0) {
                    throw new IOException("Read past EOF");
                }

                dest.writeBytes(writeBuffer, positionInBuffer, bytesToCopy);
                pos += bytesToCopy;
                length -= bytesToCopy;
            } else if (writeBuffer == null && writeBufferStartPosition.get() <= pos) {
                // here we reach the end
                break;
                // first check if there is anything we can grab from the readBuffer
            } else if (readBufferStartPosition <= pos && pos < readBufferStartPosition + readBuffer.writerIndex()) {
                int positionInBuffer = (int) (pos - readBufferStartPosition);
                int bytesToCopy = Math.min(readBuffer.writerIndex() - positionInBuffer, dest.writableBytes());
                dest.writeBytes(readBuffer, positionInBuffer, bytesToCopy);
                pos += bytesToCopy;
                length -= bytesToCopy;
                // let's read it
            } else {
                readBufferStartPosition = pos;

                int readBytes = fileChannel.read(readBuffer.internalNioBuffer(0, readCapacity),
                        readBufferStartPosition);
                if (readBytes <= 0) {
                    throw new IOException("Reading from filechannel returned a non-positive value. Short read.");
                }
                readBuffer.writerIndex(readBytes);
            }
        }
        return (int) (pos - prevPos);
    }

    @Override
    public synchronized void clear() {
        if (freshLifecycle && (freshCloseStarted || freshPendingFlush != null)) {
            throw new IllegalStateException("Cannot clear a closing channel or a pending fresh flush");
        }
        super.clear();
        writeBuffer.clear();
    }

    public synchronized int getNumOfBytesInWriteBuffer() {
        return writeBuffer.readableBytes();
    }

    long getUnpersistedBytes() {
        return unpersistedBytes.get();
    }
}

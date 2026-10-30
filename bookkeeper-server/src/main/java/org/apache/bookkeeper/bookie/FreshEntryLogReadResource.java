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

import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/** One actual fresh entry-log read fd; weak heap-buffer aliases do not own its native-close result. */
final class FreshEntryLogReadResource {
    private final long logId;
    private FileChannel channel;
    private Thread openingThread = Thread.currentThread();
    private boolean openReturned;
    private volatile boolean closing;
    private boolean nativeCloseStarted;
    private boolean nativeCloseReturned;
    private boolean closed;
    private Throwable closeFailure;
    // Values must never point back to the weak key. Unreachable wrappers contain only unpooled heap buffers.
    private final Map<Reader, Boolean> readers = new WeakHashMap<>();

    FreshEntryLogReadResource(long logId) {
        this.logId = logId;
    }

    long logId() {
        return logId;
    }

    synchronized void beginClosing() {
        closing = true;
    }

    void open(File file) throws IOException {
        checkReadable();
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    synchronized void completeOpen() {
        openReturned = true;
        openingThread = null;
        notifyAll();
    }

    Reader newReader(int capacity, boolean sealed) throws IOException {
        checkReadable();
        Reader reader = new Reader(this, capacity, sealed);
        try {
            synchronized (this) {
                checkReadable();
                readers.put(reader, Boolean.TRUE);
            }
            return reader;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                reader.releaseOwnedBuffer();
            } catch (IOException | RuntimeException | Error cleanupFailure) {
                DefaultEntryLogger.mergeFreshFailure(failure, cleanupFailure);
                recordCloseFailure(cleanupFailure);
            }
            throw failure;
        }
    }

    void discardReader(Reader reader) throws IOException {
        try {
            reader.releaseOwnedBuffer();
        } catch (IOException | RuntimeException | Error failure) {
            recordCloseFailure(failure);
            throw failure;
        } finally {
            synchronized (this) {
                readers.remove(reader);
            }
        }
    }

    void checkReadable() throws IOException {
        if (closing) {
            synchronized (this) {
                throw new IOException("Fresh entry-log read channel is closing: " + logId, closeFailure);
            }
        }
    }

    synchronized boolean closedSuccessfully() {
        return closed;
    }

    private synchronized void recordCloseFailure(Throwable failure) {
        closing = true;
        closeFailure = DefaultEntryLogger.mergeFreshFailure(closeFailure, failure);
    }

    void close() throws IOException, InterruptedException {
        List<Reader> alive;
        synchronized (this) {
            closing = true;
            try {
                while (!openReturned) {
                    if (Thread.currentThread() == openingThread) {
                        throw new IllegalStateException("The opening thread cannot await its own actual open");
                    }
                    wait();
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw failure;
            }
            alive = new ArrayList<>(readers.keySet());
        }
        // Never hold the resource monitor while waiting for a reader monitor. No new read can
        // start after CLOSING, including a sealed-size or read-buffer cache hit.
        for (Reader reader : alive) {
            reader.awaitReadCompletion();
        }
        synchronized (this) {
            if (nativeCloseStarted && !nativeCloseReturned) {
                throw new IllegalStateException("Fresh native read close has not actually returned");
            }
            if (!nativeCloseStarted) {
                nativeCloseStarted = true;
                try {
                    if (channel != null) {
                        if (!channel.isOpen()) {
                            throw new IOException("Fresh read fd closed without an observed native-close result");
                        }
                        channel.close();
                    }
                } catch (IOException | RuntimeException | Error failure) {
                    closeFailure = DefaultEntryLogger.mergeFreshFailure(failure, closeFailure);
                } finally {
                    nativeCloseReturned = true;
                }
            }
        }
        for (Reader reader : alive) {
            try {
                reader.releaseOwnedBuffer();
            } catch (IOException | RuntimeException | Error failure) {
                synchronized (this) {
                    closeFailure = DefaultEntryLogger.mergeFreshFailure(closeFailure, failure);
                }
            }
        }
        synchronized (this) {
            if (closeFailure != null) {
                DefaultEntryLogger.throwFreshFailure(closeFailure);
            }
            closed = true;
        }
    }

    static final class Reader extends BufferedReadChannel {
        final FreshEntryLogReadResource resource;
        private boolean releaseAttempted;
        private Throwable releaseFailure;

        private Reader(FreshEntryLogReadResource resource, int capacity, boolean sealed) {
            super(resource.channel, capacity, sealed);
            this.resource = resource;
        }

        @Override
        public synchronized long size() throws IOException {
            resource.checkReadable();
            return super.size();
        }

        @Override
        public synchronized int read(ByteBuf destination, long position, int length) throws IOException {
            resource.checkReadable();
            return super.read(destination, position, length);
        }

        private synchronized void awaitReadCompletion() { }

        private synchronized void releaseOwnedBuffer() throws IOException {
            if (!releaseAttempted) {
                releaseAttempted = true;
                try {
                    readBuffer.release();
                } catch (RuntimeException | Error failure) {
                    releaseFailure = failure;
                }
            }
            if (releaseFailure != null) {
                DefaultEntryLogger.throwFreshFailure(releaseFailure);
            }
        }
    }
}

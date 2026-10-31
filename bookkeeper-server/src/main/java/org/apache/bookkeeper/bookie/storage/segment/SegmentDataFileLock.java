/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Keeps one resource owner per in-process lock scope; failed cleanup retains that owner and its native handles. */
final class SegmentDataFileLock implements AutoCloseable {
    private static final Map<Path, SegmentDataFileLock> ACTIVE = new HashMap<>();

    record ReleaseResult(boolean acquired, boolean started, boolean returned, Throwable failure) { }

    record DrainResult(boolean drained, Throwable acquisitionFailure, ReleaseResult lock, ReleaseResult channel) { }

    private final Path requested;
    private final boolean create;
    private final boolean control;
    private Path scope;
    private FileChannel channel;
    private FileLock lock;
    private boolean attempted;
    private boolean registered;
    private boolean closing;
    private Thread initializingThread;
    private Thread closingThread;
    private Throwable acquisitionFailure;
    private boolean releaseStarted;
    private boolean releaseReturned;
    private Throwable releaseFailure;
    private boolean channelCloseStarted;
    private boolean channelCloseReturned;
    private Throwable channelCloseFailure;
    private boolean aggregated;
    private Throwable closeFailure;

    private SegmentDataFileLock(Path requested, boolean create, boolean control) {
        this.requested = requested.toAbsolutePath();
        this.create = create;
        this.control = control;
    }

    static SegmentDataFileLock create(Path directory) throws IOException {
        return acquire(directory.resolve("isolated.lock"), true, false);
    }

    static SegmentDataFileLock open(Path directory) throws IOException {
        return acquire(directory.resolve("isolated.lock"), false, false);
    }

    static SegmentDataFileLock openControl(Path path) throws IOException {
        return acquire(path, false, true);
    }

    /** Inspection must not create a missing lock or open a second channel for an active in-process owner. */
    static SegmentDataFileLock openExistingControl(Path path) throws IOException {
        return acquire(path, false, false);
    }

    /** Pure owner for a caller to register before any path lookup, file open or lock acquisition. */
    static SegmentDataFileLock existingControlOwner(Path path) {
        return new SegmentDataFileLock(path, false, false);
    }

    /** Pure owner for the first private lock-file creation in an explicitly commissioned common root. */
    static SegmentDataFileLock creatingControlOwner(Path path) {
        return new SegmentDataFileLock(path, true, true);
    }

    private static SegmentDataFileLock acquire(Path path, boolean create, boolean control) throws IOException {
        SegmentDataFileLock owner = new SegmentDataFileLock(path, create, control);
        try {
            owner.initialize();
            return owner;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                owner.close();
            } catch (Throwable cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /** Acquisition is one-use and serialized with close; failures leave this same owner available for cleanup. */
    synchronized void initialize() throws IOException {
        if (attempted || closing) {
            throw new IOException("File lock acquisition is one-use and cannot follow close");
        }
        attempted = true;
        initializingThread = Thread.currentThread();
        try {
            scope = requested.getParent().toRealPath().resolve(requested.getFileName());
            synchronized (ACTIVE) {
                if (ACTIVE.putIfAbsent(scope, this) != null) {
                    throw new IOException("Isolated file scope already has a writer or unresolved resource owner");
                }
                registered = true;
            }
            if (create && control) {
                channel = FileChannel.open(scope, Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            } else {
                channel = control
                        ? FileChannel.open(scope, StandardOpenOption.CREATE, StandardOpenOption.READ,
                                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
                        : create ? FileChannel.open(scope, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                        : FileChannel.open(scope, StandardOpenOption.READ, StandardOpenOption.WRITE,
                                LinkOption.NOFOLLOW_LINKS);
            }
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Isolated file scope already has a writer", overlapping);
            }
            if (lock == null) {
                throw new IOException("Isolated file scope already has a writer");
            }
        } catch (IOException | RuntimeException | Error failure) {
            acquisitionFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    synchronized FileChannel channel() {
        return channel;
    }

    synchronized FileLock lock() {
        return lock;
    }

    /** Borrowers keep this original owner until all their actual users have terminated. */
    synchronized void requireHeld(Path expected) throws IOException {
        if (closing || !registered || scope == null || !scope.equals(expected)
                || lock == null || !lock.isValid() || channel == null || !channel.isOpen()) {
            throw new IOException("Original supervisor guard is not held for the expected scope");
        }
    }

    synchronized DrainResult drainResult() {
        boolean drained = closing && initializingThread == null && closingThread == null
                && (lock == null || releaseReturned && releaseFailure == null)
                && (channel == null || channelCloseReturned && channelCloseFailure == null);
        return new DrainResult(drained, acquisitionFailure,
                new ReleaseResult(lock != null, releaseStarted, releaseReturned, releaseFailure),
                new ReleaseResult(channel != null, channelCloseStarted, channelCloseReturned, channelCloseFailure));
    }

    @Override
    public synchronized void close() throws IOException {
        if (initializingThread == Thread.currentThread() || closingThread == Thread.currentThread()) {
            throw new IllegalStateException("File lock activity cannot close itself");
        }
        closing = true;
        closingThread = Thread.currentThread();
        try {
            if (lock != null && !releaseStarted) {
                releaseStarted = true;
                try {
                    if (!lock.isValid()) {
                        throw new IOException("File lock became invalid without an observed release result: " + scope);
                    }
                    lock.release();
                } catch (Throwable failure) {
                    releaseFailure = failure;
                } finally {
                    releaseReturned = true;
                }
            }
            if (channel != null && !channelCloseStarted) {
                channelCloseStarted = true;
                try {
                    if (!channel.isOpen()) {
                        throw new IOException("File channel closed without an observed native result: " + scope);
                    }
                    channel.close();
                } catch (Throwable failure) {
                    channelCloseFailure = failure;
                } finally {
                    channelCloseReturned = true;
                }
            }
            if (!aggregated) {
                aggregated = true;
                closeFailure = releaseFailure == null ? channelCloseFailure : releaseFailure;
                if (releaseFailure != null && channelCloseFailure != null && releaseFailure != channelCloseFailure) {
                    closeFailure.addSuppressed(channelCloseFailure);
                }
            }
            if (closeFailure == null && registered) {
                synchronized (ACTIVE) {
                    ACTIVE.remove(scope, this);
                    registered = false;
                }
            }
            rethrow(closeFailure);
        } finally {
            closingThread = null;
        }
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException io) {
            throw io;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IOException("Unexpected file lock cleanup failure", failure);
        }
    }
}

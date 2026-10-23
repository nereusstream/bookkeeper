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
import java.util.HashSet;
import java.util.Set;

/** Keeps one channel per in-process lock scope; closing another channel can drop POSIX record locks. */
final class SegmentDataFileLock implements AutoCloseable {
    private static final Set<Path> ACTIVE = new HashSet<>();

    private final Path scope;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean closed;

    private SegmentDataFileLock(Path scope, FileChannel channel, FileLock lock) {
        this.scope = scope;
        this.channel = channel;
        this.lock = lock;
    }

    static SegmentDataFileLock create(Path directory) throws IOException {
        return acquire(directory, true);
    }

    static SegmentDataFileLock open(Path directory) throws IOException {
        return acquire(directory, false);
    }

    private static SegmentDataFileLock acquire(Path directory, boolean create) throws IOException {
        Path scope = directory.toRealPath().resolve("isolated.lock");
        return acquireFile(scope, create, false);
    }

    static SegmentDataFileLock openControl(Path path) throws IOException {
        return acquireFile(path.getParent().toRealPath().resolve(path.getFileName()), false, true);
    }

    /** Inspection must not create a missing lock or open a second channel for an active in-process owner. */
    static SegmentDataFileLock openExistingControl(Path path) throws IOException {
        return acquireFile(path.getParent().toRealPath().resolve(path.getFileName()), false, false);
    }

    private static SegmentDataFileLock acquireFile(Path scope, boolean create, boolean control) throws IOException {
        synchronized (ACTIVE) {
            if (!ACTIVE.add(scope)) {
                throw new IOException("Isolated file scope already has a writer");
            }
        }
        FileChannel channel = null;
        try {
            channel = control
                    ? FileChannel.open(scope, StandardOpenOption.CREATE, StandardOpenOption.READ,
                            StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
                    : create ? FileChannel.open(scope, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                    : FileChannel.open(scope, StandardOpenOption.READ, StandardOpenOption.WRITE,
                            LinkOption.NOFOLLOW_LINKS);
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Isolated file scope already has a writer", overlapping);
            }
            if (lock == null) {
                throw new IOException("Isolated file scope already has a writer");
            }
            return new SegmentDataFileLock(scope, channel, lock);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            synchronized (ACTIVE) {
                ACTIVE.remove(scope);
            }
            throw failure;
        }
    }

    FileChannel channel() {
        return channel;
    }

    FileLock lock() {
        return lock;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        IOException first = null;
        boolean released = false;
        try {
            lock.release();
            released = true;
        } catch (IOException failure) {
            first = failure;
        }
        try {
            channel.close();
            released = true;
        } catch (IOException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        if (released) {
            closed = true;
            synchronized (ACTIVE) {
                ACTIVE.remove(scope);
            }
        }
        if (first != null) {
            throw first;
        }
    }
}

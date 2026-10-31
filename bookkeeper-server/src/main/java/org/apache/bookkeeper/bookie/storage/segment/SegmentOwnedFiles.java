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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Native file lifetime for the explicit control owner. Callers stop actual file users before draining. */
final class SegmentOwnedFiles {
    record FileResult(Path path, boolean acquired, boolean closeStarted, boolean closeReturned, Throwable failure) { }

    record Result(boolean drained, int opening, List<FileResult> retained, Throwable closeFailure) { }

    private final Object gate = new Object();
    private final Set<Entry> entries = new LinkedHashSet<>();
    private final Map<Thread, Integer> openingThreads = new IdentityHashMap<>();
    private final List<Path> protectedLocks;
    private int opening;
    private volatile boolean closing;
    private volatile Throwable closeFailure;

    SegmentOwnedFiles() {
        this(List.of());
    }

    /** Closing another descriptor for a POSIX lock's inode can release that process's exclusion. */
    SegmentOwnedFiles(List<Path> protectedLocks) {
        this.protectedLocks = List.copyOf(protectedLocks);
    }

    private void requireDistinctFromLocks(Path path) throws IOException {
        for (Path lock : protectedLocks) {
            if (Files.isSameFile(path, lock)) {
                throw new IOException("Checked file aliases a protected lock: " + path);
            }
        }
    }

    static FileChannel open(SegmentOwnedFiles owner, Path path, OpenOption... options) throws IOException {
        if (owner == null) {
            return FileChannel.open(path, options);
        }
        Entry entry = owner.begin(path);
        try {
            owner.requireDistinctFromLocks(path);
            entry.resource = FileChannel.open(path, options);
            entry.wrapper = owner.new Channel(entry);
            return entry.wrapper;
        } finally {
            owner.end(entry);
        }
    }

    static FileChannel open(SegmentOwnedFiles owner, Path path, Set<? extends OpenOption> options,
            FileAttribute<?>... attributes) throws IOException {
        if (owner == null) {
            return FileChannel.open(path, options, attributes);
        }
        Entry entry = owner.begin(path);
        try {
            owner.requireDistinctFromLocks(path);
            entry.resource = FileChannel.open(path, options, attributes);
            entry.wrapper = owner.new Channel(entry);
            return entry.wrapper;
        } finally {
            owner.end(entry);
        }
    }

    static void syncDirectory(SegmentOwnedFiles owner, Path directory) throws IOException {
        try (FileChannel channel = open(owner, directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private Entry begin(Path path) throws IOException {
        synchronized (gate) {
            requireUsable();
            Entry entry = new Entry(path);
            Thread thread = Thread.currentThread();
            Integer depth = openingThreads.get(thread);
            try {
                openingThreads.put(thread, depth == null ? 1 : depth + 1);
                entries.add(entry);
                opening++;
                return entry;
            } catch (RuntimeException | Error failure) {
                entries.remove(entry);
                if (depth == null) {
                    openingThreads.remove(thread);
                } else {
                    openingThreads.put(thread, depth);
                }
                throw failure;
            }
        }
    }

    private void end(Entry entry) {
        synchronized (gate) {
            if (entry.resource == null) {
                entries.remove(entry);
            }
            opening--;
            Thread thread = Thread.currentThread();
            int depth = openingThreads.get(thread);
            if (depth == 1) {
                openingThreads.remove(thread);
            } else {
                openingThreads.put(thread, depth - 1);
            }
            gate.notifyAll();
        }
    }

    void requireHealthy() throws IOException {
        Throwable failure = closeFailure;
        if (failure != null) {
            rethrow(failure);
        }
    }

    boolean healthy() {
        return !closing && closeFailure == null;
    }

    private void requireUsable() throws IOException {
        requireHealthy();
        if (closing) {
            throw new IOException("Control file owner is closing");
        }
    }

    private void retired(Entry entry) {
        synchronized (gate) {
            entries.remove(entry);
        }
    }

    private void failed(Throwable failure) {
        synchronized (gate) {
            if (closeFailure == null) {
                closeFailure = failure;
            } else if (closeFailure != failure) {
                for (Throwable retained : closeFailure.getSuppressed()) {
                    if (retained == failure) {
                        return;
                    }
                }
                closeFailure.addSuppressed(failure);
            }
        }
    }

    /** Does not cancel an opening thread or retry an unknown close. Interrupting this waiter preserves progress. */
    Result drain() throws InterruptedException {
        List<Entry> pending;
        synchronized (gate) {
            if (openingThreads.containsKey(Thread.currentThread())) {
                throw new IllegalStateException("Control file open cannot await itself");
            }
            for (Entry entry : entries) {
                if (entry.closingThread == Thread.currentThread()) {
                    throw new IllegalStateException("Control file close cannot await itself");
                }
            }
            closing = true;
            while (opening != 0) {
                gate.wait();
            }
            pending = new ArrayList<>(entries);
        }
        for (int index = pending.size() - 1; index >= 0; index--) {
            Entry entry = pending.get(index);
            try {
                if (entry.wrapper == null) {
                    entry.close();
                } else {
                    entry.wrapper.close();
                    // A JDK logical close without our first native call is never a successful release receipt.
                    if (!entry.closeStarted) {
                        entry.missingClose();
                    }
                }
            } catch (Throwable ignored) {
                // Each entry records its actual first result before reporting it. Continue independent cleanup.
            }
        }
        return result();
    }

    Result result() {
        synchronized (gate) {
            List<FileResult> retained = new ArrayList<>(entries.size());
            for (Entry entry : entries) {
                retained.add(new FileResult(entry.path, entry.resource != null, entry.closeStarted,
                        entry.closeReturned, entry.failure));
            }
            return new Result(closing && opening == 0 && entries.isEmpty() && closeFailure == null,
                    opening, List.copyOf(retained), closeFailure);
        }
    }

    private final class Entry {
        final Path path;
        volatile FileChannel resource;
        volatile Channel wrapper;
        volatile boolean closeStarted;
        volatile boolean closeReturned;
        volatile Throwable failure;
        volatile Thread closingThread;

        Entry(Path path) {
            this.path = path;
        }

        synchronized void missingClose() {
            if (!closeStarted) {
                closeStarted = true;
                closeReturned = true;
                failure = new IOException("Control channel logically closed without its native receipt: " + path);
                failed(failure);
            }
        }

        synchronized void close() throws IOException {
            if (closeStarted) {
                if (!closeReturned) {
                    throw new IllegalStateException("Control file native close is still executing");
                }
                rethrow(failure);
                return;
            }
            closeStarted = true;
            closingThread = Thread.currentThread();
            try {
                if (!resource.isOpen()) {
                    throw new IOException("Control file closed without an observed native result: " + path);
                }
                resource.close();
                resource = null;
                retired(this);
            } catch (Throwable nativeFailure) {
                failure = nativeFailure;
                failed(nativeFailure);
            } finally {
                closeReturned = true;
                closingThread = null;
            }
            rethrow(failure);
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
            throw new IOException("Unexpected control native release failure", failure);
        }
    }

    private final class Channel extends FileChannel {
        private final Entry entry;

        Channel(Entry entry) {
            this.entry = entry;
        }

        private FileChannel raw() throws IOException {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            requireUsable();
            FileChannel channel = entry.resource;
            if (channel == null) {
                throw new ClosedChannelException();
            }
            return channel;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            entry.close();
        }

        @Override
        public int read(ByteBuffer buffer) throws IOException {
            return raw().read(buffer);
        }

        @Override
        public long read(ByteBuffer[] buffers, int offset, int length) throws IOException {
            return raw().read(buffers, offset, length);
        }

        @Override
        public int write(ByteBuffer buffer) throws IOException {
            return raw().write(buffer);
        }

        @Override
        public long write(ByteBuffer[] buffers, int offset, int length) throws IOException {
            return raw().write(buffers, offset, length);
        }

        @Override
        public long position() throws IOException {
            return raw().position();
        }

        @Override
        public FileChannel position(long position) throws IOException {
            raw().position(position);
            return this;
        }

        @Override
        public long size() throws IOException {
            return raw().size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            raw().truncate(size);
            return this;
        }

        @Override
        public void force(boolean metadata) throws IOException {
            raw().force(metadata);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return raw().transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel source, long position, long count) throws IOException {
            return raw().transferFrom(source, position, count);
        }

        @Override
        public int read(ByteBuffer buffer, long position) throws IOException {
            return raw().read(buffer, position);
        }

        @Override
        public int write(ByteBuffer buffer, long position) throws IOException {
            return raw().write(buffer, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return raw().map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return raw().lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return raw().tryLock(position, size, shared);
        }
    }
}

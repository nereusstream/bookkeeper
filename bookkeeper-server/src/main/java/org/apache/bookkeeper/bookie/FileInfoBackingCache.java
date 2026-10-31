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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.CustomLog;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;

@CustomLog
class FileInfoBackingCache {
    static final int DEAD_REF = -0xdead;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    final ConcurrentLongHashMap<CachedFileInfo> fileInfos =
            ConcurrentLongHashMap.<CachedFileInfo>newBuilder().build();
    final FileLoader fileLoader;
    final int fileInfoVersionToWrite;
    private final boolean freshLifecycle;
    private final boolean historical;
    private final Object freshCloseLock = new Object();
    private final Condition freshProgress = lock.writeLock().newCondition();
    private boolean freshClosing;
    private int freshLoads;
    private int freshRetirements;
    private Throwable freshOperationFailure;
    private final Map<Thread, Integer> freshRetirementThreads = new IdentityHashMap<>();

    record FreshDrainResult(boolean drained, Throwable operationFailure,
                            Throwable flushFailure, Throwable closeFailure) { }

    FileInfoBackingCache(FileLoader fileLoader, int fileInfoVersionToWrite) {
        this(fileLoader, fileInfoVersionToWrite, false);
    }

    static FileInfoBackingCache forClassicHistory(FileLoader loader, int version) {
        return new FileInfoBackingCache(loader, version, true, true);
    }

    FileInfoBackingCache(FileLoader fileLoader, int fileInfoVersionToWrite, boolean freshLifecycle) {
        this(fileLoader, fileInfoVersionToWrite, freshLifecycle, false);
    }

    private FileInfoBackingCache(FileLoader fileLoader, int fileInfoVersionToWrite, boolean freshLifecycle,
            boolean historical) {
        this.historical = historical;
        this.fileLoader = fileLoader;
        this.fileInfoVersionToWrite = fileInfoVersionToWrite;
        this.freshLifecycle = freshLifecycle;
    }

    /**
     * This method should be under `lock` of FileInfoBackingCache.
     */
    private static CachedFileInfo tryRetainFileInfo(CachedFileInfo fi) throws IOException {
        boolean retained = fi.tryRetain();
        if (!retained) {
            throw new IOException("FileInfo " + fi + " is already marked dead");
        } else if (fi.isDeleted()) {
            throw new Bookie.NoLedgerException(fi.ledgerId);
        }
        return fi;
    }

    CachedFileInfo loadFileInfo(long ledgerId, byte[] masterKey) throws IOException {
        if (freshLifecycle) {
            return loadForFresh(ledgerId, masterKey);
        }
        lock.readLock().lock();
        try {
            CachedFileInfo fi = fileInfos.get(ledgerId);
            if (fi != null) {
                // tryRetain only fails if #markDead() has been called
                // on fi. This is only called from within the write lock,
                // and if it is called (and succeeds) the fi will have been
                // removed from fileInfos at the same time, so we should not
                // have been able to get a reference to it here.
                // The caller of loadFileInfo owns the reference, and is
                // responsible for calling the corresponding #release().
                return tryRetainFileInfo(fi);
            }
        } finally {
            lock.readLock().unlock();
        }

        File backingFile = fileLoader.load(ledgerId, masterKey != null);
        CachedFileInfo newFi = new CachedFileInfo(ledgerId, backingFile, masterKey);

        // else FileInfo not found, create it under write lock
        lock.writeLock().lock();
        try {
            CachedFileInfo fi = fileInfos.get(ledgerId);
            if (fi != null) {
                // someone is already putting a fileinfo here, so use the existing one and recycle the new one
                newFi.recycle();
            } else {
                fileInfos.put(ledgerId, newFi);
                fi = newFi;
            }

            // see comment above for why we assert
            return tryRetainFileInfo(fi);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void releaseFileInfo(long ledgerId, CachedFileInfo fileInfo) {
        if (freshLifecycle) {
            boolean retire;
            boolean force;
            lock.writeLock().lock();
            try {
                retire = fileInfo.markDead();
                force = !freshClosing;
                if (retire) {
                    Thread thread = Thread.currentThread();
                    freshRetirementThreads.put(thread, freshRetirementThreads.getOrDefault(thread, 0) + 1);
                    freshRetirements++;
                }
                freshProgress.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
            if (retire) {
                // Neither native I/O nor watcher callbacks execute under the backing-cache lock.
                retireForFresh(ledgerId, fileInfo, force);
            }
            return;
        }
        lock.writeLock().lock();
        try {
            if (fileInfo.markDead()) {
                fileInfo.close(true);
                fileInfos.remove(ledgerId, fileInfo);
            }
        } catch (IOException ioe) {
            log.error()
                    .exception(ioe)
                    .attr("fileInfo", fileInfo)
                    .attr("ledgerId", ledgerId)
                .log("Error evicting file info from backing cache");
        } finally {
            lock.writeLock().unlock();
        }
    }

    void closeAllWithoutFlushing() throws IOException {
        if (freshLifecycle) {
            throw new IllegalStateException("Use the explicit fresh cache drain result");
        }
        try {
            fileInfos.forEach((key, fileInfo) -> {
                try {
                    fileInfo.close(false);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException uioe) {
            throw uioe.getCause();
        }
    }

    void requireFreshAdmission() throws IOException {
        if (!freshLifecycle) {
            return;
        }
        lock.readLock().lock();
        try {
            if (freshClosing || freshOperationFailure != null) {
                throw new IOException("Fresh index backing cache is closing or failed", freshOperationFailure);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private CachedFileInfo loadForFresh(long ledgerId, byte[] masterKey) throws IOException {
        lock.writeLock().lock();
        try {
            requireFreshAdmission();
            CachedFileInfo existing = fileInfos.get(ledgerId);
            if (existing != null) {
                return retainForFresh(existing);
            }
            freshLoads++;
        } finally {
            lock.writeLock().unlock();
        }
        CachedFileInfo candidate = null;
        boolean published = false;
        try {
            File backingFile = fileLoader.load(ledgerId, masterKey != null);
            candidate = new CachedFileInfo(ledgerId, backingFile, masterKey);
            lock.writeLock().lock();
            try {
                requireFreshAdmission();
                CachedFileInfo existing = fileInfos.get(ledgerId);
                if (existing != null) {
                    return retainForFresh(existing);
                }
                fileInfos.put(ledgerId, candidate);
                published = true;
                return retainForFresh(candidate);
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            // Construction is heap-only; an unpublished candidate has never performed lazy open.
            if (candidate != null && !published) {
                candidate.recycle();
            }
            lock.writeLock().lock();
            try {
                freshLoads--;
                freshProgress.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private CachedFileInfo retainForFresh(CachedFileInfo file) throws IOException {
        if (!file.tryRetain()) {
            throw new IOException("Fresh FileInfo is retiring: " + file.ledgerId);
        }
        return file;
    }

    void beginClosingForFresh() {
        List<CachedFileInfo> snapshot = new ArrayList<>();
        lock.writeLock().lock();
        try {
            freshClosing = true;
            fileInfos.forEach((id, file) -> snapshot.add(file));
        } finally {
            lock.writeLock().unlock();
        }
        for (CachedFileInfo file : snapshot) {
            file.beginClosingForFresh();
        }
    }

    private void retireForFresh(long ledgerId, CachedFileInfo file, boolean force) {
        FileInfo.FreshCloseResult result = null;
        Throwable unexpected = null;
        try {
            result = file.closeForFresh(force);
        } catch (Throwable failure) {
            unexpected = failure;
        }
        lock.writeLock().lock();
        try {
            if (result != null && result.drained()) {
                fileInfos.remove(ledgerId, file);
            }
            Throwable failure = unexpected != null ? unexpected : result == null ? null
                    : result.operationFailure() != null ? result.operationFailure()
                    : result.flushFailure() != null ? result.flushFailure() : result.closeFailure();
            if (freshOperationFailure == null) {
                freshOperationFailure = failure;
            }
            freshRetirements--;
            Thread thread = Thread.currentThread();
            int depth = freshRetirementThreads.get(thread);
            if (depth == 1) {
                freshRetirementThreads.remove(thread);
            } else {
                freshRetirementThreads.put(thread, depth - 1);
            }
            freshProgress.signalAll();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Called after both front caches have released their own references. */
    FreshDrainResult closeForFresh() throws InterruptedException {
        if (!freshLifecycle || isCurrentRetirementThread()) {
            throw new IllegalStateException("Not a fresh backing cache");
        }
        synchronized (freshCloseLock) {
            beginClosingForFresh();
            List<CachedFileInfo> snapshot = new ArrayList<>();
            lock.writeLock().lockInterruptibly();
            try {
                while (freshLoads != 0 || freshRetirements != 0 || hasBorrowersForFresh()) {
                    freshProgress.await();
                }
                fileInfos.forEach((id, file) -> {
                    file.markDead();
                    snapshot.add(file);
                });
            } finally {
                lock.writeLock().unlock();
            }
            Throwable operationFailure = freshOperationFailure;
            Throwable flushFailure = null;
            Throwable closeFailure = null;
            for (CachedFileInfo file : snapshot) {
                FileInfo.FreshCloseResult result = file.closeForFresh(false);
                if (operationFailure == null) {
                    operationFailure = result.operationFailure();
                }
                if (flushFailure == null) {
                    flushFailure = result.flushFailure();
                }
                if (closeFailure == null) {
                    closeFailure = result.closeFailure();
                }
                if (result.drained()) {
                    lock.writeLock().lock();
                    try {
                        fileInfos.remove(file.ledgerId, file);
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            }
            return new FreshDrainResult(fileInfos.isEmpty() && flushFailure == null && closeFailure == null,
                    operationFailure, flushFailure, closeFailure);
        }
    }

    boolean isCurrentRetirementThread() {
        lock.readLock().lock();
        try {
            return freshRetirementThreads.containsKey(Thread.currentThread());
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean hasBorrowersForFresh() {
        boolean[] borrowed = {false};
        fileInfos.forEach((id, file) -> borrowed[0] |= file.getRefCount() > 0);
        return borrowed[0];
    }

    class CachedFileInfo extends FileInfo {
        final long ledgerId;
        final AtomicInteger refCount;

        CachedFileInfo(long ledgerId, File lf, byte[] masterKey) throws IOException {
            super(lf, masterKey, fileInfoVersionToWrite, freshLifecycle, historical);
            this.ledgerId = ledgerId;
            this.refCount = new AtomicInteger(0);
        }

        /**
         * Mark this fileinfo as dead. We can only mark a fileinfo as
         * dead if noone currently holds a reference to it.
         *
         * @return true if we marked as dead, false otherwise
         */
        private boolean markDead() {
            return refCount.compareAndSet(0, DEAD_REF);
        }

        /**
         * Attempt to retain the file info.
         * When a client obtains a fileinfo from a container object,
         * but that container object may release the fileinfo before
         * the client has a chance to call retain. In this case, the
         * file info could be released and the destroyed before we ever
         * get a chance to use it.
         *
         * <p>tryRetain avoids this problem, by doing a compare-and-swap on
         * the reference count. If the refCount is negative, it means that
         * the fileinfo is being cleaned up, and this fileinfo object should
         * not be used. This works in tandem with #markDead, which will only
         * set the refCount to negative if noone currently has it retained
         * (i.e. the refCount is 0).
         *
         * @return true if we managed to increment the refcount, false otherwise
         */
        boolean tryRetain() {
            if (freshLifecycle) {
                lock.readLock().lock();
                try {
                    if (freshClosing || freshOperationFailure != null || isClosed()) {
                        return false;
                    }
                    return tryRetainInternal();
                } finally {
                    lock.readLock().unlock();
                }
            }
            return tryRetainInternal();
        }

        private boolean tryRetainInternal() {
            while (true) {
                int count = refCount.get();
                if (count < 0) {
                    return false;
                } else if (refCount.compareAndSet(count, count + 1)) {
                    return true;
                }
            }
        }

        int getRefCount() {
            return refCount.get();
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                releaseFileInfo(ledgerId, this);
            }
        }

        @Override
        public String toString() {
            return "CachedFileInfo(ledger=" + ledgerId
                + ",refCount=" + refCount.get()
                + ",closed=" + isClosed()
                + ",id=" + System.identityHashCode(this) + ")";
        }
    }

    interface FileLoader {
        File load(long ledgerId, boolean createIfMissing) throws IOException;
    }
}

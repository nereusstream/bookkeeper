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
import java.io.InterruptedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/** One bounded import under an external bootstrap owner's exclusive control and source scopes. */
final class SegmentClassicBootstrapClaims {
    @FunctionalInterface
    interface LedgerVisitor {
        void visit(long ledgerId) throws IOException;
    }

    @FunctionalInterface
    interface LedgerSource {
        /** Visit synchronously without retaining the visitor. Success requires closing the reader and its listings. */
        void scan(LedgerVisitor visitor) throws IOException;
    }

    private static final class Batch {
        final CountDownLatch terminal = new CountDownLatch(1);
        List<SegmentBookieControlLog.Result> results;
        Throwable failure;

        void completed(List<SegmentBookieControlLog.Result> results, Throwable failure) {
            this.results = results;
            this.failure = failure;
        }
    }

    private final SegmentBookieControlLog.Owned owned;
    private final SegmentBookieControlDriver driver;
    private final Map<Long, SegmentBookieControlLog.RouteChange> pending = new LinkedHashMap<>();
    private Thread scanner;
    private Throwable failure;
    private boolean drained;

    SegmentClassicBootstrapClaims(SegmentBookieControlLog.Owned owned, SegmentBookieControlDriver driver) {
        this.owned = Objects.requireNonNull(owned, "owned");
        this.driver = Objects.requireNonNull(driver, "driver");
    }

    /**
     * Called once on an external cold thread outside the owner loop and its I/O service. The enclosing scope keeps
     * the sources and both control resources exclusive and alive, including after stop, until they actually drain.
     */
    void importClaims(LedgerSource indexes, LedgerSource journals) throws IOException {
        if (driver.isOwnerThread()) {
            throw new IllegalStateException("Bookie control owner cannot wait for its own batch");
        }
        synchronized (this) {
            throwFailure(failure);
            if (scanner != null || drained) {
                throw new IllegalStateException("Classic bootstrap claim import is one-use");
            }
            scanner = Thread.currentThread();
        }

        Throwable outcome;
        try {
            checkRunning();
            var log = owned.log();
            driver.requireLog(log);
            int limit = log.unsealedBootstrapClaimLimit();
            LedgerVisitor visitor = ledger -> visit(log, limit, ledger);
            indexes.scan(visitor);
            checkRunning();
            journals.scan(visitor);
            checkRunning();
            flush(log);
            log.unsealedBootstrapClaimLimit();
            checkRunning();
        } catch (IOException | RuntimeException | Error problem) {
            synchronized (this) {
                retainFailure(problem);
            }
        } finally {
            pending.clear();
            synchronized (this) {
                scanner = null;
                drained = true;
                outcome = failure;
            }
        }
        throwFailure(outcome);
    }

    /** Stop admitting claims. The scanning thread still owns the current reader and any accepted batch. */
    synchronized void stop(IOException cause) {
        Objects.requireNonNull(cause, "cause");
        if (!drained && failure == null) {
            retainFailure(cause);
            drained = scanner == null;
        }
    }

    /** This import has returned all of its source calls and accepted batches. The outer resources drain separately. */
    synchronized boolean drained() {
        return drained;
    }

    private synchronized void checkRunning() throws IOException {
        if (scanner != Thread.currentThread()) {
            throw new IllegalStateException("Classic bootstrap claims require the original scanning thread");
        }
        if (Thread.currentThread().isInterrupted() && failure == null) {
            retainFailure(new InterruptedIOException("Classic bootstrap claim import interrupted"));
        }
        throwFailure(failure);
    }

    private void visit(SegmentBookieControlLog log, int limit, long ledger) throws IOException {
        checkRunning();
        if (pending.containsKey(ledger)) {
            return;
        }
        var claim = log.prepareBootstrapClaim(ledger);
        if (claim.applied().isEmpty()) {
            pending.put(ledger, claim.command());
            if (pending.size() == limit) {
                flush(log);
            }
        }
    }

    private void flush(SegmentBookieControlLog log) throws IOException {
        if (pending.isEmpty()) {
            return;
        }
        var commands = List.copyOf(pending.values());
        var batch = new Batch();
        synchronized (this) {
            checkRunning();
            if (!driver.submitChanges(commands, batch::completed, batch.terminal::countDown)) {
                throw new IOException("Classic bootstrap claim batch was not admitted");
            }
        }
        awaitTerminal(batch);
        throwFailure(batch.failure);
        verifyApplied(log, commands, batch.results);
        pending.clear();
        checkRunning();
    }

    private void awaitTerminal(Batch batch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    batch.terminal.await();
                    return;
                } catch (InterruptedException cause) {
                    if (!interrupted) {
                        var failure = new InterruptedIOException("Classic bootstrap claim batch interrupted");
                        failure.initCause(cause);
                        stop(failure);
                        interrupted = true;
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void verifyApplied(SegmentBookieControlLog log,
            List<SegmentBookieControlLog.RouteChange> commands, List<SegmentBookieControlLog.Result> results)
            throws IOException {
        if (results == null || results.size() != commands.size()) {
            throw new IOException("Classic bootstrap claim batch result count differs");
        }
        long previousSequence = -1;
        for (int i = 0; i < commands.size(); i++) {
            var command = commands.get(i);
            var result = results.get(i);
            var claim = log.prepareBootstrapClaim(command.value());
            var original = claim.applied().orElseThrow(() -> new IOException("Classic bootstrap claim did not apply"));
            if (!command.equals(claim.command()) || !command.operationId().equals(result.operationId())
                    || result.sequence() <= previousSequence
                    || (result.status() == SegmentBookieControlLog.Status.APPLIED_DURABLE
                            ? !original.equals(result)
                            : result.status() != SegmentBookieControlLog.Status.ALREADY_DURABLE
                                    || result.sequence() < original.sequence()
                                    || result.localGeneration() != original.localGeneration()
                                    || result.localInstallGeneration() != original.localInstallGeneration())) {
                throw new IOException("Classic bootstrap claim result differs from its applied command");
            }
            previousSequence = result.sequence();
        }
    }

    private void retainFailure(Throwable problem) {
        if (failure == null) {
            failure = problem;
        } else if (failure != problem) {
            failure.addSuppressed(problem);
        }
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException cause) {
            throw cause;
        }
        if (failure instanceof RuntimeException cause) {
            throw cause;
        }
        if (failure instanceof Error cause) {
            throw cause;
        }
        if (failure != null) {
            throw new IOException("Classic bootstrap claim import failed", failure);
        }
    }
}

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
package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicBootstrap;
import org.apache.bookkeeper.stats.NullStatsLogger;

/** Bookie-side bridge for the original offline bootstrap owner's scoped inventory and recovery. */
public final class ClassicBootstrapStorage extends OwnedClassicLifecycle {
    @FunctionalInterface
    public interface LedgerVisitor {
        void visit(long ledgerId) throws IOException;
    }

    public record Drain(boolean drained, Throwable closeFailure) { }

    private SegmentClassicBootstrap.InventorySession session;
    private IndexPersistenceMgr.HistoricalIndexReader indexes;
    private ClassicJournalRecovery journals;
    private boolean reading;
    private boolean scanned;
    private volatile boolean closing;
    private Drain inventoryTerminal;
    private BookieImpl recoveryBookie;
    private Thread recovering;
    private boolean recoveryAttempted;

    private ClassicBootstrapStorage() { }

    /** Heap-only. The holder cannot read without its one original session. */
    public static ClassicBootstrapStorage prepare() {
        return new ClassicBootstrapStorage();
    }

    public synchronized void initialize(SegmentClassicBootstrap.InventorySession original) throws IOException {
        if (session != null || closing || inventoryTerminal != null) {
            throw new IOException("Classic bootstrap reader initializes once");
        }
        var configuration = original.consume(this);
        session = original;
        var ledgers = BookieImpl.getCurrentDirectories(configuration.getLedgerDirs());
        var indexDirectories = configuration.getIndexDirs();
        indexes = new IndexPersistenceMgr.HistoricalIndexReader(indexDirectories == null ? List.of(ledgers)
                : List.of(BookieImpl.getCurrentDirectories(indexDirectories)));
        journals = ClassicJournalRecovery.forInventory(
                Arrays.stream(BookieImpl.getCurrentDirectories(configuration.getJournalDirs()))
                        .map(File::toPath).toList(),
                Arrays.stream(ledgers).map(File::toPath).toList(),
                configuration.getNettyMaxFrameSizeBytes(), configuration.getJournalAlignmentSize());
    }

    /** Normal return covers both sources. The caller also observes actual drain before publishing success. */
    public synchronized void scan(LedgerVisitor visitor) throws IOException {
        if (session == null || closing || scanned || inventoryTerminal != null) {
            throw new IOException("Classic bootstrap reader requires its original one-use session");
        }
        session.requireReader(this);
        scanned = true;
        reading = true;
        try {
            indexes.readLedgerIds(visitor::visit);
            session.requireReader(this);
            journals.readLedgerIds(visitor::visit);
        } finally {
            reading = false;
        }
    }

    public synchronized Drain closeInventory() {
        if (reading) {
            throw new IllegalStateException("Classic bootstrap reader cannot drain its own activity");
        }
        if (inventoryTerminal == null) {
            var journal = journals == null ? null : journals.closeAndDrain();
            var index = indexes == null ? null : indexes.closeAndDrain();
            boolean drained = (journal == null || journal.drained()) && (index == null || index.drained());
            Throwable failure = index == null ? null : index.closeFailure();
            if (failure == null && journal != null) {
                failure = journal.closeFailure();
            }
            inventoryTerminal = new Drain(drained, failure);
        }
        return inventoryTerminal;
    }

    public synchronized void recover(SegmentClassicBootstrap.RecoverySession original)
            throws IOException, InterruptedException {
        checkStarting();
        if (!scanned || inventoryTerminal == null || !inventoryTerminal.drained() || recoveryAttempted) {
            throw new IOException("Classic bootstrap recovery requires its drained original inventory");
        }
        var configuration = original.consume(this);
        recoveryAttempted = true;
        recovering = Thread.currentThread();
        try {
            recoveryBookie = BookieImpl.forClassicBootstrap(configuration, NullStatsLogger.INSTANCE, this);
            recoveryBookie.recoverForClassicBootstrap(this, ledger -> {
                checkStarting();
                original.requireClaim(this, ledger);
            });
            var bookie = recoveryBookie;
            var result = closeRecoveryBookie();
            if (!result.drained()) {
                throw new IOException("Classic bootstrap recovery has unresolved resources", result.closeFailure());
            }
            if (bookie.operationFailureForFresh() != null) {
                throw new IOException("Classic bootstrap storage recovery failed", bookie.operationFailureForFresh());
            }
            checkStarting();
        } catch (BookieException failure) {
            throw new IOException("Classic bootstrap storage recovery failed", failure);
        } finally {
            recovering = null;
        }
    }

    void requireRecoveryBookie(BookieImpl candidate) throws IOException {
        checkStarting();
        if (recovering != Thread.currentThread() || candidate != recoveryBookie) {
            throw new IOException("Classic bootstrap recovery belongs to another Bookie or episode");
        }
    }

    public synchronized Drain closeAndDrain() throws InterruptedException {
        if (recovering == Thread.currentThread()) {
            throw new IllegalStateException("Classic bootstrap recovery cannot drain its own activity");
        }
        closing = true;
        var readers = closeInventory();
        if (!readers.drained() || recoveryBookie == null) {
            return readers;
        }
        try {
            var result = closeRecoveryBookie();
            return new Drain(result.drained(), result.closeFailure());
        } catch (IOException failure) {
            return new Drain(false, failure);
        }
    }

    private BookieImpl.FreshDrainResult closeRecoveryBookie() throws IOException, InterruptedException {
        var result = recoveryBookie.closeForFresh();
        if (result.drained()) {
            recoveryBookie = null;
        }
        return result;
    }

    @Override
    void checkStarting() throws IOException {
        if (closing) {
            throw new IOException("Classic bootstrap storage is closing");
        }
    }

    @Override
    boolean isClosing() {
        return closing;
    }

    @Override
    void requestClose() {
        closing = true;
    }
}

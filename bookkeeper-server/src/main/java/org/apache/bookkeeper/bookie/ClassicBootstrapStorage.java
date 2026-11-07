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

/** Bookie-side bridge for the original offline bootstrap owner's scoped Classic inventory. */
public final class ClassicBootstrapStorage {
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
    private boolean closing;
    private Drain terminal;

    private ClassicBootstrapStorage() { }

    /** Heap-only. The holder cannot read without its one original session. */
    public static ClassicBootstrapStorage prepare() {
        return new ClassicBootstrapStorage();
    }

    public synchronized void initialize(SegmentClassicBootstrap.InventorySession original) throws IOException {
        if (session != null || closing) {
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
        if (session == null || closing || scanned) {
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

    public synchronized Drain closeAndDrain() {
        if (reading) {
            throw new IllegalStateException("Classic bootstrap reader cannot drain its own activity");
        }
        if (terminal == null) {
            closing = true;
            var journal = journals == null ? null : journals.closeAndDrain();
            var index = indexes == null ? null : indexes.closeAndDrain();
            boolean drained = (journal == null || journal.drained()) && (index == null || index.drained());
            Throwable failure = index == null ? null : index.closeFailure();
            if (failure == null && journal != null) {
                failure = journal.closeFailure();
            }
            terminal = new Drain(drained, failure);
        }
        return terminal;
    }
}

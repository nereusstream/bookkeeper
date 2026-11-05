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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookieImpl.METAENTRY_ID_FENCE_KEY;
import static org.apache.bookkeeper.bookie.BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC;
import static org.apache.bookkeeper.bookie.BookieImpl.METAENTRY_ID_LEDGER_KEY;
import static org.apache.bookkeeper.bookie.ClassicHistoryFiles.fileIdentity;
import static org.apache.bookkeeper.bookie.ClassicHistoryFiles.readFully;
import static org.apache.bookkeeper.bookie.ClassicHistoryFiles.requireDirectory;
import static org.apache.bookkeeper.bookie.ClassicHistoryFiles.requireSame;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Read-only V6 recovery resources. Supplies no startup, metadata, launcher or bootstrap permission. */
final class ClassicJournalRecovery {
    record Marks(long logId, long offset, Set<Path> existing) {
        Marks {
            existing = Set.copyOf(existing);
        }
    }

    record Drain(boolean drained, int retainedResources, Throwable readFailure, Throwable closeFailure) { }

    private record Source(Path journal, String markName) {
        Source {
            Objects.requireNonNull(journal, "journal");
            Objects.requireNonNull(markName, "markName");
            if (markName.isBlank() || markName.contains("/")) {
                throw new IllegalArgumentException("Explicit bounded historical Journal layout required");
            }
        }
    }

    private final List<Source> sources;
    private final List<Path> markDirectories;
    private final int maximumRecord;
    private final int alignment;
    private final ClassicHistoryFiles files = new ClassicHistoryFiles();
    private Thread active;
    private boolean closing;
    private Throwable readFailure;

    /** Heap-only; the enclosing episode retains this owner before any read or directory open. */
    ClassicJournalRecovery(Path journal, List<Path> markDirectories, String markName, int maximumRecord,
            int alignment) {
        this(List.of(new Source(journal, markName)), markDirectories, maximumRecord, alignment);
    }

    private ClassicJournalRecovery(List<Source> sources, List<Path> markDirectories, int maximumRecord, int alignment) {
        this.sources = List.copyOf(sources);
        this.markDirectories = List.copyOf(markDirectories);
        if (this.sources.isEmpty() || this.markDirectories.isEmpty()
                || maximumRecord < 16 || alignment < JournalChannel.SECTOR_SIZE
                || alignment % JournalChannel.SECTOR_SIZE != 0) {
            throw new IllegalArgumentException("Explicit bounded historical Journal layout required");
        }
        this.maximumRecord = maximumRecord;
        this.alignment = alignment;
    }

    /** The enclosing owner supplies the current directories in their original configured order. */
    static ClassicJournalRecovery forInventory(List<Path> journalDirectories, List<Path> ledgerDirectories,
            int maximumRecord, int alignment) {
        var journals = List.copyOf(journalDirectories);
        var sources = new ArrayList<Source>(journals.size());
        for (int i = 0; i < journals.size(); i++) {
            sources.add(new Source(journals.get(i), Journal.lastMarkFileName(i, journals.size())));
        }
        return new ClassicJournalRecovery(sources, ledgerDirectories, maximumRecord, alignment);
    }

    /**
     * Visits each checked record in the required suffix of every Journal, including repeated ledger IDs.
     * The outer owner must exclude changes to the roots. Only normal return completes the inventory.
     */
    synchronized void readLedgerIds(ClassicHistoryFiles.LedgerVisitor visitor) throws IOException {
        Objects.requireNonNull(visitor, "visitor");
        enter();
        try {
            for (var source : sources) {
                var mark = readMarks(source.markName());
                var logs = readIds(source.journal(), mark.logId());
                requireMarkedJournal(mark.logId(), logs, source.journal());
                for (long id : logs) {
                    long offset = id == mark.logId() ? mark.offset() : 0;
                    scanFile(source.journal(), id, offset, (version, position, record) ->
                            visitor.visit(validateRecord(record)));
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    private Source singleSource() {
        if (sources.size() != 1) {
            throw new IllegalStateException("Individual Journal reads require a single Journal source");
        }
        return sources.get(0);
    }

    synchronized Marks marks() throws IOException {
        enter();
        try {
            return readMarks(singleSource().markName());
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    private Marks readMarks(String markName) throws IOException {
        long logId = 0;
        long offset = 0;
        Set<Path> existing = new HashSet<>();
        for (Path directory : markDirectories) {
            requireDirectory(directory);
            Path path = directory.resolve(markName);
            BasicFileAttributes before;
            try {
                before = fileIdentity(path);
            } catch (NoSuchFileException absent) {
                continue; // An absent checkpoint copy contributes the original zero mark, never a success claim.
            }
            if (before.size() != 16) {
                throw new IOException("Historical lastMark must contain exactly two longs: " + path);
            }
            ByteBuffer bytes = ByteBuffer.allocate(16);
            try (var read = files.openRead(path)) {
                readFully(read.channel, bytes);
                requireSame(path, before, read.channel);
            }
            bytes.flip();
            long candidate = bytes.getLong();
            long position = bytes.getLong();
            if (candidate < 0 || position < 0 || candidate == 0 && position != 0
                    || position != 0 && position < JournalChannel.HEADER_SIZE) {
                throw new IOException("Invalid historical lastMark: " + path);
            }
            existing.add(path);
            if (candidate > logId || candidate == logId && position > offset) {
                logId = candidate;
                offset = position;
            }
        }
        return new Marks(logId, offset, existing);
    }

    synchronized List<Long> ids(long minimum) throws IOException {
        enter();
        try {
            return readIds(singleSource().journal(), minimum);
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    private List<Long> readIds(Path journal, long minimum) throws IOException {
        requireDirectory(journal);
        Set<Long> found = new HashSet<>();
        try (var read = files.openDirectory(journal)) {
            for (Path path : read.listing) {
                String name = path.getFileName().toString();
                if (!name.endsWith(".txn")) {
                    continue;
                }
                long id;
                try {
                    id = Long.parseLong(name.substring(0, name.length() - 4), 16);
                } catch (NumberFormatException malformed) {
                    throw new IOException("Invalid historical Journal file name: " + path, malformed);
                }
                fileIdentity(path);
                if (id < 0 || !found.add(id)) {
                    throw new IOException("Ambiguous historical Journal identity: " + path);
                }
            }
        }
        return found.stream().filter(id -> id >= minimum).sorted().toList();
    }

    static void requireMarkedJournal(long markedId, List<Long> logs, Path journal) throws IOException {
        if (markedId > 0 && (logs.isEmpty() || logs.get(0) != markedId)) {
            throw new IOException("Recovery log " + markedId + " is missing at " + journal.toAbsolutePath());
        }
    }

    synchronized long scan(long id, long offset, Journal.JournalScanner scanner) throws IOException {
        enter();
        try {
            return scanFile(singleSource().journal(), id, offset, scanner);
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    private long scanFile(Path journal, long id, long offset, Journal.JournalScanner scanner) throws IOException {
        if (id < 0 || offset < 0 || offset != 0 && offset < JournalChannel.HEADER_SIZE) {
            throw new IOException("Invalid historical Journal scan position");
        }
        Path path = journal.resolve(Long.toHexString(id) + ".txn");
        BasicFileAttributes before = fileIdentity(path);
        try (var read = files.openRead(path)) {
            ByteBuffer header = ByteBuffer.allocate(JournalChannel.HEADER_SIZE);
            readFully(read.channel, header);
            header.flip();
            if (header.getInt() != 0x424b4c47 || header.getInt() != JournalChannel.V6 || offset > before.size()) {
                throw new IOException("Historical child requires an existing V6 Journal and valid cut");
            }
            read.channel.position(offset == 0 ? JournalChannel.HEADER_SIZE : offset);
            long result = Journal.scanRecords(read.channel, JournalChannel.V6, scanner, maximumRecord, alignment);
            requireSame(path, before, read.channel);
            return result;
        }
    }

    /** Shared V6 payload validation. Leaves the buffer positioned for the original replay consumer. */
    static long validateRecord(ByteBuffer record) throws IOException {
        int start = record.position();
        int size = record.remaining();
        if (size < 16 || record.getLong(start) < 0) {
            throw new IOException("Historical Journal record lacks its bounded ledger/entry header");
        }
        long entry = record.getLong(start + 8);
        if (entry == METAENTRY_ID_LEDGER_KEY || entry == METAENTRY_ID_LEDGER_EXPLICITLAC) {
            if (size < 20) {
                throw new IOException("Historical Journal metadata lacks its length field");
            }
            int length = record.getInt(start + 16);
            if (length < 0 || length != size - 20 || entry == METAENTRY_ID_LEDGER_EXPLICITLAC && length < 16) {
                throw new IOException("Historical Journal metadata length differs from its bounded payload");
            }
        } else if (entry == METAENTRY_ID_FENCE_KEY) {
            if (size != 16) {
                throw new IOException("Historical Journal fence has an unexpected payload");
            }
        } else if (entry < 0 || size < 24) {
            throw new IOException("Unknown or incomplete historical Journal entry payload");
        }
        return record.getLong(start);
    }

    private void enter() throws IOException {
        if (closing || active != null || readFailure != null || !files.healthy()) {
            throw new IOException("Historical Journal reader is closing, active or failed",
                    files.closeFailure() != null ? files.closeFailure() : readFailure);
        }
        active = Thread.currentThread();
    }

    private void failed(Throwable failure) {
        if (readFailure == null) {
            readFailure = failure;
        }
    }

    synchronized boolean healthy() {
        return !closing && readFailure == null && files.healthy();
    }

    synchronized Drain closeAndDrain() {
        if (active == Thread.currentThread()) {
            throw new IllegalStateException("Historical Journal reader cannot drain its own actual operation");
        }
        closing = true;
        var result = files.closeAndDrain();
        return new Drain(result.drained(), result.retainedResources(), readFailure, result.closeFailure());
    }
}

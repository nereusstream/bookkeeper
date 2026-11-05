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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Test access to the two package-private Classic inventory readers and their real file formats. */
public final class ClassicClaimTestHistory {
    @FunctionalInterface
    public interface Visitor {
        void visit(long ledger) throws IOException;
    }

    private ClassicClaimTestHistory() { }

    public static void readIndex(Path index, Visitor visitor) throws IOException {
        var reader = new IndexPersistenceMgr.HistoricalIndexReader(List.of(index.toFile()));
        try {
            reader.readLedgerIds(visitor::visit);
        } finally {
            assertTrue(reader.closeAndDrain().drained());
        }
    }

    public static void readJournal(Path journal, Path ledger, Visitor visitor) throws IOException {
        var reader = ClassicJournalRecovery.forInventory(List.of(journal), List.of(ledger), 1024, 512);
        try {
            reader.readLedgerIds(visitor::visit);
        } finally {
            assertTrue(reader.closeAndDrain().drained());
        }
    }

    public static void writeIndex(Path index, long ledger) throws IOException {
        Path file = index.resolve(IndexPersistenceMgr.getLedgerName(ledger));
        Files.createDirectories(file.getParent());
        var info = new FileInfo(file.toFile(), new byte[]{1, 2, 3}, 1);
        try {
            info.write(new ByteBuffer[]{ByteBuffer.allocate(Long.BYTES)}, 0);
        } finally {
            info.close(true);
        }
    }

    public static void writeJournal(Path file, long... ledgers) throws IOException {
        var bytes = journalImage(ledgers.length * 28);
        for (int entry = 0; entry < ledgers.length; entry++) {
            bytes.putInt(24).putLong(ledgers[entry]).putLong(entry).putLong(0);
        }
        Files.write(file, bytes.array());
    }

    public static void writeBadRepeatedRecord(Path file, long ledger) throws IOException {
        var bytes = journalImage(28);
        bytes.putInt(24).putLong(ledger).putLong(BookieImpl.METAENTRY_ID_LEDGER_KEY).putInt(8).putInt(0);
        Files.write(file, bytes.array());
    }

    private static ByteBuffer journalImage(int bytes) {
        var image = ByteBuffer.allocate(JournalChannel.HEADER_SIZE + bytes)
                .putInt(0x424b4c47).putInt(JournalChannel.V6);
        return image.position(JournalChannel.HEADER_SIZE);
    }
}

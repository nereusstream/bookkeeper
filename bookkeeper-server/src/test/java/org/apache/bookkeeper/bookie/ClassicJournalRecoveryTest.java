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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Historical file/resource facts only; no controlled launcher episode, registration or Unix qualification. */
public class ClassicJournalRecoveryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 30000)
    public void realOwnedV6HistoryReplaysThenAppendsAndUpdatesExistingMultiJournalMarks() throws Exception {
        try (var f = new Fixture()) {
            Journal first = f.fresh();
            first.initializeForFresh();
            first.start();
            first.awaitStartedForFresh();
            f.append(first, 0);
            assertTrue(first.quiesceForFresh().quiesced());
            first.checkpointComplete(first.newCheckpoint(), false);
            assertTrue(first.closeForFresh().drained());
            Path mark = f.ledgers.get(0).resolve("lastMark.0");
            Object markKey = Files.readAttributes(mark, "unix:ino").get("ino");
            long oldId = Journal.listJournalIds(f.journal.toFile(), null).get(0);
            byte[] oldBytes = Files.readAllBytes(f.journal.resolve(Long.toHexString(oldId) + ".txn"));
            Journal recovered = f.history();
            assertThrows(IllegalStateException.class, recovered::initializeForFresh);
            recovered.initializeForClassicHistory();
            assertEquals(oldId, recovered.getLastLogMark().getCurMark().getLogFileId());
            List<Long> replayed = new ArrayList<>();
            recovered.scanJournal(oldId, 0, (version, offset, bytes) -> replayed.add(bytes.getLong(8)), false);
            assertEquals(List.of(0L), replayed);
            assertEquals(List.of(oldId), recovered.journalIdsForRecovery(oldId));
            recovered.start();
            recovered.awaitStartedForFresh();
            assertThrows(IOException.class, () -> recovered.scanJournal(oldId, 0, (v, o, b) -> { }, false));
            f.append(recovered, 1);
            assertTrue(recovered.quiesceForFresh().quiesced());
            recovered.checkpointComplete(recovered.newCheckpoint(), false);
            assertTrue(recovered.closeForFresh().drained());
            assertEquals(markKey, Files.readAttributes(mark, "unix:ino").get("ino"));
            assertArrayEquals(Files.readAllBytes(mark), Files.readAllBytes(f.ledgers.get(1).resolve("lastMark.0")));
            assertArrayEquals(oldBytes, Files.readAllBytes(f.journal.resolve(Long.toHexString(oldId) + ".txn")));
            Journal legacy = new Journal(0, f.journal.toFile(), f.conf, f.directories);
            try {
                List<Long> ids = legacy.journalIdsForRecovery(0);
                assertEquals(2, ids.size());
                replayed.clear();
                legacy.scanJournal(ids.get(1), 0, (v, o, b) -> replayed.add(b.getLong(8)), false);
                assertEquals(List.of(1L), replayed);
            } finally {
                legacy.shutdown();
            }
        }
    }

    @Test(timeout = 30000)
    public void marksUseOriginalMaximumAndMissingCopiesButNeverSkipMalformedPresentCopies() throws Exception {
        try (var f = new Fixture()) {
            Files.write(f.ledgers.get(1).resolve("lastMark.0"), mark(7, 600));
            var reader = f.reader();
            assertEquals(7, reader.marks().logId());
            assertEquals(600, reader.marks().offset());
            assertEquals(1, reader.marks().existing().size());
            Files.write(f.ledgers.get(0).resolve("lastMark.0"), mark(8, 512));
            assertEquals(8, reader.marks().logId());
            assertEquals(512, reader.marks().offset());
            assertTrue(reader.closeAndDrain().drained());
            Files.write(f.ledgers.get(0).resolve("lastMark.0"), new byte[15]);
            var invalid = f.reader();
            assertThrows(IOException.class, invalid::marks);
            assertTrue(invalid.closeAndDrain().drained());
            assertEquals(15, Files.size(f.ledgers.get(0).resolve("lastMark.0")));
        }
    }

    @Test(timeout = 30000)
    public void v6BoundsRejectUnsupportedHistoryWithoutCreatingOrChangingAFile() throws Exception {
        try (var f = new Fixture()) {
            var missing = f.reader();
            assertThrows(IOException.class, () -> missing.scan(99, 0, (v, o, b) -> { }));
            assertFalse(Files.exists(f.journal.resolve("63.txn")));
            assertTrue(missing.closeAndDrain().drained());
            List<byte[]> invalid = List.of(new byte[7], image(5, 16), image(6, Integer.MAX_VALUE),
                    image(6, -7), image(6, Journal.PADDING_MASK, -1), image(6, Journal.PADDING_MASK, 1025));
            for (byte[] bytes : invalid) {
                Path file = f.journal.resolve("1.txn");
                Files.write(file, bytes);
                var reader = f.reader();
                assertThrows(IOException.class, () -> reader.scan(1, 0, (v, o, b) -> { }));
                assertTrue(reader.closeAndDrain().drained());
                assertArrayEquals(bytes, Files.readAllBytes(file));
            }
        }
    }

    @Test(timeout = 30000)
    public void originalPaddingAndIncompleteTailRemainReadOnlyAndDoNotInventARecord() throws Exception {
        try (var f = new Fixture()) {
            Path file = f.journal.resolve("1.txn");
            ByteBuffer bytes = ByteBuffer.allocate(560);
            bytes.putInt(0x424b4c47).putInt(6).position(512);
            bytes.putInt(16).putLong(7).putLong(2);
            bytes.putInt(Journal.PADDING_MASK).putInt(8).putLong(0);
            bytes.putInt(32).putLong(7);
            Files.write(file, bytes.array());
            List<Long> records = new ArrayList<>();
            var reader = f.reader();
            assertEquals(560, reader.scan(1, 0, (v, o, b) -> records.add(b.getLong(8))));
            assertEquals(List.of(2L), records);
            assertTrue(reader.closeAndDrain().drained());
            assertArrayEquals(bytes.array(), Files.readAllBytes(file));
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryStreamsAllRequiredSuffixesInOrderWithoutChangingFiles() throws Exception {
        try (var f = new Fixture()) {
            byte[] before = entryRecord(99, 0, 0, 0);
            byte[] padding = ByteBuffer.allocate(12).putInt(Journal.PADDING_MASK).putInt(4).putInt(0).array();
            byte[] partial = ByteBuffer.allocate(12).putInt(24).putLong(98).array();
            var inputs = Map.of(
                    f.ledgers.get(0).resolve("lastMark.0"), mark(7, JournalChannel.HEADER_SIZE),
                    f.ledgers.get(1).resolve("lastMark.0"), mark(7, JournalChannel.HEADER_SIZE + before.length),
                    f.ledgers.get(0).resolve("lastMark.1"), mark(3, JournalChannel.HEADER_SIZE + before.length),
                    f.journal.resolve("2.txn"), suffixImage(entryRecord(97, 0, 0, 0)),
                    f.journal.resolve("7.txn"), suffixImage(before,
                            entryRecord(44, BookieImpl.METAENTRY_ID_LEDGER_KEY, 4, 17),
                            entryRecord(44, BookieImpl.METAENTRY_ID_FENCE_KEY), padding, partial),
                    f.journal.resolve("9.txn"), suffixImage(
                            entryRecord(55, BookieImpl.METAENTRY_ID_LEDGER_EXPLICITLAC, 16, 0, 0, 0, 0),
                            entryRecord(44, 1, 0, 0)),
                    f.secondJournal.resolve("3.txn"), suffixImage(before, entryRecord(77, 2, 0, 0)));
            for (var input : inputs.entrySet()) {
                Files.write(input.getKey(), input.getValue());
            }
            var reader = f.inventory();
            var ledgerIds = new ArrayList<Long>();
            try {
                reader.readLedgerIds(ledgerIds::add);
                assertEquals(List.of(44L, 44L, 55L, 44L, 77L), ledgerIds);
                assertFalse(Files.exists(f.ledgers.get(1).resolve("lastMark.1")));
                for (var input : inputs.entrySet()) {
                    assertArrayEquals(input.getValue(), Files.readAllBytes(input.getKey()));
                }
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryVisitsSortedPagesAndDoesNotOverflowAtTheLastJournalId() throws Exception {
        try (var f = new Fixture()) {
            Files.write(f.journal.resolve("7fffffffffffffff.txn"),
                    suffixImage(entryRecord(Long.MAX_VALUE, 0, 0, 0)));
            for (long ledger = 511; ledger > 0; ledger--) {
                Files.write(f.journal.resolve(Long.toHexString(ledger) + ".txn"),
                        suffixImage(entryRecord(ledger, 0, 0, 0)));
            }
            var opened = new AtomicInteger();
            var reader = f.inventory();
            var visited = new ArrayList<Long>();
            try (var files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
                files.when(() -> Files.newDirectoryStream(f.journal)).thenAnswer(invocation -> {
                    opened.incrementAndGet();
                    return invocation.callRealMethod();
                });
                reader.readLedgerIds(ledger -> {
                    assertEquals(visited.size() / 256 + 1, opened.get());
                    if (ledger != Long.MAX_VALUE) {
                        assertEquals(visited.size() + 1L, ledger);
                    }
                    visited.add(ledger);
                });
                assertEquals(512, visited.size());
                assertEquals(Long.MAX_VALUE, visited.get(511).longValue());
                assertEquals(2, opened.get());
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryCannotCompleteAfterItsDirectoryIsReplaced() throws Exception {
        try (var f = new Fixture()) {
            Path journalFile = f.journal.resolve("1.txn");
            Files.write(journalFile, suffixImage(entryRecord(44, 0, 0, 0)));
            Path original = f.journal.resolveSibling("original-current");
            var reader = f.inventory();
            try {
                var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(ledger -> {
                    assertEquals(44, ledger);
                    Files.move(f.journal, original);
                    Files.createDirectory(f.journal);
                    Files.move(original.resolve(journalFile.getFileName()), journalFile);
                }));
                assertTrue(failure.getMessage(), failure.getMessage().contains("directory changed during inventory"));
                assertSame(failure, reader.closeAndDrain().readFailure());
            } finally {
                assertTrue(reader.closeAndDrain().drained());
                if (Files.exists(original)) {
                    if (Files.exists(journalFile)) {
                        Files.move(journalFile, original.resolve(journalFile.getFileName()));
                    }
                    Files.deleteIfExists(f.journal);
                    Files.move(original, f.journal);
                }
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryRejectsNoncanonicalJournalNamesBelowTheMark() throws Exception {
        try (var f = new Fixture()) {
            Files.write(f.ledgers.get(0).resolve("lastMark.0"), mark(10, JournalChannel.HEADER_SIZE));
            Files.write(f.journal.resolve("a.txn"), suffixImage(entryRecord(44, 0, 0, 0)));
            Files.write(f.journal.resolve("09.txn"), suffixImage(entryRecord(55, 0, 0, 0)));
            var reader = f.inventory();
            var visited = new ArrayList<Long>();
            try {
                var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(visited::add));
                assertTrue(failure.getMessage(), failure.getMessage().contains("Noncanonical historical Journal"));
                assertTrue(visited.isEmpty());
                assertSame(failure, reader.closeAndDrain().readFailure());
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryRejectsMissingMarkedFileEvenWithANewerFile() throws Exception {
        try (var f = new Fixture()) {
            Files.write(f.ledgers.get(0).resolve("lastMark"), mark(2, JournalChannel.HEADER_SIZE));
            Files.write(f.journal.resolve("3.txn"), suffixImage(entryRecord(44, 0, 0, 0)));
            var reader = ClassicJournalRecovery.forInventory(List.of(f.journal), f.ledgers, 1024, 512);
            var ledgerIds = new ArrayList<Long>();
            try {
                var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(ledgerIds::add));
                assertTrue(failure.getMessage(), failure.getMessage().contains("Recovery log 2 is missing"));
                assertTrue(ledgerIds.isEmpty());
                assertSame(failure, reader.closeAndDrain().readFailure());
            } finally {
                assertTrue(reader.closeAndDrain().drained());
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryFailsTheWholeReadOnALaterBadMarkOrCompletePayload() throws Exception {
        for (boolean badMark : List.of(false, true)) {
            try (var f = new Fixture()) {
                Files.write(f.journal.resolve("1.txn"), suffixImage(entryRecord(44, 0, 0, 0)));
                Files.write(f.ledgers.get(0).resolve("lastMark.1"),
                        badMark ? new byte[15] : mark(2, JournalChannel.HEADER_SIZE));
                Files.write(f.secondJournal.resolve("2.txn"),
                        suffixImage(entryRecord(55, BookieImpl.METAENTRY_ID_LEDGER_KEY, 8, 0)));
                var reader = f.inventory();
                var ledgerIds = new ArrayList<Long>();
                try {
                    var failure = assertThrows(IOException.class, () -> reader.readLedgerIds(ledgerIds::add));
                    assertTrue(failure.getMessage(), failure.getMessage().contains(badMark
                            ? "lastMark must contain exactly two longs" : "metadata length differs"));
                    assertEquals(List.of(44L), ledgerIds);
                    assertSame(failure, assertThrows(IOException.class, () -> reader.readLedgerIds(ledgerIds::add))
                            .getCause());
                    assertSame(failure, reader.closeAndDrain().readFailure());
                } finally {
                    assertTrue(reader.closeAndDrain().drained());
                }
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryRetainsBothVisitorFailureAndTheFirstNativeCloseFailure() throws Exception {
        try (var f = new Fixture()) {
            Path file = f.journal.resolve("1.txn");
            Files.write(file, suffixImage(entryRecord(44, 0, 0, 0), entryRecord(55, 0, 0, 0)));
            var channel = new ControlledFileChannel(FileChannel.open(file, StandardOpenOption.READ));
            channel.closeFailure = new IOException("inventory first native close");
            var visitorFailure = new IOException("inventory visitor failed");
            var reader = f.inventory();
            try (var opens = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                opens.when(() -> FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(visitorFailure, assertThrows(IOException.class, () -> reader.readLedgerIds(ledger -> {
                    assertEquals(44, ledger);
                    throw visitorFailure;
                })));
                assertArrayEquals(new Throwable[]{channel.closeFailure}, visitorFailure.getSuppressed());
            } finally {
                var drain = reader.closeAndDrain();
                assertFalse(drain.drained());
                assertEquals(1, drain.retainedResources());
                assertSame(visitorFailure, drain.readFailure());
                assertSame(channel.closeFailure, drain.closeFailure());
                assertEquals(drain, reader.closeAndDrain());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
                channel.delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void journalInventoryDrainsOnlyAfterTheActualVisitorAndAllSourcesFinish() throws Exception {
        try (var f = new Fixture()) {
            Files.write(f.journal.resolve("1.txn"), suffixImage(entryRecord(44, 0, 0, 0)));
            Files.write(f.secondJournal.resolve("1.txn"), suffixImage(entryRecord(55, 0, 0, 0)));
            var reader = f.inventory();
            var ledgerIds = new ArrayList<Long>();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var closingStarted = new CountDownLatch(1);
            var reading = new FutureTask<Void>(() -> {
                reader.readLedgerIds(ledger -> {
                    ledgerIds.add(ledger);
                    if (ledger == 44) {
                        assertThrows(IllegalStateException.class, reader::closeAndDrain);
                        entered.countDown();
                        try {
                            assertTrue(release.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new IOException(failure);
                        }
                    }
                });
                return null;
            });
            var draining = new FutureTask<>(() -> {
                closingStarted.countDown();
                return reader.closeAndDrain();
            });
            var scanning = new Thread(reading, "journal-inventory-test-reader");
            var closing = new Thread(draining, "journal-inventory-test-drain");
            scanning.start();
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                closing.start();
                assertTrue(closingStarted.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> draining.get(150, TimeUnit.MILLISECONDS));
            } finally {
                release.countDown();
                scanning.join(5000);
                closing.join(5000);
            }
            reading.get(5, TimeUnit.SECONDS);
            assertEquals(List.of(44L, 55L), ledgerIds);
            assertTrue(draining.get(5, TimeUnit.SECONDS).drained());
            assertFalse(scanning.isAlive());
            assertFalse(closing.isAlive());
        }
    }

    @Test(timeout = 30000)
    public void aliasesAndAmbiguousNamesFailBeforeReadAndFreshStillRejectsHistory() throws Exception {
        try (var f = new Fixture()) {
            Path file = f.journal.resolve("1.txn");
            Files.write(file, image(6, 0));
            Path other = f.journal.resolve("other");
            Files.createLink(other, file);
            var aliased = f.reader();
            assertThrows(IOException.class, () -> aliased.scan(1, 0, (v, o, b) -> { }));
            assertTrue(aliased.closeAndDrain().drained());
            Files.delete(other);
            Files.createSymbolicLink(other, file);
            Files.move(other, f.journal.resolve("2.txn"));
            var linked = f.reader();
            assertThrows(IOException.class, () -> linked.ids(0));
            assertTrue(linked.closeAndDrain().drained());
            Journal fresh = f.fresh();
            assertThrows(IOException.class, fresh::initializeForFresh);
            assertTrue(fresh.closeForFresh().drained());
            Journal history = f.history();
            assertThrows(IllegalStateException.class, history::initializeForFresh);
            assertTrue(history.closeForFresh().drained());
        }
    }

    @Test(timeout = 30000)
    public void actualReadNativeCloseFailureIsRetainedAndNeverRetried() throws Exception {
        try (var f = new Fixture()) {
            Path file = f.journal.resolve("1.txn");
            Files.write(file, image(6, 0));
            var channel = new ControlledFileChannel(FileChannel.open(file, StandardOpenOption.READ));
            channel.closeFailure = new IOException("first native historical read close");
            var reader = f.reader();
            try (var opens = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                opens.when(() -> FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
                        .thenReturn(channel);
                assertSame(channel.closeFailure, assertThrows(IOException.class,
                        () -> reader.scan(1, 0, (v, o, b) -> { })));
            } finally {
                var failed = reader.closeAndDrain();
                assertFalse(failed.drained());
                assertSame(channel.closeFailure, failed.closeFailure());
                assertEquals(1, failed.retainedResources());
                assertFalse(reader.closeAndDrain().drained());
                assertEquals(1, channel.closeAttempts.get());
                assertTrue(channel.delegate.isOpen());
                channel.delegate.close();
            }
            assertFalse(reader.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void directoryFirstCloseFailureAlsoRemainsUnresolved() throws Exception {
        try (var f = new Fixture()) {
            DirectoryStream<Path> actual = Files.newDirectoryStream(f.journal);
            var attempts = new AtomicInteger();
            IOException failure = new IOException("historical directory native close");
            var controlled = new DirectoryStream<Path>() {
                @Override
                public java.util.Iterator<Path> iterator() {
                    return actual.iterator();
                }

                @Override
                public void close() throws IOException {
                    attempts.incrementAndGet();
                    throw failure;
                }
            };
            var reader = f.reader();
            try (var files = mockStatic(Files.class, CALLS_REAL_METHODS)) {
                files.when(() -> Files.newDirectoryStream(f.journal)).thenReturn(controlled);
                assertSame(failure, assertThrows(IOException.class, () -> reader.ids(0)));
            } finally {
                assertFalse(reader.closeAndDrain().drained());
                assertFalse(reader.closeAndDrain().drained());
                assertSame(failure, reader.closeAndDrain().closeFailure());
                assertEquals(1, attempts.get());
                actual.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void journalCloseWaitsForTheActualScanAndCallerTimeoutDoesNotCancelIt() throws Exception {
        try (var f = new Fixture()) {
            ByteBuffer bytes = ByteBuffer.allocate(536).putInt(0x424b4c47).putInt(6);
            bytes.position(512);
            bytes.putInt(16).putLong(7).putLong(2).putInt(0);
            Files.write(f.journal.resolve("1.txn"), bytes.array());
            Journal journal = f.history();
            journal.initializeForClassicHistory();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var scan = new FutureTask<>(() -> journal.scanJournal(1, 0, (v, o, b) -> {
                assertThrows(IllegalStateException.class, journal::closeForFresh);
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
            }, false));
            Thread scanning = new Thread(scan, "owned-historical-scan");
            var close = new FutureTask<>(journal::closeForFresh);
            Thread closing = new Thread(close, "owned-historical-close");
            scanning.start();
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertThrows(IllegalStateException.class, journal::start);
                closing.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> closing.getState() == Thread.State.WAITING);
                assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
                assertFalse(scan.isCancelled());
                assertFalse(close.isDone());
            } finally {
                release.countDown();
                scanning.join(5000);
                closing.join(5000);
            }
            assertEquals(536, scan.get().longValue());
            assertTrue(close.get().drained());
            assertFalse(scanning.isAlive());
            assertFalse(closing.isAlive());
            assertArrayEquals(bytes.array(), Files.readAllBytes(f.journal.resolve("1.txn")));
        }
    }

    private static byte[] mark(long id, long offset) {
        return ByteBuffer.allocate(16).putLong(id).putLong(offset).array();
    }

    private static byte[] entryRecord(long ledger, long entry, int... words) {
        int length = 16 + 4 * words.length;
        var bytes = ByteBuffer.allocate(4 + length).putInt(length).putLong(ledger).putLong(entry);
        for (int word : words) {
            bytes.putInt(word);
        }
        return bytes.array();
    }

    private static byte[] suffixImage(byte[]... records) {
        int length = JournalChannel.HEADER_SIZE;
        for (byte[] record : records) {
            length += record.length;
        }
        var bytes = ByteBuffer.allocate(length).putInt(0x424b4c47).putInt(JournalChannel.V6);
        bytes.position(JournalChannel.HEADER_SIZE);
        for (byte[] record : records) {
            bytes.put(record);
        }
        return bytes.array();
    }

    private static byte[] image(int version, int... words) {
        ByteBuffer bytes = ByteBuffer.allocate(512 + 4 * words.length).putInt(0x424b4c47).putInt(version);
        bytes.position(512);
        for (int word : words) {
            bytes.putInt(word);
        }
        return bytes.array();
    }

    private final class Fixture implements AutoCloseable {
        private final Path journal;
        private final Path secondJournal;
        private final List<Path> ledgers = new ArrayList<>();
        private final List<Journal> journals = new ArrayList<>();
        private final ServerConfiguration conf;
        private final LedgerDirsManager directories;

        private Fixture() throws Exception {
            Path first = temporary.newFolder().toPath();
            Path second = temporary.newFolder().toPath();
            journal = Files.createDirectory(first.resolve("current"));
            secondJournal = Files.createDirectory(second.resolve("current"));
            List<String> roots = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Path root = temporary.newFolder().toPath();
                roots.add(root.toString());
                ledgers.add(Files.createDirectory(root.resolve("current")));
            }
            conf = new ServerConfiguration().setJournalDirsName(new String[]{first.toString(), second.toString()})
                    .setLedgerDirNames(roots.toArray(String[]::new)).setJournalMaxMemorySizeMb(2)
                    .setJournalFormatVersionToWrite(6).setFileInfoFormatVersionToWrite(1)
                    .setJournalQueueSize(4).setJournalWriteBufferSizeKB(4).setMaxJournalSizeMB(1)
                    .setJournalBufferedWritesThreshold(1).setJournalMaxGroupWaitMSec(1)
                    .setJournalFlushWhenQueueEmpty(true);
            conf.setProperty("journalPreAllocSizeMB", 1);
            directories = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(.999f, .99f));
        }

        private Journal fresh() {
            Journal journal = Journal.forFresh(0, this.journal.toFile(), conf, directories,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT, null);
            journals.add(journal);
            return journal;
        }

        private Journal history() {
            Journal journal = Journal.forClassicHistory(0, this.journal.toFile(), conf, directories,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT, null);
            journals.add(journal);
            return journal;
        }

        private ClassicJournalRecovery reader() {
            return new ClassicJournalRecovery(journal, ledgers, "lastMark.0", 1024, 512);
        }

        private ClassicJournalRecovery inventory() {
            return ClassicJournalRecovery.forInventory(List.of(journal, secondJournal), ledgers, 1024, 512);
        }

        private void append(Journal journal, long id) throws Exception {
            var completed = new CountDownLatch(1);
            var code = new AtomicInteger(-1);
            var input = Unpooled.buffer(24).writeLong(7).writeLong(id).writeLong(0);
            try {
                journal.logAddEntry(7, id, input, false, (rc, l, e, address, context) -> {
                    code.set(rc);
                    completed.countDown();
                }, null);
            } finally {
                input.release();
            }
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertEquals(BookieProtocol.EOK, code.get());
        }

        @Override
        public void close() throws Exception {
            for (Journal journal : journals) {
                assertTrue(journal.closeForFresh().drained());
            }
        }
    }
}

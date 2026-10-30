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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.FreshClassicEntryLogDrainTest.ControlledFileChannel;
import org.apache.bookkeeper.bookie.storage.segment.ClassicRouteStartup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Filesystem ownership only; no startup permit, Bookie drain, registration or commissioning proof. */
public class FreshClassicRootsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private ServerConfiguration configuration() throws IOException {
        return new ServerConfiguration().setBookieId("fresh-local:3181")
                .setJournalDirName(temporary.newFolder("01-journal").getCanonicalPath())
                .setLedgerDirNames(new String[]{temporary.newFolder("02-ledger").getCanonicalPath()});
    }

    private Path ownedRoot(String directory) throws IOException {
        Path root = Path.of(directory).toAbsolutePath().normalize();
        assertTrue("Test path escapes its temporary root: " + root,
                root.startsWith(temporary.getRoot().toPath().toRealPath()));
        return root;
    }

    private Path marker(String directory) throws IOException {
        return ownedRoot(directory).resolve(ClassicRouteStartup.FRESH_LOCK_FILE_NAME);
    }

    /** Independent JVM opens the actual retained file, never the Java owner's bookkeeping. */
    public static final class LockProbe {
        public static void main(String[] arguments) throws Exception {
            try (var channel = FileChannel.open(Path.of(arguments[0]), StandardOpenOption.WRITE);
                    var lock = channel.tryLock()) {
                System.out.println(lock == null ? "HELD" : "AVAILABLE");
            }
        }
    }

    static void probe(Path marker, String expected) throws Exception {
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), LockProbe.class.getName(), marker.toString())
                .redirectErrorStream(true).start();
        try {
            assertTrue("Lock probe did not terminate", process.waitFor(5, TimeUnit.SECONDS));
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            assertEquals(output, 0, process.exitValue());
            assertEquals(expected, output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void emptyRootsHoldRealProcessLocksAndRetainTheirOneUseMarkersAfterClose() throws Exception {
        var conf = configuration();
        try (var roots = new FreshClassicRoots(conf)) {
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                assertFalse(Files.exists(marker(directory)));
            }
            roots.acquire();
            roots.verify(conf);
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                assertEquals(PosixFilePermissions.fromString("rw-------"),
                        Files.getPosixFilePermissions(marker(directory)));
                probe(marker(directory), "HELD");
            }
            assertThrows(IOException.class, roots::acquire);
            var changed = new ServerConfiguration(conf).setBookieId("other:3181");
            assertThrows(IOException.class, () -> roots.verify(changed));
            try (var second = new FreshClassicRoots(conf)) {
                assertThrows(IOException.class, second::acquire);
                probe(marker(conf.getJournalDirNames()[0]), "HELD");
            }
        }
        for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
            assertTrue(Files.exists(marker(directory)));
            probe(marker(directory), "AVAILABLE");
        }
        try (var restart = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, restart::acquire);
        }
    }

    @Test
    public void aliasesKeepTheirOriginalMembersAndEffectiveIndexFallbackUsesTheSameLease() throws Exception {
        var conf = configuration();
        Path alias = temporary.getRoot().toPath().resolve("journal-alias");
        Files.createSymbolicLink(alias, ownedRoot(conf.getJournalDirNames()[0]));
        conf.setJournalDirsName(new String[]{conf.getJournalDirNames()[0], alias.toString()});
        try (var roots = new FreshClassicRoots(conf)) {
            roots.acquire();
            roots.verify(conf);
            probe(marker(conf.getLedgerDirNames()[0]), "HELD");
            Files.delete(alias);
            Files.createSymbolicLink(alias, ownedRoot(conf.getLedgerDirNames()[0]));
            assertThrows(IOException.class, () -> roots.verify(conf));
            probe(marker(conf.getJournalDirNames()[0]), "HELD");
        }
    }

    @Test
    public void completeRootValidationRejectsHistoricalFilesAndMissingRootsBeforeCreatingAnyMarker() throws Exception {
        var conf = configuration();
        Path journalMarker = marker(conf.getJournalDirNames()[0]);
        Path ledger = ownedRoot(conf.getLedgerDirNames()[0]);
        Path historical = ledger.resolve("unknown-old-data");
        Files.write(historical, new byte[]{1});
        try (var roots = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, roots::acquire);
            assertFalse(Files.exists(journalMarker));
            assertEquals(1, Files.size(historical));
        }
        var missing = new ServerConfiguration(conf)
                .setLedgerDirNames(new String[]{ledger.resolve("missing").toString()});
        try (var roots = new FreshClassicRoots(missing)) {
            assertThrows(IOException.class, roots::acquire);
            assertFalse(Files.exists(ledger.resolve("missing")));
            assertFalse(Files.exists(journalMarker));
        }
    }

    @Test
    public void partialAcquisitionRetainsItsActualFirstLockUntilExplicitClose() throws Exception {
        var conf = configuration();
        Path ledger = ownedRoot(conf.getLedgerDirNames()[0]);
        var permissions = Files.getPosixFilePermissions(ledger);
        Files.setPosixFilePermissions(ledger, PosixFilePermissions.fromString("r-x------"));
        try (var roots = new FreshClassicRoots(conf)) {
            Assume.assumeFalse("Privilege bypasses this real permission failure", Files.isWritable(ledger));
            assertThrows(IOException.class, roots::acquire);
            assertThrows(IOException.class, () -> roots.verify(conf));
            probe(marker(conf.getJournalDirNames()[0]), "HELD");
            assertFalse(Files.exists(marker(ledger.toString())));
        } finally {
            Files.setPosixFilePermissions(ledger, permissions);
        }
        probe(marker(conf.getJournalDirNames()[0]), "AVAILABLE");
        try (var next = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, next::acquire);
        }
    }

    @Test
    public void samePathAndIdenticalMarkerCannotReplaceTheOriginalPhysicalRoot() throws Exception {
        var conf = configuration();
        Path journal = ownedRoot(conf.getJournalDirNames()[0]);
        Path moved = journal.resolveSibling("moved-journal");
        try (var roots = new FreshClassicRoots(conf)) {
            roots.acquire();
            Files.move(journal, moved);
            Files.createDirectory(journal);
            // Independently construct the identical empty marker. Opening/copying the held inode in
            // this process would close a second descriptor and drop its POSIX record lock.
            Files.createFile(marker(journal.toString()),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            assertEquals(0, Files.size(marker(moved.toString())));
            assertEquals(0, Files.size(marker(journal.toString())));
            assertThrows(IOException.class, () -> roots.verify(conf));
            probe(marker(moved.toString()), "HELD");
        }
        probe(marker(moved.toString()), "AVAILABLE");
        try (var next = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, next::acquire);
        }
    }

    @Test
    public void replacementMarkerCannotReuseTheOldFileLockEvenWithIdenticalBytesAndPermissions() throws Exception {
        var conf = configuration();
        Path marker = marker(conf.getJournalDirNames()[0]);
        Path retained = marker.resolveSibling("retained-original-marker");
        try (var roots = new FreshClassicRoots(conf)) {
            roots.acquire();
            Files.move(marker, retained);
            Files.createFile(marker,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            assertThrows(IOException.class, () -> roots.verify(conf));
            probe(retained, "HELD");
            probe(marker, "AVAILABLE");
        }
        probe(retained, "AVAILABLE");
    }

    @Test
    public void noAcquisitionCloseCreatesNothingAndNativeAbsenceCannotBypassRetainedFreshMarker() throws Exception {
        var conf = configuration();
        var unused = new FreshClassicRoots(conf);
        unused.close();
        unused.close();
        assertThrows(IOException.class, unused::acquire);
        assertFalse(Files.exists(marker(conf.getJournalDirNames()[0])));
        try (var roots = new FreshClassicRoots(conf)) {
            roots.acquire();
            var registration = mock(RegistrationManager.class);
            when(registration.readBookieRouteBinding(any())).thenReturn(BookieRouteBinding.ReadResult.status(
                    BookieRouteBinding.ReadStatus.ABSENT, "explicit unit fixture"));
            var failure = assertThrows(IOException.class, () -> ClassicRouteStartup.preflight(conf, registration));
            assertTrue(failure.getMessage().contains("fresh Classic owner marker"));
            probe(marker(conf.getJournalDirNames()[0]), "HELD");
        }
    }

    @Test
    public void firstNativeCloseFailureCannotBecomeSuccessOnTheSecondNoop() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        Object first = root(roots, 0);
        Object record = field(first, "channel");
        FileChannel actual = (FileChannel) field(record, "resource");
        var controlled = new ControlledFileChannel(actual);
        IOException failure = new IOException("marker native close failed");
        controlled.closeFailure = failure;
        setField(record, "resource", controlled);
        try {
            assertSame(failure, assertThrows(IOException.class, roots::close));
            assertFalse(controlled.isOpen());
            assertTrue(actual.isOpen());
            assertSame(failure, assertThrows(IOException.class, roots::close));
            assertEquals(1, controlled.closeAttempts.get());
            assertFalse(roots.closeForFresh().drained());
            assertSame(failure, roots.closeForFresh().closeFailure());
        } finally {
            actual.close();
            try {
                roots.close();
            } catch (IOException retainedFailure) {
                assertSame(failure, retainedFailure);
            }
        }
    }

    @Test
    public void actualAutomaticCloseWithoutTheOwnersResultStaysUnknown() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        FileChannel actual = (FileChannel) field(field(root(roots, 0), "channel"), "resource");
        try {
            Thread.currentThread().interrupt();
            assertThrows(ClosedByInterruptException.class,
                    () -> actual.write(ByteBuffer.wrap(new byte[]{0}), 0));
        } finally {
            Thread.interrupted();
        }
        assertFalse(actual.isOpen());
        try {
            IOException first = assertThrows(IOException.class, roots::close);
            assertSame(first, assertThrows(IOException.class, roots::close));
            assertFalse(roots.closeForFresh().drained());
            probe(marker(conf.getJournalDirNames()[0]), "AVAILABLE");
        } finally {
            try {
                roots.close();
            } catch (IOException retainedFailure) {
                // The actual fd is already closed; its unobserved original result remains unknown.
            }
        }
    }

    @Test(timeout = 30000)
    public void explicitReleaseFailureSurvivesSuccessfulChannelCloseAndIndependentRootCleanup() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        Object last = root(roots, 1);
        Object lockRecord = field(last, "lock");
        var lock = new ControlledFileLock((FileLock) field(lockRecord, "resource"));
        IOException failure = new IOException("explicit lock release failed");
        lock.failure = failure;
        setField(lockRecord, "resource", lock);
        Object channelRecord = field(last, "channel");
        var channel = new ControlledFileChannel((FileChannel) field(channelRecord, "resource"));
        setField(channelRecord, "resource", channel);
        Path ledgerMarker = marker(conf.getLedgerDirNames()[0]);
        try {
            var result = roots.closeForFresh();
            assertFalse(result.drained());
            assertSame(failure, result.closeFailure());
            assertTrue(result.releases().stream().anyMatch(r -> r.kind().equals("marker channel")
                    && r.path().equals(ledgerMarker)
                    && r.closeReturned() && r.failure() == null));
            assertFalse(channel.delegate.isOpen());
            assertFalse(lock.delegate.isValid());
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                probe(marker(directory), "AVAILABLE");
            }
            assertSame(failure, roots.closeForFresh().closeFailure());
            assertSame(failure, assertThrows(IOException.class, roots::close));
            assertSame(failure, assertThrows(IOException.class, roots::close));
            assertEquals(1, lock.attempts.get());
            assertEquals(1, channel.closeAttempts.get());
            assertEquals(0, failure.getSuppressed().length);
        } finally {
            roots.closeForFresh();
            channel.delegate.close();
        }
    }

    @Test(timeout = 30000)
    public void heldActualReleaseKeepsClosePendingAndRejectsReentrantSelfWait() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        Object record = field(root(roots, 1), "lock");
        var lock = new ControlledFileLock((FileLock) field(record, "resource"));
        setField(record, "resource", lock);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        lock.beforeRelease = () -> {
            assertThrows(IllegalStateException.class, roots::closeForFresh);
            entered.countDown();
            try {
                assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        var close = new FutureTask<>(roots::closeForFresh);
        Thread worker = new Thread(close);
        try {
            worker.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                probe(marker(directory), "HELD");
            }
            release.countDown();
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            worker.join(5000);
            assertFalse(worker.isAlive());
            assertTrue(roots.closeForFresh().drained());
            assertEquals(1, lock.attempts.get());
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                probe(marker(directory), "AVAILABLE");
            }
        } finally {
            release.countDown();
            worker.join(5000);
            roots.closeForFresh();
        }
    }

    @Test(timeout = 30000)
    public void failedDirectoryForceCanDrainResourcesWithoutRestoringFreshQualification() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        var controlled = new AtomicReference<ControlledFileChannel>();
        IOException failure = new IOException("directory force failed");
        try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
            open.when(() -> FileChannel.open(any(Path.class), eq(StandardOpenOption.READ)))
                    .thenAnswer(invocation -> {
                        var wrapper = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                        wrapper.beforeForce = () -> assertThrows(IllegalStateException.class, roots::closeForFresh);
                        wrapper.forceFailure = failure;
                        controlled.set(wrapper);
                        return wrapper;
                    });
            assertSame(failure, assertThrows(IOException.class, roots::acquire));
        }
        assertFalse(controlled.get().delegate.isOpen());
        assertEquals(1, controlled.get().closeAttempts.get());
        probe(marker(conf.getJournalDirNames()[0]), "HELD");
        var result = roots.closeForFresh();
        assertTrue(result.drained());
        assertSame(failure, result.acquisitionFailure());
        assertTrue(result.releases().stream().allMatch(r -> r.failure() == null));
        assertTrue(((List<?>) field(roots, "auxiliary")).isEmpty());
        probe(marker(conf.getJournalDirNames()[0]), "AVAILABLE");
        assertThrows(IOException.class, () -> roots.verify(conf));
        assertThrows(IOException.class, roots::acquire);
        try (var next = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, next::acquire);
        }
    }

    @Test(timeout = 30000)
    public void directorySyncCloseFailureRetainsItsFirstNativeResultAndPartialOwner() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        var controlled = new AtomicReference<ControlledFileChannel>();
        IOException failure = new IOException("directory sync close failed");
        try {
            try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
                open.when(() -> FileChannel.open(any(Path.class), eq(StandardOpenOption.READ)))
                        .thenAnswer(invocation -> {
                            var wrapper = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                            wrapper.closeFailure = failure;
                            controlled.set(wrapper);
                            return wrapper;
                        });
                assertSame(failure, assertThrows(IOException.class, roots::acquire));
            }
            assertTrue(controlled.get().delegate.isOpen());
            probe(marker(conf.getJournalDirNames()[0]), "HELD");
            var result = roots.closeForFresh();
            assertFalse(result.drained());
            assertSame(failure, result.acquisitionFailure());
            assertSame(failure, result.closeFailure());
            assertTrue(result.releases().stream().anyMatch(r -> r.kind().equals("directory sync channel")
                    && r.closeReturned() && r.failure() == failure));
            assertEquals(1, controlled.get().closeAttempts.get());
            probe(marker(conf.getJournalDirNames()[0]), "AVAILABLE");
            controlled.get().delegate.close();
            assertFalse(roots.closeForFresh().drained());
            assertSame(failure, assertThrows(IOException.class, roots::close));
            assertEquals(1, controlled.get().closeAttempts.get());
        } finally {
            roots.closeForFresh();
            if (controlled.get() != null) {
                controlled.get().delegate.close();
            }
        }
    }

    @Test(timeout = 30000)
    public void listingOperationAndCloseFailuresStaySeparateWithoutDuplicateSuppression() throws Exception {
        var conf = configuration();
        Path journal = ownedRoot(conf.getJournalDirNames()[0]);
        Files.write(journal.resolve("historical"), new byte[]{1});
        var roots = new FreshClassicRoots(conf);
        IOException failure = new IOException("listing close failed after native close");
        var closes = new AtomicInteger();
        try (var listing = mockStatic(Files.class, CALLS_REAL_METHODS)) {
            listing.when(() -> Files.newDirectoryStream(any(Path.class))).thenAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                DirectoryStream<Path> actual = (DirectoryStream<Path>) invocation.callRealMethod();
                return new DirectoryStream<Path>() {
                    @Override
                    public Iterator<Path> iterator() {
                        return actual.iterator();
                    }

                    @Override
                    public void close() throws IOException {
                        closes.incrementAndGet();
                        actual.close();
                        throw failure;
                    }
                };
            });
            IOException operation = assertThrows(IOException.class, roots::acquire);
            assertTrue(operation.getMessage().contains("historical or unclassified"));
            assertEquals(1, operation.getSuppressed().length);
            assertSame(failure, operation.getSuppressed()[0]);
            var result = roots.closeForFresh();
            assertFalse(result.drained());
            assertSame(operation, result.acquisitionFailure());
            assertSame(failure, result.closeFailure());
            assertSame(failure, roots.closeForFresh().closeFailure());
            assertEquals(1, closes.get());
            assertEquals(1, operation.getSuppressed().length);
            assertFalse(Files.exists(marker(conf.getJournalDirNames()[0])));
        }
    }

    private static final class ControlledFileLock extends FileLock {
        final FileLock delegate;
        final AtomicInteger attempts = new AtomicInteger();
        IOException failure;
        Runnable beforeRelease;

        ControlledFileLock(FileLock delegate) {
            super(delegate.channel(), delegate.position(), delegate.size(), delegate.isShared());
            this.delegate = delegate;
        }

        @Override
        public boolean isValid() {
            return delegate.isValid();
        }

        @Override
        public void release() throws IOException {
            attempts.incrementAndGet();
            if (beforeRelease != null) {
                beforeRelease.run();
            }
            if (failure != null) {
                throw failure;
            }
            delegate.release();
        }
    }

    @Test(timeout = 30000)
    public void autoCloseableRetainsEveryRootsCloseFailureWithoutRepeatingSuppression() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        var journalRecord = field(root(roots, 0), "channel");
        var ledgerRecord = field(root(roots, 1), "channel");
        var journal = new ControlledFileChannel((FileChannel) field(journalRecord, "resource"));
        var ledger = new ControlledFileChannel((FileChannel) field(ledgerRecord, "resource"));
        IOException journalFailure = new IOException("journal root close failed");
        IOException ledgerFailure = new IOException("ledger root close failed");
        journal.closeFailure = journalFailure;
        ledger.closeFailure = ledgerFailure;
        setField(journalRecord, "resource", journal);
        setField(ledgerRecord, "resource", ledger);
        try {
            assertSame(ledgerFailure, assertThrows(IOException.class, roots::close));
            assertEquals(1, ledgerFailure.getSuppressed().length);
            assertSame(journalFailure, ledgerFailure.getSuppressed()[0]);
            assertSame(ledgerFailure, assertThrows(IOException.class, roots::close));
            assertEquals(1, ledgerFailure.getSuppressed().length);
            assertFalse(roots.closeForFresh().drained());
            assertEquals(2, roots.closeForFresh().releases().stream().filter(r -> r.failure() != null).count());
            assertEquals(1, journal.closeAttempts.get());
            assertEquals(1, ledger.closeAttempts.get());
        } finally {
            roots.closeForFresh();
            journal.delegate.close();
            ledger.delegate.close();
        }
    }

    private static Object root(FreshClassicRoots roots, int index) throws Exception {
        return ((List<?>) field(roots, "roots")).get(index);
    }

    @Test(timeout = 30000)
    public void currentDirectoriesAreCreatedOnceUnderTheCompleteOriginalRootLease() throws Exception {
        var conf = configuration();
        try (var roots = new FreshClassicRoots(conf)) {
            assertThrows(IOException.class, roots::prepareCurrentDirectories);
            roots.acquire();
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                assertFalse(Files.exists(ownedRoot(directory).resolve("current")));
                probe(marker(directory), "HELD");
            }
            roots.prepareCurrentDirectories();
            roots.verify(conf);
            for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
                assertTrue(Files.isDirectory(ownedRoot(directory).resolve("current")));
                assertEquals(0, ownedRoot(directory).resolve("current").toFile().list().length);
                probe(marker(directory), "HELD");
            }
            assertThrows(IOException.class, roots::prepareCurrentDirectories);
        }
        assertTrue(Files.isDirectory(ownedRoot(conf.getJournalDirNames()[0]).resolve("current")));
        assertTrue(Files.exists(marker(conf.getJournalDirNames()[0])));
    }

    @Test(timeout = 30000)
    public void partialCurrentPreparationRetainsBothRootLocksAndItsDirectorySyncOwner() throws Exception {
        var conf = configuration();
        var roots = new FreshClassicRoots(conf);
        roots.acquire();
        var controlled = new AtomicReference<ControlledFileChannel>();
        IOException failure = new IOException("new current parent directory force failed");
        try (var open = mockStatic(FileChannel.class, CALLS_REAL_METHODS)) {
            open.when(() -> FileChannel.open(any(Path.class), eq(StandardOpenOption.READ)))
                    .thenAnswer(invocation -> {
                        var wrapper = new ControlledFileChannel((FileChannel) invocation.callRealMethod());
                        wrapper.forceFailure = failure;
                        controlled.set(wrapper);
                        return wrapper;
                    });
            assertSame(failure, assertThrows(IOException.class, roots::prepareCurrentDirectories));
        }
        assertFalse(controlled.get().delegate.isOpen());
        assertEquals(1, controlled.get().closeAttempts.get());
        assertTrue(Files.isDirectory(ownedRoot(conf.getJournalDirNames()[0]).resolve("current")));
        assertFalse(Files.exists(ownedRoot(conf.getLedgerDirNames()[0]).resolve("current")));
        for (String directory : List.of(conf.getJournalDirNames()[0], conf.getLedgerDirNames()[0])) {
            probe(marker(directory), "HELD");
        }
        assertThrows(IOException.class, () -> roots.verify(conf));
        var result = roots.closeForFresh();
        assertTrue(result.drained());
        assertSame(failure, result.acquisitionFailure());
        assertThrows(IOException.class, roots::prepareCurrentDirectories);
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private static void setField(Object owner, String name, Object value) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(owner, value);
    }
}

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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

    private static void probe(Path marker, String expected) throws Exception {
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
}

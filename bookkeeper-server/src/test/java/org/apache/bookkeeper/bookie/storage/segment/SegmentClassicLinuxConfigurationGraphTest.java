/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Alias;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Loading;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Requirement;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual protected temporary files/directories only; cannot issue Installed, START or Linux qualification. */
public class SegmentClassicLinuxConfigurationGraphTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private Path root;
    private long uid;

    private static void write(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--"));
    }

    @Before
    public void fixture() throws Exception {
        uid = new UnixSystem().getUid();
        Path parent = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwx------"));
        root = Files.createDirectory(parent.resolve("trust"));
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        write(root.resolve("classic-1.recipe"), SegmentClassicLinuxDeploymentTest.vector("classic-launch-recipe-v1"));
        write(root.resolve("issuer-1.ed25519"), HexFormat.of().parseHex(
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"));
    }

    private Path directory(String path) throws IOException {
        Path result = Files.createDirectories(root.resolve(path));
        Files.setPosixFilePermissions(result, PosixFilePermissions.fromString("rwx------"));
        return result;
    }

    private SegmentClassicLinuxDeployment.File config(String path, String text) throws IOException {
        return config(path, text.getBytes(StandardCharsets.UTF_8));
    }

    private SegmentClassicLinuxDeployment.File config(String path, byte[] data) throws IOException {
        Path file = root.resolve(path);
        write(file, data);
        return new SegmentClassicLinuxDeployment.File(file, SegmentClassicLinuxDeployment.Role.LOADER_CONFIG,
                data.length, SegmentClassicHandoff.digest(data));
    }

    private SegmentClassicLaunchTrust owner() throws IOException {
        var result = SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", uid);
        result.initialize();
        return result;
    }

    private Loading loading(Requirement entry, List<Alias> aliases) {
        return new Loading(0, 0, root.resolve("issuer-1.ed25519"),
                aliases.stream().sorted(Comparator.comparing(a -> a.link().toString())).toList(), List.of(),
                List.of(entry), List.of(), List.of());
    }

    private SegmentClassicLaunchTrust.LinuxConfigurationGraph observe(List<SegmentClassicLinuxDeployment.File> files,
            List<Alias> aliases) throws Exception {
        var entry = new Requirement(files.get(0).path(), 0);
        var owner = owner();
        try {
            var result = owner.verifyLinuxConfigurationGraph(loading(entry, aliases), files, entry,
                    SegmentClassicLinuxElfTest.deadline());
            assertThrows(IOException.class, owner::installed);
            return result;
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    private IOException rejected(List<SegmentClassicLinuxDeployment.File> files, List<Alias> aliases)
            throws Exception {
        var entry = new Requirement(files.get(0).path(), 0);
        var owner = owner();
        try {
            var failure = assertThrows(IOException.class, () -> owner.verifyLinuxConfigurationGraph(
                    loading(entry, aliases), files, entry, SegmentClassicLinuxElfTest.deadline()));
            assertSame(failure, owner.closeAndDrain().launcherFailure());
            return failure;
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void graphPreservesPatternWordOrderCompleteMatchesRelativeNamesAndRepeatedReads() throws Exception {
        directory("parts");
        var first = config("root.conf", "include parts/[ab].conf parts/a.conf\n");
        var a = config("parts/a.conf", "/library/a\n");
        var b = config("parts/b.conf", "include ../tail.conf\n");
        var tail = config("tail.conf", "/library/tail\n");
        write(root.resolve("parts/unrelated.txt"), new byte[]{7});
        var result = observe(List.of(first, a, b, tail), List.of());
        assertFalse(result.absent());
        assertEquals(List.of(0, 1, 2, 3, 1), result.files().stream().map(f -> f.file()).toList());
        assertEquals(root + "/parts/../tail.conf", result.files().get(3).logical());
        assertEquals(List.of(root + "/parts/a.conf", root + "/parts/b.conf"), result.includes().get(0).matches());
        assertEquals(List.of(0, 2, 0), result.includes().stream().map(i -> i.source()).toList());
        assertEquals(List.of(0, 0, 1), result.includes().stream().map(i -> i.word()).toList());
        assertEquals(new SegmentClassicLinuxConfiguration.Usage(5, 3,
                (int) (first.length() + 2 * a.length() + b.length() + tail.length())), result.usage());
        assertEquals(List.of("a.conf", "b.conf", "unrelated.txt"), result.directories().get(0).members());
        assertThrows(UnsupportedOperationException.class, () -> result.files().clear());
        assertThrows(UnsupportedOperationException.class, () -> result.includes().get(0).matches().clear());
    }

    @Test
    public void activePhysicalFileCanReappearUnderADifferentLogicalIncludeBase() throws Exception {
        directory("a");
        directory("b");
        var first = config("root.conf", "include a/config.conf\n");
        var shared = config("shared.conf", "include leaf.conf\n");
        var a = config("a/leaf.conf", "include ../b/config.conf\n");
        var b = config("b/leaf.conf", "/library/b\n");
        Path left = Files.createSymbolicLink(root.resolve("a/config.conf"), Path.of("../shared.conf"));
        Path right = Files.createSymbolicLink(root.resolve("b/config.conf"), Path.of("../shared.conf"));
        var result = observe(List.of(first, shared, a, b),
                List.of(new Alias(left, "../shared.conf"), new Alias(right, "../shared.conf")));
        assertEquals(List.of(0, 1, 2, 1, 3), result.files().stream().map(f -> f.file()).toList());
        assertEquals(root + "/a/../b/config.conf", result.files().get(3).logical());
        assertEquals(root + "/a/../b/leaf.conf", result.files().get(4).logical());
        assertEquals(5, result.usage().files());
    }

    @Test
    public void missingRootRequiresExplicitMembershipAndActualProtectedAbsence() throws Exception {
        var entry = new Requirement(root.resolve("absent.conf"), -1);
        var owner = owner();
        try {
            var graph = owner.verifyLinuxConfigurationGraph(loading(entry, List.of()), List.of(), entry,
                    SegmentClassicLinuxElfTest.deadline());
            assertTrue(graph.absent());
            assertEquals(new SegmentClassicLinuxConfiguration.Usage(1, 0, 0), graph.usage());
            assertTrue(graph.files().isEmpty());
            var missing = new Requirement(root.resolve("different.conf"), -1);
            assertThrows(IOException.class, () -> owner.verifyLinuxConfigurationGraph(loading(entry, List.of()),
                    List.of(), missing, SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void knownNondirectoryBranchesAndActualZeroMatchesDoNotEraseFileDotDot() throws Exception {
        var first = config("root.conf", "include absent.conf file/child.conf file/../ignored.conf\n");
        write(root.resolve("file"), new byte[]{7});
        var result = observe(List.of(first), List.of());
        assertEquals(1, result.files().size());
        assertEquals(3, result.includes().size());
        assertTrue(result.includes().stream().allMatch(i -> i.matches().isEmpty()));
        assertEquals(3, result.usage().patterns());
    }

    @Test
    public void danglingLiteralAndUnapprovedMatchedConfigurationCannotTurnIntoEmptySuccess() throws Exception {
        var first = config("root.conf", "include dangling.conf\n");
        Path alias = Files.createSymbolicLink(root.resolve("dangling.conf"), Path.of("not-present"));
        rejected(List.of(first), List.of());
        rejected(List.of(first), List.of(new Alias(alias, "not-present")));
        var changed = config("root.conf", "include unapproved.conf\n");
        write(root.resolve("unapproved.conf"), new byte[]{7});
        try (var calls = mockStatic(FileChannel.class, invocation -> {
            if (invocation.getMethod().getName().equals("open")) {
                assertFalse(root.resolve("unapproved.conf").equals(invocation.getArgument(0)));
            }
            return invocation.callRealMethod();
        })) {
            assertTrue(rejected(List.of(changed), List.of()).getMessage().contains("approved configuration"));
        }
    }

    @Test
    public void virtualDotCandidatesParticipateButNeverEnterThePhysicalMemberList() throws Exception {
        var first = config("root.conf", "include .?.conf\n");
        var hidden = config(".a.conf", "/hidden/library\n");
        var graph = observe(List.of(first, hidden), List.of());
        assertEquals(List.of(0, 1), graph.files().stream().map(f -> f.file()).toList());
        assertFalse(graph.directories().get(0).members().contains("."));
        assertFalse(graph.directories().get(0).members().contains(".."));
        for (String pattern : List.of(".*", ".?")) {
            var rootFile = config("root.conf", "include " + pattern + "\n");
            rejected(List.of(rootFile, hidden), List.of());
        }
    }

    @Test
    public void dirnameMagicFiltersRegularFilesAndEscapesRetainTheReturnedLogicalNames() throws Exception {
        directory("groups/a");
        directory("groups/b");
        var first = config("root.conf", "include groups/*/??.conf quoted\\?.conf\n");
        var a = config("groups/a/01.conf", "/a/one\n");
        var b = config("groups/a/02.conf", "/a/two\n");
        var c = config("groups/b/01.conf", "/b/one\n");
        var quoted = config("quoted?.conf", "/quoted\n");
        write(root.resolve("groups/regular"), new byte[]{7});
        var graph = observe(List.of(first, a, b, c, quoted), List.of());
        assertEquals(List.of(0, 1, 2, 3, 4), graph.files().stream().map(f -> f.file()).toList());
        assertEquals(root + "/quoted?.conf", graph.files().get(4).logical());
        var escaped = config("root.conf", "include groups\\/a/01.conf\n");
        assertEquals(root + "/groups/a/01.conf", observe(List.of(escaped, a), List.of()).files().get(1).logical());
    }

    @Test
    public void trailingMarkPreservesLiteralRegularNamesAndTheMagicDirectoryOnlyBranch() throws Exception {
        var first = config("root.conf", "include leaf.conf/\n");
        var leaf = config("leaf.conf", "/library\n");
        var graph = observe(List.of(first, leaf), List.of());
        assertEquals(List.of(0, 1), graph.files().stream().map(f -> f.file()).toList());
        assertEquals("leaf.conf/", graph.includes().get(0).pattern());
        assertEquals(List.of(root + "/leaf.conf"), graph.includes().get(0).matches());
        Path alias = Files.createSymbolicLink(root.resolve("alias.conf"), Path.of("leaf.conf"));
        var throughAlias = config("root.conf", "include alias.conf/\n");
        assertEquals(root + "/alias.conf", observe(List.of(throughAlias, leaf),
                List.of(new Alias(alias, "leaf.conf"))).files().get(1).logical());
        var magic = config("root.conf", "include *.conf/\n");
        var empty = observe(List.of(magic, leaf), List.of(new Alias(alias, "leaf.conf")));
        assertEquals(1, empty.files().size());
        assertTrue(empty.includes().get(0).matches().isEmpty());
    }

    @Test
    public void activeCycleAndTheThirtyThirdContextAreRejectedWhileThirtyTwoIsAccepted() throws Exception {
        var cycle = config("cycle.conf", "include cycle.conf\n");
        assertTrue(rejected(List.of(cycle), List.of()).getMessage().contains("cycle"));
        var chain = new ArrayList<SegmentClassicLinuxDeployment.File>();
        for (int i = 0; i < 32; i++) {
            chain.add(config(String.format("c%02d.conf", i),
                    i == 31 ? "" : String.format("include c%02d.conf\n", i + 1)));
        }
        assertEquals(32, observe(chain, List.of()).usage().files());
        chain.set(31, config("c31.conf", "include c32.conf\n"));
        chain.add(config("c32.conf", ""));
        assertTrue(rejected(chain, List.of()).getMessage().contains("active-context capacity"));
    }

    @Test
    public void repeatedFilesConsumeFullCumulativeBytesBeforeTheNextPayloadOpen() throws Exception {
        var first = config("root.conf", "include " + "big.conf ".repeat(17) + "\n");
        byte[] content = new byte[SegmentClassicLinuxConfiguration.MAX_BYTES];
        java.util.Arrays.fill(content, (byte) 'x');
        content[0] = '#';
        var big = config("big.conf", content);
        var opens = new AtomicInteger();
        try (var calls = mockStatic(FileChannel.class, invocation -> {
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && big.path().equals(invocation.getArgument(0))) {
                opens.incrementAndGet();
            }
            return invocation.callRealMethod();
        })) {
            assertTrue(rejected(List.of(first, big), List.of()).getMessage().contains("file-byte capacity"));
        }
        assertEquals(15, opens.get());
    }

    @Test
    public void stickyExistingLiteralIsUsableButAbsenceAndEnumerationRemainUntrusted() throws Exception {
        Path sticky = directory("sticky");
        Files.setAttribute(sticky, "unix:mode", 01777);
        var first = config("root.conf", "include sticky/held.conf\n");
        var held = config("sticky/held.conf", "/library\n");
        assertEquals(2, observe(List.of(first, held), List.of()).files().size());
        var absent = config("root.conf", "include sticky/missing.conf\n");
        assertTrue(rejected(List.of(absent, held), List.of()).getMessage().contains("absence parent"));
        var magic = config("root.conf", "include sticky/*.conf\n");
        assertTrue(rejected(List.of(magic, held), List.of()).getMessage().contains("protected nonwritable"));
        assertEquals(01777, ((Number) Files.getAttribute(sticky, "unix:mode")).intValue() & 07777);
    }

    @Test
    public void oversizedGraphConfigurationRejectsBeforeAnyTargetMetadataIo() throws Exception {
        var file = new SegmentClassicLinuxDeployment.File(root.resolve("not-opened.conf"),
                SegmentClassicLinuxDeployment.Role.LOADER_CONFIG, SegmentClassicLinuxConfiguration.MAX_BYTES + 1,
                SegmentClassicHandoff.digest(new byte[0]));
        var owner = owner();
        var entry = new Requirement(file.path(), 0);
        try (var calls = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Oversized graph configuration reached target metadata I/O");
        })) {
            var failure = assertThrows(IOException.class, () -> owner.verifyLinuxConfigurationGraph(
                    loading(entry, List.of()), List.of(file), entry, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("configuration length"));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void graphBodyOwnsActualDirectoryIoAndExternalCloseWaitsWithoutCancellingIt() throws Exception {
        Path parts = directory("parts");
        var first = config("root.conf", "include parts/*.conf\n");
        var child = config("parts/a.conf", "/library\n");
        var entry = new Requirement(first.path(), 0);
        var owner = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var once = new AtomicBoolean();
        var closes = new AtomicInteger();
        var read = new FutureTask<>(() -> {
            try (var calls = mockStatic(Files.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("newDirectoryStream")
                        && parts.equals(invocation.getArgument(0))) {
                    @SuppressWarnings("unchecked")
                    DirectoryStream<Path> original = (DirectoryStream<Path>) actual;
                    return new DirectoryStream<Path>() {
                        @Override
                        public java.util.Iterator<Path> iterator() {
                            if (once.compareAndSet(false, true)) {
                                assertThrows(IllegalStateException.class, owner::closeAndDrain);
                                entered.countDown();
                                try {
                                    assertTrue(release.await(5, TimeUnit.SECONDS));
                                } catch (InterruptedException failure) {
                                    Thread.currentThread().interrupt();
                                    throw new IllegalStateException(failure);
                                }
                            }
                            return original.iterator();
                        }

                        @Override
                        public void close() throws IOException {
                            closes.incrementAndGet();
                            original.close();
                        }
                    };
                }
                return actual;
            })) {
                return owner.verifyLinuxConfigurationGraph(loading(entry, List.of()), List.of(first, child), entry,
                        SegmentClassicLinuxElfTest.deadline());
            }
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
            assertEquals(0, closes.get());
            release.countDown();
            assertEquals(2, read.get(10, TimeUnit.SECONDS).files().size());
            assertTrue(close.get(10, TimeUnit.SECONDS).drained());
            assertEquals(2, closes.get());
        } finally {
            release.countDown();
            reader.join(5000);
            closer.join(5000);
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    @Test
    public void completeDirectoryChangesBetweenPassesRejectEvenWhenNewNamesDoNotMatch() throws Exception {
        directory("parts");
        var first = config("root.conf", "include parts/*.conf\n");
        var child = config("parts/a.conf", "/a\n");
        var roots = new AtomicInteger();
        try (var calls = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && first.path().equals(invocation.getArgument(0)) && roots.incrementAndGet() == 2) {
                write(root.resolve("parts/unrelated.txt"), new byte[]{7});
            }
            return actual;
        })) {
            assertTrue(rejected(List.of(first, child), List.of()).getMessage().contains("changed between"));
        }
        assertEquals(2, roots.get());
    }
}

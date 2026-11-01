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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual file/ACL fixture with a chosen owner only. It cannot produce installed trust or a startup permit. */
public class SegmentClassicLaunchTrustTest {
    private static final byte[] PUBLIC = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private Path root;
    private Path recipe;
    private Path key;
    private long uid;

    private static byte[] vector() throws IOException {
        try (var input = SegmentClassicLaunchTrustTest.class.getResourceAsStream(
                "/unified-wal-segment/classic-launch-recipe-v1.hex")) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }

    private static void write(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--"));
    }

    private static Path rawLink(Path path, String target) throws Exception {
        // Path.of removes trailing '/', so use the host symlink command to retain those actual target bytes.
        Process process = new ProcessBuilder("/bin/ln", "-s", target, path.toString())
                .redirectErrorStream(true).start();
        String output;
        try (var stream = process.getInputStream()) {
            output = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertEquals(output, 0, process.waitFor());
        assertEquals(target, Files.readSymbolicLink(path).toString());
        return path;
    }

    @Before
    public void fixture() throws Exception {
        uid = new UnixSystem().getUid();
        Path parent = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwx------"));
        root = Files.createDirectory(parent.resolve("trust"));
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        recipe = root.resolve("classic-1.recipe");
        key = root.resolve("issuer-1.ed25519");
        write(recipe, vector());
        write(key, PUBLIC);
    }

    private SegmentClassicLaunchTrust owner() {
        return SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", uid);
    }

    private Path launcherDirectory() throws Exception {
        Path directory = Files.createDirectory(root.resolve("launcher"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        return directory;
    }

    private static SegmentClassicLinuxDeployment.Loading loading(Path interpreter,
            List<SegmentClassicLinuxDeployment.Alias> aliases,
            List<SegmentClassicLinuxDeployment.Directory> directories,
            List<SegmentClassicLinuxDeployment.Requirement> configuration) {
        return new SegmentClassicLinuxDeployment.Loading(0, 0, interpreter,
                aliases.stream().sorted(Comparator.comparing(a -> a.link().toString())).toList(),
                directories.stream().sorted(Comparator.comparing(d -> d.path().toString())).toList(),
                configuration.stream().sorted(Comparator.comparing(c -> c.path().toString())).toList(),
                List.of(), List.of());
    }

    @Test
    public void loadingDirectoriesReturnSortedActualNamesIncludingUnrelatedMembersWithoutFollowingThem()
            throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
        write(directory.resolve("aaa.so"), new byte[]{7});
        write(directory.resolve("公钥"), new byte[]{8});
        write(directory.resolve("x%#?.so"), new byte[]{9});
        write(directory.resolve("\ue000"), new byte[]{10});
        write(directory.resolve("😀"), new byte[]{11});
        Files.createDirectory(directory.resolve("subdir"));
        Files.createSymbolicLink(directory.resolve("dangling"), Path.of("not-present"));
        Path first = Files.createSymbolicLink(root.resolve("first"), Path.of("physical"));
        Path second = Files.createSymbolicLink(root.resolve("second"), Path.of("physical"));
        Path missing = root.resolve("absent");
        var input = loading(key, List.of(new SegmentClassicLinuxDeployment.Alias(first, "physical"),
                new SegmentClassicLinuxDeployment.Alias(second, "physical")), List.of(
                new SegmentClassicLinuxDeployment.Directory(directory, true),
                new SegmentClassicLinuxDeployment.Directory(first, true),
                new SegmentClassicLinuxDeployment.Directory(second, true),
                new SegmentClassicLinuxDeployment.Directory(missing, false)), List.of());
        var owner = owner();
        owner.initialize();
        var result = owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline());
        assertEquals(4, result.size());
        for (var observed : result) {
            if (observed.path().entry().equals(missing)) {
                assertEquals(missing, observed.path().absentAt());
                assertTrue(observed.members().isEmpty());
                assertTrue(observed.matchingMembers("*", SegmentClassicLinuxElfTest.deadline()).isEmpty());
            } else {
                assertEquals(directory, observed.path().resolved());
                assertEquals(List.of("aaa.so", "dangling", "subdir", "x%#?.so", "公钥", "😀", "\ue000"),
                        observed.members());
                assertEquals(List.of("aaa.so", "x%#?.so"),
                        observed.matchingMembers("*.so", SegmentClassicLinuxElfTest.deadline()));
                assertEquals(List.of("x%#?.so"),
                        observed.matchingMembers("x%#\\?.so", SegmentClassicLinuxElfTest.deadline()));
                assertEquals(List.of("aaa.so", "dangling", "subdir", "x%#?.so", "公钥", "\ue000", "😀"),
                        observed.matchingMembers("*", SegmentClassicLinuxElfTest.deadline()));
                assertThrows(UnsupportedOperationException.class, () -> observed.members().add("new"));
            }
        }
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(directory.resolve("aaa.so")));
        assertTrue(Files.isSymbolicLink(directory.resolve("dangling")));
    }

    @Test
    public void loadingDirectoryReplacementAfterOpenRejectsTheOriginalIteratorResult() throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        write(directory.resolve("original.so"), new byte[]{7});
        var input = loading(key, List.of(), List.of(
                new SegmentClassicLinuxDeployment.Directory(directory, true)), List.of());
        var owner = owner();
        owner.initialize();
        try (var calls = mockStatic(Files.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("newDirectoryStream")
                    && directory.equals(invocation.getArgument(0))) {
                Files.move(directory, root.resolve("original-directory"));
                Files.createDirectory(directory);
            }
            return actual;
        })) {
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline()));
        }
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(root.resolve("original-directory/original.so")));
        assertTrue(Files.isDirectory(directory));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadingDirectoryFirstCloseFailureRetainsOriginalIteratorWithoutRetry() throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        var input = loading(key, List.of(), List.of(
                new SegmentClassicLinuxDeployment.Directory(directory, true)), List.of());
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<DirectoryStream<Path>>();
        var closes = new AtomicInteger();
        IOException failure = new IOException("loading iterator original first close");
        try (var calls = mockStatic(Files.class, invocation -> {
            Object value = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("newDirectoryStream")
                    && directory.equals(invocation.getArgument(0))) {
                DirectoryStream<Path> actual = (DirectoryStream<Path>) value;
                captured.set(actual);
                return new DirectoryStream<Path>() {
                    @Override
                    public java.util.Iterator<Path> iterator() {
                        return actual.iterator();
                    }

                    @Override
                    public void close() throws IOException {
                        closes.incrementAndGet();
                        throw failure;
                    }
                };
            }
            return value;
        })) {
            assertSame(failure, assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline())));
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertEquals(1, result.retainedAuxiliaryReads());
                assertSame(failure, result.auxiliaryCloseFailure());
                assertSame(failure, result.launcherFailure());
            }
            assertEquals(1, closes.get());
        } finally {
            captured.get().close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rawFilenameVectorRejectsReplacementDecodingAndStillClosesOriginalDirectory() throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        Path raw = Path.of(java.net.URI.create(directory.toUri().toASCIIString() + "%FF.conf"));
        var input = loading(key, List.of(), List.of(
                new SegmentClassicLinuxDeployment.Directory(directory, true)), List.of());
        var owner = owner();
        owner.initialize();
        var closes = new AtomicInteger();
        try (var calls = mockStatic(Files.class, invocation -> {
            Object value = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("newDirectoryStream")) {
                DirectoryStream<Path> actual = (DirectoryStream<Path>) value;
                return new DirectoryStream<Path>() {
                    @Override
                    public java.util.Iterator<Path> iterator() {
                        // A raw native-name vector, independent of whether this host permits creating that name.
                        return List.of(raw).iterator();
                    }

                    @Override
                    public void close() throws IOException {
                        closes.incrementAndGet();
                        actual.close();
                    }
                };
            }
            return value;
        })) {
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline()));
        }
        assertEquals(1, closes.get());
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadingDirectoryNameAndCountBudgetsCoverAllPhysicalDirectoriesAndCloseOverflowIterators()
            throws Exception {
        Path first = Files.createDirectory(root.resolve("first-directory"));
        Path second = Files.createDirectory(root.resolve("second-directory"));
        var input = loading(key, List.of(), List.of(
                new SegmentClassicLinuxDeployment.Directory(first, true),
                new SegmentClassicLinuxDeployment.Directory(second, true)), List.of());
        for (boolean bytes : new boolean[]{true, false}) {
            var owner = owner();
            owner.initialize();
            var closes = new AtomicInteger();
            try (var calls = mockStatic(Files.class, invocation -> {
                Object value = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("newDirectoryStream")) {
                    Path directory = invocation.getArgument(0);
                    DirectoryStream<Path> actual = (DirectoryStream<Path>) value;
                    return new DirectoryStream<Path>() {
                        @Override
                        public java.util.Iterator<Path> iterator() {
                            // Capacity fault only: no synthesized member is accepted as an existing/approved file.
                            return java.util.stream.IntStream.range(0, bytes ? 32769 : 131073)
                                    .mapToObj(i -> directory.resolve((bytes ? "x".repeat(1020) : "x") + i)).iterator();
                        }

                        @Override
                        public void close() throws IOException {
                            closes.incrementAndGet();
                            actual.close();
                        }
                    };
                }
                return value;
            })) {
                var failure = assertThrows(IOException.class,
                        () -> owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(failure.getMessage().contains("capacity"));
            }
            assertTrue(owner.closeAndDrain().drained());
            assertEquals(2, closes.get());
        }
    }

    @Test(timeout = 30000)
    public void loadingDirectoryOpenBodyOwnsExternalCloseAndCannotDrainItself() throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        var input = loading(key, List.of(), List.of(
                new SegmentClassicLinuxDeployment.Directory(directory, true)), List.of());
        var owner = owner();
        owner.initialize();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var read = new FutureTask<>(() -> {
            try (var calls = mockStatic(Files.class, invocation -> {
                Object value = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("newDirectoryStream")) {
                    assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return value;
            })) {
                return owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline());
            }
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
            release.countDown();
            assertEquals(1, read.get(5, TimeUnit.SECONDS).size());
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    /** External filename-byte fixture; standard DirectoryStream owns the real original native iterator. */
    public static final class DirectoryProbe {
        public static void main(String[] args) throws Exception {
            var fixture = new SegmentClassicLaunchTrustTest();
            fixture.temporary.create();
            try {
                fixture.fixture();
                var owner = fixture.owner();
                owner.initialize();
                var input = loading(fixture.key, List.of(), List.of(
                        new SegmentClassicLinuxDeployment.Directory(Path.of(args[0]), true)), List.of());
                assertThrows(IOException.class,
                        () -> owner.verifyLinuxLoadingDirectories(input, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(owner.closeAndDrain().drained());
            } finally {
                fixture.temporary.delete();
            }
        }
    }

    @Test
    public void loadingPathDotDotFollowsTheOriginalLinkBeforeChoosingThePhysicalParent() throws Exception {
        Path remote = Files.createDirectory(root.resolve("remote"));
        Files.createDirectory(remote.resolve("deep"));
        Path expected = remote.resolve("interpreter");
        write(expected, new byte[]{7});
        write(root.resolve("interpreter"), new byte[]{8});
        Path jump = Files.createSymbolicLink(root.resolve("jump"), Path.of("remote/deep"));
        Path entry = Files.createSymbolicLink(root.resolve("entry"), Path.of("jump/../interpreter"));
        String absoluteTarget = root + "/jump/../interpreter";
        Path absolute = Files.createSymbolicLink(root.resolve("absolute"), Path.of(absoluteTarget));
        for (Path interpreter : List.of(entry, absolute)) {
            assertEquals(expected, interpreter.toRealPath());
            var input = loading(interpreter, List.of(
                    new SegmentClassicLinuxDeployment.Alias(absolute, absoluteTarget),
                    new SegmentClassicLinuxDeployment.Alias(entry, "jump/../interpreter"),
                    new SegmentClassicLinuxDeployment.Alias(jump, "remote/deep")), List.of(), List.of());
            var owner = owner();
            owner.initialize();
            try {
                var paths = owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline());
                assertEquals(expected, paths.get(paths.size() - 1).resolved());
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void loadingPathCannotEraseNonDirectoriesMissingOrUndeclaredComponents() throws Exception {
        write(root.resolve("interpreter"), new byte[]{7});
        Files.createSymbolicLink(root.resolve("undeclared"), Path.of("."));
        var targets = List.of("interpreter/../interpreter", "interpreter/./../interpreter", "interpreter/",
                "missing/../interpreter", "undeclared/../interpreter");
        for (int i = 0; i < targets.size(); i++) {
            String target = targets.get(i);
            Path entry = rawLink(root.resolve("entry" + i), target);
            if (i < 4) {
                assertThrows(IOException.class, () -> Files.readAttributes(entry, "unix:mode"));
            }
            var input = loading(entry, List.of(new SegmentClassicLinuxDeployment.Alias(entry, target)),
                    List.of(), List.of());
            var owner = owner();
            owner.initialize();
            try {
                assertThrows(target, IOException.class,
                        () -> owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline()));
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void loadingPathCanRevisitAnAliasWithADifferentSuffixAndRequireATrailingDirectory() throws Exception {
        Path directory = Files.createDirectory(root.resolve("directory"));
        Path expected = directory.resolve("interpreter");
        write(expected, new byte[]{7});
        Path again = Files.createSymbolicLink(root.resolve("again"), Path.of("."));
        String target = "again/again/directory/";
        Path entry = rawLink(root.resolve("entry"), target);
        var input = loading(entry.resolve("interpreter"), List.of(
                new SegmentClassicLinuxDeployment.Alias(again, "."),
                new SegmentClassicLinuxDeployment.Alias(entry, target)),
                List.of(new SegmentClassicLinuxDeployment.Directory(entry, true)), List.of());
        assertEquals(expected, entry.resolve("interpreter").toRealPath());
        var owner = owner();
        owner.initialize();
        try {
            var paths = owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline());
            assertEquals(expected, paths.get(paths.size() - 1).resolved());
            assertEquals(directory, paths.get(paths.size() - 2).resolved());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void loadingPathCountsEveryFollowAndRejectsTheFortyFirstWithoutChangingTheAliasTable() throws Exception {
        Path expected = root.resolve("interpreter");
        write(expected, new byte[]{7});
        var aliases = new java.util.ArrayList<SegmentClassicLinuxDeployment.Alias>();
        for (int i = 0; i < 41; i++) {
            String target = i == 40 ? "interpreter" : String.format("chain%02d", i + 1);
            Path entry = Files.createSymbolicLink(root.resolve(String.format("chain%02d", i)), Path.of(target));
            aliases.add(new SegmentClassicLinuxDeployment.Alias(entry, target));
        }
        for (int first : List.of(1, 0)) {
            var input = loading(root.resolve(String.format("chain%02d", first)), aliases, List.of(), List.of());
            var owner = owner();
            owner.initialize();
            try {
                if (first == 1) {
                    var paths = owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline());
                    assertEquals(expected, paths.get(paths.size() - 1).resolved());
                } else {
                    assertThrows(IOException.class,
                            () -> owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline()));
                }
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void loadingPathsObserveActualRelativeMultiHopAliasesDirectoriesAndProtectedAbsence() throws Exception {
        Path physical = Files.createDirectory(root.resolve("lib-real"));
        Files.setPosixFilePermissions(physical, PosixFilePermissions.fromString("rwx------"));
        Path file = physical.resolve("ld-real.so");
        write(file, new byte[]{7});
        Path first = Files.createSymbolicLink(root.resolve("lib"), Path.of("lib-link"));
        Path second = Files.createSymbolicLink(root.resolve("lib-link"), Path.of("lib-real"));
        Path leaf = Files.createSymbolicLink(physical.resolve("ld.so"), Path.of("ld-real.so"));
        var input = loading(first.resolve("ld.so"), List.of(
                new SegmentClassicLinuxDeployment.Alias(first, "lib-link"),
                new SegmentClassicLinuxDeployment.Alias(second, "lib-real"),
                new SegmentClassicLinuxDeployment.Alias(leaf, "ld-real.so")), List.of(
                new SegmentClassicLinuxDeployment.Directory(first, true),
                new SegmentClassicLinuxDeployment.Directory(first.resolve("hwcaps"), false),
                new SegmentClassicLinuxDeployment.Directory(first.resolve("hwcaps/v4"), false)), List.of(
                new SegmentClassicLinuxDeployment.Requirement(root.resolve("loader.cache"), -1)));
        var owner = owner();
        owner.initialize();
        var paths = owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline());
        assertEquals(8, paths.size());
        var interpreter = paths.get(paths.size() - 1);
        assertEquals(file, interpreter.resolved());
        assertEquals(SegmentClassicHandoffPaths.fileIdentity(file), interpreter.identity());
        assertTrue(paths.stream().anyMatch(o -> o.absentAt() != null
                && o.absentAt().equals(physical.resolve("hwcaps"))));
        assertThrows(IOException.class, owner::installed);
        var aliasFile = new SegmentClassicLinuxDeployment.File(first.resolve("ld.so"),
                SegmentClassicLinuxDeployment.Role.LOADER_CONFIG, 1, SegmentClassicHandoff.digest(new byte[]{7}));
        assertThrows(IOException.class, () -> owner.verifyLinuxFile(aliasFile, SegmentClassicLinuxElfTest.deadline()));
        assertTrue(owner.closeAndDrain().drained());
        assertEquals(Path.of("lib-link"), Files.readSymbolicLink(first));
    }

    @Test
    public void loadingPathsRejectFalseAbsenceUnknownLinksWrongTargetsAndUnprotectedParents() throws Exception {
        Path file = root.resolve("interpreter");
        write(file, new byte[]{7});
        Path alias = Files.createSymbolicLink(root.resolve("alias"), Path.of("interpreter"));
        Path dangling = Files.createSymbolicLink(root.resolve("dangling"), Path.of("missing"));
        Path unsafe = Files.createDirectory(root.resolve("unsafe"));
        Files.setAttribute(unsafe, "unix:mode", 01777);
        var inputs = List.of(
                loading(file, List.of(), List.of(), List.of(new SegmentClassicLinuxDeployment.Requirement(file, -1))),
                loading(file, List.of(), List.of(), List.of(
                        new SegmentClassicLinuxDeployment.Requirement(dangling, -1))),
                loading(alias, List.of(), List.of(), List.of()),
                loading(alias, List.of(new SegmentClassicLinuxDeployment.Alias(alias, "different")),
                        List.of(), List.of()),
                loading(file, List.of(), List.of(new SegmentClassicLinuxDeployment.Directory(unsafe, true)), List.of()),
                loading(file, List.of(), List.of(), List.of(
                        new SegmentClassicLinuxDeployment.Requirement(unsafe.resolve("missing"), -1))));
        for (var input : inputs) {
            var owner = owner();
            owner.initialize();
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline()));
            assertThrows(IOException.class, owner::parsed);
            var result = owner.closeAndDrain();
            assertSame(failure, result.launcherFailure());
            assertTrue(result.drained());
        }
        assertTrue(Files.isSymbolicLink(alias));
        assertTrue(Files.isSymbolicLink(dangling));
        assertEquals(01777, ((Number) Files.getAttribute(unsafe, "unix:mode")).intValue() & 07777);
    }

    @Test
    public void replacementOfOriginalAliasAfterNativeObservationCannotReturnMatchingPathFacts() throws Exception {
        Path file = root.resolve("interpreter");
        write(file, new byte[]{7});
        Path alias = Files.createSymbolicLink(root.resolve("alias"), Path.of("interpreter"));
        var input = loading(alias, List.of(new SegmentClassicLinuxDeployment.Alias(alias, "interpreter")),
                List.of(), List.of());
        var owner = owner();
        owner.initialize();
        var reads = new AtomicInteger();
        try (var calls = mockStatic(Files.class, invocation -> {
            if (invocation.getMethod().getName().equals("readAttributes")
                    && alias.equals(invocation.getArgument(0)) && reads.incrementAndGet() == 2) {
                Files.delete(alias);
                Files.createSymbolicLink(alias, Path.of("interpreter"));
            }
            return invocation.callRealMethod();
        })) {
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline()));
        }
        assertTrue(reads.get() >= 2);
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test(timeout = 30000)
    public void loadingPathBodyOwnsExternalCloseAndCannotDrainItself() throws Exception {
        var owner = owner();
        owner.initialize();
        var input = loading(key, List.of(), List.of(), List.of());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var read = new FutureTask<>(() -> {
            try (var calls = mockStatic(Files.class, invocation -> {
                if (invocation.getMethod().getName().equals("readAttributes")
                        && key.equals(invocation.getArgument(0))) {
                    assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return invocation.callRealMethod();
            })) {
                return owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline());
            }
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
            release.countDown();
            assertEquals(1, read.get(5, TimeUnit.SECONDS).size());
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    /** Raw invalid UTF-8 is retained by JNI and must fail the declaration reader, never replacement-decode. */
    public static final class RawAliasProbe {
        public static void main(String[] args) throws Exception {
            var fixture = new SegmentClassicLaunchTrustTest();
            fixture.temporary.create();
            try {
                fixture.fixture();
                var owner = fixture.owner();
                owner.initialize();
                Path alias = Path.of(args[0]);
                var input = loading(fixture.key,
                        List.of(new SegmentClassicLinuxDeployment.Alias(alias, "\uFFFD\uFFFD/./a//b")),
                        List.of(), List.of());
                assertThrows(java.nio.charset.CharacterCodingException.class,
                        () -> owner.verifyLinuxLoadingPaths(input, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(owner.closeAndDrain().drained());
            } finally {
                fixture.temporary.delete();
            }
        }
    }

    private SegmentClassicLinuxDeployment.File linuxFile(Path path, SegmentClassicLinuxDeployment.Role role,
                                                        byte[] bytes) throws Exception {
        write(path, bytes);
        return new SegmentClassicLinuxDeployment.File(path, role, bytes.length,
                new SegmentClassicHandoff.Digest(MessageDigest.getInstance("SHA-256").digest(bytes)));
    }

    @Test
    public void loaderConfigurationUsesItsOriginalHashHandleWithoutLookingUpTheIncludePatterns() throws Exception {
        byte[] bytes = SegmentClassicLinuxConfigurationTest.vector();
        var file = linuxFile(root.resolve("loader.conf"), SegmentClassicLinuxDeployment.Role.LOADER_CONFIG, bytes);
        var owner = owner();
        owner.initialize();
        var channels = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<FileChannel, Boolean>());
        try (var calls = mockStatic(FileChannel.class, invocation -> {
            if (invocation.getMethod().getName().equals("open")) {
                assertEquals(file.path(), invocation.getArgument(0));
            }
            Object actual = invocation.callRealMethod();
            if (actual instanceof FileChannel channel) {
                channels.add(channel);
            }
            return actual;
        })) {
            var observed = owner.verifyLinuxFile(file, SegmentClassicLinuxElfTest.deadline());
            assertArrayEquals(bytes, observed.configuration().bytes());
            assertEquals(null, observed.elf());
            assertEquals(null, observed.cache());
            assertEquals(1, channels.size());
            assertFalse(channels.iterator().next().isOpen());
            assertEquals("/lib", observed.configuration().cursor(SegmentClassicLinuxElfTest.deadline())
                    .next().value().utf8());
            assertThrows(IOException.class, owner::installed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        var nul = linuxFile(file.path(), file.role(), new byte[]{'#', 0});
        var rejected = owner();
        rejected.initialize();
        try {
            var failure = assertThrows(SegmentClassicLinuxConfiguration.Unsupported.class,
                    () -> rejected.verifyLinuxFile(nul, SegmentClassicLinuxElfTest.deadline()));
            assertSame(failure, rejected.closeAndDrain().launcherFailure());
        } finally {
            assertTrue(rejected.closeAndDrain().drained());
        }
    }

    @Test
    public void loaderConfigurationOverLimitRejectsBeforeAnyTargetFileIo() throws Exception {
        var input = new SegmentClassicLinuxDeployment.File(root.resolve("not-opened"),
                SegmentClassicLinuxDeployment.Role.LOADER_CONFIG, SegmentClassicLinuxConfiguration.MAX_BYTES + 1,
                SegmentClassicHandoff.digest(new byte[0]));
        var owner = owner();
        owner.initialize();
        try (var calls = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Oversized configuration reached file I/O");
        })) {
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxFile(input, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("configuration length"));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void loaderCacheUsesTheSameOriginalHashHandleAndCannotPromoteItsParsedBytes() throws Exception {
        var cache = linuxFile(root.resolve("loader.cache"), SegmentClassicLinuxDeployment.Role.LOADER_CACHE,
                SegmentClassicLinuxCacheTest.vector());
        var owner = owner();
        owner.initialize();
        var observed = owner.verifyLinuxFile(cache, SegmentClassicLinuxElfTest.deadline());
        assertEquals(7, observed.cache().entries().size());
        assertEquals(null, observed.elf());
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());
        byte[] bad = SegmentClassicLinuxCacheTest.vector();
        bad[28] = 3;
        var malformed = linuxFile(cache.path(), cache.role(), bad);
        var rejected = owner();
        rejected.initialize();
        var failure = assertThrows(IOException.class,
                () -> rejected.verifyLinuxFile(malformed, SegmentClassicLinuxElfTest.deadline()));
        assertSame(failure, rejected.closeAndDrain().launcherFailure());
        assertTrue(rejected.closeAndDrain().drained());
    }

    @Test
    public void loaderCacheOverLimitRejectsBeforeAnyFileIo() throws Exception {
        var input = new SegmentClassicLinuxDeployment.File(root.resolve("not-opened"),
                SegmentClassicLinuxDeployment.Role.LOADER_CACHE, 16 * 1024 * 1024 + 1,
                new SegmentClassicHandoff.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])));
        var owner = owner();
        owner.initialize();
        try (var calls = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Oversized cache reached file I/O");
        })) {
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxFile(input, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("cache length"));
        }
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void loaderMetadataReadAndFirstCloseFailuresKeepBothOriginalResults() throws Exception {
        var cache = linuxFile(root.resolve("loader.cache"), SegmentClassicLinuxDeployment.Role.LOADER_CACHE,
                SegmentClassicLinuxCacheTest.vector());
        var configuration = linuxFile(root.resolve("loader.conf"), SegmentClassicLinuxDeployment.Role.LOADER_CONFIG,
                SegmentClassicLinuxConfigurationTest.vector());
        for (var file : List.of(cache, configuration)) {
            var owner = owner();
            owner.initialize();
            var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
            IOException readFailure = new IOException("original metadata positional read");
            IOException closeFailure = new IOException("original metadata native first close");
            try (var calls = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && file.path().equals(invocation.getArgument(0))) {
                    var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                    fault.closeFailure = closeFailure;
                    captured.set(fault);
                    doAnswer(call -> {
                        throw readFailure;
                    }).when(fault).read(any(ByteBuffer.class), anyLong());
                    return fault;
                }
                return actual;
            })) {
                assertSame(readFailure, assertThrows(IOException.class,
                        () -> owner.verifyLinuxFile(file, SegmentClassicLinuxElfTest.deadline())));
            }
            try {
                var drain = owner.closeAndDrain();
                assertFalse(drain.drained());
                assertSame(readFailure, drain.launcherFailure());
                assertSame(closeFailure, drain.files().closeFailure());
                assertTrue(Arrays.asList(readFailure.getSuppressed()).contains(closeFailure));
                assertEquals(1, captured.get().closes.get());
                assertTrue(captured.get().actual.isOpen());
            } finally {
                captured.get().actual.close();
            }
            assertFalse(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void opaqueNonUtf8LoadingPathIsRejectedBeforeAnyFileIo() throws Exception {
        Path raw = Path.of(java.net.URI.create(root.toUri().toASCIIString() + "%FF.conf"));
        var file = new SegmentClassicLinuxDeployment.File(raw, SegmentClassicLinuxDeployment.Role.LOADER_CONFIG,
                0, new SegmentClassicHandoff.Digest(MessageDigest.getInstance("SHA-256").digest(new byte[0])));
        var owner = owner();
        owner.initialize();
        try (var calls = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Opaque invalid UTF-8 path reached file I/O");
        })) {
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxFile(file, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("strict UTF-8"));
        }
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void linuxFilesUseOriginalHashAndElfHandleIncludingAnEmptyConfiguration() throws Exception {
        var empty = linuxFile(root.resolve("loader.conf"), SegmentClassicLinuxDeployment.Role.LOADER_CONFIG,
                new byte[0]);
        var library = linuxFile(root.resolve("lib-公钥.so.3"), SegmentClassicLinuxDeployment.Role.LIBRARY,
                SegmentClassicLinuxElfTest.image());
        var owner = owner();
        owner.initialize();
        var config = owner.verifyLinuxFile(empty, SegmentClassicLinuxElfTest.deadline());
        assertEquals(0, config.identity().size());
        assertEquals(null, config.configuration().cursor(SegmentClassicLinuxElfTest.deadline()).next());
        assertEquals(null, config.elf());
        var image = owner.verifyLinuxFile(library, SegmentClassicLinuxElfTest.deadline());
        assertEquals(Arrays.asList("libz.so.1", "liba.so.2"), image.elf().needed());
        assertFalse(image.parents().isEmpty());
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void linuxFilePermissionsHashAndNativeRolesFailWithoutPromotionOrRepair() throws Exception {
        for (int scenario = 0; scenario < 5; scenario++) {
            var declared = linuxFile(root.resolve("image.so"), scenario == 0
                    ? SegmentClassicLinuxDeployment.Role.SYSTEMCTL : SegmentClassicLinuxDeployment.Role.LIBRARY,
                    SegmentClassicLinuxElfTest.image());
            if (scenario == 1) {
                Files.setPosixFilePermissions(declared.path(), PosixFilePermissions.fromString("rw-rw-r--"));
            } else if (scenario == 2) {
                byte[] changed = SegmentClassicLinuxElfTest.image();
                changed[1400] = 1;
                Files.write(declared.path(), changed);
            } else if (scenario == 3) {
                Files.createLink(root.resolve("same-inode.so"), declared.path());
            } else if (scenario == 4) {
                byte[] changed = SegmentClassicLinuxElfTest.image();
                ByteBuffer.wrap(changed).order(java.nio.ByteOrder.LITTLE_ENDIAN).putShort(16, (short) 2);
                declared = linuxFile(declared.path(), declared.role(), changed);
            }
            var owner = owner();
            owner.initialize();
            var input = declared;
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxFile(input, SegmentClassicLinuxElfTest.deadline()));
            assertThrows(IOException.class, owner::parsed);
            var result = owner.closeAndDrain();
            assertSame(failure, result.launcherFailure());
            assertTrue(result.drained());
            Files.delete(declared.path());
        }
    }

    @Test
    public void linuxFirstCloseFailureRetainsOriginalHandleAndReplacementCannotReturnFacts() throws Exception {
        var file = linuxFile(root.resolve("original.so"), SegmentClassicLinuxDeployment.Role.LIBRARY,
                SegmentClassicLinuxElfTest.image());
        for (boolean failClose : new boolean[]{true, false}) {
            var owner = owner();
            owner.initialize();
            var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
            IOException failure = new IOException("Linux original first close");
            Path replacement = root.resolve("replacement.so");
            write(replacement, SegmentClassicLinuxElfTest.image());
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && file.path().equals(invocation.getArgument(0))) {
                    var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                    captured.set(fault);
                    if (failClose) {
                        fault.closeFailure = failure;
                    } else {
                        fault.beforeClose = () -> {
                            try {
                                Files.move(replacement, file.path(), StandardCopyOption.REPLACE_EXISTING);
                            } catch (IOException problem) {
                                throw new AssertionError(problem);
                            }
                        };
                    }
                    return fault;
                }
                return actual;
            })) {
                var result = assertThrows(IOException.class,
                        () -> owner.verifyLinuxFile(file, SegmentClassicLinuxElfTest.deadline()));
                if (failClose) {
                    assertSame(failure, result);
                }
            }
            try {
                assertEquals(!failClose, owner.closeAndDrain().drained());
                assertEquals(1, captured.get().closes.get());
                assertEquals(failClose, captured.get().actual.isOpen());
                assertThrows(IOException.class, owner::parsed);
            } finally {
                captured.get().actual.close();
            }
            assertEquals(!failClose, owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void linuxExternalCloseWaitsForActualElfReadAndRejectsReentrantDrain() throws Exception {
        var file = linuxFile(root.resolve("inflight.so"), SegmentClassicLinuxDeployment.Role.LIBRARY,
                SegmentClassicLinuxElfTest.image());
        var owner = owner();
        owner.initialize();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var captured = new AtomicReference<FileChannel>();
        var read = new FutureTask<>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && file.path().equals(invocation.getArgument(0))) {
                    captured.set((FileChannel) actual);
                    var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                    doAnswer(call -> {
                        assertThrows(IllegalStateException.class, owner::closeAndDrain);
                        entered.countDown();
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                        return call.callRealMethod();
                    }).when(fault).read(any(ByteBuffer.class), anyLong());
                    return fault;
                }
                return actual;
            })) {
                return owner.verifyLinuxFile(file, SegmentClassicLinuxElfTest.deadline());
            }
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
            assertTrue(captured.get().isOpen());
            release.countDown();
            assertEquals(2, read.get(5, TimeUnit.SECONDS).elf().needed().size());
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
        assertFalse(captured.get().isOpen());
    }

    @Test
    public void parentOnlyMaterialLeavesRemainPrivateAndCannotProduceInstalledProvenance() throws Exception {
        Path directory = launcherDirectory();
        byte[] descriptor = new byte[32];
        byte[] privateKey = new byte[48];
        Path input = directory.resolve("classic-1.launcher");
        Path secret = directory.resolve("issuer-1.pk8");
        Files.write(input, descriptor);
        Files.write(secret, privateKey);
        Files.setPosixFilePermissions(input, PosixFilePermissions.fromString("rw-------"));
        Files.setPosixFilePermissions(secret, PosixFilePermissions.fromString("rw-------"));
        var owner = owner();
        owner.initialize();
        assertThrows(IOException.class, owner::readLauncherPrivateKey);
        assertArrayEquals(descriptor, owner.readLauncherDescriptor());
        assertArrayEquals(privateKey, owner.readLauncherPrivateKey());
        assertThrows(IOException.class, owner::readLauncherDescriptor);
        assertThrows(IOException.class, owner::readLauncherPrivateKey);
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(privateKey, Files.readAllBytes(secret));
    }

    @Test
    public void parentDescriptorReadsLargerV2AndTheExactCapacityWithoutTruncationOrInstalledProvenance()
            throws Exception {
        write(recipe, SegmentClassicLinuxDeploymentTest.vector("classic-linux-recipe-v1"));
        Path input = launcherDirectory().resolve("classic-1.launcher");
        byte[] larger = SegmentClassicLinuxDeploymentTest.largerLauncherInput();
        assertTrue(larger.length > SegmentClassicLauncherInputs.MAX_BYTES);
        for (byte[] bytes : java.util.List.of(larger, new byte[SegmentClassicLauncherInputs.MAX_V2_BYTES],
                new byte[SegmentClassicLauncherInputs.MAX_V2_BYTES + 1])) {
            Files.write(input, bytes);
            Files.setPosixFilePermissions(input, PosixFilePermissions.fromString("rw-------"));
            var owner = owner();
            owner.initialize();
            try {
                if (bytes.length > SegmentClassicLauncherInputs.MAX_V2_BYTES) {
                    assertThrows(IOException.class, owner::readLauncherDescriptor);
                } else {
                    byte[] read = owner.readLauncherDescriptor();
                    assertArrayEquals(bytes, read);
                    if (bytes == larger) {
                        SegmentClassicLauncherInputs.decode(read).requireRecipe(owner.parsed());
                    }
                }
                assertThrows(IOException.class, owner::installed);
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
            assertEquals(bytes.length, Files.size(input));
        }
    }

    @Test
    public void parentPrivateDirectoryAndFilePermissionsDoNotInheritTheChildPublicReadPolicy() throws Exception {
        Path directory = launcherDirectory();
        Path input = directory.resolve("classic-1.launcher");
        write(input, new byte[32]); // Public-readable is valid for child public files, but never parent files.
        var publicFile = owner();
        publicFile.initialize();
        assertThrows(IOException.class, publicFile::readLauncherDescriptor);
        assertTrue(publicFile.closeAndDrain().drained());
        assertTrue(publicFile.closeAndDrain().launcherFailure() instanceof IOException);
        Files.setPosixFilePermissions(input, PosixFilePermissions.fromString("rw-------"));
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-x---"));
        var readableDirectory = owner();
        readableDirectory.initialize();
        assertThrows(IOException.class, readableDirectory::readLauncherDescriptor);
        assertTrue(readableDirectory.closeAndDrain().drained());
        assertEquals("rwxr-x---", PosixFilePermissions.toString(Files.getPosixFilePermissions(directory)));
    }

    @Test
    public void privateKeyFirstCloseFailureRetainsTheOriginalHandleAndDoesNotRetry() throws Exception {
        Path directory = launcherDirectory();
        Path input = directory.resolve("classic-1.launcher");
        Path secret = directory.resolve("issuer-1.pk8");
        for (Path path : new Path[]{input, secret}) {
            Files.write(path, new byte[path.equals(secret) ? 48 : 32]);
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        }
        var owner = owner();
        owner.initialize();
        owner.readLauncherDescriptor();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("private key first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && secret.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class, owner::readLauncherPrivateKey));
        }
        try {
            assertThrows(IOException.class, owner::readLauncherPrivateKey);
            assertFalse(owner.closeAndDrain().drained());
            assertSame(failure, owner.closeAndDrain().launcherFailure());
            assertSame(failure, owner.closeAndDrain().files().closeFailure());
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    @Test
    public void dropperFileHashAndExactNativeHeadersRejectScriptsClassFilesAndWrongArchitectures() throws Exception {
        Path executable = root.resolve("exec-dropper");
        for (String os : java.util.List.of("linux", "macos")) {
            for (int variant = 0; variant < 5; variant++) {
                byte[] bytes = new byte[32];
                ByteBuffer little = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                if (os.equals("linux")) {
                    ByteBuffer.wrap(bytes).putInt(0x7f454c46);
                    bytes[4] = 2;
                    bytes[5] = 1;
                    bytes[6] = 1;
                    little.putShort(16, (short) 3).putShort(18, (short) (variant == 2 ? 62 : 183));
                    little.putLong(24, variant == 3 ? 0 : 1);
                } else {
                    little.putInt(0, 0xfeedfacf).putInt(4, variant == 2 ? 0x01000007 : 0x0100000c);
                    little.putInt(12, variant == 3 ? 6 : 2);
                }
                if (variant == 1) {
                    ByteBuffer.wrap(bytes).putInt(0xcafebabe); // Java class prefix is not a supported exec binary.
                } else if (variant == 4) {
                    ByteBuffer.wrap(bytes).put("#!/bin/sh\n".getBytes(StandardCharsets.US_ASCII));
                }
                Files.write(executable, bytes);
                Files.setPosixFilePermissions(executable, PosixFilePermissions.fromString("rwxr-xr-x"));
                var owner = owner();
                owner.initialize();
                var parsed = owner.parsed();
                Path directory = root.getParent().resolve("supervisor");
                var descriptor = new SegmentClassicLauncherInputs("classic-1", parsed.recipeIdentity(),
                        parsed.recipe().signerKeyId(),
                        SegmentClassicLauncherInputs.parentProgramIdentity(parsed.recipe().program()), os, "aarch64",
                        new SegmentClassicLaunchProgram.File(executable, SegmentClassicLaunchProgram.NATIVE,
                                bytes.length, SegmentClassicHandoff.digest(bytes)), directory, 501,
                        root.resolve("manifest"), directory.resolve("proof"), directory.resolve("policy"),
                        directory.resolve("aes"));
                if (variant == 0) {
                    owner.verifyLauncherDropper(descriptor, System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
                } else {
                    assertThrows(IOException.class, () -> owner.verifyLauncherDropper(descriptor,
                            System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
                }
                assertThrows(IOException.class, owner::installed);
                assertTrue(owner.closeAndDrain().drained());
            }
        }
        // These are synthetic header files and real reads, never executed or qualified droppers.
    }

    @Test
    public void typedDescriptorUsesTheOriginalOneReadAndFixtureCannotBindAProvider() throws Exception {
        Path directory = launcherDirectory();
        Path descriptor = directory.resolve("classic-1.launcher");
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v1");
        Files.write(descriptor, bytes);
        Files.setPosixFilePermissions(descriptor, PosixFilePermissions.fromString("rw-------"));
        var owner = owner();
        owner.initialize();
        assertEquals(SegmentClassicLauncherInputs.decode(bytes),
                owner.readLauncherInputs(SegmentClassicLinuxProviderTest.deadline()));
        assertThrows(IOException.class, owner::readLauncherDescriptor);
        assertThrows(IOException.class,
                () -> owner.verifyLinuxProvider(SegmentClassicLinuxProviderTest.deadline()));
        assertThrows(IOException.class, owner::parsed);
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class,
                () -> owner.verifyLinuxProvider(SegmentClassicLinuxProviderTest.deadline()));
    }

    @Test
    public void byteOnlyReadCannotBeReusedAsTypedOriginalSourceAndDeadlineFailureIsSticky() throws Exception {
        Path directory = launcherDirectory();
        Path descriptor = directory.resolve("classic-1.launcher");
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v1");
        Files.write(descriptor, bytes);
        Files.setPosixFilePermissions(descriptor, PosixFilePermissions.fromString("rw-------"));
        var read = owner();
        read.initialize();
        assertArrayEquals(bytes, read.readLauncherDescriptor());
        assertThrows(IOException.class, () -> read.readLauncherInputs(SegmentClassicLinuxProviderTest.deadline()));
        assertThrows(IOException.class, read::parsed);
        assertTrue(read.closeAndDrain().drained());
        var expired = owner();
        expired.initialize();
        assertThrows(IOException.class, () -> expired.readLauncherInputs(System.nanoTime()));
        assertThrows(IOException.class, expired::parsed);
        assertTrue(expired.closeAndDrain().drained());
    }

    @Test
    public void callerConstructedD2InputCannotReachDropperIoWithoutTheOriginalProviderBinding() throws Exception {
        var owner = owner();
        owner.initialize();
        IOException refusal = assertThrows(IOException.class, () -> owner.verifyLauncherDropper(
                SegmentClassicLinuxProviderTest.schemaTwoInput(), SegmentClassicLinuxProviderTest.deadline()));
        assertTrue(refusal.getMessage().contains("original provider/source binding"));
        assertThrows(IOException.class, owner::parsed);
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void typedElfMemberWithoutOriginalProviderSourceCannotReachTargetIo() throws Exception {
        for (int member : new int[]{-1, 0, 4, Integer.MAX_VALUE}) {
            var owner = owner();
            owner.initialize();
            try (var targets = mockStatic(Files.class, invocation -> {
                throw new AssertionError("Unbound D2 member reached target I/O");
            })) {
                IOException failure = assertThrows(IOException.class,
                        () -> owner.verifyLinuxElfMember(member, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(failure.getMessage().contains("original provider/source binding"));
            }
            assertThrows(IOException.class, owner::parsed);
            assertTrue(owner.closeAndDrain().drained());
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxElfMember(member, SegmentClassicLinuxElfTest.deadline()));
        }
    }

    @Test
    public void typedLoaderConfigurationWithoutOriginalProviderSourceCannotReachTargetIo() throws Exception {
        for (int rule : new int[]{-1, 0, 1, Integer.MAX_VALUE}) {
            var owner = owner();
            owner.initialize();
            try (var targets = mockStatic(Files.class, invocation -> {
                throw new AssertionError("Unbound D2 configuration reached target I/O");
            })) {
                IOException failure = assertThrows(IOException.class,
                        () -> owner.verifyLinuxLoaderConfiguration(rule, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(failure.getMessage().contains("original provider/source binding"));
            }
            assertThrows(IOException.class, owner::parsed);
            assertTrue(owner.closeAndDrain().drained());
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoaderConfiguration(rule, SegmentClassicLinuxElfTest.deadline()));
        }
    }

    private void rejected() throws Exception {
        var owner = owner();
        try {
            assertThrows(IOException.class, owner::initialize);
            assertThrows(IOException.class, owner::parsed);
            assertThrows(IOException.class, owner::installed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void realPublicFilesMatchIndependentRecipeVectorButFixtureCannotAcquireInstalledProvenance()
            throws Exception {
        var owner = owner();
        assertThrows(IOException.class, owner::parsed);
        owner.initialize();
        var parsed = owner.parsed();
        assertEquals("classic-1", parsed.recipe().recipeId());
        assertEquals("deployment-1", parsed.recipe().deployment());
        assertEquals("issuer-1", parsed.recipe().keyName());
        assertEquals(1001, parsed.recipe().oldUid());
        assertEquals(20, parsed.recipe().primaryGid());
        assertEquals(java.util.List.of(20L, 21L), parsed.recipe().groups());
        assertArrayEquals(PUBLIC, Arrays.copyOfRange(parsed.publicKey().getEncoded(), 12, 44));
        assertEquals("bcc36aef3bb13800478055cfc37b7befdccb74089951b2e2d75bdc5f8f3f7270",
                HexFormat.of().formatHex(parsed.recipeIdentity().value()));
        assertThrows(IOException.class, owner::installed);
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class, owner::parsed);
        assertArrayEquals(vector(), Files.readAllBytes(recipe));
        assertArrayEquals(PUBLIC, Files.readAllBytes(key));
    }

    @Test
    public void parsedTrustWithAnOrdinaryNativeBackendCannotPrepareTheProductionLoader() throws Exception {
        var fixture = owner();
        fixture.initialize(); // Actual file/ACL I/O uses the ordinary backend in this unqualified JVM.
        assertEquals("classic-1", fixture.parsed().recipe().recipeId());
        var preload = SegmentClassicLaunchPreload.production("classic-1",
                System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.production(preload));
        assertTrue(preload.closeAndDrain().drained());
        assertThrows(IOException.class, fixture::installed);
        assertTrue(fixture.closeAndDrain().drained());
    }

    @Test
    public void constructionDoesNoIoAndMissingRootsFilesAndKeyLengthsAreNeverRepaired() throws Exception {
        Path missing = root.resolve("missing");
        var owner = SegmentClassicLaunchTrust.forMaterialIoTests(missing, "classic-1", uid);
        assertFalse(Files.exists(missing));
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertFalse(Files.exists(missing));
        Files.delete(key);
        rejected();
        assertFalse(Files.exists(key));
        write(key, new byte[31]);
        rejected();
        write(key, new byte[33]);
        rejected();
        assertEquals(33, Files.size(key));
        Files.delete(recipe);
        rejected();
        assertFalse(Files.exists(recipe));
    }

    @Test
    public void malformedFramingCredentialsAndPathSelectorsCannotChooseAKey() throws Exception {
        for (int[] mutation : java.util.List.of(new int[]{8, 0x00010001}, new int[]{12, 0},
                new int[]{179, 0}, new int[]{183, 0}, new int[]{193, 20})) {
            byte[] bytes = vector();
            ByteBuffer.wrap(bytes).putInt(mutation[0], mutation[1]);
            write(recipe, bytes);
            rejected();
        }
        byte[] pathKey = vector();
        pathKey[43] = '/';
        write(recipe, pathKey);
        rejected();
        write(recipe, new byte[4097]);
        rejected();
        write(recipe, Arrays.copyOf(vector(), 16));
        rejected();
    }

    @Test
    public void aliasesWrongOwnerAndWritableFilesOrParentsRejectBeforeAnyAdoption() throws Exception {
        Path held = root.resolve("held");
        Files.move(key, held);
        Files.createSymbolicLink(key, held);
        rejected();
        Files.delete(key);
        Files.createLink(key, held);
        rejected();
        Files.delete(key);
        Files.move(held, key);
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-rw-rw-"));
        rejected();
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-r--r--"));
        Files.setPosixFilePermissions(root.getParent(), PosixFilePermissions.fromString("rwxrwxrwx"));
        rejected();
        Files.setPosixFilePermissions(root.getParent(), PosixFilePermissions.fromString("rwx------"));
        var other = SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", uid == 0 ? 1 : 0);
        assertThrows(IOException.class, other::initialize);
        assertTrue(other.closeAndDrain().drained());
    }

    @Test
    public void replacementAfterOpenRejectsEvenIdenticalRecipeBytes() throws Exception {
        var owner = owner();
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && recipe.equals(invocation.getArgument(0))) {
                Files.move(recipe, root.resolve("original"));
                write(recipe, vector());
            }
            return actual;
        })) {
            assertTrue(assertThrows(IOException.class, owner::initialize).getMessage().contains("identity changed"));
            assertThrows(IOException.class, owner::parsed);
        }
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(Files.readAllBytes(root.resolve("original")), Files.readAllBytes(recipe));
    }

    @Test
    public void failedFirstNativeCloseRetainsTheActualDescriptorAndCannotExposeTrust() throws Exception {
        var owner = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("public key first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && key.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class, owner::initialize));
            assertThrows(IOException.class, owner::parsed);
            assertThrows(IOException.class, owner::installed);
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.files().closeFailure());
            }
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    private Path configuration(byte[] content) throws Exception {
        var frozen = SegmentClassicLaunchConfiguration.parse(content);
        byte[] recipeBytes = vector();
        ByteBuffer.wrap(recipeBytes).position(115).put(frozen.identity().value());
        write(recipe, recipeBytes);
        Path configuration = root.resolve("classic-1.conf");
        write(configuration, content);
        Files.setPosixFilePermissions(configuration, PosixFilePermissions.fromString("rw-------"));
        return configuration;
    }

    @Test
    public void fixedConfigurationReadProducesOnlyBoundedImmutableDataAndRecipeMismatchPoisonsTheAttempt()
            throws Exception {
        byte[] bytes = SegmentClassicLaunchConfigurationTest.configuration();
        Path configuration = configuration(bytes);
        var owner = owner();
        owner.initialize();
        var frozen = owner.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        assertEquals(2, frozen.properties().get("serverNumIOThreads"));
        assertThrows(IOException.class, () -> owner.verifyConfiguration(Long.MAX_VALUE));
        Files.writeString(configuration, "after-read mutation");
        assertEquals(frozen.identity(), SegmentClassicLaunchConfiguration.identity(frozen.properties()));
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());

        configuration(bytes);
        Files.writeString(configuration, new String(bytes, StandardCharsets.UTF_8) + "additional=covered\n");
        var mismatched = owner();
        mismatched.initialize();
        var failure = assertThrows(IOException.class,
                () -> mismatched.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
        assertTrue(failure.getMessage().contains("recipe digest"));
        assertThrows(IOException.class, mismatched::parsed);
        var result = mismatched.closeAndDrain();
        assertTrue(result.drained());
        assertSame(failure, result.configurationFailure());

        configuration(bytes);
        Files.setPosixFilePermissions(configuration, PosixFilePermissions.fromString("rw-r--r--"));
        var publicCredentials = owner();
        publicCredentials.initialize();
        assertThrows(IOException.class,
                () -> publicCredentials.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
        assertTrue(publicCredentials.closeAndDrain().drained());
    }

    @Test
    public void configurationFirstNativeCloseFailureNeverReturnsParsedDataOrRetiresTheOriginalHandle()
            throws Exception {
        Path configuration = configuration(SegmentClassicLaunchConfigurationTest.configuration());
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("configuration first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && configuration.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class,
                    () -> owner.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1))));
            assertThrows(IOException.class, owner::parsed);
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.files().closeFailure());
                assertSame(failure, result.configurationFailure());
            }
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    @Test(timeout = 30000)
    public void externalCloseWaitsForTheActualOpenBodyAndSelfDrainIsRejected() throws Exception {
        var owner = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var read = new FutureTask<Void>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && recipe.equals(invocation.getArgument(0))) {
                    assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return actual;
            })) {
                owner.initialize();
            }
            return null;
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
            release.countDown();
            read.get(5, TimeUnit.SECONDS);
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    @Test
    public void additionalAclOnFileOrParentRejectsDespitePrivatePosixMode() throws Exception {
        assumeTrue(SystemUtils.IS_OS_MAC_OSX);
        for (Path member : new Path[]{key, root.getParent()}) {
            var mode = Files.getPosixFilePermissions(member);
            Process command = new ProcessBuilder("/bin/chmod", "+a", Files.isDirectory(member)
                    ? "everyone allow delete_child" : "everyone allow write", member.toString())
                    .redirectErrorStream(true).start();
            try {
                assertTrue(command.waitFor(5, TimeUnit.SECONDS));
                assertEquals(new String(command.getInputStream().readAllBytes(), StandardCharsets.UTF_8),
                        0, command.exitValue());
            } finally {
                if (command.isAlive()) {
                    command.destroyForcibly();
                    assertTrue(command.waitFor(5, TimeUnit.SECONDS));
                }
                command.getInputStream().close();
                command.getOutputStream().close();
                command.getErrorStream().close();
            }
            assertEquals(mode, Files.getPosixFilePermissions(member));
            rejected();
            if (member.equals(key)) {
                Files.delete(key);
                write(key, PUBLIC);
            }
        }
    }
}

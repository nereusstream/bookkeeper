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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxSearchDirectory;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LoadingDirectoryBudget;
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
    public void searchDirectoriesKeepRawLogicalIndicesAliasViewsAndExplicitAbsence() throws Exception {
        Path physical = Files.createDirectory(root.resolve("physical"));
        Files.createDirectory(physical.resolve("nested"));
        write(physical.resolve("unrelated.txt"), new byte[]{7});
        Files.createSymbolicLink(physical.resolve("dangling"), Path.of("not-present"));
        Path alias = Files.createSymbolicLink(root.resolve("view"), Path.of("physical"));
        Path empty = Files.createDirectory(root.resolve("empty"));
        String logical = alias + "/nested/../";
        var directories = List.of(new LinuxSearchDirectory(3, logical, true),
                new LinuxSearchDirectory(9, physical + "/.", true),
                new LinuxSearchDirectory(11, root + "/absent/", false),
                new LinuxSearchDirectory(15, empty.toString(), true));
        var owner = owner();
        owner.initialize();
        var physicalOpens = new AtomicInteger();
        try (var calls = mockStatic(Files.class, invocation -> {
            if (invocation.getMethod().getName().equals("newDirectoryStream")
                    && physical.equals(invocation.getArgument(0))) {
                physicalOpens.incrementAndGet();
            }
            return invocation.callRealMethod();
        })) {
            var result = owner.readLinuxSearchDirectories(directories, Map.of(alias, "physical"),
                    SegmentClassicLinuxElfTest.deadline());
            assertEquals(List.of(3, 9, 11, 15), result.stream().map(d -> d.directory()).toList());
            assertEquals(logical, result.get(0).logical());
            assertEquals(physical, result.get(0).path().resolved());
            assertEquals(List.of("dangling", "nested", "unrelated.txt"), result.get(0).members());
            assertEquals(result.get(0).members(), result.get(1).members());
            assertEquals(2, physicalOpens.get());
            assertFalse(result.get(2).present());
            assertEquals(root.resolve("absent"), result.get(2).path().absentAt());
            assertTrue(result.get(2).members().isEmpty());
            assertTrue(result.get(3).present());
            assertTrue(result.get(3).path().identity().directory());
            assertTrue(result.get(3).members().isEmpty());
            assertThrows(UnsupportedOperationException.class, () -> result.get(0).members().add("new"));
            assertThrows(IOException.class, owner::installed);
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxSearchDirectories(SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void searchDirectoryFileDotDotAndTrailingSlashStillRequireRealDirectories() throws Exception {
        write(root.resolve("ordinary"), new byte[]{7});
        Files.createDirectory(root.resolve("physical"));
        for (String logical : List.of(root + "/ordinary/../physical", root + "/ordinary/")) {
            var owner = owner();
            owner.initialize();
            try {
                assertThrows(IOException.class, () -> owner.readLinuxSearchDirectories(
                        List.of(new LinuxSearchDirectory(3, logical, true)), Map.of(),
                        SegmentClassicLinuxElfTest.deadline()));
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void searchDirectorySecondPassChangeRejectsTheWholeObservation() throws Exception {
        Path directory = Files.createDirectory(root.resolve("physical"));
        write(directory.resolve("original.so"), new byte[]{7});
        var owner = owner();
        owner.initialize();
        var opens = new AtomicInteger();
        try (var calls = mockStatic(Files.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("newDirectoryStream")
                    && directory.equals(invocation.getArgument(0)) && opens.incrementAndGet() == 2) {
                write(directory.resolve("later.so"), new byte[]{8});
            }
            return actual;
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxSearchDirectories(
                    List.of(new LinuxSearchDirectory(3, directory.toString(), true)), Map.of(),
                    SegmentClassicLinuxElfTest.deadline()));
            assertEquals(2, opens.get());
            assertThrows(IOException.class, owner::parsed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void searchDirectoryAliasCannotReuseOldNamesWithANewPhysicalIdentity() throws Exception {
        Path physical = Files.createDirectory(root.resolve("physical"));
        write(physical.resolve("original.so"), new byte[]{7});
        Path alias = Files.createSymbolicLink(root.resolve("view"), Path.of("physical"));
        var owner = owner();
        owner.initialize();
        var changed = new AtomicInteger();
        var opens = new AtomicInteger();
        try (var calls = mockStatic(Files.class, invocation -> {
            if (invocation.getMethod().getName().equals("readAttributes")
                    && alias.equals(invocation.getArgument(0)) && changed.compareAndSet(0, 1)) {
                Files.move(physical, root.resolve("original-physical"));
                Files.createDirectory(physical);
                write(physical.resolve("replacement.so"), new byte[]{8});
            }
            if (invocation.getMethod().getName().equals("newDirectoryStream")
                    && physical.equals(invocation.getArgument(0))) {
                opens.incrementAndGet();
            }
            return invocation.callRealMethod();
        })) {
            var failure = assertThrows(IOException.class, () -> owner.readLinuxSearchDirectories(List.of(
                    new LinuxSearchDirectory(3, physical.toString(), true),
                    new LinuxSearchDirectory(9, alias.toString(), true)), Map.of(alias, "physical"),
                    SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("Previously enumerated physical search directory changed"));
            assertEquals(1, opens.get());
            assertEquals(1, changed.get());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void sharedDirectoryEntryAndByteBudgetsRejectAcrossRealDirectoriesAndCloseBothIterators() throws Exception {
        Path first = Files.createDirectory(root.resolve("first"));
        Path second = Files.createDirectory(root.resolve("second"));
        write(first.resolve("a"), new byte[]{7});
        write(second.resolve("b"), new byte[]{8});
        var directories = List.of(new LinuxSearchDirectory(3, first.toString(), true),
                new LinuxSearchDirectory(9, second.toString(), true));
        for (int mode = 0; mode < 2; mode++) {
            var budget = new LoadingDirectoryBudget();
            if (mode == 0) {
                for (int i = 0; i < 262143; i++) {
                    budget.retain("x");
                }
            } else {
                String charge = "x".repeat(4096);
                for (int i = 0; i < 16383; i++) {
                    budget.retain(charge);
                }
                budget.retain(charge.substring(0, 4095));
            }
            var owner = owner();
            owner.initialize();
            var opens = new AtomicInteger();
            var closes = new AtomicInteger();
            try (var calls = mockStatic(Files.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("newDirectoryStream")) {
                    opens.incrementAndGet();
                    DirectoryStream<Path> stream = (DirectoryStream<Path>) actual;
                    return new DirectoryStream<Path>() {
                        @Override
                        public java.util.Iterator<Path> iterator() {
                            return stream.iterator();
                        }

                        @Override
                        public void close() throws IOException {
                            closes.incrementAndGet();
                            stream.close();
                        }
                    };
                }
                return actual;
            })) {
                var failure = assertThrows(IOException.class, () -> owner.readLinuxDirectoryPass(
                        directories, Map.of(), budget, SegmentClassicLinuxElfTest.deadline()));
                assertTrue(failure.getMessage().contains("fixed total name capacity"));
                assertEquals(2, opens.get());
                assertEquals(2, closes.get());
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void searchDirectoryRootObservationPreservesLegacyRootRefusalAndExplicitPresence() throws Exception {
        var owner = owner();
        owner.initialize();
        try {
            var result = owner.readLinuxSearchDirectories(List.of(new LinuxSearchDirectory(3, "/", true)), Map.of(),
                    SegmentClassicLinuxElfTest.deadline());
            assertEquals("/", result.get(0).logical());
            assertEquals(Path.of("/"), result.get(0).path().resolved());
            assertTrue(result.get(0).path().identity().directory());
            assertTrue(result.get(0).members().contains(root.getName(0).toString()));
            assertThrows(IOException.class, owner::installed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        var absent = owner();
        absent.initialize();
        try {
            assertThrows(IOException.class, () -> absent.readLinuxSearchDirectories(
                    List.of(new LinuxSearchDirectory(3, "/", false)), Map.of(),
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(absent.closeAndDrain().drained());
        }
        Path alias = Files.createSymbolicLink(root.resolve("root-view"), Path.of("/"));
        var legacy = owner();
        legacy.initialize();
        try {
            assertThrows(IOException.class, () -> legacy.verifyLinuxLoadingDirectories(loading(key,
                    List.of(new SegmentClassicLinuxDeployment.Alias(alias, "/")),
                    List.of(new SegmentClassicLinuxDeployment.Directory(alias, true)), List.of()),
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(legacy.closeAndDrain().drained());
        }
    }

    @Test
    public void typedSearchDirectoriesWithoutOriginalProviderSourceCannotReachTargetIo() throws Exception {
        var owner = owner();
        owner.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unbound D2 search directories reached target I/O");
        })) {
            var failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxSearchDirectories(SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("original provider/source binding"));
        }
        assertThrows(IOException.class, owner::parsed);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class,
                () -> owner.verifyLinuxSearchDirectories(SegmentClassicLinuxElfTest.deadline()));
    }

    private SegmentClassicLinuxDeploymentV2.File loadPathFile(String name, SegmentClassicLinuxDeployment.Role role)
            throws IOException {
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-linux-elf-v2");
        Path path = root.resolve(name);
        write(path, bytes);
        return new SegmentClassicLinuxDeploymentV2.File(path.toString(), role, bytes.length,
                SegmentClassicHandoff.digest(bytes));
    }

    private record CandidateFixture(List<SegmentClassicLinuxDeploymentV2.File> files,
                                    SegmentClassicLinuxDeploymentV2.Loading loading) { }

    private CandidateFixture candidateFixture() throws IOException {
        Files.createDirectory(root.resolve("lib"));
        var loader = loadPathFile("lib/loader.so", SegmentClassicLinuxDeployment.Role.LOADER);
        Files.setPosixFilePermissions(Path.of(loader.path()), PosixFilePermissions.fromString("rwxr-xr-x"));
        var module = loadPathFile("lib/module.so", SegmentClassicLinuxDeployment.Role.MODULE);
        Path fixed = Files.createSymbolicLink(root.resolve("fixed-loader"), Path.of("lib/loader.so"));
        Path first = Files.createSymbolicLink(root.resolve("view-a"), Path.of("lib"));
        Path second = Files.createSymbolicLink(root.resolve("view-b"), Path.of("lib"));
        var aliases = List.of(new SegmentClassicLinuxDeploymentV2.Alias(fixed.toString(), "lib/loader.so"),
                new SegmentClassicLinuxDeploymentV2.Alias(first.toString(), "lib"),
                new SegmentClassicLinuxDeploymentV2.Alias(second.toString(), "lib"));
        var contexts = List.of(new SegmentClassicLinuxDeploymentV2.Context(0, first + "/loader.so"),
                new SegmentClassicLinuxDeploymentV2.Context(0, second + "/loader.so"),
                new SegmentClassicLinuxDeploymentV2.Context(1, module.path()));
        var expected = SegmentClassicLinuxContentTest.expected();
        var metadata = List.of(new SegmentClassicLinuxDeploymentV2.Elf(0, expected.needed(), expected.rpath(),
                expected.runpath(), expected.flags(), expected.flags1()),
                new SegmentClassicLinuxDeploymentV2.Elf(1, expected.needed(), expected.rpath(), expected.runpath(),
                        expected.flags(), expected.flags1()));
        var needed = new java.util.ArrayList<SegmentClassicLinuxDeploymentV2.NeededRule>();
        for (int context = 0; context < contexts.size(); context++) {
            for (int ordinal = 0; ordinal < expected.needed().size(); ordinal++) {
                needed.add(new SegmentClassicLinuxDeploymentV2.NeededRule(context, ordinal, List.of(1, 2)));
            }
        }
        var modules = List.of(new SegmentClassicLinuxDeploymentV2.ModuleRequest(1, "second", List.of(2)),
                new SegmentClassicLinuxDeploymentV2.ModuleRequest(0, "repeated", List.of(2)),
                new SegmentClassicLinuxDeploymentV2.ModuleRequest(0, "repeated", List.of(2)));
        var original = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2")).versionTwo();
        // This independent Loading subtree does not assert systemctl/config/unit or full deployment qualification.
        var loading = new SegmentClassicLinuxDeploymentV2.Loading(0, 0, fixed.toString(), aliases, List.of(), List.of(),
                original.loading().policy(), metadata, contexts, needed, modules);
        return new CandidateFixture(List.of(loader, module), loading);
    }

    private static SegmentClassicLinuxDeploymentV2.Loading candidateLoading(SegmentClassicLinuxDeploymentV2.Loading l,
            int loader, String interpreter, List<SegmentClassicLinuxDeploymentV2.NeededRule> needed,
            List<SegmentClassicLinuxDeploymentV2.ModuleRequest> modules) {
        return new SegmentClassicLinuxDeploymentV2.Loading(l.systemctl(), loader, interpreter, l.aliases(),
                l.directories(), l.configuration(), l.policy(), l.elf(), l.contexts(), needed, modules);
    }

    private CandidateFixture lookupFixture(String name) throws Exception {
        var original = candidateFixture();
        var l = original.loading();
        byte[] image = SegmentClassicLinuxDeploymentTest.vector("classic-linux-elf-v2");
        byte[] before = "libz.so.1".getBytes(StandardCharsets.UTF_8);
        byte[] after = name.getBytes(StandardCharsets.UTF_8);
        assertEquals(before.length, after.length);
        int changed = 0;
        for (int i = 0; i <= image.length - before.length; i++) {
            if (Arrays.equals(before, Arrays.copyOfRange(image, i, i + before.length))) {
                System.arraycopy(after, 0, image, i, after.length);
                changed++;
            }
        }
        assertTrue(changed > 0);
        Path library = root.resolve("lib").resolve(name);
        Files.move(Path.of(original.files().get(1).path()), library);
        var members = new java.util.ArrayList<SegmentClassicLinuxDeploymentV2.File>();
        for (int i = 0; i < 2; i++) {
            Path path = i == 0 ? Path.of(original.files().get(0).path()) : library;
            write(path, image);
            members.add(new SegmentClassicLinuxDeploymentV2.File(path.toString(), i == 0
                    ? SegmentClassicLinuxDeployment.Role.LOADER : SegmentClassicLinuxDeployment.Role.LIBRARY,
                    image.length, SegmentClassicHandoff.digest(image)));
        }
        Files.setPosixFilePermissions(Path.of(members.get(0).path()), PosixFilePermissions.fromString("rwxr-xr-x"));
        Path literal = Files.createSymbolicLink(root.resolve("view:$HOME;雪"), Path.of("lib"));
        var aliases = new java.util.ArrayList<>(l.aliases());
        aliases.add(new SegmentClassicLinuxDeploymentV2.Alias(literal.toString(), "lib"));
        aliases.sort(Comparator.comparing(SegmentClassicLinuxDeploymentV2.Alias::link));
        var directories = List.of(new SegmentClassicLinuxDeploymentV2.Directory(root + "/absent", false),
                new SegmentClassicLinuxDeploymentV2.Directory(Files.createDirectory(root.resolve("empty")).toString(),
                        true), new SegmentClassicLinuxDeploymentV2.Directory(root + "/lib", true),
                new SegmentClassicLinuxDeploymentV2.Directory(literal.toString(), true));
        var contexts = List.of(l.contexts().get(0), l.contexts().get(1),
                new SegmentClassicLinuxDeploymentV2.Context(1, library.toString()),
                new SegmentClassicLinuxDeploymentV2.Context(1, literal + "/" + name));
        var metadata = l.elf().stream().map(e -> new SegmentClassicLinuxDeploymentV2.Elf(e.file(),
                e.needed().stream().map(n -> n.equals("libz.so.1") ? name : n).toList(), e.rpath(), e.runpath(),
                e.flags(), e.flags1())).toList();
        var needed = new java.util.ArrayList<SegmentClassicLinuxDeploymentV2.NeededRule>();
        for (int context = 0; context < contexts.size(); context++) {
            for (int ordinal = 0; ordinal < 4; ordinal++) {
                needed.add(new SegmentClassicLinuxDeploymentV2.NeededRule(context, ordinal,
                        List.of(ordinal == 1 ? 3 : 2)));
            }
        }
        var loading = new SegmentClassicLinuxDeploymentV2.Loading(l.systemctl(), l.loader(), l.interpreter(), aliases,
                directories, l.configuration(), l.policy(), metadata, contexts, needed, List.of());
        return new CandidateFixture(members, loading);
    }

    @Test
    public void neededLookupKeepsExactRuleDirectoryContextAndLiteralDirectorySpelling() throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        try {
            for (int rule = 0; rule < 2; rule++) {
                int directory = rule + 2;
                var result = owner.readLinuxNeededLookup(f.files(), f.loading(), rule, directory,
                        SegmentClassicLinuxElfTest.deadline());
                assertEquals(rule, result.rule());
                assertEquals(directory, result.directory());
                assertSame(f.loading().needed().get(rule), result.declaration());
                assertEquals(f.loading().directories().get(directory).path() + "/libz.so.1", result.logical());
                assertEquals(0, result.requester().context());
                assertEquals(rule + 2, result.candidate().context());
                assertEquals(1, result.candidate().member());
                assertEquals(result.path(), result.candidate().path());
                assertEquals(result.path().identity(), result.candidate().content().identity());
                assertEquals(Path.of(f.files().get(1).path()), result.path().resolved());
            }
            assertThrows(IOException.class, owner::installed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void neededLookupPreservesColonSemicolonUnicodeAndRawDotSlashQuery() throws Exception {
        String name = "lib:;雪x";
        var f = lookupFixture(name);
        var l = f.loading();
        var directories = new java.util.ArrayList<>(l.directories());
        String raw = root + "/lib//./../lib///";
        directories.set(2, new SegmentClassicLinuxDeploymentV2.Directory(raw, true));
        var contexts = new java.util.ArrayList<>(l.contexts());
        String logical = root + "/lib//./../lib/" + name;
        contexts.set(2, new SegmentClassicLinuxDeploymentV2.Context(1, logical));
        var loading = new SegmentClassicLinuxDeploymentV2.Loading(l.systemctl(), l.loader(), l.interpreter(),
                l.aliases(), directories, l.configuration(), l.policy(), l.elf(), contexts, l.needed(), l.modules());
        var owner = owner();
        owner.initialize();
        try {
            var result = owner.readLinuxNeededLookup(f.files(), loading, 0, 2, SegmentClassicLinuxElfTest.deadline());
            assertEquals(logical, result.logical());
            assertEquals(name, result.requester().content().elf().needed().get(0));
            assertEquals(2, result.candidate().context());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void neededLookupRecordsProtectedMissingFileAndDeclaredAbsentDirectory() throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        try {
            for (int directory = 0; directory < 2; directory++) {
                var result = owner.readLinuxNeededLookup(f.files(), f.loading(), 0, directory,
                        SegmentClassicLinuxElfTest.deadline());
                assertNull(result.candidate());
                assertNull(result.path().identity());
                assertEquals(directory == 0, result.directoryPath().identity() == null);
                assertEquals(directory == 0 ? root.resolve("absent") : root.resolve("empty/libz.so.1"),
                        result.path().absentAt());
                assertEquals(0, result.requester().context());
            }
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void neededLookupPresentSamePhysicalButOtherLogicalApprovalRejectsBeforeCandidateOpen() throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        var requesterOpens = new AtomicInteger();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3) {
                if (Path.of(f.files().get(1).path()).equals(invocation.getArgument(0))) {
                    throw new AssertionError("Candidate content opened without this rule's exact logical approval");
                }
                if (Path.of(f.files().get(0).path()).equals(invocation.getArgument(0))) {
                    requesterOpens.incrementAndGet();
                }
            }
            return invocation.callRealMethod();
        })) {
            var failure = assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), f.loading(),
                    0, 3, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("exact approved logical context"));
            assertEquals(1, requesterOpens.get());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    private void neededLookupProbeReplacement(boolean alias) throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        Path library = Path.of(f.files().get(1).path());
        Path view = Path.of(f.loading().directories().get(3).path());
        var stats = new AtomicInteger();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && library.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                captured.set(fault);
                return fault;
            }
            return actual;
        }); var targets = mockStatic(Files.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("readAttributes") && library.equals(invocation.getArgument(0))
                    && "unix:*".equals(invocation.getArgument(1)) && stats.incrementAndGet() == 3) {
                // The probe's raw identity and both ACL-guarded identities have already been captured.
                if (alias) {
                    Files.delete(view);
                    Files.createSymbolicLink(view, Path.of("lib"));
                } else {
                    Files.move(library, root.resolve("original-needed.so"));
                    Files.copy(root.resolve("original-needed.so"), library);
                }
            }
            return actual;
        })) {
            var failure = assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), f.loading(),
                    alias ? 1 : 0, alias ? 3 : 2, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage(), failure.getMessage().contains("between its lookup probe and owned ELF"));
            assertTrue(stats.get() >= 3);
            assertEquals(1, captured.get().closes.get());
            assertFalse(captured.get().actual.isOpen());
            assertThrows(IOException.class, owner::parsed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        if (!alias) {
            assertArrayEquals(Files.readAllBytes(root.resolve("original-needed.so")), Files.readAllBytes(library));
        }
    }

    @Test
    public void neededLookupProbeSameByteNewInodeCannotBeHiddenByStableLaterOwnedRead() throws Exception {
        neededLookupProbeReplacement(false);
    }

    @Test
    public void neededLookupProbeSameTargetNewAliasCannotBeHiddenByStableLaterOwnedRead() throws Exception {
        neededLookupProbeReplacement(true);
    }

    @Test
    public void neededLookupDirectoryChangeAtContentCloseCannotReturnLookupFacts() throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && Path.of(f.files().get(1).path()).equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.beforeClose = () -> {
                    try {
                        write(root.resolve("lib/extra"), new byte[]{7});
                        Files.setLastModifiedTime(root.resolve("lib"), FileTime.fromMillis(1));
                    } catch (IOException failed) {
                        throw new AssertionError(failed);
                    }
                };
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            var failure = assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), f.loading(),
                    0, 2, SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("directory changed across its lookup observation"));
            assertEquals(1, captured.get().closes.get());
            assertFalse(captured.get().actual.isOpen());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void neededLookupAbsentProbeChangedToPresentCannotReturnAbsence() throws Exception {
        var f = lookupFixture("libz.so.1");
        var owner = owner();
        owner.initialize();
        Path missing = root.resolve("empty/libz.so.1");
        var changed = new AtomicInteger();
        try (var targets = mockStatic(Files.class, invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (java.nio.file.NoSuchFileException absent) {
                if (invocation.getMethod().getName().equals("readAttributes")
                        && missing.equals(invocation.getArgument(0)) && changed.compareAndSet(0, 1)) {
                    write(missing, new byte[]{7});
                }
                throw absent;
            }
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), f.loading(), 0, 1,
                    SegmentClassicLinuxElfTest.deadline()));
            assertEquals(1, changed.get());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void neededLookupDeniedDirectoryAndUndeclaredAliasAreNeverAbsenceOrContentReads() throws Exception {
        var f = lookupFixture("libz.so.1");
        Path library = Path.of(f.files().get(1).path());
        for (int kind = 0; kind < 3; kind++) {
            if (kind == 1) {
                Files.delete(library);
                Files.createDirectory(library);
            } else if (kind == 2) {
                Files.delete(library);
                Files.createSymbolicLink(library, Path.of("loader.so"));
            }
            var owner = owner();
            owner.initialize();
            boolean denied = kind == 0;
            try (var channels = mockStatic(FileChannel.class, invocation -> {
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && library.equals(invocation.getArgument(0))) {
                    throw new AssertionError("Uncertain or non-file candidate reached content open");
                }
                return invocation.callRealMethod();
            }); var targets = mockStatic(Files.class, invocation -> {
                if (denied && invocation.getMethod().getName().equals("readAttributes")
                        && library.equals(invocation.getArgument(0))) {
                    throw new AccessDeniedException(library.toString());
                }
                return invocation.callRealMethod();
            })) {
                var failure = assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), f.loading(),
                        0, 2, SegmentClassicLinuxElfTest.deadline()));
                if (denied) {
                    assertTrue(failure instanceof AccessDeniedException);
                }
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void neededLookupUnsupportedNamesIndicesAndJoinedByteBoundsRejectBeforeTargetIo() throws Exception {
        var f = lookupFixture("libz.so.1");
        var l = f.loading();
        record LookupCase(SegmentClassicLinuxDeploymentV2.Loading loading, int rule, int directory) { }
        var cases = new java.util.ArrayList<LookupCase>();
        for (int[] indices : List.of(new int[]{2, 2}, new int[]{3, 2}, new int[]{-1, 2},
                new int[]{l.needed().size(), 2}, new int[]{0, -1}, new int[]{0, l.directories().size()})) {
            cases.add(new LookupCase(l, indices[0], indices[1]));
        }
        for (String name : List.of(".", "..")) {
            var metadata = l.elf().stream().map(e -> new SegmentClassicLinuxDeploymentV2.Elf(e.file(),
                    List.of(name, name, e.needed().get(2), e.needed().get(3)), e.rpath(), e.runpath(), e.flags(),
                    e.flags1())).toList();
            cases.add(new LookupCase(new SegmentClassicLinuxDeploymentV2.Loading(l.systemctl(), l.loader(),
                    l.interpreter(), l.aliases(), l.directories(), l.configuration(), l.policy(), metadata,
                    l.contexts(), l.needed(), l.modules()), 0, 2));
        }
        var excessive = new SegmentClassicLinuxDeploymentV2.Loading(l.systemctl(), l.loader(), l.interpreter(),
                l.aliases(), List.of(new SegmentClassicLinuxDeploymentV2.Directory("/" + "x".repeat(4087), true)),
                l.configuration(), l.policy(), l.elf(), l.contexts(), l.needed(), l.modules());
        cases.add(new LookupCase(excessive, 0, 0));
        for (var input : cases) {
            var owner = owner();
            owner.initialize();
            try (var targets = mockStatic(Files.class, invocation -> {
                throw new AssertionError("Unsupported or invalid lookup data reached target I/O");
            })) {
                assertThrows(IOException.class, () -> owner.readLinuxNeededLookup(f.files(), input.loading(),
                        input.rule(), input.directory(), SegmentClassicLinuxElfTest.deadline()));
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void typedNeededLookupWithoutOriginalProviderSourceCannotReachTargetIo() throws Exception {
        var owner = owner();
        owner.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unbound typed NEEDED lookup reached target I/O");
        })) {
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxNeededLookup(0, 0, SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, owner::parsed);
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void typedLoaderDiagnosticsWithoutOriginalSourceCannotConstructOrStartAProcess() throws Exception {
        var owner = owner();
        owner.initialize();
        IOException failure;
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unbound diagnostics reached target I/O");
        }); var builders = mockConstruction(ProcessBuilder.class)) {
            failure = assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoaderDiagnostics(SegmentClassicLinuxElfTest.deadline()));
            assertTrue(builders.constructed().isEmpty());
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoaderDiagnostics(SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, owner::parsed);
        var drained = owner.closeAndDrain();
        assertTrue(drained.drained());
        assertFalse(drained.diagnosticsInFlight());
        assertFalse(drained.diagnosticsStartClaimed());
        assertTrue(drained.diagnosticsDrained());
        assertSame(failure, drained.diagnosticsFailure());
    }

    private static SegmentClassicLinuxElf.Image2 diagnosticsElf(int type, long entry, String interpreter,
            List<String> needed) {
        var absent = new SegmentClassicLinuxDeploymentV2.DynamicPath(false, "");
        return new SegmentClassicLinuxElf.Image2(type, entry, interpreter, null, needed, absent, absent,
                false, 0, false, 0);
    }

    @Test
    public void unverifiedJdkProcessStartupSourceRejectsBeforeProcessConstruction() throws Exception {
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unverified process backend reached target I/O");
        }); var builders = mockConstruction(ProcessBuilder.class)) {
            var failure = assertThrows(IOException.class,
                    SegmentClassicLaunchTrust::requireLinuxDiagnosticsProcessSource);
            assertTrue(failure.getMessage().contains("Unsupported original JDK process startup chain"));
            assertTrue(builders.constructed().isEmpty());
        }
    }

    @Test
    public void diagnosticsFixedCommandAndSelfContainedDataChecksDoNotTouchAPathOrGrantApproval() throws Exception {
        var valid = diagnosticsElf(3, 0x1000, null, List.of());
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Diagnostic command data validation reached target I/O");
        })) {
            for (String path : List.of("/fixed/ld.so", "/fixed:$HOME;雪//./ld.so")) {
                var command = SegmentClassicLaunchTrust.linuxLoaderDiagnosticsCommand(path, valid,
                        SegmentClassicLinuxElfTest.deadline());
                assertEquals(List.of(path, "--list-diagnostics"), command);
                assertThrows(UnsupportedOperationException.class, () -> command.add("/usr/bin/systemctl"));
            }
            for (var elf : List.of(diagnosticsElf(2, 0x1000, null, List.of()), diagnosticsElf(3, 0, null, List.of()),
                    diagnosticsElf(3, 0x1000, "/other/loader", List.of()),
                    diagnosticsElf(3, 0x1000, null, List.of("unverified.so")))) {
                assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.linuxLoaderDiagnosticsCommand(
                        "/fixed/ld.so", elf, SegmentClassicLinuxElfTest.deadline()));
            }
            for (String path : List.of("ld.so", "", "/fixed/" + (char) 0)) {
                assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.linuxLoaderDiagnosticsCommand(path,
                        valid, SegmentClassicLinuxElfTest.deadline()));
            }
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.linuxLoaderDiagnosticsCommand(
                    "/fixed/ld.so", valid, System.nanoTime()));
        }
    }

    @Test
    public void diagnosticsPrePostDataBindingRejectsSameBytesInDifferentOwnedInodes() throws Exception {
        var file = loadPathFile("diagnostics-loader.so", SegmentClassicLinuxDeployment.Role.LOADER);
        Path path = Path.of(file.path());
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-xr-x"));
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, file.path());
        var owner = owner();
        owner.initialize();
        try {
            var before = owner.readLinuxLoadPath(-1, context, file, SegmentClassicLinuxContentTest.expected(),
                    file.path(), Map.of(), SegmentClassicLinuxElfTest.deadline());
            var same = owner.readLinuxLoadPath(-1, context, file, SegmentClassicLinuxContentTest.expected(),
                    file.path(), Map.of(), SegmentClassicLinuxElfTest.deadline());
            SegmentClassicLaunchTrust.requireLinuxDiagnosticsBinding(before, same);
            Path original = root.resolve("old-diagnostics-loader.so");
            Files.move(path, original);
            Files.copy(original, path);
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-xr-x"));
            var changed = owner.readLinuxLoadPath(-1, context, file, SegmentClassicLinuxContentTest.expected(),
                    file.path(), Map.of(), SegmentClassicLinuxElfTest.deadline());
            assertArrayEquals(Files.readAllBytes(original), Files.readAllBytes(path));
            assertFalse(before.content().identity().equals(changed.content().identity()));
            assertThrows(IOException.class,
                    () -> SegmentClassicLaunchTrust.requireLinuxDiagnosticsBinding(before, changed));
            assertThrows(IOException.class,
                    () -> SegmentClassicLaunchTrust.requireLinuxDiagnosticsBinding(null, before));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void candidateContextSetKeepsEveryLogicalViewOriginalOrdinalAndRepeatedModuleRequest() throws Exception {
        var fixture = candidateFixture();
        var owner = owner();
        owner.initialize();
        try {
            var result = owner.readLinuxCandidateContexts(fixture.files(), fixture.loading(),
                    SegmentClassicLinuxElfTest.deadline());
            assertEquals(List.of(0, 1, 2), result.contexts().stream().map(c -> c.context()).toList());
            assertEquals(List.of(0, 0, 1), result.contexts().stream().map(c -> c.member()).toList());
            assertEquals(fixture.loading().contexts().stream().map(c -> c.logicalLoadName()).toList(),
                    result.contexts().stream().map(c -> c.logical()).toList());
            assertEquals(result.contexts().get(0).path().resolved(), result.contexts().get(1).path().resolved());
            assertFalse(result.contexts().get(0).logical().equals(result.contexts().get(1).logical()));
            assertEquals(-1, result.interpreter().context());
            assertEquals(0, result.interpreter().member());
            assertEquals(fixture.loading().interpreter(), result.interpreter().logical());
            assertEquals(12, result.needed().size());
            assertEquals("libz.so.1", result.needed().get(0).name());
            assertEquals(result.needed().get(0).name(), result.needed().get(1).name());
            assertEquals(0, result.needed().get(0).declaration().ordinal());
            assertEquals(1, result.needed().get(1).declaration().ordinal());
            for (int i = 0; i < result.needed().size(); i++) {
                assertSame(fixture.loading().needed().get(i), result.needed().get(i).declaration());
            }
            assertEquals(fixture.loading().modules(), result.modules());
            assertSame(fixture.loading().modules().get(2), result.modules().get(2));
            assertEquals(List.of(1, 0, 0), result.modules().stream().map(m -> m.requester()).toList());
            assertThrows(UnsupportedOperationException.class, () -> result.contexts().clear());
            assertThrows(UnsupportedOperationException.class, () -> result.needed().clear());
            assertThrows(UnsupportedOperationException.class, () -> result.modules().clear());
            assertThrows(IOException.class, owner::installed);
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxCandidateContexts(SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void candidateContextFailureInUnselectedCandidateCannotReturnAPartialCollection() throws Exception {
        var fixture = candidateFixture();
        var l = fixture.loading();
        var noModules = candidateLoading(l, l.loader(), l.interpreter(), l.needed(), List.of());
        Path module = Path.of(fixture.files().get(1).path());
        byte[] damaged = Files.readAllBytes(module);
        damaged[damaged.length - 1] ^= 1;
        write(module, damaged);
        var owner = owner();
        owner.initialize();
        var loaderOpens = new AtomicInteger();
        var moduleChannel = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3) {
                if (Path.of(fixture.files().get(0).path()).equals(invocation.getArgument(0))) {
                    loaderOpens.incrementAndGet();
                } else if (module.equals(invocation.getArgument(0))) {
                    var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                    moduleChannel.set(fault);
                    return fault;
                }
            }
            return actual;
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxCandidateContexts(fixture.files(), noModules,
                    SegmentClassicLinuxElfTest.deadline()));
            assertTrue(loaderOpens.get() > 0);
            assertEquals(1, moduleChannel.get().closes.get());
            assertFalse(moduleChannel.get().actual.isOpen());
            assertThrows(IOException.class, owner::parsed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void candidateSubgraphAndFixedLoaderReferencesRejectBeforeAnyTargetIo() throws Exception {
        var fixture = candidateFixture();
        var l = fixture.loading();
        var invalid = List.of(candidateLoading(l, l.loader(), l.interpreter(),
                        l.needed().subList(0, l.needed().size() - 1), l.modules()),
                candidateLoading(l, 1, l.interpreter(), l.needed(), l.modules()),
                candidateLoading(l, l.loader(), fixture.files().get(1).path(), l.needed(), l.modules()));
        for (var loading : invalid) {
            var owner = owner();
            owner.initialize();
            try (var targets = mockStatic(Files.class, invocation -> {
                throw new AssertionError("Invalid candidate references reached target I/O");
            })) {
                assertThrows(IOException.class, () -> owner.readLinuxCandidateContexts(fixture.files(), loading,
                        SegmentClassicLinuxElfTest.deadline()));
            } finally {
                assertTrue(owner.closeAndDrain().drained());
            }
        }
        var owner = owner();
        owner.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Oversized candidate file table reached target I/O");
        })) {
            var excessive = java.util.Collections.nCopies(1025, fixture.files().get(0));
            assertThrows(IOException.class, () -> owner.readLinuxCandidateContexts(excessive, l,
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void typedCandidateUniverseWithoutOriginalSourceCannotReachTargetIo() throws Exception {
        var owner = owner();
        owner.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unbound candidate context set reached target I/O");
        })) {
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxCandidateContexts(SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, owner::parsed);
        assertTrue(owner.closeAndDrain().drained());
    }

    @Test
    public void loadPathAssociationKeepsOriginalIndicesLogicalSpellingAndTheSameOwnedMember() throws Exception {
        Files.createDirectories(root.resolve("lib/nested"));
        var file = loadPathFile("lib/physical.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        Path alias = Files.createSymbolicLink(root.resolve("view"), Path.of("lib"));
        String logical = alias + "/nested/../physical.so";
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, logical);
        var owner = owner();
        owner.initialize();
        var opens = new AtomicInteger();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && Path.of(file.path()).equals(invocation.getArgument(0))) {
                opens.incrementAndGet();
            }
            return invocation.callRealMethod();
        })) {
            var result = owner.readLinuxLoadPath(7, context, file, SegmentClassicLinuxContentTest.expected(),
                    logical, Map.of(alias, "lib"), SegmentClassicLinuxElfTest.deadline());
            assertEquals(7, result.context());
            assertEquals(4, result.member());
            assertEquals(logical, result.logical());
            assertEquals(Path.of(file.path()), result.path().resolved());
            assertEquals(result.path().identity(), result.content().identity());
            assertEquals(file.sha256(), result.content().sha256());
            assertEquals(SegmentClassicLinuxContentTest.expected().needed(), result.content().elf().needed());
            assertEquals(1, opens.get());
            assertThrows(IOException.class, owner::installed);
            assertThrows(IOException.class,
                    () -> owner.verifyLinuxLoadContext(7, SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void loadPathReferenceAndPhysicalMismatchCannotOpenUnapprovedContent() throws Exception {
        var file = loadPathFile("physical.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        var mismatched = new SegmentClassicLinuxDeploymentV2.Context(5, file.path());
        var owner = owner();
        owner.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Mismatched load reference reached target metadata");
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxLoadPath(7, mismatched, file,
                    SegmentClassicLinuxContentTest.expected(), file.path(), Map.of(),
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        Path wrong = root.resolve("wrong.so");
        write(wrong, new byte[]{7});
        var physicalMismatch = owner();
        physicalMismatch.initialize();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            throw new AssertionError("Physical mismatch reached content open");
        })) {
            var context = new SegmentClassicLinuxDeploymentV2.Context(4, wrong.toString());
            assertThrows(IOException.class, () -> physicalMismatch.readLinuxLoadPath(7, context, file,
                    SegmentClassicLinuxContentTest.expected(), file.path(), Map.of(),
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(physicalMismatch.closeAndDrain().drained());
        }
    }

    @Test
    public void loadPathSameByteReplacementAfterOwnedReadCannotReturnAnAssociation() throws Exception {
        Files.createDirectory(root.resolve("lib"));
        var file = loadPathFile("lib/physical.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        Path alias = Files.createSymbolicLink(root.resolve("view"), Path.of("lib"));
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, alias + "/physical.so");
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        var changed = new AtomicInteger();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && Path.of(file.path()).equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                captured.set(fault);
                return fault;
            }
            return actual;
        }); var targets = mockStatic(Files.class, invocation -> {
            var channel = captured.get();
            if (invocation.getMethod().getName().equals("readAttributes") && alias.equals(invocation.getArgument(0))
                    && channel != null && channel.closes.get() == 1 && changed.compareAndSet(0, 1)) {
                Path original = root.resolve("original.so");
                Files.move(Path.of(file.path()), original);
                Files.copy(original, Path.of(file.path()));
            }
            return invocation.callRealMethod();
        })) {
            var failure = assertThrows(IOException.class, () -> owner.readLinuxLoadPath(7, context, file,
                    SegmentClassicLinuxContentTest.expected(), context.logicalLoadName(), Map.of(alias, "lib"),
                    SegmentClassicLinuxElfTest.deadline()));
            assertTrue(failure.getMessage().contains("identity changed across the original ELF read"));
            assertEquals(1, changed.get());
            assertEquals(1, captured.get().closes.get());
            assertFalse(captured.get().actual.isOpen());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        assertArrayEquals(Files.readAllBytes(root.resolve("original.so")), Files.readAllBytes(Path.of(file.path())));
    }

    @Test
    public void loadPathAliasChangeAtFirstCloseCannotReturnAnAssociation() throws Exception {
        Files.createDirectory(root.resolve("lib"));
        Files.createDirectory(root.resolve("other"));
        var file = loadPathFile("lib/physical.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        loadPathFile("other/physical.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        Path alias = Files.createSymbolicLink(root.resolve("view"), Path.of("lib"));
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, alias + "/physical.so");
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && Path.of(file.path()).equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.beforeClose = () -> {
                    try {
                        Files.delete(alias);
                        Files.createSymbolicLink(alias, Path.of("other"));
                    } catch (IOException failed) {
                        throw new AssertionError(failed);
                    }
                };
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxLoadPath(7, context, file,
                    SegmentClassicLinuxContentTest.expected(), context.logicalLoadName(), Map.of(alias, "lib"),
                    SegmentClassicLinuxElfTest.deadline()));
            assertEquals(Path.of("other"), Files.readSymbolicLink(alias));
            assertEquals(1, captured.get().closes.get());
            assertFalse(captured.get().actual.isOpen());
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void interpreterDataUsesOnlyLoaderMarkerAndRequiresExecutePermission() throws Exception {
        var file = loadPathFile("loader.so", SegmentClassicLinuxDeployment.Role.LOADER);
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, file.path());
        var owner = owner();
        owner.initialize();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            throw new AssertionError("Non-executable interpreter reached content open");
        })) {
            assertThrows(IOException.class, () -> owner.readLinuxLoadPath(-1, context, file,
                    SegmentClassicLinuxContentTest.expected(), file.path(), Map.of(),
                    SegmentClassicLinuxElfTest.deadline()));
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
        Files.setPosixFilePermissions(Path.of(file.path()), PosixFilePermissions.fromString("rwxr-xr-x"));
        var executable = owner();
        executable.initialize();
        try {
            var result = executable.readLinuxLoadPath(-1, context, file, SegmentClassicLinuxContentTest.expected(),
                    file.path(), Map.of(), SegmentClassicLinuxElfTest.deadline());
            assertEquals(-1, result.context());
            assertEquals(4, result.member());
            assertEquals(file.path(), result.logical());
            assertEquals(result.path().identity(), result.content().identity());
            assertThrows(IOException.class, executable::installed);
        } finally {
            assertTrue(executable.closeAndDrain().drained());
        }
    }

    @Test
    public void loadPathReadKeepsSelfDrainRefusalAndExternalCloseWaitsForActualElfRead() throws Exception {
        var file = loadPathFile("inflight.so", SegmentClassicLinuxDeployment.Role.LIBRARY);
        var context = new SegmentClassicLinuxDeploymentV2.Context(4, file.path());
        var owner = owner();
        owner.initialize();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var captured = new AtomicReference<FileChannel>();
        var read = new FutureTask<>(() -> {
            try (var channels = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && Path.of(file.path()).equals(invocation.getArgument(0))) {
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
                return owner.readLinuxLoadPath(7, context, file, SegmentClassicLinuxContentTest.expected(),
                        file.path(), Map.of(), SegmentClassicLinuxElfTest.deadline());
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
            assertEquals(7, read.get(5, TimeUnit.SECONDS).context());
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
    public void typedLoadContextAndInterpreterWithoutOriginalSourceCannotReachTargetIo() throws Exception {
        for (int index : new int[]{-1, 0, Integer.MAX_VALUE}) {
            var owner = owner();
            owner.initialize();
            try (var targets = mockStatic(Files.class, invocation -> {
                throw new AssertionError("Unbound D2 context reached target I/O");
            })) {
                assertThrows(IOException.class,
                        () -> owner.verifyLinuxLoadContext(index, SegmentClassicLinuxElfTest.deadline()));
            }
            assertThrows(IOException.class, owner::parsed);
            assertTrue(owner.closeAndDrain().drained());
        }
        var interpreter = owner();
        interpreter.initialize();
        try (var targets = mockStatic(Files.class, invocation -> {
            throw new AssertionError("Unbound D2 interpreter reached target I/O");
        })) {
            assertThrows(IOException.class,
                    () -> interpreter.verifyLinuxInterpreter(SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, interpreter::parsed);
        assertTrue(interpreter.closeAndDrain().drained());
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

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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real file/JAR fixtures, with a synthetic JDK-shaped tree. Never an installed program or supervised child. */
public class SegmentClassicLaunchProgramTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicLaunchProgramTest.class.getResourceAsStream("/unified-wal-segment/" + name)) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }

    private static long deadline() {
        return System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    }

    @Test
    public void postLoadProgramAndConfigurationUseTheSameEarlyBytesAndDeadlineButFixtureNeverBecomesInstalled()
            throws Exception {
        try (var f = new Fixture(null, false)) {
            byte[] configuration = SegmentClassicLaunchConfigurationTest.configuration();
            Path conf = Files.write(f.root.resolve("classic-1.conf"), configuration);
            Files.setPosixFilePermissions(conf, PosixFilePermissions.fromString("rw-------"));
            byte[] recipe = Files.readAllBytes(f.root.resolve("classic-1.recipe"));
            ByteBuffer.wrap(recipe).position(115).put(SegmentClassicLaunchConfiguration.parse(configuration)
                    .identity().value());
            Files.write(f.root.resolve("classic-1.recipe"), recipe);
            var early = SegmentClassicLaunchPreload.forFileIoTests(f.root, "classic-1", new UnixSystem().getUid(),
                    deadline());
            early.initialize();
            try {
                var later = SegmentClassicLaunchTrust.forPreloadedIoTests(early);
                f.owners.add(later);
                later.initialize();
                assertEquals(f.program, later.verifyProgram(early.deadline()));
                assertEquals(2, later.verifyConfiguration(early.deadline()).properties().get("serverNumIOThreads"));
                assertThrows(IOException.class, later::installed);
            } finally {
                for (var owner : f.owners) {
                    assertTrue(owner.closeAndDrain().drained());
                }
                assertTrue(early.closeAndDrain().drained());
            }
        }
        try (var f = new Fixture(null, false)) {
            var early = SegmentClassicLaunchPreload.forFileIoTests(f.root, "classic-1", new UnixSystem().getUid(),
                    deadline());
            early.initialize();
            try {
                var later = SegmentClassicLaunchTrust.forPreloadedIoTests(early);
                f.owners.add(later);
                later.initialize();
                assertThrows(IOException.class, () -> later.verifyProgram(early.deadline() + 1));
                assertThrows(IOException.class, later::parsed);
            } finally {
                for (var owner : f.owners) {
                    assertTrue(owner.closeAndDrain().drained());
                }
                assertTrue(early.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void rawProgramPathsSurviveBeforeTheLegacyPathRepresentationCollapsesSlashes() throws Exception {
        byte[] original = SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1");
        byte[] changed = SegmentClassicLinuxProviderTest.replaceFirstText(original, "/opt/jdk", "/opt//jdk");
        changed = SegmentClassicLinuxProviderTest.replaceFirstText(changed, "/opt/jdk/bin/java", "/opt//jdk/bin/java");
        var decoded = SegmentClassicLaunchProgram.decodeWithRawPaths(changed);
        assertEquals("/opt//jdk", decoded.javaHome());
        assertEquals("/opt//jdk/bin/java", decoded.filePaths().get(0));
        assertEquals(Path.of("/opt/jdk"), decoded.value().javaHome());
        assertEquals(Path.of("/opt/jdk/bin/java"), decoded.value().files().get(0).path());
        assertThrows(UnsupportedOperationException.class, () -> decoded.filePaths().clear());
        assertArrayEquals(original, encode(decoded.value()));
    }

    @Test
    public void independentProgramVectorPinsRolesOrderedClasspathArgumentsAndDigest() throws Exception {
        byte[] bytes = vector("classic-launch-program-v1.hex");
        var p = SegmentClassicLaunchProgram.decode(bytes);
        assertEquals("macos", p.os());
        assertEquals(Path.of("/fixed/jdk"), p.javaHome());
        assertEquals(List.of(0, 2), p.classPath());
        assertEquals(7, p.files().size());
        assertEquals(684, bytes.length);
        assertEquals("82bd4d851167415565f8226f523de006dc7d590f9a9e4e07f34d87aa1dcc7e07",
                HexFormat.of().formatHex(SegmentClassicLaunchProgram.identity(bytes).value()));
        assertArrayEquals(bytes, encode(p));
        var command = p.command("classic-1");
        assertEquals(List.of("/fixed/jdk/bin/java", "-Xms64m", "-Xmx128m", "-XX:MaxDirectMemorySize=64m",
                "-XX:ActiveProcessorCount=2", "-cp", "/fixed/a.jar:/fixed/b.jar",
                "org.apache.bookkeeper.server.ControlledClassicMain", "classic-1"), command);
        var reversed = copy(p, List.of(2, 0), p.jvmArguments());
        assertNotEquals(SegmentClassicLaunchProgram.identity(bytes),
                SegmentClassicLaunchProgram.identity(encode(reversed)));
        assertThrows(UnsupportedOperationException.class, () -> command.add("-Dextra=true"));
    }

    @Test
    public void malformedFramingAndUnsupportedJvmOptionsCannotBecomeLaunchArguments() throws Exception {
        byte[] bytes = vector("classic-launch-program-v1.hex");
        for (int i = 0; i < bytes.length; i++) {
            byte[] truncated = Arrays.copyOf(bytes, i);
            assertThrows(IOException.class, () -> SegmentClassicLaunchProgram.decode(truncated));
        }
        for (int[] change : List.of(new int[]{8, 0x00010001}, new int[]{12, 0})) {
            byte[] changed = bytes.clone();
            ByteBuffer.wrap(changed).putInt(change[0], change[1]);
            assertThrows(IOException.class, () -> SegmentClassicLaunchProgram.decode(changed));
        }
        var p = SegmentClassicLaunchProgram.decode(bytes);
        for (String option : List.of("-javaagent:/x", "-Djava.system.class.loader=X", "--module-path=/x",
                "-Xbootclasspath/a:/x", "--patch-module=java.base=/x", "@args", "-jar", "-XX:OnError=command")) {
            var args = new ArrayList<>(p.jvmArguments());
            args.add(option);
            assertThrows(IllegalArgumentException.class, () -> copy(p, p.classPath(), args));
        }
        assertThrows(IllegalArgumentException.class, () -> copy(p, p.classPath(), List.of("-Xmx128m")));
        var duplicate = new ArrayList<>(p.jvmArguments());
        duplicate.add("-Xmx256m");
        assertThrows(IllegalArgumentException.class, () -> copy(p, p.classPath(), duplicate));
        assertThrows(IllegalArgumentException.class, () -> copy(p, List.of(0, 0), p.jvmArguments()));
        assertThrows(IOException.class, () -> SegmentClassicLaunchProgram.decode(new byte[1024 * 1024 + 1]));
    }

    @Test
    public void environmentIsEmptyAndInjectionSourcesAreRejectedBeforeSpawn() {
        assertEquals(Map.of(), SegmentClassicLaunchProgram.environment(Map.of("HOME", "/caller", "PATH", "/caller")));
        for (String key : List.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "CLASSPATH",
                "BOOKKEEPER_NATIVE_IO_LIBRARY_PATH", "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT",
                "DYLD_INSERT_LIBRARIES")) {
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentClassicLaunchProgram.environment(Map.of(key, "unapproved")));
        }
    }

    @Test
    public void realProgramFilesStreamHashAndEnumerateTheEntireDeclaredJdkWithoutReturningInstalledTrust()
            throws Exception {
        try (var f = new Fixture(null, false)) {
            var owner = f.owner();
            var verified = owner.verifyProgram(deadline());
            assertEquals(f.program, verified);
            assertThrows(IOException.class, () -> owner.verifyProgram(deadline()));
            assertThrows(IOException.class, owner::installed);
            assertTrue(owner.closeAndDrain().drained());
            assertArrayEquals(f.programBytes, Files.readAllBytes(f.root.resolve("classic-1.program")));
        }
    }

    @Test
    public void elapsedCallerDeadlineCannotReadOrRestartTheSameProgramVerification() throws Exception {
        try (var f = new Fixture(null, false)) {
            var owner = f.owner();
            var error = assertThrows(IOException.class, () -> owner.verifyProgram(System.nanoTime() - 1));
            assertTrue(error.getMessage().contains("deadline elapsed"));
            assertThrows(IOException.class, () -> owner.verifyProgram(deadline()));
            assertSame(error, owner.closeAndDrain().programFailure());
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void changedDependencyBytesClasspathOrderAndUndeclaredJdkFilesAreRejectedWithoutRepair() throws Exception {
        try (var f = new Fixture(null, false)) {
            byte[] bytes = Files.readAllBytes(f.jar);
            bytes[bytes.length - 1] ^= 1;
            Files.write(f.jar, bytes);
            var owner = f.owner();
            assertTrue(assertThrows(IOException.class, () -> owner.verifyProgram(deadline()))
                    .getMessage().contains("SHA-256"));
            assertThrows(IOException.class, owner::parsed);
            assertTrue(owner.closeAndDrain().drained());
            assertArrayEquals(bytes, Files.readAllBytes(f.jar));
        }
        try (var f = new Fixture(null, false)) {
            Files.write(f.root.resolve("classic-1.program"), encode(copy(f.program,
                    List.of(f.program.classPath().get(1), f.program.classPath().get(0)), f.program.jvmArguments())));
            var owner = f.owner();
            assertTrue(assertThrows(IOException.class, () -> owner.verifyProgram(deadline()))
                    .getMessage().contains("recipe digest"));
        }
        try (var f = new Fixture(null, false)) {
            Path unknown = f.javaHome.resolve("conf/unlisted");
            Files.writeString(unknown, "not declared");
            var owner = f.owner();
            assertTrue(assertThrows(IOException.class, () -> owner.verifyProgram(deadline()))
                    .getMessage().contains("undeclared"));
            assertEquals("not declared", Files.readString(unknown));
        }
    }

    @Test
    public void implicitJarClasspathAndJarIndexAreRejectedAfterTheirRealFileHashesMatch() throws Exception {
        for (boolean index : new boolean[]{false, true}) {
            try (var f = new Fixture(index ? null : "hidden.jar", index)) {
                var owner = f.owner();
                assertTrue(assertThrows(IOException.class, () -> owner.verifyProgram(deadline()))
                        .getMessage().contains("Implicit JAR"));
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    @Test
    public void failedJdkJarConstructionCannotInventItsHiddenNativeCloseResult() throws Exception {
        try (var f = new Fixture(null, false)) {
            byte[] invalid = new byte[(int) Files.size(f.jar)];
            Arrays.fill(invalid, (byte) 7);
            Files.write(f.jar, invalid);
            List<SegmentClassicLaunchProgram.File> rows = new ArrayList<>(f.program.files());
            rows.set(f.program.classPath().get(0), new SegmentClassicLaunchProgram.File(f.jar, 8, invalid.length,
                    new SegmentClassicHandoff.Digest(MessageDigest.getInstance("SHA-256").digest(invalid))));
            var p = f.program;
            byte[] bytes = encode(new SegmentClassicLaunchProgram(p.os(), p.architecture(), p.runtime(), p.vendor(),
                    p.javaHome(), p.mainClass(), p.javaFile(), p.aclJniFile(), p.classPath(), p.jvmArguments(), rows));
            Files.write(f.root.resolve("classic-1.program"), bytes);
            byte[] recipe = Files.readAllBytes(f.root.resolve("classic-1.recipe"));
            ByteBuffer.wrap(recipe).position(83).put(SegmentClassicLaunchProgram.identity(bytes).value());
            Files.write(f.root.resolve("classic-1.recipe"), recipe);
            var owner = f.owner();
            IOException failure = assertThrows(IOException.class, () -> owner.verifyProgram(deadline()));
            f.unresolved.add(owner);
            assertFalse(owner.closeAndDrain().drained());
            assertSame(failure, owner.closeAndDrain().programFailure());
            assertSame(failure, owner.closeAndDrain().auxiliaryCloseFailure().getCause());
            assertEquals(1, owner.closeAndDrain().retainedAuxiliaryReads());
        }
    }

    @Test
    public void firstDirectoryCloseFailureIsRetainedAndCannotBeRetriedIntoSuccess() throws Exception {
        try (var f = new Fixture(null, false)) {
            var owner = f.owner();
            var captured = new AtomicReference<DirectoryStream<Path>>();
            AtomicInteger closes = new AtomicInteger();
            IOException failure = new IOException("JDK directory native close");
            try (var streams = mockStatic(Files.class, invocation -> {
                Object value = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("newDirectoryStream")
                        && invocation.getMethod().getParameterCount() == 1
                        && f.javaHome.equals(invocation.getArgument(0))) {
                    @SuppressWarnings("unchecked")
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
                assertSame(failure, assertThrows(IOException.class, () -> owner.verifyProgram(deadline())));
            }
            try {
                f.unresolved.add(owner);
                assertFalse(owner.closeAndDrain().drained());
                assertSame(failure, owner.closeAndDrain().auxiliaryCloseFailure());
                assertEquals(1, owner.closeAndDrain().retainedAuxiliaryReads());
                assertEquals(1, closes.get());
            } finally {
                captured.get().close();
            }
            assertFalse(owner.closeAndDrain().drained());
            assertEquals(1, closes.get());
        }
    }

    @Test
    public void streamHashNativeCloseFailureCannotExposeProgramOrRetryTheOriginalDescriptor() throws Exception {
        try (var f = new Fixture(null, false)) {
            var owner = f.owner();
            Path executable = f.program.files().get(f.program.javaFile()).path();
            var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
            IOException failure = new IOException("program hash native close");
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && executable.equals(invocation.getArgument(0))) {
                    var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                    fault.closeFailure = failure;
                    fault.beforeClose = () -> assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    captured.set(fault);
                    return fault;
                }
                return actual;
            })) {
                assertSame(failure, assertThrows(IOException.class, () -> owner.verifyProgram(deadline())));
            }
            try {
                f.unresolved.add(owner);
                assertFalse(owner.closeAndDrain().drained());
                assertSame(failure, owner.closeAndDrain().files().closeFailure());
                assertThrows(IOException.class, owner::parsed);
                assertEquals(1, captured.get().closes.get());
                assertTrue(captured.get().actual.isOpen());
            } finally {
                captured.get().actual.close();
            }
            assertFalse(owner.closeAndDrain().drained());
        }
    }

    @Test(timeout = 30000)
    public void realProgramOpenBodyOutlivesCallerTimeoutAndExternalCloseWaitsForIt() throws Exception {
        try (var f = new Fixture(null, false)) {
            var owner = f.owner();
            Path executable = f.program.files().get(f.program.javaFile()).path();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var read = new FutureTask<SegmentClassicLaunchProgram>(() -> {
                try (var opens = mockStatic(FileChannel.class, invocation -> {
                    Object actual = invocation.callRealMethod();
                    if (invocation.getMethod().getName().equals("open")
                            && invocation.getMethod().getParameterCount() == 3
                            && executable.equals(invocation.getArgument(0))) {
                        assertThrows(IllegalStateException.class, owner::closeAndDrain);
                        entered.countDown();
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    }
                    return actual;
                })) {
                    return owner.verifyProgram(deadline());
                }
            });
            var close = new FutureTask<>(owner::closeAndDrain);
            Thread reader = new Thread(read);
            Thread closer = new Thread(close);
            try {
                reader.start();
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                closer.start();
                await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
                assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
                release.countDown();
                assertEquals(f.program, read.get(10, TimeUnit.SECONDS));
                assertTrue(close.get(5, TimeUnit.SECONDS).drained());
            } finally {
                release.countDown();
                reader.join(10000);
                if (closer.getState() != Thread.State.NEW) {
                    closer.join(10000);
                }
            }
            assertFalse(reader.isAlive());
            assertFalse(closer.isAlive());
        }
    }

    private static SegmentClassicLaunchProgram copy(SegmentClassicLaunchProgram p,
            List<Integer> jars, List<String> args) {
        return new SegmentClassicLaunchProgram(p.os(), p.architecture(), p.runtime(), p.vendor(), p.javaHome(),
                p.mainClass(), p.javaFile(), p.aclJniFile(), jars, args, p.files());
    }

    private static void text(ByteBuffer b, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        b.putShort((short) bytes.length).put(bytes);
    }

    private static byte[] encode(SegmentClassicLaunchProgram p) {
        ByteBuffer b = ByteBuffer.allocate(SegmentClassicLaunchProgram.MAX_BYTES);
        b.putLong(0x5557434c50303031L).putShort((short) 1).putShort((short) 0).putInt(0);
        for (String value : List.of(p.os(), p.architecture(), p.runtime(), p.vendor(),
                p.javaHome().toString(), p.mainClass())) {
            text(b, value);
        }
        b.putInt(p.javaFile()).putInt(p.aclJniFile()).putInt(p.classPath().size());
        p.classPath().forEach(b::putInt);
        b.putInt(p.jvmArguments().size());
        p.jvmArguments().forEach(value -> text(b, value));
        b.putInt(p.files().size());
        for (var file : p.files()) {
            text(b, file.path().toString());
            b.putInt(file.roles()).putLong(file.length()).put(file.sha256().value());
        }
        b.putInt(12, b.position() - 16);
        return Arrays.copyOf(b.array(), b.position());
    }

    @Test
    public void actualWrappedDefaultFilesystemIsRejectedBeforeD2CandidatePathsEvenAfterPropertyClear()
            throws Exception {
        try (var fixture = new Fixture(null, false)) {
            Path directory = Files.createDirectory(fixture.root.resolve("launcher"));
            Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
            Path descriptor = directory.resolve("classic-1.launcher");
            Files.write(descriptor, SegmentClassicLinuxProviderTest.schemaTwoInput().encode());
            Files.setPosixFilePermissions(descriptor, PosixFilePermissions.fromString("rw-------"));
            var command = List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-Djava.nio.file.spi.DefaultFileSystemProvider="
                            + SegmentClassicLinuxProviderTest.WrapperProvider.class.getName(),
                    "-Dbookkeeper.native.io.library.path=" + System.getProperty("bookkeeper.native.io.library.path"),
                    "-cp", System.getProperty("java.class.path"), WrappedProviderProbe.class.getName(),
                    fixture.root.toString());
            Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
            byte[] output;
            try (var stream = child.getInputStream()) {
                output = stream.readAllBytes();
            }
            assertEquals(new String(output, StandardCharsets.UTF_8), 0, child.waitFor());
            assertTrue(new String(output, StandardCharsets.UTF_8).contains("WRAPPER_REJECTED_PATHS_0_CONTROL_1"));
        }
    }

    /** Real SPI/default filesystem and owned recipe/descriptor reads, still a non-installed fixture. */
    public static final class WrappedProviderProbe {
        public static void main(String[] args) throws Exception {
            var owner = SegmentClassicLaunchTrust.forMaterialIoTests(Path.of(args[0]), "classic-1",
                    new UnixSystem().getUid());
            try {
                owner.initialize();
                System.clearProperty("java.nio.file.spi.DefaultFileSystemProvider");
                SegmentClassicLinuxProviderTest.tripwire = true;
                IOException refused = assertThrows(IOException.class, () -> owner.readLauncherInputs(deadline()));
                assertTrue(refused.getMessage().contains("implementation source"));
                assertEquals(0, SegmentClassicLinuxProviderTest.candidatePaths);
                assertThrows(IOException.class, owner::parsed);
                assertThrows(IOException.class, owner::installed);
                assertThrows(AssertionError.class, () -> Path.of("/explicit-tripwire-control"));
                assertEquals(1, SegmentClassicLinuxProviderTest.candidatePaths);
                SegmentClassicLinuxProviderTest.tripwire = false;
                System.out.write("WRAPPER_REJECTED_PATHS_0_CONTROL_1\n".getBytes(StandardCharsets.US_ASCII));
                System.out.flush();
            } finally {
                SegmentClassicLinuxProviderTest.tripwire = false;
                assertTrue(owner.closeAndDrain().drained());
            }
        }
    }

    private final class Fixture implements AutoCloseable {
        final Path root;
        final Path javaHome;
        final Path jar;
        final SegmentClassicLaunchProgram program;
        final byte[] programBytes;
        final List<SegmentClassicLaunchTrust> owners = new ArrayList<>();
        final Set<SegmentClassicLaunchTrust> unresolved = new HashSet<>();

        Fixture(String hiddenClassPath, boolean jarIndex) throws Exception {
            Path base = temporary.newFolder().toPath().toRealPath();
            Files.setPosixFilePermissions(base, PosixFilePermissions.fromString("rwx------"));
            root = Files.createDirectory(base.resolve("trust"));
            javaHome = Files.createDirectories(base.resolve("installation/jdk"));
            List<SegmentClassicLaunchProgram.File> rows = new ArrayList<>();
            row(rows, javaHome.resolve("bin/java"), 3, new byte[]{1}, true);
            byte[] image = new byte[2 * 1024 * 1024 + 17];
            image[image.length - 1] = 7;
            row(rows, javaHome.resolve("lib/modules"), 1, image, false);
            row(rows, javaHome.resolve("lib/server/libjvm.dylib"), 17, new byte[]{2}, false);
            row(rows, javaHome.resolve("conf/runtime.conf"), 1, new byte[]{3}, false);
            row(rows, javaHome.resolve("release"), 1, new byte[]{4}, false);
            Path nativeFile = base.resolve("installation/acljni.dylib");
            row(rows, nativeFile, 20, new byte[]{5}, false);
            jar = base.resolve("installation/a.jar");
            jar(jar, hiddenClassPath, jarIndex);
            row(rows, jar, 8, Files.readAllBytes(jar), false);
            Path otherJar = base.resolve("installation/b.jar");
            jar(otherJar, null, false);
            row(rows, otherJar, 8, Files.readAllBytes(otherJar), false);
            rows.sort(Comparator.comparing(file -> file.path().toString()));
            List<Path> paths = rows.stream().map(SegmentClassicLaunchProgram.File::path).toList();
            program = new SegmentClassicLaunchProgram("macos", "aarch64", "fixture-runtime", "fixture-vendor",
                    javaHome, SegmentClassicLaunchProgram.MAIN, paths.indexOf(javaHome.resolve("bin/java")),
                    paths.indexOf(nativeFile), List.of(paths.indexOf(jar), paths.indexOf(otherJar)),
                    List.of("-Xms64m", "-Xmx128m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2"), rows);
            programBytes = encode(program);
            Files.write(root.resolve("classic-1.program"), programBytes);
            byte[] recipe = vector("classic-launch-recipe-v1.hex");
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update("BookKeeper/ControlledClassicProgram/v1\0".getBytes(StandardCharsets.US_ASCII));
            digest.update(programBytes);
            ByteBuffer.wrap(recipe).position(83).put(digest.digest());
            Files.write(root.resolve("classic-1.recipe"), recipe);
            Files.write(root.resolve("issuer-1.ed25519"), HexFormat.of().parseHex(
                    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"));
        }

        private void row(List<SegmentClassicLaunchProgram.File> rows, Path path, int roles,
                byte[] bytes, boolean executable) throws Exception {
            Files.createDirectories(path.getParent());
            Files.write(path, bytes);
            Files.setPosixFilePermissions(path,
                    PosixFilePermissions.fromString(executable ? "rwxr-xr-x" : "rw-r--r--"));
            rows.add(new SegmentClassicLaunchProgram.File(path, roles, bytes.length,
                    new SegmentClassicHandoff.Digest(MessageDigest.getInstance("SHA-256").digest(bytes))));
        }

        private void jar(Path path, String hiddenClassPath, boolean index) throws IOException {
            var manifest = new Manifest();
            manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
            if (hiddenClassPath != null) {
                manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, hiddenClassPath);
            }
            try (var out = new JarOutputStream(Files.newOutputStream(path), manifest)) {
                out.putNextEntry(new JarEntry(index ? "META-INF/INDEX.LIST" : "fixture.txt"));
                out.write(new byte[]{7});
                out.closeEntry();
            }
        }

        SegmentClassicLaunchTrust owner() throws IOException {
            var result = SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", new UnixSystem().getUid());
            owners.add(result);
            result.initialize();
            return result;
        }

        @Override
        public void close() throws Exception {
            for (var owner : owners) {
                assertEquals(!unresolved.contains(owner), owner.closeAndDrain().drained());
            }
        }
    }
}

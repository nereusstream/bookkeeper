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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Early read fixtures only, including a fresh JVM proving no native-io was initialized by preload. */
public class SegmentClassicLaunchPreloadTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private Path root;
    private Path recipe;
    private Path program;
    private byte[] recipeBytes;
    private byte[] programBytes;

    private static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicLaunchPreloadTest.class.getResourceAsStream("/unified-wal-segment/" + name)) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }

    @Before
    public void fixture() throws Exception {
        root = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        recipe = root.resolve("classic-1.recipe");
        program = root.resolve("classic-1.program");
        programBytes = vector("classic-launch-program-v1.hex");
        recipeBytes = vector("classic-launch-recipe-v1.hex");
        ByteBuffer.wrap(recipeBytes).position(83).put(SegmentClassicLaunchProgram.identity(programBytes).value());
        Files.write(recipe, recipeBytes);
        Files.write(program, programBytes);
        Files.write(root.resolve("issuer-1.ed25519"), HexFormat.of().parseHex(
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"));
    }

    private SegmentClassicLaunchPreload owner() {
        return SegmentClassicLaunchPreload.forFileIoTests(root, "classic-1", new UnixSystem().getUid(),
                System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
    }

    @Test
    public void originalReadsCanBeBorrowedOnlyOnceAndFixtureCannotLoadOrPromoteInstalledTrust() throws Exception {
        var early = owner();
        assertThrows(IOException.class, early::program);
        early.initialize();
        assertEquals(Path.of("/fixed/jdk"), early.program().javaHome());
        assertThrows(IOException.class, early::initialize);
        assertThrows(IOException.class, early::loadNative);
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.production(early));
        SegmentClassicLaunchTrust later;
        try (var attributes = mockStatic(Files.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("readAttributes")
                    && recipe.equals(invocation.getArgument(0)) && "unix:*".equals(invocation.getArgument(1))) {
                assertThrows(IllegalStateException.class, early::closeAndDrain);
                assertThrows(IllegalStateException.class, () -> early.verifyOriginal(early.deadline()));
            }
            return actual;
        })) {
            later = SegmentClassicLaunchTrust.forPreloadedIoTests(early);
        }
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.forPreloadedIoTests(early));
        later.initialize();
        assertEquals("classic-1", later.parsed().recipe().recipeId());
        assertThrows(IOException.class, later::installed);
        assertTrue(early.closeAndDrain().drained());
        assertThrows(IOException.class, later::parsed);
        assertTrue(later.closeAndDrain().drained());
    }

    @Test
    public void identicalReplacementAndDifferentReadBytesCannotBeSubstitutedForTheOriginalPreload()
            throws Exception {
        var replaced = owner();
        replaced.initialize();
        Files.move(recipe, root.resolve("original-recipe"));
        Files.write(recipe, recipeBytes);
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.forPreloadedIoTests(replaced));
        assertTrue(replaced.closeAndDrain().drained());

        var early = owner();
        early.initialize();
        var later = SegmentClassicLaunchTrust.forPreloadedIoTests(early);
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && recipe.equals(invocation.getArgument(0))) {
                var wrapper = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                doAnswer(read -> {
                    ByteBuffer destination = read.getArgument(0);
                    int start = destination.position();
                    int count = (int) read.callRealMethod();
                    if (count > 0) {
                        destination.put(start, (byte) (destination.get(start) ^ 1));
                    }
                    return count;
                }).when(wrapper).read(any(ByteBuffer.class), anyLong());
                return wrapper;
            }
            return actual;
        })) {
            assertTrue(assertThrows(IOException.class, later::initialize).getMessage().contains("preload bytes"));
        } finally {
            assertTrue(later.closeAndDrain().drained());
            assertTrue(early.closeAndDrain().drained());
        }
    }

    @Test
    public void firstJdkCloseFailureRetainsTheOriginalReadAndNeverReturnsParsedValues() throws Exception {
        var early = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("original early close failure");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && recipe.equals(invocation.getArgument(0))) {
                var channel = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                channel.closeFailure = failure;
                captured.set(channel);
                return channel;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class, early::initialize));
            assertThrows(IOException.class, early::program);
            assertThrows(IOException.class, early::loadNative);
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = early.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.failure());
                assertSame(failure, result.closeFailure());
                assertEquals(1, result.retainedReads());
            }
            assertTrue(captured.get().actual.isOpen());
            assertEquals(1, captured.get().closes.get());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(early.closeAndDrain().drained());
    }

    @Test
    public void acquisitionThatThrowsWithoutAHandleHasNoInventedCloseResultOrRetry() throws Exception {
        var early = owner();
        IOException failure = new IOException("early open result lost after native acquisition");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && recipe.equals(invocation.getArgument(0))) {
                ((FileChannel) actual).close(); // Only the fixture knows this actual cleanup succeeded.
                throw failure;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class, early::initialize));
        }
        assertFalse(early.closeAndDrain().drained());
        assertEquals(1, early.closeAndDrain().retainedReads());
        assertThrows(IOException.class, early::initialize);
    }

    @Test(timeout = 30000)
    public void observationTimeoutDoesNotCancelActualOpenAndExternalCloseWaitsForTheOriginalBody() throws Exception {
        var early = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var initialize = new FutureTask<Void>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && recipe.equals(invocation.getArgument(0))) {
                    assertThrows(IllegalStateException.class, early::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return actual;
            })) {
                early.initialize();
                return null;
            }
        });
        Thread reader = new Thread(initialize, "actual-preload-body");
        reader.start();
        var closing = new FutureTask<>(early::closeAndDrain);
        Thread closer = new Thread(closing, "preload-external-close");
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> initialize.get(20, TimeUnit.MILLISECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertFalse(closing.isDone());
        } finally {
            release.countDown();
            reader.join(5000);
            closer.join(5000);
        }
        initialize.get();
        assertTrue(closing.get().drained());
        assertThrows(IOException.class, early::program);
    }

    @Test
    public void badBoundedFilesAliasesAndExpiredDeadlineAreRejectedWithoutRepair() throws Exception {
        for (boolean recipeCase : List.of(false, true)) {
            Path path = recipeCase ? recipe : program;
            byte[] original = recipeCase ? recipeBytes : programBytes;
            for (byte[] bad : List.of(new byte[0],
                    new byte[recipeCase ? 4097 : SegmentClassicLaunchProgram.MAX_BYTES + 1], new byte[]{7})) {
                Files.write(path, bad);
                var early = owner();
                assertThrows(IOException.class, early::initialize);
                assertTrue(early.closeAndDrain().drained());
                assertEquals(bad.length, Files.size(path));
            }
            Files.write(path, original);
        }
        var expired = SegmentClassicLaunchPreload.forFileIoTests(root, "classic-1", new UnixSystem().getUid(),
                System.nanoTime() - 1);
        assertThrows(IOException.class, expired::initialize);
        assertTrue(expired.closeAndDrain().drained());
        var missing = SegmentClassicLaunchPreload.forFileIoTests(root.resolve("missing"), "classic-1",
                new UnixSystem().getUid(), System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        assertFalse(Files.exists(root.resolve("missing")));
        assertThrows(IOException.class, missing::initialize);
        assertTrue(missing.closeAndDrain().drained());
        assertFalse(Files.exists(root.resolve("missing")));
        Files.move(recipe, root.resolve("target"));
        Files.createSymbolicLink(recipe, root.resolve("target"));
        var alias = owner();
        assertThrows(IOException.class, alias::initialize);
        assertTrue(alias.closeAndDrain().drained());
    }

    @Test
    public void freshJvmCanReadPreloadWithAnUnusableNativeOverrideAndInitializeNativeIoOnlyAfterwards()
            throws Exception {
        Path log = root.resolve("jdk-only-probe.log");
        String nativeLibrary = System.getProperty("bookkeeper.native.io.library.path");
        assertTrue(nativeLibrary != null);
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-cp", System.getProperty("java.class.path"), JdkOnlyProbe.class.getName(), root.toString(),
                nativeLibrary).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS));
            String transcript = Files.readString(log);
            System.out.print(transcript);
            assertEquals(transcript, 0, process.exitValue());
            assertTrue(transcript.contains("JDK_PRELOAD_BEFORE_NATIVE_OK"));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(30, TimeUnit.SECONDS));
            }
            process.getInputStream().close();
            process.getOutputStream().close();
            process.getErrorStream().close();
        }
    }

    public static final class JdkOnlyProbe {
        public static void main(String[] args) throws Exception {
            System.setProperty("bookkeeper.native.io.library.path", "/never/load/this/early/library");
            var early = SegmentClassicLaunchPreload.forFileIoTests(Path.of(args[0]), "classic-1",
                    new UnixSystem().getUid(), System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
            early.initialize();
            assertEquals(Path.of("/fixed/jdk"), early.program().javaHome());
            assertThrows(IOException.class, early::loadNative);
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.production(early));
            assertTrue(early.closeAndDrain().drained());
            System.setProperty("bookkeeper.native.io.library.path", args[1]);
            assertFalse(new NativeIOImpl().hasExtendedAcl(Path.of(args[0], "classic-1.recipe").toString()));
            System.out.println("JDK_PRELOAD_BEFORE_NATIVE_OK");
        }
    }
}

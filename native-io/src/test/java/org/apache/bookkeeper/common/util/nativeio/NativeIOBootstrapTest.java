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
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.Permission;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Independent JDK-17/21 JVMs and actual library fixtures. Never a system installation or child START. */
@SuppressWarnings("removal")
public class NativeIOBootstrapTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private void run(String scenario) throws Exception {
        String source = NativeIOLibraryPath.configuredLibraryPath();
        assumeTrue("Actual host JNI must be supplied", source != null);
        assumeTrue("Test interposition uses the supported JDK SecurityManager", Runtime.version().feature() <= 21);
        Path root = temporary.newFolder(scenario).toPath().toRealPath();
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        Path log = root.resolve("process.log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.security.manager=allow", "-cp", System.getProperty("java.class.path"),
                Probe.class.getName(), scenario, source, root.toString());
        builder.environment().clear();
        if (scenario.equals("environment")) {
            builder.environment().put("BOOKKEEPER_NATIVE_IO_LIBRARY_PATH", source);
        }
        Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(scenario + "\n" + Files.readString(log), process.waitFor(30, TimeUnit.SECONDS));
            String transcript = Files.readString(log);
            System.out.print(transcript);
            assertEquals(scenario + "\n" + transcript, 0, process.exitValue());
            assertTrue(transcript.contains("PROBE_OK " + scenario));
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

    @Test
    public void ordinaryResultsCannotBeUpgradedAndOtherClassLoaderCopiesCannotResetTheAttempt() throws Exception {
        for (String scenario : List.of("ordinary-success", "ordinary-failure", "other-loader")) {
            run(scenario);
        }
    }

    @Test
    public void actualFileChecksAndFirstCloseMustFinishBeforeAnyNativeLoad() throws Exception {
        for (String scenario : List.of("hash", "length", "alias", "replacement", "read-failure", "close-failure",
                "expired", "unbounded", "property", "environment")) {
            run(scenario);
        }
    }

    @Test
    public void exactFixtureLoadsOnceAndJniInitializationReusesItWithoutProductionProvenance() throws Exception {
        run("success");
    }

    @Test
    public void concurrentReentrantAndLateNativeCallsCannotPublishAControlledResult() throws Exception {
        for (String scenario : List.of("concurrent", "ordinary-interference", "reentrant", "late-load",
                "load-failure")) {
            run(scenario);
        }
    }

    /** JDK permission hooks interpose real I/O/load boundaries without adding production callback or reset APIs. */
    public static final class Probe {
        private static Object retainedRead() throws Exception {
            Field field = NativeIOBootstrap.class.getDeclaredField("read");
            field.setAccessible(true);
            return field.get(null);
        }

        private static Field channelField(Object read) throws Exception {
            Field field = read.getClass().getDeclaredField("channel");
            field.setAccessible(true);
            return field;
        }

        public static void main(String[] args) throws Exception {
            String scenario = args[0];
            Path source = Path.of(args[1]);
            Path file = Files.copy(source, Path.of(args[2]).resolve(source.getFileName()));
            Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
            if (scenario.equals("load-failure")) {
                Files.write(file, new byte[]{7});
            }
            long length = Files.size(file);
            long uid = ((Number) Files.getAttribute(file, "unix:uid")).longValue();
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            var links = new AtomicInteger();
            var intercepted = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var captured = new AtomicReference<FileChannel>();
            var fault = new AtomicReference<FailClose>();
            IOException closeFailure = new IOException("original first close failure");
            SecurityException readFailure = new SecurityException("original read failure");
            assertThrows(IOException.class, NativeIOBootstrap::controlledLibrary); // Initialize, never reset.

            System.setSecurityManager(new SecurityManager() {
                @Override
                public void checkPermission(Permission permission) { }

                @Override
                public void checkRead(String path) {
                    if (!file.toString().equals(path) || intercepted.get()) {
                        return;
                    }
                    try {
                        Object read = retainedRead();
                        FileChannel channel = read == null ? null : (FileChannel) channelField(read).get(read);
                        if (channel == null || !intercepted.compareAndSet(false, true)) {
                            return;
                        }
                        captured.set(channel);
                        switch (scenario) {
                            case "replacement" -> {
                                Files.move(file, file.resolveSibling("original-library"));
                                Files.copy(source, file);
                                Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
                            }
                            case "read-failure" -> throw readFailure;
                            case "close-failure" -> {
                                var wrapper = new FailClose(channel, closeFailure);
                                fault.set(wrapper);
                                channelField(read).set(read, wrapper);
                            }
                            case "concurrent", "ordinary-interference" -> {
                                entered.countDown();
                                assertTrue(release.await(5, TimeUnit.SECONDS));
                            }
                            default -> { }
                        }
                    } catch (SecurityException failure) {
                        throw failure;
                    } catch (Exception failure) {
                        throw new AssertionError(failure);
                    }
                }

                @Override
                public void checkLink(String library) {
                    links.incrementAndGet();
                    if (scenario.equals("reentrant")) {
                        assertThrows(NativeIOException.class, () -> new NativeIOImpl().hasExtendedAcl(file.toString()));
                    } else if (scenario.equals("late-load")) {
                        while (deadline - System.nanoTime() >= 0) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException failure) {
                                throw new AssertionError(failure);
                            }
                        }
                    }
                }
            });
            try {
                if (scenario.startsWith("ordinary-") && !scenario.equals("ordinary-interference")
                        || scenario.equals("other-loader")) {
                    System.setProperty("bookkeeper.native.io.library.path",
                            scenario.equals("ordinary-failure") ? file.resolveSibling("missing-library").toString()
                                    : file.toString());
                    if (scenario.equals("ordinary-failure")) {
                        assertThrows(LinkageError.class, () -> NativeIOJni.hasExtendedAcl(file.toString()));
                    } else {
                        assertFalse(new NativeIOImpl().hasExtendedAcl(file.toString()));
                    }
                    System.clearProperty("bookkeeper.native.io.library.path");
                    assertThrows(IOException.class,
                            () -> NativeIOBootstrap.forLibraryIoTests(file, uid, length, hash, deadline));
                    if (scenario.equals("other-loader")) {
                        URL[] urls = java.util.Arrays.stream(System.getProperty("java.class.path")
                                .split(File.pathSeparator)).map(value -> {
                                    try {
                                        return Path.of(value).toUri().toURL();
                                    } catch (Exception failure) {
                                        throw new AssertionError(failure);
                                    }
                                }).toArray(URL[]::new);
                        try (var copy = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
                            Class<?> type = copy.loadClass(NativeIOBootstrap.class.getName());
                            assertNotSameLoader(type);
                            var failure = assertThrows(InvocationTargetException.class, () -> type.getMethod(
                                    "loadControlledClassic", long.class, byte[].class, long.class)
                                    .invoke(null, length, hash, deadline));
                            assertTrue(failure.getCause() instanceof IOException);
                            assertTrue(failure.getCause().getMessage().contains("ClassLoader"));
                        }
                    }
                    assertEquals(1, links.get());
                } else if (scenario.equals("concurrent") || scenario.equals("ordinary-interference")) {
                    var first = new FutureTask<Throwable>(() -> {
                        try {
                            NativeIOBootstrap.forLibraryIoTests(file, uid, length, hash, deadline);
                            return null;
                        } catch (Throwable failure) {
                            return failure;
                        }
                    });
                    Thread worker = new Thread(first, "actual-controlled-load");
                    worker.start();
                    try {
                        assertTrue(entered.await(5, TimeUnit.SECONDS));
                        assertFalse(first.isDone());
                        if (scenario.equals("concurrent")) {
                            assertThrows(IOException.class,
                                    () -> NativeIOBootstrap.forLibraryIoTests(file, uid, length, hash, deadline));
                        } else {
                            assertThrows(NativeIOException.class,
                                    () -> new NativeIOImpl().hasExtendedAcl(file.toString()));
                        }
                        assertFalse(first.isDone());
                    } finally {
                        release.countDown();
                        worker.join(5000);
                    }
                    assertFalse(worker.isAlive());
                    assertNotNull(first.get());
                    assertFalse(captured.get().isOpen());
                    assertEquals(0, links.get());
                } else {
                    if (scenario.equals("property")) {
                        System.setProperty("bookkeeper.native.io.library.path", file.toString());
                    }
                    if (scenario.equals("alias")) {
                        Files.move(file, file.resolveSibling("aliased-library"));
                        Files.createSymbolicLink(file, file.resolveSibling("aliased-library"));
                    }
                    byte[] expected = hash.clone();
                    if (scenario.equals("hash")) {
                        expected[0] ^= 1;
                    }
                    long suppliedLength = scenario.equals("length") ? length + 1 : length;
                    long suppliedDeadline = scenario.equals("expired") ? System.nanoTime() - 1
                            : scenario.equals("unbounded") ? Long.MAX_VALUE : deadline;
                    if (scenario.equals("success")) {
                        var loaded = NativeIOBootstrap.forLibraryIoTests(file, uid, length, expected, suppliedDeadline);
                        assertEquals(file, loaded.path());
                        assertEquals(length, loaded.length());
                        assertArrayEquals(hash, loaded.sha256());
                        loaded.requireUnchanged();
                        loaded.sha256()[0] ^= 1;
                        assertArrayEquals(hash, loaded.sha256());
                        // If JNI consulted the ordinary selector, this intentionally invalid override would fail.
                        System.setProperty("bookkeeper.native.io.library.path", "/no/ordinary/fallback");
                        assertFalse(new NativeIOImpl().hasExtendedAcl(file.toString()));
                        assertEquals(1, links.get());
                        assertThrows(IOException.class,
                                () -> NativeIOBootstrap.forLibraryIoTests(file, uid, length, hash, deadline));
                        Files.move(file, file.resolveSibling("original-loaded-library"));
                        Files.copy(source, file);
                        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
                        assertArrayEquals(hash, MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
                        assertThrows(IOException.class, loaded::requireUnchanged);
                    } else {
                        Throwable problem = assertThrows(Throwable.class, () -> NativeIOBootstrap.forLibraryIoTests(
                                file, uid, suppliedLength, expected, suppliedDeadline));
                        if (scenario.equals("read-failure")) {
                            assertSame(readFailure, problem);
                            assertFalse(captured.get().isOpen());
                        } else if (scenario.equals("close-failure")) {
                            assertSame(closeFailure, problem);
                            assertTrue(captured.get().isOpen());
                            assertSame(fault.get(), channelField(retainedRead()).get(retainedRead()));
                            assertEquals(1, fault.get().closes);
                        } else if (scenario.equals("load-failure")) {
                            assertTrue(problem.toString(), problem instanceof UnsatisfiedLinkError);
                        } else {
                            assertTrue(problem.toString(), problem instanceof IOException);
                        }
                        boolean late = scenario.equals("reentrant") || scenario.equals("late-load");
                        assertEquals(late || scenario.equals("load-failure") ? 1 : 0, links.get());
                        var observed = assertThrows(IOException.class, NativeIOBootstrap::controlledLibrary);
                        assertEquals(observed.getMessage(), late,
                                observed.getMessage().contains("controlledLoadReturned=true"));
                        if (scenario.equals("load-failure")) {
                            assertSame(problem, observed.getCause());
                        }
                    }
                }
                assertThrows(IOException.class, NativeIOBootstrap::controlledLibrary);
                assertThrows(IOException.class, () -> NativeIOBootstrap.loadControlledClassic(length, hash, deadline));
                if (fault.get() != null) {
                    assertEquals(1, fault.get().closes);
                }
                System.out.println("PROBE_OK " + scenario);
            } finally {
                System.setSecurityManager(null);
                if (fault.get() != null) {
                    captured.get().close(); // Fixture cleanup never changes the original failed attempt.
                    assertThrows(IOException.class, NativeIOBootstrap::controlledLibrary);
                }
            }
        }

        private static void assertNotSameLoader(Class<?> type) {
            assertFalse(type.getClassLoader() == ClassLoader.getSystemClassLoader());
        }
    }

    private static final class FailClose extends FileChannel {
        private final FileChannel actual;
        private final IOException failure;
        int closes;

        FailClose(FileChannel actual, IOException failure) {
            this.actual = actual;
            this.failure = failure;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return actual.read(dst);
        }

        @Override
        public long size() throws IOException {
            return actual.size();
        }

        @Override
        protected void implCloseChannel() throws IOException {
            closes++;
            throw failure;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return actual.read(dsts, offset, length);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return actual.read(dst, position);
        }

        @Override
        public long position() throws IOException {
            return actual.position();
        }

        @Override
        public FileChannel position(long position) throws IOException {
            actual.position(position);
            return this;
        }

        @Override
        public int write(ByteBuffer src) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src, long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileChannel truncate(long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force(boolean metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            throw new UnsupportedOperationException();
        }
    }
}

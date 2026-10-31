/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual host JNI and local filesystem atomicity only; no publication durability or handoff authority. */
public class NativeIORenameNoReplaceTest {
    private static final byte[] READY = "READY\n".getBytes(StandardCharsets.US_ASCII);
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private Path root() throws IOException {
        Path root = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        return root;
    }

    private static NativeIO nativeIo() {
        assumeTrue("Host JNI requires Linux packaging or the existing explicit library override",
                SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        return new NativeIOImpl();
    }

    private static void writeForced(Path path, byte[] bytes) throws IOException {
        try (FileChannel file = FileChannel.open(path,
                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
            ByteBuffer input = ByteBuffer.wrap(bytes);
            while (input.hasRemaining()) {
                if (file.write(input) <= 0) {
                    throw new IOException("Fixture file write made no progress");
                }
            }
            file.force(true);
        }
    }

    @Test
    public void anOlderJavaImplementationRejectsTheCapabilityWithoutPerformingIo() throws Exception {
        Path source = root().resolve("legacy-source");
        Path target = root().resolve("legacy-target");
        writeForced(source, new byte[]{7});
        var failure = assertThrows(NativeIOException.class,
                () -> new LegacyImplementation().renameNoReplace(source.toString(), target.toString()));
        assertEquals(-1, failure.getErrno());
        assertThrows(NativeIOException.class,
                () -> new LegacyImplementation().hasExtendedAcl(source.toString()));
        assertThrows(NativeIOException.class, () -> new LegacyImplementation().currentProcessCredentials());
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(source));
        assertFalse(Files.exists(target));
    }

    @Test
    public void successfulUnicodeRenamePreservesTheOriginalInodeSingleLinkAndCompleteBytes() throws Exception {
        NativeIO io = nativeIo();
        Path source = root().resolve("源-\uD834\uDD1E-é.tmp");
        Path target = root().resolve("结果-\uD834\uDD22.proof");
        byte[] contents = new byte[8192];
        Arrays.fill(contents, (byte) 71);
        writeForced(source, contents);
        System.out.println("RENAME_RUNTIME fs=" + Files.getFileStore(source).type()
                + " implementation=" + NativeIOImpl.class.getProtectionDomain().getCodeSource().getLocation()
                + " library=" + NativeIOLibraryPath.configuredLibraryPath());
        Object inode = Files.getAttribute(source, "unix:ino");
        Object device = Files.getAttribute(source, "unix:dev");
        assertEquals(0, io.renameNoReplace(source.toString(), target.toString()));
        assertFalse(Files.exists(source));
        assertArrayEquals(contents, Files.readAllBytes(target));
        assertEquals(inode, Files.getAttribute(target, "unix:ino"));
        assertEquals(device, Files.getAttribute(target, "unix:dev"));
        assertEquals(1, ((Number) Files.getAttribute(target, "unix:nlink")).intValue());
    }

    @Test
    public void existingFilesDirectoriesAndLiveOrDanglingSymlinksAreNeverReplaced() throws Exception {
        NativeIO io = nativeIo();
        Path sentinel = root().resolve("sentinel");
        writeForced(sentinel, new byte[]{9});
        for (int kind = 0; kind < 4; kind++) {
            Path source = root().resolve("source-" + kind);
            Path target = root().resolve("target-" + kind);
            writeForced(source, new byte[]{7});
            switch (kind) {
                case 0 -> writeForced(target, new byte[]{11});
                case 1 -> Files.createDirectory(target);
                case 2 -> Files.createSymbolicLink(target, sentinel);
                case 3 -> Files.createSymbolicLink(target, root().resolve("missing-link-target"));
                default -> throw new AssertionError();
            }
            Object inode = Files.getAttribute(target, "unix:ino", java.nio.file.LinkOption.NOFOLLOW_LINKS);
            var failure = assertThrows(NativeIOException.class,
                    () -> io.renameNoReplace(source.toString(), target.toString()));
            assertTrue(failure.getErrno() > 0);
            assertEquals(inode, Files.getAttribute(target, "unix:ino", java.nio.file.LinkOption.NOFOLLOW_LINKS));
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(source));
            assertArrayEquals(new byte[]{9}, Files.readAllBytes(sentinel));
            if (kind == 0) {
                assertArrayEquals(new byte[]{11}, Files.readAllBytes(target));
            }
        }
    }

    @Test
    public void invalidNullNulAndUtf16NeverRenameAReplacementCharacterOrTruncatedPath() throws Exception {
        NativeIO io = nativeIo();
        Path source = root().resolve("source");
        Path replacement = root().resolve("\uFFFD");
        Path target = root().resolve("target");
        writeForced(source, new byte[]{7});
        writeForced(replacement, new byte[]{9});
        for (String invalid : new String[]{null, source + "\0suffix", root() + "/\uD800", root() + "/\uDC00"}) {
            assertEquals(22, assertThrows(NativeIOException.class,
                    () -> io.renameNoReplace(invalid, target.toString())).getErrno());
            assertEquals(22, assertThrows(NativeIOException.class,
                    () -> io.renameNoReplace(source.toString(), invalid)).getErrno());
            assertFalse(Files.exists(target));
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(source));
            assertArrayEquals(new byte[]{9}, Files.readAllBytes(replacement));
        }
    }

    @Test
    public void missingSourceKeepsItsNativeErrorAndDoesNotCreateATarget() throws Exception {
        NativeIO io = nativeIo();
        Path source = root().resolve("missing");
        Path target = root().resolve("target");
        assertTrue(assertThrows(NativeIOException.class,
                () -> io.renameNoReplace(source.toString(), target.toString())).getErrno() > 0);
        assertFalse(Files.exists(source));
        assertFalse(Files.exists(target));
    }

    @Test
    public void existingNativeOpenSyncAndCloseRemainUsable() throws Exception {
        NativeIO io = nativeIo();
        Path source = root().resolve("legacy-functions");
        writeForced(source, new byte[]{7});
        int descriptor = io.open(source.toString(), NativeIO.O_WRONLY, 0);
        assertTrue(descriptor >= 0);
        try {
            assertEquals(0, io.fsync(descriptor));
        } finally {
            assertEquals(0, io.close(descriptor));
        }
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(source));
    }

    @Test(timeout = 30000)
    public void twoActualProcessesContendForOneTargetAndOnlyOneWins() throws Exception {
        nativeIo();
        Path first = root().resolve("first");
        Path second = root().resolve("second");
        Path target = root().resolve("target");
        writeForced(first, new byte[]{1});
        writeForced(second, new byte[]{2});
        try (Child left = new Child("race", first, target); Child right = new Child("race", second, target)) {
            left.ready();
            right.ready();
            left.go();
            right.go();
            String l = left.result();
            String r = right.result();
            assertEquals(1, (l.equals("WIN") ? 1 : 0) + (r.equals("WIN") ? 1 : 0));
            assertTrue(l.equals("LOSE") || r.equals("LOSE"));
            boolean firstWon = l.equals("WIN");
            assertArrayEquals(new byte[]{(byte) (firstWon ? 1 : 2)}, Files.readAllBytes(target));
            assertEquals(!firstWon, Files.exists(first));
            assertEquals(firstWon, Files.exists(second));
            assertEquals(1, ((Number) Files.getAttribute(target, "unix:nlink")).intValue());
        }
    }

    @Test(timeout = 30000)
    public void processCutsBeforeAndAfterRenameLeaveOnlyWholeFiles() throws Exception {
        nativeIo();
        for (String mode : new String[]{"before", "after"}) {
            Path source = root().resolve(mode + ".tmp");
            Path target = root().resolve(mode + ".final");
            writeForced(source, new byte[]{7});
            try (Child child = new Child(mode, source, target)) {
                child.ready();
                child.process.destroyForcibly();
                assertTrue(child.process.waitFor(5, TimeUnit.SECONDS));
            }
            boolean renamed = mode.equals("after");
            assertEquals(!renamed, Files.exists(source));
            assertEquals(renamed, Files.exists(target));
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(renamed ? target : source));
            assertEquals(1, ((Number) Files.getAttribute(renamed ? target : source, "unix:nlink")).intValue());
        }
    }

    private static final class Child implements AutoCloseable {
        private final Process process;

        private Child(String mode, Path source, Path target) throws IOException {
            var command = new java.util.ArrayList<String>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            String library = NativeIOLibraryPath.configuredLibraryPath();
            if (library != null) {
                command.add("-Dbookkeeper.native.io.library.path=" + library);
            }
            command.addAll(java.util.List.of("-cp", System.getProperty("java.class.path"),
                    RenameProcess.class.getName(), mode, source.toString(), target.toString()));
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
        }

        private void ready() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (process.getInputStream().available() < READY.length && process.isAlive()
                    && deadline - System.nanoTime() > 0) {
                Thread.sleep(10);
            }
            assertTrue("Child did not produce its complete ready marker",
                    process.getInputStream().available() >= READY.length);
            assertArrayEquals(READY, process.getInputStream().readNBytes(READY.length));
        }

        private void go() throws IOException {
            process.getOutputStream().write(1);
            process.getOutputStream().flush();
        }

        private String result() throws Exception {
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            String result = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            assertEquals(result, 0, process.exitValue());
            return result;
        }

        @Override
        public void close() throws Exception {
            try {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(5, TimeUnit.SECONDS));
                }
            } finally {
                process.getOutputStream().close();
                process.getInputStream().close();
                process.getErrorStream().close();
            }
        }
    }

    public static final class RenameProcess {
        public static void main(String[] args) throws Exception {
            NativeIO io = new NativeIOImpl();
            if (args[0].equals("after")) {
                io.renameNoReplace(args[1], args[2]);
            }
            System.out.write(READY);
            System.out.flush();
            if (System.in.read() < 0) {
                throw new IOException("Parent closed the controlled test channel");
            }
            if (args[0].equals("race")) {
                try {
                    io.renameNoReplace(args[1], args[2]);
                    System.out.println("WIN");
                } catch (NativeIOException failure) {
                    if (failure.getErrno() != 17) {
                        throw failure;
                    }
                    System.out.println("LOSE");
                }
            }
        }
    }

    /** Separate JVM with the exact pre-change JNI library: no new-symbol fallback is permitted. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            NativeIO io = new NativeIOImpl();
            int descriptor = io.open(args[0], NativeIO.O_RDONLY, 0);
            assertTrue(descriptor >= 0);
            assertEquals(0, io.close(descriptor));
            var failure = assertThrows(NativeIOException.class, () -> io.renameNoReplace(args[0], args[1]));
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(Files.isRegularFile(Path.of(args[0])));
            assertFalse(Files.exists(Path.of(args[1])));
            System.out.println("OLD_JNI_UNSUPPORTED");
        }
    }

    // The old method set only: the added interface method must remain a default refusal.
    private static final class LegacyImplementation implements NativeIO {
        @Override
        public int open(String path, int flags, int mode) {
            throw unused();
        }

        @Override
        public int fsync(int fd) {
            throw unused();
        }

        @Override
        public int fallocate(int fd, int mode, long offset, long length) {
            throw unused();
        }

        @Override
        public int posix_fadvise(int fd, long offset, long length, int flag) {
            throw unused();
        }

        @Override
        public int pwrite(int fd, long pointer, int count, long offset) {
            throw unused();
        }

        @Override
        public long posix_memalign(int alignment, int size) {
            throw unused();
        }

        @Override
        public void free(long pointer) {
            throw unused();
        }

        @Override
        public long lseek(int fd, long offset, int whence) {
            throw unused();
        }

        @Override
        public long pread(int fd, long pointer, long size, long offset) {
            throw unused();
        }

        @Override
        public int close(int fd) {
            throw unused();
        }

        private static UnsupportedOperationException unused() {
            return new UnsupportedOperationException("Legacy fixture does not implement the new capability");
        }
    }
}

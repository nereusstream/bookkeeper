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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual original-channel query and explicit native/platform failures. */
public class NativeIOExt4MountTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void defaultAndNoncanonicalInputsReject() {
        var defaultIo = mock(NativeIO.class, CALLS_REAL_METHODS);
        assertThrows(NativeIOException.class, () -> defaultIo.linuxExt4Mount(null, "/"));
        var io = new NativeIOImpl();
        for (String invalid : Arrays.asList(null, "", "relative", "//a", "/a/", "/a/./b", "/a/../b",
                "/a\0b", "/\ud800", "/" + "é".repeat(2048))) {
            assertThrows(NativeIOException.class, () -> io.linuxExt4Mount(null, invalid));
        }
    }

    @Test
    public void retainedMemberDefaultAndNoncanonicalInputsReject() {
        var defaultIo = mock(NativeIO.class, CALLS_REAL_METHODS);
        assertThrows(NativeIOException.class, () -> defaultIo.linuxStorageMember(null, "current"));
        var io = new NativeIOImpl();
        for (String invalid : Arrays.asList(null, "", "/current", "./file", "../file", "a/../file", "a//file",
                "a/", "a\0b", "\ud800", "é".repeat(2048))) {
            var failure = assertThrows(NativeIOException.class, () -> io.linuxStorageMember(null, invalid));
            assertTrue(failure.getMessage().contains("canonical") || failure.getMessage().contains("Canonical"));
        }
    }

    @Test
    public void realDirectoryOnUnsupportedPlatformOrNonExt4RejectsWithoutClosingOriginal() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        Path path = Path.of(SystemUtils.IS_OS_LINUX ? "/proc" : "/");
        var io = new NativeIOImpl();
        try (FileChannel root = FileChannel.open(path, StandardOpenOption.READ)) {
            var original = io.fileIdentity(root);
            var failure = assertThrows(NativeIOException.class, () -> io.linuxExt4Mount(root, path.toString()));
            assertTrue(failure.getMessage(), failure.getMessage().contains(
                    SystemUtils.IS_OS_LINUX ? "filesystem magic" : "unsupported"));
            assertTrue(root.isOpen());
            assertEquals(original, io.fileIdentity(root));
            if (!SystemUtils.IS_OS_LINUX) {
                var member = assertThrows(NativeIOException.class, () -> io.linuxStorageMember(root, "tmp"));
                assertTrue(member.getMessage().contains("unsupported"));
                assertEquals(original, io.fileIdentity(root));
            }
        }
    }

    @Test
    public void linuxMemberResolvesFromHeldDirectoryAndRejectsAliases() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        Path parent = temporary.getRoot().toPath().toRealPath();
        Path root = Files.createDirectory(parent.resolve("retained"));
        Path current = Files.createDirectory(root.resolve("current"));
        Path file = Files.writeString(current.resolve("旧 file"), "original");
        var io = new NativeIOImpl();
        try (FileChannel held = FileChannel.open(root, StandardOpenOption.READ);
             FileChannel entry = FileChannel.open(file, StandardOpenOption.READ)) {
            var directory = io.linuxStorageMember(held, "current");
            var member = io.linuxStorageMember(held, "current/旧 file");
            assertTrue(directory.directory());
            assertTrue(!member.directory());
            assertEquals(io.fileIdentity(entry), member.identity());
            assertEquals(directory.mountId(), member.mountId());
            Files.createSymbolicLink(root.resolve("alias"), current.getFileName());
            assertThrows(NativeIOException.class, () -> io.linuxStorageMember(held, "alias/旧 file"));
            assertThrows(NativeIOException.class, () -> io.linuxStorageMember(held, "alias"));
            for (byte[] malformed : new byte[][]{null, new byte[4096], {(byte) 0xff}, {'a', 0},
                    "../current".getBytes(StandardCharsets.UTF_8)}) {
                assertThrows(NativeIOException.class, () -> NativeIOJni.linuxStorageMember(held, malformed));
            }
            Files.move(root, parent.resolve("original"), StandardCopyOption.ATOMIC_MOVE);
            Path substitute = Files.createDirectory(root);
            Files.createDirectory(substitute.resolve("current"));
            assertEquals(directory, io.linuxStorageMember(held, "current"));
            assertEquals(member, io.linuxStorageMember(held, "current/旧 file"));
            assertTrue(held.isOpen());
            held.close();
            assertThrows(NativeIOException.class, () -> io.linuxStorageMember(held, "current"));
        }
    }

    @Test
    public void nativeAbiRejectsMalformedBytesAndClosedOriginalOnLinux() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        try (FileChannel root = FileChannel.open(Path.of("/proc"), StandardOpenOption.READ)) {
            for (byte[] malformed : new byte[][]{null, new byte[4096], {'/', (byte) 0xff}, {'/', 'a', 0},
                    "/a/../b".getBytes(StandardCharsets.UTF_8)}) {
                var failure = assertThrows(NativeIOException.class, () -> NativeIOJni.linuxExt4Mount(root, malformed));
                assertTrue(failure.getMessage(), failure.getMessage().contains("root"));
            }
            root.close();
            var failure = assertThrows(NativeIOException.class, () -> new NativeIOImpl().linuxExt4Mount(root, "/proc"));
            assertTrue(failure.getMessage(), failure.getMessage().contains("closed"));
        }
    }

    @Test
    public void approvedScratchExt4MatchesKernelViewAndRejectsWrongRoot() throws Exception {
        String scratch = System.getProperty("bookkeeper.test.ext4.scratch");
        assumeTrue(SystemUtils.IS_OS_LINUX && scratch != null);
        Path directory = Path.of(scratch);
        var io = new NativeIOImpl();
        try (FileChannel root = FileChannel.open(directory, StandardOpenOption.READ)) {
            var observed = io.linuxExt4Mount(root, scratch);
            assertEquals(io.fileIdentity(root), observed.identity());
            assertEquals(observed, io.linuxExt4Mount(root, scratch));
            assertTrue(directory.startsWith(Path.of(observed.mountPoint())));
            assertEquals("mnt:[" + observed.mountNamespaceInode() + "]",
                    Files.readSymbolicLink(Path.of("/proc/thread-self/ns/mnt")).toString());
            assertTrue(Files.readAllLines(Path.of("/proc/thread-self/mountinfo"), StandardCharsets.ISO_8859_1)
                    .stream().anyMatch(line -> line.startsWith(observed.mountId() + " " + observed.parentMountId()
                            + " ") && line.contains(" - ext4 ")));
            assertThrows(NativeIOException.class, () -> io.linuxExt4Mount(root, "/proc"));
            assertTrue(root.isOpen());
        }
    }

    /** Exact parent library keeps the old descriptor query and rejects the missing new JNI symbol. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel channel = FileChannel.open(Path.of("/"), StandardOpenOption.READ)) {
                var directory = io.directoryFileSystem(channel);
                var failure = assertThrows(NativeIOException.class, () -> io.linuxExt4Mount(channel, "/"));
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(directory, io.directoryFileSystem(channel));
            }
            System.out.println("OLD_JNI_EXT4_MOUNT_UNSUPPORTED");
        }
    }

    /** Preceding library retains root observation and explicitly rejects the new missing member symbol. */
    public static final class OldMemberLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel channel = FileChannel.open(Path.of("/"), StandardOpenOption.READ)) {
                var directory = io.directoryFileSystem(channel);
                var ext4 = assertThrows(NativeIOException.class, () -> io.linuxExt4Mount(channel, "/proc"));
                assertTrue(!(ext4.getCause() instanceof LinkageError));
                var member = assertThrows(NativeIOException.class, () -> io.linuxStorageMember(channel, "tmp"));
                assertTrue(member.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(directory, io.directoryFileSystem(channel));
            }
            System.out.println("OLD_JNI_RETAINED_MEMBER_UNSUPPORTED");
        }
    }
}

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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Current fixed-path filesystem type only; neither a host mount layout nor an episode cgroup proof. */
public class NativeIOCgroupRootTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void actualLinuxTypeOrErrorMatchesIndependentStatfsAndOtherHostsReject() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        var io = new NativeIOImpl();
        if (!SystemUtils.IS_OS_LINUX) {
            var failure = assertThrows(NativeIOException.class, io::currentCgroupRootFileSystemType);
            assertTrue(failure.getMessage().contains("unsupported"));
            System.out.println("CGROUP_ROOT_NATIVE_UNSUPPORTED errno=" + failure.getErrno());
            return;
        }
        var source = temporary.getRoot().toPath().resolve("cgroup-statfs.c");
        var binary = temporary.getRoot().toPath().resolve("cgroup-statfs");
        Files.writeString(source, """
                #include <errno.h>
                #include <inttypes.h>
                #include <stdint.h>
                #include <stdio.h>
                #include <sys/vfs.h>
                int main(void) {
                    struct statfs value;
                    if (statfs("/sys/fs/cgroup/", &value) != 0) {
                        int saved = errno;
                        printf("ERROR %d\\n", saved);
                    } else {
                        printf("TYPE %016" PRIx64 "\\n", (uint64_t)value.f_type);
                    }
                    return 0;
                }
                """);
        command("cc", "-std=c11", "-Wall", "-Wextra", "-Werror", source.toString(), "-o", binary.toString());
        String[] reference = command(binary.toString()).split(" ", 2);
        assertEquals(2, reference.length);
        if (reference[0].equals("ERROR")) {
            var failure = assertThrows(NativeIOException.class, io::currentCgroupRootFileSystemType);
            assertEquals(Integer.parseInt(reference[1]), failure.getErrno());
            System.out.println("ACTUAL_CGROUP_ROOT_ERROR errno=" + failure.getErrno());
        } else {
            assertEquals("TYPE", reference[0]);
            long actual = io.currentCgroupRootFileSystemType();
            assertEquals(Long.parseUnsignedLong(reference[1], 16), actual);
            System.out.println("ACTUAL_CGROUP_ROOT_TYPE " + Long.toHexString(actual)
                    + " cgroup2=" + (actual == 0x63677270L));
        }
    }

    @Test
    public void defaultImplementationCannotSupplyAnAssumedCgroupType() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        var failure = assertThrows(NativeIOException.class, unsupported::currentCgroupRootFileSystemType);
        assertTrue(failure.getMessage().contains("unsupported"));
        assertThrows(NativeIOException.class, () -> unsupported.directoryFileSystem(null));
        assertThrows(NativeIOException.class, () -> unsupported.linuxCgroupMembership(null));
    }

    @Test
    public void heldDirectoryMatchesIndependentFstatAndFstatfs() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        Path root = temporary.getRoot().toPath().toRealPath();
        Path directory = Files.createDirectory(root.resolve("original-directory"));
        Path source = root.resolve("held-fstatfs.c");
        Path binary = root.resolve("held-fstatfs");
        Files.writeString(source, """
                #include <fcntl.h>
                #include <inttypes.h>
                #include <stdio.h>
                #include <sys/stat.h>
                #ifdef __APPLE__
                #include <sys/mount.h>
                #else
                #include <sys/vfs.h>
                #endif
                #include <unistd.h>
                int main(int argc, char **argv) {
                    if (argc != 2) return 1;
                    int fd = open(argv[1], O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
                    struct stat st;
                    struct statfs fs;
                    if (fd < 0 || fstat(fd, &st) != 0 || fstatfs(fd, &fs) != 0 || close(fd) != 0) return 2;
                    printf("%" PRId64 " %" PRIu64 " %" PRId64 "\\n",
                           (int64_t)st.st_dev, (uint64_t)st.st_ino, (int64_t)fs.f_type);
                    return 0;
                }
                """);
        command("cc", "-std=gnu11", "-Wall", "-Wextra", "-Werror", source.toString(), "-o", binary.toString());
        assertDirectoryFacts(binary, directory);
        Path cgroupRoot = Path.of("/sys/fs/cgroup");
        if (SystemUtils.IS_OS_LINUX && Files.isDirectory(cgroupRoot, LinkOption.NOFOLLOW_LINKS)) {
            assertDirectoryFacts(binary, cgroupRoot);
        }
    }

    @Test
    public void actualNonCgroupDirectoryCannotProduceAFixedMountObservation() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        Path directory = temporary.newFolder().toPath();
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            var io = new NativeIOImpl();
            var identity = io.fileIdentity(channel);
            var failure = assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(channel));
            var membershipFailure = assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(channel));
            if (!SystemUtils.IS_OS_LINUX) {
                assertTrue(failure.getMessage(), failure.getMessage().contains("unsupported"));
                assertTrue(membershipFailure.getMessage(), membershipFailure.getMessage().contains("unsupported"));
                if (SystemUtils.IS_OS_MAC) {
                    assertEquals(45, failure.getErrno()); // Darwin ENOTSUP.
                    assertEquals(45, membershipFailure.getErrno());
                }
                System.out.println("CGROUP_MOUNT_NATIVE_UNSUPPORTED errno=" + failure.getErrno());
            }
            assertEquals(identity, io.fileIdentity(channel));
        }
    }

    @Test
    public void actualLinuxFixedCgroupDirectoryMatchesItsMountRecordOrRejectsNestedNamespace() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        Path root = Path.of("/sys/fs/cgroup");
        Path proc = Path.of("/proc/thread-self");
        assumeTrue(Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)
                && Files.isRegularFile(proc.resolve("mountinfo")));
        var io = new NativeIOImpl();
        try (FileChannel channel = FileChannel.open(root, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            assumeTrue(io.directoryFileSystem(channel).type() == 0x63677270L);
            String cgroup = Files.readSymbolicLink(proc.resolve("ns/cgroup")).toString();
            if (!cgroup.equals("cgroup:[4026531835]")) {
                assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(channel));
                assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(channel));
                return;
            }
            List<String[]> rows = Files.readAllLines(proc.resolve("mountinfo"), StandardCharsets.ISO_8859_1).stream()
                    .map(line -> line.split(" "))
                    .filter(row -> row.length >= 10 && row[4].equals(root.toString()))
                    .toList();
            assumeTrue("Single mount at the fixed path is required for the positive comparison", rows.size() == 1);
            var expected = rows.get(0);
            int separator = Arrays.asList(expected).indexOf("-");
            assumeTrue("The cgroup2 hierarchy root must be at the fixed path", expected[3].equals("/")
                    && separator == expected.length - 4 && expected[separator + 1].equals("cgroup2"));
            String mnt = Files.readSymbolicLink(proc.resolve("ns/mnt")).toString();
            assertTrue(mnt.startsWith("mnt:[") && mnt.endsWith("]"));
            long namespace = Long.parseUnsignedLong(mnt.substring(5, mnt.length() - 1));
            var observed = io.linuxCgroupMount(channel);
            assertEquals(new NativeIO.LinuxCgroupMount(Long.parseLong(expected[0]), Long.parseLong(expected[1]),
                    namespace), observed);
            assertEquals(observed, io.linuxCgroupMount(channel));
            assertActualMembership(root, proc, channel, io, observed);
            channel.close();
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(channel));
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(channel));
            System.out.println("ACTUAL_LINUX_FIXED_CGROUP_MOUNT " + observed);
        }
    }

    private static void assertActualMembership(Path root, Path proc, FileChannel channel, NativeIO io,
                                                NativeIO.LinuxCgroupMount mount) throws Exception {
        String row = Files.readString(proc.resolve("cgroup"), StandardCharsets.ISO_8859_1);
        if (!row.matches("0::/(?:[A-Za-z0-9:_.@/-]|\\\\x[0-9A-Fa-f]{2})*\n")) {
            assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(channel));
            return;
        }
        String path = row.substring(3, row.length() - 1);
        var member = io.linuxCgroupMembership(channel);
        assertEquals(path, member.path());
        assertEquals(mount, member.mount());
        Path directory = root.resolve(path.substring(1));
        assertEquals(Files.getAttribute(directory, "unix:dev"), member.directory().device());
        assertEquals(Files.getAttribute(directory, "unix:ino"), member.directory().inode());
        assertEquals(member, io.linuxCgroupMembership(channel));
        System.out.println("ACTUAL_LINUX_CALLING_THREAD_CGROUP_MEMBERSHIP " + member);
    }

    private static void assertDirectoryFacts(Path binary, Path directory) throws Exception {
        String[] reference = command(binary.toString(), directory.toString()).split(" ");
        assertEquals(3, reference.length);
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            var io = new NativeIOImpl();
            var facts = io.directoryFileSystem(channel);
            assertEquals(Long.parseLong(reference[0]), facts.identity().device());
            assertEquals(Long.parseUnsignedLong(reference[1]), facts.identity().inode());
            assertEquals(Long.parseLong(reference[2]), facts.type());
            assertEquals(io.fileIdentity(channel), facts.identity());
            assertTrue(channel.isOpen());
            channel.close();
            assertThrows(NativeIOException.class, () -> io.directoryFileSystem(channel));
            System.out.println("HELD_DIRECTORY_FSTATFS path=" + directory + " type=" + Long.toHexString(facts.type()));
        }
    }

    private static String command(String... arguments) throws Exception {
        Process process = new ProcessBuilder(arguments).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS));
            assertEquals(new String(process.getErrorStream().readNBytes(4096), StandardCharsets.UTF_8),
                    0, process.exitValue());
            byte[] bytes = process.getInputStream().readNBytes(4097);
            // A successful compiler has no stdout; the oracle emits exactly its own final LF.
            if (bytes.length == 0) {
                return "";
            }
            assertTrue(bytes.length <= 4096 && bytes[bytes.length - 1] == '\n');
            return new String(bytes, 0, bytes.length - 1, StandardCharsets.US_ASCII);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
            process.getOutputStream().close();
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }

    /** Separate JVM with exact previous 804093b0 library: uname works and only the new symbol is absent. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            assertTrue(!io.currentKernelIdentity().sysname().isEmpty());
            var failure = assertThrows(NativeIOException.class, io::currentCgroupRootFileSystemType);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(!io.currentKernelIdentity().sysname().isEmpty());
            System.out.println("OLD_JNI_CGROUP_ROOT_UNSUPPORTED");
        }
    }

    /** Exact previous library retains original file identity and lacks the new directory symbol. */
    public static final class OldDirectoryLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                var original = io.fileIdentity(channel);
                var failure = assertThrows(NativeIOException.class, () -> io.directoryFileSystem(channel));
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(original, io.fileIdentity(channel));
            }
            System.out.println("OLD_JNI_DIRECTORY_FILESYSTEM_UNSUPPORTED");
        }
    }

    /** The exact preceding JNI still reports held-directory identity and type. */
    public static final class OldMountLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                var directory = io.directoryFileSystem(channel);
                var failure = assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(channel));
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(directory, io.directoryFileSystem(channel));
            }
            System.out.println("OLD_JNI_CGROUP_MOUNT_UNSUPPORTED");
        }
    }

    /** The old library has the directory and mount symbols, and lacks the combined membership symbol. */
    public static final class OldMembershipLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                var directory = io.directoryFileSystem(channel);
                var mountFailure = assertThrows(NativeIOException.class, () -> io.linuxCgroupMount(channel));
                assertTrue(!(mountFailure.getCause() instanceof LinkageError));
                var failure = assertThrows(NativeIOException.class, () -> io.linuxCgroupMembership(channel));
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(directory, io.directoryFileSystem(channel));
            }
            System.out.println("OLD_JNI_CGROUP_MEMBERSHIP_UNSUPPORTED");
        }
    }
}

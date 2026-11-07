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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Fixed cgroup.events query only. The observations in these tests carry no episode authority. */
public class NativeIOCgroupEventsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void defaultRejectsAndMountIdMustBeAnOrdinaryPositiveId() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        assertThrows(NativeIOException.class, () -> unsupported.linuxCgroupEvents(null));
        var identity = new NativeIO.FileIdentity(Long.MIN_VALUE, 0);
        assertEquals(identity, new NativeIO.LinuxCgroupEvents(identity, Integer.MAX_VALUE, true).directory());
        for (long invalid : new long[]{0L, -1L, (long) Integer.MAX_VALUE + 1}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new NativeIO.LinuxCgroupEvents(identity, invalid, false));
        }
    }

    @Test
    public void nativeRejectsAnOrdinaryDirectoryWithForgedEventsAndLeavesTheOriginalOpen() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        Path directory = temporary.getRoot().toPath().toRealPath();
        Path forged = Files.writeString(directory.resolve("cgroup.events"), "populated 0\nfrozen 0\n");
        var io = new NativeIOImpl();
        try (FileChannel original = FileChannel.open(directory, StandardOpenOption.READ);
             FileChannel regular = FileChannel.open(forged, StandardOpenOption.READ)) {
            var before = io.fileIdentity(original);
            var rejection = assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(original));
            assertTrue(rejection.getMessage(), rejection.getMessage().contains(
                    SystemUtils.IS_OS_LINUX ? "actual cgroup2" : "unsupported"));
            assertEquals(before, io.fileIdentity(original));
            assertTrue(original.isOpen());
            if (SystemUtils.IS_OS_LINUX) {
                assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(null));
                assertTrue(assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(regular))
                        .getMessage().contains("original directory"));
                original.close();
                assertTrue(assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(original))
                        .getMessage().contains("closed"));
            }
        }
    }

    @Test
    public void absentEventsOnTheActualLinuxHierarchyRootIsAnError() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        Path root = Path.of("/sys/fs/cgroup");
        var io = new NativeIOImpl();
        try (FileChannel original = FileChannel.open(root, StandardOpenOption.READ)) {
            assumeTrue(io.directoryFileSystem(original).type() == 0x63677270L);
            assumeTrue(Files.notExists(root.resolve("cgroup.events")));
            var failure = assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(original));
            assertEquals(2, failure.getErrno()); // Linux ENOENT.
            assertTrue(original.isOpen());
        }
    }

    @Test
    public void approvedTestCgroupMatchesAnIndependentFreshKernelRead() throws Exception {
        String fixture = System.getProperty("bookkeeper.test.cgroup.events.directory");
        assumeTrue(SystemUtils.IS_OS_LINUX && fixture != null);
        Path directory = Path.of(fixture);
        var io = new NativeIOImpl();
        try (FileChannel original = FileChannel.open(directory, StandardOpenOption.READ)) {
            var observed = io.linuxCgroupEvents(original);
            assertEquals(io.fileIdentity(original), observed.directory());
            var populated = Files.readAllLines(directory.resolve("cgroup.events")).stream()
                    .filter(line -> line.startsWith("populated ")).toList();
            assertEquals(1, populated.size());
            assertEquals(observed.populated() ? "populated 1" : "populated 0", populated.get(0));
            assertEquals(observed, io.linuxCgroupEvents(original));
        }
    }

    /** A fresh JVM with the exact earlier native library fails closed for the missing new symbol. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            try (FileChannel original = FileChannel.open(Path.of("/"), StandardOpenOption.READ)) {
                var facts = io.directoryFileSystem(original);
                var failure = assertThrows(NativeIOException.class, () -> io.linuxCgroupEvents(original));
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertEquals(facts, io.directoryFileSystem(original));
            }
            System.out.println("OLD_JNI_LINUX_CGROUP_EVENTS_UNSUPPORTED");
        }
    }
}

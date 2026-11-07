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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

/** Fixed self and PID 1 filesystem observations, including the actual platform and compatibility failures. */
public class NativeIOFilesystemViewTest {
    @Test
    public void viewDataRequiresBoundedMountAndNamespaceButPreservesRawIdentityAndBirth() {
        var identity = new NativeIO.FileIdentity(Long.MIN_VALUE, -1L);
        var view = new NativeIO.LinuxFilesystemView(0xffff_ffffL, identity, Integer.MAX_VALUE);
        assertEquals(identity, view.root());
        assertEquals(Long.MIN_VALUE, new NativeIO.LinuxPid1FilesystemView(view, Long.MIN_VALUE).pid1StartTicks());
        assertEquals(0L, new NativeIO.LinuxPid1FilesystemView(view, 0L).pid1StartTicks());
        for (long invalid : new long[]{-1L, 0L, 0x1_0000_0000L}) {
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxFilesystemView(invalid, identity, 1));
        }
        for (long invalid : new long[]{-1L, 0L, (long) Integer.MAX_VALUE + 1}) {
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxFilesystemView(1, identity, invalid));
        }
        assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxFilesystemView(1, null, 1));
    }

    @Test
    public void defaultAndUnsupportedHostFailClosed() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        assertThrows(NativeIOException.class, unsupported::currentLinuxFilesystemView);
        assertThrows(NativeIOException.class, unsupported::currentLinuxPid1FilesystemView);
        if (!SystemUtils.IS_OS_LINUX && NativeIOLibraryPath.configuredLibraryPath() != null) {
            var io = new NativeIOImpl();
            assertTrue(assertThrows(NativeIOException.class, io::currentLinuxFilesystemView)
                    .getMessage().contains("unsupported"));
            assertTrue(assertThrows(NativeIOException.class, io::currentLinuxPid1FilesystemView)
                    .getMessage().contains("unsupported"));
        }
    }

    @Test
    public void realLinuxSelfViewMatchesIndependentKernelSources() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        var io = new NativeIOImpl();
        var observed = io.currentLinuxFilesystemView();
        assertEquals(rootIdentity(Path.of("/")), observed.root());
        assertEquals("mnt:[" + observed.mountNamespaceInode() + "]",
                Files.readSymbolicLink(Path.of("/proc/thread-self/ns/mnt")).toString());
        assertTrue(Files.readAllLines(Path.of("/proc/thread-self/mountinfo"), StandardCharsets.ISO_8859_1).stream()
                .anyMatch(line -> line.startsWith(observed.rootMountId() + " ") && line.split(" ")[4].equals("/")));
        assertEquals(observed, io.currentLinuxFilesystemView());
    }

    @Test
    public void nonRootRejectsPid1LookupAndApprovedRootCanCompareActualViews() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        var io = new NativeIOImpl();
        var credentials = io.currentProcessCredentials();
        if (credentials.realUid() != 0 || credentials.effectiveUid() != 0) {
            assertTrue(assertThrows(NativeIOException.class, io::currentLinuxPid1FilesystemView)
                    .getMessage().contains("UID zero"));
            return;
        }
        assumeTrue(Boolean.getBoolean("bookkeeper.test.pid1.filesystem.view"));
        var observed = io.currentLinuxPid1FilesystemView();
        assertEquals(io.currentLinuxFilesystemView(), observed.view());
        assertEquals(io.currentLinuxProcessBirth().pid1StartTicks(), observed.pid1StartTicks());
        assertEquals(rootIdentity(Path.of("/proc/1/root")), observed.view().root());
        assertEquals("mnt:[" + observed.view().mountNamespaceInode() + "]",
                Files.readSymbolicLink(Path.of("/proc/1/ns/mnt")).toString());
        assertEquals(observed, io.currentLinuxPid1FilesystemView());
    }

    private static NativeIO.FileIdentity rootIdentity(Path path) throws Exception {
        var facts = Files.readAttributes(path, "unix:dev,ino");
        return new NativeIO.FileIdentity(((Number) facts.get("dev")).longValue(),
                ((Number) facts.get("ino")).longValue());
    }

    /** The parent JNI in a fresh VM keeps its original queries and explicitly rejects both new symbols. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            var kernel = io.currentKernelIdentity();
            assertTrue(assertThrows(NativeIOException.class, io::currentLinuxFilesystemView)
                    .getCause() instanceof UnsatisfiedLinkError);
            assertTrue(assertThrows(NativeIOException.class, io::currentLinuxPid1FilesystemView)
                    .getCause() instanceof UnsatisfiedLinkError);
            assertEquals(kernel, io.currentKernelIdentity());
            System.out.println("OLD_JNI_LINUX_FILESYSTEM_VIEWS_UNSUPPORTED");
        }
    }
}

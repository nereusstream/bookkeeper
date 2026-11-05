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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

/** Native thread namespace observation only; an actual nested observation is a valid read. */
public class NativeIOThreadNamespacesTest {
    @Test
    public void nonLinuxExplicitlyRejectsThreadNamespaceObservation() throws Exception {
        assumeTrue(!SystemUtils.IS_OS_LINUX && NativeIOLibraryPath.configuredLibraryPath() != null);
        var failure = assertThrows(NativeIOException.class, () -> new NativeIOImpl().currentLinuxThreadNamespaces());
        assertTrue(failure.getMessage(), failure.getMessage().contains("unsupported"));
        if (SystemUtils.IS_OS_MAC) {
            assertEquals(78, failure.getErrno());
        }
        System.out.println("THREAD_NAMESPACES_NATIVE_UNSUPPORTED errno=" + failure.getErrno());
    }

    @Test
    public void actualLinuxLinkTargetsMatchIndependentCallingThreadReadsOrReportRestrictedSource() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        var io = new NativeIOImpl();
        NativeIO.LinuxThreadNamespaces first;
        try {
            first = io.currentLinuxThreadNamespaces();
        } catch (NativeIOException restricted) {
            String message = restricted.getMessage();
            int errno = restricted.getErrno();
            // ELOOP at openat2 could mean we failed to hold the terminal magic link with the required flags.
            boolean openRestriction = message.contains("fixed openat2")
                    && Set.of(1, 2, 11, 13, 18, 38, 95).contains(errno)
                    || message.contains("fixed proc root open") && Set.of(1, 2, 13, 20, 40).contains(errno);
            boolean linkRestriction = message.contains("thread namespace held readlinkat failed")
                    && Set.of(1, 13, 38).contains(restricted.getErrno());
            boolean wrongRoot = restricted.getErrno() == 22 && message.contains("proc identity source is not");
            assertTrue(message, openRestriction || linkRestriction || wrongRoot);
            System.out.println("LINUX_THREAD_NAMESPACES_SOURCE_POSITIVE_NOT_EXECUTED " + message);
            assumeNoException("Restricted procfs/openat2/readlinkat source; no Linux positive", restricted);
            throw restricted;
        }
        assertEquals(procNamespaceInode("pid"), first.pidInode());
        assertEquals(procNamespaceInode("user"), first.userInode());
        assertEquals(first, io.currentLinuxThreadNamespaces());
        System.out.println("ACTUAL_LINUX_THREAD_NAMESPACES_SOURCE_POSITIVE pid=" + first.pidInode()
                + " user=" + first.userInode());
    }

    private static long procNamespaceInode(String type) throws Exception {
        String target = Files.readSymbolicLink(Path.of("/proc/thread-self/ns", type)).toString();
        String prefix = type + ":[";
        assertTrue(target, target.startsWith(prefix) && target.endsWith("]"));
        long inode = Long.parseUnsignedLong(target.substring(prefix.length(), target.length() - 1));
        assertTrue(inode > 0 && inode <= 0xffff_ffffL);
        return inode;
    }

    @Test
    public void nonzeroUnsignedNamespaceNumbersArePreservedWithoutAssigningInitialAuthority() {
        var actual = new NativeIO.LinuxThreadNamespaces(0xefff_fffcL, 0xffff_ffffL);
        assertEquals(0xefff_fffcL, actual.pidInode());
        assertEquals(0xffff_ffffL, actual.userInode());
        for (long invalid : new long[]{0, -1, 0x1_0000_0000L}) {
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxThreadNamespaces(invalid, 1));
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxThreadNamespaces(1, invalid));
        }
    }

    @Test
    public void defaultImplementationCannotInferNamespaceFromProcessIdentityOrCredentials() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        var failure = assertThrows(NativeIOException.class, unsupported::currentLinuxThreadNamespaces);
        assertTrue(failure.getMessage().contains("unsupported"));
    }

    /** Independent JVM against prior e3d11649: old credentials work and only the new symbol refuses. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            var failure = assertThrows(NativeIOException.class, io::currentLinuxThreadNamespaces);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            System.out.println("OLD_JNI_THREAD_NAMESPACES_UNSUPPORTED");
        }
    }
}

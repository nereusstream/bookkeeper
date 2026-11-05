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
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

/** Visible proc/time birth data only, separate from host PID1, systemd or namespace qualification. */
public class NativeIOProcessBirthTest {
    @Test
    public void nonLinuxExplicitlyRejectsProcessBirthCapability() throws Exception {
        assumeTrue(!SystemUtils.IS_OS_LINUX && NativeIOLibraryPath.configuredLibraryPath() != null);
        var failure = assertThrows(NativeIOException.class, () -> new NativeIOImpl().currentLinuxProcessBirth());
        assertTrue(failure.getMessage(), failure.getMessage().contains("unsupported"));
        if (SystemUtils.IS_OS_MAC) {
            assertEquals(78, failure.getErrno());
        }
        System.out.println("PROCESS_BIRTH_NATIVE_UNSUPPORTED errno=" + failure.getErrno());
    }

    @Test
    public void actualLinuxSelfAndPid1BirthMatchIndependentStatAndJvmPidOrReportRestrictedSource() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        var io = new NativeIOImpl();
        NativeIO.LinuxProcessBirth first;
        try {
            first = io.currentLinuxProcessBirth();
        } catch (NativeIOException restricted) {
            String message = restricted.getMessage();
            boolean openRestriction = (message.contains("fixed openat2") || message.contains("fixed proc root open"))
                    && Set.of(1, 2, 11, 13, 18, 38, 40, 95).contains(restricted.getErrno());
            boolean wrongRoot = restricted.getErrno() == 22 && message.contains("proc identity source is not");
            assertTrue(message, openRestriction || wrongRoot);
            System.out.println("LINUX_PROCESS_BIRTH_SOURCE_POSITIVE_NOT_EXECUTED " + message);
            assumeNoException("Restricted procfs/openat2 source; no Linux positive", restricted);
            throw restricted;
        }
        assertEquals(ProcessHandle.current().pid(), first.pid());
        assertEquals(statTicks(Path.of("/proc/self/stat"), first.pid()), first.startTicks());
        assertEquals(statTicks(Path.of("/proc/1/stat"), 1), first.pid1StartTicks());
        assertEquals(first, io.currentLinuxProcessBirth());
        System.out.println("ACTUAL_LINUX_PROCESS_BIRTH_SOURCE_POSITIVE pid=" + first.pid()
                + " self=" + Long.toUnsignedString(first.startTicks())
                + " pid1=" + Long.toUnsignedString(first.pid1StartTicks()));
    }

    private static long statTicks(Path path, long pid) throws Exception {
        byte[] bytes = Files.readAllBytes(path);
        assertTrue(bytes.length > 0 && bytes.length <= 4096 && bytes[bytes.length - 1] == '\n');
        // Preserve opaque comm bytes one-for-one; only PID and the ASCII tail are interpreted by this oracle.
        String text = new String(bytes, StandardCharsets.ISO_8859_1);
        assertEquals(pid, Long.parseLong(text.substring(0, text.indexOf(' '))));
        String[] tail = text.substring(text.lastIndexOf(')') + 2).trim().split("\\s+");
        assertTrue(tail.length >= 20);
        return Long.parseUnsignedLong(tail[19]);
    }

    @Test
    public void rawTicksPreserveZeroAndEveryUnsignedBitPatternWhilePidMustBeCurrentProcessShaped() {
        var zero = new NativeIO.LinuxProcessBirth(1, 0, 0);
        assertEquals(0, zero.startTicks());
        var high = new NativeIO.LinuxProcessBirth(Integer.MAX_VALUE, Long.MIN_VALUE, -1);
        assertEquals(Long.MIN_VALUE, high.startTicks());
        assertEquals("18446744073709551615", Long.toUnsignedString(high.pid1StartTicks()));
        for (long pid : new long[]{0, -1, 0x8000_0000L}) {
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.LinuxProcessBirth(pid, 0, 0));
        }
    }

    @Test
    public void defaultImplementationCannotInferBirthFromWallClockOrProperties() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        var failure = assertThrows(NativeIOException.class, unsupported::currentLinuxProcessBirth);
        assertTrue(failure.getMessage().contains("unsupported"));
    }

    /** Independent JVM against prior 189d9294: old credentials work, only the new symbol refuses. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            var failure = assertThrows(NativeIOException.class, io::currentLinuxProcessBirth);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            System.out.println("OLD_JNI_PROCESS_BIRTH_UNSUPPORTED");
        }
    }
}

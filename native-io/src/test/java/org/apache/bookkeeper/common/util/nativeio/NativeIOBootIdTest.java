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

/** Fixed native boot source only; host namespace and systemd authority are outside this observation. */
public class NativeIOBootIdTest {
    @Test
    public void nonLinuxExplicitlyRejectsTheFixedProcfsCapability() throws Exception {
        assumeTrue(!SystemUtils.IS_OS_LINUX && NativeIOLibraryPath.configuredLibraryPath() != null);
        var failure = assertThrows(NativeIOException.class, () -> new NativeIOImpl().currentLinuxBootId());
        assertTrue(failure.getMessage(), failure.getMessage().contains("unsupported"));
        if (SystemUtils.IS_OS_MAC) {
            assertEquals(78, failure.getErrno());
        }
        System.out.println("BOOT_ID_NATIVE_UNSUPPORTED errno=" + failure.getErrno());
    }

    @Test
    public void actualLinuxColdReadsMatchIndependentKernelOutputOrReportARestrictedSource() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        var io = new NativeIOImpl();
        java.util.UUID first;
        try {
            first = io.currentLinuxBootId();
        } catch (NativeIOException restricted) {
            String message = restricted.getMessage();
            boolean openRestriction = (message.contains("fixed openat2") || message.contains("fixed proc root open"))
                    && Set.of(1, 2, 11, 13, 18, 38, 40, 95).contains(restricted.getErrno());
            boolean wrongRoot = restricted.getErrno() == 22 && message.contains("boot ID source is not");
            assertTrue(message, openRestriction || wrongRoot);
            System.out.println("LINUX_BOOT_ID_SOURCE_POSITIVE_NOT_EXECUTED " + message);
            assumeNoException("Restricted procfs/openat2 source; no Linux positive", restricted);
            throw restricted;
        }
        assertArrayEquals((first + "\n").getBytes(StandardCharsets.US_ASCII),
                Files.readAllBytes(Path.of("/proc/sys/kernel/random/boot_id")));
        assertEquals(first, io.currentLinuxBootId());
        System.out.println("ACTUAL_LINUX_BOOT_ID_SOURCE_POSITIVE " + first);
    }

    @Test
    public void defaultImplementationCannotSupplyACachedOrInventedBootId() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        var failure = assertThrows(NativeIOException.class, unsupported::currentLinuxBootId);
        assertTrue(failure.getMessage().contains("unsupported"));
    }

    /** Separate JVM against exact prior 9406191e: old credential ABI works, new boot symbol refuses. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            var failure = assertThrows(NativeIOException.class, io::currentLinuxBootId);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            System.out.println("OLD_JNI_BOOT_ID_UNSUPPORTED");
        }
    }
}

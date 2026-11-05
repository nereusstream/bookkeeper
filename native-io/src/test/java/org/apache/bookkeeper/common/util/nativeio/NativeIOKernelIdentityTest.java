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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

/** Actual local uname data and ABI rejection only; never host namespace or Linux deployment qualification. */
public class NativeIOKernelIdentityTest {
    @Test
    public void actualUnameMatchesIndependentSingleFieldCommandsWithoutTrimmingVersion() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        var actual = new NativeIOImpl().currentKernelIdentity();
        assertEquals(uname("-s"), actual.sysname());
        assertEquals(uname("-m"), actual.machine());
        assertEquals(uname("-r"), actual.release());
        assertEquals(uname("-v"), actual.version());
        System.out.println("ACTUAL_UNAME_VIEW " + actual);
    }

    @Test
    public void fieldsPreserveFullTextAndRejectMissingEmptyInvalidOrExcessiveData() {
        String version = "#1 SMP  雪; release:build ";
        var value = new NativeIO.KernelIdentity("Linux", "x86_64", "6.6.0+build", version);
        assertEquals(version, value.version());
        for (String invalid : new String[]{null, "", "x\0y", String.valueOf((char) 0xd800), "雪".repeat(1366)}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new NativeIO.KernelIdentity("Linux", "x86_64", "6.6", invalid));
        }
    }

    @Test
    public void defaultImplementationCannotInferKernelDataFromProperties() {
        var unsupported = mock(NativeIO.class, CALLS_REAL_METHODS);
        var failure = assertThrows(NativeIOException.class, unsupported::currentKernelIdentity);
        assertTrue(failure.getMessage().contains("unsupported"));
    }

    private static String uname(String option) throws Exception {
        Process process = new ProcessBuilder("/usr/bin/uname", option).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS));
            assertEquals(new String(process.getErrorStream().readNBytes(4096), StandardCharsets.UTF_8),
                    0, process.exitValue());
            byte[] bytes = process.getInputStream().readNBytes(4097);
            assertTrue(bytes.length > 1 && bytes.length <= 4096 && bytes[bytes.length - 1] == '\n');
            // Remove only the command's own one-byte line terminator, preserving all native field text.
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes, 0, bytes.length - 1)).toString();
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

    /** Run in a separate JVM against exact sealed70bb, which still supports the prior credential ABI. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            var failure = assertThrows(NativeIOException.class, io::currentKernelIdentity);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            assertTrue(io.currentProcessCredentials().realUid() >= 0);
            System.out.println("OLD_JNI_KERNEL_UNSUPPORTED");
        }
    }
}

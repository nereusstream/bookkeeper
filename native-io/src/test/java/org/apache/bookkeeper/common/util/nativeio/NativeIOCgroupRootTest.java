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
}

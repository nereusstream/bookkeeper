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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual local credentials only; no exec/dropper, saved-credential or launch authority. */
public class NativeIOCredentialsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void actualCredentialsMatchInheritedProcessIdsAndGroupMembership() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        var actual = new NativeIOImpl().currentProcessCredentials();
        assertEquals(Long.parseLong(id("-ru")), actual.realUid());
        assertEquals(Long.parseLong(id("-u")), actual.effectiveUid());
        assertEquals(Long.parseLong(id("-rg")), actual.realGid());
        assertEquals(Long.parseLong(id("-g")), actual.effectiveGid());
        // macOS id -G may enumerate account groups beyond the process's actual getgroups set.
        // Use an independent executable and the exact POSIX calls, retaining all groups verbatim.
        var source = temporary.getRoot().toPath().resolve("credentials.c");
        var binary = temporary.getRoot().toPath().resolve("credentials");
        Files.writeString(source, """
                #include <stdio.h>
                #include <sys/types.h>
                #include <unistd.h>
                int main(void) {
                    gid_t groups[64];
                    int n = getgroups(64, groups);
                    if (n < 0) return 2;
                    printf("%lu %lu %lu %lu", (unsigned long)getuid(), (unsigned long)geteuid(),
                           (unsigned long)getgid(), (unsigned long)getegid());
                    for (int i = 0; i < n; i++) printf(" %lu", (unsigned long)groups[i]);
                    puts("");
                    return 0;
                }
                """);
        command("cc", "-std=c11", "-Wall", "-Wextra", "-Werror", source.toString(), "-o", binary.toString());
        var reported = Arrays.stream(command(binary.toString()).split("\\s+"))
                .map(Long::parseLong).toList();
        assertEquals(new NativeIO.ProcessCredentials(reported.get(0), reported.get(1), reported.get(2),
                reported.get(3), reported.subList(4, reported.size())), actual);
        assertEquals(actual, new NativeIOImpl().currentProcessCredentials());
    }

    @Test
    public void normalizationPreservesTheExactSupplementarySetWithoutAddingThePrimaryGroup() {
        var actual = new NativeIO.ProcessCredentials(1, 2, 3, 4, List.of(9L, 8L, 9L));
        assertEquals(List.of(8L, 9L), actual.supplementaryGroups());
        assertEquals(List.of(), new NativeIO.ProcessCredentials(1, 2, 3, 4, List.of()).supplementaryGroups());
        assertThrows(UnsupportedOperationException.class, () -> actual.supplementaryGroups().add(3L));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.ProcessCredentials(-1, 2, 3, 4, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.ProcessCredentials(1, 2, 3, 4, List.of(0x1_0000_0000L)));
    }

    private static String id(String option) throws Exception {
        return command("/usr/bin/id", option);
    }

    private static String command(String... arguments) throws Exception {
        Process process = new ProcessBuilder(arguments).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS));
            assertEquals(new String(process.getErrorStream().readNBytes(4096), StandardCharsets.UTF_8),
                    0, process.exitValue());
            return new String(process.getInputStream().readNBytes(4096), StandardCharsets.US_ASCII).trim();
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

    /** Run against the exact previous library in a separate JVM. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            var io = new NativeIOImpl();
            int fd = io.open(args[0], NativeIO.O_RDONLY, 0);
            assertTrue(fd >= 0);
            assertEquals(0, io.close(fd));
            var failure = assertThrows(NativeIOException.class, io::currentProcessCredentials);
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            System.out.println("OLD_JNI_CREDENTIALS_UNSUPPORTED");
        }
    }
}

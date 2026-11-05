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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NativeIOEnvironmentTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void defaultDoesNotInferAnEnvironmentSample() {
        assertThrows(NativeIOException.class, mock(NativeIO.class, CALLS_REAL_METHODS)::currentLinuxNativeEnvironment);
    }

    @Test
    public void macOsReportsUnsupportedFromTheActualJniSymbol() {
        assumeTrue(SystemUtils.IS_OS_MAC && NativeIOLibraryPath.configuredLibraryPath() != null);
        var failure = assertThrows(NativeIOException.class, new NativeIOImpl()::currentLinuxNativeEnvironment);
        assertEquals(78, failure.getErrno()); // Darwin ENOSYS.
    }

    @Test(timeout = 60000)
    public void actualLinuxSampleSeesNativeMutationAfterJavaTookItsEmptySnapshot() throws Exception {
        assumeTrue("Actual libc environ sampling requires Linux", SystemUtils.IS_OS_LINUX);
        String empty = runProbe("empty", null);
        assumeTrue("Linux process_vm_readv is unavailable or denied", !empty.contains("ENV_KERNEL_UNAVAILABLE"));
        assertTrue(empty, empty.contains("ENV_EMPTY_SAMPLED"));

        Path directory = temporary.getRoot().toPath();
        Path source = directory.resolve("environment-probe.c");
        Path helper = directory.resolve("environment-probe.so");
        try (var resource = getClass().getResourceAsStream("/current-linux-environment-probe.c")) {
            assertNotNull(resource);
            Files.copy(resource, source);
        }
        Path include = Path.of(System.getProperty("java.home"), "include");
        run(List.of("cc", "-D_POSIX_C_SOURCE=200809L", "-shared", "-fPIC",
                "-I", include.toString(), "-I", include.resolve("linux").toString(),
                "-o", helper.toString(), source.toString()), "fixture-compile", false);
        String changed = runProbe("changed", helper);
        assertTrue(changed, changed.contains("ENV_NONEMPTY_WITH_STALE_JAVA_SNAPSHOT"));
    }

    private String runProbe(String mode, Path helper) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:ActiveProcessorCount=2", "-Xcheck:jni"));
        String library = NativeIOLibraryPath.configuredLibraryPath();
        if (library != null) {
            command.add("-Dbookkeeper.native.io.library.path=" + library);
        }
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), Probe.class.getName(), mode));
        if (helper != null) {
            command.add(helper.toAbsolutePath().toString());
        }
        return run(command, mode, true);
    }

    private String run(List<String> command, String label, boolean emptyEnvironment) throws Exception {
        Path log = temporary.newFile(label + ".log").toPath();
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        if (emptyEnvironment) {
            builder.environment().clear();
        }
        Process process = builder.start();
        try {
            process.getOutputStream().close();
            assertTrue(process.waitFor(15, TimeUnit.SECONDS));
            String transcript = Files.readString(log);
            assertEquals(transcript, 0, process.exitValue());
            return transcript;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }

    /** Only the isolated fixture can mutate its environment. */
    public static final class Probe {
        private static native int addFixtureEntry();

        public static void main(String[] arguments) throws Exception {
            var io = new NativeIOImpl();
            if (arguments[0].equals("old")) {
                assertTrue(io.currentProcessCredentials().realUid() >= 0);
                var failure = assertThrows(NativeIOException.class, io::currentLinuxNativeEnvironment);
                assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
                assertTrue(io.currentProcessCredentials().realUid() >= 0);
                System.out.println("OLD_JNI_LINUX_ENVIRONMENT_UNSUPPORTED");
                return;
            }

            assertTrue(System.getenv().isEmpty());
            final NativeIO.NativeEnvironmentSample initial;
            try {
                initial = io.currentLinuxNativeEnvironment();
            } catch (NativeIOException restricted) {
                if (arguments[0].equals("empty") && (restricted.getErrno() == 1 || restricted.getErrno() == 38)
                        && restricted.getMessage().startsWith("Current Linux environ pointer copy failed")) {
                    System.out.println("ENV_KERNEL_UNAVAILABLE");
                    return;
                }
                throw restricted;
            }
            assertEquals(NativeIO.NativeEnvironmentSample.EMPTY_SAMPLED, initial);
            if (arguments[0].equals("empty")) {
                System.out.println("ENV_EMPTY_SAMPLED");
                return;
            }
            System.load(arguments[1]);
            assertEquals(0, addFixtureEntry());
            assertTrue(System.getenv().isEmpty());
            assertEquals(NativeIO.NativeEnvironmentSample.NONEMPTY_SAMPLED, io.currentLinuxNativeEnvironment());
            System.out.println("ENV_NONEMPTY_WITH_STALE_JAVA_SNAPSHOT");
        }
    }
}

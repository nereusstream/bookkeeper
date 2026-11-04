/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual isolated JNI reads, never an approved vendor/source or permission to execute the observed backend. */
public class NativeIOJdkProcessLaunchTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void observationCopiesOpaqueBytesAndRejectsUnknownOrMalformedData() {
        byte[] bytes = {(byte) 0xff, (byte) 0xc0, 0};
        var data = new NativeIO.JdkProcessLaunchObservation("FORK", 0, bytes);
        bytes[0] = 0;
        assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0xc0, 0}, data.helperPath());
        byte[] changed = data.helperPath();
        changed[0] = 0;
        assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0xc0, 0}, data.helperPath());
        byte[] maximum = new byte[4097];
        Arrays.fill(maximum, 0, maximum.length - 1, (byte) 1);
        assertEquals(4097, new NativeIO.JdkProcessLaunchObservation("POSIX_SPAWN", 1, maximum).helperPath().length);
        assertEquals("VFORK", new NativeIO.JdkProcessLaunchObservation("VFORK", 2, new byte[]{1, 0}).mechanism());
        for (byte[] invalid : Arrays.asList(null, new byte[0], new byte[]{0}, new byte[]{1}, new byte[]{1, 2},
                new byte[]{1, 0, 2, 0}, new byte[]{1, 0, 0}, new byte[4098])) {
            assertThrows(IllegalArgumentException.class,
                    () -> new NativeIO.JdkProcessLaunchObservation("FORK", 0, invalid));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.JdkProcessLaunchObservation("FORK", 1, new byte[]{1, 0}));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.JdkProcessLaunchObservation("UNKNOWN", 3, new byte[]{1, 0}));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.JdkProcessLaunchObservation(null, 0, new byte[]{1, 0}));
    }

    @Test(timeout = 30000)
    public void actualCachedPosixSelectionSurvivesLaterInvalidProperty() throws Exception {
        run("cached", "POSIX_SPAWN");
    }

    @Test(timeout = 30000)
    public void differentJvmObservesForkChosenBeforeInitialization() throws Exception {
        run("cached", "FORK");
    }

    @Test(timeout = 30000)
    public void failedClassInitializationNeverReturnsADefaultObservation() throws Exception {
        run("failed", "UNSUPPORTED_FIXTURE_VALUE");
    }

    private void run(String mode, String mechanism) throws Exception {
        String library = NativeIOLibraryPath.configuredLibraryPath();
        assumeTrue(SystemUtils.IS_OS_LINUX || library != null);
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:ActiveProcessorCount=2", "-Xcheck:jni",
                "-Djdk.lang.Process.launchMechanism=" + mechanism));
        if (library != null) {
            command.add("-Dbookkeeper.native.io.library.path=" + library);
        }
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), Probe.class.getName(), mode, mechanism));
        Path log = temporary.newFile(mode + '-' + mechanism + ".log").toPath();
        var builder = new ProcessBuilder(command).directory(new java.io.File("/"));
        builder.environment().clear();
        Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        process.getOutputStream().close();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS));
            String transcript = Files.readString(log);
            System.out.print(transcript);
            assertEquals(transcript, 0, process.exitValue());
            assertTrue(transcript, transcript.contains("JNI_CACHE_OK " + mode + ' ' + mechanism));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }

    /** Each JVM owns its immutable JDK initialization episode; no private field writes or reset are used. */
    public static final class Probe {
        public static void main(String[] arguments) throws Exception {
            NativeIO io = new NativeIOImpl();
            if (arguments[0].equals("failed")) {
                var initial = assertThrows(NativeIOException.class, io::jdkProcessLaunch);
                assertNotNull(initial.getCause());
                System.setProperty("jdk.lang.Process.launchMechanism", "POSIX_SPAWN"); // Isolated fixture only.
                assertThrows(NativeIOException.class, io::jdkProcessLaunch);
            } else {
                var original = io.jdkProcessLaunch(); // Must call the new native symbol, never a property fallback.
                assertEquals(arguments[1], original.mechanism());
                assertEquals(arguments[1].equals("FORK") ? 0 : 1, original.ordinal());
                byte[] expected = (System.getProperty("java.home") + "/lib/jspawnhelper" + (char) 0)
                        .getBytes(StandardCharsets.UTF_8);
                assertArrayEquals(expected, original.helperPath());
                byte[] changed = original.helperPath();
                changed[0] = 0;
                assertArrayEquals(expected, original.helperPath());
                System.setProperty("jdk.lang.Process.launchMechanism", "UNSUPPORTED_FIXTURE_VALUE");
                var cached = io.jdkProcessLaunch();
                assertEquals(original.mechanism(), cached.mechanism());
                assertEquals(original.ordinal(), cached.ordinal());
                assertArrayEquals(expected, cached.helperPath());
                System.out.println("JNI_CACHE_BYTES " + HexFormat.of().formatHex(cached.helperPath()));
            }
            System.out.println("JNI_CACHE_OK " + arguments[0] + ' ' + arguments[1]);
        }
    }

    /** The preceding actual library must reject the new capability, while its old credential symbol still works. */
    public static final class OldLibraryProbe {
        public static void main(String[] arguments) throws Exception {
            NativeIO io = new NativeIOImpl();
            assertTrue(io.currentProcessCredentials().effectiveUid() >= 0);
            var failure = assertThrows(NativeIOException.class, io::jdkProcessLaunch);
            assertTrue(failure.getCause() instanceof LinkageError);
            System.out.println("JNI_OLD_SYMBOL_REJECTED");
        }
    }
}

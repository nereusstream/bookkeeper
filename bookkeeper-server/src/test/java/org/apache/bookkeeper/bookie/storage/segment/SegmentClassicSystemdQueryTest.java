/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Pure argv/property data and actual child pipe ownership; never validates a real systemd unit. */
public class SegmentClassicSystemdQueryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private static final String UNIT = "bookkeeper-uw@classic-1.service";
    private static final String NAMES = "Id,MainPID,ExecMainPID,InvocationID,ControlGroup,LoadState,ActiveState,"
            + "SubState,Transient,Job,ControlPID,FragmentPath,SourcePath,DropInPaths,UnitFileState,NeedDaemonReload,"
            + "Type,User,Group,DynamicUser,ExecCondition,ExecConditionEx,ExecStartPre,ExecStartPreEx,ExecStart,"
            + "ExecStartEx,ExecStartPost,ExecStartPostEx,ExecReload,ExecReloadEx,ExecStop,ExecStopEx,ExecStopPost,"
            + "ExecStopPostEx,Restart,NRestarts,Triggers,TriggeredBy,OnFailure,OnSuccess";

    private static String properties() {
        var value = new StringBuilder();
        for (String name : NAMES.split(",")) {
            value.append(name).append('=').append(name.equals("Id") ? UNIT : "").append('\n');
        }
        return value.toString();
    }

    @Test
    public void onlyTheFixedSystemManagerQueryAndOneLiteralServiceCanBeSelected() throws Exception {
        var command = SegmentClassicSystemdQuery.command(UNIT);
        assertEquals(List.of("/usr/bin/systemctl", "--system", "--no-pager", "--no-ask-password", "--all",
                "--property=" + NAMES, "show", UNIT), command);
        assertThrows(UnsupportedOperationException.class, () -> command.add("--user"));
        for (String invalid : List.of("", "--user", "*.service", "classic?.service", "classic.socket",
                "/etc/systemd/system/classic.service", "a.service b.service", "a\n.service", "a\\x20.service",
                "a".repeat(256) + ".service")) {
            assertThrows(IllegalArgumentException.class, () -> SegmentClassicSystemdQuery.command(invalid));
        }
        assertThrows(IllegalArgumentException.class, () -> SegmentClassicSystemdQuery.command(null));
    }

    @Test
    public void rawValuesRemainDataAndEmptyIsPreservedWithoutReplacingMissingFields() throws Exception {
        String opaque = "{ path=/fixed/program ; argv[]=/fixed/program raw=value ; unparsed=\\x20 ; }";
        String text = properties().replace("ExecStart=\n", "ExecStart=" + opaque + "\n")
                .replace("MainPID=\n", "MainPID=unknown\n");
        var parsed = SegmentClassicSystemdQuery.decode(UNIT, text.getBytes(StandardCharsets.UTF_8));
        assertEquals(40, parsed.size());
        assertEquals("", parsed.get("SourcePath"));
        assertEquals(opaque, parsed.get("ExecStart"));
        assertEquals("unknown", parsed.get("MainPID")); // No numeric identity or execution approval is issued.
        assertThrows(UnsupportedOperationException.class, () -> parsed.put("MainPID", "1"));
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT,
                properties().replace("SourcePath=\n", "").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode("different.service",
                properties().getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void duplicateUnknownTruncatedAndMalformedOutputIsRejected() throws Exception {
        String valid = properties();
        for (String invalid : List.of(valid + "Id=" + UNIT + "\n", valid + "Unknown=\n", valid + "\n",
                valid + "bare\n", valid.substring(0, valid.length() - 1), valid.replace("Id=", "Id=\0"),
                valid.replace("Id=", "Id=\r"))) {
            assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT,
                    invalid.getBytes(StandardCharsets.UTF_8)));
        }
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT, new byte[]{(byte) 0xff}));
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT, new byte[65537]));
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT, new byte[0]));
        assertThrows(IOException.class, () -> SegmentClassicSystemdQuery.decode(UNIT, null));
    }

    @Test(timeout = 30000)
    public void realChildStdinEofAndBothPipeConsumersFinishBeforeReturningBytes() throws Exception {
        var output = new SegmentClassicSystemdQuery.Output();
        output.initialize(child("ok"));
        try {
            var bytes = output.read(deadline());
            assertEquals(properties(), new String(bytes.stdout(), StandardCharsets.UTF_8));
            assertEquals("diagnostic", new String(bytes.stderr(), StandardCharsets.UTF_8));
            assertTrue(output.drain(deadline()));
            assertThrows(IOException.class, () -> output.initialize(child("ok")));
        } finally {
            assertTrue(output.drain(deadline()));
        }
    }

    @Test(timeout = 30000)
    public void oversizedParallelPipesAndNonzeroExitNeverProduceAnObservationButActuallyDrain() throws Exception {
        for (String mode : List.of("oversized", "failed")) {
            var output = new SegmentClassicSystemdQuery.Output();
            output.initialize(child(mode));
            try {
                assertThrows(IOException.class, () -> output.read(deadline()));
            } finally {
                assertTrue(output.drain(deadline()));
            }
        }
    }

    @Test(timeout = 30000)
    public void observationDeadlineDoesNotCancelTheProcessOrPromoteItsLateResult() throws Exception {
        Path release = temporary.getRoot().toPath().resolve("release");
        var output = new SegmentClassicSystemdQuery.Output();
        output.initialize(child("late", release.toString()));
        try {
            assertThrows(SocketTimeoutException.class,
                    () -> output.read(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(80)));
            assertFalse(output.drain(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(80)));
            Files.writeString(release, "release");
            assertTrue(output.drain(deadline()));
            assertThrows(IOException.class, () -> output.read(deadline()));
        } finally {
            Files.writeString(release, "release");
            assertTrue(output.drain(deadline()));
        }
    }

    @Test
    public void nonLinuxOwnerRejectsBeforeCreatingAQueryProcess() throws Exception {
        assumeFalse(SystemUtils.IS_OS_LINUX);
        var owner = new SegmentClassicSystemdQuery.Owner(UNIT);
        assertThrows(IOException.class, owner::initialize);
        assertThrows(IOException.class, () -> owner.properties(deadline()));
        assertTrue(owner.drain(deadline()));
        assertThrows(IOException.class, owner::initialize);
    }

    private static long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    }

    private static ProcessBuilder child(String... arguments) {
        var command = new java.util.ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java")
                .toString(), "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2",
                "-cp", System.getProperty("java.class.path"), PipePeer.class.getName()));
        command.addAll(List.of(arguments));
        return new ProcessBuilder(command);
    }

    public static final class PipePeer {
        public static void main(String[] arguments) throws Exception {
            if (System.in.read() != -1) {
                throw new IOException("Original parent did not close query stdin");
            }
            switch (arguments[0]) {
                case "ok" -> {
                    System.out.print(properties());
                    System.err.print("diagnostic");
                }
                case "oversized" -> {
                    byte[] chunk = new byte[4096];
                    for (int i = 0; i < 1024; i++) {
                        System.out.write(chunk);
                        System.err.write(chunk);
                    }
                }
                case "failed" -> System.exit(7); // Query fixture exit, never a Classic drain assertion.
                case "late" -> {
                    System.out.print(properties());
                    System.out.flush();
                    long end = deadline();
                    while (!Files.exists(Path.of(arguments[1]))) {
                        if (end - System.nanoTime() <= 0) {
                            throw new IOException("Private query fixture was not released");
                        }
                        Thread.sleep(5);
                    }
                }
                default -> throw new IOException("Unknown private query fixture");
            }
        }
    }
}

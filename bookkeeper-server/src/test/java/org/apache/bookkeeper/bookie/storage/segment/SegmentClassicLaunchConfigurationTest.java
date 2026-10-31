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
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Parser and effective-data checks, including fresh JVMs. Not the controlled launcher or resource graph. */
public class SegmentClassicLaunchConfigurationTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    static byte[] configuration() {
        return ("serverNumIOThreads=2\nnumAddWorkerThreads=1\nnumReadWorkerThreads=2\n"
                + "numLongPollWorkerThreads=1\nnumHighPriorityWorkerThreads=1\nnumJournalCallbackThreads=1\n"
                + "allocatorPoolingConcurrency=1\njournalMaxMemorySizeMb=16\npageLimit=128\nbookieId=history:3181\n"
                + "advertisedAddress=127.0.0.1\nbookiePort=3181\nallowLoopback=true\n"
                + "journalDirectories=/controlled-fixture/journal-1,/controlled-fixture/journal-2\n"
                + "ledgerDirectories=/controlled-fixture/ledger\nindexDirectories=/controlled-fixture/index\n"
                + "metadataServiceUri=zk+hierarchical://127.0.0.1:2181/original/ledgers\n")
                .getBytes(StandardCharsets.UTF_8);
    }

    private static SegmentClassicLaunchConfiguration.Frozen parse(String text) throws IOException {
        return SegmentClassicLaunchConfiguration.parse(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String input() {
        return new String(configuration(), StandardCharsets.UTF_8);
    }

    @Test
    public void commonsEscapesContinuationsAndOrderedListsBecomeIndependentFrozenValues() throws Exception {
        byte[] input = (input() + "credential=literal\\,comma\nextra=one,two\nextra=three\n"
                + "unicode=\\u03a9\ncontinued=first\\\n  second\n").getBytes(StandardCharsets.UTF_8);
        var frozen = SegmentClassicLaunchConfiguration.parse(input);
        var properties = frozen.properties();
        assertEquals("literal,comma", properties.get("credential"));
        assertEquals(List.of("one", "two", "three"), properties.get("extra"));
        assertEquals("Ω", properties.get("unicode"));
        assertEquals("firstsecond", properties.get("continued"));
        assertEquals(2, properties.get("serverNumIOThreads"));
        assertEquals(1, properties.get("allocatorPoolingConcurrency"));
        assertEquals(16L, properties.get("journalMaxMemorySizeMb"));
        assertEquals(128, properties.get("pageLimit"));
        assertEquals(List.of("/controlled-fixture/journal-1", "/controlled-fixture/journal-2"),
                properties.get("journalDirectories"));
        var actual = SegmentClassicLaunchConfiguration.materialize(frozen);
        assertEquals(2, actual.getServerNumIOThreads());
        assertEquals("history:3181", actual.getBookieId());
        assertEquals(List.of("/controlled-fixture/journal-1", "/controlled-fixture/journal-2"),
                List.of(actual.getJournalDirNames()));
        assertEquals("literal,comma", actual.getString("credential"));
        actual.setProperty("extra", "changed");
        assertEquals(List.of("one", "two", "three"), properties.get("extra"));
        Arrays.fill(input, (byte) 0);
        assertEquals(frozen.identity(), SegmentClassicLaunchConfiguration.identity(properties));
        assertThrows(UnsupportedOperationException.class, () -> properties.put("extra", "changed"));
        assertThrows(UnsupportedOperationException.class, () -> ((List<?>) properties.get("extra")).clear());
        assertEquals("history:3181", properties.get("bookieId"));
    }

    @Test
    public void includeInterpolationAndEscapedInjectionAreRejectedBeforeLoadingAnyOtherInput() throws Exception {
        Path foreign = temporary.newFile("not-properties").toPath();
        Files.writeString(foreign, "never parse this foreign file");
        for (String line : List.of("include=" + foreign, "INCLUDEOPTIONAL=" + foreign,
                "\\u0069nclude=" + foreign, "include\\\n Optional=" + foreign,
                "x=${env:HOME}", "x=\\u0024{sys:user.home}", "${sys:bad}=value",
                "x=$\\\n {sys:user.home}")) {
            assertThrows(line, IOException.class, () -> parse(input() + line + "\n"));
        }
        assertEquals("never parse this foreign file", Files.readString(foreign));
        assertThrows(IOException.class, () -> SegmentClassicLaunchConfiguration.parse(new byte[0]));
        assertThrows(IOException.class, () -> SegmentClassicLaunchConfiguration.parse(
                new byte[SegmentClassicLaunchConfiguration.MAX_BYTES + 1]));
        assertThrows(IOException.class, () -> SegmentClassicLaunchConfiguration.parse(new byte[]{(byte) 0xc3, 0x28}));
        assertThrows(IOException.class, () -> parse(input() + "x=\\uD800\n"));
        assertThrows(IOException.class, () -> parse(input() + "x=\\u0000\n"));
    }

    @Test
    public void missingDynamicInputsAndFallbackAliasesCannotBorrowTheParsersHostDefaults() throws Exception {
        for (String line : input().split("\n")) {
            if (!line.startsWith("allowLoopback=")) {
                assertThrows(line, IOException.class, () -> parse(input().replace(line + "\n", "")));
            }
        }
        for (String[] replacement : List.of(new String[]{"numLongPollWorkerThreads=1", "numLongPollWorkerThreads=0"},
                new String[]{"allocatorPoolingConcurrency=1", "allocatorPoolingConcurrency=0"},
                new String[]{"journalMaxMemorySizeMb=16", "journalMaxMemorySizeMb=0"},
                new String[]{"pageLimit=128", "pageLimit=0"},
                new String[]{"bookiePort=3181", "bookiePort=0"},
                new String[]{"advertisedAddress=127.0.0.1", "advertisedAddress=localhost"},
                new String[]{"indexDirectories=/controlled-fixture/index", "indexDirectories=relative"},
                new String[]{"indexDirectories=/controlled-fixture/index", "indexDirectories=/same,/same"},
                new String[]{"numReadWorkerThreads=2", "numReadWorkerThreads=2,3"},
                new String[]{"journalDirectories=/controlled-fixture/journal-1,/controlled-fixture/journal-2",
                        "journalDirectories="})) {
            assertThrows(Arrays.toString(replacement), IOException.class,
                    () -> parse(input().replace(replacement[0], replacement[1])));
        }
        for (String alias : List.of("journalDirectory=/unapproved", "zkServers=unapproved:2181")) {
            assertThrows(IOException.class, () -> parse(input() + alias + "\n"));
        }
    }

    @Test
    public void canonicalSnapshotCoversEveryPropertyAndListOrderButNotSourceLineOrder() throws Exception {
        var original = parse(input());
        var lines = new ArrayList<>(List.of(input().split("\n")));
        Collections.reverse(lines);
        assertEquals(original.identity(), parse(String.join("\n", lines)).identity());
        assertEquals(original.identity(), parse(input().replace("serverNumIOThreads=2", "serverNumIOThreads=02"))
                .identity());
        assertNotEquals(original.identity(), parse(input() + "unknown-but-covered=value\n").identity());
        assertNotEquals(original.identity(), parse(input().replace("journal-1,/controlled-fixture/journal-2",
                "journal-2,/controlled-fixture/journal-1")).identity());
        for (String key : original.properties().keySet()) {
            var changed = new HashMap<String, Object>(original.properties());
            changed.remove(key);
            assertNotEquals(key, original.identity(), SegmentClassicLaunchConfiguration.identity(changed));
        }
    }

    @Test
    public void independentTypedCanonicalVectorRejectsMutableAndNonfiniteValues() throws Exception {
        var list = new ArrayList<>(List.of("/a", "/b", "quoted,comma"));
        var values = new HashMap<String, Object>();
        values.put("alpha", true);
        values.put("count", 7);
        values.put("long", 1L << 40);
        values.put("ratio", 0.25d);
        values.put("roots", list);
        values.put("text", "Ω");
        values.put("\uE000", "bmp");
        values.put("\uD800\uDC00", "astral");
        var identity = SegmentClassicLaunchConfiguration.identity(values);
        assertEquals("954c53da6c0c7c1368be118c6a693a0df2235c1ffc8a91bc880c20a5e76d5d71",
                HexFormat.of().formatHex(identity.value()));
        var frozen = new SegmentClassicLaunchConfiguration.Frozen(new java.util.TreeMap<>(values), identity);
        list.set(0, "/changed");
        assertEquals(identity, SegmentClassicLaunchConfiguration.identity(frozen.properties()));
        for (Object bad : List.of(new Object(), new String[]{"array"}, Double.NaN, Double.POSITIVE_INFINITY,
                Float.valueOf(1), List.of(List.of("nested")), "\uD800")) {
            assertThrows(IllegalArgumentException.class,
                    () -> SegmentClassicLaunchConfiguration.identity(Map.of("bad", bad)));
        }
        assertNotEquals(SegmentClassicLaunchConfiguration.identity(Map.of("v", 1)),
                SegmentClassicLaunchConfiguration.identity(Map.of("v", 1L)));
        assertNotEquals(SegmentClassicLaunchConfiguration.identity(Map.of("v", "true")),
                SegmentClassicLaunchConfiguration.identity(Map.of("v", true)));
    }

    @Test
    public void freshJvmsAgreeWhenSystemInheritanceIsEnabledBeforeConfigurationClassInitialization()
            throws Exception {
        Path input = temporary.newFile("controlled.conf").toPath();
        Files.write(input, configuration());
        String expected = HexFormat.of().formatHex(SegmentClassicLaunchConfiguration.parse(configuration())
                .identity().value());
        for (boolean inherit : List.of(false, true)) {
            Path log = temporary.newFile("probe-" + inherit + ".log").toPath();
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Dorg.apache.bookkeeper.conf.readsystemproperties=" + inherit,
                    "-DserverNumIOThreads=999", "-DunapprovedInheritedProperty=present",
                    "-cp", System.getProperty("java.class.path"), ConfigurationProbe.class.getName(),
                    input.toString(), expected).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            try {
                assertTrue("Configuration-only subprocess must exit normally", process.waitFor(30, TimeUnit.SECONDS));
                assertEquals(Files.readString(log), 0, process.exitValue());
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(30, TimeUnit.SECONDS));
                }
            }
        }
    }

    /** Separate JVM specifically exercises AbstractConfiguration's static inheritance flag. */
    public static final class ConfigurationProbe {
        public static void main(String[] args) throws Exception {
            var frozen = SegmentClassicLaunchConfiguration.parse(Files.readAllBytes(Path.of(args[0])));
            assertEquals(args[1], HexFormat.of().formatHex(frozen.identity().value()));
            assertEquals(2, frozen.properties().get("serverNumIOThreads"));
            assertFalse(frozen.properties().containsKey("unapprovedInheritedProperty"));
            assertFalse(frozen.properties().containsKey("user.home"));
        }
    }
}

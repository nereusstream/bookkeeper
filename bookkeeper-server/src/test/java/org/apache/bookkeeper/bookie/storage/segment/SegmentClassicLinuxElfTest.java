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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Independent synthetic layout and real file reads; no ELF is loaded or executed. */
public class SegmentClassicLinuxElfTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    static long deadline() {
        return System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    }

    static byte[] image() {
        ByteBuffer b = ByteBuffer.allocate(1536).order(ByteOrder.LITTLE_ENDIAN);
        b.put(new byte[]{0x7f, 'E', 'L', 'F', 2, 1, 1, 0});
        b.putShort(16, (short) 3).putShort(18, (short) 62).putInt(20, 1);
        b.putLong(24, 0x400100).putLong(32, 64);
        b.putShort(52, (short) 64).putShort(54, (short) 56).putShort(56, (short) 3);
        byte[] interpreter = "/lib64/ld-linux-x86-64.so.2\0".getBytes(StandardCharsets.UTF_8);
        ph(b, 64, 3, 4, 256, 0x400100, interpreter.length, interpreter.length, 1);
        ph(b, 120, 1, 5, 0, 0x400000, 1536, 1536, 4096);
        ph(b, 176, 2, 4, 512, 0x400200, 160, 160, 8);
        b.position(256).put(interpreter);
        var strings = new ByteArrayOutputStream();
        strings.write(0);
        int z = text(strings, "libz.so.1");
        int a = text(strings, "liba.so.2");
        int name = text(strings, "libowned.so.3");
        int rpath = text(strings, "/opt/approved/lib:/usr/lib");
        int runpath = text(strings, "/opt/approved/hwcaps");
        b.position(1024).put(strings.toByteArray());
        b.position(512);
        for (long[] pair : new long[][]{{5, 0x400400}, {10, strings.size()}, {1, z}, {1, a},
                {14, name}, {15, rpath}, {29, runpath}, {30, 8}, {0x6ffffffbL, 1}, {0, 0}}) {
            b.putLong(pair[0]).putLong(pair[1]);
        }
        return b.array();
    }

    private static int text(ByteArrayOutputStream stream, String text) {
        int index = stream.size();
        stream.writeBytes(text.getBytes(StandardCharsets.UTF_8));
        stream.write(0);
        return index;
    }

    private static void ph(ByteBuffer b, int offset, int type, int flags, long fileOffset,
                           long address, long fileBytes, long memoryBytes, long alignment) {
        b.position(offset).putInt(type).putInt(flags).putLong(fileOffset).putLong(address).putLong(0)
                .putLong(fileBytes).putLong(memoryBytes).putLong(alignment);
    }

    private SegmentClassicLinuxElf.Image read(byte[] bytes) throws IOException {
        Path path = temporary.newFile().toPath();
        Files.write(path, bytes);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return SegmentClassicLinuxElf.read(channel, bytes.length, deadline());
        }
    }

    private void invalid(Consumer<ByteBuffer> change) throws IOException {
        byte[] bytes = image();
        change.accept(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
        assertThrows(IOException.class, () -> read(bytes));
    }

    @Test
    public void readsActualFileMetadataInDependencyOrderWithoutClosingOrMovingOriginalChannel() throws Exception {
        Path path = temporary.newFile().toPath();
        Files.write(path, image());
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(17);
            var value = SegmentClassicLinuxElf.read(channel, channel.size(), deadline());
            assertEquals(3, value.type());
            assertEquals(0x400100, value.entry());
            assertEquals("/lib64/ld-linux-x86-64.so.2", value.interpreter());
            assertEquals("libowned.so.3", value.soname());
            assertEquals(List.of("libz.so.1", "liba.so.2"), value.needed());
            assertEquals(List.of(Path.of("/opt/approved/lib"), Path.of("/usr/lib")), value.rpath());
            assertEquals(List.of(Path.of("/opt/approved/hwcaps")), value.runpath());
            assertEquals(8, value.flags());
            assertEquals(1, value.flags1());
            assertEquals(17, channel.position());
            assertTrue(channel.isOpen());
        }
    }

    @Test
    public void staticImageAndUnusedProgramHeaderDoNotInventDependencies() throws Exception {
        byte[] bytes = image();
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(176, 0).putLong(184, -1).putLong(208, -1);
        var value = read(bytes);
        assertTrue(value.needed().isEmpty());
        assertNull(value.soname());
    }

    @Test
    public void rejectsOtherArchitecturesMalformedTablesAndLoadedBounds() throws Exception {
        invalid(b -> b.put(4, (byte) 1));
        invalid(b -> b.put(5, (byte) 2));
        invalid(b -> b.putShort(18, (short) 183));
        invalid(b -> b.putLong(32, Long.MAX_VALUE));
        invalid(b -> b.putShort(56, (short) 129));
        invalid(b -> b.putInt(176, 0x70000000));
        invalid(b -> b.putLong(152, 1537));
        invalid(b -> b.putLong(160, 1535));
        invalid(b -> b.putLong(168, 3));
        invalid(b -> b.putLong(136, Long.MAX_VALUE));
        invalid(b -> b.putLong(24, 0x500000));
        invalid(b -> b.putLong(192, Long.MAX_VALUE));
    }

    @Test
    public void rejectsUnmappedAmbiguousAndUnterminatedDynamicTables() throws Exception {
        invalid(b -> b.putLong(192, 0x400201));
        invalid(b -> b.putLong(520, 0x500000));
        invalid(b -> b.putLong(520, Long.MAX_VALUE));
        invalid(b -> b.putLong(536, 0));
        invalid(b -> b.putLong(536, Long.MAX_VALUE));
        invalid(b -> b.putLong(552, -1));
        invalid(b -> b.putLong(552, 4096));
        invalid(b -> b.putLong(656, 4));
        invalid(b -> ph(b, 64, 1, 4, 0, 0x400000, 1536, 1536, 1));
    }

    @Test
    public void rejectsDuplicateAndUnapprovedLoadingExtensions() throws Exception {
        invalid(b -> b.putLong(576, 5));
        invalid(b -> b.putLong(568, b.getLong(552)));
        invalid(b -> b.putLong(576, 0x6ffffefcL)); // DT_AUDIT
        invalid(b -> b.putLong(576, 0x6ffffefbL)); // DT_DEPAUDIT
        invalid(b -> b.putLong(576, 0x7fffffffL)); // DT_FILTER
        invalid(b -> b.putLong(576, 0x7ffffffdL)); // DT_AUXILIARY
        invalid(b -> b.putLong(576, 22)); // DT_TEXTREL
        invalid(b -> b.putLong(632, 1)); // DF_ORIGIN
        invalid(b -> b.putLong(648, 0x400)); // DF_1_INTERPOSE
    }

    @Test
    public void rejectsRelativeSearchTokensInvalidNamesAndMalformedUtf8() throws Exception {
        invalid(b -> b.put(1025, (byte) '/'));
        invalid(b -> b.put(1025, (byte) 0xc0));
        invalid(b -> b.putLong(600, 0)); // Empty RPATH
        invalid(b -> b.put(256, (byte) '$'));
        invalid(b -> b.put(257, (byte) 0)); // Interior interpreter terminator
        invalid(b -> b.put(256 + "/lib64/ld-linux-x86-64.so.2".length(), (byte) 'x')); // Missing interpreter terminator
        for (String path : List.of("relative", "/lib:", "/lib::/usr/lib", "$ORIGIN/lib", "/lib/../usr/lib",
                "/lib//x", "/")) {
            invalid(b -> {
                b.position(1200).put(path.getBytes(StandardCharsets.UTF_8)).put((byte) 0);
                b.putLong(536, 256).putLong(600, 176);
            });
        }
    }

    @Test
    public void deadlinesAndTruncatedOriginalFilesFailWithoutClosingTheCallersChannel() throws Exception {
        Path path = temporary.newFile().toPath();
        Files.write(path, image());
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxElf.read(channel, 1536, System.nanoTime()));
            channel.truncate(63);
            assertThrows(IOException.class, () -> SegmentClassicLinuxElf.read(channel, 1536, deadline()));
            assertTrue(channel.isOpen());
        }
    }

    private SegmentClassicLinuxElf.Image2 readVersionTwo(byte[] bytes) throws IOException {
        Path path = temporary.newFile().toPath();
        Files.write(path, bytes);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return SegmentClassicLinuxElf.readVersionTwo(channel, bytes.length, deadline());
        }
    }

    @Test
    public void independentVersionTwoVectorKeepsRawFactsWithoutPathOrSearchInterpretation() throws Exception {
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-linux-elf-v2");
        assertEquals(1536, bytes.length);
        assertEquals("894bbf508d445c4323df38818df24e56b091cf5feadd832c8b9fad192e2bebc3",
                java.util.HexFormat.of().formatHex(SegmentClassicHandoff.digest(bytes).value()));
        Path path = temporary.newFile().toPath();
        Files.write(path, bytes);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(17);
            var value = SegmentClassicLinuxElf.readVersionTwo(channel, bytes.length, deadline());
            assertEquals("/lib64/../lib/ld-linux-x86-64.so.2", value.interpreter());
            assertEquals("/not/a/whitelisted SONAME/雪.so", value.soname());
            assertEquals(List.of("libz.so.1", "libz.so.1", "../${LIB}/lib雪.so", "$ORIGIN/deps/libx.so"),
                    value.needed());
            assertEquals(new DynamicPath(true, "$ORIGIN/../lib::relative:${LIB}/x:${PLATFORM}/p;/literal"),
                    value.rpath());
            assertEquals(new DynamicPath(true, ""), value.runpath());
            assertTrue(value.flagsPresent());
            assertTrue(value.flags1Present());
            assertEquals(0x1b, value.flags());
            assertEquals(0x08000889, value.flags1());
            assertEquals(17, channel.position());
            assertTrue(channel.isOpen());
            assertThrows(UnsupportedOperationException.class, () -> value.needed().clear());
            assertThrows(IOException.class, () -> SegmentClassicLinuxElf.read(channel, bytes.length, deadline()));
            assertEquals(17, channel.position());
        }
    }

    /** Independent synthetic dynamic table, with all strings in one unambiguous file-backed load mapping. */
    private static byte[] dynamicImage(List<long[]> entries, byte[] strings) {
        int dynamicBytes = (entries.size() + 3) * 16;
        int stringOffset = (512 + dynamicBytes + 63) & ~63;
        int length = Math.max(1536, stringOffset + strings.length);
        ByteBuffer b = ByteBuffer.wrap(Arrays.copyOf(image(), length)).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(152, length).putLong(160, length).putLong(208, dynamicBytes).putLong(216, dynamicBytes);
        b.position(512).putLong(5).putLong(0x400000 + stringOffset).putLong(10).putLong(strings.length);
        for (long[] entry : entries) {
            b.putLong(entry[0]).putLong(entry[1]);
        }
        b.putLong(0).putLong(0);
        b.position(stringOffset).put(strings);
        return b.array();
    }

    @Test
    public void versionTwoDistinguishesAbsentAndPresentEmptyTagsWithoutInferringOriginFlags() throws Exception {
        var absent = readVersionTwo(dynamicImage(List.of(), new byte[]{0}));
        assertNull(absent.soname());
        assertEquals(new DynamicPath(false, ""), absent.rpath());
        assertEquals(new DynamicPath(false, ""), absent.runpath());
        assertFalse(absent.flagsPresent());
        assertFalse(absent.flags1Present());
        var empty = readVersionTwo(dynamicImage(List.of(new long[]{14, 0}, new long[]{15, 0}, new long[]{29, 0},
                new long[]{30, 0}, new long[]{0x6ffffffbL, 0}), new byte[]{0}));
        assertEquals("", empty.soname());
        assertEquals(new DynamicPath(true, ""), empty.rpath());
        assertEquals(new DynamicPath(true, ""), empty.runpath());
        assertTrue(empty.flagsPresent());
        assertTrue(empty.flags1Present());
        assertEquals(0, empty.flags());
        assertEquals(0, empty.flags1());
        var token = readVersionTwo(dynamicImage(List.of(new long[]{15, 1}),
                "\0$ORIGIN::../relative\0".getBytes(StandardCharsets.UTF_8)));
        assertEquals(new DynamicPath(true, "$ORIGIN::../relative"), token.rpath());
        assertFalse(token.flagsPresent());
        assertFalse(token.flags1Present());
        assertEquals(0, token.flags());
        assertEquals(0, token.flags1());
    }

    @Test
    public void versionTwoHasExplicitlyBroaderFlagsNamesAndDuplicateRulesWhileLegacyRejectsThem() throws Exception {
        for (long[] flag : List.of(new long[]{30, 1}, new long[]{0x6ffffffbL, 0x80},
                new long[]{0x6ffffffbL, 0x800})) {
            byte[] bytes = dynamicImage(List.of(flag), new byte[]{0});
            readVersionTwo(bytes);
            assertThrows(IOException.class, () -> read(bytes));
        }
        byte[] duplicated = image();
        ByteBuffer.wrap(duplicated).order(ByteOrder.LITTLE_ENDIAN).putLong(568, 1);
        // The first needed name starts at string index 1; each repeated ordinal remains observable.
        assertEquals(List.of("libz.so.1", "libz.so.1"), readVersionTwo(duplicated).needed());
        assertThrows(IOException.class, () -> read(duplicated));
        byte[] relative = dynamicImage(List.of(new long[]{1, 1}, new long[]{14, 1}),
                "\0../lib/雪.so\0".getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of("../lib/雪.so"), readVersionTwo(relative).needed());
        assertEquals("../lib/雪.so", readVersionTwo(relative).soname());
        assertThrows(IOException.class, () -> read(relative));
    }

    private static byte[] repeatedNeeded(int count, int bytes) {
        var entries = new ArrayList<long[]>();
        for (int i = 0; i < count; i++) {
            entries.add(new long[]{1, 1});
        }
        byte[] strings = new byte[bytes + 2];
        Arrays.fill(strings, 1, bytes + 1, (byte) 'a');
        return dynamicImage(entries, strings);
    }

    @Test
    public void repeatedTextIsChargedInFullAndNeededCountDoesNotReplaceTheByteBudget() throws Exception {
        assertEquals(1024, readVersionTwo(repeatedNeeded(1024, 1)).needed().size());
        assertThrows(IOException.class, () -> readVersionTwo(repeatedNeeded(1025, 1)));
        assertEquals(255, readVersionTwo(repeatedNeeded(255, 4096)).needed().size());
        // 256 * 4096 plus the original interpreter already exceeds the one MiB cumulative text budget.
        assertThrows(IOException.class, () -> readVersionTwo(repeatedNeeded(256, 4096)));
        assertThrows(IOException.class, () -> readVersionTwo(repeatedNeeded(1, 4097)));
    }

    @Test
    public void versionTwoStillRejectsMalformedMappingUtf8SingletonsAndUnsupportedExtensions() throws Exception {
        for (Consumer<ByteBuffer> change : List.<Consumer<ByteBuffer>>of(
                b -> b.putShort(18, (short) 183),
                b -> b.putLong(32, Long.MAX_VALUE),
                b -> b.putLong(192, 0x400201),
                b -> b.putLong(520, Long.MAX_VALUE),
                b -> b.putLong(576, 5),
                b -> b.putLong(576, 0x6ffffefcL),
                b -> b.putLong(576, 0x7fffffffL),
                b -> b.putLong(576, 22),
                b -> b.putLong(632, 4),
                b -> b.putLong(648, 0x400),
                b -> b.putLong(656, 4),
                b -> b.put(1025, (byte) 0xc0),
                b -> b.put(257, (byte) 0))) {
            byte[] bytes = image();
            change.accept(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
            assertThrows(IOException.class, () -> readVersionTwo(bytes));
        }
    }

    @Test
    public void versionTwoStaticAndNoStringReturnsKeepPresenceAndOriginalDeadlineChecks() throws Exception {
        byte[] staticBytes = image();
        ByteBuffer.wrap(staticBytes).order(ByteOrder.LITTLE_ENDIAN).putInt(176, 0).putLong(184, -1);
        var noDynamic = readVersionTwo(staticBytes);
        assertTrue(noDynamic.needed().isEmpty());
        assertFalse(noDynamic.flagsPresent());
        byte[] noStrings = image();
        ByteBuffer.wrap(noStrings).order(ByteOrder.LITTLE_ENDIAN).position(512)
                .putLong(30).putLong(0).putLong(0x6ffffffbL).putLong(0).putLong(0).putLong(0);
        var onlyFlags = readVersionTwo(noStrings);
        assertTrue(onlyFlags.needed().isEmpty());
        assertTrue(onlyFlags.flagsPresent());
        assertTrue(onlyFlags.flags1Present());
        for (byte[] bytes : List.of(staticBytes, noStrings, image())) {
            Path path = temporary.newFile().toPath();
            Files.write(path, bytes);
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                assertThrows(IOException.class,
                        () -> SegmentClassicLinuxElf.readVersionTwo(channel, bytes.length, System.nanoTime()));
                channel.truncate(63);
                assertThrows(IOException.class,
                        () -> SegmentClassicLinuxElf.readVersionTwo(channel, bytes.length, deadline()));
                assertTrue(channel.isOpen());
            }
        }
    }

    /** External raw metadata diagnostic only; caller owns the actual file channel. */
    public static final class Probe2 {
        public static void main(String[] args) throws Exception {
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                byte[] metadata = (SegmentClassicLinuxElf.readVersionTwo(channel, channel.size(), deadline()) + "\n")
                        .getBytes(StandardCharsets.UTF_8);
                System.out.write(metadata);
                System.out.flush();
            }
        }
    }

    /** External diagnostic for an explicitly supplied existing file; never loads/executes it. */
    public static final class Probe {
        public static void main(String[] args) throws Exception {
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                System.out.println(SegmentClassicLinuxElf.read(channel, channel.size(), deadline()));
            }
        }
    }
}

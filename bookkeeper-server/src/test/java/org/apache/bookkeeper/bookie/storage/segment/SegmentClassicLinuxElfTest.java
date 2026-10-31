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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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

    /** External diagnostic for an explicitly supplied existing file; never loads/executes it. */
    public static final class Probe {
        public static void main(String[] args) throws Exception {
            try (FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ)) {
                System.out.println(SegmentClassicLinuxElf.read(channel, channel.size(), deadline()));
            }
        }
    }
}

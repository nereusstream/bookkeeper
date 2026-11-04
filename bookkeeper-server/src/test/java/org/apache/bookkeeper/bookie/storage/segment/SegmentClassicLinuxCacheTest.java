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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Independently written file-format vectors and actual private file reads, never a loader/qualification oracle. */
public class SegmentClassicLinuxCacheTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    static byte[] vector() throws IOException {
        return SegmentClassicLinuxDeploymentTest.vector("classic-linux-cache-v1");
    }

    private static ByteBuffer bytes(byte[] value) {
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
    }

    private SegmentClassicLinuxCache.Image read(byte[] value) throws IOException {
        Path file = temporary.newFile().toPath();
        Files.write(file, value);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.position(17);
            var image = SegmentClassicLinuxCache.read(channel, value.length, SegmentClassicLinuxElfTest.deadline());
            assertTrue(channel.isOpen());
            assertEquals(17, channel.position());
            return image;
        }
    }

    private void invalid(Consumer<ByteBuffer> change) throws IOException {
        byte[] value = vector();
        change.accept(bytes(value));
        assertThrows(IOException.class, () -> read(value));
    }

    @Test
    public void independentVectorRetainsNumericOrderAllAbisRawBytesAndNonTerminatedGenerator() throws Exception {
        byte[] value = vector();
        var image = read(value);
        assertEquals(551, value.length);
        assertEquals(2, image.endianFlag());
        assertEquals(7, image.entries().size());
        assertEquals("ldconfig (independent byte vector) release version 2.39", image.generator().utf8());
        assertEquals(List.of("x86-64-v2", "x86-64-v3"),
                List.of(image.hwcaps().get(0).utf8(), image.hwcaps().get(1).utf8()));
        assertEquals("libz.so.10", image.entries().get(0).name().utf8());
        assertEquals("libz.so.2", image.entries().get(1).name().utf8());
        assertEquals(0, image.entries().get(1).hwcapIndex());
        assertEquals(2, image.entries().get(1).isaLevel());
        assertEquals(1, image.entries().get(2).hwcapIndex());
        assertEquals(3, image.entries().get(2).isaLevel());
        assertEquals(-1, image.entries().get(3).hwcapIndex());
        assertTrue(image.entries().get(3).lp64());
        assertFalse(image.entries().get(4).lp64());
        assertEquals(0x12345678L, image.entries().get(4).osVersionUnused());
        assertEquals(0x803, image.entries().get(5).flags());
        assertEquals("liba.so.01", image.entries().get(6).name().utf8());
        assertEquals(Long.MIN_VALUE, image.entries().get(6).hwcap());
        assertThrows(IOException.class, () -> image.entries().get(6).path().utf8());
        byte[] raw = image.entries().get(6).path().bytes();
        assertEquals(255, Byte.toUnsignedInt(raw[7]));
        raw[0] = 0;
        assertEquals('/', image.entries().get(6).path().bytes()[0]);
        assertThrows(UnsupportedOperationException.class, () -> image.entries().clear());
        assertThrows(UnsupportedOperationException.class, () -> image.hwcaps().clear());
        assertEquals(image, read(value));
    }

    @Test
    public void explicitEndianUnsetAndNoExtensionRetainLegacyHwcapWithoutInventingNamedIndexes() throws Exception {
        byte[] original = vector();
        int end = 48 + 24 * 7 + bytes(original).getInt(24);
        byte[] value = Arrays.copyOf(original, end);
        ByteBuffer b = bytes(value);
        b.put(28, (byte) 0).putInt(32, 0).putLong(48 + 24 + 16, 0).putLong(48 + 48 + 16, 0);
        var image = read(value);
        assertEquals(0, image.endianFlag());
        assertNull(image.generator());
        assertTrue(image.hwcaps().isEmpty());
        assertEquals(Long.MIN_VALUE, image.entries().get(6).hwcap());
        assertEquals(-1, image.entries().get(6).hwcapIndex());
    }

    @Test
    public void relatedRowsUseNumericEquivalenceAndPreserveOriginalOrderAndCapabilities() throws Exception {
        byte[] value = vector();
        // The other ABI's equivalent name has an invalid-UTF8 path; group matching must not decode it.
        bytes(value).putInt(48 + 4 * 24 + 8, bytes(value).getInt(48 + 6 * 24 + 8));
        bytes(value).putInt(48 + 6 * 24, 0x303); // Keep the unrelated raw-path row in this query's ABI.
        var image = read(value);
        var rows = SegmentClassicLinuxCache.related(image, "libz.so.02", SegmentClassicLinuxElfTest.deadline());
        assertEquals(List.of(1, 2, 3), rows.stream().map(SegmentClassicLinuxCache.Entry::ordinal).toList());
        assertEquals(image.entries().subList(1, 4), rows);
        assertEquals(List.of(0, 1, -1), rows.stream().map(SegmentClassicLinuxCache.Entry::hwcapIndex).toList());
        assertEquals(List.of(2, 3, 0), rows.stream().map(SegmentClassicLinuxCache.Entry::isaLevel).toList());
        var raw = SegmentClassicLinuxCache.related(image, "liba.so.1", SegmentClassicLinuxElfTest.deadline());
        assertEquals(6, raw.get(0).ordinal());
        assertEquals(255, Byte.toUnsignedInt(raw.get(0).path().bytes()[7]));
        assertThrows(UnsupportedOperationException.class, rows::clear);
    }

    @Test
    public void cacheQueriesApplyTheSameSignedCIntermediateOverflowAndStrictByteBounds() throws Exception {
        var image = read(vector());
        assertTrue(SegmentClassicLinuxCache.related(image, "libz.so.2147483599",
                SegmentClassicLinuxElfTest.deadline()).isEmpty());
        for (String name : Arrays.asList(null, "", "bad\0name", "libz.so.2147483600", "libz.so.2147483647",
                "libz.so.999999999999999", "\ud800", "雪".repeat(1366), "x".repeat(4097))) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxCache.related(image, name,
                    SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxCache.related(image, "libz.so.2", System.nanoTime()));
    }

    private static SegmentClassicLinuxCache.Capabilities capabilities(long isa1, List<String> active) {
        return new SegmentClassicLinuxCache.Capabilities(0, 0, Optional.empty(), isa1, active);
    }

    private static SegmentClassicLinuxCache.Selection select(SegmentClassicLinuxCache.Image image, String name,
            SegmentClassicLinuxCache.Capabilities capabilities, long flags1, List<String> directories)
            throws IOException {
        return SegmentClassicLinuxCache.select(image, name, capabilities, flags1, directories,
                SegmentClassicLinuxElfTest.deadline());
    }

    @Test
    public void namedSelectionUsesIndependentIsaAndActivePriorityWithFirstRowWinningTies() throws Exception {
        var image = read(vector());
        var both = capabilities(12, List.of("x86-64-v4", "x86-64-v3", "x86-64-v2"));
        var best = select(image, "libz.so.02", both, 0, List.of("/lib/"));
        assertEquals(SegmentClassicLinuxCache.SelectionState.SELECTED, best.state());
        assertSame(image.entries().get(2), best.entry());
        assertSame(image.entries().get(1), select(image, "libz.so.2",
                capabilities(12, List.of("x86-64-v2")), 0, List.of("/lib/")).entry());
        assertSame(image.entries().get(3), select(image, "libz.so.2",
                capabilities(4, List.of("x86-64-v3")), 0, List.of("/lib/")).entry());
        assertSame(image.entries().get(3), select(image, "libz.so.2",
                capabilities(12, List.of()), 0, List.of("/lib/")).entry());
        byte[] ties = vector();
        bytes(ties).putLong(48 + 2 * 24 + 16, 0x4000000300000000L);
        var tied = read(ties);
        assertSame(tied.entries().get(1), select(tied, "libz.so.2", both, 0, List.of("/lib/")).entry());
        byte[] v4 = vector();
        int hwcapName = bytes(v4).getInt(492);
        v4[hwcapName + 8] = '4';
        var fourth = read(v4);
        assertSame(fourth.entries().get(2), select(fourth, "libz.so.2", both, 0, List.of("/lib/")).entry());
    }

    @Test
    public void legacySelectionUsesRawMaskTlsAndOnlyTheTwoX8664Platforms() throws Exception {
        long haswell = 1L << 50;
        long xeon = 1L << 51;
        var image = read(legacy(new long[]{1L << 48, 1L << 49, haswell | xeon, haswell, xeon, 0},
                "/i586/x", "/i686/x", "/both/x", "/haswell/x", "/xeon/x", "/base/x"));
        for (var item : List.of(new Object[]{"haswell", 3}, new Object[]{"xeon_phi", 4},
                new Object[]{"i586", 5}, new Object[]{"unknown", 5})) {
            var facts = new SegmentClassicLinuxCache.Capabilities(-1L, -1L,
                    Optional.of((String) item[0]), 0, List.of());
            assertEquals(item[1], select(image, "libx.so.1", facts, 0, List.of("/lib/")).entry().ordinal());
        }
        assertSame(image.entries().get(5), select(image, "libx.so.1",
                capabilities(0, List.of()), 0, List.of("/lib/")).entry());
        var tls = read(legacy(new long[]{Long.MIN_VALUE, 0}, "/tls/x", "/base/x"));
        assertSame(tls.entries().get(0), select(tls, "libx.so.1",
                capabilities(0, List.of()), 0, List.of("/lib/")).entry());
        var masked = read(legacy(new long[]{128 | Long.MIN_VALUE, 0}, "/masked/x", "/base/x"));
        assertSame(masked.entries().get(1), select(masked, "libx.so.1",
                new SegmentClassicLinuxCache.Capabilities(128, 0, Optional.empty(), 0, List.of()),
                0, List.of("/lib/")).entry());
        assertSame(masked.entries().get(0), select(masked, "libx.so.1",
                new SegmentClassicLinuxCache.Capabilities(128, 128, Optional.empty(), 0, List.of()),
                0, List.of("/lib/")).entry());
        var absent = select(masked, "libx.so.2", capabilities(0, List.of()), 0, List.of("/lib/"));
        assertEquals(SegmentClassicLinuxCache.SelectionState.NO_MATCH, absent.state());
        assertNull(absent.entry());
    }

    @Test
    public void reachedIsaIndexesRejectSignedCUndefinedShiftsWithoutTighteningStructuralParsing() throws Exception {
        byte[] value = vector();
        bytes(value).putLong(48 + 24 + 16, 0x4000001e00000000L);
        var valid = read(value);
        assertSame(valid.entries().get(1), select(valid, "libz.so.2",
                capabilities(1L << 30, List.of("x86-64-v2")), 0, List.of("/lib/")).entry());
        assertSame(valid.entries().get(3), select(valid, "libz.so.2",
                capabilities(0x80000000L, List.of("x86-64-v2")), 0, List.of("/lib/")).entry());
        for (int index : List.of(31, 32, 1023)) {
            bytes(value).putLong(48 + 24 + 16, 0x4000000000000000L | (long) index << 32);
            var unsupported = read(value);
            assertEquals(index, unsupported.entries().get(1).isaLevel());
            assertThrows(IOException.class, () -> select(unsupported, "libz.so.2",
                    capabilities(0xffffffffL, List.of()), 0, List.of("/lib/")));
        }
        value = vector();
        bytes(value).putLong(48 + 4 * 24 + 16, 0x4000001f00000000L);
        bytes(value).putInt(48 + 6 * 24, 0x303).putLong(48 + 6 * 24 + 16, 0x4000002000000000L);
        var irrelevant = read(value);
        assertSame(irrelevant.entries().get(3), select(irrelevant, "libz.so.02",
                capabilities(0, List.of()), 0, List.of("/lib/")).entry());
    }

    @Test
    public void unknownRawNamedCapsAndSelectedRawPathsAreNeverReplacementDecoded() throws Exception {
        byte[] value = vector();
        value[bytes(value).getInt(492)] = (byte) 255;
        bytes(value).putInt(48 + 24 + 8, bytes(value).getInt(48 + 6 * 24 + 8));
        var image = read(value);
        assertThrows(IOException.class, () -> image.hwcaps().get(1).utf8());
        var selected = select(image, "libz.so.2", capabilities(12, List.of("x86-64-v3", "x86-64-v2")),
                0x800, List.of("/lib/"));
        assertSame(image.entries().get(1), selected.entry());
        assertSame(image.entries().get(1).path(), selected.entry().path());
        assertThrows(IOException.class, () -> selected.entry().path().utf8());
    }

    @Test
    public void nodeflibFiltersOnlyTheChosenRawPathAndNeverRescansOrResolvesAliases() throws Exception {
        var facts = capabilities(0, List.of());
        var image = read(legacy(new long[]{0, 0}, "/lib/missing.so", "/opt/existing.so"));
        var rejected = select(image, "libx.so.1", facts, 0x800, List.of("/lib/"));
        assertEquals(SegmentClassicLinuxCache.SelectionState.REJECTED_NODEFLIB, rejected.state());
        assertSame(image.entries().get(0), rejected.entry());
        assertSame(image.entries().get(0), select(image, "libx.so.1", facts, 0, List.of("/lib/")).entry());
        assertEquals(SegmentClassicLinuxCache.SelectionState.SELECTED,
                select(image, "libx.so.1", facts, 0x800, List.of("/lib64/")).state());
        for (String path : List.of("/lib64/x", "/alias-to-lib/x", "/opt/../lib/x", "/lib")) {
            assertEquals(SegmentClassicLinuxCache.SelectionState.SELECTED,
                    select(read(legacy(new long[]{0}, path)), "libx.so.1", facts, 0x800, List.of("/lib/")).state());
        }
        for (String path : List.of("/lib/x", "/lib/../opt/x", "/lib//x")) {
            assertEquals(SegmentClassicLinuxCache.SelectionState.REJECTED_NODEFLIB,
                    select(read(legacy(new long[]{0}, path)), "libx.so.1", facts, 0x800, List.of("/lib/")).state());
        }
        assertEquals(SegmentClassicLinuxCache.SelectionState.REJECTED_NODEFLIB,
                select(image, "libx.so.1", facts, 0x800, List.of("/")).state());
        var named = read(vector());
        var winner = select(named, "libz.so.2", capabilities(12, List.of("x86-64-v3", "x86-64-v2")),
                0x800, List.of("/"));
        assertEquals(SegmentClassicLinuxCache.SelectionState.REJECTED_NODEFLIB, winner.state());
        assertSame(named.entries().get(2), winner.entry());
    }

    @Test
    public void missingAndUnboundedSelectionFactsCannotBecomeNoMatchEvenWhenNodeflibIsUnset() throws Exception {
        var image = read(vector());
        var valid = capabilities(0, List.of());
        for (var facts : Arrays.asList(null,
                new SegmentClassicLinuxCache.Capabilities(0, 0, null, 0, List.of()),
                capabilities(-1, List.of()), capabilities(0x100000000L, List.of()), capabilities(0, null),
                capabilities(0, Arrays.asList((String) null)), capabilities(0, List.of("custom")),
                capabilities(0, List.of("x86-64-v2", "x86-64-v3")),
                capabilities(0, List.of("x86-64-v3", "x86-64-v3")),
                capabilities(0, List.of("x86-64-v4", "x86-64-v3", "x86-64-v2", "extra")),
                new SegmentClassicLinuxCache.Capabilities(0, 0, Optional.of("bad\0platform"), 0, List.of()))) {
            assertThrows(IOException.class, () -> select(image, "absent.so", facts, 0, List.of("/lib/")));
        }
        for (List<String> prefixes : Arrays.<List<String>>asList(null, List.of(), Arrays.asList((String) null),
                List.of(""), List.of("relative/"), List.of("/lib"), List.of("/bad\0/"), List.of("/\ud800/"),
                List.of("/" + "雪".repeat(1365) + "/"), java.util.Collections.nCopies(65, "/lib/"))) {
            assertThrows(IOException.class, () -> select(image, "absent.so", valid, 0, prefixes));
        }
        assertEquals(SegmentClassicLinuxCache.SelectionState.NO_MATCH,
                select(image, "absent.so", valid, 0, List.of("/" + "x".repeat(4094) + "/")).state());
        assertThrows(IOException.class, () -> SegmentClassicLinuxCache.select(image, "libz.so.2", valid,
                0, List.of("/lib/"), System.nanoTime()));
    }

    private static byte[] legacy(long[] hwcaps, String... paths) {
        byte[] name = "libx.so.1".getBytes(StandardCharsets.US_ASCII);
        var strings = Arrays.stream(paths).map(s -> s.getBytes(StandardCharsets.UTF_8)).toList();
        int start = 48 + hwcaps.length * 24;
        int length = start + name.length + 1 + strings.stream().mapToInt(s -> s.length + 1).sum();
        byte[] value = new byte[length];
        ByteBuffer b = bytes(value);
        b.put("glibc-ld.so.cache1.1".getBytes(StandardCharsets.US_ASCII));
        b.putInt(20, hwcaps.length).putInt(24, length - start).put(28, (byte) 2);
        b.position(start).put(name).put((byte) 0);
        int offset = start + name.length + 1;
        for (int i = 0; i < hwcaps.length; i++) {
            b.position(48 + i * 24).putInt(0x303).putInt(start).putInt(offset).putInt(-1).putLong(hwcaps[i]);
            b.position(offset).put(strings.get(i)).put((byte) 0);
            offset += strings.get(i).length + 1;
        }
        return value;
    }

    @Test
    public void rejectsLegacyUnknownHeadersLengthsAndStringReferences() throws Exception {
        invalid(b -> b.put(0, (byte) 'x'));
        invalid(b -> b.put(19, (byte) '2'));
        invalid(b -> b.put(28, (byte) 1));
        invalid(b -> b.put(28, (byte) 3));
        invalid(b -> b.put(28, (byte) 0x82));
        invalid(b -> b.put(29, (byte) 1));
        invalid(b -> b.putInt(36, 1));
        invalid(b -> b.putInt(20, -1));
        invalid(b -> b.putInt(24, -1));
        invalid(b -> b.putInt(52, 48));
        invalid(b -> b.putInt(52, 551));
        invalid(b -> b.putInt(56, -1));
        invalid(b -> b.putInt(52, 445)); // Pool's terminal NUL is an empty name.
        invalid(b -> b.put(445, (byte) 'x'));
        invalid(b -> {
            b.position(b.getInt(52));
            b.put("9999999999".getBytes(StandardCharsets.US_ASCII));
        });
        assertThrows(IOException.class, () -> read(Arrays.copyOf(vector(), 47)));
        assertThrows(IOException.class, () -> read(Arrays.copyOf(vector(), 200)));
    }

    @Test
    public void rejectsUnknownOverlappingUnalignedAndDanglingExtensions() throws Exception {
        invalid(b -> b.putInt(32, 449));
        invalid(b -> b.putInt(32, -1));
        invalid(b -> b.putInt(448, 0));
        invalid(b -> b.putInt(452, 3));
        invalid(b -> b.putInt(456, 2));
        invalid(b -> b.putInt(460, 1));
        invalid(b -> b.putInt(464, 448));
        invalid(b -> b.putInt(468, -1));
        invalid(b -> b.putInt(472, 0));
        invalid(b -> b.putInt(480, 489));
        invalid(b -> b.putInt(484, 7));
        invalid(b -> b.putInt(484, 0));
        invalid(b -> b.putInt(488, b.getInt(492))); // Duplicate v3 name.
        invalid(b -> b.putInt(488, 216)); // A path containing slash cannot be an hwcaps name.
        invalid(b -> b.putLong(48 + 24 + 16, 0x4000000200000002L));
        byte[] extra = Arrays.copyOf(vector(), 552);
        assertThrows(IOException.class, () -> read(extra));
    }

    @Test
    public void rejectsWrongNumericFlagAndNamedHwcapSortOrder() throws Exception {
        invalid(b -> swapRows(b, 0, 1));
        invalid(b -> swapRows(b, 1, 2));
        invalid(b -> b.putInt(48 + 4 * 24, 0x7ff));
    }

    private static void swapRows(ByteBuffer b, int first, int second) {
        byte[] a = new byte[24];
        byte[] c = new byte[24];
        b.position(48 + first * 24).get(a);
        b.position(48 + second * 24).get(c);
        b.position(48 + first * 24).put(c);
        b.position(48 + second * 24).put(a);
    }

    @Test
    public void comparatorAcceptsLeadingZerosButRejectsCIntermediateOverflowWithoutInventingWrap() throws Exception {
        String accepted = "libx.so." + "0".repeat(100) + "2147483599";
        assertEquals(accepted, read(oneName(accepted)).entries().get(0).name().utf8());
        assertEquals("libx.so.0000000000000000001",
                read(oneName("libx.so.0000000000000000001")).entries().get(0).name().utf8());
        var unsupported = assertThrows(IOException.class, () -> read(oneName("libx.so.2147483600")));
        assertTrue(unsupported.getMessage().contains("Unsupported cache comparator"));
        assertThrows(IOException.class, () -> read(oneName("libx.so.2147483647")));
    }

    private static byte[] oneName(String name) {
        byte[] key = name.getBytes(StandardCharsets.US_ASCII);
        byte[] path = "/lib/libx.so.1".getBytes(StandardCharsets.US_ASCII);
        byte[] value = new byte[72 + key.length + path.length + 2];
        ByteBuffer b = bytes(value);
        b.put("glibc-ld.so.cache1.1".getBytes(StandardCharsets.US_ASCII));
        b.putInt(20, 1).putInt(24, value.length - 72).put(28, (byte) 2);
        b.position(48).putInt(0x303).putInt(72).putInt(73 + key.length).putInt(0).putLong(0);
        b.position(72).put(key).put((byte) 0).put(path).put((byte) 0);
        return value;
    }

    @Test
    public void fullSixteenMiBAndEntryCountBoundaryRemainBoundedWithSharedStringReferences() throws Exception {
        var image = read(large(262144));
        assertEquals(262144, image.entries().size());
        assertEquals("libx.so.1", image.entries().get(262143).name().utf8());
        assertThrows(IOException.class, () -> read(large(262145)));
        Path oversized = temporary.newFile().toPath();
        try (FileChannel channel = FileChannel.open(oversized, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            channel.position(16 * 1024 * 1024).write(ByteBuffer.wrap(new byte[]{0}));
            assertThrows(IOException.class,
                    () -> SegmentClassicLinuxCache.read(channel, channel.size(),
                            SegmentClassicLinuxElfTest.deadline()));
        }
    }

    private static byte[] large(int count) {
        byte[] value = new byte[16 * 1024 * 1024];
        ByteBuffer b = bytes(value);
        b.put("glibc-ld.so.cache1.1".getBytes(StandardCharsets.US_ASCII));
        int start = 48 + count * 24;
        b.putInt(20, count).putInt(24, value.length - start).put(28, (byte) 2);
        for (int i = 0; i < count; i++) {
            b.position(48 + i * 24).putInt(0x303).putInt(start).putInt(start + 10).putInt(0).putLong(0);
        }
        b.position(start).put("libx.so.1\0/lib/libx.so.1\0".getBytes(StandardCharsets.US_ASCII));
        return value;
    }

    @Test
    public void actualChannelDeadlineAndZeroProgressNeverCloseOrMoveTheOriginalHandle() throws Exception {
        Path file = temporary.newFile().toPath();
        Files.write(file, vector());
        try (FileChannel actual = FileChannel.open(file, StandardOpenOption.READ)) {
            var channel = spy(new SegmentDataFileLockTest.FaultChannel(actual));
            doReturn(0).when(channel).read(any(ByteBuffer.class), anyLong());
            assertThrows(IOException.class,
                    () -> SegmentClassicLinuxCache.read(channel, 551, SegmentClassicLinuxElfTest.deadline()));
            assertThrows(IOException.class, () -> SegmentClassicLinuxCache.read(actual, 551, System.nanoTime()));
            assertTrue(actual.isOpen());
            assertEquals(0, channel.closes.get());
            assertEquals(0, actual.position());
        }
    }
}

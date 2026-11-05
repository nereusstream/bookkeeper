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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Bounded glibc 2.39 new-cache 1.1 data algorithms; no source qualification, file approval or execution. */
final class SegmentClassicLinuxCache {
    static final int MAX_BYTES = 16 * 1024 * 1024;
    static final int MAX_ENTRIES = 262144;
    private static final byte[] MAGIC = "glibc-ld.so.cache1.1".getBytes(StandardCharsets.US_ASCII);
    private static final int EXTENSION_MAGIC = 0xeaa42174;
    private static final long PLATFORM_MASK = 0xfL << 48;
    private static final List<String> BUILTIN_HWCAPS = List.of("x86-64-v4", "x86-64-v3", "x86-64-v2");

    /** Immutable slice of the one privately read image. No per-reference copy of long shared suffix strings. */
    static final class Text {
        private final byte[] image;
        private final int offset;
        private final int length;

        private Text(byte[] image, int offset, int length) {
            this.image = image;
            this.offset = offset;
            this.length = length;
        }

        byte[] bytes() {
            return Arrays.copyOfRange(image, offset, offset + length);
        }

        int byteLength() {
            return length;
        }

        String utf8() throws IOException {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(image, offset, length)).toString();
        }

        private int at(int index) {
            return index == length ? 0 : image[offset + index];
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Text text && Arrays.mismatch(image, offset, offset + length,
                    text.image, text.offset, text.offset + text.length) == -1;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = offset; i < offset + length; i++) {
                result = 31 * result + image[i];
            }
            return result;
        }
    }

    record Entry(int ordinal, int flags, Text name, Text path, long osVersionUnused, long hwcap,
                 int hwcapIndex, int isaLevel) {
        boolean lp64() {
            return flags == 0x303;
        }
    }

    /** All structurally valid ABI entries retained, in the original lookup order. Winner is a later observation. */
    record Image(int endianFlag, List<Entry> entries, Text generator, List<Text> hwcaps) {
        Image {
            entries = List.copyOf(entries);
            hwcaps = List.copyOf(hwcaps);
        }
    }

    record HashedImage(Image image, SegmentClassicHandoff.Digest sha256) { }

    /** Explicit assumed data, not an observation of the running loader. Empty platform means observed absence. */
    record Capabilities(long dlHwcap, long hwcapMask, Optional<String> platform, long isa1,
                        List<String> activeHwcaps) { }

    enum SelectionState { NO_MATCH, SELECTED, REJECTED_NODEFLIB }

    /** Retains the original row even when its selected path is subsequently excluded. */
    record Selection(SelectionState state, Entry entry) { }

    private record Region(int offset, int size) { }

    private final byte[] data;
    private final ByteBuffer buffer;
    private final long deadline;
    private final int stringStart;
    private final int stringEnd;
    private final BitSet terminators;

    private SegmentClassicLinuxCache(byte[] data, int stringStart, int stringEnd, long deadline) throws IOException {
        this.data = data;
        this.buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.deadline = deadline;
        this.stringStart = stringStart;
        this.stringEnd = stringEnd;
        terminators = new BitSet(stringEnd - stringStart);
        for (int i = stringStart; i < stringEnd; i++) {
            if ((i & 65535) == 0) {
                requireTime(deadline);
            }
            if (data[i] == 0) {
                terminators.set(i - stringStart);
            }
        }
        if (stringEnd > stringStart && data[stringEnd - 1] != 0) {
            throw new IOException("Cache string pool is not terminated within its declared length");
        }
    }

    /** Caller owns original open/close. Positional reads neither map nor move the original channel. */
    static Image read(FileChannel channel, long length, long deadline) throws IOException {
        var result = parseImage(readBytes(channel, length, deadline), deadline);
        if (channel.size() != length) {
            throw new IOException("Original cache length changed during observation");
        }
        requireTime(deadline);
        return result;
    }

    /** Owns SHA-256 and the sole private byte image shared with the parser; no mutable image escapes. */
    static HashedImage readHashed(FileChannel channel, long length, long deadline) throws IOException {
        byte[] bytes = readBytes(channel, length, deadline);
        var image = parseImage(bytes, deadline);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IOException("Required cache SHA-256 is unavailable", unavailable);
        }
        for (int offset = 0; offset < bytes.length; offset += 64 * 1024) {
            requireTime(deadline);
            digest.update(bytes, offset, Math.min(64 * 1024, bytes.length - offset));
        }
        if (channel.size() != length) {
            throw new IOException("Original cache length changed during observation");
        }
        var result = new HashedImage(image, new SegmentClassicHandoff.Digest(digest.digest()));
        requireTime(deadline);
        return result;
    }

    private static byte[] readBytes(FileChannel channel, long length, long deadline) throws IOException {
        requireTime(deadline);
        if (length < 48 || length > MAX_BYTES || channel.size() != length) {
            throw new IOException("Invalid bounded new-cache file length");
        }
        byte[] image = new byte[(int) length];
        ByteBuffer bytes = ByteBuffer.allocate(Math.min(64 * 1024, image.length));
        int offset = 0;
        while (offset < image.length) {
            requireTime(deadline);
            bytes.clear().limit(Math.min(bytes.capacity(), image.length - offset));
            int read = channel.read(bytes, offset);
            if (read <= 0) {
                throw new IOException("Cache original file read ended or made no progress");
            }
            bytes.flip();
            bytes.get(image, offset, read);
            offset += read;
        }
        requireTime(deadline);
        return image;
    }

    private static Image parseImage(byte[] bytes, long deadline) throws IOException {
        try {
            return parse(bytes, deadline);
        } catch (ArithmeticException | IllegalArgumentException malformed) {
            throw new IOException("Invalid or unsupported glibc new-cache structure", malformed);
        }
    }

    private static Image parse(byte[] data, long deadline) throws IOException {
        if (!Arrays.equals(MAGIC, Arrays.copyOf(data, MAGIC.length))) {
            throw new IOException("Only standalone glibc new-cache 1.1 is supported");
        }
        ByteBuffer b = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long count = u32(b, 20);
        long strings = u32(b, 24);
        int endian = Byte.toUnsignedInt(data[28]);
        if (count > MAX_ENTRIES || endian != 0 && endian != 2) {
            throw new IOException("Unknown cache entry count or little-endian/unset marker");
        }
        for (int i = 29; i < 48; i++) {
            if ((i < 32 || i >= 36) && data[i] != 0) {
                throw new IOException("Unknown reserved new-cache header extension");
            }
        }
        int start = Math.toIntExact(48 + count * 24);
        int end = checkedRange(start, strings, data.length);
        var reader = new SegmentClassicLinuxCache(data, start, end, deadline);
        return reader.entries((int) count, endian, u32(b, 32));
    }

    private Image entries(int count, int endian, long extensionOffset) throws IOException {
        List<Text> hwcaps = new ArrayList<>();
        Text generator = null;
        List<Region> regions = new ArrayList<>();
        if (extensionOffset != 0) {
            int offset = Math.toIntExact(extensionOffset);
            if ((offset & 3) != 0 || offset < stringEnd
                    || buffer.getInt(checkedPosition(offset, 8)) != EXTENSION_MAGIC) {
                throw new IOException("Invalid cache extension directory offset or magic");
            }
            long sections = u32(buffer, offset + 4);
            if (sections > 2) {
                throw new IOException("Unknown cache extension set");
            }
            int directoryEnd = checkedRange(offset, 8 + sections * 16, data.length);
            regions.add(new Region(offset, directoryEnd - offset));
            boolean[] seen = new boolean[2];
            for (int i = 0; i < sections; i++) {
                requireTime(deadline);
                int row = offset + 8 + i * 16;
                long tag = u32(buffer, row);
                long flags = u32(buffer, row + 4);
                int sectionOffset = Math.toIntExact(u32(buffer, row + 8));
                int sectionEnd = checkedRange(sectionOffset, u32(buffer, row + 12), data.length);
                if (tag > 1 || flags != 0 || seen[(int) tag] || sectionOffset < stringEnd) {
                    throw new IOException("Unknown, duplicate or overlapping cache extension");
                }
                seen[(int) tag] = true;
                regions.add(new Region(sectionOffset, sectionEnd - sectionOffset));
                if (tag == 0) {
                    if (sectionEnd - sectionOffset > 4096) {
                        throw new IOException("Cache generator text exceeds its fixed capacity");
                    }
                    generator = new Text(data, sectionOffset, sectionEnd - sectionOffset);
                } else {
                    int size = sectionEnd - sectionOffset;
                    if ((sectionOffset & 3) != 0 || size == 0 || (size & 3) != 0 || size / 4 > MAX_ENTRIES) {
                        throw new IOException("Invalid cache hwcaps extension alignment or size");
                    }
                    for (int p = sectionOffset; p < sectionEnd; p += 4) {
                        requireTime(deadline);
                        Text name = string(u32(buffer, p));
                        if (name.length == 0 || name.length == 1 && name.at(0) == '.'
                                || name.length == 2 && name.at(0) == '.' && name.at(1) == '.') {
                            throw new IOException("Invalid cache hwcaps directory name");
                        }
                        for (int n = 0; n < name.length; n++) {
                            if (name.at(n) == '/') {
                                throw new IOException("Cache hwcaps name is not a single directory component");
                            }
                        }
                        if (!hwcaps.isEmpty() && lexical(hwcaps.get(hwcaps.size() - 1), name) >= 0) {
                            throw new IOException("Cache hwcaps directory names are not strictly sorted");
                        }
                        hwcaps.add(name);
                    }
                }
            }
        }
        regions.sort(Comparator.comparingInt(Region::offset));
        int cursor = stringEnd;
        for (var region : regions) {
            int gap = region.offset() - cursor;
            if (gap < 0 || gap > 3) {
                throw new IOException("Overlapping or unrecognized cache extension data");
            }
            while (cursor < region.offset()) {
                if (data[cursor++] != 0) {
                    throw new IOException("Nonzero cache alignment padding");
                }
            }
            cursor += region.size();
        }
        if (cursor != data.length) {
            throw new IOException("Unclassified trailing cache bytes");
        }
        List<Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            requireTime(deadline);
            int p = 48 + i * 24;
            Text name = string(u32(buffer, p + 4));
            Text path = string(u32(buffer, p + 8));
            if (name.length == 0 || path.length == 0) {
                throw new IOException("Empty cache library name or path");
            }
            validateNumericRuns(name);
            long raw = buffer.getLong(p + 16);
            boolean named = ((raw >>> 32) & ~0x3ffL) == 0x40000000L;
            long index = raw & 0xffff_ffffL;
            if (named && index >= hwcaps.size()) {
                throw new IOException("Cache entry has a dangling hwcaps index");
            }
            var entry = new Entry(i, buffer.getInt(p), name, path, u32(buffer, p + 12), raw,
                    named ? (int) index : -1, named ? (int) ((raw >>> 32) & 0x3ff) : 0);
            if (!entries.isEmpty() && order(entries.get(entries.size() - 1), entry, hwcaps) > 0) {
                throw new IOException("Cache entries do not follow the approved lookup order");
            }
            entries.add(entry);
        }
        return new Image(endian, entries, generator, hwcaps);
    }

    private int checkedPosition(int offset, int size) throws IOException {
        checkedRange(offset, size, data.length);
        return offset;
    }

    private Text string(long offset) throws IOException {
        if (offset < stringStart || offset >= stringEnd) {
            throw new IOException("Cache string reference is outside its declared pool");
        }
        int start = (int) offset;
        int end = terminators.nextSetBit(start - stringStart) + stringStart;
        if (end < start || end - start > 4096) {
            throw new IOException("Cache string reference lacks a bounded terminator");
        }
        return new Text(data, start, end - start);
    }

    private static int order(Entry left, Entry right, List<Text> hwcaps) {
        int comparison = libraryCompare(left.name(), right.name());
        if (comparison != 0) {
            return -comparison;
        }
        comparison = Integer.compare(right.flags(), left.flags());
        if (comparison != 0) {
            return comparison;
        }
        if ((left.hwcapIndex() >= 0) != (right.hwcapIndex() >= 0)) {
            return left.hwcapIndex() >= 0 ? -1 : 1;
        }
        return left.hwcapIndex() >= 0 ? lexical(hwcaps.get(left.hwcapIndex()), hwcaps.get(right.hwcapIndex())) : 0;
    }

    private static void validateNumericRuns(Text text) throws IOException {
        long number = 0;
        for (int i = 0; i < text.length; i++) {
            int c = text.at(i);
            if (digit(c)) {
                long product = number * 10;
                if (product > Integer.MAX_VALUE || product + c > Integer.MAX_VALUE) {
                    throw new IOException("Unsupported cache comparator: signed-C intermediate numeric overflow");
                }
                number = product + c - '0';
            } else {
                number = 0;
            }
        }
    }

    /** Query validation before target I/O; numeric runs share the parser's signed-C overflow boundary. */
    static void validateLookupName(String name, long deadline) throws IOException {
        lookupText(name, deadline);
    }

    private static Text lookupText(String name, long deadline) throws IOException {
        requireTime(deadline);
        if (name == null || name.isEmpty() || name.length() > 4096 || name.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(name)) {
            throw new IOException("Unsupported bounded strict UTF8 cache query name");
        }
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 4096) {
            throw new IOException("Cache query name exceeds its original byte bound");
        }
        var query = new Text(bytes, 0, bytes.length);
        validateNumericRuns(query);
        requireTime(deadline);
        return query;
    }

    /** Numeric-equivalent LP64 rows in original order, without capability filtering or winner selection. */
    static List<Entry> related(Image image, String name, long deadline) throws IOException {
        var query = lookupText(name, deadline);
        if (image.entries().size() > MAX_ENTRIES) {
            throw new IOException("Cache query exceeds the original entry count bound");
        }
        var result = new ArrayList<Entry>();
        for (var entry : image.entries()) {
            requireTime(deadline);
            if (entry.lp64() && libraryCompare(query, entry.name()) == 0) {
                result.add(entry);
            }
        }
        requireTime(deadline);
        return List.copyOf(result);
    }

    /** Pure single-row selection followed by raw compiled-prefix NODEFLIB filtering; never probes a path. */
    static Selection select(Image image, String name, Capabilities capabilities, long requesterFlags1,
                            List<String> systemDirectories, long deadline) throws IOException {
        requireTime(deadline);
        if (capabilities == null || capabilities.platform() == null || capabilities.isa1() < 0
                || capabilities.isa1() > 0xffffffffL || capabilities.activeHwcaps() == null
                || capabilities.activeHwcaps().size() > BUILTIN_HWCAPS.size()) {
            throw new IOException("Missing or unsupported explicit cache capability facts");
        }
        var active = new ArrayList<String>();
        int previous = -1;
        for (String hwcap : capabilities.activeHwcaps()) {
            requireTime(deadline);
            int position = hwcap == null ? -1 : BUILTIN_HWCAPS.indexOf(hwcap);
            if (position <= previous) {
                throw new IOException("Active cache hwcaps must be an ordered subsequence of v4/v3/v2");
            }
            active.add(hwcap);
            previous = position;
        }
        long platform = 0;
        if (capabilities.platform().isPresent()) {
            String value = capabilities.platform().get();
            strictFactBytes(value);
            platform = switch (value) {
                case "haswell" -> 1L << 50;
                case "xeon_phi" -> 1L << 51;
                default -> 0;
            };
        }
        if (systemDirectories == null || systemDirectories.isEmpty() || systemDirectories.size() > 64) {
            throw new IOException("Missing or unsupported explicit compiled system_dirs table");
        }
        var prefixes = new ArrayList<byte[]>();
        for (String directory : systemDirectories) {
            requireTime(deadline);
            byte[] prefix = strictFactBytes(directory);
            if (prefix.length == 0 || prefix[0] != '/' || prefix[prefix.length - 1] != '/') {
                throw new IOException("Compiled system_dirs prefix must be absolute and end in slash");
            }
            prefixes.add(prefix);
        }
        long exclude = ~((capabilities.dlHwcap() & capabilities.hwcapMask()) | PLATFORM_MASK | Long.MIN_VALUE);
        Entry best = null;
        int bestPriority = 0;
        for (var entry : related(image, name, deadline)) {
            requireTime(deadline);
            if (entry.hwcapIndex() >= 0) {
                // glibc uses signed C `1 << index`; index 31 and wider have no supported defined result.
                if (entry.isaLevel() < 0 || entry.isaLevel() >= 31) {
                    throw new IOException("Unsupported reached cache ISA bit index");
                }
                if ((capabilities.isa1() & (1L << entry.isaLevel())) == 0) {
                    continue;
                }
                int priority = activePriority(image.hwcaps().get(entry.hwcapIndex()), active);
                if (priority == 0 || best != null && priority >= bestPriority) {
                    continue;
                }
                best = entry;
                bestPriority = priority;
            } else {
                // Named rows precede legacy rows. An existing named best ends the search before legacy checks.
                if (best != null) {
                    break;
                }
                if ((entry.hwcap() & exclude) != 0 || (entry.hwcap() & PLATFORM_MASK) != 0
                        && (entry.hwcap() & PLATFORM_MASK) != platform) {
                    continue;
                }
                best = entry;
                break;
            }
        }
        SelectionState state = best == null ? SelectionState.NO_MATCH : SelectionState.SELECTED;
        if (best != null && (requesterFlags1 & 0x800L) != 0) {
            for (byte[] prefix : prefixes) {
                requireTime(deadline);
                if (startsWith(best.path(), prefix)) {
                    state = SelectionState.REJECTED_NODEFLIB;
                    break;
                }
            }
        }
        requireTime(deadline);
        return new Selection(state, best);
    }

    private static byte[] strictFactBytes(String value) throws IOException {
        if (value == null || value.length() > 4096 || value.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IOException("Unsupported bounded strict UTF8 cache selection fact");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 4096) {
            throw new IOException("Cache selection fact exceeds its original byte bound");
        }
        return bytes;
    }

    private static int activePriority(Text name, List<String> active) {
        for (int i = 0; i < active.size(); i++) {
            String candidate = active.get(i);
            if (name.length != candidate.length()) {
                continue;
            }
            int p = 0;
            while (p < name.length && name.at(p) == candidate.charAt(p)) {
                p++;
            }
            if (p == name.length) {
                return i + 1;
            }
        }
        return 0;
    }

    private static boolean startsWith(Text text, byte[] prefix) {
        if (text.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (text.at(i) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static int libraryCompare(Text left, Text right) {
        int a = 0;
        int b = 0;
        while (left.at(a) != 0) {
            int x = left.at(a);
            int y = right.at(b);
            if (digit(x) && digit(y)) {
                int first = 0;
                int second = 0;
                while (digit(left.at(a))) {
                    first = first * 10 + left.at(a++) - '0';
                }
                while (digit(right.at(b))) {
                    second = second * 10 + right.at(b++) - '0';
                }
                if (first != second) {
                    return Integer.compare(first, second);
                }
            } else if (digit(x) != digit(y)) {
                return digit(x) ? 1 : -1;
            } else if (x != y) {
                return x - y;
            } else {
                a++;
                b++;
            }
        }
        return -right.at(b);
    }

    private static boolean digit(int c) {
        return c >= '0' && c <= '9';
    }

    private static int lexical(Text left, Text right) {
        int common = Math.min(left.length, right.length);
        for (int i = 0; i < common; i++) {
            int result = Integer.compare(left.at(i) & 255, right.at(i) & 255);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static long u32(ByteBuffer buffer, int offset) {
        return Integer.toUnsignedLong(buffer.getInt(offset));
    }

    private static int checkedRange(long offset, long size, int length) throws IOException {
        if (offset < 0 || size < 0 || offset > length || size > length - offset) {
            throw new IOException("Cache byte range is outside the original bounded file");
        }
        return (int) (offset + size);
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Original cache observation deadline elapsed");
        }
    }
}

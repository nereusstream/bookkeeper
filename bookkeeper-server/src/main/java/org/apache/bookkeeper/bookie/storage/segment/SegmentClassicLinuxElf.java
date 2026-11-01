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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;

/** Bounded ELF64 little-endian x86-64 loading metadata, read from an original owner's still-open file. */
final class SegmentClassicLinuxElf {
    private static final int MAX_PROGRAM_HEADERS = 128;
    private static final int MAX_DYNAMIC_ENTRIES = 4096;
    private static final int MAX_TEXT_BYTES = 1024 * 1024;
    private static final Set<Long> GNU_TAGS = Set.of(0x6ffffef5L, 0x6ffffef6L, 0x6ffffef7L, 0x6ffffff0L,
            0x6ffffff9L, 0x6ffffffaL, 0x6ffffffbL, 0x6ffffffcL, 0x6ffffffdL, 0x6ffffffeL, 0x6fffffffL);
    private static final Set<Long> PROGRAM_TYPES = Set.of(0L, 1L, 2L, 3L, 4L, 6L, 7L,
            0x6474e550L, 0x6474e551L, 0x6474e552L, 0x6474e553L);

    /** File observations only. These values prove neither dependency selection nor a running/isolated process. */
    record Image(int type, long entry, String interpreter, String soname, List<String> needed,
                 List<Path> rpath, List<Path> runpath, long flags, long flags1) {
        Image {
            needed = List.copyOf(needed);
            rpath = List.copyOf(rpath);
            runpath = List.copyOf(runpath);
        }
    }

    /** Raw metadata only. Null interpreter/SONAME denotes absence; an empty SONAME remains present. */
    record Image2(int type, long entry, String interpreter, String soname, List<String> needed,
                  DynamicPath rpath, DynamicPath runpath, boolean flagsPresent, long flags,
                  boolean flags1Present, long flags1) {
        Image2 {
            needed = List.copyOf(needed);
        }
    }

    private record Segment(long type, long flags, long offset, long address, long fileBytes, long memoryBytes) { }

    private final FileChannel channel;
    private final long length;
    private final long deadline;
    private int retainedText;

    private SegmentClassicLinuxElf(FileChannel channel, long length, long deadline) {
        this.channel = channel;
        this.length = length;
        this.deadline = deadline;
    }

    /** No open/close, mapping, native load, executable/helper invocation, search or approval fallback. */
    static Image read(FileChannel channel, long length, long deadline) throws IOException {
        try {
            var parser = new SegmentClassicLinuxElf(channel, length, deadline);
            Image2 raw = parser.read();
            if ((raw.flags() & ~0x1aL) != 0 || (raw.flags1() & ~0x08000009L) != 0) {
                throw new IOException("ELF origin, search, audit or unsupported dynamic flags are not supported");
            }
            if (raw.interpreter() != null) {
                canonicalPath(raw.interpreter());
            }
            Set<String> unique = new HashSet<>();
            for (String name : raw.needed()) {
                soname(name);
                if (!unique.add(name)) {
                    throw new IOException("Duplicate ELF dependency name");
                }
            }
            if (raw.soname() != null) {
                soname(raw.soname());
            }
            var result = new Image(raw.type(), raw.entry(), raw.interpreter(), raw.soname(), raw.needed(),
                    raw.rpath().present() ? search(raw.rpath().value()) : List.of(),
                    raw.runpath().present() ? search(raw.runpath().value()) : List.of(), raw.flags(), raw.flags1());
            parser.requireTime();
            return result;
        } catch (ArithmeticException | IllegalArgumentException malformed) {
            throw new IOException("Unsupported or invalid bounded Linux ELF loading metadata", malformed);
        }
    }

    /** Same structural reader, without path conversion, token expansion, lookup, open/close or channel movement. */
    static Image2 readVersionTwo(FileChannel channel, long length, long deadline) throws IOException {
        try {
            var parser = new SegmentClassicLinuxElf(channel, length, deadline);
            Image2 result = parser.read();
            parser.requireTime();
            return result;
        } catch (ArithmeticException | IllegalArgumentException malformed) {
            throw new IOException("Unsupported or invalid bounded Linux ELF two metadata", malformed);
        }
    }

    private Image2 read() throws IOException {
        ByteBuffer header = read(0, 64);
        if (header.getInt(0) != 0x464c457f || header.get(4) != 2 || header.get(5) != 1 || header.get(6) != 1
                || header.get(7) != 0 && header.get(7) != 3 || header.get(8) != 0
                || Short.toUnsignedInt(header.getShort(18)) != 62 || header.getInt(20) != 1
                || header.getInt(48) != 0 || Short.toUnsignedInt(header.getShort(52)) != 64
                || Short.toUnsignedInt(header.getShort(54)) != 56) {
            throw new IOException("Only the fixed ELF64 little-endian Linux x86-64 format is supported");
        }
        for (int i = 9; i < 16; i++) {
            if (header.get(i) != 0) {
                throw new IOException("Unknown ELF identification extension");
            }
        }
        int type = Short.toUnsignedInt(header.getShort(16));
        long entry = header.getLong(24);
        long phoff = header.getLong(32);
        int count = Short.toUnsignedInt(header.getShort(56));
        if (type != 2 && type != 3 || entry < 0 || count < 1 || count > MAX_PROGRAM_HEADERS || phoff < 64) {
            throw new IOException("Unsupported ELF type, entry or program header table");
        }
        range(phoff, Math.multiplyExact((long) count, 56), length);
        List<Segment> loads = new ArrayList<>();
        Segment dynamic = null;
        String interpreter = null;
        long previousLoad = -1;
        for (int i = 0; i < count; i++) {
            ByteBuffer ph = read(Math.addExact(phoff, (long) i * 56), 56);
            var segment = new Segment(Integer.toUnsignedLong(ph.getInt()), Integer.toUnsignedLong(ph.getInt()),
                    ph.getLong(), ph.getLong(), ph.getLong(32), ph.getLong(40));
            if (!PROGRAM_TYPES.contains(segment.type())) {
                throw new IOException("Unknown ELF program loading extension");
            }
            if (segment.type() == 0) {
                continue; // PT_NULL has no interpreted fields.
            }
            range(segment.offset(), segment.fileBytes(), length);
            if (segment.type() == 1) {
                long alignment = ph.getLong(48);
                if (segment.address() < 0 || segment.memoryBytes() < segment.fileBytes()
                        || segment.address() < previousLoad || (segment.flags() & ~7L) != 0
                        || alignment < 0 || alignment > 1 && ((alignment & (alignment - 1)) != 0
                        || segment.address() % alignment != segment.offset() % alignment)) {
                    throw new IOException("Invalid ELF load segment bounds, order, flags or alignment");
                }
                Math.addExact(segment.address(), segment.memoryBytes());
                previousLoad = segment.address();
                loads.add(segment);
            } else if (segment.type() == 2) {
                if (dynamic != null || segment.fileBytes() < 16 || segment.fileBytes() % 16 != 0
                        || segment.fileBytes() / 16 > MAX_DYNAMIC_ENTRIES) {
                    throw new IOException("Invalid or duplicate ELF dynamic segment");
                }
                dynamic = segment;
            } else if (segment.type() == 3) {
                if (interpreter != null || !loads.isEmpty()
                        || segment.fileBytes() < 2 || segment.fileBytes() > 4097) {
                    throw new IOException("Invalid or duplicate ELF interpreter segment");
                }
                ByteBuffer bytes = read(segment.offset(), (int) segment.fileBytes());
                if (bytes.get(bytes.limit() - 1) != 0) {
                    throw new IOException("ELF interpreter lacks its exact terminator");
                }
                interpreter = terminated(bytes, bytes.limit() - 1);
            }
        }
        if (loads.isEmpty() || entry != 0 && loads.stream().noneMatch(s -> (s.flags() & 1) != 0
                && entry >= s.address() && entry - s.address() < s.memoryBytes())) {
            throw new IOException("ELF entry is outside its executable load segments");
        }
        if (dynamic == null) {
            return new Image2(type, entry, interpreter, null, List.of(), new DynamicPath(false, ""),
                    new DynamicPath(false, ""), false, 0, false, 0);
        }
        if (virtualOffset(dynamic.address(), dynamic.fileBytes(), loads) != dynamic.offset()) {
            throw new IOException("ELF dynamic metadata differs from its loaded file mapping");
        }
        return dynamic(type, entry, interpreter, dynamic, loads);
    }

    private Image2 dynamic(int type, long entry, String interpreter, Segment dynamic, List<Segment> loads)
            throws IOException {
        ByteBuffer entries = read(dynamic.offset(), (int) dynamic.fileBytes());
        Map<Long, Long> values = new HashMap<>();
        List<Long> needed = new ArrayList<>();
        boolean end = false;
        while (entries.hasRemaining()) {
            long tag = entries.getLong();
            long value = entries.getLong();
            if (tag == 0) {
                end = true;
                break;
            }
            if (!(tag >= 1 && tag <= 30 || tag >= 32 && tag <= 37 || GNU_TAGS.contains(tag))
                    || tag == 22) {
                throw new IOException("Unknown/unsupported ELF loading, audit, filter or configuration extension");
            }
            if (tag == 1) {
                if (needed.size() == 1024) {
                    throw new IOException("ELF dependency count exceeds its fixed capacity");
                }
                needed.add(value);
            } else if (values.putIfAbsent(tag, value) != null) {
                throw new IOException("Duplicate ELF dynamic singleton");
            }
        }
        if (!end) {
            throw new IOException("ELF dynamic segment has no bounded terminator");
        }
        long flags = values.getOrDefault(30L, 0L);
        long flags1 = values.getOrDefault(0x6ffffffbL, 0L);
        if ((flags & ~0x1bL) != 0 || (flags1 & ~0x08000889L) != 0) {
            throw new IOException("ELF loading flags exceed the supported ELF two masks");
        }
        boolean strings = !needed.isEmpty() || values.containsKey(14L) || values.containsKey(15L)
                || values.containsKey(29L) || values.containsKey(5L) || values.containsKey(10L);
        if (!strings) {
            return new Image2(type, entry, interpreter, null, List.of(), new DynamicPath(false, ""),
                    new DynamicPath(false, ""), values.containsKey(30L), flags,
                    values.containsKey(0x6ffffffbL), flags1);
        }
        if (!values.containsKey(5L) || !values.containsKey(10L) || values.get(10L) <= 0) {
            throw new IOException("Complete ELF dynamic string mapping is required");
        }
        long size = values.get(10L);
        long offset = virtualOffset(values.get(5L), size, loads);
        List<String> names = new ArrayList<>(needed.size());
        for (long index : needed) {
            names.add(string(offset, size, index));
        }
        String name = values.containsKey(14L) ? string(offset, size, values.get(14L)) : null;
        DynamicPath rpath = dynamicPath(offset, size, values, 15L);
        DynamicPath runpath = dynamicPath(offset, size, values, 29L);
        return new Image2(type, entry, interpreter, name, names, rpath, runpath, values.containsKey(30L), flags,
                values.containsKey(0x6ffffffbL), flags1);
    }

    private DynamicPath dynamicPath(long offset, long size, Map<Long, Long> values, long tag) throws IOException {
        return values.containsKey(tag) ? new DynamicPath(true, string(offset, size, values.get(tag)))
                : new DynamicPath(false, "");
    }

    private long virtualOffset(long address, long bytes, List<Segment> loads) throws IOException {
        if (address < 0 || bytes < 0) {
            throw new IOException("Unsupported ELF virtual address range");
        }
        Long found = null;
        for (Segment load : loads) {
            if (address >= load.address() && address - load.address() <= load.fileBytes()
                    && bytes <= load.fileBytes() - (address - load.address())) {
                long candidate = Math.addExact(load.offset(), address - load.address());
                if (found != null) {
                    throw new IOException("Ambiguous ELF file-backed virtual mapping");
                }
                range(candidate, bytes, length);
                found = candidate;
            }
        }
        if (found == null) {
            throw new IOException("ELF dynamic bytes are not backed by an approved load segment");
        }
        return found;
    }

    private String string(long offset, long size, long index) throws IOException {
        if (index < 0 || index >= size) {
            throw new IOException("ELF string index is outside its table");
        }
        int count = (int) Math.min(4097, size - index);
        ByteBuffer bytes = read(Math.addExact(offset, index), count);
        int end = 0;
        while (end < count && bytes.get(end) != 0) {
            end++;
        }
        if (end == count || end > 4096) {
            throw new IOException("ELF string exceeds its bounded terminator");
        }
        return terminated(bytes, end);
    }

    private String terminated(ByteBuffer bytes, int end) throws IOException {
        retainedText = Math.addExact(retainedText, end);
        if (retainedText > MAX_TEXT_BYTES) {
            throw new IOException("ELF retained loading text exceeds its fixed capacity");
        }
        bytes.limit(end);
        String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
        if (value.indexOf(0) >= 0) {
            throw new IOException("ELF loading text has an interior terminator");
        }
        return value;
    }

    private static void soname(String name) throws IOException {
        if (name.isEmpty() || name.equals(".") || name.equals("..") || name.length() > 255
                || !name.matches("[A-Za-z0-9_.+-]+")) {
            throw new IOException("Unsupported ELF dependency/SONAME string");
        }
    }

    private static List<Path> search(String value) throws IOException {
        String[] entries = value.split(":", -1);
        if (entries.length > 64) {
            throw new IOException("ELF search directory count exceeds its fixed capacity");
        }
        List<Path> result = new ArrayList<>(entries.length);
        for (String entry : entries) {
            result.add(canonicalPath(entry));
        }
        return List.copyOf(result);
    }

    private static Path canonicalPath(String value) throws IOException {
        if (value.isEmpty() || value.indexOf('$') >= 0) {
            throw new IOException("ELF loading path uses an unsupported empty or token expansion");
        }
        Path path = Path.of(value);
        if (!path.isAbsolute() || !path.normalize().equals(path) || !path.toString().equals(value)
                || path.getParent() == null) {
            throw new IOException("ELF loading path must be an explicit canonical absolute path");
        }
        return path;
    }

    private ByteBuffer read(long offset, int bytes) throws IOException {
        requireTime();
        range(offset, bytes, length);
        ByteBuffer value = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
        while (value.hasRemaining()) {
            requireTime();
            int count = channel.read(value, offset + value.position());
            if (count <= 0) {
                throw new IOException("ELF metadata read ended or made no progress");
            }
        }
        requireTime();
        value.flip();
        return value;
    }

    private static void range(long offset, long bytes, long length) throws IOException {
        if (offset < 0 || bytes < 0 || offset > length || bytes > length - offset) {
            throw new IOException("ELF metadata file range is invalid");
        }
    }

    private void requireTime() throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Original Linux ELF verification deadline elapsed");
        }
    }
}

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Supplied glibc 2.39 diagnostics bytes only; no process success, loader source or cache capabilities implied. */
final class SegmentClassicLinuxDiagnostics {
    static final int MAX_BYTES = SegmentClassicSystemdQuery.MAX_BYTES;
    private static final String SYSTEM_DIRS = "path.system_dirs[";
    private static final Set<String> REQUIRED = Set.of("version.version", "dl_dst_lib", "dl_hwcap",
            "dl_hwcap_important", "dl_hwcap2", "dl_hwcaps_subdirs", "dl_hwcaps_subdirs_active", "dl_platform",
            "dl_string_platform", "x86.cpu_features.isa_1");

    /** Printed facts only. hwcapImportant is not runtime hwcap_mask; built-in active bits are not final capstr. */
    record Fields(String version, String dstLib, long hwcap, long hwcapImportant, long hwcap2,
                  String hwcapsSubdirs, long hwcapsSubdirsActive, Optional<String> platform,
                  long stringPlatform, long isa1, List<String> systemDirectories) {
        Fields {
            systemDirectories = List.copyOf(systemDirectories);
        }
    }

    /** Unknown fields remain opaque in this private raw image. No result can create an owner observation. */
    static final class Parsed {
        private final byte[] raw;
        private final Fields fields;

        private Parsed(byte[] raw, Fields fields) {
            this.raw = raw;
            this.fields = fields;
        }

        byte[] raw() {
            return raw.clone();
        }

        Fields fields() {
            return fields;
        }
    }

    private record Value(boolean string, int start, int end, boolean numericEscape, long number) { }

    private final byte[] raw;
    private final long deadline;
    private int cursor;

    private SegmentClassicLinuxDiagnostics(byte[] raw, long deadline) {
        this.raw = raw;
        this.deadline = deadline;
    }

    static Parsed parse(byte[] supplied, long deadline) throws IOException {
        requireTime(deadline);
        if (supplied == null || supplied.length == 0 || supplied.length > MAX_BYTES) {
            throw new IOException("Invalid bounded diagnostics byte length");
        }
        return new SegmentClassicLinuxDiagnostics(supplied.clone(), deadline).parse();
    }

    private Parsed parse() throws IOException {
        for (byte b : raw) {
            requireTime(deadline);
            if (b != '\n' && (b < 32 || b > 126)) {
                throw new IOException("Diagnostics requires printable ASCII syntax and LF framing");
            }
        }
        var seen = new HashSet<String>();
        var values = new HashMap<String, Value>();
        var directories = new ArrayList<String>();
        while (cursor < raw.length) {
            requireTime(deadline);
            int start = cursor;
            key();
            String key = new String(raw, start, cursor - start, StandardCharsets.US_ASCII);
            expect('=');
            Value value = value();
            expect('\n');
            if (!seen.add(key)) {
                throw new IOException("Duplicate diagnostics field");
            }
            if (REQUIRED.contains(key)) {
                values.put(key, value);
            } else if (key.startsWith(SYSTEM_DIRS) && key.endsWith("]")) {
                String index = key.substring(SYSTEM_DIRS.length(), key.length() - 1);
                if (!index.equals("0x" + Integer.toHexString(directories.size())) || directories.size() == 64) {
                    throw new IOException("Diagnostics system_dirs indices must be contiguous from zero, at most 64");
                }
                String directory = text(value);
                if (!directory.startsWith("/") || !directory.endsWith("/")) {
                    throw new IOException("Diagnostics system_dirs requires absolute slash-terminated prefixes");
                }
                directories.add(directory);
            }
        }
        if (!values.keySet().containsAll(REQUIRED) || directories.isEmpty()) {
            throw new IOException("Missing required printed diagnostics fields");
        }
        String version = text(values.get("version.version"));
        String subdirs = text(values.get("dl_hwcaps_subdirs"));
        long active = number(values, "dl_hwcaps_subdirs_active");
        long isa1 = number(values, "x86.cpu_features.isa_1");
        if (!version.equals("2.39") || !subdirs.equals("x86-64-v4:x86-64-v3:x86-64-v2")
                || active < 0 || active > 7 || isa1 < 0 || isa1 > 0xffffffffL) {
            throw new IOException("Unsupported diagnostics version, built-in hwcaps or ISA data");
        }
        Value platform = values.get("dl_platform");
        Optional<String> platformText;
        if (platform.string()) {
            platformText = Optional.of(text(platform));
        } else if (platform.number() == 0) {
            platformText = Optional.empty();
        } else {
            throw new IOException("Diagnostics platform must be a string or explicit NULL 0x0");
        }
        var fields = new Fields(version, text(values.get("dl_dst_lib")), number(values, "dl_hwcap"),
                number(values, "dl_hwcap_important"), number(values, "dl_hwcap2"), subdirs, active,
                platformText, number(values, "dl_string_platform"), isa1, directories);
        requireTime(deadline);
        return new Parsed(raw, fields);
    }

    private void key() throws IOException {
        while (true) {
            requireTime(deadline);
            if (cursor == raw.length || !labelStart(raw[cursor])) {
                throw new IOException("Invalid diagnostics label");
            }
            cursor++;
            while (cursor < raw.length && (labelStart(raw[cursor]) || digit(raw[cursor]))) {
                requireTime(deadline);
                cursor++;
            }
            if (cursor < raw.length && raw[cursor] == '[') {
                cursor++;
                hex();
                expect(']');
            }
            if (cursor == raw.length || raw[cursor] != '.') {
                return;
            }
            cursor++;
        }
    }

    private Value value() throws IOException {
        if (cursor == raw.length || raw[cursor] != '"') {
            return new Value(false, 0, 0, false, hex());
        }
        int start = ++cursor;
        boolean numericEscape = false;
        while (cursor < raw.length && raw[cursor] != '"') {
            requireTime(deadline);
            if (raw[cursor] == '\n') {
                throw new IOException("Unterminated diagnostics string");
            }
            if (raw[cursor++] == '\\') {
                if (cursor < raw.length && (raw[cursor] == '\\' || raw[cursor] == '"')) {
                    cursor++;
                } else {
                    numericEscape = true;
                    for (int i = 0; i < 3; i++) {
                        if (cursor == raw.length || raw[cursor] < '0' || raw[cursor] > '7') {
                            throw new IOException("Invalid diagnostics numeric escape");
                        }
                        cursor++;
                    }
                }
            }
        }
        int end = cursor;
        expect('"');
        return new Value(true, start, end, numericEscape, 0);
    }

    /** Canonical lower-case hex prevents duplicate index spellings, without signed overflow or truncation. */
    private long hex() throws IOException {
        expect('0');
        expect('x');
        int start = cursor;
        long value = 0;
        while (cursor < raw.length && hexDigit(raw[cursor]) >= 0) {
            requireTime(deadline);
            if (cursor - start == 16 || cursor > start && raw[start] == '0') {
                throw new IOException("Overflow or noncanonical diagnostics hexadecimal integer");
            }
            value = value << 4 | hexDigit(raw[cursor++]);
        }
        if (cursor == start) {
            throw new IOException("Empty diagnostics hexadecimal integer");
        }
        return value;
    }

    private String text(Value value) throws IOException {
        if (!value.string() || value.numericEscape()) {
            throw new IOException("Unsupported diagnostics string type or irreversible glibc2.39 numeric escape");
        }
        var result = new StringBuilder();
        for (int i = value.start(); i < value.end(); i++) {
            requireTime(deadline);
            if (result.length() == 4096) {
                throw new IOException("Diagnostics consumed string exceeds 4096 bytes");
            }
            if (raw[i] == '\\') {
                i++;
            }
            result.append((char) raw[i]);
        }
        return result.toString();
    }

    private static long number(Map<String, Value> values, String key) throws IOException {
        Value value = values.get(key);
        if (value.string()) {
            throw new IOException("Diagnostics numeric field has string type");
        }
        return value.number();
    }

    private void expect(int expected) throws IOException {
        requireTime(deadline);
        if (cursor == raw.length || raw[cursor++] != expected) {
            throw new IOException("Invalid or incomplete diagnostics syntax");
        }
    }

    private static boolean labelStart(int value) {
        return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_';
    }

    private static boolean digit(int value) {
        return value >= '0' && value <= '9';
    }

    private static int hexDigit(int value) {
        return digit(value) ? value - '0' : value >= 'a' && value <= 'f' ? value - 'a' + 10 : -1;
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Original diagnostics parsing deadline expired");
        }
    }
}

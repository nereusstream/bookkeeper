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
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Bounded glibc 2.39 ldconfig input observations; never loader search order or an include-file approval. */
final class SegmentClassicLinuxConfiguration {
    static final int MAX_BYTES = 1024 * 1024;
    static final int MAX_GRAPH_BYTES = 16 * 1024 * 1024;
    static final int MAX_FILE_EXPANSIONS = 65536;
    static final int MAX_PATTERN_EVALUATIONS = 65536;
    static final int MAX_ACTIVE_CONTEXTS = 32;
    private static final int MAX_TOKEN_BYTES = 4096;

    enum Kind { DIRECTORY, INCLUDE, IGNORED_HWCAP }

    record Usage(int files, int patterns, int bytes) { }

    /** One observation pass. Repeated files/patterns always consume their logical work and complete byte cost. */
    static final class ExpansionBudget {
        private int files;
        private int patterns;
        private int bytes;

        void beginFile(int activeContexts) throws IOException {
            if (activeContexts < 1 || activeContexts > MAX_ACTIVE_CONTEXTS || files == MAX_FILE_EXPANSIONS) {
                throw new IOException("Configuration graph file or active-context capacity exceeded");
            }
            files++;
        }

        void retainFile(long length) throws IOException {
            if (length < 0 || length > MAX_BYTES || length > MAX_GRAPH_BYTES - bytes) {
                throw new IOException("Configuration graph complete file-byte capacity exceeded");
            }
            bytes += (int) length;
        }

        void pattern() throws IOException {
            if (patterns == MAX_PATTERN_EVALUATIONS) {
                throw new IOException("Configuration graph include-pattern capacity exceeded");
            }
            patterns++;
        }

        Usage usage() {
            return new Usage(files, patterns, bytes);
        }
    }

    static final class Unsupported extends IOException {
        private static final long serialVersionUID = 1L;

        private Unsupported(String message) {
            super(message);
        }
    }

    /** Immutable view of the one private image. Comments and ignored directives need not be UTF-8. */
    static final class Text {
        private final byte[] data;
        private final int start;
        private final int end;

        private Text(byte[] data, int start, int end) {
            this.data = data;
            this.start = start;
            this.end = end;
        }

        byte[] bytes() {
            return Arrays.copyOfRange(data, start, end);
        }

        String utf8() throws IOException {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(data, start, end - start)).toString();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Text text
                    && Arrays.mismatch(data, start, end, text.data, text.start, text.end) == -1;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (int i = start; i < end; i++) {
                result = 31 * result + data[i];
            }
            return result;
        }
    }

    /** One-based original line and byte column; include word is zero-based within its original directive. */
    record Directive(Kind kind, int line, int column, int word, Text value, Text sourceLine) { }

    static final class Image {
        private final byte[] data;

        private Image(byte[] data) {
            this.data = data;
        }

        byte[] bytes() {
            return data.clone();
        }

        Cursor cursor(long deadline) {
            return new Cursor(data, deadline);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Image image && Arrays.equals(data, image.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    record HashedImage(Image image, SegmentClassicHandoff.Digest sha256) { }

    /** Sequential facts without a potentially enormous retained object table for very short lines. */
    static final class Cursor {
        private final byte[] data;
        private final long deadline;
        private int position;
        private int line;
        private int lineStart;
        private int lineEnd;
        private int include = -1;
        private int includeEnd;
        private int word;

        private Cursor(byte[] data, long deadline) {
            this.data = data;
            this.deadline = deadline;
        }

        Directive next() throws IOException {
            while (true) {
                requireTime(deadline);
                if (include >= 0) {
                    while (include < includeEnd && blank(data[include])) {
                        include++;
                    }
                    if (include < includeEnd) {
                        int start = include;
                        while (include < includeEnd && !blank(data[include])) {
                            include++;
                        }
                        return directive(Kind.INCLUDE, start, include, word++);
                    }
                    include = -1;
                }
                if (position == data.length) {
                    return null;
                }
                lineStart = position;
                while (position < data.length && data[position] != '\n') {
                    if ((position & 65535) == 0) {
                        requireTime(deadline);
                    }
                    position++;
                }
                int end = position;
                if (position < data.length) {
                    position++;
                }
                lineEnd = position;
                line++;
                int start = lineStart;
                for (int i = start; i < end; i++) {
                    if ((i & 65535) == 0) {
                        requireTime(deadline);
                    }
                    if (data[i] == '#') {
                        end = i;
                        break;
                    }
                }
                while (start < end && space(data[start])) {
                    start++;
                }
                if (start == end) {
                    continue;
                }
                if (prefix(start, end, "include", false)) {
                    include = start + 8;
                    includeEnd = end;
                    word = 0;
                    continue;
                }
                if (prefix(start, end, "hwcap", true)) {
                    return directive(Kind.IGNORED_HWCAP, start, end, 0);
                }
                while (end > start && space(data[end - 1])) {
                    end--;
                }
                while (end > start && data[end - 1] == '/') {
                    end--;
                }
                if (end != start) {
                    return directive(Kind.DIRECTORY, start, end, 0);
                }
            }
        }

        private boolean prefix(int start, int end, String expected, boolean ignoreCase) {
            if (end - start <= expected.length() || !blank(data[start + expected.length()])) {
                return false;
            }
            for (int i = 0; i < expected.length(); i++) {
                int actual = Byte.toUnsignedInt(data[start + i]);
                if (ignoreCase && actual >= 'A' && actual <= 'Z') {
                    actual += 'a' - 'A';
                }
                if (actual != expected.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private Directive directive(Kind kind, int start, int end, int ordinal) throws IOException {
            Text value = new Text(data, start, end);
            if (kind != Kind.IGNORED_HWCAP) {
                if (end - start > MAX_TOKEN_BYTES) {
                    throw new IOException("Configuration path or pattern exceeds 4096 UTF-8 bytes");
                }
                value.utf8();
            }
            requireTime(deadline);
            return new Directive(kind, line, start - lineStart + 1, ordinal, value,
                    new Text(data, lineStart, lineEnd));
        }
    }

    /** Positional read of the caller's original handle; no open, close, mmap or include path lookup. */
    static Image read(FileChannel channel, long length, long deadline) throws IOException {
        var result = parseImage(readBytes(channel, length, deadline), deadline);
        if (channel.size() != length) {
            throw new IOException("Original loader configuration length changed during observation");
        }
        requireTime(deadline);
        return result;
    }

    /** SHA-256 and directives retain one private image; caller channel sees only bounded scratch storage. */
    static HashedImage readHashed(FileChannel channel, long length, long deadline) throws IOException {
        byte[] bytes = readBytes(channel, length, deadline);
        var image = parseImage(bytes, deadline);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IOException("Required configuration SHA-256 is unavailable", unavailable);
        }
        for (int offset = 0; offset < bytes.length; offset += 64 * 1024) {
            requireTime(deadline);
            digest.update(bytes, offset, Math.min(64 * 1024, bytes.length - offset));
        }
        if (channel.size() != length) {
            throw new IOException("Original loader configuration length changed during observation");
        }
        var result = new HashedImage(image, new SegmentClassicHandoff.Digest(digest.digest()));
        requireTime(deadline);
        return result;
    }

    private static byte[] readBytes(FileChannel channel, long length, long deadline) throws IOException {
        requireTime(deadline);
        if (length < 0 || length > MAX_BYTES || channel.size() != length) {
            throw new IOException("Invalid bounded loader configuration length");
        }
        byte[] image = new byte[(int) length];
        ByteBuffer bytes = ByteBuffer.allocate(Math.min(64 * 1024, image.length));
        int offset = 0;
        while (offset < image.length) {
            requireTime(deadline);
            bytes.clear().limit(Math.min(bytes.capacity(), image.length - offset));
            int read = channel.read(bytes, offset);
            if (read <= 0) {
                throw new IOException("Original loader configuration read ended or made no progress");
            }
            bytes.flip();
            bytes.get(image, offset, read);
            offset += read;
        }
        requireTime(deadline);
        return image;
    }

    private static Image parseImage(byte[] bytes, long deadline) throws IOException {
        for (int i = 0; i < bytes.length; i++) {
            if ((i & 65535) == 0) {
                requireTime(deadline);
            }
            if (bytes[i] == 0) {
                throw new Unsupported("NUL anywhere in loader configuration is unsupported at byte " + i);
            }
        }
        Image image = new Image(bytes);
        Cursor cursor = image.cursor(deadline);
        while (cursor.next() != null) {
            // Validate every effective token before publishing the immutable image, retaining no directive table.
        }
        requireTime(deadline);
        return image;
    }

    private static boolean blank(byte value) {
        return value == ' ' || value == '\t';
    }

    private static boolean space(byte value) {
        return blank(value) || value == '\n' || value == '\r' || value == '\f' || value == 11;
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Loader configuration observation deadline elapsed");
        }
    }

    private SegmentClassicLinuxConfiguration() { }
}

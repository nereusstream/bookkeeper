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
import java.util.Objects;

/** Bounded single-string DST data mechanism; no source, secure-mode policy or loader qualification is implied. */
final class SegmentClassicLinuxTokens {
    private static final int MAX_BYTES = 4096;

    enum State { AVAILABLE, UNAVAILABLE, UNOBSERVED }

    /** Replacement or expansion data only. An available empty string is distinct from either blocked state. */
    record Value(State state, String text) {
        Value {
            Objects.requireNonNull(state, "state");
            if ((state == State.AVAILABLE) != (text != null)) {
                throw new IllegalArgumentException("Only available token data can carry text");
            }
        }

        static Value available(String text) {
            return new Value(State.AVAILABLE, text);
        }

        static Value unavailable() {
            return new Value(State.UNAVAILABLE, null);
        }

        static Value unobserved() {
            return new Value(State.UNOBSERVED, null);
        }
    }

    private enum Token { ORIGIN, LIB, PLATFORM }

    private record Match(Token token, int end) { }

    private SegmentClassicLinuxTokens() { }

    /** The same bounded lexer as expand; an unknown or incomplete dollar expression is not a known DST. */
    static boolean hasKnownToken(String raw, long deadline) throws IOException {
        requireTime(deadline);
        utf8Length(raw, deadline);
        for (int cursor = 0; cursor < raw.length(); cursor++) {
            requireTime(deadline);
            if (raw.charAt(cursor) == '$' && match(raw, cursor + 1) != null) {
                requireTime(deadline);
                return true;
            }
        }
        requireTime(deadline);
        return false;
    }

    /** First blocked known token returns its state. Unknown syntax and colons remain literal, never list syntax. */
    static Value expand(String raw, Value origin, Value lib, Value platform, long deadline) throws IOException {
        requireTime(deadline);
        utf8Length(raw, deadline);
        Value[] replacements = {origin, lib, platform};
        int[] lengths = new int[replacements.length];
        for (int i = 0; i < replacements.length; i++) {
            Value replacement = replacements[i];
            if (replacement == null) {
                throw new IOException("Explicit token replacement data is required");
            }
            if (replacement.state() == State.AVAILABLE) {
                lengths[i] = utf8Length(replacement.text(), deadline);
            }
        }
        var output = new StringBuilder(MAX_BYTES);
        int bytes = 0;
        int cursor = 0;
        while (cursor < raw.length()) {
            requireTime(deadline);
            Match match = raw.charAt(cursor) == '$' ? match(raw, cursor + 1) : null;
            if (match != null) {
                int index = match.token().ordinal();
                Value replacement = replacements[index];
                if (replacement.state() != State.AVAILABLE) {
                    requireTime(deadline);
                    return replacement;
                }
                bytes = retain(bytes, lengths[index]);
                output.append(replacement.text());
                cursor = match.end();
            } else {
                int codePoint = raw.codePointAt(cursor);
                bytes = retain(bytes, width(codePoint));
                output.appendCodePoint(codePoint);
                cursor += Character.charCount(codePoint);
            }
        }
        var result = Value.available(output.toString());
        requireTime(deadline);
        return result;
    }

    private static Match match(String raw, int nameStart) {
        if (nameStart == raw.length()) {
            return null;
        }
        boolean braced = raw.charAt(nameStart) == '{';
        if (braced) {
            nameStart++;
        }
        for (Token token : Token.values()) {
            String name = token.name();
            int end = nameStart + name.length();
            if (!raw.startsWith(name, nameStart)) {
                continue;
            }
            if (braced) {
                if (end < raw.length() && raw.charAt(end) == '}') {
                    return new Match(token, end + 1);
                }
            } else if (end == raw.length() || !identifier(raw.charAt(end))) {
                return new Match(token, end);
            }
        }
        return null;
    }

    private static boolean identifier(char value) {
        return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
                || value >= '0' && value <= '9' || value == '_';
    }

    /** Strict bounded text validation only; this does not recognize or substitute dollar expressions. */
    static int utf8Length(String text, long deadline) throws IOException {
        if (text == null || text.length() > MAX_BYTES) {
            throw new IOException("Unsupported bounded UTF8 token text");
        }
        int bytes = 0;
        for (int i = 0; i < text.length(); i++) {
            requireTime(deadline);
            char current = text.charAt(i);
            int codePoint = current;
            if (current == 0 || Character.isLowSurrogate(current)) {
                throw new IOException("NUL or malformed UTF16 cannot be token data");
            }
            if (Character.isHighSurrogate(current)) {
                if (i + 1 == text.length() || !Character.isLowSurrogate(text.charAt(i + 1))) {
                    throw new IOException("Malformed UTF16 cannot be token data");
                }
                codePoint = Character.toCodePoint(current, text.charAt(++i));
            }
            bytes = retain(bytes, width(codePoint));
        }
        requireTime(deadline);
        return bytes;
    }

    private static int width(int codePoint) {
        return codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
    }

    private static int retain(int current, int additional) throws IOException {
        if (additional > MAX_BYTES - current) {
            throw new IOException("Expanded token text exceeds its fixed UTF8 byte bound");
        }
        return current + additional;
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Dynamic token substitution deadline elapsed");
        }
    }
}

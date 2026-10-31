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
import java.util.Arrays;
import java.util.List;

/** Fixed C-locale, flags-zero glibc component matching data only; not filesystem glob expansion or approval. */
final class SegmentClassicLinuxGlob {
    private static final int MAX_BYTES = 4096;
    private static final int CLASS_NAME_BYTES = 2048;
    private final byte[] pattern;

    SegmentClassicLinuxGlob(String component) throws IOException {
        if (component.isEmpty() || component.length() > MAX_BYTES
                || component.indexOf('/') >= 0 || component.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(component)) {
            throw new IOException("Invalid strict UTF-8 include pattern component");
        }
        pattern = component.getBytes(StandardCharsets.UTF_8);
        if (pattern.length > MAX_BYTES) {
            throw new IOException("Include pattern component exceeds its fixed byte limit");
        }
    }

    private record Name(String text, byte[] bytes) { }

    /** Filters the supplied complete member facts, preserving their identity and returning C byte order. */
    static List<String> matchingMembers(List<String> members, String component, long deadline) throws IOException {
        requireTime(deadline);
        var matcher = new SegmentClassicLinuxGlob(component);
        var matched = new ArrayList<Name>();
        long total = 0;
        if (members.size() > 262144) {
            throw new IOException("Include member facts exceed their fixed count limit");
        }
        for (String member : members) {
            requireTime(deadline);
            if (member.isEmpty() || member.length() > MAX_BYTES
                    || member.indexOf('/') >= 0 || member.indexOf(0) >= 0
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(member)) {
                throw new IOException("Invalid strict UTF-8 include member facts");
            }
            byte[] name = member.getBytes(StandardCharsets.UTF_8);
            total += name.length;
            if (name.length > MAX_BYTES || total > 64 * 1024 * 1024) {
                throw new IOException("Include member facts exceed their fixed byte limits");
            }
            if (matcher.matches(name, deadline)) {
                matched.add(new Name(member, name));
            }
        }
        matched.sort((left, right) -> Arrays.compareUnsigned(left.bytes(), right.bytes()));
        List<String> result = matched.stream().map(Name::text).toList();
        requireTime(deadline);
        return result;
    }

    boolean matches(byte[] name, long deadline) throws IOException {
        requireTime(deadline);
        if (name.length > MAX_BYTES) {
            throw new IOException("Include member exceeds its fixed byte limit");
        }
        for (byte value : name) {
            if (value == 0 || value == '/') {
                throw new IOException("Include member cannot contain a path separator or NUL");
            }
        }
        boolean matched = matchBytes(name, deadline);
        requireTime(deadline);
        return matched;
    }

    private boolean matchBytes(byte[] name, long deadline) throws IOException {
        if (name.length != 0 && name[0] == '.' && pattern[0] != '.'
                && !(pattern[0] == '\\' && pattern.length > 1 && pattern[1] == '.')) {
            return false;
        }
        int p = 0;
        int n = 0;
        int star = -1;
        int retry = 0;
        while (n < name.length) {
            requireTime(deadline);
            if (at(p) == '*') {
                star = ++p;
                retry = n;
                continue;
            }
            int next = unit(p, Byte.toUnsignedInt(name[n]));
            if (next >= 0) {
                p = next;
                n++;
            } else if (star >= 0) {
                p = star;
                n = ++retry;
            } else {
                return false;
            }
        }
        while (at(p) == '*') {
            p++;
        }
        requireTime(deadline);
        return p == pattern.length;
    }

    private int unit(int position, int value) {
        int c = at(position);
        if (c == 0) {
            return -1;
        }
        if (c == '?') {
            return position + 1;
        }
        if (c == '\\') {
            return at(position + 1) != 0 && at(position + 1) == value ? position + 2 : -1;
        }
        if (c != '[') {
            return c == value ? position + 1 : -1;
        }
        int p = position + 1;
        boolean inverted = at(p) == '!' || at(p) == '^';
        if (inverted) {
            p++;
        }
        while (true) {
            c = at(p++);
            if (c == 0) {
                return literalBracket(position, value);
            }
            if (c == '[' && at(p) == ':') {
                int end = classEnd(p, CLASS_NAME_BYTES);
                if (end == -2) {
                    return -1;
                }
                if (end >= 0) {
                    String type = new String(pattern, p + 1, end - p - 1, StandardCharsets.US_ASCII);
                    int result = characterClass(type, value);
                    if (result < 0) {
                        return -1;
                    }
                    p = end + 2;
                    if (result != 0) {
                        return matchedBracket(position, value, p, inverted);
                    }
                    if (at(p) == ']') {
                        return inverted ? p + 1 : -1;
                    }
                    continue;
                }
            }
            if (c == '[' && at(p) == '=' && at(p + 1) != 0 && at(p + 2) == '=' && at(p + 3) == ']') {
                int equivalent = at(p + 1);
                p += 4;
                if (equivalent == value) {
                    return matchedBracket(position, value, p, inverted);
                }
                if (at(p) == ']') {
                    return inverted ? p + 1 : -1;
                }
                continue;
            }
            boolean collating = c == '[' && at(p) == '.';
            if (collating) {
                int end = sequenceEnd(p, '.');
                if (end < 0 || end - p != 2) {
                    return -1;
                }
                c = at(p + 1);
                p = end + 2;
            } else if (c == '\\') {
                c = at(p++);
                if (c == 0) {
                    return -1;
                }
            }
            boolean range = at(p) == '-' && at(p + 1) != 0 && (collating || at(p + 1) != ']');
            if (!range && c == value) {
                return matchedBracket(position, value, p, inverted);
            }
            if (range) {
                int upper = at(++p);
                p++;
                if (upper == '[' && at(p) == '.') {
                    int end = sequenceEnd(p, '.');
                    if (end < 0 || end - p != 2) {
                        return -1;
                    }
                    upper = at(p + 1);
                    p = end + 2;
                } else if (upper == '\\') {
                    upper = at(p++);
                }
                if (upper == 0) {
                    return -1;
                }
                if (c <= value && value <= upper) {
                    return matchedBracket(position, value, p, inverted);
                }
            }
            if (at(p) == ']') {
                return inverted ? p + 1 : -1;
            }
        }
    }

    private int matchedBracket(int opening, int value, int position, boolean inverted) {
        int p = position;
        while (true) {
            int c = at(p++);
            if (c == 0) {
                return literalBracket(opening, value);
            }
            if (c == ']') {
                return inverted ? -1 : p;
            }
            if (c == '\\') {
                if (at(p++) == 0) {
                    return -1;
                }
            } else if (c == '[' && at(p) == ':') {
                int end = classEnd(p, CLASS_NAME_BYTES - 1);
                if (end == -2) {
                    return -1;
                }
                if (end >= 0) {
                    p = end + 2;
                }
            } else if (c == '[' && at(p) == '=') {
                if (at(p + 1) == 0 || at(p + 2) != '=' || at(p + 3) != ']') {
                    return -1;
                }
                p += 4;
            } else if (c == '[' && at(p) == '.') {
                int end = sequenceEnd(p, '.');
                if (end < 0) {
                    return -1;
                }
                p = end + 2;
            }
        }
    }

    private int literalBracket(int opening, int value) {
        return value == '[' ? opening + 1 : -1;
    }

    /** Returns the ':' of ':]', -1 for ordinary bracket text, or -2 for a class-name overflow. */
    private int classEnd(int colon, int limit) {
        for (int p = colon + 1; ; p++) {
            if (p - colon - 1 == limit) {
                return -2;
            }
            if (at(p) == ':' && at(p + 1) == ']') {
                return p;
            }
            if (at(p) < 'a' || at(p) >= 'z') {
                return -1;
            }
        }
    }

    private int sequenceEnd(int marker, int delimiter) {
        for (int p = marker + 1; at(p) != 0; p++) {
            if (at(p) == delimiter && at(p + 1) == ']') {
                return p;
            }
        }
        return -1;
    }

    private static int characterClass(String type, int value) {
        boolean alpha = value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z';
        boolean digit = value >= '0' && value <= '9';
        return switch (type) {
            case "alnum" -> alpha || digit ? 1 : 0;
            case "alpha" -> alpha ? 1 : 0;
            case "blank" -> value == ' ' || value == '\t' ? 1 : 0;
            case "cntrl" -> value < 32 || value == 127 ? 1 : 0;
            case "digit" -> digit ? 1 : 0;
            case "graph" -> value >= 33 && value <= 126 ? 1 : 0;
            case "lower" -> value >= 'a' && value <= 'z' ? 1 : 0;
            case "print" -> value >= 32 && value <= 126 ? 1 : 0;
            case "punct" -> value >= 33 && value <= 126 && !alpha && !digit ? 1 : 0;
            case "space" -> value == ' ' || value >= 9 && value <= 13 ? 1 : 0;
            case "upper" -> value >= 'A' && value <= 'Z' ? 1 : 0;
            case "xdigit" -> digit || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F' ? 1 : 0;
            default -> -1;
        };
    }

    private int at(int position) {
        return position >= pattern.length ? 0 : Byte.toUnsignedInt(pattern[position]);
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Include component matching deadline elapsed");
        }
    }
}

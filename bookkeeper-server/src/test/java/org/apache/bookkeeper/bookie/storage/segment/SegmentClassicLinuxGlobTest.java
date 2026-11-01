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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/** Fixed semantic examples and raw bytes, not an actual Linux fnmatch/glob deployment oracle. */
public class SegmentClassicLinuxGlobTest {
    private static boolean match(String pattern, String name) throws IOException {
        return new SegmentClassicLinuxGlob(pattern).matches(name.getBytes(StandardCharsets.UTF_8),
                SegmentClassicLinuxElfTest.deadline());
    }

    @Test
    public void dirnamePlansPreserveMagicLiteralEscapesAndTrailingDirectoryMarks() throws Exception {
        long deadline = SegmentClassicLinuxElfTest.deadline();
        var steps = SegmentClassicLinuxGlob.plan("/etc/[ab]/*.conf", deadline);
        assertEquals(2, steps.size());
        assertEquals(new SegmentClassicLinuxGlob.Step(SegmentClassicLinuxGlob.StepKind.MATCH, "", "*.conf", false),
                steps.get(0));
        assertEquals(new SegmentClassicLinuxGlob.Step(SegmentClassicLinuxGlob.StepKind.DIRECTORY, "/etc", "[ab]", true),
                steps.get(1));
        assertEquals(new SegmentClassicLinuxGlob.Step(SegmentClassicLinuxGlob.StepKind.DIRECTORY, "/etc/a", "b.conf",
                false), SegmentClassicLinuxGlob.plan("/etc/a\\/b.conf", deadline).get(0));
        assertEquals(SegmentClassicLinuxGlob.StepKind.MARK,
                SegmentClassicLinuxGlob.plan("/etc/a*/", deadline).get(0).kind());
        assertEquals("/a//b", SegmentClassicLinuxGlob.logicalDirectory("/a//b///c"));
        assertEquals("//", SegmentClassicLinuxGlob.logicalDirectory("//file"));
        assertEquals("/", SegmentClassicLinuxGlob.logicalDirectory("///file"));
        assertEquals("/a", SegmentClassicLinuxGlob.logicalDirectory("/a/.."));
        assertEquals("/a//b", SegmentClassicLinuxGlob.logicalJoin("/a/", "b"));
        assertEquals(2048, SegmentClassicLinuxGlob.plan("/" + "*/".repeat(2047) + "x", deadline).size());
        assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.plan("relative", deadline));
        assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.plan("/a", System.nanoTime() - 1));
    }

    @Test
    public void ordinaryWildcardsEscapesAndHiddenNamesUseByteSemantics() throws Exception {
        for (String[] pair : List.of(new String[]{"*.conf", "one.conf"}, new String[]{"a**?c", "abbc"},
                new String[]{"??", "é"}, new String[]{"é", "é"}, new String[]{"\\*", "*"},
                new String[]{"\\?", "?"}, new String[]{".*", ".hidden"}, new String[]{".*", "."},
                new String[]{".*", ".."}, new String[]{"\\.*", ".hidden"}, new String[]{"*", ""},
                new String[]{"{a,b}", "{a,b}"}, new String[]{"~*", "~user"}, new String[]{"@(a)", "@(a)"})) {
            assertTrue(List.of(pair).toString(), match(pair[0], pair[1]));
        }
        for (String[] pair : List.of(new String[]{"?", "é"}, new String[]{"*", ".hidden"},
                new String[]{"?hidden", ".hidden"}, new String[]{"[.]hidden", ".hidden"},
                new String[]{"a*", "A"}, new String[]{"*?", ""}, new String[]{"a\\", "a"},
                new String[]{"{a,b}", "a"}, new String[]{"@(a)", "a"})) {
            assertFalse(List.of(pair).toString(), match(pair[0], pair[1]));
        }
        assertTrue(new SegmentClassicLinuxGlob("?").matches(new byte[]{(byte) 255},
                SegmentClassicLinuxElfTest.deadline()));
    }

    @Test
    public void bracketsPreserveRangesNegationEscapesAndTheLiteralUnterminatedFallback() throws Exception {
        for (String[] pair : List.of(new String[]{"[abc]", "b"}, new String[]{"[!a]", "b"},
                new String[]{"[^a]", "b"}, new String[]{"[]a]", "]"}, new String[]{"[]a]", "a"},
                new String[]{"[-a]", "-"}, new String[]{"[a-]", "-"}, new String[]{"[a-c]", "b"},
                new String[]{"[\\]]", "]"}, new String[]{"[", "["}, new String[]{"[]", "[]"},
                new String[]{"[!]", "[!]"}, new String[]{"[abc", "[abc"},
                new String[]{"[[.a.]-[.c.]]", "b"}, new String[]{"[[=a=]]", "a"})) {
            assertTrue(pair[0] + " / " + pair[1], match(pair[0], pair[1]));
        }
        for (String[] pair : List.of(new String[]{"[abc]", "d"}, new String[]{"[!a]", "a"},
                new String[]{"[^a]", "a"}, new String[]{"[z-a]", "z"}, new String[]{"[abc", "a"},
                new String[]{"[[.ab.]]", "a"}, new String[]{"[[=a=]]", "A"})) {
            assertFalse(pair[0] + " / " + pair[1], match(pair[0], pair[1]));
        }
    }

    @Test
    public void namedClassesAndMatchedPrefixScanningDoNotBecomeJavaCharacterSets() throws Exception {
        String[] names = {"alnum", "alpha", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
                "space", "upper", "xdigit"};
        String[] accepted = {"9", "Q", "\t", "\u007f", "2", "!", "q", " ", "!", "\n", "Q", "F"};
        String[] rejected = {"!", "9", "q", "q", "Q", " ", "Q", "\u007f", "q", "q", "q", "G"};
        for (int i = 0; i < names.length; i++) {
            assertTrue(names[i], match("[[:" + names[i] + ":]]", accepted[i]));
            assertFalse(names[i], match("[[:" + names[i] + ":]]", rejected[i]));
        }
        assertTrue(match("[a[:bogus:]]", "a"));
        assertFalse(match("[a[:bogus:]]", "b"));
        assertFalse(match("[[:bogus:]a]", "a"));
        assertTrue(match("[a[.invalid.]]", "a"));
        assertFalse(match("[a[.invalid.]]", "b"));
        assertFalse(match("[a[=ab=]]", "a"));
        assertTrue(match("[a[:Upper:]]", "a]"));
        assertTrue(match("[a[:" + "a".repeat(2046) + ":]]", "a"));
        assertFalse(match("[a[:" + "a".repeat(2047) + ":]]", "a"));
    }

    @Test
    public void memberFilteringKeepsDuplicatesAndSortsUtf8BytesWithinThisOnePattern() throws Exception {
        var members = List.of("😀", "z", "\ue000", "é", ".hidden", "z");
        var matched = SegmentClassicLinuxGlob.matchingMembers(members, "*", SegmentClassicLinuxElfTest.deadline());
        assertEquals(List.of("z", "z", "é", "\ue000", "😀"), matched);
        assertEquals(members, List.of("😀", "z", "\ue000", "é", ".hidden", "z"));
        assertThrows(UnsupportedOperationException.class, () -> matched.add("new"));
        assertEquals(List.of(".hidden"), SegmentClassicLinuxGlob.matchingMembers(members, ".*",
                SegmentClassicLinuxElfTest.deadline()));
        assertTrue(SegmentClassicLinuxGlob.matchingMembers(List.of(), "*",
                SegmentClassicLinuxElfTest.deadline()).isEmpty());
    }

    @Test
    public void byteCapacityInvalidFactsAndDeadlineFailuresCannotBeNomatch() throws Exception {
        assertTrue(match("é".repeat(2048), "é".repeat(2048)));
        for (String invalid : List.of("", "a/b", "a\0b", "é".repeat(2048) + "x", "\ud800")) {
            assertThrows(IOException.class, () -> new SegmentClassicLinuxGlob(invalid));
        }
        for (String invalid : List.of("", "a/b", "a\0b", "\ud800", "x".repeat(4097))) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.matchingMembers(List.of(invalid), "*",
                    SegmentClassicLinuxElfTest.deadline()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.matchingMembers(
                Collections.nCopies(262145, "x"), "*", SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.matchingMembers(
                Collections.nCopies(16385, "x".repeat(4096)), "nomatch", SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> new SegmentClassicLinuxGlob("*").matches(new byte[]{0},
                SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> match("*", "a/b"));
        assertThrows(IOException.class, () -> SegmentClassicLinuxGlob.matchingMembers(List.of(), "*",
                System.nanoTime() - 1));
        assertThrows(IOException.class, () -> new SegmentClassicLinuxGlob("*").matches(new byte[0],
                System.nanoTime() - 1));
    }
}

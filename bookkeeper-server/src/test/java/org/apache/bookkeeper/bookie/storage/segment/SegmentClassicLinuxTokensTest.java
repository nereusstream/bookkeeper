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

import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;
import org.junit.Test;

/** Single-string pure data contracts, never actual loader execution or source qualification. */
public class SegmentClassicLinuxTokensTest {
    private static long deadline() {
        return SegmentClassicLinuxElfTest.deadline();
    }

    private static String text(String raw) throws IOException {
        var result = SegmentClassicLinuxTokens.expand(raw, Value.available("/app"), Value.available("lib64"),
                Value.available("x86_64"), deadline());
        assertEquals(State.AVAILABLE, result.state());
        return result.text();
    }

    @Test
    public void knownNamesBracesAsciiIdentifierBoundariesAndAdjacentTokensAreExact() throws Exception {
        assertEquals("/app//app/lib64/lib64/x86_64/x86_64",
                text("$ORIGIN/${ORIGIN}/$LIB/${LIB}/$PLATFORM/${PLATFORM}"));
        assertEquals("/appx:lib64-1:x86_64.1", text("${ORIGIN}x:$LIB-1:$PLATFORM.1"));
        assertEquals("$ORIGIN_/$LIB0/$PLATFORMx", text("$ORIGIN_/$LIB0/$PLATFORMx"));
        assertEquals("${ORIGINx}:lib64:x86_640", text("${ORIGINx}:${LIB}:${PLATFORM}0"));
        assertEquals("/appé/lib64雪/x86_64😀", text("$ORIGINé/$LIB雪/$PLATFORM😀"));
        assertEquals("/applib64/appx86_64", text("$ORIGIN$LIB${ORIGIN}$PLATFORM"));
        assertEquals("$lib64:${OTHERlib64}", text("$$LIB:${OTHER$LIB}"));
    }

    @Test
    public void unknownAndIncompleteSyntaxIsLiteralAndReplacementValuesAreNotRescanned() throws Exception {
        String raw = "~/${HOME}/$origin/$LIBRARY/${ORIGIN/$UNKNOWN/$";
        var literal = SegmentClassicLinuxTokens.expand(raw, Value.unobserved(), Value.unavailable(), Value.unobserved(),
                deadline());
        assertEquals(Value.available(raw), literal);
        var result = SegmentClassicLinuxTokens.expand("$ORIGIN:$LIB", Value.available("$LIB:$PLATFORM/../x"),
                Value.available("real-lib"), Value.available("actual-platform"), deadline());
        assertEquals(Value.available("$LIB:$PLATFORM/../x:real-lib"), result);
        assertEquals("/./lib64/../", text("/./$LIB/../"));
        assertEquals("a:lib64;b", text("a:$LIB;b"));
    }

    @Test
    public void onlyEncounteredKnownTokensConsumeStatusAndFirstBlockedStatusWins() throws Exception {
        var firstUnknown = SegmentClassicLinuxTokens.expand("prefix/$ORIGIN/$PLATFORM", Value.unobserved(),
                Value.available("lib64"), Value.unavailable(), deadline());
        assertEquals(State.UNOBSERVED, firstUnknown.state());
        assertNull(firstUnknown.text());
        var firstUnavailable = SegmentClassicLinuxTokens.expand("prefix/$PLATFORM/$ORIGIN", Value.unobserved(),
                Value.available("lib64"), Value.unavailable(), deadline());
        assertEquals(State.UNAVAILABLE, firstUnavailable.state());
        assertNull(firstUnavailable.text());
        assertEquals(Value.available("literal:$HOME"), SegmentClassicLinuxTokens.expand("literal:$HOME",
                Value.unobserved(), Value.unobserved(), Value.unobserved(), deadline()));
        assertEquals(Value.available("lib64"), SegmentClassicLinuxTokens.expand("$LIB", Value.unobserved(),
                Value.available("lib64"), Value.unobserved(), deadline()));
        assertEquals(Value.available("$ORIGIN_suffix"), SegmentClassicLinuxTokens.expand("$ORIGIN_suffix",
                Value.unavailable(), Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void originalEmptyAndAvailableEmptyAreDistinctFromBlockedValues() throws Exception {
        assertEquals(Value.available(""), SegmentClassicLinuxTokens.expand("", Value.unavailable(),
                Value.unobserved(), Value.unavailable(), deadline()));
        assertEquals(Value.available(""), SegmentClassicLinuxTokens.expand("$ORIGIN", Value.available(""),
                Value.unobserved(), Value.unobserved(), deadline()));
        assertEquals(Value.available("pre/post"), SegmentClassicLinuxTokens.expand("pre${ORIGIN}/post",
                Value.available(""), Value.unobserved(), Value.unobserved(), deadline()));
        assertEquals(Value.unavailable(), SegmentClassicLinuxTokens.expand("$ORIGIN", Value.unavailable(),
                Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IllegalArgumentException.class, () -> new Value(State.AVAILABLE, null));
        assertThrows(IllegalArgumentException.class, () -> new Value(State.UNAVAILABLE, ""));
        assertThrows(IllegalArgumentException.class, () -> new Value(State.UNOBSERVED, "text"));
    }

    @Test
    public void rawReplacementAndExpandedUtf8ByteBoundsRejectBeforeTruncation() throws Exception {
        String exact = "雪".repeat(1365) + "a";
        assertEquals(exact, text(exact));
        assertEquals("a".repeat(4096), text("a".repeat(4096)));
        assertEquals("😀".repeat(1024), text("😀".repeat(1024)));
        String origin = "雪".repeat(1365);
        assertEquals(Value.available(origin + "a"), SegmentClassicLinuxTokens.expand("${ORIGIN}a",
                Value.available(origin), Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("${ORIGIN}ab", Value.available(origin),
                Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("$ORIGIN$ORIGIN",
                Value.available("雪".repeat(683)), Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> text("雪".repeat(1366)));
        assertThrows(IOException.class, () -> text("a".repeat(4097)));
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("literal",
                Value.available("😀".repeat(1025)), Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void completeRawAndAllAvailableInputsRejectNulAndMalformedUnicodeEvenBeforeBlockedReturn() {
        var invalid = List.of("a" + (char) 0 + "b", String.valueOf((char) 0xd800), String.valueOf((char) 0xdc00),
                String.valueOf((char) 0xd800) + "a");
        for (String bad : invalid) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("$ORIGIN" + bad,
                    Value.unavailable(), Value.unobserved(), Value.unobserved(), deadline()));
            assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("literal", Value.unavailable(),
                    Value.available(bad), Value.unobserved(), deadline()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand(null, Value.unavailable(),
                Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand("literal", null,
                Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void originalDeadlineRejectsLiteralEmptyAndBothBlockedReturnShapes() {
        for (String raw : List.of("", "literal", "$ORIGIN", "$PLATFORM", "$UNKNOWN")) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.expand(raw, Value.unavailable(),
                    Value.unobserved(), Value.unobserved(), System.nanoTime()));
        }
    }
}

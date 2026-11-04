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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.Directory;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.Expansion;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.Kind;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;
import org.junit.Test;

/** Pure list data semantics, never environment/CWD observation, search selection or actual loader execution. */
public class SegmentClassicLinuxDynamicPathTest {
    private static long deadline() {
        return SegmentClassicLinuxElfTest.deadline();
    }

    private static Expansion expand(String raw) throws IOException {
        return SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, raw), Value.available("/app"),
                Value.available("lib64"), Value.available("x86_64"), deadline());
    }

    private static Directory directory(String dirname) {
        return new Directory(Kind.DIRECTORY, dirname);
    }

    private static Directory cwd() {
        return new Directory(Kind.CWD, "");
    }

    private static SegmentClassicLinuxDynamicPath.LibraryPath library(String raw) throws IOException {
        return SegmentClassicLinuxDynamicPath.expandLibraryPath(Value.available(raw), Value.available("/main"),
                Value.available("lib64"), Value.available("x86_64"), deadline());
    }

    @Test
    public void libraryKnownNullWholeEmptyAndUnobservedKeepDistinctInputsWithoutInventingCwd() throws Exception {
        var knownNull = Value.unavailable();
        var empty = Value.available("");
        var unknown = Value.unobserved();
        for (Value effective : List.of(knownNull, empty, unknown)) {
            var result = SegmentClassicLinuxDynamicPath.expandLibraryPath(effective, Value.unobserved(),
                    Value.unavailable(), Value.unobserved(), deadline());
            assertSame(effective, result.effective());
            assertEquals(effective == unknown ? State.UNOBSERVED : State.AVAILABLE, result.state());
            assertEquals(List.of(), result.directories());
        }
        assertNotEquals(knownNull, empty);
        assertNotEquals(empty, unknown);
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expandLibraryPath(null,
                Value.unobserved(), Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void libraryBothSeparatorsKeepFirstEmptyPositionAndQuoteOrBackslashDoesNotEscapeThem() throws Exception {
        assertEquals(List.of(cwd(), directory("/a/"), directory("/b/")), library(":/a;;/b:").directories());
        assertEquals(List.of(directory("/a/"), cwd(), directory("/b/")), library("/a:;/b;").directories());
        assertEquals(List.of(directory("/a/"), directory("/b/"), cwd()), library("/a;/b;;").directories());
        assertEquals(List.of(cwd()), library(";:").directories());
        assertEquals(List.of(directory("\"a/"), directory("b\"/"), directory("c\\/"), directory("d/")),
                library("\"a;b\":c\\;d").directories());
    }

    @Test
    public void librarySplitsBeforeSubstitutionAndUsesTheGivenInitializationOriginWithoutResplittingIt()
            throws Exception {
        var origin = SegmentClassicLinuxOrigin.compute("/logical:part;semi/root/lib.so", Value.unobserved(),
                deadline());
        var effective = Value.available("$ORIGIN;/tail");
        var result = SegmentClassicLinuxDynamicPath.expandLibraryPath(effective, Value.available(origin.text()),
                Value.unobserved(), Value.unobserved(), deadline());
        assertSame(effective, result.effective());
        assertEquals(List.of(directory("/logical:part;semi/root/"), directory("/tail/")), result.directories());
        assertEquals(List.of(directory("$LIB:/a;雪/"), directory("/actual/")),
                SegmentClassicLinuxDynamicPath.expandLibraryPath(Value.available("$ORIGIN:/actual"),
                        Value.available("$LIB:/a;雪"), Value.available("lib64"), Value.unobserved(), deadline())
                        .directories());
    }

    @Test
    public void libraryNonemptyBlockedOrExpandedEmptyComponentsNeverBecomeCwdOrPublishAnUnknownPrefix()
            throws Exception {
        for (Value origin : List.of(Value.unavailable(), Value.available(""))) {
            var skipped = SegmentClassicLinuxDynamicPath.expandLibraryPath(Value.available("/valid;$ORIGIN;/next"),
                    origin, Value.unobserved(), Value.unobserved(), deadline());
            assertEquals(State.AVAILABLE, skipped.state());
            assertEquals(List.of(directory("/valid/"), directory("/next/")), skipped.directories());
        }
        var unknown = SegmentClassicLinuxDynamicPath.expandLibraryPath(Value.available("/valid;$PLATFORM;$ORIGIN"),
                Value.unavailable(), Value.unobserved(), Value.unobserved(), deadline());
        assertEquals(State.UNOBSERVED, unknown.state());
        assertEquals(List.of(), unknown.directories());
        var firstBlocked = SegmentClassicLinuxDynamicPath.expandLibraryPath(Value.available("$ORIGIN/$PLATFORM;/last"),
                Value.unavailable(), Value.unobserved(), Value.unobserved(), deadline());
        assertEquals(State.AVAILABLE, firstBlocked.state());
        assertEquals(List.of(directory("/last/")), firstBlocked.directories());
    }

    @Test
    public void libraryDirectoryRulesStayExactAndTheOriginalDtEntryStillTreatsSemicolonLiterally() throws Exception {
        var result = library("/a//;/a/;/;..;雪;雪/");
        assertEquals(List.of(directory("/a/"), directory("/"), directory("../"), directory("雪/")),
                result.directories());
        assertThrows(UnsupportedOperationException.class, () -> result.directories().add(cwd()));
        assertEquals(List.of(directory("/a;/b/")), expand("/a;/b").directories());
        assertEquals(List.of(directory("lib64;/tail/")), expand("$LIB;/tail").directories());
    }

    @Test
    public void libraryValidatesRawReplacementAddedSlashAndDeadlineEvenForNullEmptyOrUnknownEffectiveData()
            throws Exception {
        String exact = "/" + "a".repeat(4094) + '/';
        assertEquals(List.of(directory(exact)), library(exact).directories());
        assertEquals(List.of(directory("a".repeat(4095) + '/')), library("a".repeat(4095)).directories());
        assertThrows(IOException.class, () -> library("a".repeat(4096)));
        for (String bad : List.of("a" + (char) 0, String.valueOf((char) 0xd800), "雪".repeat(1366))) {
            assertThrows(IOException.class, () -> library(bad));
            for (Value effective : List.of(Value.unavailable(), Value.available(""), Value.unobserved())) {
                assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expandLibraryPath(effective,
                        Value.unobserved(), Value.available(bad), Value.unobserved(), deadline()));
            }
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expandLibraryPath(
                Value.available("$ORIGIN"), Value.available("a".repeat(4096)), Value.unobserved(), Value.unobserved(),
                deadline()));
        for (Value effective : List.of(Value.unavailable(), Value.available(""), Value.unobserved(),
                Value.available("/literal"), Value.available("$ORIGIN"))) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expandLibraryPath(effective,
                    Value.unobserved(), Value.unavailable(), Value.unobserved(), System.nanoTime()));
        }
    }

    @Test
    public void absentAndPresentEmptyRetainTheirDeclarationWithoutCwd() throws Exception {
        var absent = new DynamicPath(false, "");
        var empty = new DynamicPath(true, "");
        var absentResult = SegmentClassicLinuxDynamicPath.expand(absent, Value.unobserved(), Value.unavailable(),
                Value.unobserved(), deadline());
        var emptyResult = SegmentClassicLinuxDynamicPath.expand(empty, Value.unobserved(), Value.unavailable(),
                Value.unobserved(), deadline());
        assertSame(absent, absentResult.declaration());
        assertSame(empty, emptyResult.declaration());
        assertNotEquals(absentResult, emptyResult);
        assertEquals(State.AVAILABLE, absentResult.state());
        assertEquals(State.AVAILABLE, emptyResult.state());
        assertEquals(List.of(), absentResult.directories());
        assertEquals(List.of(), emptyResult.directories());
        assertEquals(List.of(cwd()), expand(":").directories());
    }

    @Test
    public void leadingAdjacentAndTrailingEmptyComponentsRetainTheFirstCwdPosition() throws Exception {
        assertEquals(List.of(cwd(), directory("a/"), directory("b/")), expand(":a::b:").directories());
        assertEquals(List.of(directory("a/"), cwd(), directory("b/")), expand("a::b:").directories());
        assertEquals(List.of(directory("a/"), cwd()), expand("a:").directories());
        assertEquals(List.of(cwd()), expand(":".repeat(4096)).directories());
    }

    @Test
    public void splittingPrecedesSubstitutionAndSemicolonAndReplacementColonStayLiteral() throws Exception {
        var result = SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "${ORIGIN}:$LIB;/tail:literal;dir"),
                Value.available("left:right;$LIB"), Value.available("lib64"), Value.unobserved(), deadline());
        assertEquals(List.of(directory("left:right;$LIB/"), directory("lib64;/tail/"), directory("literal;dir/")),
                result.directories());
        assertEquals(List.of(directory("$HOME/"), directory("$ORIGIN_suffix/")),
                expand("$HOME:$ORIGIN_suffix").directories());
    }

    @Test
    public void unavailableAndExpandedEmptyAreOmittedWithoutBecomingCwd() throws Exception {
        var declaration = new DynamicPath(true, "$ORIGIN:$LIB:$PLATFORM:/kept");
        var result = SegmentClassicLinuxDynamicPath.expand(declaration, Value.available(""), Value.unavailable(),
                Value.available(""), deadline());
        assertSame(declaration, result.declaration());
        assertEquals(State.AVAILABLE, result.state());
        assertEquals(List.of(directory("/kept/")), result.directories());
        assertEquals(List.of(), SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "$ORIGIN:$LIB"),
                Value.available(""), Value.unavailable(), Value.unobserved(), deadline()).directories());
        assertEquals(List.of(cwd()), SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "$ORIGIN:"),
                Value.available(""), Value.unobserved(), Value.unobserved(), deadline()).directories());
    }

    @Test
    public void unobservedComponentDiscardsTheWholePrefixButUnusedOrUnreachableValuesDoNotBlock() throws Exception {
        var declaration = new DynamicPath(true, ":/prefix:$ORIGIN:$PLATFORM:/suffix");
        var result = SegmentClassicLinuxDynamicPath.expand(declaration, Value.unavailable(), Value.available("lib"),
                Value.unobserved(), deadline());
        assertSame(declaration, result.declaration());
        assertEquals(State.UNOBSERVED, result.state());
        assertEquals(List.of(), result.directories());
        var skipped = SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "$ORIGIN/$PLATFORM:/ok"),
                Value.unavailable(), Value.unobserved(), Value.unobserved(), deadline());
        assertEquals(State.AVAILABLE, skipped.state());
        assertEquals(List.of(directory("/ok/")), skipped.directories());
        assertThrows(UnsupportedOperationException.class, () -> skipped.directories().add(cwd()));
    }

    @Test
    public void trailingSlashAndFirstExactByteDedupPreserveInternalAndRelativeSpelling() throws Exception {
        assertEquals(List.of(directory("/"), directory("/a/"), directory("/a//b/"), directory("./"),
                directory("a/../"), directory("a/"), cwd(), directory("雪/"), directory("é/"),
                directory("e\u0301/")), expand("///:/:/a///:/a/:/a//b//:.:a/..:a/:a::雪:雪/:é:e\u0301").directories());
        assertEquals(List.of(directory("/app/"), directory("/x/")),
                expand("/app///:/x:$ORIGIN/:/x///").directories());
    }

    @Test
    public void utf8BoundIncludesTheAddedSlashAndDoesNotNormalizeBeforeTokenBounds() throws Exception {
        String exact = "雪".repeat(1365) + '/';
        assertEquals(List.of(directory(exact)), expand("雪".repeat(1365)).directories());
        assertEquals(List.of(directory("a".repeat(4095) + '/')), expand("a".repeat(4095)).directories());
        assertEquals(List.of(directory("/")), expand("/".repeat(4096)).directories());
        assertThrows(IOException.class, () -> expand("a".repeat(4096)));
        assertThrows(IOException.class, () -> expand("雪".repeat(1365) + 'a'));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "$ORIGIN"),
                Value.available("a".repeat(4096)), Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, "$ORIGIN/"),
                Value.available("/".repeat(4096)), Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void invalidReplacementDataAndOriginalDeadlineRejectEvenEmptyOrUnobservedPaths() {
        for (String raw : List.of("", ":", "/prefix:$ORIGIN")) {
            for (String bad : List.of("a" + (char) 0, String.valueOf((char) 0xd800), "雪".repeat(1366))) {
                assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, raw),
                        Value.unobserved(), Value.available(bad), Value.unavailable(), deadline()));
            }
            assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(new DynamicPath(true, raw),
                    Value.unobserved(), Value.unavailable(), Value.unobserved(), System.nanoTime()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(new DynamicPath(false, ""),
                null, Value.unobserved(), Value.unobserved(), deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDynamicPath.expand(null, Value.unobserved(),
                Value.unobserved(), Value.unobserved(), deadline()));
    }
}

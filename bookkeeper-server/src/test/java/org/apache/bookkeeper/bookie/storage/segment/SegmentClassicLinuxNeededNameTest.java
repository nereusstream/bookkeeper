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

import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxNeededContextObservation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.NeededRule;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.ObjectData;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.Branch;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.Lookup;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.Secure;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;
import org.junit.Test;

/** Given-data stage decisions, not actual ELF/source, namespace, audit or secure-mode qualification. */
public class SegmentClassicLinuxNeededNameTest {
    private static long deadline() {
        return SegmentClassicLinuxElfTest.deadline();
    }

    private static LinuxNeededContextObservation request(int ordinal, String raw) {
        return new LinuxNeededContextObservation(new NeededRule(2, ordinal, List.of(4, 8)), raw);
    }

    private static Lookup prepare(String raw, Value origin, Value lib, Value platform) throws IOException {
        return SegmentClassicLinuxNeededName.prepare(request(3, raw), origin, lib, platform, Secure.NON_SECURE,
                deadline());
    }

    private static SegmentClassicLinuxNeededName.AfterMiss miss(Lookup first, Value origin, Value lib, Value platform)
            throws IOException {
        var absent = SegmentClassicLinuxLoadedNames.lookup(List.of(), first.name(), deadline());
        return SegmentClassicLinuxNeededName.afterNameMiss(first, absent, origin, lib, platform, deadline());
    }

    @Test
    public void decideNameRejectsFirstUnapprovedSonameRatherThanPrefilteringToALaterApprovedPrimary()
            throws Exception {
        var early = new ObjectData(11, 5, "/early.so", List.of("early"), "libx.so", false, false, false);
        var late = new ObjectData(22, 4, "libx.so", List.of("late"), null, false, false, false);
        var namespace = List.of(early, late);
        var unknown = Value.unobserved();
        var rejected = request(3, "libx.so");
        var failure = assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.decideName(rejected,
                namespace, unknown, unknown, unknown, Secure.NON_SECURE, deadline()));
        assertTrue(failure.getMessage().contains("First loaded-name match"));
        var approved = new LinuxNeededContextObservation(new NeededRule(2, 4, List.of(5)), "libx.so");
        var result = SegmentClassicLinuxNeededName.decideName(approved, namespace, unknown, unknown, unknown,
                Secure.NON_SECURE, deadline());
        assertSame(approved, result.first().request());
        assertEquals(4, result.first().request().declaration().ordinal());
        assertEquals(0, result.reuse().ordinal());
        assertEquals(SegmentClassicLinuxLoadedNames.State.REUSED_SONAME, result.reuse().state());
        var remembered = result.reuse().replacement();
        assertEquals(11L, remembered.identity());
        assertEquals(5, remembered.context());
        assertEquals(List.of("early", "libx.so"), remembered.names());
        assertTrue(remembered.sonameAdded());
        assertNull(result.afterMiss());
        assertSame(early, namespace.get(0));
        assertEquals(List.of("early"), early.names());
        assertFalse(early.sonameAdded());
        assertThrows(UnsupportedOperationException.class, () -> remembered.names().clear());
    }

    @Test
    public void decideNameApprovesTheHitContextNotItsOrdinalAndDoesNotExpandAgainAfterANameHit() throws Exception {
        var unknown = Value.unobserved();
        var request = request(3, "$ORIGIN");
        var decoy = new ObjectData(10, 4, "/unrelated.so", List.of("other"), null, false, false, false);
        var hit = new ObjectData(20, 8, "$PLATFORM/lib.so", List.of("original"), null, false, false, false);
        var result = SegmentClassicLinuxNeededName.decideName(request, List.of(decoy, hit),
                Value.available(hit.name()), unknown, unknown, Secure.NON_SECURE, deadline());
        assertSame(request, result.first().request());
        assertEquals("$ORIGIN", result.first().request().name());
        assertEquals(hit.name(), result.first().name());
        assertEquals(1, result.reuse().ordinal());
        assertEquals(8, result.reuse().replacement().context());
        assertEquals(20L, result.reuse().replacement().identity());
        assertEquals(SegmentClassicLinuxLoadedNames.State.REUSED_NAME, result.reuse().state());
        assertEquals(hit, result.reuse().replacement());
        assertEquals(List.of("original"), hit.names());
        assertNull(result.afterMiss());
    }

    @Test
    public void decideNameKnownEmptyTableMissUsesThisFirstLookupAndTheSameTokenValues() throws Exception {
        var unknown = Value.unobserved();
        var directory = SegmentClassicLinuxNeededName.decideName(request(3, "$ORIGIN"), List.of(),
                Value.available("$LIB"), Value.available("lib64"), unknown, Secure.NON_SECURE, deadline());
        assertEquals(SegmentClassicLinuxLoadedNames.State.NO_REUSE, directory.reuse().state());
        assertEquals(-1, directory.reuse().ordinal());
        assertNull(directory.reuse().replacement());
        assertSame(directory.first(), directory.afterMiss().lookup());
        assertEquals("$LIB", directory.first().name());
        assertEquals(Branch.DIRECTORY_SEARCH, directory.afterMiss().branch());
        assertNull(directory.afterMiss().directPath());
        var libraries = List.of(Value.available("lib64"), Value.unavailable(), unknown);
        for (int i = 0; i < libraries.size(); i++) {
            var direct = SegmentClassicLinuxNeededName.decideName(request(3, "$ORIGIN"), List.of(),
                    Value.available("$LIB/x.so"), libraries.get(i), unknown, Secure.NON_SECURE, deadline());
            assertEquals(SegmentClassicLinuxLoadedNames.State.NO_REUSE, direct.reuse().state());
            assertSame(direct.first(), direct.afterMiss().lookup());
            assertEquals("$LIB/x.so", direct.first().name());
            assertEquals(i == 2 ? Branch.UNOBSERVED : Branch.DIRECT_PATH, direct.afterMiss().branch());
            assertEquals(i == 0 ? "lib64/x.so" : i == 1 ? "" : null, direct.afterMiss().directPath());
        }
    }

    @Test
    public void decideNameFirstStopDoesNotReadNamespaceWhileALookupRequiresAnExplicitTable() throws Exception {
        var unknown = Value.unobserved();
        List<ObjectData> unreadable = new java.util.AbstractList<>() {
            @Override
            public ObjectData get(int index) {
                throw new AssertionError("Stopped first preparation must not read the namespace");
            }

            @Override
            public int size() {
                throw new AssertionError("Stopped first preparation must not inspect the namespace");
            }
        };
        for (Value origin : List.of(Value.unavailable(), unknown)) {
            for (int mode = 0; mode < 2; mode++) {
                var result = SegmentClassicLinuxNeededName.decideName(request(3, "$ORIGIN"),
                        mode == 0 ? null : unreadable, origin, unknown, unknown, Secure.NON_SECURE, deadline());
                assertEquals(origin == unknown ? State.UNOBSERVED : State.SKIPPED_EMPTY, result.first().state());
                assertNull(result.first().name());
                assertNull(result.reuse());
                assertNull(result.afterMiss());
            }
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.decideName(request(3, "plain"), null,
                unknown, unknown, unknown, Secure.NON_SECURE, deadline()));
    }

    @Test
    public void decideNameChecksTheProfileCandidateBoundAndKeepsWholeNamespaceValidation() throws Exception {
        var unknown = Value.unobserved();
        var last = new ObjectData(11, 4095, "plain", List.of("plain"), null, false, false, false);
        var valid = new LinuxNeededContextObservation(new NeededRule(2, 3, List.of(4095)), "plain");
        var result = SegmentClassicLinuxNeededName.decideName(valid, List.of(last), unknown, unknown, unknown,
                Secure.NON_SECURE, deadline());
        assertEquals(4095, result.reuse().replacement().context());
        var invalid = new LinuxNeededContextObservation(new NeededRule(2, 3, List.of(4, 4096)), "plain");
        var approved = new ObjectData(22, 4, "plain", List.of("plain"), null, false, false, false);
        var failure = assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.decideName(invalid,
                List.of(approved), unknown, unknown, unknown, Secure.NON_SECURE, deadline()));
        assertTrue(failure.getMessage().contains("candidate context exceeds"));
        var badLater = new ObjectData(33, 4096, "/later", List.of("later"), null, false, false, false);
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.decideName(request(3, "plain"),
                List.of(approved, badLater), unknown, unknown, unknown, Secure.NON_SECURE, deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.decideName(valid, List.of(last),
                unknown, unknown, unknown, Secure.NON_SECURE, System.nanoTime()));
    }

    @Test
    public void expandedNameDoesNotRewriteOriginalDeclarationOrCollapseRepeatedRawOrdinals() throws Exception {
        for (int ordinal : List.of(3, 4)) {
            var request = request(ordinal, "$ORIGIN/$LIB/libx.so");
            var first = SegmentClassicLinuxNeededName.prepare(request, Value.available("/app"),
                    Value.available("lib64"), Value.unobserved(), Secure.NON_SECURE, deadline());
            assertSame(request, first.request());
            assertEquals(2, first.request().declaration().requester());
            assertEquals(ordinal, first.request().declaration().ordinal());
            assertEquals(List.of(4, 8), first.request().declaration().candidates());
            assertEquals("$ORIGIN/$LIB/libx.so", first.request().name());
            assertEquals(State.LOOKUP_NAME, first.state());
            assertEquals("/app/lib64/libx.so", first.name());
        }
    }

    @Test
    public void firstUnavailableAndAvailableEmptySkipButFirstUnobservedExposesNoUsablePrefix() throws Exception {
        var unavailable = prepare("prefix/$ORIGIN/$PLATFORM", Value.unavailable(), Value.available("lib64"),
                Value.unobserved());
        assertEquals(State.SKIPPED_EMPTY, unavailable.state());
        assertNull(unavailable.name());
        var unknown = prepare("prefix/$PLATFORM/$ORIGIN", Value.unavailable(), Value.available("lib64"),
                Value.unobserved());
        assertEquals(State.UNOBSERVED, unknown.state());
        assertNull(unknown.name());
        var empty = prepare("${ORIGIN}", Value.available(""), Value.unobserved(), Value.unobserved());
        assertEquals(State.SKIPPED_EMPTY, empty.state());
        assertNull(empty.name());
        assertEquals("prefix/", prepare("prefix/$ORIGIN", Value.available(""), Value.unobserved(),
                Value.unobserved()).name());
        var noReuse = new SegmentClassicLinuxLoadedNames.Reuse(SegmentClassicLinuxLoadedNames.State.NO_REUSE, -1,
                null);
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.afterNameMiss(empty, noReuse,
                Value.unobserved(), Value.unobserved(), Value.unobserved(), deadline()));
    }

    @Test
    public void secureModeStopsOnlyRecognizedDstAndUnknownModeNeverDefaultsToNonSecure() throws Exception {
        for (String raw : List.of("$ORIGIN/lib", "${LIB}", "prefix/$PLATFORM")) {
            assertTrue(SegmentClassicLinuxTokens.hasKnownToken(raw, deadline()));
            assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.prepare(request(3, raw),
                    Value.available("/app"), Value.available("lib64"), Value.available("x86_64"), Secure.SECURE,
                    deadline()));
            var unknown = SegmentClassicLinuxNeededName.prepare(request(3, raw), Value.available("/app"),
                    Value.available("lib64"), Value.available("x86_64"), Secure.UNOBSERVED, deadline());
            assertEquals(State.UNOBSERVED, unknown.state());
            assertNull(unknown.name());
        }
        for (String raw : List.of("$ORIGIN_suffix", "${HOME}/x", "${ORIGIN", "$", "plain")) {
            assertFalse(SegmentClassicLinuxTokens.hasKnownToken(raw, deadline()));
            for (Secure secure : List.of(Secure.SECURE, Secure.UNOBSERVED)) {
                var first = SegmentClassicLinuxNeededName.prepare(request(3, raw), Value.unobserved(),
                        Value.unobserved(), Value.unavailable(), secure, deadline());
                assertEquals(State.LOOKUP_NAME, first.state());
                assertEquals(raw, first.name());
            }
        }
    }

    @Test
    public void literalSlashDotUnknownDollarColonAndUnicodeAreNotDirectoriesOrNormalizedPaths() throws Exception {
        String raw = ".././$HOME/雪:part;😀//lib.so";
        var first = prepare(raw, Value.unobserved(), Value.unavailable(), Value.unobserved());
        assertEquals(raw, first.name());
        var direct = miss(first, Value.unobserved(), Value.unavailable(), Value.unobserved());
        assertSame(first, direct.lookup());
        assertEquals(Branch.DIRECT_PATH, direct.branch());
        assertEquals(raw, direct.directPath());
    }

    @Test
    public void twoSinglePassExpansionsStaySeparatedAndFileIdRememberingUsesTheFirstLookupName() throws Exception {
        var origin = Value.available("$LIB/../x");
        var lib = Value.available("lib64");
        var first = prepare("$ORIGIN/lib.so", origin, lib, Value.unobserved());
        assertEquals("$LIB/../x/lib.so", first.name());
        var direct = miss(first, origin, lib, Value.unobserved());
        assertEquals(Branch.DIRECT_PATH, direct.branch());
        assertEquals("lib64/../x/lib.so", direct.directPath());
        assertEquals("$LIB/../x/lib.so", first.name());
        var object = new ObjectData(10, 1, "/old.so", List.of("old"), null, false, false, false);
        var id = new SegmentClassicLinuxLoadedNames.FileId(1, 7);
        var reused = SegmentClassicLinuxLoadedNames.lookupFile(
                List.of(new SegmentClassicLinuxLoadedNames.FileObject(object, id)), id, first.name(), deadline());
        assertEquals(List.of("old", first.name()), reused.replacement().names());
    }

    @Test
    public void nameHitDoesNotNeedSecondStageFactsAndOnlyAnExplicitNameMissCanContinue() throws Exception {
        var origin = Value.available("$PLATFORM/lib.so");
        var first = prepare("$ORIGIN", origin, Value.unobserved(), Value.unobserved());
        assertEquals(State.LOOKUP_NAME, first.state());
        var object = new ObjectData(10, 1, first.name(), List.of("known"), null, false, false, false);
        var hit = SegmentClassicLinuxLoadedNames.lookup(List.of(object), first.name(), deadline());
        assertEquals(SegmentClassicLinuxLoadedNames.State.REUSED_NAME, hit.state());
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.afterNameMiss(first, hit, origin,
                Value.unobserved(), Value.unobserved(), deadline()));
        var unobserved = miss(first, origin, Value.unobserved(), Value.unobserved());
        assertEquals(Branch.UNOBSERVED, unobserved.branch());
        assertNull(unobserved.directPath());
        assertEquals("$PLATFORM/lib.so", unobserved.lookup().name());
        var noSlash = prepare("$ORIGIN", Value.available("$PLATFORM"), Value.unobserved(), Value.unobserved());
        var search = miss(noSlash, Value.unobserved(), Value.unobserved(), Value.unobserved());
        assertEquals(Branch.DIRECTORY_SEARCH, search.branch());
        assertEquals("$PLATFORM", search.lookup().name());
        assertNull(search.directPath());
    }

    @Test
    public void secondUnavailableIsEmptyDirectTextNotASkippedRequestAndAvailableEmptyPreservesLiteralSlash()
            throws Exception {
        var origin = Value.available("$PLATFORM/lib.so");
        var first = prepare("$ORIGIN", origin, Value.unobserved(), Value.unavailable());
        assertEquals(State.LOOKUP_NAME, first.state());
        var direct = miss(first, origin, Value.unobserved(), Value.unavailable());
        assertEquals(Branch.DIRECT_PATH, direct.branch());
        assertEquals("", direct.directPath());
        assertSame(first, direct.lookup());
        assertEquals("$PLATFORM/lib.so", direct.lookup().name());
        var slash = prepare("$ORIGIN", Value.available("$PLATFORM/"), Value.unobserved(), Value.available(""));
        assertEquals("/", miss(slash, Value.unobserved(), Value.unobserved(), Value.available("")).directPath());
    }

    @Test
    public void originalIdentityStrictTextExpansionBoundsAndDeadlineRejectWithoutProducingStageFacts()
            throws Exception {
        var unknown = Value.unobserved();
        for (String raw : List.of("", "a" + (char) 0, String.valueOf((char) 0xd800), "雪".repeat(1366))) {
            assertThrows(IOException.class, () -> prepare(raw, unknown, unknown, unknown));
        }
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.prepare(
                new LinuxNeededContextObservation(new NeededRule(-1, 3, List.of(4)), "plain"), unknown, unknown,
                unknown, Secure.NON_SECURE, deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.prepare(request(-1, "plain"), unknown,
                unknown, unknown, Secure.NON_SECURE, deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.prepare(request(3, "$ORIGIN"), unknown,
                Value.available("bad" + (char) 0), unknown, Secure.UNOBSERVED, deadline()));
        assertEquals("雪".repeat(1365) + "a", prepare("${ORIGIN}a", Value.available("雪".repeat(1365)), unknown,
                unknown).name());
        assertThrows(IOException.class, () -> prepare("${ORIGIN}ab", Value.available("雪".repeat(1365)), unknown,
                unknown));
        var first = prepare("$ORIGIN", Value.available("$LIB/x"), unknown, unknown);
        assertThrows(IOException.class, () -> miss(first, unknown, Value.available("a".repeat(4096)), unknown));
        for (String raw : List.of("plain", "$ORIGIN", "$HOME/x")) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.prepare(request(3, raw), unknown,
                    unknown, unknown, Secure.NON_SECURE, System.nanoTime()));
        }
        var noReuse = SegmentClassicLinuxLoadedNames.lookup(List.of(), first.name(), deadline());
        assertThrows(IOException.class, () -> SegmentClassicLinuxNeededName.afterNameMiss(first, noReuse, unknown,
                unknown, unknown, System.nanoTime()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxTokens.hasKnownToken("$ORIGIN" + (char) 0,
                deadline()));
    }
}

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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.FileId;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.FileObject;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.ObjectData;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.State;
import org.junit.Test;

/** Supplied object order and name-state examples only, not actual namespace or dynamic-loader evidence. */
public class SegmentClassicLinuxLoadedNamesTest {
    private static ObjectData object(long identity, String name, List<String> names, String soname,
                                     boolean added, boolean faked, boolean removed) {
        return new ObjectData(identity, 0, name, names, soname, added, faked, removed);
    }

    private static SegmentClassicLinuxLoadedNames.Reuse lookup(List<ObjectData> objects, String query)
            throws IOException {
        return SegmentClassicLinuxLoadedNames.lookup(objects, query, SegmentClassicLinuxElfTest.deadline());
    }

    private static SegmentClassicLinuxLoadedNames.Reuse file(List<FileObject> objects, FileId id, String query)
            throws IOException {
        return SegmentClassicLinuxLoadedNames.lookupFile(objects, id, query, SegmentClassicLinuxElfTest.deadline());
    }

    @Test
    public void fileIdReuseKeepsFirstOriginalObjectAndSkipsRemovedWithoutSkippingFaked() throws Exception {
        var id = new FileId(9, 21);
        var removed = new FileObject(object(1, "/removed", List.of("removed"), null, false, false, true), id);
        var fake = new FileObject(new ObjectData(8, 27, "/old/path", List.of("old"), "soname", false, true, false), id);
        var later = new FileObject(object(2, "/different/path", List.of("later"), null, false, false, false), id);
        var result = file(List.of(removed, fake, later), id, "/new/$literal");
        assertEquals(State.REUSED_FILE_ID, result.state());
        assertEquals(1, result.ordinal());
        assertEquals(8, result.replacement().identity());
        assertEquals(27, result.replacement().context());
        assertEquals("/old/path", result.replacement().name());
        assertEquals("soname", result.replacement().soname());
        assertEquals(List.of("old", "/new/$literal"), result.replacement().names());
        assertTrue(result.replacement().faked());
        assertFalse(result.replacement().sonameAdded());
    }

    @Test
    public void fileIdentityRequiresBothRaw64BitFieldsAndExplicitZeroIsNotMissing() throws Exception {
        var loaded = object(1, "same-name", List.of("alias"), null, false, false, false);
        for (FileId different : List.of(new FileId(3, 8), new FileId(2, 9))) {
            var result = file(List.of(new FileObject(loaded, new FileId(2, 8))), different, "same-name");
            assertEquals(State.NO_REUSE, result.state());
            assertEquals(-1, result.ordinal());
            assertNull(result.replacement());
        }
        for (FileId raw : List.of(new FileId(0, 0), new FileId(Long.MIN_VALUE, -1))) {
            assertEquals(State.REUSED_FILE_ID, file(List.of(new FileObject(loaded, raw)), raw, "alias").state());
        }
        assertEquals(State.NO_REUSE, file(List.of(), new FileId(0, 0), "name").state());
    }

    @Test
    public void fileHitAppendsOnlyOriginalRequestAliasWithoutUsingMainNameOrSonameAndKeepsFlag() throws Exception {
        var id = new FileId(2, 3);
        for (boolean added : List.of(false, true)) {
            var names = new ArrayList<>(List.of("original"));
            var loaded = object(1, "/main-name", names, "soname", added, false, false);
            for (String query : List.of("/main-name", "soname", "$ORIGIN/new")) {
                var result = file(List.of(new FileObject(loaded, id)), id, query);
                assertEquals(List.of("original", query), result.replacement().names());
                assertEquals(added, result.replacement().sonameAdded());
                assertEquals(State.REUSED_NAME, lookup(List.of(result.replacement()), query).state());
                assertEquals(List.of("original"), loaded.names());
                assertThrows(UnsupportedOperationException.class, () -> result.replacement().names().clear());
            }
            var unchanged = file(List.of(new FileObject(loaded, id)), id, "original");
            assertEquals(loaded, unchanged.replacement());
            names.clear();
            assertEquals(List.of("original"), unchanged.replacement().names());
        }
    }

    @Test
    public void fileIdAndAllLaterDataAreRequiredAndOnlyNewAliasesConsumeCapacity() throws Exception {
        var id = new FileId(1, 2);
        var first = new FileObject(object(1, "main", List.of("alias"), null, false, false, false), id);
        for (FileObject invalid : Arrays.asList(null, new FileObject(first.object(), null),
                new FileObject(null, id), new FileObject(first.object(), id),
                new FileObject(object(2, "bad\0", List.of("alias"), null, false, false, true), id))) {
            assertThrows(IOException.class, () -> file(Arrays.asList(first, invalid), id, "alias"));
        }
        assertThrows(IOException.class, () -> file(List.of(first), null, "alias"));
        assertThrows(IOException.class, () -> file(null, id, "alias"));
        var full = new FileObject(object(1, "main", Collections.nCopies(12288, "alias"), "soname",
                false, false, false), id);
        assertEquals(12288, file(List.of(full), id, "alias").replacement().names().size());
        assertThrows(IOException.class, () -> file(List.of(full), id, "main"));
        assertThrows(IOException.class, () -> file(List.of(full), id, "soname"));
        assertEquals(12288, full.object().names().size());
        assertFalse(full.object().sonameAdded());
        assertThrows(IOException.class,
                () -> SegmentClassicLinuxLoadedNames.lookupFile(List.of(first), id, "alias", System.nanoTime()));
    }

    @Test
    public void earlierSonameBeatsLaterMainNameAndRecordsOneAliasForSubsequentLookup() throws Exception {
        var names = new ArrayList<>(List.of("original"));
        var early = object(8, "/early", names, "libx.so.1", false, false, false);
        var late = object(2, "libx.so.1", List.of("late"), null, false, false, false);
        var objects = new ArrayList<>(List.of(early, late)); // Same context, independent identities and explicit order.
        var first = lookup(objects, "libx.so.1");
        assertEquals(State.REUSED_SONAME, first.state());
        assertEquals(0, first.ordinal());
        assertEquals(8, first.replacement().identity());
        assertEquals(0, first.replacement().context());
        assertEquals(List.of("original", "libx.so.1"), first.replacement().names());
        assertTrue(first.replacement().sonameAdded());
        assertEquals(List.of("original"), early.names());
        assertFalse(early.sonameAdded());
        assertEquals(List.of(early, late), objects);
        objects.set(first.ordinal(), first.replacement());
        var repeated = lookup(objects, "libx.so.1");
        assertEquals(State.REUSED_NAME, repeated.state());
        assertEquals(first.replacement(), repeated.replacement());
        names.clear();
        assertEquals(List.of("original", "libx.so.1"), first.replacement().names());
        assertThrows(UnsupportedOperationException.class, () -> first.replacement().names().clear());
    }

    @Test
    public void skipsFakedAndRemovedAndDoesNotChangeFlagsOnDirectNameOrAliasHits() throws Exception {
        var fake = object(1, "wanted", List.of("wanted"), "wanted", false, true, false);
        var removed = object(2, "wanted", List.of("wanted"), "wanted", false, false, true);
        var live = object(3, "wanted", List.of("alias"), "wanted", false, false, false);
        for (String query : List.of("wanted", "alias")) {
            var result = lookup(List.of(fake, removed, live), query);
            assertEquals(State.REUSED_NAME, result.state());
            assertEquals(2, result.ordinal());
            assertEquals(live, result.replacement());
            assertFalse(result.replacement().sonameAdded());
        }
        var alreadyAliased = object(4, "", List.of("wanted"), "wanted", false, false, false);
        assertFalse(lookup(List.of(alreadyAliased), "wanted").replacement().sonameAdded());
    }

    @Test
    public void matchingIsLiteralWithoutNumericPathTokenUnicodeOrPhysicalEquivalence() throws Exception {
        var first = object(1, "libx.so.01", List.of("/a/../b", "$ORIGIN/x", "é"), null, false, false, false);
        for (String query : List.of("libx.so.01", "/a/../b", "$ORIGIN/x", "é")) {
            assertEquals(State.REUSED_NAME, lookup(List.of(first), query).state());
        }
        for (String query : List.of("libx.so.1", "/b", "/expanded/x", "e\u0301", "absent")) {
            var missing = lookup(List.of(first), query);
            assertEquals(State.NO_REUSE, missing.state());
            assertEquals(-1, missing.ordinal());
            assertNull(missing.replacement());
        }
        assertEquals(State.NO_REUSE, lookup(List.of(), "libx.so.1").state());
    }

    @Test
    public void sonameAddedAbsentAndEmptyAreNotRepairedOrConflated() throws Exception {
        for (ObjectData object : List.of(object(1, "", List.of(""), null, false, false, false),
                object(2, "", List.of(""), "", false, false, false),
                object(3, "", List.of(""), "wanted", true, false, false))) {
            assertEquals(State.NO_REUSE, lookup(List.of(object), "wanted").state());
        }
        var presentEmpty = object(4, "main", List.of(""), "", false, false, false);
        assertEquals("", lookup(List.of(presentEmpty), "main").replacement().soname());
        var absent = object(5, "main", List.of(""), null, false, false, false);
        assertNull(lookup(List.of(absent), "main").replacement().soname());
    }

    @Test
    public void fullTableValidationRejectsBadLaterDataEvenAfterAnEarlierMatch() throws Exception {
        var early = object(1, "wanted", List.of("a"), null, false, false, false);
        for (ObjectData invalid : Arrays.asList(null, object(1, "duplicate", List.of("a"), null, false, true, true),
                object(2, null, List.of("a"), null, false, false, false),
                object(2, "", null, null, false, false, false), object(2, "", List.of(), null, false, false, false),
                object(2, "", Arrays.asList((String) null), null, false, false, false),
                object(2, "bad\0", List.of("a"), null, false, false, false),
                object(2, "", List.of("\ud800"), null, false, false, false),
                object(2, "", List.of("a"), "雪".repeat(1366), false, true, true),
                new ObjectData(2, -1, "", List.of("a"), null, false, false, false),
                new ObjectData(2, 4096, "", List.of("a"), null, false, false, false))) {
            assertThrows(IOException.class, () -> lookup(Arrays.asList(early, invalid), "wanted"));
        }
        assertThrows(IOException.class, () -> lookup(null, "wanted"));
        for (String query : Arrays.asList(null, "", "bad\0", "\ud800", "x".repeat(4097))) {
            assertThrows(IOException.class, () -> lookup(List.of(early), query));
        }
    }

    @Test
    public void objectAssociationUpdateStringAndDeadlineBoundsApplyWithoutMutatingInputs() throws Exception {
        var objects = new ArrayList<ObjectData>();
        for (int i = 0; i < 4096; i++) {
            objects.add(object(i, "", List.of("alias"), null, false, false, false));
        }
        assertEquals(State.NO_REUSE, lookup(objects, "absent").state());
        objects.add(object(4096, "", List.of("alias"), null, false, false, false));
        assertThrows(IOException.class, () -> lookup(objects, "alias"));
        var full = object(1, "", Collections.nCopies(12288, "alias"), "soname", false, false, false);
        assertEquals(State.REUSED_NAME, lookup(List.of(full), "alias").state());
        assertEquals(State.NO_REUSE, lookup(List.of(full), "absent").state());
        assertThrows(IOException.class, () -> lookup(List.of(full), "soname"));
        assertThrows(IOException.class, () -> lookup(List.of(
                object(2, "wanted", List.of("a"), null, false, false, false), full), "wanted"));
        assertFalse(full.sonameAdded());
        assertEquals(12288, full.names().size());
        var room = object(1, "", Collections.nCopies(12287, "alias"), "soname", false, false, false);
        assertEquals(12288, lookup(List.of(room), "soname").replacement().names().size());
        var tooMany = object(1, "wanted", Collections.nCopies(12289, "alias"), null, false, false, false);
        assertThrows(IOException.class, () -> lookup(List.of(tooMany), "wanted"));
        String maximum = "雪".repeat(1365) + "x";
        assertEquals(State.REUSED_NAME, lookup(List.of(object(1, maximum, List.of(""), null, false, false, false)),
                maximum).state());
        assertThrows(IOException.class,
                () -> SegmentClassicLinuxLoadedNames.lookup(List.of(), "wanted", System.nanoTime()));
    }
}

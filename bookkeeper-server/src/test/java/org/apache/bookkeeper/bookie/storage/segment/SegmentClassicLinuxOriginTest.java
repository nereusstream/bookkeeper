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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxNeededContextObservation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.NeededRule;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxOrigin.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;
import org.junit.Test;

/** Supplied realname/CWD strings only, not actual object creation, getcwd, main-origin or loader evidence. */
public class SegmentClassicLinuxOriginTest {
    private static long deadline() {
        return SegmentClassicLinuxElfTest.deadline();
    }

    private static SegmentClassicLinuxOrigin.Origin compute(String realname, Value cwd) throws IOException {
        return SegmentClassicLinuxOrigin.compute(realname, cwd, deadline());
    }

    @Test
    public void absoluteRealnameKeepsLiteralTextAndDoesNotNeedAnObservedOrAvailableCwd() throws Exception {
        var cases = Map.of("/a/../b/lib.so", "/a/../b", "/a//lib.so", "/a/", "/a//", "/a/",
                "/lib.so", "/", "/", "/", "///lib.so", "//", "/$LIB/雪:part;😀/x.so", "/$LIB/雪:part;😀");
        for (var entry : cases.entrySet()) {
            for (Value cwd : List.of(Value.unobserved(), Value.unavailable())) {
                var origin = compute(entry.getKey(), cwd);
                assertSame(entry.getKey(), origin.realname());
                assertSame(cwd, origin.cwd());
                assertEquals(State.AVAILABLE, origin.state());
                assertEquals(entry.getValue(), origin.text());
            }
        }
    }

    @Test
    public void relativeRealnameJoinsOnlyAvailableCwdAndPreservesDotsRepeatedAndExistingSlashes() throws Exception {
        for (var entry : Map.of("../lib.so", "/opt/app/..", "./lib.so", "/opt/app/.", "lib.so", "/opt/app",
                "a//lib.so", "/opt/app/a/", "$ORIGIN/雪.so", "/opt/app/$ORIGIN").entrySet()) {
            assertEquals(entry.getValue(), compute(entry.getKey(), Value.available("/opt/app")).text());
        }
        assertEquals("/", compute("lib.so", Value.available("/")).text());
        assertEquals("/opt/app", compute("lib.so", Value.available("/opt/app/")).text());
        assertEquals("/opt/app/", compute("lib.so", Value.available("/opt/app//")).text());
        var unavailable = compute("prefix/lib.so", Value.unavailable());
        assertEquals(State.UNAVAILABLE, unavailable.state());
        assertNull(unavailable.text());
        var unobserved = compute("prefix/lib.so", Value.unobserved());
        assertEquals(State.UNOBSERVED, unobserved.state());
        assertNull(unobserved.text());
    }

    @Test
    public void emptyRealnameDefersGetOriginWithoutUsingCwdOrInventingKnownUnavailability() throws Exception {
        for (Value cwd : List.of(Value.available("/plausible/physical/parent"), Value.unavailable(),
                Value.unobserved())) {
            var deferred = compute("", cwd);
            assertEquals("", deferred.realname());
            assertSame(cwd, deferred.cwd());
            assertEquals(State.DEFERRED_GET_ORIGIN, deferred.state());
            assertNull(deferred.text());
        }
        assertEquals(State.AVAILABLE, compute("/explicit/main", Value.unobserved()).state());
        assertEquals("/explicit", compute("/explicit/main", Value.unobserved()).text());
        assertThrows(IOException.class, () -> compute(null, Value.available("/cwd")));
    }

    @Test
    public void logicalAliasRealnameFeedsNeededWithoutReplacingItWithAPhysicalParent() throws Exception {
        var origin = compute("/logical/alias/../lib.so", Value.unobserved());
        assertEquals("/logical/alias/..", origin.text());
        var request = new LinuxNeededContextObservation(new NeededRule(2, 3, List.of(4)), "$ORIGIN/dep.so");
        var first = SegmentClassicLinuxNeededName.prepare(request, Value.available(origin.text()), Value.unobserved(),
                Value.unobserved(), SegmentClassicLinuxNeededName.Secure.NON_SECURE, deadline());
        assertSame(request, first.request());
        assertEquals("$ORIGIN/dep.so", first.request().name());
        assertEquals("/logical/alias/../dep.so", first.name());
        assertEquals("/physical", compute("/physical/lib.so", Value.unobserved()).text());
        assertEquals("/logical/alias/..", origin.text());
    }

    @Test
    public void strictInputAndCompleteJoinedUtf8BoundsAreCheckedBeforeRemovingTheFilename() throws Exception {
        var unknown = Value.unobserved();
        assertEquals("/", compute("/" + "雪".repeat(1365), unknown).text());
        assertEquals("/x", compute("a".repeat(4093), Value.available("/x")).text());
        assertEquals("/x", compute("a".repeat(4093), Value.available("/x/")).text());
        assertThrows(IOException.class, () -> compute("a".repeat(4094), Value.available("/x")));
        assertThrows(IOException.class, () -> compute("a".repeat(4094), Value.available("/x/")));
        // The resulting parent would fit, but the complete path used for creation would exceed the bound.
        assertThrows(IOException.class, () -> compute("f", Value.available("/" + "a".repeat(4095))));
        for (String bad : List.of("a" + (char) 0, String.valueOf((char) 0xd800), "雪".repeat(1366))) {
            assertThrows(IOException.class, () -> compute(bad, unknown));
            assertThrows(IOException.class, () -> compute("/absolute/lib.so", Value.available("/" + bad)));
        }
        for (String badCwd : List.of("", "relative", ".")) {
            assertThrows(IOException.class, () -> compute("/absolute/lib.so", Value.available(badCwd)));
            assertThrows(IOException.class, () -> compute("", Value.available(badCwd)));
        }
        assertThrows(IOException.class, () -> compute("/absolute/lib.so", null));
    }

    @Test
    public void originalDeadlineAppliesToAbsoluteRelativeBlockedAndDeferredResults() {
        for (String raw : List.of("", "/lib.so", "lib.so")) {
            for (Value cwd : List.of(Value.available("/cwd"), Value.unavailable(), Value.unobserved())) {
                assertThrows(IOException.class, () -> SegmentClassicLinuxOrigin.compute(raw, cwd, System.nanoTime()));
            }
        }
    }
}

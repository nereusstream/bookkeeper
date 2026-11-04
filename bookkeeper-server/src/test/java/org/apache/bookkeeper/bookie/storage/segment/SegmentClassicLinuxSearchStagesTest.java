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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxNeededContextObservation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.NeededRule;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.Directory;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.AfterMiss;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.DirectoryPlan;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.ObjectData;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Source;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Stages;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Tag;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;
import org.junit.Test;

/** Explicit chain/conditional directory data only; no actual namespace, environment or selected library evidence. */
public class SegmentClassicLinuxSearchStagesTest {
    private static final DynamicPath ABSENT = new DynamicPath(false, "");

    private static DynamicPath path(String text) {
        return new DynamicPath(true, text);
    }

    private static ObjectData object(long identity, int context, DynamicPath rpath, DynamicPath runpath) {
        return new ObjectData(identity, context, rpath, runpath);
    }

    private static Stages select(List<ObjectData> chain, ObjectData main) throws IOException {
        return SegmentClassicLinuxSearchStages.select(chain, main, SegmentClassicLinuxElfTest.deadline());
    }

    private static Source rpath(ObjectData owner) {
        return new Source(Tag.RPATH, owner.identity(), owner.context(), owner.rpath());
    }

    private static AfterMiss request(String raw) throws IOException {
        return SegmentClassicLinuxNeededName.decideName(
                new LinuxNeededContextObservation(new NeededRule(2, 3, List.of(4)), raw), List.of(),
                Value.unobserved(), Value.unobserved(), Value.unobserved(),
                SegmentClassicLinuxNeededName.Secure.NON_SECURE, SegmentClassicLinuxElfTest.deadline()).afterMiss();
    }

    private static DirectoryPlan plan(AfterMiss request, List<ObjectData> chain, ObjectData main,
                                       Map<Long, Value> origins, Value effective, Value initializationOrigin)
            throws IOException {
        return SegmentClassicLinuxSearchStages.plan(request, chain, main, origins, effective, initializationOrigin,
                Value.unobserved(), Value.unobserved(), SegmentClassicLinuxElfTest.deadline());
    }

    private static List<String> names(List<Directory> directories) {
        return directories.stream().map(Directory::dirname).toList();
    }

    @Test
    public void directoryPlanUsesEachOpaqueOwnerOriginAndKeepsAllCrossSourceDirectoryAssociations() throws Exception {
        var requester = object(10, 2, path("$ORIGIN:/shared"), ABSENT);
        var ancestor = object(20, 2, path("$ORIGIN:/shared"), ABSENT);
        var main = object(30, 3, path("$ORIGIN:/shared"), ABSENT);
        var chain = new ArrayList<>(List.of(requester, ancestor));
        var origins = new HashMap<>(Map.of(10L, Value.available("/child"), 20L, Value.available("/ancestor"),
                30L, Value.available("/main")));
        var request = request("libx.so");
        var effective = Value.available("$ORIGIN;/shared");
        var initialization = Value.available("/initialization");
        var result = plan(request, chain, main, origins, effective, initialization);
        assertSame(request, result.request());
        assertEquals("libx.so", result.request().lookup().request().name());
        assertEquals(3, result.request().lookup().request().declaration().ordinal());
        assertEquals(State.AVAILABLE, result.state());
        assertEquals(List.of(rpath(requester), rpath(ancestor), rpath(main)),
                result.sources().rpathBeforeEnvironment());
        assertEquals(List.of("/child/", "/shared/"), names(result.rpathBeforeLibrary().get(0).directories()));
        assertEquals(List.of("/ancestor/", "/shared/"), names(result.rpathBeforeLibrary().get(1).directories()));
        assertEquals(List.of("/main/", "/shared/"), names(result.rpathBeforeLibrary().get(2).directories()));
        assertSame(ancestor.rpath(), result.rpathBeforeLibrary().get(1).source().declaration());
        assertEquals(List.of("/initialization/", "/shared/"), names(result.libraryPath().directories()));
        assertSame(effective, result.effectiveLibraryPath());
        assertSame(initialization, result.initializationOrigin());
        assertEquals(List.of(), result.runpathAfterLibrary());
        chain.clear();
        origins.clear();
        assertEquals(3, result.rpathBeforeLibrary().size());
        assertEquals(Value.available("/ancestor"), result.rpathBeforeLibrary().get(1).origin());
        assertThrows(UnsupportedOperationException.class, () -> result.rpathBeforeLibrary().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> result.rpathBeforeLibrary().get(0).directories().clear());
    }

    @Test
    public void directoryPlanRunpathPresenceMasksUnusedAncestorsAndKeepsEvenAnEmptyBlockAfterLibrary()
            throws Exception {
        var ancestor = object(20, 7, path("$ORIGIN/ancestor"), ABSENT);
        var main = object(30, 9, path("$ORIGIN/main"), ABSENT);
        for (String raw : List.of("", "$ORIGIN/run")) {
            var requester = object(10, 2, path("$ORIGIN/masked"), path(raw));
            Value origin = raw.isEmpty() ? Value.unobserved() : Value.available("/requester");
            var result = plan(request("libx.so"), List.of(requester, ancestor), main, Map.of(10L, origin),
                    Value.available("$ORIGIN"), Value.available("/initialization"));
            assertEquals(State.AVAILABLE, result.state());
            assertEquals(List.of(), result.rpathBeforeLibrary());
            assertEquals(List.of("/initialization/"), names(result.libraryPath().directories()));
            assertEquals(1, result.runpathAfterLibrary().size());
            assertEquals(new Source(Tag.RUNPATH, 10, 2, requester.runpath()),
                    result.runpathAfterLibrary().get(0).source());
            assertEquals(raw.isEmpty() ? List.of() : List.of("/requester/run/"),
                    names(result.runpathAfterLibrary().get(0).directories()));
        }
    }

    @Test
    public void directoryPlanUnknownRequiredDataNeverExposesEarlierBlocksAndUnusedUnknownDoesNotBlock()
            throws Exception {
        var literal = object(10, 2, path("/known"), ABSENT);
        var needed = object(10, 2, path("$ORIGIN"), ABSENT);
        var runpath = object(10, 2, ABSENT, path("$ORIGIN"));
        for (ObjectData requester : List.of(literal, needed, runpath)) {
            var result = plan(request("libx.so"), List.of(requester), requester, Map.of(10L, Value.unobserved()),
                    requester == literal ? Value.unobserved() : Value.available("/library"), Value.unobserved());
            assertEquals(State.UNOBSERVED, result.state());
            assertEquals(List.of(), result.rpathBeforeLibrary());
            assertNull(result.libraryPath());
            assertEquals(List.of(), result.runpathAfterLibrary());
            assertEquals(1, result.sources().rpathBeforeEnvironment().size()
                    + result.sources().runpathAfterEnvironment().size());
        }
        var complete = plan(request("libx.so"), List.of(literal), literal,
                Map.of(10L, Value.unobserved(), 99L, Value.unobserved()), Value.unavailable(), Value.unobserved());
        assertEquals(State.AVAILABLE, complete.state());
        assertEquals(List.of("/known/"), names(complete.rpathBeforeLibrary().get(0).directories()));
        assertEquals(List.of(), complete.libraryPath().directories());
        var omitted = plan(request("libx.so"), List.of(needed), needed, Map.of(10L, Value.unavailable()),
                Value.available(""), Value.unobserved());
        assertEquals(State.AVAILABLE, omitted.state());
        assertEquals(List.of(), omitted.rpathBeforeLibrary().get(0).directories());
        assertSame(needed.rpath(), omitted.rpathBeforeLibrary().get(0).source().declaration());
    }

    @Test
    public void directoryPlanRejectsWrongRequestContextMissingIdentityFactsAndInvalidUnusedDataOrDeadline()
            throws Exception {
        var requester = object(10, 2, path("/literal"), ABSENT);
        var request = request("libx.so");
        var unknown = Value.unobserved();
        assertThrows(IOException.class, () -> plan(request("/direct.so"), List.of(requester), requester,
                Map.of(10L, unknown), Value.unavailable(), unknown));
        var wrongContext = object(10, 1, path("/literal"), ABSENT);
        assertThrows(IOException.class, () -> plan(request, List.of(wrongContext), wrongContext,
                Map.of(10L, unknown), Value.unavailable(), unknown));
        assertThrows(IOException.class, () -> plan(request, List.of(requester), requester, Map.of(2L, unknown),
                Value.unavailable(), unknown));
        assertThrows(IOException.class, () -> plan(request, List.of(requester), requester,
                Map.of(10L, unknown, 99L, Value.available("bad" + (char) 0)), Value.unavailable(), unknown));
        var nullValue = new HashMap<Long, Value>();
        nullValue.put(10L, null);
        assertThrows(IOException.class, () -> plan(request, List.of(requester), requester, nullValue,
                Value.unavailable(), unknown));
        var excessive = new HashMap<Long, Value>();
        for (long i = 0; i < 4097; i++) {
            excessive.put(i, unknown);
        }
        assertThrows(IOException.class, () -> plan(request, List.of(requester), requester, excessive,
                Value.unavailable(), unknown));
        assertThrows(IOException.class, () -> SegmentClassicLinuxSearchStages.plan(request, List.of(requester),
                requester, Map.of(10L, unknown), Value.unavailable(), unknown, unknown, unknown, System.nanoTime()));
    }

    @Test
    public void directoryPlanAggregateCountsRepeatedAssociationsAndCwdBeforeRetainingAnotherBlock() throws Exception {
        var raw = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            raw.append(":p").append(i);
        }
        var declaration = path(raw.toString());
        var chain = new ArrayList<ObjectData>();
        var origins = new HashMap<Long, Value>();
        for (long i = 0; i < 4000; i++) {
            chain.add(object(i, 2, declaration, ABSENT));
            origins.put(i, Value.unobserved());
        }
        // 4000 * (65 literal directories + CWD) exceeds 262144; ignoring CWD or global dedup would hide it.
        var failure = assertThrows(IOException.class, () -> plan(request("libx.so"), chain, chain.get(3999), origins,
                Value.unavailable(), Value.unobserved()));
        assertTrue(failure.getMessage(), failure.getMessage().contains("fixed total name capacity"));
    }

    @Test
    public void requesterRunpathPresenceEvenEmptySuppressesAllRpathsWithoutEvaluatingTags() throws Exception {
        var ancestor = object(2, 1, path("/ancestor"), path("/ancestor-run"));
        var main = object(3, 2, path("/main"), ABSENT);
        for (DynamicPath runpath : List.of(path(""), path("$ORIGIN:$PLATFORM"))) {
            var requester = object(1, 0, path("$ORIGIN:$LIB"), runpath);
            var stages = select(List.of(requester, ancestor), main);
            assertEquals(List.of(), stages.rpathBeforeEnvironment());
            assertEquals(List.of(new Source(Tag.RUNPATH, 1, 0, runpath)), stages.runpathAfterEnvironment());
            assertSame(runpath, stages.runpathAfterEnvironment().get(0).declaration());
        }
    }

    @Test
    public void rpathOrderContinuesPastAncestorWithRunpathAndNeverInheritsItsRunpath() throws Exception {
        var requester = object(10, 4, path("$ORIGIN/requester"), ABSENT);
        var masked = object(20, 8, path("/masked"), path(""));
        var farther = object(30, 12, path(""), ABSENT);
        var main = object(40, 16, path("/main"), ABSENT);
        var stages = select(List.of(requester, masked, farther), main);
        assertEquals(List.of(rpath(requester), rpath(farther), rpath(main)), stages.rpathBeforeEnvironment());
        assertSame(requester.rpath(), stages.rpathBeforeEnvironment().get(0).declaration());
        assertSame(farther.rpath(), stages.rpathBeforeEnvironment().get(1).declaration());
        assertEquals(List.of(), stages.runpathAfterEnvironment());
        var absent = object(50, 20, ABSENT, ABSENT);
        assertEquals(List.of(), select(List.of(absent, masked), object(60, 24, path("/main-r"), path("/main-run")))
                .rpathBeforeEnvironment());
        assertEquals(List.of(), select(List.of(absent, masked), main).runpathAfterEnvironment());
    }

    @Test
    public void mainUsesObjectIdentityAndEqualPathOrContextDoesNotDeduplicateDifferentObjects() throws Exception {
        var samePath = path("/shared");
        var requester = object(1, 7, samePath, ABSENT);
        var main = object(2, 7, samePath, ABSENT);
        var inChain = select(List.of(requester, main), object(2, 7, path("/shared"), ABSENT));
        assertEquals(List.of(rpath(requester), rpath(main)), inChain.rpathBeforeEnvironment());
        assertSame(main.rpath(), inChain.rpathBeforeEnvironment().get(1).declaration());
        assertEquals(List.of(rpath(requester), rpath(main)), select(List.of(requester), main).rpathBeforeEnvironment());
        assertEquals(List.of(rpath(requester)), select(List.of(requester), requester).rpathBeforeEnvironment());
    }

    @Test
    public void repeatedIdentitiesCyclesAndConflictingMainDataRejectEvenWhenRunpathMasksSources() {
        var requester = object(1, 0, path("/r"), path(""));
        var ancestor = object(2, 1, path("/a"), ABSENT);
        assertThrows(IOException.class, () -> select(List.of(requester, ancestor, requester), ancestor));
        assertThrows(IOException.class,
                () -> select(List.of(requester, object(1, 2, path("/other"), ABSENT)), ancestor));
        assertThrows(IOException.class,
                () -> select(List.of(requester, ancestor), object(2, 1, path("/changed"), ABSENT)));
        assertThrows(IOException.class,
                () -> select(List.of(requester, ancestor), object(2, 2, ancestor.rpath(), ABSENT)));
        assertThrows(IOException.class,
                () -> select(List.of(requester, ancestor), object(2, 1, ancestor.rpath(), path(""))));
        assertThrows(IOException.class, () -> select(List.of(), ancestor));
        assertThrows(IOException.class, () -> select(null, ancestor));
        assertThrows(IOException.class, () -> select(Arrays.asList(requester, null), ancestor));
        assertThrows(IOException.class, () -> select(List.of(requester), null));
        assertThrows(IllegalArgumentException.class, () -> object(1, -1, ABSENT, ABSENT));
        assertThrows(IllegalArgumentException.class, () -> object(1, 4096, ABSENT, ABSENT));
    }

    @Test
    public void combinedDistinctObjectBoundIs4096AndResultsKeepImmutableOriginalSources() throws Exception {
        var original = path("/same");
        var chain = new ArrayList<ObjectData>();
        for (int i = 0; i < 4096; i++) {
            chain.add(object(i, 0, original, ABSENT));
        }
        var main = chain.get(4095);
        var stages = select(chain, main);
        assertEquals(4096, stages.rpathBeforeEnvironment().size());
        assertEquals(rpath(main), stages.rpathBeforeEnvironment().get(4095));
        assertSame(original, stages.rpathBeforeEnvironment().get(4095).declaration());
        assertThrows(IOException.class, () -> select(chain, object(4096, 0, ABSENT, ABSENT)));
        chain.add(object(4096, 0, ABSENT, ABSENT));
        assertThrows(IOException.class, () -> select(chain, main));
        chain.clear();
        assertEquals(4096, stages.rpathBeforeEnvironment().size());
        assertThrows(UnsupportedOperationException.class, () -> stages.rpathBeforeEnvironment().clear());
        var requester = object(0, 0, ABSENT, path(""));
        assertThrows(UnsupportedOperationException.class, () -> select(List.of(requester), requester)
                .runpathAfterEnvironment().clear());
    }

    @Test
    public void originalDeadlineAppliesToBothSourceBranchesAndNoSourceResult() {
        for (ObjectData requester : List.of(object(0, 0, ABSENT, ABSENT), object(1, 1, path("/r"), ABSENT),
                object(2, 2, path("/r"), path("")))) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxSearchStages.select(List.of(requester), requester,
                    System.nanoTime()));
        }
    }
}

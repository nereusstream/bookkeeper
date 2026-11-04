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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.ObjectData;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Source;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Stages;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxSearchStages.Tag;
import org.junit.Test;

/** Explicit chain data only; no actual namespace, environment, path expansion or selected library evidence. */
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

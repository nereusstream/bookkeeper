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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;

/** Dynamic-tag source stages for an explicit base-namespace chain, already in the directory-search branch. */
final class SegmentClassicLinuxSearchStages {
    private static final int MAX_OBJECTS = 4096;

    /** Identity names an object, independently of context, file or SONAME; this data proves no actual load history. */
    record ObjectData(long identity, int context, DynamicPath rpath, DynamicPath runpath) {
        ObjectData {
            Objects.requireNonNull(rpath, "rpath");
            Objects.requireNonNull(runpath, "runpath");
            if (context < 0 || context >= MAX_OBJECTS) {
                throw new IllegalArgumentException("An original bounded context index is required");
            }
        }
    }

    enum Tag { RPATH, RUNPATH }

    record Source(Tag tag, long owner, int context, DynamicPath declaration) { }

    /** The intervening environment stage is not observed or asserted empty by these two source lists. */
    record Stages(List<Source> rpathBeforeEnvironment, List<Source> runpathAfterEnvironment) {
        Stages {
            rpathBeforeEnvironment = List.copyOf(rpathBeforeEnvironment);
            runpathAfterEnvironment = List.copyOf(runpathAfterEnvironment);
        }
    }

    private SegmentClassicLinuxSearchStages() { }

    static Stages select(List<ObjectData> requesterToAncestors, ObjectData main, long deadline) throws IOException {
        requireTime(deadline);
        if (requesterToAncestors == null || requesterToAncestors.isEmpty()
                || requesterToAncestors.size() > MAX_OBJECTS || main == null) {
            throw new IOException("An explicit bounded requester chain and main object are required");
        }
        var chain = new ArrayList<ObjectData>();
        var objects = new HashMap<Long, ObjectData>();
        for (ObjectData object : requesterToAncestors) {
            requireTime(deadline);
            if (chain.size() == MAX_OBJECTS || object == null) {
                throw new IOException("Invalid or excessive explicit loader-chain data");
            }
            if (objects.putIfAbsent(object.identity(), object) != null) {
                throw new IOException("Repeated object identity or cycle in the explicit loader chain");
            }
            chain.add(object);
        }
        ObjectData existingMain = objects.get(main.identity());
        if (existingMain != null && !existingMain.equals(main)) {
            throw new IOException("Main object identity conflicts with its loader-chain data");
        }
        if (existingMain == null && objects.size() == MAX_OBJECTS) {
            throw new IOException("Explicit loader-chain and main objects exceed their fixed bound");
        }
        ObjectData requester = chain.get(0);
        Stages result;
        if (requester.runpath().present()) {
            result = new Stages(List.of(), List.of(new Source(Tag.RUNPATH, requester.identity(), requester.context(),
                    requester.runpath())));
        } else {
            var rpaths = new ArrayList<Source>();
            for (ObjectData object : chain) {
                requireTime(deadline);
                retainRpath(object, rpaths);
            }
            if (existingMain == null) {
                requireTime(deadline);
                retainRpath(main, rpaths);
            }
            result = new Stages(rpaths, List.of());
        }
        requireTime(deadline);
        return result;
    }

    private static void retainRpath(ObjectData object, List<Source> sources) {
        if (!object.runpath().present() && object.rpath().present()) {
            sources.add(new Source(Tag.RPATH, object.identity(), object.context(), object.rpath()));
        }
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Dynamic-tag source selection deadline elapsed");
        }
    }
}

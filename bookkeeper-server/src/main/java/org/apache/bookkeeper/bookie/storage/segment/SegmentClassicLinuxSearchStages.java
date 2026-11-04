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
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LoadingDirectoryBudget;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.Directory;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDynamicPath.LibraryPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxNeededName.AfterMiss;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;

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

    /** One selected tag's given owner origin and complete directories; equal directories in other blocks remain. */
    record TagBlock(Source source, Value origin, List<Directory> directories) {
        TagBlock {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(origin, "origin");
            directories = List.copyOf(directories);
        }
    }

    /** Fixed order is RPATH blocks, library path, RUNPATH blocks. Unknown plans expose no usable blocks. */
    record DirectoryPlan(AfterMiss request, Stages sources, Value effectiveLibraryPath, Value initializationOrigin,
                         State state, List<TagBlock> rpathBeforeLibrary, LibraryPath libraryPath,
                         List<TagBlock> runpathAfterLibrary) {
        DirectoryPlan {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(sources, "sources");
            Objects.requireNonNull(effectiveLibraryPath, "effectiveLibraryPath");
            Objects.requireNonNull(initializationOrigin, "initializationOrigin");
            Objects.requireNonNull(state, "state");
            rpathBeforeLibrary = List.copyOf(rpathBeforeLibrary);
            runpathAfterLibrary = List.copyOf(runpathAfterLibrary);
            if (state == State.UNAVAILABLE || (state == State.AVAILABLE) != (libraryPath != null)
                    || state == State.UNOBSERVED && (!rpathBeforeLibrary.isEmpty() || !runpathAfterLibrary.isEmpty())) {
                throw new IllegalArgumentException("A directory plan is complete or unobserved without a prefix");
            }
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

    /** Given-data pre-cache plan only; the caller must establish actual name miss, chain and all source facts. */
    static DirectoryPlan plan(AfterMiss request, List<ObjectData> requesterToAncestors, ObjectData main,
                              Map<Long, Value> origins, Value effectiveLibraryPath, Value initializationOrigin,
                              Value lib, Value platform, long deadline) throws IOException {
        requireTime(deadline);
        if (request == null || request.branch() != SegmentClassicLinuxNeededName.Branch.DIRECTORY_SEARCH
                || request.lookup().state() != SegmentClassicLinuxNeededName.State.LOOKUP_NAME
                || request.lookup().name().isEmpty() || request.lookup().name().indexOf('/') >= 0
                || request.lookup().request().declaration() == null) {
            throw new IOException("An explicit original directory-search request is required");
        }
        SegmentClassicLinuxTokens.utf8Length(request.lookup().name(), deadline);
        var original = request.lookup().request();
        SegmentClassicLinuxTokens.utf8Length(original.name(), deadline);
        if (original.name().isEmpty() || original.declaration().requester() < 0
                || original.declaration().requester() >= MAX_OBJECTS || original.declaration().ordinal() < 0
                || original.declaration().ordinal() >= SegmentClassicLinuxDeploymentV2.MAX_REQUESTS) {
            throw new IOException("A bounded original requester/NEEDED ordinal and raw name are required");
        }
        if (requesterToAncestors == null || requesterToAncestors.isEmpty()
                || requesterToAncestors.size() > MAX_OBJECTS) {
            throw new IOException("An explicit bounded requester chain is required");
        }
        var chain = new ArrayList<ObjectData>();
        for (ObjectData object : requesterToAncestors) {
            requireTime(deadline);
            if (chain.size() == MAX_OBJECTS || object == null) {
                throw new IOException("Invalid or excessive requester-chain data");
            }
            chain.add(object);
        }
        Stages sources = select(chain, main, deadline);
        if (chain.get(0).context() != original.declaration().requester()) {
            throw new IOException("The chain head differs from the original requester context");
        }
        if (origins == null || origins.size() > MAX_OBJECTS) {
            throw new IOException("An explicit bounded object-origin table is required");
        }
        var byIdentity = new HashMap<Long, Value>();
        for (var entry : origins.entrySet()) {
            requireTime(deadline);
            if (byIdentity.size() == MAX_OBJECTS || entry.getKey() == null) {
                throw new IOException("Invalid or excessive object-origin data");
            }
            validateValue(entry.getValue(), deadline);
            byIdentity.put(entry.getKey(), entry.getValue());
        }
        for (Source source : sources.rpathBeforeEnvironment()) {
            requireOrigin(source, byIdentity, deadline);
        }
        for (Source source : sources.runpathAfterEnvironment()) {
            requireOrigin(source, byIdentity, deadline);
        }
        validateValue(effectiveLibraryPath, deadline);
        validateValue(initializationOrigin, deadline);
        validateValue(lib, deadline);
        validateValue(platform, deadline);
        var budget = new LoadingDirectoryBudget();
        var rpaths = new ArrayList<TagBlock>();
        var runpaths = new ArrayList<TagBlock>();
        for (Source source : sources.rpathBeforeEnvironment()) {
            var expanded = SegmentClassicLinuxDynamicPath.expand(source.declaration(), byIdentity.get(source.owner()),
                    lib, platform, deadline);
            if (expanded.state() == State.UNOBSERVED) {
                return unobserved(request, sources, effectiveLibraryPath, initializationOrigin, deadline);
            }
            retain(expanded.directories(), budget, deadline);
            rpaths.add(new TagBlock(source, byIdentity.get(source.owner()), expanded.directories()));
        }
        var library = SegmentClassicLinuxDynamicPath.expandLibraryPath(effectiveLibraryPath, initializationOrigin,
                lib, platform, deadline);
        if (library.state() == State.UNOBSERVED) {
            return unobserved(request, sources, effectiveLibraryPath, initializationOrigin, deadline);
        }
        retain(library.directories(), budget, deadline);
        for (Source source : sources.runpathAfterEnvironment()) {
            var expanded = SegmentClassicLinuxDynamicPath.expand(source.declaration(), byIdentity.get(source.owner()),
                    lib, platform, deadline);
            if (expanded.state() == State.UNOBSERVED) {
                return unobserved(request, sources, effectiveLibraryPath, initializationOrigin, deadline);
            }
            retain(expanded.directories(), budget, deadline);
            runpaths.add(new TagBlock(source, byIdentity.get(source.owner()), expanded.directories()));
        }
        requireTime(deadline);
        return new DirectoryPlan(request, sources, effectiveLibraryPath, initializationOrigin, State.AVAILABLE, rpaths,
                library, runpaths);
    }

    private static void validateValue(Value value, long deadline) throws IOException {
        requireTime(deadline);
        if (value == null) {
            throw new IOException("Explicit token or effective library-path data is required");
        }
        if (value.state() == State.AVAILABLE) {
            SegmentClassicLinuxTokens.utf8Length(value.text(), deadline);
        }
    }

    private static void requireOrigin(Source source, Map<Long, Value> origins, long deadline) throws IOException {
        requireTime(deadline);
        if (!origins.containsKey(source.owner())) {
            throw new IOException("A selected tag has no explicit origin for its original object identity");
        }
    }

    private static void retain(List<Directory> directories, LoadingDirectoryBudget budget, long deadline)
            throws IOException {
        for (Directory directory : directories) {
            requireTime(deadline);
            budget.retain(directory.dirname());
        }
    }

    private static DirectoryPlan unobserved(AfterMiss request, Stages sources, Value effectiveLibraryPath,
                                            Value initializationOrigin, long deadline) throws IOException {
        requireTime(deadline);
        return new DirectoryPlan(request, sources, effectiveLibraryPath, initializationOrigin, State.UNOBSERVED,
                List.of(), null, List.of());
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

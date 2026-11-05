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
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Command;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Execution;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Isolation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Operations;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Phase;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Role;

/** Schema two approval data. Decoding uses raw strings, never a Path/provider, target I/O or qualification. */
record SegmentClassicLinuxDeploymentV2(Platform platform, List<File> files, Provider provider, Loading loading,
        List<Command> startChain, List<Unit> units, int launcherUnit, List<InstallLink> installLinks,
        Isolation isolation, Operations operations) {
    static final long MAGIC = 0x5557434c44303032L; // UWCLD002
    static final int MAX_REQUESTS = 8192;
    static final int MAX_CANDIDATES = 262144;
    static final int MAX_RELATIONS = 65536;
    private static final byte[] DOMAIN = "BookKeeper/ControlledClassicLinuxDeployment/v2\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final Set<String> FILE_STATES = Set.of("enabled", "enabled-runtime", "disabled", "static",
            "indirect", "linked", "linked-runtime");

    record Platform(String profile, String kernelRelease, String kernelVersion, String systemdVersion,
                    String systemctlVersion, String loaderVersion, int showFormat, int elfFormat) {
        Platform {
            if (!SegmentClassicLinuxDeployment.PROFILE.equals(profile) || showFormat != 2 || elfFormat != 2) {
                throw new IllegalArgumentException("Unknown schema two Linux platform or parser format");
            }
            List.of(kernelRelease, kernelVersion, systemdVersion, systemctlVersion, loaderVersion)
                    .forEach(SegmentClassicLinuxDeploymentV2::text);
        }
    }

    record File(String path, Role role, long length, SegmentClassicHandoff.Digest sha256) {
        File {
            physical(path);
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(sha256, "sha256");
            if (length < 0 || role.elf() && length < 64) {
                throw new IllegalArgumentException("Invalid schema two file length");
            }
        }
    }

    /** Rule one is the approved Linux default filesystem provider/UnixPath/UnixUri implementation contract. */
    record Provider(int rule, String runtime, String vendor, int image) {
        Provider {
            one(rule);
            text(runtime);
            text(vendor);
        }
    }

    record Alias(String link, String target) {
        Alias {
            physical(link);
            text(target);
        }
    }

    record Directory(String path, boolean present) {
        Directory {
            logical(path);
        }
    }

    enum ConfigurationKind { LDCONFIG, CACHE, PRELOAD_ABSENT, MODULE_RUNTIME }

    /** Parser dispatch is by mandatory kind/format, never inferred from the material's file role. */
    record Configuration(String path, int file, ConfigurationKind kind, int format) {
        Configuration {
            physical(path);
            Objects.requireNonNull(kind, "kind");
            one(format);
            if (file < -1 || kind == ConfigurationKind.PRELOAD_ABSENT && file != -1) {
                throw new IllegalArgumentException("Invalid explicit configuration presence");
            }
        }
    }

    /** Rule IDs bind fixed algorithms and budgets; they are not runtime facts or selectable implementations. */
    record LoaderPolicy(int elf, int cache, int comparator, int paths, int configuration, int glob,
                        int relevance, int hwcaps, int tokens, int diagnostics, String libToken,
                        List<String> defaultDirectories) {
        LoaderPolicy {
            if (elf != 2) {
                throw new IllegalArgumentException("Schema two requires the ELF two contract");
            }
            for (int rule : new int[]{cache, comparator, paths, configuration, glob, relevance, hwcaps, tokens,
                    diagnostics}) {
                one(rule);
            }
            text(libToken);
            if (libToken.startsWith("/") || libToken.endsWith("/") || libToken.contains("//")
                    || Arrays.stream(libToken.split("/", -1)).anyMatch(s -> s.equals(".") || s.equals(".."))) {
                throw new IllegalArgumentException("LIB must be the approved loader build's relative directory token");
            }
            defaultDirectories = bounded(defaultDirectories, 64);
            if (defaultDirectories.isEmpty() || new HashSet<>(defaultDirectories).size() != defaultDirectories.size()) {
                throw new IllegalArgumentException("Explicit ordered loader default directories required");
            }
            defaultDirectories.forEach(SegmentClassicLinuxDeploymentV2::logical);
        }
    }

    /** An absent dynamic tag differs from a present empty value; colon components remain in the original string. */
    record DynamicPath(boolean present, String value) {
        DynamicPath {
            utf8(value, true);
            if (!present && !value.isEmpty()) {
                throw new IllegalArgumentException("Absent dynamic path cannot carry a value");
            }
        }
    }

    record Elf(int file, List<String> needed, DynamicPath rpath, DynamicPath runpath, long flags, long flags1) {
        Elf {
            needed = bounded(needed, MAX_REQUESTS);
            needed.forEach(SegmentClassicLinuxDeploymentV2::text);
            Objects.requireNonNull(rpath, "rpath");
            Objects.requireNonNull(runpath, "runpath");
        }
    }

    /** This pair identifies a load position, not the complete ancestor RPATH chain or first-loaded namespace. */
    record Context(int file, String logicalLoadName) {
        Context {
            logical(logicalLoadName);
        }
    }

    record NeededRule(int requester, int ordinal, List<Integer> candidates) {
        NeededRule {
            candidates = references(candidates, 4096, true);
        }
    }

    /** Requests preserve their original order, including repeats, and bind candidates per requesting context. */
    record ModuleRequest(int requester, String name, List<Integer> candidates) {
        ModuleRequest {
            text(name);
            candidates = references(candidates, 4096, true);
        }
    }

    record Loading(int systemctl, int loader, String interpreter, List<Alias> aliases, List<Directory> directories,
                   List<Configuration> configuration, LoaderPolicy policy, List<Elf> elf, List<Context> contexts,
                   List<NeededRule> needed, List<ModuleRequest> modules) {
        Loading {
            logical(interpreter);
            aliases = bounded(aliases, 128);
            directories = bounded(directories, 64);
            configuration = bounded(configuration, 1024);
            elf = bounded(elf, 1024);
            contexts = bounded(contexts, 4096);
            needed = bounded(needed, MAX_REQUESTS);
            modules = bounded(modules, MAX_REQUESTS);
            Objects.requireNonNull(policy, "policy");
            ordered(aliases.stream().map(Alias::link).toList());
            ordered(directories.stream().map(Directory::path).toList());
            ordered(configuration.stream().map(Configuration::path).toList());
            ascending(elf.stream().map(Elf::file).toList());
            int previousFile = -1;
            String previousName = "";
            for (Context context : contexts) {
                if (context.file() < previousFile || context.file() == previousFile
                        && compare(previousName, context.logicalLoadName()) >= 0) {
                    throw new IllegalArgumentException("Contexts require unique file/name order");
                }
                previousFile = context.file();
                previousName = context.logicalLoadName();
            }
            int previousRequester = -1;
            int previousOrdinal = -1;
            for (NeededRule rule : needed) {
                if (rule.requester() < previousRequester || rule.requester() == previousRequester
                        && rule.ordinal() <= previousOrdinal || rule.ordinal() < 0) {
                    throw new IllegalArgumentException("NEEDED rules require requester/original ordinal order");
                }
                previousRequester = rule.requester();
                previousOrdinal = rule.ordinal();
            }
            limit((long) needed.size() + modules.size(), MAX_REQUESTS);
            long memberships = 0;
            for (NeededRule rule : needed) {
                memberships += rule.candidates().size();
            }
            for (ModuleRequest rule : modules) {
                memberships += rule.candidates().size();
            }
            limit(memberships, MAX_CANDIDATES);
        }
    }

    enum Kind { SERVICE, TARGET }

    // Each consecutive pair is a forward/reverse family. Every unit must explicitly supply every row.
    enum Family {
        WANTS, WANTED_BY, REQUIRES, REQUIRED_BY, REQUISITE, REQUISITE_OF, TRIGGERS, TRIGGERED_BY,
        UPHOLDS, UPHELD_BY, ON_FAILURE, ON_FAILURE_OF, ON_SUCCESS, ON_SUCCESS_OF, BINDS_TO, BOUND_BY,
        PART_OF, CONSISTS_OF, PROPAGATES_RELOAD_TO, RELOAD_PROPAGATED_FROM,
        PROPAGATES_STOP_TO, STOP_PROPAGATED_FROM, BEFORE, AFTER, CONFLICTS, CONFLICTED_BY
    }

    record Relation(Family family, List<String> names) {
        Relation {
            Objects.requireNonNull(family, "family");
            names = bounded(names, 1024);
            names.forEach(SegmentClassicLinuxDeploymentV2::unitName);
            ordered(names);
        }
    }

    record Service(long group, List<Execution> execution, int constraint) {
        Service {
            one(constraint);
            if (group < 0 || group >= 0xffff_ffffL) {
                throw new IllegalArgumentException("Invalid fixed service group");
            }
            execution = bounded(execution, 32);
            int previous = -1;
            for (Execution entry : execution) {
                if (entry.phase().ordinal() < previous) {
                    throw new IllegalArgumentException("Service phases require their original order");
                }
                previous = entry.phase().ordinal();
            }
            if (execution.stream().filter(e -> e.phase() == Phase.START).count() != 1) {
                throw new IllegalArgumentException("Type exec requires exactly one START command");
            }
        }
    }

    /** Common rule one requires loaded/nontransient/fresh/file-backed units, stable state and no job in A/B. */
    record Unit(Kind kind, String id, List<String> names, String fileState, int fragment, List<Integer> dropIns,
                int commonRule, List<Relation> relations, Service service) {
        Unit {
            Objects.requireNonNull(kind, "kind");
            unitName(id);
            one(commonRule);
            if (!id.endsWith(kind == Kind.SERVICE ? ".service" : ".target")
                    || (kind == Kind.SERVICE) != (service != null) || !FILE_STATES.contains(fileState)) {
                throw new IllegalArgumentException("Unit kind and its typed constraints differ");
            }
            names = bounded(names, 32);
            ordered(names);
            if (!names.contains(id)) {
                throw new IllegalArgumentException("Names must contain the original Id");
            }
            for (String name : names) {
                unitName(name);
                if (!name.endsWith(kind == Kind.SERVICE ? ".service" : ".target")) {
                    throw new IllegalArgumentException("An alias must have the same unit type");
                }
            }
            dropIns = references(dropIns, 32, false);
            relations = bounded(relations, Family.values().length);
            if (relations.size() != Family.values().length) {
                throw new IllegalArgumentException("Missing explicit relation family");
            }
            for (int i = 0; i < relations.size(); i++) {
                if (relations.get(i).family().ordinal() != i) {
                    throw new IllegalArgumentException("Relation families must occur once in fixed order");
                }
            }
        }
    }

    enum LinkKind { UNIT_LOOKUP, WANTS, REQUIRES, UPHOLDS }

    /** An actual symlink declaration, not an instruction to create/install a unit or synthesize manager relations. */
    record InstallLink(LinkKind kind, String path, String target, int sourceUnit, int targetUnit) {
        InstallLink {
            Objects.requireNonNull(kind, "kind");
            physical(path);
            text(target);
            if ((kind == LinkKind.UNIT_LOOKUP) != (sourceUnit == -1)) {
                throw new IllegalArgumentException("Unit lookup links have no dependency source unit");
            }
        }
    }

    SegmentClassicLinuxDeploymentV2 {
        Objects.requireNonNull(platform, "platform");
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(loading, "loading");
        Objects.requireNonNull(isolation, "isolation");
        Objects.requireNonNull(operations, "operations");
        files = bounded(files, 1024);
        units = bounded(units, 32);
        startChain = bounded(startChain, 32);
        installLinks = bounded(installLinks, 1024);
        if (files.isEmpty() || units.isEmpty() || startChain.isEmpty()) {
            throw new IllegalArgumentException("Complete schema two approval data required");
        }
        ordered(files.stream().map(File::path).toList());
        ordered(units.stream().map(Unit::id).toList());
        ordered(installLinks.stream().map(InstallLink::path).toList());
        role(files, provider.image(), Role.JDK_RUNTIME);
        role(files, loading.systemctl(), Role.SYSTEMCTL);
        role(files, loading.loader(), Role.LOADER);
        role(files, isolation.dropper(), Role.DROPPER);
        if (!files.get(loading.systemctl()).path().equals("/usr/bin/systemctl")) {
            throw new IllegalArgumentException("Only the fixed systemctl executable is supported");
        }
        long bytes = 0;
        int images = 0;
        for (File file : files) {
            bytes = Math.addExact(bytes, file.length());
            images += file.role() == Role.JDK_RUNTIME ? 1 : 0;
        }
        if (images != 1) {
            throw new IllegalArgumentException("Exactly the original JDK runtime image must be bound");
        }
        requireLoading(files, loading);
        for (Command command : startChain) {
            command(files, command);
        }
        for (int i = 1; i < startChain.size(); i++) {
            List<String> previous = startChain.get(i - 1).arguments();
            List<String> next = startChain.get(i).arguments();
            if (previous.size() <= next.size() || !previous.subList(previous.size() - next.size(), previous.size())
                    .equals(next)) {
                throw new IllegalArgumentException("Startup chain must preserve the exact next argv");
            }
        }
        requireUnits(files, units, launcherUnit, installLinks);
        List<Command> starts = units.get(launcherUnit).service().execution().stream()
                .filter(e -> e.phase() == Phase.START).map(Execution::command).toList();
        if (!starts.equals(List.of(startChain.get(0)))) {
            throw new IllegalArgumentException("Launcher START must name the original startup chain");
        }
    }

    private static void requireLoading(List<File> files, Loading loading) {
        Set<String> paths = new HashSet<>(files.stream().map(File::path).toList());
        for (Alias alias : loading.aliases()) {
            if (!paths.add(alias.link())) {
                throw new IllegalArgumentException("Conflicting file/alias path");
            }
            resolve(alias.link(), loading.aliases());
        }
        for (File file : files) {
            if (!resolve(file.path(), loading.aliases()).equals(file.path())) {
                throw new IllegalArgumentException("Final files cannot use a system alias");
            }
        }
        requireInterpreterReference(files, loading);
        Map<String, Configuration> configuration = new HashMap<>();
        Set<Integer> materials = new HashSet<>();
        for (Configuration entry : loading.configuration()) {
            configuration.put(entry.path(), entry);
            String resolved = resolve(entry.path(), loading.aliases());
            if (entry.file() == -1) {
                if (files.stream().anyMatch(f -> f.path().equals(resolved))
                        || loading.aliases().stream().anyMatch(a -> a.link().equals(entry.path()))) {
                    throw new IllegalArgumentException("Absent configuration conflicts with a present member");
                }
            } else {
                role(files, entry.file(), entry.kind() == ConfigurationKind.CACHE ? Role.LOADER_CACHE
                        : Role.LOADER_CONFIG);
                if (!files.get(entry.file()).path().equals(resolved)) {
                    throw new IllegalArgumentException("Configuration physical reference differs");
                }
                materials.add(entry.file());
            }
        }
        for (var required : Map.of("/etc/ld.so.preload", ConfigurationKind.PRELOAD_ABSENT,
                "/etc/ld.so.cache", ConfigurationKind.CACHE, "/etc/ld.so.conf", ConfigurationKind.LDCONFIG)
                .entrySet()) {
            var entry = configuration.get(required.getKey());
            if (entry == null || entry.kind() != required.getValue()) {
                throw new IllegalArgumentException("Missing or mistyped fixed loader configuration rule");
            }
        }
        for (Configuration entry : loading.configuration()) {
            if (entry.kind() == ConfigurationKind.PRELOAD_ABSENT && !entry.path().equals("/etc/ld.so.preload")
                    || entry.kind() == ConfigurationKind.CACHE && !entry.path().equals("/etc/ld.so.cache")) {
                throw new IllegalArgumentException("Fixed preload/cache kind has another path");
            }
        }
        for (int i = 0; i < files.size(); i++) {
            if (Set.of(Role.LOADER_CONFIG, Role.LOADER_CACHE).contains(files.get(i).role()) && !materials.contains(i)) {
                throw new IllegalArgumentException("Configuration material lacks its typed presence rule");
            }
        }
        for (Directory directory : loading.directories()) {
            String resolved = resolve(directory.path(), loading.aliases());
            if (files.stream().anyMatch(f -> f.path().equals(resolved)
                    || !directory.present() && beneath(f.path(), resolved))
                    || !directory.present() && loading.aliases().stream().anyMatch(a -> beneath(a.link(), resolved))) {
                throw new IllegalArgumentException("Directory presence conflicts with an approved member");
            }
        }
        defaultDirectoryIndices(loading);
        requireContextReferences(files, loading);
    }

    /** Original fixed preload rule index; no caller path, I/O or startup qualification. */
    int preloadAbsentConfigurationIndex() throws IOException {
        for (int i = 0; i < loading.configuration().size(); i++) {
            var rule = loading.configuration().get(i);
            if (rule.path().equals("/etc/ld.so.preload") && rule.kind() == ConfigurationKind.PRELOAD_ABSENT
                    && rule.format() == 1 && rule.file() == -1) {
                return i;
            }
        }
        throw new IOException("Missing original fixed preload absence rule");
    }

    /** Exact raw declarations, in policy search order; no physical normalization or runtime selection. */
    static List<Integer> defaultDirectoryIndices(Loading loading) {
        var indices = new ArrayList<Integer>();
        for (String directory : loading.policy().defaultDirectories()) {
            int selected = -1;
            for (int i = 0; i < loading.directories().size(); i++) {
                if (loading.directories().get(i).path().equals(directory)) {
                    if (selected != -1) {
                        throw new IllegalArgumentException("Default search directory has conflicting presence rules");
                    }
                    selected = i;
                }
            }
            if (selected == -1) {
                throw new IllegalArgumentException("Default search directory lacks its explicit presence rule");
            }
            indices.add(selected);
        }
        return List.copyOf(indices);
    }

    /** Pure context-subgraph check only; the complete constructor retains all other deployment checks. */
    static void requireContextReferences(List<File> files, Loading loading) {
        Map<Integer, Elf> elf = new HashMap<>();
        long needed = 0;
        for (Elf entry : loading.elf()) {
            reference(entry.file(), files.size());
            if (!files.get(entry.file()).role().elf()) {
                throw new IllegalArgumentException("ELF metadata refers to a non-ELF member");
            }
            elf.put(entry.file(), entry);
            needed += entry.needed().size();
        }
        limit(needed, MAX_REQUESTS);
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).role().elf() != elf.containsKey(i)) {
                throw new IllegalArgumentException("Every ELF member requires its original dynamic metadata");
            }
        }
        Set<Integer> contextFiles = new HashSet<>();
        long requests = 0;
        for (Context context : loading.contexts()) {
            Elf entry = elf.get(context.file());
            if (entry == null || !resolve(context.logicalLoadName(), loading.aliases())
                    .equals(files.get(context.file()).path())) {
                throw new IllegalArgumentException("Load context differs from its physical ELF member");
            }
            contextFiles.add(context.file());
            requests += entry.needed().size();
        }
        if (!contextFiles.equals(elf.keySet()) || requests != loading.needed().size()) {
            throw new IllegalArgumentException("Every load context requires every original NEEDED ordinal");
        }
        int ruleIndex = 0;
        for (int i = 0; i < loading.contexts().size(); i++) {
            int size = elf.get(loading.contexts().get(i).file()).needed().size();
            for (int ordinal = 0; ordinal < size; ordinal++) {
                NeededRule rule = loading.needed().get(ruleIndex++);
                if (rule.requester() != i || rule.ordinal() != ordinal) {
                    throw new IllegalArgumentException("Missing or conflicting NEEDED context/ordinal");
                }
                candidates(files, loading.contexts(), rule.candidates(), false);
            }
        }
        for (ModuleRequest request : loading.modules()) {
            reference(request.requester(), loading.contexts().size());
            candidates(files, loading.contexts(), request.candidates(), true);
        }
    }

    /** Fixed interpreter reference only, never complete deployment validity or startup authority. */
    static void requireInterpreterReference(List<File> files, Loading loading) {
        role(files, loading.loader(), Role.LOADER);
        if (!resolve(loading.interpreter(), loading.aliases()).equals(files.get(loading.loader()).path())) {
            throw new IllegalArgumentException("Interpreter differs from the approved loader");
        }
    }

    private static void candidates(List<File> files, List<Context> contexts, List<Integer> candidates, boolean module) {
        for (int index : candidates) {
            reference(index, contexts.size());
            Role role = files.get(contexts.get(index).file()).role();
            if (module ? role != Role.MODULE : !Set.of(Role.LIBRARY, Role.MODULE, Role.LOADER).contains(role)) {
                throw new IllegalArgumentException("Candidate context has an invalid loading role");
            }
        }
    }

    private static void requireUnits(List<File> files, List<Unit> units, int launcher, List<InstallLink> links) {
        reference(launcher, units.size());
        if (units.get(launcher).kind() != Kind.SERVICE
                || units.stream().filter(u -> u.kind() == Kind.SERVICE).count() != 1) {
            throw new IllegalArgumentException("Only the unique launcher service and related targets are approved");
        }
        Map<String, Integer> names = new HashMap<>();
        long memberships = 0;
        for (int i = 0; i < units.size(); i++) {
            Unit unit = units.get(i);
            for (String name : unit.names()) {
                if (names.put(name, i) != null) {
                    throw new IllegalArgumentException("Unit aliases must be globally unique");
                }
            }
            role(files, unit.fragment(), Role.UNIT_FRAGMENT);
            for (int dropIn : unit.dropIns()) {
                role(files, dropIn, Role.UNIT_DROPIN);
            }
            if (unit.service() != null) {
                for (Execution execution : unit.service().execution()) {
                    command(files, execution.command());
                }
            }
            for (Relation relation : unit.relations()) {
                memberships += relation.names().size();
            }
        }
        limit(memberships, MAX_RELATIONS);
        for (Unit unit : units) {
            for (Relation row : unit.relations()) {
                for (String name : row.names()) {
                    Integer index = names.get(name);
                    if (index != null && units.get(index).relations().get(row.family().ordinal() ^ 1).names()
                            .stream().noneMatch(unit.names()::contains)) {
                        throw new IllegalArgumentException("Observed units disagree on reverse manager relation");
                    }
                }
            }
        }
        for (InstallLink link : links) {
            reference(link.targetUnit(), units.size());
            Unit target = units.get(link.targetUnit());
            String leaf = link.path().substring(link.path().lastIndexOf('/') + 1);
            if (!target.names().contains(leaf)) {
                throw new IllegalArgumentException("Install link leaf differs from its target's Names");
            }
            if (link.kind() != LinkKind.UNIT_LOOKUP) {
                reference(link.sourceUnit(), units.size());
                Unit source = units.get(link.sourceUnit());
                String parent = link.path().substring(0, link.path().lastIndexOf('/'));
                String directory = parent.substring(parent.lastIndexOf('/') + 1);
                String suffix = "." + link.kind().name().toLowerCase(java.util.Locale.ROOT);
                Family family = switch (link.kind()) {
                    case WANTS -> Family.WANTS;
                    case REQUIRES -> Family.REQUIRES;
                    case UPHOLDS -> Family.UPHOLDS;
                    case UNIT_LOOKUP -> throw new IllegalArgumentException("Unexpected lookup dependency");
                };
                if (source.kind() != Kind.TARGET || !directory.endsWith(suffix)
                        || !source.names().contains(directory.substring(0, directory.length() - suffix.length()))
                        || source.relations().get(family.ordinal()).names().stream()
                                .noneMatch(target.names()::contains)) {
                    throw new IllegalArgumentException("Install dependency lacks its exact source/manager relation");
                }
            }
        }
    }

    void requireProgram(SegmentClassicLaunchProgram child, String recipeId) throws IOException {
        File image = files.get(provider.image());
        String imagePath = child.javaHome().toString() + "/lib/modules";
        var original = child.files().stream().filter(f -> f.path().toString().equals(imagePath)).findFirst();
        if (!child.os().equals("linux") || !child.architecture().equals("x86_64")
                || !provider.runtime().equals(child.runtime()) || !provider.vendor().equals(child.vendor())
                || !image.path().equals(imagePath) || original.isEmpty()
                || (original.get().roles() & SegmentClassicLaunchProgram.JDK) == 0
                || image.length() != original.get().length() || !image.sha256().equals(original.get().sha256())) {
            throw new IOException("Provider must bind the original child JDK runtime/vendor/image");
        }
        for (var member : child.files()) {
            String path = member.path().toString();
            if (!resolve(path, loading.aliases()).equals(path)) {
                throw new IOException("System aliases cannot relax the original child file contract");
            }
            for (File file : files) {
                if (file.path().equals(path)
                        && (file.length() != member.length() || !file.sha256().equals(member.sha256()))) {
                    throw new IOException("Parent and child approve conflicting bytes for the same file");
                }
            }
        }
        Command last = startChain.get(startChain.size() - 1);
        if (!files.get(last.executable()).path().equals(child.files().get(child.javaFile()).path().toString())
                || !last.arguments().equals(SegmentClassicLauncherInputs.parentCommand(child, recipeId))) {
            throw new IOException("Startup chain must end at the original child JDK executable");
        }
    }

    SegmentClassicHandoff.Digest identity() throws IOException {
        return SegmentClassicHandoff.digest(DOMAIN, encode());
    }

    byte[] encode() throws IOException {
        try {
            ByteBuffer b = ByteBuffer.allocate(SegmentClassicLinuxDeployment.MAX_BYTES);
            b.putLong(MAGIC).putShort((short) 2).putShort((short) 0).putInt(0);
            putText(b, platform.profile());
            for (String value : List.of(platform.kernelRelease(), platform.kernelVersion(), platform.systemdVersion(),
                    platform.systemctlVersion(), platform.loaderVersion())) {
                putText(b, value);
            }
            putIndex(b, platform.showFormat());
            putIndex(b, platform.elfFormat());
            putIndex(b, files.size());
            for (File file : files) {
                putText(b, file.path());
                putIndex(b, file.role().ordinal() + 1);
                b.putLong(file.length()).put(file.sha256().value());
            }
            putIndex(b, provider.rule());
            putText(b, provider.runtime());
            putText(b, provider.vendor());
            putIndex(b, provider.image());
            putIndex(b, loading.systemctl());
            putIndex(b, loading.loader());
            putText(b, loading.interpreter());
            putIndex(b, loading.aliases().size());
            for (Alias alias : loading.aliases()) {
                putText(b, alias.link());
                putText(b, alias.target());
            }
            putIndex(b, loading.directories().size());
            for (Directory directory : loading.directories()) {
                putText(b, directory.path());
                b.put((byte) (directory.present() ? 1 : 0));
            }
            putIndex(b, loading.configuration().size());
            for (Configuration entry : loading.configuration()) {
                putText(b, entry.path());
                putIndex(b, entry.file() == -1 ? 0xffff : entry.file());
                putIndex(b, entry.kind().ordinal() + 1);
                putIndex(b, entry.format());
            }
            LoaderPolicy policy = loading.policy();
            for (int rule : new int[]{policy.elf(), policy.cache(), policy.comparator(), policy.paths(),
                    policy.configuration(), policy.glob(), policy.relevance(), policy.hwcaps(), policy.tokens(),
                    policy.diagnostics()}) {
                putIndex(b, rule);
            }
            putText(b, policy.libToken());
            putStrings(b, policy.defaultDirectories());
            putIndex(b, loading.elf().size());
            for (Elf entry : loading.elf()) {
                putIndex(b, entry.file());
                putStrings(b, entry.needed());
                for (DynamicPath value : List.of(entry.rpath(), entry.runpath())) {
                    b.put((byte) (value.present() ? 1 : 0));
                    putText(b, value.value());
                }
                b.putLong(entry.flags()).putLong(entry.flags1());
            }
            putIndex(b, loading.contexts().size());
            for (Context context : loading.contexts()) {
                putIndex(b, context.file());
                putText(b, context.logicalLoadName());
            }
            putIndex(b, loading.needed().size());
            for (NeededRule rule : loading.needed()) {
                putIndex(b, rule.requester());
                putIndex(b, rule.ordinal());
                putReferences(b, rule.candidates());
            }
            putIndex(b, loading.modules().size());
            for (ModuleRequest request : loading.modules()) {
                putIndex(b, request.requester());
                putText(b, request.name());
                putReferences(b, request.candidates());
            }
            putIndex(b, startChain.size());
            for (Command command : startChain) {
                putCommand(b, command);
            }
            putIndex(b, units.size());
            putIndex(b, launcherUnit);
            for (Unit unit : units) {
                putIndex(b, unit.kind().ordinal() + 1);
                putText(b, unit.id());
                putStrings(b, unit.names());
                putText(b, unit.fileState());
                putIndex(b, unit.fragment());
                putReferences(b, unit.dropIns());
                putIndex(b, unit.commonRule());
                putIndex(b, unit.relations().size());
                for (Relation relation : unit.relations()) {
                    putIndex(b, relation.family().ordinal() + 1);
                    putStrings(b, relation.names());
                }
                if (unit.kind() == Kind.SERVICE) {
                    b.putInt((int) unit.service().group());
                    putIndex(b, unit.service().constraint());
                    putIndex(b, unit.service().execution().size());
                    for (Execution execution : unit.service().execution()) {
                        putIndex(b, execution.phase().ordinal() + 1);
                        putCommand(b, execution.command());
                    }
                }
            }
            putIndex(b, installLinks.size());
            for (InstallLink link : installLinks) {
                putIndex(b, link.kind().ordinal() + 1);
                putText(b, link.path());
                putText(b, link.target());
                putIndex(b, link.sourceUnit() == -1 ? 0xffff : link.sourceUnit());
                putIndex(b, link.targetUnit());
            }
            putIndex(b, isolation.dropper());
            putIndex(b, isolation.dropperAbi());
            b.put(isolation.filter().value());
            putIndex(b, isolation.filterFormat());
            putIndex(b, isolation.credentialRule());
            putIndex(b, isolation.cgroupRule());
            putIndex(b, operations.endpointRule());
            b.putInt((int) operations.uid());
            putIndex(b, operations.protocol());
            b.putInt(12, b.position() - 16);
            return Arrays.copyOf(b.array(), b.position());
        } catch (BufferOverflowException | IllegalArgumentException malformed) {
            throw new IOException("Schema two deployment exceeds canonical encoding bounds", malformed);
        }
    }

    static SegmentClassicLinuxDeploymentV2 decode(byte[] encoded) throws IOException {
        if (encoded == null || encoded.length < 16 || encoded.length > SegmentClassicLinuxDeployment.MAX_BYTES) {
            throw new IOException("Schema two deployment exceeds framing bounds");
        }
        try {
            ByteBuffer b = ByteBuffer.wrap(encoded.clone());
            if (b.getLong() != MAGIC || index(b) != 2 || index(b) != 0 || b.getInt() != encoded.length - 16) {
                throw new IllegalArgumentException("Unknown schema two deployment framing");
            }
            var budget = new DecodeBudget();
            var platform = new Platform(getText(b), getText(b), getText(b), getText(b), getText(b), getText(b),
                    index(b), index(b));
            int fileCount = count(b, 1024, 44);
            List<File> files = new ArrayList<>(fileCount);
            for (int i = 0; i < fileCount; i++) {
                files.add(new File(getText(b), Role.values()[tag(b, Role.values().length) - 1],
                        b.getLong(), digest(b)));
            }
            var provider = new Provider(index(b), getText(b), getText(b), index(b));
            int systemctl = index(b);
            int loader = index(b);
            String interpreter = getText(b);
            int aliasCount = count(b, 128, 6);
            List<Alias> aliases = new ArrayList<>(aliasCount);
            for (int i = 0; i < aliasCount; i++) {
                aliases.add(new Alias(getText(b), getText(b)));
            }
            int directoryCount = count(b, 64, 4);
            List<Directory> directories = new ArrayList<>(directoryCount);
            for (int i = 0; i < directoryCount; i++) {
                directories.add(new Directory(getText(b), presence(b)));
            }
            int configurationCount = count(b, 1024, 9);
            List<Configuration> configuration = new ArrayList<>(configurationCount);
            for (int i = 0; i < configurationCount; i++) {
                configuration.add(new Configuration(getText(b), optional(b),
                        ConfigurationKind.values()[tag(b, ConfigurationKind.values().length) - 1], index(b)));
            }
            var policy = new LoaderPolicy(index(b), index(b), index(b), index(b), index(b), index(b), index(b),
                    index(b), index(b), index(b), getText(b), getStrings(b, 64));
            int elfCount = count(b, 1024, 26);
            List<Elf> elf = new ArrayList<>(elfCount);
            for (int i = 0; i < elfCount; i++) {
                int file = index(b);
                int neededCount = count(b, MAX_REQUESTS, 3);
                budget.metadata(neededCount);
                List<String> needed = readStrings(b, neededCount);
                elf.add(new Elf(file, needed, new DynamicPath(presence(b), getText(b)),
                        new DynamicPath(presence(b), getText(b)), b.getLong(), b.getLong()));
            }
            int contextCount = count(b, 4096, 5);
            List<Context> contexts = new ArrayList<>(contextCount);
            for (int i = 0; i < contextCount; i++) {
                contexts.add(new Context(index(b), getText(b)));
            }
            int neededCount = count(b, MAX_REQUESTS, 6);
            budget.requests(neededCount);
            List<NeededRule> needed = new ArrayList<>(neededCount);
            for (int i = 0; i < neededCount; i++) {
                needed.add(new NeededRule(index(b), index(b), getCandidates(b, budget)));
            }
            int moduleCount = count(b, MAX_REQUESTS, 7);
            budget.requests(moduleCount);
            List<ModuleRequest> modules = new ArrayList<>(moduleCount);
            for (int i = 0; i < moduleCount; i++) {
                modules.add(new ModuleRequest(index(b), getText(b), getCandidates(b, budget)));
            }
            var loading = new Loading(systemctl, loader, interpreter, aliases, directories, configuration, policy,
                    elf, contexts, needed, modules);
            int chainCount = count(b, 32, 7);
            List<Command> chain = new ArrayList<>(chainCount);
            for (int i = 0; i < chainCount; i++) {
                chain.add(getCommand(b));
            }
            int unitCount = count(b, 32, 8);
            int launcher = index(b);
            List<Unit> units = new ArrayList<>(unitCount);
            for (int i = 0; i < unitCount; i++) {
                Kind kind = Kind.values()[tag(b, Kind.values().length) - 1];
                String id = getText(b);
                List<String> names = getStrings(b, 32);
                String fileState = getText(b);
                int fragment = index(b);
                List<Integer> dropIns = getReferences(b, 32);
                int common = index(b);
                int relationCount = count(b, Family.values().length, 4);
                List<Relation> relations = new ArrayList<>(relationCount);
                for (int j = 0; j < relationCount; j++) {
                    Family family = Family.values()[tag(b, Family.values().length) - 1];
                    int nameCount = count(b, 1024, 3);
                    budget.relations(nameCount);
                    relations.add(new Relation(family, readStrings(b, nameCount)));
                }
                Service service = null;
                if (kind == Kind.SERVICE) {
                    long group = Integer.toUnsignedLong(b.getInt());
                    int constraint = index(b);
                    int executionCount = count(b, 32, 9);
                    List<Execution> execution = new ArrayList<>(executionCount);
                    for (int j = 0; j < executionCount; j++) {
                        execution.add(new Execution(Phase.values()[tag(b, Phase.values().length) - 1], getCommand(b)));
                    }
                    service = new Service(group, execution, constraint);
                }
                units.add(new Unit(kind, id, names, fileState, fragment, dropIns, common, relations, service));
            }
            int linkCount = count(b, 1024, 12);
            List<InstallLink> links = new ArrayList<>(linkCount);
            for (int i = 0; i < linkCount; i++) {
                links.add(new InstallLink(LinkKind.values()[tag(b, LinkKind.values().length) - 1], getText(b),
                        getText(b), optional(b), index(b)));
            }
            var isolation = new Isolation(index(b), index(b), digest(b), index(b), index(b), index(b));
            var operations = new Operations(index(b), Integer.toUnsignedLong(b.getInt()), index(b));
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("Trailing schema two deployment bytes");
            }
            var result = new SegmentClassicLinuxDeploymentV2(platform, files, provider, loading, chain, units, launcher,
                    links, isolation, operations);
            if (!Arrays.equals(encoded, result.encode())) {
                throw new IllegalArgumentException("Noncanonical schema two deployment bytes");
            }
            return result;
        } catch (IllegalArgumentException | ArithmeticException | BufferUnderflowException malformed) {
            throw new IOException("Invalid schema two deployment", malformed);
        }
    }

    /** All aggregate counts are charged before allocating the corresponding list, including repeated references. */
    private static final class DecodeBudget {
        private long metadata;
        private long requests;
        private long candidates;
        private long relations;

        void metadata(int count) {
            metadata += count;
            limit(metadata, MAX_REQUESTS);
        }

        void requests(int count) {
            requests += count;
            limit(requests, MAX_REQUESTS);
        }

        void candidates(int count) {
            candidates += count;
            limit(candidates, MAX_CANDIDATES);
        }

        void relations(int count) {
            relations += count;
            limit(relations, MAX_RELATIONS);
        }
    }

    private static void putIndex(ByteBuffer b, int value) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException("Schema two index exceeds unsigned short");
        }
        b.putShort((short) value);
    }

    private static void putText(ByteBuffer b, String value) {
        byte[] encoded = utf8(value, true);
        putIndex(b, encoded.length);
        b.put(encoded);
    }

    private static void putStrings(ByteBuffer b, List<String> values) {
        putIndex(b, values.size());
        for (String value : values) {
            putText(b, value);
        }
    }

    private static void putReferences(ByteBuffer b, List<Integer> values) {
        putIndex(b, values.size());
        for (int value : values) {
            putIndex(b, value);
        }
    }

    private static void putCommand(ByteBuffer b, Command command) {
        putIndex(b, command.executable());
        putStrings(b, command.arguments());
    }

    private static int index(ByteBuffer b) {
        return Short.toUnsignedInt(b.getShort());
    }

    private static int optional(ByteBuffer b) {
        int index = index(b);
        return index == 0xffff ? -1 : index;
    }

    private static int tag(ByteBuffer b, int maximum) {
        int value = index(b);
        if (value < 1 || value > maximum) {
            throw new IllegalArgumentException("Unknown schema two tag");
        }
        return value;
    }

    private static boolean presence(ByteBuffer b) {
        int value = Byte.toUnsignedInt(b.get());
        if (value > 1) {
            throw new IllegalArgumentException("Unknown schema two presence flag");
        }
        return value == 1;
    }

    private static int count(ByteBuffer b, int maximum, int minimumBytes) {
        int count = index(b);
        if (count > maximum || (long) count * minimumBytes > b.remaining()) {
            throw new IllegalArgumentException("Invalid schema two bounded count");
        }
        return count;
    }

    private static String getText(ByteBuffer b) throws CharacterCodingException {
        int length = count(b, 4096, 1);
        byte[] value = new byte[length];
        b.get(value);
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
        utf8(text, true);
        return text;
    }

    private static List<String> readStrings(ByteBuffer b, int size) throws CharacterCodingException {
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(getText(b));
        }
        return values;
    }

    private static List<String> getStrings(ByteBuffer b, int maximum) throws CharacterCodingException {
        return readStrings(b, count(b, maximum, 2));
    }

    private static List<Integer> readReferences(ByteBuffer b, int size) {
        List<Integer> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(index(b));
        }
        return values;
    }

    private static List<Integer> getReferences(ByteBuffer b, int maximum) {
        return readReferences(b, count(b, maximum, 2));
    }

    private static List<Integer> getCandidates(ByteBuffer b, DecodeBudget budget) {
        int size = count(b, 4096, 2);
        budget.candidates(size);
        return readReferences(b, size);
    }

    private static Command getCommand(ByteBuffer b) throws CharacterCodingException {
        return new Command(index(b), getStrings(b, 256));
    }

    private static SegmentClassicHandoff.Digest digest(ByteBuffer b) {
        byte[] value = new byte[32];
        b.get(value);
        return new SegmentClassicHandoff.Digest(value);
    }

    /** Syntactic declared substitution only. The actual owner must verify every lookup, type and raw link byte. */
    static String resolve(String name, List<Alias> aliases) {
        logical(name);
        var pending = new ArrayDeque<String>();
        prepend(pending, name);
        var components = new ArrayList<String>();
        int followed = 0;
        while (!pending.isEmpty()) {
            String component = pending.removeFirst();
            if (component.equals(".")) {
                continue;
            }
            if (component.equals("..")) {
                if (!components.isEmpty()) {
                    components.remove(components.size() - 1);
                }
                continue;
            }
            String candidate = "/" + (components.isEmpty() ? "" : String.join("/", components) + "/") + component;
            Alias selected = null;
            for (Alias alias : aliases) {
                if (alias.link().equals(candidate)) {
                    selected = alias;
                    break;
                }
            }
            if (selected == null) {
                components.add(component);
            } else {
                if (++followed > SegmentClassicLinuxDeployment.MAX_ALIAS_FOLLOWS) {
                    throw new IllegalArgumentException("Excessive declared loading alias expansion");
                }
                if (selected.target().startsWith("/")) {
                    components.clear();
                }
                prepend(pending, selected.target());
                text("/" + String.join("/", components) + "/" + String.join("/", pending));
            }
        }
        return "/" + String.join("/", components);
    }

    private static void prepend(ArrayDeque<String> pending, String target) {
        String[] pieces = target.split("/", -1);
        for (int i = pieces.length - 1; i >= 0; i--) {
            if (!pieces[i].isEmpty()) {
                pending.addFirst(pieces[i]);
            } else if (i == pieces.length - 1) {
                pending.addFirst(".");
            }
        }
    }

    static boolean beneath(String path, String directory) {
        return path.equals(directory) || path.startsWith(directory.equals("/") ? "/" : directory + "/");
    }

    private static void command(List<File> files, Command command) {
        role(files, command.executable(), Role.START_CHAIN);
        if (!command.arguments().get(0).equals(files.get(command.executable()).path())) {
            throw new IllegalArgumentException("Command must retain its exact executable argv");
        }
    }

    private static void role(List<File> files, int index, Role role) {
        reference(index, files.size());
        if (files.get(index).role() != role) {
            throw new IllegalArgumentException("Schema two file reference has another role");
        }
    }

    private static void reference(int index, int size) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Dangling schema two reference");
        }
    }

    private static void one(int rule) {
        if (rule != 1) {
            throw new IllegalArgumentException("Unknown fixed schema two rule");
        }
    }

    private static void limit(long value, int maximum) {
        if (value > maximum) {
            throw new IllegalArgumentException("Schema two aggregate capacity exceeded");
        }
    }

    private static <T> List<T> bounded(List<T> values, int maximum) {
        limit(values.size(), maximum);
        return List.copyOf(values);
    }

    private static List<Integer> references(List<Integer> values, int maximum, boolean ordered) {
        var result = bounded(values, maximum);
        if (new HashSet<>(result).size() != result.size() || result.stream().anyMatch(i -> i < 0)
                || ordered && result.isEmpty()) {
            throw new IllegalArgumentException("Invalid, duplicate or empty references");
        }
        if (ordered) {
            ascending(result);
        }
        return result;
    }

    private static void ascending(List<Integer> values) {
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) < 0 || i > 0 && values.get(i - 1) >= values.get(i)) {
                throw new IllegalArgumentException("Unique ascending references required");
            }
        }
    }

    private static int compare(String left, String right) {
        return Arrays.compareUnsigned(utf8(left, true), utf8(right, true));
    }

    private static void ordered(List<String> values) {
        for (int i = 1; i < values.size(); i++) {
            if (compare(values.get(i - 1), values.get(i)) >= 0) {
                throw new IllegalArgumentException("Unique UTF8 byte order required");
            }
        }
    }

    private static void unitName(String name) {
        text(name);
        if (name.length() > 255 || !name.matches("(?:[A-Za-z0-9:_.@-]|\\\\x[0-9A-Fa-f]{2})+"
                + "\\.(?:service|target|socket|timer|path|mount|automount|swap|slice|scope|device)")) {
            throw new IllegalArgumentException("Unsupported systemd unit name");
        }
    }

    private static void logical(String value) {
        text(value);
        if (!value.startsWith("/")) {
            throw new IllegalArgumentException("Absolute raw Linux path required");
        }
    }

    /** Raw canonical syntax only; does not construct a Path, observe a provider or establish source authority. */
    static void physical(String value) {
        logical(value);
        if (value.equals("/") || value.endsWith("/") || value.contains("//")
                || Arrays.stream(value.substring(1).split("/", -1)).anyMatch(s -> s.equals(".") || s.equals(".."))) {
            throw new IllegalArgumentException("Canonical physical Linux path syntax required");
        }
    }

    private static void text(String value) {
        utf8(value, false);
    }

    private static byte[] utf8(String value, boolean empty) {
        if (value == null || !empty && value.isEmpty() || value.length() > 4096 || value.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Invalid raw schema two text");
        }
        try {
            ByteBuffer b = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
            limit(b.remaining(), 4096);
            byte[] result = new byte[b.remaining()];
            b.get(result);
            return result;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("Malformed raw schema two UTF8", malformed);
        }
    }
}

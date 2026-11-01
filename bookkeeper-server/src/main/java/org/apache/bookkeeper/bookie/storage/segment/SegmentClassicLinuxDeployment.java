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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Approved Linux input data only. No boot, invocation, live process, drain or isolation observation is stored. */
final class SegmentClassicLinuxDeployment {
    private final Platform platform;
    private final List<File> files;
    private final Loading loading;
    private final List<Command> startChain;
    private final List<Unit> units;
    private final int launcherUnit;
    private final Isolation isolation;
    private final Operations operations;
    private final SegmentClassicLinuxDeploymentV2 versionTwo;
    static final int MAX_BYTES = 1024 * 1024;
    static final int MAX_ALIAS_FOLLOWS = 40;
    static final String PROFILE = "linux-x86_64-systemd-cgroup2-ext4-v1";
    private static final long MAGIC = 0x5557434c44303031L; // UWCLD001
    private static final byte[] DOMAIN = "BookKeeper/ControlledClassicLinuxDeployment/v1\0"
            .getBytes(StandardCharsets.US_ASCII);

    enum Role {
        SYSTEMCTL, LOADER, LIBRARY, MODULE, LOADER_CONFIG, LOADER_CACHE, UNIT_FRAGMENT, UNIT_DROPIN,
        START_CHAIN, DROPPER, JDK_RUNTIME;

        boolean elf() {
            return this == SYSTEMCTL || this == LOADER || this == LIBRARY || this == MODULE
                    || this == START_CHAIN || this == DROPPER;
        }
    }

    record Platform(String profile, String kernelRelease, String kernelVersion, String systemdVersion,
                    String systemctlVersion, String loaderVersion, int showFormat, int elfFormat) {
        Platform {
            if (!PROFILE.equals(profile) || showFormat != 1 || elfFormat != 1) {
                throw new IllegalArgumentException("Unknown fixed Linux platform or parser format");
            }
            for (String value : List.of(kernelRelease, kernelVersion, systemdVersion, systemctlVersion,
                    loaderVersion)) {
                text(value);
            }
        }
    }

    record File(Path path, Role role, long length, SegmentClassicHandoff.Digest sha256) {
        File {
            absolute(path);
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(sha256, "sha256");
            if (length < 0 || role.elf() && length < 64) {
                throw new IllegalArgumentException("Invalid approved Linux file length");
            }
        }
    }

    /** Exact readlink bytes, including an explicitly approved relative target; never a general file alias rule. */
    record Alias(Path link, String target) {
        Alias {
            absolute(link);
            text(target);
        }
    }

    record Directory(Path path, boolean present) {
        Directory {
            absolute(path);
        }
    }

    /** Index -1 means this exact configuration/cache path must actually be absent; no missing-value fallback. */
    record Requirement(Path path, int file) {
        Requirement {
            absolute(path);
            if (file < -1) {
                throw new IllegalArgumentException("Invalid optional Linux file reference");
            }
        }
    }

    record Needed(String soname, int file) {
        Needed {
            text(soname);
            if (soname.length() > 255 || !soname.matches("[A-Za-z0-9_.+-]+")) {
                throw new IllegalArgumentException("Unsupported ELF dependency name");
            }
        }
    }

    record Elf(int file, List<Needed> needed, List<Integer> rpath, List<Integer> runpath) {
        Elf {
            needed = bounded(needed, 1024);
            rpath = indices(rpath, 64);
            runpath = indices(runpath, 64);
            ordered(needed.stream().map(Needed::soname).toList());
        }
    }

    record Loading(int systemctl, int loader, Path interpreter, List<Alias> aliases, List<Directory> directories,
                   List<Requirement> configuration, List<Elf> elf, List<Integer> modules) {
        Loading {
            absolute(interpreter);
            aliases = bounded(aliases, 128);
            directories = bounded(directories, 64);
            configuration = bounded(configuration, 1024);
            elf = bounded(elf, 1024);
            modules = indices(modules, 1024);
            ordered(aliases.stream().map(a -> a.link().toString()).toList());
            ordered(directories.stream().map(d -> d.path().toString()).toList());
            ordered(configuration.stream().map(c -> c.path().toString()).toList());
            orderedIndices(elf.stream().map(Elf::file).toList());
            orderedIndices(modules);
        }
    }

    record Command(int executable, List<String> arguments) {
        Command {
            arguments = bounded(arguments, 256);
            if (arguments.isEmpty()) {
                throw new IllegalArgumentException("An explicit executable argument is required");
            }
            arguments.forEach(SegmentClassicLinuxDeployment::text);
        }
    }

    enum Phase { CONDITION, START_PRE, START, START_POST, RELOAD, STOP, STOP_POST }

    /** Format one has no systemd execution prefixes; unknown Ex flags must be rejected by the actual observer. */
    record Execution(Phase phase, Command command) {
        Execution {
            Objects.requireNonNull(phase, "phase");
            Objects.requireNonNull(command, "command");
        }
    }

    /** Constraint one requires UID zero, the declared GID, Type=exec, DynamicUser=no and Restart=no. */
    record Unit(String name, String fileState, long group, int fragment, List<Integer> dropIns,
                List<Execution> execution,
                List<Integer> triggers, List<Integer> triggeredBy, List<Integer> onFailure, List<Integer> onSuccess,
                int constraintFormat) {
        Unit {
            SegmentClassicSystemdQuery.command(name);
            if (group < 0 || group >= 0xffff_ffffL || constraintFormat != 1
                    || !Set.of("enabled", "enabled-runtime", "disabled", "static", "indirect",
                    "linked", "linked-runtime").contains(fileState)) {
                throw new IllegalArgumentException("Unknown fixed service constraint format");
            }
            dropIns = indices(dropIns, 32);
            execution = bounded(execution, 32);
            triggers = orderedReferences(triggers, 32);
            triggeredBy = orderedReferences(triggeredBy, 32);
            onFailure = orderedReferences(onFailure, 32);
            onSuccess = orderedReferences(onSuccess, 32);
            int previous = -1;
            for (Execution entry : execution) {
                if (entry.phase().ordinal() < previous) {
                    throw new IllegalArgumentException("Approved service phases must be in their fixed order");
                }
                previous = entry.phase().ordinal();
            }
            if (execution.stream().filter(e -> e.phase() == Phase.START).count() != 1) {
                throw new IllegalArgumentException("Type=exec requires one approved start command");
            }
        }
    }

    record Isolation(int dropper, int dropperAbi, SegmentClassicHandoff.Digest filter, int filterFormat,
                     int credentialRule, int cgroupRule) {
        Isolation {
            Objects.requireNonNull(filter, "filter");
            if (dropperAbi != 2 || filterFormat != 1 || credentialRule != 1 || cgroupRule != 1) {
                throw new IllegalArgumentException("Unknown fixed dropper, filter, credential or cgroup rule");
            }
        }
    }

    record Operations(int endpointRule, long uid, int protocol) {
        Operations {
            if (endpointRule != 1 || uid != 0 || protocol != 1) {
                throw new IllegalArgumentException("Only the fixed root operation endpoint is supported");
            }
        }
    }

    SegmentClassicLinuxDeployment(Platform platform, List<File> files, Loading loading,
            List<Command> startChain, List<Unit> units, int launcherUnit, Isolation isolation, Operations operations) {
        Objects.requireNonNull(platform, "platform");
        Objects.requireNonNull(loading, "loading");
        Objects.requireNonNull(isolation, "isolation");
        Objects.requireNonNull(operations, "operations");
        files = bounded(files, 1024);
        units = bounded(units, 32);
        startChain = bounded(startChain, 32);
        if (files.isEmpty() || units.isEmpty() || startChain.isEmpty()) {
            throw new IllegalArgumentException("Complete files, units and original startup chain are required");
        }
        ordered(files.stream().map(f -> f.path().toString()).toList());
        ordered(units.stream().map(Unit::name).toList());
        reference(launcherUnit, units.size());
        role(files, loading.systemctl(), Role.SYSTEMCTL);
        if (!files.get(loading.systemctl()).path().equals(Path.of("/usr/bin/systemctl"))) {
            throw new IllegalArgumentException("Only the fixed systemctl path is supported");
        }
        role(files, loading.loader(), Role.LOADER);
        role(files, isolation.dropper(), Role.DROPPER);
        long bytes = 0;
        for (File file : files) {
            if (file.role() == Role.JDK_RUNTIME) {
                throw new IllegalArgumentException("The JDK runtime image role requires deployment schema two");
            }
            bytes = Math.addExact(bytes, file.length());
        }
        requireLoading(files, loading);
        for (Command command : startChain) {
            requireCommand(files, command);
        }
        for (int i = 1; i < startChain.size(); i++) {
            List<String> previous = startChain.get(i - 1).arguments();
            List<String> next = startChain.get(i).arguments();
            if (previous.size() <= next.size() || !previous.subList(previous.size() - next.size(), previous.size())
                    .equals(next)) {
                throw new IllegalArgumentException("Approved exec startup chain must preserve the exact next argv");
            }
        }
        for (Unit unit : units) {
            role(files, unit.fragment(), Role.UNIT_FRAGMENT);
            for (int ref : unit.dropIns()) {
                role(files, ref, Role.UNIT_DROPIN);
            }
            for (Execution execution : unit.execution()) {
                requireCommand(files, execution.command());
            }
            for (List<Integer> refs : List.of(unit.triggers(), unit.triggeredBy(), unit.onFailure(),
                    unit.onSuccess())) {
                for (int index : refs) {
                    reference(index, units.size());
                }
            }
        }
        List<Command> starts = units.get(launcherUnit).execution().stream()
                .filter(e -> e.phase() == Phase.START).map(Execution::command).toList();
        if (!starts.equals(List.of(startChain.get(0)))) {
            throw new IllegalArgumentException("Original unit must name the exact first startup command once");
        }
        this.platform = platform;
        this.files = files;
        this.loading = loading;
        this.startChain = startChain;
        this.units = units;
        this.launcherUnit = launcherUnit;
        this.isolation = isolation;
        this.operations = operations;
        this.versionTwo = null;
    }

    SegmentClassicLinuxDeployment(SegmentClassicLinuxDeploymentV2 versionTwo) {
        this.versionTwo = Objects.requireNonNull(versionTwo, "versionTwo");
        platform = null;
        files = null;
        loading = null;
        startChain = null;
        units = null;
        launcherUnit = -1;
        isolation = null;
        operations = null;
    }

    int schema() {
        return versionTwo == null ? 1 : 2;
    }

    SegmentClassicLinuxDeploymentV2 versionTwo() {
        if (versionTwo == null) {
            throw new IllegalStateException("Deployment schema one has no schema two approval data");
        }
        return versionTwo;
    }

    private void requireVersionOne() {
        if (versionTwo != null) {
            throw new IllegalStateException("Deployment schema two requires its typed consumer");
        }
    }

    Platform platform() {
        requireVersionOne();
        return platform;
    }

    List<File> files() {
        requireVersionOne();
        return files;
    }

    Loading loading() {
        requireVersionOne();
        return loading;
    }

    List<Command> startChain() {
        requireVersionOne();
        return startChain;
    }

    List<Unit> units() {
        requireVersionOne();
        return units;
    }

    int launcherUnit() {
        requireVersionOne();
        return launcherUnit;
    }

    Isolation isolation() {
        requireVersionOne();
        return isolation;
    }

    Operations operations() {
        requireVersionOne();
        return operations;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SegmentClassicLinuxDeployment deployment)) {
            return false;
        }
        if (versionTwo != null || deployment.versionTwo != null) {
            return Objects.equals(versionTwo, deployment.versionTwo);
        }
        return platform.equals(deployment.platform)
                && files.equals(deployment.files) && loading.equals(deployment.loading)
                && startChain.equals(deployment.startChain) && units.equals(deployment.units)
                && launcherUnit == deployment.launcherUnit && isolation.equals(deployment.isolation)
                && operations.equals(deployment.operations);
    }

    @Override
    public int hashCode() {
        return versionTwo == null
                ? Objects.hash(platform, files, loading, startChain, units, launcherUnit, isolation, operations)
                : versionTwo.hashCode();
    }

    void requireProgram(SegmentClassicLaunchProgram child, String recipeId) throws IOException {
        if (versionTwo != null) {
            versionTwo.requireProgram(child, recipeId);
            return;
        }
        if (!child.os().equals("linux") || !child.architecture().equals("x86_64")) {
            throw new IOException("Linux deployment differs from the original child platform");
        }
        for (File file : files) {
            for (var member : child.files()) {
                if (file.path().equals(member.path())
                        && (file.length() != member.length() || !file.sha256().equals(member.sha256()))) {
                    throw new IOException("Parent and child approve conflicting bytes for the same file");
                }
            }
        }
        for (var member : child.files()) {
            if (!resolve(member.path(), loading.aliases()).equals(member.path())) {
                throw new IOException("Linux system aliases cannot relax the original child file contract");
            }
        }
        Command last = startChain.get(startChain.size() - 1);
        if (!files.get(last.executable()).path().equals(child.files().get(child.javaFile()).path())
                || !last.arguments().equals(SegmentClassicLauncherInputs.parentCommand(child, recipeId))) {
            throw new IOException("Approved startup chain must end at the original child JDK executable");
        }
    }

    SegmentClassicHandoff.Digest identity() throws IOException {
        return versionTwo == null ? SegmentClassicHandoff.digest(DOMAIN, encode()) : versionTwo.identity();
    }

    /** Canonical ordered tables and explicit references; the buffer cannot grow past the descriptor capacity. */
    byte[] encode() throws IOException {
        if (versionTwo != null) {
            return versionTwo.encode();
        }
        try {
            ByteBuffer b = ByteBuffer.allocate(MAX_BYTES);
            b.putLong(MAGIC).putShort((short) 1).putShort((short) 0).putInt(0);
            putText(b, platform.profile());
            for (String value : List.of(platform.kernelRelease(), platform.kernelVersion(), platform.systemdVersion(),
                    platform.systemctlVersion(), platform.loaderVersion())) {
                putText(b, value);
            }
            b.putShort((short) platform.showFormat()).putShort((short) platform.elfFormat());
            putCount(b, files.size());
            for (File file : files) {
                putText(b, file.path().toString());
                b.putShort((short) (file.role().ordinal() + 1)).putLong(file.length()).put(file.sha256().value());
            }
            b.putShort((short) loading.systemctl()).putShort((short) loading.loader());
            putText(b, loading.interpreter().toString());
            putCount(b, loading.aliases().size());
            for (Alias alias : loading.aliases()) {
                putText(b, alias.link().toString());
                putText(b, alias.target());
            }
            putCount(b, loading.directories().size());
            for (Directory directory : loading.directories()) {
                putText(b, directory.path().toString());
                b.put((byte) (directory.present() ? 1 : 0));
            }
            putCount(b, loading.configuration().size());
            for (Requirement requirement : loading.configuration()) {
                putText(b, requirement.path().toString());
                b.putShort((short) requirement.file());
            }
            putCount(b, loading.elf().size());
            for (Elf image : loading.elf()) {
                b.putShort((short) image.file());
                putCount(b, image.needed().size());
                for (Needed needed : image.needed()) {
                    putText(b, needed.soname());
                    b.putShort((short) needed.file());
                }
                putIndices(b, image.rpath());
                putIndices(b, image.runpath());
            }
            putIndices(b, loading.modules());
            putCount(b, startChain.size());
            for (Command command : startChain) {
                putCommand(b, command);
            }
            putCount(b, units.size());
            b.putShort((short) launcherUnit);
            for (Unit unit : units) {
                putText(b, unit.name());
                putText(b, unit.fileState());
                b.putInt((int) unit.group());
                b.putShort((short) unit.fragment());
                putIndices(b, unit.dropIns());
                putCount(b, unit.execution().size());
                for (Execution execution : unit.execution()) {
                    b.putShort((short) (execution.phase().ordinal() + 1));
                    putCommand(b, execution.command());
                }
                putIndices(b, unit.triggers());
                putIndices(b, unit.triggeredBy());
                putIndices(b, unit.onFailure());
                putIndices(b, unit.onSuccess());
                b.putShort((short) unit.constraintFormat());
            }
            b.putShort((short) isolation.dropper()).putShort((short) isolation.dropperAbi())
                    .put(isolation.filter().value()).putShort((short) isolation.filterFormat())
                    .putShort((short) isolation.credentialRule()).putShort((short) isolation.cgroupRule());
            b.putShort((short) operations.endpointRule()).putInt((int) operations.uid())
                    .putShort((short) operations.protocol());
            b.putInt(12, b.position() - 16);
            return Arrays.copyOf(b.array(), b.position());
        } catch (BufferOverflowException | IllegalArgumentException malformed) {
            throw new IOException("Linux deployment exceeds its canonical encoding bounds", malformed);
        }
    }

    static SegmentClassicLinuxDeployment decode(byte[] encoded) throws IOException {
        if (encoded == null || encoded.length < 16 || encoded.length > MAX_BYTES) {
            throw new IOException("Linux deployment exceeds its fixed bounds");
        }
        if (ByteBuffer.wrap(encoded).getLong() == SegmentClassicLinuxDeploymentV2.MAGIC) {
            return new SegmentClassicLinuxDeployment(SegmentClassicLinuxDeploymentV2.decode(encoded));
        }
        try {
            ByteBuffer b = ByteBuffer.wrap(encoded.clone());
            if (b.getLong() != MAGIC || b.getShort() != 1 || b.getShort() != 0
                    || b.getInt() != encoded.length - 16) {
                throw new IllegalArgumentException("Unknown Linux deployment framing");
            }
            var platform = new Platform(getText(b), getText(b), getText(b), getText(b), getText(b), getText(b),
                    index(b), index(b));
            int fileCount = count(b, 1024, 44);
            List<File> files = new ArrayList<>(fileCount);
            for (int i = 0; i < fileCount; i++) {
                Path path = Path.of(getText(b));
                int role = tag(b, 10);
                long length = b.getLong();
                files.add(new File(path, Role.values()[role - 1], length, digest(b)));
            }
            int systemctl = index(b);
            int loader = index(b);
            Path interpreter = Path.of(getText(b));
            int aliasesCount = count(b, 128, 6);
            List<Alias> aliases = new ArrayList<>(aliasesCount);
            for (int i = 0; i < aliasesCount; i++) {
                aliases.add(new Alias(Path.of(getText(b)), getText(b)));
            }
            int directoryCount = count(b, 64, 4);
            List<Directory> directories = new ArrayList<>(directoryCount);
            for (int i = 0; i < directoryCount; i++) {
                Path path = Path.of(getText(b));
                byte present = b.get();
                if (present != 0 && present != 1) {
                    throw new IllegalArgumentException("Unknown directory presence requirement");
                }
                directories.add(new Directory(path, present == 1));
            }
            int configurationCount = count(b, 1024, 5);
            List<Requirement> configuration = new ArrayList<>(configurationCount);
            for (int i = 0; i < configurationCount; i++) {
                Path path = Path.of(getText(b));
                int ref = index(b);
                configuration.add(new Requirement(path, ref == 0xffff ? -1 : ref));
            }
            int elfCount = count(b, 1024, 8);
            List<Elf> elf = new ArrayList<>(elfCount);
            for (int i = 0; i < elfCount; i++) {
                int ref = index(b);
                int neededCount = count(b, 1024, 5);
                List<Needed> needed = new ArrayList<>(neededCount);
                for (int j = 0; j < neededCount; j++) {
                    needed.add(new Needed(getText(b), index(b)));
                }
                elf.add(new Elf(ref, needed, getIndices(b, 64), getIndices(b, 64)));
            }
            var loading = new Loading(systemctl, loader, interpreter, aliases, directories, configuration, elf,
                    getIndices(b, 1024));
            int chainCount = count(b, 32, 7);
            List<Command> chain = new ArrayList<>(chainCount);
            for (int i = 0; i < chainCount; i++) {
                chain.add(getCommand(b));
            }
            int unitCount = count(b, 32, 19);
            int launcher = index(b);
            List<Unit> units = new ArrayList<>(unitCount);
            for (int i = 0; i < unitCount; i++) {
                String name = getText(b);
                String fileState = getText(b);
                long group = Integer.toUnsignedLong(b.getInt());
                int fragment = index(b);
                List<Integer> dropIns = getIndices(b, 32);
                int executionCount = count(b, 32, 9);
                List<Execution> execution = new ArrayList<>(executionCount);
                for (int j = 0; j < executionCount; j++) {
                    Phase phase = Phase.values()[tag(b, 7) - 1];
                    execution.add(new Execution(phase, getCommand(b)));
                }
                units.add(new Unit(name, fileState, group, fragment, dropIns, execution, getIndices(b, 32),
                        getIndices(b, 32), getIndices(b, 32), getIndices(b, 32), index(b)));
            }
            var isolation = new Isolation(index(b), index(b), digest(b), index(b), index(b), index(b));
            var operations = new Operations(index(b), Integer.toUnsignedLong(b.getInt()), index(b));
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("Trailing Linux deployment input");
            }
            var result = new SegmentClassicLinuxDeployment(platform, files, loading, chain, units, launcher,
                    isolation, operations);
            if (!Arrays.equals(encoded, result.encode())) {
                throw new IllegalArgumentException("Noncanonical Linux deployment input");
            }
            return result;
        } catch (IllegalArgumentException | ArithmeticException | BufferUnderflowException malformed) {
            throw new IOException("Invalid Linux deployment input", malformed);
        }
    }

    private static void putCount(ByteBuffer b, int count) {
        b.putShort((short) count);
    }

    private static void putText(ByteBuffer b, String value) {
        byte[] bytes = text(value);
        b.putShort((short) bytes.length).put(bytes);
    }

    private static void putIndices(ByteBuffer b, List<Integer> values) {
        putCount(b, values.size());
        for (int value : values) {
            b.putShort((short) value);
        }
    }

    private static void putCommand(ByteBuffer b, Command value) {
        b.putShort((short) value.executable());
        putCount(b, value.arguments().size());
        for (String argument : value.arguments()) {
            putText(b, argument);
        }
    }

    private static int count(ByteBuffer b, int maximum, int minimumBytes) {
        int count = index(b);
        if (count > maximum || (long) count * minimumBytes > b.remaining()) {
            throw new IllegalArgumentException("Invalid bounded Linux deployment count");
        }
        return count;
    }

    private static int index(ByteBuffer b) {
        return Short.toUnsignedInt(b.getShort());
    }

    private static int tag(ByteBuffer b, int maximum) {
        int value = index(b);
        if (value < 1 || value > maximum) {
            throw new IllegalArgumentException("Unknown Linux deployment tag");
        }
        return value;
    }

    private static String getText(ByteBuffer b) throws CharacterCodingException {
        int length = index(b);
        if (length < 1 || length > 4096 || length > b.remaining()) {
            throw new IllegalArgumentException("Invalid Linux deployment UTF8 length");
        }
        byte[] value = new byte[length];
        b.get(value);
        String result = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
        text(result);
        return result;
    }

    private static List<Integer> getIndices(ByteBuffer b, int maximum) {
        int size = count(b, maximum, 2);
        List<Integer> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(index(b));
        }
        return values;
    }

    private static Command getCommand(ByteBuffer b) throws CharacterCodingException {
        int ref = index(b);
        int size = count(b, 256, 3);
        List<String> arguments = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            arguments.add(getText(b));
        }
        return new Command(ref, arguments);
    }

    private static SegmentClassicHandoff.Digest digest(ByteBuffer b) {
        byte[] bytes = new byte[32];
        b.get(bytes);
        return new SegmentClassicHandoff.Digest(bytes);
    }

    private static void requireLoading(List<File> files, Loading loading) {
        Set<Path> paths = new HashSet<>(files.stream().map(File::path).toList());
        for (Alias alias : loading.aliases()) {
            if (!paths.add(alias.link())) {
                throw new IllegalArgumentException("Conflicting system loading alias");
            }
            resolve(alias.link(), loading.aliases());
        }
        Set<Path> used = new HashSet<>();
        Path interpreter = resolve(loading.interpreter(), loading.aliases(), used);
        for (Requirement requirement : loading.configuration()) {
            resolve(requirement.path(), loading.aliases(), used);
        }
        for (Directory directory : loading.directories()) {
            Path resolved = resolve(directory.path(), loading.aliases(), used);
            for (Alias alias : loading.aliases()) {
                if (alias.link().startsWith(directory.path()) || alias.link().startsWith(resolved)) {
                    resolve(alias.link(), loading.aliases(), used);
                }
            }
        }
        for (Alias alias : loading.aliases()) {
            if (!used.contains(alias.link())) {
                throw new IllegalArgumentException("System alias is unrelated to any approved loading entry");
            }
        }
        if (!interpreter.equals(files.get(loading.loader()).path())) {
            throw new IllegalArgumentException("ELF interpreter does not resolve to the approved loader");
        }
        Set<Path> required = Set.of(Path.of("/etc/ld.so.preload"), Path.of("/etc/ld.so.cache"),
                Path.of("/etc/ld.so.conf"));
        Set<Path> named = new HashSet<>();
        for (Requirement requirement : loading.configuration()) {
            named.add(requirement.path());
            if (requirement.path().equals(Path.of("/etc/ld.so.preload")) && requirement.file() != -1) {
                throw new IllegalArgumentException("System preload must explicitly be absent");
            }
            if (requirement.file() >= 0) {
                Role expected = requirement.path().equals(Path.of("/etc/ld.so.cache"))
                        ? Role.LOADER_CACHE : Role.LOADER_CONFIG;
                role(files, requirement.file(), expected);
                if (!files.get(requirement.file()).path().equals(resolve(requirement.path(), loading.aliases()))) {
                    throw new IllegalArgumentException("Configuration reference differs from its exact path");
                }
            } else if (files.stream().anyMatch(f -> f.path().equals(requirement.path()))
                    || loading.aliases().stream().anyMatch(a -> a.link().equals(requirement.path()))) {
                throw new IllegalArgumentException("An absent loading path cannot also be an approved present member");
            }
        }
        if (!named.containsAll(required)) {
            throw new IllegalArgumentException("Explicit preload/cache/config presence rules are required");
        }
        for (File file : files) {
            if (!resolve(file.path(), loading.aliases()).equals(file.path())) {
                throw new IllegalArgumentException("Approved final files cannot use the system alias exception");
            }
            if (Set.of(Role.LOADER_CONFIG, Role.LOADER_CACHE).contains(file.role())
                    && loading.configuration().stream().noneMatch(c -> c.file() >= 0
                            && files.get(c.file()).path().equals(file.path()))) {
                throw new IllegalArgumentException("Loader configuration/cache member lacks its exact presence rule");
            }
        }
        for (Directory directory : loading.directories()) {
            Path resolved = resolve(directory.path(), loading.aliases());
            if (files.stream().anyMatch(f -> f.path().equals(resolved)
                    || !directory.present() && f.path().startsWith(resolved))
                    || !directory.present() && loading.aliases().stream()
                            .anyMatch(a -> a.link().startsWith(resolved))) {
                throw new IllegalArgumentException("Loading directory rule conflicts with an approved file");
            }
        }
        Set<Integer> elf = new HashSet<>();
        int edges = 0;
        for (Elf image : loading.elf()) {
            reference(image.file(), files.size());
            if (!files.get(image.file()).role().elf()) {
                throw new IllegalArgumentException("ELF rules refer to a non-ELF file role");
            }
            elf.add(image.file());
            edges = Math.addExact(edges, image.needed().size());
            if (edges > 8192) {
                throw new IllegalArgumentException("Approved ELF dependency edges exceed their fixed capacity");
            }
            for (Needed needed : image.needed()) {
                reference(needed.file(), files.size());
                if (!Set.of(Role.LIBRARY, Role.MODULE, Role.LOADER).contains(files.get(needed.file()).role())) {
                    throw new IllegalArgumentException("ELF dependency must name an approved loading file");
                }
            }
            image.rpath().forEach(i -> reference(i, loading.directories().size()));
            image.runpath().forEach(i -> reference(i, loading.directories().size()));
        }
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).role().elf() != elf.contains(i)) {
                throw new IllegalArgumentException("Every approved ELF requires exactly its complete loading rule");
            }
        }
        for (int ref : loading.modules()) {
            role(files, ref, Role.MODULE);
        }
        if (!new HashSet<>(loading.modules()).equals(new HashSet<>(java.util.stream.IntStream.range(0, files.size())
                .filter(i -> files.get(i).role() == Role.MODULE).boxed().toList()))) {
            throw new IllegalArgumentException("Dynamic module references must cover the declared module set");
        }
    }

    /** Models only the declared link substitutions. Actual readlink/parents/ELF/directory checks remain mandatory. */
    static Path resolve(Path path, List<Alias> aliases) {
        return resolve(path, aliases, null);
    }

    private static Path resolve(Path path, List<Alias> aliases, Set<Path> used) {
        absolute(path);
        Path current = path.getRoot();
        var pending = new ArrayDeque<String>();
        prependLoadingTarget(pending, path.toString());
        int followed = 0;
        while (!pending.isEmpty()) {
            String component = pending.removeFirst();
            if (component.equals(".")) {
                continue;
            }
            if (component.equals("..")) {
                current = current.getParent() == null ? current : current.getParent();
                continue;
            }
            Path candidate = current.resolve(component);
            Alias selected = null;
            for (Alias alias : aliases) {
                if (candidate.equals(alias.link())) {
                    selected = alias;
                    break;
                }
            }
            if (selected == null) {
                current = candidate;
                continue;
            }
            if (++followed > MAX_ALIAS_FOLLOWS) {
                throw new IllegalArgumentException("Cyclic or excessive system loading alias expansion");
            }
            if (used != null) {
                used.add(selected.link());
            }
            if (selected.target().startsWith("/")) {
                current = path.getRoot();
            }
            prependLoadingTarget(pending, selected.target());
            requireLoadingPathCapacity(current, pending);
        }
        return current;
    }

    /** Retain dot components and the directory traversal required by a raw trailing slash. */
    static void prependLoadingTarget(ArrayDeque<String> pending, String target) {
        String[] components = target.split("/", -1);
        for (int i = components.length - 1; i >= 0; i--) {
            if (!components[i].isEmpty()) {
                pending.addFirst(components[i]);
            } else if (i == components.length - 1) {
                pending.addFirst(".");
            }
        }
    }

    static void requireLoadingPathCapacity(Path prefix, ArrayDeque<String> pending) {
        String base = prefix.toString();
        text(base + (base.endsWith("/") ? "" : "/") + String.join("/", pending));
    }

    private static void requireCommand(List<File> files, Command command) {
        role(files, command.executable(), Role.START_CHAIN);
        if (!command.arguments().get(0).equals(files.get(command.executable()).path().toString())) {
            throw new IllegalArgumentException("Startup argv must name its exact approved executable");
        }
    }

    private static void role(List<File> files, int index, Role role) {
        reference(index, files.size());
        if (files.get(index).role() != role) {
            throw new IllegalArgumentException("Linux file reference differs from its required role");
        }
    }

    private static void reference(int index, int size) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Dangling Linux deployment reference");
        }
    }

    private static <T> List<T> bounded(List<T> list, int maximum) {
        if (list.size() > maximum) {
            throw new IllegalArgumentException("Linux deployment collection exceeds its fixed capacity");
        }
        return List.copyOf(list);
    }

    private static List<Integer> indices(List<Integer> list, int maximum) {
        List<Integer> result = bounded(list, maximum);
        if (new HashSet<>(result).size() != result.size() || result.stream().anyMatch(i -> i < 0)) {
            throw new IllegalArgumentException("Invalid or duplicate Linux deployment references");
        }
        return result;
    }

    private static List<Integer> orderedReferences(List<Integer> list, int maximum) {
        List<Integer> result = indices(list, maximum);
        orderedIndices(result);
        return result;
    }

    private static void orderedIndices(List<Integer> list) {
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i - 1) >= list.get(i)) {
                throw new IllegalArgumentException("Linux deployment references must be uniquely ordered");
            }
        }
    }

    private static void ordered(List<String> values) {
        for (int i = 1; i < values.size(); i++) {
            if (values.get(i - 1).compareTo(values.get(i)) >= 0) {
                throw new IllegalArgumentException("Linux deployment paths/names must be uniquely ordered");
            }
        }
    }

    private static void absolute(Path path) {
        Objects.requireNonNull(path, "path");
        text(path.toString());
        if (!path.isAbsolute() || !path.normalize().equals(path) || path.getParent() == null) {
            throw new IllegalArgumentException("Canonical absolute Linux deployment path required");
        }
    }

    private static byte[] text(String value) {
        if (value == null || value.isEmpty() || value.length() > 4096 || value.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Nonempty Linux deployment text required");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
            if (encoded.remaining() > 4096) {
                throw new IllegalArgumentException("Linux deployment text exceeds 4096 UTF8 bytes");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("Invalid Linux deployment UTF8 text", malformed);
        }
    }
}

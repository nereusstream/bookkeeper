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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Alias;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Command;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Elf;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Execution;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Isolation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Loading;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Needed;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Phase;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Requirement;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Unit;
import org.junit.Test;

/** Synthetic independent approval-input vectors only; never executes these files or creates Linux qualification. */
public class SegmentClassicLinuxDeploymentTest {
    static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicLinuxDeploymentTest.class.getResourceAsStream(
                "/unified-wal-segment/" + name + ".hex")) {
            assertNotNull(name, input);
            String text = new String(input.readAllBytes(), StandardCharsets.US_ASCII);
            return HexFormat.of().parseHex(text.lines().filter(line -> !line.startsWith("#"))
                    .collect(Collectors.joining()).trim());
        }
    }

    private static SegmentClassicLinuxDeployment original() throws IOException {
        return SegmentClassicLinuxDeployment.decode(vector("classic-linux-deployment-v1"));
    }

    static byte[] largerLauncherInput() throws IOException {
        var input = SegmentClassicLauncherInputs.decode(vector("classic-launcher-inputs-v2"));
        var d = input.linuxDeployment();
        var l = d.loading();
        var files = new ArrayList<>(d.files());
        var config = new ArrayList<>(l.configuration());
        String base = "/z/" + ("d".repeat(100) + "/").repeat(35);
        for (int i = 0; i < 10; i++) {
            Path path = Path.of(base + "c0" + i + ".conf");
            config.add(new Requirement(path, files.size()));
            files.add(new SegmentClassicLinuxDeployment.File(path, SegmentClassicLinuxDeployment.Role.LOADER_CONFIG,
                    0, SegmentClassicHandoff.digest(new byte[]{(byte) i})));
        }
        var loading = new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(), l.directories(), config,
                l.elf(), l.modules());
        var enlarged = new SegmentClassicLinuxDeployment(d.platform(), files, loading, d.startChain(), d.units(),
                d.launcherUnit(), d.isolation(), d.operations());
        var program = SegmentClassicLaunchProgram.identity(vector("classic-linux-program-v1"));
        return new SegmentClassicLauncherInputs(input.recipeId(), input.recipe(), input.signerKeyId(),
                SegmentClassicLauncherInputs.parentProgramIdentity(program, enlarged), input.os(), input.architecture(),
                input.dropper(), input.supervisorDirectory(), input.targetUid(), input.manifest(), input.proof(),
                input.policy(), input.aesKey(), enlarged).encode();
    }

    @Test
    public void declaredResolverKeepsDotDotUntilEarlierAliasesHaveBeenExpanded() {
        var aliases = List.of(new Alias(Path.of("/entry"), "jump/../interpreter"),
                new Alias(Path.of("/jump"), "remote/deep"));
        assertEquals(Path.of("/remote/interpreter"), SegmentClassicLinuxDeployment.resolve(Path.of("/entry"),
                aliases));
        assertEquals(Path.of("/interpreter"), SegmentClassicLinuxDeployment.resolve(Path.of("/again/again/interpreter"),
                List.of(new Alias(Path.of("/again"), "."))));
        assertEquals(Path.of("/remote/entry"), SegmentClassicLinuxDeployment.resolve(Path.of("/entry"),
                List.of(new Alias(Path.of("/entry"), "jump/../entry"),
                        new Alias(Path.of("/jump"), "remote/deep"))));
        assertEquals(Path.of("/interpreter"), SegmentClassicLinuxDeployment.resolve(Path.of("/entry"),
                List.of(new Alias(Path.of("/entry"), "/../../interpreter"))));
    }

    @Test
    public void declaredResolverUsesFortyFollowsRegardlessOfTheNumberOfDeclaredAliases() {
        var aliases = new ArrayList<Alias>();
        for (int i = 0; i < 41; i++) {
            aliases.add(new Alias(Path.of(String.format("/chain%02d", i)),
                    i == 40 ? "interpreter" : String.format("chain%02d", i + 1)));
        }
        assertEquals(Path.of("/interpreter"), SegmentClassicLinuxDeployment.resolve(Path.of("/chain01"), aliases));
        assertThrows(IllegalArgumentException.class,
                () -> SegmentClassicLinuxDeployment.resolve(Path.of("/chain00"), aliases));
        assertThrows(IllegalArgumentException.class, () -> SegmentClassicLinuxDeployment.resolve(Path.of("/cycle"),
                List.of(new Alias(Path.of("/cycle"), "cycle"))));
    }

    @Test
    public void independentVectorHasOneCanonicalIdentityAndExplicitAbsenceRules() throws Exception {
        byte[] bytes = vector("classic-linux-deployment-v1");
        var d = original();
        assertEquals(1205, bytes.length);
        assertArrayEquals(bytes, d.encode());
        assertEquals("490df21d62cdfc28b967084f48b72d7478edcd1fb2968fdead7518926eb36edb",
                HexFormat.of().formatHex(d.identity().value()));
        assertEquals(Path.of("/usr/bin/systemctl"), d.files().get(d.loading().systemctl()).path());
        assertEquals(List.of(new Requirement(Path.of("/etc/ld.so.cache"), -1),
                new Requirement(Path.of("/etc/ld.so.conf"), -1),
                new Requirement(Path.of("/etc/ld.so.preload"), -1)), d.loading().configuration());
        assertEquals(d.files().get(d.loading().loader()).path(),
                SegmentClassicLinuxDeployment.resolve(d.loading().interpreter(), d.loading().aliases()));
        var child = SegmentClassicLaunchProgram.decode(vector("classic-linux-program-v1"));
        d.requireProgram(child, "classic-1");
        assertThrows(UnsupportedOperationException.class, () -> d.files().add(d.files().get(0)));
        assertThrows(UnsupportedOperationException.class, () -> d.startChain().get(0).arguments().add("changed"));
    }

    @Test
    public void independentPopulatedTablesPreserveRelativeAliasChainsAndEveryBoundField() throws Exception {
        byte[] bytes = vector("classic-linux-deployment-rich-v1");
        var d = SegmentClassicLinuxDeployment.decode(bytes);
        assertEquals(2501, bytes.length);
        assertArrayEquals(bytes, d.encode());
        assertEquals("be613399a1e3ced73319d736bc7e4eb19612c0ec87aa51315a4c494905e463eb",
                HexFormat.of().formatHex(d.identity().value()));
        assertEquals(0xffff_fffeL, d.units().get(0).group());
        assertEquals(1, d.units().get(0).dropIns().size());
        assertEquals(List.of(1), d.units().get(0).onFailure());
        assertEquals(List.of(0), d.units().get(1).onSuccess());
        assertEquals(1, d.loading().modules().size());
        assertEquals(List.of(1, 0), d.loading().elf().get(0).rpath());
        assertEquals(2, d.loading().elf().stream().filter(e -> e.file() == d.loading().systemctl())
                .findFirst().orElseThrow().needed().size());
        assertEquals(Path.of("/var/cache/bookkeeper-ld.so.cache"),
                SegmentClassicLinuxDeployment.resolve(Path.of("/etc/ld.so.cache"), d.loading().aliases()));
        assertEquals(d.files().get(d.loading().loader()).path(),
                SegmentClassicLinuxDeployment.resolve(d.loading().interpreter(), d.loading().aliases()));
        d.requireProgram(SegmentClassicLaunchProgram.decode(vector("classic-linux-program-v1")), "classic-1");
    }

    @Test
    public void unknownFramingRolesCountsAndTruncationCannotProduceApprovalInput() throws Exception {
        byte[] bytes = vector("classic-linux-deployment-v1");
        for (int length = 0; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(truncated));
        }
        for (int offset : List.of(0, 8, 10, 12, 18)) {
            byte[] corrupted = bytes.clone();
            corrupted[offset] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(corrupted));
        }
        ByteBuffer cursor = ByteBuffer.wrap(bytes);
        cursor.position(16);
        for (int i = 0; i < 6; i++) {
            int length = Short.toUnsignedInt(cursor.getShort());
            cursor.position(cursor.position() + length);
        }
        cursor.position(cursor.position() + 4);
        int countOffset = cursor.position();
        byte[] tooMany = bytes.clone();
        ByteBuffer.wrap(tooMany).putShort(countOffset, (short) 1025);
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(tooMany));
        cursor.getShort();
        int pathLength = Short.toUnsignedInt(cursor.getShort());
        cursor.position(cursor.position() + pathLength);
        byte[] badRole = bytes.clone();
        ByteBuffer.wrap(badRole).putShort(cursor.position(), (short) 11);
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(badRole));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(new byte[1024 * 1024 + 1]));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(null));
    }

    @Test
    public void missingPresenceRulesDanglingDependenciesAndCyclesReject() throws Exception {
        var d = original();
        var l = d.loading();
        var missing = new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(), l.directories(),
                l.configuration().subList(0, 2), l.elf(), l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, missing));
        List<Requirement> preload = new ArrayList<>(l.configuration());
        preload.set(2, new Requirement(Path.of("/etc/ld.so.preload"), l.systemctl()));
        var present = new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(), l.directories(), preload,
                l.elf(), l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, present));
        List<Elf> images = new ArrayList<>(l.elf());
        images.set(0, new Elf(images.get(0).file(), List.of(new Needed("libmissing.so", 1023)), List.of(), List.of()));
        var dangling = new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(), l.directories(),
                l.configuration(), images, l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, dangling));
        var cycles = new Loading(l.systemctl(), l.loader(), l.interpreter(),
                List.of(new Alias(Path.of("/cycle"), l.interpreter().toString()),
                        new Alias(l.interpreter(), "/cycle")), l.directories(), l.configuration(), l.elf(),
                l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, cycles));
        var unused = new Loading(l.systemctl(), l.loader(), l.interpreter(),
                List.of(l.aliases().get(0), new Alias(Path.of("/unused/loader"), "/usr/lib/ld-linux-x86-64.so.2")),
                l.directories(), l.configuration(), l.elf(), l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, unused));
        var absent = new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(),
                List.of(new SegmentClassicLinuxDeployment.Directory(Path.of("/lib64"), false),
                        l.directories().get(0)), l.configuration(), l.elf(), l.modules());
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, absent));
    }

    @Test
    public void identitiesCoverRulesAsWellAsTheFileTableAndRejectOtherRootAccess() throws Exception {
        var d = original();
        var i = d.isolation();
        var changed = new Isolation(i.dropper(), i.dropperAbi(), SegmentClassicHandoff.digest(new byte[]{9}),
                i.filterFormat(), i.credentialRule(), i.cgroupRule());
        var other = new SegmentClassicLinuxDeployment(d.platform(), d.files(), d.loading(), d.startChain(), d.units(),
                d.launcherUnit(), changed, d.operations());
        assertNotEquals(d.identity(), other.identity());
        assertEquals(d.files(), other.files());
        var unit = d.units().get(0);
        var group = new Unit(unit.name(), unit.fileState(), 123, unit.fragment(), unit.dropIns(), unit.execution(),
                unit.triggers(), unit.triggeredBy(), unit.onFailure(), unit.onSuccess(), unit.constraintFormat());
        var regrouped = new SegmentClassicLinuxDeployment(d.platform(), d.files(), d.loading(), d.startChain(),
                List.of(group), 0, d.isolation(), d.operations());
        assertNotEquals(d.identity(), regrouped.identity());
        var child = SegmentClassicLaunchProgram.identity(vector("classic-linux-program-v1"));
        assertNotEquals(SegmentClassicLauncherInputs.parentProgramIdentity(child, d),
                SegmentClassicLauncherInputs.parentProgramIdentity(child, other));
        assertNotEquals(SegmentClassicLauncherInputs.parentProgramIdentity(child),
                SegmentClassicLauncherInputs.parentProgramIdentity(child, d));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentClassicLinuxDeployment.Operations(1, 501, 1));
        assertThrows(IllegalArgumentException.class, () -> new Isolation(i.dropper(), 1, i.filter(), 1, 1, 1));
    }

    @Test
    public void childOverlapAndGeneralFileAliasesCannotChangeTheApprovedChild() throws Exception {
        var d = original();
        var child = SegmentClassicLaunchProgram.decode(vector("classic-linux-program-v1"));
        List<SegmentClassicLinuxDeployment.File> files = new ArrayList<>(d.files());
        var java = files.get(2);
        files.set(2, new SegmentClassicLinuxDeployment.File(java.path(), java.role(), java.length(),
                SegmentClassicHandoff.digest(new byte[]{1})));
        var conflicting = new SegmentClassicLinuxDeployment(d.platform(), files, d.loading(), d.startChain(), d.units(),
                d.launcherUnit(), d.isolation(), d.operations());
        assertThrows(IOException.class, () -> conflicting.requireProgram(child, "classic-1"));
        var l = d.loading();
        var aliases = new Loading(l.systemctl(), l.loader(), l.interpreter(),
                List.of(l.aliases().get(0), new Alias(Path.of("/opt/jdk/lib"), "/usr/lib")),
                List.of(new SegmentClassicLinuxDeployment.Directory(Path.of("/opt/jdk/lib"), true),
                        l.directories().get(0)),
                l.configuration(), l.elf(), l.modules());
        var redirected = withLoading(d, aliases);
        assertThrows(IOException.class, () -> redirected.requireProgram(child, "classic-1"));
    }

    @Test
    public void aggregateEncodingAndCollectionCapsNeverTruncateTheApprovedInput() throws Exception {
        var d = original();
        var l = d.loading();
        assertThrows(IllegalArgumentException.class, () -> new Loading(l.systemctl(), l.loader(), l.interpreter(),
                Collections.nCopies(129, l.aliases().get(0)), l.directories(), l.configuration(), l.elf(),
                l.modules()));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicLinuxDeployment(d.platform(),
                Collections.nCopies(1025, d.files().get(0)), l, d.startChain(), d.units(), d.launcherUnit(),
                d.isolation(), d.operations()));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicLinuxDeployment(d.platform(), d.files(),
                l, d.startChain(), Collections.nCopies(33, d.units().get(0)), 0, d.isolation(), d.operations()));
        List<String> arguments = new ArrayList<>();
        arguments.add(d.startChain().get(0).arguments().get(0));
        arguments.addAll(Collections.nCopies(255, "x".repeat(4096)));
        var large = new Command(d.startChain().get(0).executable(), arguments);
        var unit = d.units().get(0);
        var enlarged = new Unit(unit.name(), unit.fileState(), unit.group(), unit.fragment(), unit.dropIns(),
                List.of(new Execution(Phase.START, large)), unit.triggers(), unit.triggeredBy(), unit.onFailure(),
                unit.onSuccess(), unit.constraintFormat());
        var oversized = new SegmentClassicLinuxDeployment(d.platform(), d.files(), l, List.of(large),
                List.of(enlarged), 0, d.isolation(), d.operations());
        assertThrows(IOException.class, oversized::encode);
    }

    private static SegmentClassicLinuxDeployment withLoading(SegmentClassicLinuxDeployment d, Loading loading) {
        return new SegmentClassicLinuxDeployment(d.platform(), d.files(), loading, d.startChain(), d.units(),
                d.launcherUnit(), d.isolation(), d.operations());
    }
}

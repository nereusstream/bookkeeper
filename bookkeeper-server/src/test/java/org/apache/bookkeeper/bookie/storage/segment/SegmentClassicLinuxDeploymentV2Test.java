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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Role;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Alias;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Configuration;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.ConfigurationKind;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Context;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Elf;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Family;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Kind;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Loading;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.NeededRule;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Provider;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Relation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Unit;
import org.junit.Test;

/** Independent synthetic approval vectors and rejection contracts; no provider, loader or systemd qualification. */
public class SegmentClassicLinuxDeploymentV2Test {
    private static byte[] vector() throws IOException {
        return SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2");
    }

    private static SegmentClassicLinuxDeploymentV2 original() throws IOException {
        return SegmentClassicLinuxDeployment.decode(vector()).versionTwo();
    }

    private static SegmentClassicLinuxDeploymentV2 withLoading(SegmentClassicLinuxDeploymentV2 d, Loading loading) {
        return new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), d.provider(), loading, d.startChain(),
                d.units(), d.launcherUnit(), d.installLinks(), d.isolation(), d.operations());
    }

    private static Loading loading(Loading l, List<Elf> elf, List<Context> contexts, List<NeededRule> needed) {
        return new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(), l.directories(), l.configuration(),
                l.policy(), elf, contexts, needed, l.modules());
    }

    private static Unit relations(Unit u, List<Relation> rows) {
        return new Unit(u.kind(), u.id(), u.names(), u.fileState(), u.fragment(), u.dropIns(), u.commonRule(), rows,
                u.service());
    }

    private static SegmentClassicLinuxDeploymentV2 units(SegmentClassicLinuxDeploymentV2 d, List<Unit> units) {
        return new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), d.provider(), d.loading(), d.startChain(),
                units, d.launcherUnit(), d.installLinks(), d.isolation(), d.operations());
    }

    @Test
    public void independentBytesKeepOriginalDynamicOrderContextAndTypedUnitFacts() throws Exception {
        byte[] bytes = vector();
        var wrapper = SegmentClassicLinuxDeployment.decode(bytes);
        var d = wrapper.versionTwo();
        assertEquals(2804, bytes.length);
        assertEquals(2, wrapper.schema());
        assertArrayEquals(bytes, d.encode());
        assertArrayEquals(bytes, wrapper.encode());
        assertEquals("b1c5c07cdef05919f47a85ff519327128e46921e92ae38c6fa9e95971fc2383f",
                HexFormat.of().formatHex(wrapper.identity().value()));
        assertEquals(wrapper, SegmentClassicLinuxDeployment.decode(bytes));
        assertEquals(wrapper.hashCode(), SegmentClassicLinuxDeployment.decode(bytes).hashCode());
        assertEquals(List.of("libc.so.6", "libc.so.6"), d.loading().elf().get(0).needed());
        assertEquals(new DynamicPath(true, ""), d.loading().elf().get(0).rpath());
        assertEquals(new DynamicPath(false, ""), d.loading().elf().get(0).runpath());
        assertEquals("/lib64/./libc.so.6", d.loading().contexts().get(3).logicalLoadName());
        assertEquals(List.of(3, 4), d.loading().needed().get(0).candidates());
        assertEquals(d.loading().modules().get(0), d.loading().modules().get(1));
        assertEquals(ConfigurationKind.MODULE_RUNTIME, d.loading().configuration().get(3).kind());
        assertEquals(Kind.TARGET, d.units().get(1).kind());
        assertNull(d.units().get(1).service());
        assertEquals(List.of("default.target", "multi-user.target"), d.units().get(1).names());
        assertEquals(List.of("launcher.service", "unrelated.service"),
                d.units().get(1).relations().get(Family.WANTS.ordinal()).names());
        assertEquals(0xffff_fffeL, d.units().get(0).service().group());
        assertThrows(UnsupportedOperationException.class, () -> d.loading().contexts().clear());
        assertThrows(UnsupportedOperationException.class, () -> d.loading().needed().get(0).candidates().clear());
    }

    @Test
    public void fixedPreloadAbsenceSelectionKeepsItsOriginalIndexWhenOtherRulesPrecedeIt() throws Exception {
        var d = original();
        var l = d.loading();
        var selected = l.configuration().get(d.preloadAbsentConfigurationIndex());
        assertEquals("/etc/ld.so.preload", selected.path());
        assertEquals(ConfigurationKind.PRELOAD_ABSENT, selected.kind());
        assertEquals(1, selected.format());
        assertEquals(-1, selected.file());
        assertEquals(l.configuration().indexOf(selected), d.preloadAbsentConfigurationIndex());
        var configuration = new ArrayList<>(l.configuration());
        configuration.add(0, new Configuration("/etc/aaa.no-material", -1, ConfigurationKind.MODULE_RUNTIME, 1));
        var shifted = withLoading(d, new Loading(l.systemctl(), l.loader(), l.interpreter(), l.aliases(),
                l.directories(), configuration, l.policy(), l.elf(), l.contexts(), l.needed(), l.modules()));
        assertTrue(shifted.preloadAbsentConfigurationIndex() > d.preloadAbsentConfigurationIndex());
        assertEquals(configuration.indexOf(selected), shifted.preloadAbsentConfigurationIndex());
        assertEquals(selected, shifted.loading().configuration().get(shifted.preloadAbsentConfigurationIndex()));
    }

    @Test
    public void legacyTypedAccessorsAndLegacyRoleCannotSilentlyConsumeNewSchema() throws Exception {
        var d = SegmentClassicLinuxDeployment.decode(vector());
        assertThrows(IllegalStateException.class, d::platform);
        assertThrows(IllegalStateException.class, d::files);
        assertThrows(IllegalStateException.class, d::loading);
        assertThrows(IllegalStateException.class, d::startChain);
        assertThrows(IllegalStateException.class, d::units);
        assertThrows(IllegalStateException.class, d::launcherUnit);
        assertThrows(IllegalStateException.class, d::isolation);
        assertThrows(IllegalStateException.class, d::operations);
        var old = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v1"));
        assertEquals(1, old.schema());
        assertThrows(IllegalStateException.class, old::versionTwo);
        assertNotEquals(old, d);
        var files = new ArrayList<>(old.files());
        var first = files.get(0);
        files.set(0, new SegmentClassicLinuxDeployment.File(first.path(), Role.JDK_RUNTIME, first.length(),
                first.sha256()));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicLinuxDeployment(old.platform(), files,
                old.loading(), old.startChain(), old.units(), old.launcherUnit(), old.isolation(), old.operations()));
    }

    @Test
    public void originalChildRuntimeVendorAndImageAreRequiredTogether() throws Exception {
        var d = original();
        var child = SegmentClassicLaunchProgram.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1"));
        d.requireProgram(child, "classic-1");
        for (Provider provider : List.of(new Provider(1, "other-build", d.provider().vendor(), d.provider().image()),
                new Provider(1, d.provider().runtime(), "other-vendor", d.provider().image()))) {
            var changed = new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), provider, d.loading(),
                    d.startChain(), d.units(), d.launcherUnit(), d.installLinks(), d.isolation(), d.operations());
            assertThrows(IOException.class, () -> changed.requireProgram(child, "classic-1"));
            assertNotEquals(d.identity(), changed.identity());
        }
        var files = new ArrayList<>(d.files());
        var image = files.get(d.provider().image());
        files.set(d.provider().image(), new SegmentClassicLinuxDeploymentV2.File(image.path(), image.role(),
                image.length(), SegmentClassicHandoff.digest(new byte[]{1})));
        var changed = new SegmentClassicLinuxDeploymentV2(d.platform(), files, d.provider(), d.loading(),
                d.startChain(), d.units(), d.launcherUnit(), d.installLinks(), d.isolation(), d.operations());
        assertThrows(IOException.class, () -> changed.requireProgram(child, "classic-1"));
        assertNotEquals(d.identity(), changed.identity());
        var l = d.loading();
        var aliases = List.of(new Alias("/lib64", "usr/lib"), new Alias("/opt/jdk/release", "../other-release"));
        var altered = withLoading(d, new Loading(l.systemctl(), l.loader(), l.interpreter(), aliases, l.directories(),
                l.configuration(), l.policy(), l.elf(), l.contexts(), l.needed(), l.modules()));
        assertThrows(IOException.class, () -> altered.requireProgram(child, "classic-1"));
    }

    @Test
    public void completeOrdinalsCandidateReferencesAndPhysicalContextBindingAreMandatory() throws Exception {
        var d = original();
        var l = d.loading();
        assertThrows(IllegalArgumentException.class, () -> withLoading(d,
                loading(l, l.elf(), l.contexts(), l.needed().subList(0, l.needed().size() - 1))));
        var needed = new ArrayList<>(l.needed());
        needed.set(0, new NeededRule(0, 0, List.of(4000)));
        assertThrows(IllegalArgumentException.class,
                () -> withLoading(d, loading(l, l.elf(), l.contexts(), needed)));
        assertThrows(IllegalArgumentException.class, () -> new NeededRule(0, 0, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new NeededRule(0, 0, List.of(4, 4)));
        assertThrows(IllegalArgumentException.class, () -> new NeededRule(0, 0, List.of(4, 3)));
        assertThrows(IllegalArgumentException.class, () -> loading(l, l.elf(), l.contexts(),
                List.of(l.needed().get(0), l.needed().get(0))));
        var contexts = new ArrayList<>(l.contexts());
        contexts.set(3, new Context(contexts.get(3).file(), "/lib64/./other.so"));
        assertThrows(IllegalArgumentException.class,
                () -> withLoading(d, loading(l, l.elf(), contexts, l.needed())));
        assertThrows(IllegalArgumentException.class, () -> withLoading(d,
                loading(l, l.elf().subList(0, l.elf().size() - 1), l.contexts(), l.needed())));
    }

    @Test
    public void rawPathSyntaxUtf8AndExplicitPolicyKindsRejectUnknownInterpretation() throws Exception {
        var d = original();
        var f = d.files().get(0);
        for (String path : List.of("/", "/a/../b", "/a//b", "/a/", "/a/./b", "relative", "/a\0b",
                "/" + (char) 0xd800)) {
            assertThrows(IllegalArgumentException.class,
                    () -> new SegmentClassicLinuxDeploymentV2.File(path, f.role(), f.length(), f.sha256()));
        }
        assertEquals("/usr/lib/libc.so.6", SegmentClassicLinuxDeploymentV2.resolve("/entry/../libc.so.6",
                List.of(new Alias("/entry", "usr/lib/deep"))));
        assertEquals("/", new SegmentClassicLinuxDeploymentV2.Directory("/", true).path());
        assertThrows(IllegalArgumentException.class, () -> new DynamicPath(false, "value"));
        assertThrows(IllegalArgumentException.class, () -> new Provider(2, "21-test", "test-vendor", 3));
        assertThrows(IllegalArgumentException.class,
                () -> new Configuration("/etc/ld.so.conf", 0, ConfigurationKind.LDCONFIG, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new Configuration("/etc/ld.so.preload", 0, ConfigurationKind.PRELOAD_ABSENT, 1));
        var configuration = new ArrayList<>(d.loading().configuration());
        configuration.set(1, new Configuration("/etc/ld.so.conf", 0, ConfigurationKind.MODULE_RUNTIME, 1));
        var l = d.loading();
        assertThrows(IllegalArgumentException.class, () -> withLoading(d, new Loading(l.systemctl(), l.loader(),
                l.interpreter(), l.aliases(), l.directories(), configuration, l.policy(), l.elf(), l.contexts(),
                l.needed(), l.modules())));
    }

    @Test
    public void missingRowsTypedServiceAliasesAndReverseRelationsCannotBeDefaulted() throws Exception {
        var d = original();
        var service = d.units().get(0);
        var target = d.units().get(1);
        assertThrows(IllegalArgumentException.class,
                () -> relations(service, service.relations().subList(0, 25)));
        var rows = new ArrayList<>(service.relations());
        rows.set(0, rows.get(1));
        assertThrows(IllegalArgumentException.class, () -> relations(service, rows));
        var empty = new ArrayList<>(service.relations());
        empty.set(Family.WANTED_BY.ordinal(), new Relation(Family.WANTED_BY, List.of()));
        assertThrows(IllegalArgumentException.class, () -> units(d, List.of(relations(service, empty), target)));
        assertThrows(IllegalArgumentException.class, () -> new Unit(Kind.TARGET, target.id(), target.names(),
                target.fileState(), target.fragment(), target.dropIns(), target.commonRule(), target.relations(),
                service.service()));
        assertThrows(IllegalArgumentException.class, () -> new Unit(Kind.SERVICE, service.id(), service.names(),
                service.fileState(), service.fragment(), service.dropIns(), service.commonRule(),
                service.relations(), null));
        assertThrows(IllegalArgumentException.class, () -> new Unit(Kind.TARGET, target.id(),
                List.of("launcher.service", target.id()), target.fileState(), target.fragment(), target.dropIns(),
                target.commonRule(), target.relations(), null));
        var twoTargets = List.of(service, target, new Unit(Kind.TARGET, "z.target",
                List.of("default.target", "z.target"),
                "static", target.fragment(), List.of(), 1, target.relations(), null));
        assertThrows(IllegalArgumentException.class, () -> units(d, twoTargets));
        assertFalse(target.relations().get(Family.WANTS.ordinal()).names().contains("default.target"));
    }

    @Test
    public void independentFramingTruncationCountsAndMalformedUtf8Reject() throws Exception {
        byte[] bytes = vector();
        for (int length = 0; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(truncated));
        }
        for (int offset : List.of(0, 8, 10, 12, 18)) {
            byte[] bad = bytes.clone();
            bad[offset] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(bad));
        }
        for (int[] mutation : new int[][]{{138, 1025}, {1124, 1}, {1431, 4097}, {1608, 8193}, {1664, 8193},
                {1909, 33}}) {
            byte[] bad = bytes.clone();
            ByteBuffer.wrap(bad).putShort(mutation[0], (short) mutation[1]);
            assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(bad));
        }
        byte[] malformed = bytes.clone();
        malformed[142] = (byte) 0xc0;
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(malformed));
        byte[] trailing = Arrays.copyOf(bytes, bytes.length + 1);
        ByteBuffer.wrap(trailing).putInt(12, trailing.length - 16);
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeployment.decode(trailing));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDeploymentV2.decode(null));
    }

    private static byte[] replaceNeeded(int count, int candidates) throws IOException {
        var bytes = new ByteArrayOutputStream();
        bytes.write(vector(), 0, 1608);
        var output = new DataOutputStream(bytes);
        output.writeShort(count);
        for (int i = 0; i < count; i++) {
            output.writeShort(0);
            output.writeShort(i);
            output.writeShort(candidates);
            for (int j = 0; j < candidates; j++) {
                output.writeShort(j);
            }
        }
        bytes.write(vector(), 1664, vector().length - 1664);
        byte[] result = bytes.toByteArray();
        ByteBuffer.wrap(result).putInt(12, result.length - 16);
        return result;
    }

    @Test
    public void sharedRequestAndCandidateBudgetsRejectBeforeLaterReferenceValidation() throws Exception {
        // Locally valid ordered rows: 65 * 4096 exceeds the shared 262144 membership budget at row 65.
        IOException candidates = assertThrows(IOException.class,
                () -> SegmentClassicLinuxDeploymentV2.decode(replaceNeeded(65, 4096)));
        assertEquals("Schema two aggregate capacity exceeded", candidates.getCause().getMessage());
        // The subsequent two module requests are charged against the same 8192 request budget.
        IOException requests = assertThrows(IOException.class,
                () -> SegmentClassicLinuxDeploymentV2.decode(replaceNeeded(8192, 1)));
        assertEquals("Schema two aggregate capacity exceeded", requests.getCause().getMessage());
        var l = original().loading();
        var rows = new ArrayList<NeededRule>();
        for (int i = 0; i < 8190; i++) {
            rows.add(new NeededRule(0, i, List.of(4)));
        }
        assertEquals(8190, loading(l, l.elf(), l.contexts(), rows).needed().size());
        rows.add(new NeededRule(0, 8190, List.of(4)));
        assertThrows(IllegalArgumentException.class, () -> loading(l, l.elf(), l.contexts(), rows));
    }

    @Test
    public void repeatedNamesAcrossRelationRowsConsumeTheFullSharedBudget() throws Exception {
        var d = original();
        var names = new ArrayList<String>();
        for (int i = 0; i < 1024; i++) {
            names.add(String.format("q%03x.target", i));
        }
        var units = new ArrayList<Unit>();
        for (int i = 0; i < 3; i++) {
            var rows = new ArrayList<Relation>();
            for (Family family : Family.values()) {
                int row = i * Family.values().length + family.ordinal();
                rows.add(new Relation(family, row < 64 ? names : List.of()));
            }
            Unit base = i == 0 ? d.units().get(0) : d.units().get(1);
            units.add(i < 2 ? relations(base, rows) : new Unit(Kind.TARGET, "z.target", List.of("z.target"),
                    "static", base.fragment(), List.of(), 1, rows, null));
        }
        var boundary = new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), d.provider(), d.loading(),
                d.startChain(), units, 0, List.of(), d.isolation(), d.operations());
        assertEquals(boundary, SegmentClassicLinuxDeploymentV2.decode(boundary.encode()));
        var last = units.get(2);
        var rows = new ArrayList<>(last.relations());
        rows.set(12, new Relation(Family.values()[12], List.of("extra.target")));
        units.set(2, relations(last, rows));
        IllegalArgumentException capacity = assertThrows(IllegalArgumentException.class,
                () -> new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), d.provider(), d.loading(),
                        d.startChain(), units, 0, List.of(), d.isolation(), d.operations()));
        assertEquals("Schema two aggregate capacity exceeded", capacity.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> new Relation(Family.WANTS, java.util.Collections.nCopies(1025, "same.target")));
    }

    @Test
    public void unitLookupCanCarryTheSameIdWithoutInventingAnAliasOrDependency() throws Exception {
        var d = original();
        var links = new ArrayList<>(d.installLinks());
        links.add(1, new SegmentClassicLinuxDeploymentV2.InstallLink(
                SegmentClassicLinuxDeploymentV2.LinkKind.UNIT_LOOKUP, "/etc/systemd/system/launcher.service",
                "/opt/approved-fragment-without-unit-suffix", -1, 0));
        var changed = new SegmentClassicLinuxDeploymentV2(d.platform(), d.files(), d.provider(), d.loading(),
                d.startChain(), d.units(), d.launcherUnit(), links, d.isolation(), d.operations());
        assertEquals(changed, SegmentClassicLinuxDeploymentV2.decode(changed.encode()));
        assertNotEquals(d.identity(), changed.identity());
        // This is data only: alias versus external linked-fragment lookup still needs the actual systemd owner.
        assertEquals(d.units(), changed.units());
    }

    @Test
    public void outerInputCarriesD2IdentityButCannotLaunchWithoutOriginalQualification() throws Exception {
        var d = new SegmentClassicLinuxDeployment(original());
        var old = SegmentClassicLauncherInputs.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v2"));
        var child = SegmentClassicLaunchProgram.identity(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1"));
        var dropper = d.versionTwo().files().get(d.versionTwo().isolation().dropper());
        var input = new SegmentClassicLauncherInputs(old.recipeId(), old.recipe(), old.signerKeyId(),
                SegmentClassicLauncherInputs.parentProgramIdentity(child, d), old.os(), old.architecture(),
                new SegmentClassicLaunchProgram.File(Path.of(dropper.path()), SegmentClassicLaunchProgram.NATIVE,
                        dropper.length(), dropper.sha256()), old.supervisorDirectory(), old.targetUid(), old.manifest(),
                old.proof(), old.policy(), old.aesKey(), d);
        assertArrayEquals(input.encode(), SegmentClassicLauncherInputs.decode(input.encode()).encode());
        assertNotEquals(old.parentProgram(), input.parentProgram());
        byte[] recipe = SegmentClassicLinuxDeploymentTest.vector("classic-linux-recipe-v1");
        var parsed = new SegmentClassicLaunchTrust.Parsed(SegmentClassicLaunchTrust.decode(recipe),
                SegmentClassicHandoff.digest("BookKeeper/ControlledClassicRecipe/v1\0"
                        .getBytes(StandardCharsets.US_ASCII), recipe), null);
        input.requireRecipe(parsed);
        IOException failure = assertThrows(IOException.class, () -> input.childCommand(parsed,
                SegmentClassicLaunchProgram.decode(
                        SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1"))));
        assertTrue(failure.getMessage().contains("original Linux qualification"));
    }
}

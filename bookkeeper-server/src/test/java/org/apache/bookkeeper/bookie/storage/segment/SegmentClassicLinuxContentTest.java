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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxContentKind;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeployment.Role;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.ConfigurationKind;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.Elf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real owned channels and common data functions only; never injects provider ready or installed provenance. */
public class SegmentClassicLinuxContentTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static long deadline() {
        return SegmentClassicLinuxElfTest.deadline();
    }

    private Path write(byte[] bytes) throws IOException {
        return Files.write(temporary.newFile().toPath(), bytes);
    }

    private static byte[] image() throws IOException {
        return SegmentClassicLinuxDeploymentTest.vector("classic-linux-elf-v2");
    }

    static Elf expected() {
        return new Elf(4, List.of("libz.so.1", "libz.so.1", "../${LIB}/lib雪.so", "$ORIGIN/deps/libx.so"),
                new DynamicPath(true, "$ORIGIN/../lib::relative:${LIB}/x:${PLATFORM}/p;/literal"),
                new DynamicPath(true, ""), 0x1b, 0x08000889);
    }

    @Test
    public void oneOwnedChannelHashesTheFullImageAndReadsRawElfWithoutClosingIt() throws Exception {
        byte[] bytes = image();
        Path path = write(bytes);
        var owner = new SegmentOwnedFiles();
        var opens = new AtomicInteger();
        var nativeChannel = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                opens.incrementAndGet();
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                nativeChannel.set(fault);
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ,
                    LinkOption.NOFOLLOW_LINKS)) {
                channel.position(17);
                var observed = SegmentClassicLaunchTrust.readLinuxContent(channel, bytes.length,
                        LinuxContentKind.ELF2, deadline());
                assertEquals(SegmentClassicHandoff.digest(bytes), observed.sha256());
                assertEquals(expected().needed(), observed.elf2().needed());
                assertEquals(expected().rpath(), observed.elf2().rpath());
                assertEquals(expected().runpath(), observed.elf2().runpath());
                assertEquals("/not/a/whitelisted SONAME/雪.so", observed.elf2().soname());
                assertTrue(observed.elf2().flagsPresent());
                assertTrue(observed.elf2().flags1Present());
                assertNull(observed.elf());
                assertEquals(bytes.length, channel.position());
                assertTrue(channel.isOpen());
                assertEquals(0, nativeChannel.get().closes.get());
                SegmentClassicLaunchTrust.requireLinuxElfMetadata(Role.SYSTEMCTL, observed.elf2().interpreter(),
                        expected(), observed.elf2());
            }
        }
        assertEquals(1, opens.get());
        assertEquals(1, nativeChannel.get().closes.get());
        assertFalse(nativeChannel.get().actual.isOpen());
        assertTrue(owner.drain().drained());
    }

    @Test
    public void declaredMetadataComparesOrderDuplicatesPresenceFlagsAndExactSystemctlInterpreter() throws Exception {
        Path path = write(image());
        var owner = new SegmentOwnedFiles();
        try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
            var observed = SegmentClassicLaunchTrust.readLinuxContent(channel, channel.size(),
                    LinuxContentKind.ELF2, deadline()).elf2();
            Elf e = expected();
            for (Elf wrong : List.of(
                    new Elf(4, e.needed().subList(1, e.needed().size()), e.rpath(), e.runpath(), e.flags(), e.flags1()),
                    new Elf(4, List.of("other.so", "libz.so.1", "../${LIB}/lib雪.so", "$ORIGIN/deps/libx.so"),
                            e.rpath(), e.runpath(), e.flags(), e.flags1()),
                    new Elf(4, e.needed(), new DynamicPath(false, ""), e.runpath(), e.flags(), e.flags1()),
                    new Elf(4, e.needed(), e.rpath(), new DynamicPath(false, ""), e.flags(), e.flags1()),
                    new Elf(4, e.needed(), e.rpath(), e.runpath(), 0x1a, e.flags1()),
                    new Elf(4, e.needed(), e.rpath(), e.runpath(), e.flags(), 0x08000089))) {
                assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireLinuxElfMetadata(
                        Role.SYSTEMCTL, observed.interpreter(), wrong, observed));
            }
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireLinuxElfMetadata(
                    Role.SYSTEMCTL, observed.interpreter() + "/", e, observed));
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireLinuxElfMetadata(
                    Role.LOADER_CONFIG, observed.interpreter(), e, observed));
            var noEntry = new SegmentClassicLinuxElf.Image2(2, 0, observed.interpreter(), observed.soname(),
                    observed.needed(), observed.rpath(), observed.runpath(), true, observed.flags(), true,
                    observed.flags1());
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireLinuxElfMetadata(
                    Role.SYSTEMCTL, observed.interpreter(), e, noEntry));
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireLinuxElfMetadata(
                    Role.LIBRARY, observed.interpreter(), e, noEntry));
        }
        assertTrue(owner.drain().drained());
    }

    @Test
    public void originalTableSelectionRejectsBoundsAndNonElfRolesWithoutPathConversion() throws Exception {
        var deployment = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2")).versionTwo();
        assertSame(deployment.files().get(4), SegmentClassicLaunchTrust.selectLinuxElfMember(deployment, 4));
        for (int index : new int[]{-1, deployment.files().size(), Integer.MAX_VALUE}) {
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.selectLinuxElfMember(deployment, index));
        }
        for (int i = 0; i < deployment.files().size(); i++) {
            int index = i;
            if (!deployment.files().get(i).role().elf()) {
                assertThrows(IOException.class,
                        () -> SegmentClassicLaunchTrust.selectLinuxElfMember(deployment, index));
            }
        }
    }

    @Test
    public void originalContextSelectionRetainsItsPairAndNeverTreatsMinusOneAsInterpreter() throws Exception {
        var deployment = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2")).versionTwo();
        for (int i = 0; i < deployment.loading().contexts().size(); i++) {
            assertSame(deployment.loading().contexts().get(i),
                    SegmentClassicLaunchTrust.selectLinuxLoadContext(deployment, i));
        }
        for (int index : new int[]{-1, deployment.loading().contexts().size(), Integer.MAX_VALUE}) {
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.selectLinuxLoadContext(deployment, index));
        }
    }

    @Test
    public void configurationSelectionUsesOriginalRuleKindAndRejectsIndependentModuleRuntime() throws Exception {
        var d = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2")).versionTwo();
        int moduleRules = 0;
        for (int i = 0; i < d.loading().configuration().size(); i++) {
            int index = i;
            if (d.loading().configuration().get(i).kind() == ConfigurationKind.MODULE_RUNTIME) {
                assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.selectLinuxConfiguration(d, index));
                moduleRules++;
            } else {
                assertSame(d.loading().configuration().get(i),
                        SegmentClassicLaunchTrust.selectLinuxConfiguration(d, i));
            }
        }
        assertTrue(moduleRules > 0);
        for (int i : new int[]{-1, d.loading().configuration().size(), Integer.MAX_VALUE}) {
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.selectLinuxConfiguration(d, i));
        }
    }

    @Test
    public void commonHeldChannelCacheKindHashesTheFullImageAndKeepsRawCacheMetadata() throws Exception {
        byte[] bytes = SegmentClassicLinuxCacheTest.vector();
        Path path = write(bytes);
        var owner = new SegmentOwnedFiles();
        try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
            channel.position(17);
            var content = SegmentClassicLaunchTrust.readLinuxContent(channel, bytes.length,
                    LinuxContentKind.CACHE, deadline());
            assertEquals(SegmentClassicHandoff.digest(bytes), content.sha256());
            assertEquals(bytes.length, channel.position());
            assertEquals(7, content.cache().entries().size());
            assertEquals("libz.so.10", content.cache().entries().get(0).name().utf8());
            assertEquals("libz.so.2", content.cache().entries().get(1).name().utf8());
            assertEquals("ldconfig (independent byte vector) release version 2.39", content.cache().generator().utf8());
            assertNull(content.elf());
            assertNull(content.elf2());
            assertNull(content.configuration());
            assertTrue(channel.isOpen());
        }
        assertTrue(owner.drain().drained());
    }

    @Test
    public void cacheDigestAndParsedRowsUseTheSameBytesWhenTheFileChangesAfterTheFirstRead() throws Exception {
        byte[] original = SegmentClassicLinuxCacheTest.vector();
        byte[] replacement = original.clone();
        int pathOffset = ByteBuffer.wrap(replacement).order(ByteOrder.LITTLE_ENDIAN).getInt(48 + 8);
        replacement[pathOffset + 1] = 'X';
        Path path = write(original);
        Path other = write(replacement);
        SegmentClassicLinuxCache.Image first;
        try (FileChannel a = FileChannel.open(path, StandardOpenOption.READ);
             FileChannel b = FileChannel.open(other, StandardOpenOption.READ)) {
            first = SegmentClassicLinuxCache.read(a, original.length, deadline());
            var second = SegmentClassicLinuxCache.read(b, replacement.length, deadline());
            assertFalse(first.entries().get(0).path().equals(second.entries().get(0).path()));
        }
        var owner = new SegmentOwnedFiles();
        var changed = new AtomicInteger();
        var retained = new AtomicReference<ByteBuffer>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                org.mockito.stubbing.Answer<Integer> changeAfterRead = call -> {
                    int read = (int) call.callRealMethod();
                    ByteBuffer target = call.getArgument(0);
                    if (read > 0 && target.position() == original.length && changed.compareAndSet(0, 1)) {
                        retained.set(target.duplicate());
                        Files.write(path, replacement);
                    }
                    return read;
                };
                doAnswer(changeAfterRead).when(fault).read(any(ByteBuffer.class));
                doAnswer(changeAfterRead).when(fault).read(any(ByteBuffer.class), anyLong());
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                channel.position(17);
                var content = SegmentClassicLaunchTrust.readLinuxContent(channel, original.length,
                        LinuxContentKind.CACHE, deadline());
                retained.get().put(pathOffset + 1, (byte) 'Y');
                assertEquals(SegmentClassicHandoff.digest(original), content.sha256());
                assertEquals(first, content.cache());
                assertEquals(original.length, channel.position());
                assertTrue(channel.isOpen());
                assertEquals(1, changed.get());
            }
        }
        assertTrue(owner.drain().drained());
        assertTrue(java.util.Arrays.equals(replacement, Files.readAllBytes(path)));
    }

    @Test
    public void configurationDigestAndDirectivesUseOneReadWhenDiskBytesChange() throws Exception {
        configurationContentAfterRead(true);
    }

    @Test
    public void configurationCallerRetainedReadBufferCannotChangeReturnedDirectives() throws Exception {
        configurationContentAfterRead(false);
    }

    private void configurationContentAfterRead(boolean replaceDisk) throws Exception {
        byte[] original = "include child-A.conf\n".getBytes(StandardCharsets.UTF_8);
        byte[] replacement = "include child-B.conf\n".getBytes(StandardCharsets.UTF_8);
        assertEquals(original.length, replacement.length);
        Path path = write(original);
        var owner = new SegmentOwnedFiles();
        var changed = new AtomicInteger();
        var retained = new AtomicReference<ByteBuffer>();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                org.mockito.stubbing.Answer<Integer> afterRead = call -> {
                    int read = (int) call.callRealMethod();
                    ByteBuffer target = call.getArgument(0);
                    if (read > 0 && target.position() == original.length) {
                        retained.set(target.duplicate());
                        if (replaceDisk && changed.compareAndSet(0, 1)) {
                            Files.write(path, replacement);
                        }
                    }
                    return read;
                };
                doAnswer(afterRead).when(fault).read(any(ByteBuffer.class));
                doAnswer(afterRead).when(fault).read(any(ByteBuffer.class), anyLong());
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                channel.position(17);
                var content = SegmentClassicLaunchTrust.readLinuxContent(channel, original.length,
                        LinuxContentKind.LDCONFIG, deadline());
                assertEquals(SegmentClassicHandoff.digest(original), content.sha256());
                assertArrayEquals(original, content.configuration().bytes());
                retained.get().put("include child-".length(), (byte) 'C');
                assertArrayEquals(original, content.configuration().bytes());
                var directive = content.configuration().cursor(deadline()).next();
                assertEquals(SegmentClassicLinuxConfiguration.Kind.INCLUDE, directive.kind());
                assertEquals("child-A.conf", directive.value().utf8());
                assertEquals(original.length, channel.position());
                assertTrue(channel.isOpen());
                assertEquals(replaceDisk ? 1 : 0, changed.get());
            }
        }
        assertTrue(owner.drain().drained());
    }

    @Test
    public void elf2DigestAndMetadataUseTheSameImageAfterAnEqualLengthFileChange() throws Exception {
        byte[] original = image();
        byte[] replacement = original.clone();
        int name = new String(original, StandardCharsets.ISO_8859_1).indexOf("libz.so.1\0");
        assertTrue(name >= 0);
        replacement[name + 3] = 'y';
        Path path = write(original);
        Path other = write(replacement);
        try (FileChannel changed = FileChannel.open(other, StandardOpenOption.READ)) {
            assertEquals("liby.so.1", SegmentClassicLinuxElf.readVersionTwo(changed, replacement.length, deadline())
                    .needed().get(0));
        }
        var owner = new SegmentOwnedFiles();
        var changed = new AtomicInteger();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                doAnswer(call -> {
                    int read = (int) call.callRealMethod();
                    ByteBuffer target = call.getArgument(0);
                    if (read > 0 && target.position() == original.length && changed.compareAndSet(0, 1)) {
                        Files.write(path, replacement);
                    }
                    return read;
                }).when(fault).read(any(ByteBuffer.class));
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                channel.position(17);
                var content = SegmentClassicLaunchTrust.readLinuxContent(channel, original.length,
                        LinuxContentKind.ELF2, deadline());
                assertEquals(SegmentClassicHandoff.digest(original), content.sha256());
                assertEquals(expected().needed(), content.elf2().needed());
                assertEquals(original.length, channel.position());
                assertEquals(1, changed.get());
                assertTrue(channel.isOpen());
            }
        }
        assertTrue(owner.drain().drained());
        assertArrayEquals(replacement, Files.readAllBytes(path));
    }

    @Test
    public void elf2PrivateParsedRangesRejectAChangedStreamDespiteRetainedChannelBuffers() throws Exception {
        byte[] original = image();
        byte[] replacement = original.clone();
        int name = new String(original, StandardCharsets.ISO_8859_1).indexOf("libz.so.1\0");
        assertTrue(name >= 0);
        int changedByte = name + 3;
        replacement[changedByte] = 'y';
        Path path = write(original);
        record Retained(long offset, ByteBuffer bytes) { }
        var retained = new ArrayList<Retained>();
        var changed = new AtomicInteger();
        var owner = new SegmentOwnedFiles();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                doAnswer(call -> {
                    int read = (int) call.callRealMethod();
                    if (read > 0) {
                        ByteBuffer target = call.getArgument(0);
                        retained.add(new Retained(call.getArgument(1), target.duplicate()));
                    }
                    return read;
                }).when(fault).read(any(ByteBuffer.class), anyLong());
                doAnswer(call -> {
                    if (changed.compareAndSet(0, 1)) {
                        assertFalse(retained.isEmpty());
                        Files.write(path, replacement);
                        for (var part : retained) {
                            long relative = changedByte - part.offset();
                            if (relative >= 0 && relative < part.bytes().position()) {
                                part.bytes().put((int) relative, (byte) 'y');
                            }
                        }
                    }
                    return call.callRealMethod();
                }).when(fault).read(any(ByteBuffer.class));
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                var failure = assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.readLinuxContent(
                        channel, original.length, LinuxContentKind.ELF2, deadline()));
                assertTrue(failure.getMessage().contains("metadata differs from the complete stream hash bytes"));
                assertEquals(1, changed.get());
                assertTrue(channel.isOpen());
            }
        }
        assertTrue(owner.drain().drained());
        assertArrayEquals(replacement, Files.readAllBytes(path));
    }

    @Test
    public void elf2CrossBlockOverlappingMetadataStillHashesAllUnparsedPayload() throws Exception {
        crossBlockElf2Hash(false);
    }

    @Test
    public void elf2CrossBlockContinuationRejectsAChangedByteAfterALegalShortRead() throws Exception {
        crossBlockElf2Hash(true);
    }

    private void crossBlockElf2Hash(boolean corrupt) throws Exception {
        byte[] original = image();
        byte[] bytes = java.util.Arrays.copyOf(original, 160 * 1024);
        java.util.Arrays.fill(bytes, original.length, bytes.length, (byte) 0x5a);
        ByteBuffer fixture = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(64, fixture.getLong(32));
        assertEquals(3, Short.toUnsignedInt(fixture.getShort(56)));
        int load = 64 + 56;
        assertEquals(1, fixture.getInt(load));
        long address = fixture.getLong(load + 16);
        assertEquals(5, fixture.getLong(512));
        assertEquals(10, fixture.getLong(528));
        int strings = Math.toIntExact(fixture.getLong(536));
        int oldOffset = Math.toIntExact(fixture.getLong(520) - address);
        int newOffset = 64 * 1024 - 16;
        System.arraycopy(original, oldOffset, bytes, newOffset, strings);
        fixture.putLong(520, address + newOffset);
        fixture.putLong(load + 32, bytes.length).putLong(load + 40, bytes.length);
        bytes[128 * 1024 + 7] = 0x7b;
        assertTrue(bytes[65536] != 'X');
        Path path = write(bytes);
        var owner = new SegmentOwnedFiles();
        var boundaryReads = new AtomicInteger();
        var changed = new AtomicInteger();
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                doAnswer(call -> {
                    ByteBuffer target = call.getArgument(0);
                    long offset = fault.actual.position();
                    if (offset < 65535) {
                        target.limit((int) Math.min(target.limit(), 65535 - offset));
                    }
                    int read = (int) call.callRealMethod();
                    if (offset < 65535 && offset + read == 65535) {
                        boundaryReads.incrementAndGet();
                    }
                    if (corrupt && offset <= 65536 && 65536 < offset + read) {
                        assertTrue(offset >= 65535);
                        target.put((int) (65536 - offset), (byte) 'X');
                        changed.incrementAndGet();
                    }
                    return read;
                }).when(fault).read(any(ByteBuffer.class));
                return fault;
            }
            return actual;
        })) {
            try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                channel.position(17);
                var raw = SegmentClassicLinuxElf.readVersionTwo(channel, bytes.length, deadline());
                assertEquals(17, channel.position());
                assertEquals(expected().needed(), raw.needed());
                if (corrupt) {
                    var failure = assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.readLinuxContent(
                            channel, bytes.length, LinuxContentKind.ELF2, deadline()));
                    assertTrue(failure.getMessage().contains("metadata differs from the complete stream hash bytes"));
                } else {
                    var content = SegmentClassicLaunchTrust.readLinuxContent(channel, bytes.length,
                            LinuxContentKind.ELF2, deadline());
                    assertEquals(raw, content.elf2());
                    assertEquals(SegmentClassicHandoff.digest(bytes), content.sha256());
                    assertEquals(bytes.length, channel.position());
                }
                assertEquals(1, boundaryReads.get());
                assertEquals(corrupt ? 1 : 0, changed.get());
                assertTrue(channel.isOpen());
            }
        }
        assertTrue(owner.drain().drained());
    }

    @Test
    public void truncationNoProgressAndParserFailureNeverCloseTheCallersChannel() throws Exception {
        for (int scenario = 0; scenario < 3; scenario++) {
            byte[] bytes = image();
            if (scenario == 2) {
                bytes[4] = 1;
            }
            Path path = write(bytes);
            var owner = new SegmentOwnedFiles();
            int variant = scenario;
            var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
            try (var channels = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && path.equals(invocation.getArgument(0))) {
                    var fault = spy(new SegmentDataFileLockTest.FaultChannel((FileChannel) actual));
                    if (variant != 2) {
                        doAnswer(call -> {
                            if (variant == 0) {
                                fault.actual.truncate(63);
                                return call.callRealMethod();
                            }
                            return 0;
                        }).when(fault).read(any(ByteBuffer.class));
                    }
                    captured.set(fault);
                    return fault;
                }
                return actual;
            })) {
                try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
                    assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.readLinuxContent(channel, 1536,
                            LinuxContentKind.ELF2, deadline()));
                    assertTrue(channel.isOpen());
                    assertEquals(0, captured.get().closes.get());
                }
            }
            assertEquals(1, captured.get().closes.get());
            assertTrue(owner.drain().drained());
        }
    }

    @Test
    public void originalDeadlineAndFirstCloseRemainIndependentFromContentSuccess() throws Exception {
        Path path = write(image());
        var owner = new SegmentOwnedFiles();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException firstClose = new IOException("Original content native close");
        try (var channels = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && path.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = firstClose;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            IOException failure = assertThrows(IOException.class, () -> {
                try (FileChannel channel = SegmentOwnedFiles.open(owner, path, StandardOpenOption.READ)) {
                    assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.readLinuxContent(channel, 1536,
                            LinuxContentKind.ELF2, System.nanoTime()));
                    assertTrue(channel.isOpen());
                    assertEquals(0, captured.get().closes.get());
                    var content = SegmentClassicLaunchTrust.readLinuxContent(channel, 1536,
                            LinuxContentKind.ELF2, deadline());
                    assertEquals(expected().needed(), content.elf2().needed());
                }
            });
            assertSame(firstClose, failure);
        }
        try {
            assertFalse(owner.drain().drained());
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.drain().drained());
    }
}

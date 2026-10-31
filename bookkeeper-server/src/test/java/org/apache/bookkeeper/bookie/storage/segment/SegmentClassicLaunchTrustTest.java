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

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mockStatic;

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual file/ACL fixture with a chosen owner only. It cannot produce installed trust or a startup permit. */
public class SegmentClassicLaunchTrustTest {
    private static final byte[] PUBLIC = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();
    private Path root;
    private Path recipe;
    private Path key;
    private long uid;

    private static byte[] vector() throws IOException {
        try (var input = SegmentClassicLaunchTrustTest.class.getResourceAsStream(
                "/unified-wal-segment/classic-launch-recipe-v1.hex")) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }

    private static void write(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes);
        Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--"));
    }

    @Before
    public void fixture() throws Exception {
        uid = new UnixSystem().getUid();
        Path parent = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwx------"));
        root = Files.createDirectory(parent.resolve("trust"));
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        recipe = root.resolve("classic-1.recipe");
        key = root.resolve("issuer-1.ed25519");
        write(recipe, vector());
        write(key, PUBLIC);
    }

    private SegmentClassicLaunchTrust owner() {
        return SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", uid);
    }

    private void rejected() throws Exception {
        var owner = owner();
        try {
            assertThrows(IOException.class, owner::initialize);
            assertThrows(IOException.class, owner::parsed);
            assertThrows(IOException.class, owner::installed);
        } finally {
            assertTrue(owner.closeAndDrain().drained());
        }
    }

    @Test
    public void realPublicFilesMatchIndependentRecipeVectorButFixtureCannotAcquireInstalledProvenance()
            throws Exception {
        var owner = owner();
        assertThrows(IOException.class, owner::parsed);
        owner.initialize();
        var parsed = owner.parsed();
        assertEquals("classic-1", parsed.recipe().recipeId());
        assertEquals("deployment-1", parsed.recipe().deployment());
        assertEquals("issuer-1", parsed.recipe().keyName());
        assertEquals(1001, parsed.recipe().oldUid());
        assertEquals(20, parsed.recipe().primaryGid());
        assertEquals(java.util.List.of(20L, 21L), parsed.recipe().groups());
        assertArrayEquals(PUBLIC, Arrays.copyOfRange(parsed.publicKey().getEncoded(), 12, 44));
        assertEquals("bcc36aef3bb13800478055cfc37b7befdccb74089951b2e2d75bdc5f8f3f7270",
                HexFormat.of().formatHex(parsed.recipeIdentity().value()));
        assertThrows(IOException.class, owner::installed);
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertThrows(IOException.class, owner::parsed);
        assertArrayEquals(vector(), Files.readAllBytes(recipe));
        assertArrayEquals(PUBLIC, Files.readAllBytes(key));
    }

    @Test
    public void parsedTrustWithAnOrdinaryNativeBackendCannotPrepareTheProductionLoader() throws Exception {
        var fixture = owner();
        fixture.initialize(); // Actual file/ACL I/O uses the ordinary backend in this unqualified JVM.
        assertEquals("classic-1", fixture.parsed().recipe().recipeId());
        var preload = SegmentClassicLaunchPreload.production("classic-1",
                System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.production(preload));
        assertTrue(preload.closeAndDrain().drained());
        assertThrows(IOException.class, fixture::installed);
        assertTrue(fixture.closeAndDrain().drained());
    }

    @Test
    public void constructionDoesNoIoAndMissingRootsFilesAndKeyLengthsAreNeverRepaired() throws Exception {
        Path missing = root.resolve("missing");
        var owner = SegmentClassicLaunchTrust.forMaterialIoTests(missing, "classic-1", uid);
        assertFalse(Files.exists(missing));
        assertThrows(IOException.class, owner::initialize);
        assertTrue(owner.closeAndDrain().drained());
        assertFalse(Files.exists(missing));
        Files.delete(key);
        rejected();
        assertFalse(Files.exists(key));
        write(key, new byte[31]);
        rejected();
        write(key, new byte[33]);
        rejected();
        assertEquals(33, Files.size(key));
        Files.delete(recipe);
        rejected();
        assertFalse(Files.exists(recipe));
    }

    @Test
    public void malformedFramingCredentialsAndPathSelectorsCannotChooseAKey() throws Exception {
        for (int[] mutation : java.util.List.of(new int[]{8, 0x00010001}, new int[]{12, 0},
                new int[]{179, 0}, new int[]{183, 0}, new int[]{193, 20})) {
            byte[] bytes = vector();
            ByteBuffer.wrap(bytes).putInt(mutation[0], mutation[1]);
            write(recipe, bytes);
            rejected();
        }
        byte[] pathKey = vector();
        pathKey[43] = '/';
        write(recipe, pathKey);
        rejected();
        write(recipe, new byte[4097]);
        rejected();
        write(recipe, Arrays.copyOf(vector(), 16));
        rejected();
    }

    @Test
    public void aliasesWrongOwnerAndWritableFilesOrParentsRejectBeforeAnyAdoption() throws Exception {
        Path held = root.resolve("held");
        Files.move(key, held);
        Files.createSymbolicLink(key, held);
        rejected();
        Files.delete(key);
        Files.createLink(key, held);
        rejected();
        Files.delete(key);
        Files.move(held, key);
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-rw-rw-"));
        rejected();
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-r--r--"));
        Files.setPosixFilePermissions(root.getParent(), PosixFilePermissions.fromString("rwxrwxrwx"));
        rejected();
        Files.setPosixFilePermissions(root.getParent(), PosixFilePermissions.fromString("rwx------"));
        var other = SegmentClassicLaunchTrust.forMaterialIoTests(root, "classic-1", uid == 0 ? 1 : 0);
        assertThrows(IOException.class, other::initialize);
        assertTrue(other.closeAndDrain().drained());
    }

    @Test
    public void replacementAfterOpenRejectsEvenIdenticalRecipeBytes() throws Exception {
        var owner = owner();
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && recipe.equals(invocation.getArgument(0))) {
                Files.move(recipe, root.resolve("original"));
                write(recipe, vector());
            }
            return actual;
        })) {
            assertTrue(assertThrows(IOException.class, owner::initialize).getMessage().contains("identity changed"));
            assertThrows(IOException.class, owner::parsed);
        }
        assertTrue(owner.closeAndDrain().drained());
        assertArrayEquals(Files.readAllBytes(root.resolve("original")), Files.readAllBytes(recipe));
    }

    @Test
    public void failedFirstNativeCloseRetainsTheActualDescriptorAndCannotExposeTrust() throws Exception {
        var owner = owner();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("public key first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && key.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class, owner::initialize));
            assertThrows(IOException.class, owner::parsed);
            assertThrows(IOException.class, owner::installed);
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.files().closeFailure());
            }
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    private Path configuration(byte[] content) throws Exception {
        var frozen = SegmentClassicLaunchConfiguration.parse(content);
        byte[] recipeBytes = vector();
        ByteBuffer.wrap(recipeBytes).position(115).put(frozen.identity().value());
        write(recipe, recipeBytes);
        Path configuration = root.resolve("classic-1.conf");
        write(configuration, content);
        Files.setPosixFilePermissions(configuration, PosixFilePermissions.fromString("rw-------"));
        return configuration;
    }

    @Test
    public void fixedConfigurationReadProducesOnlyBoundedImmutableDataAndRecipeMismatchPoisonsTheAttempt()
            throws Exception {
        byte[] bytes = SegmentClassicLaunchConfigurationTest.configuration();
        Path configuration = configuration(bytes);
        var owner = owner();
        owner.initialize();
        var frozen = owner.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
        assertEquals(2, frozen.properties().get("serverNumIOThreads"));
        assertThrows(IOException.class, () -> owner.verifyConfiguration(Long.MAX_VALUE));
        Files.writeString(configuration, "after-read mutation");
        assertEquals(frozen.identity(), SegmentClassicLaunchConfiguration.identity(frozen.properties()));
        assertThrows(IOException.class, owner::installed);
        assertTrue(owner.closeAndDrain().drained());

        configuration(bytes);
        Files.writeString(configuration, new String(bytes, StandardCharsets.UTF_8) + "additional=covered\n");
        var mismatched = owner();
        mismatched.initialize();
        var failure = assertThrows(IOException.class,
                () -> mismatched.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
        assertTrue(failure.getMessage().contains("recipe digest"));
        assertThrows(IOException.class, mismatched::parsed);
        var result = mismatched.closeAndDrain();
        assertTrue(result.drained());
        assertSame(failure, result.configurationFailure());

        configuration(bytes);
        Files.setPosixFilePermissions(configuration, PosixFilePermissions.fromString("rw-r--r--"));
        var publicCredentials = owner();
        publicCredentials.initialize();
        assertThrows(IOException.class,
                () -> publicCredentials.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1)));
        assertTrue(publicCredentials.closeAndDrain().drained());
    }

    @Test
    public void configurationFirstNativeCloseFailureNeverReturnsParsedDataOrRetiresTheOriginalHandle()
            throws Exception {
        Path configuration = configuration(SegmentClassicLaunchConfigurationTest.configuration());
        var owner = owner();
        owner.initialize();
        var captured = new AtomicReference<SegmentDataFileLockTest.FaultChannel>();
        IOException failure = new IOException("configuration first native close");
        try (var opens = mockStatic(FileChannel.class, invocation -> {
            Object actual = invocation.callRealMethod();
            if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                    && configuration.equals(invocation.getArgument(0))) {
                var fault = new SegmentDataFileLockTest.FaultChannel((FileChannel) actual);
                fault.closeFailure = failure;
                captured.set(fault);
                return fault;
            }
            return actual;
        })) {
            assertSame(failure, assertThrows(IOException.class,
                    () -> owner.verifyConfiguration(System.nanoTime() + TimeUnit.MINUTES.toNanos(1))));
            assertThrows(IOException.class, owner::parsed);
        }
        try {
            for (int i = 0; i < 2; i++) {
                var result = owner.closeAndDrain();
                assertFalse(result.drained());
                assertSame(failure, result.files().closeFailure());
                assertSame(failure, result.configurationFailure());
            }
            assertEquals(1, captured.get().closes.get());
            assertTrue(captured.get().actual.isOpen());
        } finally {
            captured.get().actual.close();
        }
        assertFalse(owner.closeAndDrain().drained());
    }

    @Test(timeout = 30000)
    public void externalCloseWaitsForTheActualOpenBodyAndSelfDrainIsRejected() throws Exception {
        var owner = owner();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var read = new FutureTask<Void>(() -> {
            try (var opens = mockStatic(FileChannel.class, invocation -> {
                Object actual = invocation.callRealMethod();
                if (invocation.getMethod().getName().equals("open") && invocation.getMethod().getParameterCount() == 3
                        && recipe.equals(invocation.getArgument(0))) {
                    assertThrows(IllegalStateException.class, owner::closeAndDrain);
                    entered.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return actual;
            })) {
                owner.initialize();
            }
            return null;
        });
        var close = new FutureTask<>(owner::closeAndDrain);
        Thread reader = new Thread(read);
        Thread closer = new Thread(close);
        try {
            reader.start();
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            closer.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> closer.getState() == Thread.State.BLOCKED);
            assertThrows(TimeoutException.class, () -> close.get(20, TimeUnit.MILLISECONDS));
            release.countDown();
            read.get(5, TimeUnit.SECONDS);
            assertTrue(close.get(5, TimeUnit.SECONDS).drained());
        } finally {
            release.countDown();
            reader.join(5000);
            if (closer.getState() != Thread.State.NEW) {
                closer.join(5000);
            }
            assertTrue(owner.closeAndDrain().drained());
        }
        assertFalse(reader.isAlive());
        assertFalse(closer.isAlive());
    }

    @Test
    public void additionalAclOnFileOrParentRejectsDespitePrivatePosixMode() throws Exception {
        assumeTrue(SystemUtils.IS_OS_MAC_OSX);
        for (Path member : new Path[]{key, root.getParent()}) {
            var mode = Files.getPosixFilePermissions(member);
            Process command = new ProcessBuilder("/bin/chmod", "+a", Files.isDirectory(member)
                    ? "everyone allow delete_child" : "everyone allow write", member.toString())
                    .redirectErrorStream(true).start();
            try {
                assertTrue(command.waitFor(5, TimeUnit.SECONDS));
                assertEquals(new String(command.getInputStream().readAllBytes(), StandardCharsets.UTF_8),
                        0, command.exitValue());
            } finally {
                if (command.isAlive()) {
                    command.destroyForcibly();
                    assertTrue(command.waitFor(5, TimeUnit.SECONDS));
                }
                command.getInputStream().close();
                command.getOutputStream().close();
                command.getErrorStream().close();
            }
            assertEquals(mode, Files.getPosixFilePermissions(member));
            rejected();
            if (member.equals(key)) {
                Files.delete(key);
                write(key, PUBLIC);
            }
        }
    }
}

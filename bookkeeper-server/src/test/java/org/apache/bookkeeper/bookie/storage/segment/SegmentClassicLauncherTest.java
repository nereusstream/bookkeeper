/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.server.ControlledClassicLauncherMain;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Parent credential values, actual heap and unqualified Main only; no installed issuer or START context. */
public class SegmentClassicLauncherTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static SegmentClassicLauncherInputs groupInput(long group) throws IOException {
        var d = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2")).versionTwo();
        var service = d.units().get(d.launcherUnit());
        var target = d.units().get(1);
        // Keep the complete validated descriptor, but put the launcher at a nonzero original index.
        target = new SegmentClassicLinuxDeploymentV2.Unit(target.kind(), "default.target", target.names(),
                target.fileState(), target.fragment(), target.dropIns(), target.commonRule(), target.relations(), null);
        service = new SegmentClassicLinuxDeploymentV2.Unit(service.kind(), service.id(), service.names(),
                service.fileState(), service.fragment(), service.dropIns(), service.commonRule(), service.relations(),
                new SegmentClassicLinuxDeploymentV2.Service(group, service.service().execution(),
                        service.service().constraint()));
        var links = d.installLinks().stream().map(l -> new SegmentClassicLinuxDeploymentV2.InstallLink(l.kind(),
                l.path(), l.target(), l.sourceUnit() < 0 ? -1 : 1 - l.sourceUnit(), 1 - l.targetUnit())).toList();
        var deployment = new SegmentClassicLinuxDeployment(new SegmentClassicLinuxDeploymentV2(d.platform(),
                d.files(), d.provider(), d.loading(), d.startChain(), List.of(target, service), 1, links,
                d.isolation(), d.operations()));
        var old = SegmentClassicLauncherInputs.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v2"));
        var child = SegmentClassicLaunchProgram.identity(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1"));
        var dropper = d.files().get(d.isolation().dropper());
        return new SegmentClassicLauncherInputs(old.recipeId(), old.recipe(), old.signerKeyId(),
                SegmentClassicLauncherInputs.parentProgramIdentity(child, deployment), old.os(), old.architecture(),
                new SegmentClassicLaunchProgram.File(Path.of(dropper.path()), SegmentClassicLaunchProgram.NATIVE,
                        dropper.length(), dropper.sha256()), old.supervisorDirectory(), old.targetUid(), old.manifest(),
                old.proof(), old.policy(), old.aesKey(), deployment);
    }

    @Test
    public void d2ParentRequiresBothPrimaryGroupsFromItsOriginalLauncherUnit() throws Exception {
        for (long group : List.of(20L, 0xffff_fffeL)) {
            var input = groupInput(group);
            assertEquals(1, input.linuxDeployment().versionTwo().launcherUnit());
            SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 0, group, group, List.of(7L)));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 0, 0, group, List.of(group))));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 0, group, 0, List.of(group))));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(1, 0, group, group, List.of(group))));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 1, group, group, List.of(group))));
        }
    }

    @Test
    public void legacyAndD1ParentKeepUidOnlyChecksWithoutConstrainingPrimaryGroups() throws Exception {
        for (String name : List.of("classic-launcher-inputs-v1", "classic-launcher-inputs-v2")) {
            var input = SegmentClassicLauncherInputs.decode(SegmentClassicLinuxDeploymentTest.vector(name));
            SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 0, 123, 0xffff_fffeL, List.of()));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(1, 0, 123, 0xffff_fffeL, List.of())));
            assertThrows(IOException.class, () -> SegmentClassicLauncher.requireParentCredentials(input,
                    new NativeIO.ProcessCredentials(0, 1, 123, 0xffff_fffeL, List.of())));
        }
    }

    @Test
    public void passiveWaitInvalidDeadlinesAndRejectedHandoffDoNotStartOrRetireTheOwner() throws Exception {
        var owner = SegmentClassicLauncher.create("unstarted-parent");
        assertEquals(SegmentClassicLauncher.State.NEW, owner.status().state());
        assertThrows(TimeoutException.class, () -> owner.awaitTermination(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> owner.closeAndAwait(Duration.ofNanos(-1)));
        assertThrows(ArithmeticException.class, () -> owner.closeAndAwait(Duration.ofSeconds(Long.MAX_VALUE)));
        assertThrows(IOException.class, () -> owner.requestHandoff(UUID.randomUUID()));
        assertEquals(SegmentClassicLauncher.State.NEW, owner.status().state());
        assertEquals(SegmentClassicLauncher.State.DRAINED, owner.closeAndAwait(Duration.ZERO).state());
        assertEquals(SegmentClassicLauncher.State.DRAINED, owner.awaitTermination(Duration.ZERO).state());
        assertThrows(IOException.class, owner::start);
    }

    @Test(timeout = 10000)
    public void libraryCannotStartAnIssuerAndFailureWaitsForTheOriginalWorker() throws Exception {
        var owner = SegmentClassicLauncher.create("not-the-fixed-parent-main");
        assertThrows(IOException.class, owner::start);
        var ended = owner.awaitTermination(Duration.ofSeconds(5));
        assertEquals(SegmentClassicLauncher.State.FAILED, ended.state());
        assertNotNull(ended.initializationFailure());
        assertEquals(ended, owner.awaitTermination(Duration.ZERO));
    }

    @Test(timeout = 30000)
    public void realLauncherMainWithUninstalledInputsExitsFailedWithoutStartingAReplacementChild() throws Exception {
        Path error = temporary.getRoot().toPath().resolve("parent.err");
        var arguments = List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2", "-cp",
                System.getProperty("java.class.path"), ControlledClassicLauncherMain.class.getName(),
                "uninstalled-parent-" + UUID.randomUUID());
        var builder = new ProcessBuilder(arguments).redirectError(error.toFile());
        builder.environment().clear();
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS));
            assertFalse(process.exitValue() == 0);
            assertArrayEquals(new byte[0], process.getInputStream().readAllBytes());
            String failure = Files.readString(error, StandardCharsets.UTF_8);
            assertTrue(failure, failure.contains("Controlled Classic launcher startup failed"));
            assertTrue(failure, failure.contains("Controlled Classic launcher episode failed"));
        } finally {
            process.getOutputStream().close();
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }
}

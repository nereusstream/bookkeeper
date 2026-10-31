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
import org.apache.bookkeeper.server.ControlledClassicLauncherMain;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual heap and unqualified Main failures only; no fake installed issuer, Process adoption or START context. */
public class SegmentClassicLauncherTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

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

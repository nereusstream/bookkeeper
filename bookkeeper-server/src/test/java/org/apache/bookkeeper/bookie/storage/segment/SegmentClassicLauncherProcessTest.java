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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Real Process/pipe transport only. The probe cannot issue or consume production START/Installed authority. */
public class SegmentClassicLauncherProcessTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private record Run(SegmentClassicLauncherProcess transport,
                       AtomicReference<SegmentClassicLauncherProcess.Identity> identity,
                       FutureTask<SegmentClassicLauncherProcess.Drain> result, Thread owner,
                       CountDownLatch ready, Path release, Path terminal) { }

    private Run run(String mode) throws Exception {
        Path directory = temporary.newFolder().toPath();
        Path release = directory.resolve("release");
        Path terminal = directory.resolve("terminal");
        UUID episode = UUID.randomUUID();
        var transport = new SegmentClassicLauncherProcess();
        var identity = new AtomicReference<SegmentClassicLauncherProcess.Identity>();
        var ready = new CountDownLatch(1);
        var task = new FutureTask<SegmentClassicLauncherProcess.Drain>(() -> {
            var command = List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                    "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2", "-cp",
                    System.getProperty("java.class.path"), PipeProbe.class.getName(), mode, episode.toString(),
                    release.toString(), terminal.toString());
            var builder = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT);
            builder.environment().clear();
            SegmentClassicLauncherProcess.Drain result;
            try {
                transport.initialize(builder);
                identity.set(transport.identity());
                assertEquals(16, transport.awaitHello(Duration.ofSeconds(10)).length);
                // Opaque framing data deliberately contains no UWCLA001, valid signature or startup permission.
                transport.flushStart(new byte[112], episode);
                if (mode.equals("early-failure") || mode.equals("early-normal")) {
                    assertThrows(IOException.class, () -> transport.awaitReady(Duration.ofSeconds(10)));
                } else {
                    var facts = transport.awaitReady(Duration.ofSeconds(10));
                    assertEquals(1234, facts.session());
                    assertEquals(5678, facts.birth());
                    assertFalse(facts.readOnly());
                    ready.countDown();
                    if (mode.equals("lost-control") || mode.equals("bad-frame")) {
                        transport.awaitCloseRequested();
                    } else {
                        transport.requestClose();
                    }
                }
            } finally {
                transport.requestClose();
                result = transport.closeAndDrain();
            }
            return result;
        });
        Thread owner = new Thread(task, "fixture-original-process-owner");
        owner.start();
        return new Run(transport, identity, task, owner, ready, release, terminal);
    }

    private static void cleanup(Run run) throws Exception {
        run.transport().requestClose();
        Files.write(run.release(), new byte[0]);
        var known = run.identity().get();
        if (known != null) {
            var handle = ProcessHandle.of(known.pid());
            if (handle.isPresent() && handle.get().isAlive()) {
                var actual = handle.get().info();
                if (actual.startInstant().orElseThrow().equals(known.started())
                        && List.of(actual.arguments().orElseThrow()).contains(PipeProbe.class.getName())) {
                    // Fixture cleanup only; a forced exit never counts as a successful drain assertion.
                    handle.get().destroyForcibly();
                    handle.get().onExit().get(5, TimeUnit.SECONDS);
                }
            }
        }
        run.owner().join(5000);
        assertFalse(run.owner().isAlive());
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(run.transport()::readerTerminated);
    }

    @Test(timeout = 30000)
    public void normalStopFlushesAndClosesInputBeforeWaitingForTheOriginalProcess() throws Exception {
        var run = run("normal");
        try {
            var result = run.result().get(15, TimeUnit.SECONDS);
            assertTrue(result.drained());
            assertTrue(result.processEnded());
            assertEquals(Integer.valueOf(0), result.exitCode());
            assertNull(result.failure());
            assertNull(result.closeFailure());
        } finally {
            cleanup(run);
        }
    }

    @Test(timeout = 30000)
    public void earlyFailureAndUnrequestedNormalResultStopSendingBeforeWakingTheOwner() throws Exception {
        for (String mode : List.of("early-failure", "early-normal")) {
            var run = run(mode);
            try {
                var result = run.result().get(15, TimeUnit.SECONDS);
                assertTrue(result.drained());
                assertNotNull(result.failure());
                assertNull(result.closeFailure());
                assertEquals(Integer.valueOf(mode.equals("early-failure") ? 1 : 0), result.exitCode());
            } finally {
                cleanup(run);
            }
        }
    }

    @Test(timeout = 30000)
    public void outputEofAndBadFrameCloseInputWithoutSendingStopAfterTheFailure() throws Exception {
        for (String mode : List.of("lost-control", "bad-frame")) {
            var run = run(mode);
            try {
                assertTrue(run.ready().await(10, TimeUnit.SECONDS));
                Files.write(run.release(), new byte[0]);
                var result = run.result().get(15, TimeUnit.SECONDS);
                assertTrue(result.drained());
                assertNotNull(result.failure());
                assertNull(result.closeFailure());
                assertEquals(Integer.valueOf(0), result.exitCode()); // Probe rejects any late STOP bytes.
            } finally {
                cleanup(run);
            }
        }
    }

    @Test(timeout = 30000)
    public void componentResultDoesNotFinishWhileOriginalOutputReaderAndProcessRemainHeld() throws Exception {
        var run = run("held-output");
        try {
            assertTrue(run.ready().await(10, TimeUnit.SECONDS));
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> Files.exists(run.terminal()));
            assertThrows(TimeoutException.class, () -> run.result().get(20, TimeUnit.MILLISECONDS));
            assertFalse(run.transport().readerTerminated());
            Files.write(run.release(), new byte[0]);
            var result = run.result().get(10, TimeUnit.SECONDS);
            assertTrue(result.drained());
            assertEquals(Integer.valueOf(0), result.exitCode());
            assertNull(result.failure());
        } finally {
            cleanup(run);
        }
    }

    /** No storage, credentials, signature verification, registration or production child code is invoked. */
    public static final class PipeProbe {
        public static void main(String[] arguments) throws Exception {
            System.setOut(System.err);
            String mode = arguments[0];
            UUID episode = UUID.fromString(arguments[1]);
            Path release = Path.of(arguments[2]);
            Path terminal = Path.of(arguments[3]);
            var control = new SegmentClassicChild.Control();
            control.open();
            control.write(SegmentClassicChild.Control.HELLO, new byte[16]);
            control.read(SegmentClassicChild.Control.START);
            boolean early = mode.equals("early-failure") || mode.equals("early-normal");
            if (!early) {
                control.write(SegmentClassicChild.Control.READY, ByteBuffer.allocate(40)
                        .putLong(episode.getMostSignificantBits()).putLong(episode.getLeastSignificantBits())
                        .putLong(1234).putLong(5678).put((byte) 1).put(new byte[7]).array());
            }
            if (mode.equals("lost-control") || mode.equals("bad-frame")) {
                waitFor(release);
                if (mode.equals("bad-frame")) {
                    var output = new java.io.FileOutputStream(java.io.FileDescriptor.out);
                    output.write(ByteBuffer.allocate(16).putInt(0x55574343).putShort((short) 1)
                            .putShort((short) 5).putInt(1).putInt(24).array());
                    output.flush();
                }
                control.closeOutput();
                control.requireOutputEof(); // Receiving STOP here is a real protocol failure.
            } else {
                if (!early) {
                    byte[] stopped = control.read(SegmentClassicChild.Control.STOP);
                    assertEquals(episode, new UUID(ByteBuffer.wrap(stopped).getLong(),
                            ByteBuffer.wrap(stopped).getLong(8)));
                    control.requireOutputEof();
                }
                control.write(SegmentClassicChild.Control.COMPONENTS_TERMINAL, ByteBuffer.allocate(24)
                        .putLong(episode.getMostSignificantBits()).putLong(episode.getLeastSignificantBits())
                        .putInt(mode.equals("early-failure") ? 1 : 0).putInt(0).array());
                if (mode.equals("held-output")) {
                    Files.write(terminal, new byte[0]);
                    waitFor(release);
                }
                control.closeOutput();
                if (early) {
                    control.requireOutputEof();
                }
            }
            control.closeInput();
            assertTrue(control.drained());
            if (mode.equals("early-failure")) {
                throw new IllegalStateException("Fixture reported failed components");
            }
        }

        private static void waitFor(Path release) throws Exception {
            while (!Files.exists(release)) {
                Thread.sleep(5);
            }
        }
    }
}

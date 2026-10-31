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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.bookie.ClassicChildResources;
import org.apache.bookkeeper.server.ControlledClassicMain;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Heap/API, bounded pipe bytes and real unqualified process failure; never a production START fixture. */
public class SegmentClassicChildTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void passiveWaitAndInvalidDeadlinesDoNotStartOrCloseTheHeapOnlyOwner() throws Exception {
        var owner = SegmentClassicChild.create("unstarted");
        assertEquals(SegmentClassicChild.State.NEW, owner.status().state());
        assertThrows(TimeoutException.class, () -> owner.awaitTermination(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> owner.closeAndAwait(Duration.ofNanos(-1)));
        assertThrows(ArithmeticException.class, () -> owner.closeAndAwait(Duration.ofSeconds(Long.MAX_VALUE)));
        assertEquals(SegmentClassicChild.State.NEW, owner.status().state());
        assertEquals(SegmentClassicChild.State.DRAINED, owner.closeAndAwait(Duration.ZERO).state());
        assertEquals(SegmentClassicChild.State.DRAINED, owner.awaitTermination(Duration.ZERO).state());
        assertThrows(IOException.class, owner::start);
        var bridge = ClassicChildResources.prepare();
        assertThrows(IOException.class, bridge::prepareNetwork);
        assertEquals(ClassicChildResources.State.DRAINED, bridge.closeAndDrain().state());
    }

    @Test
    public void fixedPipeHeaderAndOneUseDoNotChangeExistingLaunchPayloadBytes() throws Exception {
        var output = new ByteArrayOutputStream();
        var writer = new SegmentClassicChild.Control(new ByteArrayInputStream(new byte[0]), output);
        byte[] payload = new byte[16];
        writer.write(SegmentClassicChild.Control.HELLO, payload);
        assertArrayEquals(HexFormat.of().parseHex("55574343000100010000000000000010"),
                java.util.Arrays.copyOf(output.toByteArray(), 16));
        assertEquals(32, output.size());
        assertThrows(IOException.class, () -> writer.write(SegmentClassicChild.Control.HELLO, payload));
        assertEquals(32, output.size());
        byte[] frame = output.toByteArray();
        output.write(frame);
        var reader = new SegmentClassicChild.Control(new ByteArrayInputStream(output.toByteArray()),
                new ByteArrayOutputStream());
        assertArrayEquals(payload, reader.read(SegmentClassicChild.Control.HELLO));
        assertThrows(IOException.class, () -> reader.read(SegmentClassicChild.Control.HELLO));
    }

    @Test
    public void invalidHeadersRejectBeforeReadingOrAllocatingTheirAdvertisedBody() {
        for (int field : List.of(0, 4, 6, 8, 12)) {
            byte[] encoded = ByteBuffer.allocate(16).putInt(0x55574343).putShort((short) 1)
                    .putShort((short) SegmentClassicChild.Control.START).putInt(0).putInt(112).array();
            if (field == 0) {
                ByteBuffer.wrap(encoded).putInt(0, 0);
            } else if (field == 4) {
                ByteBuffer.wrap(encoded).putShort(4, (short) 2);
            } else if (field == 6) {
                ByteBuffer.wrap(encoded).putShort(6, (short) SegmentClassicChild.Control.STOP);
            } else if (field == 8) {
                ByteBuffer.wrap(encoded).putInt(8, 1);
            } else {
                ByteBuffer.wrap(encoded).putInt(12, Integer.MAX_VALUE);
            }
            InputStream boundedHeader = new ByteArrayInputStream(encoded) {
                @Override
                public int read(byte[] destination, int offset, int length) {
                    assertEquals("Only the fixed header may be read", 16, length);
                    System.arraycopy(encoded, 0, destination, offset, length);
                    return length;
                }
            };
            var reader = new SegmentClassicChild.Control(boundedHeader, new ByteArrayOutputStream());
            assertThrows(IOException.class, () -> reader.read(SegmentClassicChild.Control.START));
        }
    }

    @Test
    public void fragmentedFramesReadCompletelyAndTruncatedPayloadCannotBecomeAnAuthorization() throws Exception {
        var bytes = new ByteArrayOutputStream();
        byte[] payload = new byte[112];
        java.util.Arrays.fill(payload, (byte) 7);
        new SegmentClassicChild.Control(new ByteArrayInputStream(new byte[0]), bytes)
                .write(SegmentClassicChild.Control.START, payload);
        InputStream fragments = new ByteArrayInputStream(bytes.toByteArray()) {
            @Override
            public int read(byte[] buffer, int offset, int length) {
                return super.read(buffer, offset, Math.min(length, 3));
            }
        };
        assertArrayEquals(payload, new SegmentClassicChild.Control(fragments, new ByteArrayOutputStream())
                .read(SegmentClassicChild.Control.START));
        var truncated = new SegmentClassicChild.Control(new ByteArrayInputStream(
                java.util.Arrays.copyOf(bytes.toByteArray(), bytes.size() - 1)), new ByteArrayOutputStream());
        assertThrows(IOException.class, () -> truncated.read(SegmentClassicChild.Control.START));
    }

    @Test(timeout = 10000)
    public void waitingBeforeStartAndMultipleObserversReturnOnlyTheActualFailedEpisode() throws Exception {
        var owner = SegmentClassicChild.create("not-the-fixed-main");
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        var waiters = Executors.newFixedThreadPool(2, task -> {
            Thread thread = new Thread(task);
            threads.add(thread);
            return thread;
        });
        try {
            var first = waiters.submit(() -> owner.awaitTermination(Duration.ofSeconds(5)));
            var second = waiters.submit(() -> owner.awaitTermination(Duration.ofSeconds(5)));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> threads.size() == 2
                    && threads.stream().allMatch(thread -> thread.getState() == Thread.State.TIMED_WAITING));
            assertEquals(SegmentClassicChild.State.NEW, owner.status().state());
            assertThrows(IOException.class, owner::start);
            assertEquals(SegmentClassicChild.State.FAILED, first.get(5, TimeUnit.SECONDS).state());
            assertEquals(SegmentClassicChild.State.FAILED, second.get(5, TimeUnit.SECONDS).state());
            assertNotNull(owner.awaitTermination(Duration.ZERO).initializationFailure());
        } finally {
            owner.requestClose();
            waiters.shutdown();
            assertTrue(waiters.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 30000)
    public void realMainFailureClosesOutputBeforeWaitingForParentInputEofAndExitsWithoutStop() throws Exception {
        for (byte[] incomplete : List.of(new byte[0], new byte[]{0x55, 0x57, 0x43, 0x43, 0x00})) {
            failedMainWithOpenInput(incomplete);
        }
    }

    private void failedMainWithOpenInput(byte[] incomplete) throws Exception {
        Path error = temporary.getRoot().toPath().resolve("child-" + incomplete.length + ".err");
        var arguments = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2", "-cp",
                System.getProperty("java.class.path"), ControlledClassicMain.class.getName(),
                "uninstalled-" + UUID.randomUUID()));
        var builder = new ProcessBuilder(arguments).redirectError(error.toFile());
        builder.environment().clear();
        Process process = builder.start();
        var outputReader = Executors.newSingleThreadExecutor();
        try {
            process.getOutputStream().write(incomplete);
            process.getOutputStream().flush();
            var output = outputReader.submit(() -> process.getInputStream().readAllBytes());
            assertArrayEquals(new byte[0], output.get(10, TimeUnit.SECONDS));
            assertTrue("The original reader must still await the parent's input EOF", process.isAlive());
            assertFalse(process.waitFor(100, TimeUnit.MILLISECONDS));
            process.getOutputStream().close();
            assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            assertFalse(process.exitValue() == 0);
            String failure = Files.readString(error, StandardCharsets.UTF_8);
            assertTrue(failure, failure.contains("Controlled Classic startup failed"));
            assertTrue(failure, failure.contains("Controlled Classic episode failed"));
        } finally {
            process.getOutputStream().close();
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
            }
            process.getInputStream().close();
            process.getErrorStream().close();
            outputReader.shutdown();
            assertTrue(outputReader.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}

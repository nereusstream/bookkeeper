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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** The original Process and its pipes only. No key, installed trust, START issuance or handoff authority. */
final class SegmentClassicLauncherProcess {
    record Identity(long pid, Instant started, String command, List<String> arguments, boolean alive) {
        Identity {
            arguments = List.copyOf(arguments);
        }
    }

    record Ready(long session, long birth, boolean readOnly) { }
    record Drain(boolean drained, boolean processEnded, Integer exitCode, Throwable failure,
                 Throwable closeFailure, boolean acquisitionUnknown) { }

    private final Object gate = new Object();
    // Retain the heap pipe holder before Process construction can acquire native descriptors.
    private final SegmentClassicChild.Control control = new SegmentClassicChild.Control();
    private Thread originalCaller;
    private Thread reader;
    private Process process;
    private Instant createdStart;
    private boolean initializeAttempted;
    private boolean processAcquiring;
    private boolean initialized;
    private boolean closing;
    private boolean startAttempted;
    private boolean startFlushed;
    private boolean stopSending;
    private UUID episode;
    private byte[] hello;
    private Ready ready;
    private Integer terminalResult;
    private Throwable failure;
    private Throwable closeFailure;
    private boolean processWaitAttempted;
    private Integer exitCode;
    private boolean errorCloseAttempted;

    void initialize(ProcessBuilder approvedBuilder) throws IOException {
        synchronized (gate) {
            if (initializeAttempted || closing) {
                throw new IOException("Original supervised Process initialization is one-use");
            }
            initializeAttempted = true;
            originalCaller = Thread.currentThread();
            processAcquiring = true;
        }
        try {
            process = approvedBuilder.start();
            processAcquiring = false;
            control.attach(process.getInputStream(), process.getOutputStream());
            createdStart = process.info().startInstant().orElseThrow(
                    () -> new IOException("Original Process start time is unavailable"));
            reader = new Thread(this::readControl, "controlled-classic-launcher-reader");
            reader.setDaemon(false);
            reader.start();
            initialized = true;
        } catch (IOException | RuntimeException | Error problem) {
            fail(problem);
            throw problem;
        }
    }

    boolean isCurrentThread() {
        return Thread.currentThread() == reader;
    }

    boolean readerTerminated() {
        return reader == null || !reader.isAlive();
    }

    Identity identity() throws IOException {
        requireCaller();
        if (process == null) {
            throw new IOException("Original Process has not returned its handle");
        }
        var info = process.info();
        Instant actualStart = info.startInstant().orElseThrow(
                () -> new IOException("Original Process start time is unavailable"));
        if (!actualStart.equals(createdStart)) {
            throw new IOException("Original supervised Process birth changed");
        }
        return new Identity(process.pid(), actualStart, info.command().orElseThrow(
                () -> new IOException("Original Process command is unavailable")), List.of(info.arguments().orElseThrow(
                () -> new IOException("Original Process arguments are unavailable"))), process.isAlive());
    }

    Instant createdStartTime() throws IOException {
        requireCaller();
        if (createdStart == null) {
            throw new IOException("Original supervised Process birth is unavailable");
        }
        return createdStart;
    }

    byte[] awaitHello(Duration timeout) throws IOException, InterruptedException {
        requireCaller();
        long end = System.nanoTime() + positive(timeout);
        synchronized (gate) {
            while (hello == null) {
                awaitProgress(end);
            }
            requireActive();
            return hello.clone();
        }
    }

    void flushStart(byte[] originalAuthorization, UUID originalEpisode) throws IOException {
        requireCaller();
        synchronized (gate) {
            requireActive();
            if (startAttempted || originalEpisode.equals(new UUID(0, 0))) {
                throw new IOException("Original supervised Process accepts one START transmission");
            }
            startAttempted = true;
            episode = Objects.requireNonNull(originalEpisode, "episode");
        }
        try {
            control.write(SegmentClassicChild.Control.START, originalAuthorization);
        } catch (IOException | RuntimeException | Error problem) {
            fail(problem); // Retire sending before waking the closing owner, including partial writes.
            throw problem;
        }
        synchronized (gate) {
            startFlushed = true;
            gate.notifyAll();
        }
    }

    Ready awaitReady(Duration timeout) throws IOException, InterruptedException {
        requireCaller();
        long end = System.nanoTime() + positive(timeout);
        synchronized (gate) {
            while (ready == null) {
                awaitProgress(end);
            }
            requireActive();
            return ready;
        }
    }

    void awaitCloseRequested() throws InterruptedException {
        requireCaller();
        synchronized (gate) {
            while (!closing) {
                gate.wait();
            }
        }
    }

    void requestClose() {
        synchronized (gate) {
            closing = true;
            gate.notifyAll();
        }
    }

    private void requireActive() throws IOException {
        if (!initialized || closing || failure != null) {
            throw new IOException("Original Process control is closing or failed", failure);
        }
    }

    private void awaitProgress(long end) throws IOException, InterruptedException {
        requireActive();
        long remaining = end - System.nanoTime();
        if (remaining <= 0) {
            throw new IOException("Original Process control observation deadline elapsed");
        }
        TimeUnit.NANOSECONDS.timedWait(gate, remaining);
    }

    private static long positive(Duration timeout) {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive Process control observation timeout required");
        }
        return timeout.toNanos();
    }

    private void requireCaller() {
        if (isCurrentThread() || originalCaller != null && originalCaller != Thread.currentThread()) {
            throw new IllegalStateException("Only the original Process owner may send or await control");
        }
    }

    private void readControl() {
        try {
            byte[] packet = control.read(SegmentClassicChild.Control.HELLO);
            synchronized (gate) {
                hello = packet;
                gate.notifyAll();
                while (!startFlushed && failure == null && (startAttempted || !closing)) {
                    gate.wait();
                }
                if (!startFlushed) {
                    return;
                }
            }
            var frame = control.readFrame(SegmentClassicChild.Control.READY,
                    SegmentClassicChild.Control.COMPONENTS_TERMINAL);
            if (frame.type() == SegmentClassicChild.Control.READY) {
                Ready observed = decodeReady(frame.payload());
                synchronized (gate) {
                    ready = observed;
                    gate.notifyAll();
                }
                frame = control.readFrame(SegmentClassicChild.Control.COMPONENTS_TERMINAL);
            }
            requireEpisode(frame.payload());
            ByteBuffer bytes = ByteBuffer.wrap(frame.payload());
            int code = bytes.getInt(16);
            if (code < 0 || code > 1 || bytes.getInt(20) != 0) {
                throw new IOException("Invalid child component terminal result");
            }
            synchronized (gate) {
                if (code == 0 && !closing) {
                    throw new IOException("Unexpected normal child terminal result before close");
                }
                terminalResult = code;
                stopSending = true;
                if (code == 1 && failure == null) {
                    failure = new IOException("Original child reported failed components");
                }
                closing = true;
                gate.notifyAll();
            }
            control.requireOutputEof();
        } catch (Throwable problem) {
            fail(problem);
        }
    }

    private Ready decodeReady(byte[] payload) throws IOException {
        requireEpisode(payload);
        ByteBuffer bytes = ByteBuffer.wrap(payload);
        long session = bytes.getLong(16);
        long birth = bytes.getLong(24);
        int mode = Byte.toUnsignedInt(bytes.get(32));
        if (session == 0 || birth <= 0 || mode != 1 && mode != 2) {
            throw new IOException("Child READY registration facts are invalid");
        }
        for (int i = 33; i < 40; i++) {
            if (bytes.get(i) != 0) {
                throw new IOException("Child READY reserved bytes must be zero");
            }
        }
        return new Ready(session, birth, mode == 2);
    }

    private void requireEpisode(byte[] payload) throws IOException {
        ByteBuffer bytes = ByteBuffer.wrap(payload);
        UUID observed = new UUID(bytes.getLong(), bytes.getLong());
        if (!startFlushed || !observed.equals(episode)) {
            throw new IOException("Child control report belongs to a different original episode");
        }
    }

    private void fail(Throwable problem) {
        synchronized (gate) {
            // This ordering prevents STOP from being sent after a bad frame, output EOF or failed START write.
            stopSending = true;
            if (failure == null) {
                failure = problem;
            }
            closing = true;
            gate.notifyAll();
        }
    }

    private void closeFailed(Throwable problem) {
        if (closeFailure == null) {
            closeFailure = problem;
        }
    }

    Drain closeAndDrain() {
        requireCaller();
        requestClose();
        boolean sendStop;
        synchronized (gate) {
            sendStop = startFlushed && !stopSending;
            stopSending = true;
        }
        if (sendStop) {
            try {
                control.write(SegmentClassicChild.Control.STOP, ByteBuffer.allocate(16)
                        .putLong(episode.getMostSignificantBits()).putLong(episode.getLeastSignificantBits()).array());
            } catch (Throwable problem) {
                closeFailed(problem);
            }
        }
        control.closeOutput(); // Original stdin closes before any Process or reader wait.
        if (process != null && !processWaitAttempted) {
            processWaitAttempted = true;
            try {
                exitCode = process.waitFor();
                if (reader != null) {
                    reader.join();
                }
                if (exitCode != 0 || !startFlushed || terminalResult == null || terminalResult != 0) {
                    fail(new IOException("Original child did not return a normal component/Process result"));
                }
            } catch (Throwable problem) {
                closeFailed(problem);
            }
        }
        if (readerTerminated()) {
            control.closeInput();
        }
        if (control.closeFailure() != null) {
            closeFailed(control.closeFailure());
        }
        if (process != null && !errorCloseAttempted) {
            errorCloseAttempted = true;
            try {
                process.getErrorStream().close();
            } catch (Throwable problem) {
                closeFailed(problem);
            }
        }
        boolean ended = process == null ? !processAcquiring : exitCode != null && !process.isAlive();
        return new Drain(ended && readerTerminated() && control.drained() && closeFailure == null,
                ended, exitCode, failure, closeFailure, processAcquiring);
    }
}

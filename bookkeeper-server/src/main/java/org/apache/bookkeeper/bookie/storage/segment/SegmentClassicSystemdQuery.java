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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;

/** One fixed CLI query and bounded raw property data; never unit approval or isolation authority. */
final class SegmentClassicSystemdQuery {
    static final int MAX_BYTES = 64 * 1024;
    private static final List<String> PROPERTIES = List.of("Id", "MainPID", "ExecMainPID", "InvocationID",
            "ControlGroup", "LoadState", "ActiveState", "SubState", "Transient", "Job", "ControlPID",
            "FragmentPath", "SourcePath", "DropInPaths", "UnitFileState", "NeedDaemonReload", "Type", "User",
            "Group", "DynamicUser", "ExecCondition", "ExecConditionEx", "ExecStartPre", "ExecStartPreEx",
            "ExecStart", "ExecStartEx", "ExecStartPost", "ExecStartPostEx", "ExecReload", "ExecReloadEx",
            "ExecStop", "ExecStopEx", "ExecStopPost", "ExecStopPostEx", "Restart", "NRestarts", "Triggers",
            "TriggeredBy", "OnFailure", "OnSuccess");
    private static final Set<String> PROPERTY_SET = Set.copyOf(PROPERTIES);

    /** Original query resources only. Outer launcher must verify the approved CLI/dependencies before initialize. */
    static final class Owner {
        private final String unit;
        private final Output output = new Output();
        private boolean attempted;

        Owner(String unit) {
            requireUnit(unit);
            this.unit = unit;
        }

        void initialize() throws IOException {
            synchronized (this) {
                if (attempted) {
                    throw new IOException("Original systemd query starts once");
                }
                attempted = true;
            }
            if (!SystemUtils.IS_OS_LINUX) {
                throw new IOException("Actual systemd query is unsupported on this platform");
            }
            var builder = new ProcessBuilder(command(unit));
            builder.directory(new java.io.File("/"));
            output.initialize(builder);
        }

        Map<String, String> properties(long deadline) throws IOException {
            var original = output.read(deadline);
            try {
                if (original.stderr().length != 0) {
                    throw new IOException("Original systemd query produced diagnostic output");
                }
                return decode(unit, original.stdout());
            } catch (IOException invalid) {
                output.reject(invalid);
                throw invalid;
            }
        }

        boolean drain(long deadline) throws InterruptedException {
            return output.drain(deadline);
        }
    }

    /** Physical pipe/Process component, also used by real child fixtures; its byte result cannot approve a unit. */
    static final class Output {
        private final Object gate = new Object();
        private final Capture stdout = new Capture(MAX_BYTES, "systemd-query-stdout");
        private final Capture stderr = new Capture(16 * 1024, "systemd-query-stderr");
        private boolean attempted;
        private boolean initializing;
        private boolean initializationFinished;
        private String acquiring;
        private Process process;
        private OutputStream stdin;
        private boolean stdinClosed;
        private Throwable stdinCloseFailure;
        private Throwable observationFailure;

        record Bytes(byte[] stdout, byte[] stderr) { }

        void initialize(ProcessBuilder builder) throws IOException {
            synchronized (gate) {
                if (attempted) {
                    throw new IOException("Original query Process starts once");
                }
                attempted = true;
                initializing = true;
            }
            try {
                // Start both fixed consumers before acquiring the Process, including partial Thread.start failures.
                stdout.thread.start();
                stderr.thread.start();
                builder.environment().clear();
                builder.redirectInput(ProcessBuilder.Redirect.PIPE);
                builder.redirectOutput(ProcessBuilder.Redirect.PIPE);
                builder.redirectError(ProcessBuilder.Redirect.PIPE);
                builder.redirectErrorStream(false);
                synchronized (gate) {
                    acquiring = "original systemd query construction/pipe attachment";
                }
                Process original = builder.start();
                synchronized (gate) {
                    process = original;
                    stdin = original.getOutputStream();
                    stdout.input = original.getInputStream();
                    stderr.input = original.getErrorStream();
                    acquiring = null;
                    gate.notifyAll();
                }
                try {
                    stdin.close();
                    synchronized (gate) {
                        stdinClosed = true;
                    }
                } catch (IOException | RuntimeException | Error failedClose) {
                    synchronized (gate) {
                        stdinCloseFailure = failedClose;
                    }
                    throw failedClose;
                }
            } catch (IOException | RuntimeException | Error failure) {
                reject(failure);
                throw failure;
            } finally {
                synchronized (gate) {
                    initializing = false;
                    initializationFinished = true;
                    gate.notifyAll();
                }
            }
        }

        Bytes read(long deadline) throws IOException {
            synchronized (gate) {
                if (!attempted) {
                    throw new IOException("Original query has not started");
                }
            }
            try {
                if (!drain(deadline)) {
                    throw new SocketTimeoutException("Original query Process/pipes have not physically terminated");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                var failure = new InterruptedIOException("Original query observation was interrupted");
                failure.initCause(interrupted);
                reject(failure);
                throw failure;
            } catch (IOException failure) {
                reject(failure);
                throw failure;
            }
            synchronized (gate) {
                if (observationFailure == null && process != null && process.exitValue() != 0) {
                    reject(new IOException("Original query exited with status " + process.exitValue()));
                }
                if (observationFailure != null || process == null) {
                    throw new IOException("Original query did not produce a valid observation", observationFailure);
                }
                return new Bytes(Arrays.copyOf(stdout.bytes, stdout.count), Arrays.copyOf(stderr.bytes, stderr.count));
            }
        }

        /** Observation only: never kills, interrupts a reader, retries close or closes a pipe under its active read. */
        boolean drain(long deadline) throws InterruptedException {
            if (Thread.currentThread() == stdout.thread || Thread.currentThread() == stderr.thread) {
                throw new IllegalStateException("Query pipe consumer cannot await its own termination");
            }
            Process original;
            synchronized (gate) {
                while (initializing) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedWait(gate, remaining);
                }
                if (acquiring != null) {
                    return false;
                }
                original = process;
            }
            if (original != null) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !original.waitFor(remaining, TimeUnit.NANOSECONDS)) {
                    return false;
                }
            }
            for (Capture capture : List.of(stdout, stderr)) {
                if (capture.thread.isAlive()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return false;
                    }
                    TimeUnit.NANOSECONDS.timedJoin(capture.thread, remaining);
                    if (capture.thread.isAlive()) {
                        return false;
                    }
                }
            }
            synchronized (gate) {
                return !initializing && acquiring == null && stdinCloseFailure == null
                        && (process == null || stdinClosed)
                        && (stdout.input == null || stdout.closed) && (stderr.input == null || stderr.closed)
                        && stdout.closeFailure == null && stderr.closeFailure == null;
            }
        }

        private void reject(Throwable failure) {
            synchronized (gate) {
                if (observationFailure == null) {
                    observationFailure = failure;
                } else if (observationFailure != failure && observationFailure.getSuppressed().length < 3) {
                    observationFailure.addSuppressed(failure);
                }
            }
        }

        private final class Capture implements Runnable {
            private final byte[] bytes;
            private final Thread thread;
            private InputStream input;
            private int count;
            private boolean overflow;
            private boolean closed;
            private Throwable closeFailure;

            Capture(int limit, String name) {
                bytes = new byte[limit];
                thread = new Thread(this, name);
                thread.setDaemon(false);
            }

            @Override
            public void run() {
                InputStream original = null;
                try {
                    synchronized (gate) {
                        while (input == null && !initializationFinished) {
                            gate.wait();
                        }
                        original = input;
                    }
                    if (original != null) {
                        byte[] chunk = new byte[4096];
                        int read;
                        while ((read = original.read(chunk)) != -1) {
                            int retain = Math.min(bytes.length - count, read);
                            System.arraycopy(chunk, 0, bytes, count, retain);
                            count += retain;
                            if (retain < read && !overflow) {
                                overflow = true;
                                reject(new IOException("Original query output exceeded its fixed capture budget"));
                            }
                            // Continue draining both real pipes after overflow; never grow the retained prefix.
                        }
                    }
                } catch (Throwable failure) {
                    reject(failure);
                } finally {
                    if (original != null) {
                        try {
                            original.close();
                            synchronized (gate) {
                                closed = true;
                            }
                        } catch (Throwable failedClose) {
                            synchronized (gate) {
                                closeFailure = failedClose;
                            }
                            reject(failedClose);
                        }
                    }
                }
            }
        }
    }

    private SegmentClassicSystemdQuery() { }

    /** No caller-selected executable, fields, shell, remote host, user manager or pattern. */
    static List<String> command(String unit) {
        requireUnit(unit);
        return List.of("/usr/bin/systemctl", "--system", "--no-pager", "--no-ask-password", "--all",
                "--property=" + String.join(",", PROPERTIES), "show", unit);
    }

    /**
     * Preserves the complete raw values, including empty values. Exec values are not parsed argv or an approval.
     * The actual CLI owner must first establish original Process/source identity, exit and physical pipe completion.
     */
    static Map<String, String> decode(String unit, byte[] output) throws IOException {
        requireUnit(unit);
        if (output == null || output.length == 0 || output.length > MAX_BYTES) {
            throw new IOException("Systemd property output exceeds its fixed bounds");
        }
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(output)).toString();
        if (text.charAt(text.length() - 1) != '\n' || text.indexOf(0) >= 0 || text.indexOf('\r') >= 0) {
            throw new IOException("Systemd property output is truncated or contains unsupported control bytes");
        }
        var values = new LinkedHashMap<String, String>();
        int position = 0;
        while (position < text.length()) {
            int end = text.indexOf('\n', position);
            int equals = text.indexOf('=', position);
            if (end < 0 || equals < position || equals >= end || equals == position) {
                throw new IOException("Invalid named systemd property line");
            }
            String name = text.substring(position, equals);
            if (!PROPERTY_SET.contains(name) || values.putIfAbsent(name, text.substring(equals + 1, end)) != null) {
                throw new IOException("Unknown or duplicate systemd property");
            }
            position = end + 1;
        }
        if (!values.keySet().equals(PROPERTY_SET) || !unit.equals(values.get("Id"))) {
            throw new IOException("Incomplete systemd property set or different original unit");
        }
        return Map.copyOf(values);
    }

    private static void requireUnit(String unit) {
        if (unit == null || unit.length() > 255 || !unit.matches("[A-Za-z0-9][A-Za-z0-9_.:@-]*\\.service")) {
            throw new IllegalArgumentException("One exact supported service unit name required");
        }
    }
}

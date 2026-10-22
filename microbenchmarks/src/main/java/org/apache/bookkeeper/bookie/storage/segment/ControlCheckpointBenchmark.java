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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import com.sun.management.ThreadMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Frozen image digest/write/file force/directory sync only; excludes control commit, publication and DATA. */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ControlCheckpointBenchmark {
    @State(Scope.Thread)
    public static class ImageState {
        @Param({"1", "64"})
        public int records;
        private Path directory;
        private IsolatedSegmentConfig config;
        private SegmentFileHeader identity;
        private SegmentControlCheckpoint.Snapshot snapshot;
        private SegmentControlCheckpoint.Reference reference;
        private SegmentFileIo io;
        private long operations;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            directory = Files.createTempDirectory("uw-checkpoint-benchmark-");
            config = new IsolatedSegmentConfig(directory, 1, 1, 1, 128L * 1024 * 1024, 4096, 1024, 256,
                    "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            byte[] incarnation = new byte[16];
            Arrays.fill(incarnation, (byte) 7);
            identity = new SegmentFileHeader(new StorageIncarnation(incarnation), new UUID(0, 9),
                    BookieId.parse("isolated:3181"));
            var entries = new ArrayList<SegmentControlCheckpoint.Operation>();
            long logOffset = 4096;
            for (int i = 0; i < records; i++) {
                var command = new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, i + 1), new Generation(1), 0, 0, 0, new Generation(1),
                        i + 1, 4096L + i * SegmentArenaControlLog.BLOCK_BYTES, 1, 1);
                ByteBuffer record = SegmentControlFormat.encode(command.type(), new Generation(1), i,
                        command.operationId(), command.operationGeneration(), command.expectedGeneration(),
                        command.body());
                entries.add(new SegmentControlCheckpoint.Operation(i, 1, record));
                logOffset += record.remaining();
            }
            snapshot = new SegmentControlCheckpoint.Snapshot(records - 1, logOffset, entries, true);
            io = new SegmentFileIo();
        }

        @TearDown(Level.Trial)
        public void verifyAndCleanup() throws Exception {
            try {
                if (reference == null || operations == 0 || io.forces != operations
                        || !snapshot.equals(SegmentControlCheckpoint.read(config, identity,
                                SegmentControlFormat.Owner.ARENA, reference, 1024 * 1024))) {
                    throw new IOException("Benchmark did not retain its exact forced image");
                }
                String image = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(
                        Files.readAllBytes(SegmentControlCheckpoint.path(
                                config, SegmentControlFormat.Owner.ARENA, 0))));
                String implementation;
                try (var bytes = SegmentControlCheckpoint.class.getResourceAsStream(
                        "SegmentControlCheckpoint.class")) {
                    implementation = HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes()));
                }
                System.out.println("CHECKPOINT_VERIFY records=" + records + " payload=" + reference.payloadBytes()
                        + " payloadSha256=" + reference.digest() + " imageSha256=" + image
                        + " operations=" + operations + " fileForces=" + io.forces + " writeCalls=" + io.writes
                        + " implementationSha256=" + implementation);
            } finally {
                Files.deleteIfExists(SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, 0));
                Files.delete(directory);
            }
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class IoCounters {
        public long operations;
        public long writeCalls;
        public long fileForces;
        public long cpuNanos;
        private long started;
        private final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        @Setup(Level.Iteration)
        public void start() {
            if (!bean.isCurrentThreadCpuTimeSupported()) {
                throw new IllegalStateException("Thread CPU measurement is unavailable");
            }
            if (!bean.isThreadCpuTimeEnabled()) {
                bean.setThreadCpuTimeEnabled(true);
            }
            operations = 0;
            writeCalls = 0;
            fileForces = 0;
            cpuNanos = 0;
            started = bean.getCurrentThreadCpuTime();
        }

        @TearDown(Level.Iteration)
        public void stop() {
            cpuNanos = bean.getCurrentThreadCpuTime() - started;
        }
    }

    @Benchmark
    public int digestAndForceImage(ImageState state, IoCounters counters) throws IOException {
        int beforeWrites = state.io.writes;
        int beforeForces = state.io.forces;
        var reference = SegmentControlCheckpoint.reference(1, 0, state.snapshot, 1024 * 1024);
        SegmentControlCheckpoint.write(state.config, state.identity, SegmentControlFormat.Owner.ARENA,
                reference, state.snapshot, state.io);
        state.reference = reference;
        state.operations++;
        counters.operations++;
        counters.writeCalls += state.io.writes - beforeWrites;
        counters.fileForces += state.io.forces - beforeForces;
        return reference.payloadBytes();
    }
}

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
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.net.BookieId;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** Fixed native append/force/apply episodes; setup and restart verification are outside the timed invocation. */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ControlBatchBenchmark {
    private static final int BATCHES = 256;
    private static final long LOG_BYTES = 16L * 1024 * 1024;
    private static final ProfileDescriptorValidator VALIDATOR = new ProfileDescriptorValidator(
            (id, version) -> false, (id, generation) -> id == 7 && generation == 11);
    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(
            EngineProfile.SEGMENT_WAL_ENGINE, PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK,
            3, 3, 2, 0, 7, 11, List.of());
    private static final byte[] DESCRIPTOR_BYTES = ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR);
    private static final ProfileDescriptorIdentity DESCRIPTOR_IDENTITY =
            ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR);

    @State(Scope.Thread)
    public static class BatchState {
        @Param({"ARENA", "BOOKIE"})
        public String owner;
        @Param({"1", "64"})
        public int records;
        private Path directory;
        private IsolatedSegmentConfig config;
        private SegmentFileHeader identity;
        private SegmentArenaControlLog arena;
        private SegmentBookieControlLog bookie;
        private IsolatedSegmentFiles files;
        private SegmentFileIo io;
        private SecretKeySpec key;
        private List<List<SegmentArenaControlLog.Allocation>> allocations;
        private List<List<ProtectedProfileStateStore.Transition>> installs;
        private long operations;
        private final ThreadMXBean metrics = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            if ((!owner.equals("ARENA") && !owner.equals("BOOKIE")) || records < 1 || records > 64) {
                throw new IllegalArgumentException("One bounded ARENA or BOOKIE episode required");
            }
            if (!metrics.isCurrentThreadCpuTimeSupported() || !metrics.isThreadAllocatedMemorySupported()) {
                throw new IllegalStateException("Thread CPU/allocation measurement is unavailable");
            }
            metrics.setThreadCpuTimeEnabled(true);
            metrics.setThreadAllocatedMemoryEnabled(true);
            directory = Files.createTempDirectory("uw-control-batch-benchmark-");
            config = new IsolatedSegmentConfig(directory.resolve("arena"), 1, 1, 1, 4L * 1024 * 1024,
                    4096, 1024, 64, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            identity = new SegmentFileHeader(new StorageIncarnation(bytes(16, 7)), new UUID(0, 9),
                    BookieId.parse("isolated:3181"));
            key = new SecretKeySpec(bytes(32, 7), "AES");
            files = IsolatedSegmentFiles.create(config, identity);
            io = new SegmentFileIo();
            operations = 0;
            if (owner.equals("ARENA")) {
                allocations = new ArrayList<>();
                for (int batch = 0; batch < BATCHES; batch++) {
                    var input = new ArrayList<SegmentArenaControlLog.Allocation>();
                    for (int record = 0; record < records; record++) {
                        int id = batch * records + record + 1;
                        input.add(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                                new UUID(0, id), new Generation(1), 0, 0, 0, new Generation(1), id, 4096, 1, 1));
                    }
                    allocations.add(List.copyOf(input));
                }
                arena = SegmentArenaControlLog.open(config, identity, LOG_BYTES, io);
            } else {
                installs = new ArrayList<>();
                for (int batch = 0; batch < BATCHES; batch++) {
                    var input = new ArrayList<ProtectedProfileStateStore.Transition>();
                    for (int record = 0; record < records; record++) {
                        input.add(install(batch * records + record + 1));
                    }
                    installs.add(List.copyOf(input));
                }
                bookie = SegmentBookieControlLog.open(config, identity, LOG_BYTES, key, VALIDATOR, io);
            }
        }

        @TearDown(Level.Iteration)
        public void verifyAndCleanup() throws Exception {
            try {
                long through = (long) BATCHES * records - 1;
                if (operations != BATCHES || io.forces != BATCHES || io.writes < operations * records) {
                    throw new IOException("Benchmark did not complete its exact forced batches");
                }
                if (arena != null) {
                    arena.close();
                    arena = null;
                    try (var replay = SegmentArenaControlLog.open(config, identity, LOG_BYTES)) {
                        if (replay.appliedThrough() != through
                                || !replay.hasAppliedGrant(0, 4096, SegmentArenaControlLog.BLOCK_BYTES, 0, 1, 1, 1)) {
                            throw new IOException("Different Arena replay cut or winner");
                        }
                    }
                } else {
                    bookie.close();
                    bookie = null;
                    try (var replay = SegmentBookieControlLog.open(config, identity, LOG_BYTES, key, VALIDATOR)) {
                        if (replay.appliedThrough() != through) {
                            throw new IOException("Different Bookie replay cut");
                        }
                        for (int id = 1; id <= BATCHES * records; id++) {
                            var context = new ProfileLedgerContext(1000L + id, bytes(16, 1),
                                    DESCRIPTOR_IDENTITY.toBytes());
                            if (replay.normalRejection(context, bytes(20, 8))
                                        != SegmentBookieControlLog.NormalRejection.NOT_ACTIVE
                                    || replay.normalRejection(context, bytes(20, 9))
                                        != SegmentBookieControlLog.NormalRejection.UNAUTHORIZED) {
                                throw new IOException("Different installed context or credential");
                            }
                        }
                    }
                }
                Path journal = config.directory().resolve(owner.equals("ARENA") ? "arena-control" : "bookie-control");
                String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(Files.readAllBytes(journal)));
                System.out.println("CONTROL_BATCH_VERIFY owner=" + owner + " records=" + records
                        + " batches=" + operations + " fileForces=" + io.forces + " writeCalls=" + io.writes
                        + " through=" + through + " journalBytes=" + Files.size(journal) + " journalSha256=" + digest
                        + " arenaSha256=" + implementation(SegmentArenaControlLog.class)
                        + " bookieSha256=" + implementation(SegmentBookieControlLog.class)
                        + " checkpointSha256=" + implementation(SegmentControlCheckpoint.class)
                        + " ioSha256=" + implementation(SegmentFileIo.class));
            } finally {
                try {
                    if (arena != null) {
                        arena.close();
                        arena = null;
                    }
                    if (bookie != null) {
                        bookie.close();
                        bookie = null;
                    }
                } finally {
                    if (files != null) {
                        files.close();
                        files = null;
                    }
                    allocations = null;
                    installs = null;
                    if (directory != null) {
                        try (var paths = Files.walk(directory)) {
                            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                                Files.delete(path);
                            }
                        }
                        directory = null;
                    }
                }
            }
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Counters {
        public long operations;
        public long writeCalls;
        public long fileForces;
        public long cpuNanos;
        public long javaBytes;

        @Setup(Level.Iteration)
        public void reset() {
            operations = 0;
            writeCalls = 0;
            fileForces = 0;
            cpuNanos = 0;
            javaBytes = 0;
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCHES)
    public long appendForceAndApply(BatchState state, Counters counters) throws IOException {
        long thread = Thread.currentThread().getId();
        long beforeBytes = state.metrics.getThreadAllocatedBytes(thread);
        long beforeCpu = state.metrics.getCurrentThreadCpuTime();
        int beforeWrites = state.io.writes;
        int beforeForces = state.io.forces;
        try {
            for (int batch = 0; batch < BATCHES; batch++) {
                if (state.arena != null) {
                    var durable = state.arena.appendDurably(state.allocations.get(batch));
                    var results = state.arena.applyDurable(durable);
                    for (int record = 0; record < state.records; record++) {
                        var expected = batch == 0 && record == 0 ? SegmentArenaControlLog.Status.APPLIED_DURABLE
                                : SegmentArenaControlLog.Status.CONDITION_FAILED;
                        if (results.get(record).status() != expected) {
                            throw new IOException("Different Arena batch result");
                        }
                    }
                } else {
                    var durable = state.bookie.appendDurably(state.installs.get(batch));
                    for (var result : state.bookie.applyDurable(durable)) {
                        if (result.status() != SegmentBookieControlLog.Status.APPLIED_DURABLE) {
                            throw new IOException("Different Bookie batch result");
                        }
                    }
                }
                state.operations++;
                counters.operations++;
            }
        } finally {
            counters.cpuNanos += state.metrics.getCurrentThreadCpuTime() - beforeCpu;
            counters.javaBytes += state.metrics.getThreadAllocatedBytes(thread) - beforeBytes;
            counters.writeCalls += state.io.writes - beforeWrites;
            counters.fileForces += state.io.forces - beforeForces;
        }
        return state.operations;
    }

    private static ProtectedProfileStateStore.Transition install(int id) {
        var scope = new ProfileControlScope(ProfileControlOperation.INSTALL, ProfileControlOperation.INSTALL.purpose(),
                1000L + id, bytes(16, 1), DESCRIPTOR_IDENTITY, "isolated:3181", bytes(16, 7), "ledger-profile", 1,
                ByteBuffer.allocate(16).putLong(1).putLong(id).array(), bytes(32, 4), null);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                EngineProfile.SEGMENT_WAL_ENGINE, ProtectedProfileCredential.of(
                        ProtectedProfileCredential.BK_MASTER_KEY_SHA1, bytes(20, 8))),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 1, true), DESCRIPTOR);
    }

    private static byte[] bytes(int size, int value) {
        byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static String implementation(Class<?> type) throws Exception {
        try (var input = type.getResourceAsStream(type.getSimpleName() + ".class")) {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input.readAllBytes()));
        }
    }
}

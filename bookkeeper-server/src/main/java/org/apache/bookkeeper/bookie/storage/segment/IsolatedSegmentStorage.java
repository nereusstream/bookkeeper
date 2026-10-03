/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;

/** Owns the isolated local storage graph. This is not Bookie registration or production ACK readiness. */
final class IsolatedSegmentStorage implements AutoCloseable {
    record Limits(long controlLogBytes, int hotEntries, int poolBlocks,
                  SegmentDataPhysicalDriver.BatchLimits batches) {
        Limits {
            Objects.requireNonNull(batches, "batches");
            if (controlLogBytes < 4096 + 320 || hotEntries < 1 || poolBlocks < 1) {
                throw new IllegalArgumentException("Invalid isolated storage bounds");
            }
        }
    }

    private final Object gate = new Object();
    private final Object closeGate = new Object();
    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final SegmentExecutionGroup.Settings executionSettings;
    private final Limits limits;
    private final SegmentDataPhysicalDriver.NormalBatcher[] writes;
    private final SegmentPointRead.Reader[] reads;
    private IsolatedSegmentFiles files;
    private SegmentArenaControlLog arena;
    private SegmentBookieControlLog bookie;
    private SegmentDerivedIndex index;
    private volatile SegmentExecutionGroup execution;
    private SegmentBookieControlDriver controls;
    private boolean startAttempted;
    private boolean accepting;
    private boolean closed;

    /** Construct the resource owner before startup so a failed start still has an explicit cleanup handle. */
    IsolatedSegmentStorage(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
                           ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings executionSettings,
                           Limits limits) {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.executionSettings = Objects.requireNonNull(executionSettings, "executionSettings");
        this.limits = Objects.requireNonNull(limits, "limits");
        if (limits.hotEntries() < config.logicalShards()) {
            throw new IllegalArgumentException("Global hot capacity must provide at least one entry per shard");
        }
        writes = new SegmentDataPhysicalDriver.NormalBatcher[config.logicalShards()];
        reads = new SegmentPointRead.Reader[config.logicalShards()];
    }

    void startNew() throws IOException, InterruptedException {
        start(false);
    }

    void restart() throws IOException, InterruptedException {
        start(true);
    }

    private void start(boolean existing) throws IOException, InterruptedException {
        synchronized (closeGate) {
            synchronized (gate) {
                if (startAttempted || closed) {
                    throw new IllegalStateException("Storage startup already attempted or storage closed");
                }
                startAttempted = true;
            }
            try {
                if (!existing) {
                    files = IsolatedSegmentFiles.create(config, identity);
                }
                arena = SegmentArenaControlLog.open(config, identity, limits.controlLogBytes());
                bookie = SegmentBookieControlLog.open(config, identity, limits.controlLogBytes(), key, validator);
                index = SegmentDerivedIndex.open(config, identity, limits.hotEntries());
                if (existing) {
                    files = IsolatedSegmentFiles.openForLocalResults(config, identity, arena, bookie,
                            new SegmentFileIo(), limits.hotEntries(), config.logicalShards(), index);
                } else {
                    files.startSession(arena);
                }
                execution = SegmentExecutionGroup.start(config, executionSettings);
                controls = new SegmentBookieControlDriver(execution.owner(0), execution.io(), bookie);
                bootstrap();
                synchronized (gate) {
                    accepting = true;
                }
            } catch (IOException | InterruptedException | RuntimeException | Error failure) {
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failure.addSuppressed(new IOException("Storage startup cleanup still requires drain"));
                    }
                } catch (IOException | InterruptedException | RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw failure;
            }
        }
    }

    private void bootstrap() throws IOException, InterruptedException {
        SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(execution.owner(0), execution.io(), arena);
        long end = System.nanoTime() + executionSettings.startupTimeout().toNanos();
        for (int shard = 0; shard < config.logicalShards(); shard++) {
            final int selected = shard;
            SegmentOwnerLoop owner = execution.owner(shard);
            FutureTask<Void> task = new FutureTask<>(() -> {
                SegmentArenaControlLog.SingleStream stream = arena.restartStreamForShard(selected).orElse(
                        new SegmentArenaControlLog.SingleStream(new Generation(1), 1));
                int capacity = limits.hotEntries() / config.logicalShards()
                        + (selected < limits.hotEntries() % config.logicalShards() ? 1 : 0);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity, capacity);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(selected, owner, execution.io(),
                        files, arena, new SegmentPhysicalPrefix(owner, selected, stream.generation(),
                                stream.firstSequence(), config.maxPendingEntries()), hot);
                writes[selected] = driver.enableNormalBatching(
                        bookie, limits.batches(), allocator, limits.poolBlocks());
                reads[selected] = SegmentPointRead.Reader.live(owner, execution.io(), files, arena, bookie, hot);
                return null;
            });
            if (!owner.control(task)) {
                throw new IOException("Storage bootstrap control capacity exhausted");
            }
            // Do not fill a worker's bounded control queue with the other shards it owns.
            await(task, end);
        }
        for (int worker = 0; worker < executionSettings.workers(); worker++) {
            SegmentOwnerLoop owner = execution.owner(worker);
            while (owner.hasPendingCommands()) {
                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Storage bootstrap commands did not terminate");
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException("Storage bootstrap interrupted");
                }
                LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
            }
            if (owner.failure() != null) {
                throw new IOException("Storage bootstrap owner failed", owner.failure());
            }
        }
    }

    private static void await(FutureTask<Void> task, long end) throws IOException, InterruptedException {
        try {
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException("Storage bootstrap deadline expired");
            }
            task.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IOException("Storage owner bootstrap failed", failure);
        }
    }

    SegmentBookieControlDriver controls() {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                throw new IllegalStateException("Storage is not accepting control requests");
            }
            return controls;
        }
    }

    /** Borrow and copy the entry into the existing shared source budget; refusal never owns the caller's buffer. */
    boolean submitNormal(ProfileLedgerContext context, byte[] credential, ByteBuf entry,
                         SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                return false;
            }
            Objects.requireNonNull(context, "context");
            Objects.requireNonNull(credential, "credential");
            Objects.requireNonNull(entry, "entry");
            Objects.requireNonNull(request, "request");
            SegmentBookieControlLog.Admission admission = bookie.admitNormal(context, credential).orElse(null);
            if (admission == null) {
                return false;
            }
            SegmentEntryInputs.Input input = null;
            try {
                input = execution.inputs().acquire(context, entry.readableBytes());
                if (input == null) {
                    admission.physicalTerminal();
                    return false;
                }
                input.bytes().writeBytes(entry, entry.readerIndex(), entry.readableBytes());
            } catch (RuntimeException | Error failure) {
                if (input != null) {
                    input.close();
                }
                admission.physicalTerminal();
                throw failure;
            }
            int shard = SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards());
            try {
                return writes[shard].submit(input, admission, request);
            } catch (IOException | RuntimeException | Error failure) {
                input.close();
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    boolean submitRecovery(ProfileOperationCodec.AddRecovery request,
                           SegmentDataPhysicalDriver.RecoveryRequest response)
            throws IOException {
        synchronized (gate) {
            if (!accepting || !execution.accepting()) {
                return false;
            }
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(response, "response");
            SegmentBookieControlLog.RecoveryAdmission admission = bookie.admitRecovery(request).orElse(null);
            if (admission == null) {
                return false;
            }
            try {
                return writes[SegmentOwnerLoop.shardFor(admission.context().ledgerId(),
                        config.logicalShards())].submitRecovery(admission, response);
            } catch (IOException | RuntimeException | Error failure) {
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    boolean submitRead(ProfileLedgerContext context, long entryId, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            return accepting && execution.accepting()
                    && reads[SegmentOwnerLoop.shardFor(context.ledgerId(), config.logicalShards())]
                            .submit(context, entryId, request);
        }
    }

    /** False retains the same graph and locks for another drain attempt; it never closes around live I/O. */
    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive storage drain timeout required");
        }
        timeout.toNanos();
        SegmentExecutionGroup group = execution;
        if (group != null) {
            for (int worker = 0; worker < executionSettings.workers(); worker++) {
                if (group.owner(worker).isOwnerThread()) {
                    throw new IllegalStateException("Storage drain requires an external thread");
                }
            }
        }
        synchronized (closeGate) {
            synchronized (gate) {
                accepting = false;
                if (closed) {
                    return true;
                }
                if (execution != null) {
                    execution.stopAdmission();
                }
            }
            IOException first = null;
            if (execution != null) {
                try {
                    if (!execution.shutdown(timeout)) {
                        return false;
                    }
                } catch (IOException failure) {
                    if (!execution.terminated() || execution.inputs().usedBytes() != 0) {
                        throw failure;
                    }
                    first = failure;
                }
            }
            for (AutoCloseable resource : new AutoCloseable[] {files, index, bookie, arena}) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (Exception failure) {
                        IOException cause = failure instanceof IOException io ? io
                                : new IOException("Storage resource cleanup failed", failure);
                        if (first == null) {
                            first = cause;
                        } else {
                            first.addSuppressed(cause);
                        }
                    }
                }
            }
            synchronized (gate) {
                closed = first == null;
            }
            if (first != null) {
                throw first;
            }
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Storage drain incomplete; retain this owner and retry after work ends");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Storage drain interrupted", interrupted);
        }
    }
}

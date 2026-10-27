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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** One in-flight Arena control batch, forced off-owner and applied on its owner. */
final class SegmentArenaControlDriver {
    @FunctionalInterface
    interface Completion {
        void complete(List<SegmentArenaControlLog.Result> results, Throwable failure);
    }

    private final SegmentOwnerLoop owner;
    private final SegmentIoExecutor io;
    private final SegmentArenaControlLog log;
    private final int maintenanceBlocks;
    private final AtomicBoolean busy = new AtomicBoolean();
    private static final class PoolSubmission {
        private final SegmentArenaControlLog.PoolRequest request;
        private Completion completion;

        private PoolSubmission(SegmentArenaControlLog.PoolRequest request, Completion completion) {
            this.request = request;
            this.completion = completion;
        }

        private SegmentArenaControlLog.PoolRequest request() {
            return request;
        }

        /** Consumed on the original owner before callback code can publish a later DATA obligation. */
        private Completion takeCompletion() {
            Completion taken = completion;
            completion = null;
            if (taken == null) {
                throw new IllegalStateException("Pool callback already consumed");
            }
            return taken;
        }
    }
    private final Object poolGate = new Object();
    private final List<PoolSubmission> pools = new ArrayList<>();
    private boolean collecting;
    private final FailureNotice[] notices;
    private final ArrayDeque<FailureNotice> waitingFailures = new ArrayDeque<>();

    static long controlCapacity(SegmentControlCapacity.Layout layout, int pending, int files, int shards) {
        long notices = SegmentControlCapacity.sum(layout.refs(files),
                // ArrayDeque growth and a concurrently held previous array.
                SegmentControlCapacity.times(2, layout.refs(SegmentControlCapacity.sum(2L * files, 16))),
                SegmentControlCapacity.times(files, failureNoticeCapacity(layout)));
        long pools = SegmentControlCapacity.sum(SegmentControlCapacity.times(6,
                layout.refs(Math.min(pending, shards))), SegmentControlCapacity.times(Math.min(pending, shards),
                SegmentControlCapacity.sum(layout.object(PoolSubmission.class),
                        layout.object(SegmentArenaControlLog.PoolRequest.class),
                        layout.object(Optional.class), layout.object(Generation.class))));
        return SegmentControlCapacity.sum(layout.object(SegmentArenaControlDriver.class),
                layout.object(AtomicBoolean.class), layout.object(Object.class), layout.object(ArrayDeque.class),
                layout.object(PoolCommand.class), layout.list(), notices, pools,
                layout.object(SegmentOwnerLoop.Ticket.class), layout.object(AtomicReference.class),
                SegmentControlCapacity.times(8, layout.captured(8)),
                SegmentControlCapacity.times(pending, layout.object(FailureLease.class)));
    }

    static long failureNoticeCapacity(SegmentControlCapacity.Layout layout) {
        return SegmentControlCapacity.sum(layout.object(FailureNotice.class),
                layout.object(SegmentOwnerLoop.Ticket.class),
                SegmentControlCapacity.times(4, layout.captured(8)));
    }

    boolean reserveMaintenance() {
        synchronized (poolGate) {
            for (FailureNotice notice : notices) {
                if (notice != null) {
                    return false;
                }
            }
            return busy.compareAndSet(false, true);
        }
    }

    void releaseMaintenance() {
        releaseBusy();
    }

    SegmentArenaControlDriver(SegmentOwnerLoop owner, SegmentIoExecutor io, SegmentArenaControlLog log) {
        this(owner, io, log, 0);
    }

    SegmentArenaControlDriver(SegmentOwnerLoop owner, SegmentIoExecutor io, SegmentArenaControlLog log,
                              int maintenanceBlocks) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.io = Objects.requireNonNull(io, "io");
        this.log = Objects.requireNonNull(log, "log");
        if (maintenanceBlocks < 0) {
            throw new IllegalArgumentException("Negative maintenance reserve");
        }
        this.maintenanceBlocks = maintenanceBlocks;
        notices = new FailureNotice[log.dataFiles()];
    }

    /** One shared reservation per file while DATA obligations are in flight. */
    final class FailureLease implements AutoCloseable {
        private final FailureNotice notice;
        private boolean closed;

        private FailureLease(FailureNotice notice) {
            this.notice = notice;
        }

        void failed(Throwable cause) {
            Objects.requireNonNull(cause, "cause");
            synchronized (poolGate) {
                if (notice.failure != null) {
                    return;
                }
                if (closed) {
                    throw new IllegalStateException("DATA failure reported after reservation release");
                }
                notice.failure = cause;
                log.blockDataFile(notice.file);
                notice.ticket.resume(() -> enqueueFailure(notice));
            }
        }

        @Override
        public void close() {
            synchronized (poolGate) {
                if (closed) {
                    throw new IllegalStateException("DATA failure reservation already released");
                }
                closed = true;
                if (--notice.users == 0 && notice.failure == null) {
                    notices[notice.file] = null;
                    log.releaseQuarantineRecord(notice.file);
                    notice.ticket.complete(() -> { });
                }
            }
        }
    }

    private final class FailureNotice implements SegmentOwnerLoop.Command {
        private final int file;
        private SegmentOwnerLoop.Ticket ticket;
        private int users = 1;
        private Throwable failure;
        private boolean inFlight;

        private FailureNotice(int file) {
            this.file = file;
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket unused) {
            throw new IllegalStateException("Failure notification starts through its reserved completion");
        }

        @Override
        public void failed(Throwable cause) {
            owner.requireOwner();
            if (inFlight) {
                deliver(this::finished, null, cause);
            } else {
                finished(null, cause);
            }
        }

        private void finished(List<SegmentArenaControlLog.Result> results, Throwable cause) {
            owner.requireOwner();
            if (cause == null && (results == null || results.size() != 1
                    || !log.hasDurableQuarantine(file))) {
                cause = new IOException("DATA quarantine command was not applied");
            }
            synchronized (poolGate) {
                notices[file] = null;
                waitingFailures.remove(this);
                log.releaseQuarantineRecord(file);
            }
            if (cause != null && cause != failure) {
                failure.addSuppressed(cause);
            }
        }
    }

    FailureLease reserveFailure(int file) {
        if (file < 0 || file >= notices.length) {
            throw new IllegalArgumentException("Invalid DATA file");
        }
        synchronized (poolGate) {
            if (!log.dataFileAvailable(file) || owner.failure() != null) {
                return null;
            }
            FailureNotice notice = notices[file];
            if (notice != null) {
                notice.users++;
                return new FailureLease(notice);
            }
            if (!log.reserveQuarantineRecord(file)) {
                return null;
            }
            notice = new FailureNotice(file);
            try {
                notice.ticket = owner.reserveControlNotification(notice);
            } catch (RuntimeException | Error cause) {
                log.releaseQuarantineRecord(file);
                throw cause;
            }
            if (notice.ticket == null) {
                log.releaseQuarantineRecord(file);
                return null;
            }
            notices[file] = notice;
            return new FailureLease(notice);
        }
    }

    private void enqueueFailure(FailureNotice notice) {
        owner.requireOwner();
        if (owner.failure() != null) {
            notice.ticket.complete(() -> notice.finished(null, owner.failure()));
            return;
        }
        synchronized (poolGate) {
            if (busy.get()) {
                waitingFailures.addLast(notice);
                return;
            }
            busy.set(true);
            notice.inFlight = true;
        }
        forceFailure(notice);
    }

    private void forceFailure(FailureNotice notice) {
        if (owner.failure() != null) {
            notice.ticket.complete(() -> deliver(notice::finished, null, owner.failure()));
        } else {
            forceAndApply(notice.ticket, List.of(new SegmentArenaControlLog.FileQuarantine(
                    UUID.randomUUID(), new Generation(1), notice.file)), notice::finished);
        }
    }

    boolean submit(List<SegmentArenaControlLog.Allocation> commands, Completion completion) {
        return submitCommands(commands, completion);
    }

    boolean submitMove(SegmentArenaMove command, Completion completion) {
        return submitCommands(List.of(command), completion);
    }

    boolean submitFree(SegmentArenaControlLog.Free command, Completion completion) {
        return submitCommands(List.of(command), completion);
    }

    boolean submitFrees(List<SegmentArenaControlLog.Free> commands, Completion completion) {
        return submitCommands(commands, completion);
    }

    /** Block immediately; refusal or failure does not establish a durable quarantine witness. */
    boolean submitQuarantine(int file, Completion completion) {
        Objects.requireNonNull(completion, "completion");
        log.blockDataFile(file);
        return submitCommands(List.of(new SegmentArenaControlLog.FileQuarantine(
                UUID.randomUUID(), new Generation(1), file)), completion);
    }

    private boolean submitCommands(List<? extends SegmentArenaControlLog.Command> commands,
                                    Completion completion) {
        List<SegmentArenaControlLog.Command> batch = List.copyOf(commands);
        Objects.requireNonNull(completion, "completion");
        synchronized (poolGate) {
            if (!busy.compareAndSet(false, true)) {
                return false;
            }
            boolean accepted;
            try {
                log.claimProtectedCommands(batch);
                accepted = owner.submitControl(new SegmentOwnerLoop.Command() {
                    @Override
                    public void run(SegmentOwnerLoop.Ticket ticket) {
                        forceAndApply(ticket, batch, completion);
                    }

                    @Override
                    public void failed(Throwable cause) {
                        owner.requireOwner();
                        log.finishProtectedCommands(batch);
                        deliver(completion, null, cause);
                    }
                });
            } catch (IOException failure) {
                busy.set(false);
                throw new IllegalStateException("FREE runtime protection could not be claimed", failure);
            } catch (RuntimeException | Error failure) {
                log.finishProtectedCommands(batch);
                busy.set(false);
                throw failure;
            }
            if (!accepted) {
                log.finishProtectedCommands(batch);
                busy.set(false);
            }
            return accepted;
        }
    }

    boolean owns(SegmentArenaControlLog authority) {
        return log == authority;
    }

    boolean supportsFailureReservations() {
        return owner.supportsControlNotification();
    }

    /** Merge at most one request per shard until the queued control batch starts; no retry queue. */
    boolean requestPool(SegmentArenaControlLog.PoolRequest request, Completion completion) {
        log.validatePoolRequest(request);
        Objects.requireNonNull(completion, "completion");
        synchronized (poolGate) {
            if (busy.get()) {
                if (!collecting || pools.size() == log.maxPoolRequests()
                        || pools.stream().anyMatch(p -> p.request().shard() == request.shard())) {
                    return false;
                }
                pools.add(new PoolSubmission(request, completion));
                return true;
            }
            if (!busy.compareAndSet(false, true)) {
                return false;
            }
            pools.add(new PoolSubmission(request, completion));
            collecting = true;
            try {
                if (owner.submitControl(new PoolCommand())) {
                    return true;
                }
            } catch (RuntimeException | Error failure) {
                pools.clear();
                collecting = false;
                busy.set(false);
                throw failure;
            }
            pools.clear();
            collecting = false;
            busy.set(false);
            return false;
        }
    }

    private final class PoolCommand implements SegmentOwnerLoop.Command {
        private List<PoolSubmission> batch;
        private List<Optional<SegmentArenaControlLog.Allocation>> planned;

        private void freezeRequests() {
            synchronized (poolGate) {
                if (batch == null) {
                    batch = List.copyOf(pools);
                    pools.clear();
                    collecting = false;
                }
            }
        }

        @Override
        public void run(SegmentOwnerLoop.Ticket ticket) {
            freezeRequests();
            try {
                planned = log.planPools(batch.stream().map(PoolSubmission::request).toList(), maintenanceBlocks);
            } catch (IOException failure) {
                ticket.complete(() -> deliver(this::completed, null, failure));
                return;
            }
            List<SegmentArenaControlLog.Allocation> commands = planned.stream().flatMap(Optional::stream).toList();
            if (commands.isEmpty()) {
                ticket.complete(() -> deliver(this::completed, List.of(), null));
            } else {
                forceAndApply(ticket, commands, this::completed);
            }
        }

        @Override
        public void failed(Throwable cause) {
            freezeRequests();
            deliver(this::completed, null, cause);
        }

        private void completed(List<SegmentArenaControlLog.Result> results, Throwable failure) {
            Throwable callbackFailure = null;
            int result = 0;
            for (int i = 0; i < batch.size(); i++) {
                List<SegmentArenaControlLog.Result> one = failure == null && planned.get(i).isPresent()
                        ? List.of(results.get(result++)) : List.of();
                try {
                    batch.get(i).takeCompletion().complete(one, failure);
                } catch (Throwable cause) {
                    if (callbackFailure == null) {
                        callbackFailure = cause;
                    } else if (callbackFailure != cause) {
                        callbackFailure.addSuppressed(cause);
                    }
                }
            }
            if (callbackFailure != null) {
                throw new IllegalStateException("Pool refill callback failed", callbackFailure);
            }
        }
    }

    private void forceAndApply(SegmentOwnerLoop.Ticket ticket,
                               List<? extends SegmentArenaControlLog.Command> batch, Completion completion) {
        AtomicReference<SegmentArenaControlLog.DurableBatch> durable = new AtomicReference<>();
        io.submitControl(ticket, () -> durable.set(log.appendDurably(batch)), cause -> {
            owner.requireOwner();
            if (cause != null) {
                log.finishProtectedCommands(batch);
                deliver(completion, null, cause);
                return;
            }
            List<SegmentArenaControlLog.Result> results;
            try {
                results = log.applyDurable(durable.get());
            } catch (Throwable failure) {
                log.finishProtectedCommands(batch);
                deliver(completion, null, failure);
                return;
            }
            log.finishProtectedCommands(batch);
            deliver(completion, results, null);
        });
    }

    private void deliver(Completion completion, List<SegmentArenaControlLog.Result> results, Throwable failure) {
        try {
            completion.complete(results, failure);
        } finally {
            releaseBusy();
        }
    }

    private void releaseBusy() {
        synchronized (poolGate) {
            busy.set(false);
            FailureNotice next = waitingFailures.pollFirst();
            if (next != null) {
                busy.set(true);
                next.inFlight = true;
                next.ticket.resume(() -> forceFailure(next));
            }
        }
    }
}

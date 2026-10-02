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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;

/** Isolated Bookie-owned control log; no production route, DATA or ACK authority. */
final class SegmentBookieControlLog implements AutoCloseable {
    private static final long FIRST_RECORD = 4096;
    private static final Generation LOG_GENERATION = new Generation(1);
    private static final byte[] RECOVERY_GRANT_DOMAIN =
            "UW-RECOVERY-GRANT-V1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RECOVERY_CLOSE_DOMAIN =
            "UW-RECOVERY-CLOSE-V1".getBytes(StandardCharsets.US_ASCII);

    enum Status {
        APPLIED_DURABLE, ALREADY_DURABLE, CONDITION_FAILED, INCOMPATIBLE
    }

    record Result(Status status, UUID operationId, long sequence, long localGeneration) { }

    record ExplicitLac(long lac, byte[] body) {
        ExplicitLac {
            body = body.clone();
        }

        @Override
        public byte[] body() {
            return body.clone();
        }
    }

    record LacAttempt(Status immediate, LacWrite write) { }

    /** Typed, cold-authorized input to the isolated local grant record. */
    record RecoveryGrant(ProtectedProfileStateStore.Transition transition, byte[] repairIntentId,
                         long repairIntentGeneration, long grantGeneration) {
        RecoveryGrant {
            Objects.requireNonNull(transition, "transition");
            repairIntentId = repairIntentId.clone();
            if (repairIntentId.length != 16 || isZero(repairIntentId)
                    || repairIntentGeneration <= 0 || grantGeneration <= 0) {
                throw new IllegalArgumentException("Invalid recovery grant binding");
            }
        }

        @Override
        public byte[] repairIntentId() {
            return repairIntentId.clone();
        }
    }

    record RecoveryGrantClose(ProtectedProfileStateStore.Transition transition, UUID grantOperationId,
                              byte[] repairIntentId, long repairIntentGeneration, long grantGeneration) {
        RecoveryGrantClose {
            Objects.requireNonNull(transition, "transition");
            Objects.requireNonNull(grantOperationId, "grantOperationId");
            repairIntentId = repairIntentId.clone();
            if (repairIntentId.length != 16 || isZero(repairIntentId)
                    || repairIntentGeneration <= 0 || grantGeneration <= 0) {
                throw new IllegalArgumentException("Invalid recovery close binding");
            }
        }

        @Override
        public byte[] repairIntentId() {
            return repairIntentId.clone();
        }
    }

    enum RecoveryCloseStatus {
        APPLIED_DURABLE, ALREADY_DURABLE, WAITING_PRECUT, CONDITION_FAILED, INCOMPATIBLE
    }

    record RecoveryCloseAttempt(RecoveryCloseStatus immediate, RecoveryCloseWrite write) { }

    static final class RecoveryCloseWrite {
        private final Command command;
        private final ByteBuffer record;
        private final long sequence;
        private final long offset;
        private boolean durable;

        private RecoveryCloseWrite(Command command, ByteBuffer record, long sequence, long offset) {
            this.command = command;
            this.record = record;
            this.sequence = sequence;
            this.offset = offset;
        }
    }

    private record GrantBinding(byte[] intentId, long intentGeneration, long grantGeneration) {
        private boolean sameMeaning(GrantBinding other) {
            return other != null && Arrays.equals(intentId, other.intentId)
                    && intentGeneration == other.intentGeneration && grantGeneration == other.grantGeneration;
        }
    }

    static final class LacWrite {
        private final ProfileLedgerContext context;
        private final long lac;
        private final byte[] body;
        private final ByteBuffer record;
        private final long sequence;
        private final long offset;
        private final long generation;
        private boolean durable;

        private LacWrite(ProfileLedgerContext context, long lac, byte[] body, ByteBuffer record,
                         long sequence, long offset, long generation) {
            this.context = context;
            this.lac = lac;
            this.body = body;
            this.record = record;
            this.sequence = sequence;
            this.offset = offset;
            this.generation = generation;
        }
    }

    enum FenceStatus {
        APPLIED_DURABLE, ALREADY_DURABLE, WAITING_PRECUT, REJECTED
    }

    record FenceResult(FenceStatus status, long localGeneration) { }

    record FenceAttempt(FenceResult immediate, FenceWrite write) { }

    static final class FenceWrite {
        private final ProfileLedgerContext context;
        private final UUID operationId;
        private final ByteBuffer record;
        private final long sequence;
        private final long offset;
        private boolean durable;

        private FenceWrite(ProfileLedgerContext context, UUID operationId, ByteBuffer record,
                           long sequence, long offset) {
            this.context = context;
            this.operationId = operationId;
            this.record = record;
            this.sequence = sequence;
            this.offset = offset;
        }
    }

    /** Held through normal DATA termination or duplicate point-read completion. */
    static final class Admission {
        private final SegmentBookieControlLog owner;
        private final LedgerState state;
        private final long generation;
        private boolean terminal;
        private boolean claimed;

        private Admission(SegmentBookieControlLog owner, LedgerState state) {
            this.owner = owner;
            this.state = state;
            this.generation = state.localGeneration;
        }

        long generation() {
            return generation;
        }

        ProfileLedgerContext context() {
            return state.context;
        }

        void physicalTerminal() {
            owner.finishAdmission(this, false);
        }

        /** An outer resource owner may clean up a failed handoff, but never revoke a driver's ticket. */
        void releaseUnclaimed() {
            synchronized (owner) {
                if (!claimed && !terminal) {
                    owner.finishAdmission(this, false);
                }
            }
        }

        void driverTerminal() {
            owner.finishAdmission(this, true);
        }
    }

    /** Local route ticket for a point read; fence does not revoke an installed replica's read access. */
    static final class ReadAdmission {
        private final SegmentBookieControlLog owner;
        private final LedgerState state;
        private boolean terminal;

        private ReadAdmission(SegmentBookieControlLog owner, LedgerState state) {
            this.owner = owner;
            this.state = state;
        }

        void finish() {
            owner.finishRead(this);
        }
    }

    /** Isolated recovery ticket retained through physical DATA, duplicate reads and close ordering. */
    static final class RecoveryAdmission {
        private final SegmentBookieControlLog owner;
        private final LedgerState state;
        private final GrantState grant;
        private final long entryId;
        private byte[] entryPayload;
        private boolean terminal;
        private boolean claimed;

        private RecoveryAdmission(SegmentBookieControlLog owner, LedgerState state, GrantState grant,
                                  ProfileOperationCodec.AddRecovery request) {
            this.owner = owner;
            this.state = state;
            this.grant = grant;
            this.entryId = request.entryId();
            this.entryPayload = request.entryPayload();
        }

        ProfileLedgerContext context() {
            return state.context;
        }

        long entryId() {
            return entryId;
        }

        byte[] entryPayload() {
            if (entryPayload == null) {
                throw new IllegalStateException("Recovery payload ownership has ended or moved to its driver");
            }
            return entryPayload.clone();
        }

        byte[] takeEntryPayloadForDriver() {
            return owner.takeRecoveryPayload(this);
        }

        void finish() {
            owner.finishRecovery(this, false);
        }

        void releaseUnclaimed() {
            synchronized (owner) {
                if (!claimed && !terminal) {
                    owner.finishRecovery(this, false);
                }
            }
        }

        void driverTerminal() {
            owner.finishRecovery(this, true);
        }
    }

    record DurableBatch(long firstSequence, List<Command> commands) {
        DurableBatch {
            commands = List.copyOf(commands);
        }
    }

    private static final class Command {
        private final SegmentControlFormat.Type type;
        private final ProfileControlScope scope;
        private final byte[] descriptor;
        private final ProtectedProfileCredential credential;
        private final ProtectedProfileCredential.SealedStorageValue sealed;
        private final GrantBinding grant;
        private final UUID closedGrantId;

        private Command(SegmentControlFormat.Type type, ProfileControlScope scope, byte[] descriptor,
                        ProtectedProfileCredential credential,
                        ProtectedProfileCredential.SealedStorageValue sealed) {
            this.type = type;
            this.scope = scope;
            this.descriptor = descriptor == null ? null : descriptor.clone();
            this.credential = credential == null ? null : credential.copy();
            this.sealed = sealed;
            this.grant = null;
            this.closedGrantId = null;
        }

        private Command(ProfileControlScope scope, GrantBinding grant) {
            this.type = SegmentControlFormat.Type.RECOVERY_GRANT;
            this.scope = scope;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = grant;
            this.closedGrantId = null;
        }

        private Command(ProfileControlScope scope, GrantBinding grant, UUID closedGrantId) {
            this.type = SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE;
            this.scope = scope;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = grant;
            this.closedGrantId = closedGrantId;
        }

        private UUID operationId() {
            ByteBuffer bytes = ByteBuffer.wrap(scope.operationId());
            return new UUID(bytes.getLong(), bytes.getLong());
        }

        private boolean sameMeaning(Command other) {
            return type == other.type && scope.equals(other.scope)
                    && Arrays.equals(descriptor, other.descriptor)
                    && (credential == null ? other.credential == null : credential.matches(other.credential))
                    && (grant == null ? other.grant == null : grant.sameMeaning(other.grant))
                    && Objects.equals(closedGrantId, other.closedGrantId);
        }
    }

    private record AppliedOperation(Command command, Status status) { }

    private static final class GrantState {
        private final Command command;
        private int inFlight;
        private UUID pendingClose;
        private boolean committedReadable;

        private GrantState(Command command) {
            this.command = command;
        }
    }

    private static final class LedgerState {
        private final ProfileControlScope install;
        private final ProfileLedgerContext context;
        private final byte[] descriptor;
        private final ProtectedProfileCredential credential;
        private long localGeneration = 1;
        private long activationGeneration;
        private boolean active;
        private boolean fenced;
        private boolean pendingTombstone;
        private UUID pendingFence;
        private ExplicitLac explicitLac;
        private final Map<UUID, GrantState> recoveryGrants = new HashMap<>();
        private int inFlight;

        private LedgerState(Command command) {
            install = command.scope;
            context = context(command.scope);
            descriptor = command.descriptor;
            credential = command.credential;
        }
    }

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final long maxLogBytes;
    private final SegmentFileIo io;
    private final SecretKey key;
    private final ProfileDescriptorValidator descriptorValidator;
    private final FileChannel channel;
    private final FileLock lock;
    private final Map<Long, LedgerState> ledgers = new HashMap<>();
    private final Map<Long, ProfileLedgerContext> tombstones = new HashMap<>();
    private final Map<UUID, AppliedOperation> operations = new HashMap<>();
    private final Map<UUID, ProfileLedgerContext> fenceOperations = new HashMap<>();
    private long nextOffset = FIRST_RECORD;
    private long durableThrough = -1;
    private long appliedThrough = -1;
    private DurableBatch pending;
    private FenceWrite pendingFenceWrite;
    private LacWrite pendingLacWrite;
    private RecoveryCloseWrite pendingRecoveryCloseWrite;
    private boolean writing;
    private boolean failed;
    private boolean closed;
    private int activeAdmissions;

    private SegmentBookieControlLog(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                    long maxLogBytes, SegmentFileIo io, SecretKey key,
                                    ProfileDescriptorValidator descriptorValidator,
                                    FileChannel channel, FileLock lock) {
        this.config = config;
        this.identity = identity;
        this.maxLogBytes = maxLogBytes;
        this.io = io;
        this.key = key;
        this.descriptorValidator = descriptorValidator;
        this.channel = channel;
        this.lock = lock;
    }

    static SegmentBookieControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                        long maxLogBytes, SecretKey key,
                                        ProfileDescriptorValidator descriptorValidator) throws IOException {
        return open(config, identity, maxLogBytes, key, descriptorValidator, new SegmentFileIo());
    }

    static SegmentBookieControlLog open(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                        long maxLogBytes, SecretKey key,
                                        ProfileDescriptorValidator descriptorValidator, SegmentFileIo io)
            throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(descriptorValidator, "descriptorValidator");
        Objects.requireNonNull(io, "io");
        if (maxLogBytes < FIRST_RECORD + 2048) {
            throw new IllegalArgumentException("Bookie control capacity cannot hold an install");
        }
        FileChannel channel = FileChannel.open(config.directory().resolve("bookie-control"),
                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        FileLock lock = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Bookie control log already has a writer", overlapping);
            }
            if (lock == null) {
                throw new IOException("Bookie control log already has a writer");
            }
            SegmentBookieControlLog result = new SegmentBookieControlLog(config, identity, maxLogBytes,
                    io, key, descriptorValidator, channel, lock);
            result.validateStorageFiles();
            result.recover();
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            if (lock != null) {
                try {
                    lock.release();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                channel.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Force all records first; conditional transitions are applied only by {@link #applyDurable}. */
    DurableBatch appendDurably(List<ProtectedProfileStateStore.Transition> transitions) throws IOException {
        List<ProtectedProfileStateStore.Transition> input = List.copyOf(transitions);
        if (input.isEmpty() || input.size() > config.maxPendingEntries()) {
            throw new IllegalArgumentException("Invalid Bookie control batch size");
        }
        List<Command> commands = new ArrayList<>(input.size());
        for (ProtectedProfileStateStore.Transition transition : input) {
            commands.add(command(transition));
        }
        return appendCommands(commands);
    }

    /** The caller must have cold-read the live RepairIntent and exact committed grant authority. */
    Result submitRecoveryGrant(RecoveryGrant grant) throws IOException {
        DurableBatch batch = appendRecoveryGrantDurably(grant);
        return applyDurable(batch).get(0);
    }

    DurableBatch appendRecoveryGrantDurably(RecoveryGrant grant) throws IOException {
        return appendCommands(List.of(command(grant)));
    }

    /** Cut admission before the durable close; retry after the pre-cut tickets become terminal. */
    RecoveryCloseStatus submitRecoveryClose(RecoveryGrantClose close) throws IOException {
        RecoveryCloseAttempt attempt = prepareRecoveryClose(close);
        if (attempt.immediate() != null) {
            return attempt.immediate();
        }
        writeRecoveryCloseDurably(attempt.write());
        return applyRecoveryCloseDurable(attempt.write());
    }

    synchronized RecoveryCloseAttempt prepareRecoveryClose(RecoveryGrantClose close) throws IOException {
        Command command = command(close);
        requireWritable();
        AppliedOperation prior = operations.get(command.operationId());
        if (prior != null) {
            return new RecoveryCloseAttempt(prior.command().sameMeaning(command)
                    ? closeStatus(prior.status() == Status.APPLIED_DURABLE
                            ? Status.ALREADY_DURABLE : prior.status())
                    : RecoveryCloseStatus.INCOMPATIBLE, null);
        }
        if (fenceOperations.containsKey(command.operationId())) {
            return new RecoveryCloseAttempt(RecoveryCloseStatus.INCOMPATIBLE, null);
        }
        LedgerState state = ledgers.get(command.scope.ledgerId());
        GrantState grant = state == null ? null : state.recoveryGrants.get(command.closedGrantId);
        if (grant != null && !grant.committedReadable && matchesClose(command, grant.command)
                && (grant.pendingClose == null || grant.pendingClose.equals(command.operationId()))) {
            grant.pendingClose = command.operationId();
            if (grant.inFlight != 0) {
                return new RecoveryCloseAttempt(RecoveryCloseStatus.WAITING_PRECUT, null);
            }
        }
        if (writing || pending != null || pendingFenceWrite != null || pendingLacWrite != null
                || pendingRecoveryCloseWrite != null) {
            return new RecoveryCloseAttempt(RecoveryCloseStatus.WAITING_PRECUT, null);
        }
        long sequence = durableThrough + 1;
        ByteBuffer record = SegmentControlFormat.encode(command.type, LOG_GENERATION, sequence,
                command.operationId(), new Generation(command.scope.authorityGeneration()), 0, body(command));
        if (record.remaining() > maxLogBytes - nextOffset) {
            throw new IOException("Bookie control log capacity exhausted");
        }
        RecoveryCloseWrite write = new RecoveryCloseWrite(command, record, sequence, nextOffset);
        nextOffset += record.remaining();
        pendingRecoveryCloseWrite = write;
        writing = true;
        return new RecoveryCloseAttempt(null, write);
    }

    void writeRecoveryCloseDurably(RecoveryCloseWrite write) throws IOException {
        synchronized (this) {
            requireWritable();
            if (write != pendingRecoveryCloseWrite || write.durable) {
                throw new IllegalArgumentException("Invalid or already durable recovery close write");
            }
        }
        try {
            io.writeFully(channel, write.record, write.offset);
            io.force(channel, true);
            synchronized (this) {
                write.durable = true;
                durableThrough = write.sequence;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
                pendingRecoveryCloseWrite = null;
                writing = false;
            }
            throw failure;
        }
    }

    synchronized void abortRecoveryCloseBeforeIo(RecoveryCloseWrite write) {
        if (write != pendingRecoveryCloseWrite || write.durable || !writing) {
            throw new IllegalArgumentException("Only an unwritten recovery close can be retried");
        }
        nextOffset = write.offset;
        pendingRecoveryCloseWrite = null;
        writing = false;
    }

    synchronized RecoveryCloseStatus applyRecoveryCloseDurable(RecoveryCloseWrite write) throws IOException {
        requireWritable();
        if (write != pendingRecoveryCloseWrite || !write.durable || write.sequence != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable recovery close can be applied");
        }
        try {
            Status status = apply(write.command);
            appliedThrough = write.sequence;
            return closeStatus(status);
        } catch (RuntimeException | Error failure) {
            failed = true;
            throw failure;
        } finally {
            pendingRecoveryCloseWrite = null;
            writing = false;
        }
    }

    private static RecoveryCloseStatus closeStatus(Status status) {
        return switch (status) {
            case APPLIED_DURABLE -> RecoveryCloseStatus.APPLIED_DURABLE;
            case ALREADY_DURABLE -> RecoveryCloseStatus.ALREADY_DURABLE;
            case CONDITION_FAILED -> RecoveryCloseStatus.CONDITION_FAILED;
            case INCOMPATIBLE -> RecoveryCloseStatus.INCOMPATIBLE;
        };
    }

    private DurableBatch appendCommands(List<Command> commands) throws IOException {
        List<ByteBuffer> records = new ArrayList<>(commands.size());
        long first;
        long start;
        synchronized (this) {
            requireWritable();
            if (writing || pending != null || pendingFenceWrite != null || pendingLacWrite != null
                    || pendingRecoveryCloseWrite != null) {
                throw new IllegalArgumentException("Invalid or unresolved Bookie control batch");
            }
            for (Command command : commands) {
                LedgerState state = ledgers.get(command.scope.ledgerId());
                if (command.type == SegmentControlFormat.Type.ACTIVATE
                        && state != null && state.pendingFence != null) {
                    throw new IllegalArgumentException("Bookie activation is after the pending fence cut");
                }
            }
            long bytes = 0;
            first = durableThrough + 1;
            for (Command command : commands) {
                ByteBuffer record = SegmentControlFormat.encode(command.type, LOG_GENERATION,
                        first + records.size(), command.operationId(),
                        new Generation(command.scope.authorityGeneration()), 0, body(command));
                bytes = Math.addExact(bytes, record.remaining());
                records.add(record);
            }
            if (bytes > maxLogBytes - nextOffset) {
                throw new IOException("Bookie control log capacity exhausted");
            }
            start = nextOffset;
            nextOffset += bytes;
            for (Command command : commands) {
                if (command.type == SegmentControlFormat.Type.TOMBSTONE
                        && !operations.containsKey(command.operationId())
                        && !fenceOperations.containsKey(command.operationId())) {
                    LedgerState state = ledgers.get(command.scope.ledgerId());
                    if (state != null && state.context.equals(context(command.scope))
                            && command.scope.authorityGeneration() >= state.install.authorityGeneration()) {
                        state.pendingTombstone = true;
                    }
                }
            }
            writing = true;
        }
        try {
            long position = start;
            for (ByteBuffer record : records) {
                io.writeFully(channel, record, position);
                position += record.remaining();
            }
            io.force(channel, true);
            synchronized (this) {
                durableThrough = first + commands.size() - 1;
                pending = new DurableBatch(first, commands);
                return pending;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
            }
            throw failure;
        } finally {
            synchronized (this) {
                writing = false;
            }
        }
    }

    synchronized List<Result> applyDurable(DurableBatch batch) throws IOException {
        requireWritable();
        if (pending != batch || batch.firstSequence() != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable Bookie batch can be applied");
        }
        List<Result> results = new ArrayList<>(batch.commands().size());
        try {
            for (Command command : batch.commands()) {
                long sequence = ++appliedThrough;
                Status status = apply(command);
                LedgerState state = ledgers.get(command.scope.ledgerId());
                results.add(new Result(status, command.operationId(), sequence,
                        state == null ? 0 : state.localGeneration));
            }
        } catch (RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
        pending = null;
        return List.copyOf(results);
    }

    List<Result> submitBatch(List<ProtectedProfileStateStore.Transition> transitions) throws IOException {
        return applyDurable(appendDurably(transitions));
    }

    /** Diagnostic predicate only; a DATA request must hold an {@link Admission} through physical termination. */
    synchronized boolean normalAdmission(ProfileLedgerContext context, byte[] wireVerifier) {
        LedgerState state = ledgers.get(context.ledgerId());
        return !closed && !failed && state != null && state.active && !state.fenced
                && !state.pendingTombstone && !tombstones.containsKey(context.ledgerId())
                && state.pendingFence == null && state.context.equals(context)
                && state.credential.matchesWireVerifier(wireVerifier);
    }

    synchronized Optional<Admission> admitNormal(ProfileLedgerContext context, byte[] wireVerifier) {
        if (!normalAdmission(context, wireVerifier) || activeAdmissions == config.maxPendingEntries()) {
            return Optional.empty();
        }
        LedgerState state = ledgers.get(context.ledgerId());
        state.inFlight++;
        activeAdmissions++;
        return Optional.of(new Admission(this, state));
    }

    synchronized Optional<RecoveryAdmission> admitRecovery(ProfileOperationCodec.AddRecovery request) {
        Objects.requireNonNull(request, "request");
        ProfileLedgerContext context = request.context();
        LedgerState state = ledgers.get(context.ledgerId());
        if (closed || failed || state == null || !state.context.equals(context)
                || state.pendingTombstone || tombstones.containsKey(context.ledgerId())
                || !state.credential.matchesWireVerifier(request.credential())
                || activeAdmissions == config.maxPendingEntries()) {
            return Optional.empty();
        }
        for (GrantState grant : state.recoveryGrants.values()) {
            ProfileExactRange range = grant.command.scope.exactRange();
            if (grant.pendingClose == null && !grant.committedReadable
                    && Arrays.equals(grant.command.grant.intentId(), request.repairIntentId())
                    && grant.command.grant.intentGeneration() == request.repairIntentGeneration()
                    && grant.command.grant.grantGeneration() == request.grantGeneration()
                    && range.firstEntryId() == request.rangeStart()
                    && range.lastEntryId() == request.rangeEnd()
                    && request.entryId() >= range.firstEntryId()
                    && request.entryId() <= range.lastEntryId()) {
                activeAdmissions++;
                grant.inFlight++;
                return Optional.of(new RecoveryAdmission(this, state, grant, request));
            }
        }
        return Optional.empty();
    }

    synchronized boolean canCompleteRecovery(RecoveryAdmission admission) {
        if (admission.owner != this || admission.terminal) {
            throw new IllegalArgumentException("Recovery result requires a live Bookie admission");
        }
        LedgerState state = admission.state;
        return !closed && !failed && ledgers.get(state.context.ledgerId()) == state
                && !state.pendingTombstone && !tombstones.containsKey(state.context.ledgerId())
                && admission.grant.pendingClose == null && !admission.grant.committedReadable
                && state.recoveryGrants.get(admission.grant.command.operationId()) == admission.grant;
    }

    private synchronized void finishRecovery(RecoveryAdmission admission, boolean fromDriver) {
        if (admission.owner != this || admission.terminal || admission.claimed != fromDriver) {
            throw new IllegalArgumentException("Recovery admission already finished or belongs to another control log");
        }
        admission.terminal = true;
        admission.entryPayload = null;
        admission.grant.inFlight--;
        activeAdmissions--;
    }

    private synchronized byte[] takeRecoveryPayload(RecoveryAdmission admission) {
        if (admission.owner != this || admission.terminal || !admission.claimed || admission.entryPayload == null) {
            throw new IllegalArgumentException("Recovery payload requires exclusive live driver ownership");
        }
        byte[] bytes = admission.entryPayload;
        admission.entryPayload = null;
        return bytes;
    }

    synchronized void claimRecoveryForDriver(List<RecoveryAdmission> admissions) throws IOException {
        requireWritable();
        if (admissions.isEmpty()) {
            throw new IllegalArgumentException("Driver requires recovery admission for every entry");
        }
        Map<RecoveryAdmission, Boolean> distinct = new IdentityHashMap<>();
        for (RecoveryAdmission admission : admissions) {
            if (admission == null || admission.owner != this || admission.terminal || admission.claimed
                    || distinct.put(admission, true) != null) {
                throw new IllegalArgumentException("Invalid or repeated Bookie recovery admission");
            }
        }
        for (RecoveryAdmission admission : admissions) {
            admission.claimed = true;
        }
    }

    /** Local fact only; a read still needs its cluster selector and visibility authority. */
    synchronized boolean locallyCommittedReadable(ProfileLedgerContext context, long entryId) {
        LedgerState state = ledgers.get(context.ledgerId());
        if (closed || failed || state == null || !state.context.equals(context)
                || state.pendingTombstone || tombstones.containsKey(context.ledgerId())) {
            return false;
        }
        boolean coveredByClosedGrant = false;
        for (GrantState grant : state.recoveryGrants.values()) {
            ProfileExactRange range = grant.command.scope.exactRange();
            if (entryId >= range.firstEntryId() && entryId <= range.lastEntryId()) {
                if (!grant.committedReadable) {
                    return false;
                }
                coveredByClosedGrant = true;
            }
        }
        return coveredByClosedGrant;
    }

    /** Only the local route cut; transport read authorization and current-selector checks remain separate. */
    synchronized Optional<ReadAdmission> admitRead(ProfileLedgerContext context) {
        if (!locallyReadableContext(context) || activeAdmissions == config.maxPendingEntries()) {
            return Optional.empty();
        }
        LedgerState state = ledgers.get(context.ledgerId());
        activeAdmissions++;
        return Optional.of(new ReadAdmission(this, state));
    }

    /** Local replayed route only; it does not select a physical location or grant network read authority. */
    synchronized boolean locallyReadableContext(ProfileLedgerContext context) {
        LedgerState state = ledgers.get(context.ledgerId());
        return !closed && !failed && state != null && state.context.equals(context)
                && !state.pendingTombstone && !tombstones.containsKey(context.ledgerId());
    }

    /** Local replayed value only; a Profile read still needs its own authorization and visibility boundary. */
    synchronized Optional<ExplicitLac> explicitLac(ProfileLedgerContext context) {
        if (!locallyReadableContext(context)) {
            return Optional.empty();
        }
        return Optional.ofNullable(ledgers.get(context.ledgerId()).explicitLac);
    }

    synchronized boolean canCompleteRead(ReadAdmission admission) {
        if (admission.owner != this || admission.terminal) {
            throw new IllegalArgumentException("Read result requires a live Bookie admission");
        }
        LedgerState state = admission.state;
        return !closed && !failed && ledgers.get(state.context.ledgerId()) == state
                && !state.pendingTombstone && !tombstones.containsKey(state.context.ledgerId());
    }

    private synchronized void finishRead(ReadAdmission admission) {
        if (admission.owner != this || admission.terminal) {
            throw new IllegalArgumentException("Read admission already finished or belongs to another control log");
        }
        admission.terminal = true;
        activeAdmissions--;
    }

    synchronized void claimForDriver(List<Admission> admissions) throws IOException {
        requireWritable();
        if (admissions.isEmpty()) {
            throw new IllegalArgumentException("Driver requires admission for every entry");
        }
        Map<Admission, Boolean> distinct = new IdentityHashMap<>();
        for (Admission admission : admissions) {
            if (admission == null || admission.owner != this || admission.terminal || admission.claimed
                    || distinct.put(admission, true) != null) {
                throw new IllegalArgumentException("Invalid or repeated Bookie DATA admission");
            }
        }
        for (Admission admission : admissions) {
            admission.claimed = true;
        }
    }

    /** Check the captured normal admission before its I/O driver releases the ticket. */
    synchronized boolean canCompleteNormal(Admission admission) {
        if (admission.owner != this || !admission.claimed || admission.terminal) {
            throw new IllegalArgumentException("Normal result requires a live physical admission");
        }
        LedgerState state = admission.state;
        // A pending fence is after this pre-cut admission; the durable fence waits for this ticket.
        return !closed && !failed && ledgers.get(state.context.ledgerId()) == state
                && state.active && !state.fenced && !state.pendingTombstone
                && !tombstones.containsKey(state.context.ledgerId())
                && state.localGeneration == admission.generation;
    }

    synchronized boolean matchesScope(IsolatedSegmentFiles files) {
        return !closed && files.matchesScope(config, identity);
    }

    synchronized boolean matchesScope(IsolatedSegmentConfig source, SegmentFileHeader sourceIdentity) {
        return !closed && !failed && config.sameStorageScope(source) && identity.equals(sourceIdentity);
    }

    /** Capture or recheck a replayed local read cut without holding the Bookie monitor during DATA I/O. */
    synchronized long recoveryReadCut(IsolatedSegmentConfig source, SegmentFileHeader sourceIdentity)
            throws IOException {
        if (!matchesScope(source, sourceIdentity) || writing || pending != null || activeAdmissions != 0) {
            throw new IOException("Bookie recovery read cut has unresolved control or entry obligations");
        }
        return appliedThrough;
    }

    private synchronized void finishAdmission(Admission admission, boolean fromDriver) {
        if (admission.owner != this || admission.terminal || admission.claimed != fromDriver) {
            throw new IllegalArgumentException("Admission already finished or belongs to another Bookie control log");
        }
        admission.terminal = true;
        admission.state.inFlight--;
        activeAdmissions--;
    }

    /** A cold, authenticated LAC update uses the Bookie control barrier, never the normal DATA path. */
    Status submitExplicitLac(ProfileOperationCodec.WriteLac request) throws IOException {
        LacAttempt attempt = prepareExplicitLac(request);
        if (attempt.immediate() != null) {
            return attempt.immediate();
        }
        writeExplicitLacDurably(attempt.write());
        return applyExplicitLacDurable(attempt.write());
    }

    synchronized LacAttempt prepareExplicitLac(ProfileOperationCodec.WriteLac request) throws IOException {
        Objects.requireNonNull(request, "request");
        ProfileLedgerContext context = request.context();
        byte[] body = request.body();
        byte[] verifier = request.credential();
        if (body.length > 65536 - 128 - 16 - ProfileLedgerContext.ENCODED_LENGTH - 12) {
            throw new IllegalArgumentException("Explicit LAC body exceeds the control record bound");
        }
        requireWritable();
        if (writing || pending != null) {
            throw new IOException("Bookie control operation is unresolved");
        }
        LedgerState state = ledgers.get(context.ledgerId());
        if (request.lac() < -1 || state == null || !state.context.equals(context)
                || !state.credential.matchesWireVerifier(verifier)
                || state.pendingTombstone || tombstones.containsKey(context.ledgerId())) {
            return new LacAttempt(Status.CONDITION_FAILED, null);
        }
        ExplicitLac prior = state.explicitLac;
        if (prior != null && request.lac() <= prior.lac()) {
            if (request.lac() < prior.lac()) {
                return new LacAttempt(Status.CONDITION_FAILED, null);
            }
            return new LacAttempt(Arrays.equals(body, prior.body()) ? Status.ALREADY_DURABLE : Status.INCOMPATIBLE,
                    null);
        }
        if (!state.active || state.fenced || state.pendingFence != null) {
            return new LacAttempt(Status.CONDITION_FAILED, null);
        }
        long sequence = durableThrough + 1;
        long generation = state.localGeneration;
        ByteBuffer value = ByteBuffer.allocate(Math.addExact(ProfileLedgerContext.ENCODED_LENGTH + 12,
                body.length)).put(context.encode()).putLong(request.lac()).putInt(body.length).put(body).flip();
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.EXPLICIT_LAC, LOG_GENERATION,
                sequence, UUID.randomUUID(), new Generation(generation), 0, value);
        if (record.remaining() > maxLogBytes - nextOffset) {
            throw new IOException("Bookie control log capacity exhausted");
        }
        LacWrite write = new LacWrite(context, request.lac(), body, record, sequence, nextOffset, generation);
        nextOffset += record.remaining();
        pendingLacWrite = write;
        writing = true;
        return new LacAttempt(null, write);
    }

    void writeExplicitLacDurably(LacWrite write) throws IOException {
        synchronized (this) {
            requireWritable();
            if (write != pendingLacWrite || write.durable) {
                throw new IllegalArgumentException("Invalid or already durable explicit LAC write");
            }
        }
        try {
            io.writeFully(channel, write.record, write.offset);
            io.force(channel, true);
            synchronized (this) {
                write.durable = true;
                durableThrough = write.sequence;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
                pendingLacWrite = null;
                writing = false;
            }
            throw failure;
        }
    }

    synchronized void abortExplicitLacBeforeIo(LacWrite write) {
        if (write != pendingLacWrite || write.durable || !writing) {
            throw new IllegalArgumentException("Only an unwritten explicit LAC can be retried");
        }
        nextOffset = write.offset;
        pendingLacWrite = null;
        writing = false;
    }

    synchronized Status applyExplicitLacDurable(LacWrite write) throws IOException {
        requireWritable();
        if (write != pendingLacWrite || !write.durable || write.sequence != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable explicit LAC can be applied");
        }
        try {
            applyExplicitLac(write.context, write.lac, write.body, write.generation);
            appliedThrough = write.sequence;
            return Status.APPLIED_DURABLE;
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        } finally {
            pendingLacWrite = null;
            writing = false;
        }
    }

    private void applyExplicitLac(ProfileLedgerContext context, long lac, byte[] body, long generation)
            throws IOException {
        LedgerState state = ledgers.get(context.ledgerId());
        if (state == null || !state.context.equals(context) || !state.active || state.fenced
                || tombstones.containsKey(context.ledgerId()) || state.localGeneration != generation || lac < -1) {
            throw new IOException("Invalid durable explicit LAC authority");
        }
        ExplicitLac prior = state.explicitLac;
        if (prior != null && (lac < prior.lac() || (lac == prior.lac() && !Arrays.equals(body, prior.body())))) {
            throw new IOException("Non-monotonic durable explicit LAC");
        }
        if (prior == null || lac > prior.lac()) {
            state.explicitLac = new ExplicitLac(lac, body);
        }
    }

    /** Closes the admission cut first; caller retries WAITING_PRECUT after prior DATA becomes terminal. */
    FenceResult submitFence(ProfileOperationCodec.FenceLedger request) throws IOException {
        FenceAttempt attempt = prepareFence(request);
        if (attempt.immediate() != null) {
            return attempt.immediate();
        }
        writeFenceDurably(attempt.write());
        return applyFenceDurable(attempt.write());
    }

    synchronized FenceAttempt prepareFence(ProfileOperationCodec.FenceLedger request) throws IOException {
        Objects.requireNonNull(request, "request");
        ProfileLedgerContext context = request.context();
        UUID operationId = operationId(request.operationId());
        requireWritable();
        LedgerState state = ledgers.get(context.ledgerId());
        if (state == null || tombstones.containsKey(context.ledgerId()) || state.pendingTombstone
                || !state.context.equals(context)
                || !state.credential.matchesWireVerifier(request.credential())) {
            return new FenceAttempt(new FenceResult(FenceStatus.REJECTED, 0), null);
        }
        ProfileLedgerContext prior = fenceOperations.get(operationId);
        if (prior != null) {
            return new FenceAttempt(new FenceResult(
                    prior.equals(context) ? FenceStatus.ALREADY_DURABLE : FenceStatus.REJECTED,
                    prior.equals(context) ? state.localGeneration : 0), null);
        }
        if (operations.containsKey(operationId)) {
            return new FenceAttempt(new FenceResult(FenceStatus.REJECTED, 0), null);
        }
        if (state.fenced) {
            return new FenceAttempt(new FenceResult(FenceStatus.ALREADY_DURABLE, state.localGeneration), null);
        }
        if (state.pendingFence != null && !state.pendingFence.equals(operationId)) {
            return new FenceAttempt(new FenceResult(FenceStatus.WAITING_PRECUT, state.localGeneration), null);
        }
        state.pendingFence = operationId;
        if (state.inFlight != 0 || writing || pending != null) {
            return new FenceAttempt(new FenceResult(FenceStatus.WAITING_PRECUT, state.localGeneration), null);
        }
        long sequence = durableThrough + 1;
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.FENCE, LOG_GENERATION,
                sequence, operationId, LOG_GENERATION, 0, ByteBuffer.wrap(context.encode()));
        if (record.remaining() > maxLogBytes - nextOffset) {
            throw new IOException("Bookie control log capacity exhausted");
        }
        FenceWrite write = new FenceWrite(context, operationId, record, sequence, nextOffset);
        nextOffset += record.remaining();
        pendingFenceWrite = write;
        writing = true;
        return new FenceAttempt(null, write);
    }

    void writeFenceDurably(FenceWrite write) throws IOException {
        synchronized (this) {
            requireWritable();
            if (write != pendingFenceWrite || write.durable) {
                throw new IllegalArgumentException("Invalid or already durable Bookie fence write");
            }
        }
        try {
            io.writeFully(channel, write.record, write.offset);
            io.force(channel, true);
            synchronized (this) {
                write.durable = true;
                durableThrough = write.sequence;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
                pendingFenceWrite = null;
                writing = false;
            }
            throw failure;
        }
    }

    synchronized void abortFenceBeforeIo(FenceWrite write) {
        if (write != pendingFenceWrite || write.durable || !writing) {
            throw new IllegalArgumentException("Only an unwritten Bookie fence can be retried");
        }
        nextOffset = write.offset;
        pendingFenceWrite = null;
        writing = false;
    }

    synchronized FenceResult applyFenceDurable(FenceWrite write) throws IOException {
        requireWritable();
        if (write != pendingFenceWrite || !write.durable || write.sequence != appliedThrough + 1) {
            throw new IllegalArgumentException("Only the next durable Bookie fence can be applied");
        }
        try {
            applyFence(write.context, write.operationId);
            appliedThrough = write.sequence;
            return new FenceResult(FenceStatus.APPLIED_DURABLE,
                    ledgers.get(write.context.ledgerId()).localGeneration);
        } catch (IOException | RuntimeException | Error failure) {
            failed = true;
            throw failure;
        } finally {
            pendingFenceWrite = null;
            writing = false;
        }
    }

    private void applyFence(ProfileLedgerContext context, UUID operationId) throws IOException {
        LedgerState state = ledgers.get(context.ledgerId());
        if (state == null || tombstones.containsKey(context.ledgerId())
                || !state.context.equals(context) || state.fenced || state.inFlight != 0
                || operations.containsKey(operationId) || fenceOperations.containsKey(operationId)) {
            throw new IOException("Invalid durable fence order");
        }
        state.active = false;
        state.fenced = true;
        state.pendingFence = null;
        state.localGeneration++;
        fenceOperations.put(operationId, context);
    }

    private static UUID operationId(byte[] bytes) {
        ByteBuffer input = ByteBuffer.wrap(bytes);
        return new UUID(input.getLong(), input.getLong());
    }

    private Command command(ProtectedProfileStateStore.Transition transition) {
        ProfileControlScope scope = transition.request().scope();
        CommittedProfileAuthority authority = transition.authority();
        if (!authority.scope().equals(scope) || !authority.permits(scope.operation())
                || authority.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || !matchesTarget(scope)) {
            throw new IllegalArgumentException("Bookie control authority or target mismatch");
        }
        if (scope.operation() == ProfileControlOperation.INSTALL) {
            byte[] descriptor = transition.request().canonicalDescriptor().orElseThrow();
            ProfileDescriptor decoded = ProfileDescriptorCodec.decodeAndVerify(descriptor,
                    scope.descriptorIdentity(), descriptorValidator);
            if (decoded.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                    || transition.request().expectedEngine().orElse(null) != decoded.requiredEngine()
                    || !decoded.equals(transition.decodedDescriptor())) {
                throw new IllegalArgumentException("Bookie install descriptor mismatch");
            }
            ProtectedProfileCredential credential = transition.request().protectedCredential().orElseThrow();
            return new Command(SegmentControlFormat.Type.INSTALL, scope, descriptor, credential,
                    credential.sealForStorage(key, storageBinding(scope)));
        }
        if (scope.operation() == ProfileControlOperation.ACTIVATE_INITIAL
                || scope.operation() == ProfileControlOperation.ACTIVATE_REPLACEMENT) {
            return new Command(SegmentControlFormat.Type.ACTIVATE, scope, null, null, null);
        }
        if (scope.operation() == ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY) {
            return new Command(SegmentControlFormat.Type.TOMBSTONE, scope, null, null, null);
        }
        throw new IllegalArgumentException("Bookie control operation is not implemented");
    }

    private Command command(RecoveryGrant grant) {
        ProtectedProfileStateStore.Transition transition = grant.transition();
        ProfileControlScope scope = transition.request().scope();
        CommittedProfileAuthority authority = transition.authority();
        if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT
                || scope.exactRange() == null || !authority.scope().equals(scope)
                || !authority.permits(scope.operation())
                || authority.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || !matchesTarget(scope) || grant.grantGeneration() != scope.authorityGeneration()
                || !Arrays.equals(scope.publicSemanticPayloadIdentity(),
                        recoveryGrantPayloadIdentity(scope, grant.repairIntentId(),
                                grant.repairIntentGeneration(), grant.grantGeneration()))) {
            throw new IllegalArgumentException("Bookie recovery grant authority or binding mismatch");
        }
        return new Command(scope, new GrantBinding(grant.repairIntentId(),
                grant.repairIntentGeneration(), grant.grantGeneration()));
    }

    private Command command(RecoveryGrantClose close) {
        ProfileControlScope scope = close.transition().request().scope();
        CommittedProfileAuthority authority = close.transition().authority();
        if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE
                || scope.exactRange() == null || !authority.scope().equals(scope)
                || !authority.permits(scope.operation())
                || authority.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || !matchesTarget(scope) || close.grantGeneration() <= 0
                || !Arrays.equals(scope.publicSemanticPayloadIdentity(),
                        recoveryClosePayloadIdentity(scope, close.grantOperationId(),
                                close.repairIntentId(), close.repairIntentGeneration(), close.grantGeneration()))) {
            throw new IllegalArgumentException("Bookie recovery close authority or binding mismatch");
        }
        return new Command(scope, new GrantBinding(close.repairIntentId(),
                close.repairIntentGeneration(), close.grantGeneration()), close.grantOperationId());
    }

    private Status apply(Command command) {
        UUID operationId = command.operationId();
        LedgerState state = ledgers.get(command.scope.ledgerId());
        if (command.type == SegmentControlFormat.Type.TOMBSTONE && state != null
                && state.context.equals(context(command.scope))) {
            state.pendingTombstone = false;
        }
        if (fenceOperations.containsKey(operationId)) {
            return Status.INCOMPATIBLE;
        }
        AppliedOperation prior = operations.get(operationId);
        if (prior != null) {
            return prior.command().sameMeaning(command)
                    ? prior.status() == Status.APPLIED_DURABLE ? Status.ALREADY_DURABLE : prior.status()
                    : Status.INCOMPATIBLE;
        }
        Status status;
        if (command.type == SegmentControlFormat.Type.INSTALL) {
            if (tombstones.containsKey(command.scope.ledgerId())) {
                status = Status.CONDITION_FAILED;
            } else if (state == null) {
                state = new LedgerState(command);
                ledgers.put(command.scope.ledgerId(), state);
                status = Status.APPLIED_DURABLE;
            } else if (state.context.equals(context(command.scope))
                    && Arrays.equals(state.descriptor, command.descriptor)
                    && state.credential.matches(command.credential)
                    && Arrays.equals(state.install.publicSemanticPayloadIdentity(),
                            command.scope.publicSemanticPayloadIdentity())) {
                status = Status.ALREADY_DURABLE;
            } else {
                status = Status.CONDITION_FAILED;
            }
        } else if (command.type == SegmentControlFormat.Type.TOMBSTONE) {
            ProfileLedgerContext target = context(command.scope);
            ProfileLedgerContext terminal = tombstones.get(command.scope.ledgerId());
            if (terminal != null) {
                status = terminal.equals(target) ? Status.ALREADY_DURABLE : Status.CONDITION_FAILED;
            } else if (state != null && (!state.context.equals(target)
                    || command.scope.authorityGeneration() < state.install.authorityGeneration())) {
                status = Status.CONDITION_FAILED;
            } else {
                tombstones.put(command.scope.ledgerId(), target);
                if (state != null) {
                    state.active = false;
                    state.pendingFence = null;
                    state.localGeneration++;
                    state.recoveryGrants.clear();
                }
                status = Status.APPLIED_DURABLE;
            }
        } else if (command.type == SegmentControlFormat.Type.RECOVERY_GRANT) {
            if (tombstones.containsKey(command.scope.ledgerId()) || state == null
                    || !state.context.equals(context(command.scope)) || state.pendingTombstone
                    || command.scope.authorityGeneration() < state.install.authorityGeneration()
                    || state.recoveryGrants.size() >= config.maxPendingEntries()) {
                status = Status.CONDITION_FAILED;
            } else {
                state.recoveryGrants.put(operationId, new GrantState(command));
                status = Status.APPLIED_DURABLE;
            }
        } else if (command.type == SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE) {
            GrantState grant = state == null ? null : state.recoveryGrants.get(command.closedGrantId);
            if (tombstones.containsKey(command.scope.ledgerId()) || state == null
                    || !state.context.equals(context(command.scope)) || state.pendingTombstone
                    || grant == null || grant.committedReadable || grant.inFlight != 0
                    || (grant.pendingClose != null && !grant.pendingClose.equals(operationId))
                    || !matchesClose(command, grant.command)) {
                status = Status.CONDITION_FAILED;
            } else {
                grant.pendingClose = operationId;
                grant.committedReadable = true;
                status = Status.APPLIED_DURABLE;
            }
        } else {
            if (tombstones.containsKey(command.scope.ledgerId())
                    || state == null || !state.context.equals(context(command.scope))
                    || state.fenced
                    || command.scope.authorityGeneration() < state.install.authorityGeneration()
                    || command.scope.authorityGeneration() <= state.activationGeneration
                    || (command.scope.operation() == ProfileControlOperation.ACTIVATE_INITIAL
                            && state.activationGeneration != 0)) {
                status = Status.CONDITION_FAILED;
            } else {
                state.active = true;
                state.activationGeneration = command.scope.authorityGeneration();
                state.localGeneration++;
                status = Status.APPLIED_DURABLE;
            }
        }
        operations.put(operationId, new AppliedOperation(command, status));
        return status;
    }

    private static boolean matchesClose(Command close, Command grant) {
        return grant.scope.ledgerId() == close.scope.ledgerId()
                && Arrays.equals(grant.scope.ledgerInstanceId(), close.scope.ledgerInstanceId())
                && grant.scope.descriptorIdentity().equals(close.scope.descriptorIdentity())
                && grant.scope.targetBookie().equals(close.scope.targetBookie())
                && Arrays.equals(grant.scope.targetStorageIncarnation(), close.scope.targetStorageIncarnation())
                && grant.scope.authorityDomain().equals(close.scope.authorityDomain())
                && grant.scope.exactRange().equals(close.scope.exactRange())
                && grant.grant.sameMeaning(close.grant);
    }

    private void recover() throws IOException {
        long size = channel.size();
        if (size < FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid Bookie control log size");
        }
        long sequence = 0;
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            offset += length;
            sequence++;
        }
        channel.force(true);
        sequence = 0;
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            ByteBuffer bytes = ByteBuffer.allocate(length);
            SegmentFileIo.readFully(channel, bytes, offset);
            SegmentControlParser.Command parsed = SegmentControlParser.parse(bytes,
                    SegmentControlFormat.Owner.BOOKIE, LOG_GENERATION, sequence);
            if (parsed.type() == SegmentControlFormat.Type.FENCE) {
                ByteBuffer body = parsed.body();
                if (parsed.operationGeneration().value() != 1 || parsed.expectedGeneration() != 0
                        || body.remaining() != ProfileLedgerContext.ENCODED_LENGTH) {
                    throw new IOException("Invalid durable fence record");
                }
                byte[] contextBytes = new byte[ProfileLedgerContext.ENCODED_LENGTH];
                body.get(contextBytes);
                try {
                    applyFence(ProfileLedgerContext.decode(contextBytes), parsed.operationId());
                } catch (IllegalArgumentException failure) {
                    throw new IOException("Invalid durable fence context", failure);
                }
            } else if (parsed.type() == SegmentControlFormat.Type.EXPLICIT_LAC) {
                replayExplicitLac(parsed);
            } else {
                Command command = decode(parsed);
                apply(command);
            }
            durableThrough = sequence;
            appliedThrough = sequence;
            offset += length;
            sequence++;
        }
        nextOffset = size;
    }

    private void replayExplicitLac(SegmentControlParser.Command parsed) throws IOException {
        ByteBuffer body = parsed.body();
        if (parsed.expectedGeneration() != 0
                || body.remaining() < ProfileLedgerContext.ENCODED_LENGTH + 12) {
            throw new IOException("Invalid durable explicit LAC record");
        }
        byte[] contextBytes = new byte[ProfileLedgerContext.ENCODED_LENGTH];
        body.get(contextBytes);
        long lac = body.getLong();
        int length = body.getInt();
        if (length < 0 || length != body.remaining()) {
            throw new IOException("Invalid durable explicit LAC body length");
        }
        byte[] value = new byte[length];
        body.get(value);
        try {
            applyExplicitLac(ProfileLedgerContext.decode(contextBytes), lac, value,
                    parsed.operationGeneration().value());
        } catch (IllegalArgumentException failure) {
            throw new IOException("Invalid durable explicit LAC context", failure);
        }
    }

    private int recordLength(long offset, long size, long sequence) throws IOException {
        if (size - offset < 128) {
            throw new IOException("Incomplete Bookie control tail");
        }
        ByteBuffer header = ByteBuffer.allocate(128);
        SegmentFileIo.readFully(channel, header, offset);
        int length = SegmentControlParser.recordLength(header, SegmentControlFormat.Owner.BOOKIE,
                LOG_GENERATION, sequence);
        if (length > size - offset) {
            throw new IOException("Incomplete Bookie control record");
        }
        return length;
    }

    private Command decode(SegmentControlParser.Command parsed) throws IOException {
        if (parsed.type() != SegmentControlFormat.Type.INSTALL
                && parsed.type() != SegmentControlFormat.Type.ACTIVATE
                && parsed.type() != SegmentControlFormat.Type.TOMBSTONE
                && parsed.type() != SegmentControlFormat.Type.RECOVERY_GRANT
                && parsed.type() != SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE) {
            throw new IOException("Unsupported Bookie control command");
        }
        try {
            ByteBuffer body = parsed.body();
            ProfileControlScope scope = decodeScope(body, parsed.operationId());
            if (!matchesTarget(scope) || parsed.operationGeneration().value() != scope.authorityGeneration()
                    || parsed.expectedGeneration() != 0) {
                throw new IOException("Bookie control scope or generation mismatch");
            }
            if (parsed.type() == SegmentControlFormat.Type.ACTIVATE) {
                if ((scope.operation() != ProfileControlOperation.ACTIVATE_INITIAL
                        && scope.operation() != ProfileControlOperation.ACTIVATE_REPLACEMENT)
                        || body.hasRemaining()) {
                    throw new IOException("Invalid activation body");
                }
                return new Command(parsed.type(), scope, null, null, null);
            }
            if (parsed.type() == SegmentControlFormat.Type.TOMBSTONE) {
                if (scope.operation() != ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY
                        || body.hasRemaining()) {
                    throw new IOException("Invalid tombstone body");
                }
                return new Command(parsed.type(), scope, null, null, null);
            }
            if (parsed.type() == SegmentControlFormat.Type.RECOVERY_GRANT) {
                if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT
                        || scope.exactRange() == null || body.remaining() != 32) {
                    throw new IOException("Invalid recovery grant body");
                }
                byte[] intentId = new byte[16];
                body.get(intentId);
                long intentGeneration = body.getLong();
                long grantGeneration = body.getLong();
                if (isZero(intentId) || intentGeneration <= 0
                        || grantGeneration != scope.authorityGeneration()
                        || !Arrays.equals(scope.publicSemanticPayloadIdentity(),
                                recoveryGrantPayloadIdentity(scope, intentId, intentGeneration, grantGeneration))) {
                    throw new IOException("Invalid recovery grant binding");
                }
                return new Command(scope, new GrantBinding(intentId, intentGeneration, grantGeneration));
            }
            if (parsed.type() == SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE) {
                if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE
                        || scope.exactRange() == null || body.remaining() != 48) {
                    throw new IOException("Invalid recovery close body");
                }
                UUID grantOperationId = new UUID(body.getLong(), body.getLong());
                byte[] intentId = new byte[16];
                body.get(intentId);
                long intentGeneration = body.getLong();
                long grantGeneration = body.getLong();
                if (isZero(intentId) || intentGeneration <= 0 || grantGeneration <= 0
                        || !Arrays.equals(scope.publicSemanticPayloadIdentity(),
                                recoveryClosePayloadIdentity(scope, grantOperationId, intentId,
                                        intentGeneration, grantGeneration))) {
                    throw new IOException("Invalid recovery close binding");
                }
                return new Command(scope, new GrantBinding(intentId, intentGeneration, grantGeneration),
                        grantOperationId);
            }
            if (scope.operation() != ProfileControlOperation.INSTALL || body.remaining() < 2) {
                throw new IOException("Invalid install body");
            }
            int descriptorLength = Short.toUnsignedInt(body.getShort());
            if (descriptorLength < ProfileDescriptorCodec.MIN_CANONICAL_LENGTH
                    || descriptorLength > ProfileDescriptorCodec.MAX_CANONICAL_LENGTH
                    || body.remaining() != descriptorLength + ProtectedProfileCredential.SealedStorageValue.LENGTH) {
                throw new IOException("Invalid install descriptor or credential length");
            }
            byte[] descriptor = new byte[descriptorLength];
            body.get(descriptor);
            byte[] encrypted = new byte[ProtectedProfileCredential.SealedStorageValue.LENGTH];
            body.get(encrypted);
            ProfileDescriptor decoded = ProfileDescriptorCodec.decodeAndVerify(descriptor,
                    scope.descriptorIdentity(), descriptorValidator);
            if (decoded.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE) {
                throw new IOException("Unsupported installed engine");
            }
            ProtectedProfileCredential credential = ProtectedProfileCredential.openFromStorage(key,
                    storageBinding(scope), ProtectedProfileCredential.SealedStorageValue.fromBytes(encrypted));
            return new Command(parsed.type(), scope, descriptor, credential,
                    ProtectedProfileCredential.SealedStorageValue.fromBytes(encrypted));
        } catch (BufferUnderflowException | IllegalArgumentException failure) {
            throw new IOException("Invalid Bookie control record", failure);
        }
    }

    private ByteBuffer body(Command command) {
        ByteBuffer scope = encodeScope(command.scope);
        if (command.type == SegmentControlFormat.Type.RECOVERY_GRANT) {
            return ByteBuffer.allocate(scope.remaining() + 32).put(scope).put(command.grant.intentId())
                    .putLong(command.grant.intentGeneration()).putLong(command.grant.grantGeneration()).flip();
        }
        if (command.type == SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE) {
            return ByteBuffer.allocate(scope.remaining() + 48).put(scope)
                    .putLong(command.closedGrantId.getMostSignificantBits())
                    .putLong(command.closedGrantId.getLeastSignificantBits()).put(command.grant.intentId())
                    .putLong(command.grant.intentGeneration()).putLong(command.grant.grantGeneration()).flip();
        }
        if (command.type != SegmentControlFormat.Type.INSTALL) {
            return scope;
        }
        return ByteBuffer.allocate(scope.remaining() + 2 + command.descriptor.length
                        + ProtectedProfileCredential.SealedStorageValue.LENGTH)
                .put(scope).putShort((short) command.descriptor.length).put(command.descriptor)
                .put(command.sealed.toBytes()).flip();
    }

    private static ByteBuffer encodeScope(ProfileControlScope scope) {
        byte[] target = scope.targetBookie().getBytes(StandardCharsets.UTF_8);
        byte[] domain = scope.authorityDomain().getBytes(StandardCharsets.UTF_8);
        int operation = switch (scope.operation()) {
            case INSTALL -> 1;
            case ACTIVATE_INITIAL -> 2;
            case ACTIVATE_REPLACEMENT -> 3;
            case TOMBSTONE_OR_DELETE_APPLY -> 4;
            case RECOVERY_GRANT -> 5;
            case RECOVERY_GRANT_CLOSE -> 6;
            default -> throw new IllegalArgumentException("Unsupported Bookie control scope");
        };
        int rangeBytes = scope.exactRange() == null ? 0 : 16;
        ByteBuffer encoded = ByteBuffer.allocate(4 + 8 + 16 + 36 + 16 + 8 + 32
                        + 2 + target.length + 2 + domain.length + rangeBytes)
                .putInt(operation).putLong(scope.ledgerId()).put(scope.ledgerInstanceId())
                .put(scope.descriptorIdentity().toBytes()).put(scope.targetStorageIncarnation())
                .putLong(scope.authorityGeneration()).put(scope.publicSemanticPayloadIdentity())
                .putShort((short) target.length).put(target).putShort((short) domain.length).put(domain);
        if (scope.exactRange() != null) {
            encoded.putLong(scope.exactRange().firstEntryId()).putLong(scope.exactRange().lastEntryId());
        }
        return encoded.flip();
    }

    private static ProfileControlScope decodeScope(ByteBuffer body, UUID operationId) throws IOException {
        int operation = body.getInt();
        ProfileControlOperation type = switch (operation) {
            case 1 -> ProfileControlOperation.INSTALL;
            case 2 -> ProfileControlOperation.ACTIVATE_INITIAL;
            case 3 -> ProfileControlOperation.ACTIVATE_REPLACEMENT;
            case 4 -> ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY;
            case 5 -> ProfileControlOperation.RECOVERY_GRANT;
            case 6 -> ProfileControlOperation.RECOVERY_GRANT_CLOSE;
            default -> throw new IOException("Unsupported Bookie control operation");
        };
        long ledgerId = body.getLong();
        byte[] instance = new byte[16];
        byte[] descriptor = new byte[36];
        byte[] storage = new byte[16];
        byte[] payload = new byte[32];
        body.get(instance).get(descriptor).get(storage);
        long generation = body.getLong();
        body.get(payload);
        String target = readText(body, ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        String domain = readText(body, ProfileControlScope.MAX_AUTHORITY_DOMAIN_BYTES);
        ProfileExactRange range = type.rangeAllowed()
                ? new ProfileExactRange(body.getLong(), body.getLong()) : null;
        byte[] id = ByteBuffer.allocate(16).putLong(operationId.getMostSignificantBits())
                .putLong(operationId.getLeastSignificantBits()).array();
        return new ProfileControlScope(type, type.purpose(), ledgerId, instance,
                ProfileDescriptorIdentity.fromBytes(descriptor), target, storage,
                domain, generation, id, payload, range);
    }

    private static boolean isZero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return false;
            }
        }
        return true;
    }

    /** Experimental domain-separated commitment to the exact local grant payload. */
    static byte[] recoveryGrantPayloadIdentity(ProfileControlScope scope, byte[] intentId,
                                               long intentGeneration, long grantGeneration) {
        if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT || scope.exactRange() == null
                || intentId.length != 16 || isZero(intentId) || intentGeneration <= 0
                || grantGeneration != scope.authorityGeneration()) {
            throw new IllegalArgumentException("Invalid recovery grant identity input");
        }
        return recoveryBindingIdentity(RECOVERY_GRANT_DOMAIN, scope, null, intentId,
                intentGeneration, grantGeneration);
    }

    static byte[] recoveryClosePayloadIdentity(ProfileControlScope scope, UUID grantOperationId,
                                               byte[] intentId, long intentGeneration, long grantGeneration) {
        if (scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE || scope.exactRange() == null
                || grantOperationId == null || intentId.length != 16 || isZero(intentId)
                || intentGeneration <= 0 || grantGeneration <= 0) {
            throw new IllegalArgumentException("Invalid recovery close identity input");
        }
        return recoveryBindingIdentity(RECOVERY_CLOSE_DOMAIN, scope, grantOperationId, intentId,
                intentGeneration, grantGeneration);
    }

    private static byte[] recoveryBindingIdentity(byte[] prefix, ProfileControlScope scope, UUID grantOperationId,
                                                  byte[] intentId, long intentGeneration, long grantGeneration) {
        byte[] target = scope.targetBookie().getBytes(StandardCharsets.UTF_8);
        byte[] domain = scope.authorityDomain().getBytes(StandardCharsets.UTF_8);
        ByteBuffer input = ByteBuffer.allocate(prefix.length + ProfileLedgerContext.ENCODED_LENGTH
                + 2 + target.length + 16 + 2 + domain.length + (grantOperationId == null ? 0 : 16)
                + 16 + 8 + 8 + 16);
        input.put(prefix);
        input.put(context(scope).encode()).putShort((short) target.length).put(target)
                .put(scope.targetStorageIncarnation()).putShort((short) domain.length).put(domain);
        if (grantOperationId != null) {
            input.putLong(grantOperationId.getMostSignificantBits())
                    .putLong(grantOperationId.getLeastSignificantBits());
        }
        input.put(intentId).putLong(intentGeneration).putLong(grantGeneration)
                .putLong(scope.exactRange().firstEntryId()).putLong(scope.exactRange().lastEntryId());
        try {
            return MessageDigest.getInstance("SHA-256").digest(input.array());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String readText(ByteBuffer body, int maxBytes) throws IOException {
        int length = Short.toUnsignedInt(body.getShort());
        if (length < 1 || length > maxBytes || length > body.remaining()) {
            throw new IOException("Invalid Bookie control text length");
        }
        byte[] bytes = new byte[length];
        body.get(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IOException("Invalid Bookie control UTF-8");
        }
        return value;
    }

    private byte[] storageBinding(ProfileControlScope scope) {
        byte[] bookie = identity.bookieId().toString().getBytes(StandardCharsets.UTF_8);
        byte[] context = context(scope).encode();
        return ByteBuffer.allocate(16 + 16 + 2 + bookie.length + context.length)
                .put(identity.incarnation().value())
                .putLong(identity.arenaId().getMostSignificantBits())
                .putLong(identity.arenaId().getLeastSignificantBits())
                .putShort((short) bookie.length).put(bookie).put(context).array();
    }

    private static ProfileLedgerContext context(ProfileControlScope scope) {
        return new ProfileLedgerContext(scope.ledgerId(), scope.ledgerInstanceId(),
                scope.descriptorIdentity().toBytes());
    }

    private boolean matchesTarget(ProfileControlScope scope) {
        return scope.targetBookie().equals(identity.bookieId().toString())
                && Arrays.equals(scope.targetStorageIncarnation(), identity.incarnation().value());
    }

    private void validateStorageFiles() throws IOException {
        if (Files.isSymbolicLink(config.directory())) {
            throw new IOException("Isolated storage directory cannot be a symlink");
        }
        validateIdentity(config.directory().resolve("superblock-a"), -1, 4096);
        validateIdentity(config.directory().resolve("superblock-b"), -2, 4096);
        validateIdentity(config.directory().resolve("arena-control"), -4, -1);
        for (int file = 0; file < config.dataFiles(); file++) {
            validateIdentity(IsolatedSegmentFiles.dataPath(config, file), file, config.bytesPerFile());
        }
        validateIdentity(config.directory().resolve("bookie-control"), -3, -1);
    }

    private void validateIdentity(Path path, int fileId, long expectedSize) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("Isolated storage file cannot be a symlink");
        }
        try (FileChannel input = FileChannel.open(path, StandardOpenOption.READ)) {
            if ((expectedSize < 0 && input.size() < FIRST_RECORD)
                    || (expectedSize >= 0 && input.size() != expectedSize)) {
                throw new IOException("Isolated storage file size mismatch");
            }
            ByteBuffer header = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(input, header, 0);
            identity.validate(header, config, fileId);
        }
    }

    private void requireWritable() throws IOException {
        if (closed || failed) {
            throw new IOException("Bookie control log is closed or has uncertain I/O");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (writing || activeAdmissions != 0) {
            throw new IOException("Bookie control I/O or admissions are still in flight");
        }
        closed = true;
        IOException first = null;
        try {
            lock.release();
        } catch (IOException failure) {
            first = failure;
        }
        try {
            channel.close();
        } catch (IOException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        if (first != null) {
            throw first;
        }
    }
}

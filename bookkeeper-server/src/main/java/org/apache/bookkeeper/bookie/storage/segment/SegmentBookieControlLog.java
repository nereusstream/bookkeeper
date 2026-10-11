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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
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

    enum NormalRejection {
        UNAVAILABLE, IDENTITY_CONFLICT, UNAUTHORIZED, TOMBSTONED, FENCED, NOT_ACTIVE, INVALID_GRANT
    }

    record Result(Status status, UUID operationId, long sequence, long localGeneration,
                  long localInstallGeneration) { }

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
                                  ProfileOperationCodec.AddRecovery request, boolean snapshotPayload) {
            this.owner = owner;
            this.state = state;
            this.grant = grant;
            this.entryId = request.entryId();
            this.entryPayload = snapshotPayload ? request.entryPayload() : null;
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

    /** No record or admission cut was written for an out-of-order delivery. */
    static final class DeleteSequenceException extends IOException {
        private final boolean retryable;
        DeleteSequenceException(boolean retryable) {
            super("Delete delivery is outside the continuous initial assignment");
            this.retryable = retryable;
        }
        boolean retryable() {
            return retryable;
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
        private final ProfileControlRequest.TerminalBinding terminal;
        private final ProfileControlRequest.DeleteBinding deletion;

        private Command(SegmentControlFormat.Type type, ProfileControlScope scope, byte[] descriptor,
                        ProtectedProfileCredential credential,
                        ProtectedProfileCredential.SealedStorageValue sealed) {
            this.deletion = null;
            this.terminal = null;
            this.type = type;
            this.scope = scope;
            this.descriptor = descriptor == null ? null : descriptor.clone();
            this.credential = credential == null ? null : credential.copy();
            this.sealed = sealed;
            this.grant = null;
            this.closedGrantId = null;
        }

        private Command(ProfileControlScope scope, GrantBinding grant) {
            this.deletion = null;
            this.terminal = null;
            this.type = SegmentControlFormat.Type.RECOVERY_GRANT;
            this.scope = scope;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = grant;
            this.closedGrantId = null;
        }

        private Command(ProfileControlScope scope, GrantBinding grant, UUID closedGrantId) {
            this(scope, grant, closedGrantId, false);
        }

        private Command(ProfileControlScope scope, GrantBinding grant, UUID closedGrantId, boolean terminal) {
            this.deletion = null;
            this.terminal = null;
            this.type = terminal ? SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE
                    : SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE;
            this.scope = scope;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = grant;
            this.closedGrantId = closedGrantId;
        }

        private Command(ProfileControlScope scope, ProfileControlRequest.TerminalBinding terminal) {
            this.type = SegmentControlFormat.Type.RECOVERY_TERMINAL_BOUNDARY;
            this.scope = scope;
            this.deletion = null;
            this.terminal = terminal;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = null;
            this.closedGrantId = null;
        }

        private Command(ProfileControlScope scope, ProfileControlRequest.DeleteBinding deletion) {
            this.type = SegmentControlFormat.Type.DELETE_DELIVERY_APPLY;
            this.scope = scope;
            this.deletion = deletion;
            this.terminal = null;
            this.descriptor = null;
            this.credential = null;
            this.sealed = null;
            this.grant = null;
            this.closedGrantId = null;
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
                    && Objects.equals(closedGrantId, other.closedGrantId) && Objects.equals(terminal, other.terminal)
                    && Objects.equals(deletion, other.deletion);
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
        private final long localInstallGeneration;
        private long activationGeneration;
        private boolean active;
        private boolean fenced;
        private boolean pendingTombstone;
        private UUID pendingFence;
        private ExplicitLac explicitLac;
        private Long terminalPrefix;
        private Long pendingTerminalPrefix;
        private final Map<UUID, GrantState> recoveryGrants = new HashMap<>();
        private int inFlight;

        private LedgerState(Command command) {
            localInstallGeneration = localGeneration;
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
    private FileChannel channel;
    private final FileChannel originalChannel;
    private final SegmentDataFileLock writerLock;
    private SegmentControlRotation.Root rotationRoot;
    private Thread rotating;
    private Generation physicalGeneration = LOG_GENERATION;
    private final Map<Long, LedgerState> ledgers = new HashMap<>();
    private final Map<Long, ProfileLedgerContext> tombstones = new HashMap<>();
    private ProfileControlRequest.DeleteBinding deleteApplied;
    private final Map<UUID, AppliedOperation> operations = new HashMap<>();
    private final Map<UUID, ProfileLedgerContext> fenceOperations = new HashMap<>();
    // First applied outcomes plus the latest LAC per ledger, bounded by the retained control log.
    private final Map<UUID, SegmentControlCheckpoint.Operation> checkpointState = new HashMap<>();
    private final Map<Long, SegmentControlCheckpoint.Operation> checkpointLacs = new HashMap<>();
    private final HashSet<UUID> checkpointIds = new HashSet<>();
    private long nextOffset = FIRST_RECORD;
    private boolean rotationRequested;
    private long durableThrough = -1;
    private long appliedThrough = -1;
    private long appliedOffset = FIRST_RECORD;
    private long checkpointGeneration;
    private SegmentControlCheckpoint.Reference publishedCheckpoint;
    private boolean checkpointing;
    private DurableBatch pending;
    private FenceWrite pendingFenceWrite;
    private LacWrite pendingLacWrite;
    private RecoveryCloseWrite pendingRecoveryCloseWrite;
    private boolean writing;
    private boolean failed;
    private boolean closed;
    private int activeAdmissions;
    private ReclamationLease reclamation;

    /** Volatile admission exclusion only; terminal facts still come from applied durable control records. */
    final class ReclamationLease implements AutoCloseable {
        private final long cut;
        private final Set<ProfileLedgerContext> terminalContexts;
        private boolean released;

        private ReclamationLease(long cut) {
            this(cut, null);
        }

        private ReclamationLease(long cut, Set<ProfileLedgerContext> terminalContexts) {
            this.cut = cut;
            this.terminalContexts = terminalContexts;
        }

        long cut() {
            return cut;
        }

        /** Volatile snapshot of already durable terminal facts, never another deletion authority. */
        boolean terminalAtCut(ProfileLedgerContext context) throws IOException {
            synchronized (SegmentBookieControlLog.this) {
                if (released || reclamation != this || closed || failed || terminalContexts == null) {
                    throw new IOException("Terminal cleanup has no held Bookie authority snapshot");
                }
                return terminalContexts.contains(context) && context.equals(tombstones.get(context.ledgerId()));
            }
        }

        @Override
        public void close() {
            synchronized (SegmentBookieControlLog.this) {
                if (!released) {
                    released = true;
                    reclamation = null;
                }
            }
        }
    }

    private SegmentBookieControlLog(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                    long maxLogBytes, SegmentFileIo io, SecretKey key,
                                    ProfileDescriptorValidator descriptorValidator,
                                    FileChannel channel, SegmentDataFileLock writerLock) {
        this.config = config;
        this.identity = identity;
        this.maxLogBytes = maxLogBytes;
        this.io = io;
        this.key = key;
        this.descriptorValidator = descriptorValidator;
        this.channel = channel;
        this.originalChannel = channel;
        this.writerLock = writerLock;
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
        SegmentDataFileLock writerLock = SegmentDataFileLock.openControl(
                config.directory().resolve("bookie-control.lock"));
        FileChannel original = null;
        SegmentBookieControlLog result = null;
        try {
            original = FileChannel.open(config.directory().resolve("bookie-control"),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            result = new SegmentBookieControlLog(config, identity, maxLogBytes, io, key, descriptorValidator,
                    original, writerLock);
            result.validateStorageFiles();
            result.recover();
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            if (result != null && result.channel != original) {
                try {
                    result.channel.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            if (original != null) {
                try {
                    original.close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            try {
                writerLock.close();
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
        return prepareClose(command(close));
    }

    synchronized RecoveryCloseAttempt prepareTerminalClose(ProtectedProfileStateStore.Transition transition)
            throws IOException {
        if (transition.request().scope().operation() != ProfileControlOperation.RECOVERY_TERMINAL_CLOSE) {
            throw new IllegalArgumentException("Expected a standalone terminal close");
        }
        return prepareClose(command(transition));
    }

    private RecoveryCloseAttempt prepareClose(Command command) throws IOException {
        requireWritable();
        AppliedOperation prior = operations.get(command.operationId());
        if (prior != null && !terminalUpgrade(prior.command(), command)) {
            return new RecoveryCloseAttempt(prior.command().sameMeaning(command)
                    ? closeStatus(prior.status() == Status.APPLIED_DURABLE
                            ? Status.ALREADY_DURABLE : prior.status())
                    : RecoveryCloseStatus.INCOMPATIBLE, null);
        }
        if (fenceOperations.containsKey(command.operationId()) || checkpointIds.contains(command.operationId())) {
            return new RecoveryCloseAttempt(RecoveryCloseStatus.INCOMPATIBLE, null);
        }
        LedgerState state = ledgers.get(command.scope.ledgerId());
        GrantState grant = state == null ? null : state.recoveryGrants.get(command.closedGrantId);
        if ((command.type == SegmentControlFormat.Type.RECOVERY_TERMINAL_BOUNDARY
                || command.type == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE
                    && grant != null && matchesClose(command, grant.command)) && state != null
                && state.context.equals(context(command.scope))) {
            long prefix = command.terminal != null ? command.terminal.prefixEntryId()
                    : command.scope.exactRange().lastEntryId();
            if (state.terminalPrefix != null && state.terminalPrefix != prefix
                    || state.pendingTerminalPrefix != null && state.pendingTerminalPrefix != prefix) {
                return new RecoveryCloseAttempt(RecoveryCloseStatus.INCOMPATIBLE, null);
            }
            state.pendingTerminalPrefix = prefix;
            if (state.inFlight != 0
                    || state.recoveryGrants.values().stream().anyMatch(active -> active.inFlight != 0)) {
                return new RecoveryCloseAttempt(RecoveryCloseStatus.WAITING_PRECUT, null);
            }
        }
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
        requireCapacity(record.remaining());
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
            io.writeFully(channel, physicalRecord(write.record), write.offset);
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
            retain(write.sequence, write.command.operationId(), status, write.record);
            appliedOffset = write.offset + write.record.remaining();
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
            if (commands.stream().anyMatch(command -> command.deletion != null) && commands.size() != 1) {
                throw new IllegalArgumentException("One continuous delete effect per control batch");
            }
            for (Command command : commands) {
                verifyDeleteSequence(command);
                if (command.terminal != null) {
                    throw new IllegalArgumentException("Terminal boundary requires its pre-cut close path");
                }
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
            requireCapacity(bytes);
            start = nextOffset;
            nextOffset += bytes;
            for (Command command : commands) {
                if (isTombstone(command.type)
                        && !operations.containsKey(command.operationId())
                        && !fenceOperations.containsKey(command.operationId())
                        && !checkpointIds.contains(command.operationId())) {
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
                io.writeFully(channel, physicalRecord(record), position);
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
                ByteBuffer record = encode(command, sequence);
                retain(sequence, command.operationId(), status, record);
                appliedOffset += record.remaining();
                LedgerState state = ledgers.get(command.scope.ledgerId());
                results.add(new Result(status, command.operationId(), sequence,
                        command.deletion != null
                                && (status == Status.APPLIED_DURABLE || status == Status.ALREADY_DURABLE)
                                ? command.deletion.sequence() : state == null ? 0 : state.localGeneration,
                        state == null ? 0 : state.localInstallGeneration));
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

    SegmentControlCheckpoint.Reference checkpoint() throws IOException {
        return checkpoint(new SegmentFileIo());
    }

    SegmentControlCheckpoint.Reference checkpoint(SegmentFileIo checkpointIo) throws IOException {
        Objects.requireNonNull(checkpointIo, "checkpointIo");
        SegmentControlCheckpoint.Snapshot snapshot;
        long generation;
        int slot;
        synchronized (this) {
            requireWritable();
            if (checkpointing || checkpointGeneration == Long.MAX_VALUE) {
                throw new IOException("Bookie checkpoint is active or its generation is exhausted");
            }
            Set<UUID> retainedCheckpoints = retainedCheckpointIds();
            List<SegmentControlCheckpoint.Operation> state = new ArrayList<>();
            checkpointState.forEach((id, operation) -> {
                if (!checkpointIds.contains(id) || retainedCheckpoints.contains(id)) {
                    state.add(operation);
                }
            });
            state.addAll(checkpointLacs.values());
            snapshot = new SegmentControlCheckpoint.Snapshot(appliedThrough, appliedOffset, state, true);
            generation = checkpointGeneration + 1;
            slot = publishedCheckpoint == null ? 0 : 1 - publishedCheckpoint.slot();
            checkpointing = true;
        }
        boolean committed = false;
        try {
            snapshot = new SegmentControlCheckpoint.Snapshot(snapshot.through(), snapshot.logOffset(),
                    snapshot.operations().stream().sorted(Comparator.comparingLong(
                            SegmentControlCheckpoint.Operation::sequence)).toList(),
                    snapshot.compactedCheckpointHistory());
            SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.reference(
                    generation, slot, snapshot, maxLogBytes);
            SegmentControlCheckpoint.write(config, identity, SegmentControlFormat.Owner.BOOKIE,
                    reference, snapshot, checkpointIo);
            if (!snapshot.equals(SegmentControlCheckpoint.read(config, identity, SegmentControlFormat.Owner.BOOKIE,
                    reference, maxLogBytes))) {
                throw new IOException("Bookie checkpoint differs from its frozen applied state");
            }
            commitCheckpoint(reference);
            committed = true;
            if (rotationRoot == null) {
                SegmentControlCheckpoint.publish(config, SegmentControlFormat.Owner.BOOKIE, reference, checkpointIo);
            } else {
                SegmentControlRotation.publish(config, SegmentControlFormat.Owner.BOOKIE, reference,
                        rotationRoot, checkpointIo);
            }
            if (!reference.equals(SegmentControlCheckpoint.publication(
                    config, SegmentControlFormat.Owner.BOOKIE, slot))) {
                throw new IOException("Bookie checkpoint publication verification failed");
            }
            synchronized (this) {
                publishedCheckpoint = reference;
            }
            return reference;
        } catch (IOException | RuntimeException | Error failure) {
            if (committed) {
                synchronized (this) {
                    failed = true;
                }
            }
            throw failure;
        } finally {
            synchronized (this) {
                checkpointing = false;
            }
        }
    }

    synchronized Optional<SegmentControlCheckpoint.Reference> publishedCheckpoint() {
        return Optional.ofNullable(publishedCheckpoint);
    }

    private void commitCheckpoint(SegmentControlCheckpoint.Reference reference) throws IOException {
        ByteBuffer record;
        long sequence;
        long offset;
        UUID operationId = UUID.randomUUID();
        synchronized (this) {
            requireWritable();
            if (writing || pending != null || pendingFenceWrite != null || pendingLacWrite != null
                    || pendingRecoveryCloseWrite != null || durableThrough != appliedThrough) {
                throw new IOException("Bookie control operation is unresolved at checkpoint commit");
            }
            if (operations.containsKey(operationId) || fenceOperations.containsKey(operationId)
                    || checkpointIds.contains(operationId)) {
                throw new IOException("Bookie checkpoint operation identity is already used");
            }
            sequence = durableThrough + 1;
            record = SegmentControlFormat.encode(SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT,
                    LOG_GENERATION, sequence, operationId, new Generation(reference.generation()),
                    reference.generation() - 1, reference.encode());
            requireCapacity(record.remaining());
            offset = nextOffset;
            nextOffset += record.remaining();
            writing = true;
        }
        try {
            io.writeFully(channel, physicalRecord(record), offset);
            io.force(channel, true);
            synchronized (this) {
                applyCheckpoint(reference, operationId, sequence);
                retain(sequence, operationId, Status.APPLIED_DURABLE, record);
                durableThrough = sequence;
                appliedThrough = sequence;
                appliedOffset = offset + record.remaining();
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

    /** Offline maintenance: two cuts differ only by the first checkpoint's own commit. */
    SegmentControlRotation.Root rotate() throws IOException {
        return rotate(new SegmentFileIo());
    }

    SegmentControlRotation.Root rotate(SegmentFileIo rotationIo) throws IOException {
        Objects.requireNonNull(rotationIo, "rotationIo");
        synchronized (this) {
            requireWritable();
            if (rotating != null || checkpointing || writing || pending != null || activeAdmissions != 0
                    || pendingFenceWrite != null || pendingLacWrite != null || pendingRecoveryCloseWrite != null
                    || checkpointGeneration > Long.MAX_VALUE - 2
                    || physicalGeneration.value() == Long.MAX_VALUE || appliedThrough > Long.MAX_VALUE - 3
                    || rotationReserveBytes() == 0
                    || nextOffset > maxLogBytes - 416) {
                throw new IOException("Bookie rotation requires a quiescent writer and two commit slots");
            }
            rotating = Thread.currentThread();
        }
        FileChannel candidate = null;
        Path target = null;
        boolean created = false;
        boolean publicationAttempted = false;
        try {
            SegmentControlCheckpoint.Reference first = checkpoint(rotationIo);
            SegmentControlCheckpoint.Reference second = checkpoint(rotationIo);
            SegmentControlRotation.Root root = new SegmentControlRotation.Root(physicalGeneration.value() + 1,
                    first, checkpointId(first), second, checkpointId(second));
            target = SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE, root.generation());
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                // Both fresh publications recover the current journal. Delete only a proven older/orphan file.
                try (FileChannel orphan = FileChannel.open(target, StandardOpenOption.READ,
                        LinkOption.NOFOLLOW_LINKS)) {
                    ByteBuffer header = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(orphan, header, 0);
                    identity.validate(header, config, -3);
                    SegmentControlRotation.Root old = SegmentControlRotation.readHeader(orphan,
                            SegmentControlFormat.Owner.BOOKIE);
                    if (old.generation() == physicalGeneration.value()
                            || old.generation() > root.generation() || !knownRotation(old)) {
                        throw new IOException("Bookie rotation target has unproven authority");
                    }
                }
                Files.delete(target);
                IsolatedSegmentFiles.syncDirectory(config.directory());
            }
            candidate = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            created = true;
            rotationIo.writeFully(candidate, identity.encode(config, -3), 0);
            rotationIo.writeFully(candidate, SegmentControlRotation.header(SegmentControlFormat.Owner.BOOKIE, root),
                    4096);
            rotationIo.force(candidate, true);
            IsolatedSegmentFiles.syncDirectory(config.directory());
            if (!root.equals(SegmentControlRotation.readHeader(candidate, SegmentControlFormat.Owner.BOOKIE))) {
                throw new IOException("Bookie rotation candidate verification failed");
            }
            publicationAttempted = true;
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.BOOKIE, first, root, rotationIo);
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.BOOKIE, second, root, rotationIo);
            // The independent lock remains held. This marker prevents empty-log and old-decoder fallback.
            rotationIo.writeFully(originalChannel,
                    SegmentControlRotation.header(SegmentControlFormat.Owner.BOOKIE, root),
                    4096);
            originalChannel.truncate(SegmentControlRotation.FIRST_RECORD);
            rotationIo.force(originalChannel, true);
            FileChannel previous = channel;
            long previousGeneration = physicalGeneration.value();
            synchronized (this) {
                channel = candidate;
                candidate = null;
                rotationRoot = root;
                physicalGeneration = new Generation(root.generation());
                nextOffset = SegmentControlRotation.FIRST_RECORD;
                rotationRequested = false;
                appliedOffset = nextOffset;
            }
            if (previous != originalChannel) {
                previous.close();
                Files.delete(SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE,
                        previousGeneration));
                IsolatedSegmentFiles.syncDirectory(config.directory());
            }
            synchronized (this) {
                Set<UUID> retained = retainedCheckpointIds();
                checkpointState.keySet().removeIf(id -> checkpointIds.contains(id) && !retained.contains(id));
                checkpointIds.retainAll(retained);
            }
            return root;
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                failed = true;
            }
            if (created && !publicationAttempted) {
                try {
                    candidate.close();
                    candidate = null;
                    Files.delete(target);
                    IsolatedSegmentFiles.syncDirectory(config.directory());
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        } finally {
            try {
                if (candidate != null) {
                    candidate.close();
                }
            } finally {
                synchronized (this) {
                    rotating = null;
                }
            }
        }
    }

    /** Retain the active physical roots and latest predecessors; business results remain unchanged. */
    private Set<UUID> retainedCheckpointIds() {
        Set<UUID> retained = new HashSet<>();
        checkpointState.entrySet().stream().filter(entry -> checkpointIds.contains(entry.getKey()))
                .sorted(Comparator.comparingLong((Map.Entry<UUID, SegmentControlCheckpoint.Operation> entry) ->
                        entry.getValue().sequence()).reversed()).limit(2)
                .forEach(entry -> retained.add(entry.getKey()));
        if (rotationRoot != null) {
            retained.add(rotationRoot.firstId());
            retained.add(rotationRoot.secondId());
        }
        return retained;
    }

    private synchronized UUID checkpointId(SegmentControlCheckpoint.Reference reference) throws IOException {
        for (Map.Entry<UUID, SegmentControlCheckpoint.Operation> entry : checkpointState.entrySet()) {
            SegmentControlParser.Command parsed = parse(entry.getValue().record(), entry.getValue().sequence());
            if (parsed.type() == SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT
                    && reference.equals(decodeCheckpoint(parsed, entry.getValue().sequence()))) {
                return entry.getKey();
            }
        }
        throw new IOException("Bookie checkpoint commit is missing");
    }

    private boolean knownRotation(SegmentControlRotation.Root root) throws IOException {
        return root.firstId().equals(checkpointId(root.first())) && root.secondId().equals(checkpointId(root.second()));
    }

    private void requireCapacity(long bytes) throws IOException {
        long reserve = rotationReserveBytes();
        if (bytes > maxLogBytes - nextOffset - reserve) {
            rotationRequested |= reserve != 0
                    && bytes <= maxLogBytes - SegmentControlRotation.FIRST_RECORD - reserve;
            throw new SegmentControlCapacityException("Bookie control log capacity exhausted");
        }
    }

    synchronized boolean rotationRequired() {
        return rotationRequested && !closed && !failed;
    }

    /** A precheck only; driver exclusion and actual admission protection are still required. */
    synchronized boolean quiescentForRotation() {
        return !closed && !failed && !writing && !checkpointing && pending == null && activeAdmissions == 0
                && rotating == null && reclamation == null && pendingFenceWrite == null
                && pendingLacWrite == null && pendingRecoveryCloseWrite == null;
    }

    private long rotationReserveBytes() {
        return rotating == null && maxLogBytes >= SegmentControlRotation.FIRST_RECORD + 416 + 2048 ? 416 : 0;
    }

    private ByteBuffer encode(Command command, long sequence) {
        return SegmentControlFormat.encode(command.type, LOG_GENERATION, sequence, command.operationId(),
                new Generation(command.scope.authorityGeneration()), 0, body(command));
    }

    private void retain(long sequence, UUID operationId, Status status, ByteBuffer record) {
        var snapshot = new SegmentControlCheckpoint.Operation(sequence, status.ordinal() + 1, record);
        if (record.getInt(16) == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE.code
                && status == Status.APPLIED_DURABLE) {
            checkpointState.put(operationId, snapshot);
        } else {
            checkpointState.putIfAbsent(operationId, snapshot);
        }
    }

    /** Diagnostic predicate only; a DATA request must hold an {@link Admission} through physical termination. */
    synchronized boolean normalAdmission(ProfileLedgerContext context, byte[] wireVerifier) {
        LedgerState state = ledgers.get(context.ledgerId());
        return !closed && !failed && state != null && state.active && !state.fenced
                && !state.pendingTombstone && !tombstones.containsKey(context.ledgerId())
                && state.pendingFence == null && state.pendingTerminalPrefix == null && state.context.equals(context)
                && state.credential.matchesWireVerifier(wireVerifier);
    }

    /** Current initial activation completion only; it is a snapshot, never a new DATA admission. */
    synchronized long initialActiveGeneration(ProfileControlScope scope) {
        LedgerState state = ledgers.get(scope.ledgerId());
        return !closed && !failed && state != null && state.active && !state.fenced
                && !state.pendingTombstone && !tombstones.containsKey(scope.ledgerId()) && state.pendingFence == null
                && state.pendingTerminalPrefix == null && state.context.equals(context(scope))
                && state.activationGeneration == scope.authorityGeneration()
                ? state.localGeneration : 0;
    }

    /** Matching durable close result only; this snapshot neither grants DATA nor proves cluster membership. */
    synchronized long recoveryClosedGeneration(ProfileControlScope scope) {
        ByteBuffer id = ByteBuffer.wrap(scope.operationId());
        AppliedOperation applied = operations.get(new UUID(id.getLong(), id.getLong()));
        LedgerState state = ledgers.get(scope.ledgerId());
        if (closed || failed || state == null || state.pendingTombstone || tombstones.containsKey(scope.ledgerId())
                || !state.context.equals(context(scope)) || applied == null
                || applied.status() != Status.APPLIED_DURABLE
                || !isRecoveryClose(applied.command().type)
                || !applied.command().scope.equals(scope)) {
            return 0;
        }
        if (applied.command().terminal != null) {
            return Objects.equals(state.terminalPrefix, applied.command().terminal.prefixEntryId())
                    ? state.localGeneration : 0;
        }
        GrantState grant = state.recoveryGrants.get(applied.command().closedGrantId);
        return grant != null && grant.committedReadable ? state.localGeneration : 0;
    }

    /** Explains a refused, unsubmitted request; this snapshot never grants DATA admission. */
    synchronized NormalRejection normalRejection(ProfileLedgerContext context, byte[] wireVerifier) {
        NormalRejection rejection = fenceRejection(context, wireVerifier);
        if (rejection != null) {
            return rejection;
        }
        LedgerState state = ledgers.get(context.ledgerId());
        if (state.fenced || state.pendingFence != null || state.pendingTerminalPrefix != null) {
            return NormalRejection.FENCED;
        }
        return state.active ? null : NormalRejection.NOT_ACTIVE;
    }

    /** Fence uses the exact installed data credential even after normal admission has closed. */
    synchronized NormalRejection fenceRejection(ProfileLedgerContext context, byte[] wireVerifier) {
        if (closed || failed) {
            return NormalRejection.UNAVAILABLE;
        }
        LedgerState state = ledgers.get(context.ledgerId());
        ProfileLedgerContext deleted = tombstones.get(context.ledgerId());
        if (state != null && !state.context.equals(context) || deleted != null && !deleted.equals(context)) {
            return NormalRejection.IDENTITY_CONFLICT;
        }
        if (state == null) {
            return deleted != null ? NormalRejection.TOMBSTONED : NormalRejection.NOT_ACTIVE;
        }
        if (!state.credential.matchesWireVerifier(wireVerifier)) {
            return NormalRejection.UNAUTHORIZED;
        }
        if (state.pendingTombstone || deleted != null) {
            return NormalRejection.TOMBSTONED;
        }
        return null;
    }

    synchronized Optional<Admission> admitNormal(ProfileLedgerContext context, byte[] wireVerifier) {
        if (reclamation != null || rotating != null || !normalAdmission(context, wireVerifier)
                || activeAdmissions == config.maxPendingEntries()) {
            return Optional.empty();
        }
        LedgerState state = ledgers.get(context.ledgerId());
        state.inFlight++;
        activeAdmissions++;
        return Optional.of(new Admission(this, state));
    }

    synchronized Optional<RecoveryAdmission> admitRecovery(ProfileOperationCodec.AddRecovery request) {
        return admitRecovery(request, true);
    }

    /** The socket adapter supplies a separately charged source input, without a reference-codec payload copy. */
    synchronized Optional<RecoveryAdmission> admitRecoveryTuple(ProfileOperationCodec.AddRecovery request) {
        return admitRecovery(request, false);
    }

    synchronized NormalRejection recoveryRejection(ProfileOperationCodec.AddRecovery request) {
        NormalRejection rejection = fenceRejection(request.context(), request.credential());
        if (rejection != null) {
            return rejection;
        }
        return matchingRecoveryGrant(ledgers.get(request.context().ledgerId()), request) != null
                ? null : NormalRejection.INVALID_GRANT;
    }

    private Optional<RecoveryAdmission> admitRecovery(ProfileOperationCodec.AddRecovery request,
                                                      boolean snapshotPayload) {
        Objects.requireNonNull(request, "request");
        ProfileLedgerContext context = request.context();
        LedgerState state = ledgers.get(context.ledgerId());
        if (closed || failed || reclamation != null || rotating != null || state == null
                || !state.context.equals(context)
                || state.pendingTombstone || tombstones.containsKey(context.ledgerId())
                || !state.credential.matchesWireVerifier(request.credential())
                || activeAdmissions == config.maxPendingEntries()) {
            return Optional.empty();
        }
        GrantState grant = matchingRecoveryGrant(state, request);
        if (grant == null) {
            return Optional.empty();
        }
        RecoveryAdmission admission = new RecoveryAdmission(this, state, grant, request, snapshotPayload);
        activeAdmissions++;
        grant.inFlight++;
        return Optional.of(admission);
    }

    private GrantState matchingRecoveryGrant(LedgerState state, ProfileOperationCodec.AddRecovery request) {
        if (state.pendingTerminalPrefix != null || state.terminalPrefix != null) {
            return null;
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
                return grant;
            }
        }
        return null;
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
                || state.pendingTombstone || tombstones.containsKey(context.ledgerId())
                || entryId > readUpperBound(state)) {
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
        if (reclamation != null || rotating != null || !locallyReadableContext(context)
                || activeAdmissions == config.maxPendingEntries()) {
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

    synchronized boolean locallyReadableCoordinate(ProfileLedgerContext context, long entryId) {
        return locallyReadableContext(context) && (ledgers.get(context.ledgerId()).terminalPrefix == null
                || entryId <= ledgers.get(context.ledgerId()).terminalPrefix);
    }

    /** Ordinary network reads cannot expose an uncommitted recovery range. */
    synchronized boolean networkReadable(ProfileLedgerContext context, long entryId) {
        if (!locallyReadableContext(context)) {
            return false;
        }
        if (entryId >= 0 && entryId > readUpperBound(ledgers.get(context.ledgerId()))) {
            return false;
        }
        for (GrantState grant : ledgers.get(context.ledgerId()).recoveryGrants.values()) {
            ProfileExactRange range = grant.command.scope.exactRange();
            if ((entryId < 0 || entryId >= range.firstEntryId() && entryId <= range.lastEntryId())
                    && !grant.committedReadable) {
                return false;
            }
        }
        return true;
    }

    private static long readUpperBound(LedgerState state) {
        return state.pendingTerminalPrefix != null ? state.pendingTerminalPrefix
                : state.terminalPrefix != null ? state.terminalPrefix : Long.MAX_VALUE;
    }

    synchronized long readUpperBound(ReadAdmission admission) {
        return readUpperBound(admission.state);
    }

    synchronized boolean canCompleteRead(ReadAdmission admission, long entryId) {
        return canCompleteRead(admission) && (entryId < 0 || entryId <= readUpperBound(admission.state));
    }

    /** A local absence sample is stable only after durable fence and with no other admitted work. */
    synchronized boolean canProveReadAbsence(ReadAdmission admission, long entryId) {
        return canProveReadAbsence(admission, entryId, false);
    }

    synchronized boolean canProveLacDataAbsence(ReadAdmission admission, long entryId) {
        return canProveReadAbsence(admission, entryId, true);
    }

    private boolean canProveReadAbsence(ReadAdmission admission, long entryId, boolean explicitComponent) {
        if (!canCompleteRead(admission, entryId) || !admission.state.fenced || admission.state.pendingFence != null
                || activeAdmissions != 1
                || entryId < 0 && admission.state.explicitLac != null && !explicitComponent) {
            return false;
        }
        for (GrantState grant : admission.state.recoveryGrants.values()) {
            ProfileExactRange range = grant.command.scope.exactRange();
            if ((entryId < 0 || entryId >= range.firstEntryId() && entryId <= range.lastEntryId())
                    && !grant.committedReadable) {
                return false;
            }
        }
        return true;
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

    /** A diagnostic change counter; actual reclamation still requires its existing protected lease. */
    synchronized long appliedThrough() {
        return appliedThrough;
    }

    /** Capture or recheck a replayed local read cut without holding the Bookie monitor during DATA I/O. */
    synchronized long recoveryReadCut(IsolatedSegmentConfig source, SegmentFileHeader sourceIdentity)
            throws IOException {
        if (!matchesScope(source, sourceIdentity) || writing || pending != null || rotating != null
                || activeAdmissions != 0) {
            throw new IOException("Bookie recovery read cut has unresolved control or entry obligations");
        }
        return appliedThrough;
    }

    /** Inactive, fenced, absent or unreadable is not a deletion proof. Only this exact terminal context is dead. */
    synchronized boolean hasTerminalTombstone(ProfileLedgerContext context) {
        return !closed && !failed && context.equals(tombstones.get(context.ledgerId()));
    }

    /** A durable suffix cut excludes only coordinates above P; pending cuts are never FREE authority. */
    synchronized boolean hasTerminalExclusion(ProfileLedgerContext context, long entryId) {
        LedgerState state = ledgers.get(context.ledgerId());
        return !closed && !failed && (hasTerminalTombstone(context)
                || state != null && state.context.equals(context)
                    && state.terminalPrefix != null && entryId > state.terminalPrefix);
    }

    synchronized ReclamationLease protectReclamation(IsolatedSegmentConfig source, SegmentFileHeader sourceIdentity)
            throws IOException {
        if (reclamation != null) {
            throw new IOException("Another whole-allocation reclamation holds Bookie entry admission");
        }
        reclamation = new ReclamationLease(recoveryReadCut(source, sourceIdentity));
        return reclamation;
    }

    synchronized ReclamationLease protectTerminalCleanup(IsolatedSegmentConfig source,
                                                          SegmentFileHeader sourceIdentity) throws IOException {
        if (reclamation != null) {
            throw new IOException("Another reclamation holds Bookie entry admission");
        }
        long cut = recoveryReadCut(source, sourceIdentity);
        reclamation = new ReclamationLease(cut, Set.copyOf(tombstones.values()));
        return reclamation;
    }

    /** Bind a derived checkpoint to applied authority; unlike recovery, this grants no quiescent read cut. */
    synchronized long indexAppliedCut(IsolatedSegmentConfig source, SegmentFileHeader sourceIdentity)
            throws IOException {
        if (!matchesScope(source, sourceIdentity)) {
            throw new IOException("Derived checkpoint has no current Bookie storage identity");
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
        requireCapacity(record.remaining());
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
            io.writeFully(channel, physicalRecord(write.record), write.offset);
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
            checkpointLacs.put(write.context.ledgerId(),
                    new SegmentControlCheckpoint.Operation(write.sequence, 1, write.record));
            appliedOffset = write.offset + write.record.remaining();
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
        if (operations.containsKey(operationId) || checkpointIds.contains(operationId)) {
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
        requireCapacity(record.remaining());
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
            io.writeFully(channel, physicalRecord(write.record), write.offset);
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
            retain(write.sequence, write.operationId, Status.APPLIED_DURABLE, write.record);
            appliedOffset = write.offset + write.record.remaining();
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
                || operations.containsKey(operationId) || fenceOperations.containsKey(operationId)
                || checkpointIds.contains(operationId)) {
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
        if (scope.operation() == ProfileControlOperation.RECOVERY_TERMINAL_CLOSE) {
            var terminal = transition.request().terminalBinding().orElseThrow();
            if (scope.exactRange() != null || !Arrays.equals(scope.publicSemanticPayloadIdentity(),
                    ProfileControlRequest.terminalPayloadIdentity(scope, terminal))) {
                throw new IllegalArgumentException("Terminal close boundary binding mismatch");
            }
            return new Command(scope, terminal);
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
            if (transition.request().deleteBinding().isPresent()) {
                var deletion = transition.request().deleteBinding().orElseThrow();
                ProfileControlRequest.deleteApply(scope, deletion);
                return new Command(scope, deletion);
            }
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
                close.repairIntentGeneration(), close.grantGeneration()), close.grantOperationId(),
                authority.state() == CommittedProfileAuthority.State.RECOVERY_TERMINAL_CLOSE_AUTHORIZED);
    }

    private Status apply(Command command) {
        UUID operationId = command.operationId();
        LedgerState state = ledgers.get(command.scope.ledgerId());
        if (isTombstone(command.type) && state != null
                && state.context.equals(context(command.scope))) {
            state.pendingTombstone = false;
        }
        if (fenceOperations.containsKey(operationId) || checkpointIds.contains(operationId)) {
            return Status.INCOMPATIBLE;
        }
        AppliedOperation prior = operations.get(operationId);
        if (prior != null && !terminalUpgrade(prior.command(), command)) {
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
        } else if (isTombstone(command.type)) {
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
                    || state.terminalPrefix != null
                    || command.scope.authorityGeneration() < state.install.authorityGeneration()
                    || state.recoveryGrants.size() >= config.maxPendingEntries()) {
                status = Status.CONDITION_FAILED;
            } else {
                state.recoveryGrants.put(operationId, new GrantState(command));
                status = Status.APPLIED_DURABLE;
            }
        } else if (command.type == SegmentControlFormat.Type.RECOVERY_TERMINAL_BOUNDARY) {
            if (tombstones.containsKey(command.scope.ledgerId()) || state == null
                    || !state.context.equals(context(command.scope)) || state.pendingTombstone
                    || state.terminalPrefix != null && state.terminalPrefix != command.terminal.prefixEntryId()) {
                status = Status.CONDITION_FAILED;
            } else {
                if (state.inFlight != 0
                        || state.recoveryGrants.values().stream().anyMatch(active -> active.inFlight != 0)) {
                    throw new IllegalStateException("Terminal boundary cannot cross unresolved DATA");
                }
                state.terminalPrefix = command.terminal.prefixEntryId();
                state.pendingTerminalPrefix = null;
                state.active = false;
                state.fenced = true;
                status = Status.APPLIED_DURABLE;
            }
        } else if (isRecoveryClose(command.type)) {
            GrantState grant = state == null ? null : state.recoveryGrants.get(command.closedGrantId);
            if (tombstones.containsKey(command.scope.ledgerId()) || state == null
                    || !state.context.equals(context(command.scope)) || state.pendingTombstone
                    || grant == null || grant.committedReadable && command.type
                        != SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE || grant.inFlight != 0
                    || (grant.pendingClose != null && !grant.pendingClose.equals(operationId))
                    || !matchesClose(command, grant.command)) {
                status = Status.CONDITION_FAILED;
            } else {
                grant.pendingClose = operationId;
                grant.committedReadable = true;
                if (command.type == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE) {
                    long prefix = command.scope.exactRange().lastEntryId();
                    if (state.inFlight != 0
                            || state.recoveryGrants.values().stream().anyMatch(active -> active.inFlight != 0)
                            || state.terminalPrefix != null && state.terminalPrefix != prefix) {
                        throw new IllegalStateException("Terminal close cannot cross normal I/O or another prefix");
                    }
                    state.terminalPrefix = prefix;
                    state.pendingTerminalPrefix = null;
                    state.active = false;
                    state.fenced = true;
                }
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
        if (command.deletion != null && (status == Status.APPLIED_DURABLE || status == Status.ALREADY_DURABLE)) {
            deleteApplied = command.deletion;
        }
        operations.put(operationId, new AppliedOperation(command, status));
        return status;
    }

    private static boolean isTombstone(SegmentControlFormat.Type type) {
        return type == SegmentControlFormat.Type.TOMBSTONE || type == SegmentControlFormat.Type.DELETE_DELIVERY_APPLY;
    }

    /** Observed control authority only; absence of local-success receipts is not a rollback permission. */
    synchronized org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts rollbackSafetyFacts() {
        return new org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts(false, !ledgers.isEmpty(),
                ledgers.values().stream().anyMatch(state -> state.activationGeneration != 0),
                !fenceOperations.isEmpty() || ledgers.values().stream().anyMatch(state -> state.fenced
                        || !state.recoveryGrants.isEmpty()), !tombstones.isEmpty(), false, failed);
    }

    synchronized ProfileControlRequest.DeleteBinding deleteAppliedCursor() {
        return deleteApplied;
    }

    private void verifyDeleteSequence(Command command) throws DeleteSequenceException {
        if (command.deletion == null) {
            return;
        }
        var prior = operations.get(command.operationId());
        if (prior != null) {
            if (!prior.command().sameMeaning(command) || prior.status() != Status.APPLIED_DURABLE
                    && prior.status() != Status.ALREADY_DURABLE) {
                throw new DeleteSequenceException(false);
            }
            return;
        }
        var deletion = command.deletion;
        if (deleteApplied != null && (!Arrays.equals(deleteApplied.streamId(), deletion.streamId())
                || deleteApplied.streamGeneration() != deletion.streamGeneration()
                || deleteApplied.assignmentGeneration() != deletion.assignmentGeneration())) {
            throw new DeleteSequenceException(false);
        }
        long next = deleteApplied == null ? 1 : deleteApplied.sequence() + 1;
        if (deletion.sequence() != next) {
            throw new DeleteSequenceException(deletion.sequence() > next);
        }
        var state = ledgers.get(command.scope.ledgerId());
        var terminal = tombstones.get(command.scope.ledgerId());
        var target = context(command.scope);
        if (fenceOperations.containsKey(command.operationId()) || checkpointIds.contains(command.operationId())
                || terminal != null && !terminal.equals(target)
                || state != null && (!state.context.equals(target)
                    || command.scope.authorityGeneration() < state.install.authorityGeneration())) {
            throw new DeleteSequenceException(false);
        }
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

    private static boolean isRecoveryClose(SegmentControlFormat.Type type) {
        return type == SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE
                || type == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE
                || type == SegmentControlFormat.Type.RECOVERY_TERMINAL_BOUNDARY;
    }

    private static boolean terminalUpgrade(Command before, Command after) {
        return before.type == SegmentControlFormat.Type.RECOVERY_GRANT_CLOSE
                && after.type == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE
                && before.scope.equals(after.scope) && before.grant.sameMeaning(after.grant)
                && before.closedGrantId.equals(after.closedGrantId);
    }

    private void selectRotation() throws IOException {
        for (int slot = 0; slot < 2; slot++) {
            SegmentControlRotation.Root root;
            try {
                SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.BOOKIE, slot);
                root = SegmentControlRotation.publication(config, SegmentControlFormat.Owner.BOOKIE, slot);
            } catch (IOException invalidPublication) {
                // An invalid page is optional. A selected generation's journal is never optional.
                continue;
            }
            if (root != null && (rotationRoot == null || root.generation() > rotationRoot.generation())) {
                rotationRoot = root;
            } else if (root != null && rotationRoot.generation() == root.generation()
                    && !root.equals(rotationRoot)) {
                throw new IOException("Conflicting Bookie rotation roots");
            }
        }
        if (rotationRoot != null) {
            channel = FileChannel.open(SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE,
                    rotationRoot.generation()),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            ByteBuffer header = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(channel, header, 0);
            identity.validate(header, config, -3);
            if (!rotationRoot.equals(SegmentControlRotation.readHeader(channel, SegmentControlFormat.Owner.BOOKIE))) {
                throw new IOException("Bookie rotation publication differs from its journal");
            }
            physicalGeneration = new Generation(rotationRoot.generation());
        }
    }

    private void recoverRotated() throws IOException {
        long size = channel.size();
        if (size < SegmentControlRotation.FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid rotated Bookie control log size");
        }
        List<SegmentControlCheckpoint.Reference> candidates = new ArrayList<>();
        candidates.add(rotationRoot.first());
        candidates.add(rotationRoot.second());
        SegmentControlCheckpoint.Reference[] publications = new SegmentControlCheckpoint.Reference[2];
        for (int slot = 0; slot < 2; slot++) {
            boolean valid = false;
            try {
                SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.publication(
                        config, SegmentControlFormat.Owner.BOOKIE, slot);
                if (rotationRoot.equals(SegmentControlRotation.publication(config,
                        SegmentControlFormat.Owner.BOOKIE, slot))) {
                    publications[slot] = reference;
                    valid = true;
                }
            } catch (IOException invalidPublication) {
                // Recovery still has to validate one complete publication and every current suffix record.
            }
            if (valid) {
                SegmentControlCheckpoint.syncPublication(config, slot);
            }
        }
        long sequence = rotationRoot.through() + 1;
        long generation = rotationRoot.second().generation();
        HashSet<UUID> operations = new HashSet<>();
        for (long offset = SegmentControlRotation.FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            SegmentControlParser.Command parsed = parse(readRecord(offset, length), sequence);
            boolean first = parsed.type() != SegmentControlFormat.Type.EXPLICIT_LAC
                    && operations.add(parsed.operationId());
            if (parsed.type() == SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT) {
                SegmentControlCheckpoint.Reference reference = decodeCheckpoint(parsed, sequence);
                if (!first || parsed.expectedGeneration() != generation) {
                    throw new IOException("Invalid rotated Bookie checkpoint order");
                }
                if (reference.through() < rotationRoot.through()
                        || reference.logOffset() < SegmentControlRotation.FIRST_RECORD
                        || reference.logOffset() > offset) {
                    throw new IOException("Invalid rotated Bookie checkpoint cut");
                }
                recordLength(reference.logOffset(), size, reference.through() + 1);
                generation = reference.generation();
                if (reference.equals(publications[reference.slot()])) {
                    candidates.add(reference);
                }
            } else if (parsed.type() == SegmentControlFormat.Type.FENCE) {
                fenceContext(parsed);
            } else if (parsed.type() == SegmentControlFormat.Type.EXPLICIT_LAC) {
                decodeLac(parsed);
            } else {
                decode(parsed);
            }
            offset += length;
            sequence++;
        }
        channel.force(true);
        candidates.sort(Comparator.comparingLong(SegmentControlCheckpoint.Reference::generation).reversed());
        boolean restored = false;
        for (SegmentControlCheckpoint.Reference reference : candidates) {
            try {
                restoreSnapshot(reference);
                if (reference.equals(rotationRoot.first()) || reference.equals(rotationRoot.second())) {
                    if (reference.equals(rotationRoot.first())) {
                        replayRootCommit(rotationRoot.firstId(), rotationRoot.first());
                    }
                    replayRootCommit(rotationRoot.secondId(), rotationRoot.second());
                    appliedOffset = SegmentControlRotation.FIRST_RECORD;
                }
                restored = true;
                break;
            } catch (IOException | IllegalArgumentException invalidImage) {
                resetRecoveredState();
            }
        }
        if (!restored) {
            throw new IOException("No recoverable Bookie checkpoint after prefix reclamation");
        }
        sequence = appliedThrough + 1;
        for (long offset = appliedOffset; offset < size;) {
            int length = recordLength(offset, size, sequence);
            replay(readRecord(offset, length), sequence++, null);
            offset += length;
            appliedOffset = offset;
        }
        nextOffset = size;
    }

    private void replayRootCommit(UUID operationId, SegmentControlCheckpoint.Reference reference) throws IOException {
        long sequence = appliedThrough + 1;
        ByteBuffer record = SegmentControlFormat.encode(SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT,
                LOG_GENERATION, sequence, operationId, new Generation(reference.generation()),
                reference.generation() - 1, reference.encode());
        replay(record, sequence, null);
    }

    private void restoreSnapshot(SegmentControlCheckpoint.Reference reference) throws IOException {
        SegmentControlCheckpoint.Snapshot snapshot = SegmentControlCheckpoint.read(
                config, identity, SegmentControlFormat.Owner.BOOKIE, reference, maxLogBytes);
        resetRecoveredState();
        for (SegmentControlCheckpoint.Operation operation : snapshot.operations()) {
            if (snapshot.compactedCheckpointHistory()) {
                SegmentControlParser.Command parsed = parse(operation.record(), operation.sequence());
                if (parsed.type() == SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT) {
                    SegmentControlCheckpoint.Reference predecessor = decodeCheckpoint(parsed, operation.sequence());
                    if (operation.status() != Status.APPLIED_DURABLE.ordinal() + 1
                            || predecessor.generation() <= checkpointGeneration
                            || predecessor.generation() >= reference.generation()) {
                        throw new IOException("Invalid compacted Bookie checkpoint predecessor");
                    }
                    checkpointGeneration = predecessor.generation() - 1;
                }
            }
            replay(operation.record(), operation.sequence(), operation.status());
        }
        if (checkpointGeneration != reference.generation() - 1) {
            throw new IOException("Bookie checkpoint does not retain its committed predecessor");
        }
        appliedThrough = snapshot.through();
        durableThrough = snapshot.through();
        appliedOffset = snapshot.logOffset();
        publishedCheckpoint = reference;
    }

    private void recover() throws IOException {
        ByteBuffer originalIdentity = ByteBuffer.allocate(4096);
        SegmentFileIo.readFully(originalChannel, originalIdentity, 0);
        identity.validate(originalIdentity, config, -3);
        selectRotation();
        if (rotationRoot != null) {
            recoverRotated();
            return;
        }
        long size = channel.size();
        if (size < FIRST_RECORD || size > maxLogBytes) {
            throw new IOException("Invalid Bookie control log size");
        }
        SegmentControlCheckpoint.Reference[] publications = new SegmentControlCheckpoint.Reference[2];
        for (int slot = 0; slot < publications.length; slot++) {
            try {
                publications[slot] = SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.BOOKIE,
                        slot);
            } catch (IOException incompletePublication) {
                // The immutable identity page remains mandatory; only this owner's publication can fall back.
            }
        }
        List<SegmentControlCheckpoint.Reference> checkpoints = new ArrayList<>(2);
        HashSet<UUID> ids = new HashSet<>();
        long committedGeneration = 0;
        long sequence = 0;
        for (long offset = FIRST_RECORD; offset < size;) {
            int length = recordLength(offset, size, sequence);
            ByteBuffer bytes = readRecord(offset, length);
            SegmentControlParser.Command parsed = parse(bytes, sequence);
            boolean first = parsed.type() != SegmentControlFormat.Type.EXPLICIT_LAC && ids.add(parsed.operationId());
            if (parsed.type() == SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT) {
                SegmentControlCheckpoint.Reference reference = decodeCheckpoint(parsed, sequence);
                if (!first || reference.generation() != committedGeneration + 1 || reference.logOffset() > offset) {
                    throw new IOException("Invalid Bookie checkpoint commit order");
                }
                recordLength(reference.logOffset(), size, reference.through() + 1);
                committedGeneration = reference.generation();
                if (reference.equals(publications[reference.slot()])) {
                    checkpoints.add(reference);
                }
            } else if (parsed.type() == SegmentControlFormat.Type.FENCE) {
                fenceContext(parsed);
            } else if (parsed.type() == SegmentControlFormat.Type.EXPLICIT_LAC) {
                decodeLac(parsed);
            } else {
                decode(parsed);
            }
            offset += length;
            sequence++;
        }
        channel.force(true);
        recoverCheckpoint(checkpoints);
        sequence = appliedThrough + 1;
        for (long offset = appliedOffset; offset < size;) {
            int length = recordLength(offset, size, sequence);
            replay(readRecord(offset, length), sequence, null);
            offset += length;
            appliedOffset = offset;
            sequence++;
        }
        nextOffset = size;
    }

    private void recoverCheckpoint(List<SegmentControlCheckpoint.Reference> checkpoints) throws IOException {
        for (int index = checkpoints.size() - 1; index >= 0; index--) {
            SegmentControlCheckpoint.Reference reference = checkpoints.get(index);
            try {
                restoreSnapshot(reference);
            } catch (IOException | IllegalArgumentException invalidCheckpoint) {
                resetRecoveredState();
                continue;
            }
            SegmentControlCheckpoint.syncPublication(config, reference.slot());
            return;
        }
        resetRecoveredState();
    }

    private void resetRecoveredState() {
        ledgers.clear();
        tombstones.clear();
        deleteApplied = null;
        operations.clear();
        fenceOperations.clear();
        checkpointState.clear();
        checkpointLacs.clear();
        checkpointIds.clear();
        checkpointGeneration = 0;
        publishedCheckpoint = null;
        appliedThrough = -1;
        durableThrough = -1;
        appliedOffset = FIRST_RECORD;
    }

    private ByteBuffer readRecord(long offset, int length) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        SegmentFileIo.readFully(channel, bytes, offset);
        if (physicalGeneration.equals(LOG_GENERATION)) {
            return bytes;
        }
        SegmentControlParser.Command parsed = SegmentControlParser.parse(bytes, SegmentControlFormat.Owner.BOOKIE,
                physicalGeneration, bytes.getLong(24));
        return SegmentControlFormat.encode(parsed.type(), LOG_GENERATION, bytes.getLong(24), parsed.operationId(),
                parsed.operationGeneration(), parsed.expectedGeneration(), parsed.body());
    }

    private ByteBuffer physicalRecord(ByteBuffer canonical) throws IOException {
        if (physicalGeneration.equals(LOG_GENERATION)) {
            return canonical;
        }
        SegmentControlParser.Command parsed = parse(canonical, canonical.getLong(24));
        return SegmentControlFormat.encode(parsed.type(), physicalGeneration, canonical.getLong(24),
                parsed.operationId(),
                parsed.operationGeneration(), parsed.expectedGeneration(), parsed.body());
    }

    private static SegmentControlParser.Command parse(ByteBuffer bytes, long sequence) throws IOException {
        return SegmentControlParser.parse(bytes, SegmentControlFormat.Owner.BOOKIE, LOG_GENERATION, sequence);
    }

    private void replay(ByteBuffer bytes, long sequence, Integer frozenStatus) throws IOException {
        SegmentControlParser.Command parsed = parse(bytes, sequence);
        if (frozenStatus != null && parsed.type() != SegmentControlFormat.Type.EXPLICIT_LAC
                && checkpointState.containsKey(parsed.operationId())) {
            throw new IOException("Repeated operation in Bookie checkpoint");
        }
        Status status = Status.APPLIED_DURABLE;
        if (parsed.type() == SegmentControlFormat.Type.FENCE) {
            applyFence(fenceContext(parsed), parsed.operationId());
            retain(sequence, parsed.operationId(), status, bytes);
        } else if (parsed.type() == SegmentControlFormat.Type.EXPLICIT_LAC) {
            DurableLac lac = decodeLac(parsed);
            applyExplicitLac(lac.context(), lac.lac(), lac.body(), parsed.operationGeneration().value());
            checkpointLacs.put(lac.context().ledgerId(), new SegmentControlCheckpoint.Operation(sequence, 1, bytes));
        } else if (parsed.type() == SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT) {
            applyCheckpoint(decodeCheckpoint(parsed, sequence), parsed.operationId(), sequence);
            retain(sequence, parsed.operationId(), status, bytes);
        } else {
            Command command = decode(parsed);
            verifyDeleteSequence(command);
            status = apply(command);
            retain(sequence, parsed.operationId(), status, bytes);
        }
        if (frozenStatus != null && status.ordinal() + 1 != frozenStatus) {
            throw new IOException("Bookie checkpoint result differs from its applied outcome");
        }
        appliedThrough = sequence;
        durableThrough = sequence;
    }

    private void applyCheckpoint(SegmentControlCheckpoint.Reference reference, UUID operationId, long sequence)
            throws IOException {
        if (reference.generation() != checkpointGeneration + 1 || reference.through() >= sequence
                || operations.containsKey(operationId) || fenceOperations.containsKey(operationId)
                || !checkpointIds.add(operationId)) {
            throw new IOException("Invalid durable Bookie checkpoint transition");
        }
        checkpointGeneration = reference.generation();
    }

    private SegmentControlCheckpoint.Reference decodeCheckpoint(SegmentControlParser.Command parsed, long sequence)
            throws IOException {
        SegmentControlCheckpoint.Reference reference = SegmentControlCheckpoint.decodeReference(parsed.body());
        if (parsed.operationGeneration().value() != reference.generation()
                || parsed.expectedGeneration() != reference.generation() - 1 || reference.through() >= sequence
                || reference.payloadBytes() - maxLogBytes > maxLogBytes) {
            throw new IOException("Invalid Bookie checkpoint binding");
        }
        return reference;
    }

    private static ProfileLedgerContext fenceContext(SegmentControlParser.Command parsed) throws IOException {
        ByteBuffer body = parsed.body();
        if (parsed.operationGeneration().value() != 1 || parsed.expectedGeneration() != 0
                || body.remaining() != ProfileLedgerContext.ENCODED_LENGTH) {
            throw new IOException("Invalid durable fence record");
        }
        byte[] bytes = new byte[ProfileLedgerContext.ENCODED_LENGTH];
        body.get(bytes);
        try {
            return ProfileLedgerContext.decode(bytes);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid durable fence context", invalid);
        }
    }

    private record DurableLac(ProfileLedgerContext context, long lac, byte[] body) { }

    private static DurableLac decodeLac(SegmentControlParser.Command parsed) throws IOException {
        ByteBuffer body = parsed.body();
        if (parsed.expectedGeneration() != 0 || body.remaining() < ProfileLedgerContext.ENCODED_LENGTH + 12) {
            throw new IOException("Invalid durable explicit LAC record");
        }
        byte[] bytes = new byte[ProfileLedgerContext.ENCODED_LENGTH];
        body.get(bytes);
        long lac = body.getLong();
        int length = body.getInt();
        if (length < 0 || length != body.remaining() || lac < -1) {
            throw new IOException("Invalid durable explicit LAC body length");
        }
        byte[] value = new byte[length];
        body.get(value);
        try {
            return new DurableLac(ProfileLedgerContext.decode(bytes), lac, value);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid durable explicit LAC context", invalid);
        }
    }

    private int recordLength(long offset, long size, long sequence) throws IOException {
        if (size - offset < 128) {
            throw new IOException("Incomplete Bookie control tail");
        }
        ByteBuffer header = ByteBuffer.allocate(128);
        SegmentFileIo.readFully(channel, header, offset);
        int length = SegmentControlParser.recordLength(header, SegmentControlFormat.Owner.BOOKIE,
                physicalGeneration, sequence);
        if (length > size - offset) {
            throw new IOException("Incomplete Bookie control record");
        }
        return length;
    }

    private Command decode(SegmentControlParser.Command parsed) throws IOException {
        if (parsed.type() != SegmentControlFormat.Type.INSTALL
                && parsed.type() != SegmentControlFormat.Type.ACTIVATE
                && !isTombstone(parsed.type())
                && parsed.type() != SegmentControlFormat.Type.RECOVERY_GRANT
                && !isRecoveryClose(parsed.type())) {
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
            if (parsed.type() == SegmentControlFormat.Type.DELETE_DELIVERY_APPLY) {
                var deletion = ProfileControlRequest.DeleteBinding.decode(body);
                ProfileControlRequest.deleteApply(scope, deletion);
                return new Command(scope, deletion);
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
            if (parsed.type() == SegmentControlFormat.Type.RECOVERY_TERMINAL_BOUNDARY) {
                if (scope.operation() != ProfileControlOperation.RECOVERY_TERMINAL_CLOSE
                        || scope.exactRange() != null || body.remaining() != 48) {
                    throw new IOException("Invalid terminal boundary body");
                }
                long prefix = body.getLong();
                long length = body.getLong();
                byte[] digest = new byte[32];
                body.get(digest);
                var binding = new ProfileControlRequest.TerminalBinding(prefix, length, digest);
                if (!Arrays.equals(scope.publicSemanticPayloadIdentity(),
                        ProfileControlRequest.terminalPayloadIdentity(scope, binding))) {
                    throw new IOException("Invalid terminal boundary identity");
                }
                return new Command(scope, binding);
            }
            if (isRecoveryClose(parsed.type())) {
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
                        grantOperationId, parsed.type() == SegmentControlFormat.Type.RECOVERY_TERMINAL_CLOSE);
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
        if (command.deletion != null) {
            return ByteBuffer.allocate(scope.remaining() + ProfileControlRequest.DeleteBinding.BYTES)
                    .put(scope).put(command.deletion.encode()).flip();
        }
        if (command.type == SegmentControlFormat.Type.RECOVERY_GRANT) {
            return ByteBuffer.allocate(scope.remaining() + 32).put(scope).put(command.grant.intentId())
                    .putLong(command.grant.intentGeneration()).putLong(command.grant.grantGeneration()).flip();
        }
        if (command.terminal != null) {
            return ByteBuffer.allocate(scope.remaining() + 48).put(scope)
                    .putLong(command.terminal.prefixEntryId()).putLong(command.terminal.prefixLength())
                    .put(command.terminal.selectionDigest()).flip();
        }
        if (isRecoveryClose(command.type)) {
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
            case RECOVERY_TERMINAL_CLOSE -> 7;
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
            case 7 -> ProfileControlOperation.RECOVERY_TERMINAL_CLOSE;
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
            boolean superblock = fileId == -1 || fileId == -2;
            if ((superblock && (input.size() < FIRST_RECORD || input.size() > 12288))
                    || (!superblock && expectedSize < 0 && input.size() < FIRST_RECORD)
                    || (!superblock && expectedSize >= 0 && input.size() != expectedSize)) {
                throw new IOException("Isolated storage file size mismatch");
            }
            ByteBuffer header = ByteBuffer.allocate(4096);
            SegmentFileIo.readFully(input, header, 0);
            identity.validate(header, config, fileId);
        }
    }

    private void requireWritable() throws IOException {
        if (closed || failed || (rotating != null && rotating != Thread.currentThread())) {
            throw new IOException("Bookie control log is closed, rotating or has uncertain I/O");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (writing || checkpointing || rotating != null || activeAdmissions != 0 || reclamation != null) {
            throw new IOException("Bookie control I/O or admissions are still in flight");
        }
        closed = true;
        IOException first = null;
        try {
            if (channel != originalChannel) {
                channel.close();
            }
        } catch (IOException failure) {
            first = failure;
        }
        try {
            originalChannel.close();
        } catch (IOException failure) {
            if (first == null) {
                first = failure;
            } else {
                first.addSuppressed(failure);
            }
        }
        try {
            writerLock.close();
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

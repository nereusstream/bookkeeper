/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.IsolatedProfileControlEndpoint;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileTransportContext;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus.DurableResult;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus.RetryDisposition;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus.StatusClass;
import org.apache.bookkeeper.proto.profile.ProfileInboundFrame;
import org.apache.bookkeeper.proto.profile.ProfileInboundPipeline;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Isolated normal/recovery Add and fence; caller owns listener, event loops and storage lifetime. */
final class SegmentProfileAddHandler extends ChannelInboundHandlerAdapter {
    private static final int ADD_PREFIX_BYTES = 100;
    private static final int RECOVERY_PREFIX_BYTES = 144;
    private static final int RESPONSE_BYTES = ProfileFrameCodec.OUTER_LENGTH_PREFIX
            + ProfileFrameHeader.ENCODED_LENGTH + ProfileStatus.ENCODED_LENGTH;
    private final IsolatedSegmentStorage storage;
    private final TLSContextFactory tls;
    private final String trustDomain;
    private final ProfileHello.Server hello;
    private final int maxEntryBytes;
    private final int maxPending;
    private final Set<Pending> pending = new HashSet<>();
    private final SegmentProfileControlConfig control;
    private final SegmentProfileInstallStore physicalControl;
    private ProfileTransportContext transport;
    private boolean initialControlNegotiated;
    private boolean replacementControlNegotiated;
    private boolean established;
    private boolean controlEstablished;
    private boolean closed;

    SegmentProfileAddHandler(IsolatedSegmentStorage storage, TLSContextFactory tls, String trustDomain,
                             ProfileHello.Server hello, int maxEntryBytes, int maxPending) {
        this(storage, tls, trustDomain, hello, maxEntryBytes, maxPending, null);
    }

    SegmentProfileAddHandler(IsolatedSegmentStorage storage, TLSContextFactory tls, String trustDomain,
                             ProfileHello.Server hello, int maxEntryBytes, int maxPending,
                             SegmentProfileControlConfig control) {
        this.control = control;
        physicalControl = control == null ? null : storage.profileInstallStore(control.timeout());
        this.storage = Objects.requireNonNull(storage, "storage");
        this.tls = Objects.requireNonNull(tls, "tls");
        this.trustDomain = Objects.requireNonNull(trustDomain, "trustDomain");
        this.hello = Objects.requireNonNull(hello, "hello");
        this.maxEntryBytes = maxEntryBytes;
        this.maxPending = maxPending;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        ProfileInboundFrame frame = (ProfileInboundFrame) message;
        if (closed || pending.size() >= maxPending || frame.header().flags() != 0) {
            frame.close();
            ctx.close();
            return;
        }
        ProfileInboundPipeline.ResponseCredit credit = null;
        Pending request = null;
        try {
            // Reserve before encoding/copying: HELLO has a bounded temporary body plus its output buffer;
            // Add has only its fixed context/credential parse copies and one fixed status response.
            int bytes = frame.header().operation() == ProfileOperation.HELLO
                    ? 2 * (ProfileFrameHeader.ENCODED_LENGTH + ProfileFrameCodec.OUTER_LENGTH_PREFIX
                            + 36 + ProfileHello.MAX_BOOKIE_ID_BYTES
                            + (control == null ? 0 : 8 * ProfileHello.MAX_CAPABILITIES))
                    : (control != null && ProfileInitialControlCodec.supported(frame.header().operation())
                            ? 8 * (int) frame.header().bodyLength() + 2 * ProfileInitialControlCodec.REPLY_BYTES
                            : frame.header().operation() == ProfileOperation.FENCE_LEDGER
                            || frame.header().operation() == ProfileOperation.ADD_RECOVERY ? 200 : ADD_PREFIX_BYTES)
                            + RESPONSE_BYTES;
            credit = ProfileInboundPipeline.reserveResponse(ctx.pipeline(), bytes);
            request = new Pending(frame, credit);
            pending.add(request);
            Pending held = request;
            request.deadline = ctx.executor().schedule(() -> {
                if (!held.finished.get()) {
                    ctx.close();
                }
            }, 30, TimeUnit.SECONDS);
            if (frame.header().operation() == ProfileOperation.HELLO) {
                SslHandler ssl = ctx.pipeline().get(SslHandler.class);
                transport = new ProfileTransportContext(ProfileTransportContext.PROFILE_ENDPOINT,
                        ssl.engine().getSession().getProtocol(), true, true, true,
                        tls.authenticatedProfilePrincipal(ssl, trustDomain));
                if (control != null) {
                    byte[] offer = new byte[frame.body().readableBytes()];
                    frame.body().getBytes(frame.body().readerIndex(), offer);
                    initialControlNegotiated = ProfileHello.decodeClient(offer).capabilities().contains(
                            new ProfileHello.Capability(ProfileInitialControlCodec.CAPABILITY_ID,
                                    ProfileInitialControlCodec.SCHEMA));
                    replacementControlNegotiated = ProfileHello.decodeClient(offer).capabilities().contains(
                            new ProfileHello.Capability(ProfileInitialControlCodec.REPLACEMENT_CAPABILITY_ID,
                                    ProfileInitialControlCodec.REPLACEMENT_SCHEMA));
                }
                // Capability agreement grants neither principal permissions nor metadata authority.
                ProfileInboundPipeline.markEstablished(ctx.pipeline());
                established = true;
                byte[] body = hello.encode();
                write(ctx, request, null, body);
            } else if (!established) {
                ctx.close();
            } else if (ProfileInitialControlCodec.supported(frame.header().operation())) {
                initialControl(ctx, request);
            } else if (frame.header().operation() == ProfileOperation.FENCE_LEDGER) {
                fence(ctx, request);
            } else if (frame.header().operation() == ProfileOperation.ADD_RECOVERY) {
                recoveryAdd(ctx, request);
            } else if (frame.header().operation() != ProfileOperation.ADD_NORMAL) {
                write(ctx, request, ProfileStatus.unsupported(), null);
            } else {
                add(ctx, request);
            }
        } catch (Exception | Error failure) {
            if (request != null) {
                finish(request);
            } else {
                try {
                    frame.close();
                } finally {
                    if (credit != null) {
                        credit.close();
                    }
                }
            }
            ctx.close();
            throw failure;
        }
    }

    private void initialControl(ChannelHandlerContext ctx, Pending request) {
        if (request.frame.header().operation() == ProfileOperation.ACTIVATE_REPLACEMENT
                ? !replacementControlNegotiated : !initialControlNegotiated) {
            request.closeAfterWrite = !controlEstablished;
            write(ctx, request, ProfileStatus.unsupported(), null);
            return;
        }
        request.beginCold();
        try {
            control.executor().execute(() -> {
                try {
                    if (request.finished.get()) {
                        return;
                    }
                    ProfileControlRequest parsed;
                    try {
                        ByteBuf body = request.frame.body();
                        parsed = ProfileInitialControlCodec.decodeRequest(request.frame.header().operation(),
                                body.nioBuffer(body.readerIndex(), body.readableBytes()), hello);
                    } catch (IllegalArgumentException invalid) {
                        complete(ctx, request, status(StatusClass.BAD_REQUEST,
                                RetryDisposition.NEVER, DurableResult.NONE));
                        return;
                    }
                    ProtectedProfileStateStore admittedStore = new ProtectedProfileStateStore() {
                        @Override
                        public TransitionResult conditionalApply(Transition transition) {
                            CompletableFuture<Boolean> admitted = new CompletableFuture<>();
                            try {
                                ctx.executor().execute(() -> {
                                    try {
                                        if (admitted.isDone() || request.finished.get() || !ctx.channel().isActive()) {
                                            admitted.complete(false);
                                            return;
                                        }
                                        if (!controlEstablished) {
                                            ProfileInboundPipeline.markControlAuthorized(
                                                    ctx.pipeline(), request.frame, request.credit);
                                            controlEstablished = true;
                                        }
                                        request.beginControl();
                                        if (!admitted.complete(true)) {
                                            request.endControl();
                                        }
                                    } catch (Exception failedAdmission) {
                                        admitted.complete(false);
                                    }
                                });
                                // No effects before this actual event-loop budget transfer. A timed-out task
                                // cannot later authorize: release stops it and the EL checks finished again.
                                if (!admitted.get(control.timeout().toNanos(), TimeUnit.NANOSECONDS)) {
                                    return new TransitionResult(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                                            0, false);
                                }
                            } catch (Exception unavailable) {
                                if (unavailable instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                                if (!admitted.complete(false) && admitted.getNow(false)) {
                                    request.endControl();
                                }
                                return new TransitionResult(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                                        0, false);
                            }
                            if (request.finished.get()) {
                                request.endControl();
                                return new TransitionResult(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                                        0, false);
                            }
                            return physicalControl.conditionalApply(transition, request::endControl);
                        }

                        @Override
                        public TransitionResult queryOperationResult(
                                ProfileControlScope scope) {
                            return new TransitionResult(ProfileControlResult.Outcome.UNSUPPORTED, 0, false);
                        }
                    };
                    ProfileControlResult result = new IsolatedProfileControlEndpoint(
                            EngineProfile.SEGMENT_WAL_ENGINE, control.validator(), control.staticAuthorizer(),
                            control.controlStore(), control.exactAuthorizer(), admittedStore).handle(transport, parsed);
                    complete(ctx, request, ProfileInitialControlCodec.status(result.outcome()),
                            ProfileInitialControlCodec.encodeReply(result));
                } finally {
                    request.endCold();
                }
            });
        } catch (RejectedExecutionException exhausted) {
            request.endCold();
            request.closeAfterWrite = !controlEstablished;
            write(ctx, request, status(StatusClass.TRANSIENT_UNAVAILABLE,
                    RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.NONE), null);
        }
    }

    private void add(ChannelHandlerContext ctx, Pending pendingRequest) {
        ByteBuf body = pendingRequest.frame.body();
        ProfileLedgerContext context;
        long entryId;
        byte[] credential;
        ByteBuf entry;
        try {
            int start = body.readerIndex();
            int length = body.readableBytes();
            if (length < ADD_PREFIX_BYTES
                    || body.getUnsignedShort(start + 72) != ProfileOperationCodec.CREDENTIAL_KIND
                    || body.getUnsignedShort(start + 74) != ProfileOperationCodec.CREDENTIAL_LENGTH
                    || body.getUnsignedInt(start + 96) != length - ADD_PREFIX_BYTES
                    || length - ADD_PREFIX_BYTES < SegmentBatchFormat.ENTRY_OVERHEAD
                    || length - ADD_PREFIX_BYTES > maxEntryBytes) {
                throw new IllegalArgumentException("Invalid Profile Add body");
            }
            context = ProfileLedgerContext.decode(body.nioBuffer(start, ProfileLedgerContext.ENCODED_LENGTH));
            entryId = body.getLong(start + 60);
            if (entryId < 0) {
                throw new IllegalArgumentException("Invalid Profile entry coordinate");
            }
            if (body.getUnsignedInt(start + 68) != 0) {
                write(ctx, pendingRequest, ProfileStatus.unsupported(), null);
                return;
            }
            credential = new byte[ProfileOperationCodec.CREDENTIAL_LENGTH];
            body.getBytes(start + 76, credential);
            entry = body.slice(start + ADD_PREFIX_BYTES, length - ADD_PREFIX_BYTES);
        } catch (IllegalArgumentException failure) {
            write(ctx, pendingRequest, status(StatusClass.BAD_REQUEST, RetryDisposition.NEVER, DurableResult.NONE),
                    null);
            return;
        }
        pendingRequest.storageRequest = new SegmentDataPhysicalDriver.NormalRequest((result, failure) ->
                complete(ctx, pendingRequest, resultStatus(result, failure)));
        try {
            SegmentBookieControlLog.NormalRejection refused = storage.submitProfileNormal(
                    context, credential, entryId, entry, pendingRequest.storageRequest);
            if (refused != null) {
                pendingRequest.storageRequest.stopWaiting();
                write(ctx, pendingRequest, rejectionStatus(refused), null);
            }
        } catch (IllegalArgumentException invalidEntry) {
            pendingRequest.storageRequest.stopWaiting();
            write(ctx, pendingRequest, status(StatusClass.BAD_REQUEST, RetryDisposition.NEVER, DurableResult.NONE),
                    null);
        } catch (IOException uncertain) {
            pendingRequest.storageRequest.stopWaiting();
            write(ctx, pendingRequest, unknown(), null);
        }
    }

    private void recoveryAdd(ChannelHandlerContext ctx, Pending request) {
        ProfileOperationCodec.AddRecovery tuple;
        ByteBuf entry;
        try {
            ByteBuf body = request.frame.body();
            int start = body.readerIndex();
            int length = body.readableBytes();
            if (length < RECOVERY_PREFIX_BYTES
                    || body.getUnsignedShort(start + 116) != ProfileOperationCodec.CREDENTIAL_KIND
                    || body.getUnsignedShort(start + 118) != ProfileOperationCodec.CREDENTIAL_LENGTH
                    || body.getUnsignedInt(start + 140) != length - RECOVERY_PREFIX_BYTES
                    || length - RECOVERY_PREFIX_BYTES < SegmentBatchFormat.ENTRY_OVERHEAD
                    || length - RECOVERY_PREFIX_BYTES > maxEntryBytes) {
                throw new IllegalArgumentException("Invalid Profile recovery Add body");
            }
            ProfileLedgerContext context = ProfileLedgerContext.decode(
                    body.nioBuffer(start, ProfileLedgerContext.ENCODED_LENGTH));
            byte[] intentId = new byte[ProfileOperationCodec.OPERATION_ID_LENGTH];
            byte[] credential = new byte[ProfileOperationCodec.CREDENTIAL_LENGTH];
            body.getBytes(start + 60, intentId);
            body.getBytes(start + 120, credential);
            // Only the small authorization tuple is copied. The reference codec must not clone the payload.
            tuple = new ProfileOperationCodec.AddRecovery(context, intentId,
                    body.getLong(start + 76), body.getLong(start + 84), body.getLong(start + 92),
                    body.getLong(start + 100), body.getLong(start + 108), credential, new byte[0]);
            entry = body.slice(start + RECOVERY_PREFIX_BYTES, length - RECOVERY_PREFIX_BYTES);
        } catch (IllegalArgumentException invalid) {
            write(ctx, request, status(StatusClass.BAD_REQUEST, RetryDisposition.NEVER, DurableResult.NONE), null);
            return;
        }
        request.recoveryRequest = new SegmentDataPhysicalDriver.RecoveryRequest((result, failure) ->
                complete(ctx, request, recoveryResultStatus(result, failure)));
        try {
            SegmentBookieControlLog.NormalRejection refused = storage.submitProfileRecovery(
                    tuple, entry, request.recoveryRequest);
            if (refused != null) {
                request.recoveryRequest.stopWaiting();
                write(ctx, request, rejectionStatus(refused), null);
            }
        } catch (IllegalArgumentException invalidEntry) {
            request.recoveryRequest.stopWaiting();
            write(ctx, request, status(StatusClass.BAD_REQUEST, RetryDisposition.NEVER, DurableResult.NONE), null);
        } catch (IOException uncertain) {
            request.recoveryRequest.stopWaiting();
            write(ctx, request, unknown(), null);
        }
    }

    private void fence(ChannelHandlerContext ctx, Pending request) throws IOException {
        ProfileOperationCodec.FenceLedger fence;
        try {
            ByteBuf body = request.frame.body();
            int start = body.readerIndex();
            if (body.readableBytes() != 100
                    || body.getUnsignedShort(start + 76) != ProfileOperationCodec.CREDENTIAL_KIND
                    || body.getUnsignedShort(start + 78) != ProfileOperationCodec.CREDENTIAL_LENGTH) {
                throw new IllegalArgumentException("Invalid Profile fence body");
            }
            ProfileLedgerContext context = ProfileLedgerContext.decode(
                    body.nioBuffer(start, ProfileLedgerContext.ENCODED_LENGTH));
            byte[] operationId = new byte[ProfileOperationCodec.OPERATION_ID_LENGTH];
            byte[] credential = new byte[ProfileOperationCodec.CREDENTIAL_LENGTH];
            body.getBytes(start + 60, operationId);
            body.getBytes(start + 80, credential);
            fence = new ProfileOperationCodec.FenceLedger(context, operationId, credential);
        } catch (IllegalArgumentException invalid) {
            request.closeAfterWrite = !controlEstablished;
            write(ctx, request, status(StatusClass.BAD_REQUEST, RetryDisposition.NEVER, DurableResult.NONE), null);
            return;
        }
        SegmentBookieControlLog.NormalRejection refused = storage.profileFenceRejection(fence);
        if (refused != null) {
            request.closeAfterWrite = !controlEstablished;
            write(ctx, request, rejectionStatus(refused), null);
            return;
        }
        if (!controlEstablished) {
            ProfileInboundPipeline.markControlAuthorized(ctx.pipeline(), request.frame, request.credit);
            controlEstablished = true;
        }
        request.beginControl();
        boolean submitted = false;
        try {
            refused = storage.submitProfileFence(fence, (result, failure) -> {
                try {
                    ProfileStatus response;
                    if (failure != null || result == null) {
                        response = unknown();
                    } else {
                        response = switch (result.status()) {
                            case APPLIED_DURABLE -> status(StatusClass.OK, RetryDisposition.NEVER,
                                    DurableResult.APPLIED);
                            case ALREADY_DURABLE -> status(StatusClass.OK, RetryDisposition.NEVER,
                                    DurableResult.ALREADY_APPLIED);
                            case WAITING_PRECUT -> status(StatusClass.TRANSIENT_UNAVAILABLE,
                                    RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.NONE);
                            case REJECTED -> {
                                SegmentBookieControlLog.NormalRejection reason = storage.profileFenceRejection(fence);
                                yield rejectionStatus(reason != null ? reason
                                        : SegmentBookieControlLog.NormalRejection.IDENTITY_CONFLICT);
                            }
                        };
                    }
                    complete(ctx, request, response);
                } finally {
                    request.endControl();
                }
            });
            submitted = refused == null;
            if (!submitted) {
                write(ctx, request, rejectionStatus(refused), null);
            }
        } finally {
            if (!submitted) {
                request.endControl();
            }
        }
    }

    private void complete(ChannelHandlerContext ctx, Pending request, ProfileStatus response) {
        complete(ctx, request, response, null);
    }

    private void complete(ChannelHandlerContext ctx, Pending request, ProfileStatus response, byte[] body) {
        if (!request.beginDelivery()) {
            return;
        }
        try {
            ctx.executor().execute(() -> {
                try {
                    if (!request.finished.get()) {
                        if (ProfileInitialControlCodec.supported(request.frame.header().operation())) {
                            request.closeAfterWrite = !controlEstablished;
                        }
                        write(ctx, request, response, body);
                    }
                } catch (RuntimeException | Error failedResponse) {
                    finish(request);
                    ctx.close();
                    throw failedResponse;
                } finally {
                    request.endDelivery();
                }
            });
        } catch (RejectedExecutionException shutdown) {
            request.endDelivery();
            request.release();
            ctx.close();
        }
    }

    private static ProfileStatus rejectionStatus(SegmentBookieControlLog.NormalRejection rejection) {
        return switch (rejection) {
            case UNAVAILABLE -> status(StatusClass.TRANSIENT_UNAVAILABLE,
                    RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.NONE);
            case NOT_ACTIVE -> status(StatusClass.PROFILE_NOT_READY_OR_STALE,
                    RetryDisposition.AFTER_CONTROL_RECONCILIATION, DurableResult.NONE);
            case IDENTITY_CONFLICT -> status(StatusClass.PROFILE_IDENTITY_CONFLICT,
                    RetryDisposition.NEVER, DurableResult.NONE);
            case UNAUTHORIZED -> status(StatusClass.UNAUTHORIZED, RetryDisposition.NEVER, DurableResult.NONE);
            case TOMBSTONED -> status(StatusClass.TOMBSTONED_OR_DELETED,
                    RetryDisposition.NEVER, DurableResult.NONE);
            case FENCED -> status(StatusClass.FENCED, RetryDisposition.NEVER, DurableResult.NONE);
            case INVALID_GRANT -> status(StatusClass.RECOVERY_GRANT_INVALID,
                    RetryDisposition.NEVER, DurableResult.NONE);
        };
    }

    private static ProfileStatus resultStatus(SegmentDataPhysicalDriver.NormalResult result, Throwable failure) {
        if (failure != null || result == null) {
            return unknown();
        }
        return switch (result.status()) {
            case LOCAL_DURABLE -> status(StatusClass.OK, RetryDisposition.NEVER, DurableResult.APPLIED);
            case TRANSIENT_UNAVAILABLE -> status(StatusClass.TRANSIENT_UNAVAILABLE,
                    RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.NONE);
            case CONFLICT -> status(StatusClass.PROFILE_IDENTITY_CONFLICT,
                    RetryDisposition.NEVER, DurableResult.NONE);
            // A stale admission can follow a durable physical write. Do not claim this attempt did no DATA I/O.
            case STALE_ADMISSION -> status(StatusClass.PROFILE_NOT_READY_OR_STALE,
                    RetryDisposition.AFTER_CONTROL_RECONCILIATION, DurableResult.UNKNOWN);
            case UNKNOWN -> unknown();
        };
    }

    private static ProfileStatus recoveryResultStatus(SegmentDataPhysicalDriver.RecoveryResult result,
                                                       Throwable failure) {
        if (failure != null || result == null) {
            return unknown();
        }
        return switch (result.status()) {
            case LOCAL_DURABLE -> status(StatusClass.OK, RetryDisposition.NEVER, DurableResult.APPLIED);
            case TRANSIENT_UNAVAILABLE -> status(StatusClass.TRANSIENT_UNAVAILABLE,
                    RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.NONE);
            case CONFLICT -> status(StatusClass.PROFILE_IDENTITY_CONFLICT,
                    RetryDisposition.NEVER, DurableResult.NONE);
            // A close/tombstone cut after physical I/O cannot establish that this attempt did no DATA write.
            case STALE_ADMISSION -> status(StatusClass.RECOVERY_GRANT_INVALID,
                    RetryDisposition.AFTER_CONTROL_RECONCILIATION, DurableResult.UNKNOWN);
            case UNKNOWN -> unknown();
        };
    }

    private static ProfileStatus unknown() {
        return status(StatusClass.DURABILITY_RESULT_UNKNOWN,
                RetryDisposition.SAME_PROFILE_OPERATION, DurableResult.UNKNOWN);
    }

    private static ProfileStatus status(StatusClass status, RetryDisposition retry, DurableResult durable) {
        return new ProfileStatus(status, retry, durable, 0);
    }

    private void write(ChannelHandlerContext ctx, Pending request, ProfileStatus status, byte[] helloBody) {
        if (closed || !ctx.channel().isActive()) {
            finish(request);
            return;
        }
        int bodyLength = helloBody != null ? helloBody.length : ProfileStatus.ENCODED_LENGTH;
        int bytes = ProfileFrameCodec.OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH + bodyLength;
        ByteBuf response = ProfileInboundPipeline.allocateResponse(ctx.pipeline(), request.credit, bytes);
        try {
            int flags = ProfileFrameHeader.RESPONSE;
            if (status != null && status.statusClass() != StatusClass.OK) {
                flags |= ProfileFrameHeader.ERROR;
            }
            response.writeInt(bytes - ProfileFrameCodec.OUTER_LENGTH_PREFIX).writeInt(ProfileFrameHeader.MAGIC)
                    .writeShort(ProfileFrameHeader.PROTOCOL_MAJOR).writeShort(ProfileFrameHeader.PROTOCOL_MINOR)
                    .writeShort(ProfileFrameHeader.ENCODED_LENGTH)
                    .writeShort(request.frame.header().operation().wireValue())
                    .writeInt(flags).writeLong(request.frame.header().requestId()).writeInt(bodyLength).writeInt(0);
            if (helloBody != null) {
                response.writeBytes(helloBody);
            } else {
                response.writeShort(status.statusClass().wireValue()).writeByte(status.retryDisposition().wireValue())
                        .writeByte(status.durableResult().wireValue()).writeShort(status.detailCode()).writeShort(0);
            }
        } catch (RuntimeException | Error failure) {
            response.release();
            throw failure;
        }
        request.writing = true;
        ctx.writeAndFlush(response).addListener(written -> {
            request.writing = false;
            finish(request);
            if (!written.isSuccess() || request.closeAfterWrite) {
                ctx.close();
            }
        });
    }

    private void finish(Pending request) {
        pending.remove(request);
        request.release();
    }

    private void closePending() {
        closed = true;
        for (Pending request : pending) {
            request.release();
        }
        pending.clear();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        closePending();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        closePending();
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }

    private static final class Pending {
        private final ProfileInboundFrame frame;
        private final ProfileInboundPipeline.ResponseCredit credit;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicBoolean resourcesReturned = new AtomicBoolean();
        private volatile boolean writing;
        private boolean delivering;
        private boolean controlInFlight;
        private boolean coldInFlight;
        private boolean closeAfterWrite;
        private ScheduledFuture<?> deadline;
        private SegmentDataPhysicalDriver.NormalRequest storageRequest;
        private SegmentDataPhysicalDriver.RecoveryRequest recoveryRequest;

        private Pending(ProfileInboundFrame frame, ProfileInboundPipeline.ResponseCredit credit) {
            this.frame = frame;
            this.credit = credit;
        }

        private synchronized void beginCold() {
            coldInFlight = true;
        }

        private synchronized void endCold() {
            coldInFlight = false;
            returnResourcesIfTerminal();
        }

        private synchronized void beginControl() {
            controlInFlight = true;
        }

        private synchronized void endControl() {
            controlInFlight = false;
            returnResourcesIfTerminal();
        }

        private synchronized boolean beginDelivery() {
            if (finished.get()) {
                return false;
            }
            delivering = true;
            return true;
        }

        private synchronized void endDelivery() {
            delivering = false;
            returnResourcesIfTerminal();
        }

        private synchronized void release() {
            if (finished.compareAndSet(false, true)) {
                if (deadline != null) {
                    deadline.cancel(false);
                }
                if (storageRequest != null) {
                    storageRequest.stopWaiting();
                }
                if (recoveryRequest != null) {
                    recoveryRequest.stopWaiting();
                }
            }
            returnResourcesIfTerminal();
        }

        private void returnResourcesIfTerminal() {
            // Removing the application handler can precede the downstream write's actual completion.
            // A submitted fence also retains its charged immutable request until the actual owner terminal.
            // Cancellation stops responses, but the write promise still owns the charged response workspace.
            if (finished.get() && !writing && !delivering && !controlInFlight && !coldInFlight
                    && resourcesReturned.compareAndSet(false, true)) {
                try {
                    frame.close();
                } finally {
                    credit.close();
                }
            }
        }
    }
}

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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
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

/** Real isolated normal-Add dispatch. Caller owns the listener, event loops and storage lifetime. */
final class SegmentProfileAddHandler extends ChannelInboundHandlerAdapter {
    private static final int ADD_PREFIX_BYTES = 100;
    private static final int RESPONSE_BYTES = ProfileFrameCodec.OUTER_LENGTH_PREFIX
            + ProfileFrameHeader.ENCODED_LENGTH + ProfileStatus.ENCODED_LENGTH;
    private final IsolatedSegmentStorage storage;
    private final TLSContextFactory tls;
    private final String trustDomain;
    private final ProfileHello.Server hello;
    private final int maxEntryBytes;
    private final int maxPending;
    private final Set<Pending> pending = new HashSet<>();
    private boolean established;
    private boolean closed;

    SegmentProfileAddHandler(IsolatedSegmentStorage storage, TLSContextFactory tls, String trustDomain,
                             ProfileHello.Server hello, int maxEntryBytes, int maxPending) {
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
                            + 36 + ProfileHello.MAX_BOOKIE_ID_BYTES)
                    : ADD_PREFIX_BYTES + RESPONSE_BYTES;
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
                tls.authenticatedProfilePrincipal(ctx.pipeline().get(SslHandler.class), trustDomain);
                // The decoder validated protocol range and the canonical client capability vector.
                // This slice advertises no optional capabilities; it cannot enable recovery/range/control.
                ProfileInboundPipeline.markEstablished(ctx.pipeline());
                established = true;
                byte[] body = hello.encode();
                write(ctx, request, null, body);
            } else if (!established) {
                ctx.close();
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
        pendingRequest.storageRequest = new SegmentDataPhysicalDriver.NormalRequest((result, failure) -> {
            if (pendingRequest.finished.get()) {
                return;
            }
            ProfileStatus response = resultStatus(result, failure);
            if (!pendingRequest.beginDelivery()) {
                return;
            }
            try {
                ctx.executor().execute(() -> {
                    try {
                        if (!pendingRequest.finished.get()) {
                            write(ctx, pendingRequest, response, null);
                        }
                    } catch (RuntimeException | Error failedResponse) {
                        finish(pendingRequest);
                        ctx.close();
                        throw failedResponse;
                    } finally {
                        pendingRequest.endDelivery();
                    }
                });
            } catch (RejectedExecutionException shutdown) {
                // No original frame view is used after submitProfileNormal's synchronous copy.
                pendingRequest.endDelivery();
                pendingRequest.release();
                ctx.close();
            }
        });
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
        ByteBuf response = ctx.alloc().buffer(bytes, bytes);
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
            if (!written.isSuccess()) {
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
        private ScheduledFuture<?> deadline;
        private SegmentDataPhysicalDriver.NormalRequest storageRequest;

        private Pending(ProfileInboundFrame frame, ProfileInboundPipeline.ResponseCredit credit) {
            this.frame = frame;
            this.credit = credit;
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
            }
            returnResourcesIfTerminal();
        }

        private void returnResourcesIfTerminal() {
            // Removing the application handler can precede the downstream write's actual completion.
            // Cancellation stops responses, but the write promise still owns the charged response workspace.
            if (finished.get() && !writing && !delivering && resourcesReturned.compareAndSet(false, true)) {
                try {
                    frame.close();
                } finally {
                    credit.close();
                }
            }
        }
    }
}

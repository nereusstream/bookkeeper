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
package org.apache.bookkeeper.proto.profile;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;

/** Charged plaintext framing after TLS, sharing the connection ledger with its backing allocator. */
final class ProfileReceiveDecoder extends ChannelInboundHandlerAdapter {
    private enum ConnectionPurpose { UNCLASSIFIED, DATA, CONTROL_PENDING, CONTROL }

    private final class HeldCredit implements ProfileInboundPipeline.ResponseCredit {
        private final ProfileReceiveDecoder receiver = ProfileReceiveDecoder.this;
        private final ProfileReceiveBudget.Credit shared;
        private final AtomicBoolean closed = new AtomicBoolean();
        private boolean backingOwned;

        private HeldCredit(ProfileReceiveBudget.Credit shared) {
            this.shared = shared;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                throw new IllegalStateException("Connection receive credit returned twice");
            }
            if (!backingOwned) {
                shared.close();
            }
        }
    }

    private static final int FIXED_HEADER_BYTES = ProfileFrameCodec.OUTER_LENGTH_PREFIX
            + ProfileFrameHeader.ENCODED_LENGTH;
    private static final long HELLO_TIMEOUT_MILLIS = 10_000;
    private static final long FIRST_REQUEST_TIMEOUT_MILLIS = 30_000;
    private final ProfileReceiveBudget budget;
    private final ProfileReceiveBudget.ConnectionPermit connectionPermit;
    private final int connectionBaseBytes;
    private int maxNormalBodyBytes = ProfileFrameCodec.ABSOLUTE_FRAME_MAX;
    private int maxRecoveryBodyBytes = ProfileFrameCodec.ABSOLUTE_FRAME_MAX;
    private final byte[] fixedHeader = new byte[FIXED_HEADER_BYTES];
    private ChannelHandlerContext context;
    private HeldCredit baseCredit;
    private HeldCredit frameCredit;
    private HeldCredit pendingControlCredit;
    private ProfileInboundFrame pendingControlFrame;
    private ScheduledFuture<?> stageDeadline;
    private ByteBuf frame;
    private ProfileFrameHeader frameHeader;
    private int headerRead;
    private boolean initialControlBounds;
    private boolean helloDelivered;
    private boolean firstRequestDelivered;
    private boolean terminated;
    private boolean tlsReady;
    private ProfileReceiveBudget.Lane lane = ProfileReceiveBudget.Lane.UNCLASSIFIED;
    private ConnectionPurpose purpose = ConnectionPurpose.UNCLASSIFIED;
    private ProfileFrameCodec.DecodePhase phase = ProfileFrameCodec.DecodePhase.PRE_HELLO;

    ProfileReceiveDecoder(ProfileReceiveBudget budget, ProfileReceiveBudget.ConnectionPermit connectionPermit,
                          int connectionLimit, int connectionBaseBytes) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.connectionPermit = Objects.requireNonNull(connectionPermit, "connectionPermit");
        if (connectionBaseBytes < FIXED_HEADER_BYTES || connectionLimit <= connectionBaseBytes) {
            throw new IllegalArgumentException("Invalid fixed connection or receive limit");
        }
        connectionPermit.limit(connectionLimit);
        this.connectionBaseBytes = connectionBaseBytes;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        try {
            baseCredit = charge(connectionBaseBytes);
        } catch (IOException failure) {
            ctx.close();
            throw failure;
        }
    }

    /** Called only after the downstream HELLO handler accepts authentication and capabilities. */
    void markEstablished() {
        if (terminated || context == null || !context.executor().inEventLoop() || !helloDelivered
                || phase != ProfileFrameCodec.DecodePhase.PRE_HELLO) {
            throw new IllegalStateException("HELLO was not accepted on this connection");
        }
        phase = ProfileFrameCodec.DecodePhase.ESTABLISHED;
        if (tlsReady) {
            armDeadline(FIRST_REQUEST_TIMEOUT_MILLIS);
        }
    }

    ProfileInboundPipeline.ResponseCredit reserveResponse(int bytes) throws IOException {
        if (terminated || context == null || !context.executor().inEventLoop()) {
            throw new IOException("Profile response requires a live receiving event loop");
        }
        return charge(bytes);
    }

    ByteBuf allocateResponse(ProfileInboundPipeline.ResponseCredit workspace, int bytes) {
        if (!(workspace instanceof HeldCredit held) || held.receiver != this || held.closed.get()
                || held.backingOwned || context == null || !context.executor().inEventLoop() || terminated) {
            throw new IllegalStateException("Response workspace is not owned by this receiving connection");
        }
        if (!(context.alloc() instanceof ProfileByteBufAllocator allocator)) {
            return context.alloc().buffer(bytes, bytes);
        }
        ProfileReceiveBudget.Credit backing = held.shared.split(bytes);
        try {
            return allocator.adopt(backing, bytes);
        } catch (RuntimeException | Error failure) {
            backing.close();
            throw failure;
        }
    }

    void limitInitialControlBodies() {
        if (context != null) {
            throw new IllegalStateException("Control bounds must precede decoder installation");
        }
        initialControlBounds = true;
    }

    void limitNormalBody(int bytes) {
        if (context != null || bytes < 100 || bytes > ProfileFrameCodec.ABSOLUTE_FRAME_MAX) {
            throw new IllegalArgumentException("Normal body bound must be set before installing the decoder");
        }
        maxNormalBodyBytes = bytes;
    }

    void limitRecoveryBody(int bytes) {
        if (context != null || bytes < 144 || bytes > ProfileFrameCodec.ABSOLUTE_FRAME_MAX) {
            throw new IllegalArgumentException("Recovery body bound must be set before installing the decoder");
        }
        maxRecoveryBodyBytes = bytes;
    }

    /** Called by the downstream operation handler after it validates and authorizes the first control request. */
    void markControlAuthorized(ProfileInboundFrame validatedFrame) throws IOException {
        markControlAuthorized(validatedFrame, null);
    }

    void markControlAuthorized(ProfileInboundFrame validatedFrame,
                               ProfileInboundPipeline.ResponseCredit workspace) throws IOException {
        if (terminated || context == null || !context.executor().inEventLoop()
                || purpose != ConnectionPurpose.CONTROL_PENDING || pendingControlCredit == null
                || pendingControlFrame != validatedFrame) {
            throw new IllegalStateException("No validated control request on this connection");
        }
        final boolean transferred;
        try {
            if (workspace == null) {
                transferred = budget.transfer(ProfileReceiveBudget.Lane.CONTROL,
                        connectionPermit, baseCredit.shared, pendingControlCredit.shared);
            } else if (workspace instanceof HeldCredit held && held.receiver == this && !held.closed.get()) {
                transferred = budget.transfer(ProfileReceiveBudget.Lane.CONTROL,
                        connectionPermit, baseCredit.shared, pendingControlCredit.shared, held.shared);
            } else {
                throw new IllegalStateException("Control workspace is not owned by this receiving connection");
            }
        } catch (IllegalStateException failure) {
            context.close();
            throw failure;
        }
        if (!transferred) {
            context.close();
            throw new IOException("Bookie Profile control receive capacity exhausted");
        }
        pendingControlCredit = null;
        pendingControlFrame = null;
        lane = ProfileReceiveBudget.Lane.CONTROL;
        purpose = ConnectionPurpose.CONTROL;
        cancelDeadline();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof SslHandshakeCompletionEvent handshake) {
            if (!handshake.isSuccess()) {
                ctx.close();
                return;
            }
            tlsReady = true;
            armDeadline(HELLO_TIMEOUT_MILLIS);
        }
        ctx.fireUserEventTriggered(event);
    }

    private void armDeadline(long delayMillis) {
        cancelDeadline();
        stageDeadline = context.executor().schedule(() -> {
            if (!terminated) {
                context.close();
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void cancelDeadline() {
        if (stageDeadline != null) {
            stageDeadline.cancel(false);
            stageDeadline = null;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        if (!(message instanceof ByteBuf input)) {
            ReferenceCountUtil.release(message);
            ctx.close();
            throw new IOException("Profile receiver requires plaintext ByteBuf input");
        }
        HeldCredit inbound = null;
        try {
            if (!(ctx.alloc() instanceof ProfileByteBufAllocator allocator) || !allocator.owns(input)) {
                inbound = charge(input.readableBytes());
            }
            decode(ctx, input);
        } catch (Exception | Error failure) {
            discardPartial();
            ctx.close();
            throw failure;
        } finally {
            input.release();
            if (inbound != null) {
                inbound.close();
            }
        }
    }

    private void decode(ChannelHandlerContext ctx, ByteBuf input) throws IOException {
        while (input.isReadable()) {
            if (terminated || !ctx.channel().isActive()) {
                return;
            }
            if (frame == null) {
                if (firstRequestDelivered && purpose == ConnectionPurpose.CONTROL_PENDING) {
                    throw new IOException("Profile control request is not authorized yet");
                }
                if (headerRead == 0 && firstRequestDelivered
                        && (purpose == ConnectionPurpose.DATA || purpose == ConnectionPurpose.CONTROL)) {
                    armDeadline(FIRST_REQUEST_TIMEOUT_MILLIS);
                }
                int copy = Math.min(input.readableBytes(), FIXED_HEADER_BYTES - headerRead);
                input.readBytes(fixedHeader, headerRead, copy);
                headerRead += copy;
                if (headerRead < FIXED_HEADER_BYTES) {
                    return;
                }
                ProfileFrameCodec.InspectedHeader inspected = ProfileFrameCodec.inspectHeader(fixedHeader, phase);
                validatePurpose(inspected.header());
                classify(inspected.header());
                HeldCredit reserved = charge(inspected.frameBytes());
                ByteBuf allocated = null;
                try {
                    if (ctx.alloc() instanceof ProfileByteBufAllocator allocator) {
                        allocated = allocator.adopt(reserved.shared, inspected.frameBytes());
                        reserved.backingOwned = true;
                    } else {
                        allocated = ctx.alloc().buffer(inspected.frameBytes(), inspected.frameBytes());
                    }
                    allocated.writeBytes(fixedHeader);
                } catch (RuntimeException | Error failure) {
                    if (allocated != null) {
                        allocated.release();
                    }
                    reserved.close();
                    throw failure;
                }
                frameCredit = reserved;
                frame = allocated;
                frameHeader = inspected.header();
            }
            int copy = Math.min(input.readableBytes(), frame.writableBytes());
            frame.writeBytes(input, copy);
            if (frame.isWritable()) {
                return;
            }
            ProfileFrameHeader header = frameHeader;
            if (header.operation() == ProfileOperation.HELLO) {
                HeldCredit helloCopy = charge(header.bodyLength());
                try {
                    byte[] body = new byte[header.bodyLength()];
                    frame.getBytes(FIXED_HEADER_BYTES, body);
                    ProfileHello.decodeClient(body);
                } finally {
                    helloCopy.close();
                }
            }
            ProfileInboundFrame completed = new ProfileInboundFrame(header, frame, frameCredit::close);
            if (purpose == ConnectionPurpose.CONTROL_PENDING) {
                pendingControlCredit = frameCredit;
                pendingControlFrame = completed;
            }
            frame = null;
            frameCredit = null;
            frameHeader = null;
            headerRead = 0;
            if (header.operation() == ProfileOperation.HELLO) {
                helloDelivered = true;
            } else {
                firstRequestDelivered = true;
                if (purpose == ConnectionPurpose.DATA || purpose == ConnectionPurpose.CONTROL) {
                    cancelDeadline();
                }
            }
            ctx.fireChannelRead(completed);
            if (terminated || !ctx.channel().isActive()) {
                return;
            }
        }
    }

    private void validatePurpose(ProfileFrameHeader header) throws IOException {
        ProfileOperation operation = header.operation();
        if (header.isResponse() || (operation == ProfileOperation.HELLO && header.flags() != 0)
                || (helloDelivered && phase == ProfileFrameCodec.DecodePhase.PRE_HELLO)
                || (phase == ProfileFrameCodec.DecodePhase.ESTABLISHED
                && operation == ProfileOperation.HELLO)
                // The frozen fence request body is exactly 100 bytes; reject a larger claim before reserving it.
                || (operation == ProfileOperation.FENCE_LEDGER && header.bodyLength() != 100)
                || (operation == ProfileOperation.READ_ENTRY && header.bodyLength() != 72)
                || (operation == ProfileOperation.READ_LAC && header.bodyLength() != 60)
                || (operation == ProfileOperation.ADD_NORMAL && header.bodyLength() > maxNormalBodyBytes)
                || (operation == ProfileOperation.ADD_RECOVERY && header.bodyLength() > maxRecoveryBodyBytes)
                || (initialControlBounds && ProfileInitialControlCodec.supported(operation)
                    && (header.bodyLength() < (operation == ProfileOperation.INSTALL
                            ? ProfileInitialControlCodec.MIN_INSTALL_BYTES
                            : ProfileInitialControlCodec.COMMON_BYTES + 1
                                + (operation == ProfileOperation.RECOVERY_GRANT
                                    ? ProfileInitialControlCodec.RECOVERY_BINDING_BYTES : 0))
                        || header.bodyLength() > (operation == ProfileOperation.INSTALL
                            ? ProfileInitialControlCodec.MAX_INSTALL_BYTES
                            : operation == ProfileOperation.RECOVERY_GRANT
                                ? ProfileInitialControlCodec.MAX_GRANT_BYTES
                                : ProfileInitialControlCodec.MAX_ACTIVATE_BYTES)))
                || (operation.isReservedDisabled() && header.bodyLength() != 0)) {
            throw new IOException("Invalid Profile request header");
        }
        if (purpose == ConnectionPurpose.CONTROL
                && operation != ProfileOperation.HELLO && !isControlConnectionOperation(operation)) {
            throw new IOException("DATA operation cannot consume reserved control receive capacity");
        }
        if (purpose == ConnectionPurpose.DATA && isControlConnectionOperation(operation)) {
            throw new IOException("Control operation requires a control connection");
        }
        if (purpose == ConnectionPurpose.CONTROL_PENDING) {
            throw new IOException("Profile connection purpose is not established");
        }
    }

    private void classify(ProfileFrameHeader header) throws IOException {
        if (purpose != ConnectionPurpose.UNCLASSIFIED || header.operation() == ProfileOperation.HELLO) {
            return;
        }
        ProfileOperation operation = header.operation();
        if (isControlConnectionOperation(operation)) {
            purpose = ConnectionPurpose.CONTROL_PENDING;
            return;
        }
        if (!budget.transfer(ProfileReceiveBudget.Lane.DATA, connectionPermit, baseCredit.shared)) {
            throw new IOException("Bookie Profile DATA receive capacity exhausted");
        }
        lane = ProfileReceiveBudget.Lane.DATA;
        purpose = ConnectionPurpose.DATA;
    }

    private static boolean isControlConnectionOperation(ProfileOperation operation) {
        return operation == ProfileOperation.FENCE_LEDGER || operation.isControl();
    }

    private HeldCredit charge(int bytes) throws IOException {
        ProfileReceiveBudget.Credit shared = connectionPermit.reserve(bytes);
        if (shared == null) {
            throw new IOException("Connection or Bookie Profile receive capacity exhausted");
        }
        return new HeldCredit(shared);
    }

    private void discardPartial() {
        if (frame != null) {
            frame.release();
            frame = null;
        }
        if (frameCredit != null) {
            frameCredit.close();
            frameCredit = null;
        }
        frameHeader = null;
        headerRead = 0;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        terminated = true;
        cancelDeadline();
        pendingControlCredit = null;
        pendingControlFrame = null;
        discardPartial();
        releaseBase();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        terminated = true;
        cancelDeadline();
        pendingControlCredit = null;
        pendingControlFrame = null;
        discardPartial();
        releaseBase();
        context = null;
    }

    private void releaseBase() {
        if (baseCredit != null) {
            baseCredit.close();
            baseCredit = null;
        }
        connectionPermit.close();
    }
}

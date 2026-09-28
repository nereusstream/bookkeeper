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
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;

/** Charged plaintext framing after TLS. The eventual listener must also account for TLS/socket buffers. */
final class ProfileReceiveDecoder extends ChannelInboundHandlerAdapter {
    enum ConnectionPurpose { DATA, CONTROL }

    private final class HeldCredit implements AutoCloseable {
        private final int bytes;
        private final ProfileReceiveBudget.Credit shared;
        private final AtomicBoolean closed = new AtomicBoolean();

        private HeldCredit(int bytes, ProfileReceiveBudget.Credit shared) {
            this.bytes = bytes;
            this.shared = shared;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                throw new IllegalStateException("Connection receive credit returned twice");
            }
            try {
                shared.close();
            } finally {
                connectionBytes.addAndGet(-bytes);
            }
        }
    }

    private static final int FIXED_HEADER_BYTES = ProfileFrameCodec.OUTER_LENGTH_PREFIX
            + ProfileFrameHeader.ENCODED_LENGTH;
    private final ProfileReceiveBudget budget;
    private final ProfileReceiveBudget.Lane lane;
    private final ConnectionPurpose purpose;
    private final int connectionLimit;
    private final int connectionBaseBytes;
    private final AtomicLong connectionBytes = new AtomicLong();
    private final byte[] fixedHeader = new byte[FIXED_HEADER_BYTES];
    private ChannelHandlerContext context;
    private HeldCredit baseCredit;
    private HeldCredit frameCredit;
    private ByteBuf frame;
    private ProfileFrameHeader frameHeader;
    private int headerRead;
    private boolean helloDelivered;
    private ProfileFrameCodec.DecodePhase phase = ProfileFrameCodec.DecodePhase.PRE_HELLO;

    ProfileReceiveDecoder(ProfileReceiveBudget budget, ConnectionPurpose purpose, int connectionLimit,
                          int connectionBaseBytes) {
        this.budget = Objects.requireNonNull(budget, "budget");
        this.purpose = Objects.requireNonNull(purpose, "purpose");
        lane = purpose == ConnectionPurpose.DATA ? ProfileReceiveBudget.Lane.DATA
                : ProfileReceiveBudget.Lane.CONTROL;
        if (connectionBaseBytes < FIXED_HEADER_BYTES || connectionLimit <= connectionBaseBytes) {
            throw new IllegalArgumentException("Invalid fixed connection or receive limit");
        }
        this.connectionLimit = connectionLimit;
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
        if (context == null || !context.executor().inEventLoop() || !helloDelivered
                || phase != ProfileFrameCodec.DecodePhase.PRE_HELLO) {
            throw new IllegalStateException("HELLO was not accepted on this connection");
        }
        phase = ProfileFrameCodec.DecodePhase.ESTABLISHED;
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
            inbound = charge(input.readableBytes());
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
            if (frame == null) {
                int copy = Math.min(input.readableBytes(), FIXED_HEADER_BYTES - headerRead);
                input.readBytes(fixedHeader, headerRead, copy);
                headerRead += copy;
                if (headerRead < FIXED_HEADER_BYTES) {
                    return;
                }
                ProfileFrameCodec.InspectedHeader inspected = ProfileFrameCodec.inspectHeader(fixedHeader, phase);
                validatePurpose(inspected.header());
                HeldCredit reserved = charge(inspected.frameBytes());
                ByteBuf allocated = null;
                try {
                    allocated = ctx.alloc().buffer(inspected.frameBytes(), inspected.frameBytes());
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
            frame = null;
            frameCredit = null;
            frameHeader = null;
            headerRead = 0;
            if (header.operation() == ProfileOperation.HELLO) {
                helloDelivered = true;
            }
            ctx.fireChannelRead(completed);
        }
    }

    private void validatePurpose(ProfileFrameHeader header) throws IOException {
        ProfileOperation operation = header.operation();
        if (header.isResponse() || (operation == ProfileOperation.HELLO && header.flags() != 0)
                || (helloDelivered && phase == ProfileFrameCodec.DecodePhase.PRE_HELLO)
                || (phase == ProfileFrameCodec.DecodePhase.ESTABLISHED
                && operation == ProfileOperation.HELLO)
                || (operation.isReservedDisabled() && header.bodyLength() != 0)) {
            throw new IOException("Invalid Profile request header");
        }
        if (purpose == ConnectionPurpose.CONTROL
                && operation != ProfileOperation.HELLO && operation != ProfileOperation.FENCE_LEDGER
                && !operation.isControl()) {
            throw new IOException("DATA operation cannot consume reserved control receive capacity");
        }
        if (purpose == ConnectionPurpose.DATA && operation.isControl()) {
            throw new IOException("Control operation requires a control connection");
        }
    }

    private HeldCredit charge(int bytes) throws IOException {
        for (;;) {
            long before = connectionBytes.get();
            if (bytes > connectionLimit - before) {
                throw new IOException("Connection Profile receive capacity exhausted");
            }
            if (connectionBytes.compareAndSet(before, before + bytes)) {
                break;
            }
        }
        ProfileReceiveBudget.Credit shared = budget.reserve(lane, bytes);
        if (shared == null) {
            connectionBytes.addAndGet(-bytes);
            throw new IOException("Bookie Profile receive capacity exhausted");
        }
        return new HeldCredit(bytes, shared);
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
        discardPartial();
        releaseBase();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        discardPartial();
        releaseBase();
        context = null;
    }

    private void releaseBase() {
        if (baseCredit != null) {
            baseCredit.close();
            baseCredit = null;
        }
    }
}

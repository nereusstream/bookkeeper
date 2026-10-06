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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Installs the isolated Profile receive path after obtaining its pre-TLS connection slot. */
public final class ProfileInboundPipeline {
    private static final long TLS_HANDSHAKE_TIMEOUT_MILLIS = 10_000;

    private ProfileInboundPipeline() {
    }

    public static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           ChannelInboundHandler application) throws SecurityException {
        return install(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                ProfileFrameCodec.ABSOLUTE_FRAME_MAX, application);
    }

    /** An installed normal-entry limit is checked from the fixed header, before body allocation. */
    public static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           int maxNormalBodyBytes, ChannelInboundHandler application) throws SecurityException {
        return install(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                maxNormalBodyBytes, ProfileFrameCodec.ABSOLUTE_FRAME_MAX, application);
    }

    /** Installed normal and recovery entry bounds precede body allocation and receive-lane transfer. */
    public static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           int maxNormalBodyBytes, int maxRecoveryBodyBytes,
                           ChannelInboundHandler application) throws SecurityException {
        return install(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                maxNormalBodyBytes, maxRecoveryBodyBytes, application, false);
    }

    /** Initial-control body caps are enabled only by an explicitly opted-in application. */
    public static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           int maxNormalBodyBytes, int maxRecoveryBodyBytes,
                           ChannelInboundHandler application, boolean initialControl)
            throws SecurityException {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(application, "application");
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        if (permit == null) {
            pipeline.channel().close();
            return false;
        }
        try {
            ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(
                    budget, permit, connectionLimit, connectionBaseBytes);
            decoder.limitNormalBody(maxNormalBodyBytes);
            decoder.limitRecoveryBody(maxRecoveryBodyBytes);
            if (initialControl) {
                decoder.limitInitialControlBodies();
            }
            pipeline.channel().config().setAllocator(new ProfileByteBufAllocator(permit));
            pipeline.channel().closeFuture().addListener(closed -> permit.close());
            SslHandler ssl = tls.newProfileServerTLSHandler();
            ssl.setHandshakeTimeoutMillis(TLS_HANDSHAKE_TIMEOUT_MILLIS);
            pipeline.addLast("profileTls", ssl);
            pipeline.addLast("profileReceive", decoder);
            pipeline.addLast("profileApplication", application);
            return true;
        } catch (SecurityException | RuntimeException | Error failure) {
            pipeline.channel().close();
            permit.close();
            throw failure;
        }
    }

    /** The operation handler has accepted this connection's authenticated, validated HELLO. */
    public static void markEstablished(ChannelPipeline pipeline) {
        pipeline.get(ProfileReceiveDecoder.class).markEstablished();
    }

    /** Move this authorized request and its still-held workspace together; no effect precedes a failed move. */
    public static void markControlAuthorized(ChannelPipeline pipeline, ProfileInboundFrame frame,
                                             ResponseCredit workspace) throws IOException {
        pipeline.get(ProfileReceiveDecoder.class).markControlAuthorized(frame, workspace);
    }

    @FunctionalInterface
    public interface ResponseCredit extends AutoCloseable {
        @Override
        void close();
    }

    /** Charge response/parse buffers against the existing connection and Bookie ledger until their final use. */
    public static ResponseCredit reserveResponse(ChannelPipeline pipeline, int bytes) throws IOException {
        return pipeline.get(ProfileReceiveDecoder.class).reserveResponse(bytes);
    }

    /** Transfer the response's exact capacity from its precharged workspace to the final backing lifetime. */
    public static ByteBuf allocateResponse(ChannelPipeline pipeline, ResponseCredit workspace, int bytes) {
        return pipeline.get(ProfileReceiveDecoder.class).allocateResponse(workspace, bytes);
    }

    /** Share a Profile source's ledger; other callers retain the original independent unpooled copy contract. */
    public static ByteBufAllocator inputCopyAllocator(ByteBuf source) {
        ByteBufAllocator allocator = Objects.requireNonNull(source, "source").alloc();
        return allocator instanceof ProfileByteBufAllocator ? allocator : UnpooledByteBufAllocator.DEFAULT;
    }
}

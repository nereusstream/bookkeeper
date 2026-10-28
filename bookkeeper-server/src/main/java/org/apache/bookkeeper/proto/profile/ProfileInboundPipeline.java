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
import io.netty.channel.AbstractEventLoopGroup;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Installs the isolated Profile receive path after obtaining its pre-TLS connection slot. */
public final class ProfileInboundPipeline {
    private static final long TLS_HANDSHAKE_TIMEOUT_MILLIS = 10_000;
    private static final AttributeKey<ConnectionAdmission> ADMISSION =
            AttributeKey.valueOf(ProfileInboundPipeline.class, "admission");

    private ProfileInboundPipeline() {
    }

    /** Admit accepted sockets before worker registration; the caller still owns the supplied event loops. */
    public static EventLoopGroup admittingGroup(EventLoopGroup worker, ProfileReceiveBudget budget,
                                                int connectionLimit, int connectionBaseBytes) {
        Objects.requireNonNull(worker, "worker");
        Objects.requireNonNull(budget, "budget");
        if (connectionBaseBytes < ProfileFrameCodec.OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH
                || connectionLimit <= connectionBaseBytes) {
            throw new IllegalArgumentException("Invalid fixed connection or receive limit");
        }
        return new AdmittingGroup(worker, budget, connectionLimit, connectionBaseBytes);
    }

    private static final class ConnectionAdmission implements AutoCloseable {
        private final ProfileReceiveBudget budget;
        private final ProfileReceiveBudget.ConnectionPermit permit;
        private final int connectionLimit;
        private final int connectionBaseBytes;
        private ProfileReceiveBudget.Credit base;

        private ConnectionAdmission(ProfileReceiveBudget budget, ProfileReceiveBudget.ConnectionPermit permit,
                                    int connectionLimit, int connectionBaseBytes,
                                    ProfileReceiveBudget.Credit base) {
            this.budget = budget;
            this.permit = permit;
            this.connectionLimit = connectionLimit;
            this.connectionBaseBytes = connectionBaseBytes;
            this.base = base;
        }

        synchronized ProfileReceiveBudget.Credit takeBase() {
            if (base == null) {
                throw new IllegalStateException("Connection admission was already consumed or closed");
            }
            var result = base;
            base = null;
            return result;
        }

        @Override
        public synchronized void close() {
            if (base != null) {
                base.close();
                base = null;
            }
            permit.close();
        }
    }

    private static ConnectionAdmission admit(Channel channel, ProfileReceiveBudget budget,
                                              int connectionLimit, int connectionBaseBytes) {
        ConnectionAdmission existing = channel.attr(ADMISSION).get();
        if (existing != null) {
            if (existing.budget != budget || existing.connectionLimit != connectionLimit
                    || existing.connectionBaseBytes != connectionBaseBytes) {
                throw new IllegalStateException("Accepted connection changed its receive budget");
            }
            return existing;
        }
        var permit = budget.admitUnclassified();
        if (permit == null) {
            return null;
        }
        ProfileReceiveBudget.Credit base = null;
        ConnectionAdmission admission = null;
        try {
            permit.limit(connectionLimit);
            base = permit.reserve(connectionBaseBytes);
            if (base == null) {
                permit.close();
                return null;
            }
            admission = new ConnectionAdmission(budget, permit, connectionLimit, connectionBaseBytes, base);
            channel.attr(ADMISSION).set(admission);
            channel.config().setAllocator(new ProfileByteBufAllocator(permit));
            ConnectionAdmission owned = admission;
            channel.closeFuture().addListener(closed -> owned.close());
            return admission;
        } catch (RuntimeException | Error failure) {
            if (admission != null) {
                admission.close();
            } else if (base != null) {
                base.close();
            }
            permit.close();
            throw failure;
        }
    }

    /** Only registration is intercepted; scheduling and lifecycle remain on the borrowed group. */
    private static final class AdmittingGroup extends AbstractEventLoopGroup {
        private final EventLoopGroup worker;
        private final ProfileReceiveBudget budget;
        private final int connectionLimit;
        private final int connectionBaseBytes;

        private AdmittingGroup(EventLoopGroup worker, ProfileReceiveBudget budget,
                               int connectionLimit, int connectionBaseBytes) {
            this.worker = worker;
            this.budget = budget;
            this.connectionLimit = connectionLimit;
            this.connectionBaseBytes = connectionBaseBytes;
        }

        @Override
        public ChannelFuture register(Channel channel) {
            // A refused socket must not enqueue listener notification on a blocked/terminated worker.
            return register(channel, new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE));
        }

        @Override
        public ChannelFuture register(ChannelPromise promise) {
            return register(promise.channel(), promise);
        }

        @Override
        @SuppressWarnings("deprecation")
        public ChannelFuture register(Channel channel, ChannelPromise promise) {
            try {
                ConnectionAdmission admission = admit(channel, budget, connectionLimit, connectionBaseBytes);
                if (admission == null) {
                    channel.unsafe().closeForcibly();
                    return promise.setFailure(new IOException("Profile connection admission exhausted or retired"));
                }
                promise.addListener(registered -> {
                    if (!registered.isSuccess()) {
                        channel.unsafe().closeForcibly();
                        if (!channel.isOpen()) {
                            admission.close();
                        }
                    }
                });
                worker.register(channel, promise);
            } catch (RuntimeException | Error failure) {
                channel.unsafe().closeForcibly();
                ConnectionAdmission admission = channel.attr(ADMISSION).get();
                if (admission != null && !channel.isOpen()) {
                    admission.close();
                }
                promise.tryFailure(failure);
            }
            return promise;
        }

        @Override
        public EventLoop next() {
            return worker.next();
        }

        @Override
        public Iterator<EventExecutor> iterator() {
            return worker.iterator();
        }

        @Override
        public boolean isShuttingDown() {
            return worker.isShuttingDown();
        }

        @Override
        public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
            return worker.shutdownGracefully(quietPeriod, timeout, unit);
        }

        @Override
        public Future<?> terminationFuture() {
            return worker.terminationFuture();
        }

        @Override
        @SuppressWarnings("deprecation")
        public void shutdown() {
            worker.shutdown();
        }

        @Override
        public boolean isShutdown() {
            return worker.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return worker.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return worker.awaitTermination(timeout, unit);
        }
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
        return install(pipeline, tls, budget, connectionLimit, connectionBaseBytes,
                maxNormalBodyBytes, maxRecoveryBodyBytes, application, initialControl, 0);
    }

    /** The storage application supplies its computed frame metadata price before any fixed-header parse. */
    public static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           int maxNormalBodyBytes, int maxRecoveryBodyBytes,
                           ChannelInboundHandler application, boolean initialControl, int frameMetadataBytes)
            throws SecurityException {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(application, "application");
        if (!pipeline.channel().isOpen()) {
            return false;
        }
        ConnectionAdmission admission = null;
        ProfileReceiveBudget.Credit base = null;
        ProfileReceiveDecoder decoder = null;
        try {
            admission = admit(pipeline.channel(), budget, connectionLimit, connectionBaseBytes);
            if (admission == null) {
                pipeline.channel().close();
                return false;
            }
            base = admission.takeBase();
            decoder = new ProfileReceiveDecoder(
                    budget, admission.permit, connectionLimit, connectionBaseBytes, frameMetadataBytes, base);
            base = null;
            ProfileReceiveDecoder receiver = decoder;
            pipeline.channel().closeFuture().addListener(closed -> receiver.releaseBase());
            decoder.limitNormalBody(maxNormalBodyBytes);
            decoder.limitRecoveryBody(maxRecoveryBodyBytes);
            if (initialControl) {
                decoder.limitInitialControlBodies();
            }
            SslHandler ssl = tls.newProfileServerTLSHandler();
            ssl.setHandshakeTimeoutMillis(TLS_HANDSHAKE_TIMEOUT_MILLIS);
            pipeline.addLast("profileTls", ssl);
            pipeline.addLast("profileReceive", decoder);
            pipeline.addLast("profileApplication", application);
            return true;
        } catch (SecurityException | RuntimeException | Error failure) {
            pipeline.channel().close();
            if (base != null) {
                base.close();
            }
            if (decoder != null) {
                decoder.releaseBase();
            }
            if (admission != null) {
                admission.close();
            }
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

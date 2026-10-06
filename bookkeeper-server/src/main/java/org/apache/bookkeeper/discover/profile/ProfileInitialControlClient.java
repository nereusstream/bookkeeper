/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.discover.profile;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLParameters;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;

/**
 * Disposable, request-correlated nonblocking initial-control client. Caller owns TLS and event loops;
 * this client owns one channel and bounded queued/pending requests, no retry, reconnect or new executor.
 * Timeout/disconnect are exceptional UNKNOWN, never a fabricated durable result or cancellation of server I/O.
 */
public final class ProfileInitialControlClient implements ProfileInitialMetadataCoordinator.InstallClient,
        ProfileInitialLifecycleCoordinator.ActivationClient, AutoCloseable {
    private static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(
            ProfileInitialControlCodec.CAPABILITY_ID, ProfileInitialControlCodec.SCHEMA);
    private static final int MAX_RESPONSE_FRAME = 36 + 36 + ProfileHello.MAX_BOOKIE_ID_BYTES
            + 8 * ProfileHello.MAX_CAPABILITIES;
    private final ProfileHello.Server expected;
    private final Semaphore slots;
    private final long timeoutNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<ProfileInitialControlClient> ready = new CompletableFuture<>();
    private final Map<Long, Pending> pending = new HashMap<>(); // Event-loop confined.
    private volatile Channel channel;
    private ProfileHello.Server hello;
    private long nextId = 2;
    private io.netty.util.concurrent.ScheduledFuture<?> helloDeadline;

    private ProfileInitialControlClient(String bookie, byte[] incarnation, int maxPending, Duration timeout) {
        expected = new ProfileHello.Server(EngineProfile.SEGMENT_WAL_ENGINE.wireValue(), incarnation, 0,
                bookie, List.of());
        if (maxPending < 1 || maxPending > ProfileReservation.MAX_INITIAL_TARGETS
                || timeout.compareTo(Duration.ofMillis(1)) < 0 || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid bounded initial-control client settings");
        }
        slots = new Semaphore(maxPending);
        timeoutNanos = timeout.toNanos();
    }

    public static CompletableFuture<ProfileInitialControlClient> connect(EventLoopGroup group, SslContext tls,
            InetSocketAddress remote, String peerHostname, String expectedBookie, byte[] expectedIncarnation,
            int maxPending, Duration timeout) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(remote, "remote");
        Objects.requireNonNull(peerHostname, "peerHostname");
        if (!tls.isClient() || peerHostname.isBlank() || remote.isUnresolved()) {
            throw new IllegalArgumentException("Initial control requires client TLS, hostname and a resolved endpoint");
        }
        var client = new ProfileInitialControlClient(expectedBookie, expectedIncarnation, maxPending, timeout);
        client.ready.whenComplete((connected, failure) -> {
            if (failure != null) {
                client.close();
            }
        });
        new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(timeout.toMillis()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socket) {
                        client.channel = socket;
                        SslHandler ssl = tls.newHandler(socket.alloc(), peerHostname, remote.getPort());
                        ssl.engine().setEnabledProtocols(new String[] {"TLSv1.3"});
                        SSLParameters parameters = ssl.engine().getSSLParameters();
                        parameters.setEndpointIdentificationAlgorithm("HTTPS");
                        ssl.engine().setSSLParameters(parameters);
                        ssl.setHandshakeTimeoutMillis(timeout.toMillis());
                        socket.pipeline().addLast(ssl);
                        socket.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_RESPONSE_FRAME, 0, 4, 0, 0));
                        socket.pipeline().addLast(client.new Responses());
                        if (client.closed.get()) {
                            socket.close();
                        }
                    }
                }).connect(remote).addListener(connected -> {
                    if (!connected.isSuccess()) {
                        client.ready.completeExceptionally(new IOException("Initial control connection failed",
                                connected.cause()));
                    }
                });
        return client.ready;
    }

    public ProfileHello.Server hello() {
        if (!ready.isDone() || ready.isCompletedExceptionally()) {
            throw new IllegalStateException("Profile peer HELLO is not verified");
        }
        return hello;
    }

    @Override
    public CompletableFuture<ProfileControlResult> install(ProfileControlRequest request) {
        return send(request, ProfileControlOperation.INSTALL, ProfileOperation.INSTALL);
    }

    @Override
    public CompletableFuture<ProfileControlResult> activate(ProfileControlRequest request) {
        return send(request, ProfileControlOperation.ACTIVATE_INITIAL, ProfileOperation.ACTIVATE_INITIAL);
    }

    private CompletableFuture<ProfileControlResult> send(ProfileControlRequest request,
            ProfileControlOperation expectedOperation, ProfileOperation operation) {
        Objects.requireNonNull(request, "request");
        ProfileControlScope scope = request.scope();
        if (scope.operation() != expectedOperation || !scope.targetBookie().equals(hello().bookieId())
                || !Arrays.equals(scope.targetStorageIncarnation(), hello().storageIncarnation())) {
            throw new IllegalArgumentException("Initial request is not bound to the verified peer and operation");
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(unknown());
        }
        if (!slots.tryAcquire()) {
            return CompletableFuture.completedFuture(result(scope, Outcome.TRANSIENT_RECONCILING));
        }
        Pending held = new Pending(scope, operation);
        try {
            channel.eventLoop().execute(() -> {
                if (closed.get() || !channel.isActive() || held.reply.isCancelled()) {
                    held.writeDone = true;
                    held.fail(unknown());
                    return;
                }
                if (nextId == Long.MAX_VALUE) {
                    held.writeDone = true;
                    held.fail(unknown());
                    close();
                    return;
                }
                long id = nextId++;
                pending.put(id, held);
                try {
                    held.deadline = channel.eventLoop().schedule(this::close, timeoutNanos, TimeUnit.NANOSECONDS);
                    ByteBuf bytes = encode(operation, id, ProfileInitialControlCodec.encodeRequest(request));
                    held.plaintext = bytes.retainedDuplicate();
                    channel.writeAndFlush(bytes).addListener(written -> {
                        held.plaintext.setZero(0, held.plaintext.capacity());
                        held.plaintext.release();
                        held.plaintext = null;
                        held.writeDone = true;
                        held.releaseIfTerminal();
                        if (!written.isSuccess()) {
                            close();
                        }
                    });
                } catch (RuntimeException failure) {
                    held.writeDone = true;
                    pending.remove(id);
                    held.fail(unknown());
                    close();
                }
            });
        } catch (RuntimeException shutdown) {
            held.writeDone = true;
            held.fail(unknown());
            close();
        }
        // Cancelling this future stops waiting only. Pending/serialized bytes keep their real terminal lifetimes.
        return held.reply;
    }

    private ByteBuf encode(ProfileOperation operation, long id, byte[] body) {
        ByteBuf bytes = null;
        try {
            int length = 36 + body.length;
            bytes = channel.alloc().buffer(length, length);
            bytes.writeInt(length - 4).writeInt(ProfileFrameHeader.MAGIC)
                    .writeShort(ProfileFrameHeader.PROTOCOL_MAJOR).writeShort(ProfileFrameHeader.PROTOCOL_MINOR)
                    .writeShort(ProfileFrameHeader.ENCODED_LENGTH).writeShort(operation.wireValue())
                    .writeInt(0).writeLong(id).writeInt(body.length).writeInt(0).writeBytes(body);
            return bytes;
        } catch (RuntimeException | Error failure) {
            if (bytes != null) {
                bytes.setZero(0, bytes.capacity());
                bytes.release();
            }
            throw failure;
        } finally {
            Arrays.fill(body, (byte) 0);
        }
    }

    private final class Responses extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            helloDeadline = ctx.executor().schedule(ProfileInitialControlClient.this::close,
                    timeoutNanos, TimeUnit.NANOSECONDS);
            SslHandler ssl = ctx.pipeline().get(SslHandler.class);
            ssl.handshakeFuture().addListener(handshake -> {
                try {
                    if (!handshake.isSuccess() || closed.get()
                            || ssl.engine().getSession().getLocalCertificates() == null
                            || ssl.engine().getSession().getPeerCertificates().length == 0) {
                        close();
                        return;
                    }
                    ctx.writeAndFlush(encode(ProfileOperation.HELLO, 1,
                            new ProfileHello.Client(List.of(CAPABILITY)).encode())).addListener(written -> {
                        if (!written.isSuccess()) {
                            close();
                        }
                    });
                } catch (Exception invalidTls) {
                    close();
                }
            });
            ctx.fireChannelActive();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
            try {
                byte[] frameBytes = new byte[bytes.readableBytes()];
                bytes.getBytes(bytes.readerIndex(), frameBytes);
                ProfileFrame frame = ProfileFrameCodec.decode(frameBytes, hello == null
                        ? ProfileFrameCodec.DecodePhase.PRE_HELLO : ProfileFrameCodec.DecodePhase.ESTABLISHED);
                var header = frame.header();
                if (!header.isResponse() || (header.flags() & ProfileFrameHeader.END_OF_STREAM) != 0) {
                    throw invalidReply();
                }
                byte[] body = frame.body();
                if (hello == null) {
                    if (header.operation() != ProfileOperation.HELLO || header.requestId() != 1 || header.isError()) {
                        throw invalidReply();
                    }
                    ProfileHello.Server peer = ProfileHello.decodeServer(body);
                    if (peer.activeEngine() != expected.activeEngine() || !peer.bookieId().equals(expected.bookieId())
                            || !Arrays.equals(peer.storageIncarnation(), expected.storageIncarnation())
                            || !peer.capabilities().contains(CAPABILITY)) {
                        throw invalidReply();
                    }
                    hello = peer;
                    helloDeadline.cancel(false);
                    ready.complete(ProfileInitialControlClient.this);
                    return;
                }
                Pending held = pending.get(header.requestId());
                if (held == null || held.operation != header.operation()) {
                    throw invalidReply();
                }
                ProfileControlResult received;
                if (body.length == ProfileStatus.ENCODED_LENGTH) {
                    ProfileStatus status = ProfileStatus.decode(body);
                    Outcome refusal = switch (status.statusClass()) {
                        case BAD_REQUEST -> Outcome.BAD_REQUEST;
                        case UNAUTHORIZED -> Outcome.DENIED;
                        case TRANSIENT_UNAVAILABLE -> Outcome.TRANSIENT_RECONCILING;
                        case UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE -> Outcome.UNSUPPORTED;
                        default -> throw invalidReply();
                    };
                    if (!status.equals(ProfileInitialControlCodec.status(refusal))) {
                        throw invalidReply();
                    }
                    received = result(held.scope, refusal);
                } else {
                    received = ProfileInitialControlCodec.decodeReply(held.operation, body);
                }
                if (header.isError() != (ProfileInitialControlCodec.status(received.outcome()).statusClass()
                        != ProfileStatus.StatusClass.OK) || received.ledgerId() != held.scope.ledgerId()
                        || received.operation() != held.scope.operation()
                        || received.authorityGeneration() != held.scope.authorityGeneration()
                        || !received.operationIdHex().equals(HexFormat.of().formatHex(held.scope.operationId()))) {
                    throw invalidReply();
                }
                pending.remove(header.requestId());
                held.wireDone = true;
                held.deadline.cancel(false);
                held.releaseIfTerminal();
                held.reply.complete(received);
            } catch (RuntimeException invalid) {
                close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            closed.set(true);
            if (helloDeadline != null) {
                helloDeadline.cancel(false);
            }
            ready.completeExceptionally(unknown());
            for (Pending held : pending.values()) {
                held.fail(unknown());
            }
            pending.clear();
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            close();
        }
    }

    private final class Pending {
        private final ProfileControlScope scope;
        private final ProfileOperation operation;
        private final CompletableFuture<ProfileControlResult> reply = new CompletableFuture<>();
        private io.netty.util.concurrent.ScheduledFuture<?> deadline;
        private ByteBuf plaintext;
        private boolean writeDone;
        private boolean wireDone;
        private boolean released;

        private Pending(ProfileControlScope scope, ProfileOperation operation) {
            this.scope = scope;
            this.operation = operation;
        }

        private void fail(IOException failure) {
            wireDone = true;
            if (deadline != null) {
                deadline.cancel(false);
            }
            releaseIfTerminal();
            reply.completeExceptionally(failure);
        }

        private void releaseIfTerminal() {
            if (writeDone && wireDone && !released) {
                released = true;
                slots.release();
            }
        }
    }

    private static ProfileControlResult result(ProfileControlScope scope, Outcome outcome) {
        return new ProfileControlResult(outcome, scope.operation(), scope.ledgerId(),
                HexFormat.of().formatHex(scope.operationId()), scope.authorityGeneration(), 0, false);
    }

    private static IllegalArgumentException invalidReply() {
        return new IllegalArgumentException("Invalid or uncorrelated initial-control reply");
    }

    private static IOException unknown() {
        return new IOException("Initial-control result is unknown");
    }

    @Override
    public void close() {
        closed.set(true);
        Channel socket = channel;
        if (socket != null) {
            socket.close();
        }
    }
}

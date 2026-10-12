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
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.net.ssl.SSLParameters;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult.Outcome;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.common.profile.wire.ProfilePhysicalDeleteCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.util.ByteBufList;

/**
 * Shared isolated TLS/HELLO, bounded correlation and output custody for two concrete connection purposes.
 * this client owns one channel and bounded queued/pending requests, no retry, reconnect or new executor.
 * Timeout/disconnect are exceptional UNKNOWN, never a fabricated durable result or cancellation of server I/O.
 */
final class ProfileSocketClient implements AutoCloseable {
    private static final ProfileHello.Capability CAPABILITY = new ProfileHello.Capability(
            ProfileInitialControlCodec.CAPABILITY_ID, ProfileInitialControlCodec.SCHEMA);
    private static final ProfileHello.Capability RECOVERY_CAPABILITY = new ProfileHello.Capability(
            ProfileInitialControlCodec.RECOVERY_CAPABILITY_ID, ProfileInitialControlCodec.RECOVERY_SCHEMA);
    private static final ProfileHello.Capability RECOVERY_CLOSE_CAPABILITY = new ProfileHello.Capability(
            ProfileInitialControlCodec.RECOVERY_CLOSE_CAPABILITY_ID, ProfileInitialControlCodec.RECOVERY_CLOSE_SCHEMA);
    private static final ProfileHello.Capability REPLACEMENT_CAPABILITY = new ProfileHello.Capability(
            ProfileInitialControlCodec.REPLACEMENT_CAPABILITY_ID, ProfileInitialControlCodec.REPLACEMENT_SCHEMA);
    private static final int MAX_RESPONSE_FRAME = 36 + 36 + ProfileHello.MAX_BOOKIE_ID_BYTES
            + 8 * ProfileHello.MAX_CAPABILITIES;
    private final boolean normalData;
    private final int maxEntryBytes;
    private final ProfileHello.Server expected;
    private final Semaphore slots;
    private final int maxSlots;
    private final AtomicInteger retiredSlots = new AtomicInteger();
    private final AtomicInteger helloWrites = new AtomicInteger();
    private final AtomicBoolean channelAttached = new AtomicBoolean();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private volatile boolean initialized;
    private volatile boolean transportClosed;
    private final long timeoutNanos;
    private volatile long helloTimeoutNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<ProfileSocketClient> ready = new CompletableFuture<>();
    private final Map<Long, Pending> pending = new HashMap<>(); // Event-loop confined.
    private volatile Channel channel;
    private ProfileHello.Server hello;
    private long nextId = 2;
    private io.netty.util.concurrent.ScheduledFuture<?> helloDeadline;

    private ProfileSocketClient(String bookie, byte[] incarnation, int maxPending, Duration timeout,
                                boolean normalData, int maxEntryBytes) {
        this.normalData = normalData;
        this.maxEntryBytes = maxEntryBytes;
        expected = new ProfileHello.Server(EngineProfile.SEGMENT_WAL_ENGINE.wireValue(), incarnation, 0,
                bookie, List.of());
        validateSettings(maxPending, timeout, normalData, maxEntryBytes);
        maxSlots = maxPending;
        slots = new Semaphore(maxPending);
        timeoutNanos = timeout.toNanos();
    }

    private static void validateSettings(int maxPending, Duration timeout, boolean normalData, int maxEntryBytes) {
        if (normalData && (maxEntryBytes < 36 || maxEntryBytes > 1024 * 1024 + 36)) {
            throw new IllegalArgumentException("Invalid installed ordinary entry bound");
        }
        if (maxPending < 1 || maxPending > ProfileReservation.MAX_INITIAL_TARGETS
                || timeout.compareTo(Duration.ofMillis(1)) < 0 || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid bounded initial-control client settings");
        }
    }

    public static CompletableFuture<ProfileSocketClient> connect(EventLoopGroup group, SslContext tls,
            InetSocketAddress remote, String peerHostname, String expectedBookie, byte[] expectedIncarnation,
            int maxPending, Duration timeout, boolean normalData, int maxEntryBytes) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(remote, "remote");
        Objects.requireNonNull(peerHostname, "peerHostname");
        if (!tls.isClient() || peerHostname.isBlank() || remote.isUnresolved()) {
            throw new IllegalArgumentException("Initial control requires client TLS, hostname and a resolved endpoint");
        }
        var client = new ProfileSocketClient(expectedBookie, expectedIncarnation, maxPending, timeout,
                normalData, maxEntryBytes);
        client.start(group, tls, remote, peerHostname, timeout);
        return client.ready;
    }

    private void start(EventLoopGroup group, SslContext tls, InetSocketAddress remote, String peerHostname,
            Duration timeout) {
        helloTimeoutNanos = timeout.toNanos();
        ready.whenComplete((connected, failure) -> {
            if (failure != null) {
                close();
            }
        });
        try {
            var connecting = new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(timeout.toMillis()))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socket) {
                            attachChannel(socket);
                            SslHandler ssl = tls.newHandler(socket.alloc(), peerHostname, remote.getPort());
                            ssl.engine().setEnabledProtocols(new String[] {"TLSv1.3"});
                            SSLParameters parameters = ssl.engine().getSSLParameters();
                            parameters.setEndpointIdentificationAlgorithm("HTTPS");
                            ssl.engine().setSSLParameters(parameters);
                            ssl.setHandshakeTimeoutMillis(timeout.toMillis());
                            socket.pipeline().addLast(ssl);
                            socket.pipeline().addLast(new LengthFieldBasedFrameDecoder(normalData
                                    ? Math.max(MAX_RESPONSE_FRAME, 36 + ProfileLacReplyCodec.EXPLICIT_PREFIX_BYTES
                                        + ProfileLacReplyCodec.EXPLICIT_BYTES + maxEntryBytes)
                                            : MAX_RESPONSE_FRAME, 0, 4, 0, 0) {
                                @Override
                                protected long getUnadjustedFrameLength(ByteBuf buffer, int offset, int length,
                                        java.nio.ByteOrder order) {
                                    long value = super.getUnadjustedFrameLength(buffer, offset, length, order);
                                    if (hello == null && value + 4 > MAX_RESPONSE_FRAME) {
                                        throw new io.netty.handler.codec.TooLongFrameException(
                                            "Oversized pre-HELLO reply");
                                    }
                                    return value;
                                }
                            });
                            socket.pipeline().addLast(new Responses());
                            if (closed.get()) {
                                socket.close();
                            }
                        }
                    }).connect(remote);
            attachChannel(connecting.channel());
            connecting.addListener(connected -> {
                if (!connected.isSuccess()) {
                    ready.completeExceptionally(
                            new IOException("Initial control connection failed", connected.cause()));
                }
            });
        } catch (RuntimeException failure) {
            ready.completeExceptionally(unknown());
        } finally {
            initialized = true;
            if (channel == null) {
                transportClosed = true;
            }
            checkDrained();
        }
    }

    private void attachChannel(Channel socket) {
        channel = socket;
        if (channelAttached.compareAndSet(false, true)) {
            socket.closeFuture().addListener(ignored -> {
                Runnable terminal = () -> {
                    transportClosed = true;
                    ready.completeExceptionally(unknown());
                    checkDrained();
                };
                try {
                    socket.eventLoop().execute(terminal); // After close's outbound failures and inactive callbacks.
                } catch (RuntimeException stopped) {
                    terminal.run();
                }
            });
        }
        if (closed.get()) {
            socket.close();
        }
    }

    /** Owns one registered connection from cold checks through actual local close/drain. */
    public static final class RegisteredConnection<T> {
        private final CompletableFuture<T> completion = new CompletableFuture<>();
        private final CompletableFuture<Void> drained = new CompletableFuture<>();
        private final AtomicBoolean stopped = new AtomicBoolean();
        private volatile ProfileSocketClient socket;
        private volatile boolean coldDone;

        private RegisteredConnection() { }

        public CompletableFuture<T> completion() {
            var waiter = completion.copy();
            waiter.whenComplete((value, failure) -> {
                if (waiter.isCancelled()) {
                    closeAsync();
                }
            });
            return waiter;
        }

        /** Cancellation stops future phases; this future also waits for actual queued/running cold work. */
        public CompletableFuture<Void> closeAsync() {
            ProfileSocketClient owned;
            synchronized (this) {
                stopped.set(true);
                owned = socket;
            }
            if (owned != null) {
                owned.close();
            }
            checkDrained();
            // User completion callbacks must not hold the lifecycle lock or postpone actual close.
            completion.completeExceptionally(registeredUnavailable());
            return drained.copy();
        }

        private void checkDrained() {
            var owned = socket;
            if (stopped.get() && coldDone && (owned == null || owned.drained.isDone())) {
                drained.complete(null);
            }
        }
    }

    static <T> RegisteredConnection<T> connectRegistered(EventLoopGroup group, SslContext tls,
            ZKProfileRegistrationStore registry, ZKProfileRegistrationStore.RegisteredPeer peer,
            ThreadPoolExecutor cold, int maxPending, Duration timeout, boolean normalData, int maxEntryBytes,
            Function<ProfileSocketClient, T> wrap) {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(cold, "cold");
        if (!tls.isClient() || !(cold.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue<?>)
                || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException(
                    "Registered connection needs client TLS and a bounded rejecting cold owner");
        }
        // Validate fixed limits before submitting work, without creating a channel.
        validateSettings(maxPending, timeout, normalData, maxEntryBytes);
        long deadline = System.nanoTime() + timeout.toNanos();
        var attempt = new RegisteredConnection<T>();
        try {
            CompletableFuture.supplyAsync(() -> {
                requireRegisteredPhase(attempt, deadline);
                if (registry.revalidatePeer(peer).peer() == null
                        || peer.readiness().engine() != EngineProfile.SEGMENT_WAL_ENGINE
                        || peer.readiness().protocolMajor() != ProfileFrameHeader.PROTOCOL_MAJOR
                        || peer.readiness().protocolMinor() != ProfileFrameHeader.PROTOCOL_MINOR) {
                    throw new IllegalStateException("Registered peer is unavailable or unsupported");
                }
                URI endpoint = URI.create(peer.projection().endpoint());
                String host = endpoint.getHost();
                if (!"tls".equals(endpoint.getScheme()) || host == null || endpoint.getPort() < 1
                        || endpoint.getPort() > 65535 || endpoint.getRawUserInfo() != null
                        || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null
                        || endpoint.getRawPath() == null || !endpoint.getRawPath().isEmpty()) {
                    throw new IllegalArgumentException("Unsupported Profile endpoint");
                }
                if (host.startsWith("[")) {
                    host = host.substring(1, host.length() - 1);
                }
                var remote = new InetSocketAddress(host, endpoint.getPort());
                if (remote.isUnresolved()) {
                    throw new IllegalStateException("Registered endpoint is unresolved");
                }
                requireRegisteredPhase(attempt, deadline);
                return new RegisteredEndpoint(remote, host);
            }, cold).thenCompose(endpoint -> {
                requireRegisteredPhase(attempt, deadline);
                long remaining = deadline - System.nanoTime();
                if (remaining < TimeUnit.MILLISECONDS.toNanos(1)) {
                    return CompletableFuture.<ProfileSocketClient>failedFuture(registeredUnavailable());
                }
                var budget = Duration.ofNanos(remaining);
                var client = new ProfileSocketClient(peer.projection().bookieId(),
                        peer.projection().storageIncarnation().value(), maxPending, timeout, normalData, maxEntryBytes);
                synchronized (attempt) {
                    requireRegisteredPhase(attempt, deadline);
                    attempt.socket = client;
                    client.drained.whenComplete((value, failure) -> attempt.checkDrained());
                }
                client.start(group, tls, endpoint.remote(), endpoint.hostname(), budget);
                return client.ready;
            }).thenCompose(client -> CompletableFuture.supplyAsync(() -> {
                requireRegisteredPhase(attempt, deadline);
                if (client.hello().readinessGeneration() != peer.projection().readinessGeneration().value()
                        || registry.revalidatePeer(peer).peer() == null) {
                    throw new IllegalStateException("Registered peer changed across HELLO");
                }
                requireRegisteredPhase(attempt, deadline);
                return wrap.apply(client);
            }, cold)).whenComplete((client, failure) -> {
                attempt.coldDone = true;
                boolean deliver;
                synchronized (attempt) {
                    deliver = failure == null && !attempt.stopped.get();
                }
                // A stop racing this already verified cut closes the same socket; callbacks hold no lifecycle lock.
                if (!deliver || !attempt.completion.complete(client)) {
                    attempt.closeAsync();
                }
                attempt.checkDrained();
            });
        } catch (RuntimeException rejected) {
            attempt.coldDone = true;
            attempt.closeAsync();
        }
        return attempt;
    }

    private record RegisteredEndpoint(InetSocketAddress remote, String hostname) { }

    private static void requireRegisteredPhase(RegisteredConnection<?> attempt, long deadline) {
        if (attempt.stopped.get() || deadline - System.nanoTime() <= 0) {
            throw new IllegalStateException("Registered connection stopped or expired");
        }
    }

    private static IOException registeredUnavailable() {
        return new IOException("Registered Profile connection unavailable");
    }

    public ProfileHello.Server hello() {
        if (!ready.isDone() || ready.isCompletedExceptionally()) {
            throw new IllegalStateException("Profile peer HELLO is not verified");
        }
        return hello;
    }

    public CompletableFuture<ProfileControlResult> install(ProfileControlRequest request) {
        return send(request, ProfileControlOperation.INSTALL, ProfileOperation.INSTALL);
    }

    public CompletableFuture<ProfileControlResult> activate(ProfileControlRequest request) {
        if (request.scope().operation() == ProfileControlOperation.ACTIVATE_REPLACEMENT) {
            if (!hello().capabilities().contains(REPLACEMENT_CAPABILITY)) {
                return CompletableFuture.completedFuture(result(request.scope(), Outcome.UNSUPPORTED));
            }
            return send(request, ProfileControlOperation.ACTIVATE_REPLACEMENT, ProfileOperation.ACTIVATE_REPLACEMENT);
        }
        return send(request, ProfileControlOperation.ACTIVATE_INITIAL, ProfileOperation.ACTIVATE_INITIAL);
    }

    CompletableFuture<ProfileControlResult> recoveryGrant(ProfileControlRequest request) {
        if (!hello().capabilities().contains(RECOVERY_CAPABILITY)) {
            return CompletableFuture.completedFuture(result(request.scope(), Outcome.UNSUPPORTED));
        }
        return send(request, ProfileControlOperation.RECOVERY_GRANT, ProfileOperation.RECOVERY_GRANT);
    }

    CompletableFuture<ProfileControlResult> recoveryClose(ProfileControlRequest request) {
        if (!hello().capabilities().contains(RECOVERY_CLOSE_CAPABILITY)) {
            return CompletableFuture.completedFuture(result(request.scope(), Outcome.UNSUPPORTED));
        }
        return send(request, ProfileControlOperation.RECOVERY_GRANT_CLOSE, ProfileOperation.RECOVERY_GRANT_CLOSE);
    }

    CompletableFuture<ProfileControlResult> terminalClose(ProfileControlRequest request) {
        if (!hello().capabilities().contains(ProfileInitialControlCodec.TERMINAL_BOUNDARY_CAPABILITY)) {
            return CompletableFuture.completedFuture(result(request.scope(), Outcome.UNSUPPORTED));
        }
        return send(request, ProfileControlOperation.RECOVERY_TERMINAL_CLOSE, ProfileOperation.RECOVERY_TERMINAL_CLOSE);
    }

    CompletableFuture<ProfileControlResult> deleteApply(ProfileControlRequest request) {
        if (!hello().capabilities().contains(ProfileInitialControlCodec.DELETE_APPLY_CAPABILITY)) {
            return CompletableFuture.completedFuture(result(request.scope(), Outcome.UNSUPPORTED));
        }
        return send(request, ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY,
                ProfileOperation.TOMBSTONE_OR_DELETE_APPLY);
    }

    ProfileInitialControlClient.PhysicalAttempt physicalDelete(ProfilePhysicalDeleteCodec.Request request) {
        Objects.requireNonNull(request, "request");
        ProfileControlScope scope = request.deletion().scope();
        if (normalData || !scope.targetBookie().equals(hello().bookieId())
                || !Arrays.equals(scope.targetStorageIncarnation(), hello().storageIncarnation())) {
            throw new IllegalArgumentException("Physical delete request is not bound to the verified CONTROL peer");
        }
        if (!hello().capabilities().contains(ProfilePhysicalDeleteCodec.CAPABILITY)) {
            return new ProfileInitialControlClient.PhysicalAttempt(
                    CompletableFuture.failedFuture(new IOException("Peer does not support UIC8")),
                    CompletableFuture.completedFuture(null));
        }
        if (closed.get() || !slots.tryAcquire()) {
            return new ProfileInitialControlClient.PhysicalAttempt(CompletableFuture.failedFuture(unknown()),
                    CompletableFuture.completedFuture(null));
        }
        Pending held = new Pending(request);
        enqueue(held);
        return new ProfileInitialControlClient.PhysicalAttempt(held.physicalReply, held.completion.copy());
    }

    private CompletableFuture<ProfileControlResult> send(ProfileControlRequest request,
            ProfileControlOperation expectedOperation, ProfileOperation operation) {
        Objects.requireNonNull(request, "request");
        ProfileControlScope scope = request.scope();
        if (normalData || scope.operation() != expectedOperation || !scope.targetBookie().equals(hello().bookieId())
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
        held.controlRequest = request;
        enqueue(held);
        return held.reply;
    }

    CompletableFuture<ProfileStatus> fence(ProfileLedgerContext context, byte[] operationId,
            ProtectedProfileCredential credential) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(credential, "credential");
        byte[] owned = ProfileLifecycleHead.fixed(operationId, 16, true);
        if (normalData) {
            throw new IllegalArgumentException("Fence requires a verified CONTROL connection");
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(unknown());
        }
        if (!slots.tryAcquire()) {
            return CompletableFuture.completedFuture(transientNone());
        }
        Pending held = new Pending(context, owned, credential);
        enqueue(held);
        return held.dataReply;
    }

    private ByteBuf encodeFence(Pending held, long id) {
        ByteBuf bytes = channel.alloc().buffer(136, 136);
        try {
            bytes.writeInt(132).writeInt(ProfileFrameHeader.MAGIC)
                    .writeShort(ProfileFrameHeader.PROTOCOL_MAJOR).writeShort(ProfileFrameHeader.PROTOCOL_MINOR)
                    .writeShort(ProfileFrameHeader.ENCODED_LENGTH).writeShort(ProfileOperation.FENCE_LEDGER.wireValue())
                    .writeInt(0).writeLong(id).writeInt(100).writeInt(0)
                    .writeBytes(held.context.encode()).writeBytes(held.fenceId).writeShort(1).writeShort(20);
            int keyPosition = bytes.writerIndex();
            bytes.writeZero(20);
            held.credential.writeWireCredential(bytes.nioBuffer(keyPosition, 20));
            return bytes;
        } catch (RuntimeException | Error failedEncoding) {
            bytes.setZero(0, bytes.capacity());
            bytes.release();
            throw failedEncoding;
        }
    }

    CompletableFuture<ProfileStatus> addNormal(ProfileLedgerContext context, ProtectedProfileCredential credential,
            long entryId, ReferenceCounted entry) {
        return beginNormalAdd(context, credential, entryId, entry).result();
    }

    ProfileNormalDataClient.AddAttempt beginNormalAdd(ProfileLedgerContext context,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry) {
        return sendEntry(context, credential, entryId, entry, null);
    }

    CompletableFuture<ProfileStatus> addRecovery(ProfileRepairIntent intent, ProtectedProfileCredential credential,
            long entryId, ReferenceCounted entry) {
        Objects.requireNonNull(intent, "intent");
        if (entryId < intent.range().firstEntryId() || entryId > intent.range().lastEntryId()) {
            throw new IllegalArgumentException("Recovery entry lies outside its frozen intent range");
        }
        return sendEntry(intent.recovery().ledger(), credential, entryId, entry, intent).result();
    }

    private ProfileNormalDataClient.AddAttempt sendEntry(ProfileLedgerContext context,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry, ProfileRepairIntent intent) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(credential, "credential");
        Objects.requireNonNull(entry, "entry");
        if (!normalData || entryId < 0) {
            throw new IllegalArgumentException("Ordinary Add requires a DATA connection and valid coordinate");
        }
        long size;
        if (entry instanceof ByteBuf buffer) {
            size = buffer.readableBytes();
        } else if (entry instanceof ByteBufList buffers && buffers.size() > 0 && buffers.size() <= 2) {
            size = 0;
            for (int index = 0; index < buffers.size(); index++) {
                size += buffers.getBuffer(index).readableBytes();
            }
        } else {
            throw new IllegalArgumentException("Expected CRC32C BookKeeper entry buffer(s)");
        }
        if (size < 36 || size > maxEntryBytes) {
            throw new IllegalArgumentException("Ordinary entry is outside the installed bound");
        }
        if (closed.get()) {
            return new ProfileNormalDataClient.AddAttempt(CompletableFuture.failedFuture(unknown()),
                    CompletableFuture.completedFuture(null));
        }
        if (!slots.tryAcquire()) {
            return new ProfileNormalDataClient.AddAttempt(CompletableFuture.completedFuture(transientNone()),
                    CompletableFuture.completedFuture(null));
        }
        Pending held = new Pending(context, credential, entryId, (int) size, intent);
        try {
            held.entry = entry instanceof ByteBuf buffer
                    ? buffer.retainedSlice(buffer.readerIndex(), buffer.readableBytes())
                    : ByteBufList.clone((ByteBufList) entry);
        } catch (RuntimeException | Error failedBorrow) {
            returnSlot();
            throw failedBorrow;
        }
        enqueue(held);
        return new ProfileNormalDataClient.AddAttempt(held.dataReply, held.completion.copy());
    }

    ProfileNormalDataClient.LacWriteAttempt beginWriteLac(ProfileLedgerContext context,
            ProtectedProfileCredential credential, long lac, ByteBufList digest) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(credential, "credential");
        Objects.requireNonNull(digest, "digest");
        if (!normalData || lac < 0 || digest.size() != 1 || digest.readableBytes() != 20) {
            throw new IllegalArgumentException("Explicit LAC requires DATA and the original CRC32C digest");
        }
        if (!hello().capabilities().contains(ProfileOperationCodec.WRITE_LAC_CAPABILITY)) {
            return new ProfileNormalDataClient.LacWriteAttempt(
                    CompletableFuture.completedFuture(ProfileStatus.unsupported()),
                    CompletableFuture.completedFuture(null));
        }
        if (closed.get()) {
            return new ProfileNormalDataClient.LacWriteAttempt(
                    CompletableFuture.failedFuture(unknown()), CompletableFuture.completedFuture(null));
        }
        if (!slots.tryAcquire()) {
            return new ProfileNormalDataClient.LacWriteAttempt(
                    CompletableFuture.completedFuture(transientNone()), CompletableFuture.completedFuture(null));
        }
        Pending held = new Pending(context, credential, lac, 20, null, ProfileOperation.WRITE_LAC);
        try {
            held.entry = ByteBufList.clone(digest);
        } catch (RuntimeException | Error failedBorrow) {
            returnSlot();
            throw failedBorrow;
        }
        enqueue(held);
        return new ProfileNormalDataClient.LacWriteAttempt(held.dataReply, held.completion.copy());
    }

    CompletableFuture<ProfileReadEntryReplyCodec.Reply> readEntry(ProfileLedgerContext context, long entryId) {
        Objects.requireNonNull(context, "context");
        if (!normalData || entryId < 0) {
            throw new IllegalArgumentException("Point read requires DATA and a nonnegative coordinate");
        }
        if (!hello().capabilities().contains(ProfileReadEntryReplyCodec.CAPABILITY)) {
            return CompletableFuture.completedFuture(ProfileReadEntryReplyCodec.refused(ProfileStatus.unsupported()));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(unknown());
        }
        if (!slots.tryAcquire()) {
            return CompletableFuture.completedFuture(ProfileReadEntryReplyCodec.refused(transientNone()));
        }
        Pending held = new Pending(context, entryId);
        enqueue(held);
        return held.readReply;
    }

    CompletableFuture<ProfileLacReplyCodec.Reply> readLac(ProfileLedgerContext context) {
        Objects.requireNonNull(context, "context");
        if (!normalData) {
            throw new IllegalArgumentException("LAC source query requires DATA");
        }
        if (!hello().capabilities().contains(ProfileLacReplyCodec.CAPABILITY)
                && !hello().capabilities().contains(ProfileLacReplyCodec.EXPLICIT_CAPABILITY)) {
            return CompletableFuture.completedFuture(ProfileLacReplyCodec.refused(ProfileStatus.unsupported()));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(unknown());
        }
        if (!slots.tryAcquire()) {
            return CompletableFuture.completedFuture(ProfileLacReplyCodec.refused(transientNone()));
        }
        Pending held = new Pending(context);
        enqueue(held);
        return held.lacReply;
    }

    private ByteBuf encodeWriteLac(Pending held, long id) {
        ByteBuf bytes = channel.alloc().buffer(152, 152);
        try {
            bytes.writeInt(148).writeInt(ProfileFrameHeader.MAGIC)
                    .writeShort(ProfileFrameHeader.PROTOCOL_MAJOR).writeShort(ProfileFrameHeader.PROTOCOL_MINOR)
                    .writeShort(ProfileFrameHeader.ENCODED_LENGTH).writeShort(ProfileOperation.WRITE_LAC.wireValue())
                    .writeInt(0).writeLong(id).writeInt(ProfileOperationCodec.WRITE_LAC_BODY_BYTES).writeInt(0)
                    .writeBytes(held.context.encode()).writeLong(held.entryId).writeShort(1).writeShort(20);
            int keyPosition = bytes.writerIndex();
            bytes.writeZero(20);
            held.credential.writeWireCredential(bytes.nioBuffer(keyPosition, 20));
            var body = ((ByteBufList) held.entry).getBuffer(0);
            bytes.writeInt(20).writeBytes(body, body.readerIndex(), body.readableBytes());
            return bytes;
        } catch (RuntimeException | Error failure) {
            bytes.setZero(0, bytes.capacity());
            bytes.release();
            throw failure;
        }
    }

    private ByteBuf encodeNormal(Pending held, long id) {
        int bodyBytes = (held.repair == null ? 100 : 144) + held.entryBytes;
        ByteBuf bytes = channel.alloc().buffer(36 + bodyBytes, 36 + bodyBytes);
        try {
            bytes.writeInt(32 + bodyBytes).writeInt(ProfileFrameHeader.MAGIC)
                    .writeShort(ProfileFrameHeader.PROTOCOL_MAJOR).writeShort(ProfileFrameHeader.PROTOCOL_MINOR)
                    .writeShort(ProfileFrameHeader.ENCODED_LENGTH).writeShort(held.operation.wireValue())
                    .writeInt(0).writeLong(id).writeInt(bodyBytes).writeInt(0)
                    .writeBytes(held.context.encode());
            if (held.repair == null) {
                bytes.writeLong(held.entryId).writeInt(0);
            } else {
                bytes.writeBytes(held.repair.intentId()).writeLong(held.repair.intentGeneration())
                        .writeLong(held.repair.grantGeneration()).writeLong(held.repair.range().firstEntryId())
                        .writeLong(held.repair.range().lastEntryId()).writeLong(held.entryId);
            }
            bytes.writeShort(1).writeShort(20);
            int keyPosition = bytes.writerIndex();
            bytes.writeZero(20);
            held.credential.writeWireCredential(bytes.nioBuffer(keyPosition, 20));
            bytes.writeInt(held.entryBytes);
            if (held.entry instanceof ByteBuf entry) {
                bytes.writeBytes(entry, entry.readerIndex(), entry.readableBytes());
            } else {
                ByteBufList buffers = (ByteBufList) held.entry;
                for (int index = 0; index < buffers.size(); index++) {
                    ByteBuf part = buffers.getBuffer(index);
                    bytes.writeBytes(part, part.readerIndex(), part.readableBytes());
                }
            }
            return bytes;
        } catch (RuntimeException | Error failure) {
            bytes.setZero(0, bytes.capacity());
            bytes.release();
            throw failure;
        }
    }

    private static ProfileStatus transientNone() {
        return new ProfileStatus(ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, ProfileStatus.DurableResult.NONE, 0);
    }

    private void enqueue(Pending held) {
        try {
            channel.eventLoop().execute(() -> {
                if (closed.get() || !channel.isActive() || held.cancelled()) {
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
                    ByteBuf bytes;
                    try {
                        bytes = held.operation == ProfileOperation.ADD_NORMAL
                                || held.operation == ProfileOperation.ADD_RECOVERY
                                ? encodeNormal(held, id)
                                : held.operation == ProfileOperation.WRITE_LAC ? encodeWriteLac(held, id)
                                : held.operation == ProfileOperation.FENCE_LEDGER ? encodeFence(held, id)
                                : held.operation == ProfileOperation.READ_ENTRY ? encode(held.operation, id,
                                        ProfileOperationCodec.encode(ProfileOperation.READ_ENTRY,
                                                new ProfileOperationCodec.ReadEntry(held.context, held.entryId, 0)))
                                : held.operation == ProfileOperation.READ_LAC ? encode(held.operation, id,
                                        ProfileOperationCodec.encode(ProfileOperation.READ_LAC,
                                                new ProfileOperationCodec.ReadLac(held.context)))
                                : held.operation == ProfileOperation.OPERATION_STATUS ? encode(held.operation, id,
                                        ProfilePhysicalDeleteCodec.encodeRequest(held.physicalRequest))
                                : encode(held.operation, id,
                                        ProfileInitialControlCodec.encodeRequest(held.controlRequest));
                    } finally {
                        held.controlRequest = null;
                        held.discardSource();
                    }
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
                } catch (RuntimeException | Error failure) {
                    held.writeDone = true;
                    pending.remove(id);
                    held.fail(unknown());
                    close();
                    if (failure instanceof Error error) {
                        throw error;
                    }
                }
            });
        } catch (RuntimeException shutdown) {
            held.writeDone = true;
            held.fail(unknown());
            close();
        }
        // Cancellation stops waiting only; both wire and output ownership still reach their actual terminal.
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
            helloDeadline = ctx.executor().schedule(ProfileSocketClient.this::close,
                    helloTimeoutNanos, TimeUnit.NANOSECONDS);
            SslHandler ssl = ctx.pipeline().get(SslHandler.class);
            ssl.handshakeFuture().addListener(handshake -> {
                try {
                    if (!handshake.isSuccess() || closed.get()
                            || ssl.engine().getSession().getLocalCertificates() == null
                            || ssl.engine().getSession().getPeerCertificates().length == 0) {
                        close();
                        return;
                    }
                    ByteBuf outgoing = encode(ProfileOperation.HELLO, 1,
                            new ProfileHello.Client(normalData ? List.of(ProfileReadEntryReplyCodec.CAPABILITY,
                                    ProfileLacReplyCodec.CAPABILITY, ProfileLacReplyCodec.EXPLICIT_CAPABILITY,
                                    ProfileOperationCodec.WRITE_LAC_CAPABILITY)
                                    : List.of(CAPABILITY, REPLACEMENT_CAPABILITY, RECOVERY_CAPABILITY,
                                            RECOVERY_CLOSE_CAPABILITY,
                                            ProfileInitialControlCodec.TERMINAL_CLOSE_CAPABILITY,
                                            ProfileInitialControlCodec.TERMINAL_BOUNDARY_CAPABILITY,
                                            ProfileInitialControlCodec.DELETE_APPLY_CAPABILITY,
                                            ProfilePhysicalDeleteCodec.CAPABILITY)).encode());
                    helloWrites.incrementAndGet();
                    try {
                        ctx.writeAndFlush(outgoing).addListener(written -> {
                            helloWrites.decrementAndGet();
                            if (!written.isSuccess()) {
                                close();
                            }
                            checkDrained();
                        });
                    } catch (RuntimeException | Error failedWrite) {
                        io.netty.util.ReferenceCountUtil.safeRelease(outgoing);
                        helloWrites.decrementAndGet();
                        close();
                        throw failedWrite;
                    }
                } catch (Exception invalidTls) {
                    close();
                }
            });
            ctx.fireChannelActive();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
            try {
                byte[] fixed = new byte[36];
                bytes.getBytes(bytes.readerIndex(), fixed);
                var inspected = ProfileFrameCodec.inspectHeader(fixed, hello == null
                        ? ProfileFrameCodec.DecodePhase.PRE_HELLO : ProfileFrameCodec.DecodePhase.ESTABLISHED);
                if (inspected.frameBytes() != bytes.readableBytes()) {
                    throw invalidReply();
                }
                var readHeader = inspected.header();
                if (hello != null && readHeader.operation() == ProfileOperation.OPERATION_STATUS) {
                    Pending held = pending.get(readHeader.requestId());
                    if (held == null || held.operation != ProfileOperation.OPERATION_STATUS || !readHeader.isResponse()
                            || (readHeader.flags() & ProfileFrameHeader.END_OF_STREAM) != 0) {
                        throw invalidReply();
                    }
                    var payload = bytes.nioBuffer(bytes.readerIndex() + 36, readHeader.bodyLength());
                    ProfilePhysicalDeleteCodec.Reply received = null;
                    IOException refused = null;
                    if (readHeader.bodyLength() == ProfileStatus.ENCODED_LENGTH) {
                        byte[] statusBytes = new byte[ProfileStatus.ENCODED_LENGTH];
                        payload.get(statusBytes);
                        var status = ProfileStatus.decode(statusBytes);
                        if (!readHeader.isError() || status.statusClass() == ProfileStatus.StatusClass.OK) {
                            throw invalidReply();
                        }
                        refused = new IOException("Physical deletion refused: " + status);
                    } else {
                        received = ProfilePhysicalDeleteCodec.decodeReply(payload, held.physicalIdentity);
                        if (readHeader.isError() != (received.status().statusClass()
                                != ProfileStatus.StatusClass.OK)) {
                            throw invalidReply();
                        }
                    }
                    pending.remove(readHeader.requestId());
                    held.wireDone = true;
                    held.deadline.cancel(false);
                    held.releaseIfTerminal();
                    if (refused != null) {
                        held.physicalReply.completeExceptionally(refused);
                    } else {
                        held.physicalReply.complete(received);
                    }
                    return;
                }
                if (hello != null && readHeader.operation() == ProfileOperation.READ_LAC) {
                    Pending held = pending.get(readHeader.requestId());
                    if (held == null || held.operation != ProfileOperation.READ_LAC || !readHeader.isResponse()
                            || (readHeader.flags() & ProfileFrameHeader.END_OF_STREAM) != 0) {
                        throw invalidReply();
                    }
                    var payload = bytes.nioBuffer(bytes.readerIndex() + 36, readHeader.bodyLength());
                    var received = hello.capabilities().contains(ProfileLacReplyCodec.EXPLICIT_CAPABILITY)
                            ? ProfileLacReplyCodec.decodeExplicit(payload, held.context, maxEntryBytes)
                            : ProfileLacReplyCodec.decode(payload, held.context, maxEntryBytes);
                    if (readHeader.isError() != (received.status().statusClass() != ProfileStatus.StatusClass.OK)) {
                        throw invalidReply();
                    }
                    pending.remove(readHeader.requestId());
                    held.wireDone = true;
                    held.deadline.cancel(false);
                    held.releaseIfTerminal();
                    held.lacReply.complete(received);
                    return;
                }
                if (hello != null && readHeader.operation() == ProfileOperation.READ_ENTRY) {
                    Pending held = pending.get(readHeader.requestId());
                    if (held == null || held.operation != ProfileOperation.READ_ENTRY || !readHeader.isResponse()
                            || (readHeader.flags() & ProfileFrameHeader.END_OF_STREAM) != 0) {
                        throw invalidReply();
                    }
                    var received = ProfileReadEntryReplyCodec.decode(bytes.nioBuffer(bytes.readerIndex() + 36,
                            readHeader.bodyLength()), held.context, held.entryId, maxEntryBytes);
                    if (readHeader.isError() != (received.status().statusClass() != ProfileStatus.StatusClass.OK)) {
                        throw invalidReply();
                    }
                    pending.remove(readHeader.requestId());
                    held.wireDone = true;
                    held.deadline.cancel(false);
                    held.releaseIfTerminal();
                    held.readReply.complete(received);
                    return;
                }
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
                            || !normalData && !peer.capabilities().contains(CAPABILITY)) {
                        throw invalidReply();
                    }
                    hello = peer;
                    helloDeadline.cancel(false);
                    ready.complete(ProfileSocketClient.this);
                    return;
                }
                Pending held = pending.get(header.requestId());
                if (held == null || held.operation != header.operation()) {
                    throw invalidReply();
                }
                if (held.operation == ProfileOperation.ADD_NORMAL || held.operation == ProfileOperation.ADD_RECOVERY
                        || held.operation == ProfileOperation.FENCE_LEDGER
                        || held.operation == ProfileOperation.WRITE_LAC) {
                    ProfileStatus status = ProfileStatus.decode(body);
                    if (header.isError() != (status.statusClass() != ProfileStatus.StatusClass.OK)
                            || status.statusClass() == ProfileStatus.StatusClass.OK
                                && (status.retryDisposition() != ProfileStatus.RetryDisposition.NEVER
                                    || status.durableResult() != ProfileStatus.DurableResult.APPLIED
                                        && status.durableResult() != ProfileStatus.DurableResult.ALREADY_APPLIED)) {
                        throw invalidReply();
                    }
                    pending.remove(header.requestId());
                    held.wireDone = true;
                    held.deadline.cancel(false);
                    held.releaseIfTerminal();
                    held.dataReply.complete(status);
                    return;
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
            close();
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
        private final CompletableFuture<ProfileControlResult> reply;
        private final CompletableFuture<ProfileStatus> dataReply;
        private final CompletableFuture<ProfileReadEntryReplyCodec.Reply> readReply;
        private final CompletableFuture<ProfileLacReplyCodec.Reply> lacReply;
        private ProfileControlRequest controlRequest;
        private ProfilePhysicalDeleteCodec.Request physicalRequest;
        private byte[] physicalIdentity;
        private CompletableFuture<ProfilePhysicalDeleteCodec.Reply> physicalReply;
        private ProfileRepairIntent repair;
        private byte[] fenceId;
        private ProfileLedgerContext context;
        private ProtectedProfileCredential credential;
        private long entryId;
        private int entryBytes;
        private ReferenceCounted entry;
        private io.netty.util.concurrent.ScheduledFuture<?> deadline;
        private ByteBuf plaintext;
        private boolean writeDone;
        private boolean wireDone;
        private boolean released;
        private CompletableFuture<Void> completion;

        private Pending(ProfilePhysicalDeleteCodec.Request request) {
            scope = request.deletion().scope();
            operation = ProfileOperation.OPERATION_STATUS;
            reply = null;
            dataReply = null;
            readReply = null;
            lacReply = null;
            physicalRequest = request;
            physicalIdentity = ProfilePhysicalDeleteCodec.requestIdentity(request);
            physicalReply = new CompletableFuture<>();
            completion = new CompletableFuture<>();
        }

        private Pending(ProfileControlScope scope, ProfileOperation operation) {
            this.scope = scope;
            this.operation = operation;
            reply = new CompletableFuture<>();
            dataReply = null;
            readReply = null;
            lacReply = null;
        }

        private Pending(ProfileLedgerContext context, ProtectedProfileCredential credential,
                        long entryId, int entryBytes, ProfileRepairIntent intent) {
            this(context, credential, entryId, entryBytes, intent,
                    intent == null ? ProfileOperation.ADD_NORMAL : ProfileOperation.ADD_RECOVERY);
        }

        private Pending(ProfileLedgerContext context, ProtectedProfileCredential credential,
                        long entryId, int entryBytes, ProfileRepairIntent intent, ProfileOperation operation) {
            scope = null;
            repair = intent;
            this.operation = operation;
            reply = null;
            dataReply = new CompletableFuture<>();
            completion = new CompletableFuture<>();
            readReply = null;
            lacReply = null;
            this.context = context;
            this.credential = credential;
            this.entryId = entryId;
            this.entryBytes = entryBytes;
        }

        private Pending(ProfileLedgerContext context, byte[] fenceId, ProtectedProfileCredential credential) {
            scope = null;
            operation = ProfileOperation.FENCE_LEDGER;
            reply = null;
            dataReply = new CompletableFuture<>();
            readReply = null;
            lacReply = null;
            this.context = context;
            this.fenceId = fenceId;
            this.credential = credential;
        }

        private Pending(ProfileLedgerContext context, long entryId) {
            scope = null;
            operation = ProfileOperation.READ_ENTRY;
            reply = null;
            dataReply = null;
            readReply = new CompletableFuture<>();
            lacReply = null;
            this.context = context;
            this.entryId = entryId;
        }

        private Pending(ProfileLedgerContext context) {
            scope = null;
            operation = ProfileOperation.READ_LAC;
            reply = null;
            dataReply = null;
            readReply = null;
            lacReply = new CompletableFuture<>();
            this.context = context;
        }

        private boolean cancelled() {
            return physicalReply != null ? physicalReply.isCancelled()
                    : lacReply != null ? lacReply.isCancelled() : readReply != null ? readReply.isCancelled()
                    : dataReply == null ? reply.isCancelled() : dataReply.isCancelled();
        }

        private void discardSource() {
            if (entry != null) {
                entry.release();
                entry = null;
            }
            credential = null;
            fenceId = null;
            controlRequest = null;
            physicalRequest = null;
        }

        private void fail(IOException failure) {
            wireDone = true;
            if (deadline != null) {
                deadline.cancel(false);
            }
            discardSource();
            releaseIfTerminal();
            if (physicalReply != null) {
                physicalReply.completeExceptionally(failure);
            } else if (lacReply != null) {
                lacReply.completeExceptionally(failure);
            } else if (readReply != null) {
                readReply.completeExceptionally(failure);
            } else if (dataReply == null) {
                reply.completeExceptionally(failure);
            } else {
                dataReply.completeExceptionally(failure);
            }
        }

        private void releaseIfTerminal() {
            if (writeDone && wireDone && !released) {
                released = true;
                returnSlot();
                if (completion != null) {
                    completion.complete(null);
                }
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

    private void returnSlot() {
        slots.release();
        if (closed.get()) {
            retireAvailableSlots();
        }
    }

    private void retireAvailableSlots() {
        retiredSlots.addAndGet(slots.drainPermits());
        checkDrained();
    }

    private void checkDrained() {
        if (closed.get() && initialized && transportClosed && retiredSlots.get() == maxSlots
                && helloWrites.get() == 0) {
            drained.complete(null);
        }
    }

    /** Actual local transport, HELLO write and every acquired RPC/source/output slot are terminal.
     * This is not a remote durable-result or server-I/O cancellation proof. */
    public CompletableFuture<Void> closeAsync() {
        close();
        return drained.copy();
    }

    @Override
    public void close() {
        closed.set(true);
        retireAvailableSlots(); // Permanently remove unused slots, including callers racing their closed check.
        Channel socket = channel;
        if (socket != null) {
            socket.close();
        }
    }
}

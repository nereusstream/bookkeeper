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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;

/** Actual current transport against a released Classic endpoint, with a separate mTLS/HELLO positive control. */
public final class CurrentProfileHandshakeProbe {
    private static final byte[] INCARNATION = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    private static final Class<?> SOCKET = socketClass();

    private CurrentProfileHandshakeProbe() {}

    /** Positive mode owns only a transport peer; reject mode must point to the driver's private stock process. */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 4 || !List.of("positive", "reject").contains(arguments[0])) {
            throw new IllegalArgumentException(
                    "usage: CurrentProfileHandshakeProbe <positive|reject> <port> <cert-root> <receipt.json>");
        }
        boolean positive = arguments[0].equals("positive");
        int port = Integer.parseInt(arguments[1]);
        if (positive ? port != 0 : port < 1024 || port > 65535) {
            throw new IllegalArgumentException("Positive control binds a fresh port; rejection uses a private port");
        }
        Path certs = Path.of(arguments[2]).toRealPath();
        Path output = Path.of(arguments[3]);
        if (Files.exists(output)) {
            throw new IllegalArgumentException("Receipt already exists");
        }
        SslContext clientTls = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                .keyManager(certs.resolve("client-cert.pem").toFile(), certs.resolve("client-key.pem").toFile())
                .trustManager(certs.resolve("server-san-cert.pem").toFile()).protocols("TLSv1.3").build();
        var acceptor = new NioEventLoopGroup(1);
        var worker = new NioEventLoopGroup(1);
        var serverChildren = new io.netty.channel.group.DefaultChannelGroup(worker.next());
        Channel listener = null;
        var attempts = new ArrayList<Map<String, Object>>();
        try {
            if (positive) {
                SslContext serverTls = SslContextBuilder.forServer(certs.resolve("server-san-cert.pem").toFile(),
                                certs.resolve("server-key.pem").toFile()).sslProvider(SslProvider.JDK)
                        .trustManager(certs.resolve("client-cert.pem").toFile()).clientAuth(ClientAuth.REQUIRE)
                        .protocols("TLSv1.3").build();
                listener = new ServerBootstrap().group(acceptor, worker).channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) {
                                serverChildren.add(channel);
                                channel.pipeline().addLast(serverTls.newHandler(channel.alloc()));
                                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(4096, 0, 4, 0, 0));
                                channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext context, ByteBuf bytes) {
                                        byte[] wire = new byte[bytes.readableBytes()];
                                        bytes.getBytes(bytes.readerIndex(), wire);
                                        ProfileFrame request = ProfileFrameCodec.decode(wire,
                                                ProfileFrameCodec.DecodePhase.PRE_HELLO);
                                        if (request.header().operation() != ProfileOperation.HELLO) {
                                            throw new IllegalStateException(
                                                    "Positive transport peer accepts HELLO only");
                                        }
                                        ProfileHello.decodeClient(request.body());
                                        byte[] reply = new ProfileHello.Server(
                                                EngineProfile.SEGMENT_WAL_ENGINE.wireValue(), INCARNATION, 0,
                                                "127.0.0.1:" + ((InetSocketAddress) channel.localAddress()).getPort(),
                                                List.of(new ProfileHello.Capability(
                                                        ProfileInitialControlCodec.CAPABILITY_ID,
                                                        ProfileInitialControlCodec.SCHEMA))).encode();
                                        context.writeAndFlush(Unpooled.wrappedBuffer(ProfileFrameCodec.encode(
                                                new ProfileFrame(new ProfileFrameHeader(
                                                        ProfileFrameHeader.PROTOCOL_MAJOR,
                                                        ProfileFrameHeader.PROTOCOL_MINOR,
                                                        ProfileFrameHeader.ENCODED_LENGTH, ProfileOperation.HELLO,
                                                        ProfileFrameHeader.RESPONSE, 1, reply.length, 0), reply))));
                                    }
                                });
                            }
                        }).bind("127.0.0.1", 0).sync().channel();
                port = ((InetSocketAddress) listener.localAddress()).getPort();
            }
            for (boolean data : new boolean[] {false, true}) {
                attempts.add(attempt(clientTls, port, data, positive));
            }
        } finally {
            if (listener != null) {
                listener.close().syncUninterruptibly();
            }
            serverChildren.close().syncUninterruptibly();
            worker.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
            acceptor.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
        var result = new LinkedHashMap<String, Object>();
        result.put("status", "PASS");
        result.put("mode", arguments[0]);
        result.put("pid", ProcessHandle.current().pid());
        result.put("port", port);
        result.put("javaHome", System.getProperty("java.home"));
        result.put("javaVersion", System.getProperty("java.version"));
        result.put("attempts", attempts);
        result.put("classes", List.of(identity(SOCKET), identity(CurrentProfileHandshakeProbe.class)));
        result.put("positiveScope", "mTLS and actual current client HELLO only; no storage or authority assertion");
        Files.write(output, new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(result),
                StandardOpenOption.CREATE_NEW);
        System.out.println("PROFILE_HANDSHAKE " + arguments[0] + " PASS");
    }

    private static Map<String, Object> attempt(SslContext tls, int port, boolean data, boolean positive)
            throws Exception {
        var channel = new AtomicReference<Channel>();
        var observed = new AtomicReference<Object>();
        var captureFailure = new AtomicReference<Throwable>();
        var wire = new ByteArrayOutputStream();
        int[] counts = new int[2];
        var group = new NioEventLoopGroup(1) {
            @Override
            public ChannelFuture register(Channel socket) {
                counts[0]++;
                channel.set(socket);
                socket.pipeline().addFirst("wireObservation", new ChannelDuplexHandler() {
                    @Override
                    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                        if (message instanceof ByteBuf bytes && bytes.readableBytes() <= 65536 - wire.size()) {
                            byte[] copy = new byte[bytes.readableBytes()];
                            bytes.getBytes(bytes.readerIndex(), copy);
                            wire.writeBytes(copy);
                        } else {
                            captureFailure.compareAndSet(null, new IllegalStateException("Unexpected raw write"));
                        }
                        context.write(message, promise);
                    }
                });
                ChannelFuture registering = super.register(socket);
                registering.addListener(done -> {
                    try {
                        if (!done.isSuccess()) {
                            throw new IllegalStateException("Registration failed", done.cause());
                        }
                        SslHandler ssl = socket.pipeline().get(SslHandler.class);
                        socket.pipeline().addAfter(socket.pipeline().context(ssl).name(), "plaintextObservation",
                                new ChannelDuplexHandler() {
                                    @Override
                                    public void write(ChannelHandlerContext context, Object message,
                                                      ChannelPromise promise) {
                                        counts[1]++;
                                        context.write(message, promise);
                                    }
                                });
                        for (var entry : socket.pipeline()) {
                            for (var field : entry.getValue().getClass().getDeclaredFields()) {
                                if (field.getType() == SOCKET) {
                                    field.setAccessible(true);
                                    observed.set(field.get(entry.getValue()));
                                }
                            }
                        }
                        if (observed.get() == null) {
                            throw new IllegalStateException("Current client owner not observed");
                        }
                    } catch (Throwable failure) {
                        captureFailure.compareAndSet(null, failure);
                    }
                });
                return registering;
            }
        };
        Throwable rejected = null;
        try {
            CompletableFuture<?> connecting = data
                    ? ProfileNormalDataClient.connect(group, tls, new InetSocketAddress("127.0.0.1", port),
                            "apache.bookkeeper.org", "127.0.0.1:" + port, INCARNATION, 1, 1024, Duration.ofSeconds(5))
                    : ProfileInitialControlClient.connect(group, tls, new InetSocketAddress("127.0.0.1", port),
                            "apache.bookkeeper.org", "127.0.0.1:" + port, INCARNATION, 1, Duration.ofSeconds(5));
            try {
                connecting.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException failure) {
                rejected = failure.getCause();
            }
            if (captureFailure.get() != null) {
                throw new IllegalStateException("Observation failed", captureFailure.get());
            }
            if (positive != (rejected == null) || observed.get() == null || channel.get() == null) {
                throw new IllegalStateException("Unexpected Profile connect outcome", rejected);
            }
            var socket = channel.get();
            var handshakeResult = socket.pipeline().get(SslHandler.class).handshakeFuture();
            ((CompletableFuture<?>) call(observed.get(), "closeAsync")).get(10, TimeUnit.SECONDS);
            socket.closeFuture().sync();
            socket.eventLoop().submit(() -> { }).sync();
            boolean handshake = handshakeResult.isSuccess();
            String handshakeFailure = handshakeResult.cause() == null ? ""
                    : handshakeResult.cause().getClass().getName();
            if (!positive && (!handshakeResult.isDone() || handshakeFailure.contains("Timeout"))) {
                throw new IllegalStateException("Not a completed peer rejection: done=" + handshakeResult.isDone()
                        + ", cause=" + handshakeFailure + ", wireBytes=" + wire.size());
            }
            if (counts[0] != 1 || counts[1] != (positive ? 1 : 0) || handshake != positive
                    || socket.isOpen() || ((Number) call(observed.get(), "backingBytes")).longValue() != 0
                    || ((Number) call(observed.get(), "codecWorkspaceBytes")).longValue() != 0 || wire.size() == 0) {
                throw new IllegalStateException("Handshake, write, channel or actual credit drain mismatch");
            }
            byte[] bytes = wire.toByteArray();
            if (bytes.length < 5 || bytes[0] != 22 || bytes[1] != 3) {
                throw new IllegalStateException("Actual client did not emit a TLS ClientHello record");
            }
            int records = 0;
            for (int offset = 0; offset < bytes.length; records++) {
                if (bytes.length - offset < 5 || bytes[offset] < 20 || bytes[offset] > 23
                        || bytes[offset + 1] != 3) {
                    throw new IllegalStateException("Non-TLS bytes on the actual client connection");
                }
                offset += 5 + ((bytes[offset + 3] & 255) << 8) + (bytes[offset + 4] & 255);
                if (offset > bytes.length) {
                    throw new IllegalStateException("Truncated outbound TLS record observation");
                }
            }
            var result = new LinkedHashMap<String, Object>(Map.of(
                    "role", data ? "DATA" : "CONTROL", "ready", rejected == null,
                    "failureClass", rejected == null ? "" : rejected.getClass().getName(),
                    "tlsHandshake", handshake, "connections", counts[0], "plaintextWrites", counts[1],
                    "tlsRecordCount", records, "wireBytes", bytes.length, "backingBytesAfterDrain", 0,
                    "codecWorkspaceBytesAfterDrain", 0));
            result.put("handshakeFailureClass", handshakeFailure);
            return result;
        } finally {
            if (observed.get() != null) {
                ((CompletableFuture<?>) call(observed.get(), "closeAsync")).get(10, TimeUnit.SECONDS);
            }
            if (channel.get() != null) {
                channel.get().close().syncUninterruptibly();
            }
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private static Class<?> socketClass() {
        try {
            return Class.forName("org.apache.bookkeeper.discover.profile.ProfileSocketClient");
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("Actual current transport is absent", failure);
        }
    }

    private static Object call(Object target, String name) throws Exception {
        var method = SOCKET.getDeclaredMethod(name);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static Map<String, String> identity(Class<?> type) throws Exception {
        try (var bytes = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            return Map.of("name", type.getName(),
                    "codeSource", type.getProtectionDomain().getCodeSource().getLocation().toExternalForm(),
                    "resourceSha256", java.util.HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes())));
        }
    }
}

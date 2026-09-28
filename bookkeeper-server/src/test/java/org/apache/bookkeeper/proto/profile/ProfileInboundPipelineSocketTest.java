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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.junit.Test;

/** Exercises the isolated Profile pipeline over real loopback sockets without enabling a Bookie endpoint. */
public class ProfileInboundPipelineSocketTest {
    private static String certificate(String name) throws Exception {
        return Paths.get(ProfileInboundPipelineSocketTest.class.getClassLoader().getResource(name).toURI()).toString();
    }

    @Test
    public void receivesHelloAndDataOverRealMutualTlsSocket() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(certificate("server-key.pem"));
        conf.setTLSCertificatePath(certificate("server-san-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(certificate("client-cert.pem"));
        TLSContextFactory profileTls = TLSContextFactory.forProfileServer();
        profileTls.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        SslContext clientTls = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                .trustManager(new File(certificate("server-san-cert.pem"))).protocols("TLSv1.3").build();
        ProfileReceiveBudget budget = new ProfileReceiveBudget(16_384, 4_096, 2_048, 2, 2, 1);
        CountDownLatch helloReceived = new CountDownLatch(1);
        CountDownLatch addReceived = new CountDownLatch(1);
        CountDownLatch serverInactive = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<ProfilePrincipal> peer = new AtomicReference<>();
        AtomicReference<Channel> accepted = new AtomicReference<>();
        EventLoopGroup acceptor = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        Channel listener = null;
        Channel client = null;
        try {
            listener = new ServerBootstrap().group(acceptor, worker).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            accepted.set(channel);
                            assertTrue(ProfileInboundPipeline.install(channel.pipeline(), profileTls, budget,
                                    8_192, 64, new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object message) {
                                    ProfileOperation operation = ((ProfileInboundFrame) message).header().operation();
                                    try (ProfileInboundFrame frame = (ProfileInboundFrame) message) {
                                        if (operation == ProfileOperation.HELLO) {
                                            peer.set(profileTls.authenticatedProfilePrincipal(
                                                    ctx.pipeline().get(SslHandler.class), "test-domain"));
                                            ctx.pipeline().get(ProfileReceiveDecoder.class).markEstablished();
                                        } else {
                                            assertEquals(ProfileOperation.ADD_NORMAL, operation);
                                            assertEquals(64, frame.body().readableBytes());
                                            ctx.close();
                                        }
                                    } catch (Throwable cause) {
                                        failure.compareAndSet(null, cause);
                                        helloReceived.countDown();
                                        addReceived.countDown();
                                        ctx.close();
                                        return;
                                    }
                                    if (operation == ProfileOperation.HELLO) {
                                        helloReceived.countDown();
                                    } else {
                                        addReceived.countDown();
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    failure.compareAndSet(null, cause);
                                    helloReceived.countDown();
                                    addReceived.countDown();
                                    ctx.close();
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    serverInactive.countDown();
                                }
                            }));
                        }
                    }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            int port = ((InetSocketAddress) listener.localAddress()).getPort();
            client = new Bootstrap().group(clientGroup).channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(clientTls.newHandler(
                                    channel.alloc(), "apache.bookkeeper.org", port));
                        }
                    }).connect("127.0.0.1", port).sync().channel();
            client.pipeline().get(SslHandler.class).handshakeFuture().sync();
            byte[] hello = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 1,
                    new ProfileHello.Client(List.of()).encode()));
            client.writeAndFlush(Unpooled.wrappedBuffer(hello)).sync();
            assertTrue(helloReceived.await(5, TimeUnit.SECONDS));
            assertNull(failure.get());
            byte[] add = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.ADD_NORMAL, 1, new byte[64]));
            client.writeAndFlush(Unpooled.wrappedBuffer(add)).sync();
            assertTrue(addReceived.await(5, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertEquals("test-domain", peer.get().trustDomain());
            assertTrue(accepted.get().closeFuture().await(5, TimeUnit.SECONDS));
            assertTrue(serverInactive.await(5, TimeUnit.SECONDS));
            assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
        } finally {
            if (client != null) {
                client.close().syncUninterruptibly();
            }
            Channel serverConnection = accepted.get();
            if (serverConnection != null) {
                serverConnection.close().syncUninterruptibly();
            }
            if (listener != null) {
                listener.close().syncUninterruptibly();
            }
            clientGroup.shutdownGracefully().syncUninterruptibly();
            worker.shutdownGracefully().syncUninterruptibly();
            acceptor.shutdownGracefully().syncUninterruptibly();
        }
    }
}

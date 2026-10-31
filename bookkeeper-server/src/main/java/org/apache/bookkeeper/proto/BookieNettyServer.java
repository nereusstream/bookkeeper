/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.uring.IoUringChannelOption;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLPeerUnverifiedException;
import lombok.CustomLog;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.BookKeeperPrincipal;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.apache.zookeeper.KeeperException;

/**
 * Netty server for serving bookie requests.
 */
@CustomLog
class BookieNettyServer {
    public static final String CONSOLIDATION_HANDLER_NAME = "consolidation";

    final int maxFrameSize;
    final ServerConfiguration conf;
    EventLoopGroup eventLoopGroup;
    EventLoopGroup acceptorGroup;
    EventLoopGroup jvmEventLoopGroup;
    RequestProcessor requestProcessor;
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Object suspensionLock = new Object();
    volatile boolean suspended = false;
    ChannelGroup allChannels;
    BookieSocketAddress bookieAddress;
    BookieId bookieId;
    InetSocketAddress bindAddress;

    BookieAuthProvider.Factory authProviderFactory;

    private final ByteBufAllocator allocator;
    private final boolean controlled;
    private final List<Thread> controlledThreads = new ArrayList<>();
    private boolean controlledAttempted;
    private boolean controlledInitialized;
    private String controlledAcquiring;
    private Throwable controlledInitializationFailure;
    private Throwable controlledCloseFailure;
    private ChannelFuture controlledBind;
    private ChannelGroupFuture controlledChannelsClose;
    private boolean controlledChannelsCloseAttempted;
    private boolean controlledAcceptorCloseAttempted;
    private boolean controlledIoCloseAttempted;
    private boolean controlledAuthCloseAttempted;

    BookieNettyServer(ServerConfiguration conf, RequestProcessor processor, ByteBufAllocator allocator)
        throws IOException, KeeperException, InterruptedException, BookieException {
        this(conf, processor, allocator, false);
        this.authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf);

        if (!conf.isDisableServerSocketBind()) {
            this.eventLoopGroup = EventLoopUtil.getServerEventLoopGroup(conf,
                    new DefaultThreadFactory("bookie-io") {
                        @Override
                        protected Thread newThread(Runnable r, String name) {
                            return super.newThread(ThreadRegistry.registerThread(r, "bookie-id"), name);
                        }
                    });
            this.acceptorGroup = EventLoopUtil.getServerAcceptorGroup(conf,
                    new DefaultThreadFactory("bookie-acceptor"));
            allChannels = new CleanupChannelGroup(eventLoopGroup);
        } else {
            this.eventLoopGroup = null;
            this.acceptorGroup = null;
        }

        if (conf.isEnableLocalTransport()) {
            jvmEventLoopGroup = new DefaultEventLoopGroup(conf.getServerNumIOThreads()) {
                @Override
                protected EventLoop newChild(Executor executor, Object... args) throws Exception {
                    return new DefaultEventLoop(this, executor) {
                        @Override
                        protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
                            if (conf.isBusyWaitEnabled()) {
                                return new BlockingMpscQueue<>(Math.min(maxPendingTasks, 10_000));
                            } else {
                                return super.newTaskQueue(maxPendingTasks);
                            }
                        }
                    };
                }
            };

            // Enable CPU affinity on IO threads
            if (conf.isBusyWaitEnabled()) {
                for (int i = 0; i < conf.getServerNumIOThreads(); i++) {
                    jvmEventLoopGroup.next().submit(() -> {
                        try {
                            CpuAffinity.acquireCore();
                        } catch (Throwable t) {
                            log.warn()
                                    .exception(t)
                                    .attr("threadName", Thread.currentThread().getName())
                                    .log("Failed to acquire CPU core for thread");
                        }
                    });
                }
            }

            allChannels = new CleanupChannelGroup(jvmEventLoopGroup);
        } else {
            jvmEventLoopGroup = null;
        }
        bookieId = BookieImpl.getBookieId(conf);
        bookieAddress = BookieImpl.getBookieAddress(conf);
        if (conf.getListeningInterface() == null) {
            bindAddress = new InetSocketAddress(conf.getBookiePort());
        } else {
            bindAddress = bookieAddress.getSocketAddress();
        }
        listenOn(bindAddress, bookieAddress);
    }

    private BookieNettyServer(ServerConfiguration conf, RequestProcessor processor, ByteBufAllocator allocator,
            boolean controlled) {
        this.allocator = allocator;
        this.maxFrameSize = conf.getNettyMaxFrameSizeBytes();
        this.conf = conf;
        this.requestProcessor = processor;
        this.controlled = controlled;
    }

    static BookieNettyServer prepareForClassicChild(ServerConfiguration conf, ByteBufAllocator allocator) {
        return new BookieNettyServer(conf, null, allocator, true);
    }

    /** Called after the enclosing server retains this holder, never from a constructor. */
    void initializeForClassicChild() throws IOException, KeeperException, InterruptedException, BookieException {
        if (!controlled || controlledAttempted || isClosed.get()) {
            throw new IllegalStateException("Controlled listener initializes once after its owner retains it");
        }
        controlledAttempted = true;
        try {
            String auth = conf.getBookieAuthProviderFactoryClass();
            String tls = conf.getTLSProviderFactoryClass();
            if (conf.isDisableServerSocketBind() || conf.isEnableLocalTransport() || conf.isBusyWaitEnabled()
                    || auth != null && !auth.isBlank() || tls != null && !tls.isBlank()
                    || conf.getServerNumIOThreads() <= 0 || conf.getServerNumAcceptorThreads() <= 0) {
                throw new IOException("Controlled Classic listener requires fixed TCP workers and built-in auth");
            }
            bookieId = BookieImpl.getBookieId(conf);
            bookieAddress = BookieImpl.getBookieAddress(conf);
            bindAddress = conf.getListeningInterface() == null ? new InetSocketAddress(conf.getBookiePort())
                    : bookieAddress.getSocketAddress();
            controlledAcquiring = "built-in authentication factory";
            authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf);
            controlledAcquiring = "TCP IO event loops";
            eventLoopGroup = EventLoopUtil.getServerEventLoopGroup(conf, controlledThreadFactory("bookie-io"));
            controlledAcquiring = "TCP acceptor event loops";
            acceptorGroup = EventLoopUtil.getServerAcceptorGroup(conf, controlledThreadFactory("bookie-acceptor"));
            controlledAcquiring = null;
            allChannels = new CleanupChannelGroup(eventLoopGroup);
            listenOn(bindAddress, bookieAddress);
            controlledInitialized = true;
        } catch (IOException | InterruptedException | RuntimeException | Error problem) {
            controlledInitializationFailure = problem;
            if (problem instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw problem;
        }
    }

    private DefaultThreadFactory controlledThreadFactory(String name) {
        return new DefaultThreadFactory(name) {
            @Override
            protected Thread newThread(Runnable body, String threadName) {
                Thread thread = super.newThread(ThreadRegistry.registerThread(body, "bookie-id"), threadName);
                synchronized (controlledThreads) {
                    controlledThreads.add(thread);
                }
                return thread;
            }
        };
    }

    boolean isBoundForClassicChild() {
        return controlled && controlledInitialized && controlledBind != null && controlledBind.isSuccess()
                && controlledBind.channel().isActive() && !isClosed.get();
    }

    boolean isCurrentThreadForClassicChild() {
        synchronized (controlledThreads) {
            return controlledThreads.contains(Thread.currentThread());
        }
    }

    void activateForClassicChild(BookieRequestProcessor processor) {
        if (!controlled || !isBoundForClassicChild() || requestProcessor != processor
                || !processor.initializedForClassicChild()) {
            throw new IllegalStateException("Controlled TCP activation requires its original initialized processor");
        }
        isRunning.set(true);
    }

    void submitRetirementForClassicChild() throws InterruptedException {
        if (!controlled || isCurrentThreadForClassicChild()) {
            throw new IllegalStateException("Controlled TCP retirement requires its external owner");
        }
        isRunning.set(false);
        isClosed.set(true);
        if (controlledBind != null) {
            controlledBind.await();
        }
        if (allChannels != null && !controlledChannelsCloseAttempted) {
            controlledChannelsCloseAttempted = true;
            try {
                controlledChannelsClose = allChannels.close();
            } catch (RuntimeException | Error problem) {
                rememberControlledCloseFailure(problem);
            }
        }
    }

    boolean awaitConnectionsForClassicChild() throws InterruptedException {
        if (controlledChannelsClose != null) {
            controlledChannelsClose.await();
            if (!controlledChannelsClose.isSuccess()) {
                rememberControlledCloseFailure(controlledChannelsClose.cause());
            }
        }
        return controlledCloseFailure == null;
    }

    /** Listener-stage drain only. The enclosing server must retire its processor before closing these loops. */
    boolean closeAndDrainForClassicChild() throws InterruptedException {
        if (!controlled || isCurrentThreadForClassicChild()) {
            throw new IllegalStateException("Controlled TCP drain requires the original external owner");
        }
        submitRetirementForClassicChild();
        awaitConnectionsForClassicChild();
        if (acceptorGroup != null && !controlledAcceptorCloseAttempted) {
            controlledAcceptorCloseAttempted = true;
            try {
                acceptorGroup.shutdownGracefully(0, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (RuntimeException | Error problem) {
                rememberControlledCloseFailure(problem);
            }
        }
        if (eventLoopGroup != null && !controlledIoCloseAttempted) {
            controlledIoCloseAttempted = true;
            try {
                eventLoopGroup.shutdownGracefully(0, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (RuntimeException | Error problem) {
                rememberControlledCloseFailure(problem);
            }
        }
        for (EventLoopGroup group : new EventLoopGroup[]{acceptorGroup, eventLoopGroup}) {
            if (group != null) {
                if (!group.isShuttingDown()) {
                    return false;
                }
                group.terminationFuture().await();
                if (!group.terminationFuture().isSuccess()) {
                    rememberControlledCloseFailure(group.terminationFuture().cause());
                }
            }
        }
        if (controlledAcquiring == null) {
            List<Thread> threads;
            synchronized (controlledThreads) {
                threads = List.copyOf(controlledThreads);
            }
            for (Thread thread : threads) {
                thread.join();
            }
        }
        // Without a constructor result, its original acquisition remains unknown even if a fixture cleans it.
        if (authProviderFactory != null && !controlledAuthCloseAttempted) {
            controlledAuthCloseAttempted = true;
            try {
                authProviderFactory.close();
            } catch (RuntimeException | Error problem) {
                rememberControlledCloseFailure(problem);
            }
        }
        return controlledAcquiring == null && controlledCloseFailure == null;
    }

    private void rememberControlledCloseFailure(Throwable problem) {
        if (controlledCloseFailure == null) {
            controlledCloseFailure = problem == null ? new IOException("TCP close has no successful result") : problem;
        }
    }

    Throwable failureForClassicChild() {
        return controlledCloseFailure != null ? controlledCloseFailure
                : controlledAcquiring != null ? controlledInitializationFailure : null;
    }

    public BookieNettyServer setRequestProcessor(RequestProcessor processor) {
        this.requestProcessor = processor;
        return this;
    }

    boolean isRunning() {
        return isRunning.get();
    }

    @VisibleForTesting
    void suspendProcessing() {
        synchronized (suspensionLock) {
            suspended = true;
            for (Channel channel : allChannels) {
                // To suspend processing in the bookie, submit a task
                // that keeps the event loop busy until resume is
                // explicitly invoked
                channel.eventLoop().submit(() -> {
                    while (suspended && isRunning()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
        }
    }

    @VisibleForTesting
    void resumeProcessing() {
        synchronized (suspensionLock) {
            suspended = false;
            for (Channel channel : allChannels) {
                channel.config().setAutoRead(true);
            }
            suspensionLock.notifyAll();
        }
    }

    class BookieSideConnectionPeerContextHandler extends ChannelInboundHandlerAdapter {

        final BookieConnectionPeer connectionPeer;
        volatile Channel channel;
        volatile BookKeeperPrincipal authorizedId = BookKeeperPrincipal.ANONYMOUS;

        public BookieSideConnectionPeerContextHandler() {
            this.connectionPeer = new BookieConnectionPeer() {
                @Override
                public SocketAddress getRemoteAddr() {
                    Channel c = channel;
                    if (c != null) {
                        return c.remoteAddress();
                    } else {
                        return null;
                    }
                }

                @Override
                public Collection<Object> getProtocolPrincipals() {
                    Channel c = channel;
                    if (c == null) {
                        return Collections.emptyList();
                    } else {
                        SslHandler ssl = c.pipeline().get(SslHandler.class);
                        if (ssl == null) {
                            return Collections.emptyList();
                        }
                        try {
                            Certificate[] certificates = ssl.engine().getSession().getPeerCertificates();
                            if (certificates == null) {
                                return Collections.emptyList();
                            }
                            List<Object> result = new ArrayList<>();
                            result.addAll(Arrays.asList(certificates));
                            return result;
                        } catch (SSLPeerUnverifiedException err) {
                            log.error().exception(err).log("Failed to get peer certificates");
                            return Collections.emptyList();
                        }

                    }
                }

                @Override
                public void disconnect() {
                    Channel c = channel;
                    if (c != null) {
                        c.close();
                    }
                    log.info().attr("channel", channel).log("authplugin disconnected channel");
                }

                @Override
                public BookKeeperPrincipal getAuthorizedId() {
                    return authorizedId;
                }

                @Override
                public void setAuthorizedId(BookKeeperPrincipal principal) {
                    log.info()
                            .attr("channel", channel)
                            .attr("principal", principal)
                            .log("connection authenticated");
                    authorizedId = principal;
                }

                @Override
                public boolean isSecure() {
                    Channel c = channel;
                    if (c == null) {
                        return false;
                    } else {
                        return c.pipeline().get("tls") != null;
                    }
                }
            };
        }

        public BookieConnectionPeer getConnectionPeer() {
            return connectionPeer;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
        }
    }

    private void listenOn(InetSocketAddress address, BookieSocketAddress bookieAddress) throws InterruptedException {
        if (!conf.isDisableServerSocketBind()) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.option(ChannelOption.ALLOCATOR, allocator);
            bootstrap.childOption(ChannelOption.ALLOCATOR, allocator);
            bootstrap.group(acceptorGroup, eventLoopGroup);
            bootstrap.childOption(ChannelOption.TCP_NODELAY, conf.getServerTcpNoDelay());
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, conf.getServerSockKeepalive());
            bootstrap.childOption(ChannelOption.SO_LINGER, conf.getServerSockLinger());
            bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(conf.getRecvByteBufAllocatorSizeMin(),
                            conf.getRecvByteBufAllocatorSizeInitial(), conf.getRecvByteBufAllocatorSizeMax()));
            bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    conf.getServerWriteBufferLowWaterMark(), conf.getServerWriteBufferHighWaterMark()));

            // Set TCP keepalive parameters if configured. Use childOption() so that these
            // options are applied to each accepted SocketChannel (not the listening
            // ServerSocketChannel), which is the Netty-idiomatic approach and avoids
            // relying on OS-level socket option inheritance behavior.
            if (EventLoopUtil.isIoUringGroup(eventLoopGroup)) {
                if (conf.getServerTcpKeepIdle() > 0) {
                    bootstrap.childOption(IoUringChannelOption.TCP_KEEPIDLE, conf.getServerTcpKeepIdle());
                }
                if (conf.getServerTcpKeepIntvl() > 0) {
                    bootstrap.childOption(IoUringChannelOption.TCP_KEEPINTVL, conf.getServerTcpKeepIntvl());
                }
                if (conf.getServerTcpKeepCnt() > 0) {
                    bootstrap.childOption(IoUringChannelOption.TCP_KEEPCNT, conf.getServerTcpKeepCnt());
                }
            } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
                if (conf.getServerTcpKeepIdle() > 0) {
                    bootstrap.childOption(EpollChannelOption.TCP_KEEPIDLE, conf.getServerTcpKeepIdle());
                }
                if (conf.getServerTcpKeepIntvl() > 0) {
                    bootstrap.childOption(EpollChannelOption.TCP_KEEPINTVL, conf.getServerTcpKeepIntvl());
                }
                if (conf.getServerTcpKeepCnt() > 0) {
                    bootstrap.childOption(EpollChannelOption.TCP_KEEPCNT, conf.getServerTcpKeepCnt());
                }
            }

            if (EventLoopUtil.isIoUringGroup(eventLoopGroup)) {
                bootstrap.channel(IoUringServerSocketChannel.class);
            } else if (eventLoopGroup instanceof EpollEventLoopGroup) {
                bootstrap.channel(EpollServerSocketChannel.class);
            } else {
                bootstrap.channel(NioServerSocketChannel.class);
            }

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    if (controlled) {
                        allChannels.add(ch);
                    }
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }

                    BookieSideConnectionPeerContextHandler contextHandler =
                        new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();
                    if (controlled && requestProcessor instanceof BookieRequestProcessor processor
                            && processor.classic != null) {
                        pipeline.addFirst("classic-owned-output", processor.classic.outputHandler());
                    }

                    pipeline.addLast(CONSOLIDATION_HANDLER_NAME, new FlushConsolidationHandler(1024, true));

                    pipeline.addLast("bytebufList", ByteBufList.ENCODER);

                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));

                    pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder());
                    pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder());
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(
                                contextHandler.getConnectionPeer(), authProviderFactory));

                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels)
                            : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);

                    pipeline.addLast("contextHandler", contextHandler);
                }
            });

            // Bind and start to accept incoming connections
            log.info().attr("address", address).log("Binding bookie-rpc endpoint");
            final Channel listen;
            if (controlled) {
                controlledAcquiring = "TCP bind submission";
                controlledBind = bootstrap.bind(address.getAddress(), address.getPort());
                controlledAcquiring = null;
                allChannels.add(controlledBind.channel());
                controlledBind.sync();
                listen = controlledBind.channel();
            } else {
                listen = bootstrap.bind(address.getAddress(), address.getPort()).sync().channel();
            }

            if (listen.localAddress() instanceof InetSocketAddress) {
                if (conf.getBookiePort() == 0) {
                    // this is really really nasty. It's using the configuration object as a notification
                    // bus. We should get rid of this at some point
                    conf.setBookiePort(((InetSocketAddress) listen.localAddress()).getPort());
                }
            }

        }

        if (conf.isEnableLocalTransport()) {
            ServerBootstrap jvmBootstrap = new ServerBootstrap();
            jvmBootstrap.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
            jvmBootstrap.group(jvmEventLoopGroup, jvmEventLoopGroup);
            jvmBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                    new AdaptiveRecvByteBufAllocator(conf.getRecvByteBufAllocatorSizeMin(),
                            conf.getRecvByteBufAllocatorSizeInitial(), conf.getRecvByteBufAllocatorSizeMax()));
            jvmBootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    conf.getServerWriteBufferLowWaterMark(), conf.getServerWriteBufferHighWaterMark()));

            // Local transport (BOOKKEEPER-896) is an in-VM transport that does not involve
            // the network stack, so network-related socket options such as TCP_NODELAY,
            // SO_KEEPALIVE, SO_LINGER and TCP_KEEP* are not applicable and must not be set here.
            jvmBootstrap.channel(LocalServerChannel.class);

            jvmBootstrap.childHandler(new ChannelInitializer<LocalChannel>() {
                @Override
                protected void initChannel(LocalChannel ch) throws Exception {
                    synchronized (suspensionLock) {
                        while (suspended) {
                            suspensionLock.wait();
                        }
                    }

                    BookieSideConnectionPeerContextHandler contextHandler =
                        new BookieSideConnectionPeerContextHandler();
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));

                    pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder());
                    pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder());
                    pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(
                                contextHandler.getConnectionPeer(), authProviderFactory));

                    ChannelInboundHandler requestHandler = isRunning.get()
                            ? new BookieRequestHandler(conf, requestProcessor, allChannels)
                            : new RejectRequestHandler();
                    pipeline.addLast("bookieRequestHandler", requestHandler);

                    pipeline.addLast("contextHandler", contextHandler);
                }
            });
            log.info().attr("bookieId", bookieId.toString()).log("Binding jvm bookie-rpc endpoint");
            // use the same address 'name', so clients can find local Bookie still discovering them using ZK
            jvmBootstrap.bind(new LocalAddress(bookieId.toString())).sync();
            LocalBookiesRegistry.registerLocalBookieAddress(bookieId);
        }
    }

    void start() throws InterruptedException {
        if (controlled) {
            throw new IllegalStateException("Controlled TCP activation belongs to its complete child graph");
        }
        isRunning.set(true);
    }

    void shutdown() {
        if (controlled) {
            throw new IllegalStateException("Controlled TCP requires actual owner drain");
        }
        log.info("Shutting down BookieNettyServer");
        isRunning.set(false);

        if (!isClosed.compareAndSet(false, true)) {
            // the netty server is already closed.
            return;
        }

        allChannels.close().awaitUninterruptibly();

        if (acceptorGroup != null) {
            try {
                acceptorGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (eventLoopGroup != null) {
            try {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                /// OK
            }
        }
        if (jvmEventLoopGroup != null) {
            LocalBookiesRegistry.unregisterLocalBookieAddress(bookieAddress.toBookieId());
            jvmEventLoopGroup.shutdownGracefully();
        }

        authProviderFactory.close();
    }

    private static class RejectRequestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().close();
        }
    }

    private static class CleanupChannelGroup extends DefaultChannelGroup {

        private AtomicBoolean closed = new AtomicBoolean(false);

        public CleanupChannelGroup(EventLoopGroup eventLoopGroup) {
            super("BookieChannelGroup", eventLoopGroup.next());
        }

        @Override
        public boolean add(Channel channel) {
            boolean ret = super.add(channel);
            if (closed.get()) {
                channel.close();
            }
            return ret;
        }

        @Override
        public ChannelGroupFuture close() {
            closed.set(true);
            return super.close();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CleanupChannelGroup)) {
                return false;
            }
            CleanupChannelGroup other = (CleanupChannelGroup) o;
            return other.closed.get() == closed.get()
                && super.equals(other);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 17 + (closed.get() ? 1 : 0);
        }
    }
}

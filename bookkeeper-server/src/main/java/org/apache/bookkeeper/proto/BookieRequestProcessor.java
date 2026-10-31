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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.proto.RequestUtils.hasFlag;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityHandlerFactory.NodeType;
import org.apache.bookkeeper.util.NettyChannelUtil;
import org.slf4j.MDC;

/**
 * An implementation of the RequestProcessor interface.
 */
@Getter(AccessLevel.PACKAGE)
@CustomLog
public class BookieRequestProcessor implements RequestProcessor {
    public static final String TLS_HANDLER_NAME = "tls";

    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private final ServerConfiguration serverCfg;
    private final long waitTimeoutOnBackpressureMillis;
    private final boolean preserveMdcForTaskExecution;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    final Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private OrderedExecutor readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private OrderedExecutor writeThreadPool;

    /**
     * TLS management.
     */
    private final SecurityHandlerFactory shFactory;

    /**
     * The threadpool used to execute all long poll requests issued to this server
     * after they are done waiting.
     */
    private OrderedExecutor longPollThreadPool;

    /**
     * The threadpool used to execute high priority requests.
     */
    private OrderedExecutor highPriorityThreadPool;

    /**
     * The Timer used to time out requests for long polling.
     */
    private HashedWheelTimer requestTimer;

    // Expose Stats
    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;

    private RequestStats requestStats;

    final Semaphore addsSemaphore;
    final Semaphore readsSemaphore;

    final ChannelGroup allChannels;

    // to temporary blacklist channels
    final Optional<Cache<Channel, Boolean>> blacklistedChannels;
    final Consumer<Channel> onResponseTimeout;

    private final ByteBufAllocator allocator;

    private final boolean throttleReadResponses;
    private final StatsLogger statsLogger;
    final ClassicRequestLifecycle classic;
    private boolean classicInitialized;
    private boolean classicAttempted;
    private boolean classicTimerStopped;
    private final Set<OrderedExecutor> classicShutdowns = Collections.newSetFromMap(new IdentityHashMap<>());

    public BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie, StatsLogger statsLogger,
                                  SecurityHandlerFactory shFactory, ByteBufAllocator allocator,
                                  ChannelGroup allChannels) throws SecurityException {
        this(serverCfg, bookie, statsLogger, shFactory, allocator, allChannels, null);
        initialize();
    }

    private BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie, StatsLogger statsLogger,
            SecurityHandlerFactory shFactory, ByteBufAllocator allocator, ChannelGroup allChannels,
            ClassicRequestLifecycle classic) {
        this.serverCfg = serverCfg;
        this.allocator = allocator;
        this.allChannels = allChannels;
        this.waitTimeoutOnBackpressureMillis = serverCfg.getWaitTimeoutOnResponseBackpressureMillis();
        this.preserveMdcForTaskExecution = serverCfg.getPreserveMdcForTaskExecution();
        this.bookie = bookie;
        this.throttleReadResponses = serverCfg.isReadWorkerThreadsThrottlingEnabled();
        this.shFactory = shFactory;
        this.statsLogger = statsLogger;
        this.classic = classic;
        if (waitTimeoutOnBackpressureMillis > 0) {
            blacklistedChannels = Optional.of(CacheBuilder.newBuilder()
                    .expireAfterWrite(waitTimeoutOnBackpressureMillis, TimeUnit.MILLISECONDS)
                    .build());
        } else {
            blacklistedChannels = Optional.empty();
        }

        if (serverCfg.getCloseChannelOnResponseTimeout()) {
            onResponseTimeout = (ch) -> {
                log.warn()
                        .attr("channel", ch)
                        .attr("timeoutMs", waitTimeoutOnBackpressureMillis)
                        .log("closing channel because it was non-writable for too long");
                ch.close();
            };
        } else {
            // noop
            onResponseTimeout = (ch) -> {};
        }

        // Expose Stats
        this.statsEnabled = serverCfg.isStatisticsEnabled();

        int maxAdds = serverCfg.getMaxAddsInProgressLimit();
        addsSemaphore = maxAdds > 0 ? new Semaphore(maxAdds, true) : null;

        int maxReads = serverCfg.getMaxReadsInProgressLimit();
        readsSemaphore = maxReads > 0 ? new Semaphore(maxReads, true) : null;
    }

    static BookieRequestProcessor prepareForClassicChild(ServerConfiguration conf, Bookie bookie, StatsLogger stats,
            ByteBufAllocator allocator, ChannelGroup channels, BookieServer owner) {
        return new BookieRequestProcessor(conf, bookie, stats, null, allocator, channels,
                new ClassicRequestLifecycle(owner));
    }

    void initializeForClassicChild() throws SecurityException {
        if (classic == null || classicAttempted || classic.isClosing()) {
            throw new IllegalStateException("Controlled processor initializes once after owner registration");
        }
        classicAttempted = true;
        try {
            initialize();
            if (classic.isClosing()) {
                throw new IllegalStateException("Controlled processor initialization was retired");
            }
            classicInitialized = true;
        } catch (SecurityException | RuntimeException | Error failure) {
            classic.failUnknown(failure);
            throw failure;
        }
    }

    private void initialize() throws SecurityException {
        this.readThreadPool = createExecutor(
                this.serverCfg.getNumReadWorkerThreads(),
                "BookieReadThreadPool",
                serverCfg.getMaxPendingReadRequestPerThread(),
                statsLogger);
        this.writeThreadPool = createExecutor(
                this.serverCfg.getNumAddWorkerThreads(),
                "BookieWriteThreadPool",
                serverCfg.getMaxPendingAddRequestPerThread(),
                statsLogger);
        if (serverCfg.getNumLongPollWorkerThreads() <= 0 && readThreadPool != null) {
            this.longPollThreadPool = this.readThreadPool;
        } else {
            int numThreads = this.serverCfg.getNumLongPollWorkerThreads();
            if (numThreads <= 0) {
                numThreads = Runtime.getRuntime().availableProcessors();
            }
            this.longPollThreadPool = createExecutor(
                numThreads,
                "BookieLongPollThread",
                OrderedExecutor.NO_TASK_LIMIT, statsLogger);
        }
        this.highPriorityThreadPool = createExecutor(
                this.serverCfg.getNumHighPriorityWorkerThreads(),
                "BookieHighPriorityThread",
                OrderedExecutor.NO_TASK_LIMIT, statsLogger);
        if (shFactory != null) {
            shFactory.init(NodeType.Server, serverCfg, allocator);
        }

        if (classic != null) {
            classic.acquiring("request timer construction");
        }
        var timerFactory = new ThreadFactoryBuilder().setNameFormat("BookieRequestTimer-%d").build();
        this.requestTimer = new HashedWheelTimer(
                classic == null ? timerFactory : classic.trackThreads(timerFactory),
                this.serverCfg.getRequestTimerTickDurationMs(),
                TimeUnit.MILLISECONDS, this.serverCfg.getRequestTimerNumTicks());

        if (classic != null) {
            classic.acquired();
        }
        this.requestStats = new RequestStats(statsLogger);
    }

    protected void onAddRequestStart(Channel channel) {
        if (addsSemaphore != null) {
            if (!addsSemaphore.tryAcquire()) {
                final long throttlingStartTimeNanos = MathUtils.nowInNano();
                channel.config().setAutoRead(false);
                log.info().attr("channel", channel).log("Too many add requests in progress, disabling autoread");
                requestStats.blockAddRequest();
                addsSemaphore.acquireUninterruptibly();
                channel.config().setAutoRead(true);
                final long delayNanos = MathUtils.elapsedNanos(throttlingStartTimeNanos);
                log.info()
                        .attr("channel", channel)
                        .attr("delayNanos", delayNanos)
                        .log("Re-enabled autoread on channel after AddRequest delay");
                requestStats.unblockAddRequest(delayNanos);
            }
        }
        requestStats.trackAddRequest();
    }

    protected void onAddRequestFinish() {
        requestStats.untrackAddRequest();
        if (addsSemaphore != null) {
            addsSemaphore.release();
        }
    }

    protected void onReadRequestStart(Channel channel) {
        if (readsSemaphore != null) {
            if (!readsSemaphore.tryAcquire()) {
                final long throttlingStartTimeNanos = MathUtils.nowInNano();
                channel.config().setAutoRead(false);
                log.info().attr("channel", channel).log("Too many read requests in progress, disabling autoread");
                requestStats.blockReadRequest();
                readsSemaphore.acquireUninterruptibly();
                channel.config().setAutoRead(true);
                final long delayNanos = MathUtils.elapsedNanos(throttlingStartTimeNanos);
                log.info()
                        .attr("channel", channel)
                        .attr("delayNanos", delayNanos)
                        .log("Re-enabled autoread on channel after ReadRequest delay");
                requestStats.unblockReadRequest(delayNanos);
            }
        }
        requestStats.trackReadRequest();
    }

    protected void onReadRequestFinish() {
        requestStats.untrackReadRequest();
        if (readsSemaphore != null) {
            readsSemaphore.release();
        }
    }

    @VisibleForTesting
    int maxAddsInProgressCount() {
        return requestStats.maxAddsInProgressCount();
    }

    @VisibleForTesting
    int maxReadsInProgressCount() {
        return requestStats.maxReadsInProgressCount();
    }

    @Override
    public void close() {
        if (classic != null) {
            throw new IllegalStateException("Controlled processor requires actual external owner drain");
        }
        log.info("Closing RequestProcessor");
        shutdownExecutor(writeThreadPool);
        shutdownExecutor(readThreadPool);
        if (serverCfg.getNumLongPollWorkerThreads() > 0 || readThreadPool == null) {
            shutdownExecutor(longPollThreadPool);
        }
        shutdownExecutor(highPriorityThreadPool);
        requestTimer.stop();
        log.info("Closed RequestProcessor");
    }

    private OrderedExecutor createExecutor(
            int numThreads,
            String nameFormat,
            int maxTasksInQueue,
            StatsLogger statsLogger) {
        if (numThreads <= 0) {
            return null;
        } else {
            var builder = OrderedExecutor.newBuilder()
                    .numThreads(numThreads)
                    .name(nameFormat)
                    .traceTaskExecution(serverCfg.getEnableTaskExecutionStats())
                    .preserveMdcForTaskExecution(serverCfg.getPreserveMdcForTaskExecution())
                    .statsLogger(statsLogger)
                    .maxTasksInQueue(maxTasksInQueue)
                    .enableThreadScopedMetrics(true);
            if (classic == null) {
                return builder.build();
            }
            classic.acquiring(nameFormat);
            builder.threadFactory(classic.trackThreads(new io.netty.util.concurrent.DefaultThreadFactory(nameFormat)));
            var executor = builder.build();
            classic.acquired();
            return executor;
        }
    }

    private void shutdownExecutor(OrderedExecutor service) {
        if (null != service) {
            service.shutdown();
            service.forceShutdown(10, TimeUnit.SECONDS);
        }
    }

    @Override
    public void processRequest(Object msg, BookieRequestHandler requestHandler) {
        if (classic == null) {
            processAcceptedRequest(msg, requestHandler);
            return;
        }
        if (msg instanceof Request request && !validControlledRequest(request)) {
            requestHandler.ctx().close();
            return;
        }
        var call = classic.begin(msg, requestHandler.ctx().channel());
        if (call == null) {
            if (msg instanceof BookieProtocol.ParsedAddRequest add) {
                add.release();
                add.recycle();
            } else if (msg instanceof BookieProtocol.Request legacy) {
                legacy.recycle();
            }
            requestHandler.ctx().close();
            return;
        }
        Throwable problem = null;
        try {
            processAcceptedRequest(msg, requestHandler);
        } catch (RejectedExecutionException rejected) {
            if (call.discardRejected()) {
                return;
            }
            problem = rejected;
            throw rejected;
        } catch (RuntimeException | Error failure) {
            problem = failure;
            throw failure;
        } finally {
            classic.end(call, problem);
        }
    }

    private static boolean validControlledRequest(Request request) {
        if (!request.hasHeader()) {
            return false;
        }
        return switch (request.getHeader().getOperation()) {
            case ADD_ENTRY -> request.hasAddRequest();
            case READ_ENTRY -> request.hasReadRequest();
            case FORCE_LEDGER -> request.hasForceLedgerRequest();
            case WRITE_LAC -> request.hasWriteLacRequest();
            case READ_LAC -> request.hasReadLacRequest();
            case GET_BOOKIE_INFO -> request.hasGetBookieInfoRequest();
            case GET_LIST_OF_ENTRIES_OF_LEDGER -> request.hasGetListOfEntriesOfLedgerRequest();
            default -> true;
        };
    }

    private void processAcceptedRequest(Object msg, BookieRequestHandler requestHandler) {
        Channel channel = requestHandler.ctx().channel();
        // If we can decode this packet as a Request protobuf packet, process
        // it as a version 3 packet. Else, just use the old protocol.
        if (msg instanceof Request) {
            Request r = (Request) msg;
            restoreMdcContextFromRequest(r);
            try {
                BKPacketHeader header = r.getHeader();
                switch (header.getOperation()) {
                    case ADD_ENTRY:
                        processAddRequestV3(r, requestHandler);
                        break;
                    case READ_ENTRY:
                        processReadRequestV3(r, requestHandler);
                        break;
                    case FORCE_LEDGER:
                        processForceLedgerRequestV3(r, requestHandler);
                        break;
                    case AUTH:
                        log.info()
                                .attr("clientAddress", channel.remoteAddress())
                                .log("Ignoring auth operation from client");
                        Response authResponse = new Response();
                        authResponse.setHeader().copyFrom(r.getHeader());
                        authResponse.setStatus(StatusCode.EOK)
                                .setAuthResponse()
                                .setAuthPluginName(AuthProviderFactoryFactory.AUTHENTICATION_DISABLED_PLUGIN_NAME)
                                .setPayload(AuthToken.NULL.getData());
                        writeAndFlush(channel, authResponse);
                        break;
                    case WRITE_LAC:
                        processWriteLacRequestV3(r, requestHandler);
                        break;
                    case READ_LAC:
                        processReadLacRequestV3(r, requestHandler);
                        break;
                    case GET_BOOKIE_INFO:
                        processGetBookieInfoRequestV3(r, requestHandler);
                        break;
                    case START_TLS:
                        processStartTLSRequestV3(r, requestHandler);
                        break;
                    case GET_LIST_OF_ENTRIES_OF_LEDGER:
                        processGetListOfEntriesOfLedgerProcessorV3(r, requestHandler);
                        break;
                    default:
                        log.info().attr("operationType", header.getOperation()).log("Unknown operation type");
                        Response response = new Response();
                        response.setHeader().copyFrom(r.getHeader());
                        response.setStatus(StatusCode.EBADREQ);
                        writeAndFlush(channel, response);
                        if (statsEnabled) {
                            bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                        }
                        break;
                }
            } finally {
                MDC.clear();
            }
        } else {
            BookieProtocol.Request r = (BookieProtocol.Request) msg;
            // process packet
            switch (r.getOpCode()) {
                case BookieProtocol.ADDENTRY:
                    checkArgument(r instanceof BookieProtocol.ParsedAddRequest);
                    processAddRequest((BookieProtocol.ParsedAddRequest) r, requestHandler);
                    break;
                case BookieProtocol.READENTRY:
                    checkArgument(r instanceof BookieProtocol.ReadRequest);
                    processReadRequest((BookieProtocol.ReadRequest) r, requestHandler);
                    break;
                case BookieProtocol.BATCH_READ_ENTRY:
                    checkArgument(r instanceof BookieProtocol.BatchedReadRequest);
                    processReadRequest((BookieProtocol.BatchedReadRequest) r, requestHandler);
                    break;
                case BookieProtocol.AUTH:
                    log.info().attr("clientAddress", requestHandler.ctx().channel().remoteAddress())
                            .log("Ignoring auth operation from client");
                    AuthMessage message = new AuthMessage()
                            .setAuthPluginName(AuthProviderFactoryFactory.AUTHENTICATION_DISABLED_PLUGIN_NAME)
                            .setPayload(AuthToken.NULL.getData());

                    final BookieProtocol.AuthResponse response = new BookieProtocol.AuthResponse(
                            BookieProtocol.CURRENT_PROTOCOL_VERSION, message);
                    writeAndFlush(channel, response);
                    break;
                default:
                    log.error().attr("opCode", r.getOpCode()).log("Unknown op type, sending error");
                    final BookieProtocol.Response errResponse = ResponseBuilder
                            .buildErrorResponse(BookieProtocol.EBADREQ, r);
                    writeAndFlush(channel, errResponse);
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                    }
                    break;
            }
        }
    }

    private void restoreMdcContextFromRequest(Request req) {
        if (preserveMdcForTaskExecution) {
            MDC.clear();
            for (int i = 0; i < req.getRequestContextsCount(); i++) {
                ContextPair pair = req.getRequestContextAt(i);
                MDC.put(pair.getKey(), pair.getValue());
            }
        }
    }

    private void processWriteLacRequestV3(final Request r,
                                          final BookieRequestHandler requestHandler) {
        WriteLacProcessorV3 writeLac = new WriteLacProcessorV3(r, requestHandler, this);
        if (null == writeThreadPool) {
            runClassicTask(writeLac);
        } else {
            executeClassicOrdered(writeThreadPool, r.getWriteLacRequest().getLedgerId(), writeLac);
        }
    }

    private void processReadLacRequestV3(final Request r,
                                         final BookieRequestHandler requestHandler) {
        ReadLacProcessorV3 readLac = new ReadLacProcessorV3(r, requestHandler, this);
        if (null == readThreadPool) {
            runClassicTask(readLac);
        } else {
            executeClassicOrdered(readThreadPool, r.getReadLacRequest().getLedgerId(), readLac);
        }
    }

    private void processAddRequestV3(final Request r, final BookieRequestHandler requestHandler) {
        WriteEntryProcessorV3 write = new WriteEntryProcessorV3(r, requestHandler, this);

        final OrderedExecutor threadPool;
        if (RequestUtils.isHighPriority(r)) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            runClassicTask(write);
        } else {
            try {
                executeClassicOrdered(threadPool, r.getAddRequest().getLedgerId(), write);
            } catch (RejectedExecutionException e) {
                log.debug().attr("ledgerId", () -> r.getAddRequest().getLedgerId())
                        .attr("entryId", () -> r.getAddRequest().getEntryId())
                        .log("Failed to process request to add entry. Too many pending requests");
                getRequestStats().getAddEntryRejectedCounter().inc();
                Response resp = new Response();
                resp.setHeader().copyFrom(write.getHeader());
                resp.setStatus(StatusCode.ETOOMANYREQUESTS);
                resp.setAddResponse()
                        .setLedgerId(r.getAddRequest().getLedgerId())
                        .setEntryId(r.getAddRequest().getEntryId())
                        .setStatus(StatusCode.ETOOMANYREQUESTS);
                write.sendResponse(StatusCode.ETOOMANYREQUESTS, resp, requestStats.getAddRequestStats());
            }
        }
    }

    private void processForceLedgerRequestV3(final Request r,
                                             final BookieRequestHandler requestHandler) {
        ForceLedgerProcessorV3 forceLedger = new ForceLedgerProcessorV3(r, requestHandler, this);

        final OrderedExecutor threadPool;
        if (RequestUtils.isHighPriority(r)) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            runClassicTask(forceLedger);
        } else {
            try {
                executeClassicOrdered(threadPool, r.getForceLedgerRequest().getLedgerId(), forceLedger);
            } catch (RejectedExecutionException e) {
                log.debug().attr("ledgerId", () -> r.getForceLedgerRequest().getLedgerId())
                        .log("Failed to process request to force ledger. Too many pending requests");
                Response resp = new Response();
                resp.setHeader().copyFrom(forceLedger.getHeader());
                resp.setStatus(StatusCode.ETOOMANYREQUESTS);
                resp.setForceLedgerResponse()
                        .setLedgerId(r.getForceLedgerRequest().getLedgerId())
                        .setStatus(StatusCode.ETOOMANYREQUESTS);
                forceLedger.sendResponse(
                    StatusCode.ETOOMANYREQUESTS,
                    resp,
                    requestStats.getForceLedgerRequestStats());
            }
        }
    }

    private void processReadRequestV3(final Request r, final BookieRequestHandler requestHandler) {
        ExecutorService fenceThread = null == highPriorityThreadPool ? null :
                highPriorityThreadPool.chooseThread(requestHandler.ctx());

        final ReadEntryProcessorV3 read;
        final OrderedExecutor threadPool;
        if (RequestUtils.isLongPollReadRequest(r.getReadRequest())) {
            ExecutorService lpThread = longPollThreadPool.chooseThread(requestHandler.ctx());

            read = new LongPollReadEntryProcessorV3(r, requestHandler, this, fenceThread,
                                                    lpThread, requestTimer);
            threadPool = longPollThreadPool;
        } else {
            read = new ReadEntryProcessorV3(r, requestHandler, this, fenceThread);

            // If it's a high priority read (fencing or as part of recovery process), we want to make sure it
            // gets executed as fast as possible, so bypass the normal readThreadPool
            // and execute in highPriorityThreadPool
            boolean isHighPriority = RequestUtils.isHighPriority(r)
                || hasFlag(r.getReadRequest(), ReadRequest.Flag.FENCE_LEDGER);
            if (isHighPriority) {
                threadPool = highPriorityThreadPool;
            } else {
                threadPool = readThreadPool;
            }
        }

        if (null == threadPool) {
            runClassicTask(read);
        } else {
            try {
                executeClassicOrdered(threadPool, r.getReadRequest().getLedgerId(), read);
            } catch (RejectedExecutionException e) {
                log.debug().attr("ledgerId", () -> r.getReadRequest().getLedgerId())
                        .attr("entryId", () -> r.getReadRequest().getEntryId())
                        .log("Failed to process request to read entry. Too many pending requests");
                getRequestStats().getReadEntryRejectedCounter().inc();
                Response resp = new Response();
                resp.setHeader().copyFrom(read.getHeader());
                resp.setStatus(StatusCode.ETOOMANYREQUESTS);
                resp.setReadResponse()
                        .setLedgerId(r.getReadRequest().getLedgerId())
                        .setEntryId(r.getReadRequest().getEntryId())
                        .setStatus(StatusCode.ETOOMANYREQUESTS);
                read.sendResponse(StatusCode.ETOOMANYREQUESTS, resp, requestStats.getReadRequestStats());
                onReadRequestFinish();
            }
        }
    }

    private void processStartTLSRequestV3(final Request r,
                                          final BookieRequestHandler requestHandler) {
        Response response = new Response();
        response.setHeader()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(r.getHeader().getOperation())
                .setTxnId(r.getHeader().getTxnId());
        final Channel c = requestHandler.ctx().channel();

        if (shFactory == null) {
            log.error("Got StartTLS request but TLS not configured");
            response.setStatus(StatusCode.EBADREQ);
            writeAndFlush(c, response);
        } else {
            log.info().attr("channel", c).log("Starting TLS handshake with client");
            // there is no need to execute in a different thread as this operation is light
            SslHandler sslHandler = shFactory.newTLSHandler();
            if (c.pipeline().names().contains(BookieNettyServer.CONSOLIDATION_HANDLER_NAME)) {
                c.pipeline().addAfter(BookieNettyServer.CONSOLIDATION_HANDLER_NAME, TLS_HANDLER_NAME, sslHandler);
            } else {
                // local transport doesn't contain FlushConsolidationHandler
                c.pipeline().addFirst(TLS_HANDLER_NAME, sslHandler);
            }

            response.setStatus(StatusCode.EOK);
            response.setStartTLSResponse();
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    // notify the AuthPlugin the completion of the handshake, even in case of failure
                    AuthHandler.ServerSideHandler authHandler = c.pipeline()
                            .get(AuthHandler.ServerSideHandler.class);
                    authHandler.authProvider.onProtocolUpgrade();

                    /*
                     * Success of the future doesn't guarantee success in authentication
                     * future.isSuccess() only checks if the result field is not null
                     */
                    if (future.isSuccess() && authHandler.isAuthenticated()) {
                        log.info().attr("cipherSuite", sslHandler.engine().getSession().getCipherSuite())
                                .log("Session is protected");
                    } else {
                        if (future.isSuccess()) {
                            log.error("TLS Handshake failed: Could not authenticate.");
                        } else {
                            log.error().exception(future.cause()).log("TLS Handshake failure");
                        }
                        Response errResponse = new Response();
                        errResponse.setHeader().copyFrom(r.getHeader());
                        errResponse.setStatus(StatusCode.EIO);
                        writeAndFlush(c, errResponse);
                        if (statsEnabled) {
                            bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                        }
                    }
                }
            });
            writeAndFlush(c, response);
        }
    }

    private void processGetBookieInfoRequestV3(final Request r,
                                               final BookieRequestHandler requestHandler) {
        GetBookieInfoProcessorV3 getBookieInfo = new GetBookieInfoProcessorV3(r, requestHandler, this);
        if (null == readThreadPool) {
            runClassicTask(getBookieInfo);
        } else {
            submitClassicTask(readThreadPool, getBookieInfo);
        }
    }

    private void processGetListOfEntriesOfLedgerProcessorV3(final Request r,
                                                            final BookieRequestHandler requestHandler) {
        GetListOfEntriesOfLedgerProcessorV3 getListOfEntriesOfLedger =
                new GetListOfEntriesOfLedgerProcessorV3(r, requestHandler, this);
        if (null == readThreadPool) {
            runClassicTask(getListOfEntriesOfLedger);
        } else {
            submitClassicTask(readThreadPool, getListOfEntriesOfLedger);
        }
    }

    private void processAddRequest(final BookieProtocol.ParsedAddRequest r, final BookieRequestHandler requestHandler) {
        WriteEntryProcessor write = WriteEntryProcessor.create(r, requestHandler, this);

        // If it's a high priority add (usually as part of recovery process), we want to make sure it gets
        // executed as fast as possible, so bypass the normal writeThreadPool and execute in highPriorityThreadPool
        final OrderedExecutor threadPool;
        if (r.isHighPriority()) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            runClassicTask(write);
        } else {
            try {
                executeClassicOrdered(threadPool, r.getLedgerId(), write);
            } catch (RejectedExecutionException e) {
                log.debug()
                        .attr("ledgerId", r.ledgerId)
                        .attr("entryId", r.entryId)
                        .log("Failed to process request to add entry. Too many pending requests");
                getRequestStats().getAddEntryRejectedCounter().inc();

                write.sendWriteReqResponse(
                    BookieProtocol.ETOOMANYREQUESTS,
                    ResponseBuilder.buildErrorResponse(BookieProtocol.ETOOMANYREQUESTS, r),
                    requestStats.getAddRequestStats());
                r.release();
                r.recycle();
                write.recycle();
            }
        }
    }

    private void processReadRequest(final BookieProtocol.ReadRequest r, final BookieRequestHandler requestHandler) {
        ExecutorService fenceThreadPool =
                null == highPriorityThreadPool ? null : highPriorityThreadPool.chooseThread(requestHandler.ctx());
        ReadEntryProcessor read = r instanceof BookieProtocol.BatchedReadRequest
                ? BatchedReadEntryProcessor.create((BookieProtocol.BatchedReadRequest) r, requestHandler,
                this, fenceThreadPool, throttleReadResponses, serverCfg.getMaxBatchReadSize())
                : ReadEntryProcessor.create(r, requestHandler,
                        this, fenceThreadPool, throttleReadResponses);

        // If it's a high priority read (fencing or as part of recovery process), we want to make sure it
        // gets executed as fast as possible, so bypass the normal readThreadPool
        // and execute in highPriorityThreadPool
        final OrderedExecutor threadPool;
        if (r.isHighPriority() || r.isFencing()) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = readThreadPool;
        }

        if (null == threadPool) {
            runClassicTask(read);
        } else {
            try {
                executeClassicOrdered(threadPool, r.getLedgerId(), read);
            } catch (RejectedExecutionException e) {
                log.debug()
                        .attr("ledgerId", r.ledgerId)
                        .attr("entryId", r.entryId)
                        .log("Failed to process request to read entry. Too many pending requests");
                getRequestStats().getReadEntryRejectedCounter().inc();
                read.sendResponse(
                    BookieProtocol.ETOOMANYREQUESTS,
                    ResponseBuilder.buildErrorResponse(BookieProtocol.ETOOMANYREQUESTS, r),
                    requestStats.getReadRequestStats());
                onReadRequestFinish();
                read.recycle();
            }
        }
    }

    public long getWaitTimeoutOnBackpressureMillis() {
        return waitTimeoutOnBackpressureMillis;
    }

    public void blacklistChannel(Channel channel) {
        blacklistedChannels
                .ifPresent(x -> x.put(channel, true));
    }

    public void invalidateBlacklist(Channel channel) {
        blacklistedChannels
                .ifPresent(x -> x.invalidate(channel));
    }

    public boolean isBlacklisted(Channel channel) {
        return blacklistedChannels
                .map(x -> x.getIfPresent(channel))
                .orElse(false);
    }

    public void handleNonWritableChannel(Channel channel) {
        onResponseTimeout.accept(channel);
    }

    private void writeAndFlush(Channel channel, Object msg) {
        try {
            if (classic == null) {
                NettyChannelUtil.writeAndFlushWithVoidPromise(channel, msg);
            } else {
                classic.writeAndFlush(channel, msg);
            }
        } finally {
            if (classic != null && classic.current() != null) {
                classic.current().complete();
            }
        }
    }

    private void runClassicTask(Runnable body) {
        if (classic == null) {
            body.run();
        } else {
            classic.task(body).run();
        }
    }

    private void executeClassicOrdered(OrderedExecutor executor, long key, Runnable body) {
        if (classic == null) {
            executor.executeOrdered(key, body);
            return;
        }
        var task = classic.task(body);
        try {
            executor.executeOrdered(key, task);
        } catch (RejectedExecutionException rejected) {
            task.rejected();
            throw rejected;
        } catch (RuntimeException | Error unknown) {
            classic.failUnknown(unknown);
            throw unknown;
        }
    }

    private void submitClassicTask(OrderedExecutor executor, Runnable body) {
        if (classic == null) {
            executor.submit(body);
            return;
        }
        var task = classic.task(body);
        try {
            executor.submit(task);
        } catch (RejectedExecutionException rejected) {
            task.rejected();
            throw rejected;
        } catch (RuntimeException | Error unknown) {
            classic.failUnknown(unknown);
            throw unknown;
        }
    }

    boolean initializedForClassicChild() {
        return classicInitialized && classic != null && !classic.isClosing();
    }

    boolean finishForClassicChild() throws InterruptedException {
        if (classic == null || classic.isCurrentActivity()) {
            throw new IllegalStateException("Controlled processor cannot await its own activity");
        }
        if (!classic.awaitRequests() || !classic.awaitOutputs()) {
            return false;
        }
        for (OrderedExecutor executor : new OrderedExecutor[]{writeThreadPool, readThreadPool,
                longPollThreadPool, highPriorityThreadPool}) {
            if (executor == null) {
                continue;
            }
            if (classicShutdowns.add(executor)) {
                executor.shutdown();
            }
        }
        if (requestTimer != null && !classicTimerStopped) {
            classicTimerStopped = true;
            requestTimer.stop();
        }
        return classic.joinThreads();
    }
}

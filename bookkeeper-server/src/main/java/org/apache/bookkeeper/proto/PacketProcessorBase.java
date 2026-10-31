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
package org.apache.bookkeeper.proto;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.StringUtils;

/**
 * A base class for bookeeper packet processors.
 */
@CustomLog
abstract class PacketProcessorBase<T extends Request> implements Runnable {
    T request;
    BookieRequestHandler requestHandler;
    BookieRequestProcessor requestProcessor;
    long enqueueNanos;
    ClassicRequestLifecycle.Call controlledCall;

    protected void init(T request, BookieRequestHandler requestHandler, BookieRequestProcessor requestProcessor) {
        this.request = request;
        this.requestHandler = requestHandler;
        this.requestProcessor = requestProcessor;
        this.controlledCall = requestProcessor.classic == null ? null : requestProcessor.classic.current();
        this.enqueueNanos = MathUtils.nowInNano();
    }

    protected void reset() {
        request = null;
        requestHandler = null;
        requestProcessor = null;
        controlledCall = null;
        enqueueNanos = -1;
    }

    protected boolean isVersionCompatible() {
        byte version = request.getProtocolVersion();
        if (version < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                || version > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            log.error().attr("expectedMin", BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION)
                    .attr("expectedMax", BookieProtocol.CURRENT_PROTOCOL_VERSION)
                    .attr("actual", request.getProtocolVersion())
                    .log("Invalid protocol version");
            return false;
        }
        return true;
    }

    protected void sendWriteReqResponse(int rc, Object response, OpStatsLogger statsLogger) {
        sendResponse(rc, response, statsLogger);
        finishAddRequest();
    }

    protected void sendReadReqResponse(int rc, Object response, OpStatsLogger statsLogger, boolean throttle) {
        if (throttle) {
            sendResponseAndWait(rc, response, statsLogger);
        } else {
            sendResponse(rc, response, statsLogger);
        }
        finishReadRequest();
    }

    protected void sendResponse(int rc, Object response, OpStatsLogger statsLogger) {
        var original = controlledCall;
        try {
            sendResponseUntracked(rc, response, statsLogger);
        } finally {
            if (original != null) {
                original.complete();
            }
        }
    }

    private void sendResponseUntracked(int rc, Object response, OpStatsLogger statsLogger) {
        final long writeNanos = MathUtils.nowInNano();
        final long timeOut = requestProcessor.getWaitTimeoutOnBackpressureMillis();

        Channel channel = requestHandler.ctx().channel();

        if (timeOut >= 0 && !channel.isWritable()) {
            if (!requestProcessor.isBlacklisted(channel)) {
                synchronized (channel) {
                    if (!channel.isWritable() && !requestProcessor.isBlacklisted(channel)) {
                        final long waitUntilNanos = writeNanos + TimeUnit.MILLISECONDS.toNanos(timeOut);
                        while (!channel.isWritable() && MathUtils.nowInNano() < waitUntilNanos) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                        if (!channel.isWritable()) {
                            requestProcessor.blacklistChannel(channel);
                            requestProcessor.handleNonWritableChannel(channel);
                        }
                    }
                }
            }

            if (!channel.isWritable()) {
                log.warn()
                        .attr("channel", channel)
                        .attr("request", StringUtils.requestToString(request))
                    .log("cannot write response to non-writable channel");
                requestProcessor.getRequestStats().getChannelWriteStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(writeNanos), TimeUnit.NANOSECONDS);
                statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
                if (response instanceof BookieProtocol.Response) {
                    ((BookieProtocol.Response) response).release();
                }
                return;
            } else {
                requestProcessor.invalidateBlacklist(channel);
            }
        }

        if (channel.isActive()) {
            final ChannelPromise promise = channel.newPromise().addListener(future -> {
                if (!future.isSuccess()) {
                    log.debug().exception(future.cause()).log("Netty channel write exception");
                }
            });
            if (controlledCall == null) {
                channel.writeAndFlush(response, promise);
            } else {
                requestProcessor.classic.writeAndFlush(channel, response, promise);
            }
        } else {
            if (response instanceof BookieProtocol.Response) {
                ((BookieProtocol.Response) response).release();
            }
            log.debug().attr("channel", channel)
                    .log("Netty channel is inactive, hence bypassing netty channel writeAndFlush during sendResponse");
        }
        if (BookieProtocol.EOK == rc) {
            statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        } else {
            statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Write on the channel and wait until the write is completed.
     *
     * <p>That will make the thread to get blocked until we're able to
     * write everything on the TCP stack, providing auto-throttling
     * and avoiding using too much memory when handling read-requests.
     */
    protected void sendResponseAndWait(int rc, Object response, OpStatsLogger statsLogger) {
        var original = controlledCall;
        try {
            sendResponseAndWaitUntracked(rc, response, statsLogger);
        } finally {
            if (original != null) {
                original.complete();
            }
        }
    }

    private void sendResponseAndWaitUntracked(int rc, Object response, OpStatsLogger statsLogger) {
        try {
            Channel channel = requestHandler.ctx().channel();
            ChannelFuture future = controlledCall == null ? channel.writeAndFlush(response)
                    : requestProcessor.classic.writeAndFlush(channel, response);
            if (!channel.eventLoop().inEventLoop()) {
                future.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            log.debug().exception(e).log("Netty channel write exception");
            return;
        }
        if (BookieProtocol.EOK == rc) {
            statsLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        } else {
            statsLogger.registerFailedEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void run() {
        if (request instanceof BookieProtocol.ReadRequest) {
            requestProcessor.getRequestStats().getReadEntrySchedulingDelayStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        }
        if (request instanceof BookieProtocol.ParsedAddRequest) {
            requestProcessor.getRequestStats().getWriteThreadQueuedLatency()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueNanos), TimeUnit.NANOSECONDS);
        }

        if (!isVersionCompatible()) {
            sendResponse(BookieProtocol.EBADVERSION,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, request),
                         requestProcessor.getRequestStats().getReadRequestStats());
            if (request instanceof BookieProtocol.ReadRequest) {
                finishReadRequest();
            }
            if (request instanceof BookieProtocol.ParsedAddRequest) {
                ((BookieProtocol.ParsedAddRequest) request).release();
                request.recycle();
                finishAddRequest();
            }
            return;
        }
        processPacket();
    }

    protected final void finishAddRequest() {
        if (controlledCall == null || controlledCall.finishRequestOnce()) {
            requestProcessor.onAddRequestFinish();
        }
    }

    protected final void finishReadRequest() {
        if (controlledCall == null || controlledCall.finishRequestOnce()) {
            requestProcessor.onReadRequestFinish();
        }
    }

    void discardForClassicChild() {
        var original = controlledCall;
        if (original == null) {
            throw new IllegalStateException("Only a bound controlled request can be discarded");
        }
        if (request instanceof BookieProtocol.ParsedAddRequest add) {
            add.release();
            add.recycle();
            finishAddRequest();
        } else if (request instanceof BookieProtocol.ReadRequest) {
            finishReadRequest();
        }
        recycle();
        original.complete();
    }

    abstract void recycle();

    protected abstract void processPacket();
}

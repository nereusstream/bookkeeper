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

import com.google.common.base.Stopwatch;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.common.util.Watcher;

/**
 * Processor handling long poll read entry request.
 */
@CustomLog
class LongPollReadEntryProcessorV3 extends ReadEntryProcessorV3 implements Watcher<LastAddConfirmedUpdateNotification> {

    private final Long previousLAC;
    private Optional<Long> lastAddConfirmedUpdateTime = Optional.empty();

    // long poll execution state
    private final ExecutorService longPollThreadPool;
    private final HashedWheelTimer requestTimer;
    private Timeout expirationTimerTask = null;
    private Future<?> deferredTask = null;
    private boolean shouldReadEntry = false;
    private boolean classicRetired;
    private boolean classicRegistering;
    private boolean classicWatching;
    private boolean classicCancelAttempted;
    private boolean classicDeferredAttempted;

    LongPollReadEntryProcessorV3(Request request,
                                 BookieRequestHandler requestHandler,
                                 BookieRequestProcessor requestProcessor,
                                 ExecutorService fenceThreadPool,
                                 ExecutorService longPollThreadPool,
                                 HashedWheelTimer requestTimer) {
        super(request, requestHandler, requestProcessor, fenceThreadPool);
        this.previousLAC = readRequest.getPreviousLAC();
        this.longPollThreadPool = longPollThreadPool;
        this.requestTimer = requestTimer;

    }

    @Override
    protected Long getPreviousLAC() {
        return previousLAC;
    }

    private synchronized boolean shouldReadEntry() {
        return shouldReadEntry;
    }

    @Override
    protected ReadResponse readEntry(ReadResponse readResponseBuilder,
                                     long entryId,
                                     Stopwatch startTimeSw)
            throws IOException, BookieException {
        if (RequestUtils.shouldPiggybackEntry(readRequest)) {
            if (!readRequest.hasPreviousLAC() || (BookieProtocol.LAST_ADD_CONFIRMED != entryId)) {
                // This is not a valid request - client bug?
                log.error()
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", entryId)
                        .log("Incorrect read request, entry piggyback requested incorrectly");
                return buildResponse(readResponseBuilder, StatusCode.EBADREQ, startTimeSw);
            } else {
                long knownLAC = requestProcessor.bookie.readLastAddConfirmed(ledgerId);
                readResponseBuilder.setMaxLAC(knownLAC);
                if (knownLAC > previousLAC) {
                    entryId = previousLAC + 1;
                    readResponseBuilder.setMaxLAC(knownLAC);
                    if (lastAddConfirmedUpdateTime.isPresent()) {
                        readResponseBuilder.setLacUpdateTimestamp(lastAddConfirmedUpdateTime.get());
                    }
                    log.debug()
                            .attr("entryId", entryId)
                            .attr("ledgerId", ledgerId)
                            .log("ReadLAC Piggy Back reading entry");
                    try {
                        return super.readEntry(readResponseBuilder, entryId, true, startTimeSw);
                    } catch (Bookie.NoEntryException e) {
                        requestProcessor.getRequestStats().getReadLastEntryNoEntryErrorCounter().inc();
                        log.info()
                                .attr("entryId", entryId)
                                .attr("ledgerId", ledgerId)
                                .attr("previousLAC", previousLAC)
                                .log("No entry found while piggyback reading entry");
                        // piggy back is best effort and this request can fail genuinely because of striping
                        // entries across the ensemble
                        return buildResponse(readResponseBuilder, StatusCode.EOK, startTimeSw);
                    }
                } else {
                    if (knownLAC < previousLAC) {
                        log.debug()
                                .attr("ledgerId", ledgerId)
                                .attr("previousLAC", previousLAC)
                                .attr("knownLAC", knownLAC)
                                .log("Found smaller lac when piggy back reading lac and entry");
                    }
                    return buildResponse(readResponseBuilder, StatusCode.EOK, startTimeSw);
                }
            }
        } else {
            return super.readEntry(readResponseBuilder, entryId, false, startTimeSw);
        }
    }

    private ReadResponse buildErrorResponse(StatusCode statusCode, Stopwatch sw) {
        ReadResponse readResponse = new ReadResponse()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        return buildResponse(readResponse, statusCode, sw);
    }

    private ReadResponse getLongPollReadResponse() {
        if (controlledCall != null) {
            return getClassicLongPollResponse();
        }
        return getUncontrolledLongPollResponse();
    }

    private ReadResponse getUncontrolledLongPollResponse() {
        if (!shouldReadEntry() && readRequest.hasTimeOut()) {
            log.trace().attr("previousLAC", previousLAC).log("Waiting For LAC Update");

            final Stopwatch startTimeSw = Stopwatch.createStarted();

            final boolean watched;
            try {
                watched = requestProcessor.getBookie().waitForLastAddConfirmedUpdate(ledgerId, previousLAC, this);
            } catch (Bookie.NoLedgerException e) {
                log.info()
                        .attr("ledgerId", ledgerId)
                        .attr("previousLAC", previousLAC)
                        .log("No ledger found while longpoll reading");
                return buildErrorResponse(StatusCode.ENOLEDGER, startTimeSw);
            } catch (IOException ioe) {
                log.error()
                        .exception(ioe)
                        .attr("ledgerId", ledgerId)
                        .attr("previousLAC", previousLAC)
                        .log("IOException while longpoll reading");
                return buildErrorResponse(StatusCode.EIO, startTimeSw);
            }

            registerSuccessfulEvent(requestProcessor.getRequestStats().getLongPollPreWaitStats(), startTimeSw);
            lastPhaseStartTime.reset().start();

            if (watched) {
                // successfully registered watcher to lac updates
                log.trace()
                        .attr("previousLAC", previousLAC)
                        .attr("timeout", () -> readRequest.getTimeOut())
                        .log("Waiting For LAC Update");
                synchronized (this) {
                    expirationTimerTask = requestTimer.newTimeout(timeout -> {
                            requestProcessor.getBookie().cancelWaitForLastAddConfirmedUpdate(ledgerId, this);
                            // When the timeout expires just get whatever is the current
                            // readLastConfirmed
                            LongPollReadEntryProcessorV3.this.scheduleDeferredRead(true);
                    }, readRequest.getTimeOut(), TimeUnit.MILLISECONDS);
                }
                return null;
            }
        }
        // request doesn't have timeout or fail to wait, proceed to read entry
        return getReadResponse();
    }

    private ReadResponse getClassicLongPollResponse() {
        if (shouldReadEntry() || !readRequest.hasTimeOut()) {
            return getReadResponse();
        }
        synchronized (this) {
            if (classicRetired) {
                finishClassicRetirement();
                return null;
            }
            classicRegistering = true;
        }
        Stopwatch start = Stopwatch.createStarted();
        boolean watched;
        try {
            watched = requestProcessor.getBookie().waitForLastAddConfirmedUpdate(ledgerId, previousLAC, this);
        } catch (Bookie.NoLedgerException absent) {
            synchronized (this) {
                classicRegistering = false;
            }
            return buildErrorResponse(StatusCode.ENOLEDGER, start);
        } catch (IOException unknown) {
            synchronized (this) {
                classicRegistering = false;
                classicRetired = true;
            }
            controlledCall.failUnknown(unknown);
            return null;
        }
        boolean retired;
        synchronized (this) {
            classicRegistering = false;
            classicWatching = watched;
            retired = classicRetired || requestProcessor.classic.isClosing();
            if (!retired && watched) {
                expirationTimerTask = requestTimer.newTimeout(timeout -> {
                    synchronized (LongPollReadEntryProcessorV3.this) {
                        if (classicRetired) {
                            return;
                        }
                    }
                    controlledCall.runCallback(() -> {
                        synchronized (LongPollReadEntryProcessorV3.this) {
                            if (classicRetired) {
                                return;
                            }
                            classicCancelAttempted = true;
                        }
                        try {
                            requestProcessor.getBookie().cancelWaitForLastAddConfirmedUpdate(ledgerId, this);
                        } catch (IOException unknown) {
                            throw new UncheckedIOException(unknown);
                        }
                        synchronized (LongPollReadEntryProcessorV3.this) {
                            classicWatching = false;
                        }
                        scheduleClassicDeferredRead(true);
                    });
                }, readRequest.getTimeOut(), TimeUnit.MILLISECONDS);
            }
        }
        registerSuccessfulEvent(requestProcessor.getRequestStats().getLongPollPreWaitStats(), start);
        lastPhaseStartTime.reset().start();
        if (retired) {
            retireForClassicChild();
            return null;
        }
        return watched ? null : getReadResponse();
    }

    void retireForClassicChild() {
        if (controlledCall == null) {
            throw new IllegalStateException("Only a controlled long poll has this retirement path");
        }
        boolean cancel;
        synchronized (this) {
            classicRetired = true;
            if (expirationTimerTask != null) {
                expirationTimerTask.cancel();
            }
            if (classicRegistering) {
                return; // Its original body cancels the returned watch before it can retire.
            }
            cancel = classicWatching && !classicCancelAttempted;
            if (cancel) {
                classicCancelAttempted = true;
            }
        }
        controlledCall.runCallback(() -> {
            if (cancel) {
                try {
                    requestProcessor.getBookie().cancelWaitForLastAddConfirmedUpdate(ledgerId, this);
                    synchronized (LongPollReadEntryProcessorV3.this) {
                        classicWatching = false;
                    }
                } catch (IOException unknown) {
                    controlledCall.failUnknown(unknown);
                    return;
                }
            }
            synchronized (LongPollReadEntryProcessorV3.this) {
                if (classicWatching) {
                    return; // The timeout's original cancel body still owns the watch.
                }
            }
            finishClassicRetirement();
        });
    }

    private void finishClassicRetirement() {
        finishReadRequest();
        controlledCall.complete();
    }

    @Override
    public void run() {
        if (controlledCall != null) {
            boolean retire;
            synchronized (this) {
                retire = classicRetired || requestProcessor.classic.isClosing();
            }
            if (retire) {
                retireForClassicChild();
                return;
            }
        }
        super.run();
    }

    @Override
    protected void sendResponse(ReadResponse response) {
        if (controlledCall != null) {
            synchronized (this) {
                classicRetired = true;
                if (expirationTimerTask != null) {
                    expirationTimerTask.cancel();
                }
            }
        }
        super.sendResponse(response);
    }

    private void scheduleClassicDeferredRead(boolean timeout) {
        synchronized (this) {
            if (classicRetired) {
                if (!classicWatching && !classicRegistering) {
                    finishClassicRetirement();
                }
                return;
            }
            if (classicDeferredAttempted) {
                return;
            }
            classicDeferredAttempted = true;
            shouldReadEntry = true;
            if (expirationTimerTask != null) {
                expirationTimerTask.cancel();
            }
        }
        var submitted = controlledCall.submitCallback(longPollThreadPool, this);
        synchronized (this) {
            deferredTask = submitted;
        }
        registerEvent(timeout, requestProcessor.getRequestStats().getLongPollWaitStats(), lastPhaseStartTime);
        lastPhaseStartTime.reset().start();
    }

    @Override
    protected void executeOp() {
        ReadResponse readResponse = getLongPollReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    @Override
    public void update(LastAddConfirmedUpdateNotification newLACNotification) {
        if (controlledCall != null) {
            controlledCall.runCallback(() -> {
                try {
                    synchronized (LongPollReadEntryProcessorV3.this) {
                        if (classicRetired) {
                            return;
                        }
                        if (newLACNotification.getLastAddConfirmed() <= previousLAC) {
                            return;
                        }
                        classicWatching = false; // Watchable detached this notification list before callbacks.
                        if (newLACNotification.getLastAddConfirmed() != Long.MAX_VALUE
                                && lastAddConfirmedUpdateTime.isEmpty()) {
                            lastAddConfirmedUpdateTime = Optional.of(newLACNotification.getTimestamp());
                        }
                    }
                    scheduleClassicDeferredRead(false);
                } finally {
                    newLACNotification.recycle();
                }
            });
            return;
        }
        if (newLACNotification.getLastAddConfirmed() > previousLAC) {
            if (newLACNotification.getLastAddConfirmed() != Long.MAX_VALUE && !lastAddConfirmedUpdateTime.isPresent()) {
                lastAddConfirmedUpdateTime = Optional.of(newLACNotification.getTimestamp());
            }
            log.trace().attr("lastAddConfirmed", () -> newLACNotification.getLastAddConfirmed())
                    .attr("request", request).log("Last Add Confirmed Advanced");
            scheduleDeferredRead(false);
        }
        newLACNotification.recycle();
    }

    private synchronized void scheduleDeferredRead(boolean timeout) {
        if (controlledCall != null) {
            scheduleClassicDeferredRead(timeout);
            return;
        }
        if (null == deferredTask) {
            log.trace()
                    .attr("expired", timeout)
                    .attr("request", request)
                    .log("Deferred Task");
            try {
                shouldReadEntry = true;
                deferredTask = longPollThreadPool.submit(this);
            } catch (RejectedExecutionException exc) {
                // If the threadPool has been shutdown, simply drop the task
            }
            if (null != expirationTimerTask) {
                expirationTimerTask.cancel();
            }

            registerEvent(timeout, requestProcessor.getRequestStats().getLongPollWaitStats(), lastPhaseStartTime);
            lastPhaseStartTime.reset().start();
        }
    }
}

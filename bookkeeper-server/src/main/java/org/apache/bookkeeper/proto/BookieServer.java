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

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;
import static org.apache.bookkeeper.conf.AbstractConfiguration.PERMITTED_STARTUP_USERS;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityProviderFactoryFactory;
import org.apache.zookeeper.KeeperException;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
@CustomLog
public class BookieServer {
    final ServerConfiguration conf;
    BookieNettyServer nettyServer;
    private volatile boolean running = false;
    private final Bookie bookie;
    DeathWatcher deathWatcher;
    UncleanShutdownDetection uncleanShutdownDetection;

    int exitCode = ExitCode.OK;

    // request processor
    private RequestProcessor requestProcessor;

    // Expose Stats
    private final StatsLogger statsLogger;
    private final boolean controlled;
    private final ByteBufAllocator controlledAllocator;
    private final Object controlledGate = new Object();
    private final Object controlledCloseLock = new Object();
    private boolean controlledListenerAttempted;
    private boolean controlledProcessorAttempted;
    private boolean controlledInitializing;
    private Thread controlledInitializer;
    private volatile boolean controlledClosing;

    // Exception handler
    private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

    public BookieServer(ServerConfiguration conf,
                        Bookie bookie,
                        StatsLogger statsLogger,
                        ByteBufAllocator allocator,
                        UncleanShutdownDetection uncleanShutdownDetection)
            throws IOException, KeeperException, InterruptedException,
            BookieException, UnavailableException, CompatibilityException, SecurityException {
        this.conf = conf;
        this.controlled = false;
        this.controlledAllocator = null;
        validateUser(conf);
        log.info()
                .attr("overrides", overriddenConfig(conf))
                .log("Starting Bookie server");

        this.statsLogger = statsLogger;
        this.bookie = bookie;
        this.nettyServer = new BookieNettyServer(this.conf, null, allocator);
        this.uncleanShutdownDetection = uncleanShutdownDetection;

        final SecurityHandlerFactory shFactory;

        shFactory = SecurityProviderFactoryFactory
                .getSecurityProviderFactory(conf.getTLSProviderFactoryClass());

        this.requestProcessor = new BookieRequestProcessor(conf, bookie,
                statsLogger.scope(SERVER_SCOPE), shFactory, allocator, nettyServer.allChannels);
        this.nettyServer.setRequestProcessor(this.requestProcessor);
    }

    private BookieServer(ServerConfiguration conf, Bookie bookie, StatsLogger stats, ByteBufAllocator allocator) {
        this.conf = conf;
        this.bookie = bookie;
        this.statsLogger = stats;
        this.controlledAllocator = allocator;
        this.controlled = true;
    }

    /** Heap-only controlled holder. The owning bridge retains it before any listener resource is acquired. */
    public static BookieServer prepareForClassicChild(ServerConfiguration conf, Bookie bookie,
            StatsLogger stats, ByteBufAllocator allocator) {
        return new BookieServer(conf, bookie, stats, allocator);
    }

    /** Listener stage only; no processor, activation, registration, READY or additional Bookie start. */
    public void initializeListenerForClassicChild()
            throws IOException, KeeperException, InterruptedException, BookieException {
        synchronized (controlledGate) {
            if (!controlled || controlledListenerAttempted || controlledClosing) {
                throw new IOException("Controlled listener initializes only once in its original server holder");
            }
            controlledListenerAttempted = true;
            controlledInitializing = true;
            controlledInitializer = Thread.currentThread();
        }
        try {
            validateUser(conf);
            nettyServer = BookieNettyServer.prepareForClassicChild(conf, controlledAllocator);
            if (controlledClosing) {
                throw new IOException("Controlled listener initialization was retired");
            }
            nettyServer.initializeForClassicChild();
            if (controlledClosing) {
                throw new IOException("Controlled listener bind returned after retirement");
            }
        } finally {
            synchronized (controlledGate) {
                controlledInitializing = false;
                controlledInitializer = null;
                controlledGate.notifyAll();
            }
        }
    }

    public void initializeProcessorForClassicChild() throws SecurityException, IOException {
        synchronized (controlledGate) {
            if (!controlled || controlledProcessorAttempted || controlledInitializing || controlledClosing
                    || !isListenerBoundForClassicChild()) {
                throw new IOException("Controlled processor requires its one actual bound listener");
            }
            controlledProcessorAttempted = true;
            controlledInitializing = true;
            controlledInitializer = Thread.currentThread();
        }
        try {
            var processor = BookieRequestProcessor.prepareForClassicChild(conf, bookie, statsLogger,
                    controlledAllocator, nettyServer.allChannels, this);
            requestProcessor = processor;
            nettyServer.setRequestProcessor(processor);
            processor.initializeForClassicChild();
            if (controlledClosing) {
                throw new IOException("Controlled processor initialization was retired");
            }
        } finally {
            synchronized (controlledGate) {
                controlledInitializing = false;
                controlledInitializer = null;
                controlledGate.notifyAll();
            }
        }
    }

    public void activateForClassicChild() throws IOException {
        synchronized (controlledGate) {
            if (!controlled || controlledClosing || controlledInitializing || !bookie.isRunning()
                    || !(requestProcessor instanceof BookieRequestProcessor processor)
                    || !processor.initializedForClassicChild() || !isListenerBoundForClassicChild()) {
                throw new IOException("Controlled activation lacks its actual Bookie, listener or processor");
            }
            nettyServer.activateForClassicChild(processor);
            running = true;
        }
    }

    public boolean isClosingForClassicChild() {
        return controlledClosing;
    }

    public void retireForClassicChild() throws InterruptedException {
        synchronized (controlledGate) {
            if (!controlled || isCurrentThreadForClassicChild()) {
                throw new IllegalStateException("Controlled server cannot retire from its own activity");
            }
            requestCloseForClassicChild();
            while (controlledInitializing) {
                controlledGate.wait();
            }
        }
        if (nettyServer != null) {
            nettyServer.submitRetirementForClassicChild();
        }
        if (requestProcessor instanceof BookieRequestProcessor processor && processor.classic != null) {
            processor.classic.retireAdmission();
        }
    }

    /** Called after the original Bookie Journal callbacks, before maintenance/final flush/file close. */
    public boolean awaitRequestsForClassicChild() throws InterruptedException {
        if (!controlled || isCurrentThreadForClassicChild()) {
            throw new IllegalStateException("Controlled request drain requires the external Bookie owner");
        }
        if (nettyServer != null && !nettyServer.awaitConnectionsForClassicChild()) {
            return false;
        }
        return !(requestProcessor instanceof BookieRequestProcessor processor)
                || processor.classic.awaitRequests() && processor.classic.awaitOutputs();
    }

    /** Only after actual storage drain; callbacks/output executors stay alive until that boundary. */
    public boolean finishNetworkForClassicChild() throws InterruptedException {
        if (!controlled || isCurrentThreadForClassicChild()) {
            throw new IllegalStateException("Controlled network cannot await itself");
        }
        synchronized (controlledCloseLock) {
            boolean complete = !(requestProcessor instanceof BookieRequestProcessor processor)
                    || processor.finishForClassicChild();
            complete = (nettyServer == null || nettyServer.closeAndDrainForClassicChild()) && complete;
            if (complete) {
                running = false;
            }
            return complete;
        }
    }

    public Throwable operationFailureForClassicChild() {
        return requestProcessor instanceof BookieRequestProcessor processor && processor.classic != null
                ? processor.classic.operationFailure() : null;
    }

    public Throwable closeFailureForClassicChild() {
        var processorFailure = requestProcessor instanceof BookieRequestProcessor processor && processor.classic != null
                ? processor.classic.closeFailure() : null;
        return processorFailure != null ? processorFailure : listenerCloseFailureForClassicChild();
    }

    public boolean isListenerBoundForClassicChild() {
        return controlled && !controlledClosing && nettyServer != null && nettyServer.isBoundForClassicChild();
    }

    public boolean isCurrentThreadForClassicChild() {
        synchronized (controlledGate) {
            return controlledInitializer == Thread.currentThread()
                    || nettyServer != null && nettyServer.isCurrentThreadForClassicChild()
                    || requestProcessor instanceof BookieRequestProcessor processor && processor.classic != null
                            && processor.classic.isCurrentActivity();
        }
    }

    public void requestCloseForClassicChild() {
        controlledClosing = true;
        if (requestProcessor instanceof BookieRequestProcessor processor && processor.classic != null) {
            processor.classic.requestClose();
        }
    }

    public boolean closeListenerAndAwaitForClassicChild() throws InterruptedException {
        synchronized (controlledGate) {
            if (!controlled || isCurrentThreadForClassicChild()) {
                throw new IllegalStateException("Controlled listener cannot wait for its own activity");
            }
            controlledClosing = true;
            while (controlledInitializing) {
                controlledGate.wait();
            }
        }
        synchronized (controlledCloseLock) {
            if (requestProcessor != null) {
                throw new IllegalStateException("Drain the complete owned processor before retiring its TCP loops");
            }
            try {
                return nettyServer == null || nettyServer.closeAndDrainForClassicChild();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
    }

    public Throwable listenerCloseFailureForClassicChild() {
        return nettyServer == null ? null : nettyServer.failureForClassicChild();
    }

    @VisibleForTesting
    public static BookieServer newBookieServer(ServerConfiguration conf,
                                               Bookie bookie,
                                               StatsLogger statsLogger,
                                               ByteBufAllocator allocator,
                                               UncleanShutdownDetection uncleanShutdownDetection)
            throws CompatibilityException, UnavailableException, SecurityException, IOException,
            InterruptedException, KeeperException, BookieException {
        return new BookieServer(conf, bookie, statsLogger, allocator, uncleanShutdownDetection);
    }

    /**
     * Currently the uncaught exception handler is used for DeathWatcher to notify
     * lifecycle management that a bookie is dead for some reasons.
     *
     * <p>in future, we can register this <tt>exceptionHandler</tt> to critical threads
     * so when those threads are dead, it will automatically trigger lifecycle management
     * to shutdown the process.
     */
    public void setExceptionHandler(UncaughtExceptionHandler exceptionHandler) {
        this.uncaughtExceptionHandler = exceptionHandler;
    }

    public void start() throws InterruptedException, IOException {
        if (controlled) {
            throw new IllegalStateException("Controlled service activation requires its complete bound child graph");
        }
        this.bookie.start();

        // fail fast, when bookie startup is not successful
        if (!this.bookie.isRunning()) {
            exitCode = bookie.getExitCode();
            this.requestProcessor.close();
            return;
        }

        this.uncleanShutdownDetection.registerStartUp();
        this.nettyServer.start();

        running = true;
        deathWatcher = new DeathWatcher(conf);
        if (null != uncaughtExceptionHandler) {
            deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        deathWatcher.start();
    }

    @VisibleForTesting
    public BookieSocketAddress getLocalAddress() throws UnknownHostException {
        return BookieImpl.getBookieAddress(conf);
    }

    @VisibleForTesting
    public BookieId getBookieId() throws UnknownHostException {
        return BookieImpl.getBookieId(conf);
    }

    @VisibleForTesting
    public Bookie getBookie() {
        return bookie;
    }

    @VisibleForTesting
    public BookieRequestProcessor getBookieRequestProcessor() {
        return (BookieRequestProcessor) requestProcessor;
    }

    /**
     * Suspend processing of requests in the bookie (for testing).
     */
    @VisibleForTesting
    public void suspendProcessing() {
        log.debug().attr("port", () -> conf.getBookiePort()).log("Suspending bookie server");
        nettyServer.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing).
     */
    @VisibleForTesting
    public void resumeProcessing() {
        log.debug().attr("port", () -> conf.getBookiePort()).log("Resuming bookie server");
        nettyServer.resumeProcessing();
    }

    public synchronized void shutdown() {
        if (controlled) {
            throw new IllegalStateException("Controlled server requires actual external owner drain");
        }
        log.info("Shutting down BookieServer");
        this.nettyServer.shutdown();
        if (!running) {
            return;
        }
        this.requestProcessor.close();
        exitCode = bookie.shutdown();
        uncleanShutdownDetection.registerCleanShutdown();
        running = false;
    }

    /**
     * Ensure the current user can start-up the process if it's restricted.
     */
    private void validateUser(ServerConfiguration conf) throws BookieException {
        if (conf.containsKey(PERMITTED_STARTUP_USERS)) {
            String currentUser = System.getProperty("user.name");
            String[] propertyValue = conf.getPermittedStartupUsers();
            for (String s : propertyValue) {
                if (s.equals(currentUser)) {
                    return;
                }
            }
            String errorMsg =
                    "System cannot start because current user isn't in permittedStartupUsers."
                            + " Current user: " + currentUser + " permittedStartupUsers: "
                            + Arrays.toString(propertyValue);
            log.error(errorMsg);
            throw new BookieException.BookieUnauthorizedAccessException(errorMsg);
        }
    }

    /**
     * Returns the configuration entries that differ from the {@link ServerConfiguration}
     * defaults — i.e. values explicitly set by the user (via config file or programmatically)
     * whose value is not the same as the built-in default.
     */
    private static Map<String, Object> overriddenConfig(ServerConfiguration conf) {
        ServerConfiguration defaults = new ServerConfiguration();
        Map<String, Object> overrides = new TreeMap<>();
        Iterator<String> keys = conf.getInMemoryConfiguration().getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = conf.getProperty(key);
            if (value == null) {
                continue;
            }
            Object defaultValue = defaults.getProperty(key);
            if (defaultValue == null || !value.toString().equals(defaultValue.toString())) {
                overrides.put(key, value);
            }
        }
        return overrides;
    }


    public boolean isRunning() {
        return bookie.isRunning() && nettyServer.isRunning() && running;
    }

    /**
     * Whether bookie is running?
     *
     * @return true if bookie is running, otherwise return false
     */
    public boolean isBookieRunning() {
        return bookie.isRunning();
    }

    public void join() throws InterruptedException {
        bookie.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie and nioserver are still alive.
     */
    private class DeathWatcher extends BookieCriticalThread {

        private final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            super("BookieDeathWatcher-" + conf.getBookiePort());
            watchInterval = conf.getDeathWatchInterval();
            // set a default uncaught exception handler to shutdown the bookie server
            // when it notices the bookie is not running any more.
            setUncaughtExceptionHandler((thread, cause) -> {
                log.info()
                        .exception(cause)
                        .attr("threadName", thread.getName())
                    .log("BookieDeathWatcher exited loop due to uncaught exception");
                shutdown();
            });
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                    Thread.currentThread().interrupt();
                }
                if (!isBookieRunning()) {
                    log.info("BookieDeathWatcher noticed the bookie is not running any more, exiting the watch loop!");
                    // death watcher has noticed that bookie is not running any more
                    // throw an exception to fail the death watcher thread and it will
                    // trigger the uncaught exception handler to handle this "bookie not running" situation.
                    throw new RuntimeException("Bookie is not running any more");
                }
            }
        }
    }


    /**
     * Legacy Method to run bookie server.
     */
    public static void main(String[] args) {
        Main.main(args);
    }

    @Override
    public  String toString() {
        String addr = "UNKNOWN";
        String id = "?";
        try {
            addr = BookieImpl.getBookieAddress(conf).toString();
            id = getBookieId().toString();
        } catch (UnknownHostException e) {
            //Ignored...
        }
        return "Bookie Server listening on " + addr + " with id " + id;
    }
}

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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileAuditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.AutoRecoveryService;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.zookeeper.KeeperException;

/**
 * Class to start/stop the AutoRecovery daemons Auditor and ReplicationWorker.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1332}
 */
@CustomLog
public class AutoRecoveryMain {

    /** Explicit experimental service cohort. No default configuration enables Profile. */
    public record ProfileSettings(String reservationRoot, String readinessRoot,
            java.util.List<org.apache.zookeeper.data.ACL> acls, io.netty.handler.ssl.SslContext tls,
            java.util.List<org.apache.bookkeeper.net.BookieId> cohort,
            org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Limits peerLimits,
            org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator.WorkerOptions work) {
        public ProfileSettings {
            org.apache.zookeeper.common.PathUtils.validatePath(reservationRoot);
            org.apache.zookeeper.common.PathUtils.validatePath(readinessRoot);
            java.util.Objects.requireNonNull(tls);
            java.util.Objects.requireNonNull(peerLimits);
            java.util.Objects.requireNonNull(work);
            if (reservationRoot.equals("/") || readinessRoot.equals("/") || reservationRoot.equals(readinessRoot)
                    || !tls.isClient() || acls.isEmpty() || cohort.isEmpty() || cohort.size() > 64
                    || new java.util.HashSet<>(cohort).size() != cohort.size()
                    || !work.data().isEmpty() || !work.controls().isEmpty()) {
                throw new IllegalArgumentException("Invalid explicit Profile service scope");
            }
            cohort = java.util.List.copyOf(cohort);
            acls = acls.stream().map(acl -> new org.apache.zookeeper.data.ACL(acl.getPerms(),
                    new org.apache.zookeeper.data.Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        }

        @Override
        public java.util.List<org.apache.zookeeper.data.ACL> acls() {
            return acls.stream().map(acl -> new org.apache.zookeeper.data.ACL(acl.getPerms(),
                    new org.apache.zookeeper.data.Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        }
    }

    private static final class ProfileResources {
        private final io.netty.channel.EventLoopGroup group = new io.netty.channel.nio.NioEventLoopGroup(1,
                new io.netty.util.concurrent.DefaultThreadFactory("profile-autorecovery"));
        private final java.util.concurrent.ThreadPoolExecutor cold = new java.util.concurrent.ThreadPoolExecutor(
                1, 1, 0, java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(64),
                new io.netty.util.concurrent.DefaultThreadFactory("profile-autorecovery-cold"));
        private ProfileAuditor profileAuditor;
        private org.apache.bookkeeper.meta.LedgerManager ownedManager;
        private org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool pool;
        private org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator coordinator;

        private BookKeeper client(ServerConfiguration conf, StatsLogger stats)
                throws IOException, InterruptedException {
            var clientConf = new org.apache.bookkeeper.conf.ClientConfiguration(conf);
            clientConf.setClientRole(org.apache.bookkeeper.conf.ClientConfiguration.CLIENT_ROLE_SYSTEM);
            clientConf.setUseV2WireProtocol(false);
            clientConf.setExplictLacInterval(0);
            try {
                return BookKeeper.forConfig(clientConf).statsLogger(stats).eventLoopGroup(group).build();
            } catch (BKException failed) {
                throw new IOException("Profile AutoRecovery SDK initialization failed", failed);
            }
        }

        private void bind(BookKeeper client, ProfileSettings settings) throws IOException {
            if (!(client.getMetadataClientDriver()
                    instanceof org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver driver)) {
                throw new IllegalArgumentException("Profile AutoRecovery requires its own ZK authority context");
            }
            ownedManager = client.getLedgerManagerFactory().newLedgerManager();
            if (!(ownedManager instanceof org.apache.bookkeeper.meta.AbstractZkLedgerManager manager)) {
                throw new IllegalArgumentException("Profile AutoRecovery requires a ZK ledger manager");
            }
            var zk = driver.getZk();
            var timeout = settings.peerLimits().timeout();
            var registry = new org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore(
                    zk, settings.readinessRoot(), settings.acls(), timeout);
            var reservations = new org.apache.bookkeeper.discover.profile.ZKProfileReservationStore(
                    zk, settings.reservationRoot(), settings.acls(), timeout);
            var metadata = new org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore(
                    zk, manager, settings.acls(), timeout);
            coordinator = new org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator(
                    reservations, metadata, cold);
            pool = new org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool(registry,
                    group, settings.tls(), cold, settings.cohort(), settings.peerLimits());
            profileAuditor = new ProfileAuditor(registry, reservations, metadata, settings.cohort(), settings.work(),
                    client.getClientCtx(), coordinator, pool);
        }

        private boolean drain() {
            boolean interrupted = false;
            if (pool != null) {
                var completed = pool.closeAsync();
                for (;;) {
                    try {
                        completed.get();
                        break;
                    } catch (InterruptedException stopping) {
                        interrupted = true;
                    } catch (java.util.concurrent.ExecutionException failed) {
                        throw new IllegalStateException(
                                "Profile service peer drain was not confirmed", failed.getCause());
                    }
                }
            }
            if (ownedManager != null) {
                try {
                    ownedManager.close();
                    ownedManager = null;
                } catch (IOException failedClose) {
                    throw new IllegalStateException("Profile metadata manager close was not confirmed", failedClose);
                }
            }
            return interrupted;
        }

        private boolean finishOwners() {
            boolean interrupted = false;
            cold.shutdown();
            for (;;) {
                try {
                    if (cold.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                        break;
                    }
                } catch (InterruptedException stopping) {
                    interrupted = true;
                }
            }
            group.shutdownGracefully(0, 5, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
            interrupted |= Thread.interrupted();
            return interrupted;
        }
    }

    private final ServerConfiguration conf;
    final BookKeeper bkc;
    final AuditorElector auditorElector;
    final ReplicationWorker replicationWorker;
    final AutoRecoveryDeathWatcher deathWatcher;
    private final ProfileResources profileResources;
    int exitCode;
    private volatile boolean shuttingDown = false;
    private boolean shutdownComplete;
    private volatile boolean running = false;

    // Exception handler
    private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

    public AutoRecoveryMain(ServerConfiguration conf) throws IOException,
            InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public AutoRecoveryMain(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this(conf, statsLogger, null);
    }

    /** Explicit isolated Profile service; existing constructors remain off. */
    public AutoRecoveryMain(ServerConfiguration conf, StatsLogger statsLogger, ProfileSettings settings)
            throws IOException, InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        if (settings != null && (conf.getAuditorPeriodicCheckInterval() != 0
                || conf.getAuditorPeriodicReplicasCheckInterval() < 0
                || conf.getAuditorPeriodicPlacementPolicyCheckInterval() != 0)) {
            throw new IllegalArgumentException(
                    "Profile service does not enable Classic payload/placement audits");
        }
        this.conf = conf;
        ProfileResources resources = settings == null ? null : new ProfileResources();
        BookKeeper client = null;
        AuditorElector elector = null;
        ReplicationWorker worker = null;
        try {
            client = resources == null ? Auditor.createBookKeeperClient(
                    conf, statsLogger.scope(BookKeeperClientStats.CLIENT_SCOPE))
                    : resources.client(conf, statsLogger.scope(BookKeeperClientStats.CLIENT_SCOPE));
            if (resources != null) {
                resources.bind(client, settings);
            }
            elector = new AuditorElector(BookieImpl.getBookieId(conf).toString(), conf,
                    client, statsLogger.scope(AUDITOR_SCOPE), false,
                    resources == null ? null : resources.profileAuditor);
            worker = resources == null ? new ReplicationWorker(conf, client, false,
                    statsLogger.scope(REPLICATION_WORKER_SCOPE))
                    : new ReplicationWorker(conf, client, statsLogger.scope(REPLICATION_WORKER_SCOPE),
                            resources.coordinator, settings.work(), resources.pool);
            bkc = client;
            auditorElector = elector;
            replicationWorker = worker;
            profileResources = resources;
            deathWatcher = new AutoRecoveryDeathWatcher(this);
            client.getMetadataClientDriver().setSessionStateListener(() -> {
                log.error("Client connection to the Metadata server expired, shutting down AutoRecoveryMain");
                CompletableFuture.runAsync(() -> shutdown(ExitCode.ZK_EXPIRED));
            });
        } catch (IOException | InterruptedException | UnavailableException
                | CompatibilityException | RuntimeException | Error failed) {
            boolean interrupted = Thread.interrupted();
            if (worker != null) {
                worker.shutdown();
                interrupted |= Thread.interrupted();
            }
            if (elector != null) {
                for (;;) {
                    try {
                        elector.shutdown();
                        break;
                    } catch (InterruptedException stopping) {
                        interrupted = true;
                        Thread.interrupted();
                    }
                }
            }
            if (resources != null) {
                interrupted |= resources.drain();
            }
            if (client != null) {
                try {
                    closeClientUninterruptibly(client);
                } catch (BKException closeFailure) {
                    failed.addSuppressed(closeFailure);
                } finally {
                    interrupted |= Thread.interrupted();
                }
            }
            if (resources != null) {
                interrupted |= resources.finishOwners();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            throw failed;
        }
    }

    /*
     * Start daemons
     */
    public synchronized void start() {
        if (shuttingDown) {
            throw new IllegalStateException("AutoRecovery has already been shut down");
        }
        auditorElector.start();
        replicationWorker.start();
        if (null != uncaughtExceptionHandler) {
            deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        deathWatcher.start();
        running = true;
    }

    /*
     * Waits till all daemons joins
     */
    public void join() throws InterruptedException {
        deathWatcher.join();
    }

    /*
     * Shutdown all daemons gracefully
     */
    public void shutdown() {
        shutdown(ExitCode.OK);
    }

    private void shutdown(int exitCode) {
        log.info().attr("exitCode", exitCode).log("Shutting down auto recovery");
        boolean interrupted = Thread.interrupted();
        boolean owner;
        synchronized (this) {
            owner = !shuttingDown;
            if (owner) {
                shuttingDown = true;
                running = false;
                this.exitCode = exitCode;
            }
        }
        try {
            if (!owner) {
                synchronized (this) {
                    while (!shutdownComplete) {
                        try {
                            wait();
                        } catch (InterruptedException stopping) {
                            interrupted = true;
                        }
                    }
                }
                return;
            }
            for (;;) {
                try {
                    auditorElector.shutdown();
                    break;
                } catch (InterruptedException stopping) {
                    interrupted = true;
                    Thread.interrupted();
                    log.warn().exception(stopping)
                            .log("Interrupted shutting down auditor elector; waiting for cleanup");
                }
            }
            replicationWorker.shutdown();
            interrupted |= Thread.interrupted(); // Worker preserved the signal; do not abort the SDK's close waits.
            if (profileResources != null) {
                interrupted |= profileResources.drain();
            }
            try {
                closeClientUninterruptibly(bkc);
            } catch (BKException failedClose) {
                log.warn().exception(failedClose).log("Failed to close bookkeeper client for auto recovery");
            } finally {
                interrupted |= Thread.interrupted();
            }
            if (profileResources != null) {
                interrupted |= profileResources.finishOwners();
            }
        } finally {
            if (owner) {
                synchronized (this) {
                    shutdownComplete = true;
                    notifyAll();
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // BookKeeper.close sets its admission flag before interruptible executor waits; retrying cannot resume it.
    // Give that one-shot procedure an independent owner so caller interrupts cannot strand its metadata session.
    static void closeClientUninterruptibly(BookKeeper client) throws BKException {
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Thread closer = new Thread(() -> {
            try {
                client.close();
            } catch (Throwable failedClose) {
                failure.set(failedClose);
            }
        }, "autorecovery-sdk-close");
        boolean interrupted = Thread.interrupted();
        try {
            closer.start();
            for (;;) {
                try {
                    closer.join();
                    break;
                } catch (InterruptedException stopping) {
                    interrupted = true;
                }
            }
            Throwable failedClose = failure.get();
            if (failedClose instanceof BKException bkFailure) {
                throw bkFailure;
            }
            if (failedClose instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            if (failedClose instanceof Error error) {
                throw error;
            }
            if (failedClose != null) {
                throw new IllegalStateException("AutoRecovery SDK close was not confirmed", failedClose);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isProfileEnabled() {
        return profileResources != null;
    }

    public org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool.Usage getProfilePeerUsage() {
        if (profileResources == null) {
            throw new IllegalStateException("Profile service is disabled");
        }
        return profileResources.pool.usage();
    }

    private int getExitCode() {
        return exitCode;
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

    @VisibleForTesting
    public Auditor getAuditor() {
        return auditorElector.getAuditor();
    }

    @VisibleForTesting
    public ReplicationWorker getReplicationWorker() {
        return replicationWorker;
    }

    /** Is auto-recovery service running? */
    public boolean isAutoRecoveryRunning() {
        return running;
    }

    /*
     * DeathWatcher for AutoRecovery daemons.
     */
    private class AutoRecoveryDeathWatcher extends BookieCriticalThread {
        private int watchInterval;
        private AutoRecoveryMain autoRecoveryMain;

        public AutoRecoveryDeathWatcher(AutoRecoveryMain autoRecoveryMain) {
            super("AutoRecoveryDeathWatcher-"
                    + autoRecoveryMain.conf.getBookiePort());
            this.autoRecoveryMain = autoRecoveryMain;
            watchInterval = autoRecoveryMain.conf.getDeathWatchInterval();
            // set a default uncaught exception handler to shutdown the AutoRecovery
            // when it notices the AutoRecovery is not running any more.
            setUncaughtExceptionHandler((thread, cause) -> {
                log.info()
                        .attr("thread", thread.getName())
                        .exception(cause)
                        .log("AutoRecoveryDeathWatcher exited loop due to uncaught exception");
                shutdown();
            });
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                // If any one service not running, then shutdown peer.
                if (!autoRecoveryMain.auditorElector.isRunning() || !autoRecoveryMain.replicationWorker.isRunning()) {
                    log.info(
                            "AutoRecoveryDeathWatcher noticed the AutoRecovery is not running any more,"
                            + "exiting the watch loop!");
                    /*
                     * death watcher has noticed that AutoRecovery is not
                     * running any more throw an exception to fail the death
                     * watcher thread and it will trigger the uncaught exception
                     * handler to handle this "AutoRecovery not running"
                     * situation.
                     */
                    throw new RuntimeException("AutoRecovery is not running any more");
                }
            }
        }
    }

    private static final Options opts = new Options();
    static {
        opts.addOption("c", "conf", true, "Bookie server configuration");
        opts.addOption("h", "help", false, "Print help message");
    }

    /*
     * Print usage
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("AutoRecoveryMain [options]\n", opts);
    }

    /*
     * load configurations from file.
     */
    private static void loadConfFile(ServerConfiguration conf, String confFile)
            throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException e) {
            log.error()
                    .attr("confFile", confFile)
                    .exception(e)
                    .log("Could not open configuration file");
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            log.error()
                    .attr("confFile", confFile)
                    .exception(e)
                    .log("Malformed configuration file");
            throw new IllegalArgumentException();
        }
        log.info().attr("confFile", confFile).log("Using configuration file");
    }

    /*
     * Parse console args
     */
    private static ServerConfiguration parseArgs(String[] args)
            throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(opts, args);

            if (cmdLine.hasOption('h')) {
                throw new IllegalArgumentException();
            }

            ServerConfiguration conf = new ServerConfiguration();
            String[] leftArgs = cmdLine.getArgs();

            if (cmdLine.hasOption('c')) {
                if (null != leftArgs && leftArgs.length > 0) {
                    throw new IllegalArgumentException("unexpected arguments [" + String.join(" ", leftArgs) + "]");
                }
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
            }

            if (null != leftArgs && leftArgs.length > 0) {
                throw new IllegalArgumentException("unexpected arguments [" + String.join(" ", leftArgs) + "]");
            }
            return conf;
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {

        ServerConfiguration conf;

        // 0. parse command line
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            log.error().exception(iae).log("Error parsing command line arguments");
            if (iae.getMessage() != null) {
                System.err.println(iae.getMessage());
            }
            printUsage();
            return ExitCode.INVALID_CONF;
        }

        // 1. building the component stack:
        LifecycleComponent server;
        try {
            server = buildAutoRecoveryServer(new BookieConfiguration(conf));
        } catch (Exception e) {
            log.error().exception(e).log("Failed to build AutoRecovery Server");
            System.err.println(e.getMessage());
            return ExitCode.SERVER_EXCEPTION;
        }

        // 2. start the server
        try {
            ComponentStarter.startComponent(server).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // the server is interrupted
            log.info("AutoRecovery server is interrupted. Exiting ...");
            System.err.println(ie.getMessage());
        } catch (ExecutionException ee) {
            log.error().exception(ee.getCause()).log("Error in bookie shutdown");
            System.err.println(ee.getMessage());
            return ExitCode.SERVER_EXCEPTION;
        }
        return ExitCode.OK;
    }

    public static LifecycleComponentStack buildAutoRecoveryServer(BookieConfiguration conf) throws Exception {
        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack.newBuilder()
                .withName("autorecovery-server");

        // 1. build stats provider
        StatsProviderService statsProviderService = new StatsProviderService(conf);
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");

        serverBuilder.addComponent(statsProviderService);
        log.info().attr("component", StatsProviderService.class.getName()).log("Load lifecycle component");

        // 2. build AutoRecovery server
        AutoRecoveryService autoRecoveryService = new AutoRecoveryService(conf, rootStatsLogger);

        serverBuilder.addComponent(autoRecoveryService);
        log.info().attr("component", AutoRecoveryService.class.getName()).log("Load lifecycle component");

        // 4. build http service
        if (conf.getServerConf().isHttpServerEnabled()) {
            BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                    .setAutoRecovery(autoRecoveryService.getAutoRecoveryServer())
                    .setServerConfiguration(conf.getServerConf())
                    .setStatsProvider(statsProviderService.getStatsProvider()).build();
            HttpService httpService = new HttpService(provider, conf, rootStatsLogger);

            serverBuilder.addComponent(httpService);
            log.info().attr("component", HttpService.class.getName()).log("Load lifecycle component");
        }

        return serverBuilder.build();
    }
}

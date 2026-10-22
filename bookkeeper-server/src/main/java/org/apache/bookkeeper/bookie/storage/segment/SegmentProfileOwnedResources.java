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
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProfileStaticOperationAuthorizer;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/** Actual resources for one isolated service episode. Only its Settings are a public bootstrap input. */
public final class SegmentProfileOwnedResources {
    public record Settings(StartupReadinessInput policy, String requirementsRoot, String readinessRoot,
            String reservationRoot, List<ACL> acls, String domain, int maxEvents,
            Duration timeout, Duration pollInterval, int networkWorkers, int coldWorkers, int coldQueueSlots) {
        public Settings {
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(domain, "domain");
            for (String path : List.of(requirementsRoot, readinessRoot, reservationRoot)) {
                PathUtils.validatePath(path);
                if (path.equals("/")) {
                    throw new IllegalArgumentException("Explicit Profile namespaces required");
                }
            }
            if (java.util.Set.of(requirementsRoot, readinessRoot, reservationRoot).size() != 3
                    || acls.isEmpty() || domain.isEmpty() || maxEvents < 1 || maxEvents > 4096
                    || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0
                    || pollInterval.compareTo(Duration.ofMillis(10)) < 0
                    || pollInterval.compareTo(Duration.ofMinutes(1)) > 0
                    || networkWorkers < 1 || networkWorkers > 64 || coldWorkers < 1 || coldWorkers > 64
                    || coldQueueSlots < 1 || coldQueueSlots > 4096) {
                throw new IllegalArgumentException("Finite isolated resource settings required");
            }
            acls = copyAcls(acls);
        }

        @Override
        public List<ACL> acls() {
            return copyAcls(acls);
        }
    }

    record Environment(SegmentProfileService.Startup startup, SegmentProfileService.Transport transport) { }

    private final ServerConfiguration configuration;
    private final Settings settings;
    private final TLSContextFactory tls;
    private final ProfileReceiveBudget budget;
    private final SegmentProfileListener.Settings listenerSettings;
    private final InetSocketAddress address;
    private final ProfileStaticOperationAuthorizer staticAuthorizer;
    private final ProfileControlAuthorizer exactAuthorizer;
    private final SegmentProfileReadConfig read;
    private final ProfileDescriptorValidator validator;
    private EventLoopGroup acceptor;
    private EventLoopGroup worker;
    private ThreadPoolExecutor cold;
    private ScheduledThreadPoolExecutor scheduler;
    private ZooKeeper zk;
    private ZKMetadataClientDriver driver;
    private AbstractZkLedgerManager manager;
    private LedgerManagerFactory factory;
    private boolean managerClosed;
    private boolean driverClosed;
    private boolean zkClosed;
    private final Set<Thread> drainThreads = ConcurrentHashMap.newKeySet();
    private volatile Thread zkEventThread;

    SegmentProfileOwnedResources(ServerConfiguration configuration, Settings settings, TLSContextFactory tls,
            ProfileReceiveBudget budget, int connectionBytes, int connectionBaseBytes, String trustDomain,
            InetSocketAddress address, ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlAuthorizer exactAuthorizer, SegmentProfileReadConfig read,
            ProfileDescriptorValidator validator) {
        this.configuration = configuration;
        this.settings = settings;
        this.tls = Objects.requireNonNull(tls, "tls");
        this.budget = Objects.requireNonNull(budget, "budget");
        listenerSettings = new SegmentProfileListener.Settings(connectionBytes, connectionBaseBytes, trustDomain,
                settings.policy().targetReadinessGeneration().value());
        this.address = Objects.requireNonNull(address, "address");
        if (address.isUnresolved() || address.getAddress().isAnyLocalAddress()) {
            throw new IllegalArgumentException("Explicit resolved Profile address required");
        }
        this.staticAuthorizer = Objects.requireNonNull(staticAuthorizer, "staticAuthorizer");
        this.exactAuthorizer = Objects.requireNonNull(exactAuthorizer, "exactAuthorizer");
        this.read = read;
        this.validator = validator;
    }

    private static List<ACL> copyAcls(List<ACL> acls) {
        return acls.stream().map(acl -> new ACL(acl.getPerms(),
                new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
    }

    private ThreadFactory drainThreadFactory(String name) {
        var factory = new DefaultThreadFactory(name);
        return task -> factory.newThread(() -> {
            Thread thread = Thread.currentThread();
            drainThreads.add(thread);
            try {
                task.run();
            } finally {
                drainThreads.remove(thread);
            }
        });
    }

    @SuppressWarnings("deprecation")
    Environment open() throws IOException, InterruptedException {
        try {
            var uri = URI.create(configuration.getMetadataServiceUri());
            String connect = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
            if (uri.getPath() == null || uri.getPath().equals("/") || configuration.getZkTimeout() < 1) {
                throw new IllegalArgumentException("Explicit ZK ledger namespace/session timeout required");
            }
            acceptor = new NioEventLoopGroup(1, new DefaultThreadFactory("profile-acceptor"));
            worker = new NioEventLoopGroup(settings.networkWorkers(), new DefaultThreadFactory("profile-worker"));
            cold = new ThreadPoolExecutor(settings.coldWorkers(), settings.coldWorkers(), 0, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(settings.coldQueueSlots()), drainThreadFactory("profile-cold"));
            scheduler = new ScheduledThreadPoolExecutor(1, drainThreadFactory("profile-delete"));
            scheduler.setRemoveOnCancelPolicy(true);
            scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            var watcher = new ZooKeeperWatcherBase(configuration.getZkTimeout(), false);
            zk = new ZooKeeper(connect, configuration.getZkTimeout(), event -> {
                zkEventThread = Thread.currentThread();
                watcher.process(event);
            });
            watcher.waitForConnection();
            var client = new ClientConfiguration(configuration);
            client.setZkLedgersRootPath(uri.getPath());
            driver = new ZKMetadataClientDriver();
            driver.initialize(client, scheduler, NullStatsLogger.INSTANCE, Optional.of(zk));
            factory = driver.getLedgerManagerFactory();
            var created = factory.newLedgerManager();
            if (!(created instanceof AbstractZkLedgerManager)) {
                created.close();
                throw new IOException("Owned Profile bootstrap requires the original ZK ledger manager");
            }
            manager = (AbstractZkLedgerManager) created;
            var source = new ZKProfileReservationStore(zk, settings.reservationRoot(), settings.acls(),
                    settings.timeout());
            var metadata = new ProfileLedgerMetadataStore(zk, manager, settings.acls(), settings.timeout());
            var policy = settings.policy();
            var bookie = policy.compatibility().bookieId();
            var install = source.installAuthorityReader(bookie, metadata);
            var activate = source.activationAuthorityReader(bookie, manager);
            var replacement = source.replacementAuthorityReader(bookie, metadata);
            var repair = source.repairAuthorityReader(bookie, metadata);
            var delete = source.deleteAuthorityReader(bookie, policy.compatibility().storageIncarnation().value(),
                    metadata);
            ProfileControlStore authorities = key -> switch (key.purpose()) {
                case PREPARING_INSTALL -> {
                    var initial = install.read(key);
                    yield initial.status() == ProfileControlStore.ReadStatus.NOT_FOUND ? repair.read(key) : initial;
                }
                case POST_MEMBERSHIP_REPLACEMENT -> replacement.read(key);
                case TOMBSTONE_OR_DELETE -> delete.read(key);
                case RECOVERY_GRANT, RECOVERY_GRANT_CLOSE, RECOVERY_TERMINAL_CLOSE -> repair.read(key);
                default -> activate.read(key);
            };
            var startup = new SegmentProfileService.Startup(
                    new ZKProfileStartupRequirementsStore(zk, settings.requirementsRoot(), settings.acls(),
                            settings.timeout()),
                    policy, new ZKProfileRegistrationStore(zk, settings.readinessRoot(), settings.acls(),
                            settings.timeout()),
                    new ZKProfileEphemeralRegistrationStore(zk, settings.readinessRoot(), bookie,
                            settings.acls(), settings.timeout()),
                    source, metadata, watcher, settings.domain(), scheduler, cold, settings.maxEvents(),
                    settings.timeout(), settings.pollInterval());
            var control = new SegmentProfileControlConfig(validator, staticAuthorizer, authorities, exactAuthorizer,
                    cold, settings.timeout());
            return new Environment(startup, new SegmentProfileService.Transport(tls, budget,
                    listenerSettings, address, acceptor, worker, control, read));
        } catch (org.apache.commons.configuration2.ex.ConfigurationException
                | org.apache.bookkeeper.meta.exceptions.MetadataException
                | org.apache.zookeeper.KeeperException failure) {
            throw new IOException("Owned Profile metadata initialization failed", failure);
        }
    }

    void retire() {
        budget.closeAdmissionAndDrain();
    }

    void requireExternalThread() {
        if (drainThreads.contains(Thread.currentThread()) || Thread.currentThread() == zkEventThread) {
            throw new IllegalStateException("Owned Profile resources cannot drain their current executing thread");
        }
        if (manager != null) {
            manager.requireExternalDrainCaller();
        }
        for (var group : new EventLoopGroup[] {acceptor, worker}) {
            if (group != null) {
                for (var loop : group) {
                    if (loop.inEventLoop()) {
                        throw new IllegalStateException("Owned Profile resources require an external drain caller");
                    }
                }
            }
        }
    }

    /** Called only after service/local I/O drain, including partial initialization failure. */
    boolean close(long deadline) throws IOException, InterruptedException {
        requireExternalThread();
        if (!budget.closeAdmissionAndDrain().isDone()) {
            return false;
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (cold != null) {
            cold.shutdown();
            if (!cold.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                return false;
            }
        }
        if (scheduler != null && !scheduler.awaitTermination(Math.max(0, deadline - System.nanoTime()),
                TimeUnit.NANOSECONDS)) {
            return false;
        }
        if (manager != null && !managerClosed) {
            long left = deadline - System.nanoTime();
            if (left <= 0 || !manager.closeAndAwaitTermination(Duration.ofNanos(left))) {
                return false;
            }
            managerClosed = true;
        }
        if (driver != null && !driverClosed) {
            // Confirm the original factory before the driver's legacy logging-only close path.
            if (factory != null) {
                factory.close();
            }
            driver.close();
            driverClosed = true;
        }
        if (zk != null && !zkClosed) {
            long millis = TimeUnit.NANOSECONDS.toMillis(Math.max(0, deadline - System.nanoTime()));
            if (millis == 0 || !zk.close((int) Math.min(Integer.MAX_VALUE, millis))) {
                return false;
            }
            zkClosed = true;
        }
        for (var group : new EventLoopGroup[] {worker, acceptor}) {
            if (group != null) {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
                if (!group.terminationFuture().await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                    return false;
                }
                if (!group.terminationFuture().isSuccess()) {
                    throw new IOException("Owned Profile event-loop termination failed",
                            group.terminationFuture().cause());
                }
            }
        }
        return true;
    }
}

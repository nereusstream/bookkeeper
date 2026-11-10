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

import io.netty.buffer.UnpooledByteBufAllocator;
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
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
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
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
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
            acls = new ResourceSettings(requirementsRoot, readinessRoot, reservationRoot, acls, domain, maxEvents,
                    timeout, pollInterval, networkWorkers, coldWorkers, coldQueueSlots).acls;
        }

        @Override
        public List<ACL> acls() {
            return copyAcls(acls);
        }

        private ResourceSettings resources() {
            return new ResourceSettings(requirementsRoot, readinessRoot, reservationRoot, acls, domain, maxEvents,
                    timeout, pollInterval, networkWorkers, coldWorkers, coldQueueSlots);
        }
    }

    record ResourceSettings(String requirementsRoot, String readinessRoot, String reservationRoot,
            List<ACL> acls, String domain, int maxEvents, Duration timeout, Duration pollInterval,
            int networkWorkers, int coldWorkers, int coldQueueSlots) {
        ResourceSettings {
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

    record ManifestEnvironment(SegmentManifestOwnedService.Startup startup,
            SegmentProfileService.Transport transport) { }

    private record Connected(ZKProfileStartupRequirementsStore requirements, ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, ZKProfileReservationStore source,
            ProfileLedgerMetadataStore metadata, ZooKeeperWatcherBase watcher) { }

    private final ServerConfiguration configuration;
    private final Settings settings;
    private final ResourceSettings resources;
    private TLSContextFactory tls;
    private final boolean ownsTls;
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
        this(configuration, settings, settings.resources(), Objects.requireNonNull(tls, "tls"), false, budget,
                legacyListenerSettings(settings, connectionBytes, connectionBaseBytes, trustDomain),
                address, staticAuthorizer, exactAuthorizer, read, validator);
    }

    SegmentProfileOwnedResources(ServerConfiguration configuration, Settings settings,
            ProfileReceiveBudget budget, int connectionBytes, int connectionBaseBytes, String trustDomain,
            InetSocketAddress address, ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlAuthorizer exactAuthorizer, SegmentProfileReadConfig read,
            ProfileDescriptorValidator validator) {
        this(configuration, settings, settings.resources(), null, true, budget,
                legacyListenerSettings(settings, connectionBytes, connectionBaseBytes, trustDomain),
                address, staticAuthorizer, exactAuthorizer, read, validator);
    }

    SegmentProfileOwnedResources(ServerConfiguration configuration, ResourceSettings resources,
            ProfileReceiveBudget budget, SegmentProfileListener.Settings listenerSettings,
            InetSocketAddress address, ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlAuthorizer exactAuthorizer, SegmentProfileReadConfig read,
            ProfileDescriptorValidator validator) {
        this(new ServerConfiguration(configuration), null, resources, null, true, budget, listenerSettings,
                address, staticAuthorizer, exactAuthorizer, read, validator);
        if (!address.getAddress().isLoopbackAddress() || read == null) {
            throw new IllegalArgumentException("Owned manifest resources require loopback transport and reads");
        }
    }

    private SegmentProfileOwnedResources(ServerConfiguration configuration, Settings settings,
            ResourceSettings resources, TLSContextFactory tls, boolean ownsTls, ProfileReceiveBudget budget,
            SegmentProfileListener.Settings listenerSettings, InetSocketAddress address,
            ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlAuthorizer exactAuthorizer, SegmentProfileReadConfig read,
            ProfileDescriptorValidator validator) {
        this.configuration = configuration;
        this.settings = settings;
        this.resources = Objects.requireNonNull(resources, "resources");
        this.tls = tls;
        this.ownsTls = ownsTls;
        this.budget = Objects.requireNonNull(budget, "budget");
        this.listenerSettings = Objects.requireNonNull(listenerSettings, "listenerSettings");
        this.address = Objects.requireNonNull(address, "address");
        if (address.isUnresolved() || address.getAddress().isAnyLocalAddress()) {
            throw new IllegalArgumentException("Explicit resolved Profile address required");
        }
        this.staticAuthorizer = Objects.requireNonNull(staticAuthorizer, "staticAuthorizer");
        this.exactAuthorizer = Objects.requireNonNull(exactAuthorizer, "exactAuthorizer");
        this.read = read;
        this.validator = validator;
    }

    private static SegmentProfileListener.Settings legacyListenerSettings(Settings settings, int connectionBytes,
            int connectionBaseBytes, String trustDomain) {
        return new SegmentProfileListener.Settings(connectionBytes, connectionBaseBytes, trustDomain,
                settings.policy().targetReadinessGeneration().value());
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

    Environment open() throws IOException, InterruptedException {
        var policy = settings.policy();
        var compatibility = policy.compatibility();
        var connected = connect(compatibility.bookieId(), false);
        var startup = new SegmentProfileService.Startup(connected.requirements(), policy, connected.persistent(),
                connected.publisher(), connected.source(), connected.metadata(), connected.watcher(),
                resources.domain(), scheduler, cold, resources.maxEvents(), resources.timeout(),
                resources.pollInterval());
        return new Environment(startup,
                transport(connected, compatibility.bookieId(), compatibility.storageIncarnation()));
    }

    ManifestEnvironment openManifest(SegmentManifestStartupPolicy policy, IsolatedSegmentConfig runtime,
            SegmentManifestAccessObservation access, Generation capabilities, List<Integer> hints)
            throws IOException, InterruptedException {
        if (settings != null) {
            throw new IllegalStateException("Manifest requires its own resource settings and owned TLS");
        }
        var manifest = policy.manifest();
        String bookie = manifest.bookieId().toString();
        var connected = connect(bookie, true);
        connected.requirements().bindDeleteAssignments(connected.source());
        var source = connected.requirements().readManifest(bookie).observation()
                .orElseThrow(() -> new IOException("Owned manifest startup requirements unavailable"));
        var preflight = policy.preflight(runtime, connected.requirements(), source, connected.source());
        var startup = new SegmentManifestOwnedService.Startup(preflight, access, connected.metadata(),
                connected.persistent(), connected.publisher(), connected.watcher(), resources.domain(), scheduler,
                cold, resources.maxEvents(), resources.timeout(), resources.pollInterval(), capabilities, hints);
        return new ManifestEnvironment(startup, transport(connected, bookie, manifest.incarnation()));
    }

    @SuppressWarnings("deprecation")
    private Connected connect(String bookie, boolean existingLayoutOnly) throws IOException, InterruptedException {
        try {
            var uri = URI.create(configuration.getMetadataServiceUri());
            String connect = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
            if (uri.getPath() == null || uri.getPath().equals("/") || configuration.getZkTimeout() < 1) {
                throw new IllegalArgumentException("Explicit ZK ledger namespace/session timeout required");
            }
            if (ownsTls) {
                tls = TLSContextFactory.forProfileServer();
                tls.init(SecurityHandlerFactory.NodeType.Server, configuration, UnpooledByteBufAllocator.DEFAULT);
            }
            acceptor = new NioEventLoopGroup(1, new DefaultThreadFactory("profile-acceptor"));
            worker = new NioEventLoopGroup(resources.networkWorkers(), new DefaultThreadFactory("profile-worker"));
            cold = new ThreadPoolExecutor(resources.coldWorkers(), resources.coldWorkers(), 0, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(resources.coldQueueSlots()), drainThreadFactory("profile-cold"));
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
            factory = existingLayoutOnly ? driver.getExistingLedgerManagerFactory()
                    : driver.getLedgerManagerFactory();
            var created = factory.newLedgerManager();
            if (!(created instanceof AbstractZkLedgerManager)) {
                created.close();
                throw new IOException("Owned Profile bootstrap requires the original ZK ledger manager");
            }
            manager = (AbstractZkLedgerManager) created;
            var source = new ZKProfileReservationStore(zk, resources.reservationRoot(), resources.acls(),
                    resources.timeout());
            var metadata = new ProfileLedgerMetadataStore(zk, manager, resources.acls(), resources.timeout());
            return new Connected(
                    new ZKProfileStartupRequirementsStore(zk, resources.requirementsRoot(), resources.acls(),
                            resources.timeout()),
                    new ZKProfileRegistrationStore(zk, resources.readinessRoot(), resources.acls(),
                            resources.timeout()),
                    new ZKProfileEphemeralRegistrationStore(zk, resources.readinessRoot(), bookie,
                            resources.acls(), resources.timeout()),
                    source, metadata, watcher);
        } catch (org.apache.bookkeeper.tls.SecurityException failure) {
            throw new IOException("Owned Profile TLS initialization failed", failure);
        } catch (org.apache.commons.configuration2.ex.ConfigurationException
                | org.apache.bookkeeper.meta.exceptions.MetadataException
                | org.apache.zookeeper.KeeperException failure) {
            throw new IOException("Owned Profile metadata initialization failed", failure);
        }
    }

    private SegmentProfileService.Transport transport(Connected connected, String bookie,
            StorageIncarnation incarnation) {
        var source = connected.source();
        var metadata = connected.metadata();
        var install = source.installAuthorityReader(bookie, metadata);
        var activate = source.activationAuthorityReader(bookie, manager);
        var replacement = source.replacementAuthorityReader(bookie, metadata);
        var repair = source.repairAuthorityReader(bookie, metadata);
        var delete = source.deleteAuthorityReader(bookie, incarnation.value(), metadata);
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
        var control = new SegmentProfileControlConfig(validator, staticAuthorizer, authorities, exactAuthorizer,
                cold, resources.timeout());
        return new SegmentProfileService.Transport(tls, budget, listenerSettings, address, acceptor, worker,
                control, read);
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
        if (ownsTls && tls != null) {
            tls.closeProfileServer();
        }
        return true;
    }
}

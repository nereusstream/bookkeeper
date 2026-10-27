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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleListener;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

/**
 * Explicit disposable Profile service episode; owns graph, receipt, admission and listener.
 * Metadata client, TLS, event loops, bounded cold executor and scheduler remain borrowed until actual drain.
 * The supplied new-scope policy still requires its external physical/old-service proof issuer.
 */
public final class SegmentProfileService implements AutoCloseable {
    public static final String CONFIG_PREFIX = "experimentalSegment";
    private static final Set<String> CONFIG_FIELDS = Set.of("Mode", "Directory", "LogicalShards", "DataFiles",
            "BytesPerFile", "MaxPayloadBytes", "MaxPendingEntries", "Backend", "Affinity", "NumaPolicy", "WaitStrategy",
            "Workers", "IoThreads", "ControlSlots", "Buffers", "BufferBytes", "InputBudgetBytes", "BufferBudgetBytes",
            "StartupTimeoutMillis", "ControlLogBytes", "HotEntries", "IndexedEntries", "PoolBlocks",
            "MaintenanceBlocks",
            "BatchBytes", "BatchEntries", "BatchDelayMillis");

    public record Startup(ZKProfileStartupRequirementsStore requirements, StartupReadinessInput requested,
            ZKProfileRegistrationStore persistent, ZKProfileEphemeralRegistrationStore publisher,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ZooKeeperWatcherBase watcher, String domain, ScheduledExecutorService scheduler,
            ThreadPoolExecutor cold, int maxEvents, Duration timeout, Duration pollInterval) {
        public Startup {
            Objects.requireNonNull(requirements, "requirements");
            Objects.requireNonNull(requested, "requested");
            Objects.requireNonNull(persistent, "persistent");
            Objects.requireNonNull(publisher, "publisher");
            Objects.requireNonNull(reservations, "reservations");
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(watcher, "watcher");
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(scheduler, "scheduler");
            Objects.requireNonNull(cold, "cold");
            if (domain.isEmpty() || maxEvents < 1 || maxEvents > 4096
                    || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0
                    || pollInterval.compareTo(Duration.ofMillis(10)) < 0
                    || pollInterval.compareTo(Duration.ofMinutes(1)) > 0
                    || cold.getMaximumPoolSize() == Integer.MAX_VALUE
                    || (long) cold.getQueue().size() + cold.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                    || !(cold.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
                throw new IllegalArgumentException("Bounded startup/catch-up settings required");
            }
        }
    }

    public record Transport(TLSContextFactory tls, ProfileReceiveBudget budget,
            int connectionBytes, int connectionBaseBytes, String trustDomain, long readinessGeneration,
            InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
            SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
        public Transport {
            Objects.requireNonNull(tls, "tls");
            Objects.requireNonNull(budget, "budget");
            new SegmentProfileListener.Settings(connectionBytes, connectionBaseBytes, trustDomain, readinessGeneration);
            Objects.requireNonNull(address, "address");
            Objects.requireNonNull(acceptor, "acceptor");
            Objects.requireNonNull(worker, "worker");
            if (address.isUnresolved() || address.getAddress().isAnyLocalAddress()) {
                throw new IllegalArgumentException("Explicit resolved Profile endpoint required");
            }
        }

        Transport(TLSContextFactory tls, ProfileReceiveBudget budget, SegmentProfileListener.Settings settings,
                InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
                SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
            this(tls, budget, settings.connectionBytes(), settings.connectionBaseBytes(), settings.trustDomain(),
                    settings.readinessGeneration(), address, acceptor, worker, control, read);
        }

        SegmentProfileListener.Settings settings() {
            return new SegmentProfileListener.Settings(connectionBytes, connectionBaseBytes, trustDomain,
                    readinessGeneration);
        }
    }

    /** Strict opt-in, including unknown-key rejection; safe before any Classic or Profile resource construction. */
    public static boolean isolatedConfigurationEnabled(ServerConfiguration configuration) {
        var keys = Objects.requireNonNull(configuration, "configuration").getKeys();
        while (keys.hasNext()) {
            String name = keys.next();
            if (!name.startsWith(CONFIG_PREFIX)) {
                continue;
            }
            if (!CONFIG_FIELDS.contains(name.substring(CONFIG_PREFIX.length()))) {
                throw new IllegalArgumentException("Unsupported experimental Segment configuration key: " + name);
            }
            Object value = configuration.getProperty(name);
            if (!(value instanceof CharSequence || value instanceof Number || value instanceof Boolean)) {
                throw new IllegalArgumentException("Experimental Segment settings must be single scalar values: "
                        + name);
            }
        }
        return switch (configuration.getString(CONFIG_PREFIX + "Mode", "OFF")) {
            case "OFF" -> false;
            case "ISOLATED" -> true;
            default -> throw new IllegalArgumentException("Experimental Segment mode must be OFF or ISOLATED");
        };
    }

    /**
     * Freeze explicit local bounds from ordinary Bookie configuration, borrowing the supplied authority/transport.
     * No metadata, directory or storage is opened here; configuration text cannot create physical startup proof.
     */
    public static SegmentProfileService fromConfiguration(ServerConfiguration configuration, SecretKey key,
            ProfileDescriptorValidator validator, Startup startup, Transport transport) {
        if (!isolatedConfigurationEnabled(configuration)) {
            throw new IllegalArgumentException("Experimental Segment configuration is OFF");
        }
        Objects.requireNonNull(startup, "startup");
        var parameters = localParameters(configuration, startup.requested());
        return new SegmentProfileService(parameters.config(), parameters.identity(), key, validator,
                parameters.execution(), parameters.limits(), startup, transport);
    }

    /**
     * Bind offline backup/import to trusted historical source identity, scopes and local bounds.
     * This creates no startup, metadata, listener or execution resources. Historical policy and images never
     * grant current restore authority; import still requires current metadata and its frozen RepairIntent.
     */
    public static SegmentProfileBackup offlineBackup(ServerConfiguration configuration, SecretKey key,
            ProfileDescriptorValidator validator, StartupReadinessInput sourcePolicy,
            String clusterScope, String deviceScope) {
        var parameters = localParameters(configuration, sourcePolicy);
        return new SegmentProfileBackup(parameters.config(), parameters.identity(), key, validator,
                parameters.limits().controlLogBytes(), parameters.limits().indexedEntries(),
                clusterScope, deviceScope);
    }

    private record LocalParameters(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentExecutionGroup.Settings execution, IsolatedSegmentStorage.Limits limits) { }

    private static LocalParameters localParameters(ServerConfiguration configuration, StartupReadinessInput policy) {
        if (!isolatedConfigurationEnabled(configuration)) {
            throw new IllegalArgumentException("Experimental Segment configuration is OFF");
        }
        String directory = configuration.getString(CONFIG_PREFIX + "Directory");
        if (directory == null || !Path.of(directory).isAbsolute()) {
            throw new IllegalArgumentException("Explicit absolute experimental Segment directory required");
        }
        var defaults = IsolatedSegmentConfig.defaults(Path.of(directory));
        var config = new IsolatedSegmentConfig(defaults.directory(), 1,
                optionalInt(configuration, "LogicalShards", defaults.logicalShards()),
                optionalInt(configuration, "DataFiles", defaults.dataFiles()),
                optionalLong(configuration, "BytesPerFile", defaults.bytesPerFile()), 4096,
                optionalInt(configuration, "MaxPayloadBytes", defaults.maxPayloadBytes()),
                optionalInt(configuration, "MaxPendingEntries", defaults.maxPendingEntries()),
                configuration.getString(CONFIG_PREFIX + "Backend", defaults.backend()),
                configuration.getString(CONFIG_PREFIX + "Affinity", defaults.affinity()),
                configuration.getString(CONFIG_PREFIX + "NumaPolicy", defaults.numaPolicy()),
                configuration.getString(CONFIG_PREFIX + "WaitStrategy", defaults.waitStrategy()));
        var execution = new SegmentExecutionGroup.Settings(requiredInt(configuration, "Workers"),
                requiredInt(configuration, "IoThreads"), requiredInt(configuration, "ControlSlots"),
                requiredInt(configuration, "Buffers"), requiredInt(configuration, "BufferBytes"),
                requiredLong(configuration, "InputBudgetBytes"), requiredLong(configuration, "BufferBudgetBytes"),
                Duration.ofMillis(requiredLong(configuration, "StartupTimeoutMillis")));
        var batches = new SegmentDataPhysicalDriver.BatchLimits(requiredInt(configuration, "BatchBytes"),
                requiredInt(configuration, "BatchEntries"),
                Duration.ofMillis(requiredLong(configuration, "BatchDelayMillis")));
        var limits = new IsolatedSegmentStorage.Limits(requiredLong(configuration, "ControlLogBytes"),
                requiredInt(configuration, "HotEntries"), requiredInt(configuration, "IndexedEntries"),
                requiredInt(configuration, "PoolBlocks"), requiredInt(configuration, "MaintenanceBlocks"), batches);
        long blocks = (config.bytesPerFile() - 4096) / SegmentArenaControlLog.BLOCK_BYTES;
        int largestEntry = SegmentBatchFormat.maxPayload(config) + SegmentBatchFormat.ENTRY_OVERHEAD;
        if (execution.workers() > config.logicalShards() || execution.workers() > config.maxPendingEntries()
                || limits.hotEntries() < config.logicalShards()
                || (long) limits.poolBlocks() + limits.maintenanceBlocks() > blocks
                || batches.bytes() > execution.bufferBytes()
                || SegmentBatchFormat.alignedLength(SegmentBatchFormat.RECORD_HEADER + largestEntry)
                    > execution.bufferBytes() || largestEntry > execution.inputBudgetBytes()) {
            throw new IllegalArgumentException("Incompatible experimental Segment layout/execution/batch bounds");
        }
        Objects.requireNonNull(policy, "policy");
        if (policy.requiredDevices().devices().size() != 1) {
            throw new IllegalArgumentException("One declared Arena required for isolated bootstrap");
        }
        var identity = new SegmentFileHeader(policy.compatibility().storageIncarnation(),
                UUID.fromString(policy.requiredDevices().devices().get(0).deviceId()),
                BookieId.parse(policy.compatibility().bookieId()));
        return new LocalParameters(config, identity, execution, limits);
    }

    /** Deferred owned initialization; caller retains this handle when startup fails. TLS stays borrowed. */
    public static SegmentProfileService withOwnedResources(ServerConfiguration configuration, SecretKey key,
            ProfileDescriptorValidator validator, SegmentProfileOwnedResources.Settings settings,
            TLSContextFactory tls, ProfileReceiveBudget budget, int connectionBytes, int connectionBaseBytes,
            String trustDomain, InetSocketAddress address,
            org.apache.bookkeeper.common.profile.control.ProfileStaticOperationAuthorizer staticAuthorizer,
            org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer exactAuthorizer,
            SegmentProfileReadConfig read) {
        var parameters = localParameters(configuration, settings.policy());
        var resources = new SegmentProfileOwnedResources(new ServerConfiguration(configuration), settings, tls,
                budget, connectionBytes, connectionBaseBytes, trustDomain, address, staticAuthorizer, exactAuthorizer,
                read, validator);
        return new SegmentProfileService(parameters, key, validator, resources);
    }

    /**
     * Own the TLS context loaded from the frozen configuration as part of the deferred service episode.
     * Release its factory reference only after the listener and owned execution resources actually terminate.
     */
    public static SegmentProfileService withOwnedResources(ServerConfiguration configuration, SecretKey key,
            ProfileDescriptorValidator validator, SegmentProfileOwnedResources.Settings settings,
            ProfileReceiveBudget budget, int connectionBytes, int connectionBaseBytes,
            String trustDomain, InetSocketAddress address,
            org.apache.bookkeeper.common.profile.control.ProfileStaticOperationAuthorizer staticAuthorizer,
            org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer exactAuthorizer,
            SegmentProfileReadConfig read) {
        var parameters = localParameters(configuration, settings.policy());
        var resources = new SegmentProfileOwnedResources(new ServerConfiguration(configuration), settings,
                budget, connectionBytes, connectionBaseBytes, trustDomain, address, staticAuthorizer, exactAuthorizer,
                read, validator);
        return new SegmentProfileService(parameters, key, validator, resources);
    }

    private static long requiredLong(ServerConfiguration configuration, String field) {
        if (!configuration.containsKey(CONFIG_PREFIX + field)) {
            throw new IllegalArgumentException("Missing experimental Segment setting: " + field);
        }
        return Long.parseLong(configuration.getString(CONFIG_PREFIX + field));
    }

    private static int requiredInt(ServerConfiguration configuration, String field) {
        return Math.toIntExact(requiredLong(configuration, field));
    }

    private static long optionalLong(ServerConfiguration configuration, String field, long fallback) {
        return configuration.containsKey(CONFIG_PREFIX + field) ? requiredLong(configuration, field) : fallback;
    }

    private static int optionalInt(ServerConfiguration configuration, String field, int fallback) {
        return Math.toIntExact(optionalLong(configuration, field, fallback));
    }

    private final Object gate = new Object();
    private final Object closeGate = new Object();
    private ProfileReceiveBudget.DataWorkingSet dataWorkingSet;
    private ProfileReceiveBudget.ControlWorkingSet controlWorkingSet;
    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final SegmentExecutionGroup.Settings execution;
    private final IsolatedSegmentStorage.Limits limits;
    private Startup startup;
    private Transport transport;
    private SegmentProfileOwnedResources ownedResources;
    private IsolatedSegmentStorage storage;
    private SegmentLocalReadinessStore local;
    private ZKProfileEphemeralRegistrationStore.Admission admission;
    private SegmentProfileListener listener;
    private boolean attempted;
    private boolean startupFinished;
    private boolean retired;
    private boolean closed;
    private boolean demoted;
    private boolean lifecycleBound;

    private SegmentProfileService(LocalParameters parameters, SecretKey key, ProfileDescriptorValidator validator,
            SegmentProfileOwnedResources resources) {
        config = parameters.config();
        identity = parameters.identity();
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        execution = parameters.execution();
        limits = parameters.limits();
        ownedResources = resources;
    }

    /** No directory, storage constructor, listener or registration is opened here. */
    SegmentProfileService(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings execution,
            IsolatedSegmentStorage.Limits limits, Startup startup, Transport transport) {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.execution = Objects.requireNonNull(execution, "execution");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.startup = Objects.requireNonNull(startup, "startup");
        this.transport = Objects.requireNonNull(transport, "transport");
        if (transport.settings().readinessGeneration() != startup.requested().targetReadinessGeneration().value()
                || transport.control() != null && transport.control().executor() != startup.cold()) {
            throw new IllegalArgumentException("Listener must use this readiness generation and cold executor");
        }
    }

    /** Blocking cold caller only. Failed starts retain this owner if real cleanup cannot yet finish. */
    InetSocketAddress start(boolean existing) throws IOException, InterruptedException {
        requireExternalThread();
        synchronized (gate) {
            if (attempted || retired) {
                throw new IllegalStateException("Profile service episode already attempted or retired");
            }
            attempted = true;
        }
        Throwable failed = null;
        try {
            if (ownedResources != null) {
                var environment = ownedResources.open();
                startup = environment.startup();
                transport = environment.transport();
                requireLive();
            }
            var source = startup.requirements().read(identity.bookieId().toString());
            if (source.status() != ZKProfileStartupRequirementsStore.Status.FOUND) {
                throw new IOException("Startup requirements unavailable: " + source.status());
            }
            var observed = source.observation().orElseThrow();
            var policy = IsolatedSegmentStorage.startupPolicy(observed, startup.requested(), config, identity);
            requireStorageRoots(policy.newScopeFallback());
            startup.publisher().bindStartupRequirements(startup.requirements(), observed);
            requireLive();
            synchronized (gate) {
                requireLive();
                try {
                    dataWorkingSet = transport.budget().reserveDataWorkingSet(SegmentControlCapacity.sum(
                            execution.reservedBufferBytes(), SegmentDerivedIndex.CONFIGURED_CAPACITY_BYTES,
                            SegmentHotDirectory.occupiedCapacity(config.logicalShards(), limits.indexedEntries()),
                            SegmentHotDirectory.metadataCapacity(config, execution, limits.hotEntries()),
                            SegmentDataRecovery.inventoryCapacity(config, limits.indexedEntries()),
                            SegmentDataPhysicalDriver.parsingCapacity(config, execution),
                            SegmentPointRead.runtimeCapacity(config, execution),
                            SegmentExecutionGroup.metadataCapacity(config, execution),
                            SegmentEntryInputs.metadataCapacity(execution.inputBudgetBytes()),
                            SegmentDataPhysicalDriver.metadataCapacity(config, execution)));
                } catch (ProfileReceiveBudget.CapacityExceededException exhausted) {
                    throw new IOException("Profile DATA quota cannot reserve storage buffers, index"
                            + " and hot/inventory/parser/read/execution/input/driver capacities", exhausted);
                }
                try {
                    controlWorkingSet = transport.budget().reserveControlWorkingSet(
                            SegmentControlCapacity.required(config, limits.controlLogBytes()));
                } catch (ProfileReceiveBudget.CapacityExceededException exhausted) {
                    throw new IOException("Profile CONTROL quota cannot reserve storage control state and workspaces",
                            exhausted);
                }
            }
            storage = new IsolatedSegmentStorage(config, identity, key, validator, execution, limits,
                    transport.budget());
            startup.publisher().bindDeleteDelivery(startup.reservations(), identity.incarnation(),
                    storage::profileDeleteProgress);
            synchronized (gate) {
                requireLive();
                admission = startup.publisher().watchAdmission(startup.watcher());
                storage.bindProfileRegistration(admission);
            }
            startup.publisher().requireStartupStores(admission, startup.persistent(), startup.reservations(),
                    identity.incarnation());
            requireLive();
            if (existing) {
                storage.restart();
            } else {
                storage.startNew();
            }
            requireLive();
            local = new SegmentLocalReadinessStore(config.directory(), identity.bookieId().toString(),
                    identity.incarnation());
            listener = new SegmentProfileListener(storage, transport.tls(), transport.budget(), transport.settings(),
                    transport.address(), transport.acceptor(), transport.worker(),
                    transport.control(), transport.read());
            InetSocketAddress bound = listener.bound().get(startup.timeout().toNanos(), TimeUnit.NANOSECONDS);
            // The published address comes from the actual native bind, including an ephemeral port.
            String host = bound.getAddress().getHostAddress();
            String endpoint = "tls://" + (host.contains(":") ? "[" + host + "]" : host) + ":" + bound.getPort();
            var boundPolicy = new StartupReadinessInput(policy.compatibility(), policy.requiredDevices(),
                    policy.recoveryFacts(), policy.newScopeFallback(), policy.rollbackSafetyFacts(),
                    policy.targetReadinessGeneration(), policy.capabilityGeneration(), policy.runtimeReaderVersion(),
                    policy.runtimeWriterVersion(), policy.supportedMandatoryFeatures(), endpoint,
                    policy.capabilityHints());
            requireLive();
            var result = storage.reconcileReadiness(boundPolicy, local, startup.persistent(), startup.publisher(),
                    startup.reservations(), startup.metadata(), startup.domain(), startup.scheduler(), startup.cold(),
                    startup.maxEvents(), startup.timeout(), startup.pollInterval(), this::isLive);
            result.completion().get(startup.timeout().toNanos(), TimeUnit.NANOSECONDS);
            if (result.readiness() == null || result.readiness().outcome() != StartupReadinessResult.Outcome.WRITABLE) {
                throw new IOException("Profile service not writable: " + result.deleteStatus()
                        + "/" + result.readiness());
            }
            synchronized (gate) {
                requireLive();
                if (!admission.getAsBoolean()) {
                    throw new IOException("Profile service admission retired during startup");
                }
                return bound;
            }
        } catch (ExecutionException | TimeoutException unavailable) {
            failed = new IOException("Profile service startup did not complete", unavailable);
            throw (IOException) failed;
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            failed = failure;
            throw failure;
        } finally {
            synchronized (gate) {
                startupFinished = true;
            }
            if (failed != null) {
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failed.addSuppressed(new IOException("Profile service cleanup incomplete; retain this owner"));
                    }
                } catch (IOException | InterruptedException | RuntimeException cleanup) {
                    failed.addSuppressed(cleanup);
                }
                if (failed instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean isLive() {
        synchronized (gate) {
            return !retired;
        }
    }

    /** Bind the external root proof to the only two filesystem roots opened by this isolated graph. */
    private void requireStorageRoots(NewScopeFallbackManifest fallback) throws IOException {
        var arena = canonicalPlannedPath(config.directory());
        var index = canonicalPlannedPath(SegmentDerivedIndex.directory(config));
        for (var root : fallback.newRoots()) {
            var expected = switch (root.kind()) {
                case ARENA -> arena;
                case INDEX -> index;
                default -> null; // The isolated graph has no Classic journal or ledger storage.
            };
            if (expected != null && !expected.equals(canonicalPlannedPath(Path.of(root.opaquePath())))) {
                throw new IOException("New-scope root proof belongs to another " + root.kind());
            }
        }
        for (var root : fallback.oldRoots()) {
            var old = canonicalPlannedPath(Path.of(root.opaquePath()));
            if (arena.startsWith(old) || old.startsWith(arena) || index.startsWith(old) || old.startsWith(index)) {
                throw new IOException("New storage overlaps an old-scope root");
            }
        }
    }

    private static Path canonicalPlannedPath(Path path) throws IOException {
        if (!path.isAbsolute()) {
            throw new IOException("Native new-scope proof requires absolute roots");
        }
        path = path.normalize();
        Path existing = path;
        while (!Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            existing = existing.getParent();
        }
        return existing.toRealPath().resolve(existing.relativize(path));
    }

    /** One explicit experimental EmbeddedServer component; no storage is opened while assembling the stack. */
    public LifecycleComponent lifecycle(boolean existing) {
        synchronized (gate) {
            if (lifecycleBound || attempted || retired) {
                throw new IllegalStateException("Profile service lifecycle must be bound once before startup");
            }
            lifecycleBound = true;
            return new ServiceLifecycle(existing);
        }
    }

    private final class ServiceLifecycle implements LifecycleComponent {
        private final boolean existing;
        private final Set<LifecycleListener> listeners = new CopyOnWriteArraySet<>();
        private volatile Lifecycle.State state = Lifecycle.State.INITIALIZED;
        private volatile Thread.UncaughtExceptionHandler handler;

        private ServiceLifecycle(boolean existing) {
            this.existing = existing;
        }

        @Override
        public String getName() {
            return "experimental-segment-profile";
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return state;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
            listeners.add(Objects.requireNonNull(listener, "listener"));
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void setExceptionHandler(Thread.UncaughtExceptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public void start() {
            synchronized (gate) {
                if (state == Lifecycle.State.STARTED) {
                    return;
                }
                if (state != Lifecycle.State.INITIALIZED || attempted || retired) {
                    throw new IllegalStateException("Use a fresh Profile episode to restart storage");
                }
            }
            listeners.forEach(LifecycleListener::beforeStart);
            try {
                SegmentProfileService.this.start(existing);
                synchronized (gate) {
                    requireLive();
                    state = Lifecycle.State.STARTED;
                }
            } catch (IOException | InterruptedException | RuntimeException | Error failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                var notify = handler;
                if (notify != null) {
                    try {
                        notify.uncaughtException(Thread.currentThread(), failure);
                    } catch (RuntimeException callbackFailure) {
                        failure.addSuppressed(callbackFailure);
                    }
                }
                throw new IllegalStateException("Profile service failed to start", failure);
            }
            listeners.forEach(LifecycleListener::afterStart);
        }

        @Override
        public void stop() {
            if (state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED) {
                return;
            }
            listeners.forEach(LifecycleListener::beforeStop);
            // Do not mark terminal or let the stack release borrowed resources before actual drain.
            drain();
            synchronized (gate) {
                if (state != Lifecycle.State.CLOSED) {
                    state = Lifecycle.State.STOPPED;
                }
            }
            listeners.forEach(LifecycleListener::afterStop);
        }

        private void drain() {
            try {
                SegmentProfileService.this.close();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }

        @Override
        public void close() {
            if (state == Lifecycle.State.CLOSED) {
                return;
            }
            stop();
            listeners.forEach(LifecycleListener::beforeClose);
            drain();
            synchronized (gate) {
                state = Lifecycle.State.CLOSED;
            }
            listeners.forEach(LifecycleListener::afterClose);
        }
    }

    private void requireLive() throws IOException {
        if (!isLive()) {
            throw new IOException("Profile service episode retired");
        }
    }

    private void requireExternalThread() {
        if (ownedResources != null) {
            ownedResources.requireExternalThread();
            return;
        }
        if (transport == null) {
            return;
        }
        for (var loop : transport.worker()) {
            if (loop.inEventLoop()) {
                throw new IllegalStateException("Profile service lifecycle requires an external cold caller");
            }
        }
        for (var loop : transport.acceptor()) {
            if (loop.inEventLoop()) {
                throw new IllegalStateException("Profile service lifecycle requires an external cold caller");
            }
        }
    }

    /** Local drain deadline; metadata demotion also has its own store timeout. False retains all live owners. */
    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Positive service drain timeout required");
        }
        requireExternalThread();
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (gate) {
            retired = true;
            if (admission != null) {
                admission.close();
            }
            if (transport != null) {
                transport.budget().closeAdmissionAndDrain();
            } else if (ownedResources != null) {
                ownedResources.retire();
            }
            if (attempted && !startupFinished) {
                return false;
            }
        }
        synchronized (closeGate) {
            if (closed) {
                return true;
            }
            IOException demotionFailure = null;
            if (!demoted && admission != null) {
                try {
                    startup.publisher().demote(identity.bookieId().toString());
                    demoted = true;
                } catch (RuntimeException unconfirmed) {
                    demotionFailure = new IOException("Profile registration demotion unconfirmed", unconfirmed);
                }
            }
            if (listener != null) {
                try {
                    listener.closeAsync().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (TimeoutException pending) {
                    if (demotionFailure != null) {
                        throw demotionFailure;
                    }
                    return false;
                } catch (ExecutionException failure) {
                    throw new IOException("Profile listener drain failed", failure.getCause());
                }
            }
            long remaining = deadline - System.nanoTime();
            if (storage != null && (remaining <= 0 || !storage.close(Duration.ofNanos(remaining)))) {
                if (demotionFailure != null) {
                    throw demotionFailure;
                }
                return false;
            }
            if (local != null) {
                local.close();
            }
            var dataCapacity = dataWorkingSet;
            dataWorkingSet = null;
            if (dataCapacity != null) {
                dataCapacity.close();
            }
            // Returning the final credit may synchronously reenter close through a caller's drain callback.
            var controlCapacity = controlWorkingSet;
            controlWorkingSet = null;
            if (controlCapacity != null) {
                controlCapacity.close();
            }
            if (demotionFailure != null) {
                throw demotionFailure;
            }
            if (ownedResources != null && !ownedResources.close(deadline)) {
                return false;
            }
            closed = true;
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Profile service drain incomplete; retain this owner and retry");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Profile service drain interrupted", interrupted);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.LocalRecoveryFacts;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.RollbackSafetyFacts;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstructionSources;
import org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery;
import org.apache.bookkeeper.discover.profile.ProfileDeleteRecord;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.profile.ProfileInboundPipeline;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.util.DiskChecker;

/** Local Profile-only graph. The manifest fixes routing independently of execution placement. */
final class ManifestSegmentStorage implements AutoCloseable, SegmentProfileAddHandler.DataOperations {
    record Capacity(long dataBytes, long controlBytes) {
        Capacity withAdditionalData(long bytes) {
            return new Capacity(SegmentControlCapacity.sum(dataBytes, bytes), controlBytes);
        }

        Capacity withAdditionalControl(long bytes) {
            return new Capacity(dataBytes, SegmentControlCapacity.sum(controlBytes, bytes));
        }

        Capacity withRecoveryDependencies(int arenas, int maximumRows) throws IOException {
            var layout = SegmentControlCapacity.Layout.hotSpot();
            // A recovery grant can project both its original and terminal scope. Allow for copied
            // identifier arrays and strings, plus temporary and immutable row containers.
            long scope = SegmentControlCapacity.sum(layout.object(ProfileControlScope.class),
                    layout.object(ProfileDescriptorIdentity.class), layout.bytes(36),
                    SegmentControlCapacity.times(8, layout.bytes(32)),
                    layout.string(ProfileControlScope.MAX_TARGET_BOOKIE_BYTES));
            long row = SegmentControlCapacity.sum(SegmentControlCapacity.times(2, scope), layout.string(64),
                    SegmentControlCapacity.times(8, layout.captured(16)));
            long projection = SegmentControlCapacity.sum(SegmentControlCapacity.times(maximumRows, row),
                    SegmentControlCapacity.times(arenas + 1L, 16 * layout.captured(16)),
                    4L * SegmentArenaManifest.MAX_BYTES);
            // Each vector can retain references from its original replay while the other is assembled.
            return new Capacity(dataBytes,
                    SegmentControlCapacity.times(2, SegmentControlCapacity.sum(controlBytes, projection)));
        }
    }

    record ArenaRecoveryDependencies(SegmentFileHeader identity,
            SegmentArenaControlLog.RecoveryDependencies control) { }

    record LedgerRoute(int shard, int arena) { }

    record RecoveryDependencies(ManifestIdentity manifest, SegmentFileHeader bookieIdentity,
            SegmentBookieControlLog.RecoveryDependencies bookie, List<ArenaRecoveryDependencies> arenas,
            Map<ProfileLedgerContext, LedgerRoute> ledgerRoutes) { }

    record ClosedArenaData(SegmentFileHeader identity, FileIdentity rootObject,
            List<ProfileClosedReconstruction.FileImage> files,
            Map<ProfileLedgerContext, List<SegmentDataRecovery.ClosedImageEntry>> entries) { }

    record ClosedDataObservation(SegmentClosedReconstruction.ManifestClosedObservation closed,
            List<ClosedArenaData> arenas) { }

    record ClosedSourceObservation(ClosedDataObservation data, SegmentClosedReconstruction.PublisherExit publisher,
            List<ProfileClosedReconstruction.Member> sources) { }

    static long closedDataCapacity(SegmentArenaManifest manifest, int maximumRows, int maximumEntries)
            throws IOException {
        var layout = SegmentControlCapacity.Layout.hotSpot();
        long files = manifest.arenas().stream().mapToLong(SegmentArenaManifest.Arena::dataFiles).sum();
        long entry = SegmentControlCapacity.sum(layout.object(SegmentDataRecovery.ClosedImageEntry.class),
                SegmentControlCapacity.times(2, layout.string(64)), 4 * 8);
        long file = SegmentControlCapacity.sum(layout.object(ProfileClosedReconstruction.FileImage.class),
                layout.object(ProfileClosedReconstruction.Image.class), layout.object(FileIdentity.class),
                layout.string(64), layout.hashEntry(), 8 * 8);
        // The base manifest reservation includes page and batch parser workspaces. One CLOSED scan at a time
        // adds grant/selector maps, boxed positions, stream state and retired/reused extent trees.
        long scanRow = SegmentControlCapacity.sum(6 * layout.hashEntry(), 4 * layout.treeEntry(),
                8 * layout.captured(16));
        long arena = SegmentControlCapacity.sum(layout.object(ClosedArenaData.class), layout.object(FileIdentity.class),
                8 * layout.hashMap(), 4 * layout.list());
        return SegmentControlCapacity.sum(layout.object(ClosedDataObservation.class),
                layout.object(ClosedSourceObservation.class),
                layout.object(SegmentClosedReconstruction.ManifestClosedObservation.class),
                layout.object(SegmentDataRecovery.ClosedImageAudit.class),
                SegmentControlCapacity.times(manifest.arenas().size(), arena),
                SegmentControlCapacity.times(ProfileClosedReconstruction.MAX_MEMBERS, 12 * layout.captured(16)),
                SegmentControlCapacity.times(files, file), SegmentControlCapacity.times(maximumRows, scanRow),
                SegmentControlCapacity.times((long) maximumEntries + SegmentBatchFormat.MAX_RECORDS, entry));
    }

    @FunctionalInterface
    interface RecoveryDependencyObserver<T> {
        T observe(RecoveryDependencies dependencies) throws Exception;
    }

    enum RecoveryFactsStatus { COMPLETE, SOURCE_UNVERIFIED, GRAPH_UNAVAILABLE, DELETE_INCOMPLETE, DEADLINE_EXPIRED }

    record ArenaCut(UUID arena, long controlThrough) { }

    /** Native format capability, independent of which checkpoint, if any, was used. */
    record NativeFormats(int identityPage, int controlFrame, List<Integer> readableCheckpointImages,
            int checkpointWriter) { }

    record RecoveredFacts(ZKProfileStartupRequirementsStore.SourceIdentity source,
            BookieCompatibilityFact commissioned, BookieId bookie, StorageIncarnation incarnation,
            ManifestIdentity manifest, NativeFormats formats, long bookieControlThrough, List<ArenaCut> arenas,
            LocalRecoveryFacts recovery, RollbackSafetyFacts rollback, ProfileDeleteDelivery.Head requiredDelete,
            ProfileControlRequest.DeleteBinding appliedDelete) { }

    record RecoveryFactsAttempt(RecoveryFactsStatus status, RecoveredFacts facts,
            IsolatedSegmentStorage.DeleteCatchUpAttempt delete) {
        CompletableFuture<Void> completion() {
            return delete == null ? CompletableFuture.completedFuture(null) : delete.completion();
        }
    }

    /** The original graph and its current cold facts, with only a checked external declaration. */
    static final class CheckedStartup {
        private final ManifestSegmentStorage owner;
        private final SegmentManifestStartupPolicy policy;
        private final RequiredDeviceManifest requiredDevices;
        private final RecoveredFacts facts;

        private CheckedStartup(ManifestSegmentStorage owner, SegmentManifestStartupPolicy policy,
                RequiredDeviceManifest requiredDevices, RecoveredFacts facts) {
            this.owner = owner;
            this.policy = policy;
            this.requiredDevices = requiredDevices;
            this.facts = facts;
        }

        SegmentManifestStartupPolicy policy() {
            return policy;
        }

        RequiredDeviceManifest requiredDevices() {
            return requiredDevices;
        }

        RecoveredFacts facts() {
            return facts;
        }
    }

    record StartupPreflightAttempt(RecoveryFactsAttempt recovery, CheckedStartup checked) { }

    private record RestartPlan(long bookieCut, List<SegmentArenaControlLog.ManifestRestart> arenas) { }

    record PreflightEpisode(SegmentManifestStartupPolicy.PreOpen preflight,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore.RetainedG1DeleteReplay retained,
            long deadline) { }

    static final class ColdPublicationOwner {
        final ManifestSegmentStorage storage;
        final CheckedStartup checked;
        final PreflightEpisode episode;
        final Path directory;
        final OwnedListener pending;

        private ColdPublicationOwner(ManifestSegmentStorage storage, CheckedStartup checked, OwnedListener pending) {
            this.storage = storage;
            this.checked = checked;
            episode = storage.startupPreflight;
            directory = storage.control.directory();
            this.pending = pending;
        }
    }

    static final class OwnedListener {
        final SegmentManifestOwnedService service;
        final CheckedStartup checked;
        final ZKProfileEphemeralRegistrationStore.Admission admission;
        final SegmentProfileListener listener;

        private OwnedListener(SegmentManifestOwnedService service, CheckedStartup checked,
                ZKProfileEphemeralRegistrationStore.Admission admission,
                SegmentProfileListener listener) {
            this.service = service;
            this.checked = checked;
            this.admission = admission;
            this.listener = listener;
        }
    }

    private record LocalCuts(long bookie, List<ArenaCut> arenas, RollbackSafetyFacts rollback,
            ProfileControlRequest.DeleteBinding applied) { }

    private static final Generation INITIAL_GENERATION = new Generation(1);
    private static final NativeFormats NATIVE_FORMATS = new NativeFormats(2, 1, List.of(1, 2), 2);

    private static final class Member {
        final IsolatedSegmentConfig config;
        final SegmentFileHeader identity;
        final SegmentOwnedFiles owned;
        SegmentDataFileLock dataGuard;
        SegmentDataFileLock controlGuard;
        SegmentArenaControlLog arena;
        IsolatedSegmentFiles files;
        SegmentDerivedIndex index;
        SegmentArenaControlDriver allocator;

        Member(IsolatedSegmentConfig config, SegmentFileHeader identity, List<Path> locks) {
            this.config = config;
            this.identity = identity;
            owned = new SegmentOwnedFiles(locks);
        }
    }

    private final Object gate = new Object();
    private final Object lifecycle = new Object();
    private final SegmentArenaManifest manifest;
    private final IsolatedSegmentConfig control;
    private final SegmentFileHeader bookieIdentity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final SegmentExecutionGroup.Settings settings;
    private final IsolatedSegmentStorage.Limits limits;
    private final ProfileReceiveBudget budget;
    private final List<Path> roots;
    private final List<Path> lockPaths;
    private final SegmentOwnedFiles bookieFiles;
    private final List<Member> members;
    private final int maxEntryBytes;
    private final SegmentDataPhysicalDriver.NormalBatcher[] writes;
    private final SegmentPointRead.Reader[] reads;
    private final SegmentHotDirectory[] hot;
    private SegmentDataFileLock bookieGuard;
    private SegmentBookieControlLog bookie;
    private SegmentBookieControlDriver controls;
    private volatile SegmentExecutionGroup execution;
    private ProfileReceiveBudget.DataWorkingSet dataCredit;
    private ProfileReceiveBudget.ControlWorkingSet controlCredit;
    private boolean attempted;
    private boolean accepting;
    private boolean recovered;
    private boolean stopping;
    private boolean freeing;
    private boolean cleanupAttempted;
    private IOException cleanupFailure;
    private boolean listenerBorrowed;
    private OwnedListener ownedListener;
    private PreflightEpisode startupPreflight;
    private SegmentManifestColdPublication coldPublication;
    private SegmentManifestAccessObservation.Probe accessProbe;
    private boolean observingAccess;
    private boolean observingRecoveryDependencies;

    ManifestSegmentStorage(SegmentArenaManifest manifest, ManifestIdentity expected, IsolatedSegmentConfig runtime,
            SecretKey key, ProfileDescriptorValidator validator, SegmentExecutionGroup.Settings settings,
            IsolatedSegmentStorage.Limits limits, ProfileReceiveBudget budget) throws IOException {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.budget = Objects.requireNonNull(budget, "budget");
        int shards = manifest.shardArenas().size();
        int largestEntry = SegmentBatchFormat.ENTRY_OVERHEAD + SegmentBatchFormat.maxPayload(runtime);
        int largestBatch = Math.max(limits.batches().bytes(),
                SegmentBatchFormat.alignedLength(SegmentBatchFormat.RECORD_HEADER + largestEntry));
        if (!manifest.identity().equals(expected) || !manifest.classicRoots().isEmpty()
                || runtime.logicalShards() != shards || !runtime.affinity().equals("OFF")
                || !runtime.numaPolicy().equals("SYSTEM") || !runtime.waitStrategy().equals("PARK")
                || limits.controlLogBytes() < 4096 + 2048 || limits.hotEntries() < shards
                || settings.workers() > shards || settings.workers() > runtime.maxPendingEntries()
                || settings.inputBudgetBytes() < largestEntry || settings.bufferBytes() < largestBatch
                || limits.poolBlocks() * SegmentArenaControlLog.BLOCK_BYTES < largestBatch) {
            throw new IOException("Exact Profile-only manifest and bounded local execution geometry required");
        }
        control = manifest.bookieConfig(runtime);
        bookieIdentity = SegmentFileHeader.forBookie(manifest);
        var directories = new ArrayList<Path>();
        var locks = new ArrayList<Path>();
        directories.add(control.directory());
        locks.add(control.directory().resolve("bookie-control.lock"));
        for (var member : manifest.arenas()) {
            directories.add(member.directory());
            locks.add(member.directory().resolve("isolated.lock"));
            locks.add(member.directory().resolve("arena-control.lock"));
            long blocks = (member.bytesPerFile() - runtime.alignment()) / SegmentArenaControlLog.BLOCK_BYTES;
            if ((long) limits.poolBlocks() + limits.maintenanceBlocks() > blocks) {
                throw new IOException("Each manifest DATA file needs its foreground pool and maintenance reserve");
            }
        }
        roots = List.copyOf(directories);
        lockPaths = List.copyOf(locks);
        bookieFiles = new SegmentOwnedFiles(lockPaths);
        var arenas = new ArrayList<Member>(manifest.arenas().size());
        for (int i = 0; i < manifest.arenas().size(); i++) {
            arenas.add(new Member(manifest.arenaConfig(runtime, i), SegmentFileHeader.forArena(manifest, i),
                    lockPaths));
        }
        members = List.copyOf(arenas);
        maxEntryBytes = SegmentBatchFormat.ENTRY_OVERHEAD + members.stream()
                .mapToInt(member -> SegmentBatchFormat.maxPayload(member.config)).min().orElseThrow();
        writes = new SegmentDataPhysicalDriver.NormalBatcher[shards];
        reads = new SegmentPointRead.Reader[shards];
        hot = new SegmentHotDirectory[shards];
    }

    static Capacity capacity(SegmentArenaManifest manifest, IsolatedSegmentConfig runtime,
            SegmentExecutionGroup.Settings settings, IsolatedSegmentStorage.Limits limits) throws IOException {
        var global = manifest.bookieConfig(runtime);
        int[] shardCounts = new int[manifest.arenas().size()];
        for (int arena : manifest.shardArenas()) {
            shardCounts[arena]++;
        }
        try {
            var layout = SegmentControlCapacity.Layout.hotSpot();
            long data = SegmentControlCapacity.sum(settings.reservedBufferBytes(), settings.inputBudgetBytes(),
                    SegmentHotDirectory.metadataCapacity(global, settings, limits.hotEntries()),
                    SegmentDataPhysicalDriver.parsingCapacity(global, settings),
                    SegmentPointRead.runtimeCapacity(global, settings),
                    SegmentExecutionGroup.metadataCapacity(global, settings),
                    SegmentEntryInputs.metadataCapacity(settings.inputBudgetBytes()),
                    SegmentDataPhysicalDriver.metadataCapacity(global, settings),
                    layout.object(SegmentProfileListener.class), layout.object(Connection.class));
            for (int i = 0; i < shardCounts.length; i++) {
                var config = manifest.arenaConfig(runtime, i);
                data = SegmentControlCapacity.sum(data, SegmentDerivedIndex.CONFIGURED_CAPACITY_BYTES,
                        SegmentDataRecovery.inventoryCapacity(config, limits.indexedEntries()),
                        SegmentDataRecovery.manifestScanCapacity(config, limits.controlLogBytes(),
                                limits.indexedEntries()),
                        shardCounts[i] == 0 ? 0
                                : SegmentHotDirectory.occupiedCapacity(shardCounts[i], limits.indexedEntries()));
            }
            return new Capacity(data, SegmentControlCapacity.requiredForManifest(manifest, global,
                    limits.controlLogBytes()));
        } catch (ArithmeticException | IllegalArgumentException unsupported) {
            throw new IOException("Manifest DATA capacity cannot be represented", unsupported);
        }
    }

    void startNew() throws IOException, InterruptedException {
        start(false, null, null);
    }

    void restart() throws IOException, InterruptedException {
        start(true, null, null);
    }

    void restart(SegmentManifestAccessObservation observed) throws IOException, InterruptedException {
        start(true, Objects.requireNonNull(observed), null);
    }

    /** Recheck the native source and declared access scope before opening this retained graph. */
    void restartForStartupPreflight(SegmentManifestStartupPolicy.PreOpen preflight,
            SegmentManifestAccessObservation observed, ProfileLedgerMetadataStore metadata, long deadline)
            throws IOException, InterruptedException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || remaining > TimeUnit.MINUTES.toNanos(1)) {
            throw new IOException("Manifest startup requires a bounded remaining deadline");
        }
        var retained = preflight.requireCurrent(manifest, control, observed, metadata, deadline);
        start(true, observed, new PreflightEpisode(preflight, metadata, retained, deadline));
    }

    void requireOwnedTransport(SegmentProfileService.Transport transport, DiskChecker diskChecker) throws IOException {
        requireLoopbackTransport(transport);
        for (var root : SegmentManifestAccessObservation.expectedRoots(manifest, control)) {
            try {
                diskChecker.checkExistingDir(Path.of(root.get("path")).toFile());
            } catch (DiskChecker.DiskWarnThresholdException warning) {
                // The original DiskChecker reports the advisory; its hard threshold still rejects startup.
            }
        }
    }

    void requireLoopbackTransport(SegmentProfileService.Transport transport) {
        if (transport.budget() != budget || !transport.address().getAddress().isLoopbackAddress()
                || transport.control() == null || transport.read() == null) {
            throw new IllegalArgumentException("Manifest service needs its original budget and loopback transport");
        }
    }

    /** The observer borrows an immutable control cut while this one-use owner holds the complete graph. */
    <T> T withRecoveryDependencies(int maximumRows, Duration timeout, RecoveryDependencyObserver<T> observer)
            throws Exception {
        Objects.requireNonNull(observer, "observer");
        long nanos = timeout.toNanos();
        if (nanos <= 0 || nanos > TimeUnit.MINUTES.toNanos(1)) {
            throw new IllegalArgumentException("Manifest recovery observation requires a bounded deadline");
        }
        return withRecoveryDependenciesUntil(maximumRows, System.nanoTime() + nanos, 0, observer);
    }

    <T> T withRecoveryDependenciesUntil(int maximumRows, long deadline, long additionalControlBytes,
            RecoveryDependencyObserver<T> observer) throws Exception {
        return withRecoveryDependenciesUntil(maximumRows, deadline, 0, additionalControlBytes, observer);
    }

    private <T> T withRecoveryDependenciesUntil(int maximumRows, long deadline, long additionalDataBytes,
            long additionalControlBytes, RecoveryDependencyObserver<T> observer) throws Exception {
        Objects.requireNonNull(observer, "observer");
        if (deadline - System.nanoTime() > TimeUnit.MINUTES.toNanos(1)
                || additionalDataBytes < 0 || additionalControlBytes < 0) {
            throw new IllegalArgumentException("Manifest recovery observation requires bounded time and capacity");
        }
        var rows = new SegmentStorageInspector.RecoveryRows(maximumRows);
        synchronized (lifecycle) {
            claimStartup();
            observingRecoveryDependencies = true;
        }
        Throwable failed = null;
        T result;
        try {
            requireRecoveryObservationLive(deadline);
            reserveCapacity(capacity(manifest, control, settings, limits)
                    .withRecoveryDependencies(members.size(), maximumRows)
                    .withAdditionalData(additionalDataBytes)
                    .withAdditionalControl(additionalControlBytes));
            requireRoots(true);
            acquireGuards(true);
            var paths = recoveryPathIdentities();
            var before = readRecoveryDependencies(rows, deadline);
            requireRecoveryPaths(paths, deadline);
            result = observer.observe(before);
            requireRecoveryPaths(paths, deadline);
            if (!before.equals(readRecoveryDependencies(new SegmentStorageInspector.RecoveryRows(maximumRows),
                    deadline))) {
                throw new IOException("Manifest recovery control dependencies changed during observation");
            }
            requireRecoveryPaths(paths, deadline);
        } catch (Exception | Error failure) {
            failed = failure;
            throw failure;
        } finally {
            finishOfflineObservation(failed);
        }
        requireRecoveryObservationTime(deadline);
        return result;
    }

    /** Local full-byte evidence bound to the current native CLOSED cohort under the original guards. */
    ClosedDataObservation observeClosedData(int maximumRows, long maximumDataBytes, int maximumEntries,
            Duration timeout, ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations)
            throws Exception {
        return observeClosedGraph(maximumRows, maximumDataBytes, maximumEntries, timeout, metadata, reservations,
                null, null).data();
    }

    /** Borrow the registered transports from an external caller while retaining the original graph. */
    ClosedSourceObservation observeClosedSources(int maximumRows, long maximumDataBytes, int maximumEntries,
            Duration timeout, ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations,
            SegmentClosedReconstruction.Sources sources, SegmentClosedReconstruction.OldPublisher publisher)
            throws Exception {
        Objects.requireNonNull(sources, "sources");
        if (publisher == null) {
            throw new IOException("Manifest CLOSED source audit requires the original controlled publisher witness");
        }
        return observeClosedGraph(maximumRows, maximumDataBytes, maximumEntries, timeout, metadata, reservations,
                sources, publisher);
    }

    private ClosedSourceObservation observeClosedGraph(int maximumRows, long maximumDataBytes, int maximumEntries,
            Duration timeout, ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations,
            SegmentClosedReconstruction.Sources sources, SegmentClosedReconstruction.OldPublisher publisher)
            throws Exception {
        long deadline = SegmentClosedReconstruction.manifestClosedDeadline(timeout);
        if (maximumDataBytes < 1 || maximumEntries < 1 || maximumEntries > 65536) {
            throw new IllegalArgumentException("Manifest CLOSED audit requires bounded DATA geometry");
        }
        long totalBytes = 0;
        for (Member member : members) {
            totalBytes = Math.addExact(totalBytes, dataBytes(member.config));
        }
        if (totalBytes > maximumDataBytes) {
            throw new IOException("Manifest CLOSED audit exceeds its total DATA byte limit");
        }
        long controlBytes = SegmentControlCapacity.sum(SegmentClosedReconstruction.manifestClosedControlBytes(),
                sources == null ? 0 : SegmentClosedReconstruction.manifestSourceControlBytes());
        return withRecoveryDependenciesUntil(maximumRows, deadline,
                closedDataCapacity(manifest, maximumRows, maximumEntries), controlBytes, graph -> {
                    var reader = reservations.closedReconstructionReader(metadata, bookieIdentity.bookieId(),
                            bookieIdentity.incarnation(), deadline);
                    var exited = publisher == null ? null
                            : publisher.requireManifestExit(bookieIdentity, sources.registry(), reader.deadline());
                    var cohort = SegmentClosedReconstruction.readManifestClosedCohort(graph, reader);
                    var nativeIo = new NativeIOImpl();
                    var images = auditClosedData(graph, cohort, maximumEntries, nativeIo);
                    Map<ProfileClosedReconstructionSources, String> registrations = sources == null ? Map.of()
                            : new LinkedHashMap<>();
                    List<ProfileClosedReconstruction.Member> verified = sources == null ? List.of()
                            : auditClosedSources(graph, cohort, images, sources, registrations);
                    cohort.revalidate(graph);
                    if (publisher != null) {
                        SegmentClosedReconstruction.verifySourceRegistrations(registrations);
                        publisher.requireManifestExit(bookieIdentity, sources.registry(), reader.deadline());
                    }
                    for (int i = 0; i < images.size(); i++) {
                        requireClosedAuditLive(cohort);
                        var member = members.get(i);
                        var image = images.get(i);
                        Path root = member.config.directory();
                        if (!image.rootObject().equals(SegmentClosedReconstruction.observeDirectory(
                                member.owned, nativeIo, root))) {
                            throw new IOException("Original Arena root changed during the manifest CLOSED audit");
                        }
                        SegmentClosedReconstruction.verifyOriginalPaths(root, image.files());
                    }
                    requireClosedAuditLive(cohort);
                    var data = new ClosedDataObservation(new SegmentClosedReconstruction.ManifestClosedObservation(
                            graph, cohort.members()), images);
                    return new ClosedSourceObservation(data, exited, verified);
                });
    }

    private List<ProfileClosedReconstruction.Member> auditClosedSources(RecoveryDependencies graph,
            SegmentClosedReconstruction.ManifestClosedCohort cohort, List<ClosedArenaData> images,
            SegmentClosedReconstruction.Sources sources, Map<ProfileClosedReconstructionSources, String> registrations)
            throws Exception {
        var verified = new ArrayList<ProfileClosedReconstruction.Member>(cohort.members().size());
        for (int i = 0; i < cohort.members().size(); i++) {
            requireClosedAuditLive(cohort);
            var member = cohort.members().get(i);
            String digest;
            if (member.kind() == ProfileClosedReconstruction.Kind.EMPTY_TERMINAL) {
                digest = ProfileClosedReconstruction.emptySourceDigest(member.closedDigest());
            } else {
                var observed = cohort.readVerified(graph, i);
                var entries = images.get(member.route().arena()).entries().get(member.ledger());
                try (var pair = ProfileClosedReconstructionSources.open(sources.sdk(), sources.eventLoop(),
                        sources.tls(), sources.registry(), sources.cold(), observed.snapshot(),
                        bookieIdentity.bookieId(), maxEntryBytes, cohort.reader().deadline())) {
                    for (int entry = 0; entry < entries.size(); entry++) {
                        requireClosedAuditLive(cohort);
                        var local = entries.get(entry);
                        pair.verify(entry, local.cumulativeLength(), local.payloadLength(), local.payloadSha256());
                    }
                    digest = pair.revalidate();
                    registrations.put(pair, digest);
                }
            }
            verified.add(new ProfileClosedReconstruction.Member(member.ledger().ledgerId(), member.ledger(),
                    member.closedDigest(), digest, member.kind()));
        }
        return List.copyOf(verified);
    }

    private List<ClosedArenaData> auditClosedData(RecoveryDependencies graph,
            SegmentClosedReconstruction.ManifestClosedCohort cohort, int maximumEntries, NativeIOImpl nativeIo)
            throws IOException {
        var ledgers = new ArrayList<List<SegmentDataRecovery.ClosedImageMember>>(members.size());
        for (int i = 0; i < members.size(); i++) {
            ledgers.add(new ArrayList<>());
        }
        long totalEntries = 0;
        int[] entryCounts = new int[members.size()];
        for (var ledger : cohort.members()) {
            if (ledger.lastEntry() < -1 || ledger.lastEntry() >= maximumEntries) {
                throw new IOException("Manifest CLOSED audit exceeds its total entry limit");
            }
            int count = Math.toIntExact(ledger.lastEntry() + 1);
            totalEntries = Math.addExact(totalEntries, count);
            if (totalEntries > maximumEntries) {
                throw new IOException("Manifest CLOSED audit exceeds its total entry limit");
            }
            int arena = ledger.route().arena();
            entryCounts[arena] += count;
            ledgers.get(arena).add(new SegmentDataRecovery.ClosedImageMember(ledger.ledger(),
                    ledger.lastEntry(), ledger.length()));
        }

        SegmentDataRecovery.AuditCheckpoint checkpoint = () -> requireClosedAuditLive(cohort);
        var objects = new HashSet<FileIdentity>();
        var images = new ArrayList<ClosedArenaData>(members.size());
        for (int i = 0; i < members.size(); i++) {
            checkpoint.check();
            var member = members.get(i);
            var arena = graph.arenas().get(i).control();
            if (!arena.cut().recordedQuarantineFiles().isEmpty()) {
                throw new IOException("Manifest CLOSED audit encountered a quarantined Arena");
            }
            SegmentStorageInspector.requireReconstructionMode(arena, graph.bookie().observedSequence(), arena.mode());
            Path root = member.config.directory();
            var rootObject = SegmentClosedReconstruction.observeDirectory(member.owned, nativeIo, root);
            var audit = new SegmentDataRecovery.ClosedImageAudit(member.config, member.identity, arena, ledgers.get(i),
                    dataBytes(member.config), Math.max(1, entryCounts[i]));
            var files = SegmentClosedReconstruction.auditOriginals(member.config, root, rootObject, member.owned,
                    nativeIo, audit, objects, checkpoint);
            images.add(new ClosedArenaData(member.identity, rootObject, files, audit.finish()));
        }
        return List.copyOf(images);
    }

    private static long dataBytes(IsolatedSegmentConfig config) {
        return Math.multiplyExact(config.bytesPerFile(), config.dataFiles());
    }

    private void requireClosedAuditLive(SegmentClosedReconstruction.ManifestClosedCohort cohort) throws IOException {
        cohort.reader().requireTime();
        synchronized (gate) {
            if (stopping) {
                throw new IOException("Manifest CLOSED audit retired");
            }
        }
    }

    private RecoveryDependencies readRecoveryDependencies(SegmentStorageInspector.RecoveryRows rows, long deadline)
            throws IOException, InterruptedException {
        requireRecoveryObservationLive(deadline);
        var shared = SegmentBookieControlLog.inspectRecoveryDependencies(control, bookieIdentity,
                limits.controlLogBytes(), key, validator, rows, bookieGuard, bookieFiles, List.of());
        var arenas = new ArrayList<ArenaRecoveryDependencies>(members.size());
        for (Member member : members) {
            requireRecoveryObservationLive(deadline);
            member.dataGuard.requireHeld(member.config.directory().resolve("isolated.lock"));
            member.identity.requireStoredManifest(member.config, -4, member.owned);
            var dependency = SegmentArenaControlLog.inspectRecoveryDependencies(member.config, member.identity,
                    limits.controlLogBytes(), rows, member.controlGuard, member.owned, List.of());
            arenas.add(new ArenaRecoveryDependencies(member.identity, dependency));
        }
        var ledgerRoutes = new HashMap<ProfileLedgerContext, LedgerRoute>();
        for (var ledger : shared.ledgers()) {
            int shard = shard(ledger.context());
            ledgerRoutes.put(ledger.context(), new LedgerRoute(shard, manifest.arenaForShard(shard)));
        }
        return new RecoveryDependencies(manifest.identity(), bookieIdentity, shared, List.copyOf(arenas),
                Map.copyOf(ledgerRoutes));
    }

    private void requireRecoveryPaths(List<FileIdentity> expected, long deadline)
            throws IOException, InterruptedException {
        requireRecoveryObservationLive(deadline);
        if (!expected.equals(recoveryPathIdentities())) {
            throw new IOException("Manifest recovery observation path identity changed");
        }
        requireRecoveryObservationLive(deadline);
    }

    private List<FileIdentity> recoveryPathIdentities() throws IOException {
        var paths = new ArrayList<FileIdentity>();
        for (Path root : roots) {
            paths.add(recoveryPathIdentity(root, true));
        }
        paths.add(recoveryPathIdentity(manifest.path(), false));
        var nativeIo = new NativeIOImpl();
        paths.add(recoveryGuardIdentity(nativeIo, bookieGuard, lockPaths.get(0)));
        for (int i = 0; i < members.size(); i++) {
            paths.add(recoveryGuardIdentity(nativeIo, members.get(i).dataGuard, lockPaths.get(2 * i + 1)));
            paths.add(recoveryGuardIdentity(nativeIo, members.get(i).controlGuard, lockPaths.get(2 * i + 2)));
        }
        return paths;
    }

    private static FileIdentity recoveryGuardIdentity(NativeIOImpl nativeIo, SegmentDataFileLock guard, Path path)
            throws IOException {
        guard.requireHeld(path);
        FileIdentity actual = recoveryPathIdentity(path, false);
        if (!actual.equals(nativeIo.fileIdentity(guard.channel()))) {
            throw new IOException("Manifest recovery guard differs from its original path: " + path);
        }
        return actual;
    }

    private static FileIdentity recoveryPathIdentity(Path path, boolean directory) throws IOException {
        var attributes = Files.readAttributes(path, "unix:dev,ino,isDirectory,isRegularFile",
                LinkOption.NOFOLLOW_LINKS);
        if (!(Boolean) attributes.get(directory ? "isDirectory" : "isRegularFile")
                || !path.equals(path.toRealPath())) {
            throw new IOException("Manifest recovery path is no longer its canonical type: " + path);
        }
        return new FileIdentity((Long) attributes.get("dev"), (Long) attributes.get("ino"));
    }

    private void requireRecoveryObservationLive(long deadline) throws IOException, InterruptedException {
        requireRecoveryObservationTime(deadline);
        synchronized (gate) {
            if (stopping) {
                throw new IOException("Manifest recovery observation retired");
            }
        }
    }

    private static void requireRecoveryObservationTime(long deadline) throws IOException, InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("Manifest recovery observation interrupted");
        }
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Manifest recovery observation deadline elapsed");
        }
    }

    /** A one-use offline observer. A timed-out result may leave the caller responsible for an explicit drain. */
    SegmentManifestAccessObservation observeAccess(SegmentManifestAccessObservation.OldScope old, long newUid,
            Path probe, Duration timeout) throws IOException, InterruptedException {
        boolean claimed = false;
        Throwable failed = null;
        try {
            SegmentManifestAccessObservation.Probe pending;
            synchronized (lifecycle) {
                claimStartup();
                claimed = true;
                reserveCapacity();
                requireRoots(true);
                acquireGuards(true);
                var requiredWrites = new HashSet<Path>();
                preflightRestart(requiredWrites);
                synchronized (gate) {
                    if (stopping) {
                        throw new IOException("Manifest access observation retired");
                    }
                }
                pending = SegmentManifestAccessObservation.startProbe(manifest, control, old, newUid, probe,
                        requiredWrites, timeout);
                accessProbe = pending;
                observingAccess = true;
            }
            var result = pending.await(timeout);
            if (Thread.interrupted()) {
                throw new InterruptedException("Manifest access observation interrupted");
            }
            synchronized (gate) {
                if (stopping) {
                    throw new IOException("Manifest access observation retired");
                }
            }
            return result;
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            failed = failure;
            throw failure;
        } finally {
            if (claimed) {
                finishOfflineObservation(failed);
            }
        }
    }

    private void finishOfflineObservation(Throwable failed) throws IOException, InterruptedException {
        boolean interrupted = failed instanceof InterruptedException;
        synchronized (lifecycle) {
            observingAccess = false;
            observingRecoveryDependencies = false;
            try {
                if (!close(Duration.ofSeconds(5)) && failed == null) {
                    throw new IOException("Manifest offline observation did not drain");
                }
            } catch (IOException | InterruptedException cleanup) {
                interrupted |= cleanup instanceof InterruptedException;
                if (failed == null) {
                    throw cleanup;
                }
                if (failed != cleanup) {
                    failed.addSuppressed(cleanup);
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void claimStartup() {
        synchronized (gate) {
            if (attempted || stopping) {
                throw new IllegalStateException("Manifest storage startup is one-use");
            }
            attempted = true;
        }
    }

    private void start(boolean existing, SegmentManifestAccessObservation observed, PreflightEpisode preflight)
            throws IOException, InterruptedException {
        synchronized (lifecycle) {
            claimStartup();
            try {
                reserveCapacity();
                requireRoots(existing);
                acquireGuards(existing);
                RestartPlan restart = null;
                if (existing) {
                    Set<Path> writes = observed == null ? null : new HashSet<>();
                    restart = preflightRestart(writes);
                    if (observed != null) {
                        observed.requireCurrent(manifest, control, writes);
                    }
                    requireRestartTime(preflight);
                } else {
                    SegmentBookieControlLog.formatManifestControl(control, bookieIdentity, bookieGuard, bookieFiles,
                            new SegmentFileIo());
                }
                bookie = SegmentBookieControlLog.openManifestWhileLocked(control, bookieIdentity,
                        limits.controlLogBytes(), key, validator, new SegmentFileIo(), bookieGuard, bookieFiles);
                if (restart != null) {
                    settleRestartSessions(restart, preflight);
                }
                for (Member member : members) {
                    prepareMember(member, existing);
                }
                for (Member member : members) {
                    member.files.startSession(member.arena);
                }
                execution = SegmentExecutionGroup.prepareOff(control, settings);
                execution.startThreads();
                controls = new SegmentBookieControlDriver(execution.owner(0), execution.io(), bookie,
                        this::controlAdmission);
                bootstrapShards();
                synchronized (gate) {
                    if (stopping || !execution.accepting() || execution.hasFailedOwner()) {
                        throw new IOException("Manifest startup lost the complete local graph");
                    }
                    recovered = existing;
                    startupPreflight = preflight;
                    accepting = true;
                }
            } catch (IOException | InterruptedException | RuntimeException | Error failure) {
                try {
                    if (!close(Duration.ofSeconds(5))) {
                        failure.addSuppressed(new IOException("Manifest startup cleanup is still draining"));
                    }
                } catch (IOException | InterruptedException cleanup) {
                    if (failure != cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    if (cleanup instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
                throw failure;
            }
        }
    }

    private void reserveCapacity() throws IOException {
        reserveCapacity(capacity(manifest, control, settings, limits));
    }

    private void reserveCapacity(Capacity capacity) throws IOException {
        try {
            dataCredit = budget.reserveDataWorkingSet(capacity.dataBytes());
            controlCredit = budget.reserveControlWorkingSet(capacity.controlBytes());
        } catch (ProfileReceiveBudget.CapacityExceededException exhausted) {
            throw new IOException("Insufficient budget for the complete manifest storage graph", exhausted);
        }
    }

    private void requireRoots(boolean existing) throws IOException {
        for (int i = 0; i < roots.size(); i++) {
            Path root = roots.get(i);
            if (existing) {
                if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || !root.equals(root.toRealPath())) {
                    throw new IOException("Required manifest root must be an existing canonical directory: " + root);
                }
                for (int j = 0; j < i; j++) {
                    if (Files.isSameFile(root, roots.get(j))) {
                        throw new IOException("Manifest roots alias each other");
                    }
                }
            } else {
                Path parent = root.getParent();
                if (!parent.equals(parent.toRealPath()) || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Fresh manifest root needs a canonical existing parent: " + root);
                }
                try {
                    Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    throw new IOException("Fresh manifest cannot adopt an existing root: " + root);
                } catch (NoSuchFileException absent) {
                    // All roots must be new before any file is created.
                }
            }
        }
        if (existing) {
            requireDistinctLocks();
        }
    }

    private void requireDistinctLocks() throws IOException {
        for (int i = 0; i < lockPaths.size(); i++) {
            Path path = lockPaths.get(i);
            if (!Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isRegularFile()) {
                throw new IOException("Required manifest lock must be a regular file: " + path);
            }
            for (int j = 0; j < i; j++) {
                if (Files.isSameFile(path, lockPaths.get(j))) {
                    throw new IOException("Manifest locks alias each other");
                }
            }
        }
    }

    private void acquireGuards(boolean existing) throws IOException {
        if (!existing) {
            createRoot(control.directory());
        }
        bookieGuard = controlGuard(control.directory().resolve("bookie-control.lock"), existing);
        bookieGuard.initialize();
        for (Member member : members) {
            Path directory = member.config.directory();
            if (!existing) {
                createRoot(directory);
            }
            member.dataGuard = SegmentDataFileLock.dataOwner(directory, !existing);
            member.dataGuard.initialize();
            member.controlGuard = controlGuard(directory.resolve("arena-control.lock"), existing);
            member.controlGuard.initialize();
        }
        requireDistinctLocks();
    }

    private static SegmentDataFileLock controlGuard(Path path, boolean existing) {
        return existing ? SegmentDataFileLock.existingControlOwner(path)
                : SegmentDataFileLock.creatingControlOwner(path);
    }

    private static void createRoot(Path directory) throws IOException {
        Files.createDirectory(directory,
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    }

    private RestartPlan preflightRestart(Set<Path> writes) throws IOException {
        var bookieInspection = SegmentBookieControlLog.inspectManifestWhileLocked(control, bookieIdentity,
                limits.controlLogBytes(), key, validator, bookieGuard, bookieFiles);
        long bookieCut = bookieInspection.cut();
        var arenas = new ArrayList<SegmentArenaControlLog.ManifestRestart>(members.size());
        if (writes != null) {
            writes.addAll(lockPaths);
            writes.add(control.directory().resolve("bookie-control"));
            writes.add(bookieInspection.activeJournal());
            for (String name : List.of("local-readiness", "local-readiness.lock")) {
                Path file = control.directory().resolve(name);
                if (Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
                    writes.add(file);
                }
            }
        }
        for (Member member : members) {
            member.dataGuard.requireHeld(member.config.directory().resolve("isolated.lock"));
            try (var arena = SegmentArenaControlLog.openManifestWhileLocked(member.config, member.identity,
                    limits.controlLogBytes(), new SegmentFileIo(), member.controlGuard, member.owned, true)) {
                arenas.add(arena.manifestRestart(bookieCut));
                if (writes != null) {
                    writes.add(member.config.directory().resolve("arena-control"));
                    writes.add(arena.manifestRestartJournal());
                }
            }
            SegmentDerivedIndex.requireExistingScope(member.config, member.identity);
            if (writes != null) {
                writes.add(SegmentDerivedIndex.directory(member.config).resolve("LOCK"));
                for (int file = 0; file < member.config.dataFiles(); file++) {
                    writes.add(IsolatedSegmentFiles.dataPath(member.config, file));
                }
            }
            for (int slot = 0; slot < 2; slot++) {
                Path coverage = SegmentDerivedIndex.publication(member.config, slot);
                if (Files.exists(coverage, LinkOption.NOFOLLOW_LINKS)) {
                    if (writes != null) {
                        writes.add(coverage);
                    }
                    for (Path lock : lockPaths) {
                        if (Files.isSameFile(coverage, lock)) {
                            throw new IOException("Index coverage aliases a manifest lock");
                        }
                    }
                }
            }
        }
        return new RestartPlan(bookieCut, List.copyOf(arenas));
    }

    private void settleRestartSessions(RestartPlan restart, PreflightEpisode preflight)
            throws IOException, InterruptedException {
        if (bookie.appliedThrough() != restart.bookieCut()) {
            throw new IOException("Shared Bookie cut changed after manifest preflight");
        }
        for (int index = 0; index < members.size(); index++) {
            requireRestartTime(preflight);
            Member member = members.get(index);
            openMemberControl(member);
            if (!restart.arenas().get(index).equals(member.arena.manifestRestart(restart.bookieCut()))) {
                throw new IOException("Arena session or cut changed after manifest preflight");
            }
        }
        for (int index = 0; index < members.size(); index++) {
            if (restart.arenas().get(index).open()) {
                requireRestartTime(preflight);
                Member member = members.get(index);
                member.arena.resolveUnclosedDataSession(member.dataGuard);
            }
        }
        requireRestartTime(preflight);
    }

    private static void requireRestartTime(PreflightEpisode preflight) throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Manifest restart interrupted before mutable recovery");
        }
        if (preflight != null && System.nanoTime() >= preflight.deadline()) {
            throw new IOException("Manifest startup preflight deadline elapsed before mutable recovery");
        }
    }

    private void prepareMember(Member member, boolean existing) throws IOException {
        if (!existing) {
            member.files = IsolatedSegmentFiles.createWhileLocked(member.config, member.identity,
                    new SegmentFileIo(), member.dataGuard, member.owned);
            openMemberControl(member);
        }
        member.index = SegmentDerivedIndex.open(member.config, member.identity, limits.indexedEntries(), member.owned);
        if (existing) {
            member.files = IsolatedSegmentFiles.prepareLocalResults(member.config, member.identity, member.arena,
                    bookie, new SegmentFileIo(), limits.indexedEntries(), writes.length, member.index,
                    member.dataGuard, member.owned);
        }
    }

    private void openMemberControl(Member member) throws IOException {
        member.arena = SegmentArenaControlLog.openManifestWhileLocked(member.config, member.identity,
                limits.controlLogBytes(), new SegmentFileIo(), member.controlGuard, member.owned, false);
    }

    private void bootstrapShards() throws IOException, InterruptedException {
        for (int i = 0; i < members.size(); i++) {
            Member member = members.get(i);
            int controlShard = Math.max(0, manifest.shardArenas().indexOf(i));
            member.allocator = new SegmentArenaControlDriver(execution.owner(controlShard), execution.io(),
                    member.arena, limits.maintenanceBlocks());
        }
        int[] counts = new int[members.size()];
        for (int arena : manifest.shardArenas()) {
            counts[arena]++;
        }
        int[] ordinals = new int[members.size()];
        long end = System.nanoTime() + settings.startupTimeout().toNanos();
        for (int shard = 0; shard < writes.length; shard++) {
            int selected = shard;
            int mapped = manifest.arenaForShard(shard);
            Member member = members.get(mapped);
            int coordinates = share(limits.indexedEntries(), counts[mapped], ordinals[mapped]++);
            SegmentOwnerLoop owner = execution.owner(shard);
            var task = new FutureTask<Void>(() -> {
                var stream = member.arena.restartStreamForShard(selected).orElse(
                        new SegmentArenaControlLog.SingleStream(new Generation(1), 1));
                int capacity = share(limits.hotEntries(), writes.length, selected);
                var directory = new SegmentHotDirectory(owner, member.config, member.identity, capacity, coordinates);
                hot[selected] = directory;
                directory.attachIndex(selected, member.index, execution.io());
                var driver = new SegmentDataPhysicalDriver(selected, owner, execution.io(), member.files, member.arena,
                        new SegmentPhysicalPrefix(owner, selected, stream.generation(), stream.firstSequence(),
                                member.config.maxPendingEntries()), directory);
                writes[selected] = driver.enableNormalBatching(bookie, limits.batches(), member.allocator,
                        limits.poolBlocks());
                reads[selected] = SegmentPointRead.Reader.live(owner, execution.io(), member.files, member.arena,
                        bookie, directory);
                return null;
            });
            if (!owner.control(task)) {
                throw new IOException("Manifest shard bootstrap control capacity exhausted");
            }
            IsolatedSegmentStorage.await(task, end);
        }
        for (int worker = 0; worker < settings.workers(); worker++) {
            var owner = execution.owner(worker);
            while (owner.hasPendingCommands()) {
                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new IOException("Manifest bootstrap commands did not terminate");
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException("Manifest startup interrupted");
                }
                LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
            }
            if (owner.failure() != null) {
                throw new IOException("Manifest bootstrap owner failed", owner.failure());
            }
        }
    }

    private static int share(int total, int count, int ordinal) {
        return total / count + (ordinal < total % count ? 1 : 0);
    }

    SegmentBookieControlDriver controls() {
        synchronized (gate) {
            if (!isLive()) {
                throw new IllegalStateException("Manifest storage is not accepting control requests");
            }
            return controls;
        }
    }

    /** Pure scope validation available before any local graph or mutable file is opened. */
    static void requireRecoverySourceScope(SegmentArenaManifest manifest,
            ZKProfileStartupRequirementsStore.Observation source) throws IOException {
        var commissioned = source.commissionedCompatibility();
        var devices = source.requiredDevices();
        if (!source.isManifestSource() || !manifest.classicRoots().isEmpty()
                || !commissioned.bookieId().equals(manifest.bookieId().toString())
                || !commissioned.storageIncarnation().equals(manifest.incarnation())
                || !commissioned.deviceManifestIdentity().equals(manifest.identity())
                || !devices.identity().equals(manifest.identity())
                || !commissioned.deviceManifestGeneration().equals(INITIAL_GENERATION)
                || !commissioned.localFormatGeneration().equals(INITIAL_GENERATION)
                || !devices.generation().equals(INITIAL_GENERATION)
                || !commissioned.effectiveDeleteAssignmentGeneration().equals(INITIAL_GENERATION)
                || !source.compatibility().effectiveDeleteAssignmentGeneration().equals(INITIAL_GENERATION)
                || devices.devices().size() != manifest.arenas().size()) {
            throw new IOException("Required source differs from the fixed Profile manifest or G1 scope");
        }
        for (int i = 0; i < manifest.arenas().size(); i++) {
            var required = devices.devices().get(i);
            if (!required.deviceId().equals(manifest.arenas().get(i).id().toString())
                    || !required.storageIncarnation().equals(manifest.incarnation())
                    || !required.manifestGeneration().equals(INITIAL_GENERATION)) {
                throw new IOException("Required device differs from the original complete Arena vector");
            }
        }
    }

    /** Cold, source-bound recovery observation. This produces no publication or runtime admission policy. */
    RecoveryFactsAttempt recoveredStartupFacts(ZKProfileStartupRequirementsStore requirements,
            ZKProfileStartupRequirementsStore.Observation source, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, Duration timeout) {
        IsolatedSegmentStorage.requireDeleteReplayArguments(reservations, metadata, domain, maxEvents, timeout,
                this::coldRecoveryAvailable);
        return recoveredStartupFactsUntil(requirements, source, reservations, metadata, domain, maxEvents,
                System.nanoTime() + timeout.toNanos(), null);
    }

    /** Consume only the original graph opened by the preflight. Keep any real DELETE completion on rejection. */
    StartupPreflightAttempt checkRecoveredStartup(String domain, int maxEvents) {
        PreflightEpisode episode;
        synchronized (gate) {
            episode = startupPreflight;
            if (episode == null || !coldRecoveryAvailable()) {
                return new StartupPreflightAttempt(
                        new RecoveryFactsAttempt(RecoveryFactsStatus.GRAPH_UNAVAILABLE, null, null), null);
            }
        }
        long remaining = episode.deadline() - System.nanoTime();
        if (remaining <= 0) {
            return new StartupPreflightAttempt(
                    new RecoveryFactsAttempt(RecoveryFactsStatus.DEADLINE_EXPIRED, null, null), null);
        }
        var preflight = episode.preflight();
        IsolatedSegmentStorage.requireDeleteReplayArguments(preflight.reservations, episode.metadata(), domain,
                maxEvents, Duration.ofNanos(remaining), this::coldRecoveryAvailable);
        var result = recoveredStartupFactsUntil(preflight.requirements, preflight.source, preflight.reservations,
                episode.metadata(), domain, maxEvents, episode.deadline(), episode.retained());
        if (result.status() != RecoveryFactsStatus.COMPLETE) {
            return new StartupPreflightAttempt(result, null);
        }
        var checked = new CheckedStartup(this, preflight.policy, preflight.source.requiredDevices(), result.facts());
        try {
            requireOriginalGraph(checked, episode.deadline());
            return new StartupPreflightAttempt(result, checked);
        } catch (IOException stale) {
            var status = System.nanoTime() >= episode.deadline()
                    ? RecoveryFactsStatus.DEADLINE_EXPIRED : RecoveryFactsStatus.GRAPH_UNAVAILABLE;
            return new StartupPreflightAttempt(new RecoveryFactsAttempt(status, null, result.delete()), null);
        }
    }

    /** Local-only recheck. Native authority and publication, if later implemented, need their own fences. */
    ProfileControlRequest.DeleteBinding requireOriginalGraph(CheckedStartup checked, long deadline) throws IOException {
        return requireOriginalGraph(checked, deadline, null);
    }

    ProfileControlRequest.DeleteBinding requireOriginalGraph(CheckedStartup checked, long deadline,
            OwnedListener pending) throws IOException {
        synchronized (gate) {
            if (checked.owner != this || System.nanoTime() >= deadline) {
                throw new IOException("Checked startup does not belong to this live original graph or deadline");
            }
            LocalCuts actual;
            if (pending == null) {
                actual = recoveredCuts();
            } else {
                if (ownedListener != pending || pending.checked != checked || !listenerBorrowed
                        || !pending.listener.isListening()
                        || !pending.admission.isPending() || !recovered || freeing || !isLive()) {
                    throw new IOException("Original owned pending listener is no longer available");
                }
                actual = inspectRecoveredCuts();
            }
            var expected = checked.facts();
            if (actual.bookie() != expected.bookieControlThrough() || !actual.arenas().equals(expected.arenas())
                    || !actual.rollback().equals(expected.rollback())
                    || !Objects.equals(actual.applied(), expected.appliedDelete()) || System.nanoTime() >= deadline) {
                throw new IOException("Checked startup no longer matches the original recovered cuts");
            }
            return actual.applied();
        }
    }

    /** Bind one isolated cold publication to this graph. The caller owns its actual session and demotion. */
    SegmentManifestColdPublication bindColdPublication(CheckedStartup checked, ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, ZKProfileEphemeralRegistrationStore.Admission admission,
            Generation readiness, Generation capabilities, String endpoint, List<Integer> hints) throws IOException {
        return bindPublication(checked, persistent, publisher, admission, readiness, capabilities,
                endpoint, hints, null);
    }

    SegmentManifestColdPublication bindOwnedPublication(OwnedListener pending, ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, Generation readiness, Generation capabilities,
            List<Integer> hints) throws IOException {
        var bound = pending.listener.bound().getNow(null);
        if (bound == null || !bound.getAddress().isLoopbackAddress() || bound.getPort() <= 0) {
            throw new IOException("Original owned manifest listener has no actual bound loopback address");
        }
        String host = bound.getAddress().getHostAddress();
        String endpoint = "tls://" + (host.contains(":") ? "[" + host + "]" : host) + ":" + bound.getPort();
        return bindPublication(pending.checked, persistent, publisher, pending.admission, readiness, capabilities,
                endpoint, hints, pending);
    }

    private SegmentManifestColdPublication bindPublication(CheckedStartup checked,
            ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, ZKProfileEphemeralRegistrationStore.Admission admission,
            Generation readiness, Generation capabilities, String endpoint, List<Integer> hints, OwnedListener pending)
            throws IOException {
        synchronized (gate) {
            var episode = startupPreflight;
            if (episode == null || coldPublication != null && !coldPublication.drained()) {
                throw new IOException("Original manifest graph has no available cold publication attempt");
            }
            requireOriginalGraph(checked, episode.deadline(), pending);
            coldPublication = new SegmentManifestColdPublication(new ColdPublicationOwner(this, checked, pending),
                    persistent, publisher, admission, readiness, capabilities, endpoint, hints);
            return coldPublication;
        }
    }

    private RecoveryFactsAttempt recoveredStartupFactsUntil(ZKProfileStartupRequirementsStore requirements,
            ZKProfileStartupRequirementsStore.Observation source, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, long deadline,
            ZKProfileReservationStore.RetainedG1DeleteReplay original) {
        Objects.requireNonNull(requirements, "requirements");
        Objects.requireNonNull(source, "source");
        try {
            requireRecoverySourceScope(manifest, source);
        } catch (IOException mismatch) {
            return new RecoveryFactsAttempt(RecoveryFactsStatus.SOURCE_UNVERIFIED, null, null);
        }

        LocalCuts before;
        IsolatedSegmentStorage.DeleteReplayTarget replay;
        synchronized (gate) {
            try {
                before = recoveredCuts();
                replay = deleteReplayTarget();
            } catch (IOException unavailable) {
                return new RecoveryFactsAttempt(RecoveryFactsStatus.GRAPH_UNAVAILABLE, null, null);
            }
        }
        if (!requirements.revalidateManifestSource(source, reservations, deadline)) {
            return new RecoveryFactsAttempt(RecoveryFactsStatus.SOURCE_UNVERIFIED, null, null);
        }
        var target = new ProfileDeleteRecord.Target(manifest.bookieId().toString(), manifest.incarnation().value());
        var retained = original == null ? reservations.retainedG1DeleteReplay(metadata, target, deadline) : original;
        var deletion = IsolatedSegmentStorage.replayProfileDeletesUntil(replay, reservations, metadata, domain,
                maxEvents, deadline, this::coldRecoveryAvailable, retained);
        if (deletion.status() != IsolatedSegmentStorage.DeleteCatchUpStatus.CAUGHT_UP) {
            return new RecoveryFactsAttempt(RecoveryFactsStatus.DELETE_INCOMPLETE, null, deletion);
        }
        if (!requirements.revalidateManifestSource(source, reservations, deadline)) {
            return new RecoveryFactsAttempt(RecoveryFactsStatus.SOURCE_UNVERIFIED, null, deletion);
        }
        var head = deletion.required();
        var fresh = retained.readHead();
        if (fresh.status() != ProfileControlStore.ReadStatus.FOUND || !head.equals(fresh.head())) {
            return new RecoveryFactsAttempt(RecoveryFactsStatus.DELETE_INCOMPLETE, null, deletion);
        }
        synchronized (gate) {
            LocalCuts after;
            try {
                after = recoveredCuts();
            } catch (IOException unavailable) {
                return new RecoveryFactsAttempt(RecoveryFactsStatus.GRAPH_UNAVAILABLE, null, deletion);
            }
            if (!after.arenas().equals(before.arenas())) {
                return new RecoveryFactsAttempt(RecoveryFactsStatus.GRAPH_UNAVAILABLE, null, deletion);
            }
            var progress = ProfileDeleteDelivery.Progress.fromEvent(after.applied());
            if (progress == null ? head.committedThrough() != 0 : !progress.matches(head)) {
                return new RecoveryFactsAttempt(RecoveryFactsStatus.DELETE_INCOMPLETE, null, deletion);
            }
            if (System.nanoTime() >= deadline) {
                return new RecoveryFactsAttempt(RecoveryFactsStatus.DEADLINE_EXPIRED, null, deletion);
            }
            var status = LocalRecoveryFacts.RecoveryStatus.RECOVERED;
            var recovery = new LocalRecoveryFacts(manifest.incarnation(), INITIAL_GENERATION, status, status, status,
                    new Generation(head.assignmentGeneration()));
            var facts = new RecoveredFacts(source.sourceIdentity(), source.commissionedCompatibility(),
                    manifest.bookieId(), manifest.incarnation(), manifest.identity(), NATIVE_FORMATS, after.bookie(),
                    after.arenas(), recovery, after.rollback(), head, after.applied());
            return new RecoveryFactsAttempt(RecoveryFactsStatus.COMPLETE, facts, deletion);
        }
    }

    private boolean coldRecoveryAvailable() {
        synchronized (gate) {
            return recovered && !listenerBorrowed && !freeing && isLive();
        }
    }

    private LocalCuts recoveredCuts() throws IOException {
        if (!coldRecoveryAvailable()) {
            throw new IOException("Original cold recovered manifest graph is unavailable");
        }
        return inspectRecoveredCuts();
    }

    private LocalCuts inspectRecoveredCuts() throws IOException {
        if (execution.onOwnerThread()) {
            throw new IllegalArgumentException("Manifest recovery observation requires an external cold caller");
        }
        for (int worker = 0; worker < settings.workers(); worker++) {
            if (execution.owner(worker).hasPendingCommands()) {
                throw new IOException("Original manifest owner still has work");
            }
        }
        bookieGuard.requireHeld(control.directory().resolve("bookie-control.lock"));
        long cut = bookie.recoveryReadCut(control, bookieIdentity);
        var arenas = new ArrayList<ArenaCut>(members.size());
        for (Member member : members) {
            member.dataGuard.requireHeld(member.config.directory().resolve("isolated.lock"));
            member.controlGuard.requireHeld(member.config.directory().resolve("arena-control.lock"));
            member.files.requireStartupRecovered(member.config, member.identity, member.arena, bookie);
            member.index.requireStartupRecovered(member.config, member.identity);
            arenas.add(new ArenaCut(member.identity.arenaId(),
                    member.arena.requireStartupRecovered(member.config, member.identity)));
        }
        var applied = bookie.deleteAppliedCursor();
        var controls = bookie.rollbackSafetyFacts();
        if (controls.durabilityUnknown() || cut != bookie.recoveryReadCut(control, bookieIdentity) || !isLive()) {
            throw new IOException("Shared recovered Bookie cut changed or is unresolved");
        }
        var rollback = new RollbackSafetyFacts(controls.localSuccess(), controls.routeOrInstall(),
                controls.activation(), controls.fenceOrGrant(), controls.tombstone(),
                arenas.stream().anyMatch(arena -> arena.controlThrough() >= 0), controls.durabilityUnknown());
        return new LocalCuts(cut, List.copyOf(arenas), rollback, applied);
    }

    IsolatedSegmentStorage.DeleteCatchUpAttempt followOwnedG1(OwnedListener original, PreflightEpisode episode,
            String domain, int maxEvents, long deadline, BooleanSupplier live) {
        IsolatedSegmentStorage.DeleteReplayTarget local;
        synchronized (gate) {
            if (ownedListener != original || startupPreflight != episode || !isLive()
                    || !original.admission.allows(bookieIdentity.bookieId().toString(), manifest.incarnation())) {
                return new IsolatedSegmentStorage.DeleteCatchUpAttempt(
                        IsolatedSegmentStorage.DeleteCatchUpStatus.UNAVAILABLE, null,
                        CompletableFuture.completedFuture(null));
            }
            local = deleteReplayTarget();
        }
        var nativeProbe = episode.retained().nextProbe(deadline);
        return IsolatedSegmentStorage.replayProfileDeletesUntil(local, episode.preflight().reservations,
                episode.metadata(), domain, maxEvents, deadline, live, nativeProbe);
    }

    /** Observe and apply the original retained G1 journal on the sole recovered Bookie control owner. */
    IsolatedSegmentStorage.DeleteCatchUpAttempt catchUpProfileDeletes(ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, String domain, int maxEvents, Duration timeout) {
        IsolatedSegmentStorage.requireDeleteReplayArguments(reservations, metadata, domain, maxEvents, timeout,
                this::controlAdmission);
        IsolatedSegmentStorage.DeleteReplayTarget local;
        synchronized (gate) {
            if (!isLive()) {
                return new IsolatedSegmentStorage.DeleteCatchUpAttempt(
                        IsolatedSegmentStorage.DeleteCatchUpStatus.UNAVAILABLE, null,
                        CompletableFuture.completedFuture(null));
            }
            local = deleteReplayTarget();
        }
        return IsolatedSegmentStorage.replayProfileDeletes(local, reservations, metadata, domain, maxEvents, timeout,
                this::controlAdmission);
    }

    private IsolatedSegmentStorage.DeleteReplayTarget deleteReplayTarget() {
        if (execution.onOwnerThread()) {
            throw new IllegalArgumentException("Manifest delete replay requires an external cold caller");
        }
        return new IsolatedSegmentStorage.DeleteReplayTarget(bookieIdentity, controls,
                () -> ProfileDeleteDelivery.Progress.fromEvent(profileDeleteProgress()), gate, this::isLive, true);
    }

    ProfileControlRequest.DeleteBinding profileDeleteProgress() {
        synchronized (gate) {
            if (!isLive()) {
                throw new IllegalStateException("Complete manifest graph is not available for delete replay");
            }
            return bookie.deleteAppliedCursor();
        }
    }

    @Override
    public SegmentProfileInstallStore profileInstallStore(Duration timeout) {
        return new SegmentProfileInstallStore(controls(), bookieIdentity, timeout);
    }

    /** One explicitly borrowed loopback listener. The borrower drains it before retiring this graph. */
    SegmentProfileListener listen(TLSContextFactory tls, SegmentProfileListener.Settings settings,
            InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
            SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(read, "read");
        if (address.getAddress() == null || !address.getAddress().isLoopbackAddress()) {
            throw new IllegalArgumentException("Manifest Profile listener requires a resolved loopback address");
        }
        synchronized (gate) {
            if (!isLive() || listenerBorrowed || coldPublication != null && !coldPublication.drained()) {
                throw new IllegalStateException("Manifest listener requires one complete live graph");
            }
            return bindListener(tls, settings, address, acceptor, worker, control, read);
        }
    }

    OwnedListener listenOwned(SegmentManifestOwnedService service, CheckedStartup checked,
            ZKProfileEphemeralRegistrationStore.Admission admission, SegmentProfileService.Transport transport,
            long deadline) throws IOException {
        requireLoopbackTransport(transport);
        synchronized (gate) {
            requireOriginalGraph(checked, deadline);
            if (!service.owns(this) || coldPublication != null || !admission.isPending()
                    || !admission.bookieId().equals(manifest.bookieId().toString())) {
                throw new IOException("Original manifest graph needs its pending owned admission");
            }
            var listener = bindListener(transport.tls(), transport.settings(), transport.address(),
                    transport.acceptor(), transport.worker(), transport.control(), transport.read());
            ownedListener = new OwnedListener(service, checked, admission, listener);
            return ownedListener;
        }
    }

    private SegmentProfileListener bindListener(TLSContextFactory tls, SegmentProfileListener.Settings settings,
            InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
            SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
        int base = SegmentProfileAddHandler.connectionBaseBytes(settings.connectionBaseBytes(),
                this.control.maxPendingEntries());
        listenerBorrowed = true;
        return new SegmentProfileListener(tls, budget, settings, address, acceptor, worker,
                base, new Connection(settings, control, read, base));
    }

    private final class Connection implements SegmentProfileListener.ConnectionInstaller {
        private final SegmentProfileListener.Settings listenerSettings;
        private final SegmentProfileControlConfig controlConfig;
        private final SegmentProfileReadConfig read;
        private final int base;

        private Connection(SegmentProfileListener.Settings listenerSettings, SegmentProfileControlConfig controlConfig,
                SegmentProfileReadConfig read, int base) {
            this.listenerSettings = listenerSettings;
            this.controlConfig = controlConfig;
            this.read = read;
            this.base = base;
        }

        @Override
        public void install(ChannelPipeline pipeline, TLSContextFactory tls) throws SecurityException {
            synchronized (gate) {
                if (!isLive() || !ProfileInboundPipeline.admitConnection(pipeline.channel(), budget,
                        listenerSettings.connectionBytes(), base)) {
                    pipeline.channel().close();
                    return;
                }
                boolean installed = false;
                try {
                    var hello = SegmentProfileAddHandler.hello(bookieIdentity, listenerSettings.readinessGeneration(),
                            true, false, read);
                    var handler = new SegmentProfileAddHandler(ManifestSegmentStorage.this, tls,
                            listenerSettings.trustDomain(), hello, maxEntryBytes, control.maxPendingEntries(),
                            controlConfig, read);
                    installed = ProfileInboundPipeline.install(pipeline, tls, budget,
                            listenerSettings.connectionBytes(), base, 100 + maxEntryBytes, 144 + maxEntryBytes,
                            handler, true, handler.frameMetadataBytes());
                } finally {
                    if (!installed) {
                        pipeline.channel().close();
                    }
                }
            }
        }
    }

    private int shard(ProfileLedgerContext context) {
        return SegmentOwnerLoop.shardFor(context.ledgerId(), writes.length);
    }

    private boolean accepts(int shard) {
        return isLive() && !freeing && !bookie.rotationRequired()
                && !members.get(manifest.arenaForShard(shard)).arena.rotationRequired();
    }

    private boolean isLive() {
        if (accepting && (!execution.accepting() || execution.hasFailedOwner() || !bookieFiles.healthy()
                || !bookie.matchesScope(control, bookieIdentity)
                || members.stream().anyMatch(member -> !member.owned.healthy() || !member.arena.manifestAccepting()))) {
            accepting = false;
            execution.stopAdmission();
        }
        return accepting;
    }

    private boolean controlAdmission() {
        synchronized (gate) {
            return isLive();
        }
    }

    @Override
    public boolean ownedStartupAllowsRequests() {
        synchronized (gate) {
            return isLive() && (ownedListener == null
                    || ownedListener.admission.hasCompletedFirstOpen(manifest.incarnation()));
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection submitProfileNormal(ProfileLedgerContext context, byte[] credential,
            long entryId, ByteBuf entry, SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            if (!isLive() || ownedListener != null && !ownedListener.admission.allows(
                    manifest.bookieId().toString(), manifest.incarnation())) {
                return SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
            }
            var rejection = bookie.normalRejection(context, credential);
            if (rejection != null) {
                return rejection;
            }
            IsolatedSegmentStorage.requireProfileCoordinate(context, entryId, entry, "Profile");
            if (submitNormal(context, credential, entry, request)) {
                return null;
            }
            rejection = bookie.normalRejection(context, credential);
            return rejection != null ? rejection : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection submitProfileRecovery(ProfileOperationCodec.AddRecovery request,
            ByteBuf entry, SegmentDataPhysicalDriver.RecoveryRequest response) throws IOException {
        synchronized (gate) {
            int shard = shard(request.context());
            return accepts(shard)
                    ? IsolatedSegmentStorage.submitLocalProfileRecovery(execution, bookie, writes[shard],
                            request, entry, response)
                    : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection profileFenceRejection(ProfileOperationCodec.FenceLedger request) {
        synchronized (gate) {
            return isLive() ? bookie.fenceRejection(request.context(), request.credential())
                    : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection submitProfileFence(ProfileOperationCodec.FenceLedger request,
            SegmentBookieControlDriver.FenceCompletion completion) {
        synchronized (gate) {
            var rejection = profileFenceRejection(request);
            if (rejection != null) {
                return rejection;
            }
            return controls.submitFence(request, completion)
                    ? null : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection profileLacRejection(ProfileOperationCodec.WriteLac request) {
        synchronized (gate) {
            return isLive() ? bookie.explicitLacRejection(request)
                    : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public SegmentBookieControlLog.NormalRejection submitProfileLac(ProfileOperationCodec.WriteLac request,
            SegmentBookieControlDriver.LacCompletion completion) {
        synchronized (gate) {
            var rejection = profileLacRejection(request);
            if (rejection != null) {
                return rejection;
            }
            return controls.submitExplicitLac(request, completion)
                    ? null : SegmentBookieControlLog.NormalRejection.UNAVAILABLE;
        }
    }

    @Override
    public boolean profileReadVisible(ProfileLedgerContext context, long entryId) {
        synchronized (gate) {
            return isLive() && bookie.networkReadable(context, entryId);
        }
    }

    @Override
    public boolean submitLacRead(ProfileLedgerContext context, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            int shard = shard(context);
            return accepts(shard) && reads[shard].submitLac(context, request);
        }
    }

    @Override
    public boolean submitLatestRead(ProfileLedgerContext context, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            int shard = shard(context);
            return accepts(shard) && reads[shard].submitLatest(context, request);
        }
    }

    boolean submitNormal(ProfileLedgerContext context, byte[] credential, ByteBuf entry,
            SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            int shard = shard(context);
            return accepts(shard) && IsolatedSegmentStorage.submitLocalNormal(execution, bookie, writes[shard],
                    context, credential, entry, request);
        }
    }

    boolean submitRecovery(ProfileOperationCodec.AddRecovery request,
            SegmentDataPhysicalDriver.RecoveryRequest response) throws IOException {
        synchronized (gate) {
            Objects.requireNonNull(response, "response");
            int shard = shard(request.context());
            if (!accepts(shard)) {
                return false;
            }
            var admission = bookie.admitRecovery(request).orElse(null);
            if (admission == null) {
                return false;
            }
            try {
                return writes[shard].submitRecovery(admission, response);
            } catch (IOException | RuntimeException | Error failure) {
                admission.releaseUnclaimed();
                throw failure;
            }
        }
    }

    @Override
    public boolean submitRead(ProfileLedgerContext context, long entryId, SegmentPointRead.ReadRequest request) {
        synchronized (gate) {
            int shard = shard(context);
            return accepts(shard) && reads[shard].submit(context, entryId, request);
        }
    }

    boolean reclaimAllocation(int arena, UUID operation, long allocationId,
            IsolatedSegmentStorage.FreeRequest request) {
        synchronized (gate) {
            Member member = members.get(arena);
            if (!isLive() || freeing || bookie.rotationRequired()
                    || member.arena.rotationRequired()) {
                return false;
            }
            var free = new IsolatedSegmentStorage.FreeOperation(Objects.requireNonNull(operation), allocationId,
                    Objects.requireNonNull(request), execution, member.arena, member.files, bookie, member.allocator,
                    () -> {
                        synchronized (gate) {
                            freeing = false;
                        }
                    });
            freeing = true;
            try {
                if (execution.owner(0).submit(0, free)) {
                    return true;
                }
            } catch (RuntimeException | Error failure) {
                freeing = false;
                throw failure;
            }
            freeing = false;
            return false;
        }
    }

    boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Positive manifest drain timeout required");
        }
        long nanos = timeout.toNanos();
        var group = execution;
        if (group != null && group.onOwnerThread()) {
            throw new IllegalStateException("Manifest drain requires an external thread");
        }
        SegmentManifestColdPublication publication;
        synchronized (gate) {
            accepting = false;
            stopping = true;
            publication = coldPublication;
            var active = execution;
            if (active != null) {
                active.stopAdmission();
            }
        }
        if (publication != null) {
            publication.retire();
            if (!publication.drained()) {
                return false;
            }
        }
        synchronized (lifecycle) {
            if (observingAccess || observingRecoveryDependencies
                    || accessProbe != null && !accessProbe.closeWhenDrained()) {
                return false;
            }
            accessProbe = null;
            if (!cleanupAttempted) {
                if (!drainExecution(nanos)) {
                    return false;
                }
                for (Member member : members) {
                    closeMember(member);
                }
                closeBookie();
                cleanupAttempted = true;
                if (cleanupFailure == null) {
                    Arrays.fill(writes, null);
                    Arrays.fill(reads, null);
                    Arrays.fill(hot, null);
                    execution = null;
                    controls = null;
                    bookie = null;
                    bookieGuard = null;
                    for (Member member : members) {
                        member.files = null;
                        member.index = null;
                        member.arena = null;
                        member.allocator = null;
                        member.controlGuard = null;
                        member.dataGuard = null;
                    }
                    closeResource(dataCredit);
                    closeResource(controlCredit);
                }
            }
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
            return true;
        }
    }

    private boolean drainExecution(long nanos) throws IOException, InterruptedException {
        if (execution == null) {
            return true;
        }
        try {
            if (!execution.shutdown(Duration.ofNanos(nanos))) {
                return false;
            }
        } catch (IOException failure) {
            if (!execution.terminated() || execution.inputs().usedBytes() != 0) {
                throw failure;
            }
            retainFailure(failure);
        }
        for (SegmentHotDirectory directory : hot) {
            if (directory != null) {
                directory.retireClosedState(execution);
            }
        }
        return true;
    }

    private void closeMember(Member member) throws InterruptedException {
        closeResource(member.files);
        if (member.files != null && !member.files.closedForDrain()) {
            return;
        }
        boolean indexClosed = closeResource(member.index);
        boolean arenaClosed = closeResource(member.arena);
        boolean filesClosed = closeFiles(member.owned);
        if (indexClosed && arenaClosed && filesClosed) {
            closeResource(member.controlGuard);
            closeResource(member.dataGuard);
            if (member.files != null) {
                member.files.retireClosedInventory();
            }
        }
    }

    private void closeBookie() throws InterruptedException {
        if (bookie != null && !closeResource(bookie::retireOwned)) {
            return;
        }
        if (closeFiles(bookieFiles)) {
            closeResource(bookieGuard);
            if (bookie != null) {
                bookie.releaseClosedState();
            }
        }
    }

    private boolean closeFiles(SegmentOwnedFiles files) throws InterruptedException {
        var result = files.drain();
        if (!result.drained()) {
            retainFailure(new IOException("Manifest native file release is unresolved", result.closeFailure()));
        }
        return result.drained();
    }

    private boolean closeResource(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception failure) {
                retainFailure(failure);
                return false;
            }
        }
        return true;
    }

    private void retainFailure(Throwable failure) {
        if (cleanupFailure == null) {
            cleanupFailure = new IOException("Manifest storage cleanup failed", failure);
        } else if (failure != cleanupFailure && failure != cleanupFailure.getCause()) {
            cleanupFailure.addSuppressed(failure);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(5))) {
                throw new IOException("Manifest storage is still draining");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Manifest storage drain interrupted", interrupted);
        }
    }
}

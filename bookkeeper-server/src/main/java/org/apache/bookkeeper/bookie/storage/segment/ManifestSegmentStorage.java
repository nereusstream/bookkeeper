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
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.proto.profile.ProfileInboundPipeline;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Local Profile-only graph. The manifest fixes routing independently of execution placement. */
final class ManifestSegmentStorage implements AutoCloseable, SegmentProfileAddHandler.DataOperations {
    record Capacity(long dataBytes, long controlBytes) { }

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
    private boolean stopping;
    private boolean freeing;
    private boolean cleanupAttempted;
    private IOException cleanupFailure;
    private boolean listenerBorrowed;

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
        start(false);
    }

    void restart() throws IOException, InterruptedException {
        start(true);
    }

    private void start(boolean existing) throws IOException, InterruptedException {
        synchronized (lifecycle) {
            synchronized (gate) {
                if (attempted || stopping) {
                    throw new IllegalStateException("Manifest storage startup is one-use");
                }
                attempted = true;
            }
            try {
                reserveCapacity();
                requireRoots(existing);
                acquireGuards(existing);
                long bookieCut = -1;
                if (existing) {
                    bookieCut = preflightRestart();
                } else {
                    SegmentBookieControlLog.formatManifestControl(control, bookieIdentity, bookieGuard, bookieFiles,
                            new SegmentFileIo());
                }
                bookie = SegmentBookieControlLog.openManifestWhileLocked(control, bookieIdentity,
                        limits.controlLogBytes(), key, validator, new SegmentFileIo(), bookieGuard, bookieFiles);
                if (existing && bookie.appliedThrough() != bookieCut) {
                    throw new IOException("Shared Bookie cut changed after manifest preflight");
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
        Capacity capacity = capacity(manifest, control, settings, limits);
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

    private long preflightRestart() throws IOException {
        long bookieCut = SegmentBookieControlLog.inspectManifestWhileLocked(control, bookieIdentity,
                limits.controlLogBytes(), key, validator, bookieGuard, bookieFiles);
        for (Member member : members) {
            member.dataGuard.requireHeld(member.config.directory().resolve("isolated.lock"));
            try (var arena = SegmentArenaControlLog.openManifestWhileLocked(member.config, member.identity,
                    limits.controlLogBytes(), new SegmentFileIo(), member.controlGuard, member.owned, true)) {
                arena.requireHealthyManifestRestart(bookieCut);
            }
            SegmentDerivedIndex.requireExistingScope(member.config, member.identity);
            for (int slot = 0; slot < 2; slot++) {
                Path coverage = SegmentDerivedIndex.publication(member.config, slot);
                if (Files.exists(coverage, LinkOption.NOFOLLOW_LINKS)) {
                    for (Path lock : lockPaths) {
                        if (Files.isSameFile(coverage, lock)) {
                            throw new IOException("Index coverage aliases a manifest lock");
                        }
                    }
                }
            }
        }
        return bookieCut;
    }

    private void prepareMember(Member member, boolean existing) throws IOException {
        if (!existing) {
            member.files = IsolatedSegmentFiles.createWhileLocked(member.config, member.identity,
                    new SegmentFileIo(), member.dataGuard, member.owned);
        }
        member.arena = SegmentArenaControlLog.openManifestWhileLocked(member.config, member.identity,
                limits.controlLogBytes(), new SegmentFileIo(), member.controlGuard, member.owned, false);
        member.index = SegmentDerivedIndex.open(member.config, member.identity, limits.indexedEntries(), member.owned);
        if (existing) {
            member.files = IsolatedSegmentFiles.prepareLocalResults(member.config, member.identity, member.arena,
                    bookie, new SegmentFileIo(), limits.indexedEntries(), writes.length, member.index,
                    member.dataGuard, member.owned);
        }
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
            if (execution.onOwnerThread()) {
                throw new IllegalArgumentException("Manifest delete replay requires an external cold caller");
            }
            local = new IsolatedSegmentStorage.DeleteReplayTarget(bookieIdentity, controls,
                    () -> ProfileDeleteDelivery.Progress.fromEvent(profileDeleteProgress()), gate, this::isLive, true);
        }
        return IsolatedSegmentStorage.replayProfileDeletes(local, reservations, metadata, domain, maxEvents, timeout,
                this::controlAdmission);
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
            if (!isLive() || listenerBorrowed) {
                throw new IllegalStateException("Manifest listener requires one complete live graph");
            }
            int base = SegmentProfileAddHandler.connectionBaseBytes(settings.connectionBaseBytes(),
                    this.control.maxPendingEntries());
            listenerBorrowed = true;
            return new SegmentProfileListener(tls, budget, settings, address, acceptor, worker,
                    base, new Connection(settings, control, read, base));
        }
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
        return controlAdmission();
    }

    @Override
    public SegmentBookieControlLog.NormalRejection submitProfileNormal(ProfileLedgerContext context, byte[] credential,
            long entryId, ByteBuf entry, SegmentDataPhysicalDriver.NormalRequest request) throws IOException {
        synchronized (gate) {
            if (!isLive()) {
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
        synchronized (gate) {
            accepting = false;
            stopping = true;
            var active = execution;
            if (active != null) {
                active.stopAdmission();
            }
        }
        synchronized (lifecycle) {
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

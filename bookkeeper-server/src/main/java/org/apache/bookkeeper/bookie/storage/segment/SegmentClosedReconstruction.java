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
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstructionSources;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedReady;
import org.apache.bookkeeper.discover.profile.ProfileManifestPreparation;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;

/** Controlled offline recovery of the complete original CLOSED DATA file set. */
public final class SegmentClosedReconstruction {
    static final int COPY_BYTES = 64 * 1024;
    private static final String CANDIDATE = "image";
    private static final String INTERRUPTED = "interrupted-image";
    private static final String RETAINED = "retained-old";
    private static final String FAILED = "image-failed";

    record Limits(long controlBytes, int controlRows, long dataBytes, int entries, Duration sourceTimeout) { }

    record Sources(ClientContext sdk, EventLoopGroup eventLoop, SslContext tls, ThreadPoolExecutor cold,
                   ZKProfileRegistrationStore registry) { }

    record ManifestClosedMember(ProfileLedgerContext ledger, ManifestSegmentStorage.LedgerRoute route,
            ProfileClosedReconstruction.Kind kind, long lastEntry, long length, String closedDigest) { }

    record ManifestClosedObservation(ManifestSegmentStorage.RecoveryDependencies dependencies,
            List<ManifestClosedMember> members) { }

    record ManifestClosedCohort(ZKProfileReservationStore.ClosedReconstructionReader reader,
            List<ManifestClosedMember> members) {
        void revalidate(ManifestSegmentStorage.RecoveryDependencies graph) throws IOException, InterruptedException {
            for (int i = 0; i < members.size(); i++) {
                readVerified(graph, i);
            }
        }

        ZKProfileReservationStore.ClosedReconstructionObservation readVerified(
                ManifestSegmentStorage.RecoveryDependencies graph, int index) throws IOException, InterruptedException {
            var ledger = graph.bookie().ledgers().get(index);
            var observed = reader.read(ledger.context());
            if (!members.get(index).equals(manifestClosedMember(graph, ledger, observed))) {
                throw new IOException("Native CLOSED evidence changed during manifest cohort observation");
            }
            return observed;
        }
    }

    static long manifestClosedControlBytes() throws IOException {
        var layout = SegmentControlCapacity.Layout.hotSpot();
        long member = SegmentControlCapacity.sum(layout.object(ManifestClosedMember.class), layout.string(64));
        int members = ProfileClosedReconstruction.MAX_MEMBERS;
        // Only compact members survive a native read. The original ZK reader owns its per-call workspace.
        return SegmentControlCapacity.sum(layout.object(ManifestClosedObservation.class),
                SegmentControlCapacity.times(members + 1L, member),
                layout.list(), layout.refs(2L * members), layout.immutableList(members));
    }

    static ManifestClosedObservation observeManifestClosedCohort(ManifestSegmentStorage owner, int controlRows,
            Duration timeout, ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations)
            throws Exception {
        long deadline = manifestClosedDeadline(timeout);
        return owner.withRecoveryDependenciesUntil(controlRows, deadline, manifestClosedControlBytes(), graph -> {
            var cohort = readManifestClosedCohort(graph, metadata, reservations, deadline);
            cohort.revalidate(graph);
            return new ManifestClosedObservation(graph, cohort.members());
        });
    }

    static long manifestClosedDeadline(Duration timeout) {
        long nanos = timeout.toNanos();
        if (nanos <= 0 || nanos > TimeUnit.MINUTES.toNanos(1)) {
            throw new IllegalArgumentException("Manifest CLOSED observation requires a bounded deadline");
        }
        return System.nanoTime() + nanos;
    }

    static ManifestClosedCohort readManifestClosedCohort(ManifestSegmentStorage.RecoveryDependencies graph,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, long deadline)
            throws IOException, InterruptedException {
        var target = graph.bookieIdentity();
        return readManifestClosedCohort(graph, reservations.closedReconstructionReader(metadata,
                target.bookieId(), target.incarnation(), deadline));
    }

    static ManifestClosedCohort readManifestClosedCohort(ManifestSegmentStorage.RecoveryDependencies graph,
            ZKProfileReservationStore.ClosedReconstructionReader reader) throws IOException, InterruptedException {
        var bookie = graph.bookie();
        SegmentStorageInspector.requireClosedBookieHistory(bookie);
        var ledgers = bookie.ledgers();
        if (ledgers.isEmpty() || ledgers.size() > ProfileClosedReconstruction.MAX_MEMBERS) {
            throw new IOException("Manifest CLOSED observation requires a complete bounded Bookie cohort");
        }
        var members = new ArrayList<ManifestClosedMember>(ledgers.size());
        for (var ledger : ledgers) {
            members.add(manifestClosedMember(graph, ledger, reader.read(ledger.context())));
        }
        long nonempty = members.stream()
                .filter(member -> member.kind() == ProfileClosedReconstruction.Kind.NONEMPTY_SELF).count();
        SegmentStorageInspector.requireClosedBookieCohort(bookie, members.size(), nonempty);
        return new ManifestClosedCohort(reader, List.copyOf(members));
    }

    private static ManifestClosedMember manifestClosedMember(ManifestSegmentStorage.RecoveryDependencies graph,
            SegmentBookieControlLog.ObservedLedger ledger,
            ZKProfileReservationStore.ClosedReconstructionObservation observed) throws IOException {
        var proof = observed.proof();
        SegmentStorageInspector.requireClosedBookieMember(ledger, proof);
        var value = observed.snapshot().closed().metadata().metadata().getValue();
        return new ManifestClosedMember(ledger.context(), graph.ledgerRoutes().get(ledger.context()),
                ProfileClosedReconstruction.kind(proof), value.getLastEntryId(), value.getLength(),
                ProfileClosedReconstruction.closedDigest(observed.snapshot(), proof));
    }

    record PublisherExit(long pid, long startMillis, long session) { }

    /** Captured by the controlled launcher while its original process and registration are both live. */
    static final class OldPublisher {
        private final SegmentFileHeader identity;
        private final ProcessHandle process;
        private final long startMillis;
        private final long session;
        private final Process manifestChild;
        private final ZKProfileRegistrationStore manifestRegistry;

        OldPublisher(SegmentFileHeader identity, ProcessHandle process, ZKProfileRegistrationStore.RegisteredPeer peer)
                throws IOException {
            this(identity, process, peer, null, null);
        }

        private OldPublisher(SegmentFileHeader identity, ProcessHandle process,
                ZKProfileRegistrationStore.RegisteredPeer peer, Process manifestChild,
                ZKProfileRegistrationStore manifestRegistry) throws IOException {
            if (!process.isAlive() || !identity.bookieId().toString().equals(peer.projection().bookieId())
                    || !identity.incarnation().equals(peer.projection().storageIncarnation())
                    || peer.publisherSession() == 0) {
                throw new IOException("Controlled launcher did not observe its original publisher episode");
            }
            this.identity = identity;
            this.process = process;
            startMillis = process.info().startInstant().orElseThrow(
                    () -> new IOException("Controlled publisher process birth is unavailable")).toEpochMilli();
            session = peer.publisherSession();
            this.manifestChild = manifestChild;
            this.manifestRegistry = manifestRegistry;
        }

        static OldPublisher captureManifest(SegmentArenaManifest manifest, Process originalChild,
                ZKProfileRegistrationStore registry, ZKProfileRegistrationStore.RegisteredPeer peer)
                throws IOException {
            var original = new OldPublisher(SegmentFileHeader.forBookie(manifest), originalChild.toHandle(),
                    peer, originalChild, registry);
            if (!manifest.identity().equals(peer.readiness().deviceManifestIdentity())
                    || registry.revalidatePeer(peer).status() != ReadStatus.FOUND
                    || !originalChild.isAlive() || !original.process.isAlive()) {
                throw new IOException("Controlled manifest publisher registration or live process changed");
            }
            return original;
        }

        PublisherExit requireManifestExit(SegmentFileHeader expected, ZKProfileRegistrationStore registry,
                long deadline) throws IOException, TimeoutException {
            if (manifestChild == null || manifestRegistry != registry || !identity.equals(expected)) {
                throw new IOException("Original publisher witness belongs to another manifest or registry");
            }
            if (manifestChild.isAlive() || process.isAlive()) {
                throw new IOException("Original controlled manifest publisher has not exited");
            }
            ProfileClosedReconstructionSources.requireOffline(registry, expected.bookieId(), deadline);
            return new PublisherExit(process.pid(), startMillis, session);
        }
    }

    static long manifestSourceControlBytes() throws IOException {
        var layout = SegmentControlCapacity.Layout.hotSpot();
        int hints = ProfileClosedReconstructionSources.MAX_RETAINED_CAPABILITY_HINTS_PER_PEER;
        // A closed pair retains two original peers. Each peer keeps readiness and ephemeral hint lists.
        long peer = SegmentControlCapacity.sum(layout.object(ZKProfileRegistrationStore.RegisteredPeer.class),
                layout.object(ProfileServiceInfoProjection.class), layout.object(ProfileReadinessRecord.class),
                3 * layout.string(255), 2 * layout.immutableList(hints / 2),
                SegmentControlCapacity.times(hints, layout.object(Integer.class)), 16 * layout.captured(16));
        long row = SegmentControlCapacity.sum(2 * peer, layout.object(ProfileClosedReconstructionSources.class),
                layout.object(ProfileClosedReconstruction.Member.class), layout.string(64),
                3 * layout.hashMap(), 4 * layout.hashEntry(), 3 * layout.list());
        return SegmentControlCapacity.sum(
                SegmentControlCapacity.times(ProfileClosedReconstruction.MAX_MEMBERS + 1L, row),
                layout.object(PublisherExit.class),
                layout.hashMap(), layout.list());
    }

    static PreparedManifestAdmission prepareManifestAdmission(ManifestSegmentStorage.AuditedClosedGraph audit,
            ZKProfileReservationStore.ClosedReconstructionReader reader, ZKProfileRegistrationStore registry,
            OldPublisher publisher, NativeIO.FileIdentity bookieRoot, UUID operation)
            throws IOException {
        var observation = audit.observation;
        var graph = observation.data().closed().dependencies();
        if (graph.arenas().stream().noneMatch(arena ->
                arena.control().cut().sessionOpen() && !arena.control().grants().isEmpty())) {
            throw new IOException("Manifest CLOSED admission requires an original OPEN Arena with current grants");
        }
        var epochs = new ArrayList<ProfileManifestClosedAdmission.ArenaEpoch>(graph.arenas().size());
        var arenas = new ArrayList<ProfileManifestClosedAdmission.Arena>(graph.arenas().size());
        for (int i = 0; i < graph.arenas().size(); i++) {
            var arena = graph.arenas().get(i);
            var control = arena.control();
            var cut = control.cut();
            var image = observation.data().arenas().get(i);
            epochs.add(new ProfileManifestClosedAdmission.ArenaEpoch(arena.identity().arenaId(), cut.sessionEpoch()));
            var point = new ProfileManifestClosedAdmission.ControlPoint(image.rootObject(), cut.appliedSequence(),
                    cut.physicalGeneration(), cut.checkpointGeneration(), control.historyDigest());
            arenas.add(new ProfileManifestClosedAdmission.Arena(point, cut.sessionOpen(), control.mode(),
                    image.files()));
        }
        var ledgers = new ArrayList<ProfileManifestClosedAdmission.Ledger>(observation.sources().size());
        for (int i = 0; i < observation.sources().size(); i++) {
            var member = observation.data().closed().members().get(i);
            ledgers.add(new ProfileManifestClosedAdmission.Ledger(observation.sources().get(i), member.route().shard(),
                    member.route().arena(), member.lastEntry(), member.length()));
        }
        var target = graph.bookieIdentity();
        var scope = new ProfileManifestClosedAdmission.Scope(target.bookieId().toString(), target.incarnation(),
                graph.manifest(), epochs);
        var bookie = graph.bookie();
        var point = new ProfileManifestClosedAdmission.ControlPoint(bookieRoot, bookie.observedSequence(),
                bookie.physicalGeneration(), bookie.checkpointGeneration(), bookie.historyDigest());
        var exited = observation.publisher();
        var request = new ProfileManifestClosedAdmission(scope, operation, exited.pid(), exited.startMillis(),
                exited.session(), point, arenas, ledgers);
        return new PreparedManifestAdmission(request, reader, audit.owner, target, publisher, registry,
                audit.registrations);
    }

    /** Only the locked manifest audit can prepare the original graph and its registered source observations. */
    public static final class PreparedManifestAdmission {
        private final ProfileManifestClosedAdmission request;
        private final ZKProfileReservationStore.ClosedReconstructionReader reader;
        private final ManifestSegmentStorage owner;
        private final SegmentFileHeader target;
        private final OldPublisher publisher;
        private final ZKProfileRegistrationStore registry;
        private final Map<ProfileClosedReconstructionSources, String> registrations;
        private ZKProfileReservationStore.ManifestAdmissionMutation preparationMutation;

        private PreparedManifestAdmission(ProfileManifestClosedAdmission request,
                ZKProfileReservationStore.ClosedReconstructionReader reader, ManifestSegmentStorage owner,
                SegmentFileHeader target, OldPublisher publisher, ZKProfileRegistrationStore registry,
                Map<ProfileClosedReconstructionSources, String> registrations) {
            this.request = request;
            this.reader = reader;
            this.owner = owner;
            this.target = target;
            this.publisher = publisher;
            this.registry = registry;
            this.registrations = Map.copyOf(registrations);
        }

        public ProfileManifestClosedAdmission request() {
            return request;
        }

        public ZKProfileReservationStore.ClosedReconstructionReader reader() {
            return reader;
        }

        public Set<ProfileClosedReconstructionSources> sources() {
            return registrations.keySet();
        }

        public void revalidateRegistrations() throws IOException, TimeoutException {
            verifySourceRegistrations(registrations);
            publisher.requireManifestExit(target, registry, reader.deadline());
        }

        public void beginMutation(ZKProfileReservationStore.ManifestAdmissionMutation mutation)
                throws IOException, InterruptedException {
            owner.beginManifestAdmission(mutation, null, reader.deadline());
        }

        public void beginPreparationMutation(ZKProfileReservationStore.ManifestAdmissionMutation mutation)
                throws IOException, InterruptedException {
            beginMutation(mutation);
            preparationMutation = mutation;
        }

        public void requireOwnerLive() throws IOException, InterruptedException {
            owner.requireManifestAdmissionLive(reader.deadline());
        }
    }

    static PreparedManifestReady prepareManifestReady(ManifestSegmentStorage.LockedManifestImages candidates) {
        var preparation = candidates.attempt;
        var attempt = preparation.attempt();
        var ready = new ProfileManifestClosedReady(attempt.admissionBirth(), attempt.admissionDigest(),
                attempt.operation(), preparation.birth(), preparation.version(),
                attempt.firstNonce(), attempt.nonce(), candidates.images.stream().map(arena ->
                        new ProfileManifestClosedReady.Arena(arena.stageObject(), arena.files())).toList());
        return new PreparedManifestReady(candidates.admission, preparation, ready);
    }

    /** Private proof from this owner's actual copy, first close, synchronization and final verification. */
    public static final class PreparedManifestReady {
        private final PreparedManifestAdmission admission;
        private final ProfileManifestPreparation.Receipt preparation;
        private final ProfileManifestClosedReady ready;
        private final ZKProfileReservationStore.ManifestAdmissionMutation claim;

        private PreparedManifestReady(PreparedManifestAdmission admission,
                ProfileManifestPreparation.Receipt preparation,
                ProfileManifestClosedReady ready) {
            this.admission = admission;
            this.preparation = preparation;
            this.ready = ready;
            claim = Objects.requireNonNull(admission.preparationMutation, "original preparation mutation");
        }

        public PreparedManifestAdmission admission() {
            return admission;
        }

        public ProfileManifestPreparation.Receipt preparation() {
            return preparation;
        }

        public ProfileManifestClosedReady ready() {
            return ready;
        }

        public void beginMutation(ZKProfileReservationStore.ManifestAdmissionMutation mutation)
                throws IOException, InterruptedException {
            admission.owner.beginManifestAdmission(mutation, claim, admission.reader.deadline());
        }
    }

    /** Opaque input from the actual source and original locked storage audit, never a caller-supplied digest. */
    public static final class PreparedAdmission {
        private final ProfileClosedReconstruction.Admission request;
        private final List<ZKProfileReservationStore.ReadOnlySnapshot> expected;

        private PreparedAdmission(ProfileClosedReconstruction.Admission request,
                List<ZKProfileReservationStore.ReadOnlySnapshot> expected) {
            this.request = request;
            this.expected = expected;
        }

        public ProfileClosedReconstruction.Admission request() {
            return request;
        }

        public List<ZKProfileReservationStore.ReadOnlySnapshot> expected() {
            return expected;
        }
    }

    /** Opaque image whose original first close and all required staging barriers returned successfully. */
    public static final class PreparedReady {
        private final ProfileClosedReconstruction.Receipt admitted;
        private final List<ZKProfileReservationStore.ReadOnlySnapshot> expected;
        private final List<ProfileClosedReconstruction.FileImage> candidates;
        private final ProfileClosedReconstruction.Shadow shadow;

        private PreparedReady(ProfileClosedReconstruction.Receipt admitted,
                List<ZKProfileReservationStore.ReadOnlySnapshot> expected,
                List<ProfileClosedReconstruction.FileImage> candidates,
                ProfileClosedReconstruction.Shadow shadow) {
            this.admitted = admitted;
            this.expected = expected;
            this.candidates = List.copyOf(candidates);
            this.shadow = shadow;
        }

        public ProfileClosedReconstruction.Receipt admitted() {
            return admitted;
        }

        public List<ZKProfileReservationStore.ReadOnlySnapshot> expected() {
            return expected;
        }

        public List<ProfileClosedReconstruction.FileImage> candidates() {
            return candidates;
        }

        public ProfileClosedReconstruction.Shadow shadow() {
            return shadow;
        }
    }

    /** Issued only under the original guards after the READY image is active and staging has been removed. */
    static final class PublishedImage {
        private final ProfileClosedReconstruction.Receipt receipt;

        private PublishedImage(ProfileClosedReconstruction.Receipt receipt) {
            this.receipt = receipt;
        }

        ProfileClosedReconstruction.Receipt receipt() {
            return receipt;
        }
    }

    static ProfileClosedReconstruction.Receipt admit(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            ZKProfileReservationStore.ReadOnlySnapshot expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation) throws Exception {
        return admitCohort(config, identity, key, validator, nativeIo, limits, metadata, reservations, sources,
                List.of(expected), stoppedPublisher, epoch, operation);
    }

    static ProfileClosedReconstruction.Receipt admitCohort(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            List<ZKProfileReservationStore.ReadOnlySnapshot> expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation) throws Exception {
        return run(config, identity, key, validator, nativeIo, limits, metadata, reservations, sources,
                expected, stoppedPublisher, epoch, operation, null, null);
    }

    static ProfileClosedReconstruction.Receipt reconstruct(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            ZKProfileReservationStore.ReadOnlySnapshot expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation, SegmentFileIo imageIo, SegmentFileIo controlIo) throws Exception {
        return reconstructCohort(config, identity, key, validator, nativeIo, limits, metadata, reservations, sources,
                expected == null ? List.of() : List.of(expected), stoppedPublisher, epoch, operation, imageIo,
                controlIo);
    }

    static ProfileClosedReconstruction.Receipt reconstructCohort(IsolatedSegmentConfig config,
            SegmentFileHeader identity, SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo,
            Limits limits, ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            List<ZKProfileReservationStore.ReadOnlySnapshot> expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation, SegmentFileIo imageIo, SegmentFileIo controlIo) throws Exception {
        return run(config, identity, key, validator, nativeIo, limits, metadata, reservations, sources,
                expected, stoppedPublisher, epoch, operation, java.util.Objects.requireNonNull(imageIo),
                java.util.Objects.requireNonNull(controlIo));
    }

    private static ProfileClosedReconstruction.Receipt run(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            List<ZKProfileReservationStore.ReadOnlySnapshot> expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation, SegmentFileIo imageIo, SegmentFileIo controlIo) throws Exception {
        if (operation == null || operation.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("Reconstruction requires an exact operation");
        }
        var scope = new ProfileClosedReconstruction.Scope(identity.bookieId().toString(), identity.incarnation(),
                identity.arenaId(), epoch);
        return SegmentStorageInspector.withRecoveryLocks(config, identity, (root, data, arena, bookie, locks) -> {
            var existing = reservations.readClosedReconstruction(scope);
            if (existing != null && !existing.admission().operation().equals(operation)) {
                throw new IOException("The original managed epoch already has a different reconstruction operation");
            }
            if (existing != null) {
                requireFileGeometry(config, existing.admission().originals());
            }
            if (existing != null && existing.ready() != null) {
                if (imageIo != null) {
                    finishReady(config, identity, key, validator, nativeIo, limits, root, data, arena, bookie, locks,
                            existing, controlIo);
                }
                return existing;
            }
            if (existing == null && (stoppedPublisher == null || !identity.equals(stoppedPublisher.identity)
                    || stoppedPublisher.process.isAlive())) {
                throw new IOException("CLOSED reconstruction requires the controlled old publisher to have exited");
            }
            var deps = SegmentStorageInspector.readRecoveryDependencies(config, identity, limits.controlBytes(),
                    limits.controlRows(), key, validator, arena, bookie, locks);
            if (deps.arena().cut().sessionEpoch() != epoch) {
                throw new IOException("CLOSED reconstruction requires the original OPEN epoch");
            }
            if (expected.isEmpty() || expected.size() > ProfileClosedReconstruction.MAX_MEMBERS
                    || expected.size() != deps.bookie().ledgers().size()) {
                throw new IOException("Reconstruction requires the complete bounded installed ledger cohort");
            }
            var cohort = expected.stream()
                    .sorted(Comparator.comparingLong(value -> value.context().ledger().ledgerId())).toList();
            var proofs = new ArrayList<ZKProfileReservationStore.ClosedReconstructionProof>(cohort.size());
            var imageMembers = new ArrayList<SegmentDataRecovery.ClosedImageMember>(cohort.size());
            long previousLedger = -1;
            for (var member : cohort) {
                var ledger = member.context().ledger();
                if (ledger.ledgerId() == previousLedger) {
                    throw new IOException("Reconstruction cohort repeats a ledger ID");
                }
                previousLedger = ledger.ledgerId();
                var closed = member.closed().metadata().metadata().getValue();
                proofs.add(reservations.readClosedReconstructionProof(metadata, member, scope.bookie(),
                        scope.incarnation().value()));
                imageMembers.add(new SegmentDataRecovery.ClosedImageMember(ledger, closed.getLastEntryId(),
                        closed.getLength()));
            }
            SegmentStorageInspector.requireClosedReconstructionScope(deps, proofs);
            var historyMode = deps.arena().mode();
            var files = new SegmentOwnedFiles(locks);
            try (AutoCloseable cleanup = () -> requireDrained(files, data)) {
                var rootObject = observeDirectory(files, nativeIo, root);
                var audit = new SegmentDataRecovery.ClosedImageAudit(config, identity, deps.arena(), imageMembers,
                        limits.dataBytes(), limits.entries());
                var originals = auditOriginals(config, root, rootObject, files, nativeIo, audit);
                var imageEntries = audit.finish();
                Duration timeout = limits.sourceTimeout();
                if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
                    throw new IllegalArgumentException("CLOSED source audit requires a bounded deadline");
                }
                long sourceDeadline = System.nanoTime() + timeout.toNanos();
                var observations = new LinkedHashMap<ProfileClosedReconstructionSources, String>();
                var members = new ArrayList<ProfileClosedReconstruction.Member>(cohort.size());
                for (int index = 0; index < cohort.size(); index++) {
                    var member = cohort.get(index);
                    var context = member.context().ledger();
                    var proof = proofs.get(index);
                    var kind = ProfileClosedReconstruction.kind(proof);
                    String closedDigest = ProfileClosedReconstruction.closedDigest(member, proof);
                    if (kind == ProfileClosedReconstruction.Kind.EMPTY_TERMINAL) {
                        members.add(new ProfileClosedReconstruction.Member(context.ledgerId(), context, closedDigest,
                                ProfileClosedReconstruction.emptySourceDigest(closedDigest), kind));
                        continue;
                    }
                    try (var peers = ProfileClosedReconstructionSources.open(sources.sdk(), sources.eventLoop(),
                            sources.tls(), sources.registry(), sources.cold(), member, identity.bookieId(),
                            config.maxPayloadBytes() + SegmentBatchFormat.ENTRY_OVERHEAD, sourceDeadline)) {
                        var entries = imageEntries.get(context);
                        for (int entry = 0; entry < entries.size(); entry++) {
                            var local = entries.get(entry);
                            peers.verify(entry, local.cumulativeLength(), local.payloadLength(), local.payloadSha256());
                        }
                        String sourceDigest = peers.revalidate();
                        members.add(new ProfileClosedReconstruction.Member(context.ledgerId(),
                                historyMode == ProfileClosedReconstruction.HistoryMode.ORIGINAL
                                        && cohort.size() == 1 && config.dataFiles() == 1 ? null : context,
                                closedDigest, sourceDigest));
                        observations.put(peers, sourceDigest);
                    }
                }
                var current = SegmentStorageInspector.readRecoveryDependencies(config, identity,
                        limits.controlBytes(), limits.controlRows(), key, validator, arena, bookie, locks);
                if (!deps.equals(current)) {
                    throw new IOException("Original scope changed before CLOSED reconstruction admission");
                }
                verifyOriginalPaths(root, originals);
                var stage = staging(root, operation);
                if (existing == null) {
                    Files.createDirectory(stage, PosixFilePermissions.asFileAttribute(
                            PosixFilePermissions.fromString("rwx------")));
                    SegmentOwnedFiles.syncDirectory(files, stage);
                    SegmentOwnedFiles.syncDirectory(files, root);
                }
                var stageObject = observeDirectory(files, nativeIo, stage);
                if (!Files.getPosixFilePermissions(stage, LinkOption.NOFOLLOW_LINKS)
                        .equals(PosixFilePermissions.fromString("rwx------"))
                        || !Files.getOwner(stage, LinkOption.NOFOLLOW_LINKS).equals(
                                Files.getOwner(root, LinkOption.NOFOLLOW_LINKS))
                        || !observeDirectory(files, nativeIo, root).equals(rootObject)) {
                    throw new IOException("Reconstruction staging does not belong to the original scope owner");
                }
                if (existing == null) {
                    try (var children = files.openDirectory(stage)) {
                        if (children.iterator().hasNext()) {
                            throw new IOException("Reconstruction admission requires an empty owned staging");
                        }
                    }
                }
                var request = new ProfileClosedReconstruction.Admission(scope, operation,
                        deps.arena().cut().appliedSequence(), deps.bookie().observedSequence(),
                        existing == null ? stoppedPublisher.process.pid() : existing.admission().publisherPid(),
                        existing == null ? stoppedPublisher.startMillis
                                : existing.admission().publisherStartMillis(),
                        existing == null ? stoppedPublisher.session : existing.admission().publisherSession(),
                        root.toString(), rootObject, stageObject, originals, dependencyDigest(deps), members,
                        historyMode);
                files.requireHealthy();
                verifySourceRegistrations(observations);
                var admission = new PreparedAdmission(request, cohort);
                var receipt = reservations.admitClosedReconstruction(metadata, admission);
                if (imageIo != null) {
                    if (present(stage.resolve(FAILED))) {
                        throw new IOException("The original reconstruction retained an observed image failure");
                    }
                    var shadow = reservations.beginClosedReconstructionShadow(admission, receipt);
                    if (shadow.ready() != null) {
                        receipt = shadow.ready();
                    } else {
                        List<ProfileClosedReconstruction.FileImage> candidates;
                        try {
                            candidates = freshImages(files, nativeIo, root, stage, receipt, imageIo);
                        } catch (IOException failure) {
                            try {
                                reservations.failClosedReconstructionShadow(admission, shadow);
                            } catch (Exception recordFailure) {
                                if (recordFailure instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                                failure.addSuppressed(recordFailure);
                            }
                            throw failure;
                        }
                        var beforeReady = SegmentStorageInspector.readRecoveryDependencies(config, identity,
                                limits.controlBytes(), limits.controlRows(), key, validator, arena, bookie, locks);
                        if (!deps.equals(beforeReady)
                                || !observeDirectory(files, nativeIo, stage).equals(stageObject)) {
                            throw new IOException("Reconstruction bindings changed before READY");
                        }
                        verifyOriginalPaths(root, originals);
                        files.requireHealthy();
                        verifySourceRegistrations(observations);
                        receipt = reservations.readyClosedReconstruction(metadata,
                                new PreparedReady(receipt, cohort, candidates, shadow));
                    }
                }
                if (imageIo != null) {
                    finishReady(config, identity, key, validator, nativeIo, limits, root, data, arena, bookie, locks,
                            receipt, controlIo);
                }
                return receipt;
            }
        });
    }

    private static List<ProfileClosedReconstruction.FileImage> auditOriginals(IsolatedSegmentConfig config,
            Path root, NativeIO.FileIdentity rootObject, SegmentOwnedFiles files, NativeIO nativeIo,
            SegmentDataRecovery.ClosedImageAudit audit) throws Exception {
        return auditOriginals(config, root, rootObject, files, nativeIo, audit, new HashSet<>(), () -> { });
    }

    static List<ProfileClosedReconstruction.FileImage> auditOriginals(IsolatedSegmentConfig config,
            Path root, NativeIO.FileIdentity rootObject, SegmentOwnedFiles files, NativeIO nativeIo,
            SegmentDataRecovery.ClosedImageAudit audit, Set<NativeIO.FileIdentity> dataObjects,
            SegmentDataRecovery.AuditCheckpoint checkpoint) throws IOException {
        var objects = new ArrayList<NativeIO.FileIdentity>(config.dataFiles());
        for (int index = 0; index < config.dataFiles(); index++) {
            checkpoint.check();
            var object = requireOriginalFile(root.resolve("data-" + index), config.bytesPerFile());
            if (object.device() != rootObject.device() || object.equals(rootObject) || !dataObjects.add(object)) {
                throw new IOException("Original DATA aliases a required object or crosses the original filesystem");
            }
            objects.add(object);
        }
        try (var names = files.openDirectory(root)) {
            for (Path path : names) {
                checkpoint.check();
                String name = path.getFileName().toString();
                if (name.equals("isolated.lock") || name.startsWith("arena-control")
                        || name.startsWith("bookie-control") || name.startsWith("superblock-")) {
                    var identity = SegmentClassicHandoffPaths.loadingIdentity(path);
                    if (objects.contains(new NativeIO.FileIdentity(identity.device(), identity.inode()))) {
                        throw new IOException("Original DATA aliases a required control object");
                    }
                }
            }
        }
        var originals = new ArrayList<ProfileClosedReconstruction.FileImage>(config.dataFiles());
        for (int index = 0; index < config.dataFiles(); index++) {
            checkpoint.check();
            Path path = root.resolve("data-" + index);
            var object = objects.get(index);
            String digest;
            try (var file = SegmentOwnedFiles.openRegularReadOnly(files, path)) {
                if (!files.fileIdentity(file, nativeIo).equals(object)) {
                    throw new IOException("Opened original DATA differs from its path object");
                }
                digest = audit.scan(file, index, checkpoint);
                if (!files.fileIdentity(file, nativeIo).equals(object)) {
                    throw new IOException("Original DATA descriptor changed during its complete audit");
                }
            }
            originals.add(new ProfileClosedReconstruction.FileImage(index,
                    new ProfileClosedReconstruction.Image(object, config.bytesPerFile(), digest)));
        }
        verifyOriginalPaths(root, originals);
        return List.copyOf(originals);
    }

    static void verifyOriginalPaths(Path root, List<ProfileClosedReconstruction.FileImage> originals)
            throws IOException {
        for (var file : originals) {
            var image = file.image();
            if (!requireOriginalFile(root.resolve("data-" + file.index()), image.length()).equals(image.object())) {
                throw new IOException("Original DATA path object changed during its complete audit");
            }
        }
    }

    static void verifySourceRegistrations(Map<ProfileClosedReconstructionSources, String> observations)
            throws IOException, TimeoutException {
        for (var observation : observations.entrySet()) {
            // Each pair of source connections has already drained. Only its exact registration observations remain.
            if (!observation.getValue().equals(observation.getKey().revalidate())) {
                throw new IOException("CLOSED reconstruction source registration changed before publication");
            }
        }
    }

    private static Path staging(Path root, UUID operation) {
        return root.resolve(".closed-reconstruction-" + operation);
    }

    private static Path staged(Path stage, String role, int index, int count) {
        return stage.resolve(count == 1 ? role : role + "-" + index);
    }

    private static void requireStagingNames(SegmentOwnedFiles files, Path stage, int count, String secondRole)
            throws IOException {
        var allowed = new HashSet<Path>();
        for (int index = 0; index < count; index++) {
            allowed.add(staged(stage, CANDIDATE, index, count));
            allowed.add(staged(stage, secondRole, index, count));
        }
        try (var children = files.openDirectory(stage)) {
            for (Path path : children) {
                if (!allowed.contains(path)) {
                    throw new IOException("Unrecognized file in the original reconstruction staging");
                }
            }
        }
    }

    private static List<ProfileClosedReconstruction.FileImage> freshImages(SegmentOwnedFiles files, NativeIO nativeIo,
            Path root, Path stage, ProfileClosedReconstruction.Receipt receipt, SegmentFileIo io) throws Exception {
        var originals = receipt.admission().originals();
        int count = originals.size();
        requireStagingNames(files, stage, count, INTERRUPTED);
        var objects = new HashSet<NativeIO.FileIdentity>();
        originals.forEach(file -> objects.add(file.image().object()));
        for (var file : originals) {
            requirePartialImage(staged(stage, CANDIDATE, file.index(), count), stage, file.image(), objects);
            requirePartialImage(staged(stage, INTERRUPTED, file.index(), count), stage, file.image(), objects);
        }
        for (int index = 0; index < count; index++) {
            Path candidate = staged(stage, CANDIDATE, index, count);
            if (present(candidate)) {
                Path interrupted = staged(stage, INTERRUPTED, index, count);
                Files.deleteIfExists(interrupted);
                // Keep each previous candidate linked until every new candidate has actually closed.
                rename(nativeIo, candidate, interrupted);
            }
        }
        SegmentOwnedFiles.syncDirectory(files, stage);
        try {
            var candidates = new ArrayList<ProfileClosedReconstruction.FileImage>(count);
            ByteBuffer buffer = ByteBuffer.allocate(COPY_BYTES);
            for (var file : originals) {
                int index = file.index();
                var image = copyFreshImage(files, nativeIo, root.resolve("data-" + index),
                        staged(stage, CANDIDATE, index, count), file.image(), buffer, io);
                candidates.add(new ProfileClosedReconstruction.FileImage(index, image));
            }
            for (var candidate : candidates) {
                var image = candidate.image();
                if (!requireOriginalFile(staged(stage, CANDIDATE, candidate.index(), count), image.length())
                        .equals(image.object())) {
                    throw new IOException("Fresh DATA path changed after its original first close");
                }
                Files.deleteIfExists(staged(stage, INTERRUPTED, candidate.index(), count));
            }
            SegmentOwnedFiles.syncDirectory(files, stage);
            return List.copyOf(candidates);
        } catch (IOException failure) {
            try {
                Files.createFile(stage.resolve(FAILED), PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-------")));
                SegmentOwnedFiles.syncDirectory(null, stage);
            } catch (IOException markerFailure) {
                failure.addSuppressed(markerFailure);
            }
            throw failure;
        }
    }

    private static ProfileClosedReconstruction.Image copyFreshImage(SegmentOwnedFiles files, NativeIO nativeIo,
            Path originalPath, Path candidate, ProfileClosedReconstruction.Image original, ByteBuffer buffer,
            SegmentFileIo io) throws Exception {
        return copyFreshImage(files, nativeIo, originalPath, candidate, original, buffer, io, () -> { });
    }

    static ProfileClosedReconstruction.Image copyFreshImage(SegmentOwnedFiles files, NativeIO nativeIo,
            Path originalPath, Path candidate, ProfileClosedReconstruction.Image original, ByteBuffer buffer,
            SegmentFileIo io, SegmentDataRecovery.AuditCheckpoint checkpoint) throws Exception {
        checkpoint.check();
        var permissions = Files.getPosixFilePermissions(originalPath, LinkOption.NOFOLLOW_LINKS);
        NativeIO.FileIdentity fresh;
        try (var input = SegmentOwnedFiles.openRegularReadOnly(files, originalPath);
                var output = SegmentOwnedFiles.open(files, candidate,
                        Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE,
                                LinkOption.NOFOLLOW_LINKS), PosixFilePermissions.asFileAttribute(permissions))) {
            fresh = files.fileIdentity(output, nativeIo);
            if (!files.fileIdentity(input, nativeIo).equals(original.object()) || input.size() != original.length()
                    || fresh.equals(original.object()) || fresh.device() != original.object().device()) {
                throw new IOException("Reconstruction copy descriptors do not match their exact roles");
            }
            var digest = MessageDigest.getInstance("SHA-256");
            for (long offset = 0; offset < original.length();) {
                checkpoint.check();
                int count = (int) Math.min(buffer.capacity(), original.length() - offset);
                buffer.clear().limit(count);
                SegmentFileIo.readFully(input, buffer, offset);
                checkpoint.check();
                io.writeFully(output, buffer, offset);
                digest.update(buffer);
                offset += count;
            }
            if (!original.sha256().equals(HexFormat.of().formatHex(digest.digest()))) {
                throw new IOException("Original DATA changed while generating the complete fresh image");
            }
            checkpoint.check();
            io.force(output, true);
            checkpoint.check();
            if (!original.sha256().equals(hash(output, original.length(), buffer, checkpoint))) {
                throw new IOException("Fresh DATA readback differs from the complete original image");
            }
        }
        files.requireHealthy();
        checkpoint.check();
        return new ProfileClosedReconstruction.Image(fresh, original.length(), original.sha256());
    }

    private static void requirePartialImage(Path path, Path stage, ProfileClosedReconstruction.Image original,
            Set<NativeIO.FileIdentity> objects) throws IOException {
        if (!present(path)) {
            return;
        }
        var prior = SegmentClassicHandoffPaths.loadingIdentity(path);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || prior.links() != 1
                || prior.device() != original.object().device()
                || !objects.add(new NativeIO.FileIdentity(prior.device(), prior.inode()))
                || prior.size() > original.length() || !Files.getOwner(path, LinkOption.NOFOLLOW_LINKS)
                        .equals(Files.getOwner(stage, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Interrupted staging file does not belong to this operation");
        }
    }

    private static String hash(java.nio.channels.FileChannel file, long length, ByteBuffer buffer,
            SegmentDataRecovery.AuditCheckpoint checkpoint) throws Exception {
        if (file.size() != length) {
            throw new IOException("Reconstruction file length differs from its native binding");
        }
        var digest = MessageDigest.getInstance("SHA-256");
        for (long offset = 0; offset < length;) {
            checkpoint.check();
            int count = (int) Math.min(buffer.capacity(), length - offset);
            buffer.clear().limit(count);
            SegmentFileIo.readFully(file, buffer, offset);
            digest.update(buffer);
            offset += count;
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static boolean present(Path path) throws IOException {
        try {
            Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException absent) {
            return false;
        }
    }

    // During publication: INSTALLED* RETAINED? ORIGINAL*. During cleanup: CLEANED* INSTALLED*.
    private enum PublicationPhase {
        CLEANED, INSTALLED, RETAINED, ORIGINAL
    }

    private static void finishReady(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits, Path root,
            SegmentDataFileLock data, SegmentDataFileLock arena, SegmentDataFileLock bookie, List<Path> locks,
            ProfileClosedReconstruction.Receipt receipt, SegmentFileIo controlIo) throws Exception {
        var admitted = receipt.admission();
        var candidates = receipt.ready().candidates();
        int count = candidates.size();
        Path stage = staging(root, admitted.operation());
        var files = new SegmentOwnedFiles(locks);
        try (AutoCloseable cleanup = () -> requireDrained(files, data)) {
            if (!root.toString().equals(admitted.directory())
                    || !observeDirectory(files, nativeIo, root).equals(admitted.directoryObject())) {
                throw new IOException("READY cannot publish into a replacement directory");
            }
            boolean stagePresent = present(stage);
            var phases = List.<PublicationPhase>of();
            var dataPaths = new ArrayList<Path>(count);
            ByteBuffer buffer = ByteBuffer.allocate(COPY_BYTES);
            if (stagePresent) {
                if (!observeDirectory(files, nativeIo, stage).equals(admitted.stagingObject())) {
                    throw new IOException("READY staging was replaced");
                }
                requireStagingNames(files, stage, count, RETAINED);
                phases = verifyPublicationPrefix(config, identity, files, nativeIo, root, stage, receipt, buffer);
                for (int index = 0; index < count; index++) {
                    dataPaths.add(phases.get(index) == PublicationPhase.RETAINED
                            ? staged(stage, RETAINED, index, count) : root.resolve("data-" + index));
                }
            }
            var deps = SegmentStorageInspector.readRecoveryDependencies(config, identity, limits.controlBytes(),
                    limits.controlRows(), key, validator, arena, bookie, locks, dataPaths);
            long epoch = deps.arena().cut().sessionEpoch();
            if (epoch > admitted.scope().epoch() || !deps.arena().cut().sessionOpen()) {
                if (stagePresent) {
                    throw new IOException("Advanced or closed Arena has unexpected historical reconstruction staging");
                }
                SegmentArenaControlLog.requireReconstructionApplied(config, identity, limits.controlBytes(),
                        arena, files, receipt);
                if (epoch == admitted.scope().epoch()) {
                    SegmentStorageInspector.requireReconstructionMode(deps, admitted.historyMode());
                    verifyActiveReadyImages(config, identity, files, nativeIo, root, candidates, buffer);
                }
                return;
            }
            requireReadyCuts(deps, admitted);
            if (stagePresent) {
                syncPublicationDirectories(files, root, stage);
                for (int index = 0; index < count; index++) {
                    Path active = root.resolve("data-" + index);
                    var phase = phases.get(index);
                    if (phase == PublicationPhase.ORIGINAL) {
                        rename(nativeIo, active, staged(stage, RETAINED, index, count));
                        syncPublicationDirectories(files, root, stage);
                    }
                    if (phase == PublicationPhase.ORIGINAL || phase == PublicationPhase.RETAINED) {
                        rename(nativeIo, staged(stage, CANDIDATE, index, count), active);
                        syncPublicationDirectories(files, root, stage);
                    }
                }
            }
            verifyActiveReadyImages(config, identity, files, nativeIo, root, candidates, buffer);
            if (stagePresent) {
                for (int index = 0; index < count; index++) {
                    if (phases.get(index) != PublicationPhase.CLEANED) {
                        Files.delete(staged(stage, RETAINED, index, count));
                        SegmentOwnedFiles.syncDirectory(files, stage);
                    }
                }
                Files.delete(stage);
            }
            SegmentOwnedFiles.syncDirectory(files, root);
            files.requireHealthy();
            SegmentArenaControlLog.resolveReconstruction(config, identity, limits.controlBytes(), controlIo,
                    arena, files, new PublishedImage(receipt));
        }
    }

    private static List<PublicationPhase> verifyPublicationPrefix(IsolatedSegmentConfig config,
            SegmentFileHeader identity, SegmentOwnedFiles files, NativeIO nativeIo, Path root, Path stage,
            ProfileClosedReconstruction.Receipt receipt, ByteBuffer buffer) throws Exception {
        var originals = receipt.admission().originals();
        var candidates = receipt.ready().candidates();
        int count = originals.size();
        var phases = new ArrayList<PublicationPhase>(count);
        PublicationPhase previous = PublicationPhase.CLEANED;
        boolean cleanup = false;
        for (int index = 0; index < count; index++) {
            var original = originals.get(index);
            var fresh = candidates.get(index);
            Path active = root.resolve("data-" + index);
            Path candidate = staged(stage, CANDIDATE, index, count);
            Path retained = staged(stage, RETAINED, index, count);
            boolean haveActive = present(active);
            boolean haveCandidate = present(candidate);
            boolean haveRetained = present(retained);
            PublicationPhase phase;
            if (haveCandidate) {
                if (haveActive == haveRetained) {
                    throw new IOException("Incomplete READY switch lost the exact placement of an old DATA file");
                }
                verifyImage(files, nativeIo, config, identity, haveActive ? active : retained, original, buffer);
                verifyImage(files, nativeIo, config, identity, candidate, fresh, buffer);
                phase = haveActive ? PublicationPhase.ORIGINAL : PublicationPhase.RETAINED;
            } else {
                verifyImage(files, nativeIo, config, identity, active, fresh, buffer);
                if (haveRetained) {
                    verifyImage(files, nativeIo, config, identity, retained, original, buffer);
                }
                phase = haveRetained ? PublicationPhase.INSTALLED : PublicationPhase.CLEANED;
            }
            cleanup |= phase == PublicationPhase.CLEANED;
            if (phase.compareTo(previous) < 0
                    || (phase == PublicationPhase.RETAINED && previous == PublicationPhase.RETAINED)
                    || (cleanup && phase.compareTo(PublicationPhase.INSTALLED) > 0)) {
                throw new IOException("READY DATA files are outside the original publication prefix");
            }
            phases.add(phase);
            previous = phase;
        }
        return phases;
    }

    private static void syncPublicationDirectories(SegmentOwnedFiles files, Path root, Path stage) throws IOException {
        SegmentOwnedFiles.syncDirectory(files, root);
        SegmentOwnedFiles.syncDirectory(files, stage);
    }

    private static void requireFileGeometry(IsolatedSegmentConfig config,
            List<ProfileClosedReconstruction.FileImage> images) throws IOException {
        if (images.size() != config.dataFiles()
                || images.stream().anyMatch(file -> file.image().length() != config.bytesPerFile())) {
            throw new IOException("Reconstruction DATA file geometry differs from the original admission");
        }
    }

    private static void verifyActiveReadyImages(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentOwnedFiles files, NativeIO nativeIo, Path root, List<ProfileClosedReconstruction.FileImage> images,
            ByteBuffer buffer) throws Exception {
        requireFileGeometry(config, images);
        for (var image : images) {
            verifyImage(files, nativeIo, config, identity, root.resolve("data-" + image.index()), image, buffer);
        }
    }

    private static void requireReadyCuts(SegmentStorageInspector.RecoveryDependencies deps,
            ProfileClosedReconstruction.Admission admitted) throws Exception {
        if (deps.arena().cut().sessionEpoch() != admitted.scope().epoch()) {
            throw new IOException("READY lost the original managed epoch");
        }
        SegmentStorageInspector.requireReconstructionMode(deps, admitted.historyMode());
        if (deps.arena().cut().sessionOpen() && (admitted.arenaCut() != deps.arena().cut().appliedSequence()
                || admitted.bookieCut() != deps.bookie().observedSequence()
                || !admitted.dependencies().equals(dependencyDigest(deps)))) {
            throw new IOException("READY control dependencies changed before original epoch resolution");
        }
    }

    static void verifyFirstOpen(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SegmentDataFileLock dataLock, SegmentArenaControlLog arena, SegmentBookieControlLog bookie,
            ZKProfileReservationStore reservations, ProfileLedgerMetadataStore metadata,
            ZKProfileEphemeralRegistrationStore.Admission ownedStartup)
            throws IOException, InterruptedException {
        var pending = arena.pendingReconstruction();
        if (pending == null) {
            return;
        }
        if (reservations == null || metadata == null || pending.binding().bookieCut() != bookie.appliedThrough()) {
            throw new IOException("First reconstructed OPEN requires the original Bookie cut and current metadata");
        }
        var scope = new ProfileClosedReconstruction.Scope(identity.bookieId().toString(), identity.incarnation(),
                identity.arenaId(), pending.epoch());
        var receipt = reservations.readClosedReconstruction(scope);
        if (receipt == null || receipt.ready() == null
                || !receipt.admission().operation().equals(pending.operation())) {
            throw new IOException("First reconstructed OPEN lost its original native READY");
        }
        arena.requireReconstructionMode(receipt.admission().historyMode(), bookie.appliedThrough());
        Path root = config.directory().toRealPath();
        dataLock.requireHeld(root.resolve("isolated.lock"));
        var files = new SegmentOwnedFiles(List.of(root.resolve("isolated.lock"), root.resolve("arena-control.lock"),
                root.resolve("bookie-control.lock")));
        var nativeIo = new org.apache.bookkeeper.common.util.nativeio.NativeIOImpl();
        try (AutoCloseable cleanup = () -> requireDrained(files, dataLock)) {
            if (!root.toString().equals(receipt.admission().directory())
                    || !observeDirectory(files, nativeIo, root).equals(receipt.admission().directoryObject())
                    || present(staging(root, pending.operation()))) {
                throw new IOException("First reconstructed OPEN requires the original published and cleaned scope");
            }
            verifyActiveReadyImages(config, identity, files, nativeIo, root, receipt.ready().candidates(),
                    ByteBuffer.allocate(COPY_BYTES));
        } catch (IOException | InterruptedException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("First reconstructed OPEN could not verify the complete published image", failure);
        }
        boolean deleted = false;
        for (var member : receipt.admission().members()) {
            var current = reservations.readClosedForOpen(metadata, member.ledger());
            if (current.status() == ZKProfileReservationStore.ReadOnlyStatus.DELETED && ownedStartup != null
                    && ownedStartup.allowsPendingDeleteStartup(reservations, scope.bookie(), scope.incarnation())) {
                reservations.verifyDeletedReconstructionPredecessor(metadata, receipt, member);
                deleted = true;
                continue;
            }
            var snapshot = current.snapshot();
            if (snapshot == null || !member.matchesContext(snapshot.context().ledger())) {
                throw new IOException("First reconstructed OPEN requires its current CLOSED lifecycle and membership");
            }
            var proof = reservations.readClosedReconstructionProof(metadata, snapshot, scope.bookie(),
                    scope.incarnation().value());
            if (!ProfileClosedReconstruction.matchesEvidence(member, snapshot, proof)) {
                throw new IOException("Original CLOSED snapshot changed before first reconstructed OPEN");
            }
        }
        if (deleted && !ownedStartup.allowsPendingDeleteStartup(reservations, scope.bookie(), scope.incarnation())) {
            throw new IOException("Original owned startup retired before reconstructed DELETE catch-up");
        }
        arena.verifyFirstReconstructionOpen(pending, receipt);
    }

    private static void verifyImage(SegmentOwnedFiles files, NativeIO nativeIo, IsolatedSegmentConfig config,
            SegmentFileHeader identity, Path path, ProfileClosedReconstruction.FileImage row, ByteBuffer buffer)
            throws Exception {
        verifyImage(files, nativeIo, config, identity, path, row, buffer, () -> { });
    }

    static void verifyImage(SegmentOwnedFiles files, NativeIO nativeIo, IsolatedSegmentConfig config,
            SegmentFileHeader identity, Path path, ProfileClosedReconstruction.FileImage row, ByteBuffer buffer,
            SegmentDataRecovery.AuditCheckpoint checkpoint) throws Exception {
        checkpoint.check();
        var image = row.image();
        if (!requireOriginalFile(path, image.length()).equals(image.object())) {
            throw new IOException("Reconstruction path does not name the exact READY object");
        }
        try (var file = SegmentOwnedFiles.openRegularReadOnly(files, path)) {
            if (!files.fileIdentity(file, nativeIo).equals(image.object())
                    || !image.sha256().equals(hash(file, image.length(), buffer, checkpoint))) {
                throw new IOException("Reconstruction object does not match the exact READY image");
            }
            ByteBuffer header = ByteBuffer.allocate(config.alignment());
            SegmentFileIo.readFully(file, header, 0);
            identity.validate(header, config, row.index());
        }
        if (!requireOriginalFile(path, image.length()).equals(image.object())) {
            throw new IOException("Reconstruction image path changed after its original close");
        }
        checkpoint.check();
    }

    private static void rename(NativeIO nativeIo, Path source, Path target) throws IOException {
        if (nativeIo.renameNoReplace(source.toString(), target.toString()) != 0) {
            throw new IOException("Native reconstruction rename did not report success");
        }
    }

    static NativeIO.FileIdentity observeDirectory(SegmentOwnedFiles files, NativeIO nativeIo, Path path)
            throws IOException {
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) || !path.toRealPath().equals(path)) {
            throw new IOException("Reconstruction requires an original directory without aliases");
        }
        var expected = SegmentClassicHandoffPaths.loadingIdentity(path);
        var object = new NativeIO.FileIdentity(expected.device(), expected.inode());
        try (var channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (!files.fileIdentity(channel, nativeIo).equals(object)) {
                throw new IOException("Opened reconstruction directory differs from its path object");
            }
        }
        return object;
    }

    static NativeIO.FileIdentity requireOriginalFile(Path path, long length) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Reconstruction requires the original regular DATA object");
        }
        var actual = SegmentClassicHandoffPaths.loadingIdentity(path);
        if (actual.links() != 1 || actual.size() != length) {
            throw new IOException("Reconstruction DATA object has another extent or more than one link");
        }
        return new NativeIO.FileIdentity(actual.device(), actual.inode());
    }

    private static String dependencyDigest(SegmentStorageInspector.RecoveryDependencies deps) throws Exception {
        var arena = deps.arena();
        var bookie = deps.bookie();
        String binding = arena.historyDigest() + bookie.historyDigest() + "/" + arena.cut().physicalGeneration()
                + "/" + arena.cut().checkpointGeneration() + "/" + bookie.physicalGeneration()
                + "/" + bookie.checkpointGeneration();
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(binding.getBytes(StandardCharsets.US_ASCII)));
    }

    private static void requireDrained(SegmentOwnedFiles files, SegmentDataFileLock guard) throws Exception {
        SegmentOwnedFiles.Result result;
        try {
            result = files.drain();
        } catch (InterruptedException failure) {
            guard.retainAfterUnresolvedReconstruction(files);
            throw failure;
        }
        if (!result.drained()) {
            guard.retainAfterUnresolvedReconstruction(files);
            throw new IOException("Reconstruction readers did not drain", result.closeFailure());
        }
    }

    private SegmentClosedReconstruction() { }
}

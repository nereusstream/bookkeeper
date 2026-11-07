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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstructionSources;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;

/** Controlled offline recovery of one complete original CLOSED image. */
public final class SegmentClosedReconstruction {
    private static final int COPY_BYTES = 64 * 1024;
    private static final String CANDIDATE = "image";
    private static final String INTERRUPTED = "interrupted-image";
    private static final String RETAINED = "retained-old";
    private static final String FAILED = "image-failed";

    record Limits(long controlBytes, int controlRows, long dataBytes, int entries, Duration sourceTimeout) { }

    record Sources(ClientContext sdk, EventLoopGroup eventLoop, SslContext tls, ThreadPoolExecutor cold,
                   ZKProfileRegistrationStore registry) { }

    /** Captured by the controlled launcher while its original process and registration are both live. */
    static final class OldPublisher {
        private final SegmentFileHeader identity;
        private final ProcessHandle process;
        private final long startMillis;
        private final long session;

        OldPublisher(SegmentFileHeader identity, ProcessHandle process, ZKProfileRegistrationStore.RegisteredPeer peer)
                throws IOException {
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
        if (imageIo != null && config.dataFiles() > 1) {
            throw new IOException("Multi-file reconstruction publication is not yet available");
        }
        var scope = new ProfileClosedReconstruction.Scope(identity.bookieId().toString(), identity.incarnation(),
                identity.arenaId(), epoch);
        return SegmentStorageInspector.withRecoveryLocks(config, identity, (root, data, arena, bookie, locks) -> {
            var existing = reservations.readClosedReconstruction(scope);
            if (existing != null && !existing.admission().operation().equals(operation)) {
                throw new IOException("The original managed epoch already has a different reconstruction operation");
            }
            if (existing != null && existing.admission().originals().size() != config.dataFiles()) {
                throw new IOException("Reconstruction DATA file count differs from the original admission");
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
                                cohort.size() == 1 && config.dataFiles() == 1 ? null : context,
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
                        root.toString(), rootObject, stageObject, originals, dependencyDigest(deps), members);
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
                        ProfileClosedReconstruction.Image candidate;
                        try {
                            candidate = freshImage(files, nativeIo, root, stage, receipt, imageIo);
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
                                new PreparedReady(receipt, cohort,
                                        List.of(new ProfileClosedReconstruction.FileImage(0, candidate)), shadow));
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
        var objects = new HashSet<NativeIO.FileIdentity>();
        objects.add(rootObject);
        try (var names = files.openDirectory(root)) {
            for (Path path : names) {
                String name = path.getFileName().toString();
                if (name.equals("isolated.lock") || name.startsWith("arena-control")
                        || name.startsWith("bookie-control") || name.startsWith("superblock-")) {
                    var identity = SegmentClassicHandoffPaths.loadingIdentity(path);
                    objects.add(new NativeIO.FileIdentity(identity.device(), identity.inode()));
                }
            }
        }
        var originals = new ArrayList<ProfileClosedReconstruction.FileImage>(config.dataFiles());
        for (int index = 0; index < config.dataFiles(); index++) {
            Path path = root.resolve("data-" + index);
            var object = requireOriginalFile(path, config.bytesPerFile());
            if (object.device() != rootObject.device() || !objects.add(object)) {
                throw new IOException("Original DATA aliases a required object or crosses the original filesystem");
            }
            String digest;
            try (var file = SegmentOwnedFiles.openRegularReadOnly(files, path)) {
                if (!files.fileIdentity(file, nativeIo).equals(object)) {
                    throw new IOException("Opened original DATA differs from its path object");
                }
                digest = audit.scan(file, index);
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

    private static void verifyOriginalPaths(Path root, List<ProfileClosedReconstruction.FileImage> originals)
            throws IOException {
        for (var file : originals) {
            var image = file.image();
            if (!requireOriginalFile(root.resolve("data-" + file.index()), image.length()).equals(image.object())) {
                throw new IOException("Original DATA path object changed during its complete audit");
            }
        }
    }

    private static void verifySourceRegistrations(Map<ProfileClosedReconstructionSources, String> observations)
            throws Exception {
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

    private static ProfileClosedReconstruction.Image freshImage(SegmentOwnedFiles files, NativeIO nativeIo,
            Path root, Path stage, ProfileClosedReconstruction.Receipt receipt, SegmentFileIo io) throws Exception {
        var admitted = receipt.admission();
        var original = admitted.original();
        Path originalPath = root.resolve("data-0");
        Path candidate = stage.resolve(CANDIDATE);
        Path interrupted = stage.resolve(INTERRUPTED);
        if (present(stage.resolve(FAILED)) || present(stage.resolve(RETAINED))) {
            throw new IOException("ADMITTED staging has an observed failure or an unpublished retained file");
        }
        try (var children = files.openDirectory(stage)) {
            for (Path path : children) {
                if (!path.equals(candidate) && !path.equals(interrupted)) {
                    throw new IOException("Unrecognized file in the original reconstruction staging");
                }
            }
        }
        if (present(interrupted)) {
            requirePartialImage(interrupted, stage, original);
        }
        if (present(candidate)) {
            requirePartialImage(candidate, stage, original);
            Files.deleteIfExists(interrupted);
            // Keep this inode linked until the next original candidate descriptor has actually closed.
            rename(nativeIo, candidate, interrupted);
            SegmentOwnedFiles.syncDirectory(files, stage);
        }
        try {
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
                ByteBuffer buffer = ByteBuffer.allocate(COPY_BYTES);
                for (long offset = 0; offset < original.length();) {
                    int count = (int) Math.min(buffer.capacity(), original.length() - offset);
                    buffer.clear().limit(count);
                    SegmentFileIo.readFully(input, buffer, offset);
                    io.writeFully(output, buffer, offset);
                    digest.update(buffer);
                    offset += count;
                }
                if (!original.sha256().equals(HexFormat.of().formatHex(digest.digest()))) {
                    throw new IOException("Original DATA changed while generating the complete fresh image");
                }
                io.force(output, true);
                if (!original.sha256().equals(hash(output, original.length()))) {
                    throw new IOException("Fresh DATA readback differs from the complete original image");
                }
            }
            files.requireHealthy();
            if (!requireOriginalFile(candidate, original.length()).equals(fresh)) {
                throw new IOException("Fresh DATA path changed during its original first close");
            }
            Files.deleteIfExists(interrupted);
            SegmentOwnedFiles.syncDirectory(files, stage);
            return new ProfileClosedReconstruction.Image(fresh, original.length(), original.sha256());
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

    private static void requirePartialImage(Path path, Path stage, ProfileClosedReconstruction.Image original)
            throws IOException {
        var prior = SegmentClassicHandoffPaths.loadingIdentity(path);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || prior.links() != 1
                || prior.device() != original.object().device() || prior.inode() == original.object().inode()
                || prior.size() > original.length() || !Files.getOwner(path, LinkOption.NOFOLLOW_LINKS)
                        .equals(Files.getOwner(stage, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Interrupted staging file does not belong to this operation");
        }
    }

    private static String hash(java.nio.channels.FileChannel file, long length) throws Exception {
        if (file.size() != length) {
            throw new IOException("Reconstruction file length differs from its native binding");
        }
        var digest = MessageDigest.getInstance("SHA-256");
        ByteBuffer buffer = ByteBuffer.allocate(COPY_BYTES);
        for (long offset = 0; offset < length;) {
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

    private static void finishReady(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits, Path root,
            SegmentDataFileLock data, SegmentDataFileLock arena, SegmentDataFileLock bookie, List<Path> locks,
            ProfileClosedReconstruction.Receipt receipt, SegmentFileIo controlIo) throws Exception {
        var admitted = receipt.admission();
        var fresh = receipt.ready().candidate();
        Path active = root.resolve("data-0");
        Path stage = staging(root, admitted.operation());
        Path candidate = stage.resolve(CANDIDATE);
        Path retained = stage.resolve(RETAINED);
        var files = new SegmentOwnedFiles(locks);
        try (AutoCloseable cleanup = () -> requireDrained(files, data)) {
            if (!root.toString().equals(admitted.directory())
                    || !observeDirectory(files, nativeIo, root).equals(admitted.directoryObject())) {
                throw new IOException("READY cannot publish into a replacement directory");
            }
            boolean stagePresent = present(stage);
            if (stagePresent) {
                if (!observeDirectory(files, nativeIo, stage).equals(admitted.stagingObject())) {
                    throw new IOException("READY staging was replaced");
                }
                try (var children = files.openDirectory(stage)) {
                    for (Path path : children) {
                        if (!path.equals(candidate) && !path.equals(retained)) {
                            throw new IOException("READY staging contains an unexpected object");
                        }
                    }
                }
            }
            boolean haveCandidate = stagePresent && present(candidate);
            boolean haveRetained = stagePresent && present(retained);
            if (present(active)) {
                var deps = SegmentStorageInspector.readRecoveryDependencies(config, identity, limits.controlBytes(),
                        limits.controlRows(), key, validator, arena, bookie, locks);
                if (deps.arena().cut().sessionEpoch() > admitted.scope().epoch()) {
                    if (stagePresent) {
                        throw new IOException("Advanced Arena has unexpected historical reconstruction staging");
                    }
                    SegmentArenaControlLog.requireReconstructionApplied(config, identity, limits.controlBytes(),
                            arena, files, receipt);
                    return;
                }
                requireReadyCuts(deps, admitted);
                if (!deps.arena().cut().sessionOpen()) {
                    if (stagePresent) {
                        throw new IOException("Closed Arena has unexpected reconstruction staging");
                    }
                    SegmentArenaControlLog.requireReconstructionApplied(config, identity, limits.controlBytes(),
                            arena, files, receipt);
                    verifyImage(files, nativeIo, config, identity, active, fresh);
                    return;
                }
                var object = requireOriginalFile(active, admitted.original().length());
                if (object.equals(admitted.original().object()) && haveCandidate && !haveRetained) {
                    verifyImage(files, nativeIo, config, identity, active, admitted.original());
                    verifyImage(files, nativeIo, config, identity, candidate, fresh);
                    rename(nativeIo, active, retained);
                    SegmentOwnedFiles.syncDirectory(files, root);
                    SegmentOwnedFiles.syncDirectory(files, stage);
                    haveRetained = true;
                } else if (!object.equals(fresh.object()) || haveCandidate) {
                    throw new IOException("Active DATA is outside the original READY publication sequence");
                }
            }
            if (!present(active)) {
                if (!haveRetained || !haveCandidate) {
                    throw new IOException("Incomplete READY switch lost an exact old or new object");
                }
                verifyImage(files, nativeIo, config, identity, retained, admitted.original());
                verifyImage(files, nativeIo, config, identity, candidate, fresh);
                SegmentOwnedFiles.syncDirectory(files, root);
                SegmentOwnedFiles.syncDirectory(files, stage);
                rename(nativeIo, candidate, active);
                SegmentOwnedFiles.syncDirectory(files, root);
                SegmentOwnedFiles.syncDirectory(files, stage);
            }
            verifyImage(files, nativeIo, config, identity, active, fresh);
            var deps = SegmentStorageInspector.readRecoveryDependencies(config, identity, limits.controlBytes(),
                    limits.controlRows(), key, validator, arena, bookie, locks);
            requireReadyCuts(deps, admitted);
            if (!deps.arena().cut().sessionOpen()) {
                throw new IOException("READY installation no longer has the original OPEN epoch");
            }
            if (stagePresent) {
                SegmentOwnedFiles.syncDirectory(files, root);
                SegmentOwnedFiles.syncDirectory(files, stage);
                if (haveRetained) {
                    verifyImage(files, nativeIo, config, identity, retained, admitted.original());
                    Files.delete(retained);
                    SegmentOwnedFiles.syncDirectory(files, stage);
                }
                Files.delete(stage);
            }
            SegmentOwnedFiles.syncDirectory(files, root);
            files.requireHealthy();
            SegmentArenaControlLog.resolveReconstruction(config, identity, limits.controlBytes(), controlIo,
                    arena, files, new PublishedImage(receipt));
        }
    }

    private static void requireReadyCuts(SegmentStorageInspector.RecoveryDependencies deps,
            ProfileClosedReconstruction.Admission admitted) throws Exception {
        if (deps.arena().cut().sessionEpoch() != admitted.scope().epoch()) {
            throw new IOException("READY lost the original managed epoch");
        }
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
            verifyImage(files, nativeIo, config, identity, root.resolve("data-0"), receipt.ready().candidate());
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
            SegmentFileHeader identity, Path path, ProfileClosedReconstruction.Image image) throws Exception {
        if (!requireOriginalFile(path, image.length()).equals(image.object())) {
            throw new IOException("Reconstruction path does not name the exact READY object");
        }
        try (var file = SegmentOwnedFiles.openRegularReadOnly(files, path)) {
            if (!files.fileIdentity(file, nativeIo).equals(image.object())
                    || !image.sha256().equals(hash(file, image.length()))) {
                throw new IOException("Reconstruction object does not match the exact READY image");
            }
            ByteBuffer header = ByteBuffer.allocate(config.alignment());
            SegmentFileIo.readFully(file, header, 0);
            identity.validate(header, config, 0);
        }
        if (!requireOriginalFile(path, image.length()).equals(image.object())) {
            throw new IOException("Reconstruction image path changed after its original close");
        }
    }

    private static void rename(NativeIO nativeIo, Path source, Path target) throws IOException {
        if (nativeIo.renameNoReplace(source.toString(), target.toString()) != 0) {
            throw new IOException("Native reconstruction rename did not report success");
        }
    }

    private static NativeIO.FileIdentity observeDirectory(SegmentOwnedFiles files, NativeIO nativeIo, Path path)
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

    private static NativeIO.FileIdentity requireOriginalFile(Path path, long length) throws IOException {
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

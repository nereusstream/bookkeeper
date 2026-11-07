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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstructionSources;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;

/** Controlled offline recovery of one complete original CLOSED image. */
public final class SegmentClosedReconstruction {
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
        private final ZKProfileReservationStore.ReadOnlySnapshot expected;

        private PreparedAdmission(ProfileClosedReconstruction.Admission request,
                ZKProfileReservationStore.ReadOnlySnapshot expected) {
            this.request = request;
            this.expected = expected;
        }

        public ProfileClosedReconstruction.Admission request() {
            return request;
        }

        public ZKProfileReservationStore.ReadOnlySnapshot expected() {
            return expected;
        }
    }

    static ProfileClosedReconstruction.Receipt admit(IsolatedSegmentConfig config, SegmentFileHeader identity,
            SecretKey key, ProfileDescriptorValidator validator, NativeIO nativeIo, Limits limits,
            ProfileLedgerMetadataStore metadata, ZKProfileReservationStore reservations, Sources sources,
            ZKProfileReservationStore.ReadOnlySnapshot expected, OldPublisher stoppedPublisher, long epoch,
            UUID operation) throws Exception {
        var scope = new ProfileClosedReconstruction.Scope(identity.bookieId().toString(), identity.incarnation(),
                identity.arenaId(), epoch);
        return SegmentStorageInspector.withRecoveryLocks(config, identity, (root, data, arena, bookie, locks) -> {
            var existing = reservations.readClosedReconstruction(scope);
            if (existing != null && !existing.admission().operation().equals(operation)) {
                throw new IOException("The original managed epoch already has a different reconstruction operation");
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
            var self = reservations.readClosedReconstructionSelf(metadata, expected, scope.bookie(),
                    scope.incarnation().value());
            SegmentStorageInspector.requireClosedReconstructionScope(config, deps, self);
            var closed = expected.closed().metadata().metadata().getValue();
            var files = new SegmentOwnedFiles(locks);
            try (AutoCloseable cleanup = () -> requireDrained(files)) {
                var originalPath = root.resolve("data-0");
                var rootObject = observeDirectory(files, nativeIo, root);
                var pathObject = requireOriginalFile(originalPath, config.bytesPerFile());
                SegmentDataRecovery.ClosedImage image;
                try (var file = SegmentOwnedFiles.openRegularReadOnly(files, originalPath)) {
                    if (!files.fileIdentity(file, nativeIo).equals(pathObject)) {
                        throw new IOException("Opened original DATA differs from its path object");
                    }
                    image = SegmentDataRecovery.auditClosedImage(file, config, identity, deps.arena(),
                            expected.context().ledger(), closed.getLastEntryId(), closed.getLength(),
                            limits.dataBytes(), limits.entries());
                    if (!files.fileIdentity(file, nativeIo).equals(pathObject)) {
                        throw new IOException("Original DATA descriptor changed during its complete audit");
                    }
                }
                if (!requireOriginalFile(originalPath, image.bytes()).equals(pathObject)) {
                    throw new IOException("Original DATA path object changed during its complete audit");
                }
                try (var peers = ProfileClosedReconstructionSources.open(sources.sdk(), sources.eventLoop(),
                        sources.tls(), sources.registry(), sources.cold(), expected, identity.bookieId(),
                        config.maxPayloadBytes() + SegmentBatchFormat.ENTRY_OVERHEAD, limits.sourceTimeout())) {
                    for (int entry = 0; entry < image.entries().size(); entry++) {
                        var local = image.entries().get(entry);
                        peers.verify(entry, local.cumulativeLength(), local.payloadLength(), local.payloadSha256());
                    }
                    var current = SegmentStorageInspector.readRecoveryDependencies(config, identity,
                            limits.controlBytes(), limits.controlRows(), key, validator, arena, bookie, locks);
                    if (!deps.equals(current) || !requireOriginalFile(originalPath, image.bytes()).equals(pathObject)) {
                        throw new IOException("Original scope changed before CLOSED reconstruction admission");
                    }
                    var stage = root.resolve(".closed-reconstruction-" + operation);
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
                    var request = new ProfileClosedReconstruction.Admission(scope, operation, closed.getLedgerId(),
                            deps.arena().cut().appliedSequence(), deps.bookie().observedSequence(),
                            existing == null ? stoppedPublisher.process.pid() : existing.admission().publisherPid(),
                            existing == null ? stoppedPublisher.startMillis
                                    : existing.admission().publisherStartMillis(),
                            existing == null ? stoppedPublisher.session : existing.admission().publisherSession(),
                            root.toString(),
                            rootObject, stageObject, new ProfileClosedReconstruction.Image(pathObject, image.bytes(),
                                    image.sha256()), dependencyDigest(deps),
                            ProfileClosedReconstruction.closedDigest(expected, self), peers.revalidate());
                    files.requireHealthy();
                    return reservations.admitClosedReconstruction(metadata, new PreparedAdmission(request, expected));
                }
            }
        });
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

    private static void requireDrained(SegmentOwnedFiles files) throws Exception {
        var result = files.drain();
        if (!result.drained()) {
            throw new IOException("Reconstruction readers did not drain", result.closeFailure());
        }
    }

    private SegmentClosedReconstruction() { }
}

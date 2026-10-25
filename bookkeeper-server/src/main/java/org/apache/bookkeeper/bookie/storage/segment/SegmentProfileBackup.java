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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.discover.profile.ProfileInitialControlClient;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;

/**
 * Explicit offline operations bound by {@link SegmentProfileService#offlineBackup} to a historical source.
 * Capture and verified image opening perform synchronous file work and must run outside event loops.
 * Images and their external references are historical input, never current metadata or writer authority.
 */
public final class SegmentProfileBackup {
    private static final long REFERENCE_MAGIC = 0x5557425245463031L;
    private static final int REFERENCE_BYTES = 56;

    private final IsolatedSegmentConfig config;
    private final SegmentFileHeader identity;
    private final SecretKey key;
    private final ProfileDescriptorValidator validator;
    private final long controlBytes;
    private final int maximumEntries;
    private final String clusterScope;
    private final String deviceScope;

    SegmentProfileBackup(IsolatedSegmentConfig config, SegmentFileHeader identity, SecretKey key,
            ProfileDescriptorValidator validator, long controlBytes, int maximumEntries,
            String clusterScope, String deviceScope) {
        this.config = Objects.requireNonNull(config, "config");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.key = Objects.requireNonNull(key, "key");
        this.validator = Objects.requireNonNull(validator, "validator");
        if (controlBytes < 4096 + 320 || maximumEntries < 1) {
            throw new IllegalArgumentException("Explicit bounded backup inventory required");
        }
        checkScope(clusterScope);
        checkScope(deviceScope);
        this.controlBytes = controlBytes;
        this.maximumEntries = maximumEntries;
        this.clusterScope = clusterScope;
        this.deviceScope = deviceScope;
    }

    /**
     * Copy a stopped, exclusively locked source into a new image directory, then force its original UUID/SHA
     * reference into a new file outside both source and image. Both output parents must already exist.
     * No output is overwritten or removed. Copy, reference write, force or parent-force failure leaves an
     * uncertain output for inspection; it does not report a successful backup or authorize automatic cleanup.
     */
    public void capture(Path imageDirectory, Path referencePath) throws IOException {
        Path source = config.directory().toRealPath();
        Path image = resolveParent(imageDirectory);
        Path reference = resolveParent(referencePath);
        if (image.startsWith(source) || reference.startsWith(source) || reference.startsWith(image)
                || Files.exists(image, LinkOption.NOFOLLOW_LINKS)
                || Files.exists(reference, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Backup requires new independent image and external reference paths");
        }
        var captured = SegmentBackupImage.capture(config, identity, key, validator, controlBytes, maximumEntries,
                clusterScope, deviceScope, image);
        ByteBuffer bytes = ByteBuffer.allocate(REFERENCE_BYTES).putLong(REFERENCE_MAGIC)
                .putLong(captured.imageId().getMostSignificantBits())
                .putLong(captured.imageId().getLeastSignificantBits())
                .put(HexFormat.of().parseHex(captured.digest())).flip();
        try (var file = FileChannel.open(reference, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS)) {
            var io = new SegmentFileIo();
            io.writeFully(file, bytes, 0);
            io.force(file, true);
        }
        IsolatedSegmentFiles.syncDirectory(reference.getParent());
    }

    /**
     * Verify a historical image and import a CLOSED fragment through current metadata/RepairIntent admission.
     * Target setup must use a new directory, BookieId and storage incarnation; normal startup retains its
     * snapshot, gapless suffix and required-through checks. Retry reuses the frozen accepted/prepared intent
     * and closeId. The original source directory need not remain available.
     *
     * <p>The returned result retains coordinator cancellation semantics: cancellation stops future work while
     * submitted I/O drains. Completion follows actual child completion and image close, including close failure.
     * Mutating either returned future cannot close the image early or cancel work already using its buffers.
     */
    public ProfileRecoveryCoordinator.RepairAttempt beginRestoreClosedFragment(Path imageDirectory,
            Path referencePath, ProfileRecoveryCoordinator coordinator,
            ZKProfileReservationStore.ReadOnlySnapshot accepted, ZKProfileReservationStore.RepairSnapshot prepared,
            ClientContext sdk, byte[] descriptor, ProtectedProfileCredential credential,
            Map<BookieId, ProfileNormalDataClient> data, Map<String, ProfileInitialControlClient> controls,
            byte[] closeId) throws IOException {
        Objects.requireNonNull(coordinator, "coordinator");
        Path imageDirectoryReal = Objects.requireNonNull(imageDirectory, "imageDirectory").toRealPath();
        Path reference = resolveParent(referencePath);
        if (reference.startsWith(imageDirectoryReal)
                || reference.startsWith(config.directory().toAbsolutePath().normalize())
                || Files.exists(config.directory()) && reference.startsWith(config.directory().toRealPath())) {
            throw new IOException("Backup reference must remain outside source and image");
        }
        var image = SegmentBackupImage.open(config, identity, key, validator, controlBytes, maximumEntries,
                clusterScope, deviceScope, imageDirectoryReal, readReference(reference));
        ProfileRecoveryCoordinator.RepairAttempt core;
        try {
            core = coordinator.beginRestoreClosedFragment(accepted, prepared, sdk, descriptor, credential,
                    data, controls, closeId, image);
        } catch (RuntimeException | Error failure) {
            try {
                image.close();
            } catch (IOException | RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        var terminal = new CompletableFuture<Void>();
        // The original coordinator completion is private: callers only receive our independent terminal copy.
        core.completion().whenComplete((unused, failure) -> {
            Throwable terminalFailure = failure;
            try {
                image.close();
            } catch (IOException | RuntimeException | Error cleanup) {
                if (terminalFailure == null) {
                    terminalFailure = cleanup;
                } else {
                    terminalFailure.addSuppressed(cleanup);
                }
            }
            if (terminalFailure == null) {
                terminal.complete(null);
            } else {
                terminal.completeExceptionally(terminalFailure);
            }
        });
        return new ProfileRecoveryCoordinator.RepairAttempt(core.result(), terminal.copy());
    }

    private static SegmentBackupImage.Reference readReference(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Backup reference must be a regular external file without symlinks");
        }
        ByteBuffer bytes = ByteBuffer.allocate(REFERENCE_BYTES);
        try (var file = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (file.size() != REFERENCE_BYTES) {
                throw new IOException("Backup reference length differs");
            }
            SegmentFileIo.readFully(file, bytes, 0);
            if (file.size() != REFERENCE_BYTES) {
                throw new IOException("Backup reference length changed");
            }
        }
        if (bytes.getLong() != REFERENCE_MAGIC) {
            throw new IOException("Unsupported backup reference format");
        }
        UUID imageId = new UUID(bytes.getLong(), bytes.getLong());
        byte[] digest = new byte[32];
        bytes.get(digest);
        boolean zeroDigest = true;
        for (byte value : digest) {
            zeroDigest &= value == 0;
        }
        if (imageId.equals(new UUID(0, 0)) || zeroDigest) {
            throw new IOException("Backup reference must bind a nonzero image and digest");
        }
        return new SegmentBackupImage.Reference(imageId, HexFormat.of().formatHex(digest));
    }

    private static Path resolveParent(Path path) throws IOException {
        Path absolute = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        if (absolute.getParent() == null || absolute.getFileName() == null) {
            throw new IOException("Explicit image/reference child path required");
        }
        return absolute.getParent().toRealPath().resolve(absolute.getFileName());
    }

    private static void checkScope(String value) {
        if (value == null || value.isEmpty() || value.length() > 255 || value.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Explicit bounded cluster/Profile and device scopes required");
        }
    }
}

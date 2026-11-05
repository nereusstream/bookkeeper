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

import com.sun.security.auth.module.UnixSystem;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicHandoffPaths.Identity;
import org.apache.bookkeeper.discover.BookieRouteBinding;

/** Owned material I/O and signed-scope comparison only. Does not establish current native/Unix adoption authority. */
final class SegmentClassicHandoffMaterials {
    record References(Path directory, Path proof, Path policy, Path aesKey) {
        References {
            directory = path(directory);
            proof = path(proof);
            policy = path(policy);
            aesKey = path(aesKey);
            if (proof.equals(policy) || proof.equals(aesKey) || policy.equals(aesKey)) {
                throw new IllegalArgumentException("Handoff materials require distinct file references");
            }
        }
    }

    record Expected(String deployment, SegmentClassicHandoff.Digest programConfiguration,
                    SegmentClassicHandoff.Digest signerKeyId, PublicKey trustedSigner,
                    BookieRouteBinding.Observation nativeScope, SegmentClassicHandoff.Requirements requirements,
                    long oldUid, long targetUid, long materialOwnerUid) {
        Expected {
            Objects.requireNonNull(deployment, "deployment");
            Objects.requireNonNull(programConfiguration, "programConfiguration");
            Objects.requireNonNull(signerKeyId, "signerKeyId");
            Objects.requireNonNull(trustedSigner, "trustedSigner");
            Objects.requireNonNull(nativeScope, "nativeScope");
            Objects.requireNonNull(requirements, "requirements");
            if (deployment.isBlank() || oldUid <= 0 || oldUid > 0xffff_ffffL || oldUid == targetUid
                    || targetUid < 0 || targetUid > 0xffff_ffffL || materialOwnerUid < 0
                    || materialOwnerUid > 0xffff_ffffL || materialOwnerUid == oldUid) {
                throw new IllegalArgumentException("Invalid externally supplied handoff material scope");
            }
        }
    }

    /** Values remain internal; the outer owner must keep this child until its actual key/scope users terminate. */
    record Parsed(SegmentClassicHandoff.Proof proof, SegmentClassicHandoff.Policy policy, SecretKey key) { }

    record Result(boolean drained, Throwable initializationFailure, SegmentOwnedFiles.Result files,
                  SegmentDataFileLock.DrainResult guard) { }

    private record ClassicGuard(Path current, Identity rootIdentity, Identity currentIdentity, Object fileKey) { }

    private final References references;
    private final Expected expected;
    private final SegmentArenaManifest manifest;
    private final Path guardPath;
    private final Path intentPath;
    private final SegmentDataFileLock guard;
    private final SegmentOwnedFiles files;
    private final SegmentClassicHandoffPaths paths;
    private boolean attempted;
    private boolean closing;
    private Thread initializingThread;
    private Thread closingThread;
    private Throwable initializationFailure;
    private Parsed parsed;
    private List<ClassicGuard> classicGuards;

    /** Heap-only preparation. Materials, metadata, directories and locks open after the caller retains this owner. */
    SegmentClassicHandoffMaterials(References references, Expected expected, SegmentArenaManifest manifest) {
        this.references = Objects.requireNonNull(references, "references");
        this.expected = Objects.requireNonNull(expected, "expected");
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        String stem = scopeName(expected.nativeScope().binding());
        guardPath = references.directory().resolve(stem + ".lock");
        intentPath = references.directory().resolve(stem + ".retirement");
        guard = SegmentDataFileLock.existingControlOwner(guardPath);
        var excluded = new ArrayList<Path>();
        excluded.add(guardPath);
        excluded.add(manifest.controlDirectory().resolve("bookie-control.lock"));
        for (var arena : manifest.arenas()) {
            excluded.add(arena.directory().resolve("isolated.lock"));
            excluded.add(arena.directory().resolve("arena-control.lock"));
        }
        files = new SegmentOwnedFiles(excluded);
        paths = new SegmentClassicHandoffPaths(expected.oldUid(), expected.targetUid(), expected.materialOwnerUid());
    }

    /** The path is fixed by cluster birth and Bookie, never by attempt PID, episode or bindingId. */
    static String scopeName(BookieRouteBinding binding) {
        return scopeName(new SegmentClassicHandoff.SupervisorScope(
                binding.clusterId(), binding.clusterBirthZxid(), binding.bookieId()));
    }

    static String scopeName(SegmentClassicHandoff.SupervisorScope binding) {
        byte[] cluster = binding.clusterId().getBytes(StandardCharsets.UTF_8);
        byte[] bookie = binding.bookieId().toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer bytes = ByteBuffer.allocate(16 + cluster.length + bookie.length)
                .putInt(cluster.length).put(cluster).putLong(binding.clusterBirthZxid())
                .putInt(bookie.length).put(bookie);
        return "scope-" + HexFormat.of().formatHex(SegmentClassicHandoff.digest(
                "BookKeeper/CommonClassicSupervisor/v1\0".getBytes(StandardCharsets.US_ASCII), bytes.array()).value());
    }

    synchronized void initialize() throws IOException {
        if (attempted || closing) {
            throw new IOException("Handoff material initialization is one-use");
        }
        attempted = true;
        initializingThread = Thread.currentThread();
        byte[] rawKey = null;
        try {
            if (new UnixSystem().getUid() != expected.targetUid()) {
                throw new IOException("Handoff material reader is not the expected actual target UID");
            }
            paths.requireDirectory(references.directory());
            Identity guardIdentity = paths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES);
            var guardParents = paths.parents(guardPath);
            guard.initialize();
            if (!guardIdentity.equals(paths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES))
                    || !guardParents.equals(paths.parents(guardPath))) {
                throw new IOException("Handoff supervisor identity changed during acquisition");
            }
            var proof = SegmentClassicHandoff.verify(read(references.proof(), 112, SegmentClassicHandoff.MAX_BYTES),
                    expected.signerKeyId(), expected.trustedSigner());
            var policy = SegmentClassicHandoff.decodePolicy(read(references.policy(), 12, 512));
            byte[] intent = read(intentPath, 12, SegmentClassicHandoff.MAX_BYTES);
            var retirement = SegmentClassicHandoff.decodeRetirement(intent);
            if (!proof.retirement().equals(SegmentClassicHandoff.digest(intent))
                    || !retirement.operation().equals(proof.operation())
                    || !retirement.members().equals(proof.members().stream()
                            .map(SegmentClassicHandoff.Member::physical).toList())) {
                throw new IOException("Original handoff retirement record differs from the signed proof");
            }
            requireExpected(proof.operation());
            rawKey = read(references.aesKey(), 32, 32);
            SegmentClassicHandoff.requireMaterials(proof, policy, rawKey, manifest);
            checkClassicGuards(allMaterialPaths());
            files.requireHealthy();
            parsed = new Parsed(proof, policy, new SecretKeySpec(rawKey, "AES"));
        } catch (IOException | RuntimeException | Error failure) {
            initializationFailure = failure;
            throw failure;
        } finally {
            if (rawKey != null) {
                Arrays.fill(rawKey, (byte) 0);
            }
            initializingThread = null;
        }
    }

    private void requireExpected(SegmentClassicHandoff.Operation operation) throws IOException {
        var original = operation.prepared();
        var current = expected.nativeScope();
        if (!operation.deployment().equals(expected.deployment())
                || !operation.programConfiguration().equals(expected.programConfiguration())
                || operation.oldUid() != expected.oldUid() || operation.targetUid() != expected.targetUid()
                || !operation.requirements().equals(expected.requirements())
                || !original.birthId().equals(current.birthId()) || original.birthZxid() != current.birthZxid()
                || !original.binding().equals(current.binding().prepared())) {
            throw new IOException("Signed handoff differs from the supplied expected source and deployment");
        }
    }

    private byte[] read(Path path, int minimum, int maximum) throws IOException {
        byte[] bytes = null;
        try {
            Identity before = paths.privateFile(path, minimum, maximum);
            var beforeParents = paths.parents(path);
            checkClassicGuards(List.of(path));
            try (FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ,
                    LinkOption.NOFOLLOW_LINKS)) {
                long length = channel.size();
                if (length != before.size() || length < minimum || length > maximum) {
                    throw new IOException("Handoff material length changed during open");
                }
                bytes = new byte[(int) length];
                SegmentFileIo.readFully(channel, ByteBuffer.wrap(bytes), 0);
                if (channel.size() != length) {
                    throw new IOException("Handoff material length changed during read");
                }
            }
            if (!before.equals(paths.privateFile(path, minimum, maximum))
                    || !beforeParents.equals(paths.parents(path))) {
                throw new IOException("Handoff material identity or parent chain changed during read");
            }
            files.requireHealthy();
            return bytes;
        } catch (IOException | RuntimeException | Error failure) {
            if (bytes != null) {
                Arrays.fill(bytes, (byte) 0);
            }
            throw failure;
        }
    }

    private List<Path> allMaterialPaths() {
        return List.of(references.proof(), references.policy(), intentPath, references.aesKey());
    }

    private void checkClassicGuards(List<Path> materialPaths) throws IOException {
        var observed = new ArrayList<ClassicGuard>(manifest.classicRoots().size());
        boolean missing = false;
        for (int index = 0; index < manifest.classicRoots().size(); index++) {
            Path root = manifest.classicRoots().get(index).directory();
            Path current = root.resolve("current");
            Identity rootIdentity = directoryIdentity(root);
            Identity currentIdentity = directoryIdentity(current);
            Path lock = current.resolve(SegmentClassicRootBindings.LOCK_NAME);
            Object fileKey = guardKey(lock);
            var observation = new ClassicGuard(current, rootIdentity, currentIdentity, fileKey);
            if (classicGuards != null) {
                var previous = classicGuards.get(index);
                if (!previous.rootIdentity().equals(rootIdentity) || !previous.currentIdentity().equals(currentIdentity)
                        || previous.fileKey() != null && !previous.fileKey().equals(fileKey)) {
                    throw new IOException("Observed Classic guard or its parent changed: " + lock);
                }
            }
            if (fileKey != null) {
                for (Path material : materialPaths) {
                    if (Files.isSameFile(material, lock)) {
                        throw new IOException("Handoff material aliases a Classic guard: " + material);
                    }
                }
            }
            missing |= fileKey == null;
            observed.add(observation);
        }
        if (missing) {
            if (expected.nativeScope().binding().phase() != BookieRouteBinding.Phase.PREPARED) {
                throw new IOException("A missing Classic guard requires native PREPARED");
            }
            Path control = manifest.controlDirectory();
            requireAbsentMarker(control, directoryIdentity(control));
            for (var member : observed) {
                requireAbsentMarker(member.current(), member.currentIdentity());
            }
        }
        for (var member : observed) {
            Path lock = member.current().resolve(SegmentClassicRootBindings.LOCK_NAME);
            if (!Objects.equals(member.fileKey(), guardKey(lock))
                    || !member.currentIdentity().equals(directoryIdentity(member.current()))
                    || !member.rootIdentity().equals(directoryIdentity(member.current().getParent()))) {
                throw new IOException("Classic guard or its parent changed during lookup: " + lock);
            }
        }
        classicGuards = List.copyOf(observed);
    }

    private static Object guardKey(Path lock) throws IOException {
        var attributes = leaf(lock);
        if (attributes == null) {
            return null;
        }
        Object fileKey = attributes.fileKey();
        if (!attributes.isRegularFile() || fileKey == null) {
            throw new IOException("Existing Classic guard must be an identifiable regular file: " + lock);
        }
        return fileKey;
    }

    private static Identity directoryIdentity(Path directory) throws IOException {
        Identity identity = SegmentClassicHandoffPaths.fileIdentity(directory);
        if (!identity.directory()) {
            throw new IOException(
                    "Classic guard or marker parent must be an existing canonical directory: " + directory);
        }
        return identity.parent();
    }

    private static void requireAbsentMarker(Path directory, Identity original) throws IOException {
        Path marker = directory.resolve(SegmentClassicRootBindings.FILE_NAME);
        if (leaf(marker) != null || !original.equals(directoryIdentity(directory))) {
            throw new IOException("Missing Classic guard requires an unchanged, unmarked root: " + marker);
        }
    }

    private static BasicFileAttributes leaf(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException absent) {
            if (!path.toString().equals(absent.getFile()) || absent.getOtherFile() != null) {
                throw absent;
            }
            return null;
        }
    }

    private static Path path(Path path) {
        Objects.requireNonNull(path, "path");
        if (!path.isAbsolute() || !path.normalize().equals(path) || path.getParent() == null) {
            throw new IllegalArgumentException("Explicit normalized handoff material path required");
        }
        return path;
    }

    synchronized Parsed parsed() throws IOException {
        if (parsed == null || closing || !files.healthy()) {
            throw new IOException("Handoff material data is not available", initializationFailure);
        }
        return parsed;
    }

    /** The same preparation owner calls this after its second cold read, before publishing input association. */
    synchronized void recheckClassicGuards() throws IOException {
        parsed();
        checkClassicGuards(allMaterialPaths());
    }

    /** Outer native/Unix checks and all actual users must terminate before releasing this child. */
    synchronized Result closeAndDrain() throws InterruptedException {
        if (initializingThread == Thread.currentThread() || closingThread == Thread.currentThread()) {
            throw new IllegalStateException("Handoff material activity cannot drain itself");
        }
        closing = true;
        closingThread = Thread.currentThread();
        try {
            var result = files.drain();
            if (result.drained()) {
                try {
                    guard.close();
                } catch (Throwable ignored) {
                    // The original guard retains its actual first release/close results.
                }
            }
            var guardResult = guard.drainResult();
            boolean drained = result.drained() && guardResult.drained();
            if (drained) {
                parsed = null;
            }
            return new Result(drained, initializationFailure, result, guardResult);
        } finally {
            closingThread = null;
        }
    }
}

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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicHandoffPaths.Identity;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;

/** Fixed-file publication only. The launcher owns process/metadata/Unix authority and the original guard. */
final class SegmentClassicHandoffPublication {
    record Access(long issuerUid, long oldUid, long targetUid, long materialOwnerUid) {
        Access {
            for (long uid : new long[]{issuerUid, oldUid, targetUid, materialOwnerUid}) {
                if (uid < 0 || uid > 0xffff_ffffL) {
                    throw new IllegalArgumentException("Explicit unsigned Unix identities required");
                }
            }
            if (oldUid == 0 || oldUid == issuerUid || oldUid == targetUid || oldUid == materialOwnerUid
                    || issuerUid != 0 && issuerUid != targetUid && issuerUid != materialOwnerUid) {
                throw new IllegalArgumentException("Old identity cannot own the handoff publisher");
            }
        }
    }

    record State(boolean retirementPresent, boolean proofPresent, boolean retirementDurable, boolean proofDurable,
                 Throwable firstFailure) { }
    record Result(boolean drained, Throwable firstFailure, SegmentOwnedFiles.Result files) { }

    private static final class Stage {
        private final Path target;
        private byte[] bytes;
        private Path temporary;
        private Identity readyIdentity;
        private boolean attempted;
        private boolean durable;
        private boolean ownFinal;

        private Stage(Path target) {
            this.target = target;
        }
    }

    private final SegmentClassicHandoffMaterials.References references;
    private final SegmentClassicHandoff.SupervisorScope scope;
    private final Access access;
    private final SegmentClassicHandoff.Digest signerKeyId;
    private final PublicKey signer;
    private final SegmentDataFileLock guard;
    private final Path guardPath;
    private final SegmentOwnedFiles files;
    private final SegmentClassicHandoffPaths paths;
    private final SegmentFileIo io;
    private final NativeIOImpl nativeIo = new NativeIOImpl();
    private final Stage intent;
    private final Stage proof;
    private Identity directory;
    private Identity guardIdentity;
    private List<Identity> parents;
    private SegmentClassicHandoff.Retirement expected;
    private boolean initializationAttempted;
    private boolean initialized;
    private boolean closing;
    private Thread active;
    private Throwable firstFailure;

    /** Heap-only. The parent retains this child before I/O and keeps its guard through actual child drain. */
    SegmentClassicHandoffPublication(SegmentClassicHandoffMaterials.References references,
            SegmentClassicHandoff.SupervisorScope scope, Access access, SegmentClassicHandoff.Digest signerKeyId,
            PublicKey signer, SegmentDataFileLock guard, SegmentFileIo io) {
        this.references = Objects.requireNonNull(references, "references");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.access = Objects.requireNonNull(access, "access");
        this.signerKeyId = Objects.requireNonNull(signerKeyId, "signerKeyId");
        this.signer = Objects.requireNonNull(signer, "signer");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.io = Objects.requireNonNull(io, "io");
        String stem = SegmentClassicHandoffMaterials.scopeName(scope);
        guardPath = references.directory().resolve(stem + ".lock");
        intent = new Stage(references.directory().resolve(stem + ".retirement"));
        proof = new Stage(references.proof());
        if (!proof.target.getParent().equals(references.directory())) {
            throw new IllegalArgumentException("Proof and candidates must use the fixed supervisor directory");
        }
        for (Path path : List.of(references.proof(), references.policy(), references.aesKey())) {
            if (path.equals(guardPath) || path.equals(intent.target)) {
                throw new IllegalArgumentException("Materials cannot alias fixed supervisor file names");
            }
        }
        files = new SegmentOwnedFiles(List.of(guardPath));
        paths = new SegmentClassicHandoffPaths(access.oldUid(), access.targetUid(), access.materialOwnerUid());
    }

    synchronized void initialize() throws IOException {
        enter();
        try {
            if (initializationAttempted) {
                throw new IOException("Handoff publisher initialization is one-use");
            }
            initializationAttempted = true;
            if (new UnixSystem().getUid() != access.issuerUid()) {
                throw new IOException("Handoff publisher is not running as the expected actual issuer UID");
            }
            paths.requireDirectory(references.directory());
            guard.requireHeld(guardPath);
            guardIdentity = paths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES);
            if (guardIdentity.uid() != access.targetUid()) {
                throw new IOException("The pre-owned supervisor guard must belong to the target identity");
            }
            directory = paths.identity(references.directory()).parent();
            parents = paths.parents(references.directory());
            initialized = true;
            check();
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    synchronized State state() throws IOException {
        enter();
        try {
            check();
            boolean retired = present(intent.target);
            boolean published = present(proof.target);
            check();
            return new State(retired, published, intent.durable, proof.durable, firstFailure);
        } finally {
            active = null;
        }
    }

    synchronized Path retire(SegmentClassicHandoff.Retirement original) throws IOException {
        enter();
        try {
            requireExpected(original);
            publish(intent, true);
            return intent.target;
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    /** Only the original owner that actually created this intent may publish a new, externally signed packet. */
    synchronized Path publishProof(byte[] encoded) throws IOException {
        enter();
        try {
            check();
            if (expected == null || !intent.durable) {
                throw new IOException("Original retirement intent is not durably established");
            }
            matches(intent);
            if (encoded.length > SegmentClassicHandoff.MAX_BYTES) {
                throw new IOException("Handoff proof exceeds the fixed bound");
            }
            byte[] stable = encoded.clone();
            verifyProof(stable);
            bind(proof, stable);
            publish(proof, intent.ownFinal);
            return proof.target;
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    /** A successor reads final only; signed orphan candidates never become authority or a source to promote. */
    synchronized SegmentClassicHandoff.Proof recoverProof(SegmentClassicHandoff.Retirement original)
            throws IOException {
        enter();
        try {
            requireExpected(original);
            publish(intent, false);
            byte[] bytes = read(proof.target, 112, SegmentClassicHandoff.MAX_BYTES);
            var decoded = verifyProof(bytes);
            bind(proof, bytes);
            publish(proof, false);
            return decoded;
        } catch (IOException | RuntimeException | Error failure) {
            failed(failure);
            throw failure;
        } finally {
            active = null;
        }
    }

    private void requireExpected(SegmentClassicHandoff.Retirement original) throws IOException {
        check();
        var operation = Objects.requireNonNull(original, "original retirement").operation();
        var nativeScope = operation.prepared().binding();
        if (!scope.equals(new SegmentClassicHandoff.SupervisorScope(nativeScope.clusterId(),
                    nativeScope.clusterBirthZxid(), nativeScope.bookieId()))
                || operation.oldUid() != access.oldUid() || operation.targetUid() != access.targetUid()
                || !operation.signerKeyId().equals(signerKeyId) || expected != null && !expected.equals(original)) {
            throw new IOException("Retirement differs from the fixed publisher scope or original operation");
        }
        expected = original;
        bind(intent, SegmentClassicHandoff.encodeRetirement(original));
    }

    private SegmentClassicHandoff.Proof verifyProof(byte[] bytes) throws IOException {
        var decoded = SegmentClassicHandoff.verify(bytes, signerKeyId, signer);
        if (!decoded.operation().equals(expected.operation())
                || !decoded.members().stream().map(SegmentClassicHandoff.Member::physical).toList()
                    .equals(expected.members())) {
            throw new IOException("Signed proof differs from the original retirement operation and members");
        }
        return decoded;
    }

    private void bind(Stage stage, byte[] bytes) throws IOException {
        if (stage.bytes != null && !Arrays.equals(stage.bytes, bytes)) {
            throw new IOException("An original publication packet cannot be replaced or re-signed");
        }
        if (stage.bytes == null) {
            stage.bytes = bytes;
        }
    }

    private void publish(Stage stage, boolean allowNew) throws IOException {
        check();
        stage.durable = false;
        if (present(stage.target)) {
            matches(stage);
            syncDirectory();
            stage.durable = true;
            return;
        }
        if (!allowNew || stage.attempted) {
            throw new IOException(
                    "No original final exists; candidates are retained without promotion or rename retry");
        }
        stage.attempted = true;
        stage.temporary = references.directory().resolve(".handoff-candidate-" + UUID.randomUUID());
        Identity opened;
        try (FileChannel channel = SegmentOwnedFiles.open(files, stage.temporary,
                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
            opened = paths.privateFile(stage.temporary, 0, SegmentClassicHandoff.MAX_BYTES);
            io.writeFully(channel, ByteBuffer.wrap(stage.bytes), 0);
            if (access.issuerUid() != access.targetUid()) {
                // Only this owner's new file, before force/publication. Never repair an existing material or final.
                Files.setAttribute(stage.temporary, "unix:uid", (int) access.targetUid(), LinkOption.NOFOLLOW_LINKS);
            }
            io.force(channel, true);
        }
        files.requireHealthy();
        stage.readyIdentity = paths.privateFile(stage.temporary, stage.bytes.length, stage.bytes.length);
        if (!sameInode(opened, stage.readyIdentity) || stage.readyIdentity.uid() != access.targetUid()) {
            throw new IOException("Owned candidate identity changed before its first close completed");
        }
        check();
        if (!stage.readyIdentity.equals(paths.privateFile(stage.temporary, stage.bytes.length, stage.bytes.length))) {
            throw new IOException("Owned candidate changed before atomic publication");
        }
        if (nativeIo.renameNoReplace(stage.temporary.toString(), stage.target.toString()) != 0) {
            throw new IOException("Atomic publication did not report a definite successful rename");
        }
        matches(stage);
        syncDirectory();
        stage.durable = true;
    }

    private void matches(Stage stage) throws IOException {
        byte[] actual = read(stage.target, stage.bytes.length, stage.bytes.length);
        Identity identity = paths.privateFile(stage.target, stage.bytes.length, stage.bytes.length);
        if (!Arrays.equals(actual, stage.bytes) || stage.readyIdentity != null
                && !sameInode(stage.readyIdentity, identity)) {
            throw new IOException("Existing final is not the original publication; no overwrite or rollback allowed");
        }
        if (stage.readyIdentity != null) {
            stage.ownFinal = true;
        }
    }

    private byte[] read(Path path, int minimum, int maximum) throws IOException {
        check();
        Identity before = paths.privateFile(path, minimum, maximum);
        if (before.uid() != access.targetUid()) {
            throw new IOException("Published file does not belong to its exact target identity");
        }
        byte[] bytes = new byte[(int) before.size()];
        try (FileChannel channel = SegmentOwnedFiles.open(files, path,
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (channel.size() != bytes.length) {
                throw new IOException("Published file changed during open");
            }
            SegmentFileIo.readFully(channel, ByteBuffer.wrap(bytes), 0);
            if (channel.size() != bytes.length) {
                throw new IOException("Published file changed during read");
            }
        }
        if (!before.equals(paths.privateFile(path, minimum, maximum))) {
            throw new IOException("Published file identity changed during read or actual close");
        }
        check();
        return bytes;
    }

    private void syncDirectory() throws IOException {
        check();
        try (FileChannel channel = SegmentOwnedFiles.open(files, references.directory(),
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            io.force(channel, true);
        }
        check();
    }

    private static boolean sameInode(Identity first, Identity second) {
        return first.device() == second.device() && first.inode() == second.inode();
    }

    private boolean present(Path path) throws IOException {
        try {
            Files.readAttributes(path, "basic:isRegularFile", LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException absent) {
            check();
            return false;
        }
    }

    private void check() throws IOException {
        if (!initialized || closing) {
            throw new IOException("Handoff publisher is not active");
        }
        files.requireHealthy();
        guard.requireHeld(guardPath);
        if (!directory.equals(paths.identity(references.directory()).parent())
                || !parents.equals(paths.parents(references.directory()))
                || !guardIdentity.equals(paths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES))) {
            throw new IOException("Protected publication directory, parent chain or supervisor identity changed");
        }
    }

    private void enter() throws IOException {
        if (closing || active != null) {
            throw new IOException("Publisher is closing or an actual operation is already active");
        }
        active = Thread.currentThread();
    }

    private void failed(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        }
    }

    /** The parent must stop all actual users first, drain this child, and only then release its original guard. */
    synchronized Result closeAndDrain() throws InterruptedException {
        if (active == Thread.currentThread()) {
            throw new IllegalStateException("Publication activity cannot drain itself");
        }
        closing = true;
        var result = files.drain();
        return new Result(result.drained(), firstFailure, result);
    }
}

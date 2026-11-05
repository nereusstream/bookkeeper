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

import java.io.IOException;
import java.nio.file.Path;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Objects;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapDrain;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapSnapshot;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapSources;

/** Binds cold inputs under the supervisor guard. Unix adoption and bootstrap authorization are separate steps. */
final class SegmentClassicBootstrapPreparation {
    record Trust(String deployment, SegmentClassicHandoff.Digest programConfiguration,
                 SegmentClassicHandoff.Digest signerKeyId, PublicKey signer,
                 long oldUid, long targetUid, long materialOwnerUid) { }

    record Drain(boolean drained, Throwable initializationFailure, ClassicBootstrapDrain sources,
                 SegmentClassicHandoffMaterials.Result materials) { }

    private final ZKRegistrationManager manager;
    private final ZKProfileStartupRequirementsStore requirements;
    private final SegmentArenaManifest manifest;
    private final SegmentClassicHandoffMaterials.References references;
    private final Trust trust;
    private final Duration readTimeout;
    private ClassicBootstrapSources sources;
    private SegmentClassicHandoffMaterials materials;
    private Thread initializing;
    private Thread draining;
    private boolean attempted;
    private boolean closing;
    private ClassicBootstrapSnapshot bound;
    private Throwable initializationFailure;
    private Drain lastDrain;

    /** No I/O before the enclosing cold caller retains this preparation owner. */
    SegmentClassicBootstrapPreparation(ZKRegistrationManager manager, ZKProfileStartupRequirementsStore requirements,
            SegmentArenaManifest manifest, SegmentClassicHandoffMaterials.References references, Trust trust,
            Duration readTimeout) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.requirements = Objects.requireNonNull(requirements, "requirements");
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.references = Objects.requireNonNull(references, "references");
        this.trust = Objects.requireNonNull(trust, "trust");
        this.readTimeout = Objects.requireNonNull(readTimeout, "readTimeout");
    }

    void initialize() throws IOException {
        synchronized (this) {
            if (attempted || closing) {
                throw new IOException("Classic bootstrap preparation is one-use");
            }
            attempted = true;
            initializing = Thread.currentThread();
        }
        try {
            sources = manager.borrowClassicBootstrapSources(requirements, manifest.bookieId(), readTimeout);
            requireOpen();
            var first = sources.read();
            requirePreparedScope(first.nativeBinding().binding());
            var commissioned = SegmentClassicHandoff.Requirements.from(
                    first.requirementsSource(), first.commissionedCompatibility());
            var expected = new SegmentClassicHandoffMaterials.Expected(trust.deployment(),
                    trust.programConfiguration(), trust.signerKeyId(), trust.signer(), first.nativeBinding(),
                    commissioned, trust.oldUid(), trust.targetUid(), trust.materialOwnerUid());
            materials = new SegmentClassicHandoffMaterials(references, expected, manifest);
            requireOpen();
            materials.initialize();
            var proof = materials.parsed().proof();
            requireMembers(proof);
            requireOpen();
            if (!first.equals(sources.read())) {
                throw new IOException("Classic bootstrap sources changed under the supervisor guard");
            }
            requireMembers(proof);
            materials.recheckClassicGuards();
            synchronized (this) {
                requireOpen();
                bound = first;
                initializing = null;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                initializationFailure = failure;
                initializing = null;
            }
            throw failure;
        }
    }

    private void requirePreparedScope(BookieRouteBinding binding) throws IOException {
        if (binding.phase() != BookieRouteBinding.Phase.PREPARED) {
            throw new IOException("Classic bootstrap preparation requires PREPARED; ENABLED recovery is separate");
        }
        if (manifest.classicRoots().isEmpty() || !binding.bookieId().equals(manifest.bookieId())
                || !binding.incarnation().equals(manifest.incarnation())
                || !binding.manifest().equals(manifest.identity())
                || !binding.controlDirectory().equals(manifest.controlDirectory().toString())) {
            throw new IOException("Classic bootstrap sources differ from the complete manifest scope");
        }
    }

    private void requireMembers(SegmentClassicHandoff.Proof proof) throws IOException {
        // The material owner has already verified the exact, ordered kind/index set against this manifest.
        for (var member : proof.members()) {
            requireOpen();
            var physical = member.physical();
            Path root = switch (physical.kind()) {
                case SegmentClassicHandoff.CLASSIC -> manifest.classicRoots().get(physical.member()).directory();
                case SegmentClassicHandoff.CONTROL -> manifest.controlDirectory();
                case SegmentClassicHandoff.ARENA -> manifest.arenas().get(physical.member()).directory();
                default -> throw new IOException("Unrecognized Classic bootstrap physical member");
            };
            var actual = SegmentClassicHandoffPaths.fileIdentity(root);
            if (!actual.directory() || actual.device() != physical.device() || actual.inode() != physical.inode()
                    || actual.uid() != member.uid() || actual.gid() != member.gid()
                    || (actual.mode() & 07777) != member.mode()) {
                throw new IOException("Classic bootstrap physical member differs from the signed scope: " + root);
            }
        }
    }

    private synchronized void requireOpen() throws IOException {
        if (closing) {
            throw new IOException("Classic bootstrap preparation is closing");
        }
    }

    /** Association of the original inputs only. There is no adoption or write permission. */
    synchronized boolean inputsBound() {
        return bound != null && !closing;
    }

    /**
     * Request close and report actual drain. A pending initializer or borrowed callback retains the material
     * owner and its guard. The borrowed manager, store and ZooKeeper client remain with their original owner.
     */
    Drain closeAndDrain() throws InterruptedException {
        synchronized (this) {
            Thread caller = Thread.currentThread();
            if (caller == initializing || caller == draining) {
                throw new IllegalStateException("Classic bootstrap preparation cannot drain its own activity");
            }
            closing = true;
            bound = null;
            if (initializing != null || draining != null) {
                return new Drain(false, initializationFailure, null, null);
            }
            if (lastDrain != null && (lastDrain.drained() || lastDrain.materials() != null)) {
                return lastDrain;
            }
            draining = caller;
        }

        try {
            var sourceResult = sources == null ? null : sources.closeAndDrain();
            boolean sourcesDrained = sourceResult == null || sourceResult.drained();
            var materialResult = sourcesDrained && materials != null ? materials.closeAndDrain() : null;
            boolean drained = sourcesDrained && (materialResult == null || materialResult.drained());
            synchronized (this) {
                lastDrain = new Drain(drained, initializationFailure, sourceResult, materialResult);
                return lastDrain;
            }
        } finally {
            synchronized (this) {
                draining = null;
            }
        }
    }
}

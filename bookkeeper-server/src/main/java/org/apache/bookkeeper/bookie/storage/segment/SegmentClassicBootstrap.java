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
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.bookie.ClassicBootstrapStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapDrain;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapSnapshot;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore.ClassicBootstrapSources;

/** Offline Classic bootstrap lifecycle. Cold verification does not establish Unix adoption or write authority. */
public final class SegmentClassicBootstrap {
    record Trust(String deployment, SegmentClassicHandoff.Digest programConfiguration,
                 SegmentClassicHandoff.Digest signerKeyId, PublicKey signer,
                 long oldUid, long targetUid, long materialOwnerUid) { }

    record Drain(boolean drained, Throwable initializationFailure, ClassicBootstrapStorage.Drain inventory,
                 ClassicBootstrapDrain sources,
                 SegmentClassicHandoffMaterials.Result materials) { }

    private record Imported(SegmentClassicRootBindings roots, SegmentClassicArenaBindings arenas,
                            SegmentBookieControlLog.Owned control, SegmentBookieControlDriver driver, long cut) { }

    /** Private issuance for the concrete Bookie reader on this owner's active verification thread. */
    public static final class InventorySession {
        private final SegmentClassicBootstrap owner;
        private final ServerConfiguration configuration;
        private boolean consumed;

        private InventorySession(SegmentClassicBootstrap owner, ServerConfiguration configuration) {
            this.owner = owner;
            this.configuration = configuration;
        }

        public ServerConfiguration consume(ClassicBootstrapStorage candidate) throws IOException {
            synchronized (owner) {
                requireOwner(candidate);
                if (consumed) {
                    throw new IOException("Classic bootstrap inventory session is one-use");
                }
                consumed = true;
                return configuration;
            }
        }

        public void requireReader(ClassicBootstrapStorage candidate) throws IOException {
            synchronized (owner) {
                requireOwner(candidate);
                if (!consumed) {
                    throw new IOException("Classic bootstrap inventory session was not consumed");
                }
            }
        }

        private void requireOwner(ClassicBootstrapStorage candidate) throws IOException {
            owner.requireOpen();
            if (candidate != owner.inventory || owner.bound == null || owner.checking != Thread.currentThread()) {
                throw new IOException("Classic bootstrap inventory session belongs to another reader or episode");
            }
        }
    }

    /** Recovery authority for the concrete bridge and the exact applied control state from its import. */
    public static final class RecoverySession {
        private final SegmentClassicBootstrap owner;
        private final Imported imported;
        private final ServerConfiguration configuration;
        private boolean consumed;

        private RecoverySession(SegmentClassicBootstrap owner, Imported imported, ServerConfiguration configuration) {
            this.owner = owner;
            this.imported = imported;
            this.configuration = configuration;
        }

        public ServerConfiguration consume(ClassicBootstrapStorage bridge) throws IOException {
            synchronized (owner) {
                requireOwner(bridge);
                if (consumed) {
                    throw new IOException("Classic bootstrap recovery session is one-use");
                }
                consumed = true;
                return configuration;
            }
        }

        public void requireClaim(ClassicBootstrapStorage bridge, long ledgerId) throws IOException {
            synchronized (owner) {
                requireOwner(bridge);
                if (!consumed) {
                    throw new IOException("Classic bootstrap recovery session was not consumed");
                }
            }
            imported.control().log().requireAppliedBootstrapClaim(ledgerId, imported.cut());
        }

        private void requireOwner(ClassicBootstrapStorage bridge) throws IOException {
            owner.requireOpen();
            if (bridge != owner.inventory || imported != owner.imported || owner.checking != Thread.currentThread()) {
                throw new IOException("Classic bootstrap recovery session belongs to another owner or episode");
            }
        }
    }

    private final ZKRegistrationManager manager;
    private final ZKProfileStartupRequirementsStore requirements;
    private final SegmentArenaManifest manifest;
    private final SegmentClassicHandoffMaterials.References references;
    private final Trust trust;
    private final Duration readTimeout;
    private final ClassicBootstrapStorage inventory = ClassicBootstrapStorage.prepare();
    private ClassicBootstrapSources sources;
    private SegmentClassicHandoffMaterials materials;
    private SegmentClassicBootstrapClaims importer;
    private Imported imported;
    private boolean recovered;
    private Thread checking;
    private Thread draining;
    private boolean attempted;
    private boolean closing;
    private ClassicBootstrapSnapshot bound;
    private Throwable initializationFailure;
    private Drain lastDrain;

    /** No I/O before the enclosing cold caller retains this preparation owner. */
    SegmentClassicBootstrap(ZKRegistrationManager manager, ZKProfileStartupRequirementsStore requirements,
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
            checking = Thread.currentThread();
        }
        try {
            sources = manager.borrowClassicBootstrapSources(requirements, manifest.bookieId(), readTimeout);
            requireOpen();
            var first = sources.read();
            requireScope(first.nativeBinding().binding());
            var commissioned = SegmentClassicHandoff.Requirements.from(
                    first.requirementsSource(), first.commissionedCompatibility());
            var expected = new SegmentClassicHandoffMaterials.Expected(trust.deployment(),
                    trust.programConfiguration(), trust.signerKeyId(), trust.signer(), first.nativeBinding(),
                    commissioned, trust.oldUid(), trust.targetUid(), trust.materialOwnerUid());
            materials = new SegmentClassicHandoffMaterials(references, expected, manifest);
            requireOpen();
            materials.initialize();
            requireCurrentSources(first, true);
            synchronized (this) {
                requireOpen();
                bound = first;
                checking = null;
            }
        } catch (IOException | RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    /** The caller retains the verified Arena, root and control owners until this scope actually drains. */
    void verifyEnabled(SegmentClassicRootBindings roots, SegmentClassicArenaBindings arenas,
            SegmentBookieControlLog.Owned control) throws IOException {
        var original = beginScopeOperation(BookieRouteBinding.Phase.ENABLED);
        try {
            var bookie = control.log();
            var seal = original.nativeBinding().binding().seal().orElseThrow();
            long before = roots.verifyEnabledOwner(manager, original.nativeBinding(), bookie);
            arenas.requireVerified(original.nativeBinding().binding().manifest());
            inventory.initialize(new InventorySession(this, roots.inventoryConfiguration()));
            inventory.scan(ledger -> {
                requireOpen();
                var applied = bookie.prepareBootstrapClaim(ledger).applied();
                if (applied.isEmpty() || applied.orElseThrow().sequence() > seal.appliedCut()) {
                    throw new IOException("Original bootstrap cut does not include the claim for ledger " + ledger);
                }
            });
            drainInventory();
            requireCurrentSources(original);
            arenas.requireVerified(original.nativeBinding().binding().manifest());
            if (before != bookie.requireSealedBootstrapCut(seal)) {
                throw new IOException("Bookie control changed during the sealed Classic inventory");
            }
            finishInventory();
        } catch (IOException | RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    /** The enclosing owner must hold the original old-writer fence, roots, Arena and control resources. */
    void importPrepared(SegmentClassicRootBindings roots, SegmentClassicArenaBindings arenas,
            SegmentBookieControlLog.Owned control, SegmentBookieControlDriver driver) throws IOException {
        var claims = new SegmentClassicBootstrapClaims(control, driver);
        ClassicBootstrapSnapshot original;
        synchronized (this) {
            original = beginScopeOperation(BookieRouteBinding.Phase.PREPARED);
            importer = claims;
        }
        try {
            var bookie = control.log();
            var binding = original.nativeBinding();
            roots.verifyPreparedOwner(manager, binding, bookie);
            arenas.requireVerified(binding.binding().manifest());
            requireCurrentSources(original, true);
            inventory.initialize(new InventorySession(this, roots.inventoryConfiguration()));
            claims.importClaims(visitor -> {
                inventory.scan(visitor::visit);
                drainInventory();
            });
            requireCurrentSources(original);
            arenas.requireVerified(binding.binding().manifest());
            long cut = bookie.unsealedBootstrapCut();
            synchronized (this) {
                requireOpen();
                imported = new Imported(roots, arenas, control, driver, cut);
                finishInventory();
            }
        } catch (IOException | RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    /** Seal only after the original claimed Classic files and Journal marks have actually drained. */
    void recoverPrepared() throws IOException, InterruptedException {
        ClassicBootstrapSnapshot original;
        Imported prepared;
        synchronized (this) {
            if (imported == null || recovered) {
                throw new IOException("Classic bootstrap recovery requires its completed claim import");
            }
            if (imported.driver().isOwnerThread()) {
                throw new IllegalStateException("Bookie control owner cannot wait for its own seal");
            }
            original = beginScopeOperation(BookieRouteBinding.Phase.PREPARED);
            prepared = imported;
        }
        try {
            var binding = original.nativeBinding();
            var bookie = prepared.control().log();
            prepared.roots().verifyPreparedOwner(manager, binding, bookie);
            prepared.arenas().requireVerified(binding.binding().manifest());
            requireCurrentSources(original, true);
            requireImportedCut(bookie, prepared);
            inventory.recover(new RecoverySession(this, prepared, prepared.roots().inventoryConfiguration()));
            requireCurrentSources(original);
            prepared.roots().verifyPreparedOwner(manager, binding, bookie);
            prepared.arenas().requireVerified(binding.binding().manifest());
            var seal = sealRecoveredClassic(prepared, bookie);
            requireCurrentSources(original);
            prepared.roots().verifyPreparedRootsForCompletion(manager, binding, bookie);
            prepared.arenas().requireVerified(binding.binding().manifest());
            if (bookie.requireSealedBootstrapCut(seal) != seal.sequence()) {
                throw new IOException("Bookie control advanced past the original Classic bootstrap seal");
            }
            synchronized (this) {
                requireOpen();
                recovered = true;
                checking = null;
            }
        } catch (IOException | InterruptedException | RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    private BookieRouteBinding.BootstrapSeal sealRecoveredClassic(Imported prepared, SegmentBookieControlLog bookie)
            throws IOException, InterruptedException {
        var command = bookie.prepareBootstrapSeal(prepared.cut());
        var seal = new BookieRouteBinding.BootstrapSeal(command.operationId(), prepared.cut() + 1, prepared.cut());
        var expected = new SegmentBookieControlLog.Result(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                seal.operationId(), seal.sequence(), 1, 0);
        var batch = new SegmentClassicBootstrapClaims.Batch();
        synchronized (this) {
            requireOpen();
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Classic bootstrap interrupted before seal admission");
            }
            if (!prepared.driver().submitChanges(List.of(command), batch::completed, batch.terminal::countDown)) {
                throw new IOException("Classic bootstrap seal was not admitted");
            }
        }
        var interrupted = batch.awaitTerminal();
        if (interrupted != null) {
            throw interrupted;
        }
        if (!List.of(expected).equals(batch.appliedResults())) {
            throw new IOException("Classic bootstrap seal result differs from the original applied cut");
        }
        return seal;
    }

    private static void requireImportedCut(SegmentBookieControlLog bookie, Imported prepared) throws IOException {
        if (bookie.unsealedBootstrapCut() != prepared.cut()) {
            throw new IOException("Bookie control changed after the Classic bootstrap inventory");
        }
    }

    /** The caller holds the old-writer fence and all original local owners through actual drain. */
    BookieRouteBinding.Observation completePrepared(SegmentClassicRootBindings roots,
            SegmentClassicArenaBindings arenas, SegmentBookieControlLog.Owned control) throws IOException {
        var original = beginScopeOperation(BookieRouteBinding.Phase.PREPARED);
        try {
            var bookie = control.log();
            var binding = original.nativeBinding();
            roots.verifyPreparedRootsForCompletion(manager, binding, bookie);
            arenas.requireVerified(binding.binding().manifest());
            var seal = bookie.bootstrapSeal().orElseThrow(
                    () -> new IOException("Native completion requires the retained Classic bootstrap seal"));
            long before = bookie.requireSealedBootstrapCut(seal);
            var proof = materials.parsed().proof();
            requireMembers(proof, false);
            materials.recheckProtection();
            requireOpen();
            var enabled = sources.completePrepared(original, seal);
            roots.verifyPreparedRootsForCompletion(manager, binding, bookie);
            arenas.requireVerified(binding.binding().manifest());
            requireMembers(proof, false);
            materials.recheckProtection();
            if (before != bookie.requireSealedBootstrapCut(seal)) {
                throw new IOException("Bookie control changed during native bootstrap completion");
            }
            synchronized (this) {
                requireOpen();
                bound = null;
                checking = null;
            }
            return enabled.nativeBinding();
        } catch (IOException | RuntimeException | Error failure) {
            fail(failure);
            throw failure;
        }
    }

    private synchronized ClassicBootstrapSnapshot beginScopeOperation(BookieRouteBinding.Phase phase)
            throws IOException {
        if (boundNative().binding().phase() != phase || checking != null) {
            throw new IOException("Classic bootstrap operation requires a bound " + phase + " scope");
        }
        checking = Thread.currentThread();
        return bound;
    }

    private void drainInventory() throws IOException {
        var drained = inventory.closeInventory();
        if (!drained.drained()) {
            throw new IOException("Classic bootstrap inventory has unresolved resources", drained.closeFailure());
        }
    }

    private synchronized void finishInventory() throws IOException {
        requireOpen();
        importer = null;
        checking = null;
    }

    private synchronized void fail(Throwable failure) {
        initializationFailure = failure;
        bound = null;
        importer = null;
        imported = null;
        checking = null;
    }

    private void requireCurrentSources(ClassicBootstrapSnapshot original) throws IOException {
        requireCurrentSources(original, false);
    }

    private void requireCurrentSources(ClassicBootstrapSnapshot original, boolean verifyStorageTree)
            throws IOException {
        var proof = materials.parsed().proof();
        requireMembers(proof, verifyStorageTree);
        requireOpen();
        if (!original.equals(sources.read())) {
            throw new IOException("Classic bootstrap sources changed under the supervisor guard");
        }
        requireMembers(proof, false);
        materials.recheckProtection();
    }

    private void requireScope(BookieRouteBinding binding) throws IOException {
        if (manifest.classicRoots().isEmpty() || !binding.bookieId().equals(manifest.bookieId())
                || !binding.incarnation().equals(manifest.incarnation())
                || !binding.manifest().equals(manifest.identity())
                || !binding.controlDirectory().equals(manifest.controlDirectory().toString())) {
            throw new IOException("Classic bootstrap sources differ from the complete manifest scope");
        }
    }

    private void requireMembers(SegmentClassicHandoff.Proof proof, boolean verifyStorageTree) throws IOException {
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
            var actual = materials.protectedMember(root);
            if (actual.device() != physical.device() || actual.inode() != physical.inode()
                    || actual.uid() != member.uid() || actual.gid() != member.gid()
                    || (actual.mode() & 07777) != member.mode()) {
                throw new IOException("Classic bootstrap physical member differs from the signed scope: " + root);
            }
            if (verifyStorageTree) {
                materials.verifyStorageTree(root, actual, physical, readTimeout.toNanos(), this::requireOpen);
            } else {
                materials.verifyStorageMount(root, actual, readTimeout.toNanos());
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

    /** Original observation for the root and control owner assembly, without adoption or write permission. */
    synchronized BookieRouteBinding.Observation boundNative() throws IOException {
        if (!inputsBound()) {
            throw new IOException("Classic bootstrap sources are not bound", initializationFailure);
        }
        return bound.nativeBinding();
    }

    /**
     * Request close and report actual drain. A pending initializer or borrowed callback retains the material
     * owner and its guard. The borrowed manager, store and ZooKeeper client remain with their original owner.
     */
    Drain closeAndDrain() throws InterruptedException {
        synchronized (this) {
            Thread caller = Thread.currentThread();
            if (caller == checking || caller == draining) {
                throw new IllegalStateException("Classic bootstrap preparation cannot drain its own activity");
            }
            if (importer != null) {
                importer.stop(new IOException("Classic bootstrap preparation is closing"));
            }
            closing = true;
            bound = null;
            if (checking != null || draining != null) {
                return new Drain(false, initializationFailure, null, null, null);
            }
            if (lastDrain != null && (lastDrain.drained() || lastDrain.materials() != null)) {
                return lastDrain;
            }
            draining = caller;
        }

        try {
            var inventoryResult = inventory.closeAndDrain();
            var sourceResult = sources == null ? null : sources.closeAndDrain();
            boolean sourcesDrained = sourceResult == null || sourceResult.drained();
            boolean readersDrained = inventoryResult.drained() && sourcesDrained;
            var materialResult = readersDrained && materials != null ? materials.closeAndDrain() : null;
            boolean drained = readersDrained && (materialResult == null || materialResult.drained());
            synchronized (this) {
                lastDrain = new Drain(drained, initializationFailure, inventoryResult, sourceResult, materialResult);
                return lastDrain;
            }
        } finally {
            synchronized (this) {
                draining = null;
            }
        }
    }
}

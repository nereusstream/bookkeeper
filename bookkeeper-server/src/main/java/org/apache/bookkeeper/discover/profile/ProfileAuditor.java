/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.discover.profile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.replication.AuditorStats;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.versioning.Versioned;

/** Explicit cohort/descriptor membership observer. Scheduling is neither DATA absence nor repair authority. */
public final class ProfileAuditor {
    private final ZKProfileRegistrationStore registry;
    private final ZKProfileReservationStore reservations;
    private final ProfileLedgerMetadataStore metadata;
    private final List<String> cohort;
    private final ProfileDescriptorIdentity descriptor;
    private final org.apache.bookkeeper.client.ClientContext sdk;
    private final ProfileRecoveryCoordinator coordinator;
    private final ProfileRecoveryPeerPool pool;
    private final ProfileRecoveryCoordinator.WorkerOptions work;

    public ProfileAuditor(ZKProfileRegistrationStore registry, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, List<BookieId> cohort,
            ProfileRecoveryCoordinator.WorkerOptions work) {
        this(registry, reservations, metadata, cohort, work, null, null, null);
    }

    public ProfileAuditor(ZKProfileRegistrationStore registry, ZKProfileReservationStore reservations,
            ProfileLedgerMetadataStore metadata, List<BookieId> cohort,
            ProfileRecoveryCoordinator.WorkerOptions work, org.apache.bookkeeper.client.ClientContext sdk,
            ProfileRecoveryCoordinator coordinator, ProfileRecoveryPeerPool pool) {
        this.sdk = sdk;
        this.coordinator = coordinator;
        this.pool = pool;
        this.work = work;
        this.registry = registry;
        this.reservations = reservations;
        this.metadata = metadata;
        this.cohort = cohort.stream().map(Object::toString).toList();
        if (this.cohort.isEmpty() || this.cohort.size() > 64
                || this.cohort.stream().distinct().count() != this.cohort.size()) {
            throw new IllegalArgumentException("Profile auditor requires a bounded unique cohort");
        }
        var decoded = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.decode(
                work.descriptor(), work.validator());
        this.descriptor = org.apache.bookkeeper.common.profile.ProfileDescriptorCodec.identityOf(
                decoded, work.validator());
    }

    public List<String> cohort() {
        return cohort;
    }

    public AutoCloseable watch(Runnable changed) throws org.apache.zookeeper.KeeperException, InterruptedException {
        return registry.watchPeers(cohort, changed);
    }

    public List<String> availableBookies() throws BKException {
        var available = new ArrayList<String>();
        for (String name : cohort) {
            var peer = registry.readPeer(name);
            if (peer.status() == ProfileRegistrationStore.ReadStatus.NOT_FOUND) {
                continue;
            }
            if (peer.peer() == null || peer.peer().readiness().engine() != EngineProfile.SEGMENT_WAL_ENGINE) {
                throw BKException.create(BKException.Code.MetaStoreException);
            }
            available.add(name);
        }
        return available;
    }

    public void audit(LedgerManager manager, LedgerUnderreplicationManager tasks, long timeout,
            AuditorStats stats) throws IOException, BKException, InterruptedException,
                    ReplicationException {
        scan(manager, tasks, timeout, stats, false);
    }

    public void auditData(LedgerManager manager, LedgerUnderreplicationManager tasks, long timeout,
            AuditorStats stats) throws IOException, BKException, InterruptedException, ReplicationException {
        if (sdk == null || coordinator == null || pool == null) {
            throw new IllegalStateException("Profile DATA audit requires the service-owned recovery context");
        }
        scan(manager, tasks, timeout, stats, true);
    }

    private void scan(LedgerManager manager, LedgerUnderreplicationManager tasks, long timeout,
            AuditorStats stats, boolean data)
            throws IOException, BKException, InterruptedException, ReplicationException {
        stats.getStatsLogger().getCounter(data ? "profile_data_audit_runs" : "profile_audit_runs").inc();
        var peers = new HashMap<String, ZKProfileRegistrationStore.PeerReadResult>();
        for (String name : cohort) {
            peers.put(name, registry.readPeer(name));
        }
        // Stream original ledger ranges; keep one metadata/probe at a time, not another whole-ledger index.
        var ranges = manager.getLedgerRanges(timeout);
        while (ranges.hasNext()) {
            for (long ledger : ranges.next().getLedgers()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Profile membership audit interrupted");
                }
                Versioned<LedgerMetadata> value;
                try {
                    value = FutureUtils.result(manager.readLedgerMetadata(ledger), BKException.HANDLER);
                } catch (BKException absent) {
                    if (absent.getCode() == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                        continue;
                    }
                    throw absent;
                }
                if (!value.getValue().getCustomMetadata().containsKey(ProfileMetadataBacklink.KEY)) {
                    continue; // Explicit Profile service does not infer Classic failures from its cohort.
                }
                if (data && !value.getValue().isClosed()) {
                    continue; // Match the original replicas-check CLOSED scope; do not fence an active writer.
                }
                var missing = missing(value, peers);
                if (data) {
                    if (missing != null && missing.isEmpty()) {
                        auditClosedData(value, tasks, stats);
                    } else {
                        stats.getStatsLogger().getCounter("profile_data_audit_deferred").inc();
                    }
                } else if (missing == null) {
                    stats.getStatsLogger().getCounter("profile_audit_deferred").inc();
                } else if (!missing.isEmpty()) {
                    FutureUtils.result(tasks.markLedgerUnderreplicatedAsync(ledger, missing),
                            ReplicationException.EXCEPTION_HANDLER);
                    stats.getStatsLogger().getCounter("profile_audit_suspected").inc();
                }
            }
        }
    }

    private void auditClosedData(Versioned<LedgerMetadata> expected, LedgerUnderreplicationManager tasks,
            AuditorStats stats) throws InterruptedException, ReplicationException {
        long ledger = expected.getValue().getLedgerId();
        var before = metadata.readMembership(ledger);
        var root = reservations.read(ledger);
        if (before.snapshot() == null || root.snapshot() == null
                || !before.snapshot().metadata().getVersion().equals(expected.getVersion())
                || !ProfileLedgerMetadataStore.sameMetadata(before.snapshot().metadata().getValue(),
                        expected.getValue())) {
            stats.getStatsLogger().getCounter("profile_data_audit_deferred").inc();
            return;
        }
        var acquisition = pool.acquire();
        ProfileRecoveryCoordinator.ReplicaHealthAttempt health = null;
        java.util.concurrent.CompletableFuture<ProfileRecoveryCoordinator.ReplicaHealthResult> actual = null;
        ProfileRecoveryCoordinator.ReplicaHealthResult result = null;
        boolean interrupted = false;
        try {
            var lease = acquisition.completion().get();
            var credential = org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential.of(1,
                    org.apache.bookkeeper.proto.checksum.DigestManager.generateMasterKey(
                            expected.getValue().getPassword()));
            health = coordinator.beginVerifyClosedReplicas(ledger, sdk, credential, lease.data(), lease.controls(),
                    work.maxCoordinates(), work.maxDuration(), null);
            actual = health.completion();
            result = actual.get();
        } catch (InterruptedException stopping) {
            interrupted = true;
            if (health != null) {
                health.stop();
            }
        } catch (java.util.concurrent.ExecutionException | java.security.NoSuchAlgorithmException failed) {
            stats.getStatsLogger().getCounter("profile_data_audit_deferred").inc();
        } finally {
            // A caller stop/deadline cannot release the coordinator's cold child or its peer reservations.
            if (actual != null) {
                for (;;) {
                    try {
                        actual.get();
                        break;
                    } catch (InterruptedException stopping) {
                        interrupted = true;
                        health.stop();
                    } catch (java.util.concurrent.ExecutionException failed) {
                        break;
                    }
                }
            }
            var drained = acquisition.closeAsync();
            for (;;) {
                try {
                    drained.get();
                    break;
                } catch (InterruptedException stopping) {
                    interrupted = true;
                } catch (java.util.concurrent.ExecutionException failed) {
                    throw new IllegalStateException("Profile DATA audit lease drain was not confirmed", failed);
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (interrupted) {
            throw new InterruptedException("Profile DATA audit stopped after actual child drain");
        }
        if (result == null) {
            return;
        }
        var fresh = metadata.readMembership(ledger);
        var freshRoot = reservations.read(ledger);
        if (fresh.snapshot() == null || freshRoot.snapshot() == null
                || fresh.snapshot().birthZxid() != before.snapshot().birthZxid()
                || !fresh.snapshot().metadata().getVersion().equals(before.snapshot().metadata().getVersion())
                || !ProfileLedgerMetadataStore.sameMetadata(fresh.snapshot().metadata().getValue(),
                        before.snapshot().metadata().getValue()) || !freshRoot.snapshot().equals(root.snapshot())) {
            stats.getStatsLogger().getCounter("profile_data_audit_deferred").inc();
            return;
        }
        if (result.status() == ProfileRecoveryCoordinator.ReplicaHealthStatus.UNDERREPLICATED) {
            var missing = result.sources().stream().filter(source -> source.outcome()
                    == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.MISSING
                        || source.outcome()
                            == org.apache.bookkeeper.client.ProfileRecoveryReadHandle.SourceOutcome.CORRUPT)
                    .map(source -> source.bookie().toString()).distinct().toList();
            if (!missing.isEmpty()) {
                FutureUtils.result(tasks.markLedgerUnderreplicatedAsync(ledger, missing),
                        ReplicationException.EXCEPTION_HANDLER);
                stats.getStatsLogger().getCounter("profile_data_audit_suspected").inc();
            }
        } else {
            stats.getStatsLogger().getCounter(result.status() == ProfileRecoveryCoordinator.ReplicaHealthStatus
                    .FULLY_REPLICATED ? "profile_data_audit_healthy" : "profile_data_audit_deferred").inc();
        }
    }

    // null is unresolved/out of scope, never an empty availability set or a proved missing DATA entry.
    private List<String> missing(Versioned<LedgerMetadata> expected,
            Map<String, ZKProfileRegistrationStore.PeerReadResult> peers) {
        long ledger = expected.getValue().getLedgerId();
        var before = metadata.readMembership(ledger);
        var root = reservations.read(ledger);
        if (before.snapshot() == null || root.snapshot() == null
                || !before.snapshot().metadata().getVersion().equals(expected.getVersion())
                || !ProfileLedgerMetadataStore.sameMetadata(before.snapshot().metadata().getValue(),
                        expected.getValue())) {
            return null;
        }
        var scope = root.snapshot();
        if (scope.lifecycle().state() == ProfileLifecycleHead.State.DELETE_INTENT) {
            return List.of();
        }
        if (!scope.reservation().backlink().equals(ProfileMetadataBacklink.from(expected.getValue()))
                || !scope.reservation().installs().get(0).descriptorIdentity().equals(descriptor)) {
            return null;
        }
        var page = reservations.readPublishedPage(ledger);
        var retained = reservations.retainedActivationTargets(ledger);
        if (page.page() == null || page.page().page().creatorToken() != expected.getValue().getCToken()
                || !page.page().page().matchesQuorum(expected.getValue())
                || retained.status() != ProfileControlStore.ReadStatus.FOUND) {
            return null;
        }
        var required = new HashSet<String>();
        expected.getValue().getAllEnsembles().values().forEach(ensemble ->
                ensemble.forEach(bookie -> required.add(bookie.toString())));
        if (!peers.keySet().containsAll(required)) {
            return null;
        }
        var missing = new ArrayList<String>();
        for (String name : required) {
            byte[] incarnation = null;
            for (var receipt : retained.targets()) {
                if (receipt.scope().targetBookie().equals(name)) {
                    byte[] current = receipt.scope().targetStorageIncarnation();
                    if (incarnation != null && !Arrays.equals(incarnation, current)) {
                        return null; // Same-ID rejoin is not silently adopted by an availability observer.
                    }
                    incarnation = current;
                }
            }
            if (incarnation == null) {
                return null;
            }
            var found = peers.get(name);
            var after = found.peer() == null ? registry.readPeer(name) : registry.revalidatePeer(found.peer());
            if (after.status() != found.status()) {
                return null;
            }
            if (after.status() == ProfileRegistrationStore.ReadStatus.NOT_FOUND) {
                missing.add(name);
            } else if (after.peer() == null || after.peer().readiness().engine() != EngineProfile.SEGMENT_WAL_ENGINE) {
                return null;
            } else if (!Arrays.equals(incarnation, after.peer().projection().storageIncarnation().value())) {
                missing.add(name);
            }
        }
        var fresh = metadata.readMembership(ledger);
        var freshRoot = reservations.read(ledger);
        if (fresh.snapshot() == null || freshRoot.snapshot() == null
                || fresh.snapshot().birthZxid() != before.snapshot().birthZxid()
                || !fresh.snapshot().metadata().getVersion().equals(before.snapshot().metadata().getVersion())
                || !ProfileLedgerMetadataStore.sameMetadata(fresh.snapshot().metadata().getValue(),
                        before.snapshot().metadata().getValue()) || !freshRoot.snapshot().equals(scope)) {
            return null;
        }
        return List.copyOf(missing);
    }
}

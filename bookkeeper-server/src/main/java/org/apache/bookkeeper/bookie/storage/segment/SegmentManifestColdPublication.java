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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.bookkeeper.bookie.storage.segment.IsolatedSegmentStorage.DeleteCatchUpAttempt;
import org.apache.bookkeeper.bookie.storage.segment.IsolatedSegmentStorage.DeleteCatchUpStatus;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.WritableRegistration;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.ManifestStartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupCrashPoint;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessHarness;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Outcome;
import org.apache.bookkeeper.discover.profile.ProfileDeleteDelivery;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;

/** Opaque original-graph input to the isolated fixed-manifest publisher. This does not grant admission. */
public final class SegmentManifestColdPublication {
    record Result(StartupReadinessResult phases, WritableRegistration reconciled) { }

    private final ManifestSegmentStorage owner;
    private final ManifestSegmentStorage.CheckedStartup checked;
    private final ManifestSegmentStorage.OwnedListener pending;
    private final ManifestSegmentStorage.PreflightEpisode episode;
    private final SegmentManifestStartupPolicy.PreOpen preflight;
    private final ZKProfileReservationStore.RetainedG1DeleteReplay retained;
    private final long deadline;
    private final ManifestStartupReadinessInput input;
    private final Path directory;
    private final ZKProfileRegistrationStore persistent;
    private final ZKProfileEphemeralRegistrationStore publisher;
    private final ZKProfileEphemeralRegistrationStore.Admission admission;
    private boolean started;
    private volatile boolean running;
    private volatile boolean bound;
    private volatile boolean retired;
    private volatile boolean demoted;
    private SegmentLocalReadinessStore ownedLocal;

    SegmentManifestColdPublication(ManifestSegmentStorage.ColdPublicationOwner original,
            ZKProfileRegistrationStore persistent,
            ZKProfileEphemeralRegistrationStore publisher, ZKProfileEphemeralRegistrationStore.Admission admission,
            Generation readiness, Generation capabilities, String endpoint, List<Integer> hints) {
        owner = original.storage;
        checked = original.checked;
        pending = original.pending;
        episode = original.episode;
        preflight = episode.preflight();
        retained = episode.retained();
        deadline = episode.deadline();
        directory = original.directory;
        this.persistent = Objects.requireNonNull(persistent, "persistent");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.admission = Objects.requireNonNull(admission, "admission");
        var facts = checked.facts();
        var policy = checked.policy();
        var old = policy.oldScope();
        var scope = new ManifestStartupReadinessInput.Scope(facts.bookie().toString(), facts.incarnation(),
                facts.manifest(), policy.roots(), old.episode(), old.bookieId(), old.user(), old.uid(), old.roots(),
                policy.newUid(), ManifestStartupReadinessInput.OldDisposition.valueOf(policy.oldDisposition().name()));
        var source = facts.source();
        input = new ManifestStartupReadinessInput(facts.commissioned(), checked.requiredDevices(), facts.recovery(),
                facts.rollback(), new ManifestStartupReadinessInput.Source(source.nodePath(), source.birthId(),
                        source.birthZxid(), new ManifestIdentity(source.sha256())), scope, readiness, capabilities,
                endpoint, hints);
    }

    public ZKProfileStartupRequirementsStore requirements() {
        return preflight.requirements;
    }

    public ZKProfileStartupRequirementsStore.Observation source() {
        return preflight.source;
    }

    public ZKProfileReservationStore reservations() {
        return preflight.reservations;
    }

    public ZKProfileReservationStore.RetainedG1DeleteReplay retained() {
        return retained;
    }

    public long deadline() {
        return deadline;
    }

    public ManifestStartupReadinessInput input() {
        return input;
    }

    public void requirePublisher(ZKProfileEphemeralRegistrationStore candidate,
            ZKProfileEphemeralRegistrationStore.Admission gate, ZKProfileRegistrationStore registration) {
        if (candidate != publisher || gate != admission || registration != persistent) {
            throw new IllegalArgumentException("Cold manifest scope belongs to another publisher or pending gate");
        }
    }

    public void requireOwnedAdmission(ZKProfileEphemeralRegistrationStore.Admission candidate) {
        if (pending == null || candidate != pending.admission || retired || !pending.listener.isListening()) {
            throw new IllegalArgumentException("Manifest publication has no matching owned pending listener");
        }
        pending.service.requirePrepared(this);
    }

    /** Match and read the local cut under the original graph gate. */
    public ProfileDeleteDelivery.Progress localProgress(long operationDeadline) {
        if (retired || operationDeadline > deadline) {
            throw new IllegalStateException("Fixed manifest cold publication retired or expired");
        }
        try {
            return ProfileDeleteDelivery.Progress.fromEvent(
                    owner.requireOriginalGraph(checked, operationDeadline, pending));
        } catch (IOException unavailable) {
            throw new IllegalStateException("Fixed manifest original graph is no longer available", unavailable);
        }
    }

    private synchronized void start() {
        if (started || retired) {
            throw new IllegalStateException("Fixed manifest cold publication is one-use");
        }
        started = true;
        running = true;
    }

    Result run() throws IOException {
        start();
        try {
            // Both the original graph and the original source/cursor must still be current before DLR is opened.
            localProgress(deadline);
            publisher.bindManifestStartup(this, admission, persistent);
            bound = true;
            long operationDeadline = publisher.verifyManifestStartup(this);
            localProgress(operationDeadline);
            if (pending != null) {
                ownedLocal = openLocal();
                return publish(ownedLocal);
            }
            try (var local = openLocal()) {
                return publish(local);
            }
        } catch (IOException | RuntimeException failure) {
            demoteAfterFailure(failure);
            throw failure;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            var failure = new IOException("Fixed manifest cold publication interrupted", interrupted);
            demoteAfterFailure(failure);
            throw failure;
        } catch (TimeoutException timeout) {
            var failure = new IOException("Fixed manifest cold publication deadline expired", timeout);
            demoteAfterFailure(failure);
            throw failure;
        } finally {
            running = false;
        }
    }

    private SegmentLocalReadinessStore openLocal() throws IOException {
        var facts = checked.facts();
        return new SegmentLocalReadinessStore(directory, facts.bookie().toString(), facts.incarnation());
    }

    private Result publish(SegmentLocalReadinessStore local) throws InterruptedException, TimeoutException {
        var bounded = publisher.manifestPersistentStore(this);
        var phases = new StartupReadinessHarness(local, bounded, publisher).runManifest(input, StartupCrashPoint.NONE);
        if (phases.outcome() != Outcome.PUBLICATION_RECONCILED) {
            demote();
            return new Result(phases, null);
        }
        return new Result(phases, publisher.reconcileManifestStartup(this));
    }

    DeleteCatchUpAttempt followG1(String domain, int maxEvents, Duration timeout, BooleanSupplier live) {
        long probeDeadline = System.nanoTime() + timeout.toNanos();
        var sourceStatus = requirements().checkManifestSource(source(), reservations(), probeDeadline);
        if (sourceStatus != ZKProfileStartupRequirementsStore.Status.FOUND) {
            var status = sourceStatus == ZKProfileStartupRequirementsStore.Status.UNAVAILABLE
                    ? DeleteCatchUpStatus.UNAVAILABLE : DeleteCatchUpStatus.QUARANTINED;
            return new DeleteCatchUpAttempt(status, null, CompletableFuture.completedFuture(null));
        }
        if (!live.getAsBoolean() || System.nanoTime() >= probeDeadline) {
            return new DeleteCatchUpAttempt(DeleteCatchUpStatus.UNAVAILABLE, null,
                    CompletableFuture.completedFuture(null));
        }
        return owner.followOwnedG1(pending, episode, domain, maxEvents, probeDeadline, live);
    }

    private void demoteAfterFailure(Throwable failure) {
        try {
            demote();
        } catch (RuntimeException unavailable) {
            if (failure != unavailable) {
                failure.addSuppressed(unavailable);
            }
        }
    }

    synchronized void retire() {
        retired = true;
        admission.close();
    }

    void demote() {
        retire();
        if (bound && !demoted) {
            publisher.demote(checked.facts().bookie().toString());
            demoted = true;
        }
    }

    void demoteUntil(long closeDeadline) {
        retire();
        if (bound && !demoted && !publisher.manifestSessionEnded(this)) {
            publisher.demoteManifestUntil(this, closeDeadline);
            demoted = true;
        }
    }

    void closeLocal() throws IOException {
        if (ownedLocal != null) {
            ownedLocal.close();
            ownedLocal = null;
        }
    }

    boolean drained() {
        return publicationRetired() && publisher.manifestMutationsDrained(this);
    }

    boolean awaitDrained(long deadline) throws InterruptedException {
        return publicationRetired() && publisher.awaitManifestMutations(this, deadline);
    }

    private boolean publicationRetired() {
        return retired && !running && (!bound || demoted || publisher.manifestSessionEnded(this));
    }
}

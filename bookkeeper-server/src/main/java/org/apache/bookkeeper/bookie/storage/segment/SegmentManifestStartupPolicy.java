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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.startup.ManifestStartupReadinessInput;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Reason;
import org.apache.bookkeeper.discover.profile.ProfileDeleteRecord;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;

/** External declarations for one fixed manifest. No issuer or permission to publish is implied. */
record SegmentManifestStartupPolicy(ZKProfileStartupRequirementsStore.SourceIdentity source,
        SegmentArenaManifest manifest, List<Map<String, String>> roots,
        SegmentManifestAccessObservation.OldScope oldScope, long newUid, OldDisposition oldDisposition) {
    private static final Set<String> OLD_ROOT_KINDS = Set.of("JOURNAL", "LEDGER", "INDEX", "ARENA");

    enum OldDisposition { DRAINED, READ_ONLY, DECOMMISSIONED, WRITABLE, UNKNOWN }

    SegmentManifestStartupPolicy {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(oldScope, "oldScope");
        Objects.requireNonNull(oldDisposition, "oldDisposition");
        if (roots.size() != 1 + 2 * manifest.arenas().size()
                || roots.stream().anyMatch(root -> !root.keySet().equals(Set.of("kind", "arena", "path"))
                        || !bounded(root.get("kind"), 7) || !bounded(root.get("arena"), 36, true)
                        || !bounded(root.get("path"), 4096))) {
            throw new IllegalArgumentException("Invalid declared manifest root vector");
        }
        roots = roots.stream().map(Map::copyOf).toList();
        if (!bounded(oldScope.bookieId(), 255) || !bounded(oldScope.user(), 255)) {
            throw new IllegalArgumentException("Invalid declared old Bookie or Unix account");
        }
        var kinds = new HashSet<String>();
        var paths = new HashSet<Path>();
        for (var root : oldScope.roots()) {
            if (!root.keySet().equals(Set.of("kind", "path")) || !OLD_ROOT_KINDS.contains(root.get("kind"))
                    || !kinds.add(root.get("kind")) || !bounded(root.get("path"), 1024)) {
                throw new IllegalArgumentException("Invalid declared old root vector");
            }
            Path path = Path.of(root.get("path"));
            if (!path.isAbsolute() || path.getParent() == null || !path.toString().equals(root.get("path"))
                    || !path.equals(path.normalize()) || !paths.add(path)) {
                throw new IllegalArgumentException("Old roots must be distinct canonical paths");
            }
        }
        if (roots.stream().map(root -> Path.of(root.get("path")))
                .anyMatch(root -> paths.stream().anyMatch(old -> root.startsWith(old) || old.startsWith(root)))) {
            throw new IllegalArgumentException("Declared old and new roots overlap");
        }
    }

    /** Validate declarations and the actual schema2 observation without opening local storage. */
    PreOpen preflight(IsolatedSegmentConfig runtime, ZKProfileStartupRequirementsStore requirements,
            ZKProfileStartupRequirementsStore.Observation observed, ZKProfileReservationStore reservations)
            throws IOException {
        ManifestSegmentStorage.requireRecoverySourceScope(manifest, observed);
        var incarnation = ByteBuffer.wrap(manifest.incarnation().value());
        var oldEpisode = oldScope.episode();
        if (!source.equals(observed.sourceIdentity())
                || !roots.equals(SegmentManifestAccessObservation.expectedRoots(manifest, runtime))
                || oldEpisode == null || oldEpisode.equals(new UUID(0, 0))
                || oldEpisode.equals(new UUID(incarnation.getLong(), incarnation.getLong()))
                || oldScope.bookieId().equals(manifest.bookieId().toString())
                || newUid < 0 || newUid > 0xffff_ffffL || newUid == oldScope.uid()
                || oldDisposition == OldDisposition.WRITABLE || oldDisposition == OldDisposition.UNKNOWN) {
            throw new IOException("Declared policy does not bind a distinct complete manifest startup scope");
        }
        var unsupported = ManifestStartupReadinessInput.fixedFormatFailure(
                observed.commissionedCompatibility(), observed.requiredDevices());
        if (unsupported != null) {
            throw new IOException(unsupported == Reason.DEVICE_NOT_READY
                    ? "Manifest startup required device format is unsupported"
                    : "Manifest startup compatibility is unsupported");
        }
        return new PreOpen(this, Objects.requireNonNull(requirements), observed, Objects.requireNonNull(reservations));
    }

    private static boolean bounded(String value, int bytes) {
        return bounded(value, bytes, false);
    }

    private static boolean bounded(String value, int bytes, boolean allowEmpty) {
        return value != null && (allowEmpty || !value.isBlank()) && value.indexOf(0) < 0
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.getBytes(StandardCharsets.UTF_8).length <= bytes;
    }

    static final class PreOpen {
        final SegmentManifestStartupPolicy policy;
        final ZKProfileStartupRequirementsStore requirements;
        final ZKProfileStartupRequirementsStore.Observation source;
        final ZKProfileReservationStore reservations;

        private PreOpen(SegmentManifestStartupPolicy policy, ZKProfileStartupRequirementsStore requirements,
                ZKProfileStartupRequirementsStore.Observation source, ZKProfileReservationStore reservations) {
            this.policy = policy;
            this.requirements = requirements;
            this.source = source;
            this.reservations = reservations;
        }

        ZKProfileReservationStore.RetainedG1DeleteReplay requireCurrent(
                SegmentArenaManifest manifest, IsolatedSegmentConfig runtime,
                SegmentManifestAccessObservation access, ProfileLedgerMetadataStore metadata, long deadline)
                throws IOException {
            if (!policy.manifest().equals(manifest)
                    || !policy.roots().equals(SegmentManifestAccessObservation.expectedRoots(manifest, runtime))) {
                throw new IOException("Preflight belongs to a different complete manifest");
            }
            access.requirePolicyScope(policy);
            var target = new ProfileDeleteRecord.Target(manifest.bookieId().toString(), manifest.incarnation().value());
            if (!requirements.revalidateManifestSource(source, reservations, deadline)) {
                throw new IOException("Original manifest requirements are no longer current");
            }
            var retained = reservations.retainedG1DeleteReplay(metadata, target, deadline);
            var head = retained.readHead();
            if (head.status() != ProfileControlStore.ReadStatus.FOUND || head.head().assignmentGeneration() != 1
                    || System.nanoTime() >= deadline) {
                throw new IOException("Original manifest startup requires current retained G1");
            }
            return retained;
        }
    }
}

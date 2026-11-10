/*
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
 */

package org.apache.bookkeeper.common.profile.startup;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact.MigrationState;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest.DeviceStatus;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.Reason;

/** Parsed fixed-manifest reference input for cold publication. Its declarations carry no deployment authority. */
public record ManifestStartupReadinessInput(BookieCompatibilityFact compatibility,
        RequiredDeviceManifest requiredDevices, LocalRecoveryFacts recoveryFacts,
        RollbackSafetyFacts rollbackSafetyFacts, Source source, Scope scope,
        Generation targetReadinessGeneration, Generation capabilityGeneration, String profileEndpoint,
        List<Integer> capabilityHints) {

    static final List<Integer> SUPPORTED_FEATURES = List.of(1, 2);
    private static final int READER_VERSION = 2;
    private static final int WRITER_VERSION = 2;

    /** Schema 2 requires an immutable version-zero node whose modification and creation zxids are equal. */
    public record Source(String path, UUID birth, long birthZxid, ManifestIdentity sha256) {
        public Source {
            path = StartupFactSupport.boundedText(path, 4096);
            StartupFactSupport.requireNonNull(birth);
            StartupFactSupport.requireNonNull(sha256);
            if (!path.startsWith("/") || path.length() == 1 || birthZxid <= 0 || birth.equals(new UUID(0, 0))) {
                throw new IllegalArgumentException("Invalid fixed manifest source identity");
            }
        }
    }

    public enum OldDisposition { DRAINED, READ_ONLY, DECOMMISSIONED, WRITABLE, UNKNOWN }

    /** Complete external scope declarations. Root rows retain the canonical access-observation roles and paths. */
    public record Scope(String bookieId, StorageIncarnation incarnation, ManifestIdentity manifest,
            List<Map<String, String>> roots, UUID oldEpisode, String oldBookie, String oldUser, long oldUid,
            List<Map<String, String>> oldRoots, long newUid, OldDisposition disposition) {
        public Scope {
            bookieId = StartupFactSupport.boundedText(bookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
            oldBookie = StartupFactSupport.boundedText(oldBookie, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
            oldUser = StartupFactSupport.boundedText(oldUser, 255);
            StartupFactSupport.requireNonNull(incarnation);
            StartupFactSupport.requireNonNull(manifest);
            StartupFactSupport.requireNonNull(oldEpisode);
            StartupFactSupport.requireNonNull(disposition);
            roots = copyRoots(roots, Set.of("kind", "arena", "path"), 3, 129);
            oldRoots = copyRoots(oldRoots, Set.of("kind", "path"), 4, 4);
        }

        private static List<Map<String, String>> copyRoots(List<Map<String, String>> rows, Set<String> keys,
                int min, int max) {
            if (rows.size() < min || rows.size() > max) {
                throw new IllegalArgumentException("Invalid fixed manifest root count");
            }
            return rows.stream().map(row -> {
                if (!row.keySet().equals(keys)) {
                    throw new IllegalArgumentException("Invalid fixed manifest root fields");
                }
                StartupFactSupport.boundedText(row.get("kind"), 7);
                StartupFactSupport.boundedText(row.get("path"), 4096);
                if (keys.contains("arena") && !row.get("arena").isEmpty()) {
                    StartupFactSupport.boundedText(row.get("arena"), 36);
                }
                return Map.copyOf(row);
            }).toList();
        }

        private boolean matches(BookieCompatibilityFact compatibility, RequiredDeviceManifest devices) {
            var identity = ByteBuffer.wrap(incarnation.value());
            if (!bookieId.equals(compatibility.bookieId()) || !incarnation.equals(compatibility.storageIncarnation())
                    || !manifest.equals(compatibility.deviceManifestIdentity()) || bookieId.equals(oldBookie)
                    || oldEpisode.equals(new UUID(0, 0))
                    || oldEpisode.equals(new UUID(identity.getLong(), identity.getLong()))
                    || oldUid <= 0 || oldUid > 0xffff_ffffL || newUid < 0 || newUid > 0xffff_ffffL || oldUid == newUid
                    || disposition == OldDisposition.WRITABLE || disposition == OldDisposition.UNKNOWN) {
                return false;
            }
            var arenas = new HashMap<String, Path>();
            var indexes = new HashMap<String, Path>();
            var paths = new HashSet<Path>();
            Path control = null;
            for (var root : roots) {
                var path = canonical(root.get("path"));
                if (path == null || !paths.add(path)) {
                    return false;
                }
                String arena = root.get("arena");
                switch (root.get("kind")) {
                    case "CONTROL":
                        if (!arena.isEmpty() || control != null) {
                            return false;
                        }
                        control = path;
                        break;
                    case "ARENA":
                        if (arenas.putIfAbsent(arena, path) != null) {
                            return false;
                        }
                        break;
                    case "INDEX":
                        if (indexes.putIfAbsent(arena, path) != null) {
                            return false;
                        }
                        break;
                    default:
                        return false;
                }
            }
            var expected = new HashSet<String>();
            for (var device : devices.devices()) {
                String id = device.deviceId();
                if (!UUID.fromString(id).toString().equals(id) || id.equals(new UUID(0, 0).toString())) {
                    return false;
                }
                expected.add(id);
            }
            if (control == null || !arenas.keySet().equals(expected) || !indexes.keySet().equals(expected)) {
                return false;
            }
            for (var member : arenas.entrySet()) {
                var arena = member.getValue();
                if (!arena.equals(indexes.get(member.getKey()).getParent())
                        || arena.startsWith(control) || control.startsWith(arena)
                        || arenas.values().stream()
                                .anyMatch(other -> !arena.equals(other) && arena.startsWith(other))) {
                    return false;
                }
            }
            var oldKinds = new HashSet<String>();
            var oldPaths = new HashSet<Path>();
            for (var root : oldRoots) {
                var path = canonical(root.get("path"));
                if (path == null || !oldKinds.add(root.get("kind")) || !oldPaths.add(path)
                        || paths.stream().anyMatch(newPath -> path.startsWith(newPath) || newPath.startsWith(path))) {
                    return false;
                }
            }
            return oldKinds.equals(Set.of("JOURNAL", "LEDGER", "INDEX", "ARENA"));
        }

        private static Path canonical(String value) {
            var path = Path.of(value);
            return path.isAbsolute() && path.getParent() != null && path.toString().equals(value)
                    && path.normalize().equals(path) ? path : null;
        }
    }

    public ManifestStartupReadinessInput {
        StartupFactSupport.requireNonNull(compatibility);
        StartupFactSupport.requireNonNull(requiredDevices);
        StartupFactSupport.requireNonNull(recoveryFacts);
        StartupFactSupport.requireNonNull(rollbackSafetyFacts);
        StartupFactSupport.requireNonNull(source);
        StartupFactSupport.requireNonNull(scope);
        StartupFactSupport.requireNonNull(targetReadinessGeneration);
        StartupFactSupport.requireNonNull(capabilityGeneration);
        profileEndpoint = StartupFactSupport.boundedText(profileEndpoint, 255);
        capabilityHints = StartupFactSupport.strictlyIncreasingPositiveIds(capabilityHints, false);
    }

    Reason compatibilityFailure() {
        Reason failure = fixedFormatFailure(compatibility, requiredDevices);
        if (failure != null) {
            return failure;
        }
        try {
            return scope.matches(compatibility, requiredDevices) ? null : Reason.UNSAFE_NEW_SCOPE;
        } catch (IllegalArgumentException invalid) {
            return Reason.UNSAFE_NEW_SCOPE;
        }
    }

    /** Fixed binary capability shared by the pre-open and reference phase inputs. */
    public static Reason fixedFormatFailure(BookieCompatibilityFact compatibility, RequiredDeviceManifest devices) {
        if (compatibility.migrationState() != MigrationState.FORMAT_READY
                || compatibility.activeEngine() != EngineProfile.SEGMENT_WAL_ENGINE
                || compatibility.profileWireMajor() != 1 || compatibility.profileWireMinor() != 0
                || compatibility.descriptorHashSuite() != 1 || compatibility.deviceManifestGeneration().value() != 1
                || compatibility.localFormatGeneration().value() != 1
                || compatibility.effectiveDeleteAssignmentGeneration().value() != 1
                || !compatibility.mandatoryLocalFeatures().equals(SUPPORTED_FEATURES)) {
            return Reason.COMPATIBILITY_REJECTED;
        }
        if (compatibility.minimumReaderVersion() > READER_VERSION
                || compatibility.minimumWriterVersion() > WRITER_VERSION) {
            return Reason.RUNTIME_VERSION_REJECTED;
        }
        if (devices.devices().size() > 64) {
            return Reason.DEVICE_MANIFEST_MISMATCH;
        }
        for (var device : devices.devices()) {
            if (device.status() != DeviceStatus.READY || device.arenaFormatVersion() != 2
                    || device.controlFormatVersion() != 1 || device.checkpointFormatVersion() != 2
                    || device.superblockGeneration().value() != 1
                    || !device.mandatoryFeatures().equals(SUPPORTED_FEATURES)) {
                return Reason.DEVICE_NOT_READY;
            }
        }
        return null;
    }
}

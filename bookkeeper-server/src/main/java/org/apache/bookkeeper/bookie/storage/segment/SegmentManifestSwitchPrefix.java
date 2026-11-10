/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedReady;

/** Classifies the physical names bound by a manifest READY while the original graph is held. */
final class SegmentManifestSwitchPrefix {
    enum Phase { INSTALLED, RETAINED, ORIGINAL }

    static List<Phase> inspectArena(IsolatedSegmentConfig config, SegmentFileHeader identity, SegmentOwnedFiles files,
            NativeIO nativeIo, Path stage, ProfileManifestClosedAdmission.Arena original,
            ProfileManifestClosedReady.Arena ready, Set<FileIdentity> protectedObjects, ByteBuffer buffer,
            SegmentDataRecovery.AuditCheckpoint checkpoint) throws Exception {
        if (protectedObjects.contains(ready.stage()) || original.files().size() != config.dataFiles()
                || ready.files().size() != config.dataFiles()) {
            throw new IOException("Manifest READY stage aliases its graph or has the wrong DATA geometry");
        }
        var phases = new ArrayList<Phase>(config.dataFiles());
        var stageNames = new HashSet<Path>();
        for (int index = 0; index < config.dataFiles(); index++) {
            checkpoint.check();
            var before = original.files().get(index);
            var after = ready.files().get(index);
            if (before.image().length() != config.bytesPerFile() || after.image().length() != config.bytesPerFile()
                    || protectedObjects.contains(before.image().object())
                    || protectedObjects.contains(after.image().object())) {
                throw new IOException("Manifest READY DATA aliases its graph or has the wrong extent");
            }
            Path active = config.directory().resolve("data-" + index);
            Path image = stage.resolve("image-" + index);
            Path retained = retained(stage, index);
            boolean haveActive = regularPresent(active);
            boolean haveImage = regularPresent(image);
            boolean haveRetained = regularPresent(retained);
            Phase phase;
            if (haveImage && haveActive != haveRetained) {
                phase = haveActive ? Phase.ORIGINAL : Phase.RETAINED;
            } else if (!haveImage && haveActive && haveRetained) {
                phase = Phase.INSTALLED;
            } else {
                throw new IOException("Manifest READY has an unrecognized DATA placement at " + active);
            }
            if (haveImage) {
                stageNames.add(image);
            }
            if (haveRetained) {
                stageNames.add(retained);
            }
            SegmentClosedReconstruction.verifyImage(files, nativeIo, config, identity,
                    phase == Phase.ORIGINAL ? active : retained, before, buffer, checkpoint);
            SegmentClosedReconstruction.verifyImage(files, nativeIo, config, identity,
                    phase == Phase.INSTALLED ? active : image, after, buffer, checkpoint);
            phases.add(phase);
        }
        try (var entries = files.openDirectory(stage)) {
            for (Path entry : entries) {
                checkpoint.check();
                if (!stageNames.remove(entry)) {
                    throw new IOException("Unrecognized entry in the manifest READY stage: " + entry);
                }
            }
        }
        if (!stageNames.isEmpty()) {
            throw new IOException("Manifest READY stage lost part of its original placement");
        }
        files.requireHealthy();
        return List.copyOf(phases);
    }

    static void requireGlobalOrder(List<List<Phase>> arenas) throws IOException {
        Phase previous = Phase.INSTALLED;
        for (var arena : arenas) {
            for (Phase phase : arena) {
                if (phase.compareTo(previous) < 0 || phase == Phase.RETAINED && previous == Phase.RETAINED) {
                    throw new IOException("Manifest READY DATA files are outside the global switch prefix");
                }
                previous = phase;
            }
        }
    }

    static List<Path> originalPaths(Path root, Path stage, List<Phase> phases) {
        var paths = new ArrayList<Path>(phases.size());
        for (int index = 0; index < phases.size(); index++) {
            paths.add(phases.get(index) == Phase.ORIGINAL ? root.resolve("data-" + index) : retained(stage, index));
        }
        return paths;
    }

    private static Path retained(Path stage, int index) {
        return stage.resolve("retained-old-" + index);
    }

    private static boolean regularPresent(Path path) throws IOException {
        try {
            if (!Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isRegularFile()) {
                throw new IOException("Manifest READY DATA role requires a regular file: " + path);
            }
            return true;
        } catch (NoSuchFileException absent) {
            return false;
        }
    }

    private SegmentManifestSwitchPrefix() { }
}

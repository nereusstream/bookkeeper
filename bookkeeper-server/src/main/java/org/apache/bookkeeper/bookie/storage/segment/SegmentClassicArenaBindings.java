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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;

/** Existing, unused Arena members only. Not commissioning, DATA recovery or service readiness. */
final class SegmentClassicArenaBindings {
    record GuardResult(Path path, SegmentDataFileLock.DrainResult result) { }

    record Result(boolean drained, Throwable initializationFailure, SegmentOwnedFiles.Result files,
                  List<GuardResult> guards) { }

    private record Guard(Path path, SegmentDataFileLock owner) { }

    private final SegmentArenaManifest manifest;
    private final ManifestIdentity commissioned;
    private final long maxControlBytes;
    private final List<Guard> guards;
    private final List<Path> protectedLocks;
    private final SegmentOwnedFiles files;
    private boolean attempted;
    private boolean initialized;
    private boolean closing;
    private Thread initializingThread;
    private Thread closingThread;
    private Throwable initializationFailure;

    /** Heap only: the outer bootstrap owner registers this child before initialize performs any I/O. */
    SegmentClassicArenaBindings(SegmentArenaManifest manifest, ManifestIdentity commissioned, long maxControlBytes)
            throws IOException {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.commissioned = Objects.requireNonNull(commissioned, "commissioned");
        this.maxControlBytes = maxControlBytes;
        if (manifest.classicRoots().isEmpty() || !manifest.identity().equals(commissioned)
                || maxControlBytes < 4096 + 192) {
            throw new IOException("Complete commissioned common manifest and bounded Arena control required");
        }
        var members = new ArrayList<Guard>();
        var excluded = new ArrayList<Path>();
        excluded.add(manifest.controlDirectory().resolve("bookie-control.lock"));
        for (var root : manifest.classicRoots()) {
            excluded.add(root.directory().resolve("current").resolve(SegmentClassicRootBindings.LOCK_NAME));
        }
        for (var arena : manifest.arenas()) {
            for (String name : List.of("isolated.lock", "arena-control.lock")) {
                Path path = arena.directory().resolve(name);
                members.add(new Guard(path, SegmentDataFileLock.existingControlOwner(path)));
                excluded.add(path);
            }
        }
        guards = List.copyOf(members);
        protectedLocks = List.copyOf(excluded);
        files = new SegmentOwnedFiles(protectedLocks);
    }

    synchronized void initialize() throws IOException {
        if (attempted || closing) {
            throw new IOException("Existing Arena verification is one-use");
        }
        attempted = true;
        initializingThread = Thread.currentThread();
        try {
            requirePhysicalRoots();
            requireDistinctGuards();
            for (Guard guard : guards) {
                guard.owner().initialize();
            }
            if (!manifest.equals(SegmentArenaManifest.read(manifest.path(), commissioned, files))) {
                throw new IOException("Stored manifest differs from the complete commissioned scope");
            }
            for (int member = 0; member < manifest.arenas().size(); member++) {
                var arena = manifest.arenas().get(member);
                var config = new IsolatedSegmentConfig(arena.directory(), 1, manifest.shardArenas().size(),
                        arena.dataFiles(), arena.bytesPerFile(), 4096, 1, 1,
                        "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
                SegmentArenaControlLog.requireUnusedInspection(config, SegmentFileHeader.forArena(manifest, member),
                        maxControlBytes, files);
            }
            files.requireHealthy();
            initialized = true;
        } catch (IOException | RuntimeException | Error failure) {
            initializationFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private void requirePhysicalRoots() throws IOException {
        var roots = new ArrayList<Path>();
        roots.add(manifest.controlDirectory());
        manifest.classicRoots().forEach(root -> roots.add(root.directory()));
        manifest.arenas().forEach(arena -> roots.add(arena.directory()));
        for (int member = 0; member < roots.size(); member++) {
            Path root = roots.get(member);
            if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) || !root.equals(root.toRealPath())) {
                throw new IOException("Required common root must be an existing canonical directory: " + root);
            }
            for (int previous = 0; previous < member; previous++) {
                if (Files.isSameFile(root, roots.get(previous))) {
                    throw new IOException("Common scope repeats one physical root");
                }
            }
        }
    }

    private void requireDistinctGuards() throws IOException {
        for (int member = 0; member < protectedLocks.size(); member++) {
            Path path = protectedLocks.get(member);
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Required existing guard is missing or not a regular file: " + path);
            }
            for (int previous = 0; previous < member; previous++) {
                if (Files.isSameFile(path, protectedLocks.get(previous))) {
                    throw new IOException("Common scope repeats one physical guard");
                }
            }
        }
    }

    synchronized boolean verified() {
        return initialized && !closing && files.healthy();
    }

    synchronized void requireVerified(ManifestIdentity expected) throws IOException {
        if (!commissioned.equals(expected) || !verified()) {
            throw new IOException("Classic bootstrap requires its verified and held Arena members");
        }
    }

    /** Serialized with actual validation. Never interrupts its reader or retries an unknown native close. */
    synchronized Result closeAndDrain() throws InterruptedException {
        if (initializingThread == Thread.currentThread() || closingThread == Thread.currentThread()) {
            throw new IllegalStateException("Arena verification activity cannot drain itself");
        }
        closing = true;
        closingThread = Thread.currentThread();
        try {
            var fileResult = files.drain();
            if (fileResult.drained()) {
                for (int member = guards.size() - 1; member >= 0; member--) {
                    try {
                        guards.get(member).owner().close();
                    } catch (Throwable ignored) {
                        // The original lease retains its first native failure and actual handles.
                    }
                }
            }
            var results = guards.stream().map(guard -> new GuardResult(guard.path(), guard.owner().drainResult()))
                    .toList();
            boolean drained = fileResult.drained() && results.stream().allMatch(guard -> guard.result().drained());
            return new Result(drained, initializationFailure, fileResult, results);
        } finally {
            closingThread = null;
        }
    }
}

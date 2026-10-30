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
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.bookie.storage.segment.ClassicRouteStartup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;

/**
 * Resource child for the explicit fresh owner; does not issue a startup permit or metadata fact.
 * Its caller must retain it until every storage user has actually terminated, including failed initialization.
 */
final class FreshClassicRoots implements AutoCloseable {
    private record Scope(BookieId bookie, List<String> journal, List<String> ledger, List<String> index) { }
    private record Alias(Path configured, Root member) { }

    record ReleaseResult(Path path, String kind, boolean acquired, boolean closeAttempted,
                         boolean closeReturned, Throwable failure) { }

    record FreshDrainResult(boolean drained, Throwable acquisitionFailure, List<ReleaseResult> releases) {
        Throwable closeFailure() {
            for (ReleaseResult result : releases) {
                if (result.failure() != null) {
                    return result.failure();
                }
            }
            return null;
        }
    }

    /** One first native-close result, shared by acquisition cleanup and final root cleanup. */
    private static final class CloseRecord<T extends AutoCloseable> {
        final Path path;
        final String kind;
        T resource;
        boolean acquired;
        boolean closeAttempted;
        boolean closeReturned;
        Throwable failure;

        CloseRecord(Path path, String kind) {
            this.path = path;
            this.kind = kind;
        }

        void acquired(T resource) {
            this.resource = resource;
            acquired = true;
        }

        void close() {
            if (closeAttempted) {
                if (!closeReturned) {
                    throw new IllegalStateException("Fresh native close is still executing");
                }
                return;
            }
            if (!acquired) {
                return;
            }
            closeAttempted = true;
            try {
                if (resource instanceof FileChannel channel && !channel.isOpen()
                        || resource instanceof FileLock lock && !lock.isValid()) {
                    throw new IOException("Fresh " + kind + " closed without an observed native result: " + path);
                }
                resource.close();
                resource = null;
            } catch (Throwable closeFailure) {
                failure = closeFailure;
            } finally {
                closeReturned = true;
            }
        }

        boolean drained() {
            return !acquired || closeReturned && failure == null;
        }

        ReleaseResult result() {
            return new ReleaseResult(path, kind, acquired, closeAttempted, closeReturned, failure);
        }
    }

    private static final class Root {
        final Path directory;
        final Object directoryKey;
        final CloseRecord<FileChannel> channel;
        final CloseRecord<FileLock> lock;
        Object markerKey;

        Root(Path directory, Object directoryKey) {
            this.directory = directory;
            this.directoryKey = directoryKey;
            channel = new CloseRecord<>(marker(), "marker channel");
            lock = new CloseRecord<>(marker(), "root lock");
        }

        Path marker() {
            return directory.resolve(ClassicRouteStartup.FRESH_LOCK_FILE_NAME);
        }
    }

    private final Scope scope;
    private final List<Root> roots = new ArrayList<>();
    private final List<Alias> aliases = new ArrayList<>();
    private final List<CloseRecord<?>> auxiliary = new ArrayList<>();
    private boolean attempted;
    private boolean prepareAttempted;
    private boolean ready;
    private boolean closing;
    private Thread acquiringThread;
    private Thread closingThread;
    private Throwable acquisitionFailure;

    /** Pure configuration snapshot: acquire is separate so failures never lose the resource owner. */
    FreshClassicRoots(ServerConfiguration configuration) throws IOException {
        scope = scope(configuration);
    }

    synchronized void acquire() throws IOException {
        if (attempted || closing) {
            throw new IOException("Fresh root ownership can only be acquired once");
        }
        attempted = true;
        acquiringThread = Thread.currentThread();
        try {
            acquireRoots();
        } catch (IOException | RuntimeException | Error failure) {
            acquisitionFailure = failure;
            throw failure;
        } finally {
            acquiringThread = null;
        }
    }

    private void acquireRoots() throws IOException {
        for (String configured : configuredRoots(scope)) {
            Path directory = Path.of(configured).toRealPath();
            var attributes = directoryAttributes(directory);
            Root member = null;
            for (Root existing : roots) {
                if (Files.isSameFile(existing.directory, directory)) {
                    member = existing;
                    break;
                }
            }
            if (member == null) {
                member = new Root(directory, attributes.fileKey());
                roots.add(member);
            }
            aliases.add(new Alias(Path.of(configured), member));
        }
        roots.sort(Comparator.comparing(root -> root.directory.toString()));
        // Validate the entire scope before leaving even the first persistent candidate.
        for (Root root : roots) {
            if (!root.directory.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                throw new IOException("Fresh roots require private POSIX lock files");
            }
            requireMembers(root, false);
        }
        for (Root root : roots) {
            verifyDirectory(root);
            // CREATE_NEW prevents opening a second channel for any old marker, even after close or failure.
            // Closing another channel to an in-process POSIX lock scope could otherwise drop the original lock.
            root.channel.acquired(FileChannel.open(root.marker(),
                    Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))));
            try {
                FileLock lock = root.channel.resource.tryLock();
                if (lock == null) {
                    throw new IOException("Fresh root already has an owner");
                }
                root.lock.acquired(lock);
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Fresh root already has an owner", overlapping);
            }
            root.markerKey = markerAttributes(root.marker()).fileKey();
            root.channel.resource.force(true);
            syncDirectory(root);
        }
        for (Root root : roots) {
            verifyHeld(root);
            requireMembers(root, true);
        }
        verifyAliases();
        ready = true;
    }

    /** Create the fresh layout only after the entire original top-level root set is exclusively held. */
    synchronized void prepareCurrentDirectories() throws IOException {
        if (!ready || closing || prepareAttempted) {
            throw new IOException("Fresh current directories require one held root acquisition episode");
        }
        prepareAttempted = true;
        acquiringThread = Thread.currentThread();
        try {
            verifyAliases();
            for (Root root : roots) {
                verifyHeld(root);
                Files.createDirectory(root.directory.resolve("current"));
                syncDirectory(root);
            }
            verifyAliases();
        } catch (IOException | RuntimeException | Error failure) {
            acquisitionFailure = failure;
            ready = false;
            throw failure;
        } finally {
            acquiringThread = null;
        }
    }

    /** Recheck before use; same paths and bytes cannot substitute a physically replaced directory or marker. */
    synchronized void verify(ServerConfiguration configuration) throws IOException {
        if (!ready || closing || !scope.equals(scope(configuration))) {
            throw new IOException("Fresh roots are not held for this Bookie and configuration episode");
        }
        verifyAliases();
        for (Root root : roots) {
            verifyHeld(root);
        }
    }

    private void verifyAliases() throws IOException {
        for (Alias alias : aliases) {
            Path directory = alias.configured().toRealPath();
            var attributes = directoryAttributes(directory);
            Root expected = alias.member();
            if (!Files.isSameFile(expected.directory, directory)
                    || !expected.directoryKey.equals(attributes.fileKey())) {
                throw new IOException("Configured fresh root physical identity changed");
            }
        }
    }

    private static Scope scope(ServerConfiguration configuration) throws IOException {
        String[] index = configuration.getIndexDirNames();
        var scope = new Scope(BookieImpl.getBookieId(configuration), List.of(configuration.getJournalDirNames()),
                List.of(configuration.getLedgerDirNames()), index == null ? null : List.of(index));
        if (scope.journal().isEmpty() || scope.ledger().isEmpty() || index != null && scope.index().isEmpty()) {
            throw new IOException("Fresh ownership requires the complete nonempty root set");
        }
        return scope;
    }

    private static List<String> configuredRoots(Scope scope) {
        List<String> result = new ArrayList<>(scope.journal());
        result.addAll(scope.ledger());
        result.addAll(scope.index() == null ? scope.ledger() : scope.index());
        return result;
    }

    private static BasicFileAttributes directoryAttributes(Path directory) throws IOException {
        var attributes = Files.readAttributes(directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.fileKey() == null) {
            throw new IOException("Fresh root needs a verifiable directory physical identity");
        }
        return attributes;
    }

    private static BasicFileAttributes markerAttributes(Path marker) throws IOException {
        var attributes = Files.readAttributes(marker, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.fileKey() == null || attributes.size() != 0
                || !Files.getPosixFilePermissions(marker, LinkOption.NOFOLLOW_LINKS)
                        .equals(PosixFilePermissions.fromString("rw-------"))) {
            throw new IOException("Fresh marker physical identity or private permissions are invalid");
        }
        return attributes;
    }

    private static void verifyDirectory(Root root) throws IOException {
        if (!root.directory.toRealPath().equals(root.directory)
                || !root.directoryKey.equals(directoryAttributes(root.directory).fileKey())) {
            throw new IOException("Fresh root physical identity changed");
        }
    }

    private static void verifyHeld(Root root) throws IOException {
        verifyDirectory(root);
        if (root.channel.resource == null || !root.channel.resource.isOpen()
                || root.lock.resource == null || !root.lock.resource.isValid()
                || !root.markerKey.equals(markerAttributes(root.marker()).fileKey())) {
            throw new IOException("Fresh root no longer holds its original exclusive marker");
        }
    }

    private void syncDirectory(Root root) throws IOException {
        var directory = new CloseRecord<FileChannel>(root.directory, "directory sync channel");
        auxiliary.add(directory);
        Throwable failure = null;
        try {
            directory.acquired(FileChannel.open(root.directory, StandardOpenOption.READ));
            directory.resource.force(true);
        } catch (IOException | RuntimeException | Error operationFailure) {
            failure = operationFailure;
            throw operationFailure;
        } finally {
            finishAuxiliary(directory, failure);
        }
    }

    private void requireMembers(Root root, boolean ownMarker) throws IOException {
        var members = new CloseRecord<DirectoryStream<Path>>(root.directory, "directory listing");
        auxiliary.add(members);
        Throwable failure = null;
        try {
            members.acquired(Files.newDirectoryStream(root.directory));
            for (Path member : members.resource) {
                if (!ownMarker || !member.equals(root.marker())) {
                    throw new IOException("Fresh root contains historical or unclassified files: " + member);
                }
            }
        } catch (IOException | RuntimeException | Error operationFailure) {
            failure = operationFailure;
            throw operationFailure;
        } finally {
            finishAuxiliary(members, failure);
        }
    }

    private void finishAuxiliary(CloseRecord<?> resource, Throwable operationFailure) throws IOException {
        resource.close();
        if (resource.drained()) {
            auxiliary.remove(resource);
        }
        if (resource.failure != null) {
            if (operationFailure == null) {
                throwCloseFailure(resource.failure);
            } else if (operationFailure != resource.failure) {
                operationFailure.addSuppressed(resource.failure);
            }
        }
    }

    /** Release resources only. Persistent candidates and data are never removed or relabelled fresh. */
    @Override
    public synchronized void close() throws IOException {
        FreshDrainResult result = closeForFresh();
        Throwable failure = result.closeFailure();
        if (failure != null) {
            for (ReleaseResult release : result.releases()) {
                Throwable other = release.failure();
                if (other != null && other != failure) {
                    boolean included = false;
                    for (Throwable suppressed : failure.getSuppressed()) {
                        included |= suppressed == other;
                    }
                    if (!included) {
                        failure.addSuppressed(other);
                    }
                }
            }
            throwCloseFailure(failure);
        }
    }

    /** Called only after initialization and every external storage user have actually returned. */
    synchronized FreshDrainResult closeForFresh() {
        Thread current = Thread.currentThread();
        if (current == acquiringThread || current == closingThread) {
            throw new IllegalStateException("Fresh root resource activity cannot await its own close");
        }
        closingThread = current;
        try {
            closing = true;
            ready = false;
            boolean drained = true;
            List<ReleaseResult> releases = new ArrayList<>();
            for (int i = roots.size() - 1; i >= 0; i--) {
                Root root = roots.get(i);
                // Closing the marker fd may release its native lock even if explicit release failed.
                // These two original results remain independent, and a failure is never erased.
                root.lock.close();
                root.channel.close();
                releases.add(root.lock.result());
                releases.add(root.channel.result());
                drained &= root.lock.drained() && root.channel.drained();
            }
            for (CloseRecord<?> resource : List.copyOf(auxiliary)) {
                resource.close();
                if (resource.drained()) {
                    auxiliary.remove(resource);
                } else {
                    releases.add(resource.result());
                    drained = false;
                }
            }
            return new FreshDrainResult(drained, acquisitionFailure, List.copyOf(releases));
        } finally {
            closingThread = null;
        }
    }

    private static void throwCloseFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException io) {
            throw io;
        } else if (failure instanceof RuntimeException runtime) {
            throw runtime;
        } else if (failure instanceof Error error) {
            throw error;
        } else {
            throw new IOException("Fresh root native close failed", failure);
        }
    }
}

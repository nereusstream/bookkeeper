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

    private static final class Root {
        final Path directory;
        final Object directoryKey;
        FileChannel channel;
        FileLock lock;
        Object markerKey;

        Root(Path directory, Object directoryKey) {
            this.directory = directory;
            this.directoryKey = directoryKey;
        }

        Path marker() {
            return directory.resolve(ClassicRouteStartup.FRESH_LOCK_FILE_NAME);
        }
    }

    private final Scope scope;
    private final List<Root> roots = new ArrayList<>();
    private final List<Alias> aliases = new ArrayList<>();
    private boolean attempted;
    private boolean ready;
    private boolean closing;
    private boolean closed;

    /** Pure configuration snapshot: acquire is separate so failures never lose the resource owner. */
    FreshClassicRoots(ServerConfiguration configuration) throws IOException {
        scope = scope(configuration);
    }

    synchronized void acquire() throws IOException {
        if (attempted || closing) {
            throw new IOException("Fresh root ownership can only be acquired once");
        }
        attempted = true;
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
            root.channel = FileChannel.open(root.marker(),
                    Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            try {
                root.lock = root.channel.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("Fresh root already has an owner", overlapping);
            }
            if (root.lock == null) {
                throw new IOException("Fresh root already has an owner");
            }
            root.markerKey = markerAttributes(root.marker()).fileKey();
            root.channel.force(true);
            try (var directory = FileChannel.open(root.directory, StandardOpenOption.READ)) {
                directory.force(true);
            }
        }
        for (Root root : roots) {
            verifyHeld(root);
            requireMembers(root, true);
        }
        verifyAliases();
        ready = true;
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
        if (root.channel == null || !root.channel.isOpen() || root.lock == null || !root.lock.isValid()
                || !root.markerKey.equals(markerAttributes(root.marker()).fileKey())) {
            throw new IOException("Fresh root no longer holds its original exclusive marker");
        }
    }

    private static void requireMembers(Root root, boolean ownMarker) throws IOException {
        try (var members = Files.newDirectoryStream(root.directory)) {
            for (Path member : members) {
                if (!ownMarker || !member.equals(root.marker())) {
                    throw new IOException("Fresh root contains historical or unclassified files: " + member);
                }
            }
        }
    }

    /** Release resources only. Persistent candidates and data are never removed or relabelled fresh. */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closing = true;
        ready = false;
        IOException first = null;
        for (int i = roots.size() - 1; i >= 0; i--) {
            Root root = roots.get(i);
            try {
                if (root.lock != null && root.lock.isValid()) {
                    root.lock.release();
                }
            } catch (IOException failure) {
                first = append(first, failure);
            }
            try {
                if (root.channel != null && root.channel.isOpen()) {
                    root.channel.close();
                }
            } catch (IOException failure) {
                first = append(first, failure);
            }
        }
        closed = roots.stream().allMatch(root -> (root.channel == null || !root.channel.isOpen())
                && (root.lock == null || !root.lock.isValid()));
        if (first != null) {
            throw first;
        }
    }

    private static IOException append(IOException first, IOException failure) {
        if (first == null) {
            return failure;
        }
        first.addSuppressed(failure);
        return first;
    }
}

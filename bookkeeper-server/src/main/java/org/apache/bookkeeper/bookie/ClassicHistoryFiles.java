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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.util.BookKeeperConstants;

/** Startup readers shared by the owned Classic Journal, index and entry logger. Never startup authority. */
final class ClassicHistoryFiles {
    record Drain(boolean drained, int retainedResources, Throwable closeFailure) { }
    record Prefix(ByteBuffer bytes, long fileSize) { }

    final class Read implements AutoCloseable {
        FileChannel channel;
        DirectoryStream<Path> listing;
        private boolean closeAttempted;
        private Throwable acquisitionFailure;

        @Override
        public void close() throws IOException {
            synchronized (ClassicHistoryFiles.this) {
                if (closeAttempted) {
                    return;
                }
                closeAttempted = true;
                try {
                    if (channel == null && listing == null) {
                        throw new IOException("Historical acquisition has no observed original native close result",
                                acquisitionFailure);
                    }
                    if (channel != null) {
                        if (!channel.isOpen()) {
                            throw new IOException("Historical reader lost its original native close result");
                        }
                        channel.close();
                    }
                    if (listing != null) {
                        listing.close();
                    }
                    resources.remove(this);
                } catch (IOException | RuntimeException | Error failure) {
                    if (closeFailure == null) {
                        closeFailure = failure;
                    }
                    throw failure;
                }
            }
        }
    }

    private final List<Read> resources = new ArrayList<>();
    private boolean closing;
    private Throwable closeFailure;

    synchronized Read openRead(Path path) throws IOException {
        requireHealthy();
        fileIdentity(path);
        Read read = new Read();
        resources.add(read);
        try {
            read.channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            return read;
        } catch (IOException | RuntimeException | Error failure) {
            unknownAcquisition(read, failure);
            throw failure;
        }
    }

    synchronized Read openDirectory(Path path) throws IOException {
        requireHealthy();
        requireDirectory(path);
        Read read = new Read();
        resources.add(read);
        try {
            read.listing = Files.newDirectoryStream(path);
            return read;
        } catch (IOException | RuntimeException | Error failure) {
            unknownAcquisition(read, failure);
            throw failure;
        }
    }

    byte[] read(Path path, int minimum, int maximum) throws IOException {
        var before = fileIdentity(path);
        if (before.size() < minimum || before.size() > maximum) {
            throw new IOException("Historical file exceeds its supported bounds: " + path);
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) before.size());
        try (Read read = openRead(path)) {
            readFully(read.channel, bytes);
            requireSame(path, before, read.channel);
        }
        return bytes.array();
    }

    /** Existing local Cookie bytes only; the bound outer owner still verifies the complete native/root scope. */
    Cookie readCookie(Path currentDirectory) throws IOException {
        requireDirectory(currentDirectory);
        byte[] bytes = read(currentDirectory.resolve(BookKeeperConstants.VERSION_FILENAME), 1, 1024 * 1024);
        StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes));
        return Cookie.parseFromBytes(bytes);
    }

    Prefix readPrefix(Path path, int length) throws IOException {
        var before = fileIdentity(path);
        if (before.size() < length) {
            throw new IOException("Incomplete historical Classic header: " + path);
        }
        ByteBuffer bytes = ByteBuffer.allocate(length);
        try (Read read = openRead(path)) {
            readFully(read.channel, bytes);
            requireSame(path, before, read.channel);
        }
        bytes.flip();
        return new Prefix(bytes.asReadOnlyBuffer(), before.size());
    }

    void syncDirectory(Path path) throws IOException {
        Read read;
        synchronized (this) {
            requireHealthy();
            requireDirectory(path);
            read = new Read();
            resources.add(read);
            try {
                read.channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            } catch (IOException | RuntimeException | Error failure) {
                unknownAcquisition(read, failure);
                throw failure;
            }
        }
        try (read) {
            read.channel.force(true);
        }
    }

    static void requireDirectory(Path path) throws IOException {
        if (!Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).isDirectory()) {
            throw new IOException("Historical Classic requires an existing directory: " + path);
        }
    }

    static BasicFileAttributes fileIdentity(Path path) throws IOException {
        var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.fileKey() == null
                || ((Number) Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS)).longValue() != 1) {
            throw new IOException("Historical Classic requires a single-link regular file: " + path);
        }
        return attributes;
    }

    static void requireSame(Path path, BasicFileAttributes before, FileChannel channel) throws IOException {
        var after = fileIdentity(path);
        if (!before.fileKey().equals(after.fileKey()) || before.size() != after.size()
                || !before.lastModifiedTime().equals(after.lastModifiedTime()) || channel.size() != before.size()) {
            throw new IOException("Historical Classic file changed during its owned read: " + path);
        }
    }

    static void readFully(FileChannel channel, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            if (channel.read(bytes) <= 0) {
                throw new IOException("Incomplete historical Classic file");
            }
        }
    }

    private void requireHealthy() throws IOException {
        if (!healthy()) {
            throw new IOException("Historical Classic readers are closing or failed", closeFailure);
        }
    }

    private void unknownAcquisition(Read read, Throwable failure) {
        read.acquisitionFailure = failure;
        if (closeFailure == null) {
            closeFailure = failure;
        }
        // The original acquisition may have opened a native resource before losing its return value.
        // Retain its slot; a subsequent cleanup cannot invent or retry that original close result.
    }

    synchronized boolean healthy() {
        return !closing && closeFailure == null;
    }

    synchronized Throwable closeFailure() {
        return closeFailure;
    }

    /** The enclosing component must first wait for its actual initializer, scanners and other users. */
    synchronized Drain closeAndDrain() {
        closing = true;
        for (Read read : List.copyOf(resources)) {
            try {
                read.close();
            } catch (Throwable ignored) {
                // Keep the original resource and its first failed native close; never retry that close.
            }
        }
        return new Drain(resources.isEmpty() && closeFailure == null, resources.size(), closeFailure);
    }
}

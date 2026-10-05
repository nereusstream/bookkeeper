/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import org.apache.bookkeeper.common.profile.startup.DurableLocalReadiness;
import org.apache.bookkeeper.common.profile.startup.LocalReadinessStore;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;

/**
 * Single-writer local receipt in a caller-owned, precreated experimental Bookie directory.
 *
 * <p>Persists supplied conclusions; it does not establish recovery, compatibility or delete catch-up.
 * Startup must freshly validate those facts before using a receipt to publish registration.
 */
public final class SegmentLocalReadinessStore implements LocalReadinessStore, AutoCloseable {
    private final Path directory;
    private final String bookie;
    private final StorageIncarnation incarnation;
    private final SegmentFileIo io;
    private final SegmentDataFileLock writer;
    private DurableLocalReadiness current;
    private IOException failure;
    private boolean closed;

    public SegmentLocalReadinessStore(Path directory, String bookie, StorageIncarnation incarnation)
            throws IOException {
        this(directory, bookie, incarnation, new SegmentFileIo());
    }

    SegmentLocalReadinessStore(Path directory, String bookie, StorageIncarnation incarnation, SegmentFileIo io)
            throws IOException {
        this.directory = directory.toRealPath();
        this.bookie = Objects.requireNonNull(bookie, "bookie");
        SegmentLocalReadinessCodec.name(bookie);
        this.incarnation = Objects.requireNonNull(incarnation, "incarnation");
        this.io = Objects.requireNonNull(io, "io");
        writer = SegmentDataFileLock.openControl(this.directory.resolve("local-readiness.lock"));
        try {
            current = load(true);
            // Also cover a recovered rename and the writer-lock directory entry, including an empty store.
            syncDirectory();
        } catch (IOException | RuntimeException | Error unavailable) {
            try {
                writer.close();
            } catch (IOException closeFailure) {
                unavailable.addSuppressed(closeFailure);
            }
            throw unavailable;
        }
    }

    private Path receipt() {
        return directory.resolve("local-readiness");
    }

    private DurableLocalReadiness load(boolean synchronize) throws IOException {
        try (FileChannel file = FileChannel.open(receipt(), StandardOpenOption.READ, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS)) {
            long length = file.size();
            if (length < SegmentLocalReadinessCodec.FIXED_BYTES + 1 || length > SegmentLocalReadinessCodec.MAX_BYTES) {
                throw new IOException("Local readiness exceeds its bounded record envelope");
            }
            ByteBuffer bytes = ByteBuffer.allocate((int) length);
            SegmentFileIo.readFully(file, bytes, 0);
            DurableLocalReadiness record = SegmentLocalReadinessCodec.decode(bytes);
            if (!bookie.equals(record.bookieId()) || !incarnation.equals(record.storageIncarnation())) {
                throw new IOException("Local readiness belongs to another Bookie or storage incarnation");
            }
            if (synchronize) {
                io.force(file, true);
            }
            return record;
        } catch (NoSuchFileException absent) {
            return null;
        }
    }

    private void syncDirectory() throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            io.force(channel, true);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Local readiness store is closed");
        }
    }

    private void verifyCurrent() throws IOException {
        if (failure != null) {
            throw failure;
        }
        if (!Objects.equals(current, load(false))) {
            throw new IOException("Local readiness changed outside its writer");
        }
    }

    @Override
    public synchronized Optional<DurableLocalReadiness> read(String bookieId) {
        requireOpen();
        if (!bookie.equals(bookieId)) {
            throw new IllegalArgumentException("Wrong local readiness scope");
        }
        try {
            verifyCurrent();
            return Optional.ofNullable(current);
        } catch (IOException unavailable) {
            failure = unavailable;
            throw new UncheckedIOException(unavailable);
        }
    }

    @Override
    public synchronized WriteResult write(DurableLocalReadiness readiness) {
        requireOpen();
        Objects.requireNonNull(readiness, "readiness");
        if (!bookie.equals(readiness.bookieId()) || !incarnation.equals(readiness.storageIncarnation())) {
            return new WriteResult(WriteStatus.CONFLICT);
        }
        try {
            verifyCurrent();
            if (readiness.equals(current)) {
                return new WriteResult(WriteStatus.ALREADY_APPLIED);
            }
            if (current != null && current.readinessGeneration().compareTo(readiness.readinessGeneration()) >= 0) {
                return new WriteResult(WriteStatus.CONFLICT);
            }
            Path pending = directory.resolve("local-readiness.pending");
            try (FileChannel file = FileChannel.open(pending, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
                io.writeFully(file, SegmentLocalReadinessCodec.encode(readiness), 0);
                io.force(file, true);
            }
            Files.move(pending, receipt(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            syncDirectory();
            current = readiness;
            return new WriteResult(WriteStatus.APPLIED);
        } catch (IOException uncertain) {
            // A readable renamed file is not evidence that a failed barrier completed.
            failure = uncertain;
            return new WriteResult(WriteStatus.DURABILITY_UNKNOWN);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        writer.close();
    }
}

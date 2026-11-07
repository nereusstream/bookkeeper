/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;

/**
 * Native root binding files and their own locks. The commissioning caller must separately fence the old process
 * and protect the roots; these files and locks do not fence an unmodified Classic binary.
 * Cold methods serialize actual file I/O with close. The shared service must drain before closing this child.
 */
final class SegmentClassicRootBindings implements AutoCloseable {
    static final String FILE_NAME = "route-claim-binding";
    static final String LOCK_NAME = "route-claim.lock";
    private static final int MAX_MARKER_BYTES = 16384;
    private static final int MAX_COOKIE_BYTES = 1024 * 1024;
    private static final long MAGIC = 0x5557435242303031L; // UWCRB001
    private final RegistrationManager manager;
    private final ServerConfiguration configuration;
    private final SegmentArenaManifest manifest;
    private BookieRouteBinding.Observation nativeBinding;
    private final Cookie master;
    private final List<SegmentDataFileLock> locks = new ArrayList<>();
    private boolean attempted;
    private boolean locked;
    private boolean closing;
    private boolean closed;

    /** No file or lock is opened by construction. The caller retains this child through initialization failures. */
    SegmentClassicRootBindings(ServerConfiguration configuration, RegistrationManager manager,
            SegmentArenaManifest manifest, BookieRouteBinding.Observation nativeBinding) throws IOException {
        if (!(manager instanceof ZKRegistrationManager)) {
            throw new IOException("First common-scope commissioning requires the native ZooKeeper adapter");
        }
        this.manager = manager;
        this.manifest = manifest;
        this.nativeBinding = nativeBinding;
        this.configuration = new ServerConfiguration(configuration)
                .setBookieId(BookieImpl.getBookieId(configuration).toString())
                .setJournalDirsName(configuration.getJournalDirNames().clone())
                .setLedgerDirNames(configuration.getLedgerDirNames().clone());
        String[] index = configuration.getIndexDirNames();
        this.configuration.setIndexDirName(index == null ? null : index.clone());
        BookieRouteBinding binding = nativeBinding.binding();
        if (manifest.classicRoots().isEmpty() || !binding.bookieId().toString().equals(this.configuration.getBookieId())
                || !binding.incarnation().equals(manifest.incarnation())
                || !binding.manifest().equals(manifest.identity())
                || !binding.controlDirectory().equals(manifest.controlDirectory().toString())) {
            throw new IOException("Native binding and complete common storage scope differ");
        }
        master = Cookie.generateCookie(this.configuration).setInstanceId(binding.clusterId()).build();
    }

    /** Read-only preflight; no missing file or directory is repaired. */
    synchronized void verifyExisting() throws IOException {
        requireLive();
        requireNative();
        verifySources();
        verifyMarkers();
    }

    /** Explicit owned initialization only, after native PREPARED and the external old-writer fence. */
    synchronized void openLocks(SegmentFileIo io) throws IOException {
        requireLive();
        if (attempted) {
            throw new IOException("Common root lock initialization already attempted");
        }
        attempted = true;
        requireNative();
        verifySources();
        boolean bound = Files.exists(manifest.controlDirectory().resolve(FILE_NAME), LinkOption.NOFOLLOW_LINKS);
        for (var member : manifest.classicRoots()) {
            bound |= Files.exists(current(member.directory()).resolve(FILE_NAME), LinkOption.NOFOLLOW_LINKS);
        }
        for (var member : manifest.classicRoots()) {
            Path current = current(member.directory());
            Path path = current.resolve(LOCK_NAME);
            boolean existing = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
            boolean prepared = nativeBinding.binding().phase() == BookieRouteBinding.Phase.PREPARED;
            if (existing && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                    || !existing && (!prepared || bound)) {
                throw new IOException("Required Classic common-scope lock is missing or is not a regular file");
            }
            var acquired = existing ? SegmentDataFileLock.existingControlOwner(path)
                    : SegmentDataFileLock.creatingControlOwner(path);
            locks.add(acquired);
            acquired.initialize();
            io.force(acquired.channel(), true);
            IsolatedSegmentFiles.syncDirectory(current);
        }
        requireNative();
        verifySources();
        locked = true;
    }

    /** Existing complete candidates are reverified and synchronized; partial or different candidates are retained. */
    synchronized void createMarkers(SegmentBookieControlLog bookie, SegmentFileIo io) throws IOException {
        requireLive();
        if (!locked || nativeBinding.binding().phase() != BookieRouteBinding.Phase.PREPARED) {
            throw new IOException("Marker creation requires held common roots and native PREPARED");
        }
        bookie.requireCommonBinding(nativeBinding.binding().bindingId(), manifest.identity());
        requireNative();
        verifySources();
        for (int member = -1; member < manifest.classicRoots().size(); member++) {
            Path path = marker(member);
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                verifyMarker(member);
                try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS)) {
                    io.force(channel, true);
                }
            } else {
                if (!bookie.emptyForCommonMarkers()) {
                    throw new IOException("Common marker is missing after Bookie control history was established");
                }
                try (FileChannel channel = FileChannel.open(path, Set.of(StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
                    io.writeFully(channel, encode(member), 0);
                    io.force(channel, true);
                }
            }
            IsolatedSegmentFiles.syncDirectory(path.getParent());
        }
        requireNative();
        verifySources();
        verifyMarkers();
    }

    /** Advance this same held root owner only after the matching native CAS and actual retained control seal. */
    synchronized void adoptEnabled(BookieRouteBinding.Observation enabled, SegmentBookieControlLog bookie)
            throws IOException {
        requireLive();
        if (!locked || enabled.binding().phase() != BookieRouteBinding.Phase.ENABLED
                || !enabled.birthId().equals(nativeBinding.birthId())
                || enabled.birthZxid() != nativeBinding.birthZxid()
                || !enabled.binding().prepared().equals(nativeBinding.binding().prepared())) {
            throw new IOException("Native completion does not retain this same held common scope");
        }
        bookie.requireCommonBinding(enabled.binding().bindingId(), manifest.identity());
        if (!enabled.binding().seal().equals(bookie.bootstrapSeal())) {
            throw new IOException("Native completion does not refer to the actual retained control seal");
        }
        var current = manager.readBookieRouteBinding(enabled.binding().bookieId());
        if (current == null || current.status() != BookieRouteBinding.ReadStatus.FOUND
                || !current.observation().orElseThrow().equals(enabled)) {
            throw new IOException("Native completion cannot be confirmed");
        }
        nativeBinding = enabled;
        verifyExisting();
    }

    synchronized long verifyEnabledOwner(ZKRegistrationManager sourceManager, BookieRouteBinding.Observation source,
            SegmentBookieControlLog bookie) throws IOException {
        requireOwner(sourceManager, source, BookieRouteBinding.Phase.ENABLED, bookie);
        long appliedThrough = bookie.requireSealedBootstrapCut(nativeBinding.binding().seal().orElseThrow());
        verifyExisting();
        return appliedThrough;
    }

    synchronized void verifyPreparedOwner(ZKRegistrationManager sourceManager, BookieRouteBinding.Observation source,
            SegmentBookieControlLog bookie) throws IOException {
        requireOwner(sourceManager, source, BookieRouteBinding.Phase.PREPARED, bookie);
        bookie.unsealedBootstrapClaimLimit();
        verifyExisting();
    }

    /** Only local checks here. The enclosing completion owns the native reads and write callbacks. */
    synchronized void verifyPreparedRootsForCompletion(ZKRegistrationManager sourceManager,
            BookieRouteBinding.Observation source, SegmentBookieControlLog bookie) throws IOException {
        requireOwner(sourceManager, source, BookieRouteBinding.Phase.PREPARED, bookie);
        verifySources();
        verifyMarkers();
    }

    private void requireOwner(ZKRegistrationManager sourceManager, BookieRouteBinding.Observation source,
            BookieRouteBinding.Phase phase, SegmentBookieControlLog bookie) throws IOException {
        requireLive();
        if (!locked || manager != sourceManager || !nativeBinding.equals(source) || source.binding().phase() != phase) {
            throw new IOException("Common owner does not hold the verified native " + phase + " scope");
        }
        bookie.requireCommonBinding(nativeBinding.binding().bindingId(), manifest.identity());
    }

    /** Original configured order, using the canonical directories of the held scope. */
    synchronized ServerConfiguration inventoryConfiguration() throws IOException {
        requireLive();
        if (!locked) {
            throw new IOException("Classic inventory requires the held common roots");
        }
        File[] index = configuration.getIndexDirs();
        return new ServerConfiguration(configuration)
                .setJournalDirsName(inventoryRoots(configuration.getJournalDirs()))
                .setLedgerDirNames(inventoryRoots(configuration.getLedgerDirs()))
                .setIndexDirName(index == null ? null : inventoryRoots(index));
    }

    private static String[] inventoryRoots(File[] configured) throws IOException {
        String[] roots = new String[configured.length];
        for (int i = 0; i < configured.length; i++) {
            Path root = configured[i].toPath().toAbsolutePath();
            current(root);
            roots[i] = root.toString();
        }
        return roots;
    }

    private void verifySources() throws IOException {
        manifest.requireClassicRoots(configuration);
        for (var root : manifest.classicRoots()) {
            Path current = current(root.directory());
            Path cookie = current.resolve("VERSION");
            if (!Files.isRegularFile(cookie, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Original Classic Cookie must exist in every common member");
            }
            MessageDigest hash = sha256();
            try (FileChannel channel = FileChannel.open(cookie, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                long length = channel.size();
                if (length < 1 || length > MAX_COOKIE_BYTES) {
                    throw new IOException("Original Classic Cookie length exceeds its fixed bound");
                }
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                for (long offset = 0; offset < length;) {
                    buffer.clear().limit((int) Math.min(buffer.capacity(), length - offset));
                    SegmentFileIo.readFully(channel, buffer, offset);
                    int read = buffer.remaining();
                    hash.update(buffer);
                    offset += read;
                }
                if (channel.size() != length) {
                    throw new IOException("Original Classic Cookie changed during preflight");
                }
            }
            if (!MessageDigest.isEqual(hash.digest(), nativeBinding.binding().sourceCookie().sha256())) {
                throw new IOException("Local Classic Cookie differs from the commissioned native source");
            }
            try {
                master.verify(Cookie.readFromDirectory(current.toFile()));
            } catch (BookieException mismatch) {
                throw new IOException("Original Classic Cookie does not match the complete configured scope", mismatch);
            }
        }
    }

    private void verifyMarkers() throws IOException {
        for (int member = -1; member < manifest.classicRoots().size(); member++) {
            verifyMarker(member);
            if (member >= 0 && !Files.isRegularFile(current(manifest.classicRoots().get(member).directory())
                    .resolve(LOCK_NAME), LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("An already bound common root is missing its original lock");
            }
        }
    }

    private void verifyMarker(int member) throws IOException {
        Path path = marker(member);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Required common root binding must be an existing regular file");
        }
        if (Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS).stream().anyMatch(permission ->
                permission != PosixFilePermission.OWNER_READ && permission != PosixFilePermission.OWNER_WRITE)) {
            throw new IOException("Common root marker must be private to its protected service credential");
        }
        ByteBuffer bytes;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            long length = channel.size();
            if (length < 60 || length > MAX_MARKER_BYTES) {
                throw new IOException("Common root marker length exceeds its fixed bounds");
            }
            bytes = ByteBuffer.allocate((int) length);
            SegmentFileIo.readFully(channel, bytes, 0);
            if (channel.size() != length) {
                throw new IOException("Common root marker changed during preflight");
            }
        }
        if (!bytes.equals(encode(member))) {
            throw new IOException("Common root marker differs from its native birth, source, manifest or member");
        }
    }

    /** Exact bytes check framing, CRC, schema, root and prepared binding without granting decode authority. */
    private ByteBuffer encode(int member) {
        Path directory = member < 0 ? manifest.controlDirectory() : manifest.classicRoots().get(member).directory();
        int roles = member < 0 ? 0 : manifest.classicRoots().get(member).roles().stream()
                .mapToInt(role -> role.bit).reduce(0, (a, b) -> a | b);
        byte[] root = directory.toString().getBytes(StandardCharsets.UTF_8);
        byte[] binding = nativeBinding.binding().prepared().encode();
        ByteBuffer out = ByteBuffer.allocate(60 + root.length + binding.length);
        out.putLong(MAGIC).putInt(1).putInt(out.capacity()).putInt(member).putInt(roles)
                .putLong(nativeBinding.birthId().getMostSignificantBits())
                .putLong(nativeBinding.birthId().getLeastSignificantBits()).putLong(nativeBinding.birthZxid())
                .putInt(root.length).put(root).putInt(binding.length).put(binding);
        CRC32C crc = new CRC32C();
        crc.update(out.slice(0, out.position()));
        out.putInt((int) crc.getValue());
        return out.flip();
    }

    private Path marker(int member) throws IOException {
        Path parent = member < 0 ? manifest.controlDirectory()
                : current(manifest.classicRoots().get(member).directory());
        if (!parent.equals(parent.toRealPath()) || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Common root marker parent must be an existing canonical directory");
        }
        return parent.resolve(FILE_NAME);
    }

    private static Path current(Path root) throws IOException {
        Path current = root.resolve("current");
        if (!current.equals(current.toRealPath()) || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Original Classic current directory must exist without aliases");
        }
        return current;
    }

    private void requireNative() throws IOException {
        var read = manager.readBookieRouteBinding(nativeBinding.binding().bookieId());
        if (read == null || read.status() != BookieRouteBinding.ReadStatus.FOUND
                || !read.observation().orElseThrow().equals(nativeBinding)) {
            throw new IOException("Required native common-scope observation is missing, changed or unavailable");
        }
    }

    private void requireLive() throws IOException {
        if (closed || closing) {
            throw new IOException("Common root binding child is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closing = true;
        Throwable first = null;
        for (int index = locks.size() - 1; index >= 0; index--) {
            SegmentDataFileLock lease = locks.get(index);
            try {
                lease.close();
                if (!lease.drainResult().drained()) {
                    throw new IOException("Common root lease has not reached its actual resource terminal state");
                }
                locks.remove(index);
            } catch (Throwable failure) {
                if (first == null) {
                    first = failure;
                } else if (first != failure) {
                    boolean retained = false;
                    for (Throwable prior : first.getSuppressed()) {
                        retained |= prior == failure;
                    }
                    if (!retained) {
                        first.addSuppressed(failure);
                    }
                }
            }
        }
        if (first instanceof IOException io) {
            throw io;
        }
        if (first instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (first instanceof Error error) {
            throw error;
        }
        if (first != null) {
            throw new IOException("Common root cleanup failed", first);
        }
        closed = true;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}

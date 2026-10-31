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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicHandoffPaths.Identity;
import org.apache.bookkeeper.common.util.nativeio.NativeIOBootstrap;

/**
 * JDK-only early public-file reads. Relies on the external preinstalled bootstrap baseline; no ACL, Installed,
 * Process, START or drain authority is inferred from these bytes or POSIX checks.
 */
final class SegmentClassicLaunchPreload {
    private record Stamp(Path path, Identity identity, List<Identity> parents, byte[] bytes) { }

    private static final class Read {
        FileChannel channel;
        boolean openStarted;
        boolean closeAttempted;
        Throwable closeFailure;
    }

    /** Drain describes only these early calls/reads; a failed native bootstrap remains unsettled. */
    record Drain(boolean drained, Throwable failure, int retainedReads, Throwable closeFailure,
                 boolean nativeBootstrapAttempted, boolean nativeBootstrapSucceeded) { }

    private final Path root;
    private final String recipeId;
    private final long uid;
    private final long deadline;
    private final boolean production;
    private final List<Read> reads = new ArrayList<>();
    private boolean attempted;
    private boolean initialized;
    private boolean closing;
    private Thread activeThread;
    private Throwable failure;
    private Throwable closeFailure;
    private Stamp recipeFile;
    private Stamp programFile;
    private SegmentClassicLaunchTrust.Recipe recipe;
    private SegmentClassicLaunchProgram program;
    private boolean nativeAttempted;
    private NativeIOBootstrap.Library library;
    private boolean adopted;

    private SegmentClassicLaunchPreload(Path root, String recipeId, long uid, long deadline, boolean production) {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        if (!root.isAbsolute() || !root.normalize().equals(root) || uid < 0 || uid > 0xffff_ffffL) {
            throw new IllegalArgumentException("Invalid early Classic material location");
        }
        this.root = root;
        this.recipeId = recipeId;
        this.uid = uid;
        this.deadline = deadline;
        this.production = production;
    }

    /** Preparation performs no I/O and cannot select a caller-supplied path. */
    static SegmentClassicLaunchPreload production(String recipeId, long deadline) throws IOException {
        return new SegmentClassicLaunchPreload(systemRoot(), recipeId, 0, deadline, true);
    }

    /** Read-only fixture data, never a production load or installed trust source, even for UID zero. */
    static SegmentClassicLaunchPreload forFileIoTests(Path root, String recipeId, long uid, long deadline) {
        return new SegmentClassicLaunchPreload(root, recipeId, uid, deadline, false);
    }

    synchronized void initialize() throws IOException {
        if (attempted || closing) {
            throw new IOException("Classic preload initialization is one-use");
        }
        attempted = true;
        beginOperation();
        try {
            requireTime();
            if (production) {
                SegmentClassicLaunchProgram.environment(System.getenv());
                if (System.getProperties().containsKey("bookkeeper.native.io.library.path")) {
                    throw new IOException("Early controlled startup rejects the ordinary native override");
                }
                SegmentClassicLaunchProgram.validateJvmArguments(ManagementFactory.getRuntimeMXBean()
                        .getInputArguments());
            }
            recipeFile = read(root.resolve(recipeId + ".recipe"), 16,
                    SegmentClassicLaunchTrust.MAX_RECIPE_BYTES);
            recipe = SegmentClassicLaunchTrust.decode(recipeFile.bytes);
            if (!recipeId.equals(recipe.recipeId())) {
                throw new IOException("Early recipe differs from its fixed selector");
            }
            programFile = read(root.resolve(recipeId + ".program"), 16, SegmentClassicLaunchProgram.MAX_BYTES);
            program = SegmentClassicLaunchProgram.decode(programFile.bytes);
            if (!recipe.program().equals(SegmentClassicLaunchProgram.identity(programFile.bytes))) {
                throw new IOException("Early program bytes differ from the selected recipe");
            }
            if (production) {
                if (!program.os().equals(systemOs()) || !program.architecture().equals(systemArch())
                        || !program.files().get(program.aclJniFile()).path().equals(root.resolve("bootstrap")
                                .resolve(systemOs() + "-" + systemArch())
                                .resolve(systemOs().equals("macos") ? "libnative_io.dylib" : "libnative_io.so"))
                        || !program.jvmArguments().equals(ManagementFactory.getRuntimeMXBean().getInputArguments())) {
                    throw new IOException("Early program platform, fixed JNI reference or actual JVM arguments differ");
                }
            }
            requireOriginal();
            initialized = true;
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            activeThread = null;
        }
    }

    /** Caller retains this owner through post-load verification; the library result alone grants no startup. */
    synchronized void loadNative() throws IOException {
        requireActive();
        if (!production || nativeAttempted) {
            throw new IOException("Fixture preload cannot load production JNI; native bootstrap is one-use");
        }
        beginOperation();
        try {
            requireOriginal();
            var file = program.files().get(program.aclJniFile());
            nativeAttempted = true;
            library = NativeIOBootstrap.loadControlledClassic(file.length(), file.sha256().value(), deadline);
            requireOriginal();
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            activeThread = null;
        }
    }

    synchronized void adopt(boolean installed) throws IOException {
        requireActive();
        if (adopted || installed && (!production || library == null)) {
            throw new IOException("One original fixed preload/native result required for production trust");
        }
        beginOperation();
        try {
            requireOriginal();
            if (installed && NativeIOBootstrap.controlledLibrary() != library) {
                throw new IOException("Post-load trust cannot use a different native bootstrap result");
            }
            adopted = true;
        } finally {
            activeThread = null;
        }
    }

    synchronized void match(Path path, Identity identity, byte[] bytes) throws IOException {
        requireActive();
        beginOperation();
        try {
            Stamp stamp = path.equals(recipeFile.path) ? recipeFile
                    : path.equals(programFile.path) ? programFile : null;
            if (stamp != null && (!stamp.identity.equals(identity) || !Arrays.equals(stamp.bytes, bytes))) {
                throw new IOException("Post-load material differs from its original preload bytes or identity");
            }
            requireOriginal();
        } finally {
            activeThread = null;
        }
    }

    synchronized void verifyOriginal(long deadlineNanos) throws IOException {
        requireActive();
        if (deadlineNanos != deadline) {
            throw new IOException("Original preload deadline must be retained");
        }
        beginOperation();
        try {
            requireOriginal();
        } finally {
            activeThread = null;
        }
    }

    synchronized void requireActive() throws IOException {
        if (!initialized || closing || failure != null || !reads.isEmpty()) {
            throw new IOException("Original Classic preload is unavailable or closing", failure);
        }
        requireTime();
    }

    synchronized SegmentClassicLaunchProgram program() throws IOException {
        requireActive();
        return program;
    }

    Path root() {
        return root;
    }

    String recipeId() {
        return recipeId;
    }

    long uid() {
        return uid;
    }

    long deadline() {
        return deadline;
    }

    synchronized Drain closeAndDrain() {
        if (activeThread == Thread.currentThread()) {
            throw new IllegalStateException("Early Classic reader cannot drain its own executing call");
        }
        closing = true;
        return new Drain(reads.isEmpty() && (!nativeAttempted || library != null), failure, reads.size(),
                closeFailure, nativeAttempted, library != null);
    }

    private void requireOriginal() throws IOException {
        requireTime();
        for (Stamp file : List.of(recipeFile, programFile)) {
            if (!file.identity.equals(checked(file.path)) || !file.parents.equals(parents(file.path))) {
                throw new IOException("Original early material or parent identity changed");
            }
        }
        requireTime();
    }

    private void beginOperation() {
        if (activeThread != null) {
            throw new IllegalStateException("Early Classic owner cannot reenter its executing operation");
        }
        activeThread = Thread.currentThread();
    }

    private Stamp read(Path path, int minimum, int maximum) throws IOException {
        requireTime();
        Identity original = checked(path);
        if (original.directory() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || original.uid() != uid || original.links() != 1 || (original.mode() & 022) != 0
                || original.size() < minimum || original.size() > maximum) {
            throw new IOException("Early material must be a protected single-link bounded existing file");
        }
        var chain = parents(path);
        var current = new Read();
        reads.add(current);
        byte[] bytes = new byte[(int) original.size()];
        Throwable operationFailure = null;
        try {
            current.openStarted = true;
            current.channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            if (current.channel.size() != original.size()) {
                throw new IOException("Early material changed during open");
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                requireTime();
                if (current.channel.read(buffer) <= 0) {
                    throw new IOException("Early material read made no progress before declared end");
                }
            }
            if (current.channel.size() != original.size()) {
                throw new IOException("Early material size changed during read");
            }
        } catch (IOException | RuntimeException | Error problem) {
            operationFailure = problem;
            throw problem;
        } finally {
            if (current.channel != null) {
                current.closeAttempted = true;
                try {
                    current.channel.close();
                    reads.remove(current);
                } catch (IOException | RuntimeException | Error problem) {
                    current.closeFailure = problem;
                    if (closeFailure == null) {
                        closeFailure = problem;
                    }
                    if (operationFailure == null) {
                        throw problem;
                    }
                    if (operationFailure != problem) {
                        operationFailure.addSuppressed(problem);
                    }
                }
            }
        }
        requireTime();
        if (!original.equals(checked(path)) || !chain.equals(parents(path))) {
            throw new IOException("Early material identity changed during read");
        }
        return new Stamp(path, original, chain, bytes);
    }

    private Identity checked(Path path) throws IOException {
        Identity identity = SegmentClassicHandoffPaths.fileIdentity(path);
        int type = identity.mode() & 0170000;
        if (type != 0100000 && type != 0040000
                || identity.uid() != uid && !(identity.directory() && identity.uid() == 0)) {
            throw new IOException("Early material requires original Unix regular files and protected directories");
        }
        return identity;
    }

    private List<Identity> parents(Path path) throws IOException {
        var result = new ArrayList<Identity>();
        Identity child = checked(path);
        for (Path parent = path.getParent(); parent != null; parent = parent.getParent()) {
            Identity identity = checked(parent);
            if (!identity.directory() || (identity.mode() & 022) != 0
                    && ((identity.mode() & 01000) == 0 || child.uid() != identity.uid() && child.uid() != uid)) {
                throw new IOException("Early material parent permits replacement");
            }
            result.add(identity.parent());
            child = identity;
        }
        return List.copyOf(result);
    }

    private void requireTime() throws IOException {
        if (deadline == Long.MAX_VALUE || deadline - System.nanoTime() <= 0) {
            throw new IOException("Original Classic preload deadline elapsed or is unbounded");
        }
    }

    private static Path systemRoot() throws IOException {
        return Path.of(systemOs().equals("macos") ? "/Library/Application Support/BookKeeper/classic-child"
                : "/etc/bookkeeper/unified-wal/classic-child");
    }

    private static String systemOs() throws IOException {
        return switch (System.getProperty("os.name", "").toLowerCase(Locale.ROOT)) {
            case "mac os x" -> "macos";
            case "linux" -> "linux";
            default -> throw new IOException("Unsupported controlled Classic operating system");
        };
    }

    private static String systemArch() throws IOException {
        return switch (System.getProperty("os.arch", "").toLowerCase(Locale.ROOT)) {
            case "aarch64", "arm64" -> "aarch64";
            case "amd64", "x86_64" -> "x86_64";
            default -> throw new IOException("Unsupported controlled Classic architecture");
        };
    }
}

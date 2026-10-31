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
package org.apache.bookkeeper.common.util.nativeio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.bookkeeper.common.util.nativelib.NativeUtils;
import org.apache.commons.lang3.SystemUtils;

/**
 * One native-io load attempt in the fixed application's ClassLoader. Controlled loading relies on an externally
 * trusted preinstalled JDK, child code and fixed library. POSIX/hash checks here do not establish ACL or startup trust.
 */
public final class NativeIOBootstrap {
    private enum State {
        UNTOUCHED, ORDINARY_LOADING, ORDINARY_LOADED, ORDINARY_FAILED,
        CONTROLLED_LOADING, CONTROLLED_LOADED, CONTROLLED_FAILED
    }

    private record Identity(long device, long inode, long uid, long gid, int mode, long links, long size,
                            FileTime modified, FileTime changed) { }

    /** Actual loaded-file facts only; never a child startup, drain or adoption permit. */
    public static final class Library {
        private final Path path;
        private final long length;
        private final byte[] sha256;
        private final boolean installed;
        private final Identity original;
        private final List<Identity> parents;
        private final long uid;

        private Library(Path path, long length, byte[] sha256, boolean installed, Identity original,
                        List<Identity> parents, long uid) {
            this.path = path;
            this.length = length;
            this.sha256 = sha256.clone();
            this.installed = installed;
            this.original = original;
            this.parents = parents;
            this.uid = uid;
        }

        public Path path() {
            return path;
        }

        public long length() {
            return length;
        }

        public byte[] sha256() {
            return sha256.clone();
        }

        void requireUnchanged() throws IOException {
            if (!original.equals(identity(path, uid)) || !parents.equals(parents(path, uid))) {
                throw new IOException("Original loaded native-io file or parent identity no longer matches");
            }
        }
    }

    private static final class Read {
        FileChannel channel;
        boolean closeAttempted;
        Throwable closeFailure;
    }

    // This lock protects transitions only, never file I/O, class initialization or System.load.
    private static final Object GATE = new Object();
    private static State state = State.UNTOUCHED;
    private static Throwable firstFailure;
    private static Read read;
    private static boolean loadEntered;
    private static boolean loadReturned;
    private static Library library;

    private NativeIOBootstrap() { }

    /**
     * Load only the fixed UID-0 installation, once. The deadline is the original local monotonic startup deadline;
     * it must not be renewed between phases. Timeout does not cancel a native load already executing.
     */
    public static Library loadControlledClassic(long expectedLength, byte[] expectedSha256, long deadlineNanos)
            throws IOException {
        return controlled(expectedLength, expectedSha256, deadlineNanos, null, 0);
    }

    /** Observe the original production load and unchanged file/parents; never ACL, program/configuration or START. */
    public static Library controlledLibrary() throws IOException {
        requireApplicationLoader();
        Library original;
        synchronized (GATE) {
            if (state != State.CONTROLLED_LOADED || library == null || !library.installed) {
                throw unavailable("No completed fixed-installation controlled native-io load");
            }
            original = library;
        }
        original.requireUnchanged();
        return original;
    }

    /** Actual library-I/O fixture only. It can never produce the production result returned by controlledLibrary. */
    static Library forLibraryIoTests(Path path, long uid, long length, byte[] sha256, long deadlineNanos)
            throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Fixture path required");
        }
        return controlled(length, sha256, deadlineNanos, path, uid);
    }

    private static Library controlled(long expectedLength, byte[] expectedSha256, long deadlineNanos,
                                      Path fixture, long uid) throws IOException {
        synchronized (GATE) {
            if (state != State.UNTOUCHED) {
                if (state == State.CONTROLLED_LOADING) {
                    failControlled(new IOException("Concurrent or reentrant controlled native-io load"));
                }
                throw unavailable("Native-io loading is one-use and cannot upgrade an ordinary attempt");
            }
            state = State.CONTROLLED_LOADING;
        }
        try {
            requireApplicationLoader();
            if (expectedLength <= 0 || expectedSha256 == null || expectedSha256.length != 32
                    || uid < 0 || uid > 0xffff_ffffL) {
                throw new IOException("Expected native-io length and SHA-256 are required");
            }
            byte[] expected = expectedSha256.clone();
            rejectOverrides();
            requireTime(deadlineNanos);
            Path path = fixture == null ? fixedPath() : fixture;
            if (!path.isAbsolute() || !path.normalize().equals(path)) {
                throw new IOException("Controlled native-io path must be absolute and canonical");
            }
            var parents = parents(path, uid);
            Identity original = identity(path, uid);
            if ((original.mode & 0170000) != 0100000 || original.links != 1 || (original.mode & 022) != 0
                    || original.uid != uid || original.size != expectedLength) {
                throw new IOException("Controlled native-io file identity, mode or expected length differs");
            }
            byte[] actual = hash(path, original, expectedLength, uid, deadlineNanos);
            requireTime(deadlineNanos);
            if (!MessageDigest.isEqual(expected, actual) || !original.equals(identity(path, uid))
                    || !parents.equals(parents(path, uid))) {
                throw new IOException("Controlled native-io SHA-256, file or parent identity changed");
            }
            synchronized (GATE) {
                requireActive();
                requireTime(deadlineNanos);
                loadEntered = true;
            }
            System.load(path.toString());
            synchronized (GATE) {
                loadReturned = true;
            }
            requireTime(deadlineNanos);
            if (!original.equals(identity(path, uid)) || !parents.equals(parents(path, uid))) {
                throw new IOException("Controlled native-io file or parent changed during System.load");
            }
            var result = new Library(path, expectedLength, actual, fixture == null, original, parents, uid);
            synchronized (GATE) {
                requireActive();
                requireTime(deadlineNanos);
                library = result;
                state = State.CONTROLLED_LOADED;
            }
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (GATE) {
                failControlled(failure);
            }
            throw failure;
        }
    }

    /** Called only by NativeIOJni's static initializer, in its own ClassLoader. */
    static void initializeJni() {
        synchronized (GATE) {
            if (state == State.CONTROLLED_LOADED) {
                return; // Already loaded by this exact class domain. Never consult the ordinary selector again.
            }
            if (state != State.UNTOUCHED) {
                var failure = new IllegalStateException("JNI initialized during an unavailable native-io load");
                if (state == State.CONTROLLED_LOADING) {
                    failControlled(failure);
                }
                throw failure;
            }
            state = State.ORDINARY_LOADING;
        }
        try {
            ordinaryLoad();
            synchronized (GATE) {
                state = State.ORDINARY_LOADED;
            }
        } catch (RuntimeException | Error failure) {
            synchronized (GATE) {
                state = State.ORDINARY_FAILED;
                firstFailure = failure;
            }
            throw failure;
        }
    }

    private static void ordinaryLoad() {
        String explicitPath = NativeIOLibraryPath.configuredLibraryPath();
        if (explicitPath != null) {
            System.load(explicitPath);
            return;
        }
        List<String> candidates = NativeIOLibraryPath.currentPlatformLibraryCandidates();
        if (candidates.isEmpty()) {
            throw new IllegalStateException("No native-io JNI library candidates found for platform "
                    + SystemUtils.OS_NAME + "/" + SystemUtils.OS_ARCH);
        }
        Throwable lastFailure = null;
        for (String candidate : candidates) {
            try {
                NativeUtils.loadLibraryFromJar(candidate);
                return;
            } catch (Exception | UnsatisfiedLinkError failure) {
                lastFailure = failure;
            }
        }
        throw new IllegalStateException("Failed to load any native-io JNI library candidate for platform "
                + SystemUtils.OS_NAME + "/" + SystemUtils.OS_ARCH, lastFailure);
    }

    private static void requireApplicationLoader() throws IOException {
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        if (loader == null || !loader.getClass().getName().equals("jdk.internal.loader.ClassLoaders$AppClassLoader")
                || NativeIOBootstrap.class.getClassLoader() != loader || NativeIOImpl.class.getClassLoader() != loader
                || NativeIOJni.class.getClassLoader() != loader) {
            throw new IOException("Controlled native-io requires the fixed application's JDK system ClassLoader");
        }
    }

    private static void rejectOverrides() throws IOException {
        if (System.getProperties().containsKey("bookkeeper.native.io.library.path")) {
            throw new IOException("Controlled native-io rejects the ordinary system-property override");
        }
        for (String key : System.getenv().keySet()) {
            if (key.equals("BOOKKEEPER_NATIVE_IO_LIBRARY_PATH") || key.equals("JAVA_TOOL_OPTIONS")
                    || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS") || key.equals("CLASSPATH")
                    || key.startsWith("LD_") || key.startsWith("DYLD_")) {
                throw new IOException("Controlled native-io rejects an injected launch environment");
            }
        }
    }

    private static Path fixedPath() throws IOException {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String target = switch (arch) {
            case "aarch64", "arm64" -> "aarch64";
            case "amd64", "x86_64" -> "x86_64";
            default -> throw new IOException("Unsupported controlled native-io architecture");
        };
        if (os.equals("mac os x")) {
            return Path.of("/Library/Application Support/BookKeeper/classic-child/bootstrap",
                    "macos-" + target, "libnative_io.dylib");
        }
        if (os.equals("linux")) {
            return Path.of("/etc/bookkeeper/unified-wal/classic-child/bootstrap",
                    "linux-" + target, "libnative_io.so");
        }
        throw new IOException("Unsupported controlled native-io operating system");
    }

    private static Identity identity(Path path, long uid) throws IOException {
        Map<String, Object> a = Files.readAttributes(path,
                "unix:dev,ino,uid,gid,mode,nlink,size,lastModifiedTime,ctime", LinkOption.NOFOLLOW_LINKS);
        var id = new Identity(((Number) a.get("dev")).longValue(), ((Number) a.get("ino")).longValue(),
                ((Number) a.get("uid")).longValue(), ((Number) a.get("gid")).longValue(),
                ((Number) a.get("mode")).intValue(), ((Number) a.get("nlink")).longValue(),
                ((Number) a.get("size")).longValue(), (FileTime) a.get("lastModifiedTime"), (FileTime) a.get("ctime"));
        int type = id.mode & 0170000;
        if (type != 0100000 && type != 0040000 || id.uid != uid && !(type == 0040000 && id.uid == 0)) {
            throw new IOException("Controlled native-io path is an alias, special file or has an untrusted owner");
        }
        return id;
    }

    private static List<Identity> parents(Path path, long uid) throws IOException {
        var result = new ArrayList<Identity>();
        Identity child = identity(path, uid);
        for (Path parent = path.getParent(); parent != null; parent = parent.getParent()) {
            Identity current = identity(parent, uid);
            if ((current.mode & 0170000) != 0040000 || (current.mode & 022) != 0
                    && ((current.mode & 01000) == 0 || child.uid != current.uid && child.uid != uid)) {
                throw new IOException("Controlled native-io parent permits replacement");
            }
            // Directory entry access need not preserve timestamps while an unrelated sibling is read/created.
            result.add(new Identity(current.device, current.inode, current.uid, current.gid, current.mode,
                    0, 0, null, null));
            child = current;
        }
        return List.copyOf(result);
    }

    private static byte[] hash(Path path, Identity original, long length, long uid, long deadline) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IOException("Required native-io SHA-256 unavailable", unavailable);
        }
        var current = new Read();
        synchronized (GATE) {
            requireActive();
            read = current; // Retain before acquisition, including an open that throws without returning a handle.
        }
        Throwable failure = null;
        try {
            requireTime(deadline);
            current.channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
            if (current.channel.size() != length || !original.equals(identity(path, uid))) {
                throw new IOException("Controlled native-io file changed during open");
            }
            ByteBuffer scratch = ByteBuffer.allocate(64 * 1024);
            long remaining = length;
            while (remaining > 0) {
                requireTime(deadline);
                synchronized (GATE) {
                    requireActive();
                }
                scratch.clear().limit((int) Math.min(scratch.capacity(), remaining));
                int count = current.channel.read(scratch);
                if (count <= 0) {
                    throw new IOException("Controlled native-io read made no progress before declared end");
                }
                remaining -= count;
                scratch.flip();
                digest.update(scratch);
            }
            if (current.channel.size() != length || !original.equals(identity(path, uid))) {
                throw new IOException("Controlled native-io file changed during hashing");
            }
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            if (current.channel != null) {
                current.closeAttempted = true;
                try {
                    current.channel.close();
                    synchronized (GATE) {
                        read = null;
                    }
                } catch (IOException | RuntimeException | Error closeFailure) {
                    current.closeFailure = closeFailure;
                    if (failure == null) {
                        throw closeFailure;
                    }
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
        }
        requireTime(deadline);
        return digest.digest();
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline == Long.MAX_VALUE || deadline - System.nanoTime() <= 0) {
            throw new IOException("Original controlled native-io startup deadline elapsed or is unbounded");
        }
    }

    private static void requireActive() throws IOException {
        if (state != State.CONTROLLED_LOADING) {
            throw unavailable("Controlled native-io attempt failed while its original call was executing");
        }
    }

    private static void failControlled(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        }
        state = State.CONTROLLED_FAILED;
    }

    private static IOException unavailable(String message) {
        return new IOException(message + " (controlledLoadEntered=" + loadEntered
                + ", controlledLoadReturned=" + loadReturned + ")",
                firstFailure);
    }
}

/*
 *
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
 *
 */
package org.apache.bookkeeper.common.util.nativeio;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

/**
 * NativeIOImpl.
 */
public class NativeIOImpl implements NativeIO {
    @Override
    public int open(String pathname, int flags, int mode) throws NativeIOException {
        int res = NativeIOJni.open(pathname, flags, mode);
        return res;
    }

    @Override
    public int fsync(int fd) throws NativeIOException {
        return NativeIOJni.fsync(fd);
    }

    @Override
    public int fallocate(int fd, int mode, long offset, long len) throws NativeIOException {
        return NativeIOJni.fallocate(fd, mode, offset, len);
    }

    @Override
    public int posix_fadvise(int fd, long offset, long len, int flag) throws NativeIOException {
        return NativeIOJni.posix_fadvise(fd, offset, len, flag);
    }

    @Override
    public long lseek(int fd, long offset, int whence) throws NativeIOException {
        return NativeIOJni.lseek(fd, offset, whence);
    }

    @Override
    public int close(int fd) throws NativeIOException {
        return NativeIOJni.close(fd);
    }

    @Override
    public int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException {
        return NativeIOJni.pwrite(fd, pointer, count, offset);
    }

    @Override
    public long posix_memalign(int alignment, int size) throws NativeIOException {
        return NativeIOJni.posix_memalign(alignment, size);
    }

    @Override
    public void free(long pointer) throws NativeIOException {
        NativeIOJni.free(pointer);
    }

    @Override
    public long pread(int fd, long pointer, long size, long offset) throws NativeIOException {
        return NativeIOJni.pread(fd, pointer, size, offset);
    }

    @Override
    public int renameNoReplace(String source, String target) throws NativeIOException {
        try {
            int result = NativeIOJni.renameNoReplace(source, target);
            if (result != 0) {
                throw new NativeIOException("Atomic rename returned an unknown native outcome");
            }
            return result;
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException(
                    "Atomic rename without replacement is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public SymbolicLinkObservation observeSymbolicLink(String path) throws NativeIOException {
        try {
            var result = NativeIOJni.observeSymbolicLink(path);
            if (result == null) {
                throw new NativeIOException("Unknown native symbolic-link observation outcome");
            }
            return result;
        } catch (IllegalArgumentException invalid) {
            var failure = new NativeIOException("Invalid native symbolic-link observation");
            failure.initCause(invalid);
            throw failure;
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Symbolic-link observation is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public boolean hasExtendedAcl(String path) throws NativeIOException {
        try {
            int result = NativeIOJni.hasExtendedAcl(path);
            if (result != 0 && result != 1) {
                throw new NativeIOException("Unknown native ACL query outcome");
            }
            return result == 1;
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("ACL presence query is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public ProcessCredentials currentProcessCredentials() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentProcessCredentials();
            if (actual == null || actual.length < 4 || actual.length > 68) {
                throw new NativeIOException("Unknown current process credential query outcome");
            }
            try {
                return new ProcessCredentials(actual[0], actual[1], actual[2], actual[3], credentialGroups(actual, 4));
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current process credentials");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current process credentials are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxThreadCredentials currentLinuxThreadCredentials() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentLinuxThreadCredentials();
            if (actual == null || actual.length < 6 || actual.length > 70) {
                throw new NativeIOException("Unknown current Linux thread credential query outcome");
            }
            try {
                var credentials = new ProcessCredentials(actual[0], actual[1], actual[3], actual[4],
                        credentialGroups(actual, 6));
                return new LinuxThreadCredentials(credentials, actual[2], actual[5]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current Linux thread credentials");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux thread credentials are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    private static ArrayList<Long> credentialGroups(long[] actual, int start) {
        var groups = new ArrayList<Long>(actual.length - start);
        for (int i = start; i < actual.length; i++) {
            groups.add(actual[i]);
        }
        return groups;
    }

    @Override
    public LinuxThreadFilesystemIds currentLinuxThreadFilesystemIds() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentLinuxThreadFilesystemIds();
            if (actual == null || actual.length != 2) {
                throw new NativeIOException("Unknown current Linux thread filesystem ID query outcome");
            }
            try {
                return new LinuxThreadFilesystemIds(actual[0], actual[1]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current Linux thread filesystem IDs");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException(
                    "Current Linux thread filesystem IDs are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public KernelIdentity currentKernelIdentity() throws NativeIOException {
        try {
            String[] actual = NativeIOJni.currentKernelIdentity();
            if (actual == null || actual.length != 4) {
                throw new NativeIOException("Unknown current kernel identity query outcome");
            }
            try {
                return new KernelIdentity(actual[0], actual[1], actual[2], actual[3]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current kernel identity");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current kernel identity is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public long currentCgroupRootFileSystemType() throws NativeIOException {
        try {
            return NativeIOJni.currentCgroupRootFileSystemType();
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException(
                    "Current cgroup root filesystem type is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public UUID currentLinuxBootId() throws NativeIOException {
        try {
            String text = NativeIOJni.currentLinuxBootId();
            try {
                UUID actual = UUID.fromString(text);
                if (!actual.toString().equals(text) || actual.equals(new UUID(0, 0))) {
                    throw new IllegalArgumentException("Noncanonical or zero boot ID");
                }
                return actual;
            } catch (IllegalArgumentException | NullPointerException invalid) {
                var failure = new NativeIOException("Invalid native current Linux boot ID");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux boot ID is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxProcessBirth currentLinuxProcessBirth() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentLinuxProcessBirth();
            if (actual == null || actual.length != 3) {
                throw new NativeIOException("Unknown current Linux process birth query outcome");
            }
            try {
                return new LinuxProcessBirth(actual[0], actual[1], actual[2]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current Linux process birth");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux process birth is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxThreadNamespaces currentLinuxThreadNamespaces() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentLinuxThreadNamespaces();
            if (actual == null || actual.length != 2) {
                throw new NativeIOException("Unknown current Linux thread namespace query outcome");
            }
            try {
                return new LinuxThreadNamespaces(actual[0], actual[1]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native current Linux thread namespaces");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux thread namespaces are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxFilesystemView currentLinuxFilesystemView() throws NativeIOException {
        try {
            return decodeFilesystemView(NativeIOJni.currentLinuxFilesystemView(), 4);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux filesystem view is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxPid1FilesystemView currentLinuxPid1FilesystemView() throws NativeIOException {
        try {
            long[] actual = NativeIOJni.currentLinuxPid1FilesystemView();
            var view = decodeFilesystemView(actual, 5);
            return new LinuxPid1FilesystemView(view, actual[4]);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Linux PID 1 filesystem view is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    private static LinuxFilesystemView decodeFilesystemView(long[] values, int expectedLength)
            throws NativeIOException {
        if (values == null || values.length != expectedLength) {
            throw new NativeIOException("Unknown current Linux filesystem view outcome");
        }
        try {
            return new LinuxFilesystemView(values[0], new FileIdentity(values[1], values[2]), values[3]);
        } catch (IllegalArgumentException invalid) {
            var failure = new NativeIOException("Invalid native current Linux filesystem view");
            failure.initCause(invalid);
            throw failure;
        }
    }

    @Override
    public NativeEnvironmentSample currentLinuxNativeEnvironment() throws NativeIOException {
        try {
            return switch (NativeIOJni.currentLinuxNativeEnvironment()) {
                case 1 -> NativeEnvironmentSample.EMPTY_SAMPLED;
                case 2 -> NativeEnvironmentSample.NONEMPTY_SAMPLED;
                default -> throw new NativeIOException("Unknown current Linux native environment sample");
            };
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Current Linux native environment sampling is unavailable");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public JdkProcessLaunchObservation jdkProcessLaunch() throws NativeIOException {
        try {
            var type = Class.forName("java.lang.ProcessImpl", false, null);
            var mechanism = Class.forName("java.lang.ProcessImpl$LaunchMechanism", false, null);
            if (type.getClassLoader() != null || mechanism.getClassLoader() != null
                    || type.getModule() != Object.class.getModule() || mechanism.getModule() != Object.class.getModule()
                    || !mechanism.isEnum()) {
                throw new NativeIOException("Unsupported ProcessImpl outside bootstrap java.base");
            }
            // Explicitly acknowledge initialization before the native static-field read; this can block or fail.
            try {
                Class.forName("java.lang.ProcessImpl", true, null);
            } catch (VirtualMachineError | ThreadDeath fatal) {
                throw fatal;
            } catch (Error initialization) {
                // ProcessImpl may throw a plain Error for an unsupported selection during initialization.
                var failure = new NativeIOException("Cached JDK process launch initialization failed");
                failure.initCause(initialization);
                throw failure;
            }
            var result = NativeIOJni.jdkProcessLaunch();
            if (result == null) {
                throw new NativeIOException("Unknown cached JDK process launch observation outcome");
            }
            return result;
        } catch (ClassNotFoundException | LinkageError unavailable) {
            var failure = new NativeIOException("Cached JDK process launch observation is unavailable");
            failure.initCause(unavailable);
            throw failure;
        } catch (IllegalArgumentException invalid) {
            var failure = new NativeIOException("Invalid cached JDK process launch observation");
            failure.initCause(invalid);
            throw failure;
        }
    }

    @Override
    public UnixPeerCredentials unixPeerCredentials(java.nio.channels.SocketChannel channel) throws NativeIOException {
        try {
            long[] observed = NativeIOJni.unixPeerCredentials(channel);
            if (observed == null || observed.length != 3) {
                throw new NativeIOException("Unknown native Linux Unix peer credential outcome");
            }
            try {
                return new UnixPeerCredentials(observed[0], observed[1], observed[2]);
            } catch (IllegalArgumentException invalid) {
                var failure = new NativeIOException("Invalid native Linux Unix peer credentials");
                failure.initCause(invalid);
                throw failure;
            }
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Linux Unix peer credentials are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public FileIdentity fileIdentity(java.nio.channels.FileChannel channel) throws NativeIOException {
        try {
            long[] observed = NativeIOJni.fileIdentity(channel);
            if (observed == null || observed.length != 2) {
                throw new NativeIOException("Unknown original file-channel identity outcome");
            }
            return new FileIdentity(observed[0], observed[1]);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original file-channel identity is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public DirectoryFileSystem directoryFileSystem(java.nio.channels.FileChannel channel) throws NativeIOException {
        try {
            long[] observed = NativeIOJni.directoryFileSystem(channel);
            if (observed == null || observed.length != 3) {
                throw new NativeIOException("Unknown original directory filesystem outcome");
            }
            return new DirectoryFileSystem(new FileIdentity(observed[0], observed[1]), observed[2]);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original directory filesystem is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxCgroupMount linuxCgroupMount(java.nio.channels.FileChannel channel) throws NativeIOException {
        try {
            long[] values = NativeIOJni.linuxCgroupMount(channel);
            return decodeCgroupMount(values, 3);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original Linux cgroup mount is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxCgroupMembership linuxCgroupMembership(java.nio.channels.FileChannel root) throws NativeIOException {
        try {
            Object[] observed = NativeIOJni.linuxCgroupMembership(root);
            if (observed == null || observed.length != 2 || !(observed[0] instanceof long[] values)
                    || !(observed[1] instanceof String path) || !path.startsWith("/") || path.length() > 4095) {
                throw new NativeIOException("Invalid original Linux cgroup membership observation");
            }
            var mount = decodeCgroupMount(values, 5);
            return new LinuxCgroupMembership(mount, new FileIdentity(values[3], values[4]), path);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original Linux cgroup membership is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxCgroupEvents linuxCgroupEvents(java.nio.channels.FileChannel directory) throws NativeIOException {
        try {
            long[] values = NativeIOJni.linuxCgroupEvents(directory);
            if (values == null || values.length != 4 || values[2] <= 0 || values[2] > Integer.MAX_VALUE
                    || values[3] != 0 && values[3] != 1) {
                throw new NativeIOException("Invalid original Linux cgroup events observation");
            }
            return new LinuxCgroupEvents(new FileIdentity(values[0], values[1]), values[2], values[3] == 1);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original Linux cgroup events are unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxExt4Mount linuxExt4Mount(java.nio.channels.FileChannel root, String canonicalRoot)
            throws NativeIOException {
        byte[] bytes = canonicalStoragePath(canonicalRoot, true);
        try {
            Object[] observed = NativeIOJni.linuxExt4Mount(root, bytes);
            if (observed == null || observed.length != 2 || !(observed[0] instanceof long[] values)
                    || values.length != 5 || values[2] <= 0 || values[2] > Integer.MAX_VALUE
                    || values[3] <= 0 || values[3] > Integer.MAX_VALUE
                    || values[4] <= 0 || values[4] > 0xffff_ffffL || !(observed[1] instanceof String point)
                    || !point.equals("/") && !point.equals(canonicalRoot) && !canonicalRoot.startsWith(point + "/")) {
                throw new NativeIOException("Invalid original Linux ext4 mount observation");
            }
            canonicalStoragePath(point, true);
            return new LinuxExt4Mount(new FileIdentity(values[0], values[1]), values[2], values[3], values[4], point);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original Linux ext4 mount is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    @Override
    public LinuxStorageMember linuxStorageMember(java.nio.channels.FileChannel root, String relativePath)
            throws NativeIOException {
        byte[] bytes = canonicalStoragePath(relativePath, false);
        try {
            long[] values = NativeIOJni.linuxStorageMember(root, bytes);
            if (values == null || values.length != 4 || values[2] <= 0 || values[2] > Integer.MAX_VALUE
                    || values[3] != 0 && values[3] != 1) {
                throw new NativeIOException("Invalid original Linux retained member observation");
            }
            return new LinuxStorageMember(new FileIdentity(values[0], values[1]), values[2], values[3] == 1);
        } catch (LinkageError unavailable) {
            var failure = new NativeIOException("Original Linux retained member is unavailable in this JNI library");
            failure.initCause(unavailable);
            throw failure;
        }
    }

    private static byte[] canonicalStoragePath(String path, boolean absolute) throws NativeIOException {
        if (path == null || path.isEmpty() || path.length() > 4095 || path.startsWith("/") != absolute
                || path.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(path)) {
            throw new NativeIOException("Invalid canonical UTF-8 storage path");
        }
        if (!path.equals("/")) {
            for (String part : (absolute ? path.substring(1) : path).split("/", -1)) {
                if (part.isEmpty() || part.equals(".") || part.equals("..")) {
                    throw new NativeIOException("Invalid canonical UTF-8 storage path component");
                }
            }
        }
        byte[] bytes = path.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 4095) {
            throw new NativeIOException("Canonical storage path exceeds the byte limit");
        }
        return bytes;
    }

    private static LinuxCgroupMount decodeCgroupMount(long[] values, int expectedLength) throws NativeIOException {
        if (values == null || values.length != expectedLength
                || values[0] <= 0 || values[0] > Integer.MAX_VALUE
                || values[1] <= 0 || values[1] > Integer.MAX_VALUE
                || values[2] <= 0 || values[2] > 0xffff_ffffL) {
            throw new NativeIOException("Invalid original Linux cgroup mount observation");
        }
        return new LinuxCgroupMount(values[0], values[1], values[2]);
    }
}

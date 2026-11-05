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

import java.util.ArrayList;

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
            var groups = new ArrayList<Long>(actual.length - 4);
            for (int i = 4; i < actual.length; i++) {
                groups.add(actual[i]);
            }
            try {
                return new ProcessCredentials(actual[0], actual[1], actual[2], actual[3], groups);
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
}

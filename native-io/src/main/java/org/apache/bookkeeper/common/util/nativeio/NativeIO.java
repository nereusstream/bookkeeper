/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.util.nativeio;

import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

/**
 * NativeIO API.
 */
public interface NativeIO {

    /** Actual descriptor facts only; every raw dev/ino bit pattern, including zero, is valid data. */
    record FileIdentity(long device, long inode) { }

    /** This process's uname view only; not host namespace, hardware or deployment qualification. */
    record KernelIdentity(String sysname, String machine, String release, String version) {
        public KernelIdentity {
            for (String value : new String[]{sysname, machine, release, version}) {
                if (value == null || value.isEmpty() || value.length() > 4096 || value.indexOf(0) >= 0
                        || !StandardCharsets.UTF_8.newEncoder().canEncode(value)
                        || value.getBytes(StandardCharsets.UTF_8).length > 4096) {
                    throw new IllegalArgumentException("Invalid bounded UTF8 uname field");
                }
            }
        }
    }

    /** Cached JDK data only, not backend approval. The copied helper bytes retain their one final NUL byte. */
    record JdkProcessLaunchObservation(String mechanism, int ordinal, byte[] helperPath) {
        public JdkProcessLaunchObservation {
            String expected = switch (ordinal) {
                case 0 -> "FORK";
                case 1 -> "POSIX_SPAWN";
                case 2 -> "VFORK";
                default -> throw new IllegalArgumentException("Unknown cached JDK process launch ordinal");
            };
            if (!expected.equals(mechanism) || helperPath == null || helperPath.length < 2
                    || helperPath.length > 4097) {
                throw new IllegalArgumentException("Invalid bounded JDK process launch observation");
            }
            helperPath = helperPath.clone();
            if (helperPath[helperPath.length - 1] != 0) {
                throw new IllegalArgumentException("JDK helper path lacks a final NUL");
            }
            for (int i = 0; i < helperPath.length - 1; i++) {
                if (helperPath[i] == 0) {
                    throw new IllegalArgumentException("JDK helper path contains an interior NUL");
                }
            }
        }

        @Override
        public byte[] helperPath() {
            return helperPath.clone();
        }
    }

    /** Connection-time Linux SO_PEERCRED data in the caller's namespaces, never service/operation qualification. */
    record UnixPeerCredentials(long pid, long uid, long gid) {
        public UnixPeerCredentials {
            if (pid < 1 || pid > Integer.MAX_VALUE || uid < 0 || uid >= 0xffff_ffffL
                    || gid < 0 || gid >= 0xffff_ffffL) {
                throw new IllegalArgumentException("Invalid Linux Unix peer credentials");
            }
        }
    }

    /** Raw ordinary-link facts only; callers separately protect and bind the original physical parent chain. */
    record SymbolicLinkObservation(long device, long inode, long uid, long gid, int mode, long links, long size,
                                  long modifiedSeconds, long modifiedNanos, long changedSeconds, long changedNanos,
                                  byte[] target) {
        public SymbolicLinkObservation {
            if (uid < 0 || uid >= 0xffff_ffffL || gid < 0 || gid >= 0xffff_ffffL
                    || (mode & 0170000) != 0120000 || links < 1 || target == null || target.length < 1
                    || target.length > 4096 || size != target.length || modifiedNanos < 0
                    || modifiedNanos >= 1_000_000_000L || changedNanos < 0 || changedNanos >= 1_000_000_000L) {
                throw new IllegalArgumentException("Invalid bounded ordinary symbolic-link observation");
            }
            target = target.clone();
        }

        @Override
        public byte[] target() {
            return target.clone();
        }
    }

    /** Read-only observations of this process; never proof of saved credentials or startup permission. */
    record ProcessCredentials(long realUid, long effectiveUid, long realGid, long effectiveGid,
                              List<Long> supplementaryGroups) {
        public ProcessCredentials {
            if (supplementaryGroups.size() > 64) {
                throw new IllegalArgumentException("Current process has too many supplementary groups");
            }
            for (long value : new long[]{realUid, effectiveUid, realGid, effectiveGid}) {
                if (value < 0 || value > 0xffff_ffffL) {
                    throw new IllegalArgumentException("Invalid current process credential");
                }
            }
            var groups = new TreeSet<Long>();
            for (long group : supplementaryGroups) {
                if (group < 0 || group > 0xffff_ffffL) {
                    throw new IllegalArgumentException("Invalid current process supplementary group");
                }
                groups.add(group);
            }
            supplementaryGroups = List.copyOf(groups);
        }
    }

    // These constants are different per each OS, so the correct values are defined in JNI code
    int O_CREAT = 0x01;
    int O_RDONLY = 0x02;
    int O_WRONLY = 0x04;
    int O_TRUNC = 0x08;
    int O_DIRECT = 0x10;
    int O_DSYNC = 0x20;

    int SEEK_SET = 0;
    int SEEK_END = 2;

    int FALLOC_FL_ZERO_RANGE = 0x10;

    int open(String pathname, int flags, int mode) throws NativeIOException;

    int fsync(int fd) throws NativeIOException;

    /**
     * fallocate is a linux-only syscall, so callers must handle the possibility that it does
     * not exist.
     */
    int fallocate(int fd, int mode, long offset, long len) throws NativeIOException;

    /**
     * posix_fadvise is a linux-only syscall, so callers must handle the possibility that it does
     * not exist.
     */
    int posix_fadvise(int fd, long offset, long len, int flag) throws NativeIOException;

    int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException;

    long posix_memalign(int alignment, int size) throws NativeIOException;

    void free(long pointer) throws NativeIOException;

    long lseek(int fd, long offset, int whence) throws NativeIOException;

    long pread(int fd, long pointer, long size, long offset) throws NativeIOException;

    int close(int fd) throws NativeIOException;

    /**
     * Atomically rename without replacing any existing target, including a symbolic link.
     * This operation neither synchronizes the directory nor retries or copies across file systems.
     * Older implementations reject this explicit capability without performing I/O.
     *
     * @return zero only when the operating system reported a successful rename
     */
    default int renameNoReplace(String source, String target) throws NativeIOException {
        throw new NativeIOException("Atomic rename without replacement is unsupported");
    }

    /**
     * Query additional ACL presence on an existing regular file or directory on a supported local filesystem.
     * This does not establish path ownership, parent safety or access authority. Symbolic links, unknown
     * filesystems and unavailable ACL queries are errors, never an absent ACL. Does not change permissions.
     */
    default boolean hasExtendedAcl(String path) throws NativeIOException {
        throw new NativeIOException("Native ACL presence query is unsupported");
    }

    /**
     * Fixed lstat/readlink/lstat observation on a supported local filesystem; target bytes are never decoded,
     * followed or normalized. At most 4096 bytes, no descriptors or permission changes. Unavailable rejects.
     * This is not atomic and cannot replace caller protection of the original parent chain.
     */
    default SymbolicLinkObservation observeSymbolicLink(String path) throws NativeIOException {
        throw new NativeIOException("Native symbolic-link observation is unsupported");
    }

    /** Direct getuid/geteuid/getgid/getegid/getgroups observations; no external PID or credential input. */
    default ProcessCredentials currentProcessCredentials() throws NativeIOException {
        throw new NativeIOException("Current process credential query is unsupported");
    }

    /** One libc uname call, no property, procfs, command or deployment-qualification fallback. */
    default KernelIdentity currentKernelIdentity() throws NativeIOException {
        throw new NativeIOException("Current kernel identity query is unsupported");
    }

    /** One statfs of the current Linux mount view's literal /sys/fs/cgroup/; no episode or membership proof. */
    default long currentCgroupRootFileSystemType() throws NativeIOException {
        throw new NativeIOException("Current cgroup root filesystem type query is unsupported");
    }

    /** Cold read through a held verified procfs root and fixed openat2 path; no host namespace qualification. */
    default UUID currentLinuxBootId() throws NativeIOException {
        throw new NativeIOException("Current Linux boot ID query is unsupported");
    }

    /**
     * Read only the fixed bootstrap ProcessImpl's cached mechanism and helper bytes. May initialize that class and
     * wait for class initialization; callers must not hold a trust monitor or infer termination from a timeout.
     * Never sets a property/field, starts a process, selects a backend or approves its execution dependencies.
     */
    default JdkProcessLaunchObservation jdkProcessLaunch() throws NativeIOException {
        throw new NativeIOException("Cached JDK process launch observation is unsupported");
    }

    /**
     * Read the connected Linux JDK Unix channel's numeric peer PID/UID/GID. The caller owns its open lifetime and
     * excludes concurrent close. The approved JDK's stateLock also excludes close/pre-close during the query.
     * No socket is opened, configured, consumed, duplicated or closed here; other JDKs are not implicitly supported.
     */
    default UnixPeerCredentials unixPeerCredentials(SocketChannel channel) throws NativeIOException {
        throw new NativeIOException("Linux Unix peer credential query is unsupported");
    }

    /**
     * Read dev/ino from the original fixed bootstrap JDK FileChannelImpl descriptor under its monitor.
     * No path lookup, duplicate, position change or close. Unknown providers/layouts and closed channels reject.
     * May block; return is not proof the channel remains open afterwards or that its owner has drained.
     */
    default FileIdentity fileIdentity(java.nio.channels.FileChannel channel) throws NativeIOException {
        throw new NativeIOException("Original file-channel identity query is unsupported");
    }
}

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

/**
 * NativeIO API.
 */
public interface NativeIO {

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
}

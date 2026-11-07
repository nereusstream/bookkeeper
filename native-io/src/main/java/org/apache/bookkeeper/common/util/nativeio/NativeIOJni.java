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

class NativeIOJni {

    static native int open(String pathname, int flags, int mode) throws NativeIOException;

    static native int fsync(int fd) throws NativeIOException;

    /**
     * fallocate is a linux-only syscall, so callers must handle the possibility that it does
     * not exist.
     */
    static native int fallocate(int fd, int mode, long offset, long len) throws NativeIOException;

    static native int posix_fadvise(int fd, long offset, long len, int flag) throws NativeIOException;

    static native int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException;

    static native long posix_memalign(int alignment, int size) throws NativeIOException;

    static native void free(long pointer) throws NativeIOException;

    static native long lseek(int fd, long offset, int whence) throws NativeIOException;

    static native long pread(int fd, long pointer, long size, long offset) throws NativeIOException;

    static native int close(int fd) throws NativeIOException;

    static native int renameNoReplace(String source, String target) throws NativeIOException;

    static native NativeIO.SymbolicLinkObservation observeSymbolicLink(String path) throws NativeIOException;

    static native int hasExtendedAcl(String path) throws NativeIOException;

    static native long[] currentProcessCredentials() throws NativeIOException;

    static native long[] currentLinuxThreadCredentials() throws NativeIOException;

    static native long[] currentLinuxThreadFilesystemIds() throws NativeIOException;

    static native String[] currentKernelIdentity() throws NativeIOException;

    static native long currentCgroupRootFileSystemType() throws NativeIOException;

    static native String currentLinuxBootId() throws NativeIOException;

    static native long[] currentLinuxProcessBirth() throws NativeIOException;

    static native long[] currentLinuxThreadNamespaces() throws NativeIOException;

    static native int currentLinuxNativeEnvironment() throws NativeIOException;

    static native NativeIO.JdkProcessLaunchObservation jdkProcessLaunch() throws NativeIOException;

    static native long[] unixPeerCredentials(java.nio.channels.SocketChannel channel) throws NativeIOException;

    static native long[] fileIdentity(java.nio.channels.FileChannel channel) throws NativeIOException;

    static native long[] directoryFileSystem(java.nio.channels.FileChannel channel) throws NativeIOException;

    static native long[] linuxCgroupMount(java.nio.channels.FileChannel channel) throws NativeIOException;

    static native Object[] linuxCgroupMembership(java.nio.channels.FileChannel root) throws NativeIOException;

    static native Object[] linuxExt4Mount(java.nio.channels.FileChannel root, byte[] canonicalRoot)
            throws NativeIOException;

    static {
        NativeIOBootstrap.initializeJni();
    }
}

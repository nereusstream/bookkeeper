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
}

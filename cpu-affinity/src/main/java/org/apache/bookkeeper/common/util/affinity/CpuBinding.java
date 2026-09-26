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

package org.apache.bookkeeper.common.util.affinity;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.Objects;
import org.apache.bookkeeper.common.util.affinity.impl.CpuAffinityImpl;

/**
 * Explicit current-platform-thread binding. Never acquires cores, changes CPU online state, or
 * claims host exclusivity. Create application threads from an unbound startup thread before use.
 */
public final class CpuBinding implements AutoCloseable {
    // Bounds allocation and JNI CPU indices; sparse masks are represented by their highest index.
    public static final int MAX_CPUS = 65536;
    private static final Method IS_VIRTUAL = virtualThreadMethod();

    interface Access {
        BitSet get() throws IOException;

        void set(BitSet mask) throws IOException;
    }

    private static final Access NATIVE = new Access() {
        @Override
        public BitSet get() throws IOException {
            return BitSet.valueOf(CpuAffinityImpl.currentThreadMask());
        }

        @Override
        public void set(BitSet mask) throws IOException {
            CpuAffinityImpl.setCurrentThreadMask(mask.stream().toArray());
        }
    };

    private final Thread owner;
    private final Access access;
    private final BitSet original;
    private final BitSet requested;
    private final BitSet actual;
    private BitSet restored;
    private boolean closed;

    private CpuBinding(Access access, BitSet original, BitSet requested, BitSet actual) {
        owner = Thread.currentThread();
        this.access = access;
        this.original = original;
        this.requested = requested;
        this.actual = actual;
    }

    public static BitSet currentThreadMask() throws IOException {
        requirePlatformThread();
        return copyValid(NATIVE.get());
    }

    public static CpuBinding bindCurrentThread(BitSet requested) throws IOException {
        return bindCurrentThread(requested, NATIVE);
    }

    static CpuBinding bindCurrentThread(BitSet requested, Access access) throws IOException {
        requirePlatformThread();
        BitSet target = copyValid(requested);
        Objects.requireNonNull(access, "access");
        BitSet original = copyValid(access.get());
        BitSet outside = (BitSet) target.clone();
        outside.andNot(original);
        if (!outside.isEmpty()) {
            throw new IllegalArgumentException("Requested CPUs are outside the thread's initial mask");
        }
        try {
            access.set((BitSet) target.clone());
            BitSet actual = copyValid(access.get());
            if (!actual.equals(target)) {
                throw new IOException("Kernel did not apply the complete requested CPU mask: " + actual);
            }
            return new CpuBinding(access, original, target, actual);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                restore(access, original);
            } catch (IOException | RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    public BitSet requestedMask() {
        return (BitSet) requested.clone();
    }

    public BitSet actualMask() {
        return (BitSet) actual.clone();
    }

    /** Read again on the bound thread; topology/cpuset monitors must not assume the original mask persists. */
    public BitSet verifyCurrentMask() throws IOException {
        requireOwner();
        if (closed) {
            throw new IllegalStateException("Binding already closed");
        }
        BitSet observed = copyValid(access.get());
        if (!observed.equals(requested)) {
            throw new IOException("Current CPU mask no longer matches binding: " + observed);
        }
        return observed;
    }

    public BitSet restoredMask() {
        requireOwner();
        if (!closed) {
            throw new IllegalStateException("Restoration has not succeeded");
        }
        return (BitSet) restored.clone();
    }

    @Override
    public void close() throws IOException {
        requireOwner();
        if (!closed) {
            restored = restore(access, original);
            closed = true;
        }
    }

    private static BitSet restore(Access access, BitSet original) throws IOException {
        access.set((BitSet) original.clone());
        BitSet observed = copyValid(access.get());
        if (!observed.equals(original)) {
            // cpuset/online changes can prevent full restoration. Caller must not claim fallback succeeded.
            throw new IOException("CPU mask restoration incomplete: requested=" + original + ", actual=" + observed);
        }
        return observed;
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Binding scope must be used and closed on its original thread");
        }
    }

    private static Method virtualThreadMethod() {
        try {
            return Thread.class.getMethod("isVirtual");
        } catch (NoSuchMethodException java17) {
            return null;
        }
    }

    private static void requirePlatformThread() throws IOException {
        if (IS_VIRTUAL != null) {
            try {
                if ((boolean) IS_VIRTUAL.invoke(Thread.currentThread())) {
                    throw new IOException("CPU affinity requires a platform thread");
                }
            } catch (IllegalAccessException | InvocationTargetException failure) {
                throw new IOException("Unable to verify platform thread", failure);
            }
        }
    }

    private static BitSet copyValid(BitSet mask) {
        Objects.requireNonNull(mask, "mask");
        if (mask.isEmpty() || mask.length() > MAX_CPUS) {
            throw new IllegalArgumentException("CPU mask must be nonempty and below " + MAX_CPUS);
        }
        return (BitSet) mask.clone();
    }
}

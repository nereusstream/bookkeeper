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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assume;
import org.junit.Test;

public class CpuBindingTest {
    private static BitSet mask(int... ids) {
        BitSet mask = new BitSet();
        for (int id : ids) {
            mask.set(id);
        }
        return mask;
    }

    // Models the kernel boundary only; this is not evidence of native Linux affinity or NUMA placement.
    private static class Kernel implements CpuBinding.Access {
        BitSet current = mask(2, 63, 2049);
        int calls;
        boolean restrictFirst;
        boolean failRestore;

        @Override
        public BitSet get() {
            return (BitSet) current.clone();
        }

        @Override
        public void set(BitSet requested) throws IOException {
            calls++;
            if (calls > 1 && failRestore) {
                throw new IOException("restore failed");
            }
            current = (BitSet) requested.clone();
            if (calls == 1 && restrictFirst) {
                current.clear(2049);
            }
        }
    }

    @Test
    public void sparseMaskIsVerifiedAndRestoredWithoutEscapingMutableState() throws Exception {
        Kernel kernel = new Kernel();
        BitSet target = mask(63, 2049);
        CpuBinding binding = CpuBinding.bindCurrentThread(target, kernel);
        target.clear();
        assertEquals(mask(63, 2049), binding.requestedMask());
        binding.actualMask().clear();
        assertEquals(mask(63, 2049), binding.verifyCurrentMask());
        binding.close();
        binding.close();
        assertEquals(2, kernel.calls);
        assertEquals(mask(2, 63, 2049), binding.restoredMask());
        assertThrows(IllegalStateException.class, binding::verifyCurrentMask);
    }

    @Test
    public void invalidConfigurationDoesNotAttemptNativeMutation() {
        Kernel kernel = new Kernel();
        for (BitSet invalid : new BitSet[] {mask(), mask(65536), mask(1)}) {
            assertThrows(IllegalArgumentException.class, () -> CpuBinding.bindCurrentThread(invalid, kernel));
        }
        assertEquals(0, kernel.calls);
        assertEquals(mask(2, 63, 2049), kernel.current);
    }

    @Test
    public void kernelRestrictionFailsAndRollsBackBeforePublishingBinding() {
        Kernel kernel = new Kernel();
        kernel.restrictFirst = true;
        IOException failure = assertThrows(IOException.class,
                () -> CpuBinding.bindCurrentThread(mask(63, 2049), kernel));
        assertTrue(failure.getMessage().contains("complete requested"));
        assertEquals(mask(2, 63, 2049), kernel.current);
        assertEquals(2, kernel.calls);
    }

    @Test
    public void incompleteRollbackIsNotReportedAsSuccessfulFallback() {
        Kernel kernel = new Kernel();
        kernel.restrictFirst = true;
        kernel.failRestore = true;
        IOException failure = assertThrows(IOException.class,
                () -> CpuBinding.bindCurrentThread(mask(63, 2049), kernel));
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("restore failed", failure.getSuppressed()[0].getMessage());
        assertEquals(mask(63), kernel.current);
    }

    @Test
    public void runtimeMaskLossAndWrongThreadCloseAreVisible() throws Exception {
        Kernel kernel = new Kernel();
        CpuBinding binding = CpuBinding.bindCurrentThread(mask(63, 2049), kernel);
        try {
            AtomicReference<Throwable> crossThread = new AtomicReference<>();
            Thread other = new Thread(() -> {
                try {
                    binding.close();
                } catch (Throwable failure) {
                    crossThread.set(failure);
                }
            });
            other.start();
            other.join(5000);
            assertFalse(other.isAlive());
            assertTrue(crossThread.get() instanceof IllegalStateException);
            assertEquals(1, kernel.calls);
            kernel.current.clear(2049);
            assertThrows(IOException.class, binding::verifyCurrentMask);
        } finally {
            binding.close();
        }
        assertEquals(mask(2, 63, 2049), kernel.current);
    }

    @Test
    public void failedCloseCanBeRetriedButCannotClaimRestoration() throws Exception {
        Kernel kernel = new Kernel();
        CpuBinding binding = CpuBinding.bindCurrentThread(mask(63), kernel);
        kernel.failRestore = true;
        assertThrows(IOException.class, binding::close);
        assertThrows(IllegalStateException.class, binding::restoredMask);
        kernel.failRestore = false;
        binding.close();
        assertEquals(mask(2, 63, 2049), binding.restoredMask());
    }

    @Test
    public void virtualThreadCannotBindItsTemporaryCarrier() throws Exception {
        java.lang.reflect.Method start;
        try {
            start = Thread.class.getMethod("startVirtualThread", Runnable.class);
        } catch (NoSuchMethodException java17) {
            Assume.assumeNoException(java17);
            return;
        }
        Kernel kernel = new Kernel();
        AtomicReference<Throwable> result = new AtomicReference<>();
        Thread virtual = (Thread) start.invoke(null, (Runnable) () -> {
            try {
                CpuBinding.bindCurrentThread(mask(63), kernel);
            } catch (Throwable failure) {
                result.set(failure);
            }
        });
        virtual.join(5000);
        assertFalse(virtual.isAlive());
        assertTrue(result.get() instanceof IOException);
        assertTrue(result.get().getMessage().contains("platform thread"));
        assertEquals(0, kernel.calls);
    }

    @Test
    public void unavailableNativeFailsExplicitlyOnNonLinux() {
        Assume.assumeFalse(System.getProperty("os.name").equals("Linux"));
        assertThrows(IOException.class, CpuBinding::currentThreadMask);
    }

    @Test
    public void linuxNativeBindingAndChildInheritanceRestoreOnSameThread() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        BitSet initial = CpuBinding.currentThreadMask();
        for (int[] invalid : new int[][] {new int[0], {-1}, {65536}, {0, 0}}) {
            assertThrows(IOException.class, () ->
                    org.apache.bookkeeper.common.util.affinity.impl.CpuAffinityImpl.setCurrentThreadMask(invalid));
            assertEquals(initial, CpuBinding.currentThreadMask());
        }
        BitSet selected = mask(initial.nextSetBit(0));
        try (CpuBinding binding = CpuBinding.bindCurrentThread(selected)) {
            assertEquals(selected, binding.verifyCurrentMask());
            AtomicReference<BitSet> childMask = new AtomicReference<>();
            AtomicReference<Throwable> childFailure = new AtomicReference<>();
            Thread child = new Thread(() -> {
                try {
                    childMask.set(CpuBinding.currentThreadMask());
                } catch (Throwable failure) {
                    childFailure.set(failure);
                }
            });
            child.start();
            child.join(5000);
            assertFalse(child.isAlive());
            assertEquals(null, childFailure.get());
            assertEquals(selected, childMask.get());
        }
        assertEquals(initial, CpuBinding.currentThreadMask());
    }
}

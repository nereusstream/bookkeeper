/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;

/**
 * One startup reservation for the two CONTROL owners and their bounded Java state/workspaces.
 * This is deliberately a HotSpot 17/21 heap-layout envelope, not a portable object-size API or a
 * budget for opaque JCA, native/JDK resources, borrowed callbacks, or the remaining storage graph.
 */
final class SegmentControlCapacity {
    static final int MAX_RECORD_BYTES = 65536;
    static final int FIRST_RECORD = 4096;

    private SegmentControlCapacity() { }

    static long required(IsolatedSegmentConfig config, long logBytes) throws IOException {
        return required(Layout.hotSpot(), logBytes, config.maxPendingEntries(), config.dataFiles(),
                config.logicalShards());
    }

    static long required(Layout layout, long logBytes, int pending, int files, int shards) throws IOException {
        if (logBytes < FIRST_RECORD + 320 || pending < 1 || files < 1 || files > 64
                || shards < 1 || shards > 64) {
            throw new IOException("Invalid storage CONTROL capacity geometry");
        }
        try {
            long codec = sum(SegmentControlCheckpoint.WORKING_SET_BYTES,
                    // The existing codec amount counts backing contents. Include their headers and views too.
                    times(8, layout.bytes(0)), times(16, layout.buffer()));
            return sum(codec, SegmentBookieControlLog.controlCapacity(layout, logBytes, pending),
                    SegmentArenaControlLog.controlCapacity(layout, logBytes, pending, files),
                    SegmentArenaControlDriver.controlCapacity(layout, pending, files, shards),
                    layout.object(SegmentBookieControlDriver.class),
                    layout.object(java.util.concurrent.atomic.AtomicBoolean.class),
                    layout.object(SegmentOwnerLoop.Ticket.class),
                    layout.object(java.util.concurrent.atomic.AtomicReference.class),
                    // One Bookie driver request: append/owner/I/O/completion closures, with at most eight captures.
                    times(8, layout.captured(8)));
        } catch (ArithmeticException | IllegalArgumentException unsupported) {
            throw new IOException("Storage CONTROL capacity cannot be represented by the supported heap model",
                    unsupported);
        }
    }

    static long imageBytes(long logBytes) {
        return logBytes > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : times(2, logBytes);
    }

    static long wireBytes(long logBytes) {
        // A selected image, the entire suffix, and the two first-image fallback root commits.
        return sum(imageBytes(logBytes), logBytes - FIRST_RECORD, 2 * 208);
    }

    static long operations(long logBytes, int imageRecordBytes, int journalRecordBytes) {
        return sum(imageBytes(logBytes) / imageRecordBytes,
                (logBytes - FIRST_RECORD) / journalRecordBytes, 2);
    }

    static long slope(long bytes, long wireBytes) {
        return sum(bytes / wireBytes, bytes % wireBytes == 0 ? 0 : 1);
    }

    static long sum(long... values) {
        long result = 0;
        for (long value : values) {
            if (value < 0) {
                throw new IllegalArgumentException("Negative CONTROL capacity component");
            }
            result = Math.addExact(result, value);
        }
        return result;
    }

    static long times(long count, long bytes) {
        if (count < 0 || bytes < 0) {
            throw new IllegalArgumentException("Negative CONTROL capacity component");
        }
        return Math.multiplyExact(count, bytes);
    }

    /** Conservative wide-reference layout; reflection counts fields, never reads object contents. */
    static final class Layout {
        private final int alignment;

        Layout(int alignment) {
            if (alignment < 8 || alignment > 256 || Integer.bitCount(alignment) != 1) {
                throw new IllegalArgumentException("Unsupported HotSpot object alignment");
            }
            this.alignment = alignment;
        }

        static Layout hotSpot() throws IOException {
            String name = System.getProperty("java.vm.name", "");
            int version = Runtime.version().feature();
            if ((!name.equals("OpenJDK 64-Bit Server VM") && !name.equals("Java HotSpot(TM) 64-Bit Server VM"))
                    || (version != 17 && version != 21)
                    || !System.getProperty("sun.arch.data.model", "").equals("64")) {
                throw new IOException("Experimental storage CONTROL accounting requires 64-bit HotSpot JDK 17 or 21");
            }
            try {
                HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
                if (bean == null) {
                    throw new IllegalArgumentException("HotSpot diagnostic MXBean is unavailable");
                }
                // Both compressed and wide configurations fit the header/reference bounds below.
                for (String option : List.of("UseCompressedOops", "UseCompressedClassPointers")) {
                    String value = bean.getVMOption(option).getValue();
                    if (!value.equals("true") && !value.equals("false")) {
                        throw new IllegalArgumentException("Unsupported HotSpot pointer layout");
                    }
                }
                return new Layout(Integer.parseInt(bean.getVMOption("ObjectAlignmentInBytes").getValue()));
            } catch (RuntimeException unsupported) {
                throw new IOException("Cannot establish the experimental storage CONTROL heap layout", unsupported);
            }
        }

        long object(Class<?> type) {
            long bytes = 16;
            for (Class<?> current = type; current != null && current != Object.class;
                    current = current.getSuperclass()) {
                rejectContended(current.getDeclaredAnnotations());
                for (var field : current.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers())) {
                        rejectContended(field.getDeclaredAnnotations());
                        bytes = sum(bytes, 8);
                    }
                }
                // Include a packing boundary for each superclass, even when its fields are empty.
                bytes = sum(bytes, alignment - 1);
            }
            return aligned(bytes);
        }

        private static void rejectContended(java.lang.annotation.Annotation[] annotations) {
            for (var annotation : annotations) {
                if (annotation.annotationType().getName().equals("jdk.internal.vm.annotation.Contended")) {
                    throw new IllegalArgumentException("Contended fields are outside the CONTROL layout model");
                }
            }
        }

        long object(String name) {
            try {
                return object(Class.forName(name, false, SegmentControlCapacity.class.getClassLoader()));
            } catch (ClassNotFoundException unavailable) {
                throw new IllegalArgumentException("Unsupported CONTROL container implementation: " + name,
                        unavailable);
            }
        }

        long bytes(long length) {
            return aligned(sum(24, length));
        }

        long refs(long length) {
            return aligned(sum(24, times(8, length)));
        }

        long longs(long length) {
            return refs(length);
        }

        long string(int maxUtf8Bytes) {
            // UTF-16 storage has at most two bytes per UTF-8 input byte, including replaced surrogates.
            return sum(object(String.class), bytes(times(2, maxUtf8Bytes)));
        }

        long context() {
            return sum(object(ProfileLedgerContext.class), bytes(16), bytes(36));
        }

        long buffer() {
            return Math.max(object(ByteBuffer.allocate(0).getClass()),
                    object(ByteBuffer.allocate(0).asReadOnlyBuffer().getClass()));
        }

        long captured(int fields) {
            return aligned(sum(16, times(fields, 8), alignment - 1));
        }

        long hashMap() {
            // With m high-water entries in g maps, old+new tables fit 32g+8m wide slots.
            // This includes collision-driven growth to 64 before treeification, not just load-factor growth.
            return sum(object(HashMap.class), times(2, refs(16)));
        }

        long hashEntry() {
            // Tree nodes are also an upper bound for ordinary nodes; cover old/new conversion overlap.
            return sum(times(2, object("java.util.HashMap$TreeNode")), 8 * 8);
        }

        long hashSet() {
            return sum(object(HashSet.class), hashMap());
        }

        long identityMap(long entries) {
            // Default table has 64 reference slots; growth before 3*size exceeds table length doubles it.
            // Each high-water table fits 64+6m slots. Reserve both old/new arrays during resize, without nodes.
            return sum(object(IdentityHashMap.class), times(2, refs(sum(64, times(6, entries)))));
        }

        long immutableList(long entries) {
            return sum(Math.max(object("java.util.ImmutableCollections$ListN"),
                    object("java.util.ImmutableCollections$List12")), refs(entries));
        }

        long immutableMap(long entries) {
            // Map.copyOf uses either inline Map1 fields or a MapN table with four reference slots per entry.
            return sum(Math.max(object("java.util.ImmutableCollections$MapN"),
                    object("java.util.ImmutableCollections$Map1")), refs(times(4, entries)));
        }

        long treeEntry() {
            return object("java.util.TreeMap$Entry");
        }

        long list() {
            return sum(object(ArrayList.class), object(List.of().getClass()), times(4, refs(0)));
        }

        long snapshot(long count, long wrapperBytes) {
            // Capture/growth, filter/sort, TimSort, toList, and List.copyOf can coexist before the cap check.
            return sum(times(2, object(SegmentControlCheckpoint.Snapshot.class)), times(4, list()),
                    times(8, refs(count)), times(count, wrapperBytes));
        }

        long scanner(long count) {
            return sum(hashSet(), times(count, sum(object(UUID.class), hashEntry())));
        }

        long reference() {
            return sum(object(SegmentControlCheckpoint.Reference.class), string(64));
        }

        private long aligned(long bytes) {
            return times(sum(bytes, alignment - 1) / alignment, alignment);
        }
    }
}

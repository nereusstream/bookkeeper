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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Explicit limits for disposable local-file experiments. Not a Bookie configuration or ACK authority.
 * Persistent shard identity is independent of the number of execution workers.
 */
public record IsolatedSegmentConfig(Path directory, int arenaCount, int logicalShards,
                                    int dataFiles, long bytesPerFile, int alignment,
                                    int maxPayloadBytes, int maxPendingEntries,
                                    String backend, String affinity, String numaPolicy, String waitStrategy) {
    public IsolatedSegmentConfig {
        directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        if (arenaCount != 1 || logicalShards < 1 || logicalShards > 64
                || dataFiles < 1 || dataFiles > 64 || maxPendingEntries < 1) {
            throw new IllegalArgumentException(
                    "One Arena, 1..64 fixed shards/files and positive pending limit required");
        }
        if (alignment != 4096 || bytesPerFile < alignment || bytesPerFile % alignment != 0
                || maxPayloadBytes < 1 || maxPayloadBytes > 1024 * 1024
                || bytesPerFile < (long) maxPayloadBytes + 2 * alignment) {
            throw new IllegalArgumentException("Invalid alignment, fixed capacity or entry limit");
        }
        Math.multiplyExact(bytesPerFile, dataFiles);
        requireSupported(backend, "FILE_CHANNEL", "I/O backend");
        if (!java.util.Set.of("OFF", "PREFER", "REQUIRE").contains(affinity)) {
            throw new IllegalArgumentException("Unsupported affinity: " + affinity);
        }
        if (!java.util.Set.of("SYSTEM", "OWNER_TOUCH").contains(numaPolicy)) {
            throw new IllegalArgumentException("Unsupported NUMA policy: " + numaPolicy);
        }
        requireSupported(waitStrategy, "PARK", "wait strategy");
    }

    public static IsolatedSegmentConfig defaults(Path directory) {
        return new IsolatedSegmentConfig(directory, 1, 4, 2, 16L * 1024 * 1024,
                4096, 1024 * 1024, 256, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private static void requireSupported(String actual, String supported, String feature) {
        if (!supported.equals(actual)) {
            throw new IllegalArgumentException(feature + " is not implemented: " + actual);
        }
    }
}

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.profile.startup;

import java.util.EnumMap;
import java.util.Map;

/** Per-run cold-path attribution plus an explicit zero normal-Add surface. */
public final class StartupReadinessMetrics {

    public enum ColdOperation {
        COMPATIBILITY_FACT_READ,
        DEVICE_MANIFEST_READ,
        DEVICE_SUPERBLOCK_READ,
        ALLOCATOR_RECOVERY,
        ROUTE_RECOVERY,
        DELETE_RECOVERY,
        LOCAL_READINESS_DURABILITY,
        PERSISTENT_READINESS_READ,
        PERSISTENT_READINESS_CAS,
        SERVICE_INFO_READ,
        SERVICE_INFO_PUBLISH,
        EPHEMERAL_REGISTRATION_READ,
        EPHEMERAL_REGISTRATION_WRITE,
        EPHEMERAL_DEMOTION
    }

    private final EnumMap<ColdOperation, Long> coldCounts = new EnumMap<>(ColdOperation.class);

    void increment(ColdOperation operation) {
        add(operation, 1);
    }

    void add(ColdOperation operation, long delta) {
        coldCounts.merge(operation, delta, Long::sum);
    }

    public Snapshot snapshot() {
        return new Snapshot(Map.copyOf(coldCounts), NormalAddCounters.ZERO);
    }

    public record Snapshot(Map<ColdOperation, Long> coldCounts, NormalAddCounters normalAdd) {
        public Snapshot {
            coldCounts = Map.copyOf(coldCounts);
            normalAdd = StartupFactSupport.requireNonNull(normalAdd);
        }

        public long coldCount(ColdOperation operation) {
            return coldCounts.getOrDefault(operation, 0L);
        }
    }

    public record NormalAddCounters(
            long formatOrReadinessReads,
            long remoteIo,
            long hashOperations,
            long tlsOperations,
            long kmsOrCertificateOperations,
            long controlFsyncs) {
        public static final NormalAddCounters ZERO = new NormalAddCounters(0, 0, 0, 0, 0, 0);

        public boolean isZero() {
            return equals(ZERO);
        }
    }
}

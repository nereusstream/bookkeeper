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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

/** Static placement only: never changes logical shard identity, schedules work, or modifies the host. */
public final class ExecutionPlacementPlan {
    private final int shards;
    private final List<BitSet> workers;
    private final BitSet services;
    private final List<Integer> preferredNodes;

    private ExecutionPlacementPlan(int shards, List<BitSet> workers, BitSet services, List<Integer> nodes) {
        this.shards = shards;
        this.workers = workers.stream().map(mask -> (BitSet) mask.clone()).toList();
        this.services = (BitSet) services.clone();
        preferredNodes = List.copyOf(nodes);
    }

    public static ExecutionPlacementPlan off(int shards, int workerCount) {
        validateCounts(shards, workerCount);
        List<BitSet> workers = new ArrayList<>();
        List<Integer> nodes = new ArrayList<>();
        for (int worker = 0; worker < workerCount; worker++) {
            workers.add(new BitSet());
            nodes.add(-1);
        }
        return new ExecutionPlacementPlan(shards, workers, new BitSet(), nodes);
    }

    /** Null candidate masks mean automatic selection. Unknown environment facts do not mean unlimited resources. */
    public static ExecutionPlacementPlan bound(CpuTopologySnapshot topology, int shards, int workerCount,
                                               BitSet appendCandidates, BitSet serviceCandidates,
                                               int serviceCpuReserve) throws IOException {
        validateCounts(shards, workerCount);
        if (serviceCpuReserve < 1 || serviceCpuReserve > CpuBinding.MAX_CPUS) {
            throw new IllegalArgumentException("Positive bounded service reserve required");
        }
        validateMask(appendCandidates);
        validateMask(serviceCandidates);
        if (appendCandidates != null && serviceCandidates != null
                && appendCandidates.intersects(serviceCandidates)) {
            throw new IllegalArgumentException("Append and service CPU configuration overlaps");
        }
        if (topology.cpuQuota().isEmpty()
                || topology.cpuQuota().getAsDouble() < (double) workerCount + serviceCpuReserve) {
            throw new IOException("CPU quota unknown or insufficient for workers plus service reserve");
        }
        BitSet eligible = topology.eligibleCpus();
        BitSet candidates = (BitSet) eligible.clone();
        if (appendCandidates != null) {
            candidates.and(appendCandidates);
        }
        BitSet service = (BitSet) eligible.clone();
        if (serviceCandidates != null) {
            service.and(serviceCandidates);
            for (int cpu = service.nextSetBit(0); cpu >= 0; cpu = service.nextSetBit(cpu + 1)) {
                BitSet siblings = knownCore(topology, cpu).siblings();
                if (appendCandidates != null && siblings.intersects(appendCandidates)) {
                    throw new IllegalArgumentException("Append/service configuration shares an SMT core");
                }
                candidates.andNot(siblings);
            }
        }
        List<Integer> ordered = new ArrayList<>(candidates.stream().boxed().toList());
        ordered.sort(Comparator.<Integer>comparingInt(cpu -> topology.deviceNode() >= 0
                && topology.cpus().get(cpu) != null
                && topology.cpus().get(cpu).node() == topology.deviceNode() ? 0 : 1)
                .thenComparingInt(Integer::intValue));
        List<BitSet> workers = new ArrayList<>();
        List<Integer> nodes = new ArrayList<>();
        BitSet occupied = new BitSet();
        for (int cpu : ordered) {
            CpuTopologySnapshot.Cpu info = knownCore(topology, cpu);
            if (occupied.get(cpu)) {
                continue;
            }
            BitSet single = new BitSet();
            single.set(cpu);
            workers.add(single);
            nodes.add(info.node() >= 0 && topology.memoryNodes().get(info.node()) ? info.node() : -1);
            occupied.or(info.siblings());
            if (workers.size() == workerCount) {
                break;
            }
        }
        if (workers.size() != workerCount) {
            throw new IOException("Insufficient distinct physical cores for append workers");
        }
        service.andNot(occupied);
        int serviceCores = 0;
        BitSet serviceSeen = new BitSet();
        for (int cpu = service.nextSetBit(0); cpu >= 0; cpu = service.nextSetBit(cpu + 1)) {
            CpuTopologySnapshot.Cpu info = knownCore(topology, cpu);
            if (!serviceSeen.get(cpu)) {
                serviceCores++;
                serviceSeen.or(info.siblings());
            }
        }
        if (serviceCores < serviceCpuReserve) {
            throw new IOException("Insufficient distinct service cores after append placement");
        }
        return new ExecutionPlacementPlan(shards, workers, service, nodes);
    }

    public int workerForShard(int shard) {
        if (shard < 0 || shard >= shards) {
            throw new IllegalArgumentException("Invalid logical shard");
        }
        return shard % workers.size();
    }

    public int logicalShards() {
        return shards;
    }

    public int workerCount() {
        return workers.size();
    }

    public BitSet workerMask(int worker) {
        return (BitSet) workers.get(worker).clone();
    }

    public BitSet serviceMask() {
        return (BitSet) services.clone();
    }

    public int preferredNode(int worker) {
        return preferredNodes.get(worker);
    }

    private static CpuTopologySnapshot.Cpu knownCore(CpuTopologySnapshot topology, int cpu) throws IOException {
        CpuTopologySnapshot.Cpu result = topology.cpus().get(cpu);
        if (result == null || !result.coreKnown() || !result.siblings().get(cpu)) {
            throw new IOException("Physical topology unknown for CPU " + cpu);
        }
        return result;
    }

    private static void validateCounts(int shards, int workers) {
        if (shards < 1 || shards > 64 || workers < 1 || workers > shards) {
            throw new IllegalArgumentException("Expected 1..64 shards and 1..shards workers");
        }
    }

    private static void validateMask(BitSet mask) {
        if (mask != null && (mask.isEmpty() || mask.length() > CpuBinding.MAX_CPUS)) {
            throw new IllegalArgumentException("Invalid configured CPU candidates");
        }
    }
}

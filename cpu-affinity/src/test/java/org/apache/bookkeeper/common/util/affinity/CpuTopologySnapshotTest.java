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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CpuTopologySnapshotTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private Path cpuRoot;
    private Path cgroupRoot;
    private Path cgroupLeaf;

    private static BitSet cpus(String value) {
        return CpuTopologySnapshot.parseCpuList(value);
    }

    private static void write(Path path, String text) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, text);
    }

    private void fixture() throws IOException {
        cpuRoot = temporary.getRoot().toPath().resolve("cpu");
        cgroupRoot = temporary.getRoot().toPath().resolve("cgroup");
        cgroupLeaf = cgroupRoot.resolve("parent/leaf");
        write(cpuRoot.resolve("online"), "0,2,4,8,10,12");
        write(cgroupRoot.resolve("cpu.max"), "max 100000");
        write(cgroupRoot.resolve("parent/cpu.max"), "350000 100000");
        write(cgroupLeaf.resolve("cpu.max"), "900000 100000");
        write(cgroupLeaf.resolve("cpuset.cpus.effective"), "0,2,4,8,10,12");
        write(cgroupLeaf.resolve("cpuset.mems.effective"), "0-1");
        for (int cpu : new int[] {0, 2, 4, 8, 10, 12}) {
            int core = cpu % 8;
            int socket = core == 0 ? 0 : 1;
            Path directory = cpuRoot.resolve("cpu" + cpu);
            write(directory.resolve("topology/physical_package_id"), Integer.toString(socket));
            write(directory.resolve("topology/core_id"), core == 4 ? "1" : "0");
            write(directory.resolve("topology/thread_siblings_list"), core + "," + (core + 8));
            Files.createDirectories(directory.resolve("node" + socket));
        }
    }

    private CpuTopologySnapshot read(Path deviceNode) throws IOException {
        return CpuTopologySnapshot.read(cpus("0,2,4,8,10,12"), cpuRoot,
                new CpuTopologySnapshot.CgroupPaths(cgroupRoot, cgroupLeaf), deviceNode, List.of());
    }

    @Test
    public void sparseListsAndQuotaRejectMalformedOrAmbiguousInput() throws Exception {
        assertEquals(6, cpus("0-2,63,2049,65535").cardinality());
        for (String invalid : new String[] {"", "-1", "2-1", "0,0", "0-2,2", "65536", "1,", "1 2"}) {
            assertThrows(IllegalArgumentException.class, () -> cpus(invalid));
        }
        assertEquals(1.5, CpuTopologySnapshot.parseQuota("150000 100000"), 0);
        assertEquals(Double.POSITIVE_INFINITY, CpuTopologySnapshot.parseQuota("max 100000"), 0);
        for (String invalid : new String[] {"0 1000", "max 0", "-1 1000", "NaN 1000", "1000", "max 1 2"}) {
            assertThrows(IOException.class, () -> CpuTopologySnapshot.parseQuota(invalid));
        }
    }

    @Test
    public void visibleAncestorQuotaAndSocketSmtIdentityConstrainPlacement() throws Exception {
        fixture();
        CpuTopologySnapshot topology = read(null);
        assertEquals(3.5, topology.cpuQuota().getAsDouble(), 0);
        ExecutionPlacementPlan plan = ExecutionPlacementPlan.bound(topology, 8, 2, null, null, 1);
        assertEquals(cpus("0"), plan.workerMask(0));
        assertEquals(cpus("2"), plan.workerMask(1));
        assertEquals(cpus("4,12"), plan.serviceMask());
        assertEquals(0, plan.preferredNode(0));
        assertEquals(1, plan.preferredNode(1));
        assertEquals(8, plan.logicalShards());
        assertEquals(1, plan.workerForShard(7));
        assertEquals(3, ExecutionPlacementPlan.off(8, 4).workerForShard(7));
        assertEquals(8, ExecutionPlacementPlan.off(8, 4).logicalShards());
        assertThrows(IOException.class, () -> ExecutionPlacementPlan.bound(topology, 8, 3, null, null, 1));
        topology.deploymentMask().clear();
        topology.cpus().get(0).siblings().clear();
        plan.workerMask(0).clear();
        plan.serviceMask().clear();
        assertEquals(cpus("0"), plan.workerMask(0));
        assertEquals(cpus("0,8"), topology.cpus().get(0).siblings());
        assertEquals(cpus("0,2,4,8,10,12"), topology.eligibleCpus());
    }

    @Test
    public void explicitServiceCoresAreReservedBeforeAppendAndCannotShareSiblings() throws Exception {
        fixture();
        CpuTopologySnapshot topology = read(null);
        ExecutionPlacementPlan plan = ExecutionPlacementPlan.bound(topology, 4, 2, null, cpus("0,8"), 1);
        assertEquals(cpus("2"), plan.workerMask(0));
        assertEquals(cpus("4"), plan.workerMask(1));
        assertEquals(cpus("0,8"), plan.serviceMask());
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionPlacementPlan.bound(topology, 4, 1, cpus("0"), cpus("0"), 1));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutionPlacementPlan.bound(topology, 4, 1, cpus("0"), cpus("8"), 1));
        assertThrows(IOException.class,
                () -> ExecutionPlacementPlan.bound(topology, 4, 2, cpus("0,8"), null, 1));
    }

    @Test
    public void unavailableQuotaOrConflictingTopologyNeverInventsCapacity() throws Exception {
        fixture();
        Files.delete(cgroupRoot.resolve("parent/cpu.max"));
        CpuTopologySnapshot unknownQuota = read(null);
        assertTrue(unknownQuota.cpuQuota().isEmpty());
        assertThrows(IOException.class, () -> ExecutionPlacementPlan.bound(unknownQuota, 4, 1, null, null, 1));
        write(cgroupRoot.resolve("parent/cpu.max"), "350000 100000");
        write(cpuRoot.resolve("cpu8/topology/thread_siblings_list"), "8");
        CpuTopologySnapshot conflict = read(null);
        assertFalse(conflict.cpus().get(0).coreKnown());
        assertFalse(conflict.cpus().get(8).coreKnown());
        assertThrows(IOException.class, () -> ExecutionPlacementPlan.bound(conflict, 4, 1, null, null, 1));
        assertTrue(conflict.limitations().stream().anyMatch(s -> s.contains("Inconsistent sibling")));
    }

    @Test
    public void eligibilityAndMemoryPreferenceUseTheEffectiveAllowedSets() throws Exception {
        fixture();
        write(cgroupLeaf.resolve("cpuset.cpus.effective"), "2,4,8,10,12");
        write(cgroupLeaf.resolve("cpuset.mems.effective"), "0");
        Path deviceNode = temporary.getRoot().toPath().resolve("device/numa_node");
        write(deviceNode, "1");
        CpuTopologySnapshot topology = read(deviceNode);
        assertEquals(cpus("2,4,8,10,12"), topology.eligibleCpus());
        ExecutionPlacementPlan plan = ExecutionPlacementPlan.bound(topology, 4, 2, null, null, 1);
        assertEquals(cpus("2"), plan.workerMask(0));
        assertEquals(cpus("4"), plan.workerMask(1));
        assertEquals(-1, plan.preferredNode(0));
        assertEquals(cpus("8"), plan.serviceMask());
        assertEquals(1, topology.deviceNode());
    }

    @Test
    public void refreshedLimitsRetainTheOriginalDeploymentMask() throws Exception {
        fixture();
        BitSet deployment = cpus("0,2,4,8,10,12");
        write(cgroupLeaf.resolve("cpuset.cpus.effective"), "2,4,10,12");
        write(cgroupLeaf.resolve("cpuset.mems.effective"), "1");
        write(cgroupLeaf.resolve("cpu.max"), "200000 100000");
        CpuTopologySnapshot refreshed = CpuTopologySnapshot.read(deployment, cpuRoot,
                new CpuTopologySnapshot.CgroupPaths(cgroupRoot, cgroupLeaf), null, List.of());
        assertEquals(deployment, refreshed.deploymentMask());
        assertEquals(cpus("2,4,10,12"), refreshed.eligibleCpus());
        assertEquals(cpus("1"), refreshed.memoryNodes());
        assertEquals(2.0, refreshed.cpuQuota().orElseThrow(), 0);
    }

    @Test
    public void cgroupMountResolutionHonorsNamespaceRootAndEscapes() throws Exception {
        String mount = "31 20 0:26 /tenant /sys/fs/cgroup\\040test rw - cgroup2 cgroup rw\n";
        CpuTopologySnapshot.CgroupPaths paths = CpuTopologySnapshot.cgroupPaths("0::/tenant/service\n", mount);
        assertEquals(Path.of("/sys/fs/cgroup test/service"), paths.leaf());
        assertEquals(Path.of("/sys/fs/cgroup test"),
                CpuTopologySnapshot.cgroupPaths("0::/\n", mount.replace(" /tenant ", " / ")).leaf());
        assertThrows(IOException.class, () -> CpuTopologySnapshot.cgroupPaths("0::/\n", mount));
        assertThrows(IOException.class, () -> CpuTopologySnapshot.cgroupPaths("0::/../escape", mount));
        assertThrows(IOException.class, () -> CpuTopologySnapshot.cgroupPaths("1:cpu:/tenant", mount));
        assertThrows(IllegalArgumentException.class,
                () -> new CpuTopologySnapshot.CgroupPaths(Path.of("/root"), Path.of("/outside")));
    }

    @Test
    public void linuxSnapshotUsesActualNativeMaskAndVisibleCgroupFacts() throws Exception {
        Assume.assumeTrue(System.getProperty("os.name").equals("Linux"));
        BitSet initial = CpuBinding.currentThreadMask();
        CpuTopologySnapshot topology = CpuTopologySnapshot.capture(null);
        assertEquals(initial, topology.deploymentMask());
        assertFalse(topology.onlineCpus().isEmpty());
        BitSet outside = topology.eligibleCpus();
        outside.andNot(initial);
        assertTrue(outside.isEmpty());
        assertEquals(initial, CpuBinding.currentThreadMask());
        BitSet selected = new BitSet();
        selected.set(initial.nextSetBit(0));
        try (CpuBinding ignored = CpuBinding.bindCurrentThread(selected)) {
            CpuTopologySnapshot refreshed = CpuTopologySnapshot.refresh(initial, null);
            assertEquals(initial, refreshed.deploymentMask());
            assertEquals(topology.eligibleCpus(), refreshed.eligibleCpus());
        }
        assertEquals(initial, CpuBinding.currentThreadMask());
        String expected = System.getProperty("uw.expected.cpus");
        if (expected != null) {
            assertEquals(cpus(expected), topology.eligibleCpus());
            assertEquals(Double.parseDouble(System.getProperty("uw.expected.quota")),
                    topology.cpuQuota().orElseThrow(), 0);
            assertEquals(cpus(System.getProperty("uw.expected.memory.nodes")), topology.memoryNodes());
        }
        System.out.println("topology eligible=" + topology.eligibleCpus() + " memoryNodes=" + topology.memoryNodes()
                + " quota=" + topology.cpuQuota() + " limitations=" + topology.limitations());
    }
}

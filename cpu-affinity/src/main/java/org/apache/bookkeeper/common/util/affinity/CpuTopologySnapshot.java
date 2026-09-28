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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

/** Read-only startup facts. Capture before narrowing any application thread's deployment mask. */
public record CpuTopologySnapshot(BitSet deploymentMask, BitSet onlineCpus, BitSet effectiveCpus,
                                  BitSet memoryNodes, OptionalDouble cpuQuota, Map<Integer, Cpu> cpus,
                                  int deviceNode, List<String> limitations) {
    public record Cpu(int packageId, int coreId, BitSet siblings, int node) {
        public Cpu {
            siblings = (BitSet) siblings.clone();
        }

        @Override
        public BitSet siblings() {
            return (BitSet) siblings.clone();
        }

        public boolean coreKnown() {
            return packageId >= 0 && coreId >= 0 && !siblings.isEmpty();
        }
    }

    record CgroupPaths(Path root, Path leaf) {
        CgroupPaths {
            root = root.toAbsolutePath().normalize();
            leaf = leaf.toAbsolutePath().normalize();
            if (!leaf.startsWith(root)) {
                throw new IllegalArgumentException("Cgroup leaf outside visible mount");
            }
        }
    }

    public CpuTopologySnapshot {
        if (deploymentMask.isEmpty() || deploymentMask.length() > CpuBinding.MAX_CPUS
                || onlineCpus.length() > CpuBinding.MAX_CPUS || effectiveCpus.length() > CpuBinding.MAX_CPUS
                || memoryNodes.length() > CpuBinding.MAX_CPUS
                || (cpuQuota.isPresent() && !(cpuQuota.getAsDouble() > 0))) {
            throw new IllegalArgumentException("Invalid topology bounds");
        }
        deploymentMask = (BitSet) deploymentMask.clone();
        onlineCpus = (BitSet) onlineCpus.clone();
        effectiveCpus = (BitSet) effectiveCpus.clone();
        memoryNodes = (BitSet) memoryNodes.clone();
        cpus = Map.copyOf(cpus);
        limitations = List.copyOf(limitations);
    }

    @Override
    public BitSet deploymentMask() {
        return (BitSet) deploymentMask.clone();
    }

    @Override
    public BitSet onlineCpus() {
        return (BitSet) onlineCpus.clone();
    }

    @Override
    public BitSet effectiveCpus() {
        return (BitSet) effectiveCpus.clone();
    }

    @Override
    public BitSet memoryNodes() {
        return (BitSet) memoryNodes.clone();
    }

    public BitSet eligibleCpus() {
        BitSet result = deploymentMask();
        result.and(onlineCpus);
        result.and(effectiveCpus);
        return result;
    }

    /** deviceNumaFile is an optional verified sysfs device numa_node path; null means UNKNOWN. */
    public static CpuTopologySnapshot capture(Path deviceNumaFile) throws IOException {
        return refresh(CpuBinding.currentThreadMask(), deviceNumaFile);
    }

    /** Re-read resource limits without mistaking a bound worker's narrow mask for deployment capacity. */
    public static CpuTopologySnapshot refresh(BitSet deployment, Path deviceNumaFile) throws IOException {
        if (deployment == null || deployment.isEmpty() || deployment.length() > CpuBinding.MAX_CPUS) {
            throw new IllegalArgumentException("Invalid initial deployment mask");
        }
        CgroupPaths paths = null;
        List<String> limitations = new ArrayList<>();
        try {
            paths = cgroupPaths(Files.readString(Path.of("/proc/self/cgroup")),
                    Files.readString(Path.of("/proc/self/mountinfo")));
        } catch (IOException invalid) {
            limitations.add("cgroup v2 unavailable: " + invalid.getMessage());
        }
        return read(deployment, Path.of("/sys/devices/system/cpu"), paths, deviceNumaFile, limitations);
    }

    static CpuTopologySnapshot read(BitSet deployment, Path cpuRoot, CgroupPaths paths, Path deviceNumaFile,
                                    List<String> initialLimitations) throws IOException {
        List<String> limitations = new ArrayList<>(initialLimitations);
        BitSet online = readSet(cpuRoot.resolve("online"));
        BitSet effective = new BitSet();
        BitSet memory = new BitSet();
        OptionalDouble quota = OptionalDouble.empty();
        if (paths != null) {
            try {
                effective = readSet(paths.leaf().resolve("cpuset.cpus.effective"));
                memory = readSet(paths.leaf().resolve("cpuset.mems.effective"));
            } catch (IOException unavailable) {
                limitations.add("effective cpuset unavailable: " + unavailable.getMessage());
            }
            double minimum = Double.POSITIVE_INFINITY;
            boolean seen = false;
            boolean complete = true;
            for (Path directory = paths.leaf();; directory = directory.getParent()) {
                Path file = directory.resolve("cpu.max");
                try {
                    minimum = Math.min(minimum, parseQuota(Files.readString(file)));
                    seen = true;
                } catch (NoSuchFileException absent) {
                    // The hierarchy root normally has no cpu.max; missing intermediate limits are unknown.
                    if (!directory.equals(paths.root())) {
                        complete = false;
                        limitations.add("CPU quota unavailable at " + directory);
                    }
                } catch (IOException invalid) {
                    complete = false;
                    limitations.add("CPU quota unknown: " + invalid.getMessage());
                }
                if (directory.equals(paths.root())) {
                    break;
                }
            }
            if (seen && complete) {
                quota = OptionalDouble.of(minimum);
            } else {
                limitations.add("CPU quota not established for visible hierarchy");
            }
            limitations.add("cgroup limits cover the visible mount hierarchy only; "
                    + "hidden ancestors are not observable");
        }
        BitSet inspect = (BitSet) deployment.clone();
        inspect.and(online);
        Map<Integer, Cpu> cpus = new LinkedHashMap<>();
        for (int cpu = inspect.nextSetBit(0); cpu >= 0; cpu = inspect.nextSetBit(cpu + 1)) {
            Path directory = cpuRoot.resolve("cpu" + cpu);
            int node = -1;
            try (var entries = Files.newDirectoryStream(directory, "node[0-9]*")) {
                for (Path entry : entries) {
                    int found = Integer.parseInt(entry.getFileName().toString().substring(4));
                    if (node != -1) {
                        throw new IOException("Multiple NUMA nodes for CPU " + cpu);
                    }
                    node = found;
                }
            } catch (IOException | NumberFormatException unavailable) {
                limitations.add("CPU " + cpu + " NUMA node unknown");
                node = -1;
            }
            Cpu info;
            try {
                int packageId = readId(directory.resolve("topology/physical_package_id"));
                int coreId = readId(directory.resolve("topology/core_id"));
                BitSet siblings = readSet(directory.resolve("topology/thread_siblings_list"));
                if (packageId < 0 || coreId < 0 || !siblings.get(cpu)) {
                    throw new IOException("Unknown core identity");
                }
                info = new Cpu(packageId, coreId, siblings, node);
            } catch (IOException unavailable) {
                limitations.add("CPU " + cpu + " physical core unknown");
                info = new Cpu(-1, -1, new BitSet(), node);
            }
            cpus.put(cpu, info);
        }
        // Validate each sibling group once, not every CPU against every other CPU.
        Map<BitSet, List<Integer>> groups = new LinkedHashMap<>();
        for (Map.Entry<Integer, Cpu> entry : cpus.entrySet()) {
            if (entry.getValue().coreKnown()) {
                groups.computeIfAbsent(entry.getValue().siblings, ignored -> new ArrayList<>()).add(entry.getKey());
            }
        }
        Map<Integer, Cpu> checked = new LinkedHashMap<>(cpus);
        Set<BitSet> invalidGroups = new HashSet<>();
        for (Map.Entry<BitSet, List<Integer>> group : groups.entrySet()) {
            Cpu first = cpus.get(group.getValue().get(0));
            BitSet visiblePeers = (BitSet) group.getKey().clone();
            visiblePeers.and(inspect);
            for (int peer = visiblePeers.nextSetBit(0); peer >= 0; peer = visiblePeers.nextSetBit(peer + 1)) {
                Cpu other = cpus.get(peer);
                if (!first.siblings.equals(other.siblings) || first.packageId != other.packageId
                        || first.coreId != other.coreId) {
                    invalidGroups.add(first.siblings);
                    invalidGroups.add(other.siblings);
                }
            }
        }
        for (Map.Entry<BitSet, List<Integer>> group : groups.entrySet()) {
            if (invalidGroups.contains(group.getKey())) {
                for (int cpu : group.getValue()) {
                    checked.put(cpu, new Cpu(-1, -1, new BitSet(), cpus.get(cpu).node));
                }
                limitations.add("Inconsistent sibling group " + group.getKey());
            }
        }
        int deviceNode = -1;
        if (deviceNumaFile != null) {
            try {
                deviceNode = readId(deviceNumaFile);
            } catch (IOException unavailable) {
                limitations.add("DATA device NUMA node unavailable");
            }
        }
        if (deviceNode < 0) {
            limitations.add("DATA device NUMA node UNKNOWN");
        }
        return new CpuTopologySnapshot(deployment, online, effective, memory, quota, checked,
                deviceNode, limitations);
    }

    public static BitSet parseCpuList(String text) {
        String value = text.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Empty CPU/node list");
        }
        BitSet result = new BitSet();
        for (String item : value.split(",", -1)) {
            if (!item.matches("[0-9]+(-[0-9]+)?")) {
                throw new IllegalArgumentException("Invalid CPU/node list: " + text);
            }
            String[] range = item.split("-");
            int first = Integer.parseInt(range[0]);
            int last = range.length == 1 ? first : Integer.parseInt(range[1]);
            if (first > last || last >= CpuBinding.MAX_CPUS) {
                throw new IllegalArgumentException("CPU/node range out of bounds");
            }
            int previous = result.nextSetBit(first);
            if (previous >= 0 && previous <= last) {
                throw new IllegalArgumentException("Duplicate CPU/node selection");
            }
            result.set(first, last + 1);
        }
        return result;
    }

    private static BitSet readSet(Path path) throws IOException {
        try {
            return parseCpuList(Files.readString(path));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid CPU/node list in " + path, invalid);
        }
    }

    private static int readId(Path path) throws IOException {
        try {
            return Integer.parseInt(Files.readString(path).trim());
        } catch (NumberFormatException invalid) {
            throw new IOException("Invalid topology identifier in " + path, invalid);
        }
    }

    static double parseQuota(String text) throws IOException {
        String[] parts = text.trim().split("\\s+");
        try {
            if (parts.length != 2 || Long.parseLong(parts[1]) <= 0) {
                throw new NumberFormatException();
            }
            if (parts[0].equals("max")) {
                return Double.POSITIVE_INFINITY;
            }
            long quota = Long.parseLong(parts[0]);
            if (quota <= 0) {
                throw new NumberFormatException();
            }
            return (double) quota / Long.parseLong(parts[1]);
        } catch (NumberFormatException invalid) {
            throw new IOException("Invalid cpu.max: " + text.trim(), invalid);
        }
    }

    static CgroupPaths cgroupPaths(String membership, String mounts) throws IOException {
        String group = membership.lines().filter(line -> line.startsWith("0::"))
                .map(line -> line.substring(3)).findFirst().orElseThrow(() -> new IOException("No cgroup v2 entry"));
        Path member = Path.of(group).normalize();
        if (!member.isAbsolute() || !member.toString().equals(group)) {
            throw new IOException("Unresolvable cgroup membership path");
        }
        for (String line : mounts.lines().toList()) {
            String[] halves = line.split(" - ", 2);
            if (halves.length != 2 || !halves[1].startsWith("cgroup2 ")) {
                continue;
            }
            String[] fields = halves[0].split(" ");
            if (fields.length < 5) {
                throw new IOException("Invalid cgroup mountinfo");
            }
            Path root = Path.of(unescape(fields[3])).normalize();
            Path mount = Path.of(unescape(fields[4])).normalize();
            if (!root.isAbsolute() || !mount.isAbsolute()) {
                throw new IOException("Relative cgroup mount path");
            }
            Path relative;
            if (member.startsWith(root)) {
                relative = root.relativize(member);
            } else {
                continue;
            }
            return new CgroupPaths(mount, mount.resolve(relative).normalize());
        }
        throw new IOException("No matching cgroup v2 mount");
    }

    private static String unescape(String path) {
        return path.replace("\\040", " ").replace("\\011", "\t")
                .replace("\\012", "\n").replace("\\134", "\\");
    }
}

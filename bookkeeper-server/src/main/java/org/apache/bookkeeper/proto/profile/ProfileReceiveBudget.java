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
package org.apache.bookkeeper.proto.profile;

import java.util.concurrent.CompletableFuture;

/** One bounded receive ledger shared by unclassified, DATA and reserved CONTROL connections. */
public final class ProfileReceiveBudget {
    public enum Lane { UNCLASSIFIED, DATA, CONTROL }

    /** The allocator refused credit before allocating backing storage. */
    public static final class CapacityExceededException extends IllegalStateException {
        CapacityExceededException() {
            super("Profile backing buffer capacity exhausted or connection closed");
        }
    }

    final class ConnectionPermit implements AutoCloseable {
        private final ProfileReceiveBudget owner = ProfileReceiveBudget.this;
        private Lane lane = Lane.UNCLASSIFIED;
        private int byteLimit;
        private long bytes;
        private long workspaceBytes;
        private long peakBytes;
        private boolean closed;
        private final CompletableFuture<Void> drained = new CompletableFuture<>();

        private ConnectionPermit() {
        }

        void limit(int limit) {
            synchronized (ProfileReceiveBudget.this) {
                if (closed || limit < 1 || byteLimit != 0) {
                    throw new IllegalStateException("Receive connection limit must be configured once");
                }
                byteLimit = limit;
            }
        }

        Credit reserve(int requested) {
            return reserve(requested, false);
        }

        Credit reserveWorkspace(int requested) {
            return reserve(requested, true);
        }

        private Credit reserve(int requested, boolean workspace) {
            synchronized (ProfileReceiveBudget.this) {
                if (closed || byteLimit == 0 || requested > byteLimit - bytes) {
                    return null;
                }
                Credit credit = ProfileReceiveBudget.this.reserveOwned(lane, requested);
                if (credit != null) {
                    credit.connection = this;
                    credit.workspace = workspace;
                    bytes += requested;
                    if (workspace) {
                        workspaceBytes += requested;
                        usedWorkspaceBytes[lane.ordinal()] += requested;
                    }
                    peakBytes = Math.max(peakBytes, bytes - workspaceBytes);
                }
                return credit;
            }
        }

        @Override
        public void close() {
            synchronized (ProfileReceiveBudget.this) {
                if (!closed) {
                    closed = true;
                    usedConnections[lane.ordinal()]--;
                }
            }
            completeConnectionDrainIfIdle();
            completeDrainIfIdle();
        }

        private void completeConnectionDrainIfIdle() {
            synchronized (ProfileReceiveBudget.this) {
                if (!closed || bytes != 0) {
                    return;
                }
            }
            drained.complete(null);
        }
    }

    final class Credit implements AutoCloseable {
        private final ProfileReceiveBudget owner = ProfileReceiveBudget.this;
        private Lane lane;
        private int bytes;
        private ConnectionPermit connection;
        private boolean workspace;
        private boolean closed;

        private Credit(Lane lane, int bytes) {
            this.lane = lane;
            this.bytes = bytes;
        }

        void require(ConnectionPermit expected, int size) {
            synchronized (ProfileReceiveBudget.this) {
                if (closed || workspace || connection != expected || expected.closed || bytes != size) {
                    throw new IllegalStateException("Invalid backing buffer receive credit");
                }
            }
        }

        Credit split(int size) {
            synchronized (ProfileReceiveBudget.this) {
                if (closed || size < 0 || size > bytes) {
                    throw new IllegalStateException("Invalid receive credit split");
                }
                Credit child = new Credit(lane, size);
                child.connection = connection;
                child.workspace = workspace;
                bytes -= size;
                return child;
            }
        }

        /** Reclassify an already reserved response capacity before its backing allocation. */
        Credit splitToBacking(int size) {
            synchronized (ProfileReceiveBudget.this) {
                if (closed || !workspace || connection == null || connection.closed || size < 0 || size > bytes) {
                    throw new IllegalStateException("Invalid response workspace transfer");
                }
                Credit child = new Credit(lane, size);
                child.connection = connection;
                bytes -= size;
                connection.workspaceBytes -= size;
                usedWorkspaceBytes[lane.ordinal()] -= size;
                connection.peakBytes = Math.max(connection.peakBytes, connection.bytes - connection.workspaceBytes);
                return child;
            }
        }

        @Override
        public void close() {
            synchronized (ProfileReceiveBudget.this) {
                if (closed) {
                    throw new IllegalStateException("Receive credit returned twice");
                }
                closed = true;
                usedBytes[lane.ordinal()] -= bytes;
                if (connection != null) {
                    connection.bytes -= bytes;
                    if (workspace) {
                        connection.workspaceBytes -= bytes;
                        usedWorkspaceBytes[lane.ordinal()] -= bytes;
                    }
                }
            }
            if (connection != null) {
                connection.completeConnectionDrainIfIdle();
            }
            completeDrainIfIdle();
        }
    }

    private final long[] limits = new long[Lane.values().length];
    private final long[] usedBytes = new long[Lane.values().length];
    private final long[] usedWorkspaceBytes = new long[Lane.values().length];
    private final int[] connectionLimits = new int[Lane.values().length];
    private final int[] usedConnections = new int[Lane.values().length];
    private boolean accepting = true;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();

    public ProfileReceiveBudget(long totalBytes, long reservedControlBytes, long unclassifiedBytes,
                         int maxUnclassifiedConnections, int maxDataConnections, int maxControlConnections) {
        if (totalBytes < 3 || reservedControlBytes < 1 || reservedControlBytes >= totalBytes
                || unclassifiedBytes < 1 || unclassifiedBytes >= totalBytes - reservedControlBytes
                || maxUnclassifiedConnections < 1 || maxDataConnections < 1 || maxControlConnections < 1) {
            throw new IllegalArgumentException("Positive unclassified, DATA and CONTROL budgets and slots required");
        }
        limits[Lane.UNCLASSIFIED.ordinal()] = unclassifiedBytes;
        limits[Lane.DATA.ordinal()] = totalBytes - reservedControlBytes - unclassifiedBytes;
        limits[Lane.CONTROL.ordinal()] = reservedControlBytes;
        connectionLimits[Lane.UNCLASSIFIED.ordinal()] = maxUnclassifiedConnections;
        connectionLimits[Lane.DATA.ordinal()] = maxDataConnections;
        connectionLimits[Lane.CONTROL.ordinal()] = maxControlConnections;
    }

    /** Outbound role is chosen by the local client before TLS; this grants no remote operation authority. */
    public final class ClientAllocation implements AutoCloseable {
        private final ConnectionPermit connection;
        private final ProfileByteBufAllocator allocator;

        private ClientAllocation(ConnectionPermit connection) {
            this.connection = connection;
            allocator = new ProfileByteBufAllocator(connection);
        }

        public io.netty.buffer.ByteBufAllocator allocator() {
            return allocator;
        }

        public long usedBytes() {
            synchronized (ProfileReceiveBudget.this) {
                return connection.bytes - connection.workspaceBytes;
            }
        }

        /** Peak reserved backing capacities, including an allocation before its construction completes. */
        public long peakBytes() {
            synchronized (ProfileReceiveBudget.this) {
                return connection.peakBytes;
            }
        }

        public CompletableFuture<Void> drained() {
            return connection.drained.copy();
        }

        /** Reserved capacity for bounded codec temporaries, retained until parsing/copy/ownership handoff ends. */
        public final class Workspace implements AutoCloseable {
            private final Credit credit;

            private Workspace(Credit credit) {
                this.credit = credit;
            }

            @Override
            public void close() {
                credit.close();
            }
        }

        public Workspace reserveWorkspace(int bytes) {
            Credit credit = connection.reserveWorkspace(bytes);
            if (credit == null) {
                throw new CapacityExceededException();
            }
            try {
                return new Workspace(credit);
            } catch (RuntimeException | Error failedConstruction) {
                credit.close();
                throw failedConstruction;
            }
        }

        public long workspaceBytes() {
            synchronized (ProfileReceiveBudget.this) {
                return connection.workspaceBytes;
            }
        }

        @Override
        public void close() {
            connection.close();
        }
    }

    /** Bound actual outbound backing with the existing byte/slot ledger; no handshake or readiness assertion. */
    public synchronized ClientAllocation admitClient(Lane role, int connectionBytes) {
        if (role == null || role == Lane.UNCLASSIFIED || connectionBytes < 1) {
            throw new IllegalArgumentException("Outbound client role and backing limit required");
        }
        int index = role.ordinal();
        if (!accepting || usedConnections[index] >= connectionLimits[index]) {
            return null;
        }
        var permit = new ConnectionPermit();
        permit.lane = role;
        permit.byteLimit = connectionBytes;
        usedConnections[index]++;
        return new ClientAllocation(permit);
    }

    /** Called before a TLS handler is created for the accepted channel. */
    synchronized ConnectionPermit admitUnclassified() {
        int index = Lane.UNCLASSIFIED.ordinal();
        if (!accepting || usedConnections[index] >= connectionLimits[index]) {
            return null;
        }
        usedConnections[index]++;
        return new ConnectionPermit();
    }

    synchronized Credit reserve(Lane lane, int bytes) {
        if (lane == null || bytes < 0) {
            throw new IllegalArgumentException("Invalid receive credit request");
        }
        return accepting ? reserveOwned(lane, bytes) : null;
    }

    // Existing connections may need charged TLS close/output buffers during retirement.
    // Their slot prevents idle completion; a closed connection cannot reserve again.
    private Credit reserveOwned(Lane lane, int bytes) {
        if (lane == null || bytes < 0) {
            throw new IllegalArgumentException("Invalid receive credit request");
        }
        int index = lane.ordinal();
        if (bytes > limits[index] - usedBytes[index]) {
            return null;
        }
        usedBytes[index] += bytes;
        return new Credit(lane, bytes);
    }

    /** Retire new connections and copy actual tracked byte/slot drain; executor resources remain caller-owned. */
    public CompletableFuture<Void> closeAdmissionAndDrain() {
        synchronized (this) {
            accepting = false;
        }
        completeDrainIfIdle();
        return drained.copy();
    }

    private void completeDrainIfIdle() {
        synchronized (this) {
            if (accepting) {
                return;
            }
            for (int lane = 0; lane < usedBytes.length; lane++) {
                if (usedBytes[lane] != 0 || usedConnections[lane] != 0) {
                    return;
                }
            }
        }
        // User callbacks must not execute under the receive ledger monitor.
        drained.complete(null);
    }

    /** Move existing ownership without a release-and-reserve gap. A failed move leaves every credit unchanged. */
    synchronized boolean transfer(Lane target, ConnectionPermit connection, Credit... credits) {
        if (target == null || connection == null || credits.length == 0 || credits[0] == null) {
            throw new IllegalArgumentException("Receive target, connection and credits required");
        }
        Lane source = credits[0].lane;
        if (connection.owner != this || connection.closed || connection.lane != source) {
            throw new IllegalStateException("Invalid receive connection transfer");
        }
        long bytes = 0;
        long workspaceBytes = 0;
        for (int i = 0; i < credits.length; i++) {
            Credit credit = credits[i];
            if (credit == null || credit.owner != this || credit.closed || credit.lane != source
                    || (credit.connection != null && credit.connection != connection)) {
                throw new IllegalStateException("Invalid receive credit transfer");
            }
            for (int previous = 0; previous < i; previous++) {
                if (credit == credits[previous]) {
                    throw new IllegalStateException("Receive credit transferred twice");
                }
            }
            bytes += credit.bytes;
            if (credit.workspace) {
                workspaceBytes += credit.bytes;
            }
        }
        if (source == target || bytes > limits[target.ordinal()] - usedBytes[target.ordinal()]
                || usedConnections[target.ordinal()] >= connectionLimits[target.ordinal()]) {
            return false;
        }
        usedBytes[source.ordinal()] -= bytes;
        usedBytes[target.ordinal()] += bytes;
        usedWorkspaceBytes[source.ordinal()] -= workspaceBytes;
        usedWorkspaceBytes[target.ordinal()] += workspaceBytes;
        usedConnections[source.ordinal()]--;
        usedConnections[target.ordinal()]++;
        connection.lane = target;
        for (Credit credit : credits) {
            credit.lane = target;
        }
        return true;
    }

    public synchronized long used(Lane lane) {
        if (lane == null) {
            throw new IllegalArgumentException("Receive lane required");
        }
        return usedBytes[lane.ordinal()];
    }

    /** Reserved parse/cold/codec work capacity; included in used(), not a measurement of heap usage. */
    public synchronized long workspaceBytes(Lane lane) {
        if (lane == null) {
            throw new IllegalArgumentException("Receive lane required");
        }
        return usedWorkspaceBytes[lane.ordinal()];
    }

    public synchronized int usedConnections(Lane lane) {
        if (lane == null) {
            throw new IllegalArgumentException("Receive lane required");
        }
        return usedConnections[lane.ordinal()];
    }
}

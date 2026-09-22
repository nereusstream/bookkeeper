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
package org.apache.bookkeeper.client;

import com.sun.management.ThreadMXBean;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Normal PendingAddOp creation, three current-address responses, callback and recycling.
 * Excludes digest/transport/payload allocation: this measures client bookkeeping, not end-to-end Add.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PendingAddOpBenchmark {
    @State(Scope.Thread)
    public static class WriteState {
        @Param({"false", "true"})
        public boolean checkFaultDomains;
        private LedgerHandle handle;
        private ClientContext context;
        private OrderedScheduler executor;
        private ByteBuf buffer;
        private List<BookieId> ensemble;
        private long nextEntry;
        private long lastCompleted;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setEnforceMinNumFaultDomainsForWrite(checkFaultDomains);
            configuration.setAddEntryQuorumTimeout(0);
            ClientInternalConf conf = ClientInternalConf.fromConfig(configuration);
            executor = OrderedScheduler.newSchedulerBuilder().name("pending-add-benchmark").numThreads(1).build();
            EnsemblePlacementPolicy policy = new DefaultEnsemblePlacementPolicy();
            BookKeeperClientStats stats = BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE);
            context = new ClientContext() {
                @Override
                public ClientInternalConf getConf() {
                    return conf;
                }
                @Override
                public LedgerManager getLedgerManager() {
                    throw new UnsupportedOperationException();
                }
                @Override
                public BookieWatcher getBookieWatcher() {
                    throw new UnsupportedOperationException();
                }
                @Override
                public EnsemblePlacementPolicy getPlacementPolicy() {
                    return policy;
                }
                @Override
                public BookieClient getBookieClient() {
                    throw new UnsupportedOperationException();
                }
                @Override
                public ByteBufAllocator getByteBufAllocator() {
                    return ByteBufAllocator.DEFAULT;
                }
                @Override
                public OrderedExecutor getMainWorkerPool() {
                    return executor;
                }
                @Override
                public OrderedScheduler getScheduler() {
                    return executor;
                }
                @Override
                public BookKeeperClientStats getClientStats() {
                    return stats;
                }
                @Override
                public boolean isClientClosed() {
                    return false;
                }
            };
            ensemble = List.of(BookieId.parse("a:3181"), BookieId.parse("b:3181"), BookieId.parse("c:3181"));
            handle = new LedgerHandle(context, 1, new Versioned<>(LedgerMetadataBuilder.create().withId(1)
                    .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                    .withDigestType(DigestType.CRC32C).withPassword(new byte[0])
                    .newEnsembleEntry(0, ensemble).build(), new LongVersion(0)),
                    BookKeeper.DigestType.CRC32C, new byte[0], WriteFlag.NONE);
            buffer = Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(new byte[1024]));
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            executor.shutdown();
        }
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class CpuCounters {
        public long cpuNanos;
        public long operations;
        private long started;
        private final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        @Setup(Level.Iteration)
        public void start() {
            cpuNanos = 0;
            operations = 0;
            started = bean.getCurrentThreadCpuTime();
        }

        @TearDown(Level.Iteration)
        public void stop() {
            cpuNanos = bean.getCurrentThreadCpuTime() - started;
        }
    }

    @Benchmark
    public long currentResponses(WriteState state, CpuCounters cpu) {
        cpu.operations++;
        PendingAddOp op = PendingAddOp.create(state.handle, state.context, state.ensemble, state.buffer,
                WriteFlag.NONE, (rc, handle, id, latency, context) -> ((WriteState) context).lastCompleted = id, state);
        op.setEntryId(state.nextEntry++);
        op.hasRun = true;
        op.toSend = op.payload;
        op.payload = null;
        op.pendingWriteRequests = 3;
        op.requestTimeNanos = System.nanoTime();
        state.handle.pendingAddOps.add(op);
        for (int i = 0; i < 3; i++) {
            op.writeComplete(BKException.Code.OK, 1, op.entryId, state.ensemble.get(i), i);
        }
        return state.lastCompleted;
    }
}

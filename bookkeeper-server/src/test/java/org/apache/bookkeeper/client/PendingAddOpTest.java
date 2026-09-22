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
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link PendingAddOp}.
 */
public class PendingAddOpTest {

    private LedgerHandle lh;
    private ClientContext mockClientContext;

    private ByteBuf payload;

    @Before
    public void setup() {
        BookKeeperClientStats clientStats = BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE);
        BookieClient bookieClient = mock(BookieClient.class);
        OrderedExecutor mainWorkerPool = mock(OrderedExecutor.class);
        mockClientContext = mock(ClientContext.class);
        when(mockClientContext.getBookieClient()).thenReturn(bookieClient);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.defaultValues());
        when(mockClientContext.getMainWorkerPool()).thenReturn(mainWorkerPool);
        when(mockClientContext.getClientStats()).thenReturn(clientStats);

        lh = mock(LedgerHandle.class);
        when(lh.getDistributionSchedule())
            .thenReturn(new RoundRobinDistributionSchedule(3, 3, 2));
        byte[] data = "test-pending-add-op".getBytes(UTF_8);
        payload = Unpooled.wrappedBuffer(data);
        payload.writerIndex(data.length);
    }

    @Test
    public void testExecuteAfterCancelled() {
        AtomicInteger rcHolder = new AtomicInteger(-0xdead);
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {
                    rcHolder.set(rc);
                }, null);
        assertSame(lh, op.lh);

        // cancel the op.
        op.submitCallback(Code.NotEnoughBookiesException);
        // if a op is cancelled, it is not recycled until it has been run.
        assertSame(lh, op.lh);
        assertEquals(Code.NotEnoughBookiesException, rcHolder.get());

        op.initiate();
        // after the op is run, the object is recycled.
        assertNull(op.lh);
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code false} without throwing
     * {@link NullPointerException} when {@code clientCtx} is {@code null}.
     *
     * <p>This is the exact scenario that caused the production NPE reported in
     * apache/bookkeeper#4759: {@code monitorPendingAddOps()} holds an iterator reference to
     * an op while {@code recyclePendAddOpObject()} runs concurrently on another thread and
     * clears {@code clientCtx} to {@code null}. After recycling the op has already completed,
     * so the correct behaviour is to skip the timeout check (return {@code false}).
     */
    @Test
    public void testMaybeTimeoutReturnsFalseWhenClientCtxIsNull() {
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);

        // Simulate the race: recyclePendAddOpObject() cleared clientCtx on the writer
        // thread while monitorPendingAddOps() is still iterating the pendingAddOps queue
        // on the scheduler thread.
        op.clientCtx = null;

        // Before the fix this threw NullPointerException; after the fix it must return false.
        assertFalse(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code false} when
     * {@code clientCtx} is non-null and the quorum timeout has not yet elapsed.
     */
    @Test
    public void testMaybeTimeoutReturnsFalseWhenWithinQuorumTimeout() {
        // Configure a 1-hour quorum timeout so the op will not have expired.
        ClientConfiguration conf = new ClientConfiguration();
        conf.setAddEntryQuorumTimeout(3600);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.fromConfig(conf));

        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);
        // Stamp the request as starting right now so elapsed time is effectively 0.
        op.requestTimeNanos = MathUtils.nowInNano();

        assertFalse(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code true} and triggers
     * {@link PendingAddOp#timeoutQuorumWait()} when the quorum timeout has already elapsed.
     */
    @Test
    public void testMaybeTimeoutReturnsTrueWhenQuorumTimeoutExpired() {
        // Configure a 1-second quorum timeout.
        ClientConfiguration conf = new ClientConfiguration();
        conf.setAddEntryQuorumTimeout(1);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.fromConfig(conf));

        LedgerMetadata meta = mock(LedgerMetadata.class);
        when(lh.getLedgerMetadata()).thenReturn(meta);
        // addEntrySuccessBookies starts empty (size 0 < ackQuorumSize 3),
        // so the fault-domain stats branch is skipped and no extra mocking is needed.
        when(meta.getAckQuorumSize()).thenReturn(3);

        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);
        // Set the request start time to 0 so that elapsed nanos is enormous (>> 1 second).
        op.requestTimeNanos = 0L;

        assertTrue(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#timeoutQuorumWait()} is a no-op when the op has already
     * been recycled (i.e. {@code lh} and {@code clientCtx} are {@code null}).
     *
     * <p>This covers the second concern raised in the review of #4760: even after
     * {@link PendingAddOp#maybeTimeout()} captures a non-null {@code clientCtx} snapshot,
     * {@code recyclePendAddOpObject()} may complete before {@code timeoutQuorumWait()}
     * acquires the monitor. The pre-existing {@code if (completed) return;} guard does not
     * cover this because recycling resets {@code completed} to {@code false}; without an
     * explicit null check the method NPEs on {@code lh.getLedgerMetadata()} (or similar)
     * and, worse, may invoke {@code handleUnrecoverableErrorDuringAdd} on a stale handle.
     */
    @Test
    public void testTimeoutQuorumWaitIsNoOpWhenAlreadyRecycled() {
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);

        // Simulate post-recycle state: recyclePendAddOpObject() has already nulled these fields.
        op.lh = null;
        op.clientCtx = null;

        // Must not throw NPE — and must not invoke the unrecoverable-error handler on the
        // (now-stale) ledger handle, which would cause spurious failures on a recycled op.
        op.timeoutQuorumWait();
        verify(lh, never()).handleUnrecoverableErrorDuringAdd(anyInt());
    }
    @Test
    public void testLastOldResponseRecyclesAfterCallback() throws Exception {
        assertResponseRecycling(true, true);
    }

    @Test
    public void testLastCurrentResponseRecyclesAfterCallback() throws Exception {
        assertResponseRecycling(false, true);
    }

    @Test
    public void testLastOldResponseWaitsForCallback() throws Exception {
        assertResponseRecycling(true, false);
    }

    @Test
    public void testLastCurrentResponseWaitsForCallback() throws Exception {
        assertResponseRecycling(false, false);
    }

    private void assertResponseRecycling(boolean oldResponseLast, boolean callbackFirst) throws Exception {
        BookieId oldBookie = BookieId.parse("old:3181");
        BookieId replacement = BookieId.parse("replacement:3181");
        BookieId unchanged = BookieId.parse("unchanged:3181");
        List<BookieId> ensemble = List.of(replacement, unchanged, BookieId.parse("third:3181"));
        when(lh.getDistributionSchedule()).thenReturn(new RoundRobinDistributionSchedule(3, 2, 3));
        AtomicInteger callbacks = new AtomicInteger();
        PendingAddOp op = PendingAddOp.create(lh, mockClientContext, ensemble, payload, WriteFlag.NONE,
                (rc, handle, entryId, latency, ctx) -> callbacks.incrementAndGet(), null);
        // Count the real lifecycle's recycle call without relying on Netty's sampling ratio or pool order.
        @SuppressWarnings("unchecked")
        Recycler.Handle<PendingAddOp> recycler = mock(Recycler.Handle.class);
        Field handleField = PendingAddOp.class.getDeclaredField("recyclerHandle");
        handleField.setAccessible(true);
        handleField.set(op, recycler);
        op.entryId = 1;
        op.hasRun = true;
        op.completed = true;
        op.toSend = payload;
        op.payload = null;
        op.pendingWriteRequests = 2;
        // Each outstanding transport retains its own reference until its response is consumed.
        payload.retain(2);
        if (callbackFirst) {
            op.submitCallback(Code.OK);
            assertEquals(2, payload.refCnt());
        }
        for (int i = 0; i < 2; i++) {
            boolean old = (i == 1) == oldResponseLast;
            op.writeComplete(Code.OK, 0, 1, old ? oldBookie : unchanged, old ? 0 : 1);
            payload.release();
            if (i == 0 || !callbackFirst) {
                verify(recycler, never()).recycle(op);
                assertSame(lh, op.lh);
            }
        }
        if (!callbackFirst) {
            assertEquals(1, payload.refCnt());
            assertEquals(0, callbacks.get());
            op.submitCallback(Code.OK);
        }
        assertEquals(1, callbacks.get());
        assertEquals(0, payload.refCnt());
        assertNull(op.lh);
        assertNull(op.toSend);
        verify(recycler, times(1)).recycle(op);
        assertFalse(op.maybeTimeout());
    }
    @Test
    public void testMultiSlotChangeStartsOutsideWriteSet() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            PendingAddOp op = scenario.add(1);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 1, scenario.original.get(1));
            scenario.ack(1, 2, scenario.original.get(2));
            assertTrue(op.completed);
            scenario.handle.changingEnsemble = false;
            scenario.handle.unsetSuccessAndSendWriteRequest(scenario.replacement,
                    new LinkedHashSet<>(List.of(0, 1)));
            assertTrue("slot outside write set must not publish success", scenario.successes.isEmpty());
            assertFalse(op.completed);
            assertEquals(4, scenario.sends.size()); // W initial sends, only the changed write-set slot resent
            scenario.ack(1, 1, scenario.replacement.get(1));
            assertEquals(List.of(1L), scenario.successes); // unchanged slot 2 ACK survived
            scenario.ack(1, 3, scenario.original.get(3));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testAllPendingMappedBeforeInlineReplacementResponse() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            PendingAddOp first = scenario.add(1);
            PendingAddOp second = scenario.add(2);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 1, scenario.original.get(1));
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.ack(2, 0, scenario.original.get(0));
            scenario.ack(2, 2, scenario.original.get(2));
            assertTrue(first.completed && second.completed);
            scenario.onSend = send -> {
                if (send.bookie.equals(scenario.replacement.get(1))) {
                    assertEquals(scenario.replacement, second.ensemble);
                    assertFalse(second.completed);
                    send.ack();
                    assertTrue("all pending must remain behind the gate", scenario.successes.isEmpty());
                }
            };
            scenario.handle.unsetSuccessAndSendWriteRequest(scenario.replacement,
                    new LinkedHashSet<>(List.of(0, 1)));
            assertTrue(scenario.handle.changingEnsemble);
            assertTrue(scenario.successes.isEmpty());
            scenario.handle.changingEnsemble = false;
            scenario.handle.sendAddSuccessCallbacks();
            assertEquals(List.of(1L), scenario.successes);
            scenario.ack(2, 0, scenario.replacement.get(0));
            assertEquals(List.of(1L, 2L), scenario.successes);
            assertEquals(8, scenario.sends.size()); // no resends for unchanged replicas
        }
    }

    @Test
    public void testQuantityQuorumLosesFaultDomainAfterReplacement() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(4, true)) {
            PendingAddOp op = scenario.add(1);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 0, scenario.original.get(0));
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.ack(1, 3, scenario.original.get(3));
            assertTrue(op.completed);
            scenario.handle.unsetSuccessAndSendWriteRequest(
                    List.of(scenario.replacement.get(0), scenario.original.get(1),
                            scenario.original.get(2), scenario.original.get(3)), Set.of(0));
            assertFalse("quantity quorum without fault-domain coverage is insufficient", op.completed);
            assertEquals(Set.of(scenario.original.get(2), scenario.original.get(3)), op.addEntrySuccessBookies);
            scenario.handle.changingEnsemble = false;
            scenario.handle.sendAddSuccessCallbacks();
            assertTrue(scenario.successes.isEmpty());
            scenario.ack(1, 0, scenario.replacement.get(0));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testNewAckCannotUseReplacedBookiesFaultDomain() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(4, true)) {
            PendingAddOp op = scenario.add(1);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 0, scenario.original.get(0));
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.handle.unsetSuccessAndSendWriteRequest(
                    List.of(scenario.replacement.get(0), scenario.original.get(1),
                            scenario.original.get(2), scenario.original.get(3)), Set.of(0));
            scenario.handle.changingEnsemble = false;
            scenario.ack(1, 3, scenario.original.get(3));
            assertFalse(op.completed);
            assertTrue(scenario.successes.isEmpty());
            assertEquals(Set.of(scenario.original.get(2), scenario.original.get(3)), op.addEntrySuccessBookies);
            scenario.ack(1, 0, scenario.replacement.get(0));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testDisabledFaultDomainPolicyDoesNotMaintainSuccessSet() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            PendingAddOp op = scenario.add(1);
            scenario.ack(1, 1, scenario.original.get(1));
            assertTrue(op.addEntrySuccessBookies == null || op.addEntrySuccessBookies.isEmpty());
            scenario.ack(1, 2, scenario.original.get(2));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testLateOldResponseCannotRestoreFaultDomain() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(4, true)) {
            PendingAddOp op = scenario.add(1);
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.ack(1, 3, scenario.original.get(3));
            scenario.handle.unsetSuccessAndSendWriteRequest(scenario.replacement, Set.of(0, 1));
            scenario.ack(1, 0, scenario.original.get(0));
            scenario.ack(1, 1, scenario.replacement.get(1));
            assertFalse(op.completed);
            assertTrue(scenario.successes.isEmpty());
            assertFalse(op.addEntrySuccessBookies.contains(scenario.original.get(0)));
            scenario.ack(1, 0, scenario.replacement.get(0));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testRetrySameEnsembleKeepsCompletionGate() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            PendingAddOp op = scenario.add(1);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 1, scenario.original.get(1));
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.handle.changingEnsemble = false;
            scenario.handle.unsetSuccessAndSendWriteRequest(scenario.original, new LinkedHashSet<>(List.of(0, 1)));
            assertFalse(op.completed);
            assertTrue(scenario.successes.isEmpty());
            scenario.ack(1, 1, scenario.original.get(1));
            assertEquals(List.of(1L), scenario.successes);
        }
    }

    @Test
    public void testChangeOutsideWriteSetUnblocksCompletedEntry() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            scenario.add(1);
            scenario.handle.changingEnsemble = true;
            scenario.ack(1, 1, scenario.original.get(1));
            scenario.ack(1, 2, scenario.original.get(2));
            scenario.handle.changingEnsemble = false;
            scenario.handle.unsetSuccessAndSendWriteRequest(
                    List.of(scenario.replacement.get(0), scenario.original.get(1),
                            scenario.original.get(2), scenario.original.get(3)), Set.of(0));
            assertEquals(List.of(1L), scenario.successes);
            assertEquals(3, scenario.sends.size());
        }
    }

    @Test
    public void testFaultDomainTimeoutCounterRequiresCurrentQuantityQuorum() {
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setEnforceMinNumFaultDomainsForWrite(true);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.fromConfig(configuration));
        BookKeeperClientStats stats = mock(BookKeeperClientStats.class);
        Counter counter = mock(Counter.class);
        when(stats.getWriteTimedOutDueToNotEnoughFaultDomains()).thenReturn(counter);
        when(mockClientContext.getClientStats()).thenReturn(stats);
        LedgerMetadata metadata = mock(LedgerMetadata.class);
        when(metadata.getAckQuorumSize()).thenReturn(2);
        when(lh.getLedgerMetadata()).thenReturn(metadata);
        for (int count : new int[] {1, 2}) {
            PendingAddOp op = PendingAddOp.create(lh, mockClientContext, List.of(), Unpooled.buffer(16).writeZero(16),
                    WriteFlag.NONE, (rc, handle, id, latency, ctx) -> { }, null);
            for (int i = 0; i < count; i++) {
                op.addEntrySuccessBookies.add(BookieId.parse("current-" + i + ":3181"));
            }
            op.timeoutQuorumWait();
            op.recyclePendAddOpObject();
        }
        verify(counter, times(1)).inc();
        verify(lh, times(2)).handleUnrecoverableErrorDuringAdd(Code.AddEntryQuorumTimeoutException);
    }

    @Test
    public void testUninitiatedAddQueuedAfterEnsembleSwitchUsesCurrentMapping() throws Exception {
        try (EnsembleScenario scenario = new EnsembleScenario(3, false)) {
            PendingAddOp op = PendingAddOp.create(scenario.handle, scenario.context, scenario.original,
                    Unpooled.wrappedBuffer(new byte[16]), WriteFlag.NONE, (rc, handle, id, latency, ctx) -> { }, null);
            op.setEntryId(1);
            op.setLedgerLength(16);
            // The app created the op before the metadata update but has not enqueued it yet.
            Versioned<LedgerMetadata> original = scenario.handle.getVersionedLedgerMetadata();
            assertTrue(scenario.handle.setLedgerMetadata(original, new Versioned<>(
                    LedgerMetadataBuilder.from(original.getValue()).replaceEnsembleEntry(0, scenario.replacement)
                            .build(), new LongVersion(1))));
            scenario.handle.unsetSuccessAndSendWriteRequest(scenario.replacement, Set.of(0, 1));
            scenario.handle.pendingAddOps.add(op);
            op.initiate();
            assertEquals(scenario.replacement, op.ensemble);
            assertEquals(scenario.replacement.get(1), scenario.sends.get(0).bookie);
        }
    }

    private static final class EnsembleScenario implements AutoCloseable {
        final MockClientContext context;
        final LedgerHandle handle;
        final List<BookieId> original = List.of(BookieId.parse("d:3181"), BookieId.parse("a:3181"),
                BookieId.parse("b:3181"), BookieId.parse("c:3181"));
        final List<BookieId> replacement = List.of(BookieId.parse("d-new:3181"), BookieId.parse("a-new:3181"),
                original.get(2), original.get(3));
        final List<Long> successes = new ArrayList<>();
        final List<ControlledSend> sends = new ArrayList<>();
        java.util.function.Consumer<ControlledSend> onSend;

        EnsembleScenario(int writeQuorum, boolean checkDomains) throws Exception {
            context = MockClientContext.create();
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setEnforceMinNumFaultDomainsForWrite(checkDomains);
            context.setConf(ClientInternalConf.fromConfig(configuration));
            doAnswer(invocation -> {
                Set<BookieId> acked = invocation.getArgument(0);
                return acked.contains(original.get(0)) || acked.contains(replacement.get(0));
            }).when(context.getPlacementPolicy()).areAckedBookiesAdheringToPlacementPolicy(any(), anyInt(), anyInt());
            doAnswer(invocation -> {
                ControlledSend send = new ControlledSend(invocation.getArgument(0), invocation.getArgument(3),
                        invocation.getArgument(4), invocation.getArgument(5), invocation.getArgument(6));
                sends.add(send);
                if (onSend != null) {
                    onSend.accept(send);
                }
                return null;
            }).when(context.getBookieClient()).addEntry(any(), anyLong(), any(), anyLong(), any(), any(), any(),
                    anyInt(), anyBoolean(), any());
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(context, 42,
                    LedgerMetadataBuilder.create().withEnsembleSize(4).withWriteQuorumSize(writeQuorum)
                            .withAckQuorumSize(2).newEnsembleEntry(0, original));
            handle = new LedgerHandle(context, 42, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            handle.pendingAddsSequenceHead = 0;
        }

        PendingAddOp add(long entryId) {
            PendingAddOp op = PendingAddOp.create(handle, context, original, Unpooled.wrappedBuffer(new byte[16]),
                    WriteFlag.NONE, (rc, ledger, id, latency, ctx) -> {
                        if (rc == Code.OK) {
                            successes.add(id);
                        }
                    }, null);
            op.setEntryId(entryId);
            op.setLedgerLength(entryId * 16);
            handle.pendingAddOps.add(op);
            op.initiate();
            return op;
        }

        void ack(long entry, int index, BookieId bookie) {
            sends.stream().filter(s -> !s.responded && s.entryId == entry
                            && s.index == index && s.bookie.equals(bookie))
                    .findFirst().orElseThrow().ack();
        }

        @Override
        public void close() {
            onSend = null;
            handle.errorOutPendingAdds(Code.InterruptedException);
            for (ControlledSend send : sends) {
                if (!send.responded) {
                    send.ack();
                }
            }
            if (handle.timeoutFuture != null) {
                handle.timeoutFuture.cancel(false);
            }
            context.getScheduler().shutdown();
        }
    }

    private static final class ControlledSend {
        final BookieId bookie;
        final long entryId;
        final ReferenceCounted data;
        final WriteCallback callback;
        final int index;
        boolean responded;

        ControlledSend(BookieId bookie, long entryId, ReferenceCounted data, WriteCallback callback, int index) {
            this.bookie = bookie;
            this.entryId = entryId;
            this.data = data.retain();
            this.callback = callback;
            this.index = index;
        }

        void ack() {
            assertFalse("each request responds once", responded);
            responded = true;
            try {
                callback.writeComplete(Code.OK, 42, entryId, bookie, index);
            } finally {
                data.release();
            }
        }
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.junit.Test;

public class ProfileByteBufAllocatorTest {
    @Test
    public void clientAllocationSharesBackingAndRetainsDrainUntilLastView() {
        var budget = budget();
        var data = budget.admitClient(ProfileReceiveBudget.Lane.DATA, 512);
        var control = budget.admitClient(ProfileReceiveBudget.Lane.CONTROL, 256);
        assertNull(budget.admitClient(ProfileReceiveBudget.Lane.CONTROL, 256));
        var root = data.allocator().heapBuffer(512, 512);
        var view = root.retainedSlice(0, 16);
        assertEquals(512, data.usedBytes());
        assertThrows(ProfileReceiveBudget.CapacityExceededException.class,
                () -> data.allocator().directBuffer(1, 1));
        var reserved = control.allocator().directBuffer(256, 256);
        assertEquals(256, control.usedBytes());
        root.release();
        data.close();
        data.drained().complete(null); // Caller copies cannot complete actual ownership.
        assertFalse(data.drained().isDone());
        assertThrows(ProfileReceiveBudget.CapacityExceededException.class,
                () -> data.allocator().heapBuffer(1, 1));
        var all = budget.closeAdmissionAndDrain();
        assertFalse(all.isDone());
        assertNull(budget.admitClient(ProfileReceiveBudget.Lane.DATA, 512));
        view.release();
        assertTrue(data.drained().isDone());
        assertEquals(0, data.usedBytes());
        control.close();
        assertFalse(control.drained().isDone());
        reserved.release();
        assertTrue(control.drained().isDone());
        assertTrue(all.isDone());
    }

    private static ProfileReceiveBudget budget() {
        return new ProfileReceiveBudget(1024, 256, 256, 2, 2, 1);
    }

    @Test
    public void retirementWaitsForActualBackingAndKeepsExistingCloseAllocationCharged() throws Exception {
        var budget = budget();
        var permit = budget.admitUnclassified();
        permit.limit(256);
        var allocator = new ProfileByteBufAllocator(permit);
        var backing = allocator.directBuffer(128, 128);
        var view = backing.retainedSlice(0, 64);
        var caller = budget.closeAdmissionAndDrain();
        assertFalse(caller.isDone());
        assertNull(budget.admitUnclassified());
        assertNull(budget.reserve(ProfileReceiveBudget.Lane.DATA, 1));
        caller.complete(null); // A returned copy cannot complete actual ownership.
        assertFalse(budget.closeAdmissionAndDrain().isDone());
        var closeOutput = allocator.heapBuffer(32, 32);
        assertEquals(160, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        permit.close();
        backing.release();
        closeOutput.release();
        assertFalse(budget.closeAdmissionAndDrain().isDone());
        view.release();
        budget.closeAdmissionAndDrain().get(5, java.util.concurrent.TimeUnit.SECONDS);
        assertNull(budget.admitUnclassified());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void heapAndDirectViewsRetainCapacityUntilFinalReleaseAfterDisconnect() {
        for (boolean direct : List.of(false, true)) {
            ProfileReceiveBudget budget = budget();
            ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
            permit.limit(256);
            ProfileByteBufAllocator allocator = new ProfileByteBufAllocator(permit);
            ByteBuf root = direct ? allocator.directBuffer(128, 128) : allocator.heapBuffer(128, 128);
            ByteBuf view = root.retainedDuplicate().asReadOnly().slice(0, 64).order(ByteOrder.LITTLE_ENDIAN);
            try {
                assertEquals(direct, root.isDirect());
                assertTrue(allocator.owns(view));
                assertEquals(128, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                root.release();
                permit.close();
                assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                assertEquals(128, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                assertThrows(IllegalStateException.class, () -> allocator.heapBuffer(1, 1));
                assertThrows(IllegalStateException.class, () -> allocator.directBuffer(1, 1));
            } finally {
                if (root.refCnt() > 1) {
                    root.release();
                }
                view.release();
                permit.close();
            }
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        }
    }

    @Test
    public void resizeReservesOldPlusNewAndRefusalPreservesDataAndIndices() {
        for (boolean direct : List.of(false, true)) {
            ProfileReceiveBudget budget = budget();
            ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
            permit.limit(80);
            ProfileByteBufAllocator allocator = new ProfileByteBufAllocator(permit);
            ByteBuf buffer = direct ? allocator.directBuffer(32, 128) : allocator.heapBuffer(32, 128);
            try {
                buffer.writeLong(123).readerIndex(2);
                assertThrows(IllegalStateException.class, () -> buffer.capacity(64));
                assertEquals(32, buffer.capacity());
                assertEquals(2, buffer.readerIndex());
                assertEquals(8, buffer.writerIndex());
                assertEquals(123, buffer.getLong(0));
                assertEquals(32, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                buffer.capacity(16);
                assertEquals(16, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                buffer.capacity(64); // 16 + 64 fits, including the temporary copy.
                assertEquals(64, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                assertEquals(123, buffer.getLong(0));
                assertThrows(IllegalArgumentException.class, () -> buffer.capacity(129));
                assertEquals(64, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            } finally {
                buffer.release();
                permit.close();
            }
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        }
    }

    @Test
    public void sharedLimitRejectsBeforeAllocationAndReleaseRestoresAdmission() {
        ProfileReceiveBudget budget = budget();
        ProfileReceiveBudget.ConnectionPermit first = budget.admitUnclassified();
        ProfileReceiveBudget.ConnectionPermit second = budget.admitUnclassified();
        first.limit(256);
        second.limit(256);
        ProfileByteBufAllocator a = new ProfileByteBufAllocator(first);
        ProfileByteBufAllocator b = new ProfileByteBufAllocator(second);
        ByteBuf held = a.heapBuffer(200, 200);
        try {
            assertThrows(IllegalStateException.class, () -> b.directBuffer(57, 57));
            assertEquals(200, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            ByteBuf fits = b.directBuffer(56, 56);
            fits.release();
        } finally {
            held.release();
        }
        ByteBuf restored = b.directBuffer(256, 256);
        restored.release();
        first.close();
        second.close();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void transferKeepsExistingReadBackingInItsOriginalLane() {
        ProfileReceiveBudget budget = budget();
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        permit.limit(256);
        ProfileByteBufAllocator allocator = new ProfileByteBufAllocator(permit);
        ProfileReceiveBudget.Credit base = permit.reserve(64);
        ByteBuf read = allocator.directBuffer(32, 64);
        try {
            assertTrue(budget.transfer(ProfileReceiveBudget.Lane.DATA, permit, base));
            assertEquals(32, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            assertEquals(64, budget.used(ProfileReceiveBudget.Lane.DATA));
            read.capacity(48);
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            assertEquals(112, budget.used(ProfileReceiveBudget.Lane.DATA));
        } finally {
            read.release();
            base.close();
            permit.close();
        }
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void responseSplitTransfersExistingReservationToBackingWithoutChargingTwice() {
        ProfileReceiveBudget budget = budget();
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        permit.limit(128);
        ProfileByteBufAllocator allocator = new ProfileByteBufAllocator(permit);
        ProfileReceiveBudget.Credit workspace = permit.reserve(128);
        ProfileReceiveBudget.Credit response = workspace.split(44);
        ByteBuf buffer = allocator.adopt(response, 44);
        try {
            assertEquals(128, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            workspace.close();
            assertEquals(44, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        } finally {
            buffer.release();
            permit.close();
        }
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void deliveredFrameRetainedBodyOutlivesFrameAndChannelWithoutReturningBackingEarly() throws Exception {
        ProfileReceiveBudget budget = budget();
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(budget, permit, 256, 64);
        ProfileByteBufAllocator allocator = new ProfileByteBufAllocator(permit);
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAllocator(allocator);
        channel.pipeline().addLast(decoder);
        byte[] hello = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 1,
                new ProfileHello.Client(List.of()).encode()));
        assertTrue(channel.writeInbound(allocator.buffer(hello.length, hello.length).writeBytes(hello)));
        ProfileInboundFrame frame = channel.readInbound();
        ByteBuf retained = frame.body().retainedDuplicate();
        frame.close();
        assertEquals(64 + hello.length, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        channel.finishAndReleaseAll();
        try {
            assertFalse(channel.isActive());
            assertEquals(hello.length, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        } finally {
            retained.release();
        }
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }
}

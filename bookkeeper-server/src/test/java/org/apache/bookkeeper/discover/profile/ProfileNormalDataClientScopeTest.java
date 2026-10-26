/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.discover.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.junit.Test;

public class ProfileNormalDataClientScopeTest {
    private static final ProfileStatus READ_OK = new ProfileStatus(ProfileStatus.StatusClass.OK,
            ProfileStatus.RetryDisposition.NEVER, ProfileStatus.DurableResult.NONE, 0);

    @Test
    public void pointRepliesReleaseIndependentlyWhenDecoderFramesShareNativeBacking() throws Exception {
        var budget = new ProfileReceiveBudget(1024, 256, 256, 1, 1, 1);
        var allocation = budget.admitClient(ProfileReceiveBudget.Lane.DATA, 256);
        var root = allocation.allocator().directBuffer(256, 256).writeZero(256);
        root.setLong(64, 44).setLong(192, 55);
        var first = new ProfileNormalDataClient.ReadEntryBuffer(new ProfileReadEntryReplyCodec.View(
                READ_OK, ProfileReadEntryReplyCodec.Outcome.FOUND, root.nioBuffer(64, 36)),
                root.slice(0, 128), 64);
        var second = new ProfileNormalDataClient.ReadEntryBuffer(new ProfileReadEntryReplyCodec.View(
                READ_OK, ProfileReadEntryReplyCodec.Outcome.FOUND, root.nioBuffer(192, 36)),
                root.slice(128, 128), 64);
        root.release(); // The decoder no longer owns the shared native allocation.
        allocation.close();
        ByteBuf[] views = new ByteBuf[4];
        try {
            views[0] = first.entry();
            views[1] = views[0].retainedDuplicate();
            views[2] = views[1].readRetainedSlice(8);
            views[3] = second.entry();
            first.released().complete(null);
            first.released().cancel(false);
            assertFalse(first.released().isDone());
            first.close();
            second.close();
            release(views, 0);
            release(views, 1);
            assertFalse(first.released().isDone());
            assertEquals(44, views[2].getLong(0));
            release(views, 2);
            first.released().get(5, TimeUnit.SECONDS);
            assertFalse(second.released().isDone());
            assertEquals(55, views[3].getLong(0));
            assertTrue(root.refCnt() > 0);
            assertEquals(256, allocation.usedBytes());
            assertFalse(allocation.drained().isDone());
            release(views, 3);
            second.released().get(5, TimeUnit.SECONDS);
            allocation.drained().get(5, TimeUnit.SECONDS);
            assertEquals(0, root.refCnt());
            assertEquals(0, allocation.usedBytes());
        } finally {
            first.close();
            second.close();
            for (int index = 0; index < views.length; index++) {
                release(views, index);
            }
            allocation.close();
        }
    }

    @Test
    public void lacRepliesWaitForOwnEntryAndExplicitViewsWhileSiblingBackingStaysCharged() throws Exception {
        var budget = new ProfileReceiveBudget(1024, 256, 256, 1, 1, 1);
        var allocation = budget.admitClient(ProfileReceiveBudget.Lane.DATA, 256);
        var root = allocation.allocator().directBuffer(256, 256).writeZero(256);
        root.setLong(64, 44).setLong(72, 100).setLong(80, 99).setLong(88, 1100);
        root.setLong(100, 44).setLong(108, 101);
        root.setLong(192, 55).setLong(200, 200).setLong(208, 199).setLong(216, 2200);
        root.setLong(228, 55).setLong(236, 201);
        var first = new ProfileNormalDataClient.LacBuffer(new ProfileLacReplyCodec.View(READ_OK,
                ProfileLacReplyCodec.Outcome.SOURCE, root.nioBuffer(64, 36), root.nioBuffer(100, 20), 101),
                root.slice(0, 128), 64);
        var second = new ProfileNormalDataClient.LacBuffer(new ProfileLacReplyCodec.View(READ_OK,
                ProfileLacReplyCodec.Outcome.SOURCE, root.nioBuffer(192, 36), root.nioBuffer(228, 20), 201),
                root.slice(128, 128), 64);
        root.release();
        allocation.close();
        ByteBuf[] views = new ByteBuf[4];
        try {
            views[0] = first.entry();
            views[1] = first.explicitBody();
            views[2] = second.entry();
            var explicit = second.explicitBody();
            try {
                views[3] = explicit.retainedSlice(8, 8);
            } finally {
                explicit.release();
            }
            first.close();
            second.close();
            first.released().complete(null);
            assertFalse(first.released().isDone());
            release(views, 0);
            assertFalse(first.released().isDone());
            assertEquals(101, views[1].getLong(8));
            release(views, 1);
            first.released().get(5, TimeUnit.SECONDS);
            assertFalse(second.released().isDone());
            assertEquals(200, views[2].getLong(8));
            assertEquals(201, views[3].getLong(0));
            assertTrue(root.refCnt() > 0);
            assertEquals(256, allocation.usedBytes());
            release(views, 2);
            assertFalse(second.released().isDone());
            assertFalse(allocation.drained().isDone());
            release(views, 3);
            second.released().get(5, TimeUnit.SECONDS);
            allocation.drained().get(5, TimeUnit.SECONDS);
            assertEquals(0, root.refCnt());
            assertEquals(0, allocation.usedBytes());
        } finally {
            first.close();
            second.close();
            for (int index = 0; index < views.length; index++) {
                release(views, index);
            }
            allocation.close();
        }
    }

    private static void release(ByteBuf[] views, int index) {
        ByteBuf view = views[index];
        views[index] = null;
        if (view != null) {
            view.release();
        }
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentArenaControlLogTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private IsolatedSegmentConfig config() {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("arena"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private SegmentFileHeader identity() {
        byte[] incarnation = new byte[16];
        java.util.Arrays.fill(incarnation, (byte) 7);
        return new SegmentFileHeader(new StorageIncarnation(incarnation), new UUID(0, 9),
                BookieId.parse("isolated:3181"));
    }

    private SegmentArenaControlLog.Allocation alloc(int operation, long offset) {
        return new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                new UUID(0, operation), new Generation(1), 0, 0, 1, new Generation(1), offset, 1);
    }

    @Test
    public void sameBatchCompetitionAppliesOnlyAfterForceAndReplaysTheSameWinner() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            SegmentFileIo io = new SegmentFileIo();
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024, io)) {
                SegmentArenaControlLog.Allocation first = alloc(1, 4096);
                SegmentArenaControlLog.Allocation rival = alloc(2, 4096);
                SegmentArenaControlLog.DurableBatch durable = log.appendDurably(List.of(first, rival));
                assertEquals(1, io.forces);
                assertEquals(1, log.durableThrough());
                assertEquals(-1, log.appliedThrough());
                assertFalse(log.hasAppliedGrant(0, 4096, 1, 1, 1));
                List<SegmentArenaControlLog.Result> results = log.applyDurable(durable);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, results.get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED, results.get(1).status());
                assertEquals(0, results.get(0).appliedThrough());
                assertEquals(1, results.get(1).appliedThrough());
                assertTrue(log.hasAppliedGrant(0, 4096, 1, 1, 1));
                assertFalse(log.hasAppliedGrant(0, 4096, 2, 1, 1));
                assertFalse(log.hasAppliedGrant(0, 4096, 1, 2, 1));
                assertFalse(log.hasAppliedGrant(0, 4097, 1, 1, 1));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(reopened.hasAppliedGrant(0, 4096, 1, 1, 1));
                List<SegmentArenaControlLog.Result> retried = reopened.submitBatch(List.of(alloc(1, 4096),
                        alloc(2, 4096)));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE, retried.get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED, retried.get(1).status());
                assertEquals(SegmentArenaControlLog.Status.INCOMPATIBLE, reopened.submitBatch(List.of(
                        alloc(2, 4096 + SegmentArenaControlLog.BLOCK_BYTES))).get(0).status());
            }
        }
    }

    @Test
    public void durableButNotYetAppliedPoolIsResolvedByRestartReplay() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            SegmentArenaControlLog.Allocation pool = new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 3), new Generation(1), 0,
                    0, 2, new Generation(1), 4096, 2);
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                log.appendDurably(List.of(pool));
                assertFalse(log.hasAppliedGrant(0, 4096, 2, 1, 1));
                assertFalse(log.hasAppliedGrant(0, 4096, 1, 2, 1));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertEquals(0, reopened.durableThrough());
                assertEquals(0, reopened.appliedThrough());
                assertTrue(reopened.hasAppliedGrant(0, 4096, 2, 1, 1));
                assertTrue(reopened.hasAppliedGrant(0, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 2, 1, 1));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        reopened.submitBatch(List.of(pool)).get(0).status());
            }
        }
    }

    @Test
    public void failedForceIsUnknownUntilReplayAndPartialTailRemainsNonWritable() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, 1024 * 1024,
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false))) {
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(4, 4096))));
                assertFalse(log.hasAppliedGrant(0, 4096, 1, 1, 1));
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(5, 4096))));
            }
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(reopened.hasAppliedGrant(0, 4096, 1, 1, 1));
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        reopened.submitBatch(List.of(alloc(4, 4096))).get(0).status());
            }
        }

        IsolatedSegmentConfig torn = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("torn"),
                1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(torn, identity)) {
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(torn, identity, 1024 * 1024,
                    new SegmentFileIo(3, 2, -1, false))) {
                assertThrows(IOException.class, () -> log.submitBatch(List.of(alloc(6, 4096))));
                assertFalse(log.hasAppliedGrant(0, 4096, 1, 1, 1));
            }
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(torn, identity, 1024 * 1024));
        }
    }

    @Test
    public void wrongIdentityAndFullLogRejectWithoutChangingPreviousGrant() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity)) {
            long oneRecordCapacity = 4096 + 176;
            try (SegmentArenaControlLog log = SegmentArenaControlLog.open(config, identity, oneRecordCapacity)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        log.submitBatch(List.of(alloc(7, 4096))).get(0).status());
                assertThrows(IOException.class, () -> log.submitBatch(List.of(
                        alloc(8, 4096 + SegmentArenaControlLog.BLOCK_BYTES))));
                assertTrue(log.hasAppliedGrant(0, 4096, 1, 1, 1));
            }
            byte[] before = Files.readAllBytes(config.directory().resolve("arena-control"));
            SegmentFileHeader wrong = new SegmentFileHeader(identity.incarnation(), new UUID(0, 10),
                    identity.bookieId());
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, wrong, oneRecordCapacity));
            assertEquals(java.util.Arrays.hashCode(before),
                    java.util.Arrays.hashCode(Files.readAllBytes(config.directory().resolve("arena-control"))));
            try (SegmentArenaControlLog reopened = SegmentArenaControlLog.open(config, identity,
                    oneRecordCapacity)) {
                assertTrue(reopened.hasAppliedGrant(0, 4096, 1, 1, 1));
            }
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                data.write(ByteBuffer.wrap(new byte[] {0}), 0);
                data.force(true);
            }
            assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity,
                    oneRecordCapacity));
        }
    }
}

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.junit.Test;

public class ProfileDeleteHandoffTest {
    private static byte[] bytes(int count, int value) {
        byte[] result = new byte[count];
        Arrays.fill(result, (byte) value);
        return result;
    }

    @Test
    public void preparedPlanBindsExactCutNextGenerationAndSnapshotWithCanonicalBounds() {
        var target = new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2));
        var cut = new ProfileDeleteDelivery.Head(bytes(16, 3), 1, 1, 4, target);
        var plan = new ProfileDeleteHandoff.Plan(bytes(16, 4), cut, 100, 2, 1, 200, bytes(32, 5));
        assertArrayEquals(plan.encode(), ProfileDeleteHandoff.Plan.decode(plan.encode()).encode());
        byte[] owned = plan.operationId();
        owned[0] ^= 1;
        assertArrayEquals(bytes(16, 4), plan.operationId());
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteHandoff.Plan(bytes(16, 4), cut, 100, 3, 1, 200, bytes(32, 5)));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteHandoff.Plan(bytes(16, 4), cut, 100, 2, 0, 0, new byte[32]));
        var empty = new ProfileDeleteDelivery.Head(bytes(16, 3), 1, 1, 0, target);
        var emptyPlan = new ProfileDeleteHandoff.Plan(bytes(16, 4), empty, 100, 2, 0, 0, new byte[32]);
        assertArrayEquals(emptyPlan.encode(), ProfileDeleteHandoff.Plan.decode(emptyPlan.encode()).encode());
        for (int size = 0; size < plan.encode().length; size++) {
            byte[] cutBytes = Arrays.copyOf(plan.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Plan.decode(cutBytes));
        }
        byte[] huge = plan.encode();
        ByteBuffer.wrap(huge).putInt(88, Integer.MAX_VALUE);
        CRC32C crc = new CRC32C();
        crc.update(huge, 0, huge.length - 4);
        ByteBuffer.wrap(huge).putInt(huge.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Plan.decode(huge));
        var active = new ProfileDeleteHandoff.Admission(1, ProfileDeleteHandoff.scopeDigest(cut), new byte[32]);
        assertFalse(active.frozen());
        var pending = new ProfileDeleteHandoff.Admission(1, ProfileDeleteHandoff.scopeDigest(cut),
                ProfileRepairIntent.digest(plan.encode()));
        assertTrue(ProfileDeleteHandoff.Admission.decode(pending.encode()).frozen());
    }
    @Test
    public void acknowledgementRetainsCanonicalPlanControlSequenceAndImmutableBirth() {
        var target = new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2));
        var cut = new ProfileDeleteDelivery.Head(bytes(16, 3), 1, 1, 0, target);
        var plan = new ProfileDeleteHandoff.Plan(bytes(16, 4), cut, 100, 2, 0, 0, new byte[32]);
        var ack = ProfileDeleteHandoff.Acknowledgement.recorded(plan, 0);
        var recovered = ProfileDeleteHandoff.Acknowledgement.decode(ack.encode());
        assertArrayEquals(ack.encode(), recovered.encode());
        assertEquals(0, recovered.controlSequence());
        assertTrue(recovered.matches(plan));
        byte[] birth = recovered.birthId();
        birth[0] ^= 1;
        assertFalse(Arrays.equals(birth, recovered.birthId()));
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Acknowledgement.recorded(plan, -1));
        for (int size = 0; size < ack.encode().length; size++) {
            byte[] truncated = Arrays.copyOf(ack.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Acknowledgement.decode(truncated));
        }
        byte[] oversized = ack.encode();
        ByteBuffer.wrap(oversized).putInt(32, Integer.MAX_VALUE);
        CRC32C crc = new CRC32C();
        crc.update(oversized, 0, oversized.length - 4);
        ByteBuffer.wrap(oversized).putInt(oversized.length - 4, (int) crc.getValue());
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Acknowledgement.decode(oversized));
    }

    @Test
    public void effectiveWitnessRetainsOriginalAckAndEmptyBaseWithoutInventingAnEvent() {
        var target = new ProfileDeleteRecord.Target("b1:3181", bytes(16, 2));
        var predecessor = new ProfileDeleteDelivery.Head(bytes(16, 3), 1, 1, 0, target);
        var plan = new ProfileDeleteHandoff.Plan(bytes(16, 4), predecessor, 100, 2, 0, 0, new byte[32]);
        var ack = ProfileDeleteHandoff.Acknowledgement.recorded(plan, 0);
        var effective = ProfileDeleteHandoff.Effective.recorded(ack);
        var decoded = ProfileDeleteHandoff.Effective.decode(effective.encode());
        assertArrayEquals(effective.encode(), decoded.encode());
        assertArrayEquals(ack.encode(), decoded.acknowledgement().encode());
        assertEquals(2, decoded.head().assignmentGeneration());
        assertEquals(0, decoded.head().committedThrough());
        var progress = new ProfileDeleteDelivery.Progress(predecessor.streamId(), 1, 2, 0);
        assertTrue(progress.matches(decoded.head()));
        assertFalse(progress.matches(predecessor));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteDelivery.Head(bytes(16, 3), 1, 65, 0, target));
        for (int size = 0; size < effective.encode().length; size++) {
            byte[] truncated = Arrays.copyOf(effective.encode(), size);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteHandoff.Effective.decode(truncated));
        }
    }

}

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
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.UUID;
import org.junit.Test;

public class ProfileDeleteRetentionTest {
    @Test
    public void boundedPolicyAndReceiptPreserveAllWindowsAndExactPublication() {
        var policy = new ProfileDeleteRetention.Policy(2, UUID.randomUUID(), 100, 20, 300);
        assertEquals(420, policy.totalWindowMillis());
        assertEquals(policy, ProfileDeleteRetention.Policy.decode(policy.encode()));
        byte[] digest = new byte[32];
        byte[] publication = new byte[16];
        Arrays.fill(digest, (byte) 7);
        Arrays.fill(publication, (byte) 8);
        var reference = new ProfileDeleteRecord.Reference(10, digest, digest, publication);
        var receipt = new ProfileDeleteRetention.Receipt(UUID.randomUUID(), policy, 12, reference);
        assertArrayEquals(receipt.encode(), ProfileDeleteRetention.Receipt.decode(receipt.encode()).encode());
        var barrier = new ProfileDeleteRetention.Barrier(UUID.randomUUID(), receipt, digest, 20);
        assertArrayEquals(barrier.encode(), ProfileDeleteRetention.Barrier.decode(barrier.encode()).encode());
        byte[] barrierDigest = barrier.terminalDigest();
        barrierDigest[0] ^= 1;
        assertArrayEquals(digest, barrier.terminalDigest());
        for (int length = 0; length < barrier.encode().length; length++) {
            byte[] cut = Arrays.copyOf(barrier.encode(), length);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRetention.Barrier.decode(cut));
        }
        byte[] damagedBarrier = barrier.encode();
        damagedBarrier[250] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRetention.Barrier.decode(damagedBarrier));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileDeleteRetention.Barrier(UUID.randomUUID(), receipt, digest, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Receipt(UUID.randomUUID(),
                policy, 12, new ProfileDeleteRecord.Reference(10, digest, digest)));
        for (int length = 0; length < policy.encode().length; length++) {
            byte[] cut = Arrays.copyOf(policy.encode(), length);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRetention.Policy.decode(cut));
        }
        for (int length = 0; length < receipt.encode().length; length++) {
            byte[] cut = Arrays.copyOf(receipt.encode(), length);
            assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRetention.Receipt.decode(cut));
        }
        byte[] damaged = receipt.encode();
        damaged[30] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ProfileDeleteRetention.Receipt.decode(damaged));
    }

    @Test
    public void policyRejectsImplicitZeroWindowsAndOverflow() {
        UUID revision = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Policy(0, revision, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Policy(1, revision, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Policy(1, revision, 1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Policy(1, revision, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProfileDeleteRetention.Policy(1, revision,
                Long.MAX_VALUE, 1, 1));
    }
}

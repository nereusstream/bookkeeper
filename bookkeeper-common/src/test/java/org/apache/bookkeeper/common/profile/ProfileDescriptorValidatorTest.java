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

package org.apache.bookkeeper.common.profile;

import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.CAPABILITY_COUNT_OUT_OF_RANGE;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.DUPLICATE_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_LOSS_BUDGET;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_POLICY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_QUORUM;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ProfileDescriptorValidatorTest {

    private final ProfileDescriptorValidator validator = TestProfileRegistries.validator();

    @Test
    public void acceptsUnsignedAndQuorumBoundaries() {
        validator.validate(new ProfileDescriptor(
                EngineProfile.DIRECT_JOURNAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.DEFERRED_SYNC_LEGACY,
                0xffff,
                0xffff,
                0xffff,
                0xfffe,
                TestProfileRegistries.TEST_POLICY_ID_UNSIGNED_MAX,
                -1L,
                TestProfileRegistries.capabilities(64)));
    }

    @Test
    public void rejectsInvalidQuorums() {
        assertReason(INVALID_QUORUM, descriptor(0, 1, 1, 0));
        assertReason(INVALID_QUORUM, descriptor(3, 4, 1, 0));
        assertReason(INVALID_QUORUM, descriptor(3, 2, 3, 0));
        assertReason(INVALID_QUORUM, descriptor(0x1_0000, 1, 1, 0));
    }

    @Test
    public void rejectsInvalidPermanentLossBudget() {
        assertReason(INVALID_LOSS_BUDGET, descriptor(3, 3, 2, -1));
        assertReason(INVALID_LOSS_BUDGET, descriptor(3, 3, 2, 2));
    }

    @Test
    public void rejectsInvalidOrUnknownPolicies() {
        assertReason(INVALID_POLICY, descriptorWithPolicy(0, 1));
        assertReason(INVALID_POLICY, descriptorWithPolicy(TestProfileRegistries.TEST_POLICY_ID, 0));
        assertReason(UNKNOWN_POLICY, descriptorWithPolicy(1, 1));
    }

    @Test
    public void rejectsInvalidCapabilitySets() {
        assertReason(
                CAPABILITY_COUNT_OUT_OF_RANGE,
                TestProfileRegistries.descriptor(TestProfileRegistries.capabilities(65)));

        ProfileCapability first = TestProfileRegistries.capability(1);
        assertReason(DUPLICATE_CAPABILITY, TestProfileRegistries.descriptor(List.of(first, first)));
        assertReason(
                OUT_OF_ORDER_CAPABILITY,
                TestProfileRegistries.descriptor(List.of(
                        TestProfileRegistries.capability(2), TestProfileRegistries.capability(1))));
        assertReason(
                UNKNOWN_CAPABILITY,
                TestProfileRegistries.descriptor(List.of(new ProfileCapability(1, 1))));
        assertReason(
                UNKNOWN_CAPABILITY,
                TestProfileRegistries.descriptor(List.of(
                        new ProfileCapability(TestProfileRegistries.TEST_CAPABILITY_BASE + 1, 2))));
    }

    private void assertReason(
            ProfileDescriptorValidationException.Reason expectedReason,
            ProfileDescriptor descriptor) {
        ProfileDescriptorValidationException exception = assertThrows(
                ProfileDescriptorValidationException.class, () -> validator.validate(descriptor));
        assertEquals(expectedReason, exception.reason());
    }

    private static ProfileDescriptor descriptor(int ensemble, int writeQuorum, int ackQuorum, int lossBudget) {
        return new ProfileDescriptor(
                EngineProfile.CLASSIC_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                ensemble,
                writeQuorum,
                ackQuorum,
                lossBudget,
                TestProfileRegistries.TEST_POLICY_ID,
                1,
                Collections.emptyList());
    }

    private static ProfileDescriptor descriptorWithPolicy(long policyId, long generation) {
        return new ProfileDescriptor(
                EngineProfile.CLASSIC_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                1,
                1,
                1,
                0,
                policyId,
                generation,
                Collections.emptyList());
    }
}

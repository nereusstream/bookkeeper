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

import java.util.ArrayList;
import java.util.List;

/** Test-only identifiers. These values are not a production registry allocation. */
final class TestProfileRegistries {

    static final long TEST_CAPABILITY_BASE = 0xff00_0000L;
    static final long TEST_CAPABILITY_UNSIGNED_MAX = 0xffff_ffffL;
    static final long TEST_POLICY_ID = 0xfffe_0001L;
    static final long TEST_POLICY_ID_UNSIGNED_MAX = 0xffff_ffffL;

    private TestProfileRegistries() {}

    static ProfileDescriptorValidator validator() {
        return new ProfileDescriptorValidator(
                TestProfileRegistries::validatorCapability, TestProfileRegistries::validatorPolicy);
    }

    static boolean validatorCapability(long capabilityId, int semanticVersion) {
        return (capabilityId > TEST_CAPABILITY_BASE
                        && capabilityId <= TEST_CAPABILITY_BASE + ProfileDescriptorValidator.MAX_CAPABILITIES
                        && semanticVersion == 1)
                || (capabilityId == TEST_CAPABILITY_UNSIGNED_MAX && semanticVersion == 0xffff);
    }

    static boolean validatorPolicy(long policyId, long generation) {
        return (policyId == TEST_POLICY_ID || policyId == TEST_POLICY_ID_UNSIGNED_MAX)
                && generation != 0;
    }

    static ProfileCapability capability(int ordinal) {
        return new ProfileCapability(TEST_CAPABILITY_BASE + ordinal, 1);
    }

    static List<ProfileCapability> capabilities(int count) {
        List<ProfileCapability> capabilities = new ArrayList<>(count);
        for (int ordinal = 1; ordinal <= count; ordinal++) {
            capabilities.add(capability(ordinal));
        }
        return capabilities;
    }

    static ProfileDescriptor descriptor(List<ProfileCapability> capabilities) {
        return new ProfileDescriptor(
                EngineProfile.SEGMENT_WAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                3,
                3,
                2,
                1,
                TEST_POLICY_ID,
                1,
                capabilities);
    }
}

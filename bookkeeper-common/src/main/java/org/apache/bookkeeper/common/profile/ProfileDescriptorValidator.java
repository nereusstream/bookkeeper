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
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_LOSS_BUDGET;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_POLICY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_QUORUM;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NULL_INPUT;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_POLICY;

import java.util.List;
import java.util.Objects;

/** Strict semantic validator for the frozen Profile descriptor schema. */
public final class ProfileDescriptorValidator {

    public static final int MAX_CAPABILITIES = 64;
    private static final int MAX_U16 = 0xffff;
    private static final long MAX_U32 = 0xffff_ffffL;

    private final ProfileCapabilityRegistry capabilityRegistry;
    private final FailureDomainPolicyRegistry failureDomainPolicyRegistry;

    public ProfileDescriptorValidator(
            ProfileCapabilityRegistry capabilityRegistry,
            FailureDomainPolicyRegistry failureDomainPolicyRegistry) {
        this.capabilityRegistry = Objects.requireNonNull(capabilityRegistry, "capabilityRegistry");
        this.failureDomainPolicyRegistry =
                Objects.requireNonNull(failureDomainPolicyRegistry, "failureDomainPolicyRegistry");
    }

    /** Validates intrinsic bounds and the exact test or future production registries. */
    public void validate(ProfileDescriptor descriptor) {
        if (descriptor == null) {
            throw failure(NULL_INPUT);
        }

        int ensembleSize = descriptor.ensembleSize();
        int writeQuorumSize = descriptor.writeQuorumSize();
        int ackQuorumSize = descriptor.ackQuorumSize();
        if (ensembleSize < 1
                || ensembleSize > MAX_U16
                || writeQuorumSize < 1
                || writeQuorumSize > ensembleSize
                || ackQuorumSize < 1
                || ackQuorumSize > writeQuorumSize) {
            throw failure(INVALID_QUORUM);
        }
        if (descriptor.permanentLossBudget() < 0
                || descriptor.permanentLossBudget() >= ackQuorumSize) {
            throw failure(INVALID_LOSS_BUDGET);
        }

        long policyId = descriptor.failureDomainPolicyId();
        long policyGeneration = descriptor.failureDomainPolicyGeneration();
        if (policyId <= 0 || policyId > MAX_U32 || policyGeneration == 0) {
            throw failure(INVALID_POLICY);
        }
        if (!failureDomainPolicyRegistry.contains(policyId, policyGeneration)) {
            throw failure(UNKNOWN_POLICY);
        }

        List<ProfileCapability> capabilities = descriptor.mandatoryCapabilities();
        if (capabilities.size() > MAX_CAPABILITIES) {
            throw failure(CAPABILITY_COUNT_OUT_OF_RANGE);
        }
        long previousId = 0;
        for (ProfileCapability capability : capabilities) {
            if (capability == null
                    || capability.capabilityId() <= 0
                    || capability.capabilityId() > MAX_U32
                    || capability.semanticVersion() <= 0
                    || capability.semanticVersion() > MAX_U16) {
                throw failure(INVALID_CAPABILITY);
            }
            if (capability.capabilityId() == previousId) {
                throw failure(DUPLICATE_CAPABILITY);
            }
            if (capability.capabilityId() < previousId) {
                throw failure(OUT_OF_ORDER_CAPABILITY);
            }
            if (!capabilityRegistry.contains(capability.capabilityId(), capability.semanticVersion())) {
                throw failure(UNKNOWN_CAPABILITY);
            }
            previousId = capability.capabilityId();
        }
    }

    private static ProfileDescriptorValidationException failure(
            ProfileDescriptorValidationException.Reason reason) {
        return new ProfileDescriptorValidationException(reason);
    }
}

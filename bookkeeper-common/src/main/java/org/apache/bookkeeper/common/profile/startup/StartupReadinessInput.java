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

package org.apache.bookkeeper.common.profile.startup;

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_VERSION;

import java.util.List;

/** Complete immutable input to one startup/readiness reference run. */
public record StartupReadinessInput(
        BookieCompatibilityFact compatibility,
        RequiredDeviceManifest requiredDevices,
        LocalRecoveryFacts recoveryFacts,
        NewScopeFallbackManifest newScopeFallback,
        RollbackSafetyFacts rollbackSafetyFacts,
        Generation targetReadinessGeneration,
        Generation capabilityGeneration,
        int runtimeReaderVersion,
        int runtimeWriterVersion,
        List<Integer> supportedMandatoryFeatures,
        String profileEndpoint,
        List<Integer> capabilityHints) {

    public StartupReadinessInput {
        compatibility = StartupFactSupport.requireNonNull(compatibility);
        requiredDevices = StartupFactSupport.requireNonNull(requiredDevices);
        recoveryFacts = StartupFactSupport.requireNonNull(recoveryFacts);
        newScopeFallback = StartupFactSupport.requireNonNull(newScopeFallback);
        rollbackSafetyFacts = StartupFactSupport.requireNonNull(rollbackSafetyFacts);
        targetReadinessGeneration = StartupFactSupport.requireNonNull(targetReadinessGeneration);
        capabilityGeneration = StartupFactSupport.requireNonNull(capabilityGeneration);
        if (runtimeReaderVersion <= 0 || runtimeWriterVersion <= 0) {
            throw new StartupReadinessValidationException(INVALID_VERSION);
        }
        supportedMandatoryFeatures = StartupFactSupport.strictlyIncreasingPositiveIds(
                supportedMandatoryFeatures, false);
        profileEndpoint = StartupFactSupport.boundedText(profileEndpoint, 255);
        capabilityHints = StartupFactSupport.strictlyIncreasingPositiveIds(capabilityHints, false);
    }
}

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

import java.util.List;

/** Secret-free result of one fail-closed startup reference run. */
public record StartupReadinessResult(
        Outcome outcome,
        Reason reason,
        List<StartupPhase> completedPhases,
        StartupReadinessMetrics.Snapshot metrics) {

    public enum Outcome {
        WRITABLE,
        NON_WRITABLE,
        RETRY_REQUIRED
    }

    public enum Reason {
        COMPLETE,
        COMPATIBILITY_REJECTED,
        UNSAFE_NEW_SCOPE,
        RUNTIME_VERSION_REJECTED,
        DEVICE_MANIFEST_MISMATCH,
        DEVICE_NOT_READY,
        RECOVERY_NOT_READY,
        LOCAL_READINESS_NOT_DURABLE,
        LOCAL_READINESS_CONFLICT,
        PERSISTENT_READINESS_UNAVAILABLE,
        PERSISTENT_READINESS_CONFLICT,
        STALE_EPHEMERAL_REGISTRATION,
        SERVICE_INFO_NOT_MATCHING,
        EPHEMERAL_REGISTRATION_FAILED,
        FINAL_RECONCILIATION_MISMATCH
    }

    public enum StartupPhase {
        COMPATIBILITY_FACT,
        REQUIRED_DEVICE_FACTS,
        ALLOCATOR_RECOVERY,
        ROUTE_RECOVERY,
        DELETE_RECOVERY,
        DURABLE_LOCAL_READINESS,
        PERSISTENT_READINESS,
        MATCHING_SERVICE_INFO,
        EPHEMERAL_WRITABLE_REGISTRATION
    }

    public StartupReadinessResult {
        outcome = StartupFactSupport.requireNonNull(outcome);
        reason = StartupFactSupport.requireNonNull(reason);
        completedPhases = List.copyOf(completedPhases);
        metrics = StartupFactSupport.requireNonNull(metrics);
    }
}

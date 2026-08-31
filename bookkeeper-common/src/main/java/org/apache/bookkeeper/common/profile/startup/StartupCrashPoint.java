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

import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.StartupPhase;

/** Deterministic crash boundary for reference-only restart tests. */
public enum StartupCrashPoint {
    NONE(null),
    AFTER_COMPATIBILITY(StartupPhase.COMPATIBILITY_FACT),
    AFTER_REQUIRED_DEVICES(StartupPhase.REQUIRED_DEVICE_FACTS),
    AFTER_ALLOCATOR_RECOVERY(StartupPhase.ALLOCATOR_RECOVERY),
    AFTER_ROUTE_RECOVERY(StartupPhase.ROUTE_RECOVERY),
    AFTER_DELETE_RECOVERY(StartupPhase.DELETE_RECOVERY),
    AFTER_DURABLE_LOCAL_READINESS(StartupPhase.DURABLE_LOCAL_READINESS),
    AFTER_PERSISTENT_READINESS(StartupPhase.PERSISTENT_READINESS),
    AFTER_MATCHING_SERVICE_INFO(StartupPhase.MATCHING_SERVICE_INFO),
    AFTER_EPHEMERAL_WRITABLE_REGISTRATION(StartupPhase.EPHEMERAL_WRITABLE_REGISTRATION);

    private final StartupPhase phase;

    StartupCrashPoint(StartupPhase phase) {
        this.phase = phase;
    }

    boolean matches(StartupPhase completedPhase) {
        return phase == completedPhase;
    }
}

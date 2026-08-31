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
import org.apache.bookkeeper.common.profile.startup.StartupReadinessResult.StartupPhase;

/** Controlled exception that preserves exact completed phases and metrics at a simulated crash cut. */
public final class SimulatedStartupCrash extends RuntimeException {

    private final List<StartupPhase> completedPhases;
    private final StartupReadinessMetrics.Snapshot metrics;

    SimulatedStartupCrash(
            List<StartupPhase> completedPhases, StartupReadinessMetrics.Snapshot metrics) {
        super("simulated crash after " + completedPhases.get(completedPhases.size() - 1));
        this.completedPhases = List.copyOf(completedPhases);
        this.metrics = metrics;
    }

    public List<StartupPhase> completedPhases() {
        return completedPhases;
    }

    public StartupReadinessMetrics.Snapshot metrics() {
        return metrics;
    }
}

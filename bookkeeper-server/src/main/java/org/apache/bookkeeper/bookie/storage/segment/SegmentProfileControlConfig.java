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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProfileStaticOperationAuthorizer;

/** Borrowed cold resources for explicit experimental INSTALL/ACTIVATE socket opt-in. */
public record SegmentProfileControlConfig(ProfileDescriptorValidator validator,
        ProfileStaticOperationAuthorizer staticAuthorizer, ProfileControlStore controlStore,
        ProfileControlAuthorizer exactAuthorizer, ThreadPoolExecutor executor, Duration timeout) {
    public SegmentProfileControlConfig {
        Objects.requireNonNull(validator, "validator");
        Objects.requireNonNull(staticAuthorizer, "staticAuthorizer");
        Objects.requireNonNull(controlStore, "controlStore");
        Objects.requireNonNull(exactAuthorizer, "exactAuthorizer");
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(timeout, "timeout");
        if (executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Initial control requires a bounded cold executor and wait");
        }
        // Caller must preserve bounds and drain accepted tasks before shutdown.
        // CallerRunsPolicy may never block the event loop.
        if (!(executor.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException("Initial control requires rejecting cold admission");
        }
    }
}

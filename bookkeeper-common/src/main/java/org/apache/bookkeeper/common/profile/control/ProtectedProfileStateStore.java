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

package org.apache.bookkeeper.common.profile.control;

import java.util.Objects;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;

/** Semantic local transition boundary; physical owner, record framing, and group commit remain blocked. */
public interface ProtectedProfileStateStore {

    record Transition(
            ProfileControlRequest request,
            CommittedProfileAuthority authority,
            ProfileDescriptor decodedDescriptor) {

        public Transition {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(authority, "authority");
        }
    }

    record TransitionResult(ProfileControlResult.Outcome outcome, long localGeneration, boolean durable) {

        public TransitionResult {
            Objects.requireNonNull(outcome, "outcome");
            if (localGeneration < 0) {
                throw new IllegalArgumentException("localGeneration must be non-negative");
            }
        }
    }

    TransitionResult conditionalApply(Transition transition);

    TransitionResult queryOperationResult(ProfileControlScope scope);
}

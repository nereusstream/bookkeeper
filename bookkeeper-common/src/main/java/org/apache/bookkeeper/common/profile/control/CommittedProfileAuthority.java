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
import org.apache.bookkeeper.common.profile.EngineProfile;

/** Strictly parsed committed authority returned by the cold direct-read interface. */
public record CommittedProfileAuthority(
        ProfileControlScope scope,
        State state,
        EngineProfile requiredEngine,
        long opaqueStoreVersion,
        boolean complete) {

    public enum State {
        PREPARING,
        READY_INITIAL,
        POST_MEMBERSHIP_REPLACEMENT,
        RECOVERY_OPEN,
        RECOVERY_CLOSE_AUTHORIZED,
        DELETE_AUTHORIZED,
        STATUS_QUERYABLE
    }

    public CommittedProfileAuthority {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(requiredEngine, "requiredEngine");
        if (opaqueStoreVersion < 0) {
            throw new IllegalArgumentException("opaqueStoreVersion must be non-negative");
        }
    }

    public boolean permits(ProfileControlOperation operation) {
        return complete
                && switch (operation) {
                    case INSTALL -> state == State.PREPARING;
                    case ACTIVATE_INITIAL -> state == State.READY_INITIAL;
                    case ACTIVATE_REPLACEMENT -> state == State.POST_MEMBERSHIP_REPLACEMENT;
                    case RECOVERY_GRANT -> state == State.RECOVERY_OPEN;
                    case RECOVERY_GRANT_CLOSE -> state == State.RECOVERY_CLOSE_AUTHORIZED;
                    case TOMBSTONE_OR_DELETE_APPLY -> state == State.DELETE_AUTHORIZED;
                    case STATUS_QUERY -> state == State.STATUS_QUERYABLE;
                };
    }
}

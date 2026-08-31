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

import java.util.EnumSet;
import java.util.Set;

/** Authority-presence facts used only to refuse unsafe old-binary same-scope rollback. */
public record RollbackSafetyFacts(
        boolean localSuccess,
        boolean routeOrInstall,
        boolean activation,
        boolean fenceOrGrant,
        boolean tombstone,
        boolean arenaAuthority,
        boolean durabilityUnknown) {

    public enum RecoveryAction {
        ROLL_FORWARD,
        VERIFIED_EXPORT_OR_REBUILD,
        IRREVERSIBLE_WIPE_OR_DECOMMISSION,
        NEW_INCARNATION_REJOIN
    }

    public boolean refusesOldBinarySameScopeRollback() {
        return localSuccess
                || routeOrInstall
                || activation
                || fenceOrGrant
                || tombstone
                || arenaAuthority
                || durabilityUnknown;
    }

    public Set<RecoveryAction> allowedRecoveryActions() {
        return Set.copyOf(EnumSet.allOf(RecoveryAction.class));
    }
}

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

/** Frozen logical operation classes for the experimental control boundary. */
public enum ProfileControlOperation {
    INSTALL(ProfileAuthorityPurpose.PREPARING_INSTALL, false),
    ACTIVATE_INITIAL(ProfileAuthorityPurpose.READY_INITIAL, false),
    ACTIVATE_REPLACEMENT(ProfileAuthorityPurpose.POST_MEMBERSHIP_REPLACEMENT, false),
    RECOVERY_GRANT(ProfileAuthorityPurpose.RECOVERY_GRANT, true),
    RECOVERY_GRANT_CLOSE(ProfileAuthorityPurpose.RECOVERY_GRANT_CLOSE, true),
    TOMBSTONE_OR_DELETE_APPLY(ProfileAuthorityPurpose.TOMBSTONE_OR_DELETE, false),
    STATUS_QUERY(ProfileAuthorityPurpose.STATUS_QUERY, false);

    private final ProfileAuthorityPurpose purpose;
    private final boolean rangeAllowed;

    ProfileControlOperation(ProfileAuthorityPurpose purpose, boolean rangeAllowed) {
        this.purpose = purpose;
        this.rangeAllowed = rangeAllowed;
    }

    public ProfileAuthorityPurpose purpose() {
        return purpose;
    }

    public boolean rangeAllowed() {
        return rangeAllowed;
    }
}

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

import java.util.Arrays;
import java.util.Objects;

/** Fixed domain key derived by the endpoint rather than accepted as a caller-supplied path. */
public final class ProfileAuthorityKey {

    private final long ledgerId;
    private final byte[] ledgerInstanceId;
    private final ProfileAuthorityPurpose purpose;
    private final String authorityDomain;

    private ProfileAuthorityKey(
            long ledgerId, byte[] ledgerInstanceId, ProfileAuthorityPurpose purpose, String authorityDomain) {
        this.ledgerId = ledgerId;
        this.ledgerInstanceId = ledgerInstanceId.clone();
        this.purpose = purpose;
        this.authorityDomain = authorityDomain;
    }

    public static ProfileAuthorityKey fromScope(ProfileControlScope scope) {
        Objects.requireNonNull(scope, "scope");
        return new ProfileAuthorityKey(
                scope.ledgerId(), scope.ledgerInstanceId(), scope.purpose(), scope.authorityDomain());
    }

    public long ledgerId() {
        return ledgerId;
    }

    public byte[] ledgerInstanceId() {
        return ledgerInstanceId.clone();
    }

    public ProfileAuthorityPurpose purpose() {
        return purpose;
    }

    public String authorityDomain() {
        return authorityDomain;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProfileAuthorityKey that)) {
            return false;
        }
        return ledgerId == that.ledgerId
                && Arrays.equals(ledgerInstanceId, that.ledgerInstanceId)
                && purpose == that.purpose
                && authorityDomain.equals(that.authorityDomain);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ledgerId, purpose, authorityDomain);
        result = 31 * result + Arrays.hashCode(ledgerInstanceId);
        return result;
    }

    @Override
    public String toString() {
        return "ProfileAuthorityKey{ledgerId=" + ledgerId
                + ", purpose=" + purpose
                + ", authorityDomain='" + authorityDomain + "'}";
    }
}

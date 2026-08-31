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
import java.util.Optional;

/** Cold direct committed-authority read; cache/watch correctness is outside this interface. */
@FunctionalInterface
public interface ProfileControlStore {

    enum ReadStatus {
        FOUND,
        NOT_FOUND,
        UNAVAILABLE,
        QUARANTINED
    }

    record ReadResult(ReadStatus status, CommittedProfileAuthority authority) {

        public ReadResult {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (authority != null)) {
                throw new IllegalArgumentException("FOUND must carry exactly one committed authority");
            }
        }

        public static ReadResult found(CommittedProfileAuthority authority) {
            return new ReadResult(ReadStatus.FOUND, Objects.requireNonNull(authority, "authority"));
        }

        public static ReadResult notFound() {
            return new ReadResult(ReadStatus.NOT_FOUND, null);
        }

        public static ReadResult unavailable() {
            return new ReadResult(ReadStatus.UNAVAILABLE, null);
        }

        public static ReadResult quarantined() {
            return new ReadResult(ReadStatus.QUARANTINED, null);
        }

        public Optional<CommittedProfileAuthority> committedAuthority() {
            return Optional.ofNullable(authority);
        }
    }

    ReadResult read(ProfileAuthorityKey authorityKey);
}

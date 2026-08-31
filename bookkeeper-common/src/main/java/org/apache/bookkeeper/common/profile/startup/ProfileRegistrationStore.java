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

import java.util.Optional;

/** Domain-specific persistent versioned readiness CAS semantic interface. */
public interface ProfileRegistrationStore {

    long NO_STORE_VERSION = -1;
    long NO_READINESS_GENERATION = 0;

    ReadResult read(String bookieId);

    CasResult compareAndSet(
            String bookieId,
            long expectedStoreVersion,
            long expectedReadinessGeneration,
            ProfileReadinessRecord newRecord);

    enum ReadStatus {
        FOUND,
        NOT_FOUND,
        UNAVAILABLE,
        CORRUPT
    }

    enum CasStatus {
        APPLIED,
        CONFLICT,
        RESPONSE_LOST,
        UNAVAILABLE
    }

    record VersionedRecord(long storeVersion, ProfileReadinessRecord record) {
        public VersionedRecord {
            if (storeVersion < 0) {
                throw new IllegalArgumentException("storeVersion must be non-negative");
            }
            record = StartupFactSupport.requireNonNull(record);
        }
    }

    record ReadResult(ReadStatus status, Optional<VersionedRecord> versionedRecord) {
        public ReadResult {
            status = StartupFactSupport.requireNonNull(status);
            versionedRecord = StartupFactSupport.requireNonNull(versionedRecord);
            if ((status == ReadStatus.FOUND) != versionedRecord.isPresent()) {
                throw new IllegalArgumentException("FOUND must carry exactly one versioned record");
            }
        }

        public static ReadResult found(VersionedRecord record) {
            return new ReadResult(ReadStatus.FOUND, Optional.of(record));
        }

        public static ReadResult notFound() {
            return new ReadResult(ReadStatus.NOT_FOUND, Optional.empty());
        }

        public static ReadResult unavailable() {
            return new ReadResult(ReadStatus.UNAVAILABLE, Optional.empty());
        }

        public static ReadResult corrupt() {
            return new ReadResult(ReadStatus.CORRUPT, Optional.empty());
        }
    }

    record CasResult(CasStatus status) {
        public CasResult {
            status = StartupFactSupport.requireNonNull(status);
        }
    }
}

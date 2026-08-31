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

import java.util.HashMap;
import java.util.Map;

/** Deterministic in-memory reference adapter for persistent readiness CAS semantics. */
public final class InMemoryProfileRegistrationStore implements ProfileRegistrationStore {

    public enum CasFault {
        NONE,
        CONFLICT_BEFORE_APPLY,
        RESPONSE_LOST_AFTER_APPLY,
        UNAVAILABLE_BEFORE_APPLY
    }

    public enum ReadFault {
        NONE,
        UNAVAILABLE,
        CORRUPT
    }

    private final Map<String, VersionedRecord> records = new HashMap<>();
    private long nextStoreVersion = 1;
    private CasFault nextCasFault = CasFault.NONE;
    private ReadFault nextReadFault = ReadFault.NONE;

    public synchronized void injectNextCasFault(CasFault fault) {
        nextCasFault = StartupFactSupport.requireNonNull(fault);
    }

    public synchronized void injectNextReadFault(ReadFault fault) {
        nextReadFault = StartupFactSupport.requireNonNull(fault);
    }

    /** Seeds reference state without selecting any production backend or path. */
    public synchronized VersionedRecord seed(ProfileReadinessRecord record) {
        VersionedRecord versioned = new VersionedRecord(nextStoreVersion++, record);
        records.put(record.bookieId(), versioned);
        return versioned;
    }

    @Override
    public synchronized ReadResult read(String bookieId) {
        ReadFault fault = nextReadFault;
        nextReadFault = ReadFault.NONE;
        if (fault == ReadFault.UNAVAILABLE) {
            return ReadResult.unavailable();
        }
        if (fault == ReadFault.CORRUPT) {
            return ReadResult.corrupt();
        }
        VersionedRecord current = records.get(bookieId);
        return current == null ? ReadResult.notFound() : ReadResult.found(current);
    }

    @Override
    public synchronized CasResult compareAndSet(
            String bookieId,
            long expectedStoreVersion,
            long expectedReadinessGeneration,
            ProfileReadinessRecord newRecord) {
        StartupFactSupport.requireNonNull(newRecord);
        if (!newRecord.bookieId().equals(bookieId)) {
            return new CasResult(CasStatus.CONFLICT);
        }
        CasFault fault = nextCasFault;
        nextCasFault = CasFault.NONE;
        if (fault == CasFault.CONFLICT_BEFORE_APPLY) {
            return new CasResult(CasStatus.CONFLICT);
        }
        if (fault == CasFault.UNAVAILABLE_BEFORE_APPLY) {
            return new CasResult(CasStatus.UNAVAILABLE);
        }
        VersionedRecord current = records.get(bookieId);
        long currentVersion = current == null ? NO_STORE_VERSION : current.storeVersion();
        long currentGeneration = current == null
                ? NO_READINESS_GENERATION
                : current.record().localReadinessGeneration().value();
        if (currentVersion != expectedStoreVersion || currentGeneration != expectedReadinessGeneration) {
            return new CasResult(CasStatus.CONFLICT);
        }
        records.put(bookieId, new VersionedRecord(nextStoreVersion++, newRecord));
        return new CasResult(
                fault == CasFault.RESPONSE_LOST_AFTER_APPLY ? CasStatus.RESPONSE_LOST : CasStatus.APPLIED);
    }
}

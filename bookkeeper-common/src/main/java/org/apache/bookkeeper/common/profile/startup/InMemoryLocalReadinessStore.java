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
import java.util.Optional;

/** Deterministic in-memory durable-local-readiness reference adapter. */
public final class InMemoryLocalReadinessStore implements LocalReadinessStore {

    public enum Fault {
        NONE,
        DURABILITY_UNKNOWN_BEFORE_APPLY,
        DURABILITY_UNKNOWN_AFTER_APPLY
    }

    private final Map<String, DurableLocalReadiness> records = new HashMap<>();
    private Fault nextFault = Fault.NONE;

    public synchronized void injectNextWriteFault(Fault fault) {
        nextFault = StartupFactSupport.requireNonNull(fault);
    }

    @Override
    public synchronized Optional<DurableLocalReadiness> read(String bookieId) {
        return Optional.ofNullable(records.get(bookieId));
    }

    @Override
    public synchronized WriteResult write(DurableLocalReadiness readiness) {
        StartupFactSupport.requireNonNull(readiness);
        Fault fault = nextFault;
        nextFault = Fault.NONE;
        if (fault == Fault.DURABILITY_UNKNOWN_BEFORE_APPLY) {
            return new WriteResult(WriteStatus.DURABILITY_UNKNOWN);
        }
        DurableLocalReadiness current = records.get(readiness.bookieId());
        WriteStatus status;
        if (current == null) {
            records.put(readiness.bookieId(), readiness);
            status = WriteStatus.APPLIED;
        } else if (current.equals(readiness)) {
            status = WriteStatus.ALREADY_APPLIED;
        } else if (current.storageIncarnation().equals(readiness.storageIncarnation())
                && current.readinessGeneration().compareTo(readiness.readinessGeneration()) < 0) {
            records.put(readiness.bookieId(), readiness);
            status = WriteStatus.APPLIED;
        } else {
            status = WriteStatus.CONFLICT;
        }
        if (fault == Fault.DURABILITY_UNKNOWN_AFTER_APPLY && status == WriteStatus.APPLIED) {
            return new WriteResult(WriteStatus.DURABILITY_UNKNOWN);
        }
        return new WriteResult(status);
    }
}

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

/** Semantic durable-local-readiness boundary; no physical owner or record format is selected. */
public interface LocalReadinessStore {

    Optional<DurableLocalReadiness> read(String bookieId);

    WriteResult write(DurableLocalReadiness readiness);

    enum WriteStatus {
        APPLIED,
        ALREADY_APPLIED,
        CONFLICT,
        DURABILITY_UNKNOWN
    }

    record WriteResult(WriteStatus status) {
        public WriteResult {
            status = StartupFactSupport.requireNonNull(status);
        }
    }
}

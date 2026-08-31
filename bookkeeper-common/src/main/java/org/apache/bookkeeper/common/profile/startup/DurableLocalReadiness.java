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

/** Durable local conclusion produced only after compatibility, device, and recovery validation. */
public record DurableLocalReadiness(
        String bookieId,
        StorageIncarnation storageIncarnation,
        ManifestIdentity deviceManifestIdentity,
        Generation deviceManifestGeneration,
        Generation localFormatGeneration,
        Generation effectiveDeleteAssignmentGeneration,
        Generation readinessGeneration,
        Status status,
        boolean durable) {

    public enum Status {
        READY,
        QUARANTINED
    }

    public DurableLocalReadiness {
        bookieId = StartupFactSupport.boundedText(bookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
        storageIncarnation = StartupFactSupport.requireNonNull(storageIncarnation);
        deviceManifestIdentity = StartupFactSupport.requireNonNull(deviceManifestIdentity);
        deviceManifestGeneration = StartupFactSupport.requireNonNull(deviceManifestGeneration);
        localFormatGeneration = StartupFactSupport.requireNonNull(localFormatGeneration);
        effectiveDeleteAssignmentGeneration =
                StartupFactSupport.requireNonNull(effectiveDeleteAssignmentGeneration);
        readinessGeneration = StartupFactSupport.requireNonNull(readinessGeneration);
        status = StartupFactSupport.requireNonNull(status);
        if (status == Status.READY && !durable) {
            throw new IllegalArgumentException("READY local readiness must be durable");
        }
    }
}

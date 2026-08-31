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

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_VERSION;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.UNSUPPORTED_ENGINE;

import java.util.List;
import org.apache.bookkeeper.common.profile.EngineProfile;

/** Strict immutable logical compatibility/readiness fact; it defines no physical bytes. */
public record BookieCompatibilityFact(
        String bookieId,
        StorageIncarnation storageIncarnation,
        EngineProfile activeEngine,
        int profileWireMajor,
        int profileWireMinor,
        int descriptorHashSuite,
        List<Integer> mandatoryLocalFeatures,
        ManifestIdentity deviceManifestIdentity,
        Generation deviceManifestGeneration,
        Generation localFormatGeneration,
        Generation effectiveDeleteAssignmentGeneration,
        int minimumReaderVersion,
        int minimumWriterVersion,
        MigrationState migrationState,
        Generation migrationGeneration) {

    public static final int MAX_BOOKIE_ID_BYTES = 255;

    public enum MigrationState {
        PREPARED,
        FORMAT_READY,
        QUARANTINED,
        UNKNOWN_MANDATORY
    }

    public BookieCompatibilityFact {
        bookieId = StartupFactSupport.boundedText(bookieId, MAX_BOOKIE_ID_BYTES);
        storageIncarnation = StartupFactSupport.requireNonNull(storageIncarnation);
        activeEngine = StartupFactSupport.requireNonNull(activeEngine);
        if (activeEngine != EngineProfile.SEGMENT_WAL_ENGINE) {
            throw new StartupReadinessValidationException(UNSUPPORTED_ENGINE);
        }
        if (profileWireMajor <= 0
                || profileWireMinor < 0
                || descriptorHashSuite <= 0
                || minimumReaderVersion <= 0
                || minimumWriterVersion <= 0) {
            throw new StartupReadinessValidationException(INVALID_VERSION);
        }
        mandatoryLocalFeatures = StartupFactSupport.strictlyIncreasingPositiveIds(
                mandatoryLocalFeatures, false);
        deviceManifestIdentity = StartupFactSupport.requireNonNull(deviceManifestIdentity);
        deviceManifestGeneration = StartupFactSupport.requireNonNull(deviceManifestGeneration);
        localFormatGeneration = StartupFactSupport.requireNonNull(localFormatGeneration);
        effectiveDeleteAssignmentGeneration =
                StartupFactSupport.requireNonNull(effectiveDeleteAssignmentGeneration);
        migrationState = StartupFactSupport.requireNonNull(migrationState);
        migrationGeneration = StartupFactSupport.requireNonNull(migrationGeneration);
    }
}

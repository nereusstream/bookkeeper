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

import java.util.List;
import org.apache.bookkeeper.common.profile.EngineProfile;

/** Domain-specific persistent cluster readiness record. */
public record ProfileReadinessRecord(
        String bookieId,
        StorageIncarnation storageIncarnation,
        EngineProfile engine,
        int protocolMajor,
        int protocolMinor,
        Generation capabilityGeneration,
        Generation localFormatGeneration,
        ManifestIdentity deviceManifestIdentity,
        Generation deviceManifestGeneration,
        Generation effectiveAssignmentGeneration,
        Generation localReadinessGeneration,
        int minimumReaderVersion,
        int minimumWriterVersion,
        List<Integer> capabilityHints,
        State state) {

    public enum State {
        READY,
        NON_WRITABLE
    }

    public ProfileReadinessRecord {
        bookieId = StartupFactSupport.boundedText(bookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
        storageIncarnation = StartupFactSupport.requireNonNull(storageIncarnation);
        engine = StartupFactSupport.requireNonNull(engine);
        if (protocolMajor <= 0 || protocolMinor < 0 || minimumReaderVersion <= 0 || minimumWriterVersion <= 0) {
            throw new StartupReadinessValidationException(INVALID_VERSION);
        }
        capabilityGeneration = StartupFactSupport.requireNonNull(capabilityGeneration);
        localFormatGeneration = StartupFactSupport.requireNonNull(localFormatGeneration);
        deviceManifestIdentity = StartupFactSupport.requireNonNull(deviceManifestIdentity);
        deviceManifestGeneration = StartupFactSupport.requireNonNull(deviceManifestGeneration);
        effectiveAssignmentGeneration = StartupFactSupport.requireNonNull(effectiveAssignmentGeneration);
        localReadinessGeneration = StartupFactSupport.requireNonNull(localReadinessGeneration);
        capabilityHints = StartupFactSupport.strictlyIncreasingPositiveIds(capabilityHints, false);
        state = StartupFactSupport.requireNonNull(state);
    }
}

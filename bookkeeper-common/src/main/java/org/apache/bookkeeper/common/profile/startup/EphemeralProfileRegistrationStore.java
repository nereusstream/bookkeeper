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

/** Semantic service-info and ephemeral writable registration boundary. */
public interface EphemeralProfileRegistrationStore {

    Optional<ProfileServiceInfoProjection> readServiceInfo(String bookieId);

    Optional<WritableRegistration> readWritable(String bookieId);

    PublicationResult publishServiceInfo(ProfileServiceInfoProjection projection);

    PublicationResult registerWritable(WritableRegistration registration);

    void demote(String bookieId);

    enum PublicationStatus {
        APPLIED,
        ALREADY_APPLIED,
        CONFLICT,
        RESPONSE_LOST,
        UNAVAILABLE
    }

    record PublicationResult(PublicationStatus status) {
        public PublicationResult {
            status = StartupFactSupport.requireNonNull(status);
        }
    }

    record WritableRegistration(
            String bookieId,
            StorageIncarnation storageIncarnation,
            Generation readinessGeneration,
            long persistentStoreVersion) {
        public WritableRegistration {
            bookieId = StartupFactSupport.boundedText(bookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
            storageIncarnation = StartupFactSupport.requireNonNull(storageIncarnation);
            readinessGeneration = StartupFactSupport.requireNonNull(readinessGeneration);
            if (persistentStoreVersion < 0) {
                throw new IllegalArgumentException("persistentStoreVersion must be non-negative");
            }
        }
    }
}

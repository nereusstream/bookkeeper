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

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.DUPLICATE_OR_UNORDERED_VALUE;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.EMPTY_COLLECTION;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_VERSION;

import java.util.ArrayList;
import java.util.List;

/** Immutable logical required-device manifest and observed superblock facts. */
public record RequiredDeviceManifest(
        ManifestIdentity identity, Generation generation, List<RequiredDeviceFact> devices) {

    public RequiredDeviceManifest {
        identity = StartupFactSupport.requireNonNull(identity);
        generation = StartupFactSupport.requireNonNull(generation);
        if (devices == null) {
            throw new StartupReadinessValidationException(
                    StartupReadinessValidationException.Reason.NULL_FIELD);
        }
        if (devices.isEmpty()) {
            throw new StartupReadinessValidationException(EMPTY_COLLECTION);
        }
        List<RequiredDeviceFact> copy = new ArrayList<>(devices.size());
        String previous = null;
        for (RequiredDeviceFact device : devices) {
            StartupFactSupport.requireNonNull(device);
            if (previous != null && previous.compareTo(device.deviceId()) >= 0) {
                throw new StartupReadinessValidationException(DUPLICATE_OR_UNORDERED_VALUE);
            }
            copy.add(device);
            previous = device.deviceId();
        }
        devices = List.copyOf(copy);
    }

    public enum DeviceStatus {
        READY,
        MISSING,
        PARTIAL,
        CORRUPT,
        UNKNOWN_MANDATORY
    }

    /** One expected required device plus its already-parsed logical superblock status. */
    public record RequiredDeviceFact(
            String deviceId,
            StorageIncarnation storageIncarnation,
            Generation manifestGeneration,
            DeviceStatus status,
            int arenaFormatVersion,
            int controlFormatVersion,
            int checkpointFormatVersion,
            Generation superblockGeneration,
            List<Integer> mandatoryFeatures) {

        public static final int MAX_DEVICE_ID_BYTES = 128;

        public RequiredDeviceFact {
            deviceId = StartupFactSupport.boundedText(deviceId, MAX_DEVICE_ID_BYTES);
            storageIncarnation = StartupFactSupport.requireNonNull(storageIncarnation);
            manifestGeneration = StartupFactSupport.requireNonNull(manifestGeneration);
            status = StartupFactSupport.requireNonNull(status);
            if (arenaFormatVersion <= 0 || controlFormatVersion <= 0 || checkpointFormatVersion <= 0) {
                throw new StartupReadinessValidationException(INVALID_VERSION);
            }
            superblockGeneration = StartupFactSupport.requireNonNull(superblockGeneration);
            mandatoryFeatures = StartupFactSupport.strictlyIncreasingPositiveIds(mandatoryFeatures, false);
        }
    }
}

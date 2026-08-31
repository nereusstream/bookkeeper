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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Deterministic in-memory service-info/ephemeral registration reference adapter. */
public final class InMemoryEphemeralProfileRegistrationStore implements EphemeralProfileRegistrationStore {

    public enum Fault {
        NONE,
        CONFLICT_BEFORE_APPLY,
        RESPONSE_LOST_AFTER_APPLY,
        UNAVAILABLE_BEFORE_APPLY
    }

    private final Map<String, ProfileServiceInfoProjection> serviceInfo = new HashMap<>();
    private final Map<String, WritableRegistration> writable = new HashMap<>();
    private final List<String> events = new ArrayList<>();
    private Fault nextServiceInfoFault = Fault.NONE;
    private Fault nextWritableFault = Fault.NONE;

    public synchronized void injectNextServiceInfoFault(Fault fault) {
        nextServiceInfoFault = StartupFactSupport.requireNonNull(fault);
    }

    public synchronized void injectNextWritableFault(Fault fault) {
        nextWritableFault = StartupFactSupport.requireNonNull(fault);
    }

    public synchronized List<String> events() {
        return List.copyOf(events);
    }

    @Override
    public synchronized Optional<ProfileServiceInfoProjection> readServiceInfo(String bookieId) {
        events.add("read-service-info");
        return Optional.ofNullable(serviceInfo.get(bookieId));
    }

    @Override
    public synchronized Optional<WritableRegistration> readWritable(String bookieId) {
        events.add("read-writable");
        return Optional.ofNullable(writable.get(bookieId));
    }

    @Override
    public synchronized PublicationResult publishServiceInfo(ProfileServiceInfoProjection projection) {
        Fault fault = nextServiceInfoFault;
        nextServiceInfoFault = Fault.NONE;
        if (fault == Fault.CONFLICT_BEFORE_APPLY) {
            return new PublicationResult(PublicationStatus.CONFLICT);
        }
        if (fault == Fault.UNAVAILABLE_BEFORE_APPLY) {
            return new PublicationResult(PublicationStatus.UNAVAILABLE);
        }
        ProfileServiceInfoProjection current = serviceInfo.get(projection.bookieId());
        if (current != null && current.equals(projection)) {
            return new PublicationResult(PublicationStatus.ALREADY_APPLIED);
        }
        if (writable.containsKey(projection.bookieId())) {
            return new PublicationResult(PublicationStatus.CONFLICT);
        }
        serviceInfo.put(projection.bookieId(), projection);
        events.add("publish-service-info");
        return new PublicationResult(
                fault == Fault.RESPONSE_LOST_AFTER_APPLY
                        ? PublicationStatus.RESPONSE_LOST
                        : PublicationStatus.APPLIED);
    }

    @Override
    public synchronized PublicationResult registerWritable(WritableRegistration registration) {
        Fault fault = nextWritableFault;
        nextWritableFault = Fault.NONE;
        if (fault == Fault.CONFLICT_BEFORE_APPLY) {
            return new PublicationResult(PublicationStatus.CONFLICT);
        }
        if (fault == Fault.UNAVAILABLE_BEFORE_APPLY) {
            return new PublicationResult(PublicationStatus.UNAVAILABLE);
        }
        ProfileServiceInfoProjection projection = serviceInfo.get(registration.bookieId());
        if (projection == null
                || !projection.storageIncarnation().equals(registration.storageIncarnation())
                || !projection.readinessGeneration().equals(registration.readinessGeneration())
                || projection.persistentStoreVersion() != registration.persistentStoreVersion()) {
            return new PublicationResult(PublicationStatus.CONFLICT);
        }
        WritableRegistration current = writable.get(registration.bookieId());
        if (registration.equals(current)) {
            return new PublicationResult(PublicationStatus.ALREADY_APPLIED);
        }
        if (current != null) {
            return new PublicationResult(PublicationStatus.CONFLICT);
        }
        writable.put(registration.bookieId(), registration);
        events.add("register-writable");
        return new PublicationResult(
                fault == Fault.RESPONSE_LOST_AFTER_APPLY
                        ? PublicationStatus.RESPONSE_LOST
                        : PublicationStatus.APPLIED);
    }

    @Override
    public synchronized void demote(String bookieId) {
        writable.remove(bookieId);
        serviceInfo.remove(bookieId);
        events.add("demote");
    }
}

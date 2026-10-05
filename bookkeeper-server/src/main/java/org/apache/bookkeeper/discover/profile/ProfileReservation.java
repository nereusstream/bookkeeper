/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;

/** Immutable experimental PREPARING request, not a second membership authority or normal-write permission. */
public record ProfileReservation(ProfileMetadataBacklink backlink, List<ProfileControlScope> installs) {
    public static final int MAX_INITIAL_TARGETS = 32;

    public ProfileReservation {
        Objects.requireNonNull(backlink, "backlink");
        installs = List.copyOf(installs);
        if (installs.isEmpty() || installs.size() > MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("Invalid initial Profile target count");
        }
        ProfileControlScope first = installs.get(0);
        HashSet<String> targets = new HashSet<>();
        for (ProfileControlScope scope : installs) {
            if (scope.operation() != ProfileControlOperation.INSTALL || scope.ledgerId() != backlink.ledgerId()
                    || !Arrays.equals(scope.ledgerInstanceId(), backlink.instanceId())
                    || !scope.descriptorIdentity().equals(first.descriptorIdentity())
                    || !scope.authorityDomain().equals(first.authorityDomain())
                    || scope.authorityGeneration() != first.authorityGeneration()
                    || !Arrays.equals(scope.operationId(), first.operationId())
                    || !Arrays.equals(scope.publicSemanticPayloadIdentity(), first.publicSemanticPayloadIdentity())
                    || !targets.add(scope.targetBookie())) {
                throw new IllegalArgumentException("Inconsistent initial Profile reservation");
            }
        }
    }
}

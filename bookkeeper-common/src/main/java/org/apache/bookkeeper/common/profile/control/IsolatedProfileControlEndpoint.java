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

package org.apache.bookkeeper.common.profile.control;

import java.util.Objects;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;

/**
 * Typed reference endpoint for the isolated {@code bookie-profile} control path.
 *
 * <p>This class consumes transport facts from a future listener adapter. It opens no socket, terminates no TLS
 * connection, and has no production storage, registration, or ACK authority.
 */
public final class IsolatedProfileControlEndpoint {

    private final EngineProfile activeEngine;
    private final ProfileDescriptorValidator descriptorValidator;
    private final ProfileStaticOperationAuthorizer staticAuthorizer;
    private final ProfileControlStore controlStore;
    private final ProfileControlAuthorizer exactAuthorizer;
    private final ProtectedProfileStateStore protectedStateStore;

    public IsolatedProfileControlEndpoint(
            EngineProfile activeEngine,
            ProfileDescriptorValidator descriptorValidator,
            ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlStore controlStore,
            ProfileControlAuthorizer exactAuthorizer,
            ProtectedProfileStateStore protectedStateStore) {
        this.activeEngine = Objects.requireNonNull(activeEngine, "activeEngine");
        this.descriptorValidator = Objects.requireNonNull(descriptorValidator, "descriptorValidator");
        this.staticAuthorizer = Objects.requireNonNull(staticAuthorizer, "staticAuthorizer");
        this.controlStore = Objects.requireNonNull(controlStore, "controlStore");
        this.exactAuthorizer = Objects.requireNonNull(exactAuthorizer, "exactAuthorizer");
        this.protectedStateStore = Objects.requireNonNull(protectedStateStore, "protectedStateStore");
    }

    /** Executes one bounded cold-path operation and returns only secret-free coordinates. */
    public ProfileControlResult handle(ProfileTransportContext transport, ProfileControlRequest request) {
        Objects.requireNonNull(request, "request");
        ProfileControlScope scope = request.scope();
        if (transport == null || !transport.isRequiredMutualTls()) {
            return result(scope, ProfileControlResult.Outcome.DENIED);
        }

        ProfilePrincipal principal = transport.authenticatedPrincipal().orElseThrow();
        if (!staticAllows(principal, scope.operation())) {
            return result(scope, ProfileControlResult.Outcome.DENIED);
        }

        ProfileControlStore.ReadResult readResult = directRead(scope);
        if (readResult == null) {
            return result(scope, ProfileControlResult.Outcome.TRANSIENT_RECONCILING);
        }
        switch (readResult.status()) {
            case NOT_FOUND:
                return result(scope, ProfileControlResult.Outcome.NOT_FOUND);
            case UNAVAILABLE:
                return result(scope, ProfileControlResult.Outcome.TRANSIENT_RECONCILING);
            case QUARANTINED:
                return result(scope, ProfileControlResult.Outcome.QUARANTINED);
            case FOUND:
                break;
            default:
                return result(scope, ProfileControlResult.Outcome.QUARANTINED);
        }

        CommittedProfileAuthority authority = readResult.committedAuthority().orElseThrow();
        if (!exactlyAuthorized(principal, scope, authority)) {
            return result(scope, ProfileControlResult.Outcome.DENIED);
        }
        if (!authority.scope().equals(scope) || !authority.permits(scope.operation())) {
            return result(scope, ProfileControlResult.Outcome.CONFLICT);
        }
        if (authority.requiredEngine() != activeEngine) {
            return result(scope, ProfileControlResult.Outcome.UNSUPPORTED);
        }

        ProfileDescriptor decodedDescriptor = null;
        if (scope.operation() == ProfileControlOperation.INSTALL) {
            try {
                byte[] canonicalDescriptor = request.canonicalDescriptor().orElseThrow();
                decodedDescriptor = ProfileDescriptorCodec.decodeAndVerify(
                        canonicalDescriptor, scope.descriptorIdentity(), descriptorValidator);
            } catch (IllegalArgumentException exception) {
                return result(scope, ProfileControlResult.Outcome.BAD_REQUEST);
            }
            if (request.expectedEngine().orElse(null) != activeEngine
                    || decodedDescriptor.requiredEngine() != activeEngine
                    || request.protectedCredential().isEmpty()) {
                return result(scope, ProfileControlResult.Outcome.UNSUPPORTED);
            }
        }

        ProtectedProfileStateStore.TransitionResult transitionResult;
        try {
            if (scope.operation() == ProfileControlOperation.STATUS_QUERY) {
                transitionResult = protectedStateStore.queryOperationResult(scope);
            } else {
                transitionResult = protectedStateStore.conditionalApply(
                        new ProtectedProfileStateStore.Transition(request, authority, decodedDescriptor));
            }
        } catch (RuntimeException failure) {
            return result(scope, ProfileControlResult.Outcome.DURABILITY_UNKNOWN);
        }
        if (transitionResult == null) {
            return result(scope, ProfileControlResult.Outcome.DURABILITY_UNKNOWN);
        }
        ProfileControlResult.Outcome outcome = transitionResult.outcome();
        boolean durable = transitionResult.durable();
        if ((outcome == ProfileControlResult.Outcome.APPLIED
                        || outcome == ProfileControlResult.Outcome.ALREADY_APPLIED)
                && !durable) {
            outcome = ProfileControlResult.Outcome.DURABILITY_UNKNOWN;
        }
        return ProfileControlResult.fromScope(
                scope, outcome, transitionResult.localGeneration(), durable);
    }

    private boolean staticAllows(ProfilePrincipal principal, ProfileControlOperation operation) {
        try {
            return staticAuthorizer.allows(principal, operation);
        } catch (RuntimeException failure) {
            return false;
        }
    }

    private ProfileControlStore.ReadResult directRead(ProfileControlScope scope) {
        try {
            return controlStore.read(ProfileAuthorityKey.fromScope(scope));
        } catch (RuntimeException failure) {
            return null;
        }
    }

    private boolean exactlyAuthorized(
            ProfilePrincipal principal,
            ProfileControlScope scope,
            CommittedProfileAuthority authority) {
        try {
            return exactAuthorizer.authorize(principal, scope, authority)
                    == ProfileControlAuthorizer.Decision.ALLOW;
        } catch (RuntimeException failure) {
            return false;
        }
    }

    private static ProfileControlResult result(
            ProfileControlScope scope, ProfileControlResult.Outcome outcome) {
        return ProfileControlResult.fromScope(scope, outcome, 0, false);
    }
}

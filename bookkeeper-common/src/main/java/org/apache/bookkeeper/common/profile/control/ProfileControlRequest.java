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

import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_REQUEST_SHAPE;

import java.util.Objects;
import java.util.Optional;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;

/** Bounded typed request; physical control-tail packing remains deliberately unfrozen. */
public final class ProfileControlRequest {

    public record RecoveryBinding(byte[] intentId, long intentGeneration, long grantGeneration) {
        public RecoveryBinding {
            intentId = Objects.requireNonNull(intentId, "intentId").clone();
            if (intentId.length != 16 || java.util.Arrays.equals(intentId, new byte[16])
                    || intentGeneration <= 0 || grantGeneration <= 0) {
                throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
            }
        }
        @Override
        public byte[] intentId() {
            return intentId.clone();
        }
    }

    private final RecoveryBinding recoveryBinding;
    private final byte[] closedGrantOperationId;
    private final ProfileControlScope scope;
    private final byte[] canonicalDescriptor;
    private final EngineProfile expectedEngine;
    private final ProtectedProfileCredential protectedCredential;

    private ProfileControlRequest(
            ProfileControlScope scope,
            byte[] canonicalDescriptor,
            EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery) {
        this(scope, canonicalDescriptor, expectedEngine, protectedCredential, recovery, null);
    }

    private ProfileControlRequest(ProfileControlScope scope, byte[] canonicalDescriptor, EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential, RecoveryBinding recovery, byte[] closedGrant) {
        recoveryBinding = recovery;
        closedGrantOperationId = closedGrant == null ? null : closedGrant.clone();
        this.scope = Objects.requireNonNull(scope, "scope");
        this.canonicalDescriptor = canonicalDescriptor == null ? null : canonicalDescriptor.clone();
        this.expectedEngine = expectedEngine;
        this.protectedCredential = protectedCredential == null ? null : protectedCredential.copy();
    }

    public static ProfileControlRequest install(
            ProfileControlScope scope,
            byte[] canonicalDescriptor,
            EngineProfile expectedEngine,
            ProtectedProfileCredential protectedCredential) {
        if (scope == null
                || scope.operation() != ProfileControlOperation.INSTALL
                || canonicalDescriptor == null
                || canonicalDescriptor.length == 0
                || canonicalDescriptor.length > ProfileDescriptorCodec.ABSOLUTE_INPUT_CAP
                || expectedEngine == null
                || protectedCredential == null) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, canonicalDescriptor, expectedEngine, protectedCredential);
    }

    public static ProfileControlRequest operation(ProfileControlScope scope) {
        if (scope == null || scope.operation() == ProfileControlOperation.INSTALL) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null);
    }

    public static ProfileControlRequest recoveryGrant(ProfileControlScope scope, RecoveryBinding binding) {
        if (scope == null || scope.operation() != ProfileControlOperation.RECOVERY_GRANT || binding == null
                || scope.authorityGeneration() != binding.grantGeneration() || scope.exactRange() == null) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, binding);
    }

    public Optional<RecoveryBinding> recoveryBinding() {
        return Optional.ofNullable(recoveryBinding);
    }

    public static ProfileControlRequest recoveryClose(ProfileControlScope scope, RecoveryBinding binding,
            byte[] grantOperationId) {
        if (scope == null || scope.operation() != ProfileControlOperation.RECOVERY_GRANT_CLOSE || binding == null
                || scope.exactRange() == null || grantOperationId == null || grantOperationId.length != 16
                || java.util.Arrays.equals(grantOperationId, new byte[16])
                || java.util.Arrays.equals(grantOperationId, scope.operationId())) {
            throw new ProfileControlValidationException(INVALID_REQUEST_SHAPE);
        }
        return new ProfileControlRequest(scope, null, null, null, binding, grantOperationId);
    }

    public Optional<byte[]> closedGrantOperationId() {
        return closedGrantOperationId == null ? Optional.empty() : Optional.of(closedGrantOperationId.clone());
    }

    public ProfileControlScope scope() {
        return scope;
    }

    public Optional<byte[]> canonicalDescriptor() {
        return canonicalDescriptor == null ? Optional.empty() : Optional.of(canonicalDescriptor.clone());
    }

    public Optional<EngineProfile> expectedEngine() {
        return Optional.ofNullable(expectedEngine);
    }

    public Optional<ProtectedProfileCredential> protectedCredential() {
        return protectedCredential == null ? Optional.empty() : Optional.of(protectedCredential.copy());
    }

    @Override
    public String toString() {
        return "ProfileControlRequest{scope=" + scope
                + ", descriptorBytes=" + (canonicalDescriptor == null ? 0 : canonicalDescriptor.length)
                + ", expectedEngine=" + expectedEngine
                + ", protectedCredential=" + (protectedCredential == null ? "absent" : protectedCredential)
                + '}';
    }
}

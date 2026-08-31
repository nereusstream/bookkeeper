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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileCapability;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.junit.Test;

public class IsolatedProfileControlEndpointTest {

    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(
            EngineProfile.SEGMENT_WAL_ENGINE,
            PayloadFormat.OPAQUE_LEDGER,
            DurabilityMode.SYNC_ON_ACK,
            3,
            3,
            2,
            0,
            7,
            11,
            List.of(new ProfileCapability(101, 1)));
    private static final ProfileDescriptorValidator VALIDATOR =
            new ProfileDescriptorValidator((id, version) -> id == 101 && version == 1, (id, generation) ->
                id == 7 && generation == 11);
    private static final byte[] DESCRIPTOR_BYTES = ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR);
    private static final ProfileDescriptorIdentity DESCRIPTOR_IDENTITY =
            ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR);

    @Test
    public void validInstallUsesStrictOrderAndOneColdRead() {
        List<String> events = new ArrayList<>();
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        CommittedProfileAuthority authority = authority(scope, CommittedProfileAuthority.State.PREPARING);
        RecordingStateStore stateStore = new RecordingStateStore(events);
        IsolatedProfileControlEndpoint endpoint = endpoint(
                (principal, operation) -> {
                    events.add("static");
                    return true;
                },
                authorityKey -> {
                    events.add("read");
                    assertEquals(ProfileAuthorityKey.fromScope(scope), authorityKey);
                    return ProfileControlStore.ReadResult.found(authority);
                },
                (principal, exactScope, committed) -> {
                    events.add("exact");
                    assertEquals(scope, exactScope);
                    assertEquals(authority, committed);
                    return ProfileControlAuthorizer.Decision.ALLOW;
                },
                stateStore);

        ProfileControlResult result = endpoint.handle(transport(), install(scope));

        assertEquals(ProfileControlResult.Outcome.APPLIED, result.outcome());
        assertEquals(9, result.localGeneration());
        assertTrue(result.durable());
        assertEquals(List.of("static", "read", "exact", "apply"), events);
        assertEquals(1, stateStore.applyCount.get());
        assertEquals(DESCRIPTOR, stateStore.lastTransition.decodedDescriptor());
    }

    @Test
    public void transportMustBeImmediateTls13MutualAuthentication() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger staticChecks = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger effects = new AtomicInteger();
        IsolatedProfileControlEndpoint endpoint = endpoint(
                (principal, operation) -> {
                    staticChecks.incrementAndGet();
                    return true;
                },
                key -> {
                    reads.incrementAndGet();
                    return ProfileControlStore.ReadResult.notFound();
                },
                (principal, exactScope, authority) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));

        List<ProfileTransportContext> invalid = List.of(
                new ProfileTransportContext("bookie-rpc", "TLSv1.3", true, true, true, principal()),
                new ProfileTransportContext("bookie-profile", "TLSv1.2", true, true, true, principal()),
                new ProfileTransportContext("bookie-profile", "TLSv1.3", false, true, true, principal()),
                new ProfileTransportContext("bookie-profile", "TLSv1.3", true, false, true, principal()),
                new ProfileTransportContext("bookie-profile", "TLSv1.3", true, true, false, principal()),
                new ProfileTransportContext("bookie-profile", "TLSv1.3", true, true, true, null));
        for (ProfileTransportContext context : invalid) {
            assertEquals(ProfileControlResult.Outcome.DENIED, endpoint.handle(context, install(scope)).outcome());
        }
        assertEquals(0, staticChecks.get());
        assertEquals(0, reads.get());
        assertEquals(0, effects.get());
    }

    @Test
    public void staticDenialHappensBeforeColdReadOrLocalEffect() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger effects = new AtomicInteger();
        IsolatedProfileControlEndpoint endpoint = endpoint(
                (principal, operation) -> false,
                key -> {
                    reads.incrementAndGet();
                    return ProfileControlStore.ReadResult.notFound();
                },
                (principal, exactScope, authority) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));

        assertEquals(ProfileControlResult.Outcome.DENIED, endpoint.handle(transport(), install(scope)).outcome());
        assertEquals(0, reads.get());
        assertEquals(0, effects.get());
    }

    @Test
    public void unavailableAndQuarantinedAuthorityFailClosedWithoutEffect() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger effects = new AtomicInteger();
        for (ProfileControlStore.ReadResult readResult :
                List.of(ProfileControlStore.ReadResult.unavailable(), ProfileControlStore.ReadResult.quarantined())) {
            IsolatedProfileControlEndpoint endpoint = endpoint(
                    (principal, operation) -> true,
                    key -> readResult,
                    (principal, exactScope, authority) -> ProfileControlAuthorizer.Decision.ALLOW,
                    countingStateStore(effects));
            ProfileControlResult result = endpoint.handle(transport(), install(scope));
            assertTrue(result.outcome() == ProfileControlResult.Outcome.TRANSIENT_RECONCILING
                    || result.outcome() == ProfileControlResult.Outcome.QUARANTINED);
        }
        assertEquals(0, effects.get());
    }

    @Test
    public void exactAuthorizationAndAuthorityTupleBothFailClosed() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger effects = new AtomicInteger();
        CommittedProfileAuthority authority = authority(scope, CommittedProfileAuthority.State.PREPARING);
        IsolatedProfileControlEndpoint denied = endpoint(
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(authority),
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.DENY,
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.DENIED, denied.handle(transport(), install(scope)).outcome());

        ProfileControlScope conflictingScope = scope(ProfileControlOperation.INSTALL, (byte) 0x33);
        IsolatedProfileControlEndpoint conflicting = endpoint(
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(
                        authority(conflictingScope, CommittedProfileAuthority.State.PREPARING)),
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.CONFLICT,
                conflicting.handle(transport(), install(scope)).outcome());
        assertEquals(0, effects.get());
    }

    @Test
    public void purposeAndCommittedStateCannotBeReplayedAcrossActivationClasses() {
        assertThrows(
                ProfileControlValidationException.class,
                () -> new ProfileControlScope(
                        ProfileControlOperation.ACTIVATE_INITIAL,
                        ProfileAuthorityPurpose.POST_MEMBERSHIP_REPLACEMENT,
                        44,
                        filled(16, (byte) 1),
                        DESCRIPTOR_IDENTITY,
                        "127.0.0.1:3181",
                        filled(16, (byte) 2),
                        "ledger-profile",
                        8,
                        filled(16, (byte) 3),
                        filled(32, (byte) 4),
                        null));

        ProfileControlScope scope = scope(ProfileControlOperation.ACTIVATE_INITIAL);
        AtomicInteger effects = new AtomicInteger();
        CommittedProfileAuthority wrongState =
                authority(scope, CommittedProfileAuthority.State.POST_MEMBERSHIP_REPLACEMENT);
        IsolatedProfileControlEndpoint endpoint = endpoint(
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(wrongState),
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.CONFLICT,
                endpoint.handle(transport(), ProfileControlRequest.operation(scope)).outcome());
        assertEquals(0, effects.get());
    }

    @Test
    public void installStrictlyVerifiesDescriptorIdentityAndEngineBeforeEffect() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger effects = new AtomicInteger();
        IsolatedProfileControlEndpoint endpoint = endpoint(
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(
                        authority(scope, CommittedProfileAuthority.State.PREPARING)),
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));
        byte[] corrupted = DESCRIPTOR_BYTES.clone();
        corrupted[corrupted.length - 1] ^= 1;
        ProfileControlRequest request = ProfileControlRequest.install(
                scope, corrupted, EngineProfile.SEGMENT_WAL_ENGINE, credential((byte) 0x5a));

        assertEquals(ProfileControlResult.Outcome.BAD_REQUEST, endpoint.handle(transport(), request).outcome());
        assertEquals(0, effects.get());

        IsolatedProfileControlEndpoint wrongEngine = new IsolatedProfileControlEndpoint(
                EngineProfile.CLASSIC_ENGINE,
                VALIDATOR,
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(
                        authority(scope, CommittedProfileAuthority.State.PREPARING)),
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.UNSUPPORTED,
                wrongEngine.handle(transport(), install(scope)).outcome());
        assertEquals(0, effects.get());
    }

    @Test
    public void appliedWithoutDurabilityIsNeverReportedAsSuccess() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        ProtectedProfileStateStore stateStore = new ProtectedProfileStateStore() {
            @Override
            public TransitionResult conditionalApply(Transition transition) {
                return new TransitionResult(ProfileControlResult.Outcome.APPLIED, 12, false);
            }

            @Override
            public TransitionResult queryOperationResult(ProfileControlScope queryScope) {
                throw new AssertionError("query not expected");
            }
        };
        ProfileControlResult result = endpoint(
                        (principal, operation) -> true,
                        key -> ProfileControlStore.ReadResult.found(
                                authority(scope, CommittedProfileAuthority.State.PREPARING)),
                        (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                        stateStore)
                .handle(transport(), install(scope));
        assertEquals(ProfileControlResult.Outcome.DURABILITY_UNKNOWN, result.outcome());
        assertFalse(result.durable());
    }

    @Test
    public void statusQueryUsesProtectedStoreQueryAndReturnsNoSecret() {
        ProfileControlScope scope = scope(ProfileControlOperation.STATUS_QUERY);
        AtomicInteger queries = new AtomicInteger();
        ProtectedProfileStateStore stateStore = new ProtectedProfileStateStore() {
            @Override
            public TransitionResult conditionalApply(Transition transition) {
                throw new AssertionError("transition not expected");
            }

            @Override
            public TransitionResult queryOperationResult(ProfileControlScope queryScope) {
                queries.incrementAndGet();
                assertEquals(scope, queryScope);
                return new TransitionResult(ProfileControlResult.Outcome.ALREADY_APPLIED, 17, true);
            }
        };
        ProfileControlResult result = endpoint(
                        (principal, operation) -> true,
                        key -> ProfileControlStore.ReadResult.found(
                                authority(scope, CommittedProfileAuthority.State.STATUS_QUERYABLE)),
                        (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                        stateStore)
                .handle(transport(), ProfileControlRequest.operation(scope));
        assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED, result.outcome());
        assertEquals(1, queries.get());
        assertFalse(result.toString().contains("5a5a5a5a"));
    }

    @Test
    public void credentialHasNoByteAccessorAndAllPublicTextIsRedacted() {
        ProtectedProfileCredential credential = credential((byte) 0x5a);
        ProtectedProfileCredential same = credential((byte) 0x5a);
        ProtectedProfileCredential different = credential((byte) 0x6b);
        assertTrue(credential.matches(same));
        assertFalse(credential.matches(different));
        assertEquals("<redacted>", credential.toString());
        assertFalse(Arrays.stream(ProtectedProfileCredential.class.getMethods())
                .map(Method::getReturnType)
                .anyMatch(byte[].class::equals));
        String requestText = install(scope(ProfileControlOperation.INSTALL)).toString();
        assertTrue(requestText.contains("<redacted>"));
        assertFalse(requestText.contains("5a5a5a5a"));
    }

    @Test
    public void fixedIdentifiersAreDefensivelyCopiedAndRangeIsOperationBound() {
        byte[] instance = filled(16, (byte) 1);
        ProfileControlScope scope = new ProfileControlScope(
                ProfileControlOperation.INSTALL,
                ProfileAuthorityPurpose.PREPARING_INSTALL,
                44,
                instance,
                DESCRIPTOR_IDENTITY,
                "127.0.0.1:3181",
                filled(16, (byte) 2),
                "ledger-profile",
                8,
                filled(16, (byte) 3),
                filled(32, (byte) 4),
                null);
        instance[0] = 9;
        assertNotEquals(9, scope.ledgerInstanceId()[0]);
        byte[] returned = scope.ledgerInstanceId();
        returned[0] = 8;
        assertNotEquals(8, scope.ledgerInstanceId()[0]);
        assertNull(scope(ProfileControlOperation.RECOVERY_GRANT).exactRange());

        assertThrows(
                ProfileControlValidationException.class,
                () -> new ProfileControlScope(
                        ProfileControlOperation.INSTALL,
                        ProfileAuthorityPurpose.PREPARING_INSTALL,
                        44,
                        filled(16, (byte) 1),
                        DESCRIPTOR_IDENTITY,
                        "127.0.0.1:3181",
                        filled(16, (byte) 2),
                        "ledger-profile",
                        8,
                        filled(16, (byte) 3),
                        filled(32, (byte) 4),
                        new ProfileExactRange(5, 9)));

        ProfileControlScope ranged = new ProfileControlScope(
                ProfileControlOperation.RECOVERY_GRANT,
                ProfileAuthorityPurpose.RECOVERY_GRANT,
                44,
                filled(16, (byte) 1),
                DESCRIPTOR_IDENTITY,
                "127.0.0.1:3181",
                filled(16, (byte) 2),
                "ledger-profile",
                8,
                filled(16, (byte) 3),
                filled(32, (byte) 4),
                new ProfileExactRange(5, 9));
        assertEquals(new ProfileExactRange(5, 9), ranged.exactRange());
    }

    @Test
    public void dependencyFailuresRemainFailClosed() {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL);
        AtomicInteger effects = new AtomicInteger();
        IsolatedProfileControlEndpoint readFailure = endpoint(
                (principal, operation) -> true,
                key -> {
                    throw new IllegalStateException("unavailable");
                },
                (principal, exactScope, committed) -> ProfileControlAuthorizer.Decision.ALLOW,
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                readFailure.handle(transport(), install(scope)).outcome());

        IsolatedProfileControlEndpoint authFailure = endpoint(
                (principal, operation) -> true,
                key -> ProfileControlStore.ReadResult.found(
                        authority(scope, CommittedProfileAuthority.State.PREPARING)),
                (principal, exactScope, committed) -> {
                    throw new IllegalStateException("authorizer unavailable");
                },
                countingStateStore(effects));
        assertEquals(ProfileControlResult.Outcome.DENIED,
                authFailure.handle(transport(), install(scope)).outcome());
        assertEquals(0, effects.get());
    }

    private static IsolatedProfileControlEndpoint endpoint(
            ProfileStaticOperationAuthorizer staticAuthorizer,
            ProfileControlStore controlStore,
            ProfileControlAuthorizer exactAuthorizer,
            ProtectedProfileStateStore stateStore) {
        return new IsolatedProfileControlEndpoint(
                EngineProfile.SEGMENT_WAL_ENGINE,
                VALIDATOR,
                staticAuthorizer,
                controlStore,
                exactAuthorizer,
                stateStore);
    }

    private static ProfileTransportContext transport() {
        return new ProfileTransportContext("bookie-profile", "TLSv1.3", true, true, true, principal());
    }

    private static ProfilePrincipal principal() {
        return new ProfilePrincipal(
                ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                "operators",
                "cn=test-ca",
                "cn=profile-controller");
    }

    private static ProfileControlRequest install(ProfileControlScope scope) {
        return ProfileControlRequest.install(
                scope, DESCRIPTOR_BYTES, EngineProfile.SEGMENT_WAL_ENGINE, credential((byte) 0x5a));
    }

    private static ProtectedProfileCredential credential(byte value) {
        return ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1, filled(20, value));
    }

    private static ProfileControlScope scope(ProfileControlOperation operation) {
        return scope(operation, (byte) 3);
    }

    private static ProfileControlScope scope(ProfileControlOperation operation, byte operationIdByte) {
        return new ProfileControlScope(
                operation,
                operation.purpose(),
                44,
                filled(16, (byte) 1),
                DESCRIPTOR_IDENTITY,
                "127.0.0.1:3181",
                filled(16, (byte) 2),
                "ledger-profile",
                8,
                filled(16, operationIdByte),
                filled(32, (byte) 4),
                null);
    }

    private static CommittedProfileAuthority authority(
            ProfileControlScope scope, CommittedProfileAuthority.State state) {
        return new CommittedProfileAuthority(scope, state, EngineProfile.SEGMENT_WAL_ENGINE, 21, true);
    }

    private static ProtectedProfileStateStore countingStateStore(AtomicInteger effects) {
        return new ProtectedProfileStateStore() {
            @Override
            public TransitionResult conditionalApply(Transition transition) {
                effects.incrementAndGet();
                return new TransitionResult(ProfileControlResult.Outcome.APPLIED, 9, true);
            }

            @Override
            public TransitionResult queryOperationResult(ProfileControlScope scope) {
                effects.incrementAndGet();
                return new TransitionResult(ProfileControlResult.Outcome.NOT_FOUND, 0, false);
            }
        };
    }

    private static byte[] filled(int length, byte value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static final class RecordingStateStore implements ProtectedProfileStateStore {
        private final List<String> events;
        private final AtomicInteger applyCount = new AtomicInteger();
        private Transition lastTransition;

        private RecordingStateStore(List<String> events) {
            this.events = events;
        }

        @Override
        public TransitionResult conditionalApply(Transition transition) {
            events.add("apply");
            applyCount.incrementAndGet();
            lastTransition = transition;
            return new TransitionResult(ProfileControlResult.Outcome.APPLIED, 9, true);
        }

        @Override
        public TransitionResult queryOperationResult(ProfileControlScope scope) {
            throw new AssertionError("query not expected");
        }
    }
}

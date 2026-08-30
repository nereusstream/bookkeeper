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

package org.apache.bookkeeper.common.profile;

import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.IDENTITY_MISMATCH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INPUT_TOO_LARGE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ProfileDescriptorCodecTest {

    private final ProfileDescriptorValidator validator = TestProfileRegistries.validator();

    @Test
    public void roundTripsMinimumSingleAndMaximumCapabilityDescriptors() {
        for (int capabilityCount : List.of(0, 1, 64)) {
            ProfileDescriptor descriptor =
                    TestProfileRegistries.descriptor(TestProfileRegistries.capabilities(capabilityCount));
            byte[] canonicalBytes = ProfileDescriptorCodec.encode(descriptor, validator);

            assertEquals(124 + 6 * capabilityCount, canonicalBytes.length);
            assertEquals(descriptor, ProfileDescriptorCodec.decode(canonicalBytes, validator));
            assertArrayEquals(new byte[] {'B', 'K', 'P', 'D'},
                    new byte[] {canonicalBytes[0], canonicalBytes[1], canonicalBytes[2], canonicalBytes[3]});
            assertEquals(canonicalBytes.length, ByteBuffer.wrap(canonicalBytes, 8, 4).getInt());

            IndependentProfileDescriptorVerifier.Verification independent =
                    IndependentProfileDescriptorVerifier.verify(
                            canonicalBytes,
                            TestProfileRegistries::validatorCapability,
                            TestProfileRegistries::validatorPolicy);
            assertArrayEquals(
                    ProfileDescriptorIdentity.compute(canonicalBytes).toBytes(), independent.identity());
        }
    }

    @Test
    public void identityIsImmutableAndSafetySensitive() {
        ProfileDescriptor descriptor = TestProfileRegistries.descriptor(List.of());
        byte[] canonicalBytes = ProfileDescriptorCodec.encode(descriptor, validator);
        ProfileDescriptorIdentity identity = ProfileDescriptorCodec.identityOf(descriptor, validator);
        byte[] identityBytes = identity.toBytes();
        byte[] digest = identity.digest();

        identityBytes[4] ^= 1;
        digest[0] ^= 1;
        assertEquals(36, identity.toBytes().length);
        assertEquals(identity, ProfileDescriptorIdentity.fromBytes(identity.toBytes()));
        assertTrue(identity.verifies(canonicalBytes));

        ProfileDescriptor changed = new ProfileDescriptor(
                descriptor.requiredEngine(),
                descriptor.payloadFormat(),
                descriptor.durabilityMode(),
                descriptor.ensembleSize(),
                descriptor.writeQuorumSize(),
                descriptor.ackQuorumSize(),
                0,
                descriptor.failureDomainPolicyId(),
                descriptor.failureDomainPolicyGeneration(),
                descriptor.mandatoryCapabilities());
        assertNotEquals(
                identity,
                ProfileDescriptorIdentity.compute(ProfileDescriptorCodec.encode(changed, validator)));
    }

    @Test
    public void declaredIdentityMismatchFailsClosed() {
        byte[] first = ProfileDescriptorCodec.encode(TestProfileRegistries.descriptor(List.of()), validator);
        byte[] second = ProfileDescriptorCodec.encode(
                TestProfileRegistries.descriptor(List.of(TestProfileRegistries.capability(1))), validator);
        ProfileDescriptorValidationException exception = assertThrows(
                ProfileDescriptorValidationException.class,
                () -> ProfileDescriptorCodec.decodeAndVerify(
                        first, ProfileDescriptorIdentity.compute(second), validator));
        assertEquals(IDENTITY_MISMATCH, exception.reason());
    }

    @Test
    public void absoluteInputCapPrecedesCapabilityListAllocation() {
        byte[] oversized = new byte[ProfileDescriptorCodec.ABSOLUTE_INPUT_CAP + 1];
        AtomicInteger allocations = new AtomicInteger();

        ProfileDescriptorValidationException exception = assertThrows(
                ProfileDescriptorValidationException.class,
                () -> ProfileDescriptorCodec.decode(
                        oversized,
                        validator,
                        size -> {
                            allocations.incrementAndGet();
                            return new ArrayList<>(size);
                        }));

        assertEquals(INPUT_TOO_LARGE, exception.reason());
        assertEquals(0, allocations.get());
        assertFalse(exception.getMessage().contains("secret"));
    }
}

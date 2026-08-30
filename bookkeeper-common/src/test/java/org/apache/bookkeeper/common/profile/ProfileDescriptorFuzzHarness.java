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

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/** Fixed-seed property and mutation fuzz harness. */
final class ProfileDescriptorFuzzHarness {

    static final long VALID_SEED = 0x5044_5641_4c49_4401L;
    static final long MUTATION_SEED = 0x5044_4d55_5441_5445L;
    static final long ARBITRARY_SEED = 0x5044_4152_4249_5452L;
    static final int VALID_ITERATIONS = 2_000;
    static final int MUTATION_ITERATIONS = 5_000;
    static final int ARBITRARY_ITERATIONS = 5_000;

    private static final ProfileDescriptorValidator VALIDATOR = TestProfileRegistries.validator();

    private ProfileDescriptorFuzzHarness() {}

    static Summary run() {
        Random validRandom = new Random(VALID_SEED);
        for (int iteration = 0; iteration < VALID_ITERATIONS; iteration++) {
            ProfileDescriptor descriptor = randomDescriptor(validRandom);
            byte[] encoded = ProfileDescriptorCodec.encode(descriptor, VALIDATOR);
            ProfileDescriptor decoded = ProfileDescriptorCodec.decode(encoded, VALIDATOR);
            require(descriptor.equals(decoded), "valid round-trip changed descriptor");
            require(Arrays.equals(encoded, ProfileDescriptorCodec.encode(decoded, VALIDATOR)),
                    "accepted valid input was not byte canonical");
            crossCheckIdentity(encoded);
        }

        Random mutationRandom = new Random(MUTATION_SEED);
        int mutationAccepted = 0;
        int mutationRejected = 0;
        for (int iteration = 0; iteration < MUTATION_ITERATIONS; iteration++) {
            byte[] original = ProfileDescriptorCodec.encode(randomDescriptor(mutationRandom), VALIDATOR);
            byte[] mutated = original.clone();
            int offset = mutationRandom.nextInt(mutated.length);
            mutated[offset] ^= (byte) (1 + mutationRandom.nextInt(255));
            try {
                ProfileDescriptor decoded = ProfileDescriptorCodec.decode(mutated, VALIDATOR);
                require(Arrays.equals(mutated, ProfileDescriptorCodec.encode(decoded, VALIDATOR)),
                        "accepted mutation was normalized");
                require(!MessageDigest.isEqual(
                                ProfileDescriptorIdentity.compute(original).toBytes(),
                                ProfileDescriptorIdentity.compute(mutated).toBytes()),
                        "different canonical bytes retained identity");
                crossCheckIdentity(mutated);
                mutationAccepted++;
            } catch (ProfileDescriptorValidationException expected) {
                mutationRejected++;
            }
        }

        Random arbitraryRandom = new Random(ARBITRARY_SEED);
        int arbitraryAccepted = 0;
        int arbitraryRejected = 0;
        int oversizePreAllocationViolations = 0;
        for (int iteration = 0; iteration < ARBITRARY_ITERATIONS; iteration++) {
            byte[] input = new byte[arbitraryRandom.nextInt(1_101)];
            arbitraryRandom.nextBytes(input);
            AtomicInteger allocations = new AtomicInteger();
            try {
                ProfileDescriptor decoded = ProfileDescriptorCodec.decode(
                        input,
                        VALIDATOR,
                        size -> {
                            allocations.incrementAndGet();
                            return new ArrayList<>(size);
                        });
                require(Arrays.equals(input, ProfileDescriptorCodec.encode(decoded, VALIDATOR)),
                        "accepted arbitrary input was normalized");
                crossCheckIdentity(input);
                arbitraryAccepted++;
            } catch (ProfileDescriptorValidationException expected) {
                arbitraryRejected++;
            }
            if (input.length > ProfileDescriptorCodec.ABSOLUTE_INPUT_CAP && allocations.get() != 0) {
                oversizePreAllocationViolations++;
            }
        }
        require(mutationAccepted + mutationRejected == MUTATION_ITERATIONS, "mutation accounting mismatch");
        require(arbitraryAccepted + arbitraryRejected == ARBITRARY_ITERATIONS, "arbitrary accounting mismatch");
        require(oversizePreAllocationViolations == 0, "oversize input allocated capability body");
        return new Summary(
                mutationAccepted,
                mutationRejected,
                arbitraryAccepted,
                arbitraryRejected,
                oversizePreAllocationViolations);
    }

    static String resultsJson() {
        Summary summary = run();
        return "{\n"
                + "  \"_license\": \"" + ProfileDescriptorCorpusFixtures.LICENSE + "\",\n"
                + "  \"status\": \"PASS\",\n"
                + "  \"valid\": {\"seedHex\":\"" + Long.toHexString(VALID_SEED)
                + "\",\"iterations\":" + VALID_ITERATIONS + ",\"accepted\":" + VALID_ITERATIONS + "},\n"
                + "  \"mutation\": {\"seedHex\":\"" + Long.toHexString(MUTATION_SEED)
                + "\",\"iterations\":" + MUTATION_ITERATIONS
                + ",\"acceptedCanonical\":" + summary.mutationAccepted()
                + ",\"rejected\":" + summary.mutationRejected() + "},\n"
                + "  \"arbitrary\": {\"seedHex\":\"" + Long.toHexString(ARBITRARY_SEED)
                + "\",\"iterations\":" + ARBITRARY_ITERATIONS
                + ",\"acceptedCanonical\":" + summary.arbitraryAccepted()
                + ",\"rejected\":" + summary.arbitraryRejected() + "},\n"
                + "  \"oversizePreAllocationViolations\": "
                + summary.oversizePreAllocationViolations() + "\n"
                + "}\n";
    }

    private static ProfileDescriptor randomDescriptor(Random random) {
        int ensemble = 1 + random.nextInt(0xffff);
        int writeQuorum = 1 + random.nextInt(ensemble);
        int ackQuorum = 1 + random.nextInt(writeQuorum);
        int lossBudget = random.nextInt(ackQuorum);
        long policyId = random.nextBoolean()
                ? TestProfileRegistries.TEST_POLICY_ID
                : TestProfileRegistries.TEST_POLICY_ID_UNSIGNED_MAX;
        long policyGeneration = random.nextLong();
        if (policyGeneration == 0) {
            policyGeneration = 1;
        }
        int capabilityCount = random.nextInt(ProfileDescriptorValidator.MAX_CAPABILITIES + 1);
        TreeSet<Integer> ordinals = new TreeSet<>();
        while (ordinals.size() < capabilityCount) {
            ordinals.add(1 + random.nextInt(ProfileDescriptorValidator.MAX_CAPABILITIES));
        }
        List<ProfileCapability> capabilities = ordinals.stream()
                .map(TestProfileRegistries::capability)
                .toList();
        return new ProfileDescriptor(
                EngineProfile.values()[random.nextInt(EngineProfile.values().length)],
                PayloadFormat.values()[random.nextInt(PayloadFormat.values().length)],
                DurabilityMode.values()[random.nextInt(DurabilityMode.values().length)],
                ensemble,
                writeQuorum,
                ackQuorum,
                lossBudget,
                policyId,
                policyGeneration,
                capabilities);
    }

    private static void crossCheckIdentity(byte[] bytes) {
        IndependentProfileDescriptorVerifier.Verification independent =
                IndependentProfileDescriptorVerifier.verify(
                        bytes,
                        TestProfileRegistries::validatorCapability,
                        TestProfileRegistries::validatorPolicy);
        require(MessageDigest.isEqual(
                        independent.identity(), ProfileDescriptorIdentity.compute(bytes).toBytes()),
                "independent identity mismatch");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    record Summary(
            int mutationAccepted,
            int mutationRejected,
            int arbitraryAccepted,
            int arbitraryRejected,
            int oversizePreAllocationViolations) {}
}

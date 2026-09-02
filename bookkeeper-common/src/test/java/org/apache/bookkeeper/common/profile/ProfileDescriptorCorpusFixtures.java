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

import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.BAD_MAGIC;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.DUPLICATE_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.DUPLICATE_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.IDENTITY_MISMATCH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INPUT_TOO_LARGE;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_LOSS_BUDGET;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_POLICY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.INVALID_QUORUM;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.MISSING_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NONZERO_FLAGS;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.NON_CANONICAL_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.OUT_OF_ORDER_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.TRUNCATED;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_CAPABILITY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_ENUM;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_FIELD;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_POLICY;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNKNOWN_TYPE;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNSUPPORTED_CODEC_VERSION;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.UNSUPPORTED_SCHEMA_VERSION;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_CAPABILITY_SET_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_FIELD_COUNT;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_TOTAL_LENGTH;
import static org.apache.bookkeeper.common.profile.ProfileDescriptorValidationException.Reason.WRONG_VALUE_LENGTH;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Deterministic authoritative corpus builder. */
final class ProfileDescriptorCorpusFixtures {

    static final String LICENSE = "Licensed to the Apache Software Foundation (ASF) under one or more "
            + "contributor license agreements. See the NOTICE file distributed with this work for additional "
            + "information regarding copyright ownership. The ASF licenses this file to you under the Apache "
            + "License, Version 2.0.";

    private static final ProfileDescriptorValidator VALIDATOR = TestProfileRegistries.validator();

    private ProfileDescriptorCorpusFixtures() {}

    static Map<String, ProfileDescriptor> validDescriptors() {
        Map<String, ProfileDescriptor> descriptors = new LinkedHashMap<>();
        descriptors.put("minimal", new ProfileDescriptor(
                EngineProfile.CLASSIC_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                1,
                1,
                1,
                0,
                TestProfileRegistries.TEST_POLICY_ID,
                1,
                List.of()));
        descriptors.put("single-capability", TestProfileRegistries.descriptor(
                List.of(TestProfileRegistries.capability(1))));
        descriptors.put("quorum-boundary", new ProfileDescriptor(
                EngineProfile.SEGMENT_WAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                3,
                2,
                1,
                0,
                TestProfileRegistries.TEST_POLICY_ID,
                2,
                TestProfileRegistries.capabilities(2)));
        descriptors.put("unsigned-policy-generation", new ProfileDescriptor(
                EngineProfile.DIRECT_JOURNAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.DEFERRED_SYNC_LEGACY,
                5,
                3,
                2,
                1,
                TestProfileRegistries.TEST_POLICY_ID_UNSIGNED_MAX,
                -1L,
                TestProfileRegistries.capabilities(3)));
        descriptors.put("unsigned-capability", new ProfileDescriptor(
                EngineProfile.CLASSIC_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                3,
                3,
                3,
                2,
                TestProfileRegistries.TEST_POLICY_ID,
                0xffff_ffff_ffff_ffffL,
                List.of(new ProfileCapability(
                        TestProfileRegistries.TEST_CAPABILITY_UNSIGNED_MAX, 0xffff))));
        descriptors.put("maximal-capabilities", new ProfileDescriptor(
                EngineProfile.SEGMENT_WAL_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                0xffff,
                0xffff,
                0xffff,
                0xfffe,
                TestProfileRegistries.TEST_POLICY_ID_UNSIGNED_MAX,
                -1L,
                TestProfileRegistries.capabilities(64)));
        return descriptors;
    }

    static Map<String, byte[]> validBytes() {
        Map<String, byte[]> encoded = new LinkedHashMap<>();
        validDescriptors().forEach((name, descriptor) ->
                encoded.put(name, ProfileDescriptorCodec.encode(descriptor, VALIDATOR)));
        return encoded;
    }

    static List<InvalidVector> invalidVectors() {
        Map<String, byte[]> valid = validBytes();
        byte[] minimal = valid.get("minimal");
        byte[] single = valid.get("single-capability");
        byte[] maximum = valid.get("maximal-capabilities");
        List<InvalidVector> vectors = new ArrayList<>();

        vectors.add(mutate("bad-magic", minimal, BAD_MAGIC, bytes -> bytes[0] = 0));
        vectors.add(mutate("unknown-codec-version", minimal, UNSUPPORTED_CODEC_VERSION,
                bytes -> putU16(bytes, 4, 2)));
        vectors.add(mutate("zero-semantic-schema", minimal, UNSUPPORTED_SCHEMA_VERSION,
                bytes -> putU16(bytes, 6, 0)));
        vectors.add(mutate("unknown-semantic-schema", minimal, UNSUPPORTED_SCHEMA_VERSION,
                bytes -> putU16(bytes, 6, 2)));
        vectors.add(mutate("wrong-total-length", minimal, WRONG_TOTAL_LENGTH,
                bytes -> putU32(bytes, 8, bytes.length - 1)));
        vectors.add(mutate("missing-field-count", minimal, WRONG_FIELD_COUNT,
                bytes -> putU16(bytes, 12, 9)));
        vectors.add(mutate("nonzero-flags", minimal, NONZERO_FLAGS,
                bytes -> putU16(bytes, 14, 1)));
        vectors.add(mutate("duplicate-field", minimal, DUPLICATE_FIELD,
                bytes -> putU16(bytes, 56, 4)));
        vectors.add(mutate("out-of-order-field", minimal, OUT_OF_ORDER_FIELD,
                bytes -> putU16(bytes, 56, 3)));
        vectors.add(mutate("missing-field", minimal, MISSING_FIELD,
                bytes -> putU16(bytes, 56, 6)));
        vectors.add(mutate("unknown-field", minimal, UNKNOWN_FIELD,
                bytes -> putU16(bytes, 56, 11)));
        vectors.add(mutate("unknown-type", minimal, UNKNOWN_TYPE,
                bytes -> putU16(bytes, 18, 5)));
        vectors.add(mutate("wrong-scalar-length", minimal, WRONG_VALUE_LENGTH,
                bytes -> putU32(bytes, 20, 1)));
        vectors.add(mutate("default-alias", minimal, UNKNOWN_ENUM,
                bytes -> putU16(bytes, 24, 0)));
        vectors.add(mutate("unknown-engine", minimal, UNKNOWN_ENUM,
                bytes -> putU16(bytes, 24, 4)));
        vectors.add(mutate("unknown-payload", minimal, UNKNOWN_ENUM,
                bytes -> putU16(bytes, 34, 2)));
        vectors.add(mutate("unknown-durability", minimal, UNKNOWN_ENUM,
                bytes -> putU16(bytes, 44, 3)));
        vectors.add(mutate("zero-ensemble", minimal, INVALID_QUORUM,
                bytes -> putU16(bytes, 54, 0)));
        vectors.add(mutate("write-quorum-over-ensemble", minimal, INVALID_QUORUM,
                bytes -> putU16(bytes, 64, 2)));
        vectors.add(mutate("ack-quorum-over-write-quorum", minimal, INVALID_QUORUM,
                bytes -> putU16(bytes, 74, 2)));
        vectors.add(mutate("loss-budget-not-below-ack", minimal, INVALID_LOSS_BUDGET,
                bytes -> putU16(bytes, 84, 1)));
        vectors.add(mutate("zero-policy-id", minimal, INVALID_POLICY,
                bytes -> putU32(bytes, 94, 0)));
        vectors.add(mutate("unknown-policy-id", minimal, UNKNOWN_POLICY,
                bytes -> putU32(bytes, 94, 1)));
        vectors.add(mutate("zero-policy-generation", minimal, INVALID_POLICY,
                bytes -> Arrays.fill(bytes, 106, 114, (byte) 0)));
        vectors.add(mutate("zero-capability-id", single, INVALID_CAPABILITY,
                bytes -> putU32(bytes, 124, 0)));
        vectors.add(mutate("zero-capability-version", single, INVALID_CAPABILITY,
                bytes -> putU16(bytes, 128, 0)));
        vectors.add(mutate("unknown-capability", single, UNKNOWN_CAPABILITY,
                bytes -> putU32(bytes, 124, 1)));
        vectors.add(mutate("unknown-capability-version", single, UNKNOWN_CAPABILITY,
                bytes -> putU16(bytes, 128, 2)));

        byte[] twoCapabilities = ProfileDescriptorCodec.encode(
                TestProfileRegistries.descriptor(TestProfileRegistries.capabilities(2)), VALIDATOR);
        vectors.add(mutate("duplicate-capability", twoCapabilities, DUPLICATE_CAPABILITY,
                bytes -> putU32(bytes, 130, TestProfileRegistries.capability(1).capabilityId())));
        vectors.add(mutate("out-of-order-capability", twoCapabilities, OUT_OF_ORDER_CAPABILITY,
                bytes -> {
                    putU32(bytes, 124, TestProfileRegistries.capability(2).capabilityId());
                    putU32(bytes, 130, TestProfileRegistries.capability(1).capabilityId());
                }));
        vectors.add(mutate("wrong-capability-set-length", single, WRONG_CAPABILITY_SET_LENGTH,
                bytes -> putU16(bytes, 122, 0)));
        vectors.add(mutate("truncated-capability-set", minimal, TRUNCATED,
                bytes -> putU32(bytes, 118, 8)));

        byte[] count65 = Arrays.copyOf(maximum, 514);
        putU32(count65, 8, count65.length);
        putU32(count65, 118, 392);
        putU16(count65, 122, 65);
        putU32(count65, 508, TestProfileRegistries.TEST_CAPABILITY_BASE + 65);
        putU16(count65, 512, 1);
        vectors.add(new InvalidVector("capability-count-65", count65, NON_CANONICAL_LENGTH, null));

        vectors.add(new InvalidVector("truncated-header", Arrays.copyOf(minimal, 15), TRUNCATED, null));
        byte[] trailing = Arrays.copyOf(minimal, 125);
        trailing[124] = 1;
        vectors.add(new InvalidVector("trailing-byte", trailing, WRONG_TOTAL_LENGTH, null));
        byte[] padding = Arrays.copyOf(minimal, 125);
        putU32(padding, 8, padding.length);
        vectors.add(new InvalidVector("padding-byte", padding, NON_CANONICAL_LENGTH, null));
        byte[] length509 = Arrays.copyOf(maximum, 509);
        putU32(length509, 8, length509.length);
        vectors.add(new InvalidVector("noncanonical-509", length509, NON_CANONICAL_LENGTH, null));
        byte[] length1024 = Arrays.copyOf(maximum, 1024);
        putU32(length1024, 8, length1024.length);
        vectors.add(new InvalidVector("noncanonical-1024", length1024, NON_CANONICAL_LENGTH, null));
        byte[] length1025 = new byte[1025];
        byte[] marker = "test-secret-marker".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(marker, 0, length1025, 0, marker.length);
        vectors.add(new InvalidVector("oversize-1025", length1025, INPUT_TOO_LARGE, null));

        byte[] mismatchedIdentity = ProfileDescriptorIdentity.compute(minimal).toBytes();
        mismatchedIdentity[mismatchedIdentity.length - 1] ^= 1;
        vectors.add(new InvalidVector(
                "declared-identity-mismatch", minimal.clone(), IDENTITY_MISMATCH, mismatchedIdentity));
        return vectors;
    }

    static Map<String, byte[]> corpusFiles() {
        Map<String, byte[]> files = new LinkedHashMap<>();
        validBytes().forEach((name, bytes) -> files.put("valid/" + name + ".bin", bytes));
        invalidVectors().forEach(vector -> files.put("invalid/" + vector.name() + ".bin", vector.bytes()));
        files.put("fixtures.json", fixturesJson().getBytes(StandardCharsets.UTF_8));
        files.put("invalid/manifest.json", invalidManifestJson().getBytes(StandardCharsets.UTF_8));
        files.put("expected/identities.json", identitiesJson().getBytes(StandardCharsets.UTF_8));
        files.put("expected/field-dumps.json", fieldDumpsJson().getBytes(StandardCharsets.UTF_8));
        files.put("expected/fuzz-results.json",
                ProfileDescriptorFuzzHarness.resultsJson().getBytes(StandardCharsets.UTF_8));
        return files;
    }

    static String checksums(Map<String, byte[]> files) {
        StringBuilder result = new StringBuilder();
        result.append("# Licensed to the Apache Software Foundation (ASF) under one or more\n")
                .append("# contributor license agreements. See the NOTICE file distributed with\n")
                .append("# this work for additional information regarding copyright ownership.\n")
                .append("# The ASF licenses this file to you under the Apache License, Version 2.0\n")
                .append("# (the License); you may not use this file except in compliance with\n")
                .append("# the License. You may obtain a copy of the License at\n")
                .append("#\n")
                .append("# http://www.apache.org/licenses/LICENSE-2.0\n")
                .append("#\n")
                .append("# Unless required by applicable law or agreed to in writing, software\n")
                .append("# distributed under the License is distributed on an AS IS BASIS,\n")
                .append("# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
                .append("# See the License for the specific language governing permissions and\n")
                .append("# limitations under the License.\n");
        files.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                result.append(hex(sha256(entry.getValue()))).append("  ").append(entry.getKey()).append('\n'));
        return result.toString();
    }

    private static String fixturesJson() {
        StringBuilder json = jsonHeader();
        json.append("  \"fixtures\": {\n");
        int descriptorIndex = 0;
        for (Map.Entry<String, ProfileDescriptor> entry : validDescriptors().entrySet()) {
            ProfileDescriptor descriptor = entry.getValue();
            json.append("    \"").append(entry.getKey()).append("\": {")
                    .append("\"requiredEngine\":\"").append(descriptor.requiredEngine()).append("\",")
                    .append("\"payloadFormat\":\"").append(descriptor.payloadFormat()).append("\",")
                    .append("\"durabilityMode\":\"").append(descriptor.durabilityMode()).append("\",")
                    .append("\"ensembleSize\":").append(descriptor.ensembleSize()).append(',')
                    .append("\"writeQuorumSize\":").append(descriptor.writeQuorumSize()).append(',')
                    .append("\"ackQuorumSize\":").append(descriptor.ackQuorumSize()).append(',')
                    .append("\"permanentLossBudget\":").append(descriptor.permanentLossBudget()).append(',')
                    .append("\"failureDomainPolicyId\":\"")
                    .append(Long.toUnsignedString(descriptor.failureDomainPolicyId())).append("\",")
                    .append("\"failureDomainPolicyGeneration\":\"")
                    .append(Long.toUnsignedString(descriptor.failureDomainPolicyGeneration())).append("\",")
                    .append("\"mandatoryCapabilities\":[");
            for (int capabilityIndex = 0;
                    capabilityIndex < descriptor.mandatoryCapabilities().size(); capabilityIndex++) {
                if (capabilityIndex > 0) {
                    json.append(',');
                }
                ProfileCapability capability = descriptor.mandatoryCapabilities().get(capabilityIndex);
                json.append("{\"capabilityId\":\"").append(capability.capabilityId())
                        .append("\",\"semanticVersion\":").append(capability.semanticVersion()).append('}');
            }
            json.append("]}");
            if (++descriptorIndex < validDescriptors().size()) {
                json.append(',');
            }
            json.append('\n');
        }
        return json.append("  }\n}\n").toString();
    }

    private static String invalidManifestJson() {
        StringBuilder json = jsonHeader();
        json.append("  \"vectors\": [\n");
        List<InvalidVector> vectors = invalidVectors();
        for (int index = 0; index < vectors.size(); index++) {
            InvalidVector vector = vectors.get(index);
            json.append("    {\"file\":\"").append(vector.name()).append(".bin\",")
                    .append("\"expectedReason\":\"").append(vector.reason()).append('"');
            if (vector.declaredIdentity() != null) {
                json.append(",\"declaredIdentityHex\":\"")
                        .append(hex(vector.declaredIdentity())).append('"');
            }
            json.append('}');
            if (index + 1 < vectors.size()) {
                json.append(',');
            }
            json.append('\n');
        }
        return json.append("  ]\n}\n").toString();
    }

    private static String identitiesJson() {
        StringBuilder json = jsonHeader();
        json.append("  \"vectors\": {\n");
        Map<String, byte[]> valid = validBytes();
        int index = 0;
        for (Map.Entry<String, byte[]> entry : valid.entrySet()) {
            IndependentProfileDescriptorVerifier.Verification verification = verify(entry.getValue());
            json.append("    \"").append(entry.getKey()).append(".bin\": {")
                    .append("\"length\":").append(entry.getValue().length).append(',')
                    .append("\"sha256\":\"").append(hex(verification.digest())).append("\",")
                    .append("\"identityHex\":\"").append(hex(verification.identity())).append("\"}");
            if (++index < valid.size()) {
                json.append(',');
            }
            json.append('\n');
        }
        return json.append("  }\n}\n").toString();
    }

    private static String fieldDumpsJson() {
        StringBuilder json = jsonHeader();
        json.append("  \"vectors\": {\n");
        Map<String, byte[]> valid = validBytes();
        int index = 0;
        for (Map.Entry<String, byte[]> entry : valid.entrySet()) {
            json.append("    \"").append(entry.getKey()).append(".bin\": \"")
                    .append(escapeJson(verify(entry.getValue()).fieldDump())).append('"');
            if (++index < valid.size()) {
                json.append(',');
            }
            json.append('\n');
        }
        return json.append("  }\n}\n").toString();
    }

    private static IndependentProfileDescriptorVerifier.Verification verify(byte[] bytes) {
        return IndependentProfileDescriptorVerifier.verify(
                bytes, TestProfileRegistries::validatorCapability, TestProfileRegistries::validatorPolicy);
    }

    private static StringBuilder jsonHeader() {
        return new StringBuilder("{\n  \"_license\": \"").append(LICENSE).append("\",\n");
    }

    private static String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static InvalidVector mutate(
            String name,
            byte[] source,
            ProfileDescriptorValidationException.Reason reason,
            ByteMutation mutation) {
        byte[] bytes = source.clone();
        mutation.mutate(bytes);
        return new InvalidVector(name, bytes, reason, null);
    }

    private static void putU16(byte[] bytes, int offset, long value) {
        bytes[offset] = (byte) (value >>> 8);
        bytes[offset + 1] = (byte) value;
    }

    private static void putU32(byte[] bytes, int offset, long value) {
        ByteBuffer.wrap(bytes, offset, 4).putInt((int) value);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException exception) {
            throw new AssertionError(exception);
        }
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    @FunctionalInterface
    private interface ByteMutation {
        void mutate(byte[] bytes);
    }

    record InvalidVector(
            String name,
            byte[] bytes,
            ProfileDescriptorValidationException.Reason reason,
            byte[] declaredIdentity) {

        InvalidVector {
            bytes = bytes.clone();
            declaredIdentity = declaredIdentity == null ? null : declaredIdentity.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        @Override
        public byte[] declaredIdentity() {
            return declaredIdentity == null ? null : declaredIdentity.clone();
        }
    }
}

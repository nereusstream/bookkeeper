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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProfileDescriptorCorpusTest {

    private static final String UPDATE_PROPERTY = "profileDescriptor.updateCorpus";
    private static final Path CORPUS_DIRECTORY =
            Path.of("src", "test", "resources", "profile-descriptor");
    private static final ProfileDescriptorValidator VALIDATOR = TestProfileRegistries.validator();

    @BeforeClass
    public static void generateCorpusWhenExplicitlyRequested() throws IOException {
        if (Boolean.getBoolean(UPDATE_PROPERTY)) {
            Map<String, byte[]> files = ProfileDescriptorCorpusFixtures.corpusFiles();
            writeCorpus(files, ProfileDescriptorCorpusFixtures.checksums(files));
        }
    }

    @Test
    public void committedCorpusMatchesDeterministicReferenceBuilder() throws Exception {
        Map<String, byte[]> files = ProfileDescriptorCorpusFixtures.corpusFiles();
        String checksums = ProfileDescriptorCorpusFixtures.checksums(files);

        for (Map.Entry<String, byte[]> entry : files.entrySet()) {
            assertArrayEquals(entry.getKey(), entry.getValue(), resource(entry.getKey()));
        }
        assertEquals(checksums, new String(resource("checksums.txt"), StandardCharsets.UTF_8));
    }

    @Test
    public void validCorpusCrossChecksProductionAndIndependentImplementations() throws Exception {
        for (Map.Entry<String, ProfileDescriptor> entry
                : ProfileDescriptorCorpusFixtures.validDescriptors().entrySet()) {
            byte[] bytes = resource("valid/" + entry.getKey() + ".bin");
            ProfileDescriptor decoded = ProfileDescriptorCodec.decode(bytes, VALIDATOR);
            assertEquals(entry.getValue(), decoded);
            assertArrayEquals(bytes, ProfileDescriptorCodec.encode(decoded, VALIDATOR));

            IndependentProfileDescriptorVerifier.Verification verification =
                    IndependentProfileDescriptorVerifier.verify(
                            bytes,
                            TestProfileRegistries::validatorCapability,
                            TestProfileRegistries::validatorPolicy);
            assertArrayEquals(
                    ProfileDescriptorIdentity.compute(bytes).toBytes(), verification.identity());
        }
    }

    @Test
    public void everyInvalidCorpusVectorFailsClosed() throws Exception {
        for (ProfileDescriptorCorpusFixtures.InvalidVector vector
                : ProfileDescriptorCorpusFixtures.invalidVectors()) {
            byte[] bytes = resource("invalid/" + vector.name() + ".bin");
            ProfileDescriptorValidationException exception;
            if (vector.declaredIdentity() == null) {
                exception = assertThrows(
                        vector.name(),
                        ProfileDescriptorValidationException.class,
                        () -> ProfileDescriptorCodec.decode(bytes, VALIDATOR));
                assertThrows(
                        vector.name(),
                        IllegalArgumentException.class,
                        () -> IndependentProfileDescriptorVerifier.verify(
                                bytes,
                                TestProfileRegistries::validatorCapability,
                                TestProfileRegistries::validatorPolicy));
            } else {
                ProfileDescriptorIdentity declared =
                        ProfileDescriptorIdentity.fromBytes(vector.declaredIdentity());
                exception = assertThrows(
                        vector.name(),
                        ProfileDescriptorValidationException.class,
                        () -> ProfileDescriptorCodec.decodeAndVerify(bytes, declared, VALIDATOR));
                IndependentProfileDescriptorVerifier.Verification verification =
                        IndependentProfileDescriptorVerifier.verify(
                                bytes,
                                TestProfileRegistries::validatorCapability,
                                TestProfileRegistries::validatorPolicy);
                assertFalse(java.security.MessageDigest.isEqual(
                        vector.declaredIdentity(), verification.identity()));
            }
            assertEquals(vector.name(), vector.reason(), exception.reason());
            assertFalse(vector.name(), exception.getMessage().contains("test-secret-marker"));
        }
    }

    @Test
    public void malformedIdentityEncodingsFailClosedWithoutEchoingInput() {
        for (byte[] bytes : new byte[][] {new byte[35], new byte[37], identityWith(0, 1), identityWith(1, 0)}) {
            ProfileDescriptorValidationException exception = assertThrows(
                    ProfileDescriptorValidationException.class,
                    () -> ProfileDescriptorIdentity.fromBytes(bytes));
            assertEquals(ProfileDescriptorValidationException.Reason.INVALID_IDENTITY, exception.reason());
            assertFalse(exception.getMessage().contains("secret"));
        }
    }

    @Test
    public void noncanonicalAndOversizeLengthsFailBeforeCapabilityBodyAllocation() throws Exception {
        for (String name : new String[] {"noncanonical-509", "noncanonical-1024", "oversize-1025"}) {
            AtomicInteger allocations = new AtomicInteger();
            byte[] bytes = resource("invalid/" + name + ".bin");
            assertThrows(
                    name,
                    ProfileDescriptorValidationException.class,
                    () -> ProfileDescriptorCodec.decode(
                            bytes,
                            VALIDATOR,
                            size -> {
                                allocations.incrementAndGet();
                                return new java.util.ArrayList<>(size);
                            }));
            assertEquals(name, 0, allocations.get());
        }
    }

    private static byte[] identityWith(int suite, int schema) {
        byte[] identity = new byte[36];
        identity[1] = (byte) suite;
        identity[3] = (byte) schema;
        return identity;
    }

    private static void writeCorpus(Map<String, byte[]> files, String checksums) throws IOException {
        for (Map.Entry<String, byte[]> entry : files.entrySet()) {
            Path target = CORPUS_DIRECTORY.resolve(entry.getKey());
            Files.createDirectories(target.getParent());
            Files.write(target, entry.getValue());
        }
        Files.writeString(
                CORPUS_DIRECTORY.resolve("checksums.txt"), checksums, StandardCharsets.UTF_8);
    }

    private static byte[] resource(String relativePath) throws IOException {
        if (Boolean.getBoolean(UPDATE_PROPERTY)) {
            return Files.readAllBytes(CORPUS_DIRECTORY.resolve(relativePath));
        }
        try (InputStream input = ProfileDescriptorCorpusTest.class.getResourceAsStream(
                "/profile-descriptor/" + relativePath)) {
            if (input == null) {
                throw new IOException("missing corpus resource: " + relativePath);
            }
            return input.readAllBytes();
        }
    }
}

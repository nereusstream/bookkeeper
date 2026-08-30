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

package org.apache.bookkeeper.common.profile.wire;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ProfileWireCorpusTest {

    private static final String ROOT = "profile-wire/";

    @Test
    public void committedCorpusIsByteExactAndMatchesTypedExpectations() throws Exception {
        List<ProfileWireCorpusFixtures.Vector> vectors = vectors();
        for (ProfileWireCorpusFixtures.Vector vector : vectors) {
            byte[] committed = resource(vector.resourcePath());
            assertArrayEquals(vector.id(), vector.bytes(), committed);
            verifyVector(vector, committed);
        }
        assertEquals(ProfileWireCorpusFixtures.FUZZ_ITERATIONS,
                vectors.stream().filter(vector -> vector.id().startsWith("fuzz/")).count());
    }

    @Test
    public void fixtureAndManifestFilesAreGeneratedFromTheSameTypedVectors() throws Exception {
        List<ProfileWireCorpusFixtures.Vector> vectors = vectors();
        assertEquals(ProfileWireCorpusGenerator.fixtures(vectors), resourceText("fixtures.tsv"));
        assertEquals(ProfileWireCorpusGenerator.manifest(vectors), resourceText("manifest.json"));
    }

    @Test
    public void everyCommittedCorpusChecksumMatches() throws Exception {
        String[] lines = resourceText("checksums.sha256").split("\\R");
        int checked = 0;
        for (String line : lines) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("  ", 2);
            assertEquals(fields[0], sha256(resource(fields[1])));
            checked++;
        }
        assertEquals(vectors().size() + 2, checked);
    }

    private static void verifyVector(ProfileWireCorpusFixtures.Vector vector, byte[] bytes) {
        AtomicInteger allocations = new AtomicInteger();
        if (vector.frameResult() == ProfileWireCorpusFixtures.FrameResult.REJECT) {
            ProfileWireValidationException exception = assertThrows(
                    vector.id(),
                    ProfileWireValidationException.class,
                    () -> ProfileFrameCodec.decode(
                            bytes,
                            vector.phase().decodePhase(),
                            size -> {
                                allocations.incrementAndGet();
                                return new byte[size];
                            }));
            assertEquals(vector.id(), vector.frameReason(), exception.reason().name());
            assertEquals(vector.id(), 0, allocations.get());
            assertEquals(vector.id(), true, vector.close());
            return;
        }
        ProfileFrame frame = ProfileFrameCodec.decode(
                bytes,
                vector.phase().decodePhase(),
                size -> {
                    allocations.incrementAndGet();
                    return new byte[size];
                });
        assertEquals(vector.id(), 1, allocations.get());
        verifyTyped(vector, frame);
    }

    private static void verifyTyped(ProfileWireCorpusFixtures.Vector vector, ProfileFrame frame) {
        switch (vector.typed()) {
            case NONE, SKIP -> {
                return;
            }
            case HELLO_CLIENT -> ProfileHello.decodeClient(frame.body());
            case HELLO_SERVER -> ProfileHello.decodeServer(frame.body());
            case DATA -> ProfileOperationCodec.decode(frame.header().operation(), frame.body());
            case BLOCKED_CONTROL -> assertTypedReason(
                    vector,
                    () -> ProfileOperationCodec.decode(frame.header().operation(), frame.body()));
            case UNSUPPORTED -> {
                ProfileOperationCodec.Unsupported unsupported = (ProfileOperationCodec.Unsupported)
                        ProfileOperationCodec.decode(frame.header().operation(), frame.body());
                assertEquals(vector.id(), vector.typedExpectation(), statusTuple(unsupported.status()));
            }
            case STATUS -> assertEquals(
                    vector.id(), vector.typedExpectation(), statusTuple(ProfileStatus.decode(frame.body())));
            case HELLO_CLIENT_REJECT -> assertTypedReason(vector, () -> ProfileHello.decodeClient(frame.body()));
            case HELLO_SERVER_REJECT -> assertTypedReason(vector, () -> ProfileHello.decodeServer(frame.body()));
            case DATA_REJECT -> assertTypedReason(
                    vector,
                    () -> ProfileOperationCodec.decode(frame.header().operation(), frame.body()));
            case STATUS_REJECT -> assertTypedReason(vector, () -> ProfileStatus.decode(frame.body()));
        }
    }

    private static void assertTypedReason(ProfileWireCorpusFixtures.Vector vector, Runnable action) {
        ProfileWireValidationException exception = assertThrows(
                vector.id(), ProfileWireValidationException.class, action::run);
        assertEquals(vector.id(), vector.typedExpectation(), exception.reason().name());
        if (vector.typed() != ProfileWireCorpusFixtures.Typed.BLOCKED_CONTROL) {
            assertEquals(vector.id(), true, vector.close());
        }
    }

    private static String statusTuple(ProfileStatus status) {
        return status.statusClass().wireValue() + "/" + status.retryDisposition().wireValue() + "/"
                + status.durableResult().wireValue() + "/" + status.detailCode();
    }

    private static List<ProfileWireCorpusFixtures.Vector> vectors() {
        return ProfileWireCorpusFixtures.allVectors().stream()
                .sorted(Comparator.comparing(ProfileWireCorpusFixtures.Vector::id))
                .toList();
    }

    private static byte[] resource(String relativePath) throws IOException {
        ClassLoader classLoader = ProfileWireCorpusTest.class.getClassLoader();
        try (InputStream input = classLoader.getResourceAsStream(ROOT + relativePath)) {
            assertNotNull(ROOT + relativePath, input);
            return input.readAllBytes();
        }
    }

    private static String resourceText(String relativePath) throws IOException {
        return new String(resource(relativePath), StandardCharsets.UTF_8);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}

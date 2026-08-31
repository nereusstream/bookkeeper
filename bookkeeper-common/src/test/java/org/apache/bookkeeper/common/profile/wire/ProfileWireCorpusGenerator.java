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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Writes the authoritative corpus bytes and their machine-readable expectations. */
public final class ProfileWireCorpusGenerator {

    private ProfileWireCorpusGenerator() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("usage: ProfileWireCorpusGenerator <output-directory>");
        }
        write(Path.of(arguments[0]));
    }

    static void write(Path output) throws IOException {
        cleanOwnedOutput(output);
        Files.createDirectories(output);
        List<ProfileWireCorpusFixtures.Vector> vectors = ProfileWireCorpusFixtures.allVectors().stream()
                .sorted(Comparator.comparing(ProfileWireCorpusFixtures.Vector::id))
                .toList();
        for (ProfileWireCorpusFixtures.Vector vector : vectors) {
            Path path = output.resolve(vector.resourcePath());
            Files.createDirectories(path.getParent());
            Files.write(path, vector.bytes());
        }
        Files.writeString(output.resolve("fixtures.tsv"), fixtures(vectors), StandardCharsets.UTF_8);
        Files.writeString(output.resolve("manifest.json"), manifest(vectors), StandardCharsets.UTF_8);
        Files.writeString(output.resolve("checksums.sha256"), checksums(output), StandardCharsets.UTF_8);
    }

    static String fixtures(List<ProfileWireCorpusFixtures.Vector> vectors) {
        StringBuilder result = new StringBuilder();
        result.append(licenseHeader());
        result.append("# id\tpath\tphase\tframeResult\tframeReason\ttyped\ttypedExpectation\tclose\tbytes\n");
        for (ProfileWireCorpusFixtures.Vector vector : vectors) {
            result.append(vector.fixtureLine()).append('\n');
        }
        return result.toString();
    }

    static String manifest(List<ProfileWireCorpusFixtures.Vector> vectors) {
        Map<String, Integer> categories = new LinkedHashMap<>();
        Map<String, Integer> frameResults = new LinkedHashMap<>();
        for (ProfileWireCorpusFixtures.Vector vector : vectors) {
            categories.merge(vector.id().substring(0, vector.id().indexOf('/')), 1, Integer::sum);
            frameResults.merge(vector.frameResult().name(), 1, Integer::sum);
        }
        return "{\n"
                + "  \"_license\": \"" + ProfileWireCorpusFixtures.LICENSE + "\",\n"
                + "  \"state\": [\"EXPERIMENTAL_TEST_MANIFEST\", \"NOT_STABLE_WIRE\", "
                + "\"NON_PROMOTABLE\", \"NO_AUTHORITY\", \"DISCARDABLE\"],\n"
                + "  \"codec\": \"ProfileFrame/0x0FF04250/protocol-1.0/header-32\",\n"
                + "  \"fixtureSchema\": "
                + "\"id,path,phase,frameResult,frameReason,typed,typedExpectation,close,bytes\",\n"
                + "  \"fuzzSeedHex\": \"" + Long.toHexString(ProfileWireCorpusFixtures.FUZZ_SEED) + "\",\n"
                + "  \"fuzzIterations\": " + ProfileWireCorpusFixtures.FUZZ_ITERATIONS + ",\n"
                + "  \"vectorCount\": " + vectors.size() + ",\n"
                + "  \"categories\": " + jsonMap(categories) + ",\n"
                + "  \"frameResults\": " + jsonMap(frameResults) + "\n"
                + "}\n";
    }

    private static String jsonMap(Map<String, Integer> values) {
        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Integer> entry : values.entrySet()) {
            if (!first) {
                result.append(',');
            }
            result.append('\"').append(entry.getKey()).append("\":").append(entry.getValue());
            first = false;
        }
        return result.append('}').toString();
    }

    private static String checksums(Path output) throws IOException {
        List<Path> paths;
        try (var stream = Files.walk(output)) {
            paths = stream.filter(Files::isRegularFile)
                    .filter(path -> !path.getFileName().toString().equals("checksums.sha256"))
                    .sorted(Comparator.comparing(path -> output.relativize(path).toString()))
                    .toList();
        }
        StringBuilder result = new StringBuilder();
        result.append(licenseHeader());
        for (Path path : paths) {
            result.append(sha256(Files.readAllBytes(path)))
                    .append("  ")
                    .append(output.relativize(path))
                    .append('\n');
        }
        return result.toString();
    }

    private static String licenseHeader() {
        return "# Licensed to the Apache Software Foundation (ASF) under one or more\n"
                + "# contributor license agreements. See the NOTICE file distributed with\n"
                + "# this work for additional information regarding copyright ownership.\n"
                + "# The ASF licenses this file to you under the Apache License, Version 2.0\n"
                + "# (the License); you may not use this file except in compliance with\n"
                + "# the License. You may obtain a copy of the License at\n"
                + "#\n"
                + "# http://www.apache.org/licenses/LICENSE-2.0\n"
                + "#\n"
                + "# Unless required by applicable law or agreed to in writing, software\n"
                + "# distributed under the License is distributed on an AS IS BASIS,\n"
                + "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
                + "# See the License for the specific language governing permissions and\n"
                + "# limitations under the License.\n";
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JDK must provide SHA-256", exception);
        }
    }

    private static void cleanOwnedOutput(Path output) throws IOException {
        if (!Files.exists(output)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        try (var stream = Files.walk(output)) {
            stream.sorted(Comparator.reverseOrder()).forEach(paths::add);
        }
        for (Path path : paths) {
            Files.delete(path);
        }
    }
}

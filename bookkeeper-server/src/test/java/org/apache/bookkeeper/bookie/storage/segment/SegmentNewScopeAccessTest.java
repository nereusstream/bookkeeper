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
package org.apache.bookkeeper.bookie.storage.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.startup.NewScopeFallbackManifest;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.junit.Assume;
import org.junit.Test;

/** Real Unix credential experiment; unavailable privilege is NOT_EXECUTED, never a synthetic access success. */
public class SegmentNewScopeAccessTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path SCRIPT = Path.of("../scripts/unified-wal/probe-new-scope-access.py")
            .toAbsolutePath().normalize();

    static void requireCredentials() throws Exception {
        Assume.assumeTrue("Unix credential experiment requires system Python and noninteractive sudo",
                Files.isExecutable(Path.of("/usr/bin/python3")) && Files.isExecutable(Path.of("/usr/bin/sudo")));
        var process = new ProcessBuilder("/usr/bin/sudo", "-n", "-u", "nobody", "/usr/bin/true")
                .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        boolean ended = process.waitFor(10, TimeUnit.SECONDS);
        if (!ended) {
            process.destroyForcibly();
            process.waitFor();
        }
        Assume.assumeTrue("Actual old-credential child unavailable; access experiment NOT_EXECUTED",
                ended && process.exitValue() == 0);
    }

    private static List<Map<String, String>> roots(Path parent) throws Exception {
        var result = new ArrayList<Map<String, String>>();
        for (var kind : NewScopeFallbackManifest.RootKind.values()) {
            var root = Files.createDirectory(parent.resolve(kind.name().toLowerCase()));
            result.add(Map.of("kind", kind.name(), "path", root.toRealPath().toString()));
        }
        return result;
    }

    static JsonNode probe(List<Map<String, String>> oldRoots, List<Map<String, String>> newRoots,
            String oldUser) throws Exception {
        byte[] input = JSON.writeValueAsBytes(Map.of("oldRoots", oldRoots, "newRoots", newRoots, "oldUser", oldUser));
        var output = Files.createTempFile("uw-scope-reply-", ".json");
        var error = Files.createTempFile("uw-scope-error-", ".txt");
        try {
            var process = new ProcessBuilder("/usr/bin/python3", "-I", "-B", SCRIPT.toString(), "--timeout", "5")
                    .redirectOutput(output.toFile()).redirectError(error.toFile()).start();
            try (var stdin = process.getOutputStream()) {
                stdin.write(input);
            }
            assertTrue("Administrative probe did not terminate", process.waitFor(30, TimeUnit.SECONDS));
            assertTrue("Oversized probe output", Files.size(output) <= 262144);
            var receipt = JSON.readTree(Files.readAllBytes(output));
            assertEquals("UWNS1", receipt.path("schema").asText());
            assertEquals("ACCESS_SNAPSHOT_ONLY", receipt.path("authority").asText());
            assertEquals("NOT_VERIFIED", receipt.path("oldServiceDisposition").asText("NOT_VERIFIED"));
            System.out.println("UWNS1_ACCESS_RECEIPT " + receipt);
            return receipt;
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(error);
        }
    }

    static StartupReadinessInput actualFallback(StartupReadinessInput requested, IsolatedSegmentConfig config)
            throws Exception {
        var parent = config.directory().getParent();
        var oldParent = Files.createDirectory(parent.resolve("declared-old-scope"));
        var oldRoots = roots(oldParent);
        var journal = Files.createDirectory(parent.resolve("new-journal-scope"));
        var ledger = Files.createDirectory(parent.resolve("new-ledger-scope"));
        var newRoots = List.of(
                Map.of("kind", "JOURNAL", "path", journal.toRealPath().toString()),
                Map.of("kind", "LEDGER", "path", ledger.toRealPath().toString()),
                Map.of("kind", "INDEX", "path", SegmentDerivedIndex.directory(config).toRealPath().toString()),
                Map.of("kind", "ARENA", "path", config.directory().toRealPath().toString()));
        var receipt = probe(oldRoots, newRoots, "nobody");
        assertEquals("VERIFIED", receipt.path("status").asText());
        assertEquals("NONE", receipt.path("oldCredentialAccessToNewScope").asText());
        assertEquals("READ_WRITE", receipt.path("newCredentialAccessToNewScope").asText());
        var declared = requested.newScopeFallback();
        var fallback = new NewScopeFallbackManifest(declared.oldBookieId(), declared.newBookieId(),
                declared.oldStorageIncarnation(), declared.newStorageIncarnation(),
                typedRoots(receipt.path("oldRoots")), typedRoots(receipt.path("newRoots")),
                new NewScopeFallbackManifest.CredentialScope("uid:" + receipt.at("/oldActor/actor/uid").asLong()),
                new NewScopeFallbackManifest.CredentialScope("uid:" + receipt.at("/newActor/actor/uid").asLong()),
                declared.oldScopeDisposition(), NewScopeFallbackManifest.AccessLevel.NONE,
                NewScopeFallbackManifest.AccessLevel.READ_WRITE);
        assertTrue(fallback.permitsNewScopeWritable());
        return new StartupReadinessInput(requested.compatibility(), requested.requiredDevices(),
                requested.recoveryFacts(), fallback, requested.rollbackSafetyFacts(),
                requested.targetReadinessGeneration(), requested.capabilityGeneration(),
                requested.runtimeReaderVersion(),
                requested.runtimeWriterVersion(), requested.supportedMandatoryFeatures(),
                requested.profileEndpoint(), requested.capabilityHints());
    }

    private static List<NewScopeFallbackManifest.StorageRoot> typedRoots(JsonNode observed) {
        List<NewScopeFallbackManifest.StorageRoot> roots = new ArrayList<>();
        observed.forEach(root -> roots.add(new NewScopeFallbackManifest.StorageRoot(
                NewScopeFallbackManifest.RootKind.valueOf(root.path("kind").asText()), root.path("path").asText())));
        roots.sort(Comparator.comparing(NewScopeFallbackManifest.StorageRoot::opaquePath));
        return roots;
    }

    @Test
    public void actualDistinctUidCannotReadOrCreateInsidePrivateNewRoots() throws Exception {
        requireCredentials();
        var root = Files.createTempDirectory(Path.of("/tmp"), "uw-new-scope-private-").toRealPath();
        try {
            var oldRoots = roots(Files.createDirectory(root.resolve("old")));
            var newRoots = roots(Files.createDirectory(root.resolve("new")));
            var receipt = probe(oldRoots, newRoots, "nobody");
            assertEquals("VERIFIED", receipt.path("status").asText());
            assertTrue(receipt.at("/oldActor/actor/uid").asLong() != receipt.at("/newActor/actor/uid").asLong());
            assertEquals(0, receipt.path("oldChildExitCode").asInt());
            assertEquals("NOT_EXECUTED", receipt.path("stockBinaryMatrix").asText());
            for (var probe : receipt.at("/oldActor/roots")) {
                assertEquals("DENIED", probe.at("/directoryRead/status").asText());
                assertEquals("DENIED", probe.at("/createWriteSync/status").asText());
            }
        } finally {
            remove(root);
        }
    }

    @Test
    public void worldAccessibleNewRootsRejectEvenWhenTheirNamesDiffer() throws Exception {
        requireCredentials();
        var root = Files.createTempDirectory(Path.of("/tmp"), "uw-new-scope-public-").toRealPath();
        try {
            var oldParent = Files.createDirectory(root.resolve("old"));
            var newParent = Files.createDirectory(root.resolve("new"));
            var oldRoots = roots(oldParent);
            var newRoots = roots(newParent);
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxr-xr-x"));
            Files.setPosixFilePermissions(newParent, PosixFilePermissions.fromString("rwxr-xr-x"));
            for (var entry : newRoots) {
                Files.setPosixFilePermissions(Path.of(entry.get("path")), PosixFilePermissions.fromString("rwxrwxrwx"));
            }
            var receipt = probe(oldRoots, newRoots, "nobody");
            assertEquals("REJECTED", receipt.path("status").asText());
            for (var probe : receipt.at("/oldActor/roots")) {
                assertEquals("ALLOWED", probe.at("/directoryRead/status").asText());
                assertEquals("ALLOWED", probe.at("/createWriteSync/status").asText());
            }
            assertFalse(receipt.path("oldCredentialAccessToNewScope").asText().equals("NONE"));
        } finally {
            remove(root);
        }
    }

    @Test
    public void executeOnlyDirectoriesCannotHideAccessToExistingKnownFiles() throws Exception {
        requireCredentials();
        var root = Files.createTempDirectory(Path.of("/tmp"), "uw-new-scope-searchable-").toRealPath();
        try {
            var oldRoots = roots(Files.createDirectory(root.resolve("old")));
            var newParent = Files.createDirectory(root.resolve("new"));
            var newRoots = roots(newParent);
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxr-xr-x"));
            Files.setPosixFilePermissions(newParent, PosixFilePermissions.fromString("rwxr-xr-x"));
            for (var entry : newRoots) {
                var path = Path.of(entry.get("path"));
                var known = Files.writeString(path.resolve("known-authority"), "public-test-vector");
                Files.setPosixFilePermissions(known, PosixFilePermissions.fromString("rw-r--r--"));
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx--x--x"));
                var reader = new ProcessBuilder("/usr/bin/sudo", "-n", "-u", "nobody",
                        "/usr/bin/head", "-c", "1", known.toString())
                        .redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
                assertTrue(reader.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, reader.exitValue());
            }
            assertEquals("REJECTED", probe(oldRoots, newRoots, "nobody").path("status").asText());
        } finally {
            remove(root);
        }
    }

    @Test
    public void physicalAliasMissingRootAndPrivilegedOldUidNeverVerify() throws Exception {
        requireCredentials();
        var root = Files.createTempDirectory(Path.of("/tmp"), "uw-new-scope-invalid-").toRealPath();
        try {
            var oldRoots = roots(Files.createDirectory(root.resolve("old")));
            var newRoots = roots(Files.createDirectory(root.resolve("new")));
            var alias = new ArrayList<>(newRoots);
            alias.set(0, Map.of("kind", alias.get(0).get("kind"), "path", oldRoots.get(0).get("path")));
            assertEquals("UNAVAILABLE", probe(oldRoots, alias, "nobody").path("status").asText());
            assertEquals("UNAVAILABLE", probe(oldRoots, newRoots, "root").path("status").asText());
            Files.delete(Path.of(newRoots.get(0).get("path")));
            assertEquals("UNAVAILABLE", probe(oldRoots, newRoots, "nobody").path("status").asText());
        } finally {
            remove(root);
        }
    }

    private static void remove(Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}

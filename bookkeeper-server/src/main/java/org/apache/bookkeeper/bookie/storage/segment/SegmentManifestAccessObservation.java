/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;

/** Point-in-time access observation of one offline Profile manifest, with no publication authority. */
final class SegmentManifestAccessObservation {
    private static final String SCHEMA = "UWNS2";
    private static final String DRAIN_SCHEMA = "UWNS2_DRAIN_V1";
    private static final int MAX_INPUT = 16_384;
    private static final int MAX_OUTPUT = 262_144;
    private static final int MAX_INVENTORY = 128;
    // Match the Python probe's Unicode code point order, including supplementary characters.
    private static final Comparator<String> PATH_ORDER = (left, right) -> Arrays.compareUnsigned(
            left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    private static final ObjectMapper JSON = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    record OldScope(String bookieId, String user, long uid, List<Map<String, String>> roots, UUID episode) {
        OldScope(String bookieId, String user, long uid, List<Map<String, String>> roots) {
            this(bookieId, user, uid, roots, null);
        }

        OldScope {
            roots = roots.stream().map(Map::copyOf).sorted(Comparator.comparing(root -> root.get("path"), PATH_ORDER))
                    .toList();
            if (bookieId.isBlank() || user.isBlank() || uid <= 0 || uid > 0xffff_ffffL || roots.size() != 4) {
                throw new IllegalArgumentException("Expected distinct old Bookie scope and Unix account required");
            }
        }
    }

    private record Identity(long device, long inode, long uid, long gid, int mode, long size) {
        static Identity observed(JsonNode node, boolean directory) throws IOException {
            long mode = number(node, "mode");
            if (mode < 0 || mode > 07777) {
                throw new IOException("Invalid access observation mode");
            }
            return new Identity(number(node, "device"), number(node, "inode"), number(node, "uid"),
                    number(node, "gid"), (int) mode, directory ? -1 : number(node, "size"));
        }

        static Identity current(Path path, boolean directory) throws IOException {
            if (!path.equals(path.toRealPath())) {
                throw new IOException("Access observation path is no longer canonical: " + path);
            }
            var attributes = Files.readAttributes(path, "unix:*", LinkOption.NOFOLLOW_LINKS);
            if (directory ? !Boolean.TRUE.equals(attributes.get("isDirectory"))
                    : !Boolean.TRUE.equals(attributes.get("isRegularFile"))
                        || ((Number) attributes.get("nlink")).longValue() != 1) {
                throw new IOException("Access observation path type changed: " + path);
            }
            return new Identity(((Number) attributes.get("dev")).longValue(),
                    ((Number) attributes.get("ino")).longValue(),
                    Integer.toUnsignedLong(((Number) attributes.get("uid")).intValue()),
                    Integer.toUnsignedLong(((Number) attributes.get("gid")).intValue()),
                    ((Number) attributes.get("mode")).intValue() & 07777,
                    directory ? -1 : ((Number) attributes.get("size")).longValue());
        }
    }

    private record Request(SegmentArenaManifest manifest, IsolatedSegmentConfig runtime, OldScope old, long newUid,
            Set<Path> requiredWrites, List<Map<String, String>> expected, Map<String, String> scope, String nonce,
            List<String> writable) { }

    private final ManifestIdentity manifestIdentity;
    private final OldScope oldScope;
    private final long newUid;
    private final List<Map<String, String>> expectedRoots;
    private final Map<Path, Identity> roots;
    private final Map<Path, Identity> files;
    private final Set<Path> requiredWrites;
    private final byte[] receipt;

    private SegmentManifestAccessObservation(ManifestIdentity manifestIdentity, OldScope oldScope, long newUid,
            List<Map<String, String>> expectedRoots, Map<Path, Identity> roots, Map<Path, Identity> files,
            Set<Path> requiredWrites, byte[] receipt) {
        this.manifestIdentity = manifestIdentity;
        this.oldScope = oldScope;
        this.newUid = newUid;
        this.expectedRoots = expectedRoots;
        this.roots = Map.copyOf(roots);
        this.files = Map.copyOf(files);
        this.requiredWrites = Set.copyOf(requiredWrites);
        this.receipt = receipt;
    }

    static Probe startProbe(SegmentArenaManifest manifest, IsolatedSegmentConfig runtime,
            OldScope old, long newUid, Path script, Set<Path> requiredWrites, Duration timeout)
            throws IOException {
        if (newUid < 0 || newUid > 0xffff_ffffL || old.uid() == newUid
                || old.bookieId().equals(manifest.bookieId().toString())
                || timeout.compareTo(Duration.ofSeconds(1)) < 0 || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Distinct observed scope and bounded probe timeout required");
        }
        var expected = expectedRoots(manifest, runtime);
        var scope = Map.of("oldBookieId", old.bookieId(), "bookieId", manifest.bookieId().toString(),
                "incarnation", HexFormat.of().formatHex(manifest.incarnation().value()),
                "manifestIdentity", HexFormat.of().formatHex(manifest.identity().value()));
        String nonce = UUID.randomUUID().toString().replace("-", "");
        var writable = requiredWrites.stream().map(Path::toString).sorted(PATH_ORDER).toList();
        byte[] input = JSON.writeValueAsBytes(Map.of("schema", SCHEMA, "scope", scope, "nonce", nonce,
                "oldUser", old.user(), "oldRoots", old.roots(), "newRoots", expected, "requiredWrites", writable));
        if (input.length > MAX_INPUT) {
            throw new IOException("Manifest access request exceeds its bounded envelope");
        }
        var request = new Request(manifest, runtime, old, newUid, Set.copyOf(requiredWrites), expected,
                scope, nonce, writable);
        return new Probe(request, input, script, timeout);
    }

    private static SegmentManifestAccessObservation bind(Request request, JsonNode reply, boolean succeeded)
            throws IOException {
        var manifest = request.manifest();
        var old = request.old();
        var expected = request.expected();
        String nonce = request.nonce();
        var requiredWrites = request.requiredWrites();
        if (reply == null || !SCHEMA.equals(reply.path("schema").asText())
                || !(succeeded ? "VERIFIED" : "REJECTED").equals(reply.path("status").asText())
                || !"ACCESS_SNAPSHOT_ONLY".equals(reply.path("authority").asText())
                || !"NOT_VERIFIED".equals(reply.path("oldServiceDisposition").asText())
                || !nonce.equals(reply.path("nonce").asText()) || number(reply, "oldChildExitCode") != 0
                || !JSON.valueToTree(request.scope()).equals(reply.path("scope"))
                || !JSON.valueToTree(request.writable()).equals(reply.path("requiredWrites"))) {
            throw new IOException("Probe result does not bind the original manifest access request");
        }
        var roots = new HashMap<Path, Identity>();
        bindRoots(reply.path("oldRoots"), old.roots(), roots);
        bindRoots(reply.path("newRoots"), expected, roots);
        var files = new HashMap<Path, Identity>();
        var inventory = reply.path("existingFiles");
        if (!inventory.isArray() || inventory.size() > MAX_INVENTORY) {
            throw new IOException("Invalid probe file inventory");
        }
        for (var row : inventory) {
            var path = Path.of(row.path("path").asText());
            if (files.putIfAbsent(path, Identity.observed(row, false)) != null) {
                throw new IOException("Probe file inventory repeats a path");
            }
        }
        if (!files.keySet().containsAll(requiredWrites)) {
            throw new IOException("Probe did not observe every required writable file");
        }
        var probed = new HashSet<>(files.keySet());
        for (var root : expected) {
            probed.add(Path.of(root.get("path")).resolve(".uw-scope-readable-" + nonce));
        }
        requireActor(reply.path("oldActor"), old.uid(), nonce, expected, probed, requiredWrites, false);
        requireActor(reply.path("newActor"), request.newUid(), nonce, expected, probed, requiredWrites, true);
        if (!succeeded) {
            throw new IOException("Actual manifest access probe refused the scope");
        }
        var result = new SegmentManifestAccessObservation(manifest.identity(), old, request.newUid(), expected,
                roots, files, requiredWrites, JSON.writeValueAsBytes(reply));
        result.requireCurrent(manifest, request.runtime(), requiredWrites);
        return result;
    }

    /** Owns the private exchange after the caller's result deadline, without a background Java waiter. */
    static final class Probe {
        private final Request request;
        private final Path exchange;
        private final Path operationLock;
        private final FileChannel operationFile;
        private final Identity lockIdentity;
        private final String requestDigest;
        private final Process collector;
        private IOException cleanupFailure;

        private Probe(Request request, byte[] input, Path script, Duration timeout) throws IOException {
            this.request = request;
            requestDigest = sha256(input);
            var temporary = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
            exchange = Files.createTempDirectory(temporary, "uw-manifest-access-");
            operationLock = exchange.resolve("operation.lock");
            FileChannel opened = null;
            try {
                Files.write(exchange.resolve("request.json"), input, StandardOpenOption.CREATE_NEW);
                opened = FileChannel.open(operationLock, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS);
                lockIdentity = Identity.current(operationLock, false);
                var descriptor = new NativeIOImpl().fileIdentity(opened);
                if (descriptor.device() != lockIdentity.device() || descriptor.inode() != lockIdentity.inode()) {
                    throw new IOException("Manifest access operation descriptor differs from its original path");
                }
                String binding = DRAIN_SCHEMA + ":" + request.nonce() + ":" + requestDigest + ":"
                        + lockIdentity.device() + ":" + lockIdentity.inode();
                collector = new ProcessBuilder("/usr/bin/python3", "-I", "-B", script.toRealPath().toString(),
                        "--owned-exchange", exchange.toString(), "--operation-binding", binding,
                        "--timeout", Double.toString(timeout.toMillis() / 2000.0))
                        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                        .redirectError(ProcessBuilder.Redirect.DISCARD).start();
                operationFile = opened;
            } catch (IOException | RuntimeException | Error startup) {
                if (opened != null) {
                    try {
                        opened.close();
                    } catch (IOException cleanup) {
                        startup.addSuppressed(cleanup);
                    }
                }
                try {
                    deleteExchange();
                } catch (IOException cleanup) {
                    startup.addSuppressed(cleanup);
                }
                throw startup;
            }
        }

        SegmentManifestAccessObservation await(Duration timeout) throws IOException, InterruptedException {
            if (!collector.waitFor(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new IOException("Actual manifest access probe did not complete successfully");
            }
            var terminal = inspectTerminal(false);
            if (terminal == null) {
                throw new IOException("Manifest probe operation is still draining");
            }
            int exit = (int) number(terminal, "exitCode");
            if (exit != collector.exitValue()) {
                throw new IOException("Manifest probe collector did not confirm the operation result");
            }
            if (exit != 0 && exit != 1) {
                throw new IOException("Actual manifest access probe unavailable with exit " + exit);
            }
            return bind(request, terminal.path("receipt"), exit == 0);
        }

        boolean closeWhenDrained() throws IOException {
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
            if (collector.isAlive() || inspectTerminal(true) == null) {
                return false;
            }
            try {
                operationFile.close();
            } catch (IOException failure) {
                cleanupFailure = failure;
                throw failure;
            }
            return true;
        }

        private JsonNode inspectTerminal(boolean cleanup) throws IOException {
            Path terminal = exchange.resolve("terminal.json");
            if (!Files.isRegularFile(terminal, LinkOption.NOFOLLOW_LINKS)) {
                return null;
            }
            requireOriginalLock();
            boolean deleting = false;
            try (var lock = operationFile.tryLock(0, 1, false)) {
                if (lock == null) {
                    return null;
                }
                requireOriginalLock();
                JsonNode record;
                try (var stream = Files.newInputStream(terminal, LinkOption.NOFOLLOW_LINKS)) {
                    byte[] bytes = stream.readNBytes(MAX_OUTPUT + 1);
                    if (bytes.length > MAX_OUTPUT) {
                        throw new IOException("Manifest access terminal record exceeds its bound");
                    }
                    record = JSON.readTree(bytes);
                }
                if (record == null || !DRAIN_SCHEMA.equals(record.path("schema").asText())
                        || !request.nonce().equals(record.path("nonce").asText())
                        || !requestDigest.equals(record.path("requestSha256").asText())
                        || number(record, "device") != lockIdentity.device()
                        || number(record, "inode") != lockIdentity.inode()) {
                    throw new IOException("Manifest access terminal record does not bind the original operation");
                }
                long exit = number(record, "exitCode");
                if (exit < 0 || exit > 2 || !record.has("cleanupFailure")
                        || (exit < 2 ? !record.path("receipt").isObject() : !record.path("receipt").isNull())) {
                    throw new IOException("Invalid manifest access operation terminal state");
                }
                if (!record.path("cleanupFailure").isNull()) {
                    if (cleanupFailure == null) {
                        cleanupFailure = new IOException("Manifest access nonce cleanup failed: "
                                + record.path("cleanupFailure").asText());
                    }
                    throw cleanupFailure;
                }
                if (cleanup) {
                    deleting = true;
                    deleteExchange();
                }
                return record;
            } catch (OverlappingFileLockException stillOwned) {
                return null;
            } catch (IOException failure) {
                if (deleting && cleanupFailure == null) {
                    cleanupFailure = failure;
                }
                throw failure;
            }
        }

        private void requireOriginalLock() throws IOException {
            if (!lockIdentity.equals(Identity.current(operationLock, false))) {
                throw new IOException("Manifest access operation lock identity changed");
            }
        }

        private void deleteExchange() throws IOException {
            Files.deleteIfExists(exchange.resolve("terminal.json"));
            Files.deleteIfExists(exchange.resolve("terminal.pending"));
            Files.deleteIfExists(exchange.resolve("operation.started"));
            Files.deleteIfExists(exchange.resolve("request.json"));
            Files.deleteIfExists(operationLock);
            Files.deleteIfExists(exchange);
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static List<Map<String, String>> expectedRoots(SegmentArenaManifest manifest,
            IsolatedSegmentConfig runtime) {
        var roots = new ArrayList<Map<String, String>>();
        roots.add(Map.of("kind", "CONTROL", "arena", "", "path", manifest.controlDirectory().toString()));
        for (int i = 0; i < manifest.arenas().size(); i++) {
            var arena = manifest.arenas().get(i);
            roots.add(Map.of("kind", "ARENA", "arena", arena.id().toString(), "path", arena.directory().toString()));
            roots.add(Map.of("kind", "INDEX", "arena", arena.id().toString(),
                    "path", SegmentDerivedIndex.directory(manifest.arenaConfig(runtime, i)).toString()));
        }
        roots.sort(Comparator.comparing(root -> root.get("path"), PATH_ORDER));
        return List.copyOf(roots);
    }

    private static void bindRoots(JsonNode rows, List<Map<String, String>> expected, Map<Path, Identity> roots)
            throws IOException {
        requireRootRows(rows, expected);
        for (var row : rows) {
            var path = Path.of(row.path("path").textValue());
            if (roots.putIfAbsent(path, Identity.observed(row.path("identity"), true)) != null) {
                throw new IOException("Probe aliases a declared root");
            }
        }
    }

    private static void requireRootRows(JsonNode rows, List<Map<String, String>> expected) throws IOException {
        if (!rows.isArray() || rows.size() != expected.size()) {
            throw new IOException("Probe did not cover the complete expected root vector");
        }
        for (int i = 0; i < expected.size(); i++) {
            for (var field : expected.get(i).entrySet()) {
                if (!field.getValue().equals(rows.get(i).path(field.getKey()).textValue())) {
                    throw new IOException("Probe root role, Arena or path differs from the expected vector");
                }
            }
        }
    }

    private static void requireActor(JsonNode actor, long uid, String nonce, List<Map<String, String>> roots,
            Set<Path> probed, Set<Path> requiredWrites, boolean target) throws IOException {
        var identity = actor.path("actor");
        long gid = number(identity, "gid");
        var groups = identity.path("groups");
        if (!nonce.equals(actor.path("nonce").asText()) || number(identity, "uid") != uid
                || gid < 0 || gid > 0xffff_ffffL || !groups.isArray()) {
            throw new IOException("Probe actor is not the expected Unix observation");
        }
        for (var group : groups) {
            if (!group.isIntegralNumber() || !group.canConvertToLong() || group.longValue() < 0
                    || group.longValue() > 0xffff_ffffL) {
                throw new IOException("Invalid probe supplementary group");
            }
        }
        String expected = target ? "ALLOWED" : "DENIED";
        var rootRows = actor.path("roots");
        requireRootRows(rootRows, roots);
        for (var row : rootRows) {
            requireAccess(row, "directoryRead", expected);
            requireAccess(row, "createWriteSync", expected);
        }
        var rows = actor.path("files");
        if (!rows.isArray() || rows.size() != probed.size()) {
            throw new IOException("Probe actor did not observe every known file and marker");
        }
        var seen = new HashSet<Path>();
        for (var row : rows) {
            var path = Path.of(row.path("path").asText());
            if (!probed.contains(path) || !seen.add(path)) {
                throw new IOException("Probe actor file inventory differs from the original request");
            }
            requireAccess(row, "readOpen", expected);
            if (!target || requiredWrites.contains(path)) {
                requireAccess(row, "writeOpen", expected);
            } else {
                String status = row.path("writeOpen").path("status").asText();
                if (!"ALLOWED".equals(status) && !"DENIED".equals(status)) {
                    throw new IOException("Unknown writeOpen for " + path);
                }
            }
        }
    }

    private static void requireAccess(JsonNode row, String operation, String expected) throws IOException {
        if (!expected.equals(row.path(operation).path("status").asText())) {
            throw new IOException("Unexpected " + operation + " for " + row.path("path").asText());
        }
    }

    private static long number(JsonNode node, String name) throws IOException {
        var value = node.path(name);
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IOException("Missing or invalid probe number: " + name);
        }
        return value.longValue();
    }

    void requireCurrent(SegmentArenaManifest manifest, IsolatedSegmentConfig runtime, Set<Path> writes)
            throws IOException {
        if (!manifestIdentity.equals(manifest.identity()) || !expectedRoots.equals(expectedRoots(manifest, runtime))
                || !requiredWrites.equals(writes)) {
            throw new IOException("Access observation does not belong to this complete manifest and file use");
        }
        for (var entry : roots.entrySet()) {
            if (!entry.getValue().equals(Identity.current(entry.getKey(), true))) {
                throw new IOException("Observed root identity changed: " + entry.getKey());
            }
        }
        var seen = new HashSet<Path>();
        int directories = 0;
        for (var root : expectedRoots) {
            if ("INDEX".equals(root.get("kind"))) {
                continue; // The member's Arena walk covers this exact subtree once.
            }
            try (var walk = Files.walk(Path.of(root.get("path")))) {
                var paths = walk.iterator();
                while (paths.hasNext()) {
                    var path = paths.next();
                    var attrs = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (attrs.isDirectory()) {
                        if (++directories > MAX_INVENTORY) {
                            throw new IOException("Manifest directory inventory exceeds its bound");
                        }
                    } else {
                        var expected = files.get(path);
                        if (expected == null || !expected.equals(Identity.current(path, false)) || !seen.add(path)) {
                            throw new IOException("Observed manifest file inventory changed: " + path);
                        }
                    }
                }
            } catch (UncheckedIOException failure) {
                throw failure.getCause();
            }
        }
        if (seen.size() != files.size()) {
            throw new IOException("Observed manifest file inventory is incomplete");
        }
    }

    byte[] receipt() {
        return receipt.clone();
    }

    /** Compare the original request scope and observed actors. The old episode is caller-declared. */
    void requirePolicyScope(SegmentManifestStartupPolicy policy) throws IOException {
        if (!manifestIdentity.equals(policy.manifest().identity()) || !expectedRoots.equals(policy.roots())
                || !oldScope.equals(policy.oldScope()) || newUid != policy.newUid()) {
            throw new IOException("Access observation differs from the declared complete policy scope");
        }
    }
}

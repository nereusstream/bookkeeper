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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.NetworkInterface;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.zookeeper.data.Stat;

/** Current-build Classic child process; its receipts do not assert any Profile route authority. */
public final class CurrentClassicEpisodeProbe {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> ROOT_KINDS = List.of("journal", "ledger", "index", "arena");

    private CurrentClassicEpisodeProbe() {}

    /**
     * Run fresh or restart an actually closed episode, then accept CLOSE nonce and EXIT nonce on standard input.
     * Fresh requires a nonexistent local root and the private namespace /uw-classic-interop-nonce.
     */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 5 || !List.of("fresh", "existing").contains(arguments[0])) {
            throw new IllegalArgumentException(
                    "usage: CurrentClassicEpisodeProbe <fresh|existing> <root> <metadata-uri> <port> <nonce>");
        }
        boolean fresh = arguments[0].equals("fresh");
        Path requestedRoot = Path.of(arguments[1]).toAbsolutePath().normalize();
        URI uri = URI.create(arguments[2]);
        int port = Integer.parseInt(arguments[3]);
        String nonce = UUID.fromString(arguments[4]).toString();
        if (!nonce.equals(arguments[4]) || port < 1024 || port > 65535
                || !"zk+hierarchical".equals(uri.getScheme()) || !"127.0.0.1".equals(uri.getHost())
                || uri.getPort() < 1024 || uri.getPort() > 65535 || uri.getUserInfo() != null
                || uri.getQuery() != null || uri.getFragment() != null
                || !uri.getPath().matches("/uw-classic-interop-[0-9a-f-]{36}")) {
            throw new IllegalArgumentException("Only private loopback scopes and canonical nonces are allowed");
        }
        if (fresh && !uri.getPath().equals("/uw-classic-interop-" + nonce)) {
            throw new IllegalArgumentException("A fresh namespace must match this episode's nonce");
        }
        Path root;
        if (fresh) {
            root = Files.createDirectory(requestedRoot, PosixFilePermissions.asFileAttribute(
                    PosixFilePermissions.fromString("rwx------"))).toRealPath();
            for (String kind : ROOT_KINDS) {
                Files.createDirectory(root.resolve(kind), PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rwx------")));
            }
            publish(root.resolve("scope.json"), Map.of("root", root.toString(), "metadataUri", uri.toString(),
                    "bookieId", "127.0.0.1:" + port, "createdNonce", nonce));
        } else {
            root = requestedRoot.toRealPath();
            JsonNode scope = JSON.readTree(Files.readAllBytes(root.resolve("scope.json")));
            JsonNode previous = JSON.readTree(Files.readAllBytes(root.resolve("last-closed.json")));
            JsonNode binding = previous.path("binding");
            if (!scope.path("root").asText().equals(root.toString())
                    || !scope.path("metadataUri").asText().equals(uri.toString())
                    || !scope.path("bookieId").asText().equals("127.0.0.1:" + port)
                    || !binding.path("root").asText().equals(root.toString())
                    || !binding.path("metadataUri").asText().equals(uri.toString())
                    || !binding.path("bookieId").asText().equals("127.0.0.1:" + port)
                    || !previous.path("stackClosed").asBoolean() || !previous.path("driverClosed").asBoolean()
                    || previous.path("exitCode").asInt(-1) != 0 || binding.path("pid").asLong() <= 0
                    || ProcessHandle.of(binding.path("pid").asLong()).map(ProcessHandle::isAlive).orElse(false)
                    || Files.exists(root.resolve("active.json"))) {
                throw new IllegalStateException("Existing scope is mismatched or its prior episode is not closed");
            }
            for (String kind : ROOT_KINDS) {
                if (!Files.isDirectory(root.resolve(kind)) || Files.isSymbolicLink(root.resolve(kind))) {
                    throw new IllegalStateException("Existing storage roots must remain original directories");
                }
                if (!kind.equals("arena") && !Files.isRegularFile(root.resolve(kind).resolve("current/VERSION"))) {
                    throw new IllegalStateException("Existing Classic storage has no cookie");
                }
            }
        }
        Path ready = root.resolve("ready-" + nonce + ".json");
        Path closed = root.resolve("closed-" + nonce + ".json");
        if (Files.exists(ready) || Files.exists(closed)) {
            throw new IllegalStateException("An episode nonce cannot be reused");
        }
        Files.write(root.resolve("active.json"), JSON.writeValueAsBytes(
                Map.of("nonce", nonce, "pid", ProcessHandle.current().pid())), StandardOpenOption.CREATE_NEW);
        ServerConfiguration conf = configuration(root, uri.toString(), port);
        if (fresh && !BookKeeperAdmin.format(conf, false, false)) {
            throw new IllegalStateException("Fresh private namespace already exists; refusing to replace it");
        }
        var driver = (ZKMetadataBookieDriver) BookieResources.createMetadataDriver(conf, NullStatsLogger.INSTANCE);
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        Map<String, Object> binding;
        EmbeddedServer server = null;
        try {
            server = EmbeddedServer.builder(new BookieConfiguration(conf)).metadataDriver(driver)
                    .statsProvider(new NullStatsProvider()).build();
            var stack = server.getLifecycleComponentStack();
            try {
                stack.start();
                String registration = uri.getPath() + "/available/"
                        + server.getBookieService().getServer().getBookieId();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                Stat stat;
                do {
                    stat = driver.getZk().exists(registration, false);
                    if (stat != null && server.getBookieService().getServer().isRunning()) {
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                } while (System.nanoTime() < deadline);
                if (stat == null || stat.getEphemeralOwner() != driver.getZk().getSessionId()
                        || !server.getBookieService().getServer().isRunning()) {
                    throw new IllegalStateException("Current Classic process did not own a healthy registration");
                }
                binding = new LinkedHashMap<>();
                binding.put("nonce", nonce);
                binding.put("mode", arguments[0]);
                binding.put("root", root.toString());
                binding.put("metadataUri", uri.toString());
                binding.put("pid", ProcessHandle.current().pid());
                binding.put("javaHome", System.getProperty("java.home"));
                binding.put("javaVersion", System.getProperty("java.version"));
                binding.put("bookieId", server.getBookieService().getServer().getBookieId().toString());
                binding.put("registration", registration);
                binding.put("registrationOwner", stat.getEphemeralOwner());
                binding.put("registrationBirth", stat.getCzxid());
                var roots = new ArrayList<Map<String, String>>();
                for (String kind : ROOT_KINDS) {
                    roots.add(Map.of("kind", kind, "path", root.resolve(kind).toRealPath().toString()));
                }
                binding.put("roots", roots);
                binding.put("configuration", Map.of("ledgerStorageClass", conf.getLedgerStorageClass(),
                        "journalSyncData", conf.getJournalSyncData(),
                        "listeningInterface", conf.getListeningInterface(),
                        "allowLoopback", conf.getAllowLoopback(), "autoRecovery", conf.isAutoRecoveryDaemonEnabled(),
                        "httpServer", conf.isHttpServerEnabled(),
                        "dataIntegrity", conf.isDataIntegrityCheckingEnabled()));
                binding.put("classes", Map.of("EmbeddedServer", classIdentity(EmbeddedServer.class),
                        "BookieImpl", classIdentity(BookieImpl.class),
                        "InterleavedLedgerStorage", classIdentity(InterleavedLedgerStorage.class),
                        "probe", classIdentity(CurrentClassicEpisodeProbe.class)));
                publish(ready, binding);
                System.out.println("READY " + nonce);
                System.out.flush();
                if (!("CLOSE " + nonce).equals(input.readLine())) {
                    throw new IllegalStateException("Missing matching CLOSE command");
                }
            } finally {
                stack.close();
            }
        } finally {
            driver.close();
        }
        if (server.getBookieService().getServer().isRunning()
                || server.getBookieService().getServer().isBookieRunning()
                || server.getBookieService().getServer().getExitCode() != 0) {
            throw new IllegalStateException("Current Classic close did not complete successfully");
        }
        var result = Map.of("binding", binding, "stackClosed", true, "driverClosed", true,
                "bookieRunning", false, "serverRunning", false,
                "exitCode", server.getBookieService().getServer().getExitCode());
        publish(closed, result);
        Path latest = root.resolve("last-closed.json");
        Path temporary = root.resolve("last-closed-" + nonce + ".tmp");
        Files.write(temporary, JSON.writeValueAsBytes(result), StandardOpenOption.CREATE_NEW);
        Files.move(temporary, latest, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.delete(root.resolve("active.json"));
        System.out.println("CLOSED " + nonce);
        System.out.flush();
        if (!("EXIT " + nonce).equals(input.readLine())) {
            throw new IllegalStateException("Missing matching EXIT command");
        }
    }

    private static ServerConfiguration configuration(Path root, String uri, int port) throws Exception {
        String loopback = NetworkInterface.getByInetAddress(java.net.InetAddress.getByName("127.0.0.1")).getName();
        ServerConfiguration conf = new ServerConfiguration().setBookiePort(port)
                .setBookieId("127.0.0.1:" + port).setAdvertisedAddress("127.0.0.1")
                .setListeningInterface(loopback).setAllowLoopback(true).setMetadataServiceUri(uri)
                .setJournalDirName(root.resolve("journal").toString())
                .setLedgerDirNames(new String[] {root.resolve("ledger").toString()})
                .setIndexDirName(new String[] {root.resolve("index").toString()})
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName()).setJournalSyncData(true)
                .setAutoRecoveryDaemonEnabled(false).setHttpServerEnabled(false).setDataIntegrityCheckingEnabled(false);
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setDiskUsageThreshold(0.999f);
        conf.setDiskUsageWarnThreshold(0.99f);
        conf.setProperty("numAddWorkerThreads", 1);
        conf.setProperty("numReadWorkerThreads", 1);
        conf.setProperty("numHighPriorityWorkerThreads", 1);
        return conf;
    }

    private static Map<String, String> classIdentity(Class<?> type) throws Exception {
        try (var bytes = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            if (bytes == null || type.getProtectionDomain().getCodeSource() == null) {
                throw new IllegalStateException("Class provenance is unavailable: " + type.getName());
            }
            return Map.of("name", type.getName(), "codeSource",
                    type.getProtectionDomain().getCodeSource().getLocation().toExternalForm(), "resourceSha256",
                    java.util.HexFormat.of().formatHex(
                            MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes())));
        }
    }

    private static void publish(Path file, Object value) throws Exception {
        Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(temporary, JSON.writeValueAsBytes(value), StandardOpenOption.CREATE_NEW);
        Files.move(temporary, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }
}

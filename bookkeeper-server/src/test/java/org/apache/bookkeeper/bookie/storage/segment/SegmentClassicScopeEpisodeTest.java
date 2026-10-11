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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/** Owned isolated Classic episode; no production decommission or released-old-binary claim. */
public class SegmentClassicScopeEpisodeTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Actual child main, including the original component construction/start/close paths. */
    public static void main(String[] args) throws Exception {
        Path parent = Path.of(args[0]);
        String uri = args[1];
        int port = Integer.parseInt(args[2]);
        String nonce = args[3];
        var roots = new ArrayList<Map<String, String>>();
        for (String kind : List.of("JOURNAL", "LEDGER", "INDEX", "ARENA")) {
            Path path = Files.createDirectory(parent.resolve(kind.toLowerCase()));
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwx------"));
            roots.add(Map.of("kind", kind, "path", path.toRealPath().toString()));
        }
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setBookiePort(port).setBookieId("127.0.0.1:" + port).setAdvertisedAddress("127.0.0.1")
                .setMetadataServiceUri(uri).setJournalDirName(parent.resolve("journal").toString())
                .setLedgerDirNames(new String[] {parent.resolve("ledger").toString()})
                .setIndexDirName(new String[] {parent.resolve("index").toString()})
                .setAutoRecoveryDaemonEnabled(false).setHttpServerEnabled(false).setDataIntegrityCheckingEnabled(false);
        conf.setProperty("numAddWorkerThreads", 1);
        conf.setProperty("numReadWorkerThreads", 1);
        conf.setProperty("numHighPriorityWorkerThreads", 1);
        if (!BookKeeperAdmin.format(conf, false, true)) {
            throw new IllegalStateException("Could not format the isolated Classic namespace");
        }
        long uid;
        var id = new ProcessBuilder("/usr/bin/id", "-u").start();
        uid = Long.parseLong(new String(id.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim());
        if (id.waitFor() != 0) {
            throw new IllegalStateException("Could not observe actual child uid");
        }
        var driver = (ZKMetadataBookieDriver) BookieResources.createMetadataDriver(conf, NullStatsLogger.INSTANCE);
        var server = EmbeddedServer.builder(new BookieConfiguration(conf)).metadataDriver(driver)
                .statsProvider(new NullStatsProvider()).build();
        var stack = server.getLifecycleComponentStack();
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        Map<String, Object> binding;
        try {
            stack.start();
            String registration = "/" + java.net.URI.create(uri).getPath().replaceFirst("^/", "")
                    + "/available/" + server.getBookieService().getServer().getBookieId();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            org.apache.zookeeper.data.Stat stat;
            do {
                stat = driver.getZk().exists(registration, false);
                if (stat == null || !server.getBookieService().getServer().isRunning()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
            } while ((stat == null || !server.getBookieService().getServer().isRunning())
                    && System.nanoTime() < deadline);
            if (stat == null || stat.getEphemeralOwner() != driver.getZk().getSessionId()) {
                throw new IllegalStateException("Original Classic session did not register");
            }
            binding = Map.of("nonce", nonce, "pid", ProcessHandle.current().pid(), "uid", uid,
                    "bookieId", server.getBookieService().getServer().getBookieId().toString(),
                    "registration", registration, "registrationOwner", stat.getEphemeralOwner(),
                    "registrationBirth", stat.getCzxid(), "roots", roots,
                    "classes", Map.of("EmbeddedServer", classDigest(EmbeddedServer.class),
                            "BookieImpl", classDigest(BookieImpl.class)));
            publish(parent.resolve("ready.json"), binding);
            String command = input.readLine();
            if (("ABRUPT " + nonce).equals(command)) {
                Runtime.getRuntime().halt(37);
            }
            if (!("CLOSE " + nonce).equals(command)) {
                throw new IllegalStateException("Missing original episode close command");
            }
        } finally {
            stack.close();
            driver.close();
        }
        if (server.getBookieService().getServer().isRunning()
                || server.getBookieService().getServer().isBookieRunning()
                || server.getBookieService().getServer().getExitCode() != 0) {
            throw new IllegalStateException("Original Classic close did not complete successfully");
        }
        publish(parent.resolve("closed.json"), Map.of("binding", binding,
                "stackClosed", true, "driverClosed", true, "bookieRunning", false, "serverRunning", false,
                "exitCode", server.getBookieService().getServer().getExitCode()));
        // A closed component stack and absent registration alone do not imply actual process termination.
        if (!("EXIT " + nonce).equals(input.readLine())) {
            throw new IllegalStateException("Missing original episode exit command");
        }
    }

    private static void publish(Path file, Object receipt) throws Exception {
        Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(temporary, JSON.writeValueAsBytes(receipt));
        Files.move(temporary, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private static String classDigest(Class<?> type) throws Exception {
        try (var bytes = type.getResourceAsStream(type.getSimpleName() + ".class")) {
            return java.util.HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes()));
        }
    }

    static final class Episode implements AutoCloseable {
        private final Path root;
        private final Path parent;
        private final Path output;
        private final String nonce = UUID.randomUUID().toString();
        private final Process process;
        private final ZooKeeper observer;
        private final String uri;
        private JsonNode ready;
        private boolean closeRequested;
        private ProcessHandle javaProcess;

        Episode(String connect, String namespace) throws Exception {
            root = Files.createTempDirectory(Path.of("/tmp"), "uw-classic-scope-").toRealPath();
            Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwxr-xr-x"));
            parent = Files.createDirectory(root.resolve("old"));
            Files.setPosixFilePermissions(parent, PosixFilePermissions.fromString("rwxrwxrwx"));
            output = root.resolve("child.log");
            int port;
            try (var socket = new ServerSocket(0)) {
                port = socket.getLocalPort();
            }
            uri = "zk+hierarchical://" + connect + namespace;
            var watcher = new ZooKeeperWatcherBase(10000, false);
            observer = new ZooKeeper(connect, 10000, watcher);
            watcher.waitForConnection();
            String classpath = System.getProperty("surefire.test.class.path",
                    System.getProperty("java.class.path"));
            process = new ProcessBuilder("/usr/bin/sudo", "-n", "-u", "nobody", "--",
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m", "-cp", classpath,
                    SegmentClassicScopeEpisodeTest.class.getName(), parent.toString(), uri, String.valueOf(port), nonce)
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            try {
                awaitFile(parent.resolve("ready.json"), Duration.ofSeconds(30));
                ready = JSON.readTree(Files.readAllBytes(parent.resolve("ready.json")));
                assertEquals(nonce, ready.path("nonce").asText());
                var lookup = new ProcessBuilder("/usr/bin/id", "-u", "nobody").start();
                long expectedUid = Long.parseLong(new String(lookup.getInputStream().readAllBytes(),
                        StandardCharsets.US_ASCII).trim());
                assertEquals(0, lookup.waitFor());
                assertEquals(expectedUid, ready.path("uid").asLong());
                javaProcess = ProcessHandle.of(ready.path("pid").asLong()).orElseThrow();
                assertTrue(javaProcess.isAlive());
                var registration = observer.exists(ready.path("registration").asText(), false);
                assertNotNull(registration);
                assertEquals(ready.path("registrationOwner").asLong(), registration.getEphemeralOwner());
                assertEquals(ready.path("registrationBirth").asLong(), registration.getCzxid());
                assertTrue(process.isAlive());
            } catch (Throwable failure) {
                close();
                throw failure;
            }
        }

        JsonNode ready() {
            return ready;
        }
        void writeAndReadConfirmedEntry() throws Exception {
            byte[] data = "old-service-durable-entry".getBytes(StandardCharsets.UTF_8);
            try (var sdk = new BookKeeper(new ClientConfiguration().setMetadataServiceUri(uri));
                    var ledger = sdk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, new byte[0])) {
                assertEquals(0, ledger.addEntry(data));
                assertArrayEquals(data, ledger.readEntries(0, 0).nextElement().getEntry());
            }
        }

        void requestCloseAndHoldProcess() throws Exception {
            assertFalse(closeRequested);
            process.getOutputStream().write(("CLOSE " + nonce + "\n").getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().flush();
            closeRequested = true;
            awaitFile(parent.resolve("closed.json"), Duration.ofSeconds(30));
            var closed = JSON.readTree(Files.readAllBytes(parent.resolve("closed.json")));
            assertEquals(ready, closed.path("binding"));
            assertTrue(closed.path("stackClosed").asBoolean());
            assertTrue(closed.path("driverClosed").asBoolean());
            assertTrue(process.isAlive());
            syncRegistration();
            assertEquals(null, observer.exists(ready.path("registration").asText(), false));
            assertThrows(java.io.IOException.class, () -> {
                try (var socket = new Socket()) {
                    socket.connect(new java.net.InetSocketAddress("127.0.0.1",
                            Integer.parseInt(ready.path("bookieId").asText().split(":")[1])), 1000);
                }
            });
        }

        JsonNode drained() throws Exception {
            if (!closeRequested || process.isAlive() || javaProcess.isAlive() || process.exitValue() != 0
                    || !Files.exists(parent.resolve("closed.json"))) {
                throw new IllegalStateException("Original observed Classic episode is not actually drained");
            }
            syncRegistration();
            assertEquals(null, observer.exists(ready.path("registration").asText(), false));
            assertEquals(null, observer.exists(ready.path("registration").asText().replace("/available/",
                    "/available/readonly/"), false));
            var result = JSON.readTree(Files.readAllBytes(parent.resolve("closed.json")));
            assertEquals(ready, result.path("binding"));
            System.out.println("UW_CLASSIC_DRAIN_RECEIPT " + result);
            return result;
        }

        UUID scopeEpisode() {
            return UUID.fromString(nonce);
        }

        void abruptExit() throws Exception {
            process.getOutputStream().write(("ABRUPT " + nonce + "\n").getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().flush();
            assertTrue(Files.readString(output), process.waitFor(30, TimeUnit.SECONDS));
            assertEquals(37, process.exitValue());
            assertFalse(javaProcess.isAlive());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (observer.exists(ready.path("registration").asText(), false) != null
                    && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            }
            syncRegistration();
            assertEquals(null, observer.exists(ready.path("registration").asText(), false));
        }

        void exit() throws Exception {
            process.getOutputStream().write(("EXIT " + nonce + "\n").getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().flush();
            assertTrue(Files.readString(output), process.waitFor(30, TimeUnit.SECONDS));
        }

        private void syncRegistration() throws Exception {
            var done = new CompletableFuture<Integer>();
            observer.sync(ready.path("registration").asText(), (rc, path, context) -> done.complete(rc), null);
            assertEquals(Integer.valueOf(0), done.get(10, TimeUnit.SECONDS));
        }

        private void awaitFile(Path file, Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (!Files.exists(file) && process.isAlive() && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            assertTrue(Files.readString(output), Files.exists(file) && process.isAlive());
        }

        @Override
        public void close() throws Exception {
            try {
                if (process.isAlive()) {
                    process.getOutputStream().close();
                    if (!process.waitFor(30, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor();
                    }
                }
            } finally {
                observer.close();
                var cleanup = new ProcessBuilder("/usr/bin/sudo", "-n", "-u", "nobody", "/usr/bin/python3", "-I", "-c",
                        "import pathlib,shutil,sys; p=pathlib.Path(sys.argv[1]); "
                            + "[(shutil.rmtree(x) if x.is_dir() else x.unlink()) for x in p.iterdir()]",
                        parent.toString()).start();
                assertTrue(cleanup.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, cleanup.exitValue());
                Files.delete(parent);
                Files.deleteIfExists(output);
                Files.delete(root);
            }
        }
    }

    @Test
    public void originalClassicServiceMustCloseAndActuallyExitBeforeEpisodeIsDrained() throws Exception {
        SegmentNewScopeAccessTest.requireCredentials();
        var zk = new ZooKeeperUtil();
        zk.startCluster();
        try (var episode = new Episode(zk.getZooKeeperConnectString(), "/old-classic-episode")) {
            episode.writeAndReadConfirmedEntry();
            assertThrows(IllegalStateException.class, episode::drained);
            episode.requestCloseAndHoldProcess();
            assertThrows(IllegalStateException.class, episode::drained);
            episode.exit();
            assertEquals(0, episode.drained().path("exitCode").asInt());
        } finally {
            zk.stopCluster();
        }
    }

    @Test
    public void abruptExitAndAbsentRegistrationDoNotProduceDrainedEpisode() throws Exception {
        SegmentNewScopeAccessTest.requireCredentials();
        var zk = new ZooKeeperUtil();
        zk.startCluster();
        try (var episode = new Episode(zk.getZooKeeperConnectString(), "/old-classic-abrupt")) {
            episode.writeAndReadConfirmedEntry();
            episode.abruptExit();
            assertThrows(IllegalStateException.class, episode::drained);
        } finally {
            zk.stopCluster();
        }
    }
}

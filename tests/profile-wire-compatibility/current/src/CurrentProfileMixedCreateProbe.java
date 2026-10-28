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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.segment.SegmentProfileInitialSocketClientTest;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.ProfileCreateRequest;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.discover.profile.ProfileInitialLifecycleCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryCoordinator;
import org.apache.bookkeeper.discover.profile.ProfileRecoveryPeerPool;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/** Standalone mixed-create probe reusing the actual file/ZK/mTLS SDK fixture without replacing production objects. */
public final class CurrentProfileMixedCreateProbe {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Class<?> TEST = SegmentProfileInitialSocketClientTest.class;

    private CurrentProfileMixedCreateProbe() {}

    /** The driver owns the old stock process and must stop it before commanding this fixture to EXIT. */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2 || !UUID.fromString(arguments[0]).toString().equals(arguments[0])) {
            throw new IllegalArgumentException("usage: CurrentProfileMixedCreateProbe <nonce> <fresh-receipt-root>");
        }
        String nonce = arguments[0];
        Path output = Files.createDirectory(Path.of(arguments[1])).toRealPath();
        var test = new SegmentProfileInitialSocketClientTest();
        test.temporary.create();
        Class<?> type = Class.forName(TEST.getName() + "$Fixture");
        var constructor = type.getDeclaredConstructor(TEST, boolean.class, boolean.class, boolean.class,
                int.class, int.class, boolean.class, boolean.class);
        constructor.setAccessible(true);
        Object fixture = constructor.newInstance(test, false, false, true, 3, 2, true, true);
        BookKeeper sdk = null;
        try (var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            var generation = type.getDeclaredField("helloReadinessGeneration");
            generation.setAccessible(true);
            generation.setLong(fixture, 1);
            ZKProfileRegistrationStore registry = register(fixture, 0);
            register(fixture, 1);
            // Establish the original SDK namespace before the driver starts the released Classic episode.
            try (BookKeeper initial = (BookKeeper) outer("sdk", new Class<?>[] {type}, fixture)) {
                if (initial.getLedgerManager() == null) {
                    throw new IllegalStateException("Fixture SDK did not start");
                }
            }
            String connect = ((ZooKeeperUtil) field(fixture, "cluster")).getZooKeeperConnectString();
            publish(output.resolve("ready.json"), Map.of("nonce", nonce, "pid", ProcessHandle.current().pid(),
                    "zkConnect", connect, "metadataUri", "zk+longhierarchical://" + connect + "/socket-ledgers",
                    "temporaryRoot", test.temporary.getRoot().getAbsolutePath(),
                    "classes", List.of(identity(TEST), identity(BookKeeper.class),
                            identity(CurrentProfileMixedCreateProbe.class))));
            marker("READY", nonce);
            String[] prepare = input.readLine().split(" ");
            if (prepare.length != 3 || !prepare[0].equals("PREPARE") || !prepare[1].equals(nonce)) {
                throw new IllegalStateException("Missing matching PREPARE");
            }
            BookieId old = BookieId.parse(prepare[2]);
            if (!old.toString().matches("127\\.0\\.0\\.1:[0-9]{4,5}")) {
                throw new IllegalArgumentException("Only the driver's private loopback Bookie is accepted");
            }
            ZooKeeper zk = (ZooKeeper) field(fixture, "zk");
            var stat = new Stat();
            byte[] registration = zk.getData("/socket-ledgers/available/" + old, false, stat);
            if (stat.getEphemeralOwner() == 0 || registry.readPeer(old.toString()).status()
                    != ProfileRegistrationStore.ReadStatus.NOT_FOUND) {
                throw new IllegalStateException("Old service must be live and have no Profile readiness authority");
            }
            var decoder = ZKRegistrationClient.class.getDeclaredMethod(
                    "deserializeBookieServiceInfo", BookieId.class, byte[].class);
            decoder.setAccessible(true);
            BookieServiceInfo info = (BookieServiceInfo) decoder.invoke(null, old, registration);
            var protocols = info.getEndpoints().stream().map(BookieServiceInfo.Endpoint::getProtocol).toList();
            if (!protocols.contains("bookie-rpc") || protocols.contains("bookie-profile")) {
                throw new IllegalStateException("Released service is not an actual Classic-only registration");
            }
            ProfileCreateRequest original = (ProfileCreateRequest) outer(
                    "publicCreateRequest", new Class<?>[] {type}, fixture);
            var scopes = new ArrayList<>(original.reservation().installs());
            ProfileControlScope prior = scopes.get(2);
            scopes.set(2, new ProfileControlScope(prior.operation(), prior.purpose(), prior.ledgerId(),
                    prior.ledgerInstanceId(), prior.descriptorIdentity(), old.toString(),
                    prior.targetStorageIncarnation(), prior.authorityDomain(), prior.authorityGeneration(),
                    prior.operationId(), prior.publicSemanticPayloadIdentity(), null));
            var cohort = scopes.stream().map(s -> BookieId.parse(s.targetBookie())).toList();
            var desired = LedgerMetadataBuilder.from(original.desired()).replaceEnsembleEntry(0, cohort).build();
            var mixed = new ProfileCreateRequest(desired,
                    new ProfileReservation(original.reservation().backlink(), scopes), original.descriptor(),
                    original.credential(), original.readyOperationId(), original.readyPayloadIdentity(),
                    original.limits());
            sdk = (BookKeeper) outer("sdk", new Class<?>[] {type}, fixture);
            var reservations = (ZKProfileReservationStore) field(fixture, "reservations");
            var manager = (AbstractZkLedgerManager) field(fixture, "manager");
            var metadata = new ProfileLedgerMetadataStore(zk, manager, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            var validator = (ProfileDescriptorValidator) staticField("VALIDATOR");
            var cold = (ThreadPoolExecutor) field(fixture, "cold");
            var pool = new ProfileRecoveryPeerPool(registry, (EventLoopGroup) field(fixture, "clientGroup"),
                    (SslContext) field(fixture, "peerTls"), cold, cohort,
                    new ProfileRecoveryPeerPool.Limits(6, 12, 2, 1060, Duration.ofSeconds(5)));
            sdk.enableProfile(new ProfileInitialLifecycleCoordinator(reservations, metadata, validator, cold),
                    new ProfileRecoveryCoordinator(reservations, metadata, cold), validator, pool, 4);
            publish(output.resolve("prepared.json"), Map.of("oldBookie", old.toString(),
                    "nativeRegistrationOwner", stat.getEphemeralOwner(), "nativeRegistrationBirth", stat.getCzxid(),
                    "nativeRegistrationSha256", hash(registration), "protocols", protocols,
                    "selectedE", cohort.stream().map(Object::toString).toList()));
            marker("PREPARED", nonce);
            require(input, "REJECT " + nonce);
            var before = namespace(zk);
            var filesBefore = storage(test.temporary.getRoot().toPath());
            var cutsBefore = cuts(fixture);
            requireNoClaim(fixture);
            int code;
            try {
                var unexpected = sdk.createProfileLedger(mixed).get(20, TimeUnit.SECONDS);
                unexpected.close();
                throw new IllegalStateException("Mixed initial E unexpectedly produced a writer");
            } catch (ExecutionException rejected) {
                code = BKException.getExceptionCode(rejected);
                if (code != BKException.Code.NotEnoughBookiesException) {
                    throw new IllegalStateException("Unexpected mixed-create rejection", rejected);
                }
            }
            sdk.close();
            sdk = null;
            if (pool.usage().reservedConnections() != 0 || pool.usage().reservedRpcSlots() != 0
                    || pool.backingBytes() != 0 || pool.codecWorkspaceBytes() != 0) {
                throw new IllegalStateException("Mixed SDK lease did not actually drain");
            }
            Map<?, ?> connected = (Map<?, ?>) field(pool, "incarnations");
            if (connected.size() != 2 || !connected.keySet().containsAll(cohort.subList(0, 2))
                    || connected.containsKey(old)) {
                throw new IllegalStateException("The two actual Profile targets were not both verified");
            }
            requireNoClaim(fixture);
            var after = namespace(zk);
            var filesAfter = storage(test.temporary.getRoot().toPath());
            var cutsAfter = cuts(fixture);
            if (!before.equals(after) || !filesBefore.equals(filesAfter) || !cutsBefore.equals(cutsAfter)) {
                throw new IllegalStateException("Mixed rejection changed namespace or local storage authority");
            }
            publish(output.resolve("rejected.json"), Map.of("status", "PASS", "resultCode", code,
                    "connectedProfilePeers", connected.keySet().stream().map(Object::toString).sorted().toList(),
                    "namespaceBefore", before, "namespaceAfter", after, "filesBefore", filesBefore,
                    "filesAfter", filesAfter, "cutsBefore", cutsBefore, "cutsAfter", cutsAfter,
                    "poolActualDrain", true));
            marker("REJECTED", nonce);
            require(input, "POSITIVE " + nonce);
            register(fixture, 2);
            try (BookKeeper positive = (BookKeeper) outer("sdk", new Class<?>[] {type}, fixture)) {
                var positivePool = (ProfileRecoveryPeerPool) outer("enablePublicCreate",
                        new Class<?>[] {BookKeeper.class, type, ZKProfileRegistrationStore.class, Duration.class},
                        positive, fixture, registry, Duration.ofSeconds(5));
                var writer = positive.createProfileLedger(original).get(20, TimeUnit.SECONDS);
                for (int target = 0; target < 3; target++) {
                    var normal = type.getDeclaredMethod("normal", int.class);
                    normal.setAccessible(true);
                    if (!(Boolean) normal.invoke(fixture, target)) {
                        throw new IllegalStateException("Positive Profile target was not activated");
                    }
                }
                if (writer.addEntry(new byte[] {9, 8, 7, 6}) != 0) {
                    throw new IllegalStateException("Positive Profile writer returned a wrong coordinate");
                }
                writer.closeAsync().get(30, TimeUnit.SECONDS);
                var closed = manager.readLedgerMetadata(44).get(5, TimeUnit.SECONDS).getValue();
                if (closed.getState() != LedgerMetadata.State.CLOSED || closed.getLastEntryId() != 0
                        || closed.getLength() != 4 || positivePool.usage().reservedConnections() != 0) {
                    throw new IllegalStateException("All-Profile create/write/close positive control failed");
                }
                try (var reader = positive.openProfileLedgerReadOnly(44, (byte[]) field(fixture, "password"))
                        .get(20, TimeUnit.SECONDS)) {
                    try (var read = reader.readAsync(0, 0).get(5, TimeUnit.SECONDS)) {
                        if (!Arrays.equals(new byte[] {9, 8, 7, 6}, read.getEntry(0).getEntryBytes())) {
                            throw new IllegalStateException("Positive Profile CLOSED payload changed");
                        }
                    }
                }
                publish(output.resolve("positive.json"), Map.of("status", "PASS", "ledger", 44,
                        "entry", 0, "length", 4, "state", "CLOSED", "activatedTargets", 3));
            }
            marker("POSITIVE", nonce);
            require(input, "EXIT " + nonce);
        } finally {
            if (sdk != null) {
                sdk.close();
            }
            ((AutoCloseable) fixture).close();
        }
        for (String name : List.of("acceptor", "worker", "clientGroup")) {
            if (!((EventLoopGroup) field(fixture, name)).isTerminated()) {
                throw new IllegalStateException("Borrowed fixture loop did not actually terminate");
            }
        }
        publish(output.resolve("closed.json"), Map.of("status", "CLOSED", "nonce", nonce,
                "pid", ProcessHandle.current().pid(), "fixtureCloseReturned", true, "eventLoopsTerminated", true));
        marker("CLOSED", nonce);
    }

    private static void requireNoClaim(Object fixture) throws Exception {
        var store = (ZKProfileReservationStore) field(fixture, "reservations");
        var manager = (AbstractZkLedgerManager) field(fixture, "manager");
        var zk = (ZooKeeper) field(fixture, "zk");
        if (store.read(44).status() != ProfileControlStore.ReadStatus.NOT_FOUND
                || zk.exists(manager.getLedgerPath(44), false) != null) {
            throw new IllegalStateException("Rejected mixed create left a fresh claim or ledger metadata");
        }
    }

    private static List<Long> cuts(Object fixture) throws Exception {
        var values = new ArrayList<Long>();
        for (Object storage : (List<?>) field(fixture, "nodes")) {
            Object bookie = field(storage, "bookie");
            var method = bookie.getClass().getDeclaredMethod("appliedThrough");
            method.setAccessible(true);
            values.add(((Number) method.invoke(bookie)).longValue());
        }
        return values;
    }

    private static ZKProfileRegistrationStore register(Object fixture, int node) throws Exception {
        var type = fixture.getClass();
        var identity = ((List<?>) field(fixture, "identities")).get(node);
        var inc = identity.getClass().getDeclaredMethod("incarnation");
        inc.setAccessible(true);
        Object incarnation = inc.invoke(identity);
        var value = incarnation.getClass().getDeclaredMethod("value");
        value.setAccessible(true);
        var channel = (io.netty.channel.Channel) ((List<?>) field(fixture, "listeners")).get(node);
        return (ZKProfileRegistrationStore) outer("registeredPeer",
                new Class<?>[] {type, ZooKeeper.class, int.class, String.class, long.class, byte[].class},
                fixture, field(fixture, "zk"), node, "tls://127.0.0.1:"
                        + ((java.net.InetSocketAddress) channel.localAddress()).getPort(), 1L,
                value.invoke(incarnation));
    }

    private static Map<String, String> namespace(ZooKeeper zk) throws Exception {
        var nodes = new TreeMap<String, String>();
        for (String root : List.of("/socket-ledgers", "/socket-initial", "/socket-readiness")) {
            walk(zk, root, nodes);
        }
        return nodes;
    }

    private static void walk(ZooKeeper zk, String path, Map<String, String> nodes) throws Exception {
        var stat = new Stat();
        byte[] data = zk.getData(path, false, stat);
        nodes.put(path, stat.getCzxid() + ":" + stat.getMzxid() + ":" + stat.getVersion() + ":" + stat.getCversion()
                + ":" + stat.getPzxid() + ":" + stat.getEphemeralOwner() + ":" + hash(data));
        for (String child : zk.getChildren(path, false)) {
            walk(zk, path + "/" + child, nodes);
        }
    }

    private static Map<String, String> storage(Path root) throws Exception {
        var files = new TreeMap<String, String>();
        try (var paths = Files.walk(root)) {
            for (Path p : paths.filter(Files::isRegularFile).sorted().toList()) {
                files.put(root.relativize(p).toString(), hash(Files.readAllBytes(p)));
            }
        }
        return files;
    }

    private static Object field(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Object staticField(String name) throws Exception {
        var field = TEST.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(null);
    }

    private static Object outer(String name, Class<?>[] types, Object... arguments) throws Exception {
        var method = TEST.getDeclaredMethod(name, types);
        method.setAccessible(true);
        try {
            return method.invoke(null, arguments);
        } catch (InvocationTargetException failure) {
            if (failure.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw failure;
        }
    }

    private static void require(BufferedReader input, String expected) throws Exception {
        if (!expected.equals(input.readLine())) {
            throw new IllegalStateException("Missing matching command " + expected);
        }
    }

    private static void marker(String phase, String nonce) {
        System.out.println(phase + " " + nonce);
        System.out.flush();
    }

    private static void publish(Path path, Object value) throws Exception {
        Files.write(path, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value),
                StandardOpenOption.CREATE_NEW);
    }

    private static String hash(byte[] bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static Map<String, String> identity(Class<?> type) throws Exception {
        try (var bytes = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            return Map.of("name", type.getName(), "codeSource",
                    type.getProtectionDomain().getCodeSource().getLocation().toExternalForm(),
                    "resourceSha256", hash(bytes.readAllBytes()));
        }
    }
}

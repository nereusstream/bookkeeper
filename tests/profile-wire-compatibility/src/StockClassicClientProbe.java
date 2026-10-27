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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.DefaultBookieAddressResolver;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookieClientImpl;

/** Actual released SDK write/close and CLOSED read; the process classpath must contain only its released artifacts. */
public final class StockClassicClientProbe {
    private StockClassicClientProbe() {}

    /** Run create or read against one explicitly private Classic metadata namespace. */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length < 4 || arguments.length > 5
                || !List.of("create", "read").contains(arguments[1])
                || (arguments[1].equals("create") && arguments.length != 4)
                || (arguments[1].equals("read") && arguments.length != 5)) {
            throw new IllegalArgumentException(
                    "usage: StockClassicClientProbe <version> <create|read> <metadata-uri> <receipt.json> [ledger-id]");
        }
        String version = arguments[0];
        if (!List.of("4.14.8", "4.15.5", "4.16.7", "4.17.2").contains(version)) {
            throw new IllegalArgumentException("Version is outside the locked released matrix");
        }
        URI uri = URI.create(arguments[2]);
        if (!"zk+hierarchical".equals(uri.getScheme()) || !"127.0.0.1".equals(uri.getHost())
                || uri.getPort() < 1024 || uri.getPort() > 65535 || uri.getUserInfo() != null
                || uri.getQuery() != null || uri.getFragment() != null
                || !uri.getPath().matches("/uw-classic-interop-[0-9a-f-]{36}")) {
            throw new IllegalArgumentException("Only an explicit private loopback metadata namespace is allowed");
        }
        Path output = Path.of(arguments[3]);
        if (Files.exists(output)) {
            throw new IllegalArgumentException("Receipt already exists");
        }
        var classes = new ArrayList<String>();
        for (Class<?> type : List.of(BookKeeper.class, LedgerHandle.class, BookieClientImpl.class,
                DefaultBookieAddressResolver.class)) {
            classes.add(classIdentity(type, version));
        }
        long ledgerId;
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(uri.toString());
        conf.setNumWorkerThreads(1);
        conf.setNumIOThreads(1);
        conf.setReadTimeout(10);
        List<String> hashes = new ArrayList<>();
        long lac;
        long length;
        try (BookKeeper sdk = new BookKeeper(conf)) {
            if (arguments[1].equals("create")) {
                try (LedgerHandle writer = sdk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, new byte[0])) {
                    ledgerId = writer.getId();
                    for (int entry = 0; entry < 2; entry++) {
                        if (writer.addEntry(payload(entry)) != entry) {
                            throw new IllegalStateException("Released SDK returned an unexpected entry identity");
                        }
                    }
                }
            } else {
                ledgerId = Long.parseLong(arguments[4]);
                if (ledgerId < 0) {
                    throw new IllegalArgumentException("Invalid ledger identity");
                }
            }
            try (LedgerHandle reader = sdk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32C, new byte[0])) {
                lac = reader.getLastAddConfirmed();
                length = reader.getLength();
                if (!reader.isClosed() || lac != 1 || length != 74) {
                    throw new IllegalStateException("Released SDK did not observe the expected CLOSED boundary");
                }
                var entries = reader.readEntries(0, 1);
                for (int entry = 0; entry < 2; entry++) {
                    if (!entries.hasMoreElements()) {
                        throw new IllegalStateException("Released SDK omitted an entry");
                    }
                    var actual = entries.nextElement();
                    byte[] value = actual.getEntry();
                    if (actual.getLedgerId() != ledgerId || actual.getEntryId() != entry
                            || !Arrays.equals(payload(entry), value)) {
                        throw new IllegalStateException("Released SDK read an unexpected entry identity or payload");
                    }
                    hashes.add(quote(sha256(value)));
                }
                if (entries.hasMoreElements()) {
                    throw new IllegalStateException("Released SDK returned extra entries");
                }
            }
        }
        String result = "{\"version\":" + quote(version) + ",\"mode\":" + quote(arguments[1])
                + ",\"pid\":" + ProcessHandle.current().pid() + ",\"javaHome\":"
                + quote(System.getProperty("java.home")) + ",\"javaVersion\":"
                + quote(System.getProperty("java.version")) + ",\"metadataUri\":" + quote(uri.toString())
                + ",\"ledgerId\":" + ledgerId + ",\"lac\":" + lac + ",\"length\":" + length
                + ",\"closed\":true,\"entryIds\":[0,1],\"payloadSha256\":[" + String.join(",", hashes)
                + "],\"classes\":[" + String.join(",", classes) + "],\"status\":\"PASS\"}\n";
        Files.writeString(output, result, StandardOpenOption.CREATE_NEW);
        System.out.println("SDK_CLOSED " + version + " " + ledgerId);
    }

    private static byte[] payload(int entry) {
        byte[] bytes = new byte[37];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) (17 + entry * 53 + index * 7);
        }
        return bytes;
    }

    private static String classIdentity(Class<?> type, String version) throws Exception {
        if (type.getProtectionDomain().getCodeSource() == null) {
            throw new IllegalStateException("Released class has no CodeSource: " + type.getName());
        }
        String source = type.getProtectionDomain().getCodeSource().getLocation().toExternalForm();
        if (!Path.of(URI.create(source)).getFileName().toString().equals("bookkeeper-server-" + version + ".jar")) {
            throw new IllegalStateException("Released class did not come from the requested server JAR");
        }
        try (var bytes = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
            if (bytes == null) {
                throw new IllegalStateException("Released class bytes are unavailable");
            }
            return "{\"name\":" + quote(type.getName()) + ",\"codeSource\":" + quote(source)
                    + ",\"resourceSha256\":" + quote(sha256(bytes.readAllBytes())) + "}";
        }
    }

    private static String sha256(byte[] value) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
    }
}

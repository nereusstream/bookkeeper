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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.segment;

import com.google.common.net.InetAddresses;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.interpol.ConfigurationInterpolator;

/** Clean, bounded configuration data. Neither parsed properties nor their digest grant startup authority. */
final class SegmentClassicLaunchConfiguration {
    static final int MAX_BYTES = 256 * 1024;
    private static final int MAX_PROPERTIES = 4096;
    private static final int MAX_CANONICAL_BYTES = 1024 * 1024;
    private static final byte[] DOMAIN = "BookKeeper/ControlledClassicConfiguration/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final List<String> REQUIRED = List.of("serverNumIOThreads", "numAddWorkerThreads",
            "numReadWorkerThreads", "numLongPollWorkerThreads", "numHighPriorityWorkerThreads",
            "numJournalCallbackThreads", "allocatorPoolingConcurrency", "journalMaxMemorySizeMb", "pageLimit",
            "bookieId", "advertisedAddress", "bookiePort", "journalDirectories", "ledgerDirectories",
            "indexDirectories", "metadataServiceUri");

    record Frozen(NavigableMap<String, Object> properties, SegmentClassicHandoff.Digest identity) {
        Frozen {
            properties = immutable(properties);
        }
    }

    private SegmentClassicLaunchConfiguration() { }

    /** Internal concrete consumer only; no mutable configuration is exported by the child lifecycle. */
    static ServerConfiguration materialize(Frozen frozen) throws IOException {
        if (!identity(frozen.properties()).equals(frozen.identity())) {
            throw new IOException("Frozen Classic configuration identity changed");
        }
        var conf = new ServerConfiguration();
        for (int i = conf.getNumberOfConfigurations() - 1; i >= 0; i--) {
            if (conf.getConfiguration(i) != conf.getInMemoryConfiguration()) {
                conf.removeConfiguration(conf.getConfiguration(i));
            }
        }
        conf.setInterpolator(new ConfigurationInterpolator());
        conf.setListDelimiterHandler(new DisabledListDelimiterHandler());
        frozen.properties().forEach(conf::setProperty);
        return conf;
    }

    static Frozen parse(byte[] supplied) throws IOException {
        if (supplied.length == 0 || supplied.length > MAX_BYTES) {
            throw new IOException("Controlled configuration exceeds its nonempty bounded input");
        }
        byte[] bytes = supplied.clone();
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        rejectInterpolation(text);

        // Use the existing logical-property reader and list conversion, never its include/URL loader.
        var properties = new PropertiesConfiguration();
        properties.setIncludesAllowed(false);
        properties.setInterpolator(new ConfigurationInterpolator());
        properties.setListDelimiterHandler(new DefaultListDelimiterHandler(','));
        try (var reader = new PropertiesConfiguration.PropertiesReader(new StringReader(text))) {
            int count = 0;
            while (reader.nextProperty()) {
                if (++count > MAX_PROPERTIES) {
                    throw new IOException("Controlled configuration property capacity exceeded");
                }
                String key = reader.getPropertyName();
                String value = reader.getPropertyValue();
                checkedText(key);
                checkedText(value);
                rejectInterpolation(key);
                rejectInterpolation(value);
                if (key.isEmpty() || key.equalsIgnoreCase("include") || key.equalsIgnoreCase("includeOptional")) {
                    throw new IOException("Controlled configuration cannot include another input");
                }
                properties.addProperty(key, value);
            }
        } catch (IllegalArgumentException failure) {
            throw new IOException("Invalid controlled properties syntax", failure);
        }

        ServerConfiguration conf = new ServerConfiguration();
        // AbstractConfiguration's static system-properties option may have been enabled before this call.
        for (int i = conf.getNumberOfConfigurations() - 1; i >= 0; i--) {
            var inherited = conf.getConfiguration(i);
            if (inherited != conf.getInMemoryConfiguration()) {
                conf.removeConfiguration(inherited);
            }
        }
        conf.setInterpolator(new ConfigurationInterpolator());
        conf.setListDelimiterHandler(new DisabledListDelimiterHandler());
        for (var keys = properties.getKeys(); keys.hasNext();) {
            String key = keys.next();
            conf.setProperty(key, properties.getProperty(key));
        }
        for (String key : REQUIRED) {
            if (!conf.containsKey(key) || conf.getProperty(key) instanceof List<?>
                    && !key.endsWith("Directories")) {
                throw new IOException("Controlled configuration requires one explicit value for " + key);
            }
        }
        if (conf.containsKey("journalDirectory") || conf.containsKey("zkServers")) {
            throw new IOException("Controlled configuration requires complete roots and the effective metadata URI");
        }
        try {
            // Validate raw lists before getJournalDirNames, whose legacy empty-string fallback is /tmp/bk-txn.
            roots(conf.getStringArray("journalDirectories"));
            roots(conf.getStringArray("ledgerDirectories"));
            roots(conf.getStringArray("indexDirectories"));
            // Resolve through actual getters, then write back typed values; never choose parent/JVM defaults.
            conf.setProperty("serverNumIOThreads", positive(conf.getServerNumIOThreads()));
            conf.setProperty("numAddWorkerThreads", nonnegative(conf.getNumAddWorkerThreads()));
            conf.setProperty("numReadWorkerThreads", nonnegative(conf.getNumReadWorkerThreads()));
            conf.setProperty("numLongPollWorkerThreads", positive(conf.getNumLongPollWorkerThreads()));
            conf.setProperty("numHighPriorityWorkerThreads", nonnegative(conf.getNumHighPriorityWorkerThreads()));
            conf.setProperty("numJournalCallbackThreads", positive(conf.getNumJournalCallbackThreads()));
            // Do not use the existing setter: it addresses allocatorPoolingPolicy instead of this property.
            conf.setProperty("allocatorPoolingConcurrency", positive(conf.getAllocatorPoolingConcurrency()));
            conf.setProperty("pageLimit", positive(conf.getPageLimit()));
            long journalMemory = conf.getJournalMaxMemorySizeMb();
            if (journalMemory <= 0) {
                throw new IllegalArgumentException("Controlled journal memory must be explicit and positive");
            }
            conf.setProperty("journalMaxMemorySizeMb", journalMemory);
            int port = conf.getBookiePort();
            if (port <= 0 || port > 65535 || !InetAddresses.isInetAddress(conf.getAdvertisedAddress())
                    || conf.getAdvertisedAddress().contains("%")) {
                throw new IllegalArgumentException("Controlled endpoint requires a fixed port and numeric address");
            }
            conf.setProperty("bookiePort", port);
            if (conf.getBookieId().isBlank()) {
                throw new IllegalArgumentException("Controlled Bookie identity must be explicit and nonempty");
            }
            conf.setProperty("bookieId", BookieImpl.getBookieId(conf).toString());
            conf.setProperty("advertisedAddress", BookieImpl.getBookieAddress(conf)
                    .getSocketAddress().getAddress().getHostAddress());
            conf.setProperty("journalDirectories", roots(conf.getJournalDirNames()));
            conf.setProperty("ledgerDirectories", roots(conf.getLedgerDirNames()));
            conf.setProperty("indexDirectories", roots(conf.getIndexDirNames()));
            String uri = conf.getMetadataServiceUri();
            if (uri == null || uri.isBlank()) {
                throw new IllegalArgumentException("Controlled metadata URI must be nonempty");
            }
            conf.setProperty("metadataServiceUri", uri);
            conf.validate();
        } catch (ConfigurationException | RuntimeException failure) {
            throw new IOException("Invalid controlled effective configuration", failure);
        }
        var values = new TreeMap<String, Object>();
        for (var keys = conf.getKeys(); keys.hasNext();) {
            String key = keys.next();
            values.put(key, conf.getProperty(key));
        }
        var snapshot = immutable(values);
        return new Frozen(snapshot, identity(snapshot));
    }

    private static int positive(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Explicit positive controlled concurrency required");
        }
        return value;
    }

    private static int nonnegative(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Controlled concurrency cannot use a dynamic fallback");
        }
        return value;
    }

    private static List<String> roots(String[] values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Complete nonempty controlled roots required");
        }
        var seen = new HashSet<Path>();
        List<String> result = new ArrayList<>();
        for (String value : values) {
            Path path = Path.of(value);
            if (!path.isAbsolute() || !path.normalize().equals(path) || path.getParent() == null
                    || !seen.add(path)) {
                throw new IllegalArgumentException("Controlled roots must be canonical, absolute and unique");
            }
            result.add(path.toString());
        }
        return List.copyOf(result);
    }

    private static void rejectInterpolation(String value) throws IOException {
        if (value.contains("${")) {
            throw new IOException("Controlled configuration interpolation is unsupported");
        }
    }

    private static byte[] checkedText(String value) {
        if (value == null || value.indexOf(0) >= 0 || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Controlled configuration text must be strict non-NUL UTF-8");
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static NavigableMap<String, Object> immutable(Map<String, ?> values) {
        if (values.isEmpty() || values.size() > MAX_PROPERTIES) {
            throw new IllegalArgumentException("Controlled frozen property count is out of bounds");
        }
        var copy = new TreeMap<String, Object>();
        for (var entry : values.entrySet()) {
            if (entry.getKey().isEmpty() || checkedText(entry.getKey()).length > 4096) {
                throw new IllegalArgumentException("Invalid controlled configuration key");
            }
            Object value = entry.getValue();
            if (value instanceof List<?> list) {
                if (list.size() > MAX_PROPERTIES) {
                    throw new IllegalArgumentException("Controlled property list exceeds its capacity");
                }
                var members = new ArrayList<>();
                for (Object member : list) {
                    members.add(scalar(member));
                }
                value = List.copyOf(members);
            } else {
                value = scalar(value);
            }
            copy.put(entry.getKey(), value);
        }
        return Collections.unmodifiableNavigableMap(copy);
    }

    private static Object scalar(Object value) {
        if (value instanceof String text) {
            if (checkedText(text).length > MAX_BYTES) {
                throw new IllegalArgumentException("Controlled property text exceeds its capacity");
            }
        } else if (!(value instanceof Boolean || value instanceof Integer || value instanceof Long
                || value instanceof Double number && Double.isFinite(number))) {
            throw new IllegalArgumentException("Unsupported or nonfinite controlled property value");
        }
        return value;
    }

    static SegmentClassicHandoff.Digest identity(Map<String, ?> supplied) throws IOException {
        var values = immutable(supplied);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException failure) {
            throw new IOException("Required configuration SHA-256 unavailable", failure);
        }
        digest.update(DOMAIN);
        try (var out = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
            out.writeLong(0x5557434c43303031L); // UWCLC001
            out.writeShort(1);
            out.writeShort(0);
            out.writeInt(values.size());
            for (var entry : values.entrySet()) {
                writeText(out, entry.getKey());
                if (entry.getValue() instanceof List<?> list) {
                    out.writeByte(6);
                    out.writeInt(list.size());
                    for (Object member : list) {
                        writeScalar(out, member);
                    }
                } else {
                    writeScalar(out, entry.getValue());
                }
                if (out.size() > MAX_CANONICAL_BYTES) {
                    throw new IOException("Controlled canonical configuration exceeds its byte capacity");
                }
            }
        }
        return new SegmentClassicHandoff.Digest(digest.digest());
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] text = checkedText(value);
        out.writeInt(text.length);
        out.write(text);
    }

    private static void writeScalar(DataOutputStream out, Object value) throws IOException {
        if (value instanceof String text) {
            out.writeByte(1);
            writeText(out, text);
        } else if (value instanceof Boolean flag) {
            out.writeByte(2);
            out.writeBoolean(flag);
        } else if (value instanceof Integer number) {
            out.writeByte(3);
            out.writeInt(number);
        } else if (value instanceof Long number) {
            out.writeByte(4);
            out.writeLong(number);
        } else if (value instanceof Double number) {
            out.writeByte(5);
            out.writeDouble(number);
        } else {
            throw new IllegalArgumentException("Unsupported controlled scalar type");
        }
    }
}

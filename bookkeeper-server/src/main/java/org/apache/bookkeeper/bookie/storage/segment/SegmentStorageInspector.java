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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.net.BookieId;

/** Offline experimental file observation. Does not sync, repair, accept a restore or open service authority. */
public final class SegmentStorageInspector {
    private SegmentStorageInspector() { }

    /** Run with bin/bookkeeper, the fully qualified class name, the directory and a control-log byte limit. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected storage directory and maximum control-log bytes");
        }
        Map<String, Object> observation = inspect(Path.of(args[0]), Long.parseLong(args[1]));
        System.out.println(new ObjectMapper().writeValueAsString(observation));
    }

    static Map<String, Object> inspect(Path directory, long maxControlBytes) throws IOException {
        Path scope = directory.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(scope)) {
            throw new IOException("Inspection directory cannot be a symlink");
        }
        // The lock files need write permission for exclusive locking; none is created or written.
        try (SegmentDataFileLock data = SegmentDataFileLock.open(scope);
                SegmentDataFileLock bookie = SegmentDataFileLock.openExistingControl(
                        scope.resolve("bookie-control.lock"))) {
            ByteBuffer header = ByteBuffer.allocate(4096);
            try (FileChannel file = FileChannel.open(scope.resolve("data-0"),
                    StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                SegmentFileIo.readFully(file, header, 0);
            }
            header.clear();
            // Derive a diagnostic scope, then validate it with the original immutable-header codec.
            int bookieBytes = header.getInt(120);
            if (bookieBytes < 1 || bookieBytes > 255) {
                throw new IOException("Invalid identity BookieId length");
            }
            byte[] incarnation = new byte[StorageIncarnation.LENGTH];
            header.get(16, incarnation);
            byte[] bookieId = new byte[bookieBytes];
            header.get(124, bookieId);
            SegmentFileHeader identity;
            IsolatedSegmentConfig config;
            try {
                identity = new SegmentFileHeader(new StorageIncarnation(incarnation),
                        new UUID(header.getLong(32), header.getLong(40)),
                        BookieId.parse(new String(bookieId, StandardCharsets.UTF_8)));
                config = new IsolatedSegmentConfig(scope, 1, header.getInt(92), header.getInt(88),
                        header.getLong(96), 4096, 1, 1, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            } catch (IllegalArgumentException | ArithmeticException invalid) {
                throw new IOException("Invalid observed storage identity or geometry", invalid);
            }
            identity.validate(header, config, 0);
            SegmentArenaControlLog.Inspection arena = SegmentArenaControlLog.inspect(config, identity, maxControlBytes);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("schemaVersion", 1);
            result.put("scope", "EXPERIMENTAL_OFFLINE_FILE_OBSERVATION");
            result.put("directory", scope.toString());
            result.put("authority", false);
            result.put("writable", false);
            result.put("recoverySyncExecuted", false);
            result.put("currentMetadataAuthorityChecked", false);
            result.put("bookieControlBodyChecked", false);
            result.put("dataBodyChecked", false);
            result.put("maximumControlLogBytes", maxControlBytes);
            result.put("observedIdentity", Map.of("bookieId", identity.bookieId().toString(),
                    "storageIncarnation", identity.incarnation().toString(), "arenaId", identity.arenaId().toString()));
            result.put("observedGeometry", Map.of("dataFiles", config.dataFiles(),
                    "logicalShards", config.logicalShards(), "bytesPerFile", config.bytesPerFile()));
            result.put("observedArena", arena);
            return result;
        }
    }
}

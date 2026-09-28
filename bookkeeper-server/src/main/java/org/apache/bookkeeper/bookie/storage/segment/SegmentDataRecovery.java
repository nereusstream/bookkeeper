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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Quiesced isolated DATA validation; zero pages and recovery sync never grant reuse or entry authority. */
final class SegmentDataRecovery {
    record Scan(long batches, long records, long validatedBytes, long zeroPages) { }

    static Scan inspect(IsolatedSegmentConfig config, SegmentFileHeader identity,
                        SegmentArenaControlLog authority) throws IOException {
        return scan(config, identity, authority, null);
    }

    /** A successful scan forces the checked DATA files; it does not establish Bookie or locator authority. */
    static Scan validateAndSync(IsolatedSegmentConfig config, SegmentFileHeader identity,
                                SegmentArenaControlLog authority, SegmentFileIo syncIo) throws IOException {
        if (syncIo == null) {
            throw new IllegalArgumentException("DATA recovery sync requires an I/O backend");
        }
        return scan(config, identity, authority, syncIo);
    }

    private static Scan scan(IsolatedSegmentConfig config, SegmentFileHeader identity,
                             SegmentArenaControlLog authority, SegmentFileIo syncIo) throws IOException {
        if (!authority.matchesScope(config, identity)) {
            throw new IOException("Arena authority does not match the DATA scope");
        }
        try (FileChannel guard = FileChannel.open(config.directory().resolve("isolated.lock"),
                StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            FileLock lock;
            try {
                lock = guard.tryLock();
            } catch (OverlappingFileLockException overlapping) {
                throw new IOException("DATA writer is active", overlapping);
            }
            if (lock == null) {
                throw new IOException("DATA writer is active");
            }
            try (lock) {
                long batches = 0;
                long records = 0;
                long validatedBytes = 0;
                long zeroPages = 0;
                for (int file = 0; file < config.dataFiles(); file++) {
                    try (FileChannel data = syncIo == null
                            ? FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                                    StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
                            : FileChannel.open(IsolatedSegmentFiles.dataPath(config, file),
                                    StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                        if (data.size() != config.bytesPerFile()) {
                            throw new IOException("DATA file size mismatch");
                        }
                        ByteBuffer header = ByteBuffer.allocate(4096);
                        SegmentFileIo.readFully(data, header, 0);
                        identity.validate(header, config, file);
                        for (long offset = 4096; offset < config.bytesPerFile();) {
                            ByteBuffer page = ByteBuffer.allocate(4096);
                            SegmentFileIo.readFully(data, page, offset);
                            if (allZero(page)) {
                                zeroPages++;
                                offset += 4096;
                                continue;
                            }
                            SegmentBatchFormat.Address address = address(page, offset);
                            if (address.fileIndex() != file
                                    || address.shard() % config.dataFiles() != file) {
                                throw new IOException("DATA batch claims a different physical file");
                            }
                            int length = SegmentBatchParser.batchLength(page.slice(0, 128), address,
                                    config.bytesPerFile());
                            if (!authority.hasAppliedGrant(file, offset, length, address.shard(),
                                    address.streamGeneration().value(), address.allocation(),
                                    address.generation().value())) {
                                throw new IOException("DATA batch has no matching applied grant");
                            }
                            ByteBuffer content = ByteBuffer.allocate(length);
                            SegmentFileIo.readFully(data, content, offset);
                            SegmentBatchParser.Batch parsed = SegmentBatchParser.parse(content, address,
                                    config.bytesPerFile());
                            batches++;
                            records += parsed.records().size();
                            validatedBytes += length;
                            offset += length;
                        }
                        if (syncIo != null) {
                            syncIo.force(data, false);
                        }
                    }
                }
                return new Scan(batches, records, validatedBytes, zeroPages);
            }
        }
    }

    private static SegmentBatchFormat.Address address(ByteBuffer page, long offset) throws IOException {
        try {
            return new SegmentBatchFormat.Address(page.getInt(16), page.getInt(20), page.getLong(24),
                    new Generation(page.getLong(32)), new Generation(page.getLong(48)),
                    page.getLong(56), offset);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid DATA batch address", invalid);
        }
    }

    private static boolean allZero(ByteBuffer page) {
        for (int i = 0; i < page.remaining(); i++) {
            if (page.get(i) != 0) {
                return false;
            }
        }
        return true;
    }

    private SegmentDataRecovery() {
    }
}

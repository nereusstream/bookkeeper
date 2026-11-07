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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.SecretKey;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
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

    record RecoveryDependencies(SegmentFileHeader identity, SegmentArenaControlLog.RecoveryDependencies arena,
                                SegmentBookieControlLog.RecoveryDependencies bookie) { }

    @FunctionalInterface
    interface DependencyObserver<T> {
        T observe(RecoveryDependencies dependencies) throws Exception;
    }

    @FunctionalInterface
    interface LockedScope<T> {
        T run(Path scope, SegmentDataFileLock data, SegmentDataFileLock arena, SegmentDataFileLock bookie,
                List<Path> locks) throws Exception;
    }

    static void requireClosedReconstructionScope(IsolatedSegmentConfig config, RecoveryDependencies dependencies,
            List<ZKProfileReservationStore.ClosedReconstructionSelf> members) throws IOException {
        var arena = dependencies.arena();
        var bookie = dependencies.bookie();
        if (config.dataFiles() != 1 || !arena.cut().sessionOpen() || arena.grants().isEmpty()
                || !arena.reconstructionCapacity()
                || !arena.cut().recordedQuarantineFiles().isEmpty() || arena.relocationOrReuseHistory()
                || !arena.selections().isEmpty() || !arena.retiredSlots().isEmpty() || !arena.retiredRanges().isEmpty()
                || bookie.classicHistory() || members == null || members.isEmpty()
                || bookie.ledgers().size() != members.size() || bookie.appliedRecoveryGrants() != members.size()
                || !bookie.tombstones().isEmpty() || bookie.deleteApplied() != null
                || bookie.deleteHandoff() || bookie.deleteEffective()) {
            throw new IOException("Original scope has unsupported CLOSED reconstruction dependencies");
        }
        var remaining = new HashMap<ProfileLedgerContext, SegmentBookieControlLog.ObservedLedger>();
        for (var local : bookie.ledgers()) {
            remaining.put(local.context(), local);
        }
        for (var self : members) {
            var local = remaining.remove(self.repair().intent().recovery().ledger());
            requireClosedReconstructionMember(local, self);
        }
    }

    private static void requireClosedReconstructionMember(SegmentBookieControlLog.ObservedLedger local,
            ZKProfileReservationStore.ClosedReconstructionSelf self) throws IOException {
        var intent = self.repair().intent();
        if (local == null || !local.fenced() || local.normalActive()
                || !Objects.equals(local.terminalPrefix(), intent.range().lastEntryId())
                || local.recoveryGrants().size() != 1) {
            throw new IOException("Original Bookie has another ledger, grant or terminal boundary");
        }
        var grant = local.recoveryGrants().get(0);
        if (!grant.committedReadable() || !grant.scope().equals(intent.grantScope())
                || !grant.intentId().equals(HexFormat.of().formatHex(intent.intentId()))
                || grant.intentGeneration() != intent.intentGeneration()
                || grant.grantGeneration() != intent.grantGeneration() || grant.grantSequence() < 0
                || grant.terminalSequence() <= grant.grantSequence()
                || !self.closeScope().equals(grant.terminalScope())) {
            throw new IOException("Original Bookie recovery grant and terminal result differ from current native SELF");
        }
    }

    /**
     * Observe the original single-Arena control scope without recovery barriers. The observer must finish using
     * its evidence before returning. No control owner, descriptor or write authority is exposed.
     */
    static <T> T withRecoveryDependencies(IsolatedSegmentConfig config, SegmentFileHeader identity,
            long maximumControlBytes, int maximumRows, SecretKey key, ProfileDescriptorValidator validator,
            DependencyObserver<T> observer) throws Exception {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(validator, "validator");
        Objects.requireNonNull(observer, "observer");
        return withRecoveryLocks(config, identity, (scope, data, arena, bookie, locks) -> {
            var before = readRecoveryDependencies(config, identity, maximumControlBytes, maximumRows,
                    key, validator, arena, bookie, locks);
            T result = observer.observe(before);
            data.requireHeld(scope.resolve("isolated.lock"));
            if (!before.equals(readRecoveryDependencies(config, identity, maximumControlBytes, maximumRows,
                    key, validator, arena, bookie, locks))) {
                throw new IOException("Recovery control dependencies changed during observation");
            }
            return result;
        });
    }

    static <T> T withRecoveryLocks(IsolatedSegmentConfig config, SegmentFileHeader identity, LockedScope<T> action)
            throws Exception {
        if (identity.manifest() != null) {
            throw new IOException("Recovery dependency observation requires an isolated single-Arena scope");
        }
        if (Files.isSymbolicLink(config.directory())) {
            throw new IOException("Recovery dependency observation requires the original canonical scope");
        }
        Path scope = config.directory().toRealPath();
        Path dataPath = scope.resolve("isolated.lock");
        Path arenaPath = scope.resolve("arena-control.lock");
        Path bookiePath = scope.resolve("bookie-control.lock");
        if (Files.isSameFile(dataPath, arenaPath) || Files.isSameFile(dataPath, bookiePath)
                || Files.isSameFile(arenaPath, bookiePath)) {
            throw new IOException("Recovery control locks must identify different files");
        }
        try (var data = SegmentDataFileLock.open(scope);
                var arena = SegmentDataFileLock.openExistingControl(arenaPath);
                var bookie = SegmentDataFileLock.openExistingControl(bookiePath)) {
            var locks = List.of(dataPath, arenaPath, bookiePath);
            data.requireHeld(dataPath);
            return action.run(scope, data, arena, bookie, locks);
        }
    }

    static RecoveryDependencies readRecoveryDependencies(IsolatedSegmentConfig config,
            SegmentFileHeader identity, long maximumControlBytes, int maximumRows, SecretKey key,
            ProfileDescriptorValidator validator, SegmentDataFileLock arena, SegmentDataFileLock bookie,
            List<Path> locks) throws Exception {
        if (maximumControlBytes < 6144 || maximumRows < 1 || maximumRows > 65536) {
            throw new IllegalArgumentException("Recovery dependency observation requires bounded control and rows");
        }
        var files = new SegmentOwnedFiles(locks);
        try (AutoCloseable cleanup = () -> {
            var result = files.drain();
            if (!result.drained()) {
                throw new IOException("Recovery control readers did not drain", result.closeFailure());
            }
        }) {
            var arenaDependencies = SegmentArenaControlLog.inspectRecoveryDependencies(config, identity,
                    maximumControlBytes, maximumRows, arena, files);
            var bookieDependencies = SegmentBookieControlLog.inspectRecoveryDependencies(config, identity,
                    maximumControlBytes, key, validator, maximumRows, bookie, files);
            return new RecoveryDependencies(identity, arenaDependencies, bookieDependencies);
        }
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
            try (FileChannel file = SegmentOwnedFiles.openRegularReadOnly(null, scope.resolve("data-0"))) {
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

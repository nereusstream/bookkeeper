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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.bookkeeper.common.profile.DurabilityMode;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.PayloadFormat;
import org.apache.bookkeeper.common.profile.ProfileDescriptor;
import org.apache.bookkeeper.common.profile.ProfileDescriptorCodec;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentBookieControlLogTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static final ProfileDescriptor DESCRIPTOR = new ProfileDescriptor(
            EngineProfile.SEGMENT_WAL_ENGINE, PayloadFormat.OPAQUE_LEDGER, DurabilityMode.SYNC_ON_ACK,
            3, 3, 2, 0, 7, 11, List.of());
    private static final ProfileDescriptorValidator VALIDATOR = new ProfileDescriptorValidator(
            (id, version) -> false, (id, generation) -> id == 7 && generation == 11);
    private static final byte[] DESCRIPTOR_BYTES = ProfileDescriptorCodec.encode(DESCRIPTOR, VALIDATOR);
    private static final ProfileDescriptorIdentity DESCRIPTOR_IDENTITY =
            ProfileDescriptorCodec.identityOf(DESCRIPTOR, VALIDATOR);

    private IsolatedSegmentConfig config() {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("bookie"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    private SegmentFileHeader identity() {
        return new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 7)), new UUID(0, 9),
                BookieId.parse("isolated:3181"));
    }

    private static SecretKey key(byte value) {
        return new SecretKeySpec(filled(32, value), "AES");
    }

    private static byte[] filled(int length, byte value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static ProfileLedgerContext context() {
        return new ProfileLedgerContext(44, filled(16, (byte) 1), DESCRIPTOR_IDENTITY.toBytes());
    }

    private static ProfileControlScope scope(ProfileControlOperation operation, int operationId, long generation) {
        byte[] id = new byte[16];
        id[15] = (byte) operationId;
        return new ProfileControlScope(operation, operation.purpose(), 44, filled(16, (byte) 1),
                DESCRIPTOR_IDENTITY, "isolated:3181", filled(16, (byte) 7), "ledger-profile", generation,
                id, filled(32, (byte) 4), null);
    }

    private static ProtectedProfileStateStore.Transition install(int operationId, byte verifier) {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, operationId, 1);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                EngineProfile.SEGMENT_WAL_ENGINE,
                ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1,
                        filled(20, verifier))),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 1, true), DESCRIPTOR);
    }

    private static ProtectedProfileStateStore.Transition activate(int operationId) {
        ProfileControlScope scope = scope(ProfileControlOperation.ACTIVATE_INITIAL, operationId, 2);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(scope),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.READY_INITIAL,
                        EngineProfile.SEGMENT_WAL_ENGINE, 2, true), null);
    }

    private static ProfileOperationCodec.FenceLedger fence(int operationId, byte verifier) {
        byte[] id = new byte[16];
        id[15] = (byte) operationId;
        return new ProfileOperationCodec.FenceLedger(context(), id, filled(20, verifier));
    }

    private static SegmentBatchFormat.FrozenBatch batch(IsolatedSegmentConfig config, long entryId,
                                                        long sequence, long offset) throws Exception {
        DigestManager digest = DigestManager.instantiate(context().ledgerId(), new byte[0], DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        ByteBufList encoded = (ByteBufList) digest.computeDigestAndPackageForSending(entryId, -1, 8,
                Unpooled.wrappedBuffer(new byte[8]), new byte[0], 0);
        ByteBuf entry = Unpooled.buffer(encoded.readableBytes());
        try {
            for (int i = 0; i < encoded.size(); i++) {
                ByteBuf part = encoded.getBuffer(i);
                entry.writeBytes(part, part.readerIndex(), part.readableBytes());
            }
            return SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(1), new Generation(1), sequence, offset),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
        } finally {
            entry.release();
            encoded.release();
        }
    }

    private static void grant(SegmentArenaControlLog arena, int blocks) throws IOException {
        arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                0, 0, new Generation(1), 5, 4096, blocks)));
    }

    @Test
    public void installAndActivationAreSeparateDurableGatesAcrossReopen() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitBatch(List.of(install(1, (byte) 5))).get(0).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitBatch(List.of(activate(2))).get(0).status());
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 6)));
            assertFalse(log.normalAdmission(new ProfileLedgerContext(44, filled(16, (byte) 8),
                    DESCRIPTOR_IDENTITY.toBytes()), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertTrue(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitBatch(List.of(install(1, (byte) 5))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.INCOMPATIBLE,
                    replay.submitBatch(List.of(install(1, (byte) 6))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitBatch(List.of(install(3, (byte) 5))).get(0).status());
            assertTrue(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void oneForceCanCoverOrderedInstallAndActivation() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentFileIo io = new SegmentFileIo();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR, io)) {
            SegmentBookieControlLog.DurableBatch durable = log.appendDurably(
                    List.of(install(1, (byte) 5), activate(2)));
            assertEquals(1, io.forces);
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            List<SegmentBookieControlLog.Result> results = log.applyDurable(durable);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, results.get(0).status());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, results.get(1).status());
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void authorityMismatchIsRejectedBeforeWriteAndBatchOrderCannotActivateAbsentRoute() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ProfileControlScope foreign = scope(ProfileControlOperation.INSTALL, 9, 1);
        ProtectedProfileStateStore.Transition install = install(1, (byte) 5);
        ProtectedProfileStateStore.Transition wrongAuthority = new ProtectedProfileStateStore.Transition(
                install.request(), new CommittedProfileAuthority(foreign,
                        CommittedProfileAuthority.State.PREPARING, EngineProfile.SEGMENT_WAL_ENGINE, 1, true),
                DESCRIPTOR);
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertThrows(IllegalArgumentException.class, () -> log.submitBatch(List.of(wrongAuthority)));
            assertEquals(4096, Files.size(config.directory().resolve("bookie-control")));
            List<SegmentBookieControlLog.Result> results = log.submitBatch(List.of(activate(2), install));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED, results.get(0).status());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, results.get(1).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    replay.submitBatch(List.of(activate(3))).get(0).status());
            assertTrue(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void unappliedRecordAndUncertainForceReplayWithoutEarlyAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.appendDurably(List.of(install(1, (byte) 5)));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR, new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false))) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertThrows(IOException.class, () -> replay.submitBatch(List.of(activate(2))));
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertTrue(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void wrongKeyAndCorruptTailFailClosed() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5)));
        }
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 4), VALIDATOR));
        SegmentFileHeader wrongIdentity = new SegmentFileHeader(
                new StorageIncarnation(filled(16, (byte) 8)), identity.arenaId(), identity.bookieId());
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, wrongIdentity,
                1024 * 1024, key((byte) 3), VALIDATOR));
        try (FileChannel channel = FileChannel.open(config.directory().resolve("bookie-control"),
                StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[] {1}), channel.size());
            channel.force(true);
        }
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR));
    }

    @Test
    public void boundedLogStopsBeforeWritingPastCapacity() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        long maxBytes = 4096 + 2048;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        maxBytes, key((byte) 3), VALIDATOR)) {
            int accepted = 0;
            for (int operationId = 1; operationId < 15; operationId++) {
                try {
                    log.submitBatch(List.of(install(operationId, (byte) 5)));
                    accepted++;
                } catch (IOException full) {
                    break;
                }
            }
            assertTrue(accepted > 0);
            assertTrue(accepted < 14);
            assertTrue(Files.size(config.directory().resolve("bookie-control")) <= maxBytes);
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void fenceCutsNewAdmissionAndWaitsForPrecutPhysicalTermination() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentBookieControlLog.Admission prior = log.admitNormal(context(), filled(20, (byte) 5))
                    .orElseThrow();
            assertEquals(2, prior.generation());
            long beforeFence = Files.size(config.directory().resolve("bookie-control"));
            assertEquals(SegmentBookieControlLog.FenceStatus.REJECTED,
                    log.submitFence(fence(3, (byte) 6)).status());
            assertEquals(SegmentBookieControlLog.FenceStatus.REJECTED,
                    log.submitFence(fence(1, (byte) 5)).status());
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertTrue(log.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
            assertEquals(beforeFence, Files.size(config.directory().resolve("bookie-control")));
            assertThrows(IOException.class, log::close);
            prior.physicalTerminal();
            assertThrows(IllegalArgumentException.class, prior::physicalTerminal);
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(activate(4))).get(0).status());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                    replay.submitFence(fence(3, (byte) 5)).status());
        }
    }

    @Test
    public void fencePreservesEarlierDurableActivationOrder() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5)));
            SegmentBookieControlLog.DurableBatch activation = log.appendDurably(List.of(activate(2)));
            assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.applyDurable(activation).get(0).status());
            assertThrows(IllegalArgumentException.class, () -> log.submitBatch(List.of(activate(4))));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void uncertainFenceForceNeverReopensNormalAdmissionInCurrentProcess() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false);
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR, io)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertThrows(IOException.class, () -> log.submitFence(fence(3, (byte) 5)));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertTrue(log.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                    replay.submitFence(fence(3, (byte) 5)).status());
        }
    }

    @Test
    public void slowFenceForceDoesNotHoldAdmissionLockOrPermitNewData() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger forces = new AtomicInteger();
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (forces.incrementAndGet() == 2) {
                forcing.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Fence force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        });
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR, io)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            AtomicReference<SegmentBookieControlLog.FenceResult> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    result.set(log.submitFence(fence(3, (byte) 5)));
                } catch (Throwable cause) {
                    failure.set(cause);
                }
            });
            worker.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(
                        () -> log.normalAdmission(context(), filled(20, (byte) 5)));
                new Thread(query).start();
                assertFalse(query.get(1, TimeUnit.SECONDS));
                assertThrows(IOException.class, log::close);
            } finally {
                release.countDown();
                worker.join(5000);
            }
            assertFalse(worker.isAlive());
            assertNull(failure.get());
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE, result.get().status());
        }
    }

    @Test
    public void realDataForceAndOwnerCompletionPrecedeDurableFence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch batch = batch(config, 1, 1, 4096);
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        AtomicReference<List<SegmentPointRead.Location>> candidates = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("DATA force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            });
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                SegmentBookieControlLog.Admission admitted = bookie.admitNormal(context(), filled(20, (byte) 5))
                        .orElseThrow();
                assertTrue(driver.submitNormal(bookie, batch, List.of(admitted), (locations, failure) -> {
                    owner.requireOwner();
                    candidates.set(locations);
                    dataFailure.set(failure);
                    completed.countDown();
                }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                assertTrue(bookie.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
                assertThrows(IOException.class, bookie::close);
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(1, candidates.get().size());
                assertEquals(1, SegmentPointRead.read(config, identity, arena, candidates.get().get(0))
                        .record().entryId());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                releaseForce.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void frozenNormalBatchRequiresOneDistinctTicketForEachRecord() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch batch = batch(config, 1, 1, 4096);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            try {
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                        new SegmentHotDirectory(owner, config, identity, 2));
                SegmentBookieControlLog.Admission repeated = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                assertThrows(IllegalArgumentException.class, () -> driver.submitNormal(bookie, batch,
                        List.of(repeated, repeated), (locations, failure) -> { }));
                repeated.physicalTerminal();
                SegmentBookieControlLog.Admission first = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                SegmentBookieControlLog.Admission extra = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                assertThrows(IOException.class, () -> driver.submitNormal(bookie, batch,
                        List.of(first, extra), (locations, failure) -> { }));
                assertEquals(4096, files.nextOffset(0));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void failedPrefixDoesNotReleaseLaterTicketBeforeItsIoTerminates() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch first = batch(config, 1, 1, 4096);
        SegmentBatchFormat.FrozenBatch second = batch(config, 2, 2, 8192);
        CountDownLatch firstForce = new CountDownLatch(1);
        CountDownLatch secondOperation = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch callbacks = new CountDownLatch(2);
        AtomicInteger operations = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 2);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false, () -> {
                firstForce.countDown();
                try {
                    if (!secondOperation.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Second DATA operation did not start");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            });
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2, 0, 0,
                    task -> new Thread(task, "isolated-bookie-data-io"), () -> {
                        if (operations.incrementAndGet() == 2) {
                            secondOperation.countDown();
                            try {
                                if (!releaseSecond.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("Second DATA operation was not released");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(interrupted);
                            }
                        }
                    });
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                assertTrue(driver.submitNormal(bookie, first, List.of(bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow()), (locations, failure) -> {
                            firstFailure.set(failure);
                            callbacks.countDown();
                        }));
                owner.start();
                assertTrue(firstForce.await(5, TimeUnit.SECONDS));
                assertTrue(driver.submitNormal(bookie, second, List.of(bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow()), (locations, failure) -> {
                            secondFailure.set(failure);
                            callbacks.countDown();
                        }));
                assertTrue(secondOperation.await(5, TimeUnit.SECONDS));
                assertTrue(callbacks.await(5, TimeUnit.SECONDS));
                assertTrue(firstFailure.get() instanceof IOException);
                assertTrue(secondFailure.get() instanceof IOException);
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                assertThrows(IOException.class, bookie::close);
                releaseSecond.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                releaseSecond.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }
}

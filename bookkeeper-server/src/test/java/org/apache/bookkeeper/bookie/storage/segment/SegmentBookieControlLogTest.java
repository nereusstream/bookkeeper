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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;
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
        return context(44);
    }

    private static ProfileLedgerContext context(long ledgerId) {
        return new ProfileLedgerContext(ledgerId, filled(16, (byte) 1), DESCRIPTOR_IDENTITY.toBytes());
    }

    private static ProfileControlScope scope(ProfileControlOperation operation, int operationId, long generation) {
        return scope(44, operation, operationId, generation);
    }

    private static ProfileControlScope scope(long ledgerId, ProfileControlOperation operation,
                                             int operationId, long generation) {
        byte[] id = new byte[16];
        id[15] = (byte) operationId;
        return new ProfileControlScope(operation, operation.purpose(), ledgerId, filled(16, (byte) 1),
                DESCRIPTOR_IDENTITY, "isolated:3181", filled(16, (byte) 7), "ledger-profile", generation,
                id, filled(32, (byte) 4), null);
    }

    private static ProtectedProfileStateStore.Transition install(int operationId, byte verifier) {
        return install(44, operationId, verifier);
    }

    private static ProtectedProfileStateStore.Transition install(long ledgerId, int operationId, byte verifier) {
        ProfileControlScope scope = scope(ledgerId, ProfileControlOperation.INSTALL, operationId, 1);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.install(scope, DESCRIPTOR_BYTES,
                EngineProfile.SEGMENT_WAL_ENGINE,
                ProtectedProfileCredential.of(ProtectedProfileCredential.BK_MASTER_KEY_SHA1,
                        filled(20, verifier))),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 1, true), DESCRIPTOR);
    }

    private static ProtectedProfileStateStore.Transition activate(int operationId) {
        return activate(44, operationId);
    }

    private static SegmentBookieControlLog.RecoveryGrant recoveryGrant(int operationId, int intentId,
                                                                        long intentGeneration, long grantGeneration) {
        ProfileControlScope base = scope(ProfileControlOperation.RECOVERY_GRANT, operationId, grantGeneration);
        ProfileControlScope unbound = new ProfileControlScope(base.operation(), base.purpose(), base.ledgerId(),
                base.ledgerInstanceId(), base.descriptorIdentity(), base.targetBookie(),
                base.targetStorageIncarnation(), base.authorityDomain(), base.authorityGeneration(),
                base.operationId(), base.publicSemanticPayloadIdentity(), new ProfileExactRange(2, 4));
        byte[] id = new byte[16];
        id[15] = (byte) intentId;
        ProfileControlScope exact = new ProfileControlScope(unbound.operation(), unbound.purpose(),
                unbound.ledgerId(), unbound.ledgerInstanceId(), unbound.descriptorIdentity(),
                unbound.targetBookie(), unbound.targetStorageIncarnation(), unbound.authorityDomain(),
                unbound.authorityGeneration(), unbound.operationId(),
                SegmentBookieControlLog.recoveryGrantPayloadIdentity(unbound, id, intentGeneration, grantGeneration),
                unbound.exactRange());
        return new SegmentBookieControlLog.RecoveryGrant(
                new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(exact),
                        new CommittedProfileAuthority(exact, CommittedProfileAuthority.State.RECOVERY_OPEN,
                                EngineProfile.SEGMENT_WAL_ENGINE, 3, true), null),
                id, intentGeneration, grantGeneration);
    }

    private static ProfileOperationCodec.AddRecovery recoveryAdd(int intentId, long intentGeneration,
                                                                  long grantGeneration, long entryId, byte verifier) {
        byte[] id = new byte[16];
        id[15] = (byte) intentId;
        return new ProfileOperationCodec.AddRecovery(context(), id, intentGeneration, grantGeneration,
                2, 4, entryId, filled(20, verifier), new byte[]{1});
    }

    private static ProfileOperationCodec.AddRecovery recoveryAddWithEntry(ByteBuffer entry) {
        byte[] id = new byte[16];
        id[15] = 3;
        byte[] payload = new byte[entry.remaining()];
        entry.slice().get(payload);
        return new ProfileOperationCodec.AddRecovery(context(), id, 7, 9,
                2, 4, 2, filled(20, (byte) 6), payload);
    }

    private static SegmentBookieControlLog.RecoveryGrantClose recoveryClose(int operationId, int grantOperationId,
                                                                             int intentId, long intentGeneration,
                                                                             long grantGeneration) {
        ProfileControlScope base = scope(ProfileControlOperation.RECOVERY_GRANT_CLOSE,
                operationId, Math.max(10, grantGeneration + 1));
        ProfileExactRange range = new ProfileExactRange(2, 4);
        ProfileControlScope unbound = new ProfileControlScope(base.operation(), base.purpose(), base.ledgerId(),
                base.ledgerInstanceId(), base.descriptorIdentity(), base.targetBookie(),
                base.targetStorageIncarnation(), base.authorityDomain(), base.authorityGeneration(),
                base.operationId(), base.publicSemanticPayloadIdentity(), range);
        byte[] id = new byte[16];
        id[15] = (byte) intentId;
        UUID grantId = new UUID(0, grantOperationId);
        ProfileControlScope exact = new ProfileControlScope(unbound.operation(), unbound.purpose(),
                unbound.ledgerId(), unbound.ledgerInstanceId(), unbound.descriptorIdentity(),
                unbound.targetBookie(), unbound.targetStorageIncarnation(), unbound.authorityDomain(),
                unbound.authorityGeneration(), unbound.operationId(),
                SegmentBookieControlLog.recoveryClosePayloadIdentity(unbound, grantId, id,
                        intentGeneration, grantGeneration), range);
        return new SegmentBookieControlLog.RecoveryGrantClose(
                new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(exact),
                        new CommittedProfileAuthority(exact,
                                CommittedProfileAuthority.State.RECOVERY_CLOSE_AUTHORIZED,
                                EngineProfile.SEGMENT_WAL_ENGINE, 4, true), null),
                grantId, id, intentGeneration, grantGeneration);
    }

    private static ProtectedProfileStateStore.Transition activate(long ledgerId, int operationId) {
        ProfileControlScope scope = scope(ledgerId, ProfileControlOperation.ACTIVATE_INITIAL, operationId, 2);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(scope),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.READY_INITIAL,
                        EngineProfile.SEGMENT_WAL_ENGINE, 2, true), null);
    }

    private static ProtectedProfileStateStore.Transition activateReplacement(long ledgerId, int operationId) {
        ProfileControlScope scope = scope(ledgerId, ProfileControlOperation.ACTIVATE_REPLACEMENT, operationId, 3);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(scope),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.POST_MEMBERSHIP_REPLACEMENT,
                        EngineProfile.SEGMENT_WAL_ENGINE, 3, true), null);
    }

    private static ProtectedProfileStateStore.Transition tombstone(long ledgerId, int operationId) {
        return tombstone(ledgerId, operationId, (byte) 1);
    }

    private static ProtectedProfileStateStore.Transition tombstone(long ledgerId, int operationId, byte instance) {
        ProfileControlScope base = scope(ledgerId, ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY,
                operationId, 4);
        ProfileControlScope scope = new ProfileControlScope(base.operation(), base.purpose(), ledgerId,
                filled(16, instance), DESCRIPTOR_IDENTITY, base.targetBookie(), base.targetStorageIncarnation(),
                base.authorityDomain(), base.authorityGeneration(), base.operationId(),
                base.publicSemanticPayloadIdentity(), null);
        return new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(scope),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.DELETE_AUTHORIZED,
                        EngineProfile.SEGMENT_WAL_ENGINE, 4, true), null);
    }

    private static ProfileOperationCodec.FenceLedger fence(int operationId, byte verifier) {
        byte[] id = new byte[16];
        id[15] = (byte) operationId;
        return new ProfileOperationCodec.FenceLedger(context(), id, filled(20, verifier));
    }

    private static ProfileOperationCodec.WriteLac writeLac(long lac, byte verifier, byte body) {
        return new ProfileOperationCodec.WriteLac(context(), lac, filled(20, verifier), new byte[] {body});
    }

    private static SegmentBatchFormat.FrozenBatch batch(IsolatedSegmentConfig config, long entryId,
                                                        long sequence, long offset) throws Exception {
        ByteBuf entry = entry(context(), entryId);
        try {
            return SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(1), new Generation(1), sequence, offset),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
        } finally {
            entry.release();
        }
    }

    private static SegmentBatchFormat.FrozenBatch secondStreamBatch(IsolatedSegmentConfig config, long entryId,
                                                                    long sequence) throws Exception {
        ByteBuf entry = entry(context(), entryId);
        try {
            return SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 6,
                    new Generation(1), new Generation(2), sequence, 4096 + SegmentArenaControlLog.BLOCK_BYTES),
                    List.of(SegmentBatchFormat.entry(context(), entry)));
        } finally {
            entry.release();
        }
    }

    private static SegmentEntryInputs.Input sourceInput(SegmentExecutionGroup group,
                                                        ProfileLedgerContext context, long entryId) throws Exception {
        ByteBuf encoded = entry(context, entryId);
        try {
            SegmentEntryInputs.Input input = group.inputs().acquire(context, encoded.readableBytes());
            assertTrue(input != null);
            input.bytes().writeBytes(encoded, encoded.readerIndex(), encoded.readableBytes());
            return input;
        } finally {
            encoded.release();
        }
    }

    private static SegmentEntryInputs.Input sourceInput(SegmentExecutionGroup group, ProfileLedgerContext context,
                                                        long entryId, long lac, long length, byte payload,
                                                        int payloadBytes) throws Exception {
        ByteBuf encoded = entry(context, entryId, lac, length, payload, payloadBytes);
        try {
            SegmentEntryInputs.Input input = group.inputs().acquire(context, encoded.readableBytes());
            assertTrue(input != null);
            input.bytes().writeBytes(encoded, encoded.readerIndex(), encoded.readableBytes());
            return input;
        } finally {
            encoded.release();
        }
    }

    private static SegmentDataPhysicalDriver.NormalBatcher normalBatcher(SegmentDataPhysicalDriver driver,
            SegmentOwnerLoop owner, SegmentBookieControlLog bookie, SegmentDataPhysicalDriver.BatchLimits limits)
            throws InterruptedException {
        AtomicReference<SegmentDataPhysicalDriver.NormalBatcher> batcher = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch ready = new CountDownLatch(1);
        assertTrue(owner.control(() -> {
            try {
                batcher.set(driver.enableNormalBatching(bookie, limits));
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                ready.countDown();
            }
        }));
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        return batcher.get();
    }

    private static SegmentDataPhysicalDriver.NormalRequest collectNormal(
            List<SegmentDataPhysicalDriver.NormalResult> results, AtomicReference<Throwable> failure,
            CountDownLatch done) {
        return new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
            results.add(result);
            if (cause != null) {
                failure.compareAndSet(null, cause);
            }
            done.countDown();
        });
    }

    private static void ownerBarrier(SegmentOwnerLoop owner) throws InterruptedException {
        ownerBarrier(owner, 0);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static void ownerBarrier(SegmentOwnerLoop owner, int shard) throws InterruptedException {
        CountDownLatch barrier = new CountDownLatch(1);
        assertTrue(owner.submit(shard, new SegmentOwnerLoop.Command() {
            @Override
            public void run(SegmentOwnerLoop.Ticket ticket) {
                ticket.complete(barrier::countDown);
            }

            @Override
            public void failed(Throwable failure) {
                throw new AssertionError(failure);
            }
        }));
        assertTrue(barrier.await(5, TimeUnit.SECONDS));
    }

    private static SegmentDataPhysicalDriver.NormalBatcher refillingBatcher(SegmentDataPhysicalDriver driver,
            SegmentOwnerLoop owner, SegmentBookieControlLog bookie, SegmentArenaControlDriver allocator,
            int poolBlocks) throws InterruptedException {
        AtomicReference<SegmentDataPhysicalDriver.NormalBatcher> batcher = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch ready = new CountDownLatch(1);
        assertTrue(owner.control(() -> {
            try {
                batcher.set(driver.enableNormalBatching(bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)),
                        allocator, poolBlocks));
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                ready.countDown();
            }
        }));
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        return batcher.get();
    }

    private static SegmentBatchFormat.FrozenBatch pooledBatch(IsolatedSegmentConfig config, long entryId,
                                                               long sequence, long offset,
                                                               SegmentBufferPool pool) throws Exception {
        ByteBuf entry = entry(context(), entryId);
        try {
            return SegmentBatchFormat.encodePooled(config, new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(1), new Generation(1), sequence, offset),
                    List.of(SegmentBatchFormat.entry(context(), entry)), pool);
        } finally {
            entry.release();
        }
    }

    private static ByteBuf entry(ProfileLedgerContext context, long entryId) throws Exception {
        return entry(context, entryId, -1, 8, (byte) 0);
    }

    private static ByteBuf entry(ProfileLedgerContext context, long entryId, long lac,
                                 long ledgerLength, byte payload) throws Exception {
        return entry(context, entryId, lac, ledgerLength, payload, 8);
    }

    private static ByteBuf entry(ProfileLedgerContext context, long entryId, long lac,
                                 long ledgerLength, byte payload, int payloadBytes) throws Exception {
        DigestManager digest = DigestManager.instantiate(context.ledgerId(), new byte[0], DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        ByteBufList encoded = (ByteBufList) digest.computeDigestAndPackageForSending(entryId, lac, ledgerLength,
                Unpooled.wrappedBuffer(filled(payloadBytes, payload)), new byte[0], 0);
        ByteBuf entry = Unpooled.buffer(encoded.readableBytes());
        try {
            for (int i = 0; i < encoded.size(); i++) {
                ByteBuf part = encoded.getBuffer(i);
                entry.writeBytes(part, part.readerIndex(), part.readableBytes());
            }
            return entry;
        } catch (RuntimeException | Error failure) {
            entry.release();
            throw failure;
        } finally {
            encoded.release();
        }
    }

    private static ByteBuffer entryBytes(ProfileLedgerContext context, long entryId, long lac,
                                         long ledgerLength, byte payload) throws Exception {
        ByteBuf entry = entry(context, entryId, lac, ledgerLength, payload);
        try {
            ByteBuffer result = ByteBuffer.allocate(entry.readableBytes());
            entry.getBytes(entry.readerIndex(), result);
            return result.flip();
        } finally {
            entry.release();
        }
    }

    private static SegmentBatchFormat.FrozenBatch twoLedgerBatch(IsolatedSegmentConfig config) throws Exception {
        ByteBuf first = entry(context(44), 1);
        ByteBuf second = entry(context(48), 2);
        try {
            return SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(44), first),
                            SegmentBatchFormat.entry(context(48), second)));
        } finally {
            first.release();
            second.release();
        }
    }

    private static void grant(SegmentArenaControlLog arena, int blocks) throws IOException {
        arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 5), new Generation(1), 0,
                0, 0, new Generation(1), 5, 4096, blocks, 1)));
    }

    private static SegmentDataPhysicalDriver.DuplicateStatus checkPublishedDuplicate(
            SegmentDataPhysicalDriver driver, SegmentBookieControlLog bookie, ByteBuffer entry) throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> status = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), entry,
                (result, cause) -> {
                    status.set(result);
                    failure.set(cause);
                    completed.countDown();
                }));
        assertTrue(completed.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        return status.get();
    }

    @Test
    public void recoveryGrantRequiresExactDurableBindingAndNeverActivatesNormalWrites() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 6)));
            assertFalse(log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isPresent());
            SegmentBookieControlLog.RecoveryGrant exact = recoveryGrant(2, 3, 7, 9);
            ProfileControlScope authorized = exact.transition().request().scope();
            ProfileControlScope unbound = new ProfileControlScope(authorized.operation(), authorized.purpose(),
                    authorized.ledgerId(), authorized.ledgerInstanceId(), authorized.descriptorIdentity(),
                    authorized.targetBookie(), authorized.targetStorageIncarnation(), authorized.authorityDomain(),
                    authorized.authorityGeneration(), authorized.operationId(), filled(32, (byte) 4),
                    authorized.exactRange());
            assertThrows(IllegalArgumentException.class, () -> log.submitRecoveryGrant(
                    new SegmentBookieControlLog.RecoveryGrant(
                            new ProtectedProfileStateStore.Transition(ProfileControlRequest.operation(unbound),
                                    new CommittedProfileAuthority(unbound,
                                            CommittedProfileAuthority.State.RECOVERY_OPEN,
                                            EngineProfile.SEGMENT_WAL_ENGINE, 3, true), null),
                            exact.repairIntentId(), 7, 9)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitRecoveryGrant(exact).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 6)));
            assertFalse(log.admitRecovery(recoveryAdd(4, 7, 9, 2, (byte) 6)).isPresent());
            assertFalse(log.admitRecovery(recoveryAdd(3, 8, 9, 2, (byte) 6)).isPresent());
            assertFalse(log.admitRecovery(recoveryAdd(3, 7, 10, 2, (byte) 6)).isPresent());
            assertFalse(log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 7)).isPresent());
            assertFalse(log.admitRecovery(new ProfileOperationCodec.AddRecovery(context(), exact.repairIntentId(),
                    7, 9, 3, 4, 3, filled(20, (byte) 6), new byte[]{1})).isPresent());
            SegmentBookieControlLog.RecoveryAdmission admission =
                    log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).orElseThrow();
            assertTrue(log.canCompleteRecovery(admission));
            admission.finish();
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    log.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9)).status());
            assertEquals(SegmentBookieControlLog.Status.INCOMPATIBLE,
                    log.submitRecoveryGrant(recoveryGrant(2, 3, 8, 9)).status());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR)) {
            SegmentBookieControlLog.RecoveryAdmission admission =
                    replay.admitRecovery(recoveryAdd(3, 7, 9, 4, (byte) 6)).orElseThrow();
            assertTrue(replay.canCompleteRecovery(admission));
            replay.submitBatch(List.of(tombstone(44, 4)));
            assertFalse(replay.canCompleteRecovery(admission));
            admission.finish();
            assertFalse(replay.admitRecovery(recoveryAdd(3, 7, 9, 3, (byte) 6)).isPresent());
        }
    }

    @Test
    public void recoveryCloseWaitsForPrecutTicketAndReplaysCommittedReadableFact() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 6)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9)).status());
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.CONDITION_FAILED,
                    log.submitRecoveryClose(recoveryClose(5, 2, 3, 8, 9)));
            SegmentBookieControlLog.RecoveryAdmission admission =
                    log.admitRecovery(recoveryAdd(3, 7, 9, 3, (byte) 6)).orElseThrow();
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT,
                    log.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            assertFalse(log.admitRecovery(recoveryAdd(3, 7, 9, 4, (byte) 6)).isPresent());
            assertFalse(log.canCompleteRecovery(admission));
            assertFalse(log.locallyCommittedReadable(context(), 3));
            admission.finish();
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                    log.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.ALREADY_DURABLE,
                    log.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            assertTrue(log.locallyCommittedReadable(context(), 3));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 6)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertTrue(replay.locallyCommittedReadable(context(), 2));
            assertFalse(replay.admitRecovery(recoveryAdd(3, 7, 9, 3, (byte) 6)).isPresent());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    replay.submitRecoveryGrant(recoveryGrant(6, 4, 1, 11)).status());
            assertFalse(replay.locallyCommittedReadable(context(), 2));
            SegmentBookieControlLog.RecoveryAdmission second =
                    replay.admitRecovery(recoveryAdd(4, 1, 11, 2, (byte) 6)).orElseThrow();
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT,
                    replay.submitRecoveryClose(recoveryClose(7, 6, 4, 1, 11)));
            assertFalse(replay.canCompleteRecovery(second));
            second.finish();
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                    replay.submitRecoveryClose(recoveryClose(7, 6, 4, 1, 11)));
            assertTrue(replay.locallyCommittedReadable(context(), 2));
            replay.submitBatch(List.of(tombstone(44, 4)));
            assertFalse(replay.locallyCommittedReadable(context(), 2));
        }
    }

    @Test
    public void uncertainRecoveryCloseForceNeverReopensAdmissionInCurrentProcess() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 3, false);
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR, io)) {
            log.submitBatch(List.of(install(1, (byte) 6)));
            log.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            assertThrows(IOException.class, () -> log.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            assertFalse(log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isPresent());
            assertFalse(log.locallyCommittedReadable(context(), 2));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertTrue(replay.locallyCommittedReadable(context(), 2));
            assertFalse(replay.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isPresent());
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.ALREADY_DURABLE,
                    replay.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
        }
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
    public void explicitLacIsMonotonicDurableAndFencedWithTheInstalledInstance() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
            log.submitBatch(List.of(activate(2)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
            assertEquals(9, log.explicitLac(context()).orElseThrow().lac());
            byte[] returned = log.explicitLac(context()).orElseThrow().body();
            returned[0] = 8;
            assertEquals(1, log.explicitLac(context()).orElseThrow().body()[0]);
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
            assertEquals(SegmentBookieControlLog.Status.INCOMPATIBLE,
                    log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 2)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitExplicitLac(writeLac(8, (byte) 5, (byte) 1)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitExplicitLac(writeLac(10, (byte) 6, (byte) 3)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitExplicitLac(writeLac(10, (byte) 5, (byte) 3)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(10, replay.explicitLac(context()).orElseThrow().lac());
            assertEquals(3, replay.explicitLac(context()).orElseThrow().body()[0]);
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitExplicitLac(writeLac(10, (byte) 5, (byte) 3)));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    replay.submitFence(fence(3, (byte) 5)).status());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitExplicitLac(writeLac(10, (byte) 5, (byte) 3)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    replay.submitExplicitLac(writeLac(11, (byte) 5, (byte) 4)));
            assertEquals(10, replay.explicitLac(context()).orElseThrow().lac());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    replay.submitBatch(List.of(tombstone(44, 20))).get(0).status());
            assertTrue(replay.explicitLac(context()).isEmpty());
        }
    }

    @Test
    public void uncertainExplicitLacForceIsNotAcknowledgedButReplaysWhenComplete() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 5), activate(2)));
        }
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR,
                new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false))) {
            assertThrows(IOException.class, () -> log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
            assertTrue(log.explicitLac(context()).isEmpty());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(9, replay.explicitLac(context()).orElseThrow().lac());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
        }
    }

    @Test
    public void pendingFenceWaitsForAnEarlierExplicitLacBarrier() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 5), activate(2)));
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Explicit LAC force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR, io)) {
            AtomicReference<SegmentBookieControlLog.Status> status = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    status.set(log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
                } catch (Throwable cause) {
                    failure.set(cause);
                }
            });
            writer.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        log.submitFence(fence(3, (byte) 5)).status());
                assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            } finally {
                release.countDown();
                writer.join(5000);
            }
            assertFalse(writer.isAlive());
            assertNull(failure.get());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, status.get());
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    log.submitFence(fence(3, (byte) 5)).status());
            assertEquals(9, log.explicitLac(context()).orElseThrow().lac());
        }
    }

    @Test
    public void tombstoneBeforeInstallRemainsTerminalAcrossReplay() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitBatch(List.of(tombstone(44, 20))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(install(21, (byte) 5))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(activate(22))).get(0).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitBatch(List.of(tombstone(44, 20))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    replay.submitBatch(List.of(install(23, (byte) 5))).get(0).status());
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void wrongInstanceTombstoneDoesNotCutActiveRoute() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(tombstone(44, 20, (byte) 8))).get(0).status());
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    log.submitBatch(List.of(tombstone(44, 21))).get(0).status());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.CONDITION_FAILED,
                    log.submitBatch(List.of(activate(22))).get(0).status());
            assertEquals(SegmentBookieControlLog.FenceStatus.REJECTED,
                    log.submitFence(fence(23, (byte) 5)).status());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    replay.submitBatch(List.of(tombstone(44, 21))).get(0).status());
        }
    }

    @Test
    public void fencedPhysicalCandidateRemainsLocallyReadableUntilTombstone() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch batch = batch(config, 1, 1, 4096);
        SegmentPointRead.Location location = SegmentPointRead.locations(
                SegmentBatchParser.parse(batch.bytes(), batch.address(), config.bytesPerFile())).get(0);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5)));
            SegmentBookieControlLog.ReadAdmission inactiveRead = bookie.admitRead(context()).orElseThrow();
            assertThrows(IOException.class, bookie::close);
            inactiveRead.finish();
            assertTrue(bookie.admitRead(new ProfileLedgerContext(44, filled(16, (byte) 8),
                    DESCRIPTOR_IDENTITY.toBytes())).isEmpty());
            files.writeAuthorized(batch, arena);
            assertEquals(1, SegmentPointRead.readLocallyGated(config, identity, arena, bookie, location)
                    .record().entryId());
            bookie.submitBatch(List.of(activate(2)));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    bookie.submitFence(fence(3, (byte) 5)).status());
            assertEquals(1, SegmentPointRead.readLocallyGated(config, identity, arena, bookie, location)
                    .record().entryId());
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(1, SegmentPointRead.readLocallyGated(config, identity, replayArena, replayBookie, location)
                    .record().entryId());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    replayBookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
            assertThrows(IOException.class,
                    () -> SegmentPointRead.readLocallyGated(config, identity, replayArena, replayBookie, location));
            assertEquals(1, SegmentPointRead.read(config, identity, replayArena, location).record().entryId());
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertThrows(IOException.class, () -> SegmentPointRead.readLocallyGated(
                    config, identity, replayArena, replayBookie, location));
        }
    }

    @Test
    public void replayedTombstoneExcludesOnlyItsLocationFromBoundedStreamRecovery() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch batch = twoLedgerBatch(config);
        List<SegmentPointRead.Location> physicalLocations = SegmentPointRead.locations(
                SegmentBatchParser.parse(batch.bytes(), batch.address(), config.bytesPerFile()));
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    bookie.submitBatch(List.of(install(44, 1, (byte) 5))).get(0).status());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    bookie.submitBatch(List.of(install(48, 2, (byte) 5))).get(0).status());
            files.writeAuthorized(batch, arena);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    bookie.submitBatch(List.of(tombstone(44, 3))).get(0).status());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalStreamCandidate(
                    config, identity, arena, new SegmentFileIo(), 0, new Generation(1), 1, 1, 1));
            SegmentDataRecovery.PhysicalStreamCandidate recovered =
                    SegmentDataRecovery.recoverLocallyReadableStreamCandidate(config, identity, arena,
                            bookie, new SegmentFileIo(), 0, new Generation(1), 1, 1, 1);
            assertEquals(2, recovered.scan().records());
            assertEquals(1, recovered.through());
            assertEquals(1, recovered.entries().size());
            assertEquals(context(48), recovered.entries().get(0).context());
            assertEquals(2, SegmentPointRead.readLocallyGated(config, identity, arena, bookie,
                    recovered.entries().get(0)).record().entryId());
            assertThrows(IOException.class, () -> SegmentPointRead.readLocallyGated(config, identity, arena,
                    bookie, physicalLocations.get(0)));
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                data.write(ByteBuffer.wrap(new byte[] {1}), physicalLocations.get(0).recordOffset() + 160 + 36);
            }
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverLocallyReadableStreamCandidate(
                    config, identity, arena, bookie, new SegmentFileIo(), 0, new Generation(1), 1, 1, 1));
        }
    }

    @Test
    public void pendingTombstoneCutsNewAdmissionBeforeForceReturns() throws Exception {
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
                        throw new IllegalStateException("Tombstone force was not released");
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
            SegmentBookieControlLog.ReadAdmission priorRead = log.admitRead(context()).orElseThrow();
            AtomicReference<List<SegmentBookieControlLog.Result>> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    result.set(log.submitBatch(List.of(tombstone(44, 20))));
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
                assertTrue(log.admitRead(context()).isEmpty());
                assertFalse(log.canCompleteRead(priorRead));
            } finally {
                release.countDown();
                worker.join(5000);
                priorRead.finish();
            }
            assertFalse(worker.isAlive());
            assertNull(failure.get());
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, result.get().get(0).status());
        }
    }

    @Test
    public void uncertainTombstoneForceNeverReopensNormalAdmissionInCurrentProcess() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false);
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR, io)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertThrows(IOException.class, () -> log.submitBatch(List.of(tombstone(44, 20))));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertTrue(log.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
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
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> results = new AtomicReference<>();
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
                assertTrue(driver.submitNormal(bookie, batch, List.of(admitted), (entries, failure) -> {
                    owner.requireOwner();
                    results.set(entries);
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
                assertEquals(1, results.get().size());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get().get(0).status());
                assertEquals(1, SegmentPointRead.read(config, identity, arena, results.get().get(0).candidate())
                        .record().entryId());
                CountDownLatch ownerFinished = new CountDownLatch(1);
                assertTrue(owner.control(ownerFinished::countDown));
                assertTrue(ownerFinished.await(5, TimeUnit.SECONDS));
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
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replayBookie.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                    replayBookie.submitFence(fence(3, (byte) 5)).status());
            SegmentDataRecovery.PhysicalStreamCandidate recovered =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity, replayArena,
                            new SegmentFileIo(), 0, new Generation(1), 1, 1, 1);
            assertEquals(1, recovered.through());
            assertEquals(1, recovered.entries().size());
            assertEquals(context(), recovered.entries().get(0).context());
            assertEquals(1, SegmentPointRead.read(config, identity, replayArena, recovered.entries().get(0))
                    .record().entryId());
        }
    }

    @Test
    public void automaticNormalRejectsSourceCapacityBelowUnifiedEntryBound() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch checked = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                assertTrue(owner.control(() -> {
                    try {
                        assertThrows(IllegalArgumentException.class, () -> driver.enableNormalBatching(bookie,
                                new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30))));
                    } catch (Throwable error) {
                        failure.set(error);
                    } finally {
                        checked.countDown();
                    }
                }));
                assertTrue(checked.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalCountBatchJoinsRetriesAndKeepsOneSharedForce() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(3);
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger stoppedCallbacks = new AtomicInteger();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
            try {
                SegmentDataPhysicalDriver.NormalRequest stopped = new SegmentDataPhysicalDriver.NormalRequest(
                        (entry, cause) -> stoppedCallbacks.incrementAndGet());
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), stopped));
                ownerBarrier(owner);
                assertEquals(44, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
                assertTrue(batcher.submit(sourceInput(group, context(), 1, 7, 8, (byte) 0, 8),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                ownerBarrier(owner);
                assertEquals(44, group.inputs().usedBytes());
                CountDownLatch conflicted = new CountDownLatch(1);
                List<SegmentDataPhysicalDriver.NormalResult> conflicts = new CopyOnWriteArrayList<>();
                assertTrue(batcher.submit(sourceInput(group, context(), 1, -1, 8, (byte) 9, 8),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(conflicts, failure, conflicted)));
                assertTrue(conflicted.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.CONFLICT, conflicts.get(0).status());
                assertEquals(44, group.inputs().usedBytes());
                assertTrue(stopped.stopWaiting());
                assertTrue(batcher.submit(sourceInput(group, context(48), 2),
                        bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(8192, files.nextOffset(0));
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                ownerBarrier(owner);
                assertEquals(0, group.inputs().usedBytes());
                release.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(0, stoppedCallbacks.get());
                assertEquals(1, files.dataIo.forces);
                for (SegmentDataPhysicalDriver.NormalResult result : results) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(1, result.candidate().address().sequence());
                    assertEquals(result.entryId(), SegmentPointRead.read(config, identity, arena, result.candidate())
                            .record().entryId());
                }
                CountDownLatch compared = new CountDownLatch(1);
                List<SegmentDataPhysicalDriver.NormalResult> duplicate = new CopyOnWriteArrayList<>();
                assertTrue(batcher.submit(sourceInput(group, context(), 1, 9, 8, (byte) 0, 8),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            duplicate.add(result);
                            if (cause != null) {
                                failure.set(cause);
                            }
                            if (group.inputs().usedBytes() != 44) {
                                failure.set(new AssertionError("Published comparison must still own its input"));
                            }
                            compared.countDown();
                        })));
                assertTrue(compared.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, duplicate.get(0).status());
                CountDownLatch publishedConflict = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 1, -1, 8, (byte) 9, 8),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(conflicts, failure, publishedConflict)));
                assertTrue(publishedConflict.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.CONFLICT, conflicts.get(1).status());
                assertEquals(1, files.dataIo.forces);
                assertEquals(8192, files.nextOffset(0));
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(5, (byte) 5)).status());
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalOldestDeadlineSealsBeforeLaterOwnerRound() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch observed = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 4, Duration.ofMillis(500)));
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                ownerBarrier(owner);
                assertTrue(owner.control(() -> {
                    blocked.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }));
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                TimeUnit.MILLISECONDS.sleep(600);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                    @Override
                    public void run(SegmentOwnerLoop.Ticket ticket) {
                        if (files.nextOffset(0) != 8192) {
                            failure.set(new AssertionError("New input postponed the oldest seal deadline"));
                        }
                        ticket.complete(observed::countDown);
                    }

                    @Override
                    public void failed(Throwable cause) {
                        failure.set(cause);
                        observed.countDown();
                    }
                }));
                release.countDown();
                assertTrue(observed.await(5, TimeUnit.SECONDS));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(2, results.size());
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalShutdownSealsAcceptedInputWithoutWaitingLongDeadline() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch done = new CountDownLatch(1);
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 4, Duration.ofSeconds(30)));
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                ownerBarrier(owner);
                assertEquals(44, group.inputs().usedBytes());
                assertEquals(0, files.dataIo.forces);
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertEquals(0, done.getCount());
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalRefusalsPreserveSequenceAndIoUnknownStopsNextSeal() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 8));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
            try {
                for (int attempt = 0; attempt < 3; attempt++) {
                    AtomicReference<SegmentBufferPool.Lease> held = new AtomicReference<>();
                    if (attempt == 1) {
                        grant(arena, 1);
                        CountDownLatch acquired = new CountDownLatch(1);
                        assertTrue(owner.control(() -> {
                            held.set(owner.pool().acquire());
                            acquired.countDown();
                        }));
                        assertTrue(acquired.await(5, TimeUnit.SECONDS));
                        assertTrue(held.get() != null);
                    }
                    List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
                    CountDownLatch done = new CountDownLatch(2);
                    for (int id = 1; id <= 2; id++) {
                        assertTrue(batcher.submit(sourceInput(group, context(), id),
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                collectNormal(results, failure, done)));
                    }
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertNull(failure.get());
                    for (SegmentDataPhysicalDriver.NormalResult result : results) {
                        assertEquals(attempt < 2 ? SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE
                                : SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                        if (attempt < 2) {
                            assertNull(result.candidate());
                        } else {
                            assertEquals(1, result.candidate().address().sequence());
                        }
                    }
                    assertEquals(attempt < 2 ? 4096 : 8192, files.nextOffset(0));
                    assertEquals(attempt < 2 ? 0 : 1, files.dataIo.forces);
                    assertEquals(0, group.inputs().usedBytes());
                    if (held.get() != null) {
                        held.get().close();
                    }
                }
                group.io().shutdown();
                for (int attempt = 0; attempt < 2; attempt++) {
                    CountDownLatch done = new CountDownLatch(2);
                    List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
                    for (int id = 3 + attempt * 2; id <= 4 + attempt * 2; id++) {
                        assertTrue(batcher.submit(sourceInput(group, context(), id),
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                collectNormal(results, failure, done)));
                    }
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertTrue(failure.get() instanceof IOException);
                    for (SegmentDataPhysicalDriver.NormalResult result : results) {
                        assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, result.status());
                        assertNull(result.candidate());
                    }
                    assertEquals(12288, files.nextOffset(0));
                    assertEquals(1, files.dataIo.forces);
                    assertEquals(0, group.inputs().usedBytes());
                }
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalBytesBoundarySealsLegalMaximumEntryAlone() throws Exception {
        IsolatedSegmentConfig base = config();
        int maximum = 1024 * 1024;
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory(), 1, 4, 1,
                base.bytesPerFile(), 4096, maximum, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        int bufferBytes = SegmentBatchFormat.alignedLength(SegmentBatchFormat.RECORD_HEADER
                + SegmentBatchFormat.ENTRY_OVERHEAD + maximum);
        long inputBudget = maximum + SegmentBatchFormat.ENTRY_OVERHEAD + 88L;
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(2);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 2, 1)));
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 2, bufferBytes, inputBudget,
                            2L * (bufferBytes + 4095) + inputBudget, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 4, Duration.ofSeconds(30)));
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                assertTrue(batcher.submit(sourceInput(group, context(), 2, -1, maximum + 8, (byte) 7, maximum),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(2, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                for (SegmentDataPhysicalDriver.NormalResult result : results) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(result.entryId(), result.candidate().address().sequence());
                    SegmentPointRead.Result read = SegmentPointRead.read(config, identity, arena, result.candidate());
                    assertEquals(result.entryId() == 2 ? maximum + SegmentBatchFormat.ENTRY_OVERHEAD : 44,
                            read.record().entry().remaining());
                }
                assertEquals(8192L + bufferBytes, files.nextOffset(0));
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticNormalOwnersUseIndependentPoolsInOneDataFile() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            arena.submitBatch(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 6), new Generation(1), 0, 0, 1, new Generation(1), 6, upper, 1, 1)));
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(45, 3, (byte) 5), activate(45, 4)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(2, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            try {
                for (int shard : new int[] {1, 0}) {
                    SegmentOwnerLoop owner = group.owner(shard);
                    SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(shard, owner, group.io(),
                            files, arena, new SegmentPhysicalPrefix(owner, shard, new Generation(1), 1, 4),
                            new SegmentHotDirectory(owner, config, identity, 4));
                    SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                            new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                    CountDownLatch done = new CountDownLatch(1);
                    ProfileLedgerContext context = context(44 + shard);
                    assertTrue(batcher.submit(sourceInput(group, context, 1),
                            bookie.admitNormal(context, filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, done)));
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    assertNull(failure.get());
                    SegmentDataPhysicalDriver.NormalResult result = results.get(results.size() - 1);
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(shard, result.candidate().address().shard());
                    assertEquals(shard == 1 ? upper : 4096, result.candidate().address().offset());
                    assertEquals(1, result.candidate().address().sequence());
                    assertEquals(context,
                            SegmentPointRead.read(config, identity, arena, result.candidate()).record().context());
                }
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertEquals(upper + 4096, files.nextOffset(0));
            assertEquals(2, files.dataIo.forces);
            assertEquals(0, group.inputs().usedBytes());
            assertNull(group.owner(0).failure());
            assertNull(group.owner(1).failure());
        }
    }

    @Test
    public void automaticNormalSkipsShortPoolTailAndOtherOwnerGrantWithoutSequenceGap() throws Exception {
        IsolatedSegmentConfig base = config();
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory(), 1, 4, 1,
                base.bytesPerFile(), 4096, 8192, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        long upper = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES;
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SegmentFileIo controlIo = new SegmentFileIo();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            arena.submitBatch(List.of(
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 6), new Generation(1), 0, 0, 1, new Generation(1), 6,
                            4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                            new UUID(0, 7), new Generation(1), 0, 0, 0, new Generation(1), 7, upper, 1, 1)));
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 2, 12288, 16384, 49150, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(8192, 1, Duration.ofSeconds(30)));
            try {
                ByteBuf first = entry(context(), 1);
                SegmentBatchFormat.FrozenBatch initial;
                try {
                    initial = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 5,
                            new Generation(1), new Generation(1), 1,
                            4096 + SegmentArenaControlLog.BLOCK_BYTES - 8192),
                            List.of(SegmentBatchFormat.entry(context(), first)));
                } finally {
                    first.release();
                }
                CountDownLatch firstDone = new CountDownLatch(1);
                assertTrue(driver.submitNormal(bookie, initial,
                        List.of(bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), (batch, cause) -> {
                            results.addAll(batch);
                            failure.set(cause);
                            firstDone.countDown();
                        }));
                assertTrue(firstDone.await(5, TimeUnit.SECONDS));
                for (int entryId : new int[] {2, 3}) {
                    CountDownLatch done = new CountDownLatch(1);
                    assertTrue(batcher.submit(entryId == 2
                                    ? sourceInput(group, context(), 2, -1, 8200, (byte) 7, 8192)
                                    : sourceInput(group, context(), 3),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, done)));
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                }
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(3, results.size());
                for (SegmentDataPhysicalDriver.NormalResult result : results) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(result.entryId(), result.candidate().address().sequence());
                    assertEquals(result.entryId(),
                            SegmentPointRead.read(config, identity, arena, result.candidate()).record().entryId());
                }
                assertEquals(upper, results.get(1).candidate().address().offset());
                assertEquals(7, results.get(1).candidate().address().allocation());
                assertEquals(upper + 12288, results.get(2).candidate().address().offset());
                assertEquals(upper + 16384, files.nextOffset(0));
                assertEquals(3, files.dataIo.forces);
                assertEquals(1, controlIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                try (java.nio.channels.FileChannel data = java.nio.channels.FileChannel.open(
                        IsolatedSegmentFiles.dataPath(config, 0))) {
                    ByteBuffer tail = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(data, tail, SegmentArenaControlLog.BLOCK_BYTES);
                    assertTrue(Arrays.equals(new byte[4096], tail.array()));
                }
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            SegmentDataRecovery.PhysicalStreamCandidate candidate = SegmentDataRecovery.recoverPhysicalStreamCandidate(
                    config, identity, replay, new SegmentFileIo(), 0, new Generation(1), 1, 3, 3);
            assertEquals(3, candidate.through());
            assertEquals(3, candidate.entries().size());
        }
    }

    @Test
    public void automaticPoolRefillGroupsOwnersAndKeepsSteadyWritesOffControlLog() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch arenaBlocked = new CountDownLatch(1);
        CountDownLatch releaseArena = new CountDownLatch(1);
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            atForce.countDown();
            await(releaseForce);
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(45, 1, (byte) 5), activate(45, 2),
                    install(46, 3, (byte) 5), activate(46, 4)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(3, 2, 6, 3, 4096, 4096, 28669, Duration.ofSeconds(5)));
            SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(group.owner(0), group.io(), arena);
            SegmentDataPhysicalDriver.NormalBatcher[] batchers = new SegmentDataPhysicalDriver.NormalBatcher[3];
            try {
                for (int shard : new int[] {1, 2}) {
                    SegmentOwnerLoop owner = group.owner(shard);
                    SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(shard, owner, group.io(),
                            files, arena, new SegmentPhysicalPrefix(owner, shard, new Generation(1), 1, 4),
                            new SegmentHotDirectory(owner, config, identity, 8));
                    batchers[shard] = refillingBatcher(driver, owner, bookie, allocator, 1);
                }
                assertTrue(group.owner(0).control(() -> {
                    arenaBlocked.countDown();
                    await(releaseArena);
                }));
                assertTrue(arenaBlocked.await(5, TimeUnit.SECONDS));
                CountDownLatch done = new CountDownLatch(3);
                for (int shard : new int[] {1, 2}) {
                    ProfileLedgerContext context = context(44 + shard);
                    assertTrue(batchers[shard].submit(sourceInput(group, context, 1),
                            bookie.admitNormal(context, filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, done)));
                    ownerBarrier(group.owner(shard), shard);
                }
                assertTrue(batchers[1].submit(sourceInput(group, context(45), 1),
                        bookie.admitNormal(context(45), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                CountDownLatch refused = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.NormalResult> refusal = new AtomicReference<>();
                assertTrue(batchers[1].submit(sourceInput(group, context(45), 2),
                        bookie.admitNormal(context(45), filled(20, (byte) 5)).orElseThrow(),
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            refusal.set(result);
                            refused.countDown();
                        })));
                assertTrue(refused.await(5, TimeUnit.SECONDS));
                ownerBarrier(group.owner(1), 1);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, refusal.get().status());
                assertEquals(88, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
                releaseArena.countDown();
                assertTrue(atForce.await(5, TimeUnit.SECONDS));
                assertTrue(arena.streamFirstSequence(1, 1).isEmpty());
                assertTrue(arena.streamFirstSequence(2, 1).isEmpty());
                assertEquals(0, files.dataIo.forces);
                assertFalse(allocator.requestPool(new SegmentArenaControlLog.PoolRequest(
                        3, new Generation(1), 1, 1), (unused, cause) -> { }));
                releaseForce.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                for (int shard : new int[] {1, 2}) {
                    CountDownLatch steady = new CountDownLatch(1);
                    ProfileLedgerContext context = context(44 + shard);
                    assertTrue(batchers[shard].submit(sourceInput(group, context, 2),
                            bookie.admitNormal(context, filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, steady)));
                    assertTrue(steady.await(5, TimeUnit.SECONDS));
                    ownerBarrier(group.owner(shard), shard);
                }
                assertNull(failure.get());
                assertEquals(5, results.size());
                for (SegmentDataPhysicalDriver.NormalResult result : results) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(result.entryId(), result.candidate().address().sequence());
                    assertEquals(result.context(),
                            SegmentPointRead.read(config, identity, arena, result.candidate()).record().context());
                }
                assertEquals(1, controlIo.forces);
                assertEquals(4, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
            } finally {
                releaseArena.countDown();
                releaseForce.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
            assertTrue(replay.streamFirstSequence(1, 1).isPresent());
            assertTrue(replay.streamFirstSequence(2, 1).isPresent());
            assertEquals(4, SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity, replay, new SegmentFileIo(), 4).entries().size());
        }
    }

    @Test
    public void automaticPoolRefillDrainsDuringShutdownForceFailureAndOwnerFailure() throws Exception {
        for (int mode : new int[] {0, 1, 2}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("refill-" + mode),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            CountDownLatch atForce = new CountDownLatch(1);
            CountDownLatch releaseForce = new CountDownLatch(1);
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, mode == 1 ? 1 : -1, false, () -> {
                atForce.countDown();
                await(releaseForce);
            });
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(
                            config, identity, 1024 * 1024, controlIo);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                bookie.submitBatch(List.of(install(45, 1, (byte) 5), activate(45, 2)));
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(2, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(1);
                SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(group.owner(0), group.io(), arena);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(1, owner, group.io(), files, arena,
                        new SegmentPhysicalPrefix(owner, 1, new Generation(1), 1, 4),
                        new SegmentHotDirectory(owner, config, identity, 4));
                SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(driver, owner, bookie, allocator, 1);
                try {
                    assertTrue(batcher.submit(sourceInput(group, context(45), 1),
                            bookie.admitNormal(context(45), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, done)));
                    assertTrue(atForce.await(5, TimeUnit.SECONDS));
                    assertEquals(44, group.inputs().usedBytes());
                    assertThrows(IOException.class, bookie::close);
                    if (mode == 2) {
                        CountDownLatch failed = new CountDownLatch(1);
                        assertTrue(owner.control(() -> {
                            failed.countDown();
                            throw new IllegalStateException("Refill waiter owner failed");
                        }));
                        assertTrue(failed.await(5, TimeUnit.SECONDS));
                    }
                    assertFalse(group.shutdown(Duration.ofMillis(50)));
                    releaseForce.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    assertEquals(mode == 0 ? SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE
                            : SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, results.get(0).status());
                    assertEquals(mode == 0 ? 8192 : 4096, files.nextOffset(0));
                    assertEquals(mode == 0 ? 1 : 0, files.dataIo.forces);
                    assertEquals(0, group.inputs().usedBytes());
                    if (mode == 0) {
                        assertNull(failure.get());
                    } else {
                        assertTrue(failure.get() != null);
                    }
                } finally {
                    releaseForce.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
            }
        }
    }

    @Test
    public void automaticPoolRefillStopsAtFixedCapacityWithoutConsumingSequence() throws Exception {
        int maximum = 1024 * 1024;
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("refill-full"),
                1, 4, 1, 4096 + 2L * maximum, 4096, maximum, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        SegmentFileIo controlIo = new SegmentFileIo();
        int bufferBytes = maximum + 4096;
        long sourceBytes = maximum + SegmentBatchFormat.ENTRY_OVERHEAD;
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, bufferBytes, sourceBytes,
                            bufferBytes + 4095L + sourceBytes, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(driver, owner, bookie, allocator, 2);
            try {
                for (int id = 1; id <= 3; id++) {
                    CountDownLatch done = new CountDownLatch(1);
                    assertTrue(batcher.submit(id < 3
                                    ? sourceInput(group, context(), id, -1, maximum + 8, (byte) 7, maximum)
                                    : sourceInput(group, context(), id),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, done)));
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertNull(failure.get());
                    assertEquals(0, group.inputs().usedBytes());
                }
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, results.get(1).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(2).status());
                assertEquals(1, results.get(0).candidate().address().sequence());
                assertEquals(2, results.get(2).candidate().address().sequence());
                assertEquals(4096L + bufferBytes + 4096, files.nextOffset(0));
                assertEquals(1, controlIo.forces);
                assertEquals(2, files.dataIo.forces);
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticPoolRefillSealsBeforeTwoLegalMaximumEntriesExceedBatchFormat() throws Exception {
        int maximum = 1024 * 1024;
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("refill-max-batch"),
                1, 4, 1, 4096 + 4L * maximum, 4096, maximum, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        long sourceBytes = 2L * (maximum + SegmentBatchFormat.ENTRY_OVERHEAD);
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SegmentFileIo controlIo = new SegmentFileIo();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 2, SegmentBatchFormat.MAX_BATCH, sourceBytes,
                            2L * (SegmentBatchFormat.MAX_BATCH + 4095) + sourceBytes, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            AtomicReference<SegmentDataPhysicalDriver.NormalBatcher> configured = new AtomicReference<>();
            CountDownLatch ready = new CountDownLatch(1);
            assertTrue(owner.control(() -> {
                configured.set(driver.enableNormalBatching(bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(SegmentBatchFormat.MAX_BATCH,
                                4, Duration.ofSeconds(30)), allocator, 4));
                ready.countDown();
            }));
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            SegmentDataPhysicalDriver.NormalBatcher batcher = configured.get();
            CountDownLatch firstDone = new CountDownLatch(1);
            CountDownLatch refused = new CountDownLatch(1);
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1, -1, maximum + 8, (byte) 7, maximum),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, firstDone)));
                assertTrue(batcher.submit(sourceInput(group, context(), 2, -1, maximum + 8, (byte) 7, maximum),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                                    result.status());
                            assertNull(cause);
                            refused.countDown();
                        })));
                assertTrue(refused.await(5, TimeUnit.SECONDS));
                assertTrue(firstDone.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                CountDownLatch lastDone = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2, -1, maximum + 8, (byte) 7, maximum),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, lastDone)));
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertTrue(lastDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(2, results.size());
                for (SegmentDataPhysicalDriver.NormalResult result : results) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                    assertEquals(result.entryId(), result.candidate().address().sequence());
                    assertEquals(maximum + SegmentBatchFormat.ENTRY_OVERHEAD,
                            SegmentPointRead.read(config, identity, arena, result.candidate())
                                    .record().entry().remaining());
                }
                assertEquals(1, controlIo.forces);
                assertEquals(2, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertNull(owner.failure());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void automaticNormalOwnerFailureDrainsUnsealedInputsAndAdmissions() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 4, Duration.ofSeconds(30)));
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                ownerBarrier(owner);
                assertEquals(44, group.inputs().usedBytes());
                assertTrue(owner.control(() -> {
                    throw new IllegalStateException("Owner failed before seal");
                }));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertTrue(failure.get() instanceof IllegalStateException);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, results.get(0).status());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertTrue(owner.failure() instanceof IllegalStateException);
        }
    }

    @Test
    public void automaticNormalCallbackFailureStillCompletesFollowersAndReturnsPool() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                    new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
            try {
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                            throw new IllegalStateException("Caller callback failed");
                        })));
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, done)));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertTrue(owner.failure() instanceof IllegalStateException);
        }
    }

    @Test
    public void normalEntryInputsBoundQueueCopyAndRealIoWithOneTicket() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch ownerBlocked = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicInteger stoppedCallbacks = new AtomicInteger();
        AtomicReference<SegmentDataPhysicalDriver.NormalResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SegmentDataPhysicalDriver.NormalRequest stopped = new SegmentDataPhysicalDriver.NormalRequest(
                (entry, cause) -> stoppedCallbacks.incrementAndGet());
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    assertTrue(releaseForce.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            });
            int sourceBytes = 2 * (SegmentBatchFormat.ENTRY_OVERHEAD + 8);
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, sourceBytes,
                            8191 + sourceBytes, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                assertTrue(owner.control(() -> {
                    ownerBlocked.countDown();
                    try {
                        assertTrue(releaseOwner.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }));
                assertTrue(ownerBlocked.await(5, TimeUnit.SECONDS));
                SegmentEntryInputs.Input first = sourceInput(group, context(44), 1);
                SegmentEntryInputs.Input second = sourceInput(group, context(48), 2);
                ByteBuf firstBytes = first.bytes();
                ByteBuf secondBytes = second.bytes();
                assertNull(group.inputs().acquire(context(), SegmentBatchFormat.ENTRY_OVERHEAD));
                assertTrue(driver.submitNormalInputs(bookie, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), 1, 4096), List.of(first, second), List.of(
                                bookie.admitNormal(context(44), filled(20, (byte) 5)).orElseThrow(),
                                bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        stopped, new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                                            result.set(entry);
                                            failure.set(cause);
                                            completed.countDown();
                                        }))));
                // Fill every remaining owner slot while its first command is still blocked.
                List<SegmentOwnerLoop.Ticket> placeholders = new CopyOnWriteArrayList<>();
                CountDownLatch placeholdersStarted = new CountDownLatch(config.maxPendingEntries() - 1);
                CountDownLatch placeholdersDone = new CountDownLatch(config.maxPendingEntries() - 1);
                for (int i = 1; i < config.maxPendingEntries(); i++) {
                    assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                        @Override
                        public void run(SegmentOwnerLoop.Ticket ticket) {
                            placeholders.add(ticket);
                            placeholdersStarted.countDown();
                        }

                        @Override
                        public void failed(Throwable cause) {
                            throw new AssertionError(cause);
                        }
                    }));
                }
                assertEquals(config.maxPendingEntries(), owner.pending());
                assertTrue(stopped.stopWaiting());
                assertThrows(IllegalStateException.class, first::bytes);
                assertThrows(IllegalStateException.class, first::close);
                assertEquals(sourceBytes, group.inputs().usedBytes());
                assertEquals(1, firstBytes.refCnt());
                releaseOwner.countDown();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(0, firstBytes.refCnt());
                assertEquals(0, secondBytes.refCnt());
                assertTrue(placeholdersStarted.await(5, TimeUnit.SECONDS));
                assertEquals(config.maxPendingEntries(), owner.pending());
                SegmentEntryInputs.Input refused = sourceInput(group, context(), 3);
                ByteBuf refusedBytes = refused.bytes();
                assertFalse(driver.submitNormalInputs(bookie, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), 2, 8192), List.of(refused), List.of(
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        new SegmentDataPhysicalDriver.NormalRequest((entry, cause) ->
                                                stoppedCallbacks.incrementAndGet()))));
                assertEquals(0, refusedBytes.refCnt());
                assertEquals(0, group.inputs().usedBytes());
                for (SegmentOwnerLoop.Ticket placeholder : placeholders) {
                    placeholder.complete(placeholdersDone::countDown);
                }
                assertTrue(placeholdersDone.await(5, TimeUnit.SECONDS));
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(0, stoppedCallbacks.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.get().status());
                assertEquals(2, SegmentPointRead.read(config, identity, arena, result.get().candidate())
                        .record().entryId());
                assertEquals(1, files.dataIo.forces);

                // A missing submission buffer is a refusal before any physical coordinate or force advances.
                AtomicReference<SegmentBufferPool.Lease> held = new AtomicReference<>();
                CountDownLatch acquired = new CountDownLatch(1);
                assertTrue(owner.control(() -> {
                    held.set(owner.pool().acquire());
                    acquired.countDown();
                }));
                assertTrue(acquired.await(5, TimeUnit.SECONDS));
                assertTrue(held.get() != null);
                SegmentEntryInputs.Input unavailable = sourceInput(group, context(), 3);
                CountDownLatch rejected = new CountDownLatch(1);
                assertTrue(driver.submitNormalInputs(bookie, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), 2, 8192), List.of(unavailable), List.of(
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                                            result.set(entry);
                                            failure.set(cause);
                                            rejected.countDown();
                                        }))));
                assertTrue(rejected.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, result.get().status());
                assertNull(result.get().candidate());
                assertEquals(8192, files.nextOffset(0));
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                held.get().close();
                ownerBarrier(owner);
                SegmentEntryInputs.Input retry = sourceInput(group, context(), 3);
                CountDownLatch retried = new CountDownLatch(1);
                assertTrue(driver.submitNormalInputs(bookie, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), 2, 8192), List.of(retry), List.of(
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                                            result.set(entry);
                                            failure.set(cause);
                                            retried.countDown();
                                        }))));
                assertTrue(retried.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.get().status());
                assertEquals(3, SegmentPointRead.read(config, identity, arena, result.get().candidate())
                        .record().entryId());
                assertEquals(2, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(5, (byte) 5)).status());
            } finally {
                releaseOwner.countDown();
                releaseForce.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void closedBookieClaimReturnsBothSourceAndSubmissionBuffers() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch checked = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentBookieControlLog.Admission old = bookie.admitNormal(context(), filled(20, (byte) 5))
                    .orElseThrow();
            old.physicalTerminal();
            bookie.close();
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                SegmentEntryInputs.Input input = sourceInput(group, context(), 1);
                assertThrows(IOException.class, () -> driver.submitNormalInputs(bookie,
                        new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                        List.of(input), List.of(old), List.of(new SegmentDataPhysicalDriver.NormalRequest(
                                (entry, cause) -> { }))));
                assertEquals(0, group.inputs().usedBytes());
                assertTrue(owner.control(() -> {
                    try {
                        SegmentBatchFormat.FrozenBatch normal = pooledBatch(config, 1, 1, 4096, owner.pool());
                        assertThrows(IOException.class, () -> driver.submitNormal(bookie, normal, List.of(old),
                                (entries, cause) -> { }));
                        assertThrows(IllegalStateException.class, normal::bytes);
                        SegmentBatchFormat.FrozenBatch recovery = pooledBatch(config, 1, 1, 4096, owner.pool());
                        assertTrue(recovery != null);
                        assertThrows(IOException.class, () -> driver.submitRecovery(bookie, recovery, List.of(),
                                (entries, cause) -> { }));
                        assertThrows(IllegalStateException.class, recovery::bytes);
                        try (SegmentBatchFormat.FrozenBatch reusable = pooledBatch(config, 1, 1, 4096, owner.pool())) {
                            assertTrue(reusable != null);
                        }
                    } catch (Throwable error) {
                        failure.set(error);
                    } finally {
                        checked.countDown();
                    }
                }));
                assertTrue(checked.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void queuedNormalEntryInputsDrainAfterOwnerFailureBeforeConversion() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<SegmentDataPhysicalDriver.NormalResult> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                assertTrue(owner.control(() -> {
                    blocked.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                    throw new IllegalStateException("Owner failed before input conversion");
                }));
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                SegmentEntryInputs.Input input = sourceInput(group, context(), 1);
                ByteBuf bytes = input.bytes();
                assertTrue(driver.submitNormalInputs(bookie, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), 1, 4096), List.of(input), List.of(
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                                            result.set(entry);
                                            failure.set(cause);
                                            completed.countDown();
                                        }))));
                release.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, result.get().status());
                assertTrue(failure.get() instanceof IllegalStateException);
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertEquals(0, bytes.refCnt());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertTrue(owner.failure() instanceof IllegalStateException);
        }
    }

    @Test
    public void normalEntryValidationAndStoppedOwnerReleaseInputsAndBookieTickets() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            SegmentBatchFormat.Address address = new SegmentBatchFormat.Address(0, 0, 5,
                    new Generation(1), new Generation(1), 1, 4096);
            List<SegmentDataPhysicalDriver.NormalRequest> requests = List.of(
                    new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                        throw new AssertionError("No submission expected");
                    }));
            try {
                SegmentEntryInputs.Input invalid = sourceInput(group, context(), 1);
                invalid.bytes().setByte(36, 99);
                assertThrows(IllegalArgumentException.class, () -> driver.submitNormalInputs(bookie, address,
                        List.of(invalid), List.of(bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        requests));
                assertEquals(0, group.inputs().usedBytes());
                SegmentEntryInputs.Input mismatched = sourceInput(group, context(), 1);
                assertThrows(IOException.class, () -> driver.submitNormalInputs(bookie, address,
                        List.of(mismatched), List.of(bookie.admitNormal(context(48), filled(20, (byte) 5))
                                .orElseThrow()), requests));
                assertEquals(0, group.inputs().usedBytes());
                SegmentEntryInputs.Input stopped = sourceInput(group, context(), 1);
                group.stopAdmission();
                assertNull(group.inputs().acquire(context(), 44));
                assertFalse(driver.submitNormalInputs(bookie, address, List.of(stopped), List.of(
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), requests));
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, files.dataIo.forces);
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(5, (byte) 5)).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void pooledDataSurvivesStoppedWaiterUntilIoAndOwnerFinish() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch encoded = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<SegmentBatchFormat.FrozenBatch> frozen = new AtomicReference<>();
        AtomicReference<SegmentDataPhysicalDriver.NormalResult> result = new AtomicReference<>();
        AtomicInteger stoppedCallbacks = new AtomicInteger();
        SegmentDataPhysicalDriver.NormalRequest stopped = new SegmentDataPhysicalDriver.NormalRequest(
                (entry, cause) -> stoppedCallbacks.incrementAndGet());
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Pooled DATA force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                assertTrue(owner.control(() -> {
                    try {
                        ByteBuf first = entry(context(44), 1);
                        ByteBuf second = entry(context(48), 2);
                        try {
                            frozen.set(SegmentBatchFormat.encodePooled(config, new SegmentBatchFormat.Address(
                                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096), List.of(
                                            SegmentBatchFormat.entry(context(44), first),
                                            SegmentBatchFormat.entry(context(48), second)), owner.pool()));
                        } finally {
                            first.release();
                            second.release();
                        }
                        assertTrue(driver.submitNormal(bookie, frozen.get(), List.of(
                                bookie.admitNormal(context(44), filled(20, (byte) 5)).orElseThrow(),
                                bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()), List.of(
                                        stopped, new SegmentDataPhysicalDriver.NormalRequest((entry, cause) -> {
                                            try {
                                                assertNull(cause);
                                                result.set(entry);
                                                assertNull(pooledBatch(config, 3, 2, 8192, owner.pool()));
                                            } catch (Throwable error) {
                                                failure.compareAndSet(null, error);
                                            } finally {
                                                completed.countDown();
                                            }
                                        }))));
                        assertThrows(IllegalStateException.class, frozen.get()::close);
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        encoded.countDown();
                    }
                }));
                assertTrue(encoded.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(stopped.stopWaiting());
                CountDownLatch checked = new CountDownLatch(1);
                assertTrue(owner.control(() -> {
                    try {
                        assertNull(pooledBatch(config, 3, 2, 8192, owner.pool()));
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        checked.countDown();
                    }
                }));
                assertTrue(checked.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                CountDownLatch reusable = new CountDownLatch(1);
                AtomicReference<SegmentBatchFormat.FrozenBatch> next = new AtomicReference<>();
                assertTrue(owner.control(() -> {
                    try {
                        next.set(pooledBatch(config, 3, 2, 8192, owner.pool()));
                        assertTrue(next.get() != null);
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        reusable.countDown();
                    }
                }));
                assertTrue(reusable.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(0, stoppedCallbacks.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.get().status());
                assertEquals(2, SegmentPointRead.read(config, identity, arena, result.get().candidate())
                        .record().entryId());
                assertThrows(IllegalStateException.class, frozen.get()::bytes);
                group.stopAdmission();
                assertFalse(driver.submitNormal(bookie, next.get(), List.of(
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (entries, cause) -> { }));
                assertThrows(IllegalStateException.class, next.get()::bytes);
            } finally {
                releaseForce.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void pooledValidationAndUnstartedIoRejectionReturnWorkingSet() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            group.io().shutdown();
            try {
                assertTrue(owner.control(() -> {
                    try {
                        SegmentBatchFormat.FrozenBatch invalid = pooledBatch(config, 1, 1, 4096, owner.pool());
                        assertThrows(IOException.class, () -> driver.submitNormal(bookie, invalid, List.of(
                                bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()),
                                (entries, cause) -> { }));
                        assertThrows(IllegalStateException.class, invalid::bytes);
                        SegmentBatchFormat.FrozenBatch valid = pooledBatch(config, 1, 1, 4096, owner.pool());
                        assertTrue(driver.submitNormal(bookie, valid, List.of(
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                                (entries, cause) -> {
                                    try {
                                        assertTrue(cause instanceof IOException);
                                        assertTrue(cause.getCause() instanceof RejectedExecutionException);
                                        assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN,
                                                entries.get(0).status());
                                    } catch (Throwable error) {
                                        failure.compareAndSet(null, error);
                                    } finally {
                                        completed.countDown();
                                    }
                                }));
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                        completed.countDown();
                    }
                }));
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                CountDownLatch returned = new CountDownLatch(1);
                assertTrue(owner.control(() -> {
                    try (SegmentBatchFormat.FrozenBatch retry = pooledBatch(config, 2, 2, 8192, owner.pool())) {
                        assertTrue(retry != null);
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        returned.countDown();
                    }
                }));
                assertTrue(returned.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(8192, files.nextOffset(0));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(5, (byte) 5)).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void sharedDataBatchDrainsBothPrecutAdmissionsBeforeFence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> results = new AtomicReference<>();
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
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
                assertTrue(driver.submitNormal(bookie, twoLedgerBatch(config), List.of(
                        bookie.admitNormal(context(44), filled(20, (byte) 5)).orElseThrow(),
                        bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()),
                        (entries, failure) -> {
                            owner.requireOwner();
                            results.set(entries);
                            dataFailure.set(failure);
                            completed.countDown();
                        }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(5, (byte) 5)).status());
                assertTrue(bookie.admitNormal(context(44), filled(20, (byte) 5)).isEmpty());
                assertTrue(bookie.normalAdmission(context(48), filled(20, (byte) 5)));
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(2, results.get().size());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        results.get().get(0).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        results.get().get(1).status());
                CountDownLatch ownerFinished = new CountDownLatch(1);
                assertTrue(owner.control(ownerFinished::countDown));
                assertTrue(ownerFinished.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(5, (byte) 5)).status());
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
    public void recoveryTicketBindsRealDataAndCloseWaitsForPhysicalTermination() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        SegmentBatchFormat.FrozenBatch batch = batch(config, 2, 1, 4096);
        ByteBuffer entry = entryBytes(context(), 2, -1, 8, (byte) 0);
        byte[] payload = new byte[entry.remaining()];
        entry.get(payload);
        byte[] intentId = new byte[16];
        intentId[15] = 3;
        ProfileOperationCodec.AddRecovery request = new ProfileOperationCodec.AddRecovery(context(),
                intentId, 7, 9, 2, 4, 2, filled(20, (byte) 6), payload);
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        AtomicReference<List<SegmentDataPhysicalDriver.RecoveryResult>> results = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Recovery DATA force was not released");
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
                assertThrows(IOException.class, () -> driver.submitRecovery(bookie,
                        batch(config, 3, 1, 4096), List.of(bookie.admitRecovery(request).orElseThrow()),
                        (unused, failure) -> { }));
                ByteBuf changed = entry(context(), 2, -1, 8, (byte) 1);
                try {
                    SegmentBatchFormat.FrozenBatch wrongPayload = SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, 5, new Generation(1),
                                    new Generation(1), 1, 4096),
                            List.of(SegmentBatchFormat.entry(context(), changed)));
                    assertThrows(IOException.class, () -> driver.submitRecovery(bookie, wrongPayload,
                            List.of(bookie.admitRecovery(request).orElseThrow()), (unused, failure) -> { }));
                } finally {
                    changed.release();
                }
                SegmentBookieControlLog.RecoveryAdmission admitted = bookie.admitRecovery(request).orElseThrow();
                assertTrue(driver.submitRecovery(bookie, batch, List.of(admitted), (entries, failure) -> {
                            owner.requireOwner();
                            results.set(entries);
                            dataFailure.set(failure);
                            completed.countDown();
                        }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(IllegalArgumentException.class, admitted::finish);
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT,
                        bookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
                assertTrue(bookie.admitRecovery(request).isEmpty());
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.STALE_ADMISSION,
                        results.get().get(0).status());
                assertEquals(2, SegmentPointRead.read(config, identity, arena,
                        results.get().get(0).candidate()).record().entryId());
            } finally {
                releaseForce.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
            assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                    bookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            assertTrue(bookie.locallyCommittedReadable(context(), 2));
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            SegmentDataRecovery.PhysicalStreamCandidate recovered =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity, replayArena,
                            new SegmentFileIo(), 0, new Generation(1), 1, 1, 1);
            assertEquals(1, recovered.entries().size());
            assertEquals(2, recovered.entries().get(0).entryId());
            assertTrue(replayBookie.locallyCommittedReadable(context(), 2));
            assertTrue(replayBookie.admitRecovery(request).isEmpty());
        }
    }

    @Test
    public void recoveryDuplicateUsesDurablePayloadIdentityWithoutAnotherWrite() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        byte[] intentId = new byte[16];
        intentId[15] = 3;
        ByteBuffer firstEntry = entryBytes(context(), 2, -1, 8, (byte) 0);
        byte[] firstPayload = new byte[firstEntry.remaining()];
        firstEntry.get(firstPayload);
        ByteBuffer conflictingEntry = entryBytes(context(), 2, -1, 8, (byte) 1);
        byte[] conflictingPayload = new byte[conflictingEntry.remaining()];
        conflictingEntry.get(conflictingPayload);
        ProfileOperationCodec.AddRecovery first = new ProfileOperationCodec.AddRecovery(context(),
                intentId, 7, 9, 2, 4, 2, filled(20, (byte) 6), firstPayload);
        ProfileOperationCodec.AddRecovery conflicting = new ProfileOperationCodec.AddRecovery(context(),
                intentId, 7, 9, 2, 4, 2, filled(20, (byte) 6), conflictingPayload);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                CountDownLatch written = new CountDownLatch(1);
                AtomicReference<Throwable> writeFailure = new AtomicReference<>();
                AtomicReference<SegmentDataPhysicalDriver.RecoveryStatus> writeStatus = new AtomicReference<>();
                assertTrue(driver.submitRecovery(bookie, batch(config, 2, 1, 4096),
                        List.of(bookie.admitRecovery(first).orElseThrow()), (entries, failure) -> {
                            writeFailure.set(failure);
                            writeStatus.set(entries.get(0).status());
                            written.countDown();
                        }));
                owner.start();
                assertTrue(written.await(5, TimeUnit.SECONDS));
                assertNull(writeFailure.get());
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE, writeStatus.get());

                CountDownLatch sameDone = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> same = new AtomicReference<>();
                assertTrue(driver.checkPublishedRecoveryDuplicate(bookie,
                        bookie.admitRecovery(first).orElseThrow(), (status, failure) -> {
                            assertNull(failure);
                            same.set(status);
                            sameDone.countDown();
                        }));
                assertTrue(sameDone.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE, same.get());

                CountDownLatch conflictDone = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> conflict = new AtomicReference<>();
                assertTrue(driver.checkPublishedRecoveryDuplicate(bookie,
                        bookie.admitRecovery(conflicting).orElseThrow(), (status, failure) -> {
                            assertNull(failure);
                            conflict.set(status);
                            conflictDone.countDown();
                        }));
                assertTrue(conflictDone.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.CONFLICT, conflict.get());
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void recoveredRecoveryCandidateRequiresSyncAndSingleCurrentStream() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ProfileOperationCodec.AddRecovery same = recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
        ProfileOperationCodec.AddRecovery changedLac = recoveryAddWithEntry(entryBytes(context(), 2, 1, 8, (byte) 0));
        ProfileOperationCodec.AddRecovery conflict = recoveryAddWithEntry(entryBytes(context(), 2, -1, 9, (byte) 0));
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            files.writeAuthorized(batch(config, 2, 1, 4096), arena);
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertThrows(IOException.class, () -> SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(
                    config, identity, replayArena, replayBookie, replayBookie.admitRecovery(same).orElseThrow(),
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false), 1, 1));
            SegmentFileIo sync = new SegmentFileIo();
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(same).orElseThrow(), sync, 1, 1));
            assertEquals(1, sync.forces);
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(changedLac).orElseThrow(),
                            new SegmentFileIo(), 1, 1));
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.CONFLICTING_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(conflict).orElseThrow(),
                            new SegmentFileIo(), 1, 1));
            replayArena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC, new UUID(0, 6), new Generation(1), 0,
                    0, 0, new Generation(2), 6, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.NOT_READY,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(same).orElseThrow(),
                            new SegmentFileIo(), 1, 1));
        }
    }

    @Test
    public void recoveredNormalDuplicateRemainsPhysicalEvidenceAfterLocalResult() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> results = new AtomicReference<>();
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            try {
                assertTrue(driver.submitNormal(bookie, batch(config, 1, 1, 4096), List.of(
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()), (entries, failure) -> {
                            results.set(entries);
                            dataFailure.set(failure);
                            completed.countDown();
                        }));
                owner.start();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        results.get().get(0).status());
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            ByteBuffer same = entryBytes(context(), 1, -1, 8, (byte) 0);
            ByteBuffer conflict = entryBytes(context(), 1, -1, 8, (byte) 9);
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredNormalCandidate(config, identity, arena, bookie,
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), same,
                            new SegmentFileIo(), 1, 1));
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.CONFLICTING_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredNormalCandidate(config, identity, arena, bookie,
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), conflict,
                            new SegmentFileIo(), 1, 1));
            assertEquals(1, SegmentDataRecovery.withLocallyReadableSingleStreamLocation(
                    config, identity, arena, bookie, new SegmentFileIo(), context(), 1, 1, 1, selected -> {
                        assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                                config, identity, arena, new SegmentFileIo()));
                        return SegmentPointRead.read(config, identity, arena, selected.location()).record().entryId();
                    }).orElseThrow().longValue());
            assertThrows(IOException.class, () -> SegmentDataRecovery.withLocallyReadableSingleStreamLocation(
                    config, identity, arena, bookie, new SegmentFileIo(), context(), 1, 1, 1, selected -> {
                        throw new IOException("Point read failed after selection");
                    }));
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity, arena, new SegmentFileIo(), 1, 1)) {
                assertTrue(reopened.matchesScope(config, identity));
                assertEquals(1, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1).record().entryId());
                try (IsolatedSegmentFiles.StartupInventoryLease lease = reopened.pinStartupInventory()) {
                    assertThrows(IOException.class, reopened::close);
                }
            }
            assertThrows(IOException.class, () -> SegmentPointRead.readRecoveredSingleStreamLocallyGated(
                    config, identity, arena, bookie, new SegmentFileIo(), context(), 2, 1, 1));
            assertThrows(IOException.class, () -> SegmentDuplicateCheck.inspectRecoveredNormalCandidate(
                    config, identity, arena, bookie,
                    bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), same,
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false), 1, 1));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    bookie.submitFence(fence(3, (byte) 5)).status());
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity, arena, new SegmentFileIo(), 1, 1)) {
                assertEquals(1, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1).record().entryId());
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
                assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1));
            }
        }
    }

    @Test
    public void recoveredNormalCandidateLosesEligibilityWhenTombstonedDuringSync() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            files.writeAuthorized(batch(config, 1, 1, 4096), arena);
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo sync = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Normal recovery sync was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            ByteBuffer same = entryBytes(context(), 1, -1, 8, (byte) 0);
            FutureTask<SegmentDuplicateCheck.PhysicalComparison> duplicate = new FutureTask<>(
                    () -> SegmentDuplicateCheck.inspectRecoveredNormalCandidate(config, identity, arena, bookie,
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(), same, sync, 1, 1));
            Thread reader = new Thread(duplicate);
            reader.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
                release.countDown();
                assertEquals(SegmentDuplicateCheck.PhysicalComparison.STALE_ADMISSION,
                        duplicate.get(5, TimeUnit.SECONDS));
            } finally {
                release.countDown();
                reader.join(5000);
            }
        }
    }

    @Test
    public void recoveredReadLosesEligibilityWhenTombstonedDuringSync() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            files.writeAuthorized(batch(config, 1, 1, 4096), arena);
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo sync = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Recovered read sync was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            FutureTask<SegmentPointRead.Result> read = new FutureTask<>(
                    () -> SegmentPointRead.readRecoveredSingleStreamLocallyGated(
                            config, identity, arena, bookie, sync, context(), 1, 1, 1));
            Thread reader = new Thread(read);
            reader.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
                release.countDown();
                assertTrue(assertThrows(ExecutionException.class,
                        () -> read.get(5, TimeUnit.SECONDS)).getCause() instanceof IOException);
            } finally {
                release.countDown();
                reader.join(5000);
            }
        }
    }

    @Test
    public void recoveredBytesAfterOldForceErrorRemainOnlyPhysicalEvidence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ProfileOperationCodec.AddRecovery same = recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> files.writeAuthorized(batch(config, 2, 1, 4096), arena));
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(same).orElseThrow(),
                            new SegmentFileIo(), 1, 1));
        }
    }

    @Test
    public void recoveredRecoveryCandidateLosesEligibilityWhenGrantClosesDuringSync() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ProfileOperationCodec.AddRecovery same = recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            files.writeAuthorized(batch(config, 2, 1, 4096), arena);
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo sync = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Recovery sync was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            FutureTask<SegmentDuplicateCheck.PhysicalComparison> duplicate = new FutureTask<>(
                    () -> SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(same).orElseThrow(), sync, 1, 1));
            Thread reader = new Thread(duplicate);
            reader.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT,
                        replayBookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
                release.countDown();
                assertEquals(SegmentDuplicateCheck.PhysicalComparison.STALE_ADMISSION,
                        duplicate.get(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                        replayBookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            } finally {
                release.countDown();
                reader.join(5000);
            }
        }
    }

    @Test
    public void recoveredRecoveryCandidateRejectsAmbiguousPhysicalCoordinate() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ProfileOperationCodec.AddRecovery same = recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            files.writeAuthorized(batch(config, 2, 1, 4096), arena);
            files.writeAuthorized(batch(config, 2, 2, 8192), arena);
        }
        try (SegmentArenaControlLog replayArena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog replayBookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.NOT_READY,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, replayArena,
                            replayBookie, replayBookie.admitRecovery(same).orElseThrow(),
                            new SegmentFileIo(), 2, 2));
        }
    }

    @Test
    public void sameDurableBatchMakesIndependentNormalDecisionsAfterActivationChange() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> results = new AtomicReference<>();
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
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
                assertTrue(driver.submitNormal(bookie, twoLedgerBatch(config), List.of(
                        bookie.admitNormal(context(44), filled(20, (byte) 5)).orElseThrow(),
                        bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()),
                        (entries, failure) -> {
                            owner.requireOwner();
                            results.set(entries);
                            dataFailure.set(failure);
                            completed.countDown();
                        }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(activateReplacement(44, 5))).get(0).status());
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(2, results.get().size());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.STALE_ADMISSION, results.get().get(0).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get().get(1).status());
                assertEquals(1, SegmentPointRead.read(config, identity, arena, results.get().get(0).candidate())
                        .record().entryId());
                assertEquals(2, SegmentPointRead.read(config, identity, arena, results.get().get(1).candidate())
                        .record().entryId());
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
    public void tombstoneMakesOnlyItsInFlightEntryStaleAfterSharedDataForce() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> results = new AtomicReference<>();
        AtomicReference<Throwable> dataFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
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
                assertTrue(driver.submitNormal(bookie, twoLedgerBatch(config), List.of(
                        bookie.admitNormal(context(44), filled(20, (byte) 5)).orElseThrow(),
                        bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()),
                        (entries, failure) -> {
                            owner.requireOwner();
                            results.set(entries);
                            dataFailure.set(failure);
                            completed.countDown();
                        }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
                assertTrue(bookie.admitNormal(context(44), filled(20, (byte) 5)).isEmpty());
                assertTrue(bookie.normalAdmission(context(48), filled(20, (byte) 5)));
                releaseForce.countDown();
                assertTrue(completed.await(5, TimeUnit.SECONDS));
                assertNull(dataFailure.get());
                assertEquals(2, results.get().size());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.STALE_ADMISSION, results.get().get(0).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get().get(1).status());
                assertEquals(1, SegmentPointRead.read(config, identity, arena, results.get().get(0).candidate())
                        .record().entryId());
                assertEquals(2, SegmentPointRead.read(config, identity, arena, results.get().get(1).candidate())
                        .record().entryId());
            } finally {
                releaseForce.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(44), filled(20, (byte) 5)));
            assertTrue(replay.normalAdmission(context(48), filled(20, (byte) 5)));
        }
    }

    @Test
    public void stoppedNormalWaiterDoesNotCancelSharedDataOrReleaseAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch secondCompleted = new CountDownLatch(1);
        AtomicInteger firstCallbacks = new AtomicInteger();
        AtomicReference<SegmentDataPhysicalDriver.NormalResult> secondResult = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
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
            SegmentDataPhysicalDriver.NormalRequest first = new SegmentDataPhysicalDriver.NormalRequest(
                    (result, failure) -> firstCallbacks.incrementAndGet());
            SegmentDataPhysicalDriver.NormalRequest second = new SegmentDataPhysicalDriver.NormalRequest(
                    (result, failure) -> {
                        owner.requireOwner();
                        secondResult.set(result);
                        secondFailure.set(failure);
                        secondCompleted.countDown();
                    });
            SegmentBookieControlLog.Admission firstAdmission = bookie.admitNormal(
                    context(44), filled(20, (byte) 5)).orElseThrow();
            SegmentBookieControlLog.Admission secondAdmission = bookie.admitNormal(
                    context(48), filled(20, (byte) 5)).orElseThrow();
            try {
                assertTrue(driver.submitNormal(bookie, twoLedgerBatch(config), List.of(
                        firstAdmission, secondAdmission),
                        List.of(first, second)));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(first.stopWaiting());
                assertFalse(first.stopWaiting());
                assertTrue(bookie.canCompleteNormal(firstAdmission));
                assertThrows(IOException.class, bookie::close);
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.CONFLICT,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 8, (byte) 1)));
                releaseForce.countDown();
                assertTrue(secondCompleted.await(5, TimeUnit.SECONDS));
                assertNull(secondFailure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, secondResult.get().status());
                assertEquals(0, firstCallbacks.get());
                assertFalse(second.stopWaiting());
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 8, (byte) 0)));
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
    public void pendingNormalDuplicateJoinsForceAndPublishedDuplicateUsesPointRead() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch duplicateReading = new CountDownLatch(1);
        CountDownLatch releaseDuplicate = new CountDownLatch(1);
        AtomicBoolean holdNextRead = new AtomicBoolean();
        AtomicReference<SegmentDataPhysicalDriver.NormalStatus> writeStatus = new AtomicReference<>();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        AtomicInteger stoppedJoinCallbacks = new AtomicInteger();
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
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 4, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2,
                    task -> new Thread(task, "isolated-duplicate-io"), () -> {
                        if (holdNextRead.compareAndSet(true, false)) {
                            duplicateReading.countDown();
                            try {
                                if (!releaseDuplicate.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("Duplicate point-read was not released");
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
                assertTrue(driver.submitNormal(bookie, batch(config, 1, 1, 4096), List.of(
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> {
                            writeStatus.set(results.get(0).status());
                            writeFailure.set(failure);
                            written.countDown();
                        }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                CountDownLatch joined = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> joinedStatus = new AtomicReference<>();
                AtomicReference<Throwable> joinedFailure = new AtomicReference<>();
                assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        entryBytes(context(), 1, 0, 8, (byte) 0), (result, failure) -> {
                            joinedStatus.set(result);
                            joinedFailure.set(failure);
                            joined.countDown();
                        }));
                SegmentBookieControlLog.Admission stoppedJoinAdmission = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                SegmentDataPhysicalDriver.DuplicateRequest stoppedJoin =
                        new SegmentDataPhysicalDriver.DuplicateRequest(
                                (result, failure) -> stoppedJoinCallbacks.incrementAndGet());
                assertTrue(driver.checkPublishedNormalDuplicate(bookie, stoppedJoinAdmission,
                        entryBytes(context(), 1, 0, 8, (byte) 0), stoppedJoin));
                assertTrue(stoppedJoin.stopWaiting());
                assertTrue(bookie.canCompleteNormal(stoppedJoinAdmission));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.CONFLICT,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 8, (byte) 1)));
                assertEquals(1L, joined.getCount());
                releaseForce.countDown();
                assertTrue(written.await(5, TimeUnit.SECONDS));
                assertTrue(joined.await(5, TimeUnit.SECONDS));
                assertNull(joinedFailure.get());
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE, joinedStatus.get());
                assertNull(writeFailure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, writeStatus.get());
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 8, (byte) 0)));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.CONFLICT,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 8, (byte) 1)));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.CONFLICT,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 1, 0, 9, (byte) 0)));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.NOT_READY,
                        checkPublishedDuplicate(driver, bookie, entryBytes(context(), 2, 0, 8, (byte) 0)));
                CountDownLatch checked = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> checkedStatus = new AtomicReference<>();
                AtomicReference<Throwable> checkedFailure = new AtomicReference<>();
                holdNextRead.set(true);
                assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        entryBytes(context(), 1, 0, 8, (byte) 0), (result, failure) -> {
                            checkedStatus.set(result);
                            checkedFailure.set(failure);
                            checked.countDown();
                        }));
                assertTrue(duplicateReading.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(activateReplacement(44, 4))).get(0).status());
                releaseDuplicate.countDown();
                assertTrue(checked.await(5, TimeUnit.SECONDS));
                assertNull(checkedFailure.get());
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.STALE_ADMISSION, checkedStatus.get());
                ByteBuffer corrupt = entryBytes(context(), 1, 0, 8, (byte) 0);
                corrupt.put(32, (byte) (corrupt.get(32) ^ 1));
                SegmentBookieControlLog.Admission invalid = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                assertThrows(IOException.class, () -> driver.checkPublishedNormalDuplicate(
                        bookie, invalid, corrupt, (result, failure) -> { }));
                owner.stopAdmission();
                assertFalse(driver.checkPublishedNormalDuplicate(bookie,
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        entryBytes(context(), 1, 0, 8, (byte) 0), (result, failure) -> { }));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                assertEquals(0, stoppedJoinCallbacks.get());
            } finally {
                releaseForce.countDown();
                releaseDuplicate.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void stoppedPublishedDuplicateWaiterKeepsPointReadAndFenceTicketUntilTerminal() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicBoolean holdRead = new AtomicBoolean();
        AtomicInteger stoppedCallbacks = new AtomicInteger();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        AtomicReference<SegmentDataPhysicalDriver.NormalStatus> writeStatus = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 3, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2,
                    task -> new Thread(task, "isolated-duplicate-cancel-io"), () -> {
                        if (holdRead.compareAndSet(true, false)) {
                            reading.countDown();
                            try {
                                if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("Duplicate point-read was not released");
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
                assertTrue(driver.submitNormal(bookie, batch(config, 1, 1, 4096), List.of(
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> {
                            writeFailure.set(failure);
                            writeStatus.set(results.get(0).status());
                            written.countDown();
                        }));
                owner.start();
                assertTrue(written.await(5, TimeUnit.SECONDS));
                assertNull(writeFailure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, writeStatus.get());
                holdRead.set(true);
                SegmentBookieControlLog.Admission duplicateAdmission = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                SegmentDataPhysicalDriver.DuplicateRequest request =
                        new SegmentDataPhysicalDriver.DuplicateRequest(
                                (status, failure) -> stoppedCallbacks.incrementAndGet());
                assertTrue(driver.checkPublishedNormalDuplicate(bookie, duplicateAdmission,
                        entryBytes(context(), 1, 0, 8, (byte) 0), request));
                assertTrue(reading.await(5, TimeUnit.SECONDS));
                assertTrue(request.stopWaiting());
                assertTrue(bookie.canCompleteNormal(duplicateAdmission));
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                releaseRead.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
                assertEquals(0, stoppedCallbacks.get());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                releaseRead.countDown();
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
    public void normalBatchRejectsLedgerOnAnotherLogicalShardBeforePhysicalReservation() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        ByteBuf entry = entry(context(45), 1);
        SegmentBatchFormat.FrozenBatch wrongShard;
        try {
            wrongShard = SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                    List.of(SegmentBatchFormat.entry(context(45), entry)));
        } finally {
            entry.release();
        }
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(45, 1, (byte) 5), activate(45, 2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            try {
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                        new SegmentHotDirectory(owner, config, identity, 2));
                assertThrows(IOException.class, () -> driver.submitNormal(bookie, wrongShard,
                        List.of(bookie.admitNormal(context(45), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> { }));
                assertEquals(4096, files.nextOffset(0));
                assertEquals(0, owner.pending());
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
        CountDownLatch firstForce = new CountDownLatch(1);
        CountDownLatch secondOperation = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch callbacks = new CountDownLatch(2);
        CountDownLatch duplicateCompleted = new CountDownLatch(1);
        AtomicInteger operations = new AtomicInteger();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> duplicateStatus = new AtomicReference<>();
        AtomicReference<Throwable> duplicateFailure = new AtomicReference<>();
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> firstResults = new AtomicReference<>();
        AtomicReference<List<SegmentDataPhysicalDriver.NormalResult>> secondResults = new AtomicReference<>();
        try (SegmentBufferPool pool = new SegmentBufferPool(0, 1, 2, 4096, 4096, false, () -> { });
                IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentBatchFormat.FrozenBatch first = pooledBatch(config, 1, 1, 4096, pool);
            SegmentBatchFormat.FrozenBatch second = pooledBatch(config, 2, 2, 8192, pool);
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
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 3, 1);
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
                        context(), filled(20, (byte) 5)).orElseThrow()), (entries, failure) -> {
                            firstResults.set(entries);
                            firstFailure.set(failure);
                            callbacks.countDown();
                        }));
                owner.start();
                assertTrue(firstForce.await(5, TimeUnit.SECONDS));
                assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        entryBytes(context(), 1, 0, 8, (byte) 0), (status, failure) -> {
                            duplicateStatus.set(status);
                            duplicateFailure.set(failure);
                            duplicateCompleted.countDown();
                        }));
                assertTrue(driver.submitNormal(bookie, second, List.of(bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow()), (entries, failure) -> {
                            secondResults.set(entries);
                            secondFailure.set(failure);
                            callbacks.countDown();
                        }));
                assertTrue(secondOperation.await(5, TimeUnit.SECONDS));
                assertTrue(callbacks.await(5, TimeUnit.SECONDS));
                assertTrue(duplicateCompleted.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.UNKNOWN, duplicateStatus.get());
                assertTrue(duplicateFailure.get() instanceof IOException);
                assertTrue(firstFailure.get() instanceof IOException);
                assertTrue(secondFailure.get() instanceof IOException);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, firstResults.get().get(0).status());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, secondResults.get().get(0).status());
                CountDownLatch ownerFinished = new CountDownLatch(1);
                assertTrue(owner.control(ownerFinished::countDown));
                assertTrue(ownerFinished.await(5, TimeUnit.SECONDS));
                try (SegmentBufferPool.Lease reusable = pool.acquire()) {
                    assertTrue(reusable != null);
                    assertNull(pool.acquire());
                }
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                assertThrows(IOException.class, bookie::close);
                releaseSecond.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                try (SegmentBufferPool.Lease reusableFirst = pool.acquire();
                        SegmentBufferPool.Lease reusableSecond = pool.acquire()) {
                    assertTrue(reusableFirst != null);
                    assertTrue(reusableSecond != null);
                }
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

    @Test
    public void bookieControlDriverKeepsOwnerResponsiveAcrossFenceWaitAndForce() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forceEntered = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        AtomicInteger forces = new AtomicInteger();
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (forces.incrementAndGet() == 2) {
                forceEntered.countDown();
                try {
                    if (!releaseForce.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Bookie fence force was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
        });
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR, controlIo)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch batchDone = new CountDownLatch(1);
            CountDownLatch waitingDone = new CountDownLatch(1);
            CountDownLatch fenceDone = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<SegmentBookieControlLog.FenceResult> waiting = new AtomicReference<>();
            AtomicReference<SegmentBookieControlLog.FenceResult> durable = new AtomicReference<>();
            try {
                assertTrue(driver.submitBatch(List.of(install(1, (byte) 5), activate(2)), (results, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, results.get(0).status());
                    assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, results.get(1).status());
                    batchDone.countDown();
                }));
                owner.start();
                assertTrue(batchDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertTrue(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                SegmentBookieControlLog.Admission prior = bookie.admitNormal(
                        context(), filled(20, (byte) 5)).orElseThrow();
                assertTrue(driver.submitFence(fence(3, (byte) 5), (result, cause) -> {
                    owner.requireOwner();
                    waiting.set(result);
                    failure.set(cause);
                    waitingDone.countDown();
                }));
                assertTrue(waitingDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT, waiting.get().status());
                CountDownLatch progressed = new CountDownLatch(1);
                assertTrue(owner.control(progressed::countDown));
                assertTrue(progressed.await(5, TimeUnit.SECONDS));
                prior.physicalTerminal();
                assertTrue(driver.submitFence(fence(3, (byte) 5), (result, cause) -> {
                    owner.requireOwner();
                    durable.set(result);
                    failure.set(cause);
                    fenceDone.countDown();
                }));
                assertTrue(forceEntered.await(5, TimeUnit.SECONDS));
                assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                CountDownLatch duringForce = new CountDownLatch(1);
                assertTrue(owner.control(duringForce::countDown));
                assertTrue(duringForce.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, bookie::close);
                releaseForce.countDown();
                assertTrue(fenceDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE, durable.get().status());
            } finally {
                releaseForce.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
            assertEquals(2, forces.get());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertFalse(replay.normalAdmission(context(), filled(20, (byte) 5)));
        }
    }

    @Test
    public void recoveryGrantUsesReservedControlIoBeforeOpeningLocalAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 6)));
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Recovery grant force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR, controlIo)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<SegmentBookieControlLog.Status> status = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9), (results, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    status.set(results == null ? null : results.get(0).status());
                    done.countDown();
                }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertFalse(driver.submitRecoveryGrant(recoveryGrant(3, 4, 1, 10), (results, cause) -> { }));
                CountDownLatch progressed = new CountDownLatch(1);
                assertTrue(owner.control(progressed::countDown));
                assertTrue(progressed.await(5, TimeUnit.SECONDS));
                assertTrue(bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isEmpty());
                assertThrows(IOException.class, bookie::close);
                release.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, status.get());
                SegmentBookieControlLog.RecoveryAdmission admitted =
                        bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).orElseThrow();
                admitted.finish();
            } finally {
                release.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR)) {
            SegmentBookieControlLog.RecoveryAdmission admitted =
                    replay.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).orElseThrow();
            admitted.finish();
        }
    }

    @Test
    public void recoveryCloseUsesReservedControlIoAfterPrecutTicketTerminates() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 6)));
            setup.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Recovery close force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR, controlIo)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            SegmentBookieControlLog.RecoveryAdmission admitted =
                    bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).orElseThrow();
            CountDownLatch waitingDone = new CountDownLatch(1);
            CountDownLatch closeDone = new CountDownLatch(1);
            AtomicReference<SegmentBookieControlLog.RecoveryCloseStatus> waiting = new AtomicReference<>();
            AtomicReference<SegmentBookieControlLog.RecoveryCloseStatus> durable = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9), (status, cause) -> {
                    owner.requireOwner();
                    waiting.set(status);
                    failure.set(cause);
                    waitingDone.countDown();
                }));
                owner.start();
                assertTrue(waitingDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT, waiting.get());
                assertTrue(bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isEmpty());
                admitted.finish();
                assertTrue(driver.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9), (status, cause) -> {
                    owner.requireOwner();
                    durable.set(status);
                    failure.set(cause);
                    closeDone.countDown();
                }));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                CountDownLatch progressed = new CountDownLatch(1);
                assertTrue(owner.control(progressed::countDown));
                assertTrue(progressed.await(5, TimeUnit.SECONDS));
                assertFalse(bookie.locallyCommittedReadable(context(), 2));
                assertThrows(IOException.class, bookie::close);
                release.countDown();
                assertTrue(closeDone.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE, durable.get());
                assertTrue(bookie.locallyCommittedReadable(context(), 2));
            } finally {
                release.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR)) {
            assertTrue(replay.locallyCommittedReadable(context(), 2));
            assertTrue(replay.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isEmpty());
        }
    }

    @Test
    public void rejectedRecoveryCloseIoKeepsCutAndRetriesWithoutSkippingSequence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            io.shutdown();
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch rejected = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9), (status, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    rejected.countDown();
                }));
                owner.start();
                assertTrue(rejected.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof RejectedExecutionException);
                assertTrue(bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isEmpty());
                assertFalse(bookie.locallyCommittedReadable(context(), 2));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                        bookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
                assertTrue(bookie.locallyCommittedReadable(context(), 2));
            } finally {
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void failedRecoveryCloseForceReportsFailureWithoutReopeningAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 6)));
            setup.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
        }
        SegmentFileIo failingIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
        try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 8), VALIDATOR, failingIo)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9), (status, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    done.countDown();
                }));
                owner.start();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof IOException);
                assertTrue(bookie.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 6)).isEmpty());
                assertFalse(bookie.locallyCommittedReadable(context(), 2));
                assertThrows(IOException.class,
                        () -> bookie.submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9)));
            } finally {
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void rejectedControlIoLeavesFenceCutRetryableWithoutSkippingSequence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            io.shutdown();
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch rejected = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitFence(fence(3, (byte) 5), (result, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    rejected.countDown();
                }));
                owner.start();
                assertTrue(rejected.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof RejectedExecutionException);
                assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
            } finally {
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void explicitLacUsesReservedControlIoAndAppliesOnTheOwner() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog setup = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            setup.submitBatch(List.of(install(1, (byte) 5), activate(2)));
        }
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Explicit LAC force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR, controlIo)) {
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 2);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<SegmentBookieControlLog.Status> status = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1), (result, cause) -> {
                    owner.requireOwner();
                    status.set(result);
                    failure.set(cause);
                    done.countDown();
                }));
                owner.start();
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertFalse(driver.submitExplicitLac(writeLac(10, (byte) 5, (byte) 2), (result, cause) -> { }));
                CountDownLatch progressed = new CountDownLatch(1);
                assertTrue(owner.control(progressed::countDown));
                assertTrue(progressed.await(5, TimeUnit.SECONDS));
                assertTrue(bookie.explicitLac(context()).isEmpty());
                assertThrows(IOException.class, bookie::close);
                release.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertNull(failure.get());
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, status.get());
                assertEquals(9, bookie.explicitLac(context()).orElseThrow().lac());
            } finally {
                release.countDown();
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
        try (SegmentBookieControlLog replay = SegmentBookieControlLog.open(config, identity,
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(9, replay.explicitLac(context()).orElseThrow().lac());
        }
    }

    @Test
    public void rejectedControlIoLeavesExplicitLacRetryableWithoutSkippingSequence() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 1, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 1, 1, 1);
            io.shutdown();
            SegmentBookieControlDriver driver = new SegmentBookieControlDriver(owner, io, bookie);
            CountDownLatch rejected = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                assertTrue(driver.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1), (result, cause) -> {
                    owner.requireOwner();
                    failure.set(cause);
                    rejected.countDown();
                }));
                owner.start();
                assertTrue(rejected.await(5, TimeUnit.SECONDS));
                assertTrue(failure.get() instanceof RejectedExecutionException);
                assertTrue(bookie.explicitLac(context()).isEmpty());
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1)));
            } finally {
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                owner.stopAdmission();
                assertTrue(owner.awaitTermination(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void abruptExitAfterRecoveryDataForceReopensAsPhysicalCandidate() throws Exception {
        assertAbruptRecoveryCandidate("after-force", 0, "LOCAL_DURABLE");
    }

    @Test
    public void abruptExitBeforeRecoveryDataBarrierNeedsRestartSync() throws Exception {
        assertAbruptRecoveryCandidate("before-force", 86, "BEFORE_DATA_FORCE");
    }

    @Test
    public void abruptExitAfterNormalDataForceReopensAsPhysicalCandidate() throws Exception {
        assertAbruptNormalCandidate("normal-after-force", 0, "LOCAL_DURABLE");
    }

    @Test
    public void abruptExitBeforeNormalDataBarrierNeedsRestartSync() throws Exception {
        assertAbruptNormalCandidate("normal-before-force", 86, "BEFORE_DATA_FORCE");
    }

    @Test
    public void abruptRestartAppendsBeyondOldGrantWithoutPromotingOldDuplicate() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        runAbruptWriter("after-force", config.directory(), 0, "LOCAL_DURABLE");
        runAbruptWriter("append-after-restart", config.directory(), 0, "APPENDED_PHYSICALLY");
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            SegmentDataRecovery.PhysicalStreamCandidate oldStream =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity, arena,
                            new SegmentFileIo(), 0, new Generation(1), 1, 1, 1);
            SegmentDataRecovery.PhysicalStreamCandidate newStream =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity, arena,
                            new SegmentFileIo(), 0, new Generation(2), 1, 1, 1);
            assertEquals(2, SegmentPointRead.read(config, identity, arena,
                    oldStream.entries().get(0)).record().entryId());
            assertEquals(3, SegmentPointRead.read(config, identity, arena,
                    newStream.entries().get(0)).record().entryId());
            ProfileOperationCodec.AddRecovery same =
                    recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.NOT_READY,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, arena, bookie,
                            bookie.admitRecovery(same).orElseThrow(), new SegmentFileIo(), 2, 2));
            assertThrows(IOException.class, () -> SegmentPointRead.readRecoveredSingleStreamLocallyGated(
                    config, identity, arena, bookie, new SegmentFileIo(), context(), 2, 2, 2));
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                    config, identity, arena, bookie, new SegmentFileIo(), 2, 2)) {
                assertEquals(2, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 2).record().entryId());
                assertEquals(3, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 3).record().entryId());
            }
        }
    }

    @Test
    public void restartReadInventorySelectsRoutesAcrossStreamsAndInvalidatesAtNewGrant() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                    SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 6), new Generation(1), 0,
                    0, 0, new Generation(2), 6, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            files.writeAuthorized(twoLedgerBatch(config), arena);
            files.writeAuthorized(secondStreamBatch(config, 3, 1), arena);
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    bookie.submitBatch(List.of(tombstone(48, 20))).get(0).status());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity, arena, new SegmentFileIo(), 2, 2));
            SegmentFileIo sync = new SegmentFileIo();
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                    config, identity, arena, bookie, sync, 2, 2)) {
                assertEquals(3, reopened.startupInventory().scan().records());
                assertEquals(2, reopened.startupInventory().entries().size());
                assertEquals(1, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1).record().entryId());
                assertEquals(3, SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 3).record().entryId());
                assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(48), 2));
                assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 2));
                assertFalse(reopened.supportsLocalResults());
                assertEquals(1, sync.forces);
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 7), new Generation(1), 0,
                        0, 0, new Generation(3), 7, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
                assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1));
            }
        }
    }

    @Test
    public void restartReadInventoryRejectsAmbiguousOrNoncontiguousRequiredData() throws Exception {
        for (boolean duplicate : List.of(false, true)) {
            IsolatedSegmentConfig base = config();
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    base.directory().resolveSibling(duplicate ? "duplicate" : "gap"), base.arenaCount(),
                    base.logicalShards(), base.dataFiles(), base.bytesPerFile(), base.alignment(),
                    base.maxPayloadBytes(), base.maxPendingEntries(), "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                grant(arena, 1);
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 6), new Generation(1), 0,
                        0, 0, new Generation(2), 6, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)));
                bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                files.writeAuthorized(batch(config, 1, 1, 4096), arena);
                files.writeAuthorized(secondStreamBatch(config, duplicate ? 1 : 2, duplicate ? 1 : 2), arena);
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                        config, identity, arena, bookie, new SegmentFileIo(), 2, 2));
                try (IsolatedSegmentFiles raw = IsolatedSegmentFiles.openAppendOnlyWithInventory(
                        config, identity, arena, new SegmentFileIo(), 2, 2)) {
                    assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                            raw, arena, bookie, context(), duplicate ? 1 : 2));
                }
            }
        }
    }

    @Test
    public void restartReadInventoryRejectsChangingBookieCutAndSyncFailure() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            files.writeAuthorized(batch(config, 1, 1, 4096), arena);
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentBookieControlLog.Admission held = bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow();
            SegmentFileIo blocked = new SegmentFileIo();
            try {
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                        config, identity, arena, bookie, blocked, 1, 1));
                assertEquals(0, blocked.forces);
            } finally {
                held.physicalTerminal();
            }
            SegmentFileIo failed = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                    config, identity, arena, bookie, failed, 1, 1));
            assertEquals(1, failed.forces);
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            SegmentFileIo revoked = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            FutureTask<Void> opening = new FutureTask<>(() -> {
                try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                        config, identity, arena, bookie, revoked, 1, 1)) {
                    return null;
                }
            });
            Thread reader = new Thread(opening);
            reader.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                        bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
                release.countDown();
                assertTrue(assertThrows(ExecutionException.class,
                        () -> opening.get(5, TimeUnit.SECONDS)).getCause() instanceof IOException);
            } finally {
                release.countDown();
                reader.join(5000);
            }
            assertFalse(reader.isAlive());
            assertEquals(1, revoked.forces);
            try (IsolatedSegmentFiles reopened = IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                    config, identity, arena, bookie, new SegmentFileIo(), 1, 1)) {
                assertEquals(0, reopened.startupInventory().entries().size());
                assertThrows(IOException.class, () -> SegmentPointRead.readRestartedLocallyGated(
                        reopened, arena, bookie, context(), 1));
            }
        }
    }

    private void assertAbruptRecoveryCandidate(String phase, int expectedExit, String marker) throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        runAbruptWriter(phase, config.directory(), expectedExit, marker);
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            SegmentFileIo sync = new SegmentFileIo();
            SegmentDataRecovery.PhysicalStreamCandidate candidate =
                    SegmentDataRecovery.recoverPhysicalStreamCandidate(config, identity, arena,
                            sync, 0, new Generation(1), 1, 1, 1);
            assertEquals(1, candidate.through());
            assertEquals(1, candidate.entries().size());
            assertEquals(1, sync.forces);
            ProfileOperationCodec.AddRecovery same =
                    recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredRecoveryCandidate(config, identity, arena, bookie,
                            bookie.admitRecovery(same).orElseThrow(), new SegmentFileIo(), 1, 1));
        }
    }

    private void assertAbruptNormalCandidate(String phase, int expectedExit, String marker) throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        runAbruptWriter(phase, config.directory(), expectedExit, marker);
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentFileIo sync = new SegmentFileIo();
            SegmentPointRead.Result recovered = SegmentPointRead.readRecoveredSingleStreamLocallyGated(
                    config, identity, arena, bookie, sync, context(), 1, 1, 1);
            assertEquals(1, sync.forces);
            assertEquals(1, recovered.record().entryId());
            assertEquals(SegmentDuplicateCheck.PhysicalComparison.MATCHED_CANDIDATE,
                    SegmentDuplicateCheck.inspectRecoveredNormalCandidate(config, identity, arena, bookie,
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            entryBytes(context(), 1, -1, 8, (byte) 0), new SegmentFileIo(), 1, 1));
        }
    }

    private void runAbruptWriter(String phase, Path directory, int expectedExit, String marker) throws Exception {
        Path output = temporary.getRoot().toPath().resolve("abrupt-" + phase + ".log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), SegmentBookieControlLogTest.class.getName(),
                phase, directory.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        boolean finished = child.waitFor(15, TimeUnit.SECONDS);
        if (!finished) {
            child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
        String log = Files.readString(output);
        assertTrue(log, finished);
        assertEquals(log, expectedExit, child.exitValue());
        assertTrue(log, log.contains(marker));
    }

    /** Child process for the real process-boundary tests; never returns after the chosen cut. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2 || (!"after-force".equals(args[0]) && !"before-force".equals(args[0])
                && !"normal-after-force".equals(args[0]) && !"normal-before-force".equals(args[0])
                && !"append-after-restart".equals(args[0]))) {
            throw new IllegalArgumentException("Expected abrupt writer phase and directory");
        }
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(Path.of(args[1]), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 7)),
                new UUID(0, 9), BookieId.parse("isolated:3181"));
        if ("append-after-restart".equals(args[0])) {
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openAppendOnly(
                            config, identity, arena, new SegmentFileIo())) {
                long offset = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC, new UUID(0, 6), new Generation(1), 0,
                        0, 0, new Generation(2), 6, offset, 1, 1)));
                ByteBuf entry = entry(context(), 3);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                            0, 0, 6, new Generation(1), new Generation(2), 1, offset),
                            List.of(SegmentBatchFormat.entry(context(), entry))), arena);
                } finally {
                    entry.release();
                }
                System.out.println("APPENDED_PHYSICALLY");
                System.out.flush();
                Runtime.getRuntime().halt(0);
            }
        }
        if (args[0].startsWith("normal-")) {
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                grant(arena, 1);
                bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                if ("normal-before-force".equals(args[0])) {
                    files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        System.out.println("BEFORE_DATA_FORCE");
                        System.out.flush();
                        Runtime.getRuntime().halt(86);
                    });
                }
                SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
                SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                        new SegmentHotDirectory(owner, config, identity, 2));
                CountDownLatch completed = new CountDownLatch(1);
                AtomicReference<SegmentDataPhysicalDriver.NormalStatus> status = new AtomicReference<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                if (!driver.submitNormal(bookie, batch(config, 1, 1, 4096),
                        List.of(bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (entries, cause) -> {
                            status.set(entries.get(0).status());
                            failure.set(cause);
                            completed.countDown();
                        })) {
                    throw new IllegalStateException("Normal DATA was not accepted");
                }
                owner.start();
                if (!completed.await(10, TimeUnit.SECONDS) || failure.get() != null
                        || status.get() != SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE) {
                    throw new IllegalStateException("Normal DATA did not reach local durable result", failure.get());
                }
                System.out.println("LOCAL_DURABLE");
                System.out.flush();
                Runtime.getRuntime().halt(0);
            }
        }
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 8), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            if ("before-force".equals(args[0])) {
                files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    System.out.println("BEFORE_DATA_FORCE");
                    System.out.flush();
                    Runtime.getRuntime().halt(86);
                });
            }
            SegmentOwnerLoop owner = new SegmentOwnerLoop(4, 1, 0, 2, 1);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 2),
                    new SegmentHotDirectory(owner, config, identity, 2));
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<SegmentDataPhysicalDriver.RecoveryStatus> status = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            ProfileOperationCodec.AddRecovery request =
                    recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0));
            if (!driver.submitRecovery(bookie, batch(config, 2, 1, 4096),
                    List.of(bookie.admitRecovery(request).orElseThrow()), (entries, cause) -> {
                        status.set(entries.get(0).status());
                        failure.set(cause);
                        completed.countDown();
                    })) {
                throw new IllegalStateException("Recovery DATA was not accepted");
            }
            owner.start();
            if (!completed.await(10, TimeUnit.SECONDS) || failure.get() != null
                    || status.get() != SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE) {
                throw new IllegalStateException("Recovery DATA did not reach local durable result", failure.get());
            }
            System.out.println("LOCAL_DURABLE");
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }
    }
}

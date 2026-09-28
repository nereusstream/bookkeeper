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

    private static ByteBuf entry(ProfileLedgerContext context, long entryId) throws Exception {
        return entry(context, entryId, -1, 8, (byte) 0);
    }

    private static ByteBuf entry(ProfileLedgerContext context, long entryId, long lac,
                                 long ledgerLength, byte payload) throws Exception {
        DigestManager digest = DigestManager.instantiate(context.ledgerId(), new byte[0], DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        ByteBufList encoded = (ByteBufList) digest.computeDigestAndPackageForSending(entryId, lac, ledgerLength,
                Unpooled.wrappedBuffer(filled(8, payload)), new byte[0], 0);
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
        SegmentBatchFormat.FrozenBatch first = batch(config, 1, 1, 4096);
        SegmentBatchFormat.FrozenBatch second = batch(config, 2, 2, 8192);
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
}

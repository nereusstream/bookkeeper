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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
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
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.PublicationStatus;
import org.apache.bookkeeper.common.profile.startup.EphemeralProfileRegistrationStore.WritableRegistration;
import org.apache.bookkeeper.common.profile.startup.Generation;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.CasStatus;
import org.apache.bookkeeper.common.profile.startup.ProfileServiceInfoProjection;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileInitialControlCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ProfileMetadataBacklink;
import org.apache.bookkeeper.discover.profile.ProfileReservation;
import org.apache.bookkeeper.discover.profile.ZKProfileEphemeralRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileRegistrationStore;
import org.apache.bookkeeper.discover.profile.ZKProfileReservationStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.profile.ProfileInboundPipeline;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
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
        return recoveryAddWithEntry(2, entry);
    }

    private static ProfileOperationCodec.AddRecovery recoveryAddWithEntry(long entryId, ByteBuffer entry) {
        byte[] id = new byte[16];
        id[15] = 3;
        byte[] payload = new byte[entry.remaining()];
        entry.slice().get(payload);
        return new ProfileOperationCodec.AddRecovery(context(), id, 7, 9,
                2, 4, entryId, filled(20, (byte) 6), payload);
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

    private static SegmentDataPhysicalDriver.RecoveryRequest collectRecovery(
            List<SegmentDataPhysicalDriver.RecoveryResult> results, AtomicReference<Throwable> failure,
            CountDownLatch done) {
        return new SegmentDataPhysicalDriver.RecoveryRequest((result, cause) -> {
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

    private record ReadReply(SegmentPointRead.ReadStatus status, SegmentPointRead.LeasedResult result,
                             Throwable failure) { }

    private static SegmentPointRead.Reader pointReader(SegmentExecutionGroup group, IsolatedSegmentFiles files,
            SegmentArenaControlLog arena, SegmentBookieControlLog bookie, SegmentHotDirectory hot) throws Exception {
        SegmentOwnerLoop owner = group.owner(0);
        FutureTask<SegmentPointRead.Reader> ready = new FutureTask<>(() -> hot == null
                ? SegmentPointRead.Reader.restarted(owner, group.io(), files, arena, bookie)
                : SegmentPointRead.Reader.live(owner, group.io(), files, arena, bookie, hot));
        assertTrue(owner.control(ready::run));
        return ready.get(5, TimeUnit.SECONDS);
    }

    private static ReadReply pointRead(SegmentPointRead.Reader reader, ProfileLedgerContext context, long entryId)
            throws Exception {
        return pointRead(reader, context, entryId, () -> { });
    }

    private static ReadReply pointRead(SegmentPointRead.Reader reader, ProfileLedgerContext context, long entryId,
            Runnable terminal) throws Exception {
        CompletableFuture<ReadReply> reply = new CompletableFuture<>();
        assertTrue(reader.submit(context, entryId, new SegmentPointRead.ReadRequest(
                (status, result, failure) -> reply.complete(new ReadReply(status, result, failure)), terminal)));
        return reply.get(5, TimeUnit.SECONDS);
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
    public void livePointReadTransfersThePoolLeaseAndBackpressuresReadsAndWrites() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        List<SegmentDataPhysicalDriver.NormalResult> writes = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SegmentPointRead.LeasedResult held = null;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity, 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                CountDownLatch written = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(writes, failure, written)));
                assertTrue(written.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                SegmentPointRead.Reader reader = pointReader(group, files, arena, bookie, hot);
                AtomicInteger actualTerminals = new AtomicInteger();
                ReadReply first = pointRead(reader, context(), 1, actualTerminals::incrementAndGet);
                ownerBarrier(owner);
                assertEquals(1, actualTerminals.get());
                assertEquals(SegmentPointRead.ReadStatus.FOUND, first.status());
                assertNull(first.failure());
                held = first.result();
                assertEquals(1, held.result().record().entryId());
                assertEquals(44, held.result().record().entry().remaining());
                ReadReply refused = pointRead(reader, context(), 1);
                assertEquals(SegmentPointRead.ReadStatus.TRANSIENT_UNAVAILABLE, refused.status());
                assertNull(refused.result());
                ReadReply unknown = pointRead(reader, context(), 99);
                assertEquals(SegmentPointRead.ReadStatus.NOT_READY, unknown.status());
                assertNull(unknown.result());
                CountDownLatch unwritten = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(writes, failure, unwritten)));
                assertTrue(unwritten.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, writes.get(1).status());
                assertEquals(8192, files.nextOffset(0));
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                held.close();
                assertThrows(IllegalStateException.class, first.result()::result);
                ReadReply afterFence = pointRead(reader, context(), 1);
                assertEquals(SegmentPointRead.ReadStatus.FOUND, afterFence.status());
                assertNull(afterFence.failure());
                held = afterFence.result();
                assertFalse(group.shutdown(Duration.ofMillis(50)));
                assertEquals(1, held.result().record().entryId());
                held.close();
                held = null;
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertFalse(reader.submit(context(), 1, new SegmentPointRead.ReadRequest(
                        (status, result, cause) -> {
                            throw new AssertionError("Closed reader delivered a result");
                        })));
            } finally {
                if (held != null) {
                    held.close();
                }
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void pointReadPinsAndBuffersDrainAfterCancelRevocationIoErrorOrCallbackFailure() throws Exception {
        for (int mode = 0; mode < 6; mode++) {
            int failureMode = mode;
            IsolatedSegmentConfig base = config();
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory().resolveSibling("read-" + mode),
                    1, 4, 1, base.bytesPerFile(), 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            CountDownLatch blocked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicInteger actualTerminals = new AtomicInteger();
            AtomicReference<ReadReply> reply = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            RuntimeException callbackFailure = new RuntimeException("Read consumer failed");
            List<SegmentDataPhysicalDriver.NormalResult> writes = new CopyOnWriteArrayList<>();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                grant(arena, 1);
                bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity, 4);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
                try {
                    SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                            new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                    CountDownLatch written = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 1),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(writes, failure, written)));
                    assertTrue(written.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertNull(failure.get());
                    SegmentPointRead.Reader reader = pointReader(group, files, arena, bookie, hot);
                    if (mode == 3) {
                        group.io().shutdown();
                    } else {
                        assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                            @Override
                            public void run(SegmentOwnerLoop.Ticket ticket) {
                                group.io().submit(ticket, () -> {
                                    blocked.countDown();
                                    await(release);
                                }, cause -> failure.compareAndSet(null, cause));
                            }

                            @Override
                            public void failed(Throwable cause) {
                                failure.compareAndSet(null, cause);
                            }
                        }));
                        assertTrue(blocked.await(5, TimeUnit.SECONDS));
                    }
                    SegmentPointRead.ReadRequest request = new SegmentPointRead.ReadRequest((status, result, cause) -> {
                        reply.set(new ReadReply(status, result, cause));
                        delivered.countDown();
                        if (failureMode == 4) {
                            throw callbackFailure;
                        }
                        if (result != null) {
                            result.close();
                        }
                    }, actualTerminals::incrementAndGet);
                    assertTrue(reader.submit(context(), 1, request));
                    if (mode != 3) {
                        ownerBarrier(owner);
                        FutureTask<Integer> leases = new FutureTask<>(() -> owner.pool().outstanding());
                        assertTrue(owner.control(leases::run));
                        assertEquals(1, leases.get(5, TimeUnit.SECONDS).intValue());
                        assertThrows(IOException.class, files::close);
                        assertEquals(0, actualTerminals.get());
                        assertThrows(IOException.class, bookie::close);
                        if (mode == 0) {
                            assertTrue(request.stopWaiting());
                        } else if (mode == 1) {
                            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                                    bookie.submitBatch(List.of(tombstone(44, 3))).get(0).status());
                        } else if (mode == 2) {
                            SegmentPointRead.Location location = writes.get(0).candidate();
                            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                                    StandardOpenOption.WRITE)) {
                                channel.write(ByteBuffer.wrap(new byte[] {1}),
                                        location.recordOffset() + SegmentBatchFormat.RECORD_HEADER + 36);
                                channel.force(false);
                            }
                        } else if (mode == 5) {
                            CountDownLatch poisoned = new CountDownLatch(1);
                            assertTrue(owner.control(() -> {
                                poisoned.countDown();
                                throw callbackFailure;
                            }));
                            assertTrue(poisoned.await(5, TimeUnit.SECONDS));
                        }
                        assertFalse(group.shutdown(Duration.ofMillis(50)));
                        release.countDown();
                    }
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertEquals(1, actualTerminals.get());
                    assertNull(failure.get());
                    assertEquals(8192, files.nextOffset(0));
                    assertEquals(1, files.dataIo.forces);
                    if (mode == 0) {
                        assertNull(reply.get());
                        assertEquals(1, delivered.getCount());
                    } else {
                        assertEquals(0, delivered.getCount());
                        assertEquals(mode == 1 ? SegmentPointRead.ReadStatus.STALE_ADMISSION
                                : mode == 4 ? SegmentPointRead.ReadStatus.FOUND : SegmentPointRead.ReadStatus.UNKNOWN,
                                reply.get().status());
                        if (mode == 2) {
                            assertTrue(reply.get().failure() instanceof IOException);
                        } else if (mode == 3) {
                            assertTrue(reply.get().failure() instanceof RejectedExecutionException);
                        } else if (mode == 5) {
                            assertEquals(callbackFailure, reply.get().failure());
                        } else {
                            assertNull(reply.get().failure());
                        }
                        if (mode == 4) {
                            assertThrows(IllegalStateException.class, reply.get().result()::result);
                        } else {
                            assertNull(reply.get().result());
                        }
                    }
                } finally {
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
                if (mode == 4 || mode == 5) {
                    assertEquals(callbackFailure, owner.failure());
                } else {
                    assertNull(owner.failure());
                }
            }
        }
    }

    @Test
    public void restartedPointReadUsesBoundedMultiStreamInventoryDespiteLowerNewWriteLimit() throws Exception {
        IsolatedSegmentConfig base = config();
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory(), 1, 4, 1,
                base.bytesPerFile(), 4096, 6000, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 5), new Generation(1), 0, 0, 0, new Generation(1), 5, 4096, 1, 1),
                    new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                            new UUID(0, 6), new Generation(1), 0, 0, 0, new Generation(2),
                            6, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 20)));
            bookie.submitBatch(List.of(install(1, (byte) 5)));
            files.writeAuthorized(batch(config, 1, 1, 4096), arena);
            ByteBuf large = entry(context(), 20, -1, 6008, (byte) 7, 6000);
            try {
                files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, 6,
                        new Generation(1), new Generation(2), 20, 4096 + SegmentArenaControlLog.BLOCK_BYTES),
                        List.of(SegmentBatchFormat.entry(context(), large))), arena);
            } finally {
                large.release();
            }
        }
        IsolatedSegmentConfig lower = new IsolatedSegmentConfig(config.directory(), 1, 4, 1,
                config.bytesPerFile(), 4096, 8, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(lower, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(lower, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR);
                IsolatedSegmentFiles files = IsolatedSegmentFiles.openAppendOnlyWithLocallyReadableInventory(
                        lower, identity, arena, bookie, new SegmentFileIo(), 2, 2)) {
            SegmentExecutionGroup tooSmall = SegmentExecutionGroup.start(lower,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            try {
                ExecutionException rejected = assertThrows(ExecutionException.class,
                        () -> pointReader(tooSmall, files, arena, bookie, null));
                assertTrue(rejected.getCause() instanceof IllegalArgumentException);
            } finally {
                assertTrue(tooSmall.shutdown(Duration.ofSeconds(5)));
            }
            SegmentExecutionGroup group = SegmentExecutionGroup.start(lower,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 8192, 44, 12331, Duration.ofSeconds(5)));
            try {
                SegmentPointRead.Reader reader = pointReader(group, files, arena, bookie, null);
                for (long entryId : new long[] {1, 20}) {
                    ReadReply reply = pointRead(reader, context(), entryId);
                    assertEquals(SegmentPointRead.ReadStatus.FOUND, reply.status());
                    assertNull(reply.failure());
                    try (SegmentPointRead.LeasedResult result = reply.result()) {
                        assertEquals(entryId, result.result().record().entryId());
                        assertEquals(entryId == 1 ? 44 : 6036, result.result().record().entry().remaining());
                    }
                }
                assertEquals(SegmentPointRead.ReadStatus.NOT_READY, pointRead(reader, context(), 99).status());
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, 7), new Generation(1), 0, 0, 0, new Generation(3),
                        7, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES, 1, 30)));
                ReadReply stale = pointRead(reader, context(), 1);
                assertEquals(SegmentPointRead.ReadStatus.NOT_READY, stale.status());
                assertNull(stale.result());
                assertTrue(stale.failure() instanceof IOException);
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(group.owner(0).failure());
        }
    }

    @Test
    public void automaticPublishedDuplicatesShareThePoolAndRetryAfterBackpressure() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        List<SegmentDataPhysicalDriver.NormalResult> normals = new CopyOnWriteArrayList<>();
        List<SegmentDataPhysicalDriver.RecoveryResult> recoveries = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<SegmentBufferPool.Lease> held = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6), activate(2)));
            bookie.submitRecoveryGrant(recoveryGrant(3, 3, 7, 9));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 4096, 12287, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                CountDownLatch written = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        collectNormal(normals, failure, written)));
                assertTrue(written.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, normals.get(0).status());
                CountDownLatch acquired = new CountDownLatch(1);
                assertTrue(owner.control(() -> {
                    held.set(owner.pool().acquire());
                    acquired.countDown();
                }));
                assertTrue(acquired.await(5, TimeUnit.SECONDS));
                assertTrue(held.get() != null);
                CountDownLatch refused = new CountDownLatch(2);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        collectNormal(normals, failure, refused)));
                assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                        entryBytes(context(), 2, -1, 8, (byte) 0))).orElseThrow(),
                        collectRecovery(recoveries, failure, refused)));
                assertTrue(refused.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, normals.get(1).status());
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.TRANSIENT_UNAVAILABLE,
                        recoveries.get(0).status());
                assertNull(normals.get(1).candidate());
                assertNull(recoveries.get(0).candidate());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(8192, files.nextOffset(0));
                assertEquals(1, files.dataIo.forces);
                held.getAndSet(null).close();

                AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> manualStatus = new AtomicReference<>();
                CountDownLatch manual = new CountDownLatch(1);
                assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        entryBytes(context(), 2, 1, 8, (byte) 0),
                        new SegmentDataPhysicalDriver.DuplicateRequest((status, cause) -> {
                            manualStatus.set(status);
                            if (cause != null) {
                                failure.compareAndSet(null, cause);
                            }
                            manual.countDown();
                        })));
                assertTrue(manual.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE, manualStatus.get());

                CountDownLatch same = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2, 1, 8, (byte) 0, 8),
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        collectNormal(normals, failure, same)));
                assertTrue(same.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, normals.get(2).status());
                assertEquals(normals.get(0).candidate(), normals.get(2).candidate());
                CountDownLatch conflict = new CountDownLatch(1);
                assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                        entryBytes(context(), 2, 1, 8, (byte) 9))).orElseThrow(),
                        collectRecovery(recoveries, failure, conflict)));
                assertTrue(conflict.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.CONFLICT, recoveries.get(1).status());
                assertNull(failure.get());
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(8192, files.nextOffset(0));
                assertEquals(1, files.dataIo.forces);
            } finally {
                if (held.get() != null) {
                    held.getAndSet(null).close();
                }
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void queuedPublishedReadRetainsSourceAndScratchThroughCancellationOrReadFailure() throws Exception {
        for (boolean cancel : new boolean[] {true, false}) {
            IsolatedSegmentConfig base = config();
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory().resolveSibling("read-" + cancel),
                    1, 4, 1, base.bytesPerFile(), 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            CountDownLatch blocked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch compared = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
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
                try {
                    SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                            new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                    CountDownLatch written = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 1),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, written)));
                    assertTrue(written.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertNull(failure.get());
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                    assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                        @Override
                        public void run(SegmentOwnerLoop.Ticket ticket) {
                            group.io().submit(ticket, () -> {
                                blocked.countDown();
                                await(release);
                            }, cause -> failure.compareAndSet(null, cause));
                        }

                        @Override
                        public void failed(Throwable cause) {
                            failure.compareAndSet(null, cause);
                        }
                    }));
                    assertTrue(blocked.await(5, TimeUnit.SECONDS));
                    SegmentEntryInputs.Input input = sourceInput(group, context(), 1);
                    ByteBuf source = input.bytes();
                    SegmentBookieControlLog.Admission admission = bookie.admitNormal(
                            context(), filled(20, (byte) 5)).orElseThrow();
                    SegmentDataPhysicalDriver.NormalRequest request = new SegmentDataPhysicalDriver.NormalRequest(
                            (result, cause) -> {
                                results.add(result);
                                failure.set(cause);
                                if (group.inputs().usedBytes() != 44 || source.refCnt() != 1
                                        || owner.pool().outstanding() != 1) {
                                    failure.set(new AssertionError("Read buffers released before owner result"));
                                }
                                compared.countDown();
                            });
                    assertTrue(batcher.submit(input, admission, request));
                    ownerBarrier(owner);
                    FutureTask<Integer> leases = new FutureTask<>(() -> owner.pool().outstanding());
                    assertTrue(owner.control(leases::run));
                    assertEquals(1, leases.get(5, TimeUnit.SECONDS).intValue());
                    assertEquals(44, group.inputs().usedBytes());
                    assertEquals(1, source.refCnt());
                    if (cancel) {
                        assertTrue(request.stopWaiting());
                    } else {
                        SegmentPointRead.Location location = results.get(0).candidate();
                        try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                                StandardOpenOption.WRITE)) {
                            channel.write(ByteBuffer.wrap(new byte[] {1}),
                                    location.recordOffset() + SegmentBatchFormat.RECORD_HEADER + 36);
                            channel.force(false);
                        }
                    }
                    assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                            bookie.submitFence(fence(3, (byte) 5)).status());
                    assertFalse(group.shutdown(Duration.ofMillis(50)));
                    assertEquals(44, group.inputs().usedBytes());
                    assertEquals(1, source.refCnt());
                    assertTrue(bookie.canCompleteNormal(admission));
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertEquals(0, source.refCnt());
                    assertEquals(0, group.inputs().usedBytes());
                    assertEquals(1, files.dataIo.forces);
                    assertEquals(8192, files.nextOffset(0));
                    if (cancel) {
                        assertEquals(1, compared.getCount());
                        assertEquals(1, results.size());
                        assertNull(failure.get());
                    } else {
                        assertEquals(0, compared.getCount());
                        assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, results.get(1).status());
                        assertTrue(failure.get() instanceof IOException);
                    }
                    assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                            bookie.submitFence(fence(3, (byte) 5)).status());
                } finally {
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
                assertNull(owner.failure());
            }
        }
    }

    @Test
    public void automaticRecoveryBatchingNeedsNoNormalActivationAndComparesDuplicates() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        List<SegmentDataPhysicalDriver.RecoveryResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
                assertTrue(owner.control(() -> {
                    paused.countDown();
                    await(release);
                }));
                assertTrue(paused.await(5, TimeUnit.SECONDS));
                CountDownLatch done = new CountDownLatch(4);
                for (ByteBuffer bytes : List.of(entryBytes(context(), 2, -1, 8, (byte) 0),
                        entryBytes(context(), 2, 1, 8, (byte) 0),
                        entryBytes(context(), 2, -1, 8, (byte) 9),
                        entryBytes(context(), 3, -1, 8, (byte) 0))) {
                    long entryId = bytes.getLong(8);
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(
                            recoveryAddWithEntry(entryId, bytes)).orElseThrow(),
                            collectRecovery(results, failure, done)));
                }
                assertEquals(176, group.inputs().usedBytes());
                assertTrue(bookie.admitNormal(context(), filled(20, (byte) 6)).isEmpty());
                release.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(1, files.dataIo.forces);
                assertEquals(3, results.stream().filter(result -> result.status()
                        == SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE).count());
                assertEquals(1, results.stream().filter(result -> result.status()
                        == SegmentDataPhysicalDriver.RecoveryStatus.CONFLICT).count());
                CountDownLatch compared = new CountDownLatch(2);
                for (byte payload : new byte[] {0, 9}) {
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                            entryBytes(context(), 2, 1, 8, payload))).orElseThrow(),
                            collectRecovery(results, failure, compared)));
                }
                assertTrue(compared.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                List<SegmentPointRead.Location> locations = results.stream()
                        .map(SegmentDataPhysicalDriver.RecoveryResult::candidate).filter(java.util.Objects::nonNull)
                        .distinct().toList();
                assertEquals(2, locations.size());
                for (SegmentPointRead.Location location : locations) {
                    assertEquals(location.entryId(), SegmentPointRead.read(config, identity, arena, location)
                            .record().entryId());
                }
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticMixedBatchAndCrossRetriesRecheckEachAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentFileHeader identity = identity();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<SegmentDataPhysicalDriver.NormalResult> normals = new CopyOnWriteArrayList<>();
        List<SegmentDataPhysicalDriver.RecoveryResult> recoveries = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6), activate(2)));
            bookie.submitRecoveryGrant(recoveryGrant(3, 3, 7, 9));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
                CountDownLatch done = new CountDownLatch(4);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        collectNormal(normals, failure, done)));
                assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(3,
                        entryBytes(context(), 3, -1, 8, (byte) 0))).orElseThrow(),
                        collectRecovery(recoveries, failure, done)));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                        entryBytes(context(), 2, 1, 8, (byte) 0))).orElseThrow(),
                        collectRecovery(recoveries, failure, done)));
                assertTrue(batcher.submit(sourceInput(group, context(), 3),
                        bookie.admitNormal(context(), filled(20, (byte) 6)).orElseThrow(),
                        collectNormal(normals, failure, done)));
                ownerBarrier(owner);
                assertEquals(SegmentBookieControlLog.FenceStatus.WAITING_PRECUT,
                        bookie.submitFence(fence(10, (byte) 6)).status());
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT,
                        bookie.submitRecoveryClose(recoveryClose(11, 3, 3, 7, 9)));
                release.countDown();
                assertTrue(done.await(5, TimeUnit.SECONDS));
                ownerBarrier(owner);
                assertNull(failure.get());
                assertEquals(2, normals.size());
                assertTrue(normals.stream().allMatch(result -> result.status()
                        == SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE));
                assertEquals(2, recoveries.size());
                assertTrue(recoveries.stream().allMatch(result -> result.status()
                        == SegmentDataPhysicalDriver.RecoveryStatus.STALE_ADMISSION));
                assertEquals(1, files.dataIo.forces);
                assertEquals(0, group.inputs().usedBytes());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(10, (byte) 6)).status());
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                        bookie.submitRecoveryClose(recoveryClose(11, 3, 3, 7, 9)));
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticRecoveryInputBudgetAndWireIdentityRejectBeforeData() throws Exception {
        IsolatedSegmentConfig base = config();
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory(), 1, 4, 1,
                base.bytesPerFile(), 4096, 8, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = identity();
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<SegmentDataPhysicalDriver.RecoveryResult> results = new CopyOnWriteArrayList<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 44, 16426, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                    new SegmentHotDirectory(owner, config, identity, 4));
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 2, Duration.ofSeconds(30)));
                ByteBuffer corrupt = entryBytes(context(), 2, -1, 8, (byte) 0);
                corrupt.put(43, (byte) 1);
                assertThrows(IllegalArgumentException.class, () -> batcher.submitRecovery(bookie.admitRecovery(
                        recoveryAddWithEntry(corrupt)).orElseThrow(),
                        new SegmentDataPhysicalDriver.RecoveryRequest((unused, cause) -> { })));
                assertEquals(0, group.inputs().usedBytes());
                assertThrows(IOException.class, () -> batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                        entryBytes(context(), 3, -1, 8, (byte) 0))).orElseThrow(),
                        new SegmentDataPhysicalDriver.RecoveryRequest((unused, cause) -> { })));
                assertEquals(0, group.inputs().usedBytes());
                assertTrue(owner.control(() -> {
                    paused.countDown();
                    await(release);
                }));
                assertTrue(paused.await(5, TimeUnit.SECONDS));
                CountDownLatch done = new CountDownLatch(1);
                assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                        entryBytes(context(), 2, -1, 8, (byte) 0))).orElseThrow(),
                        collectRecovery(results, failure, done)));
                assertEquals(44, group.inputs().usedBytes());
                assertFalse(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(3,
                        entryBytes(context(), 3, -1, 8, (byte) 0))).orElseThrow(),
                        new SegmentDataPhysicalDriver.RecoveryRequest((unused, cause) -> {
                            throw new AssertionError("Unaccepted input has no response obligation");
                        })));
                assertEquals(44, group.inputs().usedBytes());
                assertEquals(0, files.dataIo.forces);
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertEquals(0, group.inputs().usedBytes());
                assertNull(failure.get());
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE, results.get(0).status());
                assertEquals(1, files.dataIo.forces);
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
            assertNull(owner.failure());
        }
    }

    @Test
    public void automaticRecoveryRefillCancellationShutdownAndFailuresRetainObligations() throws Exception {
        for (int mode : new int[] {0, 1, 2}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("recovery-refill-" + mode),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, mode == 1 ? 1 : -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            List<SegmentDataPhysicalDriver.RecoveryResult> results = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(
                            config, identity, 1024 * 1024, controlIo);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                bookie.submitBatch(List.of(install(1, (byte) 6)));
                bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
                if (mode == 2) {
                    files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                }
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                        new SegmentHotDirectory(owner, config, identity, 4));
                try {
                    SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(
                            driver, owner, bookie, allocator, 1);
                    SegmentDataPhysicalDriver.RecoveryRequest cancelled = new SegmentDataPhysicalDriver.RecoveryRequest(
                            (unused, cause) -> {
                                throw new AssertionError("Cancelled response must stay detached");
                            });
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                            entryBytes(context(), 2, -1, 8, (byte) 0))).orElseThrow(), cancelled));
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertTrue(cancelled.stopWaiting());
                    CountDownLatch done = new CountDownLatch(1);
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(recoveryAddWithEntry(
                            entryBytes(context(), 2, 1, 8, (byte) 0))).orElseThrow(),
                            collectRecovery(results, failure, done)));
                    ownerBarrier(owner);
                    assertEquals(44, group.inputs().usedBytes());
                    assertEquals(4096, files.nextOffset(0));
                    assertEquals(0, files.dataIo.forces);
                    assertThrows(IOException.class, bookie::close);
                    assertFalse(group.shutdown(Duration.ofMillis(50)));
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertTrue(done.await(5, TimeUnit.SECONDS));
                    assertEquals(mode == 0 ? SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE
                            : SegmentDataPhysicalDriver.RecoveryStatus.UNKNOWN, results.get(0).status());
                    assertEquals(mode == 1 ? 4096 : 8192, files.nextOffset(0));
                    assertEquals(mode == 1 ? 0 : 1, files.dataIo.forces);
                    assertEquals(mode == 2 ? 2 : 1, controlIo.forces);
                    assertEquals(mode == 2, arena.hasDurableQuarantine(0));
                    assertEquals(0, group.inputs().usedBytes());
                    if (mode == 0) {
                        assertNull(failure.get());
                    } else {
                        assertTrue(failure.get() != null);
                    }
                } finally {
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
                assertNull(owner.failure());
            }
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
                    new SegmentExecutionGroup.Settings(3, 2, 9, 3, 4096, 4096, 28669, Duration.ofSeconds(5)));
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
                        new SegmentExecutionGroup.Settings(2, 2, 4, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
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
    public void automaticDataRequiresFailureControlCapacityAndBackpressuresBeforeCoordinates() throws Exception {
        for (int mode : new int[] {0, 1}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("quarantine-capacity-" + mode),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(
                            config, identity, mode == 0 ? 1024 * 1024 : 4096 + 192);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                grant(arena, 1);
                bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, mode == 0 ? 1 : 2, 1,
                                4096, 4096, 12287, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                        new SegmentHotDirectory(owner, config, identity, 4));
                try {
                    if (mode == 0) {
                        FutureTask<Void> rejected = new FutureTask<>(() -> {
                            assertThrows(IllegalArgumentException.class, () -> driver.enableNormalBatching(bookie,
                                    new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)),
                                    allocator, 1));
                            return null;
                        });
                        assertTrue(owner.control(rejected::run));
                        rejected.get(5, TimeUnit.SECONDS);
                    } else {
                        SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(
                                driver, owner, bookie, allocator, 1);
                        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
                        AtomicReference<Throwable> failure = new AtomicReference<>();
                        CountDownLatch done = new CountDownLatch(1);
                        assertFalse(batcher.submit(sourceInput(group, context(), 1),
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                collectNormal(results, failure, done)));
                        ownerBarrier(owner);
                        assertTrue(results.isEmpty());
                        assertEquals(1, done.getCount());
                        assertNull(failure.get());
                    }
                    assertEquals(4096, files.nextOffset(0));
                    assertEquals(0, files.dataIo.writes);
                    assertEquals(0, files.dataIo.forces);
                    assertEquals(0, group.inputs().usedBytes());
                    assertFalse(arena.hasDurableQuarantine(0));
                    assertTrue(arena.dataFileAvailable(0));
                    assertNull(owner.failure());
                } finally {
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
            }
        }
    }

    @Test
    public void dataForceFailureBlocksServiceAndAttemptsDurableArenaQuarantine() throws Exception {
        for (int mode : new int[] {0, 1, 2, 3}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("data-quarantine-" + mode),
                    1, 4, 1, 4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentFileHeader identity = identity();
            AtomicBoolean holdControl = new AtomicBoolean();
            CountDownLatch controlAtForce = new CountDownLatch(1);
            CountDownLatch releaseControl = new CountDownLatch(1);
            CountDownLatch controlDone = new CountDownLatch(1);
            CountDownLatch dataAtForce = new CountDownLatch(1);
            CountDownLatch releaseData = new CountDownLatch(1);
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, mode == 2 ? 2 : -1, false, () -> {
                if (holdControl.get()) {
                    controlAtForce.countDown();
                    await(releaseControl);
                }
            });
            List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(
                            config, identity, 1024 * 1024, controlIo);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity,
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                grant(arena, 1);
                bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(2, 2, 4, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(group.owner(1), group.io(), arena);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity, 4);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                        new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
                SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(driver, owner, bookie, allocator, 1);
                try {
                    CountDownLatch first = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 1),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, first)));
                    assertTrue(first.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                    SegmentPointRead.Reader reader = pointReader(group, files, arena, bookie, hot);
                    try (SegmentPointRead.LeasedResult read = pointRead(reader, context(), 1).result()) {
                        assertEquals(1, read.result().record().entryId());
                    }
                    ownerBarrier(group.owner(1), 1);
                    assertEquals(1, controlIo.forces); // Empty startup needs no sync; only the initial allocation.
                    files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false, () -> {
                        dataAtForce.countDown();
                        await(releaseData);
                    });
                    CountDownLatch second = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 2),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, second)));
                    assertTrue(dataAtForce.await(5, TimeUnit.SECONDS));
                    assertThrows(IOException.class, arena::close);
                    if (mode == 1) {
                        holdControl.set(true);
                        assertTrue(allocator.submit(List.of(new SegmentArenaControlLog.Allocation(
                                SegmentControlFormat.Type.ALLOC, new UUID(0, 9), new Generation(1), 0,
                                0, 2, new Generation(1), 9, 4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1)),
                                (applied, cause) -> {
                                    assertNull(cause);
                                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                                            applied.get(0).status());
                                    controlDone.countDown();
                                }));
                        assertTrue(controlAtForce.await(5, TimeUnit.SECONDS));
                    }
                    if (mode == 3) {
                        assertFalse(group.shutdown(Duration.ofMillis(50)));
                    }
                    releaseData.countDown();
                    assertTrue(second.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, results.get(1).status());
                    assertNull(results.get(1).candidate());
                    assertTrue(failure.get() != null);
                    assertFalse(arena.dataFileAvailable(0));
                    if (mode != 3) {
                        assertEquals(SegmentPointRead.ReadStatus.NOT_READY, pointRead(reader, context(), 1).status());
                    }
                    assertThrows(IOException.class, () -> SegmentPointRead.read(
                            config, identity, arena, results.get(0).candidate()));
                    long offset = files.nextOffset(0);
                    files.dataIo = new SegmentFileIo();
                    if (mode != 3) {
                        CountDownLatch retry = new CountDownLatch(1);
                        assertFalse(batcher.submit(sourceInput(group, context(), 1),
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                collectNormal(results, failure, retry)));
                        assertEquals(1, retry.getCount());
                        assertEquals(2, results.size());
                        assertEquals(SegmentDataPhysicalDriver.NormalStatus.UNKNOWN, results.get(1).status());
                    }
                    assertEquals(offset, files.nextOffset(0));
                    assertEquals(0, files.dataIo.writes);
                    assertEquals(0, files.dataIo.forces);
                    releaseControl.countDown();
                    if (mode == 1) {
                        assertTrue(controlDone.await(5, TimeUnit.SECONDS));
                    }
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertEquals(mode != 2, arena.hasDurableQuarantine(0));
                    assertEquals(0, group.inputs().usedBytes());
                    assertNull(owner.failure());
                } finally {
                    releaseData.countDown();
                    releaseControl.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
            }
            // An unrelated successful sync cannot clear a recorded quarantine.
            try (FileChannel channel = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                assertTrue(replay.hasDurableQuarantine(0));
                assertFalse(replay.dataFileAvailable(0));
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                        config, identity, replay, new SegmentFileIo()));
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
                    new SegmentExecutionGroup.Settings(1, 2, 2, 1, bufferBytes, sourceBytes,
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
                    new SegmentExecutionGroup.Settings(1, 2, 2, 2, SegmentBatchFormat.MAX_BATCH, sourceBytes,
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

    private IsolatedSegmentStorage storage(IsolatedSegmentConfig config, int workers, int controlSlots,
                                           int hotEntries) {
        return storage(config, workers, controlSlots, hotEntries, hotEntries);
    }

    private IsolatedSegmentStorage storage(IsolatedSegmentConfig config, int workers, int controlSlots,
                                           int hotEntries, int indexedEntries) {
        return storage(config, workers, controlSlots, hotEntries, indexedEntries, 2);
    }

    private IsolatedSegmentStorage storage(IsolatedSegmentConfig config, int workers, int controlSlots,
                                           int hotEntries, int indexedEntries, int ioThreads) {
        int buffers = workers * 2;
        return new IsolatedSegmentStorage(config, identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(workers, ioThreads, controlSlots, buffers, 4096, 4096,
                        buffers * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, hotEntries, indexedEntries, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))));
    }

    private static void installStorage(IsolatedSegmentStorage storage,
                                       ProtectedProfileStateStore.Transition... transitions) throws Exception {
        CompletableFuture<List<SegmentBookieControlLog.Result>> ready = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        // The callback can wake this caller before the prior driver's finally releases its busy gate.
        while (!storage.controls().submitBatch(List.of(transitions), (results, cause) -> {
            if (cause == null) {
                ready.complete(results);
            } else {
                ready.completeExceptionally(cause);
            }
        })) {
            assertTrue("Control admission did not resume", System.nanoTime() < deadline);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        for (SegmentBookieControlLog.Result result : ready.get(
                Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE, result.status());
        }
    }

    private static SegmentDataPhysicalDriver.NormalResult storageWrite(IsolatedSegmentStorage storage,
            ProfileLedgerContext context, long entryId) throws Exception {
        SegmentDataPhysicalDriver.NormalResult result = storageWriteReply(storage, context, entryId);
        assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
        return result;
    }

    @Test
    public void profileSocketAddReturnsRealDurabilityAndRetryDoesNotWriteAgain() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
                socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                socket.status(2, ProfileStatus.StatusClass.OK, ProfileStatus.DurableResult.APPLIED);
                int writes = io.writes;
                assertTrue(writes > 0);
                socket.add(3, context(), 1, (byte) 5, entry(context(), 1, 0, 8, (byte) 0));
                socket.status(3, ProfileStatus.StatusClass.OK, ProfileStatus.DurableResult.APPLIED);
                assertEquals(writes, io.writes);
                socket.add(4, context(), 1, (byte) 5, entry(context(), 1, -1, 8, (byte) 9));
                socket.status(4, ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT,
                        ProfileStatus.DurableResult.NONE);
                assertEquals(writes, io.writes);
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                    assertEquals(1, read.result().record().entryId());
                    assertEquals(0, read.result().record().entry().get(36));
                }
            }
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
            try (ProfileSocket socket = new ProfileSocket(restarted)) {
                socket.add(5, context(), 1, (byte) 5, entry(context(), 1));
                socket.status(5, ProfileStatus.StatusClass.OK, ProfileStatus.DurableResult.APPLIED);
            }
        }
    }

    @Test
    public void profileSocketRejectsUnboundBodiesAndUnauthorizedStateBeforeDataIo() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2), install(45, 3, (byte) 5));
            SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.add(2, context(), 1, (byte) 6, entry(context(), 1));
                socket.status(2, ProfileStatus.StatusClass.UNAUTHORIZED, ProfileStatus.DurableResult.NONE);
                ProfileLedgerContext foreign = new ProfileLedgerContext(44, filled(16, (byte) 2),
                        DESCRIPTOR_IDENTITY.toBytes());
                socket.add(3, foreign, 1, (byte) 5, entry(foreign, 1));
                socket.status(3, ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT, ProfileStatus.DurableResult.NONE);
                socket.add(4, context(45), 1, (byte) 5, entry(context(45), 1));
                socket.status(4, ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                        ProfileStatus.DurableResult.NONE);
                socket.add(5, context(), 1, (byte) 5, entry(context(), 2));
                socket.status(5, ProfileStatus.StatusClass.BAD_REQUEST, ProfileStatus.DurableResult.NONE);
                ByteBuf corrupt = entry(context(), 1);
                corrupt.setInt(32, corrupt.getInt(32) ^ 1);
                socket.add(6, context(), 1, (byte) 5, corrupt);
                socket.status(6, ProfileStatus.StatusClass.BAD_REQUEST, ProfileStatus.DurableResult.NONE);
                byte[] malformed = profileAddBody(context(), 1, (byte) 5, entry(context(), 1));
                ByteBuffer.wrap(malformed).putInt(96, malformed.length - 101);
                socket.send(ProfileOperation.ADD_NORMAL, 8, malformed);
                socket.status(8, ProfileStatus.StatusClass.BAD_REQUEST, ProfileStatus.DurableResult.NONE);
                ByteBuffer.wrap(malformed).putInt(96, malformed.length - 100).putInt(68, 1);
                socket.send(ProfileOperation.ADD_NORMAL, 9, malformed);
                socket.status(9, ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                        ProfileStatus.DurableResult.NONE);
                CompletableFuture<SegmentBookieControlLog.FenceResult> fenced = new CompletableFuture<>();
                assertTrue(storage.controls().submitFence(fence(4, (byte) 5), (result, cause) -> {
                    if (cause == null) {
                        fenced.complete(result);
                    } else {
                        fenced.completeExceptionally(cause);
                    }
                }));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        fenced.get(5, TimeUnit.SECONDS).status());
                socket.add(10, context(), 1, (byte) 5, entry(context(), 1));
                socket.status(10, ProfileStatus.StatusClass.FENCED, ProfileStatus.DurableResult.NONE);
                installStorage(storage, tombstone(44, 5));
                socket.add(11, context(), 1, (byte) 5, entry(context(), 1));
                socket.status(11, ProfileStatus.StatusClass.TOMBSTONED_OR_DELETED, ProfileStatus.DurableResult.NONE);
                assertEquals(0, io.writes);
                assertEquals(0, io.forces);
            }
        }
    }

    @Test
    public void profileSocketAddWaitsForActualForceBeforeSuccess() throws Exception {
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(io.writes > 0);
                assertEquals(0, io.forces);
                assertNull("No success before force", socket.responses.poll(100, TimeUnit.MILLISECONDS));
                assertTrue(socket.budget.used(ProfileReceiveBudget.Lane.DATA) > 64);
                release.countDown();
                socket.status(2, ProfileStatus.StatusClass.OK, ProfileStatus.DurableResult.APPLIED);
                assertEquals(1, io.forces);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void profileSocketRejectsInstalledEntryLimitFromHeaderWithoutReceivingBody() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                byte[] oversized = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.ADD_NORMAL, 2,
                        profileAddBody(context(), 1, (byte) 5,
                                entry(context(), 1, -1, 1025, (byte) 0, 1025))));
                socket.client.writeAndFlush(Unpooled.wrappedBuffer(oversized, 0, 36)).sync();
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                socket.assertBudgetZero();
                assertNull(socket.responses.poll());
                assertEquals(0, io.writes);
                assertEquals(0, io.forces);
            }
        }
    }

    @Test
    public void profileSocketForceFailureRetainsUnknownInsteadOfPreAdmissionNone() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                ProfileStatus status = socket.status(2, ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN,
                        ProfileStatus.DurableResult.UNKNOWN);
                assertEquals(ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION, status.retryDisposition());
                assertTrue(io.writes > 0);
                assertEquals(1, io.forces);
            }
            IOException failed = assertThrows(IOException.class, storage::close);
            assertTrue(failed.getMessage(), failed.getMessage().contains("Failed DATA session must remain unclosed"));
        }
    }

    @Test
    public void profileSocketDisconnectReleasesIngressButKeepsSubmittedIoAndWriterExclusion() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3);
        try {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertFalse(storage.close(Duration.ofMillis(50)));
                socket.add(3, context(), 2, (byte) 5, entry(context(), 2));
                socket.status(3, ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE, ProfileStatus.DurableResult.NONE);
                socket.disconnect();
                socket.assertBudgetZero();
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                assertEquals(1, io.forces);
                assertNull(socket.responses.poll());
            }
        } finally {
            release.countDown();
            storage.close();
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
    }

    private static byte[] profileAddBody(ProfileLedgerContext context, long entryId, byte verifier, ByteBuf entry) {
        try {
            byte[] bytes = new byte[entry.readableBytes()];
            entry.getBytes(entry.readerIndex(), bytes);
            return ProfileOperationCodec.encode(ProfileOperation.ADD_NORMAL,
                    new ProfileOperationCodec.AddNormal(context, entryId, 0, filled(20, verifier), bytes));
        } finally {
            entry.release();
        }
    }

    @Test
    public void profileSocketHandlerRemovalKeepsResponseCreditUntilActualDownstreamWriteTerminal() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                CompletableFuture<HeldProfileResponse> writing = new CompletableFuture<>();
                Channel server = socket.accepted.get();
                server.eventLoop().submit(() -> server.pipeline().addAfter("profileReceive", "heldResponse",
                        new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                                assertTrue(writing.complete(new HeldProfileResponse((ByteBuf) message, promise)));
                            }
                        })).sync();
                HeldProfileResponse held = null;
                try {
                    socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                    held = writing.get(5, TimeUnit.SECONDS);
                    assertEquals(1, held.bytes().refCnt());
                    server.eventLoop().submit(() -> server.pipeline().remove(SegmentProfileAddHandler.class)).sync();
                    assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                    assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
                    assertTrue("Held response must remain charged",
                            socket.budget.used(ProfileReceiveBudget.Lane.DATA) > 0);
                    assertEquals(1, held.bytes().refCnt());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                        assertEquals(1, read.result().record().entryId());
                    }
                } finally {
                    if (held != null) {
                        held.bytes().release();
                        held.promise().setFailure(new ClosedChannelException());
                        server.eventLoop().submit(() -> { }).sync();
                    }
                }
                socket.assertBudgetZero();
                assertNull(socket.responses.poll());
            }
        }
    }

    private record HeldProfileResponse(ByteBuf bytes, ChannelPromise promise) { }

    @Test
    public void profileSocketCancellationKeepsQueuedCompletionCreditUntilEventLoopDeliveryTerminal() throws Exception {
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch releaseIo = new CountDownLatch(1);
        CountDownLatch loopBlocked = new CountDownLatch(1);
        CountDownLatch cancelOnLoop = new CountDownLatch(1);
        CountDownLatch removed = new CountDownLatch(1);
        CountDownLatch releaseLoop = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = new SegmentFileIo(
                    Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(releaseIo);
                    });
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                try {
                    socket.add(2, context(), 1, (byte) 5, entry(context(), 1));
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    Channel server = socket.accepted.get();
                    server.eventLoop().execute(() -> {
                        loopBlocked.countDown();
                        await(cancelOnLoop);
                        server.pipeline().remove(SegmentProfileAddHandler.class);
                        removed.countDown();
                        await(releaseLoop);
                    });
                    assertTrue(loopBlocked.await(5, TimeUnit.SECONDS));
                    releaseIo.countDown();
                    // A real FOUND read completes on the same owner after Add's publication/completion callback.
                    long readDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    for (;;) {
                        CompletableFuture<ReadReply> readable = new CompletableFuture<>();
                        assertTrue(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                                (status, result, cause) -> readable.complete(new ReadReply(status, result, cause)))));
                        ReadReply read = readable.get(5, TimeUnit.SECONDS);
                        assertNull(read.failure());
                        if (read.status() == SegmentPointRead.ReadStatus.FOUND) {
                            try (SegmentPointRead.LeasedResult found = read.result()) {
                                assertEquals(1, found.result().record().entryId());
                            }
                            break;
                        }
                        assertTrue(read.status() == SegmentPointRead.ReadStatus.NOT_READY
                                || read.status() == SegmentPointRead.ReadStatus.TRANSIENT_UNAVAILABLE);
                        assertTrue("Actual durable read did not become available", System.nanoTime() < readDeadline);
                    }
                    ownerBarrier(storageOwner(storage, 0));
                    cancelOnLoop.countDown();
                    assertTrue(removed.await(5, TimeUnit.SECONDS));
                    assertTrue("Cancelled queued completion must remain charged",
                            socket.budget.used(ProfileReceiveBudget.Lane.DATA) > 64);
                    releaseLoop.countDown();
                    assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                    server.eventLoop().submit(() -> { }).sync();
                    socket.assertBudgetZero();
                    assertNull(socket.responses.poll());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                        assertEquals(1, read.result().record().entryId());
                    }
                } finally {
                    releaseIo.countDown();
                    cancelOnLoop.countDown();
                    releaseLoop.countDown();
                }
            }
        }
    }

    /** Actual TLS socket fixture; all operation dispatch and persistence use the production adapter code. */
    @Test
    public void profileSocketFencePersistsAndResolvesSameOperationAfterRestart() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            SegmentFileIo io = bookieIo(bookie);
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.FENCE_LEDGER, 2, ProfileOperationCodec.encode(
                        ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                socket.status(ProfileOperation.FENCE_LEDGER, 2, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                int writes = io.writes;
                assertTrue(writes > 0);
                socket.send(ProfileOperation.FENCE_LEDGER, 3, ProfileOperationCodec.encode(
                        ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                socket.status(ProfileOperation.FENCE_LEDGER, 3, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.ALREADY_APPLIED);
                assertEquals(writes, io.writes);
                assertEquals(1, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            }
            try (ProfileSocket data = new ProfileSocket(storage)) {
                data.add(2, context(), 1, (byte) 5, entry(context(), 1));
                data.status(2, ProfileStatus.StatusClass.FENCED, ProfileStatus.DurableResult.NONE);
            }
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (ProfileSocket socket = new ProfileSocket(restarted)) {
                socket.send(ProfileOperation.FENCE_LEDGER, 4, ProfileOperationCodec.encode(
                        ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                socket.status(ProfileOperation.FENCE_LEDGER, 4, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.ALREADY_APPLIED);
            }
        }
    }

    @Test
    public void profileSocketFenceDenialsNeverClaimControlOrChangeLocalAuthority() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            int writes = bookieIo(bookie).writes;
            byte[] valid = ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5));
            for (int variant = 0; variant < 4; variant++) {
                byte[] body = valid.clone();
                ProfileStatus.StatusClass expected;
                switch (variant) {
                    case 0 -> {
                        body[80] ^= 1;
                        expected = ProfileStatus.StatusClass.UNAUTHORIZED;
                    }
                    case 1 -> {
                        body[8] ^= 1;
                        expected = ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT;
                    }
                    case 2 -> {
                        Arrays.fill(body, 60, 76, (byte) 0);
                        expected = ProfileStatus.StatusClass.BAD_REQUEST;
                    }
                    default -> {
                        body[79] = 19;
                        expected = ProfileStatus.StatusClass.BAD_REQUEST;
                    }
                }
                try (ProfileSocket socket = new ProfileSocket(storage)) {
                    socket.send(ProfileOperation.FENCE_LEDGER, 2, body);
                    socket.status(ProfileOperation.FENCE_LEDGER, 2, expected, ProfileStatus.DurableResult.NONE);
                    assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                    socket.assertBudgetZero();
                    assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                }
                assertTrue(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                assertEquals(writes, bookieIo(bookie).writes);
            }
        }
    }

    @Test
    public void profileSocketFenceCapacityTransferFailsBeforeAdmissionCutOrIo() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            int writes = bookieIo(bookie).writes;
            // The frame and base alone fit 256 bytes; including the real response workspace must fail.
            try (ProfileSocket socket = new ProfileSocket(storage,
                    new ProfileReceiveBudget(196608, 256, 131072, 1, 1, 1))) {
                socket.send(ProfileOperation.FENCE_LEDGER, 2,
                        ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertNull(socket.responses.poll(100, TimeUnit.MILLISECONDS));
                socket.assertBudgetZero();
            }
            assertTrue(bookie.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(writes, bookieIo(bookie).writes);
        }
    }

    @Test
    public void profileSocketFenceProgressesOnSameListenerWithDataHalfFrameAndFullUnclassifiedSlots() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            ProfileReceiveBudget budget = new ProfileReceiveBudget(409600, 131072, 262144, 1, 2, 1);
            try (ProfileSocket socket = new ProfileSocket(storage, budget)) {
                byte[] full = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.ADD_NORMAL, 2,
                        profileAddBody(context(), 1, (byte) 5, entry(context(), 1, -1, 1024, (byte) 0, 1024))));
                socket.client.writeAndFlush(Unpooled.wrappedBuffer(Arrays.copyOf(full, 37))).sync();
                socket.accepted.get().eventLoop().submit(() -> { }).sync();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (budget.used(ProfileReceiveBudget.Lane.DATA) < full.length + 64) {
                    assertTrue(System.nanoTime() < deadline);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                Channel dataServer = socket.accepted.get();
                // Include actual TLS buffers, then hold backing capacity so the second full body cannot fit.
                ByteBuf pressure = dataServer.eventLoop().submit(() -> dataServer.alloc().heapBuffer(
                        Math.toIntExact(16384 - budget.used(ProfileReceiveBudget.Lane.DATA) - 64 - full.length + 1)))
                        .sync().getNow();
                try {
                    Channel refusedData = socket.peer(true);
                    Channel refusedServer = socket.accepted.get();
                    refusedData.writeAndFlush(Unpooled.wrappedBuffer(Arrays.copyOf(full, 36))).sync();
                    assertTrue("Second DATA body cannot fit the held partial frame's budget",
                            refusedServer.closeFuture().await(5, TimeUnit.SECONDS));
                    Channel control = socket.peer(true); // Same acceptor, port, TLS, decoder and shared Bookie budget.
                    Channel serverControl = socket.accepted.get();
                    byte[] fence = ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5));
                    socket.send(control, ProfileOperation.FENCE_LEDGER, 3, fence);
                    socket.status(ProfileOperation.FENCE_LEDGER, 3, ProfileStatus.StatusClass.OK,
                            ProfileStatus.DurableResult.APPLIED);
                    assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
                    assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                    // A TLS-authenticated connection withholding HELLO occupies the sole unclassified slot.
                    socket.peer(false);
                    assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                    assertThrows(Exception.class, () -> socket.peer(false));
                    socket.send(control, ProfileOperation.FENCE_LEDGER, 4, fence);
                    socket.status(ProfileOperation.FENCE_LEDGER, 4, ProfileStatus.StatusClass.OK,
                            ProfileStatus.DurableResult.ALREADY_APPLIED);
                    fence[80] ^= 1;
                    socket.send(control, ProfileOperation.FENCE_LEDGER, 5, fence);
                    socket.status(ProfileOperation.FENCE_LEDGER, 5, ProfileStatus.StatusClass.UNAUTHORIZED,
                            ProfileStatus.DurableResult.NONE);
                    assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                    // DATA cannot follow on the authenticated control connection even with only its header.
                    control.writeAndFlush(Unpooled.wrappedBuffer(Arrays.copyOf(full, 36))).sync();
                    assertTrue(serverControl.closeFuture().await(5, TimeUnit.SECONDS));
                    assertEquals(0, ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo.writes);
                } finally {
                    pressure.release();
                }
            }
        }
    }

    @Test
    public void profileSocketFenceWaitsForPrecutDataAndRetryUsesSameOperation() throws Exception {
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = new SegmentFileIo(
                    Integer.MAX_VALUE, -1, -1, false, () -> {
                        forcing.countDown();
                        await(release);
                    });
            try (ProfileSocket data = new ProfileSocket(storage); ProfileSocket control = new ProfileSocket(storage)) {
                data.add(2, context(), 1, (byte) 5, entry(context(), 1));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                byte[] fence = ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5));
                control.send(ProfileOperation.FENCE_LEDGER, 3, fence);
                control.status(ProfileOperation.FENCE_LEDGER, 3, ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                        ProfileStatus.DurableResult.NONE);
                data.add(4, context(), 2, (byte) 5, entry(context(), 2));
                data.status(4, ProfileStatus.StatusClass.FENCED, ProfileStatus.DurableResult.NONE);
                release.countDown();
                data.status(2, ProfileStatus.StatusClass.OK, ProfileStatus.DurableResult.APPLIED);
                // The actual owner callback must finish releasing its pre-cut admission before retrying.
                SegmentExecutionGroup execution = (SegmentExecutionGroup) storageField(storage, "execution");
                CountDownLatch done = new CountDownLatch(1);
                assertTrue(execution.owner(0).control(done::countDown));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                control.send(ProfileOperation.FENCE_LEDGER, 5, fence);
                control.status(ProfileOperation.FENCE_LEDGER, 5, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void profileSocketFenceDisconnectRetainsControlCopyAndIoUntilActualTerminal() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            replaceBookieIo(bookie, io);
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.FENCE_LEDGER, 2,
                        ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(1, io.writes);
                assertEquals(0, io.forces);
                assertNull(socket.responses.poll(100, TimeUnit.MILLISECONDS));
                socket.disconnect();
                assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                assertTrue("Submitted control copies must remain charged", socket.budget.used(
                        ProfileReceiveBudget.Lane.CONTROL) > 64);
                assertFalse(storage.close(Duration.ofMillis(5)));
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                assertEquals(1, io.forces);
                socket.assertBudgetZero();
            } finally {
                release.countDown();
            }
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (ProfileSocket socket = new ProfileSocket(restarted)) {
                socket.send(ProfileOperation.FENCE_LEDGER, 3,
                        ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                socket.status(ProfileOperation.FENCE_LEDGER, 3, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.ALREADY_APPLIED);
            }
        }
    }

    @Test
    public void profileSocketFenceForceFailureIsUnknownAndNeverSuccess() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            replaceBookieIo(bookie, io);
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.FENCE_LEDGER, 2,
                        ProfileOperationCodec.encode(ProfileOperation.FENCE_LEDGER, fence(3, (byte) 5)));
                socket.status(ProfileOperation.FENCE_LEDGER, 2, ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN,
                        ProfileStatus.DurableResult.UNKNOWN);
                assertEquals(1, io.writes);
                assertEquals(1, io.forces);
                assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
            }
        }
    }

    @Test
    public void profileSocketRecoveryWritesOnlyUnderExactGrantAndResolvesDuplicateAfterRestart() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6)); // Deliberately no normal ACTIVATE.
            grantStorage(storage);
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 2, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 2, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
                int writes = io.writes;
                assertTrue(writes > 0);
                socket.send(ProfileOperation.ADD_RECOVERY, 3, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 3, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                assertEquals(writes, io.writes);
                socket.send(ProfileOperation.ADD_RECOVERY, 4, recoveryBody(2, (byte) 9, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 4, ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT,
                        ProfileStatus.DurableResult.NONE);
                assertEquals(writes, io.writes);
                socket.add(5, context(), 3, (byte) 6, entry(context(), 3));
                socket.status(5, ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                        ProfileStatus.DurableResult.NONE);
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 2).result()) {
                    assertEquals(0, read.result().record().entry().get(36));
                }
            }
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (ProfileSocket socket = new ProfileSocket(restarted)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 6, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 6, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                assertEquals(0, ((IsolatedSegmentFiles) storageField(restarted, "files")).dataIo.writes);
            }
        }
    }

    @Test
    public void profileSourceAllocatorRefusalRollsBackQuotaAndKeepsPrivateCopyContract() throws Exception {
        SegmentEntryInputs inputs = new SegmentEntryInputs(4096, 1060);
        ByteBuf pooled = PooledByteBufAllocator.DEFAULT.heapBuffer(36, 36);
        try (SegmentEntryInputs.Input copy = inputs.acquire(context(), 36,
                ProfileInboundPipeline.inputCopyAllocator(pooled))) {
            assertNotNull(copy);
            assertTrue(copy.bytes().alloc() instanceof UnpooledByteBufAllocator);
            assertEquals(36, copy.bytes().array().length);
        } finally {
            pooled.release();
        }
        assertEquals(0, inputs.usedBytes());

        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(ProfileSocket.certificate("server-key.pem"));
        conf.setTLSCertificatePath(ProfileSocket.certificate("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(ProfileSocket.certificate("client-cert.pem"));
        TLSContextFactory tls = TLSContextFactory.forProfileServer();
        tls.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        ProfileReceiveBudget budget = new ProfileReceiveBudget(1024, 256, 256, 1, 1, 1);
        EmbeddedChannel channel = new EmbeddedChannel();
        assertTrue(ProfileInboundPipeline.install(channel.pipeline(), tls, budget, 128, 64,
                new ChannelInboundHandlerAdapter()));
        try {
            try (SegmentEntryInputs.Input first = inputs.acquire(context(), 36, channel.alloc())) {
                assertNotNull(first);
                assertEquals(36, inputs.usedBytes());
                assertEquals(100, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                // Source has room, but 64 + 36 + 36 cannot fit this connection's 128-byte limit.
                assertNull(inputs.acquire(context(), 36, channel.alloc()));
                assertEquals(36, inputs.usedBytes());
                assertEquals(100, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            }
            assertEquals(0, inputs.usedBytes());
            assertEquals(64, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
            try (SegmentEntryInputs.Input restored = inputs.acquire(context(), 36, channel.alloc())) {
                assertNotNull(restored);
                channel.finishAndReleaseAll();
                assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                assertEquals(36, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
                assertNull(inputs.acquire(context(), 36, channel.alloc()));
                assertEquals(36, inputs.usedBytes());
            }
        } finally {
            channel.finishAndReleaseAll();
        }
        assertEquals(0, inputs.usedBytes());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void profileSocketSourceCopyRemainsChargedAfterDisconnectUntilOwnerConsumesIt() throws Exception {
        for (ProfileOperation operation : List.of(ProfileOperation.ADD_NORMAL, ProfileOperation.ADD_RECOVERY)) {
            IsolatedSegmentConfig base = config();
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    base.directory().resolveSibling("profile-source-" + operation), 1, 4, 1,
                    4L * 1024 * 1024, 4096, 1024, 8, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            CountDownLatch paused = new CountDownLatch(1);
            CountDownLatch releaseOwner = new CountDownLatch(1);
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch releaseIo = new CountDownLatch(1);
            try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
                storage.startNew();
                if (operation == ProfileOperation.ADD_NORMAL) {
                    installStorage(storage, install(44, 1, (byte) 6), activate(44, 2));
                } else {
                    installStorage(storage, install(44, 1, (byte) 6));
                    grantStorage(storage);
                }
                SegmentExecutionGroup execution = (SegmentExecutionGroup) storageField(storage, "execution");
                SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    forcing.countDown();
                    await(releaseIo);
                });
                ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
                try (ProfileSocket socket = new ProfileSocket(storage)) {
                    assertTrue(execution.owner(0).control(() -> {
                        paused.countDown();
                        await(releaseOwner);
                    }));
                    assertTrue(paused.await(5, TimeUnit.SECONDS));
                    byte[] body = operation == ProfileOperation.ADD_NORMAL
                            ? profileAddBody(context(), 2, (byte) 6,
                                    entry(context(), 2, -1, 1024, (byte) 0, 1024))
                            : recoveryBody(2, (byte) 0, 1024);
                    socket.send(operation, 2, body);
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (execution.inputs().usedBytes() != 1060) {
                        assertTrue("The queued source must be charged", System.nanoTime() < end);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    socket.accepted.get().eventLoop().submit(() -> { }).sync();
                    int workspace = operation == ProfileOperation.ADD_NORMAL ? 144 : 244;
                    assertEquals(64 + 36 + body.length + workspace + 1060,
                            socket.budget.used(ProfileReceiveBudget.Lane.DATA));
                    assertEquals(0, io.writes);
                    socket.disconnect();
                    assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
                    assertEquals(1060, socket.budget.used(ProfileReceiveBudget.Lane.DATA));
                    assertEquals(1060, execution.inputs().usedBytes());
                    releaseOwner.countDown();
                    assertTrue(forcing.await(5, TimeUnit.SECONDS));
                    assertEquals(0, execution.inputs().usedBytes());
                    socket.assertBudgetZero(); // The preallocated aligned pool now owns the copied bytes.
                    assertFalse(storage.close(Duration.ofMillis(50)));
                    assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                    releaseIo.countDown();
                    assertTrue(storage.close(Duration.ofSeconds(5)));
                    assertEquals(1, io.forces);
                    assertNull(socket.responses.poll());
                } finally {
                    releaseOwner.countDown();
                    releaseIo.countDown();
                }
            }
        }
    }

    @Test
    public void profileSocketRecoveryRejectsWrongTupleAndMalformedEntryBeforeDataIo() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                long requestId = 2;
                byte[] body = recoveryBody(2, (byte) 0, 8);
                body[120] ^= 1;
                socket.send(ProfileOperation.ADD_RECOVERY, requestId, body);
                socket.status(ProfileOperation.ADD_RECOVERY, requestId++, ProfileStatus.StatusClass.UNAUTHORIZED,
                        ProfileStatus.DurableResult.NONE);
                body = recoveryBody(2, (byte) 0, 8);
                body[8] ^= 1;
                socket.send(ProfileOperation.ADD_RECOVERY, requestId, body);
                socket.status(ProfileOperation.ADD_RECOVERY, requestId++,
                        ProfileStatus.StatusClass.PROFILE_IDENTITY_CONFLICT, ProfileStatus.DurableResult.NONE);
                for (int offset : new int[] {75, 76, 84, 92, 100}) {
                    body = recoveryBody(2, (byte) 0, 8);
                    if (offset == 75) {
                        body[offset] ^= 1;
                    } else {
                        ByteBuffer.wrap(body).putLong(offset, offset == 92 ? 1 : 99);
                    }
                    socket.send(ProfileOperation.ADD_RECOVERY, requestId, body);
                    socket.status(ProfileOperation.ADD_RECOVERY, requestId++,
                            ProfileStatus.StatusClass.RECOVERY_GRANT_INVALID, ProfileStatus.DurableResult.NONE);
                }
                for (int offset : new int[] {60, 116, 118, 140, 108, 176}) {
                    body = recoveryBody(2, (byte) 0, 8);
                    if (offset == 60) {
                        Arrays.fill(body, 60, 76, (byte) 0);
                    } else if (offset == 108) {
                        ByteBuffer.wrap(body).putLong(offset, 3); // Outer coordinate differs from the CRC-valid entry.
                    } else {
                        body[offset] ^= 1;
                    }
                    socket.send(ProfileOperation.ADD_RECOVERY, requestId, body);
                    socket.status(ProfileOperation.ADD_RECOVERY, requestId++, ProfileStatus.StatusClass.BAD_REQUEST,
                            ProfileStatus.DurableResult.NONE);
                }
                installStorage(storage, tombstone(44, 3));
                socket.send(ProfileOperation.ADD_RECOVERY, requestId, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, requestId,
                        ProfileStatus.StatusClass.TOMBSTONED_OR_DELETED, ProfileStatus.DurableResult.NONE);
                assertEquals(0, io.writes);
                assertEquals(0, io.forces);
                assertEquals(0, ((SegmentExecutionGroup) storageField(storage, "execution")).inputs().usedBytes());
            }
        }
    }

    @Test
    public void profileSocketRecoveryRejectsInstalledLimitBeforeReceivingBody() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                byte[] bytes = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.ADD_RECOVERY, 2,
                        recoveryBody(2, (byte) 0, 1025)));
                socket.client.writeAndFlush(Unpooled.wrappedBuffer(bytes, 0, 36)).sync();
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                socket.assertBudgetZero();
                assertNull(socket.responses.poll());
                assertEquals(0, io.writes);
                assertEquals(0, io.forces);
            }
        }
    }

    @Test
    public void profileSocketRecoverySourcePressureRefusesBeforeCopyAndResumes() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = new IsolatedSegmentStorage(config(), identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(1, 3, 6, 2, 4096, 4096,
                        2 * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 16, 16, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 3, Duration.ofSeconds(30))))) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentExecutionGroup execution = (SegmentExecutionGroup) storageField(storage, "execution");
            SegmentFileIo io = ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                assertTrue(execution.owner(0).control(() -> {
                    blocked.countDown();
                    await(release);
                }));
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                for (int i = 2; i <= 4; i++) {
                    socket.send(ProfileOperation.ADD_RECOVERY, i, recoveryBody(i, (byte) 0, 1024));
                    // Isolate source pressure from a coalesced first TLS plaintext read exhausting UNCLASSIFIED.
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (execution.inputs().usedBytes() != (i - 1) * 1060) {
                        assertTrue("Expected charged source input", System.nanoTime() < end);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                }
                socket.send(ProfileOperation.ADD_RECOVERY, 5, recoveryBody(2, (byte) 0, 1024));
                socket.status(ProfileOperation.ADD_RECOVERY, 5, ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                        ProfileStatus.DurableResult.NONE);
                assertEquals(3 * 1060, execution.inputs().usedBytes());
                assertEquals(0, io.writes);
                release.countDown();
                Set<Long> awaiting = new HashSet<>(Set.of(2L, 3L, 4L));
                for (int i = 0; i < 3; i++) {
                    ProfileFrame reply = socket.response();
                    assertTrue("Each accepted request completes once", awaiting.remove(reply.header().requestId()));
                    socket.status(reply, ProfileOperation.ADD_RECOVERY, ProfileStatus.StatusClass.OK,
                            ProfileStatus.RetryDisposition.NEVER,
                            ProfileStatus.DurableResult.APPLIED);
                }
                assertTrue(awaiting.isEmpty());
                ownerBarrier(execution.owner(0));
                assertEquals(0, execution.inputs().usedBytes());
                int writes = io.writes;
                assertTrue(writes > 0);
                socket.send(ProfileOperation.ADD_RECOVERY, 6, recoveryBody(2, (byte) 0, 1024));
                socket.status(ProfileOperation.ADD_RECOVERY, 6, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                assertEquals(writes, io.writes);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void profileSocketRecoveryGrantCloseAfterWriteReportsStaleUnknown() throws Exception {
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 2, recoveryBody(2, (byte) 0, 8));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(io.writes > 0);
                assertEquals(0, io.forces);
                assertNull(socket.responses.poll(100, TimeUnit.MILLISECONDS));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.WAITING_PRECUT, closeStorageGrant(storage));
                release.countDown();
                socket.status(ProfileOperation.ADD_RECOVERY, 2, ProfileStatus.StatusClass.RECOVERY_GRANT_INVALID,
                        ProfileStatus.RetryDisposition.AFTER_CONTROL_RECONCILIATION,
                        ProfileStatus.DurableResult.UNKNOWN);
                ownerBarrier(((SegmentExecutionGroup) storageField(storage, "execution")).owner(0));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE, closeStorageGrant(storage));
                int writes = io.writes;
                socket.send(ProfileOperation.ADD_RECOVERY, 3, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 3, ProfileStatus.StatusClass.RECOVERY_GRANT_INVALID,
                        ProfileStatus.DurableResult.NONE);
                assertEquals(writes, io.writes);
                socket.add(4, context(), 3, (byte) 6, entry(context(), 3));
                socket.status(4, ProfileStatus.StatusClass.PROFILE_NOT_READY_OR_STALE,
                        ProfileStatus.DurableResult.NONE);
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 2).result()) {
                    assertEquals(2, read.result().record().entryId());
                }
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void profileSocketRecoveryDisconnectKeepsActualIoAndWriterExclusion() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3);
        try {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 2, recoveryBody(2, (byte) 0, 8));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertNull(socket.responses.poll(100, TimeUnit.MILLISECONDS));
                assertFalse(storage.close(Duration.ofMillis(50)));
                socket.disconnect();
                socket.assertBudgetZero();
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                assertEquals(1, io.forces);
                assertNull(socket.responses.poll());
            }
        } finally {
            release.countDown();
            storage.close();
        }
        try (IsolatedSegmentStorage restarted = storage(config, 1, 6, 8, 16, 3)) {
            restarted.restart();
            try (ProfileSocket socket = new ProfileSocket(restarted)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 3, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 3, ProfileStatus.StatusClass.OK,
                        ProfileStatus.DurableResult.APPLIED);
                assertEquals(0, ((IsolatedSegmentFiles) storageField(restarted, "files")).dataIo.writes);
            }
        }
    }

    @Test
    public void profileSocketRecoveryForceFailureReportsUnknown() throws Exception {
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 6));
            grantStorage(storage);
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo = io;
            try (ProfileSocket socket = new ProfileSocket(storage)) {
                socket.send(ProfileOperation.ADD_RECOVERY, 2, recoveryBody(2, (byte) 0, 8));
                socket.status(ProfileOperation.ADD_RECOVERY, 2, ProfileStatus.StatusClass.DURABILITY_RESULT_UNKNOWN,
                        ProfileStatus.DurableResult.UNKNOWN);
                assertTrue(io.writes > 0);
                assertEquals(1, io.forces);
            }
            IOException failed = assertThrows(IOException.class, storage::close);
            assertTrue(failed.getMessage(), failed.getMessage().contains("Failed DATA session must remain unclosed"));
        }
    }

    private static SegmentBookieControlLog.RecoveryCloseStatus closeStorageGrant(IsolatedSegmentStorage storage)
            throws Exception {
        CompletableFuture<SegmentBookieControlLog.RecoveryCloseStatus> ready = new CompletableFuture<>();
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!storage.controls().submitRecoveryClose(recoveryClose(3, 2, 3, 7, 9), (result, cause) -> {
            if (cause == null) {
                ready.complete(result);
            } else {
                ready.completeExceptionally(cause);
            }
        })) {
            assertTrue(System.nanoTime() < end);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        return ready.get(5, TimeUnit.SECONDS);
    }

    private static void grantStorage(IsolatedSegmentStorage storage) throws Exception {
        CompletableFuture<List<SegmentBookieControlLog.Result>> ready = new CompletableFuture<>();
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!storage.controls().submitRecoveryGrant(recoveryGrant(2, 3, 7, 9), (results, cause) -> {
            if (cause == null) {
                ready.complete(results);
            } else {
                ready.completeExceptionally(cause);
            }
        })) {
            assertTrue(System.nanoTime() < end);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                ready.get(5, TimeUnit.SECONDS).get(0).status());
    }

    private static byte[] recoveryBody(long entryId, byte payload, int payloadBytes) throws Exception {
        ByteBuf entry = entry(context(), entryId, -1, payloadBytes, payload, payloadBytes);
        try {
            return ProfileOperationCodec.encode(ProfileOperation.ADD_RECOVERY,
                    recoveryAddWithEntry(entryId, entry.nioBuffer()));
        } finally {
            entry.release();
        }
    }

    private static void replaceBookieIo(SegmentBookieControlLog bookie, SegmentFileIo io) throws Exception {
        var field = SegmentBookieControlLog.class.getDeclaredField("io");
        field.setAccessible(true);
        field.set(bookie, io);
    }

    private static SegmentFileIo bookieIo(SegmentBookieControlLog bookie) throws Exception {
        var field = SegmentBookieControlLog.class.getDeclaredField("io");
        field.setAccessible(true);
        return (SegmentFileIo) field.get(bookie);
    }

    private static ThreadPoolExecutor controlExecutor() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2));
    }

    private static ProfilePrincipal clientPrincipal() throws Exception {
        try (InputStream input = Files.newInputStream(Path.of(ProfileSocket.certificate("client-cert.pem")))) {
            var leaf = (java.security.cert.X509Certificate)
                    java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(input);
            return new ProfilePrincipal(ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                    "test-domain",
                    leaf.getIssuerX500Principal().getName(javax.security.auth.x500.X500Principal.CANONICAL),
                    leaf.getSubjectX500Principal().getName(javax.security.auth.x500.X500Principal.CANONICAL));
        }
    }

    private static SegmentProfileControlConfig initialConfig(ThreadPoolExecutor executor, Duration timeout,
            java.util.concurrent.atomic.AtomicBoolean authorized, java.util.concurrent.atomic.AtomicInteger reads)
            throws Exception {
        ProfilePrincipal expected = clientPrincipal();
        return new SegmentProfileControlConfig(VALIDATOR, (principal, operation) -> principal.equals(expected),
                key -> {
                    reads.incrementAndGet();
                    var transition = key.equals(ProfileAuthorityKey.fromScope(install(1, (byte) 5).request().scope()))
                            ? install(1, (byte) 5) : activate(2);
                    return ProfileControlStore.ReadResult.found(transition.authority());
                }, (principal, requested, authority) -> principal.equals(expected) && authorized.get()
                        && requested.equals(authority.scope())
                        ? org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision.ALLOW
                        : org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision.DENY,
                executor, timeout);
    }

    private static ProfileReceiveBudget initialBudget() {
        return new ProfileReceiveBudget(524288, 131072, 262144, 2, 2, 1);
    }

    private static ProfileControlResult initialResponse(ProfileSocket socket, ProfileOperation operation,
                                                       long requestId) throws Exception {
        ProfileFrame response = socket.response();
        assertEquals(operation, response.header().operation());
        assertEquals(requestId, response.header().requestId());
        assertTrue(response.header().isResponse());
        return ProfileInitialControlCodec.decodeReply(operation, response.body());
    }

    @Test
    public void profileSocketInitialControlUsesActualTlsAndPhysicalDriver() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean allowed = new AtomicBoolean(true);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            var control = initialConfig(executor, Duration.ofSeconds(5), allowed, reads);
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                byte[] body = ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request());
                socket.send(ProfileOperation.INSTALL, 2, body);
                ProfileControlResult installed = initialResponse(socket, ProfileOperation.INSTALL, 2);
                assertEquals(ProfileControlResult.Outcome.APPLIED, installed.outcome());
                assertTrue(installed.durable());
                assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                socket.send(ProfileOperation.INSTALL, 3, body);
                ProfileControlResult repeated = initialResponse(socket, ProfileOperation.INSTALL, 3);
                assertEquals(ProfileControlResult.Outcome.ALREADY_APPLIED, repeated.outcome());
                assertEquals(installed.localGeneration(), repeated.localGeneration());
                socket.send(ProfileOperation.ACTIVATE_INITIAL, 4,
                        ProfileInitialControlCodec.encodeRequest(activate(2).request()));
                assertEquals(ProfileControlResult.Outcome.APPLIED,
                        initialResponse(socket, ProfileOperation.ACTIVATE_INITIAL, 4).outcome());
                assertTrue(bookie.normalAdmission(context(), filled(20, (byte) 5)));
                int writes = bookieIo(bookie).writes;
                allowed.set(false);
                socket.send(ProfileOperation.ACTIVATE_INITIAL, 5,
                        ProfileInitialControlCodec.encodeRequest(activate(2).request()));
                assertEquals(ProfileControlResult.Outcome.DENIED,
                        initialResponse(socket, ProfileOperation.ACTIVATE_INITIAL, 5).outcome());
                assertEquals(writes, bookieIo(bookie).writes);
                assertEquals(4, reads.get());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void profileSocketInitialControlRejectsMissingCapabilityAndUnauthorizedFirstRequest() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        AtomicInteger reads = new AtomicInteger();
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            var control = initialConfig(executor, Duration.ofSeconds(5), new AtomicBoolean(false), reads);
            int writes = bookieIo(bookie).writes;
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, false)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                socket.status(ProfileOperation.INSTALL, 2,
                        ProfileStatus.StatusClass.UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE,
                        ProfileStatus.DurableResult.NONE);
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertEquals(0, reads.get());
            }
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                assertEquals(ProfileControlResult.Outcome.DENIED,
                        initialResponse(socket, ProfileOperation.INSTALL, 2).outcome());
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
            }
            assertEquals(writes, bookieIo(bookie).writes);
            assertFalse(bookie.normalAdmission(context(), filled(20, (byte) 5)));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void profileSocketInitialUnknownThenDisconnectRetainsCopiesUntilActualIoTerminal() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            replaceBookieIo(bookie, io);
            var control = initialConfig(executor, Duration.ofMillis(100), new AtomicBoolean(true), new AtomicInteger());
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertEquals(0, io.forces);
                assertEquals(ProfileControlResult.Outcome.DURABILITY_UNKNOWN,
                        initialResponse(socket, ProfileOperation.INSTALL, 2).outcome());
                socket.disconnect();
                assertTrue(socket.budget.used(ProfileReceiveBudget.Lane.CONTROL) > 64);
                assertFalse(storage.close(Duration.ofMillis(5)));
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(
                        ((IsolatedSegmentConfig) storageField(storage, "config")).directory()));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                socket.assertBudgetZero();
                assertEquals(1, io.forces);
            } finally {
                release.countDown();
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void profileSocketInitialInstallReadsActualZkPreparingRoot() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        ZooKeeperUtil cluster = new ZooKeeperUtil() {
            @Override
            public String getZooKeeperConnectString() {
                String address = super.getZooKeeperConnectString();
                return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
            }
        };
        ZooKeeper zk = null;
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            cluster.startCluster();
            ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(10000, false);
            zk = new ZooKeeper(cluster.getZooKeeperConnectString(), 10000, watcher);
            watcher.waitForConnection();
            String root = "/profile-socket-initial";
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            var reservations = new ZKProfileReservationStore(zk, root, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    Duration.ofSeconds(5));
            assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED, reservations.createIfAbsent(
                    new ProfileReservation(new ProfileMetadataBacklink(44, filled(16, (byte) 1),
                            filled(16, (byte) 2)), List.of(install(1, (byte) 5).request().scope()))).status());
            ProfilePrincipal expected = clientPrincipal();
            var control = new SegmentProfileControlConfig(VALIDATOR,
                    (principal, operation) -> principal.equals(expected),
                    reservations.installAuthorityReader(identity().bookieId().toString()),
                    (principal, requested, authority) -> principal.equals(expected)
                            && requested.equals(authority.scope())
                            ? org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision.ALLOW
                            : org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer.Decision.DENY,
                    executor, Duration.ofSeconds(5));
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                assertEquals(ProfileControlResult.Outcome.APPLIED,
                        initialResponse(socket, ProfileOperation.INSTALL, 2).outcome());
                assertEquals(ZKProfileReservationStore.CreateStatus.APPLIED,
                        reservations.cutDelete(reservations.read(44).snapshot(), filled(16, (byte) 9),
                                filled(32, (byte) 10)).status());
                int writes = bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes;
                socket.send(ProfileOperation.INSTALL, 3,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                assertEquals(ProfileControlResult.Outcome.NOT_FOUND,
                        initialResponse(socket, ProfileOperation.INSTALL, 3).outcome());
                assertEquals(writes, bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes);
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            if (zk != null) {
                zk.close();
            }
            cluster.stopCluster();
        }
    }

    @Test
    public void profileSocketInitialLateEventLoopAuthorizationAfterColdTimeoutHasNoEffect() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch releaseLoop = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            var base = initialConfig(executor, Duration.ofMillis(100), new AtomicBoolean(true), new AtomicInteger());
            var control = new SegmentProfileControlConfig(base.validator(), base.staticAuthorizer(), key -> {
                reading.countDown();
                await(releaseRead);
                return base.controlStore().read(key);
            }, base.exactAuthorizer(), executor, base.timeout());
            int writes = bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes;
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                assertTrue(reading.await(5, TimeUnit.SECONDS));
                socket.accepted.get().eventLoop().execute(() -> {
                    blocked.countDown();
                    await(releaseLoop);
                });
                assertTrue(blocked.await(5, TimeUnit.SECONDS));
                releaseRead.countDown();
                executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
                assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
                releaseLoop.countDown();
                assertEquals(ProfileControlResult.Outcome.TRANSIENT_RECONCILING,
                        initialResponse(socket, ProfileOperation.INSTALL, 2).outcome());
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertEquals(writes, bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes);
            }
        } finally {
            releaseRead.countDown();
            releaseLoop.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void profileSocketInitialOversizeHeaderRefusesBeforeBodyAndColdRead() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        AtomicInteger reads = new AtomicInteger();
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            var control = initialConfig(executor, Duration.ofSeconds(5), new AtomicBoolean(true), reads);
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                byte[] frame = ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.INSTALL, 2,
                        new byte[ProfileInitialControlCodec.MAX_INSTALL_BYTES + 1]));
                socket.client.writeAndFlush(Unpooled.wrappedBuffer(frame, 0,
                        ProfileFrameCodec.OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH)).sync();
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertEquals(0, reads.get());
                assertNull(socket.responses.poll());
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void profileSocketInitialColdQueueExhaustionDoesNotRunOnEventLoopOrWriteControl() throws Exception {
        ThreadPoolExecutor executor = controlExecutor();
        CountDownLatch busy = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        executor.execute(() -> {
            busy.countDown();
            await(release);
        });
        assertTrue(busy.await(5, TimeUnit.SECONDS));
        executor.execute(() -> { });
        executor.execute(() -> { });
        try (IsolatedSegmentStorage storage = storage(config(), 1, 6, 8, 16, 3)) {
            storage.startNew();
            var control = initialConfig(executor, Duration.ofSeconds(5), new AtomicBoolean(true), reads);
            int writes = bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes;
            try (ProfileSocket socket = new ProfileSocket(storage, initialBudget(), control, true)) {
                socket.send(ProfileOperation.INSTALL, 2,
                        ProfileInitialControlCodec.encodeRequest(install(1, (byte) 5).request()));
                socket.status(ProfileOperation.INSTALL, 2, ProfileStatus.StatusClass.TRANSIENT_UNAVAILABLE,
                        ProfileStatus.DurableResult.NONE);
                assertTrue(socket.inactive.await(5, TimeUnit.SECONDS));
                assertEquals(0, reads.get());
                assertEquals(writes, bookieIo((SegmentBookieControlLog) storageField(storage, "bookie")).writes);
                assertEquals(0, socket.budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
            }
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private final class ProfileSocket implements AutoCloseable {
        private final EventLoopGroup acceptor = new NioEventLoopGroup(1);
        private final EventLoopGroup worker = new NioEventLoopGroup(1);
        private final EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        private final ProfileReceiveBudget budget;
        private final List<Channel> acceptedChannels = new CopyOnWriteArrayList<>();
        private final List<Channel> peers = new ArrayList<>();
        private Bootstrap clientBootstrap;
        private SslContext peerTls;
        private final ArrayBlockingQueue<ProfileFrame> responses = new ArrayBlockingQueue<>(16);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicReference<Channel> accepted = new AtomicReference<>();
        private final CountDownLatch inactive = new CountDownLatch(1);
        private Channel listener;
        private Channel client;

        private ProfileSocket(IsolatedSegmentStorage storage) throws Exception {
            this(storage, new ProfileReceiveBudget(524288, 131072, 262144, 2, 2, 1));
        }

        private ProfileSocket(IsolatedSegmentStorage storage, ProfileReceiveBudget budget) throws Exception {
            this(storage, budget, null, false);
        }

        private ProfileSocket(IsolatedSegmentStorage storage, ProfileReceiveBudget budget,
                              SegmentProfileControlConfig control, boolean offerControl) throws Exception {
            this.budget = budget;
            try {
                ServerConfiguration conf = new ServerConfiguration();
                conf.setTLSKeyStoreType("PEM");
                conf.setTLSKeyStore(certificate("server-key.pem"));
                conf.setTLSCertificatePath(certificate("server-san-cert.pem"));
                conf.setTLSTrustStoreType("PEM");
                conf.setTLSTrustStore(certificate("client-cert.pem"));
                TLSContextFactory serverTls = TLSContextFactory.forProfileServer();
                serverTls.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
                SslContext clientTls = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                        .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                        .trustManager(new File(certificate("server-san-cert.pem"))).protocols("TLSv1.3").build();
                listener = new ServerBootstrap().group(acceptor, worker).channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) throws Exception {
                                accepted.set(channel);
                                acceptedChannels.add(channel);
                                if (!storage.installProfileConnection(channel.pipeline(), serverTls, budget,
                                        131072, 64, "test-domain", 0, control)) {
                                    return;
                                }
                                channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelInactive(ChannelHandlerContext ctx) {
                                        inactive.countDown();
                                    }
                                });
                            }
                        }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
                int port = ((InetSocketAddress) listener.localAddress()).getPort();
                peerTls = clientTls;
                clientBootstrap = new Bootstrap().group(clientGroup).channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) {
                                SslHandler ssl = clientTls.newHandler(channel.alloc(), "apache.bookkeeper.org", port);
                                javax.net.ssl.SSLParameters parameters = ssl.engine().getSSLParameters();
                                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                                ssl.engine().setSSLParameters(parameters);
                                channel.pipeline().addLast(ssl);
                                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(8192, 0, 4, 0, 0));
                                channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                    private boolean first = true;

                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf response) {
                                        byte[] bytes = new byte[response.readableBytes()];
                                        response.readBytes(bytes);
                                        ProfileFrame frame = ProfileFrameCodec.decode(bytes, first
                                                ? ProfileFrameCodec.DecodePhase.PRE_HELLO
                                                : ProfileFrameCodec.DecodePhase.ESTABLISHED);
                                        first = false;
                                        assertTrue(responses.offer(frame));
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        failure.set(cause);
                                        ctx.close();
                                    }
                                });
                            }
                        });
                client = clientBootstrap.connect("127.0.0.1", port).sync().channel();
                client.pipeline().get(SslHandler.class).handshakeFuture().sync();
                send(ProfileOperation.HELLO, 1, new ProfileHello.Client(offerControl ? List.of(
                        new ProfileHello.Capability(ProfileInitialControlCodec.CAPABILITY_ID, 1))
                        : List.of()).encode());
                ProfileFrame frame = response();
                assertEquals(ProfileOperation.HELLO, frame.header().operation());
                assertEquals(1, frame.header().requestId());
                assertTrue(frame.header().isResponse());
                assertFalse(frame.header().isError());
                ProfileHello.Server hello = ProfileHello.decodeServer(frame.body());
                assertEquals(identity().bookieId().toString(), hello.bookieId());
                assertArrayEquals(identity().incarnation().value(), hello.storageIncarnation());
                assertEquals(EngineProfile.SEGMENT_WAL_ENGINE.wireValue(), hello.activeEngine());
                assertEquals(0, hello.readinessGeneration()); // Explicitly no actual startup-readiness claim.
                assertEquals(control == null ? List.of() : List.of(
                        new ProfileHello.Capability(ProfileInitialControlCodec.CAPABILITY_ID, 1),
                        new ProfileHello.Capability(ProfileInitialControlCodec.REPLACEMENT_CAPABILITY_ID,
                                ProfileInitialControlCodec.REPLACEMENT_SCHEMA)),
                        hello.capabilities());
            } catch (Exception | Error cause) {
                close();
                throw cause;
            }
        }

        private static String certificate(String name) throws Exception {
            return Paths.get(SegmentBookieControlLogTest.class.getClassLoader().getResource(name).toURI()).toString();
        }

        private Channel peer(boolean hello) throws Exception {
            int port = ((InetSocketAddress) listener.localAddress()).getPort();
            Bootstrap bootstrap = hello ? clientBootstrap.clone()
                    : new Bootstrap().group(clientGroup).channel(NioSocketChannel.class)
                            .handler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel channel) {
                                    channel.pipeline().addLast(peerTls.newHandler(
                                            channel.alloc(), "apache.bookkeeper.org", port));
                                }
                            });
            Channel peer = bootstrap.connect("127.0.0.1", port).sync().channel();
            peers.add(peer);
            peer.pipeline().get(SslHandler.class).handshakeFuture().sync();
            if (hello) {
                send(peer, ProfileOperation.HELLO, 1, new ProfileHello.Client(List.of()).encode());
                ProfileFrame greeting = response();
                assertEquals(ProfileOperation.HELLO, greeting.header().operation());
                assertFalse(greeting.header().isError());
            }
            return peer;
        }

        private void send(ProfileOperation operation, long requestId, byte[] body) throws Exception {
            send(client, operation, requestId, body);
        }

        private void send(Channel channel, ProfileOperation operation, long requestId, byte[] body) throws Exception {
            channel.writeAndFlush(Unpooled.wrappedBuffer(ProfileFrameCodec.encode(
                    ProfileFrame.request(operation, requestId, body)))).sync();
        }

        private void add(long requestId, ProfileLedgerContext context, long entryId, byte verifier, ByteBuf entry)
                throws Exception {
            send(ProfileOperation.ADD_NORMAL, requestId, profileAddBody(context, entryId, verifier, entry));
        }

        private ProfileFrame response() throws Exception {
            ProfileFrame response = responses.poll(5, TimeUnit.SECONDS);
            assertNull(failure.get());
            assertNotNull("Expected a real wire response", response);
            return response;
        }

        private ProfileStatus status(long requestId, ProfileStatus.StatusClass status,
                                     ProfileStatus.DurableResult durable) throws Exception {
            return status(ProfileOperation.ADD_NORMAL, requestId, status, durable);
        }

        private ProfileStatus status(ProfileOperation operation, long requestId, ProfileStatus.StatusClass status,
                                     ProfileStatus.DurableResult durable) throws Exception {
            ProfileStatus.RetryDisposition retry = switch (status) {
                case PROFILE_NOT_READY_OR_STALE -> ProfileStatus.RetryDisposition.AFTER_CONTROL_RECONCILIATION;
                case TRANSIENT_UNAVAILABLE, DURABILITY_RESULT_UNKNOWN ->
                        ProfileStatus.RetryDisposition.SAME_PROFILE_OPERATION;
                default -> ProfileStatus.RetryDisposition.NEVER;
            };
            return status(operation, requestId, status, retry, durable);
        }

        private ProfileStatus status(ProfileOperation operation, long requestId, ProfileStatus.StatusClass status,
                                     ProfileStatus.RetryDisposition retry, ProfileStatus.DurableResult durable)
                throws Exception {
            ProfileFrame frame = response();
            assertEquals(requestId, frame.header().requestId());
            return status(frame, operation, status, retry, durable);
        }

        private ProfileStatus status(ProfileFrame frame, ProfileOperation operation, ProfileStatus.StatusClass status,
                                     ProfileStatus.RetryDisposition retry, ProfileStatus.DurableResult durable) {
            assertEquals(operation, frame.header().operation());
            assertTrue(frame.header().isResponse());
            assertEquals(ProfileStatus.ENCODED_LENGTH, frame.body().length);
            ProfileStatus result = ProfileStatus.decodePrefix(ByteBuffer.wrap(frame.body()));
            assertEquals(status, result.statusClass());
            assertEquals(status != ProfileStatus.StatusClass.OK, frame.header().isError());
            assertEquals(durable, result.durableResult());
            assertEquals(retry, result.retryDisposition());
            assertEquals(0, result.detailCode());
            return result;
        }

        private void disconnect() throws Exception {
            client.close().sync();
            assertTrue(inactive.await(5, TimeUnit.SECONDS));
        }

        private void assertBudgetZero() {
            for (ProfileReceiveBudget.Lane lane : ProfileReceiveBudget.Lane.values()) {
                assertEquals(0, budget.used(lane));
                assertEquals(0, budget.usedConnections(lane));
            }
        }

        @Override
        public void close() {
            if (client != null) {
                client.close().syncUninterruptibly();
            }
            for (Channel peer : peers) {
                peer.close().syncUninterruptibly();
            }
            for (Channel channel : acceptedChannels) {
                channel.close().syncUninterruptibly();
            }
            if (listener != null) {
                listener.close().syncUninterruptibly();
            }
            clientGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            worker.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            acceptor.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            assertBudgetZero();
        }
    }

    @Test
    public void restartTakesDataExclusionBeforeOpeningAnyControlOwner() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            storageWrite(storage, context(), 1);
        }
        // These fixture lock files are unused after the healthy close; their recreation observes control open.
        Files.delete(config.directory().resolve("arena-control.lock"));
        Files.delete(config.directory().resolve("bookie-control.lock"));
        Path held = temporary.getRoot().toPath().resolve("startup-data-held");
        Path output = temporary.getRoot().toPath().resolve("startup-lock-child.log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), SegmentBookieControlLogTest.class.getName(),
                "hold-data-lock", config.directory().toString(), held.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(held) && child.isAlive() && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertTrue(Files.readString(output), Files.exists(held) && child.isAlive());
            try (IsolatedSegmentStorage refused = storage(config, 1, 6, 8, 16, 3)) {
                assertThrows(IOException.class, refused::restart);
            }
            assertFalse("Arena recovery started without DATA exclusion",
                    Files.exists(config.directory().resolve("arena-control.lock")));
            assertFalse("Bookie recovery started without DATA exclusion",
                    Files.exists(config.directory().resolve("bookie-control.lock")));
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
        } finally {
            child.getOutputStream().close();
            if (!child.waitFor(5, TimeUnit.SECONDS)) {
                child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
            }
        }
        assertEquals(Files.readString(output), 0, child.exitValue());
        try (IsolatedSegmentStorage reopened = storage(config, 1, 6, 8, 16, 3)) {
            reopened.restart();
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
            try (SegmentPointRead.LeasedResult read = storageRead(reopened, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
        try (SegmentDataFileLock available = SegmentDataFileLock.open(config.directory())) {
            assertTrue(available.lock().isValid());
        }
    }

    @Test
    public void restartFailureReleasesOnlyItsOwnDataExclusionBeforeAndAfterFileHandoff() throws Exception {
        IsolatedSegmentConfig config = config();
        long payloadOffset;
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 8, 16, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            payloadOffset = storageWrite(storage, context(), 1).candidate().recordOffset()
                    + SegmentBatchFormat.RECORD_HEADER + 36;
        }
        for (String name : List.of("arena-control.lock", "bookie-control.lock")) {
            Path controlPath = config.directory().resolve(name);
            try (SegmentDataFileLock foreign = SegmentDataFileLock.openControl(controlPath);
                    IsolatedSegmentStorage refused = storage(config, 1, 6, 8, 16, 3)) {
                assertThrows(IOException.class, refused::restart);
                try (SegmentDataFileLock available = SegmentDataFileLock.open(config.directory())) {
                    assertTrue(available.lock().isValid());
                }
                assertTrue(foreign.lock().isValid());
                assertThrows(IOException.class, () -> SegmentDataFileLock.openControl(controlPath));
            }
        }
        Path data = IsolatedSegmentFiles.dataPath(config, 0);
        ByteBuffer original = ByteBuffer.allocate(1);
        try (FileChannel file = FileChannel.open(data, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            SegmentFileIo.readFully(file, original, payloadOffset);
            file.write(ByteBuffer.wrap(new byte[] {(byte) (original.get(0) ^ 1)}), payloadOffset);
            file.force(false);
        }
        try (IsolatedSegmentStorage corrupt = storage(config, 1, 6, 8, 16, 3)) {
            assertThrows(IOException.class, corrupt::restart);
            try (SegmentDataFileLock available = SegmentDataFileLock.open(config.directory())) {
                assertTrue(available.lock().isValid());
            }
        } finally {
            try (FileChannel file = FileChannel.open(data, StandardOpenOption.WRITE)) {
                file.write(original, payloadOffset);
                file.force(false);
            }
        }
        try (IsolatedSegmentStorage recovered = storage(config, 1, 6, 8, 16, 3)) {
            recovered.restart();
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
            try (SegmentPointRead.LeasedResult read = storageRead(recovered, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
    }

    @Test
    public void realRegistrationRetirementRefusesNewDataButDrainsAlreadyAdmittedStorageWork() throws Exception {
        ZooKeeperUtil cluster = new ZooKeeperUtil() {
            @Override
            public String getZooKeeperConnectString() {
                String address = super.getZooKeeperConnectString();
                return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
            }
        };
        ZooKeeper zk = null;
        IsolatedSegmentStorage storage = null;
        CountDownLatch forceHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        IsolatedSegmentConfig config = config();
        try {
            cluster.startCluster();
            ZooKeeperWatcherBase watcherManager = new ZooKeeperWatcherBase(10000, false);
            String address = cluster.getZooKeeperConnectString();
            zk = new ZooKeeper("127.0.0.1" + address.substring(address.lastIndexOf(':')), 10000, watcherManager);
            watcherManager.waitForConnection();
            String root = "/segment-registration-test";
            String bookieId = install(44, 1, (byte) 5).request().scope().targetBookie();
            zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ZKProfileRegistrationStore persistent = new ZKProfileRegistrationStore(zk, root,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            ProfileReadinessRecord record = new ProfileReadinessRecord(bookieId, identity().incarnation(),
                    EngineProfile.SEGMENT_WAL_ENGINE, 1, 0, new Generation(1), new Generation(1),
                    new ManifestIdentity(filled(32, (byte) 2)), new Generation(1), new Generation(1),
                    new Generation(1), 1, 1, List.of(11, 13), ProfileReadinessRecord.State.READY);
            assertEquals(CasStatus.APPLIED,
                    persistent.compareAndSet(bookieId, -1, 0, record).status());
            var authority = persistent.read(bookieId).versionedRecord().orElseThrow();
            ProfileServiceInfoProjection projection = new ProfileServiceInfoProjection(bookieId,
                    identity().incarnation(), new Generation(1), authority.storeVersion(),
                    "isolated-profile", List.of(11, 13));
            ZKProfileEphemeralRegistrationStore publisher = new ZKProfileEphemeralRegistrationStore(zk, root,
                    bookieId, ZooDefs.Ids.OPEN_ACL_UNSAFE, Duration.ofSeconds(5));
            var admission = publisher.watchAdmission(watcherManager);
            storage = storage(config, 1, 6, 4, 16, 3);
            storage.bindProfileRegistration(admission);
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            IsolatedSegmentStorage graph = storage;
            assertThrows(IllegalStateException.class, () -> graph.bindProfileRegistration(admission));
            ByteBuf input = entry(context(), 1);
            try {
                assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), input,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) ->
                                fail("Pending gate admitted DATA"))));
                assertEquals(1, input.refCnt());
            } finally {
                input.release();
            }
            assertEquals(PublicationStatus.APPLIED, publisher.publishServiceInfo(projection).status());
            var writable = new WritableRegistration(bookieId, identity().incarnation(), new Generation(1),
                    authority.storeVersion());
            assertEquals(PublicationStatus.APPLIED, publisher.registerWritable(writable).status());
            assertTrue(admission.openAfterReconciliation(writable));
            // Existing real DATA I/O seam: hold after write, before the covering native force.
            ((IsolatedSegmentFiles) storageField(storage, "files")).dataIo =
                    new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                        forceHeld.countDown();
                        await(release);
                    });
            CompletableFuture<SegmentDataPhysicalDriver.NormalResult> completed = new CompletableFuture<>();
            input = entry(context(), 1);
            try {
                assertTrue(storage.submitNormal(context(), filled(20, (byte) 5), input,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            if (cause == null) {
                                completed.complete(result);
                            } else {
                                completed.completeExceptionally(cause);
                            }
                        })));
            } finally {
                input.release();
            }
            assertTrue(forceHeld.await(5, TimeUnit.SECONDS));
            zk.close();
            assertFalse(admission.getAsBoolean());
            assertFalse(storage.submitRecovery(recoveryAddWithEntry(entryBytes(context(), 2, -1, 8, (byte) 0)),
                    new SegmentDataPhysicalDriver.RecoveryRequest((result, cause) ->
                            fail("Retired gate admitted DATA"))));
            input = entry(context(), 2);
            try {
                assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), input,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) ->
                            fail("Retired gate admitted DATA"))));
                assertEquals(1, input.refCnt());
            } finally {
                input.release();
            }
            SegmentExecutionGroup execution = (SegmentExecutionGroup) storageField(storage, "execution");
            assertEquals(0, execution.inputs().usedBytes()); // Sources were copied; the native I/O owns the pool lease.
            assertFalse(execution.owner(0).hasMaintenanceBuffers(2));
            assertFalse(completed.isDone());
            assertFalse(storage.close(Duration.ofMillis(50)));
            assertFalse(execution.owner(0).hasMaintenanceBuffers(2));
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
            release.countDown();
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                    completed.get(5, TimeUnit.SECONDS).status());
            assertTrue(storage.close(Duration.ofSeconds(5)));
            assertEquals(0, execution.inputs().usedBytes());
            try (IsolatedSegmentStorage reopened = storage(config, 1, 6, 4, 16, 3)) {
                reopened.restart();
                try (SegmentPointRead.LeasedResult read = storageRead(reopened, context(), 1).result()) {
                    assertEquals(1, read.result().record().entryId());
                }
            }
        } finally {
            release.countDown();
            try {
                if (storage != null) {
                    storage.close();
                }
            } finally {
                try {
                    if (zk != null) {
                        zk.close();
                    }
                } finally {
                    cluster.stopCluster();
                }
            }
        }
    }

    private static SegmentDataPhysicalDriver.NormalResult storageWriteReply(IsolatedSegmentStorage storage,
            ProfileLedgerContext context, long entryId) throws Exception {
        return storageWriteReply(storage, context, entryId, entry(context, entryId));
    }

    private static SegmentDataPhysicalDriver.NormalResult storageWriteReply(IsolatedSegmentStorage storage,
            ProfileLedgerContext context, long entryId, ByteBuf bytes) throws Exception {
        CompletableFuture<SegmentDataPhysicalDriver.NormalResult> reply = new CompletableFuture<>();
        try {
            assertTrue(storage.submitNormal(context, filled(20, (byte) 5), bytes,
                    new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                        if (cause == null) {
                            reply.complete(result);
                        } else {
                            reply.completeExceptionally(cause);
                        }
                    })));
            assertEquals(1, bytes.refCnt());
        } finally {
            bytes.release();
        }
        return reply.get(5, TimeUnit.SECONDS);
    }

    private IsolatedSegmentStorage maintenanceStorage(IsolatedSegmentConfig config, int maintenanceBlocks) {
        return new IsolatedSegmentStorage(config, identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(1, 3, 6, 2, 65536, 65536,
                        2 * (65536L + 4095) + 65536, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 4096, 4096, 1, maintenanceBlocks,
                        new SegmentDataPhysicalDriver.BatchLimits(65536, 1, Duration.ofSeconds(30))));
    }

    private static SegmentOwnerLoop storageOwner(IsolatedSegmentStorage storage, int shard) throws Exception {
        var field = IsolatedSegmentStorage.class.getDeclaredField("execution");
        field.setAccessible(true);
        return ((SegmentExecutionGroup) field.get(storage)).owner(shard);
    }

    private IsolatedSegmentStorage rotatingStorage(IsolatedSegmentConfig config, int workers) {
        return rotatingStorage(config, workers, 32768);
    }

    private IsolatedSegmentStorage rotatingStorage(IsolatedSegmentConfig config, int workers, long controlBytes) {
        int buffers = workers * 2;
        return new IsolatedSegmentStorage(config, identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(workers, 3, 8, buffers, 4096, 4096,
                        buffers * (4096L + 4095) + 4096, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(controlBytes, 16, 32, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))));
    }

    private static Object storageField(IsolatedSegmentStorage storage, String name) throws Exception {
        var field = IsolatedSegmentStorage.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(storage);
    }

    private SegmentBatchFormat.Address storageWriteAfterControlRotation(IsolatedSegmentStorage storage, long entryId)
            throws Exception {
        CompletableFuture<SegmentDataPhysicalDriver.NormalResult> reply = new CompletableFuture<>();
        ByteBuf bytes = entry(context(), entryId);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            boolean accepted = false;
            while (!accepted && System.nanoTime() < deadline) {
                accepted = storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            if (cause == null) {
                                reply.complete(result);
                            } else {
                                reply.completeExceptionally(cause);
                            }
                        }));
                if (!accepted) {
                    assertEquals(1, bytes.refCnt());
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
            }
            assertTrue("Control capacity must recover without an explicit rotation", accepted);
            SegmentDataPhysicalDriver.NormalResult result = reply.get(5, TimeUnit.SECONDS);
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
            return result.candidate().address();
        } finally {
            bytes.release();
        }
    }

    private void exhaustGraphControlLogs(IsolatedSegmentStorage storage, long allocationId) throws Exception {
        SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
        SegmentArenaControlDriver allocator = (SegmentArenaControlDriver) storageField(storage, "allocator");
        SegmentOwnerLoop owner = storageOwner(storage, 0);
        boolean full = false;
        for (int i = 0; i < 200; i++) {
            CompletableFuture<Throwable> reply = new CompletableFuture<>();
            assertTrue(storage.controls().submitBatch(List.of(install(44, 1, (byte) 5)), (results, cause) -> {
                if (cause == null) {
                    assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE, results.get(0).status());
                }
                reply.complete(cause);
            }));
            Throwable cause = reply.get(5, TimeUnit.SECONDS);
            ownerBarrier(owner);
            if (cause != null) {
                assertTrue(cause.toString(), cause instanceof IOException);
                assertTrue(cause.toString(), cause.getMessage().contains("control log capacity exhausted"));
                full = true;
                break;
            }
        }
        assertTrue("Bookie ordinary capacity must be exhausted", full);
        full = false;
        SegmentArenaControlLog.Allocation allocation = arena.currentAllocation(allocationId);
        for (int i = 0; i < 200; i++) {
            CompletableFuture<Throwable> reply = new CompletableFuture<>();
            assertTrue(allocator.submit(List.of(allocation), (results, cause) -> {
                if (cause == null) {
                    assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE, results.get(0).status());
                }
                reply.complete(cause);
            }));
            Throwable cause = reply.get(5, TimeUnit.SECONDS);
            ownerBarrier(owner);
            if (cause != null) {
                assertTrue(cause.toString(), cause instanceof IOException);
                assertTrue(cause.toString(), cause.getMessage().contains("control log capacity exhausted"));
                full = true;
                break;
            }
        }
        assertTrue("Arena ordinary capacity must be exhausted", full);
    }

    private SegmentBatchFormat.Address fullClosedControlGraph(IsolatedSegmentConfig config, int workers)
            throws Exception {
        return fullClosedControlGraph(config, workers, 32768);
    }

    private SegmentBatchFormat.Address fullClosedControlGraph(IsolatedSegmentConfig config, int workers,
                                                               long controlBytes) throws Exception {
        SegmentBatchFormat.Address first;
        try (IsolatedSegmentStorage storage = rotatingStorage(config, workers, controlBytes)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            first = storageWrite(storage, context(), 1).candidate().address();
            ownerBarrier(storageOwner(storage, 0));
            try (IsolatedSegmentFiles.DataReadLease pin =
                         ((IsolatedSegmentFiles) storageField(storage, "files")).pinDataRead()) {
                exhaustGraphControlLogs(storage, first.allocation());
                // Stop the actual owner pump while pinned, then drain once and attest a healthy CLOSE.
                ((SegmentExecutionGroup) storageField(storage, "execution")).stopAdmission();
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), controlBytes)) {
            assertFalse(arena.hasUnclosedDataSession());
            assertEquals(1, arena.dataSessionEpoch());
            assertTrue(controlBytes - Files.size(config.directory().resolve("arena-control")) < 736);
            assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
        }
        return first;
    }

    @Test
    public void fullClosedControlLogsRestartBeforeNextOpenAndPreserveRecoveredData() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("full-closed-restart-" + workers);
            SegmentBatchFormat.Address first = fullClosedControlGraph(config, workers);
            byte[] bookieBefore = Files.readAllBytes(config.directory().resolve("bookie-control"));
            try (IsolatedSegmentStorage storage = rotatingStorage(config, workers)) {
                storage.restart();
                SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
                assertEquals(2, arena.dataSessionEpoch());
                assertTrue(arena.hasUnclosedDataSession());
                assertTrue(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
                assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE, 2)));
                assertArrayEquals(bookieBefore, Files.readAllBytes(config.directory().resolve("bookie-control")));
                assertEquals(first, storageWrite(storage, context(), 1).candidate().address());
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                    assertEquals(1, read.result().record().entryId());
                }
                SegmentBatchFormat.Address second = storageWrite(storage, context(), 2).candidate().address();
                assertEquals(2, second.sequence());
                assertTrue("Cold restart must skip the old granted tail", first.allocation() != second.allocation());
                ownerBarrier(storageOwner(storage, 0));
            }
            try (IsolatedSegmentStorage storage = rotatingStorage(config, workers)) {
                storage.restart();
                for (long entryId : new long[] {1, 2}) {
                    assertEquals(entryId, storageWrite(storage, context(), entryId).candidate().address().sequence());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(entryId, read.result().record().entryId());
                    }
                }
                assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 3)));
            }
        }
    }

    @Test
    public void fullClosedControlStartupRequiresWriterExclusionAndSuccessfulRecoverySync() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("full-closed-restart-protection");
        fullClosedControlGraph(config, 1);
        SegmentFileIo controlIo = new SegmentFileIo();
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 32768, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        32768, key((byte) 3), VALIDATOR)) {
            SegmentFileIo recoveryIo = new SegmentFileIo();
            try (SegmentDataFileLock held = SegmentDataFileLock.open(config.directory())) {
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                        config, identity(), arena, bookie, recoveryIo, 32, 4));
                assertEquals(0, recoveryIo.forces);
                assertEquals(0, controlIo.writes);
            }
            SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, failedSync, 32, 4));
            assertEquals(1, failedSync.forces);
            assertEquals(0, controlIo.writes);
            assertEquals(1, arena.dataSessionEpoch());
            assertFalse(arena.hasUnclosedDataSession());
            CountDownLatch forcing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            SegmentFileIo pausedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                await(release);
            });
            FutureTask<IsolatedSegmentFiles> opening = new FutureTask<>(() -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, pausedSync, 32, 4));
            Thread opener = new Thread(opening, "cold-control-startup-test");
            opener.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
                release.countDown();
                try (IsolatedSegmentFiles files = opening.get(5, TimeUnit.SECONDS)) {
                    assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                    assertTrue(files.supportsLocalResults());
                    assertEquals(2, arena.dataSessionEpoch());
                    assertTrue(arena.hasUnclosedDataSession());
                    SegmentPointRead.Location location = files.localResultInventory(arena, 0).entries().get(0);
                    assertEquals(1, SegmentPointRead.read(config, identity(), arena, location).record().entryId());
                }
            } finally {
                release.countDown();
                opener.join(5000);
            }
            assertFalse(opener.isAlive());
            assertFalse(arena.hasUnclosedDataSession());
        }
        try (SegmentDataFileLock released = SegmentDataFileLock.open(config.directory())) {
            assertTrue(released.lock().isValid());
        }
    }

    @Test
    public void fullClosedControlStartupRejectsCorruptDataAndRotationFailureWithoutOpen() throws Exception {
        for (String fault : List.of("data", "force", "candidate", "open-force")) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("full-closed-restart-failure-" + fault);
            SegmentBatchFormat.Address first = fullClosedControlGraph(config, 1);
            if (fault.equals("data")) {
                try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                        StandardOpenOption.WRITE)) {
                    data.write(ByteBuffer.wrap(new byte[] {99}), first.offset() + 128);
                    data.force(false);
                }
            } else if (fault.equals("candidate")) {
                Files.write(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2), new byte[4096]);
            }
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1,
                    fault.equals("force") ? 1 : fault.equals("open-force") ? 3 : -1, false);
            SegmentFileIo recoveryIo = new SegmentFileIo();
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 32768, controlIo);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            32768, key((byte) 3), VALIDATOR)) {
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                        config, identity(), arena, bookie, recoveryIo, 32, 4));
                assertEquals(fault.equals("data") ? 0 : 1, recoveryIo.forces);
                assertEquals(fault.equals("data") ? 0 : fault.equals("force") ? 1
                        : fault.equals("open-force") ? 3 : 2, controlIo.forces);
                assertEquals(1, arena.dataSessionEpoch());
                assertFalse(arena.hasUnclosedDataSession());
            }
            try (SegmentDataFileLock released = SegmentDataFileLock.open(config.directory());
                    SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 32768)) {
                assertTrue(released.lock().isValid());
                assertEquals(fault.equals("open-force") ? 2 : 1, replay.dataSessionEpoch());
                assertEquals(fault.equals("open-force"), replay.hasUnclosedDataSession());
            }
            if (fault.equals("open-force")) {
                try (IsolatedSegmentStorage storage = rotatingStorage(config, 1)) {
                    IOException cause = assertThrows(IOException.class, storage::restart);
                    assertTrue(cause.getMessage(), cause.getMessage().contains("Unclosed DATA writer session"));
                }
            }
        }
    }

    @Test
    public void coldControlStartupDoesNotRotateWhenFreshJournalCannotReserveOpenAndClose() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("full-closed-restart-too-small");
        fullClosedControlGraph(config, 1, 8800);
        byte[] before = Files.readAllBytes(config.directory().resolve("arena-control"));
        SegmentFileIo controlIo = new SegmentFileIo();
        SegmentFileIo recoveryIo = new SegmentFileIo();
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 8800, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        8800, key((byte) 3), VALIDATOR)) {
            IOException cause = assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, recoveryIo, 32, 4));
            assertTrue(cause.getMessage(), cause.getMessage().contains("control log capacity exhausted"));
            assertEquals(1, recoveryIo.forces);
            assertEquals(0, controlIo.writes);
            assertEquals(1, arena.dataSessionEpoch());
            assertFalse(arena.hasUnclosedDataSession());
            assertArrayEquals(before, Files.readAllBytes(config.directory().resolve("arena-control")));
            assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
        }
    }

    @Test
    public void automaticControlRotationReclaimsFullLogsPreservesOpenSessionAndOriginalAppendCursor() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("runtime-rotation-" + workers);
            SegmentBatchFormat.Address first;
            try (IsolatedSegmentStorage storage = rotatingStorage(config, workers)) {
                storage.startNew();
                installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
                first = storageWrite(storage, context(), 1).candidate().address();
                ownerBarrier(storageOwner(storage, 0));
                SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
                long epoch = arena.dataSessionEpoch();
                for (int cycle = 0; cycle < 2; cycle++) {
                    try (IsolatedSegmentFiles.DataReadLease pin =
                                 ((IsolatedSegmentFiles) storageField(storage, "files")).pinDataRead()) {
                        exhaustGraphControlLogs(storage, first.allocation());
                        ByteBuf refused = entry(context(), cycle + 2);
                        try {
                            assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), refused,
                                    new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                                        throw new AssertionError("Capacity refusal must not produce a result");
                                    })));
                            assertEquals(1, refused.refCnt());
                        } finally {
                            refused.release();
                        }
                        ownerBarrier(storageOwner(storage, 0));
                        for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                            assertFalse(Files.exists(SegmentControlRotation.path(config, owner, cycle + 2)));
                        }
                    }
                    SegmentBatchFormat.Address next = storageWriteAfterControlRotation(storage, cycle + 2);
                    for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                        assertTrue(Files.exists(SegmentControlRotation.path(config, owner, cycle + 2)));
                    }
                    assertTrue(arena.hasUnclosedDataSession());
                    assertEquals(epoch, arena.dataSessionEpoch());
                    assertEquals(first.allocation(), next.allocation());
                    assertEquals(first.generation(), next.generation());
                    assertEquals(cycle + 2, next.sequence());
                    assertEquals(4096L * (cycle + 2), next.offset());
                    ownerBarrier(storageOwner(storage, 0));
                    for (long entryId = 1; entryId <= cycle + 2; entryId++) {
                        try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                            assertEquals(entryId, read.result().record().entryId());
                        }
                        assertEquals(first.allocation(), storageWrite(storage, context(), entryId)
                                .candidate().address().allocation());
                    }
                    if (cycle > 0) {
                        for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                            assertFalse(Files.exists(SegmentControlRotation.path(config, owner, cycle + 1)));
                        }
                    }
                }
            }
            try (IsolatedSegmentStorage storage = rotatingStorage(config, workers)) {
                storage.restart();
                for (long entryId : new long[] {1, 2, 3}) {
                    assertEquals(first.allocation(), storageWrite(storage, context(), entryId)
                            .candidate().address().allocation());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(entryId, read.result().record().entryId());
                    }
                }
                assertEquals(4, storageWrite(storage, context(), 4).candidate().address().sequence());
            }
        }
    }

    @Test
    public void arenaQuarantineCapacityRefusalWakesAutomaticRotationWithoutControlFailure() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("automatic-quarantine-capacity-" + workers);
            try (IsolatedSegmentStorage storage = rotatingStorage(config, workers)) {
                storage.startNew();
                installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
                SegmentBatchFormat.Address first = storageWrite(storage, context(), 1).candidate().address();
                SegmentOwnerLoop owner = storageOwner(storage, 0);
                ownerBarrier(owner);
                SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
                SegmentArenaControlDriver allocator = (SegmentArenaControlDriver) storageField(storage, "allocator");
                SegmentArenaControlLog.Allocation allocation = arena.currentAllocation(first.allocation());
                // Leave CLOSE (160) and the two rotation commits (416), but less than one new
                // quarantine notification (160) in addition. Every submitted control still fits.
                Path journal = config.directory().resolve("arena-control");
                for (int i = 0; i < 200 && 32768 - Files.size(journal) >= 736; i++) {
                    CompletableFuture<Throwable> reply = new CompletableFuture<>();
                    assertTrue(allocator.submit(List.of(allocation), (results, cause) -> {
                        if (cause == null) {
                            assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE, results.get(0).status());
                        }
                        reply.complete(cause);
                    }));
                    assertNull(reply.get(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                }
                long remaining = 32768 - Files.size(journal);
                assertTrue("Actual reserved-space boundary: " + remaining, remaining >= 576 && remaining < 736);
                for (SegmentControlFormat.Owner controlOwner : SegmentControlFormat.Owner.values()) {
                    assertFalse(Files.exists(SegmentControlRotation.path(config, controlOwner, 2)));
                }
                ByteBuf bytes = entry(context(), 2);
                try {
                    assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                            new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                                throw new AssertionError("Quarantine reserve refusal must retain caller ownership");
                            })));
                    assertEquals(1, bytes.refCnt());
                } finally {
                    bytes.release();
                }
                SegmentBatchFormat.Address next = storageWriteAfterControlRotation(storage, 2);
                assertEquals(first.allocation(), next.allocation());
                assertEquals(2, next.sequence());
                assertEquals(8192, next.offset());
                for (SegmentControlFormat.Owner controlOwner : SegmentControlFormat.Owner.values()) {
                    assertTrue(Files.exists(SegmentControlRotation.path(config, controlOwner, 2)));
                }
                assertNull(storage.controlMaintenanceFailure());
            }
        }
    }

    @Test
    public void oversizedControlBatchDoesNotTriggerUnproductiveAutomaticRotation() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("automatic-rotation-oversized"), 1, 4, 1,
                4 * SegmentArenaControlLog.BLOCK_BYTES, 4096, 512, 320,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 4)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            storageWrite(storage, context(), 1);
            ownerBarrier(storageOwner(storage, 0));
            CompletableFuture<Throwable> reply = new CompletableFuture<>();
            assertTrue(storage.controls().submitBatch(java.util.Collections.nCopies(300, install(44, 1, (byte) 5)),
                    (results, cause) -> reply.complete(cause)));
            Throwable cause = reply.get(5, TimeUnit.SECONDS);
            assertTrue(String.valueOf(cause), cause instanceof IOException);
            assertTrue(cause.toString(), cause.getMessage().contains("control log capacity exhausted"));
            ownerBarrier(storageOwner(storage, 0));
            assertEquals(2, storageWrite(storage, context(), 2).candidate().address().sequence());
            ownerBarrier(storageOwner(storage, 0));
            for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                assertFalse(Files.exists(SegmentControlRotation.path(config, owner, 2)));
            }
            assertNull(storage.controlMaintenanceFailure());
        }
    }

    @Test
    public void automaticControlRotationFailureIsObservableAndStopsGraphAdmission() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("automatic-rotation-failure");
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 1)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            SegmentBatchFormat.Address first = storageWrite(storage, context(), 1).candidate().address();
            ownerBarrier(storageOwner(storage, 0));
            // A real invalid candidate cannot acquire journal authority; no force-success mock is used.
            Files.write(SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE, 2), new byte[4096]);
            try (IsolatedSegmentFiles.DataReadLease pin =
                         ((IsolatedSegmentFiles) storageField(storage, "files")).pinDataRead()) {
                exhaustGraphControlLogs(storage, first.allocation());
            }
            storageOwner(storage, 0).wakeup();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (storage.controlMaintenanceFailure() == null && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertTrue(storage.controlMaintenanceFailure() instanceof IOException);
            assertThrows(IllegalStateException.class, storage::controls);
            ByteBuf bytes = entry(context(), 2);
            try {
                assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            throw new AssertionError("Failed automatic maintenance cannot admit DATA");
                        })));
                assertEquals(1, bytes.refCnt());
            } finally {
                bytes.release();
            }
        }
    }

    @Test
    public void runtimeControlRotationWaitsForDataReadPinsAndCancelledIoOwnerResolution() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("runtime-rotation-cancel");
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch releaseForce = new CountDownLatch(1);
        IsolatedSegmentStorage storage = rotatingStorage(config, 4);
        try {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            storageWrite(storage, context(), 1);
            ownerBarrier(storageOwner(storage, 0));
            SegmentBookieControlDriver cachedControls = storage.controls();
            try (IsolatedSegmentFiles.DataReadLease held =
                         ((IsolatedSegmentFiles) storageField(storage, "files")).pinDataRead()) {
                CompletableFuture<IsolatedSegmentStorage.ControlRotationResult> reply = new CompletableFuture<>();
                SegmentFileIo untouched = new SegmentFileIo();
                assertTrue(storage.rotateControls(new IsolatedSegmentStorage.ControlRotationRequest(
                        (result, cause) -> reply.complete(result)), untouched, untouched));
                assertEquals(IsolatedSegmentStorage.ControlRotationStatus.TRANSIENT_UNAVAILABLE,
                        reply.get(5, TimeUnit.SECONDS).status());
                assertEquals(0, untouched.writes);
                assertEquals(0, untouched.forces);
            }
            ownerBarrier(storageOwner(storage, 0));
            AtomicInteger responses = new AtomicInteger();
            SegmentFileIo paused = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                atForce.countDown();
                await(releaseForce);
            });
            IsolatedSegmentStorage.ControlRotationRequest request = new IsolatedSegmentStorage.ControlRotationRequest(
                    (result, cause) -> responses.incrementAndGet());
            assertTrue(storage.rotateControls(request, paused, new SegmentFileIo()));
            assertTrue(atForce.await(5, TimeUnit.SECONDS));
            assertTrue(request.stopWaiting());
            assertFalse(cachedControls.submitBatch(List.of(install(48, 10, (byte) 5)), (results, cause) -> {
                throw new AssertionError("Cancelled rotation must retain both control reservations");
            }));
            ByteBuf bytes = entry(context(), 2);
            try {
                assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            throw new AssertionError("No DATA admission while rotation is protected");
                        })));
                assertEquals(1, bytes.refCnt());
            } finally {
                bytes.release();
            }
            assertFalse(storage.close(Duration.ofMillis(50)));
            assertEquals(0, responses.get());
            releaseForce.countDown();
            assertTrue(storage.close(Duration.ofSeconds(5)));
            assertEquals(0, responses.get());
        } finally {
            releaseForce.countDown();
            storage.close();
        }
        try (IsolatedSegmentStorage reopened = rotatingStorage(config, 1)) {
            reopened.restart();
            storageWrite(reopened, context(), 1);
            assertEquals(2, storageWrite(reopened, context(), 2).candidate().address().sequence());
        }
    }

    @Test
    public void runtimeControlRotationForceFailureStopsGraphAndReportsOnlyCompletedOwnerRoots() throws Exception {
        for (boolean arenaFailure : new boolean[] {false, true}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("runtime-rotation-fail-" + arenaFailure);
            IsolatedSegmentStorage storage = rotatingStorage(config, 1);
            try {
                storage.startNew();
                installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
                storageWrite(storage, context(), 1);
                ownerBarrier(storageOwner(storage, 0));
                CompletableFuture<IsolatedSegmentStorage.ControlRotationResult> reply = new CompletableFuture<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                SegmentFileIo faulty = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                assertTrue(storage.rotateControls(new IsolatedSegmentStorage.ControlRotationRequest((result, cause) -> {
                    failure.set(cause);
                    reply.complete(result);
                }), arenaFailure ? new SegmentFileIo() : faulty, arenaFailure ? faulty : new SegmentFileIo()));
                IsolatedSegmentStorage.ControlRotationResult result = reply.get(5, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.ControlRotationStatus.UNKNOWN, result.status());
                assertTrue(failure.get() instanceof IOException);
                assertEquals(arenaFailure, result.bookieRoot() != null);
                assertNull(result.arenaRoot());
                assertThrows(IllegalStateException.class, storage::controls);
                assertFalse(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                        (status, read, cause) -> {
                            throw new AssertionError("Failed graph cannot serve success");
                        })));
            } finally {
                if (arenaFailure) {
                    assertThrows(IOException.class, storage::close);
                } else {
                    storage.close();
                }
            }
            try (IsolatedSegmentStorage reopened = rotatingStorage(config, 1)) {
                if (arenaFailure) {
                    IOException failure = assertThrows(IOException.class, reopened::restart);
                    assertTrue(failure.toString(), failure.getMessage().contains("Unclosed DATA writer session"));
                } else {
                    reopened.restart();
                    storageWrite(reopened, context(), 1);
                }
            }
        }
    }

    @Test
    public void declaredTwoBlockMaintenanceWorksetCompactsWhileSmallerBoundRefusesBeforeAlloc() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("reserve-two"),
                1, 4, 1, 4096 + 5 * SegmentArenaControlLog.BLOCK_BYTES, 4096, 32768, 16,
                "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = maintenanceStorage(config, 2)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            SegmentOwnerLoop owner = storageOwner(storage, 0);
            for (int block = 0; block < 3; block++) {
                for (int row = 0; row < 28; row++) {
                    ProfileLedgerContext context = context(row < 10 ? 44 : 48);
                    long entryId = block * 28 + row + 1;
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                            storageWriteReply(storage, context, entryId,
                                    entry(context, entryId, -1, entryId * 32768, (byte) 7, 32768)).status());
                    ownerBarrier(owner);
                }
            }
            installStorage(storage, tombstone(48, 5));
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                    storageWriteReply(storage, context(), 85,
                            entry(context(), 85, -1, 85 * 32768, (byte) 7, 32768)).status());
        }
        try (IsolatedSegmentStorage storage = maintenanceStorage(config, 1)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult refused = compactShard(storage, 951, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.TRANSIENT_UNAVAILABLE, refused.status());
            assertEquals(0, refused.destinationBytes());
            assertEquals(0, refused.movedEntries());
            assertEquals(0, refused.freedAllocations());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(4096 + 3 * SegmentArenaControlLog.BLOCK_BYTES, arena.grantedHighWatermark(0));
            assertEquals(0, arena.currentMoveLocations().size());
            assertTrue(arena.retiredAllocations().isEmpty());
        }
        try (IsolatedSegmentStorage storage = maintenanceStorage(config, 2)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult reclaimed = compactShard(storage, 952, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, reclaimed.status());
            assertEquals(30, reclaimed.movedEntries());
            assertEquals(3, reclaimed.freedAllocations());
            assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, reclaimed.destinationBytes());
            assertEquals(SegmentArenaControlLog.BLOCK_BYTES, reclaimed.netFreedBytes());
            for (int block = 0; block < 3; block++) {
                for (int row = 0; row < 10; row++) {
                    long entryId = block * 28 + row + 1;
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                    }
                    assertEquals(reclaimed.destinationAllocation(), storageWriteReply(storage, context(), entryId,
                            entry(context(), entryId, -1, entryId * 32768, (byte) 7, 32768)).candidate()
                            .address().allocation());
                }
            }
        }
    }

    private static SegmentDataPhysicalDriver.NormalResult writeAfterArenaSpaceMaintenance(
            IsolatedSegmentStorage storage, ProfileLedgerContext context, long entryId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        ByteBuf input = entry(context, entryId);
        try {
            do {
                CompletableFuture<SegmentDataPhysicalDriver.NormalResult> reply = new CompletableFuture<>();
                if (storage.submitNormal(context, filled(20, (byte) 5), input,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            if (cause == null) {
                                reply.complete(result);
                            } else {
                                reply.completeExceptionally(cause);
                            }
                        }))) {
                    SegmentDataPhysicalDriver.NormalResult result = reply.get(5, TimeUnit.SECONDS);
                    if (result.status() == SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE) {
                        return result;
                    }
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, result.status());
                }
                assertEquals(1, input.refCnt());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            } while (System.nanoTime() < deadline);
            throw new AssertionError("Arena capacity must recover without an explicit compaction");
        } finally {
            input.release();
        }
    }

    private void writeCheckpointAsLegacy(IsolatedSegmentConfig config, SegmentControlFormat.Owner owner,
                                                 SegmentControlCheckpoint.Reference reference) throws Exception {
        SegmentControlCheckpoint.Snapshot compacted = SegmentControlCheckpoint.read(
                config, identity(), owner, reference, 32768);
        SegmentControlCheckpoint.write(config, identity(), owner, reference,
                new SegmentControlCheckpoint.Snapshot(compacted.through(), compacted.logOffset(),
                        compacted.operations()), new SegmentFileIo());
    }

    @Test
    public void originalJournalRestoresLatestCompactedCheckpointAndCorruptionFallback() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("original-checkpoint-history");
        SegmentControlCheckpoint.Reference previous = null;
        SegmentControlCheckpoint.Reference latest = null;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                        32768, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), tombstone(48, 5)));
            for (int cycle = 0; cycle < 8; cycle++) {
                previous = latest;
                latest = log.checkpoint();
            }
            assertTrue(SegmentControlCheckpoint.read(config, identity(), SegmentControlFormat.Owner.BOOKIE,
                    latest, 32768).compactedCheckpointHistory());
            log.submitBatch(List.of(tombstone(44, 6)));
        }
        for (SegmentControlCheckpoint.Reference expected : List.of(latest, previous)) {
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                    32768, key((byte) 3), VALIDATOR)) {
                assertEquals(java.util.Optional.of(expected), log.publishedCheckpoint());
                assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
                assertTrue(log.admitNormal(context(48), filled(20, (byte) 5)).isEmpty());
                assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                        log.submitBatch(List.of(install(48, 3, (byte) 5), tombstone(44, 6))).get(0).status());
                assertEquals(SegmentBookieControlLog.Status.INCOMPATIBLE,
                        log.submitBatch(List.of(install(48, 3, (byte) 6))).get(0).status());
            }
            if (expected.equals(latest)) {
                Files.write(SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.BOOKIE, latest.slot()),
                        new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);
            }
        }
    }

    @Test
    public void legacyCheckpointImagesRemainReadableBeforeInternalHistoryIsCompacted() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("legacy-checkpoint-history");
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 1)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), tombstone(48, 5));
            storageWrite(storage, context(), 1);
            ownerBarrier(storageOwner(storage, 0));
            CompletableFuture<IsolatedSegmentStorage.ControlRotationResult> reply = new CompletableFuture<>();
            assertTrue(storage.rotateControls(new IsolatedSegmentStorage.ControlRotationRequest((result, cause) -> {
                if (cause == null) {
                    reply.complete(result);
                } else {
                    reply.completeExceptionally(cause);
                }
            })));
            IsolatedSegmentStorage.ControlRotationResult result = reply.get(5, TimeUnit.SECONDS);
            assertEquals(IsolatedSegmentStorage.ControlRotationStatus.ROTATED, result.status());
            ownerBarrier(storageOwner(storage, 0));
            for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                SegmentControlRotation.Root root = owner == SegmentControlFormat.Owner.ARENA
                        ? result.arenaRoot() : result.bookieRoot();
                writeCheckpointAsLegacy(config, owner, root.first());
                writeCheckpointAsLegacy(config, owner, root.second());
            }
        }
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 1)) {
            storage.restart();
            assertEquals(1, storageWrite(storage, context(), 1).candidate().address().sequence());
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            assertTrue(bookie.admitNormal(context(48), filled(20, (byte) 5)).isEmpty());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
    }

    @Test
    public void compactedEmptyControlImagesSupportSequenceGapsAndRejectLegacyMisclassification() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("empty-checkpoint-history");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity())) {
            SegmentControlRotation.Root arenaRoot = null;
            SegmentControlRotation.Root bookieRoot = null;
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 32768);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            32768, key((byte) 3), VALIDATOR)) {
                for (int cycle = 0; cycle < 8; cycle++) {
                    arenaRoot = arena.rotate();
                    bookieRoot = bookie.rotate();
                }
            }
            for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
                SegmentControlRotation.Root root = owner == SegmentControlFormat.Owner.ARENA ? arenaRoot : bookieRoot;
                SegmentControlCheckpoint.Snapshot snapshot = SegmentControlCheckpoint.read(
                        config, identity(), owner, root.second(), 32768);
                assertTrue(snapshot.compactedCheckpointHistory());
                assertTrue(snapshot.operations().get(0).sequence() > 0);
                Path first = SegmentControlCheckpoint.path(config, owner, root.first().slot());
                Path second = SegmentControlCheckpoint.path(config, owner, root.second().slot());
                byte[] firstBytes = Files.readAllBytes(first);
                byte[] secondBytes = Files.readAllBytes(second);
                writeCheckpointAsLegacy(config, owner, root.first());
                writeCheckpointAsLegacy(config, owner, root.second());
                if (owner == SegmentControlFormat.Owner.ARENA) {
                    assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 32768));
                } else {
                    assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(),
                            32768, key((byte) 3), VALIDATOR));
                }
                Files.write(first, firstBytes);
                Files.write(second, secondBytes);
                // One image is insufficiently framed; the other must still restore the same empty authority.
                Files.write(second, new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);
                if (owner == SegmentControlFormat.Owner.ARENA) {
                    try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 32768)) {
                        assertEquals(root.second().through() + 1, arena.appliedThrough());
                        assertEquals(root.second().generation() + 1, arena.checkpoint().generation());
                    }
                } else {
                    try (SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            32768, key((byte) 3), VALIDATOR)) {
                        assertTrue(bookie.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
                        assertEquals(root.second().generation() + 1, bookie.checkpoint().generation());
                    }
                }
            }
        }
    }

    @Test
    public void repeatedControlRotationKeepsFixedBusinessStateWithinCheckpointBudgetAcrossRestart() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("bounded-checkpoint-history");
        SegmentBatchFormat.Address first;
        IsolatedSegmentStorage.ControlRotationResult stableRoots;
        int bookieBytes = -1;
        int arenaBytes = -1;
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 1, 16384)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), tombstone(48, 5));
            first = storageWrite(storage, context(), 1).candidate().address();
            SegmentOwnerLoop owner = storageOwner(storage, 0);
            ownerBarrier(owner);
            IsolatedSegmentStorage.ControlRotationResult rotated = null;
            for (int cycle = 0; cycle < 96; cycle++) {
                CompletableFuture<IsolatedSegmentStorage.ControlRotationResult> reply = new CompletableFuture<>();
                assertTrue(storage.rotateControls(new IsolatedSegmentStorage.ControlRotationRequest((result, cause) -> {
                    if (cause == null) {
                        reply.complete(result);
                    } else {
                        reply.completeExceptionally(cause);
                    }
                })));
                rotated = reply.get(5, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.ControlRotationStatus.ROTATED, rotated.status());
                ownerBarrier(owner);
                if (cycle == 1) {
                    bookieBytes = rotated.bookieRoot().second().payloadBytes();
                    arenaBytes = rotated.arenaRoot().second().payloadBytes();
                }
            }
            stableRoots = rotated;
            assertEquals("Fixed Bookie business state must not accumulate internal checkpoint history",
                    bookieBytes, rotated.bookieRoot().second().payloadBytes());
            assertEquals("Fixed Arena business state must not accumulate internal checkpoint history",
                    arenaBytes, rotated.arenaRoot().second().payloadBytes());
            assertEquals(first.allocation(), storageWrite(storage, context(), 1).candidate().address().allocation());
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            assertTrue(bookie.admitNormal(context(48), filled(20, (byte) 5)).isEmpty());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    bookie.submitBatch(List.of(install(48, 3, (byte) 5))).get(0).status());
            SegmentBatchFormat.Address second = storageWrite(storage, context(), 2).candidate().address();
            assertEquals(first.allocation(), second.allocation());
            assertEquals(2, second.sequence());
        }
        for (SegmentControlFormat.Owner owner : SegmentControlFormat.Owner.values()) {
            SegmentControlRotation.Root root = owner == SegmentControlFormat.Owner.ARENA
                    ? stableRoots.arenaRoot() : stableRoots.bookieRoot();
            Path firstImage = SegmentControlCheckpoint.path(config, owner, root.first().slot());
            Path secondImage = SegmentControlCheckpoint.path(config, owner, root.second().slot());
            byte[] firstImageBytes = Files.readAllBytes(firstImage);
            byte[] secondImageBytes = Files.readAllBytes(secondImage);
            writeCheckpointAsLegacy(config, owner, root.first());
            writeCheckpointAsLegacy(config, owner, root.second());
            if (owner == SegmentControlFormat.Owner.ARENA) {
                assertThrows(IOException.class, () -> SegmentArenaControlLog.open(config, identity(), 16384));
            } else {
                assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(),
                        16384, key((byte) 3), VALIDATOR));
            }
            Files.write(firstImage, firstImageBytes);
            Files.write(secondImage, secondImageBytes);
        }
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 1, 16384)) {
            storage.restart();
            SegmentBookieControlLog bookie = (SegmentBookieControlLog) storageField(storage, "bookie");
            assertTrue(bookie.admitNormal(context(48), filled(20, (byte) 5)).isEmpty());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    bookie.submitBatch(List.of(install(48, 3, (byte) 5))).get(0).status());
            assertEquals(first.allocation(), storageWrite(storage, context(), 1).candidate().address().allocation());
            assertEquals(2, storageWrite(storage, context(), 2).candidate().address().sequence());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
    }

    @Test
    public void automaticSpaceMaintenanceReclaimsSparseBlocksAndRestoresAdmissionBeforeAndAfterRestart()
            throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("foreground-reserve-" + workers);
            long destination;
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4096, 4096, 3)) {
                storage.startNew();
                installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                        install(48, 3, (byte) 5), activate(48, 4));
                SegmentOwnerLoop owner = storageOwner(storage, 0);
                // Only real ordinary writes allocate and fill these two sources; no fixture grants.
                for (int block = 0; block < 2; block++) {
                    storageWrite(storage, context(), block + 1);
                    ownerBarrier(owner);
                    for (int row = 1; row < 256; row++) {
                        storageWrite(storage, context(48), block * 255 + row);
                        ownerBarrier(owner);
                    }
                }
                installStorage(storage, tombstone(48, 5));
                SegmentDataPhysicalDriver.NormalResult refused = storageWriteReply(storage, context(), 3);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, refused.status());
                assertNull(refused.candidate());
                SegmentPointRead.Location added = writeAfterArenaSpaceMaintenance(storage, context(), 3).candidate();
                assertEquals(1, added.address().generation().value());
                assertEquals(8192 + 2 * SegmentArenaControlLog.BLOCK_BYTES, added.address().offset());
                assertEquals(514, added.address().sequence());
                SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
                destination = arena.currentMoveLocation(context(), 1).orElseThrow().address().allocation();
                assertEquals(destination, added.address().allocation());
                assertEquals(2, arena.retiredAllocations().size());
                for (long entryId : new long[] {1, 2}) {
                    assertEquals(destination, storageWrite(storage, context(), entryId).candidate()
                            .address().allocation());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                    }
                }
            }
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4096, 4096, 3)) {
                storage.restart();
                SegmentPointRead.Location added = storageWrite(storage, context(), 3).candidate();
                assertEquals(1, added.address().generation().value());
                assertEquals(destination, added.address().allocation());
                assertEquals(514, added.address().sequence()); // Existing success remains a duplicate after restart.
                for (long entryId : new long[] {1, 2}) {
                    assertEquals(destination, storageWrite(storage, context(), entryId).candidate()
                            .address().allocation());
                }
                SegmentPointRead.Location fresh = storageWrite(storage, context(), 4).candidate();
                assertEquals(2, fresh.address().generation().value());
                assertEquals(4096, fresh.address().offset());
                assertEquals(515, fresh.address().sequence());
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 3).result()) {
                    assertEquals(3, read.result().record().entryId());
                }
            }
        }
    }

    @Test
    public void automaticSpaceMaintenanceWaitsForReadPinAndOwnerBuffersBeforeServingAnotherShard() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("automatic-space-cross-shard");
        createCompactionVictims(config, false);
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 16, 32, 3)) {
            storage.restart();
            installStorage(storage, install(45, 30, (byte) 5), activate(45, 31));
            SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
            IsolatedSegmentFiles files = (IsolatedSegmentFiles) storageField(storage, "files");
            SegmentFileIo copyIo = new SegmentFileIo();
            files.dataIo = copyIo;
            SegmentBufferPool.Lease held;
            try (IsolatedSegmentFiles.DataReadLease pin = files.pinDataRead()) {
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                        storageWriteReply(storage, context(45), 1).status());
                ownerBarrier(storageOwner(storage, 1), 1);
                assertTrue(arena.hasSpacePressure());
                assertTrue(arena.currentMoveLocations().isEmpty());
                assertEquals(0, copyIo.forces);
                FutureTask<SegmentBufferPool.Lease> borrow = new FutureTask<>(
                        () -> storageOwner(storage, 0).pool().acquire());
                assertTrue(storageOwner(storage, 0).control(borrow));
                held = borrow.get(5, TimeUnit.SECONDS);
                assertNotNull(held);
            }
            try {
                for (int worker = 0; worker < 4; worker++) {
                    ownerBarrier(storageOwner(storage, worker), worker);
                }
                assertTrue(arena.hasSpacePressure());
                assertFalse(storage.spaceMaintenanceIdle());
                assertTrue(arena.currentMoveLocations().isEmpty());
                assertEquals(0, copyIo.forces);
            } finally {
                held.close();
            }
            SegmentBatchFormat.Address reused = writeAfterArenaSpaceMaintenance(storage, context(45), 1)
                    .candidate().address();
            assertEquals(1, reused.shard());
            assertEquals(2, reused.generation().value());
            assertEquals(4096, reused.offset());
            assertEquals(2, arena.retiredAllocations().size());
            assertNull(storage.spaceMaintenanceFailure());
            for (long entryId = 1; entryId <= 3; entryId++) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                }
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 16, 32, 3)) {
            storage.restart();
            assertEquals(2, storageWrite(storage, context(45), 1).candidate().address().generation().value());
            for (long entryId = 1; entryId <= 3; entryId++) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                }
            }
        }
    }

    @Test
    public void automaticSpaceMaintenanceStopsFullLiveSweepUntilActualTombstoneCreatesCapacity() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("automatic-space-full-live"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 3072, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4096, 4096, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            SegmentOwnerLoop owner = storageOwner(storage, 0);
            for (int row = 1; row <= 512; row++) {
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        storageWriteReply(storage, context(), row,
                                entry(context(), row, -1, row * 2000, (byte) 0, 2000)).status());
                ownerBarrier(owner);
            }
            IsolatedSegmentFiles files = (IsolatedSegmentFiles) storageField(storage, "files");
            SegmentFileIo copyIo = new SegmentFileIo();
            files.dataIo = copyIo;
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                    storageWriteReply(storage, context(), 513).status());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!storage.spaceMaintenanceIdle() && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            assertTrue("Full-live sweep must finish without copying", storage.spaceMaintenanceIdle());
            SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
            assertTrue(arena.hasSpacePressure());
            long cut = arena.appliedThrough();
            ByteBuf input = entry(context(), 513);
            try {
                // Repeated demand must not reset the unchanged full-live round or own the rejected input.
                for (int attempt = 0; attempt < 12; attempt++) {
                    assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), input,
                            new SegmentDataPhysicalDriver.NormalRequest((result, failure) -> {
                                throw new AssertionError("Paused DATA must not produce a result");
                            })));
                    assertEquals(1, input.refCnt());
                    ownerBarrier(owner);
                    assertTrue(storage.spaceMaintenanceIdle());
                }
            } finally {
                input.release();
            }
            assertEquals(cut, arena.appliedThrough());
            assertEquals(0, copyIo.forces);
            assertTrue(arena.currentMoveLocations().isEmpty());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
            installStorage(storage, tombstone(44, 5));
            SegmentBatchFormat.Address reused = writeAfterArenaSpaceMaintenance(storage, context(48), 1)
                    .candidate().address();
            assertEquals(2, reused.generation().value());
            assertEquals(4096, reused.offset());
            assertEquals(2, arena.retiredAllocations().size());
            assertTrue(arena.currentMoveLocations().isEmpty()); // All dead: no COPY destination was required.
            assertEquals(1, copyIo.forces); // Only the new ordinary entry performed DATA I/O.
            assertNull(storage.spaceMaintenanceFailure());
        }
    }

    @Test
    public void automaticSpaceMaintenanceRotatesKnownControlCapacityRefusalBeforeRetryingProtectedWork()
            throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("automatic-space-control-capacity");
        createCompactionVictims(config, false);
        try (IsolatedSegmentStorage storage = rotatingStorage(config, 4)) {
            storage.restart();
            installStorage(storage, install(45, 30, (byte) 5), activate(45, 31));
            SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
            try (IsolatedSegmentFiles.DataReadLease pin =
                         ((IsolatedSegmentFiles) storageField(storage, "files")).pinDataRead()) {
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                        storageWriteReply(storage, context(45), 1).status());
                ownerBarrier(storageOwner(storage, 1), 1);
                SegmentArenaControlLog.Allocation allocation = arena.currentAllocation(5);
                int recordBytes = SegmentControlFormat.encode(allocation.type(), new Generation(1), 1,
                        allocation.operationId(), allocation.operationGeneration(), allocation.expectedGeneration(),
                        allocation.body()).remaining();
                // Stop after the last fitting ordinary record, before any operation requests rotation.
                while (Files.size(config.directory().resolve("arena-control")) + recordBytes + 416 + 192 <= 32768) {
                    assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                            arena.submitBatch(List.of(allocation)).get(0).status());
                }
                assertTrue(arena.hasSpacePressure());
                assertFalse(arena.rotationRequired());
                assertFalse(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
            }
            SegmentBatchFormat.Address reused = writeAfterArenaSpaceMaintenance(storage, context(45), 1)
                    .candidate().address();
            assertTrue(Files.exists(SegmentControlRotation.path(config, SegmentControlFormat.Owner.ARENA, 2)));
            assertEquals(2, reused.generation().value());
            assertEquals(2, arena.retiredAllocations().size());
            assertNull(storage.spaceMaintenanceFailure());
            assertNull(storage.controlMaintenanceFailure());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
            }
        }
    }

    @Test
    public void automaticSpaceMaintenanceCopyForceUnknownStopsGraphAndRetainsActualSources() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("automatic-space-copy-unknown");
        createCompactionVictims(config, false);
        IsolatedSegmentStorage storage = storage(config, 4, 6, 16, 32, 3);
        boolean expectedFailedClose = false;
        try {
            storage.restart();
            installStorage(storage, install(45, 30, (byte) 5), activate(45, 31));
            IsolatedSegmentFiles files = (IsolatedSegmentFiles) storageField(storage, "files");
            SegmentFileIo copyIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            files.dataIo = copyIo;
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                    storageWriteReply(storage, context(45), 1).status());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (storage.spaceMaintenanceFailure() == null && System.nanoTime() < deadline) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
            Throwable failure = storage.spaceMaintenanceFailure();
            expectedFailedClose = copyIo.forces == 1;
            assertNotNull("Actual COPY failure must stop automatic maintenance", failure);
            Throwable root = failure;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            assertTrue(root.toString(), root.getMessage().contains("force failure"));
            assertEquals(1, copyIo.forces);
            assertThrows(IllegalStateException.class, storage::controls);
            SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
            assertTrue(arena.retiredAllocations().isEmpty());
            assertTrue(arena.currentMoveLocations().isEmpty());
            assertFalse(arena.dataFileAvailable(0));
            ByteBuf refused = entry(context(45), 1);
            try {
                assertFalse(storage.submitNormal(context(45), filled(20, (byte) 5), refused,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            throw new AssertionError("Failed graph must not produce a DATA result");
                        })));
                assertEquals(1, refused.refCnt());
            } finally {
                refused.release();
            }
        } finally {
            if (expectedFailedClose) {
                assertThrows(IOException.class, storage::close);
            } else {
                storage.close();
            }
        }
        try (IsolatedSegmentStorage restarted = storage(config, 4, 6, 16, 32, 3)) {
            assertThrows(IOException.class, restarted::restart);
        }
    }

    private static ReadReply storageRead(IsolatedSegmentStorage storage, ProfileLedgerContext context, long entryId)
            throws Exception {
        CompletableFuture<ReadReply> reply = new CompletableFuture<>();
        assertTrue(storage.submitRead(context, entryId, new SegmentPointRead.ReadRequest(
                (status, result, cause) -> reply.complete(new ReadReply(status, result, cause)))));
        ReadReply result = reply.get(5, TimeUnit.SECONDS);
        assertNull(result.failure());
        assertEquals(SegmentPointRead.ReadStatus.FOUND, result.status());
        return result;
    }

    @Test
    public void bookieRotationReclaimsOrdinaryBudgetAndRestoresSealedPermissionsAfterEachCycle() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            for (int cycle = 0; cycle < 3; cycle++) {
                try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                        key((byte) 3), VALIDATOR)) {
                    if (cycle == 0) {
                        log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                    }
                    boolean full = false;
                    for (int retry = 0; retry < 200; retry++) {
                        try {
                            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                                    log.submitBatch(List.of(install(1, (byte) 5))).get(0).status());
                        } catch (IOException capacity) {
                            full = true;
                            break;
                        }
                    }
                    assertTrue("Ordinary budget must be exhausted before rotation", full);
                    SegmentControlRotation.Root root = log.rotate();
                    assertEquals(cycle + 2, root.generation());
                    assertEquals(8192, Files.size(SegmentControlRotation.path(
                            config, SegmentControlFormat.Owner.BOOKIE, root.generation())));
                    assertEquals(8192, Files.size(config.directory().resolve("bookie-control")));
                    assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
                    if (cycle > 0) {
                        assertFalse(Files.exists(SegmentControlRotation.path(
                                config, SegmentControlFormat.Owner.BOOKIE, root.generation() - 1)));
                    }
                    log.submitExplicitLac(writeLac(cycle + 1, (byte) 5, (byte) cycle));
                    log.checkpoint();
                }
            }
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                    key((byte) 3), VALIDATOR)) {
                assertEquals(3, log.explicitLac(context()).orElseThrow().lac());
                assertArrayEquals(new byte[] {2}, log.explicitLac(context()).orElseThrow().body());
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        log.submitFence(fence(3, (byte) 5)).status());
            }
        }
    }

    @Test
    public void bookieRotationFallbackPreservesFenceAndRequiresTheCurrentJournal() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlRotation.Root root;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                        key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            log.submitFence(fence(3, (byte) 5));
            root = log.rotate();
            log.submitBatch(List.of(tombstone(44, 4)));
        }
        Path newest = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.BOOKIE, root.second().slot());
        byte[] image = Files.readAllBytes(newest);
        try (FileChannel file = FileChannel.open(newest, StandardOpenOption.WRITE)) {
            file.write(ByteBuffer.wrap(new byte[] {0}), 4096);
        }
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                key((byte) 3), VALIDATOR)) {
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertTrue(log.admitRead(context()).isEmpty());
            assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                    log.submitBatch(List.of(tombstone(44, 4))).get(0).status());
        }
        Files.write(newest, image);
        Path current = SegmentControlRotation.path(config, SegmentControlFormat.Owner.BOOKIE, root.generation());
        byte[] journal = Files.readAllBytes(current);
        try (FileChannel file = FileChannel.open(current, StandardOpenOption.WRITE)) {
            file.truncate(journal.length - 1);
        }
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(), 32768,
                key((byte) 3), VALIDATOR));
        Files.write(current, journal);
        Files.delete(current);
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(), 32768,
                key((byte) 3), VALIDATOR));
    }

    @Test
    public void bookieRotationProcessExitKeepsAuthorityAtFileAndDualRootBarriers() throws Exception {
        for (String phase : List.of("1", "3", "5", "6", "7", "8", "done")) {
            IsolatedSegmentConfig config = checkpointConfig(temporary.newFolder("rotation-" + phase)
                    .toPath().resolve("bookie"));
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                    SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                            key((byte) 3), VALIDATOR)) {
                log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            }
            assertEquals(phase, 23, bookieRotationProbe(config.directory(), phase));
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                    key((byte) 3), VALIDATOR)) {
                assertEquals(!phase.equals("done"), log.normalAdmission(context(), filled(20, (byte) 5)));
                assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                        log.submitBatch(List.of(install(1, (byte) 5))).get(0).status());
                log.rotate();
            }
        }
    }

    @Test
    public void bookieSidecarLockSurvivesOtherOwnerIdentityChecksAndFailedSameProcessOpen() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                        key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(), 32768,
                    key((byte) 3), VALIDATOR));
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 32768)) {
                assertEquals(23, bookieRotationProbe(config.directory(), "lock"));
                log.rotate();
                arena.rotate();
                assertEquals(23, bookieRotationProbe(config.directory(), "lock"));
            }
        }
        assertEquals(0, bookieRotationProbe(config.directory(), "lock"));
    }

    @Test
    public void bookieRotationForceFailureIsNotSuccessAndPendingAdmissionsPreventMaintenance() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                        key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentBookieControlLog.ReadAdmission read = log.admitRead(context()).orElseThrow();
            assertThrows(IOException.class, log::rotate);
            read.finish();
            assertThrows(IOException.class, () -> log.rotate(new SegmentFileIo(17, -1, 7, false)));
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
        }
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                key((byte) 3), VALIDATOR)) {
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            log.rotate();
        }
    }

    private int bookieRotationProbe(Path directory, String phase) throws Exception {
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-XX:MaxDirectMemorySize=32m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                BookieRotationCrashProbe.class.getName(), phase, directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.getParent().resolve("rotation-probe.log").toFile())
                .start();
        try {
            assertTrue(phase, process.waitFor(15, TimeUnit.SECONDS));
            return process.exitValue();
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    public static final class BookieRotationCrashProbe {
        public static void main(String[] args) throws Exception {
            AtomicInteger forces = new AtomicInteger();
            SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (Integer.toString(forces.incrementAndGet()).equals(args[0])) {
                    Runtime.getRuntime().halt(23);
                }
            });
            SegmentFileHeader identity = new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 7)),
                    new UUID(0, 9), BookieId.parse("isolated:3181"));
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(checkpointConfig(Path.of(args[1])),
                    identity, 32768, key((byte) 3), VALIDATOR)) {
                if (args[0].equals("lock")) {
                    return;
                }
                log.rotate(io);
                log.submitFence(fence(3, (byte) 5));
                Runtime.getRuntime().halt(23);
            } catch (IOException failure) {
                if (args[0].equals("lock")) {
                    Runtime.getRuntime().halt(23);
                }
                throw failure;
            }
        }
    }

    @Test
    public void slowBookieRotationRejectsNewTicketsWithoutBlockingPermissionQueries() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger forces = new AtomicInteger();
        SegmentFileIo io = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (forces.incrementAndGet() == 5) {
                forcing.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Rotation was not released");
                    }
                } catch (InterruptedException interrupted) {
                    throw new AssertionError(interrupted);
                }
            }
        });
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(), 32768,
                        key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            log.submitRecoveryGrant(recoveryGrant(3, 3, 7, 9));
            FutureTask<SegmentControlRotation.Root> rotate = new FutureTask<>(() -> log.rotate(io));
            Thread thread = new Thread(rotate);
            thread.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(() ->
                        log.normalAdmission(context(), filled(20, (byte) 5)));
                new Thread(query).start();
                assertTrue(query.get(1, TimeUnit.SECONDS));
                assertTrue(log.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
                assertTrue(log.admitRead(context()).isEmpty());
                assertTrue(log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 5)).isEmpty());
                assertThrows(IOException.class, log::close);
                assertThrows(IOException.class, () -> log.submitBatch(List.of(tombstone(44, 4))));
            } finally {
                release.countDown();
                thread.join(5000);
            }
            assertEquals(2, rotate.get(1, TimeUnit.SECONDS).generation());
            log.admitNormal(context(), filled(20, (byte) 5)).orElseThrow().releaseUnclaimed();
            log.admitRead(context()).orElseThrow().finish();
            log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 5)).orElseThrow().finish();
        }
    }

    @Test
    public void bookieRotationHeaderAndThirdPageMatchIndependentByteVectors() throws Exception {
        SegmentControlCheckpoint.Reference first = new SegmentControlCheckpoint.Reference(1, 0, -1, 4096, 0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        SegmentControlCheckpoint.Reference second = new SegmentControlCheckpoint.Reference(2, 1, 0, 4304, 224,
                "8c106e348d1e37e23543ec08cd8063541cf9a7066f780044cee38051511e3a18");
        SegmentControlRotation.Root root = new SegmentControlRotation.Root(2, first, new UUID(1, 2),
                second, new UUID(3, 4));
        assertArrayEquals(checkpointVector("bookie-rotation-header-v1.hex"),
                SegmentControlRotation.header(SegmentControlFormat.Owner.BOOKIE, root).array());
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity())) {
            SegmentControlRotation.publish(config, SegmentControlFormat.Owner.BOOKIE, first, root,
                    new SegmentFileIo());
            try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, 0),
                    StandardOpenOption.READ)) {
                ByteBuffer page = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(file, page, 8192);
                assertArrayEquals(checkpointVector("bookie-rotation-publication-v1.hex"), page.array());
            }
            assertEquals(root, SegmentControlRotation.publication(config, SegmentControlFormat.Owner.BOOKIE, 0));
        }
    }

    @Test
    public void bookieEmptyCheckpointMatchesIndependentByteVectors() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentControlCheckpoint.Reference reference = log.checkpoint();
            assertArrayEquals(checkpointVector("bookie-empty-checkpoint-v2.hex"),
                    Files.readAllBytes(SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.BOOKIE, 0)));
            assertTrue(SegmentControlCheckpoint.read(config, identity(), SegmentControlFormat.Owner.BOOKIE,
                    reference, 1024 * 1024).compactedCheckpointHistory());
            SegmentControlCheckpoint.write(config, identity(), SegmentControlFormat.Owner.BOOKIE, reference,
                    new SegmentControlCheckpoint.Snapshot(-1, 4096, List.of()), new SegmentFileIo());
            assertArrayEquals(checkpointVector("bookie-empty-checkpoint-v1.hex"),
                    Files.readAllBytes(SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.BOOKIE, 0)));
            assertFalse(SegmentControlCheckpoint.read(config, identity(), SegmentControlFormat.Owner.BOOKIE,
                    reference, 1024 * 1024).compactedCheckpointHistory());
            try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, 0),
                    StandardOpenOption.READ)) {
                ByteBuffer page = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(file, page, 8192);
                assertArrayEquals(checkpointVector("bookie-checkpoint-publication-v1.hex"), page.array());
            }
            ByteBuffer encoded = SegmentControlFormat.encode(SegmentControlFormat.Type.BOOKIE_CHECKPOINT_COMMIT,
                    new Generation(1), 0, new UUID(1, 2), new Generation(1), 0, reference.encode());
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            assertArrayEquals(checkpointVector("bookie-checkpoint-commit-v1.hex"), bytes);
            SegmentControlParser.Command parsed = SegmentControlParser.parse(ByteBuffer.wrap(bytes),
                    SegmentControlFormat.Owner.BOOKIE, new Generation(1), 0);
            assertEquals(reference, SegmentControlCheckpoint.decodeReference(parsed.body()));
        }
    }

    private static byte[] checkpointVector(String name) throws IOException {
        try (InputStream input = SegmentBookieControlLogTest.class.getResourceAsStream(
                "/unified-wal-segment/" + name)) {
            assertTrue(input != null);
            String text = new String(input.readAllBytes(), StandardCharsets.US_ASCII);
            return HexFormat.of().parseHex(text.lines().filter(line -> !line.startsWith("#"))
                    .reduce("", String::concat));
        }
    }

    @Test
    public void bookieCheckpointPreservesPermissionsOutcomesLacAndClosedRecoveryGrant() throws Exception {
        for (boolean rotate : new boolean[] {false, true}) {
            IsolatedSegmentConfig config = checkpointConfig(
                    temporary.newFolder("permissions-" + rotate).toPath().resolve("bookie"));
            SegmentControlCheckpoint.Reference reference;
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                    SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                log.submitBatch(List.of(install(1, (byte) 5), activate(2),
                        install(45, 6, (byte) 7), activate(45, 7), tombstone(46, 8)));
                log.submitExplicitLac(writeLac(9, (byte) 5, (byte) 1));
                log.submitExplicitLac(writeLac(10, (byte) 5, (byte) 2));
                log.submitRecoveryGrant(recoveryGrant(3, 3, 7, 9));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.APPLIED_DURABLE,
                        log.submitRecoveryClose(recoveryClose(4, 3, 3, 7, 9)));
                log.submitFence(fence(5, (byte) 5));
                log.checkpoint();
                log.submitBatch(List.of(tombstone(45, 9), activate(10)));
                reference = rotate ? log.rotate().second() : log.checkpoint();
                byte[] image = Files.readAllBytes(SegmentControlCheckpoint.path(
                        config, SegmentControlFormat.Owner.BOOKIE, reference.slot()));
                assertFalse(HexFormat.of().formatHex(image).contains("05".repeat(20)));
            }
            assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(),
                    1024 * 1024, key((byte) 4), VALIDATOR));
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                    1024 * 1024, key((byte) 3), VALIDATOR)) {
                assertEquals(reference, log.publishedCheckpoint().orElseThrow());
                assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
                assertTrue(log.admitRead(context(45)).isEmpty());
                log.admitRead(context()).orElseThrow().finish();
                assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                        log.submitBatch(List.of(install(1, (byte) 5))).get(0).status());
                assertEquals(SegmentBookieControlLog.Status.INCOMPATIBLE,
                        log.submitBatch(List.of(install(1, (byte) 6))).get(0).status());
                assertEquals(List.of(SegmentBookieControlLog.Status.CONDITION_FAILED,
                        SegmentBookieControlLog.Status.CONDITION_FAILED),
                        log.submitBatch(List.of(activate(10), install(46, 11, (byte) 5))).stream()
                                .map(SegmentBookieControlLog.Result::status).toList());
                assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                        log.submitFence(fence(5, (byte) 5)).status());
                assertEquals(10, log.explicitLac(context()).orElseThrow().lac());
                assertArrayEquals(new byte[] {2}, log.explicitLac(context()).orElseThrow().body());
                assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                        log.submitExplicitLac(writeLac(10, (byte) 5, (byte) 2)));
                assertEquals(SegmentBookieControlLog.RecoveryCloseStatus.ALREADY_DURABLE,
                        log.submitRecoveryClose(recoveryClose(4, 3, 3, 7, 9)));
                assertTrue(log.admitRecovery(recoveryAdd(3, 7, 9, 2, (byte) 5)).isEmpty());
                assertEquals(SegmentBookieControlLog.Status.ALREADY_DURABLE,
                        log.submitRecoveryGrant(recoveryGrant(3, 3, 7, 9)).status());
            }
        }
    }

    @Test
    public void bookieCheckpointCapturesAppliedCutWhileTombstoneIsDurableButUnapplied() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo delayed = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            forcing.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Checkpoint force was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentBookieControlLog.DurableBatch pending = log.appendDurably(List.of(tombstone(44, 3)));
            assertEquals(2, pending.firstSequence());
            FutureTask<SegmentControlCheckpoint.Reference> task = new FutureTask<>(() -> log.checkpoint(delayed));
            Thread writer = new Thread(task);
            writer.start();
            try {
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                FutureTask<Boolean> query = new FutureTask<>(
                        () -> log.normalAdmission(context(), filled(20, (byte) 5)));
                new Thread(query).start();
                assertFalse(query.get(1, TimeUnit.SECONDS));
                assertThrows(IOException.class, log::close);
                assertThrows(IOException.class, log::checkpoint);
                log.applyDurable(pending);
            } finally {
                release.countDown();
                writer.join(5000);
            }
            assertFalse(writer.isAlive());
            assertEquals(1, task.get(1, TimeUnit.SECONDS).through());
        }
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(1, log.publishedCheckpoint().orElseThrow().through());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertTrue(log.admitRead(context()).isEmpty());
        }
    }

    @Test
    public void bookieAndArenaPublicationsPreserveEachOtherAndTornBookiePageUsesItsOtherSlot() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlCheckpoint.Reference arenaReference;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentControlCheckpoint.Reference first = bookie.checkpoint();
            arena.checkpoint();
            assertEquals(first, SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.BOOKIE, 0));
            arenaReference = arena.checkpoint();
            bookie.checkpoint();
            assertEquals(arenaReference,
                    SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.ARENA, 1));
        }
        try (FileChannel file = FileChannel.open(SegmentControlCheckpoint.superblock(config, 1),
                StandardOpenOption.WRITE)) {
            file.truncate(12287);
            file.force(true);
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(arenaReference, arena.publishedCheckpoint().orElseThrow());
            assertEquals(1, bookie.publishedCheckpoint().orElseThrow().generation());
            assertTrue(bookie.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(3, bookie.checkpoint().generation());
            assertEquals(arenaReference,
                    SegmentControlCheckpoint.publication(config, SegmentControlFormat.Owner.ARENA, 1));
        }
    }

    @Test
    public void bookieCheckpointFailureDoesNotAcknowledgePublicationAndRecoverySyncsCompletePage() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentControlCheckpoint.Reference first;
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            first = log.checkpoint();
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, 1, -1, false)));
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertTrue(log.normalAdmission(context(), filled(20, (byte) 5)));
            log.submitFence(fence(3, (byte) 5));
            assertThrows(IOException.class, () -> log.checkpoint(
                    new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false)));
            assertEquals(first, log.publishedCheckpoint().orElseThrow());
            assertThrows(IOException.class, () -> log.submitBatch(List.of(activate(4))));
        }
        try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(2, log.publishedCheckpoint().orElseThrow().generation());
            assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
            assertEquals(3, log.checkpoint().generation());
        }
    }

    @Test
    public void bookieCheckpointFallbackRequiresEveryRetainedControlByte() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            log.checkpoint();
            log.submitFence(fence(3, (byte) 5));
            log.checkpoint();
            log.submitBatch(List.of(tombstone(44, 4)));
        }
        for (int slot : new int[] {1, 0}) {
            Path image = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.BOOKIE, slot);
            byte[] bytes = Files.readAllBytes(image);
            bytes[4096] ^= 1;
            Files.write(image, bytes);
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                    1024 * 1024, key((byte) 3), VALIDATOR)) {
                assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
                assertTrue(log.admitRead(context()).isEmpty());
                assertEquals(slot == 1, log.publishedCheckpoint().isPresent());
            }
        }
        Path control = config.directory().resolve("bookie-control");
        byte[] original = Files.readAllBytes(control);
        Files.write(control, Arrays.copyOf(original, original.length - 1));
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(),
                1024 * 1024, key((byte) 3), VALIDATOR));
        original[4096 + 128] ^= 1;
        Files.write(control, original);
        assertThrows(IOException.class, () -> SegmentBookieControlLog.open(config, identity(),
                1024 * 1024, key((byte) 3), VALIDATOR));
    }

    @Test
    public void realBookieCheckpointProcessExitAtEachPublicationBarrierRetainsFence() throws Exception {
        for (String phase : List.of("payload", "commit", "publication", "done")) {
            IsolatedSegmentConfig config = checkpointConfig(temporary.getRoot().toPath().resolve(phase));
            try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                    SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                log.submitBatch(List.of(install(1, (byte) 5), activate(2)));
                log.checkpoint();
            }
            Path output = temporary.getRoot().toPath().resolve(phase + ".log");
            Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx64m", "-XX:MaxDirectMemorySize=32m", "-cp", System.getProperty("java.class.path"),
                    BookieCheckpointCrashProbe.class.getName(), phase, config.directory().toString())
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            try {
                assertTrue(child.waitFor(15, TimeUnit.SECONDS));
                assertEquals(Files.readString(output), 23, child.exitValue());
            } finally {
                if (child.isAlive()) {
                    child.destroyForcibly();
                    assertTrue(child.waitFor(5, TimeUnit.SECONDS));
                }
            }
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(config, identity(),
                    1024 * 1024, key((byte) 3), VALIDATOR)) {
                assertFalse(log.normalAdmission(context(), filled(20, (byte) 5)));
                assertEquals(SegmentBookieControlLog.FenceStatus.ALREADY_DURABLE,
                        log.submitFence(fence(3, (byte) 5)).status());
                assertEquals(phase.equals("payload") || phase.equals("commit") ? 1 : 2,
                        log.publishedCheckpoint().orElseThrow().generation());
            }
        }
    }

    private static IsolatedSegmentConfig checkpointConfig(Path directory) {
        return new IsolatedSegmentConfig(directory, 1, 4, 1, 4L * 1024 * 1024,
                4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    public static final class BookieCheckpointCrashProbe {
        public static void main(String[] args) throws Exception {
            String phase = args[0];
            AtomicInteger controlForces = new AtomicInteger();
            AtomicInteger checkpointForces = new AtomicInteger();
            SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                if (controlForces.incrementAndGet() == 2 && phase.equals("commit")) {
                    Runtime.getRuntime().halt(23);
                }
            });
            SegmentFileIo checkpointIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                int count = checkpointForces.incrementAndGet();
                if ((count == 1 && phase.equals("payload")) || (count == 2 && phase.equals("publication"))) {
                    Runtime.getRuntime().halt(23);
                }
            });
            SegmentFileHeader identity = new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 7)),
                    new UUID(0, 9), BookieId.parse("isolated:3181"));
            try (SegmentBookieControlLog log = SegmentBookieControlLog.open(
                    checkpointConfig(Path.of(args[1])), identity,
                    1024 * 1024, key((byte) 3), VALIDATOR, controlIo)) {
                log.submitFence(fence(3, (byte) 5));
                log.checkpoint(checkpointIo);
                Runtime.getRuntime().halt(23);
            }
        }
    }

    @Test
    public void bothControlCheckpointsPreserveTheStorageGraphReadDuplicateAndAppendAcrossRestart() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("checkpoint"),
                1, 4, 1, 16L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2));
            storageWrite(storage, context(44), 1);
        }
        for (int cycle : new int[] {1, 2}) {
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                assertFalse(arena.hasUnclosedDataSession());
                assertEquals(cycle + 1, arena.rotate().generation());
                assertEquals(cycle + 1, bookie.rotate().generation());
            }
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16)) {
                storage.restart();
                SegmentDataPhysicalDriver.NormalResult duplicate = storageWrite(storage, context(44), 1);
                assertEquals(1, duplicate.candidate().address().sequence());
                ReadReply old = storageRead(storage, context(44), 1);
                try (SegmentPointRead.LeasedResult result = old.result()) {
                    assertEquals(1, result.result().record().entryId());
                }
                SegmentDataPhysicalDriver.NormalResult added = storageWrite(storage, context(44), cycle + 1);
                assertEquals(cycle + 1, added.candidate().address().sequence());
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(4, arena.publishedCheckpoint().orElseThrow().generation());
            assertEquals(3, arena.dataSessionEpoch());
            assertFalse(arena.hasUnclosedDataSession());
        }
    }

    @Test
    public void storageGraphBootstrapsAllShardsAndRestartsLocalResults() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve("bookie"),
                1, 4, 1, 16L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16)) {
            assertThrows(IllegalStateException.class, storage::controls);
            assertFalse(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                    (status, result, cause) -> {
                        throw new AssertionError("Storage was not started");
                    })));
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(45, 3, (byte) 5), activate(45, 4));
            storageWrite(storage, context(44), 1);
            storageWrite(storage, context(45), 1);
        }
        for (int cycle : new int[] {1, 2}) {
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16)) {
                storage.restart();
                for (long ledgerId : new long[] {44, 45}) {
                    SegmentDataPhysicalDriver.NormalResult duplicate = storageWrite(storage, context(ledgerId), 1);
                    assertEquals(1, duplicate.candidate().address().sequence());
                    SegmentDataPhysicalDriver.NormalResult added = storageWrite(storage, context(ledgerId), cycle + 1);
                    assertEquals(cycle + 1, added.candidate().address().sequence());
                    ReadReply old = storageRead(storage, context(ledgerId), 1);
                    try (SegmentPointRead.LeasedResult result = old.result()) {
                        assertEquals(1, result.result().record().entryId());
                    }
                    ReadReply next = storageRead(storage, context(ledgerId), cycle + 1);
                    try (SegmentPointRead.LeasedResult result = next.result()) {
                        assertEquals(cycle + 1, result.result().record().entryId());
                    }
                }
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(3, arena.dataSessionEpoch());
            assertFalse(arena.hasUnclosedDataSession());
        }
    }

    @Test
    public void storageGraphRetainsLocksUntilConsumerReturnsReadLease() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 2, 16)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            storageWrite(storage, context(), 1);
            SegmentBookieControlDriver controls = storage.controls();
            ReadReply read = storageRead(storage, context(), 1);
            try (SegmentPointRead.LeasedResult result = read.result()) {
                assertFalse(storage.close(Duration.ofMillis(50)));
                assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
                assertThrows(IllegalStateException.class, storage::controls);
                assertFalse(controls.submitBatch(List.of(activateReplacement(44, 3)), (replies, cause) -> {
                    throw new AssertionError("Stopped storage accepted a retained control driver request");
                }));
                ByteBuf bytes = entry(context(), 2);
                try {
                    assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                            new SegmentDataPhysicalDriver.NormalRequest((reply, cause) -> {
                                throw new AssertionError("Stopped storage accepted an entry");
                            })));
                    assertEquals(1, bytes.refCnt());
                } finally {
                    bytes.release();
                }
                assertFalse(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                        (status, reply, cause) -> {
                            throw new AssertionError("Stopped storage accepted a read");
                        })));
                assertEquals(1, result.result().record().entryId());
            }
            assertTrue(storage.close(Duration.ofSeconds(5)));
            assertTrue(storage.close(Duration.ofSeconds(5)));
        }
        try (SegmentDataFileLock guard = SegmentDataFileLock.open(config.directory());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertFalse(arena.hasUnclosedDataSession());
        }
    }

    @Test
    public void storageGraphDrainsOwnerCompletionWithoutBlockingCallbackReentry() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch completionEntered = new CountDownLatch(1);
        CountDownLatch releaseCompletion = new CountDownLatch(1);
        CompletableFuture<Void> completed = new CompletableFuture<>();
        try (IsolatedSegmentStorage storage = storage(config, 1, 2, 16)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            ByteBuf bytes = entry(context(), 1);
            try {
                assertTrue(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            completionEntered.countDown();
                            try {
                                await(releaseCompletion);
                                assertNull(cause);
                                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                                assertThrows(IllegalStateException.class, () -> storage.close(Duration.ofSeconds(1)));
                                assertFalse(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                                        (status, reply, failure) -> {
                                            throw new AssertionError("Drain accepted callback reentry");
                                        })));
                                completed.complete(null);
                            } catch (Throwable failure) {
                                completed.completeExceptionally(failure);
                            }
                        })));
            } finally {
                bytes.release();
            }
            assertTrue(completionEntered.await(5, TimeUnit.SECONDS));
            assertFalse(storage.close(Duration.ofMillis(50)));
            assertThrows(IOException.class, () -> SegmentDataFileLock.open(config.directory()));
            FutureTask<Boolean> drain = new FutureTask<>(() -> storage.close(Duration.ofSeconds(10)));
            Thread closer = new Thread(drain, "isolated-storage-close-test");
            closer.start();
            try {
                // The external drain must already be waiting for this callback before it reenters close.
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (closer.getState() != Thread.State.TIMED_WAITING && closer.isAlive()
                        && System.nanoTime() < end) {
                    TimeUnit.MILLISECONDS.sleep(1);
                }
                assertEquals(Thread.State.TIMED_WAITING, closer.getState());
                releaseCompletion.countDown();
                completed.get(5, TimeUnit.SECONDS);
                assertTrue(drain.get(5, TimeUnit.SECONDS));
            } finally {
                releaseCompletion.countDown();
                closer.join(11000);
                assertFalse(closer.isAlive());
            }
        } finally {
            releaseCompletion.countDown();
        }
    }

    @Test
    public void storageGraphCleansFailedBootstrapAndEnforcesPartitionedHotCapacity() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage invalid = storage(config, 1, 1, 16)) {
            assertThrows(IOException.class, invalid::startNew);
            assertThrows(IllegalStateException.class, invalid::controls);
            assertTrue(invalid.close(Duration.ofSeconds(5)));
        }
        try (SegmentDataFileLock guard = SegmentDataFileLock.open(config.directory());
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertFalse(arena.hasUnclosedDataSession());
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 2, 16)) {
            storage.restart();
            installStorage(storage, install(45, 1, (byte) 5), activate(45, 2));
            storageWrite(storage, context(45), 1);
            // The local-result callback may precede return of the sole DATA failure-notification slot.
            ownerBarrier(storageOwner(storage, 1));
            storageWrite(storage, context(45), 2);
        }
        // Four global slots become one per shard; two entries on shard 1 cannot fit its partition.
        try (IsolatedSegmentStorage tooSmall = storage(config, 2, 6, 4)) {
            assertThrows(IOException.class, tooSmall::restart);
            assertThrows(IllegalStateException.class, tooSmall::controls);
            assertTrue(tooSmall.close(Duration.ofSeconds(5)));
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16)) {
            storage.restart();
            ReadReply old = storageRead(storage, context(45), 2);
            try (SegmentPointRead.LeasedResult result = old.result()) {
                assertEquals(2, result.result().record().entryId());
            }
            storageWrite(storage, context(45), 3);
        }
    }

    @Test
    public void storageGraphRejectsInvalidInputsWithoutLeakingAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 2, 16)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            ByteBuf bytes = entry(context(), 1);
            try {
                assertThrows(NullPointerException.class, () -> storage.submitNormal(
                        context(), filled(20, (byte) 5), bytes, null));
                assertThrows(NullPointerException.class, () -> storage.submitRecovery(
                        recoveryAddWithEntry(ByteBuffer.wrap(new byte[48])), null));
                bytes.setByte(bytes.writerIndex() - 1, 1);
                assertThrows(IllegalArgumentException.class,
                        () -> storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            throw new AssertionError("Invalid entry was accepted");
                        })));
                assertEquals(1, bytes.refCnt());
            } finally {
                bytes.release();
            }
            storageWrite(storage, context(), 1);
        }
    }

    @Test
    public void storageGraphSubmitsRecoveryWithIndependentAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 2, 16)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 6));
            CompletableFuture<List<SegmentBookieControlLog.Result>> grant = new CompletableFuture<>();
            assertTrue(storage.controls().submitRecoveryGrant(recoveryGrant(2, 3, 7, 9), (results, cause) -> {
                if (cause == null) {
                    grant.complete(results);
                } else {
                    grant.completeExceptionally(cause);
                }
            }));
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    grant.get(5, TimeUnit.SECONDS).get(0).status());
            ByteBuf bytes = entry(context(), 2);
            try {
                assertFalse(storage.submitNormal(context(), filled(20, (byte) 6), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            throw new AssertionError("Recovery grant activated normal writing");
                        })));
                CompletableFuture<SegmentDataPhysicalDriver.RecoveryResult> completed = new CompletableFuture<>();
                assertTrue(storage.submitRecovery(recoveryAddWithEntry(bytes.nioBuffer()),
                        new SegmentDataPhysicalDriver.RecoveryRequest((result, cause) -> {
                            if (cause == null) {
                                completed.complete(result);
                            } else {
                                completed.completeExceptionally(cause);
                            }
                        })));
                assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE,
                        completed.get(5, TimeUnit.SECONDS).status());
                ReadReply old = storageRead(storage, context(), 2);
                try (SegmentPointRead.LeasedResult result = old.result()) {
                    assertEquals(2, result.result().record().entryId());
                }
            } finally {
                bytes.release();
            }
        }
    }

    @Test
    public void failedControlIoBeforeDriverClaimReleasesOuterAdmission() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(config, identity());
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR,
                        new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false))) {
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentBookieControlLog.Admission admission = bookie.admitNormal(
                    context(), filled(20, (byte) 5)).orElseThrow();
            assertThrows(IOException.class, () -> bookie.submitBatch(List.of(activateReplacement(44, 3))));
            assertThrows(IOException.class, () -> bookie.claimForDriver(List.of(admission)));
            admission.releaseUnclaimed();
            bookie.close();
        }
        IsolatedSegmentConfig recoveryConfig = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("failed-recovery-handoff"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (IsolatedSegmentFiles ignored = IsolatedSegmentFiles.create(recoveryConfig, identity());
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(recoveryConfig, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR,
                        new SegmentFileIo(Integer.MAX_VALUE, -1, 3, false))) {
            bookie.submitBatch(List.of(install(1, (byte) 6)));
            bookie.submitRecoveryGrant(recoveryGrant(2, 3, 7, 9));
            SegmentBookieControlLog.RecoveryAdmission admission = bookie.admitRecovery(
                    recoveryAdd(3, 7, 9, 2, (byte) 6)).orElseThrow();
            assertThrows(IOException.class, () -> bookie.submitBatch(List.of(activateReplacement(44, 3))));
            assertThrows(IOException.class, () -> bookie.claimRecoveryForDriver(List.of(admission)));
            admission.releaseUnclaimed();
            bookie.close();
        }
    }

    @Test
    public void cleanRestartRestoresOwnerPrefixAndDuplicateAuthorityBeforeAppending() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("clean-local-result-restart"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        runAbruptWriter("normal-clean-close", config.directory(), 0, "SESSION_CLOSED");
        for (int cycle : new int[] {1, 2}) {
            SegmentFileIo recoveryIo = new SegmentFileIo();
            SegmentFileIo controlIo = new SegmentFileIo();
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024, controlIo);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                            config, identity(), arena, bookie, recoveryIo, 4, 4)) {
                assertEquals(1, recoveryIo.forces);
                assertEquals(cycle + 1, arena.dataSessionEpoch());
                assertTrue(files.supportsLocalResults());
                long floor = 4096 + cycle * SegmentArenaControlLog.BLOCK_BYTES;
                assertEquals(floor, files.nextOffset(0));
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
                List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                try {
                    FutureTask<SegmentDataPhysicalDriver> boot = new FutureTask<>(() -> {
                        if (cycle == 1) {
                            SegmentHotDirectory rejected = new SegmentHotDirectory(owner, config, identity(), 4);
                            assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(
                                    0, owner, group.io(), files, arena,
                                    new SegmentPhysicalPrefix(owner, 0, new Generation(2), 1, 4), rejected));
                            assertEquals(0, rejected.candidateCount());
                        } else {
                            assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(
                                    0, owner, group.io(), files, arena, prefix,
                                    new SegmentHotDirectory(owner, config, identity(), 1)));
                            assertEquals(0, prefix.durableThrough());
                        }
                        SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(
                                0, owner, group.io(), files, arena, prefix, hot);
                        assertEquals(cycle, prefix.durableThrough());
                        assertEquals(cycle, hot.candidateCount());
                        assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(
                                0, owner, group.io(), files, arena,
                                new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4),
                                new SegmentHotDirectory(owner, config, identity(), 4)));
                        return driver;
                    });
                    assertTrue(owner.control(boot));
                    SegmentDataPhysicalDriver driver = boot.get(5, TimeUnit.SECONDS);
                    SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
                    SegmentDataPhysicalDriver.NormalBatcher batcher = refillingBatcher(
                            driver, owner, bookie, allocator, 1);
                    ownerBarrier(owner);
                    CountDownLatch duplicate = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 1),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, duplicate)));
                    assertTrue(duplicate.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                    assertEquals(4096, results.get(0).candidate().address().offset());
                    assertEquals(0, files.dataIo.forces);
                    assertEquals(1, controlIo.forces);
                    assertEquals(floor, files.nextOffset(0));
                    CountDownLatch conflict = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 1, -1, 8, (byte) 7, 8),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, conflict)));
                    assertTrue(conflict.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.CONFLICT, results.get(1).status());
                    assertEquals(0, files.dataIo.forces);
                    assertEquals(floor, files.nextOffset(0));
                    CountDownLatch appended = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), cycle + 1),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, appended)));
                    assertTrue(appended.await(5, TimeUnit.SECONDS));
                    ownerBarrier(owner);
                    SegmentDataPhysicalDriver.NormalResult next = results.get(2);
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, next.status());
                    assertEquals(cycle + 1, next.candidate().address().sequence());
                    assertEquals(new Generation(1), next.candidate().address().streamGeneration());
                    assertEquals(floor, next.candidate().address().offset());
                    assertEquals(cycle + 1, SegmentPointRead.read(config, identity(), arena, next.candidate())
                            .record().entryId());
                    assertEquals(1, SegmentPointRead.read(config, identity(), arena, results.get(0).candidate())
                            .record().entryId());
                    assertEquals(1, files.dataIo.forces);
                    assertEquals(2, controlIo.forces);
                    assertNull(failure.get());
                    assertEquals(0, group.inputs().usedBytes());
                } finally {
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
                assertNull(owner.failure());
                files.close();
                assertFalse(arena.hasUnclosedDataSession());
                assertEquals(3, controlIo.forces);
            }
        }
    }

    @Test
    public void cleanRestartSyncFailureAndChangedBookieCutDoNotPublishAnOwner() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("clean-restart-failure"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        runAbruptWriter("normal-clean-close", config.directory(), 0, "SESSION_CLOSED");
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, failedSync, 4, 4));
            assertEquals(1, failedSync.forces);
            assertEquals(1, arena.dataSessionEpoch());
            assertFalse(arena.hasUnclosedDataSession());
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, new SegmentFileIo(), 4, 4)) {
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, 2, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                try {
                    FutureTask<Void> boot = new FutureTask<>(() -> {
                        SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4);
                        SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
                        assertThrows(IllegalArgumentException.class, () -> new SegmentDataPhysicalDriver(
                                0, owner, group.io(), files, arena, prefix, hot));
                        assertEquals(0, prefix.durableThrough());
                        assertEquals(0, hot.candidateCount());
                        return null;
                    });
                    assertTrue(owner.control(boot));
                    boot.get(5, TimeUnit.SECONDS);
                } finally {
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
                assertNull(owner.failure());
            }
        }
    }

    @Test
    public void localResultRestartRefusesLegacyHistoryAndMultipleUnretiredStreams() throws Exception {
        for (boolean legacy : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("restart-unsupported-history-" + legacy), 1, 4, 1,
                    4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            runAbruptWriter(legacy ? "normal-after-force" : "normal-clean-close", config.directory(), 0,
                    legacy ? "LOCAL_DURABLE" : "SESSION_CLOSED");
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                if (!legacy) {
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, arena.submitBatch(List.of(
                            new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC, new UUID(0, 6),
                                    new Generation(1), 0, 0, 0, new Generation(2), 6,
                                    4096 + SegmentArenaControlLog.BLOCK_BYTES, 1, 1))).get(0).status());
                }
                SegmentFileIo syncIo = new SegmentFileIo();
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                        config, identity(), arena, bookie, syncIo, 4, 4));
                assertEquals(0, syncIo.forces);
                assertEquals(legacy ? 0 : 1, arena.dataSessionEpoch());
                assertFalse(arena.hasUnclosedDataSession());
            }
        }
    }

    @Test
    public void unclosedSessionRefusesRestartAfterAbruptDataFailureWithoutQuarantineRecord() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("session-abrupt-force-failure"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        runAbruptWriter("session-force-error", config.directory(), 86, "DATA_FORCE_FAILED");
        try (SegmentArenaControlLog replay = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            assertEquals(1, replay.dataSessionEpoch());
            assertTrue(replay.hasUnclosedDataSession());
            assertFalse(replay.hasDurableQuarantine(0));
            assertFalse(replay.dataFileAvailable(0));
            assertTrue(replay.hasAppliedGrant(0, 4096, 4096, 0, 1, 5, 1));
            SegmentFileIo syncIo = new SegmentFileIo();
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), replay, bookie, syncIo, 4, 4));
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity(), replay, syncIo));
            assertEquals(0, syncIo.forces);
            try (java.nio.channels.FileChannel data = java.nio.channels.FileChannel.open(
                    IsolatedSegmentFiles.dataPath(config, 0), java.nio.file.StandardOpenOption.WRITE)) {
                data.force(false);
            }
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnlyWithInventory(
                    config, identity(), replay, syncIo, 1, 1));
            assertEquals(0, syncIo.forces);
            assertEquals(config.bytesPerFile(), Files.size(IsolatedSegmentFiles.dataPath(config, 0)));
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
        if (args.length == 3 && "hold-data-lock".equals(args[0])) {
            try (SegmentDataFileLock held = SegmentDataFileLock.open(Path.of(args[1]))) {
                Files.writeString(Path.of(args[2]), "DATA_SCOPE_HELD");
                System.in.read();
            }
            return;
        }
        if (args.length != 2 || (!"after-force".equals(args[0]) && !"before-force".equals(args[0])
                && !"normal-after-force".equals(args[0]) && !"normal-before-force".equals(args[0])
                && !"append-after-restart".equals(args[0]) && !"session-force-error".equals(args[0])
                && !"normal-clean-close".equals(args[0]))) {
            throw new IllegalArgumentException("Expected abrupt writer phase and directory");
        }
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(Path.of(args[1]), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        SegmentFileHeader identity = new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 7)),
                new UUID(0, 9), BookieId.parse("isolated:3181"));
        if ("session-force-error".equals(args[0])) {
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity);
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024)) {
                files.startSession(arena);
                grant(arena, 1);
                files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                try {
                    files.writeAuthorized(batch(config, 1, 1, 4096), arena);
                    throw new AssertionError("Injected DATA force did not fail");
                } catch (IOException expected) {
                    System.out.println("DATA_FORCE_FAILED");
                    System.out.flush();
                    Runtime.getRuntime().halt(86);
                }
            }
        }
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
                if ("normal-clean-close".equals(args[0])) {
                    files.startSession(arena);
                }
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
                if ("normal-clean-close".equals(args[0])) {
                    owner.stopAdmission();
                    if (!owner.awaitTermination(Duration.ofSeconds(5))) {
                        throw new IllegalStateException("Normal DATA owner did not drain");
                    }
                    io.shutdown();
                    if (!io.awaitTermination(Duration.ofSeconds(5))) {
                        throw new IllegalStateException("Normal DATA I/O did not drain");
                    }
                    files.close();
                    System.out.println("SESSION_CLOSED");
                } else {
                    System.out.println("LOCAL_DURABLE");
                }
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

    private void createIndexData(IsolatedSegmentConfig config) throws Exception {
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(
                        config, identity(), 1024 * 1024, key((byte) 3), VALIDATOR)) {
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(45, 3, (byte) 5), activate(45, 4)));
            files.startSession(arena);
            for (int shard = 0; shard < 2; shard++) {
                long offset = 4096L + shard * 1024 * 1024;
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                        new UUID(0, 5 + shard), new Generation(1), 0, 0, shard, new Generation(1),
                        5 + shard, offset, 1, 11 + shard)));
                ByteBuf normal = Unpooled.wrappedBuffer(entryBytes(context(44 + shard), 1, -1, 8, (byte) 0));
                ByteBuf copy = Unpooled.wrappedBuffer(entryBytes(context(44 + shard), 2, -1, 8, (byte) 0));
                try {
                    List<SegmentBatchFormat.Data> records = shard == 0
                            ? List.of(SegmentBatchFormat.entry(context(44), normal),
                                    SegmentBatchFormat.relocation(context(44), copy, new UUID(3, 4), new Generation(1)))
                            : List.of(SegmentBatchFormat.entry(context(45), normal));
                    files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                            0, shard, 5 + shard, new Generation(1), new Generation(1), 11 + shard, offset),
                            records), arena);
                } finally {
                    normal.release();
                    copy.release();
                }
            }
            files.close();
        }
    }

    private static final class IndexFixture implements AutoCloseable {
        private SegmentDataFileLock guard;
        private SegmentArenaControlLog arena;
        private SegmentBookieControlLog bookie;
        private SegmentDataRecovery.PhysicalInventory inventory;

        private IndexFixture(IsolatedSegmentConfig config, SegmentFileHeader identity) throws IOException {
            try {
                guard = SegmentDataFileLock.open(config.directory());
                arena = SegmentArenaControlLog.open(config, identity, 1024 * 1024);
                bookie = SegmentBookieControlLog.open(config, identity, 1024 * 1024, key((byte) 3), VALIDATOR);
                inventory = SegmentDataRecovery.recoverLocallyReadableInventoryWhileLocked(config, identity,
                        arena, bookie, new SegmentFileIo(), 16, 4, guard.lock());
            } catch (IOException failure) {
                try {
                    close();
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }

        @Override
        public void close() throws IOException {
            IOException first = null;
            for (AutoCloseable resource : new AutoCloseable[] {bookie, arena, guard}) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (Exception failure) {
                        IOException cause = failure instanceof IOException io ? io : new IOException(failure);
                        if (first == null) {
                            first = cause;
                        } else {
                            first.addSuppressed(cause);
                        }
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }

    @Test
    public void derivedIndexPutIsQueryableButCoverageRequiresActualFlushAndPublication() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        SegmentDerivedIndex.Coverage coverage;
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertTrue(index.coverage().isEmpty());
            index.replace(source.inventory, source.arena, source.bookie);
            assertEquals(source.inventory.entries().get(0), index.get(context(), 1).orElseThrow());
            assertTrue(index.get(context(), 2).isEmpty()); // Uncommitted relocation never becomes ordinary DATA.
            assertTrue(index.coverage().isEmpty());
            coverage = index.checkpoint(new SegmentFileIo(13, -1, -1, false));
            assertEquals(2, coverage.entries());
            assertEquals(List.of(new SegmentDataRecovery.StreamCoverage(0, new Generation(1), 11, 11),
                    new SegmentDataRecovery.StreamCoverage(1, new Generation(1), 12, 12)), coverage.streams());
            assertEquals(source.arena.appliedThrough(), coverage.arenaCut());
            assertEquals(source.bookie.recoveryReadCut(config, identity()), coverage.bookieCut());
            assertEquals(coverage, index.coverage().orElseThrow());
            assertEquals(1, SegmentPointRead.read(config, identity(), source.arena,
                    index.get(context(45), 1).orElseThrow()).record().entryId());
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertEquals(coverage, index.coverage().orElseThrow());
            assertTrue(index.get(context(45), 1).isPresent());
        }
    }

    @Test
    public void derivedIndexReplacementRetiresOldCoverageBeforeDestructiveEdits() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            SegmentDerivedIndex.Coverage first = index.rebuild(source.inventory, source.arena, source.bookie);
            index.replace(source.inventory, source.arena, source.bookie);
            assertTrue(index.coverage().isEmpty());
            SegmentDerivedIndex.Coverage next = index.checkpoint(new SegmentFileIo());
            assertFalse(first.indexGeneration().equals(next.indexGeneration()));
            assertEquals(2, next.entries());
        }
    }

    @Test
    public void derivedIndexRejectsWrongScopeStaleScanAndNativeLockConflict() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            index.rebuild(source.inventory, source.arena, source.bookie);
            assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, identity(), 16));
            assertTrue(index.coverage().isPresent());
            source.arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC,
                    new UUID(0, 19), new Generation(1), 0, 0, 0, new Generation(1),
                    19, 4096 + 2L * 1024 * 1024, 1, 11)));
            assertThrows(IOException.class, () -> index.replace(source.inventory, source.arena, source.bookie));
            assertTrue(index.get(context(), 1).isPresent());
        }
        SegmentFileHeader foreign = new SegmentFileHeader(new StorageIncarnation(filled(16, (byte) 8)),
                identity().arenaId(), identity().bookieId());
        assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, foreign, 16));
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 1)) {
            assertThrows(IOException.class, () -> index.replace(source.inventory, source.arena, source.bookie));
        }
    }

    @Test
    public void derivedIndexCoverageChecksActualContentsAndSurvivingPublication() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            index.rebuild(source.inventory, source.arena, source.bookie);
            assertEquals(2, index.checkpoint(new SegmentFileIo()).publication());
        }
        byte[] later = Files.readAllBytes(SegmentDerivedIndex.publication(config, 1));
        later[0] ^= 1;
        Files.write(SegmentDerivedIndex.publication(config, 1), later);
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertEquals(1, index.coverage().orElseThrow().publication());
        }
        try (org.rocksdb.Options options = new org.rocksdb.Options();
                org.rocksdb.RocksDB db = org.rocksdb.RocksDB.open(options,
                        SegmentDerivedIndex.directory(config).toString());
                org.rocksdb.FlushOptions flush = new org.rocksdb.FlushOptions().setWaitForFlush(true)) {
            db.delete(SegmentDerivedIndex.key(context(), 1));
            db.flush(flush);
        }
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertTrue(index.coverage().isEmpty());
            assertTrue(index.get(context(), 1).isEmpty());
            index.rebuild(source.inventory, source.arena, source.bookie);
            assertTrue(index.get(context(), 1).isPresent());
        }
    }

    @Test
    public void deletedDerivedDatabaseRebuildsFromDataWithoutRevivingTombstones() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        UUID old;
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            old = index.rebuild(source.inventory, source.arena, source.bookie).indexGeneration();
            source.bookie.submitBatch(List.of(tombstone(44, 9)));
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(SegmentDerivedIndex.directory(config))) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
        try (IndexFixture source = new IndexFixture(config, identity());
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertTrue(index.coverage().isEmpty()); // Old pages cannot cover a newly created database.
            SegmentDerivedIndex.Coverage rebuilt = index.rebuild(source.inventory, source.arena, source.bookie);
            assertFalse(old.equals(rebuilt.indexGeneration()));
            assertEquals(1, rebuilt.entries());
            assertTrue(index.get(context(), 1).isEmpty());
            assertTrue(index.get(context(45), 1).isPresent());
            assertEquals(1, SegmentPointRead.read(config, identity(), source.arena,
                    index.get(context(45), 1).orElseThrow()).record().entryId());
        }
    }

    @Test
    public void derivedCoverageWriteOrForceFailureNeverReportsCheckpointSuccess() throws Exception {
        IsolatedSegmentConfig config = config();
        createIndexData(config);
        for (boolean partial : new boolean[] {true, false}) {
            try (IndexFixture source = new IndexFixture(config, identity());
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
                index.replace(source.inventory, source.arena, source.bookie);
                SegmentFileIo broken = new SegmentFileIo(13, partial ? 2 : -1, partial ? -1 : 1, false);
                assertThrows(IOException.class, () -> index.checkpoint(broken));
                assertThrows(IOException.class, () -> index.get(context(), 1));
            }
            try (IndexFixture source = new IndexFixture(config, identity());
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
                assertEquals(!partial, index.coverage().isPresent());
                index.rebuild(source.inventory, source.arena, source.bookie);
                assertTrue(index.get(context(), 1).isPresent());
            }
        }
    }

    @Test
    public void realDerivedIndexProcessExitBeforeFlushAndAtCoveragePublicationRebuildsData() throws Exception {
        for (String phase : List.of("put", "publication", "done")) {
            IsolatedSegmentConfig config = checkpointConfig(temporary.newFolder("index-" + phase)
                    .toPath().resolve("bookie"));
            createIndexData(config);
            try (IndexFixture source = new IndexFixture(config, identity());
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
                index.rebuild(source.inventory, source.arena, source.bookie);
            }
            Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx96m", "-XX:MaxDirectMemorySize=32m", "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    DerivedIndexCrashProbe.class.getName(), phase, config.directory().toString())
                    .redirectErrorStream(true).start();
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Derived index probe did not terminate");
            }
            assertEquals(new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8),
                    23, process.exitValue());
            try (IndexFixture source = new IndexFixture(config, identity());
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
                assertEquals(!phase.equals("put"), index.coverage().isPresent());
                index.rebuild(source.inventory, source.arena, source.bookie);
                assertEquals(2, index.coverage().orElseThrow().entries());
                assertTrue(index.get(context(), 1).isPresent());
            }
        }
    }

    @Test
    public void runtimeIndexProcessExitCannotPromoteUnclosedSessionEvenAfterPhysicalRebuild() throws Exception {
        for (String phase : List.of("memtable", "before-page-force", "owner-callback")) {
            IsolatedSegmentConfig config = checkpointConfig(temporary.newFolder("runtime-exit-" + phase)
                    .toPath().resolve("bookie"));
            runRuntimeIndexProbe(config, phase);
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
                IOException failure = assertThrows(IOException.class, storage::restart);
                assertTrue(failure.getMessage(), failure.getMessage().contains("Unclosed DATA writer session"));
            }
            try (IndexFixture source = new IndexFixture(config, identity());
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
                assertTrue(source.arena.hasUnclosedDataSession());
                assertEquals(!phase.equals("memtable"), index.coverage().isPresent());
                assertEquals(2, source.inventory.scan().batches());
                assertEquals(2, source.inventory.scan().records());
                SegmentFileIo recovery = new SegmentFileIo();
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(config, identity(),
                        source.arena, source.bookie, recovery, 32, 4, index));
                assertEquals(0, recovery.forces);
                index.rebuild(source.inventory, source.arena, source.bookie);
                assertEquals(2, index.coverage().orElseThrow().entries());
                for (long entryId : new long[] {1, 2}) {
                    SegmentPointRead.Location location = index.get(context(), entryId).orElseThrow();
                    assertTrue(source.inventory.entries().contains(location));
                    IOException failure = assertThrows(IOException.class,
                            () -> SegmentPointRead.read(config, identity(), source.arena, location));
                    assertTrue(failure.getMessage(), failure.getMessage().contains("no current Arena grant"));
                }
                assertTrue(source.arena.hasUnclosedDataSession());
            }
            // Physical recovery/derived publication cannot repair the missing durable session CLOSE.
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
                IOException failure = assertThrows(IOException.class, storage::restart);
                assertTrue(failure.getMessage(), failure.getMessage().contains("Unclosed DATA writer session"));
            }
        }
    }

    @Test
    public void runtimeIndexProcessExitAfterCleanCloseReusesForcedCoverageInStorageGraph() throws Exception {
        IsolatedSegmentConfig config = checkpointConfig(temporary.newFolder("runtime-exit-clean")
                .toPath().resolve("bookie"));
        runRuntimeIndexProbe(config, "clean-close");
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            assertFalse(arena.hasUnclosedDataSession());
            SegmentDerivedIndex.Coverage published = index.coverage().orElseThrow();
            assertEquals(2, published.entries());
            SegmentFileIo recovery = new SegmentFileIo();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(),
                    arena, bookie, recovery, 32, 4, index)) {
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(2, files.startupInventory().scan().coveredBatches());
                assertEquals(0, files.startupInventory().scan().batches());
                assertEquals(2L * (4096 - SegmentBatchFormat.HEADER),
                        files.startupInventory().scan().skippedBodyBytes());
                assertEquals(1, recovery.forces);
                assertEquals(published.indexGeneration(), index.coverage().orElseThrow().indexGeneration());
                assertEquals(published.publication() + 1, index.coverage().orElseThrow().publication());
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            for (long entryId : new long[] {1, 2}) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
                storageWrite(storage, context(), entryId);
            }
            writeAfterIndexBackpressure(storage, 3);
        }
    }

    private void runRuntimeIndexProbe(IsolatedSegmentConfig config, String phase) throws Exception {
        Path output = temporary.getRoot().toPath().resolve("runtime-index-exit-" + phase + ".log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx96m", "-XX:MaxDirectMemorySize=32m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                RuntimeIndexCrashProbe.class.getName(), phase, config.directory().toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        boolean finished = child.waitFor(20, TimeUnit.SECONDS);
        if (!finished) {
            child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
        String log = Files.readString(output);
        assertTrue(log, finished);
        assertEquals(log, 23, child.exitValue());
        assertTrue(log, log.contains("RUNTIME_INDEX_" + phase));
    }

    /** Actual owner/async native index graph; halt never runs cleanup at the selected process boundary. */
    public static final class RuntimeIndexCrashProbe {
        public static void main(String[] args) throws Exception {
            if (args.length != 2 || !List.of("memtable", "before-page-force", "owner-callback", "clean-close")
                    .contains(args[0])) {
                throw new IllegalArgumentException("Expected runtime index phase and directory");
            }
            String phase = args[0];
            SegmentBookieControlLogTest test = new SegmentBookieControlLogTest();
            try (IsolatedSegmentStorage storage = test.storage(checkpointConfig(Path.of(args[1])), 2, 6, 4, 32, 3)) {
                storage.startNew();
                installStorage(storage, install(1, (byte) 5), activate(2));
                writeAfterIndexBackpressure(storage, 1);
                writeAfterIndexBackpressure(storage, 2);
                // Inspect the actual native state without adding production APIs solely for testing.
                java.lang.reflect.Field indexField = IsolatedSegmentStorage.class.getDeclaredField("index");
                indexField.setAccessible(true);
                SegmentDerivedIndex index = (SegmentDerivedIndex) indexField.get(storage);
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (index.get(context(), 2).isEmpty() && System.nanoTime() < end) {
                    Thread.sleep(1);
                }
                assertTrue(index.get(context(), 1).isPresent());
                assertTrue(index.get(context(), 2).isPresent());
                if (phase.equals("memtable")) {
                    java.lang.reflect.Field dbField = SegmentDerivedIndex.class.getDeclaredField("db");
                    dbField.setAccessible(true);
                    synchronized (index) {
                        org.rocksdb.RocksDB db = (org.rocksdb.RocksDB) dbField.get(index);
                        assertTrue(db.getLongProperty("rocksdb.num-entries-active-mem-table") >= 2);
                        assertTrue(index.coverage().isEmpty());
                        halt(phase);
                    }
                }
                SegmentFileIo publication = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (phase.equals("before-page-force")) {
                        halt(phase);
                    }
                });
                IsolatedSegmentStorage.IndexCheckpointResult published;
                do {
                    CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
                    assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest(
                            (result, cause) -> {
                                if (cause != null) {
                                    reply.completeExceptionally(cause);
                                } else {
                                    if (phase.equals("owner-callback") && result.status()
                                            == IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED) {
                                        assertEquals(2, result.coverage().entries());
                                        halt(phase);
                                    }
                                    reply.complete(result);
                                }
                            }), publication));
                    published = reply.get(5, TimeUnit.SECONDS);
                    if (published.status() == IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE) {
                        Thread.sleep(1);
                    }
                } while (published.status() == IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE
                        && System.nanoTime() < end);
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, published.status());
                assertEquals(2, published.coverage().entries());
                assertEquals("clean-close", phase);
                assertTrue(storage.close(Duration.ofSeconds(5)));
                halt(phase);
            }
        }

        private static void halt(String phase) {
            System.out.println("RUNTIME_INDEX_" + phase);
            System.out.flush();
            Runtime.getRuntime().halt(23);
        }
    }

    public static final class DerivedIndexCrashProbe {
        public static void main(String[] args) throws Exception {
            IsolatedSegmentConfig config = checkpointConfig(Path.of(args[1]));
            SegmentFileHeader identity = new SegmentBookieControlLogTest().identity();
            try (IndexFixture source = new IndexFixture(config, identity);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity, 16)) {
                index.replace(source.inventory, source.arena, source.bookie);
                if (args[0].equals("put")) {
                    Runtime.getRuntime().halt(23);
                }
                index.checkpoint(new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (args[0].equals("publication")) {
                        Runtime.getRuntime().halt(23);
                    }
                }));
                Runtime.getRuntime().halt(23);
            }
        }
    }

    @Test
    public void derivedIndexKeyLocationAndCoverageMatchIndependentByteVectors() throws Exception {
        ProfileLedgerContext context = new ProfileLedgerContext(44, filled(16, (byte) 1), filled(36, (byte) 3));
        byte[] key = SegmentDerivedIndex.key(context, 1);
        UUID generation = new UUID(1, 2);
        SegmentPointRead.Location location = new SegmentPointRead.Location(new SegmentBatchFormat.Address(
                0, 0, 5, new Generation(1), new Generation(1), 11, 4096), 4096, 4224, 204, context, 1);
        byte[] value = SegmentDerivedIndex.encodeLocation(generation, location);
        java.security.MessageDigest hash = java.security.MessageDigest.getInstance("SHA-256");
        hash.update(key);
        hash.update(value);
        SegmentDerivedIndex.Coverage coverage = new SegmentDerivedIndex.Coverage(1, generation, 3, 1, 1,
                List.of(new SegmentDataRecovery.StreamCoverage(0, new Generation(1), 11, 11)));
        assertArrayEquals(checkpointVector("index-key-v1.hex"), key);
        assertArrayEquals(checkpointVector("index-location-v1.hex"), value);
        assertArrayEquals(checkpointVector("index-coverage-v1.hex"),
                SegmentDerivedIndex.encodePublication(filled(32, (byte) 4), coverage, hash.digest()).array());
    }

    @Test
    public void storageGraphRebuildsDeletedDerivedIndexBeforeCleanRestartRequests() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 16)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            storageWrite(storage, context(), 1);
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(SegmentDerivedIndex.directory(config))) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 16)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult result = storageRead(storage, context(), 1).result()) {
                assertEquals(1, result.result().record().entryId());
            }
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                    storageWrite(storage, context(), 1).status());
            storageWrite(storage, context(), 2);
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertFalse(index.coverage().map(value -> value.entries() > 1).orElse(false));
            assertTrue(index.get(context(), 1).isPresent());
            // Runtime Put may be query-visible, but it cannot advance the cold durable coverage.
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 16)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult result = storageRead(storage, context(), 2).result()) {
                assertEquals(2, result.result().record().entryId());
            }
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
            assertEquals(2, index.coverage().orElseThrow().entries());
            assertTrue(index.get(context(), 2).isPresent());
        }
    }

    @Test
    public void asynchronousDerivedIndexStallKeepsHotSuccessAndReservedFenceProgress() throws Exception {
        exerciseDerivedIndexStall(false);
    }

    @Test
    public void asynchronousDerivedIndexDrainRetainsNativeLockUntilRealIoTerminates() throws Exception {
        exerciseDerivedIndexStall(true);
    }

    private void exerciseDerivedIndexStall(boolean drain) throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch atIndex = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger operations = new AtomicInteger();
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(
                        config, identity(), 1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 2)) {
            files.startSession(arena);
            grant(arena, 1);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 4, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentIoExecutor io = new SegmentIoExecutor(1, 16, 1, 4,
                    task -> new Thread(task, "derived-index-test-io"), () -> {
                        if (operations.incrementAndGet() == 2) {
                            atIndex.countDown();
                            await(release);
                        }
                    });
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 2);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                FutureTask<Void> attach = new FutureTask<>(() -> {
                    hot.attachIndex(0, index, io);
                    return null;
                });
                assertTrue(owner.control(attach));
                attach.get(5, TimeUnit.SECONDS);
                CountDownLatch first = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, first)));
                assertTrue(first.await(5, TimeUnit.SECONDS));
                assertTrue(atIndex.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                assertTrue(index.get(context(), 1).isEmpty());
                assertTrue(index.coverage().isEmpty());
                assertEquals(1, SegmentPointRead.read(config, identity(), arena,
                        results.get(0).candidate()).record().entryId());
                CompletableFuture<SegmentBookieControlLog.FenceResult> fenced = new CompletableFuture<>();
                assertTrue(new SegmentBookieControlDriver(owner, io, bookie).submitFence(fence(5, (byte) 5),
                        (reply, cause) -> {
                            if (cause == null) {
                                fenced.complete(reply);
                            } else {
                                fenced.completeExceptionally(cause);
                            }
                        }));
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        fenced.get(5, TimeUnit.SECONDS).status());
                if (drain) {
                    assertFalse(group.shutdown(Duration.ofMillis(50)));
                    assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, identity(), 2));
                    assertTrue(index.get(context(), 1).isEmpty());
                    release.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertTrue(index.get(context(), 1).isPresent());
                } else {
                    CountDownLatch second = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(48), 1),
                            bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, second)));
                    ownerBarrier(owner);
                    long reserved = files.nextOffset(0);
                    CountDownLatch refused = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(48), 2),
                            bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(results, failure, refused)));
                    assertTrue(refused.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                            results.get(1).status());
                    assertEquals(reserved, files.nextOffset(0));
                    assertEquals(1, files.dataIo.forces);
                    release.countDown();
                    assertTrue(second.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(2).status());
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (index.get(context(48), 1).isEmpty() && System.nanoTime() < end) {
                        Thread.sleep(1);
                    }
                    assertEquals(results.get(2).candidate(), index.get(context(48), 1).orElseThrow());
                    FutureTask<SegmentPointRead.Reader> reader = new FutureTask<>(
                            () -> SegmentPointRead.Reader.live(owner, io, files, arena, bookie, hot));
                    assertTrue(owner.control(reader));
                    try (SegmentPointRead.LeasedResult read = pointRead(
                            reader.get(5, TimeUnit.SECONDS), context(48), 1).result()) {
                        assertEquals(1, read.result().record().entryId());
                    }
                    assertTrue(index.coverage().isEmpty());
                    assertEquals(2, files.dataIo.forces);
                    assertEquals(0, group.inputs().usedBytes());
                }
                assertNull(failure.get());
                assertNull(owner.failure());
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                files.close();
            }
        }
    }

    @Test
    public void asynchronousIndexConflictRetainsHotReadAndRefusesNewDataBeforeReservation() throws Exception {
        IsolatedSegmentConfig config = config();
        List<SegmentDataPhysicalDriver.NormalResult> results = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(
                        config, identity(), 1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 4)) {
            files.startSession(arena);
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            // A real conflicting derived row cannot overwrite the new authoritative DATA location.
            index.append(List.of(new SegmentPointRead.Location(new SegmentBatchFormat.Address(
                    0, 0, 5, new Generation(1), new Generation(1), 9, 8192),
                    4096, 8320, 204, context(), 1)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 4, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                FutureTask<Void> attach = new FutureTask<>(() -> {
                    hot.attachIndex(0, index, group.io());
                    return null;
                });
                assertTrue(owner.control(attach));
                attach.get(5, TimeUnit.SECONDS);
                CountDownLatch first = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 1),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, first)));
                assertTrue(first.await(5, TimeUnit.SECONDS));
                Throwable indexFailure;
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                do {
                    FutureTask<Throwable> query = new FutureTask<>(hot::indexFailure);
                    assertTrue(owner.control(query));
                    indexFailure = query.get(5, TimeUnit.SECONDS);
                    if (indexFailure == null) {
                        Thread.sleep(1);
                    }
                } while (indexFailure == null && System.nanoTime() < end);
                assertTrue(indexFailure instanceof IOException);
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, results.get(0).status());
                long offset = files.nextOffset(0);
                CountDownLatch refused = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(results, failure, refused)));
                assertTrue(refused.await(5, TimeUnit.SECONDS));
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, results.get(1).status());
                assertEquals(offset, files.nextOffset(0));
                assertEquals(1, files.dataIo.forces);
                try (SegmentPointRead.LeasedResult read = pointRead(
                        pointReader(group, files, arena, bookie, hot), context(), 1).result()) {
                    assertEquals(1, read.result().record().entryId());
                }
                assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                        bookie.submitFence(fence(3, (byte) 5)).status());
                assertThrows(IOException.class, () -> index.get(context(), 1));
                assertNull(failure.get());
                assertNull(owner.failure());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                files.close();
            }
        }
    }

    @Test
    public void indexHandoffKeepsCapturedReadAndDrainsCancelledFreshGet() throws Exception {
        exerciseIndexHandoff(false);
    }

    @Test
    public void indexHandoffKeepsCapturedDuplicateAndChecksFreshNormalAndRecoveryDuplicates() throws Exception {
        exerciseIndexHandoff(true);
    }

    private void exerciseIndexHandoff(boolean duplicate) throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch atIndex = new CountDownLatch(1);
        CountDownLatch releaseIndex = new CountDownLatch(1);
        CountDownLatch atRead = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch atGet = new CountDownLatch(1);
        CountDownLatch releaseGet = new CountDownLatch(1);
        AtomicInteger operations = new AtomicInteger();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(
                        config, identity(), 1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 4)) {
            files.startSession(arena);
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 4, 2, 4096, 4096, 20478, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentIoExecutor io = new SegmentIoExecutor(2, 16, 1, 4,
                    task -> new Thread(task, "index-handoff-test-io"), () -> {
                        int operation = operations.incrementAndGet();
                        if (operation == 2) {
                            atIndex.countDown();
                            await(releaseIndex);
                        } else if (operation == 3) {
                            atRead.countDown();
                            await(releaseRead);
                        } else if (operation == 4 && !duplicate) {
                            atGet.countDown();
                            await(releaseGet);
                        }
                    });
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 1, 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, io, files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            try {
                SegmentDataPhysicalDriver.NormalBatcher batcher = normalBatcher(driver, owner, bookie,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30)));
                FutureTask<Void> attach = new FutureTask<>(() -> {
                    hot.attachIndex(0, index, io);
                    return null;
                });
                assertTrue(owner.control(attach));
                attach.get(5, TimeUnit.SECONDS);
                List<SegmentDataPhysicalDriver.NormalResult> normals = new CopyOnWriteArrayList<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                CountDownLatch first = new CountDownLatch(1);
                assertTrue(batcher.submit(sourceInput(group, context(), 2),
                        bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                        collectNormal(normals, failure, first)));
                assertTrue(first.await(5, TimeUnit.SECONDS));
                assertTrue(atIndex.await(5, TimeUnit.SECONDS));
                FutureTask<SegmentPointRead.Reader> makeReader = new FutureTask<>(
                        () -> SegmentPointRead.Reader.live(owner, io, files, arena, bookie, hot));
                assertTrue(owner.control(makeReader));
                SegmentPointRead.Reader reader = makeReader.get(5, TimeUnit.SECONDS);
                CompletableFuture<ReadReply> read = new CompletableFuture<>();
                CountDownLatch compared = new CountDownLatch(1);
                if (duplicate) {
                    assertTrue(batcher.submit(sourceInput(group, context(), 2),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(normals, failure, compared)));
                } else {
                    assertTrue(reader.submit(context(), 2, new SegmentPointRead.ReadRequest(
                            (status, result, cause) -> read.complete(new ReadReply(status, result, cause)))));
                }
                assertTrue(atRead.await(5, TimeUnit.SECONDS));
                releaseIndex.countDown();
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                int retained;
                do {
                    FutureTask<Integer> count = new FutureTask<>(hot::candidateCount);
                    assertTrue(owner.control(count));
                    retained = count.get(5, TimeUnit.SECONDS);
                    if (retained != 0) {
                        Thread.sleep(1);
                    }
                } while (retained != 0 && System.nanoTime() < end);
                assertEquals(0, retained);
                assertEquals(normals.get(0).candidate(), index.get(context(), 2).orElseThrow());
                assertTrue(index.coverage().isEmpty());
                releaseRead.countDown();
                if (!duplicate) {
                    ReadReply found = read.get(5, TimeUnit.SECONDS);
                    assertEquals(SegmentPointRead.ReadStatus.FOUND, found.status());
                    assertNull(found.failure());
                    found.result().close();
                    AtomicInteger replies = new AtomicInteger();
                    SegmentPointRead.ReadRequest cancelled = new SegmentPointRead.ReadRequest(
                            (status, result, cause) -> {
                                if (result != null) {
                                    result.close();
                                }
                                replies.incrementAndGet();
                            });
                    assertTrue(reader.submit(context(), 2, cancelled));
                    assertTrue(atGet.await(5, TimeUnit.SECONDS));
                    assertTrue(cancelled.stopWaiting());
                    assertFalse(group.shutdown(Duration.ofMillis(50)));
                    assertThrows(IOException.class, files::close);
                    releaseGet.countDown();
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                    assertEquals(0, replies.get());
                } else {
                    assertTrue(compared.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, normals.get(1).status());
                    CountDownLatch fresh = new CountDownLatch(1);
                    assertTrue(batcher.submit(sourceInput(group, context(), 2),
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            collectNormal(normals, failure, fresh)));
                    assertTrue(fresh.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, normals.get(2).status());
                    AtomicReference<SegmentDataPhysicalDriver.DuplicateStatus> oldApi = new AtomicReference<>();
                    CountDownLatch oldDone = new CountDownLatch(1);
                    assertTrue(driver.checkPublishedNormalDuplicate(bookie,
                            bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                            entryBytes(context(), 2, -1, 8, (byte) 0), (status, cause) -> {
                                oldApi.set(status);
                                failure.set(cause);
                                oldDone.countDown();
                            }));
                    assertTrue(oldDone.await(5, TimeUnit.SECONDS));
                    assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE, oldApi.get());
                    ownerBarrier(owner);
                    bookie.submitFence(fence(3, (byte) 5));
                    assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                            bookie.submitRecoveryGrant(recoveryGrant(4, 3, 7, 9)).status());
                    ByteBuffer same = entryBytes(context(), 2, -1, 8, (byte) 0);
                    byte[] payload = new byte[same.remaining()];
                    same.slice().get(payload);
                    byte[] intent = new byte[16];
                    intent[15] = 3;
                    ProfileOperationCodec.AddRecovery recovery = new ProfileOperationCodec.AddRecovery(
                            context(), intent, 7, 9, 2, 4, 2, filled(20, (byte) 5), payload);
                    CompletableFuture<SegmentDataPhysicalDriver.RecoveryResult> recovered = new CompletableFuture<>();
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(recovery).orElseThrow(),
                            new SegmentDataPhysicalDriver.RecoveryRequest((result, cause) -> {
                                failure.set(cause);
                                recovered.complete(result);
                            })));
                    assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.LOCAL_DURABLE,
                            recovered.get(5, TimeUnit.SECONDS).status());
                    ownerBarrier(owner);
                    assertEquals(1, files.dataIo.forces);
                    long offset = files.nextOffset(0);
                    index.close();
                    CompletableFuture<SegmentDataPhysicalDriver.RecoveryResult> unavailable = new CompletableFuture<>();
                    assertTrue(batcher.submitRecovery(bookie.admitRecovery(recovery).orElseThrow(),
                            new SegmentDataPhysicalDriver.RecoveryRequest((result, cause) -> {
                                assertTrue(cause instanceof IOException);
                                unavailable.complete(result);
                            })));
                    assertEquals(SegmentDataPhysicalDriver.RecoveryStatus.UNKNOWN,
                            unavailable.get(5, TimeUnit.SECONDS).status());
                    assertEquals(offset, files.nextOffset(0));
                    assertEquals(1, files.dataIo.forces);
                    ownerBarrier(owner);
                }
                assertEquals(0, group.inputs().usedBytes());
                assertNull(failure.get());
                assertNull(owner.failure());
            } finally {
                releaseIndex.countDown();
                releaseRead.countDown();
                releaseGet.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                io.shutdown();
                assertTrue(io.awaitTermination(Duration.ofSeconds(5)));
                files.close();
            }
        }
    }

    private static SegmentDataPhysicalDriver.NormalResult writeAfterIndexBackpressure(IsolatedSegmentStorage storage,
                                                                                      long entryId) throws Exception {
        return writeAfterIndexBackpressure(storage, context(), entryId);
    }

    private static SegmentDataPhysicalDriver.NormalResult writeAfterIndexBackpressure(IsolatedSegmentStorage storage,
            ProfileLedgerContext context, long entryId) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        SegmentDataPhysicalDriver.NormalResult result = null;
        do {
            CompletableFuture<SegmentDataPhysicalDriver.NormalResult> reply = new CompletableFuture<>();
            ByteBuf bytes = entry(context, entryId);
            boolean accepted;
            try {
                accepted = storage.submitNormal(context, filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((value, cause) -> {
                            assertNull(cause);
                            reply.complete(value);
                        }));
            } finally {
                bytes.release();
            }
            if (!accepted) {
                Thread.sleep(1);
                continue;
            }
            result = reply.get(5, TimeUnit.SECONDS);
            if (result.status() == SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE) {
                return result;
            }
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE, result.status());
            Thread.sleep(1);
        } while (System.nanoTime() < end);
        throw new AssertionError("Index handoff did not release hot capacity: " + result);
    }

    @Test
    public void storageGraphWritesBeyondHotCapacityAndRebuildsAllCoordinatesIncludingHoles() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 24)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            SegmentDataPhysicalDriver.NormalResult first = writeAfterIndexBackpressure(storage, 1);
            writeAfterIndexBackpressure(storage, 3);
            writeAfterIndexBackpressure(storage, 2);
            assertEquals(first.candidate(), storageWrite(storage, context(), 1).candidate());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(SegmentDerivedIndex.directory(config))) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 24)) {
            storage.restart();
            for (long entryId : new long[] {1, 2, 3}) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
                storageWrite(storage, context(), entryId);
            }
            writeAfterIndexBackpressure(storage, 4);
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 24)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 4).result()) {
                assertEquals(4, read.result().record().entryId());
            }
        }
    }

    private static IsolatedSegmentStorage.IndexCheckpointResult checkpointAfterBackpressure(
            IsolatedSegmentStorage storage, SegmentFileIo publicationIo) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
            assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest((result, cause) -> {
                if (cause != null) {
                    reply.completeExceptionally(cause);
                } else {
                    reply.complete(result);
                }
            }), publicationIo));
            IsolatedSegmentStorage.IndexCheckpointResult result = reply.get(5, TimeUnit.SECONDS);
            if (result.status() != IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE) {
                return result;
            }
            Thread.sleep(1);
        } while (System.nanoTime() < end);
        throw new AssertionError("Index maintenance did not reach a complete checkpoint cut");
    }

    @Test
    public void runtimeIndexCheckpointUsesAllOriginalWorkersAndMonotoneSameGenerationPublications() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("runtime-index-" + workers), 1, 4, 1,
                    8L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            SegmentDerivedIndex.Coverage latest;
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 16, 32, 3)) {
                storage.startNew();
                installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                        install(45, 3, (byte) 5), activate(45, 4),
                        install(46, 5, (byte) 5), activate(46, 6),
                        install(47, 7, (byte) 5), activate(47, 8));
                for (long ledgerId = 44; ledgerId < 48; ledgerId++) {
                    writeAfterIndexBackpressure(storage, context(ledgerId), 1);
                }
                IsolatedSegmentStorage.IndexCheckpointResult first = checkpointAfterBackpressure(
                        storage, new SegmentFileIo());
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, first.status());
                assertEquals(4, first.coverage().entries());
                assertEquals(4, first.coverage().streams().size());
                assertEquals(1, first.coverage().publication());
                storageWrite(storage, context(), 2);
                latest = checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage();
                assertEquals(first.coverage().indexGeneration(), latest.indexGeneration());
                assertEquals(2, latest.publication());
                assertEquals(5, latest.entries());
                assertEquals(3, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().publication());
                try (SegmentPointRead.LeasedResult result = storageRead(storage, context(), 2).result()) {
                    assertEquals(2, result.result().record().entryId());
                }
            }
            try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
                SegmentDerivedIndex.Coverage recovered = index.coverage().orElseThrow();
                assertEquals(3, recovered.publication());
                assertEquals(latest.indexGeneration(), recovered.indexGeneration());
                assertEquals(5, recovered.entries());
            }
        }
    }

    @Test
    public void runtimeIndexCheckpointPausedPublicationKeepsNewHotDataFenceAndCancellationDrain() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch atPageForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger replies = new AtomicInteger();
        SegmentFileIo publication = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            atPageForce.countDown();
            await(release);
        });
        IsolatedSegmentStorage storage = storage(config, 2, 6, 16, 32, 3);
        try {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            storageWrite(storage, context(), 1);
            IsolatedSegmentStorage.IndexCheckpointRequest request;
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            do {
                CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> response = new CompletableFuture<>();
                request = new IsolatedSegmentStorage.IndexCheckpointRequest((result, cause) -> {
                    if (result.status() != IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE) {
                        replies.incrementAndGet();
                    }
                    response.complete(result);
                });
                assertTrue(storage.checkpointIndex(request, publication));
                while (atPageForce.getCount() != 0 && !response.isDone() && System.nanoTime() < end) {
                    Thread.sleep(1);
                }
                if (atPageForce.getCount() == 0) {
                    break;
                }
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE,
                        response.get(5, TimeUnit.SECONDS).status());
            } while (System.nanoTime() < end);
            assertTrue(atPageForce.await(5, TimeUnit.SECONDS));
            assertFalse(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, cause) -> { })));
            storageWrite(storage, context(48), 1);
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 1).result()) {
                assertEquals(48, read.result().record().context().ledgerId());
            }
            CompletableFuture<SegmentBookieControlLog.FenceResult> fenced = new CompletableFuture<>();
            assertTrue(storage.controls().submitFence(fence(5, (byte) 5), (result, cause) -> {
                if (cause == null) {
                    fenced.complete(result);
                } else {
                    fenced.completeExceptionally(cause);
                }
            }));
            assertEquals(SegmentBookieControlLog.FenceStatus.APPLIED_DURABLE,
                    fenced.get(5, TimeUnit.SECONDS).status());
            assertTrue(request.stopWaiting());
            assertFalse(storage.close(Duration.ofMillis(50)));
            assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, identity(), 32));
            release.countDown();
            assertTrue(storage.close(Duration.ofSeconds(5)));
            assertEquals(0, replies.get());
        } finally {
            release.countDown();
            storage.close();
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            SegmentDerivedIndex.Coverage coverage = index.coverage().orElseThrow();
            assertEquals(1, coverage.entries());
            assertEquals(1, coverage.streams().get(0).through());
            assertTrue(index.get(context(48), 1).isEmpty());
        }
        try (IsolatedSegmentStorage restarted = storage(config, 2, 6, 16, 32)) {
            restarted.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(48), 1).result()) {
                assertEquals(48, read.result().record().context().ledgerId());
            }
        }
    }

    @Test
    public void runtimeIndexPublicationFailureDoesNotPoisonQueryableRowsOrReportSuccess() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 16, 32)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            storageWrite(storage, context(), 1);
            SegmentDerivedIndex.Coverage first = checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage();
            storageWrite(storage, context(), 2);
            SegmentFileIo failedPublication = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            IsolatedSegmentStorage.IndexCheckpointResult failed;
            do {
                CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
                assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest(
                        (result, cause) -> {
                    if (result.status() == IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN) {
                        assertTrue(cause instanceof IOException);
                    } else {
                        assertNull(cause);
                    }
                    reply.complete(result);
                }), failedPublication));
                failed = reply.get(5, TimeUnit.SECONDS);
                if (failed.status() != IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE) {
                    break;
                }
                Thread.sleep(1);
            } while (System.nanoTime() < end);
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN, failed.status());
            assertNull(failed.coverage());
            for (long entryId : new long[] {1, 2}) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
                storageWrite(storage, context(), entryId);
            }
            SegmentDerivedIndex.Coverage retried = checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage();
            assertEquals(first.indexGeneration(), retried.indexGeneration());
            assertEquals(3, retried.publication());
            assertEquals(2, retried.entries());
        }
    }

    @Test
    public void runtimeIndexPartialDataTicketRefusalResumesPreparedWorkersBeforeRetry() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("runtime-index-ticket-refusal"), 1, 4, 1,
                4L * 1024 * 1024, 4096, 1024, 4, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        CountDownLatch callbackHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 16, 32, 3)) {
            storage.startNew();
            installStorage(storage, install(45, 1, (byte) 5), activate(45, 2));
            ByteBuf bytes = entry(context(45), 1);
            try {
                assertTrue(storage.submitNormal(context(45), filled(20, (byte) 5), bytes,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> {
                            assertNull(cause);
                            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
                            callbackHeld.countDown();
                            await(release);
                        })));
            } finally {
                bytes.release();
            }
            try {
                assertTrue(callbackHeld.await(5, TimeUnit.SECONDS));
                CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> refused = new CompletableFuture<>();
                assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest(
                        (result, cause) -> {
                    assertNull(cause);
                    refused.complete(result);
                })));
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE,
                        refused.get(5, TimeUnit.SECONDS).status());
                release.countDown();
                IsolatedSegmentStorage.IndexCheckpointResult retried = checkpointAfterBackpressure(
                        storage, new SegmentFileIo());
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, retried.status());
                assertEquals(1, retried.coverage().entries());
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(45), 1).result()) {
                    assertEquals(45, read.result().record().context().ledgerId());
                }
            } finally {
                release.countDown();
            }
        }
    }

    private record CoveredTail(SegmentDerivedIndex.Coverage coverage,
                               SegmentDataPhysicalDriver.NormalResult first,
                               SegmentDataPhysicalDriver.NormalResult tail) { }

    private IsolatedSegmentConfig coverageRecoveryConfig(String name) {
        return coverageRecoveryConfig(name, 4L * 1024 * 1024);
    }

    private IsolatedSegmentConfig coverageRecoveryConfig(String name, long bytesPerFile) {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath().resolve(name), 1, 4, 1,
                bytesPerFile, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
    }

    /** Stop admission while page publication is paused, leaving two real durable batches outside its native cut. */
    private CoveredTail createCoveredTail(IsolatedSegmentConfig config) throws Exception {
        CountDownLatch atPageForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SegmentFileIo publication = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            atPageForce.countDown();
            await(release);
        });
        IsolatedSegmentStorage storage = storage(config, 2, 6, 16, 32, 3);
        try {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(45, 3, (byte) 5), activate(45, 4));
            SegmentDataPhysicalDriver.NormalResult first = storageWrite(storage, context(), 1);
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply;
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            do {
                reply = new CompletableFuture<>();
                CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> current = reply;
                assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest(
                        (result, cause) -> {
                            if (cause == null) {
                                current.complete(result);
                            } else {
                                current.completeExceptionally(cause);
                            }
                        }), publication));
                while (atPageForce.getCount() != 0 && !reply.isDone() && System.nanoTime() < end) {
                    Thread.sleep(1);
                }
                if (atPageForce.getCount() == 0) {
                    break;
                }
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE,
                        reply.get(5, TimeUnit.SECONDS).status());
            } while (System.nanoTime() < end);
            assertTrue(atPageForce.await(5, TimeUnit.SECONDS));
            SegmentDataPhysicalDriver.NormalResult tail = storageWrite(storage, context(), 2);
            storageWrite(storage, context(45), 1);
            assertFalse(storage.close(Duration.ofMillis(50)));
            release.countDown();
            assertTrue(storage.close(Duration.ofSeconds(5)));
            IsolatedSegmentStorage.IndexCheckpointResult persisted = reply.get(5, TimeUnit.SECONDS);
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, persisted.status());
            return new CoveredTail(persisted.coverage(), first, tail);
        } finally {
            release.countDown();
            storage.close();
        }
    }

    @Test
    public void allocationControlOrderSurvivesRotationAndBothFullAndCoveredStorageRestart() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("allocation-control-order", 5L * 1024 * 1024);
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            for (int i = 0; i < 2; i++) {
                long offset = i == 0 ? upper : 4096;
                long id = 5 + i;
                arena.submitBatch(List.of(new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                        new UUID(0, id), new Generation(1), 0, 0, 0, new Generation(1), id, offset, 1, 1)));
                ByteBuf bytes = entry(context(), i + 1);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(
                            0, 0, id, new Generation(1), new Generation(1), i + 1, offset),
                            List.of(SegmentBatchFormat.entry(context(), bytes))), arena);
                } finally {
                    bytes.release();
                }
            }
            files.close();
            arena.rotate();
        }
        SegmentDerivedIndex.Coverage published;
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            for (long entryId : new long[] {1, 2}) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
                storageWrite(storage, context(), entryId);
            }
            writeAfterIndexBackpressure(storage, 3);
            published = checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage();
            assertEquals(3, published.entries());
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            for (long entryId : new long[] {1, 2, 3}) {
                storageWrite(storage, context(), entryId);
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
            }
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            assertEquals(published.indexGeneration(), index.coverage().orElseThrow().indexGeneration());
        }
    }

    private static SegmentArenaControlLog.Allocation reclaimableAllocation(long id, long offset) {
        return new SegmentArenaControlLog.Allocation(SegmentControlFormat.Type.ALLOC_POOL,
                new UUID(0, id), new Generation(1), 0, 0, 0, new Generation(1), id, offset, 1, 1);
    }

    private static SegmentBatchFormat.FrozenBatch reclamationBatch(IsolatedSegmentConfig config,
            long allocation, long offset, long sequence, long... ledgers) throws Exception {
        List<ByteBuf> inputs = new ArrayList<>();
        try {
            List<SegmentBatchFormat.Data> records = new ArrayList<>();
            for (long ledger : ledgers) {
                ByteBuf bytes = entry(context(ledger), sequence);
                inputs.add(bytes);
                records.add(SegmentBatchFormat.entry(context(ledger), bytes));
            }
            return SegmentBatchFormat.encode(config, new SegmentBatchFormat.Address(0, 0, allocation,
                    new Generation(1), new Generation(1), sequence, offset), records);
        } finally {
            inputs.forEach(ByteBuf::release);
        }
    }

    @Test
    public void nativeTerminalCleanupUsesExactCapturedTombstonesAndBoundedDeleteBatches() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("native-terminal-cleanup");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 302)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2), install(48, 3, (byte) 5)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            long offset = 4096;
            for (int batch = 0; batch < 2; batch++) {
                List<ByteBuf> inputs = new ArrayList<>();
                try {
                    List<SegmentBatchFormat.Data> entries = new ArrayList<>();
                    for (int i = 1 + batch * 200; i <= Math.min(300, (batch + 1) * 200); i++) {
                        ByteBuf input = entry(context(), i);
                        inputs.add(input);
                        entries.add(SegmentBatchFormat.entry(context(), input));
                    }
                    SegmentBatchFormat.FrozenBatch frozen = SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, 101, new Generation(1),
                                    new Generation(1), batch + 1, offset), entries);
                    files.writeAuthorized(frozen, arena);
                    List<SegmentPointRead.Location> locations = SegmentPointRead.locations(
                            SegmentBatchParser.parse(frozen.bytes(), frozen.address(), config.bytesPerFile()));
                    index.append(locations);
                    offset += frozen.bytes().remaining();
                } finally {
                    inputs.forEach(ByteBuf::release);
                }
            }
            SegmentBatchFormat.FrozenBatch retained = reclamationBatch(config, 101, offset, 3, 48, 52);
            files.writeAuthorized(retained, arena);
            index.append(SegmentPointRead.locations(
                    SegmentBatchParser.parse(retained.bytes(), retained.address(), config.bytesPerFile())));
            assertTrue(bookie.locallyReadableContext(context(48)));
            assertFalse(bookie.hasTerminalTombstone(context(48)));
            SegmentBookieControlLog.Admission prior = bookie.admitNormal(context(), filled(20, (byte) 5))
                    .orElseThrow();
            SegmentBookieControlLog.ReadAdmission reader = bookie.admitRead(context(48)).orElseThrow();
            bookie.submitBatch(List.of(tombstone(44, 20)));
            assertThrows(IOException.class, () -> bookie.protectTerminalCleanup(config, identity()));
            prior.physicalTerminal();
            assertThrows(IOException.class, () -> bookie.protectTerminalCleanup(config, identity()));
            reader.finish();
            try (SegmentBookieControlLog.ReclamationLease cut = bookie.protectTerminalCleanup(config, identity())) {
                bookie.submitBatch(List.of(tombstone(48, 21)));
                assertFalse(cut.terminalAtCut(context(48)));
                index.retireTerminalEntries(cut);
                for (int entryId = 1; entryId <= 300; entryId++) {
                    assertTrue(index.get(context(), entryId).isEmpty());
                }
                assertTrue(index.get(context(48), 3).isPresent());
                assertTrue(index.get(context(52), 3).isPresent());
                assertThrows(IOException.class, () -> bookie.protectTerminalCleanup(config, identity()));
            }
            try (SegmentBookieControlLog.ReclamationLease cut = bookie.protectTerminalCleanup(config, identity())) {
                index.retireTerminalEntries(cut);
                assertTrue(index.get(context(48), 3).isEmpty());
                assertTrue(index.get(context(52), 3).isPresent());
            }
            files.close();
        }
    }

    private static IsolatedSegmentStorage.IndexCheckpointResult cleanupAfterBackpressure(
            IsolatedSegmentStorage storage) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
            assertTrue(storage.reclaimTerminalEntries(new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, cause) -> reply.complete(result))));
            IsolatedSegmentStorage.IndexCheckpointResult result = reply.get(5, TimeUnit.SECONDS);
            if (result.status() != IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE) {
                return result;
            }
            Thread.sleep(1);
        } while (System.nanoTime() < end);
        throw new AssertionError("Terminal cleanup did not obtain quiescent original owners");
    }

    private static void freeStorageAllocation(IsolatedSegmentStorage storage, long operation, long allocation)
            throws Exception {
        CompletableFuture<IsolatedSegmentStorage.FreeResult> reply = new CompletableFuture<>();
        assertTrue(storage.reclaimAllocation(new UUID(0, operation), allocation,
                new IsolatedSegmentStorage.FreeRequest((result, cause) -> {
                    if (cause == null) {
                        reply.complete(result);
                    } else {
                        reply.completeExceptionally(cause);
                    }
                })));
        assertEquals(IsolatedSegmentStorage.FreeStatus.APPLIED_DURABLE, reply.get(5, TimeUnit.SECONDS).status());
    }

    @Test
    public void runtimeTerminalCleanupRecyclesEveryOriginalOwnersCoordinateAndPhysicalBlock() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(temporary.getRoot().toPath()
                    .resolve("terminal-capacity-" + workers), 1, 4, 1, 8L * 1024 * 1024,
                    4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 4, 3)) {
                storage.startNew();
                for (int cycle = 0; cycle < 5; cycle++) {
                    List<ProtectedProfileStateStore.Transition> transitions = new ArrayList<>();
                    for (int shard = 0; shard < 4; shard++) {
                        long ledger = 44 + 4L * cycle + shard;
                        transitions.add(install(ledger, 1000 + cycle * 100 + shard * 2, (byte) 5));
                        transitions.add(activate(ledger, 1001 + cycle * 100 + shard * 2));
                    }
                    installStorage(storage, transitions.toArray(ProtectedProfileStateStore.Transition[]::new));
                }
                for (int cycle = 0; cycle < 5; cycle++) {
                    for (int shard = 0; shard < 4; shard++) {
                        SegmentBatchFormat.Address address = writeAfterIndexBackpressure(
                                storage, context(44 + 4L * cycle + shard), 1).candidate().address();
                        assertEquals(cycle + 1, address.generation().value());
                        assertTrue(address.offset() < 4096 + 4 * SegmentArenaControlLog.BLOCK_BYTES);
                    }
                    if (cycle == 4) {
                        assertEquals(4, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().entries());
                        break;
                    }
                    for (int shard = 0; shard < 4; shard++) {
                        installStorage(storage, tombstone(44 + 4L * cycle + shard, 2000 + cycle * 4 + shard));
                    }
                    CompletableFuture<SegmentDataPhysicalDriver.NormalResult> refused = new CompletableFuture<>();
                    ByteBuf input = entry(context(48 + cycle * 4L), 1);
                    try {
                        assertTrue(storage.submitNormal(context(48 + cycle * 4L), filled(20, (byte) 5), input,
                                new SegmentDataPhysicalDriver.NormalRequest(
                                        (result, cause) -> refused.complete(result))));
                    } finally {
                        input.release();
                    }
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                            refused.get(5, TimeUnit.SECONDS).status());
                    IsolatedSegmentStorage.IndexCheckpointResult cleaned = cleanupAfterBackpressure(storage);
                    assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, cleaned.status());
                    assertEquals(0, cleaned.coverage().entries());
                    for (int shard = 0; shard < 4; shard++) {
                        IsolatedSegmentStorage.CompactionResult reclaimed =
                                compactShard(storage, 3000 + cycle * 4 + shard, shard);
                        assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, reclaimed.status());
                        assertEquals(1, reclaimed.freedAllocations());
                        assertEquals(0, reclaimed.destinationBytes());
                    }
                }
            }
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 4, 3)) {
                storage.restart();
                for (int shard = 0; shard < 4; shard++) {
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(60 + shard), 1).result()) {
                        assertEquals(1, read.result().record().entryId());
                    }
                }
            }
        }
    }

    @Test
    public void cancelledTerminalCleanupHoldsActualIoAndDefersLaterTombstones() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("cancelled-terminal-cleanup");
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger replies = new AtomicInteger();
        SegmentFileIo publication = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            atForce.countDown();
            await(release);
        });
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(45, 3, (byte) 5), activate(45, 4));
            storageWrite(storage, context(), 1);
            storageWrite(storage, context(45), 1);
            checkpointAfterBackpressure(storage, new SegmentFileIo());
            installStorage(storage, tombstone(44, 20));
            IsolatedSegmentStorage.IndexCheckpointRequest request = new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, cause) -> replies.incrementAndGet());
            assertTrue(storage.reclaimTerminalEntries(request, publication));
            try {
                assertTrue(atForce.await(5, TimeUnit.SECONDS));
                request.stopWaiting();
                ByteBuf input = entry(context(45), 2);
                try {
                    assertFalse(storage.submitNormal(context(45), filled(20, (byte) 5), input,
                            new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> fail())));
                } finally {
                    input.release();
                }
                installStorage(storage, tombstone(45, 21));
                assertFalse(storage.close(Duration.ofMillis(20)));
            } finally {
                release.countDown();
            }
            assertTrue(storage.close(Duration.ofSeconds(5)));
            assertEquals(0, replies.get());
        }
        try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 4)) {
            assertTrue(index.get(context(), 1).isEmpty());
            assertTrue(index.get(context(45), 1).isPresent());
            assertEquals(1, index.coverage().orElseThrow().entries());
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.restart();
            assertEquals(0, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().entries());
        }
    }

    @Test
    public void terminalCleanupWaitsForActualLateNativePutBeforeReleasingCapacity() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("late-put-terminal-cleanup");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            var field = IsolatedSegmentStorage.class.getDeclaredField("index");
            field.setAccessible(true);
            SegmentDerivedIndex nativeIndex = (SegmentDerivedIndex) field.get(storage);
            synchronized (nativeIndex) {
                // Actual Put waits for this monitor on DATA I/O after the local durable reply.
                storageWrite(storage, context(), 1);
                installStorage(storage, tombstone(44, 20));
                CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
                assertTrue(storage.reclaimTerminalEntries(new IsolatedSegmentStorage.IndexCheckpointRequest(
                        (result, cause) -> reply.complete(result))));
                assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.TRANSIENT_UNAVAILABLE,
                        reply.get(5, TimeUnit.SECONDS).status());
            }
            IsolatedSegmentStorage.IndexCheckpointResult cleaned = cleanupAfterBackpressure(storage);
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED, cleaned.status());
            assertEquals(0, cleaned.coverage().entries());
            assertTrue(nativeIndex.get(context(), 1).isEmpty());
            storageWrite(storage, context(48), 1);
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
        }
    }

    @Test
    public void nativeTerminalCleanupFailureDoesNotReleaseOccupiedCoordinates() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("failed-native-terminal-cleanup");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            storageWrite(storage, context(), 1);
            checkpointAfterBackpressure(storage, new SegmentFileIo());
            installStorage(storage, tombstone(44, 20));
            // Close the actual native handle to inject its lifecycle failure, without a mock backend.
            var nativeIndex = IsolatedSegmentStorage.class.getDeclaredField("index");
            nativeIndex.setAccessible(true);
            ((SegmentDerivedIndex) nativeIndex.get(storage)).close();
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> cleanup = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.reclaimTerminalEntries(new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, cause) -> {
                        failure.set(cause);
                        cleanup.complete(result);
                    })));
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN,
                    cleanup.get(5, TimeUnit.SECONDS).status());
            assertTrue(failure.get() instanceof IOException);
            CompletableFuture<SegmentDataPhysicalDriver.NormalResult> refused = new CompletableFuture<>();
            ByteBuf input = entry(context(48), 1);
            try {
                assertTrue(storage.submitNormal(context(48), filled(20, (byte) 5), input,
                        new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> refused.complete(result))));
            } finally {
                input.release();
            }
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.TRANSIENT_UNAVAILABLE,
                    refused.get(5, TimeUnit.SECONDS).status());
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.restart();
            storageWrite(storage, context(48), 1);
        }
    }

    @Test
    public void terminalCleanupPublicationFailureKeepsNativeDeletionAndRestoresAdmission() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("failed-terminal-publication");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4));
            storageWrite(storage, context(), 1);
            checkpointAfterBackpressure(storage, new SegmentFileIo());
            installStorage(storage, tombstone(44, 20));
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> reply = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.reclaimTerminalEntries(new IsolatedSegmentStorage.IndexCheckpointRequest(
                    (result, cause) -> {
                        failure.set(cause);
                        reply.complete(result);
                    }), new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN,
                    reply.get(5, TimeUnit.SECONDS).status());
            assertTrue(failure.get() instanceof IOException);
            storageWrite(storage, context(48), 1);
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 4, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 1).result()) {
                assertEquals(1, read.result().record().entryId());
            }
            assertEquals(1, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().entries());
        }
    }

    @Test
    public void reusedAllocationKeepsOldTailOutsideRequiredDataAcrossFullAndCoveredRestart() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("reused-closed-tail");
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        byte[] oldTail = new byte[4096];
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), install(52, 5, (byte) 5), activate(52, 6)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096), reclaimableAllocation(102, upper)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44), arena);
            files.writeAuthorized(reclamationBatch(config, 101, 8192, 2, 44), arena);
            files.writeAuthorized(reclamationBatch(config, 102, upper, 3, 52), arena);
            try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                SegmentFileIo.readFully(file, ByteBuffer.wrap(oldTail), 8192);
            }
            bookie.submitBatch(List.of(tombstone(44, 20)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                arena.submitBatch(List.of(free));
                SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                assertEquals(4096, reused.offset());
                assertEquals(2, reused.expectedGeneration());
                assertFalse(arena.hasAppliedGrant(0, 4096, 4096, 0, 1, reused.allocationId(), 2));
                SegmentArenaControlLog.DurableBatch durable = arena.appendDurably(List.of(reused));
                assertFalse(arena.hasAppliedGrant(0, 4096, 4096, 0, 1, reused.allocationId(), 2));
                assertEquals(2, arena.applyDurable(durable).get(0).generation());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        arena.submitBatch(List.of(free)).get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        arena.resolvedFreeResult(free.operationId(), 101).status());
                assertThrows(IOException.class, () -> files.writeAuthorized(
                        reclamationBatch(config, 101, 4096, 4, 44), arena));
                SegmentArenaControlLog.Allocation competing = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 401), new Generation(1), 2,
                        0, 0, new Generation(1), 104, 4096, 1, 1);
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        arena.submitBatch(List.of(competing)).get(0).status());
                ByteBuf input = entry(context(48), 4);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                    new Generation(1), 4, 4096),
                            List.of(SegmentBatchFormat.entry(context(48), input))), arena);
                } finally {
                    input.release();
                }
                try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0))) {
                    ByteBuffer remaining = ByteBuffer.allocate(4096);
                    SegmentFileIo.readFully(file, remaining, 8192);
                    assertArrayEquals(oldTail, remaining.array());
                }
                assertThrows(IOException.class, () -> arena.finishDataSession(files, true));
            }
            files.close();
            assertFalse(arena.hasUnclosedDataSession());
            arena.rotate();
        }
        try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0), StandardOpenOption.WRITE)) {
            Arrays.fill(oldTail, (byte) 0x7f);
            new SegmentFileIo().writeFully(file, ByteBuffer.wrap(oldTail), 8192);
            file.force(true);
        }
        for (int restart = 0; restart < 2; restart++) {
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
                storage.restart();
                for (long ledger : new long[] {48, 52}) {
                    long entryId = ledger == 48 ? 4 : 3;
                    storageWrite(storage, context(ledger), entryId);
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(ledger), entryId).result()) {
                        assertEquals(entryId, read.result().record().entryId());
                    }
                }
                assertEquals(2, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().entries());
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            UUID generation = index.coverage().orElseThrow().indexGeneration();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, new SegmentFileIo(), 32, 4, index)) {
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(2, files.startupInventory().scan().coveredBatches());
                assertEquals(generation, index.coverage().orElseThrow().indexGeneration());
            }
        }
    }

    @Test
    public void retiredStreamRangesMergeRepeatedReuseAndRestoreCurrentDataPrefix() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("retired-range-reuse");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            for (int cycle = 0; cycle <= 24; cycle++) {
                long ledger = 44 + 4L * cycle;
                SegmentArenaControlLog.Allocation allocation = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                arena.submitBatch(List.of(allocation));
                assertEquals(4096, allocation.offset());
                assertEquals(cycle + 1, allocation.generation());
                ByteBuf input = entry(context(ledger), cycle + 1);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, allocation.allocationId(),
                                    new Generation(allocation.generation()), new Generation(1), cycle + 1, 4096),
                            List.of(SegmentBatchFormat.entry(context(ledger), input))), arena);
                } finally {
                    input.release();
                }
                if (cycle == 24) {
                    bookie.submitBatch(List.of(install(ledger, 4000, (byte) 5), activate(ledger, 4001)));
                } else {
                    bookie.submitBatch(List.of(tombstone(ledger, 2000 + cycle)));
                    try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 3000 + cycle),
                            allocation.allocationId(), files, bookie, ByteBuffer.allocate(4096))) {
                        arena.submitBatch(List.of(free));
                        List<SegmentArenaControlLog.RetiredRange> ranges = arena.retiredStreamRanges();
                        assertEquals(1, ranges.size());
                        assertEquals(1, ranges.get(0).firstSequence());
                        assertEquals(cycle + 1, ranges.get(0).through());
                        assertEquals(free.facts().bookieCut(), ranges.get(0).bookieCut());
                    }
                }
            }
            files.close();
            arena.rotate();
            bookie.rotate();
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(140), 25).result()) {
                assertEquals(25, read.result().record().entryId());
            }
            SegmentArenaControlLog arena = (SegmentArenaControlLog) storageField(storage, "arena");
            assertEquals(1, arena.retiredStreamRanges().size());
            assertEquals(24, arena.retiredStreamRanges().get(0).through());
        }
    }

    @Test
    public void retiredStreamRangesKeepUnwrittenCurrentAllocationOrderBetweenRetirements() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("retired-range-live-order");
        long middle = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        long last = middle + SegmentArenaControlLog.BLOCK_BYTES;
        long middleOrder;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            middleOrder = arena.submitBatch(List.of(reclaimableAllocation(101, 4096),
                    reclaimableAllocation(102, middle), reclaimableAllocation(103, last))).get(1).sequence();
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44), arena);
            files.writeAuthorized(reclamationBatch(config, 103, last, 2, 48), arena);
            bookie.submitBatch(List.of(tombstone(44, 201), tombstone(48, 202)));
            for (long allocation : new long[] {101, 103}) {
                try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 500 + allocation),
                        allocation, files, bookie, ByteBuffer.allocate(4096))) {
                    arena.submitBatch(List.of(free));
                }
            }
            assertEquals(2, arena.retiredStreamRanges().size());
            files.close();
            arena.rotate();
        }
        try (IndexFixture recovered = new IndexFixture(config, identity())) {
            assertEquals(2, recovered.arena.retiredStreamRanges().size());
            assertEquals(List.of(new SegmentDataRecovery.StreamCoverage(0, new Generation(1), 1, 2)),
                    recovered.inventory.indexStreams(recovered.arena));
            assertEquals(102, recovered.arena.currentAllocation(102).allocationId());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(),
                        arena, bookie, new SegmentFileIo(), 32, 4)) {
            bookie.submitBatch(List.of(install(52, 701, (byte) 5), activate(52, 702)));
            List<SegmentArenaControlLog.RetiredRange> before = arena.retiredStreamRanges();
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 703), 102,
                    files, bookie, ByteBuffer.allocate(4096))) {
                assertEquals(0, free.facts().records());
                assertEquals(middleOrder, free.facts().allocationOrder());
                arena.submitBatch(List.of(free));
                assertEquals(free.facts().bookieCut(), arena.requiredRetiredBookieCut());
            }
            assertEquals(2, before.size());
            assertEquals(1, arena.retiredStreamRanges().size());
            assertEquals(2, arena.retiredStreamRanges().get(0).through());
            SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                    new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
            assertEquals(104, reused.allocationId());
            assertEquals(2, reused.generation());
            arena.submitBatch(List.of(reused));
            ByteBuf input = entry(context(52), 3);
            try {
                files.writeAuthorized(SegmentBatchFormat.encode(config,
                        new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                new Generation(1), 3, reused.offset()),
                        List.of(SegmentBatchFormat.entry(context(52), input))), arena);
            } finally {
                input.release();
            }
            files.close();
            arena.rotate();
            bookie.rotate();
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(52), 3).result()) {
                assertEquals(3, read.result().record().entryId());
            }
        }
    }

    @Test
    public void issuedAllocationIdentityPreservesHolesHighWatermarkAndAbaAcrossCheckpointFallback() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("issued-allocation-identities", 8L * 1024 * 1024);
        long occupied = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES;
        long unused = occupied + SegmentArenaControlLog.BLOCK_BYTES;
        SegmentControlCheckpoint.Reference previous = null;
        SegmentControlCheckpoint.Reference latest = null;
        long order;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096),
                    reclaimableAllocation(103, 4096 + SegmentArenaControlLog.BLOCK_BYTES)));
            for (long id : new long[] {101, 103}) {
                try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 800 + id), id,
                        files, bookie, ByteBuffer.allocate(4096))) {
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            arena.submitBatch(List.of(free)).get(0).status());
                }
            }
            for (int cycle = 0; cycle < 8; cycle++) {
                previous = latest;
                latest = arena.checkpoint();
            }
            SegmentArenaControlLog.Result accepted = arena.submitBatch(
                    List.of(reclaimableAllocation(102, occupied))).get(0);
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, accepted.status());
            order = accepted.sequence();
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    arena.submitBatch(List.of(reclaimableAllocation(2000, occupied))).get(0).status());
            files.close();
        }
        int cycle = 0;
        for (SegmentControlCheckpoint.Reference expected : List.of(latest, previous)) {
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(),
                            arena, bookie, new SegmentFileIo(), 32, 4)) {
                assertEquals(expected, arena.publishedCheckpoint().orElseThrow());
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        arena.submitBatch(List.of(reclaimableAllocation(102, occupied))).get(0).status());
                assertEquals(order, arena.appliedAllocationOrder(new SegmentBatchFormat.Address(
                        0, 0, 102, new Generation(1), new Generation(1), 1, occupied)));
                SegmentArenaControlLog.Allocation aba = new SegmentArenaControlLog.Allocation(
                        SegmentControlFormat.Type.ALLOC_POOL, new UUID(0, 1901 + cycle), new Generation(1),
                        0, 0, 0, new Generation(1), 101, unused, 1, 1);
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        arena.submitBatch(List.of(aba)).get(0).status());
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        arena.submitBatch(List.of(reclaimableAllocation(2000, occupied))).get(0).status());
                SegmentArenaControlLog.Allocation planned = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                assertEquals(104 + cycle, planned.allocationId());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        arena.submitBatch(List.of(planned)).get(0).status());
            }
            if (cycle++ == 0) {
                byte[] damaged = Files.readAllBytes(SegmentControlCheckpoint.path(
                        config, SegmentControlFormat.Owner.ARENA, latest.slot()));
                damaged[4224] ^= 1;
                Path image = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, latest.slot());
                Files.write(image, damaged);
                try (FileChannel channel = FileChannel.open(image, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
            }
        }
    }

    @Test
    public void emptyRetiredAllocationStillRequiresItsBookieControlCut() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("empty-retired-range-cut");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            bookie.submitBatch(List.of(install(44, 201, (byte) 5), activate(44, 202)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 601), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                assertEquals(0, free.facts().records());
                assertTrue(free.facts().bookieCut() > 0);
                arena.submitBatch(List.of(free));
            }
            files.close();
            arena.rotate();
        }
        try (FileChannel control = FileChannel.open(config.directory().resolve("bookie-control"),
                StandardOpenOption.WRITE)) {
            control.truncate(4096);
            control.force(true);
        }
        IOException missing = assertThrows(IOException.class, () -> {
            try (IndexFixture recovered = new IndexFixture(config, identity())) {
                recovered.inventory.indexStreams(recovered.arena);
            }
        });
        assertTrue(missing.getMessage().contains("missing required Bookie authority cut"));
    }

    @Test
    public void runtimeAllocatorReusesOnePhysicalBlockAcrossTwoTerminalFreeGenerations() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("runtime-reuse-generations");
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.startNew();
            installStorage(storage, install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), install(52, 5, (byte) 5), activate(52, 6));
            for (int cycle = 0; cycle < 3; cycle++) {
                long ledger = 44 + 4L * cycle;
                SegmentBatchFormat.Address address = storageWrite(storage, context(ledger), cycle + 1)
                        .candidate().address();
                assertEquals(4096, address.offset());
                assertEquals(cycle + 1, address.generation().value());
                if (cycle < 2) {
                    installStorage(storage, tombstone(ledger, 20 + cycle));
                    CompletableFuture<IsolatedSegmentStorage.FreeResult> reply = new CompletableFuture<>();
                    assertTrue(storage.reclaimAllocation(new UUID(0, 501 + cycle), address.allocation(),
                            new IsolatedSegmentStorage.FreeRequest((result, cause) -> {
                                if (cause == null) {
                                    reply.complete(result);
                                } else {
                                    reply.completeExceptionally(cause);
                                }
                            })));
                    assertEquals(IsolatedSegmentStorage.FreeStatus.APPLIED_DURABLE,
                            reply.get(5, TimeUnit.SECONDS).status());
                }
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(52), 3).result()) {
                assertEquals(3, read.result().record().entryId());
            }
        }
    }

    @Test
    public void unwrittenReusedGrantDoesNotReviveOldGenerationOrInventSequence() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("unwritten-reused-grant");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44), arena);
            bookie.submitBatch(List.of(tombstone(44, 20)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                arena.submitBatch(List.of(free));
            }
            SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                    new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
            arena.submitBatch(List.of(reused));
            files.close();
            assertEquals(4096, arena.sealedAllocationEnd(reused.allocationId()));
            arena.rotate();
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            installStorage(storage, install(48, 21, (byte) 5), activate(48, 22));
            assertEquals(2, storageWrite(storage, context(48), 2).candidate().address().sequence());
        }
    }

    @Test
    public void damagedRequiredReusedPrefixCannotBeClassifiedAsUnusedOldTail() throws Exception {
        for (boolean zero : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("damaged-reused-" + zero);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                files.startSession(arena);
                bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2)));
                arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
                try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                        files, bookie, ByteBuffer.allocate(4096))) {
                    arena.submitBatch(List.of(free));
                }
                SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                arena.submitBatch(List.of(reused));
                ByteBuf input = entry(context(), 1);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                    new Generation(1), 1, 4096),
                            List.of(SegmentBatchFormat.entry(context(), input))), arena);
                } finally {
                    input.release();
                }
                files.close();
            }
            try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ByteBuffer damage = ByteBuffer.allocate(4096);
                if (!zero) {
                    SegmentFileIo.readFully(file, damage, 4096);
                    damage.put(200, (byte) (damage.get(200) ^ 1));
                }
                new SegmentFileIo().writeFully(file, damage, 4096);
                file.force(true);
            }
            try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
                assertThrows(IOException.class, storage::restart);
            }
        }
    }

    @Test
    public void reusedPartialWriteAndForceFailureKeepOpenSessionUnrecoverable() throws Exception {
        for (boolean partial : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("failed-reused-" + partial);
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                files.startSession(arena);
                arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
                try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                        files, bookie, ByteBuffer.allocate(4096))) {
                    arena.submitBatch(List.of(free));
                }
                SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                arena.submitBatch(List.of(reused));
                files.dataIo = partial ? new SegmentFileIo(128, 2, -1, false)
                        : new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
                ByteBuf input = entry(context(), 1);
                try {
                    SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                    new Generation(1), 1, 4096), List.of(SegmentBatchFormat.entry(context(), input)));
                    assertThrows(IOException.class, () -> files.writeAuthorized(batch, arena));
                } finally {
                    input.release();
                }
                assertThrows(IOException.class, files::close);
                assertTrue(arena.hasUnclosedDataSession());
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertTrue(arena.hasUnclosedDataSession());
                assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                        config, identity(), arena, new SegmentFileIo()));
            }
        }
    }

    @Test
    public void wholeDeadAllocationFreePreservesLiveSharedFileDataAcrossFullAndCoveredRestart() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("whole-allocation-free");
        long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4), install(52, 5, (byte) 5), activate(52, 6)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096), reclaimableAllocation(102, upper)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44, 48), arena);
            files.writeAuthorized(reclamationBatch(config, 102, upper, 2, 52), arena);
            bookie.submitBatch(List.of(tombstone(44, 20)));
            IOException live = assertThrows(IOException.class, () -> arena.prepareFree(
                    new UUID(0, 301), 101, files, bookie, ByteBuffer.allocate(4096)));
            assertTrue(live.getMessage(), live.getMessage().contains("exact terminal tombstone"));
            assertTrue(arena.hasAppliedGrant(0, 4096, 4096, 0, 1, 101, 1));
            bookie.submitBatch(List.of(tombstone(48, 21)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 302), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                assertEquals(2, free.facts().records());
                assertEquals(1, free.facts().firstSequence());
                assertEquals(1, free.facts().through());
                assertThrows(IOException.class, files::pinDataRead);
                assertThrows(IOException.class, files::close);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        arena.submitBatch(List.of(free)).get(0).status());
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        arena.submitBatch(List.of(free)).get(0).status());
            }
            assertEquals(2, arena.allocationGeneration(0, 4096));
            assertFalse(arena.hasAppliedGrant(0, 4096, 4096, 0, 1, 101, 1));
            assertEquals(upper + SegmentArenaControlLog.BLOCK_BYTES, arena.grantedHighWatermark(0));
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    arena.submitBatch(List.of(reclaimableAllocation(103, 4096))).get(0).status());
            files.close();
            arena.rotate();
        }
        // A legal retirement must make these bytes irrelevant, including after checkpoint/rotation replay.
        try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0), StandardOpenOption.WRITE)) {
            ByteBuffer garbage = ByteBuffer.allocate(4096);
            Arrays.fill(garbage.array(), (byte) 0x7f);
            new SegmentFileIo().writeFully(file, garbage, 4096);
            file.force(true);
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(52), 2).result()) {
                assertEquals(2, read.result().record().entryId());
            }
            storageWrite(storage, context(52), 2);
            storageWrite(storage, context(52), 3);
            assertEquals(2, checkpointAfterBackpressure(storage, new SegmentFileIo()).coverage().entries());
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            for (long entryId : new long[] {2, 3}) {
                storageWrite(storage, context(52), entryId);
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(52), entryId).result()) {
                    assertEquals(entryId, read.result().record().entryId());
                }
            }
        }
    }

    @Test
    public void wholeAllocationFreeRequiresActualWriterAndReadQuiescence() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-quiescence");
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(44, 1, (byte) 5), activate(44, 2),
                    install(48, 3, (byte) 5), activate(48, 4)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            try (IsolatedSegmentFiles.DataReadLease read = files.pinDataRead()) {
                assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 301), 101,
                        files, bookie, ByteBuffer.allocate(4096)));
            }
            SegmentBookieControlLog.Admission admission = bookie.admitNormal(
                    context(), filled(20, (byte) 5)).orElseThrow();
            assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 302), 101,
                    files, bookie, ByteBuffer.allocate(4096)));
            IsolatedSegmentFiles.Reservation pending = files.reserveAuthorized(
                    reclamationBatch(config, 101, 4096, 1, 44), arena);
            assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 302), 101,
                    files, bookie, ByteBuffer.allocate(4096)));
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                atForce.countDown();
                await(release);
            });
            FutureTask<Void> write = new FutureTask<>(() -> {
                files.writeReserved(pending);
                return null;
            });
            new Thread(write, "free-old-data-io").start();
            try {
                assertTrue(atForce.await(5, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 303), 101,
                        files, bookie, ByteBuffer.allocate(4096)));
            } finally {
                release.countDown();
            }
            write.get(5, TimeUnit.SECONDS);
            admission.physicalTerminal();
            bookie.submitBatch(List.of(tombstone(44, 20)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 304), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                assertThrows(IOException.class, files::pinDataRead);
                assertTrue(bookie.admitNormal(context(48), filled(20, (byte) 5)).isEmpty());
                assertTrue(bookie.admitRead(context(48)).isEmpty());
                assertThrows(IOException.class, bookie::close);
                assertThrows(IOException.class, () -> files.reserveAuthorized(
                        reclamationBatch(config, 101, 8192, 2, 44), arena));
            }
            try (IsolatedSegmentFiles.DataReadLease read = files.pinDataRead()) {
                assertEquals(1, arena.allocationGeneration(0, 4096));
                bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow().physicalTerminal();
            }
            files.close();
        } finally {
            release.countDown();
        }
    }

    @Test
    public void submittedFreeKeepsProtectionUntilControlIoAndOriginalOwnerFinish() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-owner");
        AtomicBoolean armed = new AtomicBoolean();
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch ownerBlocked = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (armed.get()) {
                atForce.countDown();
                await(release);
            }
        });
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44), arena);
            bookie.submitBatch(List.of(tombstone(44, 20)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 2, 1, 4096, 4096,
                            4096L + 4095 + 4096, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentArenaControlDriver allocator = new SegmentArenaControlDriver(owner, group.io(), arena);
            CompletableFuture<SegmentArenaControlLog.Result> reply = new CompletableFuture<>();
            SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096));
            try {
                armed.set(true);
                long previousDurable = arena.durableThrough();
                assertTrue(allocator.submitFree(free, (results, cause) -> {
                    owner.requireOwner();
                    if (cause == null) {
                        reply.complete(results.get(0));
                    } else {
                        reply.completeExceptionally(cause);
                    }
                }));
                assertTrue(atForce.await(5, TimeUnit.SECONDS));
                assertTrue(owner.control(() -> {
                    ownerBlocked.countDown();
                    await(releaseOwner);
                }));
                assertTrue(ownerBlocked.await(5, TimeUnit.SECONDS));
                assertThrows(IllegalStateException.class, free::close);
                assertThrows(IOException.class, () -> arena.submitBatch(List.of(free)));
                assertThrows(IOException.class, files::pinDataRead);
                assertFalse(group.shutdown(Duration.ofMillis(50)));
                assertFalse(reply.isDone());
                release.countDown();
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (arena.durableThrough() == previousDurable && System.nanoTime() < end) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
                assertTrue(arena.durableThrough() > previousDurable);
                assertEquals(1, arena.allocationGeneration(0, 4096));
                assertThrows(IOException.class, files::pinDataRead);
                assertFalse(reply.isDone());
                releaseOwner.countDown();
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        reply.get(5, TimeUnit.SECONDS).status());
                try (IsolatedSegmentFiles.DataReadLease read = files.pinDataRead()) {
                    assertEquals(2, arena.allocationGeneration(0, 4096));
                }
            } finally {
                release.countDown();
                releaseOwner.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                free.close();
            }
            files.close();
        }
    }

    @Test
    public void runtimeWholeAllocationFreeUsesExistingStorageGraphAndRestoresRetiredPrefix() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-runtime-graph");
        long allocation;
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.startNew();
            installStorage(storage, install(1, (byte) 5), activate(2));
            allocation = storageWrite(storage, context(), 1).candidate().address().allocation();
            installStorage(storage, tombstone(44, 20));
            CompletableFuture<IsolatedSegmentStorage.FreeResult> reply = new CompletableFuture<>();
            assertTrue(storage.reclaimAllocation(new UUID(0, 301), allocation,
                    new IsolatedSegmentStorage.FreeRequest((result, cause) -> {
                        if (cause == null) {
                            reply.complete(result);
                        } else {
                            reply.completeExceptionally(cause);
                        }
                    })));
            assertEquals(IsolatedSegmentStorage.FreeStatus.APPLIED_DURABLE,
                    reply.get(5, TimeUnit.SECONDS).status());
            for (long target : new long[] {allocation, allocation + 1}) {
                CompletableFuture<IsolatedSegmentStorage.FreeResult> retry = new CompletableFuture<>();
                assertTrue(storage.reclaimAllocation(new UUID(0, 301), target,
                        new IsolatedSegmentStorage.FreeRequest((result, cause) -> {
                            if (cause == null) {
                                retry.complete(result);
                            } else {
                                retry.completeExceptionally(cause);
                            }
                        })));
                SegmentArenaControlLog.Result result = retry.get(5, TimeUnit.SECONDS).control();
                assertEquals(target == allocation ? SegmentArenaControlLog.Status.ALREADY_DURABLE
                        : SegmentArenaControlLog.Status.INCOMPATIBLE, result.status());
                assertEquals(reply.get().control().sequence(), result.sequence());
                assertEquals(target == allocation ? 2 : 0, result.generation());
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(2, arena.retiredAllocations().get(0).operationGeneration().value());
            assertEquals(allocation, arena.retiredAllocations().get(0).facts().allocation().allocationId());
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            installStorage(storage, install(48, 21, (byte) 5), activate(48, 22));
            storageWrite(storage, context(48), 2);
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 2).result()) {
                assertEquals(2, read.result().record().entryId());
            }
        }
    }

    @Test
    public void wholeAllocationFreeRejectsInactiveRecordsAndUnverifiableDeadData() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-proof-refusal");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            bookie.submitBatch(List.of(install(1, (byte) 5)));
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44, 48), arena);
            assertTrue(bookie.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
            assertFalse(bookie.locallyReadableContext(context(48)));
            assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096)));
            bookie.submitBatch(List.of(tombstone(44, 20)));
            assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 302), 101,
                    files, bookie, ByteBuffer.allocate(4096)));
            bookie.submitBatch(List.of(tombstone(48, 21)));
            try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.WRITE)) {
                file.write(ByteBuffer.wrap(new byte[] {1}), 4096 + 128 + 12);
                file.force(true);
            }
            assertThrows(IOException.class, () -> arena.prepareFree(new UUID(0, 303), 101,
                    files, bookie, ByteBuffer.allocate(4096)));
            assertTrue(arena.retiredAllocations().isEmpty());
            assertEquals(1, arena.allocationGeneration(0, 4096));
            files.close();
        }
    }

    @Test
    public void wholeAllocationFreeForceUnknownCannotPublishHealthyCloseOrReopen() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-force-unknown");
        AtomicBoolean armed = new AtomicBoolean();
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (armed.get()) {
                throw new IllegalStateException("FREE barrier outcome is unknown");
            }
        });
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024, controlIo);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            files.writeAuthorized(reclamationBatch(config, 101, 4096, 1, 44), arena);
            bookie.submitBatch(List.of(tombstone(44, 20)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                armed.set(true);
                assertThrows(IllegalStateException.class, () -> arena.submitBatch(List.of(free)));
            }
            assertFalse(arena.dataFileAvailable(0));
            assertThrows(IOException.class, () -> files.reserveAuthorized(
                    reclamationBatch(config, 101, 8192, 2, 44), arena));
            assertThrows(IOException.class, files::close);
            assertTrue(arena.hasUnclosedDataSession());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertTrue(arena.hasUnclosedDataSession());
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openAppendOnly(
                    config, identity(), arena, new SegmentFileIo()));
        }
    }

    @Test
    public void emptyAllocationFreeDoesNotInventAStreamSequence() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("free-empty-allocation");
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            files.startSession(arena);
            arena.submitBatch(List.of(reclaimableAllocation(101, 4096)));
            try (SegmentArenaControlLog.Free free = arena.prepareFree(new UUID(0, 301), 101,
                    files, bookie, ByteBuffer.allocate(4096))) {
                assertEquals(0, free.facts().records());
                assertEquals(-1, free.facts().firstSequence());
                assertEquals(-1, free.facts().through());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        arena.submitBatch(List.of(free)).get(0).status());
            }
            files.close();
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 4, 32, 3)) {
            storage.restart();
            installStorage(storage, install(1, (byte) 5), activate(2));
            assertEquals(1, storageWrite(storage, context(), 1).candidate().address().sequence());
        }
    }

    @Test
    public void coveredRestartReusesNativeGenerationAndValidatesOnlyUncoveredBodies() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("covered-restart", 5L * 1024 * 1024);
        CoveredTail source = createCoveredTail(config);
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            assertEquals(source.coverage(), index.coverage().orElseThrow());
            assertTrue(index.get(context(), 2).isEmpty());
            SegmentFileIo failedSync = new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false);
            assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, failedSync, 32, 4, index));
            assertEquals(1, failedSync.forces);
            assertEquals(source.coverage(), index.coverage().orElseThrow());
            SegmentFileIo sync = new SegmentFileIo();
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, sync, 32, 4, index)) {
                SegmentDataRecovery.Scan scan = files.startupInventory().scan();
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(1, scan.coveredBatches());
                assertEquals(4096 - SegmentBatchFormat.HEADER, scan.skippedBodyBytes());
                assertEquals(2, scan.batches());
                assertEquals(2, scan.records());
                assertEquals(8192, scan.validatedBytes());
                assertEquals(1, sync.forces);
                assertEquals(3, files.startupInventory().entries().size());
                assertEquals(2, SegmentPointRead.read(config, identity(), arena,
                        index.get(context(), 2).orElseThrow()).record().entryId());
                assertEquals(45, SegmentPointRead.read(config, identity(), arena,
                        index.get(context(45), 1).orElseThrow()).record().context().ledgerId());
                SegmentDerivedIndex.Coverage recovered = index.coverage().orElseThrow();
                assertEquals(source.coverage().indexGeneration(), recovered.indexGeneration());
                assertEquals(source.coverage().publication() + 1, recovered.publication());
                assertEquals(3, recovered.entries());
                assertEquals(2, recovered.streams().size());
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 2, 6, 16, 32)) {
            storage.restart();
            assertEquals(source.first().candidate(), storageWrite(storage, context(), 1).candidate());
            storageWrite(storage, context(), 2);
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(45), 1).result()) {
                assertEquals(45, read.result().record().context().ledgerId());
            }
            storageWrite(storage, context(), 3);
        }
    }

    @Test
    public void coveredRestartInvalidPageContentsOrDeletedDbFallsBackToFullDataRebuild() throws Exception {
        for (String mode : new String[] {"bad-page", "new-native-contents", "deleted-db"}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig(mode);
            CoveredTail source = createCoveredTail(config);
            if (mode.equals("bad-page")) {
                Files.write(SegmentDerivedIndex.publication(config, 0), new byte[4096]);
            } else if (mode.equals("new-native-contents")) {
                try (SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
                    index.append(List.of(source.tail().candidate()));
                }
            } else {
                try (java.util.stream.Stream<Path> paths = Files.walk(SegmentDerivedIndex.directory(config))) {
                    for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                        Files.delete(path);
                    }
                }
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                            config, identity(), arena, bookie, new SegmentFileIo(), 32, 4, index)) {
                assertFalse(files.startupInventory().usesIndexCoverage());
                assertEquals(0, files.startupInventory().scan().coveredBatches());
                assertEquals(3, files.startupInventory().scan().batches());
                assertEquals(3, files.startupInventory().entries().size());
                assertFalse(source.coverage().indexGeneration().equals(
                        index.coverage().orElseThrow().indexGeneration()));
                assertEquals(1, SegmentPointRead.read(config, identity(), arena,
                        index.get(context(), 1).orElseThrow()).record().entryId());
            }
        }
    }

    @Test
    public void coveredRestartRejectsBadRequiredTailAndCoveredReadsStillValidateRecords() throws Exception {
        for (int damage : new int[] {0, 1, 2}) {
            boolean damageCovered = damage != 0;
            IsolatedSegmentConfig config = coverageRecoveryConfig("damage-" + damage);
            CoveredTail source = createCoveredTail(config);
            SegmentPointRead.Location damaged = (damageCovered ? source.first() : source.tail()).candidate();
            try (FileChannel file = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                if (damage == 2) {
                    new SegmentFileIo().writeFully(file, ByteBuffer.wrap(new byte[4096]), damaged.address().offset());
                } else {
                    new SegmentFileIo().writeFully(file, ByteBuffer.wrap(new byte[] {1}), damaged.recordOffset()
                            + SegmentBatchFormat.RECORD_HEADER + SegmentBatchFormat.ENTRY_OVERHEAD);
                }
                file.force(false);
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
                if (damage != 1) {
                    assertThrows(IOException.class, () -> IsolatedSegmentFiles.openForLocalResults(
                            config, identity(), arena, bookie, new SegmentFileIo(), 32, 4, index));
                } else {
                    try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                            config, identity(), arena, bookie, new SegmentFileIo(), 32, 4, index)) {
                        assertEquals(1, files.startupInventory().scan().coveredBatches());
                        IOException invalid = assertThrows(IOException.class, () -> SegmentPointRead.read(
                                config, identity(), arena, index.get(context(), 1).orElseThrow()));
                        assertTrue(invalid.getMessage().contains("CRC"));
                    }
                }
            }
        }
    }

    @Test
    public void coveredRestartReplaysTombstoneSuffixWithoutRevivingIndexedRows() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("covered-tombstone");
        CoveredTail source = createCoveredTail(config);
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 32)) {
            assertEquals(SegmentBookieControlLog.Status.APPLIED_DURABLE,
                    bookie.submitBatch(List.of(tombstone(44, 20))).get(0).status());
            assertTrue(bookie.recoveryReadCut(config, identity()) > source.coverage().bookieCut());
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                    config, identity(), arena, bookie, new SegmentFileIo(), 32, 4, index)) {
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(1, files.startupInventory().entries().size());
                assertTrue(index.get(context(), 1).isEmpty());
                assertTrue(index.get(context(), 2).isEmpty());
                assertEquals(1, index.coverage().orElseThrow().entries());
                assertFalse(bookie.locallyReadableContext(context()));
                assertEquals(45, SegmentPointRead.read(config, identity(), arena,
                        index.get(context(45), 1).orElseThrow()).record().context().ledgerId());
            }
        }
    }


    private static SegmentBatchFormat.FrozenBatch pooledRelocationBatch(IsolatedSegmentConfig config,
            SegmentOwnerLoop owner, long entryId, long sequence, long offset) throws Exception {
        CompletableFuture<SegmentBatchFormat.FrozenBatch> encoded = new CompletableFuture<>();
        assertTrue(owner.control(() -> {
            ByteBuf input = null;
            try {
                input = entry(context(), entryId);
                encoded.complete(SegmentBatchFormat.encodePooled(config, new SegmentBatchFormat.Address(0, 0, 5,
                        new Generation(1), new Generation(1), sequence, offset),
                        List.of(SegmentBatchFormat.relocation(context(), input, new UUID(6, 7), new Generation(1))),
                        owner.pool()));
            } catch (Throwable failure) {
                encoded.completeExceptionally(failure);
            } finally {
                if (input != null) {
                    input.release();
                }
            }
        }));
        return encoded.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void relocationDurabilityReturnsBoundCopyFactsWithoutOrdinaryPublication() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR)) {
            grant(arena, 1);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    prefix, hot);
            try {
                CompletableFuture<SegmentDataPhysicalDriver.DurableRelocation> reply = new CompletableFuture<>();
                ByteBuf first = entry(context(), 1);
                ByteBuf second = entry(context(48), 1);
                String digest;
                try {
                    byte[] input = new byte[first.readableBytes()];
                    first.getBytes(first.readerIndex(), input);
                    digest = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(input));
                    SegmentBatchFormat.FrozenBatch copies = SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                            List.of(SegmentBatchFormat.relocation(context(), first, new UUID(6, 7), new Generation(1)),
                                    SegmentBatchFormat.relocation(context(48), second,
                                            new UUID(6, 7), new Generation(1))));
                    assertTrue(driver.submitRelocation(copies, (copy, failure) -> {
                        owner.requireOwner();
                        if (failure == null) {
                            reply.complete(copy);
                        } else {
                            reply.completeExceptionally(failure);
                        }
                    }));
                } finally {
                    first.release();
                    second.release();
                }
                SegmentDataPhysicalDriver.DurableRelocation copy = reply.get(5, TimeUnit.SECONDS);
                assertTrue(config.sameStorageScope(copy.sourceConfig()));
                assertEquals(identity(), copy.sourceIdentity());
                assertEquals(new UUID(6, 7), copy.operationId());
                assertEquals(new Generation(1), copy.operationGeneration());
                assertEquals(2, copy.copies().size());
                assertEquals(digest, copy.copies().get(0).entrySha256());
                assertThrows(UnsupportedOperationException.class, () -> copy.copies().clear());
                for (SegmentDataPhysicalDriver.RelocationCopy record : copy.copies()) {
                    SegmentBatchParser.Record actual = SegmentPointRead.read(config, identity(), arena,
                            record.location()).record();
                    assertEquals(SegmentBatchFormat.RELOCATION_COPY, actual.origin());
                    assertEquals(copy.operationId(), actual.moveId());
                }
                FutureTask<Void> unpublished = new FutureTask<>(() -> {
                    assertEquals(1, prefix.durableThrough());
                    assertEquals(0, hot.candidateCount());
                    assertTrue(hot.liveDurableCandidate(context(), 1).isEmpty());
                    assertTrue(hot.liveDurableCandidate(context(48), 1).isEmpty());
                    return null;
                });
                assertTrue(owner.control(unpublished::run));
                unpublished.get(5, TimeUnit.SECONDS);
                CompletableFuture<List<SegmentDataPhysicalDriver.NormalResult>> normal = new CompletableFuture<>();
                assertTrue(driver.submitNormal(bookie, batch(config, 1, 2, 8192),
                        List.of(bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> {
                            if (failure == null) {
                                normal.complete(results);
                            } else {
                                normal.completeExceptionally(failure);
                            }
                        }));
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        normal.get(5, TimeUnit.SECONDS).get(0).status());
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            SegmentDataRecovery.PhysicalStreamCandidate recovered = SegmentDataRecovery.recoverPhysicalStreamCandidate(
                    config, identity(), arena, new SegmentFileIo(), 0, new Generation(1), 1, 2, 4);
            assertEquals(2, recovered.through());
            assertEquals(1, recovered.entries().size());
            assertEquals(2, recovered.entries().get(0).address().sequence());
        }
    }

    @Test
    public void relocationValidationConsumesPooledBatchesWithoutIoOrCoordinatePublication() throws Exception {
        IsolatedSegmentConfig config = config();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            grant(arena, 1);
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            try {
                FutureTask<Void> rejected = new FutureTask<>(() -> {
                    ByteBuf input = entry(context(), 1);
                    ByteBuf otherShard = entry(context(45), 1);
                    try {
                        SegmentBatchFormat.Data copy = SegmentBatchFormat.relocation(context(), input,
                                new UUID(6, 7), new Generation(1));
                        for (List<SegmentBatchFormat.Data> records : List.of(
                                List.of(SegmentBatchFormat.entry(context(), input)),
                                List.of(copy, SegmentBatchFormat.entry(context(), input)),
                                List.of(copy, SegmentBatchFormat.relocation(context(), input,
                                        new UUID(6, 8), new Generation(1))), List.of(copy, copy),
                                List.of(SegmentBatchFormat.relocation(context(45), otherShard,
                                        new UUID(6, 7), new Generation(1))))) {
                            SegmentBatchFormat.FrozenBatch invalid = SegmentBatchFormat.encodePooled(config,
                                    new SegmentBatchFormat.Address(0, 0, 5, new Generation(1),
                                            new Generation(1), 1, 4096), records, owner.pool());
                            assertTrue(invalid != null);
                            assertThrows(IOException.class, () -> driver.submitRelocation(invalid,
                                    (result, failure) -> fail("Rejected copy must not reach completion")));
                            owner.pool().drainReturns();
                            assertEquals(0, owner.pool().outstanding());
                        }
                        assertEquals(0, hot.candidateCount());
                        assertEquals(4096, files.nextOffset(0));
                        assertEquals(0, files.dataIo.writes);
                    } finally {
                        input.release();
                        otherShard.release();
                    }
                    return null;
                });
                assertTrue(owner.control(rejected::run));
                rejected.get(5, TimeUnit.SECONDS);
            } finally {
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void relocationWriteAndForceUnknownNeverReturnDurableCopyFacts() throws Exception {
        for (boolean force : new boolean[] {false, true}) {
            IsolatedSegmentConfig base = config();
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(base.directory().resolveSibling("copy-" + force),
                    1, 4, 1, 8L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                    SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                grant(arena, 1);
                files.dataIo = force ? new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)
                        : new SegmentFileIo(64, 2, -1, false);
                SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                        new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
                SegmentOwnerLoop owner = group.owner(0);
                SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
                SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4);
                SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                        prefix, hot);
                try {
                    CompletableFuture<Throwable> reply = new CompletableFuture<>();
                    assertTrue(driver.submitRelocation(pooledRelocationBatch(config, owner, 1, 1, 4096),
                            (copy, failure) -> {
                                assertNull(copy);
                                reply.complete(failure);
                            }));
                    assertTrue(reply.get(5, TimeUnit.SECONDS) instanceof IOException);
                    FutureTask<Void> released = new FutureTask<>(() -> {
                        owner.pool().drainReturns();
                        assertEquals(0, owner.pool().outstanding());
                        assertTrue(prefix.failed());
                        assertEquals(0, hot.candidateCount());
                        return null;
                    });
                    assertTrue(owner.control(released::run));
                    released.get(5, TimeUnit.SECONDS);
                } finally {
                    assertTrue(group.shutdown(Duration.ofSeconds(5)));
                }
            }
        }
    }

    @Test
    public void cancelledRelocationWaitKeepsActualForceAndOriginalPoolUntilTerminal() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch forcing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicReference<SegmentDataPhysicalDriver.DurableRelocation> actual = new AtomicReference<>();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            grant(arena, 1);
            files.dataIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                forcing.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 2, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4), hot);
            CompletableFuture<SegmentDataPhysicalDriver.DurableRelocation> waiting = new CompletableFuture<>();
            try {
                assertTrue(driver.submitRelocation(pooledRelocationBatch(config, owner, 1, 1, 4096),
                        (copy, failure) -> {
                            assertNull(failure);
                            actual.set(copy);
                            callbacks.incrementAndGet();
                            waiting.complete(copy);
                        }));
                assertTrue(forcing.await(5, TimeUnit.SECONDS));
                assertTrue(waiting.cancel(false));
                FutureTask<Void> retained = new FutureTask<>(() -> {
                    assertEquals(1, owner.pool().outstanding());
                    assertNull(pooledBatch(config, 2, 2, 8192, owner.pool()));
                    assertEquals(0, hot.candidateCount());
                    return null;
                });
                assertTrue(owner.control(retained::run));
                retained.get(5, TimeUnit.SECONDS);
                assertFalse(group.shutdown(Duration.ofMillis(20)));
                assertEquals(0, callbacks.get());
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
                assertEquals(1, callbacks.get());
                assertTrue(actual.get() != null);
                assertEquals(1, actual.get().copies().size());
            } finally {
                release.countDown();
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void laterCopyForceCannotReturnFactsAcrossAnEarlierPhysicalHole() throws Exception {
        IsolatedSegmentConfig config = config();
        CountDownLatch firstIoPaused = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger starts = new AtomicInteger();
        try (IsolatedSegmentFiles files = IsolatedSegmentFiles.create(config, identity());
                SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            grant(arena, 1);
            SegmentIoExecutor copyIo = new SegmentIoExecutor(2, 16,
                    task -> new Thread(task, "relocation-order-io"), () -> {
                if (starts.incrementAndGet() == 1) {
                    firstIoPaused.countDown();
                    try {
                        assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(failure);
                    }
                }
            });
            SegmentExecutionGroup group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 3, 1, 2, 4096, 44, 16426, Duration.ofSeconds(5)));
            SegmentOwnerLoop owner = group.owner(0);
            SegmentPhysicalPrefix prefix = new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 4);
            SegmentHotDirectory hot = new SegmentHotDirectory(owner, config, identity(), 4);
            SegmentDataPhysicalDriver driver = new SegmentDataPhysicalDriver(0, owner, copyIo, files, arena,
                    prefix, hot);
            try {
                CompletableFuture<SegmentDataPhysicalDriver.DurableRelocation> first = new CompletableFuture<>();
                CompletableFuture<SegmentDataPhysicalDriver.DurableRelocation> second = new CompletableFuture<>();
                assertTrue(driver.submitRelocation(pooledRelocationBatch(config, owner, 1, 1, 4096),
                        (copy, failure) -> first.complete(copy)));
                assertTrue(firstIoPaused.await(5, TimeUnit.SECONDS));
                assertTrue(driver.submitRelocation(pooledRelocationBatch(config, owner, 2, 2, 8192),
                        (copy, failure) -> second.complete(copy)));
                CompletableFuture<Void> laterIoReturned = new CompletableFuture<>();
                assertTrue(owner.submit(0, new SegmentOwnerLoop.Command() {
                    @Override
                    public void run(SegmentOwnerLoop.Ticket ticket) {
                        copyIo.submit(ticket, () -> { }, failure -> {
                            if (failure == null) {
                                laterIoReturned.complete(null);
                            } else {
                                laterIoReturned.completeExceptionally(failure);
                            }
                        });
                    }

                    @Override
                    public void failed(Throwable failure) {
                        laterIoReturned.completeExceptionally(failure);
                    }
                }));
                laterIoReturned.get(5, TimeUnit.SECONDS);
                assertFalse(first.isDone());
                assertFalse(second.isDone());
                FutureTask<Void> hole = new FutureTask<>(() -> {
                    assertEquals(0, prefix.durableThrough());
                    assertEquals(2, owner.pool().outstanding());
                    assertEquals(0, hot.candidateCount());
                    return null;
                });
                assertTrue(owner.control(hole::run));
                hole.get(5, TimeUnit.SECONDS);
                releaseFirst.countDown();
                assertEquals(1, first.get(5, TimeUnit.SECONDS).copies().get(0).location().address().sequence());
                assertEquals(2, second.get(5, TimeUnit.SECONDS).copies().get(0).location().address().sequence());
            } finally {
                releaseFirst.countDown();
                copyIo.shutdown();
                assertTrue(copyIo.awaitTermination(Duration.ofSeconds(5)));
                assertTrue(group.shutdown(Duration.ofSeconds(5)));
            }
        }
    }

    @Test
    public void coveredRecoveryRejectsSeedFromAnotherDataDirectoryBeforeSync() throws Exception {
        IsolatedSegmentConfig source = coverageRecoveryConfig("covered-source");
        IsolatedSegmentConfig target = coverageRecoveryConfig("covered-target");
        createCoveredTail(source);
        createCoveredTail(target);
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(source, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(source, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(source, identity(), 32);
                IndexFixture destination = new IndexFixture(target, identity())) {
            SegmentDerivedIndex.RecoveryCoverage seed = index.recoveryCoverage(arena, bookie).orElseThrow();
            SegmentFileIo sync = new SegmentFileIo();
            IOException wrongScope = assertThrows(IOException.class, () ->
                    SegmentDataRecovery.recoverLocallyReadableInventoryWhileLocked(target, identity(),
                            destination.arena, destination.bookie, sync, 32, 4, destination.guard.lock(), seed));
            assertTrue(wrongScope.getMessage().contains("scope"));
            assertEquals(0, sync.forces);
        }
    }


    /** Real DATA and control I/O using the existing isolated owner and completion services. */
    private final class MoveFixture implements AutoCloseable {
        private final IsolatedSegmentConfig config;
        private final IsolatedSegmentFiles files;
        private final SegmentArenaControlLog arena;
        private final SegmentBookieControlLog bookie;
        private final SegmentExecutionGroup group;
        private final SegmentOwnerLoop owner;
        private final SegmentHotDirectory hot;
        private final SegmentDataPhysicalDriver driver;
        private final SegmentDerivedIndex index;
        private final List<SegmentPointRead.Location> sources;
        private boolean failedCloseExpected;

        private MoveFixture(IsolatedSegmentConfig config, SegmentFileIo controlIo) throws Exception {
            this(config, controlIo, false);
        }

        private MoveFixture(IsolatedSegmentConfig config, SegmentFileIo controlIo, boolean indexed) throws Exception {
            this.config = config;
            files = IsolatedSegmentFiles.create(config, identity());
            arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024, controlIo);
            files.startSession(arena);
            grant(arena, 1);
            bookie = SegmentBookieControlLog.open(config, identity(), 1024 * 1024, key((byte) 3), VALIDATOR);
            bookie.submitBatch(List.of(install(1, (byte) 5), activate(2)));
            group = SegmentExecutionGroup.start(config,
                    new SegmentExecutionGroup.Settings(1, 4, 1, 1, 4096, 44, 8235, Duration.ofSeconds(5)));
            owner = group.owner(0);
            hot = new SegmentHotDirectory(owner, config, identity(), 4);
            index = indexed ? SegmentDerivedIndex.open(config, identity(), 16) : null;
            if (indexed) {
                FutureTask<Void> attach = new FutureTask<>(() -> {
                    hot.attachIndex(0, index, group.io());
                    return null;
                });
                assertTrue(owner.control(attach));
                attach.get(5, TimeUnit.SECONDS);
                ownerBarrier(owner);
            }
            driver = new SegmentDataPhysicalDriver(0, owner, group.io(), files, arena,
                    new SegmentPhysicalPrefix(owner, 0, new Generation(1), 1, 8), hot);
            ByteBuf first = entry(context(), 1);
            ByteBuf second = entry(context(), 2);
            CompletableFuture<List<SegmentDataPhysicalDriver.NormalResult>> written = new CompletableFuture<>();
            try {
                assertTrue(driver.submitNormal(bookie, SegmentBatchFormat.encode(config,
                        new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 1, 4096),
                        List.of(SegmentBatchFormat.entry(context(), first),
                                SegmentBatchFormat.entry(context(), second))),
                        List.of(bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> {
                            if (failure == null) {
                                written.complete(results);
                            } else {
                                written.completeExceptionally(failure);
                            }
                        }));
            } finally {
                first.release();
                second.release();
            }
            List<SegmentDataPhysicalDriver.NormalResult> results = written.get(5, TimeUnit.SECONDS);
            assertEquals(2, results.size());
            for (SegmentDataPhysicalDriver.NormalResult result : results) {
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
            }
            sources = results.stream().map(SegmentDataPhysicalDriver.NormalResult::candidate).toList();
            ownerBarrier(owner);
        }

        private SegmentDataPhysicalDriver.DurableRelocation copy(int operation, long sequence, byte payload)
                throws Exception {
            return copy(operation, sequence, payload, 5, sequence * 4096);
        }

        private SegmentDataPhysicalDriver.DurableRelocation copy(int operation, long sequence, byte payload,
                                                                  long allocation, long offset) throws Exception {
            ByteBuf first = entry(context(), 1, -1, 8, payload);
            ByteBuf second = entry(context(), 2, -1, 8, payload);
            CompletableFuture<SegmentDataPhysicalDriver.DurableRelocation> written = new CompletableFuture<>();
            try {
                assertTrue(driver.submitRelocation(SegmentBatchFormat.encode(config,
                        new SegmentBatchFormat.Address(0, 0, allocation, new Generation(1), new Generation(1),
                                sequence, offset),
                        List.of(SegmentBatchFormat.relocation(context(), first,
                                        new UUID(0, operation), new Generation(7)),
                                SegmentBatchFormat.relocation(context(), second,
                                        new UUID(0, operation), new Generation(7)))), (result, failure) -> {
                            if (failure == null) {
                                written.complete(result);
                            } else {
                                written.completeExceptionally(failure);
                            }
                        }));
            } finally {
                first.release();
                second.release();
            }
            SegmentDataPhysicalDriver.DurableRelocation result = written.get(5, TimeUnit.SECONDS);
            ownerBarrier(owner);
            return result;
        }

        private SegmentArenaMove prepare(SegmentDataPhysicalDriver.DurableRelocation copy,
                                         List<SegmentPointRead.Location> previous, long selector) throws IOException {
            return SegmentArenaMove.prepare(arena, files, bookie, copy,
                    previous.stream().map(location -> new SegmentArenaMove.Source(location, selector)).toList(),
                    ByteBuffer.allocate(4096));
        }

        private SegmentDerivedIndex.Coverage checkpointMoves(boolean refresh) throws Exception {
            SegmentHotDirectory.IndexCut cut = null;
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (cut == null && System.nanoTime() < end) {
                FutureTask<SegmentHotDirectory.IndexCut> pause = new FutureTask<>(hot::pauseIndexForCheckpoint);
                assertTrue(owner.control(pause));
                cut = pause.get(5, TimeUnit.SECONDS);
                ownerBarrier(owner);
                if (cut == null) {
                    Thread.sleep(1);
                }
            }
            assertNotNull(cut);
            try (SegmentBookieControlLog.ReclamationLease lease = bookie.protectReclamation(config, identity());
                    IsolatedSegmentFiles.ReclamationGuard guard = files.protectReclamation(arena, 0, lease)) {
                if (refresh) {
                    index.refreshMovedLocations(arena, files, guard, ByteBuffer.allocate(4096));
                }
                List<SegmentHotDirectory.IndexCut> cuts = new ArrayList<>();
                cuts.add(cut);
                for (int shard = 1; shard < config.logicalShards(); shard++) {
                    cuts.add(new SegmentHotDirectory.IndexCut(new SegmentDataRecovery.StreamCoverage(
                            shard, new Generation(1), 1, 0), 0));
                }
                return index.checkpointRuntime(cuts, arena, bookie, new SegmentFileIo());
            } finally {
                FutureTask<Void> resume = new FutureTask<>(() -> {
                    hot.resumeIndexAfterCheckpoint();
                    return null;
                });
                assertTrue(owner.control(resume));
                resume.get(5, TimeUnit.SECONDS);
                ownerBarrier(owner);
            }
        }

        @Override
        public void close() throws Exception {
            assertTrue(group.shutdown(Duration.ofSeconds(5)));
            if (index != null) {
                index.close();
            }
            try {
                if (failedCloseExpected) {
                    assertThrows(IOException.class, files::close);
                } else {
                    files.close();
                }
            } finally {
                bookie.close();
                arena.close();
            }
        }
    }

    @Test
    public void protectedFreeBatchSharesOneBarrierAndWaitsForEveryClaimedMember() throws Exception {
        SegmentFileIo controlIo = new SegmentFileIo();
        try (MoveFixture fixture = new MoveFixture(config(), controlIo)) {
            long middle = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, middle),
                    reclaimableAllocation(7, middle + SegmentArenaControlLog.BLOCK_BYTES)));
            List<SegmentArenaControlLog.Free> frees = fixture.arena.prepareFrees(List.of(
                    new SegmentArenaControlLog.FreeTarget(new UUID(0, 901), 6),
                    new SegmentArenaControlLog.FreeTarget(new UUID(0, 902), 7)), fixture.files, fixture.bookie,
                    ByteBuffer.allocate(4096));
            fixture.arena.claimProtectedCommands(frees);
            int forces = controlIo.forces;
            SegmentArenaControlLog.DurableBatch batch = fixture.arena.appendDurably(frees);
            assertEquals(forces + 1, controlIo.forces);
            assertTrue(fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
            for (SegmentArenaControlLog.Result result : fixture.arena.applyDurable(batch)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, result.status());
            }
            fixture.arena.finishProtectedCommands(List.of(frees.get(0)));
            fixture.arena.finishProtectedCommands(List.of(frees.get(0)));
            assertTrue(fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).isEmpty());
            fixture.arena.finishProtectedCommands(List.of(frees.get(1)));
            fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow().physicalTerminal();
            for (SegmentArenaControlLog.Free free : frees) {
                free.close();
            }
        }
    }

    private void createCompactionVictims(IsolatedSegmentConfig config, boolean terminalAll) throws Exception {
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, upper)));
            fixture.bookie.submitBatch(List.of(install(48, 10, (byte) 5), activate(48, 11)));
            ByteBuf live = entry(context(), 3);
            ByteBuf dead = entry(context(48), 1);
            CompletableFuture<List<SegmentDataPhysicalDriver.NormalResult>> written = new CompletableFuture<>();
            try {
                assertTrue(fixture.driver.submitNormal(fixture.bookie, SegmentBatchFormat.encode(config,
                        new SegmentBatchFormat.Address(0, 0, 6, new Generation(1), new Generation(1), 2, upper),
                        List.of(SegmentBatchFormat.entry(context(), live),
                                SegmentBatchFormat.entry(context(48), dead))),
                        List.of(fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow(),
                                fixture.bookie.admitNormal(context(48), filled(20, (byte) 5)).orElseThrow()),
                        (results, failure) -> {
                            if (failure == null) {
                                written.complete(results);
                            } else {
                                written.completeExceptionally(failure);
                            }
                        }));
            } finally {
                live.release();
                dead.release();
            }
            for (SegmentDataPhysicalDriver.NormalResult result : written.get(5, TimeUnit.SECONDS)) {
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, result.status());
            }
            ownerBarrier(fixture.owner);
            fixture.bookie.submitBatch(terminalAll ? List.of(tombstone(44, 20), tombstone(48, 21))
                    : List.of(tombstone(48, 21)));
        }
    }

    private static IsolatedSegmentStorage.CompactionResult compactStorage(IsolatedSegmentStorage storage,
            long operationId, List<Long> victims) throws Exception {
        CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
        assertTrue(storage.compactAllocations(new UUID(0, operationId), victims,
                new IsolatedSegmentStorage.CompactionRequest((result, failure) -> {
                    if (failure == null) {
                        response.complete(result);
                    } else {
                        response.completeExceptionally(failure);
                    }
                })));
        return response.get(5, TimeUnit.SECONDS);
    }

    private static IsolatedSegmentStorage.CompactionResult compactShard(IsolatedSegmentStorage storage,
            long operationId, int shard) throws Exception {
        CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
        assertTrue(storage.compactShard(new UUID(0, operationId), shard,
                new IsolatedSegmentStorage.CompactionRequest((result, failure) -> {
                    if (failure == null) {
                        response.complete(result);
                    } else {
                        response.completeExceptionally(failure);
                    }
                })));
        return response.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("net-compaction-" + workers);
            createCompactionVictims(config, false);
            long destination;
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 16, 3)) {
                storage.restart();
                IsolatedSegmentStorage.CompactionResult refused = compactStorage(storage, 911, List.of(5L));
                assertEquals(IsolatedSegmentStorage.CompactionStatus.NO_NET_GAIN, refused.status());
                assertEquals(0, refused.destinationBytes());
                assertEquals(0, refused.freedAllocations());
                IsolatedSegmentStorage.CompactionResult result = compactShard(storage, 912, 0);
                assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
                assertEquals(3, result.movedEntries());
                assertEquals(2, result.freedAllocations());
                assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, result.freedBytes());
                assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.destinationBytes());
                assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
                destination = result.destinationAllocation();
                for (long entryId = 1; entryId <= 3; entryId++) {
                    assertEquals(destination,
                            storageWrite(storage, context(), entryId).candidate().address().allocation());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(912, read.result().record().moveId().getLeastSignificantBits());
                    }
                }
                CompletableFuture<ReadReply> dead = new CompletableFuture<>();
                assertTrue(storage.submitRead(context(48), 1, new SegmentPointRead.ReadRequest(
                        (status, read, failure) -> dead.complete(new ReadReply(status, read, failure)))));
                assertEquals(SegmentPointRead.ReadStatus.NOT_READY, dead.get(5, TimeUnit.SECONDS).status());
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(), arena,
                            bookie, new SegmentFileIo(), 16, 4, index)) {
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(3, files.startupInventory().entries().size());
                List<SegmentArenaControlLog.Free> retired = arena.retiredAllocations().stream()
                        .sorted(java.util.Comparator.comparingLong(free -> free.facts().arenaCut())).toList();
                assertEquals(2, retired.size());
                assertEquals(retired.get(0).facts().arenaCut() + 1, retired.get(1).facts().arenaCut());
                SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                assertEquals(4096, reused.offset());
                assertEquals(2, reused.generation());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        arena.submitBatch(List.of(reused)).get(0).status());
                ByteBuf bytes = entry(context(), 4);
                try {
                    files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                    new Generation(1), 4, reused.offset()),
                            List.of(SegmentBatchFormat.entry(context(), bytes))), arena);
                } finally {
                    bytes.release();
                }
            }
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 16, 3)) {
                storage.restart();
                for (long entryId = 1; entryId <= 3; entryId++) {
                    assertEquals(destination,
                            storageWrite(storage, context(), entryId).candidate().address().allocation());
                }
                assertEquals(2, storageWrite(storage, context(), 4).candidate().address().generation().value());
            }
        }
    }

    @Test
    public void graphCompactionTerminalGroupNeedsNoCopyAllocation() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("terminal-compaction");
        createCompactionVictims(config, true);
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 4, 16, 3)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult result = compactStorage(storage, 913, List.of(5L, 6L));
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
            assertEquals(0, result.movedEntries());
            assertEquals(0, result.destinationBytes());
            assertEquals(-1, result.destinationAllocation());
            assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
        }
    }

    private int createMultiBatchCompactionVictims(IsolatedSegmentConfig config, int perSource) throws Exception {
        return createMultiBatchCompactionVictims(config, perSource, perSource, 8);
    }

    private int createMultiBatchCompactionVictims(IsolatedSegmentConfig config, int perSource,
                                                 int batchEntries, int payloadBytes) throws Exception {
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            long upper = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, upper)));
            long sequence = 2;
            for (int part = 0; part < 2; part++) {
                long offset = part == 0 ? 8192 : upper;
                for (int first = 0; first < perSource; first += batchEntries) {
                    List<ByteBuf> bytes = new ArrayList<>();
                    try {
                        int count = Math.min(batchEntries, perSource - first);
                        for (long entryId = 3L + part * perSource + first;
                                entryId < 3L + part * perSource + first + count; entryId++) {
                            bytes.add(entry(context(), entryId, -1, 8, (byte) 0, payloadBytes));
                        }
                        SegmentBatchFormat.FrozenBatch batch = SegmentBatchFormat.encode(config,
                                new SegmentBatchFormat.Address(0, 0, part == 0 ? 5 : 6, new Generation(1),
                                        new Generation(1), sequence++, offset),
                                bytes.stream().map(value -> SegmentBatchFormat.entry(context(), value)).toList());
                        fixture.files.writeAuthorized(batch, fixture.arena);
                        offset += batch.bytes().remaining();
                    } finally {
                        bytes.forEach(ByteBuf::release);
                    }
                }
            }
        }
        return 2 + 2 * perSource;
    }

    private IsolatedSegmentStorage multiBatchStorage(IsolatedSegmentConfig config, int workers, int bufferBytes,
                                                     long sourceBytes) {
        int buffers = workers * 2;
        return new IsolatedSegmentStorage(config, identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(workers, 3, 6, buffers, bufferBytes, sourceBytes,
                        buffers * (bufferBytes + 4095L) + sourceBytes, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 4096, 4096, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(bufferBytes, 16, Duration.ofSeconds(30))));
    }

    @Test
    public void graphCompactionSplitsByBufferAndMoveRowsAndRecoversEveryBatchAfterSourceReuse() throws Exception {
        for (int workers : new int[] {1, 4}) {
            for (int bufferBytes : new int[] {4096, 65536}) {
                IsolatedSegmentConfig config = coverageRecoveryConfig("multi-copy-" + workers + "-" + bufferBytes);
                int entries = createMultiBatchCompactionVictims(config, bufferBytes == 4096 ? 19 : 200);
                int perCopy = bufferBytes == 4096 ? 19 : SegmentBatchFormat.MAX_RECORDS;
                int copies = (entries + perCopy - 1) / perCopy;
                long destination;
                UUID operation = new UUID(0, 921);
                try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, bufferBytes, 65536)) {
                    storage.restart();
                    IsolatedSegmentStorage.CompactionResult result = compactShard(storage, 921, 0);
                    assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
                    assertEquals(entries, result.movedEntries());
                    assertEquals(2, result.freedAllocations());
                    assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
                    destination = result.destinationAllocation();
                    for (long entryId : new long[] {1, perCopy, perCopy + 1, entries}) {
                        try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                            int batchIndex = (int) ((entryId - 1) / perCopy);
                            UUID copyId = batchIndex == 0 ? operation : UUID.nameUUIDFromBytes(
                                    (operation + "/COPY/" + batchIndex)
                                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            assertEquals(copyId, read.result().record().moveId());
                        }
                        assertEquals(destination,
                                storageWrite(storage, context(), entryId).candidate().address().allocation());
                    }
                }
                try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                        SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                                1024 * 1024, key((byte) 3), VALIDATOR);
                        SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 2048);
                        IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(), arena,
                                bookie, new SegmentFileIo(), 2048, 4, index)) {
                    assertTrue(files.startupInventory().usesIndexCoverage());
                    assertEquals(entries, files.startupInventory().entries().size());
                    assertEquals(2, arena.retiredAllocations().size());
                    SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                            new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                    assertEquals(4096, reused.offset());
                    assertEquals(2, reused.generation());
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            arena.submitBatch(List.of(reused)).get(0).status());
                    ByteBuf bytes = entry(context(), entries + 1);
                    try {
                        files.writeAuthorized(SegmentBatchFormat.encode(config,
                                new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                        new Generation(1), 4 + copies, reused.offset()),
                                List.of(SegmentBatchFormat.entry(context(), bytes))), arena);
                    } finally {
                        bytes.release();
                    }
                }
                try (IsolatedSegmentStorage restarted = multiBatchStorage(config, workers, bufferBytes, 65536)) {
                    restarted.restart();
                    for (long entryId : new long[] {1, perCopy, perCopy + 1, entries}) {
                        assertEquals(destination,
                                storageWrite(restarted, context(), entryId).candidate().address().allocation());
                    }
                    assertEquals(2,
                            storageWrite(restarted, context(), entries + 1).candidate().address().generation().value());
                }
            }
        }
    }

    @Test
    public void laterCompactionCopyForceUnknownRetainsSourcesAndOnlyEarlierMoveSelectors() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("later-compaction-copy-unknown");
        createMultiBatchCompactionVictims(config, 19);
        IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 4096);
        boolean expectedFailedClose = false;
        try {
            storage.restart();
            var field = IsolatedSegmentStorage.class.getDeclaredField("files");
            field.setAccessible(true);
            IsolatedSegmentFiles files = (IsolatedSegmentFiles) field.get(storage);
            SegmentFileIo copyIo = new SegmentFileIo(Integer.MAX_VALUE, -1, 2, false);
            files.dataIo = copyIo;
            CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.compactAllocations(new UUID(0, 922), List.of(5L, 6L),
                    new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                        failure.set(cause);
                        response.complete(result);
                    })));
            IsolatedSegmentStorage.CompactionResult result = response.get(5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            assertEquals(IsolatedSegmentStorage.CompactionStatus.UNKNOWN, result.status());
            expectedFailedClose = true;
            assertEquals(19, result.movedEntries());
            assertEquals(0, result.freedAllocations());
            assertEquals(-SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
            assertEquals(2, copyIo.forces);
        } finally {
            if (expectedFailedClose) {
                assertThrows(IOException.class, storage::close);
            } else {
                storage.close();
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(0, arena.retiredAllocations().size());
            assertEquals(1, arena.allocationGeneration(0, 4096));
            assertEquals(1, arena.allocationGeneration(0, 4096 + SegmentArenaControlLog.BLOCK_BYTES));
            assertEquals(19, arena.currentMoveLocations().size());
            assertFalse(arena.dataFileAvailable(0));
        }
        try (IsolatedSegmentStorage restarted = multiBatchStorage(config, 4, 4096, 4096)) {
            assertThrows(IOException.class, restarted::restart);
        }
    }

    @Test
    public void multiBatchCompactionIncludesEveryFrameInAllocationNetCost() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("multi-copy-net-cost");
        createMultiBatchCompactionVictims(config, 440, 50, 1000);
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 65536, 1024 * 1024)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult result = compactStorage(storage, 923, List.of(5L, 6L));
            assertEquals(IsolatedSegmentStorage.CompactionStatus.NO_NET_GAIN, result.status());
            assertEquals(0, result.destinationBytes());
            assertEquals(0, result.freedAllocations());
            assertEquals(0, result.movedEntries());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(0, arena.currentMoveLocations().size());
            assertEquals(0, arena.retiredAllocations().size());
            assertEquals(0, arena.allocationGeneration(0, 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES));
        }
    }

    @Test
    public void multiBatchCompactionUsesPooledChunksBeyondSourceInputBudget() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("multi-copy-pooled-" + workers);
            int entries = createMultiBatchCompactionVictims(config, 19);
            assertTrue(entries * 44 > 1060);
            long destination;
            try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, 4096, 1060)) {
                storage.restart();
                var field = IsolatedSegmentStorage.class.getDeclaredField("files");
                field.setAccessible(true);
                SegmentFileIo copyIo = new SegmentFileIo();
                ((IsolatedSegmentFiles) field.get(storage)).dataIo = copyIo;
                IsolatedSegmentStorage.CompactionResult result = compactStorage(storage, 924, List.of(5L, 6L));
                assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
                assertEquals(entries, result.movedEntries());
                assertEquals(2, result.freedAllocations());
                assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.destinationBytes());
                assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
                assertEquals(3, copyIo.forces);
                destination = result.destinationAllocation();
                for (long entryId = 1; entryId <= entries; entryId++) {
                    assertEquals(destination,
                            storageWrite(storage, context(), entryId).candidate().address().allocation());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                    }
                }
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 4096);
                    IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(), arena,
                            bookie, new SegmentFileIo(), 4096, 4, index)) {
                assertTrue(files.startupInventory().usesIndexCoverage());
                assertEquals(entries, files.startupInventory().entries().size());
            }
            try (IsolatedSegmentStorage restarted = multiBatchStorage(config, workers, 4096, 1060)) {
                restarted.restart();
                for (long entryId : new long[] {1, 19, 20, entries}) {
                    assertEquals(destination,
                            storageWrite(restarted, context(), entryId).candidate().address().allocation());
                }
            }
        }
    }

    @Test
    public void compactionSingleOwnerBufferRefusalReturnsScratchAndReclaimsUnusedDestination() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("compaction-single-buffer");
        int entries = createMultiBatchCompactionVictims(config, 19);
        try (IsolatedSegmentStorage storage = new IsolatedSegmentStorage(config, identity(), key((byte) 3), VALIDATOR,
                new SegmentExecutionGroup.Settings(1, 3, 6, 1, 4096, 1060,
                        4096 + 4095L + 1060, Duration.ofSeconds(5)),
                new IsolatedSegmentStorage.Limits(1024 * 1024, 4096, 4096, 1, 1,
                        new SegmentDataPhysicalDriver.BatchLimits(4096, 1, Duration.ofSeconds(30))))) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult refused = compactStorage(storage, 940, List.of(5L, 6L));
            assertEquals(IsolatedSegmentStorage.CompactionStatus.PARTIAL, refused.status());
            assertEquals(0, refused.movedEntries());
            assertEquals(0, refused.freedAllocations());
            assertEquals(SegmentArenaControlLog.BLOCK_BYTES, refused.destinationBytes());
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                    storageWrite(storage, context(), entries).status());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(SegmentBatchFormat.ENTRY_DATA, read.result().record().origin());
            }
            IsolatedSegmentStorage.CompactionResult reclaimed = compactShard(storage, 941, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, reclaimed.status());
            assertEquals(1, reclaimed.freedAllocations());
            assertEquals(0, reclaimed.movedEntries());
            assertEquals(0, reclaimed.destinationBytes());
            assertEquals(SegmentArenaControlLog.BLOCK_BYTES, reclaimed.netFreedBytes());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertTrue(arena.dataFileAvailable(0));
            assertEquals(0, arena.currentMoveLocations().size());
            assertEquals(1, arena.retiredAllocations().size());
            assertEquals(1, arena.allocationGeneration(0, 4096));
            assertEquals(1, arena.allocationGeneration(0, 4096 + SegmentArenaControlLog.BLOCK_BYTES));
        }
    }

    @Test
    public void compactionRecapturesMeasuredSourceBytesAndBookieCutBeforeLaterCopy() throws Exception {
        for (boolean changeBytes : new boolean[] {true, false}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("compaction-recapture-" + changeBytes);
            int entries = createMultiBatchCompactionVictims(config, 19);
            CountDownLatch atCopyForce = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean paused = new AtomicBoolean();
            ByteBuffer original = ByteBuffer.allocate(4096);
            try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 1060)) {
                storage.restart();
                var field = IsolatedSegmentStorage.class.getDeclaredField("files");
                field.setAccessible(true);
                SegmentFileIo copyIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                    if (paused.compareAndSet(false, true)) {
                        atCopyForce.countDown();
                        await(release);
                    }
                });
                ((IsolatedSegmentFiles) field.get(storage)).dataIo = copyIo;
                CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                assertTrue(storage.compactAllocations(new UUID(0, 942), List.of(5L, 6L),
                        new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                            failure.set(cause);
                            response.complete(result);
                        })));
                assertTrue(atCopyForce.await(5, TimeUnit.SECONDS));
                if (changeBytes) {
                    List<ByteBuf> modified = new ArrayList<>();
                    try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                        SegmentFileIo.readFully(data, original, 8192);
                        for (long entryId = 3; entryId <= 21; entryId++) {
                            modified.add(entry(context(), entryId, -1, 8, (byte) (entryId == 21 ? 1 : 0), 8));
                        }
                        SegmentBatchFormat.FrozenBatch changed = SegmentBatchFormat.encode(config,
                                new SegmentBatchFormat.Address(0, 0, 5, new Generation(1), new Generation(1), 2, 8192),
                                modified.stream().map(value -> SegmentBatchFormat.entry(context(), value)).toList());
                        new SegmentFileIo().writeFully(data, changed.bytes(), 8192);
                        data.force(false);
                    } finally {
                        modified.forEach(ByteBuf::release);
                    }
                } else {
                    installStorage(storage, install(45, 10, (byte) 5), activate(45, 11));
                }
                release.countDown();
                IsolatedSegmentStorage.CompactionResult result = response.get(5, TimeUnit.SECONDS);
                assertNotNull(failure.get());
                assertTrue(failure.get().getMessage(), failure.get().getMessage().contains(changeBytes
                        ? "source bytes changed" : "Bookie authority changed"));
                assertEquals(IsolatedSegmentStorage.CompactionStatus.PARTIAL, result.status());
                assertEquals(19, result.movedEntries());
                assertEquals(0, result.freedAllocations());
                assertEquals(SegmentArenaControlLog.BLOCK_BYTES, result.destinationBytes());
                assertEquals(1, copyIo.forces);
                if (changeBytes) {
                    try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                            StandardOpenOption.WRITE)) {
                        new SegmentFileIo().writeFully(data, original, 8192);
                        data.force(false);
                    }
                }
                for (long entryId : new long[] {1, 19, 20, entries}) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                            storageWrite(storage, context(), entryId).status());
                    try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                        assertEquals(entryId <= 19 ? SegmentBatchFormat.RELOCATION_COPY : SegmentBatchFormat.ENTRY_DATA,
                                read.result().record().origin());
                    }
                }
            } finally {
                release.countDown();
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertTrue(arena.dataFileAvailable(0));
                assertEquals(19, arena.currentMoveLocations().size());
                assertEquals(0, arena.retiredAllocations().size());
            }
            try (IsolatedSegmentStorage restarted = multiBatchStorage(config, 4, 4096, 1060)) {
                restarted.restart();
                for (long entryId : new long[] {1, 19, 20, entries}) {
                    try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), entryId).result()) {
                        assertEquals(entryId, read.result().record().entryId());
                    }
                }
            }
        }
    }

    private long partiallyMoveCompaction(IsolatedSegmentConfig config, int workers) throws Exception {
        CountDownLatch atCopyForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, 4096, 1060)) {
            storage.restart();
            var field = IsolatedSegmentStorage.class.getDeclaredField("files");
            field.setAccessible(true);
            ((IsolatedSegmentFiles) field.get(storage)).dataIo = new SegmentFileIo(
                    Integer.MAX_VALUE, -1, -1, false, () -> {
                        if (paused.compareAndSet(false, true)) {
                            atCopyForce.countDown();
                            await(release);
                        }
                    });
            CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.compactAllocations(new UUID(0, 944), List.of(5L, 6L),
                    new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                        failure.set(cause);
                        response.complete(result);
                    })));
            assertTrue(atCopyForce.await(5, TimeUnit.SECONDS));
            installStorage(storage, install(45, 10, (byte) 5), activate(45, 11));
            release.countDown();
            IsolatedSegmentStorage.CompactionResult partial = response.get(5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            assertEquals(IsolatedSegmentStorage.CompactionStatus.PARTIAL, partial.status());
            assertEquals(19, partial.movedEntries());
            assertEquals(0, partial.freedAllocations());
            return partial.destinationAllocation();
        } finally {
            release.countDown();
        }
    }

    @Test
    public void shardCompactionResumesPartialMoveAtFullCapacityAfterHealthyRestart() throws Exception {
        for (int workers : new int[] {1, 4}) {
            for (boolean reusedTail : new boolean[] {false, true}) {
                IsolatedSegmentConfig config = coverageRecoveryConfig(
                        "partial-compaction-resume-" + workers + "-" + reusedTail);
                int entries = createMultiBatchCompactionVictims(config, 19);
                if (reusedTail) {
                    long upper = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES;
                    try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                            SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                                    1024 * 1024, key((byte) 3), VALIDATOR);
                            IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                                    config, identity(), arena,
                                    bookie, new SegmentFileIo(), 4096, 4)) {
                        bookie.submitBatch(List.of(install(48, 20, (byte) 5), activate(48, 21)));
                        arena.submitBatch(List.of(reclaimableAllocation(7, upper)));
                        for (int batch = 0; batch < 2; batch++) {
                            ByteBuf bytes = entry(context(48), batch + 1);
                            try {
                                files.writeAuthorized(SegmentBatchFormat.encode(config,
                                        new SegmentBatchFormat.Address(0, 0, 7, new Generation(1), new Generation(1),
                                                4L + batch, upper + batch * 4096L),
                                        List.of(SegmentBatchFormat.entry(context(48), bytes))), arena);
                            } finally {
                                bytes.release();
                            }
                        }
                        bookie.submitBatch(List.of(tombstone(48, 22)));
                    }
                    try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, 4096, 1060)) {
                        storage.restart();
                        assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED,
                                compactStorage(storage, 947, List.of(7L)).status());
                    }
                }
                long destination = partiallyMoveCompaction(config, workers);
                try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                    assertTrue(arena.planPools(List.of(new SegmentArenaControlLog.PoolRequest(
                            0, new Generation(1), 1, 1))).get(0).isEmpty());
                }
                try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, 4096, 1060)) {
                    storage.restart();
                    var field = IsolatedSegmentStorage.class.getDeclaredField("files");
                    field.setAccessible(true);
                    SegmentFileIo copyIo = new SegmentFileIo();
                    ((IsolatedSegmentFiles) field.get(storage)).dataIo = copyIo;
                    IsolatedSegmentStorage.CompactionResult resumed = compactShard(storage, 945, 0);
                    assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, resumed.status());
                    assertEquals(21, resumed.movedEntries());
                    assertEquals(2, resumed.freedAllocations());
                    assertEquals(0, resumed.destinationBytes());
                    assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, resumed.netFreedBytes());
                    assertEquals(destination, resumed.destinationAllocation());
                    assertEquals(2, copyIo.forces);
                    for (long entryId = 1; entryId <= entries; entryId++) {
                        SegmentBatchFormat.Address address = storageWrite(storage, context(), entryId)
                                .candidate().address();
                        assertEquals(destination, address.allocation());
                        assertEquals(reusedTail ? 2 : 1, address.generation().value());
                        try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                            assertEquals(SegmentBatchFormat.RELOCATION_COPY, read.result().record().origin());
                        }
                    }
                }
                try (IsolatedSegmentStorage storage = storage(config, workers, 6, 4096, 4096, 3)) {
                    storage.restart();
                    SegmentDataPhysicalDriver.NormalResult fresh = storageWrite(storage, context(), entries + 1);
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE, fresh.status());
                    assertEquals(2, fresh.candidate().address().generation().value());
                    assertEquals(4096, fresh.candidate().address().offset());
                }
                try (IsolatedSegmentStorage storage = multiBatchStorage(config, workers, 4096, 1060)) {
                    storage.restart();
                    for (long entryId : new long[] {1, 19, 20, entries}) {
                        assertEquals(destination,
                                storageWrite(storage, context(), entryId).candidate().address().allocation());
                    }
                    assertEquals(2,
                            storageWrite(storage, context(), entries + 1).candidate().address().generation().value());
                }
            }
        }
    }

    @Test
    public void retainedCompactionTailRejectsInsufficientRangeAndCorruptExistingBytes() throws Exception {
        for (boolean corrupt : new boolean[] {false, true}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("partial-compaction-tail-refusal-" + corrupt);
            int entries = createMultiBatchCompactionVictims(config, 19);
            long destination = partiallyMoveCompaction(config, 4);
            // Fill the retained tail with terminal records, preserving its first current COPY batch.
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR)) {
                bookie.submitBatch(List.of(install(48, 20, (byte) 5), activate(48, 21), tombstone(48, 22)));
                SegmentArenaControlLog.Allocation retained = arena.currentAllocation(destination);
                try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                        StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    for (int batch = 0; batch < 254; batch++) {
                        List<ByteBuf> bytes = new ArrayList<>();
                        try {
                            for (int record = 0; record < 3; record++) {
                                bytes.add(entry(context(48), 3L * batch + record, -1, 8, (byte) 0, 1000));
                            }
                            SegmentBatchFormat.FrozenBatch filler = SegmentBatchFormat.encode(config,
                                    new SegmentBatchFormat.Address(0, 0, destination, new Generation(1),
                                            new Generation(1), 5L + batch, retained.offset() + (1L + batch) * 4096),
                                    bytes.stream().map(value -> SegmentBatchFormat.entry(context(48), value)).toList());
                            new SegmentFileIo().writeFully(data, filler.bytes(), filler.address().offset());
                        } finally {
                            bytes.forEach(ByteBuf::release);
                        }
                    }
                    data.force(false);
                }
            }
            try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 1060)) {
                storage.restart();
                var field = IsolatedSegmentStorage.class.getDeclaredField("files");
                field.setAccessible(true);
                SegmentFileIo copyIo = new SegmentFileIo();
                ((IsolatedSegmentFiles) field.get(storage)).dataIo = copyIo;
                ByteBuffer saved = ByteBuffer.allocate(4096);
                long lastFiller = 4096 + 2 * SegmentArenaControlLog.BLOCK_BYTES + 254L * 4096;
                if (corrupt) {
                    try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                            StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                        SegmentFileIo.readFully(data, saved, lastFiller);
                        byte[] bad = saved.array().clone();
                        bad[328] ^= 1;
                        new SegmentFileIo().writeFully(data, ByteBuffer.wrap(bad), lastFiller);
                        data.force(false);
                    }
                }
                CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                assertTrue(storage.compactShard(new UUID(0, 946), 0,
                        new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                            failure.set(cause);
                            response.complete(result);
                        })));
                IsolatedSegmentStorage.CompactionResult refused = response.get(5, TimeUnit.SECONDS);
                assertEquals(IsolatedSegmentStorage.CompactionStatus.TRANSIENT_UNAVAILABLE, refused.status());
                assertEquals(0, refused.destinationBytes());
                assertEquals(0, refused.movedEntries());
                assertEquals(0, refused.freedAllocations());
                assertEquals(0, copyIo.forces);
                assertEquals(corrupt, failure.get() != null);
                if (corrupt) {
                    try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                            StandardOpenOption.WRITE)) {
                        new SegmentFileIo().writeFully(data, saved, lastFiller);
                        data.force(false);
                    }
                }
                for (long entryId : new long[] {1, 19, 20, entries}) {
                    assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                            storageWrite(storage, context(), entryId).status());
                }
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                assertTrue(arena.dataFileAvailable(0));
                assertEquals(19, arena.currentMoveLocations().size());
                assertEquals(0, arena.retiredAllocations().size());
            }
        }
    }

    @Test
    public void shardCompactionReclaimsAtFullCapacityBeforeCapturingUnrelatedLiveInputs() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("shard-compaction-full-capacity");
        int entries = createMultiBatchCompactionVictims(config, 19);
        long destination;
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 4096)) {
            storage.restart();
            CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.compactAllocations(new UUID(0, 925), List.of(5L, 6L),
                    new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                        failure.set(cause);
                        response.complete(result);
                    }), new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            IsolatedSegmentStorage.CompactionResult partial = response.get(5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            assertEquals(IsolatedSegmentStorage.CompactionStatus.UNKNOWN, partial.status());
            assertEquals(entries, partial.movedEntries());
            assertEquals(0, partial.freedAllocations());
            destination = partial.destinationAllocation();
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertTrue(arena.planPools(List.of(new SegmentArenaControlLog.PoolRequest(0,
                    new Generation(1), 1, 1))).get(0).isEmpty());
        }
        // The current destination owns 1760 source bytes, beyond this legal 1060-byte budget.
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 1, 4096, 1060)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult result = compactShard(storage, 926, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
            assertEquals(2, result.freedAllocations());
            assertEquals(0, result.movedEntries());
            assertEquals(0, result.destinationBytes());
            assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
            assertEquals(destination, storageWrite(storage, context(), entries).candidate().address().allocation());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(new UUID(0, 925), read.result().record().moveId());
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                        1024 * 1024, key((byte) 3), VALIDATOR);
                SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 2048);
                IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(config, identity(), arena,
                        bookie, new SegmentFileIo(), 2048, 4, index)) {
            SegmentArenaControlLog.Allocation reused = arena.planPools(List.of(
                    new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
            assertEquals(4096, reused.offset());
            assertEquals(2, reused.generation());
            assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                    arena.submitBatch(List.of(reused)).get(0).status());
            ByteBuf bytes = entry(context(), entries + 1);
            try {
                files.writeAuthorized(SegmentBatchFormat.encode(config,
                        new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                new Generation(1), 7, reused.offset()),
                        List.of(SegmentBatchFormat.entry(context(), bytes))), arena);
            } finally {
                bytes.release();
            }
        }
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 1060)) {
            storage.restart();
            assertEquals(destination, storageWrite(storage, context(), entries).candidate().address().allocation());
            assertEquals(2, storageWrite(storage, context(), entries + 1).candidate().address().generation().value());
        }
    }

    @Test
    public void shardCompactionBoundsOldestWindowAndRetainsYoungerSharedLiveData() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("shard-compaction-window"), 1, 4, 1,
                18L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            fixture.bookie.submitBatch(List.of(install(48, 10, (byte) 5), activate(48, 11)));
            for (int i = 1; i <= 16; i++) {
                long offset = 4096 + i * SegmentArenaControlLog.BLOCK_BYTES;
                long allocation = 5L + i;
                fixture.arena.submitBatch(List.of(reclaimableAllocation(allocation, offset)));
                ByteBuf bytes = entry(i == 16 ? context(48) : context(), i + 2);
                try {
                    fixture.files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, allocation, new Generation(1),
                                    new Generation(1), i + 1, offset),
                            List.of(SegmentBatchFormat.entry(i == 16 ? context(48) : context(), bytes))),
                            fixture.arena);
                } finally {
                    bytes.release();
                }
            }
            fixture.bookie.submitBatch(List.of(tombstone(44, 20)));
        }
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 16, 128, 3)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult result = compactShard(storage, 927, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, result.status());
            assertEquals(16, result.freedAllocations());
            assertEquals(16 * SegmentArenaControlLog.BLOCK_BYTES, result.netFreedBytes());
            assertEquals(0, result.destinationBytes());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 18).result()) {
                assertEquals(SegmentBatchFormat.ENTRY_DATA, read.result().record().origin());
            }
            assertEquals(IsolatedSegmentStorage.CompactionStatus.NO_NET_GAIN, compactShard(storage, 928, 0).status());
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 16, 128, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(48), 18).result()) {
                assertEquals(SegmentBatchFormat.ENTRY_DATA, read.result().record().origin());
            }
        }
    }

    private void createFullCompactionWindow(IsolatedSegmentConfig config) throws Exception {
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            fixture.bookie.submitBatch(List.of(install(48, 10, (byte) 5), activate(48, 11)));
            for (int i = 1; i <= 16; i++) {
                long offset = 4096 + i * SegmentArenaControlLog.BLOCK_BYTES;
                long allocation = 5L + i;
                fixture.arena.submitBatch(List.of(reclaimableAllocation(allocation, offset)));
                ByteBuf bytes = entry(i == 16 ? context(48) : context(), i + 2);
                try {
                    fixture.files.writeAuthorized(SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, allocation, new Generation(1),
                                    new Generation(1), i + 1, offset),
                            List.of(SegmentBatchFormat.entry(i == 16 ? context(48) : context(), bytes))),
                            fixture.arena);
                } finally {
                    bytes.release();
                }
            }
            fixture.bookie.submitBatch(List.of(tombstone(48, 20)));
        }
    }

    @Test
    public void shardCompactionAdvancesPastFullLiveWindowToRecoverDestinationSupply() throws Exception {
        IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                temporary.getRoot().toPath().resolve("shard-compaction-full-window"), 1, 4, 1,
                18L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        createFullCompactionWindow(config);
        long destination;
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 4, 4096, 1060)) {
            storage.restart();
            IsolatedSegmentStorage.CompactionResult refused = compactShard(storage, 940, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.TRANSIENT_UNAVAILABLE, refused.status());
            assertEquals(0, refused.destinationBytes());
            assertEquals(0, refused.freedAllocations());
            IsolatedSegmentStorage.CompactionResult direct = compactShard(storage, 941, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, direct.status());
            assertEquals(1, direct.freedAllocations());
            assertEquals(0, direct.destinationBytes());
            IsolatedSegmentStorage.CompactionResult moved = compactShard(storage, 942, 0);
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, moved.status());
            assertEquals(17, moved.movedEntries());
            assertEquals(16, moved.freedAllocations());
            assertEquals(15 * SegmentArenaControlLog.BLOCK_BYTES, moved.netFreedBytes());
            destination = moved.destinationAllocation();
            assertEquals(2, storageWrite(storage, context(), 17).candidate().address().generation().value());
        }
        try (IsolatedSegmentStorage storage = multiBatchStorage(config, 1, 4096, 1060)) {
            storage.restart();
            assertEquals(destination, storageWrite(storage, context(), 1).candidate().address().allocation());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 17).result()) {
                assertEquals(new UUID(0, 942), read.result().record().moveId());
            }
        }
    }

    @Test
    public void shardCompactionFreezesScanRoundAndRevisitsEarlierSourcesAfterRefill() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = new IsolatedSegmentConfig(
                    temporary.getRoot().toPath().resolve("shard-compaction-refill-" + workers), 1, 4, 1,
                    19L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
            createFullCompactionWindow(config);
            // Keep the original no-destination first window; the later window must restore both
            // the foreground refill and its maintenance reserve before admitting a new pool.
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
                arena.submitBatch(List.of(reclaimableAllocation(22, 4096 + 17 * SegmentArenaControlLog.BLOCK_BYTES)));
            }
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 16, 128, 3)) {
                storage.restart();
                assertEquals(IsolatedSegmentStorage.CompactionStatus.TRANSIENT_UNAVAILABLE,
                        compactShard(storage, 943, 0).status());
                assertEquals(2, compactShard(storage, 944, 0).freedAllocations());
                // Real refill is newer than the active round's captured allocation cut.
                SegmentBatchFormat.Address appended = storageWrite(storage, context(), 18).candidate().address();
                assertEquals(2, appended.generation().value());
                assertEquals(4096 + 16 * SegmentArenaControlLog.BLOCK_BYTES, appended.offset());
                installStorage(storage, tombstone(44, 21));
                IsolatedSegmentStorage.CompactionResult revisited = compactShard(storage, 945, 0);
                assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, revisited.status());
                assertEquals(16, revisited.freedAllocations());
                assertEquals(0, revisited.destinationBytes());
                IsolatedSegmentStorage.CompactionResult later = compactShard(storage, 946, 0);
                assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, later.status());
                assertEquals(1, later.freedAllocations());
                assertFalse(storage.compactShard(new UUID(0, 947), 0,
                        new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                            throw new AssertionError("No current source should admit a compaction request");
                        })));
                installStorage(storage, install(52, 30, (byte) 5), activate(52, 31));
                SegmentBatchFormat.Address reused = storageWrite(storage, context(52), 1).candidate().address();
                assertEquals(4096, reused.offset());
                assertEquals(2, reused.generation().value());
            }
            try (IsolatedSegmentStorage storage = storage(config, workers, 6, 16, 128, 3)) {
                storage.restart();
                assertEquals(2, storageWrite(storage, context(52), 1).candidate().address().generation().value());
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(52), 1).result()) {
                    assertEquals(1, read.result().record().entryId());
                }
            }
        }
    }

    @Test
    public void graphCompactionPublicationFailureKeepsMovedDataAndSourcesForLaterFree() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("compaction-publication-failure");
        createCompactionVictims(config, false);
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 4, 16, 3)) {
            storage.restart();
            CompletableFuture<IsolatedSegmentStorage.CompactionResult> response = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.compactAllocations(new UUID(0, 914), List.of(5L, 6L),
                    new IsolatedSegmentStorage.CompactionRequest((result, cause) -> {
                        failure.set(cause);
                        response.complete(result);
                    }), new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            IsolatedSegmentStorage.CompactionResult partial = response.get(5, TimeUnit.SECONDS);
            assertNotNull(failure.get());
            assertEquals(IsolatedSegmentStorage.CompactionStatus.UNKNOWN, partial.status());
            assertEquals(3, partial.movedEntries());
            assertEquals(0, partial.freedAllocations());
            assertEquals(-SegmentArenaControlLog.BLOCK_BYTES, partial.netFreedBytes());
            for (long entryId = 1; entryId <= 3; entryId++) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), entryId).result()) {
                    assertEquals(914, read.result().record().moveId().getLeastSignificantBits());
                }
            }
            IsolatedSegmentStorage.CompactionResult reclaimed = compactStorage(storage, 915, List.of(5L, 6L));
            assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, reclaimed.status());
            assertEquals(0, reclaimed.movedEntries());
            assertEquals(0, reclaimed.destinationBytes());
            assertEquals(2 * SegmentArenaControlLog.BLOCK_BYTES, reclaimed.netFreedBytes());
        }
        try (IsolatedSegmentStorage restarted = storage(config, 4, 6, 4, 16, 3)) {
            restarted.restart();
            for (long entryId = 1; entryId <= 3; entryId++) {
                try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), entryId).result()) {
                    assertEquals(914, read.result().record().moveId().getLeastSignificantBits());
                }
            }
        }
    }

    @Test
    public void cancelledGraphCompactionHoldsResourcesUntilPublicationAndCanReclaimAfterRestart() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("cancelled-compaction-" + workers);
            createCompactionVictims(config, false);
            CountDownLatch forced = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger replies = new AtomicInteger();
            IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 16, 3);
            try {
                storage.restart();
                IsolatedSegmentStorage.CompactionRequest request =
                        new IsolatedSegmentStorage.CompactionRequest((result, cause) -> replies.incrementAndGet());
                assertTrue(storage.compactAllocations(new UUID(0, 916), List.of(5L, 6L), request,
                        new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                            forced.countDown();
                            await(release);
                        })));
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                ByteBuf bytes = entry(context(), 4);
                try {
                    assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                            new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> { })));
                } finally {
                    bytes.release();
                }
                assertFalse(storage.compactAllocations(new UUID(0, 917), List.of(5L, 6L),
                        new IsolatedSegmentStorage.CompactionRequest((result, cause) -> { })));
                assertTrue(request.stopWaiting());
                assertFalse(storage.close(Duration.ofMillis(50)));
                assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, identity(), 16));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                assertEquals(0, replies.get());
            } finally {
                release.countDown();
                storage.close();
            }
            try (IsolatedSegmentStorage restarted = storage(config, workers, 6, 4, 16, 3)) {
                restarted.restart();
                for (long entryId = 1; entryId <= 3; entryId++) {
                    try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), entryId).result()) {
                        assertEquals(916, read.result().record().moveId().getLeastSignificantBits());
                    }
                }
                IsolatedSegmentStorage.CompactionResult reclaimed = compactStorage(restarted, 917, List.of(5L, 6L));
                assertEquals(IsolatedSegmentStorage.CompactionStatus.RECLAIMED, reclaimed.status());
                assertEquals(0, reclaimed.destinationBytes());
                assertEquals(2, reclaimed.freedAllocations());
            }
        }
    }

    @Test
    public void preparedResolvedMoveRetryReleasesItsUnclaimedProtection() throws Exception {
        try (MoveFixture fixture = new MoveFixture(config(), new SegmentFileIo())) {
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(918, 2, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
            try (SegmentArenaMove retry = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        fixture.arena.submitBatch(List.of(retry)).get(0).status());
                fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow().physicalTerminal();
                try (IsolatedSegmentFiles.DataReadLease ignored = fixture.files.pinDataRead()) {
                    fixture.bookie.admitRead(context()).orElseThrow().finish();
                }
            }
        }
    }

    @Test
    public void nativeMoveMaintenanceReplacesDrainedRowsAndCoveredRestartVerifiesCurrentBytes() throws Exception {
        for (boolean zeroTarget : new boolean[] {false, true}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("native-move-coverage-" + zeroTarget);
            SegmentDerivedIndex.Coverage movedCoverage;
            List<SegmentPointRead.Location> targets;
            try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo(), true)) {
                SegmentDerivedIndex.Coverage old = fixture.checkpointMoves(false);
                SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(801, 2, (byte) 0);
                try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            fixture.arena.submitBatch(List.of(move)).get(0).status());
                }
                targets = copy.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList();
                assertEquals(fixture.sources.get(0), fixture.index.get(context(), 1).orElseThrow());
                assertTrue(fixture.index.recoveryCoverage(fixture.arena, fixture.bookie).isEmpty());
                movedCoverage = fixture.checkpointMoves(true);
                assertEquals(old.indexGeneration(), movedCoverage.indexGeneration());
                assertEquals(old.publication() + 1, movedCoverage.publication());
                assertEquals(2, movedCoverage.streams().get(0).through());
                assertEquals(targets.get(0), fixture.index.get(context(), 1).orElseThrow());
                assertEquals(targets.get(1), fixture.index.get(context(), 2).orElseThrow());
                assertTrue(fixture.index.recoveryCoverage(fixture.arena, fixture.bookie).isPresent());
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                        checkPublishedDuplicate(fixture.driver, fixture.bookie,
                                entryBytes(context(), 1, -1, 8, (byte) 0)));
            }
            try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024);
                    SegmentBookieControlLog bookie = SegmentBookieControlLog.open(config, identity(),
                            1024 * 1024, key((byte) 3), VALIDATOR);
                    SegmentDerivedIndex index = SegmentDerivedIndex.open(config, identity(), 16)) {
                try (IsolatedSegmentFiles files = IsolatedSegmentFiles.openForLocalResults(
                        config, identity(), arena, bookie, new SegmentFileIo(), 16, 4, index)) {
                    assertTrue(files.startupInventory().usesIndexCoverage());
                    assertEquals(2, files.startupInventory().scan().coveredBatches());
                    assertEquals(2, files.startupInventory().entries().size());
                    assertEquals(movedCoverage.indexGeneration(), index.coverage().orElseThrow().indexGeneration());
                    assertEquals(targets.get(0), index.get(context(), 1).orElseThrow());
                }
            }
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(data, bytes, 8192);
                if (zeroTarget) {
                    bytes = ByteBuffer.allocate(4096);
                } else {
                    bytes.put(328, (byte) (bytes.get(328) ^ 1));
                }
                new SegmentFileIo().writeFully(data, bytes, 8192);
                data.force(true);
            }
            try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
                assertThrows(IOException.class, storage::restart);
            }
        }
    }

    @Test
    public void failedNativeMoveValidationKeepsSelectorAndRequiresFullRecovery() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("failed-native-move-validation");
        SegmentPointRead.Location target;
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo(), true)) {
            fixture.checkpointMoves(false);
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(804, 2, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
            target = copy.copies().get(0).location();
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ByteBuffer original = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(data, original, 8192);
                byte[] bad = original.array().clone();
                bad[328] ^= 1;
                new SegmentFileIo().writeFully(data, ByteBuffer.wrap(bad), 8192);
                assertThrows(IOException.class, () -> fixture.checkpointMoves(true));
                assertThrows(IOException.class, () -> fixture.index.get(context(), 1));
                assertEquals(target, fixture.arena.currentMoveLocation(context(), 1).orElseThrow());
                fixture.bookie.admitRead(context()).orElseThrow().finish();
                new SegmentFileIo().writeFully(data, original, 8192);
                data.force(true);
            }
            fixture.index.close();
            try (SegmentDerivedIndex reopened = SegmentDerivedIndex.open(config, identity(), 16)) {
                assertEquals(fixture.sources.get(0), reopened.get(context(), 1).orElseThrow());
                assertTrue(reopened.recoveryCoverage(fixture.arena, fixture.bookie).isEmpty());
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
            storage.restart();
            assertEquals(target, storageWrite(storage, context(), 1).candidate());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(804, read.result().record().moveId().getLeastSignificantBits());
            }
        }
    }

    @Test
    public void graphMoveIndexPublicationFailureRetainsCurrentReadsAndCanRepublish() throws Exception {
        IsolatedSegmentConfig config = coverageRecoveryConfig("graph-move-publication-failure");
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(802, 2, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
        }
        try (IsolatedSegmentStorage storage = storage(config, 4, 6, 4, 16, 3)) {
            storage.restart();
            CompletableFuture<IsolatedSegmentStorage.IndexCheckpointResult> failed = new CompletableFuture<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            assertTrue(storage.checkpointIndex(new IsolatedSegmentStorage.IndexCheckpointRequest((result, cause) -> {
                failure.set(cause);
                failed.complete(result);
            }), new SegmentFileIo(Integer.MAX_VALUE, -1, 1, false)));
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.UNKNOWN,
                    failed.get(5, TimeUnit.SECONDS).status());
            assertNotNull(failure.get());
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(802, read.result().record().moveId().getLeastSignificantBits());
            }
            assertEquals(IsolatedSegmentStorage.IndexCheckpointStatus.PERSISTED,
                    checkpointAfterBackpressure(storage, new SegmentFileIo()).status());
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                    storageWrite(storage, context(), 1).status());
        }
    }

    @Test
    public void cancelledGraphMoveCheckpointHoldsProtectionUntilActualPublicationAndOwnerDrain() throws Exception {
        for (int workers : new int[] {1, 4}) {
            IsolatedSegmentConfig config = coverageRecoveryConfig("cancelled-move-index-" + workers);
            try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
                SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(803, 2, (byte) 0);
                try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            fixture.arena.submitBatch(List.of(move)).get(0).status());
                }
            }
            CountDownLatch forced = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger replies = new AtomicInteger();
            IsolatedSegmentStorage storage = storage(config, workers, 6, 4, 16, 3);
            try {
                storage.restart();
                IsolatedSegmentStorage.IndexCheckpointRequest request =
                        new IsolatedSegmentStorage.IndexCheckpointRequest((result, cause) -> replies.incrementAndGet());
                assertTrue(storage.checkpointIndex(request,
                        new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
                            forced.countDown();
                            await(release);
                        })));
                assertTrue(forced.await(5, TimeUnit.SECONDS));
                ByteBuf bytes = entry(context(), 3);
                try {
                    assertFalse(storage.submitNormal(context(), filled(20, (byte) 5), bytes,
                            new SegmentDataPhysicalDriver.NormalRequest((result, cause) -> { })));
                } finally {
                    bytes.release();
                }
                assertTrue(request.stopWaiting());
                assertFalse(storage.close(Duration.ofMillis(50)));
                assertThrows(IOException.class, () -> SegmentDerivedIndex.open(config, identity(), 16));
                release.countDown();
                assertTrue(storage.close(Duration.ofSeconds(5)));
                assertEquals(0, replies.get());
            } finally {
                release.countDown();
                storage.close();
            }
            try (IsolatedSegmentStorage restarted = storage(config, workers, 6, 4, 16, 3)) {
                restarted.restart();
                try (SegmentPointRead.LeasedResult read = storageRead(restarted, context(), 1).result()) {
                    assertEquals(803, read.result().record().moveId().getLeastSignificantBits());
                }
            }
        }
    }

    @Test
    public void moveWaitsForApplyAndRedirectsOldHotReadsAndDuplicateChecks() throws Exception {
        try (MoveFixture fixture = new MoveFixture(config(), new SegmentFileIo())) {
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(601, 2, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                long sequence = fixture.arena.appliedThrough() + 1;
                ByteBuffer encoded = SegmentControlFormat.encode(move.type(), new Generation(1), sequence,
                        move.operationId(), move.operationGeneration(), move.expectedGeneration(), move.body());
                SegmentArenaMove decoded = (SegmentArenaMove) fixture.arena.decodeCommand(encoded, sequence);
                assertEquals(move, decoded);
                assertThrows(IOException.class, () -> fixture.arena.submitBatch(List.of(decoded)));
                fixture.arena.claimProtectedCommands(List.of(move));
                SegmentArenaControlLog.DurableBatch durable = fixture.arena.appendDurably(List.of(move));
                try {
                    assertEquals(fixture.sources.get(0), fixture.arena.resolveLocation(fixture.sources.get(0)));
                    assertEquals(-1, fixture.arena.currentSelectorSequence(context(), 1));
                    assertFalse(fixture.bookie.admitRead(context()).isPresent());
                    assertThrows(IOException.class, fixture.files::pinDataRead);
                    assertThrows(IllegalStateException.class, move::close);
                    SegmentArenaControlLog.Result result = fixture.arena.applyDurable(durable).get(0);
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, result.status());
                    assertEquals(7, result.generation());
                    assertEquals(sequence, fixture.arena.currentSelectorSequence(context(), 1));
                } finally {
                    fixture.arena.finishProtectedCommands(List.of(move));
                }
                assertEquals(SegmentArenaControlLog.Status.ALREADY_DURABLE,
                        fixture.arena.submitBatch(List.of(decoded)).get(0).status());
                for (int i = 0; i < 2; i++) {
                    SegmentPointRead.Location target = copy.copies().get(i).location();
                    assertEquals(target, fixture.arena.resolveLocation(fixture.sources.get(i)));
                    assertEquals(copy.operationId(), SegmentPointRead.readLocallyGated(fixture.config, identity(),
                            fixture.arena, fixture.bookie, fixture.sources.get(i)).record().moveId());
                }
                assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                        checkPublishedDuplicate(fixture.driver, fixture.bookie,
                                entryBytes(context(), 1, -1, 8, (byte) 0)));
                ReadReply read = pointRead(pointReader(fixture.group, fixture.files, fixture.arena,
                        fixture.bookie, fixture.hot), context(), 2);
                assertEquals(SegmentPointRead.ReadStatus.FOUND, read.status());
                assertNull(read.failure());
                try (SegmentPointRead.LeasedResult result = read.result()) {
                    assertEquals(copy.operationId(), result.result().record().moveId());
                }
            }
        }
    }

    @Test
    public void moveChainRejectsStalePredecessorAndReplaysSelectorsThroughRotation() throws Exception {
        IsolatedSegmentConfig config = config();
        List<SegmentPointRead.Location> original;
        List<SegmentPointRead.Location> targets;
        List<SegmentPointRead.Location> superseded;
        List<SegmentPointRead.Location> rejected;
        SegmentControlCheckpoint.Reference previous = null;
        SegmentControlCheckpoint.Reference latest = null;
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            original = fixture.sources;
            SegmentDataPhysicalDriver.DurableRelocation first = fixture.copy(601, 2, (byte) 0);
            SegmentArenaMove prior;
            long selector;
            try (SegmentArenaMove move = fixture.prepare(first, original, -1)) {
                SegmentArenaControlLog.Result result = fixture.arena.submitBatch(List.of(move)).get(0);
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, result.status());
                selector = result.sequence();
                prior = move.replayFacts();
            }
            SegmentDataPhysicalDriver.DurableRelocation losing = fixture.copy(602, 3, (byte) 0);
            try (SegmentArenaMove move = SegmentArenaMove.prepare(fixture.arena, fixture.files, fixture.bookie,
                    losing, List.of(new SegmentArenaMove.Source(original.get(0), -1),
                            new SegmentArenaMove.Source(first.copies().get(1).location(), selector)),
                    ByteBuffer.allocate(4096))) {
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
                for (int i = 0; i < 2; i++) {
                    assertEquals(first.copies().get(i).location(), fixture.arena.resolveLocation(original.get(i)));
                }
            }
            SegmentDataPhysicalDriver.DurableRelocation next = fixture.copy(603, 4, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(next,
                    first.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList(),
                    selector)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
            assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                    fixture.arena.submitBatch(List.of(prior)).get(0).status());
            targets = next.copies().stream()
                    .map(SegmentDataPhysicalDriver.RelocationCopy::location).toList();
            superseded = first.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList();
            rejected = losing.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList();
            for (int cycle = 0; cycle < 8; cycle++) {
                previous = latest;
                latest = fixture.arena.checkpoint();
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(latest, arena.publishedCheckpoint().orElseThrow());
            for (int i = 0; i < 2; i++) {
                assertEquals(targets.get(i), arena.resolveLocation(original.get(i)));
                arena.validateSelectedRecord(targets.get(i), SegmentPointRead.read(config, identity(),
                        arena, targets.get(i)).record());
                assertTrue(arena.wasCommittedCopy(superseded.get(i), SegmentPointRead.read(config, identity(),
                        arena, superseded.get(i)).record()));
                assertFalse(arena.wasCommittedCopy(rejected.get(i), SegmentPointRead.read(config, identity(),
                        arena, rejected.get(i)).record()));
            }
            SegmentDataRecovery.PhysicalInventory inventory = SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), arena, new SegmentFileIo(), 2);
            assertEquals(targets, inventory.entries());
        }
        Path image = SegmentControlCheckpoint.path(config, SegmentControlFormat.Owner.ARENA, latest.slot());
        byte[] damaged = Files.readAllBytes(image);
        damaged[4224] ^= 1;
        Files.write(image, damaged);
        try (FileChannel channel = FileChannel.open(image, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(previous, arena.publishedCheckpoint().orElseThrow());
            for (int i = 0; i < 2; i++) {
                assertTrue(arena.wasCommittedCopy(superseded.get(i), SegmentPointRead.read(config, identity(),
                        arena, superseded.get(i)).record()));
                assertFalse(arena.wasCommittedCopy(rejected.get(i), SegmentPointRead.read(config, identity(),
                        arena, rejected.get(i)).record()));
                assertTrue(arena.wasCommittedCopy(targets.get(i), SegmentPointRead.read(config, identity(),
                        arena, targets.get(i)).record()));
            }
            assertEquals(targets, SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), arena, new SegmentFileIo(), 2).entries());
            arena.rotate();
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(targets, SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), arena, new SegmentFileIo(), 2).entries());
        }
        try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0), StandardOpenOption.WRITE)) {
            new SegmentFileIo().writeFully(data, ByteBuffer.allocate(4096), targets.get(0).address().offset());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(targets.get(0), arena.resolveLocation(original.get(0)));
            assertThrows(IOException.class, () -> SegmentDataRecovery.recoverPhysicalInventory(
                    config, identity(), arena, new SegmentFileIo(), 2));
        }
    }

    @Test
    public void moveProofRejectsHeldReadsWrongBytesAndFutureSelectorsWithoutLeakingProtection() throws Exception {
        try (MoveFixture fixture = new MoveFixture(config(), new SegmentFileIo())) {
            SegmentDataPhysicalDriver.DurableRelocation wrong = fixture.copy(601, 2, (byte) 9);
            assertThrows(IOException.class, () -> fixture.prepare(wrong, fixture.sources, -1));
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(602, 3, (byte) 0);
            try (IsolatedSegmentFiles.DataReadLease ignored = fixture.files.pinDataRead()) {
                assertThrows(IOException.class, () -> fixture.prepare(copy, fixture.sources, -1));
            }
            SegmentBookieControlLog.ReadAdmission held = fixture.bookie.admitRead(context()).orElseThrow();
            try {
                assertThrows(IOException.class, () -> fixture.prepare(copy, fixture.sources, -1));
            } finally {
                held.finish();
            }
            assertThrows(IOException.class, () -> fixture.prepare(copy, fixture.sources, Long.MAX_VALUE));
            assertThrows(IOException.class, () -> SegmentArenaMove.prepare(fixture.arena, fixture.files,
                    fixture.bookie, copy, fixture.sources.stream()
                            .map(location -> new SegmentArenaMove.Source(location, -1)).toList(),
                    ByteBuffer.allocate(256)));
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
        }
    }

    @Test
    public void moveControlForceUnknownDoesNotCutOverBeforeApplyButReplaysDurableFacts() throws Exception {
        IsolatedSegmentConfig config = config();
        SegmentPointRead.Location original;
        SegmentPointRead.Location target;
        // OPEN and ALLOC are the first two barriers; the MOVE barrier has an unknown result.
        try (MoveFixture fixture = new MoveFixture(config,
                new SegmentFileIo(Integer.MAX_VALUE, -1, 3, false))) {
            original = fixture.sources.get(0);
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(601, 2, (byte) 0);
            target = copy.copies().get(0).location();
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertThrows(IOException.class, () -> fixture.arena.submitBatch(List.of(move)));
                assertEquals(original, fixture.arena.resolveLocation(original));
                assertFalse(fixture.arena.dataFileAvailable(0));
                fixture.failedCloseExpected = true;
            }
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(target, arena.resolveLocation(original));
            assertFalse(arena.dataFileAvailable(0)); // Unclosed writer still cannot provide service authority.
        }
    }


    @Test
    public void cancelledMoveWaitRetainsProtectionThroughActualForceAndOwnerApply() throws Exception {
        CountDownLatch atForce = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger barriers = new AtomicInteger();
        SegmentFileIo controlIo = new SegmentFileIo(Integer.MAX_VALUE, -1, -1, false, () -> {
            if (barriers.incrementAndGet() == 3) {
                atForce.countDown();
                await(release);
            }
        });
        try (MoveFixture fixture = new MoveFixture(config(), controlIo)) {
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(601, 2, (byte) 0);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                SegmentArenaControlDriver control = new SegmentArenaControlDriver(
                        fixture.owner, fixture.group.io(), fixture.arena);
                CompletableFuture<SegmentArenaControlLog.Result> waiter = new CompletableFuture<>();
                AtomicReference<SegmentArenaControlLog.Result> actual = new AtomicReference<>();
                AtomicInteger callbacks = new AtomicInteger();
                try {
                    assertTrue(control.submitMove(move, (results, failure) -> {
                        fixture.owner.requireOwner();
                        assertNull(failure);
                        callbacks.incrementAndGet();
                        actual.set(results.get(0));
                        waiter.complete(results.get(0));
                    }));
                    assertTrue(atForce.await(5, TimeUnit.SECONDS));
                    assertTrue(waiter.cancel(false));
                    assertThrows(IllegalStateException.class, move::close);
                    assertThrows(IOException.class, fixture.files::pinDataRead);
                    assertFalse(fixture.bookie.admitRead(context()).isPresent());
                    assertFalse(fixture.group.shutdown(Duration.ofMillis(20)));
                    assertNull(actual.get());
                } finally {
                    release.countDown();
                }
                assertTrue(fixture.group.shutdown(Duration.ofSeconds(5)));
                assertEquals(1, callbacks.get());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE, actual.get().status());
                assertEquals(copy.copies().get(0).location(), fixture.arena.resolveLocation(fixture.sources.get(0)));
                try (IsolatedSegmentFiles.DataReadLease ignored = fixture.files.pinDataRead()) {
                    assertTrue(fixture.arena.hasMoves());
                }
            }
        } finally {
            release.countDown();
        }
    }


    @Test
    public void movedSourcesFreeAndReuseWithoutNewCheckpointRecoverCurrentCopy() throws Exception {
        IsolatedSegmentConfig config = config();
        long middle = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
        long last = middle + SegmentArenaControlLog.BLOCK_BYTES;
        List<SegmentPointRead.Location> targets;
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, middle), reclaimableAllocation(7, last)));
            SegmentDataPhysicalDriver.DurableRelocation first = fixture.copy(601, 2, (byte) 0, 6, middle);
            long selector;
            try (SegmentArenaMove move = fixture.prepare(first, fixture.sources, -1)) {
                selector = fixture.arena.submitBatch(List.of(move)).get(0).sequence();
            }
            SegmentPointRead.Location oldCopy = first.copies().get(0).location();
            SegmentBatchParser.Record oldRecord = SegmentPointRead.read(config, identity(),
                    fixture.arena, oldCopy).record();
            SegmentDataPhysicalDriver.DurableRelocation next = fixture.copy(602, 3, (byte) 0, 7, last);
            try (SegmentArenaMove move = fixture.prepare(next,
                    first.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList(),
                    selector)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
            targets = next.copies().stream().map(SegmentDataPhysicalDriver.RelocationCopy::location).toList();
            assertTrue(fixture.arena.wasCommittedCopy(oldCopy, oldRecord));
            assertThrows(IOException.class, () -> fixture.arena.prepareFree(new UUID(0, 701), 7,
                    fixture.files, fixture.bookie, ByteBuffer.allocate(4096)));
            try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0),
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ByteBuffer original = ByteBuffer.allocate(4096);
                SegmentFileIo.readFully(data, original, last);
                byte[] bad = original.array().clone();
                bad[328] ^= 1;
                new SegmentFileIo().writeFully(data, ByteBuffer.wrap(bad), last);
                assertThrows(IOException.class, () -> fixture.arena.prepareFree(new UUID(0, 704), 5,
                        fixture.files, fixture.bookie, ByteBuffer.allocate(4096)));
                assertTrue(fixture.arena.hasAppliedGrant(0, 4096, 4096, 0, 1, 5, 1));
                new SegmentFileIo().writeFully(data, original, last);
            }
            for (long source : new long[] {5, 6}) {
                try (SegmentArenaControlLog.Free free = fixture.arena.prepareFree(new UUID(0, 700 + source), source,
                        fixture.files, fixture.bookie, ByteBuffer.allocate(4096))) {
                    assertEquals(136, free.body().remaining());
                    assertEquals(fixture.arena.appliedThrough(), free.facts().arenaCut());
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            fixture.arena.submitBatch(List.of(free)).get(0).status());
                }
            }
            assertFalse(fixture.arena.wasCommittedCopy(oldCopy, oldRecord));
            assertThrows(IOException.class, () -> SegmentPointRead.read(config, identity(), fixture.arena, oldCopy));
            assertTrue(fixture.arena.wasCommittedCopy(targets.get(0), SegmentPointRead.read(config, identity(),
                    fixture.arena, targets.get(0)).record()));
            assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                    checkPublishedDuplicate(fixture.driver, fixture.bookie,
                            entryBytes(context(), 1, -1, 8, (byte) 0)));
            for (long entryId : new long[] {3, 4}) {
                SegmentArenaControlLog.Allocation reused = fixture.arena.planPools(List.of(
                        new SegmentArenaControlLog.PoolRequest(0, new Generation(1), 1, 1))).get(0).orElseThrow();
                assertEquals(entryId == 3 ? 4096 : middle, reused.offset());
                assertEquals(2, reused.generation());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(reused)).get(0).status());
                ByteBuf bytes = entry(context(), entryId);
                CompletableFuture<List<SegmentDataPhysicalDriver.NormalResult>> written = new CompletableFuture<>();
                try {
                    assertTrue(fixture.driver.submitNormal(fixture.bookie, SegmentBatchFormat.encode(config,
                            new SegmentBatchFormat.Address(0, 0, reused.allocationId(), new Generation(2),
                                    new Generation(1), entryId + 1, reused.offset()),
                            List.of(SegmentBatchFormat.entry(context(), bytes))),
                            List.of(fixture.bookie.admitNormal(context(), filled(20, (byte) 5)).orElseThrow()),
                            (results, failure) -> {
                                if (failure == null) {
                                    written.complete(results);
                                } else {
                                    written.completeExceptionally(failure);
                                }
                            }));
                } finally {
                    bytes.release();
                }
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        written.get(5, TimeUnit.SECONDS).get(0).status());
                ownerBarrier(fixture.owner);
            }
        }
        // A/B now have new grants; bytes past B's healthy CLOSED end cannot resurrect the old copy.
        try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0), StandardOpenOption.WRITE)) {
            new SegmentFileIo().writeFully(data, ByteBuffer.wrap(filled(4096, (byte) 0x7f)), middle + 4096);
            data.force(true);
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
            storage.restart();
            for (int i = 0; i < 2; i++) {
                try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), i + 1).result()) {
                    assertEquals(602, read.result().record().moveId().getLeastSignificantBits());
                }
                assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                        storageWrite(storage, context(), i + 1).status());
            }
            assertEquals(SegmentDataPhysicalDriver.NormalStatus.LOCAL_DURABLE,
                    storageWrite(storage, context(), 3).status());
        }
        try (SegmentArenaControlLog arena = SegmentArenaControlLog.open(config, identity(), 1024 * 1024)) {
            assertEquals(targets.get(0), arena.currentMoveLocation(context(), 1).orElseThrow());
            arena.rotate();
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
            storage.restart();
            try (SegmentPointRead.LeasedResult read = storageRead(storage, context(), 1).result()) {
                assertEquals(602, read.result().record().moveId().getLeastSignificantBits());
            }
        }
        try (FileChannel data = FileChannel.open(IsolatedSegmentFiles.dataPath(config, 0), StandardOpenOption.WRITE)) {
            new SegmentFileIo().writeFully(data, ByteBuffer.allocate(4096), last);
            data.force(true);
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
            assertThrows(IOException.class, storage::restart);
        }
    }

    @Test
    public void orphanCopyFreeRevokesLateMoveAndStaleSelectorCutCannotRetireAllocation() throws Exception {
        try (MoveFixture fixture = new MoveFixture(config(), new SegmentFileIo())) {
            long middle = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, middle)));
            SegmentDataPhysicalDriver.DurableRelocation orphan = fixture.copy(601, 2, (byte) 0, 6, middle);
            try (SegmentArenaControlLog.Free stale = fixture.arena.prepareFree(new UUID(0, 705), 6,
                    fixture.files, fixture.bookie, ByteBuffer.allocate(4096))) {
                fixture.arena.submitBatch(List.of(
                        reclaimableAllocation(7, middle + SegmentArenaControlLog.BLOCK_BYTES)));
                assertEquals(SegmentArenaControlLog.Status.CONDITION_FAILED,
                        fixture.arena.submitBatch(List.of(stale)).get(0).status());
                assertTrue(fixture.arena.hasAppliedGrant(0, middle, 4096, 0, 1, 6, 1));
            }
            try (SegmentArenaControlLog.Free free = fixture.arena.prepareFree(new UUID(0, 706), 6,
                    fixture.files, fixture.bookie, ByteBuffer.allocate(4096))) {
                assertEquals(136, free.body().remaining());
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(free)).get(0).status());
            }
            assertFalse(fixture.arena.hasAppliedGrant(0, middle, 4096, 0, 1, 6, 1));
            assertThrows(IOException.class, () -> fixture.prepare(orphan, fixture.sources, -1));
            assertTrue(fixture.arena.currentMoveLocation(context(), 1).isEmpty());
            assertEquals(SegmentDataPhysicalDriver.DuplicateStatus.SAME_DURABLE,
                    checkPublishedDuplicate(fixture.driver, fixture.bookie,
                            entryBytes(context(), 1, -1, 8, (byte) 0)));
        }
    }


    @Test
    public void terminalCurrentCopyFreeRetiresSelectorWithoutRestoringOldPermission() throws Exception {
        IsolatedSegmentConfig config = config();
        try (MoveFixture fixture = new MoveFixture(config, new SegmentFileIo())) {
            long next = 4096 + SegmentArenaControlLog.BLOCK_BYTES;
            fixture.arena.submitBatch(List.of(reclaimableAllocation(6, next)));
            SegmentDataPhysicalDriver.DurableRelocation copy = fixture.copy(601, 2, (byte) 0, 6, next);
            try (SegmentArenaMove move = fixture.prepare(copy, fixture.sources, -1)) {
                assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                        fixture.arena.submitBatch(List.of(move)).get(0).status());
            }
            assertThrows(IOException.class, () -> fixture.arena.prepareFree(new UUID(0, 705), 6,
                    fixture.files, fixture.bookie, ByteBuffer.allocate(4096)));
            fixture.bookie.submitBatch(List.of(tombstone(44, 20)));
            for (long allocation : new long[] {6, 5}) {
                try (SegmentArenaControlLog.Free free = fixture.arena.prepareFree(new UUID(0, 710 + allocation),
                        allocation, fixture.files, fixture.bookie, ByteBuffer.allocate(4096))) {
                    assertEquals(allocation == 6 ? 136 : 128, free.body().remaining());
                    assertEquals(SegmentArenaControlLog.Status.APPLIED_DURABLE,
                            fixture.arena.submitBatch(List.of(free)).get(0).status());
                }
            }
            assertTrue(fixture.arena.currentMoveLocation(context(), 1).isEmpty());
            assertFalse(fixture.bookie.admitRead(context()).isPresent());
        }
        try (IsolatedSegmentStorage storage = storage(config, 1, 6, 4, 16, 3)) {
            storage.restart();
            CompletableFuture<ReadReply> reply = new CompletableFuture<>();
            assertTrue(storage.submitRead(context(), 1, new SegmentPointRead.ReadRequest(
                    (status, result, failure) -> reply.complete(new ReadReply(status, result, failure)))));
            ReadReply result = reply.get(5, TimeUnit.SECONDS);
            assertEquals(SegmentPointRead.ReadStatus.NOT_READY, result.status());
            assertNull(result.result());
            assertNull(result.failure());
        }
    }

}

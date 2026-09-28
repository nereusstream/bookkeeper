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
package org.apache.bookkeeper.proto.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.profile.wire.ProfileFrame;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileOperation;
import org.apache.bookkeeper.common.profile.wire.ProfileWireValidationException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.TLSContextFactory;
import org.junit.Test;

public class ProfileReceiveDecoderTest {
    private static ProfileReceiveDecoder decoder(ProfileReceiveBudget budget, int limit, int base) {
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        assertNotNull(permit);
        return new ProfileReceiveDecoder(budget, permit, limit, base);
    }

    private static byte[] frame(ProfileOperation operation, int bodyBytes) {
        return ProfileFrameCodec.encode(ProfileFrame.request(operation, 1, new byte[bodyBytes]));
    }

    private static byte[] hello() {
        return ProfileFrameCodec.encode(ProfileFrame.request(ProfileOperation.HELLO, 1,
                new ProfileHello.Client(List.of()).encode()));
    }

    private static String certificate(String name) throws Exception {
        return Paths.get(ProfileReceiveDecoderTest.class.getClassLoader().getResource(name).toURI()).toString();
    }

    private static void transfer(EmbeddedChannel source, EmbeddedChannel target) {
        io.netty.buffer.ByteBuf message;
        while ((message = source.readOutbound()) != null) {
            target.writeInbound(message);
        }
    }

    private static SslHandler clientTls() throws Exception {
        SslContext clientContext = SslContextBuilder.forClient()
                .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                .trustManager(new File(certificate("server-cert.pem")))
                .protocols("TLSv1.3").build();
        return clientContext.newHandler(UnpooledByteBufAllocator.DEFAULT, "apache.bookkeeper.org", 0);
    }

    private static void handshake(EmbeddedChannel client, EmbeddedChannel server,
                                  SslHandler clientTls, SslHandler serverTls) {
        for (int attempt = 0; attempt < 100 && (!serverTls.handshakeFuture().isDone()
                || !clientTls.handshakeFuture().isDone()); attempt++) {
            transfer(client, server);
            transfer(server, client);
            client.runPendingTasks();
            server.runPendingTasks();
        }
        assertTrue(serverTls.handshakeFuture().isSuccess());
        assertTrue(clientTls.handshakeFuture().isSuccess());
    }

    @Test
    public void chargesBeforeBodyAndTransfersCreditUntilFrameRelease() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(16_384, 4_096, 1_024, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 8_192, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        assertEquals(64, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        byte[] hello = hello();
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(hello, 0, 2))));
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(hello, 2, hello.length))));
        ProfileInboundFrame greeting = channel.readInbound();
        assertEquals(ProfileOperation.HELLO, greeting.header().operation());
        greeting.close();
        decoder.markEstablished();

        byte[] add = frame(ProfileOperation.ADD_NORMAL, 1024);
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(add, 36))));
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(add, 36, 536))));
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(add, 536, add.length))));
        ProfileInboundFrame received = channel.readInbound();
        assertEquals(1024, received.body().readableBytes());
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        channel.finish();
        assertEquals(add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        received.close();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertThrows(IllegalStateException.class, received::body);
    }

    @Test
    public void forwardsDisabledZeroBodyFirstRequestOnDataLane() {
        for (ProfileOperation operation : List.of(ProfileOperation.RANGE_READ,
                ProfileOperation.BATCH_RECOVERY_ADD)) {
            ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 512, 8, 8, 4);
            ProfileReceiveDecoder decoder = decoder(budget, 900, 64);
            EmbeddedChannel channel = new EmbeddedChannel(decoder);
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
            ProfileInboundFrame greeting = channel.readInbound();
            greeting.close();
            decoder.markEstablished();

            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(frame(operation, 0))));
            ProfileInboundFrame request = channel.readInbound();
            assertEquals(operation, request.header().operation());
            assertEquals(0, request.body().readableBytes());
            assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
            assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
            request.close();
            channel.finishAndReleaseAll();
            assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
            assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
        }
    }

    @Test
    public void controlCapacitySurvivesDataPressureAndRejectsDataSubtype() throws Exception {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_000, 1_000, 500, 8, 8, 2);
        EmbeddedChannel first = new EmbeddedChannel(decoder(budget, 2_000, 64));
        ProfileReceiveDecoder secondDecoder = decoder(budget, 2_000, 64);
        EmbeddedChannel second = new EmbeddedChannel(secondDecoder);
        byte[] hello = hello();
        first.writeInbound(Unpooled.wrappedBuffer(hello));
        ProfileInboundFrame firstHello = first.readInbound();
        firstHello.close();
        first.pipeline().get(ProfileReceiveDecoder.class).markEstablished();
        second.writeInbound(Unpooled.wrappedBuffer(hello));
        ProfileInboundFrame secondHello = second.readInbound();
        secondHello.close();
        secondDecoder.markEstablished();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 1_500);
        first.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(add, 36)));
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertThrows(IOException.class, () -> second.writeInbound(
                Unpooled.wrappedBuffer(Arrays.copyOf(add, 36))));
        assertFalse(second.isActive());

        ProfileReceiveDecoder controlDecoder = decoder(budget, 900, 64);
        EmbeddedChannel control = new EmbeddedChannel(controlDecoder);
        control.writeInbound(Unpooled.wrappedBuffer(hello));
        ProfileInboundFrame controlHello = control.readInbound();
        controlHello.close();
        controlDecoder.markEstablished();
        byte[] install = frame(ProfileOperation.INSTALL, 20);
        control.writeInbound(Unpooled.wrappedBuffer(install));
        ProfileInboundFrame accepted = control.readInbound();
        assertEquals(ProfileOperation.INSTALL, accepted.header().operation());
        assertThrows(IllegalStateException.class, () -> controlDecoder.markControlAuthorized(controlHello));
        controlDecoder.markControlAuthorized(accepted);
        assertEquals(64 + install.length, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        accepted.close();
        assertThrows(IOException.class, () -> control.writeInbound(
                Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 0))));
        assertFalse(control.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        first.finish();
        second.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void badHeaderAndPartialCloseReleaseAllReceiveCredit() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 1_024, 512, 8, 8, 4);
        EmbeddedChannel partial = new EmbeddedChannel(decoder(budget, 4_096, 64));
        byte[] hello = hello();
        assertFalse(partial.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(hello, 5))));
        partial.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        ProfileReceiveDecoder bodyDecoder = decoder(budget, 4_096, 64);
        EmbeddedChannel partialBody = new EmbeddedChannel(bodyDecoder);
        assertTrue(partialBody.writeInbound(Unpooled.wrappedBuffer(hello)));
        ProfileInboundFrame bodyHello = partialBody.readInbound();
        bodyHello.close();
        bodyDecoder.markEstablished();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 512);
        assertFalse(partialBody.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(add, 100))));
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        partialBody.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        EmbeddedChannel invalidHello = new EmbeddedChannel(decoder(budget, 4_096, 64));
        assertThrows(ProfileWireValidationException.class,
                () -> invalidHello.writeInbound(Unpooled.wrappedBuffer(frame(ProfileOperation.HELLO, 0))));
        assertFalse(invalidHello.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        EmbeddedChannel invalid = new EmbeddedChannel(decoder(budget, 4_096, 64));
        byte[] bad = hello.clone();
        ByteBuffer.wrap(bad).putInt(0, ProfileFrameCodec.ABSOLUTE_FRAME_MAX + 1);
        assertThrows(ProfileWireValidationException.class,
                () -> invalid.writeInbound(Unpooled.wrappedBuffer(bad)));
        assertFalse(invalid.isActive());
        assertNull(invalid.readInbound());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void countsPipelinedBodyAlreadyInsideOneReadBeforeAcceptingIt() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 2_500, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 4_000, 64);
        AtomicInteger hellos = new AtomicInteger();
        EmbeddedChannel data = new EmbeddedChannel(decoder, new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                try (ProfileInboundFrame received = (ProfileInboundFrame) message) {
                    assertEquals(ProfileOperation.HELLO, received.header().operation());
                    hellos.incrementAndGet();
                    decoder.markEstablished();
                }
            }
        });
        byte[] hello = hello();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 2_048);
        byte[] pipelined = Arrays.copyOf(hello, hello.length + add.length);
        System.arraycopy(add, 0, pipelined, hello.length, add.length);
        io.netty.buffer.ByteBuf input = Unpooled.wrappedBuffer(pipelined);
        assertThrows(IOException.class, () -> data.writeInbound(input));
        assertEquals(1, hellos.get());
        assertEquals(0, input.refCnt());
        assertFalse(data.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        EmbeddedChannel control = new EmbeddedChannel(decoder(budget, 900, 64));
        assertTrue(control.writeInbound(Unpooled.wrappedBuffer(hello)));
        ProfileInboundFrame controlHello = control.readInbound();
        controlHello.close();
        control.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
    }

    @Test
    public void classifiesPipelinedHelloAndDataWithoutMovingInboundCredit() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 1_024, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 3_000, 64);
        ProfileInboundFrame[] retained = new ProfileInboundFrame[1];
        EmbeddedChannel channel = new EmbeddedChannel(decoder, new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                ProfileInboundFrame received = (ProfileInboundFrame) message;
                if (received.header().operation() == ProfileOperation.HELLO) {
                    decoder.markEstablished();
                    received.close();
                } else {
                    retained[0] = received;
                }
            }
        });
        byte[] greeting = hello();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 64);
        byte[] pipelined = Arrays.copyOf(greeting, greeting.length + add.length);
        System.arraycopy(add, 0, pipelined, greeting.length, add.length);
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(pipelined)));
        assertEquals(ProfileOperation.ADD_NORMAL, retained[0].header().operation());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
        assertEquals(64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        retained[0].close();
        assertThrows(IOException.class, () -> channel.writeInbound(
                Unpooled.wrappedBuffer(frame(ProfileOperation.INSTALL, 0))));
        assertFalse(channel.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void authorizesControlCandidateInSameReadAndKeepsConnectionControl() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 1_024, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 3_000, 64);
        AtomicInteger controls = new AtomicInteger();
        ProfileInboundFrame[] retained = new ProfileInboundFrame[1];
        EmbeddedChannel channel = new EmbeddedChannel(decoder, new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
                ProfileInboundFrame received = (ProfileInboundFrame) message;
                if (received.header().operation() == ProfileOperation.HELLO) {
                    decoder.markEstablished();
                    received.close();
                } else if (controls.incrementAndGet() == 1) {
                    // This test supplies the downstream authorization decision; the real handler is still pending.
                    decoder.markControlAuthorized(received);
                    received.close();
                } else {
                    retained[0] = received;
                }
            }
        });
        byte[] greeting = hello();
        byte[] fence = frame(ProfileOperation.FENCE_LEDGER, 100);
        byte[] pipelined = Arrays.copyOf(greeting, greeting.length + fence.length * 2);
        System.arraycopy(fence, 0, pipelined, greeting.length, fence.length);
        System.arraycopy(fence, 0, pipelined, greeting.length + fence.length, fence.length);
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(pipelined)));
        assertEquals(2, controls.get());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
        assertEquals(64 + fence.length, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        assertThrows(IOException.class, () -> channel.writeInbound(
                Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 0))));
        assertFalse(channel.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
        assertEquals(fence.length, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        retained[0].close();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
    }

    @Test
    public void failedControlTransferKeepsHeldFrameChargedToUnclassified() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(1_000, 150, 500, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 900, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
        ProfileInboundFrame greeting = channel.readInbound();
        greeting.close();
        decoder.markEstablished();
        byte[] fence = frame(ProfileOperation.FENCE_LEDGER, 100);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(fence)));
        ProfileInboundFrame candidate = channel.readInbound();
        assertThrows(IOException.class, () -> decoder.markControlAuthorized(candidate));
        assertFalse(channel.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(fence.length, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        candidate.close();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void fullDataConnectionLaneStillLeavesReservedControlSlot() throws Exception {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 1_024, 3, 1, 1);
        ProfileReceiveDecoder dataDecoder = decoder(budget, 900, 64);
        EmbeddedChannel data = new EmbeddedChannel(dataDecoder);
        data.writeInbound(Unpooled.wrappedBuffer(hello()));
        ProfileInboundFrame dataHello = data.readInbound();
        dataHello.close();
        dataDecoder.markEstablished();
        data.writeInbound(Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 0)));
        ProfileInboundFrame add = data.readInbound();
        add.close();
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));

        ProfileReceiveDecoder rejectedDecoder = decoder(budget, 900, 64);
        EmbeddedChannel rejected = new EmbeddedChannel(rejectedDecoder);
        rejected.writeInbound(Unpooled.wrappedBuffer(hello()));
        ProfileInboundFrame rejectedHello = rejected.readInbound();
        rejectedHello.close();
        rejectedDecoder.markEstablished();
        assertThrows(IOException.class, () -> rejected.writeInbound(
                Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 0))));
        assertFalse(rejected.isActive());

        ProfileReceiveDecoder controlDecoder = decoder(budget, 900, 64);
        EmbeddedChannel control = new EmbeddedChannel(controlDecoder);
        control.writeInbound(Unpooled.wrappedBuffer(hello()));
        ProfileInboundFrame controlHello = control.readInbound();
        controlHello.close();
        controlDecoder.markEstablished();
        control.writeInbound(Unpooled.wrappedBuffer(frame(ProfileOperation.FENCE_LEDGER, 100)));
        ProfileInboundFrame fence = control.readInbound();
        controlDecoder.markControlAuthorized(fence);
        fence.close();
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
        control.finish();
        data.finish();
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.CONTROL));
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void stopsAtDownstreamCloseBeforePipelinedSecondFrame() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 1_024, 512, 8, 8, 4);
        AtomicInteger delivered = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(decoder(budget, 6_000, 64),
                new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                try (ProfileInboundFrame received = (ProfileInboundFrame) message) {
                    delivered.incrementAndGet();
                    assertEquals(ProfileOperation.HELLO, received.header().operation());
                    ctx.close();
                }
            }
        });
        byte[] greeting = hello();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 64);
        byte[] pipelined = Arrays.copyOf(greeting, greeting.length + add.length);
        System.arraycopy(add, 0, pipelined, greeting.length, add.length);
        io.netty.buffer.ByteBuf input = Unpooled.wrappedBuffer(pipelined);
        assertFalse(channel.writeInbound(input));
        assertEquals(1, delivered.get());
        assertFalse(channel.isActive());
        assertEquals(0, input.refCnt());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void rejectsOversizedFenceBeforeChargingControlBody() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 2_048, 512, 8, 8, 4);
        ProfileReceiveDecoder decoder = decoder(budget, 6_000, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
        ProfileInboundFrame greeting = channel.readInbound();
        greeting.close();
        decoder.markEstablished();

        byte[] oversized = frame(ProfileOperation.FENCE_LEDGER, 1_024);
        assertThrows(IOException.class, () -> channel.writeInbound(
                Unpooled.wrappedBuffer(Arrays.copyOf(oversized, 36))));
        assertFalse(channel.isActive());
        assertNull(channel.readInbound());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
    }

    @Test
    public void unclassifiedSlotRejectsBeforeTlsInstallationAndRecoversOnClose() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(certificate("server-key.pem"));
        conf.setTLSCertificatePath(certificate("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(certificate("client-cert.pem"));
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 1_024, 1, 2, 1);
        EmbeddedChannel first = new EmbeddedChannel();
        first.freezeTime();
        assertTrue(ProfileInboundPipeline.install(first.pipeline(), profile, budget, 3_000, 64,
                new ChannelInboundHandlerAdapter()));
        assertNotNull(first.pipeline().get(SslHandler.class));
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));

        EmbeddedChannel denied = new EmbeddedChannel();
        assertFalse(ProfileInboundPipeline.install(denied.pipeline(), profile, budget, 3_000, 64,
                new ChannelInboundHandlerAdapter()));
        assertFalse(denied.isActive());
        assertNull(denied.pipeline().get(SslHandler.class));
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));

        first.advanceTimeBy(11, TimeUnit.SECONDS);
        first.runScheduledPendingTasks();
        assertFalse(first.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        EmbeddedChannel wrongTls = new EmbeddedChannel();
        assertThrows(org.apache.bookkeeper.tls.SecurityException.class,
                () -> ProfileInboundPipeline.install(wrongTls.pipeline(), new TLSContextFactory(), budget,
                        3_000, 64, new ChannelInboundHandlerAdapter()));
        assertFalse(wrongTls.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        EmbeddedChannel recovered = new EmbeddedChannel();
        assertTrue(ProfileInboundPipeline.install(recovered.pipeline(), profile, budget, 3_000, 64,
                new ChannelInboundHandlerAdapter()));
        recovered.finishAndReleaseAll();
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
    }

    @Test
    public void helloDeadlineClosesSlowConnectionWithPartialPlaintext() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(certificate("server-key.pem"));
        conf.setTLSCertificatePath(certificate("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(certificate("client-cert.pem"));
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024, 1_024, 1, 2, 1);
        EmbeddedChannel server = new EmbeddedChannel();
        server.freezeTime();
        assertTrue(ProfileInboundPipeline.install(server.pipeline(), profile, budget, 3_000, 64,
                new ChannelInboundHandlerAdapter()));
        SslHandler serverTls = server.pipeline().get(SslHandler.class);
        SslHandler clientTls = clientTls();
        EmbeddedChannel client = new EmbeddedChannel(clientTls);
        handshake(client, server, clientTls, serverTls);
        client.writeOutbound(Unpooled.wrappedBuffer(Arrays.copyOf(hello(), 1)));
        transfer(client, server);
        assertTrue(server.isActive());
        server.advanceTimeBy(11, TimeUnit.SECONDS);
        server.runScheduledPendingTasks();
        assertFalse(server.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
    }

    @Test
    public void firstRequestDeadlineDoesNotResetForPartialBody() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(certificate("server-key.pem"));
        conf.setTLSCertificatePath(certificate("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(certificate("client-cert.pem"));
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 1_024, 1_024, 1, 2, 1);
        EmbeddedChannel server = new EmbeddedChannel();
        server.freezeTime();
        assertTrue(ProfileInboundPipeline.install(server.pipeline(), profile, budget, 4_000, 64,
                new ChannelInboundHandlerAdapter()));
        SslHandler serverTls = server.pipeline().get(SslHandler.class);
        SslHandler clientTls = clientTls();
        EmbeddedChannel client = new EmbeddedChannel(clientTls);
        handshake(client, server, clientTls, serverTls);
        client.writeOutbound(Unpooled.wrappedBuffer(hello()));
        transfer(client, server);
        ProfileInboundFrame greeting = server.readInbound();
        greeting.close();
        server.pipeline().get(ProfileReceiveDecoder.class).markEstablished();

        byte[] add = frame(ProfileOperation.ADD_NORMAL, 512);
        client.writeOutbound(Unpooled.wrappedBuffer(Arrays.copyOf(add, 100)));
        transfer(client, server);
        assertEquals(1, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
        server.advanceTimeBy(15, TimeUnit.SECONDS);
        server.runScheduledPendingTasks();
        assertTrue(server.isActive());
        client.writeOutbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(add, 100, 101)));
        transfer(client, server);
        server.advanceTimeBy(16, TimeUnit.SECONDS);
        server.runScheduledPendingTasks();
        assertFalse(server.isActive());
        assertEquals(0, budget.usedConnections(ProfileReceiveBudget.Lane.DATA));
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
    }

    @Test
    public void laterPartialFrameDeadlineReclaimsDataAndControlCredit() throws Exception {
        for (ProfileOperation operation : List.of(ProfileOperation.ADD_NORMAL, ProfileOperation.FENCE_LEDGER)) {
            ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 2_048, 2_048, 1, 1, 1);
            ProfileReceiveDecoder decoder = decoder(budget, 4_000, 64);
            EmbeddedChannel channel = new EmbeddedChannel(decoder);
            channel.freezeTime();
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
            ProfileInboundFrame greeting = channel.readInbound();
            greeting.close();
            decoder.markEstablished();
            byte[] first = frame(operation, operation == ProfileOperation.FENCE_LEDGER ? 100 : 512);
            assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(first)));
            ProfileInboundFrame accepted = channel.readInbound();
            if (operation == ProfileOperation.FENCE_LEDGER) {
                decoder.markControlAuthorized(accepted);
            }
            accepted.close();
            ProfileReceiveBudget.Lane lane = operation == ProfileOperation.FENCE_LEDGER
                    ? ProfileReceiveBudget.Lane.CONTROL : ProfileReceiveBudget.Lane.DATA;

            channel.advanceTimeBy(31, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isActive());
            byte[] later = frame(operation, operation == ProfileOperation.FENCE_LEDGER ? 100 : 512);
            assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(later, 100))));
            assertEquals(64 + later.length, budget.used(lane));
            channel.advanceTimeBy(15, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isActive());
            assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(later, 100, 101))));
            channel.advanceTimeBy(16, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse(channel.isActive());
            assertEquals(0, budget.used(lane));
            assertEquals(0, budget.usedConnections(lane));
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void completedLaterFrameCancelsItsReceiveDeadline() throws Exception {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 2_048, 2_048, 1, 1, 1);
        ProfileReceiveDecoder decoder = decoder(budget, 4_000, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        channel.freezeTime();
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
        ProfileInboundFrame greeting = channel.readInbound();
        greeting.close();
        decoder.markEstablished();
        byte[] add = frame(ProfileOperation.ADD_NORMAL, 512);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(add)));
        ProfileInboundFrame first = channel.readInbound();
        first.close();

        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(add, 100))));
        channel.advanceTimeBy(15, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(add, 100, add.length))));
        ProfileInboundFrame second = channel.readInbound();
        second.close();
        channel.advanceTimeBy(16, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        assertTrue(channel.isActive());
        channel.finishAndReleaseAll();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void pendingControlAuthorizationRejectsPipelinedPartialHeader() throws Exception {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 2_048, 2_048, 1, 1, 1);
        ProfileReceiveDecoder decoder = decoder(budget, 4_000, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(hello())));
        ProfileInboundFrame greeting = channel.readInbound();
        greeting.close();
        decoder.markEstablished();
        byte[] fence = frame(ProfileOperation.FENCE_LEDGER, 100);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(fence)));
        ProfileInboundFrame pending = channel.readInbound();
        assertThrows(IOException.class, () -> channel.writeInbound(Unpooled.wrappedBuffer(
                Arrays.copyOf(fence, 1))));
        assertFalse(channel.isActive());
        assertEquals(fence.length, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        pending.close();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.UNCLASSIFIED));
        channel.finishAndReleaseAll();
    }

    @Test
    public void receivesHelloAndDataOnlyAfterRealMutualTlsHandshake() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(certificate("server-key.pem"));
        conf.setTLSCertificatePath(certificate("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(certificate("client-cert.pem"));
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        ProfileReceiveBudget budget = new ProfileReceiveBudget(16_384, 4_096, 1_024, 8, 8, 4);
        EmbeddedChannel server = new EmbeddedChannel();
        assertTrue(ProfileInboundPipeline.install(server.pipeline(), profile, budget, 8_192, 64,
                new ChannelInboundHandlerAdapter()));
        SslHandler serverTls = server.pipeline().get(SslHandler.class);
        ProfileReceiveDecoder decoder = server.pipeline().get(ProfileReceiveDecoder.class);

        SslHandler clientTls = clientTls();
        EmbeddedChannel client = new EmbeddedChannel(clientTls);
        handshake(client, server, clientTls, serverTls);

        client.writeOutbound(Unpooled.wrappedBuffer(hello()));
        transfer(client, server);
        ProfileInboundFrame greeting = server.readInbound();
        assertEquals(ProfileOperation.HELLO, greeting.header().operation());
        greeting.close();
        decoder.markEstablished();

        client.writeOutbound(Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 16)));
        transfer(client, server);
        ProfileInboundFrame add = server.readInbound();
        assertEquals(ProfileOperation.ADD_NORMAL, add.header().operation());
        assertEquals(16, add.body().readableBytes());
        add.close();
        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }
}

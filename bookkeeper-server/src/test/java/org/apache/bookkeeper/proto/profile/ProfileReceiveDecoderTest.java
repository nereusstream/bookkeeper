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

    @Test
    public void chargesBeforeBodyAndTransfersCreditUntilFrameRelease() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(16_384, 4_096);
        ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 8_192, 64);
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        assertEquals(64, budget.used(ProfileReceiveBudget.Lane.DATA));
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
    public void controlCapacitySurvivesDataPressureAndRejectsDataSubtype() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_000, 1_000);
        EmbeddedChannel first = new EmbeddedChannel(new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 2_000, 64));
        ProfileReceiveDecoder secondDecoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 2_000, 64);
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
        assertEquals(64 + 64 + add.length, budget.used(ProfileReceiveBudget.Lane.DATA));
        assertThrows(IOException.class, () -> second.writeInbound(
                Unpooled.wrappedBuffer(Arrays.copyOf(add, 36))));
        assertFalse(second.isActive());

        ProfileReceiveDecoder controlDecoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.CONTROL, 900, 64);
        EmbeddedChannel control = new EmbeddedChannel(controlDecoder);
        control.writeInbound(Unpooled.wrappedBuffer(hello));
        ProfileInboundFrame controlHello = control.readInbound();
        controlHello.close();
        controlDecoder.markEstablished();
        byte[] install = frame(ProfileOperation.INSTALL, 20);
        control.writeInbound(Unpooled.wrappedBuffer(install));
        ProfileInboundFrame accepted = control.readInbound();
        assertEquals(ProfileOperation.INSTALL, accepted.header().operation());
        accepted.close();
        assertThrows(IOException.class, () -> control.writeInbound(
                Unpooled.wrappedBuffer(frame(ProfileOperation.ADD_NORMAL, 0))));
        assertFalse(control.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
        first.finish();
        second.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));
    }

    @Test
    public void badHeaderAndPartialCloseReleaseAllReceiveCredit() {
        ProfileReceiveBudget budget = new ProfileReceiveBudget(8_192, 1_024);
        EmbeddedChannel partial = new EmbeddedChannel(new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 4_096, 64));
        byte[] hello = hello();
        assertFalse(partial.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(hello, 5))));
        partial.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        ProfileReceiveDecoder bodyDecoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 4_096, 64);
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

        EmbeddedChannel invalidHello = new EmbeddedChannel(new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 4_096, 64));
        assertThrows(ProfileWireValidationException.class,
                () -> invalidHello.writeInbound(Unpooled.wrappedBuffer(frame(ProfileOperation.HELLO, 0))));
        assertFalse(invalidHello.isActive());
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.DATA));

        EmbeddedChannel invalid = new EmbeddedChannel(new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 4_096, 64));
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
        ProfileReceiveBudget budget = new ProfileReceiveBudget(4_096, 1_024);
        ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 4_000, 64);
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

        EmbeddedChannel control = new EmbeddedChannel(new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.CONTROL, 900, 64));
        assertTrue(control.writeInbound(Unpooled.wrappedBuffer(hello)));
        ProfileInboundFrame controlHello = control.readInbound();
        controlHello.close();
        control.finish();
        assertEquals(0, budget.used(ProfileReceiveBudget.Lane.CONTROL));
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
        SslHandler serverTls = profile.newTLSHandler();
        ProfileReceiveBudget budget = new ProfileReceiveBudget(16_384, 4_096);
        ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(budget,
                ProfileReceiveDecoder.ConnectionPurpose.DATA, 8_192, 64);
        EmbeddedChannel server = new EmbeddedChannel(serverTls, decoder);

        SslContext clientContext = SslContextBuilder.forClient()
                .keyManager(new File(certificate("client-cert.pem")), new File(certificate("client-key.pem")))
                .trustManager(new File(certificate("server-cert.pem")))
                .protocols("TLSv1.3").build();
        SslHandler clientTls = clientContext.newHandler(UnpooledByteBufAllocator.DEFAULT,
                "apache.bookkeeper.org", 0);
        EmbeddedChannel client = new EmbeddedChannel(clientTls);
        for (int attempt = 0; attempt < 100 && (!serverTls.handshakeFuture().isDone()
                || !clientTls.handshakeFuture().isDone()); attempt++) {
            transfer(client, server);
            transfer(server, client);
            client.runPendingTasks();
            server.runPendingTasks();
        }
        assertTrue(serverTls.handshakeFuture().isSuccess());
        assertTrue(clientTls.handshakeFuture().isSuccess());

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

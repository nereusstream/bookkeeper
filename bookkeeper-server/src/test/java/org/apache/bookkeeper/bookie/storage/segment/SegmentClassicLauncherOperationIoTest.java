/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Disposition;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Frame;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncherOperationCodec.Kind;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Private actual Unix sockets only; no root peer, protected endpoint, listener owner or accepted operation. */
public class SegmentClassicLauncherOperationIoTest {
    private static final UUID EPISODE = UUID.randomUUID();
    private static final Frame STOP = new Frame(Kind.STOP, EPISODE, null, null);
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void originalUnixPairExchangesTheBoundedGreetingCommandAndAcceptanceBytes() throws Exception {
        withPair((client, clientSelector, server, serverSelector) -> {
            long deadline = deadline();
            var greeting = new Frame(Kind.GREETING, EPISODE, null, null);
            SegmentClassicLauncherOperationIo.write(server, serverSelector, greeting, deadline);
            assertEquals(greeting, SegmentClassicLauncherOperationIo.read(client, clientSelector, deadline));
            SegmentClassicLauncherOperationIo.write(client, clientSelector, STOP, deadline);
            assertEquals(STOP, SegmentClassicLauncherOperationIo.read(server, serverSelector, deadline));
            var reply = new Frame(Kind.REPLY, EPISODE, null, Disposition.ACCEPTED);
            SegmentClassicLauncherOperationIo.write(server, serverSelector, reply, deadline);
            assertEquals(reply, SegmentClassicLauncherOperationIo.read(client, clientSelector, deadline));
            assertThrows(IOException.class, () -> SegmentClassicLauncherOperationIo.connect(client, clientSelector,
                    (UnixDomainSocketAddress) server.getLocalAddress(), deadline));
        });
    }

    @Test(timeout = 10000)
    public void halfHeaderDeadlineAndHalfBodyEofDoNotCloseTheOwnersChannelOrSelector() throws Exception {
        byte[] command = SegmentClassicLauncherOperationCodec.encode(STOP);
        withPair((client, clientSelector, server, serverSelector) -> {
            sendBytes(client, Arrays.copyOf(command, 8));
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(80);
            assertThrows(SocketTimeoutException.class,
                    () -> SegmentClassicLauncherOperationIo.read(server, serverSelector, deadline));
            assertTrue(server.isOpen());
            assertTrue(serverSelector.isOpen());
        });
        withPair((client, clientSelector, server, serverSelector) -> {
            sendBytes(client, Arrays.copyOf(command, 20));
            client.shutdownOutput();
            assertThrows(EOFException.class,
                    () -> SegmentClassicLauncherOperationIo.read(server, serverSelector, deadline()));
            assertTrue(server.isOpen());
            assertTrue(serverSelector.isOpen());
        });
    }

    @Test(timeout = 10000)
    public void anUnboundedAnnouncedBodyIsRejectedWithoutWaitingForIt() throws Exception {
        withPair((client, clientSelector, server, serverSelector) -> {
            byte[] header = Arrays.copyOf(SegmentClassicLauncherOperationCodec.encode(STOP), 16);
            ByteBuffer.wrap(header).putInt(12, Integer.MAX_VALUE);
            sendBytes(client, header);
            IOException failure = assertThrows(IOException.class,
                    () -> SegmentClassicLauncherOperationIo.read(server, serverSelector, deadline()));
            assertFalse(failure instanceof SocketTimeoutException);
            assertTrue(server.isOpen());
        });
    }

    @Test(timeout = 10000)
    public void callerDeadlineAndInterruptionDoNotSendACommandOrCancelTheOriginalSocket() throws Exception {
        withPair((client, clientSelector, server, serverSelector) -> {
            assertThrows(SocketTimeoutException.class, () -> SegmentClassicLauncherOperationIo.write(client,
                    clientSelector, STOP, System.nanoTime() - 1));
            assertEquals(0, server.read(ByteBuffer.allocate(48)));
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedIOException.class, () -> SegmentClassicLauncherOperationIo.write(client,
                        clientSelector, STOP, deadline()));
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            assertTrue(client.isOpen());
            SegmentClassicLauncherOperationIo.write(client, clientSelector, STOP, deadline());
            assertEquals(STOP, SegmentClassicLauncherOperationIo.read(server, serverSelector, deadline()));
        });
    }

    private static long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    }

    private static void sendBytes(SocketChannel channel, byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long deadline = deadline();
        while (buffer.hasRemaining()) {
            assertTrue("Private peer could not send its bounded fragment", deadline - System.nanoTime() > 0);
            if (channel.write(buffer) == 0) {
                Thread.sleep(1);
            }
        }
    }

    private void withPair(Exchange exchange) throws Exception {
        Path socket = temporary.getRoot().toPath().resolve("op.sock");
        boolean bound = false;
        try (var listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
             var client = SocketChannel.open(StandardProtocolFamily.UNIX);
             var clientSelector = Selector.open(); var serverSelector = Selector.open()) {
            listener.bind(UnixDomainSocketAddress.of(socket));
            bound = true;
            client.configureBlocking(false).register(clientSelector, 0);
            SegmentClassicLauncherOperationIo.connect(client, clientSelector, UnixDomainSocketAddress.of(socket),
                    deadline());
            try (var server = listener.accept()) {
                server.configureBlocking(false).register(serverSelector, 0);
                exchange.run(client, clientSelector, server, serverSelector);
            }
        } finally {
            if (bound) {
                Files.deleteIfExists(socket); // This private fixture created this exact endpoint.
            }
        }
    }

    @FunctionalInterface
    private interface Exchange {
        void run(SocketChannel client, Selector clientSelector, SocketChannel server, Selector serverSelector)
                throws Exception;
    }
}

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Kernel peer data or actual platform rejection only; no root check, unit, isolation or operation authority. */
public class NativeIOUnixPeerCredentialsTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void numericObservationsPreserveRootIdsButRejectMissingPidAndInvalidUidGidSentinels() {
        var root = new NativeIO.UnixPeerCredentials(1, 0, 0);
        assertEquals(0, root.uid());
        assertEquals(0, root.gid());
        assertThrows(IllegalArgumentException.class, () -> new NativeIO.UnixPeerCredentials(0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new NativeIO.UnixPeerCredentials(0x8000_0000L, 0, 0));
        for (long invalid : List.of(-1L, 0xffff_ffffL, 0x1_0000_0000L)) {
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.UnixPeerCredentials(1, invalid, 0));
            assertThrows(IllegalArgumentException.class, () -> new NativeIO.UnixPeerCredentials(1, 0, invalid));
        }
    }

    @Test(timeout = 30000)
    public void actualOtherProcessPeerOrPlatformRejectionDoesNotConsumeOrCloseTheUnixChannel() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        Path socket = temporary.getRoot().toPath().resolve("peer.sock");
        Path error = temporary.getRoot().toPath().resolve("peer.err");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2", "-cp",
                System.getProperty("java.class.path"), PeerServer.class.getName(), socket.toString())
                .redirectError(error.toFile());
        builder.environment().clear();
        Process peer = builder.start();
        try {
            awaitPeerReady(peer);
            assertTrue(Files.readString(error), peer.isAlive());
            try (var channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                channel.connect(UnixDomainSocketAddress.of(socket));
                var io = new NativeIOImpl();
                if (SystemUtils.IS_OS_LINUX) {
                    var observed = io.unixPeerCredentials(channel);
                    assertEquals(peer.pid(), observed.pid());
                    var actual = io.currentProcessCredentials();
                    assertEquals(actual.effectiveUid(), observed.uid());
                    assertEquals(actual.effectiveGid(), observed.gid());
                    assertEquals(observed, io.unixPeerCredentials(channel));
                } else {
                    assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(channel));
                }
                assertTrue(channel.isOpen());
                ByteBuffer received = ByteBuffer.allocate(1);
                assertEquals(1, channel.read(received));
                assertEquals(3, received.array()[0]);
                assertEquals(1, channel.write(ByteBuffer.wrap(new byte[]{4})));
            }
            assertTrue(peer.waitFor(10, TimeUnit.SECONDS));
            assertEquals(Files.readString(error), 0, peer.exitValue());
        } finally {
            if (peer.isAlive()) {
                peer.destroyForcibly(); // Failed fixture cleanup only, never an authority/drain assertion.
                assertTrue(peer.waitFor(5, TimeUnit.SECONDS));
            }
            if (Files.exists(error)) {
                System.err.print(Files.readString(error));
            }
            peer.getOutputStream().close();
            peer.getInputStream().close();
            peer.getErrorStream().close();
        }
    }

    @Test
    public void tcpClosedAndNullInputsNeverProducePeerCredentials() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        var io = new NativeIOImpl();
        assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(null));
        try (var listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try (var client = SocketChannel.open(listener.getLocalAddress()); var accepted = listener.accept()) {
                assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(accepted));
                assertTrue(accepted.isOpen());
                assertEquals(1, client.write(ByteBuffer.wrap(new byte[]{7})));
                ByteBuffer received = ByteBuffer.allocate(1);
                assertEquals(1, accepted.read(received));
                assertEquals(7, received.array()[0]);
            }
        }
        try (var unconnected = SocketChannel.open(StandardProtocolFamily.UNIX)) {
            assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(unconnected));
            assertTrue(unconnected.isOpen());
        }
        var closed = SocketChannel.open(StandardProtocolFamily.UNIX);
        closed.close();
        assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(closed));
    }

    @Test(timeout = 60000)
    public void linuxCloseAndDescriptorReallocationCannotReturnAnotherProcesssCredentials() throws Exception {
        assumeTrue("Actual Linux close/reallocation behavior requires Linux", SystemUtils.IS_OS_LINUX);
        Path oldSocket = temporary.getRoot().toPath().resolve("old.sock");
        Path otherSocket = temporary.getRoot().toPath().resolve("other.sock");
        Process oldPeer = loopPeer(oldSocket, 33);
        Process otherPeer = loopPeer(otherSocket, 33);
        var queries = Executors.newSingleThreadExecutor();
        try {
            awaitPeerReady(oldPeer);
            awaitPeerReady(otherPeer);
            var io = new NativeIOImpl();
            for (int i = 0; i < 32; i++) {
                var original = SocketChannel.open(UnixDomainSocketAddress.of(oldSocket));
                var go = new CountDownLatch(1);
                var query = queries.submit(() -> {
                    go.await();
                    try {
                        return io.unixPeerCredentials(original);
                    } catch (NativeIOException closed) {
                        return null;
                    }
                });
                try {
                    go.countDown();
                    original.close();
                    // Reallocate a connected descriptor with a genuinely different peer PID while the query races.
                    try (var replacement = SocketChannel.open(UnixDomainSocketAddress.of(otherSocket))) {
                        assertEquals(otherPeer.pid(), io.unixPeerCredentials(replacement).pid());
                        var observed = query.get(5, TimeUnit.SECONDS);
                        if (observed != null) {
                            assertEquals(oldPeer.pid(), observed.pid());
                        }
                    }
                } finally {
                    go.countDown();
                    original.close();
                }
            }
            finishLoopPeer(oldSocket);
            finishLoopPeer(otherSocket);
            assertTrue(oldPeer.waitFor(10, TimeUnit.SECONDS));
            assertTrue(otherPeer.waitFor(10, TimeUnit.SECONDS));
            assertEquals(0, oldPeer.exitValue());
            assertEquals(0, otherPeer.exitValue());
        } finally {
            for (Process peer : List.of(oldPeer, otherPeer)) {
                if (peer.isAlive()) {
                    peer.destroyForcibly(); // Failed fixture cleanup, never a passing drain observation.
                    assertTrue(peer.waitFor(5, TimeUnit.SECONDS));
                }
                peer.getOutputStream().close();
                peer.getInputStream().close();
                peer.getErrorStream().close();
            }
            queries.shutdown();
            assertTrue(queries.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static Process loopPeer(Path socket, int connections) throws Exception {
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Xms32m", "-Xmx96m", "-XX:MaxDirectMemorySize=64m", "-XX:ActiveProcessorCount=2", "-cp",
                System.getProperty("java.class.path"), PeerServer.class.getName(), socket.toString(),
                Integer.toString(connections)).redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().clear();
        return builder.start();
    }

    private static void awaitPeerReady(Process peer) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (peer.getInputStream().available() == 0 && peer.isAlive() && System.nanoTime() - end < 0) {
            Thread.sleep(5);
        }
        assertTrue("Peer exited before its listener was ready", peer.isAlive());
        assertTrue("Peer did not report its completed bind", peer.getInputStream().available() > 0);
        assertEquals(1, peer.getInputStream().read());
    }

    private static void finishLoopPeer(Path socket) throws Exception {
        try (var channel = SocketChannel.open(UnixDomainSocketAddress.of(socket))) {
            assertEquals(1, channel.write(ByteBuffer.wrap(new byte[]{4})));
            assertEquals(1, channel.read(ByteBuffer.allocate(1)));
        }
    }

    public static final class PeerServer {
        public static void main(String[] arguments) throws Exception {
            Path socket = Path.of(arguments[0]);
            try (var listener = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
                listener.bind(UnixDomainSocketAddress.of(socket));
                System.out.write(1);
                System.out.flush();
                int connections = arguments.length > 1 ? Integer.parseInt(arguments[1]) : 1;
                for (int i = 0; i < connections; i++) {
                    try (var accepted = listener.accept()) {
                        if (arguments.length > 1) {
                            try {
                                accepted.write(ByteBuffer.wrap(new byte[]{3}));
                                accepted.read(ByteBuffer.allocate(1));
                            } catch (IOException raceClosed) {
                                // The race fixture intentionally closes its client before this accept body finishes.
                            }
                        } else {
                            assertEquals(1, accepted.write(ByteBuffer.wrap(new byte[]{3})));
                            ByteBuffer received = ByteBuffer.allocate(1);
                            assertEquals(1, accepted.read(received));
                            assertEquals(4, received.array()[0]);
                        }
                    }
                }
            } finally {
                Files.deleteIfExists(socket); // This private fixture created its only endpoint.
            }
        }
    }

    /** Run separately with the exact previous real library; the old calls still work and there is no fallback. */
    public static final class OldLibraryProbe {
        public static void main(String[] arguments) throws Exception {
            var io = new NativeIOImpl();
            assertNotNull(io.currentProcessCredentials());
            var unavailable = assertThrows(NativeIOException.class, () -> io.unixPeerCredentials(null));
            assertTrue(unavailable.getCause() instanceof UnsatisfiedLinkError);
            System.out.println("OLD_JNI_UNIX_PEER_UNSUPPORTED");
        }
    }
}

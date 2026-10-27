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
package org.apache.bookkeeper.tls;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.ReferenceCountedOpenSslContext;
import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import javax.security.auth.x500.X500Principal;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** The Profile TLS mode must not inherit Classic START_TLS or optional client authentication. */
public class ProfileTlsContextFactoryTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private static String resource(String name) throws Exception {
        return Paths.get(ProfileTlsContextFactoryTest.class.getClassLoader().getResource(name).toURI()).toString();
    }

    private static ServerConfiguration configuration() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setTLSKeyStoreType("PEM");
        conf.setTLSKeyStore(resource("server-key.pem"));
        conf.setTLSCertificatePath(resource("server-cert.pem"));
        conf.setTLSTrustStoreType("PEM");
        conf.setTLSTrustStore(resource("client-cert.pem"));
        conf.setTLSClientAuthentication(false);
        conf.setTLSEnabledProtocols("TLSv1.2");
        return conf;
    }

    private static String[] negotiatedProtocols(SslHandler handler) {
        // Some JDK engines report SSLv2Hello as a ClientHello compatibility marker, not a negotiated TLS version.
        return Arrays.stream(handler.engine().getEnabledProtocols())
                .filter(protocol -> !"SSLv2Hello".equals(protocol)).toArray(String[]::new);
    }

    private static void transfer(EmbeddedChannel source, EmbeddedChannel target) {
        ByteBuf message;
        while ((message = source.readOutbound()) != null) {
            target.writeInbound(message);
        }
    }

    private static SslContext context(TLSContextFactory factory) throws Exception {
        Field field = TLSContextFactory.class.getDeclaredField("sslContext");
        field.setAccessible(true);
        return (SslContext) field.get(factory);
    }

    private static ReferenceCountedOpenSslContext nativeContext(TLSContextFactory factory) throws Exception {
        assertTrue("Native TLS lifecycle coverage requires OpenSSL: " + OpenSsl.unavailabilityCause(),
                OpenSsl.isAvailable());
        SslContext context = context(factory);
        assertEquals("io.netty.handler.ssl.OpenSslServerContext", context.getClass().getName());
        return (ReferenceCountedOpenSslContext) context;
    }

    private static ReferenceCountedOpenSslEngine nativeEngine(SslHandler handler) {
        assertEquals("io.netty.handler.ssl.OpenSslEngine", handler.engine().getClass().getName());
        return (ReferenceCountedOpenSslEngine) handler.engine();
    }

    private static void releaseUndelivered(SslHandler handler) {
        if (handler != null && handler.engine() instanceof ReferenceCounted engine && engine.refCnt() > 0) {
            engine.release();
        }
    }

    private ServerConfiguration refreshConfiguration() throws Exception {
        ServerConfiguration conf = configuration();
        conf.setTLSProvider("OpenSSL");
        conf.setTLSCertFilesRefreshDurationSeconds(1);
        Path directory = temporary.newFolder().toPath();
        conf.setTLSKeyStore(Files.copy(Paths.get(resource("server-key.pem")),
                directory.resolve("key.pem")).toString());
        conf.setTLSCertificatePath(Files.copy(Paths.get(resource("server-cert.pem")),
                directory.resolve("cert.pem")).toString());
        conf.setTLSTrustStore(Files.copy(Paths.get(resource("client-cert.pem")),
                directory.resolve("trust.pem")).toString());
        return conf;
    }

    private static void forceCertificateRefresh(TLSContextFactory factory, ServerConfiguration conf) throws Exception {
        Path certificate = Paths.get(conf.getTLSCertificatePath());
        Files.setLastModifiedTime(certificate,
                FileTime.fromMillis(Files.getLastModifiedTime(certificate).toMillis() + 1000));
        Field field = TLSContextFactory.class.getDeclaredField("certLastRefreshTime");
        field.setAccessible(true);
        field.setLong(factory, 0);
    }

    private static void handshakeAndRelease(SslHandler handler) throws Exception {
        SslContext clientContext = SslContextBuilder.forClient()
                .sslProvider(SslProvider.JDK)
                .keyManager(new File(resource("client-cert.pem")), new File(resource("client-key.pem")))
                .trustManager(new File(resource("server-cert.pem")))
                .protocols("TLSv1.3").build();
        EmbeddedChannel server = new EmbeddedChannel(handler);
        SslHandler clientHandler = clientContext.newHandler(
                UnpooledByteBufAllocator.DEFAULT, "apache.bookkeeper.org", 0);
        EmbeddedChannel client = new EmbeddedChannel(clientHandler);
        try {
            for (int attempt = 0; attempt < 100 && (!handler.handshakeFuture().isDone()
                    || !clientHandler.handshakeFuture().isDone()); attempt++) {
                transfer(client, server);
                transfer(server, client);
                client.runPendingTasks();
                server.runPendingTasks();
            }
            assertTrue("Server handshake: " + handler.handshakeFuture().cause(),
                    handler.handshakeFuture().isSuccess());
            assertTrue("Client handshake: " + clientHandler.handshakeFuture().cause(),
                    clientHandler.handshakeFuture().isSuccess());
        } finally {
            client.finishAndReleaseAll();
            server.finishAndReleaseAll();
            ReferenceCountUtil.release(clientContext);
        }
    }

    @Test
    public void profileRequiresImmediateTls13AndClientCertificate() throws Exception {
        ServerConfiguration conf = configuration();
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        assertThrows(SecurityException.class, profile::newProfileServerTLSHandler);
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        SslHandler handler = profile.newProfileServerTLSHandler();
        assertTrue(handler.engine().getNeedClientAuth());
        assertArrayEquals(new String[] { "TLSv1.3" }, negotiatedProtocols(handler));

        EmbeddedChannel server = new EmbeddedChannel(handler);
        assertThrows(SecurityException.class, () -> profile.authenticatedProfilePrincipal(handler, "test-domain"));
        server.writeOutbound(Unpooled.copiedBuffer("secret", StandardCharsets.UTF_8));
        assertNull(server.readOutbound());

        SslContext clientContext = SslContextBuilder.forClient()
                .keyManager(new File(resource("client-cert.pem")), new File(resource("client-key.pem")))
                .trustManager(new File(resource("server-cert.pem")))
                .protocols("TLSv1.3").build();
        SslHandler clientHandler = clientContext.newHandler(
                UnpooledByteBufAllocator.DEFAULT, "apache.bookkeeper.org", 0);
        EmbeddedChannel client = new EmbeddedChannel(clientHandler);
        for (int attempt = 0; attempt < 100 && (!handler.handshakeFuture().isDone()
                || !clientHandler.handshakeFuture().isDone()); attempt++) {
            transfer(client, server);
            transfer(server, client);
            client.runPendingTasks();
            server.runPendingTasks();
        }
        assertTrue(handler.handshakeFuture().isSuccess());
        assertTrue(clientHandler.handshakeFuture().isSuccess());
        ProfilePrincipal principal = profile.authenticatedProfilePrincipal(handler, "test-domain");
        assertEquals(ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                principal.mappingMode());
        assertEquals("test-domain", principal.trustDomain());
        assertEquals(((java.security.cert.X509Certificate) handler.engine().getSession().getPeerCertificates()[0])
                .getSubjectX500Principal().getName(X500Principal.CANONICAL),
                principal.subjectCanonical());
        transfer(server, client);
        ByteBuf decrypted = client.readInbound();
        assertEquals("secret", decrypted.toString(StandardCharsets.UTF_8));
        decrypted.release();
        client.finishAndReleaseAll();
        server.finishAndReleaseAll();
        ReferenceCountUtil.release(clientContext);
        profile.closeProfileServer();

        conf.setTLSTrustStore(null);
        TLSContextFactory missingTrust = TLSContextFactory.forProfileServer();
        assertThrows(SecurityException.class,
                () -> missingTrust.init(SecurityHandlerFactory.NodeType.Server,
                        conf, UnpooledByteBufAllocator.DEFAULT));
        assertThrows(SecurityException.class,
                () -> TLSContextFactory.forProfileServer().init(SecurityHandlerFactory.NodeType.Client,
                        conf, UnpooledByteBufAllocator.DEFAULT));
        missingTrust.closeProfileServer();
    }

    @Test
    public void closeBeforeInitializationRejectsEveryProfileEntryPoint() throws Exception {
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.closeProfileServer();
        profile.closeProfileServer();
        assertThrows(SecurityException.class,
                () -> profile.init(SecurityHandlerFactory.NodeType.Server,
                        configuration(), UnpooledByteBufAllocator.DEFAULT));
        assertThrows(SecurityException.class, profile::newProfileServerTLSHandler);
        assertThrows(IllegalStateException.class, profile::newTLSHandler);
        assertThrows(IllegalStateException.class, () -> profile.newTLSHandler("peer", 3181));
        assertNull(context(profile));
    }

    @Test
    public void nativeProfileCloseReleasesOnlyFactoryReferenceAndRejectsReinitialization() throws Exception {
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        ServerConfiguration conf = configuration().setTLSProvider("OpenSSL");
        SslHandler handler = null;
        ReferenceCountedOpenSslContext context;
        try {
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            context = nativeContext(profile);
            assertEquals(1, context.refCnt());
            assertThrows(SecurityException.class,
                    () -> profile.init(SecurityHandlerFactory.NodeType.Server,
                            conf, UnpooledByteBufAllocator.DEFAULT));
            assertSame(context, context(profile));
            assertEquals(1, context.refCnt());
            handler = profile.newTLSHandler("peer", 3181);
            ReferenceCountedOpenSslEngine engine = nativeEngine(handler);
            assertEquals(2, context.refCnt());

            profile.closeProfileServer();
            profile.closeProfileServer();
            assertNull(context(profile));
            assertEquals(1, context.refCnt());
            assertEquals(1, engine.refCnt());
            assertThrows(SecurityException.class, profile::newProfileServerTLSHandler);
            assertThrows(IllegalStateException.class, profile::newTLSHandler);
            assertThrows(IllegalStateException.class, () -> profile.newTLSHandler("peer", 3181));
            assertThrows(SecurityException.class,
                    () -> profile.init(SecurityHandlerFactory.NodeType.Server,
                            conf, UnpooledByteBufAllocator.DEFAULT));
            releaseUndelivered(handler);
            assertEquals(0, engine.refCnt());
            assertEquals(0, context.refCnt());
        } finally {
            releaseUndelivered(handler);
            profile.closeProfileServer();
        }
    }

    @Test
    public void failedProfileInitializationLeavesNoContextAndAllowsRetry() throws Exception {
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        ServerConfiguration conf = configuration().setTLSProvider("OpenSSL");
        conf.setTLSTrustStore(null);
        try {
            assertThrows(SecurityException.class,
                    () -> profile.init(SecurityHandlerFactory.NodeType.Server,
                            conf, UnpooledByteBufAllocator.DEFAULT));
            assertNull(context(profile));
            assertThrows(SecurityException.class, profile::newProfileServerTLSHandler);
            assertThrows(IllegalStateException.class, profile::newTLSHandler);
            conf.setTLSTrustStore(resource("client-cert.pem"));
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            ReferenceCountedOpenSslContext context = nativeContext(profile);
            assertEquals(1, context.refCnt());
            profile.closeProfileServer();
            assertEquals(0, context.refCnt());
        } finally {
            profile.closeProfileServer();
        }
    }

    @Test
    public void nativeRefreshReleasesRetiredFactoryReferenceAndKeepsOldEngineUsable() throws Exception {
        ServerConfiguration conf = refreshConfiguration();
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        SslHandler oldHandler = null;
        SslHandler newHandler = null;
        try {
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            ReferenceCountedOpenSslContext retired = nativeContext(profile);
            oldHandler = profile.newProfileServerTLSHandler();
            assertEquals(2, retired.refCnt());
            forceCertificateRefresh(profile, conf);
            newHandler = profile.newProfileServerTLSHandler();
            ReferenceCountedOpenSslContext current = nativeContext(profile);
            assertNotSame(retired, current);
            assertEquals(1, retired.refCnt());
            assertEquals(2, current.refCnt());
            assertEquals(1, nativeEngine(oldHandler).refCnt());

            handshakeAndRelease(oldHandler);
            assertEquals(0, retired.refCnt());
            handshakeAndRelease(newHandler);
            assertEquals(1, current.refCnt());
            profile.closeProfileServer();
            assertEquals(0, current.refCnt());
        } finally {
            releaseUndelivered(oldHandler);
            releaseUndelivered(newHandler);
            profile.closeProfileServer();
        }
    }

    @Test
    public void failedNativeRefreshKeepsCurrentContextAndExistingEngineUsable() throws Exception {
        ServerConfiguration conf = refreshConfiguration();
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        SslHandler oldHandler = null;
        SslHandler fallbackHandler = null;
        try {
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            ReferenceCountedOpenSslContext current = nativeContext(profile);
            oldHandler = profile.newProfileServerTLSHandler();
            Files.writeString(Paths.get(conf.getTLSCertificatePath()), "invalid certificate", StandardCharsets.UTF_8);
            forceCertificateRefresh(profile, conf);
            fallbackHandler = profile.newProfileServerTLSHandler();
            assertSame(current, nativeContext(profile));
            assertEquals(3, current.refCnt());
            assertEquals(1, nativeEngine(oldHandler).refCnt());
            handshakeAndRelease(oldHandler);
            handshakeAndRelease(fallbackHandler);
            assertEquals(1, current.refCnt());
            profile.closeProfileServer();
            assertEquals(0, current.refCnt());
        } finally {
            releaseUndelivered(oldHandler);
            releaseUndelivered(fallbackHandler);
            profile.closeProfileServer();
        }
    }

    @Test
    public void invalidProfileCipherReleasesUndeliveredNativeEngines() throws Exception {
        ServerConfiguration conf = configuration().setTLSProvider("OpenSSL");
        conf.setTLSEnabledCipherSuites("not-a-valid-cipher");
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        try {
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            ReferenceCountedOpenSslContext context = nativeContext(profile);
            assertThrows(IllegalArgumentException.class, profile::newProfileServerTLSHandler);
            assertEquals(1, context.refCnt());
            assertThrows(IllegalArgumentException.class, () -> profile.newTLSHandler("peer", 3181));
            assertEquals(1, context.refCnt());
            profile.closeProfileServer();
            assertEquals(0, context.refCnt());
        } finally {
            profile.closeProfileServer();
        }
    }

    @Test
    public void profileHandlerCreationSerializesWithClose() throws Exception {
        assertHandlerCreationOwnsContextUntilHandoff(false);
    }

    @Test
    public void profileHandlerCreationSerializesWithRefresh() throws Exception {
        assertHandlerCreationOwnsContextUntilHandoff(true);
    }

    private void assertHandlerCreationOwnsContextUntilHandoff(boolean refresh) throws Exception {
        ServerConfiguration conf = refreshConfiguration();
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<SslHandler> creation = null;
        Future<SslHandler> competing = null;
        Lock contextWriteLock = null;
        SslHandler handler = null;
        SslHandler competingHandler = null;
        try {
            profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
            ReferenceCountedOpenSslContext original = nativeContext(profile);
            Field field = ReferenceCountedOpenSslContext.class.getDeclaredField("ctxLock");
            field.setAccessible(true);
            contextWriteLock = ((ReadWriteLock) field.get(original)).writeLock();
            contextWriteLock.lock();
            creation = executor.submit(profile::newProfileServerTLSHandler);
            // The real OPENSSL engine has retained its parent and is waiting to allocate the native SSL object.
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> original.refCnt() == 2);
            if (refresh) {
                forceCertificateRefresh(profile, conf);
            }
            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Thread> competingThread = new AtomicReference<>();
            competing = executor.submit(() -> {
                competingThread.set(Thread.currentThread());
                started.countDown();
                if (refresh) {
                    return profile.newProfileServerTLSHandler();
                }
                profile.closeProfileServer();
                return null;
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Future<SslHandler> competingOperation = competing;
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> competingOperation.isDone()
                    || competingThread.get().getState() == Thread.State.BLOCKED);
            assertFalse("Context ownership cannot change during handler creation", competing.isDone());
            assertEquals(Thread.State.BLOCKED, competingThread.get().getState());
            contextWriteLock.unlock();
            contextWriteLock = null;
            handler = creation.get(5, TimeUnit.SECONDS);
            competingHandler = competing.get(5, TimeUnit.SECONDS);
            assertEquals(1, nativeEngine(handler).refCnt());
            assertEquals(1, original.refCnt());
            if (refresh) {
                ReferenceCountedOpenSslContext replacement = nativeContext(profile);
                assertNotSame(original, replacement);
                assertEquals(2, replacement.refCnt());
                handshakeAndRelease(handler);
                handshakeAndRelease(competingHandler);
                assertEquals(0, original.refCnt());
                profile.closeProfileServer();
                assertEquals(0, replacement.refCnt());
            } else {
                assertNull(context(profile));
                releaseUndelivered(handler);
                assertEquals(0, original.refCnt());
            }
        } finally {
            if (contextWriteLock != null) {
                contextWriteLock.unlock();
            }
            try {
                if (creation != null && handler == null) {
                    handler = creation.get(5, TimeUnit.SECONDS);
                }
                if (competing != null && competingHandler == null) {
                    competingHandler = competing.get(5, TimeUnit.SECONDS);
                }
            } finally {
                releaseUndelivered(handler);
                releaseUndelivered(competingHandler);
                profile.closeProfileServer();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void classicFactoryKeepsConfiguredStartTlsBehavior() throws Exception {
        TLSContextFactory classic = new TLSContextFactory();
        classic.init(SecurityHandlerFactory.NodeType.Server, configuration(), UnpooledByteBufAllocator.DEFAULT);
        assertThrows(SecurityException.class, classic::newProfileServerTLSHandler);
        assertThrows(IllegalStateException.class, classic::closeProfileServer);
        SslHandler handler = classic.newTLSHandler();
        assertFalse(handler.engine().getNeedClientAuth());
        assertArrayEquals(new String[] { "TLSv1.2" }, negotiatedProtocols(handler));
        assertThrows(SecurityException.class,
                () -> classic.authenticatedProfilePrincipal(handler, "test-domain"));

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        assertTrue(channel.writeOutbound(Unpooled.copiedBuffer("classic", StandardCharsets.UTF_8)));
        ByteBuf outbound = channel.readOutbound();
        assertEquals("classic", outbound.toString(StandardCharsets.UTF_8));
        outbound.release();
        channel.finishAndReleaseAll();
        ReferenceCountUtil.release(context(classic));
    }
}

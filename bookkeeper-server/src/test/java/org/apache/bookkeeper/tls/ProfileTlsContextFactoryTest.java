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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import javax.security.auth.x500.X500Principal;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;

/** The Profile TLS mode must not inherit Classic START_TLS or optional client authentication. */
public class ProfileTlsContextFactoryTest {
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

    @Test
    public void profileRequiresImmediateTls13AndClientCertificate() throws Exception {
        ServerConfiguration conf = configuration();
        TLSContextFactory profile = TLSContextFactory.forProfileServer();
        profile.init(SecurityHandlerFactory.NodeType.Server, conf, UnpooledByteBufAllocator.DEFAULT);
        SslHandler handler = profile.newTLSHandler();
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

        conf.setTLSTrustStore(null);
        TLSContextFactory missingTrust = TLSContextFactory.forProfileServer();
        assertThrows(SecurityException.class,
                () -> missingTrust.init(SecurityHandlerFactory.NodeType.Server,
                        conf, UnpooledByteBufAllocator.DEFAULT));
        assertThrows(SecurityException.class,
                () -> TLSContextFactory.forProfileServer().init(SecurityHandlerFactory.NodeType.Client,
                        conf, UnpooledByteBufAllocator.DEFAULT));
    }

    @Test
    public void classicFactoryKeepsConfiguredStartTlsBehavior() throws Exception {
        TLSContextFactory classic = new TLSContextFactory();
        classic.init(SecurityHandlerFactory.NodeType.Server, configuration(), UnpooledByteBufAllocator.DEFAULT);
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
    }
}

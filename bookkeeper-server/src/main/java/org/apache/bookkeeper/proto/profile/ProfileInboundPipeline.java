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

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import java.util.Objects;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Installs the isolated Profile receive path after obtaining its pre-TLS connection slot. */
final class ProfileInboundPipeline {
    private static final long TLS_HANDSHAKE_TIMEOUT_MILLIS = 10_000;

    private ProfileInboundPipeline() {
    }

    static boolean install(ChannelPipeline pipeline, TLSContextFactory tls,
                           ProfileReceiveBudget budget, int connectionLimit, int connectionBaseBytes,
                           ChannelInboundHandler application) throws SecurityException {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(application, "application");
        ProfileReceiveBudget.ConnectionPermit permit = budget.admitUnclassified();
        if (permit == null) {
            pipeline.channel().close();
            return false;
        }
        try {
            ProfileReceiveDecoder decoder = new ProfileReceiveDecoder(
                    budget, permit, connectionLimit, connectionBaseBytes);
            SslHandler ssl = tls.newProfileServerTLSHandler();
            ssl.setHandshakeTimeoutMillis(TLS_HANDSHAKE_TIMEOUT_MILLIS);
            pipeline.addLast("profileTls", ssl);
            pipeline.addLast("profileReceive", decoder);
            pipeline.addLast("profileApplication", application);
            return true;
        } catch (SecurityException | RuntimeException | Error failure) {
            pipeline.channel().close();
            permit.close();
            throw failure;
        }
    }
}

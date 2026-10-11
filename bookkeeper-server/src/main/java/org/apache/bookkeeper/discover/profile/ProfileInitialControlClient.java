/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.discover.profile;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;

/** Existing experimental initial-control API backed by one bounded, authenticated CONTROL connection. */
public final class ProfileInitialControlClient implements ProfileInitialMetadataCoordinator.InstallClient,
        ProfileInitialLifecycleCoordinator.ActivationClient, AutoCloseable {
    private final ProfileSocketClient socket;

    private ProfileInitialControlClient(ProfileSocketClient socket) {
        this.socket = socket;
    }

    public static CompletableFuture<ProfileInitialControlClient> connect(EventLoopGroup group, SslContext tls,
            InetSocketAddress remote, String peerHostname, String expectedBookie, byte[] expectedIncarnation,
            int maxPending, Duration timeout) {
        CompletableFuture<ProfileInitialControlClient> result = new CompletableFuture<>();
        var pending = ProfileSocketClient.connect(group, tls, remote, peerHostname, expectedBookie,
                expectedIncarnation, maxPending, timeout, false, 0);
        pending.whenComplete((socket, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
            } else if (!result.complete(new ProfileInitialControlClient(socket))) {
                socket.close();
            }
        });
        result.whenComplete((client, failure) -> {
            if (result.isCancelled()) {
                pending.cancel(false);
            }
        });
        return result;
    }

    /** Registered cold observation -> existing mTLS/HELLO -> final cold identity reconciliation. */
    public static ProfileSocketClient.RegisteredConnection<ProfileInitialControlClient> connectRegistered(
            EventLoopGroup group, SslContext tls, ZKProfileRegistrationStore registry,
            ZKProfileRegistrationStore.RegisteredPeer peer, ThreadPoolExecutor cold,
            int maxPending, Duration timeout) {
        return ProfileSocketClient.connectRegistered(group, tls, registry, peer, cold,
                maxPending, timeout, false, 0, ProfileInitialControlClient::new);
    }

    public ProfileHello.Server hello() {
        return socket.hello();
    }

    @Override
    public CompletableFuture<ProfileControlResult> install(ProfileControlRequest request) {
        return socket.install(request);
    }

    @Override
    public CompletableFuture<ProfileControlResult> activate(ProfileControlRequest request) {
        return socket.activate(request);
    }

    /** Distinct UIC3 grant; body fields do not themselves authorize recovery payload. */
    public CompletableFuture<ProfileControlResult> recoveryGrant(ProfileControlRequest request) {
        return socket.recoveryGrant(request);
    }

    /** Independent UIC4 close; the target must cold-read exact committed close authority. */
    public CompletableFuture<ProfileControlResult> recoveryClose(ProfileControlRequest request) {
        return socket.recoveryClose(request);
    }

    /** Standalone selected-boundary close, including (-1, 0); never creates a DATA grant. */
    public CompletableFuture<ProfileControlResult> terminalClose(ProfileControlRequest request) {
        return socket.terminalClose(request);
    }

    /** Apply one committed delete delivery; continuous local progress is durable with its TOMBSTONE. */
    public CompletableFuture<ProfileControlResult> deleteApply(ProfileControlRequest request) {
        return socket.deleteApply(request);
    }

    /** Frozen fence body; caller retains the operation ID across transient/UNKNOWN retries. */
    public CompletableFuture<ProfileStatus> fence(ProfileLedgerContext context, byte[] operationId,
            ProtectedProfileCredential credential) {
        return socket.fence(context, operationId, credential);
    }

    public CompletableFuture<Void> closeAsync() {
        return socket.closeAsync();
    }

    @Override
    public void close() {
        socket.close();
    }
}

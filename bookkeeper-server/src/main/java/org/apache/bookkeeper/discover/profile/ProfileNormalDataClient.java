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
import io.netty.util.ReferenceCounted;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;

/** Actual frozen ADD_NORMAL transport; preserves the full status before any SDK failure handling. */
public final class ProfileNormalDataClient implements AutoCloseable {
    private final ProfileSocketClient socket;

    private ProfileNormalDataClient(ProfileSocketClient socket) {
        this.socket = socket;
    }

    public static CompletableFuture<ProfileNormalDataClient> connect(EventLoopGroup group, SslContext tls,
            InetSocketAddress remote, String peerHostname, String expectedBookie, byte[] expectedIncarnation,
            int maxPending, int maxEntryBytes, Duration timeout) {
        CompletableFuture<ProfileNormalDataClient> result = new CompletableFuture<>();
        var pending = ProfileSocketClient.connect(group, tls, remote, peerHostname, expectedBookie,
                expectedIncarnation, maxPending, timeout, true, maxEntryBytes);
        pending.whenComplete((socket, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
            } else if (!result.complete(new ProfileNormalDataClient(socket))) {
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
    public static ProfileSocketClient.RegisteredConnection<ProfileNormalDataClient> connectRegistered(
            EventLoopGroup group, SslContext tls, ZKProfileRegistrationStore registry,
            ZKProfileRegistrationStore.RegisteredPeer peer, ThreadPoolExecutor cold,
            int maxPending, int maxEntryBytes, Duration timeout) {
        return ProfileSocketClient.connectRegistered(group, tls, registry, peer, cold,
                maxPending, timeout, true, maxEntryBytes, ProfileNormalDataClient::new);
    }

    public ProfileHello.Server hello() {
        return socket.hello();
    }

    /** Result and actual local source/output terminal are separate; neither proves a remote abort. */
    public record AddAttempt(CompletableFuture<ProfileStatus> result, CompletableFuture<Void> completion) { }

    /** Borrow one ordinary Add, exposing its actual terminal without closing the shared DATA connection. */
    public AddAttempt beginNormalAdd(ProfileLedgerContext context, ProtectedProfileCredential credential,
            long entryId, ReferenceCounted entry) {
        return socket.beginNormalAdd(context, credential, entryId, entry);
    }

    /** Borrow CRC32C/v3 BookKeeper entry bytes. Caller may release but must not modify them until the future ends. */
    public CompletableFuture<ProfileStatus> addNormal(ProfileLedgerContext context,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry) {
        return socket.addNormal(context, credential, entryId, entry);
    }

    /** Borrow the original entry for the existing single-entry Profile recovery operation. */
    public CompletableFuture<ProfileStatus> addRecovery(ProfileRepairIntent intent,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry) {
        return socket.addRecovery(intent, credential, entryId, entry);
    }

    /** Explicit LAC result and actual source/output terminal, using the same bounded DATA socket. */
    public record LacWriteAttempt(CompletableFuture<ProfileStatus> result, CompletableFuture<Void> completion) { }

    public LacWriteAttempt beginWriteLac(ProfileLedgerContext context, ProtectedProfileCredential credential,
            long lac, org.apache.bookkeeper.util.ByteBufList digest) {
        return socket.beginWriteLac(context, credential, lac, digest);
    }

    /** Explicit point read; NOT_READY/UNKNOWN refusals are not missing evidence and never cause activation. */
    public CompletableFuture<ProfileReadEntryReplyCodec.Reply> readEntry(ProfileLedgerContext context, long entryId) {
        return socket.readEntry(context, entryId);
    }

    /** Return source provenance; sourceLength is not permission to close a prefix at piggybackLac. */
    public CompletableFuture<ProfileLacReplyCodec.Reply> readLac(ProfileLedgerContext context) {
        return socket.readLac(context);
    }

    public CompletableFuture<Void> closeAsync() {
        return socket.closeAsync();
    }

    public long backingBytes() {
        return socket.backingBytes();
    }

    public int backingLimitBytes() {
        return socket.backingLimitBytes();
    }

    public long peakBackingBytes() {
        return socket.peakBackingBytes();
    }

    public long codecWorkspaceBytes() {
        return socket.codecWorkspaceBytes();
    }

    @Override
    public void close() {
        socket.close();
    }
}

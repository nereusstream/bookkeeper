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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCounted;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileCredential;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;
import org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;

/** Actual frozen ADD_NORMAL transport; preserves the full status before any SDK failure handling. */
public final class ProfileNormalDataClient implements AutoCloseable {
    private final ProfileSocketClient socket;
    private final ProfileRpcScope scope;

    private ProfileNormalDataClient(ProfileSocketClient socket) {
        this(socket, null);
    }

    private ProfileNormalDataClient(ProfileSocketClient socket, ProfileRpcScope scope) {
        this.socket = socket;
        this.scope = scope;
    }

    /** One ledger's admission and actual RPC/response custody on this connection. */
    public ProfileNormalDataClient borrowScope() {
        if (scope != null) {
            throw new IllegalStateException("Borrow a DATA scope from its connection owner");
        }
        return new ProfileNormalDataClient(socket, new ProfileRpcScope());
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
        if (scope == null) {
            return socket.beginNormalAdd(context, credential, entryId, entry);
        }
        var attempt = scope.admit(() -> socket.beginNormalAdd(context, credential, entryId, entry),
                AddAttempt::completion);
        return new AddAttempt(attempt.result().copy(), attempt.completion().copy());
    }

    /** Borrow CRC32C/v3 BookKeeper entry bytes. Caller may release but must not modify them until the future ends. */
    public CompletableFuture<ProfileStatus> addNormal(ProfileLedgerContext context,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry) {
        return beginNormalAdd(context, credential, entryId, entry).result();
    }

    /** Borrow the original entry for the existing single-entry Profile recovery operation. */
    public CompletableFuture<ProfileStatus> addRecovery(ProfileRepairIntent intent,
            ProtectedProfileCredential credential, long entryId, ReferenceCounted entry) {
        return beginAddRecovery(intent, credential, entryId, entry).result();
    }

    /** Recovery result and original source/output/wire terminal, on the same bounded DATA socket. */
    public AddAttempt beginAddRecovery(ProfileRepairIntent intent, ProtectedProfileCredential credential,
            long entryId, ReferenceCounted entry) {
        if (scope == null) {
            return socket.beginAddRecovery(intent, credential, entryId, entry);
        }
        var attempt = scope.admit(() -> socket.beginAddRecovery(intent, credential, entryId, entry),
                AddAttempt::completion);
        return new AddAttempt(attempt.result().copy(), attempt.completion().copy());
    }

    /** Explicit LAC result and actual source/output terminal, using the same bounded DATA socket. */
    public record LacWriteAttempt(CompletableFuture<ProfileStatus> result, CompletableFuture<Void> completion) { }

    public LacWriteAttempt beginWriteLac(ProfileLedgerContext context, ProtectedProfileCredential credential,
            long lac, org.apache.bookkeeper.util.ByteBufList digest) {
        if (scope == null) {
            return socket.beginWriteLac(context, credential, lac, digest);
        }
        var attempt = scope.admit(() -> socket.beginWriteLac(context, credential, lac, digest),
                LacWriteAttempt::completion);
        return new LacWriteAttempt(attempt.result().copy(), attempt.completion().copy());
    }

    /** Explicit point read; NOT_READY/UNKNOWN refusals are not missing evidence and never cause activation. */
    public CompletableFuture<ProfileReadEntryReplyCodec.Reply> readEntry(ProfileLedgerContext context, long entryId) {
        if (scope == null) {
            return socket.readEntry(context, entryId);
        }
        return copiedResult(beginReadEntryBuffer(context, entryId).result(),
                response -> response.copyReply(context, entryId));
    }

    /** A reply's views have their own lifetime even when the decoder coalesces unrelated replies. */
    private static final class ResponseBacking extends CompositeByteBuf {
        private final CompletableFuture<Void> released = new CompletableFuture<>();

        private ResponseBacking(ByteBuf source) {
            super(source.alloc(), source.isDirect(), 1);
            addComponent(true, source.retainedSlice(source.readerIndex(), source.readableBytes()));
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            released.complete(null);
        }
    }

    /** Owns the accounted response backing; each retained entry view must also be released by its caller. */
    public static final class ReadEntryBuffer implements AutoCloseable {
        private final ProfileStatus status;
        private final ProfileReadEntryReplyCodec.Outcome outcome;
        private ByteBuf frame;
        private final int offset;
        private final int length;
        private final CompletableFuture<Void> released;

        ReadEntryBuffer(ProfileReadEntryReplyCodec.View decoded, ByteBuf frame, int offset) {
            status = decoded.status();
            outcome = decoded.outcome();
            this.offset = offset - frame.readerIndex();
            length = decoded.entry().remaining();
            if (frame.isReadable()) {
                var backing = new ResponseBacking(frame);
                this.frame = backing;
                released = backing.released;
            } else {
                this.frame = Unpooled.EMPTY_BUFFER;
                released = CompletableFuture.completedFuture(null);
            }
        }

        static ReadEntryBuffer refused(ProfileStatus status) {
            var reply = ProfileReadEntryReplyCodec.refused(status);
            return new ReadEntryBuffer(new ProfileReadEntryReplyCodec.View(reply.status(), reply.outcome(),
                    reply.entry()), Unpooled.EMPTY_BUFFER, 0);
        }

        public ProfileStatus status() {
            return status;
        }

        public ProfileReadEntryReplyCodec.Outcome outcome() {
            return outcome;
        }

        public synchronized ByteBuf entry() {
            if (frame == null) {
                throw new IllegalStateException("Read response has been released");
            }
            return frame.retainedSlice(offset, length).asReadOnly();
        }

        /** This response and all its retained views have released their share of the transport backing. */
        public CompletableFuture<Void> released() {
            return released.copy();
        }

        private synchronized ProfileReadEntryReplyCodec.Reply copyReply(ProfileLedgerContext context,
                long entryId) {
            if (outcome == ProfileReadEntryReplyCodec.Outcome.REFUSED) {
                return ProfileReadEntryReplyCodec.refused(status);
            }
            return ProfileReadEntryReplyCodec.decode(frame.nioBuffer(36, frame.readableBytes() - 36),
                    context, entryId, ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES);
        }

        @Override
        public synchronized void close() {
            if (frame != null) {
                frame.release();
                frame = null;
            }
        }
    }

    /** Reply ownership and the request's actual local wire/output terminal are separate. */
    public record ReadAttempt<T>(CompletableFuture<T> result, CompletableFuture<Void> completion) { }

    public ReadAttempt<ReadEntryBuffer> beginReadEntryBuffer(ProfileLedgerContext context, long entryId) {
        return scope == null ? socket.beginReadEntryBuffer(context, entryId)
                : scopedRead(() -> socket.beginReadEntryBuffer(context, entryId),
                        ReadEntryBuffer::released, ReadEntryBuffer::close);
    }

    public ReadAttempt<LacBuffer> beginReadLacBuffer(ProfileLedgerContext context) {
        return scope == null ? socket.beginReadLacBuffer(context)
                : scopedRead(() -> socket.beginReadLacBuffer(context), LacBuffer::released, LacBuffer::close);
    }

    private <T> ReadAttempt<T> scopedRead(Supplier<ReadAttempt<T>> read,
            Function<T, CompletableFuture<Void>> released, Consumer<T> close) {
        var payloadTerminal = new CompletableFuture<Void>();
        var callbackTerminal = new CompletableFuture<Void>();
        var original = scope.admit(read, attempt -> CompletableFuture.allOf(
                attempt.completion(), payloadTerminal, callbackTerminal));
        var result = new CompletableFuture<T>();
        original.result().whenComplete((response, failure) -> {
            try {
                if (failure != null) {
                    payloadTerminal.complete(null);
                    result.completeExceptionally(failure);
                } else {
                    released.apply(response).whenComplete((ignored, releaseFailure) -> {
                        if (releaseFailure == null) {
                            payloadTerminal.complete(null);
                        } else {
                            payloadTerminal.completeExceptionally(releaseFailure);
                        }
                    });
                    if (!result.complete(response)) {
                        close.accept(response);
                    }
                }
            } finally {
                callbackTerminal.complete(null);
            }
        });
        return new ReadAttempt<>(result, original.completion().copy());
    }

    private static <T extends AutoCloseable, R> CompletableFuture<R> copiedResult(
            CompletableFuture<T> original, Function<T, R> copy) {
        var result = new CompletableFuture<R>();
        original.whenComplete((response, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
            } else {
                try (response) {
                    result.complete(copy.apply(response));
                } catch (Exception | Error problem) {
                    result.completeExceptionally(problem);
                }
            }
        });
        return result;
    }

    /** The response and all retained entry views keep their original transport backing charged. */
    public CompletableFuture<ReadEntryBuffer> readEntryBuffer(ProfileLedgerContext context, long entryId) {
        return beginReadEntryBuffer(context, entryId).result();
    }

    /** Return source provenance; sourceLength is not permission to close a prefix at piggybackLac. */
    public CompletableFuture<ProfileLacReplyCodec.Reply> readLac(ProfileLedgerContext context) {
        if (scope == null) {
            return socket.readLac(context);
        }
        boolean explicit = hello().capabilities().contains(ProfileLacReplyCodec.EXPLICIT_CAPABILITY);
        return copiedResult(beginReadLacBuffer(context).result(), response -> response.copyReply(context, explicit));
    }

    /** Owns the accounted LAC response; callers must release each retained entry or explicit digest view. */
    public static final class LacBuffer implements AutoCloseable {
        private final ProfileStatus status;
        private final ProfileLacReplyCodec.Outcome outcome;
        private final long sourceEntryId;
        private final long piggybackLac;
        private final long sourceLength;
        private final long explicitLac;
        private final int offset;
        private final int entryBytes;
        private final int explicitBytes;
        private ByteBuf frame;
        private final CompletableFuture<Void> released;

        LacBuffer(ProfileLacReplyCodec.View decoded, ByteBuf frame, int offset) {
            status = decoded.status();
            outcome = decoded.outcome();
            sourceEntryId = decoded.sourceEntryId();
            piggybackLac = decoded.piggybackLac();
            sourceLength = decoded.sourceLength();
            explicitLac = decoded.explicitLac();
            entryBytes = decoded.entry().remaining();
            explicitBytes = decoded.explicitBody().remaining();
            this.offset = offset - frame.readerIndex();
            if (frame.isReadable()) {
                var backing = new ResponseBacking(frame);
                this.frame = backing;
                released = backing.released;
            } else {
                this.frame = Unpooled.EMPTY_BUFFER;
                released = CompletableFuture.completedFuture(null);
            }
        }

        static LacBuffer refused(ProfileStatus status) {
            var reply = ProfileLacReplyCodec.refused(status);
            return new LacBuffer(new ProfileLacReplyCodec.View(reply.status(), reply.outcome(), reply.entry(),
                    reply.explicitBody(), reply.explicitLac()), Unpooled.EMPTY_BUFFER, 0);
        }

        public ProfileStatus status() {
            return status;
        }

        public ProfileLacReplyCodec.Outcome outcome() {
            return outcome;
        }

        public long sourceEntryId() {
            return sourceEntryId;
        }

        public long piggybackLac() {
            return piggybackLac;
        }

        public long sourceLength() {
            return sourceLength;
        }

        public long explicitLac() {
            return explicitLac;
        }

        public boolean hasExplicitLac() {
            return explicitBytes != 0;
        }

        public ByteBuf entry() {
            return payload(offset, entryBytes);
        }

        public ByteBuf explicitBody() {
            return payload(offset + entryBytes, explicitBytes);
        }

        private synchronized ByteBuf payload(int start, int size) {
            if (frame == null) {
                throw new IllegalStateException("LAC response has been released");
            }
            return frame.retainedSlice(start, size).asReadOnly();
        }

        /** This response and both kinds of retained payload view have released their backing share. */
        public CompletableFuture<Void> released() {
            return released.copy();
        }

        private synchronized ProfileLacReplyCodec.Reply copyReply(ProfileLedgerContext context, boolean explicit) {
            if (outcome == ProfileLacReplyCodec.Outcome.REFUSED) {
                return ProfileLacReplyCodec.refused(status);
            }
            var payload = frame.nioBuffer(36, frame.readableBytes() - 36);
            return explicit ? ProfileLacReplyCodec.decodeExplicit(payload, context,
                    ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES)
                    : ProfileLacReplyCodec.decode(payload, context, ProfileReadEntryReplyCodec.MAX_ENTRY_BYTES);
        }

        @Override
        public synchronized void close() {
            if (frame != null) {
                frame.release();
                frame = null;
            }
        }
    }

    /** The response and its payload views keep the original transport backing charged. */
    public CompletableFuture<LacBuffer> readLacBuffer(ProfileLedgerContext context) {
        return beginReadLacBuffer(context).result();
    }

    public CompletableFuture<Void> closeAsync() {
        return scope == null ? socket.closeAsync() : scope.closeAsync();
    }

    public long backingBytes() {
        return socket.backingBytes();
    }

    /** Borrow the physical connection's allocator. The caller owns scratch lifetimes; closing that connection
     * waits for release, while an RPC child scope owns only its admitted requests and response views. */
    public ByteBufAllocator allocator() {
        return socket.allocator();
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
        closeAsync();
    }
}

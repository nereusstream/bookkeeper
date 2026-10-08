/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.wire.ProfileLacReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.profile.wire.ProfileReadEntryReplyCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileStatus;
import org.apache.bookkeeper.discover.profile.ProfileNormalDataClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Versioned;

/** Bounded source coverage and a matching boundary point read; never publishes writer state or CLOSED. */
final class ProfileLacEvidenceRead {
    private record Sample<T>(BookieId bookie, T reply, Throwable failure) { }
    private final LedgerHandle handle;
    private final ProfileBookieClient transport;
    private final CompletableFuture<ProfileLedgerHandle.LacEvidence> result = new CompletableFuture<>();
    private Versioned<LedgerMetadata> expected;
    private ProfileLedgerContext context;
    private Map<BookieId, ProfileNormalDataClient> peers;
    private long lac = -1;
    private long prefix;
    private final List<ProfileLedgerHandle.LacSource> sources = new ArrayList<>();
    private final List<ProfileLedgerHandle.ExplicitLacSource> explicitSources = new ArrayList<>();

    ProfileLacEvidenceRead(LedgerHandle handle, ProfileBookieClient transport) {
        this.handle = handle;
        this.transport = transport;
    }

    CompletableFuture<ProfileLedgerHandle.LacEvidence> start() {
        ordered(this::query);
        return result;
    }

    private void ordered(Runnable task) {
        try {
            handle.executeOrdered(() -> {
                try {
                    task.run();
                } catch (RuntimeException failure) {
                    result.completeExceptionally(failure);
                }
            });
        } catch (RejectedExecutionException shutdown) {
            result.completeExceptionally(BKException.create(BKException.Code.InterruptedException));
        }
    }

    private void query() {
        expected = handle.getVersionedLedgerMetadata();
        context = transport.ledgerContext();
        peers = transport.readPeers();
        LedgerMetadata metadata = expected.getValue();
        if (transport.isClosed()
                || metadata.getState() != (transport.isRecoveryReader()
                        ? LedgerMetadata.State.IN_RECOVERY : LedgerMetadata.State.OPEN)
                || metadata.getEnsembleSize() > 32 || metadata.getAllEnsembles().size() > 32) {
            throw error(BKException.Code.IllegalOpException);
        }
        List<BookieId> ensemble = metadata.getAllEnsembles().lastEntry().getValue();
        List<CompletableFuture<Sample<ProfileLacReplyCodec.Reply>>> pending = new ArrayList<>();
        for (BookieId bookie : ensemble) {
            ProfileNormalDataClient peer = peers.get(bookie);
            if (peer == null) {
                pending.add(CompletableFuture.completedFuture(new Sample<>(bookie, null,
                        error(BKException.Code.ReadException))));
            } else {
                try {
                    pending.add(peer.readLac(context)
                            .handle((reply, failure) -> new Sample<>(bookie, reply, failure)));
                } catch (RuntimeException refused) {
                    pending.add(CompletableFuture.completedFuture(new Sample<>(bookie, null, refused)));
                }
            }
        }
        CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).whenComplete((unused, failure) ->
                ordered(() -> collect(pending.stream().map(CompletableFuture::join).toList())));
    }

    private void collect(List<Sample<ProfileLacReplyCodec.Reply>> replies) {
        requireCurrent();
        var coverage = handle.distributionSchedule.getCoverageSet();
        int lastError = BKException.Code.ReadException;
        for (int i = 0; i < replies.size(); i++) {
            var sample = replies.get(i);
            int code = BKException.Code.ReadException;
            if (sample.failure == null && sample.reply != null) {
                var reply = sample.reply;
                if (reply.status().statusClass() == ProfileStatus.StatusClass.UNAUTHORIZED) {
                    throw error(BKException.Code.UnauthorizedAccessException);
                }
                long explicitValue = -1;
                boolean valid = true;
                if (reply.hasExplicitLac()) {
                    ByteBuf raw = Unpooled.wrappedBuffer(reply.explicitBody());
                    try {
                        long value = handle.macManager.verifyDigestAndReturnLac(raw);
                        if (value != reply.explicitLac()) {
                            throw error(BKException.Code.DigestMatchException);
                        }
                        explicitValue = value;
                    } catch (BKException.BKDigestMatchException invalid) {
                        valid = false;
                        lastError = BKException.Code.DigestMatchException;
                    } finally {
                        raw.release();
                    }
                }
                ProfileLedgerHandle.LacSource verifiedSource = null;
                if (valid && reply.outcome() == ProfileLacReplyCodec.Outcome.EMPTY) {
                    sources.add(source(sample.bookie, -1, -1, 0, 0));
                    code = BKException.Code.NoSuchEntryException;
                } else if (valid && reply.outcome() == ProfileLacReplyCodec.Outcome.EXPLICIT) {
                    code = BKException.Code.OK;
                } else if (valid && reply.outcome() == ProfileLacReplyCodec.Outcome.SOURCE) {
                    ByteBuf raw = Unpooled.wrappedBuffer(reply.entry());
                    try {
                        int payload = handle.macManager.verifyDigestAndReturnData(reply.sourceEntryId(), raw)
                                .readableBytes();
                        verifiedSource = source(sample.bookie, reply.sourceEntryId(), reply.piggybackLac(),
                                reply.sourceLength(), payload);
                        code = BKException.Code.OK;
                    } catch (BKException.BKDigestMatchException invalid) {
                        lastError = BKException.Code.DigestMatchException;
                    } finally {
                        raw.release();
                    }
                }
                if (code == BKException.Code.OK) {
                    if (verifiedSource != null) {
                        sources.add(verifiedSource);
                        lac = Math.max(lac, verifiedSource.piggybackLac());
                    }
                    if (reply.hasExplicitLac()) {
                        explicitSources.add(new ProfileLedgerHandle.ExplicitLacSource(sample.bookie,
                                HexFormat.of().formatHex(peers.get(sample.bookie).hello().storageIncarnation()),
                                explicitValue));
                        lac = Math.max(lac, explicitValue);
                    }
                }
            }
            // A source enters coverage only after scope/coordinate and the original digest are valid.
            coverage.addBookie(i, code);
        }
        if (!coverage.checkCovered()) {
            throw error(lastError);
        }
        prefix = Math.max(lac, expected.getValue().getAllEnsembles().lastKey() - 1);
        if (prefix < 0) {
            finish(0);
            return;
        }
        queryAnchor();
    }

    private ProfileLedgerHandle.LacSource source(BookieId bookie, long entry, long value, long length, int payload) {
        return new ProfileLedgerHandle.LacSource(bookie,
                HexFormat.of().formatHex(peers.get(bookie).hello().storageIncarnation()),
                entry, value, length, payload);
    }

    private void queryAnchor() {
        List<BookieId> ensemble = expected.getValue().getAllEnsembles().floorEntry(prefix).getValue();
        var writeSet = handle.distributionSchedule.getWriteSet(prefix);
        Map<BookieId, ProfileLedgerHandle.LacSource> anchors = new HashMap<>();
        List<CompletableFuture<Sample<ProfileReadEntryReplyCodec.Reply>>> pending = new ArrayList<>();
        try {
            for (int i = 0; i < writeSet.size(); i++) {
                BookieId bookie = ensemble.get(writeSet.get(i));
                var known = sources.stream().filter(source -> source.bookie().equals(bookie)
                        && source.sourceEntryId() == prefix).findFirst().orElse(null);
                if (known != null) {
                    anchors.put(bookie, known);
                } else if (peers.containsKey(bookie)) {
                    try {
                        pending.add(peers.get(bookie).readEntry(context, prefix)
                                .handle((reply, failure) -> new Sample<>(bookie, reply, failure)));
                    } catch (RuntimeException refused) {
                        pending.add(CompletableFuture.completedFuture(new Sample<>(bookie, null, refused)));
                    }
                }
            }
        } finally {
            writeSet.recycle();
        }
        CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).whenComplete((unused, failure) ->
                ordered(() -> collectAnchors(anchors, pending.stream().map(CompletableFuture::join).toList())));
    }

    private void collectAnchors(Map<BookieId, ProfileLedgerHandle.LacSource> anchors,
            List<Sample<ProfileReadEntryReplyCodec.Reply>> replies) {
        requireCurrent();
        for (var sample : replies) {
            if (sample.failure != null || sample.reply == null) {
                continue;
            }
            var reply = sample.reply;
            if (reply.status().statusClass() == ProfileStatus.StatusClass.UNAUTHORIZED) {
                throw error(BKException.Code.UnauthorizedAccessException);
            }
            if (reply.outcome() != ProfileReadEntryReplyCodec.Outcome.FOUND) {
                continue;
            }
            ByteBuf raw = Unpooled.wrappedBuffer(reply.entry());
            try {
                long hint = raw.getLong(16);
                long length = raw.getLong(24);
                int payload = handle.macManager.verifyDigestAndReturnData(prefix, raw).readableBytes();
                if (length < 0 || hint < -1) {
                    throw error(BKException.Code.DigestMatchException);
                }
                anchors.put(sample.bookie, source(sample.bookie, prefix, hint, length, payload));
            } catch (BKException.BKDigestMatchException invalid) {
                // A corrupt or uncertain reply is not an anchor or an absence vote.
            } finally {
                raw.release();
            }
        }
        if (anchors.isEmpty()) {
            throw error(BKException.Code.ReadException);
        }
        long length = anchors.values().iterator().next().sourceLength();
        for (var anchor : anchors.values()) {
            if (anchor.sourceLength() != length) {
                throw error(BKException.Code.DigestMatchException);
            }
        }
        for (var source : sources) {
            if (source.sourceEntryId() == prefix && source.sourceLength() != length
                    || prefix != Long.MAX_VALUE && source.sourceEntryId() == prefix + 1
                        && (length > Long.MAX_VALUE - source.payloadBytes()
                            || source.sourceLength() != length + source.payloadBytes())) {
                throw error(BKException.Code.DigestMatchException);
            }
        }
        finish(length);
    }

    private void finish(long length) {
        requireCurrent();
        result.complete(new ProfileLedgerHandle.LacEvidence(expected, lac, prefix, length, sources, explicitSources));
    }

    private void requireCurrent() {
        if (transport.isClosed() || handle.getVersionedLedgerMetadata() != expected
                || peers != transport.readPeers()) {
            throw error(BKException.Code.ReadException);
        }
    }

    private static RuntimeException error(int code) {
        return new java.util.concurrent.CompletionException(BKException.create(code));
    }
}

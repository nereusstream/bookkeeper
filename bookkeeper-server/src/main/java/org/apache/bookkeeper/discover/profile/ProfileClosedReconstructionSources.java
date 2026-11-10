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
package org.apache.bookkeeper.discover.profile;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.ClientContext;
import org.apache.bookkeeper.client.ProfileRecoveryReadHandle;
import org.apache.bookkeeper.common.profile.startup.ProfileRegistrationStore.ReadStatus;
import org.apache.bookkeeper.net.BookieId;

/** Borrows transport infrastructure and owns the two actual data-only registrations and their audit reader. */
public final class ProfileClosedReconstructionSources implements AutoCloseable {
    // Each retained peer has a readiness and an ephemeral projection, both bounded by the original codec.
    public static final int MAX_RETAINED_CAPABILITY_HINTS_PER_PEER =
            2 * ProfileReadinessCodec.MAX_BYTES / Integer.BYTES;

    private final ZKProfileRegistrationStore registry;
    private final BookieId offline;
    private final long deadline;
    private final Map<BookieId, ZKProfileRegistrationStore.RegisteredPeer> peers = new LinkedHashMap<>();
    private final List<ProfileSocketClient.RegisteredConnection<ProfileNormalDataClient>> connections =
            new ArrayList<>();
    private ProfileRecoveryReadHandle.ClosedSourceAudit audit;

    private ProfileClosedReconstructionSources(ZKProfileRegistrationStore registry, BookieId offline,
            long deadline) {
        this.registry = registry;
        this.offline = offline;
        this.deadline = deadline;
    }

    public static ProfileClosedReconstructionSources open(ClientContext sdk, EventLoopGroup group,
            SslContext tls, ZKProfileRegistrationStore registry, ThreadPoolExecutor cold,
            ZKProfileReservationStore.ReadOnlySnapshot snapshot, BookieId offline, int maxEntryBytes,
            long deadline) throws Exception {
        ProfileZKReadClient.requireTime(deadline);
        if (deadline - System.nanoTime() > Duration.ofMinutes(1).toNanos()) {
            throw new IllegalArgumentException("CLOSED source audit requires a bounded deadline");
        }
        for (var loop : group) {
            if (loop.inEventLoop()) {
                throw new IllegalStateException("CLOSED source audit cannot drain its own event loop");
            }
        }
        var value = snapshot.closed().metadata().metadata().getValue();
        var ensemble = value.getAllEnsembles().get(0L);
        if (ensemble == null || ensemble.size() != 3 || !ensemble.contains(offline)) {
            throw new IOException("CLOSED source audit requires the original offline ensemble member");
        }
        var owned = new ProfileClosedReconstructionSources(registry, offline, deadline);
        try {
            var data = new HashMap<BookieId, ProfileNormalDataClient>();
            for (BookieId bookie : ensemble) {
                if (bookie.equals(offline)) {
                    continue;
                }
                var found = registry.readPeer(bookie.toString(), deadline);
                if (found.status() != ReadStatus.FOUND) {
                    throw new IOException("Required CLOSED survivor registration is unavailable");
                }
                var peer = found.peer();
                owned.peers.put(bookie, peer);
                var remaining = Duration.ofNanos(Math.max(1, deadline - System.nanoTime()));
                var connection = ProfileNormalDataClient.connectRegistered(group, tls, registry, peer, cold, 2,
                        maxEntryBytes, remaining);
                owned.connections.add(connection);
                data.put(bookie, ProfileZKReadClient.await(connection.completion(), owned.deadline));
            }
            if (owned.peers.values().stream().map(peer -> peer.projection().storageIncarnation()).distinct()
                    .count() != 2) {
                throw new IOException("CLOSED source audit requires distinct survivor incarnations");
            }
            owned.audit = ProfileRecoveryReadHandle.fromClosedAudit(sdk, snapshot, data);
            owned.revalidate();
            return owned;
        } catch (Throwable failure) {
            try {
                owned.close();
            } catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    public String revalidate() throws IOException, TimeoutException {
        requireOffline(registry, offline, deadline);
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            for (var source : peers.entrySet()) {
                var peer = source.getValue();
                if (registry.revalidatePeer(peer, deadline).status() != ReadStatus.FOUND) {
                    throw new IOException("A CLOSED survivor registration changed during source audit");
                }
                out.writeUTF(source.getKey().toString());
                out.write(peer.projection().storageIncarnation().value());
                out.write(peer.reconstructionIdentity());
            }
        }
        ProfileZKReadClient.requireTime(deadline);
        return ProfileClosedReconstruction.digest(bytes.toByteArray());
    }

    public static void requireOffline(ZKProfileRegistrationStore registry, BookieId offline, long deadline)
            throws IOException, TimeoutException {
        ProfileZKReadClient.requireTime(deadline);
        if (registry.readPeer(offline.toString(), deadline).status() != ReadStatus.NOT_FOUND) {
            throw new IOException("Original CLOSED target must remain offline during source audit");
        }
        ProfileZKReadClient.requireTime(deadline);
    }

    public void verify(long entry, long cumulativeLength, int bytes, String sha256) throws Exception {
        var attempt = audit.probeBufferEvidence(entry);
        try (var evidence = ProfileZKReadClient.await(attempt, deadline)) {
            if (evidence.conflict() || evidence.sources().size() != 3 || evidence.candidate() == null) {
                throw new IOException("CLOSED survivor results have no consistent entry");
            }
            for (var source : evidence.sources()) {
                var expected = peers.get(source.bookie());
                if (expected == null ? !offline.equals(source.bookie())
                        || source.outcome() != ProfileRecoveryReadHandle.SourceOutcome.UNAVAILABLE
                        : source.outcome() != ProfileRecoveryReadHandle.SourceOutcome.FOUND
                            || !expected.projection().storageIncarnation().toString()
                                .equals(source.storageIncarnation())) {
                    throw new IOException("CLOSED entry lacks both exact digest-valid survivors");
                }
            }
            var candidate = evidence.candidate();
            var payload = candidate.payload();
            if (candidate.entryId() != entry || candidate.length() != cumulativeLength
                    || payload.remaining() != bytes) {
                throw new IOException("CLOSED survivor entry differs from the complete local image extent");
            }
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update(payload);
            if (!sha256.equals(HexFormat.of().formatHex(digest.digest()))) {
                throw new IOException("CLOSED survivor payload differs from the complete local image");
            }
        } catch (TimeoutException | InterruptedException failure) {
            if (!attempt.cancel(false)) {
                attempt.thenAccept(ProfileRecoveryReadHandle.PointEvidence::close);
            }
            throw failure;
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Override
    public void close() {
        try {
            if (audit != null) {
                audit.closeAsync().join();
            }
        } finally {
            CompletableFuture.allOf(connections.stream().map(connection -> connection.closeAsync())
                    .toArray(CompletableFuture[]::new)).join();
            connections.clear();
            audit = null;
        }
    }
}

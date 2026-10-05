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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.ProfileDescriptorValidator;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.IsolatedProfileControlEndpoint;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlAuthorizer;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlRequest;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.control.ProfileTransportContext;
import org.apache.bookkeeper.common.profile.control.ProtectedProfileStateStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

public class ZKProfileControlStoreTest extends ProfileZKTestBase {
    private ZKProfileControlStore store(TestClient client) {
        return new ZKProfileControlStore(client, ROOT, Duration.ofSeconds(5));
    }

    private static byte[] bytes(int size, int value) {
        byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static ProfileControlScope scope(ProfileControlOperation operation, long generation) {
        return scope(operation, generation, BOOKIE, "ledger-profile", 44, bytes(16, 1), null);
    }

    private static ProfileControlScope scope(ProfileControlOperation operation, long generation, String bookie,
            String domain, long ledger, byte[] instance, ProfileExactRange range) {
        byte[] descriptor = ByteBuffer.allocate(36).putShort((short) 1).putShort((short) 1)
                .put(bytes(32, 9)).array();
        return new ProfileControlScope(operation, operation.purpose(), ledger, instance,
                ProfileDescriptorIdentity.fromBytes(descriptor), bookie, bytes(16, 2), domain, generation,
                bytes(16, 3), bytes(32, 4), range);
    }

    private static CommittedProfileAuthority authority(ProfileControlScope scope) {
        CommittedProfileAuthority.State state = switch (scope.operation()) {
            case INSTALL -> CommittedProfileAuthority.State.PREPARING;
            case ACTIVATE_INITIAL -> CommittedProfileAuthority.State.READY_INITIAL;
            case ACTIVATE_REPLACEMENT -> CommittedProfileAuthority.State.POST_MEMBERSHIP_REPLACEMENT;
            case RECOVERY_GRANT -> CommittedProfileAuthority.State.RECOVERY_OPEN;
            case RECOVERY_GRANT_CLOSE -> CommittedProfileAuthority.State.RECOVERY_CLOSE_AUTHORIZED;
            case TOMBSTONE_OR_DELETE_APPLY -> CommittedProfileAuthority.State.DELETE_AUTHORIZED;
            case STATUS_QUERY -> CommittedProfileAuthority.State.STATUS_QUERYABLE;
        };
        return new CommittedProfileAuthority(scope, state, EngineProfile.SEGMENT_WAL_ENGINE, 123, true);
    }

    private String create(ProfileControlScope scope) throws Exception {
        String path = store(first).path(ProfileAuthorityKey.fromScope(scope));
        first.create(path, ProfileAuthorityCodec.encode(authority(scope)), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        return path;
    }

    @Test
    public void fixedAuthorityVectorUsesExternalStatVersionRatherThanEnvelopeVersion() throws Exception {
        // Literal big-endian UWAU/schema-1 record and independently computed CRC32C, not encoder output.
        byte[] vector = HexFormat.of().parseHex(
                "5557415500000001000000b60000000100000001000000010000000100000003000000000000002c01010101"
                + "0101010101010101010101010001000109090909090909090909090909090909090909090909090909090909"
                + "0909090902020202020202020202020202020202000000000000000803030303030303030303030303030303"
                + "0404040404040404040404040404040404040404040404040404040404040404000000016200000001640000"
                + "0000731b2114");
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8, "b", "d", 44, bytes(16, 1), null);
        assertArrayEquals(vector, ProfileAuthorityCodec.encode(authority(scope)));
        assertEquals(new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                EngineProfile.SEGMENT_WAL_ENGINE, 456, true), ProfileAuthorityCodec.decode(vector, 456));
        String path = store(first).path(ProfileAuthorityKey.fromScope(scope));
        first.create(path, vector, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        var read = store(second).read(ProfileAuthorityKey.fromScope(scope));
        assertEquals(scope, read.authority().scope());
        assertEquals(second.exists(path, false).getMzxid(), read.authority().opaqueStoreVersion());
    }

    @Test
    public void allPurposeFieldsReadFromRealPersistentNodesWithActualStoreVersion() throws Exception {
        for (ProfileControlOperation operation : ProfileControlOperation.values()) {
            ProfileExactRange range = operation.rangeAllowed() ? new ProfileExactRange(5, Long.MAX_VALUE) : null;
            ProfileControlScope scope = scope(operation, 8, BOOKIE, "ledger-profile", 44, bytes(16, 1), range);
            String path = create(scope);
            var result = store(second).read(ProfileAuthorityKey.fromScope(scope));
            assertEquals(ReadStatus.FOUND, result.status());
            assertEquals(scope, result.authority().scope());
            assertEquals(authority(scope).state(), result.authority().state());
            assertEquals(EngineProfile.SEGMENT_WAL_ENGINE, result.authority().requiredEngine());
            assertEquals(second.exists(path, false).getMzxid(), result.authority().opaqueStoreVersion());
            assertTrue(result.authority().complete());
            assertTrue(result.authority().permits(operation));
        }
    }

    @Test
    public void laterReadObservesUpdateAndRecreationWithoutUsingDataVersionAsIdentity() throws Exception {
        ProfileControlScope initial = scope(ProfileControlOperation.INSTALL, 8);
        ProfileAuthorityKey key = ProfileAuthorityKey.fromScope(initial);
        String path = create(initial);
        ZKProfileControlStore store = store(second);
        long oldVersion = store.read(key).authority().opaqueStoreVersion();
        ProfileControlScope next = scope(ProfileControlOperation.INSTALL, 9);
        first.setData(path, ProfileAuthorityCodec.encode(authority(next)), 0);
        var result = store.read(key);
        assertEquals(next, result.authority().scope());
        assertTrue(result.authority().opaqueStoreVersion() > oldVersion);
        first.delete(path, 1);
        assertEquals(ReadStatus.NOT_FOUND, store.read(key).status());
        first.create(path, ProfileAuthorityCodec.encode(authority(initial)), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertEquals(0, second.exists(path, false).getVersion());
        assertTrue(store.read(key).authority().opaqueStoreVersion() > result.authority().opaqueStoreVersion());
    }

    @Test
    public void keyIsDerivedAndSeparatesLedgerInstancePurposeAndCanonicalDomain() throws Exception {
        ProfileControlScope original = scope(ProfileControlOperation.INSTALL, 8);
        String path = create(original);
        ZKProfileControlStore store = store(second);
        for (ProfileControlScope foreign : new ProfileControlScope[] {
                scope(ProfileControlOperation.INSTALL, 8, BOOKIE, "ledger-profile", 45, bytes(16, 1), null),
                scope(ProfileControlOperation.INSTALL, 8, BOOKIE, "ledger-profile", 44, bytes(16, 7), null),
                scope(ProfileControlOperation.ACTIVATE_INITIAL, 8),
                scope(ProfileControlOperation.INSTALL, 8, BOOKIE, "../控制/:", 44, bytes(16, 1), null)}) {
            assertEquals(ReadStatus.NOT_FOUND, store.read(ProfileAuthorityKey.fromScope(foreign)).status());
            first.setData(path, ProfileAuthorityCodec.encode(authority(foreign)), -1);
            assertEquals(ReadStatus.QUARANTINED, store.read(ProfileAuthorityKey.fromScope(original)).status());
        }
    }

    @Test
    public void incompleteWrongStateAndEphemeralRecordsNeverBecomeCommittedAuthority() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8);
        String path = create(scope);
        ProfileAuthorityKey key = ProfileAuthorityKey.fromScope(scope);
        for (CommittedProfileAuthority invalid : new CommittedProfileAuthority[] {
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 0, false),
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.READY_INITIAL,
                        EngineProfile.SEGMENT_WAL_ENGINE, 0, true)}) {
            first.setData(path, ProfileAuthorityCodec.encode(invalid), -1);
            assertEquals(ReadStatus.QUARANTINED, store(second).read(key).status());
        }
        first.delete(path, -1);
        first.create(path, ProfileAuthorityCodec.encode(authority(scope)), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        assertEquals(ReadStatus.QUARANTINED, store(second).read(key).status());
    }

    @Test
    public void corruptDiscriminatorsLengthsIdentitiesAndRangesAreQuarantined() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8);
        String path = create(scope);
        byte[] good = ProfileAuthorityCodec.encode(authority(scope));
        for (int field : new int[] {0, 4, 8, 12, 16, 20, 24, 28, 32, 40, 56, 92, 108, 116, 164}) {
            byte[] bad = good.clone();
            if (field == 40 || field == 92 || field == 116) {
                Arrays.fill(bad, field, field + 16, (byte) 0);
            } else if (field == 32 || field == 108) {
                ByteBuffer.wrap(bad).putLong(field, -1);
            } else {
                ByteBuffer.wrap(bad).putInt(field, 99);
            }
            checksum(bad);
            first.setData(path, bad, -1);
            assertEquals("field " + field, ReadStatus.QUARANTINED,
                    store(second).read(ProfileAuthorityKey.fromScope(scope)).status());
        }
        for (byte[] bad : new byte[][] {Arrays.copyOf(good, good.length - 1),
                Arrays.copyOf(good, good.length + 1), new byte[ProfileAuthorityCodec.MAX_BYTES + 1]}) {
            first.setData(path, bad, -1);
            assertEquals(ReadStatus.QUARANTINED, store(second).read(ProfileAuthorityKey.fromScope(scope)).status());
        }
        byte[] bad = good.clone();
        bad[bad.length - 1] ^= 1;
        first.setData(path, bad, -1);
        assertEquals(ReadStatus.QUARANTINED, store(second).read(ProfileAuthorityKey.fromScope(scope)).status());
        ProfileControlScope recovery = scope(ProfileControlOperation.RECOVERY_GRANT, 8, BOOKIE, "ledger-profile",
                44, bytes(16, 1), new ProfileExactRange(5, 6));
        bad = ProfileAuthorityCodec.encode(authority(recovery));
        ByteBuffer.wrap(bad).putLong(bad.length - 12, 4);
        checksum(bad);
        assertThrows(IllegalArgumentException.class, () -> ProfileAuthorityCodec.decode(
                ProfileAuthorityCodec.encode(new CommittedProfileAuthority(scope,
                        CommittedProfileAuthority.State.PREPARING, EngineProfile.CLASSIC_ENGINE, 0, true)), -1));
        byte[] invalidRange = bad;
        assertThrows(IllegalArgumentException.class, () -> ProfileAuthorityCodec.decode(invalidRange, 0));
    }

    @Test
    public void maximumCanonicalTextAndOptionalRangeHaveBoundedEnvelope() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.RECOVERY_GRANT, 8, "界".repeat(85),
                "界".repeat(42) + "AB", Long.MAX_VALUE, bytes(16, 1), new ProfileExactRange(0, Long.MAX_VALUE));
        byte[] encoded = ProfileAuthorityCodec.encode(authority(scope));
        assertEquals(579, encoded.length);
        create(scope);
        assertEquals(scope, store(second).read(ProfileAuthorityKey.fromScope(scope)).authority().scope());
        ProfileControlScope invalid = scope(ProfileControlOperation.INSTALL, 8, BOOKIE, "\ud800", 44,
                bytes(16, 1), null);
        assertThrows(IllegalArgumentException.class, () -> ProfileAuthorityCodec.encode(authority(invalid)));
        assertEquals(ReadStatus.QUARANTINED, store(second).read(ProfileAuthorityKey.fromScope(invalid)).status());
    }

    @Test
    public void realAclRejectionAndClosedOrExpiredSessionAreUnavailableRatherThanAbsent() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8);
        String path = create(scope);
        first.addAuthInfo("digest", "control-reader:opaque-test".getBytes(StandardCharsets.UTF_8));
        first.setACL(path, ZooDefs.Ids.CREATOR_ALL_ACL, -1);
        ProfileAuthorityKey key = ProfileAuthorityKey.fromScope(scope);
        assertEquals(ReadStatus.FOUND, store(first).read(key).status());
        assertEquals(ReadStatus.UNAVAILABLE, store(second).read(key).status());
        second.close();
        assertEquals(ReadStatus.UNAVAILABLE, store(second).read(key).status());
        cluster.expire(first.getSessionId());
        assertTrue(first.expired.await(5, TimeUnit.SECONDS));
        assertEquals(ReadStatus.UNAVAILABLE, store(first).read(key).status());
    }

    @Test
    public void actualReadWithHeldCallbackTimesOutAndLaterReadIsFresh() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8);
        create(scope);
        ProfileAuthorityKey key = ProfileAuthorityKey.fromScope(scope);
        first.holdReadReply.set(true);
        ZKProfileControlStore bounded = new ZKProfileControlStore(first, ROOT, Duration.ofMillis(100));
        var pending = CompletableFuture.supplyAsync(() -> bounded.read(key));
        assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
        assertEquals(ReadStatus.UNAVAILABLE, pending.get(5, TimeUnit.SECONDS).status());
        assertEquals(1, first.reads.get());
        first.lateReadReply.getAndSet(null).run();
        assertEquals(ReadStatus.FOUND, store(first).read(key).status());
        assertEquals(2, first.reads.get());
    }

    @Test
    public void interruptIsPreservedWithoutAFoundOrAbsentResult() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.INSTALL, 8);
        create(scope);
        Thread.currentThread().interrupt();
        try {
            assertEquals(ReadStatus.UNAVAILABLE, store(second).read(ProfileAuthorityKey.fromScope(scope)).status());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(ReadStatus.FOUND, store(second).read(ProfileAuthorityKey.fromScope(scope)).status());
    }

    @Test
    public void delayedSnapshotKeepsItsOwnPayloadAndStoreVersionAcrossConcurrentUpdate() throws Exception {
        ProfileControlScope initial = scope(ProfileControlOperation.INSTALL, 8);
        String path = create(initial);
        long version = second.exists(path, false).getMzxid();
        first.holdReadReply.set(true);
        var pending = CompletableFuture.supplyAsync(() -> store(first).read(ProfileAuthorityKey.fromScope(initial)));
        assertTrue(first.readReplyHeld.await(5, TimeUnit.SECONDS));
        ProfileControlScope next = scope(ProfileControlOperation.INSTALL, 9);
        second.setData(path, ProfileAuthorityCodec.encode(authority(next)), -1);
        first.lateReadReply.getAndSet(null).run();
        var captured = pending.get(5, TimeUnit.SECONDS);
        assertEquals(initial, captured.authority().scope());
        assertEquals(version, captured.authority().opaqueStoreVersion());
        var fresh = store(first).read(ProfileAuthorityKey.fromScope(initial));
        assertEquals(next, fresh.authority().scope());
        assertTrue(fresh.authority().opaqueStoreVersion() > version);
    }

    @Test
    public void referenceEndpointUsesRealAuthorityButStillRequiresExactAuthorizationAndScope() throws Exception {
        ProfileControlScope scope = scope(ProfileControlOperation.STATUS_QUERY, 8);
        String path = create(scope);
        AtomicInteger queries = new AtomicInteger();
        ProtectedProfileStateStore local = new ProtectedProfileStateStore() {
            @Override
            public TransitionResult conditionalApply(Transition transition) {
                throw new AssertionError("STATUS_QUERY must not apply a durable effect");
            }

            @Override
            public TransitionResult queryOperationResult(ProfileControlScope target) {
                queries.incrementAndGet();
                return new TransitionResult(ProfileControlResult.Outcome.DURABILITY_UNKNOWN, 0, false);
            }
        };
        var principal = new ProfilePrincipal(ProfilePrincipal.MappingMode.TRUST_DOMAIN_ISSUER_PLUS_SUBJECT,
                "operators", "cn=test-ca", "cn=profile-controller");
        var transport = new ProfileTransportContext("bookie-profile", "TLSv1.3", true, true, true, principal);
        ProfileControlRequest request = ProfileControlRequest.operation(scope);
        ZKProfileControlStore store = store(first);
        var denied = endpoint(store, (p, s, a) -> ProfileControlAuthorizer.Decision.DENY, local);
        assertEquals(ProfileControlResult.Outcome.DENIED, denied.handle(transport, request).outcome());
        assertEquals(0, queries.get());
        var allowed = endpoint(store, (p, s, a) -> ProfileControlAuthorizer.Decision.ALLOW, local);
        int reads = first.reads.get();
        assertEquals(ProfileControlResult.Outcome.DURABILITY_UNKNOWN, allowed.handle(transport, request).outcome());
        assertEquals(reads + 1, first.reads.get());
        assertEquals(1, queries.get());
        ProfileControlScope changed = scope(ProfileControlOperation.STATUS_QUERY, 9);
        first.setData(path, ProfileAuthorityCodec.encode(authority(changed)), -1);
        assertEquals(ProfileControlResult.Outcome.CONFLICT, allowed.handle(transport, request).outcome());
        first.setData(path, new byte[0], -1);
        assertEquals(ProfileControlResult.Outcome.QUARANTINED, allowed.handle(transport, request).outcome());
        assertEquals(1, queries.get());
    }

    private static IsolatedProfileControlEndpoint endpoint(ZKProfileControlStore store,
            ProfileControlAuthorizer authorizer, ProtectedProfileStateStore local) {
        return new IsolatedProfileControlEndpoint(EngineProfile.SEGMENT_WAL_ENGINE,
                new ProfileDescriptorValidator((id, version) -> true, (id, generation) -> true),
                (principal, operation) -> true, store, authorizer, local);
    }

    private static void checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }
}

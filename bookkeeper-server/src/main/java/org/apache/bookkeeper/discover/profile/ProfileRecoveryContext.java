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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.LedgerMetadataUtils;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.versioning.LongVersion;

/** One fixed experimental same-record recovery identity. It is not a fence/grant or CLOSED receipt. */
public record ProfileRecoveryContext(ProfileLedgerContext ledger, byte[] operationId, long metadataBirth,
        int predecessorVersion, long creatorToken, byte[] predecessorDigest, long lifecycleBirthZxid,
        UUID lifecycleBirth, long lifecycleSequence, int lifecycleVersion, long controlGeneration) {
    public static final String KEY = "bookkeeper.experimental.profile.recovery-context";
    public static final int ENCODED_BYTES = 184;
    private static final int MAGIC = 0x55575243; // UWRC

    public ProfileRecoveryContext {
        java.util.Objects.requireNonNull(ledger, "ledger");
        java.util.Objects.requireNonNull(lifecycleBirth, "lifecycleBirth");
        operationId = ProfileLifecycleHead.fixed(operationId, 16, true);
        predecessorDigest = ProfileLifecycleHead.fixed(predecessorDigest, 32, false);
        if (metadataBirth <= 0 || predecessorVersion < 0 || predecessorVersion == Integer.MAX_VALUE
                || creatorToken < 0 || lifecycleBirthZxid <= 0 || lifecycleBirth.equals(new UUID(0, 0))
                || lifecycleSequence < 2 || lifecycleSequence > 2 + ProfileReplacementPage.MAX_PUBLICATIONS
                || lifecycleVersion != lifecycleSequence - 1 || controlGeneration <= 0) {
            throw new IllegalArgumentException("Invalid frozen Profile recovery identity");
        }
    }

    static ProfileRecoveryContext capture(ProfileLedgerMetadataStore.MembershipSnapshot metadata,
            ZKProfileReservationStore.Snapshot root, byte[] operation) throws IOException {
        var value = metadata.metadata().getValue();
        var reference = ProfileMetadataBacklink.from(value);
        if (!reference.equals(root.reservation().backlink())
                || !(metadata.metadata().getVersion() instanceof LongVersion version)) {
            throw new IllegalArgumentException("Recovery scope does not match its metadata and lifecycle");
        }
        var scope = root.reservation().installs().get(0);
        return new ProfileRecoveryContext(new ProfileLedgerContext(reference.ledgerId(), reference.instanceId(),
                scope.descriptorIdentity().toBytes()), operation, metadata.birthZxid(),
                Math.toIntExact(version.getLongVersion()), value.getCToken(), metadataDigest(value),
                root.birthZxid(), root.lifecycle().birthId(), root.lifecycle().sequence(),
                root.lifecycleDataVersion(), root.lifecycle().controlGeneration());
    }

    @Override
    public byte[] operationId() {
        return operationId.clone();
    }

    @Override
    public byte[] predecessorDigest() {
        return predecessorDigest.clone();
    }

    public long generation() {
        return (long) predecessorVersion + 1;
    }

    public byte[] encode() {
        ByteBuffer out = ByteBuffer.allocate(ENCODED_BYTES).putInt(MAGIC).putShort((short) 1).putShort((short) 0)
                .put(ledger.encode()).put(operationId).putLong(metadataBirth).putInt(predecessorVersion)
                .putLong(creatorToken).put(predecessorDigest).putLong(lifecycleBirthZxid)
                .putLong(lifecycleBirth.getMostSignificantBits()).putLong(lifecycleBirth.getLeastSignificantBits())
                .putLong(lifecycleSequence).putInt(lifecycleVersion).putLong(controlGeneration);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, ENCODED_BYTES - 4);
        return out.putInt((int) crc.getValue()).array();
    }

    public static ProfileRecoveryContext decode(byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_BYTES) {
            throw new IllegalArgumentException("Recovery context must have exactly 184 bytes");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC
                || in.getShort() != 1 || in.getShort() != 0) {
            throw new IllegalArgumentException("Invalid recovery context schema or checksum");
        }
        var ledger = ProfileLedgerContext.decode(in);
        byte[] operation = new byte[16];
        in.get(operation);
        long birth = in.getLong();
        int version = in.getInt();
        long creator = in.getLong();
        byte[] digest = new byte[32];
        in.get(digest);
        return new ProfileRecoveryContext(ledger, operation, birth, version, creator, digest,
                in.getLong(), new UUID(in.getLong(), in.getLong()), in.getLong(), in.getInt(), in.getLong());
    }

    boolean matchesRoot(ZKProfileReservationStore.Snapshot root) {
        return root.birthZxid() == lifecycleBirthZxid && root.lifecycle().birthId().equals(lifecycleBirth)
                && root.lifecycleDataVersion() == lifecycleVersion && root.lifecycle().sequence() == lifecycleSequence
                && root.lifecycle().controlGeneration() == controlGeneration
                && (root.lifecycle().state() == ProfileLifecycleHead.State.READY
                    || root.lifecycle().state() == ProfileLifecycleHead.State.REPLACEMENT)
                && root.reservation().backlink().ledgerId() == ledger.ledgerId()
                && Arrays.equals(root.reservation().backlink().instanceId(), ledger.ledgerInstanceId())
                && Arrays.equals(root.reservation().installs().get(0).descriptorIdentity().toBytes(),
                        ledger.descriptorIdentity());
    }

    boolean matchesRecovery(ProfileLedgerMetadataStore.MembershipSnapshot current) throws IOException {
        var value = current.metadata().getValue();
        return value.getState() == LedgerMetadata.State.IN_RECOVERY && current.birthZxid() == metadataBirth
                && current.metadata().getVersion() instanceof LongVersion version
                && version.getLongVersion() == generation() && value.getCToken() == creatorToken
                && Arrays.equals(value.getCustomMetadata().get(KEY), encode())
                && Arrays.equals(metadataDigest(value), predecessorDigest);
    }

    /** Canonical OPEN projection is only hashed; it is never published or used to reopen a ledger. */
    static LedgerMetadata predecessorProjection(LedgerMetadata value) {
        var custom = new TreeMap<String, byte[]>();
        value.getCustomMetadata().forEach((key, bytes) -> {
            if (!key.equals(KEY) && !key.equals(ProfileRepairCommit.KEY) && !key.equals(ProfileRecoveryBoundary.KEY)) {
                custom.put(key, bytes.clone());
            }
        });
        var builder = LedgerMetadataBuilder.create().withId(value.getLedgerId())
                .withMetadataFormatVersion(value.getMetadataFormatVersion()).withEnsembleSize(value.getEnsembleSize())
                .withWriteQuorumSize(value.getWriteQuorumSize()).withAckQuorumSize(value.getAckQuorumSize())
                .withCToken(value.getCToken()).withCustomMetadata(custom).withCreationTime(value.getCtime())
                .storingCreationTime(LedgerMetadataUtils.shouldStoreCtime(value));
        if (value.hasPassword()) {
            builder.withDigestType(value.getDigestType()).withPassword(value.getPassword().clone());
        }
        value.getAllEnsembles().forEach(builder::newEnsembleEntry);
        return builder.build();
    }

    static byte[] metadataDigest(LedgerMetadata value) throws IOException {
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update("BookKeeper/ProfileRecoveryMetadata/v1\0".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            hash.update(new LedgerMetadataSerDe().serialize(predecessorProjection(value)));
            return hash.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable", impossible);
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileRecoveryContext context && Arrays.equals(encode(), context.encode());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(encode());
    }

    @Override
    public String toString() {
        return "ProfileRecoveryContext{ledger=" + ledger.ledgerId() + ", generation=" + generation() + '}';
    }
}

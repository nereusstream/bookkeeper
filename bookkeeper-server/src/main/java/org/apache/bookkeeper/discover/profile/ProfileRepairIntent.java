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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;

/** One bounded inert repair plan. Admission, local installation and recovery grant remain distinct facts. */
public record ProfileRepairIntent(ProfileRecoveryContext recovery, int slot, byte[] intentId,
        long fragmentStart, int replacedMemberSlot, byte[] oldEnsembleDigest,
        ProfileControlScope install, byte[] grantOperationId, ProfileExactRange range) {
    public static final int MAX_INTENTS = 32;
    public static final int FIXED_BYTES = 352;
    public static final int MAX_BYTES = FIXED_BYTES + 2 + 255 + 16 + 2 + 128 + 4;
    private static final int MAGIC = 0x55575250;
    // UWRP

    public ProfileRepairIntent {
        Objects.requireNonNull(recovery, "recovery");
        Objects.requireNonNull(install, "install");
        Objects.requireNonNull(range, "range");
        intentId = ProfileLifecycleHead.fixed(intentId, 16, true);
        oldEnsembleDigest = ProfileLifecycleHead.fixed(oldEnsembleDigest, 32, false);
        grantOperationId = ProfileLifecycleHead.fixed(grantOperationId, 16, true);
        if (slot < 0 || slot >= MAX_INTENTS || fragmentStart < 0 || range.firstEntryId() < fragmentStart
                || replacedMemberSlot < 0 || replacedMemberSlot >= 32
                || install.operation() != ProfileControlOperation.INSTALL
                || install.ledgerId() != recovery.ledger().ledgerId()
                || !Arrays.equals(install.ledgerInstanceId(), recovery.ledger().ledgerInstanceId())
                || !Arrays.equals(install.descriptorIdentity().toBytes(), recovery.ledger().descriptorIdentity())
                || install.authorityGeneration() != recovery.controlGeneration()
                || Arrays.equals(install.operationId(), grantOperationId)
                || Arrays.equals(recovery.operationId(), grantOperationId)) {
            throw new IllegalArgumentException("Invalid bounded repair intent scope");
        }
    }

    @Override
    public byte[] intentId() {
        return intentId.clone();
    }
    @Override
    public byte[] oldEnsembleDigest() {
        return oldEnsembleDigest.clone();
    }
    @Override
    public byte[] grantOperationId() {
        return grantOperationId.clone();
    }

    public long intentGeneration() {
        return recovery.generation();
    }
    public long grantGeneration() {
        return recovery.controlGeneration();
    }

    public ProfileControlScope grantScope() {
        var unbound = new ProfileControlScope(ProfileControlOperation.RECOVERY_GRANT,
                ProfileControlOperation.RECOVERY_GRANT.purpose(), install.ledgerId(), install.ledgerInstanceId(),
                install.descriptorIdentity(), install.targetBookie(), install.targetStorageIncarnation(),
                install.authorityDomain(), grantGeneration(), grantOperationId, new byte[32], range);
        return new ProfileControlScope(unbound.operation(), unbound.purpose(), unbound.ledgerId(),
                unbound.ledgerInstanceId(), unbound.descriptorIdentity(), unbound.targetBookie(),
                unbound.targetStorageIncarnation(), unbound.authorityDomain(), unbound.authorityGeneration(),
                unbound.operationId(), grantPayloadIdentity(unbound), range);
    }

    private byte[] grantPayloadIdentity(ProfileControlScope scope) {
        byte[] prefix = "UW-RECOVERY-GRANT-V1".getBytes(StandardCharsets.US_ASCII);
        byte[] target = scope.targetBookie().getBytes(StandardCharsets.UTF_8);
        byte[] domain = scope.authorityDomain().getBytes(StandardCharsets.UTF_8);
        ByteBuffer bytes = ByteBuffer.allocate(prefix.length + 60 + 2 + target.length + 16 + 2 + domain.length + 48);
        bytes.put(prefix).put(recovery.ledger().encode()).putShort((short) target.length).put(target)
                .put(scope.targetStorageIncarnation()).putShort((short) domain.length).put(domain)
                .put(intentId).putLong(intentGeneration()).putLong(grantGeneration())
                .putLong(range.firstEntryId()).putLong(range.lastEntryId());
        return digest(bytes.array());
    }

    public byte[] encode() {
        byte[] target = ProfileAuthorityCodec.text(install.targetBookie(), 255);
        byte[] domain = ProfileAuthorityCodec.text(install.authorityDomain(), 128);
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + 2 + target.length + 16 + 2 + domain.length + 4);
        out.putInt(MAGIC).putShort((short) 1).putShort((short) 0).put(recovery.encode()).putInt(slot)
                .put(intentId).putLong(intentGeneration()).putLong(grantGeneration()).putLong(fragmentStart)
                .putInt(replacedMemberSlot).putLong(range.firstEntryId()).putLong(range.lastEntryId())
                .put(oldEnsembleDigest).put(install.operationId()).put(grantOperationId)
                .put(install.publicSemanticPayloadIdentity()).putShort((short) target.length).put(target)
                .put(install.targetStorageIncarnation()).putShort((short) domain.length).put(domain);
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }

    public static ProfileRepairIntent decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_BYTES + 26 || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid repair intent byte bound");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC
                || in.getShort() != 1 || in.getShort() != 0) {
            throw new IllegalArgumentException("Invalid repair intent schema or CRC");
        }
        byte[] context = new byte[184];
        in.get(context);
        var recovery = ProfileRecoveryContext.decode(context);
        int slot = in.getInt();
        byte[] id = new byte[16];
        in.get(id);
        long intentGeneration = in.getLong();
        long grantGeneration = in.getLong();
        long start = in.getLong();
        int member = in.getInt();
        var range = new ProfileExactRange(in.getLong(), in.getLong());
        byte[] old = new byte[32];
        in.get(old);
        byte[] installId = new byte[16];
        in.get(installId);
        byte[] grantId = new byte[16];
        in.get(grantId);
        byte[] payload = new byte[32];
        in.get(payload);
        String target = text(in, 255, 23);
        byte[] incarnation = new byte[16];
        in.get(incarnation);
        String domain = text(in, 128, 4);
        if (in.remaining() != 4 || intentGeneration != recovery.generation()
                || grantGeneration != recovery.controlGeneration()) {
            throw new IllegalArgumentException("Repair intent scope/length mismatch");
        }
        var install = new ProfileControlScope(ProfileControlOperation.INSTALL,
                ProfileControlOperation.INSTALL.purpose(),
                recovery.ledger().ledgerId(), recovery.ledger().ledgerInstanceId(),
                ProfileDescriptorIdentity.fromBytes(recovery.ledger().descriptorIdentity()), target, incarnation,
                domain, grantGeneration, installId, payload, null);
        return new ProfileRepairIntent(recovery, slot, id, start, member, old, install, grantId, range);
    }

    private static String text(ByteBuffer in, int max, int tail) {
        int size = Short.toUnsignedInt(in.getShort());
        if (size < 1 || size > max || size > in.remaining() - tail) {
            throw new IllegalArgumentException("Invalid repair intent text");
        }
        byte[] bytes = new byte[size];
        in.get(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, ProfileAuthorityCodec.text(value, max))) {
            throw new IllegalArgumentException("Noncanonical repair intent text");
        }
        return value;
    }

    static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable", impossible);
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileRepairIntent intent && Arrays.equals(encode(), intent.encode());
    }
    @Override
    public int hashCode() {
        return Arrays.hashCode(encode());
    }
    @Override
    public String toString() {
        return "ProfileRepairIntent{ledger=" + recovery.ledger().ledgerId() + ", slot=" + slot + '}';
    }
}

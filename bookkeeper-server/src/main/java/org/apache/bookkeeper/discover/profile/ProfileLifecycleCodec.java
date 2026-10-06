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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;

/** Fixed head and bounded immutable initial target page; all encoded fields are public. */
final class ProfileLifecycleCodec {
    static final int HEAD_BYTES = 240;
    static final int PAGE_FIXED_BYTES = 220;
    static final int PAGE_MAX_BYTES = PAGE_FIXED_BYTES
            + ProfileReservation.MAX_INITIAL_TARGETS * (4 + ProfileAuthorityCodec.MAX_BYTES + 40);
    private static final int HEAD_MAGIC = 0x55574c48;
    private static final int PAGE_MAGIC = 0x55574150;

    record Page(UUID birthId, ProfileReadyPage value) {
        Page {
            if (birthId == null || birthId.equals(new UUID(0, 0)) || value == null) {
                throw new IllegalArgumentException("Invalid immutable page birth");
            }
        }
    }

    private ProfileLifecycleCodec() { }

    static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable", impossible);
        }
    }

    static byte[] encode(ProfileLifecycleHead head) {
        ByteBuffer out = ByteBuffer.allocate(HEAD_BYTES);
        out.putInt(HEAD_MAGIC).putInt(1).putInt(HEAD_BYTES).putInt(head.state().ordinal() + 1).putLong(0)
                .put(head.backlink().encode()).put(head.reservationDigest())
                .putLong(head.birthId().getMostSignificantBits()).putLong(head.birthId().getLeastSignificantBits())
                .putLong(head.sequence()).putLong(head.predecessor()).putLong(head.controlGeneration())
                .put(head.operationId()).put(head.payloadIdentity()).putLong(head.readyPageVersion())
                .put(head.readyPageDigest());
        finish(out);
        return out.array();
    }

    static ProfileLifecycleHead decodeHead(byte[] bytes) {
        ByteBuffer in = check(bytes, HEAD_MAGIC, HEAD_BYTES, HEAD_BYTES);
        int state = in.getInt();
        if (state < 1 || state > 3 || in.getLong() != 0) {
            throw new IllegalArgumentException("Unknown lifecycle head kind or mandatory feature");
        }
        ProfileMetadataBacklink backlink = ProfileMetadataBacklink.decode(take(in, 52));
        byte[] reservationDigest = take(in, 32);
        UUID birth = new UUID(in.getLong(), in.getLong());
        long sequence = in.getLong();
        long predecessor = in.getLong();
        long controlGeneration = in.getLong();
        byte[] operation = take(in, 16);
        byte[] payload = take(in, 32);
        long pageVersion = in.getLong();
        byte[] pageDigest = take(in, 32);
        return new ProfileLifecycleHead(backlink, reservationDigest, birth,
                ProfileLifecycleHead.State.values()[state - 1], sequence, predecessor, controlGeneration,
                operation, payload, pageVersion, pageDigest);
    }

    static byte[] encode(Page envelope) {
        ProfileReadyPage page = envelope.value();
        List<byte[]> scopes = page.installs().stream().map(receipt -> ProfileAuthorityCodec.encode(
                new CommittedProfileAuthority(receipt.scope(), CommittedProfileAuthority.State.PREPARING,
                        receipt.engine(), 0, true))).toList();
        int length = PAGE_FIXED_BYTES + scopes.stream().mapToInt(bytes -> 4 + bytes.length + 40).sum();
        ByteBuffer out = ByteBuffer.allocate(length);
        out.putInt(PAGE_MAGIC).putInt(1).putInt(length).putInt(1).putLong(0).put(page.backlink().encode())
                .putLong(page.lifecycleBirth().getMostSignificantBits())
                .putLong(page.lifecycleBirth().getLeastSignificantBits())
                .putLong(envelope.birthId().getMostSignificantBits())
                .putLong(envelope.birthId().getLeastSignificantBits())
                .put(page.operationId()).put(page.payloadIdentity()).putLong(page.controlGeneration())
                .putLong(page.metadataVersion()).putLong(page.creatorToken()).put(page.ensembleDigest())
                .putInt(scopes.size());
        for (int index = 0; index < scopes.size(); index++) {
            out.putInt(scopes.get(index).length).put(scopes.get(index))
                    .put(page.installs().get(index).capabilityDigest())
                    .putLong(page.installs().get(index).localInstallGeneration());
        }
        finish(out);
        return out.array();
    }

    static Page decodePage(byte[] bytes) {
        ByteBuffer in = check(bytes, PAGE_MAGIC, PAGE_FIXED_BYTES, PAGE_MAX_BYTES);
        if (in.getInt() != 1 || in.getLong() != 0) {
            throw new IllegalArgumentException("Unknown initial page kind or mandatory feature");
        }
        ProfileMetadataBacklink backlink = ProfileMetadataBacklink.decode(take(in, 52));
        UUID birth = new UUID(in.getLong(), in.getLong());
        UUID pageBirth = new UUID(in.getLong(), in.getLong());
        byte[] operation = take(in, 16);
        byte[] payload = take(in, 32);
        long generation = in.getLong();
        long metadataVersion = in.getLong();
        long creator = in.getLong();
        byte[] ensemble = take(in, 32);
        int count = in.getInt();
        if (count < 1 || count > ProfileReservation.MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("Invalid initial target page count");
        }
        List<ProfileInstallReceipt> receipts = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (in.remaining() < 48) {
                throw new IllegalArgumentException("Truncated initial target page");
            }
            int length = in.getInt();
            if (length < ProfileAuthorityCodec.FIXED_BYTES || length > ProfileAuthorityCodec.MAX_BYTES
                    || length > in.remaining() - 44) {
                throw new IllegalArgumentException("Invalid initial target scope length");
            }
            CommittedProfileAuthority authority = ProfileAuthorityCodec.decode(take(in, length), 0);
            if (!authority.complete() || authority.state() != CommittedProfileAuthority.State.PREPARING
                    || authority.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE) {
                throw new IllegalArgumentException("Invalid initial target receipt authority");
            }
            receipts.add(new ProfileInstallReceipt(authority.scope(), authority.requiredEngine(),
                    take(in, 32), in.getLong()));
        }
        if (in.remaining() != 4) {
            throw new IllegalArgumentException("Trailing initial target page bytes");
        }
        return new Page(pageBirth, new ProfileReadyPage(backlink, birth, operation, payload, generation,
                metadataVersion, creator, ensemble, receipts));
    }

    private static byte[] take(ByteBuffer in, int length) {
        byte[] result = new byte[length];
        in.get(result);
        return result;
    }

    private static ByteBuffer check(byte[] bytes, int magic, int minimum, int maximum) {
        if (bytes == null || bytes.length < minimum || bytes.length > maximum) {
            throw new IllegalArgumentException("Invalid lifecycle record length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != magic
                || in.getInt() != 1 || in.getInt() != bytes.length) {
            throw new IllegalArgumentException("Invalid lifecycle record envelope");
        }
        return in;
    }

    private static void finish(ByteBuffer out) {
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
    }
}

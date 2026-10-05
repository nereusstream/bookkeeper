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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;

/** Bounded schema-1 PREPARING root, reusing the strict secret-free authority codec for ordered targets. */
final class ProfileReservationCodec {
    static final int FIXED_BYTES = 84;
    static final int MAX_BYTES = FIXED_BYTES
            + ProfileReservation.MAX_INITIAL_TARGETS * (4 + ProfileAuthorityCodec.MAX_BYTES);
    private static final int MAGIC = 0x55575052;

    private ProfileReservationCodec() { }

    static byte[] encode(ProfileReservation reservation) {
        List<byte[]> targets = reservation.installs().stream().map(scope -> ProfileAuthorityCodec.encode(
                new CommittedProfileAuthority(scope, CommittedProfileAuthority.State.PREPARING,
                        EngineProfile.SEGMENT_WAL_ENGINE, 0, true))).toList();
        int size = FIXED_BYTES + targets.stream().mapToInt(bytes -> 4 + bytes.length).sum();
        ByteBuffer out = ByteBuffer.allocate(size);
        out.putInt(MAGIC).putInt(1).putInt(size).putInt(1).putLong(0)
                .put(reservation.backlink().encode()).putInt(targets.size());
        targets.forEach(bytes -> out.putInt(bytes.length).put(bytes));
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.array();
    }

    static ProfileReservation decode(byte[] bytes) {
        if (bytes == null || bytes.length < FIXED_BYTES || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid Profile reservation length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC
                || in.getInt() != 1 || in.getInt() != bytes.length || in.getInt() != 1 || in.getLong() != 0) {
            throw new IllegalArgumentException("Invalid Profile reservation envelope");
        }
        byte[] reference = new byte[ProfileMetadataBacklink.ENCODED_BYTES];
        in.get(reference);
        int count = in.getInt();
        if (count < 1 || count > ProfileReservation.MAX_INITIAL_TARGETS) {
            throw new IllegalArgumentException("Invalid Profile reservation target count");
        }
        List<ProfileControlScope> targets = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (in.remaining() < 8) {
                throw new IllegalArgumentException("Truncated Profile reservation target");
            }
            int length = in.getInt();
            if (length < ProfileAuthorityCodec.FIXED_BYTES || length > ProfileAuthorityCodec.MAX_BYTES
                    || length > in.remaining() - 4) {
                throw new IllegalArgumentException("Invalid Profile reservation target length");
            }
            byte[] value = new byte[length];
            in.get(value);
            CommittedProfileAuthority authority = ProfileAuthorityCodec.decode(value, 0);
            if (!authority.complete() || authority.state() != CommittedProfileAuthority.State.PREPARING
                    || authority.requiredEngine() != EngineProfile.SEGMENT_WAL_ENGINE) {
                throw new IllegalArgumentException("Invalid PREPARING target authority");
            }
            targets.add(authority.scope());
        }
        if (in.remaining() != 4) {
            throw new IllegalArgumentException("Trailing Profile reservation bytes");
        }
        return new ProfileReservation(ProfileMetadataBacklink.decode(reference), targets);
    }
}

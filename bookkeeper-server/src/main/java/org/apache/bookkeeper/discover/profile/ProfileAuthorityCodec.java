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
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.ProfileDescriptorIdentity;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityPurpose;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileExactRange;

/** Experimental single committed-authority view, with no secret, membership copy or embedded store version. */
final class ProfileAuthorityCodec {
    static final int FIXED_BYTES = 180;
    static final int MAX_BYTES = FIXED_BYTES + 255 + 128 + 16;
    private static final int MAGIC = 0x55574155;

    private ProfileAuthorityCodec() { }

    static int purposeId(ProfileAuthorityPurpose purpose) {
        return switch (purpose) {
            case PREPARING_INSTALL -> 1;
            case READY_INITIAL -> 2;
            case POST_MEMBERSHIP_REPLACEMENT -> 3;
            case RECOVERY_GRANT -> 4;
            case RECOVERY_GRANT_CLOSE -> 5;
            case TOMBSTONE_OR_DELETE -> 6;
            case STATUS_QUERY -> 7;
        };
    }

    private static int stateId(CommittedProfileAuthority.State state) {
        return switch (state) {
            case PREPARING -> 1;
            case READY_INITIAL -> 2;
            case POST_MEMBERSHIP_REPLACEMENT -> 3;
            case RECOVERY_OPEN -> 4;
            case RECOVERY_CLOSE_AUTHORIZED -> 5;
            case DELETE_AUTHORIZED -> 6;
            case STATUS_QUERYABLE -> 7;
        };
    }

    static byte[] text(String value, int maximum) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.indexOf(0) >= 0 || bytes.length > maximum
                || !value.equals(new String(bytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid authority text");
        }
        return bytes;
    }

    static byte[] encode(CommittedProfileAuthority authority) {
        ProfileControlScope scope = authority.scope();
        byte[] bookie = text(scope.targetBookie(), 255);
        byte[] domain = text(scope.authorityDomain(), 128);
        ProfileExactRange range = scope.exactRange();
        int operation = purposeId(scope.operation().purpose());
        ByteBuffer out = ByteBuffer.allocate(FIXED_BYTES + bookie.length + domain.length + (range == null ? 0 : 16));
        out.putInt(MAGIC).putInt(1).putInt(out.capacity()).putInt(authority.complete() ? 1 : 0)
                .putInt(operation).putInt(purposeId(scope.purpose())).putInt(stateId(authority.state()))
                .putInt(authority.requiredEngine().wireValue()).putLong(scope.ledgerId())
                .put(scope.ledgerInstanceId()).put(scope.descriptorIdentity().toBytes())
                .put(scope.targetStorageIncarnation()).putLong(scope.authorityGeneration())
                .put(scope.operationId()).put(scope.publicSemanticPayloadIdentity())
                .putInt(bookie.length).put(bookie).putInt(domain.length).put(domain)
                .putInt(range == null ? 0 : 1);
        if (range != null) {
            out.putLong(range.firstEntryId()).putLong(range.lastEntryId());
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.array();
    }

    static CommittedProfileAuthority decode(byte[] bytes, long version) {
        if (bytes == null || bytes.length < FIXED_BYTES + 2 || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid authority envelope length");
        }
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer in = ByteBuffer.wrap(bytes);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != MAGIC
                || in.getInt() != 1 || in.getInt() != bytes.length) {
            throw new IllegalArgumentException("Invalid authority envelope or checksum");
        }
        int complete = in.getInt();
        int operation = in.getInt();
        int purpose = in.getInt();
        int state = in.getInt();
        if ((complete != 0 && complete != 1) || operation < 1 || operation > 7
                || purpose != operation || state < 1 || state > 7) {
            throw new IllegalArgumentException("Unknown authority discriminator");
        }
        EngineProfile engine = switch (in.getInt()) {
            case 1 -> EngineProfile.CLASSIC_ENGINE;
            case 2 -> EngineProfile.DIRECT_JOURNAL_ENGINE;
            case 3 -> EngineProfile.SEGMENT_WAL_ENGINE;
            default -> throw new IllegalArgumentException("Unknown authority engine");
        };
        long ledger = in.getLong();
        byte[] instance = new byte[16];
        byte[] descriptor = new byte[ProfileDescriptorIdentity.IDENTITY_LENGTH];
        byte[] incarnation = new byte[16];
        byte[] operationId = new byte[16];
        byte[] payload = new byte[32];
        in.get(instance).get(descriptor).get(incarnation);
        long generation = in.getLong();
        in.get(operationId).get(payload);
        String bookie = readText(in, 255, 13);
        String domain = readText(in, 128, 8);
        int rangeFlag = in.getInt();
        if ((rangeFlag != 0 && rangeFlag != 1) || in.remaining() != (rangeFlag == 0 ? 4 : 20)) {
            throw new IllegalArgumentException("Invalid authority range or trailing bytes");
        }
        ProfileExactRange range = rangeFlag == 0 ? null : new ProfileExactRange(in.getLong(), in.getLong());
        ProfileControlOperation op = switch (operation) {
            case 1 -> ProfileControlOperation.INSTALL;
            case 2 -> ProfileControlOperation.ACTIVATE_INITIAL;
            case 3 -> ProfileControlOperation.ACTIVATE_REPLACEMENT;
            case 4 -> ProfileControlOperation.RECOVERY_GRANT;
            case 5 -> ProfileControlOperation.RECOVERY_GRANT_CLOSE;
            case 6 -> ProfileControlOperation.TOMBSTONE_OR_DELETE_APPLY;
            case 7 -> ProfileControlOperation.STATUS_QUERY;
            default -> throw new IllegalArgumentException("Unknown authority operation");
        };
        CommittedProfileAuthority.State authorityState = switch (state) {
            case 1 -> CommittedProfileAuthority.State.PREPARING;
            case 2 -> CommittedProfileAuthority.State.READY_INITIAL;
            case 3 -> CommittedProfileAuthority.State.POST_MEMBERSHIP_REPLACEMENT;
            case 4 -> CommittedProfileAuthority.State.RECOVERY_OPEN;
            case 5 -> CommittedProfileAuthority.State.RECOVERY_CLOSE_AUTHORIZED;
            case 6 -> CommittedProfileAuthority.State.DELETE_AUTHORIZED;
            case 7 -> CommittedProfileAuthority.State.STATUS_QUERYABLE;
            default -> throw new IllegalArgumentException("Unknown authority state");
        };
        ProfileControlScope scope = new ProfileControlScope(op, op.purpose(), ledger, instance,
                ProfileDescriptorIdentity.fromBytes(descriptor), bookie, incarnation, domain, generation,
                operationId, payload, range);
        return new CommittedProfileAuthority(scope, authorityState, engine, version, complete == 1);
    }

    private static String readText(ByteBuffer in, int maximum, int suffix) {
        int length = in.getInt();
        if (length < 1 || length > maximum || length > in.remaining() - suffix) {
            throw new IllegalArgumentException("Invalid authority text length");
        }
        byte[] bytes = new byte[length];
        in.get(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, text(value, maximum))) {
            throw new IllegalArgumentException("Non-canonical authority text");
        }
        return value;
    }
}

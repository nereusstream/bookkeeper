/*
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
 */

package org.apache.bookkeeper.common.profile.wire;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Frozen 60-byte ledger context shared by experimental Profile data operations. */
public final class ProfileLedgerContext {

    public static final int ENCODED_LENGTH = 60;
    public static final int LEDGER_INSTANCE_LENGTH = 16;
    public static final int DESCRIPTOR_IDENTITY_LENGTH = 36;

    private final long ledgerId;
    private final byte[] ledgerInstanceId;
    private final byte[] descriptorIdentity;

    public ProfileLedgerContext(long ledgerId, byte[] ledgerInstanceId, byte[] descriptorIdentity) {
        this.ledgerId = ledgerId;
        this.ledgerInstanceId = exactCopy(
                ledgerInstanceId, LEDGER_INSTANCE_LENGTH, ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        this.descriptorIdentity = exactCopy(
                descriptorIdentity,
                DESCRIPTOR_IDENTITY_LENGTH,
                ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        if (ProfileWireCodecSupport.allZero(this.ledgerInstanceId)
                || ProfileWireCodecSupport.allZero(this.descriptorIdentity)) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        }
    }

    public long ledgerId() {
        return ledgerId;
    }

    public byte[] ledgerInstanceId() {
        return ledgerInstanceId.clone();
    }

    public byte[] descriptorIdentity() {
        return descriptorIdentity.clone();
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        buffer.putLong(ledgerId);
        buffer.put(ledgerInstanceId);
        buffer.put(descriptorIdentity);
        return buffer.array();
    }

    public static ProfileLedgerContext decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        }
        return decode(ByteBuffer.wrap(bytes));
    }

    static ProfileLedgerContext decode(ByteBuffer buffer) {
        if (buffer.remaining() < ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.INVALID_LEDGER_CONTEXT);
        }
        long ledgerId = buffer.getLong();
        byte[] ledgerInstanceId = new byte[LEDGER_INSTANCE_LENGTH];
        buffer.get(ledgerInstanceId);
        byte[] descriptorIdentity = new byte[DESCRIPTOR_IDENTITY_LENGTH];
        buffer.get(descriptorIdentity);
        return new ProfileLedgerContext(ledgerId, ledgerInstanceId, descriptorIdentity);
    }

    private static byte[] exactCopy(
            byte[] value, int expectedLength, ProfileWireValidationException.Reason reason) {
        if (value == null || value.length != expectedLength) {
            throw ProfileWireCodecSupport.failure(reason);
        }
        return value.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProfileLedgerContext that)) {
            return false;
        }
        return ledgerId == that.ledgerId
                && Arrays.equals(ledgerInstanceId, that.ledgerInstanceId)
                && Arrays.equals(descriptorIdentity, that.descriptorIdentity);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(ledgerId);
        result = 31 * result + Arrays.hashCode(ledgerInstanceId);
        result = 31 * result + Arrays.hashCode(descriptorIdentity);
        return result;
    }

    @Override
    public String toString() {
        return "ProfileLedgerContext{ledgerId=" + ledgerId
                + ", ledgerInstanceId=<opaque>, descriptorIdentity=<opaque>}";
    }
}

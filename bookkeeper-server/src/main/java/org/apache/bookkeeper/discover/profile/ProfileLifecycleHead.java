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

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Bounded lifecycle head; only DELETE_INTENT advances the global control generation. */
public record ProfileLifecycleHead(ProfileMetadataBacklink backlink, byte[] reservationDigest, UUID birthId,
        State state, long sequence, long predecessor, long controlGeneration, byte[] operationId,
        byte[] payloadIdentity, long readyPageVersion, byte[] readyPageDigest) {
    public enum State { PREPARING, READY, DELETE_INTENT, REPLACEMENT }

    public ProfileLifecycleHead {
        Objects.requireNonNull(backlink, "backlink");
        Objects.requireNonNull(birthId, "birthId");
        Objects.requireNonNull(state, "state");
        reservationDigest = fixed(reservationDigest, 32, false);
        operationId = fixed(operationId, 16, true);
        payloadIdentity = fixed(payloadIdentity, 32, false);
        readyPageDigest = fixed(readyPageDigest, 32, false);
        if (birthId.equals(new UUID(0, 0)) || sequence < 1
                || sequence > 3 + ProfileReplacementPage.MAX_PUBLICATIONS || predecessor != sequence - 1
                || controlGeneration <= 0 || readyPageVersion < -1
                || state == State.PREPARING && (sequence != 1 || readyPageVersion != -1)
                || state == State.READY && (sequence != 2 || readyPageVersion < 0)
                || state == State.REPLACEMENT && (sequence < 3 || readyPageVersion < 0
                        || sequence > 2 + ProfileReplacementPage.MAX_PUBLICATIONS)
                || state == State.DELETE_INTENT && (sequence == 1
                        || sequence == 2 && readyPageVersion != -1 || sequence >= 3 && readyPageVersion < 0)
                || readyPageVersion == -1 && !Arrays.equals(readyPageDigest, new byte[32])) {
            throw new IllegalArgumentException("Invalid Profile lifecycle head");
        }
    }

    static byte[] fixed(byte[] value, int length, boolean nonzero) {
        Objects.requireNonNull(value, "value");
        if (value.length != length || nonzero && Arrays.equals(value, new byte[length])) {
            throw new IllegalArgumentException("Invalid Profile lifecycle identity");
        }
        return value.clone();
    }

    @Override
    public byte[] reservationDigest() {
        return reservationDigest.clone();
    }

    @Override
    public byte[] operationId() {
        return operationId.clone();
    }

    @Override
    public byte[] payloadIdentity() {
        return payloadIdentity.clone();
    }

    @Override
    public byte[] readyPageDigest() {
        return readyPageDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileLifecycleHead that && backlink.equals(that.backlink)
                && Arrays.equals(reservationDigest, that.reservationDigest) && birthId.equals(that.birthId)
                && state == that.state && sequence == that.sequence && predecessor == that.predecessor
                && controlGeneration == that.controlGeneration && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(payloadIdentity, that.payloadIdentity) && readyPageVersion == that.readyPageVersion
                && Arrays.equals(readyPageDigest, that.readyPageDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlink, Arrays.hashCode(reservationDigest), birthId, state, sequence, predecessor,
                controlGeneration, Arrays.hashCode(operationId), Arrays.hashCode(payloadIdentity), readyPageVersion,
                Arrays.hashCode(readyPageDigest));
    }
}

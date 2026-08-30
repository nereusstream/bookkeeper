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

import java.util.Arrays;
import java.util.Objects;

/** Immutable Profile frame detached from any transport or endpoint. */
public final class ProfileFrame {

    private final ProfileFrameHeader header;
    private final byte[] body;

    public ProfileFrame(ProfileFrameHeader header, byte[] body) {
        this.header = Objects.requireNonNull(header, "header");
        this.body = Objects.requireNonNull(body, "body").clone();
        if (header.bodyLength() != body.length) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.BODY_LENGTH_MISMATCH);
        }
        if (header.operation().isReservedDisabled() && !header.isResponse() && body.length != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.RESERVED_OPERATION_BODY);
        }
    }

    public static ProfileFrame request(ProfileOperation operation, long requestId, byte[] body) {
        Objects.requireNonNull(body, "body");
        return new ProfileFrame(ProfileFrameHeader.request(operation, requestId, body.length), body);
    }

    public ProfileFrameHeader header() {
        return header;
    }

    public byte[] body() {
        return body.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ProfileFrame that
                && header.equals(that.header)
                && Arrays.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return 31 * header.hashCode() + Arrays.hashCode(body);
    }

    @Override
    public String toString() {
        return "ProfileFrame{header=" + header + ", bodyLength=" + body.length + '}';
    }
}

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

import java.util.Objects;

/** Validated fixed 32-byte Profile frame header fields. */
public record ProfileFrameHeader(
        int protocolMajor,
        int protocolMinor,
        int headerLength,
        ProfileOperation operation,
        int flags,
        long requestId,
        int bodyLength,
        long reserved) {

    public static final int MAGIC = 0x0ff0_4250;
    public static final int PROTOCOL_MAJOR = 1;
    public static final int PROTOCOL_MINOR = 0;
    public static final int ENCODED_LENGTH = 32;
    public static final int RESPONSE = 0x0000_0001;
    public static final int ERROR = 0x0000_0002;
    public static final int END_OF_STREAM = 0x0000_0004;
    public static final int ALLOWED_FLAGS = RESPONSE | ERROR | END_OF_STREAM;

    public ProfileFrameHeader {
        if (protocolMajor != PROTOCOL_MAJOR || protocolMinor != PROTOCOL_MINOR) {
            throw ProfileWireCodecSupport.failure(
                    ProfileWireValidationException.Reason.UNSUPPORTED_PROTOCOL_VERSION);
        }
        if (headerLength != ENCODED_LENGTH) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.WRONG_HEADER_LENGTH);
        }
        Objects.requireNonNull(operation, "operation");
        if ((flags & ~ALLOWED_FLAGS) != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.UNKNOWN_FLAGS);
        }
        if (bodyLength < 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.BODY_LENGTH_OVERFLOW);
        }
        if (reserved != 0) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.NONZERO_RESERVED);
        }
    }

    public static ProfileFrameHeader request(ProfileOperation operation, long requestId, int bodyLength) {
        return new ProfileFrameHeader(
                PROTOCOL_MAJOR,
                PROTOCOL_MINOR,
                ENCODED_LENGTH,
                operation,
                0,
                requestId,
                bodyLength,
                0);
    }

    public boolean isResponse() {
        return (flags & RESPONSE) != 0;
    }

    public boolean isError() {
        return (flags & ERROR) != 0;
    }
}

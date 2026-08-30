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

/** Secret-free, fail-closed Profile wire validation failure. */
public final class ProfileWireValidationException extends IllegalArgumentException {

    public enum Reason {
        NULL_INPUT,
        TRUNCATED_OUTER_LENGTH,
        TRUNCATED_HEADER,
        TRUNCATED_BODY,
        TRAILING_BYTES,
        FRAME_TOO_SMALL,
        PRE_HELLO_FRAME_TOO_LARGE,
        CONTROL_FRAME_TOO_LARGE,
        ABSOLUTE_FRAME_TOO_LARGE,
        OUTER_LENGTH_OVERFLOW,
        OUTER_LENGTH_MISMATCH,
        BAD_MAGIC,
        UNSUPPORTED_PROTOCOL_VERSION,
        WRONG_HEADER_LENGTH,
        UNKNOWN_SUBTYPE,
        UNKNOWN_FLAGS,
        NONZERO_RESERVED,
        BODY_LENGTH_OVERFLOW,
        BODY_LENGTH_MISMATCH,
        HELLO_NOT_FIRST,
        INVALID_HELLO_FLAGS,
        INVALID_HELLO_LENGTH,
        INVALID_HELLO_VERSION_RANGE,
        CAPABILITY_COUNT_OUT_OF_RANGE,
        INVALID_CAPABILITY,
        DUPLICATE_CAPABILITY,
        OUT_OF_ORDER_CAPABILITY,
        INVALID_UTF8,
        NUL_IN_BOOKIE_ID,
        INVALID_BOOKIE_ID_LENGTH,
        ZERO_STORAGE_INCARNATION,
        INVALID_STATUS_LENGTH,
        UNKNOWN_STATUS_CLASS,
        UNKNOWN_RETRY_DISPOSITION,
        UNKNOWN_DURABLE_RESULT,
        INVALID_LEDGER_CONTEXT,
        INVALID_OPERATION_BODY,
        OPERATION_BODY_MISMATCH,
        RESERVED_OPERATION_BODY,
        BLOCKED_UNFROZEN_CONTROL_BODY
    }

    private final Reason reason;

    ProfileWireValidationException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

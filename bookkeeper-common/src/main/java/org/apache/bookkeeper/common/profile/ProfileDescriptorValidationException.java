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

package org.apache.bookkeeper.common.profile;

/** Secret-free, fail-closed descriptor validation failure. */
public final class ProfileDescriptorValidationException extends IllegalArgumentException {

    public enum Reason {
        NULL_INPUT,
        INPUT_TOO_LARGE,
        TRUNCATED,
        BAD_MAGIC,
        UNSUPPORTED_CODEC_VERSION,
        UNSUPPORTED_SCHEMA_VERSION,
        WRONG_TOTAL_LENGTH,
        WRONG_FIELD_COUNT,
        NONZERO_FLAGS,
        NON_CANONICAL_LENGTH,
        UNKNOWN_FIELD,
        DUPLICATE_FIELD,
        OUT_OF_ORDER_FIELD,
        MISSING_FIELD,
        UNKNOWN_TYPE,
        WRONG_VALUE_LENGTH,
        UNKNOWN_ENUM,
        WRONG_CAPABILITY_SET_LENGTH,
        CAPABILITY_COUNT_OUT_OF_RANGE,
        INVALID_CAPABILITY,
        DUPLICATE_CAPABILITY,
        OUT_OF_ORDER_CAPABILITY,
        UNKNOWN_CAPABILITY,
        INVALID_QUORUM,
        INVALID_LOSS_BUDGET,
        INVALID_POLICY,
        UNKNOWN_POLICY,
        TRAILING_BYTES,
        INVALID_IDENTITY,
        IDENTITY_MISMATCH
    }

    private final Reason reason;

    ProfileDescriptorValidationException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

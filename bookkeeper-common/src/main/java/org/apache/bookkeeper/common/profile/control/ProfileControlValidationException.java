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

package org.apache.bookkeeper.common.profile.control;

/** Stable fail-closed reason for an invalid experimental Profile control request. */
public final class ProfileControlValidationException extends IllegalArgumentException {

    public enum Reason {
        NULL_FIELD,
        INVALID_FIXED_LENGTH,
        ZERO_IDENTIFIER,
        INVALID_LEDGER_ID,
        INVALID_GENERATION,
        INVALID_TEXT,
        TEXT_TOO_LARGE,
        PURPOSE_MISMATCH,
        RANGE_FORBIDDEN,
        INVALID_RANGE,
        INVALID_CREDENTIAL,
        INVALID_REQUEST_SHAPE
    }

    private final Reason reason;

    public ProfileControlValidationException(Reason reason) {
        super(reason.name());
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

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

import java.util.HexFormat;
import java.util.Objects;

/** Secret-free endpoint result; it is not a production durable receipt. */
public record ProfileControlResult(
        Outcome outcome,
        ProfileControlOperation operation,
        long ledgerId,
        String operationIdHex,
        long authorityGeneration,
        long localGeneration,
        boolean durable) {

    public enum Outcome {
        APPLIED,
        ALREADY_APPLIED,
        CONFLICT,
        DURABILITY_UNKNOWN,
        NOT_FOUND,
        STALE_OR_COMPACTED,
        DENIED,
        TRANSIENT_RECONCILING,
        QUARANTINED,
        BAD_REQUEST,
        UNSUPPORTED
    }

    public ProfileControlResult {
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(operationIdHex, "operationIdHex");
        if (ledgerId < 0 || authorityGeneration <= 0 || localGeneration < 0) {
            throw new IllegalArgumentException("invalid secret-free result coordinates");
        }
    }

    static ProfileControlResult fromScope(
            ProfileControlScope scope, Outcome outcome, long localGeneration, boolean durable) {
        return new ProfileControlResult(
                outcome,
                scope.operation(),
                scope.ledgerId(),
                HexFormat.of().formatHex(scope.operationId()),
                scope.authorityGeneration(),
                localGeneration,
                durable);
    }
}

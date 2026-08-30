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

import java.util.HashMap;
import java.util.Map;

/** Frozen operation discriminators in the experimental Profile frame manifest. */
public enum ProfileOperation {
    HELLO(0x0001, Kind.HELLO),

    INSTALL(0x0101, Kind.CONTROL_BLOCKED),
    ACTIVATE_INITIAL(0x0102, Kind.CONTROL_BLOCKED),
    ACTIVATE_REPLACEMENT(0x0103, Kind.CONTROL_BLOCKED),
    RECOVERY_GRANT(0x0104, Kind.CONTROL_BLOCKED),
    RECOVERY_GRANT_CLOSE(0x0105, Kind.CONTROL_BLOCKED),
    TOMBSTONE_OR_DELETE_APPLY(0x0106, Kind.CONTROL_BLOCKED),
    OPERATION_STATUS(0x0107, Kind.CONTROL_BLOCKED),

    ADD_NORMAL(0x0201, Kind.DATA),
    ADD_RECOVERY(0x0202, Kind.DATA),
    READ_ENTRY(0x0203, Kind.DATA),
    FENCE_LEDGER(0x0204, Kind.DATA),
    READ_LAC(0x0205, Kind.DATA),
    WRITE_LAC(0x0206, Kind.DATA),
    FORCE_LEDGER(0x0207, Kind.DATA),
    LIST_ENTRIES(0x0208, Kind.DATA),

    RANGE_READ(0x0301, Kind.RESERVED_DISABLED),
    BATCH_RECOVERY_ADD(0x0302, Kind.RESERVED_DISABLED);

    public enum Kind {
        HELLO,
        CONTROL_BLOCKED,
        DATA,
        RESERVED_DISABLED
    }

    private static final Map<Integer, ProfileOperation> BY_WIRE_VALUE = new HashMap<>();

    static {
        for (ProfileOperation operation : values()) {
            BY_WIRE_VALUE.put(operation.wireValue, operation);
        }
    }

    private final int wireValue;
    private final Kind kind;

    ProfileOperation(int wireValue, Kind kind) {
        this.wireValue = wireValue;
        this.kind = kind;
    }

    public int wireValue() {
        return wireValue;
    }

    public Kind kind() {
        return kind;
    }

    public boolean isControl() {
        return kind == Kind.CONTROL_BLOCKED;
    }

    public boolean isReservedDisabled() {
        return kind == Kind.RESERVED_DISABLED;
    }

    public static ProfileOperation fromWireValue(int wireValue) {
        ProfileOperation operation = BY_WIRE_VALUE.get(wireValue);
        if (operation == null) {
            throw ProfileWireCodecSupport.failure(ProfileWireValidationException.Reason.UNKNOWN_SUBTYPE);
        }
        return operation;
    }
}

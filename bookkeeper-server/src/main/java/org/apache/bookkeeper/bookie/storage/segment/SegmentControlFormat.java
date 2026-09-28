/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
package org.apache.bookkeeper.bookie.storage.segment;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.apache.bookkeeper.common.profile.startup.Generation;

/** Experimental physical control framing only. Decoding never means a command was applied. */
final class SegmentControlFormat {
    enum Owner {
        BOOKIE(1), ARENA(2);
        final int code;

        Owner(int code) {
            this.code = code;
        }
    }

    enum Type {
        ROUTE(Owner.BOOKIE, 1), CREDENTIAL(Owner.BOOKIE, 2), INSTALL(Owner.BOOKIE, 3),
        ACTIVATE(Owner.BOOKIE, 4), FENCE(Owner.BOOKIE, 5), RECOVERY_GRANT(Owner.BOOKIE, 6),
        EXPLICIT_LAC(Owner.BOOKIE, 7), TOMBSTONE(Owner.BOOKIE, 8),
        RECOVERY_GRANT_CLOSE(Owner.BOOKIE, 9),
        ALLOC(Owner.ARENA, 1), ALLOC_POOL(Owner.ARENA, 2), MOVE(Owner.ARENA, 3),
        FREE(Owner.ARENA, 4), CHECKPOINT_BEGIN(Owner.ARENA, 5), CHECKPOINT_COMMIT(Owner.ARENA, 6),
        DEVICE_STATE(Owner.ARENA, 7);
        final Owner owner;
        final int code;

        Type(Owner owner, int code) {
            this.owner = owner;
            this.code = code;
        }
    }

    static ByteBuffer encode(Type type, Generation logGeneration, long sequence, UUID operationId,
                             Generation operationGeneration, long expectedGeneration, ByteBuffer body) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(logGeneration, "logGeneration");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(operationGeneration, "operationGeneration");
        if (sequence < 0 || expectedGeneration < 0 || operationId.equals(new UUID(0, 0))
                || body.remaining() > 65536 - 128 - 16) {
            throw new IllegalArgumentException("Invalid control record");
        }
        int bodyLength = body.remaining();
        int length = Math.multiplyExact(Math.floorDiv(128 + bodyLength + 16 + 7, 8), 8);
        ByteBuffer b = ByteBuffer.allocate(length);
        b.putLong(0x555743544c303031L).putInt(1).putInt(type.owner.code).putInt(type.code).putInt(length)
                .putLong(sequence).putLong(operationId.getMostSignificantBits())
                .putLong(operationId.getLeastSignificantBits())
                .putLong(operationGeneration.value()).putLong(expectedGeneration).putInt(bodyLength).putInt(128)
                .putLong(logGeneration.value());
        b.putInt(124, crc(b, 0, 124));
        b.position(128).put(body.duplicate());
        int footer = length - 16;
        b.position(footer).putInt(0x43544c45).putInt(crc(b, 128, footer - 128)).putLong(sequence);
        b.clear();
        return b.asReadOnlyBuffer();
    }

    private static int crc(ByteBuffer b, int from, int length) {
        CRC32C checksum = new CRC32C();
        checksum.update(b.slice(from, length));
        return (int) checksum.getValue();
    }

    private SegmentControlFormat() {
    }
}

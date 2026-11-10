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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.Test;

public class ProfileManifestPreparationTest {
    @Test
    public void encodesTheExactFixedAttemptBody() throws Exception {
        byte[] nativeRecord = HexFormat.of().parseHex("55574d5000000001000000000000000b" + "2d".repeat(32)
                + "0000000000000000000000000000000100000000000000000000000000000002"
                + "00000000000000000000000000000003");
        var attempt = new ProfileManifestPreparation(11, "2d".repeat(32), new UUID(0, 1), new UUID(0, 2),
                new UUID(0, 3));
        assertEquals(96, nativeRecord.length);
        assertEquals(attempt, ProfileManifestPreparation.decode(nativeRecord));
        assertArrayEquals(nativeRecord, attempt.encode());

        byte[] unknownVersion = nativeRecord.clone();
        ByteBuffer.wrap(unknownVersion).putInt(4, 2);
        assertThrows(IOException.class, () -> ProfileManifestPreparation.decode(unknownVersion));
        assertThrows(IOException.class, () -> ProfileManifestPreparation.decode(Arrays.copyOf(nativeRecord, 97)));
        byte[] zeroNonce = nativeRecord.clone();
        Arrays.fill(zeroNonce, 80, 96, (byte) 0);
        assertThrows(IOException.class, () -> ProfileManifestPreparation.decode(zeroNonce));
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IsolatedSegmentConfigTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void defaultsAreBoundedAndDoNotCreateStorage() {
        Path root = temporary.getRoot().toPath().resolve("disposable-arena");
        IsolatedSegmentConfig config = IsolatedSegmentConfig.defaults(root);
        assertEquals("FILE_CHANNEL", config.backend());
        assertEquals("OFF", config.affinity());
        assertEquals("SYSTEM", config.numaPolicy());
        assertEquals("PARK", config.waitStrategy());
        assertEquals(4, config.logicalShards());
        assertEquals(32L * 1024 * 1024, config.dataFiles() * config.bytesPerFile());
        assertFalse(Files.exists(root));
    }

    @Test
    public void rejectsUnsupportedModesAndUnboundedOrOverflowingCapacity() {
        for (String[] modes : new String[][] {
            {"IO_URING", "OFF", "SYSTEM", "PARK"},
            {"FILE_CHANNEL", "REQUIRE", "SYSTEM", "PARK"},
            {"FILE_CHANNEL", "OFF", "PREFERRED", "PARK"},
            {"FILE_CHANNEL", "OFF", "SYSTEM", "BOUNDED_SPIN"}
        }) {
            assertThrows(IllegalArgumentException.class, () -> config(1, 16384, 1024, modes));
        }
        String[] defaults = {"FILE_CHANNEL", "OFF", "SYSTEM", "PARK"};
        assertThrows(IllegalArgumentException.class, () -> config(2, 16384, 1024, defaults));
        assertThrows(IllegalArgumentException.class, () -> config(1, 16385, 1024, defaults));
        assertThrows(IllegalArgumentException.class, () -> config(1, 4096, 1024, defaults));
        assertThrows(IllegalArgumentException.class, () -> config(1, 16384, 0, defaults));
        assertThrows(ArithmeticException.class, () -> config(1, Long.MAX_VALUE - 4095, 1024, defaults));
    }

    private IsolatedSegmentConfig config(int arenas, long capacity, int payload, String[] modes) {
        return new IsolatedSegmentConfig(temporary.getRoot().toPath(), arenas, 4, 2,
                capacity, 4096, payload, 256, modes[0], modes[1], modes[2], modes[3]);
    }
}

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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.Test;

public class SegmentControlCapacityTest {
    @Test
    public void reservationIncludesHistoryPendingAndIndependentFileGeometry() throws Exception {
        var layout = new SegmentControlCapacity.Layout(8);
        long logBytes = 1024 * 1024;
        long base = SegmentControlCapacity.required(layout, logBytes, 16, 1, 1);
        assertTrue(base > SegmentControlCheckpoint.WORKING_SET_BYTES + 3 * logBytes);
        assertTrue(SegmentControlCapacity.required(layout, 2 * logBytes, 16, 1, 1) > base);
        assertTrue(SegmentControlCapacity.required(layout, logBytes, 32, 1, 1) > base);
        assertTrue(SegmentControlCapacity.required(layout, logBytes, 16, 2, 1) > base);
        assertTrue(SegmentControlCapacity.required(new SegmentControlCapacity.Layout(256),
                logBytes, 16, 1, 1) > base);
        // More DATA files than logical shards is an existing supported geometry.
        var config = new IsolatedSegmentConfig(Path.of("unused-control-capacity"), 1, 1, 2,
                4L * 1024 * 1024, 4096, 1024, 16, "FILE_CHANNEL", "OFF", "SYSTEM", "PARK");
        assertTrue(SegmentControlCapacity.required(config, logBytes) > 0);
        assertTrue(SegmentControlCapacity.required(config, 4096 + 320) > 0);
    }

    @Test
    public void oversizedPendingCommandsAreNotClippedToRemainingJournal() throws Exception {
        var layout = new SegmentControlCapacity.Layout(8);
        long one = SegmentControlCapacity.required(layout, 16384, 1, 1, 1);
        long sixteen = SegmentControlCapacity.required(layout, 16384, 16, 1, 1);
        // A projected maximum MOVE is already larger than this tiny journal before its I/O capacity check.
        assertTrue(sixteen - one >= 15L * SegmentControlCapacity.MAX_RECORD_BYTES);
    }

    @Test
    public void unsupportedAlignmentAndUnrepresentableCapacityAreRejected() {
        for (int alignment : List.of(0, 4, 12, 512)) {
            assertThrows(IllegalArgumentException.class, () -> new SegmentControlCapacity.Layout(alignment));
        }
        assertThrows(IOException.class, () -> SegmentControlCapacity.required(
                new SegmentControlCapacity.Layout(8), Long.MAX_VALUE, 16, 1, 1));
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class ProfileDescriptorModelTest {

    @Test
    public void descriptorDefensivelyCopiesCapabilities() {
        List<ProfileCapability> capabilities = new ArrayList<>();
        capabilities.add(new ProfileCapability(1, 1));

        ProfileDescriptor descriptor = new ProfileDescriptor(
                EngineProfile.CLASSIC_ENGINE,
                PayloadFormat.OPAQUE_LEDGER,
                DurabilityMode.SYNC_ON_ACK,
                3,
                3,
                2,
                1,
                1,
                1,
                capabilities);

        capabilities.clear();
        assertEquals(List.of(new ProfileCapability(1, 1)), descriptor.mandatoryCapabilities());
        assertThrows(
                UnsupportedOperationException.class,
                () -> descriptor.mandatoryCapabilities().add(new ProfileCapability(2, 1)));
    }

    @Test
    public void capabilityEnforcesUnsignedNonZeroWidths() {
        assertThrows(IllegalArgumentException.class, () -> new ProfileCapability(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProfileCapability(0x1_0000_0000L, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProfileCapability(1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProfileCapability(1, 0x1_0000));
        assertEquals(new ProfileCapability(0xffff_ffffL, 0xffff),
                new ProfileCapability(0xffff_ffffL, 0xffff));
    }
}

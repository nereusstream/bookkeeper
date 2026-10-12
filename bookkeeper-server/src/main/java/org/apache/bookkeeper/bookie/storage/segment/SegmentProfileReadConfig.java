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

import java.util.Objects;
import java.util.function.BiPredicate;
import org.apache.bookkeeper.common.profile.control.ProfilePrincipal;
import org.apache.bookkeeper.common.profile.wire.ProfileOperationCodec;

/** Explicit local, nonblocking read-scope policy; transport authentication alone grants no read permission. */
public record SegmentProfileReadConfig(BiPredicate<ProfilePrincipal, ProfileOperationCodec.ReadEntry> authorization,
        BiPredicate<ProfilePrincipal, ProfileOperationCodec.ReadLac> lacAuthorization) {
    public SegmentProfileReadConfig(BiPredicate<ProfilePrincipal, ProfileOperationCodec.ReadEntry> authorization) {
        this(authorization, null);
    }

    public SegmentProfileReadConfig {
        Objects.requireNonNull(authorization, "authorization");
    }
}

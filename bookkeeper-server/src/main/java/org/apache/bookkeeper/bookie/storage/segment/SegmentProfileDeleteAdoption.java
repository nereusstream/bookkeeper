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
import org.apache.bookkeeper.discover.profile.ProfileDeleteHandoff;

/** Original opened control owner's applied adoption; callers cannot manufacture a local force result. */
public final class SegmentProfileDeleteAdoption {
    private final SegmentBookieControlLog source;
    private final ProfileDeleteHandoff.Plan plan;
    private final long controlSequence;

    private SegmentProfileDeleteAdoption(SegmentBookieControlLog source, ProfileDeleteHandoff.Plan plan,
            long controlSequence) {
        this.source = Objects.requireNonNull(source, "source");
        this.plan = Objects.requireNonNull(plan, "plan");
        this.controlSequence = controlSequence;
    }

    /** Package-owned factory used after actual force/apply and owner terminal, followed by native recheck. */
    static SegmentProfileDeleteAdoption fromApplied(SegmentBookieControlLog source, ProfileDeleteHandoff.Plan plan) {
        synchronized (source) {
            long sequence = source.adoptedDeleteHandoffSequence(plan);
            if (plan == null || sequence < 0) {
                return null;
            }
            return new SegmentProfileDeleteAdoption(source, plan, sequence);
        }
    }

    public ProfileDeleteHandoff.Plan plan() {
        return plan;
    }

    public long controlSequence() {
        return controlSequence;
    }

    /** A stopped, failed or different opened graph cannot newly attest the persisted receipt. */
    public boolean isCurrent() {
        return source.adoptedDeleteHandoffSequence(plan) == controlSequence;
    }
}

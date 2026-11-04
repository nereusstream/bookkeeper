/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.Objects;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLaunchTrust.LinuxNeededContextObservation;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxLoadedNames.Reuse;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;

/** Given-data ordinary DT_NEEDED name preparation only; no source, name-miss proof or loader permission. */
final class SegmentClassicLinuxNeededName {
    enum Secure { NON_SECURE, SECURE, UNOBSERVED }

    enum State { LOOKUP_NAME, SKIPPED_EMPTY, UNOBSERVED }

    enum Branch { DIRECTORY_SEARCH, DIRECT_PATH, UNOBSERVED }

    /** The original declaration remains the candidate approval key; name is only the _dl_map_object argument. */
    record Lookup(LinuxNeededContextObservation request, Secure secure, State state, String name) {
        Lookup {
            Objects.requireNonNull(request, "request");
            Objects.requireNonNull(secure, "secure");
            Objects.requireNonNull(state, "state");
            if ((state == State.LOOKUP_NAME) != (name != null) || name != null && name.isEmpty()) {
                throw new IllegalArgumentException("Only a nonempty lookup name can be used for name reuse");
            }
        }
    }

    /** Empty DIRECT_PATH is the text passed to open_verify, never a skipped request or CWD. */
    record AfterMiss(Lookup lookup, Branch branch, String directPath) {
        AfterMiss {
            Objects.requireNonNull(lookup, "lookup");
            Objects.requireNonNull(branch, "branch");
            if ((branch == Branch.DIRECT_PATH) != (directPath != null)) {
                throw new IllegalArgumentException("Only the direct branch carries path text, including empty");
            }
        }
    }

    private SegmentClassicLinuxNeededName() { }

    static Lookup prepare(LinuxNeededContextObservation request, Value origin, Value lib, Value platform,
                          Secure secure, long deadline) throws IOException {
        requireTime(deadline);
        if (request == null || request.declaration() == null || secure == null
                || request.declaration().requester() < 0 || request.declaration().requester() >= 4096
                || request.declaration().ordinal() < 0
                || request.declaration().ordinal() >= SegmentClassicLinuxDeploymentV2.MAX_REQUESTS
                || request.name() == null || request.name().isEmpty()) {
            throw new IOException("An original bounded requester/NEEDED ordinal and nonempty raw name are required");
        }
        boolean known = SegmentClassicLinuxTokens.hasKnownToken(request.name(), deadline);
        // Validate all explicit available data even if secure mode prevents substitution.
        SegmentClassicLinuxTokens.expand("", origin, lib, platform, deadline);
        if (known && secure == Secure.SECURE) {
            throw new IOException("Ordinary DT_NEEDED DST is forbidden in secure mode");
        }
        if (known && secure == Secure.UNOBSERVED) {
            requireTime(deadline);
            return new Lookup(request, secure, State.UNOBSERVED, null);
        }
        Value value = SegmentClassicLinuxTokens.expand(request.name(), origin, lib, platform, deadline);
        State state = switch (value.state()) {
            case AVAILABLE -> value.text().isEmpty() ? State.SKIPPED_EMPTY : State.LOOKUP_NAME;
            case UNAVAILABLE -> State.SKIPPED_EMPTY;
            case UNOBSERVED -> State.UNOBSERVED;
        };
        requireTime(deadline);
        return new Lookup(request, secure, state, state == State.LOOKUP_NAME ? value.text() : null);
    }

    /** Conditional preparation only. The caller must establish that nameLookup belongs to this first name. */
    static AfterMiss afterNameMiss(Lookup lookup, Reuse nameLookup, Value origin, Value lib, Value platform,
                                   long deadline) throws IOException {
        requireTime(deadline);
        if (lookup == null || lookup.state() != State.LOOKUP_NAME || nameLookup == null
                || nameLookup.state() != SegmentClassicLinuxLoadedNames.State.NO_REUSE
                || nameLookup.ordinal() != -1 || nameLookup.replacement() != null) {
            throw new IOException("A first lookup name and an explicit ordinary name miss are required");
        }
        boolean known = SegmentClassicLinuxTokens.hasKnownToken(lookup.name(), deadline);
        SegmentClassicLinuxTokens.expand("", origin, lib, platform, deadline);
        if (lookup.name().indexOf('/') < 0) {
            requireTime(deadline);
            return new AfterMiss(lookup, Branch.DIRECTORY_SEARCH, null);
        }
        if (known && lookup.secure() == Secure.SECURE) {
            throw new IOException("Secure direct-path DST is outside ordinary DT_NEEDED preparation");
        }
        if (known && lookup.secure() == Secure.UNOBSERVED) {
            requireTime(deadline);
            return new AfterMiss(lookup, Branch.UNOBSERVED, null);
        }
        Value value = SegmentClassicLinuxTokens.expand(lookup.name(), origin, lib, platform, deadline);
        requireTime(deadline);
        return switch (value.state()) {
            case AVAILABLE -> new AfterMiss(lookup, Branch.DIRECT_PATH, value.text());
            case UNAVAILABLE -> new AfterMiss(lookup, Branch.DIRECT_PATH, "");
            case UNOBSERVED -> new AfterMiss(lookup, Branch.UNOBSERVED, null);
        };
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Ordinary DT_NEEDED preparation deadline elapsed");
        }
    }
}

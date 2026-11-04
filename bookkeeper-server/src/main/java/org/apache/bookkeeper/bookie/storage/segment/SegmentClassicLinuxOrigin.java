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
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;

/** Given-data _dl_new_object origin calculation only; no actual object, CWD or main-origin source is proved. */
final class SegmentClassicLinuxOrigin {
    enum State { AVAILABLE, UNAVAILABLE, UNOBSERVED, DEFERRED_GET_ORIGIN }

    /** Empty realname defers a different source; it is never automatically converted into a token Value. */
    record Origin(String realname, Value cwd, State state, String text) {
        Origin {
            Objects.requireNonNull(realname, "realname");
            Objects.requireNonNull(cwd, "cwd");
            Objects.requireNonNull(state, "state");
            if ((state == State.AVAILABLE) != (text != null) || text != null && text.isEmpty()) {
                throw new IllegalArgumentException("Only an available origin carries nonempty text");
            }
        }
    }

    private SegmentClassicLinuxOrigin() { }

    static Origin compute(String realname, Value cwd, long deadline) throws IOException {
        requireTime(deadline);
        SegmentClassicLinuxTokens.utf8Length(realname, deadline);
        if (cwd == null) {
            throw new IOException("Explicit current-directory data is required");
        }
        if (cwd.state() == SegmentClassicLinuxTokens.State.AVAILABLE) {
            SegmentClassicLinuxTokens.utf8Length(cwd.text(), deadline);
            if (cwd.text().isEmpty() || cwd.text().charAt(0) != '/') {
                throw new IOException("Unsupported available CWD: nonempty absolute text is required");
            }
        }
        State state;
        String text = null;
        if (realname.isEmpty()) {
            state = State.DEFERRED_GET_ORIGIN;
        } else if (realname.charAt(0) == '/') {
            state = State.AVAILABLE;
            text = parentText(realname);
        } else {
            state = switch (cwd.state()) {
                case UNAVAILABLE -> State.UNAVAILABLE;
                case UNOBSERVED -> State.UNOBSERVED;
                case AVAILABLE -> {
                    // Both inputs are bounded. Validate the complete concatenation before removing the filename.
                    String joined = cwd.text() + (cwd.text().endsWith("/") ? "" : "/") + realname;
                    SegmentClassicLinuxTokens.utf8Length(joined, deadline);
                    text = parentText(joined);
                    yield State.AVAILABLE;
                }
            };
        }
        requireTime(deadline);
        return new Origin(realname, cwd, state, text);
    }

    private static String parentText(String absolute) {
        return absolute.substring(0, Math.max(1, absolute.lastIndexOf('/')));
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("New-object origin calculation deadline elapsed");
        }
    }
}

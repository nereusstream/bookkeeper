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
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.DynamicPath;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.State;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxTokens.Value;

/** Bounded tag or effective library-path data; no filesystem observation or loader selection is implied. */
final class SegmentClassicLinuxDynamicPath {
    private static final int MAX_BYTES = 4096;

    enum Kind { CWD, DIRECTORY }

    /** CWD retains an original empty component without resolving a process working directory. */
    record Directory(Kind kind, String dirname) {
        Directory {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(dirname, "dirname");
            if ((kind == Kind.CWD) != dirname.isEmpty()) {
                throw new IllegalArgumentException("Only an original empty component represents CWD");
            }
        }
    }

    /** An unobserved list exposes no usable prefix; the original tag presence and value remain available. */
    record Expansion(DynamicPath declaration, State state, List<Directory> directories) {
        Expansion {
            Objects.requireNonNull(declaration, "declaration");
            Objects.requireNonNull(state, "state");
            directories = List.copyOf(directories);
            if (state == State.UNAVAILABLE || state == State.UNOBSERVED && !directories.isEmpty()) {
                throw new IllegalArgumentException("A list is complete or unobserved without a prefix");
            }
        }
    }

    /** Given nonsecure effective llp: unavailable means known NULL; empty and unobserved remain distinct inputs. */
    record LibraryPath(Value effective, State state, List<Directory> directories) {
        LibraryPath {
            Objects.requireNonNull(effective, "effective");
            Objects.requireNonNull(state, "state");
            directories = List.copyOf(directories);
            if (state == State.UNAVAILABLE || state == State.UNOBSERVED && !directories.isEmpty()) {
                throw new IllegalArgumentException("A library path is complete or unobserved without a prefix");
            }
        }
    }

    private record Parsed(State state, List<Directory> directories) { }

    private SegmentClassicLinuxDynamicPath() { }

    static Expansion expand(DynamicPath declaration, Value origin, Value lib, Value platform, long deadline)
            throws IOException {
        requireTime(deadline);
        if (declaration == null) {
            throw new IOException("An explicit dynamic path declaration is required");
        }
        Parsed parsed = parse(declaration.value(), false, origin, lib, platform, deadline);
        requireTime(deadline);
        return new Expansion(declaration, parsed.state(), parsed.directories());
    }

    /** Initialization origin belongs to the main object or rtld stand-in, never an arbitrary later requester. */
    static LibraryPath expandLibraryPath(Value effective, Value initializationOrigin, Value lib, Value platform,
                                         long deadline) throws IOException {
        requireTime(deadline);
        if (effective == null) {
            throw new IOException("Explicit nonsecure effective library-path data is required");
        }
        Parsed parsed = parse(effective.state() == State.AVAILABLE ? effective.text() : "", true,
                initializationOrigin, lib, platform, deadline);
        State state = effective.state() == State.UNOBSERVED ? State.UNOBSERVED : parsed.state();
        requireTime(deadline);
        return new LibraryPath(effective, state, parsed.directories());
    }

    private static Parsed parse(String raw, boolean libraryPath, Value origin, Value lib, Value platform,
                                long deadline) throws IOException {
        SegmentClassicLinuxTokens.utf8Length(raw, deadline);
        // Validate all explicit replacement data even when the list contains no nonempty component.
        SegmentClassicLinuxTokens.expand("", origin, lib, platform, deadline);
        var directories = new LinkedHashSet<Directory>();
        if (!raw.isEmpty()) {
            int start = 0;
            while (true) {
                requireTime(deadline);
                int end = start;
                while (end < raw.length() && raw.charAt(end) != ':' && (!libraryPath || raw.charAt(end) != ';')) {
                    requireTime(deadline);
                    end++;
                }
                if (end == start) {
                    directories.add(new Directory(Kind.CWD, ""));
                } else {
                    Value expanded = SegmentClassicLinuxTokens.expand(raw.substring(start, end), origin, lib,
                            platform, deadline);
                    if (expanded.state() == State.UNOBSERVED) {
                        requireTime(deadline);
                        return new Parsed(State.UNOBSERVED, List.of());
                    }
                    if (expanded.state() == State.AVAILABLE && !expanded.text().isEmpty()) {
                        directories.add(new Directory(Kind.DIRECTORY, dirname(expanded.text(), deadline)));
                    }
                }
                if (end == raw.length()) {
                    break;
                }
                start = end + 1;
            }
        }
        requireTime(deadline);
        return new Parsed(State.AVAILABLE, List.copyOf(directories));
    }

    /** Callers supply nonempty strict UTF8 bounded by the token helper or an original D2 Directory declaration. */
    static String dirname(String text, long deadline) throws IOException {
        requireTime(deadline);
        int bytes = text.getBytes(StandardCharsets.UTF_8).length;
        int end = text.length();
        while (end > 1 && text.charAt(end - 1) == '/') {
            requireTime(deadline);
            end--;
            bytes--;
        }
        if (text.charAt(end - 1) == '/') {
            return text.substring(0, end);
        }
        if (bytes == MAX_BYTES) {
            throw new IOException("Dynamic directory including its trailing slash exceeds its UTF8 byte bound");
        }
        return text.substring(0, end) + '/';
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Dynamic path expansion deadline elapsed");
        }
    }
}

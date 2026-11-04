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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Pure name reuse over supplied base-namespace order; never evidence of actual loaded objects or files. */
final class SegmentClassicLinuxLoadedNames {
    static final int MAX_OBJECTS = 4096;
    static final int MAX_NAME_REFERENCES = MAX_OBJECTS + SegmentClassicLinuxDeploymentV2.MAX_REQUESTS;

    /** Unvalidated supplied facts. Null SONAME means absent; empty SONAME is a present empty string. */
    record ObjectData(long identity, int context, String name, List<String> names, String soname,
                      boolean sonameAdded, boolean faked, boolean removed) { }

    enum State { NO_REUSE, REUSED_NAME, REUSED_SONAME }

    /** Replace only this ordinal with this immutable-name snapshot for a subsequent pure lookup. */
    record Reuse(State state, int ordinal, ObjectData replacement) { }

    private SegmentClassicLinuxLoadedNames() { }

    static Reuse lookup(List<ObjectData> objects, String query, long deadline) throws IOException {
        requireTime(deadline);
        text(query, deadline);
        if (query.isEmpty() || objects == null || objects.size() > MAX_OBJECTS) {
            throw new IOException("Explicit bounded loaded-object table and nonempty lookup name required");
        }
        var identities = new HashSet<Long>();
        int references = 0;
        // Validate the entire supplied table before selecting or copying/updating any name list.
        for (ObjectData object : objects) {
            requireTime(deadline);
            if (identities.size() == MAX_OBJECTS || object == null || object.context() < 0
                    || object.context() >= MAX_OBJECTS || !identities.add(object.identity())
                    || object.names() == null || object.names().isEmpty()
                    || object.names().size() > MAX_NAME_REFERENCES - references) {
                throw new IOException("Invalid, repeated or excessive loaded-object/name data");
            }
            references += object.names().size();
            text(object.name(), deadline);
            if (object.soname() != null) {
                text(object.soname(), deadline);
            }
            for (String name : object.names()) {
                text(name, deadline);
            }
        }
        for (int i = 0; i < objects.size(); i++) {
            requireTime(deadline);
            ObjectData object = objects.get(i);
            if (object.faked() || object.removed()) {
                continue;
            }
            boolean direct = query.equals(object.name());
            if (!direct) {
                for (String name : object.names()) {
                    requireTime(deadline);
                    if (query.equals(name)) {
                        direct = true;
                        break;
                    }
                }
            }
            boolean soname = !direct && !object.sonameAdded() && query.equals(object.soname());
            if (direct || soname) {
                if (soname && references == MAX_NAME_REFERENCES) {
                    throw new IOException("SONAME recording exceeds the loaded-name association bound");
                }
                var names = new ArrayList<>(object.names());
                if (soname) {
                    names.add(object.soname());
                }
                var replacement = new ObjectData(object.identity(), object.context(), object.name(),
                        List.copyOf(names), object.soname(), object.sonameAdded() || soname,
                        object.faked(), object.removed());
                requireTime(deadline);
                return new Reuse(direct ? State.REUSED_NAME : State.REUSED_SONAME, i, replacement);
            }
        }
        requireTime(deadline);
        return new Reuse(State.NO_REUSE, -1, null);
    }

    private static void text(String value, long deadline) throws IOException {
        requireTime(deadline);
        if (value == null || value.length() > 4096 || value.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)
                || value.getBytes(StandardCharsets.UTF_8).length > 4096) {
            throw new IOException("Unsupported loaded-object name: strict UTF8, no NUL, at most 4096 bytes");
        }
        requireTime(deadline);
    }

    private static void requireTime(long deadline) throws IOException {
        if (deadline - System.nanoTime() <= 0) {
            throw new IOException("Original loaded-name lookup deadline expired");
        }
    }
}

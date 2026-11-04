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

    record FileId(long device, long inode) { }

    /** Explicit supplied file id, never proof of fstat on an owned open fd. */
    record FileObject(ObjectData object, FileId fileId) { }

    enum State { NO_REUSE, REUSED_NAME, REUSED_SONAME, REUSED_FILE_ID }

    /** Replace only this ordinal with this immutable-name snapshot for a subsequent pure lookup. */
    record Reuse(State state, int ordinal, ObjectData replacement) { }

    private SegmentClassicLinuxLoadedNames() { }

    static Reuse lookup(List<ObjectData> objects, String query, long deadline) throws IOException {
        begin(objects, query, deadline);
        var identities = new HashSet<Long>();
        int references = 0;
        for (ObjectData object : objects) {
            references = validate(object, identities, references, deadline);
        }
        for (int i = 0; i < objects.size(); i++) {
            requireTime(deadline);
            ObjectData object = objects.get(i);
            if (object.faked() || object.removed()) {
                continue;
            }
            boolean direct = query.equals(object.name()) || hasAlias(object, query, deadline);
            boolean soname = !direct && !object.sonameAdded() && query.equals(object.soname());
            if (direct || soname) {
                var replacement = remember(object, soname ? object.soname() : null,
                        object.sonameAdded() || soname, references, deadline);
                return new Reuse(direct ? State.REUSED_NAME : State.REUSED_SONAME, i, replacement);
            }
        }
        requireTime(deadline);
        return new Reuse(State.NO_REUSE, -1, null);
    }

    /** Ordinary non-OPENEXEC branch only. No fd is opened/closed, and no preceding name miss is proved. */
    static Reuse lookupFile(List<FileObject> objects, FileId opened, String query, long deadline) throws IOException {
        begin(objects, query, deadline);
        if (opened == null) {
            throw new IOException("Explicit opened-file id data is required");
        }
        var identities = new HashSet<Long>();
        int references = 0;
        for (FileObject object : objects) {
            requireTime(deadline);
            if (object == null || object.fileId() == null) {
                throw new IOException("Missing loaded-object file id data");
            }
            references = validate(object.object(), identities, references, deadline);
        }
        for (int i = 0; i < objects.size(); i++) {
            requireTime(deadline);
            FileObject file = objects.get(i);
            ObjectData object = file.object();
            // glibc's file-id branch skips removed, but does not additionally skip faked.
            if (!object.removed() && file.fileId().equals(opened)) {
                var replacement = remember(object, hasAlias(object, query, deadline) ? null : query,
                        object.sonameAdded(), references, deadline);
                return new Reuse(State.REUSED_FILE_ID, i, replacement);
            }
        }
        requireTime(deadline);
        return new Reuse(State.NO_REUSE, -1, null);
    }

    private static void begin(List<?> objects, String query, long deadline) throws IOException {
        requireTime(deadline);
        text(query, deadline);
        if (query.isEmpty() || objects == null || objects.size() > MAX_OBJECTS) {
            throw new IOException("Explicit bounded loaded-object table and nonempty lookup name required");
        }
    }

    /** Entire table is validated before selecting or copying/updating any name list in either branch. */
    private static int validate(ObjectData object, HashSet<Long> identities, int references, long deadline)
            throws IOException {
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
        return references;
    }

    private static boolean hasAlias(ObjectData object, String query, long deadline) throws IOException {
        for (String name : object.names()) {
            requireTime(deadline);
            if (query.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static ObjectData remember(ObjectData object, String addition, boolean sonameAdded, int references,
                                       long deadline) throws IOException {
        if (addition != null && references == MAX_NAME_REFERENCES) {
            throw new IOException("Alias recording exceeds the loaded-name association bound");
        }
        var names = new ArrayList<>(object.names());
        if (addition != null) {
            names.add(addition);
        }
        var replacement = new ObjectData(object.identity(), object.context(), object.name(), List.copyOf(names),
                object.soname(), sonameAdded, object.faked(), object.removed());
        requireTime(deadline);
        return replacement;
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

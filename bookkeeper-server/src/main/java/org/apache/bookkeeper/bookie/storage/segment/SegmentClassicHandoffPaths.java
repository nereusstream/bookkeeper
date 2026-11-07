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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;

/** Shared native path checks for the fixed handoff reader and publisher. Not complete Unix/ACL authority. */
final class SegmentClassicHandoffPaths {
    record Identity(long device, long inode, long uid, long gid, int mode, long links,
                    long size, FileTime modified, FileTime changed, boolean directory) {
        Identity parent() {
            return new Identity(device, inode, uid, gid, mode, 0, 0, null, null, directory);
        }
    }

    private final long oldUid;
    private final long targetUid;
    private final long materialOwnerUid;
    private final NativeIOImpl nativeIo = new NativeIOImpl();

    SegmentClassicHandoffPaths(long oldUid, long targetUid, long materialOwnerUid) {
        this.oldUid = oldUid;
        this.targetUid = targetUid;
        this.materialOwnerUid = materialOwnerUid;
    }

    Identity privateFile(Path path, int minimum, int maximum) throws IOException {
        Identity identity = protectedIdentity(path);
        if (identity.directory() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || identity.links() != 1 || (identity.mode() & 077) != 0 || (identity.mode() & 0400) == 0
                || !trustedOwner(identity.uid()) || identity.size() < minimum || identity.size() > maximum) {
            throw new IOException("Handoff material must be a private, singly linked regular file: " + path);
        }
        return identity;
    }

    void requireDirectory(Path path) throws IOException {
        Identity identity = protectedIdentity(path);
        if (!identity.directory() || !trustedOwner(identity.uid()) || (identity.mode() & 077) != 0) {
            throw new IOException("Handoff supervisor directory must be private to its trusted owner");
        }
        parents(path);
    }

    List<Identity> parents(Path file) throws IOException {
        var identities = new ArrayList<Identity>();
        Identity child = protectedIdentity(file);
        for (Path parent = file.getParent(); parent != null; parent = parent.getParent()) {
            Identity identity = protectedIdentity(parent);
            if (!identity.directory() || !trustedOwner(identity.uid())) {
                throw new IOException("Handoff parent chain is not an existing trusted directory");
            }
            if ((identity.mode() & 022) != 0 && ((identity.mode() & 01000) == 0
                    || child.uid() == oldUid || identity.uid() == oldUid)) {
                throw new IOException("Handoff parent permits replacing the protected path");
            }
            identities.add(identity.parent());
            child = identity;
        }
        return List.copyOf(identities);
    }

    Identity protectedIdentity(Path path) throws IOException {
        Identity before = fileIdentity(path);
        if (nativeIo.hasExtendedAcl(path.toString())) {
            throw new IOException("Extended ACL on a protected Classic handoff path: " + path);
        }
        if (!before.equals(fileIdentity(path))) {
            throw new IOException("Classic handoff path changed during its ACL query: " + path);
        }
        return before;
    }

    static Identity fileIdentity(Path path) throws IOException {
        if (!path.equals(path.toRealPath())) {
            throw new IOException("Handoff path must contain no symbolic aliases");
        }
        return loadingIdentity(path);
    }

    /** Raw leaf identity only. The Linux loading observer must validate each original parent before using it. */
    static Identity loadingIdentity(Path path) throws IOException {
        try {
            Map<String, Object> attributes = Files.readAttributes(path, "unix:*", LinkOption.NOFOLLOW_LINKS);
            return new Identity(((Number) attributes.get("dev")).longValue(),
                    ((Number) attributes.get("ino")).longValue(), unsigned(attributes.get("uid")),
                    unsigned(attributes.get("gid")), ((Number) attributes.get("mode")).intValue(),
                    ((Number) attributes.get("nlink")).longValue(), ((Number) attributes.get("size")).longValue(),
                    (FileTime) attributes.get("lastModifiedTime"), (FileTime) attributes.get("ctime"),
                    Boolean.TRUE.equals(attributes.get("isDirectory")));
        } catch (UnsupportedOperationException unsupported) {
            throw new IOException("Native Unix file identity is required for handoff material I/O", unsupported);
        }
    }

    private boolean trustedOwner(long uid) {
        return uid == 0 || uid == targetUid || uid == materialOwnerUid;
    }

    private static long unsigned(Object uid) {
        return uid instanceof Integer value ? Integer.toUnsignedLong(value) : ((Number) uid).longValue();
    }

}

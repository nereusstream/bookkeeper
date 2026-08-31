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

package org.apache.bookkeeper.common.profile.startup;

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.DUPLICATE_OR_UNORDERED_VALUE;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.EMPTY_COLLECTION;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.MISSING_REQUIRED_ROOT;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Typed new-BookieId/new-roots/new-incarnation/new-credential-scope fallback manifest.
 *
 * <p>Paths and access decisions are facts supplied to the harness. This type never reads or changes OS permissions.
 */
public record NewScopeFallbackManifest(
        String oldBookieId,
        String newBookieId,
        StorageIncarnation oldStorageIncarnation,
        StorageIncarnation newStorageIncarnation,
        List<StorageRoot> oldRoots,
        List<StorageRoot> newRoots,
        CredentialScope oldCredentialScope,
        CredentialScope newCredentialScope,
        OldScopeDisposition oldScopeDisposition,
        AccessLevel oldCredentialAccessToNewScope,
        AccessLevel newCredentialAccessToNewScope) {

    public enum RootKind {
        JOURNAL,
        LEDGER,
        INDEX,
        ARENA
    }

    public enum OldScopeDisposition {
        DRAINED,
        READ_ONLY,
        DECOMMISSIONED,
        WRITABLE,
        UNKNOWN
    }

    public enum AccessLevel {
        NONE,
        READ_ONLY,
        READ_WRITE
    }

    public NewScopeFallbackManifest {
        oldBookieId = StartupFactSupport.boundedText(oldBookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
        newBookieId = StartupFactSupport.boundedText(newBookieId, BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES);
        oldStorageIncarnation = StartupFactSupport.requireNonNull(oldStorageIncarnation);
        newStorageIncarnation = StartupFactSupport.requireNonNull(newStorageIncarnation);
        oldRoots = strictRoots(oldRoots);
        newRoots = strictRoots(newRoots);
        oldCredentialScope = StartupFactSupport.requireNonNull(oldCredentialScope);
        newCredentialScope = StartupFactSupport.requireNonNull(newCredentialScope);
        oldScopeDisposition = StartupFactSupport.requireNonNull(oldScopeDisposition);
        oldCredentialAccessToNewScope = StartupFactSupport.requireNonNull(oldCredentialAccessToNewScope);
        newCredentialAccessToNewScope = StartupFactSupport.requireNonNull(newCredentialAccessToNewScope);
    }

    public boolean permitsNewScopeWritable() {
        if (oldBookieId.equals(newBookieId)
                || oldStorageIncarnation.equals(newStorageIncarnation)
                || oldCredentialScope.equals(newCredentialScope)
                || oldCredentialAccessToNewScope != AccessLevel.NONE
                || newCredentialAccessToNewScope != AccessLevel.READ_WRITE
                || !(oldScopeDisposition == OldScopeDisposition.DRAINED
                        || oldScopeDisposition == OldScopeDisposition.READ_ONLY
                        || oldScopeDisposition == OldScopeDisposition.DECOMMISSIONED)) {
            return false;
        }
        Set<String> oldPaths = new HashSet<>();
        for (StorageRoot root : oldRoots) {
            oldPaths.add(root.opaquePath());
        }
        return newRoots.stream().noneMatch(root -> oldPaths.contains(root.opaquePath()));
    }

    public boolean rejectsOldIdentity(String bookieId, StorageIncarnation incarnation, CredentialScope credential) {
        return oldBookieId.equals(bookieId)
                || oldStorageIncarnation.equals(incarnation)
                || oldCredentialScope.equals(credential);
    }

    private static List<StorageRoot> strictRoots(List<StorageRoot> roots) {
        if (roots == null) {
            throw new StartupReadinessValidationException(
                    StartupReadinessValidationException.Reason.NULL_FIELD);
        }
        if (roots.isEmpty()) {
            throw new StartupReadinessValidationException(EMPTY_COLLECTION);
        }
        List<StorageRoot> copy = new ArrayList<>(roots.size());
        Set<RootKind> kinds = EnumSet.noneOf(RootKind.class);
        String previousPath = null;
        for (StorageRoot root : roots) {
            StartupFactSupport.requireNonNull(root);
            if (previousPath != null && previousPath.compareTo(root.opaquePath()) >= 0) {
                throw new StartupReadinessValidationException(DUPLICATE_OR_UNORDERED_VALUE);
            }
            if (!kinds.add(root.kind())) {
                throw new StartupReadinessValidationException(DUPLICATE_OR_UNORDERED_VALUE);
            }
            previousPath = root.opaquePath();
            copy.add(root);
        }
        if (!kinds.equals(EnumSet.allOf(RootKind.class))) {
            throw new StartupReadinessValidationException(MISSING_REQUIRED_ROOT);
        }
        return List.copyOf(copy);
    }

    public record StorageRoot(RootKind kind, String opaquePath) {
        public static final int MAX_PATH_BYTES = 1024;

        public StorageRoot {
            kind = StartupFactSupport.requireNonNull(kind);
            opaquePath = StartupFactSupport.boundedText(opaquePath, MAX_PATH_BYTES);
        }
    }

    public record CredentialScope(String identity) {
        public static final int MAX_IDENTITY_BYTES = 255;

        public CredentialScope {
            identity = StartupFactSupport.boundedText(identity, MAX_IDENTITY_BYTES);
        }
    }
}

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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.profile.ProfileLedgerMetadataStore;
import org.apache.bookkeeper.net.BookieId;

/** Fixed native binding preflight before Classic storage side effects. It never creates a path or repairs authority. */
public final class ClassicRouteStartup {
    private ClassicRouteStartup() { }

    /** One startup episode only; this token is neither a persistent incarnation nor a common-owner permission. */
    public static final class Context {
        private final RegistrationManager manager;
        private final Snapshot scope;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private Context(RegistrationManager manager, Snapshot scope) {
            this.manager = manager;
            this.scope = scope;
        }

        public void consume(ServerConfiguration configuration, RegistrationManager registration) throws IOException {
            if (manager != registration || !scope.equals(snapshot(configuration))) {
                throw new IOException("Classic startup context belongs to another manager, Bookie or root set");
            }
            if (!consumed.compareAndSet(false, true)) {
                throw new IOException("Classic startup context was already consumed by another startup episode");
            }
            requireNoMarkers(scope);
        }
    }

    /** COMMON FOUND always needs its explicitly commissioned owner; OFF cannot select the legacy path instead. */
    public static Context preflight(ServerConfiguration configuration, RegistrationManager manager)
            throws IOException {
        if (manager == null) {
            throw new IOException("A null registration manager cannot prove an unbound Classic scope");
        }
        Snapshot scope = snapshot(configuration);
        var read = manager.readBookieRouteBinding(scope.bookie());
        if (read == null) {
            throw new IOException("Registration backend violated the common binding cold-read contract");
        }
        if (read.status() != BookieRouteBinding.ReadStatus.ABSENT) {
            throw new IOException("Classic startup requires authoritative native absence or its common owner: "
                    + read.status() + ": " + read.detail());
        }
        requireNoMarkers(scope);
        return new Context(manager, scope);
    }

    /** The independent v1 Profile graph cannot adopt a native common binding, including a matching one. */
    static void requireUnboundProfile(ProfileLedgerMetadataStore metadata, BookieId bookie, Duration timeout,
            IsolatedSegmentConfig configuration) throws IOException {
        var read = metadata.readBookieRouteBinding(bookie, timeout);
        if (read == null || read.status() != BookieRouteBinding.ReadStatus.ABSENT) {
            throw new IOException("Independent Profile startup requires authoritative native absence: "
                    + (read == null ? "null backend result" : read.status() + ": " + read.detail()));
        }
        for (Path root : List.of(configuration.directory(), SegmentDerivedIndex.directory(configuration))) {
            requireAbsentMarker(root.resolve(SegmentClassicRootBindings.FILE_NAME));
            // RocksDB has a regular CURRENT file, also named current on case-insensitive filesystems.
            // Only a retained Classic directory can contain the nested marker.
            Path current = root.resolve("current");
            BasicFileAttributes attributes;
            try {
                attributes = Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            } catch (NoSuchFileException absent) {
                continue;
            }
            if (attributes.isSymbolicLink()) {
                throw new IOException("Profile root has an unverified current directory link");
            }
            if (attributes.isDirectory()) {
                requireAbsentMarker(current.resolve(SegmentClassicRootBindings.FILE_NAME));
            }
        }
    }

    private record Snapshot(BookieId bookie, List<String> journal, List<String> ledger, List<String> index) { }

    private static Snapshot snapshot(ServerConfiguration configuration) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        String[] index = configuration.getIndexDirNames();
        return new Snapshot(BookieImpl.getBookieId(configuration), List.of(configuration.getJournalDirNames()),
                List.of(configuration.getLedgerDirNames()), index == null ? null : List.of(index));
    }

    private static void requireNoMarkers(Snapshot scope) throws IOException {
        List<String> roots = new ArrayList<>(scope.journal());
        roots.addAll(scope.ledger());
        if (scope.index() != null) {
            roots.addAll(scope.index());
        }
        for (String root : roots) {
            Path marker = Path.of(root).toAbsolutePath().normalize().resolve("current")
                    .resolve(SegmentClassicRootBindings.FILE_NAME);
            requireAbsentMarker(marker);
        }
    }

    private static void requireAbsentMarker(Path marker) throws IOException {
        try {
            Files.readAttributes(marker, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException absent) {
            return;
        }
        throw new IOException("Storage root retains a common binding without a matching common owner");
    }
}

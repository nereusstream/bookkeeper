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
package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/** Explicit, one-use local Classic episode. It owns no cluster registration or metadata authority. */
public final class FreshClassicOwner extends OwnedClassicLifecycle {
    public enum State { NEW, STARTING, RUNNING, CLOSING, DRAINED }

    public record Status(State state, int activeCalls, int pendingOperations, Throwable initializationFailure,
                         Throwable operationFailure, Throwable observationFailure, Throwable closeFailure) {
        public boolean drained() {
            return state == State.DRAINED;
        }
    }

    /** Never public, never a ClassicRouteStartup ABSENT context, and never a transfer of the root lease. */
    static final class Permit {
        private final FreshClassicOwner owner;
        private final BookieImpl bookie;
        private final FreshClassicRoots roots;
        private boolean consumed;

        private Permit(FreshClassicOwner owner, BookieImpl bookie, FreshClassicRoots roots) {
            this.owner = owner;
            this.bookie = bookie;
            this.roots = roots;
        }

        void consume(BookieImpl candidate) throws IOException {
            synchronized (owner.gate) {
                if (consumed || candidate != bookie || owner.implementation != candidate || owner.roots != roots
                        || candidate.conf != owner.configuration || owner.initializingThread != Thread.currentThread()
                        || !owner.initializing || owner.closing) {
                    throw new IOException("Fresh NO_CLUSTER_AUTHORITY permit does not match its owned episode");
                }
                consumed = true;
            }
            roots.verify(owner.configuration);
        }
    }

    private final Object gate = new Object();
    private final ServerConfiguration configuration;
    private final StatsLogger stats;
    private final FreshClassicHandle handle;
    private FreshClassicRoots roots;
    private volatile BookieImpl implementation;
    private volatile boolean closing;
    private boolean startAttempted;
    private boolean initializing;
    private Thread initializingThread;
    private boolean running;
    private boolean resourcesDrained;
    private BookieThread closeWorker;
    private boolean workerStartReturned;
    private boolean workerStartFailed;
    private Throwable initializationFailure;
    private Throwable lastCloseFailure;

    private FreshClassicOwner(ServerConfiguration configuration, StatsLogger stats) {
        this.configuration = configuration;
        this.stats = stats;
        handle = new FreshClassicHandle(this);
    }

    public static FreshClassicOwner create(ServerConfiguration configuration) throws IOException {
        return create(configuration, NullStatsLogger.INSTANCE);
    }

    public static FreshClassicOwner create(ServerConfiguration configuration, StatsLogger stats) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        var resolved = configuration.interpolatedConfiguration();
        ServerConfiguration frozen = new ServerConfiguration();
        for (int i = frozen.getNumberOfConfigurations() - 1; i >= 0; i--) {
            var inherited = frozen.getConfiguration(i);
            if (inherited != frozen.getInMemoryConfiguration()) {
                frozen.removeConfiguration(inherited);
            }
        }
        frozen.setInterpolator(new org.apache.commons.configuration2.interpol.ConfigurationInterpolator());
        frozen.setListDelimiterHandler(new org.apache.commons.configuration2.convert.DisabledListDelimiterHandler());
        for (var keys = resolved.getKeys(); keys.hasNext();) {
            String key = keys.next();
            frozen.setProperty(key, freeze(resolved.getProperty(key)));
        }
        validate(frozen);
        frozen.setBookieId(BookieImpl.getBookieId(frozen).toString());
        return new FreshClassicOwner(frozen, Objects.requireNonNull(stats, "stats"));
    }

    private static Object freeze(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Character
                || value instanceof Enum<?> || value instanceof Class<?> || value instanceof Byte
                || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double || value instanceof java.math.BigInteger
                || value instanceof java.math.BigDecimal) {
            return value;
        }
        List<Object> copy = new ArrayList<>();
        if (value instanceof List<?> values) {
            for (Object member : values) {
                copy.add(freeze(member));
            }
        } else if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                copy.add(freeze(Array.get(value, i)));
            }
        } else {
            throw new IllegalArgumentException("Fresh configuration contains a mutable unsupported value type");
        }
        return List.copyOf(copy);
    }

    private static void validate(ServerConfiguration conf) throws IOException {
        Journal.validateFreshConfiguration(conf);
        Object cache = conf.getProperty("gcEntryLogMetadataCachePath");
        if (!InterleavedLedgerStorage.class.getName().equals(conf.getLedgerStorageClass())
                || conf.isEntryLogPerLedgerEnabled() || conf.isGcEntryLogMetadataCacheEnabled()
                || cache != null && !"".equals(cache) || conf.isSanityCheckMetricsEnabled()
                || conf.isPersistBookieStatusEnabled()) {
            throw new IllegalArgumentException("Fresh requires builtin single-log Interleaved storage, local state"
                    + " and the default in-memory GC map");
        }
        if (conf.getProperty("ledgerDirectories") == null || conf.getLedgerDirNames().length == 0) {
            throw new IllegalArgumentException("Fresh requires explicit nonempty ledger roots");
        }
        for (String directory : conf.getLedgerDirNames()) {
            if (directory == null || directory.isBlank()) {
                throw new IllegalArgumentException("Fresh ledger roots cannot be empty");
            }
        }
        if (conf.getIndexDirNames() != null) {
            if (conf.getIndexDirNames().length == 0) {
                throw new IllegalArgumentException("Fresh explicit index roots cannot be empty");
            }
            for (String directory : conf.getIndexDirNames()) {
                if (directory == null || directory.isBlank()) {
                    throw new IllegalArgumentException("Fresh index roots cannot be empty");
                }
            }
        }
        try {
            conf.validate();
        } catch (org.apache.commons.configuration2.ex.ConfigurationException failure) {
            throw new IOException("Invalid frozen fresh configuration", failure);
        }
    }

    public void start() throws IOException, InterruptedException, BookieException {
        synchronized (gate) {
            if (startAttempted || closing) {
                throw new IOException("Fresh owner start is one-use");
            }
            startAttempted = true;
            initializing = true;
            initializingThread = Thread.currentThread();
        }
        boolean success = false;
        try {
            checkStarting();
            roots = new FreshClassicRoots(configuration);
            roots.acquire();
            validatePhysicalLayout();
            checkStarting();
            roots.prepareCurrentDirectories();
            checkStarting();
            implementation = BookieImpl.forFresh(configuration, stats, this);
            implementation.initializeForFresh(new Permit(this, implementation, roots));
            implementation.startForFresh();
            synchronized (gate) {
                checkStarting();
                implementation.openAdmissionForFresh();
                running = true;
            }
            success = true;
        } catch (IOException | InterruptedException | BookieException | RuntimeException | Error failure) {
            synchronized (gate) {
                initializationFailure = failure;
            }
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        } finally {
            synchronized (gate) {
                initializing = false;
                initializingThread = null;
                gate.notifyAll();
            }
            if (!success) {
                requestClose();
            }
        }
    }

    private void validatePhysicalLayout() throws IOException {
        requireDistinct(configuration.getJournalDirs());
        requireDistinct(configuration.getLedgerDirs());
        if (configuration.getIndexDirs() != null) {
            requireDistinct(configuration.getIndexDirs());
        }
    }

    private static void requireDistinct(java.io.File[] directories) throws IOException {
        for (int i = 0; i < directories.length; i++) {
            for (int j = 0; j < i; j++) {
                if (java.nio.file.Files.isSameFile(directories[i].toPath(), directories[j].toPath())) {
                    throw new IOException("Fresh configured members of one root role must be physically distinct");
                }
            }
        }
    }

    @Override
    void checkStarting() throws IOException {
        if (closing) {
            throw new IOException("Fresh close was requested during initialization");
        }
    }

    @Override
    boolean isClosing() {
        return closing;
    }

    public FreshClassicHandle bookie() {
        synchronized (gate) {
            if (!running || closing) {
                throw new IllegalStateException("Fresh handle is available only after successful local startup");
            }
            return handle;
        }
    }

    BookieImpl implementation() {
        synchronized (gate) {
            if (!running || closing) {
                throw new RejectedExecutionException("Fresh admission is closed");
            }
            return implementation;
        }
    }

    @Override
    public void requestClose() {
        synchronized (gate) {
            closing = true;
            if (resourcesDrained || closeWorker != null && (closeWorker.isAlive()
                    || !workerStartReturned && !workerStartFailed)) {
                return;
            }
            if (closeWorker != null && closeWorker.getState() != Thread.State.TERMINATED
                    && !(workerStartFailed && closeWorker.getState() == Thread.State.NEW)) {
                return;
            }
            closeWorker = new BookieThread(this::drain, "FreshClassicClose-" + configuration.getBookiePort());
            workerStartReturned = false;
            workerStartFailed = false;
            try {
                closeWorker.start();
                workerStartReturned = true;
            } catch (Throwable failure) {
                workerStartFailed = true;
                lastCloseFailure = failure;
            }
        }
    }

    private void drain() {
        try {
            synchronized (gate) {
                while (initializing) {
                    gate.wait();
                }
            }
            BookieImpl bookie = implementation;
            if (bookie != null) {
                BookieImpl.FreshDrainResult result = bookie.closeForFresh();
                if (!result.drained()) {
                    synchronized (gate) {
                        lastCloseFailure = result.closeFailure();
                    }
                    return;
                }
            }
            if (roots != null) {
                FreshClassicRoots.FreshDrainResult result = roots.closeForFresh();
                if (!result.drained()) {
                    synchronized (gate) {
                        lastCloseFailure = result.closeFailure();
                    }
                    return;
                }
            }
            synchronized (gate) {
                resourcesDrained = true;
                lastCloseFailure = null;
            }
        } catch (Throwable failure) {
            synchronized (gate) {
                lastCloseFailure = failure;
            }
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Status closeAndAwait(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Fresh close timeout cannot be negative");
        }
        synchronized (gate) {
            if (Thread.currentThread() == initializingThread || Thread.currentThread() == closeWorker) {
                throw new IllegalStateException("Fresh lifecycle activity cannot await itself");
            }
        }
        BookieImpl bookie = implementation;
        if (bookie != null && bookie.isCurrentResourceActivityForFresh()) {
            throw new IllegalStateException("Fresh resource activity cannot await its own owner");
        }
        long waitStarted = System.nanoTime();
        requestClose();
        Thread worker;
        synchronized (gate) {
            worker = closeWorker;
        }
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            nanos = Long.MAX_VALUE;
        }
        try {
            long remaining = nanos - (System.nanoTime() - waitStarted);
            if (remaining > 0 && worker != null) {
                TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
        return status();
    }

    public Status status() {
        synchronized (gate) {
            State state = closing ? State.CLOSING : running ? State.RUNNING
                    : initializing ? State.STARTING : State.NEW;
            if (resourcesDrained && workerStartReturned && closeWorker != null && !closeWorker.isAlive()
                    && closeWorker.getState() == Thread.State.TERMINATED) {
                state = State.DRAINED;
            }
            BookieImpl bookie = implementation;
            return new Status(state, bookie == null ? 0 : bookie.activeCallsForFresh(),
                    bookie == null ? 0 : bookie.pendingOperationsForFresh(), initializationFailure,
                    bookie == null ? null : bookie.operationFailureForFresh(),
                    bookie == null ? null : bookie.observationFailureForFresh(), lastCloseFailure);
        }
    }
}

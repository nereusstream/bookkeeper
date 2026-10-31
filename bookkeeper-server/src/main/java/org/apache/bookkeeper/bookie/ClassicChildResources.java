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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.bookkeeper.bookie.storage.segment.ClassicRouteStartup;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicChild;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.KeeperException;

/** Stable assembly holder. Its storage facts never establish a qualified child, registration or signed drain. */
public final class ClassicChildResources extends OwnedClassicLifecycle {
    public enum State { NEW, INITIALIZING, STORAGE_STARTED, TCP_BOUND, SERVING, CLOSING, DRAINED }

    public record Status(State state, Throwable initializationFailure, Throwable operationFailure,
                         Throwable closeFailure) { }

    public record ServiceEpisode(long session, long birth, boolean readOnly) { }

    /** Internal resource permission, never a START context; its producer must already own this exact episode. */
    static final class Permit {
        private final ClassicChildResources owner;
        private final BookieImpl bookie;
        private boolean consumed;

        private Permit(ClassicChildResources owner, BookieImpl bookie) {
            this.owner = owner;
            this.bookie = bookie;
        }

        void consume(BookieImpl candidate) throws IOException {
            synchronized (owner.gate) {
                if (consumed || owner.bookie != candidate || candidate != bookie
                        || candidate.conf != owner.configuration || owner.initializingThread != Thread.currentThread()
                        || !owner.initializing || owner.closing) {
                    throw new IOException("Historical resource permission does not match its one owned consumer");
                }
                if (!owner.fixtureOnly) {
                    owner.productionContext.requireBookie(owner, candidate);
                }
                consumed = true;
            }
        }
    }

    private final Object gate = new Object();
    private final Object closeLock = new Object();
    private final ClassicHistoryFiles reads = new ClassicHistoryFiles();
    private ServerConfiguration configuration;
    private BookieImpl bookie;
    private ClassicGcMetadata metadata;
    private BookieServer listener;
    private boolean listenerAttempted;
    private boolean listenerBound;
    private boolean serviceAttempted;
    private boolean serviceStarted;
    private boolean serviceRegistered;
    private SegmentClassicChild.StartContext productionContext;
    private boolean attempted;
    private boolean fixtureOnly;
    private boolean initializing;
    private Thread initializingThread;
    private volatile boolean closing;
    private boolean storageStarted;
    private boolean drained;
    private Throwable initializationFailure;
    private Throwable operationFailure;
    private Throwable closeFailure;

    private ClassicChildResources() { }

    /** Heap-only holder; only the original child can transfer a production START context. */
    public static ClassicChildResources prepare() {
        return new ClassicChildResources();
    }

    /** Package file/ZK resource fixture only; never constructs or exports a production StartContext. */
    void initializeForIoTests(ServerConfiguration conf, RegistrationManager registration,
            AbstractZkLedgerManager manager, ZkLedgerUnderreplicationManager replication)
            throws IOException, InterruptedException, BookieException {
        initializeStorage(conf, registration, manager, replication, true);
    }

    public void initialize(SegmentClassicChild.StartContext context) throws Exception {
        Objects.requireNonNull(context, "context").initialize(this);
    }

    /** Concrete callback of the original StartContext; public visibility exists solely for the package boundary. */
    public void initialize(SegmentClassicChild.StartContext context, ServerConfiguration conf,
            ZKRegistrationManager registration, AbstractZkLedgerManager manager,
            ZkLedgerUnderreplicationManager replication) throws IOException, InterruptedException, BookieException {
        context.accept(this, conf, registration, manager, replication);
        productionContext = context;
        initializeStorage(conf, registration, manager, replication, false);
    }

    private void initializeStorage(ServerConfiguration conf, RegistrationManager registration,
            AbstractZkLedgerManager manager, ZkLedgerUnderreplicationManager replication, boolean fixture)
            throws IOException, InterruptedException, BookieException {
        synchronized (gate) {
            if (attempted || closing) {
                throw new IOException("Historical resources initialize only once");
            }
            attempted = true;
            fixtureOnly = fixture;
            initializing = true;
            initializingThread = Thread.currentThread();
            configuration = Objects.requireNonNull(conf, "configuration");
        }
        try {
            checkStarting();
            ClassicRouteStartup.preflight(conf, registration).consume(conf, registration);
            requireExistingCookies(conf, registration);
            checkStarting();
            metadata = new ClassicGcMetadata(manager, replication,
                    conf.getGcOverreplicatedLedgerMaxConcurrentRequests());
            bookie = BookieImpl.forClassicChild(conf, NullStatsLogger.INSTANCE, this, registration);
            if (!fixtureOnly) {
                productionContext.bindBookie(this, bookie);
            }
            bookie.initializeForClassicChild(new Permit(this, bookie), metadata);
            checkStarting();
            bookie.readJournal();
            checkStarting();
            bookie.startForFresh();
            synchronized (gate) {
                checkStarting();
                bookie.openAdmissionForFresh();
                storageStarted = true;
            }
        } catch (IOException | InterruptedException | BookieException | RuntimeException | Error problem) {
            initializationFailure = problem;
            if (problem instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw problem;
        } finally {
            synchronized (gate) {
                initializing = false;
                initializingThread = null;
                gate.notifyAll();
            }
        }
    }

    private void requireExistingCookies(ServerConfiguration conf, RegistrationManager registration)
            throws IOException, BookieException {
        String cluster = registration.getClusterInstanceId();
        if (cluster == null || cluster.isBlank()) {
            throw new IOException("Historical child requires its existing cluster identity");
        }
        Cookie expected = Cookie.generateCookie(conf).setInstanceId(cluster).build();
        Cookie remote = Cookie.readFromRegistrationManager(registration, conf).getValue();
        expected.verify(remote);
        List<File> directories = new ArrayList<>(List.of(conf.getJournalDirs()));
        directories.addAll(List.of(conf.getLedgerDirs()));
        if (conf.getIndexDirs() != null) {
            directories.addAll(List.of(conf.getIndexDirs()));
        }
        for (File directory : directories) {
            checkStarting();
            Path root = directory.toPath();
            ClassicHistoryFiles.requireDirectory(root);
            Cookie local = reads.readCookie(root.resolve("current"));
            expected.verify(local);
            remote.verify(local);
        }
    }

    /** Direct package listener-stage fixture only. No processor, START, activation or READY. */
    void bindListenerForIoTests() throws IOException, KeeperException, InterruptedException, BookieException {
        if (!fixtureOnly) {
            throw new IOException("Only direct fixtures use this listener entry");
        }
        bindListener();
    }

    private void bindListener() throws IOException, KeeperException, InterruptedException, BookieException {
        synchronized (gate) {
            if ((!fixtureOnly && productionContext == null) || !storageStarted
                    || initializing || closing || listenerAttempted) {
                throw new IOException("Fixture listener stage requires its one original historical graph");
            }
            listenerAttempted = true;
            initializing = true;
            initializingThread = Thread.currentThread();
        }
        try {
            checkStarting();
            listener = BookieServer.prepareForClassicChild(configuration, bookie, NullStatsLogger.INSTANCE,
                    bookie.getAllocator());
            checkStarting();
            listener.initializeListenerForClassicChild();
            synchronized (gate) {
                checkStarting();
                if (!listener.isListenerBoundForClassicChild()) {
                    throw new IOException("Fixture listener did not establish an actual bind");
                }
                listenerBound = true;
            }
        } catch (IOException | KeeperException | InterruptedException | BookieException
                | RuntimeException | Error problem) {
            initializationFailure = problem;
            if (problem instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw problem;
        } finally {
            synchronized (gate) {
                initializing = false;
                initializingThread = null;
                gate.notifyAll();
            }
        }
    }

    /** Direct resource integration only; this never constructs a production StartContext or qualified READY. */
    void startNetworkForIoTests() throws IOException, KeeperException, InterruptedException, BookieException,
            org.apache.bookkeeper.tls.SecurityException {
        if (!fixtureOnly) {
            throw new IOException("Only direct fixtures use this network entry");
        }
        prepareNetwork();
        activateNetwork();
    }

    public void prepareNetwork() throws IOException, KeeperException, InterruptedException, BookieException,
            org.apache.bookkeeper.tls.SecurityException {
        if (!fixtureOnly) {
            if (productionContext == null) {
                throw new IOException("Original production START context required");
            }
            productionContext.requireBookie(this, bookie);
        }
        if (!listenerAttempted) {
            bindListener();
        }
        synchronized (gate) {
            if (initializing || isClosing() || !listenerBound || serviceAttempted) {
                throw new IOException("Fixture network activation requires its one original bound storage graph");
            }
            serviceAttempted = true;
            initializing = true;
            initializingThread = Thread.currentThread();
        }
        try {
            listener.initializeProcessorForClassicChild();
            checkStarting();
            try {
                bookie.getStateManager().registerAfterBindForClassicChild().get();
            } catch (java.util.concurrent.ExecutionException failed) {
                throw new IOException("Original controlled registration failed", failed.getCause());
            }
            checkStarting();
            bookie.getStateManager().requireCurrentRegistrationForClassicChild();
            synchronized (gate) {
                checkStarting();
                serviceRegistered = true;
            }
        } catch (IOException | InterruptedException | org.apache.bookkeeper.tls.SecurityException
                | RuntimeException | Error problem) {
            initializationFailure = problem;
            if (problem instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw problem;
        } finally {
            synchronized (gate) {
                initializing = false;
                initializingThread = null;
                gate.notifyAll();
            }
        }
    }

    public void activateNetwork() throws IOException {
        if (!fixtureOnly) {
            if (productionContext == null) {
                throw new IOException("Original production START context required");
            }
            productionContext.requireActivation(this, bookie);
        }
        synchronized (gate) {
            checkStarting();
            if (!serviceRegistered || serviceStarted || initializing) {
                throw new IOException("Original Classic network is not ready for its one activation");
            }
            bookie.getStateManager().requireCurrentRegistrationForClassicChild();
            listener.activateForClassicChild();
            if (!listener.isRunning()) {
                throw new IOException("Original Classic network did not establish its actual service");
            }
            serviceStarted = true;
        }
    }

    public ServiceEpisode registrationEpisode() throws IOException {
        if (!fixtureOnly) {
            if (productionContext == null) {
                throw new IOException("Original production START context required");
            }
            productionContext.requireBookie(this, bookie);
        }
        synchronized (gate) {
            checkStarting();
            if (!serviceRegistered || !bookie.isRunning() || !listener.isListenerBoundForClassicChild()) {
                throw new IOException("Original Bookie/listener/registration are not established");
            }
        }
        return bookie.getStateManager().serviceEpisodeForClassicChild();
    }

    public ServiceEpisode requireServing() throws IOException {
        var episode = registrationEpisode();
        synchronized (gate) {
            checkStarting();
            if (!serviceStarted || !listener.isRunning()) {
                throw new IOException("Original Classic service is not actually active");
            }
        }
        return episode;
    }

    public boolean isCurrentActivity() {
        synchronized (gate) {
            return Thread.currentThread() == initializingThread
                    || listener != null && listener.isCurrentThreadForClassicChild()
                    || bookie != null && bookie.isCurrentResourceActivityForFresh();
        }
    }

    /** Only package fixtures can access their unqualified instance. No public raw Bookie getter exists. */
    Bookie bookieForIoTests() {
        synchronized (gate) {
            if (!fixtureOnly || !storageStarted || closing) {
                throw new IllegalStateException("Fixture storage is not available");
            }
            return bookie;
        }
    }

    @Override
    void checkStarting() throws IOException {
        if (isClosing()) {
            throw new IOException("Historical resource close was requested");
        }
    }

    @Override
    boolean isClosing() {
        return closing || listener != null && listener.isClosingForClassicChild();
    }

    /** Non-blocking only. The external child owner supplies the sole close worker. */
    @Override
    public void requestClose() {
        closing = true;
        if (listener != null) {
            listener.requestCloseForClassicChild();
        }
        if (productionContext != null) {
            productionContext.requestClose(this);
        }
    }

    public Status closeAndDrain() throws IOException, InterruptedException {
        synchronized (gate) {
            if (initializingThread == Thread.currentThread()
                    || listener != null && listener.isCurrentThreadForClassicChild()
                    || bookie != null && bookie.isCurrentResourceActivityForFresh()) {
                throw new IllegalStateException("Historical resources cannot await their own activity");
            }
            requestClose();
            while (initializing) {
                gate.wait();
            }
        }
        synchronized (closeLock) {
            if (drained) {
                return status();
            }
            boolean complete = true;
            if (listener != null) {
                listener.requestCloseForClassicChild();
                if (serviceAttempted) {
                    listener.retireForClassicChild();
                } else {
                    complete = listener.closeListenerAndAwaitForClassicChild();
                }
                if (closeFailure == null) {
                    closeFailure = listener.closeFailureForClassicChild();
                }
            }
            boolean storageComplete = true;
            if (bookie != null) {
                var result = serviceAttempted ? bookie.closeForClassicChild(listener) : bookie.closeForFresh();
                storageComplete = result.drained();
                complete &= storageComplete;
                operationFailure = bookie.operationFailureForFresh();
                if (operationFailure == null) {
                    operationFailure = bookie.observationFailureForFresh();
                }
                if (closeFailure == null) {
                    closeFailure = result.closeFailure();
                }
            }
            if (storageComplete && serviceAttempted) {
                complete &= listener.finishNetworkForClassicChild();
                if (closeFailure == null) {
                    closeFailure = listener.closeFailureForClassicChild();
                }
                if (operationFailure == null) {
                    operationFailure = listener.operationFailureForClassicChild();
                }
            }
            if (storageComplete && metadata != null) {
                var result = metadata.closeAndDrain();
                complete &= result.drained();
                if (operationFailure == null) {
                    operationFailure = result.firstFailure();
                }
            }
            var files = reads.closeAndDrain();
            complete &= files.drained();
            if (closeFailure == null) {
                closeFailure = files.closeFailure();
            }
            synchronized (gate) {
                drained = complete && closeFailure == null;
            }
            return status();
        }
    }

    public Status status() {
        synchronized (gate) {
            return new Status(drained ? State.DRAINED : isClosing() ? State.CLOSING
                    : serviceStarted ? State.SERVING : listenerBound ? State.TCP_BOUND
                    : storageStarted ? State.STORAGE_STARTED
                    : attempted ? State.INITIALIZING : State.NEW,
                    initializationFailure, operationFailure != null ? operationFailure
                            : bookie != null && bookie.operationFailureForFresh() != null
                                    ? bookie.operationFailureForFresh()
                                    : bookie != null && bookie.observationFailureForFresh() != null
                                            ? bookie.observationFailureForFresh()
                                    : listener != null ? listener.operationFailureForClassicChild() : null,
                    closeFailure);
        }
    }
}

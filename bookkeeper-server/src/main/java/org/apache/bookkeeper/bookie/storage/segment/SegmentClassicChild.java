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
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ClassicChildResources;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.discover.profile.ZKBookieRouteBindingStore;
import org.apache.bookkeeper.meta.AbstractZkLedgerManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/** One fixed child episode and its actual resources. No caller-supplied configuration, trust or permission. */
public final class SegmentClassicChild {
    public enum State { NEW, STARTING, READY, CLOSING, DRAINED, FAILED }

    public record Status(State state, Throwable initializationFailure, Throwable operationFailure,
                         Throwable closeFailure) {
        public Throwable failure() {
            return initializationFailure != null ? initializationFailure
                    : operationFailure != null ? operationFailure : closeFailure;
        }
    }

    /** Private issuance after this owner's real START. Public methods address only its exact bound bridge. */
    public static final class StartContext {
        private final SegmentClassicChild owner;
        private final ClassicChildResources bridge;
        private boolean consumed;
        private boolean transferring;
        private boolean accepted;
        private Bookie bookie;

        private StartContext(SegmentClassicChild owner, ClassicChildResources bridge) {
            this.owner = owner;
            this.bridge = bridge;
        }

        public void initialize(ClassicChildResources candidate) throws Exception {
            synchronized (owner.gate) {
                requireOwner(candidate);
                if (consumed || owner.authorization == null) {
                    throw new IOException("Original START context is one-use");
                }
                consumed = true;
                transferring = true;
            }
            try {
                candidate.initialize(this, owner.configuration, owner.registration, owner.manager,
                        owner.replication);
            } finally {
                synchronized (owner.gate) {
                    transferring = false;
                }
            }
        }

        public void accept(ClassicChildResources candidate, ServerConfiguration configuration,
                           ZKRegistrationManager registration, AbstractZkLedgerManager manager,
                           ZkLedgerUnderreplicationManager replication) throws IOException {
            synchronized (owner.gate) {
                requireOwner(candidate);
                if (!consumed || !transferring || accepted || configuration != owner.configuration
                        || registration != owner.registration || manager != owner.manager
                        || replication != owner.replication) {
                    throw new IOException("START transfer differs from its original concrete resources");
                }
                accepted = true;
            }
        }

        public void bindBookie(ClassicChildResources candidate, Bookie original) throws IOException {
            synchronized (owner.gate) {
                requireOwner(candidate);
                if (!accepted || !transferring || bookie != null) {
                    throw new IOException("START context binds one concrete Bookie");
                }
                bookie = Objects.requireNonNull(original, "bookie");
            }
        }

        public void requireBookie(ClassicChildResources candidate, Bookie original) throws IOException {
            synchronized (owner.gate) {
                requireOwner(candidate);
                if (!accepted || bookie != original || original == null) {
                    throw new IOException("START context does not own this Bookie");
                }
            }
        }

        public void requireActivation(ClassicChildResources candidate, Bookie original) throws IOException {
            requireBookie(candidate, original);
            synchronized (owner.gate) {
                if (!owner.sourcesRevalidated) {
                    throw new IOException("Original Classic sources have not been finally revalidated");
                }
            }
        }

        public void requestClose(ClassicChildResources candidate) {
            if (candidate != bridge || owner.resources != candidate) {
                throw new IllegalArgumentException("Close notification belongs to a different bridge");
            }
            owner.requestClose();
        }

        private void requireOwner(ClassicChildResources candidate) throws IOException {
            if (candidate != bridge || owner.resources != bridge || Thread.currentThread() != owner.worker
                    || owner.closing || owner.authorization == null) {
                throw new IOException("START context is unavailable outside its original owner and worker");
            }
        }
    }

    /** Fixed process-pipe bytes, shared with bounded transport fixtures; contains no startup authority. */
    static final class Control {
        static final int HELLO = 1;
        static final int START = 2;
        static final int READY = 3;
        static final int STOP = 4;
        static final int COMPONENTS_TERMINAL = 5;
        private static final int MAGIC = 0x55574343;
        private InputStream input;
        private OutputStream output;
        private int sent;
        private int received;
        private boolean outputCloseAttempted;
        private boolean inputCloseAttempted;
        private boolean outputAcquiring;
        private boolean inputAcquiring;
        private Throwable closeFailure;

        void open() throws IOException {
            outputAcquiring = true;
            output = new FileOutputStream(FileDescriptor.out);
            outputAcquiring = false;
            inputAcquiring = true;
            input = new FileInputStream(FileDescriptor.in);
            inputAcquiring = false;
        }

        Control() { }

        Control(InputStream input, OutputStream output) {
            this.input = Objects.requireNonNull(input, "input");
            this.output = Objects.requireNonNull(output, "output");
        }

        byte[] read(int expected) throws IOException {
            byte[] header = readFully(16);
            ByteBuffer h = ByteBuffer.wrap(header);
            int magic = h.getInt();
            int version = Short.toUnsignedInt(h.getShort());
            int type = Short.toUnsignedInt(h.getShort());
            int flags = h.getInt();
            int size = h.getInt();
            if (magic != MAGIC || version != 1 || type != expected || flags != 0
                    || (received & (1 << type)) != 0 || !validSize(type, size)) {
                throw new IOException("Invalid or out-of-order controlled Classic pipe header");
            }
            received |= 1 << type;
            return readFully(size);
        }

        private byte[] readFully(int length) throws IOException {
            byte[] bytes = new byte[length];
            int offset = 0;
            while (offset < length) {
                int count = input.read(bytes, offset, length - offset);
                if (count < 0) {
                    throw new EOFException("Controlled Classic pipe ended inside its frame");
                }
                if (count == 0) {
                    throw new IOException("Controlled Classic pipe made no read progress");
                }
                offset += count;
            }
            return bytes;
        }

        void write(int type, byte[] payload) throws IOException {
            if ((sent & (1 << type)) != 0 || !validSize(type, payload.length) || outputCloseAttempted) {
                throw new IOException("Controlled Classic output frame is invalid or was already submitted");
            }
            sent |= 1 << type;
            output.write(ByteBuffer.allocate(16).putInt(MAGIC).putShort((short) 1).putShort((short) type)
                    .putInt(0).putInt(payload.length).array());
            output.write(payload);
            output.flush();
        }

        private static boolean validSize(int type, int size) {
            return switch (type) {
                case HELLO -> size >= 16 && size <= SegmentClassicLaunchCodec.MAX_HELLO_BYTES;
                case START -> size >= 112 && size <= SegmentClassicLaunchCodec.MAX_START_BYTES;
                case READY -> size == 40;
                case STOP -> size == 16;
                case COMPONENTS_TERMINAL -> size == 24;
                default -> false;
            };
        }

        void closeOutput() {
            if (output != null && !outputCloseAttempted) {
                outputCloseAttempted = true;
                try {
                    output.close();
                } catch (IOException | RuntimeException | Error problem) {
                    closeFailure = problem;
                }
            }
        }

        void closeInput() {
            if (input != null && !inputCloseAttempted) {
                inputCloseAttempted = true;
                try {
                    input.close();
                } catch (IOException | RuntimeException | Error problem) {
                    if (closeFailure == null) {
                        closeFailure = problem;
                    }
                }
            }
        }

        boolean drained() {
            return (input == null || inputCloseAttempted) && (output == null || outputCloseAttempted)
                    && !inputAcquiring && !outputAcquiring && closeFailure == null;
        }
    }

    private static final class StartupStopped extends IOException { }

    private final String recipeId;
    private final Object gate = new Object();
    private Thread worker;
    private Thread reader;
    private volatile Thread metadataCallbackThread;
    private boolean started;
    private boolean startupFinished;
    private boolean ready;
    private boolean sourcesRevalidated;
    private volatile boolean closing;
    private boolean componentsDrained;
    private boolean closed;
    private Throwable initializationFailure;
    private Throwable operationFailure;
    private Throwable closeFailure;
    private String acquiring;
    private long deadline;
    private Control control;
    private byte[] startPacket;
    private SegmentClassicLaunchCodec.Start authorization;
    private SegmentClassicLaunchCodec.Hello hello;
    private SegmentClassicLaunchPreload preload;
    private SegmentClassicLaunchTrust trust;
    private SegmentClassicLaunchTrust.Installed installed;
    private SegmentClassicLaunchConfiguration.Frozen frozen;
    private ServerConfiguration configuration;
    private ZooKeeper metadata;
    private ZKMetadataBookieDriver driver;
    private ZKRegistrationManager registration;
    private AbstractZkLedgerManager manager;
    private ZkLedgerUnderreplicationManager replication;
    private ZKBookieRouteBindingStore.ClassicLaunchSources sources;
    private volatile ClassicChildResources resources;
    private boolean replicationCloseAttempted;
    private boolean managerCloseAttempted;
    private boolean registrationCloseAttempted;
    private boolean driverCloseAttempted;
    private boolean metadataCloseAttempted;

    private SegmentClassicChild(String recipeId) {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        this.recipeId = recipeId;
    }

    /** Heap-only; neither opening a pipe nor loading JNI nor touching storage. */
    public static SegmentClassicChild create(String recipeId) {
        return new SegmentClassicChild(recipeId);
    }

    public void start() throws IOException, InterruptedException {
        requireExternal();
        synchronized (gate) {
            if (started || closing) {
                throw new IOException("Controlled Classic child starts once");
            }
            started = true;
            deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            worker = new Thread(this::run, "controlled-classic-owner");
            worker.setDaemon(false);
            try {
                worker.start();
            } catch (RuntimeException | Error problem) {
                initializationFailure = problem;
                closing = true;
                startupFinished = true;
                if (worker.getState() == Thread.State.NEW) {
                    componentsDrained = true;
                    closed = true;
                }
                gate.notifyAll();
                throw problem;
            }
            while (!startupFinished) {
                gate.wait();
            }
            if (initializationFailure != null || !ready && operationFailure != null) {
                throw new IOException("Controlled Classic startup failed", failure());
            }
        }
    }

    public void requestClose() {
        ClassicChildResources original;
        synchronized (gate) {
            if (closing) {
                return;
            }
            closing = true;
            original = resources;
            if (!started) {
                componentsDrained = true;
                closed = true;
            }
            gate.notifyAll();
        }
        if (original != null) {
            original.requestClose();
        }
    }

    public Status closeAndAwait(Duration timeout) throws InterruptedException, TimeoutException {
        requireExternal();
        duration(timeout);
        requestClose();
        return awaitTermination(timeout);
    }

    public Status awaitTermination(Duration timeout) throws InterruptedException, TimeoutException {
        requireExternal();
        long remaining = duration(timeout);
        long end = System.nanoTime() + remaining;
        for (;;) {
            Thread original;
            synchronized (gate) {
                original = worker;
                if (closed && (original == null || !original.isAlive()) && (reader == null || !reader.isAlive())) {
                    return status();
                }
                remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("Original Classic child resources have not actually terminated");
                }
                if (original == null || !original.isAlive()) {
                    TimeUnit.NANOSECONDS.timedWait(gate, remaining);
                    continue;
                }
            }
            TimeUnit.NANOSECONDS.timedJoin(original, remaining);
        }
    }

    public Status status() {
        synchronized (gate) {
            Throwable failure = failure();
            boolean terminated = closed && (worker == null || !worker.isAlive())
                    && (reader == null || !reader.isAlive());
            return new Status(terminated ? failure == null ? State.DRAINED : State.FAILED
                    : closing ? State.CLOSING : ready ? State.READY : started ? State.STARTING : State.NEW,
                    initializationFailure, operationFailure, closeFailure);
        }
    }

    private void requireExternal() {
        Thread current = Thread.currentThread();
        if (current == worker || current == reader || current == metadataCallbackThread
                || resources != null && resources.isCurrentActivity()) {
            throw new IllegalStateException("Controlled child activity cannot await its own termination");
        }
    }

    private static long duration(Duration value) {
        Objects.requireNonNull(value, "timeout");
        if (value.isNegative()) {
            throw new IllegalArgumentException("Negative Classic child observation deadline");
        }
        return value.toNanos();
    }

    private void run() {
        try {
            startup();
            synchronized (gate) {
                startupFinished = true;
                gate.notifyAll();
                while (!closing) {
                    gate.wait();
                }
            }
        } catch (StartupStopped stopped) {
            // A valid STOP may retire a START-authorized initialization before READY.
        } catch (Throwable problem) {
            synchronized (gate) {
                if (initializationFailure == null && !ready) {
                    initializationFailure = problem;
                } else if (operationFailure == null) {
                    operationFailure = problem;
                }
            }
        } finally {
            requestClose();
            synchronized (gate) {
                startupFinished = true;
                gate.notifyAll();
            }
            // No external waiter interrupts this worker. Preserve an internal failure before cleanup.
            if (Thread.interrupted()) {
                rememberClose(new InterruptedException("Original Classic owner was interrupted"));
            }
            finish();
        }
    }

    private void startup() throws Exception {
        checkStarting();
        var process = ProcessHandle.current();
        var arguments = List.of(process.info().arguments().orElseThrow(
                () -> new IOException("Actual Classic process arguments are unavailable")));
        if (arguments.size() < 2 || !arguments.subList(arguments.size() - 2, arguments.size())
                .equals(List.of(SegmentClassicLaunchProgram.MAIN, recipeId))) {
            throw new IOException("Controlled child is only valid in its fixed Main process");
        }
        checkStarting();
        control = new Control();
        control.open();
        checkStarting();
        reader = new Thread(this::readControl, "controlled-classic-reader");
        reader.setDaemon(false);
        reader.start();
        checkStarting();
        preload = SegmentClassicLaunchPreload.production(recipeId, deadline);
        preload.initialize();
        checkStarting();
        preload.loadNative();
        checkStarting();
        trust = SegmentClassicLaunchTrust.production(preload);
        trust.initialize();
        checkStarting();
        installed = trust.installed();
        var material = installed.material();
        var program = trust.verifyProgram(deadline);
        checkStarting();
        if (!System.getenv().isEmpty() || !program.runtime().equals(System.getProperty("java.runtime.version"))
                || !program.vendor().equals(System.getProperty("java.vendor"))
                || !program.javaHome().equals(Path.of(System.getProperty("java.home")).toRealPath())
                || !program.files().get(program.javaFile()).path()
                        .equals(Path.of(process.info().command().orElseThrow()).toRealPath())
                || !arguments.equals(program.command(recipeId).subList(1, program.command(recipeId).size()))
                || !System.getProperty("java.class.path").equals(String.join(":", program.classPath().stream()
                        .map(i -> program.files().get(i).path().toString()).toList()))) {
            throw new IOException("Actual Classic JVM differs from the fixed verified program");
        }
        frozen = trust.verifyConfiguration(deadline);
        configuration = SegmentClassicLaunchConfiguration.materialize(frozen);
        requireCredentials(material.recipe());
        checkStarting();
        URI uri = URI.create(configuration.getMetadataServiceUri());
        String address = ZKMetadataDriverBase.getZKServersFromServiceUri(uri);
        int sessionTimeout = configuration.getZkTimeout();
        if (sessionTimeout <= 0) {
            throw new IOException("Original Classic metadata session requires a positive timeout");
        }
        acquiring = "original raw ZooKeeper construction";
        metadata = new ZooKeeper(address, sessionTimeout, event -> {
                    metadataCallbackThread = Thread.currentThread();
                    if (event.getState() == Watcher.Event.KeeperState.Expired) {
                        rememberOperation(new IOException("Original Classic metadata SESSION_EXPIRED"));
                    }
                });
        acquiring = null;
        driver = new ZKMetadataBookieDriver();
        driver.initializeForClassicChild(configuration, NullStatsLogger.INSTANCE, metadata);
        var store = new ZKBookieRouteBindingStore(metadata, uri.getPath(), ZkUtils.getACLs(configuration),
                remaining());
        sources = store.borrowClassicLaunchSources(BookieImpl.getBookieId(configuration), remaining());
        var original = originalScope(sources.read());
        if (!original.identity().equals(material.recipe().originalScope())) {
            throw new IOException("Current original Classic source differs from the installed recipe");
        }
        byte[] challenge = new byte[32];
        new SecureRandom().nextBytes(challenge);
        hello = new SegmentClassicLaunchCodec.Hello(recipeId, material.recipeIdentity(), material.recipe().program(),
                frozen.identity(), original.identity(), material.recipe().oldUid(), process.pid(),
                process.info().startInstant().orElseThrow(), new SegmentClassicHandoff.Digest(challenge));
        checkStarting();
        control.write(Control.HELLO, SegmentClassicLaunchCodec.encodeHello(hello));
        byte[] packet;
        synchronized (gate) {
            while (startPacket == null && !closing) {
                TimeUnit.NANOSECONDS.timedWait(gate, remaining().toNanos());
            }
            checkStarting();
            packet = startPacket;
        }
        var verified = SegmentClassicLaunchCodec.verifyStart(packet, material.recipe().signerKeyId(),
                material.publicKey(), hello);
        requireCredentials(material.recipe());
        if (!originalScope(sources.read()).identity().equals(hello.originalScope())) {
            throw new IOException("Classic source changed before START consumption");
        }
        synchronized (gate) {
            checkStarting();
            authorization = verified;
            gate.notifyAll();
        }
        // Factory initialization may provision layout. This controlled driver permits only existing layout reads.
        checkStarting();
        if (driver.getLayoutManager().readLedgerLayout() == null) {
            throw new IOException("Controlled Classic requires its existing ledger layout");
        }
        checkStarting();
        // The driver registers the exact known heap factory before initialize; it owns any failed prefix.
        var factory = driver.getLedgerManagerFactory();
        checkStarting();
        acquiring = "original ledger manager construction";
        // Each selected exact builtin returns this concrete resource family, before any user operation.
        manager = (AbstractZkLedgerManager) factory.newLedgerManager();
        acquiring = null;
        checkStarting();
        acquiring = "original underreplication manager construction";
        replication = new ZkLedgerUnderreplicationManager(configuration, metadata);
        acquiring = null;
        checkStarting();
        acquiring = "original registration manager construction";
        registration = (ZKRegistrationManager) driver.createRegistrationManager();
        acquiring = null;
        registration.addClassicChildRegistrationListener(() -> {
            metadataCallbackThread = Thread.currentThread();
            rememberOperation(new IOException("Original Classic metadata SESSION_EXPIRED"));
        });
        checkStarting();
        resources = ClassicChildResources.prepare();
        var context = new StartContext(this, resources);
        resources.initialize(context);
        checkStarting();
        resources.prepareNetwork();
        var serving = resources.registrationEpisode();
        if (!originalScope(sources.readForServing(serving.session(), serving.birth(), serving.readOnly()))
                .identity().equals(hello.originalScope())) {
            throw new IOException("Original Classic source changed before READY");
        }
        requireCredentials(material.recipe());
        checkStarting();
        sourcesRevalidated = true;
        resources.activateNetwork();
        resources.requireServing();
        ByteBuffer payload = episode(40);
        payload.putLong(serving.session()).putLong(serving.birth()).put((byte) (serving.readOnly() ? 2 : 1))
                .put(new byte[7]);
        synchronized (gate) {
            checkStarting();
        }
        control.write(Control.READY, payload.array());
        synchronized (gate) {
            checkStarting();
            ready = true;
        }
    }

    private SegmentClassicLaunchCodec.OriginalScope originalScope(
            ZKBookieRouteBindingStore.ClassicLaunchSnapshot snapshot) {
        var roots = new TreeMap<String, EnumSet<SegmentArenaManifest.ClassicRole>>();
        List<File[]> directories = List.of(configuration.getJournalDirs(), configuration.getLedgerDirs(),
                configuration.getIndexDirs());
        var roles = SegmentArenaManifest.ClassicRole.values();
        for (int i = 0; i < directories.size(); i++) {
            for (File directory : directories.get(i)) {
                roots.computeIfAbsent(directory.toPath().toString(), ignored -> EnumSet.noneOf(
                        SegmentArenaManifest.ClassicRole.class)).add(roles[i]);
            }
        }
        var all = new ArrayList<SegmentArenaManifest.ClassicRoot>();
        roots.forEach((directory, role) -> all.add(new SegmentArenaManifest.ClassicRoot(Path.of(directory), role)));
        return new SegmentClassicLaunchCodec.OriginalScope(snapshot.ledgersRoot(), snapshot.clusterId(),
                snapshot.clusterBirthZxid(), snapshot.bookie(), snapshot.cookieIdentity(), all);
    }

    private void requireCredentials(SegmentClassicLaunchTrust.Recipe recipe) throws IOException {
        var actual = new NativeIOImpl().currentProcessCredentials();
        if (actual.realUid() != recipe.oldUid() || actual.effectiveUid() != recipe.oldUid()
                || actual.realGid() != recipe.primaryGid() || actual.effectiveGid() != recipe.primaryGid()
                || !actual.supplementaryGroups().equals(recipe.groups())) {
            throw new IOException("Actual Classic process credentials differ from the fixed recipe");
        }
    }

    private void readControl() {
        try {
            byte[] packet = control.read(Control.START);
            synchronized (gate) {
                startPacket = packet;
                gate.notifyAll();
                while (authorization == null && !closing) {
                    gate.wait();
                }
                if (closing) {
                    return;
                }
            }
            byte[] stop = control.read(Control.STOP);
            ByteBuffer payload = ByteBuffer.wrap(stop);
            UUID episode = new UUID(payload.getLong(), payload.getLong());
            if (!episode.equals(authorization.launcherEpisode())) {
                throw new IOException("STOP belongs to a different original launcher episode");
            }
            requestClose();
        } catch (EOFException ended) {
            if (!closing) {
                rememberOperation(ended);
            }
        } catch (Throwable problem) {
            rememberOperation(problem);
        } finally {
            synchronized (gate) {
                gate.notifyAll();
            }
        }
    }

    private Duration remaining() throws IOException {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
            throw new IOException("Original controlled Classic startup deadline elapsed");
        }
        return Duration.ofNanos(Math.min(left, TimeUnit.MINUTES.toNanos(1)));
    }

    private void checkStarting() throws IOException {
        if (closing) {
            throw new StartupStopped();
        }
        remaining();
    }

    private ByteBuffer episode(int bytes) {
        return ByteBuffer.allocate(bytes).putLong(authorization.launcherEpisode().getMostSignificantBits())
                .putLong(authorization.launcherEpisode().getLeastSignificantBits());
    }

    private void rememberOperation(Throwable problem) {
        synchronized (gate) {
            if (operationFailure == null) {
                operationFailure = problem;
            }
        }
        requestClose();
    }

    private void rememberClose(Throwable problem) {
        synchronized (gate) {
            if (closeFailure == null) {
                closeFailure = problem;
            }
        }
    }

    private Throwable failure() {
        return initializationFailure != null ? initializationFailure
                : operationFailure != null ? operationFailure : closeFailure;
    }

    private void finish() {
        boolean complete = acquiring == null;
        boolean storageComplete = true;
        try {
            if (resources != null) {
                var result = resources.closeAndDrain();
                storageComplete = result.state() == ClassicChildResources.State.DRAINED;
                complete &= storageComplete;
                if (result.operationFailure() != null) {
                    rememberOperation(result.operationFailure());
                }
                if (result.closeFailure() != null) {
                    rememberClose(result.closeFailure());
                }
            }
        } catch (Throwable problem) {
            storageComplete = false;
            complete = false;
            rememberClose(problem);
        }
        if (sources != null) {
            sources.closeAndDrain(); // Retire submissions; raw close below can finish already submitted callbacks.
        }
        if (storageComplete && acquiring == null) {
            complete &= finishMetadata();
        }
        if (sources != null) {
            var result = sources.closeAndDrain(); // Observe actual callback retirement, without reissuing I/O.
            complete &= result.drained();
            if (result.firstReadFailure() != null) {
                rememberOperation(result.firstReadFailure());
            }
        }
        if (resources != null && resources.status().operationFailure() != null) {
            rememberOperation(resources.status().operationFailure());
        }
        try {
            if (trust != null) {
                var result = trust.closeAndDrain();
                complete &= result.drained();
                if (result.files().closeFailure() != null) {
                    rememberClose(result.files().closeFailure());
                }
                if (result.auxiliaryCloseFailure() != null) {
                    rememberClose(result.auxiliaryCloseFailure());
                }
            }
            if (preload != null) {
                var result = preload.closeAndDrain();
                complete &= result.drained();
                if (result.closeFailure() != null) {
                    rememberClose(result.closeFailure());
                }
            }
        } catch (Throwable problem) {
            complete = false;
            rememberClose(problem);
        }
        componentsDrained = complete;
        if (control != null) {
            if (complete && authorization != null) {
                try {
                    control.write(Control.COMPONENTS_TERMINAL, episode(24).putInt(failure() == null ? 0 : 1)
                            .putInt(0).array());
                } catch (Throwable problem) {
                    rememberClose(problem);
                }
            }
            control.closeOutput();
            // Parent observes result/EOF, closes its input writer, then waits for Process. Never self-close a read.
            if (reader != null) {
                try {
                    reader.join();
                } catch (InterruptedException problem) {
                    rememberClose(problem);
                }
            }
            if (reader == null || !reader.isAlive()) {
                control.closeInput();
            }
            if (control.closeFailure != null) {
                rememberClose(control.closeFailure);
            }
            complete &= control.drained() && (reader == null || !reader.isAlive());
        }
        synchronized (gate) {
            closed = complete && componentsDrained && closeFailure == null;
            gate.notifyAll();
        }
    }

    private boolean finishMetadata() {
        boolean complete = true;
        if (replication != null && !replicationCloseAttempted) {
            replicationCloseAttempted = true;
            try {
                replication.close();
            } catch (Throwable problem) {
                complete = false;
                rememberClose(problem);
            }
        }
        boolean managerDrained = true;
        if (manager != null && !managerCloseAttempted) {
            managerCloseAttempted = true;
            try {
                managerDrained = manager.closeAndAwaitTermination(Duration.ofNanos(Long.MAX_VALUE));
                complete &= managerDrained;
            } catch (Throwable problem) {
                managerDrained = false;
                complete = false;
                rememberClose(problem);
            }
        }
        if (registration != null && !registrationCloseAttempted) {
            registrationCloseAttempted = true;
            try {
                registration.close();
            } catch (Throwable problem) {
                complete = false;
                rememberClose(problem);
            }
        }
        if (managerDrained && driver != null && !driverCloseAttempted) {
            driverCloseAttempted = true;
            try {
                driver.closeForClassicChild();
            } catch (Throwable problem) {
                complete = false;
                rememberClose(problem);
            }
        }
        if (managerDrained && metadata != null && !metadataCloseAttempted) {
            metadataCloseAttempted = true;
            try {
                // Pinned ZK 3.9.5 close(0) joins both original threads. No observer cancels this call.
                complete &= metadata.close(0);
            } catch (Throwable problem) {
                complete = false;
                rememberClose(problem);
            }
        }
        return complete;
    }
}

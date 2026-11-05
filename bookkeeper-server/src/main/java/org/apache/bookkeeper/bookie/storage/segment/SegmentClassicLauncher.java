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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.profile.ZKBookieRouteBindingStore;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/** Fixed UID-zero supervisor of one actual child Process. No process adoption or caller-supplied authority. */
public final class SegmentClassicLauncher {
    public enum State { NEW, STARTING, READY, CLOSING, DRAINED, FAILED }

    public record Status(State state, Throwable initializationFailure, Throwable operationFailure,
                         Throwable closeFailure) {
        public Throwable failure() {
            return initializationFailure != null ? initializationFailure
                    : operationFailure != null ? operationFailure : closeFailure;
        }
    }

    private static final class StartupStopped extends IOException { }
    private final Object gate = new Object();
    private final String recipeId;
    private Thread worker;
    private volatile Thread metadataThread;
    private boolean started;
    private boolean startupFinished;
    private boolean ready;
    private volatile boolean closing;
    private boolean closed;
    private Throwable initializationFailure;
    private Throwable operationFailure;
    private Throwable closeFailure;
    private long deadline;
    private String acquiring;
    private SegmentClassicLaunchPreload preload;
    private SegmentClassicLaunchTrust trust;
    private SegmentClassicLaunchTrust.Parsed material;
    private SegmentClassicLaunchProgram program;
    private SegmentClassicLauncherInputs input;
    private ServerConfiguration configuration;
    private SegmentClassicLaunchConfiguration.Frozen frozen;
    private ZooKeeper metadata;
    private ZKBookieRouteBindingStore.ClassicLaunchSources sources;
    private SegmentClassicLaunchCodec.OriginalScope originalScope;
    private SegmentDataFileLock supervisor;
    private SegmentClassicHandoffPaths supervisorPaths;
    private SegmentClassicHandoffPaths.Identity supervisorIdentity;
    private List<SegmentClassicHandoffPaths.Identity> supervisorParents;
    private Path guardPath;
    private Path retirementPath;
    private PrivateKey privateKey;
    private volatile SegmentClassicLauncherProcess processOwner;
    private Instant processStarted;
    private boolean metadataCloseAttempted;
    private boolean supervisorCloseAttempted;
    private UUID episode;
    private boolean startIssued;

    private SegmentClassicLauncher(String recipeId) {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        this.recipeId = recipeId;
    }

    /** Heap-only: no trust files, key, native loading, metadata, supervisor lock or child process. */
    public static SegmentClassicLauncher create(String recipeId) {
        return new SegmentClassicLauncher(recipeId);
    }

    public void start() throws IOException, InterruptedException {
        requireExternal();
        synchronized (gate) {
            if (started || closing) {
                throw new IOException("Original Classic launcher starts once");
            }
            started = true;
            deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            worker = new Thread(this::run, "controlled-classic-launcher");
            worker.setDaemon(false);
            try {
                worker.start();
            } catch (RuntimeException | Error problem) {
                initializationFailure = problem;
                closing = true;
                startupFinished = true;
                closed = worker.getState() == Thread.State.NEW;
                gate.notifyAll();
                throw problem;
            }
            while (!startupFinished) {
                gate.wait();
            }
            if (initializationFailure != null || !ready && operationFailure != null) {
                throw new IOException("Controlled Classic launcher startup failed", status().failure());
            }
        }
    }

    public void requestClose() {
        SegmentClassicLauncherProcess original;
        synchronized (gate) {
            closing = true;
            original = processOwner;
            if (!started) {
                closed = true;
            }
            gate.notifyAll();
        }
        if (original != null) {
            original.requestClose();
        }
    }

    /** No retirement or service stop is performed without the complete commissioned Unix handoff producer. */
    public void requestHandoff(UUID bindingId) throws IOException {
        Objects.requireNonNull(bindingId, "bindingId");
        if (bindingId.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("Original commissioned operation ID required");
        }
        throw new IOException("Complete commissioned/Unix handoff qualification is unavailable; no retirement issued");
    }

    public Status closeAndAwait(Duration timeout) throws InterruptedException, TimeoutException {
        requireExternal();
        duration(timeout);
        requestClose();
        return awaitTermination(timeout);
    }

    public Status awaitTermination(Duration timeout) throws InterruptedException, TimeoutException {
        requireExternal();
        long end = System.nanoTime() + duration(timeout);
        for (;;) {
            Thread original;
            synchronized (gate) {
                original = worker;
                if (terminated()) {
                    return status();
                }
                long remaining = end - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("Original launcher resources have not actually terminated");
                }
                if (original == null || !original.isAlive()) {
                    TimeUnit.NANOSECONDS.timedWait(gate, remaining);
                    continue;
                }
            }
            TimeUnit.NANOSECONDS.timedJoin(original, Math.max(1, end - System.nanoTime()));
        }
    }

    public Status status() {
        synchronized (gate) {
            Throwable failure = initializationFailure != null ? initializationFailure
                    : operationFailure != null ? operationFailure : closeFailure;
            return new Status(terminated() ? failure == null ? State.DRAINED : State.FAILED
                    : closing ? State.CLOSING : ready ? State.READY : started ? State.STARTING : State.NEW,
                    initializationFailure, operationFailure, closeFailure);
        }
    }

    private boolean terminated() {
        return closed && (worker == null || !worker.isAlive())
                && (processOwner == null || processOwner.readerTerminated());
    }

    private static long duration(Duration timeout) {
        if (Objects.requireNonNull(timeout, "timeout").isNegative()) {
            throw new IllegalArgumentException("Negative launcher observation deadline");
        }
        return timeout.toNanos();
    }

    private void requireExternal() {
        Thread current = Thread.currentThread();
        if (current == worker || current == metadataThread
                || processOwner != null && processOwner.isCurrentThread()) {
            throw new IllegalStateException("Launcher activity cannot await its own termination");
        }
    }

    private void run() {
        try {
            startup();
            synchronized (gate) {
                startupFinished = true;
                gate.notifyAll();
            }
            processOwner.awaitCloseRequested();
        } catch (StartupStopped stopped) {
            // An external close may retire an initialization before READY.
        } catch (Throwable problem) {
            synchronized (gate) {
                if (!ready && initializationFailure == null) {
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
            if (Thread.interrupted()) {
                rememberClose(new InterruptedException("Original launcher worker was interrupted"));
            }
            finish();
        }
    }

    private void startup() throws Exception {
        checkStarting();
        var current = ProcessHandle.current();
        var arguments = List.of(current.info().arguments().orElseThrow());
        if (arguments.size() < 2 || !arguments.subList(arguments.size() - 2, arguments.size())
                .equals(List.of(SegmentClassicLauncherInputs.MAIN, recipeId))) {
            throw new IOException("Launcher is only valid in its fixed Main process");
        }
        preload = SegmentClassicLaunchPreload.production(recipeId, deadline);
        preload.initialize();
        checkStarting();
        preload.loadNative();
        trust = SegmentClassicLaunchTrust.production(preload);
        trust.initialize();
        material = trust.installed().material();
        program = trust.verifyProgram(deadline);
        frozen = trust.verifyConfiguration(deadline);
        configuration = SegmentClassicLaunchConfiguration.materialize(frozen);
        input = trust.readLauncherInputs(deadline);
        input.requireRecipe(material);
        if (input.linuxDeployment() != null) {
            input.linuxDeployment().requireProgram(program, recipeId);
        }
        requireRoot();
        program.requireCurrentLauncher(recipeId);
        if (input.linuxDeployment() != null && input.linuxDeployment().schema() == 2) {
            var deployment = input.linuxDeployment().versionTwo();
            trust.verifyLinuxProvider(deadline);
            trust.verifyLinuxKernelAndCgroupRoot(deadline);
            trust.verifyLinuxBootId(deadline);
            trust.verifyLinuxProcessBirth(deadline);
            trust.verifyLinuxInitialNamespaces(deadline);
            trust.verifyLinuxLoaderConfiguration(deployment.preloadAbsentConfigurationIndex(), deadline);
            trust.verifyLinuxLoaderConfiguration(SegmentClassicLaunchTrust.fixedLinuxCacheRule(deployment.loading()),
                    deadline);
            trust.verifyLinuxLoaderConfiguration(deployment.ldconfigConfigurationIndex(), deadline);
            trust.verifyLinuxSearchDirectories(deadline);
            trust.verifyLinuxCandidateContexts(deadline);
            trust.verifyLinuxJdkHelper(deadline);
            trust.verifyLinuxUnitFiles(deadline);
            trust.verifyLinuxInstallLinks(deadline);
            trust.verifyLinuxBootId(deadline);
            trust.verifyLinuxProcessBirth(deadline);
            trust.verifyLinuxInitialNamespaces(deadline);
            trust.verifyLinuxSystemdPeer(deadline);
        }
        trust.verifyLauncherDropper(input, deadline);
        final List<String> childCommand = input.childCommand(material, program);
        loadPrivateKey();
        checkStarting();
        URI uri = URI.create(configuration.getMetadataServiceUri());
        int sessionTimeout = configuration.getZkTimeout();
        if (sessionTimeout <= 0) {
            throw new IOException("Original launcher metadata timeout must be positive");
        }
        acquiring = "original raw launcher metadata construction";
        metadata = new ZooKeeper(ZKMetadataDriverBase.getZKServersFromServiceUri(uri), sessionTimeout, event -> {
            metadataThread = Thread.currentThread();
            if (event.getState() == Watcher.Event.KeeperState.Expired) {
                rememberOperation(new IOException("Original launcher metadata SESSION_EXPIRED"));
            }
        });
        acquiring = null;
        var store = new ZKBookieRouteBindingStore(metadata, uri.getPath(), ZkUtils.getACLs(configuration), remaining());
        sources = store.borrowClassicLaunchSources(BookieImpl.getBookieId(configuration), remaining());
        originalScope = SegmentClassicChild.originalScope(configuration, sources.read());
        requireOriginal(originalScope);
        var scope = new SegmentClassicHandoff.SupervisorScope(originalScope.clusterId(),
                originalScope.clusterBirthZxid(), originalScope.bookie());
        String stem = SegmentClassicHandoffMaterials.scopeName(scope);
        guardPath = input.supervisorDirectory().resolve(stem + ".lock");
        retirementPath = input.supervisorDirectory().resolve(stem + ".retirement");
        if (input.supervisorDirectory().startsWith(preload.root())
                || preload.root().startsWith(input.supervisorDirectory())
                || input.proof().equals(guardPath) || input.proof().equals(retirementPath)
                || input.policy().equals(guardPath) || input.policy().equals(retirementPath)
                || input.aesKey().equals(guardPath) || input.aesKey().equals(retirementPath)) {
            throw new IOException("Parent secrets, supervisor guard and handoff leaves must remain separate");
        }
        supervisorPaths = new SegmentClassicHandoffPaths(material.recipe().oldUid(), input.targetUid(), 0);
        supervisorPaths.requireDirectory(input.supervisorDirectory());
        supervisorIdentity = supervisorPaths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES);
        supervisorParents = supervisorPaths.parents(guardPath);
        if (supervisorIdentity.uid() != input.targetUid() || (supervisorIdentity.mode() & 07777) != 0600) {
            throw new IOException("Original supervisor guard must remain private to the target UID");
        }
        requireSupervisorPaths();
        supervisor = SegmentDataFileLock.existingControlOwner(guardPath);
        supervisor.initialize();
        requireNotRetired();
        requireOriginal(SegmentClassicChild.originalScope(configuration, sources.read()));
        requireRoot();
        checkStarting();
        var builder = new ProcessBuilder(childCommand);
        builder.environment().clear();
        builder.directory(program.javaHome().toFile());
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processOwner = new SegmentClassicLauncherProcess();
        checkStarting();
        processOwner.initialize(builder);
        processStarted = processOwner.createdStartTime();
        byte[] received;
        try {
            received = processOwner.awaitHello(remaining());
        } catch (IOException problem) {
            checkStarting();
            throw problem;
        }
        var reported = SegmentClassicLaunchCodec.decodeHello(received);
        var actualProcess = requireChildProcess();
        requireOriginal(SegmentClassicChild.originalScope(configuration, sources.read()));
        requireNotRetired();
        var expected = new SegmentClassicLaunchCodec.Hello(recipeId, material.recipeIdentity(),
                material.recipe().program(), frozen.identity(), originalScope.identity(), material.recipe().oldUid(),
                actualProcess.pid(), processStarted, reported.challenge());
        if (!reported.equals(expected)) {
            throw new IOException("Original child HELLO differs from independent launcher observations");
        }
        requireRoot();
        checkStarting();
        requireChildProcess();
        episode = UUID.randomUUID();
        byte[] authorization = issueOriginalStart(expected);
        synchronized (gate) {
            checkStarting();
        }
        processOwner.flushStart(authorization, episode);
        SegmentClassicLauncherProcess.Ready observed;
        try {
            observed = processOwner.awaitReady(remaining());
        } catch (IOException problem) {
            checkStarting();
            throw problem;
        }
        requireOriginal(SegmentClassicChild.originalScope(configuration,
                sources.readForObservedRegistration(observed.session(), observed.birth(), observed.readOnly())));
        requireChildProcess();
        requireNotRetired();
        synchronized (gate) {
            checkStarting();
            ready = true;
        }
    }

    private void requireRoot() throws IOException {
        requireParentCredentials(input, new NativeIOImpl().currentProcessCredentials());
    }

    /** Value comparison only; actual production credentials come from the original native read at each call. */
    static void requireParentCredentials(SegmentClassicLauncherInputs input, NativeIO.ProcessCredentials credentials)
            throws IOException {
        if (credentials.realUid() != 0 || credentials.effectiveUid() != 0) {
            throw new IOException("Controlled Classic launcher requires actual real/effective UID zero");
        }
        var deployment = input.linuxDeployment();
        if (deployment != null && deployment.schema() == 2) {
            var original = deployment.versionTwo();
            long group = original.units().get(original.launcherUnit()).service().group();
            if (credentials.realGid() != group || credentials.effectiveGid() != group) {
                throw new IOException("Controlled Classic launcher real/effective GID differs from its D2 service");
            }
        }
    }

    private void requireOriginal(SegmentClassicLaunchCodec.OriginalScope actual) throws IOException {
        if (!actual.identity().equals(material.recipe().originalScope())
                || originalScope != null && !actual.identity().equals(originalScope.identity())) {
            throw new IOException("Independent original Classic source changed");
        }
    }

    private SegmentClassicLauncherProcess.Identity requireChildProcess() throws IOException {
        var actual = processOwner.identity();
        if (!actual.alive() || !actual.started().equals(processStarted)
                || !Path.of(actual.command()).toRealPath().equals(program.files().get(program.javaFile()).path())
                || !actual.arguments().equals(program.command(recipeId).subList(1, program.command(recipeId).size()))) {
            throw new IOException("Original supervised Process is not its approved final child JVM");
        }
        return actual;
    }

    private void requireSupervisorPaths() throws IOException {
        if (!supervisorIdentity.equals(supervisorPaths.privateFile(guardPath, 0, SegmentClassicHandoff.MAX_BYTES))
                || !supervisorParents.equals(supervisorPaths.parents(guardPath))) {
            throw new IOException("Original supervisor guard or parent chain changed");
        }
        for (Path path = guardPath; path != null; path = path.getParent()) {
            var before = SegmentClassicHandoffPaths.fileIdentity(path);
            if (new NativeIOImpl().hasExtendedAcl(path.toString())
                    || !before.equals(SegmentClassicHandoffPaths.fileIdentity(path))) {
                throw new IOException("Supervisor path ACL is unsupported or its identity changed");
            }
        }
    }

    private void requireNotRetired() throws IOException {
        supervisor.requireHeld(guardPath);
        requireSupervisorPaths();
        try {
            Files.readAttributes(retirementPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException absent) {
            requireSupervisorPaths();
            return;
        }
        throw new IOException("Original Classic supervisor is already retired; no new child may start");
    }

    private void loadPrivateKey() throws Exception {
        byte[] bytes = trust.readLauncherPrivateKey();
        try {
            if (!Arrays.equals(Arrays.copyOf(bytes, 16),
                    HexFormat.of().parseHex("302e020100300506032b657004220420"))) {
                throw new IOException("Only canonical fixed Ed25519 private-key encoding is supported");
            }
            privateKey = KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(bytes));
            byte[] check = "BookKeeper/ControlledClassicLauncherKeyCheck/v1\0".getBytes(StandardCharsets.US_ASCII);
            Signature signature = Signature.getInstance("Ed25519");
            signature.initSign(privateKey);
            signature.update(check);
            signature.update(material.recipeIdentity().value());
            byte[] signed = signature.sign();
            signature.initVerify(material.publicKey());
            signature.update(check);
            signature.update(material.recipeIdentity().value());
            if (!signature.verify(signed)) {
                throw new IOException("Fixed launcher private key does not match the approved child public key");
            }
        } finally {
            Arrays.fill(bytes, (byte) 0);
        }
    }

    private byte[] issueOriginalStart(SegmentClassicLaunchCodec.Hello expected) throws Exception {
        synchronized (gate) {
            checkStarting();
            if (Thread.currentThread() != worker || startIssued || privateKey == null || processOwner == null) {
                throw new IOException("Only the original launcher worker can issue one START");
            }
            startIssued = true;
        }
        byte[] unsigned = SegmentClassicLaunchCodec.encodeUnsignedStart(
                new SegmentClassicLaunchCodec.Start(material.recipe().signerKeyId(), expected, episode));
        Signature signature = Signature.getInstance("Ed25519");
        signature.initSign(privateKey);
        signature.update("BookKeeper/ControlledClassicLaunch/v1\0".getBytes(StandardCharsets.US_ASCII));
        signature.update(unsigned);
        byte[] packet = ByteBuffer.allocate(unsigned.length + 64).put(unsigned).put(signature.sign()).array();
        SegmentClassicLaunchCodec.verifyStart(packet, material.recipe().signerKeyId(),
                material.publicKey(), expected);
        return packet;
    }

    private void checkStarting() throws IOException {
        if (closing) {
            throw new StartupStopped();
        }
        remaining();
    }

    private Duration remaining() throws IOException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new IOException("Original launcher startup deadline elapsed");
        }
        return Duration.ofNanos(Math.min(remaining, TimeUnit.MINUTES.toNanos(1)));
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

    private void finish() {
        boolean complete = acquiring == null;
        boolean childEnded = processOwner == null && acquiring == null;
        try {
            if (processOwner != null) {
                var result = processOwner.closeAndDrain();
                complete &= result.drained();
                childEnded = result.processEnded();
                if (result.failure() != null) {
                    rememberOperation(result.failure());
                }
                if (result.closeFailure() != null) {
                    rememberClose(result.closeFailure());
                }
            }
        } catch (Throwable problem) {
            complete = false;
            rememberClose(problem);
        }
        try {
            if (childEnded && sources != null && originalScope != null) {
                requireOriginal(SegmentClassicChild.originalScope(configuration, sources.readAfterServiceStopped()));
            }
        } catch (Throwable problem) {
            rememberOperation(problem);
        }
        try {
            if (sources != null) {
                sources.closeAndDrain();
            }
            if (metadata != null && !metadataCloseAttempted) {
                metadataCloseAttempted = true;
                complete &= metadata.close(0);
            }
            if (sources != null) {
                complete &= sources.closeAndDrain().drained();
            }
        } catch (Throwable problem) {
            complete = false;
            rememberClose(problem);
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
            if (complete && supervisor != null && !supervisorCloseAttempted) {
                supervisorCloseAttempted = true;
                supervisor.close();
                complete &= supervisor.drainResult().drained();
            }
        } catch (Throwable problem) {
            complete = false;
            rememberClose(problem);
        }
        privateKey = null;
        synchronized (gate) {
            closed = complete && closeFailure == null;
            gate.notifyAll();
        }
    }
}
